//! Connection custody over actual source and receiver storage, before readiness.
// req: REP-002, REP-003, REC-001, PERF-007
use super::*;
use crate::replication_bootstrap::BootstrapSourceJobs;
use crate::replication_publication::ReplicationPublication;
use crate::replication_source::PublishedReplicationSource;
use riffdb_service::{ReplicationPhase, ReplicationRequest};
use riffdb_storage_api::{
    AuthoritativeStateCatalogV1, ChangelogFrameV3, ChangelogPublicationPort,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs,
};
use riffdb_types::{DigestKeyId, DualFrontier, Timestamp};

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1000, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}
#[tokio::test]
async fn receiver_connection_recovers_exact_durable_pages_and_requires_source_eof() {
    let (_scope, path) =
        crate::real_storage_support::temporary_database_scope("receiver-connection");
    let startup = crate::startup::open_redb_startup(
        &path,
        inputs(),
        &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
    )
    .unwrap();
    let (_, _, _, _, _, ports) = startup.into_parts();
    let root = path.parent().unwrap();
    let jobs = BootstrapSourceJobs::from_repository(
        ports.bootstrap_repository(&root.join("source")).unwrap(),
    );
    let (publisher, publications) = ReplicationPublication::channel();
    let pin = ports.published_changelog_snapshot_v3().unwrap();
    let history = pin.authoritative_state_v3().unwrap().history();
    publisher.observe_published_snapshot_v3(pin);
    let peer = PublishedReplicationSource::new(publications, jobs);
    let request = ReplicationRequest {
        database_id: history.lineage().database_id(),
        phase: ReplicationPhase::Bootstrap {
            hold_id: [0x65; 16],
            resume_manifest: vec![],
            after_page: 0,
        },
        history_incarnation: history.lineage().history_incarnation(),
        leadership_epoch: history.lineage().leadership_epoch().get(),
        after_sequence: 0,
        after_hash: [0; 32],
        after_frontier: DualFrontier::INITIAL,
        readable_format: ChangelogFrameV3::IDENTITY.to_owned(),
        catalog_digest: history.lineage().catalog_digest(),
        maximum_frame_bytes: riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES as u64,
        maximum_transitions: riffdb_storage_api::MAX_STAGED_COMMANDS as u64,
    };
    let jobs = BootstrapReceiverJobs::new();
    let path = root.join("transfer");
    let mut connection = jobs
        .connect(&peer, path.clone(), request.clone(), false)
        .await
        .unwrap();
    let manifest = connection.progress().unwrap().manifest();
    assert!(!connection.receive_next().await.unwrap());
    assert_eq!(connection.progress().unwrap().page_count(), 1);
    assert!(
        jobs.connect(&peer, root.join("overbook"), request.clone(), false)
            .await
            .is_err()
    );
    assert!(!root.join("overbook").exists());
    drop(connection);
    publisher.observe_published_snapshot_v3(ports.published_changelog_snapshot_v3().unwrap());
    let mut connection = jobs
        .connect(&peer, path.clone(), request.clone(), true)
        .await
        .unwrap();
    assert_eq!(connection.progress().unwrap().manifest(), manifest);
    assert_eq!(connection.progress().unwrap().page_count(), 1);
    for ordinal in 2..=manifest.page_count() {
        assert!(!connection.receive_next().await.unwrap());
        assert_eq!(connection.progress().unwrap().page_count(), ordinal);
    }
    assert!(
        connection.finish().is_err(),
        "all pages alone cannot substitute for source EOF"
    );
    let mut connection = jobs.connect(&peer, path, request, true).await.unwrap();
    assert!(connection.receive_next().await.unwrap());
    let transfer = connection.finish().unwrap();
    let mut build = transfer
        .materialize(root.join("candidate"), false, inputs())
        .await
        .unwrap();
    while !build.advance().await.unwrap() {}
    let mut applier = build
        .publish_and_activate(root.join("follower.redb"))
        .await
        .unwrap();
    assert_eq!(applier.durable_history().unwrap(), history);
    assert_eq!(
        applier.acknowledge_durable_position().unwrap(),
        manifest.fence().history().tail()
    );
}

use riffdb_service::{
    ReplicationFailure as Failure, ReplicationFuture, ReplicationItem as Item,
    ReplicationItemSource, ReplicationSourcePort,
};
use std::{collections::VecDeque, sync::Mutex};
struct ScriptedStream {
    items: VecDeque<Item>,
    wait: bool,
    dropped: Arc<AtomicBool>,
}
impl Drop for ScriptedStream {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl ReplicationItemSource for ScriptedStream {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<Item>> {
        Box::pin(async move {
            if let Some(item) = self.items.pop_front() {
                return Ok(Some(item));
            }
            if self.wait {
                std::future::pending().await
            } else {
                Ok(None)
            }
        })
    }
}
struct ScriptedPeer {
    stream: Mutex<Option<ScriptedStream>>,
    opened: AtomicBool,
}
impl ReplicationSourcePort for ScriptedPeer {
    fn open(&self, _: ReplicationRequest) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            self.opened.store(true, Ordering::SeqCst);
            Ok(Box::new(self.stream.lock().unwrap().take().unwrap())
                as Box<dyn ReplicationItemSource>)
        })
    }
}
fn scripted(items: Vec<Item>, wait: bool) -> (ScriptedPeer, Arc<AtomicBool>) {
    let dropped = Arc::new(AtomicBool::new(false));
    (
        ScriptedPeer {
            stream: Mutex::new(Some(ScriptedStream {
                items: items.into(),
                wait,
                dropped: Arc::clone(&dropped),
            })),
            opened: AtomicBool::new(false),
        },
        dropped,
    )
}
fn wire_fixture() -> (Manifest, Vec<u8>, ReplicationRequest) {
    fn unhex(value: &str) -> Vec<u8> {
        value
            .trim()
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }
    let manifest = Manifest::decode(&unhex(include_str!(
        "../../../fixtures/replication/bootstrap-manifest-v1.hex"
    )))
    .unwrap();
    let page = unhex(
        include_str!("../../../fixtures/replication/bootstrap-pages-v1.hex")
            .lines()
            .next()
            .unwrap(),
    );
    let lineage = manifest.fence().history().lineage();
    let request = ReplicationRequest {
        database_id: lineage.database_id(),
        history_incarnation: lineage.history_incarnation(),
        leadership_epoch: lineage.leadership_epoch().get(),
        phase: ReplicationPhase::Bootstrap {
            hold_id: *manifest.fence().hold_id().as_bytes(),
            resume_manifest: vec![],
            after_page: 0,
        },
        after_sequence: 0,
        after_hash: [0; 32],
        after_frontier: DualFrontier::INITIAL,
        readable_format: ChangelogFrameV3::IDENTITY.to_owned(),
        catalog_digest: AuthoritativeStateCatalogV1.digest(),
        maximum_frame_bytes: riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES as u64,
        maximum_transitions: riffdb_storage_api::MAX_STAGED_COMMANDS as u64,
    };
    (manifest, page, request)
}

#[test]
fn receiver_manifest_refuses_another_supported_catalog_with_matching_source_coordinates() {
    let (manifest, _, mut request) = wire_fixture();
    super::connection::validate_manifest(&request, manifest).unwrap();
    request.catalog_digest = riffdb_storage_api::AuthoritativeStateCatalogV2.digest();
    assert_eq!(
        super::connection::validate_manifest(&request, manifest),
        Err(Failure::Source(
            riffdb_errors::ReplicationStreamErrorV3::UnsupportedCatalog
        ))
    );
}

#[tokio::test]
async fn receiver_connection_cancellation_releases_stream_file_and_capacity_before_resume() {
    let (_scope, db) =
        crate::real_storage_support::temporary_database_scope("receiver-cancel-network");
    let path = db.parent().unwrap().join("transfer");
    let (manifest, _, request) = wire_fixture();
    let (peer, dropped) = scripted(
        vec![Item::BootstrapManifest(manifest.encode().unwrap())],
        true,
    );
    let jobs = BootstrapReceiverJobs::new();
    let mut connection = jobs
        .connect(&peer, path.clone(), request.clone(), false)
        .await
        .unwrap();
    let (other, _) = scripted(vec![], false);
    assert!(
        jobs.connect(&other, path.clone(), request, false)
            .await
            .is_err()
    );
    assert!(
        !other.opened.load(Ordering::SeqCst),
        "admission precedes all peer work"
    );
    let mut waiting = Box::pin(connection.receive_next());
    assert!(futures_util::poll!(&mut waiting).is_pending());
    drop(waiting);
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(jobs.capacity.available_permits(), 1);
    let recovered = RedbBootstrapStage::recover(&path).unwrap();
    assert_eq!(recovered.progress().page_count(), 0);
    assert_eq!(recovered.progress().manifest(), manifest);
    assert!(connection.progress().is_err());
    assert!(connection.receive_next().await.is_err());
    assert!(connection.finish().is_err());
}

#[tokio::test]
async fn receiver_connection_refuses_wrong_phases_truncation_and_repeated_pages_without_progress() {
    let (manifest, page, request) = wire_fixture();
    let first = Item::BootstrapManifest(manifest.encode().unwrap());
    for items in [
        vec![Item::BootstrapPage(page.clone())],
        vec![Item::BootstrapManifest(vec![1])],
        vec![Item::BootstrapManifest(vec![1; 513])],
        vec![first.clone()],
        vec![first.clone(), first.clone()],
        vec![first.clone(), Item::Frame(vec![1].into())],
        vec![
            first.clone(),
            Item::BootstrapPage(page.clone()),
            Item::BootstrapPage(page.clone()),
        ],
    ] {
        let (_scope, db) =
            crate::real_storage_support::temporary_database_scope("receiver-wire-refusals");
        let path = db.parent().unwrap().join("transfer");
        let (peer, dropped) = scripted(items, false);
        let jobs = BootstrapReceiverJobs::new();
        if let Ok(mut connection) = jobs
            .connect(&peer, path.clone(), request.clone(), false)
            .await
        {
            let mut received = 0;
            while let Ok(complete) = connection.receive_next().await {
                assert!(!complete, "malformed transfer cannot complete");
                received += 1;
                assert!(received <= 1);
            }
            assert!(connection.progress().is_err());
            assert!(connection.finish().is_err());
            let recovered = RedbBootstrapStage::recover(&path).unwrap();
            assert_eq!(recovered.progress().page_count(), received);
        } else {
            assert!(!path.exists());
        }
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(jobs.capacity.available_permits(), 1);
    }
}

#[tokio::test]
async fn receiver_connection_refuses_foreign_local_resume_before_contacting_the_peer() {
    let (_scope, db) =
        crate::real_storage_support::temporary_database_scope("receiver-foreign-resume");
    let path = db.parent().unwrap().join("transfer");
    let (manifest, _, mut request) = wire_fixture();
    drop(RedbBootstrapStage::create(&path, manifest).unwrap());
    request.history_incarnation += 1;
    let (peer, _) = scripted(vec![], false);
    let jobs = BootstrapReceiverJobs::new();
    assert_eq!(
        jobs.connect(&peer, path.clone(), request, true).await.err(),
        Some(Failure::Source(
            riffdb_errors::ReplicationStreamErrorV3::ForeignLineage
        ))
    );
    assert!(!peer.opened.load(Ordering::SeqCst));
    assert_eq!(
        RedbBootstrapStage::recover(&path)
            .unwrap()
            .progress()
            .page_count(),
        0
    );
    assert_eq!(jobs.capacity.available_permits(), 1);
}

#[tokio::test]
async fn receiver_connection_deadline_drops_pending_peer_without_modifying_local_progress() {
    let (_scope, db) =
        crate::real_storage_support::temporary_database_scope("receiver-network-deadline");
    let path = db.parent().unwrap().join("transfer");
    let (manifest, _, request) = wire_fixture();
    let (peer, dropped) = scripted(
        vec![Item::BootstrapManifest(manifest.encode().unwrap())],
        true,
    );
    let jobs = BootstrapReceiverJobs::new();
    let mut connection = jobs
        .connect(&peer, path.clone(), request, false)
        .await
        .unwrap();
    tokio::time::pause();
    let mut waiting = Box::pin(connection.receive_next());
    assert!(futures_util::poll!(&mut waiting).is_pending());
    tokio::time::advance(LIFETIME).await;
    assert_eq!(waiting.await.err(), Some(Failure::Unavailable));
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(jobs.capacity.available_permits(), 1);
    assert_eq!(
        RedbBootstrapStage::recover(&path)
            .unwrap()
            .progress()
            .page_count(),
        0
    );
    assert!(connection.finish().is_err());
}
