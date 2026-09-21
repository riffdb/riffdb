//! Managed receiver custody from initial-create recovery through tail apply.
// req: REP-002, REP-003, REC-001, PERF-007
use super::*;
use crate::{
    replication_bootstrap::BootstrapSourceJobs, replication_source::PublishedReplicationSource,
};
use riffdb_service::{
    ReplicationFailure, ReplicationFuture, ReplicationItem, ReplicationItemSource,
    ReplicationPhase, ReplicationRequest, ReplicationSourcePort,
};
use riffdb_storage_api::{
    ChangelogFrameV3, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory,
};
use riffdb_storage_redb::RedbBootstrapReceiverRepository as Repository;
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
async fn managed_receiver_recovers_initial_scratch_and_keeps_inventory_owned_through_tail() {
    managed_receiver(false).await;
}

#[tokio::test]
async fn managed_receiver_retires_on_confirmed_eof_and_reopens_without_the_manifest() {
    managed_receiver(true).await;
}

// The source performs real durable attachment. Its reply is then lost, or the
// successful stream ends before its first frame, at an explicit test boundary.
struct ReplyPeer<'a> {
    source: &'a dyn ReplicationSourcePort,
    lose: bool,
}
struct Eof;
impl ReplicationItemSource for Eof {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        Box::pin(async { Ok(None) })
    }
}
impl ReplicationSourcePort for ReplyPeer<'_> {
    fn open(
        &self,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            drop(self.source.open(request).await?);
            if self.lose {
                Err(ReplicationFailure::Unavailable)
            } else {
                Ok(Box::new(Eof) as Box<dyn ReplicationItemSource>)
            }
        })
    }
}

async fn managed_receiver(eof: bool) {
    let (_scope, source_path) =
        crate::real_storage_support::temporary_database_scope("managed-receiver");
    let root = source_path.parent().unwrap();
    let mut startup = crate::startup::open_redb_startup(
        &source_path,
        inputs(),
        &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
    )
    .unwrap();
    let publications = startup.take_replication_publications().unwrap();
    let (_, _, _, _, _, ports) = startup.into_parts();
    let history = ports
        .published_changelog_snapshot_v3()
        .unwrap()
        .authoritative_state_v3()
        .unwrap()
        .history();
    let peer = PublishedReplicationSource::new(
        publications,
        BootstrapSourceJobs::from_repository(
            ports
                .bootstrap_repository(&root.join("source-scratch"))
                .unwrap(),
        ),
    );
    let request = ReplicationRequest {
        database_id: history.lineage().database_id(),
        history_incarnation: history.lineage().history_incarnation(),
        leadership_epoch: history.lineage().leadership_epoch().get(),
        phase: ReplicationPhase::Bootstrap {
            hold_id: [0x72; 16],
            resume_manifest: vec![],
            after_page: 0,
        },
        after_sequence: 0,
        after_hash: [0; 32],
        after_frontier: DualFrontier::INITIAL,
        readable_format: ChangelogFrameV3::IDENTITY.to_owned(),
        catalog_digest: history.lineage().catalog_digest(),
        maximum_frame_bytes: riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES as u64,
        maximum_transitions: riffdb_storage_api::MAX_STAGED_COMMANDS as u64,
    };
    let receiver_root = root.join("receiver-scratch");
    let repository = Repository::open(&receiver_root).unwrap();
    let pending = receiver_root.join("transfer.creating");
    std::fs::create_dir(&pending).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pending, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::write(pending.join("transfer.redb"), []).unwrap();
    let jobs = BootstrapReceiverJobs::from_repository(repository);
    let mut receiving = jobs.connect_managed(&peer, request.clone()).await.unwrap();
    let manifest = receiving.progress().unwrap().manifest();
    assert!(!receiving.receive_next().await.unwrap());
    assert_eq!(receiving.progress().unwrap().page_count(), 1);
    assert!(!pending.exists());
    drop(jobs);
    assert!(Repository::open(&receiver_root).is_err());
    drop(receiving);
    let jobs = BootstrapReceiverJobs::from_repository(Repository::open(&receiver_root).unwrap());
    let mut receiving = jobs.connect_managed(&peer, request).await.unwrap();
    assert_eq!(receiving.progress().unwrap().page_count(), 1);
    assert_eq!(receiving.progress().unwrap().manifest(), manifest);
    while !receiving.receive_next().await.unwrap() {}
    let mut build = receiving
        .finish()
        .unwrap()
        .materialize_managed(inputs())
        .await
        .unwrap();
    while !build.advance().await.unwrap() {}
    let mut follower = build
        .publish_and_follow(root.join("follower.redb"))
        .await
        .unwrap();
    drop(jobs);
    assert!(
        Repository::open(&receiver_root).is_err(),
        "tail custody keeps the root even after the candidate/stage owners drop"
    );
    assert!(receiver_root.join("candidate/follower.redb").exists());
    assert!(receiver_root.join("transfer/transfer.redb").exists());
    assert!(
        follower
            .advance(&ReplyPeer {
                source: &peer,
                lose: true
            })
            .await
            .is_err()
    );
    assert!(
        receiver_root.join("candidate/follower.redb").exists(),
        "lost replies retain retry evidence"
    );
    assert!(receiver_root.join("transfer/transfer.redb").exists());
    // Lose the process-local manifest as well as the first attachment reply.
    // Reopen must recover it from scratch after the complete startup proof.
    follower.close().await.unwrap();
    let jobs = BootstrapReceiverJobs::from_repository(Repository::open(&receiver_root).unwrap());
    follower = jobs
        .reopen_follower(
            root.join("follower.redb"),
            inputs(),
            manifest.fence().history().lineage(),
            manifest.fence().hold_id(),
            None,
        )
        .await
        .unwrap();
    drop(jobs);
    if eof {
        assert!(
            follower
                .advance(&ReplyPeer {
                    source: &peer,
                    lose: false
                })
                .await
                .unwrap()
                .is_none()
        );
    } else {
        assert!(follower.advance(&peer).await.unwrap().is_some());
    }
    assert!(!receiver_root.join("candidate").exists());
    assert!(!receiver_root.join("transfer").exists());
    follower.close().await.unwrap();
    let repository = Repository::open(&receiver_root).unwrap();
    assert!(repository.recover_transfer().unwrap().is_none());
    let jobs = BootstrapReceiverJobs::from_repository(repository);
    let mut follower = jobs
        .reopen_follower(
            root.join("follower.redb"),
            inputs(),
            manifest.fence().history().lineage(),
            manifest.fence().hold_id(),
            None,
        )
        .await
        .unwrap();
    assert!(follower.advance(&peer).await.unwrap().is_some());
    follower.close().await.unwrap();
}
