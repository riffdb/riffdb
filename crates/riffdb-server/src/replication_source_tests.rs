//! Real source storage composed with the production item adapter.
// req: REP-002, REP-003, REC-001, PERF-007
use super::*;
use crate::replication_bootstrap::BootstrapSourceJobs;
use crate::replication_publication::ReplicationPublication;
use riffdb_service::{ReplicationItem, ReplicationPhase};
use riffdb_storage_api::{
    AuthoritativeStateCatalogV1, ChangelogFrameV3, ChangelogPublicationPort,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    ReplicationBootstrapManifestV1 as Manifest, ReplicationBootstrapPageV3 as Page,
    ReplicationBootstrapTranscriptV3, StartupValidationInputs,
};
use riffdb_types::{DigestKeyId, DualFrontier, Timestamp};

#[tokio::test]
async fn production_source_bootstrap_resumes_exact_pages_and_attaches_to_retained_successors() {
    let (_scope, path) = crate::real_storage_support::temporary_database_scope("production-source");
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    let startup = crate::startup::open_redb_startup(
        &path,
        StartupValidationInputs::new(
            Timestamp::new(1000, 0).unwrap(),
            ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
            ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
        ),
        &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
    )
    .unwrap();
    let (_, _, _, _, _, ports) = startup.into_parts();
    let root = path.parent().unwrap().join("source-artifacts");
    let jobs = BootstrapSourceJobs::from_repository(ports.bootstrap_repository(&root).unwrap());
    let (publisher, publications) = ReplicationPublication::channel();
    let pin = ports.published_changelog_snapshot_v3().unwrap();
    let history = pin.authoritative_state_v3().unwrap().history();
    publisher.observe_published_snapshot_v3(pin);
    let source = PublishedReplicationSource::new(publications, jobs);
    let id = [0x62; 16];
    let request = ReplicationRequest {
        database_id: history.lineage().database_id(),
        phase: ReplicationPhase::Bootstrap {
            hold_id: id,
            resume_manifest: vec![],
            after_page: 0,
        },
        history_incarnation: history.lineage().history_incarnation(),
        leadership_epoch: history.lineage().leadership_epoch().get(),
        after_sequence: 0,
        after_hash: [0; 32],
        after_frontier: DualFrontier::INITIAL,
        readable_format: ChangelogFrameV3::IDENTITY.to_owned(),
        catalog_digest: AuthoritativeStateCatalogV1.digest(),
        maximum_frame_bytes: riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES as u64,
        maximum_transitions: riffdb_storage_api::MAX_STAGED_COMMANDS as u64,
    };
    for changed in 0..3 {
        let mut wrong = request.clone();
        match changed {
            0 => wrong.catalog_digest = [0; 32],
            1 => wrong.history_incarnation += 1,
            _ => wrong.leadership_epoch += 1,
        }
        assert!(source.open(wrong).await.is_err());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    }
    let mut stream = source.open(request.clone()).await.unwrap();
    let Some(ReplicationItem::BootstrapManifest(bytes)) = stream.next_item().await.unwrap() else {
        panic!("manifest first")
    };
    let manifest = Manifest::decode(&bytes).unwrap();
    assert_eq!(manifest.fence().history(), history);
    let mut transcript = ReplicationBootstrapTranscriptV3::new(manifest.fence());
    let Some(ReplicationItem::BootstrapPage(bytes)) = stream.next_item().await.unwrap() else {
        panic!("page one")
    };
    transcript.observe(&Page::decode(&bytes).unwrap()).unwrap();
    drop(stream);
    // The source hold receipt may advance publication while the immutable
    // artifact remains bound to the earlier complete snapshot.
    publisher.observe_published_snapshot_v3(ports.published_changelog_snapshot_v3().unwrap());
    let mut resume = request.clone();
    resume.phase = ReplicationPhase::Bootstrap {
        hold_id: id,
        resume_manifest: manifest.encode().unwrap(),
        after_page: 1,
    };
    let mut stream = source.open(resume.clone()).await.unwrap();
    assert_eq!(
        stream.next_item().await.unwrap(),
        Some(ReplicationItem::BootstrapManifest(
            manifest.encode().unwrap()
        ))
    );
    for ordinal in 2..=manifest.page_count() {
        let Some(ReplicationItem::BootstrapPage(bytes)) = stream.next_item().await.unwrap() else {
            panic!("next page")
        };
        let page = Page::decode(&bytes).unwrap();
        assert_eq!(page.ordinal(), ordinal);
        transcript.observe(&page).unwrap();
    }
    transcript.verify_manifest(manifest).unwrap();
    assert_eq!(stream.next_item().await.unwrap(), None);
    drop(stream);
    let mut interrupted = request.clone();
    interrupted.phase = ReplicationPhase::Bootstrap {
        hold_id: id,
        resume_manifest: manifest.encode().unwrap(),
        after_page: 0,
    };
    let mut stream = source.open(interrupted).await.unwrap();
    assert!(matches!(
        stream.next_item().await.unwrap(),
        Some(ReplicationItem::BootstrapManifest(_))
    ));
    publisher.observe_source_unavailable_v3();
    assert_eq!(
        stream.next_item().await.err(),
        Some(ReplicationFailure::Source(
            riffdb_errors::ReplicationStreamErrorV3::Unavailable
        ))
    );
    publisher.observe_published_snapshot_v3(ports.published_changelog_snapshot_v3().unwrap());
    assert_eq!(
        stream.next_item().await.unwrap(),
        None,
        "failed source cannot recover its artifact owner"
    );
    drop(stream);
    let mut changed = manifest.encode().unwrap();
    changed[30] ^= 1;
    resume.phase = ReplicationPhase::Bootstrap {
        hold_id: id,
        resume_manifest: changed,
        after_page: 1,
    };
    assert!(source.open(resume).await.is_err());
    let mut attachment = request;
    attachment.phase = ReplicationPhase::Attach {
        manifest: manifest.encode().unwrap(),
    };
    let acknowledged = manifest.fence().history().tail();
    attachment.after_sequence = acknowledged.sequence().get();
    attachment.after_hash = acknowledged.history_hash();
    attachment.after_frontier = acknowledged.frontier();
    let mut wrong = attachment.clone();
    wrong.after_hash[0] ^= 1;
    assert!(source.open(wrong).await.is_err());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
    for _ in 0..2 {
        let mut stream = source.open(attachment.clone()).await.unwrap();
        let Some(ReplicationItem::Frame(bytes)) = stream.next_item().await.unwrap() else {
            panic!("retained successor")
        };
        let frame = ChangelogFrameV3::decode(&bytes).unwrap();
        assert_eq!(
            frame.receipts()[0].binding().predecessor,
            Some(acknowledged.sequence())
        );
        assert_eq!(
            frame.receipts()[0].binding().prior_history_hash,
            acknowledged.history_hash()
        );
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    }
    let current_history = || {
        ports
            .published_changelog_snapshot_v3()
            .unwrap()
            .authoritative_state_v3()
            .unwrap()
            .history()
    };
    let before = current_history();
    publisher.observe_published_snapshot_v3(ports.published_changelog_snapshot_v3().unwrap());
    let mut follower = attachment.clone();
    follower.phase = ReplicationPhase::Follower { hold_id: id };
    follower.after_sequence = before.tail().sequence().get();
    follower.after_hash = before.tail().history_hash();
    follower.after_frontier = before.tail().frontier();
    for fault in 0..4 {
        let mut wrong = follower.clone();
        let expected = match fault {
            0 => {
                wrong.phase = ReplicationPhase::Follower {
                    hold_id: [0x63; 16],
                };
                riffdb_errors::ReplicationStreamErrorV3::InvalidPosition
            }
            1 => {
                wrong.after_hash[0] ^= 1;
                riffdb_errors::ReplicationStreamErrorV3::InvalidPosition
            }
            2 => {
                wrong.history_incarnation += 1;
                riffdb_errors::ReplicationStreamErrorV3::ForeignLineage
            }
            _ => {
                wrong.leadership_epoch += 1;
                riffdb_errors::ReplicationStreamErrorV3::StaleEpoch
            }
        };
        assert_eq!(
            source.open(wrong).await.err(),
            Some(ReplicationFailure::Source(expected))
        );
        assert_eq!(current_history(), before);
    }
    drop(source.open(follower.clone()).await.unwrap());
    let advanced = current_history();
    assert_eq!(
        advanced.tail().sequence(),
        before.tail().sequence().checked_next().unwrap()
    );
    drop(source.open(follower).await.unwrap());
    assert_eq!(
        current_history(),
        advanced,
        "an uncertain acknowledgement retries without another receipt"
    );
    let mut regression = attachment;
    regression.phase = ReplicationPhase::Follower { hold_id: id };
    assert_eq!(
        source.open(regression).await.err(),
        Some(ReplicationFailure::Source(
            riffdb_errors::ReplicationStreamErrorV3::InvalidPosition
        ))
    );
    assert_eq!(current_history(), advanced);
}
