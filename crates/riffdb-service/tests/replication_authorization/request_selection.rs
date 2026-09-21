// req: REP-003, REP-005
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1, ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    ChangelogTransactionSequence, LeadershipEpochV1, ReplicationAuthorityClassV1,
    ReplicationBootstrapFenceV3, ReplicationBootstrapPageV3, ReplicationBootstrapTranscriptV3,
    ReplicationSourceHoldIdV1,
};

pub(super) fn attachment_request() -> ReplicationRequest {
    let mut request = request();
    let lineage = ChangelogLineageV3::new(
        request.database_id,
        request.history_incarnation,
        LeadershipEpochV1::initial(),
    )
    .unwrap();
    let point = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(request.after_sequence).unwrap(),
        request.after_hash,
        request.after_frontier,
    );
    let fence = ReplicationBootstrapFenceV3::new(
        ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
        ChangelogHistoryStateV3::new(lineage, point, point, point).unwrap(),
    );
    let mut transcript = ReplicationBootstrapTranscriptV3::new(fence);
    let mut previous = fence.digest();
    for (index, namespace) in AuthoritativeNamespaceV1::ALL
        .into_iter()
        .filter(|n| n.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative)
        .enumerate()
    {
        let page = ReplicationBootstrapPageV3::new(
            fence.digest(),
            u32::try_from(index + 1).unwrap(),
            namespace,
            true,
            previous,
            vec![],
        )
        .unwrap();
        previous = *page.encode().unwrap().last_chunk::<32>().unwrap();
        transcript.observe(&page).unwrap();
    }
    request.catalog_digest = lineage.catalog_digest();
    request.phase = ReplicationPhase::Attach {
        manifest: transcript.manifest().unwrap().encode().unwrap(),
    };
    request
}

#[test]
fn substituted_attachment_lineage_is_refused_before_source_access() {
    support::run_async(async move {
        let original = attachment_request();
        for change in 0..4 {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let (source, _sender, reads, _drops) = source();
            let service = ReplicationService::new(policy.clone(), source.clone());
            let mut request = original.clone();
            match change {
                0 => request.database_id = database(2),
                1 => request.history_incarnation += 1,
                2 => request.leadership_epoch += 1,
                _ => request.catalog_digest[0] ^= 1,
            }
            assert!(matches!(
                ready(service.stream_changelog(policy.principal(), request)),
                Err(ReplicationFailure::Source(
                    ReplicationStreamErrorV3::InvalidPosition
                ))
            ));
            assert_eq!(source.opens.load(Ordering::Acquire), 0);
            assert_eq!(reads.load(Ordering::Acquire), 0);
        }
    });
}
