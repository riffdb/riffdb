//! Real checked request manifests for transport tests; no source trust implied.
use riffdb_storage_api::{
    AuthoritativeNamespaceV1, ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    ChangelogTransactionSequence, LeadershipEpochV1, ReplicationAuthorityClassV1,
    ReplicationBootstrapFenceV3, ReplicationBootstrapPageV3, ReplicationBootstrapTranscriptV3,
    ReplicationSourceHoldIdV1,
};

pub(super) fn bytes() -> Vec<u8> {
    let lineage =
        ChangelogLineageV3::new(super::database_id(), 7, LeadershipEpochV1::new(11).unwrap())
            .unwrap();
    let point = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(19).unwrap(),
        [0x52; 32],
        riffdb_types::DualFrontier::new(
            riffdb_types::CommitSequence::new(3),
            riffdb_types::AdministrationSequence::new(5),
        ),
    );
    let fence = ReplicationBootstrapFenceV3::new(
        ReplicationSourceHoldIdV1::new([0x18; 16]).unwrap(),
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
    transcript.manifest().unwrap().encode().unwrap()
}
