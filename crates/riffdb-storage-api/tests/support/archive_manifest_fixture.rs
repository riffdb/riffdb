//! Deterministic inputs for external archive format vectors.
use riffdb_storage_api::*;
use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};

pub(crate) fn fixture() -> (ChangelogLineageV3, ChangelogHistoryPointV3) {
    (
        ChangelogLineageV3::new(
            DatabaseId::from_unix_milliseconds_and_random(1000, [1; 10]).unwrap(),
            1,
            LeadershipEpochV1::new(1).unwrap(),
        )
        .unwrap(),
        ChangelogHistoryPointV3::new(
            ChangelogTransactionSequence::new(1).unwrap(),
            [0x77; 32],
            DualFrontier::INITIAL,
        ),
    )
}
pub(crate) fn frame(
    lineage: ChangelogLineageV3,
    before: ChangelogHistoryPointV3,
    application: u64,
) -> Vec<u8> {
    let receipt = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: lineage.database_id(),
            history_incarnation: lineage.history_incarnation(),
            predecessor: Some(before.sequence()),
            sequence: before.sequence().checked_next().unwrap(),
            predecessor_frontier: before.frontier(),
            prior_history_hash: before.history_hash(),
            covered_frontier: DualFrontier::new(CommitSequence::new(application), None),
        },
        ChangelogAttributionV3::JournaledApplicationGroup,
        vec![
            AuthoritativeMutationV3::put(
                AuthoritativeNamespaceV1::Entities,
                b"key",
                None,
                b"private-value",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    ChangelogFrameV3::new(
        ChangelogFrameBindingV3::new(
            lineage.database_id(),
            lineage.history_incarnation(),
            lineage.leadership_epoch().get(),
            lineage.catalog_digest(),
            before.history_hash(),
        )
        .unwrap(),
        vec![receipt],
    )
    .unwrap()
    .encode()
    .unwrap()
}
