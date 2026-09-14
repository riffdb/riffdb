//! Exact control-state oracle for the legacy-row migration fixtures. The raw
//! V1 fixture substitution is not a production V3 mutation or replay proof.
// req: REP-003, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, ChangelogAttributionV3 as A,
    proto_codec::{
        decode_changelog_history_state_v3, encode_changelog_history_state_v3,
        encode_changelog_transaction_allocator_v3,
    },
};

pub(super) fn assert_progress(
    path: &Path,
    before: &MigrationControlState,
    migrated: Option<(&StoredIndexEntryV2, &[u8])>,
) {
    let after = read_migration_control_state(path);
    assert_eq!(
        after.tables, before.tables,
        "migration cannot invent a table"
    );
    assert!(after.receipts.len() >= before.receipts.len());
    assert_eq!(
        &after.receipts[..before.receipts.len()],
        before.receipts,
        "migration and restart preserve every prior receipt byte"
    );
    let mut expected_metadata = before.metadata.clone();
    let history_key = N::ChangelogHistoryState.metadata_key().unwrap();
    let root = before
        .metadata
        .iter()
        .find(|(key, _)| key == history_key)
        .unwrap();
    let mut expected_history = *decode_changelog_history_state_v3(&root.1).unwrap().value();
    let mut migrations = 0;
    for receipt in &after.receipts[before.receipts.len()..] {
        assert_eq!(
            receipt.binding().predecessor_frontier,
            receipt.binding().covered_frontier,
            "migration and startup proof publication allocate no application or audit sequence"
        );
        match receipt.attribution() {
            A::IndexMigrationBatch => {
                let (current, prior) =
                    migrated.expect("precommit crash cannot leave a migration receipt");
                migrations += 1;
                assert_eq!(receipt.mutations().len(), 1);
                let mutation = &receipt.mutations()[0];
                assert_eq!(mutation.namespace(), N::SecondaryIndexes);
                assert_eq!(mutation.key(), current.key().as_bytes());
                assert!(mutation.matches_prior(Some(prior)));
                assert_eq!(
                    mutation.value(),
                    Some(encode_index_entry_v2(current).unwrap().as_bytes())
                );
            }
            A::ValidatedPrefixCheckpoint => {
                assert!(
                    receipt
                        .mutations()
                        .iter()
                        .any(|m| m.namespace() == N::ValidatedPrefixCheckpoint)
                );
                assert!(receipt.mutations().iter().all(|m| matches!(
                    m.namespace(),
                    N::ValidatedPrefixCheckpoint | N::ValidatedPrefixEntityHeads
                )));
            }
            A::DirtyActivation => assert!(receipt.mutations().is_empty()),
            _ => panic!("unexpected authoritative operation in migration recovery"),
        }
        expected_history = expected_history.advance(receipt).unwrap();
    }
    assert_eq!(
        migrations,
        usize::from(migrated.is_some()),
        "exactly one migration receipt, including on retry"
    );
    for (namespace, value) in [
        (
            N::ChangelogHistoryState,
            encode_changelog_history_state_v3(expected_history).unwrap(),
        ),
        (
            N::NextChangelogTransaction,
            encode_changelog_transaction_allocator_v3(expected_history.expected_allocator())
                .unwrap(),
        ),
    ] {
        let field = expected_metadata
            .iter_mut()
            .find(|(key, _)| key == namespace.metadata_key().unwrap())
            .unwrap();
        field.1 = value.as_bytes().to_vec();
    }
    assert_eq!(
        after.metadata, expected_metadata,
        "only the exactly derived receipt tail and allocator may advance; no other control marker changes"
    );
}
