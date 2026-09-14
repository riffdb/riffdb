//! Migration's immutable-authority witness must coexist with exact V3 progress.
// req: REC-001, STO-012

use super::*;
use redb::ReadableDatabase;
use riffdb_storage_api::{
    ChangelogAttributionV3, ChangelogLineageV3, DatabaseInitializationPort, LeadershipEpochV1,
};
use riffdb_types::{DatabaseId, DualFrontier};

#[test]
fn migration_immutable_witness_accepts_receipted_progress_but_not_authority_changes() {
    let scope = crate::test_path::ScopedDirectory::new("migration-v3-witness");
    let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x77; 10]).unwrap();
    store.initialize_database(database).unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(database, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let initial = store.shared.database.begin_read().unwrap();
    let initial_history = capture_v3_history(&initial).unwrap();
    let before = immutable_history_digest(&initial, initial_history).unwrap();
    drop(initial);
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &store.shared.database,
        crate::RedbCommitProfile::Hardened,
        ChangelogAttributionV3::ContractMigrationBatch,
    )
    .unwrap();
    // Opaque operation-owned payload: this test proves witness classification,
    // not migration-journal semantics (covered by the actual migration gates).
    write
        .open_table(CONTRACT_MIGRATION_JOURNAL)
        .unwrap()
        .insert(b"operation".as_slice(), b"progress".as_slice())
        .unwrap();
    write.finish().unwrap().commit_for_test().unwrap();
    let pin = store.shared.database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&pin)
        .unwrap()
        .unwrap();
    assert_eq!(history.tail().sequence().get(), 2);
    assert_eq!(
        immutable_history_digest(&pin, initial_history).unwrap(),
        before
    );
    drop(pin);

    // An otherwise valid receipt may not hide a mutation of immutable authority.
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &store.shared.database,
        crate::RedbCommitProfile::Hardened,
        ChangelogAttributionV3::ContractMigrationBatch,
    )
    .unwrap();
    write
        .open_table(COMMITS)
        .unwrap()
        .insert(b"forbidden".as_slice(), b"history-rewrite".as_slice())
        .unwrap();
    write.finish().unwrap().commit_for_test().unwrap();
    assert_ne!(
        immutable_history_digest(
            &store.shared.database.begin_read().unwrap(),
            initial_history
        )
        .unwrap(),
        before
    );

    // Even a structurally valid shortened chain must not erase the witness's
    // original prefix. Retention is not authority held by a private migration.
    let current = capture_v3_history(&store.shared.database.begin_read().unwrap())
        .unwrap()
        .unwrap();
    let shortened = riffdb_storage_api::ChangelogHistoryStateV3::new(
        current.lineage(),
        current.anchor(),
        current.tail(),
        current.tail(),
    )
    .unwrap();
    let transaction = store.shared.database.begin_write().unwrap();
    {
        let mut table = transaction
            .open_table(crate::changelog_v3_activation::HISTORY)
            .unwrap();
        for sequence in 1_u64..current.tail().sequence().get() {
            table.remove(sequence.to_be_bytes().as_slice()).unwrap();
        }
        let encoded =
            riffdb_storage_api::proto_codec::encode_changelog_history_state_v3(shortened).unwrap();
        transaction
            .open_table(META)
            .unwrap()
            .insert(
                riffdb_storage_api::AuthoritativeNamespaceV1::ChangelogHistoryState
                    .metadata_key()
                    .unwrap(),
                encoded.as_bytes(),
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let pin = store.shared.database.begin_read().unwrap();
    assert_eq!(capture_v3_history(&pin).unwrap(), Some(shortened));
    assert_eq!(
        immutable_history_digest(&pin, initial_history)
            .unwrap_err()
            .kind(),
        StorageErrorKind::CorruptData
    );
}
