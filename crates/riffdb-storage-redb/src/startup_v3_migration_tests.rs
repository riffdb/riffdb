//! Actual catalog-owned index migration batch over isolated V3 activation.
// req: REP-003, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3, ChangelogAttributionV3 as A,
    ChangelogLineageV3, LeadershipEpochV1,
};

fn migration_store(path: &TestDatabasePath) -> (RedbStore, Vec<Vec<u8>>) {
    migration_store_with_confirmation(path, false)
}

fn migration_store_with_confirmation(
    path: &TestDatabasePath,
    confirm_last: bool,
) -> (RedbStore, Vec<Vec<u8>>) {
    let id = database_id(0xd6);
    let store = deployed_migration_store(path, id);
    let write = store.shared.database.begin_write().unwrap();
    let rows = [
        compiled_migration_legacy_row(1),
        compiled_migration_legacy_row(2),
    ];
    let before = rows
        .iter()
        .enumerate()
        .map(|(offset, row)| {
            if confirm_last && offset == 1 {
                let current = StoredIndexEntryV2::new(
                    row.key().clone(),
                    row.schema_binding().clone(),
                    row.covered_values().clone(),
                    compiled_migration_partition(2),
                )
                .unwrap();
                codec::encode_index_entry_v2(&current).unwrap().into_bytes()
            } else {
                riffdb_storage_api::encode_index_entry_v1_fixture(row)
                    .unwrap()
                    .into_bytes()
            }
        })
        .collect::<Vec<_>>();
    {
        let mut indexes = write.open_table(SECONDARY_INDEXES).unwrap();
        for (row, bytes) in rows.iter().zip(&before) {
            indexes
                .insert(row.key().as_bytes(), bytes.as_slice())
                .unwrap();
        }
    }
    write.commit().unwrap();
    let read = store.shared.database.begin_read().unwrap();
    let frontier = riffdb_types::DualFrontier::new(
        crate::store::read_commit_tail(&read).unwrap(),
        crate::store::read_administration_tail(&read).unwrap(),
    );
    drop(read);
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        frontier,
    )
    .unwrap();
    (store, before)
}

fn run_migration(
    store: RedbStore,
    substitute: Option<StoredIndexEntryV2>,
) -> Result<RedbStore, CatalogIndexMigrationDriveError> {
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural_end = finish_structural(&mut session);
    let (catalog, historical_end) = validate_catalog_history(&mut session).unwrap().into_parts();
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog else {
        panic!("V1 rows need migration");
    };
    let StructuralOpenOutcome::MigrationRequired(mut port) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("migration owner");
    };
    if let Some(substitute) = substitute {
        port.substitute_before_apply(substitute);
    }
    CatalogIndexMigrationDriver::new(context, port)
        .unwrap()
        .run()
}

#[test]
fn actual_v3_catalog_index_migration_receipts_exact_batch_without_second_commit() {
    let path = TestDatabasePath::new("v3-index-migration");
    let (store, before) = migration_store(&path);
    let shared = Arc::clone(&store.shared);
    let epoch = shared.durable_commit_epoch();
    let store = run_migration(store, None).unwrap();
    assert_eq!(
        shared.durable_commit_epoch(),
        epoch + 1,
        "only the existing migration batch commits"
    );
    let read = store.shared.database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    assert_eq!(
        history.tail().sequence().get(),
        2,
        "migration must retain one exact receipt"
    );
    let rows = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let encoded = rows.get(2u64.to_be_bytes().as_slice()).unwrap().unwrap();
    let receipt = AuthoritativeTransactionV3::decode(encoded.value()).unwrap();
    assert_eq!(receipt.attribution(), A::IndexMigrationBatch);
    assert_eq!(receipt.mutations().len(), 2);
    let indexes = read.open_table(SECONDARY_INDEXES).unwrap();
    for (mutation, old) in receipt.mutations().iter().zip(&before) {
        assert_eq!(mutation.namespace(), N::SecondaryIndexes);
        assert!(mutation.matches_prior(Some(old)));
        let current = indexes.get(mutation.key()).unwrap().unwrap();
        assert_eq!(mutation.value(), Some(current.value()));
        assert!(riffdb_storage_api::decode_index_entry_v2(current.value()).is_ok());
    }
}

#[test]
fn actual_v3_mixed_index_migration_confirms_current_rows_without_receipting_them_again() {
    let path = TestDatabasePath::new("v3-index-migration-confirm");
    let (store, before) = migration_store_with_confirmation(&path, true);
    let store = run_migration(store, None).unwrap();
    let read = store.shared.database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    assert_eq!(history.tail().sequence().get(), 2);
    let rows = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let encoded = rows.get(2u64.to_be_bytes().as_slice()).unwrap().unwrap();
    let receipt = AuthoritativeTransactionV3::decode(encoded.value()).unwrap();
    assert_eq!(receipt.mutations().len(), 1);
    assert_eq!(
        receipt.mutations()[0].key(),
        compiled_migration_legacy_row(1).key().as_bytes()
    );
    assert!(receipt.mutations()[0].matches_prior(Some(&before[0])));
    assert_eq!(
        read.open_table(SECONDARY_INDEXES)
            .unwrap()
            .get(compiled_migration_legacy_row(2).key().as_bytes())
            .unwrap()
            .unwrap()
            .value(),
        before[1]
    );
}

#[test]
fn actual_v3_epoch_repair_marker_is_receipted_once_in_its_existing_commit() {
    let path = TestDatabasePath::new("v3-index-repair-marker");
    let id = database_id(0xd7);
    let store = initialized_store(&path, id);
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .remove(META_INDEX_EPOCH_ROWS_REPAIRED)
        .unwrap();
    write.commit().unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        riffdb_types::DualFrontier::INITIAL,
    )
    .unwrap();
    let epoch = store.shared.durable_commit_epoch();
    store
        .complete_partition_index_generation_migration()
        .unwrap();
    assert_eq!(store.shared.durable_commit_epoch(), epoch + 1);
    let read = store.shared.database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    assert_eq!(
        history.tail().sequence().get(),
        2,
        "repair marker needs one receipt"
    );
    let rows = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let encoded = rows.get(2u64.to_be_bytes().as_slice()).unwrap().unwrap();
    let receipt = AuthoritativeTransactionV3::decode(encoded.value()).unwrap();
    assert_eq!(receipt.attribution(), A::StorageFormatMigration);
    assert_eq!(receipt.mutations().len(), 1);
    assert!(receipt.mutations()[0].matches_prior(None));
    assert_eq!(receipt.mutations()[0].value(), Some([1u8].as_slice()));
    store
        .complete_partition_index_generation_migration()
        .unwrap();
    assert_eq!(store.shared.durable_commit_epoch(), epoch + 1);
}

#[test]
fn actual_v3_epoch_generation_repair_receipts_put_delete_and_marker_in_original_batches() {
    let path = TestDatabasePath::new("v3-index-epoch-repair");
    let id = database_id(0xd8);
    let store = initialized_store(&path, id);
    let row = compiled_migration_legacy_row(1);
    let current = StoredIndexEntryV2::new(
        row.key().clone(),
        row.schema_binding().clone(),
        row.covered_values().clone(),
        compiled_migration_partition(1),
    )
    .unwrap();
    let mut prefix = riffdb_storage_api::IndexRangePrefixBuilder::new(row.key().index_id());
    prefix.push_u64(1).unwrap();
    let prefix =
        riffdb_storage_api::StructurallyDecodedIndexRangePrefixV1::from_live(&prefix.finish());
    let old_epoch = riffdb_storage_api::LegacyStoredIndexEpochV1::new(
        prefix.clone(),
        row.schema_binding().clone(),
        riffdb_types::IndexEpoch::new(3).unwrap(),
    );
    let encoded = riffdb_storage_api::encode_legacy_index_epoch_v1_fixture(&old_epoch).unwrap();
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .remove(META_INDEX_EPOCH_ROWS_REPAIRED)
        .unwrap();
    write
        .open_table(SECONDARY_INDEXES)
        .unwrap()
        .insert(
            row.key().as_bytes(),
            codec::encode_index_entry_v2(&current).unwrap().as_bytes(),
        )
        .unwrap();
    write
        .open_table(INDEX_EPOCHS)
        .unwrap()
        .insert(
            keys::encode_index_range_prefix_key(&prefix),
            encoded.as_bytes(),
        )
        .unwrap();
    write.commit().unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        riffdb_types::DualFrontier::INITIAL,
    )
    .unwrap();
    let epoch = store.shared.durable_commit_epoch();
    store
        .complete_partition_index_generation_migration()
        .unwrap();
    assert_eq!(store.shared.durable_commit_epoch(), epoch + 3);
    let read = store.shared.database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    assert_eq!(
        history.tail().sequence().get(),
        4,
        "each original repair transaction needs its receipt"
    );
    let rows = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    for sequence in 2u64..=4 {
        let receipt = AuthoritativeTransactionV3::decode(
            rows.get(sequence.to_be_bytes().as_slice())
                .unwrap()
                .unwrap()
                .value(),
        )
        .unwrap();
        assert_eq!(receipt.attribution(), A::StorageFormatMigration);
        assert_eq!(receipt.mutations().len(), 1);
        let mutation = &receipt.mutations()[0];
        if sequence < 4 {
            assert_eq!(mutation.namespace(), N::IndexEpochs);
        }
        if sequence == 2 {
            assert!(mutation.matches_prior(None));
            let epoch =
                riffdb_storage_api::decode_index_epoch_v1(mutation.value().unwrap()).unwrap();
            assert_eq!(epoch.value().epoch().get(), 4);
        } else if sequence == 3 {
            assert!(mutation.matches_prior(Some(encoded.as_bytes())));
            assert!(mutation.value().is_none());
        } else {
            assert_eq!(mutation.value(), Some([1u8].as_slice()));
        }
    }
    store
        .complete_partition_index_generation_migration()
        .unwrap();
    assert_eq!(store.shared.durable_commit_epoch(), epoch + 3);
}

#[test]
fn actual_v3_migration_stale_compare_aborts_prior_row_rewrite_and_receipt() {
    let path = TestDatabasePath::new("v3-migration-stale");
    let (store, before) = migration_store(&path);
    let shared = Arc::clone(&store.shared);
    let epoch = shared.durable_commit_epoch();
    let row = compiled_migration_legacy_row(2);
    let replacement = StoredIndexEntryV2::new(
        row.key().clone(),
        row.schema_binding().clone(),
        CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(999))]).unwrap(),
        compiled_migration_partition(2),
    )
    .unwrap();
    let expected = codec::encode_index_entry_v2(&replacement).unwrap();
    let result = run_migration(store, Some(replacement));
    assert!(
        matches!(result, Err(CatalogIndexMigrationDriveError::Storage(ref error)) if error.kind() == StorageErrorKind::CorruptData)
    );
    assert_eq!(
        shared.durable_commit_epoch(),
        epoch + 1,
        "the explicit substitution fixture is the only commit; the migration batch rolls back"
    );
    let read = shared.database.begin_read().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&read)
            .unwrap()
            .unwrap()
            .tail()
            .sequence()
            .get(),
        1
    );
    let indexes = read.open_table(SECONDARY_INDEXES).unwrap();
    assert_eq!(
        indexes
            .get(compiled_migration_legacy_row(1).key().as_bytes())
            .unwrap()
            .unwrap()
            .value(),
        before[0]
    );
    assert_eq!(
        indexes.get(row.key().as_bytes()).unwrap().unwrap().value(),
        expected.as_bytes()
    );
}

#[test]
fn actual_v3_migration_keeps_precommit_and_unknown_outcomes() {
    for committed in [false, true] {
        let path = TestDatabasePath::new("v3-migration-hooks");
        drop(migration_store(&path));
        let controller = if committed {
            crate::RedbTestController::return_unknown_after_commit(
                crate::RedbTestOperation::IndexMigrationBatch,
            )
        } else {
            crate::RedbTestController::return_before_commit(
                crate::RedbTestOperation::IndexMigrationBatch,
            )
        };
        let store = RedbStore::open_with_test_controller(&path.0, controller).unwrap();
        let result = run_migration(store, None);
        let expected = if committed {
            StorageErrorKind::CommitStatusUnknown
        } else {
            StorageErrorKind::Unavailable
        };
        assert!(
            matches!(result, Err(CatalogIndexMigrationDriveError::Storage(ref error)) if error.kind() == expected)
        );
        let store = RedbStore::open(&path.0).unwrap();
        let read = store.shared.database.begin_read().unwrap();
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(&read)
                .unwrap()
                .unwrap()
                .tail()
                .sequence()
                .get(),
            if committed { 2 } else { 1 }
        );
        drop(read);
        let store = if committed {
            store
        } else {
            run_migration(store, None).unwrap()
        };
        drop(open_cleanly(store));
    }
}

#[test]
fn v3_actual_migration_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_V3_MIGRATION_PATH") else {
        return;
    };
    drop(run_migration(RedbStore::open(path).unwrap(), None).unwrap());
    panic!("migration crash edge not reached");
}

#[test]
fn actual_v3_index_migration_crashes_keep_original_or_complete_receipted_batch() {
    for edge in ["mutations", "receipt", "roots", "committed"] {
        let path = TestDatabasePath::new("v3-migration-crash");
        let (store, before) = migration_store(&path);
        drop(store);
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "startup::tests::v3_migration::v3_actual_migration_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_V3_MIGRATION_PATH", &path.0)
            .env("RIFFDB_V3_DIRECT_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93));
        for _ in 0..2 {
            let store = RedbStore::open(&path.0).unwrap();
            let read = store.shared.database.begin_read().unwrap();
            let history = crate::changelog_v3_roots::validate_retained_history(&read)
                .unwrap()
                .unwrap();
            assert_eq!(
                history.tail().sequence().get(),
                if edge == "committed" { 2 } else { 1 }
            );
            let indexes = read.open_table(SECONDARY_INDEXES).unwrap();
            for (offset, old) in before.iter().enumerate() {
                let row = compiled_migration_legacy_row(offset as u64 + 1);
                let value = indexes.get(row.key().as_bytes()).unwrap().unwrap();
                if edge == "committed" {
                    assert!(riffdb_storage_api::decode_index_entry_v2(value.value()).is_ok());
                } else {
                    assert_eq!(value.value(), old);
                }
            }
        }
        let store = RedbStore::open(&path.0).unwrap();
        let store = if edge == "committed" {
            store
        } else {
            run_migration(store, None).unwrap()
        };
        drop(open_cleanly(store));
    }
}
