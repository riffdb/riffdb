//! Offline prune over a populated semantic fixture activated before startup.
// req: REP-003, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3, ChangelogAttributionV3 as A,
    ChangelogLineageV3, LeadershipEpochV1,
};

fn initialized(path: &TestDatabasePath) {
    initialized_with_identity(path, database_id(0xd5));
}

fn initialized_with_identity(path: &TestDatabasePath, id: DatabaseId) {
    let (store, _) = checkpointable_history_store(path, id, 3);
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        riffdb_types::DualFrontier::new(
            CommitSequence::new(3),
            Some(AdministrationSequence::first()),
        ),
    )
    .unwrap();
    // The open below is the store's FIRST open under the current record
    // registry, so it takes the complete path and writes the validated-prefix
    // checkpoint: receipts [activation, prefix-checkpoint, dirty]. The prune
    // below then invalidates that checkpoint in its first transaction.
    drop(open_cleanly(store));
}

fn receipts(path: &Path) -> Vec<AuthoritativeTransactionV3> {
    let store = RedbStore::open(path).unwrap();
    let read = store.shared.database.begin_read().unwrap();
    crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    read.open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| AuthoritativeTransactionV3::decode(row.unwrap().1.value()).unwrap())
        .collect()
}

#[test]
fn actual_offline_prune_receipts_preserve_exact_deletions_and_watermark_atomicity() {
    let path = TestDatabasePath::new("v3-offline-prune");
    initialized(&path);
    let original = receipts(&path.0);
    assert_eq!(original.len(), 3);
    let commands = {
        let database = redb::Database::open(&path.0).unwrap();
        let read = database.begin_read().unwrap();
        read.open_table(COMMITS)
            .unwrap()
            .iter()
            .unwrap()
            .map(|row| {
                let (key, value) = row.unwrap();
                (key.value().to_vec(), value.value().to_vec())
            })
            .collect::<Vec<_>>()
    };
    let offline = crate::RedbOfflineRetention::bind(&path.0);
    let status = offline.prune_to(2).unwrap();
    assert_eq!(status.watermark_sequence, 2);
    let after = receipts(&path.0);
    assert_eq!(
        after.len(),
        original.len() + 2,
        "checkpoint invalidation and prune each need a receipt"
    );
    for (before, after) in original.iter().zip(after.iter()) {
        assert_eq!(before.encode().unwrap(), after.encode().unwrap());
    }
    let invalidation = &after[3];
    assert_eq!(invalidation.attribution(), A::RetentionPrune);
    assert_eq!(invalidation.mutations().len(), 1);
    assert_eq!(
        invalidation.mutations()[0].namespace(),
        N::ValidatedPrefixCheckpoint
    );
    assert!(invalidation.mutations()[0].value().is_none());
    let pruned = &after[4];
    assert_eq!(pruned.attribution(), A::RetentionPrune);
    assert_eq!(pruned.mutations().len(), 4);
    for (key, value) in &commands[..2] {
        let deletion = pruned
            .mutations()
            .iter()
            .find(|mutation| mutation.namespace() == N::Commits && mutation.key() == key)
            .unwrap();
        assert!(deletion.value().is_none());
        assert!(deletion.matches_prior(Some(value)));
    }
    assert!(
        pruned
            .mutations()
            .iter()
            .any(|mutation| mutation.namespace() == N::HistoryTombstones)
    );
    assert!(
        pruned
            .mutations()
            .iter()
            .any(|mutation| mutation.namespace() == N::RetentionWatermark)
    );
    // Independently reproduce the old v1 buffered preimage, not the new walker.
    let mut preimage = b"riffdb.history-tombstone-content/v1\0".to_vec();
    preimage.extend_from_slice(&1u64.to_be_bytes());
    preimage.extend_from_slice(&2u64.to_be_bytes());
    for (key, value) in &commands[..2] {
        preimage.extend_from_slice(b"commits\0");
        preimage.extend_from_slice(key);
        preimage.extend_from_slice(&(value.len() as u64).to_be_bytes());
        preimage.extend_from_slice(value);
    }
    let expected_digest = riffdb_types::hash(riffdb_types::HashDomain::Schema, &preimage);
    let tombstone = pruned
        .mutations()
        .iter()
        .find(|mutation| mutation.namespace() == N::HistoryTombstones)
        .unwrap();
    let tombstone =
        riffdb_storage_api::decode_history_tombstone_v1(tombstone.value().unwrap()).unwrap();
    assert_eq!(
        tombstone.value().content_digest().as_bytes(),
        expected_digest.as_bytes()
    );
    assert_eq!(
        pruned.binding().covered_frontier,
        original[0].binding().covered_frontier
    );
    assert_eq!(offline.prune_to(2).unwrap().watermark_sequence, 2);
    assert_eq!(receipts(&path.0), after);
    assert_eq!(offline.prune_to(3).unwrap().watermark_sequence, 3);
    let complete = receipts(&path.0);
    assert_eq!(
        complete.len(),
        6,
        "absent checkpoint is not another authoritative transition"
    );
    for (before, after) in after.iter().zip(complete.iter()) {
        assert_eq!(before.encode().unwrap(), after.encode().unwrap());
    }
    // The prune deleted the validated-prefix checkpoint, so the post-prune
    // open has no active checkpoint and takes the complete path again (no
    // clean-certificate fast path, no bounded dirty path).
    let ports = open_cleanly(RedbStore::open(&path.0).unwrap());
    assert!(!ports.clean_close_fast_startup());
}

#[test]
fn actual_v3_prune_preserves_per_transaction_refusal_unknown_and_retry_semantics() {
    use crate::{RedbOfflineRetention, RedbTestController, RedbTestOperation};
    for operation in [
        RedbTestOperation::RetentionPruneCheckpointDelete,
        RedbTestOperation::RetentionPruneSubrange,
    ] {
        for committed in [false, true] {
            let path = TestDatabasePath::new("v3-prune-hooks");
            initialized(&path);
            let controller = if committed {
                RedbTestController::return_unknown_after_commit(operation)
            } else {
                RedbTestController::return_before_commit(operation)
            };
            let offline = RedbOfflineRetention::bind_with_test_controller(&path.0, controller);
            let error = offline.prune_to(2).unwrap_err();
            assert_eq!(
                error.kind(),
                if committed {
                    StorageErrorKind::CommitStatusUnknown
                } else {
                    StorageErrorKind::Unavailable
                }
            );
            let expected = match (operation, committed) {
                (RedbTestOperation::RetentionPruneCheckpointDelete, false) => 3,
                (RedbTestOperation::RetentionPruneSubrange, true) => 5,
                _ => 4,
            };
            assert_eq!(receipts(&path.0).len(), expected);
            let offline = RedbOfflineRetention::bind(&path.0);
            assert_eq!(
                offline.status().unwrap().watermark_sequence,
                if expected == 5 { 2 } else { 0 }
            );
            offline.prune_to(2).unwrap();
            assert_eq!(receipts(&path.0).len(), 5);
        }
    }
}

#[test]
fn actual_v3_prune_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_V3_PRUNE_PATH") else {
        return;
    };
    crate::RedbOfflineRetention::bind(path).prune_to(2).unwrap();
    panic!("prune crash edge was not reached");
}

#[test]
fn actual_v3_prune_crashes_keep_checkpoint_invalidation_and_subrange_atomic() {
    for (edge, expected) in [
        ("checkpoint-staged", 3),
        ("checkpoint-committed", 4),
        ("subrange-staged", 4),
        ("subrange-committed", 5),
    ] {
        let path = TestDatabasePath::new("v3-prune-crash");
        initialized(&path);
        let original = receipts(&path.0);
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "startup::tests::v3_retention::actual_v3_prune_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_V3_PRUNE_PATH", &path.0)
            .env("RIFFDB_V3_PRUNE_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        let mut previous = None;
        for _ in 0..2 {
            let current = receipts(&path.0);
            assert_eq!(current.len(), expected);
            for (old, new) in original.iter().zip(current.iter()) {
                assert_eq!(old.encode().unwrap(), new.encode().unwrap());
            }
            if let Some(previous) = &previous {
                assert_eq!(previous, &current);
            }
            previous = Some(current);
            let offline = crate::RedbOfflineRetention::bind(&path.0);
            assert_eq!(
                offline.status().unwrap().watermark_sequence,
                if expected == 5 { 2 } else { 0 }
            );
        }
        crate::RedbOfflineRetention::bind(&path.0)
            .prune_to(2)
            .unwrap();
        assert_eq!(receipts(&path.0).len(), 5);
        drop(open_cleanly(RedbStore::open(&path.0).unwrap()));
    }
}

// req: REC-001, STO-012, END-008, END-009
#[test]
fn offline_pruned_backup_restore_resolves_its_own_retained_journal_after_source_closes() {
    use crate::journal::{
        FrameChangelogBinding, JournalFileHeader, JournalFrame, JournalLane, JournalMutation,
        JournalTable,
    };
    use riffdb_storage_api::{
        BackupBuildMetadataV1, OfflineBackupPersistencePort, OfflineRestoreOverwritePolicyV1,
        OfflineRestorePersistencePort, OfflineRestoreResultV1,
    };
    let path = TestDatabasePath::new("pruned-backup-restore-resolver");
    // A distinct identity keeps concurrent fixtures out of the resolver lookup;
    // the production registry deliberately allows same-identity backup copies.
    let id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, *b"prunedrest").unwrap();
    initialized_with_identity(&path, id);
    assert_eq!(
        crate::RedbOfflineRetention::bind(&path.0)
            .prune_to(2)
            .unwrap()
            .watermark_sequence,
        2
    );
    let store = RedbStore::open(&path.0).unwrap();
    let before = crate::changelog_v3_roots::validate_retained_history(
        &store.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    let predecessor = before.tail().frontier();
    let assigned =
        AdministrationSequence::new(predecessor.administration().unwrap().get() + 1).unwrap();
    let unbound = JournalFrame::service_audit(
        before.lineage().database_id(),
        predecessor.application(),
        predecessor.administration(),
        Some(assigned),
        1,
        [0x42; 32],
        vec![
            JournalMutation::put(
                JournalTable::Audit,
                crate::keys::encode_audit_key(assigned),
                crate::test_audit::canonical_denied_service_audit(assigned),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Meta,
                META_ADMINISTRATION_SEQUENCE.as_bytes(),
                codec::encode_administration_sequence_allocator_v1(
                    AdministrationSequenceAllocator::Next(assigned),
                )
                .unwrap()
                .as_bytes(),
                codec::encode_administration_sequence_allocator_v1(
                    AdministrationSequenceAllocator::Next(
                        AdministrationSequence::new(assigned.get() + 1).unwrap(),
                    ),
                )
                .unwrap()
                .into_bytes(),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Meta,
                N::NextChangelogTransaction
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                riffdb_storage_api::proto_codec::encode_changelog_transaction_allocator_v3(
                    before.expected_allocator(),
                )
                .unwrap()
                .as_bytes(),
                riffdb_storage_api::proto_codec::encode_changelog_transaction_allocator_v3(
                    before.expected_allocator().allocate_one().unwrap().1,
                )
                .unwrap()
                .into_bytes(),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let write = store.shared.database.begin_write().unwrap();
    let receipt =
        crate::changelog_v3_journal::materialize_recovered_frame(&write, &unbound).unwrap();
    write.abort().unwrap();
    let source = unbound.with_changelog(Some(FrameChangelogBinding::new(
        &receipt.binding(),
        before.lineage().catalog_digest(),
    )));
    drop(store);
    let journal = crate::journal::journal_path(&path.0);
    // The fixture has no journaled suffix; initialize its next real source from
    // the already durable checkpoint rather than claiming an application write.
    if journal.exists() {
        std::fs::remove_file(&journal).unwrap();
    }
    let lane = JournalLane::open(
        &journal,
        &JournalFileHeader::with_frontiers(
            before.lineage().database_id(),
            predecessor.application(),
            predecessor.administration(),
            source.previous_hash(),
        ),
    )
    .unwrap();
    lane.submit(source.encode().unwrap())
        .unwrap()
        .wait()
        .unwrap();
    drop(lane);
    let recovered = RedbStore::open(&path.0).unwrap();
    let read = recovered.shared.database.begin_read().unwrap();
    let table = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let tail = table.last().unwrap().unwrap().1.value().to_vec();
    assert!(
        crate::history_stub::HistoryStub::decode(&tail)
            .unwrap()
            .is_some(),
        "the test must exercise an actual recovered journal locator"
    );
    assert_eq!(
        crate::history_stub::resolve_history_value(&tail)
            .unwrap()
            .as_ref(),
        receipt.encode().unwrap()
    );
    drop(table);
    drop(read);
    drop(recovered);
    assert_eq!(
        crate::history_stub::resolve_history_value(&tail)
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable,
        "no live source resolver may make restore pass accidentally"
    );
    let scope = crate::test_path::ScopedDirectory::new("pruned-backup-restore-target");
    let backup = scope.join("backup");
    let manifest = crate::RedbOfflineBackup::bind(&path.0, &backup)
        .create_offline_backup(
            &BackupBuildMetadataV1::new("0.1.0", "abc", "rustc", 1, vec![]).unwrap(),
        )
        .unwrap();
    assert_eq!(manifest.effective_retention_watermark_sequence(), 2);
    let backup_database = backup.join(crate::backup::DATABASE_ARTIFACT_FILE_NAME);
    assert!(std::fs::metadata(&backup_database).unwrap().len() <= 64 * 1024 * 1024);
    let original = std::fs::read(&backup_database).unwrap();
    let target = scope.join("restored");
    let result = crate::RedbOfflineRestore::bind(&backup, &target)
        .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty);
    assert!(
        matches!(result, Ok(OfflineRestoreResultV1::Restored { .. })),
        "pruned backup must resolve its own retained receipt while restoring its watermark: {result:?}"
    );
    let restored =
        RedbStore::open(target.join(crate::backup::DATABASE_ARTIFACT_FILE_NAME)).unwrap();
    let read = restored.shared.database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    assert_eq!(history, before.advance(&receipt).unwrap());
    assert_eq!(
        crate::retention::load_watermark(&read)
            .unwrap()
            .unwrap()
            .watermark_sequence(),
        2
    );
    assert_eq!(
        std::fs::metadata(&backup_database).unwrap().len(),
        original.len() as u64
    );
    assert_eq!(std::fs::read(&backup_database).unwrap(), original);
}
