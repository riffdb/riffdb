//! Native archive conversion retains custody after consuming its source proof.
use super::*;
use riffdb_storage_api::*;
use riffdb_types::{BackupNameV1, DigestKeyId, OfflineMaintenanceOperationId, Timestamp};

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}

fn force_staged_allocator_repair(path: &Path) {
    let mut bytes = std::fs::read(path).unwrap();
    assert_eq!(&bytes[..9], b"redb\x1a\x0a\xa9\x0d\x0a");
    assert_eq!(bytes[9] & 2, 0, "fixture must begin clean");
    bytes[9] = (bytes[9] | 2) & !4;
    std::fs::write(path, bytes).unwrap();
}

fn materialize_native_archive(
    scope: &crate::test_path::ScopedDirectory,
    backup: &Path,
) -> RedbStagedRestore {
    RedbStagedRestore::materialize_owned_archive(
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(
            1_700_000_000_000,
            [0x78; 10],
        )
        .unwrap(),
        BackupNameV1::new("native-archive").unwrap(),
        backup.to_path_buf(),
        scope.join("stage"),
        scope.join("configured.redb"),
        inputs(),
        None,
    )
    .unwrap()
}

// req: REC-001, REC-006, END-001, REP-007, AFC-007
#[test]
fn native_archive_image_validation_preserves_the_immutable_backup_history_fence() {
    use redb::ReadableDatabase;
    let scope = crate::test_path::ScopedDirectory::new("native-archive-original-image");
    let (backup, expected) =
        crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let stage = materialize_native_archive(&scope, &backup);
    let owner = stage.native_archive_preparation.as_ref().unwrap();
    let database = owner.open_preparation_database().unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&read).unwrap(),
        Some(expected),
        "archive image verification must not append ordinary source compatibility receipts"
    );
    drop(read);
    drop(database);
    let repository = crate::RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(
            &scope.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let converted = stage
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    assert_eq!(converted.history, expected);
    crate::maintenance::probe_restore_custody(converted.staged_database_file(), true);
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
}

// req: REC-001, REC-006, END-001
#[test]
fn native_archive_conversion_retains_exclusion_after_original_transaction_and_journal_removal() {
    let scope = crate::test_path::ScopedDirectory::new("native-archive-conversion");
    let (backup, history) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let repository = crate::RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(
            &scope.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let stage = materialize_native_archive(&scope, &backup)
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    // Both original conversion engine opens have returned and the original
    // empty source journal has been removed. Native exclusion must continue.
    crate::maintenance::probe_restore_custody(stage.staged_database_file(), true);
    assert!(!crate::journal::journal_path(stage.staged_database_file()).exists());
    assert_eq!(stage.history, history);
    assert!(matches!(
        stage.selection().suffix(),
        ArchiveRestoreSuffixV3::Empty
    ));
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
    drop(stage);
    assert!(!scope.join("stage").exists());
}

// req: REC-001, REC-006, END-001
#[test]
fn native_archive_preparation_refuses_repair_and_cannot_recover_its_retired_source_proof() {
    use redb::ReadableDatabase;
    let scope = crate::test_path::ScopedDirectory::new("native-archive-preparation-repair");
    let (backup, _) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let repository = crate::RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(
            &scope.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let mut stage = materialize_native_archive(&scope, &backup)
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    assert!(stage.stage.native_authorization_owner.is_none());
    assert!(
        stage.stage.take_owned_authorization_evidence().is_err(),
        "a source proof consumed before conversion cannot authorize archive bytes"
    );
    let owner = stage.stage.native_archive_preparation.as_ref().unwrap();
    let database = owner.open_preparation_database().unwrap();
    let read = database.begin_read().unwrap();
    assert!(crate::follower_lifecycle::is_attached(&read).unwrap());
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&read).unwrap(),
        Some(stage.history)
    );
    drop(read);
    drop(database);
    let healthy = fs::read(stage.staged_database_file()).unwrap();
    force_staged_allocator_repair(stage.staged_database_file());
    let damaged = fs::read(stage.staged_database_file()).unwrap();
    let error = match owner.open_preparation_database() {
        Err(error) => error,
        Ok(_) => panic!("native repair must be refused"),
    };
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    assert_eq!(fs::read(stage.staged_database_file()).unwrap(), damaged);
    fs::write(stage.staged_database_file(), healthy).unwrap();
    assert!(
        owner.open_preparation_database().is_err(),
        "repair uncertainty must remain sticky"
    );
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
}

// req: REC-001, REC-006, END-001
#[test]
fn native_archive_preparation_refuses_identical_primary_replacement() {
    let scope = crate::test_path::ScopedDirectory::new("native-archive-identical-replacement");
    let (backup, _) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let repository = crate::RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(
            &scope.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let stage = materialize_native_archive(&scope, &backup)
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let path = stage.staged_database_file();
    let bytes = fs::read(path).unwrap();
    fs::rename(path, scope.join("retained-primary.redb")).unwrap();
    fs::write(path, &bytes).unwrap();
    let owner = stage.stage.native_archive_preparation.as_ref().unwrap();
    assert!(owner.open_preparation_database().is_err());
    assert!(owner.verify().is_err());
    assert_eq!(fs::read(path).unwrap(), bytes);
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
}

// req: REC-001, REC-006, END-001, REP-007, AFC-007
#[test]
fn native_archive_replay_uses_original_follower_join_and_applies_every_selected_frame() {
    let scope = crate::test_path::ScopedDirectory::new("native-archive-frame-replay");
    let (backup, history) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let (repository, last) = crate::maintenance::staged::archive_tests::archive(&scope, &backup);
    let stage = materialize_native_archive(&scope, &backup)
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let applier = stage
        .stage
        .open_archive_follower(inputs(), &AtomicBool::new(false))
        .expect("native archive replay must preserve the original follower proof join");
    assert_eq!(applier.durable_history().unwrap(), history);
    crate::maintenance::probe_restore_custody(stage.staged_database_file(), true);
    let replayed = stage.replay(applier, &AtomicBool::new(false)).unwrap();
    assert_eq!(replayed.history().tail(), last);
    crate::maintenance::probe_restore_custody(replayed.staged_database_file(), true);
    let database = replayed
        .stage
        .open_archive_preparation_read(&replayed.file)
        .unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&read).unwrap(),
        Some(replayed.history())
    );
    assert!(crate::follower_lifecycle::is_attached(&read).unwrap());
    drop(read);
    drop(database);
    assert!(!crate::journal::journal_path(replayed.staged_database_file()).exists());
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
    replayed.discard().unwrap();
    assert!(!scope.join("stage").exists());
}

// req: REC-001, REC-006, END-001
#[test]
fn native_archive_replay_cancellation_precedes_the_follower_engine_open() {
    let scope = crate::test_path::ScopedDirectory::new("native-archive-replay-cancel");
    let (backup, _) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let repository = crate::RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(
            &scope.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let stage = materialize_native_archive(&scope, &backup)
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let before = fs::read(stage.staged_database_file()).unwrap();
    assert!(
        stage
            .stage
            .open_archive_follower(inputs(), &AtomicBool::new(true))
            .is_err()
    );
    assert_eq!(fs::read(stage.staged_database_file()).unwrap(), before);
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
    drop(stage);
    assert!(!scope.join("stage").exists());
}

// req: REC-001, REC-006, END-001
#[test]
fn native_archive_replay_checks_the_original_namespace_before_opening_an_engine() {
    let scope = crate::test_path::ScopedDirectory::new("native-archive-replay-marker");
    let (backup, _) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let repository = crate::RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(
            &scope.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let stage = materialize_native_archive(&scope, &backup)
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let marker = crate::durable_format_marker_path(stage.staged_database_file());
    let retained_marker = fs::read(&marker).unwrap();
    let primary = fs::read(stage.staged_database_file()).unwrap();
    fs::write(&marker, b"invalid private marker").unwrap();
    assert!(
        stage
            .stage
            .open_archive_follower(inputs(), &AtomicBool::new(false))
            .is_err()
    );
    assert_eq!(fs::read(stage.staged_database_file()).unwrap(), primary);
    fs::write(&marker, retained_marker).unwrap();
    assert!(
        stage
            .stage
            .open_archive_follower(inputs(), &AtomicBool::new(false))
            .is_err(),
        "a failed preparation open invalidates its owner"
    );
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
}

fn replay_complete_native_fixture(
    scope: &crate::test_path::ScopedDirectory,
    backup: &Path,
) -> RedbReplayedArchiveRestore {
    let (repository, _) = crate::maintenance::staged::archive_tests::archive(scope, backup);
    let stage = materialize_native_archive(scope, backup)
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let applier = stage
        .stage
        .open_archive_follower(inputs(), &AtomicBool::new(false))
        .unwrap();
    stage.replay(applier, &AtomicBool::new(false)).unwrap()
}

// req: REC-001, REC-006, END-001, REP-007, AFC-007
#[test]
fn native_archive_complete_follower_validation_retains_the_actual_physical_and_logical_owner() {
    let scope = crate::test_path::ScopedDirectory::new("native-archive-complete-proof");
    let (backup, _) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let mut replayed = replay_complete_native_fixture(&scope, &backup);
    let path = replayed.staged_database_file().to_path_buf();
    let frontier = replayed.history().tail().frontier();
    let owner = replayed.stage.native_archive_preparation.take().unwrap();
    let complete = owner.validate_complete_follower(&path, inputs(),
        std::sync::Arc::new(AtomicBool::new(false)))
        .expect("native archive must reach complete physical and exact-end logical validation on its owned artifact");
    let (owner, logical) = complete.into_parts_for_test();
    assert_eq!(logical.native_authorization_inputs().unwrap(), inputs());
    let shared = logical.native_owner_for_test();
    let parts = logical.into_authorization_parts().unwrap();
    assert_eq!(parts.0, replayed.history().lineage().database_id());
    assert_eq!(
        parts.4.application_frontier().unwrap(),
        frontier.application()
    );
    let expected_administration =
        frontier
            .administration()
            .map_or(AdministrationSequenceAllocator::initial(), |sequence| {
                sequence.checked_next().map_or(
                    AdministrationSequenceAllocator::Exhausted,
                    AdministrationSequenceAllocator::Next,
                )
            });
    assert_eq!(parts.2.administration_sequence(), expected_administration);
    crate::maintenance::probe_restore_custody(&path, true);
    drop(parts);
    drop(shared);
    crate::maintenance::probe_restore_custody(&path, true);
    drop(owner);
    crate::maintenance::probe_restore_custody(&path, false);
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
    replayed.discard().unwrap();
}

// req: REC-001, REC-006, END-001
#[test]
fn native_archive_complete_follower_validation_refuses_a_raw_engine_commit() {
    use redb::ReadableTable;
    let scope = crate::test_path::ScopedDirectory::new("native-archive-proof-raw-commit");
    let (backup, _) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let mut replayed = replay_complete_native_fixture(&scope, &backup);
    let path = replayed.staged_database_file().to_path_buf();
    let owner = replayed.stage.native_archive_preparation.take().unwrap();
    let complete = owner
        .validate_complete_follower(&path, inputs(), std::sync::Arc::new(AtomicBool::new(false)))
        .unwrap();
    let (owner, logical) = complete.into_parts_for_test();
    assert_eq!(logical.native_authorization_inputs().unwrap(), inputs());
    let shared = logical.native_owner_for_test();
    let epoch = shared.durable_commit_epoch();
    let write = shared.database.begin_write().unwrap();
    {
        let mut meta = write.open_table(crate::layout::META).unwrap();
        let key = AuthoritativeNamespaceV1::RetentionWatermark
            .metadata_key()
            .unwrap();
        let value = meta.get(key).unwrap().unwrap().value().to_vec();
        meta.insert(key, value.as_slice()).unwrap();
    }
    write.commit().unwrap();
    assert_eq!(
        shared.durable_commit_epoch(),
        epoch,
        "the raw native commit bypasses the facade epoch"
    );
    assert!(logical.native_authorization_inputs().is_err());
    assert!(
        logical.into_authorization_parts().is_err(),
        "fresh native bytes cannot reuse the prior complete archive proof"
    );
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
    drop(shared);
    drop(owner);
    replayed.discard().unwrap();
}

// req: REC-001, REC-006, END-001
#[test]
fn native_archive_complete_follower_cancellation_precedes_any_verification_engine_open() {
    let scope = crate::test_path::ScopedDirectory::new("native-archive-complete-cancel");
    let (backup, _) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let mut replayed = replay_complete_native_fixture(&scope, &backup);
    let path = replayed.staged_database_file().to_path_buf();
    let before = fs::read(&path).unwrap();
    let owner = replayed.stage.native_archive_preparation.take().unwrap();
    let error = owner
        .validate_complete_follower(&path, inputs(), std::sync::Arc::new(AtomicBool::new(true)))
        .err()
        .expect("canceled complete validation");
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
    replayed.discard().unwrap();
}

fn prepare_native_authorization_fixture(
    scope: &crate::test_path::ScopedDirectory,
    backup: &Path,
    cancellation: std::sync::Arc<AtomicBool>,
) -> (PathBuf, super::preparation::RedbPreparedArchiveRestore) {
    let (repository, _) = crate::maintenance::staged::archive_tests::archive(scope, backup);
    let stage = materialize_native_archive(scope, backup)
        .begin_archive_replay(repository, &cancellation)
        .unwrap();
    let path = stage.staged_database_file().to_path_buf();
    let prepared = stage
        .prepare_restore(
            riffdb_types::ArchiveRestoreStopV1::LastArchived,
            inputs(),
            cancellation,
        )
        .unwrap();
    (path, prepared)
}

// req: REC-001, REC-006, END-001, REP-007, AFC-007
#[test]
fn native_archive_authorization_uses_the_complete_catalog_session_and_retains_custody_through_close()
 {
    let scope = crate::test_path::ScopedDirectory::new("native-archive-auth-complete");
    let (backup, history) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let (path, mut prepared) = prepare_native_authorization_fixture(
        &scope,
        &backup,
        std::sync::Arc::new(AtomicBool::new(false)),
    );
    let evidence = prepared.take_owned_authorization_evidence().unwrap();
    assert!(evidence.matches_inputs(&inputs()));
    let (database, session, retained, catalog, reader) = evidence.into_parts().unwrap();
    assert!(catalog.matches(database, session));
    assert_eq!(retained.database_id(), database);
    assert_eq!(
        reader.application_frontier().unwrap(),
        prepared.restored_frontier().application()
    );
    crate::maintenance::probe_restore_custody(&path, true);
    drop(reader);
    let sealed = prepared.seal_after_authorization(database).unwrap();
    crate::maintenance::probe_restore_custody(&path, true);
    assert_eq!(sealed.restored_frontier(), history.tail().frontier());
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
    drop(sealed);
    assert!(!scope.join("stage").exists());
}

// req: REC-001, REC-006, END-001
#[test]
fn native_archive_authorization_refuses_a_surviving_immutable_reader_before_close() {
    let scope = crate::test_path::ScopedDirectory::new("native-archive-auth-reader-clone");
    let (backup, _) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let (_, prepared) = prepare_native_authorization_fixture(
        &scope,
        &backup,
        std::sync::Arc::new(AtomicBool::new(false)),
    );
    let reader = prepared.authorization_snapshot().unwrap();
    let database = prepared.selection().lineage().database_id();
    let error = match prepared.seal_after_authorization(database) {
        Err(error) => error,
        Ok(_) => panic!("every reader must close before the native engine"),
    };
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert!(
        reader.application_frontier().is_err(),
        "failed close invalidates every surviving reader"
    );
    drop(reader);
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    assert!(!scope.join("configured.redb").exists());
}

// req: REC-001, REC-006, END-001
#[test]
fn native_archive_authorization_refuses_cancellation_and_changed_sealed_artifacts_before_authentication()
 {
    for artifact in ["cancellation", "primary", "journal", "marker"] {
        let scope = crate::test_path::ScopedDirectory::new("native-archive-auth-seal");
        let (backup, _) = crate::maintenance::archive_backup_tests::backup_current_source(&scope);
        let original = crate::backup::validate_immutable_backup(&backup).unwrap();
        let cancellation = std::sync::Arc::new(AtomicBool::new(false));
        let (path, mut prepared) =
            prepare_native_authorization_fixture(&scope, &backup, cancellation.clone());
        let marker = crate::durable_format_marker_path(&path);
        let original_primary_length = fs::metadata(&path).unwrap().len();
        let original_marker_length = fs::metadata(&marker).unwrap().len();
        let expected = if artifact == "cancellation" {
            cancellation.store(true, std::sync::atomic::Ordering::Release);
            StorageErrorKind::Unavailable
        } else {
            let changed = match artifact {
                "primary" => path.clone(),
                "journal" => crate::journal::journal_path(&path),
                _ => crate::durable_format_marker_path(&path),
            };
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&changed)
                .unwrap();
            std::io::Write::write_all(&mut file, b"changed-before-archive-authentication").unwrap();
            file.sync_all().unwrap();
            StorageErrorKind::CorruptData
        };
        let error = prepared
            .take_owned_authorization_evidence()
            .err()
            .expect("changed or cancelled proof must never reach bearer authentication");
        assert_eq!(error.kind(), expected);
        match artifact {
            "cancellation" => cancellation.store(false, std::sync::atomic::Ordering::Release),
            "journal" => fs::remove_file(crate::journal::journal_path(&path)).unwrap(),
            "primary" => fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(original_primary_length)
                .unwrap(),
            _ => fs::OpenOptions::new()
                .write(true)
                .open(&marker)
                .unwrap()
                .set_len(original_marker_length)
                .unwrap(),
        }
        assert!(
            prepared.take_owned_authorization_evidence().is_err(),
            "restoring bytes or clearing cancellation cannot revive a witnessed failed native proof"
        );
        drop(prepared);
        assert_eq!(
            crate::backup::validate_immutable_backup(&backup).unwrap(),
            original
        );
        assert!(!scope.join("configured.redb").exists());
    }
}
