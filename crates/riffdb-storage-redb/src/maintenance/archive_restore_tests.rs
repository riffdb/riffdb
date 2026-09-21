//! Private full-backup replay; final authorization/publication are separate gates.
// req: REP-007, AFC-007
use super::*;
use riffdb_storage_api::*;
use riffdb_types::{DigestKeyId, Timestamp};
use std::sync::atomic::AtomicBool;

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}

fn materialize(
    scope: &crate::test_path::ScopedDirectory,
    backup: &Path,
    name: &str,
) -> RedbStagedRestore {
    materialize_at(&scope.join(""), backup, name)
}

fn materialize_at(root: &Path, backup: &Path, name: &str) -> RedbStagedRestore {
    RedbStagedRestore::materialize(
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(
            1_700_000_000_000,
            [0x79; 10],
        )
        .unwrap(),
        BackupNameV1::new("baseline").unwrap(),
        backup.to_path_buf(),
        root.join(name),
        root.join("configured.redb"),
        inputs(),
        None,
    )
    .unwrap()
}

fn authority(path: &Path, attached: bool) -> Vec<AuthoritativeStateRowV3> {
    use redb::ReadableDatabase;
    use std::sync::Arc;
    let database = redb::ReadOnlyDatabase::open(path).unwrap();
    let read = database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    let root = Arc::new(crate::checkpoint_root::CheckpointRoot::new(read, 0));
    let mut cursor = if attached {
        crate::changelog_v3_cursor::state::open_attached(root, history).unwrap()
    } else {
        crate::changelog_v3_cursor::state::open(&crate::store::RedbReadAccess::Current(root))
            .unwrap()
    };
    let mut rows = vec![];
    while let Some(step) = cursor.next_item().unwrap() {
        if let AuthoritativeStateStepV3::Row(row) = step {
            rows.push(row);
        }
    }
    rows
}

fn validated_applier(path: &Path) -> crate::RedbFollowerApplier {
    crate::startup::open_validated_follower_fixture(path, inputs())
}

fn archive(
    scope: &crate::test_path::ScopedDirectory,
    backup: &Path,
) -> (crate::RedbArchiveRepository, ChangelogHistoryPointV3) {
    let binding = crate::RedbVerifiedArchiveBackup::open(backup).unwrap();
    let history = binding.history();
    let repository = binding
        .open_archive(
            &scope.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let mut consumer = ArchiveConsumerV1::new(repository, history.lineage(), history.tail());
    let encoded = proto_codec::encode_retention_watermark_v1(
        &StoredRetentionWatermarkV1::new(0, 1, None).unwrap(),
    )
    .unwrap();
    let mut before = history.tail();
    for mutations in [
        vec![
            AuthoritativeMutationV3::put(
                AuthoritativeNamespaceV1::RetentionWatermark,
                AuthoritativeNamespaceV1::RetentionWatermark
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                None,
                encoded.as_bytes(),
            )
            .unwrap(),
        ],
        vec![],
    ] {
        let receipt = AuthoritativeTransactionV3::new(
            AuthoritativeTransactionBindingV3 {
                database_id: history.lineage().database_id(),
                history_incarnation: 1,
                predecessor: Some(before.sequence()),
                sequence: before.sequence().checked_next().unwrap(),
                predecessor_frontier: before.frontier(),
                covered_frontier: before.frontier(),
                prior_history_hash: before.history_hash(),
            },
            ChangelogAttributionV3::RetentionPrune,
            mutations,
        )
        .unwrap();
        let frame = ChangelogFrameV3::new(
            ChangelogFrameBindingV3::new(
                history.lineage().database_id(),
                1,
                1,
                history.lineage().catalog_digest(),
                before.history_hash(),
            )
            .unwrap(),
            vec![receipt],
        )
        .unwrap()
        .encode()
        .unwrap();
        consumer.begin_stream().unwrap();
        before = consumer.append(frame).unwrap();
    }
    (consumer.into_sink(), before)
}

#[test]
fn archive_restore_replays_current_v2_source_without_transferring_primary_admission() {
    let scope = crate::test_path::ScopedDirectory::new("archive-replay-v2");
    let (backup, original) =
        crate::maintenance::archive_backup_tests::backup_current_source(&scope);
    let (repository, last) = archive(&scope, &backup);
    let stage = materialize(&scope, &backup, "stage");
    let path = stage.staged_database_file().to_path_buf();
    let before = authority(&path, false);
    let stage = stage
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    assert_eq!(authority(&path, true), before);
    assert!(crate::RedbStore::open(&path).is_err());
    let applier = validated_applier(&path);
    assert_eq!(applier.durable_history().unwrap(), original);
    let replayed = stage.replay(applier, &AtomicBool::new(false)).unwrap();
    assert_eq!(replayed.history().tail(), last);
    assert_eq!(
        replayed.history().lineage().catalog_digest(),
        AuthoritativeStateCatalogV2.digest()
    );
    crate::startup::RedbOfflineIntegrityScrub::from_inputs(&path, inputs())
        .run_follower()
        .unwrap();
    replayed.discard().unwrap();
}

#[test]
fn archive_restore_replays_verified_full_backup_through_the_real_follower_applier() {
    let scope = crate::test_path::ScopedDirectory::new("archive-replay");
    let (backup, original) = crate::maintenance::archive_backup_tests::backup(&scope);
    let (repository, last) = archive(&scope, &backup);
    fs::write(scope.join("configured.redb"), b"current database untouched").unwrap();
    let stage = materialize(&scope, &backup, "stage");
    let path = stage.staged_database_file().to_path_buf();
    let before = authority(&path, false);
    let stage = stage
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    assert_eq!(
        authority(&path, true),
        before,
        "conversion preserves every replicated authoritative row"
    );
    assert!(crate::RedbStore::open(&path).is_err());
    let applier = validated_applier(stage.staged_database_file());
    assert_eq!(applier.durable_history().unwrap(), original);
    let replayed = stage.replay(applier, &AtomicBool::new(false)).unwrap();
    assert_eq!(replayed.history().tail(), last);
    let after = authority(&path, true);
    let watermark = AuthoritativeNamespaceV1::RetentionWatermark;
    assert_eq!(
        after
            .iter()
            .filter(|row| row.namespace() != watermark)
            .collect::<Vec<_>>(),
        before.iter().collect::<Vec<_>>()
    );
    assert_eq!(
        after
            .iter()
            .filter(|row| row.namespace() == watermark)
            .count(),
        1
    );
    crate::startup::RedbOfflineIntegrityScrub::from_inputs(
        replayed.staged_database_file(),
        inputs(),
    )
    .run_follower()
    .unwrap();
    assert_eq!(
        fs::read(scope.join("configured.redb")).unwrap(),
        b"current database untouched"
    );
    assert_eq!(
        crate::RedbVerifiedArchiveBackup::open(&backup)
            .unwrap()
            .history(),
        original
    );
    replayed.discard().unwrap();
    assert!(!path.exists());
}

#[test]
fn archive_restore_preflight_refuses_damaged_or_foreign_archives_without_changing_copied_bytes() {
    for mode in 0..3 {
        let scope = crate::test_path::ScopedDirectory::new("archive-replay-preflight");
        let (backup, history) = crate::maintenance::archive_backup_tests::backup(&scope);
        let (repository, _) = archive(&scope, &backup);
        let repository = if mode == 1 {
            drop(repository);
            crate::RedbArchiveRepository::open(
                &scope.join("foreign"),
                history.lineage(),
                history.tail(),
                [0x55; 32],
                ArchiveEncryptionPostureV1::Unencrypted,
            )
            .unwrap()
        } else {
            repository
        };
        let stage = materialize(&scope, &backup, "stage");
        let witness = scope.join("witness.redb");
        fs::hard_link(stage.staged_database_file(), &witness).unwrap();
        let before = sha256_file(&witness).unwrap();
        if mode == 0 {
            fs::write(
                scope.join("archive").join(format!(
                    "frame-{:016x}.v3",
                    history.tail().sequence().get() + 1
                )),
                b"truncated",
            )
            .unwrap();
        }
        assert!(
            stage
                .begin_archive_replay(repository, &AtomicBool::new(mode == 2))
                .is_err()
        );
        assert_eq!(sha256_file(&witness).unwrap(), before);
        assert!(!scope.join("stage").exists());
        assert!(!scope.join("configured.redb").exists());
    }
}

#[test]
fn archive_restore_rejects_an_applier_for_another_file_even_with_the_same_history() {
    let scope = crate::test_path::ScopedDirectory::new("archive-replay-wrong-owner");
    let (backup, history) = crate::maintenance::archive_backup_tests::backup(&scope);
    let (repository, _) = archive(&scope, &backup);
    let stage = materialize(&scope, &backup, "stage")
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let binding = crate::RedbVerifiedArchiveBackup::open(&backup).unwrap();
    let second_repository = binding
        .open_archive(
            &scope.join("second-archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let second = materialize(&scope, &backup, "second-stage")
        .begin_archive_replay(second_repository, &AtomicBool::new(false))
        .unwrap();
    let applier = validated_applier(second.staged_database_file());
    assert!(stage.replay(applier, &AtomicBool::new(false)).is_err());
    let unaffected = validated_applier(second.staged_database_file());
    assert_eq!(unaffected.durable_history().unwrap(), history);
    unaffected.close().unwrap();
    assert!(!scope.join("stage").exists());
}

#[test]
fn archive_restore_drops_a_partial_private_replay_on_later_corruption() {
    let scope = crate::test_path::ScopedDirectory::new("archive-replay-late-corruption");
    let (backup, _) = crate::maintenance::archive_backup_tests::backup(&scope);
    let (repository, last) = archive(&scope, &backup);
    let stage = materialize(&scope, &backup, "stage")
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let applier = validated_applier(stage.staged_database_file());
    fs::write(
        scope
            .join("archive")
            .join(format!("frame-{:016x}.v3", last.sequence().get())),
        b"corrupt second frame",
    )
    .unwrap();
    assert!(stage.replay(applier, &AtomicBool::new(false)).is_err());
    assert!(!scope.join("stage").exists());
    assert!(!scope.join("configured.redb").exists());
}

#[test]
fn archive_restore_crash_child() {
    let Some(root) = std::env::var_os("RIFFDB_ARCHIVE_REPLAY_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let backup = root.join("backup");
    let binding = crate::RedbVerifiedArchiveBackup::open(&backup).unwrap();
    let repository = binding
        .open_archive(
            &root.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let stage = materialize_at(root, &backup, "stage")
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let applier = validated_applier(stage.staged_database_file());
    let _replayed = stage.replay(applier, &AtomicBool::new(false)).unwrap();
    panic!("crash boundary did not fire");
}

#[test]
fn archive_restore_crashes_rebuild_the_private_stage_from_the_original_backup() {
    for edge in [
        "conversion-staged",
        "conversion-committed",
        "journal-removed",
        "frame-applied",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("archive-replay-crash");
        let (backup, _) = crate::maintenance::archive_backup_tests::backup(&scope);
        let (repository, last) = archive(&scope, &backup);
        drop(repository);
        fs::write(scope.join("configured.redb"), b"untouched current").unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "maintenance::staged::archive_tests::archive_restore_crash_child",
                "--nocapture",
            ])
            .env("RIFFDB_ARCHIVE_REPLAY_ROOT", scope.join(""))
            .env("RIFFDB_ARCHIVE_REPLAY_CRASH", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(98), "{edge}");
        assert_eq!(
            fs::read(scope.join("configured.redb")).unwrap(),
            b"untouched current"
        );
        let binding = crate::RedbVerifiedArchiveBackup::open(&backup).unwrap();
        let repository = binding
            .open_archive(
                &scope.join("archive"),
                ArchiveEncryptionPostureV1::Unencrypted,
            )
            .unwrap();
        let stage = materialize(&scope, &backup, "stage")
            .begin_archive_replay(repository, &AtomicBool::new(false))
            .unwrap();
        let applier = validated_applier(stage.staged_database_file());
        let result = stage.replay(applier, &AtomicBool::new(false)).unwrap();
        assert_eq!(result.history().tail(), last);
        crate::startup::RedbOfflineIntegrityScrub::from_inputs(
            result.staged_database_file(),
            inputs(),
        )
        .run_follower()
        .unwrap();
        result.discard().unwrap();
    }
}

#[test]
fn archive_restore_retries_replay_the_frozen_prefix_after_archive_advances() {
    for empty in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("archive-replay-frozen");
        let (backup, original) = crate::maintenance::archive_backup_tests::backup(&scope);
        let (repository, last) = archive(&scope, &backup);
        let first = repository.frames().next().unwrap().unwrap().0;
        assert_ne!(first.covered(), last);
        let current = fs::read(scope.join("archive/CURRENT")).unwrap();
        let stage = materialize(&scope, &backup, "stage");
        let selection = ArchiveRestoreSelectionV3::new(
            stage.manifest_identity().clone(),
            original.lineage(),
            original.tail(),
            if empty {
                ArchiveRestoreSuffixV3::Empty
            } else {
                ArchiveRestoreSuffixV3::Terminal(Box::new(first))
            },
        )
        .unwrap();
        let expected = if empty {
            original.tail()
        } else {
            first.covered()
        };
        let stage = stage
            .begin_selected_archive_replay(repository, selection.clone(), &AtomicBool::new(false))
            .unwrap();
        let applier = validated_applier(stage.staged_database_file());
        let replayed = stage.replay(applier, &AtomicBool::new(false)).unwrap();
        assert_eq!(replayed.history().tail(), expected);
        assert_eq!(replayed.selection(), &selection);
        crate::startup::RedbOfflineIntegrityScrub::from_inputs(
            replayed.staged_database_file(),
            inputs(),
        )
        .run_follower()
        .unwrap();
        assert_eq!(fs::read(scope.join("archive/CURRENT")).unwrap(), current);
        assert!(!scope.join("configured.redb").exists());
        replayed.discard().unwrap();
    }
}

#[path = "archive_publication_tests.rs"]
mod publication;

#[path = "archive_preparation_tests.rs"]
mod preparation;
