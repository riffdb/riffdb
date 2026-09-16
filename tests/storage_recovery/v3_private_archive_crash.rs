//! Private construction crashes must restart from the unchanged archive selection.
// req: REP-007, REC-001
use super::*;

#[test]
fn private_archive_reconstruction_crash_child() {
    let Ok(root) = std::env::var("RIFFDB_PRIVATE_PREFIX_ROOT") else {
        return;
    };
    check_private_archive(RedbCommitProfile::Hardened, Some(PathBuf::from(root)));
    panic!("armed construction edge was not reached");
}

#[test]
fn private_archive_reconstruction_recovers_quarantined_marker() {
    crash_and_rebuild("prefix-quarantined");
}

#[test]
fn private_archive_reconstruction_recovers_uncommitted_cut() {
    crash_and_rebuild("prefix-staged");
}

#[test]
fn private_archive_reconstruction_recovers_committed_cut() {
    crash_and_rebuild("prefix-committed");
}

fn crash_and_rebuild(phase: &str) {
    let scope = ScratchScope::new("private-prefix-crash");
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("v3_command_receipts::private_archive_prefix::crash::private_archive_reconstruction_crash_child")
        .env("RIFFDB_PRIVATE_PREFIX_ROOT", scope.path())
        .env("RIFFDB_PRIVATE_PREFIX_CRASH", phase)
        .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
        .status().unwrap();
    assert_eq!(status.code(), Some(98), "{phase}");
    let target = scope.path().join("configured.redb");
    assert!(!target.exists());
    let backups = scope.path().join("backups");
    let backup = backups.join("baseline");
    let backup_bytes = std::fs::read(backup.join("database.redb")).unwrap();
    let archive = scope.path().join("archive");
    let id =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [2; 10]).unwrap();
    let old_candidate = backups
        .join(".maintenance")
        .join("staged")
        .join(id.to_string())
        .join("database.redb");
    assert!(old_candidate.exists());
    assert!(RedbStore::open(&old_candidate).is_err());
    assert!(riffdb_storage_redb::RedbFollowerStore::open(&old_candidate).is_err());
    let binding = RedbVerifiedArchiveBackup::open(&backup).unwrap();
    let history = binding.history();
    let repository = binding
        .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
        .unwrap();
    let selection = ArchiveRestoreSelectionV3::new(
        OfflineBackupManifestIdentityV1::new(
            BackupIntegrityChecksumV1::new(binding.manifest_digest().to_vec()).unwrap(),
            binding.manifest().database_id(),
            binding.manifest().last_commit_sequence(),
        ),
        history.lineage(),
        history.tail(),
        ArchiveRestoreSuffixV3::Terminal(Box::new(repository.head().unwrap())),
    )
    .unwrap();
    drop(repository);
    for _ in 0..2 {
        let repository = RedbVerifiedArchiveBackup::open(&backup)
            .unwrap()
            .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
            .unwrap();
        let (maintenance, _) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
        let stage = maintenance
            .stage_recovery_restore_candidate(
                id,
                &BackupNameV1::new("baseline").unwrap(),
                prefix_predecessors::inputs(),
            )
            .unwrap()
            .begin_selected_archive_replay(repository, selection.clone(), &AtomicBool::new(false))
            .unwrap();
        let applier = prefix_predecessors::open_follower(stage.staged_database_file());
        let candidate = stage
            .replay(applier, &AtomicBool::new(false))
            .unwrap()
            .reconstruct_inside_group(
                CommitSequence::new(2).unwrap(),
                prefix_predecessors::inputs(),
                &AtomicBool::new(false),
            )
            .unwrap();
        assert_eq!(candidate.selection(), &selection);
        assert_eq!(
            candidate.restored_frontier().application(),
            CommitSequence::new(2)
        );
        let database = redb::ReadOnlyDatabase::open(candidate.staged_database_file()).unwrap();
        let read = database.begin_read().unwrap();
        let entities = read
            .open_table(TableDefinition::<&[u8], &[u8]>::new(N::Entities.table()))
            .unwrap();
        let first = command_fixture_at(1);
        let expected = superseding_command_fixture_at(2, 1, &first);
        let row = entities
            .get(expected.target.key().as_bytes())
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_entity_record_v1(row.value()).unwrap().value(),
            expected.records.entities()[0].post_image()
        );
        drop(row);
        drop(entities);
        drop(read);
        drop(database);
        candidate.discard().unwrap();
        assert!(!target.exists());
        assert_eq!(
            std::fs::read(backup.join("database.redb")).unwrap(),
            backup_bytes
        );
    }
}
