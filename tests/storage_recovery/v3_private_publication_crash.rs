//! Interrupted publication reconstructs from receipt selection before retrying.
// req: REP-007, REC-001
use super::*;

#[test]
fn private_archive_publication_crash_child() {
    let Ok(root) = std::env::var("RIFFDB_PRIVATE_PUBLICATION_ROOT") else {
        return;
    };
    check_private_archive_case(
        RedbCommitProfile::Hardened,
        Some(PathBuf::from(root)),
        Some("publish"),
    );
    panic!("armed publication edge was not reached");
}

#[test]
fn private_archive_publication_recovers_after_incarnation_stamp() {
    crash_and_rebuild("private-incarnation-stamped");
}

#[test]
fn private_archive_publication_recovers_after_journal_creation() {
    crash_and_rebuild("private-journal-created");
}

#[test]
fn private_archive_publication_recovers_after_marker_restoration() {
    crash_and_rebuild("private-marker-restored");
}

fn crash_and_rebuild(phase: &str) {
    let scope = ScratchScope::new("private-publication-crash");
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("v3_command_receipts::private_archive_prefix::publication::crash::private_archive_publication_crash_child")
        .env("RIFFDB_PRIVATE_PUBLICATION_ROOT", scope.path())
        .env("RIFFDB_PRIVATE_PREFIX_CRASH", phase)
        .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
        .status().unwrap();
    assert_eq!(status.code(), Some(98), "{phase}");
    let target = scope.path().join("configured.redb");
    assert!(!target.exists());
    let backups = scope.path().join("backups");
    let backup = backups.join("baseline");
    let backup_bytes = std::fs::read(backup.join("database.redb")).unwrap();
    let id =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [2; 10]).unwrap();
    let (mut maintenance, _) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
    let receipt = maintenance.read_archive_receipt(id).unwrap().unwrap();
    assert_eq!(
        receipt.current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Offline
    );
    assert_eq!(receipt.published_history_incarnation(), Some(2));
    let selection = receipt.selection().unwrap().clone();
    let frontier = receipt.restored_frontier().unwrap();
    for attempt in 0..2 {
        let repository = RedbVerifiedArchiveBackup::open(&backup)
            .unwrap()
            .open_archive(
                &scope.path().join("archive"),
                ArchiveEncryptionPostureV1::Unencrypted,
            )
            .unwrap();
        let stage = maintenance
            .stage_archive_restore(id, prefix_predecessors::inputs())
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
            .unwrap()
            .validate(
                prefix_predecessors::inputs(),
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
        assert_eq!(candidate.selection(), &selection);
        assert_eq!(candidate.restored_frontier(), frontier);
        let prior = command_fixture_at(1);
        let command = superseding_command_fixture_at(2, 1, &prior);
        let snapshot = candidate.authorization_snapshot().unwrap();
        assert_eq!(
            snapshot.read_entity(&command.target).unwrap().as_ref(),
            command.records.entities()[0].live_post_image()
        );
        drop(snapshot);
        if attempt == 0 {
            candidate.discard().unwrap();
            assert!(!target.exists());
        } else {
            publish(candidate, &mut maintenance, &target, &command, "publish");
        }
        assert_eq!(
            std::fs::read(backup.join("database.redb")).unwrap(),
            backup_bytes
        );
        assert_eq!(
            maintenance
                .read_archive_receipt(id)
                .unwrap()
                .unwrap()
                .selection(),
            Some(&selection)
        );
    }
}
