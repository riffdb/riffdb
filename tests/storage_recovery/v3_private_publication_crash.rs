//! Interrupted publication reconstructs from receipt selection before retrying.
// req: REP-007, REC-001
use super::*;

#[test]
fn private_archive_publication_crash_child() {
    let Ok(root) = std::env::var("RIFFDB_PRIVATE_PUBLICATION_ROOT") else {
        return;
    };
    let native = std::env::var("RIFFDB_PRIVATE_PUBLICATION_NATIVE").is_ok_and(|value| value == "1");
    let profile = match std::env::var("RIFFDB_PRIVATE_PUBLICATION_PROFILE").as_deref() {
        Ok("standard") => RedbCommitProfile::Standard,
        Ok("hardened") | Err(_) => RedbCommitProfile::Hardened,
        Ok(_) => panic!("unknown crash fixture profile"),
    };
    check_private_archive_case(
        profile,
        Some(PathBuf::from(root)),
        Some(if native { "publish-owned" } else { "publish" }),
    );
    panic!("armed publication edge was not reached");
}

#[test]
fn private_archive_publication_recovers_after_incarnation_stamp() {
    crash_and_rebuild(
        "private-incarnation-stamped",
        false,
        RedbCommitProfile::Hardened,
    );
}

#[test]
fn private_archive_publication_recovers_after_journal_creation() {
    crash_and_rebuild(
        "private-journal-created",
        false,
        RedbCommitProfile::Hardened,
    );
}

#[test]
fn private_archive_publication_recovers_after_marker_restoration() {
    crash_and_rebuild(
        "private-marker-restored",
        false,
        RedbCommitProfile::Hardened,
    );
}

// req: REP-007, REC-001, REC-006, END-001, AFC-007
#[test]
fn native_private_archive_publication_recovers_after_incarnation_stamp_in_both_profiles() {
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        crash_and_rebuild("private-incarnation-stamped", true, profile);
    }
}

// req: REP-007, REC-001, REC-006, END-001, AFC-007
#[test]
fn native_private_archive_publication_recovers_after_journal_creation_in_both_profiles() {
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        crash_and_rebuild("private-journal-created", true, profile);
    }
}

// req: REP-007, REC-001, REC-006, END-001, AFC-007
#[test]
fn native_private_archive_publication_recovers_after_marker_restoration_in_both_profiles() {
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        crash_and_rebuild("private-marker-restored", true, profile);
    }
}

fn crash_and_rebuild(phase: &str, native: bool, profile: RedbCommitProfile) {
    let scope = ScratchScope::new("private-publication-crash");
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("v3_command_receipts::private_archive_prefix::publication::crash::private_archive_publication_crash_child")
        .env("RIFFDB_PRIVATE_PUBLICATION_ROOT", scope.path())
        .env("RIFFDB_PRIVATE_PREFIX_CRASH", phase)
        .env("RIFFDB_PRIVATE_PUBLICATION_NATIVE", if native { "1" } else { "0" })
        .env("RIFFDB_PRIVATE_PUBLICATION_PROFILE", match profile { RedbCommitProfile::Standard => "standard", RedbCommitProfile::Hardened => "hardened" })
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
        let candidate = stage
            .prepare_restore(
                ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(2).unwrap()),
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
            publish_prepared(candidate, &mut maintenance, &target, &command, "publish");
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
