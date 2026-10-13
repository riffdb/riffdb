//! Actual successful startup, terminal receipt and intent removal crash cuts.
// req: REC-001, REC-004, REC-005, REC-006, REC-008
use super::*;
use std::os::unix::fs::MetadataExt;

#[test]
fn native_publication_success_cleanup_process_child() {
    let Some(root) = std::env::var_os("RIFFDB_TEST_NATIVE_CLEANUP_ROOT") else {
        return;
    };
    let selection: usize = std::env::var("RIFFDB_TEST_NATIVE_CLEANUP_CUT")
        .unwrap()
        .parse()
        .unwrap();
    assert!(selection < 8);
    let (mut storage, operation) = published(Path::new(&root));
    let session = storage
        .begin_published_restore_readiness(
            operation,
            inputs(),
            crate::RedbCommitProfile::Standard,
            Arc::new(NoChangelogPublicationPort),
        )
        .unwrap();
    let _retain_validated_engine = finish_native_session(session);
    let mut receipt = read_receipt_file(&storage.receipt_path(operation)).unwrap();
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::Succeeded,
        ))
        .unwrap();
    if selection < 5 {
        storage.test_controller = Some(RedbMaintenanceTestController::abort_at(
            CONSUMED_CUTS[selection],
        ));
    }
    storage.replace_receipt(&receipt).unwrap();
    if selection >= 5 {
        storage.test_controller = Some(RedbMaintenanceTestController::abort_at(
            CLEANUP_CUTS[selection - 5],
        ));
    }
    storage
        .complete_native_publication_cleanup(operation)
        .unwrap();
    panic!("successful native cleanup child must abort at its selected boundary");
}

fn abort_cleanup_child(root: &Path, selection: usize) {
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "maintenance::store::restored_readiness::native_transfer_tests::cleanup_tests::native_publication_success_cleanup_process_child", "--nocapture"])
        .env("RIFFDB_TEST_NATIVE_CLEANUP_ROOT", root)
        .env("RIFFDB_TEST_NATIVE_CLEANUP_CUT", selection.to_string())
        .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::inherit()).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("native cleanup child exceeded its deadline");
        }
        std::thread::yield_now();
    };
    assert_eq!(
        status.signal(),
        Some(6),
        "must observe the actual success/cleanup abort boundary"
    );
}

#[test]
fn native_publication_success_cleanup_process_cuts_preserve_the_actual_target_and_admission_fence()
{
    use riffdb_storage_api::StructuralEvidenceSession;
    for selection in 0..8 {
        let scope = ScopedDirectory::new("native-success-cleanup-crash");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        abort_cleanup_child(&root, selection);
        let storage = RedbMaintenanceStorage::acquire_owner(
            &root.join("configured.redb"),
            &root.join("backups"),
            None,
        )
        .unwrap();
        let backup = validate_immutable_backup(&root.join("backups/ordinary-transfer")).unwrap();
        let (receipts, _, _, _, _) = storage.read_complete_inventory_mode(true, false).unwrap();
        let receipt = receipts
            .receipts()
            .iter()
            .find(|r| r.operation_kind() == OfflineMaintenanceOperationKind::RestoreBackup)
            .unwrap();
        let operation = receipt.operation_id();
        let intent_path = storage
            .receipts_directory
            .join(format!("{operation}.publication-intent-v1"));
        assert_eq!(intent_path.exists(), selection < 6);
        let before = fs::metadata(&storage.database_file).unwrap();
        let identity = (before.dev(), before.ino());
        if selection < 3 {
            assert_eq!(
                receipt.current_phase(),
                OfflineMaintenanceReceiptPhaseV1::Validating
            );
            let bytes = fs::read(&intent_path).unwrap();
            let artifacts = [
                storage.database_file.clone(),
                crate::journal::journal_path(&storage.database_file),
                crate::durable_format_marker_path(&storage.database_file),
            ];
            let checksums: Vec<_> = artifacts
                .iter()
                .map(|p| crate::backup::sha256_file(p).unwrap())
                .collect();
            // These cuts have no terminal receipt. Cleanup must refuse and
            // preserve evidence; complete consumed recovery is a separate gate.
            assert!(
                storage
                    .complete_native_publication_cleanup(operation)
                    .is_err()
            );
            assert_eq!(fs::read(&intent_path).unwrap(), bytes);
            for (path, checksum) in artifacts.iter().zip(checksums) {
                assert_eq!(crate::backup::sha256_file(path).unwrap(), checksum);
            }
        } else {
            assert_eq!(
                receipt.current_phase(),
                OfflineMaintenanceReceiptPhaseV1::Succeeded
            );
            storage
                .complete_native_publication_cleanup(operation)
                .unwrap();
            storage
                .complete_native_publication_cleanup(operation)
                .unwrap();
            assert!(!intent_path.exists());
            // After terminal success, accepted ADR-0294 permits normal engine
            // recovery on the actual target. Fully validate it here as extra
            // evidence, without granting application admission in this test.
            let store = crate::RedbStore::open_with_changelog_publication_port(
                &storage.database_file,
                crate::RedbCommitProfile::Standard,
                Arc::new(NoChangelogPublicationPort),
            )
            .unwrap();
            let read = store.shared.database.begin_read().unwrap();
            let retained = crate::startup::read_retained_metadata_pub(&read).unwrap();
            assert_eq!(retained.database_id(), backup.0.database_id());
            assert_eq!(
                retained.history_incarnation(),
                receipt.published_history_incarnation().unwrap()
            );
            assert_eq!(
                crate::retention::durable_application_head(&read).unwrap(),
                backup.0.last_commit_sequence().map_or(0, |s| s.get())
            );
            drop(read);
            let session = store
                .begin_native_published_restore_recovery(inputs())
                .unwrap();
            assert_eq!(session.database_id(), backup.0.database_id());
            assert!(!session.clean_close_fast_path());
            assert!(!session.bounded_dirty_startup_path());
            assert!(!session.checkpoint_verified());
            drop(finish_native_session(session));
        }
        let after = fs::metadata(&storage.database_file).unwrap();
        assert_eq!(
            (after.dev(), after.ino()),
            identity,
            "cleanup can never republish the backup"
        );
        assert_eq!(
            validate_immutable_backup(&root.join("backups/ordinary-transfer")).unwrap(),
            backup
        );
    }
}
