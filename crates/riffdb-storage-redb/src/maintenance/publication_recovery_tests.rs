//! Cold recovery checks use real native, authorized, stamped restore artifacts.
// req: REC-001, REC-004, REC-005, REC-006, REC-008
use super::*;
use crate::maintenance::staged::transfer_tests::prepared_transfer_fixture;
use crate::test_path::ScopedDirectory;
use std::os::unix::fs::MetadataExt;

fn positions(
    storage: &RedbMaintenanceStorage,
    operation: OfflineMaintenanceOperationId,
) -> ([PathBuf; 3], [PathBuf; 3], [PathBuf; 3]) {
    let finals = artifact_names(&storage.database_file).unwrap();
    let staged = artifact_names(Path::new(crate::backup::DATABASE_ARTIFACT_FILE_NAME)).unwrap();
    (
        staged.map(|n| storage.staged_directory.join(operation.to_string()).join(n)),
        finals.each_ref().map(|n| {
            storage.database_file.parent().unwrap().join(
                super::super::super::staged::target_temporary_name(n, operation),
            )
        }),
        finals.map(|n| storage.database_file.parent().unwrap().join(n)),
    )
}

fn move_prefix(paths: &([PathBuf; 3], [PathBuf; 3], [PathBuf; 3]), moves: usize) {
    for index in 0..moves.min(3) {
        fs::rename(&paths.0[index], &paths.1[index]).unwrap();
    }
    for index in 0..moves.saturating_sub(3) {
        fs::rename(&paths.1[index], &paths.2[index]).unwrap();
    }
}

#[test]
fn native_publication_recovery_resumes_every_partial_position_without_copy_or_new_incarnation() {
    for moves in 0..=6 {
        let scope = ScopedDirectory::new("native-recovery-positions");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        let (storage, configured, named, operation) = prepared_transfer_fixture(&root);
        let paths = positions(&storage, operation);
        let expected: Vec<_> = paths
            .0
            .iter()
            .map(|p| {
                let metadata = fs::metadata(p).unwrap();
                (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.len(),
                    crate::backup::sha256_file(p).unwrap(),
                )
            })
            .collect();
        let original_backup = validate_immutable_backup(&named).unwrap();
        move_prefix(&paths, moves);
        drop(storage);
        // Acquire a fresh maintenance owner; no source engine or stage object
        // survives. The old copy-only reconciler is deliberately fenced.
        let storage =
            RedbMaintenanceStorage::acquire_owner(&configured, &root.join("backups"), None)
                .unwrap();
        let recovered = storage
            .prepare_native_publication_recovery()
            .unwrap()
            .unwrap();
        assert_eq!(recovered.intent.incarnation, 2);
        let published = recovered.resume_publication(&storage).unwrap();
        assert_eq!(published.phase, PublicationPhase::Published);
        assert_eq!(published.incarnation, 2);
        assert_eq!(published.manifest, original_backup.1);
        for (index, (dev, inode, length, seal)) in expected.into_iter().enumerate() {
            let metadata = fs::metadata(&paths.2[index]).unwrap();
            assert_eq!(
                (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.len(),
                    metadata.nlink()
                ),
                (dev, inode, length, 1)
            );
            assert_eq!(crate::backup::sha256_file(&paths.2[index]).unwrap(), seal);
            assert!(!paths.0[index].exists());
            assert!(!paths.1[index].exists());
        }
        assert_eq!(validate_immutable_backup(&named).unwrap(), original_backup);
        let repeated = storage
            .prepare_native_publication_recovery()
            .unwrap()
            .unwrap()
            .resume_publication(&storage)
            .unwrap();
        assert!(repeated == published);
        assert_eq!(
            read_receipt_file(&storage.receipt_path(operation))
                .unwrap()
                .current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Offline
        );
    }
}

#[test]
fn native_publication_recovery_refuses_changed_or_ambiguous_ownership_before_any_cleanup() {
    for variant in 0..8 {
        let scope = ScopedDirectory::new("native-recovery-refusal");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        let (storage, _, _, operation) = prepared_transfer_fixture(&root);
        let paths = positions(&storage, operation);
        move_prefix(&paths, 1);
        match variant {
            0 => {
                let mut file = OpenOptions::new().write(true).open(&paths.1[0]).unwrap();
                file.seek(SeekFrom::Start(100)).unwrap();
                file.write_all(&[0x31]).unwrap();
            }
            1 => {
                fs::copy(&paths.1[0], &paths.0[0]).unwrap();
            }
            2 => {
                fs::remove_file(&paths.0[1]).unwrap();
            }
            3 => {
                fs::hard_link(&paths.1[0], root.join("alias")).unwrap();
            }
            4 => {
                fs::write(paths.0[0].parent().unwrap().join("foreign"), b"foreign").unwrap();
            }
            5 => {
                fs::rename(&paths.0[2], &paths.2[2]).unwrap();
            }
            6 => {
                fs::rename(&paths.0[1], root.join("original-journal")).unwrap();
                std::os::unix::fs::symlink(root.join("original-journal"), &paths.0[1]).unwrap();
            }
            _ => {
                let len = fs::metadata(&paths.0[2]).unwrap().len();
                OpenOptions::new()
                    .write(true)
                    .open(&paths.0[2])
                    .unwrap()
                    .set_len(len + 1)
                    .unwrap();
            }
        }
        let ordinary = storage
            .receipts_directory
            .join(format!(".{operation}.receipt-v1.tmp"));
        fs::write(&ordinary, b"ordinary failure evidence").unwrap();
        assert!(storage.prepare_native_publication_recovery().is_err());
        assert_eq!(fs::read(&ordinary).unwrap(), b"ordinary failure evidence");
        assert!(
            paths.1[0].exists(),
            "failed recovery cannot erase a moved primary"
        );
        assert!(
            !paths.2[0].exists(),
            "refusal cannot publish or open the primary"
        );
        assert_eq!(
            IntentInventory::read(&storage.receipts_directory_guard)
                .unwrap()
                .current
                .unwrap()
                .phase,
            PublicationPhase::Prepared
        );
    }
}

#[test]
fn native_publication_recovery_invalidates_retained_authority_after_a_write() {
    let scope = ScopedDirectory::new("native-recovery-retained-write");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (storage, _, _, operation) = prepared_transfer_fixture(&root);
    let paths = positions(&storage, operation);
    let recovered = storage
        .prepare_native_publication_recovery()
        .unwrap()
        .unwrap();
    let mut writer = OpenOptions::new().write(true).open(&paths.0[1]).unwrap();
    writer.seek(SeekFrom::Start(0)).unwrap();
    writer.write_all(&[0x41]).unwrap();
    writer.sync_all().unwrap();
    assert!(recovered.resume_publication(&storage).is_err());
    assert!(paths.0[0].exists());
    assert!(!paths.2[0].exists());
}

#[test]
fn native_publication_recovery_rechecks_positions_before_cleaning_receipts() {
    for foreign_stage_entry in [false, true] {
        let scope = ScopedDirectory::new("native-recovery-retained-position");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        let (storage, _, _, operation) = prepared_transfer_fixture(&root);
        let paths = positions(&storage, operation);
        let recovered = storage
            .prepare_native_publication_recovery()
            .unwrap()
            .unwrap();
        if foreign_stage_entry {
            fs::write(
                paths.0[0].parent().unwrap().join("foreign"),
                b"foreign ownership evidence",
            )
            .unwrap();
        } else {
            fs::copy(&paths.0[0], &paths.1[0]).unwrap();
        }
        let ordinary = storage
            .receipts_directory
            .join(format!(".{operation}.receipt-v1.tmp"));
        fs::write(&ordinary, b"must preserve before position refusal").unwrap();
        let result = recovered.resume_publication(&storage);
        assert!(
            ordinary.exists(),
            "retained recovery must recheck all positions before any cleanup"
        );
        assert!(result.is_err());
        assert!(paths.0[0].exists());
        assert!(!paths.2[0].exists());
    }
}

const MOVE_CUTS: [RedbMaintenanceFailpoint; 19] = [
    RedbMaintenanceFailpoint::NativeTransferPrimaryRename,
    RedbMaintenanceFailpoint::NativeTransferPrimarySourceSync,
    RedbMaintenanceFailpoint::NativeTransferPrimaryTargetSync,
    RedbMaintenanceFailpoint::NativeTransferJournalRename,
    RedbMaintenanceFailpoint::NativeTransferJournalSourceSync,
    RedbMaintenanceFailpoint::NativeTransferJournalTargetSync,
    RedbMaintenanceFailpoint::NativeTransferFormatMarkerRename,
    RedbMaintenanceFailpoint::NativeTransferFormatMarkerSourceSync,
    RedbMaintenanceFailpoint::NativeTransferFormatMarkerTargetSync,
    RedbMaintenanceFailpoint::NativeTransferReadback,
    RedbMaintenanceFailpoint::NativePublishPrimaryRename,
    RedbMaintenanceFailpoint::NativePublishPrimarySourceSync,
    RedbMaintenanceFailpoint::NativePublishPrimaryTargetSync,
    RedbMaintenanceFailpoint::NativePublishJournalRename,
    RedbMaintenanceFailpoint::NativePublishJournalSourceSync,
    RedbMaintenanceFailpoint::NativePublishJournalTargetSync,
    RedbMaintenanceFailpoint::NativePublishFormatMarkerRename,
    RedbMaintenanceFailpoint::NativePublishFormatMarkerSourceSync,
    RedbMaintenanceFailpoint::NativePublishFormatMarkerTargetSync,
];
const PUBLISHED_INTENT_CUTS: [RedbMaintenanceFailpoint; 5] = [
    RedbMaintenanceFailpoint::BeforeReceiptFileSync,
    RedbMaintenanceFailpoint::AfterReceiptFileSync,
    RedbMaintenanceFailpoint::BeforeReceiptRename,
    RedbMaintenanceFailpoint::AfterReceiptRename,
    RedbMaintenanceFailpoint::AfterReceiptParentSync,
];

#[test]
fn native_publication_recovery_process_child() {
    use crate::maintenance::staged::transfer_tests::{
        arm_native_transfer_fixture, native_transfer_fixture,
    };
    let Some(root) = std::env::var_os("RIFFDB_TEST_NATIVE_TRANSFER_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let mode: usize = std::env::var("RIFFDB_TEST_NATIVE_TRANSFER_MODE")
        .unwrap()
        .parse()
        .unwrap();
    let selection: usize = std::env::var("RIFFDB_TEST_NATIVE_TRANSFER_CUT")
        .unwrap()
        .parse()
        .unwrap();
    assert!(mode < 3);
    let cut = if mode == 2 {
        PUBLISHED_INTENT_CUTS[selection]
    } else {
        MOVE_CUTS[selection]
    };
    let controller = RedbMaintenanceTestController::abort_at(cut);
    if mode == 0 {
        let mut fixture = native_transfer_fixture(root);
        let paths = positions(&fixture.storage, fixture.operation);
        write_original_identities(root, &paths.0);
        fixture.storage.test_controller = Some(controller.clone());
        arm_native_transfer_fixture(&mut fixture, controller);
        fixture
            .storage
            .publish_native_transfer_for_qualification(fixture.sealed)
            .unwrap();
    } else {
        let (mut storage, _, _, operation) = prepared_transfer_fixture(root);
        let paths = positions(&storage, operation);
        write_original_identities(root, &paths.0);
        if mode == 2 {
            move_prefix(&paths, 3);
        }
        storage.test_controller = Some(controller);
        storage
            .prepare_native_publication_recovery()
            .unwrap()
            .unwrap()
            .resume_publication(&storage)
            .unwrap();
    }
    panic!("native publication child must abort at the selected boundary");
}

fn write_original_identities(root: &Path, paths: &[PathBuf; 3]) {
    let mut expected = File::create(root.join("expected-original-identities")).unwrap();
    for path in paths {
        let metadata = fs::metadata(path).unwrap();
        writeln!(
            expected,
            "{} {} {}",
            metadata.dev(),
            metadata.ino(),
            metadata.len()
        )
        .unwrap();
    }
    expected.sync_all().unwrap();
}

fn run_child(root: &Path, mode: usize, selection: usize) {
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "maintenance::store::native_recovery::tests::native_publication_recovery_process_child",
            "--nocapture",
        ])
        .env("RIFFDB_TEST_NATIVE_TRANSFER_ROOT", root)
        .env("RIFFDB_TEST_NATIVE_TRANSFER_MODE", mode.to_string())
        .env("RIFFDB_TEST_NATIVE_TRANSFER_CUT", selection.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("native transfer crash child exceeded its deadline");
        }
        std::thread::yield_now();
    };
    assert_eq!(
        status.signal(),
        Some(6),
        "must observe the actual native abort boundary"
    );
}

#[test]
fn native_publication_recovery_process_cuts_keep_original_artifacts_and_same_operation() {
    for mode in 0..3 {
        let count = if mode == 2 {
            PUBLISHED_INTENT_CUTS.len()
        } else {
            MOVE_CUTS.len()
        };
        for selection in 0..count {
            let scope = ScopedDirectory::new("native-transfer-crash-recovery");
            let root = scope.join("fixture");
            fs::create_dir(&root).unwrap();
            run_child(&root, mode, selection);
            let configured = root.join("configured.redb");
            let storage =
                RedbMaintenanceStorage::acquire_owner(&configured, &root.join("backups"), None)
                    .unwrap();
            let recovered = storage
                .prepare_native_publication_recovery()
                .unwrap()
                .unwrap();
            let operation = recovered.intent.operation_id;
            let published = recovered.resume_publication(&storage).unwrap();
            assert_eq!(published.phase, PublicationPhase::Published);
            assert_eq!(published.incarnation, 2);
            let paths = positions(&storage, operation);
            let observed = paths
                .2
                .iter()
                .map(|p| {
                    let m = fs::metadata(p).unwrap();
                    assert_eq!(m.nlink(), 1);
                    format!("{} {} {}\n", m.dev(), m.ino(), m.len())
                })
                .collect::<String>();
            assert_eq!(
                observed,
                fs::read_to_string(root.join("expected-original-identities")).unwrap()
            );
            for index in 0..3 {
                assert!(!paths.0[index].exists());
                assert!(!paths.1[index].exists());
                assert_eq!(
                    crate::backup::sha256_file(&paths.2[index]).unwrap(),
                    published.artifacts[index].checksum
                );
            }
            assert_eq!(
                read_receipt_file(&storage.receipt_path(operation))
                    .unwrap()
                    .current_phase(),
                OfflineMaintenanceReceiptPhaseV1::Offline
            );
            assert_eq!(
                validate_immutable_backup(&root.join("backups/ordinary-transfer"))
                    .unwrap()
                    .1,
                published.manifest
            );
        }
    }
}

#[test]
fn native_publication_recovery_returns_polling_uncertainty_after_every_interrupted_move() {
    use crate::maintenance::staged::transfer_tests::{
        arm_native_transfer_fixture, native_transfer_fixture,
    };
    for cold in [false, true] {
        for cut in MOVE_CUTS {
            let scope = ScopedDirectory::new("native-transfer-return-recovery");
            let root = scope.join("fixture");
            fs::create_dir(&root).unwrap();
            let controller = RedbMaintenanceTestController::return_at(cut);
            let (storage, error) = if cold {
                let (mut storage, _, _, _) = prepared_transfer_fixture(&root);
                storage.test_controller = Some(controller);
                let error = storage
                    .prepare_native_publication_recovery()
                    .unwrap()
                    .unwrap()
                    .resume_publication(&storage)
                    .err()
                    .unwrap();
                (storage, error)
            } else {
                let mut fixture = native_transfer_fixture(&root);
                fixture.storage.test_controller = Some(controller.clone());
                arm_native_transfer_fixture(&mut fixture, controller);
                let error = fixture
                    .storage
                    .publish_native_transfer_for_qualification(fixture.sealed)
                    .unwrap_err();
                (fixture.storage, error)
            };
            assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
            let published = storage
                .prepare_native_publication_recovery()
                .unwrap()
                .unwrap()
                .resume_publication(&storage)
                .unwrap();
            assert_eq!(published.phase, PublicationPhase::Published);
            assert_eq!(published.incarnation, 2);
        }
    }
}

#[test]
fn native_publication_cold_recovery_refuses_corrupted_original_sealed_backup_history() {
    use crate::maintenance::staged::transfer_tests::native_transfer_fixture_with_history;
    use std::io::{Seek, SeekFrom, Write};
    for phase in [PublicationPhase::Prepared, PublicationPhase::Published] {
        let scope = ScopedDirectory::new("native-cold-original-history");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        let mut fixture = native_transfer_fixture_with_history(&root, true);
        let configured = fixture.configured.clone();
        let operation = fixture.operation;
        let named = fixture.named.clone();
        crate::maintenance::staged::transfer_tests::arm_native_transfer_fixture(
            &mut fixture,
            RedbMaintenanceTestController::return_at(
                RedbMaintenanceFailpoint::BeforeTargetPublication,
            ),
        );
        assert!(
            fixture
                .storage
                .publish_native_transfer_for_qualification(fixture.sealed)
                .is_err()
        );
        if phase == PublicationPhase::Published {
            fixture
                .storage
                .prepare_native_publication_recovery()
                .unwrap()
                .unwrap()
                .resume_publication(&fixture.storage)
                .unwrap();
        }
        let paths = positions(&fixture.storage, operation);
        let before: Vec<_> = paths
            .0
            .iter()
            .chain(&paths.1)
            .chain(&paths.2)
            .filter(|p| p.exists())
            .map(|p| (p.clone(), crate::backup::sha256_file(p).unwrap()))
            .collect();
        let extent = fs::read_dir(&named)
            .unwrap()
            .map(|r| r.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("journal.sealed.")
            })
            .unwrap();
        let mut file = fs::OpenOptions::new().write(true).open(&extent).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"invalid!").unwrap();
        file.sync_all().unwrap();
        drop(file);
        drop(fixture.storage);
        let storage =
            RedbMaintenanceStorage::acquire_owner(&configured, &root.join("backups"), None)
                .unwrap();
        assert!(
            storage.prepare_native_publication_recovery().is_err(),
            "cold native publication must revalidate original sealed backup history before any mutation"
        );
        for (path, checksum) in before {
            assert_eq!(crate::backup::sha256_file(&path).unwrap(), checksum);
        }
    }
}
