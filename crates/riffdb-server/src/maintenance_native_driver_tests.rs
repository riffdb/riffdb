// Native restore must finish durable intent cleanup before returning startup.
// req: REC-001, REC-004, REC-005, REC-006, REC-008
use super::*;
use std::os::unix::fs::MetadataExt;

fn remove_target_artifacts(database: &std::path::Path) {
    let mut journal = database.as_os_str().to_owned();
    journal.push(".riffjournal");
    std::fs::remove_file(database).expect("fixture primary must exist before replacement");
    for path in [
        std::path::PathBuf::from(journal),
        riffdb_storage_redb::durable_format_marker_path(database),
    ] {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("fixture artifact cleanup failed: {error}"),
        }
        assert!(!path.exists(), "native replacement must be genuinely empty");
    }
    assert!(!database.exists());
}

#[test]
fn native_restore_driver_finishes_success_and_intent_cleanup_before_returning_startup() {
    let StagedRestoreFixture {
        root,
        database,
        mut storage,
        lifecycle,
        mut receipt,
        prepared,
    } = staged_restore_fixture_with_mode("native-empty", 0x75, 7, true, None);
    let stage = root
        .path()
        .join("backups/.maintenance/staged")
        .join(receipt.operation_id().to_string())
        .join("database.redb");
    let before = std::fs::metadata(&stage).expect("original staged primary");
    remove_target_artifacts(&database);
    let clocks = ProductionWallClocks::new();
    let recovery = MaintenanceRecoveryController::disabled();
    let dependencies = fixture_dependencies(&clocks, &recovery, None);
    let startup = run_restore(
        &mut storage,
        &lifecycle,
        &dependencies,
        &mut receipt,
        None,
        Some(prepared),
    )
    .expect("native restore must complete before returning startup");
    let after = std::fs::metadata(&database).expect("published primary");
    assert_eq!(
        (before.dev(), before.ino()),
        (after.dev(), after.ino()),
        "driver must transfer the original primary"
    );
    assert_eq!(startup.retained_metadata().history_incarnation(), 8);
    assert_eq!(
        receipt.current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Succeeded
    );
    let intent = root
        .path()
        .join("backups/.maintenance/receipts")
        .join(format!("{}.publication-intent-v1", receipt.operation_id()));
    assert!(
        !intent.exists(),
        "returned startup cannot precede durable intent cleanup"
    );
    drop(startup);
}

#[test]
fn native_restore_driver_cleanup_uncertainty_cannot_return_application_startup() {
    for (index, cut) in [
        riffdb_storage_redb::RedbMaintenanceFailpoint::BeforeNativeIntentRemoval,
        riffdb_storage_redb::RedbMaintenanceFailpoint::AfterNativeIntentRemoval,
        riffdb_storage_redb::RedbMaintenanceFailpoint::AfterNativeIntentDirectorySync,
    ]
    .into_iter()
    .enumerate()
    {
        let controller = riffdb_storage_redb::RedbMaintenanceTestController::return_at(cut);
        let StagedRestoreFixture {
            root,
            database,
            mut storage,
            lifecycle,
            mut receipt,
            prepared,
        } = staged_restore_fixture_with_mode(
            "native-cleanup-refusal",
            0x76 + index as u8,
            7,
            true,
            Some(controller),
        );
        remove_target_artifacts(&database);
        let clocks = ProductionWallClocks::new();
        let recovery = MaintenanceRecoveryController::disabled();
        let dependencies = fixture_dependencies(&clocks, &recovery, None);
        let result = run_restore(
            &mut storage,
            &lifecycle,
            &dependencies,
            &mut receipt,
            None,
            Some(prepared),
        );
        assert!(
            matches!(result, Err(DriverFault::ArtifactStorage(ref error))
            if error.kind() == StorageErrorKind::CommitStatusUnknown),
            "uncertain intent cleanup must return typed uncertainty, never an application startup capability"
        );
        assert_eq!(
            receipt.current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Succeeded,
            "cleanup uncertainty cannot erase durable success"
        );
        let intent = root
            .path()
            .join("backups/.maintenance/receipts")
            .join(format!("{}.publication-intent-v1", receipt.operation_id()));
        assert_eq!(intent.exists(), index == 0);
        assert_eq!(
            riffdb_storage_redb::read_history_incarnation(&database).unwrap(),
            Some(8)
        );
    }
}
