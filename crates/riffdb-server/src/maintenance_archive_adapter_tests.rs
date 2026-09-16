//! Real external receipt admission and exclusive trigger custody.
// req: REP-007, AFC-007
use super::*;
use riffdb_types::{
    ActorId, ActorKind, ArchiveNameV1, ArchiveRestoreStopV1, BackupNameV1, CapabilityId,
};

fn id(seed: u8) -> OfflineMaintenanceOperationId {
    OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [seed; 10]).unwrap()
}
fn candidate(
    seed: u8,
    archive: &str,
) -> (RestoreArchivedBackupRequest, OfflineMaintenanceReceiptV3) {
    let request = RestoreArchivedBackupRequest::new(
        id(seed),
        BackupNameV1::new("backup").unwrap(),
        ArchiveNameV1::new(archive).unwrap(),
        ArchiveRestoreStopV1::LastArchived,
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
    )
    .unwrap();
    let admission = OfflineMaintenanceAdmissionV1::new(
        ActorId::new("operator").unwrap(),
        ActorKind::Human,
        CapabilityId::from_unix_milliseconds_and_random(1, [2; 10]).unwrap(),
        None,
    );
    let receipt = OfflineMaintenanceReceiptV3::accepted_archive_restore(
        id(seed),
        request.backup_name().clone(),
        request.archive_name().clone(),
        request.stop(),
        request.input_hash(),
        request.confirmation(),
        admission,
        Some(DatabaseId::from_unix_milliseconds_and_random(1, [3; 10]).unwrap()),
    )
    .unwrap();
    (request, receipt)
}
fn controller() -> (
    tempfile::TempDir,
    MaintenanceController,
    mpsc::Receiver<MaintenanceTrigger>,
) {
    let dir = tempfile::tempdir().unwrap();
    let (storage, _) =
        RedbMaintenanceStorage::open(dir.path().join("database.redb"), dir.path().join("backups"))
            .unwrap();
    let (sender, receiver) = maintenance_trigger_channel();
    let controller = MaintenanceController::new(
        shared_maintenance_storage(storage),
        Arc::new(MaintenanceLifecycle::ready()),
        sender,
    );
    (dir, controller, receiver)
}
fn credential() -> riffdb_auth::RetainedOpaqueCredential {
    riffdb_auth::RetainedOpaqueCredential::new(b"private-credential").unwrap()
}

#[tokio::test]
async fn archive_admission_restart_reacquires_exact_receipt_once() {
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::Accepted,
        OfflineMaintenanceReceiptPhaseV1::Draining,
        OfflineMaintenanceReceiptPhaseV1::Offline,
    ] {
        let (_dir, controller, mut receiver) = controller();
        let (request, mut receipt) = candidate(8, "daily");
        controller
            .storage
            .lock()
            .unwrap()
            .create_or_read_archive_receipt(&receipt)
            .unwrap();
        for next in [
            OfflineMaintenanceReceiptPhaseV1::Draining,
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ] {
            if receipt.current_phase() == phase {
                break;
            }
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(next))
                .unwrap();
            controller
                .storage
                .lock()
                .unwrap()
                .replace_archive_receipt(&receipt)
                .unwrap();
        }
        controller
            .lifecycle
            .await_restore_retry(request.operation_id(), request.input_hash())
            .unwrap();
        let (changed, changed_candidate) = candidate(8, "changed");
        assert!(matches!(
            admit_candidate(&controller, changed, changed_candidate, credential()),
            Err(OfflineMaintenanceStartPortError::InputMismatch)
        ));
        assert!(receiver.try_recv().is_err());
        let result =
            admit_candidate(&controller, request.clone(), receipt.clone(), credential()).unwrap();
        assert_eq!(
            result.disposition(),
            OfflineMaintenanceStartDisposition::AlreadyAccepted
        );
        let mut trigger = receiver.try_recv().unwrap();
        trigger.wait_for_start_ready().await.unwrap();
        assert!(matches!(
            trigger,
            MaintenanceTrigger::RestoreArchivedBackup { .. }
        ));
        let persisted = controller
            .storage
            .lock()
            .unwrap()
            .read_archive_receipt(request.operation_id())
            .unwrap()
            .unwrap();
        assert_eq!(persisted.admission(), receipt.admission());
        assert_eq!(persisted.selection(), receipt.selection());
        assert_eq!(
            persisted.current_phase(),
            if phase == OfflineMaintenanceReceiptPhaseV1::Accepted {
                OfflineMaintenanceReceiptPhaseV1::Draining
            } else {
                phase
            }
        );
        let duplicate = admit_candidate(&controller, request, receipt, credential()).unwrap();
        assert_eq!(
            duplicate.disposition(),
            OfflineMaintenanceStartDisposition::AlreadyAccepted
        );
        assert!(receiver.try_recv().is_err());
    }
}

#[tokio::test]
async fn archive_admission_persists_before_trigger_and_exact_retry_has_one_driver() {
    let (_dir, controller, mut receiver) = controller();
    let (request, receipt) = candidate(1, "archive");
    let accepted =
        admit_candidate(&controller, request.clone(), receipt.clone(), credential()).unwrap();
    assert_eq!(
        accepted.disposition(),
        OfflineMaintenanceStartDisposition::Accepted
    );
    assert_eq!(
        accepted.operation().phase(),
        OfflineMaintenanceObservationPhase::Draining
    );
    assert_eq!(
        accepted
            .operation()
            .archive_restore()
            .unwrap()
            .archive_name(),
        request.archive_name()
    );
    let mut trigger = receiver.try_recv().unwrap();
    trigger.wait_for_start_ready().await.unwrap();
    let stored = controller
        .storage
        .lock()
        .unwrap()
        .read_archive_receipt(id(1))
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Draining
    );
    assert_eq!(stored.input_hash(), request.input_hash());
    assert!(!format!("{trigger:?}").contains("private-credential"));
    let retried = admit_candidate(&controller, request, receipt, credential()).unwrap();
    assert_eq!(
        retried.disposition(),
        OfflineMaintenanceStartDisposition::AlreadyAccepted
    );
    assert!(receiver.try_recv().is_err());
    let (changed, changed_receipt) = candidate(1, "changed");
    assert!(matches!(
        admit_candidate(&controller, changed, changed_receipt, credential()),
        Err(OfflineMaintenanceStartPortError::InputMismatch)
    ));
    assert!(receiver.try_recv().is_err());
    assert_eq!(
        controller
            .storage
            .lock()
            .unwrap()
            .read_archive_receipt(id(1))
            .unwrap()
            .unwrap(),
        stored
    );
}

#[test]
fn archive_admission_closed_queue_persists_terminal_failure_and_retry_does_not_requeue() {
    let (_dir, controller, receiver) = controller();
    drop(receiver);
    let (request, receipt) = candidate(2, "archive");
    let result =
        admit_candidate(&controller, request.clone(), receipt.clone(), credential()).unwrap();
    assert_eq!(
        result.disposition(),
        OfflineMaintenanceStartDisposition::Terminal
    );
    assert_eq!(
        result.operation().phase(),
        OfflineMaintenanceObservationPhase::FailedClosed
    );
    let stored = controller
        .storage
        .lock()
        .unwrap()
        .read_archive_receipt(id(2))
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.current_phase(),
        OfflineMaintenanceReceiptPhaseV1::FailedClosed
    );
    let retried = admit_candidate(&controller, request, receipt, credential()).unwrap();
    assert_eq!(retried, result);
}

#[test]
fn archive_admission_refuses_receipt_version_collisions_without_mutating_old_bytes() {
    let (_dir, controller, _receiver) = controller();
    let (request, candidate) = candidate(3, "archive");
    let old = OfflineMaintenanceReceiptV1::accepted(
        id(3),
        OfflineMaintenanceOperationKind::RestoreBackup,
        request.backup_name().clone(),
        riffdb_types::offline_maintenance_input_hash(
            OfflineMaintenanceOperationKind::RestoreBackup,
            request.backup_name(),
            request.confirmation(),
        ),
        request.confirmation(),
        candidate.admission().clone(),
    )
    .unwrap();
    controller
        .storage
        .lock()
        .unwrap()
        .create_or_read_receipt(&old)
        .unwrap();
    assert!(matches!(
        admit_candidate(&controller, request, candidate, credential()),
        Err(OfflineMaintenanceStartPortError::InputMismatch)
    ));
    assert_eq!(
        controller
            .storage
            .lock()
            .unwrap()
            .read_receipt(id(3))
            .unwrap()
            .unwrap(),
        old
    );
    assert!(controller.lifecycle.ordinary_admission_available());
    let (_second_dir, controller, _second_receiver) = self::controller();
    let (request, candidate) = self::candidate(4, "archive");
    controller
        .storage
        .lock()
        .unwrap()
        .create_or_read_archive_receipt(&candidate)
        .unwrap();
    assert!(matches!(
        refuse_archive_collision(
            &controller,
            &mut controller.storage.lock().unwrap(),
            request.operation_id()
        ),
        Err(OfflineMaintenanceStartPortError::InputMismatch)
    ));
    assert!(controller.lifecycle.ordinary_admission_available());
}

#[test]
fn archive_admission_terminal_retry_requires_parent_sync_before_acknowledgement() {
    use riffdb_storage_redb::{RedbMaintenanceFailpoint, RedbMaintenanceTestController};
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("database.redb");
    let backups = dir.path().join("backups");
    let (request, candidate) = candidate(5, "archive");
    {
        let (mut storage, _) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
        storage.create_or_read_archive_receipt(&candidate).unwrap();
        let mut terminal = candidate.clone();
        terminal
            .advance(OfflineMaintenanceReceiptTransitionV1::failed(
                OfflineMaintenanceReceiptFailureV1::InternalFailure,
            ))
            .unwrap();
        storage.replace_archive_receipt(&terminal).unwrap();
    }
    let failpoint =
        RedbMaintenanceTestController::return_at(RedbMaintenanceFailpoint::AfterReceiptParentSync);
    let (storage, _) =
        RedbMaintenanceStorage::open_with_test_controller(&target, &backups, failpoint.clone())
            .unwrap();
    let (sender, mut receiver) = maintenance_trigger_channel();
    let controller = MaintenanceController::new(
        shared_maintenance_storage(storage),
        Arc::new(MaintenanceLifecycle::ready()),
        sender,
    );
    assert!(matches!(
        admit_candidate(
            &controller,
            request.clone(),
            candidate.clone(),
            credential()
        ),
        Err(OfflineMaintenanceStartPortError::OutcomeUnknown)
    ));
    assert!(
        failpoint
            .events()
            .iter()
            .any(|event| event.failpoint() == RedbMaintenanceFailpoint::AfterReceiptParentSync)
    );
    assert!(receiver.try_recv().is_err());
    let terminal = admit_candidate(&controller, request, candidate, credential()).unwrap();
    assert_eq!(
        terminal.disposition(),
        OfflineMaintenanceStartDisposition::Terminal
    );
    assert!(receiver.try_recv().is_err());
}
