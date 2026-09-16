//! Archive restore conversion preserves independent progress and response bounds.
#![cfg(feature = "server")]
// req: REP-007, AFC-007
use riffdb_api_grpc::{
    offline_maintenance_start_result_to_proto, restore_archived_backup_request_from_proto,
};
use riffdb_proto::{v1, validate_restore_archived_backup_exchange};
use riffdb_service::{
    ArchiveBackupFrontier, ArchiveRestoreObservation, OfflineMaintenanceObservationPhase,
    OfflineMaintenanceOperationObservation, OfflineMaintenanceStartDisposition,
    OfflineMaintenanceStartResult, ServiceResponseCharge,
};
use riffdb_types::{
    AdministrationSequence, ArchiveRestoreStopV1, CommitSequence, DualFrontier,
    OfflineMaintenanceOperationKind,
};

#[test]
fn archive_restore_conversion_retains_exact_frontier_and_bounds_maximum_width_values() {
    let id = vec![
        1, 155, 246, 170, 166, 64, 125, 230, 137, 201, 138, 127, 112, 187, 189, 35,
    ];
    for stop in [None, Some(u64::MAX)] {
        let wire = v1::RestoreArchivedBackupRequest {
            request_id: id.clone(),
            operation_id: id.clone(),
            backup_name: "b".repeat(64),
            archive_name: "a".repeat(64),
            stop_at_sequence: stop,
            replacement_confirmation:
                v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget as i32,
        };
        let (_, request) = restore_archived_backup_request_from_proto(wire.clone()).unwrap();
        assert_eq!(
            matches!(request.stop(), ArchiveRestoreStopV1::LastArchived),
            stop.is_none()
        );
        let frontier = DualFrontier::new(
            CommitSequence::new(u64::MAX),
            AdministrationSequence::new(u64::MAX),
        );
        let detail = ArchiveRestoreObservation::new(
            request.archive_name().clone(),
            request.stop(),
            Some(ArchiveBackupFrontier::new(None)),
            Some(frontier),
        )
        .unwrap();
        let operation = OfflineMaintenanceOperationObservation::new(
            request.operation_id(),
            OfflineMaintenanceOperationKind::RestoreBackup,
            request.backup_name().clone(),
            request.input_hash(),
            OfflineMaintenanceObservationPhase::Succeeded,
            None,
        )
        .unwrap()
        .with_archive_restore(detail)
        .unwrap();
        let result = OfflineMaintenanceStartResult::new(
            OfflineMaintenanceStartDisposition::Terminal,
            operation,
        )
        .unwrap();
        let charge = result.service_response_charge_v1().unwrap();
        let (disposition, operation) = offline_maintenance_start_result_to_proto(&result);
        let response = v1::RestoreArchivedBackupResponse {
            disposition,
            operation,
        };
        validate_restore_archived_backup_exchange(&wire, &response).unwrap();
        assert!(
            riffdb_proto::validate_public_message_encoded_len(&response).unwrap() <= charge.bytes()
        );
        assert!(charge.fits());
        let detail = response.operation.unwrap().archive_restore.unwrap();
        assert!(matches!(
            detail
                .backup_frontier
                .unwrap()
                .application
                .unwrap()
                .position,
            Some(v1::frontier_position::Position::BeforeFirst(_))
        ));
        assert_eq!(
            detail
                .restored_frontier
                .unwrap()
                .application
                .unwrap()
                .position,
            Some(v1::frontier_position::Position::AppliedThrough(u64::MAX))
        );
    }
}
