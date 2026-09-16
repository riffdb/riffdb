//! V3 admission through the existing policy proof, receipt owner and trigger queue.
use super::*;
use riffdb_service::{
    ArchiveBackupFrontier, ArchiveRestoreObservation, RestoreArchivedBackupRequest,
};
use riffdb_storage_api::{
    OfflineArchiveReceiptPersistencePort, OfflineMaintenanceReceiptCreateResultV3,
    OfflineMaintenanceReceiptV3,
};

pub(super) fn admit(
    controller: &MaintenanceController,
    start: AuthorizedOfflineMaintenanceStart,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    let AuthorizedOfflineMaintenanceStart::RestoreArchivedBackup {
        request,
        authorization,
        credential,
    } = start
    else {
        return Err(OfflineMaintenanceStartPortError::Integrity);
    };
    ensure_exact_proof(
        &authorization,
        &OfflineMaintenanceAuthorizationRequest::restore_backup(
            request.operation_id(),
            request.input_hash(),
        ),
    )?;
    let receipt = OfflineMaintenanceReceiptV3::accepted_archive_restore(
        request.operation_id(),
        request.backup_name().clone(),
        request.archive_name().clone(),
        request.stop(),
        request.input_hash(),
        request.confirmation(),
        admission_from_proof(&authorization),
        Some(authorization.database_id()),
    )
    .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
    admit_candidate(controller, request, receipt, credential)
}

fn admit_candidate(
    controller: &MaintenanceController,
    request: RestoreArchivedBackupRequest,
    candidate: OfflineMaintenanceReceiptV3,
    credential: riffdb_auth::RetainedOpaqueCredential,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    let id = request.operation_id();
    let mut storage = lock_start_storage(controller, id)?;
    if storage
        .read_receipt(id)
        .map_err(|e| map_start_read_error(controller, id, e))?
        .is_some()
        || storage
            .read_retire_receipt(id)
            .map_err(|e| map_start_read_error(controller, id, e))?
            .is_some()
    {
        return Err(OfflineMaintenanceStartPortError::InputMismatch);
    }
    let existing = storage
        .read_archive_receipt(id)
        .map_err(|e| map_start_read_error(controller, id, e))?;
    let was_existing = existing.is_some();
    let mut receipt = match existing {
        Some(receipt) => {
            // A previous replacement may have returned uncertainty after rename.
            // Reading those bytes alone does not establish parent-directory durability.
            storage
                .replace_archive_receipt(&receipt)
                .map_err(|e| map_receipt_create_error(controller, id, e))?;
            receipt
        }
        None => {
            if !controller.lifecycle.ordinary_admission_available() {
                return Err(OfflineMaintenanceStartPortError::Unavailable);
            }
            match storage
                .create_or_read_archive_receipt(&candidate)
                .map_err(|e| map_receipt_create_error(controller, id, e))?
            {
                OfflineMaintenanceReceiptCreateResultV3::Created => candidate.clone(),
                OfflineMaintenanceReceiptCreateResultV3::Existing(receipt) => *receipt,
            }
        }
    };
    if receipt.operation_id() != id
        || receipt.backup_name() != request.backup_name()
        || receipt.archive_name() != request.archive_name()
        || receipt.stop() != request.stop()
        || receipt.input_hash() != request.input_hash()
        || receipt.replacement_confirmation() != request.confirmation()
    {
        return Err(OfflineMaintenanceStartPortError::InputMismatch);
    }
    // The current global policy proof is rechecked on every call. After success
    // the restored database may differ from the former target's database id.
    let current = candidate.source_database_id();
    if receipt.source_database_id() != current
        && !(receipt.current_phase().is_terminal() && receipt.staged_database_id() == current)
    {
        return Err(OfflineMaintenanceStartPortError::Integrity);
    }
    if receipt.current_phase().is_terminal() {
        drop(credential);
        return start_result(OfflineMaintenanceStartDisposition::Terminal, &receipt);
    }
    let disposition = match controller.lifecycle.claim_nonterminal_receipt(id) {
        Ok(MaintenanceReceiptClaim::AlreadyActive) => {
            drop(credential);
            return start_result(
                OfflineMaintenanceStartDisposition::AlreadyAccepted,
                &receipt,
            );
        }
        Ok(MaintenanceReceiptClaim::Reacquired) => {
            if was_existing {
                OfflineMaintenanceStartDisposition::AlreadyAccepted
            } else {
                OfflineMaintenanceStartDisposition::Accepted
            }
        }
        Err(_) => return Err(OfflineMaintenanceStartPortError::Integrity),
    };
    if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Accepted {
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::Draining,
            ))
            .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
        // Replacing an already-current V3 also synchronizes its parent. A mere
        // reread is insufficient to resolve an uncertain rename or directory sync.
        if storage.replace_archive_receipt(&receipt).is_err()
            && storage.replace_archive_receipt(&receipt).is_err()
        {
            controller.lifecycle.fail_closed(id);
            return Err(OfflineMaintenanceStartPortError::OutcomeUnknown);
        }
    }
    let (start_ready, ready) = oneshot::channel();
    let trigger = MaintenanceTrigger::RestoreArchivedBackup {
        request,
        credential,
        start_ready: ready,
    };
    if let Err(error) = controller.triggers.try_send(trigger) {
        drop(error.into_inner());
        controller.lifecycle.fail_closed(id);
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::failed(
                OfflineMaintenanceReceiptFailureV1::InternalFailure,
            ))
            .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
        storage
            .replace_archive_receipt(&receipt)
            .map_err(|_| OfflineMaintenanceStartPortError::OutcomeUnknown)?;
        return start_result(OfflineMaintenanceStartDisposition::Terminal, &receipt);
    }
    let result = start_result(disposition, &receipt);
    drop(storage);
    let _ = start_ready.send(());
    result
}

pub(crate) fn start_result(
    disposition: OfflineMaintenanceStartDisposition,
    receipt: &OfflineMaintenanceReceiptV3,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    OfflineMaintenanceStartResult::new(
        disposition,
        observation(receipt).map_err(|_| OfflineMaintenanceStartPortError::Integrity)?,
    )
    .map_err(|_| OfflineMaintenanceStartPortError::Integrity)
}

pub(super) fn observation(
    receipt: &OfflineMaintenanceReceiptV3,
) -> Result<OfflineMaintenanceOperationObservation, OfflineMaintenanceObservationPortError> {
    let transition = receipt
        .transitions()
        .last()
        .ok_or(OfflineMaintenanceObservationPortError::Integrity)?;
    let detail = ArchiveRestoreObservation::new(
        receipt.archive_name().clone(),
        receipt.stop(),
        receipt.selection().map(|selected| {
            ArchiveBackupFrontier::new(selected.backup().included_application_frontier())
        }),
        receipt.restored_frontier(),
    )
    .map_err(|_| OfflineMaintenanceObservationPortError::Integrity)?;
    OfflineMaintenanceOperationObservation::new(
        receipt.operation_id(),
        receipt.operation_kind(),
        receipt.backup_name().clone(),
        receipt.input_hash(),
        map_phase(transition.receipt_phase()),
        transition.failure().map(map_failure),
    )
    .and_then(|observation| observation.with_archive_restore(detail))
    .map_err(|_| OfflineMaintenanceObservationPortError::Integrity)
}

/// Inventory decoding keeps version collisions distinct from malformed receipts.
pub(super) fn refuse_archive_collision(
    controller: &MaintenanceController,
    storage: &mut RedbMaintenanceStorage,
    id: OfflineMaintenanceOperationId,
) -> Result<(), OfflineMaintenanceStartPortError> {
    let inventory = storage
        .validate_archive_receipt_inventory()
        .map_err(|e| map_start_read_error(controller, id, e))?;
    if inventory
        .receipts()
        .iter()
        .any(|receipt| receipt.operation_id() == id)
    {
        return Err(OfflineMaintenanceStartPortError::InputMismatch);
    }
    Ok(())
}

#[cfg(test)]
#[path = "maintenance_archive_adapter_tests.rs"]
mod tests;
