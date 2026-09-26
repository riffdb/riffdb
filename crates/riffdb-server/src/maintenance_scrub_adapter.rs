//! ADR-0203 public offline storage scrub admission through the existing policy
//! proof, receipt owner, and trigger queue. Only scrub writes receipt V4.
use super::*;
use riffdb_storage_api::{
    OfflineArchiveReceiptPersistencePort, OfflineMaintenanceReceiptCreateResultV4,
    OfflineMaintenanceReceiptV4,
};

pub(super) fn admit(
    controller: &MaintenanceController,
    start: AuthorizedOfflineMaintenanceStart,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    let AuthorizedOfflineMaintenanceStart::StorageScrub {
        request,
        authorization,
    } = start
    else {
        return Err(OfflineMaintenanceStartPortError::Integrity);
    };
    let id = request.operation_id();
    ensure_exact_proof(
        &authorization,
        &OfflineMaintenanceAuthorizationRequest::storage_scrub(id),
    )?;
    let mut storage = lock_start_storage(controller, id)?;
    // Decision 7: reusing an identity for another kind is the stable mismatch.
    if storage
        .read_receipt(id)
        .map_err(|e| map_start_read_error(controller, id, e))?
        .is_some()
        || storage
            .read_retire_receipt(id)
            .map_err(|e| map_start_read_error(controller, id, e))?
            .is_some()
        || storage
            .read_archive_receipt(id)
            .map_err(|e| map_start_read_error(controller, id, e))?
            .is_some()
    {
        return Err(OfflineMaintenanceStartPortError::InputMismatch);
    }
    let existing = storage
        .read_scrub_receipt(id)
        .map_err(|e| map_start_read_error(controller, id, e))?;
    let was_existing = existing.is_some();
    let mut receipt = match existing {
        Some(receipt) => receipt,
        None => {
            if !controller.lifecycle.ordinary_admission_available() {
                return Err(OfflineMaintenanceStartPortError::Unavailable);
            }
            let candidate = OfflineMaintenanceReceiptV4::accepted_storage_scrub(
                id,
                admission_from_proof(&authorization),
            )
            .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
            match storage
                .create_or_read_scrub_receipt(&candidate)
                .map_err(|e| map_receipt_create_error(controller, id, e))?
            {
                OfflineMaintenanceReceiptCreateResultV4::Created => candidate,
                OfflineMaintenanceReceiptCreateResultV4::Existing(receipt) => *receipt,
            }
        }
    };
    if receipt.operation_id() != id || receipt.input_hash() != request.input_hash() {
        return Err(OfflineMaintenanceStartPortError::InputMismatch);
    }
    if receipt.current_phase().is_terminal() {
        return start_result(OfflineMaintenanceStartDisposition::Terminal, &receipt);
    }
    let disposition = match controller.lifecycle.claim_nonterminal_receipt(id) {
        Ok(MaintenanceReceiptClaim::AlreadyActive) => {
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
        let mut draining = receipt.clone();
        draining
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::Draining,
            ))
            .map_err(|_| OfflineMaintenanceStartPortError::Integrity)?;
        // Replacing an already-current V4 also synchronizes its parent, so one
        // retry resolves an uncertain rename; a reread alone would not.
        if storage.replace_scrub_receipt(&draining).is_err()
            && storage.replace_scrub_receipt(&draining).is_err()
        {
            controller.lifecycle.fail_closed(id);
            return Err(OfflineMaintenanceStartPortError::OutcomeUnknown);
        }
        receipt = draining;
    }
    let (start_ready, ready) = oneshot::channel();
    let trigger = MaintenanceTrigger::StorageScrub {
        request,
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
            .replace_scrub_receipt(&receipt)
            .map_err(|_| OfflineMaintenanceStartPortError::OutcomeUnknown)?;
        return start_result(OfflineMaintenanceStartDisposition::Terminal, &receipt);
    }
    let result = start_result(disposition, &receipt);
    drop(storage);
    let _ = start_ready.send(());
    result
}

pub(super) fn start_result(
    disposition: OfflineMaintenanceStartDisposition,
    receipt: &OfflineMaintenanceReceiptV4,
) -> Result<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError> {
    OfflineMaintenanceStartResult::new(
        disposition,
        observation(receipt).map_err(|_| OfflineMaintenanceStartPortError::Integrity)?,
    )
    .map_err(|_| OfflineMaintenanceStartPortError::Integrity)
}

pub(super) fn observation(
    receipt: &OfflineMaintenanceReceiptV4,
) -> Result<OfflineMaintenanceOperationObservation, OfflineMaintenanceObservationPortError> {
    let transition = receipt
        .transitions()
        .last()
        .ok_or(OfflineMaintenanceObservationPortError::Integrity)?;
    OfflineMaintenanceOperationObservation::storage_scrub(
        receipt.operation_id(),
        map_phase(transition.receipt_phase()),
        transition.failure().map(map_failure),
    )
    .map_err(|_| OfflineMaintenanceObservationPortError::Integrity)
}

/// A scrub identity is never reusable by create, restore, retire, or archive
/// restore; the collision is the stable mismatch, not a corrupt inventory.
pub(super) fn refuse_scrub_collision(
    controller: &MaintenanceController,
    storage: &mut RedbMaintenanceStorage,
    id: OfflineMaintenanceOperationId,
) -> Result<(), OfflineMaintenanceStartPortError> {
    if storage
        .read_scrub_receipt(id)
        .map_err(|e| map_start_read_error(controller, id, e))?
        .is_some()
    {
        return Err(OfflineMaintenanceStartPortError::InputMismatch);
    }
    Ok(())
}
