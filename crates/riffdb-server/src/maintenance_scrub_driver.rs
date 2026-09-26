//! Receipt V4 execution inside the existing exclusive offline lifecycle: the
//! ADR-0203 closed-database complete validation pass.
use super::*;
use riffdb_storage_api::OfflineMaintenanceReceiptV4;
use riffdb_storage_redb::RedbOfflineIntegrityScrub;

pub(super) fn read(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    id: OfflineMaintenanceOperationId,
) -> Result<Option<OfflineMaintenanceReceiptV4>, MaintenanceDriverFailure> {
    storage.read_scrub_receipt(id).map_err(|_| {
        lifecycle.fail_closed(id);
        MaintenanceDriverFailure::without_receipt(
            id,
            OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
        )
    })
}

fn advance(
    storage: &mut RedbMaintenanceStorage,
    receipt: &mut OfflineMaintenanceReceiptV4,
    phase: OfflineMaintenanceReceiptPhaseV1,
) -> Result<(), DriverFault> {
    if receipt.current_phase() == phase {
        return Ok(());
    }
    let mut candidate = receipt.clone();
    candidate
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
        .map_err(|_| DriverFault::ReceiptValue)?;
    storage
        .replace_scrub_receipt(&candidate)
        .map_err(|_| DriverFault::ReceiptStorage)?;
    *receipt = candidate;
    Ok(())
}

fn fail(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    mut receipt: OfflineMaintenanceReceiptV4,
    fault: DriverFault,
) -> MaintenanceDriverFailure {
    lifecycle.fail_closed(receipt.operation_id());
    let failure = fault.receipt_failure();
    if !receipt.current_phase().is_terminal()
        && receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::failed(failure))
            .is_ok()
    {
        // Failure reporting never claims durability when replacement is uncertain.
        let _ = storage.replace_scrub_receipt(&receipt);
    }
    MaintenanceDriverFailure::without_receipt(receipt.operation_id(), failure)
}

pub(super) fn mark_phase(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    mut receipt: OfflineMaintenanceReceiptV4,
    phase: OfflineMaintenanceReceiptPhaseV1,
) -> Result<(), MaintenanceDriverFailure> {
    let result = (|| {
        use OfflineMaintenanceReceiptPhaseV1::*;
        match (phase, receipt.current_phase()) {
            (Draining, Accepted) | (Offline, Draining) => advance(storage, &mut receipt, phase)?,
            (Draining, Draining | Offline | Validating) | (Offline, Offline | Validating) => {}
            _ => return Err(DriverFault::ReceiptIntegrity),
        }
        if phase == Offline {
            lifecycle
                .mark_offline(receipt.operation_id())
                .map_err(|_| DriverFault::Lifecycle)?;
        }
        Ok(())
    })();
    result.map_err(|fault| fail(storage, lifecycle, receipt, fault))
}

/// Streams the shared complete structural and catalog-semantic exact-end
/// validator over the closed database, then reopens through ordinary
/// bounded-root startup. Success creates no clean certificate and forces no
/// readiness (Decision 9); any refusal is the terminal `ValidationFailed`.
pub(super) fn run(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    dependencies: &MaintenanceDriverDependencies<'_>,
    mut receipt: OfflineMaintenanceReceiptV4,
    request: MaintenanceDriverRequest,
) -> Result<MaintenanceDriverSuccess, MaintenanceDriverFailure> {
    let result = (|| {
        if !matches!(request, MaintenanceDriverRequest::StorageScrub { .. })
            || request.operation_id() != receipt.operation_id()
        {
            return Err(DriverFault::ReceiptIntegrity);
        }
        match receipt.current_phase() {
            OfflineMaintenanceReceiptPhaseV1::Offline => advance(
                storage,
                &mut receipt,
                OfflineMaintenanceReceiptPhaseV1::Validating,
            )?,
            // An interrupted pass restarts from the beginning; the validator
            // is read-only, so rerunning it is exact.
            OfflineMaintenanceReceiptPhaseV1::Validating => {}
            _ => return Err(DriverFault::ReceiptIntegrity),
        }
        RedbOfflineIntegrityScrub::from_inputs(
            storage.configured_database_file(),
            dependencies.startup_inputs.clone(),
        )
        .run()
        .map_err(|error| match error.kind() {
            StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown => {
                DriverFault::ScrubUnavailable
            }
            _ => DriverFault::Validation,
        })?;
        mark_lifecycle_validating(lifecycle, receipt.operation_id())?;
        let startup =
            reopen_current_source(storage, dependencies).map_err(|_| DriverFault::Validation)?;
        advance(
            storage,
            &mut receipt,
            OfflineMaintenanceReceiptPhaseV1::Succeeded,
        )?;
        Ok(startup)
    })();
    match result {
        Ok(startup) => Ok(MaintenanceDriverSuccess {
            receipt: MaintenanceTerminalReceipt::V4(receipt),
            startup,
        }),
        Err(fault) => Err(fail(storage, lifecycle, receipt, fault)),
    }
}
