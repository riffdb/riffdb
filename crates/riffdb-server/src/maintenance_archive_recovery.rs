//! Recovery admission authenticates a fully validated private archive candidate.
use super::*;
use riffdb_service::RestoreArchivedBackupRequest;
use riffdb_storage_api::OfflineMaintenanceReceiptCreateResultV3;

pub(in crate::maintenance_driver) fn matches_request(
    receipt: &OfflineMaintenanceReceiptV3,
    request: &RestoreArchivedBackupRequest,
) -> bool {
    receipt.operation_id() == request.operation_id()
        && receipt.input_hash() == request.input_hash()
        && receipt.backup_name() == request.backup_name()
        && receipt.archive_name() == request.archive_name()
        && receipt.stop() == request.stop()
        && receipt.replacement_confirmation() == request.confirmation()
}

pub(in crate::maintenance_driver) fn run_recovery(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    dependencies: &MaintenanceDriverDependencies<'_>,
    request: RestoreArchivedBackupRequest,
    credential: Option<RetainedOpaqueCredential>,
) -> Result<MaintenanceDriverSuccess, MaintenanceDriverFailure> {
    let id = request.operation_id();
    let mut receipt = storage.read_archive_receipt(id).map_err(|_| {
        lifecycle.fail_closed(id);
        MaintenanceDriverFailure::without_receipt(
            id,
            OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
        )
    })?;
    if receipt
        .as_ref()
        .is_some_and(|r| !matches_request(r, &request))
    {
        return Err(MaintenanceDriverFailure::without_receipt(
            id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }
    if receipt
        .as_ref()
        .is_some_and(|r| r.source_database_id().is_none() && r.selection().is_none())
    {
        lifecycle.fail_closed(id);
        return Err(MaintenanceDriverFailure::without_receipt(
            id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }
    if let Some(terminal) = receipt.as_ref().filter(|r| r.current_phase().is_terminal()) {
        // Return the retained outcome without performing another restore. Resolve
        // parent synchronization before claiming those terminal bytes are durable.
        storage.replace_archive_receipt(terminal).map_err(|_| {
            lifecycle.fail_closed(id);
            MaintenanceDriverFailure::without_receipt(
                id,
                OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
            )
        })?;
        lifecycle.fail_closed(id);
        return Err(MaintenanceDriverFailure {
            operation_id: id,
            failure: terminal
                .transitions()
                .last()
                .and_then(|t| t.failure())
                .unwrap_or(OfflineMaintenanceReceiptFailureV1::InternalFailure),
            terminal_receipt: None,
            archive_terminal_receipt: Some(Box::new(terminal.clone())),
        });
    }
    let result = (|| {
        if lifecycle.stage() != MaintenanceLifecycleStage::Offline
            || lifecycle.claim_nonterminal_receipt(id) != Ok(MaintenanceReceiptClaim::AlreadyActive)
        {
            return Err(DriverFault::Lifecycle);
        }
        use OfflineMaintenanceReceiptPhaseV1::*;
        let published = if let Some(receipt) = &receipt {
            matches!(receipt.current_phase(), ArtifactPublished | Validating)
                || (receipt.current_phase() == Offline
                    && storage
                        .archive_restore_target_matches(id)
                        .map_err(DriverFault::ArtifactStorage)?)
        } else {
            false
        };
        if !published {
            let credential = credential.ok_or(DriverFault::StagedAuthorization)?;
            let configured = dependencies
                .archives
                .iter()
                .find(|(database, archive)| {
                    database == storage.configured_database_file()
                        && archive.name() == request.archive_name()
                })
                .map(|(_, archive)| archive)
                .ok_or(DriverFault::ArtifactUnavailable)?;
            let repository = storage
                .open_recovery_archive_repository(
                    request.backup_name(),
                    configured.path(),
                    configured.encryption(),
                )
                .map_err(DriverFault::ArtifactStorage)?;
            let candidate = storage
                .stage_recovery_restore_candidate(
                    id,
                    request.backup_name(),
                    dependencies.startup_inputs.clone(),
                )
                .map_err(DriverFault::ArtifactStorage)?;
            let cancellation = Arc::new(AtomicBool::new(false));
            let candidate = match receipt.as_ref().and_then(|r| r.selection()) {
                Some(selection) => candidate.begin_selected_archive_replay(
                    repository,
                    selection.clone(),
                    &cancellation,
                ),
                None => candidate.begin_archive_replay(repository, &cancellation),
            }
            .map_err(DriverFault::ArtifactStorage)?;
            let selection = candidate.selection().clone();
            if let Some(receipt) = receipt.as_mut() {
                // A previously admitted current-source restore freezes selection
                // before replay, even when its current target is now unavailable.
                if receipt.current_phase() == Accepted && receipt.source_database_id().is_some() {
                    advance(storage, receipt, Draining)?;
                }
                if matches!(receipt.current_phase(), Accepted | Draining) {
                    advance(storage, receipt, Offline)?;
                }
                let mut selected = receipt.clone();
                selected
                    .record_selection(selection.clone())
                    .map_err(|_| DriverFault::ReceiptValue)?;
                update(storage, receipt, selected)?;
            }
            let prepared = candidate
                .prepare_restore(
                    request.stop(),
                    dependencies.startup_inputs.clone(),
                    cancellation,
                )
                .map_err(DriverFault::ArtifactStorage)?;
            let database = selection.lineage().database_id();
            let frontier = prepared.restored_frontier();
            let admission = authorize_staged_restore(
                prepared
                    .authorization_snapshot()
                    .map_err(DriverFault::ArtifactStorage)?,
                database,
                credential,
                id,
                request.input_hash(),
                dependencies,
            )
            .map_err(|_| DriverFault::StagedAuthorization)?;
            let sealed = prepared
                .seal_after_authorization(database)
                .map_err(DriverFault::ArtifactStorage)?;
            dependencies
                .recovery
                .reached(MaintenanceRecoveryBoundary::StagedAuthorizationComplete);
            if receipt.is_none() {
                let mut admitted = OfflineMaintenanceReceiptV3::accepted_archive_restore(
                    id,
                    request.backup_name().clone(),
                    request.archive_name().clone(),
                    request.stop(),
                    request.input_hash(),
                    request.confirmation(),
                    admission,
                    None,
                )
                .map_err(|_| DriverFault::ReceiptValue)?;
                admitted
                    .record_selection(selection.clone())
                    .map_err(|_| DriverFault::ReceiptValue)?;
                // Retain the candidate before the uncertain durable call. Any
                // error from this point must stop, never reopen new admission.
                receipt = Some(admitted.clone());
                match storage
                    .create_or_read_archive_receipt(&admitted)
                    .map_err(|_| DriverFault::ReceiptStorage)?
                {
                    OfflineMaintenanceReceiptCreateResultV3::Created => {}
                    OfflineMaintenanceReceiptCreateResultV3::Existing(existing)
                        if *existing == admitted => {}
                    _ => return Err(DriverFault::ReceiptIntegrity),
                }
            }
            let receipt = receipt.as_mut().ok_or(DriverFault::ReceiptIntegrity)?;
            if matches!(receipt.current_phase(), Accepted | Draining) {
                advance(storage, receipt, Offline)?;
            }
            publish_prepared(
                storage,
                receipt,
                sealed,
                database,
                frontier,
                selection.lineage().history_incarnation(),
                dependencies,
            )?;
        } else {
            drop(credential);
        }
        let receipt = receipt.as_mut().ok_or(DriverFault::ReceiptIntegrity)?;
        restore(storage, lifecycle, dependencies, receipt, None)
    })();
    match result {
        Ok(startup) => Ok(MaintenanceDriverSuccess {
            receipt: MaintenanceTerminalReceipt::V3(receipt.ok_or_else(|| {
                MaintenanceDriverFailure::without_receipt(
                    id,
                    OfflineMaintenanceReceiptFailureV1::InternalFailure,
                )
            })?),
            startup,
        }),
        Err(fault) => match receipt {
            Some(receipt) => Err(fail(storage, lifecycle, receipt, fault)),
            None => {
                if storage.discard_pre_receipt_recovery_stage(id).is_err() {
                    lifecycle.fail_closed(id);
                    return Err(MaintenanceDriverFailure::without_receipt(
                        id,
                        OfflineMaintenanceReceiptFailureV1::InternalFailure,
                    ));
                }
                Err(MaintenanceDriverFailure::without_receipt(
                    id,
                    fault.receipt_failure(),
                ))
            }
        },
    }
}
