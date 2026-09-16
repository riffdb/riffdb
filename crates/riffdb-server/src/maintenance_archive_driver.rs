//! Receipt V3 execution inside the existing exclusive offline lifecycle.
use super::*;
use riffdb_storage_api::{
    OfflineArchiveReceiptPersistencePort, OfflineArchiveRestorePublicationV3,
    OfflineMaintenanceReceiptV3,
};
use std::sync::atomic::AtomicBool;

pub(super) fn read(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    id: OfflineMaintenanceOperationId,
) -> Result<Option<OfflineMaintenanceReceiptV3>, MaintenanceDriverFailure> {
    storage
        .validate_archive_receipt_inventory()
        .map(|inventory| {
            inventory
                .receipts()
                .iter()
                .find(|receipt| receipt.operation_id() == id)
                .cloned()
        })
        .map_err(|_| {
            lifecycle.fail_closed(id);
            MaintenanceDriverFailure::without_receipt(
                id,
                OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
            )
        })
}

fn update(
    storage: &mut RedbMaintenanceStorage,
    receipt: &mut OfflineMaintenanceReceiptV3,
    candidate: OfflineMaintenanceReceiptV3,
) -> Result<(), DriverFault> {
    storage
        .replace_archive_receipt(&candidate)
        .map_err(|_| DriverFault::ReceiptStorage)?;
    *receipt = candidate;
    Ok(())
}
fn advance(
    storage: &mut RedbMaintenanceStorage,
    receipt: &mut OfflineMaintenanceReceiptV3,
    phase: OfflineMaintenanceReceiptPhaseV1,
) -> Result<(), DriverFault> {
    if receipt.current_phase() == phase {
        return Ok(());
    }
    let mut candidate = receipt.clone();
    candidate
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
        .map_err(|_| DriverFault::ReceiptValue)?;
    update(storage, receipt, candidate)
}
fn fail(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    mut receipt: OfflineMaintenanceReceiptV3,
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
        let _ = storage.replace_archive_receipt(&receipt);
    }
    MaintenanceDriverFailure::without_receipt(receipt.operation_id(), failure)
}

pub(super) fn mark_phase(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    mut receipt: OfflineMaintenanceReceiptV3,
    phase: OfflineMaintenanceReceiptPhaseV1,
) -> Result<(), MaintenanceDriverFailure> {
    let result = (|| {
        if receipt.source_database_id().is_none() {
            return Err(DriverFault::ReceiptIntegrity);
        }
        use OfflineMaintenanceReceiptPhaseV1::*;
        match (phase, receipt.current_phase()) {
            (Draining, Accepted) | (Offline, Draining) => advance(storage, &mut receipt, phase)?,
            (Draining, Draining | Offline | ArtifactPublished | Validating)
            | (Offline, Offline | ArtifactPublished | Validating) => {}
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

pub(super) fn run(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    dependencies: &MaintenanceDriverDependencies<'_>,
    mut receipt: OfflineMaintenanceReceiptV3,
    request: MaintenanceDriverRequest,
) -> Result<MaintenanceDriverSuccess, MaintenanceDriverFailure> {
    let result = (|| {
        if request.operation_kind() != OfflineMaintenanceOperationKind::RestoreBackup
            || receipt.source_database_id().is_none()
        {
            return Err(DriverFault::ReceiptIntegrity);
        }
        let credential = match request {
            MaintenanceDriverRequest::RestoreBackup { credential, .. } => Some(credential),
            MaintenanceDriverRequest::ResumePublishedRestore { .. } => None,
            _ => return Err(DriverFault::ReceiptIntegrity),
        };
        restore(storage, lifecycle, dependencies, &mut receipt, credential)
    })();
    match result {
        Ok(startup) => Ok(MaintenanceDriverSuccess {
            receipt: MaintenanceTerminalReceipt::V3(receipt),
            startup,
        }),
        Err(fault) => Err(fail(storage, lifecycle, receipt, fault)),
    }
}

fn restore(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    dependencies: &MaintenanceDriverDependencies<'_>,
    receipt: &mut OfflineMaintenanceReceiptV3,
    credential: Option<RetainedOpaqueCredential>,
) -> Result<CheckedRedbStartup, DriverFault> {
    use OfflineMaintenanceReceiptPhaseV1::*;
    let published = receipt.current_phase() == Offline
        && storage
            .archive_restore_target_matches(receipt.operation_id())
            .map_err(DriverFault::ArtifactStorage)?;
    match receipt.current_phase() {
        Offline if published => {
            drop(credential);
            advance(storage, receipt, ArtifactPublished)?;
        }
        Offline => {
            let credential = credential.ok_or(DriverFault::StagedAuthorization)?;
            let configured = dependencies
                .archives
                .iter()
                .find(|(database, archive)| {
                    database == storage.configured_database_file()
                        && archive.name() == receipt.archive_name()
                })
                .map(|(_, archive)| archive)
                .ok_or(DriverFault::ArtifactUnavailable)?;
            let repository = storage
                .open_archive_restore_repository(
                    receipt.operation_id(),
                    configured.path(),
                    configured.encryption(),
                )
                .map_err(DriverFault::ArtifactStorage)?;
            let stage = storage
                .stage_archive_restore(receipt.operation_id(), dependencies.startup_inputs.clone())
                .map_err(DriverFault::ArtifactStorage)?;
            let cancellation = Arc::new(AtomicBool::new(false));
            let stage = match receipt.selection() {
                Some(selection) => stage.begin_selected_archive_replay(
                    repository,
                    selection.clone(),
                    &cancellation,
                ),
                None => stage.begin_archive_replay(repository, &cancellation),
            }
            .map_err(DriverFault::ArtifactStorage)?;
            let mut selected = receipt.clone();
            selected
                .record_selection(stage.selection().clone())
                .map_err(|_| DriverFault::ReceiptValue)?;
            update(storage, receipt, selected)?;
            let prepared = stage
                .prepare_restore(
                    receipt.stop(),
                    dependencies.startup_inputs.clone(),
                    cancellation,
                )
                .map_err(DriverFault::ArtifactStorage)?;
            let database_id = prepared.selection().lineage().database_id();
            let staged_incarnation = prepared.selection().lineage().history_incarnation();
            let frontier = prepared.restored_frontier();
            authorize_staged_restore(
                prepared
                    .authorization_snapshot()
                    .map_err(DriverFault::ArtifactStorage)?,
                database_id,
                credential,
                receipt.operation_id(),
                receipt.input_hash(),
                dependencies,
            )
            .map_err(|_| DriverFault::StagedAuthorization)?;
            let sealed = prepared
                .seal_after_authorization(database_id)
                .map_err(DriverFault::ArtifactStorage)?;
            dependencies
                .recovery
                .reached(MaintenanceRecoveryBoundary::StagedAuthorizationComplete);
            let incarnation = match receipt.published_history_incarnation() {
                Some(recorded) => recorded,
                None => target_history_incarnation_for_bump(
                    storage.configured_database_file(),
                    dependencies.retained_target_history_incarnation,
                    staged_incarnation,
                    dependencies.metrics.as_ref(),
                )
                .max(staged_incarnation)
                .checked_add(1)
                .ok_or(DriverFault::ArtifactInvalid)?,
            };
            let mut validated = receipt.clone();
            validated
                .record_validated_restore(database_id, frontier)
                .map_err(|_| DriverFault::ReceiptValue)?;
            validated
                .record_published_incarnation(incarnation)
                .map_err(|_| DriverFault::ReceiptValue)?;
            update(storage, receipt, validated)?;
            match storage.publish_sealed_archive_restore(sealed) {
                Ok(OfflineArchiveRestorePublicationV3::Published {
                    restored_frontier,
                    published_history_incarnation,
                    ..
                }) if restored_frontier == frontier
                    && published_history_incarnation == incarnation => {}
                Err(error) if error.kind() == StorageErrorKind::CommitStatusUnknown => {
                    if !storage
                        .archive_restore_target_matches(receipt.operation_id())
                        .map_err(DriverFault::ArtifactStorage)?
                    {
                        return Err(DriverFault::PublicationUncertain);
                    }
                }
                Err(error) => return Err(DriverFault::ArtifactStorage(error)),
                _ => return Err(DriverFault::ArtifactUnavailable),
            }
            advance(storage, receipt, ArtifactPublished)?;
        }
        ArtifactPublished | Validating => {
            drop(credential);
        }
        _ => return Err(DriverFault::ReceiptIntegrity),
    }
    advance(storage, receipt, Validating)?;
    mark_lifecycle_validating(lifecycle, receipt.operation_id())?;
    let evidence = storage
        .archive_restore_validation_evidence(receipt.operation_id())
        .map_err(DriverFault::ArtifactStorage)?;
    let startup = open_redb_startup_with_commit_profile(
        storage.configured_database_file(),
        dependencies.startup_inputs.clone(),
        &dependencies.database_ids,
        dependencies.application_commit_profile,
    )
    .map_err(|_| DriverFault::Validation)?;
    let metadata = startup.retained_metadata();
    let frontier = receipt
        .restored_frontier()
        .ok_or(DriverFault::ReceiptIntegrity)?;
    use riffdb_storage_api::{
        AdministrationSequenceAllocator as Admin, ApplicationSequenceAllocator as App,
    };
    let application = frontier.application().map_or_else(App::initial, |s| {
        s.checked_next().map_or(App::Exhausted, App::Next)
    });
    let administration = frontier.administration().map_or_else(Admin::initial, |s| {
        s.checked_next().map_or(Admin::Exhausted, Admin::Next)
    });
    if Some(startup.database_id()) != receipt.staged_database_id()
        || Some(metadata.history_incarnation()) != receipt.published_history_incarnation()
        || metadata.application_sequence() != application
        || metadata.administration_sequence() != administration
    {
        return Err(DriverFault::ValidationIdentity);
    }
    if !startup
        .matches_archive_restore_evidence(evidence)
        .map_err(|_| DriverFault::Validation)?
    {
        return Err(DriverFault::ValidationIdentity);
    }
    dependencies
        .recovery
        .reached(MaintenanceRecoveryBoundary::FreshValidationComplete);
    advance(storage, receipt, Succeeded)?;
    Ok(startup)
}
