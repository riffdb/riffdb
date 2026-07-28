//! Blocking, transport-free execution of one exclusive offline maintenance operation.
//!
//! The daemon calls this module only at explicit lifecycle boundaries. No type
//! here is public API or durable format ownership; receipts remain owned by
//! `riffdb-storage-api` and their encoding remains owned by
//! `riffdb-storage-redb`.

use std::fmt;
use std::sync::Arc;

use riffdb_auth::{
    AuthenticationTelemetry, CapabilityDigestKeyProvider, CredentialAuthenticator,
    RetainedOpaqueCredential,
};
use riffdb_contract_ir::EXECUTABLE_IR_VERSION_V1;
use riffdb_policy::{
    AuthorizationTelemetry, AuthorizedOfflineMaintenance, OfflineMaintenanceAuthorizationRequest,
    OfflineMaintenanceDecision, OutputClassification, TrustedAudienceCatalog,
};
use riffdb_service::{CurrentPolicyPort, RestoreOfflineBackupRequest};
use riffdb_storage_api::{
    BackupBuildMetadataV1, OfflineMaintenanceAdmissionV1, OfflineMaintenanceReceiptCreateResultV1,
    OfflineMaintenanceReceiptFailureV1, OfflineMaintenanceReceiptPersistencePort,
    OfflineMaintenanceReceiptPhaseV1, OfflineMaintenanceReceiptTransitionV1,
    OfflineMaintenanceReceiptV1, OfflineRestoreOverwritePolicyV1, OfflineRestoreResultV1,
    StartupValidationInputs, StorageError, StorageErrorKind, StorageValueError,
};
use riffdb_storage_redb::{
    RedbMaintenanceOperationEvidence, RedbMaintenanceStorage, RedbSealedStagedRestore,
    RedbStagedRestore,
};
use riffdb_types::{
    Audience, Environment, OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation, TenantScope,
};

use crate::auth_adapters::{ServerCredentialAuthenticator, ServerCurrentPolicyPort};
use crate::clocks::ProductionWallClocks;
use crate::identifiers::DatabaseIdCandidateSource;
use crate::maintenance_lifecycle::{
    MaintenanceLifecycle, MaintenanceLifecycleStage, MaintenanceReceiptClaim,
};
use crate::maintenance_recovery_controller::{
    MaintenanceRecoveryBoundary, MaintenanceRecoveryController,
};
use crate::startup::{CheckedRedbStartup, open_redb_startup};
use crate::storage::SharedRedbOperationalPorts;

/// Production dependencies needed only while a private staged database is open.
pub(crate) struct MaintenanceDriverDependencies<'a> {
    startup_inputs: StartupValidationInputs,
    database_ids: DatabaseIdCandidateSource,
    capability_keys: Arc<CapabilityDigestKeyProvider>,
    environment: Environment,
    grpc_audience: Audience,
    trusted_audiences: TrustedAudienceCatalog,
    clocks: &'a ProductionWallClocks,
    authentication_telemetry: Arc<dyn AuthenticationTelemetry>,
    authorization_telemetry: Arc<dyn AuthorizationTelemetry>,
    recovery: &'a MaintenanceRecoveryController,
}

impl<'a> MaintenanceDriverDependencies<'a> {
    /// Captures value-only validation facts and the existing production auth adapters.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        startup_inputs: StartupValidationInputs,
        database_ids: DatabaseIdCandidateSource,
        capability_keys: Arc<CapabilityDigestKeyProvider>,
        environment: Environment,
        grpc_audience: Audience,
        trusted_audiences: TrustedAudienceCatalog,
        clocks: &'a ProductionWallClocks,
        authentication_telemetry: Arc<dyn AuthenticationTelemetry>,
        authorization_telemetry: Arc<dyn AuthorizationTelemetry>,
        recovery: &'a MaintenanceRecoveryController,
    ) -> Self {
        Self {
            startup_inputs,
            database_ids,
            capability_keys,
            environment,
            grpc_audience,
            trusted_audiences,
            clocks,
            authentication_telemetry,
            authorization_telemetry,
            recovery,
        }
    }
}

impl fmt::Debug for MaintenanceDriverDependencies<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MaintenanceDriverDependencies([REDACTED])")
    }
}

/// One normal, already-authorized operation selected by its durable receipt.
pub(crate) enum MaintenanceDriverRequest {
    CreateBackup {
        operation_id: OfflineMaintenanceOperationId,
    },
    RestoreBackup {
        operation_id: OfflineMaintenanceOperationId,
        credential: RetainedOpaqueCredential,
    },
    ResumePublishedRestore {
        operation_id: OfflineMaintenanceOperationId,
    },
}

impl MaintenanceDriverRequest {
    #[must_use]
    pub(crate) const fn create_backup(operation_id: OfflineMaintenanceOperationId) -> Self {
        Self::CreateBackup { operation_id }
    }

    #[must_use]
    pub(crate) const fn restore_backup(
        operation_id: OfflineMaintenanceOperationId,
        credential: RetainedOpaqueCredential,
    ) -> Self {
        Self::RestoreBackup {
            operation_id,
            credential,
        }
    }

    /// Resumes only when exact reconciliation proves publication already occurred.
    #[must_use]
    pub(crate) const fn resume_published_restore(
        operation_id: OfflineMaintenanceOperationId,
    ) -> Self {
        Self::ResumePublishedRestore { operation_id }
    }

    pub(crate) const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        match self {
            Self::CreateBackup { operation_id }
            | Self::RestoreBackup { operation_id, .. }
            | Self::ResumePublishedRestore { operation_id } => *operation_id,
        }
    }

    const fn operation_kind(&self) -> OfflineMaintenanceOperationKind {
        match self {
            Self::CreateBackup { .. } => OfflineMaintenanceOperationKind::CreateBackup,
            Self::RestoreBackup { .. } | Self::ResumePublishedRestore { .. } => {
                OfflineMaintenanceOperationKind::RestoreBackup
            }
        }
    }
}

impl fmt::Debug for MaintenanceDriverRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CreateBackup { .. } => "MaintenanceDriverRequest::CreateBackup([REDACTED])",
            Self::RestoreBackup { .. } => "MaintenanceDriverRequest::RestoreBackup([REDACTED])",
            Self::ResumePublishedRestore { .. } => {
                "MaintenanceDriverRequest::ResumePublishedRestore([REDACTED])"
            }
        })
    }
}

/// Staged-only restore request accepted while ordinary readiness is unavailable.
pub(crate) struct RecoveryMaintenanceDriverRequest {
    request: RestoreOfflineBackupRequest,
    credential: Option<RetainedOpaqueCredential>,
}

impl RecoveryMaintenanceDriverRequest {
    #[must_use]
    pub(crate) const fn new(
        request: RestoreOfflineBackupRequest,
        credential: RetainedOpaqueCredential,
    ) -> Self {
        Self {
            request,
            credential: Some(credential),
        }
    }

    /// Resumes only a reconciliation-proven already-published source-less restore.
    #[must_use]
    pub(crate) const fn resume_published(request: RestoreOfflineBackupRequest) -> Self {
        Self {
            request,
            credential: None,
        }
    }
}

impl fmt::Debug for RecoveryMaintenanceDriverRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecoveryMaintenanceDriverRequest([REDACTED])")
    }
}

/// Checked successful handoff to the daemon's fresh graph builder.
pub(crate) struct MaintenanceDriverSuccess {
    receipt: OfflineMaintenanceReceiptV1,
    startup: CheckedRedbStartup,
}

impl MaintenanceDriverSuccess {
    /// Separates the durable terminal receipt from the newly activated storage ports.
    #[must_use]
    pub(crate) fn into_parts(self) -> (OfflineMaintenanceReceiptV1, CheckedRedbStartup) {
        (self.receipt, self.startup)
    }
}

impl fmt::Debug for MaintenanceDriverSuccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaintenanceDriverSuccess")
            .field("operation_id", &self.receipt.operation_id())
            .field("receipt", &"[CHECKED_SUCCEEDED]")
            .field("startup", &"[CHECKED]")
            .finish()
    }
}

/// Fail-closed result without a lower error source, path, or credential.
pub(crate) struct MaintenanceDriverFailure {
    operation_id: OfflineMaintenanceOperationId,
    failure: OfflineMaintenanceReceiptFailureV1,
    terminal_receipt: Option<Box<OfflineMaintenanceReceiptV1>>,
}

impl MaintenanceDriverFailure {
    #[must_use]
    pub(crate) const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        self.operation_id
    }

    #[must_use]
    pub(crate) const fn failure(&self) -> OfflineMaintenanceReceiptFailureV1 {
        self.failure
    }

    /// Returns a failed-closed receipt only when its replacement completed durably.
    #[must_use]
    pub(crate) fn terminal_receipt(&self) -> Option<&OfflineMaintenanceReceiptV1> {
        self.terminal_receipt.as_deref()
    }

    fn without_receipt(
        operation_id: OfflineMaintenanceOperationId,
        failure: OfflineMaintenanceReceiptFailureV1,
    ) -> Self {
        Self {
            operation_id,
            failure,
            terminal_receipt: None,
        }
    }
}

impl fmt::Debug for MaintenanceDriverFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaintenanceDriverFailure")
            .field("operation_id", &self.operation_id)
            .field("failure", &self.failure)
            .field(
                "terminal_receipt",
                &self.terminal_receipt.as_ref().map(|_| "[DURABLE]"),
            )
            .finish()
    }
}

/// Durably records entry into the drain interval.
///
/// The lifecycle must already have been claimed by `MaintenanceLifecycle::begin`
/// or exact receipt reacquisition. This function performs no drain itself.
pub(crate) fn mark_draining(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    operation_id: OfflineMaintenanceOperationId,
) -> Result<OfflineMaintenanceReceiptV1, MaintenanceDriverFailure> {
    if !matches!(
        lifecycle.claim_nonterminal_receipt(operation_id),
        Ok(MaintenanceReceiptClaim::Reacquired | MaintenanceReceiptClaim::AlreadyActive)
    ) || lifecycle.stage() != MaintenanceLifecycleStage::Draining
    {
        lifecycle.fail_closed(operation_id);
        return Err(MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }
    let mut receipt = read_required_receipt(storage, operation_id).map_err(|fault| {
        lifecycle.fail_closed(operation_id);
        MaintenanceDriverFailure::without_receipt(operation_id, fault.receipt_failure())
    })?;
    match receipt.current_phase() {
        OfflineMaintenanceReceiptPhaseV1::Accepted => {
            if let Err(fault) = advance_receipt(
                storage,
                &mut receipt,
                OfflineMaintenanceReceiptPhaseV1::Draining,
            ) {
                lifecycle.fail_closed(operation_id);
                return Err(fail_receipt(storage, receipt, fault.receipt_failure()));
            }
        }
        OfflineMaintenanceReceiptPhaseV1::Draining
        | OfflineMaintenanceReceiptPhaseV1::Offline
        | OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
        | OfflineMaintenanceReceiptPhaseV1::Validating => {}
        OfflineMaintenanceReceiptPhaseV1::Succeeded
        | OfflineMaintenanceReceiptPhaseV1::FailedClosed => {
            lifecycle.fail_closed(operation_id);
            return Err(failure_from_terminal_or_conflict(receipt));
        }
    }
    Ok(receipt)
}

/// Durably records that every transport, worker, port, and redb handle is closed.
///
/// The lifecycle moves to `Offline` only after the corresponding receipt state
/// is durable. Later receipt phases are accepted for crash-resume, but never
/// regressed.
pub(crate) fn mark_offline(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    operation_id: OfflineMaintenanceOperationId,
) -> Result<OfflineMaintenanceReceiptV1, MaintenanceDriverFailure> {
    if lifecycle.claim_nonterminal_receipt(operation_id)
        != Ok(MaintenanceReceiptClaim::AlreadyActive)
        || lifecycle.stage() != MaintenanceLifecycleStage::Draining
    {
        lifecycle.fail_closed(operation_id);
        return Err(MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }
    let mut receipt = read_required_receipt(storage, operation_id).map_err(|fault| {
        lifecycle.fail_closed(operation_id);
        MaintenanceDriverFailure::without_receipt(operation_id, fault.receipt_failure())
    })?;
    if receipt.source_database_id().is_none() {
        lifecycle.fail_closed(operation_id);
        return Err(fail_receipt(
            storage,
            receipt,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }
    match receipt.current_phase() {
        OfflineMaintenanceReceiptPhaseV1::Draining => {
            if let Err(fault) = advance_receipt(
                storage,
                &mut receipt,
                OfflineMaintenanceReceiptPhaseV1::Offline,
            ) {
                lifecycle.fail_closed(operation_id);
                return Err(fail_receipt(storage, receipt, fault.receipt_failure()));
            }
        }
        OfflineMaintenanceReceiptPhaseV1::Offline
        | OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
        | OfflineMaintenanceReceiptPhaseV1::Validating => {}
        OfflineMaintenanceReceiptPhaseV1::Accepted
        | OfflineMaintenanceReceiptPhaseV1::Succeeded
        | OfflineMaintenanceReceiptPhaseV1::FailedClosed => {
            lifecycle.fail_closed(operation_id);
            return Err(failure_from_terminal_or_conflict(receipt));
        }
    }
    if lifecycle.mark_offline(operation_id).is_err() {
        lifecycle.fail_closed(operation_id);
        return Err(fail_receipt(
            storage,
            receipt,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }
    Ok(receipt)
}

/// Executes or resumes one healthy-current operation after complete quiescence.
pub(crate) fn run_offline_maintenance(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    dependencies: &MaintenanceDriverDependencies<'_>,
    request: MaintenanceDriverRequest,
) -> Result<MaintenanceDriverSuccess, MaintenanceDriverFailure> {
    let operation_id = request.operation_id();
    if lifecycle.claim_nonterminal_receipt(operation_id)
        != Ok(MaintenanceReceiptClaim::AlreadyActive)
        || lifecycle.stage() != MaintenanceLifecycleStage::Offline
    {
        lifecycle.fail_closed(operation_id);
        return Err(MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }

    let mut receipt = match read_required_receipt(storage, operation_id) {
        Ok(receipt) => receipt,
        Err(fault) => {
            lifecycle.fail_closed(operation_id);
            return Err(MaintenanceDriverFailure::without_receipt(
                operation_id,
                fault.receipt_failure(),
            ));
        }
    };
    if receipt.operation_kind() != request.operation_kind()
        || receipt.source_database_id().is_none()
    {
        lifecycle.fail_closed(operation_id);
        return Err(MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }

    let operation = match request {
        MaintenanceDriverRequest::CreateBackup { .. } => {
            run_create(storage, lifecycle, dependencies, &mut receipt)
        }
        MaintenanceDriverRequest::RestoreBackup { credential, .. } => run_restore(
            storage,
            lifecycle,
            dependencies,
            &mut receipt,
            Some(credential),
            None,
        ),
        MaintenanceDriverRequest::ResumePublishedRestore { .. } => {
            run_restore(storage, lifecycle, dependencies, &mut receipt, None, None)
        }
    };
    match operation {
        Ok(startup) => Ok(MaintenanceDriverSuccess { receipt, startup }),
        Err(fault) => {
            lifecycle.fail_closed(operation_id);
            Err(fail_receipt(storage, receipt, fault.receipt_failure()))
        }
    }
}

/// Executes or resumes the sole staged-only recovery operation.
///
/// A new receipt is not admitted until the immutable backup has passed full
/// startup validation and the retained bearer has independently passed staged
/// authentication and authorization. An incomplete source-bearing receipt is
/// deliberately not adopted by this route.
pub(crate) fn run_recovery_restore(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    dependencies: &MaintenanceDriverDependencies<'_>,
    invocation: RecoveryMaintenanceDriverRequest,
) -> Result<MaintenanceDriverSuccess, MaintenanceDriverFailure> {
    let RecoveryMaintenanceDriverRequest {
        request,
        credential,
    } = invocation;
    let operation_id = request.operation_id();

    let existing = match storage.read_receipt(operation_id) {
        Ok(existing) => existing,
        Err(_) => {
            return Err(MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
            ));
        }
    };
    let pre_receipt_attempt = existing.is_none();
    if let Some(receipt) = existing.as_ref()
        && (!receipt_matches_restore_request(receipt, &request)
            || receipt.source_database_id().is_some())
    {
        return Err(MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }

    let configured_target_matches = if existing.is_some() {
        match reconcile_operation(storage, operation_id) {
            Ok(evidence) => evidence.configured_target_matches(),
            Err(fault) => {
                return Err(MaintenanceDriverFailure::without_receipt(
                    operation_id,
                    fault.receipt_failure(),
                ));
            }
        }
    } else {
        false
    };
    let needs_staged_safe_point = existing.as_ref().is_none_or(|receipt| {
        matches!(
            receipt.current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Accepted
                | OfflineMaintenanceReceiptPhaseV1::Draining
                | OfflineMaintenanceReceiptPhaseV1::Offline
        ) && !configured_target_matches
    });
    let prepared = if needs_staged_safe_point {
        let Some(credential) = credential else {
            return Err(MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::StagedAuthorizationFailed,
            ));
        };
        let stage =
            match storage.stage_recovery_restore_candidate(operation_id, request.backup_name()) {
                Ok(stage) => stage,
                Err(error) => {
                    return Err(MaintenanceDriverFailure::without_receipt(
                        operation_id,
                        artifact_storage_failure(&error),
                    ));
                }
            };
        match validate_and_authorize_stage(
            stage,
            credential,
            operation_id,
            request.input_hash(),
            dependencies,
        ) {
            Ok(prepared) => Some(prepared),
            Err(fault) => {
                if pre_receipt_attempt
                    && storage
                        .discard_pre_receipt_recovery_stage(operation_id)
                        .is_err()
                {
                    return Err(MaintenanceDriverFailure::without_receipt(
                        operation_id,
                        OfflineMaintenanceReceiptFailureV1::InternalFailure,
                    ));
                }
                return Err(MaintenanceDriverFailure::without_receipt(
                    operation_id,
                    fault.receipt_failure(),
                ));
            }
        }
    } else {
        drop(credential);
        None
    };

    let lifecycle_claimed = if lifecycle.stage() == MaintenanceLifecycleStage::FailedClosed {
        lifecycle.begin_recovery_restore(operation_id)
    } else if lifecycle.stage() == MaintenanceLifecycleStage::Offline
        && lifecycle.claim_nonterminal_receipt(operation_id)
            == Ok(MaintenanceReceiptClaim::AlreadyActive)
    {
        Ok(())
    } else {
        Err(crate::maintenance_lifecycle::MaintenanceLifecycleError)
    };
    if lifecycle_claimed.is_err() {
        lifecycle.fail_closed(operation_id);
        return Err(MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }

    let mut receipt = match existing {
        Some(receipt) => receipt,
        None => {
            let admission = prepared
                .as_ref()
                .map(|stage| stage.admission.clone())
                .expect("the new recovery receipt has a fresh staged admission");
            let candidate = match OfflineMaintenanceReceiptV1::accepted(
                operation_id,
                OfflineMaintenanceOperationKind::RestoreBackup,
                request.backup_name().clone(),
                request.input_hash(),
                request.confirmation(),
                admission,
            ) {
                Ok(candidate) => candidate,
                Err(_) => {
                    lifecycle.fail_closed(operation_id);
                    return Err(MaintenanceDriverFailure::without_receipt(
                        operation_id,
                        OfflineMaintenanceReceiptFailureV1::InternalFailure,
                    ));
                }
            };
            match storage.create_or_read_receipt(&candidate) {
                Ok(OfflineMaintenanceReceiptCreateResultV1::Created) => candidate,
                Ok(OfflineMaintenanceReceiptCreateResultV1::Existing(existing))
                    if receipt_matches_restore_request(&existing, &request)
                        && existing.source_database_id().is_none() =>
                {
                    *existing
                }
                Ok(OfflineMaintenanceReceiptCreateResultV1::Existing(_)) => {
                    lifecycle.fail_closed(operation_id);
                    return Err(MaintenanceDriverFailure::without_receipt(
                        operation_id,
                        OfflineMaintenanceReceiptFailureV1::InternalFailure,
                    ));
                }
                Err(_) => {
                    lifecycle.fail_closed(operation_id);
                    return Err(resolve_uncertain_receipt_creation(
                        storage,
                        candidate,
                        OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
                    ));
                }
            }
        }
    };

    if matches!(
        receipt.current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Accepted | OfflineMaintenanceReceiptPhaseV1::Draining
    ) && let Err(fault) = advance_receipt(
        storage,
        &mut receipt,
        OfflineMaintenanceReceiptPhaseV1::Offline,
    ) {
        lifecycle.fail_closed(operation_id);
        return Err(fail_receipt(storage, receipt, fault.receipt_failure()));
    }

    let operation = run_restore(
        storage,
        lifecycle,
        dependencies,
        &mut receipt,
        None,
        prepared,
    );
    match operation {
        Ok(startup) => Ok(MaintenanceDriverSuccess { receipt, startup }),
        Err(fault) => {
            lifecycle.fail_closed(operation_id);
            Err(fail_receipt(storage, receipt, fault.receipt_failure()))
        }
    }
}

fn run_create(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    dependencies: &MaintenanceDriverDependencies<'_>,
    receipt: &mut OfflineMaintenanceReceiptV1,
) -> Result<CheckedRedbStartup, DriverFault> {
    let evidence = reconcile_operation(storage, receipt.operation_id())?;
    match receipt.current_phase() {
        OfflineMaintenanceReceiptPhaseV1::Offline => {
            let build = production_backup_build_metadata()?;
            let (manifest, identity) = storage
                .create_named_backup(receipt.operation_id(), receipt.backup_name(), &build)
                .map_err(DriverFault::ArtifactStorage)?;
            if manifest.database_id() != identity.database_id()
                || manifest.last_commit_sequence() != identity.included_application_frontier()
                || receipt.source_database_id() != Some(identity.database_id())
            {
                return Err(DriverFault::ArtifactInvalid);
            }
            update_receipt(storage, receipt, |candidate| {
                candidate.record_manifest_identity(identity)
            })?;
            advance_receipt(
                storage,
                receipt,
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
            )?;
        }
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
        | OfflineMaintenanceReceiptPhaseV1::Validating
        | OfflineMaintenanceReceiptPhaseV1::Succeeded => {
            if !evidence.named_backup_matches() {
                return Err(DriverFault::ArtifactInvalid);
            }
        }
        OfflineMaintenanceReceiptPhaseV1::Accepted
        | OfflineMaintenanceReceiptPhaseV1::Draining
        | OfflineMaintenanceReceiptPhaseV1::FailedClosed => {
            return Err(DriverFault::ReceiptIntegrity);
        }
    }
    complete_post_publication_validation(storage, lifecycle, dependencies, receipt)
}

fn run_restore(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    dependencies: &MaintenanceDriverDependencies<'_>,
    receipt: &mut OfflineMaintenanceReceiptV1,
    credential: Option<RetainedOpaqueCredential>,
    prepared: Option<PreparedStagedRestore>,
) -> Result<CheckedRedbStartup, DriverFault> {
    // A freshly prepared stage is already sealed after complete validation.
    // Reopening it here could change backend-private bytes and invalidate that
    // exact publication seal. Other paths still require fresh evidence.
    let evidence = if prepared.is_some()
        && receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Offline
    {
        None
    } else {
        Some(reconcile_operation(storage, receipt.operation_id())?)
    };
    match receipt.current_phase() {
        OfflineMaintenanceReceiptPhaseV1::Offline
            if evidence
                .is_some_and(RedbMaintenanceOperationEvidence::configured_target_matches) =>
        {
            drop(credential);
            drop(prepared);
            if receipt.manifest_identity().is_none() || receipt.staged_database_id().is_none() {
                return Err(DriverFault::ReceiptIntegrity);
            }
            advance_receipt(
                storage,
                receipt,
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
            )?;
        }
        OfflineMaintenanceReceiptPhaseV1::Offline => {
            let prepared = match prepared {
                Some(prepared) => {
                    drop(credential);
                    prepared
                }
                None => {
                    let credential = credential.ok_or(DriverFault::StagedAuthorization)?;
                    let stage = storage
                        .stage_restore(receipt.operation_id(), receipt.backup_name())
                        .map_err(DriverFault::ArtifactStorage)?;
                    validate_and_authorize_stage(
                        stage,
                        credential,
                        receipt.operation_id(),
                        receipt.input_hash(),
                        dependencies,
                    )?
                }
            };
            let manifest_identity = prepared.sealed.manifest_identity().clone();
            update_receipt(storage, receipt, |candidate| {
                candidate.record_staged_database_id(manifest_identity.database_id())?;
                candidate.record_manifest_identity(manifest_identity.clone())
            })?;
            let overwrite_policy = overwrite_policy(receipt.replacement_confirmation());
            let publication = storage.publish_sealed_restore(prepared.sealed, overwrite_policy);
            let manifest = match publication {
                Ok(OfflineRestoreResultV1::Restored { manifest }) => *manifest,
                Ok(OfflineRestoreResultV1::TargetNotEmpty) => {
                    return Err(DriverFault::ArtifactUnavailable);
                }
                Err(error) if error.kind() == StorageErrorKind::CommitStatusUnknown => {
                    let recovered = reconcile_operation(storage, receipt.operation_id())?;
                    if !recovered.configured_target_matches() {
                        return Err(DriverFault::PublicationUncertain);
                    }
                    advance_receipt(
                        storage,
                        receipt,
                        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
                    )?;
                    return complete_post_publication_validation(
                        storage,
                        lifecycle,
                        dependencies,
                        receipt,
                    );
                }
                Err(error) => return Err(DriverFault::ArtifactStorage(error)),
            };
            let expected = receipt
                .manifest_identity()
                .ok_or(DriverFault::ReceiptIntegrity)?;
            if manifest.database_id() != expected.database_id()
                || manifest.last_commit_sequence() != expected.included_application_frontier()
            {
                return Err(DriverFault::ArtifactInvalid);
            }
            advance_receipt(
                storage,
                receipt,
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
            )?;
        }
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
        | OfflineMaintenanceReceiptPhaseV1::Validating
        | OfflineMaintenanceReceiptPhaseV1::Succeeded => {
            drop(credential);
            drop(prepared);
            if !evidence.is_some_and(RedbMaintenanceOperationEvidence::configured_target_matches)
                || receipt.manifest_identity().is_none()
                || receipt.staged_database_id().is_none()
            {
                return Err(DriverFault::ArtifactInvalid);
            }
        }
        OfflineMaintenanceReceiptPhaseV1::Accepted
        | OfflineMaintenanceReceiptPhaseV1::Draining
        | OfflineMaintenanceReceiptPhaseV1::FailedClosed => {
            drop(credential);
            drop(prepared);
            return Err(DriverFault::ReceiptIntegrity);
        }
    }
    complete_post_publication_validation(storage, lifecycle, dependencies, receipt)
}

fn complete_post_publication_validation(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    dependencies: &MaintenanceDriverDependencies<'_>,
    receipt: &mut OfflineMaintenanceReceiptV1,
) -> Result<CheckedRedbStartup, DriverFault> {
    match receipt.current_phase() {
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished => {
            advance_receipt(
                storage,
                receipt,
                OfflineMaintenanceReceiptPhaseV1::Validating,
            )?;
        }
        OfflineMaintenanceReceiptPhaseV1::Validating
        | OfflineMaintenanceReceiptPhaseV1::Succeeded => {}
        _ => return Err(DriverFault::ReceiptIntegrity),
    }
    mark_lifecycle_validating(lifecycle, receipt.operation_id())?;

    let startup = open_redb_startup(
        storage.configured_database_file(),
        dependencies.startup_inputs.clone(),
        &dependencies.database_ids,
    )
    .map_err(|_| DriverFault::Validation)?;
    let expected_database_id = match receipt.operation_kind() {
        OfflineMaintenanceOperationKind::CreateBackup => receipt.source_database_id(),
        OfflineMaintenanceOperationKind::RestoreBackup => receipt.staged_database_id(),
    }
    .ok_or(DriverFault::ReceiptIntegrity)?;
    if startup.database_id() != expected_database_id
        || receipt
            .manifest_identity()
            .is_none_or(|manifest| manifest.database_id() != startup.database_id())
    {
        return Err(DriverFault::ValidationIdentity);
    }
    dependencies
        .recovery
        .reached(MaintenanceRecoveryBoundary::FreshValidationComplete);

    match receipt.current_phase() {
        OfflineMaintenanceReceiptPhaseV1::Validating => {
            advance_receipt(
                storage,
                receipt,
                OfflineMaintenanceReceiptPhaseV1::Succeeded,
            )?;
        }
        OfflineMaintenanceReceiptPhaseV1::Succeeded => {}
        _ => return Err(DriverFault::ReceiptIntegrity),
    }
    Ok(startup)
}

fn validate_and_authorize_stage(
    stage: RedbStagedRestore,
    credential: RetainedOpaqueCredential,
    operation_id: OfflineMaintenanceOperationId,
    input_hash: riffdb_types::OfflineMaintenanceInputHash,
    dependencies: &MaintenanceDriverDependencies<'_>,
) -> Result<PreparedStagedRestore, DriverFault> {
    let staged_startup = open_redb_startup(
        stage.staged_database_file(),
        dependencies.startup_inputs.clone(),
        &dependencies.database_ids,
    )
    .map_err(|_| DriverFault::Validation)?;
    if staged_startup.database_id() != stage.manifest_identity().database_id() {
        return Err(DriverFault::ValidationIdentity);
    }

    let (
        staged_database_id,
        _retained_metadata,
        _catalog_history,
        _startup_lifecycle,
        _allocator_capacity,
        operational_ports,
    ) = staged_startup.into_parts();
    let storage = SharedRedbOperationalPorts::new(operational_ports);
    let authenticator = ServerCredentialAuthenticator::new(
        storage.clone(),
        Arc::clone(&dependencies.capability_keys),
        dependencies.clocks.authentication(),
        Arc::clone(&dependencies.authentication_telemetry),
    );
    let authentication = riffdb_auth::AuthenticationContext::new(
        staged_database_id,
        dependencies.environment.clone(),
        dependencies.grpc_audience.clone(),
    );
    let principal = authenticator
        .authenticate(credential.borrow(), &authentication)
        .map_err(|_| DriverFault::StagedAuthorization)?;
    let policy_request =
        OfflineMaintenanceAuthorizationRequest::restore_backup(operation_id, input_hash);
    let policy = ServerCurrentPolicyPort::new(
        storage.clone(),
        dependencies.clocks.authorization(),
        staged_database_id,
        dependencies.environment.clone(),
        dependencies.trusted_audiences.clone(),
        Arc::clone(&dependencies.authorization_telemetry),
    );
    let proof = match policy
        .authorize_offline_maintenance(&principal, policy_request.clone())
        .map_err(|_| DriverFault::StagedAuthorization)?
    {
        OfflineMaintenanceDecision::Allow(proof) => proof,
        OfflineMaintenanceDecision::Deny(_) => return Err(DriverFault::StagedAuthorization),
    };
    if !valid_staged_authorization(
        &proof,
        &principal,
        staged_database_id,
        &dependencies.environment,
        &policy_request,
    ) {
        return Err(DriverFault::StagedAuthorization);
    }
    let admission = OfflineMaintenanceAdmissionV1::new(
        proof.principal_id().clone(),
        proof.actor_kind(),
        proof.authorizing_capability_id(),
        proof.obligations().validated_approval().cloned(),
    );

    // These explicit drops make the publication boundary auditable: neither a
    // bearer, proof, principal, adapter, nor open staged database survives it.
    drop(proof);
    drop(principal);
    drop(policy);
    drop(authenticator);
    drop(storage);
    drop(credential);
    dependencies
        .recovery
        .reached(MaintenanceRecoveryBoundary::StagedAuthorizationComplete);
    let sealed = stage
        .seal_after_validation(staged_database_id)
        .map_err(DriverFault::ArtifactStorage)?;
    Ok(PreparedStagedRestore { sealed, admission })
}

fn valid_staged_authorization(
    proof: &AuthorizedOfflineMaintenance,
    principal: &riffdb_auth::AuthenticatedPrincipal,
    database_id: riffdb_types::DatabaseId,
    environment: &Environment,
    request: &OfflineMaintenanceAuthorizationRequest,
) -> bool {
    let obligations = proof.obligations();
    proof.database_id() == database_id
        && proof.environment() == environment
        && proof.request() == request
        && proof.authorizing_capability_id() == principal.capability_id()
        && proof.authorizing_capability_revision() == principal.capability_revision()
        && proof.principal_id() == principal.principal_id()
        && proof.actor_kind() == principal.actor_kind()
        && obligations.effective_tenant_scope() == &TenantScope::Global
        && obligations.partition_constraint().is_none()
        && obligations.field_mask().is_none()
        && obligations.row_limit().is_none()
        && obligations.audit_class().is_none()
        && obligations.output_classification() == OutputClassification::AdministrativeRedactedData
}

fn reconcile_operation(
    storage: &mut RedbMaintenanceStorage,
    operation_id: OfflineMaintenanceOperationId,
) -> Result<RedbMaintenanceOperationEvidence, DriverFault> {
    let reconciliation = storage.reconcile().map_err(DriverFault::ArtifactStorage)?;
    reconciliation
        .operations()
        .iter()
        .copied()
        .find(|evidence| evidence.operation_id() == operation_id)
        .ok_or(DriverFault::ReceiptIntegrity)
}

fn read_required_receipt(
    storage: &mut RedbMaintenanceStorage,
    operation_id: OfflineMaintenanceOperationId,
) -> Result<OfflineMaintenanceReceiptV1, DriverFault> {
    storage
        .read_receipt(operation_id)
        .map_err(|_| DriverFault::ReceiptStorage)?
        .ok_or(DriverFault::ReceiptIntegrity)
}

fn update_receipt(
    storage: &mut RedbMaintenanceStorage,
    receipt: &mut OfflineMaintenanceReceiptV1,
    update: impl FnOnce(&mut OfflineMaintenanceReceiptV1) -> Result<(), StorageValueError>,
) -> Result<(), DriverFault> {
    let mut candidate = receipt.clone();
    update(&mut candidate).map_err(|_| DriverFault::ReceiptValue)?;
    storage
        .replace_receipt(&candidate)
        .map_err(|_| DriverFault::ReceiptStorage)?;
    *receipt = candidate;
    Ok(())
}

fn advance_receipt(
    storage: &mut RedbMaintenanceStorage,
    receipt: &mut OfflineMaintenanceReceiptV1,
    phase: OfflineMaintenanceReceiptPhaseV1,
) -> Result<(), DriverFault> {
    if receipt.current_phase() == phase {
        return Ok(());
    }
    update_receipt(storage, receipt, |candidate| {
        candidate.advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
    })
}

fn mark_lifecycle_validating(
    lifecycle: &MaintenanceLifecycle,
    operation_id: OfflineMaintenanceOperationId,
) -> Result<(), DriverFault> {
    if lifecycle.claim_nonterminal_receipt(operation_id)
        != Ok(MaintenanceReceiptClaim::AlreadyActive)
    {
        return Err(DriverFault::Lifecycle);
    }
    match lifecycle.stage() {
        MaintenanceLifecycleStage::Offline => lifecycle
            .mark_validating(operation_id)
            .map_err(|_| DriverFault::Lifecycle),
        MaintenanceLifecycleStage::Validating => Ok(()),
        _ => Err(DriverFault::Lifecycle),
    }
}

fn fail_receipt(
    storage: &mut RedbMaintenanceStorage,
    receipt: OfflineMaintenanceReceiptV1,
    failure: OfflineMaintenanceReceiptFailureV1,
) -> MaintenanceDriverFailure {
    let operation_id = receipt.operation_id();
    if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::FailedClosed {
        return failure_from_terminal_or_conflict(receipt);
    }
    if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Succeeded {
        return MaintenanceDriverFailure::without_receipt(operation_id, failure);
    }

    let mut candidate = receipt;
    if candidate
        .advance(OfflineMaintenanceReceiptTransitionV1::failed(failure))
        .is_err()
    {
        return MaintenanceDriverFailure::without_receipt(operation_id, failure);
    }
    match storage.replace_receipt(&candidate) {
        Ok(_) => MaintenanceDriverFailure {
            operation_id,
            failure,
            terminal_receipt: Some(Box::new(candidate)),
        },
        Err(_) => MaintenanceDriverFailure::without_receipt(operation_id, failure),
    }
}

fn failure_from_terminal_or_conflict(
    receipt: OfflineMaintenanceReceiptV1,
) -> MaintenanceDriverFailure {
    let operation_id = receipt.operation_id();
    if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::FailedClosed {
        let failure = receipt
            .transitions()
            .last()
            .and_then(|transition| transition.failure())
            .unwrap_or(OfflineMaintenanceReceiptFailureV1::InternalFailure);
        MaintenanceDriverFailure {
            operation_id,
            failure,
            terminal_receipt: Some(Box::new(receipt)),
        }
    } else {
        MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        )
    }
}

fn resolve_uncertain_receipt_creation(
    storage: &mut RedbMaintenanceStorage,
    candidate: OfflineMaintenanceReceiptV1,
    failure: OfflineMaintenanceReceiptFailureV1,
) -> MaintenanceDriverFailure {
    match storage.read_receipt(candidate.operation_id()) {
        Ok(Some(existing)) if existing == candidate => fail_receipt(storage, existing, failure),
        _ => MaintenanceDriverFailure::without_receipt(candidate.operation_id(), failure),
    }
}

pub(crate) fn receipt_matches_restore_request(
    receipt: &OfflineMaintenanceReceiptV1,
    request: &RestoreOfflineBackupRequest,
) -> bool {
    receipt.operation_id() == request.operation_id()
        && receipt.operation_kind() == OfflineMaintenanceOperationKind::RestoreBackup
        && receipt.backup_name() == request.backup_name()
        && receipt.input_hash() == request.input_hash()
        && receipt.replacement_confirmation() == request.confirmation()
}

fn overwrite_policy(
    confirmation: OfflineMaintenanceReplacementConfirmation,
) -> OfflineRestoreOverwritePolicyV1 {
    match confirmation {
        OfflineMaintenanceReplacementConfirmation::NotProvided => {
            OfflineRestoreOverwritePolicyV1::RefuseNonEmpty
        }
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget => {
            OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive
        }
    }
}

fn production_backup_build_metadata() -> Result<BackupBuildMetadataV1, DriverFault> {
    BackupBuildMetadataV1::new(
        env!("CARGO_PKG_VERSION"),
        option_env!("RIFFDB_GIT_REVISION").unwrap_or("development-unversioned"),
        "rustc-1.97.0",
        EXECUTABLE_IR_VERSION_V1,
        Vec::new(),
    )
    .map_err(|_| DriverFault::ReceiptValue)
}

fn artifact_storage_failure(error: &StorageError) -> OfflineMaintenanceReceiptFailureV1 {
    match error.kind() {
        StorageErrorKind::CorruptData
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::LimitExceeded => OfflineMaintenanceReceiptFailureV1::ArtifactInvalid,
        StorageErrorKind::Unavailable => OfflineMaintenanceReceiptFailureV1::ArtifactUnavailable,
        StorageErrorKind::CommitStatusUnknown => {
            OfflineMaintenanceReceiptFailureV1::StorageUnavailable
        }
        StorageErrorKind::InvariantViolation | StorageErrorKind::SequenceExhausted => {
            OfflineMaintenanceReceiptFailureV1::InternalFailure
        }
    }
}

struct PreparedStagedRestore {
    sealed: RedbSealedStagedRestore,
    admission: OfflineMaintenanceAdmissionV1,
}

impl fmt::Debug for PreparedStagedRestore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedStagedRestore([REDACTED])")
    }
}

enum DriverFault {
    ArtifactUnavailable,
    ArtifactInvalid,
    ArtifactStorage(StorageError),
    PublicationUncertain,
    StagedAuthorization,
    Validation,
    ValidationIdentity,
    ReceiptStorage,
    ReceiptValue,
    ReceiptIntegrity,
    Lifecycle,
}

impl DriverFault {
    fn receipt_failure(&self) -> OfflineMaintenanceReceiptFailureV1 {
        match self {
            Self::ArtifactUnavailable => OfflineMaintenanceReceiptFailureV1::ArtifactUnavailable,
            Self::ArtifactInvalid => OfflineMaintenanceReceiptFailureV1::ArtifactInvalid,
            Self::ArtifactStorage(error) => artifact_storage_failure(error),
            Self::PublicationUncertain => OfflineMaintenanceReceiptFailureV1::StorageUnavailable,
            Self::StagedAuthorization => {
                OfflineMaintenanceReceiptFailureV1::StagedAuthorizationFailed
            }
            Self::Validation | Self::ValidationIdentity => {
                OfflineMaintenanceReceiptFailureV1::ValidationFailed
            }
            Self::ReceiptStorage => OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
            Self::ReceiptValue | Self::ReceiptIntegrity | Self::Lifecycle => {
                OfflineMaintenanceReceiptFailureV1::InternalFailure
            }
        }
    }
}

impl fmt::Debug for DriverFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ArtifactUnavailable => "DriverFault::ArtifactUnavailable",
            Self::ArtifactInvalid => "DriverFault::ArtifactInvalid",
            Self::ArtifactStorage(_) => "DriverFault::ArtifactStorage([REDACTED])",
            Self::PublicationUncertain => "DriverFault::PublicationUncertain",
            Self::StagedAuthorization => "DriverFault::StagedAuthorization",
            Self::Validation => "DriverFault::Validation([REDACTED])",
            Self::ValidationIdentity => "DriverFault::ValidationIdentity",
            Self::ReceiptStorage => "DriverFault::ReceiptStorage([REDACTED])",
            Self::ReceiptValue => "DriverFault::ReceiptValue([REDACTED])",
            Self::ReceiptIntegrity => "DriverFault::ReceiptIntegrity",
            Self::Lifecycle => "DriverFault::Lifecycle",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_confirmation_maps_only_to_the_two_backend_policies() {
        assert_eq!(
            overwrite_policy(OfflineMaintenanceReplacementConfirmation::NotProvided),
            OfflineRestoreOverwritePolicyV1::RefuseNonEmpty
        );
        assert_eq!(
            overwrite_policy(OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget),
            OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive
        );
    }

    #[test]
    fn production_backup_metadata_matches_the_release_build_identity() {
        let metadata = production_backup_build_metadata().expect("checked production metadata");
        assert_eq!(metadata.semantic_version(), env!("CARGO_PKG_VERSION"));
        assert_eq!(
            metadata.git_revision(),
            option_env!("RIFFDB_GIT_REVISION").unwrap_or("development-unversioned")
        );
        assert_eq!(metadata.rust_version(), "rustc-1.97.0");
        assert_eq!(metadata.executable_ir_version(), EXECUTABLE_IR_VERSION_V1);
        assert!(metadata.enabled_features().is_empty());
    }

    #[test]
    fn private_debug_shapes_never_expose_credentials_or_paths() {
        let operation_id =
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [7; 10])
                .expect("valid operation ID");
        let failure = MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        );
        let credential =
            RetainedOpaqueCredential::new(b"secret-presentation").expect("bounded credential");
        let request = MaintenanceDriverRequest::restore_backup(operation_id, credential);

        assert!(!format!("{failure:?}").contains("secret"));
        assert_eq!(
            format!("{request:?}"),
            "MaintenanceDriverRequest::RestoreBackup([REDACTED])"
        );
    }
}
