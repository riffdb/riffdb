#![expect(
    clippy::expect_used,
    reason = "an admitted maintenance operation retains its validated operation-specific input"
)]

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
    OfflineMaintenanceReceiptV1, OfflineMaintenanceReceiptV2, OfflineRestoreOverwritePolicyV1,
    OfflineRestoreResultV1, StartupValidationInputs, StorageError, StorageErrorKind,
    StorageValueError,
};
use riffdb_storage_redb::{
    RedbCommitProfile, RedbMaintenanceOperationEvidence, RedbMaintenanceStorage,
    RedbSealedStagedRestore, RedbStagedRestore, read_history_incarnation,
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
use crate::startup::{CheckedRedbStartup, open_redb_startup_with_commit_profile};
use crate::storage::SharedRedbOperationalPorts;

/// Production dependencies needed only while a private staged database is open.
pub(crate) struct MaintenanceDriverDependencies<'a> {
    startup_inputs: StartupValidationInputs,
    database_ids: DatabaseIdCandidateSource,
    application_commit_profile: RedbCommitProfile,
    capability_keys: Arc<CapabilityDigestKeyProvider>,
    environment: Environment,
    grpc_audience: Audience,
    trusted_audiences: TrustedAudienceCatalog,
    clocks: &'a ProductionWallClocks,
    authentication_telemetry: Arc<dyn AuthenticationTelemetry>,
    authorization_telemetry: Arc<dyn AuthorizationTelemetry>,
    recovery: &'a MaintenanceRecoveryController,
    /// History incarnation retained from a successful open of this process's
    /// configured target. `None` in recovery when startup never read the target.
    retained_target_history_incarnation: Option<u64>,
    /// Optional process metrics for operator-visible unproven-bump counts.
    metrics: Option<riffdb_observability::MetricRegistry>,
}

impl<'a> MaintenanceDriverDependencies<'a> {
    /// Captures value-only validation facts and the existing production auth adapters.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        startup_inputs: StartupValidationInputs,
        database_ids: DatabaseIdCandidateSource,
        application_commit_profile: RedbCommitProfile,
        capability_keys: Arc<CapabilityDigestKeyProvider>,
        environment: Environment,
        grpc_audience: Audience,
        trusted_audiences: TrustedAudienceCatalog,
        clocks: &'a ProductionWallClocks,
        authentication_telemetry: Arc<dyn AuthenticationTelemetry>,
        authorization_telemetry: Arc<dyn AuthorizationTelemetry>,
        recovery: &'a MaintenanceRecoveryController,
        retained_target_history_incarnation: Option<u64>,
        metrics: Option<riffdb_observability::MetricRegistry>,
    ) -> Self {
        Self {
            startup_inputs,
            database_ids,
            application_commit_profile,
            capability_keys,
            environment,
            grpc_audience,
            trusted_audiences,
            clocks,
            authentication_telemetry,
            authorization_telemetry,
            recovery,
            retained_target_history_incarnation,
            metrics,
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
    RetireBackup {
        operation_id: OfflineMaintenanceOperationId,
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

    #[must_use]
    pub(crate) const fn retire_backup(operation_id: OfflineMaintenanceOperationId) -> Self {
        Self::RetireBackup { operation_id }
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
            | Self::RetireBackup { operation_id }
            | Self::ResumePublishedRestore { operation_id } => *operation_id,
        }
    }

    const fn operation_kind(&self) -> OfflineMaintenanceOperationKind {
        match self {
            Self::CreateBackup { .. } => OfflineMaintenanceOperationKind::CreateBackup,
            Self::RestoreBackup { .. } | Self::ResumePublishedRestore { .. } => {
                OfflineMaintenanceOperationKind::RestoreBackup
            }
            Self::RetireBackup { .. } => OfflineMaintenanceOperationKind::RetireBackup,
        }
    }
}

impl fmt::Debug for MaintenanceDriverRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CreateBackup { .. } => "MaintenanceDriverRequest::CreateBackup([REDACTED])",
            Self::RestoreBackup { .. } => "MaintenanceDriverRequest::RestoreBackup([REDACTED])",
            Self::RetireBackup { .. } => "MaintenanceDriverRequest::RetireBackup([REDACTED])",
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
    receipt: MaintenanceTerminalReceipt,
    startup: CheckedRedbStartup,
}

impl MaintenanceDriverSuccess {
    /// Separates the durable terminal receipt from the newly activated storage ports.
    #[must_use]
    pub(crate) fn into_parts(self) -> (MaintenanceTerminalReceipt, CheckedRedbStartup) {
        (self.receipt, self.startup)
    }
}

/// Exact terminal receipt version produced by one maintenance driver.
pub(crate) enum MaintenanceTerminalReceipt {
    V1(OfflineMaintenanceReceiptV1),
    V2(OfflineMaintenanceReceiptV2),
}

impl MaintenanceTerminalReceipt {
    const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        match self {
            Self::V1(receipt) => receipt.operation_id(),
            Self::V2(receipt) => receipt.operation_id(),
        }
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
) -> Result<(), MaintenanceDriverFailure> {
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
    let receipt = storage.read_receipt(operation_id).map_err(|_| {
        lifecycle.fail_closed(operation_id);
        MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
        )
    })?;
    let Some(mut receipt) = receipt else {
        let retirement = storage
            .read_retire_receipt(operation_id)
            .map_err(|_| {
                MaintenanceDriverFailure::without_receipt(
                    operation_id,
                    OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
                )
            })?
            .ok_or_else(|| {
                MaintenanceDriverFailure::without_receipt(
                    operation_id,
                    OfflineMaintenanceReceiptFailureV1::InternalFailure,
                )
            })?;
        if !matches!(
            retirement.current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Draining
                | OfflineMaintenanceReceiptPhaseV1::Offline
                | OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                | OfflineMaintenanceReceiptPhaseV1::Validating
        ) {
            return Err(MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::InternalFailure,
            ));
        }
        return Ok(());
    };
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
    Ok(())
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
) -> Result<(), MaintenanceDriverFailure> {
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

    let receipt = storage.read_receipt(operation_id).map_err(|_| {
        lifecycle.fail_closed(operation_id);
        MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
        )
    })?;
    let Some(mut receipt) = receipt else {
        return mark_retirement_offline(storage, lifecycle, operation_id);
    };
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
    Ok(())
}

fn mark_retirement_offline(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    operation_id: OfflineMaintenanceOperationId,
) -> Result<(), MaintenanceDriverFailure> {
    let mut receipt = storage
        .read_retire_receipt(operation_id)
        .map_err(|_| {
            MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
            )
        })?
        .ok_or_else(|| {
            MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::InternalFailure,
            )
        })?;
    if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Draining {
        let mut offline = receipt.clone();
        offline
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::Offline,
            ))
            .map_err(|_| {
                MaintenanceDriverFailure::without_receipt(
                    operation_id,
                    OfflineMaintenanceReceiptFailureV1::InternalFailure,
                )
            })?;
        storage.replace_retire_receipt(&offline).map_err(|_| {
            MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
            )
        })?;
        receipt = offline;
    }
    if !matches!(
        receipt.current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Offline
            | OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
            | OfflineMaintenanceReceiptPhaseV1::Validating
    ) || lifecycle.mark_offline(operation_id).is_err()
    {
        lifecycle.fail_closed(operation_id);
        return Err(MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::InternalFailure,
        ));
    }
    Ok(())
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

    if matches!(request, MaintenanceDriverRequest::RetireBackup { .. }) {
        return run_retirement(storage, lifecycle, dependencies, operation_id);
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
        MaintenanceDriverRequest::RetireBackup { .. } => Err(DriverFault::ReceiptIntegrity),
    };
    match operation {
        Ok(startup) => Ok(MaintenanceDriverSuccess {
            receipt: MaintenanceTerminalReceipt::V1(receipt),
            startup,
        }),
        Err(fault) => {
            lifecycle.fail_closed(operation_id);
            Err(fail_receipt(storage, receipt, fault.receipt_failure()))
        }
    }
}

fn run_retirement(
    storage: &mut RedbMaintenanceStorage,
    lifecycle: &MaintenanceLifecycle,
    dependencies: &MaintenanceDriverDependencies<'_>,
    operation_id: OfflineMaintenanceOperationId,
) -> Result<MaintenanceDriverSuccess, MaintenanceDriverFailure> {
    let mut receipt = storage
        .read_retire_receipt(operation_id)
        .map_err(|_| {
            MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
            )
        })?
        .ok_or_else(|| {
            MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::InternalFailure,
            )
        })?;
    if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Offline {
        storage
            .publish_backup_retirement(operation_id)
            .map_err(|_| {
                MaintenanceDriverFailure::without_receipt(
                    operation_id,
                    OfflineMaintenanceReceiptFailureV1::ArtifactUnavailable,
                )
            })?;
        advance_retire_receipt(
            storage,
            &mut receipt,
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        )?;
    }
    if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::ArtifactPublished {
        advance_retire_receipt(
            storage,
            &mut receipt,
            OfflineMaintenanceReceiptPhaseV1::Validating,
        )?;
    }
    if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Validating {
        storage
            .delete_published_backup_retirement(operation_id)
            .map_err(|_| {
                MaintenanceDriverFailure::without_receipt(
                    operation_id,
                    OfflineMaintenanceReceiptFailureV1::ArtifactUnavailable,
                )
            })?;
        mark_lifecycle_validating(lifecycle, operation_id).map_err(|_| {
            MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::InternalFailure,
            )
        })?;
        let startup = open_redb_startup_with_commit_profile(
            storage.configured_database_file(),
            dependencies.startup_inputs.clone(),
            &dependencies.database_ids,
            dependencies.application_commit_profile,
        )
        .map_err(|_| {
            MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::ValidationFailed,
            )
        })?;
        if startup.database_id() != receipt.retirement().manifest_identity().database_id() {
            return Err(MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::ValidationFailed,
            ));
        }
        advance_retire_receipt(
            storage,
            &mut receipt,
            OfflineMaintenanceReceiptPhaseV1::Succeeded,
        )?;
        return Ok(MaintenanceDriverSuccess {
            receipt: MaintenanceTerminalReceipt::V2(receipt),
            startup,
        });
    }
    Err(MaintenanceDriverFailure::without_receipt(
        operation_id,
        OfflineMaintenanceReceiptFailureV1::InternalFailure,
    ))
}

fn advance_retire_receipt(
    storage: &mut RedbMaintenanceStorage,
    receipt: &mut OfflineMaintenanceReceiptV2,
    phase: OfflineMaintenanceReceiptPhaseV1,
) -> Result<(), MaintenanceDriverFailure> {
    let operation_id = receipt.operation_id();
    let mut candidate = receipt.clone();
    candidate
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
        .map_err(|_| {
            MaintenanceDriverFailure::without_receipt(
                operation_id,
                OfflineMaintenanceReceiptFailureV1::InternalFailure,
            )
        })?;
    storage.replace_retire_receipt(&candidate).map_err(|_| {
        MaintenanceDriverFailure::without_receipt(
            operation_id,
            OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable,
        )
    })?;
    *receipt = candidate;
    Ok(())
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
        Ok(startup) => Ok(MaintenanceDriverSuccess {
            receipt: MaintenanceTerminalReceipt::V1(receipt),
            startup,
        }),
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
            // Pre-fence receipt resume: recompute published incarnation when the
            // receipt field is absent (upgrade mid-restore). Target already matches
            // the staged seal; staged material is gone after prior publish attempts
            // so only retained process evidence is available as a floor.
            ensure_published_incarnation_on_receipt(
                storage,
                receipt,
                None,
                dependencies.retained_target_history_incarnation,
                dependencies.metrics.as_ref(),
            )?;
            advance_receipt(
                storage,
                receipt,
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
            )?;
        }
        OfflineMaintenanceReceiptPhaseV1::Offline => {
            let mut prepared = match prepared {
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
            let staged_incarnation = prepared.sealed.staged_history_incarnation();
            if let Some(manifest_incarnation) = prepared.sealed.manifest().history_incarnation()
                && manifest_incarnation > staged_incarnation
            {
                return Err(DriverFault::ArtifactInvalid);
            }
            // Prefer a receipt-recorded bump (exactly-once across crash-before-rename
            // resume where staged may already hold the stamped value). Otherwise
            // compute max(target, staged)+1 with corrupt-target fallback (N1).
            let published_incarnation = match receipt.published_history_incarnation() {
                Some(recorded) => recorded,
                None => {
                    // This arm is entered only when the receipt records no
                    // published incarnation, so retained and staged evidence are
                    // the only floors that can apply.
                    let target_incarnation = target_history_incarnation_for_bump(
                        storage.configured_database_file(),
                        dependencies.retained_target_history_incarnation,
                        staged_incarnation,
                        dependencies.metrics.as_ref(),
                    );
                    target_incarnation.max(staged_incarnation) + 1
                }
            };
            // Persist receipt authority before the staged stamp so a crash between
            // stamp and receipt update cannot lose the bump and double-advance.
            let manifest_identity = prepared.sealed.manifest_identity().clone();
            update_receipt(storage, receipt, |candidate| {
                candidate.record_staged_database_id(manifest_identity.database_id())?;
                candidate.record_manifest_identity(manifest_identity.clone())?;
                candidate.record_published_incarnation(published_incarnation)
            })?;
            // Stamp staged before rename so publication stays byte-identical and
            // crash-resume reconcile (checksum seal) remains valid.
            prepared
                .sealed
                .apply_published_history_incarnation(published_incarnation)
                .map_err(DriverFault::ArtifactStorage)?;
            let overwrite_policy = overwrite_policy(receipt.replacement_confirmation());
            // Pure file swap — no writes between re-seal and rename.
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
            ensure_published_incarnation_on_receipt(
                storage,
                receipt,
                None,
                dependencies.retained_target_history_incarnation,
                dependencies.metrics.as_ref(),
            )?;
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
            // Stamp already applied to staged bytes before rename (C2 option b).
            ensure_published_incarnation_on_receipt(
                storage,
                receipt,
                None,
                dependencies.retained_target_history_incarnation,
                dependencies.metrics.as_ref(),
            )?;
            advance_receipt(
                storage,
                receipt,
                OfflineMaintenanceReceiptPhaseV1::Validating,
            )?;
        }
        OfflineMaintenanceReceiptPhaseV1::Validating
        | OfflineMaintenanceReceiptPhaseV1::Succeeded => {
            ensure_published_incarnation_on_receipt(
                storage,
                receipt,
                None,
                dependencies.retained_target_history_incarnation,
                dependencies.metrics.as_ref(),
            )?;
        }
        _ => return Err(DriverFault::ReceiptIntegrity),
    }
    mark_lifecycle_validating(lifecycle, receipt.operation_id())?;

    let startup = open_redb_startup_with_commit_profile(
        storage.configured_database_file(),
        dependencies.startup_inputs.clone(),
        &dependencies.database_ids,
        dependencies.application_commit_profile,
    )
    .map_err(|_| DriverFault::Validation)?;
    if receipt.operation_kind() == OfflineMaintenanceOperationKind::RestoreBackup {
        let expected = receipt
            .published_history_incarnation()
            .ok_or(DriverFault::ReceiptIntegrity)?;
        let observed = startup.retained_metadata().history_incarnation();
        if observed != expected {
            return Err(DriverFault::Validation);
        }
    }
    let expected_database_id = match receipt.operation_kind() {
        OfflineMaintenanceOperationKind::CreateBackup => receipt.source_database_id(),
        OfflineMaintenanceOperationKind::RestoreBackup => receipt.staged_database_id(),
        OfflineMaintenanceOperationKind::RetireBackup => None,
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
    let staged_startup = open_redb_startup_with_commit_profile(
        stage.staged_database_file(),
        dependencies.startup_inputs.clone(),
        &dependencies.database_ids,
        dependencies.application_commit_profile,
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
    let storage = SharedRedbOperationalPorts::new(operational_ports, None)
        .map_err(|_| DriverFault::Validation)?;
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
    contract_migration_backup_build_metadata().map_err(|_| DriverFault::ReceiptValue)
}

pub(crate) fn contract_migration_backup_build_metadata()
-> Result<BackupBuildMetadataV1, StorageValueError> {
    BackupBuildMetadataV1::new(
        env!("CARGO_PKG_VERSION"),
        option_env!("RIFFDB_GIT_REVISION").unwrap_or("development-unversioned"),
        "rustc-1.97.0",
        EXECUTABLE_IR_VERSION_V1,
        Vec::new(),
    )
}

/// Floor for the restore bump rule `max(target, staged) + 1`.
///
/// - Absent file or pre-fence key → 0.
/// - Readable value → that value.
/// - Present-but-unreadable (corrupt target, the common recovery case): never
///   abort. Fall back to (1) retained metadata from this process when provided,
///   then (2) the staged incarnation as the floor so restore still completes.
///   Case (2) cannot prove monotonicity against a destroyed high-water fence;
///   that is recorded for operators.
///
/// A receipt-recorded published incarnation is deliberately not a tier here.
/// Every caller inspects the receipt first and returns that value directly, so
/// this helper is only ever reached once the receipt is known to carry none.
fn target_history_incarnation_for_bump(
    path: &std::path::Path,
    retained_target_incarnation: Option<u64>,
    staged_incarnation: u64,
    metrics: Option<&riffdb_observability::MetricRegistry>,
) -> u64 {
    if !path.exists() {
        return 0;
    }
    match read_history_incarnation(path) {
        Ok(None) => 0,
        Ok(Some(incarnation)) => incarnation,
        Err(_) => {
            if let Some(retained) = retained_target_incarnation {
                return retained;
            }
            // Fresh non-resume restore over a corrupt target: use staged as floor.
            // max(staged, staged)+1 is applied by the caller.
            note_unproven_corrupt_target_bump(metrics);
            staged_incarnation
        }
    }
}

/// Process-local count of recovery bumps that could not prove monotonicity
/// against a corrupt target.
///
/// Operators can scrape this without a debugger; production also increments
/// [`riffdb_observability::MetricKey::UnprovenCorruptTargetHistoryBump`] when a
/// metrics registry is wired through the driver dependencies.
static UNPROVEN_CORRUPT_TARGET_BUMPS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn note_unproven_corrupt_target_bump(metrics: Option<&riffdb_observability::MetricRegistry>) {
    UNPROVEN_CORRUPT_TARGET_BUMPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if let Some(metrics) = metrics {
        metrics.increment(riffdb_observability::MetricKey::UnprovenCorruptTargetHistoryBump);
    }
}

/// Process-local count of unproven corrupt-target history bumps.
///
/// Production-visible operator signal: increments whenever a restore cannot
/// prove monotonicity against a present-but-unreadable target and falls back
/// to the staged floor alone. Host diagnostics and tests scrape this without a
/// debugger; the observability metric is the parallel scrape surface when a
/// registry is wired through driver dependencies.
#[must_use]
#[allow(dead_code)] // operator scrape surface; also used by unit tests
pub(crate) fn unproven_corrupt_target_bump_count() -> u64 {
    UNPROVEN_CORRUPT_TARGET_BUMPS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Ensures the receipt carries `published_history_incarnation`.
///
/// Pre-fence receipts recompute with the same max(target, staged)+1 rule and
/// fallback order as the offline bump path. After C2 option (b) a stamped
/// target already holds the published value; re-read it as authority.
///
/// A receipt that already records a published incarnation returns immediately,
/// so the recomputation below never has a receipt value to prefer.
fn ensure_published_incarnation_on_receipt(
    storage: &mut RedbMaintenanceStorage,
    receipt: &mut OfflineMaintenanceReceiptV1,
    staged_incarnation_hint: Option<u64>,
    retained_target_incarnation: Option<u64>,
    metrics: Option<&riffdb_observability::MetricRegistry>,
) -> Result<(), DriverFault> {
    if receipt.published_history_incarnation().is_some() {
        return Ok(());
    }
    if receipt.operation_kind() != OfflineMaintenanceOperationKind::RestoreBackup {
        return Ok(());
    }
    let target_path = storage.configured_database_file();
    if !target_path.exists() {
        return Err(DriverFault::ReceiptIntegrity);
    }
    let staged_floor = staged_incarnation_hint.unwrap_or(0);
    let published = match read_history_incarnation(target_path) {
        // Option-(b) stamped target: receipt authority is the value already on disk.
        Ok(Some(incarnation)) => incarnation,
        Ok(None) => {
            // Pre-fence / unstamped publish: recompute max(target=0, staged)+1.
            let published = target_history_incarnation_for_bump(
                target_path,
                retained_target_incarnation,
                staged_floor,
                metrics,
            )
            .max(staged_floor)
            .saturating_add(1)
            .max(1);
            let _ = riffdb_storage_redb::stamp_history_incarnation(target_path, published);
            published
        }
        Err(_) => {
            // Unreadable after publish: prefer the retained floor, then staged.
            let target_floor = target_history_incarnation_for_bump(
                target_path,
                retained_target_incarnation,
                staged_floor,
                metrics,
            );
            let published = target_floor.max(staged_floor).saturating_add(1).max(1);
            let _ = riffdb_storage_redb::stamp_history_incarnation(target_path, published);
            published
        }
    };
    update_receipt(storage, receipt, |candidate| {
        candidate.record_published_incarnation(published)
    })
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
        StorageErrorKind::InvariantViolation
        | StorageErrorKind::SequenceExhausted
        | StorageErrorKind::HistoryPruned => OfflineMaintenanceReceiptFailureV1::InternalFailure,
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

    #[test]
    fn target_bump_absent_readable_and_unreadable_fallback_order() {
        use riffdb_storage_api::DatabaseInitializationPort;

        let scope = tempfile::TempDir::with_prefix("riffdb-driver-bump-").expect("root");
        let root = scope.path();
        let absent = root.join("missing.redb");
        assert_eq!(
            target_history_incarnation_for_bump(&absent, None, 7, None),
            0
        );

        let readable = root.join("readable.redb");
        let mut store = riffdb_storage_redb::RedbStore::open(&readable).expect("open");
        let database_id =
            riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [0xab; 10]).expect("id");
        store.initialize_database(database_id).expect("initialize");
        drop(store);
        riffdb_storage_redb::stamp_history_incarnation(&readable, 4).expect("stamp");
        assert_eq!(
            target_history_incarnation_for_bump(&readable, None, 1, None),
            4
        );

        let corrupt = root.join("corrupt.redb");
        std::fs::write(&corrupt, b"not-a-redb").expect("corrupt");
        // Retained metadata wins over the staged floor.
        assert_eq!(
            target_history_incarnation_for_bump(&corrupt, Some(50), 7, None),
            50
        );
        // Staged floor last; does not abort. Production-visible counter advances.
        let before = unproven_corrupt_target_bump_count();
        let metrics = riffdb_observability::MetricRegistry::new();
        assert_eq!(
            target_history_incarnation_for_bump(&corrupt, None, 7, Some(&metrics)),
            7
        );
        assert!(unproven_corrupt_target_bump_count() > before);
        assert!(
            metrics.value(riffdb_observability::MetricKey::UnprovenCorruptTargetHistoryBump) >= 1
        );
    }

    #[test]
    fn ensure_published_no_op_when_receipt_already_records_incarnation() {
        use riffdb_storage_api::{
            DatabaseInitializationPort, OfflineMaintenanceAdmissionV1,
            OfflineMaintenanceReceiptPersistencePort, OfflineMaintenanceReceiptPhaseV1,
            OfflineMaintenanceReceiptTransitionV1, OfflineMaintenanceReceiptV1,
        };
        use riffdb_types::{
            ActorId, ActorKind, ApprovalId, BackupNameV1, CapabilityId, DatabaseId,
            OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
            OfflineMaintenanceReplacementConfirmation, offline_maintenance_input_hash,
        };

        let scope = tempfile::TempDir::with_prefix("riffdb-ensure-noop-").expect("root");
        let database = scope.path().join("database.redb");
        let backup_root = scope.path().join("backups");
        let mut store = riffdb_storage_redb::RedbStore::open(&database).expect("open");
        let database_id = DatabaseId::from_unix_milliseconds_and_random(2, [0x22; 10]).expect("id");
        store.initialize_database(database_id).expect("init");
        drop(store);
        riffdb_storage_redb::stamp_history_incarnation(&database, 5).expect("stamp");

        let (mut storage, _) =
            riffdb_storage_redb::RedbMaintenanceStorage::open(&database, &backup_root)
                .expect("maintenance");
        let operation_id =
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [0x61; 10])
                .expect("op");
        let backup_name = BackupNameV1::new("ensure-noop").expect("name");
        let confirmation = OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
        let admission = OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator").expect("actor"),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(3, [0x33; 10]).expect("cap"),
            Some(ApprovalId::new("approval").expect("approval")),
        );
        let mut receipt = OfflineMaintenanceReceiptV1::accepted(
            operation_id,
            OfflineMaintenanceOperationKind::RestoreBackup,
            backup_name.clone(),
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::RestoreBackup,
                &backup_name,
                confirmation,
            ),
            confirmation,
            admission,
        )
        .expect("receipt");
        for phase in [
            OfflineMaintenanceReceiptPhaseV1::Draining,
            OfflineMaintenanceReceiptPhaseV1::Offline,
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        ] {
            // ArtifactPublished requires staged + manifest evidence; use Offline only.
            if phase == OfflineMaintenanceReceiptPhaseV1::ArtifactPublished {
                break;
            }
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                .expect("advance");
        }
        receipt
            .record_published_incarnation(6)
            .expect("record published");
        storage.create_or_read_receipt(&receipt).expect("persist");
        // Resume with receipt present: ensure is a pure no-op.
        ensure_published_incarnation_on_receipt(&mut storage, &mut receipt, None, Some(50), None)
            .expect("ensure");
        assert_eq!(receipt.published_history_incarnation(), Some(6));
    }

    #[test]
    fn ensure_pre_fence_receipt_recomputation_uses_staged_and_retained_floors() {
        use riffdb_storage_api::{
            DatabaseInitializationPort, OfflineMaintenanceAdmissionV1,
            OfflineMaintenanceReceiptPersistencePort, OfflineMaintenanceReceiptPhaseV1,
            OfflineMaintenanceReceiptTransitionV1, OfflineMaintenanceReceiptV1,
        };
        use riffdb_types::{
            ActorId, ActorKind, ApprovalId, BackupNameV1, CapabilityId, DatabaseId,
            OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
            OfflineMaintenanceReplacementConfirmation, offline_maintenance_input_hash,
        };

        let scope = tempfile::TempDir::with_prefix("riffdb-ensure-recompute-").expect("root");
        let database = scope.path().join("database.redb");
        let backup_root = scope.path().join("backups");
        let mut store = riffdb_storage_redb::RedbStore::open(&database).expect("open");
        let database_id = DatabaseId::from_unix_milliseconds_and_random(2, [0x23; 10]).expect("id");
        store.initialize_database(database_id).expect("init");
        drop(store);
        // Pre-fence-shaped target: key absent so ensure recomputes.
        assert_eq!(
            riffdb_storage_redb::read_history_incarnation(&database).expect("read"),
            Some(1),
            "fresh DB has initial incarnation from migrate; stamp-clear via rewrite not needed"
        );
        // Overwrite META to simulate pre-fence absence is not public; instead
        // corrupt target so floor path uses staged+retained.
        std::fs::write(&database, b"not-a-redb").expect("corrupt");

        let (mut storage, _) =
            riffdb_storage_redb::RedbMaintenanceStorage::open(&database, &backup_root)
                .expect("maintenance");
        let operation_id =
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [0x62; 10])
                .expect("op");
        let backup_name = BackupNameV1::new("ensure-recompute").expect("name");
        let confirmation = OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
        let admission = OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator").expect("actor"),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(3, [0x34; 10]).expect("cap"),
            Some(ApprovalId::new("approval").expect("approval")),
        );
        let mut receipt = OfflineMaintenanceReceiptV1::accepted(
            operation_id,
            OfflineMaintenanceOperationKind::RestoreBackup,
            backup_name.clone(),
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::RestoreBackup,
                &backup_name,
                confirmation,
            ),
            confirmation,
            admission,
        )
        .expect("receipt");
        for phase in [
            OfflineMaintenanceReceiptPhaseV1::Draining,
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ] {
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                .expect("advance");
        }
        storage
            .create_or_read_receipt(&receipt)
            .expect("persist pre-fence receipt without published field");
        assert!(receipt.published_history_incarnation().is_none());

        // staged_hint=7, retained=None → max(7,7)+1 = 8 (not 1).
        ensure_published_incarnation_on_receipt(&mut storage, &mut receipt, Some(7), None, None)
            .expect("ensure recompute");
        assert_eq!(
            receipt.published_history_incarnation(),
            Some(8),
            "pre-fence receipt + staged-7 must recompute 8, not 1"
        );
    }

    /// One real, sealed, pre-fence restore ready for the driver.
    ///
    /// Everything the bump rule reads is produced by production code: the named
    /// backup and its stage are copied by `RedbMaintenanceStorage`, the staged
    /// incarnation is whatever the source database carried, and the restore
    /// receipt is persisted without `published_history_incarnation` (the
    /// pre-fence shape). Only staged authentication and authorization are
    /// skipped, because they own no part of the incarnation decision.
    struct StagedRestoreFixture {
        /// Whole-directory scope owning every fixture artifact; removed on
        /// drop — pass, fail, or panic.
        root: tempfile::TempDir,
        database: std::path::PathBuf,
        storage: RedbMaintenanceStorage,
        lifecycle: MaintenanceLifecycle,
        receipt: OfflineMaintenanceReceiptV1,
        prepared: PreparedStagedRestore,
    }

    fn fixture_admission(seed: u8) -> OfflineMaintenanceAdmissionV1 {
        use riffdb_types::{ActorId, ActorKind, ApprovalId, CapabilityId};

        OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator-1").expect("actor ID"),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(3, [seed; 10]).expect("capability ID"),
            Some(ApprovalId::new("approval-1").expect("approval ID")),
        )
    }

    fn fixture_startup_inputs() -> StartupValidationInputs {
        use riffdb_storage_api::{
            ReadableCapabilityDigestInventory, ReadableDigestKey,
            ReadableIdempotencyDigestInventory,
        };
        use riffdb_types::{DigestKeyId, Timestamp};

        let digest = ReadableDigestKey::v1(DigestKeyId::new(7).expect("digest key ID"));
        StartupValidationInputs::new(
            Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
            ReadableCapabilityDigestInventory::new(vec![digest]).expect("capability inventory"),
            ReadableIdempotencyDigestInventory::new(vec![digest]).expect("idempotency inventory"),
        )
    }

    fn fixture_dependencies<'a>(
        clocks: &'a ProductionWallClocks,
        recovery: &'a MaintenanceRecoveryController,
        retained_target_history_incarnation: Option<u64>,
    ) -> MaintenanceDriverDependencies<'a> {
        let audience = Audience::new("maintenance-driver-test").expect("audience");
        let capability_keys = CapabilityDigestKeyProvider::parse_document(
            b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
        )
        .expect("capability digest keys");
        MaintenanceDriverDependencies::new(
            fixture_startup_inputs(),
            crate::identifiers::ProductionIdentifierSources::new().database_ids(),
            RedbCommitProfile::Standard,
            Arc::new(capability_keys),
            Environment::new("maintenance-driver-test").expect("environment"),
            audience.clone(),
            TrustedAudienceCatalog::new(vec![audience]).expect("trusted audiences"),
            clocks,
            Arc::new(riffdb_auth::NoopAuthenticationTelemetry),
            Arc::new(riffdb_policy::NoopAuthorizationTelemetry),
            recovery,
            retained_target_history_incarnation,
            None,
        )
    }

    fn staged_restore_fixture(
        label: &str,
        seed: u8,
        source_incarnation: u64,
    ) -> StagedRestoreFixture {
        use riffdb_storage_api::DatabaseInitializationPort;
        use riffdb_types::{BackupNameV1, DatabaseId, offline_maintenance_input_hash};

        let root = tempfile::Builder::new()
            .prefix(&format!("riffdb-driver-restore-{label}-"))
            .tempdir()
            .expect("root");
        let database = root.path().join("database.redb");
        let backup_root = root.path().join("backups");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(2, [seed; 10]).expect("database ID");
        let mut store = riffdb_storage_redb::RedbStore::open(&database).expect("open source");
        store
            .initialize_database(database_id)
            .expect("initialize source");
        drop(store);
        // The source is already several destructive restores deep; the backup and
        // its stage inherit that fence through the production copy paths.
        riffdb_storage_redb::stamp_history_incarnation(&database, source_incarnation)
            .expect("stamp source incarnation");

        let (mut storage, _) = RedbMaintenanceStorage::open(&database, &backup_root)
            .expect("open maintenance storage");
        let backup_name = BackupNameV1::new("driver-restore-fixture").expect("backup name");

        let create_confirmation = OfflineMaintenanceReplacementConfirmation::NotProvided;
        let mut create = OfflineMaintenanceReceiptV1::accepted(
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [seed; 10])
                .expect("create operation ID"),
            OfflineMaintenanceOperationKind::CreateBackup,
            backup_name.clone(),
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::CreateBackup,
                &backup_name,
                create_confirmation,
            ),
            create_confirmation,
            fixture_admission(seed),
        )
        .expect("create receipt");
        create
            .record_source_database_id(database_id)
            .expect("create source identity");
        for phase in [
            OfflineMaintenanceReceiptPhaseV1::Draining,
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ] {
            create
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                .expect("advance create");
        }
        storage
            .create_or_read_receipt(&create)
            .expect("persist create receipt");
        let (_manifest, manifest_identity) = storage
            .create_named_backup(
                create.operation_id(),
                create.backup_name(),
                &production_backup_build_metadata().expect("build metadata"),
            )
            .expect("create named backup");
        create
            .record_manifest_identity(manifest_identity)
            .expect("create manifest identity");
        for phase in [
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
            OfflineMaintenanceReceiptPhaseV1::Validating,
            OfflineMaintenanceReceiptPhaseV1::Succeeded,
        ] {
            create
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                .expect("advance create");
        }
        storage
            .replace_receipt(&create)
            .expect("persist create completion");

        let restore_confirmation =
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
        let mut receipt = OfflineMaintenanceReceiptV1::accepted(
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(
                2,
                [seed.wrapping_add(1); 10],
            )
            .expect("restore operation ID"),
            OfflineMaintenanceOperationKind::RestoreBackup,
            backup_name.clone(),
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::RestoreBackup,
                &backup_name,
                restore_confirmation,
            ),
            restore_confirmation,
            fixture_admission(seed),
        )
        .expect("restore receipt");
        receipt
            .record_source_database_id(database_id)
            .expect("restore source identity");
        for phase in [
            OfflineMaintenanceReceiptPhaseV1::Draining,
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ] {
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                .expect("advance restore");
        }
        storage
            .create_or_read_receipt(&receipt)
            .expect("persist pre-fence restore receipt");
        assert!(
            receipt.published_history_incarnation().is_none(),
            "fixture must present the pre-fence receipt shape"
        );

        let stage = storage
            .stage_restore(receipt.operation_id(), receipt.backup_name())
            .expect("stage restore");
        let staged_database_id = stage.manifest_identity().database_id();
        let sealed = stage
            .seal_after_validation(staged_database_id)
            .expect("seal staged restore");
        assert_eq!(sealed.staged_history_incarnation(), source_incarnation);

        let lifecycle = MaintenanceLifecycle::ready();
        lifecycle
            .begin(receipt.operation_id())
            .expect("claim receipt");
        lifecycle
            .mark_offline(receipt.operation_id())
            .expect("quiesce offline");

        StagedRestoreFixture {
            root,
            database,
            storage,
            lifecycle,
            receipt,
            prepared: PreparedStagedRestore {
                sealed,
                admission: fixture_admission(seed),
            },
        }
    }

    #[test]
    fn run_restore_over_an_absent_target_publishes_staged_seven_as_eight() {
        let StagedRestoreFixture {
            root: _root,
            database,
            mut storage,
            lifecycle,
            mut receipt,
            prepared,
        } = staged_restore_fixture("absent-target", 0x71, 7);
        assert_eq!(prepared.sealed.staged_history_incarnation(), 7);
        // The configured target is gone, so no fence can be read from it.
        std::fs::remove_file(&database).expect("remove target");

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
        .expect("restore completes over an absent target");

        assert_eq!(startup.retained_metadata().history_incarnation(), 8);
        drop(startup);
        assert_eq!(
            riffdb_storage_redb::read_history_incarnation(&database).expect("read published META"),
            Some(8),
            "pre-fence receipt + staged-7 over an absent target must publish 8, not 1"
        );
        assert_eq!(receipt.published_history_incarnation(), Some(8));
        assert_eq!(
            receipt.current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Succeeded
        );
    }

    #[test]
    fn run_restore_over_an_unreadable_target_prefers_retained_metadata_evidence() {
        let StagedRestoreFixture {
            root: _root,
            database,
            mut storage,
            lifecycle,
            mut receipt,
            prepared,
        } = staged_restore_fixture("retained-evidence", 0x81, 7);
        assert_eq!(prepared.sealed.staged_history_incarnation(), 7);
        // Present but unreadable: the only surviving fence is the value this
        // process retained from its own successful open of the target.
        std::fs::write(&database, b"not-a-redb").expect("corrupt target");

        let clocks = ProductionWallClocks::new();
        let recovery = MaintenanceRecoveryController::disabled();
        let dependencies = fixture_dependencies(&clocks, &recovery, Some(12));
        let startup = run_restore(
            &mut storage,
            &lifecycle,
            &dependencies,
            &mut receipt,
            None,
            Some(prepared),
        )
        .expect("restore completes over an unreadable target");

        assert_eq!(startup.retained_metadata().history_incarnation(), 13);
        drop(startup);
        assert_eq!(
            riffdb_storage_redb::read_history_incarnation(&database).expect("read published META"),
            Some(13),
            "retained-metadata evidence 12 must outrank the staged floor 7"
        );
        assert_eq!(receipt.published_history_incarnation(), Some(13));
        assert_eq!(
            receipt.current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Succeeded
        );
    }
}
