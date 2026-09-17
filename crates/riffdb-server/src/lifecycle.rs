//! Atomic production routing across startup, bootstrap, deployment, and readiness.

// The process composition consumes the installation and shutdown methods later in WP-130.
#![allow(dead_code)]

use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use riffdb_api_grpc::{
    CheckedGrpcRestoreRetrySecurityContext, CheckedGrpcSecurityContext,
    GrpcApplicationExportOperation, GrpcApplicationReimportOperation, GrpcBootstrapCompletion,
    GrpcContractMigrationOperation, GrpcDeploymentCompletion, GrpcLifecycleRoute,
    GrpcOfflineMaintenanceOperation,
};
use riffdb_service::{
    ApplicationExportApplication, ApplicationReimportApplication, ApplicationService,
    ContractMigrationApplication, HealthContext, HealthRequest, HealthResult,
    InitializingRiffDbService, PreBootstrapHealthContextIssuer, PreBootstrapLifecycle,
    RecoveryOfflineMaintenanceApplication, RestoreRetryOfflineMaintenanceApplication,
    ServiceFuture, ServiceTelemetry,
};
use riffdb_types::{OfflineMaintenanceOperationId, ServiceOperationV1};

use crate::maintenance_lifecycle::MaintenanceLifecycle;
use crate::runtime_support::RuntimeRoutingState;
use crate::server_generation::{SERVER_GENERATION_BYTES, ServerGenerationV1};
use crate::startup::{ValidatedAllocatorCapacity, ValidatedStartupLifecycle};

/// Server-owned lifecycle route shared by every in-process transport adapter.
pub(crate) struct ProductionLifecycleRoute {
    initializing: InitializingRiffDbService,
    follower: OnceLock<crate::replication_bootstrap::FollowerReadSnapshots>,
    activated: OnceLock<Arc<dyn ApplicationService>>,
    replication: OnceLock<Arc<dyn riffdb_service::ReplicationApplication>>,
    migration: OnceLock<Arc<dyn ContractMigrationApplication>>,
    application_export: OnceLock<Arc<dyn ApplicationExportApplication>>,
    application_reimport: OnceLock<Arc<dyn ApplicationReimportApplication>>,
    security: OnceLock<CheckedGrpcSecurityContext>,
    server_generation: OnceLock<ServerGenerationV1>,
    history_incarnation: OnceLock<u64>,
    recovery: OnceLock<Arc<dyn RecoveryOfflineMaintenanceApplication>>,
    restore_retry: OnceLock<Arc<dyn RestoreRetryOfflineMaintenanceApplication>>,
    restore_retry_security: OnceLock<CheckedGrpcRestoreRetrySecurityContext>,
    read_stage_telemetry: OnceLock<Arc<dyn ServiceTelemetry>>,
    runtime: RuntimeRoutingState,
    maintenance: Arc<MaintenanceLifecycle>,
    state: Mutex<RouteState>,
}

impl ProductionLifecycleRoute {
    /// Begins in the Health-only validation phase with the one matching issuer.
    pub(crate) fn new(
        initializing: InitializingRiffDbService,
        issuer: PreBootstrapHealthContextIssuer,
        runtime: RuntimeRoutingState,
    ) -> Self {
        Self::new_with_maintenance(
            initializing,
            issuer,
            runtime,
            Arc::new(MaintenanceLifecycle::ready()),
        )
    }

    /// Begins with one process-wide private maintenance admission authority.
    pub(crate) fn new_with_maintenance(
        initializing: InitializingRiffDbService,
        issuer: PreBootstrapHealthContextIssuer,
        runtime: RuntimeRoutingState,
        maintenance: Arc<MaintenanceLifecycle>,
    ) -> Self {
        Self {
            initializing,
            follower: OnceLock::new(),
            activated: OnceLock::new(),
            replication: OnceLock::new(),
            migration: OnceLock::new(),
            application_export: OnceLock::new(),
            application_reimport: OnceLock::new(),
            security: OnceLock::new(),
            server_generation: OnceLock::new(),
            history_incarnation: OnceLock::new(),
            recovery: OnceLock::new(),
            restore_retry: OnceLock::new(),
            restore_retry_security: OnceLock::new(),
            read_stage_telemetry: OnceLock::new(),
            runtime,
            maintenance,
            state: Mutex::new(RouteState {
                model: LifecycleModel::initializing(),
                issuer: Some(issuer),
            }),
        }
    }

    /// Configures source-driven follower admission before publishing a service.
    pub(crate) fn bind_follower_reads(
        &self,
        reads: crate::replication_bootstrap::FollowerReadSnapshots,
    ) -> Result<(), LifecycleInstallError> {
        let mut state = self.lock_state();
        if state.model.stage != LifecycleStage::InitializingValidation {
            return Err(LifecycleInstallError::AlreadyInstalled);
        }
        self.follower
            .set(reads)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)?;
        // Followers have no local bootstrap authority, including the convenience path.
        close_issuer(&mut state.issuer);
        Ok(())
    }
    fn allows_operation(
        &self,
        model: LifecycleModel,
        operation: ServiceOperationV1,
        runtime_ready: bool,
    ) -> bool {
        if let Some(reads) = self.follower.get() {
            return runtime_ready
                && !matches!(
                    model.stage,
                    LifecycleStage::InitializingValidation | LifecycleStage::Stopped
                )
                && reads.latest().is_ok();
        }
        model.allows_authenticated(operation, runtime_ready)
    }
    fn allows_maintenance(&self, model: LifecycleModel, runtime_ready: bool) -> bool {
        if self.follower.get().is_some() {
            return self.allows_operation(
                model,
                ServiceOperationV1::ApplyContractMigration,
                runtime_ready,
            );
        }
        model.allows_offline_maintenance(runtime_ready)
    }

    /// Installs administrative replication over the activated published source.
    pub(crate) fn install_replication(
        &self,
        service: Arc<dyn riffdb_service::ReplicationApplication>,
    ) -> Result<(), LifecycleInstallError> {
        self.replication
            .set(service)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)
    }

    /// Installs the disjoint migration surface owned by the same activated service.
    pub(crate) fn install_contract_migration(
        &self,
        service: Arc<dyn ContractMigrationApplication>,
    ) -> Result<(), LifecycleInstallError> {
        self.migration
            .set(service)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)
    }

    /// Installs the disjoint symbolic-export surface owned by the activated service.
    pub(crate) fn install_application_export(
        &self,
        service: Arc<dyn ApplicationExportApplication>,
    ) -> Result<(), LifecycleInstallError> {
        self.application_export
            .set(service)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)
    }

    /// Installs the disjoint compiler-owned reimport surface.
    pub(crate) fn install_application_reimport(
        &self,
        service: Arc<dyn ApplicationReimportApplication>,
    ) -> Result<(), LifecycleInstallError> {
        self.application_reimport
            .set(service)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)
    }

    /// Installs the sole activated service after the complete startup proof join.
    pub(crate) fn install_activated(
        &self,
        service: Arc<dyn ApplicationService>,
        security: CheckedGrpcSecurityContext,
        server_generation: ServerGenerationV1,
        history_incarnation: u64,
        lifecycle: ValidatedStartupLifecycle,
        capacity: ValidatedAllocatorCapacity,
    ) -> Result<(), LifecycleInstallError> {
        self.install_activated_with_telemetry(
            service,
            security,
            server_generation,
            history_incarnation,
            lifecycle,
            capacity,
            None,
        )
    }

    /// Installs the activated service and optional read-stage telemetry sink.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn install_activated_with_telemetry(
        &self,
        service: Arc<dyn ApplicationService>,
        security: CheckedGrpcSecurityContext,
        server_generation: ServerGenerationV1,
        history_incarnation: u64,
        lifecycle: ValidatedStartupLifecycle,
        capacity: ValidatedAllocatorCapacity,
        read_stage_telemetry: Option<Arc<dyn ServiceTelemetry>>,
    ) -> Result<(), LifecycleInstallError> {
        let installed = LifecycleModel::from_startup(lifecycle, capacity)?;
        let mut state = self.lock_state();
        if state.model.stage != LifecycleStage::InitializingValidation {
            return Err(LifecycleInstallError::AlreadyInstalled);
        }
        if self.activated.get().is_some()
            || self.security.get().is_some()
            || self.server_generation.get().is_some()
            || self.history_incarnation.get().is_some()
        {
            return Err(LifecycleInstallError::AlreadyInstalled);
        }
        self.activated
            .set(service)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)?;
        self.security
            .set(security)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)?;
        self.server_generation
            .set(server_generation)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)?;
        self.history_incarnation
            .set(history_incarnation)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)?;
        if let Some(telemetry) = read_stage_telemetry {
            self.read_stage_telemetry
                .set(telemetry)
                .map_err(|_| LifecycleInstallError::AlreadyInstalled)?;
        }

        if lifecycle != ValidatedStartupLifecycle::BootstrapRequired {
            close_issuer(&mut state.issuer);
        }
        state.model = installed;
        Ok(())
    }

    /// Installs the least-authority restore-only service for failed startup.
    pub(crate) fn install_recovery(
        &self,
        recovery: Arc<dyn RecoveryOfflineMaintenanceApplication>,
    ) -> Result<(), LifecycleInstallError> {
        if !self.maintenance.recovery_restore_available() {
            return Err(LifecycleInstallError::InvalidRecoveryStage);
        }
        self.recovery
            .set(recovery)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)
    }

    /// Installs one exact current-database retry without activating a full service.
    pub(crate) fn install_restore_retry(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: riffdb_types::OfflineMaintenanceInputHash,
        service: Arc<dyn RestoreRetryOfflineMaintenanceApplication>,
        security: CheckedGrpcRestoreRetrySecurityContext,
    ) -> Result<(), LifecycleInstallError> {
        if !self
            .maintenance
            .credential_retry_restore_matches(operation_id, input_hash)
        {
            return Err(LifecycleInstallError::InvalidRestoreRetryStage);
        }
        let mut state = self.lock_state();
        if self.activated.get().is_some()
            || self.security.get().is_some()
            || self.server_generation.get().is_some()
            || self.recovery.get().is_some()
            || self.restore_retry.get().is_some()
            || self.restore_retry_security.get().is_some()
        {
            return Err(LifecycleInstallError::AlreadyInstalled);
        }
        self.restore_retry
            .set(service)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)?;
        self.restore_retry_security
            .set(security)
            .map_err(|_| LifecycleInstallError::AlreadyInstalled)?;
        close_issuer(&mut state.issuer);
        Ok(())
    }

    /// Irreversibly stops every lifecycle route, including restricted Health.
    pub(crate) fn stop(&self) {
        let mut state = self.lock_state();
        state.model.stop();
        close_issuer(&mut state.issuer);
    }

    /// Returns the one routing authority owned by this lifecycle route.
    pub(crate) fn runtime_routing(&self) -> RuntimeRoutingState {
        self.runtime.clone()
    }

    /// Process-retained history incarnation installed at activation, if any.
    ///
    /// Unlike the gRPC route getter, this ignores admission and lifecycle stage
    /// so offline maintenance can still use the value as a corrupt-target floor
    /// after ordinary admission has been closed.
    pub(crate) fn retained_history_incarnation(&self) -> Option<u64> {
        self.history_incarnation.get().copied()
    }

    fn activated_service(&self) -> Option<Arc<dyn ApplicationService>> {
        self.activated.get().cloned()
    }

    fn lock_state(&self) -> MutexGuard<'_, RouteState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.model.stop();
                close_issuer(&mut state.issuer);
                state
            }
        }
    }
}

impl GrpcLifecycleRoute for ProductionLifecycleRoute {
    fn admit_replication(&self) -> Option<Arc<dyn riffdb_service::ReplicationApplication>> {
        if !self.maintenance.ordinary_admission_available() {
            return None;
        }
        self.lock_state()
            .model
            .allows_offline_maintenance(self.runtime.is_routing_allowed())
            .then(|| self.replication.get().cloned())
            .flatten()
    }

    fn admit_authenticated(
        &self,
        operation: ServiceOperationV1,
    ) -> Option<Arc<dyn ApplicationService>> {
        if !self.maintenance.ordinary_admission_available() {
            return None;
        }
        let runtime_ready = self.runtime.is_routing_allowed();
        let state = self.lock_state();
        self.allows_operation(state.model, operation, runtime_ready)
            .then(|| self.activated_service())
            .flatten()
    }

    fn admit_offline_maintenance(
        &self,
        operation: GrpcOfflineMaintenanceOperation,
    ) -> Option<Arc<dyn ApplicationService>> {
        if !self.maintenance.ordinary_admission_available() {
            return None;
        }
        let _operation = operation;
        let runtime_ready = self.runtime.is_routing_allowed();
        let state = self.lock_state();
        self.allows_maintenance(state.model, runtime_ready)
            .then(|| self.activated_service())
            .flatten()
    }

    fn admit_contract_migration(
        &self,
        _operation: GrpcContractMigrationOperation,
    ) -> Option<Arc<dyn ContractMigrationApplication>> {
        if !self.maintenance.ordinary_admission_available() {
            return None;
        }
        let runtime_ready = self.runtime.is_routing_allowed();
        let state = self.lock_state();
        self.allows_maintenance(state.model, runtime_ready)
            .then(|| self.migration.get().cloned())
            .flatten()
    }

    fn admit_application_export(
        &self,
        operation: GrpcApplicationExportOperation,
    ) -> Option<Arc<dyn ApplicationExportApplication>> {
        if !self.maintenance.ordinary_admission_available() {
            return None;
        }
        let operation = match operation {
            GrpcApplicationExportOperation::Start => ServiceOperationV1::StartApplicationExport,
            GrpcApplicationExportOperation::GetPage => ServiceOperationV1::GetApplicationExportPage,
            GrpcApplicationExportOperation::GetOperation => {
                ServiceOperationV1::GetApplicationExport
            }
            GrpcApplicationExportOperation::Cancel => ServiceOperationV1::CancelApplicationExport,
        };
        let runtime_ready = self.runtime.is_routing_allowed();
        let state = self.lock_state();
        self.allows_operation(state.model, operation, runtime_ready)
            .then(|| self.application_export.get().cloned())
            .flatten()
    }

    fn admit_application_reimport(
        &self,
        operation: GrpcApplicationReimportOperation,
    ) -> Option<Arc<dyn ApplicationReimportApplication>> {
        if !self.maintenance.ordinary_admission_available() {
            return None;
        }
        let operation = match operation {
            GrpcApplicationReimportOperation::Start => ServiceOperationV1::StartApplicationReimport,
            GrpcApplicationReimportOperation::ApplyPage => {
                ServiceOperationV1::ApplyApplicationReimportPage
            }
            GrpcApplicationReimportOperation::GetOperation => {
                ServiceOperationV1::GetApplicationReimport
            }
            GrpcApplicationReimportOperation::Cancel => {
                ServiceOperationV1::CancelApplicationReimport
            }
        };
        let runtime_ready = self.runtime.is_routing_allowed();
        let state = self.lock_state();
        self.allows_operation(state.model, operation, runtime_ready)
            .then(|| self.application_reimport.get().cloned())
            .flatten()
    }

    fn admit_restore_retry(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: riffdb_types::OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RestoreRetryOfflineMaintenanceApplication>> {
        self.maintenance
            .credential_retry_restore_matches(operation_id, input_hash)
            .then(|| self.restore_retry.get().cloned())
            .flatten()
    }

    fn admit_recovery_restore(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: riffdb_types::OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RecoveryOfflineMaintenanceApplication>> {
        self.maintenance
            .recovery_restore_matches(operation_id, input_hash)
            .then(|| self.recovery.get().cloned())
            .flatten()
    }

    fn security_context(&self) -> Option<CheckedGrpcSecurityContext> {
        if !self.maintenance.ordinary_admission_available() {
            return None;
        }
        let state = self.lock_state();
        if matches!(
            state.model.stage,
            LifecycleStage::InitializingValidation | LifecycleStage::Stopped
        ) {
            return None;
        }
        self.security.get().cloned()
    }

    fn restore_retry_security_context(&self) -> Option<CheckedGrpcRestoreRetrySecurityContext> {
        self.maintenance
            .credential_retry_restore_available()
            .then(|| self.restore_retry_security.get().cloned())
            .flatten()
    }

    fn server_generation(&self) -> Option<[u8; SERVER_GENERATION_BYTES]> {
        if !self.maintenance.ordinary_admission_available() {
            return None;
        }
        let state = self.lock_state();
        if matches!(
            state.model.stage,
            LifecycleStage::InitializingValidation | LifecycleStage::Stopped
        ) {
            return None;
        }
        self.server_generation.get().map(ServerGenerationV1::bytes)
    }

    fn history_incarnation(&self) -> Option<u64> {
        if !self.maintenance.ordinary_admission_available() {
            return None;
        }
        let state = self.lock_state();
        if matches!(
            state.model.stage,
            LifecycleStage::InitializingValidation | LifecycleStage::Stopped
        ) {
            return None;
        }
        self.history_incarnation.get().copied()
    }

    fn read_stage_telemetry(&self) -> Option<Arc<dyn ServiceTelemetry>> {
        self.read_stage_telemetry.get().cloned()
    }

    fn restricted_health(&self, request: HealthRequest) -> Option<ServiceFuture<'_, HealthResult>> {
        if !self.maintenance.ordinary_admission_available() {
            return None;
        }
        let state = self.lock_state();
        let lifecycle = state.model.restricted_health_lifecycle()?;
        let context = state.issuer.as_ref()?.issue(lifecycle)?;

        if state.model.stage == LifecycleStage::InitializingValidation {
            // Keep issuance and the service's admission check ordered against close.
            let health = self.initializing.health(context, request);
            drop(state);
            Some(health)
        } else {
            let service = self.activated_service()?;
            drop(state);
            Some(Box::pin(async move {
                service
                    .health(HealthContext::pre_bootstrap(context), request)
                    .await
            }))
        }
    }

    fn bootstrap_available(&self) -> bool {
        self.follower.get().is_none()
            && self.maintenance.ordinary_admission_available()
            && self.runtime.is_routing_allowed()
            && self.lock_state().model.bootstrap_available()
    }

    fn begin_bootstrap(&self) -> Option<Arc<dyn ApplicationService>> {
        if self.follower.get().is_some()
            || !self.maintenance.ordinary_admission_available()
            || !self.runtime.is_routing_allowed()
        {
            return None;
        }
        let mut state = self.lock_state();
        let initial = state.model.stage == LifecycleStage::BootstrapRequired;
        if !state.model.begin_bootstrap() {
            return None;
        }
        if initial {
            close_issuer(&mut state.issuer);
        }
        self.activated_service()
    }

    fn finish_bootstrap(&self, completion: GrpcBootstrapCompletion) {
        let mut state = self.lock_state();
        state.model.finish_bootstrap(completion);
        if state.model.stage == LifecycleStage::Stopped {
            close_issuer(&mut state.issuer);
        }
    }

    fn finish_deployment(&self, completion: GrpcDeploymentCompletion) {
        let mut state = self.lock_state();
        state.model.finish_deployment(completion);
        if state.model.stage == LifecycleStage::Stopped {
            close_issuer(&mut state.issuer);
        }
    }
}

impl fmt::Debug for ProductionLifecycleRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionLifecycleRoute([CAPABILITY])")
    }
}

fn close_issuer(issuer: &mut Option<PreBootstrapHealthContextIssuer>) {
    if let Some(issuer) = issuer.take() {
        issuer.close();
    }
}

struct RouteState {
    model: LifecycleModel,
    issuer: Option<PreBootstrapHealthContextIssuer>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleStage {
    InitializingValidation,
    BootstrapRequired,
    DeploymentRequired,
    Ready,
    Exhausted,
    BootstrapInFlight,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LifecycleModel {
    stage: LifecycleStage,
    active_observed: bool,
}

impl LifecycleModel {
    const fn initializing() -> Self {
        Self {
            stage: LifecycleStage::InitializingValidation,
            active_observed: false,
        }
    }

    fn from_startup(
        lifecycle: ValidatedStartupLifecycle,
        capacity: ValidatedAllocatorCapacity,
    ) -> Result<Self, LifecycleInstallError> {
        let available = capacity == ValidatedAllocatorCapacity::Available;
        let active_observed = lifecycle == ValidatedStartupLifecycle::ActiveContract;
        let stage = match (lifecycle, available) {
            (ValidatedStartupLifecycle::BootstrapRequired, true) => {
                LifecycleStage::BootstrapRequired
            }
            (ValidatedStartupLifecycle::BootstrapRequired, false) => {
                return Err(LifecycleInstallError::MarkerAbsentAllocatorExhausted);
            }
            (ValidatedStartupLifecycle::DeploymentRequired, true) => {
                LifecycleStage::DeploymentRequired
            }
            (ValidatedStartupLifecycle::ActiveContract, true) => LifecycleStage::Ready,
            (
                ValidatedStartupLifecycle::DeploymentRequired
                | ValidatedStartupLifecycle::ActiveContract,
                false,
            ) => LifecycleStage::Exhausted,
        };
        Ok(Self {
            stage,
            active_observed,
        })
    }

    const fn restricted_health_lifecycle(self) -> Option<PreBootstrapLifecycle> {
        match self.stage {
            LifecycleStage::InitializingValidation => {
                Some(PreBootstrapLifecycle::InitializingValidation)
            }
            LifecycleStage::BootstrapRequired => Some(PreBootstrapLifecycle::InitializingBootstrap),
            LifecycleStage::DeploymentRequired
            | LifecycleStage::Ready
            | LifecycleStage::Exhausted
            | LifecycleStage::BootstrapInFlight
            | LifecycleStage::Stopped => None,
        }
    }

    fn allows_authenticated(self, operation: ServiceOperationV1, runtime_ready: bool) -> bool {
        if operation == ServiceOperationV1::GetHealth {
            return matches!(
                self.stage,
                LifecycleStage::DeploymentRequired
                    | LifecycleStage::Ready
                    | LifecycleStage::Exhausted
            );
        }
        if !runtime_ready {
            return false;
        }
        match self.stage {
            LifecycleStage::DeploymentRequired => matches!(
                operation,
                ServiceOperationV1::ValidateContract
                    | ServiceOperationV1::ExplainCommand
                    | ServiceOperationV1::DeployContract
                    | ServiceOperationV1::GetActiveContract
                    | ServiceOperationV1::CreateCapability
                    | ServiceOperationV1::RevokeCapability
                    | ServiceOperationV1::RegisterFollower
                    | ServiceOperationV1::RetireFollower
                    | ServiceOperationV1::DiscoverCommandTools
                    | ServiceOperationV1::DiscoverResources
            ),
            LifecycleStage::Ready => true,
            LifecycleStage::InitializingValidation
            | LifecycleStage::BootstrapRequired
            | LifecycleStage::Exhausted
            | LifecycleStage::BootstrapInFlight
            | LifecycleStage::Stopped => false,
        }
    }

    const fn bootstrap_available(self) -> bool {
        match self.stage {
            LifecycleStage::BootstrapRequired | LifecycleStage::DeploymentRequired => true,
            // Exact retained-token replay is available after restart. Its completion
            // preserves the already observed active catalog.
            LifecycleStage::Ready => true,
            LifecycleStage::InitializingValidation
            | LifecycleStage::Exhausted
            | LifecycleStage::BootstrapInFlight
            | LifecycleStage::Stopped => false,
        }
    }

    const fn allows_offline_maintenance(self, runtime_ready: bool) -> bool {
        runtime_ready && matches!(self.stage, LifecycleStage::Ready)
    }

    fn begin_bootstrap(&mut self) -> bool {
        if !self.bootstrap_available() {
            return false;
        }
        self.stage = LifecycleStage::BootstrapInFlight;
        true
    }

    fn finish_bootstrap(&mut self, completion: GrpcBootstrapCompletion) {
        if self.stage == LifecycleStage::Stopped {
            return;
        }
        if self.stage != LifecycleStage::BootstrapInFlight {
            self.stop();
            return;
        }
        self.stage = match completion {
            GrpcBootstrapCompletion::Created
            | GrpcBootstrapCompletion::Replayed
            | GrpcBootstrapCompletion::Conflict => {
                if self.active_observed {
                    LifecycleStage::Ready
                } else {
                    LifecycleStage::DeploymentRequired
                }
            }
            GrpcBootstrapCompletion::OutcomeUnknown
            | GrpcBootstrapCompletion::Failed
            | GrpcBootstrapCompletion::Abandoned => LifecycleStage::Stopped,
        };
    }

    fn finish_deployment(&mut self, completion: GrpcDeploymentCompletion) {
        if self.stage == LifecycleStage::Stopped {
            return;
        }
        match completion {
            GrpcDeploymentCompletion::Activated | GrpcDeploymentCompletion::AlreadyActive => {
                self.active_observed = true;
                match self.stage {
                    LifecycleStage::DeploymentRequired | LifecycleStage::Ready => {
                        self.stage = LifecycleStage::Ready;
                    }
                    LifecycleStage::BootstrapInFlight => {}
                    LifecycleStage::InitializingValidation
                    | LifecycleStage::BootstrapRequired
                    | LifecycleStage::Exhausted
                    | LifecycleStage::Stopped => self.stop(),
                }
            }
            GrpcDeploymentCompletion::NotActivated => match self.stage {
                LifecycleStage::DeploymentRequired
                | LifecycleStage::Ready
                | LifecycleStage::BootstrapInFlight => {}
                LifecycleStage::InitializingValidation
                | LifecycleStage::BootstrapRequired
                | LifecycleStage::Exhausted
                | LifecycleStage::Stopped => self.stop(),
            },
            GrpcDeploymentCompletion::OutcomeUnknown | GrpcDeploymentCompletion::Abandoned => {
                self.stop();
            }
        }
    }

    fn stop(&mut self) {
        self.stage = LifecycleStage::Stopped;
    }
}

/// Failure to install one validated startup classification into the route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleInstallError {
    /// A marker-absent store cannot offer the required authenticated exhausted Health.
    MarkerAbsentAllocatorExhausted,
    /// The one activated service or startup classification was already installed.
    AlreadyInstalled,
    /// A recovery service was installed outside the failed-closed recovery stage.
    InvalidRecoveryStage,
    /// A current-database retry service did not match the frozen receipt.
    InvalidRestoreRetryStage,
}

impl fmt::Display for LifecycleInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MarkerAbsentAllocatorExhausted => {
                "marker-absent startup cannot install exhausted authoritative allocators"
            }
            Self::AlreadyInstalled => "the activated application service is already installed",
            Self::InvalidRecoveryStage => {
                "the recovery application was installed outside recovery mode"
            }
            Self::InvalidRestoreRetryStage => {
                "the restore-retry application did not match the frozen receipt"
            }
        })
    }
}

impl Error for LifecycleInstallError {}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::task::{Context, Poll, Waker};
    use std::thread;
    use std::time::Duration;

    use riffdb_auth::{
        AuthenticationContext, AuthenticationFailure, CapabilityDigestKeyProvider,
        CredentialAuthenticator, OpaqueCredential,
    };
    use riffdb_errors::PublicError;
    use riffdb_service::{
        AdministrationApplication, CommandApplication, CommitApplication, ContractApplication,
        CreateCapabilityInvocation, CreateCapabilityResult, DiscoveryApplication,
        EventServiceApplication, OfflineMaintenanceApplication, QueryApplication,
        RecoveryOfflineMaintenanceApplication, RequestContext,
        RestoreRetryOfflineMaintenanceApplication, ServiceFailure, SymbolicQueryApplication,
    };
    use riffdb_types::{Audience, DatabaseId, Environment};

    use super::*;

    struct RejectingAuthenticator;

    impl CredentialAuthenticator for RejectingAuthenticator {
        fn authenticate(
            &self,
            _credential: OpaqueCredential<'_>,
            _context: &AuthenticationContext,
        ) -> Result<riffdb_auth::AuthenticatedPrincipal, AuthenticationFailure> {
            Err(AuthenticationFailure::Unauthenticated)
        }
    }

    struct ClosedApplicationService;

    fn denied<T>() -> ServiceFuture<'static, T> {
        Box::pin(async { Err(ServiceFailure::from(PublicError::authorization_denied())) })
    }

    macro_rules! denied_operation {
        ($name:ident, $context:ty, $request:ty, $result:ty) => {
            fn $name(&self, _context: $context, _request: $request) -> ServiceFuture<'_, $result> {
                denied()
            }
        };
    }

    impl riffdb_service::ApplicationInstallationApplication for ClosedApplicationService {
        denied_operation!(
            start_application_installation,
            RequestContext,
            riffdb_service::StartApplicationInstallationRequest,
            riffdb_service::ApplicationInstallationOperationResult
        );
        denied_operation!(
            get_application_installation,
            RequestContext,
            riffdb_service::GetApplicationInstallationRequest,
            riffdb_service::GetApplicationInstallationResult
        );
    }

    impl ContractApplication for ClosedApplicationService {
        denied_operation!(
            validate_contract,
            RequestContext,
            riffdb_service::ValidateContractRequest,
            riffdb_service::ContractValidationResult
        );
        denied_operation!(
            explain_command,
            RequestContext,
            riffdb_service::ExplainCommandRequest,
            riffdb_service::ExplainCommandResult
        );
        denied_operation!(
            deploy_contract,
            RequestContext,
            riffdb_service::DeployContractRequest,
            riffdb_service::DeployContractResult
        );
        denied_operation!(
            get_active_contract,
            RequestContext,
            riffdb_service::GetActiveContractRequest,
            riffdb_service::GetActiveContractResult
        );
        denied_operation!(
            get_contract_version,
            RequestContext,
            riffdb_service::GetContractVersionRequest,
            riffdb_service::GetContractVersionResult
        );
    }

    impl CommandApplication for ClosedApplicationService {
        denied_operation!(
            execute_command,
            RequestContext,
            riffdb_service::ExecuteCommandRequest,
            riffdb_service::ExecuteCommandResult
        );
        denied_operation!(
            resolve_command_outcome,
            RequestContext,
            riffdb_service::ResolveCommandOutcomeRequest,
            riffdb_service::ResolveCommandOutcomeResult
        );
    }

    impl QueryApplication for ClosedApplicationService {
        denied_operation!(
            get_entity,
            RequestContext,
            riffdb_service::GetEntityRequest,
            riffdb_service::GetEntityResult
        );
        denied_operation!(
            scan_index,
            RequestContext,
            riffdb_service::ScanIndexRequest,
            riffdb_service::ScanIndexResult
        );
        denied_operation!(
            query_projection,
            RequestContext,
            riffdb_service::QueryProjectionRequest,
            riffdb_service::QueryProjectionResult
        );
        denied_operation!(
            get_projection_status,
            RequestContext,
            riffdb_service::GetProjectionStatusRequest,
            riffdb_service::GetProjectionStatusResult
        );
        denied_operation!(
            inspect_vector_state,
            RequestContext,
            riffdb_service::InspectVectorStateRequest,
            riffdb_service::InspectVectorStateResult
        );
    }

    impl EventServiceApplication for ClosedApplicationService {
        denied_operation!(
            describe_event,
            RequestContext,
            riffdb_service::DescribeEventRequest,
            riffdb_service::DescribeEventResult
        );
        denied_operation!(
            replay_events,
            RequestContext,
            riffdb_service::ReplayEventsRequest,
            riffdb_service::ReplayEventsResult
        );
        denied_operation!(
            tail_events,
            RequestContext,
            riffdb_service::TailEventsRequest,
            riffdb_service::TailEventsResult
        );
    }

    impl riffdb_service::EventConsumerServiceApplication for ClosedApplicationService {
        denied_operation!(
            consume_event_stream,
            RequestContext,
            riffdb_service::ConsumeEventStreamRequest,
            riffdb_service::ConsumeEventStreamResult
        );
        denied_operation!(
            acknowledge_event_stream,
            RequestContext,
            riffdb_service::EventConsumerLeaseSelection,
            riffdb_service::EventConsumerMutationResult
        );
        denied_operation!(
            negative_acknowledge_event_stream,
            RequestContext,
            riffdb_service::NegativeAcknowledgeEventStreamRequest,
            riffdb_service::EventConsumerMutationResult
        );
        denied_operation!(
            seek_event_stream_consumer,
            RequestContext,
            riffdb_service::SeekEventStreamConsumerRequest,
            riffdb_service::EventConsumerMutationResult
        );
        denied_operation!(
            retire_event_stream_consumer,
            RequestContext,
            riffdb_service::EventConsumerSelection,
            riffdb_service::EventConsumerMutationResult
        );
        denied_operation!(
            get_event_stream_consumer_status,
            RequestContext,
            riffdb_service::EventConsumerSelection,
            Option<riffdb_service::EventConsumerPublicStatus>
        );
    }

    impl riffdb_service::ContextualSubscriptionApplication for ClosedApplicationService {
        denied_operation!(
            consume_contextual_subscription,
            RequestContext,
            riffdb_service::ConsumeContextualSubscriptionRequest,
            riffdb_service::ConsumeContextualSubscriptionResult
        );
        denied_operation!(
            acknowledge_contextual_subscription,
            RequestContext,
            riffdb_service::EventConsumerLeaseSelection,
            riffdb_service::EventConsumerMutationResult
        );

        fn negative_acknowledge_contextual_subscription(
            &self,
            _context: RequestContext,
            _lease: riffdb_service::EventConsumerLeaseSelection,
            _retry_delay: Duration,
        ) -> ServiceFuture<'_, riffdb_service::EventConsumerMutationResult> {
            denied()
        }

        denied_operation!(
            get_contextual_subscription_status,
            RequestContext,
            riffdb_service::EventConsumerSelection,
            Option<riffdb_service::EventConsumerPublicStatus>
        );
        denied_operation!(
            execute_contextual_reaction,
            RequestContext,
            riffdb_service::ExecuteContextualReactionRequest,
            riffdb_service::ExecuteCommandResult
        );
    }

    impl SymbolicQueryApplication for ClosedApplicationService {
        denied_operation!(
            describe_symbolic_contract,
            RequestContext,
            riffdb_service::SymbolicContractSelector,
            riffdb_service::DescribeSymbolicContractResult
        );
        denied_operation!(
            get_application_catalog,
            RequestContext,
            riffdb_service::ApplicationCatalogRequest,
            riffdb_service::ApplicationCatalogResult
        );
        denied_operation!(
            check_symbolic_query,
            RequestContext,
            riffdb_service::CompileSymbolicQueryRequest,
            riffdb_service::CheckSymbolicQueryResult
        );
        denied_operation!(
            explain_symbolic_query,
            RequestContext,
            riffdb_service::CompileSymbolicQueryRequest,
            riffdb_service::ExplainSymbolicQueryResult
        );
        denied_operation!(
            execute_symbolic_query,
            RequestContext,
            riffdb_service::ExecuteSymbolicQueryRequest,
            riffdb_service::ExecuteSymbolicQueryResult
        );
        denied_operation!(
            deploy_query_module,
            RequestContext,
            riffdb_service::DeployQueryModuleRequest,
            riffdb_service::DeployQueryModuleResult
        );
        denied_operation!(
            deploy_reactive_module,
            RequestContext,
            riffdb_service::DeployReactiveModuleRequest,
            riffdb_service::DeployReactiveModuleResult
        );
        denied_operation!(
            get_query_module,
            RequestContext,
            riffdb_service::GetQueryModuleRequest,
            Option<riffdb_service::QueryModuleInspection>
        );
        denied_operation!(
            explain_named_symbolic_query,
            RequestContext,
            riffdb_service::NamedSymbolicQueryRequest,
            riffdb_service::ExplainSymbolicQueryResult
        );
        denied_operation!(
            execute_named_symbolic_query,
            RequestContext,
            riffdb_service::NamedSymbolicQueryRequest,
            riffdb_service::ExecuteSymbolicQueryResult
        );
    }

    impl riffdb_service::LiveNamedQueryApplication for ClosedApplicationService {
        denied_operation!(
            watch_live_named_query,
            RequestContext,
            riffdb_service::WatchLiveNamedQueryRequest,
            riffdb_service::WatchLiveNamedQueryResult
        );
    }

    impl riffdb_service::ProjectedQueryApplication for ClosedApplicationService {
        denied_operation!(
            execute_projected_query,
            RequestContext,
            riffdb_service::ExecuteProjectedQueryRequest,
            riffdb_service::ExecuteProjectedQueryResult
        );
    }

    impl CommitApplication for ClosedApplicationService {
        denied_operation!(
            get_commit,
            RequestContext,
            riffdb_service::GetCommitRequest,
            riffdb_service::GetCommitResult
        );
        denied_operation!(
            scan_commits,
            RequestContext,
            riffdb_service::ScanCommitsRequest,
            riffdb_service::ScanCommitsResult
        );
        denied_operation!(
            subscribe_to_commits,
            RequestContext,
            riffdb_service::SubscribeToCommitsRequest,
            riffdb_service::SubscribeToCommitsResult
        );
        denied_operation!(
            trace_provenance,
            RequestContext,
            riffdb_service::TraceProvenanceRequest,
            riffdb_service::TraceProvenanceResult
        );
    }

    impl AdministrationApplication for ClosedApplicationService {
        denied_operation!(health, HealthContext, HealthRequest, HealthResult);
        denied_operation!(
            statistics,
            RequestContext,
            riffdb_service::StatisticsRequest,
            riffdb_service::StatisticsResult
        );

        fn create_capability(
            &self,
            _invocation: CreateCapabilityInvocation,
        ) -> ServiceFuture<'_, CreateCapabilityResult> {
            denied()
        }

        denied_operation!(
            revoke_capability,
            RequestContext,
            riffdb_service::RevokeCapabilityRequest,
            riffdb_service::RevokeCapabilityResult
        );
        denied_operation!(
            list_pending_outbox_deliveries,
            RequestContext,
            riffdb_service::ListPendingOutboxDeliveriesRequest,
            riffdb_service::ListPendingOutboxDeliveriesResult
        );
    }

    impl OfflineMaintenanceApplication for ClosedApplicationService {
        denied_operation!(
            create_offline_backup,
            RequestContext,
            riffdb_service::CreateOfflineBackupRequest,
            riffdb_service::OfflineMaintenanceStartResult
        );

        fn restore_offline_backup(
            &self,
            _invocation: riffdb_service::RestoreOfflineBackupInvocation,
        ) -> ServiceFuture<'_, riffdb_service::OfflineMaintenanceStartResult> {
            denied()
        }

        denied_operation!(
            retire_offline_backup,
            RequestContext,
            riffdb_service::RetireOfflineBackupRequest,
            riffdb_service::OfflineMaintenanceStartResult
        );

        denied_operation!(
            get_offline_maintenance_operation,
            RequestContext,
            riffdb_service::GetOfflineMaintenanceOperationRequest,
            riffdb_service::GetOfflineMaintenanceOperationResult
        );
    }

    impl RecoveryOfflineMaintenanceApplication for ClosedApplicationService {
        fn restore_offline_backup(
            &self,
            _invocation: riffdb_service::RecoveryRestoreOfflineBackupInvocation,
        ) -> ServiceFuture<'_, riffdb_service::OfflineMaintenanceStartResult> {
            denied()
        }
    }

    impl RestoreRetryOfflineMaintenanceApplication for ClosedApplicationService {
        fn restore_offline_backup(
            &self,
            _invocation: riffdb_service::RestoreOfflineBackupInvocation,
        ) -> ServiceFuture<'_, riffdb_service::OfflineMaintenanceStartResult> {
            denied()
        }
    }

    impl DiscoveryApplication for ClosedApplicationService {
        denied_operation!(
            discover_command_tools,
            RequestContext,
            riffdb_service::DiscoverCommandToolsRequest,
            riffdb_service::DiscoverCommandToolsResult
        );
        denied_operation!(
            discover_resources,
            RequestContext,
            riffdb_service::DiscoverResourcesRequest,
            riffdb_service::DiscoverResourcesResult
        );

        fn get_reactive_wakeup(
            &self,
            _context: RequestContext,
        ) -> ServiceFuture<'_, riffdb_service::GetReactiveWakeupResult> {
            denied()
        }
    }

    fn test_security_context() -> CheckedGrpcSecurityContext {
        let authentication = AuthenticationContext::new(
            DatabaseId::from_unix_milliseconds_and_random(1, [0; 10]).expect("valid database ID"),
            Environment::new("lifecycle-test").expect("valid environment"),
            Audience::new("lifecycle-grpc").expect("valid audience"),
        );
        let keys = CapabilityDigestKeyProvider::parse_document(
            b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
        )
        .expect("valid capability digest keys");
        CheckedGrpcSecurityContext::new(
            Arc::new(RejectingAuthenticator),
            authentication,
            Arc::new(keys),
        )
    }

    fn test_restore_retry_security_context() -> CheckedGrpcRestoreRetrySecurityContext {
        CheckedGrpcRestoreRetrySecurityContext::new(
            Arc::new(RejectingAuthenticator),
            AuthenticationContext::new(
                DatabaseId::from_unix_milliseconds_and_random(1, [0; 10])
                    .expect("valid database ID"),
                Environment::new("lifecycle-test").expect("valid environment"),
                Audience::new("lifecycle-grpc").expect("valid audience"),
            ),
        )
    }

    fn test_server_generation() -> ServerGenerationV1 {
        ServerGenerationV1::for_test([0xa5; SERVER_GENERATION_BYTES])
    }

    fn maintenance_input_hash(byte: u8) -> riffdb_types::OfflineMaintenanceInputHash {
        riffdb_types::OfflineMaintenanceInputHash::from_bytes([byte; 32])
    }

    fn installed_route(
        lifecycle: ValidatedStartupLifecycle,
    ) -> (Arc<ProductionLifecycleRoute>, Arc<dyn ApplicationService>) {
        let (initializing, _activator, issuer) =
            riffdb_service::RiffDbService::begin_initialization();
        let route = Arc::new(ProductionLifecycleRoute::new(
            initializing,
            issuer,
            RuntimeRoutingState::new(),
        ));
        let service: Arc<dyn ApplicationService> = Arc::new(ClosedApplicationService);
        route
            .install_activated(
                Arc::clone(&service),
                test_security_context(),
                test_server_generation(),
                1,
                lifecycle,
                ValidatedAllocatorCapacity::Available,
            )
            .expect("one checked activated target");
        (route, service)
    }

    fn model(
        lifecycle: ValidatedStartupLifecycle,
        capacity: ValidatedAllocatorCapacity,
    ) -> LifecycleModel {
        LifecycleModel::from_startup(lifecycle, capacity).expect("valid lifecycle")
    }

    #[test]
    fn authenticated_operation_matrix_is_exhaustive_and_fail_closed() {
        let initializing = LifecycleModel::initializing();
        let bootstrap = model(
            ValidatedStartupLifecycle::BootstrapRequired,
            ValidatedAllocatorCapacity::Available,
        );
        let deployment = model(
            ValidatedStartupLifecycle::DeploymentRequired,
            ValidatedAllocatorCapacity::Available,
        );
        let ready = model(
            ValidatedStartupLifecycle::ActiveContract,
            ValidatedAllocatorCapacity::Available,
        );
        let exhausted = model(
            ValidatedStartupLifecycle::ActiveContract,
            ValidatedAllocatorCapacity::ApplicationExhausted,
        );
        let stopped = LifecycleModel {
            stage: LifecycleStage::Stopped,
            active_observed: true,
        };

        for operation in ServiceOperationV1::ALL {
            assert!(!initializing.allows_authenticated(operation, true));
            assert!(!bootstrap.allows_authenticated(operation, true));
            assert_eq!(
                deployment.allows_authenticated(operation, true),
                matches!(
                    operation,
                    ServiceOperationV1::GetHealth
                        | ServiceOperationV1::ValidateContract
                        | ServiceOperationV1::ExplainCommand
                        | ServiceOperationV1::DeployContract
                        | ServiceOperationV1::GetActiveContract
                        | ServiceOperationV1::CreateCapability
                        | ServiceOperationV1::RevokeCapability
                        | ServiceOperationV1::RegisterFollower
                        | ServiceOperationV1::RetireFollower
                        | ServiceOperationV1::DiscoverCommandTools
                        | ServiceOperationV1::DiscoverResources
                )
            );
            assert!(ready.allows_authenticated(operation, true));
            assert_eq!(
                exhausted.allows_authenticated(operation, true),
                operation == ServiceOperationV1::GetHealth
            );
            assert!(!stopped.allows_authenticated(operation, true));
        }
    }

    #[test]
    fn health_availability_is_independent_of_authoritative_runtime_readiness() {
        for lifecycle in [
            ValidatedStartupLifecycle::DeploymentRequired,
            ValidatedStartupLifecycle::ActiveContract,
        ] {
            let stable = model(lifecycle, ValidatedAllocatorCapacity::Available);
            assert!(stable.allows_authenticated(ServiceOperationV1::GetHealth, false));
            assert!(!stable.allows_authenticated(ServiceOperationV1::DeployContract, false));
            assert!(!stable.allows_authenticated(ServiceOperationV1::GetEntity, false));

            let exhausted = model(
                lifecycle,
                ValidatedAllocatorCapacity::AdministrationExhausted,
            );
            assert!(exhausted.allows_authenticated(ServiceOperationV1::GetHealth, false));
            assert!(!exhausted.allows_authenticated(ServiceOperationV1::DeployContract, true));
        }
    }

    #[test]
    fn marker_absent_exhaustion_is_rejected_for_every_exhausted_capacity() {
        for capacity in [
            ValidatedAllocatorCapacity::ApplicationExhausted,
            ValidatedAllocatorCapacity::AdministrationExhausted,
            ValidatedAllocatorCapacity::BothExhausted,
        ] {
            assert_eq!(
                LifecycleModel::from_startup(
                    ValidatedStartupLifecycle::BootstrapRequired,
                    capacity,
                ),
                Err(LifecycleInstallError::MarkerAbsentAllocatorExhausted)
            );
        }
    }

    #[test]
    fn bootstrap_completion_uses_the_monotonic_active_observation() {
        let mut pre_active = model(
            ValidatedStartupLifecycle::DeploymentRequired,
            ValidatedAllocatorCapacity::Available,
        );
        assert!(pre_active.begin_bootstrap());
        pre_active.finish_bootstrap(GrpcBootstrapCompletion::Conflict);
        assert_eq!(pre_active.stage, LifecycleStage::DeploymentRequired);

        let mut active = model(
            ValidatedStartupLifecycle::ActiveContract,
            ValidatedAllocatorCapacity::Available,
        );
        assert!(active.begin_bootstrap());
        active.finish_bootstrap(GrpcBootstrapCompletion::Conflict);
        assert_eq!(active.stage, LifecycleStage::Ready);

        let mut concurrent = model(
            ValidatedStartupLifecycle::DeploymentRequired,
            ValidatedAllocatorCapacity::Available,
        );
        assert!(concurrent.begin_bootstrap());
        concurrent.finish_deployment(GrpcDeploymentCompletion::Activated);
        assert_eq!(concurrent.stage, LifecycleStage::BootstrapInFlight);
        concurrent.finish_bootstrap(GrpcBootstrapCompletion::Replayed);
        assert_eq!(concurrent.stage, LifecycleStage::Ready);
    }

    #[test]
    fn deployment_results_never_demote_an_observed_active_catalog() {
        let mut deployment = model(
            ValidatedStartupLifecycle::DeploymentRequired,
            ValidatedAllocatorCapacity::Available,
        );
        deployment.finish_deployment(GrpcDeploymentCompletion::Activated);
        assert_eq!(deployment.stage, LifecycleStage::Ready);
        deployment.finish_deployment(GrpcDeploymentCompletion::NotActivated);
        assert_eq!(deployment.stage, LifecycleStage::Ready);
        deployment.finish_deployment(GrpcDeploymentCompletion::AlreadyActive);
        assert_eq!(deployment.stage, LifecycleStage::Ready);
    }

    #[test]
    fn no_terminal_completion_resurrects_stopped_routing() {
        for bootstrap in [
            GrpcBootstrapCompletion::Created,
            GrpcBootstrapCompletion::Replayed,
            GrpcBootstrapCompletion::Conflict,
            GrpcBootstrapCompletion::OutcomeUnknown,
            GrpcBootstrapCompletion::Failed,
            GrpcBootstrapCompletion::Abandoned,
        ] {
            let mut stopped = LifecycleModel {
                stage: LifecycleStage::Stopped,
                active_observed: true,
            };
            stopped.finish_bootstrap(bootstrap);
            assert_eq!(stopped.stage, LifecycleStage::Stopped);
        }
        for deployment in [
            GrpcDeploymentCompletion::Activated,
            GrpcDeploymentCompletion::AlreadyActive,
            GrpcDeploymentCompletion::NotActivated,
            GrpcDeploymentCompletion::OutcomeUnknown,
            GrpcDeploymentCompletion::Abandoned,
        ] {
            let mut stopped = LifecycleModel {
                stage: LifecycleStage::Stopped,
                active_observed: true,
            };
            stopped.finish_deployment(deployment);
            assert_eq!(stopped.stage, LifecycleStage::Stopped);
        }
    }

    #[tokio::test]
    async fn initializing_route_serves_only_the_restricted_health_shape() {
        let (initializing, _activator, issuer) =
            riffdb_service::RiffDbService::begin_initialization();
        let route = ProductionLifecycleRoute::new(initializing, issuer, RuntimeRoutingState::new());
        let result = route
            .restricted_health(HealthRequest)
            .expect("initializing Health")
            .await
            .expect("restricted result");
        let HealthResult::PreBootstrap(report) = result else {
            panic!("initializing route returned authenticated Health");
        };
        assert_eq!(
            report.lifecycle(),
            PreBootstrapLifecycle::InitializingValidation
        );
        assert!(!report.readiness());
        assert!(!route.bootstrap_available());
        for operation in ServiceOperationV1::ALL {
            assert!(route.admit_authenticated(operation).is_none());
        }
    }

    #[tokio::test]
    async fn production_route_atomically_replaces_the_initializing_target() {
        let (initializing, _activator, issuer) =
            riffdb_service::RiffDbService::begin_initialization();
        let route = ProductionLifecycleRoute::new(initializing, issuer, RuntimeRoutingState::new());
        let initial = route
            .restricted_health(HealthRequest)
            .expect("initializing target serves restricted Health")
            .await
            .expect("initializing Health result");
        assert!(matches!(initial, HealthResult::PreBootstrap(_)));
        assert!(route.security_context().is_none());
        assert!(route.server_generation().is_none());
        assert!(
            route
                .admit_authenticated(ServiceOperationV1::GetEntity)
                .is_none()
        );

        let activated: Arc<dyn ApplicationService> = Arc::new(ClosedApplicationService);
        route
            .install_activated(
                Arc::clone(&activated),
                test_security_context(),
                test_server_generation(),
                1,
                ValidatedStartupLifecycle::ActiveContract,
                ValidatedAllocatorCapacity::Available,
            )
            .expect("checked startup target installs once");

        assert!(route.restricted_health(HealthRequest).is_none());
        assert!(route.security_context().is_some());
        assert_eq!(
            route.server_generation(),
            Some([0xa5; SERVER_GENERATION_BYTES])
        );
        let admitted = route
            .admit_authenticated(ServiceOperationV1::GetEntity)
            .expect("ready target admits entity reads");
        assert!(Arc::ptr_eq(&admitted, &activated));
        assert_eq!(
            route.install_activated(
                Arc::new(ClosedApplicationService),
                test_security_context(),
                test_server_generation(),
                1,
                ValidatedStartupLifecycle::ActiveContract,
                ValidatedAllocatorCapacity::Available,
            ),
            Err(LifecycleInstallError::AlreadyInstalled)
        );
    }

    #[test]
    fn read_stage_telemetry_is_published_only_when_activation_supplies_a_sink() {
        use std::sync::Mutex as StdMutex;

        use riffdb_service::{ServiceTelemetry, ServiceTelemetryEvent};

        #[derive(Default)]
        struct RecordingTelemetry {
            stages: StdMutex<Vec<riffdb_service::ReadPipelineStage>>,
        }

        impl ServiceTelemetry for RecordingTelemetry {
            fn record(&self, event: ServiceTelemetryEvent) {
                if let ServiceTelemetryEvent::ReadPipelineStageCompleted { stage, .. } = event {
                    self.stages.lock().expect("stage mutex").push(stage);
                }
            }
        }

        // Activation without a sink: the route publishes nothing and the gRPC
        // layer simply does not record residual stages.
        let (initializing, _activator, issuer) =
            riffdb_service::RiffDbService::begin_initialization();
        let without =
            ProductionLifecycleRoute::new(initializing, issuer, RuntimeRoutingState::new());
        assert!(without.read_stage_telemetry().is_none());
        without
            .install_activated(
                Arc::new(ClosedApplicationService),
                test_security_context(),
                test_server_generation(),
                1,
                ValidatedStartupLifecycle::ActiveContract,
                ValidatedAllocatorCapacity::Available,
            )
            .expect("activation installs once");
        assert!(
            without.read_stage_telemetry().is_none(),
            "an activation without a telemetry sink must publish none"
        );

        // Activation with a sink: the published sink is the one composition
        // supplied, and recording through it reaches that exact sink.
        let (initializing, _activator, issuer) =
            riffdb_service::RiffDbService::begin_initialization();
        let with = ProductionLifecycleRoute::new(initializing, issuer, RuntimeRoutingState::new());
        let sink = Arc::new(RecordingTelemetry::default());
        with.install_activated_with_telemetry(
            Arc::new(ClosedApplicationService),
            test_security_context(),
            test_server_generation(),
            1,
            ValidatedStartupLifecycle::ActiveContract,
            ValidatedAllocatorCapacity::Available,
            Some(Arc::clone(&sink) as Arc<dyn ServiceTelemetry>),
        )
        .expect("activation installs once");
        let published = with
            .read_stage_telemetry()
            .expect("an activation with a telemetry sink must publish it");
        published.record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage: riffdb_service::ReadPipelineStage::TransportAdapt,
            elapsed: std::time::Duration::from_micros(3),
        });
        assert_eq!(
            sink.stages.lock().expect("stage mutex").as_slice(),
            [riffdb_service::ReadPipelineStage::TransportAdapt]
        );
    }

    #[test]
    fn credential_retry_route_exposes_only_narrow_exact_restore_and_current_security() {
        let (initializing, _activator, issuer) =
            riffdb_service::RiffDbService::begin_initialization();
        let maintenance = Arc::new(MaintenanceLifecycle::ready());
        let route = ProductionLifecycleRoute::new_with_maintenance(
            initializing,
            issuer,
            RuntimeRoutingState::new(),
            Arc::clone(&maintenance),
        );
        let operation =
            riffdb_types::OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(
                1, [0x5a; 10],
            )
            .expect("valid maintenance operation ID");
        let unrelated =
            riffdb_types::OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(
                1, [0x6b; 10],
            )
            .expect("valid unrelated maintenance operation ID");
        let input_hash = maintenance_input_hash(0x31);
        maintenance
            .await_restore_retry(operation, input_hash)
            .expect("install exact restore retry");
        let retry: Arc<dyn RestoreRetryOfflineMaintenanceApplication> =
            Arc::new(ClosedApplicationService);
        route
            .install_restore_retry(
                operation,
                input_hash,
                Arc::clone(&retry),
                test_restore_retry_security_context(),
            )
            .expect("exact current-database retry installs");

        for service_operation in ServiceOperationV1::ALL {
            assert!(
                route.admit_authenticated(service_operation).is_none(),
                "ordinary operation {service_operation:?} must remain closed"
            );
        }
        assert!(
            route
                .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::CreateBackup)
                .is_none()
        );
        assert!(
            route
                .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::GetOperation)
                .is_none()
        );
        assert!(
            route
                .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::RestoreBackup {
                    operation_id: unrelated,
                    input_hash,
                })
                .is_none(),
            "an unrelated operation ID must not cross the current-target authentication boundary"
        );
        assert!(
            route
                .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::RestoreBackup {
                    operation_id: operation,
                    input_hash,
                })
                .is_none(),
            "the broad application service must remain unavailable"
        );
        let restore = route
            .admit_restore_retry(operation, input_hash)
            .expect("exact restore transport identity is admitted");
        assert!(Arc::ptr_eq(&restore, &retry));
        assert!(
            route.admit_restore_retry(unrelated, input_hash).is_none(),
            "an unrelated operation ID must not cross the current-target authentication boundary"
        );
        assert!(
            route
                .admit_restore_retry(operation, maintenance_input_hash(0x32))
                .is_none(),
            "mismatched immutable input must not cross the authentication boundary"
        );
        assert!(
            route
                .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::RestoreBackup {
                    operation_id: unrelated,
                    input_hash: maintenance_input_hash(0x32),
                })
                .is_none()
        );
        assert!(route.security_context().is_none());
        assert!(route.restore_retry_security_context().is_some());
        assert!(route.server_generation().is_none());
        assert!(route.restricted_health(HealthRequest).is_none());
        assert!(!route.bootstrap_available());
        assert!(route.begin_bootstrap().is_none());
        assert!(
            route
                .admit_recovery_restore(operation, input_hash)
                .is_none()
        );
        assert!(
            route
                .admit_recovery_restore(unrelated, input_hash)
                .is_none()
        );

        assert_eq!(
            maintenance.claim_nonterminal_receipt(operation),
            Ok(crate::maintenance_lifecycle::MaintenanceReceiptClaim::Reacquired)
        );
        assert!(route.admit_restore_retry(operation, input_hash).is_none());
        assert!(route.security_context().is_none());
        assert!(route.restore_retry_security_context().is_none());
    }

    #[test]
    fn recovery_route_fences_staged_authentication_by_checked_operation_id() {
        let (initializing, _activator, issuer) =
            riffdb_service::RiffDbService::begin_initialization();
        let generic_maintenance = Arc::new(MaintenanceLifecycle::ready());
        generic_maintenance
            .enter_recovery_mode()
            .expect("generic recovery mode starts from closed ordinary readiness");
        let generic_route = ProductionLifecycleRoute::new_with_maintenance(
            initializing,
            issuer,
            RuntimeRoutingState::new(),
            Arc::clone(&generic_maintenance),
        );
        let recovery: Arc<dyn RecoveryOfflineMaintenanceApplication> =
            Arc::new(ClosedApplicationService);
        generic_route
            .install_recovery(Arc::clone(&recovery))
            .expect("restricted recovery service installs");
        let operation =
            riffdb_types::OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(
                1, [0x7c; 10],
            )
            .expect("valid maintenance operation ID");
        let unrelated =
            riffdb_types::OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(
                1, [0x8d; 10],
            )
            .expect("valid unrelated maintenance operation ID");
        let input_hash = maintenance_input_hash(0x41);

        let admitted = generic_route
            .admit_recovery_restore(operation, input_hash)
            .expect("generic recovery admits one structurally checked identity");
        assert!(Arc::ptr_eq(&admitted, &recovery));
        let admitted = generic_route
            .admit_recovery_restore(unrelated, maintenance_input_hash(0x42))
            .expect("generic recovery has no preexisting receipt identity");
        assert!(Arc::ptr_eq(&admitted, &recovery));

        let (initializing, _activator, issuer) =
            riffdb_service::RiffDbService::begin_initialization();
        let exact_maintenance = Arc::new(MaintenanceLifecycle::ready());
        exact_maintenance
            .await_recovery_retry(operation, input_hash)
            .expect("recovered receipt freezes the exact identity");
        let exact_route = ProductionLifecycleRoute::new_with_maintenance(
            initializing,
            issuer,
            RuntimeRoutingState::new(),
            exact_maintenance,
        );
        exact_route
            .install_recovery(Arc::clone(&recovery))
            .expect("exact restricted recovery service installs");
        assert!(
            exact_route
                .admit_recovery_restore(unrelated, input_hash)
                .is_none(),
            "unrelated identity must not reach staged authentication"
        );
        assert!(
            exact_route
                .admit_recovery_restore(operation, maintenance_input_hash(0x43))
                .is_none(),
            "mismatched immutable input must not reach staged authentication"
        );
        let admitted = exact_route
            .admit_recovery_restore(operation, input_hash)
            .expect("the frozen recovery identity remains retryable");
        assert!(Arc::ptr_eq(&admitted, &recovery));
    }

    #[derive(Debug, Eq, PartialEq)]
    enum CoordinatedHealthOutcome {
        ClosedBeforeIssue,
        AdmittedBeforeClose,
        Unexpected,
    }

    #[test]
    fn target_installation_closes_every_side_of_a_health_race() {
        let (initializing, _activator, issuer) =
            riffdb_service::RiffDbService::begin_initialization();
        let route = Arc::new(ProductionLifecycleRoute::new(
            initializing,
            issuer,
            RuntimeRoutingState::new(),
        ));
        let start = Arc::new(Barrier::new(2));
        let installed = Arc::new(Barrier::new(2));
        let health_route = Arc::clone(&route);
        let health_start = Arc::clone(&start);
        let health_installed = Arc::clone(&installed);
        let health = thread::spawn(move || {
            health_start.wait();
            let future = health_route.restricted_health(HealthRequest);
            health_installed.wait();
            let Some(mut future) = future else {
                return CoordinatedHealthOutcome::ClosedBeforeIssue;
            };
            let mut context = Context::from_waker(Waker::noop());
            match future.as_mut().poll(&mut context) {
                Poll::Ready(Ok(HealthResult::PreBootstrap(report)))
                    if report.lifecycle() == PreBootstrapLifecycle::InitializingValidation =>
                {
                    CoordinatedHealthOutcome::AdmittedBeforeClose
                }
                Poll::Ready(Ok(_)) | Poll::Ready(Err(_)) | Poll::Pending => {
                    CoordinatedHealthOutcome::Unexpected
                }
            }
        });

        start.wait();
        let activated: Arc<dyn ApplicationService> = Arc::new(ClosedApplicationService);
        route
            .install_activated(
                Arc::clone(&activated),
                test_security_context(),
                test_server_generation(),
                1,
                ValidatedStartupLifecycle::ActiveContract,
                ValidatedAllocatorCapacity::Available,
            )
            .expect("checked active target installs");
        installed.wait();

        assert!(matches!(
            health.join().expect("Health racer completed"),
            CoordinatedHealthOutcome::ClosedBeforeIssue
                | CoordinatedHealthOutcome::AdmittedBeforeClose
        ));
        assert!(route.restricted_health(HealthRequest).is_none());
        let admitted = route
            .admit_authenticated(ServiceOperationV1::GetHealth)
            .expect("replacement target owns authenticated Health");
        assert!(Arc::ptr_eq(&admitted, &activated));
    }

    #[test]
    fn known_bootstrap_success_enters_authenticated_deployment_routing() {
        let (route, activated) = installed_route(ValidatedStartupLifecycle::BootstrapRequired);
        assert!(route.restricted_health(HealthRequest).is_some());

        let bootstrap = route
            .begin_bootstrap()
            .expect("one checked bootstrap begins");
        assert!(Arc::ptr_eq(&bootstrap, &activated));
        assert!(!route.bootstrap_available());
        assert!(route.restricted_health(HealthRequest).is_none());
        for operation in ServiceOperationV1::ALL {
            assert!(route.admit_authenticated(operation).is_none());
        }

        route.finish_bootstrap(GrpcBootstrapCompletion::Created);
        for operation in ServiceOperationV1::ALL {
            let expected = matches!(
                operation,
                ServiceOperationV1::GetHealth
                    | ServiceOperationV1::ValidateContract
                    | ServiceOperationV1::ExplainCommand
                    | ServiceOperationV1::DeployContract
                    | ServiceOperationV1::GetActiveContract
                    | ServiceOperationV1::CreateCapability
                    | ServiceOperationV1::RevokeCapability
                    | ServiceOperationV1::RegisterFollower
                    | ServiceOperationV1::RetireFollower
                    | ServiceOperationV1::DiscoverCommandTools
                    | ServiceOperationV1::DiscoverResources
            );
            let admitted = route.admit_authenticated(operation);
            assert_eq!(admitted.is_some(), expected, "operation {operation:?}");
            if let Some(admitted) = admitted {
                assert!(Arc::ptr_eq(&admitted, &activated));
            }
        }
        assert!(route.security_context().is_some());
        assert!(route.restricted_health(HealthRequest).is_none());
    }

    #[test]
    fn uncertain_bootstrap_completion_irreversibly_stops_production_routing() {
        let (route, _activated) = installed_route(ValidatedStartupLifecycle::BootstrapRequired);
        assert!(route.begin_bootstrap().is_some());

        route.finish_bootstrap(GrpcBootstrapCompletion::OutcomeUnknown);
        assert!(!route.bootstrap_available());
        assert!(route.security_context().is_none());
        assert!(route.restricted_health(HealthRequest).is_none());
        for operation in ServiceOperationV1::ALL {
            assert!(route.admit_authenticated(operation).is_none());
        }

        route.finish_bootstrap(GrpcBootstrapCompletion::Created);
        route.finish_deployment(GrpcDeploymentCompletion::Activated);
        assert!(!route.bootstrap_available());
        assert!(route.security_context().is_none());
        for operation in ServiceOperationV1::ALL {
            assert!(route.admit_authenticated(operation).is_none());
        }
    }
}
