//! Concrete API-neutral service composition and independent job ownership.

use std::error::Error;
use std::fmt;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::task::Poll;
use std::time::Instant;

use riffdb_commit::{
    AdministrationAuditExecutor, CommandExecutor, CommandIdempotencyInspector, ControlPlaneExecutor,
};
use riffdb_errors::{IncidentIdSource, InternalError, PublicError, PublicErrorKind};
use riffdb_policy::AgentSessionAdmissionPolicy;
use riffdb_query_executor::QueryExecutionPort;
use riffdb_types::{DatabaseId, Environment, ServiceOperationV1, Timestamp};

use crate::ContextualCausationTokenCodec;
use crate::context::PreBootstrapHealthAdmission;
use crate::orchestration::{
    ContainedAuditFailure, OperationAuditLifecycle, with_operation_audit_lifecycle,
};
use crate::{
    ApplicationExportCoordinatorPort, ApplicationInstallationCoordinatorPort,
    ApplicationReimportCoordinatorPort, AuthoritativeReadPort, BuildInfo, CapabilityTokenIssuer,
    CatalogReadPort, ColumnarProjectionPort, ContractMigrationCoordinatorPort, CurrentPolicyPort,
    CursorMonotonicClock, CursorTokenGenerator, EventConsumerClock, EventConsumerPort,
    EventLeaseTokenSource, HealthRequest, HealthResult, LiveQueryClock,
    OfflineMaintenanceCoordinatorPort, OperationalStatusPort, OutboxStatusPort, PortDriverStopped,
    PortReceipt, PreBootstrapHealthContext, PreBootstrapHealthContextIssuer,
    PreBootstrapHealthReport, ProjectionQueryPort, QueryModuleReadPort, ReactiveModuleReadPort,
    RequestDeadlineScheduler, ServiceCursorRegistries, ServiceDiagnostics, ServiceFailure,
    ServiceFuture, ServiceHealthHooks, ServiceJob, ServiceJobSpawner, ServiceResponseCharge,
    ServiceResult, ServiceTelemetry, ServiceTelemetryEvent, ensure_response_budget,
    port_completion_channel,
};

/// Trusted immutable process facts displayed by authenticated health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceProcessMetadata {
    started_at: Timestamp,
    build: BuildInfo,
}

impl ServiceProcessMetadata {
    /// Joins the process start instant and checked build manifest.
    #[must_use]
    pub const fn new(started_at: Timestamp, build: BuildInfo) -> Self {
        Self { started_at, build }
    }

    /// Returns the process start timestamp captured by the server composition.
    #[must_use]
    pub const fn started_at(&self) -> Timestamp {
        self.started_at
    }

    /// Borrows immutable checked build metadata.
    #[must_use]
    pub const fn build(&self) -> &BuildInfo {
        &self.build
    }
}

/// Trusted process identity and command-claim admission policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceIdentity {
    database_id: DatabaseId,
    environment: Environment,
    agent_session_policy: AgentSessionAdmissionPolicy,
    history_incarnation: u64,
}

impl ServiceIdentity {
    /// Joins values fixed by the production database process.
    #[must_use]
    pub const fn new(
        database_id: DatabaseId,
        environment: Environment,
        agent_session_policy: AgentSessionAdmissionPolicy,
        history_incarnation: u64,
    ) -> Self {
        Self {
            database_id,
            environment,
            agent_session_policy,
            history_incarnation,
        }
    }

    /// Returns the process's durable database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Borrows the process's exact environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the checked agent-session admission rule.
    #[must_use]
    pub const fn agent_session_policy(&self) -> AgentSessionAdmissionPolicy {
        self.agent_session_policy
    }

    /// Returns the durable history incarnation fixed at process activation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
}

/// Cloneable least-authority handles to the sole-writer coordinator actor.
#[derive(Clone)]
pub struct ServiceExecutors {
    pub(crate) audit: AdministrationAuditExecutor,
    pub(crate) control_plane: ControlPlaneExecutor,
    pub(crate) command: CommandExecutor,
    pub(crate) idempotency: CommandIdempotencyInspector,
}

impl ServiceExecutors {
    /// Groups the four already bounded WP-100 executor capabilities.
    #[must_use]
    pub const fn new(
        audit: AdministrationAuditExecutor,
        control_plane: ControlPlaneExecutor,
        command: CommandExecutor,
        idempotency: CommandIdempotencyInspector,
    ) -> Self {
        Self {
            audit,
            control_plane,
            command,
            idempotency,
        }
    }
}

impl fmt::Debug for ServiceExecutors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServiceExecutors([CAPABILITIES])")
    }
}

/// Consumer-owned semantic and process capabilities required by the service.
#[derive(Clone)]
pub struct ServiceProviders {
    pub(crate) catalog: Arc<dyn CatalogReadPort>,
    pub(crate) policy: Arc<dyn CurrentPolicyPort>,
    pub(crate) authoritative: Arc<dyn AuthoritativeReadPort>,
    pub(crate) projection: Arc<dyn ProjectionQueryPort>,
    pub(crate) outbox: Option<Arc<dyn OutboxStatusPort>>,
    pub(crate) maintenance: Option<Arc<dyn OfflineMaintenanceCoordinatorPort>>,
    pub(crate) migration: Option<Arc<dyn ContractMigrationCoordinatorPort>>,
    pub(crate) installation: Option<Arc<dyn ApplicationInstallationCoordinatorPort>>,
    pub(crate) application_export: Option<Arc<dyn ApplicationExportCoordinatorPort>>,
    pub(crate) application_reimport: Option<Arc<dyn ApplicationReimportCoordinatorPort>>,
    pub(crate) operational: Arc<dyn OperationalStatusPort>,
    pub(crate) token_issuer: Arc<dyn CapabilityTokenIssuer>,
    pub(crate) incident_ids: Arc<dyn IncidentIdSource>,
    pub(crate) diagnostics: Arc<dyn ServiceDiagnostics>,
    pub(crate) telemetry: Arc<dyn ServiceTelemetry>,
    pub(crate) health: Arc<dyn ServiceHealthHooks>,
    pub(crate) spawner: Arc<dyn ServiceJobSpawner>,
    pub(crate) deadline_scheduler: Arc<dyn RequestDeadlineScheduler>,
    pub(crate) cursor_tokens: Arc<dyn CursorTokenGenerator>,
    pub(crate) cursor_clock: Arc<dyn CursorMonotonicClock>,
    pub(crate) query_executor: Option<Arc<dyn QueryExecutionPort>>,
    pub(crate) query_modules: Option<Arc<dyn QueryModuleReadPort>>,
    pub(crate) reactive_modules: Option<Arc<dyn ReactiveModuleReadPort>>,
    pub(crate) event_consumers: Option<Arc<dyn EventConsumerPort>>,
    pub(crate) consumer_clock: Option<Arc<dyn EventConsumerClock>>,
    pub(crate) live_query_clock: Option<Arc<dyn LiveQueryClock>>,
    pub(crate) event_lease_tokens: Option<Arc<dyn EventLeaseTokenSource>>,
    pub(crate) contextual_causation: Option<ContextualCausationTokenCodec>,
    pub(crate) columnar: Option<Arc<dyn ColumnarProjectionPort>>,
    pub(crate) exact_text: Option<Arc<dyn crate::ExactTextProjectionPort>>,
    pub(crate) vector_projection: Option<Arc<dyn crate::VectorProjectionPort>>,
}

impl ServiceProviders {
    /// Groups disjoint consumer capabilities without exposing their implementation.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        catalog: Arc<dyn CatalogReadPort>,
        policy: Arc<dyn CurrentPolicyPort>,
        authoritative: Arc<dyn AuthoritativeReadPort>,
        projection: Arc<dyn ProjectionQueryPort>,
        outbox: Option<Arc<dyn OutboxStatusPort>>,
        operational: Arc<dyn OperationalStatusPort>,
        token_issuer: Arc<dyn CapabilityTokenIssuer>,
        incident_ids: Arc<dyn IncidentIdSource>,
        diagnostics: Arc<dyn ServiceDiagnostics>,
        telemetry: Arc<dyn ServiceTelemetry>,
        health: Arc<dyn ServiceHealthHooks>,
        spawner: Arc<dyn ServiceJobSpawner>,
        deadline_scheduler: Arc<dyn RequestDeadlineScheduler>,
        cursor_tokens: Arc<dyn CursorTokenGenerator>,
        cursor_clock: Arc<dyn CursorMonotonicClock>,
    ) -> Self {
        Self {
            catalog,
            policy,
            authoritative,
            projection,
            outbox,
            maintenance: None,
            migration: None,
            installation: None,
            application_export: None,
            application_reimport: None,
            operational,
            token_issuer,
            incident_ids,
            diagnostics,
            telemetry,
            health,
            spawner,
            deadline_scheduler,
            cursor_tokens,
            cursor_clock,
            query_executor: None,
            query_modules: None,
            reactive_modules: None,
            event_consumers: None,
            consumer_clock: None,
            live_query_clock: None,
            event_lease_tokens: None,
            contextual_causation: None,
            columnar: None,
            exact_text: None,
            vector_projection: None,
        }
    }

    /// Installs the closed one-snapshot symbolic query executor.
    #[must_use]
    pub fn with_query_executor(mut self, query_executor: Arc<dyn QueryExecutionPort>) -> Self {
        self.query_executor = Some(query_executor);
        self
    }

    /// Installs immutable query-module reads for named execution.
    #[must_use]
    pub fn with_query_modules(mut self, query_modules: Arc<dyn QueryModuleReadPort>) -> Self {
        self.query_modules = Some(query_modules);
        self
    }

    /// Installs immutable reactive-module reads for event consumers.
    #[must_use]
    pub fn with_reactive_modules(
        mut self,
        reactive_modules: Arc<dyn ReactiveModuleReadPort>,
    ) -> Self {
        self.reactive_modules = Some(reactive_modules);
        self
    }

    /// Installs the complete least-authority durable-consumer boundary.
    #[must_use]
    pub fn with_event_consumers(
        mut self,
        port: Arc<dyn EventConsumerPort>,
        clock: Arc<dyn EventConsumerClock>,
        tokens: Arc<dyn EventLeaseTokenSource>,
    ) -> Self {
        self.event_consumers = Some(port);
        self.consumer_clock = Some(clock);
        self.event_lease_tokens = Some(tokens);
        self
    }

    /// Installs server-owned sealing and verification for contextual causation.
    #[must_use]
    pub fn with_contextual_causation(mut self, codec: ContextualCausationTokenCodec) -> Self {
        self.contextual_causation = Some(codec);
        self
    }

    /// Installs the wall clock used to bound live-query cursor validity.
    #[must_use]
    pub fn with_live_query_clock(mut self, clock: Arc<dyn LiveQueryClock>) -> Self {
        self.live_query_clock = Some(clock);
        self
    }

    /// Installs published columnar projection observation for projected queries.
    #[must_use]
    pub fn with_columnar(mut self, columnar: Arc<dyn ColumnarProjectionPort>) -> Self {
        self.columnar = Some(columnar);
        self
    }

    /// Installs the derived exact-count and ordinal-window provider.
    #[must_use]
    pub fn with_exact_text(mut self, exact_text: Arc<dyn crate::ExactTextProjectionPort>) -> Self {
        self.exact_text = Some(exact_text);
        self
    }

    /// Installs the compiler-owned projected-vector provider.
    #[must_use]
    pub fn with_vector_projection(
        mut self,
        vector_projection: Arc<dyn crate::VectorProjectionPort>,
    ) -> Self {
        self.vector_projection = Some(vector_projection);
        self
    }

    /// Installs the server-private offline-maintenance lifecycle controller.
    #[must_use]
    pub fn with_offline_maintenance(
        mut self,
        maintenance: Arc<dyn OfflineMaintenanceCoordinatorPort>,
    ) -> Self {
        self.maintenance = Some(maintenance);
        self
    }

    /// Installs the server-private contract-migration lifecycle controller.
    #[must_use]
    pub fn with_contract_migration(
        mut self,
        migration: Arc<dyn ContractMigrationCoordinatorPort>,
    ) -> Self {
        self.migration = Some(migration);
        self
    }

    /// Installs the server-private exact installation campaign coordinator.
    #[must_use]
    pub fn with_application_installation(
        mut self,
        installation: Arc<dyn ApplicationInstallationCoordinatorPort>,
    ) -> Self {
        self.installation = Some(installation);
        self
    }

    /// Installs the server-private symbolic export lifecycle coordinator.
    #[must_use]
    pub fn with_application_export(
        mut self,
        application_export: Arc<dyn ApplicationExportCoordinatorPort>,
    ) -> Self {
        self.application_export = Some(application_export);
        self
    }

    /// Installs the server-private compiler-owned reimport coordinator.
    #[must_use]
    pub fn with_application_reimport(
        mut self,
        application_reimport: Arc<dyn ApplicationReimportCoordinatorPort>,
    ) -> Self {
        self.application_reimport = Some(application_reimport);
        self
    }
}

impl fmt::Debug for ServiceProviders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServiceProviders([CAPABILITIES])")
    }
}

pub(crate) struct RiffDbServiceInner {
    pub(crate) identity: ServiceIdentity,
    pub(crate) process: ServiceProcessMetadata,
    pub(crate) executors: ServiceExecutors,
    pub(crate) providers: ServiceProviders,
    pub(crate) cursors: ServiceCursorRegistries,
    pub(crate) pre_bootstrap_health: Arc<PreBootstrapHealthAdmission>,
    pub(crate) audit_failures: crate::orchestration::AuditFailureTracker,
    active_commit_subscribers: Arc<AtomicU16>,
}

/// The one concrete implementation shared by gRPC, MCP, CLI, and SDK adapters.
#[derive(Clone)]
pub struct RiffDbService {
    pub(crate) inner: Arc<RiffDbServiceInner>,
}

/// Health-only application surface available while startup validation runs.
///
/// This type deliberately carries no executor, storage, catalog, policy, or
/// authenticated application-service capability.
pub struct InitializingRiffDbService {
    admission: Arc<PreBootstrapHealthAdmission>,
}

impl InitializingRiffDbService {
    /// Serves the restricted pre-bootstrap Health result.
    pub fn health(
        &self,
        context: PreBootstrapHealthContext,
        _request: HealthRequest,
    ) -> ServiceFuture<'_, HealthResult> {
        pre_bootstrap_health_result(&self.admission, context)
    }
}

impl fmt::Debug for InitializingRiffDbService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InitializingRiffDbService([CAPABILITY])")
    }
}

/// Move-only authority to activate the complete service after startup proofs join.
pub struct RiffDbServiceActivator {
    admission: Arc<PreBootstrapHealthAdmission>,
}

impl RiffDbServiceActivator {
    /// Installs already validated dependencies without reopening health admission.
    #[must_use]
    pub fn activate(
        self,
        identity: ServiceIdentity,
        process: ServiceProcessMetadata,
        executors: ServiceExecutors,
        providers: ServiceProviders,
    ) -> RiffDbService {
        RiffDbService::with_pre_bootstrap_health(
            identity,
            process,
            executors,
            providers,
            self.admission,
        )
    }
}

impl fmt::Debug for RiffDbServiceActivator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RiffDbServiceActivator([CAPABILITY])")
    }
}

impl RiffDbService {
    /// Begins startup with only the restricted Health surface and one activation authority.
    #[must_use]
    pub fn begin_initialization() -> (
        InitializingRiffDbService,
        RiffDbServiceActivator,
        PreBootstrapHealthContextIssuer,
    ) {
        let admission = Arc::new(PreBootstrapHealthAdmission::open());
        (
            InitializingRiffDbService {
                admission: Arc::clone(&admission),
            },
            RiffDbServiceActivator {
                admission: Arc::clone(&admission),
            },
            PreBootstrapHealthContextIssuer::new(admission),
        )
    }

    /// Composes the service and the sole authority for restricted pre-bootstrap health.
    #[must_use]
    pub fn compose(
        identity: ServiceIdentity,
        process: ServiceProcessMetadata,
        executors: ServiceExecutors,
        providers: ServiceProviders,
    ) -> (Self, PreBootstrapHealthContextIssuer) {
        let (_initializing, activator, issuer) = Self::begin_initialization();
        (
            activator.activate(identity, process, executors, providers),
            issuer,
        )
    }

    /// Constructs one API-neutral service over already activated dependencies.
    #[must_use]
    pub fn new(
        identity: ServiceIdentity,
        process: ServiceProcessMetadata,
        executors: ServiceExecutors,
        providers: ServiceProviders,
    ) -> Self {
        Self::with_pre_bootstrap_health(
            identity,
            process,
            executors,
            providers,
            Arc::new(PreBootstrapHealthAdmission::closed()),
        )
    }

    fn with_pre_bootstrap_health(
        identity: ServiceIdentity,
        process: ServiceProcessMetadata,
        executors: ServiceExecutors,
        providers: ServiceProviders,
        pre_bootstrap_health: Arc<PreBootstrapHealthAdmission>,
    ) -> Self {
        let cursors = ServiceCursorRegistries::new(
            Arc::clone(&providers.cursor_tokens),
            Arc::clone(&providers.cursor_clock),
        );
        Self {
            inner: Arc::new(RiffDbServiceInner {
                identity,
                process,
                executors,
                providers,
                cursors,
                pre_bootstrap_health,
                audit_failures: crate::orchestration::AuditFailureTracker::new(),
                active_commit_subscribers: Arc::new(AtomicU16::new(0)),
            }),
        }
    }

    pub(crate) fn spawn_operation<T, F>(
        &self,
        operation: ServiceOperationV1,
        ingress: riffdb_types::ServiceIngressKindV1,
        future: F,
    ) -> ServiceFuture<'static, T>
    where
        T: ServiceResponseCharge + Send + 'static,
        F: Future<Output = ServiceResult<T>> + Send + 'static,
    {
        let (sender, receipt) = port_completion_channel();
        let job_inner = Arc::clone(&self.inner);
        let lifecycle = Arc::new(OperationAuditLifecycle::new(operation));
        let spawn_submitted_at = Instant::now();
        let job = Box::pin(async move {
            let result = observe_operation(
                job_inner.as_ref(),
                operation,
                ingress,
                future,
                lifecycle,
                Some(spawn_submitted_at),
            )
            .await;
            sender.complete(result);
        });

        spawn_trusted_service_job(self.inner.providers.spawner.as_ref(), job);

        Box::pin(trusted_service_job_completion(receipt))
    }

    pub(crate) fn spawn_maintenance_operation<T, F>(&self, future: F) -> ServiceFuture<'static, T>
    where
        T: ServiceResponseCharge + Send + 'static,
        F: Future<Output = ServiceResult<T>> + Send + 'static,
    {
        self.spawn_tracked_maintenance_operation(
            Arc::new(MaintenanceSubmissionState::new()),
            future,
        )
    }

    pub(crate) fn spawn_tracked_maintenance_operation<T, F>(
        &self,
        submission: Arc<MaintenanceSubmissionState>,
        future: F,
    ) -> ServiceFuture<'static, T>
    where
        T: ServiceResponseCharge + Send + 'static,
        F: Future<Output = ServiceResult<T>> + Send + 'static,
    {
        let (sender, receipt) = port_completion_channel();
        let job_inner = Arc::clone(&self.inner);
        let job = Box::pin(async move {
            let result = match catch_maintenance_future_panic(future).await {
                Ok(result) => result,
                Err(()) if submission.may_have_been_submitted() => {
                    Err(PublicError::outcome_unknown().into())
                }
                Err(()) => {
                    Err(job_inner.maintenance_internal_failure(MaintenanceInternalDefect::Panic))
                }
            };
            sender.complete(result);
        });

        spawn_trusted_service_job(self.inner.providers.spawner.as_ref(), job);
        Box::pin(trusted_service_job_completion(receipt))
    }
}

pub(crate) async fn observe_inline_operation<T, F>(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    ingress: riffdb_types::ServiceIngressKindV1,
    future: F,
) -> ServiceResult<T>
where
    F: Future<Output = ServiceResult<T>>,
{
    observe_operation(
        service,
        operation,
        ingress,
        future,
        Arc::new(OperationAuditLifecycle::new(operation)),
        None,
    )
    .await
}

async fn observe_operation<T, F>(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    ingress: riffdb_types::ServiceIngressKindV1,
    future: F,
    lifecycle: Arc<OperationAuditLifecycle>,
    spawn_submitted_at: Option<Instant>,
) -> ServiceResult<T>
where
    F: Future<Output = ServiceResult<T>>,
{
    // SpawnDispatch belongs only to the public query submission boundary.
    if operation == ServiceOperationV1::ExecuteQuery
        && let Some(submitted_at) = spawn_submitted_at
    {
        service.providers.telemetry.record(
            crate::ServiceTelemetryEvent::ReadPipelineStageCompleted {
                stage: crate::ReadPipelineStage::SpawnDispatch,
                elapsed: submitted_at.elapsed(),
            },
        );
    }
    let started_at = Instant::now();
    let observed = catch_future_panic(future, &lifecycle).await;
    let result = match observed {
        Ok(result) if !lifecycle.normal_completion_requires_containment(result.is_ok()) => result,
        // Pre-admission rejections with no durable Started may settle without an
        // append under saturation. A post-Started rejection must be contained.
        Ok(Err(failure)) if !lifecycle.has_durable_started() => {
            lifecycle.force_terminal_settled_for_pre_admission();
            Err(failure)
        }
        Ok(_) => {
            let failure = service.internal_failure(operation, InternalDefect::UnterminatedAudit);
            match catch_future_panic(
                service.settle_contained_failure_audit(&lifecycle),
                &lifecycle,
            )
            .await
            {
                Ok(Ok(())) => Err(failure),
                Ok(Err(failure)) => Err(contained_audit_failure(failure)),
                Err(()) => {
                    service.note_audit_failure(operation);
                    Err(PublicError::storage_unavailable().into())
                }
            }
        }
        Err(()) => {
            let failure = service.internal_failure(operation, InternalDefect::Panic);
            match catch_future_panic(
                service.settle_contained_failure_audit(&lifecycle),
                &lifecycle,
            )
            .await
            {
                Ok(Ok(())) => Err(failure),
                Ok(Err(failure)) => Err(contained_audit_failure(failure)),
                Err(()) => {
                    service.note_audit_failure(operation);
                    Err(PublicError::storage_unavailable().into())
                }
            }
        }
    };
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::OperationTerminal {
            operation,
            ingress,
            terminal: service_terminal_class(&result),
            elapsed: started_at.elapsed(),
        });
    result
}

pub(crate) struct MaintenanceSubmissionState(AtomicBool);

impl MaintenanceSubmissionState {
    pub(crate) const fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    pub(crate) fn mark_submit_in_flight(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub(crate) fn mark_submit_rejected(&self) {
        self.0.store(false, Ordering::Release);
    }

    pub(crate) fn may_have_been_submitted(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

const fn service_terminal_class<T>(result: &ServiceResult<T>) -> crate::ServiceTerminalClass {
    match result {
        Ok(_) => crate::ServiceTerminalClass::Succeeded,
        Err(ServiceFailure::Public(error)) => match error.kind() {
            PublicErrorKind::Validation => crate::ServiceTerminalClass::Validation,
            PublicErrorKind::IdempotencyKeyReuse => {
                crate::ServiceTerminalClass::IdempotencyMismatch
            }
            PublicErrorKind::AuthorizationDenied => {
                crate::ServiceTerminalClass::AuthorizationDenied
            }
            PublicErrorKind::ConcurrencyDeadlineExceeded => {
                crate::ServiceTerminalClass::ConcurrencyDeadlineExceeded
            }
            PublicErrorKind::ContractMismatch => crate::ServiceTerminalClass::ContractMismatch,
            PublicErrorKind::StorageUnavailable => crate::ServiceTerminalClass::StorageUnavailable,
            PublicErrorKind::OutcomeUnknown => crate::ServiceTerminalClass::OutcomeUnknown,
            PublicErrorKind::InternalDefect => crate::ServiceTerminalClass::InternalDefect,
            PublicErrorKind::CommandExecutionFailed => {
                crate::ServiceTerminalClass::CommandExecutionFailed
            }
            PublicErrorKind::HistoryIncarnationMismatch => {
                crate::ServiceTerminalClass::HistoryIncarnationMismatch
            }
            PublicErrorKind::HistoryPruned => crate::ServiceTerminalClass::HistoryPruned,
            PublicErrorKind::Overloaded => crate::ServiceTerminalClass::Overloaded,
        },
        Err(ServiceFailure::Cancelled) => crate::ServiceTerminalClass::Cancelled,
        Err(ServiceFailure::DeadlineExceeded) => crate::ServiceTerminalClass::DeadlineExceeded,
        Err(ServiceFailure::ResponseTooLarge) => crate::ServiceTerminalClass::ResponseTooLarge,
        Err(ServiceFailure::EmergencyInternal(_)) => crate::ServiceTerminalClass::EmergencyInternal,
    }
}

pub(crate) fn pre_bootstrap_health_result(
    admission: &Arc<PreBootstrapHealthAdmission>,
    context: PreBootstrapHealthContext,
) -> ServiceFuture<'_, HealthResult> {
    if !context.is_admitted_by(admission) {
        return Box::pin(async { Err(PublicError::authorization_denied().into()) });
    }
    let result =
        HealthResult::PreBootstrap(PreBootstrapHealthReport::new(context.lifecycle(), true));
    Box::pin(async move {
        ensure_response_budget(&result)?;
        Ok(result)
    })
}

fn spawn_trusted_service_job(spawner: &dyn ServiceJobSpawner, job: ServiceJob) {
    spawner.spawn(job);
}

// Losing an accepted job is an uncertain-execution composition breach, not a
// request failure: lower irreversible work may already have been submitted.
async fn trusted_service_job_completion<T>(
    receipt: PortReceipt<T, ServiceFailure>,
) -> ServiceResult<T> {
    match receipt.completion().await {
        Ok(result) => result,
        Err(PortDriverStopped) => {
            panic!("accepted service job stopped without publishing its result")
        }
    }
}

impl RiffDbServiceInner {
    pub(crate) fn reserve_commit_subscriber(
        &self,
    ) -> Result<CommitSubscriberLease, CommitSubscriberLimitReached> {
        let count = Arc::clone(&self.active_commit_subscribers);
        count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < crate::MAX_LIVE_COMMIT_SUBSCRIBERS).then_some(current + 1)
            })
            .map_err(|_| CommitSubscriberLimitReached)?;
        Ok(CommitSubscriberLease { count })
    }

    pub(crate) fn active_commit_subscribers(&self) -> u16 {
        self.active_commit_subscribers.load(Ordering::Acquire)
    }

    pub(crate) fn internal_failure(
        &self,
        operation: ServiceOperationV1,
        defect: InternalDefect,
    ) -> ServiceFailure {
        contained_internal_failure(
            self.providers.telemetry.as_ref(),
            self.providers.incident_ids.as_ref(),
            self.providers.diagnostics.as_ref(),
            self.providers.health.as_ref(),
            operation,
            defect,
        )
    }

    pub(crate) fn maintenance_internal_failure(
        &self,
        defect: MaintenanceInternalDefect,
    ) -> ServiceFailure {
        if matches!(defect, MaintenanceInternalDefect::LowerIntegrity) {
            self.providers
                .health
                .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
        }
        match self.providers.incident_ids.next_incident_id() {
            Ok(incident_id) => {
                self.providers
                    .diagnostics
                    .record_internal(InternalError::new(incident_id, defect));
                PublicError::internal_defect(incident_id).into()
            }
            Err(error) => {
                self.providers
                    .health
                    .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
                riffdb_errors::EmergencyInternalFailure::from(error).into()
            }
        }
    }
}

pub(crate) fn contained_internal_failure(
    telemetry: &dyn ServiceTelemetry,
    incident_ids: &dyn IncidentIdSource,
    diagnostics: &dyn ServiceDiagnostics,
    health: &dyn ServiceHealthHooks,
    operation: ServiceOperationV1,
    defect: InternalDefect,
) -> ServiceFailure {
    telemetry.record(ServiceTelemetryEvent::InternalIntegrity { operation });
    match incident_ids.next_incident_id() {
        Ok(incident_id) => {
            diagnostics.record_internal(InternalError::new(incident_id, defect));
            PublicError::internal_defect(incident_id).into()
        }
        Err(error) => {
            health.fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
            riffdb_errors::EmergencyInternalFailure::from(error).into()
        }
    }
}

/// A fully shaped response withheld until its exact terminal audit is durable.
pub(crate) struct PendingTerminalResponse<T, Terminal> {
    terminal: Terminal,
    response: ServiceResult<T>,
}

impl<T, Terminal: Copy> PendingTerminalResponse<T, Terminal> {
    pub(crate) fn new(
        result: T,
        terminal: Terminal,
        budget: impl FnOnce(&T) -> Result<(), ServiceFailure>,
    ) -> Self {
        let response = budget(&result).map(|()| result);
        Self { terminal, response }
    }

    pub(crate) const fn terminal(&self) -> Terminal {
        self.terminal
    }

    pub(crate) fn into_response(self) -> ServiceResult<T> {
        self.response
    }
}

/// One live service-owned commit-subscription slot.
pub(crate) struct CommitSubscriberLease {
    count: Arc<AtomicU16>,
}

impl Drop for CommitSubscriberLease {
    fn drop(&mut self) {
        let previous = self.count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

impl fmt::Debug for CommitSubscriberLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommitSubscriberLease([CAPACITY])")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommitSubscriberLimitReached;

impl fmt::Debug for RiffDbService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RiffDbService([CAPABILITIES])")
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum InternalDefect {
    Panic,
    UnterminatedAudit,
    ProofMismatch,
    LowerIntegrity,
}

impl fmt::Display for InternalDefect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Panic => "contained service panic",
            Self::UnterminatedAudit => "service operation omitted its terminal audit",
            Self::ProofMismatch => "checked service proofs did not join",
            Self::LowerIntegrity => "lower semantic state failed integrity",
        })
    }
}

impl Error for InternalDefect {}

#[derive(Clone, Copy, Debug)]
pub(crate) enum MaintenanceInternalDefect {
    Panic,
    ProofMismatch,
    LowerIntegrity,
}

impl fmt::Display for MaintenanceInternalDefect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Panic => "contained offline-maintenance service panic",
            Self::ProofMismatch => "offline-maintenance service proof mismatch",
            Self::LowerIntegrity => "offline-maintenance lower state failed integrity",
        })
    }
}

impl Error for MaintenanceInternalDefect {}

fn contained_audit_failure(failure: ContainedAuditFailure) -> ServiceFailure {
    match failure {
        ContainedAuditFailure::StorageUnavailable => PublicError::storage_unavailable().into(),
        ContainedAuditFailure::OutcomeUnknown => PublicError::outcome_unknown().into(),
    }
}

async fn catch_future_panic<F>(
    future: F,
    lifecycle: &Arc<OperationAuditLifecycle>,
) -> Result<F::Output, ()>
where
    F: Future,
{
    let mut future = Box::pin(future);
    poll_fn(move |context| {
        match catch_unwind(AssertUnwindSafe(|| {
            with_operation_audit_lifecycle(lifecycle, || Pin::as_mut(&mut future).poll(context))
        })) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    })
    .await
}

pub(crate) async fn catch_maintenance_future_panic<F>(future: F) -> Result<F::Output, ()>
where
    F: Future,
{
    let mut future = Box::pin(future);
    poll_fn(move |context| {
        match catch_unwind(AssertUnwindSafe(|| Pin::as_mut(&mut future).poll(context))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    })
    .await
}

/// Catches a containable panic while polling a post-invocation continuation.
///
/// Stream items run after the audited establishment invocation has completed,
/// so this boundary deliberately carries no operation-audit lifecycle.
pub(crate) async fn catch_continuation_panic<F>(future: F) -> Result<F::Output, ()>
where
    F: Future,
{
    let mut future = Box::pin(future);
    poll_fn(move |context| {
        match catch_unwind(AssertUnwindSafe(|| Pin::as_mut(&mut future).poll(context))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Waker};

    use crate::orchestration::current_operation_audit_lifecycle;
    use riffdb_errors::PublicErrorKind;
    use riffdb_types::{
        AdministrationSequence, CommitSequence, ProvenanceId, ServiceAuditLinkV1,
        ServiceAuditPhaseV1,
    };

    use super::*;

    #[derive(Default)]
    struct EnqueueThenPanicSpawner {
        accepted: Mutex<Option<ServiceJob>>,
    }

    impl ServiceJobSpawner for EnqueueThenPanicSpawner {
        fn spawn(&self, job: ServiceJob) {
            *self
                .accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(job);
            panic!("injected panic after service-job acceptance");
        }
    }

    struct PollOnceThenDropSpawner;

    impl ServiceJobSpawner for PollOnceThenDropSpawner {
        fn spawn(&self, mut job: ServiceJob) {
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(job.as_mut().poll(&mut context), Poll::Pending));
        }
    }

    #[test]
    fn initialization_surface_is_health_only_and_uses_one_revocable_admission() {
        let (initializing, _activator, issuer) = RiffDbService::begin_initialization();
        let admitted = issuer
            .issue(crate::PreBootstrapLifecycle::InitializingValidation)
            .expect("initial admission is open");
        let revoked = issuer
            .issue(crate::PreBootstrapLifecycle::InitializingBootstrap)
            .expect("initial admission is open");

        let mut health = initializing.health(admitted, HealthRequest);
        let mut context = Context::from_waker(Waker::noop());
        let result = match health.as_mut().poll(&mut context) {
            Poll::Ready(Ok(result)) => result,
            observed => panic!("restricted health must complete immediately: {observed:?}"),
        };
        assert_eq!(
            result,
            HealthResult::PreBootstrap(PreBootstrapHealthReport::new(
                crate::PreBootstrapLifecycle::InitializingValidation,
                true,
            ))
        );

        issuer.close();
        assert!(
            issuer
                .issue(crate::PreBootstrapLifecycle::InitializingBootstrap)
                .is_none()
        );
        let mut denied = initializing.health(revoked, HealthRequest);
        let failure = match denied.as_mut().poll(&mut context) {
            Poll::Ready(Err(failure)) => failure,
            observed => panic!("revoked health must fail immediately: {observed:?}"),
        };
        assert_eq!(
            failure.public_error().map(PublicError::kind),
            Some(PublicErrorKind::AuthorizationDenied)
        );
    }

    #[test]
    fn enqueue_then_panic_remains_a_fail_fast_spawner_contract_breach() {
        let spawner = EnqueueThenPanicSpawner::default();
        let panicked = catch_unwind(AssertUnwindSafe(|| {
            spawn_trusted_service_job(&spawner, Box::pin(async {}));
        }));

        assert!(panicked.is_err());
        assert!(
            spawner
                .accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some(),
            "the job was accepted before the trusted spawner breached its contract"
        );
    }

    #[test]
    fn accepted_job_drop_after_lower_submission_panics_instead_of_returning_failure() {
        let lower_submitted = Arc::new(AtomicBool::new(false));
        let job_lower_submitted = Arc::clone(&lower_submitted);
        let (sender, receipt) = port_completion_channel::<(), ServiceFailure>();
        let job: ServiceJob = Box::pin(async move {
            job_lower_submitted.store(true, Ordering::Release);
            pending::<()>().await;
            sender.complete(Ok(()));
        });

        spawn_trusted_service_job(&PollOnceThenDropSpawner, job);
        assert!(lower_submitted.load(Ordering::Acquire));

        let mut completion = Box::pin(trusted_service_job_completion(receipt));
        let mut context = Context::from_waker(Waker::noop());
        let panicked = catch_unwind(AssertUnwindSafe(|| completion.as_mut().poll(&mut context)));

        assert!(panicked.is_err());
    }

    #[test]
    fn panic_boundary_retains_the_started_audit_lifecycle() {
        let operation = ServiceOperationV1::GetEntity;
        let lifecycle = Arc::new(OperationAuditLifecycle::new(operation));
        let expected = Arc::clone(&lifecycle);
        let operation_future = async move {
            let current = current_operation_audit_lifecycle(operation);
            assert!(Arc::ptr_eq(&current, &expected));
            current.mark_started_for_test();
            panic!("contained test panic");
        };
        let mut caught = Box::pin(catch_future_panic(operation_future, &lifecycle));
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(
            caught.as_mut().poll(&mut context),
            Poll::Ready(Err(()))
        ));
        assert!(lifecycle.normal_completion_requires_containment(false));
    }

    #[test]
    fn durable_started_lifecycle_does_not_allow_silent_pre_admission_settle() {
        let lifecycle = Arc::new(OperationAuditLifecycle::new(
            ServiceOperationV1::ExecuteCommand,
        ));
        assert!(!lifecycle.has_durable_started());
        // Synthetic post-Started Overloaded must not take the no-append settle path.
        lifecycle.mark_started_for_test();
        lifecycle.mark_durable_start();
        assert!(lifecycle.has_durable_started());
        assert!(
            lifecycle.normal_completion_requires_containment(false),
            "durable Started still requires containment / terminal audit"
        );
        // Force-settle would orphan the Started pair — spawn_operation must not
        // call it when has_durable_started() is true.
        assert!(lifecycle.has_durable_started());
    }

    #[test]
    fn pre_admission_lifecycle_may_settle_without_durable_start() {
        let lifecycle = Arc::new(OperationAuditLifecycle::new(
            ServiceOperationV1::ExecuteCommand,
        ));
        assert!(!lifecycle.has_durable_started());
        lifecycle.force_terminal_settled_for_pre_admission();
        assert!(!lifecycle.normal_completion_requires_containment(false));
    }

    #[test]
    fn oversized_known_results_retain_their_normal_terminal_and_exact_link() {
        let mut provenance_bytes = [0x41; 16];
        provenance_bytes[6] = 0x71;
        provenance_bytes[8] = 0x81;
        let links = [
            ServiceAuditLinkV1::Command {
                commit_sequence: CommitSequence::first(),
                provenance_id: ProvenanceId::from_bytes(provenance_bytes)
                    .expect("valid UUIDv7 provenance"),
            },
            ServiceAuditLinkV1::ControlPlane {
                administration_sequence: AdministrationSequence::first(),
            },
        ];

        for link in links {
            let pending =
                PendingTerminalResponse::new((), (ServiceAuditPhaseV1::Succeeded, link), |_| {
                    Err(ServiceFailure::ResponseTooLarge)
                });

            assert_eq!(pending.terminal(), (ServiceAuditPhaseV1::Succeeded, link));
            assert!(matches!(
                pending.into_response(),
                Err(ServiceFailure::ResponseTooLarge)
            ));
        }
    }
}
