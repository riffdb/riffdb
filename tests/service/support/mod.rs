#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use riffdb_auth::{
    AuthenticatedPrincipal, CapabilityDigestKeyProvider, NewlyIssuedCapabilityToken,
    RawCapabilityToken,
};
use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogError, CatalogErrorKind, CatalogHistoryOutcome,
    CatalogPreparationResult, ResolvedExecutablePlan, ValidatedContractBundle,
    ValidatedQueryModule, ValidatedReactiveModule, prepare_catalog_activation,
    resolve_executable_plan, validate_catalog_history,
};
use riffdb_columnar::{
    ColumnarEngine, ColumnarOutcome, ColumnarProjectionDefinition,
    OpenOptions as ColumnarOpenOptions, RegisteredDefinition, SortDirection,
};
use riffdb_commit::{
    AdministrationClock, AdministrationClockError, AdmissionClock, AdmissionClockError,
    ApplicationCommitNotificationError, ApplicationCommitNotificationSink,
    CommandExecutionCapacityPermit, CoordinatorDurability, CoordinatorWorkloadCapacity,
    ProvenanceIdSource, ProvenanceIdSourceError, RunningCommandCoordinator, ServiceUuidV7Source,
    ServiceUuidV7SourceError,
};
use riffdb_conflict::{ConflictManager, ConflictManagerConfig, ShardedConflictManager};
use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_contract_ir::{CommandPlan, ContractBundle, RecordSchema};
use riffdb_errors::{IncidentIdSource, IncidentIdSourceError, InternalError};
use riffdb_idempotency::{
    IdempotencyDigestCandidatesV1, IdempotencyDigestError, IdempotencyDigestProvider,
};
use riffdb_invariant::derive_input_command_facts;
use riffdb_policy::{
    AgentSessionAdmissionPolicy, AuthorizationClock, AuthorizationClockError, AuthorizationError,
    CapabilityViewCheckpoint, CurrentAuthorizer, Decision, NoopAuthorizationTelemetry,
    NormalizedCapabilityCreateRecord, OperationRequest, PartitionConstraint, ProvenanceSelector,
    TrustedAudienceCatalog, UntrustedInvocationClaims,
};
use riffdb_service::{
    AbsentCapabilityRevokeTargetSnapshot, AffectedEntityView, AuthoritativeCommitNotification,
    AuthoritativeCommitPage, AuthoritativeCommitScanRequest, AuthoritativeCommitSnapshot,
    AuthoritativeCommitSubscriptionRequest, AuthoritativeEntityRequest,
    AuthoritativeEntitySnapshot, AuthoritativeIndexPage, AuthoritativeIndexRequest,
    AuthoritativeIndexRow, AuthoritativeJournaledOutcome, AuthoritativeOutcomeFacts,
    AuthoritativeOutcomeRequest, AuthoritativeOutcomeSnapshot, AuthoritativeProvenanceSnapshot,
    AuthoritativeReadError, AuthoritativeReadPort, AuthoritativeReadinessFailure,
    AuthoritativeSchemaBinding, BootstrapCapabilityRequest, BootstrapRequestContext, BuildInfo,
    CapabilityRevokeTargetSnapshot, CapabilityTokenIssueError, CapabilityTokenIssuer,
    CatalogExecutablePlanRequest, CatalogReadPort, CommandDurability, CommitNotificationSource,
    ComponentHealth, ContractSelection, ContractSource, CursorClockError, CursorMonotonicClock,
    CursorTick, CursorTokenGenerationError, CursorTokenGenerator, DeclaredOutcomeView,
    DeployContractRequest, DurableEventView, ExecuteCommandRequest, FieldSelection,
    GetContractVersionRequest, GetEntityRequest, GetProjectionStatusRequest, HealthComponentKind,
    HealthComponentStatus, HealthContext, JournaledCommandResult,
    ListPendingOutboxDeliveriesRequest, LiveQueryClock, NormalCreateCapabilityRequest,
    OperationalHealthSnapshot, OperationalStatisticsSnapshot, OperationalStatusError,
    OperationalStatusPort, OutboxStatusPort, OutboxStatusPortError, OutboxStatusRequest,
    OutboxStatusSnapshot, PageLimit, PageRequest, PortAdmissionError, PortCapacityPermit,
    PortCompletionSender, PortFuture, PortReceipt, PreBootstrapHealthContextIssuer,
    PreBootstrapLifecycle, ProjectionPageFence, ProjectionPortError, ProjectionPortReady,
    ProjectionPortRequest, ProjectionPortResult, ProjectionQueryPort, ProjectionStatusSnapshot,
    ProvenanceClaimsView, QueryModuleReadError, QueryModuleReadPort, QueryProjectionRequest,
    ReactiveModuleReadError, ReactiveModuleReadPort, RequestCancellationHandle, RequestContext,
    RequestControl, RequestDeadlineFuture, RequestDeadlineScheduler, ResolveCommandOutcomeRequest,
    RevokeCapabilityRequest, RiffDbService, ScanCommitsRequest, ScanIndexRequest,
    ServiceDiagnostics, ServiceExecutors, ServiceHealthHooks, ServiceIdentity, ServiceJob,
    ServiceJobSpawner, ServiceProcessMetadata, ServiceProviders, ServiceTelemetry,
    ServiceTelemetryEvent, SourceName, SubmittedRecord, TraceProvenanceRequest,
    ValidateContractRequest, port_completion_channel,
};
use riffdb_service::{
    ColumnarLifecycle, ColumnarNotifier, ColumnarObservation, ColumnarPortError,
    ColumnarProjectionPort, ExecuteProjectedQueryRequest, ExecuteSymbolicQueryRequest,
    ProjectedColumnPredicate, ProjectedOrderSpec, ProjectedQueryBody, SubmittedEnum,
    SubmittedValue, SymbolicContractSelector, SymbolicQueryParameters, SymbolicQuerySource,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AdministrationAuditReader, AdministrationAuditScan,
    AdministrationAuditScanRequest, AuditPrincipalV1, CatalogActivationIntentV1,
    CatalogActivationResult, CatalogAdministrationRepository, DatabaseInitializationPort,
    DatabaseInitializationResult, EvidencePageLimit, ExecutablePlanRef, IdempotencyKeyDigest,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StorageScanLimit, StoredAdministrationAuditRecordV1,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession, StructuralOpenOutcome,
};
use riffdb_storage_api::{AuthoritativeScanReader, CommitScanRequest};
use riffdb_storage_redb::{RedbDormantPorts, RedbOperationalPorts, RedbSharedPorts, RedbStore};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, Audience, CanonicalInputHash,
    CanonicalRecord, CanonicalString, CanonicalValue, CapabilityGrantV1, CapabilityId,
    CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1, CommitSequence,
    ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, Decimal, DecimalSpec,
    DigestKeyId, EntityKey, EntityKeyBuilder, EntityVersion, Environment, EventId, FieldId,
    FreshnessPolicy, FrontierPosition, IdempotencyKey, IncidentId, IndexEntryKey,
    IndexEntryKeyBuilder, IndexEpoch, IndexEpochPosition, LogicalTime, MAX_STRING_BYTES, OutcomeId,
    PartitionKey, PartitionKeyBuilder, PartitionKeyHash, PartitionScopeV1, ProjectionFrontier,
    ProjectionGeneration, ProjectionId, ProjectionIdentity, ProvenanceId, RequestId,
    RevocationReasonCodeV1, ScopedPartitionV1, ServiceAuditPhaseV1, ServiceAuditTargetV1,
    ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, TenantId, TenantScope,
    Timestamp,
};
use tokio::sync::Notify;

pub(crate) const BASE_SECONDS: i64 = 1_700_200_000;
const BUDGET_SOURCE: &str = include_str!("../../../contracts/examples/budget.riff");
const COMMAND_NAME: &str = "CreateBudget";
const COMMAND_CALLER_KEY: &str = "service-harness-command-key";
const ORGANIZATION_ID: [u8; 16] = [0x31; 16];
const ALTERNATE_ORGANIZATION_ID: [u8; 16] = [0x32; 16];
const FISCAL_YEAR: i64 = 2026;
static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadCommitMode {
    ImmediateNotFound,
    Pending,
}

const CATALOG_READY: u8 = 0;
const CATALOG_STORAGE_ERROR: u8 = 1;
const CATALOG_PANIC: u8 = 2;
const CATALOG_PENDING: u8 = 3;
const CATALOG_ABSENT: u8 = 4;
const CATALOG_INTEGRITY_ERROR: u8 = 5;
const CATALOG_COMPATIBLE_ACTIVATION_RACE: u8 = 6;

const BOOTSTRAP_TOKEN: &[u8] = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const CAPABILITY_DIGEST_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";

const COMMIT_CONTINUATION_PANIC_NONE: u8 = 0;
const COMMIT_CONTINUATION_PANIC_SOURCE_NEXT: u8 = 1;
const COMMIT_CONTINUATION_PANIC_SOURCE_POLL: u8 = 2;
const COMMIT_CONTINUATION_PANIC_READ_SUBMIT: u8 = 3;
const COMMIT_CONTINUATION_PANIC_SOURCE_DROP: u8 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitContinuationPanic {
    SourceNext,
    SourcePoll,
    ReadSubmit,
    SourceDrop,
}

impl CommitContinuationPanic {
    const fn code(self) -> u8 {
        match self {
            Self::SourceNext => COMMIT_CONTINUATION_PANIC_SOURCE_NEXT,
            Self::SourcePoll => COMMIT_CONTINUATION_PANIC_SOURCE_POLL,
            Self::ReadSubmit => COMMIT_CONTINUATION_PANIC_READ_SUBMIT,
            Self::SourceDrop => COMMIT_CONTINUATION_PANIC_SOURCE_DROP,
        }
    }
}

pub(crate) struct ServiceHarness {
    pub(crate) service: RiffDbService,
    pub(crate) policy: Arc<HarnessPolicy>,
    pub(crate) ports: Arc<HarnessPorts>,
    pub(crate) telemetry: Arc<HarnessTelemetry>,
    pub(crate) health: Arc<HarnessHealth>,
    pub(crate) columnar: Option<Arc<HarnessColumnar>>,
    deadline_scheduler: Arc<HarnessDeadlineScheduler>,
    cursor_tokens: Arc<SequentialCursorTokens>,
    pre_bootstrap: PreBootstrapHealthContextIssuer,
    coordinator: Option<RunningCommandCoordinator>,
    database: AuditDatabase,
}

impl ServiceHarness {
    /// The real source coordinator remains a canary, but this service receives
    /// none of its writer capabilities.
    pub(crate) fn follower_service(&self) -> RiffDbService {
        let providers = ServiceProviders::new(
            Arc::clone(&self.ports) as Arc<dyn CatalogReadPort>,
            Arc::clone(&self.policy) as Arc<dyn riffdb_service::CurrentPolicyPort>,
            Arc::clone(&self.ports) as Arc<dyn AuthoritativeReadPort>,
            Arc::clone(&self.ports) as Arc<dyn ProjectionQueryPort>,
            Some(Arc::clone(&self.ports) as Arc<dyn OutboxStatusPort>),
            Arc::clone(&self.ports) as Arc<dyn OperationalStatusPort>,
            Arc::clone(&self.ports) as Arc<dyn CapabilityTokenIssuer>,
            Arc::new(HarnessIncidentIds::new(false)),
            Arc::new(HarnessDiagnostics),
            Arc::clone(&self.telemetry) as Arc<dyn ServiceTelemetry>,
            Arc::clone(&self.health) as Arc<dyn ServiceHealthHooks>,
            Arc::new(TokioSpawner),
            Arc::clone(&self.deadline_scheduler) as Arc<dyn RequestDeadlineScheduler>,
            Arc::clone(&self.cursor_tokens) as Arc<dyn CursorTokenGenerator>,
            Arc::new(FixedCursorClock),
        );
        RiffDbService::new(
            ServiceIdentity::new(
                database_id(),
                environment(),
                AgentSessionAdmissionPolicy::Discard,
                1,
            ),
            ServiceProcessMetadata::new(
                timestamp(BASE_SECONDS),
                BuildInfo::new(
                    "0.1.0-test",
                    "follower-harness",
                    "rustc-1.97.0",
                    Vec::new(),
                    1,
                    1,
                    "2025-03-26",
                )
                .unwrap(),
            ),
            ServiceExecutors::follower(),
            providers,
        )
    }

    pub(crate) fn new(read_mode: ReadCommitMode, allow_read_commit: bool) -> Self {
        Self::compose(read_mode, allow_read_commit, false, false, false, false)
    }

    pub(crate) fn command() -> Self {
        Self::compose(
            ReadCommitMode::ImmediateNotFound,
            false,
            true,
            false,
            false,
            false,
        )
    }

    /// Command harness with a single coordinator workload slot for saturation tests.
    pub(crate) fn command_capacity_one() -> Self {
        Self::command_capacity_n(1)
    }

    /// Command harness with `n` coordinator workload slots for saturation tests.
    ///
    /// Uses a direct empty idempotency lane so inspect does not share the
    /// writer queue (production uses a direct MVCC reader for the same reason).
    pub(crate) fn command_capacity_n(workload_capacity: u16) -> Self {
        Self::compose_with_additional_commands_and_capacity(
            ReadCommitMode::ImmediateNotFound,
            false,
            true,
            false,
            false,
            false,
            0,
            false,
            workload_capacity.max(1),
            true,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
        )
    }

    /// Capacity harness that also activates one read-only ObserveBudget command.
    pub(crate) fn command_capacity_n_with_observe(workload_capacity: u16) -> Self {
        Self::compose_with_additional_commands_and_capacity(
            ReadCommitMode::ImmediateNotFound,
            false,
            true,
            false,
            false,
            false,
            1,
            false,
            workload_capacity.max(1),
            true,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
        )
    }

    /// Operations harness with Observability telemetry plus named-query ports.
    pub(crate) fn operations_with_read_stage_telemetry(
        telemetry: Arc<dyn ServiceTelemetry>,
        query_executor: Arc<dyn riffdb_query_executor::QueryExecutionPort>,
        query_modules: Arc<dyn QueryModuleReadPort>,
        named_permissions: Vec<CapabilityPermissionV1>,
    ) -> Self {
        Self::compose_with_additional_commands_and_capacity(
            ReadCommitMode::ImmediateNotFound,
            true,
            false,
            false,
            true,
            false,
            0,
            false,
            8,
            false,
            Some(telemetry),
            Some(query_executor),
            Some(query_modules),
            None,
            None,
            named_permissions,
        )
    }

    /// Operations harness with the complete exact live named-query provider set.
    pub(crate) fn live_named_queries(
        query_executor: Arc<dyn riffdb_query_executor::QueryExecutionPort>,
        query_modules: Arc<dyn QueryModuleReadPort>,
        reactive_modules: Arc<dyn ReactiveModuleReadPort>,
        live_query_clock: Arc<dyn LiveQueryClock>,
        permissions: Vec<CapabilityPermissionV1>,
    ) -> Self {
        Self::compose_with_additional_commands_and_capacity(
            ReadCommitMode::ImmediateNotFound,
            true,
            false,
            false,
            true,
            false,
            0,
            false,
            8,
            false,
            None,
            Some(query_executor),
            Some(query_modules),
            Some(reactive_modules),
            Some(live_query_clock),
            permissions,
        )
    }

    pub(crate) fn command_with_failing_incident_source() -> Self {
        Self::compose(
            ReadCommitMode::ImmediateNotFound,
            false,
            true,
            true,
            false,
            false,
        )
    }

    pub(crate) fn operations() -> Self {
        Self::compose(
            ReadCommitMode::ImmediateNotFound,
            true,
            false,
            false,
            true,
            false,
        )
    }

    pub(crate) fn reimport_discovery() -> Self {
        Self::compose_with_additional_commands_and_capacity(
            ReadCommitMode::ImmediateNotFound,
            true,
            false,
            false,
            true,
            false,
            0,
            true,
            8,
            false,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
        )
    }

    pub(crate) fn discovery_inventory(additional_commands: usize) -> Self {
        Self::compose_with_additional_commands(
            ReadCommitMode::ImmediateNotFound,
            true,
            false,
            false,
            true,
            false,
            additional_commands,
        )
    }

    pub(crate) fn restricted_operations() -> Self {
        Self::compose(
            ReadCommitMode::ImmediateNotFound,
            true,
            true,
            false,
            true,
            false,
        )
    }

    pub(crate) fn pre_bootstrap() -> Self {
        Self::compose(
            ReadCommitMode::ImmediateNotFound,
            false,
            false,
            false,
            false,
            true,
        )
    }

    fn compose(
        read_mode: ReadCommitMode,
        allow_read_commit: bool,
        restrict_command_partition: bool,
        fail_incident_source: bool,
        broad_operations: bool,
        pre_bootstrap: bool,
    ) -> Self {
        Self::compose_with_additional_commands(
            read_mode,
            allow_read_commit,
            restrict_command_partition,
            fail_incident_source,
            broad_operations,
            pre_bootstrap,
            0,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn compose_with_additional_commands(
        read_mode: ReadCommitMode,
        allow_read_commit: bool,
        restrict_command_partition: bool,
        fail_incident_source: bool,
        broad_operations: bool,
        pre_bootstrap: bool,
        additional_commands: usize,
    ) -> Self {
        Self::compose_with_additional_commands_and_capacity(
            read_mode,
            allow_read_commit,
            restrict_command_partition,
            fail_incident_source,
            broad_operations,
            pre_bootstrap,
            additional_commands,
            false,
            8,
            false,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn compose_with_additional_commands_and_capacity(
        read_mode: ReadCommitMode,
        allow_read_commit: bool,
        restrict_command_partition: bool,
        fail_incident_source: bool,
        broad_operations: bool,
        pre_bootstrap: bool,
        additional_commands: usize,
        include_reimport_command: bool,
        workload_capacity: u16,
        direct_empty_idempotency: bool,
        telemetry_override: Option<Arc<dyn ServiceTelemetry>>,
        query_executor: Option<Arc<dyn riffdb_query_executor::QueryExecutionPort>>,
        query_modules: Option<Arc<dyn QueryModuleReadPort>>,
        reactive_modules: Option<Arc<dyn ReactiveModuleReadPort>>,
        live_query_clock: Option<Arc<dyn LiveQueryClock>>,
        extra_permissions: Vec<CapabilityPermissionV1>,
    ) -> Self {
        let database = if pre_bootstrap {
            AuditDatabase::create_pre_bootstrap(broad_operations)
        } else if include_reimport_command {
            AuditDatabase::create_with_reimport(broad_operations)
        } else {
            AuditDatabase::create_with_additional_commands(broad_operations, additional_commands)
        };
        let command_reference = database.executable_plan.reference();
        let capability_order = Arc::new(Mutex::new(Vec::new()));
        let discovery_order = Arc::new(Mutex::new(Vec::new()));
        let partition_scope = if restrict_command_partition {
            PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(
                command_reference.contract_lineage().clone(),
                database.partition.clone(),
            )])
            .expect("one explicit command partition")
        } else {
            PartitionScopeV1::All
        };
        let policy = Arc::new(HarnessPolicy::new(
            allow_read_commit,
            command_reference,
            database
                .active_catalog
                .bundle()
                .bundle()
                .projections()
                .first()
                .expect("budget projection")
                .projection_id(),
            partition_scope,
            broad_operations,
            database.active_catalog.bundle().bundle(),
            database.alternate_partition.clone(),
            additional_commands != 0,
            Arc::clone(&capability_order),
            Arc::clone(&discovery_order),
            extra_permissions,
        ));
        let ports = Arc::new(HarnessPorts::new(
            read_mode,
            database.active_catalog.clone(),
            database.executable_plan.clone(),
            capability_order,
            discovery_order,
        ));
        let coordinator = start_coordinator_with_capacity(database.open(), workload_capacity);
        let mut inspector =
            coordinator.command_idempotency_inspector(Arc::new(FixedDigestProvider));
        if direct_empty_idempotency {
            // Saturation tests hold the writer queue; inspect must not share it.
            inspector = inspector.with_direct_repository(Arc::new(EmptyAdmissionLookup));
        }
        let executors = ServiceExecutors::new(
            coordinator.administration_audit_executor(),
            coordinator.control_plane_executor(),
            coordinator.command_executor(),
            inspector,
        );
        let telemetry = Arc::new(HarnessTelemetry::default());
        let service_telemetry = telemetry_override
            .unwrap_or_else(|| Arc::clone(&telemetry) as Arc<dyn ServiceTelemetry>);
        let health = Arc::new(HarnessHealth::default());
        let deadline_scheduler = Arc::new(HarnessDeadlineScheduler::default());
        let cursor_tokens = Arc::new(SequentialCursorTokens::default());
        let mut providers = ServiceProviders::new(
            Arc::clone(&ports) as Arc<dyn CatalogReadPort>,
            Arc::clone(&policy) as Arc<dyn riffdb_service::CurrentPolicyPort>,
            Arc::clone(&ports) as Arc<dyn AuthoritativeReadPort>,
            Arc::clone(&ports) as Arc<dyn ProjectionQueryPort>,
            broad_operations.then(|| Arc::clone(&ports) as Arc<dyn OutboxStatusPort>),
            Arc::clone(&ports) as Arc<dyn OperationalStatusPort>,
            Arc::clone(&ports) as Arc<dyn CapabilityTokenIssuer>,
            Arc::new(HarnessIncidentIds::new(fail_incident_source)),
            Arc::new(HarnessDiagnostics),
            service_telemetry,
            Arc::clone(&health) as Arc<dyn ServiceHealthHooks>,
            Arc::new(TokioSpawner),
            Arc::clone(&deadline_scheduler) as Arc<dyn RequestDeadlineScheduler>,
            Arc::clone(&cursor_tokens) as Arc<dyn CursorTokenGenerator>,
            Arc::new(FixedCursorClock),
        );
        if let (Some(executor), Some(modules)) = (query_executor, query_modules) {
            providers = providers
                .with_query_executor(executor)
                .with_query_modules(modules);
        }
        if let Some(modules) = reactive_modules {
            providers = providers.with_reactive_modules(modules);
        }
        if let Some(clock) = live_query_clock {
            providers = providers.with_live_query_clock(clock);
        }
        let identity = ServiceIdentity::new(
            database_id(),
            environment(),
            AgentSessionAdmissionPolicy::Discard,
            1,
        );
        let process = ServiceProcessMetadata::new(
            timestamp(BASE_SECONDS),
            BuildInfo::new(
                "0.1.0-test",
                "service-harness",
                "rustc-1.97.0",
                Vec::new(),
                1,
                1,
                "2025-03-26",
            )
            .expect("bounded build metadata"),
        );
        let (service, pre_bootstrap) =
            RiffDbService::compose(identity, process, executors, providers);
        Self {
            service,
            policy,
            ports,
            telemetry,
            health,
            columnar: None,
            deadline_scheduler,
            cursor_tokens,
            pre_bootstrap,
            coordinator: Some(coordinator),
            database,
        }
    }

    pub(crate) fn context(&self, request_seed: u8) -> (RequestContext, RequestCancellationHandle) {
        self.context_with_deadline(request_seed, Instant::now() + Duration::from_secs(30))
    }

    pub(crate) fn set_application_head(&self, head: Option<CommitSequence>) {
        self.ports.set_application_head(head);
    }

    pub(crate) fn application_head_observations(&self) -> usize {
        self.ports.application_head_observations()
    }

    pub(crate) fn context_with_deadline(
        &self,
        request_seed: u8,
        deadline: Instant,
    ) -> (RequestContext, RequestCancellationHandle) {
        let (control, cancellation) = RequestControl::new(deadline);
        (
            RequestContext::new(
                request_id(request_seed),
                self.policy.principal(),
                ServiceIngressKindV1::Grpc,
                UntrustedInvocationClaims::new(None, None, None, None, None),
                control,
                None,
            ),
            cancellation,
        )
    }

    pub(crate) fn pre_bootstrap_health(&self, lifecycle: PreBootstrapLifecycle) -> HealthContext {
        HealthContext::pre_bootstrap(
            self.pre_bootstrap
                .issue(lifecycle)
                .expect("pre-bootstrap Health admission remains open"),
        )
    }

    pub(crate) fn close_pre_bootstrap_health(&self) {
        self.pre_bootstrap.close();
    }

    /// Holds one command coordinator workload slot for capacity-saturation tests.
    ///
    /// Dropping the returned permit releases the slot. Tests that need a
    /// constructed full channel MUST keep the permit alive across the probe and
    /// assert [`Self::try_command_capacity_is_full`] before submitting work.
    pub(crate) fn hold_command_capacity(&self) -> CommandExecutionCapacityPermit {
        self.coordinator
            .as_ref()
            .expect("coordinator is running")
            .command_executor()
            .try_reserve_capacity()
            .expect("hold one free command workload slot")
    }

    /// Holds `count` command workload slots (for capacity-N saturation tests).
    ///
    /// Same constructed-saturation contract as [`Self::hold_command_capacity`]:
    /// keep the permits until probes complete.
    pub(crate) fn hold_command_capacity_n(
        &self,
        count: usize,
    ) -> Vec<CommandExecutionCapacityPermit> {
        let executor = self
            .coordinator
            .as_ref()
            .expect("coordinator is running")
            .command_executor();
        let mut held = Vec::with_capacity(count);
        for _ in 0..count {
            held.push(
                executor
                    .try_reserve_capacity()
                    .expect("hold free command workload slot"),
            );
        }
        held
    }

    /// Returns whether the next non-blocking command reservation is overload.
    ///
    /// Used to prove constructed saturation before a probe. Must not be used as
    /// a wait loop against wall-clock progress.
    pub(crate) fn try_command_capacity_is_full(&self) -> bool {
        matches!(
            self.coordinator
                .as_ref()
                .expect("coordinator is running")
                .command_executor()
                .try_reserve_capacity(),
            Err(riffdb_commit::CommandExecutionAdmissionError::Overloaded)
        )
    }

    /// Accepted administration-audit coordinator messages since start.
    pub(crate) fn audit_submission_count(&self) -> u64 {
        self.coordinator
            .as_ref()
            .expect("coordinator is running")
            .administration_audit_executor()
            .accepted_submission_count()
    }

    /// Force the command writer's EWMA queue-delay estimate (pre-admission shed).
    pub(crate) fn force_queue_delay_estimate_micros(&self, micros: u64) {
        self.coordinator
            .as_ref()
            .expect("coordinator is running")
            .command_executor()
            .force_queue_delay_estimate_micros_for_tests(micros);
    }

    /// Counts deadline-scheduler registrations inside the bounded admission
    /// window — the wait a pre-admission shed must never reach.
    ///
    /// A shed that rejects before admission leaves this at zero; disabling the
    /// shed falls through to the bounded wait and registers one.
    pub(crate) fn admission_wait_registrations(&self) -> usize {
        self.deadline_scheduler.admission_wait_registrations()
    }

    /// Read the current queue-delay estimate.
    pub(crate) fn queue_delay_estimate_micros(&self) -> u64 {
        self.coordinator
            .as_ref()
            .expect("coordinator is running")
            .command_executor()
            .estimated_queue_delay_micros()
    }

    /// Exhausts the independent retained-byte budget (for RetainedBytes stage tests).
    pub(crate) fn hold_all_retained_bytes(&self) -> tokio::sync::OwnedSemaphorePermit {
        // Acquire the full 32 MiB / 1 KiB unit budget used by the coordinator.
        const TOTAL_UNITS: u32 = (32 * 1_024 * 1_024) / 1_024;
        self.coordinator
            .as_ref()
            .expect("coordinator is running")
            .command_executor()
            .try_acquire_retained_bytes(TOTAL_UNITS)
            .expect("full retained-byte budget at start")
    }

    pub(crate) fn deploy_request(&self) -> DeployContractRequest {
        DeployContractRequest::new(
            ContractSource::new(BUDGET_SOURCE).expect("bounded example contract"),
            Some(self.database.active_catalog.pointer().contract_version()),
        )
        .expect("bounded deployment request")
    }

    pub(crate) fn mismatched_exact_deploy_request(&self) -> DeployContractRequest {
        let active = self.database.active_catalog.bundle().bundle();
        DeployContractRequest::new_exact(
            ContractSource::new(self.database.source.clone()).expect("bounded active contract"),
            Some(active.contract_version()),
            Some(active.bundle_hash()),
            ContractBundleHash::from_bytes([0xa5; 32]),
        )
        .expect("bounded exact deployment request")
    }

    pub(crate) fn exact_active_deploy_request(&self) -> DeployContractRequest {
        let active = self.database.active_catalog.bundle().bundle();
        DeployContractRequest::new_exact(
            ContractSource::new(self.database.source.clone()).expect("bounded active contract"),
            None,
            None,
            active.bundle_hash(),
        )
        .expect("bounded exact active request")
    }

    pub(crate) fn validate_request(&self) -> ValidateContractRequest {
        ValidateContractRequest::new(
            ContractSource::new(BUDGET_SOURCE).expect("bounded example contract"),
        )
        .expect("bounded validation request")
    }

    pub(crate) fn preview_candidate_request(&self) -> ValidateContractRequest {
        ValidateContractRequest::preview_active_successor(
            ContractSource::new(self.database.source.clone()).expect("bounded example contract"),
        )
        .expect("bounded preview request")
    }

    pub(crate) fn explain_request(&self) -> riffdb_service::ExplainCommandRequest {
        riffdb_service::ExplainCommandRequest::new(
            ContractSelection::Active,
            SourceName::new(COMMAND_NAME).expect("checked command name"),
        )
    }

    pub(crate) fn contract_version_request(&self) -> GetContractVersionRequest {
        let bundle = self.database.active_catalog.bundle().bundle();
        GetContractVersionRequest::new(bundle.lineage().clone(), bundle.contract_version())
    }

    pub(crate) fn entity_request(&self) -> GetEntityRequest {
        let entity = self
            .database
            .active_catalog
            .bundle()
            .bundle()
            .schema()
            .entities()
            .first()
            .expect("budget entity");
        let mut key = EntityKeyBuilder::new(entity.id());
        key.push_uuid(&ORGANIZATION_ID)
            .expect("organization key component");
        key.push_i64(FISCAL_YEAR).expect("year key component");
        GetEntityRequest::new(
            ContractSelection::Active,
            entity.id(),
            key.finish().expect("budget entity key"),
            FieldSelection::new(Vec::new()).expect("empty field selection"),
        )
        .expect("checked entity request")
    }

    pub(crate) fn index_request(&self) -> ScanIndexRequest {
        self.index_request_with_cursor(None)
    }

    pub(crate) fn index_request_with_cursor(
        &self,
        cursor: Option<riffdb_service::CursorToken>,
    ) -> ScanIndexRequest {
        let index = self
            .database
            .active_catalog
            .bundle()
            .bundle()
            .schema()
            .entities()
            .first()
            .and_then(|entity| entity.indexes().first())
            .expect("operations harness index");
        ScanIndexRequest::new(
            ContractSelection::Active,
            index.id(),
            Vec::new(),
            FieldSelection::new(Vec::new()).expect("empty field selection"),
            PageRequest::new(PageLimit::new(10).expect("page limit"), cursor),
        )
        .expect("checked index request")
    }

    pub(crate) fn primary_index_row(&self) -> AuthoritativeIndexRow {
        self.index_row(ORGANIZATION_ID, self.database.partition.clone())
    }

    pub(crate) fn alternate_index_row(&self) -> AuthoritativeIndexRow {
        self.index_row(
            ALTERNATE_ORGANIZATION_ID,
            self.database.alternate_partition.clone(),
        )
    }

    pub(crate) fn primary_index_row_with_wrong_partition(&self) -> AuthoritativeIndexRow {
        self.index_row(ORGANIZATION_ID, self.database.alternate_partition.clone())
    }

    fn index_row(
        &self,
        organization_id: [u8; 16],
        stored_partition: PartitionKey,
    ) -> AuthoritativeIndexRow {
        let bundle = self.database.active_catalog.bundle().bundle();
        let entity = bundle.schema().entities().first().expect("budget entity");
        let index = entity.indexes().first().expect("operations harness index");
        let mut entity_key = EntityKeyBuilder::new(entity.id());
        entity_key
            .push_uuid(&organization_id)
            .expect("organization key component");
        entity_key
            .push_i64(FISCAL_YEAR)
            .expect("year key component");
        let mut index_key = IndexEntryKeyBuilder::new(index.id());
        index_key
            .push_i64(FISCAL_YEAR)
            .expect("fiscal-year index component");
        AuthoritativeIndexRow::new(
            index_key
                .finish(entity_key.finish().expect("budget entity key"))
                .expect("budget index key"),
            AuthoritativeSchemaBinding::new(
                bundle.lineage().clone(),
                bundle.contract_version(),
                bundle.bundle_hash(),
            ),
            CanonicalRecord::new(Vec::new()).expect("empty covered values"),
            stored_partition,
        )
    }

    pub(crate) fn configure_index_pages(
        &self,
        pages: Vec<(Vec<AuthoritativeIndexRow>, Option<IndexEntryKey>)>,
    ) {
        self.ports.configure_index_pages(pages);
    }

    pub(crate) fn narrow_policy_after_next_index_submission(&self) {
        let policy = Arc::clone(&self.policy);
        self.ports
            .set_index_submission_hook(Arc::new(move || policy.use_narrowed_once_now()));
    }

    pub(crate) fn use_compatible_successor_as_active(&self) {
        self.ports
            .replace_active_catalog(self.database.compatible_successor_snapshot());
    }

    pub(crate) fn projection_status_request(&self) -> GetProjectionStatusRequest {
        GetProjectionStatusRequest::new(
            ContractSelection::Active,
            self.projection_identity().projection_id(),
        )
    }

    pub(crate) fn scan_commits_request(&self) -> ScanCommitsRequest {
        ScanCommitsRequest::new(PageRequest::new(
            PageLimit::new(10).expect("page limit"),
            None,
        ))
    }

    pub(crate) fn trace_request(&self) -> TraceProvenanceRequest {
        TraceProvenanceRequest::new(riffdb_service::ProvenanceSelection::Commit(sequence(1)))
    }

    pub(crate) fn outbox_request(&self) -> ListPendingOutboxDeliveriesRequest {
        ListPendingOutboxDeliveriesRequest::new(PageRequest::new(
            PageLimit::new(10).expect("page limit"),
            None,
        ))
    }

    pub(crate) fn create_capability_request(&self) -> NormalCreateCapabilityRequest {
        self.create_capability_request_with_scope(PartitionScopeV1::All)
    }

    pub(crate) fn valid_explicit_capability_request(&self) -> NormalCreateCapabilityRequest {
        self.create_capability_request_with_scope(
            PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(
                self.database.active_catalog.bundle().lineage().clone(),
                self.database.partition.clone(),
            )])
            .expect("one valid explicit capability partition"),
        )
    }

    pub(crate) fn malformed_explicit_capability_request(&self) -> NormalCreateCapabilityRequest {
        let aggregate = self.database.partition.aggregate_type_id();
        let envelope_only = PartitionKeyBuilder::new(aggregate)
            .finish()
            .expect("known aggregate envelope without its required component");
        self.explicit_capability_request(
            self.database.active_catalog.bundle().lineage().clone(),
            envelope_only,
        )
    }

    pub(crate) fn mixed_valid_invalid_capability_request(&self) -> NormalCreateCapabilityRequest {
        let lineage = self.database.active_catalog.bundle().lineage().clone();
        let envelope_only = PartitionKeyBuilder::new(self.database.partition.aggregate_type_id())
            .finish()
            .expect("known aggregate envelope without its required component");
        self.create_capability_request_with_scope(
            PartitionScopeV1::explicit(vec![
                ScopedPartitionV1::new(lineage.clone(), self.database.partition.clone()),
                ScopedPartitionV1::new(lineage, envelope_only),
            ])
            .expect("canonical mixed explicit capability partitions"),
        )
    }

    pub(crate) fn absent_aggregate_capability_request(&self) -> NormalCreateCapabilityRequest {
        let aggregate = AggregateTypeId::new(u32::MAX).expect("nonzero absent aggregate ID");
        assert!(
            self.database
                .active_catalog
                .bundle()
                .bundle()
                .schema()
                .aggregate(aggregate)
                .is_none(),
            "absent aggregate fixture must remain absent"
        );
        let key = PartitionKeyBuilder::new(aggregate)
            .finish()
            .expect("bounded absent-aggregate envelope");
        self.explicit_capability_request(
            self.database.active_catalog.bundle().lineage().clone(),
            key,
        )
    }

    pub(crate) fn wrong_lineage_capability_request(&self) -> NormalCreateCapabilityRequest {
        self.explicit_capability_request(
            ContractLineage::new("different-service-harness-lineage")
                .expect("bounded alternate lineage"),
            self.database.partition.clone(),
        )
    }

    fn explicit_capability_request(
        &self,
        lineage: ContractLineage,
        partition: PartitionKey,
    ) -> NormalCreateCapabilityRequest {
        self.create_capability_request_with_scope(
            PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(lineage, partition)])
                .expect("one canonical explicit capability partition"),
        )
    }

    fn create_capability_request_with_scope(
        &self,
        partition_scope: PartitionScopeV1,
    ) -> NormalCreateCapabilityRequest {
        let requested = NormalizedCapabilityCreateRecord::new(
            database_id(),
            environment(),
            ActorId::new("service-harness-child").expect("bounded child principal"),
            ActorKind::Service,
            NonZeroU32::new(60).expect("nonzero lifetime"),
            vec![audience()],
            CapabilityGrantV1::new(
                TenantScope::Global,
                partition_scope,
                CapabilityPermissionsV1::new(Vec::new()).expect("empty subset permissions"),
                Vec::new(),
                NonZeroU16::new(1).expect("nonzero row limit"),
                Vec::new(),
            )
            .expect("bounded subset grant"),
        )
        .expect("normalized child capability");
        NormalCreateCapabilityRequest::new(
            CapabilityId::from_bytes(uuid_bytes(0x63)).expect("child capability UUIDv7"),
            requested,
        )
        .expect("bounded capability-create request")
    }

    pub(crate) fn bootstrap_context(&self, request_seed: u8) -> BootstrapRequestContext {
        let provider = CapabilityDigestKeyProvider::parse_document(CAPABILITY_DIGEST_KEYS)
            .expect("checked harness capability digest keys");
        let token = RawCapabilityToken::parse_canonical(BOOTSTRAP_TOKEN)
            .expect("checked harness bootstrap token");
        let (control, _cancellation) =
            RequestControl::new(Instant::now() + Duration::from_secs(30));
        BootstrapRequestContext::from_loopback_grpc(
            request_id(request_seed),
            control,
            provider.prepare_bootstrap_token(token),
        )
    }

    pub(crate) fn explicit_bootstrap_request(&self) -> BootstrapCapabilityRequest {
        self.bootstrap_request(
            PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(
                self.database.active_catalog.bundle().lineage().clone(),
                self.database.partition.clone(),
            )])
            .expect("one valid explicit bootstrap partition envelope"),
        )
    }

    pub(crate) fn all_partitions_bootstrap_request(&self) -> BootstrapCapabilityRequest {
        self.bootstrap_request(PartitionScopeV1::All)
    }

    fn bootstrap_request(&self, partition_scope: PartitionScopeV1) -> BootstrapCapabilityRequest {
        let administer = CapabilityPermissionV1::unparameterized(
            CapabilityPermissionKindV1::AdministerCapabilities,
        )
        .expect("bootstrap administration permission");
        let requested = NormalizedCapabilityCreateRecord::new(
            database_id(),
            environment(),
            ActorId::new("service-harness-bootstrap").expect("bounded bootstrap principal"),
            ActorKind::Human,
            NonZeroU32::new(60).expect("nonzero lifetime"),
            vec![audience()],
            CapabilityGrantV1::new(
                TenantScope::Global,
                partition_scope,
                CapabilityPermissionsV1::new(vec![administer]).expect("one bootstrap permission"),
                Vec::new(),
                NonZeroU16::new(1).expect("nonzero row limit"),
                Vec::new(),
            )
            .expect("bounded bootstrap grant"),
        )
        .expect("normalized bootstrap capability");
        BootstrapCapabilityRequest::new(
            CapabilityId::from_bytes(uuid_bytes(0x64)).expect("bootstrap capability UUIDv7"),
            requested,
        )
        .expect("bounded bootstrap request")
    }

    pub(crate) fn deploy_targets(&self) -> ServiceAuditTargetsV1 {
        let pointer = self.database.active_catalog.pointer();
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::ContractVersion {
            lineage: pointer.lineage().clone(),
            version: pointer.contract_version(),
        }])
        .expect("one deploy target")
    }

    pub(crate) fn invalid_deploy_request(&self) -> DeployContractRequest {
        DeployContractRequest::new(
            ContractSource::new("contract definitely-not-valid").expect("bounded invalid contract"),
            Some(self.database.active_catalog.pointer().contract_version()),
        )
        .expect("bounded invalid deployment request")
    }

    pub(crate) fn revoke_request(&self) -> RevokeCapabilityRequest {
        RevokeCapabilityRequest::new(
            self.policy.principal().capability_id(),
            RevocationReasonCodeV1::Requested,
        )
    }

    pub(crate) fn fail_active_catalog_preparation(&self) {
        self.ports.set_prepare_active_mode(CATALOG_STORAGE_ERROR);
    }

    pub(crate) fn remove_active_catalog(&self) {
        self.ports.set_prepare_active_mode(CATALOG_ABSENT);
    }

    pub(crate) fn corrupt_active_catalog_preparation(&self) {
        self.ports.set_prepare_active_mode(CATALOG_INTEGRITY_ERROR);
    }

    pub(crate) fn block_active_catalog_preparation(&self) {
        self.ports.set_prepare_active_mode(CATALOG_PENDING);
    }

    pub(crate) async fn wait_for_active_catalog_preparation(&self) {
        self.ports.wait_for_prepare_active().await;
    }

    pub(crate) fn configure_compatible_activation_race(&self) {
        self.ports
            .configure_compatible_activation_race(self.database.compatible_successor_snapshot());
    }

    pub(crate) fn complete_compatible_activation_race(&self) {
        self.ports.complete_compatible_activation_race();
    }

    pub(crate) fn prepared_active_catalog_version(&self) -> ContractVersion {
        self.ports.active_catalog_version()
    }

    pub(crate) fn activate_compatible_successor_on_active_catalog_reservation(&self) {
        let ports = Arc::clone(&self.ports);
        let successor = self.database.compatible_successor_snapshot();
        self.ports
            .set_active_catalog_reservation_hook(Arc::new(move || {
                ports.replace_active_catalog(successor.clone());
            }));
    }

    pub(crate) fn clear_discovery_order(&self) {
        self.ports.clear_discovery_order();
    }

    pub(crate) fn discovery_order(&self) -> Vec<&'static str> {
        self.ports.discovery_order()
    }

    pub(crate) fn fail_deployment_preparation(&self) {
        self.ports
            .set_prepare_deployment_mode(CATALOG_STORAGE_ERROR);
    }

    pub(crate) fn panic_deployment_preparation(&self) {
        self.ports.set_prepare_deployment_mode(CATALOG_PANIC);
    }

    pub(crate) fn block_deployment_preparation(&self) {
        self.ports.set_prepare_deployment_mode(CATALOG_PENDING);
    }

    pub(crate) async fn wait_for_deployment_preparation(&self) {
        self.ports.wait_for_prepare_deployment().await;
    }

    pub(crate) fn deployment_preparation_calls(&self) -> usize {
        self.ports.prepare_deployment_calls()
    }

    pub(crate) fn panic_next_policy_check(&self) {
        self.policy.panic_next();
    }

    pub(crate) fn revoke_policy(&self) {
        self.policy.revoke();
    }

    pub(crate) fn audit_discovery_operations(&self) {
        self.policy.audit_discovery_operations();
    }

    pub(crate) fn audit_discovery_on_catalog_reservation(&self) {
        let policy = Arc::clone(&self.policy);
        self.ports
            .set_active_catalog_reservation_hook(Arc::new(move || {
                policy.audit_discovery_operations();
            }));
    }

    pub(crate) fn panic_executable_plan_resolution(&self) {
        self.ports.panic_executable_plan();
    }

    pub(crate) fn panic_revoke_target_read(&self) {
        self.ports.panic_revoke_target_read();
    }

    pub(crate) fn revoke_on_read_reservation(&self) {
        let policy = Arc::clone(&self.policy);
        self.ports.set_read_reservation_hook(Arc::new(move || {
            policy.revoke();
        }));
    }

    pub(crate) fn query_projection_request(
        &self,
        required_sequence: CommitSequence,
    ) -> QueryProjectionRequest {
        QueryProjectionRequest::new(
            ContractSelection::Active,
            self.projection_identity().projection_id(),
            Vec::new(),
            Some(required_sequence),
            Duration::from_secs(5),
            PageRequest::new(PageLimit::new(10).expect("bounded projection page"), None),
        )
        .expect("bounded projection query")
    }

    pub(crate) fn projection_identity(&self) -> ProjectionIdentity {
        let contract = self.database.active_catalog.bundle().bundle();
        let projection = contract.projections().first().expect("budget projection");
        contract
            .bound_projection_group_schema(projection.projection_id())
            .expect("bound budget projection")
            .identity()
            .clone()
    }

    pub(crate) fn projection_fence(
        &self,
        generation: ProjectionGeneration,
        frontier: FrontierPosition,
    ) -> ProjectionPageFence {
        ProjectionPageFence::new(self.projection_identity(), generation, frontier)
    }

    pub(crate) fn set_projection_observations(&self, observations: Vec<ProjectionObservation>) {
        self.ports.set_projection_observations(observations);
    }

    pub(crate) fn control_projection_deadline(&self) {
        self.deadline_scheduler.control_projection_deadline();
    }

    pub(crate) fn control_request_deadline(&self) {
        self.deadline_scheduler.control_request_deadline();
    }

    pub(crate) async fn wait_for_request_deadline(&self) {
        self.deadline_scheduler.wait_for_request_waiter().await;
    }

    pub(crate) fn request_deadline_is_waiting(&self) -> bool {
        self.deadline_scheduler.request_deadline_is_waiting()
    }

    pub(crate) fn elapse_request_deadline(&self) {
        self.deadline_scheduler.elapse_request_deadline();
    }

    pub(crate) fn cursor_token_calls(&self) -> u64 {
        self.cursor_tokens.calls()
    }

    pub(crate) fn deny_after_next_policy_allows(&self, allow_count: usize) {
        self.policy.deny_after_next_allows(allow_count);
    }

    /// Advances the policy clock to `seconds` after the `allow_count`-th allow.
    ///
    /// Models wall time passing between one safe point and the next without
    /// any capability-view mutation.
    pub(crate) fn advance_policy_clock_after_next_allows(&self, allow_count: usize, seconds: i64) {
        self.policy
            .advance_clock_after_next_allows(allow_count, seconds);
    }

    pub(crate) async fn wait_for_stalled_projection(&self) {
        self.ports.wait_for_stalled_projection().await;
        self.deadline_scheduler.wait_for_projection_waiters(2).await;
    }

    pub(crate) fn elapse_projection_deadline(&self) {
        self.deadline_scheduler.elapse_projection_deadline();
    }

    /// Deterministic guarantee probe: a controlled projection-deadline
    /// future that is created (and enrolled) but never yet polled must still
    /// receive an elapse fired before its first poll. Guards the tokio
    /// `notify_waiters`-reaches-created-futures guarantee the barrier fix
    /// relies on.
    pub(crate) async fn probe_projection_deadline_wakeup_before_first_poll(&self) {
        self.deadline_scheduler.control_projection_deadline();
        let future = self
            .deadline_scheduler
            .wait_until(std::time::Instant::now() + Duration::from_secs(5));
        assert_eq!(self.deadline_scheduler.projection_waiters(), 1);
        self.deadline_scheduler.elapse_projection_deadline();
        future.await;
    }

    pub(crate) fn revoke_after_next_projection_observation(&self) {
        let policy = Arc::clone(&self.policy);
        self.ports
            .set_projection_observation_hook(Arc::new(move || policy.revoke()));
    }

    pub(crate) fn use_narrowed_policy_for_next_safe_point(&self) {
        self.policy.use_narrowed_for_next_allow();
    }

    pub(crate) fn configure_commit_subscription(
        &self,
        notifications: Vec<AuthoritativeCommitNotification>,
        pending_establishment: bool,
    ) {
        self.ports
            .configure_commit_subscription(notifications, pending_establishment);
    }

    pub(crate) fn stall_next_commit_notification(&self) {
        self.ports.stall_next_commit_notification();
    }

    pub(crate) fn commit_notification_is_stalled(&self) -> bool {
        self.ports.commit_notification_is_stalled()
    }

    pub(crate) async fn wait_for_commit_subscription_submission(&self) {
        self.ports.wait_for_commit_subscription_submission().await;
    }

    pub(crate) fn release_commit_subscription_source(&self) {
        self.ports.release_commit_subscription_source();
    }

    pub(crate) fn commit_subscription_source_dropped(&self) -> bool {
        self.ports.commit_subscription_source_dropped()
    }

    pub(crate) fn commit_subscription_acknowledgements(&self) -> Vec<CommitSequence> {
        self.ports.commit_subscription_acknowledgements()
    }

    pub(crate) fn control_stream_lifetime(&self) {
        self.deadline_scheduler.control_stream_lifetime();
    }

    pub(crate) fn stream_lifetime_is_waiting(&self) -> bool {
        self.deadline_scheduler.stream_lifetime_is_waiting()
    }

    pub(crate) fn elapse_stream_lifetime(&self) {
        self.deadline_scheduler.elapse_stream_lifetime();
    }

    pub(crate) fn panic_commit_continuation_at(&self, point: CommitContinuationPanic) {
        self.ports.panic_commit_continuation_at(point);
    }

    pub(crate) fn set_read_commit_snapshot(&self, snapshot: AuthoritativeCommitSnapshot) {
        self.ports.set_read_commit_snapshot(snapshot);
    }

    pub(crate) fn set_commit_scan_snapshots(&self, snapshots: Vec<AuthoritativeCommitSnapshot>) {
        self.ports.set_commit_scan_snapshots(snapshots);
    }

    pub(crate) fn set_provenance_snapshot(&self, snapshot: AuthoritativeProvenanceSnapshot) {
        self.ports.set_provenance_snapshot(snapshot);
    }

    pub(crate) fn oversized_commit_snapshot(
        &self,
        sequence: CommitSequence,
    ) -> AuthoritativeCommitSnapshot {
        self.database.oversized_commit_snapshot(sequence)
    }

    pub(crate) fn commit_snapshot(&self, sequence: CommitSequence) -> AuthoritativeCommitSnapshot {
        self.database
            .commit_snapshot(sequence, Vec::new(), Vec::new())
    }

    pub(crate) fn commit_snapshot_with_valid_affected_key(
        &self,
        sequence: CommitSequence,
        key_ordinal: u32,
    ) -> AuthoritativeCommitSnapshot {
        self.database.commit_snapshot(
            sequence,
            vec![AffectedEntityView::new(
                self.database.budget_entity_key(key_ordinal),
                EntityVersion::first(),
            )],
            Vec::new(),
        )
    }

    pub(crate) fn commit_snapshot_with_incomplete_affected_key(
        &self,
        sequence: CommitSequence,
    ) -> AuthoritativeCommitSnapshot {
        self.database.commit_snapshot(
            sequence,
            vec![AffectedEntityView::new(
                self.database.incomplete_budget_entity_key(),
                EntityVersion::first(),
            )],
            Vec::new(),
        )
    }

    pub(crate) fn commit_snapshot_with_trailing_affected_key(
        &self,
        sequence: CommitSequence,
    ) -> AuthoritativeCommitSnapshot {
        self.database.commit_snapshot(
            sequence,
            vec![AffectedEntityView::new(
                self.database.trailing_budget_entity_key(),
                EntityVersion::first(),
            )],
            Vec::new(),
        )
    }

    pub(crate) fn provenance_snapshot_with_incomplete_affected_key(
        &self,
        sequence: CommitSequence,
    ) -> AuthoritativeProvenanceSnapshot {
        self.database
            .provenance_snapshot(sequence, self.database.incomplete_budget_entity_key())
    }

    pub(crate) fn provenance_snapshot_with_trailing_affected_key(
        &self,
        sequence: CommitSequence,
    ) -> AuthoritativeProvenanceSnapshot {
        self.database
            .provenance_snapshot(sequence, self.database.trailing_budget_entity_key())
    }

    pub(crate) fn execute_command_request(&self) -> ExecuteCommandRequest {
        self.execute_command_request_with_key(COMMAND_CALLER_KEY)
    }

    pub(crate) fn execute_command_request_with_key(
        &self,
        caller_key: &str,
    ) -> ExecuteCommandRequest {
        let caller_key = IdempotencyKey::new(caller_key).expect("checked idempotency key");
        let idempotency_field = self
            .database
            .executable_plan
            .plan()
            .idempotency_input()
            .expect("CreateBudget has a direct idempotency input");
        let input = CanonicalRecord::new(
            self.database
                .command_input
                .fields()
                .iter()
                .map(|(field, value)| {
                    let value = if *field == idempotency_field {
                        CanonicalValue::string(caller_key.expose_secret())
                            .expect("bounded idempotency value")
                    } else {
                        value.clone()
                    };
                    (*field, value)
                })
                .collect(),
        )
        .expect("canonical command input");
        ExecuteCommandRequest::new(
            SourceName::new(COMMAND_NAME).expect("checked command name"),
            Some(self.database.executable_plan.reference().contract_version()),
            SubmittedRecord::try_from(input).expect("bounded submitted command input"),
        )
        .expect("bounded command request")
    }

    /// Read-only ObserveBudget0000 request (requires `command_capacity_n_with_observe`).
    pub(crate) fn observe_budget_request(&self) -> ExecuteCommandRequest {
        let plan = self
            .database
            .active_catalog
            .bundle()
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == "ObserveBudget0000")
            .expect("ObserveBudget0000 must be activated");
        let input = build_command_input(plan, ORGANIZATION_ID);
        ExecuteCommandRequest::new(
            SourceName::new("ObserveBudget0000").expect("checked observe command name"),
            Some(self.database.executable_plan.reference().contract_version()),
            SubmittedRecord::try_from(input).expect("bounded observe input"),
        )
        .expect("bounded observe request")
    }

    pub(crate) fn resolve_command_outcome_request(&self) -> ResolveCommandOutcomeRequest {
        ResolveCommandOutcomeRequest::new(
            self.database
                .executable_plan
                .reference()
                .contract_lineage()
                .clone(),
            SourceName::new(COMMAND_NAME).expect("checked command name"),
            IdempotencyKey::new(COMMAND_CALLER_KEY).expect("checked idempotency key"),
        )
    }

    pub(crate) fn set_actual_outcome(&self, result: &JournaledCommandResult) {
        self.ports
            .set_outcome_response(Ok(Some(self.outcome_snapshot(
                result,
                self.policy.principal().principal_id().clone(),
                TenantScope::Global,
                self.database.partition.clone(),
            ))));
    }

    pub(crate) fn set_outcome_with_wrong_owner(&self, result: &JournaledCommandResult) {
        self.ports
            .set_outcome_response(Ok(Some(self.outcome_snapshot(
                result,
                ActorId::new("different-outcome-owner").expect("bounded owner"),
                TenantScope::Global,
                self.database.partition.clone(),
            ))));
    }

    pub(crate) fn set_outcome_with_wrong_tenant(&self, result: &JournaledCommandResult) {
        self.ports
            .set_outcome_response(Ok(Some(self.outcome_snapshot(
                result,
                self.policy.principal().principal_id().clone(),
                TenantScope::Tenant(TenantId::new("wrong-tenant").expect("bounded tenant")),
                self.database.partition.clone(),
            ))));
    }

    pub(crate) fn set_outcome_with_wrong_partition(&self, result: &JournaledCommandResult) {
        self.ports
            .set_outcome_response(Ok(Some(self.outcome_snapshot(
                result,
                self.policy.principal().principal_id().clone(),
                TenantScope::Global,
                self.database.alternate_partition.clone(),
            ))));
    }

    pub(crate) fn set_outcome_with_unknown_id(&self, result: &JournaledCommandResult) {
        let snapshot = self.outcome_snapshot_with_id(
            result,
            self.policy.principal().principal_id().clone(),
            TenantScope::Global,
            self.database.partition.clone(),
            OutcomeId::new(u32::MAX).expect("nonzero unknown outcome ID"),
        );
        self.ports.set_outcome_response(Ok(Some(snapshot)));
    }

    pub(crate) fn set_outcome_storage_failure(&self) {
        self.ports
            .set_outcome_response(Err(AuthoritativeReadError::Unavailable));
    }

    pub(crate) fn revoke_on_outcome_submission(&self) {
        let policy = Arc::clone(&self.policy);
        self.ports.set_outcome_submission_hook(Arc::new(move || {
            policy.revoke();
        }));
    }

    fn outcome_snapshot(
        &self,
        result: &JournaledCommandResult,
        owner_principal_id: ActorId,
        owner_tenant_scope: TenantScope,
        partition: PartitionKey,
    ) -> AuthoritativeOutcomeSnapshot {
        self.outcome_snapshot_with_id(
            result,
            owner_principal_id,
            owner_tenant_scope,
            partition,
            result.outcome().outcome_id(),
        )
    }

    fn outcome_snapshot_with_id(
        &self,
        result: &JournaledCommandResult,
        owner_principal_id: ActorId,
        owner_tenant_scope: TenantScope,
        partition: PartitionKey,
        outcome_id: OutcomeId,
    ) -> AuthoritativeOutcomeSnapshot {
        let facts = AuthoritativeOutcomeFacts::new(
            result.lineage().clone(),
            result.contract_version(),
            self.database
                .executable_plan
                .reference()
                .contract_bundle_hash(),
            result.command_id(),
            result.plan_hash(),
            owner_principal_id,
            owner_tenant_scope,
            partition,
            result.outcome_locator().digest_evidence().clone(),
        );
        let stored = AuthoritativeJournaledOutcome::new(
            result.commit_sequence(),
            outcome_id,
            result.outcome().value().clone(),
            result.provenance_id(),
            result.durability(),
        );
        AuthoritativeOutcomeSnapshot::journaled(facts, stored)
    }

    /// Active validated contract retained by the harness catalog.
    pub(crate) fn active_validated_bundle(&self) -> ValidatedContractBundle {
        self.database.active_catalog.bundle().clone()
    }

    pub(crate) fn stop_coordinator(&mut self) {
        self.coordinator
            .take()
            .expect("coordinator is running")
            .shutdown()
            .expect("coordinator shuts down cleanly");
    }

    pub(crate) fn audit_phases(&self, request_seed: u8) -> Vec<ServiceAuditPhaseV1> {
        self.database.audit_phases(request_id(request_seed))
    }

    pub(crate) fn audit_records(
        &self,
        request_seed: u8,
    ) -> Vec<riffdb_storage_api::StoredServiceAuditRecordV1> {
        self.database.audit_records(request_id(request_seed))
    }

    pub(crate) fn all_audit_phases(&self) -> Vec<ServiceAuditPhaseV1> {
        self.database.all_audit_phases()
    }
}

impl Drop for ServiceHarness {
    fn drop(&mut self) {
        if let Some(coordinator) = self.coordinator.take() {
            let _result = coordinator.shutdown();
        }
    }
}

pub(crate) fn run_async(output: impl Future<Output = ()> + Send + 'static) {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .expect("build service test runtime")
        .block_on(output);
}

pub(crate) fn sequence(value: u64) -> CommitSequence {
    CommitSequence::try_from(value).expect("nonzero commit sequence")
}

pub(crate) fn request_id(seed: u8) -> RequestId {
    RequestId::from_bytes(uuid_bytes(seed)).expect("request UUIDv7")
}

struct AuditDatabase {
    path: PathBuf,
    source: String,
    active_catalog: ActiveCatalogSnapshot,
    executable_plan: ResolvedExecutablePlan,
    command_input: CanonicalRecord,
    partition: PartitionKey,
    alternate_partition: PartitionKey,
}

impl AuditDatabase {
    fn create(with_index: bool) -> Self {
        Self::create_with_additional_commands(with_index, 0)
    }

    fn create_with_additional_commands(with_index: bool, additional_commands: usize) -> Self {
        Self::create_with_options(with_index, additional_commands, false)
    }

    fn create_with_reimport(with_index: bool) -> Self {
        Self::create_with_options(with_index, 0, true)
    }

    fn create_with_options(
        with_index: bool,
        additional_commands: usize,
        include_reimport_command: bool,
    ) -> Self {
        let path = next_database_path();
        let mut store = RedbStore::open(&path).expect("create service harness database");
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize service harness database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        let mut source = if with_index {
            BUDGET_SOURCE.replacen(
                "    invariant non_negative:",
                "    index by_fiscal_year (fiscal_year)\n\n    invariant non_negative:",
                1,
            )
        } else {
            BUDGET_SOURCE.to_owned()
        };
        if additional_commands != 0 {
            let mut declarations = String::new();
            for ordinal in 0..additional_commands {
                declarations.push_str(&format!(
                    "  command ObserveBudget{ordinal:04} {{\n    input organization_id: uuid\n    input fiscal_year: i64\n    read Budget(organization_id, fiscal_year) as budget\n      else BudgetNotFound {{}}\n    return BudgetObserved {{ budget: budget }}\n  }}\n\n"
                ));
            }
            source = source.replacen(
                "  projection BudgetUtilizationDaily {",
                &format!("{declarations}  projection BudgetUtilizationDaily {{"),
                1,
            );
        }
        if include_reimport_command {
            source = source.replacen(
                "  projection BudgetUtilizationDaily {",
                "  reimport command ReconstituteBudgets {\n    input records: list<Budget, 1..32>\n    reconstitute Budget from records else BudgetAlreadyExists {}\n    return BudgetsReconstituted {}\n  }\n\n  projection BudgetUtilizationDaily {",
                1,
            );
        }
        let checked_bundle = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(&source).expect("compile budget contract"),
        )
        .expect("validate budget contract");
        let mut ports = open_operational(store);
        let stored_bundle = checked_bundle.to_stored().expect("encode budget bundle");
        let activation = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                stored_bundle.clone(),
                request_id(0x61),
                catalog_principal(),
                timestamp(BASE_SECONDS + 1),
                None,
            ))
            .expect("activate budget catalog");
        assert!(matches!(
            activation,
            CatalogActivationResult::Activated { active, .. }
                if active == ActiveCatalogPointerV1::from_bundle(&stored_bundle)
        ));
        let active_catalog = ActiveCatalogSnapshot::read(&ports)
            .expect("read active budget catalog")
            .expect("budget catalog is active");
        let command = active_catalog
            .bundle()
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == COMMAND_NAME)
            .expect("CreateBudget command");
        let reference = ExecutablePlanRef::new(
            checked_bundle.lineage().clone(),
            checked_bundle.contract_version(),
            checked_bundle.bundle_hash(),
            command.command_id(),
            command.plan_hash(),
        );
        let executable_plan =
            resolve_executable_plan(&ports, &reference).expect("resolve CreateBudget plan");
        let command_input = build_command_input(command, ORGANIZATION_ID);
        let partition = derive_input_command_facts(command, command_input.clone())
            .expect("derive command partition")
            .partition_key()
            .clone();
        let alternate_partition = derive_input_command_facts(
            command,
            build_command_input(command, ALTERNATE_ORGANIZATION_ID),
        )
        .expect("derive alternate command partition")
        .partition_key()
        .clone();
        drop(ports);
        Self {
            path,
            source,
            active_catalog,
            executable_plan,
            command_input,
            partition,
            alternate_partition,
        }
    }

    fn create_pre_bootstrap(with_index: bool) -> Self {
        let mut database = Self::create(with_index);
        let populated_fixture_path = std::mem::replace(&mut database.path, next_database_path());
        let mut store = RedbStore::open(&database.path).expect("create pre-bootstrap database");
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize pre-bootstrap database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        drop(store);
        std::fs::remove_file(populated_fixture_path)
            .expect("remove populated pre-bootstrap fixture database");
        database
    }

    fn compatible_successor_snapshot(&self) -> ActiveCatalogSnapshot {
        let source = self.source.replacen("version 1", "version 2", 1);
        let successor = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_successor(&source, self.active_catalog.bundle().bundle())
                .expect("unchanged partition schemas form a compatible successor"),
        )
        .expect("compatible successor is catalog-valid");
        let path = next_database_path();
        let mut store = RedbStore::open(&path).expect("create compatible-race catalog database");
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize compatible-race catalog database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        let mut ports = open_operational(store);
        let genesis = self
            .active_catalog
            .bundle()
            .to_stored()
            .expect("encode race genesis");
        let activated = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                genesis,
                request_id(0x65),
                catalog_principal(),
                timestamp(BASE_SECONDS + 1),
                None,
            ))
            .expect("activate race genesis");
        assert!(matches!(
            activated,
            CatalogActivationResult::Activated { .. }
        ));
        let successor = successor.to_stored().expect("encode race successor");
        let activated = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                Some(self.active_catalog.pointer().contract_version()),
                successor,
                request_id(0x66),
                catalog_principal(),
                timestamp(BASE_SECONDS + 2),
                None,
            ))
            .expect("activate compatible race successor");
        assert!(matches!(
            activated,
            CatalogActivationResult::Activated { .. }
        ));
        let snapshot = ActiveCatalogSnapshot::read(&ports)
            .expect("read compatible race catalog")
            .expect("compatible successor is active");
        drop(ports);
        std::fs::remove_file(path).expect("remove compatible-race catalog database");
        snapshot
    }

    fn commit_snapshot(
        &self,
        sequence: CommitSequence,
        affected_entities: Vec<AffectedEntityView>,
        events: Vec<DurableEventView>,
    ) -> AuthoritativeCommitSnapshot {
        let bundle = self.active_catalog.bundle();
        let command = bundle
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == COMMAND_NAME)
            .expect("CreateBudget command");
        let outcome_schema = command
            .outcomes()
            .iter()
            .find(|outcome| outcome.name() == "BudgetAlreadyExists")
            .expect("bounded existing-budget outcome");
        let outcome = DeclaredOutcomeView::from_bundle(
            bundle,
            command.command_id(),
            outcome_schema.id(),
            input_record(
                outcome_schema.payload(),
                [
                    ("organization_id", CanonicalValue::Uuid(ORGANIZATION_ID)),
                    ("fiscal_year", CanonicalValue::I64(FISCAL_YEAR)),
                ],
            ),
        )
        .expect("checked declared outcome");
        AuthoritativeCommitSnapshot::new(
            sequence,
            request_id(0xa1),
            bundle.lineage().clone(),
            bundle.contract_version(),
            command.command_id(),
            command.plan_hash(),
            CanonicalInputHash::from_bytes([0xa2; 32]),
            AdmittedActorContext::new(
                ActorId::new("service-harness-commit-actor").expect("bounded actor"),
                ActorKind::Human,
                TenantScope::Global,
                None,
            ),
            LogicalTime::new(timestamp(BASE_SECONDS + 40)),
            PartitionKeyHash::from_bytes([0xa3; 32]),
            Vec::new(),
            affected_entities,
            events,
            outcome,
            ProvenanceId::from_bytes(uuid_bytes(0xa4)).expect("provenance UUIDv7"),
            CommandDurability::Synchronous,
        )
        .expect("bounded authoritative commit fixture")
    }

    fn budget_entity_key(&self, ordinal: u32) -> EntityKey {
        let entity = self
            .active_catalog
            .bundle()
            .bundle()
            .schema()
            .entities()
            .first()
            .expect("Budget entity");
        entity
            .primary_key()
            .encode_entity(&[
                CanonicalValue::Uuid(ORGANIZATION_ID),
                CanonicalValue::I64(FISCAL_YEAR + i64::from(ordinal)),
            ])
            .expect("complete Budget primary key")
    }

    fn incomplete_budget_entity_key(&self) -> EntityKey {
        let complete = self.budget_entity_key(0);
        let truncated = complete
            .as_bytes()
            .get(..complete.as_bytes().len() - i64::BITS as usize / 8)
            .expect("Budget key contains its i64 component")
            .to_vec();
        EntityKey::from_bytes(truncated).expect("incomplete Budget key retains a valid envelope")
    }

    fn trailing_budget_entity_key(&self) -> EntityKey {
        let mut trailing = self.budget_entity_key(0).as_bytes().to_vec();
        trailing.push(0);
        EntityKey::from_bytes(trailing).expect("trailing Budget key retains a valid envelope")
    }

    fn oversized_commit_snapshot(&self, sequence: CommitSequence) -> AuthoritativeCommitSnapshot {
        let bundle = self.active_catalog.bundle().bundle();
        let event_type_id = bundle
            .schema()
            .events()
            .first()
            .expect("BudgetAllocated event")
            .id();
        let payload_field = bundle
            .schema()
            .events()
            .first()
            .and_then(|event| event.payload().fields().first())
            .expect("BudgetAllocated payload field")
            .id();
        let large_value = CanonicalValue::String(
            CanonicalString::new("x".repeat(MAX_STRING_BYTES))
                .expect("maximum-size canonical string"),
        );
        let events = (0..5)
            .map(|ordinal| {
                DurableEventView::new(
                    EventId::new(sequence, ordinal),
                    event_type_id,
                    CanonicalRecord::new(vec![(payload_field, large_value.clone())])
                        .expect("one-field canonical event payload"),
                )
            })
            .collect();
        let affected_entities = (0..1_024)
            .map(|ordinal| {
                AffectedEntityView::new(self.budget_entity_key(ordinal), EntityVersion::first())
            })
            .collect();
        self.commit_snapshot(sequence, affected_entities, events)
    }

    fn provenance_snapshot(
        &self,
        sequence: CommitSequence,
        affected_entity_key: EntityKey,
    ) -> AuthoritativeProvenanceSnapshot {
        let bundle = self.active_catalog.bundle();
        let command = bundle
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == COMMAND_NAME)
            .expect("CreateBudget command");
        let outcome_id = command
            .outcomes()
            .iter()
            .find(|outcome| outcome.name() == "BudgetAlreadyExists")
            .expect("bounded existing-budget outcome")
            .id();
        AuthoritativeProvenanceSnapshot::new(
            ProvenanceId::from_bytes(uuid_bytes(0xa4)).expect("provenance UUIDv7"),
            sequence,
            request_id(0xa1),
            bundle.lineage().clone(),
            bundle.contract_version(),
            command.command_id(),
            command.plan_hash(),
            AdmittedActorContext::new(
                ActorId::new("service-harness-commit-actor").expect("bounded actor"),
                ActorKind::Human,
                TenantScope::Global,
                None,
            ),
            LogicalTime::new(timestamp(BASE_SECONDS + 40)),
            outcome_id,
            vec![AffectedEntityView::new(
                affected_entity_key,
                EntityVersion::first(),
            )],
            Vec::new(),
            ProvenanceClaimsView::new(None, None, None, None),
        )
        .expect("bounded authoritative provenance fixture")
    }

    fn open(&self) -> RedbOperationalPorts {
        open_operational(RedbStore::open(&self.path).expect("open service harness database"))
    }

    fn audit_phases(&self, request_id: RequestId) -> Vec<ServiceAuditPhaseV1> {
        self.audit_records(request_id)
            .into_iter()
            .map(|record| record.phase())
            .collect()
    }

    fn audit_records(
        &self,
        request_id: RequestId,
    ) -> Vec<riffdb_storage_api::StoredServiceAuditRecordV1> {
        scan_service_audits(&self.open())
            .into_iter()
            .filter(|record| record.request_id() == request_id)
            .collect()
    }

    fn all_audit_phases(&self) -> Vec<ServiceAuditPhaseV1> {
        scan_service_audits(&self.open())
            .into_iter()
            .map(|record| record.phase())
            .collect()
    }
}

fn next_database_path() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target")
        .join("service-harness");
    std::fs::create_dir_all(&root).expect("create service harness directory");
    root.join(format!(
        "riffdb-service-harness-{}-{}.redb",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ))
}

impl Drop for AuditDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(riffdb_storage_redb::durable_format_marker_path(&self.path));
        for suffix in [
            ".riffjournal",
            ".riffjournal.checkpoint",
            ".riffjournal.next",
            ".riffjournal.rewrite",
            ".riffjournal.extent-v3",
        ] {
            let mut companion = self.path.as_os_str().to_os_string();
            companion.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(companion));
        }
    }
}

fn scan_service_audits(
    ports: &RedbOperationalPorts,
) -> Vec<riffdb_storage_api::StoredServiceAuditRecordV1> {
    let scan = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).expect("audit scan limit"),
        ))
        .expect("scan service audit records");
    let AdministrationAuditScan::ExactEnd { records } = scan else {
        panic!("bounded harness audit scan must reach exact end");
    };
    records
        .into_iter()
        .filter_map(|record| match record.into_parts().0 {
            StoredAdministrationAuditRecordV1::Service(service) => Some(service),
            StoredAdministrationAuditRecordV1::Catalog(_)
            | StoredAdministrationAuditRecordV1::Capability(_)
            | StoredAdministrationAuditRecordV1::QueryModule(_)
            | StoredAdministrationAuditRecordV1::ReactiveModule(_)
            | StoredAdministrationAuditRecordV1::Retention(_) => None,
        })
        .collect()
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
    let mut session = store
        .begin_structural_evidence(startup_inputs())
        .expect("begin structural validation");
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("evidence page limit");
    let mut cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty(), "valid harness has no findings");
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (history, historical_end) = validate_catalog_history(&mut session)
        .expect("validate empty catalog history")
        .into_parts();
    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural validation");
    let history = match history {
        CatalogHistoryOutcome::Ready(history) => history,
        CatalogHistoryOutcome::MigrationRequired(_) => {
            panic!("V2-only service harness cannot require index migration")
        }
    };
    let opened = match opened {
        StructuralOpenOutcome::Clean(opened) => opened,
        StructuralOpenOutcome::MigrationRequired(_) => {
            panic!("V2-only service harness cannot expose a migration port")
        }
    };
    assert!(
        history.matches(opened.database_id(), opened.open_session_id()),
        "catalog proof belongs to the structural-open session"
    );
    let (_, _, _, dormant): (_, _, _, RedbDormantPorts) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate checked redb ports")
}

fn startup_inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
    StartupValidationInputs::new(
        timestamp(BASE_SECONDS),
        ReadableCapabilityDigestInventory::new(vec![key]).expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency digest inventory"),
    )
}

/// Read-only idempotency lane that always reports no durable admission state.
///
/// Production uses a shared storage reader; the harness only needs Absent so
/// inspect does not compete with the command writer queue under capacity tests.
struct EmptyAdmissionLookup;

impl riffdb_storage_api::AdmissionLookupRepository for EmptyAdmissionLookup {
    fn lookup_admission(
        &self,
        _candidates: riffdb_storage_api::IdempotencyLookupCandidatesV1,
    ) -> Result<riffdb_storage_api::AdmissionLookupResultV1, riffdb_storage_api::StorageError> {
        Ok(riffdb_storage_api::AdmissionLookupResultV1::NotFound)
    }

    fn lookup_admission_group(
        &self,
        candidates: Vec<riffdb_storage_api::IdempotencyLookupCandidatesV1>,
    ) -> Result<Vec<riffdb_storage_api::AdmissionLookupResultV1>, riffdb_storage_api::StorageError>
    {
        Ok(candidates
            .into_iter()
            .map(|_| riffdb_storage_api::AdmissionLookupResultV1::NotFound)
            .collect())
    }
}

fn start_coordinator(ports: RedbOperationalPorts) -> RunningCommandCoordinator {
    start_coordinator_with_capacity(ports, 8)
}

fn start_coordinator_with_capacity(
    ports: RedbOperationalPorts,
    workload_capacity: u16,
) -> RunningCommandCoordinator {
    let conflicts: Arc<dyn ConflictManager> = Arc::new(
        ShardedConflictManager::new(ConflictManagerConfig::default())
            .expect("start conflict manager"),
    );
    RunningCommandCoordinator::start(
        CoordinatorWorkloadCapacity::new(workload_capacity).expect("nonzero coordinator capacity"),
        CoordinatorDurability::Sync,
        ports,
        conflicts,
        Arc::new(FixedAdmissionClock),
        Arc::new(SequentialServiceUuids::default()),
        Arc::new(IncrementingAdministrationClock::new()),
        Arc::new(FixedAuthorizationClock),
        Arc::new(SequentialProvenanceIds::default()),
        Arc::new(DiscardApplicationCommitNotifications),
    )
    .expect("start production coordinator")
}

struct DiscardApplicationCommitNotifications;

impl ApplicationCommitNotificationSink for DiscardApplicationCommitNotifications {
    fn publish_first_commit(
        &self,
        _: CommitSequence,
    ) -> Result<(), ApplicationCommitNotificationError> {
        Ok(())
    }
}

struct FixedAdmissionClock;

impl AdmissionClock for FixedAdmissionClock {
    fn now(&self) -> Result<Timestamp, AdmissionClockError> {
        Ok(timestamp(BASE_SECONDS + 20))
    }
}

struct FixedAuthorizationClock;

impl AuthorizationClock for FixedAuthorizationClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(timestamp(BASE_SECONDS + 20))
    }
}

struct IncrementingAdministrationClock(AtomicI64);

impl IncrementingAdministrationClock {
    fn new() -> Self {
        Self(AtomicI64::new(BASE_SECONDS + 30))
    }
}

impl AdministrationClock for IncrementingAdministrationClock {
    fn now(&self) -> Result<Timestamp, AdministrationClockError> {
        Ok(timestamp(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

#[derive(Default)]
struct SequentialProvenanceIds(AtomicU64);

#[derive(Default)]
struct SequentialServiceUuids(AtomicU64);

impl ServiceUuidV7Source for SequentialServiceUuids {
    fn next_uuid_v7(&self) -> Result<[u8; 16], ServiceUuidV7SourceError> {
        let seed = self.0.fetch_add(1, Ordering::Relaxed) as u8;
        Ok(uuid_bytes(seed.wrapping_add(0x50)))
    }
}

impl ProvenanceIdSource for SequentialProvenanceIds {
    fn next_provenance_id(&self) -> Result<ProvenanceId, ProvenanceIdSourceError> {
        let seed = self.0.fetch_add(1, Ordering::Relaxed) as u8;
        ProvenanceId::from_bytes(uuid_bytes(seed.wrapping_add(0x70)))
            .map_err(|_| ProvenanceIdSourceError)
    }
}

struct FixedDigestProvider;

impl IdempotencyDigestProvider for FixedDigestProvider {
    fn digest_candidates(
        &self,
        caller_key: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
        let mut digest = [0_u8; 32];
        for (index, byte) in caller_key.expose_secret().bytes().enumerate() {
            let slot = index % digest.len();
            digest[slot] = digest[slot]
                .wrapping_mul(31)
                .wrapping_add(byte)
                .wrapping_add(index as u8);
        }
        digest[31] ^= caller_key.expose_secret().len() as u8;
        IdempotencyDigestCandidatesV1::new(vec![IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key ID"),
            digest,
        )])
    }
}

pub(crate) struct HarnessPolicy {
    fixture: AuthorizationFixture,
    narrowed_fixture: AuthorizationFixture,
    trusted_audiences: TrustedAudienceCatalog,
    use_narrowed_fixture: AtomicBool,
    narrow_after_next_allow: AtomicBool,
    restore_after_narrowed_allow: AtomicBool,
    calls: AtomicUsize,
    allowed_calls: AtomicUsize,
    revoke_after_allowed_call: AtomicUsize,
    advance_clock_after_allowed_call: AtomicUsize,
    advance_clock_to_seconds: AtomicI64,
    audit_discovery_operations: AtomicBool,
    partition_constraints: Mutex<Vec<Option<PartitionConstraint>>>,
    panic_next: AtomicBool,
    capability_order: Arc<Mutex<Vec<&'static str>>>,
    discovery_order: Arc<Mutex<Vec<&'static str>>>,
    /// Settable authorization time shared by evaluation and the view checkpoint.
    ///
    /// Production samples one wall clock for both; the harness samples this.
    now_seconds: AtomicI64,
}

impl HarnessPolicy {
    #[allow(clippy::too_many_arguments)]
    fn new(
        allow_read_commit: bool,
        command_reference: &ExecutablePlanRef,
        projection_id: ProjectionId,
        partition_scope: PartitionScopeV1,
        broad_operations: bool,
        contract: &ContractBundle,
        narrow_partition: PartitionKey,
        authorize_all_commands: bool,
        capability_order: Arc<Mutex<Vec<&'static str>>>,
        discovery_order: Arc<Mutex<Vec<&'static str>>>,
        extra_permissions: Vec<CapabilityPermissionV1>,
    ) -> Self {
        let mut permissions = vec![
            CapabilityPermissionV1::InvokeCommand(
                command_reference.contract_lineage().clone(),
                command_reference.command_id(),
            ),
            CapabilityPermissionV1::QueryProjection(
                command_reference.contract_lineage().clone(),
                projection_id,
            ),
        ];
        if allow_read_commit {
            permissions.push(
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadCommit)
                    .expect("unparameterized read-commit permission"),
            );
            permissions.push(
                CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::SubscribeCommits,
                )
                .expect("unparameterized subscribe-commits permission"),
            );
            permissions.push(
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadStatistics)
                    .expect("unparameterized read-statistics permission"),
            );
        }
        if broad_operations {
            let unparameterized = [
                CapabilityPermissionKindV1::ValidateContract,
                CapabilityPermissionKindV1::ReadContract,
                CapabilityPermissionKindV1::ScanCommits,
                CapabilityPermissionKindV1::ReadProvenance,
                CapabilityPermissionKindV1::InspectOutbox,
                CapabilityPermissionKindV1::ReadHealth,
                CapabilityPermissionKindV1::CheckAdHocQuery,
                CapabilityPermissionKindV1::ExplainAdHocQuery,
                CapabilityPermissionKindV1::ExecuteAdHocQuery,
                CapabilityPermissionKindV1::CreateCapability,
                CapabilityPermissionKindV1::RevokeCapability,
                CapabilityPermissionKindV1::AdministerCapabilities,
            ];
            permissions.extend(unparameterized.into_iter().map(|kind| {
                CapabilityPermissionV1::unparameterized(kind)
                    .expect("broad harness permission is unparameterized")
            }));
            for command in contract.commands() {
                permissions.push(CapabilityPermissionV1::ExplainCommand(
                    contract.lineage().clone(),
                    command.command_id(),
                ));
                if authorize_all_commands && command.command_id() != command_reference.command_id()
                {
                    permissions.push(CapabilityPermissionV1::InvokeCommand(
                        contract.lineage().clone(),
                        command.command_id(),
                    ));
                }
            }
            for entity in contract.schema().entities() {
                permissions.push(CapabilityPermissionV1::ReadEntity(
                    contract.lineage().clone(),
                    entity.id(),
                ));
                permissions.extend(entity.indexes().iter().map(|index| {
                    CapabilityPermissionV1::ScanIndex(contract.lineage().clone(), index.id())
                }));
            }
            for projection in contract.projections() {
                permissions.push(CapabilityPermissionV1::ReadProjectionStatus(
                    contract.lineage().clone(),
                    projection.projection_id(),
                ));
            }
        }
        permissions.extend(extra_permissions);
        permissions.push(
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::DeployContract)
                .expect("unparameterized deploy-contract permission"),
        );
        let permissions = CapabilityPermissionsV1::new(permissions).expect("canonical permissions");
        let field_visibility = if broad_operations {
            contract
                .schema()
                .entities()
                .iter()
                .map(|entity| {
                    riffdb_types::EntityFieldVisibilityV1::new(
                        contract.lineage().clone(),
                        entity.id(),
                        entity
                            .record()
                            .fields()
                            .iter()
                            .map(riffdb_contract_ir::FieldSchema::id)
                            .collect(),
                    )
                    .expect("entity field visibility")
                })
                .collect()
        } else {
            Vec::new()
        };
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            partition_scope,
            permissions.clone(),
            field_visibility.clone(),
            NonZeroU16::new(100).expect("nonzero row limit"),
            Vec::new(),
        )
        .expect("bounded capability grant");
        let narrowed_grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(
                contract.lineage().clone(),
                narrow_partition,
            )])
            .expect("one narrow harness partition"),
            permissions,
            field_visibility,
            NonZeroU16::new(100).expect("nonzero row limit"),
            Vec::new(),
        )
        .expect("bounded narrowed capability grant");
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database_id(),
            environment(),
            ActorId::new("service-harness-maintainer").expect("bounded principal"),
            ActorKind::Human,
            audience(),
            AuthorizationFixtureTimes::new(
                timestamp(BASE_SECONDS),
                timestamp(BASE_SECONDS + 1_000),
                timestamp(BASE_SECONDS + 10),
            ),
            grant,
        ))
        .expect("authorization fixture");
        let narrowed_fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database_id(),
            environment(),
            ActorId::new("service-harness-maintainer").expect("bounded principal"),
            ActorKind::Human,
            audience(),
            AuthorizationFixtureTimes::new(
                timestamp(BASE_SECONDS),
                timestamp(BASE_SECONDS + 1_000),
                timestamp(BASE_SECONDS + 10),
            ),
            narrowed_grant,
        ))
        .expect("narrowed authorization fixture");
        Self {
            fixture,
            narrowed_fixture,
            trusted_audiences: TrustedAudienceCatalog::new(vec![audience()])
                .expect("one trusted harness audience"),
            use_narrowed_fixture: AtomicBool::new(false),
            narrow_after_next_allow: AtomicBool::new(false),
            restore_after_narrowed_allow: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            allowed_calls: AtomicUsize::new(0),
            revoke_after_allowed_call: AtomicUsize::new(0),
            advance_clock_after_allowed_call: AtomicUsize::new(0),
            advance_clock_to_seconds: AtomicI64::new(0),
            audit_discovery_operations: AtomicBool::new(false),
            partition_constraints: Mutex::new(Vec::new()),
            panic_next: AtomicBool::new(false),
            capability_order,
            discovery_order,
            now_seconds: AtomicI64::new(BASE_SECONDS + 20),
        }
    }

    pub(crate) fn principal(&self) -> AuthenticatedPrincipal {
        self.fixture.authenticated_principal().clone()
    }

    fn revoke(&self) {
        // No generation is hand-set here. The fixture records the real
        // active-to-revoked record transition, and the capability-view
        // generation this policy reports is derived from that transition —
        // exactly as production derives it from a view publish.
        self.fixture
            .revoke_current(timestamp(BASE_SECONDS + 15))
            .expect("revoke current harness capability");
        self.narrowed_fixture
            .revoke_current(timestamp(BASE_SECONDS + 15))
            .expect("revoke narrowed harness capability");
    }

    /// Returns the authorization time this policy currently samples.
    pub(crate) fn now(&self) -> Timestamp {
        timestamp(self.now_seconds.load(Ordering::Acquire))
    }

    /// Moves the authorization clock forward, as wall time does between safe points.
    pub(crate) fn advance_to(&self, seconds: i64) {
        self.now_seconds.store(seconds, Ordering::Release);
    }

    fn use_narrowed_for_next_allow(&self) {
        self.narrow_after_next_allow.store(true, Ordering::Release);
        self.restore_after_narrowed_allow
            .store(true, Ordering::Release);
    }

    fn use_narrowed_once_now(&self) {
        self.use_narrowed_fixture.store(true, Ordering::Release);
        self.restore_after_narrowed_allow
            .store(true, Ordering::Release);
    }

    fn advance_clock_after_next_allows(&self, allow_count: usize, seconds: i64) {
        assert!(allow_count != 0, "the clock advances after an allow");
        self.advance_clock_to_seconds
            .store(seconds, Ordering::Release);
        self.advance_clock_after_allowed_call.store(
            self.allowed_calls
                .load(Ordering::Acquire)
                .checked_add(allow_count)
                .expect("bounded policy-call count"),
            Ordering::Release,
        );
    }

    fn deny_after_next_allows(&self, allow_count: usize) {
        assert!(
            allow_count != 0,
            "revocation needs a future allow safe point"
        );
        self.revoke_after_allowed_call.store(
            self.allowed_calls
                .load(Ordering::Acquire)
                .checked_add(allow_count)
                .expect("bounded policy-call count"),
            Ordering::Release,
        );
    }

    fn audit_discovery_operations(&self) {
        self.audit_discovery_operations
            .store(true, Ordering::Release);
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }

    pub(crate) fn allowed_calls(&self) -> usize {
        self.allowed_calls.load(Ordering::Acquire)
    }

    pub(crate) fn partition_constraints(&self) -> Vec<Option<PartitionConstraint>> {
        self.partition_constraints
            .lock()
            .expect("policy partition-constraint mutex")
            .clone()
    }

    fn panic_next(&self) {
        self.panic_next.store(true, Ordering::Release);
    }
}

impl riffdb_service::CurrentPolicyPort for HarnessPolicy {
    fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OperationRequest,
    ) -> Result<Decision, AuthorizationError> {
        let operation = request.operation();
        if matches!(
            operation,
            ServiceOperationV1::DiscoverCommandTools | ServiceOperationV1::DiscoverResources
        ) {
            self.discovery_order
                .lock()
                .expect("discovery-order mutex")
                .push("policy");
        }
        if matches!(
            operation,
            ServiceOperationV1::CreateCapability | ServiceOperationV1::ExecuteQuery
        ) {
            self.capability_order
                .lock()
                .expect("capability-order mutex")
                .push("policy");
        }
        self.calls.fetch_add(1, Ordering::AcqRel);
        assert!(
            !self.panic_next.swap(false, Ordering::AcqRel),
            "injected current-policy panic"
        );
        let fixture = if self.use_narrowed_fixture.load(Ordering::Acquire) {
            &self.narrowed_fixture
        } else {
            &self.fixture
        };
        let resolver = fixture.current_capability_resolver();
        let decision = CurrentAuthorizer::new(
            &resolver,
            &HarnessAuthorizationClock(self.now()),
            &NoopAuthorizationTelemetry,
            database_id(),
            environment(),
        )
        .with_trusted_audience_catalog(&self.trusted_audiences)
        .authorize(principal, request)?;
        let decision = if self.audit_discovery_operations.load(Ordering::Acquire)
            && matches!(
                operation,
                ServiceOperationV1::DiscoverCommandTools | ServiceOperationV1::DiscoverResources
            )
            && matches!(&decision, Decision::Allow(_))
        {
            decision
                .with_test_standard_read_discovery_audit()
                .expect("current discovery allow accepts the test-only audit fixture")
        } else {
            decision
        };
        if let Decision::Allow(authorized) = &decision {
            let allowed_call = self.allowed_calls.fetch_add(1, Ordering::AcqRel) + 1;
            self.partition_constraints
                .lock()
                .expect("policy partition-constraint mutex")
                .push(authorized.obligations().partition_constraint().cloned());
            if self.narrow_after_next_allow.swap(false, Ordering::AcqRel) {
                self.use_narrowed_fixture.store(true, Ordering::Release);
            } else if self
                .restore_after_narrowed_allow
                .swap(false, Ordering::AcqRel)
            {
                self.use_narrowed_fixture.store(false, Ordering::Release);
            }
            if self
                .revoke_after_allowed_call
                .compare_exchange(allowed_call, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.revoke();
            }
            if self
                .advance_clock_after_allowed_call
                .compare_exchange(allowed_call, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.advance_to(self.advance_clock_to_seconds.load(Ordering::Acquire));
            }
        }
        Ok(decision)
    }

    fn capability_view_generation(&self) -> Option<u64> {
        // Derived, never hand-set: the two fixture readers each advance their
        // own generation on any record or resolution-mode change, and which
        // fixture is current is itself part of what resolution returns. The
        // packing is injective for the small counters a test can reach, so
        // "unchanged" here means "nothing that resolution depends on moved".
        let current = self.fixture.view_generation()?;
        let narrowed = self.narrowed_fixture.view_generation()?;
        let selector = u64::from(self.use_narrowed_fixture.load(Ordering::Acquire));
        Some((current << 33) | (narrowed << 1) | selector)
    }

    fn capability_view_checkpoint(&self) -> Option<CapabilityViewCheckpoint> {
        // Same clock the evaluation above samples.
        Some(CapabilityViewCheckpoint::new(
            self.capability_view_generation()?,
            self.now(),
        ))
    }
}

/// Authorization clock reading one settable harness instant.
struct HarnessAuthorizationClock(Timestamp);

impl AuthorizationClock for HarnessAuthorizationClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(self.0)
    }
}

struct PendingReadState {
    sender: Mutex<
        Option<PortCompletionSender<Option<AuthoritativeCommitSnapshot>, AuthoritativeReadError>>,
    >,
    submitted: Notify,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProjectionObservation {
    Pending(ProjectionPageFence),
    ReadyEmpty(ProjectionPageFence),
    Stalled,
}

pub(crate) struct HarnessPorts {
    shared: Arc<HarnessPortState>,
}

struct HarnessPortState {
    active_catalog: Mutex<ActiveCatalogSnapshot>,
    historical_bundles:
        Mutex<BTreeMap<(ContractLineage, ContractVersion), ValidatedContractBundle>>,
    executable_plan: ResolvedExecutablePlan,
    additional_plans: Mutex<Vec<ResolvedExecutablePlan>>,
    prepare_active_mode: AtomicU8,
    prepare_active_calls: AtomicUsize,
    active_catalog_reservations: AtomicUsize,
    active_catalog_submissions: AtomicUsize,
    active_catalog_reservation_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    prepare_contract_version_calls: AtomicUsize,
    prepare_active_started: Notify,
    compatible_activation: Mutex<Option<ActiveCatalogSnapshot>>,
    compatible_activation_release: Notify,
    prepare_deployment_mode: AtomicU8,
    prepare_deployment_calls: AtomicUsize,
    prepare_deployment_started: Notify,
    panic_executable_plan: AtomicBool,
    panic_revoke_target_read: AtomicBool,
    read_mode: ReadCommitMode,
    read_reservations: AtomicUsize,
    read_submissions: AtomicUsize,
    read_reservation_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    application_head: AtomicU64,
    application_head_observations: AtomicUsize,
    read_commit_snapshot: Mutex<Option<AuthoritativeCommitSnapshot>>,
    index_pages: Mutex<VecDeque<(Vec<AuthoritativeIndexRow>, Option<IndexEntryKey>)>>,
    index_requests: Mutex<Vec<AuthoritativeIndexRequest>>,
    index_submissions: AtomicUsize,
    index_submission_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    commit_scan_snapshots: Mutex<Vec<AuthoritativeCommitSnapshot>>,
    provenance_snapshot: Mutex<Option<AuthoritativeProvenanceSnapshot>>,
    pending_read: PendingReadState,
    subscription_notifications: Mutex<VecDeque<AuthoritativeCommitNotification>>,
    subscription_pending: AtomicBool,
    pending_subscription: Mutex<
        Option<PortCompletionSender<Box<dyn CommitNotificationSource>, AuthoritativeReadError>>,
    >,
    subscription_submitted: Notify,
    subscription_source_dropped: Arc<AtomicBool>,
    subscription_acknowledgements: Arc<Mutex<Vec<CommitSequence>>>,
    subscription_stall_next: AtomicBool,
    subscription_next_waiting: Arc<AtomicBool>,
    commit_continuation_panic: Arc<AtomicU8>,
    outcome_response: Mutex<Result<Option<AuthoritativeOutcomeSnapshot>, AuthoritativeReadError>>,
    outcome_reservations: AtomicUsize,
    outcome_submissions: AtomicUsize,
    outcome_submission_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    last_outcome_request: Mutex<Option<AuthoritativeOutcomeRequest>>,
    projection_observations: Mutex<VecDeque<ProjectionObservation>>,
    projection_observation_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    projection_reservations: AtomicUsize,
    projection_submissions: AtomicUsize,
    projection_requests: Mutex<Vec<ProjectionPortRequest>>,
    pending_projection:
        Mutex<Option<PortCompletionSender<ProjectionPortResult, ProjectionPortError>>>,
    projection_stalled: Notify,
    operation_calls: Mutex<Vec<&'static str>>,
    token_issue_calls: AtomicUsize,
    capability_order: Arc<Mutex<Vec<&'static str>>>,
    discovery_order: Arc<Mutex<Vec<&'static str>>>,
}

impl HarnessPorts {
    fn new(
        read_mode: ReadCommitMode,
        active_catalog: ActiveCatalogSnapshot,
        executable_plan: ResolvedExecutablePlan,
        capability_order: Arc<Mutex<Vec<&'static str>>>,
        discovery_order: Arc<Mutex<Vec<&'static str>>>,
    ) -> Self {
        let initial_bundle = active_catalog.bundle().clone();
        let mut historical_bundles = BTreeMap::new();
        historical_bundles.insert(
            (
                initial_bundle.lineage().clone(),
                initial_bundle.contract_version(),
            ),
            initial_bundle,
        );
        Self {
            shared: Arc::new(HarnessPortState {
                active_catalog: Mutex::new(active_catalog),
                historical_bundles: Mutex::new(historical_bundles),
                executable_plan,
                additional_plans: Mutex::new(Vec::new()),
                prepare_active_mode: AtomicU8::new(CATALOG_READY),
                prepare_active_calls: AtomicUsize::new(0),
                active_catalog_reservations: AtomicUsize::new(0),
                active_catalog_submissions: AtomicUsize::new(0),
                active_catalog_reservation_hook: Mutex::new(None),
                prepare_contract_version_calls: AtomicUsize::new(0),
                prepare_active_started: Notify::new(),
                compatible_activation: Mutex::new(None),
                compatible_activation_release: Notify::new(),
                prepare_deployment_mode: AtomicU8::new(CATALOG_READY),
                prepare_deployment_calls: AtomicUsize::new(0),
                prepare_deployment_started: Notify::new(),
                panic_executable_plan: AtomicBool::new(false),
                panic_revoke_target_read: AtomicBool::new(false),
                read_mode,
                read_reservations: AtomicUsize::new(0),
                read_submissions: AtomicUsize::new(0),
                read_reservation_hook: Mutex::new(None),
                application_head: AtomicU64::new(0),
                application_head_observations: AtomicUsize::new(0),
                read_commit_snapshot: Mutex::new(None),
                index_pages: Mutex::new(VecDeque::new()),
                index_requests: Mutex::new(Vec::new()),
                index_submissions: AtomicUsize::new(0),
                index_submission_hook: Mutex::new(None),
                commit_scan_snapshots: Mutex::new(Vec::new()),
                provenance_snapshot: Mutex::new(None),
                pending_read: PendingReadState {
                    sender: Mutex::new(None),
                    submitted: Notify::new(),
                },
                subscription_notifications: Mutex::new(VecDeque::new()),
                subscription_pending: AtomicBool::new(false),
                pending_subscription: Mutex::new(None),
                subscription_submitted: Notify::new(),
                subscription_source_dropped: Arc::new(AtomicBool::new(false)),
                subscription_acknowledgements: Arc::new(Mutex::new(Vec::new())),
                subscription_stall_next: AtomicBool::new(false),
                subscription_next_waiting: Arc::new(AtomicBool::new(false)),
                commit_continuation_panic: Arc::new(AtomicU8::new(COMMIT_CONTINUATION_PANIC_NONE)),
                outcome_response: Mutex::new(Ok(None)),
                outcome_reservations: AtomicUsize::new(0),
                outcome_submissions: AtomicUsize::new(0),
                outcome_submission_hook: Mutex::new(None),
                last_outcome_request: Mutex::new(None),
                projection_observations: Mutex::new(VecDeque::new()),
                projection_observation_hook: Mutex::new(None),
                projection_reservations: AtomicUsize::new(0),
                projection_submissions: AtomicUsize::new(0),
                projection_requests: Mutex::new(Vec::new()),
                pending_projection: Mutex::new(None),
                projection_stalled: Notify::new(),
                operation_calls: Mutex::new(Vec::new()),
                token_issue_calls: AtomicUsize::new(0),
                capability_order,
                discovery_order,
            }),
        }
    }

    fn install_additional_plan(&self, plan: ResolvedExecutablePlan) {
        self.shared
            .additional_plans
            .lock()
            .expect("additional plans mutex")
            .push(plan);
    }

    fn set_active_catalog_reservation_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .shared
            .active_catalog_reservation_hook
            .lock()
            .expect("active-catalog reservation hook mutex") = Some(hook);
    }

    fn set_read_reservation_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .shared
            .read_reservation_hook
            .lock()
            .expect("read reservation hook mutex") = Some(hook);
    }

    fn set_application_head(&self, head: Option<CommitSequence>) {
        self.shared
            .application_head
            .store(head.map_or(0, CommitSequence::get), Ordering::Release);
    }

    fn application_head_observations(&self) -> usize {
        self.shared
            .application_head_observations
            .load(Ordering::Acquire)
    }

    fn configure_index_pages(
        &self,
        pages: Vec<(Vec<AuthoritativeIndexRow>, Option<IndexEntryKey>)>,
    ) {
        *self.shared.index_pages.lock().expect("index pages mutex") = pages.into();
    }

    fn set_index_submission_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .shared
            .index_submission_hook
            .lock()
            .expect("index submission hook mutex") = Some(hook);
    }

    pub(crate) fn index_submissions(&self) -> usize {
        self.shared.index_submissions.load(Ordering::Acquire)
    }

    pub(crate) fn index_requests(&self) -> Vec<AuthoritativeIndexRequest> {
        self.shared
            .index_requests
            .lock()
            .expect("index requests mutex")
            .clone()
    }

    fn replace_active_catalog(&self, successor: ActiveCatalogSnapshot) {
        let bundle = successor.bundle().clone();
        self.shared
            .historical_bundles
            .lock()
            .expect("historical bundles mutex")
            .insert(
                (bundle.lineage().clone(), bundle.contract_version()),
                bundle,
            );
        *self
            .shared
            .active_catalog
            .lock()
            .expect("active catalog mutex") = successor;
    }

    fn set_prepare_active_mode(&self, mode: u8) {
        self.shared
            .prepare_active_mode
            .store(mode, Ordering::Release);
    }

    fn configure_compatible_activation_race(&self, successor: ActiveCatalogSnapshot) {
        let replaced = self
            .shared
            .compatible_activation
            .lock()
            .expect("compatible activation mutex")
            .replace(successor);
        assert!(
            replaced.is_none(),
            "only one compatible activation may be pending"
        );
        self.set_prepare_active_mode(CATALOG_COMPATIBLE_ACTIVATION_RACE);
    }

    fn complete_compatible_activation_race(&self) {
        let successor = self
            .shared
            .compatible_activation
            .lock()
            .expect("compatible activation mutex")
            .take()
            .expect("one pending compatible activation");
        self.replace_active_catalog(successor);
        self.shared.compatible_activation_release.notify_one();
    }

    fn active_catalog_version(&self) -> ContractVersion {
        self.shared
            .active_catalog
            .lock()
            .expect("active catalog mutex")
            .pointer()
            .contract_version()
    }

    pub(crate) fn prepare_active_calls(&self) -> usize {
        self.shared.prepare_active_calls.load(Ordering::Acquire)
    }

    pub(crate) fn active_catalog_reservations(&self) -> usize {
        self.shared
            .active_catalog_reservations
            .load(Ordering::Acquire)
    }

    pub(crate) fn active_catalog_submissions(&self) -> usize {
        self.shared
            .active_catalog_submissions
            .load(Ordering::Acquire)
    }

    pub(crate) fn prepare_contract_version_calls(&self) -> usize {
        self.shared
            .prepare_contract_version_calls
            .load(Ordering::Acquire)
    }

    pub(crate) fn token_issue_calls(&self) -> usize {
        self.shared.token_issue_calls.load(Ordering::Acquire)
    }

    pub(crate) fn capability_order(&self) -> Vec<&'static str> {
        self.shared
            .capability_order
            .lock()
            .expect("capability-order mutex")
            .clone()
    }

    fn clear_discovery_order(&self) {
        self.shared
            .discovery_order
            .lock()
            .expect("discovery-order mutex")
            .clear();
    }

    fn discovery_order(&self) -> Vec<&'static str> {
        self.shared
            .discovery_order
            .lock()
            .expect("discovery-order mutex")
            .clone()
    }

    async fn wait_for_prepare_active(&self) {
        self.shared.prepare_active_started.notified().await;
    }

    fn set_prepare_deployment_mode(&self, mode: u8) {
        self.shared
            .prepare_deployment_mode
            .store(mode, Ordering::Release);
    }

    async fn wait_for_prepare_deployment(&self) {
        self.shared.prepare_deployment_started.notified().await;
    }

    fn prepare_deployment_calls(&self) -> usize {
        self.shared.prepare_deployment_calls.load(Ordering::Acquire)
    }

    fn panic_executable_plan(&self) {
        self.shared
            .panic_executable_plan
            .store(true, Ordering::Release);
    }

    fn panic_revoke_target_read(&self) {
        self.shared
            .panic_revoke_target_read
            .store(true, Ordering::Release);
    }

    pub(crate) fn read_reservations(&self) -> usize {
        self.shared.read_reservations.load(Ordering::Acquire)
    }

    pub(crate) fn read_submissions(&self) -> usize {
        self.shared.read_submissions.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_for_read_submission(&self) {
        loop {
            if self
                .shared
                .pending_read
                .sender
                .lock()
                .expect("pending read sender mutex")
                .is_some()
            {
                return;
            }
            self.shared.pending_read.submitted.notified().await;
        }
    }

    pub(crate) fn release_read_not_found(&self) {
        self.shared
            .pending_read
            .sender
            .lock()
            .expect("pending read sender mutex")
            .take()
            .expect("one pending read")
            .complete(Ok(None));
    }

    pub(crate) fn abandon_pending_read(&self) {
        self.shared
            .pending_read
            .sender
            .lock()
            .expect("pending read sender mutex")
            .take();
    }

    fn set_read_commit_snapshot(&self, snapshot: AuthoritativeCommitSnapshot) {
        *self
            .shared
            .read_commit_snapshot
            .lock()
            .expect("read commit snapshot mutex") = Some(snapshot);
    }

    fn set_commit_scan_snapshots(&self, snapshots: Vec<AuthoritativeCommitSnapshot>) {
        *self
            .shared
            .commit_scan_snapshots
            .lock()
            .expect("commit scan snapshots mutex") = snapshots;
    }

    fn set_provenance_snapshot(&self, snapshot: AuthoritativeProvenanceSnapshot) {
        *self
            .shared
            .provenance_snapshot
            .lock()
            .expect("provenance snapshot mutex") = Some(snapshot);
    }

    fn configure_commit_subscription(
        &self,
        notifications: Vec<AuthoritativeCommitNotification>,
        pending_establishment: bool,
    ) {
        *self
            .shared
            .subscription_notifications
            .lock()
            .expect("subscription notifications mutex") = notifications.into();
        self.shared
            .subscription_pending
            .store(pending_establishment, Ordering::Release);
        self.shared
            .subscription_source_dropped
            .store(false, Ordering::Release);
        self.shared
            .subscription_stall_next
            .store(false, Ordering::Release);
        self.shared
            .subscription_next_waiting
            .store(false, Ordering::Release);
        self.shared
            .commit_continuation_panic
            .store(COMMIT_CONTINUATION_PANIC_NONE, Ordering::Release);
        self.shared
            .subscription_acknowledgements
            .lock()
            .expect("subscription acknowledgements mutex")
            .clear();
    }

    fn stall_next_commit_notification(&self) {
        self.shared
            .subscription_stall_next
            .store(true, Ordering::Release);
    }

    fn commit_notification_is_stalled(&self) -> bool {
        self.shared
            .subscription_next_waiting
            .load(Ordering::Acquire)
    }

    async fn wait_for_commit_subscription_submission(&self) {
        loop {
            if self
                .shared
                .pending_subscription
                .lock()
                .expect("pending subscription mutex")
                .is_some()
            {
                return;
            }
            self.shared.subscription_submitted.notified().await;
        }
    }

    fn release_commit_subscription_source(&self) {
        let sender = self
            .shared
            .pending_subscription
            .lock()
            .expect("pending subscription mutex")
            .take()
            .expect("one pending subscription");
        sender.complete(Ok(subscription_source(&self.shared)));
    }

    fn commit_subscription_source_dropped(&self) -> bool {
        self.shared
            .subscription_source_dropped
            .load(Ordering::Acquire)
    }

    fn commit_subscription_acknowledgements(&self) -> Vec<CommitSequence> {
        self.shared
            .subscription_acknowledgements
            .lock()
            .expect("subscription acknowledgements mutex")
            .clone()
    }

    fn panic_commit_continuation_at(&self, point: CommitContinuationPanic) {
        self.shared
            .commit_continuation_panic
            .store(point.code(), Ordering::Release);
    }

    fn set_outcome_response(
        &self,
        response: Result<Option<AuthoritativeOutcomeSnapshot>, AuthoritativeReadError>,
    ) {
        *self
            .shared
            .outcome_response
            .lock()
            .expect("outcome response mutex") = response;
    }

    fn set_outcome_submission_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .shared
            .outcome_submission_hook
            .lock()
            .expect("outcome submission hook mutex") = Some(hook);
    }

    pub(crate) fn outcome_reservations(&self) -> usize {
        self.shared.outcome_reservations.load(Ordering::Acquire)
    }

    pub(crate) fn outcome_submissions(&self) -> usize {
        self.shared.outcome_submissions.load(Ordering::Acquire)
    }

    pub(crate) fn last_outcome_request(&self) -> Option<AuthoritativeOutcomeRequest> {
        self.shared
            .last_outcome_request
            .lock()
            .expect("outcome request mutex")
            .clone()
    }

    fn set_projection_observations(&self, observations: Vec<ProjectionObservation>) {
        *self
            .shared
            .projection_observations
            .lock()
            .expect("projection observations mutex") = observations.into();
    }

    fn set_projection_observation_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .shared
            .projection_observation_hook
            .lock()
            .expect("projection observation hook mutex") = Some(hook);
    }

    pub(crate) fn projection_reservations(&self) -> usize {
        self.shared.projection_reservations.load(Ordering::Acquire)
    }

    pub(crate) fn projection_submissions(&self) -> usize {
        self.shared.projection_submissions.load(Ordering::Acquire)
    }

    pub(crate) fn projection_requests(&self) -> Vec<ProjectionPortRequest> {
        self.shared
            .projection_requests
            .lock()
            .expect("projection requests mutex")
            .clone()
    }

    pub(crate) fn operation_calls(&self) -> Vec<&'static str> {
        self.shared
            .operation_calls
            .lock()
            .expect("operation-call mutex")
            .clone()
    }

    async fn wait_for_stalled_projection(&self) {
        loop {
            if self
                .shared
                .pending_projection
                .lock()
                .expect("pending projection mutex")
                .is_some()
            {
                return;
            }
            self.shared.projection_stalled.notified().await;
        }
    }
}

struct ReadCommitPermit {
    shared: Arc<HarnessPortState>,
}

struct ApplicationHeadPermit {
    shared: Arc<HarnessPortState>,
}

struct SubscribeCommitPermit {
    shared: Arc<HarnessPortState>,
}

struct HarnessCommitNotificationSource {
    notifications: VecDeque<AuthoritativeCommitNotification>,
    dropped: Arc<AtomicBool>,
    stall_next: bool,
    waiting: Arc<AtomicBool>,
    panic: Arc<AtomicU8>,
    acknowledgements: Arc<Mutex<Vec<CommitSequence>>>,
}

impl CommitNotificationSource for HarnessCommitNotificationSource {
    fn next(&mut self) -> PortFuture<'_, AuthoritativeCommitNotification, AuthoritativeReadError> {
        assert!(
            self.panic
                .compare_exchange(
                    COMMIT_CONTINUATION_PANIC_SOURCE_NEXT,
                    COMMIT_CONTINUATION_PANIC_NONE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err(),
            "injected commit-notification source call panic"
        );
        let notification = self
            .notifications
            .pop_front()
            .unwrap_or(AuthoritativeCommitNotification::Closed);
        let stall = std::mem::take(&mut self.stall_next);
        let waiting = Arc::clone(&self.waiting);
        let panic = Arc::clone(&self.panic);
        Box::pin(async move {
            if stall {
                waiting.store(true, Ordering::Release);
                std::future::pending::<()>().await;
            }
            assert!(
                panic
                    .compare_exchange(
                        COMMIT_CONTINUATION_PANIC_SOURCE_POLL,
                        COMMIT_CONTINUATION_PANIC_NONE,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err(),
                "injected commit-notification source poll panic"
            );
            Ok(notification)
        })
    }

    fn acknowledge(
        &mut self,
        delivered_through: CommitSequence,
    ) -> Result<(), AuthoritativeReadError> {
        self.acknowledgements
            .lock()
            .expect("subscription acknowledgements mutex")
            .push(delivered_through);
        Ok(())
    }
}

impl Drop for HarnessCommitNotificationSource {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
        assert!(
            self.panic
                .compare_exchange(
                    COMMIT_CONTINUATION_PANIC_SOURCE_DROP,
                    COMMIT_CONTINUATION_PANIC_NONE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err(),
            "injected commit-notification source drop panic"
        );
    }
}

fn subscription_source(shared: &Arc<HarnessPortState>) -> Box<dyn CommitNotificationSource> {
    let notifications = std::mem::take(
        &mut *shared
            .subscription_notifications
            .lock()
            .expect("subscription notifications mutex"),
    );
    Box::new(HarnessCommitNotificationSource {
        notifications,
        dropped: Arc::clone(&shared.subscription_source_dropped),
        stall_next: shared.subscription_stall_next.swap(false, Ordering::AcqRel),
        waiting: Arc::clone(&shared.subscription_next_waiting),
        panic: Arc::clone(&shared.commit_continuation_panic),
        acknowledgements: Arc::clone(&shared.subscription_acknowledgements),
    })
}

struct ReadOutcomePermit {
    shared: Arc<HarnessPortState>,
}

struct ProjectionQueryPermit {
    shared: Arc<HarnessPortState>,
}

struct ActiveCatalogPermit {
    shared: Arc<HarnessPortState>,
}

struct ContractVersionPermit {
    shared: Arc<HarnessPortState>,
}

struct ReadEntityPermit {
    shared: Arc<HarnessPortState>,
}

struct ScanIndexPermit {
    shared: Arc<HarnessPortState>,
}

struct ScanCommitsPermit {
    shared: Arc<HarnessPortState>,
}

struct TraceProvenancePermit {
    shared: Arc<HarnessPortState>,
}

struct ProjectionStatusPermit {
    shared: Arc<HarnessPortState>,
}

struct HealthPermit {
    shared: Arc<HarnessPortState>,
}

struct StatisticsPermit {
    shared: Arc<HarnessPortState>,
}

struct OutboxPermit {
    shared: Arc<HarnessPortState>,
}

fn record_operation(shared: &HarnessPortState, operation: &'static str) {
    shared
        .operation_calls
        .lock()
        .expect("operation-call mutex")
        .push(operation);
}

impl PortCapacityPermit<(), FrontierPosition, AuthoritativeReadError> for ApplicationHeadPermit {
    fn submit(
        self: Box<Self>,
        (): (),
    ) -> Result<PortReceipt<FrontierPosition, AuthoritativeReadError>, PortAdmissionError> {
        let (sender, receipt) = port_completion_channel();
        let head = self.shared.application_head.load(Ordering::Acquire);
        let position = CommitSequence::new(head).map_or(
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough,
        );
        sender.complete(Ok(position));
        Ok(receipt)
    }
}

impl PortCapacityPermit<(), Option<ActiveCatalogSnapshot>, CatalogError> for ActiveCatalogPermit {
    fn submit(
        self: Box<Self>,
        (): (),
    ) -> Result<PortReceipt<Option<ActiveCatalogSnapshot>, CatalogError>, PortAdmissionError> {
        self.shared
            .active_catalog_submissions
            .fetch_add(1, Ordering::AcqRel);
        self.shared
            .discovery_order
            .lock()
            .expect("discovery-order mutex")
            .push("catalog_submit");
        record_operation(&self.shared, "active_catalog");
        let (sender, receipt) = port_completion_channel();
        let active = self
            .shared
            .active_catalog
            .lock()
            .expect("active catalog mutex")
            .clone();
        sender.complete(Ok(Some(active)));
        Ok(receipt)
    }
}

impl
    PortCapacityPermit<
        (ContractLineage, ContractVersion),
        Option<ValidatedContractBundle>,
        CatalogError,
    > for ContractVersionPermit
{
    fn submit(
        self: Box<Self>,
        request: (ContractLineage, ContractVersion),
    ) -> Result<PortReceipt<Option<ValidatedContractBundle>, CatalogError>, PortAdmissionError>
    {
        record_operation(&self.shared, "contract_version");
        let bundle = self
            .shared
            .active_catalog
            .lock()
            .expect("active catalog mutex")
            .bundle()
            .clone();
        let result = (bundle.lineage() == &request.0 && bundle.contract_version() == request.1)
            .then_some(bundle);
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(result));
        Ok(receipt)
    }
}

impl
    PortCapacityPermit<
        AuthoritativeEntityRequest,
        Option<AuthoritativeEntitySnapshot>,
        AuthoritativeReadError,
    > for ReadEntityPermit
{
    fn submit(
        self: Box<Self>,
        _request: AuthoritativeEntityRequest,
    ) -> Result<
        PortReceipt<Option<AuthoritativeEntitySnapshot>, AuthoritativeReadError>,
        PortAdmissionError,
    > {
        record_operation(&self.shared, "entity");
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(None));
        Ok(receipt)
    }
}

impl PortCapacityPermit<AuthoritativeIndexRequest, AuthoritativeIndexPage, AuthoritativeReadError>
    for ScanIndexPermit
{
    fn submit(
        self: Box<Self>,
        request: AuthoritativeIndexRequest,
    ) -> Result<PortReceipt<AuthoritativeIndexPage, AuthoritativeReadError>, PortAdmissionError>
    {
        record_operation(&self.shared, "index");
        self.shared.index_submissions.fetch_add(1, Ordering::AcqRel);
        self.shared
            .index_requests
            .lock()
            .expect("index requests mutex")
            .push(request.clone());
        let configured = self
            .shared
            .index_pages
            .lock()
            .expect("index pages mutex")
            .pop_front();
        let (rows, scanned_through) = configured.unwrap_or((Vec::new(), None));
        let page = AuthoritativeIndexPage::new(
            &request,
            rows,
            scanned_through,
            IndexEpochPosition::Value(IndexEpoch::new(1).expect("index epoch")),
        )
        .expect("configured authoritative index page");
        let hook = self
            .shared
            .index_submission_hook
            .lock()
            .expect("index submission hook mutex")
            .take();
        if let Some(hook) = hook {
            hook();
        }
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(page));
        Ok(receipt)
    }
}

impl
    PortCapacityPermit<
        AuthoritativeCommitScanRequest,
        AuthoritativeCommitPage,
        AuthoritativeReadError,
    > for ScanCommitsPermit
{
    fn submit(
        self: Box<Self>,
        request: AuthoritativeCommitScanRequest,
    ) -> Result<PortReceipt<AuthoritativeCommitPage, AuthoritativeReadError>, PortAdmissionError>
    {
        record_operation(&self.shared, "commit_scan");
        let configured = self
            .shared
            .commit_scan_snapshots
            .lock()
            .expect("commit scan snapshots mutex")
            .clone();
        let upper_sequence = request
            .inclusive_upper()
            .or_else(|| configured.last().map(AuthoritativeCommitSnapshot::sequence));
        let upper = upper_sequence.map_or(
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough,
        );
        let mut eligible = configured.into_iter().filter(|snapshot| {
            request
                .after()
                .is_none_or(|after| snapshot.sequence() > after)
                && upper_sequence.is_none_or(|upper| snapshot.sequence() <= upper)
        });
        let limit = usize::from(request.limit().get().get());
        let snapshots = eligible.by_ref().take(limit).collect::<Vec<_>>();
        let next_after = eligible
            .next()
            .and_then(|_| snapshots.last().map(AuthoritativeCommitSnapshot::sequence));
        let page = AuthoritativeCommitPage::new(request, upper, snapshots, next_after)
            .expect("configured authoritative commit page matches its request and fence");
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(page));
        Ok(receipt)
    }
}

impl
    PortCapacityPermit<
        ProvenanceSelector,
        Option<AuthoritativeProvenanceSnapshot>,
        AuthoritativeReadError,
    > for TraceProvenancePermit
{
    fn submit(
        self: Box<Self>,
        request: ProvenanceSelector,
    ) -> Result<
        PortReceipt<Option<AuthoritativeProvenanceSnapshot>, AuthoritativeReadError>,
        PortAdmissionError,
    > {
        record_operation(&self.shared, "provenance");
        let (sender, receipt) = port_completion_channel();
        let snapshot = self
            .shared
            .provenance_snapshot
            .lock()
            .expect("provenance snapshot mutex")
            .clone()
            .filter(|snapshot| match request {
                ProvenanceSelector::Commit(sequence) => snapshot.commit_sequence() == sequence,
                ProvenanceSelector::Provenance(provenance_id) => {
                    snapshot.provenance_id() == provenance_id
                }
            });
        sender.complete(Ok(snapshot));
        Ok(receipt)
    }
}

impl PortCapacityPermit<ProjectionIdentity, Option<ProjectionStatusSnapshot>, ProjectionPortError>
    for ProjectionStatusPermit
{
    fn submit(
        self: Box<Self>,
        identity: ProjectionIdentity,
    ) -> Result<
        PortReceipt<Option<ProjectionStatusSnapshot>, ProjectionPortError>,
        PortAdmissionError,
    > {
        record_operation(&self.shared, "projection_status");
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(Some(ProjectionStatusSnapshot::uninitialized(
            identity,
            FrontierPosition::BeforeFirst,
        ))));
        Ok(receipt)
    }
}

impl PortCapacityPermit<(), OperationalHealthSnapshot, OperationalStatusError> for HealthPermit {
    fn submit(
        self: Box<Self>,
        (): (),
    ) -> Result<PortReceipt<OperationalHealthSnapshot, OperationalStatusError>, PortAdmissionError>
    {
        record_operation(&self.shared, "health");
        let snapshot = OperationalHealthSnapshot::new(vec![
            ComponentHealth::new(
                HealthComponentKind::AuthoritativeStorage,
                HealthComponentStatus::Healthy,
            ),
            ComponentHealth::new(HealthComponentKind::Catalog, HealthComponentStatus::Healthy),
            ComponentHealth::new(
                HealthComponentKind::CommitCoordinator,
                HealthComponentStatus::Healthy,
            ),
        ])
        .expect("canonical healthy components");
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(snapshot));
        Ok(receipt)
    }
}

impl PortCapacityPermit<(), OperationalStatisticsSnapshot, OperationalStatusError>
    for StatisticsPermit
{
    fn submit(
        self: Box<Self>,
        (): (),
    ) -> Result<
        PortReceipt<OperationalStatisticsSnapshot, OperationalStatusError>,
        PortAdmissionError,
    > {
        record_operation(&self.shared, "statistics");
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(OperationalStatisticsSnapshot::new(None, None, None)));
        Ok(receipt)
    }
}

impl PortCapacityPermit<OutboxStatusRequest, OutboxStatusSnapshot, OutboxStatusPortError>
    for OutboxPermit
{
    fn submit(
        self: Box<Self>,
        request: OutboxStatusRequest,
    ) -> Result<PortReceipt<OutboxStatusSnapshot, OutboxStatusPortError>, PortAdmissionError> {
        record_operation(&self.shared, "outbox");
        let snapshot = OutboxStatusSnapshot::new(request, Vec::new(), None)
            .expect("empty payload-free outbox page");
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(snapshot));
        Ok(receipt)
    }
}

impl PortCapacityPermit<ProjectionPortRequest, ProjectionPortResult, ProjectionPortError>
    for ProjectionQueryPermit
{
    fn submit(
        self: Box<Self>,
        request: ProjectionPortRequest,
    ) -> Result<PortReceipt<ProjectionPortResult, ProjectionPortError>, PortAdmissionError> {
        self.shared
            .projection_submissions
            .fetch_add(1, Ordering::AcqRel);
        self.shared
            .projection_requests
            .lock()
            .expect("projection requests mutex")
            .push(request.clone());
        let observation = self
            .shared
            .projection_observations
            .lock()
            .expect("projection observations mutex")
            .pop_front()
            .expect("one configured projection observation per submission");
        let result = match observation {
            ProjectionObservation::Pending(fence) => {
                Some(ProjectionPortResult::PendingObservation { fence })
            }
            ProjectionObservation::ReadyEmpty(fence) => {
                assert_eq!(fence.identity(), request.identity());
                Some(ProjectionPortResult::Ready(
                    ProjectionPortReady::from_provider(
                        &request,
                        fence.generation(),
                        fence.frontier(),
                        Vec::new(),
                        None,
                    )
                    .expect("empty ready projection observation"),
                ))
            }
            ProjectionObservation::Stalled => None,
        };
        let hook = self
            .shared
            .projection_observation_hook
            .lock()
            .expect("projection observation hook mutex")
            .take();
        if let Some(hook) = hook {
            hook();
        }
        let (sender, receipt) = port_completion_channel();
        if let Some(result) = result {
            sender.complete(Ok(result));
        } else {
            let replaced = self
                .shared
                .pending_projection
                .lock()
                .expect("pending projection mutex")
                .replace(sender);
            assert!(replaced.is_none(), "only one projection receipt may stall");
            self.shared.projection_stalled.notify_one();
        }
        Ok(receipt)
    }
}

impl
    PortCapacityPermit<
        AuthoritativeOutcomeRequest,
        Option<AuthoritativeOutcomeSnapshot>,
        AuthoritativeReadError,
    > for ReadOutcomePermit
{
    fn submit(
        self: Box<Self>,
        request: AuthoritativeOutcomeRequest,
    ) -> Result<
        PortReceipt<Option<AuthoritativeOutcomeSnapshot>, AuthoritativeReadError>,
        PortAdmissionError,
    > {
        self.shared
            .outcome_submissions
            .fetch_add(1, Ordering::AcqRel);
        *self
            .shared
            .last_outcome_request
            .lock()
            .expect("outcome request mutex") = Some(request);
        let response = self
            .shared
            .outcome_response
            .lock()
            .expect("outcome response mutex")
            .clone();
        let hook = self
            .shared
            .outcome_submission_hook
            .lock()
            .expect("outcome submission hook mutex")
            .take();
        if let Some(hook) = hook {
            hook();
        }
        let (sender, receipt) = port_completion_channel();
        sender.complete(response);
        Ok(receipt)
    }
}

impl PortCapacityPermit<CommitSequence, Option<AuthoritativeCommitSnapshot>, AuthoritativeReadError>
    for ReadCommitPermit
{
    fn submit(
        self: Box<Self>,
        _request: CommitSequence,
    ) -> Result<
        PortReceipt<Option<AuthoritativeCommitSnapshot>, AuthoritativeReadError>,
        PortAdmissionError,
    > {
        assert!(
            self.shared
                .commit_continuation_panic
                .compare_exchange(
                    COMMIT_CONTINUATION_PANIC_READ_SUBMIT,
                    COMMIT_CONTINUATION_PANIC_NONE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err(),
            "injected read-commit permit submit panic"
        );
        self.shared.read_submissions.fetch_add(1, Ordering::AcqRel);
        let (sender, receipt) = port_completion_channel();
        if let Some(snapshot) = self
            .shared
            .read_commit_snapshot
            .lock()
            .expect("read commit snapshot mutex")
            .clone()
        {
            sender.complete(Ok(Some(snapshot)));
            return Ok(receipt);
        }
        match self.shared.read_mode {
            ReadCommitMode::ImmediateNotFound => sender.complete(Ok(None)),
            ReadCommitMode::Pending => {
                let replaced = self
                    .shared
                    .pending_read
                    .sender
                    .lock()
                    .expect("pending read sender mutex")
                    .replace(sender);
                assert!(replaced.is_none(), "only one harness read may be pending");
                self.shared.pending_read.submitted.notify_one();
            }
        }
        Ok(receipt)
    }
}

impl
    PortCapacityPermit<
        AuthoritativeCommitSubscriptionRequest,
        Box<dyn CommitNotificationSource>,
        AuthoritativeReadError,
    > for SubscribeCommitPermit
{
    fn submit(
        self: Box<Self>,
        _request: AuthoritativeCommitSubscriptionRequest,
    ) -> Result<
        PortReceipt<Box<dyn CommitNotificationSource>, AuthoritativeReadError>,
        PortAdmissionError,
    > {
        let (sender, receipt) = port_completion_channel();
        if self.shared.subscription_pending.load(Ordering::Acquire) {
            let replaced = self
                .shared
                .pending_subscription
                .lock()
                .expect("pending subscription mutex")
                .replace(sender);
            assert!(replaced.is_none(), "only one subscription may be pending");
            self.shared.subscription_submitted.notify_one();
        } else {
            sender.complete(Ok(subscription_source(&self.shared)));
        }
        Ok(receipt)
    }
}

fn unexpected_port<'a, T, E>(name: &'static str) -> PortFuture<'a, T, E> {
    Box::pin(async move { panic!("unexpected {name} consumer-port call") })
}

impl CatalogReadPort for HarnessPorts {
    fn prepare_active_catalog(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<'_, Option<ActiveCatalogSnapshot>, CatalogError> {
        self.shared
            .prepare_active_calls
            .fetch_add(1, Ordering::AcqRel);
        self.shared
            .capability_order
            .lock()
            .expect("capability-order mutex")
            .push("catalog");
        let shared = Arc::clone(&self.shared);
        Box::pin(async move {
            let observed = shared
                .active_catalog
                .lock()
                .expect("active catalog mutex")
                .clone();
            shared.prepare_active_started.notify_one();
            match shared.prepare_active_mode.load(Ordering::Acquire) {
                CATALOG_READY => Ok(Some(observed)),
                CATALOG_STORAGE_ERROR => Err(CatalogError::new(CatalogErrorKind::Storage)),
                CATALOG_PANIC => panic!("injected active-catalog preparation panic"),
                CATALOG_PENDING => std::future::pending().await,
                CATALOG_ABSENT => Ok(None),
                CATALOG_INTEGRITY_ERROR => Err(CatalogError::new(
                    CatalogErrorKind::InvalidHistoricalEvidence,
                )),
                CATALOG_COMPATIBLE_ACTIVATION_RACE => {
                    shared.compatible_activation_release.notified().await;
                    Ok(Some(observed))
                }
                _ => panic!("invalid active-catalog test mode"),
            }
        })
    }

    fn prepare_contract_version<'a>(
        &'a self,
        _control: &'a RequestControl,
        lineage: ContractLineage,
        version: ContractVersion,
    ) -> PortFuture<'a, Option<ValidatedContractBundle>, CatalogError> {
        self.shared
            .prepare_contract_version_calls
            .fetch_add(1, Ordering::AcqRel);
        let bundle = self
            .shared
            .historical_bundles
            .lock()
            .expect("historical bundles mutex")
            .get(&(lineage, version))
            .cloned();
        Box::pin(async move { Ok(bundle) })
    }

    fn reserve_active_catalog<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<(), Option<ActiveCatalogSnapshot>, CatalogError>,
        PortAdmissionError,
    > {
        self.shared
            .active_catalog_reservations
            .fetch_add(1, Ordering::AcqRel);
        self.shared
            .discovery_order
            .lock()
            .expect("discovery-order mutex")
            .push("catalog_reserve");
        let hook = self
            .shared
            .active_catalog_reservation_hook
            .lock()
            .expect("active-catalog reservation hook mutex")
            .take();
        if let Some(hook) = hook {
            hook();
        }
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ActiveCatalogPermit { shared }) as _) })
    }

    fn reserve_contract_version<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<
            (ContractLineage, ContractVersion),
            Option<ValidatedContractBundle>,
            CatalogError,
        >,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ContractVersionPermit { shared }) as _) })
    }

    fn executable_plan<'a>(
        &'a self,
        _control: &'a RequestControl,
        request: CatalogExecutablePlanRequest,
    ) -> PortFuture<'a, ResolvedExecutablePlan, CatalogError> {
        let panic = self
            .shared
            .panic_executable_plan
            .swap(false, Ordering::AcqRel);
        let request_matches = |resolved: &ResolvedExecutablePlan| {
            let reference = resolved.reference();
            request.lineage() == reference.contract_lineage()
                && request.version() == reference.contract_version()
                && request.bundle_hash() == reference.contract_bundle_hash()
                && request.command_id() == reference.command_id()
                && request.plan_hash() == reference.command_plan_hash()
        };
        let mut matched = request_matches(&self.shared.executable_plan)
            .then(|| self.shared.executable_plan.clone());
        if matched.is_none() {
            matched = self
                .shared
                .additional_plans
                .lock()
                .expect("additional plans mutex")
                .iter()
                .find(|candidate| request_matches(candidate))
                .cloned();
        }
        Box::pin(async move {
            assert!(!panic, "injected executable-plan resolution panic");
            matched.ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))
        })
    }

    fn prepare_deployment<'a>(
        &'a self,
        _control: &'a RequestControl,
        candidate: ContractBundle,
        expected_active_version: Option<ContractVersion>,
    ) -> PortFuture<'a, CatalogPreparationResult, CatalogError> {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move {
            shared
                .prepare_deployment_calls
                .fetch_add(1, Ordering::AcqRel);
            shared.prepare_deployment_started.notify_one();
            match shared.prepare_deployment_mode.load(Ordering::Acquire) {
                CATALOG_READY => {
                    let active = shared.active_catalog.lock().expect("active catalog mutex");
                    prepare_catalog_activation(candidate, expected_active_version, Some(&active))
                }
                CATALOG_STORAGE_ERROR => Err(CatalogError::new(CatalogErrorKind::Storage)),
                CATALOG_PANIC => panic!("injected deployment-preparation panic"),
                CATALOG_PENDING => std::future::pending().await,
                _ => panic!("invalid deployment-preparation test mode"),
            }
        })
    }
}

impl AuthoritativeReadPort for HarnessPorts {
    fn reserve_application_head<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<(), FrontierPosition, AuthoritativeReadError>,
        PortAdmissionError,
    > {
        self.shared
            .application_head_observations
            .fetch_add(1, Ordering::AcqRel);
        self.shared
            .capability_order
            .lock()
            .expect("capability-order mutex")
            .push("application_head");
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ApplicationHeadPermit { shared }) as _) })
    }

    fn reserve_read_entity<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<
            AuthoritativeEntityRequest,
            Option<AuthoritativeEntitySnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ReadEntityPermit { shared }) as _) })
    }

    fn reserve_scan_index<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<
            AuthoritativeIndexRequest,
            AuthoritativeIndexPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ScanIndexPermit { shared }) as _) })
    }

    fn reserve_read_outcome<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<
            AuthoritativeOutcomeRequest,
            Option<AuthoritativeOutcomeSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        self.shared
            .outcome_reservations
            .fetch_add(1, Ordering::AcqRel);
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ReadOutcomePermit { shared }) as _) })
    }

    fn reserve_read_commit<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<
            CommitSequence,
            Option<AuthoritativeCommitSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        self.shared.read_reservations.fetch_add(1, Ordering::AcqRel);
        let hook = self
            .shared
            .read_reservation_hook
            .lock()
            .expect("read reservation hook mutex")
            .take();
        if let Some(hook) = hook {
            hook();
        }
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ReadCommitPermit { shared }) as _) })
    }

    fn reserve_scan_commits<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<
            AuthoritativeCommitScanRequest,
            AuthoritativeCommitPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ScanCommitsPermit { shared }) as _) })
    }

    fn reserve_subscribe_to_commits<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<
            AuthoritativeCommitSubscriptionRequest,
            Box<dyn CommitNotificationSource>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(SubscribeCommitPermit { shared }) as _) })
    }

    fn reserve_trace_provenance<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<
            riffdb_policy::ProvenanceSelector,
            Option<AuthoritativeProvenanceSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(TraceProvenancePermit { shared }) as _) })
    }

    fn read_capability_revoke_target<'a>(
        &'a self,
        _control: &'a RequestControl,
        capability_id: riffdb_types::CapabilityId,
    ) -> PortFuture<'a, CapabilityRevokeTargetSnapshot, AuthoritativeReadError> {
        let panic = self
            .shared
            .panic_revoke_target_read
            .swap(false, Ordering::AcqRel);
        Box::pin(async move {
            assert!(!panic, "injected revoke-target read panic");
            Ok(CapabilityRevokeTargetSnapshot::Absent(
                AbsentCapabilityRevokeTargetSnapshot::new(
                    capability_id,
                    database_id(),
                    environment(),
                ),
            ))
        })
    }
}

impl ProjectionQueryPort for HarnessPorts {
    fn reserve_query_projection(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        riffdb_service::BoxPortCapacityPermit<
            ProjectionPortRequest,
            ProjectionPortResult,
            ProjectionPortError,
        >,
        PortAdmissionError,
    > {
        self.shared
            .projection_reservations
            .fetch_add(1, Ordering::AcqRel);
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ProjectionQueryPermit { shared }) as _) })
    }

    fn reserve_projection_status(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        riffdb_service::BoxPortCapacityPermit<
            riffdb_types::ProjectionIdentity,
            Option<ProjectionStatusSnapshot>,
            ProjectionPortError,
        >,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ProjectionStatusPermit { shared }) as _) })
    }
}

impl OperationalStatusPort for HarnessPorts {
    fn reserve_health(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        riffdb_service::BoxPortCapacityPermit<
            (),
            OperationalHealthSnapshot,
            OperationalStatusError,
        >,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(HealthPermit { shared }) as _) })
    }

    fn reserve_statistics(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        riffdb_service::BoxPortCapacityPermit<
            (),
            OperationalStatisticsSnapshot,
            OperationalStatusError,
        >,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(StatisticsPermit { shared }) as _) })
    }
}

impl CapabilityTokenIssuer for HarnessPorts {
    fn issue(&self) -> Result<NewlyIssuedCapabilityToken, CapabilityTokenIssueError> {
        self.shared.token_issue_calls.fetch_add(1, Ordering::AcqRel);
        self.shared
            .capability_order
            .lock()
            .expect("capability-order mutex")
            .push("token");
        Err(CapabilityTokenIssueError::Unavailable)
    }
}

impl OutboxStatusPort for HarnessPorts {
    fn reserve_pending_status(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        riffdb_service::BoxPortCapacityPermit<
            riffdb_service::OutboxStatusRequest,
            riffdb_service::OutboxStatusSnapshot,
            riffdb_service::OutboxStatusPortError,
        >,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(OutboxPermit { shared }) as _) })
    }
}

struct TokioSpawner;

impl ServiceJobSpawner for TokioSpawner {
    fn spawn(&self, job: ServiceJob) {
        let _task = tokio::spawn(job);
    }
}

/// Longest distance from now at which a registered wait can still be a bounded
/// pre-admission wait rather than a request deadline.
///
/// Production caps the absolute admission window at 150 ms
/// (`COMMAND_ADMISSION_MAX_WAIT`), while every harness request deadline is at
/// least a second away. A registration inside this horizon is therefore an
/// admission wait, which is the observable that separates the pre-admission
/// queue-delay shed (returns before any admission wait exists) from a
/// fallthrough that parks on the cap.
const ADMISSION_WAIT_HORIZON: Duration = Duration::from_secs(1);

#[derive(Default)]
struct HarnessDeadlineScheduler {
    admission_wait_registrations: AtomicUsize,
    control_request_deadline: AtomicBool,
    request_waiters: AtomicUsize,
    request_deadline: Notify,
    request_registered: Notify,
    control_projection_deadline: AtomicBool,
    projection_waiters: AtomicUsize,
    projection_deadline: Notify,
    projection_registered: Notify,
    controlled_stream_lifetime_waits: AtomicU8,
    stream_lifetime_waiters: AtomicUsize,
    stream_lifetime: Notify,
}

impl HarnessDeadlineScheduler {
    /// Waits registered inside [`ADMISSION_WAIT_HORIZON`] since start.
    fn admission_wait_registrations(&self) -> usize {
        self.admission_wait_registrations.load(Ordering::Acquire)
    }

    fn control_request_deadline(&self) {
        self.control_request_deadline.store(true, Ordering::Release);
    }

    async fn wait_for_request_waiter(&self) {
        loop {
            if self.request_waiters.load(Ordering::Acquire) != 0 {
                return;
            }
            self.request_registered.notified().await;
        }
    }

    fn request_deadline_is_waiting(&self) -> bool {
        self.request_waiters.load(Ordering::Acquire) != 0
    }

    fn elapse_request_deadline(&self) {
        self.request_deadline.notify_waiters();
    }

    fn control_projection_deadline(&self) {
        self.control_projection_deadline
            .store(true, Ordering::Release);
    }

    async fn wait_for_projection_waiters(&self, expected: usize) {
        loop {
            if self.projection_waiters.load(Ordering::Acquire) >= expected {
                return;
            }
            self.projection_registered.notified().await;
        }
    }

    fn elapse_projection_deadline(&self) {
        self.projection_deadline.notify_waiters();
    }

    fn projection_waiters(&self) -> usize {
        self.projection_waiters.load(Ordering::Acquire)
    }

    fn control_stream_lifetime(&self) {
        let prior = self
            .controlled_stream_lifetime_waits
            .swap(2, Ordering::AcqRel);
        assert_eq!(prior, 0, "only one stream lifetime wait may be controlled");
    }

    fn stream_lifetime_is_waiting(&self) -> bool {
        self.stream_lifetime_waiters.load(Ordering::Acquire) != 0
    }

    fn elapse_stream_lifetime(&self) {
        self.stream_lifetime.notify_waiters();
    }
}

impl RequestDeadlineScheduler for HarnessDeadlineScheduler {
    fn wait_until(&self, deadline: Instant) -> RequestDeadlineFuture<'_> {
        if deadline.saturating_duration_since(Instant::now()) <= ADMISSION_WAIT_HORIZON {
            self.admission_wait_registrations
                .fetch_add(1, Ordering::AcqRel);
        }
        // Lost-wakeup hazard closed by ORDER: the waiter counters are what
        // the tests' barriers observe, and `Notify::notify_waiters` only
        // reaches `Notified` futures that already EXIST (tokio's creation
        // guarantee; it stores no permit). The old order incremented the
        // counter BEFORE creating the future, so on the multi-thread test
        // runtime a barrier could observe the count and fire the elapse
        // inside the create window, losing the wakeup (deterministically
        // reproducible by widening that window). Creation — plus `enable()`
        // as an explicit enrolment — now happens-before the increment, so an
        // observed counter implies a reachable waiter.
        if self.control_request_deadline.swap(false, Ordering::AcqRel) {
            let mut notified = Box::pin(self.request_deadline.notified());
            notified.as_mut().enable();
            self.request_waiters.fetch_add(1, Ordering::AcqRel);
            self.request_registered.notify_waiters();
            return notified;
        }
        if self
            .controlled_stream_lifetime_waits
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok_and(|remaining| remaining == 1)
        {
            let mut notified = Box::pin(self.stream_lifetime.notified());
            notified.as_mut().enable();
            self.stream_lifetime_waiters.fetch_add(1, Ordering::AcqRel);
            return notified;
        }
        if self.control_projection_deadline.load(Ordering::Acquire)
            && deadline.saturating_duration_since(Instant::now()) < Duration::from_secs(10)
        {
            let mut notified = Box::pin(self.projection_deadline.notified());
            notified.as_mut().enable();
            self.projection_waiters.fetch_add(1, Ordering::AcqRel);
            self.projection_registered.notify_waiters();
            return notified;
        }
        Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
            deadline,
        )))
    }
}

#[derive(Default)]
struct SequentialCursorTokens(AtomicU64);

impl SequentialCursorTokens {
    fn calls(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

impl CursorTokenGenerator for SequentialCursorTokens {
    fn fill_cursor_token(
        &self,
        destination: &mut [u8; riffdb_service::CURSOR_TOKEN_BYTES],
    ) -> Result<(), CursorTokenGenerationError> {
        let value = self.0.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        destination.fill(0);
        destination[..8].copy_from_slice(&value.to_be_bytes());
        Ok(())
    }
}

struct FixedCursorClock;

impl CursorMonotonicClock for FixedCursorClock {
    fn now(&self) -> Result<CursorTick, CursorClockError> {
        CursorTick::from_process_elapsed(Duration::ZERO).map_err(|_| CursorClockError)
    }
}

struct HarnessIncidentIds {
    next: AtomicU64,
    fail: bool,
}

impl HarnessIncidentIds {
    fn new(fail: bool) -> Self {
        Self {
            next: AtomicU64::new(0),
            fail,
        }
    }
}

impl IncidentIdSource for HarnessIncidentIds {
    fn next_incident_id(&self) -> Result<IncidentId, IncidentIdSourceError> {
        if self.fail {
            return Err(IncidentIdSourceError);
        }
        let seed = self.next.fetch_add(1, Ordering::Relaxed) as u8;
        IncidentId::from_bytes(uuid_bytes(seed.wrapping_add(0x40)))
            .map_err(|_| IncidentIdSourceError)
    }
}

struct HarnessDiagnostics;

impl ServiceDiagnostics for HarnessDiagnostics {
    fn record_internal(&self, _error: InternalError) {}
}

#[derive(Default)]
pub(crate) struct HarnessTelemetry(Mutex<Vec<ServiceTelemetryEvent>>);

impl HarnessTelemetry {
    pub(crate) fn events(&self) -> Vec<ServiceTelemetryEvent> {
        self.0.lock().expect("telemetry mutex").clone()
    }
}

impl ServiceTelemetry for HarnessTelemetry {
    fn record(&self, event: ServiceTelemetryEvent) {
        self.0.lock().expect("telemetry mutex").push(event);
    }
}

/// Fixed active module for named symbolic-query harness tests.
pub(crate) struct FixedQueryModulePort {
    module: ValidatedQueryModule,
}

impl FixedQueryModulePort {
    pub(crate) fn new(module: ValidatedQueryModule) -> Self {
        Self { module }
    }
}

impl QueryModuleReadPort for FixedQueryModulePort {
    fn prepare_active_query_module<'a>(
        &'a self,
        _control: &'a RequestControl,
        _contract: ValidatedContractBundle,
    ) -> PortFuture<'a, Option<ValidatedQueryModule>, QueryModuleReadError> {
        let module = self.module.clone();
        Box::pin(async move { Ok(Some(module)) })
    }

    fn prepare_query_module<'a>(
        &'a self,
        _control: &'a RequestControl,
        _contract: ValidatedContractBundle,
        module_hash: riffdb_types::QueryModuleHash,
    ) -> PortFuture<'a, Option<ValidatedQueryModule>, QueryModuleReadError> {
        let module = (self.module.identity() == module_hash).then(|| self.module.clone());
        Box::pin(async move { Ok(module) })
    }
}

/// Fixed content-addressed reactive module for live-query harness tests.
pub(crate) struct FixedReactiveModulePort {
    module: ValidatedReactiveModule,
}

impl FixedReactiveModulePort {
    pub(crate) fn new(module: ValidatedReactiveModule) -> Self {
        Self { module }
    }
}

impl ReactiveModuleReadPort for FixedReactiveModulePort {
    fn prepare_reactive_module<'a>(
        &'a self,
        _control: &'a RequestControl,
        _contract: ValidatedContractBundle,
        module_hash: riffdb_types::ReactiveModuleHash,
    ) -> PortFuture<'a, Option<ValidatedReactiveModule>, ReactiveModuleReadError> {
        let module = (self.module.identity() == module_hash).then(|| self.module.clone());
        Box::pin(async move { Ok(module) })
    }
}

/// Stable wall clock for cursor-expiry evidence.
pub(crate) struct FixedLiveQueryClock;

impl LiveQueryClock for FixedLiveQueryClock {
    fn now(&self) -> Result<Timestamp, riffdb_service::LiveQueryClockError> {
        Ok(timestamp(BASE_SECONDS + 20))
    }
}

/// Empty read view that returns absence for every point/scan access.
pub(crate) struct EmptyQueryExecutor;

/// Read view proving that a bounded top-N result remains complete when its
/// engine scan reports that later pages exist.
#[derive(Default)]
pub(crate) struct ContinuedEmptyQueryExecutor {
    calls: std::sync::atomic::AtomicU64,
}

impl riffdb_query_executor::QueryExecutionPort for ContinuedEmptyQueryExecutor {
    fn execute_query_page(
        &self,
        program: &riffdb_query_ir::QueryAccessProgramV1,
        parameters: &riffdb_query_executor::QueryParameters,
        prior: Option<&riffdb_query_executor::QueryContinuation>,
    ) -> Result<riffdb_query_executor::QueryOwnedSnapshot, riffdb_query_executor::QueryExecutionError>
    {
        struct ContinuedEmptyView {
            application_head: u64,
        }
        impl riffdb_query_executor::QueryReadView for ContinuedEmptyView {
            type Error = ();

            fn fault(&self, _error: &Self::Error) -> riffdb_query_executor::QueryBackendFault {
                riffdb_query_executor::QueryBackendFault::Unavailable
            }

            fn application_head(&self) -> u64 {
                self.application_head
            }

            fn point(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<Option<riffdb_query_executor::QueryRow>, Self::Error> {
                Ok(None)
            }

            fn dependent_point_batch(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                predicates: &[Vec<riffdb_query_executor::BoundPredicate>],
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<Vec<Option<riffdb_query_executor::QueryRow>>, Self::Error> {
                Ok(vec![None; predicates.len()])
            }

            fn scan(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _limit: u64,
                _after: Option<&[u8]>,
                _after_inclusive: bool,
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<riffdb_query_executor::QueryScanPage, Self::Error> {
                Ok(riffdb_query_executor::QueryScanPage::continued(
                    Vec::new(),
                    1,
                    1,
                    b"more".to_vec(),
                )
                .expect("bounded continuation"))
            }

            fn nearest(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _k: u32,
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<riffdb_query_executor::QueryNearestPage, Self::Error> {
                Ok(riffdb_query_executor::QueryNearestPage {
                    rows: Vec::new(),
                    scanned_rows: 0,
                })
            }
        }
        let application_head = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        riffdb_query_executor::execute_page_in_snapshot(
            program,
            parameters,
            prior,
            &mut ContinuedEmptyView { application_head },
        )
    }

    fn execute_query_group(
        &self,
        _requests: &[riffdb_query_executor::QueryExecutionRequest<'_>],
    ) -> Result<
        Vec<riffdb_query_executor::QueryOwnedSnapshot>,
        riffdb_query_executor::QueryExecutionError,
    > {
        Err(riffdb_query_executor::QueryExecutionError::InvalidProgram)
    }
}

impl riffdb_query_executor::QueryExecutionPort for EmptyQueryExecutor {
    fn execute_query_page(
        &self,
        program: &riffdb_query_ir::QueryAccessProgramV1,
        parameters: &riffdb_query_executor::QueryParameters,
        prior: Option<&riffdb_query_executor::QueryContinuation>,
    ) -> Result<riffdb_query_executor::QueryOwnedSnapshot, riffdb_query_executor::QueryExecutionError>
    {
        struct EmptyView;
        impl riffdb_query_executor::QueryReadView for EmptyView {
            type Error = ();

            fn fault(&self, _error: &Self::Error) -> riffdb_query_executor::QueryBackendFault {
                riffdb_query_executor::QueryBackendFault::Unavailable
            }

            fn application_head(&self) -> u64 {
                0
            }

            fn point(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<Option<riffdb_query_executor::QueryRow>, Self::Error> {
                Ok(None)
            }

            fn dependent_point_batch(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                predicates: &[Vec<riffdb_query_executor::BoundPredicate>],
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<Vec<Option<riffdb_query_executor::QueryRow>>, Self::Error> {
                Ok(vec![None; predicates.len()])
            }

            fn scan(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _limit: u64,
                _after: Option<&[u8]>,
                _after_inclusive: bool,
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<riffdb_query_executor::QueryScanPage, Self::Error> {
                Ok(riffdb_query_executor::QueryScanPage::exact_end(
                    Vec::new(),
                    0,
                ))
            }

            fn nearest(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _k: u32,
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<riffdb_query_executor::QueryNearestPage, Self::Error> {
                Ok(riffdb_query_executor::QueryNearestPage {
                    rows: Vec::new(),
                    scanned_rows: 0,
                })
            }
        }
        riffdb_query_executor::execute_page_in_snapshot(program, parameters, prior, &mut EmptyView)
    }

    fn execute_query_group(
        &self,
        requests: &[riffdb_query_executor::QueryExecutionRequest<'_>],
    ) -> Result<
        Vec<riffdb_query_executor::QueryOwnedSnapshot>,
        riffdb_query_executor::QueryExecutionError,
    > {
        struct EmptyView;
        impl riffdb_query_executor::QueryReadView for EmptyView {
            type Error = ();

            fn fault(&self, _error: &Self::Error) -> riffdb_query_executor::QueryBackendFault {
                riffdb_query_executor::QueryBackendFault::Unavailable
            }

            fn application_head(&self) -> u64 {
                0
            }

            fn point(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<Option<riffdb_query_executor::QueryRow>, Self::Error> {
                Ok(None)
            }

            fn dependent_point_batch(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                predicates: &[Vec<riffdb_query_executor::BoundPredicate>],
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<Vec<Option<riffdb_query_executor::QueryRow>>, Self::Error> {
                Ok(vec![None; predicates.len()])
            }

            fn scan(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _limit: u64,
                _after: Option<&[u8]>,
                _after_inclusive: bool,
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<riffdb_query_executor::QueryScanPage, Self::Error> {
                Ok(riffdb_query_executor::QueryScanPage::exact_end(
                    Vec::new(),
                    0,
                ))
            }

            fn nearest(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _k: u32,
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<riffdb_query_executor::QueryNearestPage, Self::Error> {
                Ok(riffdb_query_executor::QueryNearestPage {
                    rows: Vec::new(),
                    scanned_rows: 0,
                })
            }
        }

        riffdb_query_executor::validate_query_execution_group(requests)?;
        let mut view = EmptyView;
        requests
            .iter()
            .map(|request| {
                riffdb_query_executor::execute_in_snapshot(
                    request.program(),
                    request.parameters(),
                    &mut view,
                )
            })
            .collect()
    }

    fn execute_operational_query_page(
        &self,
        program: &riffdb_query_ir::QueryAccessProgramV1,
        aggregates: &[riffdb_query_ir::OperationalAggregateV1],
        parameters: &riffdb_query_executor::QueryParameters,
        prior: Option<&riffdb_query_executor::QueryContinuation>,
    ) -> Result<riffdb_query_executor::QueryOwnedSnapshot, riffdb_query_executor::QueryExecutionError>
    {
        struct EmptyAggregateView;
        impl riffdb_query_executor::QueryReadView for EmptyAggregateView {
            type Error = ();

            fn fault(&self, _error: &Self::Error) -> riffdb_query_executor::QueryBackendFault {
                riffdb_query_executor::QueryBackendFault::Unavailable
            }

            fn application_head(&self) -> u64 {
                0
            }

            fn point(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<Option<riffdb_query_executor::QueryRow>, Self::Error> {
                Ok(None)
            }

            fn dependent_point_batch(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                predicates: &[Vec<riffdb_query_executor::BoundPredicate>],
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<Vec<Option<riffdb_query_executor::QueryRow>>, Self::Error> {
                Ok(vec![None; predicates.len()])
            }

            fn scan(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _limit: u64,
                _after: Option<&[u8]>,
                _after_inclusive: bool,
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<riffdb_query_executor::QueryScanPage, Self::Error> {
                Ok(riffdb_query_executor::QueryScanPage::exact_end(
                    Vec::new(),
                    0,
                ))
            }

            fn nearest(
                &mut self,
                _step: &riffdb_query_ir::QueryAccessStep,
                _predicates: &[riffdb_query_executor::BoundPredicate],
                _k: u32,
                _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
            ) -> Result<riffdb_query_executor::QueryNearestPage, Self::Error> {
                Ok(riffdb_query_executor::QueryNearestPage {
                    rows: Vec::new(),
                    scanned_rows: 0,
                })
            }
        }
        riffdb_query_executor::execute_operational_page_in_snapshot(
            program,
            aggregates,
            parameters,
            prior,
            &mut EmptyAggregateView,
        )
    }
}

#[derive(Default)]
pub(crate) struct HarnessHealth(Mutex<Vec<AuthoritativeReadinessFailure>>);

impl HarnessHealth {
    pub(crate) fn failures(&self) -> Vec<AuthoritativeReadinessFailure> {
        self.0.lock().expect("health mutex").clone()
    }
}

impl ServiceHealthHooks for HarnessHealth {
    fn fail_authoritative_readiness(&self, reason: AuthoritativeReadinessFailure) {
        self.0.lock().expect("health mutex").push(reason);
    }
}

fn build_command_input(plan: &CommandPlan, organization_id: [u8; 16]) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string(COMMAND_CALLER_KEY).expect("bounded idempotency key"),
            ),
            ("organization_id", CanonicalValue::Uuid(organization_id)),
            ("fiscal_year", CanonicalValue::I64(FISCAL_YEAR)),
            ("approved_amount", decimal(10_000)),
        ],
    )
}

fn input_record<const N: usize>(
    schema: &RecordSchema,
    fields: [(&str, CanonicalValue); N],
) -> CanonicalRecord {
    let by_name = fields.into_iter().collect::<BTreeMap<_, _>>();
    CanonicalRecord::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                (
                    field.id(),
                    by_name
                        .get(field.name())
                        .unwrap_or_else(|| panic!("missing input field {}", field.name()))
                        .clone(),
                )
            })
            .collect(),
    )
    .expect("canonical command input")
}

fn decimal(coefficient: i128) -> CanonicalValue {
    CanonicalValue::Decimal(
        Decimal::new(
            DecimalSpec::new(28, 2).expect("budget decimal spec"),
            coefficient,
        )
        .expect("budget decimal value"),
    )
}

fn catalog_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("service-harness-catalog-owner").expect("bounded catalog principal"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(0x62)).expect("catalog capability UUIDv7"),
        NonZeroU64::MIN,
    )
}

pub(super) fn database_id() -> DatabaseId {
    DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database UUIDv7")
}

fn environment() -> Environment {
    Environment::new("test").expect("bounded environment")
}

fn audience() -> Audience {
    Audience::new("riffdb-service-tests").expect("bounded audience")
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("canonical timestamp")
}

fn uuid_bytes(seed: u8) -> [u8; 16] {
    let mut bytes = [seed; 16];
    bytes[6] = 0x70 | (seed & 0x0f);
    bytes[8] = 0x80 | (seed & 0x3f);
    bytes
}

// ---------------------------------------------------------------------------
// CP2b board acceptance harness: real ticketdesk-shaped contract, real
// coordinator/storage, real columnar apply, real policy. No mocks on the
// write path, the apply path, the symbolic read path, or revocation.
// ---------------------------------------------------------------------------

/// Ticketdesk board shape replicated for the service acceptance spine
/// (`examples/` is out of tree for tests; the CP2a precedent replicated the
/// same key shape as `BOARD_KEY_CONTRACT`). `Ticket` keeps the REAL board key
/// `(organization_id: uuid, ticket_id: uuid)`, the real board fields, the
/// real `TicketStatus` enum, and the real `by_project_status` index the
/// compiled board_page queries plan against. `Note` is the interleaved
/// irrelevant entity.
const TICKETDESK_BOARD_SOURCE: &str = r#"
contract TicketDeskBoard version 1 {
  enum TicketStatus { Open, InProgress, Closed }

  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field project_id: uuid
    field reporter_id: uuid
    field assignee_id: uuid
    field status: TicketStatus
    field title: string<128>
    field story_points: i64
    field cost: money<USD>
    index by_project_status (organization_id, project_id, status, ticket_id)
  }

  entity Note {
    key (organization_id: uuid, note_id: uuid)
    field body: string<128>
  }

  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }

  aggregate Notes {
    root Note
    partition_by organization_id
    conflict_key (organization_id, note_id)
  }

  command CreateTicket {
    input idempotency_key: string<128>
    input organization_id: uuid
    input ticket_id: uuid
    input project_id: uuid
    input reporter_id: uuid
    input assignee_id: uuid
    input status: TicketStatus
    input title: string<128>
    input story_points: i64
    input cost: money<USD>
    idempotency_key idempotency_key
    create Ticket(organization_id, ticket_id) as ticket
      else TicketExists { ticket_id: ticket_id }
    set ticket.project_id = project_id
    set ticket.reporter_id = reporter_id
    set ticket.assignee_id = assignee_id
    set ticket.status = status
    set ticket.title = title
    set ticket.story_points = story_points
    set ticket.cost = cost
    return Created { ticket: ticket }
  }

  command CreateNote {
    input idempotency_key: string<128>
    input organization_id: uuid
    input note_id: uuid
    input body: string<128>
    idempotency_key idempotency_key
    create Note(organization_id, note_id) as note
      else NoteExists { note_id: note_id }
    set note.body = body
    return NoteCreated { note: note }
  }
}
"#;

/// The compiled-path query: the exact `board_page_50.riffq` shape over the
/// replicated contract (parameters typed against `Ticket` because the trimmed
/// fixture has no separate `Organization`/`Project` entities).
const BOARD_PAGE_QUERY: &str = r#"
query BoardPage(
    $organization_id: Ticket.organization_id,
    $project_id: Ticket.project_id,
    $status: TicketStatus,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take 50

    return Found {
        tickets: tickets {
            ticket_id
            project_id
            title
            status
            reporter_id
            assignee_id
        }
    }

    outcomes Found
}
"#;

pub(crate) const BOARD_PROJECTION_NAME: &str = "board";
pub(crate) const BOARD_HISTORY_INCARNATION: u64 = 1;
/// Board select order used by the projected path (ticket_id arrives via PK).
pub(crate) const BOARD_SELECT: [&str; 5] = [
    "project_id",
    "title",
    "status",
    "reporter_id",
    "assignee_id",
];

/// Resolves one field id on a board-contract entity by source name.
pub(crate) fn board_field_id(bundle: &ContractBundle, entity: &str, field: &str) -> FieldId {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|candidate| candidate.name() == entity)
        .expect("board entity");
    entity
        .record()
        .fields()
        .iter()
        .find(|candidate| candidate.name() == field)
        .map(riffdb_contract_ir::FieldSchema::id)
        .expect("board field")
}

/// Canonical `TicketStatus` enum value for one variant source name.
pub(crate) fn ticket_status_value(bundle: &ContractBundle, variant: &str) -> CanonicalValue {
    let ticket_status = bundle
        .schema()
        .enums()
        .iter()
        .find(|candidate| candidate.name() == "TicketStatus")
        .expect("TicketStatus enum");
    let variant = ticket_status
        .variants()
        .iter()
        .find(|candidate| candidate.name() == variant)
        .expect("TicketStatus variant");
    CanonicalValue::Enum {
        type_id: ticket_status.id(),
        variant_id: variant.id(),
    }
}

fn board_ticket_input(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    organization_id: [u8; 16],
) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string(COMMAND_CALLER_KEY).expect("bounded idempotency key"),
            ),
            ("organization_id", CanonicalValue::Uuid(organization_id)),
            ("ticket_id", CanonicalValue::Uuid(uuid_bytes(0x9c))),
            ("project_id", CanonicalValue::Uuid(uuid_bytes(0x51))),
            ("reporter_id", CanonicalValue::Uuid(uuid_bytes(0x52))),
            ("assignee_id", CanonicalValue::Uuid(uuid_bytes(0x53))),
            ("status", ticket_status_value(bundle, "Open")),
            (
                "title",
                CanonicalValue::string("seed").expect("bounded title"),
            ),
            ("story_points", CanonicalValue::I64(0)),
            ("cost", ServiceHarness::board_cost(0)),
        ],
    )
}

fn board_projection_directory() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target")
        .join("service-harness");
    std::fs::create_dir_all(&root).expect("create service harness directory");
    let path = root.join(format!(
        "riffdb-service-board-{}-{}",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create board projection directory");
    path
}

impl AuditDatabase {
    /// Board-contract twin of [`AuditDatabase::create`]; also resolves the
    /// `CreateNote` plan so irrelevant-entity commands run through the same
    /// real coordinator.
    fn create_board() -> (Self, ResolvedExecutablePlan) {
        let path = next_database_path();
        let mut store = RedbStore::open(&path).expect("create board harness database");
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize board harness database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        let source = TICKETDESK_BOARD_SOURCE.to_owned();
        let checked_bundle = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(&source).expect("compile board contract"),
        )
        .expect("validate board contract");
        let mut ports = open_operational(store);
        let stored_bundle = checked_bundle.to_stored().expect("encode board bundle");
        let activation = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                stored_bundle.clone(),
                request_id(0x67),
                catalog_principal(),
                timestamp(BASE_SECONDS + 1),
                None,
            ))
            .expect("activate board catalog");
        assert!(matches!(
            activation,
            CatalogActivationResult::Activated { active, .. }
                if active == ActiveCatalogPointerV1::from_bundle(&stored_bundle)
        ));
        let active_catalog = ActiveCatalogSnapshot::read(&ports)
            .expect("read active board catalog")
            .expect("board catalog is active");
        let resolve = |name: &str, ports: &RedbOperationalPorts| {
            let command = active_catalog
                .bundle()
                .bundle()
                .commands()
                .iter()
                .find(|plan| plan.name() == name)
                .unwrap_or_else(|| panic!("{name} command"));
            let reference = ExecutablePlanRef::new(
                checked_bundle.lineage().clone(),
                checked_bundle.contract_version(),
                checked_bundle.bundle_hash(),
                command.command_id(),
                command.plan_hash(),
            );
            resolve_executable_plan(ports, &reference)
                .unwrap_or_else(|_| panic!("resolve {name} plan"))
        };
        let executable_plan = resolve("CreateTicket", &ports);
        let note_plan = resolve("CreateNote", &ports);
        let command = active_catalog
            .bundle()
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == "CreateTicket")
            .expect("CreateTicket command");
        let bundle = active_catalog.bundle().bundle();
        let command_input = board_ticket_input(bundle, command, ORGANIZATION_ID);
        let partition = derive_input_command_facts(command, command_input.clone())
            .expect("derive board partition")
            .partition_key()
            .clone();
        let alternate_partition = derive_input_command_facts(
            command,
            board_ticket_input(bundle, command, ALTERNATE_ORGANIZATION_ID),
        )
        .expect("derive alternate board partition")
        .partition_key()
        .clone();
        drop(ports);
        (
            Self {
                path,
                source,
                active_catalog,
                executable_plan,
                command_input,
                partition,
                alternate_partition,
            },
            note_plan,
        )
    }
}

/// Real columnar runtime for the board harness: one real [`ColumnarEngine`]
/// applying from the SAME live redb database the coordinator commits to, a
/// real [`ColumnarNotifier`], and a dedicated apply thread mirroring the
/// server worker pass (apply, publish, notify-on-publication).
pub(crate) struct HarnessColumnar {
    engine: Arc<Mutex<ColumnarEngine>>,
    definition: RegisteredDefinition,
    notifier: ColumnarNotifier,
    names: Vec<String>,
    shared: RedbSharedPorts,
    directory: PathBuf,
    apply_enabled: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    last_apply_error: Arc<Mutex<Option<String>>>,
}

impl HarnessColumnar {
    fn start(bundle: &ContractBundle, shared: RedbSharedPorts) -> Arc<Self> {
        let ticket = |field: &str| board_field_id(bundle, "Ticket", field);
        let definition = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: BOARD_PROJECTION_NAME.into(),
                entity_name: "Ticket".into(),
                projected_fields: vec![
                    ticket("project_id"),
                    ticket("reporter_id"),
                    ticket("assignee_id"),
                    ticket("status"),
                    ticket("title"),
                    // Summable integer column and a deliberately unsummable
                    // money column for the aggregate acceptance arc.
                    ticket("story_points"),
                    ticket("cost"),
                ],
                org_scope_field: ticket("organization_id"),
            },
            bundle,
        )
        .expect("register board projection");
        let directory = board_projection_directory();
        let engine = ColumnarEngine::open(
            definition.clone(),
            ColumnarOpenOptions::new(directory.clone())
                .with_history_incarnation(BOARD_HISTORY_INCARNATION),
        )
        .expect("open board columnar engine");
        let engine = Arc::new(Mutex::new(engine));
        let notifier = ColumnarNotifier::from_names([BOARD_PROJECTION_NAME.to_owned()]);
        let apply_enabled = Arc::new(AtomicBool::new(true));
        let stop = Arc::new(AtomicBool::new(false));
        let last_apply_error = Arc::new(Mutex::new(None));
        let worker = {
            let engine = Arc::clone(&engine);
            let shared = shared.clone();
            let notifier = notifier.clone();
            let apply_enabled = Arc::clone(&apply_enabled);
            let stop = Arc::clone(&stop);
            let last_apply_error = Arc::clone(&last_apply_error);
            std::thread::Builder::new()
                .name("service-harness-columnar".to_owned())
                .spawn(move || {
                    while !stop.load(Ordering::Acquire) {
                        if apply_enabled.load(Ordering::Acquire) {
                            let published_changed = {
                                let mut engine = engine.lock().expect("board engine mutex");
                                let before = engine.published_frontier_position();
                                if let Err(error) = engine.apply_available(&shared) {
                                    *last_apply_error.lock().expect("apply error mutex") =
                                        Some(format!("{error:?}"));
                                }
                                engine.published_frontier_position() != before
                            };
                            // Engine lock released before notifying waiters.
                            if published_changed {
                                let _ = notifier.notify(BOARD_PROJECTION_NAME);
                            }
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                })
                .expect("spawn board columnar worker")
        };
        Arc::new(Self {
            engine,
            definition,
            notifier,
            names: vec![BOARD_PROJECTION_NAME.to_owned()],
            shared,
            directory,
            apply_enabled,
            stop,
            worker: Mutex::new(Some(worker)),
            last_apply_error,
        })
    }

    fn read_head(&self) -> Result<FrontierPosition, ColumnarPortError> {
        let limit = StorageScanLimit::new(1).ok_or(ColumnarPortError::Integrity)?;
        let page =
            AuthoritativeScanReader::scan_commits(&self.shared, CommitScanRequest::initial(limit))
                .map_err(|_| ColumnarPortError::Unavailable)?;
        Ok(page.inclusive_upper())
    }

    /// Pauses/resumes the real apply thread (freshness matrix control).
    pub(crate) fn set_apply_enabled(&self, enabled: bool) {
        self.apply_enabled.store(enabled, Ordering::Release);
    }

    /// Current published frontier position of the board engine.
    pub(crate) fn published_position(&self) -> FrontierPosition {
        self.engine
            .lock()
            .expect("board engine mutex")
            .published_frontier_position()
    }

    /// Current application head position read from live storage.
    pub(crate) fn head_position(&self) -> FrontierPosition {
        self.read_head().expect("read board application head")
    }

    /// Panics if the real apply thread has recorded a failure.
    pub(crate) fn assert_no_apply_error(&self) {
        let error = self.last_apply_error.lock().expect("apply error mutex");
        assert!(error.is_none(), "board apply failed: {error:?}");
    }

    /// Blocks until the published frontier covers `sequence` or panics after
    /// `timeout` (real apply must catch up on its own).
    pub(crate) fn wait_until_applied(&self, sequence: CommitSequence, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            self.assert_no_apply_error();
            if self.published_position() >= FrontierPosition::AppliedThrough(sequence) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "board apply did not reach the requested sequence within {timeout:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for HarnessColumnar {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.lock().expect("board worker mutex").take() {
            let _ = worker.join();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

impl ColumnarProjectionPort for HarnessColumnar {
    fn observe(&self, projection_name: &str) -> Result<ColumnarObservation, ColumnarPortError> {
        if projection_name != BOARD_PROJECTION_NAME {
            return Err(ColumnarPortError::Integrity);
        }
        // Head from storage without holding the engine lock (adapter shape).
        let head_position = self.read_head()?;
        let head = ProjectionFrontier::new(BOARD_HISTORY_INCARNATION, head_position);
        let observation = {
            let engine = self
                .engine
                .lock()
                .map_err(|_| ColumnarPortError::Unavailable)?;
            let definition = engine.definition().clone();
            let snapshot = engine.published_snapshot();
            let published_frontier = engine.published_frontier();
            let outcome = engine.outcome(head_position);
            let (has_published, lifecycle) = map_columnar_outcome(&outcome);
            ColumnarObservation::new(
                definition,
                snapshot,
                published_frontier,
                head,
                has_published,
                lifecycle,
            )
        };
        // Engine lock dropped before return; callers query the Arc snapshot.
        Ok(observation)
    }

    fn definition(&self, projection_name: &str) -> Option<RegisteredDefinition> {
        (projection_name == BOARD_PROJECTION_NAME).then(|| self.definition.clone())
    }

    fn notifier(&self) -> &ColumnarNotifier {
        &self.notifier
    }

    fn known_names(&self) -> Vec<String> {
        self.names.clone()
    }
}

fn map_columnar_outcome(outcome: &ColumnarOutcome) -> (bool, Option<ColumnarLifecycle>) {
    match outcome {
        ColumnarOutcome::Building(_) => (false, Some(ColumnarLifecycle::Building)),
        ColumnarOutcome::Ready(_) | ColumnarOutcome::Lagging(_) => {
            (true, Some(ColumnarLifecycle::Ready))
        }
        ColumnarOutcome::Invalid(invalid) => (
            false,
            Some(ColumnarLifecycle::Invalid {
                expected_fingerprint: invalid.expected_fingerprint,
                found_fingerprint: invalid.found_fingerprint,
            }),
        ),
        ColumnarOutcome::Rebuilding(rebuilding) => (
            true,
            Some(ColumnarLifecycle::Rebuilding {
                reason: rebuilding.reason,
                progress_applied: rebuilding.progress_applied,
                progress_total: rebuilding.progress_total,
            }),
        ),
        ColumnarOutcome::Degraded(degraded) => (
            true,
            Some(ColumnarLifecycle::Degraded {
                reason: degraded.reason,
            }),
        ),
    }
}

impl ServiceHarness {
    /// Board acceptance harness: real coordinator + real redb storage + real
    /// symbolic executor over that storage + real columnar apply + real policy.
    pub(crate) fn columnar_board() -> Self {
        let (database, note_plan) = AuditDatabase::create_board();
        let command_reference = database.executable_plan.reference();
        let capability_order = Arc::new(Mutex::new(Vec::new()));
        let discovery_order = Arc::new(Mutex::new(Vec::new()));
        let policy = Arc::new(HarnessPolicy::new(
            false,
            command_reference,
            ProjectionId::new(1).expect("nonzero synthetic projection id"),
            PartitionScopeV1::All,
            true,
            database.active_catalog.bundle().bundle(),
            database.alternate_partition.clone(),
            true,
            Arc::clone(&capability_order),
            Arc::clone(&discovery_order),
            Vec::new(),
        ));
        let ports = Arc::new(HarnessPorts::new(
            ReadCommitMode::ImmediateNotFound,
            database.active_catalog.clone(),
            database.executable_plan.clone(),
            capability_order,
            discovery_order,
        ));
        ports.install_additional_plan(note_plan);
        let operational = database.open();
        // Live pure-read handle over the SAME activated database instance the
        // coordinator mutates: symbolic reads and columnar apply share it.
        let shared = operational.shared_ports();
        let coordinator = start_coordinator_with_capacity(operational, 8);
        let inspector = coordinator.command_idempotency_inspector(Arc::new(FixedDigestProvider));
        let executors = ServiceExecutors::new(
            coordinator.administration_audit_executor(),
            coordinator.control_plane_executor(),
            coordinator.command_executor(),
            inspector,
        );
        let telemetry = Arc::new(HarnessTelemetry::default());
        let health = Arc::new(HarnessHealth::default());
        let deadline_scheduler = Arc::new(HarnessDeadlineScheduler::default());
        let cursor_tokens = Arc::new(SequentialCursorTokens::default());
        let columnar =
            HarnessColumnar::start(database.active_catalog.bundle().bundle(), shared.clone());
        let providers = ServiceProviders::new(
            Arc::clone(&ports) as Arc<dyn CatalogReadPort>,
            Arc::clone(&policy) as Arc<dyn riffdb_service::CurrentPolicyPort>,
            Arc::clone(&ports) as Arc<dyn AuthoritativeReadPort>,
            Arc::clone(&ports) as Arc<dyn ProjectionQueryPort>,
            Some(Arc::clone(&ports) as Arc<dyn OutboxStatusPort>),
            Arc::clone(&ports) as Arc<dyn OperationalStatusPort>,
            Arc::clone(&ports) as Arc<dyn CapabilityTokenIssuer>,
            Arc::new(HarnessIncidentIds::new(false)),
            Arc::new(HarnessDiagnostics),
            Arc::clone(&telemetry) as Arc<dyn ServiceTelemetry>,
            Arc::clone(&health) as Arc<dyn ServiceHealthHooks>,
            Arc::new(TokioSpawner),
            Arc::clone(&deadline_scheduler) as Arc<dyn RequestDeadlineScheduler>,
            Arc::clone(&cursor_tokens) as Arc<dyn CursorTokenGenerator>,
            Arc::new(FixedCursorClock),
        )
        .with_query_executor(
            Arc::new(riffdb_query_executor::StorageQueryExecutor::new(shared))
                as Arc<dyn riffdb_query_executor::QueryExecutionPort>,
        )
        .with_columnar(Arc::clone(&columnar) as Arc<dyn ColumnarProjectionPort>);
        let identity = ServiceIdentity::new(
            database_id(),
            environment(),
            AgentSessionAdmissionPolicy::Discard,
            BOARD_HISTORY_INCARNATION,
        );
        let process = ServiceProcessMetadata::new(
            timestamp(BASE_SECONDS),
            BuildInfo::new(
                "0.1.0-test",
                "service-harness",
                "rustc-1.97.0",
                Vec::new(),
                1,
                1,
                "2025-03-26",
            )
            .expect("bounded build metadata"),
        );
        let (service, pre_bootstrap) =
            RiffDbService::compose(identity, process, executors, providers);
        Self {
            service,
            policy,
            ports,
            telemetry,
            health,
            columnar: Some(columnar),
            deadline_scheduler,
            cursor_tokens,
            pre_bootstrap,
            coordinator: Some(coordinator),
            database,
        }
    }

    /// Real columnar runtime attached by [`ServiceHarness::columnar_board`].
    pub(crate) fn board_columnar(&self) -> &Arc<HarnessColumnar> {
        self.columnar
            .as_ref()
            .expect("harness was composed with columnar_board")
    }

    /// Active board contract bundle.
    pub(crate) fn board_bundle(&self) -> &ContractBundle {
        self.database.active_catalog.bundle().bundle()
    }

    /// Canonical `TicketStatus` value by variant name.
    pub(crate) fn board_status(&self, variant: &str) -> CanonicalValue {
        ticket_status_value(self.board_bundle(), variant)
    }

    /// One real CreateTicket command request with the default metrics.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_ticket_request(
        &self,
        caller_key: &str,
        organization_id: [u8; 16],
        ticket_id: [u8; 16],
        project_id: [u8; 16],
        reporter_id: [u8; 16],
        assignee_id: [u8; 16],
        status: &str,
        title: &str,
    ) -> ExecuteCommandRequest {
        self.create_ticket_request_with_metrics(
            caller_key,
            organization_id,
            ticket_id,
            project_id,
            reporter_id,
            assignee_id,
            status,
            title,
            0,
            0,
        )
    }

    /// Canonical `money<USD>` value from minor units (fixed v1 spec 38,2).
    pub(crate) fn board_cost(cost_minor_units: i64) -> CanonicalValue {
        CanonicalValue::Money(riffdb_types::Money::new(
            riffdb_types::CurrencyCode::new("USD").expect("currency"),
            riffdb_types::Decimal::new(
                riffdb_types::DecimalSpec::new(38, 2).expect("money v1 spec"),
                i128::from(cost_minor_units),
            )
            .expect("bounded money amount"),
        ))
    }

    /// One real CreateTicket command request carrying explicit metrics.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_ticket_request_with_metrics(
        &self,
        caller_key: &str,
        organization_id: [u8; 16],
        ticket_id: [u8; 16],
        project_id: [u8; 16],
        reporter_id: [u8; 16],
        assignee_id: [u8; 16],
        status: &str,
        title: &str,
        story_points: i64,
        cost_minor_units: i64,
    ) -> ExecuteCommandRequest {
        let bundle = self.board_bundle();
        let plan = bundle
            .commands()
            .iter()
            .find(|plan| plan.name() == "CreateTicket")
            .expect("CreateTicket plan");
        let input = input_record(
            plan.input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(caller_key).expect("bounded idempotency key"),
                ),
                ("organization_id", CanonicalValue::Uuid(organization_id)),
                ("ticket_id", CanonicalValue::Uuid(ticket_id)),
                ("project_id", CanonicalValue::Uuid(project_id)),
                ("reporter_id", CanonicalValue::Uuid(reporter_id)),
                ("assignee_id", CanonicalValue::Uuid(assignee_id)),
                ("status", ticket_status_value(bundle, status)),
                (
                    "title",
                    CanonicalValue::string(title).expect("bounded title"),
                ),
                ("story_points", CanonicalValue::I64(story_points)),
                ("cost", Self::board_cost(cost_minor_units)),
            ],
        );
        ExecuteCommandRequest::new(
            SourceName::new("CreateTicket").expect("checked command name"),
            Some(self.database.executable_plan.reference().contract_version()),
            SubmittedRecord::try_from(input).expect("bounded ticket input"),
        )
        .expect("bounded ticket request")
    }

    /// One real CreateNote command request (irrelevant interleaved entity).
    pub(crate) fn create_note_request(
        &self,
        caller_key: &str,
        organization_id: [u8; 16],
        note_id: [u8; 16],
        body: &str,
    ) -> ExecuteCommandRequest {
        let bundle = self.board_bundle();
        let plan = bundle
            .commands()
            .iter()
            .find(|plan| plan.name() == "CreateNote")
            .expect("CreateNote plan");
        let input = input_record(
            plan.input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(caller_key).expect("bounded idempotency key"),
                ),
                ("organization_id", CanonicalValue::Uuid(organization_id)),
                ("note_id", CanonicalValue::Uuid(note_id)),
                ("body", CanonicalValue::string(body).expect("bounded body")),
            ],
        );
        ExecuteCommandRequest::new(
            SourceName::new("CreateNote").expect("checked command name"),
            Some(self.database.executable_plan.reference().contract_version()),
            SubmittedRecord::try_from(input).expect("bounded note input"),
        )
        .expect("bounded note request")
    }

    /// The compiled-path request: the exact board_page shape as an ad-hoc
    /// symbolic query executed by the REAL query executor over live storage.
    pub(crate) fn board_symbolic_request(
        &self,
        organization_id: [u8; 16],
        project_id: [u8; 16],
        status: &str,
    ) -> ExecuteSymbolicQueryRequest {
        let mut values = BTreeMap::new();
        values.insert(
            "organization_id".to_owned(),
            SubmittedValue::Uuid(organization_id),
        );
        values.insert("project_id".to_owned(), SubmittedValue::Uuid(project_id));
        values.insert(
            "status".to_owned(),
            SubmittedValue::Enum(SubmittedEnum::name_only(
                SourceName::new(status).expect("status variant name"),
            )),
        );
        ExecuteSymbolicQueryRequest::new(
            SymbolicContractSelector::active(),
            SymbolicQuerySource::new(BOARD_PAGE_QUERY.to_owned())
                .expect("bounded board query source"),
            SymbolicQueryParameters::new(values).expect("board parameters"),
        )
    }

    /// The projected-path request equivalent to [`Self::board_symbolic_request`]:
    /// select the board fields (ticket_id via PK return), eq predicates on
    /// project_id + status, order ticket_id asc, limit 50.
    pub(crate) fn board_projected_request(
        &self,
        organization_id: [u8; 16],
        project_id: [u8; 16],
        status: &str,
        freshness: FreshnessPolicy,
    ) -> ExecuteProjectedQueryRequest {
        let body = ProjectedQueryBody::new(CanonicalValue::Uuid(organization_id))
            .with_select(BOARD_SELECT.iter().map(|name| (*name).to_owned()).collect())
            .with_predicates(vec![
                ProjectedColumnPredicate::Eq {
                    field: "project_id".to_owned(),
                    value: CanonicalValue::Uuid(project_id),
                },
                ProjectedColumnPredicate::Eq {
                    field: "status".to_owned(),
                    value: self.board_status(status),
                },
            ])
            .with_order(vec![ProjectedOrderSpec {
                field: "ticket_id".to_owned(),
                direction: SortDirection::Asc,
            }])
            .with_limit(Some(50));
        ExecuteProjectedQueryRequest::new(
            SymbolicContractSelector::active(),
            BOARD_PROJECTION_NAME,
            body,
            freshness,
        )
    }
}

/// [`run_async`] with a wider worker pool for tests that park one real
/// blocking columnar wait while commands and apply progress elsewhere.
pub(crate) fn run_async_threads(
    worker_threads: usize,
    output: impl Future<Output = ()> + Send + 'static,
) {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_time()
        .build()
        .expect("build service test runtime")
        .block_on(output);
}
