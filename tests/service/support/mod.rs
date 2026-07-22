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
    ActiveCatalogSnapshot, CatalogError, CatalogErrorKind, CatalogPreparationResult,
    ResolvedExecutablePlan, ValidatedContractBundle, prepare_catalog_activation,
    resolve_executable_plan, validate_catalog_history,
};
use riffdb_commit::{
    AdministrationClock, AdministrationClockError, AdmissionClock, AdmissionClockError,
    CoordinatorDurability, CoordinatorWorkloadCapacity, ProvenanceIdSource,
    ProvenanceIdSourceError, RunningCommandCoordinator,
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
    CurrentAuthorizer, Decision, NoopAuthorizationTelemetry, NormalizedCapabilityCreateRecord,
    OperationRequest, PartitionConstraint, ProvenanceSelector, TrustedAudienceCatalog,
    UntrustedInvocationClaims,
};
use riffdb_service::{
    AbsentCapabilityRevokeTargetSnapshot, AffectedEntityView, AuthoritativeCommitNotification,
    AuthoritativeCommitPage, AuthoritativeCommitScanRequest, AuthoritativeCommitSnapshot,
    AuthoritativeCommitSubscriptionRequest, AuthoritativeEntityRequest,
    AuthoritativeEntitySnapshot, AuthoritativeIndexPage, AuthoritativeIndexRequest,
    AuthoritativeJournaledOutcome, AuthoritativeOutcomeFacts, AuthoritativeOutcomeRequest,
    AuthoritativeOutcomeSnapshot, AuthoritativeProvenanceSnapshot, AuthoritativeReadError,
    AuthoritativeReadPort, AuthoritativeReadinessFailure, BootstrapCapabilityRequest,
    BootstrapRequestContext, BuildInfo, CapabilityRevokeTargetSnapshot, CapabilityTokenIssueError,
    CapabilityTokenIssuer, CatalogExecutablePlanRequest, CatalogReadPort, CommandDurability,
    CommitNotificationSource, ComponentHealth, ContractSelection, ContractSource, CursorClockError,
    CursorMonotonicClock, CursorTick, CursorTokenGenerationError, CursorTokenGenerator,
    DeclaredOutcomeView, DeployContractRequest, DurableEventView, ExecuteCommandRequest,
    FieldSelection, GetContractVersionRequest, GetEntityRequest, GetProjectionStatusRequest,
    HealthComponentKind, HealthComponentStatus, HealthContext, JournaledCommandResult,
    ListPendingOutboxDeliveriesRequest, NormalCreateCapabilityRequest, OperationalHealthSnapshot,
    OperationalStatisticsSnapshot, OperationalStatusError, OperationalStatusPort, OutboxStatusPort,
    OutboxStatusPortError, OutboxStatusRequest, OutboxStatusSnapshot, PageLimit, PageRequest,
    PortAdmissionError, PortCapacityPermit, PortCompletionSender, PortFuture, PortReceipt,
    PreBootstrapHealthContextIssuer, PreBootstrapLifecycle, ProjectionPageFence,
    ProjectionPortError, ProjectionPortReady, ProjectionPortRequest, ProjectionPortResult,
    ProjectionQueryPort, ProjectionStatusSnapshot, ProvenanceClaimsView, QueryProjectionRequest,
    RequestCancellationHandle, RequestContext, RequestControl, RequestDeadlineFuture,
    RequestDeadlineScheduler, ResolveCommandOutcomeRequest, RevokeCapabilityRequest, RiffDbService,
    ScanCommitsRequest, ScanIndexRequest, ServiceDiagnostics, ServiceExecutors, ServiceHealthHooks,
    ServiceIdentity, ServiceJob, ServiceJobSpawner, ServiceProcessMetadata, ServiceProviders,
    ServiceTelemetry, ServiceTelemetryEvent, SourceName, TraceProvenanceRequest,
    ValidateContractRequest, port_completion_channel,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AdministrationAuditReader, AdministrationAuditScan,
    AdministrationAuditScanRequest, AuditPrincipalV1, CatalogActivationIntentV1,
    CatalogActivationResult, CatalogAdministrationRepository, DatabaseInitializationPort,
    DatabaseInitializationResult, EvidencePageLimit, ExecutablePlanRef, IdempotencyKeyDigest,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StorageScanLimit, StoredAdministrationAuditRecordV1,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession,
};
use riffdb_storage_redb::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, Audience, CanonicalInputHash,
    CanonicalRecord, CanonicalString, CanonicalValue, CapabilityGrantV1, CapabilityId,
    CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1, CommitSequence,
    ContractLineage, ContractVersion, DatabaseId, Decimal, DecimalSpec, DigestKeyId, EntityKey,
    EntityKeyBuilder, EntityVersion, Environment, EventId, FrontierPosition, IdempotencyKey,
    IncidentId, IndexEpoch, LogicalTime, MAX_STRING_BYTES, OutcomeId, PartitionKey,
    PartitionKeyBuilder, PartitionKeyHash, PartitionScopeV1, ProjectionGeneration, ProjectionId,
    ProjectionIdentity, ProvenanceId, RequestId, RevocationReasonCodeV1, ScopedPartitionV1,
    ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    ServiceOperationV1, TenantId, TenantScope, Timestamp,
};
use tokio::sync::Notify;

const BASE_SECONDS: i64 = 1_700_200_000;
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
    deadline_scheduler: Arc<HarnessDeadlineScheduler>,
    cursor_tokens: Arc<SequentialCursorTokens>,
    pre_bootstrap: PreBootstrapHealthContextIssuer,
    coordinator: Option<RunningCommandCoordinator>,
    database: AuditDatabase,
}

impl ServiceHarness {
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
        let database = if pre_bootstrap {
            AuditDatabase::create_pre_bootstrap(broad_operations)
        } else {
            AuditDatabase::create(broad_operations)
        };
        let command_reference = database.executable_plan.reference();
        let capability_order = Arc::new(Mutex::new(Vec::new()));
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
            Arc::clone(&capability_order),
        ));
        let ports = Arc::new(HarnessPorts::new(
            read_mode,
            database.active_catalog.clone(),
            database.executable_plan.clone(),
            capability_order,
        ));
        let coordinator = start_coordinator(database.open());
        let executors = ServiceExecutors::new(
            coordinator.administration_audit_executor(),
            coordinator.control_plane_executor(),
            coordinator.command_executor(),
            coordinator.command_idempotency_inspector(Arc::new(FixedDigestProvider)),
        );
        let telemetry = Arc::new(HarnessTelemetry::default());
        let health = Arc::new(HarnessHealth::default());
        let deadline_scheduler = Arc::new(HarnessDeadlineScheduler::default());
        let cursor_tokens = Arc::new(SequentialCursorTokens::default());
        let providers = ServiceProviders::new(
            Arc::clone(&ports) as Arc<dyn CatalogReadPort>,
            Arc::clone(&policy) as Arc<dyn riffdb_service::CurrentPolicyPort>,
            Arc::clone(&ports) as Arc<dyn AuthoritativeReadPort>,
            Arc::clone(&ports) as Arc<dyn ProjectionQueryPort>,
            broad_operations.then(|| Arc::clone(&ports) as Arc<dyn OutboxStatusPort>),
            Arc::clone(&ports) as Arc<dyn OperationalStatusPort>,
            Arc::clone(&ports) as Arc<dyn CapabilityTokenIssuer>,
            Arc::new(HarnessIncidentIds::new(fail_incident_source)),
            Arc::new(HarnessDiagnostics),
            Arc::clone(&telemetry) as Arc<dyn ServiceTelemetry>,
            Arc::clone(&health) as Arc<dyn ServiceHealthHooks>,
            Arc::new(TokioSpawner),
            Arc::clone(&deadline_scheduler) as Arc<dyn RequestDeadlineScheduler>,
            Arc::clone(&cursor_tokens) as Arc<dyn CursorTokenGenerator>,
            Arc::new(FixedCursorClock),
        );
        let identity = ServiceIdentity::new(
            database_id(),
            environment(),
            AgentSessionAdmissionPolicy::Discard,
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
            deadline_scheduler,
            cursor_tokens,
            pre_bootstrap,
            coordinator: Some(coordinator),
            database,
        }
    }

    pub(crate) fn context(&self, request_seed: u8) -> (RequestContext, RequestCancellationHandle) {
        let (control, cancellation) = RequestControl::new(Instant::now() + Duration::from_secs(30));
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

    pub(crate) fn deploy_request(&self) -> DeployContractRequest {
        DeployContractRequest::new(
            ContractSource::new(BUDGET_SOURCE).expect("bounded example contract"),
            Some(self.database.active_catalog.pointer().contract_version()),
        )
        .expect("bounded deployment request")
    }

    pub(crate) fn validate_request(&self) -> ValidateContractRequest {
        ValidateContractRequest::new(
            ContractSource::new(BUDGET_SOURCE).expect("bounded example contract"),
        )
        .expect("bounded validation request")
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
            PageRequest::new(PageLimit::new(10).expect("page limit"), None),
        )
        .expect("checked index request")
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

    pub(crate) fn panic_next_policy_check(&self) {
        self.policy.panic_next();
    }

    pub(crate) fn revoke_policy(&self) {
        self.policy.revoke();
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

    pub(crate) async fn wait_for_stalled_projection(&self) {
        self.ports.wait_for_stalled_projection().await;
        self.deadline_scheduler.wait_for_projection_waiters(2).await;
    }

    pub(crate) fn elapse_projection_deadline(&self) {
        self.deadline_scheduler.elapse_projection_deadline();
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
            input,
        )
        .expect("bounded command request")
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
        let path = next_database_path();
        let mut store = RedbStore::open(&path).expect("create service harness database");
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize service harness database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        let source = if with_index {
            BUDGET_SOURCE.replacen(
                "    invariant non_negative:",
                "    index by_fiscal_year (fiscal_year)\n\n    invariant non_negative:",
                1,
            )
        } else {
            BUDGET_SOURCE.to_owned()
        };
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
    std::env::temp_dir().join(format!(
        "riffdb-service-harness-{}-{}.redb",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ))
}

impl Drop for AuditDatabase {
    fn drop(&mut self) {
        let _result = std::fs::remove_file(&self.path);
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
            | StoredAdministrationAuditRecordV1::Capability(_) => None,
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
    assert!(
        history.matches(opened.database_id(), opened.open_session_id()),
        "catalog proof belongs to the structural-open session"
    );
    let (_, _, dormant): (_, _, RedbDormantPorts) = opened.into_parts();
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

fn start_coordinator(ports: RedbOperationalPorts) -> RunningCommandCoordinator {
    let conflicts: Arc<dyn ConflictManager> = Arc::new(
        ShardedConflictManager::new(ConflictManagerConfig::default())
            .expect("start conflict manager"),
    );
    RunningCommandCoordinator::start(
        CoordinatorWorkloadCapacity::new(8).expect("nonzero coordinator capacity"),
        CoordinatorDurability::Sync,
        ports,
        conflicts,
        Arc::new(FixedAdmissionClock),
        Arc::new(IncrementingAdministrationClock::new()),
        Arc::new(FixedAuthorizationClock),
        Arc::new(SequentialProvenanceIds::default()),
    )
    .expect("start production coordinator")
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
    partition_constraints: Mutex<Vec<Option<PartitionConstraint>>>,
    panic_next: AtomicBool,
    capability_order: Arc<Mutex<Vec<&'static str>>>,
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
        capability_order: Arc<Mutex<Vec<&'static str>>>,
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
        permissions.push(
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::DeployContract)
                .expect("unparameterized deploy-contract permission"),
        );
        let permissions = CapabilityPermissionsV1::new(permissions).expect("canonical permissions");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            partition_scope,
            permissions.clone(),
            Vec::new(),
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
            Vec::new(),
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
            partition_constraints: Mutex::new(Vec::new()),
            panic_next: AtomicBool::new(false),
            capability_order,
        }
    }

    pub(crate) fn principal(&self) -> AuthenticatedPrincipal {
        self.fixture.authenticated_principal().clone()
    }

    fn revoke(&self) {
        self.fixture
            .revoke_current(timestamp(BASE_SECONDS + 15))
            .expect("revoke current harness capability");
        self.narrowed_fixture
            .revoke_current(timestamp(BASE_SECONDS + 15))
            .expect("revoke narrowed harness capability");
    }

    fn use_narrowed_for_next_allow(&self) {
        self.narrow_after_next_allow.store(true, Ordering::Release);
        self.restore_after_narrowed_allow
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
        if request.operation() == ServiceOperationV1::CreateCapability {
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
            &FixedAuthorizationClock,
            &NoopAuthorizationTelemetry,
            database_id(),
            environment(),
        )
        .with_trusted_audience_catalog(&self.trusted_audiences)
        .authorize(principal, request)?;
        if let Decision::Allow(authorized) = &decision {
            self.allowed_calls.fetch_add(1, Ordering::AcqRel);
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
        }
        Ok(decision)
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
    executable_plan: ResolvedExecutablePlan,
    prepare_active_mode: AtomicU8,
    prepare_active_calls: AtomicUsize,
    prepare_contract_version_calls: AtomicUsize,
    prepare_active_started: Notify,
    compatible_activation: Mutex<Option<ActiveCatalogSnapshot>>,
    compatible_activation_release: Notify,
    prepare_deployment_mode: AtomicU8,
    prepare_deployment_started: Notify,
    panic_executable_plan: AtomicBool,
    panic_revoke_target_read: AtomicBool,
    read_mode: ReadCommitMode,
    read_reservations: AtomicUsize,
    read_submissions: AtomicUsize,
    read_reservation_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    read_commit_snapshot: Mutex<Option<AuthoritativeCommitSnapshot>>,
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
}

impl HarnessPorts {
    fn new(
        read_mode: ReadCommitMode,
        active_catalog: ActiveCatalogSnapshot,
        executable_plan: ResolvedExecutablePlan,
        capability_order: Arc<Mutex<Vec<&'static str>>>,
    ) -> Self {
        Self {
            shared: Arc::new(HarnessPortState {
                active_catalog: Mutex::new(active_catalog),
                executable_plan,
                prepare_active_mode: AtomicU8::new(CATALOG_READY),
                prepare_active_calls: AtomicUsize::new(0),
                prepare_contract_version_calls: AtomicUsize::new(0),
                prepare_active_started: Notify::new(),
                compatible_activation: Mutex::new(None),
                compatible_activation_release: Notify::new(),
                prepare_deployment_mode: AtomicU8::new(CATALOG_READY),
                prepare_deployment_started: Notify::new(),
                panic_executable_plan: AtomicBool::new(false),
                panic_revoke_target_read: AtomicBool::new(false),
                read_mode,
                read_reservations: AtomicUsize::new(0),
                read_submissions: AtomicUsize::new(0),
                read_reservation_hook: Mutex::new(None),
                read_commit_snapshot: Mutex::new(None),
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
            }),
        }
    }

    fn set_read_reservation_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .shared
            .read_reservation_hook
            .lock()
            .expect("read reservation hook mutex") = Some(hook);
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
        *self
            .shared
            .active_catalog
            .lock()
            .expect("active catalog mutex") = successor;
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

struct SubscribeCommitPermit {
    shared: Arc<HarnessPortState>,
}

struct HarnessCommitNotificationSource {
    notifications: VecDeque<AuthoritativeCommitNotification>,
    dropped: Arc<AtomicBool>,
    stall_next: bool,
    waiting: Arc<AtomicBool>,
    panic: Arc<AtomicU8>,
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

impl PortCapacityPermit<(), Option<ActiveCatalogSnapshot>, CatalogError> for ActiveCatalogPermit {
    fn submit(
        self: Box<Self>,
        (): (),
    ) -> Result<PortReceipt<Option<ActiveCatalogSnapshot>, CatalogError>, PortAdmissionError> {
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
        let page = AuthoritativeIndexPage::new(
            &request,
            Vec::new(),
            None,
            IndexEpoch::new(1).expect("index epoch"),
        )
        .expect("empty authoritative index page");
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

    fn prepare_contract_version(
        &self,
        _control: &RequestControl,
        lineage: ContractLineage,
        version: ContractVersion,
    ) -> PortFuture<'_, Option<ValidatedContractBundle>, CatalogError> {
        self.shared
            .prepare_contract_version_calls
            .fetch_add(1, Ordering::AcqRel);
        let bundle = self
            .shared
            .active_catalog
            .lock()
            .expect("active catalog mutex")
            .bundle()
            .clone();
        Box::pin(async move {
            Ok(
                (bundle.lineage() == &lineage && bundle.contract_version() == version)
                    .then_some(bundle),
            )
        })
    }

    fn reserve_active_catalog(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        riffdb_service::BoxPortCapacityPermit<(), Option<ActiveCatalogSnapshot>, CatalogError>,
        PortAdmissionError,
    > {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { Ok(Box::new(ActiveCatalogPermit { shared }) as _) })
    }

    fn reserve_contract_version(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
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

    fn executable_plan(
        &self,
        _control: &RequestControl,
        request: CatalogExecutablePlanRequest,
    ) -> PortFuture<'_, ResolvedExecutablePlan, CatalogError> {
        let panic = self
            .shared
            .panic_executable_plan
            .swap(false, Ordering::AcqRel);
        let resolved = self.shared.executable_plan.clone();
        let reference = resolved.reference();
        let matches = request.lineage() == reference.contract_lineage()
            && request.version() == reference.contract_version()
            && request.bundle_hash() == reference.contract_bundle_hash()
            && request.command_id() == reference.command_id()
            && request.plan_hash() == reference.command_plan_hash();
        Box::pin(async move {
            assert!(!panic, "injected executable-plan resolution panic");
            if matches {
                Ok(resolved)
            } else {
                Err(CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))
            }
        })
    }

    fn prepare_deployment(
        &self,
        _control: &RequestControl,
        candidate: ContractBundle,
        expected_active_version: Option<ContractVersion>,
    ) -> PortFuture<'_, CatalogPreparationResult, CatalogError> {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move {
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
    fn reserve_read_entity(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
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

    fn reserve_scan_index(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
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

    fn reserve_read_outcome(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
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

    fn reserve_read_commit(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
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

    fn reserve_scan_commits(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
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

    fn reserve_subscribe_to_commits(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
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

    fn reserve_trace_provenance(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
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

    fn read_capability_revoke_target(
        &self,
        _control: &RequestControl,
        capability_id: riffdb_types::CapabilityId,
    ) -> PortFuture<'_, CapabilityRevokeTargetSnapshot, AuthoritativeReadError> {
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

#[derive(Default)]
struct HarnessDeadlineScheduler {
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
        if self.control_request_deadline.swap(false, Ordering::AcqRel) {
            self.request_waiters.fetch_add(1, Ordering::AcqRel);
            self.request_registered.notify_waiters();
            return Box::pin(self.request_deadline.notified());
        }
        if self
            .controlled_stream_lifetime_waits
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok_and(|remaining| remaining == 1)
        {
            self.stream_lifetime_waiters.fetch_add(1, Ordering::AcqRel);
            return Box::pin(self.stream_lifetime.notified());
        }
        if self.control_projection_deadline.load(Ordering::Acquire)
            && deadline.saturating_duration_since(Instant::now()) < Duration::from_secs(10)
        {
            self.projection_waiters.fetch_add(1, Ordering::AcqRel);
            self.projection_registered.notify_waiters();
            return Box::pin(self.projection_deadline.notified());
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

fn database_id() -> DatabaseId {
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
