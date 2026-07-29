#![forbid(unsafe_code)]

//! In-process RiffDB service comparison and correctness preflight.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU64};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod budget_diagnostics;
mod performance_support;

use riffdb_auth::{AuthenticatedPrincipal, NewlyIssuedCapabilityToken};
use riffdb_budget_comparison_core::{
    BudgetOperation, ContentionObservation, ContentionWorkload, SequentialObservation,
    SequentialWorkload, canonical_workload, evaluate_sequential, expected_contention_observation,
    observe_sequential, verify_contention, verify_sequential,
};
use riffdb_budget_comparison_postgres::{PostgresBudgetAdapter, live_database_url};
use riffdb_budget_comparison_riffdb_service::{
    AllocateBudgetInputFields, BudgetContractBinding, BudgetEntityFields, BudgetOutcomeFields,
    CreateBudgetInputFields, RequestContextFactory, RiffDbServiceBudgetAdapter,
    riffdb_service_guarantee_profile,
};
use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogError, CatalogErrorKind, CatalogHistoryOutcome,
    CatalogPreparationResult, ResolvedExecutablePlan, ValidatedContractBundle,
    resolve_executable_plan, validate_catalog_history,
};
use riffdb_commit::{
    AdministrationClock, AdministrationClockError, AdmissionClock, AdmissionClockError,
    ApplicationCommitNotificationError, ApplicationCommitNotificationSink, CoordinatorDurability,
    CoordinatorWorkloadCapacity, ProvenanceIdSource, ProvenanceIdSourceError,
    RunningCommandCoordinator,
};
use riffdb_conflict::{ConflictManager, ConflictManagerConfig, ShardedConflictManager};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{CommandPlan, ContractBundle, RecordSchema};
use riffdb_errors::{IncidentIdSource, IncidentIdSourceError, InternalError, PublicErrorKind};
use riffdb_idempotency::{
    IdempotencyDigestCandidatesV1, IdempotencyDigestError, IdempotencyDigestProvider,
};
use riffdb_policy::{
    AgentSessionAdmissionPolicy, AuthorizationClock, AuthorizationClockError, AuthorizationError,
    CurrentAuthorizer, Decision, NoopAuthorizationTelemetry, OperationRequest,
    TrustedAudienceCatalog, UntrustedInvocationClaims,
};
use riffdb_service::{
    AbsentCapabilityRevokeTargetSnapshot, AuthoritativeCommitPage, AuthoritativeCommitScanRequest,
    AuthoritativeCommitSnapshot, AuthoritativeCommitSubscriptionRequest,
    AuthoritativeEntityRequest, AuthoritativeEntitySnapshot, AuthoritativeIndexPage,
    AuthoritativeIndexRequest, AuthoritativeOutcomeRequest, AuthoritativeOutcomeSnapshot,
    AuthoritativeProvenanceSnapshot, AuthoritativeReadError, AuthoritativeReadPort,
    AuthoritativeReadinessFailure, BoxPortCapacityPermit, BuildInfo,
    CapabilityRevokeTargetSnapshot, CapabilityTokenIssueError, CapabilityTokenIssuer,
    CatalogExecutablePlanRequest, CatalogReadPort, CommandApplication, CommitNotificationSource,
    CursorClockError, CursorMonotonicClock, CursorTick, CursorTokenGenerationError,
    CursorTokenGenerator, ExecuteCommandRequest, ExecuteCommandResult, GetEntityRequest,
    GetEntityResult, GetProjectionStatusRequest, GetProjectionStatusResult,
    OperationalHealthSnapshot, OperationalStatisticsSnapshot, OperationalStatusError,
    OperationalStatusPort, PortAdmissionError, PortCapacityPermit, PortFuture, PortReceipt,
    ProjectionPortError, ProjectionPortRequest, ProjectionPortResult, ProjectionQueryPort,
    ProjectionStatusSnapshot, QueryApplication, QueryProjectionRequest, QueryProjectionResult,
    RequestCancellationHandle, RequestContext, RequestControl, RequestDeadlineFuture,
    RequestDeadlineScheduler, RiffDbService, ScanIndexRequest, ScanIndexResult, ServiceDiagnostics,
    ServiceExecutors, ServiceFuture, ServiceHealthHooks, ServiceIdentity, ServiceJob,
    ServiceJobSpawner, ServiceProcessMetadata, ServiceProviders, ServiceTelemetry,
    ServiceTelemetryEvent, port_completion_channel,
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
use riffdb_storage_redb::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CanonicalRecord, CanonicalValue, CapabilityGrantV1, CapabilityId,
    CapabilityPermissionV1, CapabilityPermissionsV1, CommandId, CommitSequence, ContractLineage,
    ContractVersion, DatabaseId, DigestKeyId, EntityFieldVisibilityV1, EntityKey, EntityVersion,
    Environment, FieldId, IdempotencyKey, IncidentId, PartitionScopeV1, ProvenanceId, RequestId,
    ServiceAuditPhaseV1, ServiceIngressKindV1, ServiceOperationV1, TenantScope, Timestamp,
};

const BASE_SECONDS: i64 = 1_700_300_000;
const BUDGET_SOURCE: &str = include_str!("../../../contracts/examples/budget.riff");
static NEXT_DATABASE: AtomicU64 = AtomicU64::new(1);

#[test]
fn service_comparison_matches_the_shared_semantic_oracle() {
    run_async(async move {
        let workload = canonical_workload();
        verify_parameterized_backend(
            || {
                let mut harness = BudgetServiceHarness::new();
                let result = observe_sequential(&harness.adapter, &workload.sequential);
                harness.stop();
                result
            },
            || {
                let mut harness = BudgetServiceHarness::new();
                let result = harness.adapter.run_contention(&workload.contention);
                harness.stop();
                result
            },
            &workload.sequential,
            &workload.contention,
        )
        .expect("in-process RiffDB service matches the shared oracle");
    });

    let Some(database_url) = live_database_url().expect("live PostgreSQL config is valid") else {
        return;
    };
    let workload = canonical_workload();
    let postgres = PostgresBudgetAdapter::new(database_url).expect("bounded database URL");
    verify_parameterized_backend(
        || {
            postgres.reset_schema()?;
            postgres.run_sequential(&workload.sequential)
        },
        || {
            postgres.reset_schema()?;
            postgres.run_contention(&workload.contention)
        },
        &workload.sequential,
        &workload.contention,
    )
    .expect("PostgreSQL and RiffDB use the same normalized oracle");
}

#[test]
fn service_comparison_preflight_proves_replay_mismatch_and_shared_authorization() {
    run_async(async move {
        let workload = canonical_workload();
        let create = workload
            .sequential
            .operations
            .iter()
            .find(|operation| {
                matches!(
                    operation,
                    BudgetOperation::Create(command) if command.approved_amount.to_string() == "100.00"
                )
            })
            .expect("canonical successful CreateBudget")
            .clone();
        let mut harness = BudgetServiceHarness::new();

        let calls_before_commit = harness.policy.calls();
        let committed = harness
            .adapter
            .execute_with_metadata(&create)
            .expect("first execution commits");
        let calls_after_commit = harness.policy.calls();
        let replayed = harness
            .adapter
            .execute_with_metadata(&create)
            .expect("equal retry resolves the original outcome");
        let calls_after_replay = harness.policy.calls();
        assert_eq!(
            committed.completion,
            riffdb_service::JournaledCompletion::Committed
        );
        assert_eq!(
            replayed.completion,
            riffdb_service::JournaledCompletion::Replayed
        );
        assert_eq!(replayed.commit_sequence, committed.commit_sequence);
        assert_eq!(replayed.provenance_id, committed.provenance_id);
        assert_eq!(replayed.observation, committed.observation);
        assert_eq!(
            committed.durability,
            riffdb_service::CommandDurability::Synchronous
        );
        assert_eq!(
            replayed.durability,
            riffdb_service::CommandDurability::Synchronous
        );
        assert!(
            calls_after_commit.saturating_sub(calls_before_commit) >= 2,
            "first execution must cross every current-policy safe point"
        );
        assert!(
            calls_after_replay.saturating_sub(calls_after_commit) >= 2,
            "replay must independently cross current-policy safe points"
        );

        let BudgetOperation::Create(mut mismatched) = create else {
            unreachable!("selected operation is CreateBudget")
        };
        mismatched.approved_amount =
            riffdb_budget_comparison_core::Amount::parse("90.00").expect("canonical amount");
        let error = harness
            .adapter
            .execute_with_metadata(&BudgetOperation::Create(mismatched))
            .expect_err("same key with different canonical input is rejected");
        assert_eq!(
            error
                .service_failure()
                .and_then(riffdb_service::ServiceFailure::public_error)
                .map(riffdb_errors::PublicError::kind),
            Some(PublicErrorKind::IdempotencyKeyReuse)
        );

        harness.stop();
        let records = harness.audit_records();
        assert_eq!(
            records
                .iter()
                .filter(|record| record.operation() == ServiceOperationV1::ExecuteCommand)
                .map(|record| record.phase())
                .collect::<Vec<_>>(),
            [
                ServiceAuditPhaseV1::Started,
                ServiceAuditPhaseV1::Succeeded,
                ServiceAuditPhaseV1::Started,
                ServiceAuditPhaseV1::Succeeded,
                ServiceAuditPhaseV1::Started,
                ServiceAuditPhaseV1::Failed,
            ]
        );
        assert!(
            records.iter().all(|record| {
                record.ingress() == ServiceIngressKindV1::InProcessTestComparison
            })
        );
    });
}

#[test]
fn service_comparison_profile_and_dependency_boundary_are_explicit() {
    let profile = riffdb_service_guarantee_profile();
    assert_eq!(
        profile.command_invariants,
        riffdb_budget_comparison_core::GuaranteeLevel::Matched
    );
    assert_eq!(
        profile.idempotency,
        riffdb_budget_comparison_core::GuaranteeLevel::Matched
    );
    assert_eq!(
        profile.authorization,
        riffdb_budget_comparison_core::GuaranteeLevel::Matched
    );
    assert_eq!(
        profile.projections,
        riffdb_budget_comparison_core::GuaranteeLevel::Unsupported
    );

    let adapter_manifest = include_str!("../riffdb-service/Cargo.toml");
    let adapter_source = include_str!("../riffdb-service/src/lib.rs");
    for banned in [
        "riffdb-commit",
        "riffdb-runtime",
        "riffdb-storage-api",
        "riffdb-storage-redb",
        "riffdb-conflict",
    ] {
        assert!(
            !adapter_manifest.contains(banned),
            "adapter depends on {banned}"
        );
        assert!(!adapter_source.contains(banned), "adapter imports {banned}");
    }
    let root_manifest = include_str!("../../../Cargo.toml");
    assert!(!root_manifest.contains("budget-comparison"));
}

fn verify_parameterized_backend<SequentialRun, ContentionRun, Failure>(
    sequential_run: SequentialRun,
    contention_run: ContentionRun,
    sequential: &SequentialWorkload,
    contention: &ContentionWorkload,
) -> Result<(), Failure>
where
    SequentialRun: FnOnce() -> Result<SequentialObservation, Failure>,
    ContentionRun: FnOnce() -> Result<ContentionObservation, Failure>,
    Failure: std::fmt::Debug,
{
    let expected = evaluate_sequential(sequential).expect("reference sequential workload");
    let actual = sequential_run()?;
    verify_sequential(&expected, &actual).expect("sequential normalized observation");

    let expected = expected_contention_observation(contention).expect("reference contention");
    let actual = contention_run()?;
    verify_contention(&expected, &actual).expect("contention normalized observation");
    Ok(())
}

struct BudgetServiceHarness {
    adapter: RiffDbServiceBudgetAdapter,
    policy: Arc<BudgetPolicy>,
    coordinator: Option<RunningCommandCoordinator>,
    database: BudgetDatabase,
}

impl BudgetServiceHarness {
    fn new() -> Self {
        Self::with_configuration(CoordinatorDurability::Sync, 16)
    }

    fn with_durability(durability: CoordinatorDurability) -> Self {
        Self::with_configuration(durability, 256)
    }

    fn with_configuration(durability: CoordinatorDurability, workload_capacity: u16) -> Self {
        let database = BudgetDatabase::create();
        let catalog = Arc::new(BudgetCatalog::new(
            database.active.clone(),
            database.plans.clone(),
        ));
        let policy = Arc::new(BudgetPolicy::new(&database.active));
        let entities = Arc::new(MirroredEntities::default());
        let authoritative = Arc::new(BudgetAuthoritativeReads {
            entities: Arc::clone(&entities),
        });
        let unavailable = Arc::new(UnavailableProviders);
        let coordinator = start_coordinator(database.open(), durability, workload_capacity);
        let executors = ServiceExecutors::new(
            coordinator.administration_audit_executor(),
            coordinator.control_plane_executor(),
            coordinator.command_executor(),
            coordinator.command_idempotency_inspector(Arc::new(FixedDigestProvider)),
        );
        let providers = ServiceProviders::new(
            catalog,
            Arc::clone(&policy) as Arc<dyn riffdb_service::CurrentPolicyPort>,
            authoritative,
            Arc::clone(&unavailable) as Arc<dyn ProjectionQueryPort>,
            None,
            Arc::clone(&unavailable) as Arc<dyn OperationalStatusPort>,
            unavailable,
            Arc::new(SequentialIncidentIds::default()),
            Arc::new(NoopDiagnostics),
            Arc::new(NoopTelemetry),
            Arc::new(ReadinessRecorder::default()),
            Arc::new(TokioSpawner(tokio::runtime::Handle::current())),
            Arc::new(TokioDeadlineScheduler(tokio::runtime::Handle::current())),
            Arc::new(SequentialCursorTokens::default()),
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
                "wp125-service-comparison",
                "rustc-1.97.0",
                Vec::new(),
                1,
                1,
                "2026-07-21",
            )
            .expect("bounded build metadata"),
        );
        let service = RiffDbService::new(identity, process, executors, providers);
        let tracking = Arc::new(TrackingBudgetService {
            service,
            entities,
            binding: database.binding.clone(),
        });
        let contexts = Arc::new(ComparisonContexts::new(policy.principal()));
        let adapter = RiffDbServiceBudgetAdapter::new(tracking, contexts, database.binding.clone());
        Self {
            adapter,
            policy,
            coordinator: Some(coordinator),
            database,
        }
    }

    fn stop(&mut self) {
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.shutdown().expect("coordinator shutdown");
        }
    }

    fn audit_records(&self) -> Vec<riffdb_storage_api::StoredServiceAuditRecordV1> {
        let ports = self.database.open();
        let scan = ports
            .scan_administration_audit(AdministrationAuditScanRequest::new(
                None,
                StorageScanLimit::new(128).expect("audit scan bound"),
            ))
            .expect("scan service comparison audit");
        let AdministrationAuditScan::ExactEnd { records } = scan else {
            panic!("comparison audit fits one bounded page")
        };
        records
            .into_iter()
            .filter_map(|record| match record.into_parts().0 {
                StoredAdministrationAuditRecordV1::Service(record) => Some(record),
                StoredAdministrationAuditRecordV1::Catalog(_)
                | StoredAdministrationAuditRecordV1::Capability(_) => None,
            })
            .collect()
    }
}

impl Drop for BudgetServiceHarness {
    fn drop(&mut self) {
        self.stop();
    }
}

struct BudgetDatabase {
    path: PathBuf,
    active: ActiveCatalogSnapshot,
    plans: BTreeMap<CommandId, ResolvedExecutablePlan>,
    binding: BudgetContractBinding,
}

impl BudgetDatabase {
    fn create() -> Self {
        let path = std::env::temp_dir().join(format!(
            "riffdb-wp125-{}-{}.redb",
            std::process::id(),
            NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut store = RedbStore::open(&path).expect("create comparison database");
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize comparison database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        let checked = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(BUDGET_SOURCE).expect("compile canonical budget contract"),
        )
        .expect("catalog validates canonical budget contract");
        let mut ports = open_operational(store);
        let stored = checked.to_stored().expect("encode checked budget bundle");
        let activated = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                stored.clone(),
                request_id(0x51),
                catalog_principal(),
                timestamp(BASE_SECONDS + 1),
                None,
            ))
            .expect("activate canonical budget contract");
        assert!(matches!(
            activated,
            CatalogActivationResult::Activated { active, .. }
                if active == ActiveCatalogPointerV1::from_bundle(&stored)
        ));
        let active = ActiveCatalogSnapshot::read(&ports)
            .expect("read active catalog")
            .expect("budget contract is active");
        let plans = active
            .bundle()
            .bundle()
            .commands()
            .iter()
            .map(|command| {
                let reference = ExecutablePlanRef::new(
                    active.bundle().lineage().clone(),
                    active.bundle().contract_version(),
                    active.bundle().bundle_hash(),
                    command.command_id(),
                    command.plan_hash(),
                );
                (
                    command.command_id(),
                    resolve_executable_plan(&ports, &reference)
                        .expect("resolve checked budget command"),
                )
            })
            .collect();
        let binding = budget_binding(active.bundle().bundle());
        drop(ports);
        Self {
            path,
            active,
            plans,
            binding,
        }
    }

    fn open(&self) -> RedbOperationalPorts {
        open_operational(RedbStore::open(&self.path).expect("open comparison database"))
    }
}

impl Drop for BudgetDatabase {
    fn drop(&mut self) {
        let _result = std::fs::remove_file(&self.path);
    }
}

fn budget_binding(bundle: &ContractBundle) -> BudgetContractBinding {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Budget")
        .expect("Budget entity");
    let create = command(bundle, "CreateBudget");
    let allocate = command(bundle, "AllocateBudget");
    BudgetContractBinding::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        entity.id(),
        BudgetEntityFields {
            organization_id: field(entity.record(), "organization_id"),
            fiscal_year: field(entity.record(), "fiscal_year"),
            approved_amount: field(entity.record(), "approved_amount"),
            allocated_amount: field(entity.record(), "allocated_amount"),
            updated_at: field(entity.record(), "updated_at"),
        },
        CreateBudgetInputFields {
            idempotency_key: field(create.input().record(), "idempotency_key"),
            organization_id: field(create.input().record(), "organization_id"),
            fiscal_year: field(create.input().record(), "fiscal_year"),
            approved_amount: field(create.input().record(), "approved_amount"),
        },
        AllocateBudgetInputFields {
            idempotency_key: field(allocate.input().record(), "idempotency_key"),
            organization_id: field(allocate.input().record(), "organization_id"),
            fiscal_year: field(allocate.input().record(), "fiscal_year"),
            matter_id: field(allocate.input().record(), "matter_id"),
            amount: field(allocate.input().record(), "amount"),
        },
        BudgetOutcomeFields {
            budget_created_budget: outcome_field(create, "BudgetCreated", "budget"),
            budget_exists_organization_id: outcome_field(
                create,
                "BudgetAlreadyExists",
                "organization_id",
            ),
            budget_exists_fiscal_year: outcome_field(create, "BudgetAlreadyExists", "fiscal_year"),
            invalid_approved_minimum: outcome_field(create, "InvalidApprovedAmount", "minimum"),
            budget_not_found_organization_id: outcome_field(
                allocate,
                "BudgetNotFound",
                "organization_id",
            ),
            budget_not_found_fiscal_year: outcome_field(allocate, "BudgetNotFound", "fiscal_year"),
            invalid_amount_minimum: outcome_field(allocate, "InvalidAmount", "minimum"),
            insufficient_approved: outcome_field(allocate, "InsufficientBudget", "approved"),
            insufficient_allocated: outcome_field(allocate, "InsufficientBudget", "allocated"),
            insufficient_requested: outcome_field(allocate, "InsufficientBudget", "requested"),
            allocated_budget: outcome_field(allocate, "Allocated", "budget"),
            allocated_remaining: outcome_field(allocate, "Allocated", "remaining"),
        },
    )
}

fn command<'a>(bundle: &'a ContractBundle, name: &str) -> &'a CommandPlan {
    bundle
        .commands()
        .iter()
        .find(|command| command.name() == name)
        .unwrap_or_else(|| panic!("missing command {name}"))
}

fn field(record: &RecordSchema, name: &str) -> FieldId {
    record
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .unwrap_or_else(|| panic!("missing field {name}"))
        .id()
}

fn outcome_field(command: &CommandPlan, outcome: &str, name: &str) -> FieldId {
    let record = command
        .outcomes()
        .iter()
        .find(|candidate| candidate.name() == outcome)
        .unwrap_or_else(|| panic!("missing outcome {outcome}"))
        .payload();
    field(record, name)
}

#[derive(Clone)]
struct BudgetCatalog {
    active: ActiveCatalogSnapshot,
    plans: BTreeMap<CommandId, ResolvedExecutablePlan>,
}

impl BudgetCatalog {
    fn new(
        active: ActiveCatalogSnapshot,
        plans: BTreeMap<CommandId, ResolvedExecutablePlan>,
    ) -> Self {
        Self { active, plans }
    }
}

impl CatalogReadPort for BudgetCatalog {
    fn prepare_active_catalog(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<'_, Option<ActiveCatalogSnapshot>, CatalogError> {
        let active = self.active.clone();
        Box::pin(async move { Ok(Some(active)) })
    }

    fn prepare_contract_version(
        &self,
        _control: &RequestControl,
        lineage: ContractLineage,
        version: ContractVersion,
    ) -> PortFuture<'_, Option<ValidatedContractBundle>, CatalogError> {
        let bundle = self.active.bundle().clone();
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
        BoxPortCapacityPermit<(), Option<ActiveCatalogSnapshot>, CatalogError>,
        PortAdmissionError,
    > {
        let active = self.active.clone();
        Box::pin(async move { Ok(Box::new(ActiveCatalogPermit(active)) as _) })
    }

    fn reserve_contract_version(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<'_, riffdb_service::ContractVersionReadPermit, PortAdmissionError> {
        let bundle = self.active.bundle().clone();
        Box::pin(async move { Ok(Box::new(ContractVersionPermit(bundle)) as _) })
    }

    fn executable_plan(
        &self,
        _control: &RequestControl,
        request: CatalogExecutablePlanRequest,
    ) -> PortFuture<'_, ResolvedExecutablePlan, CatalogError> {
        let resolved = self.plans.get(&request.command_id()).cloned();
        Box::pin(async move {
            resolved
                .filter(|plan| {
                    let reference = plan.reference();
                    request.lineage() == reference.contract_lineage()
                        && request.version() == reference.contract_version()
                        && request.bundle_hash() == reference.contract_bundle_hash()
                        && request.plan_hash() == reference.command_plan_hash()
                })
                .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))
        })
    }

    fn prepare_deployment(
        &self,
        _control: &RequestControl,
        _candidate: ContractBundle,
        _expected_active_version: Option<ContractVersion>,
    ) -> PortFuture<'_, CatalogPreparationResult, CatalogError> {
        Box::pin(async { Err(CatalogError::new(CatalogErrorKind::Storage)) })
    }
}

struct ActiveCatalogPermit(ActiveCatalogSnapshot);

impl PortCapacityPermit<(), Option<ActiveCatalogSnapshot>, CatalogError> for ActiveCatalogPermit {
    fn submit(
        self: Box<Self>,
        (): (),
    ) -> Result<PortReceipt<Option<ActiveCatalogSnapshot>, CatalogError>, PortAdmissionError> {
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(Some(self.0)));
        Ok(receipt)
    }
}

struct ContractVersionPermit(ValidatedContractBundle);

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
        let result = (self.0.lineage() == &request.0 && self.0.contract_version() == request.1)
            .then_some(self.0);
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(result));
        Ok(receipt)
    }
}

struct BudgetPolicy {
    fixture: AuthorizationFixture,
    audiences: TrustedAudienceCatalog,
    calls: AtomicUsize,
}

impl BudgetPolicy {
    fn new(active: &ActiveCatalogSnapshot) -> Self {
        let bundle = active.bundle().bundle();
        let entity = bundle
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Budget")
            .expect("Budget entity");
        let mut permissions = bundle
            .commands()
            .iter()
            .map(|command| {
                CapabilityPermissionV1::InvokeCommand(
                    bundle.lineage().clone(),
                    command.command_id(),
                )
            })
            .collect::<Vec<_>>();
        permissions.push(CapabilityPermissionV1::ReadEntity(
            bundle.lineage().clone(),
            entity.id(),
        ));
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(permissions).expect("comparison permissions"),
            vec![
                EntityFieldVisibilityV1::new(
                    bundle.lineage().clone(),
                    entity.id(),
                    vec![
                        field(entity.record(), "approved_amount"),
                        field(entity.record(), "allocated_amount"),
                        field(entity.record(), "updated_at"),
                    ],
                )
                .expect("comparison field visibility"),
            ],
            NonZeroU16::new(100).expect("nonzero row bound"),
            Vec::new(),
        )
        .expect("comparison capability grant");
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database_id(),
            environment(),
            ActorId::new("wp125-comparison-principal").expect("bounded principal"),
            ActorKind::Agent,
            audience(),
            AuthorizationFixtureTimes::new(
                timestamp(BASE_SECONDS),
                timestamp(BASE_SECONDS + 10_000),
                timestamp(BASE_SECONDS + 10),
            ),
            grant,
        ))
        .expect("comparison authorization fixture");
        Self {
            fixture,
            audiences: TrustedAudienceCatalog::new(vec![audience()])
                .expect("trusted comparison audience"),
            calls: AtomicUsize::new(0),
        }
    }

    fn principal(&self) -> AuthenticatedPrincipal {
        self.fixture.authenticated_principal().clone()
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }
}

impl riffdb_service::CurrentPolicyPort for BudgetPolicy {
    fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OperationRequest,
    ) -> Result<Decision, AuthorizationError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let resolver = self.fixture.current_capability_resolver();
        CurrentAuthorizer::new(
            &resolver,
            &FixedAuthorizationClock,
            &NoopAuthorizationTelemetry,
            database_id(),
            environment(),
        )
        .with_trusted_audience_catalog(&self.audiences)
        .authorize(principal, request)
    }
}

#[derive(Default)]
struct MirroredEntities(Mutex<HashMap<EntityKey, MirroredEntity>>);

#[derive(Clone)]
struct MirroredEntity {
    version: EntityVersion,
    written_by: ContractVersion,
    fields: CanonicalRecord,
}

impl MirroredEntities {
    fn observe(
        &self,
        binding: &BudgetContractBinding,
        result: &ExecuteCommandResult,
    ) -> Result<(), ()> {
        let ExecuteCommandResult::Journaled(result) = result else {
            return Err(());
        };
        if result.completion() == riffdb_service::JournaledCompletion::Replayed {
            return Ok(());
        }
        let nested_field = match result.outcome().outcome_name().as_str() {
            "BudgetCreated" => binding.outcome_fields().budget_created_budget,
            "Allocated" => binding.outcome_fields().allocated_budget,
            _ => return Ok(()),
        };
        let CanonicalValue::Record(fields) = record_field(result.outcome().value(), nested_field)?
        else {
            return Err(());
        };
        let entity = binding.entity_fields();
        let CanonicalValue::Uuid(organization) = record_field(fields, entity.organization_id)?
        else {
            return Err(());
        };
        let CanonicalValue::I64(fiscal_year) = record_field(fields, entity.fiscal_year)? else {
            return Err(());
        };
        let mut key = riffdb_types::EntityKeyBuilder::new(binding.budget_entity_type());
        key.push_uuid(organization)
            .and_then(|builder| builder.push_i64(*fiscal_year))
            .map_err(|_| ())?;
        let key = key.finish().map_err(|_| ())?;
        let mut state = self.0.lock().map_err(|_| ())?;
        let version = state.get(&key).map_or(EntityVersion::first(), |current| {
            current
                .version
                .checked_next()
                .expect("fixture entity version")
        });
        state.insert(
            key,
            MirroredEntity {
                version,
                written_by: result.contract_version(),
                fields: fields.clone(),
            },
        );
        Ok(())
    }
}

struct TrackingBudgetService {
    service: RiffDbService,
    entities: Arc<MirroredEntities>,
    binding: BudgetContractBinding,
}

impl CommandApplication for TrackingBudgetService {
    fn execute_command(
        &self,
        context: RequestContext,
        request: ExecuteCommandRequest,
    ) -> ServiceFuture<'_, ExecuteCommandResult> {
        let service = self.service.clone();
        let entities = Arc::clone(&self.entities);
        let binding = self.binding.clone();
        Box::pin(async move {
            let result = service.execute_command(context, request).await?;
            entities.observe(&binding, &result).map_err(|()| {
                riffdb_errors::PublicError::internal_defect(
                    IncidentId::from_bytes(uuid_bytes(0xee)).expect("incident UUIDv7"),
                )
            })?;
            Ok(result)
        })
    }

    fn resolve_command_outcome(
        &self,
        context: RequestContext,
        request: riffdb_service::ResolveCommandOutcomeRequest,
    ) -> ServiceFuture<'_, riffdb_service::ResolveCommandOutcomeResult> {
        self.service.resolve_command_outcome(context, request)
    }
}

impl QueryApplication for TrackingBudgetService {
    fn get_entity(
        &self,
        context: RequestContext,
        request: GetEntityRequest,
    ) -> ServiceFuture<'_, GetEntityResult> {
        self.service.get_entity(context, request)
    }

    fn scan_index(
        &self,
        context: RequestContext,
        request: ScanIndexRequest,
    ) -> ServiceFuture<'_, ScanIndexResult> {
        self.service.scan_index(context, request)
    }

    fn query_projection(
        &self,
        context: RequestContext,
        request: QueryProjectionRequest,
    ) -> ServiceFuture<'_, QueryProjectionResult> {
        self.service.query_projection(context, request)
    }

    fn get_projection_status(
        &self,
        context: RequestContext,
        request: GetProjectionStatusRequest,
    ) -> ServiceFuture<'_, GetProjectionStatusResult> {
        self.service.get_projection_status(context, request)
    }
}

struct BudgetAuthoritativeReads {
    entities: Arc<MirroredEntities>,
}

struct EntityReadPermit {
    entities: Arc<MirroredEntities>,
}

impl
    PortCapacityPermit<
        AuthoritativeEntityRequest,
        Option<AuthoritativeEntitySnapshot>,
        AuthoritativeReadError,
    > for EntityReadPermit
{
    fn submit(
        self: Box<Self>,
        request: AuthoritativeEntityRequest,
    ) -> Result<
        PortReceipt<Option<AuthoritativeEntitySnapshot>, AuthoritativeReadError>,
        PortAdmissionError,
    > {
        let result = self
            .entities
            .0
            .lock()
            .map_err(|_| PortAdmissionError::Unavailable)?
            .get(request.key())
            .cloned()
            .map(|entity| {
                AuthoritativeEntitySnapshot::new(
                    request.key().clone(),
                    entity.version,
                    entity.written_by,
                    entity.fields,
                )
            });
        let (sender, receipt) = port_completion_channel();
        sender.complete(Ok(result));
        Ok(receipt)
    }
}

impl AuthoritativeReadPort for BudgetAuthoritativeReads {
    fn reserve_read_entity(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeEntityRequest,
            Option<AuthoritativeEntitySnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        let entities = Arc::clone(&self.entities);
        Box::pin(async move { Ok(Box::new(EntityReadPermit { entities }) as _) })
    }

    fn reserve_scan_index(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeIndexRequest,
            AuthoritativeIndexPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        unavailable_port()
    }

    fn reserve_read_outcome(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeOutcomeRequest,
            Option<AuthoritativeOutcomeSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        unavailable_port()
    }

    fn reserve_read_commit(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            riffdb_types::CommitSequence,
            Option<AuthoritativeCommitSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        unavailable_port()
    }

    fn reserve_scan_commits(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeCommitScanRequest,
            AuthoritativeCommitPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        unavailable_port()
    }

    fn reserve_subscribe_to_commits(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeCommitSubscriptionRequest,
            Box<dyn CommitNotificationSource>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        unavailable_port()
    }

    fn reserve_trace_provenance(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            riffdb_policy::ProvenanceSelector,
            Option<AuthoritativeProvenanceSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        unavailable_port()
    }

    fn read_capability_revoke_target(
        &self,
        _control: &RequestControl,
        capability_id: CapabilityId,
    ) -> PortFuture<'_, CapabilityRevokeTargetSnapshot, AuthoritativeReadError> {
        Box::pin(async move {
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

fn unavailable_port<T>() -> PortFuture<'static, T, PortAdmissionError> {
    Box::pin(async { Err(PortAdmissionError::Unavailable) })
}

struct UnavailableProviders;

impl ProjectionQueryPort for UnavailableProviders {
    fn reserve_query_projection(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<ProjectionPortRequest, ProjectionPortResult, ProjectionPortError>,
        PortAdmissionError,
    > {
        unavailable_port()
    }

    fn reserve_projection_status(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            riffdb_types::ProjectionIdentity,
            Option<ProjectionStatusSnapshot>,
            ProjectionPortError,
        >,
        PortAdmissionError,
    > {
        unavailable_port()
    }
}

impl OperationalStatusPort for UnavailableProviders {
    fn reserve_health(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<(), OperationalHealthSnapshot, OperationalStatusError>,
        PortAdmissionError,
    > {
        unavailable_port()
    }

    fn reserve_statistics(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<(), OperationalStatisticsSnapshot, OperationalStatusError>,
        PortAdmissionError,
    > {
        unavailable_port()
    }
}

impl CapabilityTokenIssuer for UnavailableProviders {
    fn issue(&self) -> Result<NewlyIssuedCapabilityToken, CapabilityTokenIssueError> {
        Err(CapabilityTokenIssueError::Unavailable)
    }
}

struct ComparisonContexts {
    principal: AuthenticatedPrincipal,
    next: AtomicU64,
}

impl ComparisonContexts {
    fn new(principal: AuthenticatedPrincipal) -> Self {
        Self {
            principal,
            next: AtomicU64::new(1),
        }
    }
}

impl RequestContextFactory for ComparisonContexts {
    fn next_context(&self) -> RequestContext {
        let ordinal = self.next.fetch_add(1, Ordering::Relaxed);
        let (control, _cancellation): (RequestControl, RequestCancellationHandle) =
            RequestControl::new(Instant::now() + Duration::from_secs(30));
        RequestContext::new(
            request_id_from_ordinal(ordinal),
            self.principal.clone(),
            ServiceIngressKindV1::InProcessTestComparison,
            UntrustedInvocationClaims::new(None, None, None, None, None),
            control,
            None,
        )
    }
}

struct TokioSpawner(tokio::runtime::Handle);

impl ServiceJobSpawner for TokioSpawner {
    fn spawn(&self, job: ServiceJob) {
        let _task = self.0.spawn(job);
    }
}

struct TokioDeadlineScheduler(tokio::runtime::Handle);

impl RequestDeadlineScheduler for TokioDeadlineScheduler {
    fn wait_until(&self, deadline: Instant) -> RequestDeadlineFuture<'_> {
        let runtime = self.0.clone();
        Box::pin(async move {
            let sleep = {
                let _entered = runtime.enter();
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))
            };
            sleep.await;
        })
    }
}

#[derive(Default)]
struct SequentialCursorTokens(AtomicU64);

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

#[derive(Default)]
struct SequentialIncidentIds(AtomicU64);

impl IncidentIdSource for SequentialIncidentIds {
    fn next_incident_id(&self) -> Result<IncidentId, IncidentIdSourceError> {
        let ordinal = self.0.fetch_add(1, Ordering::Relaxed);
        IncidentId::from_bytes(uuid_bytes_from_ordinal(0x40, ordinal))
            .map_err(|_| IncidentIdSourceError)
    }
}

struct NoopDiagnostics;

impl ServiceDiagnostics for NoopDiagnostics {
    fn record_internal(&self, _error: InternalError) {}
}

struct NoopTelemetry;

impl ServiceTelemetry for NoopTelemetry {
    fn record(&self, _event: ServiceTelemetryEvent) {}
}

#[derive(Default)]
struct ReadinessRecorder(Mutex<Vec<AuthoritativeReadinessFailure>>);

impl ServiceHealthHooks for ReadinessRecorder {
    fn fail_authoritative_readiness(&self, reason: AuthoritativeReadinessFailure) {
        self.0.lock().expect("readiness recorder").push(reason);
    }
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
    let mut session = store
        .begin_structural_evidence(startup_inputs())
        .expect("begin structural validation");
    let database = session.database_id();
    let open_session = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("evidence page bound");
    let mut cursor = StructuralEvidenceCursor::start(database, open_session);
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty());
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (history, historical_end) = validate_catalog_history(&mut session)
        .expect("validate catalog history")
        .into_parts();
    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural validation");
    let history = match history {
        CatalogHistoryOutcome::Ready(history) => Ok(history),
        CatalogHistoryOutcome::MigrationRequired(context) => Err(context),
    };
    let opened = match opened {
        StructuralOpenOutcome::Clean(opened) => Ok(opened),
        StructuralOpenOutcome::MigrationRequired(port) => Err(port),
    };
    let (history, opened) = join_v2_only_startup(history, opened)
        .expect("comparison fixture requires matching Ready and Clean startup outcomes");
    assert!(history.matches(opened.database_id(), opened.open_session_id()));
    let (_, _, _, dormant): (_, _, _, RedbDormantPorts) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate validated redb ports")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ComparisonStartupJoinError {
    MigrationRequired,
    CrossedOutcomes,
}

fn join_v2_only_startup<CatalogReady, CatalogMigration, StorageClean, StorageMigration>(
    catalog: Result<CatalogReady, CatalogMigration>,
    storage: Result<StorageClean, StorageMigration>,
) -> Result<(CatalogReady, StorageClean), ComparisonStartupJoinError> {
    match (catalog, storage) {
        (Ok(catalog), Ok(storage)) => Ok((catalog, storage)),
        (Err(_), Err(_)) => Err(ComparisonStartupJoinError::MigrationRequired),
        (Ok(_), Err(_)) | (Err(_), Ok(_)) => Err(ComparisonStartupJoinError::CrossedOutcomes),
    }
}

#[test]
fn service_comparison_startup_accepts_only_ready_with_clean() {
    assert_eq!(
        join_v2_only_startup::<_, (), _, ()>(Ok("ready"), Ok("clean")),
        Ok(("ready", "clean"))
    );
    assert_eq!(
        join_v2_only_startup::<(), _, (), _>(Err("catalog migration"), Err("storage migration")),
        Err(ComparisonStartupJoinError::MigrationRequired)
    );
    assert_eq!(
        join_v2_only_startup::<_, (), (), _>(Ok("ready"), Err("storage migration")),
        Err(ComparisonStartupJoinError::CrossedOutcomes)
    );
    assert_eq!(
        join_v2_only_startup::<(), _, _, ()>(Err("catalog migration"), Ok("clean")),
        Err(ComparisonStartupJoinError::CrossedOutcomes)
    );
}

fn startup_inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
    StartupValidationInputs::new(
        timestamp(BASE_SECONDS),
        ReadableCapabilityDigestInventory::new(vec![key]).expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency digest inventory"),
    )
}

fn start_coordinator(
    ports: RedbOperationalPorts,
    durability: CoordinatorDurability,
    workload_capacity: u16,
) -> RunningCommandCoordinator {
    let conflicts: Arc<dyn ConflictManager> = Arc::new(
        ShardedConflictManager::new(ConflictManagerConfig::default())
            .expect("comparison conflict manager"),
    );
    RunningCommandCoordinator::start(
        CoordinatorWorkloadCapacity::new(workload_capacity).expect("coordinator capacity"),
        durability,
        ports,
        conflicts,
        Arc::new(FixedAdmissionClock),
        Arc::new(IncrementingAdministrationClock::new()),
        Arc::new(FixedAuthorizationClock),
        Arc::new(SequentialProvenanceIds::default()),
        Arc::new(DiscardApplicationCommitNotifications),
    )
    .expect("start comparison coordinator")
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

impl ProvenanceIdSource for SequentialProvenanceIds {
    fn next_provenance_id(&self) -> Result<ProvenanceId, ProvenanceIdSourceError> {
        let ordinal = self.0.fetch_add(1, Ordering::Relaxed);
        ProvenanceId::from_bytes(uuid_bytes_from_ordinal(0x70, ordinal))
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

fn catalog_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("wp125-catalog-owner").expect("catalog actor"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(0x52)).expect("capability UUIDv7"),
        NonZeroU64::MIN,
    )
}

fn database_id() -> DatabaseId {
    DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database UUIDv7")
}

fn environment() -> Environment {
    Environment::new("wp125-test").expect("bounded environment")
}

fn audience() -> Audience {
    Audience::new("riffdb-budget-service-comparison").expect("bounded audience")
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("canonical timestamp")
}

fn request_id(seed: u8) -> RequestId {
    RequestId::from_bytes(uuid_bytes(seed)).expect("request UUIDv7")
}

fn request_id_from_ordinal(ordinal: u64) -> RequestId {
    RequestId::from_bytes(uuid_bytes_from_ordinal(0x60, ordinal)).expect("request UUIDv7")
}

fn uuid_bytes(seed: u8) -> [u8; 16] {
    uuid_bytes_from_ordinal(seed, u64::from(seed))
}

fn uuid_bytes_from_ordinal(seed: u8, ordinal: u64) -> [u8; 16] {
    let mut bytes = [seed; 16];
    bytes[6] = 0x70 | (seed & 0x0f);
    bytes[8] = 0x80 | (seed & 0x3f);
    bytes[8..].copy_from_slice(&ordinal.to_be_bytes());
    bytes[8] = 0x80 | (bytes[8] & 0x3f);
    bytes
}

fn record_field(record: &CanonicalRecord, id: FieldId) -> Result<&CanonicalValue, ()> {
    record
        .fields()
        .binary_search_by_key(&id, |(candidate, _)| *candidate)
        .ok()
        .and_then(|index| record.fields().get(index))
        .map(|(_, value)| value)
        .ok_or(())
}

fn run_async(output: impl Future<Output = ()> + Send + 'static) {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .expect("build comparison runtime")
        .block_on(output);
}
