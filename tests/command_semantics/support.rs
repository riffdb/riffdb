#![allow(dead_code)]

//! Shared real-redb fixtures for command-semantic integration tests.

use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroU64};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use riffdb_catalog::{ValidatedContractBundle, resolve_executable_plan, validate_catalog_history};
use riffdb_commit::{
    AdministrationClock, AdministrationClockError, AdmissionClock, AdmissionClockError,
    ApplicationCommitNotificationError, ApplicationCommitNotificationSink,
    CommandExecutionPreparation, CommandRequestControl, CoordinatorDurability,
    CoordinatorWorkloadCapacity, ProvenanceIdSource, ProvenanceIdSourceError,
    RunningCommandCoordinator,
};
use riffdb_conflict::{ConflictManager, ConflictManagerConfig, ShardedConflictManager};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{CommandPlan, RecordSchema};
use riffdb_idempotency::{
    CommandIdempotencyScopeV1, IdempotencyDigestCandidatesV1, IdempotencyDigestError,
    IdempotencyDigestProvider, IdempotencyInspectionExecutor, prepare_idempotency_lookup,
};
use riffdb_invariant::derive_input_command_facts;
use riffdb_policy::{
    AgentSessionAdmissionPolicy, AuthorizationClock, AuthorizationClockError,
    AuthorizedCommandExecution, CommandExecutionClass, CurrentAuthorizer, Decision,
    NoopAuthorizationTelemetry, OperationRequest, UntrustedInvocationClaims,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuditPrincipalV1, AuthoritativePointReader, CatalogActivationIntentV1,
    CatalogActivationResult, CatalogAdministrationRepository, DatabaseInitializationPort,
    EvidencePageLimit, ExecutablePlanRef, IdempotencyKeyDigest, ReadableCapabilityDigestInventory,
    ReadableDigestKey, ReadableIdempotencyDigestInventory, StartupValidationInputs,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession,
};
use riffdb_storage_redb::{RedbDormantPorts, RedbOperationalPorts, RedbStore, RedbTestController};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CanonicalRecord, CanonicalValue, CapabilityGrantV1, CapabilityId,
    CapabilityPermissionV1, CapabilityPermissionsV1, CommitSequence, ContractLineage, DatabaseId,
    Decimal, DecimalSpec, DigestKeyId, EntityKeyBuilder, EntityTypeId, EntityVersion, Environment,
    FieldId, IdempotencyKey, PartitionKey, PartitionScopeV1, ProvenanceId, RequestId, TenantScope,
    Timestamp,
};

const BUDGET_SOURCE: &str = include_str!("../../contracts/examples/budget.riff");
const PRINCIPAL: &str = "command-semantics-agent";
const CALLER_KEY: &str = "command-semantics-idempotency-key";
const ORGANIZATION_ID: [u8; 16] = [0x31; 16];
const FISCAL_YEAR: i64 = 2026;
const ENVIRONMENT: &str = "integration";
static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

pub(crate) struct BudgetDatabase {
    path: PathBuf,
    checked_bundle: ValidatedContractBundle,
    target_entity_type: EntityTypeId,
    approved_amount_field: FieldId,
}

impl BudgetDatabase {
    pub(crate) fn create(label: &str) -> Self {
        let ordinal = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "riffdb-command-semantics-{label}-{}-{ordinal}.redb",
            std::process::id()
        ));
        let compiled = compile_contract_source(BUDGET_SOURCE).expect("budget contract compiles");
        let checked_bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("budget bundle passes catalog validation");
        let budget = checked_bundle
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Budget")
            .expect("Budget entity schema");
        let approved_amount_field = budget
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "approved_amount")
            .expect("approved amount field")
            .id();
        let target_entity_type = budget.id();

        let mut store = RedbStore::open(&path).expect("create redb command database");
        store
            .initialize_database(database_id())
            .expect("initialize command database");
        let mut ports = open_operational(store);
        let stored_bundle = checked_bundle.to_stored().expect("stored checked bundle");
        let activation = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                stored_bundle.clone(),
                request_id(0x61),
                catalog_principal(),
                timestamp(1_700_000_000),
                None,
            ))
            .expect("activate checked budget bundle");
        assert!(matches!(
            activation,
            CatalogActivationResult::Activated { active, .. }
                if active == ActiveCatalogPointerV1::from_bundle(&stored_bundle)
        ));
        drop(ports);

        Self {
            path,
            checked_bundle,
            target_entity_type,
            approved_amount_field,
        }
    }

    pub(crate) fn open(&self) -> RedbOperationalPorts {
        open_operational(RedbStore::open(&self.path).expect("reopen command database"))
    }

    pub(crate) fn open_with_controller(
        &self,
        controller: RedbTestController,
    ) -> RedbOperationalPorts {
        open_operational(
            RedbStore::open_with_test_controller(&self.path, controller)
                .expect("reopen command database with test controller"),
        )
    }

    pub(crate) fn prepare(
        &self,
        ports: &RedbOperationalPorts,
        approved_amount: i128,
        request_seed: u8,
    ) -> CommandExecutionPreparation {
        let plan = self.command_plan();
        let reference = ExecutablePlanRef::new(
            self.checked_bundle.lineage().clone(),
            self.checked_bundle.contract_version(),
            self.checked_bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let resolved = resolve_executable_plan(ports, &reference)
            .expect("deployed CreateBudget plan resolves");
        let input = normalized_input(plan, approved_amount);
        let facts = derive_input_command_facts(plan, input.clone())
            .expect("checked CreateBudget input facts");
        let caller_key = IdempotencyKey::new(CALLER_KEY).expect("bounded caller key");
        let scope = CommandIdempotencyScopeV1::new(
            database_id(),
            environment(),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("bounded principal"),
            reference.contract_lineage().clone(),
            reference.command_id(),
        );
        let lookup = prepare_idempotency_lookup(&scope, &caller_key, &FixedDigestProvider)
            .expect("prepare caller-key lookup");
        let idempotency = IdempotencyInspectionExecutor::new(ports)
            .inspect(lookup)
            .expect("inspect durable idempotency state")
            .confirm_input(
                &input,
                plan.idempotency_input()
                    .expect("CreateBudget idempotency input"),
                &caller_key,
            )
            .expect("confirm canonical command input")
            .bind_selected_plan(reference)
            .expect("bind selected historical command plan");
        let authorization = authorize_command(
            plan,
            self.checked_bundle.lineage().clone(),
            facts.partition_key().clone(),
        );
        let (control, _cancellation) = CommandRequestControl::new(
            Instant::now()
                .checked_add(Duration::from_secs(30))
                .expect("representable command deadline"),
        );

        CommandExecutionPreparation::new(
            database_id(),
            &environment(),
            resolved,
            input,
            idempotency,
            facts,
            authorization,
            request_id(request_seed),
            control,
        )
        .expect("join exact command preparation proofs")
    }

    pub(crate) fn assert_one_budget_commit(
        &self,
        ports: &RedbOperationalPorts,
        outcome: &riffdb_storage_api::StoredOutcomeV1,
        approved_amount: i128,
    ) {
        let entity = ports
            .read_entity(&self.entity_target())
            .expect("query committed budget")
            .expect("one budget row");
        assert_eq!(entity.entity_version(), EntityVersion::first());
        let approved = entity
            .fields()
            .fields()
            .iter()
            .find(|(field, _)| *field == self.approved_amount_field)
            .map(|(_, value)| value)
            .expect("committed approved amount");
        assert_eq!(approved, &decimal(approved_amount));

        assert_eq!(
            ports
                .read_stored_outcome(outcome.identity())
                .expect("query stored outcome"),
            Some(outcome.clone())
        );
        let commit = ports
            .read_commit(CommitSequence::first())
            .expect("query first commit")
            .expect("first commit exists");
        assert_eq!(commit.provenance_id(), outcome.provenance_id());
        assert_eq!(commit.durability_mode(), outcome.durability_mode());
        assert_eq!(
            ports
                .read_commit(
                    CommitSequence::first()
                        .checked_next()
                        .expect("second commit sequence"),
                )
                .expect("query second commit"),
            None,
            "replay or input mismatch must not allocate another sequence"
        );
        let provenance = ports
            .read_provenance(outcome.provenance_id())
            .expect("query provenance")
            .expect("first commit provenance exists");
        assert_eq!(provenance.commit_sequence(), CommitSequence::first());
    }

    fn command_plan(&self) -> &CommandPlan {
        self.checked_bundle
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == "CreateBudget")
            .expect("CreateBudget command plan")
    }

    fn entity_target(&self) -> riffdb_storage_api::EntityTarget {
        let mut key = EntityKeyBuilder::new(self.target_entity_type);
        key.push_uuid(&ORGANIZATION_ID)
            .expect("organization key component");
        key.push_i64(FISCAL_YEAR)
            .expect("fiscal-year key component");
        riffdb_storage_api::EntityTarget::new(
            self.target_entity_type,
            key.finish().expect("budget entity key"),
        )
        .expect("Budget entity target")
    }
}

impl Drop for BudgetDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(crate) struct FixedAdmissionClock {
    value: Timestamp,
    calls: AtomicUsize,
}

impl FixedAdmissionClock {
    pub(crate) fn new(value: Timestamp) -> Self {
        Self {
            value,
            calls: AtomicUsize::new(0),
        }
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl AdmissionClock for FixedAdmissionClock {
    fn now(&self) -> Result<Timestamp, AdmissionClockError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.value)
    }
}

pub(crate) struct FixedAdministrationClock(Timestamp);

impl AdministrationClock for FixedAdministrationClock {
    fn now(&self) -> Result<Timestamp, AdministrationClockError> {
        Ok(self.0)
    }
}

pub(crate) struct CountingProvenanceSource {
    value: ProvenanceId,
    calls: AtomicUsize,
}

impl CountingProvenanceSource {
    pub(crate) fn new(seed: u8) -> Self {
        Self {
            value: ProvenanceId::from_bytes(uuid_bytes(seed)).expect("provenance UUIDv7"),
            calls: AtomicUsize::new(0),
        }
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl ProvenanceIdSource for CountingProvenanceSource {
    fn next_provenance_id(&self) -> Result<ProvenanceId, ProvenanceIdSourceError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.value)
    }
}

pub(crate) fn start_coordinator(
    ports: RedbOperationalPorts,
    admission_clock: Arc<FixedAdmissionClock>,
    provenance_source: Arc<CountingProvenanceSource>,
) -> RunningCommandCoordinator {
    start_coordinator_with_notifications(
        ports,
        admission_clock,
        provenance_source,
        Arc::new(DiscardApplicationCommitNotifications),
    )
}

pub(crate) fn start_coordinator_with_notifications(
    ports: RedbOperationalPorts,
    admission_clock: Arc<FixedAdmissionClock>,
    provenance_source: Arc<CountingProvenanceSource>,
    notifications: Arc<dyn ApplicationCommitNotificationSink>,
) -> RunningCommandCoordinator {
    let conflicts: Arc<dyn ConflictManager> = Arc::new(
        ShardedConflictManager::new(ConflictManagerConfig::default())
            .expect("start conflict manager"),
    );
    let admission_clock: Arc<dyn AdmissionClock> = admission_clock;
    let administration_clock: Arc<dyn AdministrationClock> =
        Arc::new(FixedAdministrationClock(timestamp(1_700_000_002)));
    let authorization_clock: Arc<dyn AuthorizationClock> =
        Arc::new(FixedAuthorizationClock(timestamp(1_700_000_002)));
    let provenance_source: Arc<dyn ProvenanceIdSource> = provenance_source;
    RunningCommandCoordinator::start(
        CoordinatorWorkloadCapacity::new(8).expect("nonzero coordinator capacity"),
        CoordinatorDurability::Sync,
        ports,
        conflicts,
        admission_clock,
        administration_clock,
        authorization_clock,
        provenance_source,
        notifications,
    )
    .expect("start command coordinator")
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

#[derive(Default)]
pub(crate) struct RecordingApplicationCommitNotifications(Mutex<Vec<CommitSequence>>);

impl RecordingApplicationCommitNotifications {
    pub(crate) fn sequences(&self) -> Vec<CommitSequence> {
        self.0.lock().expect("notification lock").clone()
    }
}

impl ApplicationCommitNotificationSink for RecordingApplicationCommitNotifications {
    fn publish_first_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<(), ApplicationCommitNotificationError> {
        self.0.lock().expect("notification lock").push(sequence);
        Ok(())
    }
}

pub(crate) struct FailingApplicationCommitNotifications;

impl ApplicationCommitNotificationSink for FailingApplicationCommitNotifications {
    fn publish_first_commit(
        &self,
        _: CommitSequence,
    ) -> Result<(), ApplicationCommitNotificationError> {
        Err(ApplicationCommitNotificationError)
    }
}

pub(crate) struct PanickingApplicationCommitNotifications;

impl ApplicationCommitNotificationSink for PanickingApplicationCommitNotifications {
    fn publish_first_commit(
        &self,
        _: CommitSequence,
    ) -> Result<(), ApplicationCommitNotificationError> {
        panic!("injected notification panic")
    }
}

pub(crate) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build command test runtime")
}

pub(crate) fn command_timestamp() -> Timestamp {
    timestamp(1_700_000_001)
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
    let opened = complete_structural_open(store);
    let (_, _, _, dormant) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate structurally checked redb ports")
}

fn complete_structural_open(
    store: RedbStore,
) -> riffdb_storage_api::StructurallyOpened<RedbDormantPorts> {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        timestamp(1_700_000_000),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin structural validation");
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("evidence page limit");

    let mut structural_cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural_cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty(), "valid fixture has no findings");
                structural_cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };

    let (history, historical_end) = validate_catalog_history(&mut session)
        .expect("validate exact catalog history")
        .into_parts();
    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural validation");
    assert!(
        history.matches(opened.database_id(), opened.open_session_id()),
        "catalog proof must belong to the exact structural-open session"
    );
    opened
}

fn normalized_input(plan: &CommandPlan, approved_amount: i128) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string(CALLER_KEY).expect("bounded idempotency key"),
            ),
            ("organization_id", CanonicalValue::Uuid(ORGANIZATION_ID)),
            ("fiscal_year", CanonicalValue::I64(FISCAL_YEAR)),
            ("approved_amount", decimal(approved_amount)),
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
                        .unwrap_or_else(|| panic!("missing field {}", field.name()))
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

fn authorize_command(
    plan: &CommandPlan,
    lineage: ContractLineage,
    partition: PartitionKey,
) -> AuthorizedCommandExecution {
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![CapabilityPermissionV1::InvokeCommand(
            lineage.clone(),
            plan.command_id(),
        )])
        .expect("command permission"),
        Vec::new(),
        NonZeroU16::new(10).expect("nonzero row bound"),
        Vec::new(),
    )
    .expect("valid capability grant");
    let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database_id(),
        environment(),
        ActorId::new(PRINCIPAL).expect("bounded principal"),
        ActorKind::Agent,
        Audience::new("riffdb-command-semantics").expect("bounded audience"),
        AuthorizationFixtureTimes::new(timestamp(100), timestamp(300), timestamp(150)),
        grant,
    ))
    .expect("authorization fixture");
    let resolver = fixture.current_capability_resolver();
    let clock = FixedAuthorizationClock(timestamp(200));
    let decision = CurrentAuthorizer::new(
        &resolver,
        &clock,
        &NoopAuthorizationTelemetry,
        database_id(),
        environment(),
    )
    .authorize(
        fixture.authenticated_principal(),
        OperationRequest::execute_command(
            lineage,
            plan.contract_version(),
            plan.command_id(),
            CommandExecutionClass::Mutation,
            partition,
        ),
    )
    .expect("authorize CreateBudget");
    let Decision::Allow(proof) = decision else {
        panic!("CreateBudget fixture must be authorized");
    };
    proof
        .into_command_execution(
            UntrustedInvocationClaims::new(None, None, None, None, None),
            AgentSessionAdmissionPolicy::Discard,
        )
        .expect("exact command authorization")
}

struct FixedAuthorizationClock(Timestamp);

impl AuthorizationClock for FixedAuthorizationClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(self.0)
    }
}

struct FixedDigestProvider;

impl IdempotencyDigestProvider for FixedDigestProvider {
    fn digest_candidates(
        &self,
        _: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
        IdempotencyDigestCandidatesV1::new(vec![IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key ID"),
            [0x51; 32],
        )])
    }
}

fn catalog_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("command-semantics-maintainer").expect("catalog principal"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(0x62)).expect("catalog capability UUIDv7"),
        NonZeroU64::MIN,
    )
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
        .expect("database UUIDv7")
}

fn environment() -> Environment {
    Environment::new(ENVIRONMENT).expect("bounded environment")
}

fn request_id(seed: u8) -> RequestId {
    RequestId::from_bytes(uuid_bytes(seed)).expect("request UUIDv7")
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("canonical timestamp")
}

fn uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}
