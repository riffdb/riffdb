#![allow(dead_code)]

//! Support harness for WP-684 / ADR-0212 correlated-index model, crash/reopen,
//! and real `riffdbd` remote loopback proofs.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::num::{NonZeroU16, NonZeroU64};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str;
use std::time::{Duration, Instant};

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_catalog::{ValidatedContractBundle, resolve_executable_plan};
use riffdb_client_rust::generated::{GeneratedCommand, GeneratedCommandError};
use riffdb_client_rust::{
    AttemptBudget, BearerCredential, BootstrapCallMetadata,
    BootstrapCredential as TransportBootstrapCredential, CallMetadata, IdempotentCommand,
    RiffDbClient, generate_request_id, v1,
};
use riffdb_commit::{
    AdministrationAuditInputView, CommandExecutionPreparation, CommandExecutionResult,
    CommandRequestControl, CommittedOutcome, PostEvaluationAuthorizationError,
    PostEvaluationCommandAuthorizer,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::CommandPlan;
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
use riffdb_proto::canonical_value_to_proto;
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuditPrincipalV1, AuthoritativePointReader, CatalogActivationIntentV1,
    CatalogActivationResult, CatalogAdministrationRepository, DatabaseInitializationPort,
    ExecutablePlanRef, IdempotencyKeyDigest, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, StartupValidationInputs,
};
use riffdb_storage_redb::{RedbOperationalPorts, RedbStore, RedbTestController};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_testkit::inspection::{
    CorrelatedCorpusNamespace, CorrelatedNamespaceInspectionSession,
    open_correlated_redb_inspection,
};
use riffdb_testkit::model::correlated_index::{
    CorrelatedIndexWorkModel, CorrelatedIndexedMutationRow, CorrelatedStoreAgreement,
    indexed_mutation_bulk_bundle, verify_correlated_index_work_against_redb,
};
use riffdb_testkit::scratch::ScratchDir;
use riffdb_testkit_server::process::{ChildProcessController, ChildProcessSpec};
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, Audience, CanonicalRecord, CanonicalValue, CapabilityGrantV1,
    CapabilityId, CapabilityPermissionV1, CapabilityPermissionsV1, ContractLineage, DatabaseId,
    DigestKeyId, Environment, IdempotencyKey, PartitionKey, PartitionScopeV1, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    ServiceOperationV1, TenantScope, Timestamp,
};
use tonic::transport::Endpoint;

use super::support::{
    CountingProvenanceSource, FixedAdmissionClock, command_timestamp, open_operational, runtime,
    start_coordinator,
};

const INDEXED_MUTATIONS_SOURCE: &str =
    include_str!("../fixtures/contracts/bulk/indexed-mutations.riff");
const PRINCIPAL: &str = "correlated-index-agent";
const ENVIRONMENT: &str = "integration";
const REMOTE_AUDIENCE: &str = "riffdb-correlated-index-loopback";
const REMOTE_ENVIRONMENT: &str = "correlated-index-remote";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const START_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(30);
const RPC_TIMEOUT: Duration = Duration::from_secs(20);
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_000_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;
const CAPABILITY_KEY_DOCUMENT: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] = b"riffdb-idempotency-digest-keys-v1\n1:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";

pub(crate) struct IndexedMutationBulkDatabase {
    path: PathBuf,
    checked_bundle: ValidatedContractBundle,
    _scratch: Option<ScratchDir>,
}

impl IndexedMutationBulkDatabase {
    pub(crate) fn create(label: &str) -> Self {
        let scratch = ScratchDir::new(&format!("indexed-mutations-{label}"))
            .expect("create indexed-mutations scratch directory");
        let path = scratch.join("db.redb");
        let database = Self::from_path(path, Some(scratch));
        let mut store =
            RedbStore::open(&database.path).expect("create redb indexed-mutations database");
        store
            .initialize_database(database_id())
            .expect("initialize indexed-mutations database");
        let mut ports = open_operational(store);
        let stored_bundle = database
            .checked_bundle
            .to_stored()
            .expect("stored checked indexed-mutations bundle");
        let activation = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                stored_bundle.clone(),
                request_id(0x61),
                catalog_principal(),
                timestamp(1_700_000_000),
                None,
            ))
            .expect("activate checked indexed-mutations bundle");
        assert!(matches!(
            activation,
            CatalogActivationResult::Activated { active, .. }
                if active == ActiveCatalogPointerV1::from_bundle(&stored_bundle)
        ));
        drop(ports);
        database
    }

    pub(crate) fn attach(path: &Path) -> Self {
        Self::from_path(path.to_path_buf(), None)
    }

    fn from_path(path: PathBuf, scratch: Option<ScratchDir>) -> Self {
        let compiled = compile_contract_source(INDEXED_MUTATIONS_SOURCE)
            .expect("IndexedMutationBulk contract compiles");
        let checked_bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("IndexedMutationBulk bundle passes catalog validation");
        Self {
            path,
            checked_bundle,
            _scratch: scratch,
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn open(&self) -> RedbOperationalPorts {
        open_operational(
            RedbStore::open(&self.path).expect("reopen indexed-mutations redb database"),
        )
    }

    pub(crate) fn open_with_controller(
        &self,
        controller: RedbTestController,
    ) -> RedbOperationalPorts {
        open_operational(
            RedbStore::open_with_test_controller(&self.path, controller)
                .expect("reopen indexed-mutations redb database with test controller"),
        )
    }

    pub(crate) fn open_inspection(
        &self,
        namespace: CorrelatedCorpusNamespace,
    ) -> CorrelatedNamespaceInspectionSession {
        open_correlated_redb_inspection(&self.path, correlated_startup_inputs(), namespace)
            .expect("open stopped redb correlated-index inspection session")
    }

    pub(crate) fn prepare_seed(
        &self,
        ports: &RedbOperationalPorts,
        rows: &[CorrelatedIndexedMutationRow],
        input_request_seed: u8,
        digest_seed: u8,
        admission_seed: u8,
    ) -> CommandExecutionPreparation {
        let plan = self.command_plan("SeedInitialIndexedStateRows");
        let input = build_mutation_command_input(plan.name(), input_request_seed, rows);
        let caller_key = canonical_uuid_text(input_request_seed);
        self.prepare_command(ports, plan, input, &caller_key, digest_seed, admission_seed)
    }

    pub(crate) fn prepare_apply(
        &self,
        ports: &RedbOperationalPorts,
        rows: &[CorrelatedIndexedMutationRow],
        input_request_seed: u8,
        digest_seed: u8,
        admission_seed: u8,
    ) -> CommandExecutionPreparation {
        let plan = self.command_plan("ApplyIndexedMutations");
        let input = build_mutation_command_input(plan.name(), input_request_seed, rows);
        let caller_key = canonical_uuid_text(input_request_seed);
        self.prepare_command(ports, plan, input, &caller_key, digest_seed, admission_seed)
    }

    pub(crate) fn execute_committed(
        &self,
        preparation: CommandExecutionPreparation,
        ports: RedbOperationalPorts,
        provenance_seed: u8,
    ) -> CommittedOutcome {
        let coordinator = start_coordinator(
            ports,
            std::sync::Arc::new(FixedAdmissionClock::new(command_timestamp())),
            std::sync::Arc::new(CountingProvenanceSource::new(provenance_seed)),
        );
        let executor = coordinator.command_executor();
        let result = runtime().block_on(async {
            executor
                .reserve_capacity()
                .await
                .expect("reserve correlated-index command")
                .submit(preparation)
                .expect("submit correlated-index command")
                .completion()
                .await
                .expect("complete correlated-index command")
        });
        drop(executor);
        coordinator
            .shutdown()
            .expect("drain correlated-index coordinator");
        let CommandExecutionResult::Committed(outcome) = result else {
            panic!("correlated-index command must commit");
        };
        outcome
    }

    pub(crate) fn assert_commit_graph(
        &self,
        ports: &RedbOperationalPorts,
        outcome: &riffdb_storage_api::StoredOutcomeV1,
        expected_entity_refs: usize,
        expected_events: usize,
    ) {
        assert_eq!(
            ports
                .read_stored_outcome(outcome.identity())
                .expect("read stored outcome"),
            Some(outcome.clone())
        );
        let commit = ports
            .read_commit(outcome.commit_sequence())
            .expect("read commit")
            .expect("commit exists");
        assert_eq!(commit.declared_outcome(), outcome.declared_outcome());
        assert_eq!(commit.provenance_id(), outcome.provenance_id());
        assert_eq!(commit.entity_references().len(), expected_entity_refs);
        assert_eq!(commit.events().len(), expected_events);
        for event in commit.events() {
            assert_eq!(
                ports
                    .read_durable_event(event.event_id())
                    .expect("read durable event"),
                Some(event.clone())
            );
        }
        assert!(
            ports
                .read_provenance(outcome.provenance_id())
                .expect("read provenance")
                .is_some()
        );
    }

    fn command_plan(&self, name: &str) -> &CommandPlan {
        self.checked_bundle
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == name)
            .unwrap_or_else(|| panic!("{name} command plan"))
    }

    fn prepare_command(
        &self,
        ports: &RedbOperationalPorts,
        plan: &CommandPlan,
        input: CanonicalRecord,
        caller_key_text: &str,
        digest_seed: u8,
        admission_request_seed: u8,
    ) -> CommandExecutionPreparation {
        let reference = ExecutablePlanRef::new(
            self.checked_bundle.lineage().clone(),
            self.checked_bundle.contract_version(),
            self.checked_bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let resolved = resolve_executable_plan(ports, &reference)
            .expect("deployed IndexedMutationBulk command resolves");
        let facts = derive_input_command_facts(plan, input.clone())
            .expect("checked IndexedMutationBulk command input facts");
        let caller_key = IdempotencyKey::new(caller_key_text).expect("bounded caller key");
        let scope = CommandIdempotencyScopeV1::new(
            database_id(),
            environment(),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("bounded principal"),
            reference.contract_lineage().clone(),
            reference.command_id(),
        );
        let lookup =
            prepare_idempotency_lookup(&scope, &caller_key, &FixedDigestProvider(digest_seed))
                .expect("prepare caller-key lookup");
        let idempotency = IdempotencyInspectionExecutor::new(ports)
            .inspect(lookup)
            .expect("inspect idempotency state")
            .confirm_input(
                &input,
                plan.idempotency_input()
                    .expect("IndexedMutationBulk command idempotency input"),
                &caller_key,
            )
            .expect("confirm command input")
            .bind_selected_plan(reference)
            .expect("bind command plan");
        let authorization = authorize_command(
            plan,
            self.checked_bundle.lineage().clone(),
            facts.partition_key().clone(),
        );
        let post_evaluation_authorization = FixedPostEvaluationCommandAuthorizer::new(
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
            request_id(admission_request_seed),
            ServiceIngressKindV1::Grpc,
            control,
        )
        .expect("join exact command preparation proofs")
        .with_audited_lifecycle(Box::new(StartedCommandAuditInput::new(request_id(
            admission_request_seed,
        ))))
        .expect("attach exact audit lifecycle")
        .with_post_evaluation_authorizer(Box::new(post_evaluation_authorization))
        .expect("attach exact post-evaluation authorization")
    }
}

pub(crate) fn correlated_startup_inputs() -> StartupValidationInputs {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    StartupValidationInputs::new(
        timestamp(1_700_000_000),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    )
}

fn build_mutation_command_input(
    command_name: &str,
    input_request_seed: u8,
    rows: &[CorrelatedIndexedMutationRow],
) -> CanonicalRecord {
    let partition_id = rows
        .first()
        .map(|row| row.partition_id.as_str())
        .unwrap_or("tenant-default");
    CorrelatedIndexWorkModel::new(partition_id).encode_command_input(
        command_name,
        uuid_bytes(input_request_seed),
        rows,
    )
}

fn canonical_uuid_text(seed: u8) -> String {
    let bytes = uuid_bytes(seed);
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

struct StartedCommandAuditInput {
    request_id: RequestId,
    principal_id: ActorId,
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
    targets: ServiceAuditTargetsV1,
}

impl StartedCommandAuditInput {
    fn new(request_id: RequestId) -> Self {
        Self {
            request_id,
            principal_id: ActorId::new(PRINCIPAL).expect("bounded principal"),
            capability_id: CapabilityId::from_bytes(uuid_bytes(0xd1)).expect("valid capability ID"),
            capability_revision: NonZeroU64::new(1).expect("nonzero capability revision"),
            targets: ServiceAuditTargetsV1::empty(),
        }
    }
}

impl AdministrationAuditInputView for StartedCommandAuditInput {
    fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    fn operation(&self) -> &ServiceOperationV1 {
        &ServiceOperationV1::ExecuteCommand
    }

    fn phase(&self) -> &ServiceAuditPhaseV1 {
        &ServiceAuditPhaseV1::Started
    }

    fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    fn actor_kind(&self) -> &ActorKind {
        &ActorKind::Agent
    }

    fn capability_id(&self) -> &CapabilityId {
        &self.capability_id
    }

    fn capability_revision(&self) -> &NonZeroU64 {
        &self.capability_revision
    }

    fn ingress(&self) -> &ServiceIngressKindV1 {
        &ServiceIngressKindV1::Grpc
    }

    fn targets(&self) -> &ServiceAuditTargetsV1 {
        &self.targets
    }

    fn approval_id(&self) -> Option<&ApprovalId> {
        None
    }

    fn link(&self) -> &ServiceAuditLinkV1 {
        &ServiceAuditLinkV1::None
    }
}

struct FixedPostEvaluationCommandAuthorizer {
    plan: CommandPlan,
    lineage: ContractLineage,
    partition: PartitionKey,
}

impl FixedPostEvaluationCommandAuthorizer {
    fn new(plan: &CommandPlan, lineage: ContractLineage, partition: PartitionKey) -> Self {
        Self {
            plan: plan.clone(),
            lineage,
            partition,
        }
    }
}

impl PostEvaluationCommandAuthorizer for FixedPostEvaluationCommandAuthorizer {
    fn authorize(&self) -> Result<AuthorizedCommandExecution, PostEvaluationAuthorizationError> {
        Ok(authorize_command(
            &self.plan,
            self.lineage.clone(),
            self.partition.clone(),
        ))
    }
}

struct FixedAuthorizationClock(Timestamp);

impl AuthorizationClock for FixedAuthorizationClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(self.0)
    }
}

struct FixedDigestProvider(u8);

impl IdempotencyDigestProvider for FixedDigestProvider {
    fn digest_candidates(
        &self,
        _: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
        IdempotencyDigestCandidatesV1::new(vec![IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key ID"),
            [self.0; 32],
        )])
    }
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
        Audience::new("riffdb-correlated-index").expect("bounded audience"),
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
    .expect("authorize IndexedMutationBulk command");
    let Decision::Allow(proof) = decision else {
        panic!("IndexedMutationBulk command fixture must be authorized");
    };
    proof
        .into_command_execution(
            UntrustedInvocationClaims::new(None, None, None, None, None),
            AgentSessionAdmissionPolicy::Discard,
        )
        .expect("exact command authorization")
}

fn catalog_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("correlated-index-maintainer").expect("catalog principal"),
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

/// Typed `GeneratedCommand` wrapper for `SeedInitialIndexedStateRows` over remote gRPC.
pub(crate) struct RemoteSeedInitialIndexedStateRows {
    pub(crate) request_seed: u8,
    pub(crate) rows: Vec<CorrelatedIndexedMutationRow>,
}

impl GeneratedCommand for RemoteSeedInitialIndexedStateRows {
    type Outcome = ();

    fn idempotent_command(&self) -> Result<IdempotentCommand, GeneratedCommandError> {
        let input = build_mutation_command_input(
            "SeedInitialIndexedStateRows",
            self.request_seed,
            &self.rows,
        );
        let wire = canonical_value_to_proto(&CanonicalValue::Record(input))
            .map_err(|_| GeneratedCommandError::InvalidInputShape)?;
        IdempotentCommand::new("SeedInitialIndexedStateRows", Some(1), wire).map_err(Into::into)
    }

    fn outcome_request(
        &self,
        request_id: RequestId,
    ) -> Result<v1::GetOutcomeRequest, GeneratedCommandError> {
        Ok(v1::GetOutcomeRequest {
            request_id: request_id.into_bytes().to_vec(),
            contract_lineage: "IndexedMutationBulk".to_owned(),
            command_name: "SeedInitialIndexedStateRows".to_owned(),
            idempotency_key: canonical_uuid_text(self.request_seed),
            outcome_uri: None,
        })
    }

    fn decode_outcome(
        &self,
        response: &v1::ExecuteCommandResponse,
    ) -> Result<Self::Outcome, GeneratedCommandError> {
        if matches!(
            v1::execute_command_response::CompletionStatus::try_from(response.status),
            Ok(v1::execute_command_response::CompletionStatus::Committed
                | v1::execute_command_response::CompletionStatus::Replayed)
        ) && response.outcome_type == "IndexedStatesSeeded"
        {
            Ok(())
        } else {
            Err(GeneratedCommandError::InvalidOutcomeShape)
        }
    }
}

/// Typed `GeneratedCommand` wrapper for `ApplyIndexedMutations` over remote gRPC.
pub(crate) struct RemoteApplyIndexedMutations {
    pub(crate) request_seed: u8,
    pub(crate) rows: Vec<CorrelatedIndexedMutationRow>,
}

impl GeneratedCommand for RemoteApplyIndexedMutations {
    type Outcome = ();

    fn idempotent_command(&self) -> Result<IdempotentCommand, GeneratedCommandError> {
        let input =
            build_mutation_command_input("ApplyIndexedMutations", self.request_seed, &self.rows);
        let wire = canonical_value_to_proto(&CanonicalValue::Record(input))
            .map_err(|_| GeneratedCommandError::InvalidInputShape)?;
        IdempotentCommand::new("ApplyIndexedMutations", Some(1), wire).map_err(Into::into)
    }

    fn outcome_request(
        &self,
        request_id: RequestId,
    ) -> Result<v1::GetOutcomeRequest, GeneratedCommandError> {
        Ok(v1::GetOutcomeRequest {
            request_id: request_id.into_bytes().to_vec(),
            contract_lineage: "IndexedMutationBulk".to_owned(),
            command_name: "ApplyIndexedMutations".to_owned(),
            idempotency_key: canonical_uuid_text(self.request_seed),
            outcome_uri: None,
        })
    }

    fn decode_outcome(
        &self,
        response: &v1::ExecuteCommandResponse,
    ) -> Result<Self::Outcome, GeneratedCommandError> {
        if matches!(
            v1::execute_command_response::CompletionStatus::try_from(response.status),
            Ok(v1::execute_command_response::CompletionStatus::Committed
                | v1::execute_command_response::CompletionStatus::Replayed)
        ) && response.outcome_type == "IndexedMutationsApplied"
        {
            Ok(())
        } else {
            Err(GeneratedCommandError::InvalidOutcomeShape)
        }
    }
}

pub(crate) fn run_remote_correlated_index_loopback_case(count: usize) -> CorrelatedStoreAgreement {
    let riffdbd_bin = ensure_riffdbd_binary();
    let scratch = ScratchDir::new(&format!("correlated-remote-loopback-{count}"))
        .expect("create remote loopback scratch dir");
    let database_path = scratch.join("riffdb.redb");
    let capability_keys_path = scratch.join("capability.keys");
    let idempotency_keys_path = scratch.join("idempotency.keys");
    let bootstrap_path = scratch.join("bootstrap.credential");
    let backup_root = scratch.join("backups");
    let projections_root = scratch.join("projections");

    fs::create_dir_all(&backup_root).expect("create backup root");
    fs::set_permissions(&backup_root, fs::Permissions::from_mode(0o700))
        .expect("chmod backup root");
    fs::create_dir_all(&projections_root).expect("create projections root");
    fs::set_permissions(&projections_root, fs::Permissions::from_mode(0o700))
        .expect("chmod projections root");

    write_protected_file(&capability_keys_path, CAPABILITY_KEY_DOCUMENT);
    write_protected_file(&idempotency_keys_path, IDEMPOTENCY_KEY_DOCUMENT);

    let generated_bootstrap =
        generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)
            .expect("generate bootstrap credential");
    write_protected_file(
        &bootstrap_path,
        generated_bootstrap.render_document().expose_secret(),
    );
    drop(generated_bootstrap);
    let retained_bootstrap =
        load_bootstrap_credential_file(&bootstrap_path).expect("load bootstrap credential");

    let spec = ChildProcessSpec::new(riffdbd_bin)
        .expect("riffdbd spec")
        .arg("--database")
        .expect("arg")
        .arg(database_path.as_os_str())
        .expect("arg")
        .arg("--listen")
        .expect("arg")
        .arg("127.0.0.1:0")
        .expect("arg")
        .arg("--environment")
        .expect("arg")
        .arg(REMOTE_ENVIRONMENT)
        .expect("arg")
        .arg("--audience")
        .expect("arg")
        .arg(REMOTE_AUDIENCE)
        .expect("arg")
        .arg("--backup-root")
        .expect("arg")
        .arg(backup_root.as_os_str())
        .expect("arg")
        .arg("--projections-root")
        .expect("arg")
        .arg(projections_root.as_os_str())
        .expect("arg")
        .arg("--capability-keys")
        .expect("arg")
        .arg(capability_keys_path.as_os_str())
        .expect("arg")
        .arg("--idempotency-keys")
        .expect("arg")
        .arg(idempotency_keys_path.as_os_str())
        .expect("arg");

    let mut child = ChildProcessController::spawn(&spec).expect("spawn riffdbd child");
    let ready_line = child
        .wait_for_readiness(READY_PREFIX, START_TIMEOUT)
        .expect("wait for riffdbd readiness");
    let address: SocketAddr = ready_line
        .strip_prefix(READY_PREFIX)
        .expect("ready prefix")
        .parse()
        .expect("socket address");

    let partition_id = format!("tenant-remote-{count}");
    let rows =
        CorrelatedIndexedMutationRow::deterministic_batch(&partition_id, count, true, false, 1);
    let mut model = CorrelatedIndexWorkModel::new(&partition_id);
    model
        .apply_seed_initial_rows(&rows)
        .expect("model applies SeedInitialIndexedStateRows");
    model
        .apply_indexed_mutations(&rows)
        .expect("model applies ApplyIndexedMutations");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build remote loopback runtime");

    rt.block_on(async {
        let endpoint = Endpoint::from_shared(format!("http://{address}"))
            .expect("valid endpoint")
            .connect_timeout(RPC_TIMEOUT)
            .timeout(RPC_TIMEOUT);
        let mut client = RiffDbClient::connect(endpoint)
            .await
            .expect("connect to riffdbd loopback");

        let bootstrap_token = str::from_utf8(retained_bootstrap.token().expose_secret())
            .expect("utf8 bootstrap token");
        let bootstrap_meta = BootstrapCallMetadata::new(
            TransportBootstrapCredential::new(bootstrap_token).expect("bootstrap credential"),
        );
        let bootstrap_resp = client
            .create_bootstrap_capability(
                remote_bootstrap_request(&retained_bootstrap),
                &bootstrap_meta,
            )
            .await
            .expect("create bootstrap capability");
        assert!(matches!(
            bootstrap_resp.result,
            Some(v1::create_capability_response::Result::Bootstrap(_))
        ));

        let bearer = BearerCredential::new(bootstrap_token).expect("bearer credential");
        let authenticated = CallMetadata::authenticated(bearer);

        let deployed = client
            .deploy_contract(
                v1::DeployContractRequest {
                    request_id: fresh_request_id_bytes(),
                    source: INDEXED_MUTATIONS_SOURCE.to_owned(),
                    expected_active_version: None,
                    expected_active_bundle_hash: Vec::new(),
                    expected_candidate_bundle_hash: Vec::new(),
                },
                &authenticated,
            )
            .await
            .expect("deploy IndexedMutationBulk contract");
        assert!(matches!(
            deployed.result,
            Some(v1::deploy_contract_response::Result::Activated(_))
        ));

        let seed_cmd = RemoteSeedInitialIndexedStateRows {
            request_seed: 0x81,
            rows: rows.clone(),
        };
        let seeded = client
            .execute_generated(
                &seed_cmd,
                AttemptBudget::new(1).expect("nonzero budget"),
                &authenticated,
            )
            .await
            .expect("execute SeedInitialIndexedStateRows over remote loopback");
        assert_eq!(seeded.response().commit_sequence, 1);

        let apply_cmd = RemoteApplyIndexedMutations {
            request_seed: 0x82,
            rows: rows.clone(),
        };
        let applied = client
            .execute_generated(
                &apply_cmd,
                AttemptBudget::new(1).expect("nonzero budget"),
                &authenticated,
            )
            .await
            .expect("execute ApplyIndexedMutations over remote loopback");
        assert_eq!(applied.response().commit_sequence, 2);

        // Same-key idempotent replay over remote loopback must return the exact
        // committed sequence without advancing the frontier.
        let replayed = client
            .execute_generated(
                &apply_cmd,
                AttemptBudget::new(1).expect("nonzero budget"),
                &authenticated,
            )
            .await
            .expect("replay ApplyIndexedMutations over remote loopback");
        assert_eq!(replayed.response().commit_sequence, 2);
        assert_eq!(replayed.response().outcome_type, "IndexedMutationsApplied");
    });

    let exit = child
        .shutdown_cleanly(b"shutdown\n", STOP_TIMEOUT)
        .expect("shutdown riffdbd cleanly");
    assert!(exit.status.success(), "riffdbd must exit cleanly");

    let session = open_correlated_redb_inspection(
        &database_path,
        correlated_startup_inputs(),
        model.namespace().clone(),
    )
    .expect("open stopped redb after riffdbd shutdown");
    assert!(
        session.structural_findings().is_empty(),
        "stopped redb after riffdbd shutdown must have zero structural findings"
    );
    verify_correlated_index_work_against_redb(&model, &session)
        .expect("stopped redb after remote loopback must match independent model")
}

fn remote_bootstrap_request(
    credential: &RetainedBootstrapCredential,
) -> v1::CreateCapabilityRequest {
    use v1::capability_permission::Permission;

    let bundle = indexed_mutation_bulk_bundle();
    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: "IndexedMutationBulk".to_owned(),
        stable_id,
    };
    let mut permissions = vec![v1::CapabilityPermission {
        permission: Some(Permission::DeployContract(v1::Unit {})),
    }];
    let mut command_ids: Vec<u32> = bundle
        .commands()
        .iter()
        .map(|command| u32::from(command.command_id()))
        .collect();
    command_ids.sort_unstable();
    for command_id in command_ids {
        permissions.push(v1::CapabilityPermission {
            permission: Some(Permission::InvokeCommand(scoped(command_id))),
        });
    }
    let mut entity_ids: Vec<u32> = bundle
        .schema()
        .entities()
        .iter()
        .map(|entity| u32::from(entity.id()))
        .collect();
    entity_ids.sort_unstable();
    for entity_id in entity_ids {
        permissions.push(v1::CapabilityPermission {
            permission: Some(Permission::ReadEntity(scoped(entity_id))),
        });
    }
    let mut index_ids: Vec<u32> = bundle
        .schema()
        .entities()
        .iter()
        .flat_map(|entity| entity.indexes().iter().map(|index| u32::from(index.id())))
        .collect();
    index_ids.sort_unstable();
    for index_id in index_ids {
        permissions.push(v1::CapabilityPermission {
            permission: Some(Permission::ScanIndex(scoped(index_id))),
        });
    }
    permissions.extend([
        v1::CapabilityPermission {
            permission: Some(Permission::SubscribeCommits(v1::Unit {})),
        },
        v1::CapabilityPermission {
            permission: Some(Permission::ReadHealth(v1::Unit {})),
        },
        v1::CapabilityPermission {
            permission: Some(Permission::AdministerCapabilities(v1::Unit {})),
        },
    ]);

    let mut field_visibility = Vec::new();
    for entity in bundle.schema().entities() {
        let mut field_ids: Vec<u32> = entity
            .record()
            .fields()
            .iter()
            .map(|field| u32::from(field.id()))
            .collect();
        field_ids.sort_unstable();
        field_visibility.push(v1::EntityFieldVisibility {
            contract_lineage: "IndexedMutationBulk".to_owned(),
            entity_type_id: u32::from(entity.id()),
            field_ids,
            secret_field_ids: Vec::new(),
        });
    }
    field_visibility.sort_by_key(|entry| entry.entity_type_id);

    v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes(),
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "correlated-remote-maintainer".to_owned(),
        actor_kind: v1::ActorKind::Human as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
        audiences: vec![REMOTE_AUDIENCE.to_owned()],
        grant: Some(v1::CapabilityGrant {
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            partition_scope: Some(v1::PartitionScope {
                scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
            }),
            permissions,
            field_visibility,
            max_scan_rows: 100,
            approval_required: Vec::new(),
            row_policy: None,
            export: None,
            reimport: None,
            vector_inspection: None,
        }),
    }
}

fn fresh_request_id_bytes() -> Vec<u8> {
    generate_request_id()
        .expect("generate request ID")
        .into_bytes()
        .to_vec()
}

fn write_protected_file(path: &Path, document: &[u8]) {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .expect("create protected file");
    file.write_all(document).expect("write protected file");
    file.sync_all().expect("sync protected file");
    drop(file);
    File::open(path.parent().expect("protected file parent"))
        .expect("open protected file parent")
        .sync_all()
        .expect("sync protected file parent");
}

fn ensure_riffdbd_binary() -> PathBuf {
    let current = std::env::current_exe().expect("current test executable");
    let profile_dir = current
        .parent()
        .and_then(Path::parent)
        .expect("target profile directory");
    let bin_path = profile_dir.join("riffdbd");
    if !bin_path.is_file() {
        let status = std::process::Command::new(env!("CARGO"))
            .args(["build", "-p", "riffdb-server", "--bin", "riffdbd"])
            .status()
            .expect("build riffdbd binary");
        assert!(
            status.success(),
            "cargo build -p riffdb-server --bin riffdbd must succeed"
        );
    }
    bin_path
}
