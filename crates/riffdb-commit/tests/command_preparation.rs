#![forbid(unsafe_code)]

//! Public-boundary tests for exact command-execution preparation.

use std::{collections::BTreeMap, num::NonZeroU16, time::Duration, time::Instant};

use riffdb_catalog::{ResolvedExecutablePlan, ValidatedContractBundle, resolve_executable_plan};
use riffdb_commit::{
    CommandExecutionPreparation, CommandExecutionPreparationError, CommandRequestControl,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{CommandPlan, RecordSchema};
use riffdb_idempotency::{
    CommandIdempotencyScopeV1, IdempotencyDigestCandidatesV1, IdempotencyDigestError,
    IdempotencyDigestProvider, IdempotencyInspectionExecutor, PreparedIdempotencyRecheckV1,
    prepare_idempotency_lookup,
};
use riffdb_invariant::derive_input_command_facts;
use riffdb_policy::{
    AgentSessionAdmissionPolicy, AuthorizationClock, AuthorizationClockError,
    AuthorizedCommandExecution, CommandExecutionClass, CurrentAuthorizer, Decision,
    NoopAuthorizationTelemetry, OperationRequest, UntrustedInvocationClaims,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AdmissionLookupResultV1, AdmissionRepository, AdmissionRequestV1,
    AdmissionResultV1, CatalogRepository, ExecutablePlanRef, IdempotencyKeyDigest,
    IdempotencyLookupCandidatesV1, StorageError, StoredContractBundleV1,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CanonicalRecord, CanonicalValue, CapabilityGrantV1,
    CapabilityPermissionV1, CapabilityPermissionsV1, CommandId, ContractLineage, ContractVersion,
    DatabaseId, Decimal, DecimalSpec, DigestKeyId, Environment, IdempotencyKey, PartitionKey,
    PartitionScopeV1, PlanHash, RequestId, TenantId, TenantScope, Timestamp,
};

const CALLER_KEY: &str = "preparation-secret-canary";
const PRINCIPAL: &str = "command-preparation-agent";
const READ_ONLY_SOURCE: &str = r#"
contract PreparationReadOnly version 1 {
  entity Row {
    key (row_id: uuid)
  }
  aggregate Rows {
    root Row
    partition_by row_id
    conflict_key (row_id)
  }
  command GetRow {
    input caller_key: string<128>
    input row_id: uuid
    read Row(row_id) as row else Missing {}
    return Found { row: row }
  }
}
"#;

struct CommandFixture {
    resolved: ResolvedExecutablePlan,
    reference: ExecutablePlanRef,
    normalized_input: CanonicalRecord,
    partition: PartitionKey,
}

struct SingleBundleCatalog {
    active: ActiveCatalogPointerV1,
    bundle: StoredContractBundleV1,
}

impl CatalogRepository for SingleBundleCatalog {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(Some(self.active.clone()))
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        Ok(
            (self.bundle.lineage() == lineage
                && self.bundle.contract_version() == contract_version)
                .then(|| self.bundle.clone()),
        )
    }
}

fn resolve_genesis_plan(
    bundle: &ValidatedContractBundle,
    reference: &ExecutablePlanRef,
) -> ResolvedExecutablePlan {
    let stored = bundle.to_stored().expect("stored fixture bundle");
    let repository = SingleBundleCatalog {
        active: ActiveCatalogPointerV1::from_bundle(&stored),
        bundle: stored,
    };
    resolve_executable_plan(&repository, reference).expect("exact fixture plan resolves")
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
            DigestKeyId::new(1).expect("nonzero digest key ID"),
            [0x51; 32],
        )])
    }
}

struct AbsentAdmissionRepository;

impl AdmissionRepository for AbsentAdmissionRepository {
    fn admit_or_resolve(&self, _: AdmissionRequestV1) -> Result<AdmissionResultV1, StorageError> {
        panic!("command preparation must not mutate admission state")
    }

    fn lookup_admission(
        &self,
        _: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        Ok(AdmissionLookupResultV1::NotFound)
    }
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("valid timestamp")
}

fn database(seed: u8) -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
        .expect("valid UUIDv7")
}

fn request(seed: u8) -> RequestId {
    RequestId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
        .expect("valid UUIDv7")
}

fn environment(value: &str) -> Environment {
    Environment::new(value).expect("bounded environment")
}

fn decimal(coefficient: i128) -> CanonicalValue {
    CanonicalValue::Decimal(
        Decimal::new(
            DecimalSpec::new(28, 2).expect("valid decimal spec"),
            coefficient,
        )
        .expect("bounded decimal"),
    )
}

fn normalized_input(
    plan: &CommandPlan,
    organization_id: [u8; 16],
    approved_amount: i128,
) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string(CALLER_KEY).expect("bounded key"),
            ),
            ("organization_id", CanonicalValue::Uuid(organization_id)),
            ("fiscal_year", CanonicalValue::I64(2026)),
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
    .expect("canonical input")
}

fn command_fixture() -> CommandFixture {
    let bundle =
        ValidatedContractBundle::decode(include_bytes!("../../../fixtures/compiler/bundle.bin"))
            .expect("checked compiler fixture");
    let plan = bundle
        .bundle()
        .commands()
        .iter()
        .find(|plan| plan.name() == "CreateBudget")
        .expect("CreateBudget plan");
    let reference = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        plan.command_id(),
        plan.plan_hash(),
    );
    let normalized_input = normalized_input(plan, [0x31; 16], 12_500);
    let facts = derive_input_command_facts(plan, normalized_input.clone())
        .expect("input facts for compiler fixture");
    let partition = facts.partition_key().clone();
    let resolved = resolve_genesis_plan(&bundle, &reference);
    CommandFixture {
        resolved,
        reference,
        normalized_input,
        partition,
    }
}

fn read_only_fixture() -> CommandFixture {
    let compiled = compile_contract_source(READ_ONLY_SOURCE).expect("checked read-only source");
    let bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
        .expect("catalog-valid read-only bundle");
    let plan = bundle
        .bundle()
        .commands()
        .iter()
        .find(|plan| plan.name() == "GetRow")
        .expect("GetRow plan");
    let reference = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        plan.command_id(),
        plan.plan_hash(),
    );
    let normalized_input = input_record(
        plan.input().record(),
        [
            (
                "caller_key",
                CanonicalValue::string(CALLER_KEY).expect("bounded caller key"),
            ),
            ("row_id", CanonicalValue::Uuid([0x44; 16])),
        ],
    );
    let facts =
        derive_input_command_facts(plan, normalized_input.clone()).expect("read-only input facts");
    let partition = facts.partition_key().clone();
    let resolved = resolve_genesis_plan(&bundle, &reference);
    CommandFixture {
        resolved,
        reference,
        normalized_input,
        partition,
    }
}

fn exact_scope(command: &CommandFixture) -> CommandIdempotencyScopeV1 {
    scope(
        database(1),
        environment("development"),
        TenantScope::Global,
        PRINCIPAL,
        command.reference.contract_lineage().clone(),
        command.reference.command_id(),
    )
}

fn scope(
    database_id: DatabaseId,
    environment: Environment,
    tenant_scope: TenantScope,
    principal: &str,
    lineage: ContractLineage,
    command_id: CommandId,
) -> CommandIdempotencyScopeV1 {
    CommandIdempotencyScopeV1::new(
        database_id,
        environment,
        tenant_scope,
        ActorId::new(principal).expect("bounded principal"),
        lineage,
        command_id,
    )
}

fn prepared_idempotency(
    command: &CommandFixture,
    idempotency_scope: CommandIdempotencyScopeV1,
    input: &CanonicalRecord,
    selected_plan: ExecutablePlanRef,
) -> PreparedIdempotencyRecheckV1 {
    let caller_key = IdempotencyKey::new(CALLER_KEY).expect("bounded caller key");
    let lookup = prepare_idempotency_lookup(&idempotency_scope, &caller_key, &FixedDigestProvider)
        .expect("idempotency lookup preparation");
    let repository = AbsentAdmissionRepository;
    IdempotencyInspectionExecutor::new(&repository)
        .inspect(lookup)
        .expect("absent inspection")
        .confirm_input(
            input,
            command
                .resolved
                .plan()
                .idempotency_input()
                .expect("mutation idempotency field"),
            &caller_key,
        )
        .expect("exact input confirmation")
        .bind_selected_plan(selected_plan)
        .expect("absence may select requested plan")
}

#[allow(clippy::too_many_arguments)]
fn authorized(
    database_id: DatabaseId,
    environment: Environment,
    principal: &str,
    lineage: ContractLineage,
    version: ContractVersion,
    command_id: CommandId,
    class: CommandExecutionClass,
    partition: PartitionKey,
) -> AuthorizedCommandExecution {
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![CapabilityPermissionV1::InvokeCommand(
            lineage.clone(),
            command_id,
        )])
        .expect("canonical permissions"),
        Vec::new(),
        NonZeroU16::new(10).expect("nonzero row bound"),
        Vec::new(),
    )
    .expect("valid grant");
    let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database_id,
        environment.clone(),
        ActorId::new(principal).expect("bounded principal"),
        ActorKind::Agent,
        Audience::new("riffdb-command-preparation").expect("bounded audience"),
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
        database_id,
        environment,
    )
    .authorize(
        fixture.authenticated_principal(),
        OperationRequest::execute_command(lineage, version, command_id, class, partition),
    )
    .expect("policy decision");
    let Decision::Allow(proof) = decision else {
        panic!("command must be allowed by exact fixture grant");
    };
    proof
        .into_command_execution(
            UntrustedInvocationClaims::new(None, None, None, None, None),
            AgentSessionAdmissionPolicy::Discard,
        )
        .expect("exact command authorization")
}

fn exact_authorization(command: &CommandFixture) -> AuthorizedCommandExecution {
    authorized(
        database(1),
        environment("development"),
        PRINCIPAL,
        command.reference.contract_lineage().clone(),
        command.reference.contract_version(),
        command.reference.command_id(),
        CommandExecutionClass::Mutation,
        command.partition.clone(),
    )
}

#[allow(clippy::too_many_arguments)]
fn attempt(
    command: &CommandFixture,
    trusted_database: DatabaseId,
    trusted_environment: &Environment,
    idempotency_scope: CommandIdempotencyScopeV1,
    idempotency_input: &CanonicalRecord,
    selected_plan: ExecutablePlanRef,
    facts_input: CanonicalRecord,
    authorization: AuthorizedCommandExecution,
) -> Result<CommandExecutionPreparation, CommandExecutionPreparationError> {
    let idempotency =
        prepared_idempotency(command, idempotency_scope, idempotency_input, selected_plan);
    let facts = derive_input_command_facts(command.resolved.plan(), facts_input)
        .expect("input facts derive");
    let (control, _handle) = CommandRequestControl::new(
        Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("representable future deadline"),
    );
    CommandExecutionPreparation::new(
        trusted_database,
        trusted_environment,
        command.resolved.clone(),
        command.normalized_input.clone(),
        idempotency,
        facts,
        authorization,
        request(1),
        control,
    )
}

#[test]
fn exact_proofs_accept_past_cancelled_control_and_distinct_request_ids() {
    let command = command_fixture();
    let first_request = request(1);
    let second_request = request(2);
    assert_ne!(first_request, second_request);

    for (request_id, cancel_before_join) in [(first_request, true), (second_request, false)] {
        let idempotency = prepared_idempotency(
            &command,
            exact_scope(&command),
            &command.normalized_input,
            command.reference.clone(),
        );
        let facts =
            derive_input_command_facts(command.resolved.plan(), command.normalized_input.clone())
                .expect("exact input facts");
        let deadline = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("representable past deadline");
        let (control, cancellation) = CommandRequestControl::new(deadline);
        if cancel_before_join {
            cancellation.clone().cancel();
        }

        let prepared = CommandExecutionPreparation::new(
            database(1),
            &environment("development"),
            command.resolved.clone(),
            command.normalized_input.clone(),
            idempotency,
            facts,
            exact_authorization(&command),
            request_id,
            control,
        )
        .expect("control state and request identity do not alter proof joins");
        assert_eq!(
            format!("{prepared:?}"),
            "CommandExecutionPreparation([REDACTED])"
        );
        assert_eq!(
            format!("{cancellation:?}"),
            "CommandCancellationHandle([REDACTED])"
        );
    }
}

#[test]
fn exact_plan_input_and_facts_are_required() {
    let command = command_fixture();
    let trusted_environment = environment("development");
    let changed_input = normalized_input(command.resolved.plan(), [0x31; 16], 12_600);
    let wrong_plan = ExecutablePlanRef::new(
        command.reference.contract_lineage().clone(),
        command.reference.contract_version(),
        command.reference.contract_bundle_hash(),
        command.reference.command_id(),
        PlanHash::from_bytes([0x91; 32]),
    );

    let input_error = attempt(
        &command,
        database(1),
        &trusted_environment,
        exact_scope(&command),
        &changed_input,
        command.reference.clone(),
        command.normalized_input.clone(),
        exact_authorization(&command),
    )
    .expect_err("idempotency input substitution must reject");
    let plan_error = attempt(
        &command,
        database(1),
        &trusted_environment,
        exact_scope(&command),
        &command.normalized_input,
        wrong_plan,
        command.normalized_input.clone(),
        exact_authorization(&command),
    )
    .expect_err("idempotency plan substitution must reject");
    let facts_error = attempt(
        &command,
        database(1),
        &trusted_environment,
        exact_scope(&command),
        &command.normalized_input,
        command.reference.clone(),
        changed_input,
        exact_authorization(&command),
    )
    .expect_err("input-facts substitution must reject");

    assert_eq!(input_error, plan_error);
    assert_eq!(plan_error, facts_error);
    assert_eq!(
        format!("{input_error:?}"),
        "CommandExecutionPreparationError([REDACTED])"
    );
    assert_eq!(
        input_error.to_string(),
        "command execution proofs are inconsistent"
    );
    assert!(!format!("{input_error:?}").contains(CALLER_KEY));
}

#[test]
fn trusted_database_environment_and_authorized_actor_scope_are_exact() {
    let command = command_fixture();
    let trusted_environment = environment("development");
    let mismatched_scopes = [
        scope(
            database(1),
            trusted_environment.clone(),
            TenantScope::Tenant(TenantId::new("tenant-a").expect("bounded tenant")),
            PRINCIPAL,
            command.reference.contract_lineage().clone(),
            command.reference.command_id(),
        ),
        scope(
            database(1),
            trusted_environment.clone(),
            TenantScope::Global,
            "different-principal",
            command.reference.contract_lineage().clone(),
            command.reference.command_id(),
        ),
        scope(
            database(1),
            trusted_environment.clone(),
            TenantScope::Global,
            PRINCIPAL,
            ContractLineage::new("different.lineage").expect("bounded lineage"),
            command.reference.command_id(),
        ),
        scope(
            database(1),
            trusted_environment.clone(),
            TenantScope::Global,
            PRINCIPAL,
            command.reference.contract_lineage().clone(),
            CommandId::new(command.reference.command_id().get() + 1).expect("command ID"),
        ),
    ];
    for idempotency_scope in mismatched_scopes {
        assert!(
            attempt(
                &command,
                database(1),
                &trusted_environment,
                idempotency_scope,
                &command.normalized_input,
                command.reference.clone(),
                command.normalized_input.clone(),
                exact_authorization(&command),
            )
            .is_err()
        );
    }

    for (trusted_database, trusted_environment) in [
        (database(2), environment("development")),
        (database(1), environment("staging")),
    ] {
        assert!(
            attempt(
                &command,
                trusted_database,
                &trusted_environment,
                exact_scope(&command),
                &command.normalized_input,
                command.reference.clone(),
                command.normalized_input.clone(),
                exact_authorization(&command),
            )
            .is_err()
        );
    }

    for authorization in [
        authorized(
            database(2),
            trusted_environment.clone(),
            PRINCIPAL,
            command.reference.contract_lineage().clone(),
            command.reference.contract_version(),
            command.reference.command_id(),
            CommandExecutionClass::Mutation,
            command.partition.clone(),
        ),
        authorized(
            database(1),
            environment("staging"),
            PRINCIPAL,
            command.reference.contract_lineage().clone(),
            command.reference.contract_version(),
            command.reference.command_id(),
            CommandExecutionClass::Mutation,
            command.partition.clone(),
        ),
    ] {
        assert!(
            attempt(
                &command,
                database(1),
                &trusted_environment,
                exact_scope(&command),
                &command.normalized_input,
                command.reference.clone(),
                command.normalized_input.clone(),
                authorization,
            )
            .is_err(),
            "an allow proof from another deployment boundary must not join"
        );
    }
}

#[test]
fn checked_read_only_plan_with_matching_read_only_authorization_cannot_enter_mutation_admission() {
    let command = read_only_fixture();
    assert!(command.resolved.plan().idempotency_input().is_none());

    let caller_key = IdempotencyKey::new(CALLER_KEY).expect("bounded caller key");
    let caller_key_field = command
        .resolved
        .plan()
        .input()
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "caller_key")
        .expect("caller-key-shaped read-only field")
        .id();
    let lookup =
        prepare_idempotency_lookup(&exact_scope(&command), &caller_key, &FixedDigestProvider)
            .expect("lookup preparation");
    let idempotency = IdempotencyInspectionExecutor::new(&AbsentAdmissionRepository)
        .inspect(lookup)
        .expect("absent inspection")
        .confirm_input(&command.normalized_input, caller_key_field, &caller_key)
        .expect("read-only-shaped input can form only an untrusted idempotency proof")
        .bind_selected_plan(command.reference.clone())
        .expect("absence may retain the checked read-only plan");
    let facts =
        derive_input_command_facts(command.resolved.plan(), command.normalized_input.clone())
            .expect("read-only facts");
    let (control, _handle) = CommandRequestControl::new(
        Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("representable future deadline"),
    );

    let error = CommandExecutionPreparation::new(
        database(1),
        &environment("development"),
        command.resolved.clone(),
        command.normalized_input.clone(),
        idempotency,
        facts,
        authorized(
            database(1),
            environment("development"),
            PRINCIPAL,
            command.reference.contract_lineage().clone(),
            command.reference.contract_version(),
            command.reference.command_id(),
            CommandExecutionClass::ReadOnly,
            command.partition.clone(),
        ),
        request(9),
        control,
    )
    .expect_err("a checked read-only plan has no mutation-admission authority");

    assert_eq!(
        error.to_string(),
        "command execution proofs are inconsistent"
    );
}

#[test]
fn authorization_identity_class_and_partition_are_exact() {
    let command = command_fixture();
    let trusted_environment = environment("development");
    let alternate_input = normalized_input(command.resolved.plan(), [0x72; 16], 12_500);
    let alternate_partition = derive_input_command_facts(command.resolved.plan(), alternate_input)
        .expect("alternate partition facts")
        .partition_key()
        .clone();
    let alternate_lineage = ContractLineage::new("different.lineage").expect("bounded lineage");
    let alternate_command =
        CommandId::new(command.reference.command_id().get() + 1).expect("command ID");
    let alternate_version = ContractVersion::new(command.reference.contract_version().get() + 1)
        .expect("contract version");

    let authorizations = [
        authorized(
            database(1),
            trusted_environment.clone(),
            PRINCIPAL,
            alternate_lineage,
            command.reference.contract_version(),
            command.reference.command_id(),
            CommandExecutionClass::Mutation,
            command.partition.clone(),
        ),
        authorized(
            database(1),
            trusted_environment.clone(),
            PRINCIPAL,
            command.reference.contract_lineage().clone(),
            alternate_version,
            command.reference.command_id(),
            CommandExecutionClass::Mutation,
            command.partition.clone(),
        ),
        authorized(
            database(1),
            trusted_environment.clone(),
            PRINCIPAL,
            command.reference.contract_lineage().clone(),
            command.reference.contract_version(),
            alternate_command,
            CommandExecutionClass::Mutation,
            command.partition.clone(),
        ),
        authorized(
            database(1),
            trusted_environment.clone(),
            PRINCIPAL,
            command.reference.contract_lineage().clone(),
            command.reference.contract_version(),
            command.reference.command_id(),
            CommandExecutionClass::ReadOnly,
            command.partition.clone(),
        ),
        authorized(
            database(1),
            trusted_environment.clone(),
            PRINCIPAL,
            command.reference.contract_lineage().clone(),
            command.reference.contract_version(),
            command.reference.command_id(),
            CommandExecutionClass::Mutation,
            alternate_partition,
        ),
    ];

    for authorization in authorizations {
        assert!(
            attempt(
                &command,
                database(1),
                &trusted_environment,
                exact_scope(&command),
                &command.normalized_input,
                command.reference.clone(),
                command.normalized_input.clone(),
                authorization,
            )
            .is_err()
        );
    }
}
