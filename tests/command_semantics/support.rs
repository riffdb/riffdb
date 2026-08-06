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
    AdministrationAuditInputView, AdministrationClock, AdministrationClockError, AdmissionClock,
    AdmissionClockError, ApplicationCommitNotificationError, ApplicationCommitNotificationSink,
    CommandExecutionPreparation, CommandRequestControl, CommitTelemetry, CoordinatorDurability,
    CoordinatorWorkloadCapacity, PostEvaluationAuthorizationError, PostEvaluationCommandAuthorizer,
    ProvenanceIdSource, ProvenanceIdSourceError, RunningCommandCoordinator,
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
    ActorId, ActorKind, ApprovalId, Audience, CanonicalRecord, CanonicalValue, CapabilityGrantV1,
    CapabilityId, CapabilityPermissionV1, CapabilityPermissionsV1, CommitSequence, ContractLineage,
    DatabaseId, Decimal, DecimalSpec, DigestKeyId, EntityKeyBuilder, EntityTypeId, EntityVersion,
    Environment, FieldId, IdempotencyKey, PartitionKey, PartitionScopeV1, ProvenanceId, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    ServiceOperationV1, TenantScope, Timestamp,
};

const BUDGET_SOURCE: &str = include_str!("../../contracts/examples/budget.riff");
const UNIQUE_SOURCE: &str = r#"
contract UniqueUsers version 1 {
  entity Organization {
    key (organization_id: uuid)
  }
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field email: string<128>
    unique user_email (organization_id, email)
  }
  aggregate Users {
    root Organization
    child User
    partition_by organization_id
    conflict_key (organization_id)
  }
  command CreateOrganization {
    input idempotency_key: string<128>
    input organization_id: uuid
    idempotency_key idempotency_key
    create Organization(organization_id) as organization
      else OrganizationExists { organization_id: organization_id }
    return Created { organization: organization }
  }
  command CreateUser {
    input idempotency_key: string<128>
    input organization_id: uuid
    input user_id: uuid
    input email: string<128>
    idempotency_key idempotency_key
    read Organization(organization_id) as organization
      else OrganizationMissing { organization_id: organization_id }
    create User(organization_id, user_id) as user
      else UserExists { user_id: user_id }
    set user.email = email
    return Created { user: user }
  }
  command ChangeEmail {
    input idempotency_key: string<128>
    input organization_id: uuid
    input user_id: uuid
    input email: string<128>
    idempotency_key idempotency_key
    mutate User(organization_id, user_id) as user
      else UserMissing { user_id: user_id }
    set user.email = email
    return Changed { user: user }
  }
}
"#;
const PRINCIPAL: &str = "command-semantics-agent";
const CALLER_KEY: &str = "command-semantics-idempotency-key";
const ORGANIZATION_ID: [u8; 16] = [0x31; 16];
const FISCAL_YEAR: i64 = 2026;
const ENVIRONMENT: &str = "integration";
static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

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
    authorization: Mutex<Option<AuthorizedCommandExecution>>,
}

impl FixedPostEvaluationCommandAuthorizer {
    fn new(authorization: AuthorizedCommandExecution) -> Self {
        Self {
            authorization: Mutex::new(Some(authorization)),
        }
    }
}

impl PostEvaluationCommandAuthorizer for FixedPostEvaluationCommandAuthorizer {
    fn authorize(&self) -> Result<AuthorizedCommandExecution, PostEvaluationAuthorizationError> {
        self.authorization
            .lock()
            .map_err(|_| PostEvaluationAuthorizationError::Unavailable)?
            .take()
            .ok_or(PostEvaluationAuthorizationError::Integrity)
    }
}

pub(crate) struct BudgetDatabase {
    path: PathBuf,
    checked_bundle: ValidatedContractBundle,
    target_entity_type: EntityTypeId,
    approved_amount_field: FieldId,
}

pub(crate) struct UniqueUserDatabase {
    path: PathBuf,
    checked_bundle: ValidatedContractBundle,
    user_entity_type: EntityTypeId,
    email_field: FieldId,
}

impl UniqueUserDatabase {
    pub(crate) fn create(label: &str) -> Self {
        let ordinal = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "riffdb-command-unique-{label}-{}-{ordinal}.redb",
            std::process::id()
        ));
        let compiled = compile_contract_source(UNIQUE_SOURCE).expect("unique contract compiles");
        let checked_bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("unique bundle passes catalog validation");
        let user = checked_bundle
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "User")
            .expect("User entity schema");
        let user_entity_type = user.id();
        let email_field = user
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "email")
            .expect("email field")
            .id();

        let mut store = RedbStore::open(&path).expect("create redb unique database");
        store
            .initialize_database(database_id())
            .expect("initialize unique database");
        let mut ports = open_operational(store);
        let stored_bundle = checked_bundle.to_stored().expect("stored unique bundle");
        let activation = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                stored_bundle.clone(),
                request_id(0x71),
                catalog_principal(),
                timestamp(1_700_000_000),
                None,
            ))
            .expect("activate unique bundle");
        assert!(matches!(
            activation,
            CatalogActivationResult::Activated { active, .. }
                if active == ActiveCatalogPointerV1::from_bundle(&stored_bundle)
        ));
        drop(ports);

        Self {
            path,
            checked_bundle,
            user_entity_type,
            email_field,
        }
    }

    pub(crate) fn open(&self) -> RedbOperationalPorts {
        open_operational(RedbStore::open(&self.path).expect("reopen unique database"))
    }

    pub(crate) fn open_with_controller(
        &self,
        controller: RedbTestController,
    ) -> RedbOperationalPorts {
        open_operational(
            RedbStore::open_with_test_controller(&self.path, controller)
                .expect("reopen unique database with test controller"),
        )
    }

    pub(crate) fn prepare(
        &self,
        ports: &RedbOperationalPorts,
        user_id: [u8; 16],
        email: &str,
        caller_key: &str,
        digest_seed: u8,
        request_seed: u8,
    ) -> CommandExecutionPreparation {
        self.prepare_user_command(
            ports,
            "CreateUser",
            ORGANIZATION_ID,
            user_id,
            email,
            caller_key,
            digest_seed,
            request_seed,
        )
    }

    pub(crate) fn prepare_email_change(
        &self,
        ports: &RedbOperationalPorts,
        user_id: [u8; 16],
        email: &str,
        caller_key: &str,
        digest_seed: u8,
        request_seed: u8,
    ) -> CommandExecutionPreparation {
        self.prepare_user_command(
            ports,
            "ChangeEmail",
            ORGANIZATION_ID,
            user_id,
            email,
            caller_key,
            digest_seed,
            request_seed,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_for_organization(
        &self,
        ports: &RedbOperationalPorts,
        organization_id: [u8; 16],
        user_id: [u8; 16],
        email: &str,
        caller_key: &str,
        digest_seed: u8,
        request_seed: u8,
    ) -> CommandExecutionPreparation {
        self.prepare_user_command(
            ports,
            "CreateUser",
            organization_id,
            user_id,
            email,
            caller_key,
            digest_seed,
            request_seed,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_user_command(
        &self,
        ports: &RedbOperationalPorts,
        command_name: &str,
        organization_id: [u8; 16],
        user_id: [u8; 16],
        email: &str,
        caller_key: &str,
        digest_seed: u8,
        request_seed: u8,
    ) -> CommandExecutionPreparation {
        let plan = self.command_plan(command_name);
        let reference = ExecutablePlanRef::new(
            self.checked_bundle.lineage().clone(),
            self.checked_bundle.contract_version(),
            self.checked_bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let resolved =
            resolve_executable_plan(ports, &reference).expect("deployed user command resolves");
        let input = input_record(
            plan.input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(caller_key).expect("bounded caller key"),
                ),
                ("organization_id", CanonicalValue::Uuid(organization_id)),
                ("user_id", CanonicalValue::Uuid(user_id)),
                (
                    "email",
                    CanonicalValue::string(email).expect("bounded email"),
                ),
            ],
        );
        let facts = derive_input_command_facts(plan, input.clone())
            .expect("checked user command input facts");
        let caller_key = IdempotencyKey::new(caller_key).expect("bounded caller key");
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
                .expect("prepare unique caller-key lookup");
        let idempotency = IdempotencyInspectionExecutor::new(ports)
            .inspect(lookup)
            .expect("inspect unique idempotency state")
            .confirm_input(
                &input,
                plan.idempotency_input()
                    .expect("user command idempotency input"),
                &caller_key,
            )
            .expect("confirm unique command input")
            .bind_selected_plan(reference)
            .expect("bind unique command plan");
        let authorization = authorize_command(
            plan,
            self.checked_bundle.lineage().clone(),
            facts.partition_key().clone(),
        );
        let (control, _cancellation) = CommandRequestControl::new(
            Instant::now()
                .checked_add(Duration::from_secs(30))
                .expect("representable unique command deadline"),
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
            riffdb_types::ServiceIngressKindV1::Grpc,
            control,
        )
        .expect("join exact unique command preparation proofs")
    }

    pub(crate) fn prepare_organization(
        &self,
        ports: &RedbOperationalPorts,
    ) -> CommandExecutionPreparation {
        self.prepare_organization_for(ports, ORGANIZATION_ID, "create-organization", 0x60, 0x50)
    }

    pub(crate) fn prepare_organization_for(
        &self,
        ports: &RedbOperationalPorts,
        organization_id: [u8; 16],
        caller_key_text: &str,
        digest_seed: u8,
        request_seed: u8,
    ) -> CommandExecutionPreparation {
        self.prepare_organization_for_mode(
            ports,
            organization_id,
            caller_key_text,
            digest_seed,
            request_seed,
            false,
        )
    }

    pub(crate) fn prepare_audited_organization_for(
        &self,
        ports: &RedbOperationalPorts,
        organization_id: [u8; 16],
        caller_key_text: &str,
        digest_seed: u8,
        request_seed: u8,
    ) -> CommandExecutionPreparation {
        self.prepare_organization_for_mode(
            ports,
            organization_id,
            caller_key_text,
            digest_seed,
            request_seed,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_organization_for_mode(
        &self,
        ports: &RedbOperationalPorts,
        organization_id: [u8; 16],
        caller_key_text: &str,
        digest_seed: u8,
        request_seed: u8,
        audited: bool,
    ) -> CommandExecutionPreparation {
        let plan = self.command_plan("CreateOrganization");
        let reference = ExecutablePlanRef::new(
            self.checked_bundle.lineage().clone(),
            self.checked_bundle.contract_version(),
            self.checked_bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let resolved = resolve_executable_plan(ports, &reference)
            .expect("deployed CreateOrganization plan resolves");
        let input = input_record(
            plan.input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(caller_key_text).expect("bounded caller key"),
                ),
                ("organization_id", CanonicalValue::Uuid(organization_id)),
            ],
        );
        let facts = derive_input_command_facts(plan, input.clone())
            .expect("checked CreateOrganization input facts");
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
                .expect("prepare organization caller-key lookup");
        let idempotency = IdempotencyInspectionExecutor::new(ports)
            .inspect(lookup)
            .expect("inspect organization idempotency state")
            .confirm_input(
                &input,
                plan.idempotency_input()
                    .expect("CreateOrganization idempotency input"),
                &caller_key,
            )
            .expect("confirm organization command input")
            .bind_selected_plan(reference)
            .expect("bind organization command plan");
        let authorization = authorize_command(
            plan,
            self.checked_bundle.lineage().clone(),
            facts.partition_key().clone(),
        );
        let post_evaluation_authorization = audited.then(|| {
            authorize_command(
                plan,
                self.checked_bundle.lineage().clone(),
                facts.partition_key().clone(),
            )
        });
        let (control, _cancellation) = CommandRequestControl::new(
            Instant::now()
                .checked_add(Duration::from_secs(30))
                .expect("representable organization command deadline"),
        );
        let preparation = CommandExecutionPreparation::new(
            database_id(),
            &environment(),
            resolved,
            input,
            idempotency,
            facts,
            authorization,
            request_id(request_seed),
            riffdb_types::ServiceIngressKindV1::Grpc,
            control,
        )
        .expect("join exact organization preparation proofs");
        if let Some(post_evaluation_authorization) = post_evaluation_authorization {
            preparation
                .with_audited_lifecycle(Box::new(StartedCommandAuditInput::new(request_id(
                    request_seed,
                ))))
                .expect("attach matching checked Started audit lifecycle")
                .with_post_evaluation_authorizer(Box::new(
                    FixedPostEvaluationCommandAuthorizer::new(post_evaluation_authorization),
                ))
                .expect("attach exact post-evaluation authorization safe point")
        } else {
            preparation
        }
    }

    pub(crate) fn assert_user_exists(
        &self,
        ports: &RedbOperationalPorts,
        user_id: [u8; 16],
        expected: bool,
    ) {
        assert_eq!(
            ports
                .read_entity(&self.user_target(user_id))
                .expect("read unique user")
                .is_some(),
            expected
        );
    }

    pub(crate) fn assert_user_email(
        &self,
        ports: &RedbOperationalPorts,
        user_id: [u8; 16],
        expected: &str,
    ) {
        let user = ports
            .read_entity(&self.user_target(user_id))
            .expect("read unique user")
            .expect("unique user exists");
        assert_eq!(
            user.fields()
                .fields()
                .iter()
                .find(|(field, _)| *field == self.email_field)
                .map(|(_, value)| value),
            Some(&CanonicalValue::string(expected).expect("bounded expected email"))
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

    fn user_target(&self, user_id: [u8; 16]) -> riffdb_storage_api::EntityTarget {
        let mut key = EntityKeyBuilder::new(self.user_entity_type);
        key.push_uuid(&ORGANIZATION_ID)
            .expect("organization key component");
        key.push_uuid(&user_id).expect("user key component");
        riffdb_storage_api::EntityTarget::new(
            self.user_entity_type,
            key.finish().expect("user entity key"),
        )
        .expect("User entity target")
    }
}

impl Drop for UniqueUserDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
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
        let lookup = prepare_idempotency_lookup(&scope, &caller_key, &FixedDigestProvider(0x51))
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
            riffdb_types::ServiceIngressKindV1::Grpc,
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

pub(crate) struct IncrementingProvenanceSource {
    seed: u8,
    calls: AtomicUsize,
}

impl IncrementingProvenanceSource {
    pub(crate) fn new(seed: u8) -> Self {
        Self {
            seed,
            calls: AtomicUsize::new(0),
        }
    }
}

impl ProvenanceIdSource for IncrementingProvenanceSource {
    fn next_provenance_id(&self) -> Result<ProvenanceId, ProvenanceIdSourceError> {
        let offset = self.calls.fetch_add(1, Ordering::Relaxed);
        let offset = u8::try_from(offset).unwrap_or(u8::MAX);
        ProvenanceId::from_bytes(uuid_bytes(self.seed.wrapping_add(offset)))
            .map_err(|_| ProvenanceIdSourceError)
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

pub(crate) fn start_coordinator_with_notifications<P>(
    ports: RedbOperationalPorts,
    admission_clock: Arc<FixedAdmissionClock>,
    provenance_source: Arc<P>,
    notifications: Arc<dyn ApplicationCommitNotificationSink>,
) -> RunningCommandCoordinator
where
    P: ProvenanceIdSource + 'static,
{
    start_coordinator_with_durability(
        ports,
        admission_clock,
        provenance_source,
        notifications,
        CoordinatorDurability::Sync,
    )
}

pub(crate) fn start_group_coordinator_with_notifications<P>(
    ports: RedbOperationalPorts,
    admission_clock: Arc<FixedAdmissionClock>,
    provenance_source: Arc<P>,
    notifications: Arc<dyn ApplicationCommitNotificationSink>,
) -> RunningCommandCoordinator
where
    P: ProvenanceIdSource + 'static,
{
    start_coordinator_with_durability(
        ports,
        admission_clock,
        provenance_source,
        notifications,
        CoordinatorDurability::Group,
    )
}

fn start_coordinator_with_durability<P>(
    ports: RedbOperationalPorts,
    admission_clock: Arc<FixedAdmissionClock>,
    provenance_source: Arc<P>,
    notifications: Arc<dyn ApplicationCommitNotificationSink>,
    durability: CoordinatorDurability,
) -> RunningCommandCoordinator
where
    P: ProvenanceIdSource + 'static,
{
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
        durability,
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

pub(crate) fn start_group_coordinator_with_commit_telemetry<P>(
    ports: RedbOperationalPorts,
    admission_clock: Arc<FixedAdmissionClock>,
    provenance_source: Arc<P>,
    notifications: Arc<dyn ApplicationCommitNotificationSink>,
    telemetry: Arc<dyn CommitTelemetry>,
) -> RunningCommandCoordinator
where
    P: ProvenanceIdSource + 'static,
{
    let conflicts: Arc<dyn ConflictManager> = Arc::new(
        ShardedConflictManager::new(ConflictManagerConfig::default())
            .expect("start conflict manager"),
    );
    RunningCommandCoordinator::start_with_commit_telemetry(
        CoordinatorWorkloadCapacity::new(8).expect("nonzero coordinator capacity"),
        CoordinatorDurability::Group,
        ports,
        conflicts,
        admission_clock,
        Arc::new(FixedAdministrationClock(timestamp(1_700_000_002))),
        Arc::new(FixedAuthorizationClock(timestamp(1_700_000_002))),
        provenance_source,
        notifications,
        telemetry,
    )
    .expect("start group coordinator with commit telemetry")
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
    let riffdb_catalog::CatalogHistoryOutcome::Ready(history) = history else {
        panic!("V2-only command fixture must not require index migration");
    };
    let riffdb_storage_api::StructuralOpenOutcome::Clean(opened) = opened else {
        panic!("V2-only command fixture must finish with a clean structural open");
    };
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
