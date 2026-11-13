#![forbid(unsafe_code)]
// req: TEST-001, TEST-003, TXN-001, TXN-002, CMP-001

//! Production-coordinator sequence exploration across deferred group-commit epochs,
//! duplicate idempotent submissions, conflict-key serialization, and post-mutation
//! aggregate `invariant` rejections (`CommitCheckRejected`).

use std::collections::BTreeMap;
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU64};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

use riffdb_catalog::{
    ResolvedExecutablePlan, ValidatedContractBundle, resolve_executable_plan,
    validate_catalog_history,
};
use riffdb_commit::{
    AdministrationAuditInputView, AdministrationClock, AdministrationClockError, AdmissionClock,
    AdmissionClockError, ApplicationCommitNotificationError, ApplicationCommitNotificationSink,
    CommandExecutionErrorKind, CommandExecutionPreparation, CommandExecutionResult,
    CommandIdempotencyInspectionRequest, CommandIdempotencyInspector, CommandRequestControl,
    CommittedOutcomeDisposition, CoordinatorDurability, CoordinatorLifecycleState,
    CoordinatorWorkloadCapacity, PostEvaluationAuthorizationError, PostEvaluationCommandAuthorizer,
    ProvenanceIdSource, ProvenanceIdSourceError, RunningCommandCoordinator, ServiceUuidV7Source,
    ServiceUuidV7SourceError,
};
use riffdb_conflict::{ConflictManager, ConflictManagerConfig, ShardedConflictManager};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{CommandPlan, RecordSchema};
use riffdb_idempotency::{
    IdempotencyDigestCandidatesV1, IdempotencyDigestError, IdempotencyDigestProvider,
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
    EntityTarget, EvidencePageLimit, ExecutablePlanRef, IdempotencyKeyDigest,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StoredOutcomeV1, StructuralEvidenceCursor, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession,
};
use riffdb_storage_redb::{
    RedbCheckpointTestPoint as Point, RedbDormantPorts, RedbOperationalPorts, RedbStore,
    RedbTestController,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_testkit::scratch::ScratchDir;
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, Audience, CanonicalRecord, CanonicalValue, CapabilityGrantV1,
    CapabilityId, CapabilityPermissionV1, CapabilityPermissionsV1, CommitSequence, ContractLineage,
    DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId, Environment, FieldId, IdempotencyKey,
    PartitionKey, PartitionScopeV1, ProvenanceId, RequestId, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1,
    TenantScope, Timestamp,
};

const ENVIRONMENT: &str = "deferred-pipeline-test";
const PRINCIPAL: &str = "ledger-agent";
const TENANT_ID: [u8; 16] = [0x41; 16];
const ACCOUNT_ID: [u8; 16] = [0x52; 16];

const CREDIT_LEDGER_SOURCE: &str = r#"
contract CreditLedger version 1 {
  entity Account {
    key (tenant_id: uuid, account_id: uuid)
    field credit_limit: i64
    field posted_total: i64
  }

  entity Charge {
    key (tenant_id: uuid, account_id: uuid, charge_id: uuid)
    field amount: i64
  }

  aggregate Accounts {
    root Account
    child Charge
    partition_by tenant_id
    conflict_key (tenant_id, account_id)
    invariant within_credit_limit: posted_total <= credit_limit
    invariant non_negative_posted: posted_total >= 0
  }

  command OpenAccount {
    input idempotency_key: string<128>
    input tenant_id: uuid
    input account_id: uuid
    input credit_limit: i64

    idempotency_key idempotency_key
    create Account(tenant_id, account_id) as account
      else AccountExists { account_id: account_id }

    require positive_limit: credit_limit > 0
      else InvalidLimit {}

    set account.credit_limit = credit_limit
    set account.posted_total = 0
    return AccountOpened { account: account }
  }

  command PostCharge {
    input idempotency_key: string<128>
    input tenant_id: uuid
    input account_id: uuid
    input charge_id: uuid
    input amount: i64

    idempotency_key idempotency_key
    mutate Account(tenant_id, account_id) as account
      else AccountMissing { account_id: account_id }
    create Charge(tenant_id, account_id, charge_id) as charge
      else ChargeExists { charge_id: charge_id }

    set charge.amount = amount
    set account.posted_total = account.posted_total + amount
    return ChargePosted {
      account: account,
      charge: charge
    }
  }
}
"#;

struct CreditLedgerDatabase {
    path: PathBuf,
    checked_bundle: ValidatedContractBundle,
    open_account_plan: ResolvedExecutablePlan,
    post_charge_plan: ResolvedExecutablePlan,
    account_entity_type: EntityTypeId,
    posted_total_field: FieldId,
    charge_entity_type: EntityTypeId,
    charge_amount_field: FieldId,
    charge_posted_account_field: FieldId,
    request_counter: AtomicU64,
    _scratch: ScratchDir,
}

impl CreditLedgerDatabase {
    fn create(label: &str) -> Self {
        let scratch =
            ScratchDir::new(&format!("deferred-seq-{label}")).expect("create scratch dir");
        let path = scratch.join("ledger.redb");
        let compiled =
            compile_contract_source(CREDIT_LEDGER_SOURCE).expect("compile CreditLedger contract");
        let checked_bundle =
            ValidatedContractBundle::from_compiler_bundle(compiled).expect("validate bundle");

        let (
            account_entity_type,
            posted_total_field,
            charge_entity_type,
            charge_amount_field,
            charge_posted_account_field,
            open_ref,
            post_ref,
        ) = {
            let bundle = checked_bundle.bundle();
            let open_cmd = bundle
                .commands()
                .iter()
                .find(|plan| plan.name() == "OpenAccount")
                .expect("OpenAccount command");
            let post_cmd = bundle
                .commands()
                .iter()
                .find(|plan| plan.name() == "PostCharge")
                .expect("PostCharge command");
            assert_eq!(
                post_cmd.commit_checks().len(),
                2,
                "PostCharge must retain both aggregate post-mutation commit checks"
            );
            let charge_posted_outcome = post_cmd
                .outcomes()
                .iter()
                .find(|outcome| outcome.name() == "ChargePosted")
                .expect("ChargePosted outcome");
            let charge_posted_account_field = charge_posted_outcome
                .payload()
                .fields()
                .iter()
                .find(|field| field.name() == "account")
                .expect("account payload field")
                .id();

            let schema = bundle.schema();
            let account = schema
                .entities()
                .iter()
                .find(|entity| entity.name() == "Account")
                .expect("Account entity");
            let posted_total_field = account
                .record()
                .fields()
                .iter()
                .find(|field| field.name() == "posted_total")
                .expect("posted_total field")
                .id();
            let charge = schema
                .entities()
                .iter()
                .find(|entity| entity.name() == "Charge")
                .expect("Charge entity");
            let charge_amount_field = charge
                .record()
                .fields()
                .iter()
                .find(|field| field.name() == "amount")
                .expect("amount field")
                .id();

            let open_ref = ExecutablePlanRef::new(
                checked_bundle.lineage().clone(),
                checked_bundle.contract_version(),
                checked_bundle.bundle_hash(),
                open_cmd.command_id(),
                open_cmd.plan_hash(),
            );
            let post_ref = ExecutablePlanRef::new(
                checked_bundle.lineage().clone(),
                checked_bundle.contract_version(),
                checked_bundle.bundle_hash(),
                post_cmd.command_id(),
                post_cmd.plan_hash(),
            );
            (
                account.id(),
                posted_total_field,
                charge.id(),
                charge_amount_field,
                charge_posted_account_field,
                open_ref,
                post_ref,
            )
        };

        let mut store = RedbStore::open(&path).expect("create redb store");
        store
            .initialize_database(database_id())
            .expect("initialize database");
        let mut ports = open_operational(store);
        let stored_bundle = checked_bundle.to_stored().expect("encode stored bundle");
        let activation = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                stored_bundle.clone(),
                request_id_from_counter(1),
                catalog_principal(),
                timestamp(1_700_000_000),
                None,
            ))
            .expect("activate CreditLedger bundle");
        assert!(matches!(
            activation,
            CatalogActivationResult::Activated { active, .. }
                if active == ActiveCatalogPointerV1::from_bundle(&stored_bundle)
        ));
        let open_account_plan =
            resolve_executable_plan(&ports, &open_ref).expect("resolve OpenAccount plan");
        let post_charge_plan =
            resolve_executable_plan(&ports, &post_ref).expect("resolve PostCharge plan");
        drop(ports);

        Self {
            path,
            checked_bundle,
            open_account_plan,
            post_charge_plan,
            account_entity_type,
            posted_total_field,
            charge_entity_type,
            charge_amount_field,
            charge_posted_account_field,
            request_counter: AtomicU64::new(10),
            _scratch: scratch,
        }
    }

    fn open(&self) -> RedbOperationalPorts {
        open_operational(RedbStore::open(&self.path).expect("open ledger database"))
    }

    fn open_with_controller(&self, controller: RedbTestController) -> RedbOperationalPorts {
        open_operational(
            RedbStore::open_with_test_controller(&self.path, controller)
                .expect("open ledger database with checkpoint controller"),
        )
    }

    fn outcome_id(&self, command_name: &str, outcome_name: &str) -> riffdb_types::OutcomeId {
        self.checked_bundle
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == command_name)
            .unwrap_or_else(|| panic!("missing command plan {command_name}"))
            .outcomes()
            .iter()
            .find(|outcome| outcome.name() == outcome_name)
            .unwrap_or_else(|| panic!("missing outcome {outcome_name} on {command_name}"))
            .id()
    }

    async fn prepare_open_account(
        &self,
        inspector: &CommandIdempotencyInspector,
        caller_key: &str,
        credit_limit: i64,
    ) -> CommandExecutionPreparation {
        let plan = self.open_account_plan.plan();
        let input = input_record(
            plan.input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(caller_key).expect("caller key"),
                ),
                ("tenant_id", CanonicalValue::Uuid(TENANT_ID)),
                ("account_id", CanonicalValue::Uuid(ACCOUNT_ID)),
                ("credit_limit", CanonicalValue::I64(credit_limit)),
            ],
        );
        self.prepare_checked(inspector, &self.open_account_plan, input, caller_key)
            .await
    }

    async fn prepare_post_charge(
        &self,
        inspector: &CommandIdempotencyInspector,
        caller_key: &str,
        charge_id: [u8; 16],
        amount: i64,
    ) -> CommandExecutionPreparation {
        let plan = self.post_charge_plan.plan();
        let input = input_record(
            plan.input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(caller_key).expect("caller key"),
                ),
                ("tenant_id", CanonicalValue::Uuid(TENANT_ID)),
                ("account_id", CanonicalValue::Uuid(ACCOUNT_ID)),
                ("charge_id", CanonicalValue::Uuid(charge_id)),
                ("amount", CanonicalValue::I64(amount)),
            ],
        );
        self.prepare_checked(inspector, &self.post_charge_plan, input, caller_key)
            .await
    }

    async fn prepare_checked(
        &self,
        inspector: &CommandIdempotencyInspector,
        resolved: &ResolvedExecutablePlan,
        input: CanonicalRecord,
        caller_key_text: &str,
    ) -> CommandExecutionPreparation {
        let plan = resolved.plan();
        let reference = resolved.reference();
        let facts =
            derive_input_command_facts(plan, input.clone()).expect("derive input command facts");
        let caller_key = IdempotencyKey::new(caller_key_text).expect("bounded caller key");
        let inspection_req = CommandIdempotencyInspectionRequest::new(
            database_id(),
            environment(),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("principal"),
            reference.contract_lineage().clone(),
            reference.command_id(),
            caller_key,
        );
        let inspected = inspector
            .inspect(inspection_req)
            .await
            .expect("inspect durable idempotency");
        let idempotency = inspected
            .confirm_selected_plan(
                &input,
                plan.idempotency_input().expect("command idempotency input"),
                reference.clone(),
            )
            .expect("confirm selected plan");

        let req_id = request_id_from_counter(self.request_counter.fetch_add(1, Ordering::Relaxed));
        let authorization = authorize_command(
            plan,
            self.checked_bundle.lineage().clone(),
            facts.partition_key().clone(),
        );
        let post_eval = FixedPostEvaluationCommandAuthorizer {
            plan: plan.clone(),
            lineage: self.checked_bundle.lineage().clone(),
            partition: facts.partition_key().clone(),
        };
        let (control, _cancellation) = CommandRequestControl::new(
            Instant::now()
                .checked_add(Duration::from_secs(30))
                .expect("deadline"),
        );

        CommandExecutionPreparation::new(
            database_id(),
            &environment(),
            resolved.clone(),
            input,
            idempotency,
            facts,
            authorization,
            req_id,
            ServiceIngressKindV1::Grpc,
            control,
        )
        .expect("build command execution preparation")
        .with_audited_lifecycle(Box::new(StartedCommandAuditInput::new(req_id)))
        .expect("attach audited lifecycle")
        .with_post_evaluation_authorizer(Box::new(post_eval))
        .expect("attach post-evaluation authorizer")
    }

    fn posted_total_from_outcome(&self, outcome: &StoredOutcomeV1) -> i64 {
        let account_val = outcome
            .declared_outcome()
            .value()
            .fields()
            .iter()
            .find(|(fid, _)| *fid == self.charge_posted_account_field)
            .map(|(_, val)| val)
            .expect("account field in ChargePosted payload");
        let CanonicalValue::Record(account_rec) = account_val else {
            panic!("expected Record for account in ChargePosted");
        };
        account_rec
            .fields()
            .iter()
            .find(|(fid, _)| *fid == self.posted_total_field)
            .and_then(|(_, val)| match val {
                CanonicalValue::I64(total) => Some(*total),
                _ => None,
            })
            .expect("posted_total in ChargePosted.account")
    }

    fn read_posted_total(&self, ports: &RedbOperationalPorts) -> Option<i64> {
        let mut key = EntityKeyBuilder::new(self.account_entity_type);
        key.push_uuid(&TENANT_ID).expect("tenant key");
        key.push_uuid(&ACCOUNT_ID).expect("account key");
        let target = EntityTarget::new(
            self.account_entity_type,
            key.finish().expect("account entity key"),
        )
        .expect("account entity target");
        let stored = ports.read_entity(&target).expect("read Account")?;
        stored
            .fields()
            .fields()
            .iter()
            .find(|(field, _)| *field == self.posted_total_field)
            .and_then(|(_, value)| match value {
                CanonicalValue::I64(total) => Some(*total),
                _ => None,
            })
    }

    fn read_charge_amount(&self, ports: &RedbOperationalPorts, charge_id: [u8; 16]) -> Option<i64> {
        let mut key = EntityKeyBuilder::new(self.charge_entity_type);
        key.push_uuid(&TENANT_ID).expect("tenant key");
        key.push_uuid(&ACCOUNT_ID).expect("account key");
        key.push_uuid(&charge_id).expect("charge key");
        let target = EntityTarget::new(
            self.charge_entity_type,
            key.finish().expect("charge entity key"),
        )
        .expect("charge entity target");
        let stored = ports.read_entity(&target).expect("read Charge")?;
        stored
            .fields()
            .fields()
            .iter()
            .find(|(field, _)| *field == self.charge_amount_field)
            .and_then(|(_, value)| match value {
                CanonicalValue::I64(amount) => Some(*amount),
                _ => None,
            })
    }
}

// req: TEST-001, TEST-003, TXN-001, TXN-002
#[test]
fn intra_group_and_cross_epoch_duplicate_idempotent_submissions_coalesce_and_replay_cleanly() {
    let db = CreditLedgerDatabase::create("cross-epoch-duplicates");
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = db.open_with_controller(controller);
    let coordinator = start_coordinator(ports, CoordinatorDurability::Group, 32);
    let executor = coordinator.command_executor();
    let inspector = coordinator.command_idempotency_inspector(Arc::new(DeterministicKeyDigests));
    // Bind `pauses` AFTER `coordinator` so reverse-drop order on panic always releases
    // paused storage workers before coordinator shutdown joins them.
    let pauses = schedule;
    let rt = runtime();

    let open = rt.block_on(db.prepare_open_account(&inspector, "open-acct-1", 500));
    let open_res = submit_sync(&rt, &executor, open).expect("OpenAccount commits");
    let CommandExecutionResult::Committed(open_committed) = open_res else {
        panic!("expected Committed OpenAccount");
    };
    assert_eq!(
        open_committed.disposition(),
        CommittedOutcomeDisposition::FirstCommit
    );
    assert_eq!(
        open_committed.stored_outcome().commit_sequence(),
        CommitSequence::first()
    );

    // Prepare 4 identical submissions + 1 mismatched payload + 1 distinct charge BEFORE
    // any of them publish, matching concurrent callers arriving before durable visibility.
    let charge_1 = charge_uuid(1);
    let charge_2 = charge_uuid(2);
    let (dup_1, dup_2, dup_3, dup_4, key_mismatch_in_flight, sibling_charge) = rt.block_on(async {
        (
            db.prepare_post_charge(&inspector, "charge-dup-epoch", charge_1, 40)
                .await,
            db.prepare_post_charge(&inspector, "charge-dup-epoch", charge_1, 40)
                .await,
            db.prepare_post_charge(&inspector, "charge-dup-epoch", charge_1, 40)
                .await,
            db.prepare_post_charge(&inspector, "charge-dup-epoch", charge_1, 40)
                .await,
            db.prepare_post_charge(&inspector, "charge-dup-epoch", charge_1, 99)
                .await,
            db.prepare_post_charge(&inspector, "charge-sibling", charge_2, 25)
                .await,
        )
    });

    // Hold epoch 1 in the deferred group pipeline (BeforeJournalSync -> BeforePublication)
    // while duplicates and sibling commands cross unpublished group-commit units.
    pauses.arm(Point::BeforeMaterialization);
    pauses.arm(Point::BeforeJournalSync);
    pauses.arm(Point::BeforePublication);
    pauses.arm(Point::Materialized);
    pauses.arm(Point::AfterCommandSeal);
    pauses.request_checkpoint();

    let receipt_1 = rt
        .block_on(executor.reserve_capacity())
        .expect("cap 1")
        .submit(dup_1)
        .expect("submit dup_1");
    pauses.wait(Point::BeforeMaterialization);
    pauses.wait(Point::AfterCommandSeal);
    pauses.release(Point::AfterCommandSeal);
    pauses.wait(Point::BeforeJournalSync);

    let mut fut_1 = Box::pin(receipt_1.completion());
    let mut noop_cx = Context::from_waker(Waker::noop());
    assert!(
        fut_1.as_mut().poll(&mut noop_cx).is_pending(),
        "predecessor group unit must still be unpublished while paused at BeforeJournalSync"
    );

    // Submit dup_2, dup_3, key_mismatch_in_flight, and sibling_charge while epoch 1's
    // journal sync is still in flight. Because dup_2 overlaps dup_1's DeferredPipelineFootprint,
    // the coordinator drains dup_1's unpublished flush before rechecking durable admission.
    let receipt_2 = rt
        .block_on(executor.reserve_capacity())
        .expect("cap 2")
        .submit(dup_2)
        .expect("submit dup_2");
    let receipt_3 = rt
        .block_on(executor.reserve_capacity())
        .expect("cap 3")
        .submit(dup_3)
        .expect("submit dup_3");
    let receipt_mismatch = rt
        .block_on(executor.reserve_capacity())
        .expect("cap mismatch")
        .submit(key_mismatch_in_flight)
        .expect("submit mismatch");
    let receipt_sibling = rt
        .block_on(executor.reserve_capacity())
        .expect("cap sibling")
        .submit(sibling_charge)
        .expect("submit sibling");

    let mut fut_2 = Box::pin(receipt_2.completion());
    assert!(fut_2.as_mut().poll(&mut noop_cx).is_pending());

    pauses.release(Point::BeforeJournalSync);
    pauses.wait(Point::BeforePublication);

    // Submit dup_4 while predecessor unit is synced in journal but not yet published!
    let receipt_4 = rt
        .block_on(executor.reserve_capacity())
        .expect("cap 4")
        .submit(dup_4)
        .expect("submit dup_4");
    assert!(fut_1.as_mut().poll(&mut noop_cx).is_pending());
    assert!(fut_2.as_mut().poll(&mut noop_cx).is_pending());

    pauses.release(Point::BeforePublication);
    pauses.release(Point::BeforeMaterialization);
    pauses.wait(Point::Materialized);
    pauses.release(Point::Materialized);

    let res_1 = rt.block_on(fut_1).expect("dup_1 completes");
    let res_2 = rt.block_on(fut_2).expect("dup_2 completes");
    let res_3 = rt
        .block_on(receipt_3.completion())
        .expect("dup_3 completes");
    let res_4 = rt
        .block_on(receipt_4.completion())
        .expect("dup_4 completes");
    let res_mismatch = rt
        .block_on(receipt_mismatch.completion())
        .expect("mismatch completes without stopping coordinator");
    let res_sibling = rt
        .block_on(receipt_sibling.completion())
        .expect("sibling completes");

    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Accepting,
        "coordinator must remain Accepting across multi-duplicate deferred pipeline epochs"
    );
    assert!(matches!(
        res_mismatch,
        CommandExecutionResult::InputMismatch
    ));

    let dup_outcomes = [res_1, res_2, res_3, res_4]
        .into_iter()
        .map(|res| match res {
            CommandExecutionResult::Committed(committed) => committed,
            other => panic!("expected Committed/Replay outcome, got {other:?}"),
        })
        .collect::<Vec<_>>();
    let first_commits = dup_outcomes
        .iter()
        .filter(|outcome| outcome.disposition() == CommittedOutcomeDisposition::FirstCommit)
        .count();
    let replays = dup_outcomes
        .iter()
        .filter(|outcome| outcome.disposition() == CommittedOutcomeDisposition::Replay)
        .count();
    assert_eq!(first_commits, 1, "exactly one duplicate may FirstCommit");
    assert_eq!(replays, 3, "remaining three duplicates must Replay");

    let canonical_stored: StoredOutcomeV1 = dup_outcomes[0].stored_outcome().clone();
    for outcome in &dup_outcomes[1..] {
        assert_eq!(outcome.stored_outcome(), &canonical_stored);
    }

    let CommandExecutionResult::Committed(sibling_committed) = res_sibling else {
        panic!("expected sibling charge to commit");
    };
    assert_eq!(
        sibling_committed.disposition(),
        CommittedOutcomeDisposition::FirstCommit
    );
    assert_eq!(
        sibling_committed.stored_outcome().commit_sequence().get(),
        canonical_stored.commit_sequence().get() + 1
    );
    assert_eq!(
        db.posted_total_from_outcome(sibling_committed.stored_outcome()),
        65
    );

    // Post-flush durable replay and key-reuse mismatch through the live inspector + coordinator.
    let post_flush_replay =
        rt.block_on(db.prepare_post_charge(&inspector, "charge-dup-epoch", charge_1, 40));
    let post_flush_res =
        submit_sync(&rt, &executor, post_flush_replay).expect("post-flush replay completes");
    let CommandExecutionResult::Committed(post_flush_outcome) = post_flush_res else {
        panic!("expected Committed replay");
    };
    assert_eq!(
        post_flush_outcome.disposition(),
        CommittedOutcomeDisposition::Replay
    );
    assert_eq!(post_flush_outcome.stored_outcome(), &canonical_stored);

    let post_flush_mismatch =
        rt.block_on(db.prepare_post_charge(&inspector, "charge-dup-epoch", charge_1, 41));
    let mismatch_res =
        submit_sync(&rt, &executor, post_flush_mismatch).expect("post-flush mismatch completes");
    assert!(matches!(
        mismatch_res,
        CommandExecutionResult::InputMismatch
    ));

    drop(inspector);
    drop(executor);
    drop(pauses);
    coordinator.shutdown().expect("clean coordinator shutdown");

    let reopened = db.open();
    assert_eq!(db.read_posted_total(&reopened), Some(65));
    assert_eq!(db.read_charge_amount(&reopened, charge_1), Some(40));
    assert_eq!(db.read_charge_amount(&reopened, charge_2), Some(25));
}

// req: TEST-001, TEST-003, TXN-001, TXN-002, CMP-001
#[test]
fn post_mutation_aggregate_invariant_rejection_rolls_back_without_poisoning_idempotency_or_coordinator()
 {
    for durability in [CoordinatorDurability::Group, CoordinatorDurability::Sync] {
        let label = match durability {
            CoordinatorDurability::Group => "invariant-group",
            CoordinatorDurability::Sync => "invariant-sync",
        };
        let db = CreditLedgerDatabase::create(label);
        let ports = db.open();
        let coordinator = start_coordinator(ports, durability, 16);
        let executor = coordinator.command_executor();
        let inspector =
            coordinator.command_idempotency_inspector(Arc::new(DeterministicKeyDigests));
        let rt = runtime();

        // 1. Open account with credit_limit = 100.
        let open_prep = rt.block_on(db.prepare_open_account(&inspector, "open-inv-1", 100));
        let open_res = submit_sync(&rt, &executor, open_prep).expect("OpenAccount commits");
        assert!(matches!(open_res, CommandExecutionResult::Committed(_)));

        // 2. Post valid charge of 60 -> posted_total = 60, sequence = 2.
        let c1 = charge_uuid(10);
        let valid_1_prep = rt.block_on(db.prepare_post_charge(&inspector, "charge-ok-1", c1, 60));
        let res_valid_1 = submit_sync(&rt, &executor, valid_1_prep).expect("valid charge commits");
        let CommandExecutionResult::Committed(valid_1) = res_valid_1 else {
            panic!("expected valid charge to commit");
        };
        assert_eq!(valid_1.stored_outcome().commit_sequence().get(), 2);
        assert_eq!(db.posted_total_from_outcome(valid_1.stored_outcome()), 60);

        // 3. Violate `within_credit_limit` (`60 + 50 = 110 > 100`) at post-mutation commit check.
        let c_over = charge_uuid(11);
        let over_prep =
            rt.block_on(db.prepare_post_charge(&inspector, "charge-over-limit", c_over, 50));
        let err_over = submit_sync(&rt, &executor, over_prep)
            .expect_err("post-mutation invariant violation must fail command execution");
        assert_eq!(
            err_over.kind(),
            CommandExecutionErrorKind::CommitCheckRejected
        );
        assert_eq!(
            executor.lifecycle_state(),
            CoordinatorLifecycleState::Accepting,
            "CommitCheckRejected must never stop or fence the coordinator"
        );

        // 4. Violate `non_negative_posted` (`60 + (-75) = -15 < 0`) at post-mutation commit check.
        let c_neg = charge_uuid(12);
        let neg_prep =
            rt.block_on(db.prepare_post_charge(&inspector, "charge-negative", c_neg, -75));
        let err_neg = submit_sync(&rt, &executor, neg_prep)
            .expect_err("negative posted_total invariant violation must fail");
        assert_eq!(
            err_neg.kind(),
            CommandExecutionErrorKind::CommitCheckRejected
        );

        // 5. Because `CommitCheckRejected` does not persist a terminal outcome or allocate a
        //    commit sequence, the next valid charge commits cleanly at CommitSequence(3).
        let c2 = charge_uuid(13);
        let valid_2_prep = rt.block_on(db.prepare_post_charge(&inspector, "charge-ok-2", c2, 40));
        let res_valid_2 =
            submit_sync(&rt, &executor, valid_2_prep).expect("subsequent valid charge commits");
        let CommandExecutionResult::Committed(valid_2) = res_valid_2 else {
            panic!("expected second valid charge to commit");
        };
        assert_eq!(
            valid_2.stored_outcome().commit_sequence().get(),
            3,
            "failed commit checks must not leave sequence gaps"
        );
        assert_eq!(db.posted_total_from_outcome(valid_2.stored_outcome()), 100);

        drop(inspector);
        drop(executor);
        coordinator.shutdown().expect("clean coordinator shutdown");

        let reopened = db.open();
        assert_eq!(db.read_posted_total(&reopened), Some(100));
        assert_eq!(db.read_charge_amount(&reopened, c1), Some(60));
        assert_eq!(db.read_charge_amount(&reopened, c_over), None);
        assert_eq!(db.read_charge_amount(&reopened, c_neg), None);
        assert_eq!(db.read_charge_amount(&reopened, c2), Some(40));
    }
}

// req: TEST-001, TEST-003, TXN-001, TXN-002, CMP-001
#[test]
fn concurrent_same_conflict_key_mutations_and_duplicates_match_reference_oracle_across_modes() {
    for durability in [CoordinatorDurability::Group, CoordinatorDurability::Sync] {
        let label = match durability {
            CoordinatorDurability::Group => "concurrent-oracle-group",
            CoordinatorDurability::Sync => "concurrent-oracle-sync",
        };
        let db = CreditLedgerDatabase::create(label);
        let ports = db.open();
        let coordinator = start_coordinator(ports, durability, 64);
        let executor = coordinator.command_executor();
        let inspector =
            coordinator.command_idempotency_inspector(Arc::new(DeterministicKeyDigests));
        let rt = runtime();

        let open_prep = rt.block_on(db.prepare_open_account(&inspector, "open-conc-1", 150));
        submit_sync(&rt, &executor, open_prep).expect("OpenAccount commits");

        // Prepare a wave of concurrent submissions sharing conflict_key (TENANT_ID, ACCOUNT_ID):
        // - 5 valid distinct charges of +20 each (total +100 <= 150), with key #0 and #2 each
        //   submitted 3 times concurrently (intra-wave duplicates).
        // - 2 invariant-violating charges of +200 each (always > 150 regardless of order -> CommitCheckRejected).
        let wave = rt.block_on(async {
            let mut items = Vec::new();
            for idx in 0_u8..5 {
                let key = format!("wave-valid-{idx}");
                let copies = if idx % 2 == 0 { 3 } else { 1 };
                for _ in 0..copies {
                    let prep = db
                        .prepare_post_charge(&inspector, &key, charge_uuid(30 + idx), 20)
                        .await;
                    items.push((key.clone(), true, prep));
                }
            }
            for bad_idx in 0_u8..2 {
                let key = format!("wave-reject-{bad_idx}");
                let prep = db
                    .prepare_post_charge(&inspector, &key, charge_uuid(60 + bad_idx), 200)
                    .await;
                items.push((key, false, prep));
            }
            items
        });

        let results = rt.block_on(async {
            let mut receipts = Vec::with_capacity(wave.len());
            for (key, expect_ok, prep) in wave {
                let permit = executor.reserve_capacity().await.expect("reserve capacity");
                let receipt = permit.submit(prep).expect("submit wave command");
                receipts.push((key, expect_ok, receipt));
            }
            let mut collected = Vec::with_capacity(receipts.len());
            for (key, expect_ok, receipt) in receipts {
                collected.push((key, expect_ok, receipt.completion().await));
            }
            collected
        });

        let mut by_key: BTreeMap<String, Vec<riffdb_commit::CommittedOutcome>> = BTreeMap::new();
        for (key, expect_ok, outcome) in results {
            if expect_ok {
                let res = outcome.expect("valid wave command succeeds");
                let CommandExecutionResult::Committed(committed) = res else {
                    panic!("expected Committed for {key}, got {res:?}");
                };
                assert_eq!(committed.durability().expect("durability"), durability);
                by_key.entry(key).or_default().push(committed);
            } else {
                let err = outcome.expect_err("over-limit wave command must reject");
                assert_eq!(err.kind(), CommandExecutionErrorKind::CommitCheckRejected);
            }
        }

        assert_eq!(by_key.len(), 5);
        let mut sequences = Vec::new();
        for (key, outcomes) in &by_key {
            let first_count = outcomes
                .iter()
                .filter(|o| o.disposition() == CommittedOutcomeDisposition::FirstCommit)
                .count();
            assert_eq!(first_count, 1, "key {key} must FirstCommit once");
            let canonical = outcomes[0].stored_outcome();
            for extra in &outcomes[1..] {
                assert_eq!(extra.stored_outcome(), canonical);
            }
            sequences.push(canonical.commit_sequence().get());
        }
        sequences.sort_unstable();
        assert_eq!(
            sequences,
            vec![2, 3, 4, 5, 6],
            "5 distinct valid charges after OpenAccount(1) must occupy contiguous CommitSequences 2..=6"
        );

        drop(inspector);
        drop(executor);
        coordinator.shutdown().expect("clean coordinator shutdown");

        // Reopen database from disk and verify durability + post-reopen replay equivalence.
        let reopened_ports = db.open();
        assert_eq!(db.read_posted_total(&reopened_ports), Some(100));
        let reopened_coord = start_coordinator(reopened_ports, durability, 16);
        let reopened_exec = reopened_coord.command_executor();
        let reopened_insp =
            reopened_coord.command_idempotency_inspector(Arc::new(DeterministicKeyDigests));
        for idx in 0_u8..5 {
            let key = format!("wave-valid-{idx}");
            let replay_prep = rt.block_on(db.prepare_post_charge(
                &reopened_insp,
                &key,
                charge_uuid(30 + idx),
                20,
            ));
            let replay_res =
                submit_sync(&rt, &reopened_exec, replay_prep).expect("post-reopen replay");
            let CommandExecutionResult::Committed(replayed) = replay_res else {
                panic!("expected post-reopen replay");
            };
            assert_eq!(replayed.disposition(), CommittedOutcomeDisposition::Replay);
            assert_eq!(replayed.stored_outcome(), by_key[&key][0].stored_outcome());
        }
        drop(reopened_insp);
        drop(reopened_exec);
        reopened_coord
            .shutdown()
            .expect("clean reopened coordinator shutdown");
    }
}

// req: TEST-001, TEST-003, TXN-001, TXN-002, CMP-001
#[test]
fn deterministic_multi_epoch_action_sequences_preserve_oracle_equivalence() {
    #[derive(Clone, Debug)]
    struct Oracle {
        limit: i64,
        posted_total: i64,
        next_sequence: u64,
        charges: BTreeMap<[u8; 16], i64>,
        completed_keys: BTreeMap<String, (([u8; 16], i64), StoredOutcomeV1)>,
    }

    let charge_posted_outcome = {
        let probe = CreditLedgerDatabase::create("outcome-id-probe");
        (
            probe.outcome_id("PostCharge", "ChargePosted"),
            probe.outcome_id("PostCharge", "ChargeExists"),
        )
    };

    // Explore 12 deterministic schedules of 8 steps each over Group durability, mixing:
    // 0 = fresh valid charge (+15..+30)
    // 1 = duplicate of an already-committed key (post-flush replay)
    // 2 = two back-to-back in-flight duplicates of a new key
    // 3 = key-reuse mismatch against an already-committed key
    // 4 = declared-outcome duplicate charge_id under a fresh idempotency key (`ChargeExists`, zero-mutation commit!)
    // 5 = post-mutation invariant rejection (`posted_total + 500 > limit`)
    let rt = runtime();
    for schedule_seed in 0_u64..12 {
        let db = CreditLedgerDatabase::create(&format!("oracle-seq-{schedule_seed}"));
        let ports = db.open();
        let coordinator = start_coordinator(ports, CoordinatorDurability::Group, 32);
        let executor = coordinator.command_executor();
        let inspector =
            coordinator.command_idempotency_inspector(Arc::new(DeterministicKeyDigests));

        let limit = 200_i64;
        let open_prep = rt.block_on(db.prepare_open_account(&inspector, "open-oracle", limit));
        submit_sync(&rt, &executor, open_prep).expect("OpenAccount commits");

        let mut oracle = Oracle {
            limit,
            posted_total: 0,
            next_sequence: 2,
            charges: BTreeMap::new(),
            completed_keys: BTreeMap::new(),
        };
        let mut state = schedule_seed
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(1);

        for step in 0_u8..8 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let kind = (state >> 32) % 6;
            match kind {
                0 => {
                    let key = format!("s{schedule_seed}-step{step}-fresh");
                    let cid = charge_uuid(100 + step);
                    let amount = 15_i64 + ((state >> 40) % 16) as i64;
                    let prep = rt.block_on(db.prepare_post_charge(&inspector, &key, cid, amount));
                    if oracle.posted_total + amount <= oracle.limit {
                        let CommandExecutionResult::Committed(committed) =
                            submit_sync(&rt, &executor, prep).expect("fresh charge commits")
                        else {
                            panic!("expected Committed");
                        };
                        assert_eq!(
                            committed.disposition(),
                            CommittedOutcomeDisposition::FirstCommit
                        );
                        assert_eq!(
                            committed.stored_outcome().commit_sequence().get(),
                            oracle.next_sequence
                        );
                        assert_eq!(
                            committed.stored_outcome().declared_outcome().outcome_id(),
                            charge_posted_outcome.0
                        );
                        oracle.next_sequence += 1;
                        oracle.posted_total += amount;
                        assert_eq!(
                            db.posted_total_from_outcome(committed.stored_outcome()),
                            oracle.posted_total
                        );
                        oracle.charges.insert(cid, amount);
                        oracle
                            .completed_keys
                            .insert(key, ((cid, amount), committed.into_stored_outcome()));
                    } else {
                        let err = submit_sync(&rt, &executor, prep).expect_err("must reject");
                        assert_eq!(err.kind(), CommandExecutionErrorKind::CommitCheckRejected);
                    }
                }
                1 if !oracle.completed_keys.is_empty() => {
                    let (key, ((cid, amount), expected)) =
                        oracle.completed_keys.iter().next().expect("existing key");
                    let prep = rt.block_on(db.prepare_post_charge(&inspector, key, *cid, *amount));
                    let CommandExecutionResult::Committed(replayed) =
                        submit_sync(&rt, &executor, prep).expect("replay succeeds")
                    else {
                        panic!("expected Committed replay");
                    };
                    assert_eq!(replayed.disposition(), CommittedOutcomeDisposition::Replay);
                    assert_eq!(replayed.stored_outcome(), expected);
                }
                2 => {
                    let key = format!("s{schedule_seed}-step{step}-pair");
                    let cid = charge_uuid(130 + step);
                    let amount = 10_i64;
                    if oracle.posted_total + amount <= oracle.limit {
                        let (p1, p2) = rt.block_on(async {
                            (
                                db.prepare_post_charge(&inspector, &key, cid, amount).await,
                                db.prepare_post_charge(&inspector, &key, cid, amount).await,
                            )
                        });
                        let (r1, r2) = rt.block_on(async {
                            let sub1 = executor
                                .reserve_capacity()
                                .await
                                .expect("cap1")
                                .submit(p1)
                                .expect("sub1");
                            let sub2 = executor
                                .reserve_capacity()
                                .await
                                .expect("cap2")
                                .submit(p2)
                                .expect("sub2");
                            (sub1.completion().await, sub2.completion().await)
                        });
                        let CommandExecutionResult::Committed(c1) = r1.expect("r1 ok") else {
                            panic!("expected c1 Committed");
                        };
                        let CommandExecutionResult::Committed(c2) = r2.expect("r2 ok") else {
                            panic!("expected c2 Committed");
                        };
                        assert_eq!(c1.stored_outcome(), c2.stored_outcome());
                        assert_eq!(
                            usize::from(
                                c1.disposition() == CommittedOutcomeDisposition::FirstCommit
                            ) + usize::from(
                                c2.disposition() == CommittedOutcomeDisposition::FirstCommit
                            ),
                            1
                        );
                        assert_eq!(
                            c1.stored_outcome().commit_sequence().get(),
                            oracle.next_sequence
                        );
                        oracle.next_sequence += 1;
                        oracle.posted_total += amount;
                        assert_eq!(
                            db.posted_total_from_outcome(c1.stored_outcome()),
                            oracle.posted_total
                        );
                        oracle.charges.insert(cid, amount);
                        oracle
                            .completed_keys
                            .insert(key, ((cid, amount), c1.into_stored_outcome()));
                    }
                }
                3 if !oracle.completed_keys.is_empty() => {
                    let (key, ((cid, amount), _)) =
                        oracle.completed_keys.iter().next().expect("existing key");
                    let prep =
                        rt.block_on(db.prepare_post_charge(&inspector, key, *cid, amount + 1));
                    let res = submit_sync(&rt, &executor, prep).expect("mismatch completes");
                    assert!(matches!(res, CommandExecutionResult::InputMismatch));
                }
                4 if !oracle.charges.is_empty() => {
                    let (&existing_cid, _) = oracle.charges.iter().next().expect("existing charge");
                    let key = format!("s{schedule_seed}-step{step}-dup-cid");
                    let prep =
                        rt.block_on(db.prepare_post_charge(&inspector, &key, existing_cid, 999));
                    let CommandExecutionResult::Committed(committed) =
                        submit_sync(&rt, &executor, prep).expect("declared outcome commits")
                    else {
                        panic!("expected Committed ChargeExists outcome");
                    };
                    assert_eq!(
                        committed.stored_outcome().declared_outcome().outcome_id(),
                        charge_posted_outcome.1
                    );
                    assert_eq!(
                        committed.stored_outcome().commit_sequence().get(),
                        oracle.next_sequence
                    );
                    oracle.next_sequence += 1;
                    oracle
                        .completed_keys
                        .insert(key, ((existing_cid, 999), committed.into_stored_outcome()));
                }
                _ => {
                    let key = format!("s{schedule_seed}-step{step}-inv-fail");
                    let cid = charge_uuid(160 + step);
                    let prep = rt.block_on(db.prepare_post_charge(&inspector, &key, cid, 500));
                    let err = submit_sync(&rt, &executor, prep).expect_err("must reject");
                    assert_eq!(err.kind(), CommandExecutionErrorKind::CommitCheckRejected);
                    assert_eq!(
                        executor.lifecycle_state(),
                        CoordinatorLifecycleState::Accepting
                    );
                }
            }
        }

        drop(inspector);
        drop(executor);
        coordinator.shutdown().expect("clean coordinator shutdown");

        let reopened = db.open();
        assert_eq!(db.read_posted_total(&reopened), Some(oracle.posted_total));
        for (&cid, &expected_amount) in &oracle.charges {
            assert_eq!(db.read_charge_amount(&reopened, cid), Some(expected_amount));
        }
    }
}

fn submit_sync(
    rt: &tokio::runtime::Runtime,
    executor: &riffdb_commit::CommandExecutor,
    preparation: CommandExecutionPreparation,
) -> Result<CommandExecutionResult, riffdb_commit::CommandExecutionError> {
    rt.block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve capacity")
            .submit(preparation)
            .expect("submit command")
            .completion()
            .await
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("build tokio runtime")
}

fn start_coordinator(
    ports: RedbOperationalPorts,
    durability: CoordinatorDurability,
    capacity: u16,
) -> RunningCommandCoordinator {
    let conflicts: Arc<dyn ConflictManager> = Arc::new(
        ShardedConflictManager::new(ConflictManagerConfig::default())
            .expect("start conflict manager"),
    );
    RunningCommandCoordinator::start(
        CoordinatorWorkloadCapacity::new(capacity).expect("nonzero capacity"),
        durability,
        ports,
        conflicts,
        Arc::new(FixedClock(timestamp(1_700_000_010))),
        Arc::new(SequentialUuidV7Source::new(0x200)),
        Arc::new(FixedClock(timestamp(1_700_000_010))),
        Arc::new(FixedClock(timestamp(1_700_000_010))),
        Arc::new(SequentialUuidV7Source::new(0x800)),
        Arc::new(NoopCommitNotifications),
    )
    .expect("start coordinator")
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
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
    let db_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("evidence page limit");
    let mut cursor = StructuralEvidenceCursor::start(db_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty(), "unexpected findings: {findings:?}");
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
    let riffdb_catalog::CatalogHistoryOutcome::Ready(history) = history else {
        panic!("unexpected migration requirement");
    };
    let riffdb_storage_api::StructuralOpenOutcome::Clean(opened) = opened else {
        panic!("unexpected migration port");
    };
    assert!(history.matches(opened.database_id(), opened.open_session_id()));
    let (_, _, _, dormant): (_, _, _, RedbDormantPorts) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports")
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
    .expect("canonical input record")
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
        NonZeroU16::new(10).expect("row limit"),
        Vec::new(),
    )
    .expect("capability grant");
    let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database_id(),
        environment(),
        ActorId::new(PRINCIPAL).expect("principal"),
        ActorKind::Agent,
        Audience::new("riffdb-deferred-pipeline").expect("audience"),
        AuthorizationFixtureTimes::new(timestamp(100), timestamp(300), timestamp(150)),
        grant,
    ))
    .expect("authorization fixture");
    let resolver = fixture.current_capability_resolver();
    let clock = FixedClock(timestamp(200));
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
    .expect("authorize command");
    let Decision::Allow(proof) = decision else {
        panic!("expected Allow");
    };
    proof
        .into_command_execution(
            UntrustedInvocationClaims::new(None, None, None, None, None),
            AgentSessionAdmissionPolicy::Discard,
        )
        .expect("authorized command execution")
}

struct DeterministicKeyDigests;

impl IdempotencyDigestProvider for DeterministicKeyDigests {
    fn digest_candidates(
        &self,
        key: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
        // Deterministic 256-bit mixing (4 distinct seeded FNV-1a lanes) so distinct caller keys
        // never collide across multi-epoch schedules without OS randomness.
        let mut bytes = [0_u8; 32];
        for lane in 0_u64..4 {
            let mut acc = 0xcbf2_9ce4_8422_2325_u64 ^ lane.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            for &b in key.expose_secret().as_bytes() {
                acc ^= u64::from(b);
                acc = acc.wrapping_mul(0x0000_0100_0000_01b3);
            }
            let start = (lane as usize) * 8;
            bytes[start..start + 8].copy_from_slice(&acc.to_le_bytes());
        }
        IdempotencyDigestCandidatesV1::new(vec![IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key ID"),
            bytes,
        )])
    }
}

struct FixedClock(Timestamp);

impl AdmissionClock for FixedClock {
    fn now(&self) -> Result<Timestamp, AdmissionClockError> {
        Ok(self.0)
    }
}

impl AdministrationClock for FixedClock {
    fn now(&self) -> Result<Timestamp, AdministrationClockError> {
        Ok(self.0)
    }
}

impl AuthorizationClock for FixedClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(self.0)
    }
}

struct SequentialUuidV7Source {
    counter: AtomicU64,
}

impl SequentialUuidV7Source {
    const fn new(start: u64) -> Self {
        Self {
            counter: AtomicU64::new(start),
        }
    }

    fn next_bytes(&self) -> [u8; 16] {
        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        uuid_v7_from_counter(seq)
    }
}

impl ServiceUuidV7Source for SequentialUuidV7Source {
    fn next_uuid_v7(&self) -> Result<[u8; 16], ServiceUuidV7SourceError> {
        Ok(self.next_bytes())
    }
}

impl ProvenanceIdSource for SequentialUuidV7Source {
    fn next_provenance_id(&self) -> Result<ProvenanceId, ProvenanceIdSourceError> {
        ProvenanceId::from_bytes(self.next_bytes()).map_err(|_| ProvenanceIdSourceError)
    }
}

struct NoopCommitNotifications;

impl ApplicationCommitNotificationSink for NoopCommitNotifications {
    fn publish_first_commit(
        &self,
        _: CommitSequence,
    ) -> Result<(), ApplicationCommitNotificationError> {
        Ok(())
    }
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
            principal_id: ActorId::new(PRINCIPAL).expect("principal"),
            capability_id: CapabilityId::from_bytes(uuid_v7_from_counter(0x9000))
                .expect("capability UUIDv7"),
            capability_revision: NonZeroU64::MIN,
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

impl PostEvaluationCommandAuthorizer for FixedPostEvaluationCommandAuthorizer {
    fn authorize(&self) -> Result<AuthorizedCommandExecution, PostEvaluationAuthorizationError> {
        Ok(authorize_command(
            &self.plan,
            self.lineage.clone(),
            self.partition.clone(),
        ))
    }
}

fn catalog_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("deferred-pipeline-maintainer").expect("catalog principal"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_v7_from_counter(1)).expect("catalog capability UUIDv7"),
        NonZeroU64::MIN,
    )
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x73; 10])
        .expect("database UUIDv7")
}

fn environment() -> Environment {
    Environment::new(ENVIRONMENT).expect("environment")
}

fn request_id_from_counter(counter: u64) -> RequestId {
    RequestId::from_bytes(uuid_v7_from_counter(counter)).expect("request UUIDv7")
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("timestamp")
}

fn charge_uuid(tag: u8) -> [u8; 16] {
    uuid_v7_from_counter(0x40_000 + u64::from(tag))
}

fn uuid_v7_from_counter(counter: u64) -> [u8; 16] {
    let millis = 1_700_000_000_000_u64.wrapping_add(counter);
    let mut bytes = [0_u8; 16];
    bytes[0..6].copy_from_slice(&millis.to_be_bytes()[2..8]);
    bytes[6] = 0x70 | ((counter >> 8) as u8 & 0x0f);
    bytes[7] = (counter & 0xff) as u8;
    bytes[8] = 0x80 | ((counter >> 16) as u8 & 0x3f);
    bytes[9..16].copy_from_slice(&counter.to_be_bytes()[1..8]);
    bytes
}
