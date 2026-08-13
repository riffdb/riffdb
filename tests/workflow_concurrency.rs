#![forbid(unsafe_code)]

//! Deterministic schedules for compiler-enforced workflow lease concurrency.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{CommandPlan, ContractBundle, RecordSchema, ServiceValueKind};
use riffdb_invariant::{ExpressionValueSource, evaluate_expression};
use riffdb_runtime::{ExecutionResult, TransactionContext, execute_command};
use riffdb_storage_api::{
    DurableKeySchemaBindingV1, EntityObservation, EntityTarget, EvaluationBudget,
    ExecutablePlanRef, ReadSnapshot, SnapshotRequest, StoredEntityRecordV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, CanonicalRecord, CanonicalValue, EntityVersion,
    FieldId, LogicalTime, OutcomeId, RequestId, TenantScope, Timestamp,
};

const SOURCE: &str = include_str!("../fixtures/workflows/compiler/valid/workflow_surface.riff");

#[test]
fn competing_claims_cannot_both_survive_transaction_current_revision_validation() {
    let bundle = compile_contract_source(SOURCE).expect("workflow fixture compiles");
    let claim = command(&bundle, "ClaimWork");
    let first_input = claim_input(claim, "claim-a", [0xa1; 16], 1);
    let second_input = claim_input(claim, "claim-b", [0xb1; 16], 1);
    let target = derive_target(claim, &first_input);
    assert_eq!(target, derive_target(claim, &second_input));
    let initial = stored_work(
        &bundle,
        claim,
        target,
        EntityVersion::first(),
        LeaseState {
            owner: CanonicalValue::Null,
            expiry: CanonicalValue::Null,
            fence: 0,
            attempts: 0,
        },
    );

    let first = evaluate(
        &bundle,
        claim,
        &first_input,
        initial.clone(),
        Timestamp::new(100, 0).expect("time"),
    );
    let second_from_same_snapshot = evaluate(
        &bundle,
        claim,
        &second_input,
        initial,
        Timestamp::new(100, 0).expect("time"),
    );
    assert_eq!(first.outcome().outcome_id(), outcome_id(claim, "Claimed"));
    assert_eq!(
        second_from_same_snapshot.outcome().outcome_id(),
        outcome_id(claim, "Claimed")
    );

    let successor_version = EntityVersion::first().checked_next().expect("version two");
    let committed = StoredEntityRecordV1::from_checked_post_image(
        first.mutations()[0].post_image(),
        successor_version,
        DurableKeySchemaBindingV1::from_plan(&plan_ref(&bundle, claim)),
    )
    .expect("first claim post-image is storable");
    let retried = evaluate(
        &bundle,
        claim,
        &second_input,
        committed,
        Timestamp::new(100, 0).expect("time"),
    );
    assert_eq!(
        retried.outcome().outcome_id(),
        outcome_id(claim, "ClaimStale")
    );
    assert!(retried.mutations().is_empty());
}

#[test]
fn reclaimed_work_rejects_the_previous_holder_and_accepts_only_the_new_fence() {
    let bundle = compile_contract_source(SOURCE).expect("workflow fixture compiles");
    let claim = command(&bundle, "ClaimWork");
    let claim_input = claim_input(claim, "reclaim", [0xb1; 16], 1);
    let target = derive_target(claim, &claim_input);
    let expired = stored_work(
        &bundle,
        claim,
        target,
        EntityVersion::first(),
        LeaseState {
            owner: CanonicalValue::Uuid([0xa1; 16]),
            expiry: CanonicalValue::Timestamp(Timestamp::new(99, 0).expect("expired")),
            fence: 7,
            attempts: 3,
        },
    );
    let reclaimed = evaluate(
        &bundle,
        claim,
        &claim_input,
        expired,
        Timestamp::new(100, 0).expect("time"),
    );
    assert_eq!(
        reclaimed.outcome().outcome_id(),
        outcome_id(claim, "Claimed")
    );
    let successor_version = EntityVersion::first().checked_next().expect("version two");
    let committed = StoredEntityRecordV1::from_checked_post_image(
        reclaimed.mutations()[0].post_image(),
        successor_version,
        DurableKeySchemaBindingV1::from_plan(&plan_ref(&bundle, claim)),
    )
    .expect("reclaim post-image is storable");

    let start = command(&bundle, "StartWork");
    let stale_input = start_input(start, "stale-worker", [0xa1; 16], 7, 2);
    let rejected = evaluate(
        &bundle,
        start,
        &stale_input,
        rebind_record(&bundle, start, committed.clone()),
        Timestamp::new(101, 0).expect("time"),
    );
    assert_eq!(
        rejected.outcome().outcome_id(),
        outcome_id(start, "FenceInvalid")
    );
    assert!(rejected.mutations().is_empty());

    let current_input = start_input(start, "current-worker", [0xb1; 16], 8, 2);
    let accepted = evaluate(
        &bundle,
        start,
        &current_input,
        rebind_record(&bundle, start, committed),
        Timestamp::new(101, 0).expect("time"),
    );
    assert_eq!(
        accepted.outcome().outcome_id(),
        outcome_id(start, "Started")
    );
    assert_eq!(accepted.mutations().len(), 1);
}

#[test]
fn first_claim_after_reimport_mints_strictly_above_the_preserved_fence() {
    let bundle = compile_contract_source(SOURCE).expect("workflow fixture compiles");
    let claim = command(&bundle, "ClaimWork");
    let claim_input = claim_input(claim, "post-reimport-claim", [0xc1; 16], 1);
    let target = derive_target(claim, &claim_input);
    let preserved_fence = 11;
    let reimported = stored_work(
        &bundle,
        claim,
        target,
        EntityVersion::first(),
        LeaseState {
            owner: CanonicalValue::Null,
            expiry: CanonicalValue::Null,
            fence: preserved_fence,
            attempts: 4,
        },
    );

    let claimed = evaluate(
        &bundle,
        claim,
        &claim_input,
        reimported,
        Timestamp::new(100, 0).expect("time"),
    );
    assert_eq!(claimed.outcome().outcome_id(), outcome_id(claim, "Claimed"));
    let post_image = claimed.mutations()[0].post_image();
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "WorkItem")
        .expect("WorkItem");
    let value = |name: &str| {
        let field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .expect("workflow field");
        post_image
            .fields()
            .fields()
            .iter()
            .find_map(|(id, value)| (*id == field.id()).then_some(value))
            .expect("post-image field")
    };
    assert_eq!(
        value("lease_fence"),
        &CanonicalValue::U64(preserved_fence + 1)
    );
    assert_eq!(value("lease_attempts"), &CanonicalValue::U64(5));
}

fn evaluate(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    input: &CanonicalRecord,
    stored: StoredEntityRecordV1,
    time: Timestamp,
) -> riffdb_storage_api::EvaluatedCommand {
    let snapshot = snapshot(plan_ref(bundle, plan), stored);
    let context = context(bundle, plan, input, time);
    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(bundle, input, &snapshot, &context, EvaluationBudget::v1())
            .expect("workflow command evaluates")
    else {
        panic!("workflow mutation and declared rejection both require commit");
    };
    evaluated
}

fn command<'a>(bundle: &'a ContractBundle, name: &str) -> &'a CommandPlan {
    bundle
        .commands()
        .iter()
        .find(|command| command.name() == name)
        .expect("command")
}

fn claim_input(
    plan: &CommandPlan,
    request: &str,
    owner: [u8; 16],
    revision: u64,
) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "request_key",
                CanonicalValue::string(request).expect("request"),
            ),
            ("organization_id", CanonicalValue::Uuid([0x11; 16])),
            ("work_id", CanonicalValue::Uuid([0x22; 16])),
            ("owner_id", CanonicalValue::Uuid(owner)),
            ("duration", CanonicalValue::U64(30)),
            ("expected_revision", CanonicalValue::U64(revision)),
        ],
    )
}

fn start_input(
    plan: &CommandPlan,
    request: &str,
    owner: [u8; 16],
    fence: u64,
    revision: u64,
) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "request_key",
                CanonicalValue::string(request).expect("request"),
            ),
            ("organization_id", CanonicalValue::Uuid([0x11; 16])),
            ("work_id", CanonicalValue::Uuid([0x22; 16])),
            ("owner_id", CanonicalValue::Uuid(owner)),
            ("fencing_token", CanonicalValue::U64(fence)),
            ("expected_revision", CanonicalValue::U64(revision)),
        ],
    )
}

fn input_record<const N: usize>(
    schema: &RecordSchema,
    values: [(&str, CanonicalValue); N],
) -> CanonicalRecord {
    let mut values = values.into_iter().collect::<BTreeMap<_, _>>();
    CanonicalRecord::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                (
                    field.id(),
                    values.remove(field.name()).expect("input field value"),
                )
            })
            .collect(),
    )
    .expect("canonical input")
}

struct LeaseState {
    owner: CanonicalValue,
    expiry: CanonicalValue,
    fence: u64,
    attempts: u64,
}

fn stored_work(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    target: EntityTarget,
    version: EntityVersion,
    lease: LeaseState,
) -> StoredEntityRecordV1 {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "WorkItem")
        .expect("WorkItem");
    let keys = entity
        .primary_key()
        .decode_entity(target.key())
        .expect("key");
    let state = bundle
        .schema()
        .enums()
        .iter()
        .find(|enumeration| enumeration.name() == "WorkState")
        .expect("state enum");
    let queued = state
        .variants()
        .iter()
        .find(|variant| variant.name() == "Queued")
        .expect("Queued");
    let fields = input_record(
        entity.record(),
        [
            ("organization_id", keys[0].clone()),
            ("work_id", keys[1].clone()),
            (
                "state",
                CanonicalValue::Enum {
                    type_id: state.id(),
                    variant_id: queued.id(),
                },
            ),
            ("lease_owner", lease.owner),
            ("lease_expires_at", lease.expiry),
            ("lease_fence", CanonicalValue::U64(lease.fence)),
            ("lease_attempts", CanonicalValue::U64(lease.attempts)),
        ],
    );
    StoredEntityRecordV1::new(
        target,
        version,
        bundle.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan_ref(bundle, plan)),
        fields,
    )
    .expect("stored work")
}

fn rebind_record(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    record: StoredEntityRecordV1,
) -> StoredEntityRecordV1 {
    StoredEntityRecordV1::new(
        record.target().clone(),
        record.entity_version(),
        bundle.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan_ref(bundle, plan)),
        record.fields().clone(),
    )
    .expect("record rebound to exact command plan")
}

fn derive_target(plan: &CommandPlan, input: &CanonicalRecord) -> EntityTarget {
    let binding = &plan.bindings()[0];
    let values = Inputs { input };
    let components = binding
        .key_expressions()
        .iter()
        .map(|expression| {
            evaluate_expression(plan.expressions(), *expression, &values).expect("key expression")
        })
        .collect::<Vec<_>>();
    let key = binding
        .key_schema()
        .encode_entity(&components)
        .expect("entity key");
    EntityTarget::new(binding.entity_type(), key).expect("target")
}

fn snapshot(plan: ExecutablePlanRef, stored: StoredEntityRecordV1) -> ReadSnapshot {
    let target = stored.target().clone();
    let request = SnapshotRequest::new(plan, vec![target], vec![], vec![]).expect("request");
    ReadSnapshot::new(
        &request,
        None,
        vec![EntityObservation::Present(stored)],
        vec![],
        vec![],
    )
    .expect("snapshot")
}

fn context(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    input: &CanonicalRecord,
    time: Timestamp,
) -> TransactionContext {
    let logical_time = LogicalTime::new(time);
    let value = evaluate_expression(
        plan.expressions(),
        plan.locality().partition_expression(),
        &Inputs { input },
    )
    .expect("partition");
    let partition = plan
        .locality()
        .partition_schema()
        .encode_partition(&[value])
        .expect("partition key");
    let service_values = CanonicalRecord::new(
        plan.service_values()
            .iter()
            .map(|value| {
                let observed = match value.kind() {
                    ServiceValueKind::TransactionTime => CanonicalValue::Timestamp(time),
                    ServiceValueKind::UuidV7 => CanonicalValue::Uuid([0x77; 16]),
                };
                (value.field().id(), observed)
            })
            .collect(),
    )
    .expect("service values");
    TransactionContext::new_with_service_values(
        RequestId::from_unix_milliseconds_and_random(1, [0x55; 10]).expect("request ID"),
        AdmittedActorContext::new(
            ActorId::new("workflow-test").expect("actor"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        ),
        plan_ref(bundle, plan),
        logical_time,
        partition,
        service_values,
    )
}

fn plan_ref(bundle: &ContractBundle, plan: &CommandPlan) -> ExecutablePlanRef {
    ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        plan.command_id(),
        plan.plan_hash(),
    )
}

fn outcome_id(plan: &CommandPlan, name: &str) -> OutcomeId {
    plan.outcomes()
        .iter()
        .find(|outcome| outcome.name() == name)
        .expect("outcome")
        .id()
}

struct Inputs<'a> {
    input: &'a CanonicalRecord,
}

impl ExpressionValueSource for Inputs<'_> {
    fn input_field(&self, field: FieldId) -> Option<CanonicalValue> {
        self.input
            .fields()
            .iter()
            .find(|(candidate, _)| *candidate == field)
            .map(|(_, value)| value.clone())
    }
}
