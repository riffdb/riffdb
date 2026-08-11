//! Snapshot-based deterministic runtime conformance tests.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{CommandPlan, ContractBundle, RecordSchema};
use riffdb_invariant::{ExpressionValueSource, derive_input_command_facts, evaluate_expression};
use riffdb_runtime::{ExecutionFault, ExecutionResult, TransactionContext, execute_command};
use riffdb_storage_api::{
    DurableKeySchemaBindingV1, EntityObservation, EntityTarget, EvaluationBudget,
    ExecutablePlanRef, ReadDependency, ReadSnapshot, SnapshotRequest, StoredEntityRecordV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, CanonicalBytes, CanonicalList, CanonicalRecord,
    CanonicalValue, Decimal, DecimalSpec, EntityVersion, FieldId, LogicalTime, OutcomeId,
    RequestId, TenantScope, Timestamp,
};

const BUDGET_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../contracts/examples/budget.riff"
));
const BULK_TUPLE_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/contracts/bulk/openfga-tuples.riff"
));
const BULK_DELETE_SOURCE: &str = r#"
contract BulkDeleteRuntime version 1 {
  entity Row {
    key (tenant_id: uuid, row_id: uuid)
    delete_policy no_inbound
  }
  aggregate Rows { root Row partition_by tenant_id conflict_key (tenant_id, row_id) }
  bulk command DeleteRows {
    input request_id: uuid
    input tenant_id: uuid
    input row_ids: list<uuid, 1..8>
    idempotency_key request_id
    for row_id in row_ids {
      delete Row(tenant_id, row_id) as row else Missing {}
    }
    return Deleted {}
  }
}
"#;

#[test]
fn bounded_collection_delete_retains_exact_predecessors_without_post_delete_rows() {
    let bundle = compile_contract_source(BULK_DELETE_SOURCE).expect("delete fixture compiles");
    let plan = command(&bundle, "DeleteRows");
    let tenant = [0x81; 16];
    let input = input_record(
        plan.input().record(),
        [
            ("request_id", CanonicalValue::Uuid([0x82; 16])),
            ("tenant_id", CanonicalValue::Uuid(tenant)),
            (
                "row_ids",
                CanonicalValue::List(
                    CanonicalList::new(vec![
                        CanonicalValue::Uuid([0x83; 16]),
                        CanonicalValue::Uuid([0x84; 16]),
                    ])
                    .expect("row IDs"),
                ),
            ),
        ],
    );
    let facts = derive_input_command_facts(plan, input.clone()).expect("delete facts");
    let row = bundle.schema().entities().first().expect("row entity");
    let records = facts
        .binding_plan_indices()
        .iter()
        .zip(facts.binding_entity_keys())
        .map(|(index, key)| {
            let target =
                EntityTarget::new(plan.bindings()[*index as usize].entity_type(), key.clone())
                    .expect("target");
            let key_values = row.primary_key().decode_entity(key).expect("row key");
            stored_record(
                &bundle,
                plan,
                target,
                input_record(
                    row.record(),
                    [
                        ("tenant_id", key_values[0].clone()),
                        ("row_id", key_values[1].clone()),
                    ],
                ),
            )
        })
        .collect::<Vec<_>>();
    let targets = records
        .iter()
        .map(|record| record.target().clone())
        .collect::<Vec<_>>();
    let request = SnapshotRequest::new(plan_ref(&bundle, plan), targets, vec![], vec![])
        .expect("snapshot request");
    let snapshot = ReadSnapshot::new(
        &request,
        None,
        records
            .iter()
            .cloned()
            .map(EntityObservation::Present)
            .collect(),
        vec![],
        vec![],
    )
    .expect("snapshot");
    let context = TransactionContext::new(
        RequestId::from_unix_milliseconds_and_random(1, [0x12; 10]).expect("request ID"),
        AdmittedActorContext::new(
            ActorId::new("runtime-test").expect("actor"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        ),
        plan_ref(&bundle, plan),
        LogicalTime::new(Timestamp::new(1, 0).expect("time")),
        facts.partition_key().clone(),
    );

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("delete evaluates")
    else {
        panic!("delete mutates");
    };
    assert_eq!(evaluated.mutations().len(), 2);
    for (mutation, predecessor) in evaluated.mutations().iter().zip(records) {
        assert!(mutation.is_delete());
        assert_eq!(
            mutation.expected_version(),
            Some(predecessor.entity_version())
        );
        assert_eq!(mutation.post_image().fields(), predecessor.fields());
    }
}

#[test]
fn bounded_collection_create_executes_as_one_complete_evaluated_graph() {
    let bundle = compile_contract_source(BULK_TUPLE_SOURCE).expect("bulk fixture compiles");
    let plan = command(&bundle, "WriteTuples");
    let tuple = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Tuple")
        .expect("tuple entity");
    let store = [0x31; 16];
    let tuple_value = |id: u8, object: &str| {
        CanonicalValue::Record(
            CanonicalRecord::new(
                tuple
                    .record()
                    .fields()
                    .iter()
                    .map(|field| {
                        let value = match field.name() {
                            "store_id" => CanonicalValue::Uuid(store),
                            "tuple_id" => CanonicalValue::Uuid([id; 16]),
                            "object" => CanonicalValue::string(object).expect("object"),
                            "relation" => CanonicalValue::string("reader").expect("relation"),
                            "subject" => CanonicalValue::string("user:alice").expect("subject"),
                            other => panic!("unexpected tuple field {other}"),
                        };
                        (field.id(), value)
                    })
                    .collect(),
            )
            .expect("tuple record"),
        )
    };
    let input = CanonicalRecord::new(
        plan.input()
            .record()
            .fields()
            .iter()
            .map(|field| {
                let value = match field.name() {
                    "request_id" => CanonicalValue::Uuid([0x21; 16]),
                    "tuples" => CanonicalValue::List(
                        CanonicalList::new(vec![
                            tuple_value(0x41, "document:first"),
                            tuple_value(0x42, "document:second"),
                        ])
                        .expect("tuple list"),
                    ),
                    other => panic!("unexpected input field {other}"),
                };
                (field.id(), value)
            })
            .collect(),
    )
    .expect("bulk input");
    let facts = derive_input_command_facts(plan, input.clone()).expect("collection facts");
    let targets = facts
        .binding_plan_indices()
        .iter()
        .zip(facts.binding_entity_keys())
        .map(|(index, key)| {
            EntityTarget::new(plan.bindings()[*index as usize].entity_type(), key.clone())
                .expect("binding target")
        })
        .collect::<Vec<_>>();
    let request = SnapshotRequest::new(plan_ref(&bundle, plan), targets.clone(), vec![], vec![])
        .expect("snapshot request");
    let snapshot = ReadSnapshot::new(
        &request,
        None,
        targets
            .iter()
            .cloned()
            .map(EntityObservation::Absent)
            .collect(),
        vec![],
        vec![],
    )
    .expect("snapshot");
    let logical_time = LogicalTime::new(Timestamp::new(1, 0).expect("time"));
    let context = TransactionContext::new(
        RequestId::from_unix_milliseconds_and_random(1, [0x12; 10]).expect("request ID"),
        AdmittedActorContext::new(
            ActorId::new("runtime-test").expect("actor"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        ),
        plan_ref(&bundle, plan),
        logical_time,
        facts.partition_key().clone(),
    );

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("collection evaluates")
    else {
        panic!("bulk create mutates");
    };
    assert_eq!(evaluated.mutations().len(), 2);
    assert_eq!(
        evaluated.outcome().outcome_id(),
        outcome_id(plan, "Written")
    );
    let objects = evaluated
        .mutations()
        .iter()
        .map(|mutation| {
            field(
                mutation.post_image().fields(),
                entity_field(&bundle, "Tuple", "object"),
            )
            .clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        objects,
        vec![
            CanonicalValue::string("document:first").expect("object"),
            CanonicalValue::string("document:second").expect("object"),
        ]
    );
}

const CROSS_DOMAIN_SOURCE: &str = r#"
contract CrossDomain version 1 {
  entity Ledger {
    key (organization_id: uuid, domain: i64)
    field value: i64
    invariant non_negative: value >= 0
  }

  aggregate OrganizationLedger {
    root Ledger
    partition_by organization_id
    conflict_key (organization_id, domain)
  }

  command Advance {
    input idempotency_key: string<128>
    input organization_id: uuid
    input write_domain: i64
    input observed_domain: i64
    idempotency_key idempotency_key

    mutate Ledger(organization_id, write_domain) as target
      else TargetMissing { domain: write_domain }
    read Ledger(organization_id, observed_domain) as observed
      else ObservationMissing { domain: observed_domain }

    require observed_non_negative: observed.value >= 0
      else ObservationInvalid { value: observed.value }
    set target.value = target.value + 1
    return Advanced { target: target, observed: observed }
  }
}
"#;

const RELATIONSHIP_SOURCE: &str = r#"
contract Relationships version 1 {
  entity Tenant { key (tenant_id: uuid) }
  entity Parent { key (tenant_id: uuid, parent_id: uuid) }
  entity Child {
    key (tenant_id: uuid, child_id: uuid)
    field parent_id: uuid
    reference parent_ref (tenant_id, parent_id) -> Parent(tenant_id, parent_id)
  }
  aggregate Family {
    root Tenant
    child Parent
    child Child
    partition_by tenant_id
    conflict_key (tenant_id)
  }
  command CreateChild {
    input idempotency_key: string<128>
    input tenant_id: uuid
    input parent_id: uuid
    input child_id: uuid
    idempotency_key idempotency_key
    read Parent(tenant_id, parent_id) as parent
      else ParentMissing { parent_id: parent_id }
    create Child(tenant_id, child_id) as row
      else ChildExists { child_id: child_id }
    set row.parent_id = parent_id
    return Created { record: row }
  }
}
"#;

const READ_ONLY_SOURCE: &str = r#"
contract ReadOnlyRows version 1 {
  entity Row {
    key (id: i64)
    field value: i64
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
  command ReadRow {
    input id: i64
    read Row(id) as row else Missing { id: id }
    return Found { row: row }
  }
}
"#;

const OPTIONAL_STORED_SOURCE: &str = r#"
contract OptionalStored version 1 {
  entity Row {
    key (id: i64)
    field value: i64
    field note: optional<string<8>>
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
  command GetRow {
    input id: i64
    read Row(id) as row else Missing { id: id }
    return Found { row: row }
  }
}
"#;

const RESOURCE_LIMIT_SOURCE: &str = r#"
contract BoundedEffects version 1 {
  entity Counter {
    key (id: i64)
    field value: i64
  }
  event Chunk { payload: bytes<1000000> }
  aggregate Counters {
    root Counter
    partition_by id
    conflict_key (id)
  }
  command EmitMany {
    input idempotency_key: string<128>
    input id: i64
    input payload: bytes<1000000>
    idempotency_key idempotency_key
    mutate Counter(id) as counter else Missing { id: id }
    set counter.value = counter.value + 1
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    emit Chunk { payload: payload }
    return Emitted { value: counter.value }
  }
}
"#;

const EARLY_RECORD_LIMIT_SOURCE: &str = r#"
contract EarlyRecordLimit version 1 {
  entity Counter {
    key (id: i64)
    field value: i64
  }
  event Amplified {
    a_first: bytes<600000>
    b_second: bytes<600000>
    z_marker: i64
  }
  aggregate Counters {
    root Counter
    partition_by id
    conflict_key (id)
  }
  command EmitAmplified {
    input idempotency_key: string<128>
    input id: i64
    input payload: bytes<600000>
    idempotency_key idempotency_key
    mutate Counter(id) as counter else Missing { id: id }
    emit Amplified {
      a_first: payload,
      b_second: payload,
      z_marker: counter.value + 1
    }
    return Emitted {}
  }
}
"#;

const POST_EFFECT_FAULT_SOURCE: &str = r#"
contract PostEffectFault version 1 {
  entity Counter {
    key (id: i64)
    field value: i64
  }
  event CounterChanged { value: i64 }
  aggregate Counters {
    root Counter
    partition_by id
    conflict_key (id)
  }
  command ChangeThenFault {
    input idempotency_key: string<128>
    input id: i64
    input divisor: i64
    idempotency_key idempotency_key
    mutate Counter(id) as counter else Missing { id: id }
    set counter.value = counter.value + 1
    emit CounterChanged { value: counter.value }
    return Changed { quotient: counter.value / divisor }
  }
}
"#;

const ROOT_VALIDATION_SOURCE: &str = r#"
contract RootValidation version 1 {
  entity Root {
    key (tenant: uuid, root_id: uuid)
    field total: i64
  }
  entity Child {
    key (tenant: uuid, root_id: uuid, child_id: uuid)
    field amount: i64
  }
  aggregate Family {
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
    invariant non_negative: total >= 0
  }
  command ChangeChildren {
    input request_key: string<128>
    input tenant: uuid
    input root_id: uuid
    input first_child: uuid
    input second_child: uuid
    input amount: i64
    idempotency_key request_key
    mutate Child(tenant, root_id, first_child) as first else MissingFirst {}
    mutate Child(tenant, root_id, second_child) as second else MissingSecond {}
    set first.amount = amount
    set second.amount = amount
    return Changed { first: first, second: second }
  }
}
"#;

const PREPARED_ARITHMETIC_SOURCE: &str = r#"
contract PreparedArithmetic version 1 {
  entity Row {
    key (id: i64)
    field value: i64
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
  command CreateRow {
    input idempotency_key: string<128>
    input id: i64
    idempotency_key idempotency_key
    create Row(id + 1) as row else AlreadyExists {}
    set row.value = 0
    return Created { row: row }
  }
}
"#;

const WORKFLOW_TRANSITION_SOURCE: &str = r#"
contract WorkflowTransitions version 1 {
  enum WorkState {
    Queued,
    Running,
    Complete,
  }

  entity WorkItem {
    key (organization_id: uuid, work_id: uuid)
    field state: WorkState
  }

  aggregate WorkItems {
    root WorkItem
    partition_by organization_id
    conflict_key (organization_id, work_id)
  }

  workflow WorkLifecycle {
    entity WorkItem
    state state
    transition Start from (Queued) to Running
    transition Finish from (Running) to Complete
  }

  command StartWork {
    input request_key: string<128>
    input organization_id: uuid
    input work_id: uuid
    input expected_revision: u64
    idempotency_key request_key
    mutate WorkItem(organization_id, work_id) as work else Missing {}
    transition Start on work revision expected_revision
      stale StaleRevision {}
      illegal IllegalState {}
    return Started { work: work }
  }
}
"#;

const WORKFLOW_LEASE_SOURCE: &str = r#"
contract WorkflowLeases version 1 {
  enum WorkState { Queued, Running, Complete }
  entity WorkItem {
    key (organization_id: uuid, work_id: uuid)
    field state: WorkState
    field lease_owner: optional<uuid>
    field lease_expires_at: optional<timestamp>
    field lease_fence: u64
    field lease_attempts: u64
  }
  aggregate WorkItems {
    root WorkItem
    partition_by organization_id
    conflict_key (organization_id, work_id)
  }
  workflow WorkLifecycle {
    entity WorkItem
    state state
    transition Finish from (Running) to Complete
    lease execution {
      owner lease_owner
      expires_at lease_expires_at
      fencing_token lease_fence
      attempts lease_attempts
      duration_seconds (5, 900)
    }
  }
  command ClaimWork {
    input request_key: string<128>
    input organization_id: uuid
    input work_id: uuid
    input owner_id: uuid
    input duration: u64
    input expected_revision: u64
    idempotency_key request_key
    mutate WorkItem(organization_id, work_id) as work else ClaimMissing {}
    lease claim execution on work owner owner_id duration_seconds duration revision expected_revision
      stale ClaimStale {} unavailable ClaimUnavailable {} invalid ClaimInvalid {} exhausted ClaimExhausted {}
    return Claimed { work: work }
  }
  command RenewWork {
    input request_key: string<128>
    input organization_id: uuid
    input work_id: uuid
    input owner_id: uuid
    input token: u64
    input duration: u64
    input expected_revision: u64
    idempotency_key request_key
    mutate WorkItem(organization_id, work_id) as work else RenewMissing {}
    lease renew execution on work owner owner_id fencing_token token duration_seconds duration revision expected_revision
      stale RenewStale {} invalid RenewInvalid {} expired RenewExpired {} exhausted RenewExhausted {}
    return Renewed { work: work }
  }
  command ReleaseWork {
    input request_key: string<128>
    input organization_id: uuid
    input work_id: uuid
    input owner_id: uuid
    input token: u64
    input expected_revision: u64
    idempotency_key request_key
    mutate WorkItem(organization_id, work_id) as work else ReleaseMissing {}
    lease release execution on work owner owner_id fencing_token token revision expected_revision
      stale ReleaseStale {} invalid ReleaseInvalid {}
    return Released { work: work }
  }
  command ExpireWork {
    input request_key: string<128>
    input organization_id: uuid
    input work_id: uuid
    input expected_revision: u64
    idempotency_key request_key
    mutate WorkItem(organization_id, work_id) as work else ExpireMissing {}
    lease expire execution on work revision expected_revision stale ExpireStale {} active ExpireActive {}
    return ExpiredWork { work: work }
  }
  command CompleteWork {
    input request_key: string<128>
    input organization_id: uuid
    input work_id: uuid
    input owner_id: uuid
    input token: u64
    input expected_revision: u64
    idempotency_key request_key
    mutate WorkItem(organization_id, work_id) as work else CompleteMissing {}
    lease fence execution on work owner owner_id fencing_token token revision expected_revision
      stale CompleteStale {} invalid CompleteInvalid {} expired CompleteExpired {}
    transition Finish on work revision expected_revision stale TransitionStale {} illegal TransitionIllegal {}
    return Completed { work: work }
  }
}
"#;

const SERVICE_TRANSACTION_TIME_SOURCE: &str = r#"
contract ServiceTransactionTime version 1 {
  entity WorkItem {
    key (organization_id: uuid, work_id: uuid)
    field updated_at: timestamp
    field execution_id: uuid
  }

  aggregate WorkItems {
    root WorkItem
    partition_by organization_id
    conflict_key (organization_id, work_id)
  }

  command TouchWork {
    input request_key: string<128>
    input organization_id: uuid
    input work_id: uuid
    service observed_at: transaction_time
    service execution_id: uuid_v7
    idempotency_key request_key
    mutate WorkItem(organization_id, work_id) as work else Missing {}
    set work.updated_at = observed_at
    set work.execution_id = execution_id
    return Touched { observed_at: observed_at, execution_id: execution_id }
  }
}
"#;

#[test]
fn service_transaction_time_is_the_sealed_admitted_logical_time() {
    let bundle = compile_contract_source(SERVICE_TRANSACTION_TIME_SOURCE)
        .expect("service transaction-time contract compiles");
    let plan = command(&bundle, "TouchWork");
    let input = input_record(
        plan.input().record(),
        [
            (
                "request_key",
                CanonicalValue::string("touch-work-1").expect("string"),
            ),
            ("organization_id", CanonicalValue::Uuid([0xe1; 16])),
            ("work_id", CanonicalValue::Uuid([0xe2; 16])),
        ],
    );
    let target = derive_binding_target(plan, &input, 0);
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "WorkItem")
        .expect("WorkItem entity");
    let key_values = entity
        .primary_key()
        .decode_entity(target.key())
        .expect("work key");
    let stored = stored_record(
        &bundle,
        plan,
        target,
        input_record(
            entity.record(),
            [
                ("organization_id", key_values[0].clone()),
                ("work_id", key_values[1].clone()),
                (
                    "updated_at",
                    CanonicalValue::Timestamp(Timestamp::new(1, 0).expect("old timestamp")),
                ),
                ("execution_id", CanonicalValue::Uuid([0; 16])),
            ],
        ),
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let admitted_time = Timestamp::new(-55, 987_654_321).expect("admitted timestamp");
    let context = context(&bundle, plan, &input, LogicalTime::new(admitted_time));

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("sealed transaction time evaluates")
    else {
        panic!("touch mutates");
    };

    let expected = CanonicalValue::Timestamp(admitted_time);
    let expected_uuid = CanonicalValue::Uuid(
        RequestId::from_unix_milliseconds_and_random(7, [0x77; 10])
            .expect("service UUID")
            .into_bytes(),
    );
    assert_eq!(
        field(
            evaluated.mutations()[0].post_image().fields(),
            entity_field(&bundle, "WorkItem", "updated_at")
        ),
        &expected
    );
    assert_eq!(
        field(
            evaluated.mutations()[0].post_image().fields(),
            entity_field(&bundle, "WorkItem", "execution_id")
        ),
        &expected_uuid
    );
    assert_eq!(
        field(
            evaluated.outcome().value(),
            outcome_field(plan, "Touched", "observed_at")
        ),
        &expected
    );
    assert_eq!(
        field(
            evaluated.outcome().value(),
            outcome_field(plan, "Touched", "execution_id")
        ),
        &expected_uuid
    );
}

#[test]
fn workflow_transition_requires_exact_revision_and_assigns_declared_state() {
    let bundle = compile_contract_source(WORKFLOW_TRANSITION_SOURCE)
        .expect("workflow transition contract compiles");
    let plan = command(&bundle, "StartWork");
    let input = workflow_transition_input(plan, EntityVersion::first().get());
    let target = derive_binding_target(plan, &input, 0);
    let stored = workflow_stored_record(&bundle, plan, target, EntityVersion::first(), "Queued");
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(70, 0).expect("timestamp")),
    );

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("legal exact-revision transition evaluates")
    else {
        panic!("workflow transition mutates");
    };

    assert_eq!(
        evaluated.outcome().outcome_id(),
        outcome_id(plan, "Started")
    );
    assert_eq!(evaluated.mutations().len(), 1);
    assert_eq!(
        evaluated.mutations()[0].expected_version(),
        Some(EntityVersion::first())
    );
    assert_eq!(
        field(
            evaluated.mutations()[0].post_image().fields(),
            entity_field(&bundle, "WorkItem", "state")
        ),
        &enum_value(&bundle, "WorkState", "Running")
    );
    assert!(matches!(
        evaluated.read_dependencies().as_slice(),
        [ReadDependency::EntityObservation {
            expected: riffdb_storage_api::ExpectedEntityState::Present(version),
            ..
        }] if *version == EntityVersion::first()
    ));
}

#[test]
fn workflow_transition_returns_declared_stale_outcome_without_effects() {
    let bundle = compile_contract_source(WORKFLOW_TRANSITION_SOURCE)
        .expect("workflow transition contract compiles");
    let plan = command(&bundle, "StartWork");
    let input = workflow_transition_input(plan, 2);
    let target = derive_binding_target(plan, &input, 0);
    let stored = workflow_stored_record(&bundle, plan, target, EntityVersion::first(), "Queued");
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(71, 0).expect("timestamp")),
    );

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("stale transition is a declared outcome")
    else {
        panic!("mutating rejection requires commit");
    };

    assert_eq!(
        evaluated.outcome().outcome_id(),
        outcome_id(plan, "StaleRevision")
    );
    assert!(evaluated.mutations().is_empty());
    assert!(evaluated.event_intents().is_empty());
}

#[test]
fn workflow_transition_returns_declared_illegal_outcome_without_effects() {
    let bundle = compile_contract_source(WORKFLOW_TRANSITION_SOURCE)
        .expect("workflow transition contract compiles");
    let plan = command(&bundle, "StartWork");
    let input = workflow_transition_input(plan, EntityVersion::first().get());
    let target = derive_binding_target(plan, &input, 0);
    let stored = workflow_stored_record(&bundle, plan, target, EntityVersion::first(), "Running");
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(72, 0).expect("timestamp")),
    );

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("illegal transition is a declared outcome")
    else {
        panic!("mutating rejection requires commit");
    };

    assert_eq!(
        evaluated.outcome().outcome_id(),
        outcome_id(plan, "IllegalState")
    );
    assert!(evaluated.mutations().is_empty());
    assert!(evaluated.event_intents().is_empty());
}

#[test]
fn workflow_lease_claim_is_fenced_by_revision_time_and_monotonic_counters() {
    let bundle =
        compile_contract_source(WORKFLOW_LEASE_SOURCE).expect("workflow lease contract compiles");
    assert_eq!(bundle.format_version(), 3);
    assert_eq!(bundle.ir_version(), 3);
    let bytes = bundle.canonical_bytes().to_vec();
    let decoded = ContractBundle::decode(&bytes).expect("lease bundle decodes");
    assert_eq!(decoded.canonical_bytes(), bytes);
    let plan = command(&bundle, "ClaimWork");
    let input = workflow_lease_claim_input(plan, "claim-1", 5, EntityVersion::first().get());
    let target = derive_binding_target(plan, &input, 0);
    let stored = workflow_lease_stored_record(
        &bundle,
        plan,
        target,
        EntityVersion::first(),
        CanonicalValue::Null,
        CanonicalValue::Null,
        0,
        0,
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(100, 7).expect("timestamp")),
    );

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("claim evaluates")
    else {
        panic!("claim mutates");
    };
    assert_eq!(
        evaluated.outcome().outcome_id(),
        outcome_id(plan, "Claimed")
    );
    let fields = evaluated.mutations()[0].post_image().fields();
    assert_eq!(
        field(fields, entity_field(&bundle, "WorkItem", "lease_owner")),
        &CanonicalValue::Uuid([0xd3; 16])
    );
    assert_eq!(
        field(
            fields,
            entity_field(&bundle, "WorkItem", "lease_expires_at")
        ),
        &CanonicalValue::Timestamp(Timestamp::new(105, 7).expect("timestamp"))
    );
    assert_eq!(
        field(fields, entity_field(&bundle, "WorkItem", "lease_fence")),
        &CanonicalValue::U64(1)
    );
    assert_eq!(
        field(fields, entity_field(&bundle, "WorkItem", "lease_attempts")),
        &CanonicalValue::U64(1)
    );
}

#[test]
fn workflow_lease_stale_revision_returns_declared_outcome_without_mutation() {
    let bundle =
        compile_contract_source(WORKFLOW_LEASE_SOURCE).expect("workflow lease contract compiles");
    let plan = command(&bundle, "ClaimWork");
    let input = workflow_lease_claim_input(plan, "claim-stale", 5, 2);
    let target = derive_binding_target(plan, &input, 0);
    let stored = workflow_lease_stored_record(
        &bundle,
        plan,
        target,
        EntityVersion::first(),
        CanonicalValue::Null,
        CanonicalValue::Null,
        0,
        0,
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(100, 0).expect("timestamp")),
    );
    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("stale claim is declared")
    else {
        panic!("rejection is durable");
    };
    assert_eq!(
        evaluated.outcome().outcome_id(),
        outcome_id(plan, "ClaimStale")
    );
    assert!(evaluated.mutations().is_empty());
}

#[test]
fn workflow_lease_renew_release_expire_and_fence_are_closed_and_exact() {
    let bundle = compile_contract_source(WORKFLOW_LEASE_SOURCE).expect("lease contract compiles");
    let now = LogicalTime::new(Timestamp::new(100, 0).expect("timestamp"));

    let renew = command(&bundle, "RenewWork");
    let renew_input = input_record(
        renew.input().record(),
        [
            (
                "request_key",
                CanonicalValue::string("renew-1").expect("string"),
            ),
            ("organization_id", CanonicalValue::Uuid([0xd1; 16])),
            ("work_id", CanonicalValue::Uuid([0xd2; 16])),
            ("owner_id", CanonicalValue::Uuid([0xd3; 16])),
            ("token", CanonicalValue::U64(7)),
            ("duration", CanonicalValue::U64(10)),
            ("expected_revision", CanonicalValue::U64(1)),
        ],
    );
    let renew_target = derive_binding_target(renew, &renew_input, 0);
    let renew_stored = workflow_lease_stored_record(
        &bundle,
        renew,
        renew_target,
        EntityVersion::first(),
        CanonicalValue::Uuid([0xd3; 16]),
        CanonicalValue::Timestamp(Timestamp::new(150, 0).expect("timestamp")),
        7,
        2,
    );
    let renew_snapshot = snapshot(
        plan_ref(&bundle, renew),
        vec![EntityObservation::Present(renew_stored)],
    );
    let ExecutionResult::CommitRequired(renewed) = execute_command(
        &bundle,
        &renew_input,
        &renew_snapshot,
        &context(&bundle, renew, &renew_input, now),
        EvaluationBudget::v1(),
    )
    .expect("renew") else {
        panic!("renew mutates");
    };
    assert_eq!(renewed.outcome().outcome_id(), outcome_id(renew, "Renewed"));
    assert_eq!(
        field(
            renewed.mutations()[0].post_image().fields(),
            entity_field(&bundle, "WorkItem", "lease_expires_at")
        ),
        &CanonicalValue::Timestamp(Timestamp::new(110, 0).expect("timestamp"))
    );

    let release = command(&bundle, "ReleaseWork");
    let release_input = input_record(
        release.input().record(),
        [
            (
                "request_key",
                CanonicalValue::string("release-1").expect("string"),
            ),
            ("organization_id", CanonicalValue::Uuid([0xd1; 16])),
            ("work_id", CanonicalValue::Uuid([0xd2; 16])),
            ("owner_id", CanonicalValue::Uuid([0xd3; 16])),
            ("token", CanonicalValue::U64(7)),
            ("expected_revision", CanonicalValue::U64(1)),
        ],
    );
    let release_target = derive_binding_target(release, &release_input, 0);
    let release_stored = workflow_lease_stored_record(
        &bundle,
        release,
        release_target,
        EntityVersion::first(),
        CanonicalValue::Uuid([0xd3; 16]),
        CanonicalValue::Timestamp(Timestamp::new(150, 0).expect("timestamp")),
        7,
        2,
    );
    let release_snapshot = snapshot(
        plan_ref(&bundle, release),
        vec![EntityObservation::Present(release_stored)],
    );
    let ExecutionResult::CommitRequired(released) = execute_command(
        &bundle,
        &release_input,
        &release_snapshot,
        &context(&bundle, release, &release_input, now),
        EvaluationBudget::v1(),
    )
    .expect("release") else {
        panic!("release mutates");
    };
    assert_eq!(
        released.outcome().outcome_id(),
        outcome_id(release, "Released")
    );
    assert_eq!(
        field(
            released.mutations()[0].post_image().fields(),
            entity_field(&bundle, "WorkItem", "lease_owner")
        ),
        &CanonicalValue::Null
    );
    assert_eq!(
        field(
            released.mutations()[0].post_image().fields(),
            entity_field(&bundle, "WorkItem", "lease_fence")
        ),
        &CanonicalValue::U64(7)
    );

    let expire = command(&bundle, "ExpireWork");
    let expire_input = input_record(
        expire.input().record(),
        [
            (
                "request_key",
                CanonicalValue::string("expire-1").expect("string"),
            ),
            ("organization_id", CanonicalValue::Uuid([0xd1; 16])),
            ("work_id", CanonicalValue::Uuid([0xd2; 16])),
            ("expected_revision", CanonicalValue::U64(1)),
        ],
    );
    let expire_target = derive_binding_target(expire, &expire_input, 0);
    let expire_stored = workflow_lease_stored_record(
        &bundle,
        expire,
        expire_target,
        EntityVersion::first(),
        CanonicalValue::Uuid([0xd3; 16]),
        CanonicalValue::Timestamp(Timestamp::new(99, 0).expect("timestamp")),
        7,
        2,
    );
    let expire_snapshot = snapshot(
        plan_ref(&bundle, expire),
        vec![EntityObservation::Present(expire_stored)],
    );
    let ExecutionResult::CommitRequired(expired) = execute_command(
        &bundle,
        &expire_input,
        &expire_snapshot,
        &context(&bundle, expire, &expire_input, now),
        EvaluationBudget::v1(),
    )
    .expect("expire") else {
        panic!("expire mutates");
    };
    assert_eq!(
        expired.outcome().outcome_id(),
        outcome_id(expire, "ExpiredWork")
    );
    assert_eq!(
        field(
            expired.mutations()[0].post_image().fields(),
            entity_field(&bundle, "WorkItem", "lease_owner")
        ),
        &CanonicalValue::Null
    );

    let complete = command(&bundle, "CompleteWork");
    let complete_input = input_record(
        complete.input().record(),
        [
            (
                "request_key",
                CanonicalValue::string("complete-stale-holder").expect("string"),
            ),
            ("organization_id", CanonicalValue::Uuid([0xd1; 16])),
            ("work_id", CanonicalValue::Uuid([0xd2; 16])),
            ("owner_id", CanonicalValue::Uuid([0xee; 16])),
            ("token", CanonicalValue::U64(7)),
            ("expected_revision", CanonicalValue::U64(1)),
        ],
    );
    let complete_target = derive_binding_target(complete, &complete_input, 0);
    let complete_stored = workflow_lease_stored_record(
        &bundle,
        complete,
        complete_target,
        EntityVersion::first(),
        CanonicalValue::Uuid([0xd3; 16]),
        CanonicalValue::Timestamp(Timestamp::new(150, 0).expect("timestamp")),
        7,
        2,
    );
    let complete_snapshot = snapshot(
        plan_ref(&bundle, complete),
        vec![EntityObservation::Present(complete_stored)],
    );
    let ExecutionResult::CommitRequired(rejected) = execute_command(
        &bundle,
        &complete_input,
        &complete_snapshot,
        &context(&bundle, complete, &complete_input, now),
        EvaluationBudget::v1(),
    )
    .expect("stale holder is declared") else {
        panic!("rejection durable");
    };
    assert_eq!(
        rejected.outcome().outcome_id(),
        outcome_id(complete, "CompleteInvalid")
    );
    assert!(rejected.mutations().is_empty());
}

#[test]
fn create_executes_with_fixed_time_and_complete_absence_dependency() {
    let bundle = budget_bundle();
    let plan = command(&bundle, "CreateBudget");
    let input = create_input(plan, [0x11; 16], 2027, 10_000);
    let target = derive_binding_target(plan, &input, 0);
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Absent(target)],
    );
    let timestamp = Timestamp::new(-1_234, 987).expect("timestamp");
    let context = context(&bundle, plan, &input, LogicalTime::new(timestamp));

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("create evaluates")
    else {
        panic!("create must require commit");
    };

    assert_eq!(evaluated.mutations().len(), 1);
    assert!(evaluated.event_intents().is_empty());
    assert_eq!(
        evaluated.outcome().outcome_id(),
        outcome_id(plan, "BudgetCreated")
    );
    assert_eq!(evaluated.read_dependencies().as_slice().len(), 1);
    assert!(matches!(
        evaluated.read_dependencies().as_slice()[0],
        ReadDependency::EntityObservation {
            expected: riffdb_storage_api::ExpectedEntityState::Absent,
            ..
        }
    ));
    assert_eq!(
        field(
            evaluated.mutations()[0].post_image().fields(),
            entity_field(&bundle, "Budget", "updated_at")
        ),
        &CanonicalValue::Timestamp(timestamp)
    );
}

#[test]
fn logical_time_extrema_flow_through_runtime_without_conversion() {
    let bundle = budget_bundle();
    let plan = command(&bundle, "CreateBudget");
    let input = create_input(plan, [0x19; 16], 2027, 10_000);
    let target = derive_binding_target(plan, &input, 0);
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Absent(target)],
    );

    for timestamp in [
        Timestamp::new(i64::MIN, 0).expect("minimum timestamp"),
        Timestamp::new(0, 999_999_999).expect("maximum nanoseconds"),
        Timestamp::new(i64::MAX, 999_999_999).expect("maximum timestamp"),
    ] {
        let context = context(&bundle, plan, &input, LogicalTime::new(timestamp));
        let ExecutionResult::CommitRequired(evaluated) =
            execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
                .expect("boundary logical time evaluates")
        else {
            panic!("create must require commit");
        };
        assert_eq!(
            field(
                evaluated.mutations()[0].post_image().fields(),
                entity_field(&bundle, "Budget", "updated_at")
            ),
            &CanonicalValue::Timestamp(timestamp)
        );
    }
}

#[test]
fn duplicate_create_is_a_persistable_zero_mutation_business_outcome() {
    let bundle = budget_bundle();
    let plan = command(&bundle, "CreateBudget");
    let input = create_input(plan, [0x22; 16], 2028, 5_000);
    let target = derive_binding_target(plan, &input, 0);
    let stored = stored_budget(
        &bundle,
        plan,
        target,
        5_000,
        0,
        Timestamp::new(1, 0).expect("timestamp"),
        vec![],
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let logical_time = LogicalTime::new(Timestamp::new(2, 0).expect("timestamp"));
    let context = context(&bundle, plan, &input, logical_time);

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("duplicate is declared")
    else {
        panic!("mutating rejection must require commit");
    };
    assert!(evaluated.mutations().is_empty());
    assert!(evaluated.event_intents().is_empty());
    assert_eq!(
        evaluated.outcome().outcome_id(),
        outcome_id(plan, "BudgetAlreadyExists")
    );
    assert_eq!(evaluated.read_dependencies().as_slice().len(), 1);
}

#[test]
fn mutation_event_and_outcome_are_deterministic_and_instruction_ordered() {
    let bundle = budget_bundle();
    let plan = command(&bundle, "AllocateBudget");
    let input = allocate_input(plan, [0x33; 16], 2029, [0x44; 16], 2_500);
    let target = derive_binding_target(plan, &input, 0);
    let stored = stored_budget(
        &bundle,
        plan,
        target,
        10_000,
        1_500,
        Timestamp::new(10, 0).expect("timestamp"),
        vec![],
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let timestamp = Timestamp::new(20, 123).expect("timestamp");
    let context = context(&bundle, plan, &input, LogicalTime::new(timestamp));

    let first = execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
        .expect("allocation evaluates");
    let second = execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
        .expect("same allocation evaluates");
    assert_eq!(first, second);

    let ExecutionResult::CommitRequired(evaluated) = first else {
        panic!("allocation must require commit");
    };
    assert_eq!(evaluated.mutations().len(), 1);
    assert_eq!(evaluated.event_intents().len(), 1);
    assert_eq!(
        evaluated.outcome().outcome_id(),
        outcome_id(plan, "Allocated")
    );
    let fields = evaluated.mutations()[0].post_image().fields();
    assert_eq!(
        decimal_coefficient(field(
            fields,
            entity_field(&bundle, "Budget", "allocated_amount")
        )),
        4_000
    );
    assert_eq!(
        field(fields, entity_field(&bundle, "Budget", "updated_at")),
        &CanonicalValue::Timestamp(timestamp)
    );
    assert_eq!(
        decimal_coefficient(field(
            evaluated.event_intents()[0].payload(),
            event_field(&bundle, "BudgetAllocated", "amount")
        )),
        2_500
    );
}

#[test]
fn unknown_fields_survive_mutation_but_are_hidden_from_historical_outputs() {
    let bundle = budget_bundle();
    let plan = command(&bundle, "AllocateBudget");
    let input = allocate_input(plan, [0x35; 16], 2029, [0x46; 16], 2_500);
    let target = derive_binding_target(plan, &input, 0);
    let future_field = FieldId::new(65_000).expect("future field ID");
    let future_value = CanonicalValue::string("future-private-value").expect("future value");
    let stored = stored_budget(
        &bundle,
        plan,
        target,
        10_000,
        1_500,
        Timestamp::new(10, 0).expect("timestamp"),
        vec![(future_field, future_value.clone())],
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(20, 0).expect("timestamp")),
    );

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("allocation evaluates")
    else {
        panic!("allocation must require commit");
    };
    assert_eq!(
        field(evaluated.mutations()[0].post_image().fields(), future_field),
        &future_value
    );

    let outcome_schema = plan
        .outcomes()
        .iter()
        .find(|outcome| outcome.name() == "Allocated")
        .expect("Allocated outcome");
    let budget_field = outcome_schema
        .payload()
        .fields()
        .iter()
        .find(|candidate| candidate.name() == "budget")
        .expect("budget outcome field")
        .id();
    let CanonicalValue::Record(visible_budget) = field(evaluated.outcome().value(), budget_field)
    else {
        panic!("budget outcome is a record");
    };
    assert!(
        visible_budget
            .fields()
            .iter()
            .all(|(field_id, _)| *field_id != future_field)
    );
    assert!(
        evaluated
            .event_intents()
            .iter()
            .all(|event| !record_contains_field(event.payload(), future_field))
    );
}

#[test]
fn stored_optional_fields_must_be_normalized_before_runtime_entry() {
    let bundle = compile_contract_source(OPTIONAL_STORED_SOURCE).expect("optional contract");
    let plan = command(&bundle, "GetRow");
    let input = input_record(plan.input().record(), [("id", CanonicalValue::I64(7))]);
    let target = derive_binding_target(plan, &input, 0);
    let id = entity_field(&bundle, "Row", "id");
    let value = entity_field(&bundle, "Row", "value");
    let note = entity_field(&bundle, "Row", "note");
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(20, 0).expect("timestamp")),
    );

    let missing = stored_record(
        &bundle,
        plan,
        target.clone(),
        CanonicalRecord::new(vec![
            (id, CanonicalValue::I64(7)),
            (value, CanonicalValue::I64(11)),
        ])
        .expect("physically incomplete historical record"),
    );
    let missing_snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(missing)],
    );
    assert_eq!(
        execute_command(
            &bundle,
            &input,
            &missing_snapshot,
            &context,
            EvaluationBudget::v1(),
        ),
        Err(ExecutionFault::Integrity)
    );

    let normalized = stored_record(
        &bundle,
        plan,
        target,
        CanonicalRecord::new(vec![
            (id, CanonicalValue::I64(7)),
            (value, CanonicalValue::I64(11)),
            (note, CanonicalValue::Null),
        ])
        .expect("catalog-normalized record"),
    );
    let normalized_snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(normalized)],
    );
    assert!(matches!(
        execute_command(
            &bundle,
            &input,
            &normalized_snapshot,
            &context,
            EvaluationBudget::v1(),
        ),
        Ok(ExecutionResult::ReadOnly(_))
    ));
}

#[test]
fn arithmetic_fault_discards_all_provisional_effects() {
    let bundle = budget_bundle();
    let plan = command(&bundle, "AllocateBudget");
    let input = allocate_input(plan, [0x55; 16], 2030, [0x66; 16], 1);
    let target = derive_binding_target(plan, &input, 0);
    let maximum = 10_i128.pow(28) - 1;
    let stored = stored_budget(
        &bundle,
        plan,
        target,
        maximum,
        maximum,
        Timestamp::new(1, 0).expect("timestamp"),
        vec![],
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(2, 0).expect("timestamp")),
    );
    assert_eq!(
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1()),
        Err(ExecutionFault::Arithmetic)
    );
}

#[test]
fn arithmetic_fault_after_event_and_mutation_returns_no_provisional_effects() {
    let bundle =
        compile_contract_source(POST_EFFECT_FAULT_SOURCE).expect("fault contract compiles");
    let plan = command(&bundle, "ChangeThenFault");
    let input = input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string("post-effect-fault-1").expect("string"),
            ),
            ("id", CanonicalValue::I64(1)),
            ("divisor", CanonicalValue::I64(0)),
        ],
    );
    let target = derive_binding_target(plan, &input, 0);
    let stored = stored_record(
        &bundle,
        plan,
        target,
        input_record(
            bundle
                .schema()
                .entity(plan.bindings()[0].entity_type())
                .expect("counter entity")
                .record(),
            [
                ("id", CanonicalValue::I64(1)),
                ("value", CanonicalValue::I64(41)),
            ],
        ),
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(3, 0).expect("timestamp")),
    );

    assert_eq!(
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1()),
        Err(ExecutionFault::Arithmetic)
    );
}

#[test]
fn aggregate_output_budget_is_charged_before_retention() {
    let bundle =
        compile_contract_source(RESOURCE_LIMIT_SOURCE).expect("resource contract compiles");
    let plan = command(&bundle, "EmitMany");
    let input = input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string("emit-many-1").expect("string"),
            ),
            ("id", CanonicalValue::I64(1)),
            (
                "payload",
                CanonicalValue::Bytes(
                    CanonicalBytes::new(vec![0x5a; 1_000_000]).expect("bounded bytes"),
                ),
            ),
        ],
    );
    let target = derive_binding_target(plan, &input, 0);
    let stored = stored_record(
        &bundle,
        plan,
        target,
        input_record(
            bundle.schema().entities()[0].record(),
            [
                ("id", CanonicalValue::I64(1)),
                ("value", CanonicalValue::I64(0)),
            ],
        ),
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(2, 0).expect("timestamp")),
    );
    assert_eq!(
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1()),
        Err(ExecutionFault::ResourceLimit)
    );
}

#[test]
fn oversized_record_stops_before_evaluating_later_fields() {
    let bundle = compile_contract_source(EARLY_RECORD_LIMIT_SOURCE)
        .expect("early record limit contract compiles");
    let plan = command(&bundle, "EmitAmplified");
    let input = input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string("early-record-limit-1").expect("string"),
            ),
            ("id", CanonicalValue::I64(1)),
            (
                "payload",
                CanonicalValue::Bytes(
                    CanonicalBytes::new(vec![0x5a; 600_000]).expect("bounded bytes"),
                ),
            ),
        ],
    );
    let target = derive_binding_target(plan, &input, 0);
    let stored = stored_record(
        &bundle,
        plan,
        target,
        input_record(
            bundle.schema().entities()[0].record(),
            [
                ("id", CanonicalValue::I64(1)),
                ("value", CanonicalValue::I64(i64::MAX)),
            ],
        ),
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(stored)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(2, 0).expect("timestamp")),
    );

    // The second 600 KiB field crosses the canonical record ceiling. The
    // trailing marker would overflow, so ResourceLimit proves it was not run.
    assert_eq!(
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1()),
        Err(ExecutionFault::ResourceLimit)
    );
}

#[test]
fn read_only_outcome_is_direct_and_unjournaled() {
    let bundle = compile_contract_source(READ_ONLY_SOURCE).expect("read-only contract compiles");
    let plan = command(&bundle, "ReadRow");
    let input = input_record(plan.input().record(), [("id", CanonicalValue::I64(7))]);
    let target = derive_binding_target(plan, &input, 0);
    let record = stored_record(
        &bundle,
        plan,
        target,
        input_record(
            bundle.schema().entities()[0].record(),
            [
                ("id", CanonicalValue::I64(7)),
                ("value", CanonicalValue::I64(9)),
            ],
        ),
    );
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Present(record)],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(42, 0).expect("timestamp")),
    );
    let ExecutionResult::ReadOnly(outcome) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("read evaluates")
    else {
        panic!("read-only command must not produce an evaluated commit");
    };
    assert_eq!(outcome.outcome_id(), outcome_id(plan, "Found"));
}

#[test]
fn one_partition_cross_domain_read_preserves_every_influential_dependency() {
    let bundle = compile_contract_source(CROSS_DOMAIN_SOURCE).expect("contract compiles");
    let plan = command(&bundle, "Advance");
    let input = input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string("advance-1").expect("string"),
            ),
            ("organization_id", CanonicalValue::Uuid([0x99; 16])),
            ("write_domain", CanonicalValue::I64(1)),
            ("observed_domain", CanonicalValue::I64(2)),
        ],
    );
    let observations = plan
        .bindings()
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let target = derive_binding_target(plan, &input, index);
            let key_values = plan.bindings()[index]
                .key_schema()
                .decode_entity(target.key())
                .expect("key decodes");
            let entity = bundle
                .schema()
                .entity(plan.bindings()[index].entity_type())
                .expect("entity");
            let fields = input_record(
                entity.record(),
                [
                    ("organization_id", key_values[0].clone()),
                    ("domain", key_values[1].clone()),
                    ("value", CanonicalValue::I64(10 + index as i64)),
                ],
            );
            EntityObservation::Present(stored_record(&bundle, plan, target, fields))
        })
        .collect();
    let snapshot = snapshot(plan_ref(&bundle, plan), observations);
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(50, 0).expect("timestamp")),
    );

    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("cross-domain command evaluates")
    else {
        panic!("advance mutates");
    };
    assert_eq!(evaluated.mutations().len(), 1);
    assert_eq!(evaluated.read_dependencies().as_slice().len(), 2);
    assert!(
        evaluated
            .read_dependencies()
            .as_slice()
            .iter()
            .all(|dependency| matches!(dependency, ReadDependency::EntityObservation { .. }))
    );
}

#[test]
fn relationship_target_read_remains_an_ordinary_commit_dependency() {
    let bundle = compile_contract_source(RELATIONSHIP_SOURCE).expect("relationship contract");
    let plan = command(&bundle, "CreateChild");
    assert_eq!(plan.relationship_checks().len(), 1);
    let input = input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string("child-1").expect("string"),
            ),
            ("tenant_id", CanonicalValue::Uuid([0x11; 16])),
            ("parent_id", CanonicalValue::Uuid([0x22; 16])),
            ("child_id", CanonicalValue::Uuid([0x33; 16])),
        ],
    );
    let parent_target = derive_binding_target(plan, &input, 0);
    let parent_entity = bundle
        .schema()
        .entity(plan.bindings()[0].entity_type())
        .expect("parent schema");
    let parent_fields = input_record(
        parent_entity.record(),
        [
            ("tenant_id", CanonicalValue::Uuid([0x11; 16])),
            ("parent_id", CanonicalValue::Uuid([0x22; 16])),
        ],
    );
    let child_target = derive_binding_target(plan, &input, 1);
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![
            EntityObservation::Present(stored_record(
                &bundle,
                plan,
                parent_target.clone(),
                parent_fields,
            )),
            EntityObservation::Absent(child_target),
        ],
    );
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(51, 0).expect("timestamp")),
    );
    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("relationship command evaluates")
    else {
        panic!("create requires commit");
    };
    assert_eq!(evaluated.read_dependencies().as_slice().len(), 2);
    assert!(
        evaluated
            .read_dependencies()
            .as_slice()
            .iter()
            .any(|dependency| matches!(
                dependency,
                ReadDependency::EntityObservation { target, .. } if target == &parent_target
            ))
    );
}

#[test]
fn binding_failure_priority_precedes_internal_root_validation() {
    let bundle = compile_contract_source(ROOT_VALIDATION_SOURCE).expect("root contract compiles");
    let plan = command(&bundle, "ChangeChildren");
    let input = root_validation_input(plan);
    let bindings = (0..2)
        .map(|index| EntityObservation::Absent(derive_binding_target(plan, &input, index)))
        .collect();
    let roots = vec![EntityObservation::Absent(derive_root_target(
        plan, &input, 0,
    ))];
    let snapshot = snapshot_with_roots(plan_ref(&bundle, plan), bindings, roots);
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(60, 0).expect("timestamp")),
    );
    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("binding failure is declared before root absence")
    else {
        panic!("mutating rejection must require commit");
    };
    assert_eq!(
        evaluated.outcome().outcome_id(),
        outcome_id(plan, "MissingFirst")
    );
    assert!(evaluated.mutations().is_empty());
}

#[test]
fn missing_internal_root_after_successful_bindings_is_integrity() {
    let bundle = compile_contract_source(ROOT_VALIDATION_SOURCE).expect("root contract compiles");
    let plan = command(&bundle, "ChangeChildren");
    let input = root_validation_input(plan);
    let bindings = plan
        .bindings()
        .iter()
        .enumerate()
        .map(|(index, binding)| {
            let target = derive_binding_target(plan, &input, index);
            let key_values = binding
                .key_schema()
                .decode_entity(target.key())
                .expect("child key");
            let entity = bundle
                .schema()
                .entity(binding.entity_type())
                .expect("child entity");
            let fields = input_record(
                entity.record(),
                [
                    ("tenant", key_values[0].clone()),
                    ("root_id", key_values[1].clone()),
                    ("child_id", key_values[2].clone()),
                    ("amount", CanonicalValue::I64(index as i64)),
                ],
            );
            EntityObservation::Present(stored_record(&bundle, plan, target, fields))
        })
        .collect();
    let roots = vec![EntityObservation::Absent(derive_root_target(
        plan, &input, 0,
    ))];
    let snapshot = snapshot_with_roots(plan_ref(&bundle, plan), bindings, roots);
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(61, 0).expect("timestamp")),
    );
    assert_eq!(
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1()),
        Err(ExecutionFault::Integrity)
    );
}

#[test]
fn multiple_mutations_are_canonical_even_when_binding_order_is_not() {
    let bundle = compile_contract_source(ROOT_VALIDATION_SOURCE).expect("root contract compiles");
    let plan = command(&bundle, "ChangeChildren");
    let input = root_validation_input_with(plan, [0xff; 16], [0x01; 16]);
    let bindings = plan
        .bindings()
        .iter()
        .enumerate()
        .map(|(index, binding)| {
            let target = derive_binding_target(plan, &input, index);
            let key_values = binding
                .key_schema()
                .decode_entity(target.key())
                .expect("child key");
            let entity = bundle
                .schema()
                .entity(binding.entity_type())
                .expect("child entity");
            EntityObservation::Present(stored_record(
                &bundle,
                plan,
                target,
                input_record(
                    entity.record(),
                    [
                        ("tenant", key_values[0].clone()),
                        ("root_id", key_values[1].clone()),
                        ("child_id", key_values[2].clone()),
                        ("amount", CanonicalValue::I64(index as i64)),
                    ],
                ),
            ))
        })
        .collect();
    let root_target = derive_root_target(plan, &input, 0);
    let root_read = &plan.root_validation_reads()[0];
    let root_entity = bundle
        .schema()
        .entity(root_read.entity_type())
        .expect("root entity");
    let key_values = root_read
        .key_schema()
        .decode_entity(root_target.key())
        .expect("root key");
    let root = EntityObservation::Present(stored_record(
        &bundle,
        plan,
        root_target,
        input_record(
            root_entity.record(),
            [
                ("tenant", key_values[0].clone()),
                ("root_id", key_values[1].clone()),
                ("total", CanonicalValue::I64(0)),
            ],
        ),
    ));
    let snapshot = snapshot_with_roots(plan_ref(&bundle, plan), bindings, vec![root]);
    let context = context(
        &bundle,
        plan,
        &input,
        LogicalTime::new(Timestamp::new(62, 0).expect("timestamp")),
    );
    let ExecutionResult::CommitRequired(evaluated) =
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1())
            .expect("multi-mutation command evaluates")
    else {
        panic!("command mutates");
    };
    assert_eq!(evaluated.mutations().len(), 2);
    assert!(
        evaluated.mutations()[0].target().key().as_bytes()
            < evaluated.mutations()[1].target().key().as_bytes()
    );
}

#[test]
fn context_partition_mismatch_is_integrity_and_debug_is_redacted() {
    let bundle = budget_bundle();
    let plan = command(&bundle, "CreateBudget");
    let input = create_input(plan, [0xaa; 16], 2032, 1_000);
    let target = derive_binding_target(plan, &input, 0);
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Absent(target)],
    );
    let other_input = create_input(plan, [0xbb; 16], 2032, 1_000);
    let context = context(
        &bundle,
        plan,
        &other_input,
        LogicalTime::new(Timestamp::new(1, 0).expect("timestamp")),
    );
    assert_eq!(
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1()),
        Err(ExecutionFault::Integrity)
    );
    assert_eq!(format!("{context:?}"), "TransactionContext([REDACTED])");
}

#[test]
fn prepared_locality_arithmetic_failure_is_integrity_not_business_arithmetic() {
    let bundle = compile_contract_source(PREPARED_ARITHMETIC_SOURCE)
        .expect("prepared-arithmetic contract compiles");
    let plan = command(&bundle, "CreateRow");
    let input = input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string("prepared-overflow").expect("string"),
            ),
            ("id", CanonicalValue::I64(i64::MAX)),
        ],
    );

    // These values stand in for an impossible admitted preparation. The
    // repeated `id + 1` derivation must close as integrity before execution.
    let key = plan.bindings()[0]
        .key_schema()
        .encode_entity(&[CanonicalValue::I64(0)])
        .expect("synthetic entity key");
    let target =
        EntityTarget::new(plan.bindings()[0].entity_type(), key).expect("synthetic entity target");
    let snapshot = snapshot(
        plan_ref(&bundle, plan),
        vec![EntityObservation::Absent(target)],
    );
    let partition = plan
        .locality()
        .partition_schema()
        .encode_partition(&[CanonicalValue::I64(0)])
        .expect("synthetic partition key");
    let context = TransactionContext::new(
        RequestId::from_unix_milliseconds_and_random(1, [0x13; 10]).expect("request ID"),
        AdmittedActorContext::new(
            ActorId::new("runtime-test").expect("actor"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        ),
        plan_ref(&bundle, plan),
        LogicalTime::new(Timestamp::new(1, 0).expect("timestamp")),
        partition,
    );

    assert_eq!(
        execute_command(&bundle, &input, &snapshot, &context, EvaluationBudget::v1()),
        Err(ExecutionFault::Integrity)
    );
}

fn budget_bundle() -> ContractBundle {
    compile_contract_source(BUDGET_SOURCE).expect("budget contract compiles")
}

fn command<'a>(bundle: &'a ContractBundle, name: &str) -> &'a CommandPlan {
    bundle
        .commands()
        .iter()
        .find(|command| command.name() == name)
        .expect("command exists")
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

fn snapshot(plan: ExecutablePlanRef, bindings: Vec<EntityObservation>) -> ReadSnapshot {
    snapshot_with_roots(plan, bindings, vec![])
}

fn snapshot_with_roots(
    plan: ExecutablePlanRef,
    bindings: Vec<EntityObservation>,
    roots: Vec<EntityObservation>,
) -> ReadSnapshot {
    let targets = bindings
        .iter()
        .map(|observation| observation.target().clone())
        .collect();
    let root_targets = roots
        .iter()
        .map(|observation| observation.target().clone())
        .collect();
    let request =
        SnapshotRequest::new(plan, targets, root_targets, vec![]).expect("snapshot request");
    ReadSnapshot::new(&request, None, bindings, roots, vec![]).expect("snapshot")
}

fn context(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    input: &CanonicalRecord,
    logical_time: LogicalTime,
) -> TransactionContext {
    let partition_expression = plan.locality().partition_expression();
    let values = Inputs { input };
    let value = evaluate_expression(plan.expressions(), partition_expression, &values)
        .expect("partition expression");
    let partition = plan
        .locality()
        .partition_schema()
        .encode_partition(&[value])
        .expect("partition key");
    let service_uuid = RequestId::from_unix_milliseconds_and_random(7, [0x77; 10])
        .expect("service UUID")
        .into_bytes();
    let service_values = CanonicalRecord::new(
        plan.service_values()
            .iter()
            .map(|schema| {
                let value = match schema.kind() {
                    riffdb_contract_ir::ServiceValueKind::TransactionTime => {
                        CanonicalValue::Timestamp(logical_time.timestamp())
                    }
                    riffdb_contract_ir::ServiceValueKind::UuidV7 => {
                        CanonicalValue::Uuid(service_uuid)
                    }
                };
                (schema.field().id(), value)
            })
            .collect(),
    )
    .expect("canonical service values");
    TransactionContext::new_with_service_values(
        RequestId::from_unix_milliseconds_and_random(1, [0x12; 10]).expect("request ID"),
        AdmittedActorContext::new(
            ActorId::new("runtime-test").expect("actor"),
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

fn derive_binding_target(
    plan: &CommandPlan,
    input: &CanonicalRecord,
    index: usize,
) -> EntityTarget {
    let binding = &plan.bindings()[index];
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
    EntityTarget::new(binding.entity_type(), key).expect("entity target")
}

fn derive_root_target(plan: &CommandPlan, input: &CanonicalRecord, index: usize) -> EntityTarget {
    let read = &plan.root_validation_reads()[index];
    let values = Inputs { input };
    let components = read
        .key_expressions()
        .iter()
        .map(|expression| {
            evaluate_expression(plan.expressions(), *expression, &values)
                .expect("root key expression")
        })
        .collect::<Vec<_>>();
    let key = read
        .key_schema()
        .encode_entity(&components)
        .expect("root entity key");
    EntityTarget::new(read.entity_type(), key).expect("root entity target")
}

fn root_validation_input(plan: &CommandPlan) -> CanonicalRecord {
    root_validation_input_with(plan, [0xc3; 16], [0xc4; 16])
}

fn root_validation_input_with(
    plan: &CommandPlan,
    first_child: [u8; 16],
    second_child: [u8; 16],
) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "request_key",
                CanonicalValue::string("change-children-1").expect("string"),
            ),
            ("tenant", CanonicalValue::Uuid([0xc1; 16])),
            ("root_id", CanonicalValue::Uuid([0xc2; 16])),
            ("first_child", CanonicalValue::Uuid(first_child)),
            ("second_child", CanonicalValue::Uuid(second_child)),
            ("amount", CanonicalValue::I64(5)),
        ],
    )
}

fn workflow_transition_input(plan: &CommandPlan, expected_revision: u64) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "request_key",
                CanonicalValue::string("start-work-1").expect("string"),
            ),
            ("organization_id", CanonicalValue::Uuid([0xd1; 16])),
            ("work_id", CanonicalValue::Uuid([0xd2; 16])),
            ("expected_revision", CanonicalValue::U64(expected_revision)),
        ],
    )
}

fn workflow_lease_claim_input(
    plan: &CommandPlan,
    request_key: &str,
    duration: u64,
    expected_revision: u64,
) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "request_key",
                CanonicalValue::string(request_key).expect("string"),
            ),
            ("organization_id", CanonicalValue::Uuid([0xd1; 16])),
            ("work_id", CanonicalValue::Uuid([0xd2; 16])),
            ("owner_id", CanonicalValue::Uuid([0xd3; 16])),
            ("duration", CanonicalValue::U64(duration)),
            ("expected_revision", CanonicalValue::U64(expected_revision)),
        ],
    )
}

#[allow(clippy::too_many_arguments)]
fn workflow_lease_stored_record(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    target: EntityTarget,
    version: EntityVersion,
    owner: CanonicalValue,
    expiry: CanonicalValue,
    fence: u64,
    attempts: u64,
) -> StoredEntityRecordV1 {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "WorkItem")
        .expect("WorkItem entity");
    let key_values = entity
        .primary_key()
        .decode_entity(target.key())
        .expect("workflow key");
    stored_record_with_version(
        bundle,
        plan,
        target,
        version,
        input_record(
            entity.record(),
            [
                ("organization_id", key_values[0].clone()),
                ("work_id", key_values[1].clone()),
                ("state", enum_value(bundle, "WorkState", "Running")),
                ("lease_owner", owner),
                ("lease_expires_at", expiry),
                ("lease_fence", CanonicalValue::U64(fence)),
                ("lease_attempts", CanonicalValue::U64(attempts)),
            ],
        ),
    )
}

fn workflow_stored_record(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    target: EntityTarget,
    version: EntityVersion,
    state: &str,
) -> StoredEntityRecordV1 {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "WorkItem")
        .expect("WorkItem entity");
    let key_values = entity
        .primary_key()
        .decode_entity(target.key())
        .expect("workflow key");
    stored_record_with_version(
        bundle,
        plan,
        target,
        version,
        input_record(
            entity.record(),
            [
                ("organization_id", key_values[0].clone()),
                ("work_id", key_values[1].clone()),
                ("state", enum_value(bundle, "WorkState", state)),
            ],
        ),
    )
}

fn create_input(
    plan: &CommandPlan,
    organization: [u8; 16],
    year: i64,
    approved: i128,
) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string("create-1").expect("string"),
            ),
            ("organization_id", CanonicalValue::Uuid(organization)),
            ("fiscal_year", CanonicalValue::I64(year)),
            ("approved_amount", decimal(approved)),
        ],
    )
}

fn allocate_input(
    plan: &CommandPlan,
    organization: [u8; 16],
    year: i64,
    matter: [u8; 16],
    amount: i128,
) -> CanonicalRecord {
    input_record(
        plan.input().record(),
        [
            (
                "idempotency_key",
                CanonicalValue::string("allocate-1").expect("string"),
            ),
            ("organization_id", CanonicalValue::Uuid(organization)),
            ("fiscal_year", CanonicalValue::I64(year)),
            ("matter_id", CanonicalValue::Uuid(matter)),
            ("amount", decimal(amount)),
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
    .expect("canonical record")
}

fn stored_budget(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    target: EntityTarget,
    approved: i128,
    allocated: i128,
    updated_at: Timestamp,
    unknowns: Vec<(FieldId, CanonicalValue)>,
) -> StoredEntityRecordV1 {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Budget")
        .expect("Budget entity");
    let key_values = entity
        .primary_key()
        .decode_entity(target.key())
        .expect("budget key");
    let mut fields = entity
        .record()
        .fields()
        .iter()
        .map(|field| {
            let value = match field.name() {
                "organization_id" => key_values[0].clone(),
                "fiscal_year" => key_values[1].clone(),
                "approved_amount" => decimal(approved),
                "allocated_amount" => decimal(allocated),
                "updated_at" => CanonicalValue::Timestamp(updated_at),
                other => panic!("unexpected Budget field {other}"),
            };
            (field.id(), value)
        })
        .collect::<Vec<_>>();
    fields.extend(unknowns);
    stored_record(
        bundle,
        plan,
        target,
        CanonicalRecord::new(fields).expect("budget record"),
    )
}

fn stored_record(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    target: EntityTarget,
    fields: CanonicalRecord,
) -> StoredEntityRecordV1 {
    stored_record_with_version(bundle, plan, target, EntityVersion::first(), fields)
}

fn stored_record_with_version(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    target: EntityTarget,
    version: EntityVersion,
    fields: CanonicalRecord,
) -> StoredEntityRecordV1 {
    StoredEntityRecordV1::new(
        target,
        version,
        bundle.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan_ref(bundle, plan)),
        fields,
    )
    .expect("stored record")
}

fn enum_value(bundle: &ContractBundle, enumeration: &str, variant: &str) -> CanonicalValue {
    let enumeration = bundle
        .schema()
        .enums()
        .iter()
        .find(|candidate| candidate.name() == enumeration)
        .expect("enumeration");
    let variant_id = enumeration
        .variants()
        .iter()
        .find(|candidate| candidate.name() == variant)
        .expect("enum variant")
        .id();
    CanonicalValue::Enum {
        type_id: enumeration.id(),
        variant_id,
    }
}

fn outcome_id(plan: &CommandPlan, name: &str) -> OutcomeId {
    plan.outcomes()
        .iter()
        .find(|outcome| outcome.name() == name)
        .expect("outcome exists")
        .id()
}

fn outcome_field(plan: &CommandPlan, outcome: &str, field: &str) -> FieldId {
    plan.outcomes()
        .iter()
        .find(|candidate| candidate.name() == outcome)
        .and_then(|outcome| {
            outcome
                .payload()
                .fields()
                .iter()
                .find(|candidate| candidate.name() == field)
        })
        .expect("outcome field")
        .id()
}

fn entity_field(bundle: &ContractBundle, entity: &str, field: &str) -> FieldId {
    bundle
        .schema()
        .entities()
        .iter()
        .find(|candidate| candidate.name() == entity)
        .and_then(|entity| {
            entity
                .record()
                .fields()
                .iter()
                .find(|candidate| candidate.name() == field)
        })
        .expect("entity field")
        .id()
}

fn event_field(bundle: &ContractBundle, event: &str, field: &str) -> FieldId {
    bundle
        .schema()
        .events()
        .iter()
        .find(|candidate| candidate.name() == event)
        .and_then(|event| {
            event
                .payload()
                .fields()
                .iter()
                .find(|candidate| candidate.name() == field)
        })
        .expect("event field")
        .id()
}

fn field(record: &CanonicalRecord, field: FieldId) -> &CanonicalValue {
    record
        .fields()
        .iter()
        .find(|(candidate, _)| *candidate == field)
        .map(|(_, value)| value)
        .expect("record field")
}

fn record_contains_field(record: &CanonicalRecord, field: FieldId) -> bool {
    record.fields().iter().any(|(candidate, value)| {
        *candidate == field
            || match value {
                CanonicalValue::Record(record) => record_contains_field(record, field),
                CanonicalValue::List(values) => values.values().iter().any(|value| match value {
                    CanonicalValue::Record(record) => record_contains_field(record, field),
                    _ => false,
                }),
                _ => false,
            }
    })
}

fn decimal(coefficient: i128) -> CanonicalValue {
    let spec = DecimalSpec::new(28, 2).expect("budget decimal spec");
    CanonicalValue::Decimal(Decimal::new(spec, coefficient).expect("budget decimal"))
}

fn decimal_coefficient(value: &CanonicalValue) -> i128 {
    let CanonicalValue::Decimal(value) = value else {
        panic!("expected decimal");
    };
    value.coefficient()
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
