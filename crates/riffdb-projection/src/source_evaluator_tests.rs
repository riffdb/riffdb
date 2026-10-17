//! Pure admitted evaluator semantics; native input capture/publication pending.
// req: PRJ-004, STO-020, END-008
use super::*;
use riffdb_catalog::{ActiveCatalogSnapshot, ResolvedProjectionPlan, ValidatedContractBundle};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_storage_api::{
    ActiveCatalogPointerV1, CatalogRepository, CheckedProjectionSchema, DeclaredOutcome,
    DurabilityMode, ExecutablePlanRef, ReadDependencies, StorageError, StoredCommitRecordV1,
    StoredContractBundleV1, StoredDurableEventV1, StoredReadDependenciesV1, derive_event_hash_v1,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash, CanonicalRecord,
    CanonicalValue, CommitSequence, ContractLineage, EventId, FieldId, LogicalTime, OutcomeId,
    PartitionKeyBuilder, ProjectionGeneration, ProjectionIdentity, ProvenanceId, RequestId,
    TenantScope, Timestamp, hash_partition_key,
};

#[test]
fn source_evaluator_pressure_refuses_before_work_and_ordinary_evaluation_remains_independent() {
    let fixture = evaluator_fixture();
    let commit = evaluator_commit(&fixture, CommitSequence::first(), &[(10, 4)]);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let work = capacity::work_bound(&fixture.resolved, &fixture.schema, &commit).unwrap();
    drop(scratch);
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.owned_bytes
        - before.reserved_bytes;
    let occupied = budget
        .reserve(available - (OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + work - 1))
        .unwrap();
    let held = budget.observation().unwrap();
    // The actual census fits: this failure is the main work reservation at n-1,
    // not total exhaustion rejecting the first census admission.
    assert!(work > 0);
    let census_fits = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    drop(census_fits);
    assert_eq!(budget.observation().unwrap(), held);
    assert!(
        matches!(evaluate_projection_commit_source_admitted_v1(&fixture.resolved, &fixture.schema, &commit, &budget),
        Err(error) if error.kind() == ProjectionEvaluationErrorKind::StorageUnavailable)
    );
    assert_eq!(
        budget.observation().unwrap(),
        held,
        "failed admission must release census scratch"
    );
    let ordinary = evaluate_projection_commit(
        &fixture.resolved,
        fixture.schema.clone(),
        ProjectionGeneration::first(),
        &commit,
    )
    .unwrap();
    assert_eq!(ordinary.changed_group_count(), 1);
    drop(occupied);
    assert_eq!(budget.observation().unwrap(), before);

    // Exactly census+work fits. This also rules out an implementation that
    // returns Unavailable regardless of capacity or changes the charged bound.
    let occupied = budget
        .reserve(available - (OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + work))
        .unwrap();
    let held = budget.observation().unwrap();
    let admitted = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &commit,
        &budget,
    )
    .unwrap();
    assert_eq!(admitted.groups().len(), 1);
    let admitted_balance = budget.observation().unwrap();
    assert_eq!(admitted_balance.reserved_bytes, held.reserved_bytes + work);
    assert_eq!(admitted_balance.owned_bytes, held.owned_bytes);
    drop(admitted);
    assert_eq!(budget.observation().unwrap(), held);
    drop(occupied);
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn source_evaluator_matches_ordinary_groups_filter_and_empty_command_progress_then_releases() {
    let fixture = evaluator_fixture();
    let commit = evaluator_commit(
        &fixture,
        CommitSequence::first(),
        &[(10, 4), (10, -1), (20, 100)],
    );
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let ordinary = evaluate_projection_commit(
        &fixture.resolved,
        fixture.schema.clone(),
        ProjectionGeneration::first(),
        &commit,
    )
    .unwrap();
    let admitted = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &commit,
        &budget,
    )
    .unwrap();
    assert_eq!(admitted.identity(), ordinary.identity());
    assert_eq!(admitted.sequence(), ordinary.sequence());
    assert_eq!(
        admitted.groups().collect::<Vec<_>>(),
        ordinary.grouped_deltas().iter().collect::<Vec<_>>()
    );
    assert!(budget.observation().unwrap().reserved_bytes > before.reserved_bytes);
    drop(admitted);
    assert_eq!(budget.observation().unwrap(), before);
    let irrelevant = evaluator_commit(&fixture, CommitSequence::new(2).unwrap(), &[(20, 3)]);
    let admitted = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &irrelevant,
        &budget,
    )
    .unwrap();
    assert_eq!(admitted.sequence().get(), 2);
    assert_eq!(admitted.groups().len(), 0);
    drop(admitted);
    let empty = evaluator_commit(&fixture, CommitSequence::new(3).unwrap(), &[]);
    let admitted = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &empty,
        &budget,
    )
    .unwrap();
    assert_eq!(admitted.sequence().get(), 3);
    assert_eq!(admitted.groups().len(), 0);
    drop(admitted);
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn source_evaluator_preserves_complete_command_checked_overflow_association() {
    let fixture = evaluator_fixture();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let commit = evaluator_commit(
        &fixture,
        CommitSequence::first(),
        &[(10, i64::MAX), (10, -i64::MAX)],
    );
    let admitted = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &commit,
        &budget,
    )
    .unwrap();
    let (_, delta) = admitted.groups().next().unwrap();
    let prior = CanonicalRecord::new(
        fixture
            .measure_fields
            .iter()
            .map(|(field, count)| {
                (
                    *field,
                    if *count {
                        CanonicalValue::U64(1)
                    } else {
                        CanonicalValue::I64(i64::MAX)
                    },
                )
            })
            .collect(),
    )
    .unwrap();
    let combined = crate::evaluator::add_measure_records(&prior, delta).unwrap();
    for ((_, value), (_, is_count)) in combined.fields().iter().zip(&fixture.measure_fields) {
        assert_eq!(
            value,
            &if *is_count {
                CanonicalValue::U64(3)
            } else {
                CanonicalValue::I64(i64::MAX)
            }
        );
    }
    drop(admitted);
    let overflow = evaluator_commit(
        &fixture,
        CommitSequence::new(2).unwrap(),
        &[(10, i64::MAX), (10, 1), (10, -1)],
    );
    let ordinary = evaluate_projection_commit(
        &fixture.resolved,
        fixture.schema.clone(),
        ProjectionGeneration::first(),
        &overflow,
    )
    .unwrap_err();
    let source = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &overflow,
        &budget,
    )
    .err()
    .unwrap();
    assert_eq!(
        ordinary.kind(),
        ProjectionEvaluationErrorKind::ArithmeticOverflow
    );
    assert_eq!(source.kind(), ordinary.kind());
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn source_evaluator_retains_foreign_catalog_custody_without_reassigning_shared_identity() {
    let fixture = evaluator_fixture();
    let commit = evaluator_commit(&fixture, CommitSequence::first(), &[(10, 2)]);
    let provider = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
    let schema_owner = provider
        .reserve(8 * 1024 * 1024)
        .unwrap()
        .settle_shared_value(fixture.schema.clone())
        .unwrap();
    let provider_before = provider.observation().unwrap();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let admitted = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &schema_owner,
        &commit,
        &budget,
    )
    .unwrap();
    assert_eq!(admitted.groups().len(), 1);
    assert_eq!(
        budget.observation().unwrap().owned_bytes,
        before.owned_bytes
    );
    assert_eq!(provider.observation().unwrap(), provider_before);
    drop(admitted);
    assert_eq!(budget.observation().unwrap(), before);
    assert_eq!(provider.observation().unwrap(), provider_before);
}

#[test]
fn source_evaluator_sizing_refusal_never_invents_authoritative_failure_kind() {
    let fixture = evaluator_fixture();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let command = fixture.bundle.bundle().commands().first().unwrap();
    let wrong_writer = ExecutablePlanRef::new(
        fixture.bundle.lineage().clone(),
        fixture.bundle.contract_version(),
        fixture.bundle.bundle_hash(),
        command.command_id(),
        riffdb_types::PlanHash::from_bytes([0xf1; 32]),
    );
    assert_ne!(wrong_writer.command_plan_hash(), command.plan_hash());
    let commit = evaluator_commit_with_plan(
        &fixture,
        CommitSequence::first(),
        &[(10, 2)],
        Some(wrong_writer),
    );
    let ordinary = evaluate_projection_commit(
        &fixture.resolved,
        fixture.schema.clone(),
        ProjectionGeneration::first(),
        &commit,
    )
    .unwrap_err();
    assert_eq!(
        ordinary.kind(),
        ProjectionEvaluationErrorKind::MalformedDurableEvent
    );
    let source = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &commit,
        &budget,
    )
    .err()
    .unwrap();
    assert_eq!(
        source.kind(),
        ProjectionEvaluationErrorKind::StorageUnavailable
    );
    assert_eq!(
        source.kind().failure_code(),
        None,
        "sizing must not invent an authoritative failure"
    );
    assert_eq!(budget.observation().unwrap(), before);

    let other = evaluator_fixture_from_source(&EVALUATOR_CONTRACT.replace(
        "contract ProjectionEvaluation version",
        "contract ProjectionEvaluationOther version",
    ));
    assert_ne!(other.schema.identity(), fixture.schema.identity());
    let valid = evaluator_commit(&fixture, CommitSequence::first(), &[(10, 2)]);
    let ordinary = evaluate_projection_commit(
        &fixture.resolved,
        other.schema.clone(),
        ProjectionGeneration::first(),
        &valid,
    )
    .unwrap_err();
    let source = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &other.schema,
        &valid,
        &budget,
    )
    .err()
    .unwrap();
    assert_eq!(
        ordinary.kind(),
        ProjectionEvaluationErrorKind::PlanOrSchemaUnavailable
    );
    assert_eq!(source.kind(), ordinary.kind());
    assert_eq!(budget.observation().unwrap(), before);
}

const EVALUATOR_CONTRACT: &str = r#"
contract ProjectionEvaluation version 1 {
  entity Row {
    key (id: i64)
    field seen: u64
  }

  event Added {
    group: i64
    amount: i64
  }

  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }

  command Add {
    input request_key: string<128>
    input id: i64
    input group: i64
    input amount: i64
    idempotency_key request_key
    create Row(id) as row else Exists { id: id }
    set row.seen = 1
    emit Added { group: group, amount: amount }
    return AddedOutcome { row: row }
  }

  projection Totals {
    source event Added
    where group == 10
    key (group)
    measure item_count = count()
    measure total = sum(amount)
    frontier transactionally_ordered
  }
}
"#;

struct ProjectionCatalogRepository {
    active: ActiveCatalogPointerV1,
    bundle: StoredContractBundleV1,
}

impl CatalogRepository for ProjectionCatalogRepository {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(Some(self.active.clone()))
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        version: riffdb_types::ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        Ok(
            (self.bundle.lineage() == lineage && self.bundle.contract_version() == version)
                .then(|| self.bundle.clone()),
        )
    }
}

struct EvaluatorFixture {
    bundle: ValidatedContractBundle,
    resolved: ResolvedProjectionPlan,
    schema: CheckedProjectionSchema,
    group_field: FieldId,
    amount_field: FieldId,
    measure_fields: Vec<(FieldId, bool)>,
}

fn evaluator_fixture() -> EvaluatorFixture {
    evaluator_fixture_from_source(EVALUATOR_CONTRACT)
}

fn evaluator_fixture_from_source(source: &str) -> EvaluatorFixture {
    let compiled = compile_contract_source(source).expect("evaluator contract");
    let bundle =
        ValidatedContractBundle::from_compiler_bundle(compiled).expect("validated catalog bundle");
    let stored = bundle.to_stored().expect("stored bundle");
    let repository = ProjectionCatalogRepository {
        active: ActiveCatalogPointerV1::from_bundle(&stored),
        bundle: stored,
    };
    let active = ActiveCatalogSnapshot::read(&repository)
        .expect("catalog read")
        .expect("active catalog");
    let projection = bundle
        .bundle()
        .projections()
        .first()
        .expect("projection plan");
    let identity = ProjectionIdentity::new(
        bundle.lineage().clone(),
        projection.projection_id(),
        projection.plan_hash(),
    );
    let resolved = active
        .resolve_projection(&identity)
        .expect("resolved projection");
    let schema = CheckedProjectionSchema::new(
        bundle
            .bundle()
            .bound_projection_group_schema(projection.projection_id())
            .expect("bound group schema"),
    );
    let event = bundle
        .bundle()
        .schema()
        .event(projection.source_event())
        .expect("source event");
    let field = |name: &str| {
        event
            .payload()
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .map(riffdb_contract_ir::FieldSchema::id)
            .expect("event field")
    };
    let group_field = field("group");
    let amount_field = field("amount");
    let count_field = projection
        .measures()
        .iter()
        .find(|measure| measure.expression().is_none())
        .expect("count")
        .field()
        .id();
    let measure_fields = projection
        .group_schema()
        .measures()
        .fields()
        .iter()
        .map(|field| (field.id(), field.id() == count_field))
        .collect();
    EvaluatorFixture {
        bundle,
        resolved,
        schema,
        group_field,
        amount_field,
        measure_fields,
    }
}

fn uuid_bytes(seed: u8) -> [u8; 16] {
    let mut bytes = [seed; 16];
    bytes[6] = 0x70 | (seed & 0x0f);
    bytes[8] = 0x80 | (seed & 0x3f);
    bytes
}

fn evaluator_commit(
    fixture: &EvaluatorFixture,
    sequence: CommitSequence,
    values: &[(i64, i64)],
) -> StoredCommitRecordV1 {
    evaluator_commit_with_plan(fixture, sequence, values, None)
}

fn evaluator_commit_with_plan(
    fixture: &EvaluatorFixture,
    sequence: CommitSequence,
    values: &[(i64, i64)],
    replacement_plan: Option<ExecutablePlanRef>,
) -> StoredCommitRecordV1 {
    let command = fixture
        .bundle
        .bundle()
        .commands()
        .first()
        .expect("writer command");
    let plan = ExecutablePlanRef::new(
        fixture.bundle.lineage().clone(),
        fixture.bundle.contract_version(),
        fixture.bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    );
    let plan = replacement_plan.unwrap_or(plan);
    let events = values
        .iter()
        .enumerate()
        .map(|(ordinal, (group, amount))| {
            let event_id = EventId::new(
                sequence,
                u32::try_from(ordinal).expect("bounded event ordinal"),
            );
            let payload = CanonicalRecord::new(vec![
                (fixture.group_field, CanonicalValue::I64(*group)),
                (fixture.amount_field, CanonicalValue::I64(*amount)),
            ])
            .expect("event payload");
            let event_type = fixture.resolved.projection_plan().source_event();
            let event_hash =
                derive_event_hash_v1(event_id, event_type, &payload).expect("event hash");
            StoredDurableEventV1::new(event_id, event_type, payload, event_hash)
                .expect("stored event")
        })
        .collect::<Vec<_>>();
    let event_ids = events
        .iter()
        .map(StoredDurableEventV1::event_id)
        .collect::<Vec<_>>();
    let actor = AdmittedActorContext::new(
        ActorId::new("projection-worker-fixture").expect("actor"),
        ActorKind::Service,
        TenantScope::Global,
        None,
    );
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
    partition.push_i64(1).expect("partition component");
    let partition_hash = hash_partition_key(partition.finish().expect("partition key").as_bytes());
    let dependencies = StoredReadDependenciesV1::from_live(
        &ReadDependencies::new(Vec::new()).expect("empty dependencies"),
    )
    .expect("stored dependencies");
    StoredCommitRecordV1::new(
        sequence,
        RequestId::from_bytes(uuid_bytes(u8::try_from(sequence.get()).unwrap_or(0x21)))
            .expect("request"),
        plan,
        CanonicalInputHash::from_bytes([0x41; 32]),
        actor,
        LogicalTime::new(Timestamp::new(2 * 86_400, 123).expect("logical time")),
        partition_hash,
        Vec::new(),
        dependencies,
        Vec::new(),
        events,
        DeclaredOutcome::new(
            OutcomeId::first(),
            CanonicalRecord::new(Vec::new()).expect("outcome payload"),
        )
        .expect("outcome"),
        ProvenanceId::from_bytes(uuid_bytes(
            u8::try_from(sequence.get())
                .unwrap_or(0x21)
                .wrapping_add(0x40),
        ))
        .expect("provenance"),
        event_ids,
        DurabilityMode::Memory,
    )
    .expect("stored commit")
}
