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
    let mut scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let work = capacity::work_bound(
        &fixture.resolved,
        &fixture.schema,
        &commit,
        scratch.inspection_scope().unwrap(),
    )
    .unwrap();
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
    let (key, _) = admitted.groups().next().unwrap();
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
    let combined = admitted.evaluated.combine_with_prior(key, &prior).unwrap();
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

// req: PRJ-004, STO-020, END-008
#[test]
fn source_evaluator_all_numeric_aggregations_preserve_complete_command_and_prior_row() {
    let source = EVALUATOR_CONTRACT.replace(
        "measure total = sum(amount)",
        "measure total = sum(amount)\n    measure smallest = min(amount)\n    measure largest = max(amount)\n    measure average = mean(amount)",
    );
    let fixture = evaluator_fixture_from_source(&source);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let first_commit = evaluator_commit(&fixture, CommitSequence::first(), &[(10, 10), (10, 10)]);
    let first = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &first_commit,
        &budget,
    )
    .unwrap();
    let first_key = first
        .prepare_source_group_key_v1(first.groups().next().unwrap().0)
        .unwrap();
    let locator = riffdb_types::ProjectionSourceLocatorV1::new(
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(42, [0x39; 10]).unwrap(),
        1,
        riffdb_types::ProjectionCheckpointOrdinal::first(),
    )
    .unwrap();
    let prior = first
        .prepare_source_row_v1(&first_key, None, locator)
        .unwrap();
    let commit = evaluator_commit(
        &fixture,
        CommitSequence::new(2).unwrap(),
        &[(10, 6), (10, -2)],
    );
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
    let (group, _) = admitted.groups().next().unwrap();
    let expected = ordinary
        .combine_with_prior(group, prior.measures())
        .unwrap();
    let key = admitted.prepare_source_group_key_v1(group).unwrap();
    let row = admitted
        .prepare_source_row_v1(&key, Some(&prior), locator)
        .unwrap();
    assert_eq!(row.measures(), &expected);
    for measure in fixture.resolved.projection_plan().measures() {
        let value = row
            .measures()
            .fields()
            .iter()
            .find(|(id, _)| *id == measure.field().id())
            .unwrap();
        let expected = match measure.aggregation() {
            riffdb_contract_ir::ProjectionAggregation::Count => CanonicalValue::U64(4),
            riffdb_contract_ir::ProjectionAggregation::Sum => CanonicalValue::I64(24),
            riffdb_contract_ir::ProjectionAggregation::Min => CanonicalValue::I64(-2),
            riffdb_contract_ir::ProjectionAggregation::Max => CanonicalValue::I64(10),
            riffdb_contract_ir::ProjectionAggregation::Mean => CanonicalValue::I64(6),
        };
        assert_eq!(&value.1, &expected);
    }
    drop(row);
    drop(key);
    drop(admitted);
    drop(prior);
    drop(first_key);
    drop(first);
    assert_source_row_retired(before, budget.observation().unwrap());
}

// req: PRJ-004, STO-020, END-008
#[test]
fn source_evaluator_empty_and_grouped_mean_work_refuses_n_minus_one_then_matches_ordinary() {
    let source = EVALUATOR_CONTRACT.replace("where group == 10", "").replace(
        "measure total = sum(amount)",
        "measure total = sum(amount)\n    measure smallest = min(amount)\n    measure largest = max(amount)\n    measure average = mean(amount)",
    );
    let fixture = evaluator_fixture_from_source(&source);
    assert!(fixture.resolved.projection_plan().filter().is_none());
    for values in [&[][..], &[(10, 6), (10, -2), (20, 8), (20, 2)][..]] {
        let commit = evaluator_commit(&fixture, CommitSequence::first(), values);
        let budget = ProjectionSourceWorkingBudgetV1::new();
        let before = budget.observation().unwrap();
        let mut census = budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let work = capacity::work_bound(
            &fixture.resolved,
            &fixture.schema,
            &commit,
            census.inspection_scope().unwrap(),
        )
        .unwrap();
        drop(census);
        let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
            - before.fixed_owner_bytes
            - before.owned_bytes
            - before.reserved_bytes;
        let exact = OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
            .checked_add(work)
            .unwrap();
        assert!(work > 0 && exact <= available);
        let pressure = budget.reserve(available - (exact - 1)).unwrap();
        let held = budget.observation().unwrap();
        assert!(matches!(evaluate_projection_commit_source_admitted_v1(
            &fixture.resolved, &fixture.schema, &commit, &budget,
        ), Err(error) if error.kind() == ProjectionEvaluationErrorKind::StorageUnavailable));
        assert_eq!(budget.observation().unwrap(), held);
        drop(pressure);
        let pressure = budget.reserve(available - exact).unwrap();
        let held = budget.observation().unwrap();
        let admitted = evaluate_projection_commit_source_admitted_v1(
            &fixture.resolved,
            &fixture.schema,
            &commit,
            &budget,
        )
        .unwrap();
        let ordinary = evaluate_projection_commit(
            &fixture.resolved,
            fixture.schema.clone(),
            ProjectionGeneration::first(),
            &commit,
        )
        .unwrap();
        assert_eq!(
            admitted.groups().collect::<Vec<_>>(),
            ordinary.grouped_deltas().iter().collect::<Vec<_>>()
        );
        assert_eq!(
            admitted.groups().len(),
            if values.is_empty() { 0 } else { 2 }
        );
        assert_eq!(
            budget.observation().unwrap().reserved_bytes,
            held.reserved_bytes + work
        );
        drop(admitted);
        assert_eq!(budget.observation().unwrap(), held);
        drop(pressure);
        assert_eq!(budget.observation().unwrap(), before);
    }
}

pub(crate) const EVALUATOR_CONTRACT: &str = r#"
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

pub(crate) struct EvaluatorFixture {
    pub(crate) bundle: ValidatedContractBundle,
    resolved: ResolvedProjectionPlan,
    schema: CheckedProjectionSchema,
    group_field: FieldId,
    amount_field: FieldId,
    measure_fields: Vec<(FieldId, bool)>,
}

fn evaluator_fixture() -> EvaluatorFixture {
    evaluator_fixture_from_source(EVALUATOR_CONTRACT)
}

pub(crate) fn evaluator_fixture_from_source(source: &str) -> EvaluatorFixture {
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

pub(crate) fn evaluator_commit(
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

// req: PRJ-004, STO-020, END-008
#[test]
fn source_evaluator_rows_preserve_complete_command_arithmetic_and_retire_same_ledger() {
    let fixture = evaluator_fixture();
    let first = evaluator_commit(&fixture, CommitSequence::first(), &[(10, 4), (10, -1)]);
    let second = evaluator_commit(&fixture, CommitSequence::new(2).unwrap(), &[(10, 7)]);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let locator = riffdb_types::ProjectionSourceLocatorV1::new(
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(42, [0x36; 10]).unwrap(),
        1,
        riffdb_types::ProjectionCheckpointOrdinal::first(),
    )
    .unwrap();
    let evaluated = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &first,
        &budget,
    )
    .unwrap();
    assert!(evaluated.belongs_to(&budget));
    let (ordinary_key, _) = evaluated.groups().next().unwrap();
    let key = evaluated.prepare_source_group_key_v1(ordinary_key).unwrap();
    assert!(key.belongs_to(&budget));
    assert_eq!(&*key, ordinary_key);
    let first_row = evaluated
        .prepare_source_row_v1(&key, None, locator)
        .unwrap();
    assert!(first_row.belongs_to(&budget));
    drop(key);
    drop(evaluated);
    let evaluated = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &second,
        &budget,
    )
    .unwrap();
    let key = evaluated
        .prepare_source_group_key_v1(evaluated.groups().next().unwrap().0)
        .unwrap();
    let second_row = evaluated
        .prepare_source_row_v1(&key, Some(&first_row), locator)
        .unwrap();
    assert_eq!(second_row.last_changed_sequence(), second.commit_sequence());
    for ((_, value), (_, count)) in second_row
        .measures()
        .fields()
        .iter()
        .zip(&fixture.measure_fields)
    {
        assert_eq!(
            value,
            &if *count {
                CanonicalValue::U64(3)
            } else {
                CanonicalValue::I64(10)
            }
        );
    }
    let held = budget.observation().unwrap();
    assert!(
        matches!(evaluated.prepare_source_row_v1(&key, Some(&second_row), locator),
        Err(error) if error.kind()==ProjectionEvaluationErrorKind::ProjectionStateIntegrity)
    );
    assert_eq!(budget.observation().unwrap(), held);
    drop(second_row);
    drop(first_row);
    drop(key);
    drop(evaluated);
    assert_source_row_retired(before, budget.observation().unwrap());
}

// req: PRJ-004, STO-020, END-008
#[test]
fn source_evaluator_rows_refuse_foreign_ledger_and_pressure_without_changing_live_rows() {
    let fixture = evaluator_fixture();
    let commit = evaluator_commit(&fixture, CommitSequence::first(), &[(10, 4)]);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let foreign = ProjectionSourceWorkingBudgetV1::new();
    let foreign_before = foreign.observation().unwrap();
    let evaluated = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &commit,
        &budget,
    )
    .unwrap();
    let other = evaluate_projection_commit_source_admitted_v1(
        &fixture.resolved,
        &fixture.schema,
        &commit,
        &foreign,
    )
    .unwrap();
    let foreign_key = other
        .prepare_source_group_key_v1(other.groups().next().unwrap().0)
        .unwrap();
    let locator = riffdb_types::ProjectionSourceLocatorV1::new(
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(42, [0x37; 10]).unwrap(),
        1,
        riffdb_types::ProjectionCheckpointOrdinal::first(),
    )
    .unwrap();
    let held = budget.observation().unwrap();
    let foreign_held = foreign.observation().unwrap();
    assert!(!evaluated.belongs_to(&foreign));
    assert!(
        matches!(evaluated.prepare_source_row_v1(&foreign_key, None, locator),
        Err(error) if error.kind()==ProjectionEvaluationErrorKind::ProjectionStateIntegrity)
    );
    assert_eq!(budget.observation().unwrap(), held);
    assert_eq!(foreign.observation().unwrap(), foreign_held);
    let key = evaluated
        .prepare_source_group_key_v1(evaluated.groups().next().unwrap().0)
        .unwrap();
    let live = budget.observation().unwrap();
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - live.fixed_owner_bytes
        - live.owned_bytes
        - live.reserved_bytes;
    let pressure = budget.reserve(available).unwrap();
    let full = budget.observation().unwrap();
    assert!(
        matches!(evaluated.prepare_source_row_v1(&key, None, locator),
        Err(error) if error.kind()==ProjectionEvaluationErrorKind::StorageUnavailable)
    );
    assert!(matches!(evaluated.prepare_source_group_key_v1(&key),
        Err(error) if error.kind()==ProjectionEvaluationErrorKind::StorageUnavailable));
    assert_eq!(budget.observation().unwrap(), full);
    drop(pressure);
    let row = evaluated
        .prepare_source_row_v1(&key, None, locator)
        .unwrap();
    assert_eq!(row.last_changed_sequence(), commit.commit_sequence());
    drop(row);
    drop(key);
    drop(foreign_key);
    drop(other);
    drop(evaluated);
    assert_source_row_retired(foreign_before, foreign.observation().unwrap());
    assert_source_row_retired(before, budget.observation().unwrap());
}

fn assert_source_row_retired(
    before: riffdb_types::exact_provider_budget_v1::BudgetObservation,
    after: riffdb_types::exact_provider_budget_v1::BudgetObservation,
) {
    // Live ownership is fully retired; cumulative release history must advance.
    assert!(after.released_allocations > before.released_allocations);
    assert_eq!(
        after,
        riffdb_types::exact_provider_budget_v1::BudgetObservation {
            released_allocations: after.released_allocations,
            ..before
        }
    );
}
