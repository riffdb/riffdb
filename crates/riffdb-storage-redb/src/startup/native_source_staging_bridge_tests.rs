//! Structural bridge proofs. The genuine V3 allocator/native-command-prefix
//! authority and final source candidate are independent pending integrations.
// req: STO-020, PRJ-004, END-008
use super::*;
use crate::{
    maintenance::{
        SourceOwnedStagingBackend, SourceStageEvaluationError, SourceStageMap, SourceStagingDiskJob,
    },
    source_evaluation_work::SourceEvaluationWorkOwner,
};
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
    PartitionKeyBuilder, ProjectionCheckpointOrdinal, ProjectionGeneration, ProjectionIdentity,
    ProjectionSourceLocatorV1, ProjectionSourceWorkingBudgetV1, ProvenanceId, RequestId,
    TenantScope, Timestamp, hash_partition_key,
};
use std::sync::{Arc, atomic::AtomicBool};

fn structural_expectation(
    prefix: &crate::store::OriginalPrefixRetentionLease,
) -> ProjectionSourceLocatorV1 {
    let lineage = prefix.captured_history().lineage();
    ProjectionSourceLocatorV1::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        ProjectionCheckpointOrdinal::first(),
    )
    .unwrap()
}

#[test]
fn source_bridge_count_sum_filter_and_empty_commands_match_ordinary_rows_and_progress() {
    for filtered in [false, true] {
        let (_directory, ports) = super::native_source_staging_slot::fixture();
        let prefix = ports.capture_original_prefix_retention().unwrap();
        assert_eq!(prefix.captured_through().get(), 2);
        let budget = prefix.originating_budget().clone();
        let fixture = evaluator_fixture();
        let capture = fixture
            .active
            .capture_projection_source_admitted_v1(fixture.schema.identity(), &budget)
            .unwrap();
        let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
        let scope = crate::test_path::ScopedDirectory::new("source-bridge-parity");
        let parent = scope.join("parent");
        std::fs::create_dir(&parent).unwrap();
        let (captured, slot) = work.split_for_stage();
        let locator = structural_expectation(captured);
        let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
        let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
        let mut map =
            SourceStageMap::new_structural(owned, capture.schema(), captured, locator).unwrap();
        let first = evaluator_commit(
            &fixture,
            CommitSequence::first(),
            &[(10, 4), (10, -1), (20, 99)],
        );
        let second_values: &[(i64, i64)] = if filtered { &[(20, 99)] } else { &[] };
        let second = evaluator_commit(&fixture, CommitSequence::new(2).unwrap(), second_values);
        map.evaluate_and_apply(&capture, &first).unwrap();
        map.evaluate_and_apply(&capture, &second).unwrap();
        let ordinary = riffdb_projection::evaluate_projection_commit(
            capture.plan(),
            capture.schema().clone(),
            ProjectionGeneration::first(),
            &first,
        )
        .unwrap();
        assert_eq!(ordinary.changed_group_count(), 1);
        let mut frozen = map.freeze().unwrap();
        assert_eq!(frozen.rows(), 1);
        let mut cursor = frozen.cursor().unwrap();
        let staged = cursor.next().unwrap().unwrap();
        assert_eq!(staged.row().last_changed_sequence().get(), 1);
        for ((_, value), (_, count)) in staged
            .row()
            .measures()
            .fields()
            .iter()
            .zip(&fixture.measure_fields)
        {
            assert_eq!(
                value,
                &if *count {
                    CanonicalValue::U64(2)
                } else {
                    CanonicalValue::I64(3)
                }
            );
        }
        assert!(cursor.next().unwrap().is_none());
        drop(staged);
        drop(cursor);
        drop(frozen);
        drop(job);
        assert!(!parent.join("source-staging-v1").exists());
    }
}

#[test]
fn source_bridge_chunks_129_groups_without_advancing_partial_command_progress() {
    let (_directory, ports) = super::native_source_staging_slot::fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let fixture =
        evaluator_fixture_from_source(&EVALUATOR_CONTRACT.replace("    where group == 10\n", ""));
    let capture = fixture
        .active
        .capture_projection_source_admitted_v1(fixture.schema.identity(), &budget)
        .unwrap();
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let scope = crate::test_path::ScopedDirectory::new("source-bridge-chunks");
    let parent = scope.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let (captured, slot) = work.split_for_stage();
    let locator = structural_expectation(captured);
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let mut map =
        SourceStageMap::new_structural(owned, capture.schema(), captured, locator).unwrap();
    let values = (0..129).map(|value| (value, value)).collect::<Vec<_>>();
    let first = evaluator_commit(&fixture, CommitSequence::first(), &values);
    map.evaluate_and_apply(&capture, &first).unwrap();
    assert_eq!(
        map.partial_finishes_for_test(),
        1,
        "exactly one actual bounded transaction continuation"
    );
    let empty = evaluator_commit(&fixture, CommitSequence::new(2).unwrap(), &[]);
    map.evaluate_and_apply(&capture, &empty).unwrap();
    let mut frozen = map.freeze().unwrap();
    assert_eq!(frozen.rows(), 129);
    let mut cursor = frozen.cursor().unwrap();
    let mut count = 0;
    while let Some(staged) = cursor.next().unwrap() {
        assert_eq!(staged.row().last_changed_sequence().get(), 1);
        let CanonicalValue::I64(group) = &staged.row().group_values()[0] else {
            panic!("i64 group")
        };
        for ((_, value), (_, is_count)) in staged
            .row()
            .measures()
            .fields()
            .iter()
            .zip(&fixture.measure_fields)
        {
            assert_eq!(
                value,
                &if *is_count {
                    CanonicalValue::U64(1)
                } else {
                    CanonicalValue::I64(*group)
                }
            );
        }
        count += 1;
    }
    assert_eq!(count, 129);
    drop(cursor);
    drop(frozen);
    drop(job);
}

#[test]
fn source_bridge_preserves_complete_command_overflow_association() {
    let (_directory, ports) = super::native_source_staging_slot::fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let fixture = evaluator_fixture();
    let capture = fixture
        .active
        .capture_projection_source_admitted_v1(fixture.schema.identity(), &budget)
        .unwrap();
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let scope = crate::test_path::ScopedDirectory::new("source-bridge-overflow-association");
    let parent = scope.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let (captured, slot) = work.split_for_stage();
    let locator = structural_expectation(captured);
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let mut map =
        SourceStageMap::new_structural(owned, capture.schema(), captured, locator).unwrap();
    map.evaluate_and_apply(
        &capture,
        &evaluator_commit(&fixture, CommitSequence::first(), &[(10, i64::MAX)]),
    )
    .unwrap();
    map.evaluate_and_apply(
        &capture,
        &evaluator_commit(
            &fixture,
            CommitSequence::new(2).unwrap(),
            &[(10, i64::MAX), (10, -i64::MAX)],
        ),
    )
    .unwrap();
    let mut frozen = map.freeze().unwrap();
    let mut cursor = frozen.cursor().unwrap();
    let row = cursor.next().unwrap().unwrap();
    for ((_, value), (_, count)) in row
        .row()
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
                CanonicalValue::I64(i64::MAX)
            }
        );
    }
    drop(row);
    drop(cursor);
    drop(frozen);
    drop(job);
}

#[test]
fn source_bridge_foreign_capture_and_source_pressure_retire_without_progress_or_leaks() {
    for pressure in [false, true] {
        let (_directory, ports) = super::native_source_staging_slot::fixture();
        let prefix = ports.capture_original_prefix_retention().unwrap();
        let budget = prefix.originating_budget().clone();
        let fixture = evaluator_fixture();
        let capture = fixture
            .active
            .capture_projection_source_admitted_v1(fixture.schema.identity(), &budget)
            .unwrap();
        let foreign_budget = ProjectionSourceWorkingBudgetV1::new();
        let foreign = fixture
            .active
            .capture_projection_source_admitted_v1(fixture.schema.identity(), &foreign_budget)
            .unwrap();
        let before = budget.observation().unwrap();
        let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
        let scope = crate::test_path::ScopedDirectory::new("source-bridge-refusal");
        let parent = scope.join("parent");
        std::fs::create_dir(&parent).unwrap();
        let (captured, slot) = work.split_for_stage();
        let locator = structural_expectation(captured);
        let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
        let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
        let mut map =
            SourceStageMap::new_structural(owned, capture.schema(), captured, locator).unwrap();
        let command = evaluator_commit(&fixture, CommitSequence::first(), &[(10, 1)]);
        let now = budget.observation().unwrap();
        let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
            - now.fixed_owner_bytes
            - now.owned_bytes
            - now.reserved_bytes;
        let occupied = pressure.then(|| budget.reserve(free).unwrap());
        let error = map
            .evaluate_and_apply(if pressure { &capture } else { &foreign }, &command)
            .unwrap_err();
        match error {
            SourceStageEvaluationError::Evaluation(error) => {
                assert!(pressure);
                assert_eq!(
                    error.kind(),
                    riffdb_projection::ProjectionEvaluationErrorKind::StorageUnavailable
                );
            }
            SourceStageEvaluationError::Storage(error) => {
                assert!(!pressure);
                assert_eq!(error.kind(), StorageErrorKind::InvariantViolation);
            }
        }
        assert!(map.begin_update().is_err());
        assert!(map.freeze().is_err());
        drop(occupied);
        drop(job);
        assert_eq!(budget.observation().unwrap(), before);
        assert!(!parent.join("source-staging-v1").exists());
    }
}

#[test]
fn source_bridge_checked_arithmetic_failure_retires_without_candidate_progress() {
    let (_directory, ports) = super::native_source_staging_slot::fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let fixture = evaluator_fixture();
    let capture = fixture
        .active
        .capture_projection_source_admitted_v1(fixture.schema.identity(), &budget)
        .unwrap();
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let scope = crate::test_path::ScopedDirectory::new("source-bridge-arithmetic-failure");
    let parent = scope.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let (captured, slot) = work.split_for_stage();
    let locator = structural_expectation(captured);
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let mut map =
        SourceStageMap::new_structural(owned, capture.schema(), captured, locator).unwrap();
    map.evaluate_and_apply(
        &capture,
        &evaluator_commit(&fixture, CommitSequence::first(), &[(10, i64::MAX)]),
    )
    .unwrap();
    let error = map
        .evaluate_and_apply(
            &capture,
            &evaluator_commit(&fixture, CommitSequence::new(2).unwrap(), &[(10, 1)]),
        )
        .unwrap_err();
    assert!(
        matches!(error, SourceStageEvaluationError::Evaluation(error)
        if error.kind() == riffdb_projection::ProjectionEvaluationErrorKind::ArithmeticOverflow)
    );
    assert!(map.freeze().is_err());
    drop(job);
    assert!(!parent.join("source-staging-v1").exists());
}

#[test]
fn source_bridge_real_overflow_after_committed_partial_chunk_retires_and_releases() {
    let (_directory, ports) = super::native_source_staging_slot::fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let fixture =
        evaluator_fixture_from_source(&EVALUATOR_CONTRACT.replace("    where group == 10\n", ""));
    let capture = fixture
        .active
        .capture_projection_source_admitted_v1(fixture.schema.identity(), &budget)
        .unwrap();
    let before = budget.observation().unwrap();
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let scope = crate::test_path::ScopedDirectory::new("source-bridge-real-partial-overflow");
    let parent = scope.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let (captured, slot) = work.split_for_stage();
    let locator = structural_expectation(captured);
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let mut map =
        SourceStageMap::new_structural(owned, capture.schema(), captured, locator).unwrap();
    // Select the final actual native BTree key, not an assumed numeric/string
    // ordering. The last replacement really overflows after the128-row commit.
    let last = (0..130)
        .max_by_key(|group| {
            fixture
                .schema
                .group_key(
                    ProjectionGeneration::first(),
                    &[CanonicalValue::I64(*group)],
                )
                .unwrap()
                .as_bytes()
                .to_vec()
        })
        .unwrap();
    let first_values = (0..130)
        .map(|group| (group, if group == last { i64::MAX } else { 0 }))
        .collect::<Vec<_>>();
    map.evaluate_and_apply(
        &capture,
        &evaluator_commit(&fixture, CommitSequence::first(), &first_values),
    )
    .unwrap();
    let prior_partials = map.partial_finishes_for_test();
    assert_eq!(prior_partials, 1);
    let second_values = (0..130).map(|group| (group, 1)).collect::<Vec<_>>();
    let error = map
        .evaluate_and_apply(
            &capture,
            &evaluator_commit(&fixture, CommitSequence::new(2).unwrap(), &second_values),
        )
        .unwrap_err();
    assert!(
        matches!(error, SourceStageEvaluationError::Evaluation(error)
        if error.kind() == riffdb_projection::ProjectionEvaluationErrorKind::ArithmeticOverflow)
    );
    assert_eq!(
        map.partial_finishes_for_test(),
        prior_partials + 1,
        "the second command committed a real128-row partial chunk before failure"
    );
    assert_eq!(
        map.applied_for_test(),
        Some(CommitSequence::first()),
        "partial commit grants no command progress"
    );
    assert!(map.begin_update().is_err());
    assert!(map.freeze().is_err());
    drop(job);
    let after = budget.observation().unwrap();
    // Historical release counts increase when real row graphs are destroyed.
    // Compare every live charge/capability explicitly, without erasing history.
    assert_eq!(
        (
            after.fixed_owner_bytes,
            after.reserved_bytes,
            after.owned_bytes,
            after.reservations,
            after.allocations,
            after.active_owners,
            after.retiring_owners,
            after.pinned_owners,
            after.pins
        ),
        (
            before.fixed_owner_bytes,
            before.reserved_bytes,
            before.owned_bytes,
            before.reservations,
            before.allocations,
            before.active_owners,
            before.retiring_owners,
            before.pinned_owners,
            before.pins
        ),
    );
    assert_eq!(after.released_owners, before.released_owners);
    assert!(
        after.released_allocations > before.released_allocations,
        "the real row graphs were physically released after partial-commit failure"
    );
    assert!(!parent.join("source-staging-v1").exists());
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
    active: ActiveCatalogSnapshot,
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
            .map(|field| field.id())
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
        active,
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
