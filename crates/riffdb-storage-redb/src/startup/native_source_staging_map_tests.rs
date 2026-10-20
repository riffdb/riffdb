//! Disposable map proofs only: no evaluator, assigned locator or publication.
// req: STO-020, PRJ-004, END-008
use super::*;
use crate::{
    maintenance::{SourceOwnedStagingBackend, SourceStageMap, SourceStagingDiskJob},
    source_evaluation_work::SourceEvaluationWorkOwner,
};
use riffdb_storage_api::{CheckedProjectionSchema, ProjectionSourceRowV1};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, FieldId, ProjectionCheckpointOrdinal, ProjectionGeneration,
    ProjectionId, ProjectionSourceLocatorV1, ProjectionSourceWorkAllocationV1,
    ProjectionSourceWorkingBudgetV1,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

const MAP_WORKING_BYTES: u64 = 40 * 1024 * 1024;

fn stage_schema() -> CheckedProjectionSchema {
    let bundle = riffdb_contract_compiler::compile_contract_source(
        "contract Stage version 1 { event Source { group: string<128> } projection Totals { source event Source key (group) measure total = count() frontier transactionally_ordered } }",
    ).unwrap();
    CheckedProjectionSchema::new(
        bundle
            .bound_projection_group_schema(ProjectionId::first())
            .unwrap(),
    )
}

fn structural_expectation(
    prefix: &crate::store::OriginalPrefixRetentionLease,
) -> ProjectionSourceLocatorV1 {
    let lineage = prefix.captured_history().lineage();
    // Test-only syntax tied to a real pinned DB/incarnation. It is expressly
    // neither an allocator observation nor an assigned source/candidate.
    ProjectionSourceLocatorV1::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        ProjectionCheckpointOrdinal::first(),
    )
    .unwrap()
}

fn row(
    budget: &ProjectionSourceWorkingBudgetV1,
    schema: &CheckedProjectionSchema,
    locator: ProjectionSourceLocatorV1,
    group: &str,
    sequence: u64,
    count: u64,
) -> ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1> {
    let values = [CanonicalValue::string(group).unwrap()];
    let measures =
        CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(count))]).unwrap();
    let key = schema
        .group_key(ProjectionGeneration::first(), &values)
        .unwrap();
    let scratch = ProjectionSourceRowV1::construction_work_bound_v1(&key, &measures).unwrap();
    let _scratch = budget.reserve(scratch).unwrap();
    let row = ProjectionSourceRowV1::new(
        schema,
        locator,
        CommitSequence::new(2).unwrap(),
        key,
        measures,
        CommitSequence::new(sequence).unwrap(),
    )
    .unwrap();
    let work = row.clone_work_bound_v1().unwrap() + 4 * 1024 * 1024 + 128 * 1024;
    budget
        .reserve(work)
        .unwrap()
        .settle_shared_value(row)
        .unwrap()
}

#[test]
fn original_map_whole_redb_admission_refuses_before_header_write_and_releases_custody() {
    let (_directory, ports) = super::native_source_staging_slot::fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let baseline = budget.observation().unwrap();
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let scope = crate::test_path::ScopedDirectory::new("source-map-memory-refusal");
    let parent = scope.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let (captured, slot) = work.split_for_stage();
    let locator = structural_expectation(captured);
    let schema = stage_schema();
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let file = std::fs::File::open(parent.join("source-staging-v1/map.redb")).unwrap();
    let now = budget.observation().unwrap();
    let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - now.fixed_owner_bytes
        - now.owned_bytes
        - now.reserved_bytes;
    let occupied = budget.reserve(free - (MAP_WORKING_BYTES - 1)).unwrap();
    let result = SourceStageMap::new_structural(owned, &schema, captured, locator);
    assert!(matches!(&result, Err(error) if error.kind() == StorageErrorKind::Unavailable));
    drop(result);
    use std::os::unix::fs::FileExt;
    let mut header = [1; 4096];
    file.read_exact_at(&mut header, 0).unwrap();
    assert_eq!(
        header, [0; 4096],
        "redb must not allocate/write before admission"
    );
    drop(file); // Keep the disk-job claim until this private probe is gone.
    assert!(!parent.join("source-staging-v1").exists());
    drop(occupied);
    drop(job);
    assert_eq!(budget.observation().unwrap(), baseline);
    assert!(budget.try_acquire_worker().is_err());
}

#[test]
fn original_map_chunk_progress_current_replace_and_canonical_cursor_preserve_exact_context() {
    let (_directory, ports) = super::native_source_staging_slot::fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let scope = crate::test_path::ScopedDirectory::new("source-map-canonical-progress");
    let parent = scope.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let (captured, slot) = work.split_for_stage();
    let locator = structural_expectation(captured);
    let schema = stage_schema();
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let before = budget.observation().unwrap().reserved_bytes;
    let mut map = SourceStageMap::new_structural(owned, &schema, captured, locator).unwrap();
    assert!(budget.observation().unwrap().reserved_bytes >= before + MAP_WORKING_BYTES);
    let z = row(&budget, &schema, locator, "z", 1, 1);
    let aa = row(&budget, &schema, locator, "aa", 1, 2);
    let mut update = map.begin_update().unwrap();
    update.put(&z).unwrap();
    update.finish(None).unwrap();
    let mut update = map.begin_update().unwrap();
    update.put(&aa).unwrap();
    update.finish(Some(CommitSequence::first())).unwrap();
    let key = schema
        .group_key(ProjectionGeneration::first(), aa.group_values())
        .unwrap();
    let key = budget
        .reserve(4 * 1024 * 1024 + 128 * 1024)
        .unwrap()
        .settle_shared_value(key)
        .unwrap();
    let changed = row(&budget, &schema, locator, "aa", 2, 3);
    let z_key = schema
        .group_key(ProjectionGeneration::first(), z.group_values())
        .unwrap();
    let z_key = budget
        .reserve(4 * 1024 * 1024 + 128 * 1024)
        .unwrap()
        .settle_shared_value(z_key)
        .unwrap();
    let mut update = map.begin_update().unwrap();
    let prior = update.current(&key).unwrap().unwrap();
    assert_eq!(prior.value().measures(), aa.measures());
    drop(prior);
    update.put(&changed).unwrap();
    update.remove(&z_key).unwrap();
    assert!(update.current(&z_key).unwrap().is_none());
    update.put(&z).unwrap();
    update
        .finish(Some(CommitSequence::new(2).unwrap()))
        .unwrap();
    let mut frozen = map.freeze().unwrap();
    assert_eq!(frozen.rows(), 2);
    let mut cursor = frozen.cursor().unwrap();
    let first = cursor.next().unwrap().unwrap();
    let second = cursor.next().unwrap().unwrap();
    assert_eq!(first.row().group_values(), z.group_values());
    assert_eq!(first.row().measures(), z.measures());
    assert_eq!(first.row().locator(), locator);
    assert_eq!(first.row().last_changed_sequence().get(), 1);
    assert_eq!(second.row().group_values(), changed.group_values());
    assert_eq!(second.row().measures(), changed.measures());
    assert_eq!(second.row().last_changed_sequence().get(), 2);
    assert!(first.order_key().as_bytes() < second.order_key().as_bytes());
    assert!(cursor.next().unwrap().is_none());
    let expected_bytes = first.order_key().as_bytes().len() as u64
        + first.encoded_content_charge().get() as u64
        + second.order_key().as_bytes().len() as u64
        + second.encoded_content_charge().get() as u64;
    drop(cursor);
    assert_eq!(frozen.encoded_bytes(), expected_bytes);
    // The source-owned decoded row remains safe after all native/map custody.
    drop(frozen);
    assert!(!parent.join("source-staging-v1").exists());
    assert_eq!(first.row().locator(), locator);
    drop(first);
    drop(second);
    drop(job);
    assert!(budget.try_acquire_worker().is_err());
}

#[test]
fn original_map_unwind_and_incomplete_progress_release_whole_reservation_after_native_drop() {
    let (_directory, ports) = super::native_source_staging_slot::fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let baseline = budget.observation().unwrap();
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let scope = crate::test_path::ScopedDirectory::new("source-map-unwind");
    let parent = scope.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let schema = stage_schema();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (captured, slot) = work.split_for_stage();
        let locator = structural_expectation(captured);
        let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
        let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
        let _map = SourceStageMap::new_structural(owned, &schema, captured, locator).unwrap();
        assert!(
            budget.observation().unwrap().reserved_bytes
                >= baseline.reserved_bytes + MAP_WORKING_BYTES
        );
        panic!("unwind a real admitted private redb map");
    }));
    assert!(result.is_err());
    assert!(!parent.join("source-staging-v1").exists());
    assert_eq!(budget.observation().unwrap(), baseline);
    let (captured, slot) = work.split_for_stage();
    let locator = structural_expectation(captured);
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let mut map = SourceStageMap::new_structural(owned, &schema, captured, locator).unwrap();
    map.begin_update().unwrap().finish(None).unwrap();
    assert!(
        matches!(map.freeze(), Err(error) if error.kind() == StorageErrorKind::InvariantViolation)
    );
    drop(job);
    assert_eq!(budget.observation().unwrap(), baseline);
    assert!(!parent.join("source-staging-v1").exists());
}

#[test]
fn original_map_exact_capture_binding_refuses_another_engine_before_whole_memory_admission() {
    let (_first_directory, first_ports) = super::native_source_staging_slot::fixture();
    let first = first_ports.capture_original_prefix_retention().unwrap();
    let budget = first.originating_budget().clone();
    let baseline = budget.observation().unwrap();
    let (_second_directory, second_ports) = super::native_source_staging_slot::fixture();
    let second = second_ports.capture_original_prefix_retention().unwrap();
    let mut work = SourceEvaluationWorkOwner::new(first, Arc::new(AtomicBool::new(false)));
    let scope = crate::test_path::ScopedDirectory::new("source-map-capture-mismatch");
    let parent = scope.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let schema = stage_schema();
    let (_, slot) = work.split_for_stage();
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let result =
        SourceStageMap::new_structural(owned, &schema, &second, structural_expectation(&second));
    assert!(matches!(&result, Err(error) if error.kind() == StorageErrorKind::InvariantViolation));
    drop(result);
    drop(job);
    assert_eq!(budget.observation().unwrap(), baseline);
    assert!(!parent.join("source-staging-v1").exists());
}

#[test]
fn original_map_step_refusal_cancellation_and_foreign_input_retire_private_database() {
    let (_directory, ports) = super::native_source_staging_slot::fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let cancellation = Arc::new(AtomicBool::new(false));
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::clone(&cancellation));
    let scope = crate::test_path::ScopedDirectory::new("source-map-step-refusal");
    let parent = scope.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let (captured, slot) = work.split_for_stage();
    let locator = structural_expectation(captured);
    let schema = stage_schema();
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let mut map = SourceStageMap::new_structural(owned, &schema, captured, locator).unwrap();
    let mut update = map.begin_update().unwrap();
    for index in 0..128 {
        update
            .put(&row(&budget, &schema, locator, &index.to_string(), 1, 1))
            .unwrap();
    }
    let refused = row(&budget, &schema, locator, "overflow", 1, 1);
    assert!(
        matches!(update.put(&refused), Err(error) if error.kind() == StorageErrorKind::LimitExceeded)
    );
    assert!(update.finish(Some(CommitSequence::first())).is_err());
    assert!(map.begin_update().is_err());
    assert!(map.freeze().is_err());
    drop(job);
    assert!(!parent.join("source-staging-v1").exists());
    let (captured, slot) = work.split_for_stage();
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let mut map = SourceStageMap::new_structural(owned, &schema, captured, locator).unwrap();
    let foreign_budget = ProjectionSourceWorkingBudgetV1::new();
    let foreign_schema = stage_schema();
    let foreign = row(&foreign_budget, &foreign_schema, locator, "foreign", 1, 1);
    let mut update = map.begin_update().unwrap();
    assert!(
        matches!(update.put(&foreign), Err(error) if error.kind() == StorageErrorKind::InvariantViolation)
    );
    drop(update);
    assert!(map.freeze().is_err());
    drop(job);
    let (captured, slot) = work.split_for_stage();
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    let owned = SourceOwnedStagingBackend::new(job.prepare(&parent).unwrap()).unwrap();
    let mut map = SourceStageMap::new_structural(owned, &schema, captured, locator).unwrap();
    cancellation.store(true, Ordering::Release);
    assert!(
        matches!(map.begin_update(), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert!(map.freeze().is_err());
    drop(job);
    assert!(!parent.join("source-staging-v1").exists());
}
