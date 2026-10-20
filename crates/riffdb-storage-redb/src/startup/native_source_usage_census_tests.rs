//! Real native usage reads/publication hooks, not Source publication authority.
// req: STO-012, STO-020, PRJ-004, REC-001
use crate::source_usage_domain::SourceUsageCensus;
use redb::{ReadableDatabase, ReadableTable};
use riffdb_storage_api::{
    ChangelogAttributionV3, DatabaseInitializationResult, ProjectionCheckpointOrdinalAllocatorV1,
    ProjectionCheckpointOrdinalPositionV1, StorageError, StorageErrorKind,
    proto_codec::encode_projection_checkpoint_ordinal_allocator_v1,
};
use riffdb_types::{DatabaseId, ProjectionCheckpointOrdinal};
use std::sync::Arc;

fn fresh(
    controller: Option<crate::RedbTestController>,
) -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
) {
    let directory = crate::test_path::ScopedDirectory::new("source-usage-census");
    let path = directory.join("database.redb");
    let mut store = match controller {
        Some(controller) => crate::RedbStore::open_with_test_controller(&path, controller).unwrap(),
        None => crate::RedbStore::open(&path).unwrap(),
    };
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap();
    assert!(
        matches!(store.initialize_projection_source_fixture(database).unwrap(),DatabaseInitializationResult::Installed(id) if id==database)
    );
    // Component fixture only: no completion/readiness/worker authority claim.
    let ports = crate::RedbOperationalPorts {
        shared: Arc::clone(&store.shared),
    };
    (directory, ports)
}
fn fresh_operational() -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
) {
    use riffdb_storage_api::{
        StructuralEvidenceOpen, StructuralEvidenceSession, StructuralOpenOutcome,
    };
    let directory = crate::test_path::ScopedDirectory::new("source-usage-capture-handoff");
    let mut store = crate::RedbStore::open(directory.join("database.redb")).unwrap();
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap();
    store
        .initialize_projection_source_fixture(database)
        .unwrap();
    let mut session = store.begin_structural_evidence(super::inputs()).unwrap();
    let structural = super::finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("same-owner Fresh fixture must complete genuine startup");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    super::native_source_command_fixture::activate(&mut ports).unwrap();
    super::native_source_command_fixture::append_journaled(&ports, 1).unwrap();
    (directory, ports)
}
// The component-only Fresh fixture intentionally leaves native locator work
// Pending. Commands need the same real builder completion as ordinary startup;
// this does not construct a Source worker, warm Source state, or fabricate absence.
fn join_actual_native_command_prerequisite(ports: &crate::RedbOperationalPorts) {
    let access = ports.begin_composite_read().unwrap();
    assert!(access.locator_capability().is_pending());
    assert_eq!(
        access
            .locator_capability()
            .require_complete()
            .err()
            .unwrap()
            .kind(),
        StorageErrorKind::Unavailable
    );
    crate::locator_builder::start_locator_builder(access.clone(), Some(Arc::clone(&ports.shared)))
        .unwrap();
    access
        .locator_capability()
        .wait_for_builder_for_test()
        .unwrap();
    access.locator_capability().require_complete().unwrap();
    ports
        .begin_composite_read()
        .unwrap()
        .locator_capability()
        .require_complete()
        .unwrap();
}
fn actual_allocator_bytes(ports: &crate::RedbOperationalPorts) -> u64 {
    let read = ports.shared.database.begin_read().unwrap();
    let meta = read.open_table(crate::layout::META).unwrap();
    let key = "projection_checkpoint_ordinal/v1";
    let value = meta.get(key).unwrap().unwrap();
    (key.len() + value.value().len()) as u64
}
fn finish(mut census: SourceUsageCensus<'_>) -> riffdb_storage_api::ProjectionSourceTableUsageV1 {
    for _ in 0..32 {
        let step = census.step().unwrap();
        assert!(step.rows <= 128 && step.bytes <= 4 * 1024 * 1024);
        if step.done {
            return census.finish().unwrap().usage();
        }
    }
    panic!("empty physical Source census must finish within its finite phase bound")
}
fn replace_allocator(ports: &crate::RedbOperationalPorts, next: u64) -> Result<(), StorageError> {
    let _lease = ports.shared.acquire_source_usage_fixture_lease()?;
    let value = ProjectionCheckpointOrdinalAllocatorV1::new(
        1,
        ProjectionCheckpointOrdinalPositionV1::Next(
            ProjectionCheckpointOrdinal::new(next)
                .ok_or_else(|| StorageError::new(StorageErrorKind::InvariantViolation, None))?,
        ),
    )
    .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))?;
    let encoded = encode_projection_checkpoint_ordinal_allocator_v1(&value)
        .map_err(crate::error::codec_error)?;
    // Structural exact native mutation only, not coordinator publication proof.
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        crate::RedbCommitProfile::Hardened,
        ChangelogAttributionV3::ProjectionControl,
    )?;
    write
        .open_table(crate::layout::META)?
        .insert("projection_checkpoint_ordinal/v1", encoded.as_bytes())
        .map_err(crate::error::precommit_storage_error)?;
    write.finish()?.commit(&ports.shared)
}

#[test]
fn source_usage_census_reads_real_fresh_allocator_charge_and_exact_engine() {
    let (_directory, ports) = fresh(None);
    let bytes = actual_allocator_bytes(&ports);
    assert!(bytes > 0);
    let usage = finish(SourceUsageCensus::start(&ports).unwrap());
    assert_eq!(usage.total_bytes(), bytes);
    assert_eq!(usage.selected_bytes(), 0);
    assert_eq!(usage.candidate_reserved_bytes(), None);
    assert_ne!(
        usage,
        riffdb_storage_api::ProjectionSourceTableUsageV1::empty()
    );
    assert_eq!(
        ports
            .shared
            .database
            .source_usage_domain()
            .complete_observation()
            .unwrap(),
        usage
    );
    let (_foreign_directory, foreign) = fresh(None);
    let foreign_domain = foreign.shared.database.source_usage_domain();
    assert!(!std::ptr::eq(
        foreign_domain,
        ports.shared.database.source_usage_domain()
    ));
    assert_eq!(
        finish(SourceUsageCensus::start(&foreign).unwrap()).total_bytes(),
        bytes
    );
}

#[test]
fn actual_source_allocator_mutation_invalidates_old_census_and_reconstructs_new_bytes() {
    let (_directory, ports) = fresh(None);
    let first = finish(SourceUsageCensus::start(&ports).unwrap());
    let mut stale = SourceUsageCensus::start(&ports).unwrap();
    assert!(!stale.step().unwrap().done);
    replace_allocator(&ports, 128).unwrap();
    assert_eq!(
        stale.step().unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert!(stale.finish().is_err());
    assert!(
        ports
            .shared
            .database
            .source_usage_domain()
            .complete_observation()
            .is_err()
    );
    let after = finish(SourceUsageCensus::start(&ports).unwrap());
    assert_eq!(after.total_bytes(), actual_allocator_bytes(&ports));
    assert!(
        after.total_bytes() > first.total_bytes(),
        "actual ordinal framing grew at the varint boundary"
    );
}

#[test]
fn source_usage_census_pressure_single_worker_and_physical_release() {
    let (_directory, ports) = fresh_operational();
    let budget = ports.shared.database.source_working_budget();
    let before = budget.observation().unwrap();
    let census = SourceUsageCensus::start(&ports).unwrap();
    assert!(
        matches!(SourceUsageCensus::start(&ports),Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    drop(census);
    let mut census = SourceUsageCensus::start(&ports).unwrap();
    for _ in 0..16 {
        if census.step().unwrap().done {
            break;
        }
    }
    let completion = census.finish().unwrap();
    assert!(
        matches!(SourceUsageCensus::start(&ports),Err(error) if error.kind()==StorageErrorKind::Unavailable),
        "completed numeric reconstruction retains the SAME worker phase"
    );
    let (_foreign_directory, foreign) = fresh(None);
    let gate = foreign.shared.acquire_source_usage_fixture_lease().unwrap();
    assert!(
        completion
            .into_worker_for_original_capture(&foreign, &gate)
            .is_err(),
        "equal database identity and totals cannot transfer the actual engine claim"
    );
    drop(gate);
    let mut census = SourceUsageCensus::start(&ports).unwrap();
    for _ in 0..16 {
        if census.step().unwrap().done {
            break;
        }
    }
    let completion = census.finish().unwrap();
    let prefix = ports
        .capture_original_prefix_from_usage(completion)
        .unwrap();
    assert!(prefix.belongs_to_ports(&ports));
    assert_eq!(
        prefix.captured_through(),
        riffdb_types::CommitSequence::first()
    );
    assert!(
        budget.try_acquire_worker().is_err(),
        "handoff retains the original claim without a reacquisition gap"
    );
    drop(prefix);
    let after = budget.observation().unwrap();
    assert_eq!(after.fixed_owner_bytes, before.fixed_owner_bytes);
    assert_eq!(after.owned_bytes, before.owned_bytes);
    assert_eq!(after.reserved_bytes, before.reserved_bytes);
    assert!(after.released_allocations >= before.released_allocations);
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - after.fixed_owner_bytes
        - after.owned_bytes
        - after.reserved_bytes;
    let pressure = budget.reserve(remaining).unwrap();
    assert!(
        matches!(SourceUsageCensus::start(&ports),Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    drop(pressure);
    finish(SourceUsageCensus::start(&ports).unwrap());
}

#[cfg(feature = "test-fixtures")]
#[test]
fn actual_source_publication_window_refuses_census_before_old_root_changes() {
    use crate::hooks::{RedbCheckpointTestPoint as Point, RedbCheckpointTestSchedule};
    struct Release {
        schedule: Option<Arc<RedbCheckpointTestSchedule>>,
        writer: Option<std::thread::JoinHandle<Result<(), StorageError>>>,
    }
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(schedule) = self.schedule.take() {
                schedule.release(Point::BeforeSourceUsagePublication);
            }
            if let Some(writer) = self.writer.take() {
                let _ = writer.join();
            }
        }
    }
    let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
    let schedule = Arc::new(schedule);
    let (_directory, ports) = fresh(Some(controller));
    let initial = finish(SourceUsageCensus::start(&ports).unwrap());
    let mut stale = SourceUsageCensus::start(&ports).unwrap();
    stale.step().unwrap();
    let mut release = Release {
        schedule: Some(schedule),
        writer: None,
    };
    release
        .schedule
        .as_ref()
        .unwrap()
        .arm(Point::BeforeSourceUsagePublication);
    let writer_ports = crate::RedbOperationalPorts {
        shared: Arc::clone(&ports.shared),
    };
    let writer_schedule = Arc::clone(release.schedule.as_ref().unwrap());
    release.writer = Some(std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            replace_allocator(&writer_ports, 128)
        }));
        if !matches!(&result, Ok(Ok(()))) {
            writer_schedule.release(Point::BeforeSourceUsagePublication);
        }
        match result {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }));
    let entered = release
        .schedule
        .as_ref()
        .unwrap()
        .wait_entered_or_released(Point::BeforeSourceUsagePublication);
    if !entered {
        release
            .writer
            .take()
            .unwrap()
            .join()
            .expect("allocator writer panicked before publication")
            .expect("allocator writer failed before publication");
        panic!("writer released the fixture without entering publication");
    }
    // This point is BEFORE transaction.commit: old physical bytes still exist,
    // but the exact actual guard/version already blocks both capture and CAS.
    assert_eq!(actual_allocator_bytes(&ports), initial.total_bytes());
    assert!(
        ports
            .shared
            .database
            .source_usage_domain()
            .capture_epoch()
            .is_err()
    );
    assert_eq!(
        stale.step().unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert!(
        ports
            .shared
            .database
            .source_usage_domain()
            .complete_observation()
            .is_err()
    );
    release
        .schedule
        .as_ref()
        .unwrap()
        .release(Point::BeforeSourceUsagePublication);
    release.writer.take().unwrap().join().unwrap().unwrap();
    drop(stale);
    assert!(
        ports
            .shared
            .database
            .source_usage_domain()
            .complete_observation()
            .is_err()
    );
    assert_eq!(
        finish(SourceUsageCensus::start(&ports).unwrap()).total_bytes(),
        actual_allocator_bytes(&ports)
    );
}

#[test]
fn ordinary_admitted_command_preserves_census_and_does_not_borrow_source_capacity() {
    let (_directory, ports) = fresh(None);
    join_actual_native_command_prerequisite(&ports);
    let initial = finish(SourceUsageCensus::start(&ports).unwrap());
    let census = SourceUsageCensus::start(&ports).unwrap();
    let budget = ports.shared.database.source_working_budget();
    let before = budget.observation().unwrap();
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.owned_bytes
        - before.reserved_bytes;
    let pressure = budget.reserve(remaining).unwrap();
    // Existing compiled-fixture records pass through the real admitted native
    // coordinator; actual runtime execution is a separate qualification.
    super::native_source_command_fixture::append(&ports, 1).unwrap();
    assert_eq!(
        ports
            .shared
            .database
            .source_usage_domain()
            .complete_observation()
            .unwrap(),
        initial
    );
    drop(pressure);
    assert_eq!(
        finish(census),
        initial,
        "the exact captured Source epoch survives unrelated actual command/history writes"
    );
}

#[test]
fn retained_original_source_inputs_preserve_pin_owner_affinity_and_control_budget() {
    let (_directory, ports) = fresh_operational();
    let (_foreign_directory, foreign) = fresh(None);
    let budget = ports.shared.database.source_working_budget();
    let baseline = budget.observation().unwrap();
    let lease = ports.capture_original_prefix_retention().unwrap();
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap();
    let expected_locator = riffdb_types::ProjectionSourceLocatorV1::new(
        database,
        1,
        ProjectionCheckpointOrdinal::first(),
    )
    .unwrap();
    assert_eq!(lease.tentative_source_locator(), Some(expected_locator));
    let retained: crate::store::RetainedOriginalSourceInputs =
        lease.retain_original_source_inputs().unwrap();
    assert_eq!(retained.captured_history(), lease.captured_history());
    assert_eq!(retained.captured_locator(), expected_locator);
    assert!(retained.matches_owner(&ports.shared.database));
    assert!(!retained.matches_owner(&foreign.shared.database));
    let incarnation = retained.captured_history().lineage().history_incarnation();
    drop(lease);
    assert!(
        budget.try_acquire_worker().is_ok(),
        "dropping OriginalPrefixRetentionLease releases the worker while RetainedOriginalSourceInputs keeps the pin"
    );
    assert_eq!(
        ports
            .shared
            .database
            .source_prefix_pins()
            .prepare_prune_in_incarnation(incarnation, 1)
            .err()
            .unwrap()
            .kind(),
        StorageErrorKind::HistoryPruned
    );
    drop(retained);
    let after = budget.observation().unwrap();
    assert_eq!(after.reserved_bytes, baseline.reserved_bytes);
    assert_eq!(after.owned_bytes, baseline.owned_bytes);
    assert!(
        ports
            .shared
            .database
            .source_prefix_pins()
            .prepare_prune_in_incarnation(incarnation, 1)
            .is_ok()
    );
}

#[path = "native_source_usage_census_seed.rs"]
mod seed;
