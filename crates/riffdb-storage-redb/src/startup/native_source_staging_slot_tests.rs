//! Real Original capture plus one sealed staging-slot integration.
// req: STO-020, PRJ-004, END-008
use super::*;
use crate::{
    maintenance::{SourceOwnedStagingBackend, SourceStagingDiskJob},
    source_evaluation_work::SourceEvaluationWorkOwner,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub(super) fn fixture() -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
) {
    let root = crate::test_path::ScopedDirectory::new("source-staging-fixture");
    let mut store = RedbStore::open(root.join("db.redb")).unwrap();
    let id =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap();
    store.initialize_projection_source_fixture(id).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("genuine same-owner Fresh must validate before readiness");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    super::native_source_command_fixture::activate(&mut ports).unwrap();
    super::native_source_command_fixture::append_journaled(&ports, 1).unwrap();
    super::native_source_command_fixture::append_journaled(&ports, 2).unwrap();
    (root, ports)
}

#[test]
fn original_work_staging_slot_reuses_claim_and_keeps_prefix_readable_through_physical_drop() {
    let (_directory, ports) = fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let before = budget.observation().unwrap();
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let (captured, slot) = work.split_for_stage();
    let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
    assert_eq!(captured.captured_through().get(), 2);
    assert!(budget.try_acquire_worker().is_err());
    let parent_scope = crate::test_path::ScopedDirectory::new("source-slot-real-disk");
    let parent = parent_scope.join("owned-parent");
    std::fs::create_dir(&parent).unwrap();
    let disk = job.prepare(&parent).unwrap();
    let backend = SourceOwnedStagingBackend::new(disk).unwrap();
    assert_eq!(backend.len().unwrap(), 0);
    backend.set_len(8192).unwrap();
    backend.write(7, &[37, 38]).unwrap();
    let mut bytes = [0; 2];
    backend.read(7, &mut bytes).unwrap();
    assert_eq!(bytes, [37, 38]);
    backend.sync_data().unwrap();
    let (admitted, allocated) = backend.observation().unwrap();
    assert!(allocated <= admitted);
    assert_eq!(captured.captured_through().get(), 2);
    assert!(budget.observation().unwrap().reserved_bytes > before.reserved_bytes);
    backend.close().unwrap();
    assert!(parent.join("source-staging-v1").exists());
    drop(backend);
    assert!(!parent.join("source-staging-v1").exists());
    drop(job);
    assert_eq!(budget.observation().unwrap(), before);
    assert!(budget.try_acquire_worker().is_err());
    // Only after the whole previous backend/custody lifetime ends can the
    // exclusive slot be lent again; immutable capture ownership stays live.
    let (captured, slot) = work.split_for_stage();
    let second = SourceStagingDiskJob::from_slot(slot).unwrap();
    assert_eq!(captured.captured_through().get(), 2);
    drop(second);
    drop(work);
    assert!(budget.try_acquire_worker().is_ok());
}

#[test]
fn original_work_staging_slot_constructor_refusal_keeps_original_worker_and_control() {
    let (_directory, ports) = fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let cancellation = Arc::new(AtomicBool::new(false));
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::clone(&cancellation));
    let before = budget.observation().unwrap();
    let available = riffdb_types::exact_provider_budget_v1::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.reserved_bytes
        - before.owned_bytes;
    let occupied = budget.reserve(available).unwrap();
    let (captured, slot) = work.split_for_stage();
    assert_eq!(captured.captured_through().get(), 2);
    assert!(
        matches!(SourceStagingDiskJob::from_slot(slot), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert!(budget.try_acquire_worker().is_err());
    drop(occupied);
    assert_eq!(budget.observation().unwrap(), before);
    cancellation.store(true, Ordering::Release);
    let (captured, slot) = work.split_for_stage();
    assert_eq!(captured.captured_through().get(), 2);
    assert!(
        matches!(SourceStagingDiskJob::from_slot(slot), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), before);
    assert!(budget.try_acquire_worker().is_err());
    drop(work);
    assert!(budget.try_acquire_worker().is_ok());
}

#[test]
fn original_work_staging_slot_panic_destroys_backend_and_disk_before_releasing_slot() {
    let (_directory, ports) = fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget().clone();
    let before = budget.observation().unwrap();
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let parent_scope = crate::test_path::ScopedDirectory::new("source-slot-panic-disk");
    let parent = parent_scope.join("owned-parent");
    std::fs::create_dir(&parent).unwrap();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (captured, slot) = work.split_for_stage();
        let mut job = SourceStagingDiskJob::from_slot(slot).unwrap();
        let disk = job.prepare(&parent).unwrap();
        let backend = SourceOwnedStagingBackend::new(disk).unwrap();
        backend.set_len(4096).unwrap();
        assert_eq!(captured.captured_through().get(), 2);
        panic!("unwind actual private staging ownership");
    }));
    assert!(unwind.is_err());
    assert!(!parent.join("source-staging-v1").exists());
    assert_eq!(budget.observation().unwrap(), before);
    assert!(budget.try_acquire_worker().is_err());
    let (_, slot) = work.split_for_stage();
    drop(SourceStagingDiskJob::from_slot(slot).unwrap());
    drop(work);
    assert!(budget.try_acquire_worker().is_ok());
}
