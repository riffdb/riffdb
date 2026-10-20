// req: STO-020, PRJ-004, END-008
use super::*;
use crate::{engine_owner::EngineOwner, locator_memory::LocatorAllocationClass};
use riffdb_types::exact_provider_budget_v1::PROJECTION_SOURCE_WORKING_MEMORY_BYTES;
use std::fs::OpenOptions;

fn file(scope: &crate::test_path::ScopedDirectory, name: &str) -> File {
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(scope.join(name))
        .unwrap()
}

#[test]
fn staging_backend_rejects_one_over_and_overflow_before_any_file_change() {
    let scope = crate::test_path::ScopedDirectory::new("source-staging-growth");
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let cancellation = Arc::new(AtomicBool::new(false));
    let owned_file = file(&scope, "stage.redb");
    let probe = owned_file.try_clone().unwrap();
    let backend = SourceStagingBackend::new(owned_file, cancellation, &engine).unwrap();
    let maximum = MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES;
    backend.set_len(maximum).unwrap();
    backend.write(maximum - 1, &[37]).unwrap();
    assert_eq!(probe.metadata().unwrap().len(), maximum);
    assert_eq!(
        backend.set_len(maximum + 1).unwrap_err().kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(
        backend.write(maximum, &[38]).unwrap_err().kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(
        backend.write(u64::MAX, &[39]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(probe.metadata().unwrap().len(), maximum);
    let mut last = [0];
    backend.read(maximum - 1, &mut last).unwrap();
    assert_eq!(last, [37]);
    backend.close().unwrap();
}

#[test]
fn staging_backend_refuses_unadmitted_writes_and_nonempty_inherited_files() {
    let scope = crate::test_path::ScopedDirectory::new("source-staging-shape");
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let cancellation = Arc::new(AtomicBool::new(false));
    let owned_file = file(&scope, "stage.redb");
    let probe = owned_file.try_clone().unwrap();
    let backend =
        SourceStagingBackend::new(owned_file, Arc::clone(&cancellation), &engine).unwrap();
    assert_eq!(
        backend.write(0, &[37]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(probe.metadata().unwrap().len(), 0);
    for byte in 1..1024 {
        assert!(matches!(
            backend.try_lock_range(Bound::Included(byte), Bound::Included(byte)),
            Err(BackendError::Unsupported)
        ));
        assert!(matches!(
            backend.try_lock_shared_range(Bound::Included(byte), Bound::Unbounded),
            Err(BackendError::Unsupported)
        ));
    }
    assert!(
        backend
            .try_lock_range(Bound::Included(0), Bound::Unbounded)
            .unwrap()
    );
    backend
        .unlock_range(Bound::Unbounded, Bound::Unbounded)
        .unwrap();
    backend.set_len(4096).unwrap();
    backend.close().unwrap();
    drop(backend);
    let before = engine.source_working_budget().observation().unwrap();
    assert_eq!(
        SourceStagingBackend::new(probe, cancellation, &engine)
            .unwrap_err()
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    assert_eq!(
        engine.source_working_budget().observation().unwrap(),
        before
    );
}

#[test]
fn staging_backend_uses_primary_ledger_and_releases_control_after_physical_drop() {
    let scope = crate::test_path::ScopedDirectory::new("source-staging-owner");
    let engine = EngineOwner::try_new(Arc::new(()), 0, LocatorAllocationClass::Ordinary).unwrap();
    let budget = engine.source_working_budget().clone();
    let before = budget.observation().unwrap();
    let weak_engine = Arc::downgrade(&engine);
    let backend = SourceStagingBackend::new(
        file(&scope, "stage.redb"),
        Arc::new(AtomicBool::new(false)),
        &engine,
    )
    .unwrap();
    let control = backend._control.bytes();
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        before.reserved_bytes + control
    );
    drop(engine);
    assert_eq!(weak_engine.strong_count(), 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, control);
    backend.close().unwrap();
    assert_eq!(budget.observation().unwrap().reserved_bytes, control);
    drop(backend);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn staging_backend_control_refusal_allocates_no_file_bytes_or_fallback_ledger() {
    let scope = crate::test_path::ScopedDirectory::new("source-staging-memory-refusal");
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let budget = engine.source_working_budget();
    let before = budget.observation().unwrap();
    let available = PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.reserved_bytes
        - before.owned_bytes;
    let occupied = budget.reserve(available).unwrap();
    let admitted_file = file(&scope, "stage.redb");
    let probe = admitted_file.try_clone().unwrap();
    assert_eq!(
        SourceStagingBackend::new(admitted_file, Arc::new(AtomicBool::new(false)), &engine)
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(probe.metadata().unwrap().len(), 0);
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        before.reserved_bytes + available
    );
    drop(occupied);
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn staging_backend_cancelled_io_refuses_without_preventing_actual_lock_release() {
    let scope = crate::test_path::ScopedDirectory::new("source-staging-cancel");
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let cancellation = Arc::new(AtomicBool::new(false));
    let owned_file = file(&scope, "stage.redb");
    let probe = owned_file.try_clone().unwrap();
    let backend =
        SourceStagingBackend::new(owned_file, Arc::clone(&cancellation), &engine).unwrap();
    backend.set_len(4096).unwrap();
    backend.write(0, &[37]).unwrap();
    // Independent open-file description: a try_clone would share ownership
    // and could reacquire the original lock without proving its release.
    let independent_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(scope.join("stage.redb"))
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let original = probe.metadata().unwrap();
        let independent = independent_file.metadata().unwrap();
        assert_eq!(
            (original.dev(), original.ino()),
            (independent.dev(), independent.ino())
        );
    }
    let another = FileBackend::new(independent_file).unwrap();
    assert!(
        backend
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    assert!(
        !another
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let worker_barrier = Arc::clone(&barrier);
    let worker = std::thread::spawn(move || {
        cancellation.store(true, Ordering::Release);
        worker_barrier.wait();
    });
    barrier.wait();
    worker.join().unwrap();
    assert_eq!(
        backend.set_len(8192).unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(
        backend.write(0, &[38]).unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(
        backend.read(0, &mut [0]).unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(
        backend.sync_data().unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(probe.metadata().unwrap().len(), 4096);
    backend.close().unwrap();
    assert!(
        another
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    another.close().unwrap();
}
