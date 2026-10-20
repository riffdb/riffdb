// req: STO-020, PRJ-004, END-008
use super::*;
use crate::locator_memory::LocatorAllocationClass;
use riffdb_types::exact_provider_budget_v1::PROJECTION_SOURCE_WORKING_MEMORY_BYTES;
use std::{fs::OpenOptions, sync::atomic::Ordering};

struct ReleaseGrowth(Option<std::sync::mpsc::SyncSender<()>>);

impl Drop for ReleaseGrowth {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.try_send(());
        }
    }
}

fn file(scope: &crate::test_path::ScopedDirectory) -> File {
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(scope.join("stage.redb"))
        .unwrap();
    // Sparse placement is sufficient for this logical-backend proof. Actual
    // ext4 block preadmission belongs to the independent disk-owner tests.
    file.set_len(PHYSICAL_PREFIX_BYTES).unwrap();
    file
}

#[test]
fn virtual_staging_bounds_physical_writes_and_zeroes_unmapped_logical_tail() {
    let scope = crate::test_path::ScopedDirectory::new("source-virtual-tail");
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let file = file(&scope);
    let probe = file.try_clone().unwrap();
    let backend =
        SourceVirtualStagingBackend::new(file, Arc::new(AtomicBool::new(false)), &engine).unwrap();
    assert_eq!(backend.len().unwrap(), 0);
    backend.set_len(MAX_LOGICAL_BYTES).unwrap();
    backend.write(PHYSICAL_PREFIX_BYTES - 1, &[37]).unwrap();
    let mut edge = [255; 4];
    backend.read(PHYSICAL_PREFIX_BYTES - 1, &mut edge).unwrap();
    assert_eq!(edge, [37, 0, 0, 0]);
    assert_eq!(
        backend
            .write(PHYSICAL_PREFIX_BYTES, &[38])
            .unwrap_err()
            .kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(
        backend
            .write(PHYSICAL_PREFIX_BYTES - 1, &[38, 39])
            .unwrap_err()
            .kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(
        backend.set_len(MAX_LOGICAL_BYTES + 1).unwrap_err().kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(
        backend.write(u64::MAX, &[40]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    backend.read(PHYSICAL_PREFIX_BYTES - 1, &mut edge).unwrap();
    assert_eq!(edge, [37, 0, 0, 0]);
    assert_eq!(probe.metadata().unwrap().len(), PHYSICAL_PREFIX_BYTES);
    backend.close().unwrap();
}

#[test]
fn virtual_staging_shrink_regrow_keeps_physical_prefix_and_erases_reused_bytes() {
    let scope = crate::test_path::ScopedDirectory::new("source-virtual-regrow");
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let file = file(&scope);
    let probe = file.try_clone().unwrap();
    let backend =
        SourceVirtualStagingBackend::new(file, Arc::new(AtomicBool::new(false)), &engine).unwrap();
    backend.set_len(16384).unwrap();
    backend.write(0, &[37; 8192]).unwrap();
    backend.set_len(3).unwrap();
    assert_eq!(probe.metadata().unwrap().len(), PHYSICAL_PREFIX_BYTES);
    assert_eq!(
        backend.read(3, &mut [0]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    backend.set_len(8192).unwrap();
    let mut bytes = [255; 8192];
    backend.read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[..3], &[37; 3]);
    assert!(bytes[3..].iter().all(|byte| *byte == 0));
    backend.set_len(0).unwrap();
    assert_eq!(probe.metadata().unwrap().len(), PHYSICAL_PREFIX_BYTES);
    backend.set_len(1).unwrap();
    backend.read(0, &mut bytes[..1]).unwrap();
    assert_eq!(bytes[0], 0);
    backend.close().unwrap();
}

#[test]
fn virtual_staging_real_growth_lock_prevents_partial_length_publication_on_cancel() {
    let scope = crate::test_path::ScopedDirectory::new("source-virtual-cancel");
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let cancellation = Arc::new(AtomicBool::new(false));
    let file = file(&scope);
    let probe = file.try_clone().unwrap();
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let release = ReleaseGrowth(Some(release));
    let pause = Arc::new(GrowthPause {
        entered: std::sync::Barrier::new(2),
        release: Mutex::new(released),
        used: AtomicBool::new(false),
    });
    let mut backend =
        SourceVirtualStagingBackend::new(file, Arc::clone(&cancellation), &engine).unwrap();
    backend.growth_pause = Some(Arc::clone(&pause));
    let backend = Arc::new(backend);
    let worker_backend = Arc::clone(&backend);
    let worker = std::thread::spawn(move || worker_backend.set_len(8192));
    pause.entered.wait();
    // The actual first zero write has completed, but logical length is still
    // held under the real production mutex; no synthetic completion is used.
    assert!(matches!(
        backend.length.try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    ));
    cancellation.store(true, Ordering::Release);
    drop(release);
    assert_eq!(
        worker.join().unwrap().unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(*backend.length.lock().unwrap(), 0);
    assert_eq!(
        backend.len().unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(probe.metadata().unwrap().len(), PHYSICAL_PREFIX_BYTES);
    backend.close().unwrap();
}

#[test]
fn virtual_staging_uses_one_originating_ledger_and_control_outlives_close() {
    let scope = crate::test_path::ScopedDirectory::new("source-virtual-budget");
    let engine = EngineOwner::try_new(Arc::new(()), 0, LocatorAllocationClass::Ordinary).unwrap();
    let budget = engine.source_working_budget().clone();
    let before = budget.observation().unwrap();
    let backend =
        SourceVirtualStagingBackend::new(file(&scope), Arc::new(AtomicBool::new(false)), &engine)
            .unwrap();
    let admitted = budget.observation().unwrap().reserved_bytes - before.reserved_bytes;
    assert!(admitted > backend._control.bytes());
    let weak = Arc::downgrade(&engine);
    drop(engine);
    assert_eq!(weak.strong_count(), 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, admitted);
    backend.close().unwrap();
    assert_eq!(budget.observation().unwrap().reserved_bytes, admitted);
    drop(backend);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn virtual_staging_budget_and_prefix_shape_refuse_before_file_mutation() {
    let scope = crate::test_path::ScopedDirectory::new("source-virtual-refuse");
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let budget = engine.source_working_budget();
    let before = budget.observation().unwrap();
    let available = PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.reserved_bytes
        - before.owned_bytes;
    let occupied = budget.reserve(available).unwrap();
    let file = file(&scope);
    let probe = file.try_clone().unwrap();
    assert_eq!(
        SourceVirtualStagingBackend::new(file, Arc::new(AtomicBool::new(false)), &engine)
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(probe.metadata().unwrap().len(), PHYSICAL_PREFIX_BYTES);
    drop(occupied);
    assert_eq!(budget.observation().unwrap(), before);
    probe.set_len(PHYSICAL_PREFIX_BYTES - 1).unwrap();
    assert_eq!(
        SourceVirtualStagingBackend::new(probe, Arc::new(AtomicBool::new(false)), &engine)
            .unwrap_err()
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn virtual_staging_poison_refuses_io_but_preserves_native_lock_release() {
    let scope = crate::test_path::ScopedDirectory::new("source-virtual-poison");
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let backend = Arc::new(
        SourceVirtualStagingBackend::new(file(&scope), Arc::new(AtomicBool::new(false)), &engine)
            .unwrap(),
    );
    assert!(
        backend
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    let independent = OpenOptions::new()
        .read(true)
        .write(true)
        .open(scope.join("stage.redb"))
        .unwrap();
    let another = redb::backends::FileBackend::new(independent).unwrap();
    assert!(
        !another
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    let worker_backend = Arc::clone(&backend);
    assert!(
        std::thread::spawn(move || {
            let _guard = worker_backend.length.lock().unwrap();
            panic!("poison private virtual length");
        })
        .join()
        .is_err()
    );
    assert_eq!(backend.len().unwrap_err().kind(), io::ErrorKind::Other);
    assert_eq!(backend.set_len(1).unwrap_err().kind(), io::ErrorKind::Other);
    assert_eq!(
        backend.read(0, &mut []).unwrap_err().kind(),
        io::ErrorKind::Other
    );
    assert_eq!(
        backend.write(0, &[]).unwrap_err().kind(),
        io::ErrorKind::Other
    );
    assert_eq!(
        backend.sync_data().unwrap_err().kind(),
        io::ErrorKind::Other
    );
    backend.close().unwrap();
    assert!(
        another
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    another.close().unwrap();
}
