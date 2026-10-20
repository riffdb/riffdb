// req: STO-020, PRJ-004, END-008
use super::*;
use crate::locator_memory::LocatorAllocationClass;

#[test]
fn ext4_staging_indirect_prefix_envelope_includes_all_pointer_levels_before_growth() {
    let expected = [
        (1024, 507 * 1024),
        (2048, 127 * 2048),
        (4096, 33 * 4096),
        (8192, 9 * 8192),
        (16384, 3 * 16384),
        (32768, 32768),
        (65536, 65536),
    ];
    for (block, bytes) in expected {
        assert_eq!(
            indirect_metadata_bytes(DATA_FILE_BYTES, block).unwrap(),
            bytes
        );
        let plan = disk_plan(EXT4_MAGIC, block, 0).unwrap();
        assert_eq!(plan.block_bytes, block);
        assert_eq!(plan.initial_allocated_bytes, 0);
        assert_eq!(
            plan.maximum_allocated_bytes,
            DATA_FILE_BYTES + bytes + 4 * block
        );
        assert!(plan.maximum_allocated_bytes <= MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES);
        assert_eq!(indirect_metadata_bytes(0, block).unwrap(), 0);
        assert_eq!(indirect_metadata_bytes(12 * block, block).unwrap(), 0);
        assert_eq!(
            indirect_metadata_bytes(12 * block + 1, block).unwrap(),
            block
        );
    }
}

#[test]
fn ext4_staging_unsupported_geometry_overflow_and_initial_charge_refuse_before_io() {
    for filesystem in [0, 0x0102_1994, 0x5846_5342, u64::MAX] {
        assert_eq!(
            disk_plan(filesystem, 4096, 0).unwrap_err().kind(),
            StorageErrorKind::IncompatibleFormat
        );
    }
    for block in [0, 1, 512, 1023, 4095, 65537, u64::MAX] {
        assert_eq!(
            disk_plan(EXT4_MAGIC, block, 0).unwrap_err().kind(),
            StorageErrorKind::IncompatibleFormat
        );
    }
    assert_eq!(
        disk_plan(EXT4_MAGIC, 4096, u64::MAX).unwrap_err().kind(),
        StorageErrorKind::LimitExceeded
    );
    assert_eq!(
        disk_plan(EXT4_MAGIC, 4096, 2 * 1024 * 1024)
            .unwrap_err()
            .kind(),
        StorageErrorKind::LimitExceeded
    );
    assert_eq!(
        indirect_metadata_bytes(DATA_FILE_BYTES + 1, 4096)
            .unwrap_err()
            .kind(),
        StorageErrorKind::LimitExceeded
    );
    assert_eq!(
        indirect_metadata_bytes(u64::MAX, 4096).unwrap_err().kind(),
        StorageErrorKind::LimitExceeded
    );
}

#[test]
fn ext4_staging_worker_claim_and_control_refusal_precede_directory_or_inode_changes() {
    let scope = crate::test_path::ScopedDirectory::new("source-disk-refusal");
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let budget = engine.source_working_budget();
    let before = budget.observation().unwrap();
    let cancellation = Arc::new(AtomicBool::new(false));
    let mut job = SourceStagingDiskJob::new(&engine, Arc::clone(&cancellation)).unwrap();
    assert_eq!(
        job.reserved_disk_bytes(),
        MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES
    );
    assert!(SourceStagingDiskJob::new(&engine, Arc::clone(&cancellation)).is_err());
    let observed = budget.observation().unwrap();
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - observed.fixed_owner_bytes
        - observed.reserved_bytes
        - observed.owned_bytes;
    let occupied = budget.reserve(remaining).unwrap();
    assert_eq!(
        job.prepare(&scope.join("missing-parent"))
            .err()
            .unwrap()
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert!(!scope.join("missing-parent").exists());
    drop(occupied);
    cancellation.store(true, Ordering::Release);
    assert_eq!(
        job.prepare(&scope.join("missing-parent"))
            .err()
            .unwrap()
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert!(!scope.join("missing-parent").exists());
    drop(job);
    assert_eq!(budget.observation().unwrap(), before);
    assert!(budget.try_acquire_worker().is_ok());
}

#[cfg(target_os = "linux")]
#[test]
fn ext4_staging_preallocates_full_admitted_prefix_and_removes_exact_private_custody() {
    use rustix::fs::{fstatfs, ioctl_getflags};
    let scope = crate::test_path::ScopedDirectory::new("source-disk-full-ext4");
    let parent = scope.join("owned-parent");
    std::fs::create_dir(&parent).unwrap();
    assert_eq!(
        fstatfs(File::open(&parent).unwrap()).unwrap().f_type as u64,
        EXT4_MAGIC,
        "qualification must select actual ext4 disk scratch, not a RAM-backed test root"
    );
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let budget = engine.source_working_budget();
    let before = budget.observation().unwrap();
    let mut job = SourceStagingDiskJob::new(&engine, Arc::new(AtomicBool::new(false))).unwrap();
    {
        let stage = job.prepare(&parent).unwrap();
        assert_eq!(stage.file.metadata().unwrap().len(), DATA_FILE_BYTES);
        assert_eq!(
            ioctl_getflags(&stage.file).unwrap().bits() & EXTENTS_FLAG,
            0
        );
        let (maximum, allocated) = stage.observation().unwrap();
        assert!(allocated >= DATA_FILE_BYTES);
        assert!(allocated <= maximum);
        assert!(maximum <= MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES);
        assert!(
            budget.observation().unwrap().reserved_bytes
                >= before.reserved_bytes + WORK_CONTROL_BYTES
        );
        assert!(parent.join(DIRECTORY).join(FILE).is_file());
    }
    assert!(!parent.join(DIRECTORY).exists());
    drop(job);
    assert_eq!(budget.observation().unwrap(), before);
}

#[cfg(target_os = "linux")]
#[test]
fn ext4_staging_refuses_inherited_file_and_preserves_all_original_bytes_and_flags() {
    use rustix::fs::ioctl_getflags;
    let scope = crate::test_path::ScopedDirectory::new("source-disk-inherited-refusal");
    let path = scope.join("existing.redb");
    std::fs::write(&path, b"already-owned-material").unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let flags = ioctl_getflags(&file).unwrap();
    let before = allocated_bytes(&file).unwrap();
    assert_eq!(
        admit_ext4(&file).unwrap_err().kind(),
        StorageErrorKind::InvariantViolation
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"already-owned-material");
    assert_eq!(ioctl_getflags(&file).unwrap(), flags);
    assert_eq!(allocated_bytes(&file).unwrap(), before);
}

#[cfg(target_os = "linux")]
#[test]
fn ext4_staging_cancellation_after_actual_first_block_releases_partial_inode_and_claim() {
    let scope = crate::test_path::ScopedDirectory::new("source-disk-partial-cancel");
    let parent = scope.join("owned-parent");
    std::fs::create_dir(&parent).unwrap();
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let budget = engine.source_working_budget();
    let before = budget.observation().unwrap();
    let mut job = SourceStagingDiskJob::new(&engine, Arc::new(AtomicBool::new(false))).unwrap();
    job.cancel_after_first_block = true;
    assert_eq!(
        job.prepare(&parent).err().unwrap().kind(),
        StorageErrorKind::Unavailable
    );
    assert!(
        job.first_block_written,
        "the schedule must reach actual data allocation before cancellation"
    );
    assert!(!parent.join(DIRECTORY).exists());
    drop(job);
    assert_eq!(budget.observation().unwrap(), before);
    assert!(budget.try_acquire_worker().is_ok());
}
