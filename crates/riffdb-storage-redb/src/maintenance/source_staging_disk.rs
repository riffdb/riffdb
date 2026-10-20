//! Private ext4 scratch admission. It grants no source or writer authority.
#![cfg_attr(not(test), allow(dead_code))]

use super::path_guard::PinnedDirectory;
#[cfg(test)]
use crate::engine_owner::EngineOwner;
use crate::source_evaluation_work::SourceStageSlotLease;
use redb::StorageBackend;
use riffdb_storage_api::{
    MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES, StorageError, StorageErrorKind,
};
#[cfg(test)]
use riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkerOwnershipV1;
use riffdb_types::{ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1};
use std::{
    ffi::OsStr,
    fs::File,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

const DIRECTORY: &str = "source-staging-v1";
const FILE: &str = "map.redb";
const MAX_PARENT_PATH_BYTES: usize = 4096;
const DATA_FILE_BYTES: u64 = MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES - 2 * 1024 * 1024;
const WORK_CONTROL_BYTES: u64 = 128 * 1024;
const EXT4_MAGIC: u64 = 0xef53;
const EXTENTS_FLAG: u32 = 0x0008_0000;
// Compressed, encrypted, verity, inline-data and DAX scratch require a separate
// allocation envelope. Preserve all flags; refuse rather than clear these.
const UNSUPPORTED_INODE_FLAGS: u32 =
    0x0000_0004 | 0x0000_0800 | 0x0010_0000 | 0x1000_0000 | 0x0200_0000;

enum StageAdmission<'work> {
    Combined {
        _slot: SourceStageSlotLease<'work>,
    },
    #[cfg(test)]
    Standalone {
        _worker: ProjectionSourceWorkerOwnershipV1,
    },
}

pub(crate) struct SourceStagingDiskJob<'work> {
    budget: ProjectionSourceWorkingBudgetV1,
    cancellation: Arc<AtomicBool>,
    _admission: StageAdmission<'work>,
    _control: ProjectionSourceWorkReservationV1,
    #[cfg(test)]
    cancel_after_first_block: bool,
    #[cfg(test)]
    first_block_written: bool,
}

#[cfg(test)]
impl SourceStagingDiskJob<'static> {
    pub(crate) fn new<T>(
        engine: &EngineOwner<T>,
        cancellation: Arc<AtomicBool>,
    ) -> Result<Self, StorageError> {
        cancelled(&cancellation)?;
        let budget = engine.source_working_budget();
        let bytes = u64::try_from(std::mem::size_of::<Self>())
            .ok()
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or_else(unavailable)?;
        let control = budget.reserve(bytes).map_err(|_| unavailable())?;
        let worker = budget.try_acquire_worker().map_err(|_| unavailable())?;
        // One move-only claim reserves the complete 128-MiB staging disk
        // envelope now. prepare() borrows this owner exclusively; another file
        // cannot coexist under this claim. This is accounting, not role proof.
        Ok(Self {
            budget: budget.clone(),
            cancellation,
            _admission: StageAdmission::Standalone { _worker: worker },
            _control: control,
            #[cfg(test)]
            cancel_after_first_block: false,
            #[cfg(test)]
            first_block_written: false,
        })
    }
}

impl<'work> SourceStagingDiskJob<'work> {
    pub(crate) fn from_slot(slot: SourceStageSlotLease<'work>) -> Result<Self, StorageError> {
        let cancellation = Arc::clone(slot.cancellation());
        cancelled(&cancellation)?;
        let budget = slot.originating_budget().clone();
        let bytes = u64::try_from(std::mem::size_of::<Self>())
            .ok()
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or_else(unavailable)?;
        let control = budget.reserve(bytes).map_err(|_| unavailable())?;
        // The sealed exclusive slot already belongs to the retained Original
        // work owner. Never acquire another worker or accept another ledger.
        Ok(Self {
            budget,
            cancellation,
            _admission: StageAdmission::Combined { _slot: slot },
            _control: control,
            #[cfg(test)]
            cancel_after_first_block: false,
            #[cfg(test)]
            first_block_written: false,
        })
    }

    pub(crate) const fn reserved_disk_bytes(&self) -> u64 {
        MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES
    }

    pub(crate) fn prepare<'stage>(
        &'stage mut self,
        private_parent: &Path,
    ) -> Result<SourceStagingDiskOwner<'stage, 'work>, StorageError> {
        cancelled(&self.cancellation)?;
        if private_parent.as_os_str().len() > MAX_PARENT_PATH_BYTES {
            return Err(limit());
        }
        // Before directory capabilities, inode creation, flags or data I/O:
        // admit paths (<=4KiB, bounded copies), control, and a 4-KiB zero block.
        // This is not the separate complete redb Database memory census.
        let control = self
            .budget
            .reserve(WORK_CONTROL_BYTES)
            .map_err(|_| unavailable())?;
        let parent = PinnedDirectory::open(private_parent)?;
        parent.verify_private()?;
        let directory = parent.create_private_child(OsStr::new(DIRECTORY))?;
        let custody = ScratchDirectory {
            directory,
            parent,
            budget: self.budget.clone(),
        };
        let created = custody.directory.create_new_file(OsStr::new(FILE))?;
        created.sync_all().map_err(|_| unavailable())?;
        drop(created);
        let file = custody
            .directory
            .open_file_read_write(OsStr::new(FILE))?
            .into_std();
        let plan = admit_ext4(&file)?;
        cancelled(&self.cancellation)?;
        preallocate(&file, self)?;
        file.sync_all().map_err(|_| unavailable())?;
        verify_allocated_consistency(&file, plan)?;
        Ok(SourceStagingDiskOwner {
            file,
            custody,
            plan,
            _control: control,
            _job: self,
        })
    }
}

struct ScratchDirectory {
    directory: PinnedDirectory,
    parent: PinnedDirectory,
    budget: ProjectionSourceWorkingBudgetV1,
}

impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        // The private File and all engine aliases must drop before this owner.
        // Unknown cleanup permanently closes this ledger; never re-admit disk
        // over an inode whose physical removal was not established.
        if self
            .directory
            .remove_self_all()
            .and_then(|_| self.parent.sync())
            .is_err()
        {
            self.budget.refuse_future_work();
        }
    }
}

pub(crate) struct SourceStagingDiskOwner<'stage, 'work> {
    file: File,
    custody: ScratchDirectory,
    plan: DiskPlan,
    _control: ProjectionSourceWorkReservationV1,
    _job: &'stage mut SourceStagingDiskJob<'work>,
}

impl SourceStagingDiskOwner<'_, '_> {
    pub(crate) fn observation(&self) -> Result<(u64, u64), StorageError> {
        self.custody.directory.verify_private()?;
        let allocated = allocated_bytes(&self.file)?;
        Ok((self.plan.maximum_allocated_bytes, allocated))
    }
}

#[derive(Clone, Copy, Debug)]
struct DiskPlan {
    maximum_allocated_bytes: u64,
    initial_allocated_bytes: u64,
    block_bytes: u64,
}

fn disk_plan(filesystem: u64, block: u64, initial: u64) -> Result<DiskPlan, StorageError> {
    if filesystem != EXT4_MAGIC || !block.is_power_of_two() || !(1024..=65536).contains(&block) {
        return Err(unsupported());
    }
    let metadata = indirect_metadata_bytes(DATA_FILE_BYTES, block)?;
    let maximum = DATA_FILE_BYTES
        .checked_add(metadata)
        .and_then(|bytes| bytes.checked_add(initial))
        // Fresh bounded private directory and at most one incomplete metadata
        // branch during a refused allocation; no inode flags change afterward.
        .and_then(|bytes| bytes.checked_add(4 * block))
        .ok_or_else(limit)?;
    if maximum > MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES {
        return Err(limit());
    }
    Ok(DiskPlan {
        maximum_allocated_bytes: maximum,
        initial_allocated_bytes: initial,
        block_bytes: block,
    })
}

fn indirect_metadata_bytes(bytes: u64, block: u64) -> Result<u64, StorageError> {
    if bytes > DATA_FILE_BYTES || !block.is_power_of_two() || !(1024..=65536).contains(&block) {
        return Err(limit());
    }
    let pointers = block / 4;
    let mut remaining = bytes.div_ceil(block).saturating_sub(12);
    let mut metadata_blocks = 0_u64;
    let mut capacity = pointers;
    for depth in 1..=3 {
        if remaining == 0 {
            break;
        }
        let used = remaining.min(capacity);
        remaining -= used;
        let mut below = used;
        for _ in 0..depth {
            below = below.div_ceil(pointers);
            metadata_blocks = metadata_blocks.checked_add(below).ok_or_else(limit)?;
        }
        capacity = capacity.checked_mul(pointers).ok_or_else(limit)?;
    }
    if remaining != 0 {
        return Err(limit());
    }
    metadata_blocks.checked_mul(block).ok_or_else(limit)
}

#[cfg(target_os = "linux")]
fn admit_ext4(file: &File) -> Result<DiskPlan, StorageError> {
    use rustix::fs::{IFlags, fstatfs, ioctl_getflags, ioctl_setflags};
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata().map_err(|_| unavailable())?;
    if !metadata.is_file() || metadata.len() != 0 || metadata.nlink() != 1 {
        return Err(integrity());
    }
    let stat = fstatfs(file).map_err(|_| unsupported())?;
    let block = u64::try_from(stat.f_bsize).map_err(|_| unsupported())?;
    let initial = allocated_bytes(file)?;
    let plan = disk_plan(stat.f_type as u64, block, initial)?;
    let flags = ioctl_getflags(file).map_err(|_| unsupported())?;
    if flags.bits() & EXTENTS_FLAG == 0 || flags.bits() & UNSUPPORTED_INODE_FLAGS != 0 {
        return Err(unsupported());
    }
    // Kernel ext4_ind_migrate refuses bigalloc before migration. Success on
    // this fresh empty inode proves allocation uses ordinary filesystem blocks.
    let expected = flags.bits() & !EXTENTS_FLAG;
    ioctl_setflags(file, IFlags::from_bits_retain(expected)).map_err(|_| unsupported())?;
    if ioctl_getflags(file).map_err(|_| unsupported())?.bits() != expected {
        return Err(integrity());
    }
    Ok(plan)
}

#[cfg(not(target_os = "linux"))]
fn admit_ext4(_: &File) -> Result<DiskPlan, StorageError> {
    Err(unsupported())
}

#[cfg(unix)]
fn allocated_bytes(file: &File) -> Result<u64, StorageError> {
    use std::os::unix::fs::MetadataExt;
    file.metadata()
        .map_err(|_| unavailable())?
        .blocks()
        .checked_mul(512)
        .ok_or_else(limit)
}

#[cfg(not(unix))]
fn allocated_bytes(_: &File) -> Result<u64, StorageError> {
    Err(unsupported())
}

fn preallocate(file: &File, job: &mut SourceStagingDiskJob<'_>) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        // Indirect files do not support fallocate. Populate the fixed backing
        // prefix once; the future redb backend changes only a logical length,
        // never truncating or extending this pre-admitted physical mapping.
        let zero = [0_u8; 4096];
        for offset in (0..DATA_FILE_BYTES).step_by(zero.len()) {
            cancelled(&job.cancellation)?;
            file.write_all_at(&zero, offset)
                .map_err(|_| unavailable())?;
            #[cfg(test)]
            if offset == 0 && job.cancel_after_first_block {
                job.first_block_written = true;
                job.cancellation.store(true, Ordering::Release);
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (file, job);
        Err(unsupported())
    }
}

fn verify_allocated_consistency(file: &File, plan: DiskPlan) -> Result<(), StorageError> {
    if file.metadata().map_err(|_| unavailable())?.len() != DATA_FILE_BYTES
        || allocated_bytes(file)? > plan.maximum_allocated_bytes
    {
        return Err(integrity());
    }
    Ok(())
}

fn cancelled(cancellation: &AtomicBool) -> Result<(), StorageError> {
    if cancellation.load(Ordering::Acquire) {
        return Err(unavailable());
    }
    Ok(())
}
fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
fn unsupported() -> StorageError {
    StorageError::new(StorageErrorKind::IncompatibleFormat, None)
}
fn integrity() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}
fn limit() -> StorageError {
    StorageError::new(StorageErrorKind::LimitExceeded, None)
}

// Raw aliases never leave this closed factory. StorageBackend requires
// 'static; keep the independently owned backend and its borrowed disk custody
// together, backend first. A future admitted map is a private child module.
pub(crate) struct SourceOwnedStagingBackend<'stage, 'work> {
    backend: crate::source_staging_virtual::SourceVirtualStagingBackend,
    disk: SourceStagingDiskOwner<'stage, 'work>,
}

impl<'stage, 'work> SourceOwnedStagingBackend<'stage, 'work> {
    pub(crate) fn new(disk: SourceStagingDiskOwner<'stage, 'work>) -> Result<Self, StorageError> {
        use crate::source_staging_virtual::SourceVirtualStagingBackend;
        let control = SourceVirtualStagingBackend::admit_control(&disk._job.budget)?;
        disk.custody.directory.verify_private()?;
        let file = disk.file.try_clone().map_err(|_| unavailable())?;
        let backend = SourceVirtualStagingBackend::from_admitted_prefix(
            file,
            Arc::clone(&disk._job.cancellation),
            &disk._job.budget,
            control,
        )?;
        Ok(Self { backend, disk })
    }
    pub(crate) fn len(&self) -> std::io::Result<u64> {
        self.backend.len()
    }
    pub(crate) fn set_len(&self, bytes: u64) -> std::io::Result<()> {
        self.backend.set_len(bytes)
    }
    pub(crate) fn read(&self, offset: u64, bytes: &mut [u8]) -> std::io::Result<()> {
        self.backend.read(offset, bytes)
    }
    pub(crate) fn write(&self, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
        self.backend.write(offset, bytes)
    }
    pub(crate) fn sync_data(&self) -> std::io::Result<()> {
        self.backend.sync_data()
    }
    pub(crate) fn close(&self) -> std::io::Result<()> {
        self.backend.close()
    }
    pub(crate) fn observation(&self) -> Result<(u64, u64), StorageError> {
        self.disk.observation()
    }
}

#[path = "source_staging_map.rs"]
mod map;
#[cfg(all(test, feature = "projection-source-driver"))]
pub(crate) use map::SourceStageEvaluationError;
pub(crate) use map::{
    SourceFrozenStage, SourceStageCursor, SourceStageMap, SourceStageUpdate, SourceStagedRow,
};

#[cfg(test)]
#[path = "source_staging_disk_tests.rs"]
mod tests;

// The independently owned backend can outlive its last method call. Keep the
// originating job's slot/custody borrow until this physical owner is dropped.
impl Drop for SourceStagingDiskOwner<'_, '_> {
    fn drop(&mut self) {}
}
