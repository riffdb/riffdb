//! Private staging I/O prerequisite; this is not a source, worker, or publisher.
//!
//! This component bounds the redb file's logical address space before growth.
//! A complete staging map must separately admit redb's cache and transaction
//! metadata, enforce canonical encoded charge, and retain source/plan custody.

use redb::{BackendError, StorageBackend, backends::FileBackend};
use riffdb_storage_api::{
    MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES, StorageError, StorageErrorKind,
};
use riffdb_types::exact_provider_budget_v1::{
    ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
};
use std::{
    fmt,
    fs::File,
    io,
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

pub(crate) struct SourceStagingBackend {
    // Physical backend and cancellation aliases drop before the admission.
    inner: FileBackend,
    cancellation: Arc<AtomicBool>,
    maximum_file_bytes: u64,
    _control: ProjectionSourceWorkReservationV1,
}

impl SourceStagingBackend {
    #[cfg(test)]
    pub(crate) fn new<T>(
        file: File,
        cancellation: Arc<AtomicBool>,
        originating_primary_engine: &crate::engine_owner::EngineOwner<T>,
    ) -> Result<Self, StorageError> {
        Self::with_limit(
            file,
            cancellation,
            originating_primary_engine.source_working_budget(),
            MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES,
            0,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_preallocated_prefix<T>(
        file: File,
        cancellation: Arc<AtomicBool>,
        originating_primary_engine: &crate::engine_owner::EngineOwner<T>,
        physical_prefix_bytes: u64,
    ) -> Result<Self, StorageError> {
        Self::with_limit(
            file,
            cancellation,
            originating_primary_engine.source_working_budget(),
            physical_prefix_bytes,
            physical_prefix_bytes,
        )
    }

    pub(crate) fn from_preallocated_working_prefix(
        file: File,
        cancellation: Arc<AtomicBool>,
        budget: &ProjectionSourceWorkingBudgetV1,
        physical_prefix_bytes: u64,
    ) -> Result<Self, StorageError> {
        Self::with_limit(
            file,
            cancellation,
            budget,
            physical_prefix_bytes,
            physical_prefix_bytes,
        )
    }

    fn with_limit(
        file: File,
        cancellation: Arc<AtomicBool>,
        budget: &ProjectionSourceWorkingBudgetV1,
        maximum_file_bytes: u64,
        expected_initial_bytes: u64,
    ) -> Result<Self, StorageError> {
        // The surrounding private stage owns the pinned directory, exact fresh
        // or preallocated prefix, and pre-existing cancellation allocation. No source capture's
        // separate engine budget may replace this originating primary ledger.
        if cancellation.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        if maximum_file_bytes == 0
            || maximum_file_bytes > MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES
            || expected_initial_bytes > maximum_file_bytes
        {
            return Err(integrity());
        }
        let bytes = u64::try_from(std::mem::size_of::<Self>())
            .ok()
            // Box metadata and one compiled whole-storage lock slot, including
            // fixed HashSet/control slack. Partial ranges refuse below, so
            // FileBackend cannot accumulate caller-selected range records.
            // This does not cover a redb Database/cache.
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(unavailable)?;
        let control = budget.reserve(bytes).map_err(|_| unavailable())?;
        let metadata = file.metadata().map_err(|_| unavailable())?;
        if !metadata.is_file() || metadata.len() != expected_initial_bytes {
            return Err(integrity());
        }
        let inner = FileBackend::new(file).map_err(|_| unavailable())?;
        Ok(Self {
            inner,
            cancellation,
            maximum_file_bytes,
            _control: control,
        })
    }

    fn check_running(&self) -> io::Result<()> {
        if self.cancellation.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "private source staging cancelled",
            ));
        }
        Ok(())
    }

    fn check_length(&self, length: u64) -> io::Result<()> {
        if length > self.maximum_file_bytes {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "private source staging capacity exhausted",
            ));
        }
        Ok(())
    }

    fn check_range(&self, offset: u64, bytes: usize) -> io::Result<()> {
        self.check_running()?;
        let end = u64::try_from(bytes)
            .ok()
            .and_then(|bytes| offset.checked_add(bytes))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid private staging range")
            })?;
        self.check_length(end)?;
        if end > self.inner.len()? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "private staging range exceeds admitted file",
            ));
        }
        Ok(())
    }

    fn checked_lock_range(
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<(Bound<u64>, Bound<u64>), BackendError> {
        // StorageBackend's documented level 2: ExclusiveWriter may fall back
        // to a whole-storage lock. Normalize both supported spellings to one
        // record; partial/native multiprocess ranges are unsupported here.
        if matches!(start, Bound::Unbounded | Bound::Included(0)) && end == Bound::Unbounded {
            Ok((Bound::Unbounded, Bound::Unbounded))
        } else {
            Err(BackendError::Unsupported)
        }
    }
}

impl fmt::Debug for SourceStagingBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceStagingBackend")
            .field("maximum_file_bytes", &self.maximum_file_bytes)
            .finish_non_exhaustive()
    }
}

impl StorageBackend for SourceStagingBackend {
    fn len(&self) -> io::Result<u64> {
        self.check_running()?;
        let length = self.inner.len()?;
        self.check_length(length)?;
        Ok(length)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.check_range(offset, out.len())?;
        self.inner.read(offset, out)
    }

    fn set_len(&self, length: u64) -> io::Result<()> {
        self.check_running()?;
        self.check_length(length)?;
        self.inner.set_len(length)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.check_running()?;
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.check_range(offset, bytes.len())?;
        self.inner.write(offset, bytes)
    }

    fn close(&self) -> io::Result<()> {
        // Cancellation must never stop release of redb's actual native locks.
        self.inner.close()
    }

    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.check_running()?;
        let (start, end) = Self::checked_lock_range(start, end)?;
        self.inner.try_lock_range(start, end)
    }

    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.check_running()?;
        let (start, end) = Self::checked_lock_range(start, end)?;
        self.inner.try_lock_shared_range(start, end)
    }

    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.check_running()?;
        let (start, end) = Self::checked_lock_range(start, end)?;
        self.inner.lock_range(start, end)
    }

    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.check_running()?;
        let (start, end) = Self::checked_lock_range(start, end)?;
        self.inner.lock_shared_range(start, end)
    }

    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        let (start, end) = Self::checked_lock_range(start, end)?;
        self.inner.unlock_range(start, end)
    }

    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.check_running()?;
        let (start, end) = Self::checked_lock_range(start, end)?;
        self.inner.query_lock_range(start, end)
    }
}

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

fn integrity() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}

#[cfg(test)]
#[path = "source_staging_backend_tests.rs"]
mod tests;
