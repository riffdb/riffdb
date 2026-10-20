//! Logical redb growth over an admitted fixed physical prefix.
//!
//! This prerequisite grants no disk, source, map-memory, or worker authority.
//! The enclosing map must retain its physical custodian until this backend
//! and every database/page alias have been destroyed.

#[cfg(test)]
use crate::engine_owner::EngineOwner;
use crate::source_staging_backend::SourceStagingBackend;
use redb::{BackendError, StorageBackend};
use riffdb_storage_api::{
    MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES, StorageError, StorageErrorKind,
};
use riffdb_types::ProjectionSourceWorkReservationV1;
use std::{
    fmt,
    fs::File,
    io,
    ops::Bound,
    sync::{Arc, Mutex, atomic::AtomicBool},
};

pub(crate) const PHYSICAL_PREFIX_BYTES: u64 =
    MAX_PROJECTION_SOURCE_STAGING_ALLOCATED_BYTES - 2 * 1024 * 1024;
// redb grows by max(2*old_usable, old_usable+2*required_allocation).
// The future closed row map must prove its largest allocation is <=4 MiB
// against the sealed readable-record bound. V3 fresh files have no region
// header; include the 4-KiB superheader. Physical writes still refuse at the
// smaller admitted prefix; this virtual tail neither owns disk nor grants
// permission to allocate any redb Database/cache.
pub(crate) const MAX_LOGICAL_BYTES: u64 = 2 * PHYSICAL_PREFIX_BYTES + 2 * (4 * 1024 * 1024) + 4096;

pub(crate) struct SourceVirtualStagingBackend {
    inner: SourceStagingBackend,
    length: Mutex<u64>,
    #[cfg(test)]
    growth_pause: Option<Arc<GrowthPause>>,
    _control: ProjectionSourceWorkReservationV1,
}

impl SourceVirtualStagingBackend {
    #[cfg(test)]
    pub(crate) fn new<T>(
        file: File,
        cancellation: Arc<AtomicBool>,
        engine: &EngineOwner<T>,
    ) -> Result<Self, StorageError> {
        // Admit the wrapper/Box and complete fixed zeroing scratch before
        // constructing FileBackend. Its finite lock/control charge is separate.
        let bytes = u64::try_from(std::mem::size_of::<Self>())
            .ok()
            .and_then(|n| n.checked_add(4096 + 128))
            .ok_or_else(unavailable)?;
        let control = engine
            .source_working_budget()
            .reserve(bytes)
            .map_err(|_| unavailable())?;
        let inner = SourceStagingBackend::from_preallocated_prefix(
            file,
            cancellation,
            engine,
            PHYSICAL_PREFIX_BYTES,
        )?;
        Ok(Self {
            inner,
            length: Mutex::new(0),
            #[cfg(test)]
            growth_pause: None,
            _control: control,
        })
    }

    pub(crate) fn admit_control(
        budget: &riffdb_types::ProjectionSourceWorkingBudgetV1,
    ) -> Result<ProjectionSourceWorkReservationV1, StorageError> {
        let bytes = u64::try_from(std::mem::size_of::<Self>())
            .ok()
            .and_then(|n| n.checked_add(4096 + 128))
            .ok_or_else(unavailable)?;
        budget.reserve(bytes).map_err(|_| unavailable())
    }

    pub(crate) fn from_admitted_prefix(
        file: File,
        cancellation: Arc<AtomicBool>,
        budget: &riffdb_types::ProjectionSourceWorkingBudgetV1,
        control: ProjectionSourceWorkReservationV1,
    ) -> Result<Self, StorageError> {
        let inner = SourceStagingBackend::from_preallocated_working_prefix(
            file,
            cancellation,
            budget,
            PHYSICAL_PREFIX_BYTES,
        )?;
        Ok(Self {
            inner,
            length: Mutex::new(0),
            #[cfg(test)]
            growth_pause: None,
            _control: control,
        })
    }

    fn check_physical(&self) -> io::Result<()> {
        if self.inner.len()? != PHYSICAL_PREFIX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "private staging physical prefix changed",
            ));
        }
        Ok(())
    }

    fn state(&self) -> io::Result<std::sync::MutexGuard<'_, u64>> {
        self.check_physical()?;
        self.length
            .lock()
            .map_err(|_| io::Error::other("private staging length unavailable"))
    }

    fn range_end(offset: u64, bytes: usize, length: u64) -> io::Result<u64> {
        let end = u64::try_from(bytes)
            .ok()
            .and_then(|n| offset.checked_add(n))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid private staging range")
            })?;
        if end > length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "private staging range exceeds logical length",
            ));
        }
        Ok(end)
    }
}

impl fmt::Debug for SourceVirtualStagingBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceVirtualStagingBackend")
            .field("maximum_logical_bytes", &MAX_LOGICAL_BYTES)
            .field("physical_prefix_bytes", &PHYSICAL_PREFIX_BYTES)
            .finish_non_exhaustive()
    }
}

impl StorageBackend for SourceVirtualStagingBackend {
    fn len(&self) -> io::Result<u64> {
        Ok(*self.state()?)
    }

    fn set_len(&self, length: u64) -> io::Result<()> {
        let mut old = self.state()?;
        if length > MAX_LOGICAL_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "private staging logical capacity exhausted",
            ));
        }
        // Never physically truncate or extend the admitted prefix. Re-growing
        // must clear reused bytes before publishing the new logical length.
        let mut offset = *old;
        let mapped_end = length.min(PHYSICAL_PREFIX_BYTES);
        let zeros = [0_u8; 4096];
        while offset < mapped_end {
            let count = usize::try_from((mapped_end - offset).min(4096))
                .map_err(|_| io::Error::other("private staging zero range unavailable"))?;
            self.inner.write(offset, &zeros[..count])?;
            offset += u64::try_from(count)
                .map_err(|_| io::Error::other("private staging zero range unavailable"))?;
            #[cfg(test)]
            if let Some(pause) = &self.growth_pause
                && !pause.used.swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                pause.entered.wait();
                // A bounded test-only release channel disconnects/releases
                // under panic as well; production has no callback or channel.
                if let Ok(release) = pause.release.lock() {
                    let _ = release.recv();
                }
            }
        }
        // Cancellation may arrive after the final zero write or for an all-zero
        // virtual-tail growth. Check it before making the new length visible.
        self.check_physical()?;
        *old = length;
        Ok(())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let length = self.state()?;
        let end = Self::range_end(offset, out.len(), *length)?;
        let mapped = end.min(PHYSICAL_PREFIX_BYTES).saturating_sub(offset);
        let mapped = usize::try_from(mapped)
            .map_err(|_| io::Error::other("private staging read range unavailable"))?;
        if mapped != 0 {
            self.inner.read(offset, &mut out[..mapped])?;
        }
        out[mapped..].fill(0);
        Ok(())
    }

    fn write(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let length = self.state()?;
        let end = Self::range_end(offset, bytes.len(), *length)?;
        if end > PHYSICAL_PREFIX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "private staging physical capacity exhausted",
            ));
        }
        self.inner.write(offset, bytes)
    }

    fn sync_data(&self) -> io::Result<()> {
        let _length = self.state()?;
        self.inner.sync_data()
    }
    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.inner.try_lock_range(start, end)
    }
    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.inner.try_lock_shared_range(start, end)
    }
    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.lock_range(start, end)
    }
    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.lock_shared_range(start, end)
    }
    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.unlock_range(start, end)
    }
    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.inner.query_lock_range(start, end)
    }
}

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

#[cfg(test)]
struct GrowthPause {
    entered: std::sync::Barrier,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    used: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
#[path = "source_staging_virtual_tests.rs"]
mod tests;
