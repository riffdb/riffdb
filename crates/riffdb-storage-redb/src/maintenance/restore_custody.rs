//! ADR-0289 custody foundation, exercised before production-stage integration.
//!
//! The outer owner acquires native exclusion independently of engine leases.
//! Engine range methods acquire bounded subleases of that stronger exclusion;
//! close releases every sublease without unlocking the outer native owner.

use super::path_guard::PinnedDirectory;
use crate::error::storage_error;
use redb::{BackendError, StorageBackend};
use riffdb_storage_api::{StorageError, StorageErrorKind};
use std::{
    ffi::OsString,
    fs::File,
    io,
    ops::Bound,
    path::Path,
    sync::{
        Arc, RwLock, RwLockReadGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

type Range = (Bound<u64>, Bound<u64>);
const MAX_ENGINE_RANGES: usize = 64;

pub(super) struct RestoreCustody {
    shared: Arc<Shared>,
}

struct Shared {
    parent: PinnedDirectory,
    name: OsString,
    file: File,
    native_guard: redb::backends::FileBackend,
    // Zero is the private preparation phase, before any validation proof.
    // A frozen verifier is bounded by the exact completed artifact length.
    maximum_bytes: AtomicU64,
    cancellation: Arc<AtomicBool>,
    invalid: AtomicBool,
    leases: RwLock<Leases>,
}

#[derive(Default)]
struct Leases {
    generation: u64,
    active: Option<u64>,
    ranges: Vec<Range>,
}

pub(super) struct RestoreEngineBackend {
    file: redb::backends::FileBackend,
    shared: Arc<Shared>,
    generation: u64,
}

impl std::fmt::Debug for RestoreEngineBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RestoreEngineBackend([REDACTED])")
    }
}

impl std::fmt::Debug for RestoreCustody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RestoreCustody([REDACTED])")
    }
}

impl RestoreCustody {
    // Test-only construction while the native lock protocol is established.
    // Production construction must consume verified native materialization.
    fn capture(path: &Path, cancellation: Arc<AtomicBool>) -> Result<Self, StorageError> {
        let parent = PinnedDirectory::open(path.parent().ok_or_else(corrupt)?)?;
        let name = path.file_name().ok_or_else(corrupt)?.to_os_string();
        let file = parent.open_file_read_write(&name)?.into_std();
        file.try_lock().map_err(unavailable)?;
        let native_guard = redb::backends::FileBackend::new(file.try_clone().map_err(unavailable)?)
            .map_err(unavailable)?;
        if !native_guard
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .map_err(unavailable)?
        {
            return Err(unavailable(()));
        }
        let maximum_bytes = file.metadata().map_err(unavailable)?.len();
        if maximum_bytes == 0 {
            return Err(corrupt());
        }
        let owner = Self {
            shared: Arc::new(Shared {
                parent,
                name,
                file,
                native_guard,
                maximum_bytes: AtomicU64::new(maximum_bytes),
                cancellation,
                invalid: AtomicBool::new(false),
                leases: RwLock::new(Leases::default()),
            }),
        };
        owner.verify_identity()?;
        Ok(owner)
    }

    // This module remains test-only. A production preparation constructor must
    // consume authenticated copied-artifact provenance, never a raw pathname.
    // Preparation preserves the native allocator and caller's existing bounded
    // hydration rules; the verification backend cannot invent growth headroom.
    fn capture_for_preparation(
        path: &Path,
        cancellation: Arc<AtomicBool>,
    ) -> Result<Self, StorageError> {
        let owner = Self::capture(path, cancellation)?;
        owner.shared.maximum_bytes.store(0, Ordering::Release);
        Ok(owner)
    }

    fn freeze_prepared_artifact(&self) -> Result<u64, StorageError> {
        self.verify_identity()?;
        let leases = self.shared.leases.write().map_err(|_| {
            self.shared.invalid.store(true, Ordering::Release);
            unavailable(())
        })?;
        self.shared.available().map_err(unavailable)?;
        // Observe close rather than assuming that dropping an engine completed
        // it. No backend operation can race the exact-length freeze.
        if leases.active.is_some() || !leases.ranges.is_empty() {
            return Err(unavailable(()));
        }
        if self.shared.maximum_bytes.load(Ordering::Acquire) != 0 {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let length = self
            .shared
            .observe_io(self.shared.file.metadata())
            .map_err(unavailable)?
            .len();
        if length == 0 {
            self.shared.invalid.store(true, Ordering::Release);
            return Err(corrupt());
        }
        self.shared.maximum_bytes.store(length, Ordering::Release);
        self.verify_identity()?;
        Ok(length)
    }

    fn verify_identity(&self) -> Result<(), StorageError> {
        let result = (|| {
            self.shared.available().map_err(unavailable)?;
            self.shared.parent.verify()?;
            let length = self
                .shared
                .observe_io(self.shared.file.metadata())
                .map_err(unavailable)?
                .len();
            let maximum_bytes = self.shared.maximum_bytes.load(Ordering::Acquire);
            if !self
                .shared
                .parent
                .regular_file_matches(&self.shared.name, &self.shared.file)?
                || length == 0
                || (maximum_bytes != 0 && length != maximum_bytes)
            {
                return Err(corrupt());
            }
            Ok(())
        })();
        if result.is_err() {
            self.shared.invalid.store(true, Ordering::Release);
        }
        result
    }

    fn lease(&self) -> Result<RestoreEngineBackend, StorageError> {
        self.verify_identity()?;
        let mut leases = self.shared.leases.write().map_err(|_| {
            self.shared.invalid.store(true, Ordering::Release);
            unavailable(())
        })?;
        self.shared.available().map_err(unavailable)?;
        if leases.active.is_some() || !leases.ranges.is_empty() {
            return Err(unavailable(()));
        }
        let generation = leases
            .generation
            .checked_add(1)
            .ok_or_else(|| unavailable(()))?;
        // Construct before granting the lease: descriptor failure leaves no owner.
        let descriptor = self
            .shared
            .observe_io(self.shared.file.try_clone())
            .map_err(unavailable)?;
        let file = self
            .shared
            .observe_io(redb::backends::FileBackend::new(descriptor))
            .map_err(unavailable)?;
        leases.generation = generation;
        leases.active = Some(generation);
        Ok(RestoreEngineBackend {
            file,
            shared: Arc::clone(&self.shared),
            generation,
        })
    }
}

impl Shared {
    fn observe_io<T, E>(&self, result: Result<T, E>) -> Result<T, E> {
        if result.is_err() {
            // A later successful operation, close or unchanged inode cannot
            // restore authority to an owner which observed native I/O failure.
            self.invalid.store(true, Ordering::Release);
        }
        result
    }

    fn available(&self) -> io::Result<()> {
        if self.cancellation.load(Ordering::Acquire) {
            self.invalid.store(true, Ordering::Release);
        }
        if self.invalid.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "private restore custody invalidated",
            ));
        }
        Ok(())
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // All engine descriptors retain this owner. Native exclusion ends only
        // after the last owner/engine handle is gone, even during cancellation.
        let _ = self.native_guard.close();
        let _ = self.file.unlock();
    }
}

impl RestoreEngineBackend {
    fn access(&self) -> io::Result<RwLockReadGuard<'_, Leases>> {
        let leases = self.shared.leases.read().map_err(|_| {
            self.shared.invalid.store(true, Ordering::Release);
            io::Error::other("private restore lease poisoned")
        })?;
        if leases.active != Some(self.generation) {
            return Err(closed());
        }
        self.shared.available()?;
        Ok(leases)
    }

    fn lock(&self, range: Range) -> Result<bool, BackendError> {
        interval(range)?;
        let mut leases = self.shared.leases.write().map_err(|_| {
            self.shared.invalid.store(true, Ordering::Release);
            io::Error::other("private restore lease poisoned")
        })?;
        if leases.active != Some(self.generation) {
            return Err(closed().into());
        }
        self.shared.available()?;
        if !leases.ranges.contains(&range) && leases.ranges.len() == MAX_ENGINE_RANGES {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "private restore lease capacity exhausted",
            )
            .into());
        }
        if !leases.ranges.contains(&range) {
            leases.ranges.push(range);
        }
        Ok(true)
    }

    fn capacity(&self, end: u64) -> io::Result<()> {
        let maximum_bytes = self.shared.maximum_bytes.load(Ordering::Acquire);
        if maximum_bytes != 0 && end > maximum_bytes {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "private restore artifact capacity exhausted",
            ));
        }
        Ok(())
    }
}

impl StorageBackend for RestoreEngineBackend {
    fn len(&self) -> io::Result<u64> {
        let _access = self.access()?;
        self.shared.observe_io(self.file.len())
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let _access = self.access()?;
        self.shared.observe_io(self.file.read(offset, out))
    }
    fn set_len(&self, length: u64) -> io::Result<()> {
        let _access = self.access()?;
        self.capacity(length)?;
        self.shared.observe_io(self.file.set_len(length))
    }
    fn sync_data(&self) -> io::Result<()> {
        let _access = self.access()?;
        self.shared.observe_io(self.file.sync_data())
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let _access = self.access()?;
        let end = offset
            .checked_add(u64::try_from(data.len()).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("private restore offset overflow"))?;
        self.capacity(end)?;
        self.shared.observe_io(self.file.write(offset, data))
    }
    fn close(&self) -> io::Result<()> {
        let mut leases = match self.shared.leases.write() {
            Ok(leases) => leases,
            Err(poisoned) => {
                self.shared.invalid.store(true, Ordering::Release);
                poisoned.into_inner()
            }
        };
        if leases.active == Some(self.generation) {
            leases.ranges.clear();
            leases.active = None;
        }
        // This backend never takes native locks through engine methods. Its
        // data backend still receives close, preserving redb's close contract.
        self.shared.observe_io(self.file.close())
    }
    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.lock((start, end))
    }
    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        // A shared engine sublease cannot downgrade the outer native guard.
        self.lock((start, end))
    }
    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.lock((start, end)).map(|_| ())
    }
    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.lock((start, end)).map(|_| ())
    }
    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        let mut leases = self.shared.leases.write().map_err(|_| {
            self.shared.invalid.store(true, Ordering::Release);
            io::Error::other("private restore lease poisoned")
        })?;
        if leases.active != Some(self.generation) {
            return Err(closed().into());
        }
        self.shared.available()?;
        let position = leases
            .ranges
            .iter()
            .position(|range| *range == (start, end))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private restore lease not held",
                )
            })?;
        leases.ranges.swap_remove(position);
        Ok(())
    }
    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        let query = interval((start, end))?;
        let leases = self.access()?;
        for range in &leases.ranges {
            let held = interval(*range)?;
            if query.0 < held.1 && held.0 < query.1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private restore query overlaps own lease",
                )
                .into());
            }
        }
        // The enclosing exclusive native guard prevents any foreign holder.
        Ok(false)
    }
}

impl Drop for RestoreEngineBackend {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn interval(range: Range) -> io::Result<(u128, u128)> {
    let start = match range.0 {
        Bound::Unbounded => 0,
        Bound::Included(value) => u128::from(value),
        Bound::Excluded(value) => u128::from(value) + 1,
    };
    let end = match range.1 {
        Bound::Unbounded => u128::from(u64::MAX) + 1,
        Bound::Included(value) => u128::from(value) + 1,
        Bound::Excluded(value) => u128::from(value),
    };
    if start >= end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "private restore empty lock range",
        ));
    }
    Ok((start, end))
}

fn closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "private restore engine lease closed",
    )
}
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn unavailable<T>(_: T) -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}

#[cfg(test)]
#[path = "restore_custody_tests.rs"]
mod tests;
