//! Native private materialization custody; full validation integration is pending.
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

pub(crate) struct RestoreCustody {
    shared: Arc<Shared>,
}

struct Shared {
    parent: Arc<PinnedDirectory>,
    name: RwLock<OsString>,
    file: File,
    native_guard: redb::backends::FileBackend,
    // Zero is the private preparation phase, before any validation proof.
    // A frozen verifier is bounded by the exact completed artifact length.
    maximum_bytes: AtomicU64,
    mutation_generation: AtomicU64,
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

pub(crate) struct RestoreEngineBackend {
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
    // A raw pathname is deliberately unavailable to production construction.
    #[cfg(test)]
    fn capture(path: &Path, cancellation: Arc<AtomicBool>) -> Result<Self, StorageError> {
        let parent = Arc::new(PinnedDirectory::open(path.parent().ok_or_else(corrupt)?)?);
        let name = path.file_name().ok_or_else(corrupt)?.to_os_string();
        let file = parent.open_file_read_write(&name)?.into_std();
        Self::from_created_parts(parent, name, file, cancellation, false)
    }

    pub(crate) fn from_verified_copy(
        copy: crate::backup::VerifiedCopiedPrimary,
        cancellation: Arc<AtomicBool>,
    ) -> Result<Self, StorageError> {
        let (parent, name, file) = copy.into_parts();
        Self::from_created_parts(parent, name, file, cancellation, true)
    }

    fn from_created_parts(
        parent: Arc<PinnedDirectory>,
        name: OsString,
        file: File,
        cancellation: Arc<AtomicBool>,
        preparing: bool,
    ) -> Result<Self, StorageError> {
        parent.verify()?;
        if !parent.regular_file_matches(&name, &file)? {
            return Err(corrupt());
        }
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
                name: RwLock::new(name),
                file,
                native_guard,
                maximum_bytes: AtomicU64::new(if preparing { 0 } else { maximum_bytes }),
                cancellation,
                mutation_generation: AtomicU64::new(0),
                invalid: AtomicBool::new(false),
                leases: RwLock::new(Leases::default()),
            }),
        };
        owner.verify_identity()?;
        Ok(owner)
    }

    // Raw-path preparation remains available only to native protocol tests.
    // Preparation preserves the native allocator and caller's existing bounded
    // hydration rules; the verification backend cannot invent growth headroom.
    #[cfg(test)]
    fn capture_for_preparation(
        path: &Path,
        cancellation: Arc<AtomicBool>,
    ) -> Result<Self, StorageError> {
        let owner = Self::capture(path, cancellation)?;
        owner.shared.maximum_bytes.store(0, Ordering::Release);
        Ok(owner)
    }

    pub(crate) fn cancellation(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shared.cancellation)
    }

    pub(crate) fn freeze_prepared_artifact(&self) -> Result<u64, StorageError> {
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

    pub(crate) fn invalidate(&self) {
        self.shared.invalid.store(true, Ordering::Release);
    }

    pub(crate) fn verify_identity(&self) -> Result<(), StorageError> {
        let result = (|| {
            self.shared.available().map_err(unavailable)?;
            self.shared.parent.verify()?;
            let length = self
                .shared
                .observe_io(self.shared.file.metadata())
                .map_err(unavailable)?
                .len();
            let maximum_bytes = self.shared.maximum_bytes.load(Ordering::Acquire);
            let name = self.shared.name.read().map_err(|_| unavailable(()))?;
            if !self
                .shared
                .parent
                .regular_file_matches(&name, &self.shared.file)?
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

    /// The native factory performs its known primary rename under the same
    /// owner. No caller may merely replace the identity after a path change.
    pub(crate) fn rename_created_primary(
        &self,
        source: &Path,
        destination: &Path,
    ) -> Result<(), StorageError> {
        let result = (|| {
            self.verify_identity()?;
            if source.parent() != destination.parent() {
                return Err(corrupt());
            }
            let source_name = source.file_name().ok_or_else(corrupt)?;
            let destination_name = destination.file_name().ok_or_else(corrupt)?;
            let mut name = self.shared.name.write().map_err(|_| unavailable(()))?;
            if name.as_os_str() != source_name {
                return Err(corrupt());
            }
            if !self
                .shared
                .parent
                .regular_file_matches(source_name, &self.shared.file)?
            {
                return Err(corrupt());
            }
            self.shared.parent.rename(source_name, destination_name)?;
            *name = destination_name.to_os_string();
            drop(name);
            self.verify_identity()
        })();
        if result.is_err() {
            self.shared.invalid.store(true, Ordering::Release);
        }
        result
    }

    pub(crate) fn lease(&self) -> Result<RestoreEngineBackend, StorageError> {
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
    fn note_mutation(&self) -> io::Result<()> {
        if self
            .mutation_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .is_err()
        {
            self.invalid.store(true, Ordering::Release);
            return Err(io::Error::other(
                "private restore mutation generation exhausted",
            ));
        }
        Ok(())
    }

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

/// Bound to the actual backend, original file and engine lease. Only a real
/// clean physical pass can set its validated mutation generation.
pub(crate) struct RestoreIntegrityBinding {
    shared: Arc<Shared>,
    engine_generation: u64,
    validated_mutation: Option<u64>,
}

impl RestoreIntegrityBinding {
    fn verify_owner(&self) -> Result<(), StorageError> {
        let result = (|| {
            RestoreCustody {
                shared: Arc::clone(&self.shared),
            }
            .verify_identity()?;
            let leases = self.shared.leases.read().map_err(|_| unavailable(()))?;
            if leases.active != Some(self.engine_generation) {
                return Err(corrupt());
            }
            Ok(())
        })();
        if result.is_err() {
            self.shared.invalid.store(true, Ordering::Release);
        }
        result
    }

    /// Called with the uniquely retained engine, before any validation read.
    /// A caller-supplied successful check or receipt cannot mint this binding.
    pub(crate) fn check_integrity(
        &mut self,
        database: &mut redb::Database,
    ) -> Result<(), StorageError> {
        self.verify_owner()?;
        match database.check_integrity() {
            Ok(true) => {}
            Ok(false) => {
                self.shared.invalid.store(true, Ordering::Release);
                return Err(corrupt());
            }
            Err(error) => {
                self.shared.invalid.store(true, Ordering::Release);
                return Err(crate::error::database_error(error));
            }
        }
        self.verify_owner()?;
        self.validated_mutation = Some(self.shared.mutation_generation.load(Ordering::Acquire));
        self.verify()
    }

    pub(crate) fn verify(&self) -> Result<(), StorageError> {
        self.verify_owner()?;
        if self.validated_mutation != Some(self.shared.mutation_generation.load(Ordering::Acquire))
        {
            self.shared.invalid.store(true, Ordering::Release);
            return Err(corrupt());
        }
        Ok(())
    }
}

impl RestoreEngineBackend {
    pub(crate) fn integrity_binding(&self) -> Result<RestoreIntegrityBinding, StorageError> {
        let _access = self.access().map_err(unavailable)?;
        Ok(RestoreIntegrityBinding {
            shared: Arc::clone(&self.shared),
            engine_generation: self.generation,
            validated_mutation: None,
        })
    }

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
        self.shared.note_mutation()?;
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
        self.shared.note_mutation()?;
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
pub(crate) mod tests;
