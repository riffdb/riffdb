//! One private, fully allocated verification artifact under original-file custody.

use super::path_guard::{PinnedDirectory, check_current_marker};
use crate::error::{database_error, storage_error};
use redb::StorageBackend;
use riffdb_storage_api::{BackupIntegrityChecksumV1, StorageError, StorageErrorKind};
use std::{
    ffi::OsString,
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    ops::Bound,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

pub(crate) struct PhysicalScrubCopy {
    parent: PinnedDirectory,
    source_name: OsString,
    source: File,
    _source_custody: crate::native_file_custody::NativeFileCustody,
    marker_name: OsString,
    marker: File,
    directory: PinnedDirectory,
    copy: Option<File>,
    maximum_bytes: u64,
    checksum: Option<BackupIntegrityChecksumV1>,
    cancellation: Arc<AtomicBool>,
}

impl PhysicalScrubCopy {
    pub(crate) fn capture(
        path: &Path,
        cancellation: Arc<AtomicBool>,
    ) -> Result<Self, StorageError> {
        cancelled(&cancellation)?;
        let parent = PinnedDirectory::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?;
        let source_name = path.file_name().ok_or_else(corrupt)?.to_os_string();
        let source = parent.open_file_read_write(&source_name)?.into_std();
        // Exclude older flock-based engines and the reviewed range-lock engine.
        // Neither constructor nor lock acquisition opens or repairs a database.
        let source_custody = crate::native_file_custody::NativeFileCustody::acquire(
            source.try_clone().map_err(unavailable)?,
        )?;
        let marker_name = crate::durable_format_marker_path(path)
            .file_name()
            .ok_or_else(corrupt)?
            .to_os_string();
        let mut marker = parent.open_file(&marker_name)?.into_std();
        check_current_marker(&mut marker)?;
        let maximum_bytes = source.metadata().map_err(unavailable)?.len();
        if maximum_bytes == 0 {
            return Err(corrupt());
        }
        let mut directory_name = OsString::from(".");
        directory_name.push(&source_name);
        directory_name.push(".riffdb-offline-verification-v1");
        // Exclusive source custody also serializes the single scratch name.
        // A stopped verifier leaves only this bounded disposable artifact.
        if let Some(stale) = parent.child_directory(&directory_name)? {
            stale.verify_private()?;
            let entries = stale.bounded_entries(1)?;
            if let Some((name, is_directory)) = entries.first() {
                if name != "primary.redb" || *is_directory {
                    return Err(corrupt());
                }
                let file = stale.open_file(name)?.into_std();
                if stale.regular_file_matches(name, &source)?
                    || file.metadata().map_err(unavailable)?.len() > maximum_bytes
                {
                    return Err(corrupt());
                }
                stale.remove_file_if_present(name)?;
            }
            stale.remove_self_empty()?;
        }
        let directory = parent.create_private_child(&directory_name)?;
        let mut owner = Self {
            parent,
            source_name,
            source,
            _source_custody: source_custody,
            marker_name,
            marker,
            directory,
            copy: None,
            maximum_bytes,
            checksum: None,
            cancellation,
        };
        owner.verify_source()?;
        owner.copy = Some(
            owner
                .directory
                .create_new_read_write_file(std::ffi::OsStr::new("primary.redb"))?
                .into_std(),
        );
        owner.copy_source()?;
        owner.verify_source()?;
        let copy = owner.copy.as_mut().ok_or_else(corrupt)?;
        copy.seek(SeekFrom::Start(0)).map_err(unavailable)?;
        let copied_checksum = crate::backup::sha256_reader(
            CancellableReader {
                file: copy.try_clone().map_err(unavailable)?,
                cancellation: Arc::clone(&owner.cancellation),
            }
            .take(owner.maximum_bytes),
        )?;
        if copied_checksum != owner.source_checksum()? {
            return Err(corrupt());
        }
        owner.checksum = Some(copied_checksum);
        owner.check_engine()?;
        owner.verify_source()?;
        Ok(owner)
    }

    fn copy_source(&mut self) -> Result<(), StorageError> {
        self.source.seek(SeekFrom::Start(0)).map_err(unavailable)?;
        let copy = self.copy.as_mut().ok_or_else(corrupt)?;
        let mut remaining = self.maximum_bytes;
        let mut buffer = [0_u8; 64 * 1024];
        while remaining != 0 {
            cancelled(&self.cancellation)?;
            let count = usize::try_from(remaining.min(buffer.len() as u64)).map_err(unavailable)?;
            self.source
                .read_exact(&mut buffer[..count])
                .map_err(unavailable)?;
            // Dense bounded writes allocate the admitted artifact before engine
            // allocation. A sparse length change is not disk-space admission.
            copy.write_all(&buffer[..count]).map_err(unavailable)?;
            remaining -= count as u64;
        }
        copy.sync_all().map_err(unavailable)?;
        self.directory.sync()?;
        self.source.seek(SeekFrom::Start(0)).map_err(unavailable)?;
        Ok(())
    }

    fn verify_source(&mut self) -> Result<(), StorageError> {
        cancelled(&self.cancellation)?;
        self.parent.verify()?;
        if !self
            .parent
            .regular_file_matches(&self.source_name, &self.source)?
            || !self
                .parent
                .regular_file_matches(&self.marker_name, &self.marker)?
            || self.source.metadata().map_err(unavailable)?.len() != self.maximum_bytes
        {
            return Err(corrupt());
        }
        check_current_marker(&mut self.marker)
    }

    pub(crate) fn backend(&self) -> Result<VerificationBackend, StorageError> {
        let copy = self.copy.as_ref().ok_or_else(corrupt)?;
        if !self
            .directory
            .regular_file_matches(std::ffi::OsStr::new("primary.redb"), copy)?
        {
            return Err(corrupt());
        }
        Ok(VerificationBackend {
            file: redb::backends::FileBackend::new(copy.try_clone().map_err(unavailable)?)
                .map_err(database_error)?,
            maximum_bytes: self.maximum_bytes,
            cancellation: Arc::clone(&self.cancellation),
        })
    }

    fn check_engine(&self) -> Result<(), StorageError> {
        cancelled(&self.cancellation)?;
        let repaired = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&repaired);
        let mut builder = redb::Builder::new();
        builder.set_cache_size(crate::store::PRIMARY_REDB_CACHE_SIZE_BYTES);
        builder.set_repair_callback(move |session| {
            observed.store(true, Ordering::Release);
            session.abort();
        });
        let opened = builder.create_with_backend(self.backend()?);
        if repaired.load(Ordering::Acquire) {
            return Err(corrupt());
        }
        let mut database = opened.map_err(database_error)?;
        let checked = database.check_integrity();
        if repaired.load(Ordering::Acquire) {
            return Err(corrupt());
        }
        if !checked.map_err(database_error)? {
            return Err(corrupt());
        }
        drop(database);
        cancelled(&self.cancellation)
    }

    pub(crate) fn finish(mut self) -> Result<(), StorageError> {
        self.verify_source()?;
        self.source.seek(SeekFrom::Start(0)).map_err(unavailable)?;
        if self.checksum.as_ref() != Some(&self.source_checksum()?) {
            return Err(corrupt());
        }
        self.verify_source()?;
        self.cleanup()
    }

    fn source_checksum(&self) -> Result<BackupIntegrityChecksumV1, StorageError> {
        crate::backup::sha256_reader(
            CancellableReader {
                file: self.source.try_clone().map_err(unavailable)?,
                cancellation: Arc::clone(&self.cancellation),
            }
            .take(self.maximum_bytes),
        )
    }

    fn cleanup(&mut self) -> Result<(), StorageError> {
        if let Some(file) = self.copy.take() {
            if !self
                .directory
                .regular_file_matches(std::ffi::OsStr::new("primary.redb"), &file)?
            {
                self.copy = Some(file);
                return Err(corrupt());
            }
            drop(file);
            self.directory
                .remove_file_if_present(std::ffi::OsStr::new("primary.redb"))?;
        }
        self.directory.remove_self_empty()
    }
}

struct CancellableReader {
    file: File,
    cancellation: Arc<AtomicBool>,
}

impl Read for CancellableReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.cancellation.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "offline verification cancelled",
            ));
        }
        self.file.read(buffer)
    }
}

impl Drop for PhysicalScrubCopy {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

/// No engine allocation can exceed the captured and allocated artifact. Every
/// engine read observes cancellation even during its opaque checksum walk.
#[derive(Debug)]
pub(crate) struct VerificationBackend {
    file: redb::backends::FileBackend,
    maximum_bytes: u64,
    cancellation: Arc<AtomicBool>,
}

impl VerificationBackend {
    fn admitted(&self, end: u64) -> io::Result<()> {
        if self.cancellation.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "offline verification cancelled",
            ));
        }
        if end > self.maximum_bytes {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "offline verification capacity exhausted",
            ));
        }
        Ok(())
    }
}

impl StorageBackend for VerificationBackend {
    fn len(&self) -> io::Result<u64> {
        self.admitted(0)?;
        self.file.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.admitted(0)?;
        self.file.read(offset, out)
    }
    fn set_len(&self, length: u64) -> io::Result<()> {
        self.admitted(length)?;
        self.file.set_len(length)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.admitted(0)?;
        self.file.sync_data()
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let end = offset
            .checked_add(u64::try_from(data.len()).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("offline verification offset overflow"))?;
        self.admitted(end)?;
        self.file.write(offset, data)
    }
    fn close(&self) -> io::Result<()> {
        self.file.close()
    }
    fn try_lock_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, redb::BackendError> {
        self.file.try_lock_range(start, end)
    }
    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, redb::BackendError> {
        self.file.try_lock_shared_range(start, end)
    }
    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), redb::BackendError> {
        self.file.lock_range(start, end)
    }
    fn lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<(), redb::BackendError> {
        self.file.lock_shared_range(start, end)
    }
    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), redb::BackendError> {
        self.file.unlock_range(start, end)
    }
    fn query_lock_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, redb::BackendError> {
        self.file.query_lock_range(start, end)
    }
}

fn cancelled(cancellation: &AtomicBool) -> Result<(), StorageError> {
    if cancellation.load(Ordering::Acquire) {
        Err(unavailable(()))
    } else {
        Ok(())
    }
}
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn unavailable<T>(_: T) -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_storage_api::DatabaseInitializationPort;

    // req: REC-001, REC-005, STO-032
    #[test]
    fn scrub_releases_native_locks_with_an_inherited_description_on_every_exit() {
        for exit in ["drop", "finish", "cancel"] {
            let scope = crate::test_path::ScopedDirectory::new("scrub-inherited-lock");
            let path = scope.join("source.redb");
            let mut store = crate::RedbStore::open(&path).unwrap();
            store
                .initialize_database(
                    riffdb_types::DatabaseId::from_unix_milliseconds_and_random(
                        1_700_000_000_000,
                        [0x6d; 10],
                    )
                    .unwrap(),
                )
                .unwrap();
            drop(store);
            let before = std::fs::read(&path).unwrap();
            let cancellation = Arc::new(AtomicBool::new(false));
            let copy = PhysicalScrubCopy::capture(&path, Arc::clone(&cancellation)).unwrap();
            // A descriptor inherited during concurrent process creation shares
            // the same open-file description. Retain that exact ownership
            // deterministically, without a timing-dependent process schedule.
            let inherited = copy.source.try_clone().unwrap();
            let observer = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            assert!(matches!(
                observer.try_lock(),
                Err(std::fs::TryLockError::WouldBlock)
            ));
            match exit {
                "finish" => copy.finish().unwrap(),
                "cancel" => {
                    cancellation.store(true, Ordering::Release);
                    assert_eq!(
                        copy.finish().unwrap_err().kind(),
                        StorageErrorKind::Unavailable
                    );
                }
                _ => drop(copy),
            }
            observer
                .try_lock()
                .expect("completed scrub must release flock despite an inherited description");
            observer.unlock().unwrap();
            let backend = redb::backends::FileBackend::new(observer).unwrap();
            assert!(
                backend
                    .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                    .unwrap()
            );
            backend.close().unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), before);
            assert_eq!(inherited.metadata().unwrap().len(), before.len() as u64);
        }
    }
}
