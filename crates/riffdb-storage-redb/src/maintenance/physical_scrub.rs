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
    source_backend: redb::backends::FileBackend,
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
        source.try_lock().map_err(unavailable)?;
        let source_backend =
            redb::backends::FileBackend::new(source.try_clone().map_err(unavailable)?)
                .map_err(database_error)?;
        if !source_backend
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .map_err(unavailable)?
        {
            return Err(unavailable(()));
        }
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
            source_backend,
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
        let _ = self.source_backend.close();
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
