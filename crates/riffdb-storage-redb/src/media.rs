//! Journal media port: the filesystem seam for the database-sibling side-file
//! family (durability journal, checkpoint, spare, and format-marker files).
//!
//! ADR-0113 Phase 1 item 2: the journal lane and the format-preflight marker
//! publication are parameterized over this port so a deterministic simulation
//! can drive them from a simulated disk. The production implementation
//! ([`RealJournalMedia`]) performs exactly the syscalls the call sites
//! performed before the seam existed, in the same order — the journal unit
//! tests pin that byte-identity by running unmodified against the wrappers
//! that default to this implementation.
//!
//! The file handle is an enum rather than an associated type so the store can
//! hold one `Arc<dyn JournalMedia>` without becoming generic: the production
//! arm carries a plain [`std::fs::File`] and every operation on it is a direct
//! (non-virtual) call, keeping the journal worker's write/sync hot loop free
//! of `dyn` dispatch. Only the simulated arm pays a virtual call.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

/// Metadata for one media path: whether it names a regular file and its
/// current length. Mirrors the two facts the format preflight reads from
/// [`std::fs::metadata`].
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MediaFileMetadata {
    /// Whether the path names a regular file.
    pub is_file: bool,
    /// Current file length in bytes.
    pub len: u64,
}

/// The dyn seam for simulated media file handles.
///
/// Production files never route through this trait: the [`MediaFile::Fs`] arm
/// calls [`std::fs::File`] directly. Sequential operations (`read`,
/// `read_exact`, `write_all`) advance a per-handle cursor; positional
/// operations (`write_all_at`, `read_exact_at`) must not move it, matching the
/// unix `pread`/`pwrite` semantics the production journal relies on.
#[doc(hidden)]
#[allow(
    clippy::len_without_is_empty,
    reason = "mirrors redb::StorageBackend; emptiness is never a media question"
)]
pub trait JournalMediaFile: std::fmt::Debug + Send {
    /// Current file length (`fstat` on the production side).
    fn len(&mut self) -> io::Result<u64>;
    /// Sequential read at the handle cursor; returns bytes read, `Ok(0)` at
    /// end of file.
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize>;
    /// Sequential exact read at the handle cursor.
    fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()>;
    /// Sequential complete write at the handle cursor, extending the file.
    fn write_all(&mut self, data: &[u8]) -> io::Result<()>;
    /// Positional complete write; does not move the sequential cursor.
    fn write_all_at(&mut self, data: &[u8], offset: usize) -> io::Result<()>;
    /// Positional exact read; does not move the sequential cursor.
    fn read_exact_at(&mut self, out: &mut [u8], offset: usize) -> io::Result<()>;
    /// Flushes file data (`fdatasync`).
    fn sync_data(&mut self) -> io::Result<()>;
    /// Flushes file data and metadata (`fsync`).
    fn sync_all(&mut self) -> io::Result<()>;
}

/// One open media file: a real [`File`] on the production path, a boxed
/// [`JournalMediaFile`] on the simulated path.
#[doc(hidden)]
#[allow(
    clippy::len_without_is_empty,
    reason = "mirrors redb::StorageBackend; emptiness is never a media question"
)]
#[derive(Debug)]
pub enum MediaFile {
    /// Production handle; operations are direct syscalls.
    Fs(File),
    /// Simulated handle behind the dyn seam.
    Port(Box<dyn JournalMediaFile>),
}

impl MediaFile {
    /// Current file length.
    pub fn len(&mut self) -> io::Result<u64> {
        match self {
            Self::Fs(file) => file.metadata().map(|metadata| metadata.len()),
            Self::Port(file) => file.len(),
        }
    }

    /// Sequential read at the handle cursor; `Ok(0)` at end of file.
    pub fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Fs(file) => Read::read(file, out),
            Self::Port(file) => file.read(out),
        }
    }

    /// Sequential exact read at the handle cursor.
    pub fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        match self {
            Self::Fs(file) => Read::read_exact(file, out),
            Self::Port(file) => file.read_exact(out),
        }
    }

    /// Sequential complete write at the handle cursor.
    pub fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        match self {
            Self::Fs(file) => Write::write_all(file, data),
            Self::Port(file) => file.write_all(data),
        }
    }

    /// Positional complete write; does not move the sequential cursor.
    pub fn write_all_at(&mut self, data: &[u8], offset: usize) -> io::Result<()> {
        match self {
            Self::Fs(file) => crate::journal::write_all_at(file, data, offset),
            Self::Port(file) => file.write_all_at(data, offset),
        }
    }

    /// Positional exact read; does not move the sequential cursor.
    pub fn read_exact_at(&mut self, out: &mut [u8], offset: usize) -> io::Result<()> {
        match self {
            Self::Fs(file) => crate::journal::read_exact_at(file, out, offset),
            Self::Port(file) => file.read_exact_at(out, offset),
        }
    }

    /// Flushes file data (`fdatasync`).
    pub fn sync_data(&mut self) -> io::Result<()> {
        match self {
            Self::Fs(file) => file.sync_data(),
            Self::Port(file) => file.sync_data(),
        }
    }

    /// Flushes file data and metadata (`fsync`).
    pub fn sync_all(&mut self) -> io::Result<()> {
        match self {
            Self::Fs(file) => file.sync_all(),
            Self::Port(file) => file.sync_all(),
        }
    }
}

/// The journal media port: every filesystem operation the durability journal,
/// its checkpoint/spare rotation, and the durable-format marker perform.
///
/// The operation set is exactly what the current call sites need — no
/// speculative surface. `io::Error` kinds are contract: `NotFound`,
/// `AlreadyExists`, and `StorageFull` drive fail-closed decisions at the call
/// sites, so implementations must preserve them.
#[doc(hidden)]
pub trait JournalMedia: std::fmt::Debug + Send + Sync {
    /// Whether the path exists (distinguishing absence from probe failure).
    fn try_exists(&self, path: &Path) -> io::Result<bool>;
    /// File-kind and length metadata (`NotFound` when absent).
    fn metadata(&self, path: &Path) -> io::Result<MediaFileMetadata>;
    /// Opens an existing file read-only.
    fn open_read(&self, path: &Path) -> io::Result<MediaFile>;
    /// Opens an existing file read-write.
    fn open_read_write(&self, path: &Path) -> io::Result<MediaFile>;
    /// Creates a new file read-write, failing with `AlreadyExists` if present
    /// (extent preallocation).
    fn create_new_read_write(&self, path: &Path) -> io::Result<MediaFile>;
    /// Creates a new file write-only, failing with `AlreadyExists` if present
    /// (marker staging).
    fn create_new_write_only(&self, path: &Path) -> io::Result<MediaFile>;
    /// Atomically renames `from` over `to`.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Removes a file (`NotFound` when absent).
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    /// Opens the parent directory and flushes it with `fdatasync`.
    fn sync_parent_data(&self, path: &Path) -> io::Result<()>;
    /// Opens the parent directory and flushes it with `fsync`.
    fn sync_parent_all(&self, path: &Path) -> io::Result<()>;
}

/// The production journal media: plain `std::fs` with exactly the syscalls
/// and flag sets the call sites used before the port existed.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct RealJournalMedia;

fn parent_of(path: &Path) -> io::Result<&Path> {
    path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "media path has no parent directory",
        )
    })
}

impl JournalMedia for RealJournalMedia {
    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }

    fn metadata(&self, path: &Path) -> io::Result<MediaFileMetadata> {
        std::fs::metadata(path).map(|metadata| MediaFileMetadata {
            is_file: metadata.is_file(),
            len: metadata.len(),
        })
    }

    fn open_read(&self, path: &Path) -> io::Result<MediaFile> {
        File::open(path).map(MediaFile::Fs)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map(MediaFile::Fs)
    }

    fn create_new_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map(MediaFile::Fs)
    }

    fn create_new_write_only(&self, path: &Path) -> io::Result<MediaFile> {
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map(MediaFile::Fs)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn sync_parent_data(&self, path: &Path) -> io::Result<()> {
        File::open(parent_of(path)?).and_then(|directory| directory.sync_data())
    }

    fn sync_parent_all(&self, path: &Path) -> io::Result<()> {
        File::open(parent_of(path)?).and_then(|directory| directory.sync_all())
    }
}

/// Simulated storage media bundle for the hidden test-only open: a redb
/// engine backend plus a journal media instance, so a simulated open is fully
/// simulated from the first filesystem touch (format preflight) onward.
#[doc(hidden)]
pub struct RedbStorageMedia {
    pub(crate) engine: Box<dyn redb::StorageBackend>,
    pub(crate) journal: std::sync::Arc<dyn JournalMedia>,
}

impl RedbStorageMedia {
    /// Bundles an engine backend with the journal media that must serve every
    /// side-file operation of the same open.
    #[doc(hidden)]
    #[must_use]
    pub fn new(
        engine: impl redb::StorageBackend,
        journal: std::sync::Arc<dyn JournalMedia>,
    ) -> Self {
        Self {
            engine: Box::new(engine),
            journal,
        }
    }
}

/// Delegating adapter so an already-boxed backend can flow into
/// `redb::Builder::create_with_backend` (which takes `impl StorageBackend`).
#[derive(Debug)]
pub(crate) struct DynStorageBackend(pub(crate) Box<dyn redb::StorageBackend>);

impl redb::StorageBackend for DynStorageBackend {
    fn len(&self) -> Result<u64, io::Error> {
        self.0.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        self.0.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.0.set_len(len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.0.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.0.write(offset, data)
    }

    fn close(&self) -> Result<(), io::Error> {
        self.0.close()
    }
}
