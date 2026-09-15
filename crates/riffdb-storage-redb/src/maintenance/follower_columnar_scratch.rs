//! Exclusive disposable follower build files, with no database or control ports.
use super::path_guard::PinnedDirectory;
use riffdb_storage_api::{StorageError, StorageErrorKind};
use std::{
    ffi::OsStr,
    fs::File,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
};

const AREA: &str = "follower-columnar";
const LOCK: &str = "inventory.lock";
const BUILD: &str = "build";
const GENERATION: &str = "generation-0000000000000001";
const TEMPORARY: &str = "generation-0000000000000001.tmp";
const SCRATCH: &str = ".rebuild-scratch-v1";

/// One worker's exclusively locked, bounded disposable V2 build area. File
/// ceilings come from the columnar format owner. No file is reopened as a view.
/// Opening checks only the fixed top-level inventory, never a generation.
pub struct RedbFollowerColumnarScratch {
    path: PathBuf,
    directory: PinnedDirectory,
    lock: File,
    maximum_files: NonZeroUsize,
    maximum_bytes: NonZeroU64,
}

/// Borrowed exclusive build lease. Call `discard` after the builder releases its
/// file handles, before installing its independently owned in-memory snapshot.
/// Abandonment leaves disposable material for the next bounded `begin` cleanup.
pub struct RedbFollowerColumnarBuild<'a> {
    owner: &'a mut RedbFollowerColumnarScratch,
    directory: PinnedDirectory,
    path: PathBuf,
}

impl RedbFollowerColumnarScratch {
    /// Creates/opens only the private follower area beneath an existing root.
    /// Primary source directories and other root entries are never enumerated.
    pub fn open(
        projections_root: &Path,
        maximum_files: NonZeroUsize,
        maximum_bytes: NonZeroU64,
    ) -> Result<Self, StorageError> {
        let parent = PinnedDirectory::open(projections_root)?;
        let directory = match parent.child_directory(OsStr::new(AREA))? {
            Some(directory) => directory,
            None => parent.create_private_child(OsStr::new(AREA))?,
        };
        directory.verify_private()?;
        let entries = directory.bounded_entries(2)?;
        check_inventory(&entries)?;
        let lock = match directory.regular_file_length(OsStr::new(LOCK))? {
            Some(0) => directory.open_file_read_write(OsStr::new(LOCK))?.into_std(),
            None if entries.is_empty() => directory.create_new_file(OsStr::new(LOCK))?.into_std(),
            _ => return Err(corrupt()),
        };
        lock.try_lock().map_err(|_| unavailable())?;
        lock.sync_all().map_err(|_| unavailable())?;
        directory.sync()?;
        let owner = Self {
            path: projections_root.join(AREA),
            directory,
            lock,
            maximum_files,
            maximum_bytes,
        };
        owner.verify()?;
        Ok(owner)
    }

    /// Reclaims abandoned private bytes and creates an empty input directory.
    /// The mutable borrow prevents another build while this lease is alive.
    pub fn begin(&mut self) -> Result<RedbFollowerColumnarBuild<'_>, StorageError> {
        self.verify()?;
        if let Some(previous) = self.directory.child_directory(OsStr::new(BUILD))? {
            self.discard_directory(&previous)?;
        }
        let directory = self.directory.create_private_child(OsStr::new(BUILD))?;
        let path = self.path.join(BUILD);
        Ok(RedbFollowerColumnarBuild {
            owner: self,
            directory,
            path,
        })
    }

    fn verify(&self) -> Result<(), StorageError> {
        self.directory.verify_private()?;
        if self.lock.metadata().map_err(|_| unavailable())?.len() != 0
            || !self
                .directory
                .regular_file_matches(OsStr::new(LOCK), &self.lock)?
        {
            return Err(corrupt());
        }
        check_inventory(&self.directory.bounded_entries(2)?)
    }

    fn discard_directory(&self, directory: &PinnedDirectory) -> Result<(), StorageError> {
        self.verify()?;
        directory.verify_private()?;
        // Validate every entry and the cumulative bounds before deleting any.
        // Repeat the same bounded traversal through retained directory handles.
        for remove in [false, true] {
            let mut budget = Budget {
                files: self.maximum_files.get(),
                bytes: self.maximum_bytes.get(),
                directories: 4,
            };
            walk(directory, 0, remove, &mut budget)?;
        }
        directory.remove_self_empty()?;
        self.directory.sync()?;
        self.verify()
    }
}

impl RedbFollowerColumnarBuild<'_> {
    /// Fresh build input for the existing V2 builder. Generation one is solely
    /// a temporary format value; no replicated generation is allocated here.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Removes exactly this drained build, refusing bounds or path substitution.
    pub fn discard(self) -> Result<(), StorageError> {
        self.owner.discard_directory(&self.directory)
    }
}

struct Budget {
    files: usize,
    bytes: u64,
    directories: usize,
}
fn walk(
    directory: &PinnedDirectory,
    depth: u8,
    remove: bool,
    budget: &mut Budget,
) -> Result<(), StorageError> {
    let maximum = budget.files.saturating_add(budget.directories);
    directory.visit_entries_bounded(maximum, |name, is_directory| {
        if is_directory {
            let allowed = match depth {
                0 => name == OsStr::new(GENERATION) || name == OsStr::new(TEMPORARY),
                1 => name == OsStr::new(SCRATCH),
                _ => false,
            };
            if !allowed {
                return Err(corrupt());
            }
            budget.directories = budget.directories.checked_sub(1).ok_or_else(corrupt)?;
            let child = directory.child_directory(name)?.ok_or_else(corrupt)?;
            walk(&child, depth + 1, remove, budget)?;
            if remove {
                child.remove_self_empty()?;
            }
        } else {
            if depth == 0 {
                return Err(corrupt());
            }
            budget.files = budget.files.checked_sub(1).ok_or_else(corrupt)?;
            let length = directory.regular_file_length(name)?.ok_or_else(corrupt)?;
            budget.bytes = budget.bytes.checked_sub(length).ok_or_else(corrupt)?;
            if remove {
                directory.remove_file_if_present(name)?;
            }
        }
        Ok(())
    })
}

fn check_inventory(entries: &[(std::ffi::OsString, bool)]) -> Result<(), StorageError> {
    for (name, directory) in entries {
        if !((name == LOCK && !directory) || (name == BUILD && *directory)) {
            return Err(corrupt());
        }
    }
    Ok(())
}
fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}
fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
