use std::ffi::OsStr;
use std::fs::{self, File};
use std::path::{Component, Path, PathBuf};

use cap_std::ambient_authority;
use cap_std::fs::{Dir, File as CapFile, OpenOptions as CapOpenOptions};
use riffdb_storage_api::{StorageError, StorageErrorKind};

use crate::error::storage_error;

#[cfg(unix)]
use cap_std::fs::MetadataExt as CapMetadataExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as StdMetadataExt;

/// A directory handle and identity retained across maintenance path operations.
pub(super) struct PinnedDirectory {
    path: PathBuf,
    canonical_path: PathBuf,
    directory: Dir,
    sync_handle: File,
    #[cfg(unix)]
    identity: UnixFileIdentity,
}

impl PinnedDirectory {
    pub(super) fn open(path: &Path) -> Result<Self, StorageError> {
        reject_symlink_components(path)?;
        let path_metadata = fs::symlink_metadata(path).map_err(io_unavailable)?;
        if !path_metadata.file_type().is_dir() {
            return Err(corrupt());
        }
        let canonical_path = fs::canonicalize(path).map_err(io_unavailable)?;
        let directory = Dir::open_ambient_dir(path, ambient_authority()).map_err(io_unavailable)?;
        let retained_metadata = directory.dir_metadata().map_err(io_unavailable)?;
        if !retained_metadata.is_dir() {
            return Err(corrupt());
        }
        let sync_handle = File::open(path).map_err(io_unavailable)?;
        let sync_metadata = sync_handle.metadata().map_err(io_unavailable)?;
        if !sync_metadata.file_type().is_dir() {
            return Err(corrupt());
        }

        #[cfg(unix)]
        {
            if UnixFileIdentity::from_cap_metadata(&retained_metadata)
                != UnixFileIdentity::from_std_metadata(&path_metadata)
                || UnixFileIdentity::from_std_metadata(&sync_metadata)
                    != UnixFileIdentity::from_std_metadata(&path_metadata)
            {
                return Err(corrupt());
            }
            let identity = UnixFileIdentity::from_cap_metadata(&retained_metadata);
            Ok(Self {
                path: path.to_path_buf(),
                canonical_path,
                directory,
                sync_handle,
                identity,
            })
        }

        #[cfg(not(unix))]
        {
            Ok(Self {
                path: path.to_path_buf(),
                canonical_path,
                directory,
                sync_handle,
            })
        }
    }

    pub(super) fn verify(&self) -> Result<(), StorageError> {
        reject_symlink_components(&self.path)?;
        let metadata = fs::symlink_metadata(&self.path).map_err(io_unavailable)?;
        if !metadata.file_type().is_dir()
            || fs::canonicalize(&self.path).map_err(io_unavailable)? != self.canonical_path
        {
            return Err(corrupt());
        }

        self.verify_retained()?;

        #[cfg(unix)]
        {
            if UnixFileIdentity::from_std_metadata(&metadata) != self.identity {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    /// Verifies the retained directory object without resolving its ambient
    /// pathname.
    pub(super) fn verify_retained(&self) -> Result<(), StorageError> {
        let retained = self.directory.dir_metadata().map_err(io_unavailable)?;
        if !retained.is_dir() {
            return Err(corrupt());
        }
        let sync_metadata = self.sync_handle.metadata().map_err(io_unavailable)?;
        if !sync_metadata.file_type().is_dir() {
            return Err(corrupt());
        }

        #[cfg(unix)]
        if UnixFileIdentity::from_cap_metadata(&retained) != self.identity
            || UnixFileIdentity::from_std_metadata(&sync_metadata) != self.identity
        {
            return Err(corrupt());
        }
        Ok(())
    }

    /// Returns the length of one direct regular-file child.
    ///
    /// `None` means that no directory entry exists. Symlinks and every other
    /// file type fail closed.
    pub(super) fn regular_file_length(&self, name: &OsStr) -> Result<Option<u64>, StorageError> {
        let relative = checked_child_name(name)?;
        match self.directory.symlink_metadata(relative) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_unavailable(error)),
            Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
            Ok(_) => Err(corrupt()),
        }
    }

    /// Opens one direct regular-file child through the retained capability.
    pub(super) fn open_file(&self, name: &OsStr) -> Result<CapFile, StorageError> {
        self.open_file_with(name, CapOpenOptions::new().read(true))
    }

    /// Opens one direct regular-file child read/write through the retained
    /// capability.
    pub(super) fn open_file_read_write(&self, name: &OsStr) -> Result<CapFile, StorageError> {
        self.open_file_with(name, CapOpenOptions::new().read(true).write(true))
    }

    /// Checks whether one direct regular-file child is still the retained
    /// open file.
    pub(super) fn regular_file_matches(
        &self,
        name: &OsStr,
        retained_file: &File,
    ) -> Result<bool, StorageError> {
        let relative = checked_child_name(name)?;
        let entry = match self.directory.symlink_metadata(relative) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(io_unavailable(error)),
            Ok(metadata) => metadata,
        };
        let retained = cap_std::fs::Metadata::from_file(retained_file).map_err(io_unavailable)?;
        if !entry.is_file() || !retained.is_file() {
            return Ok(false);
        }

        #[cfg(unix)]
        return Ok(UnixFileIdentity::from_cap_metadata(&entry)
            == UnixFileIdentity::from_cap_metadata(&retained));

        #[cfg(not(unix))]
        {
            // The Linux POC target has stable device/inode identity. Other
            // targets retain capability confinement but conservatively compare
            // the observable regular-file shape.
            Ok(entry.len() == retained.len())
        }
    }

    /// Creates one new direct regular-file child through the retained
    /// capability.
    pub(super) fn create_new_file(&self, name: &OsStr) -> Result<CapFile, StorageError> {
        let relative = checked_child_name(name)?;
        self.directory
            .open_with(relative, CapOpenOptions::new().write(true).create_new(true))
            .map_err(io_unavailable)
    }

    /// Atomically renames one direct child over another within this retained
    /// directory.
    pub(super) fn rename(&self, from: &OsStr, to: &OsStr) -> Result<(), StorageError> {
        let from = checked_child_name(from)?;
        let to = checked_child_name(to)?;
        self.directory
            .rename(from, &self.directory, to)
            .map_err(io_unavailable)
    }

    /// Removes one direct regular-file child if present.
    pub(super) fn remove_file_if_present(&self, name: &OsStr) -> Result<bool, StorageError> {
        let relative = checked_child_name(name)?;
        match self.directory.symlink_metadata(relative) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(io_unavailable(error)),
            Ok(metadata) if metadata.is_file() => {
                self.directory
                    .remove_file(relative)
                    .map_err(io_unavailable)?;
                Ok(true)
            }
            Ok(_) => Err(corrupt()),
        }
    }

    /// Recursively removes this exact retained directory object.
    ///
    /// The capability implementation walks and removes the open directory by
    /// identity, so a same-name replacement in its parent is never selected.
    pub(super) fn remove_self_all(&self) -> Result<(), StorageError> {
        self.verify_retained()?;
        self.directory
            .try_clone()
            .and_then(Dir::remove_open_dir_all)
            .map_err(io_unavailable)
    }

    /// Synchronizes this exact retained directory, independent of its ambient
    /// pathname.
    pub(super) fn sync(&self) -> Result<(), StorageError> {
        self.sync_handle.sync_all().map_err(io_unavailable)
    }

    fn open_file_with(
        &self,
        name: &OsStr,
        options: &CapOpenOptions,
    ) -> Result<CapFile, StorageError> {
        let relative = checked_child_name(name)?;
        let before = self
            .directory
            .symlink_metadata(relative)
            .map_err(io_unavailable)?;
        if !before.is_file() {
            return Err(corrupt());
        }
        let file = self
            .directory
            .open_with(relative, options)
            .map_err(io_unavailable)?;
        let after = self
            .directory
            .symlink_metadata(relative)
            .map_err(io_unavailable)?;
        let retained = file.metadata().map_err(io_unavailable)?;
        if !after.is_file() || !retained.is_file() {
            return Err(corrupt());
        }

        #[cfg(unix)]
        if UnixFileIdentity::from_cap_metadata(&before)
            != UnixFileIdentity::from_cap_metadata(&retained)
            || UnixFileIdentity::from_cap_metadata(&after)
                != UnixFileIdentity::from_cap_metadata(&retained)
        {
            return Err(corrupt());
        }
        Ok(file)
    }
}

pub(super) fn verify_regular_file_path(file: &File, path: &Path) -> Result<(), StorageError> {
    reject_symlink_components(path)?;
    let path_metadata = fs::symlink_metadata(path).map_err(io_unavailable)?;
    let handle_metadata = file.metadata().map_err(io_unavailable)?;
    if !path_metadata.file_type().is_file()
        || !handle_metadata.file_type().is_file()
        || path_metadata.len() != 0
    {
        return Err(corrupt());
    }

    #[cfg(unix)]
    if UnixFileIdentity::from_std_metadata(&path_metadata)
        != UnixFileIdentity::from_std_metadata(&handle_metadata)
    {
        return Err(corrupt());
    }
    Ok(())
}

fn checked_child_name(name: &OsStr) -> Result<&Path, StorageError> {
    let path = Path::new(name);
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err(corrupt());
    }
    Ok(path)
}

fn reject_symlink_components(path: &Path) -> Result<(), StorageError> {
    let mut prefix = PathBuf::new();
    for component in path.components() {
        prefix.push(component.as_os_str());
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {}
            Component::CurDir | Component::ParentDir => return Err(corrupt()),
        }
        match fs::symlink_metadata(&prefix) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Err(corrupt()),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(io_unavailable(error)),
        }
    }
    Ok(())
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct UnixFileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl UnixFileIdentity {
    fn from_std_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: StdMetadataExt::dev(metadata),
            inode: StdMetadataExt::ino(metadata),
        }
    }

    fn from_cap_metadata(metadata: &cap_std::fs::Metadata) -> Self {
        Self {
            device: CapMetadataExt::dev(metadata),
            inode: CapMetadataExt::ino(metadata),
        }
    }
}

fn io_unavailable(_error: std::io::Error) -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
