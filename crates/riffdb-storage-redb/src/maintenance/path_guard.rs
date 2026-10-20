use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use cap_std::ambient_authority;
use cap_std::fs::{Dir, File as CapFile, OpenOptions as CapOpenOptions};
use riffdb_storage_api::{StorageError, StorageErrorKind};

use crate::error::storage_error;

#[path = "path_guard/source_verification.rs"]
mod source_verification;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) use source_verification::SourceParentVerificationWorkV1;

#[cfg(unix)]
#[derive(Clone, Copy)]
pub(super) struct SourceChildIdentityV1 {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
use cap_std::fs::MetadataExt as CapMetadataExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as StdMetadataExt;

/// A directory handle and identity retained across maintenance path operations.
pub(crate) struct PinnedDirectory {
    path: PathBuf,
    canonical_path: PathBuf,
    directory: Dir,
    sync_handle: File,
    #[cfg(unix)]
    identity: UnixFileIdentity,
}

impl PinnedDirectory {
    pub(super) fn same_directory(&self, other: &Self) -> Result<bool, StorageError> {
        self.verify()?;
        other.verify()?;
        #[cfg(unix)]
        return Ok(self.identity == other.identity);
        #[cfg(not(unix))]
        Ok(self.canonical_path == other.canonical_path)
    }

    /// Creates a private direct child before any unredacted bootstrap bytes
    /// exist. The parent capability and both path identities remain checked.
    #[cfg(unix)]
    pub(super) fn create_private_child(&self, name: &OsStr) -> Result<Self, StorageError> {
        self.verify()?;
        let relative = checked_child_name(name)?;
        let mut builder = cap_std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use cap_std::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        self.directory
            .create_dir_with(relative, &builder)
            .map_err(io_unavailable)?;
        self.sync()?;
        self.verify()?;
        let child = Self::open(&self.path.join(relative))?;
        child.verify_private()?;
        Ok(child)
    }

    #[cfg(not(unix))]
    pub(super) fn create_private_child(&self, _name: &OsStr) -> Result<Self, StorageError> {
        // No unredacted stage may be created without an enforced private ACL.
        Err(storage_error(StorageErrorKind::IncompatibleFormat))
    }

    #[cfg(not(unix))]
    pub(super) fn verify_private(&self) -> Result<(), StorageError> {
        Err(storage_error(StorageErrorKind::IncompatibleFormat))
    }

    #[cfg(unix)]
    pub(super) fn verify_private(&self) -> Result<(), StorageError> {
        self.verify()?;
        #[cfg(unix)]
        {
            use cap_std::fs::PermissionsExt;
            if self
                .directory
                .dir_metadata()
                .map_err(io_unavailable)?
                .permissions()
                .mode()
                & 0o077
                != 0
            {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    pub(crate) fn open(path: &Path) -> Result<Self, StorageError> {
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

    pub(crate) fn verify(&self) -> Result<(), StorageError> {
        self.verify_with_backend(&mut source_verification::OrdinaryVerificationBackend::default())
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

    /// Reads at most maximum + 1 direct entries and refuses every other file
    /// kind. No recursive inventory walk or population-sized allocation occurs.
    pub(super) fn bounded_entries(
        &self,
        maximum: usize,
    ) -> Result<Vec<(std::ffi::OsString, bool)>, StorageError> {
        self.verify()?;
        let mut entries = Vec::new();
        for entry in self
            .directory
            .entries()
            .map_err(io_unavailable)?
            .take(maximum.saturating_add(1))
        {
            let entry = entry.map_err(io_unavailable)?;
            let kind = entry.file_type().map_err(io_unavailable)?;
            if entries.len() == maximum || !(kind.is_file() || kind.is_dir()) {
                return Err(corrupt());
            }
            entries.push((entry.file_name(), kind.is_dir()));
        }
        self.verify()?;
        Ok(entries)
    }

    /// Streams a bounded direct inventory through the retained capability.
    /// Large disposable generations need no population-sized metadata vector.
    pub(super) fn visit_entries_bounded(
        &self,
        maximum: usize,
        mut visit: impl FnMut(&OsStr, bool) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        self.verify()?;
        for (index, entry) in self
            .directory
            .entries()
            .map_err(io_unavailable)?
            .take(maximum.saturating_add(1))
            .enumerate()
        {
            let entry = entry.map_err(io_unavailable)?;
            let kind = entry.file_type().map_err(io_unavailable)?;
            if index == maximum || !(kind.is_file() || kind.is_dir()) {
                return Err(corrupt());
            }
            visit(&entry.file_name(), kind.is_dir())?;
        }
        self.verify()
    }

    pub(super) fn child_directory(&self, name: &OsStr) -> Result<Option<Self>, StorageError> {
        self.verify()?;
        let relative = checked_child_name(name)?;
        match self.directory.symlink_metadata(relative) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_unavailable(error)),
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(corrupt()),
        }
        let child = Self::open(&self.path.join(relative))?;
        self.verify()?;
        child.verify()?;
        Ok(Some(child))
    }

    /// Removes only this empty retained directory, never recursively.
    pub(super) fn remove_self_empty(&self) -> Result<(), StorageError> {
        self.verify()?;
        self.directory
            .try_clone()
            .and_then(Dir::remove_open_dir)
            .map_err(io_unavailable)
    }

    /// Returns the length of one direct regular-file child.
    ///
    /// `None` means that no directory entry exists. Symlinks and every other
    /// file type fail closed.
    pub(crate) fn regular_file_length(&self, name: &OsStr) -> Result<Option<u64>, StorageError> {
        let relative = checked_child_name(name)?;
        match self.directory.symlink_metadata(relative) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_unavailable(error)),
            Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
            Ok(_) => Err(corrupt()),
        }
    }

    /// Opens one direct regular-file child through the retained capability.
    pub(crate) fn open_file(&self, name: &OsStr) -> Result<CapFile, StorageError> {
        self.open_file_with(name, CapOpenOptions::new().read(true))
    }

    /// Opens one direct regular-file child read/write through the retained
    /// capability.
    pub(super) fn open_file_read_write(&self, name: &OsStr) -> Result<CapFile, StorageError> {
        self.open_file_with(name, CapOpenOptions::new().read(true).write(true))
    }

    /// Checks whether one direct regular-file child is still the retained
    /// open file.
    pub(crate) fn regular_file_matches(
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

    pub(super) fn create_new_read_write_file(&self, name: &OsStr) -> Result<CapFile, StorageError> {
        let relative = checked_child_name(name)?;
        self.directory
            .open_with(
                relative,
                CapOpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true),
            )
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

    // These Source-only single-child syscalls take an ALREADY-ADMITTED CStr.
    // rustix's &CStr Arg borrows it directly: no path resolver/Vec/CString is
    // allocated inside openat/statat, including error paths. Ordinary methods
    // above and below retain their existing cap-std behavior unchanged.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    fn source_child_stat(
        &self,
        name: &std::ffi::CStr,
    ) -> Result<Option<rustix::fs::Stat>, StorageError> {
        use std::os::fd::AsFd;
        use std::os::unix::ffi::OsStrExt;
        self.verify_retained()?;
        checked_child_name(OsStr::from_bytes(name.to_bytes()))?;
        match rustix::fs::statat(
            self.directory.as_fd(),
            name,
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Err(rustix::io::Errno::NOENT) => Ok(None),
            Err(_) => Err(storage_error(StorageErrorKind::Unavailable)),
            Ok(stat)
                if rustix::fs::FileType::from_raw_mode(stat.st_mode)
                    == rustix::fs::FileType::RegularFile =>
            {
                Ok(Some(stat))
            }
            Ok(_) => Err(corrupt()),
        }
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub(super) fn source_regular_file_length(
        &self,
        name: &std::ffi::CStr,
    ) -> Result<Option<u64>, StorageError> {
        self.source_child_stat(name)?
            .map(|stat| u64::try_from(stat.st_size).map_err(|_| corrupt()))
            .transpose()
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub(super) fn source_open_file(&self, name: &std::ffi::CStr) -> Result<File, StorageError> {
        use std::os::fd::AsFd;
        let before = self.source_child_stat(name)?.ok_or_else(corrupt)?;
        let file = File::from(
            rustix::fs::openat(
                self.directory.as_fd(),
                name,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::CLOEXEC
                    | rustix::fs::OFlags::NOFOLLOW,
                rustix::fs::Mode::empty(),
            )
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?,
        );
        let retained =
            rustix::fs::fstat(&file).map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let after = self.source_child_stat(name)?.ok_or_else(corrupt)?;
        if rustix::fs::FileType::from_raw_mode(retained.st_mode)
            != rustix::fs::FileType::RegularFile
            || (before.st_dev, before.st_ino) != (retained.st_dev, retained.st_ino)
            || (after.st_dev, after.st_ino) != (retained.st_dev, retained.st_ino)
        {
            return Err(corrupt());
        }
        Ok(file)
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub(super) fn capture_source_child_identity(
        &self,
        name: &std::ffi::CStr,
        file: &File,
    ) -> Result<SourceChildIdentityV1, StorageError> {
        let entry = self.source_child_stat(name)?.ok_or_else(corrupt)?;
        let retained =
            rustix::fs::fstat(file).map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        if rustix::fs::FileType::from_raw_mode(retained.st_mode)
            != rustix::fs::FileType::RegularFile
            || (entry.st_dev, entry.st_ino) != (retained.st_dev, retained.st_ino)
        {
            return Err(corrupt());
        }
        Ok(SourceChildIdentityV1 {
            device: retained.st_dev,
            inode: retained.st_ino,
        })
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub(super) fn source_child_still_matches(
        &self,
        name: &std::ffi::CStr,
        actual: SourceChildIdentityV1,
    ) -> Result<bool, StorageError> {
        Ok(self
            .source_child_stat(name)?
            .is_some_and(|stat| (stat.st_dev, stat.st_ino) == (actual.device, actual.inode)))
    }
    pub(crate) fn retained_clone(&self) -> Result<Self, StorageError> {
        self.verify()?;
        let retained = Self {
            path: self.path.clone(),
            canonical_path: self.canonical_path.clone(),
            directory: self.directory.try_clone().map_err(io_unavailable)?,
            sync_handle: self.sync_handle.try_clone().map_err(io_unavailable)?,
            #[cfg(unix)]
            identity: self.identity,
        };
        retained.verify()?;
        self.verify()?;
        Ok(retained)
    }

    pub(crate) fn child_path(&self, name: &OsStr) -> Result<PathBuf, StorageError> {
        self.verify()?;
        Ok(self.path.join(checked_child_name(name)?))
    }

    /// The transfer preflight cannot fall back after durable intent creation.
    pub(crate) fn same_filesystem(&self, other: &Self) -> Result<bool, StorageError> {
        self.verify()?;
        other.verify()?;
        #[cfg(unix)]
        return Ok(self.identity.device == other.identity.device);
        #[cfg(not(unix))]
        Ok(false)
    }

    /// Moves one direct child through two retained directory capabilities.
    /// The protocol retains original descriptors and owns seal verification.
    pub(crate) fn rename_to(
        &self,
        from: &OsStr,
        target: &Self,
        to: &OsStr,
    ) -> Result<(), StorageError> {
        self.verify()?;
        target.verify()?;
        if !self.same_filesystem(target)? || target.regular_file_length(to)?.is_some() {
            return Err(corrupt());
        }
        // Atomically refuse a destination that appeared after preflight. This
        // uses the already required safe rustix API and retained directory FDs;
        // a competing file must never be overwritten by the move.
        #[cfg(target_os = "linux")]
        rustix::fs::renameat_with(
            &self.directory,
            checked_child_name(from)?,
            &target.directory,
            checked_child_name(to)?,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(|error| io_unavailable(error.into()))?;
        #[cfg(not(target_os = "linux"))]
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        self.verify()?;
        target.verify()
    }

    /// Links one direct child into another retained directory. The caller owns
    /// file identity checks and publication sync; cross-filesystem links refuse.
    pub(super) fn hard_link_to(
        &self,
        from: &OsStr,
        target: &Self,
        to: &OsStr,
    ) -> Result<(), StorageError> {
        self.verify()?;
        target.verify()?;
        self.directory
            .hard_link(
                checked_child_name(from)?,
                &target.directory,
                checked_child_name(to)?,
            )
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
    pub(crate) fn sync(&self) -> Result<(), StorageError> {
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

/// Checks the exact current marker through the retained descriptor with a fixed
/// read ceiling. Repeated checks never depend on a shared prior file offset.
pub(super) fn check_current_marker(file: &mut File) -> Result<(), StorageError> {
    let expected = riffdb_storage_api::encode_durable_format_marker(
        riffdb_storage_api::current_durable_format_marker(),
    );
    if file.metadata().map_err(io_unavailable)?.len() != expected.len() as u64 {
        return Err(corrupt());
    }
    let mut actual = [0; riffdb_storage_api::DURABLE_FORMAT_MARKER_BYTES];
    file.seek(SeekFrom::Start(0)).map_err(io_unavailable)?;
    file.read_exact(&mut actual).map_err(io_unavailable)?;
    if actual != expected {
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
    source_verification::reject_symlink_components_with(
        path,
        &mut source_verification::OrdinaryVerificationBackend::default(),
    )
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

impl PinnedDirectory {
    pub(crate) fn inspect_source_descriptor_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1<'_>,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        use crate::source_descriptor_capacity as capacity;
        let Self {
            path,
            canonical_path,
            directory,
            sync_handle,
            #[cfg(unix)]
            identity,
        } = self;
        capacity::path(inspection, path)?;
        capacity::path(inspection, canonical_path)?;
        capacity::directory(inspection, directory)?;
        capacity::file(inspection, sync_handle)?;
        #[cfg(unix)]
        inspection.inline(identity)?;
        Ok(())
    }
}
