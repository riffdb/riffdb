//! Directory-anchored private custody for the destination engine.

use crate::LocalArchiveError;
use cap_std::fs::Dir;
use std::fs::File;
use std::path::{Path, PathBuf};

const ARCHIVE_FILE: &str = "archive.redb";
const MAX_COMPONENTS: usize = 32;

pub(crate) struct ArchiveCustody {
    directory: Dir,
    path: PathBuf,
    file: File,
    owner: u32,
    directory_identity: (u64, u64),
    file_identity: (u64, u64),
}

impl ArchiveCustody {
    pub(crate) fn open(path: &Path, create: bool) -> Result<Self, LocalArchiveError> {
        #[cfg(target_os = "linux")]
        {
            use cap_std::fs::{MetadataExt, OpenOptions, OpenOptionsExt};
            use std::os::unix::fs::MetadataExt as _;
            let owner = effective_uid()?;
            let directory = open_directory(path, owner)?;
            let metadata = directory.dir_metadata().map_err(unavailable)?;
            let directory_identity = (metadata.dev(), metadata.ino());
            for (index, entry) in directory.entries().map_err(unavailable)?.enumerate() {
                let entry = entry.map_err(unavailable)?;
                if index > 0
                    || entry.file_name() != ARCHIVE_FILE
                    || !entry.file_type().map_err(unavailable)?.is_file()
                {
                    return Err(LocalArchiveError::UnsafePath);
                }
            }
            let present = match directory.symlink_metadata(ARCHIVE_FILE) {
                Ok(metadata) => {
                    check_cap_file(&metadata, owner)?;
                    Some((metadata.dev(), metadata.ino()))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(unavailable(error)),
            };
            let mut options = OpenOptions::new();
            // Linux O_NOFOLLOW | O_NONBLOCK. Refuse symlinks and avoid waiting
            // on a raced special file before the opened-object checks.
            options
                .read(true)
                .write(true)
                .mode(0o600)
                .custom_flags(0o400000 | 0o4000);
            if present.is_none() {
                if !create {
                    return Err(LocalArchiveError::Unavailable);
                }
                options.create_new(true);
            }
            let file = directory
                .open_with(ARCHIVE_FILE, &options)
                .map_err(unavailable)?
                .into_std();
            let opened = file.metadata().map_err(unavailable)?;
            if !opened.is_file()
                || opened.uid() != owner
                || opened.mode() & 0o077 != 0
                || opened.nlink() != 1
            {
                return Err(LocalArchiveError::UnsafePath);
            }
            let file_identity = (opened.dev(), opened.ino());
            if present.is_some_and(|identity| identity != file_identity) {
                return Err(LocalArchiveError::UnsafePath);
            }
            let value = Self {
                directory,
                path: path.to_path_buf(),
                file,
                owner,
                directory_identity,
                file_identity,
            };
            value.verify()?;
            Ok(value)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (path, create);
            Err(LocalArchiveError::UnsupportedPlatform)
        }
    }

    pub(crate) fn engine_file(&self) -> Result<File, LocalArchiveError> {
        self.verify()?;
        self.file.try_clone().map_err(unavailable)
    }

    pub(crate) fn sync_directory(&self) -> Result<(), LocalArchiveError> {
        self.verify()?;
        // cap-std directory capabilities may be O_PATH descriptors on Linux;
        // open a readable handle through the pinned capability for fsync.
        self.directory
            .open(".")
            .map_err(unavailable)?
            .into_std()
            .sync_all()
            .map_err(unavailable)?;
        self.verify()
    }

    pub(crate) fn physical_bytes(&self) -> Result<u64, LocalArchiveError> {
        self.verify()?;
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            self.file
                .metadata()
                .map_err(unavailable)?
                .blocks()
                .checked_mul(512)
                .ok_or(LocalArchiveError::Integrity)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(LocalArchiveError::UnsupportedPlatform)
        }
    }

    pub(crate) fn verify(&self) -> Result<(), LocalArchiveError> {
        #[cfg(target_os = "linux")]
        {
            use cap_std::fs::MetadataExt;
            use std::os::unix::fs::MetadataExt as _;
            let current = open_directory(&self.path, self.owner)?
                .dir_metadata()
                .map_err(unavailable)?;
            let retained = self.directory.dir_metadata().map_err(unavailable)?;
            let named = self
                .directory
                .symlink_metadata(ARCHIVE_FILE)
                .map_err(unavailable)?;
            check_cap_file(&named, self.owner)?;
            let opened = self.file.metadata().map_err(unavailable)?;
            if (current.dev(), current.ino()) != self.directory_identity
                || (retained.dev(), retained.ino()) != self.directory_identity
                || (named.dev(), named.ino()) != self.file_identity
                || (opened.dev(), opened.ino()) != self.file_identity
                || !opened.is_file()
                || opened.uid() != self.owner
                || opened.mode() & 0o077 != 0
                || opened.nlink() != 1
            {
                return Err(LocalArchiveError::UnsafePath);
            }
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(LocalArchiveError::UnsupportedPlatform)
        }
    }
}

#[cfg(target_os = "linux")]
fn check_cap_file(metadata: &cap_std::fs::Metadata, owner: u32) -> Result<(), LocalArchiveError> {
    use cap_std::fs::MetadataExt;
    if !metadata.is_file()
        || metadata.uid() != owner
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
    {
        return Err(LocalArchiveError::UnsafePath);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_directory(path: &Path, owner: u32) -> Result<Dir, LocalArchiveError> {
    use cap_std::fs::MetadataExt;
    use std::path::Component;
    if !path.is_absolute() || path.as_os_str().as_encoded_bytes().len() > 4096 {
        return Err(LocalArchiveError::UnsafePath);
    }
    let mut directory =
        Dir::open_ambient_dir("/", cap_std::ambient_authority()).map_err(unavailable)?;
    let mut count = 0;
    for part in path.components() {
        let name = match part {
            Component::RootDir => continue,
            Component::Normal(name) => name,
            _ => return Err(LocalArchiveError::UnsafePath),
        };
        count += 1;
        if count > MAX_COMPONENTS {
            return Err(LocalArchiveError::UnsafePath);
        }
        let before = directory.symlink_metadata(name).map_err(unavailable)?;
        if !before.is_dir() {
            return Err(LocalArchiveError::UnsafePath);
        }
        let child = directory.open_dir(name).map_err(unavailable)?;
        let opened = child.dir_metadata().map_err(unavailable)?;
        let after = directory.symlink_metadata(name).map_err(unavailable)?;
        if !after.is_dir()
            || (before.dev(), before.ino()) != (opened.dev(), opened.ino())
            || (after.dev(), after.ino()) != (opened.dev(), opened.ino())
        {
            return Err(LocalArchiveError::UnsafePath);
        }
        directory = child;
    }
    let metadata = directory.dir_metadata().map_err(unavailable)?;
    if count == 0
        || metadata.uid() != owner
        || metadata.mode() & 0o077 != 0
        || metadata.mode() & 0o700 != 0o700
    {
        return Err(LocalArchiveError::UnsafePath);
    }
    Ok(directory)
}

#[cfg(target_os = "linux")]
fn effective_uid() -> Result<u32, LocalArchiveError> {
    use std::io::Read;
    let mut bytes = Vec::new();
    File::open("/proc/self/status")
        .map_err(unavailable)?
        .take(65537)
        .read_to_end(&mut bytes)
        .map_err(unavailable)?;
    if bytes.len() > 65536 {
        return Err(LocalArchiveError::Unavailable);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| LocalArchiveError::Unavailable)?;
    let mut lines = text.lines().filter_map(|line| line.strip_prefix("Uid:"));
    let line = lines.next().ok_or(LocalArchiveError::Unavailable)?;
    if lines.next().is_some() {
        return Err(LocalArchiveError::Unavailable);
    }
    let ids = line
        .split_ascii_whitespace()
        .take(5)
        .map(str::parse::<u32>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| LocalArchiveError::Unavailable)?;
    if ids.len() != 4 {
        return Err(LocalArchiveError::Unavailable);
    }
    ids.get(1).copied().ok_or(LocalArchiveError::Unavailable)
}

fn unavailable(_error: impl std::fmt::Display) -> LocalArchiveError {
    LocalArchiveError::Unavailable
}
