//! Directory-anchored custody for derived exact-provider checkpoints.
// req: OQ-040, PRJ-004
#![forbid(unsafe_code)]

pub(crate) const CUSTODY_WORK_BYTES: u64 = 512 * 1024;

#[derive(Clone)]
pub(crate) struct CheckpointDirectoryIdentity {
    device: u64,
    inode: u64,
    owner: u32,
    custody: std::sync::Arc<DirectoryCustody>,
}

struct DirectoryCustody {
    _directory: std::fs::File,
    // The original graph metadata charge follows the final directory reference.
    _charge: Option<std::sync::Arc<riffdb_types::exact_provider_budget_v1::OwnershipCharge>>,
}
impl CheckpointDirectoryIdentity {
    pub(crate) fn retain_charge(
        mut self,
        charge: std::sync::Arc<riffdb_types::exact_provider_budget_v1::OwnershipCharge>,
    ) -> std::io::Result<Self> {
        std::sync::Arc::get_mut(&mut self.custody)
            .ok_or_else(|| std::io::Error::other("derived checkpoint custody unavailable"))?
            ._charge = Some(charge);
        Ok(self)
    }
}
impl PartialEq for CheckpointDirectoryIdentity {
    fn eq(&self, other: &Self) -> bool {
        (self.device, self.inode, self.owner) == (other.device, other.inode, other.owner)
    }
}
impl Eq for CheckpointDirectoryIdentity {}
#[derive(Clone)]
struct CheckpointFileIdentity {
    device: u64,
    inode: u64,
    // A live descriptor keeps this inode allocated until every ownership claim drops.
    _custody: std::sync::Arc<std::fs::File>,
}
impl PartialEq for CheckpointFileIdentity {
    fn eq(&self, other: &Self) -> bool {
        (self.device, self.inode) == (other.device, other.inode)
    }
}
impl Eq for CheckpointFileIdentity {}

/// Process-local custody evidence, never a durable format or public identity.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CheckpointManifest {
    directory: CheckpointDirectoryIdentity,
    name: [u8; 255],
    name_length: u16,
    selected: Option<CheckpointFileIdentity>,
    pending: Option<CheckpointFileIdentity>,
}

impl CheckpointManifest {
    pub(crate) fn empty(
        directory: CheckpointDirectoryIdentity,
        name: &std::ffi::OsStr,
    ) -> std::io::Result<Self> {
        let source = name.as_encoded_bytes();
        if source.is_empty() || source.len() > 255 {
            return Err(std::io::Error::other(
                "derived checkpoint custody unavailable",
            ));
        }
        let mut bytes = [0; 255];
        bytes[..source.len()].copy_from_slice(source);
        Ok(Self {
            directory,
            name: bytes,
            name_length: source.len() as u16,
            selected: None,
            pending: None,
        })
    }
    pub(crate) fn same_binding(&self, other: &Self) -> bool {
        self.directory == other.directory
            && self.name == other.name
            && self.name_length == other.name_length
    }
    #[cfg(target_os = "linux")]
    fn contains_inode(&self, device: u64, inode: u64) -> bool {
        self.selected
            .as_ref()
            .into_iter()
            .chain(self.pending.as_ref())
            .any(|file| (file.device, file.inode) == (device, inode))
    }
}
impl std::fmt::Debug for CheckpointManifest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CheckpointManifest([REDACTED])")
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::{CheckpointCustody, create_private_directory};

#[cfg(target_os = "linux")]
mod linux {
    use super::{CheckpointDirectoryIdentity, CheckpointFileIdentity, CheckpointManifest};
    use std::fs::File;
    use std::io::{self, Read, Write};
    use std::path::{Component, Path};

    use cap_std::fs::{Dir, DirBuilder, DirBuilderExt, MetadataExt, OpenOptions, OpenOptionsExt};

    // Closed path/component ceilings cover transient capability resolution, names
    // and metadata. The caller reserves this work before constructing custody.
    const MAX_COMPONENTS: usize = 32;
    const MAX_PATH_BYTES: usize = 4096;
    type Identity = (u64, u64);

    pub(crate) struct CheckpointCustody<'a> {
        directory: Dir,
        parent: std::path::PathBuf,
        selected: &'a std::ffi::OsStr,
        pending: std::ffi::OsString,
        owner: u32,
        identity: Identity,
        binding: CheckpointDirectoryIdentity,
        selected_file: Option<File>,
        pending_file: Option<File>,
    }

    impl<'a> CheckpointCustody<'a> {
        #[cfg(test)]
        pub(crate) fn open(path: &'a Path) -> io::Result<Self> {
            Self::open_checked(path, None, false)
        }
        pub(crate) fn open_bound(
            path: &'a Path,
            binding: &CheckpointManifest,
            allow_initial: bool,
        ) -> io::Result<Self> {
            Self::open_checked(path, Some(binding), !allow_initial)
        }
        pub(crate) fn open_for_cleanup(
            path: &'a Path,
            manifest: &CheckpointManifest,
        ) -> io::Result<CheckpointCleanup<'a>> {
            Self::open_checked(path, Some(manifest), true).map(CheckpointCleanup)
        }
        fn open_checked(
            path: &'a Path,
            expected: Option<&CheckpointManifest>,
            cleanup: bool,
        ) -> io::Result<Self> {
            let owner = effective_uid()?;
            let supplied_parent = path.parent().ok_or_else(refused)?;
            let parent = if supplied_parent.is_absolute() {
                supplied_parent.to_path_buf()
            } else {
                std::env::current_dir()?.join(supplied_parent)
            };
            let selected = path.file_name().ok_or_else(refused)?;
            if path.as_os_str().as_encoded_bytes().len() > MAX_PATH_BYTES
                || selected.as_encoded_bytes().len() > 255
            {
                return Err(refused());
            }
            let pending = path.with_extension("pending");
            let pending = pending.file_name().ok_or_else(refused)?.to_os_string();
            if pending == selected || pending.as_encoded_bytes().len() > 255 {
                return Err(refused());
            }
            let directory = open_directory(&parent, owner, false)?;
            let metadata = directory.dir_metadata()?;
            let identity = (metadata.dev(), metadata.ino());
            let binding = CheckpointManifest::empty(
                if let Some(expected) = expected {
                    if (
                        expected.directory.device,
                        expected.directory.inode,
                        expected.directory.owner,
                    ) != (identity.0, identity.1, owner)
                    {
                        return Err(refused());
                    }
                    expected.directory.clone()
                } else {
                    CheckpointDirectoryIdentity {
                        device: identity.0,
                        inode: identity.1,
                        owner,
                        custody: std::sync::Arc::new(super::DirectoryCustody {
                            _directory: directory.try_clone()?.into_std_file(),
                            _charge: None,
                        }),
                    }
                },
                selected,
            )?;
            if let Some(expected) = expected {
                if !expected.same_binding(&binding) {
                    return Err(refused());
                }
                if cleanup {
                    for name in [selected, pending.as_os_str()] {
                        match directory.symlink_metadata(name) {
                            Ok(metadata) => {
                                if !expected.contains_inode(metadata.dev(), metadata.ino()) {
                                    return Err(refused());
                                }
                            }
                            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            // A crash between no-replace linking and removal can leave precisely
            // the two owned names linked to the same synced regular file. Refuse
            // any third link or unequal object; complete only this known publication.
            if let (Ok(a), Ok(b)) = (
                directory.symlink_metadata(selected),
                directory.symlink_metadata(&pending),
            ) && a.nlink() == 2
                && b.nlink() == 2
                && (a.dev(), a.ino()) == (b.dev(), b.ino())
            {
                check_metadata(&a, owner, 2)?;
                check_metadata(&b, owner, 2)?;
                let file = open_file(&directory, selected, owner, 2)?;
                check_named(&directory, &pending, &file, owner, 2)?;
                let current = open_directory(&parent, owner, false)?.dir_metadata()?;
                if (current.dev(), current.ino()) != identity {
                    return Err(refused());
                }
                directory.remove_file(&pending)?;
                directory.open(".")?.into_std().sync_all()?;
            }
            let selected_file = existing_file(&directory, selected, owner)?;
            let pending_file = existing_file(&directory, &pending, owner)?;
            let value = Self {
                directory,
                parent,
                selected,
                pending,
                owner,
                identity,
                binding: binding.directory,
                selected_file,
                pending_file,
            };
            value.verify()?;
            Ok(value)
        }

        pub(crate) fn owned_manifest(&self) -> io::Result<CheckpointManifest> {
            self.verify()?;
            let mut manifest = CheckpointManifest::empty(self.binding.clone(), self.selected)?;
            manifest.selected = self.selected_file.as_ref().map(file_identity).transpose()?;
            manifest.pending = self.pending_file.as_ref().map(file_identity).transpose()?;
            Ok(manifest)
        }

        fn verify_anchor(&self) -> io::Result<()> {
            let current = open_directory(&self.parent, self.owner, false)?.dir_metadata()?;
            let retained = self.directory.dir_metadata()?;
            if (current.dev(), current.ino()) != self.identity
                || (retained.dev(), retained.ino()) != self.identity
            {
                return Err(refused());
            }
            Ok(())
        }

        pub(crate) fn verify(&self) -> io::Result<()> {
            self.verify_anchor()?;
            for (name, file) in [
                (self.selected, self.selected_file.as_ref()),
                (self.pending.as_os_str(), self.pending_file.as_ref()),
            ] {
                match file {
                    Some(file) => check_named(&self.directory, name, file, self.owner, 1)?,
                    None => match self.directory.symlink_metadata(name) {
                        Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                        _ => return Err(refused()),
                    },
                }
            }
            Ok(())
        }

        pub(crate) fn read(&self) -> io::Result<Option<File>> {
            self.verify()?;
            self.selected_file.as_ref().map(File::try_clone).transpose()
        }

        #[cfg(test)]
        pub(crate) fn publish(
            self,
            bytes: &[u8],
            before_sync: impl FnOnce(),
            after_sync: impl FnOnce(),
        ) -> io::Result<()> {
            self.publish_tracked(bytes, before_sync, after_sync, |_| Ok(()))
        }
        pub(crate) fn publish_tracked(
            mut self,
            bytes: &[u8],
            before_sync: impl FnOnce(),
            after_sync: impl FnOnce(),
            mut track: impl FnMut(CheckpointManifest) -> io::Result<()>,
        ) -> io::Result<()> {
            self.verify()?;
            let mut manifest = self.owned_manifest()?;
            track(manifest.clone())?;
            if self.pending_file.take().is_some() {
                // Only the opened and verified private partial object is removed.
                self.directory.remove_file(&self.pending)?;
                manifest.pending = None;
                track(manifest.clone())?;
            }
            let mut options = OpenOptions::new();
            options
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(0o400000 | 0o4000);
            let mut file = self
                .directory
                .open_with(&self.pending, &options)?
                .into_std();
            check_named(&self.directory, &self.pending, &file, self.owner, 1)?;
            let next_identity = file_identity(&file)?;
            manifest.pending = Some(next_identity.clone());
            track(manifest.clone())?;
            file.write_all(bytes)?;
            before_sync();
            #[cfg(test)]
            crash_checkpoint("before-file-sync");
            file.sync_all()?;
            after_sync();
            #[cfg(test)]
            crash_checkpoint("after-file-sync");
            self.verify_anchor()?;
            check_named(&self.directory, &self.pending, &file, self.owner, 1)?;
            match self.selected_file.as_ref() {
                Some(previous) => {
                    check_named(&self.directory, self.selected, previous, self.owner, 1)?;
                    self.directory.remove_file(self.selected)?;
                }
                None => {
                    if self.directory.symlink_metadata(self.selected).is_ok() {
                        return Err(refused());
                    }
                }
            }
            manifest.selected = None;
            track(manifest.clone())?;
            // Linking refuses an occupied destination. The data has already synced;
            // neither publication nor cleanup follows a replacement path or symlink.
            #[cfg(test)]
            crash_checkpoint("after-prior-removal");
            self.directory
                .hard_link(&self.pending, &self.directory, self.selected)?;
            manifest.selected = Some(next_identity);
            track(manifest.clone())?;
            #[cfg(test)]
            crash_checkpoint("after-no-replace-link");
            check_named(&self.directory, self.selected, &file, self.owner, 2)?;
            check_named(&self.directory, &self.pending, &file, self.owner, 2)?;
            self.directory.remove_file(&self.pending)?;
            manifest.pending = None;
            track(manifest.clone())?;
            self.directory.open(".")?.into_std().sync_all()?;
            #[cfg(test)]
            crash_checkpoint("after-directory-sync");
            self.verify_anchor()?;
            check_named(&self.directory, self.selected, &file, self.owner, 1)
        }
    }

    /// Constructible only after comparing the complete retained custody record.
    pub(crate) struct CheckpointCleanup<'a>(CheckpointCustody<'a>);
    impl CheckpointCleanup<'_> {
        pub(crate) fn remove_owned(mut self) -> io::Result<()> {
            self.0.verify()?;
            if self.0.selected_file.take().is_some() {
                self.0.directory.remove_file(self.0.selected)?;
            }
            if self.0.pending_file.take().is_some() {
                self.0.directory.remove_file(&self.0.pending)?;
            }
            self.0.directory.open(".")?.into_std().sync_all()?;
            self.0.verify()
        }
    }
    fn file_identity(file: &File) -> io::Result<CheckpointFileIdentity> {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata()?;
        Ok(CheckpointFileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
            _custody: std::sync::Arc::new(file.try_clone()?),
        })
    }

    fn existing_file(
        directory: &Dir,
        name: &std::ffi::OsStr,
        owner: u32,
    ) -> io::Result<Option<File>> {
        match directory.symlink_metadata(name) {
            Ok(metadata) => {
                check_metadata(&metadata, owner, 1)?;
                open_file(directory, name, owner, 1).map(Some)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn open_file(
        directory: &Dir,
        name: &std::ffi::OsStr,
        owner: u32,
        links: u64,
    ) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.read(true).custom_flags(0o400000 | 0o4000);
        let file = directory.open_with(name, &options)?.into_std();
        check_named(directory, name, &file, owner, links)?;
        Ok(file)
    }

    fn check_metadata(metadata: &cap_std::fs::Metadata, owner: u32, links: u64) -> io::Result<()> {
        if !metadata.is_file()
            || metadata.uid() != owner
            || metadata.mode() & 0o077 != 0
            || metadata.nlink() != links
        {
            return Err(refused());
        }
        Ok(())
    }

    fn check_named(
        directory: &Dir,
        name: &std::ffi::OsStr,
        file: &File,
        owner: u32,
        links: u64,
    ) -> io::Result<()> {
        use std::os::unix::fs::MetadataExt as _;
        let named = directory.symlink_metadata(name)?;
        check_metadata(&named, owner, links)?;
        let opened = file.metadata()?;
        if !opened.is_file()
            || opened.uid() != owner
            || opened.mode() & 0o077 != 0
            || opened.nlink() != links
            || (opened.dev(), opened.ino()) != (named.dev(), named.ino())
        {
            return Err(refused());
        }
        Ok(())
    }

    fn open_directory(path: &Path, owner: u32, create: bool) -> io::Result<Dir> {
        if !path.is_absolute() || path.as_os_str().as_encoded_bytes().len() > MAX_PATH_BYTES {
            return Err(refused());
        }
        let mut directory = Dir::open_ambient_dir("/", cap_std::ambient_authority())?;
        let mut count = 0;
        for component in path.components() {
            let name = match component {
                Component::RootDir => continue,
                Component::Normal(name) => name,
                _ => return Err(refused()),
            };
            count += 1;
            if count > MAX_COMPONENTS {
                return Err(refused());
            }
            let before = match directory.symlink_metadata(name) {
                Ok(metadata) => metadata,
                Err(error) if create && error.kind() == io::ErrorKind::NotFound => {
                    let mut builder = DirBuilder::new();
                    builder.mode(0o700);
                    match directory.create_dir_with(name, &builder) {
                        Ok(()) => (),
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
                        Err(error) => return Err(error),
                    }
                    directory.symlink_metadata(name)?
                }
                Err(error) => return Err(error),
            };
            if !before.is_dir() {
                return Err(refused());
            }
            let child = directory.open_dir(name)?;
            let opened = child.dir_metadata()?;
            let after = directory.symlink_metadata(name)?;
            if !after.is_dir()
                || (before.dev(), before.ino()) != (opened.dev(), opened.ino())
                || (after.dev(), after.ino()) != (opened.dev(), opened.ino())
            {
                return Err(refused());
            }
            directory = child;
        }
        let metadata = directory.dir_metadata()?;
        if count == 0
            || metadata.uid() != owner
            || metadata.mode() & 0o077 != 0
            || metadata.mode() & 0o700 != 0o700
        {
            return Err(refused());
        }
        Ok(directory)
    }

    pub(crate) fn create_private_directory(path: &Path) -> io::Result<CheckpointDirectoryIdentity> {
        let owned_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let owner = effective_uid()?;
        let directory = open_directory(&owned_path, owner, true)?;
        let created = directory.dir_metadata()?;
        let current = open_directory(&owned_path, owner, false)?.dir_metadata()?;
        if (created.dev(), created.ino()) != (current.dev(), current.ino()) {
            return Err(refused());
        }
        Ok(CheckpointDirectoryIdentity {
            device: created.dev(),
            inode: created.ino(),
            owner,
            custody: std::sync::Arc::new(super::DirectoryCustody {
                _directory: directory.into_std_file(),
                _charge: None,
            }),
        })
    }

    fn effective_uid() -> io::Result<u32> {
        // Stack-only bounded status inspection: no persistent or unadmitted heap.
        let mut bytes = [0_u8; 65537];
        let mut file = File::open("/proc/self/status")?;
        let mut length = 0;
        while length < bytes.len() {
            let read = file.read(&mut bytes[length..])?;
            if read == 0 {
                break;
            }
            length += read;
        }
        if length > 65536 {
            return Err(refused());
        }
        let text = std::str::from_utf8(&bytes[..length]).map_err(|_| refused())?;
        let mut lines = text.lines().filter_map(|line| line.strip_prefix("Uid:"));
        let line = lines.next().ok_or_else(refused)?;
        if lines.next().is_some() {
            return Err(refused());
        }
        let mut ids = line.split_ascii_whitespace();
        let _real = ids
            .next()
            .ok_or_else(refused)?
            .parse::<u32>()
            .map_err(|_| refused())?;
        let effective = ids
            .next()
            .ok_or_else(refused)?
            .parse::<u32>()
            .map_err(|_| refused())?;
        for _ in 0..2 {
            ids.next()
                .ok_or_else(refused)?
                .parse::<u32>()
                .map_err(|_| refused())?;
        }
        if ids.next().is_some() {
            return Err(refused());
        }
        Ok(effective)
    }

    #[cfg(test)]
    fn crash_checkpoint(point: &str) {
        if std::env::var("RIFFDB_CUSTODY_CRASH_POINT").as_deref() == Ok(point) {
            std::process::exit(81);
        }
    }

    fn refused() -> io::Error {
        io::Error::other("derived checkpoint custody unavailable")
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) struct CheckpointCustody<'a>(std::marker::PhantomData<&'a std::path::Path>);

#[cfg(not(target_os = "linux"))]
impl<'a> CheckpointCustody<'a> {
    #[cfg(test)]
    pub(crate) fn open(_: &'a std::path::Path) -> std::io::Result<Self> {
        Err(std::io::Error::other(
            "derived checkpoint custody unavailable",
        ))
    }
    pub(crate) fn open_bound(
        _: &'a std::path::Path,
        _: &CheckpointManifest,
        _: bool,
    ) -> std::io::Result<Self> {
        Err(std::io::Error::other(
            "derived checkpoint custody unavailable",
        ))
    }
    pub(crate) fn open_for_cleanup(
        _: &'a std::path::Path,
        _: &CheckpointManifest,
    ) -> std::io::Result<CheckpointCleanup> {
        Err(std::io::Error::other(
            "derived checkpoint custody unavailable",
        ))
    }
    pub(crate) fn owned_manifest(&self) -> std::io::Result<CheckpointManifest> {
        Err(std::io::Error::other(
            "derived checkpoint custody unavailable",
        ))
    }
    pub(crate) fn publish_tracked(
        self,
        _: &[u8],
        _: impl FnOnce(),
        _: impl FnOnce(),
        _: impl FnMut(CheckpointManifest) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        Err(std::io::Error::other(
            "derived checkpoint custody unavailable",
        ))
    }
    pub(crate) fn verify(&self) -> std::io::Result<()> {
        Err(std::io::Error::other(
            "derived checkpoint custody unavailable",
        ))
    }
    pub(crate) fn read(&self) -> std::io::Result<Option<std::fs::File>> {
        Err(std::io::Error::other(
            "derived checkpoint custody unavailable",
        ))
    }
    #[cfg(test)]
    pub(crate) fn publish(
        self,
        _: &[u8],
        _: impl FnOnce(),
        _: impl FnOnce(),
    ) -> std::io::Result<()> {
        Err(std::io::Error::other(
            "derived checkpoint custody unavailable",
        ))
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) struct CheckpointCleanup;
#[cfg(not(target_os = "linux"))]
impl CheckpointCleanup {
    pub(crate) fn remove_owned(self) -> std::io::Result<()> {
        Err(std::io::Error::other(
            "derived checkpoint custody unavailable",
        ))
    }
}

#[cfg(all(test, target_os = "linux"))]
#[path = "exact_provider_checkpoint_custody_tests.rs"]
mod tests;

#[cfg(all(test, target_os = "linux"))]
mod recovery_tests {
    use super::CheckpointCustody;
    use std::fs;
    use std::io::Read;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn custody_checkpoint_process_child() {
        let Ok(root) = std::env::var("RIFFDB_CUSTODY_CRASH_ROOT") else {
            return;
        };
        let path = std::path::Path::new(&root).join("epoch.rxts");
        CheckpointCustody::open(&path)
            .unwrap()
            .publish(b"next complete epoch", || {}, || {})
            .unwrap();
        panic!("the requested checkpoint crash boundary must terminate the child");
    }

    #[test]
    fn custody_process_crashes_recover_complete_or_absent_derived_selection() {
        for point in [
            "before-file-sync",
            "after-file-sync",
            "after-prior-removal",
            "after-no-replace-link",
            "after-directory-sync",
        ] {
            let root = tempfile::tempdir().unwrap();
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let path = root.path().join("epoch.rxts");
            CheckpointCustody::open(&path)
                .unwrap()
                .publish(b"prior complete epoch", || {}, || {})
                .unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "exact_provider_checkpoint_custody::recovery_tests::custody_checkpoint_process_child", "--nocapture"])
                .env("RIFFDB_CUSTODY_CRASH_ROOT", root.path())
                .env("RIFFDB_CUSTODY_CRASH_POINT", point)
                .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null())
                .status().unwrap();
            assert_eq!(status.code(), Some(81), "{point}");
            let custody = CheckpointCustody::open(&path).unwrap();
            let recovered = custody.read().unwrap().map(|mut file| {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).unwrap();
                bytes
            });
            match point {
                "before-file-sync" | "after-file-sync" => assert_eq!(
                    recovered.as_deref(),
                    Some(b"prior complete epoch".as_slice())
                ),
                "after-prior-removal" => assert!(recovered.is_none()),
                _ => assert_eq!(
                    recovered.as_deref(),
                    Some(b"next complete epoch".as_slice())
                ),
            }
            custody
                .publish(b"rebuilt complete epoch", || {}, || {})
                .unwrap();
            assert_eq!(fs::read(&path).unwrap(), b"rebuilt complete epoch");
            assert!(!path.with_extension("pending").exists());
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn create_private_directory(
    _: &std::path::Path,
) -> std::io::Result<CheckpointDirectoryIdentity> {
    Err(std::io::Error::other(
        "derived checkpoint custody unavailable",
    ))
}
