//! Directory-anchored custody for derived exact-provider checkpoints.
// req: OQ-040, PRJ-004
#![forbid(unsafe_code)]

pub(crate) const CUSTODY_WORK_BYTES: u64 = 512 * 1024;

#[cfg(target_os = "linux")]
pub(crate) use linux::{CheckpointCustody, create_private_directory};

#[cfg(target_os = "linux")]
mod linux {
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
        selected_file: Option<File>,
        pending_file: Option<File>,
    }

    impl<'a> CheckpointCustody<'a> {
        pub(crate) fn open(path: &'a Path) -> io::Result<Self> {
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
                selected_file,
                pending_file,
            };
            value.verify()?;
            Ok(value)
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

        pub(crate) fn publish(
            mut self,
            bytes: &[u8],
            before_sync: impl FnOnce(),
            after_sync: impl FnOnce(),
        ) -> io::Result<()> {
            self.verify()?;
            if self.pending_file.take().is_some() {
                // Only the opened and verified private partial object is removed.
                self.directory.remove_file(&self.pending)?;
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
            // Linking refuses an occupied destination. The data has already synced;
            // neither publication nor cleanup follows a replacement path or symlink.
            #[cfg(test)]
            crash_checkpoint("after-prior-removal");
            self.directory
                .hard_link(&self.pending, &self.directory, self.selected)?;
            #[cfg(test)]
            crash_checkpoint("after-no-replace-link");
            check_named(&self.directory, self.selected, &file, self.owner, 2)?;
            check_named(&self.directory, &self.pending, &file, self.owner, 2)?;
            self.directory.remove_file(&self.pending)?;
            self.directory.open(".")?.into_std().sync_all()?;
            #[cfg(test)]
            crash_checkpoint("after-directory-sync");
            self.verify_anchor()?;
            check_named(&self.directory, self.selected, &file, self.owner, 1)
        }
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

    pub(crate) fn create_private_directory(path: &Path) -> io::Result<()> {
        let owned_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let owner = effective_uid()?;
        let created = open_directory(&owned_path, owner, true)?.dir_metadata()?;
        let current = open_directory(&owned_path, owner, false)?.dir_metadata()?;
        if (created.dev(), created.ino()) != (current.dev(), current.ino()) {
            return Err(refused());
        }
        Ok(())
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
    pub(crate) fn open(_: &'a std::path::Path) -> std::io::Result<Self> {
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::CheckpointCustody;
    use std::fs;
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    use std::path::Path;

    fn private_root() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        root
    }
    fn private_file(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    fn read(path: &Path) -> Vec<u8> {
        let mut bytes = Vec::new();
        CheckpointCustody::open(path)
            .unwrap()
            .read()
            .unwrap()
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        bytes
    }

    #[test]
    fn custody_creates_private_roots_without_following_symlinked_components() {
        let root = private_root();
        let path = root.path().join("derived").join("exact");
        super::create_private_directory(&path).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o700);
        let foreign = root.path().join("foreign");
        fs::create_dir(&foreign).unwrap();
        let alias = root.path().join("alias");
        symlink(&foreign, &alias).unwrap();
        assert!(super::create_private_directory(&alias.join("unexpected")).is_err());
        assert!(!foreign.join("unexpected").exists());
    }

    #[test]
    fn custody_supports_relative_paths_without_changing_process_directory() {
        let root = tempfile::Builder::new().tempdir_in(".").unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let current = std::env::current_dir().unwrap();
        let path = root
            .path()
            .strip_prefix(&current)
            .unwrap()
            .join("epoch.rxts");
        assert!(!path.is_absolute());
        CheckpointCustody::open(&path)
            .unwrap()
            .publish(b"relative derived bytes", || {}, || {})
            .unwrap();
        assert_eq!(read(&path), b"relative derived bytes");
    }

    #[test]
    fn custody_publishes_replaces_and_reopens_one_private_link() {
        let root = private_root();
        let path = root.path().join("epoch.rxts");
        assert!(
            CheckpointCustody::open(&path)
                .unwrap()
                .read()
                .unwrap()
                .is_none()
        );
        for bytes in [b"generation one".as_slice(), b"generation two".as_slice()] {
            CheckpointCustody::open(&path)
                .unwrap()
                .publish(bytes, || {}, || {})
                .unwrap();
            assert_eq!(read(&path), bytes);
            let metadata = fs::metadata(&path).unwrap();
            assert_eq!(metadata.mode() & 0o777, 0o600);
            assert_eq!(metadata.nlink(), 1);
            assert!(!path.with_extension("pending").exists());
        }
    }

    #[test]
    fn custody_discards_only_verified_private_partial_work() {
        let root = private_root();
        let path = root.path().join("epoch.rxts");
        private_file(&path.with_extension("pending"), b"incomplete derived bytes");
        CheckpointCustody::open(&path)
            .unwrap()
            .publish(b"complete bytes", || {}, || {})
            .unwrap();
        assert_eq!(read(&path), b"complete bytes");
    }

    #[test]
    fn custody_rejects_selected_symlinks_and_foreign_hardlinks() {
        let root = private_root();
        let foreign = root.path().join("foreign");
        private_file(&foreign, b"foreign evidence");
        let path = root.path().join("epoch.rxts");
        symlink(&foreign, &path).unwrap();
        assert!(CheckpointCustody::open(&path).is_err());
        fs::remove_file(&path).unwrap();
        fs::hard_link(&foreign, &path).unwrap();
        assert!(CheckpointCustody::open(&path).is_err());
        assert_eq!(fs::read(&foreign).unwrap(), b"foreign evidence");
        assert_eq!(fs::metadata(&foreign).unwrap().nlink(), 2);
    }

    #[test]
    fn custody_rejects_replaced_selection_after_sync_without_deleting_foreign_object() {
        let root = private_root();
        let path = root.path().join("epoch.rxts");
        let retained = root.path().join("retained");
        private_file(&path, b"previous derived bytes");
        let custody = CheckpointCustody::open(&path).unwrap();
        let result = custody.publish(
            b"replacement derived bytes",
            || {},
            || {
                fs::rename(&path, &retained).unwrap();
                private_file(&path, b"foreign evidence");
            },
        );
        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"foreign evidence");
        assert_eq!(fs::read(&retained).unwrap(), b"previous derived bytes");
    }

    #[test]
    fn custody_rejects_replaced_pending_object_after_sync() {
        let root = private_root();
        let path = root.path().join("epoch.rxts");
        let pending = path.with_extension("pending");
        let retained = root.path().join("retained");
        let result = CheckpointCustody::open(&path).unwrap().publish(
            b"derived bytes",
            || {},
            || {
                fs::rename(&pending, &retained).unwrap();
                private_file(&pending, b"foreign evidence");
            },
        );
        assert!(result.is_err());
        assert!(!path.exists());
        assert_eq!(fs::read(&pending).unwrap(), b"foreign evidence");
        assert_eq!(fs::read(&retained).unwrap(), b"derived bytes");
    }

    #[test]
    fn custody_rejects_directory_replacement_after_sync() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("private");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.join("epoch.rxts");
        let retained = parent.path().join("retained");
        let result = CheckpointCustody::open(&path).unwrap().publish(
            b"derived bytes",
            || {},
            || {
                fs::rename(&root, &retained).unwrap();
                fs::create_dir(&root).unwrap();
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
                private_file(&path, b"foreign evidence");
            },
        );
        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"foreign evidence");
        assert_eq!(
            fs::read(retained.join("epoch.pending")).unwrap(),
            b"derived bytes"
        );
    }

    #[test]
    fn custody_recovers_exact_two_name_publication_without_exposing_partial_bytes() {
        let root = private_root();
        let path = root.path().join("epoch.rxts");
        let pending = path.with_extension("pending");
        private_file(&pending, b"complete synced derived bytes");
        fs::hard_link(&pending, &path).unwrap();
        assert_eq!(read(&path), b"complete synced derived bytes");
        assert!(!pending.exists());
        assert_eq!(fs::metadata(&path).unwrap().nlink(), 1);
    }

    #[test]
    fn custody_rejects_a_third_link_without_deleting_any_name() {
        let root = private_root();
        let path = root.path().join("epoch.rxts");
        let pending = path.with_extension("pending");
        let foreign = root.path().join("foreign");
        private_file(&pending, b"foreign evidence");
        fs::hard_link(&pending, &path).unwrap();
        fs::hard_link(&pending, &foreign).unwrap();
        assert!(CheckpointCustody::open(&path).is_err());
        for name in [&path, &pending, &foreign] {
            assert_eq!(fs::read(name).unwrap(), b"foreign evidence");
            assert_eq!(fs::metadata(name).unwrap().nlink(), 3);
        }
    }

    #[test]
    fn custody_rejects_broadened_permissions_and_symlinked_ancestors() {
        let root = private_root();
        let path = root.path().join("epoch.rxts");
        private_file(&path, b"derived bytes");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(CheckpointCustody::open(&path).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let alias = root.path().join("alias");
        symlink(root.path(), &alias).unwrap();
        assert!(CheckpointCustody::open(&alias.join("epoch.rxts")).is_err());
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(CheckpointCustody::open(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"derived bytes");
    }
}

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
pub(crate) fn create_private_directory(_: &std::path::Path) -> std::io::Result<()> {
    Err(std::io::Error::other(
        "derived checkpoint custody unavailable",
    ))
}
