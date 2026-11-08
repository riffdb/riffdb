//! Directory-anchored, bounded best-effort log rotation. Never opens a data file.

use crate::config::logging::LoggingFileConfig;
use std::io::{self, Write};

#[derive(Debug)]
pub(crate) enum OpenError {
    Custody,
    DirectoryInUse,
    PermissionDenied,
    StorageFull,
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    Unsupported,
}
impl From<io::Error> for OpenError {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::PermissionDenied => Self::PermissionDenied,
            io::ErrorKind::StorageFull => Self::StorageFull,
            _ => Self::Custody,
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod platform {
    use super::*;
    use cap_std::fs::{Dir, File, Metadata, MetadataExt, OpenOptions, OpenOptionsExt};
    use std::io::{Read, Seek, SeekFrom};

    #[cfg(target_os = "macos")]
    const FLAGS: i32 = 0x0100 | 0x0004;
    #[cfg(target_os = "linux")]
    const FLAGS: i32 = 0o400000 | 0o4000;

    pub(crate) struct ProtectedLogFile {
        directory: Dir,
        _directory_lock: std::fs::File,
        file: Option<File>,
        owner: u32,
        bytes: u64,
        files: u8,
        repair_tail: bool,
    }

    fn refused() -> io::Error {
        io::Error::other("protected logging file custody unavailable")
    }
    fn name(index: u8) -> String {
        if index == 0 {
            "riffdb.log".to_owned()
        } else {
            format!("riffdb.log.{index}")
        }
    }

    impl ProtectedLogFile {
        pub(crate) fn open(config: &LoggingFileConfig) -> Result<Self, OpenError> {
            if !(2..=32).contains(&config.max_files)
                || !(1024 * 1024..=256 * 1024 * 1024).contains(&config.max_file_bytes)
            {
                return Err(OpenError::Custody);
            }
            let directory = crate::exact_provider_checkpoint_custody::open_private_directory(
                config.directory.as_path(),
                true,
            )?;
            let metadata = directory.dir_metadata()?;
            if metadata.mode() & 0o7777 != 0o700 {
                return Err(OpenError::Custody);
            }
            // The capability directory may use O_PATH. Reopen the same
            // anchored directory read-only before taking a kernel file lock.
            let lock_file = directory.open(".")?;
            let lock_metadata = lock_file.metadata()?;
            if (lock_metadata.dev(), lock_metadata.ino()) != (metadata.dev(), metadata.ino()) {
                return Err(OpenError::Custody);
            }
            let directory_lock = lock_file.into_std();
            directory_lock.try_lock().map_err(|error| match error {
                std::fs::TryLockError::WouldBlock => OpenError::DirectoryInUse,
                std::fs::TryLockError::Error(error) => OpenError::from(error),
            })?;
            let mut result = Self {
                directory,
                _directory_lock: directory_lock,
                file: None,
                owner: metadata.uid(),
                bytes: config.max_file_bytes,
                files: config.max_files,
                repair_tail: true,
            };
            // Probe only the complete fixed inventory: never enumerate a directory
            // whose size can be controlled by another process.
            for index in 0..32 {
                if let Some(metadata) = result.checked(index)?
                    && (index >= result.files || metadata.len() > result.bytes)
                {
                    result.directory.remove_file(name(index))?;
                }
            }
            result.file = Some(result.open_current()?);
            result.repair()?;
            Ok(result)
        }

        fn checked(&self, index: u8) -> io::Result<Option<Metadata>> {
            match self.directory.symlink_metadata(name(index)) {
                Ok(meta)
                    if meta.is_file()
                        && meta.uid() == self.owner
                        && meta.mode() & 0o7777 == 0o600
                        && meta.nlink() == 1 =>
                {
                    Ok(Some(meta))
                }
                Ok(_) => Err(refused()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            }
        }

        fn open_current(&self) -> io::Result<File> {
            let before = self.checked(0)?;
            let mut options = OpenOptions::new();
            options
                .read(true)
                .append(true)
                .custom_flags(FLAGS)
                .mode(0o600);
            if before.is_none() {
                options.create_new(true);
            }
            let file = self.directory.open_with(name(0), &options)?;
            let opened = file.metadata()?;
            let after = self.checked(0)?.ok_or_else(refused)?;
            if (opened.dev(), opened.ino()) != (after.dev(), after.ino())
                || before.is_some_and(|m| (m.dev(), m.ino()) != (opened.dev(), opened.ino()))
            {
                return Err(refused());
            }
            Ok(file)
        }

        fn current(&self) -> io::Result<&File> {
            let file = self.file.as_ref().ok_or_else(refused)?;
            let opened = file.metadata()?;
            let named = self.checked(0)?.ok_or_else(refused)?;
            if (opened.dev(), opened.ino()) != (named.dev(), named.ino()) || opened.nlink() != 1 {
                return Err(refused());
            }
            Ok(file)
        }

        fn repair(&mut self) -> io::Result<()> {
            if !self.repair_tail {
                return Ok(());
            }
            let length = self.current()?.metadata()?.len();
            if length > 0 {
                let scan = length.min(super::super::events::MAX_RECORD_BYTES as u64);
                let file = self.file.as_mut().ok_or_else(refused)?;
                file.seek(SeekFrom::End(-(scan as i64)))?;
                let mut tail = vec![0; scan as usize];
                file.read_exact(&mut tail)?;
                if tail.last() != Some(&b'\n') {
                    let valid = match tail.iter().rposition(|b| *b == b'\n') {
                        Some(offset) => length - scan + offset as u64 + 1,
                        None if length == scan => 0,
                        None => return Err(refused()),
                    };
                    file.set_len(valid)?;
                }
            }
            self.repair_tail = false;
            Ok(())
        }

        fn rotate(&mut self) -> io::Result<()> {
            self.current()?;
            self.file.take();
            if self.checked(self.files - 1)?.is_some() {
                self.directory.remove_file(name(self.files - 1))?;
            }
            for index in (0..self.files - 1).rev() {
                if self.checked(index)?.is_some() {
                    self.directory
                        .rename(name(index), &self.directory, name(index + 1))?;
                }
            }
            self.file = Some(self.open_current()?);
            self.repair_tail = false;
            Ok(())
        }
    }

    impl Write for ProtectedLogFile {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > super::super::events::MAX_RECORD_BYTES || bytes.last() != Some(&b'\n')
            {
                return Err(refused());
            }
            if self.file.is_none() {
                self.file = Some(self.open_current()?);
                self.repair_tail = true;
            }
            self.repair()?;
            let length = self.current()?.metadata()?.len();
            if length > self.bytes {
                return Err(refused());
            }
            if length
                .checked_add(bytes.len() as u64)
                .is_none_or(|n| n > self.bytes)
            {
                self.rotate()?;
            }
            let file = self.file.as_mut().ok_or_else(refused)?;
            match file.write_all(bytes) {
                Ok(()) => Ok(bytes.len()),
                Err(error) => {
                    self.repair_tail = true;
                    Err(error)
                }
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            self.file.as_mut().ok_or_else(refused)?.flush()
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) use platform::ProtectedLogFile;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) struct ProtectedLogFile;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl ProtectedLogFile {
    pub(crate) fn open(_config: &LoggingFileConfig) -> Result<Self, OpenError> {
        Err(OpenError::Unsupported)
    }
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl Write for ProtectedLogFile {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    fn configuration(root: &std::path::Path, files: u8) -> LoggingFileConfig {
        LoggingFileConfig {
            directory: riffdb_config::ProtectedFilePath::new(root.join("logs")).unwrap(),
            max_file_bytes: 1024 * 1024,
            max_files: files,
        }
    }
    #[test]
    // req: ID-005, END-007, END-008
    fn protected_log_rotation_is_bounded_across_restart() {
        let root = tempfile::TempDir::new().unwrap();
        let config = configuration(root.path(), 3);
        let record = format!("{{\"synthetic\":\"{}\"}}\n", "x".repeat(1000));
        let mut file = ProtectedLogFile::open(&config).unwrap();
        std::fs::write(config.directory.as_path().join("unrelated"), b"preserve").unwrap();
        for _ in 0..4000 {
            file.write_all(record.as_bytes()).unwrap();
        }
        drop(file);
        let config = configuration(root.path(), 2);
        let mut file = ProtectedLogFile::open(&config).unwrap();
        file.write_all(b"{\"restart\":true}\n").unwrap();
        drop(file);
        let mut owned = 0;
        for entry in std::fs::read_dir(config.directory.as_path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name() == "unrelated" {
                continue;
            }
            let metadata = entry.metadata().unwrap();
            owned += 1;
            assert!(metadata.len() <= config.max_file_bytes);
            assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
        }
        assert!(owned <= 2);
        assert_eq!(
            std::fs::read(config.directory.as_path().join("unrelated")).unwrap(),
            b"preserve"
        );
        assert_eq!(
            std::fs::metadata(config.directory.as_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o700
        );
    }

    #[test]
    // req: ID-005, DX-003
    fn protected_logs_refuse_symlinks_hardlinks_and_unsafe_permissions() {
        let root = tempfile::TempDir::new().unwrap();
        let config = configuration(root.path(), 2);
        drop(ProtectedLogFile::open(&config).unwrap());
        let path = config.directory.as_path().join("riffdb.log");
        std::fs::remove_file(&path).unwrap();
        let sentinel = root.path().join("sentinel");
        std::fs::write(&sentinel, b"preserve").unwrap();
        std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o600)).unwrap();
        symlink(&sentinel, &path).unwrap();
        assert!(ProtectedLogFile::open(&config).is_err());
        std::fs::remove_file(&path).unwrap();
        std::fs::hard_link(&sentinel, &path).unwrap();
        assert!(ProtectedLogFile::open(&config).is_err());
        std::fs::remove_file(&path).unwrap();
        drop(ProtectedLogFile::open(&config).unwrap());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(ProtectedLogFile::open(&config).is_err());
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"preserve");
    }

    #[test]
    // req: ID-005, END-007, END-008
    fn restart_discards_only_an_incomplete_final_record() {
        let root = tempfile::TempDir::new().unwrap();
        let config = configuration(root.path(), 2);
        let mut file = ProtectedLogFile::open(&config).unwrap();
        file.write_all(b"{\"first\":true}\n").unwrap();
        drop(file);
        let path = config.directory.as_path().join("riffdb.log");
        use std::fs::OpenOptions;
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{\"partial\"")
            .unwrap();
        let mut file = ProtectedLogFile::open(&config).unwrap();
        file.write_all(b"{\"second\":true}\n").unwrap();
        drop(file);
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            "{\"first\":true}\n{\"second\":true}\n"
        );
    }
    // req: DX-003, ID-005, END-007, END-008
    #[test]
    fn one_log_directory_has_one_rotation_owner_until_release() {
        let root = tempfile::TempDir::new().unwrap();
        let config = configuration(root.path(), 2);
        let mut first = ProtectedLogFile::open(&config).unwrap();
        first.write_all(b"{\"owner\":1}\n").unwrap();
        assert!(
            ProtectedLogFile::open(&config).is_err(),
            "another owner must not race the same retention inventory"
        );
        drop(first);
        let mut successor = ProtectedLogFile::open(&config).unwrap();
        successor.write_all(b"{\"owner\":2}\n").unwrap();
        drop(successor);
        assert_eq!(
            std::fs::read(config.directory.as_path().join("riffdb.log")).unwrap(),
            b"{\"owner\":1}\n{\"owner\":2}\n"
        );
    }
}
