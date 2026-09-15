//! A follower open establishes file AND directory durability independently of
//! an earlier publisher. No store escapes after an uncertain namespace sync.
use super::path_guard::{PinnedDirectory, check_current_marker};
use crate::error::storage_error;
use riffdb_storage_api::{StorageError, StorageErrorKind};
use std::{ffi::OsString, fs::File, path::Path};

pub(crate) struct FollowerNamespace {
    parent: PinnedDirectory,
    name: OsString,
    file: File,
    marker_name: OsString,
    marker: File,
    #[cfg(test)]
    fail_directory_sync: bool,
}

impl FollowerNamespace {
    pub(crate) fn open(path: &Path) -> Result<Self, StorageError> {
        let parent = PinnedDirectory::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?;
        let name = path.file_name().ok_or_else(corrupt)?.to_os_string();
        if parent
            .regular_file_length(&name)?
            .is_none_or(|length| length == 0)
        {
            return Err(corrupt());
        }
        let marker_name = crate::durable_format_marker_path(path)
            .file_name()
            .ok_or_else(corrupt)?
            .to_os_string();
        let file = parent.open_file_read_write(&name)?.into_std();
        let marker = parent.open_file(&marker_name)?.into_std();
        let mut namespace = Self {
            parent,
            name,
            file,
            marker_name,
            marker,
            #[cfg(test)]
            fail_directory_sync: false,
        };
        namespace.verify()?;
        Ok(namespace)
    }

    pub(crate) fn backend(&self) -> Result<redb::backends::FileBackend, StorageError> {
        // The normal builder keeps its existing configuration and repair
        // callback; only the backing descriptor is pinned before engine open.
        redb::backends::FileBackend::new(self.file.try_clone().map_err(unavailable)?)
            .map_err(unavailable)
    }

    pub(crate) fn synchronize(&mut self) -> Result<(), StorageError> {
        self.verify()?;
        self.file.sync_all().map_err(unavailable)?;
        #[cfg(test)]
        if self.fail_directory_sync {
            return Err(unavailable(()));
        }
        self.parent.sync()?;
        self.verify()
    }

    pub(crate) fn verify_database_file(&mut self, file: &File) -> Result<(), StorageError> {
        self.verify()?;
        if !self.parent.regular_file_matches(&self.name, file)? {
            return Err(corrupt());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fail_directory_sync(&mut self) {
        self.fail_directory_sync = true;
    }

    pub(super) fn verify(&mut self) -> Result<(), StorageError> {
        self.parent.verify()?;
        if !self.parent.regular_file_matches(&self.name, &self.file)?
            || !self
                .parent
                .regular_file_matches(&self.marker_name, &self.marker)?
        {
            return Err(corrupt());
        }
        check_current_marker(&mut self.marker)
    }

    pub(super) fn verify_scratch(
        &mut self,
        directory: &PinnedDirectory,
    ) -> Result<(), StorageError> {
        self.verify()?;
        if self.parent.same_directory(directory)? {
            return Err(corrupt());
        }
        for (name, file) in [
            (std::ffi::OsStr::new("follower.redb"), &self.file),
            (
                std::ffi::OsStr::new("follower.redb.riffdb-format-v1"),
                &self.marker,
            ),
        ] {
            if directory.regular_file_length(name)?.is_some()
                && !directory.regular_file_matches(name, file)?
            {
                return Err(corrupt());
            }
        }
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
    // req: REP-002, REC-001
    #[test]
    fn follower_namespace_rejects_file_substitution_after_pinning_without_touching_the_replacement()
    {
        for marker in [false, true] {
            let scope = crate::test_path::ScopedDirectory::new("follower-namespace-pin");
            let path = scope.join("db.redb");
            drop(redb::Database::create(&path).unwrap());
            let marker_path = crate::durable_format_marker_path(&path);
            std::fs::write(
                &marker_path,
                riffdb_storage_api::encode_durable_format_marker(
                    riffdb_storage_api::current_durable_format_marker(),
                ),
            )
            .unwrap();
            let mut namespace = FollowerNamespace::open(&path).unwrap();
            let database = redb::Database::builder()
                .create_with_backend(namespace.backend().unwrap())
                .unwrap();
            let replaced = if marker { &marker_path } else { &path };
            std::fs::rename(replaced, scope.join("retained-original")).unwrap();
            std::fs::write(replaced, b"unrelated-replacement").unwrap();
            assert_eq!(
                namespace.synchronize().unwrap_err().kind(),
                StorageErrorKind::CorruptData
            );
            assert_eq!(std::fs::read(replaced).unwrap(), b"unrelated-replacement");
            drop(database);
        }
    }
}
