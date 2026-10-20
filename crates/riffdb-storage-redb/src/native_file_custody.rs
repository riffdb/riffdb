//! Scoped file exclusion for inventory and offline native-engine custody.
#![allow(clippy::items_after_test_module)]

use crate::error::{database_error, storage_error};
use riffdb_storage_api::{StorageError, StorageErrorKind};
use std::fs::File;

pub(crate) struct LegacyFileCustody {
    file: File,
}

impl LegacyFileCustody {
    pub(crate) fn acquire(file: File) -> Result<Self, std::fs::TryLockError> {
        file.try_lock()?;
        Ok(Self { file })
    }

    pub(crate) fn acquire_shared(file: File) -> Result<Self, std::fs::TryLockError> {
        file.try_lock_shared()?;
        Ok(Self { file })
    }

    pub(crate) fn file(&self) -> &File {
        &self.file
    }
}

impl Drop for LegacyFileCustody {
    fn drop(&mut self) {
        // Closing our descriptor alone does not release a flock when a
        // concurrent fork retains the same open-file description.
        let _ = self.file.unlock();
    }
}

pub(crate) struct NativeFileCustody {
    file: LegacyFileCustody,
    backend: Option<redb::backends::FullRangeFileCustody>,
}

impl NativeFileCustody {
    #[cfg(unix)]
    pub(crate) fn verify_path_binding(&self, path: &std::path::Path) -> Result<(), StorageError> {
        use std::os::unix::fs::MetadataExt;
        let current = std::fs::symlink_metadata(path)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let held = self
            .file
            .file()
            .metadata()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        if !current.file_type().is_file()
            || current.nlink() != 1
            || held.nlink() != 1
            || current.dev() != held.dev()
            || current.ino() != held.ino()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(())
    }
    #[cfg(not(unix))]
    pub(crate) fn verify_path_binding(&self, _: &std::path::Path) -> Result<(), StorageError> {
        Err(storage_error(StorageErrorKind::IncompatibleFormat))
    }

    pub(crate) fn acquire(file: File) -> Result<Self, StorageError> {
        let file = LegacyFileCustody::acquire(file)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let mut owner = Self {
            file,
            backend: None,
        };
        owner.backend = redb::backends::FullRangeFileCustody::acquire(
            owner
                .file
                .file()
                .try_clone()
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?,
        )
        .map_err(|error| match error {
            redb::ReadOnlyReopenError::Native(error) => database_error(error),
            // The ordinary constructor has no resource-profile gate. Preserve
            // existing lock-error refusal, distinct from database construction.
            _ => storage_error(StorageErrorKind::Unavailable),
        })?;
        if owner.backend.is_none() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(owner)
    }
}

impl Drop for NativeFileCustody {
    fn drop(&mut self) {
        // An inherited description may outlive our descriptors. End both
        // native exclusions explicitly at the operation's custody boundary.
        // The closed Native owner closes its lock on actual destruction, before
        // the unchanged legacy guard releases the original flock.
        drop(self.backend.take());
        // The legacy guard releases its flock after the native backend.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::StorageBackend;
    use std::fs::OpenOptions;
    use std::ops::Bound;

    fn open(path: &std::path::Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    // req: REC-001, REC-005, STO-032
    #[test]
    fn shared_custody_releases_only_its_own_inherited_lock() {
        let scope = crate::test_path::ScopedDirectory::new("shared-custody-release");
        let path = scope.join("source");
        std::fs::write(&path, b"unchanged source").unwrap();
        let first = LegacyFileCustody::acquire_shared(open(&path)).unwrap();
        let inherited = first.file().try_clone().unwrap();
        let second = LegacyFileCustody::acquire_shared(open(&path)).unwrap();
        drop(first);
        let observer = open(&path);
        assert!(matches!(
            observer.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        drop(second);
        observer
            .try_lock()
            .expect("ended shared custody must explicitly release its flock");
        observer.unlock().unwrap();
        assert_eq!(inherited.metadata().unwrap().len(), 16);
        assert_eq!(std::fs::read(&path).unwrap(), b"unchanged source");
    }

    // req: REC-001, REC-005, STO-032
    #[test]
    fn native_custody_releases_both_locks_with_an_inherited_description() {
        for refused in [false, true] {
            let scope = crate::test_path::ScopedDirectory::new("native-custody-release");
            let path = scope.join("source");
            std::fs::write(&path, b"unchanged source").unwrap();
            let custody = NativeFileCustody::acquire(open(&path)).unwrap();
            // Cloning retains the exact open-file description a concurrent
            // fork inherits, without depending on process timing.
            let inherited = custody.file.file().try_clone().unwrap();
            let observer = open(&path);
            let ranges = redb::backends::FileBackend::new(observer.try_clone().unwrap()).unwrap();
            assert!(matches!(
                observer.try_lock(),
                Err(std::fs::TryLockError::WouldBlock)
            ));
            assert!(
                !ranges
                    .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                    .unwrap()
            );
            if refused {
                fn refuse(_custody: NativeFileCustody) -> Result<(), ()> {
                    Err(())
                }
                assert!(refuse(custody).is_err());
            } else {
                drop(custody);
            }
            observer
                .try_lock()
                .expect("ended native custody must explicitly release flock");
            observer.unlock().unwrap();
            assert!(
                ranges
                    .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                    .unwrap()
            );
            ranges.close().unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), b"unchanged source");
            assert_eq!(inherited.metadata().unwrap().len(), 16);
        }
    }

    // req: REC-001, REC-005, STO-032
    #[test]
    fn refused_native_custody_releases_only_its_own_legacy_lock() {
        let scope = crate::test_path::ScopedDirectory::new("native-custody-refusal");
        let path = scope.join("source");
        std::fs::write(&path, b"unchanged source").unwrap();
        let original = redb::backends::FileBackend::new(open(&path)).unwrap();
        assert!(
            original
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        assert_eq!(
            NativeFileCustody::acquire(open(&path))
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        let observer = open(&path);
        observer
            .try_lock()
            .expect("refused native custody must release its flock");
        observer.unlock().unwrap();
        let ranges = redb::backends::FileBackend::new(observer).unwrap();
        assert!(
            !ranges
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        original.close().unwrap();
        assert!(
            ranges
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        ranges.close().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"unchanged source");
    }
}

impl NativeFileCustody {
    pub(crate) fn source_quote_capture_bytes() -> Result<u64, StorageError> {
        redb::backends::FullRangeFileCustody::quote_capture_bytes()
            .map_err(crate::store::native_read_error)
    }
    pub(crate) fn source_retained_resource_bytes(&self) -> Result<u64, StorageError> {
        self.backend
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .retained_resource_bytes()
            .map_err(crate::store::native_read_error)
    }
    pub(crate) fn inspect_source_firstparty_fields(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1<'_>,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        let Self { file, backend } = self;
        let LegacyFileCustody { file } = file;
        crate::source_descriptor_capacity::file(inspection, file)?;
        // The actual Native backend's full header+hash allocation is ADDED from
        // its own pure quote, never inferred here from Option presence/len.
        let _: &Option<redb::backends::FullRangeFileCustody> = backend;
        Ok(())
    }
}
