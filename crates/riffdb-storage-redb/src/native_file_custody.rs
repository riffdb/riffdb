//! Scoped file exclusion for inventory and offline native-engine custody.

use crate::error::{database_error, storage_error};
use redb::StorageBackend;
use riffdb_storage_api::{StorageError, StorageErrorKind};
use std::{fs::File, ops::Bound};

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
    backend: Option<redb::backends::FileBackend>,
}

impl NativeFileCustody {
    pub(crate) fn acquire(file: File) -> Result<Self, StorageError> {
        let file = LegacyFileCustody::acquire(file)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let mut owner = Self {
            file,
            backend: None,
        };
        owner.backend = Some(
            redb::backends::FileBackend::new(
                owner
                    .file
                    .file()
                    .try_clone()
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))?,
            )
            .map_err(database_error)?,
        );
        if !owner
            .backend
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(owner)
    }
}

impl Drop for NativeFileCustody {
    fn drop(&mut self) {
        // An inherited description may outlive our descriptors. End both
        // native exclusions explicitly at the operation's custody boundary.
        if let Some(backend) = &self.backend {
            let _ = backend.close();
        }
        // The legacy guard releases its flock after the native backend.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

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
