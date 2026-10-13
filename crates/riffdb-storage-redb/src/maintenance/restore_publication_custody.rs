//! Consuming native exclusion handoff: there is deliberately no engine lease.
use super::*;
use std::ffi::OsStr;

pub(crate) struct ClosedPublicationCustody {
    // Consume the actual Shared object, retaining its original descriptor and
    // both native locks. No authority is reconstructed by opening a pathname.
    original: Shared,
    parent: Arc<PinnedDirectory>,
    name: OsString,
}

impl RestoreCustody {
    pub(crate) fn into_closed_publication(self) -> Result<ClosedPublicationCustody, StorageError> {
        self.verify_identity()?;
        {
            let leases = self.shared.leases.read().map_err(|_| unavailable(()))?;
            if leases.active.is_some()
                || !leases.ranges.is_empty()
                || self.shared.maximum_bytes.load(Ordering::Acquire) == 0
            {
                self.invalidate();
                return Err(unavailable(()));
            }
        }
        let original = Arc::try_unwrap(self.shared).map_err(|shared| {
            shared.invalid.store(true, Ordering::Release);
            unavailable(())
        })?;
        let name = original.name.read().map_err(|_| unavailable(()))?.clone();
        let owner = ClosedPublicationCustody {
            parent: Arc::clone(&original.parent),
            name,
            original,
        };
        owner.verify()?;
        Ok(owner)
    }
}

impl ClosedPublicationCustody {
    pub(crate) fn verify(&self) -> Result<(), StorageError> {
        let result = (|| {
            self.original.available().map_err(unavailable)?;
            self.original.parent.verify()?;
            self.parent.verify()?;
            let metadata = self.original.file.metadata().map_err(unavailable)?;
            if !metadata.is_file()
                || metadata.len() != self.original.maximum_bytes.load(Ordering::Acquire)
                || !self
                    .parent
                    .regular_file_matches(&self.name, &self.original.file)?
            {
                return Err(corrupt());
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(corrupt());
                }
            }
            self.parent.verify()
        })();
        if result.is_err() {
            self.original.invalid.store(true, Ordering::Release);
        }
        result
    }

    /// This moves only the original file. The enclosing sealed artifact owner
    /// must check its pre-transfer SHA-256 before and after every transition.
    /// No engine can be created from this value, and errors retain the file.
    pub(crate) fn move_to(
        &mut self,
        destination: Arc<PinnedDirectory>,
        name: &OsStr,
    ) -> Result<(), StorageError> {
        let result = (|| {
            self.verify()?;
            self.parent.rename_to(&self.name, &destination, name)?;
            self.parent = destination;
            self.name = name.to_os_string();
            self.verify()
        })();
        if result.is_err() {
            self.original.invalid.store(true, Ordering::Release);
        }
        result
    }
}

#[cfg(all(test, unix))]
#[path = "restore_publication_custody_tests.rs"]
mod tests;
