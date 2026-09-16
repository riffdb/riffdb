//! Engine-only open for a reconstruction-bound, quarantined private artifact.
use super::*;
use crate::maintenance::PrivateArchiveValidationBinding;

impl SharedRedb {
    pub(crate) fn private_restore_binding(&self) -> Option<PrivateArchiveValidationBinding> {
        match &self.open_mode {
            OpenMode::PrivateRestore(binding) => Some(**binding),
            OpenMode::Source | OpenMode::Follower => None,
        }
    }
}

impl RedbStore {
    pub(crate) fn open_private_restore(
        path: &Path,
        file: &std::fs::File,
        binding: PrivateArchiveValidationBinding,
    ) -> Result<Self, StorageError> {
        let backend = redb::backends::FileBackend::new(
            file.try_clone()
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?,
        )
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        Self::open_after_format_preflight(
            OpenMode::PrivateRestore(Arc::new(binding)),
            path,
            RedbCommitProfile::Hardened,
            None,
            default_changelog_port(),
            None,
            Some(Box::new(backend)),
            Arc::new(RealJournalMedia),
        )
    }
}
