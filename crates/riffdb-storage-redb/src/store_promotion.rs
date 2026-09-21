//! Promotion-specific opens; decoded phase metadata cannot construct a binding.
use super::*;
use crate::maintenance::PromotionValidationBinding;

impl SharedRedb {
    pub(crate) fn is_private_validation(&self) -> bool {
        self.open_mode.is_private_validation()
    }
}

impl RedbStore {
    pub(crate) fn open_restored_source(
        path: &Path,
        file: &std::fs::File,
        profile: RedbCommitProfile,
        publication: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<Self, StorageError> {
        Self::open_promotion_file(path, file, OpenMode::Source, profile, publication)
    }

    pub(crate) fn open_private_promotion(
        path: &Path,
        file: &std::fs::File,
        binding: Arc<PromotionValidationBinding>,
    ) -> Result<Self, StorageError> {
        Self::open_promotion_file(
            path,
            file,
            OpenMode::PrivatePromotion(binding),
            RedbCommitProfile::Hardened,
            default_changelog_port(),
        )
    }

    pub(crate) fn open_reconciled_promotion(
        path: &Path,
        file: &std::fs::File,
        binding: Arc<PromotionValidationBinding>,
        profile: RedbCommitProfile,
        publication: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<Self, StorageError> {
        Self::open_promotion_file(
            path,
            file,
            OpenMode::ReconciledPromotion(binding),
            profile,
            publication,
        )
    }

    fn open_promotion_file(
        path: &Path,
        file: &std::fs::File,
        mode: OpenMode,
        profile: RedbCommitProfile,
        publication: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<Self, StorageError> {
        let backend = redb::backends::FileBackend::new(
            file.try_clone()
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?,
        )
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        Self::open_after_format_preflight(
            mode,
            path,
            profile,
            None,
            publication,
            None,
            Some(Box::new(backend)),
            Arc::new(RealJournalMedia),
        )
    }
}
