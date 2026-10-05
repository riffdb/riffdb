//! Receipt-only open of an owned verification copy; original journals are read only.
use super::*;

impl RedbStore {
    pub(crate) fn open_offline_scrub(
        path: &Path,
        copy: &crate::maintenance::PhysicalScrubCopy,
        follower: bool,
    ) -> Result<Self, StorageError> {
        Self::open_after_format_preflight(
            if follower {
                OpenMode::OfflineFollower
            } else {
                OpenMode::OfflineSource
            },
            path,
            RedbCommitProfile::Hardened,
            None,
            default_changelog_port(),
            None,
            Some(Box::new(copy.backend()?)),
            Arc::new(RealJournalMedia),
        )
    }

    pub(super) fn verify_materialized_journals(&self) -> Result<(), StorageError> {
        let read = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let database_id = read_identity_from_read_transaction(&read)?;
        drop(read);
        // No replay, anchor advance, extent sealing, spare removal or capacity
        // normalization is permitted. Pending recovery needs the ordinary owner.
        for path in [
            crate::journal::checkpoint_journal_path(&self.shared.path),
            crate::journal::sealing_journal_path(&self.shared.path),
            crate::journal::journal_path(&self.shared.path),
        ] {
            crate::journal::verify_materialized_journal_with_media(
                self.shared.journal_media.as_ref(),
                &self.shared.database,
                &path,
                database_id,
            )
            .map_err(recovery_journal_error)?;
        }
        Ok(())
    }
}
