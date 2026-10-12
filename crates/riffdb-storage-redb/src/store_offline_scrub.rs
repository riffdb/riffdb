//! Receipt-only open of an owned verification copy; original journals are read only.
use super::*;

/// One owned verification copy and its sole completely checked history pin.
/// Fields stay private so a caller cannot pair another database with the pin.
pub(crate) struct OfflineScrubOpen {
    store: RedbStore,
    history: Option<crate::journal::VerifiedMaterializedHistory>,
}

impl OfflineScrubOpen {
    pub(crate) fn into_parts(
        self,
    ) -> (
        RedbStore,
        Option<crate::journal::VerifiedMaterializedHistory>,
    ) {
        (self.store, self.history)
    }
}

impl RedbStore {
    pub(crate) fn open_offline_scrub(
        path: &Path,
        copy: &crate::maintenance::PhysicalScrubCopy,
        follower: bool,
    ) -> Result<OfflineScrubOpen, StorageError> {
        let store = Self::open_after_format_preflight(
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
        )?;
        let history = if follower {
            None
        } else {
            store.verify_materialized_journals()?
        };
        Ok(OfflineScrubOpen { store, history })
    }

    pub(crate) fn verify_materialized_journals(
        &self,
    ) -> Result<Option<crate::journal::VerifiedMaterializedHistory>, StorageError> {
        self.verify_materialized_journals_for_restore(None, None)
    }

    pub(crate) fn verify_owned_restore_materialized_journals(
        &self,
    ) -> Result<Option<crate::journal::VerifiedMaterializedHistory>, StorageError> {
        self.verify_owned_restore_materialized_journals_with_history(None)
    }

    pub(crate) fn verify_owned_restore_materialized_journals_with_history(
        &self,
        history: Option<crate::restore_history::CompleteRestoreHistory>,
    ) -> Result<Option<crate::journal::VerifiedMaterializedHistory>, StorageError> {
        if !self.shared.permits_owned_source_compatibility() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let binding = self
            .shared
            .restore_integrity
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        self.verify_materialized_journals_for_restore(Some(binding), history)
    }

    fn verify_materialized_journals_for_restore(
        &self,
        binding: Option<&crate::maintenance::RestoreIntegrityBinding>,
        complete: Option<crate::restore_history::CompleteRestoreHistory>,
    ) -> Result<Option<crate::journal::VerifiedMaterializedHistory>, StorageError> {
        let read = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let database_id = read_identity_from_read_transaction(&read)?;
        let history = match binding {
            Some(binding) => match complete {
                Some(proof) => crate::journal::VerifiedMaterializedHistory::reuse_owned_restore(
                    read, binding, proof,
                ),
                None => crate::journal::VerifiedMaterializedHistory::capture_owned_restore(
                    read, binding,
                ),
            },
            None => crate::journal::VerifiedMaterializedHistory::capture(read),
        }
        .map_err(recovery_journal_error)?;
        // No replay, anchor advance, extent sealing, spare removal or capacity
        // normalization is permitted. Pending recovery needs the ordinary owner.
        for path in [
            crate::journal::checkpoint_journal_path(&self.shared.path),
            crate::journal::sealing_journal_path(&self.shared.path),
            crate::journal::journal_path(&self.shared.path),
        ] {
            match &history {
                Some(history) => {
                    history.verify_journal(self.shared.journal_media.as_ref(), &path, database_id)
                }
                None => crate::journal::verify_materialized_journal_with_media(
                    self.shared.journal_media.as_ref(),
                    &self.shared.database,
                    &path,
                    database_id,
                ),
            }
            .map_err(recovery_journal_error)?;
        }
        Ok(history)
    }
}
