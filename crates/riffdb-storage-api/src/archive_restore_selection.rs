//! Checked immutable artifact selection for the accepted archive receipt V3.
//! These values prove shape and identity agreement only. The offline owner must
//! verify the complete backup and selected archive chain before recording them;
//! no constructor grants replay, publication, authorization or receipt durability.

use riffdb_types::{ArchiveRestoreStopV1, CommitSequence, DualFrontier};

use crate::{
    ArchiveManifestV1, ChangelogHistoryPointV3, ChangelogLineageV3,
    OfflineBackupManifestIdentityV1, StorageValueError,
};

/// Closed resolved suffix designation. Unresolved selection is not an empty suffix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArchiveRestoreSuffixV3 {
    /// The verified archive has no suffix beyond the exact full-backup fence.
    Empty,
    /// Exact terminal manifest bytes bind the selected original frame chain.
    Terminal(Box<ArchiveManifestV1>),
}

/// Immutable backup/archive evidence retained once by an archive-only V3 receipt.
#[derive(Clone, Eq, PartialEq)]
pub struct ArchiveRestoreSelectionV3 {
    backup: OfflineBackupManifestIdentityV1,
    lineage: ChangelogLineageV3,
    backup_fence: ChangelogHistoryPointV3,
    suffix: ArchiveRestoreSuffixV3,
}

impl ArchiveRestoreSelectionV3 {
    /// Checks exact agreement without substituting a later archive head.
    ///
    /// The manifest's last retained command may precede the complete backup
    /// frontier after pruning. It must never be used as the replay start fence.
    pub fn new(
        backup: OfflineBackupManifestIdentityV1,
        lineage: ChangelogLineageV3,
        backup_fence: ChangelogHistoryPointV3,
        suffix: ArchiveRestoreSuffixV3,
    ) -> Result<Self, StorageValueError> {
        if backup.manifest_checksum().as_bytes().len() != 32 {
            return Err(StorageValueError::InvalidShape);
        }
        if backup.database_id() != lineage.database_id()
            || backup.included_application_frontier() > backup_fence.frontier().application()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        if let ArchiveRestoreSuffixV3::Terminal(terminal) = &suffix
            && (terminal.lineage() != lineage
                || terminal.backup_fence() != backup_fence
                || terminal.full_backup_manifest_digest() != backup.manifest_checksum().as_bytes())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            backup,
            lineage,
            backup_fence,
            suffix,
        })
    }

    /// Exact identity of the independently verified original full backup.
    #[must_use]
    pub const fn backup(&self) -> &OfflineBackupManifestIdentityV1 {
        &self.backup
    }

    /// Original database, history and leadership fences.
    #[must_use]
    pub const fn lineage(&self) -> ChangelogLineageV3 {
        self.lineage
    }

    /// Actual complete backup position, including administration and history hash.
    #[must_use]
    pub const fn backup_fence(&self) -> ChangelogHistoryPointV3 {
        self.backup_fence
    }

    /// Exact resolved suffix; the terminal manifest is never re-resolved on retry.
    #[must_use]
    pub const fn suffix(&self) -> &ArchiveRestoreSuffixV3 {
        &self.suffix
    }

    /// Selected full suffix frontier, distinct from an earlier requested stop.
    #[must_use]
    pub fn terminal_frontier(&self) -> DualFrontier {
        match &self.suffix {
            ArchiveRestoreSuffixV3::Empty => self.backup_fence.frontier(),
            ArchiveRestoreSuffixV3::Terminal(terminal) => terminal.covered().frontier(),
        }
    }

    /// Resolves a stop within the verified backup/suffix interval without rounding.
    pub fn target_application(
        &self,
        stop: ArchiveRestoreStopV1,
    ) -> Result<Option<CommitSequence>, StorageValueError> {
        match stop {
            ArchiveRestoreStopV1::LastArchived => Ok(self.terminal_frontier().application()),
            ArchiveRestoreStopV1::AtApplicationSequence(sequence) => {
                let target = Some(sequence);
                if target < self.backup_fence.frontier().application()
                    || target > self.terminal_frontier().application()
                {
                    return Err(StorageValueError::InvalidShape);
                }
                Ok(target)
            }
        }
    }

    /// Checks restored-frontier shape against the exact request and selection.
    /// The replay and full validation owners must independently prove the state.
    /// An earlier application stop makes no source receipt/hash claim.
    pub fn validate_restored_frontier(
        &self,
        stop: ArchiveRestoreStopV1,
        restored: DualFrontier,
    ) -> Result<(), StorageValueError> {
        let before = self.backup_fence.frontier();
        let terminal = self.terminal_frontier();
        if restored.application() != self.target_application(stop)?
            || !(restored == before || restored.advances_from(before))
            || !(restored == terminal || terminal.advances_from(restored))
            || (stop == ArchiveRestoreStopV1::LastArchived && restored != terminal)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(())
    }
}

impl std::fmt::Debug for ArchiveRestoreSelectionV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ArchiveRestoreSelectionV3([redacted])")
    }
}
