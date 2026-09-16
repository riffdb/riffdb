//! Validation of a quarantined logical cut, not a new replication position.
use super::*;
use redb::{ReadTransaction, ReadableTableMetadata};
use riffdb_storage_api::{StorageFormatVersion, proto_codec::*};
use std::sync::Arc;

/// Only the reconstruction owner can create this binding. Ordinary startup has
/// no optional root relaxation: the private mode checks this exact predecessor
/// plus the independently reconstructed logical frontier instead.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct PrivateArchiveValidationBinding {
    predecessor: ChangelogHistoryStateV3,
    frontier: DualFrontier,
}

impl PrivateArchiveValidationBinding {
    pub(super) fn new(predecessor: ChangelogHistoryStateV3, frontier: DualFrontier) -> Self {
        Self {
            predecessor,
            frontier,
        }
    }

    pub(crate) fn validate(&self, transaction: &ReadTransaction) -> Result<(), StorageError> {
        let meta = transaction
            .open_table(crate::layout::META)
            .map_err(unavailable)?;
        let read = |namespace: N| -> Result<Vec<u8>, StorageError> {
            let value = meta
                .get(namespace.metadata_key().ok_or_else(corrupt)?)
                .map_err(unavailable)?
                .ok_or_else(corrupt)?;
            if value.value().len() > 512 {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            Ok(value.value().to_vec())
        };
        let lineage = self.predecessor.lineage();
        decode_authoritative_state_catalog_v1(&read(N::AuthoritativeStateCatalog)?)
            .map_err(|_| corrupt())?;
        if *decode_storage_format_version_v1(&read(N::FormatVersion)?)
            .map_err(|_| corrupt())?
            .value()
            != StorageFormatVersion::V2
            || *decode_record_registry_v2(&read(N::RecordRegistry)?)
                .map_err(|_| corrupt())?
                .value()
                != current_record_registry_digest()
            || *decode_database_identity_v1(&read(N::DatabaseIdentity)?)
                .map_err(|_| corrupt())?
                .value()
                != lineage.database_id()
            || *decode_history_incarnation_v1(&read(N::HistoryIncarnation)?)
                .map_err(|_| corrupt())?
                .value()
                != lineage.history_incarnation()
            || *decode_leadership_epoch_v1(&read(N::LeadershipEpoch)?)
                .map_err(|_| corrupt())?
                .value()
                != lineage.leadership_epoch()
            || *decode_changelog_history_state_v3(&read(N::ChangelogHistoryState)?)
                .map_err(|_| corrupt())?
                .value()
                != self.predecessor
            || *decode_changelog_transaction_allocator_v3(&read(N::NextChangelogTransaction)?)
                .map_err(|_| corrupt())?
                .value()
                != self.predecessor.expected_allocator()
            || crate::changelog_v3_roots::decode_physical_frontier(
                &read(N::NextApplicationSequence)?,
                &read(N::NextAdministrationSequence)?,
            )? != self.frontier
            || self.predecessor.tail().frontier().application() >= self.frontier.application()
            || self.predecessor.tail().frontier().administration() >= self.frontier.administration()
        {
            return Err(corrupt());
        }
        for namespace in N::ALL {
            if (namespace == N::ReplicationFollowerState
                || namespace.class()
                    == ReplicationAuthorityClassV1::ReplicationControl(
                        ReplicationTransferV1::SourceOnly,
                    ))
                && let Some(key) = namespace.metadata_key()
                && meta.get(key).map_err(unavailable)?.is_some()
            {
                return Err(corrupt());
            }
        }
        for table in [
            crate::changelog_v3_activation::HISTORY,
            crate::changelog_v3_activation::SOURCE_HOLDS,
        ] {
            if !transaction
                .open_table(table)
                .map_err(unavailable)?
                .is_empty()
                .map_err(unavailable)?
            {
                return Err(corrupt());
            }
        }
        Ok(())
    }
}

/// A completely validated private cut. Authentication uses a pinned snapshot;
/// the format remains quarantined and this type grants no serving/write port.
pub struct RedbValidatedPrivateArchiveRestore {
    candidate: RedbPrivateArchiveRestoreCandidate,
    checksum: BackupIntegrityChecksumV1,
}

impl RedbPrivateArchiveRestoreCandidate {
    /// Runs the full structural/catalog and reciprocal command graph checks with
    /// no checkpoint shortcut, source journal recovery, migration or local audit.
    pub fn validate(
        self,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<RedbValidatedPrivateArchiveRestore, StorageError> {
        cancelled(&cancellation)?;
        self.verify_private()?;
        if sha256_file(self.staged_database_file())? != self.construction_checksum {
            return Err(corrupt());
        }
        crate::startup::validate_private_archive(
            &self.stage.staged_database_file,
            &self.file,
            self.binding,
            inputs,
            cancellation,
        )?;
        self.verify_private()?;
        let checksum = sha256_file(self.staged_database_file())?;
        self.verify_private()?;
        Ok(RedbValidatedPrivateArchiveRestore {
            candidate: self,
            checksum,
        })
    }

    fn verify_private(&self) -> Result<(), StorageError> {
        verify_file(&self.stage, &self.file)?;
        let directory = &self.stage.stage_cleanup.directory_guard;
        let original = crate::durable_format_marker_path(self.staged_database_file());
        if directory
            .regular_file_length(original.file_name().ok_or_else(corrupt)?)?
            .is_some()
        {
            return Err(corrupt());
        }
        let mut marker = directory.open_file(OsStr::new(PRIVATE_MARKER))?.into_std();
        crate::maintenance::path_guard::check_current_marker(&mut marker)?;
        Ok(())
    }
}

impl RedbValidatedPrivateArchiveRestore {
    #[must_use]
    /// Quarantined file for offline inspection, never an ordinary open target.
    pub fn staged_database_file(&self) -> &Path {
        self.candidate.staged_database_file()
    }
    #[must_use]
    /// Immutable original selection, distinct from the actual stopped frontier.
    pub const fn selection(&self) -> &ArchiveRestoreSelectionV3 {
        self.candidate.selection()
    }
    #[must_use]
    /// Actual validated application/audit boundary.
    pub const fn restored_frontier(&self) -> DualFrontier {
        self.candidate.restored_frontier()
    }
    /// Opens one immutable validated state for staged authentication and current
    /// policy. All clones must close before the later publication ceremony.
    pub fn authorization_snapshot(&self) -> Result<crate::RedbOwnedSnapshot, StorageError> {
        self.candidate.verify_private()?;
        let database = read_only_database(&self.candidate.file)?;
        let read = database.begin_read().map_err(unavailable)?;
        self.candidate.binding.validate(&read)?;
        if sha256_file(self.staged_database_file())? != self.checksum {
            return Err(corrupt());
        }
        self.candidate.verify_private()?;
        let root = Arc::new(crate::checkpoint_root::CheckpointRoot::new(read, 0));
        Ok(crate::RedbOwnedSnapshot::from_read_access(
            crate::store::RedbReadAccess::Durable(root),
        ))
    }
    /// Discards this private proof and artifact without replacing the target.
    pub fn discard(self) -> Result<(), StorageError> {
        self.candidate.discard()
    }
}
