//! Read-only exact publication evidence for the V3 maintenance driver.
use super::*;
use redb::ReadableDatabase;
use riffdb_storage_api::{ArchiveEncryptionPostureV1, ChangelogAttributionV3};
use std::io::{Seek, SeekFrom};

impl RedbMaintenanceStorage {
    /// Resolves only an admitted Offline receipt's named backup. The server
    /// supplies its operator-configured archive path and encryption posture.
    pub fn open_archive_restore_repository(
        &mut self,
        id: OfflineMaintenanceOperationId,
        archive: &Path,
        encryption: ArchiveEncryptionPostureV1,
    ) -> Result<crate::RedbArchiveRepository, StorageError> {
        self.verify_path_ownership()?;
        let receipt = self.read_archive_receipt(id)?.ok_or_else(corrupt)?;
        if receipt.current_phase() != OfflineMaintenanceReceiptPhaseV1::Offline {
            return Err(invariant());
        }
        let backup = crate::RedbVerifiedArchiveBackup::open(
            &self.named_backup_directory(receipt.backup_name()),
        )?;
        if receipt.selection().is_some_and(|selection| {
            selection.backup().manifest_checksum().as_bytes() != backup.manifest_digest()
        }) {
            return Err(corrupt());
        }
        let repository = backup
            .open_existing_archive(archive, encryption)
            .map_err(|error| match error {
                riffdb_storage_api::ArchiveConsumerErrorV1::SinkUnavailable => {
                    storage_error(StorageErrorKind::Unavailable)
                }
                _ => corrupt(),
            })?;
        self.verify_path_ownership()?;
        Ok(repository)
    }

    /// Opens the existing operator-configured archive against a verified named
    /// backup for private recovery staging. This grants no receipt or publication
    /// authority; the caller must freeze selection after staged authorization.
    pub fn open_recovery_archive_repository(
        &self,
        backup_name: &BackupNameV1,
        archive: &Path,
        encryption: ArchiveEncryptionPostureV1,
    ) -> Result<crate::RedbArchiveRepository, StorageError> {
        self.verify_path_ownership()?;
        let backup =
            crate::RedbVerifiedArchiveBackup::open(&self.named_backup_directory(backup_name))?;
        let repository = backup
            .open_existing_archive(archive, encryption)
            .map_err(|error| match error {
                riffdb_storage_api::ArchiveConsumerErrorV1::SinkUnavailable => {
                    storage_error(StorageErrorKind::Unavailable)
                }
                _ => corrupt(),
            })?;
        self.verify_path_ownership()?;
        Ok(repository)
    }

    /// Proves publication only from matching retained staged/target bytes,
    /// current markers, the fresh empty journal, and the exact receipt-bound
    /// RestoreAnchor. It performs no repair, replay, authorization or mutation.
    /// A false result never permits a credential-free rebuild or publication.
    pub fn archive_restore_target_matches(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<bool, StorageError> {
        Ok(self.published_archive_stage(id, true)?.is_some())
    }

    /// Complete authoritative state of the retained, receipt-bound published
    /// stage. Only a durable publication phase permits this validation route:
    /// startup may change engine bookkeeping, but every authoritative row and
    /// the exact RestoreAnchor must still equal this immutable pin.
    pub fn archive_restore_validation_evidence(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<Box<dyn riffdb_storage_api::AuthoritativeStateCursorV3>, StorageError> {
        let receipt = self.read_archive_receipt(id)?.ok_or_else(corrupt)?;
        if !matches!(
            receipt.current_phase(),
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                | OfflineMaintenanceReceiptPhaseV1::Validating
        ) {
            return Err(invariant());
        }
        self.published_archive_stage(id, false)?.ok_or_else(corrupt)
    }

    fn published_archive_stage(
        &mut self,
        id: OfflineMaintenanceOperationId,
        match_target: bool,
    ) -> Result<Option<Box<dyn riffdb_storage_api::AuthoritativeStateCursorV3>>, StorageError> {
        self.verify_path_ownership()?;
        let receipt = self.read_archive_receipt(id)?.ok_or_else(corrupt)?;
        let Some(incarnation) = receipt.published_history_incarnation() else {
            return Ok(None);
        };
        let selection = receipt.selection().ok_or_else(corrupt)?;
        let frontier = receipt.restored_frontier().ok_or_else(corrupt)?;
        if incarnation <= selection.lineage().history_incarnation() {
            return Err(corrupt());
        }
        let (_, identity) =
            validate_immutable_backup(&self.named_backup_directory(receipt.backup_name()))?;
        if selection.backup() != &identity {
            return Err(corrupt());
        }
        let stage_path = self.staged_directory.join(id.to_string());
        if !stage_path.try_exists().map_err(io_unavailable)? {
            return Ok(None);
        }
        match validate_stage_inventory(&stage_path) {
            Ok(()) => {}
            Err(error) if error.kind() == StorageErrorKind::CorruptData => return Ok(None),
            Err(error) => return Err(error),
        }
        let stage = PinnedDirectory::open(&stage_path)?;
        let database_name = OsStr::new(crate::backup::DATABASE_ARTIFACT_FILE_NAME);
        let stage_database = stage_path.join(database_name);
        let stage_paths = [
            stage_database.clone(),
            crate::journal::journal_path(&stage_database),
            crate::durable_format_marker_path(&stage_database),
        ];
        let target_paths = [
            self.database_file.clone(),
            crate::journal::journal_path(&self.database_file),
            crate::durable_format_marker_path(&self.database_file),
        ];
        let mut staged_files = Vec::with_capacity(3);
        let mut target_files = Vec::with_capacity(3);
        for (staged, target) in stage_paths.iter().zip(&target_paths) {
            let staged_name = staged.file_name().ok_or_else(invariant)?;
            staged_files.push(stage.open_file(staged_name)?.into_std());
            if match_target {
                let target_name = target.file_name().ok_or_else(invariant)?;
                if self
                    .database_parent_guard
                    .regular_file_length(target_name)?
                    .is_none()
                {
                    return Ok(None);
                }
                target_files.push(
                    self.database_parent_guard
                        .open_file(target_name)?
                        .into_std(),
                );
            }
        }
        let database = super::super::archive_backup::read_only_database(&staged_files[0])?;
        // A raw target pin also works before journal/engine recovery. It never
        // repairs a target merely to decide whether publication occurred.
        let target_lock = target_files.first();
        if let Some(file) = target_lock {
            file.try_lock_shared()
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        }
        for (staged, target) in staged_files.iter().zip(&target_files) {
            if checksum(staged)? != checksum(target)? {
                return Ok(None);
            }
        }
        let mut marker = staged_files[2].try_clone().map_err(io_unavailable)?;
        marker.seek(SeekFrom::Start(0)).map_err(io_unavailable)?;
        super::super::path_guard::check_current_marker(&mut marker)?;
        let read = database
            .begin_read()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let history =
            crate::changelog_v3_roots::validate_retained_history(&read)?.ok_or_else(corrupt)?;
        if history.lineage().database_id() != selection.lineage().database_id()
            || history.lineage().history_incarnation() != incarnation
            || history.lineage().leadership_epoch() != selection.lineage().leadership_epoch()
            || history.tail().frontier() != frontier
            || history.anchor() != history.tail()
            || history.tail().sequence().get() != 1
            || crate::follower_lifecycle::is_attached(&read)?
        {
            return Ok(None);
        }
        let receipts = read
            .open_table(crate::changelog_v3_activation::HISTORY)
            .map_err(|_| corrupt())?;
        let anchor = receipts
            .get(1_u64.to_be_bytes().as_slice())
            .map_err(|_| corrupt())?
            .ok_or_else(corrupt)?;
        let anchor = riffdb_storage_api::AuthoritativeTransactionV3::decode(anchor.value())
            .map_err(|_| corrupt())?;
        if anchor.attribution() != ChangelogAttributionV3::RestoreAnchor {
            return Ok(None);
        }
        let (header, tail) = crate::journal::scan_journal(
            &stage_paths[1],
            selection.lineage().database_id(),
            |_| Ok(()),
        )
        .map_err(crate::backup::backup_journal_error)?
        .ok_or_else(corrupt)?;
        if header
            != crate::journal::JournalFileHeader::with_frontiers(
                selection.lineage().database_id(),
                frontier.application(),
                frontier.administration(),
                [0; 32],
            )
            || tail.transition_count != 0
            || tail.incomplete_tail
            || tail.last_sequence != frontier.application()
            || tail.last_administration_sequence != frontier.administration()
        {
            return Ok(None);
        }
        drop(anchor);
        drop(receipts);
        for (path, file) in stage_paths.iter().zip(&staged_files) {
            if !stage.regular_file_matches(path.file_name().ok_or_else(invariant)?, file)? {
                return Err(corrupt());
            }
        }
        for (path, file) in target_paths.iter().zip(&target_files) {
            if !self
                .database_parent_guard
                .regular_file_matches(path.file_name().ok_or_else(invariant)?, file)?
            {
                return Err(corrupt());
            }
        }
        stage.verify()?;
        self.verify_path_ownership()?;
        let root = std::sync::Arc::new(crate::checkpoint_root::CheckpointRoot::new(read, 0));
        let access = crate::store::RedbReadAccess::Durable(root);
        crate::changelog_v3_cursor::state::open(&access)
            .map(Some)
            .map_err(|_| corrupt())
    }
}
fn checksum(file: &File) -> Result<riffdb_storage_api::BackupIntegrityChecksumV1, StorageError> {
    let mut file = file.try_clone().map_err(io_unavailable)?;
    file.seek(SeekFrom::Start(0)).map_err(io_unavailable)?;
    crate::backup::sha256_reader(&mut file)
}
