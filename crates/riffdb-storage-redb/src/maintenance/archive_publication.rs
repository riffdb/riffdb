//! Archive-specific validation/sealing before the existing offline publication.
use super::*;
use riffdb_storage_api::{OfflineMaintenanceReceiptPhaseV1, OfflineMaintenanceReceiptV3};
use riffdb_types::{ArchiveRestoreStopV1, DualFrontier};

/// Move-only seal over fully validated replay bytes. The server must complete
/// staged authentication/authorization and close its readers before sealing.
/// Only the maintenance owner may use a matching durable V3 receipt to publish.
pub struct RedbSealedArchiveRestore {
    stage: RedbStagedRestore,
    file: File,
    selection: ArchiveRestoreSelectionV3,
    lineage: riffdb_storage_api::ChangelogLineageV3,
    frontier: DualFrontier,
    private: Option<PrivateArchiveValidationBinding>,
    stop: ArchiveRestoreStopV1,
    checksum: BackupIntegrityChecksumV1,
    validation_inputs: StartupValidationInputs,
}

impl RedbReplayedArchiveRestore {
    /// Revalidates the complete restored follower and freezes its bytes after
    /// staged authorization has released all readers. No incarnation is changed.
    pub fn seal_after_validation(
        mut self,
        validated_database_id: DatabaseId,
        stop: ArchiveRestoreStopV1,
        validation_inputs: StartupValidationInputs,
    ) -> Result<RedbSealedArchiveRestore, StorageError> {
        verify_file(&self.stage, &self.file)?;
        if validated_database_id != self.history.lineage().database_id() {
            return Err(corrupt());
        }
        self.selection
            .validate_restored_frontier(stop, self.history.tail().frontier())
            .map_err(|_| corrupt())?;
        if self.stage.native_archive_preparation.is_some() {
            preparation::validate_physical(
                &mut self,
                validation_inputs.clone(),
                std::sync::Arc::new(AtomicBool::new(false)),
            )?;
        }
        if self
            .stage
            .close_native_archive_authorization(&validation_inputs)?
        {
            verify_file(&self.stage, &self.file)?;
            let checksum = sha256_file(self.staged_database_file())?;
            return Ok(RedbSealedArchiveRestore {
                stage: self.stage,
                file: self.file,
                selection: self.selection,
                lineage: self.history.lineage(),
                frontier: self.history.tail().frontier(),
                private: None,
                stop,
                checksum,
                validation_inputs,
            });
        }
        crate::startup::RedbOfflineIntegrityScrub::from_inputs(
            self.staged_database_file(),
            validation_inputs.clone(),
        )
        .run_follower()?;
        verify_file(&self.stage, &self.file)?;
        let database = read_only_database(&self.file)?;
        let read = database.begin_read().map_err(unavailable)?;
        if crate::changelog_v3_roots::validate_retained_history(&read)? != Some(self.history)
            || !crate::follower_lifecycle::is_attached(&read)?
        {
            return Err(corrupt());
        }
        drop(read);
        drop(database);
        let checksum = sha256_file(self.staged_database_file())?;
        verify_file(&self.stage, &self.file)?;
        Ok(RedbSealedArchiveRestore {
            stage: self.stage,
            file: self.file,
            selection: self.selection,
            lineage: self.history.lineage(),
            frontier: self.history.tail().frontier(),
            private: None,
            stop,
            checksum,
            validation_inputs,
        })
    }
}
impl RedbSealedArchiveRestore {
    pub(super) fn from_private(
        stage: RedbStagedRestore,
        file: File,
        selection: ArchiveRestoreSelectionV3,
        binding: PrivateArchiveValidationBinding,
        stop: ArchiveRestoreStopV1,
        checksum: BackupIntegrityChecksumV1,
        validation_inputs: StartupValidationInputs,
    ) -> Self {
        Self {
            stage,
            file,
            selection,
            lineage: binding.predecessor().lineage(),
            frontier: binding.frontier(),
            private: Some(binding),
            stop,
            checksum,
            validation_inputs,
        }
    }

    /// Actual validated replay frontier; distinct from the original manifest.
    #[must_use]
    pub fn restored_frontier(&self) -> DualFrontier {
        self.frontier
    }
    /// Immutable original backup and selected archived prefix.
    #[must_use]
    pub fn selection(&self) -> &ArchiveRestoreSelectionV3 {
        &self.selection
    }
    /// The original history fence used by the driver's max(target, staged)+1 decision.
    #[must_use]
    pub fn staged_history_incarnation(&self) -> u64 {
        self.lineage.history_incarnation()
    }
    pub(in crate::maintenance) fn operation_id(&self) -> OfflineMaintenanceOperationId {
        self.stage.operation_id
    }
    pub(in crate::maintenance) fn configured_database_file(&self) -> &Path {
        &self.stage.configured_database_file
    }

    pub(in crate::maintenance) fn stamp_and_seal(
        mut self,
        receipt: &OfflineMaintenanceReceiptV3,
    ) -> Result<RedbSealedStagedRestore, StorageError> {
        let stage = &self.stage;
        verify_file(stage, &self.file)?;
        if receipt.operation_id() != stage.operation_id
            || receipt.backup_name() != &stage.backup_name
            || receipt.current_phase() != OfflineMaintenanceReceiptPhaseV1::Offline
            || receipt.selection() != Some(&self.selection)
            || receipt.stop() != self.stop
            || receipt.staged_database_id() != Some(self.lineage.database_id())
            || receipt.restored_frontier() != Some(self.restored_frontier())
            || sha256_file(&stage.staged_database_file)? != self.checksum
        {
            return Err(corrupt());
        }
        let incarnation = receipt
            .published_history_incarnation()
            .ok_or_else(invariant)?;
        if incarnation <= self.staged_history_incarnation() {
            return Err(invariant());
        }
        if let Some(controller) = &stage.test_controller {
            controller.hit(
                RedbMaintenanceFailpoint::BetweenReceiptWriteAndStagedStamp,
                true,
            )?;
        }
        let native = self.stage.native_archive_closed.is_some();
        if native {
            if self.private.is_some() {
                verify_quarantine(&self.stage)?;
            }
            self.stage
                .native_archive_closed
                .as_mut()
                .ok_or_else(invariant)?
                .stamp_incarnation(incarnation, self.private)?;
            let seal = self
                .stage
                .native_archive_seal
                .take()
                .ok_or_else(invariant)?;
            self.stage.native_archive_seal =
                Some(seal.after_native_close(&self.stage.artifact_identities)?);
            self.stage.verify_paths()?;
            if self.private.is_some() {
                prefix::prefix_edge("private-incarnation-stamped");
            }
        } else if let Some(binding) = self.private {
            verify_quarantine(&self.stage)?;
            crate::backup::stamp_private_archive_incarnation(&self.file, binding, incarnation)?;
            prefix::prefix_edge("private-incarnation-stamped");
        } else {
            crate::backup::stamp_history_incarnation(
                &self.stage.staged_database_file,
                incarnation,
            )?;
        }
        edge("restore-incarnation-stamped");
        // The replay follower has no source journal. The new RestoreAnchor owns
        // a fresh empty extent at the actual dual frontier, never the old backup head.
        let frontier = self.restored_frontier();
        let header = crate::journal::JournalFileHeader::with_frontiers(
            self.lineage.database_id(),
            frontier.application(),
            frontier.administration(),
            [0; 32],
        );
        self.stage.verify_paths()?;
        // The receipt authorizes the existing journal creation and marker
        // restoration. Retire the old absence/quarantine seals before them;
        // original descriptor identities and native primary custody continue.
        if native {
            drop(self.stage.native_archive_seal.take());
        }
        self.stage
            .artifact_identities
            .create_archive_anchor_journal(
                &self.stage.stage_cleanup.directory_guard,
                &self.stage.staged_journal_file,
                &header,
            )?;
        self.stage.stage_cleanup.directory_guard.sync()?;
        edge("restore-journal-created");
        if self.private.is_some() {
            let stage = &self.stage;
            verify_quarantine(stage)?;
            prefix::prefix_edge("private-journal-created");
            let marker = crate::durable_format_marker_path(&stage.staged_database_file);
            stage.stage_cleanup.directory_guard.rename(
                stage
                    .staged_format_marker_file
                    .file_name()
                    .ok_or_else(corrupt)?,
                marker.file_name().ok_or_else(corrupt)?,
            )?;
            stage.stage_cleanup.directory_guard.sync()?;
            self.stage.staged_format_marker_file = marker;
            prefix::prefix_edge("private-marker-restored");
        }
        if native {
            let closed = self
                .stage
                .native_archive_closed
                .take()
                .ok_or_else(invariant)?;
            let preparation = closed.into_preparation()?;
            let complete = preparation.validate_complete_source(
                &self.stage.staged_database_file,
                self.validation_inputs.clone(),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )?;
            edge("restore-source-validated");
            complete.verify_source_history(self.lineage.database_id(), incarnation, frontier)?;
            self.stage.native_archive_closed = Some(complete.close_after_source_validation()?);
            self.stage.native_archive_seal =
                Some(super::super::identity::ArchiveArtifactSeal::capture(
                    &self.stage.artifact_identities,
                )?);
            self.stage.verify_paths()?;
        } else {
            let stage = &self.stage;
            crate::startup::RedbOfflineIntegrityScrub::from_inputs(
                &stage.staged_database_file,
                self.validation_inputs,
            )
            .run()?;
            edge("restore-source-validated");
            verify_file(stage, &self.file)?;
            let database = read_only_database(&self.file)?;
            let read = database.begin_read().map_err(unavailable)?;
            let history =
                crate::changelog_v3_roots::validate_retained_history(&read)?.ok_or_else(corrupt)?;
            if history.lineage().database_id() != self.lineage.database_id()
                || history.lineage().history_incarnation() != incarnation
                || history.tail().frontier() != frontier
                || crate::follower_lifecycle::is_attached(&read)?
            {
                return Err(corrupt());
            }
            drop(read);
            drop(database);
        }
        let stage = &self.stage;
        let sealed_artifact_checksum = sha256_file(&stage.staged_database_file)?;
        let sealed_journal_checksum = sha256_file(&stage.staged_journal_file)?;
        let sealed_format_marker_checksum = sha256_file(&stage.staged_format_marker_file)?;
        verify_file(stage, &self.file)?;
        let stage = self.stage;
        Ok(RedbSealedStagedRestore {
            operation_id: stage.operation_id,
            backup_name: stage.backup_name,
            backup_directory: stage.backup_directory,
            configured_database_file: stage.configured_database_file,
            staged_database_file: stage.staged_database_file,
            staged_journal_file: stage.staged_journal_file,
            staged_format_marker_file: stage.staged_format_marker_file,
            manifest: stage.manifest,
            manifest_identity: stage.manifest_identity,
            staged_history_incarnation: incarnation,
            native_closed: None,
            native_archive_closed: stage.native_archive_closed,
            sealed_artifact_checksum,
            sealed_journal_checksum,
            sealed_format_marker_checksum,
            backup_directory_guard: stage.backup_directory_guard,
            configured_parent_guard: stage.configured_parent_guard,
            artifact_identities: stage.artifact_identities,
            stage_cleanup: stage.stage_cleanup,
            test_controller: stage.test_controller,
        })
    }
}
impl std::fmt::Debug for RedbSealedArchiveRestore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedbSealedArchiveRestore([redacted])")
    }
}

fn verify_quarantine(stage: &RedbStagedRestore) -> Result<(), StorageError> {
    let directory = &stage.stage_cleanup.directory_guard;
    let normal = crate::durable_format_marker_path(&stage.staged_database_file);
    if directory
        .regular_file_length(normal.file_name().ok_or_else(corrupt)?)?
        .is_some()
        || stage.staged_format_marker_file.file_name() != Some(OsStr::new(prefix::PRIVATE_MARKER))
    {
        return Err(corrupt());
    }
    let mut marker = directory
        .open_file(OsStr::new(prefix::PRIVATE_MARKER))?
        .into_std();
    crate::maintenance::path_guard::check_current_marker(&mut marker)
}
