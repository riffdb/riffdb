//! A verified private backup is validation input, never local promotion authority.
use super::*;
use crate::error::transaction_error;
use crate::maintenance::PromotionValidationBinding;
use redb::ReadableDatabase;
use std::ffi::OsStr;
use std::sync::{Arc, atomic::AtomicBool};

impl RedbStagedRestore {
    pub(super) fn validate_materialized(
        &mut self,
        inputs: StartupValidationInputs,
    ) -> Result<(), StorageError> {
        self.verify_paths()?;
        let file = self
            .stage_cleanup
            .directory_guard
            .open_file_read_write(OsStr::new(DATABASE_ARTIFACT_FILE_NAME))?
            .into_std();
        let binding = {
            let database = super::super::archive_backup::read_only_database(&file)?;
            crate::backup::validate_readable_database_semantics(&database, &self.manifest)?;
            let read = database.begin_read().map_err(transaction_error)?;
            PromotionValidationBinding::for_verified_backup(&read, &self.manifest)?
        };
        if let Some(binding) = binding {
            // The immutable source was checksum-verified during materialization.
            // This open remains write-fenced and cannot yield operational ports;
            // the source's external ledger cannot authorize this private copy.
            crate::startup::validate_committed_promotion(
                &self.staged_database_file,
                &file,
                Arc::new(binding),
                inputs,
                Arc::new(AtomicBool::new(false)),
            )?;
            self.verify_promoted_file(&file)?;
            self.promoted_authorization_checksum = Some(self.promoted_file_checksum(&file)?);
        } else {
            drop(file);
            crate::startup::RedbOfflineIntegrityScrub::from_inputs(
                &self.staged_database_file,
                inputs,
            )
            .run()?;
        }
        self.verify_paths()
    }

    /// Pins the completely validated private promoted backup for authorization.
    /// Ordinary backups keep their existing staged startup path. This grants no
    /// writer, local promotion reconciliation, readiness or publication permit.
    pub fn promoted_authorization_snapshot(
        &self,
    ) -> Result<Option<crate::RedbOwnedSnapshot>, StorageError> {
        let Some(expected) = &self.promoted_authorization_checksum else {
            return Ok(None);
        };
        let file = self
            .stage_cleanup
            .open_file(OsStr::new(DATABASE_ARTIFACT_FILE_NAME))?
            .into_std();
        self.verify_promoted_file(&file)?;
        if &self.promoted_file_checksum(&file)? != expected {
            return Err(corrupt());
        }
        let database = super::super::archive_backup::read_only_database(&file)?;
        let read = database.begin_read().map_err(transaction_error)?;
        if PromotionValidationBinding::for_verified_backup(&read, &self.manifest)?.is_none()
            || &self.promoted_file_checksum(&file)? != expected
        {
            return Err(corrupt());
        }
        self.verify_promoted_file(&file)?;
        let root = Arc::new(crate::checkpoint_root::CheckpointRoot::new(read, 0));
        Ok(Some(crate::RedbOwnedSnapshot::from_read_access(
            crate::store::RedbReadAccess::Durable(root),
        )))
    }

    pub(super) fn verify_promoted_authorization_seal(&self) -> Result<(), StorageError> {
        if let Some(expected) = &self.promoted_authorization_checksum {
            let file = self
                .stage_cleanup
                .open_file(OsStr::new(DATABASE_ARTIFACT_FILE_NAME))?
                .into_std();
            self.verify_promoted_file(&file)?;
            if &self.promoted_file_checksum(&file)? != expected {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    fn verify_promoted_file(&self, file: &File) -> Result<(), StorageError> {
        self.verify_paths()?;
        if !self
            .stage_cleanup
            .directory_guard
            .regular_file_matches(OsStr::new(DATABASE_ARTIFACT_FILE_NAME), file)?
        {
            return Err(corrupt());
        }
        Ok(())
    }

    fn promoted_file_checksum(
        &self,
        file: &File,
    ) -> Result<BackupIntegrityChecksumV1, StorageError> {
        let input = self
            .stage_cleanup
            .open_file(OsStr::new(DATABASE_ARTIFACT_FILE_NAME))?
            .into_std();
        self.verify_promoted_file(file)?;
        self.verify_promoted_file(&input)?;
        let checksum = sha256_reader(input)?;
        self.verify_promoted_file(file)?;
        Ok(checksum)
    }
}
