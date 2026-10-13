//! Receipt-bound ordinary restore reopen. This grants no complete scrub proof.
use super::*;
use redb::ReadableDatabase;
use riffdb_storage_api::ChangelogPublicationPort;
use std::sync::Arc;

impl RedbMaintenanceStorage {
    /// Finishes only matching successful restore intent cleanup before admission.
    /// This cannot complete validation, advance a receipt or alter database files.
    #[doc(hidden)]
    pub fn complete_published_restore_publication_cleanup(
        &self,
        operation: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError> {
        self.complete_native_publication_cleanup(operation)
    }

    /// Opens only the target of an actually published, nonterminal restore.
    /// Receipt, immutable backup, target identity and application frontier must
    /// agree before the ordinary bounded startup evidence can be consumed.
    #[doc(hidden)]
    pub fn begin_published_restore_readiness(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        inputs: StartupValidationInputs,
        application_commit_profile: crate::RedbCommitProfile,
        publication: Arc<dyn ChangelogPublicationPort>,
    ) -> Result<crate::RedbStructuralEvidenceSession, StorageError> {
        self.verify_path_ownership()?;
        let receipt = read_receipt_file(&self.receipt_path(operation_id))?;
        if receipt.operation_kind() != OfflineMaintenanceOperationKind::RestoreBackup
            || !matches!(
                receipt.current_phase(),
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                    | OfflineMaintenanceReceiptPhaseV1::Validating
            )
        {
            return Err(invariant());
        }
        // Publication already verified every immutable artifact and the copied
        // destination. Reopen binds bounded metadata to that durable receipt;
        // this lookup grants no new artifact validation or scrub assurance.
        // Crash reconciliation retains its complete immutable-backup checks.
        let (manifest, identity) = crate::backup::read_backup_manifest_binding(
            &self.named_backup_directory(receipt.backup_name()),
        )?;
        let incarnation = receipt
            .published_history_incarnation()
            .ok_or_else(invariant)?;
        if receipt.manifest_identity() != Some(&identity)
            || receipt.staged_database_id() != Some(manifest.database_id())
        {
            return Err(corrupt());
        }
        let native = self.consume_native_publication_for_startup(operation_id)?;
        // Fresh sealed publication remains nonrepairing. Previously consumed
        // targets may need ordinary engine crash recovery, but still require
        // the complete current-state walk selected below (ADR-0295).
        let store = if native
            .as_ref()
            .is_some_and(|native| !native.requires_complete_validation())
        {
            crate::RedbStore::open_private_stage_authorization(
                &self.database_file,
                application_commit_profile,
                publication,
            )?
        } else {
            crate::RedbStore::open_with_changelog_publication_port(
                &self.database_file,
                application_commit_profile,
                publication,
            )?
        };
        let read = store
            .shared
            .database
            .begin_read()
            .map_err(crate::error::transaction_error)?;
        let retained = crate::startup::read_retained_metadata_pub(&read)?;
        if retained.database_id() != manifest.database_id()
            || retained.history_incarnation() != incarnation
            || crate::follower_lifecycle::is_attached(&read)?
            || crate::promotion_cutover::current_record(&read)?.is_some()
            || crate::retention::durable_application_head(&read)?
                != manifest
                    .last_commit_sequence()
                    .map_or(0, |sequence| sequence.get())
        {
            return Err(corrupt());
        }
        if let Some(native) = &native {
            let intent = native.intent();
            let history =
                crate::changelog_v3_roots::read_checkpoint_roots(&read)?.ok_or_else(corrupt)?;
            let watermark = crate::retention::load_watermark(&read)?
                .map_or(0, |value| value.watermark_sequence());
            if history.lineage() != intent.history.lineage()
                || history.tail().frontier().application()
                    != intent.history.tail().frontier().application()
                || history.tail().frontier().administration()
                    < intent.history.tail().frontier().administration()
                || watermark != intent.retention_watermark
            {
                return Err(corrupt());
            }
        }
        drop(read);
        self.verify_path_ownership()?;
        if native.is_some_and(|native| native.requires_complete_validation()) {
            return store.begin_native_published_restore_recovery(inputs);
        }
        store.begin_published_restore_readiness(inputs, manifest.history_incarnation().unwrap_or(1))
    }
}

#[cfg(test)]
#[path = "publication_startup_tests.rs"]
mod native_transfer_tests;
