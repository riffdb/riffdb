//! Receipt-bound ordinary restore reopen. This grants no complete scrub proof.
use super::*;
use redb::ReadableDatabase;
use riffdb_storage_api::ChangelogPublicationPort;
use std::sync::Arc;

impl RedbMaintenanceStorage {
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
        let (manifest, identity) =
            validate_immutable_backup(&self.named_backup_directory(receipt.backup_name()))?;
        let incarnation = receipt
            .published_history_incarnation()
            .ok_or_else(invariant)?;
        if receipt.manifest_identity() != Some(&identity)
            || receipt.staged_database_id() != Some(manifest.database_id())
        {
            return Err(corrupt());
        }
        let store = crate::RedbStore::open_with_changelog_publication_port(
            &self.database_file,
            application_commit_profile,
            publication,
        )?;
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
        drop(read);
        self.verify_path_ownership()?;
        store.begin_published_restore_readiness(inputs, manifest.history_incarnation().unwrap_or(1))
    }
}
