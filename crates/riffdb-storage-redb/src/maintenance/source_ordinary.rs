//! Closed ordinary access to the installed whole maintenance owner.
//! No mutable owner reference, owned take, callback or Source execution is exposed.
use super::*;
use riffdb_storage_api::{
    ArchiveEncryptionPostureV1, OfflineMaintenanceReceiptCreateResultV3,
    OfflineMaintenanceReceiptReplaceResultV3, ReplicationPromotionReceiptV1,
    StoredPromotionAdministrationV1,
};
use std::sync::Arc;

mod sealed {
    pub trait Sealed {}
}

/// Finite ordinary adapter surface; implementations remain native-owned.
/// Shared access cannot move the non-Clone maintenance owner out of its slot.
#[doc(hidden)]
pub trait SourceMaintenanceAccessV1:
    sealed::Sealed + OfflineMaintenanceReceiptPersistencePort + OfflineArchiveReceiptPersistencePort
{
    /// Borrows existing ordinary read authority, never its mutable whole owner.
    fn ordinary_ref(&self) -> &RedbMaintenanceStorage;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn open_archive_restore_repository(
        &mut self,
        id: OfflineMaintenanceOperationId,
        archive: &Path,
        encryption: ArchiveEncryptionPostureV1,
    ) -> Result<crate::RedbArchiveRepository, StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn archive_restore_target_matches(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<bool, StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn archive_restore_validation_evidence(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<Box<dyn riffdb_storage_api::AuthoritativeStateCursorV3>, StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn reconcile_for_startup(&mut self) -> Result<RedbMaintenanceReconciliation, StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn prepare_backup_retirement(
        &mut self,
        backup_name: &BackupNameV1,
    ) -> Result<
        (
            OfflineMaintenanceOperationId,
            OfflineBackupManifestIdentityV1,
        ),
        StorageError,
    >;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn publish_backup_retirement(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn delete_published_backup_retirement(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn reconcile(&mut self) -> Result<RedbMaintenanceReconciliation, StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn apply_promotion_cutover(
        &mut self,
        record: &StoredPromotionAdministrationV1,
    ) -> Result<(), StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn persist_promotion_receipt(
        &mut self,
        receipt: &riffdb_storage_api::ReplicationPromotionReceiptV1,
    ) -> Result<(), StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn publish_sealed_archive_restore(
        &mut self,
        sealed: crate::maintenance::RedbSealedArchiveRestore,
    ) -> Result<riffdb_storage_api::OfflineArchiveRestorePublicationV3, StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn reconcile_committed_promotion(
        &mut self,
        record: &riffdb_storage_api::StoredPromotionAdministrationV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<std::sync::atomic::AtomicBool>,
        profile: crate::RedbCommitProfile,
        publication: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<crate::RedbStore, StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn reconcile_restored_promotion(
        &mut self,
        attempt: &ReplicationPromotionReceiptV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<std::sync::atomic::AtomicBool>,
        profile: crate::RedbCommitProfile,
        publication: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<crate::RedbStore, StorageError>;
    /// Delegates the existing checked ordinary operation without changing ownership.
    fn stage_archive_restore(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
        validation_inputs: StartupValidationInputs,
    ) -> Result<RedbStagedRestore, StorageError>;
}

/// Private-borrow facade of one installed owner. It cannot be cloned or detached.
#[doc(hidden)]
pub struct SourceOrdinaryMaintenanceV1<'slot> {
    owner: &'slot mut RedbMaintenanceStorage,
}
impl<'slot> SourceOrdinaryMaintenanceV1<'slot> {
    pub(super) fn new(owner: &'slot mut RedbMaintenanceStorage) -> Self {
        Self { owner }
    }
    /// Borrows only shared ordinary authority, preserving the installed owner.
    pub fn ordinary_ref(&self) -> &RedbMaintenanceStorage {
        self.owner
    }
}
// Keep the exclusive installed-owner borrow until the facade is explicitly
// retired, including a facade whose methods are no longer used by its caller.
impl Drop for SourceOrdinaryMaintenanceV1<'_> {
    fn drop(&mut self) {}
}
impl sealed::Sealed for RedbMaintenanceStorage {}
impl sealed::Sealed for SourceOrdinaryMaintenanceV1<'_> {}

impl SourceMaintenanceAccessV1 for RedbMaintenanceStorage {
    fn ordinary_ref(&self) -> &RedbMaintenanceStorage {
        self
    }
    fn open_archive_restore_repository(
        &mut self,
        id: OfflineMaintenanceOperationId,
        archive: &Path,
        encryption: ArchiveEncryptionPostureV1,
    ) -> Result<crate::RedbArchiveRepository, StorageError> {
        RedbMaintenanceStorage::open_archive_restore_repository(self, id, archive, encryption)
    }
    fn archive_restore_target_matches(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<bool, StorageError> {
        RedbMaintenanceStorage::archive_restore_target_matches(self, id)
    }
    fn archive_restore_validation_evidence(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<Box<dyn riffdb_storage_api::AuthoritativeStateCursorV3>, StorageError> {
        RedbMaintenanceStorage::archive_restore_validation_evidence(self, id)
    }
    fn reconcile_for_startup(&mut self) -> Result<RedbMaintenanceReconciliation, StorageError> {
        RedbMaintenanceStorage::reconcile_for_startup(self)
    }
    fn prepare_backup_retirement(
        &mut self,
        backup_name: &BackupNameV1,
    ) -> Result<
        (
            OfflineMaintenanceOperationId,
            OfflineBackupManifestIdentityV1,
        ),
        StorageError,
    > {
        RedbMaintenanceStorage::prepare_backup_retirement(self, backup_name)
    }
    fn publish_backup_retirement(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError> {
        RedbMaintenanceStorage::publish_backup_retirement(self, operation_id)
    }
    fn delete_published_backup_retirement(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError> {
        RedbMaintenanceStorage::delete_published_backup_retirement(self, operation_id)
    }
    fn reconcile(&mut self) -> Result<RedbMaintenanceReconciliation, StorageError> {
        RedbMaintenanceStorage::reconcile(self)
    }
    fn apply_promotion_cutover(
        &mut self,
        record: &StoredPromotionAdministrationV1,
    ) -> Result<(), StorageError> {
        RedbMaintenanceStorage::apply_promotion_cutover(self, record)
    }
    fn persist_promotion_receipt(
        &mut self,
        receipt: &riffdb_storage_api::ReplicationPromotionReceiptV1,
    ) -> Result<(), StorageError> {
        RedbMaintenanceStorage::persist_promotion_receipt(self, receipt)
    }
    fn publish_sealed_archive_restore(
        &mut self,
        sealed: crate::maintenance::RedbSealedArchiveRestore,
    ) -> Result<riffdb_storage_api::OfflineArchiveRestorePublicationV3, StorageError> {
        RedbMaintenanceStorage::publish_sealed_archive_restore(self, sealed)
    }
    fn reconcile_committed_promotion(
        &mut self,
        record: &riffdb_storage_api::StoredPromotionAdministrationV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<std::sync::atomic::AtomicBool>,
        profile: crate::RedbCommitProfile,
        publication: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<crate::RedbStore, StorageError> {
        RedbMaintenanceStorage::reconcile_committed_promotion(
            self,
            record,
            inputs,
            cancellation,
            profile,
            publication,
        )
    }
    fn reconcile_restored_promotion(
        &mut self,
        attempt: &ReplicationPromotionReceiptV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<std::sync::atomic::AtomicBool>,
        profile: crate::RedbCommitProfile,
        publication: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<crate::RedbStore, StorageError> {
        RedbMaintenanceStorage::reconcile_restored_promotion(
            self,
            attempt,
            inputs,
            cancellation,
            profile,
            publication,
        )
    }
    fn stage_archive_restore(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
        validation_inputs: StartupValidationInputs,
    ) -> Result<RedbStagedRestore, StorageError> {
        RedbMaintenanceStorage::stage_archive_restore(self, operation_id, validation_inputs)
    }
}

impl SourceMaintenanceAccessV1 for SourceOrdinaryMaintenanceV1<'_> {
    fn ordinary_ref(&self) -> &RedbMaintenanceStorage {
        self.owner
    }
    fn open_archive_restore_repository(
        &mut self,
        id: OfflineMaintenanceOperationId,
        archive: &Path,
        encryption: ArchiveEncryptionPostureV1,
    ) -> Result<crate::RedbArchiveRepository, StorageError> {
        self.owner
            .open_archive_restore_repository(id, archive, encryption)
    }
    fn archive_restore_target_matches(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<bool, StorageError> {
        self.owner.archive_restore_target_matches(id)
    }
    fn archive_restore_validation_evidence(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<Box<dyn riffdb_storage_api::AuthoritativeStateCursorV3>, StorageError> {
        self.owner.archive_restore_validation_evidence(id)
    }
    fn reconcile_for_startup(&mut self) -> Result<RedbMaintenanceReconciliation, StorageError> {
        self.owner.reconcile_for_startup()
    }
    fn prepare_backup_retirement(
        &mut self,
        backup_name: &BackupNameV1,
    ) -> Result<
        (
            OfflineMaintenanceOperationId,
            OfflineBackupManifestIdentityV1,
        ),
        StorageError,
    > {
        self.owner.prepare_backup_retirement(backup_name)
    }
    fn publish_backup_retirement(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError> {
        self.owner.publish_backup_retirement(operation_id)
    }
    fn delete_published_backup_retirement(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError> {
        self.owner.delete_published_backup_retirement(operation_id)
    }
    fn reconcile(&mut self) -> Result<RedbMaintenanceReconciliation, StorageError> {
        self.owner.reconcile()
    }
    fn apply_promotion_cutover(
        &mut self,
        record: &StoredPromotionAdministrationV1,
    ) -> Result<(), StorageError> {
        self.owner.apply_promotion_cutover(record)
    }
    fn persist_promotion_receipt(
        &mut self,
        receipt: &riffdb_storage_api::ReplicationPromotionReceiptV1,
    ) -> Result<(), StorageError> {
        self.owner.persist_promotion_receipt(receipt)
    }
    fn publish_sealed_archive_restore(
        &mut self,
        sealed: crate::maintenance::RedbSealedArchiveRestore,
    ) -> Result<riffdb_storage_api::OfflineArchiveRestorePublicationV3, StorageError> {
        self.owner.publish_sealed_archive_restore(sealed)
    }
    fn reconcile_committed_promotion(
        &mut self,
        record: &riffdb_storage_api::StoredPromotionAdministrationV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<std::sync::atomic::AtomicBool>,
        profile: crate::RedbCommitProfile,
        publication: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<crate::RedbStore, StorageError> {
        self.owner
            .reconcile_committed_promotion(record, inputs, cancellation, profile, publication)
    }
    fn reconcile_restored_promotion(
        &mut self,
        attempt: &ReplicationPromotionReceiptV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<std::sync::atomic::AtomicBool>,
        profile: crate::RedbCommitProfile,
        publication: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<crate::RedbStore, StorageError> {
        self.owner
            .reconcile_restored_promotion(attempt, inputs, cancellation, profile, publication)
    }
    fn stage_archive_restore(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
        validation_inputs: StartupValidationInputs,
    ) -> Result<RedbStagedRestore, StorageError> {
        self.owner
            .stage_archive_restore(operation_id, validation_inputs)
    }
}

impl OfflineMaintenanceReceiptPersistencePort for SourceOrdinaryMaintenanceV1<'_> {
    fn create_or_read_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV1,
    ) -> Result<OfflineMaintenanceReceiptCreateResultV1, StorageError> {
        self.owner.create_or_read_receipt(receipt)
    }
    fn replace_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV1,
    ) -> Result<OfflineMaintenanceReceiptReplaceResultV1, StorageError> {
        self.owner.replace_receipt(receipt)
    }
    fn read_receipt(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<Option<OfflineMaintenanceReceiptV1>, StorageError> {
        self.owner.read_receipt(operation_id)
    }
    fn validate_receipt_inventory(
        &mut self,
    ) -> Result<OfflineMaintenanceReceiptInventoryV1, StorageError> {
        self.owner.validate_receipt_inventory()
    }
    fn create_or_read_retire_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV2,
    ) -> Result<OfflineMaintenanceReceiptCreateResultV2, StorageError> {
        self.owner.create_or_read_retire_receipt(receipt)
    }
    fn replace_retire_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV2,
    ) -> Result<OfflineMaintenanceReceiptReplaceResultV2, StorageError> {
        self.owner.replace_retire_receipt(receipt)
    }
    fn read_retire_receipt(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<Option<OfflineMaintenanceReceiptV2>, StorageError> {
        self.owner.read_retire_receipt(operation_id)
    }
    fn validate_retire_receipt_inventory(
        &mut self,
    ) -> Result<OfflineMaintenanceReceiptInventoryV2, StorageError> {
        self.owner.validate_retire_receipt_inventory()
    }
    fn create_or_read_scrub_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV4,
    ) -> Result<OfflineMaintenanceReceiptCreateResultV4, StorageError> {
        self.owner.create_or_read_scrub_receipt(receipt)
    }
    fn replace_scrub_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV4,
    ) -> Result<OfflineMaintenanceReceiptReplaceResultV4, StorageError> {
        self.owner.replace_scrub_receipt(receipt)
    }
    fn read_scrub_receipt(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<Option<OfflineMaintenanceReceiptV4>, StorageError> {
        self.owner.read_scrub_receipt(operation_id)
    }
    fn validate_scrub_receipt_inventory(
        &mut self,
    ) -> Result<OfflineMaintenanceReceiptInventoryV4, StorageError> {
        self.owner.validate_scrub_receipt_inventory()
    }
}

impl OfflineArchiveReceiptPersistencePort for SourceOrdinaryMaintenanceV1<'_> {
    fn create_or_read_archive_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV3,
    ) -> Result<OfflineMaintenanceReceiptCreateResultV3, StorageError> {
        self.owner.create_or_read_archive_receipt(receipt)
    }
    fn replace_archive_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV3,
    ) -> Result<OfflineMaintenanceReceiptReplaceResultV3, StorageError> {
        self.owner.replace_archive_receipt(receipt)
    }
    fn read_archive_receipt(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<Option<OfflineMaintenanceReceiptV3>, StorageError> {
        self.owner.read_archive_receipt(id)
    }
    fn validate_archive_receipt_inventory(
        &mut self,
    ) -> Result<OfflineMaintenanceReceiptInventoryV3, StorageError> {
        self.owner.validate_archive_receipt_inventory()
    }
}
