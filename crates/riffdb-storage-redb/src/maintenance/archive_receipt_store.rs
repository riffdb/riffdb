//! V3 receipt persistence under the existing exclusive maintenance owner.
use super::*;
use crate::maintenance::codec::encode_archive_receipt;
use riffdb_storage_api::{
    OfflineMaintenanceReceiptCreateResultV3, OfflineMaintenanceReceiptReplaceResultV3,
};

impl RedbMaintenanceStorage {
    fn archive_receipt_path(&self, id: OfflineMaintenanceOperationId) -> PathBuf {
        self.receipts_directory.join(archive_receipt_file_name(id))
    }
    fn sync_archive_receipt_parent(&self) -> Result<(), StorageError> {
        // Equality in the page cache is not resolution of an uncertain rename.
        self.receipts_directory_guard
            .sync()
            .map_err(|_| unknown())?;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptParentSync, true)
    }
    fn write_archive_receipt(
        &self,
        receipt: &OfflineMaintenanceReceiptV3,
    ) -> Result<(), StorageError> {
        let destination = self.archive_receipt_path(receipt.operation_id());
        let temporary = self.receipts_directory.join(format!(
            ".{}{}",
            receipt.operation_id(),
            ARCHIVE_RECEIPT_TEMP_SUFFIX
        ));
        let bytes = encode_archive_receipt(receipt)?;
        let mut file = TemporaryReceiptFile::create(&temporary)?;
        file.write_all(&bytes)?;
        self.hit(RedbMaintenanceFailpoint::BeforeReceiptFileSync, false)?;
        file.sync_all()?;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptFileSync, false)?;
        self.hit(RedbMaintenanceFailpoint::BeforeReceiptRename, false)?;
        fs::rename(&temporary, &destination).map_err(io_unavailable)?;
        file.published = true;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptRename, true)?;
        self.sync_archive_receipt_parent()
    }
}
impl OfflineArchiveReceiptPersistencePort for RedbMaintenanceStorage {
    fn create_or_read_archive_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV3,
    ) -> Result<OfflineMaintenanceReceiptCreateResultV3, StorageError> {
        self.verify_path_ownership()?;
        let (v1, v2, v3, _) = self.read_complete_inventory(false)?;
        if v1
            .receipts()
            .iter()
            .any(|r| r.operation_id() == receipt.operation_id())
            || v2
                .receipts()
                .iter()
                .any(|r| r.operation_id() == receipt.operation_id())
        {
            return Err(corrupt());
        }
        if let Some(existing) = v3
            .receipts()
            .iter()
            .find(|r| r.operation_id() == receipt.operation_id())
        {
            let result =
                OfflineMaintenanceReceiptCreateResultV3::Existing(Box::new(existing.clone()));
            self.sync_archive_receipt_parent()?;
            self.verify_path_ownership()?;
            return Ok(result);
        }
        if receipt.current_phase() != OfflineMaintenanceReceiptPhaseV1::Accepted
            || v1.receipts().len() + v2.receipts().len() + v3.receipts().len()
                >= MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1
            || v1
                .receipts()
                .iter()
                .any(|r| !r.current_phase().is_terminal())
            || v2
                .receipts()
                .iter()
                .any(|r| !r.current_phase().is_terminal())
            || v3
                .receipts()
                .iter()
                .any(|r| !r.current_phase().is_terminal())
            || self
                .validate_migration_inventory()?
                .iter()
                .any(|r| !r.current_phase().is_terminal())
        {
            return Err(invariant());
        }
        self.write_archive_receipt(receipt)?;
        self.verify_path_ownership()?;
        Ok(OfflineMaintenanceReceiptCreateResultV3::Created)
    }
    fn replace_archive_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV3,
    ) -> Result<OfflineMaintenanceReceiptReplaceResultV3, StorageError> {
        self.verify_path_ownership()?;
        let current = self
            .read_archive_receipt(receipt.operation_id())?
            .ok_or_else(corrupt)?;
        if current == *receipt {
            self.sync_archive_receipt_parent()?;
            self.verify_path_ownership()?;
            return Ok(OfflineMaintenanceReceiptReplaceResultV3::AlreadyCurrent);
        }
        if !receipt.monotonically_extends(&current) {
            return Err(invariant());
        }
        self.write_archive_receipt(receipt)?;
        self.verify_path_ownership()?;
        Ok(OfflineMaintenanceReceiptReplaceResultV3::Replaced)
    }
    fn read_archive_receipt(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<Option<OfflineMaintenanceReceiptV3>, StorageError> {
        self.verify_path_ownership()?;
        for path in [self.receipt_path(id), self.retire_receipt_path(id)] {
            match fs::symlink_metadata(path) {
                Ok(_) => return Err(corrupt()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_unavailable(e)),
            }
        }
        let path = self.archive_receipt_path(id);
        let result = match fs::symlink_metadata(&path) {
            Ok(metadata)
                if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
            {
                Some(read_archive_receipt_file(&path)?)
            }
            Ok(_) => return Err(corrupt()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(io_unavailable(e)),
        };
        self.verify_path_ownership()?;
        Ok(result)
    }
    fn validate_archive_receipt_inventory(
        &mut self,
    ) -> Result<OfflineMaintenanceReceiptInventoryV3, StorageError> {
        self.verify_path_ownership()?;
        let (_, _, v3, _) = self.read_complete_inventory(false)?;
        self.verify_path_ownership()?;
        Ok(v3)
    }
}
pub(super) fn archive_receipt_file_name(id: OfflineMaintenanceOperationId) -> String {
    format!("{id}{ARCHIVE_RECEIPT_FILE_SUFFIX}")
}
pub(super) fn read_archive_receipt_file(
    path: &Path,
) -> Result<OfflineMaintenanceReceiptV3, StorageError> {
    let metadata = fs::symlink_metadata(path).map_err(io_unavailable)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(corrupt());
    }
    let mut file = File::open(path).map_err(io_unavailable)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take((MAX_RECEIPT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(io_unavailable)?;
    decode_archive_receipt(&bytes)
}

impl RedbMaintenanceStorage {
    /// Publishes a separately validated/authorized archive stage only through its
    /// exact durable Offline V3 receipt and previously persisted incarnation decision.
    /// The server must still perform post-publication startup before reporting success.
    pub fn publish_sealed_archive_restore(
        &mut self,
        sealed: crate::maintenance::RedbSealedArchiveRestore,
    ) -> Result<riffdb_storage_api::OfflineArchiveRestorePublicationV3, StorageError> {
        use riffdb_storage_api::OfflineArchiveRestorePublicationV3;
        self.verify_path_ownership()?;
        if sealed.configured_database_file() != self.database_file {
            return Err(invariant());
        }
        let receipt = self
            .read_archive_receipt(sealed.operation_id())?
            .ok_or_else(corrupt)?;
        let restored_frontier = sealed.restored_frontier();
        let published_history_incarnation = receipt
            .published_history_incarnation()
            .ok_or_else(invariant)?;
        let overwrite_policy = match receipt.replacement_confirmation() {
            OfflineMaintenanceReplacementConfirmation::NotProvided => {
                OfflineRestoreOverwritePolicyV1::RefuseNonEmpty
            }
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget => {
                OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive
            }
        };
        // Resolve any retained rename uncertainty before receipt-authorized mutation.
        self.sync_archive_receipt_parent()?;
        let sealed = sealed.stamp_and_seal(&receipt)?;
        let result = match sealed.publish_to_configured_database(overwrite_policy)? {
            OfflineRestoreResultV1::TargetNotEmpty => {
                OfflineArchiveRestorePublicationV3::TargetNotEmpty
            }
            OfflineRestoreResultV1::Restored { manifest } => {
                OfflineArchiveRestorePublicationV3::Published {
                    backup_manifest: manifest,
                    restored_frontier,
                    published_history_incarnation,
                }
            }
        };
        self.verify_path_ownership()?;
        Ok(result)
    }
}

impl RedbMaintenanceStorage {
    /// Rebuilds an admitted Offline archive candidate from its exact selected
    /// backup. Replay must still use the receipt's selected prefix and pass full
    /// validation and staged authorization before publication. Prior private
    /// stage bytes do not substitute for receipt authority.
    pub fn stage_archive_restore(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
        validation_inputs: StartupValidationInputs,
    ) -> Result<RedbStagedRestore, StorageError> {
        self.verify_path_ownership()?;
        let receipt = self
            .read_archive_receipt(operation_id)?
            .ok_or_else(corrupt)?;
        if receipt.current_phase() != OfflineMaintenanceReceiptPhaseV1::Offline {
            return Err(invariant());
        }
        let selection = receipt.selection().ok_or_else(invariant)?;
        let directory = self.named_backup_directory(receipt.backup_name());
        let (_, identity) = validate_immutable_backup(&directory)?;
        if selection.backup() != &identity {
            return Err(corrupt());
        }
        self.sync_archive_receipt_parent()?;
        let stage = RedbStagedRestore::materialize(
            operation_id,
            receipt.backup_name().clone(),
            directory,
            self.staged_directory.join(operation_id.to_string()),
            self.database_file.clone(),
            validation_inputs,
            self.test_controller.clone(),
        )?;
        self.verify_path_ownership()?;
        Ok(stage)
    }
}
