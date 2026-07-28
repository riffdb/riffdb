use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use riffdb_storage_api::{
    MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1, OfflineBackupManifestIdentityV1, OfflineBackupManifestV1,
    OfflineBackupPersistencePort, OfflineMaintenanceReceiptCreateResultV1,
    OfflineMaintenanceReceiptInventoryV1, OfflineMaintenanceReceiptPersistencePort,
    OfflineMaintenanceReceiptPhaseV1, OfflineMaintenanceReceiptReplaceResultV1,
    OfflineMaintenanceReceiptV1, OfflineRestoreOverwritePolicyV1, OfflineRestoreResultV1,
    StorageError, StorageErrorKind, StorageValueError,
};
use riffdb_types::{
    BackupNameV1, OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation,
};

use super::codec::{
    MAX_RECEIPT_BYTES, RECEIPT_FILE_SUFFIX, RECEIPT_TEMP_SUFFIX, decode_receipt, encode_receipt,
};
use super::failpoint::{RedbMaintenanceFailpoint, RedbMaintenanceTestController};
use super::path_guard::{PinnedDirectory, verify_regular_file_path};
use super::staged::{RedbSealedStagedRestore, RedbStagedRestore};
use crate::backup::{
    RedbOfflineBackup, lock_existing_target, validate_database_semantics, validate_immutable_backup,
};
use crate::error::storage_error;

const MAINTENANCE_DIRECTORY_NAME: &str = ".maintenance";
const OWNERSHIP_LOCK_FILE_NAME: &str = "owner.lock";
const RECEIPTS_DIRECTORY_NAME: &str = "receipts";
const STAGED_DIRECTORY_NAME: &str = "staged";
const MAX_UNPUBLISHED_RECEIPT_TEMPS: usize = 256;

/// Filesystem evidence for one checked external maintenance receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedbMaintenanceOperationEvidence {
    operation_id: OfflineMaintenanceOperationId,
    named_backup_matches: bool,
    staged_restore_matches: bool,
    configured_target_matches: bool,
}

impl RedbMaintenanceOperationEvidence {
    /// Returns the receipt operation identity.
    #[must_use]
    pub const fn operation_id(self) -> OfflineMaintenanceOperationId {
        self.operation_id
    }

    /// Returns whether the complete immutable named backup matches the receipt.
    #[must_use]
    pub const fn named_backup_matches(self) -> bool {
        self.named_backup_matches
    }

    /// Returns whether a private stage retains the named backup's semantic identity.
    ///
    /// Exact bytes may differ after an accepted startup migration.
    #[must_use]
    pub const fn staged_restore_matches(self) -> bool {
        self.staged_restore_matches
    }

    /// Returns whether an incomplete restore's published target matches its backup.
    #[must_use]
    pub const fn configured_target_matches(self) -> bool {
        self.configured_target_matches
    }
}

/// Bounded startup reconciliation evidence for the reserved subtree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedbMaintenanceReconciliation {
    receipts: OfflineMaintenanceReceiptInventoryV1,
    operations: Vec<RedbMaintenanceOperationEvidence>,
    removed_unpublished_receipt_temps: usize,
    removed_unpublished_target_temps: usize,
    removed_incomplete_stages: usize,
    removed_terminal_stages: usize,
}

impl RedbMaintenanceReconciliation {
    /// Borrows every checksum-validated receipt in canonical operation order.
    #[must_use]
    pub const fn receipts(&self) -> &OfflineMaintenanceReceiptInventoryV1 {
        &self.receipts
    }

    /// Borrows exact artifact evidence in the same canonical operation order.
    #[must_use]
    pub fn operations(&self) -> &[RedbMaintenanceOperationEvidence] {
        &self.operations
    }

    /// Returns incomplete temp files discarded because rename never published them.
    #[must_use]
    pub const fn removed_unpublished_receipt_temps(&self) -> usize {
        self.removed_unpublished_receipt_temps
    }

    /// Returns exact operation-bound target temps removed before their rename.
    #[must_use]
    pub const fn removed_unpublished_target_temps(&self) -> usize {
        self.removed_unpublished_target_temps
    }

    /// Returns incomplete private stages discarded before publication.
    #[must_use]
    pub const fn removed_incomplete_stages(&self) -> usize {
        self.removed_incomplete_stages
    }

    /// Returns no-longer-needed stages removed for terminal receipts.
    #[must_use]
    pub const fn removed_terminal_stages(&self) -> usize {
        self.removed_terminal_stages
    }
}

/// Concrete owner of one configured database file and external backup root.
///
/// This adapter is intentionally neither `Clone` nor an ordinary operational
/// storage port. It owns only offline artifact and receipt mechanics.
pub struct RedbMaintenanceStorage {
    database_file: PathBuf,
    backup_root: PathBuf,
    receipts_directory: PathBuf,
    staged_directory: PathBuf,
    database_parent_guard: PinnedDirectory,
    backup_root_guard: PinnedDirectory,
    maintenance_directory_guard: PinnedDirectory,
    receipts_directory_guard: PinnedDirectory,
    staged_directory_guard: PinnedDirectory,
    ownership_lock_path: PathBuf,
    ownership_lock: File,
    test_controller: Option<RedbMaintenanceTestController>,
}

impl std::fmt::Debug for RedbMaintenanceStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedbMaintenanceStorage")
            .field("paths", &"[REDACTED]")
            .finish()
    }
}

impl RedbMaintenanceStorage {
    /// Opens, validates, and reconciles the complete reserved maintenance subtree.
    pub fn open(
        database_file: impl AsRef<Path>,
        backup_root: impl AsRef<Path>,
    ) -> Result<(Self, RedbMaintenanceReconciliation), StorageError> {
        Self::open_inner(database_file.as_ref(), backup_root.as_ref(), None)
    }

    /// Opens with one closed process-test failpoint controller.
    #[doc(hidden)]
    pub fn open_with_test_controller(
        database_file: impl AsRef<Path>,
        backup_root: impl AsRef<Path>,
        controller: RedbMaintenanceTestController,
    ) -> Result<(Self, RedbMaintenanceReconciliation), StorageError> {
        Self::open_inner(
            database_file.as_ref(),
            backup_root.as_ref(),
            Some(controller),
        )
    }

    /// Proves that the configured target is absent, empty, or backend-corrupt.
    ///
    /// `false` means only that the backend file opens; complete authoritative
    /// RiffDB startup validation remains mandatory before ordinary readiness.
    /// Environmental or uncertain open failures are returned rather than
    /// being treated as permission to replace the target.
    pub fn configured_target_requires_recovery(&self) -> Result<bool, StorageError> {
        self.verify_path_ownership()?;
        let requires_recovery = match fs::symlink_metadata(&self.database_file) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(io_unavailable(error)),
            Ok(metadata) if !metadata.file_type().is_file() => Err(corrupt()),
            Ok(metadata) if metadata.len() == 0 => Ok(true),
            Ok(_) => match lock_existing_target(&self.database_file)? {
                Some(database) => {
                    drop(database);
                    Ok(false)
                }
                None => Ok(true),
            },
        }?;
        self.verify_path_ownership()?;
        Ok(requires_recovery)
    }

    fn open_inner(
        database_file: &Path,
        backup_root: &Path,
        test_controller: Option<RedbMaintenanceTestController>,
    ) -> Result<(Self, RedbMaintenanceReconciliation), StorageError> {
        let database_file = resolve_database_file(database_file)?;
        validate_configured_paths(&database_file, backup_root)?;
        create_checked_directory(backup_root)?;
        let database_parent = database_file.parent().ok_or_else(invariant)?;
        let database_parent_guard = PinnedDirectory::open(database_parent)?;
        let backup_root_guard = PinnedDirectory::open(backup_root)?;
        database_parent_guard.verify()?;
        backup_root_guard.verify()?;

        let maintenance_directory = backup_root.join(MAINTENANCE_DIRECTORY_NAME);
        create_checked_directory(&maintenance_directory)?;
        backup_root_guard.verify()?;
        let maintenance_directory_guard = PinnedDirectory::open(&maintenance_directory)?;
        let ownership_lock_path = maintenance_directory.join(OWNERSHIP_LOCK_FILE_NAME);
        let ownership_lock = acquire_exclusive_ownership_lock(&ownership_lock_path)?;

        let receipts_directory = maintenance_directory.join(RECEIPTS_DIRECTORY_NAME);
        let staged_directory = maintenance_directory.join(STAGED_DIRECTORY_NAME);
        create_checked_directory(&receipts_directory)?;
        create_checked_directory(&staged_directory)?;
        maintenance_directory_guard.verify()?;
        let receipts_directory_guard = PinnedDirectory::open(&receipts_directory)?;
        let staged_directory_guard = PinnedDirectory::open(&staged_directory)?;
        validate_reserved_inventory(&maintenance_directory)?;

        let mut storage = Self {
            database_file,
            backup_root: backup_root.to_path_buf(),
            receipts_directory,
            staged_directory,
            database_parent_guard,
            backup_root_guard,
            maintenance_directory_guard,
            receipts_directory_guard,
            staged_directory_guard,
            ownership_lock_path,
            ownership_lock,
            test_controller,
        };
        storage.verify_path_ownership()?;
        let reconciliation = storage.reconcile()?;
        Ok((storage, reconciliation))
    }

    fn verify_path_ownership(&self) -> Result<(), StorageError> {
        self.database_parent_guard.verify()?;
        self.backup_root_guard.verify()?;
        self.maintenance_directory_guard.verify()?;
        self.receipts_directory_guard.verify()?;
        self.staged_directory_guard.verify()?;
        verify_regular_file_path(&self.ownership_lock, &self.ownership_lock_path)
    }

    /// Returns the exact configured database file for server-private validation.
    #[must_use]
    pub fn configured_database_file(&self) -> &Path {
        &self.database_file
    }

    /// Creates one immutable named WP-070 backup of the configured database file.
    pub fn create_named_backup(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        backup_name: &BackupNameV1,
        build: &riffdb_storage_api::BackupBuildMetadataV1,
    ) -> Result<(OfflineBackupManifestV1, OfflineBackupManifestIdentityV1), StorageError> {
        self.verify_path_ownership()?;
        let receipt = self.require_offline_operation(
            operation_id,
            OfflineMaintenanceOperationKind::CreateBackup,
            backup_name,
        )?;
        let backup_directory = self.named_backup_directory(backup_name);
        let (manifest, published_now) = match fs::symlink_metadata(&backup_directory) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (
                RedbOfflineBackup::bind(&self.database_file, &backup_directory)
                    .create_offline_backup(build)?,
                true,
            ),
            Err(error) => return Err(io_unavailable(error)),
            Ok(metadata) if metadata.file_type().is_dir() => {
                (validate_immutable_backup(&backup_directory)?.0, false)
            }
            Ok(_) => return Err(corrupt()),
        };
        let (validated, identity) = validate_immutable_backup(&backup_directory)?;
        if validated != manifest
            || manifest.build() != build
            || receipt.source_database_id() != Some(manifest.database_id())
            || receipt
                .manifest_identity()
                .is_some_and(|expected| expected != &identity)
        {
            return Err(corrupt());
        }
        let source = validate_database_semantics(&self.database_file, &manifest)?;
        drop(source);
        if published_now {
            self.hit(RedbMaintenanceFailpoint::AfterNamedBackupPublication, true)?;
        }
        self.verify_path_ownership()?;
        Ok((manifest, identity))
    }

    /// Materializes one named immutable backup into operation-private staging.
    pub fn stage_restore(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        backup_name: &BackupNameV1,
    ) -> Result<RedbStagedRestore, StorageError> {
        self.verify_path_ownership()?;
        self.require_offline_operation(
            operation_id,
            OfflineMaintenanceOperationKind::RestoreBackup,
            backup_name,
        )?;
        let stage = RedbStagedRestore::materialize(
            operation_id,
            backup_name.clone(),
            self.named_backup_directory(backup_name),
            self.staged_directory.join(operation_id.to_string()),
            self.database_file.clone(),
            self.test_controller.clone(),
        )?;
        self.verify_path_ownership()?;
        Ok(stage)
    }

    /// Materializes a source candidate for staged-only recovery authorization.
    ///
    /// This narrow path is used only when the configured target cannot provide
    /// trustworthy ordinary readiness or current authorization. It creates no
    /// receipt, changes no configured database file, grants no authority, and
    /// returns only the same move-only stage that still requires complete
    /// startup validation, fresh staged authorization, sealing, and a durable
    /// source-less restore receipt before publication.
    pub fn stage_recovery_restore_candidate(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        backup_name: &BackupNameV1,
    ) -> Result<RedbStagedRestore, StorageError> {
        self.verify_path_ownership()?;
        let stage = RedbStagedRestore::materialize(
            operation_id,
            backup_name.clone(),
            self.named_backup_directory(backup_name),
            self.staged_directory.join(operation_id.to_string()),
            self.database_file.clone(),
            self.test_controller.clone(),
        )?;
        self.verify_path_ownership()?;
        Ok(stage)
    }

    /// Removes an exact staged-only recovery attempt before receipt admission.
    ///
    /// This operation is idempotent, but fails closed if the operation already
    /// owns a durable receipt. Receipt-backed stages are reconciliation
    /// evidence and cannot be discarded through this pre-admission boundary.
    pub fn discard_pre_receipt_recovery_stage(
        &self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError> {
        self.verify_path_ownership()?;
        match fs::symlink_metadata(self.receipt_path(operation_id)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_unavailable(error)),
            Ok(_) => return Err(invariant()),
        }
        remove_stage_if_present(&self.staged_directory.join(operation_id.to_string()))?;
        self.verify_path_ownership()
    }

    /// Publishes one sealed stage only after exact receipt evidence is durable.
    pub fn publish_sealed_restore(
        &self,
        sealed: RedbSealedStagedRestore,
        overwrite_policy: OfflineRestoreOverwritePolicyV1,
    ) -> Result<OfflineRestoreResultV1, StorageError> {
        self.verify_path_ownership()?;
        let receipt = read_receipt_file(&self.receipt_path(sealed.operation_id()))?;
        let expected_policy = match receipt.replacement_confirmation() {
            OfflineMaintenanceReplacementConfirmation::NotProvided => {
                OfflineRestoreOverwritePolicyV1::RefuseNonEmpty
            }
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget => {
                OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive
            }
        };
        if receipt.operation_kind() != OfflineMaintenanceOperationKind::RestoreBackup
            || receipt.backup_name() != sealed.backup_name()
            || receipt.current_phase() != OfflineMaintenanceReceiptPhaseV1::Offline
            || receipt.staged_database_id() != Some(sealed.manifest_identity().database_id())
            || receipt.manifest_identity() != Some(sealed.manifest_identity())
            || sealed.configured_database_file() != self.database_file
            || overwrite_policy != expected_policy
        {
            return Err(invariant());
        }
        let result = sealed.publish_to_configured_database(overwrite_policy)?;
        self.verify_path_ownership()?;
        Ok(result)
    }

    /// Revalidates every receipt and associated exact filesystem evidence.
    pub fn reconcile(&mut self) -> Result<RedbMaintenanceReconciliation, StorageError> {
        self.verify_path_ownership()?;
        let (inventory, removed_temps) = self.read_complete_inventory(true)?;
        let incomplete_count = inventory
            .receipts()
            .iter()
            .filter(|receipt| !receipt.current_phase().is_terminal())
            .count();
        if incomplete_count > 1 {
            return Err(invariant());
        }

        let receipt_by_stage_name = inventory
            .receipts()
            .iter()
            .map(|receipt| (receipt.operation_id().to_string(), receipt))
            .collect::<BTreeMap<_, _>>();
        let (staged_names, removed_unadmitted_stages) =
            checked_staged_inventory(&self.staged_directory, &receipt_by_stage_name)?;
        let removed_unpublished_target_temps =
            self.remove_unpublished_target_temps(inventory.receipts())?;
        let mut removed_incomplete_stages = removed_unadmitted_stages;
        let mut removed_terminal_stages = 0usize;
        let mut operations = Vec::with_capacity(inventory.receipts().len());
        for receipt in inventory.receipts() {
            let backup_directory = self.named_backup_directory(receipt.backup_name());
            let named = match fs::symlink_metadata(&backup_directory) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(io_unavailable(error)),
                Ok(_) => Some(validate_immutable_backup(&backup_directory)?),
            };
            let named_backup_matches = named.as_ref().is_some_and(|(_, identity)| {
                receipt
                    .manifest_identity()
                    .is_none_or(|expected| expected == identity)
            });
            if receipt.manifest_identity().is_some() && !named_backup_matches {
                return Err(corrupt());
            }
            if receipt.operation_kind() == OfflineMaintenanceOperationKind::RestoreBackup
                && named.is_none()
            {
                return Err(corrupt());
            }

            let stage_name = receipt.operation_id().to_string();
            let stage_exists = staged_names.contains(&stage_name);
            let staged_artifact_checksum = if stage_exists && receipt.current_phase().is_terminal()
            {
                remove_incomplete_stage(&self.staged_directory.join(&stage_name))?;
                removed_terminal_stages = removed_terminal_stages
                    .checked_add(1)
                    .ok_or_else(limit_exceeded)?;
                None
            } else if stage_exists {
                let Some((manifest, identity)) = named.as_ref() else {
                    return Err(corrupt());
                };
                if receipt
                    .manifest_identity()
                    .is_some_and(|expected| expected != identity)
                {
                    return Err(corrupt());
                }
                let stage_directory = self.staged_directory.join(&stage_name);
                let stage_required = matches!(
                    receipt.current_phase(),
                    OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                        | OfflineMaintenanceReceiptPhaseV1::Validating
                );
                match validate_stage_inventory(&stage_directory) {
                    Ok(()) => {
                        let artifact =
                            stage_directory.join(crate::backup::DATABASE_ARTIFACT_FILE_NAME);
                        let checksum = crate::backup::sha256_file(&artifact)?;
                        match crate::backup::validate_database_semantics(&artifact, manifest) {
                            Ok(database) => {
                                drop(database);
                                Some(checksum)
                            }
                            Err(error)
                                if !stage_required
                                    && matches!(
                                        error.kind(),
                                        StorageErrorKind::CorruptData
                                            | StorageErrorKind::IncompatibleFormat
                                    ) =>
                            {
                                remove_incomplete_stage(&stage_directory)?;
                                removed_incomplete_stages = removed_incomplete_stages
                                    .checked_add(1)
                                    .ok_or_else(limit_exceeded)?;
                                None
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Err(error)
                        if !stage_required
                            && matches!(
                                error.kind(),
                                StorageErrorKind::CorruptData
                                    | StorageErrorKind::IncompatibleFormat
                            ) =>
                    {
                        remove_incomplete_stage(&stage_directory)?;
                        removed_incomplete_stages = removed_incomplete_stages
                            .checked_add(1)
                            .ok_or_else(limit_exceeded)?;
                        None
                    }
                    Err(error) => return Err(error),
                }
            } else {
                None
            };
            let staged_restore_matches = staged_artifact_checksum.is_some();

            let target_must_match = matches!(
                receipt.current_phase(),
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                    | OfflineMaintenanceReceiptPhaseV1::Validating
            );
            let configured_target_matches = if receipt.operation_kind()
                == OfflineMaintenanceOperationKind::RestoreBackup
                && !receipt.current_phase().is_terminal()
                && matches!(
                    receipt.current_phase(),
                    OfflineMaintenanceReceiptPhaseV1::Offline
                        | OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                        | OfflineMaintenanceReceiptPhaseV1::Validating
                ) {
                let Some((manifest, _)) = named.as_ref() else {
                    return Err(corrupt());
                };
                match staged_artifact_checksum.as_ref() {
                    None if target_must_match => return Err(corrupt()),
                    None => false,
                    Some(staged_checksum) => {
                        let target_checksum = match fs::symlink_metadata(&self.database_file) {
                            Ok(metadata) if metadata.file_type().is_file() => {
                                Some(crate::backup::sha256_file(&self.database_file)?)
                            }
                            Ok(_) if target_must_match => return Err(corrupt()),
                            Ok(_) => None,
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                            Err(error) => return Err(io_unavailable(error)),
                        };
                        if target_checksum.as_ref() != Some(staged_checksum) {
                            if target_must_match {
                                return Err(corrupt());
                            }
                            false
                        } else {
                            let database = crate::backup::validate_database_semantics(
                                &self.database_file,
                                manifest,
                            )?;
                            drop(database);
                            true
                        }
                    }
                }
            } else {
                false
            };

            operations.push(RedbMaintenanceOperationEvidence {
                operation_id: receipt.operation_id(),
                named_backup_matches,
                staged_restore_matches,
                configured_target_matches,
            });
        }
        let reconciliation = RedbMaintenanceReconciliation {
            receipts: inventory,
            operations,
            removed_unpublished_receipt_temps: removed_temps,
            removed_unpublished_target_temps,
            removed_incomplete_stages,
            removed_terminal_stages,
        };
        self.verify_path_ownership()?;
        Ok(reconciliation)
    }

    fn remove_unpublished_target_temps(
        &self,
        receipts: &[OfflineMaintenanceReceiptV1],
    ) -> Result<usize, StorageError> {
        let mut removed = 0usize;
        let target_name = self.database_file.file_name().ok_or_else(invariant)?;
        for receipt in receipts {
            if receipt.operation_kind() != OfflineMaintenanceOperationKind::RestoreBackup {
                continue;
            }
            let name = super::staged::target_temporary_name(target_name, receipt.operation_id());
            if self.database_parent_guard.remove_file_if_present(&name)? {
                removed = removed.checked_add(1).ok_or_else(limit_exceeded)?;
            }
        }
        if removed != 0 {
            self.database_parent_guard.sync()?;
        }
        Ok(removed)
    }

    fn named_backup_directory(&self, backup_name: &BackupNameV1) -> PathBuf {
        self.backup_root.join(backup_name.as_str())
    }

    fn require_offline_operation(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        operation_kind: OfflineMaintenanceOperationKind,
        backup_name: &BackupNameV1,
    ) -> Result<OfflineMaintenanceReceiptV1, StorageError> {
        let receipt = read_receipt_file(&self.receipt_path(operation_id))?;
        if receipt.operation_kind() != operation_kind
            || receipt.backup_name() != backup_name
            || receipt.current_phase() != OfflineMaintenanceReceiptPhaseV1::Offline
        {
            return Err(invariant());
        }
        Ok(receipt)
    }

    fn read_complete_inventory(
        &self,
        remove_temps: bool,
    ) -> Result<(OfflineMaintenanceReceiptInventoryV1, usize), StorageError> {
        let mut receipts = Vec::new();
        let mut removed_temps = 0usize;
        for entry in fs::read_dir(&self.receipts_directory).map_err(io_unavailable)? {
            if receipts.len() > MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1 {
                return Err(limit_exceeded());
            }
            let entry = entry.map_err(io_unavailable)?;
            let file_type = entry.file_type().map_err(io_unavailable)?;
            if !file_type.is_file() || file_type.is_symlink() {
                return Err(corrupt());
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(corrupt());
            };
            if let Some(operation_text) = name
                .strip_prefix('.')
                .and_then(|value| value.strip_suffix(RECEIPT_TEMP_SUFFIX))
            {
                if !remove_temps {
                    return Err(corrupt());
                }
                parse_operation_id(operation_text).ok_or_else(corrupt)?;
                if removed_temps == MAX_UNPUBLISHED_RECEIPT_TEMPS {
                    return Err(limit_exceeded());
                }
                fs::remove_file(entry.path()).map_err(io_unavailable)?;
                removed_temps = removed_temps.checked_add(1).ok_or_else(limit_exceeded)?;
                continue;
            }
            if !name.ends_with(RECEIPT_FILE_SUFFIX) {
                return Err(corrupt());
            }
            let receipt = read_receipt_file(&entry.path())?;
            if name != receipt_file_name(receipt.operation_id()) {
                return Err(corrupt());
            }
            receipts.push(receipt);
        }
        if removed_temps != 0 {
            crate::backup::sync_directory(&self.receipts_directory)?;
        }
        let inventory = OfflineMaintenanceReceiptInventoryV1::new(receipts).map_err(value_error)?;
        Ok((inventory, removed_temps))
    }

    fn receipt_path(&self, operation_id: OfflineMaintenanceOperationId) -> PathBuf {
        self.receipts_directory
            .join(receipt_file_name(operation_id))
    }

    fn write_receipt(&self, receipt: &OfflineMaintenanceReceiptV1) -> Result<(), StorageError> {
        let destination = self.receipt_path(receipt.operation_id());
        let temporary = self.receipts_directory.join(format!(
            ".{}{}",
            receipt.operation_id(),
            RECEIPT_TEMP_SUFFIX
        ));
        let bytes = encode_receipt(receipt)?;
        let mut temporary_file = TemporaryReceiptFile::create(&temporary)?;
        temporary_file.write_all(&bytes)?;
        self.hit(RedbMaintenanceFailpoint::BeforeReceiptFileSync, false)?;
        temporary_file.sync_all()?;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptFileSync, false)?;
        self.hit(RedbMaintenanceFailpoint::BeforeReceiptRename, false)?;
        fs::rename(&temporary, &destination).map_err(io_unavailable)?;
        temporary_file.published = true;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptRename, true)?;
        if crate::backup::sync_directory(&self.receipts_directory).is_err() {
            return Err(unknown());
        }
        self.hit(RedbMaintenanceFailpoint::AfterReceiptParentSync, true)
    }

    fn hit(
        &self,
        failpoint: RedbMaintenanceFailpoint,
        publication_may_be_durable: bool,
    ) -> Result<(), StorageError> {
        self.test_controller.as_ref().map_or(Ok(()), |controller| {
            controller.hit(failpoint, publication_may_be_durable)
        })
    }
}

impl OfflineMaintenanceReceiptPersistencePort for RedbMaintenanceStorage {
    fn create_or_read_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV1,
    ) -> Result<OfflineMaintenanceReceiptCreateResultV1, StorageError> {
        self.verify_path_ownership()?;
        let path = self.receipt_path(receipt.operation_id());
        let result = match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                Ok(OfflineMaintenanceReceiptCreateResultV1::Existing(Box::new(
                    read_receipt_file(&path)?,
                )))
            }
            Ok(_) => Err(corrupt()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let (inventory, _) = self.read_complete_inventory(false)?;
                if inventory
                    .receipts()
                    .iter()
                    .any(|existing| !existing.current_phase().is_terminal())
                {
                    return Err(invariant());
                }
                if receipt.operation_kind() == OfflineMaintenanceOperationKind::CreateBackup {
                    match fs::symlink_metadata(self.named_backup_directory(receipt.backup_name())) {
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(io_unavailable(error)),
                        Ok(_) => return Err(storage_error(StorageErrorKind::Unavailable)),
                    }
                }
                self.write_receipt(receipt)?;
                Ok(OfflineMaintenanceReceiptCreateResultV1::Created)
            }
            Err(error) => Err(io_unavailable(error)),
        }?;
        self.verify_path_ownership()?;
        Ok(result)
    }

    fn replace_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV1,
    ) -> Result<OfflineMaintenanceReceiptReplaceResultV1, StorageError> {
        self.verify_path_ownership()?;
        let path = self.receipt_path(receipt.operation_id());
        let current = read_receipt_file(&path)?;
        if current == *receipt {
            self.verify_path_ownership()?;
            return Ok(OfflineMaintenanceReceiptReplaceResultV1::AlreadyCurrent);
        }
        if !receipt.monotonically_extends(&current) {
            return Err(invariant());
        }
        self.write_receipt(receipt)?;
        self.verify_path_ownership()?;
        Ok(OfflineMaintenanceReceiptReplaceResultV1::Replaced)
    }

    fn read_receipt(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<Option<OfflineMaintenanceReceiptV1>, StorageError> {
        self.verify_path_ownership()?;
        let path = self.receipt_path(operation_id);
        let result = match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => read_receipt_file(&path).map(Some),
            Ok(_) => Err(corrupt()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_unavailable(error)),
        }?;
        self.verify_path_ownership()?;
        Ok(result)
    }

    fn validate_receipt_inventory(
        &mut self,
    ) -> Result<OfflineMaintenanceReceiptInventoryV1, StorageError> {
        self.verify_path_ownership()?;
        let (inventory, _) = self.read_complete_inventory(false)?;
        self.verify_path_ownership()?;
        Ok(inventory)
    }
}

struct TemporaryReceiptFile {
    path: PathBuf,
    file: File,
    published: bool,
}

impl TemporaryReceiptFile {
    fn create(path: &Path) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(io_unavailable)?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            published: false,
        })
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
        self.file.write_all(bytes).map_err(io_unavailable)
    }

    fn sync_all(&self) -> Result<(), StorageError> {
        self.file.sync_all().map_err(io_unavailable)
    }
}

impl Drop for TemporaryReceiptFile {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn validate_configured_paths(database_file: &Path, backup_root: &Path) -> Result<(), StorageError> {
    if !is_normal_absolute_path(database_file) || !is_normal_absolute_path(backup_root) {
        return Err(invariant());
    }
    if database_file.starts_with(backup_root) || database_file == backup_root {
        return Err(invariant());
    }
    reject_symlink_components(database_file)?;
    reject_symlink_components(backup_root)?;
    if let Ok(metadata) = fs::symlink_metadata(database_file)
        && metadata.file_type().is_symlink()
    {
        return Err(corrupt());
    }
    Ok(())
}

fn reject_symlink_components(path: &Path) -> Result<(), StorageError> {
    let mut prefix = PathBuf::new();
    for component in path.components() {
        prefix.push(component.as_os_str());
        match fs::symlink_metadata(&prefix) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Err(corrupt()),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(io_unavailable(error)),
        }
    }
    Ok(())
}

fn resolve_database_file(path: &Path) -> Result<PathBuf, StorageError> {
    if path.is_absolute() {
        return is_normal_absolute_path(path)
            .then(|| path.to_path_buf())
            .ok_or_else(invariant);
    }
    if path.as_os_str().is_empty()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(invariant());
    }
    let current_directory = std::env::current_dir().map_err(io_unavailable)?;
    let resolved = current_directory.join(path);
    is_normal_absolute_path(&resolved)
        .then_some(resolved)
        .ok_or_else(invariant)
}

fn is_normal_absolute_path(path: &Path) -> bool {
    path.is_absolute()
        && path.components().all(|component| {
            matches!(
                component,
                Component::Prefix(_) | Component::RootDir | Component::Normal(_)
            )
        })
}

fn create_checked_directory(path: &Path) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(corrupt()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(io_unavailable)?;
            crate::backup::sync_parent(path)
        }
        Err(error) => Err(io_unavailable(error)),
    }
}

fn acquire_exclusive_ownership_lock(path: &Path) -> Result<File, StorageError> {
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => {
            file.sync_all().map_err(io_unavailable)?;
            crate::backup::sync_parent(path)?;
            file
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path).map_err(io_unavailable)?;
            if !metadata.file_type().is_file()
                || metadata.file_type().is_symlink()
                || metadata.len() != 0
            {
                return Err(corrupt());
            }
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(io_unavailable)?
        }
        Err(error) => return Err(io_unavailable(error)),
    };
    verify_regular_file_path(&file, path)?;
    file.try_lock()
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    verify_regular_file_path(&file, path)?;
    Ok(file)
}

fn validate_reserved_inventory(maintenance_directory: &Path) -> Result<(), StorageError> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(maintenance_directory).map_err(io_unavailable)? {
        let entry = entry.map_err(io_unavailable)?;
        let name = entry.file_name();
        let file_type = entry.file_type().map_err(io_unavailable)?;
        let expected_type = if name == OsStr::new(OWNERSHIP_LOCK_FILE_NAME) {
            file_type.is_file() && !file_type.is_symlink()
        } else if matches!(
            name.to_str(),
            Some(RECEIPTS_DIRECTORY_NAME | STAGED_DIRECTORY_NAME)
        ) {
            file_type.is_dir()
        } else {
            false
        };
        if !expected_type {
            return Err(corrupt());
        }
        names.insert(name);
    }
    let expected = BTreeSet::from([
        OsStr::new(OWNERSHIP_LOCK_FILE_NAME).to_os_string(),
        OsStr::new(RECEIPTS_DIRECTORY_NAME).to_os_string(),
        OsStr::new(STAGED_DIRECTORY_NAME).to_os_string(),
    ]);
    if names != expected {
        return Err(corrupt());
    }
    Ok(())
}

fn checked_staged_inventory(
    staged_directory: &Path,
    receipts: &BTreeMap<String, &OfflineMaintenanceReceiptV1>,
) -> Result<(BTreeSet<String>, usize), StorageError> {
    let mut names = BTreeSet::new();
    let mut removed_unadmitted = 0usize;
    let mut seen = 0usize;
    for entry in fs::read_dir(staged_directory).map_err(io_unavailable)? {
        if seen == MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1 {
            return Err(limit_exceeded());
        }
        seen = seen.checked_add(1).ok_or_else(limit_exceeded)?;
        let entry = entry.map_err(io_unavailable)?;
        if !entry.file_type().map_err(io_unavailable)?.is_dir() {
            return Err(corrupt());
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Err(corrupt());
        };
        let Some(receipt) = receipts.get(&name) else {
            parse_operation_id(&name).ok_or_else(corrupt)?;
            fs::remove_dir_all(entry.path()).map_err(io_unavailable)?;
            removed_unadmitted = removed_unadmitted
                .checked_add(1)
                .ok_or_else(limit_exceeded)?;
            continue;
        };
        if receipt.operation_kind() != OfflineMaintenanceOperationKind::RestoreBackup
            || !names.insert(name)
        {
            return Err(corrupt());
        }
    }
    if removed_unadmitted != 0 {
        crate::backup::sync_directory(staged_directory)?;
    }
    Ok((names, removed_unadmitted))
}

fn validate_stage_inventory(stage_directory: &Path) -> Result<(), StorageError> {
    let mut entries = fs::read_dir(stage_directory).map_err(io_unavailable)?;
    let Some(entry) = entries.next() else {
        return Err(corrupt());
    };
    let entry = entry.map_err(io_unavailable)?;
    if entry.file_name() != OsStr::new(crate::backup::DATABASE_ARTIFACT_FILE_NAME)
        || !entry.file_type().map_err(io_unavailable)?.is_file()
        || entries.next().is_some()
    {
        return Err(corrupt());
    }
    Ok(())
}

fn remove_incomplete_stage(stage_directory: &Path) -> Result<(), StorageError> {
    fs::remove_dir_all(stage_directory).map_err(io_unavailable)?;
    crate::backup::sync_parent(stage_directory)
}

fn remove_stage_if_present(stage_directory: &Path) -> Result<(), StorageError> {
    match fs::symlink_metadata(stage_directory) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_unavailable(error)),
        Ok(metadata) if metadata.file_type().is_dir() => remove_incomplete_stage(stage_directory),
        Ok(_) => Err(corrupt()),
    }
}

fn read_receipt_file(path: &Path) -> Result<OfflineMaintenanceReceiptV1, StorageError> {
    let metadata = fs::symlink_metadata(path).map_err(io_unavailable)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(corrupt());
    }
    let mut file = File::open(path).map_err(io_unavailable)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(u64::try_from(MAX_RECEIPT_BYTES + 1).map_err(|_| limit_exceeded())?)
        .read_to_end(&mut bytes)
        .map_err(io_unavailable)?;
    if bytes.len() > MAX_RECEIPT_BYTES {
        return Err(limit_exceeded());
    }
    decode_receipt(&bytes)
}

fn receipt_file_name(operation_id: OfflineMaintenanceOperationId) -> String {
    format!("{operation_id}{RECEIPT_FILE_SUFFIX}")
}

fn parse_operation_id(value: &str) -> Option<OfflineMaintenanceOperationId> {
    if value.len() != 36 {
        return None;
    }
    let bytes = value.as_bytes();
    if bytes.get(8) != Some(&b'-')
        || bytes.get(13) != Some(&b'-')
        || bytes.get(18) != Some(&b'-')
        || bytes.get(23) != Some(&b'-')
    {
        return None;
    }
    let mut decoded = [0_u8; 16];
    let mut output = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        if matches!(index, 8 | 13 | 18 | 23) {
            index += 1;
            continue;
        }
        let high = decode_lower_hex(*bytes.get(index)?)?;
        let low = decode_lower_hex(*bytes.get(index + 1)?)?;
        *decoded.get_mut(output)? = (high << 4) | low;
        output += 1;
        index += 2;
    }
    let operation_id = OfflineMaintenanceOperationId::from_bytes(decoded).ok()?;
    (operation_id.to_string() == value).then_some(operation_id)
}

const fn decode_lower_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn value_error(error: StorageValueError) -> StorageError {
    match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => limit_exceeded(),
        StorageValueError::Empty
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::Duplicate
        | StorageValueError::IdentityMismatch
        | StorageValueError::InvalidShape => corrupt(),
    }
}

fn io_unavailable(_error: std::io::Error) -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

fn limit_exceeded() -> StorageError {
    storage_error(StorageErrorKind::LimitExceeded)
}

fn unknown() -> StorageError {
    storage_error(StorageErrorKind::CommitStatusUnknown)
}
