use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use riffdb_storage_api::{
    ContractMigrationReceiptV1, MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1,
    OfflineArchiveReceiptPersistencePort, OfflineBackupManifestIdentityV1, OfflineBackupManifestV1,
    OfflineBackupPersistencePort, OfflineMaintenanceReceiptCreateResultV1,
    OfflineMaintenanceReceiptCreateResultV2, OfflineMaintenanceReceiptInventoryV1,
    OfflineMaintenanceReceiptInventoryV2, OfflineMaintenanceReceiptInventoryV3,
    OfflineMaintenanceReceiptPersistencePort, OfflineMaintenanceReceiptPhaseV1,
    OfflineMaintenanceReceiptReplaceResultV1, OfflineMaintenanceReceiptReplaceResultV2,
    OfflineMaintenanceReceiptV1, OfflineMaintenanceReceiptV2, OfflineMaintenanceReceiptV3,
    OfflineRestoreOverwritePolicyV1, OfflineRestoreResultV1, StartupValidationInputs, StorageError,
    StorageErrorKind, StorageValueError,
};
use riffdb_types::{
    BackupNameV1, ContractMigrationOperationId, OfflineMaintenanceOperationId,
    OfflineMaintenanceOperationKind, OfflineMaintenanceReplacementConfirmation,
};

use super::codec::{
    ARCHIVE_RECEIPT_FILE_SUFFIX, ARCHIVE_RECEIPT_TEMP_SUFFIX, MAX_MIGRATION_RECEIPT_BYTES,
    MAX_RECEIPT_BYTES, RECEIPT_FILE_SUFFIX, RECEIPT_TEMP_SUFFIX, RETIRE_RECEIPT_FILE_SUFFIX,
    RETIRE_RECEIPT_TEMP_SUFFIX, decode_archive_receipt, decode_migration_receipt, decode_receipt,
    decode_retire_receipt, encode_migration_receipt, encode_receipt, encode_retire_receipt,
};
use super::failpoint::{RedbMaintenanceFailpoint, RedbMaintenanceTestController};
use super::path_guard::{PinnedDirectory, verify_regular_file_path};
use super::staged::{RedbSealedStagedRestore, RedbStagedRestore};
use crate::backup::{
    RedbOfflineBackup, lock_existing_target, validate_database_semantics, validate_immutable_backup,
};
use crate::error::storage_error;

#[path = "archive_reconciliation.rs"]
mod archive_reconciliation;

#[path = "archive_receipt_store.rs"]
mod archive_receipts;
use archive_receipts::{archive_receipt_file_name, read_archive_receipt_file};

#[path = "promotion_cutover_store.rs"]
mod promotion_cutover;
#[path = "promotion_reconciliation.rs"]
mod promotion_reconciliation;
pub(crate) use promotion_reconciliation::PromotionValidationBinding;
#[path = "promotion_receipt_store.rs"]
mod promotion_receipts;

const MAINTENANCE_DIRECTORY_NAME: &str = ".maintenance";
const OWNERSHIP_LOCK_FILE_NAME: &str = "owner.lock";
const RECEIPTS_DIRECTORY_NAME: &str = "receipts";
const STAGED_DIRECTORY_NAME: &str = "staged";
const RETIRED_DIRECTORY_NAME: &str = "retired";
const MIGRATIONS_DIRECTORY_NAME: &str = "migrations";
const PROMOTIONS_DIRECTORY_NAME: &str = "replication_promotion";
const MIGRATION_CANDIDATE_FILE_NAME: &str = "candidate.bundle";
const MIGRATION_BUNDLE_FILE_NAME: &str = "migration.bundle";
const MIGRATION_RECEIPT_FILE_NAME: &str = "receipt-v1";
const MIGRATION_RECEIPT_TEMP_FILE_NAME: &str = ".receipt-v1.tmp";
const MIGRATION_RESERVATION_CHUNK_BYTES: usize = 64 * 1024;
const MAX_UNPUBLISHED_RECEIPT_TEMPS: usize = 256;

/// Move-only conservative disk reservation retained through backup and stage creation.
#[must_use = "dropping the reservation releases its conservative disk allocation"]
pub struct RedbMigrationDiskReservation {
    operation_id: ContractMigrationOperationId,
    target: Option<ReservedDiskFile>,
    backup: Option<ReservedDiskFile>,
}

impl RedbMigrationDiskReservation {
    /// Returns the exact accepted operation bound to this reservation.
    #[must_use]
    pub const fn operation_id(&self) -> ContractMigrationOperationId {
        self.operation_id
    }

    /// Releases both allocations after their real artifacts are durable.
    pub fn release(mut self) -> Result<(), StorageError> {
        release_reserved_file(&mut self.target)?;
        release_reserved_file(&mut self.backup)
    }
}

impl std::fmt::Debug for RedbMigrationDiskReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RedbMigrationDiskReservation([ALLOCATED])")
    }
}

struct ReservedDiskFile {
    path: PathBuf,
    file: File,
}

impl Drop for RedbMigrationDiskReservation {
    fn drop(&mut self) {
        let _ = release_reserved_file(&mut self.target);
        let _ = release_reserved_file(&mut self.backup);
    }
}

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
    retire_receipts: OfflineMaintenanceReceiptInventoryV2,
    archive_receipts: OfflineMaintenanceReceiptInventoryV3,
    migration_receipts: Vec<ContractMigrationReceiptV1>,
    operations: Vec<RedbMaintenanceOperationEvidence>,
    removed_unpublished_receipt_temps: usize,
    removed_unpublished_target_temps: usize,
    removed_incomplete_stages: usize,
    removed_terminal_stages: usize,
}

impl RedbMaintenanceReconciliation {
    fn with_migration_receipts(
        mut self,
        migration_receipts: Vec<ContractMigrationReceiptV1>,
    ) -> Result<Self, StorageError> {
        let ordinary_incomplete = self
            .receipts
            .receipts()
            .iter()
            .filter(|receipt| !receipt.current_phase().is_terminal())
            .count();
        let retire_incomplete = self
            .retire_receipts
            .receipts()
            .iter()
            .filter(|receipt| !receipt.current_phase().is_terminal())
            .count();
        let migration_incomplete = migration_receipts
            .iter()
            .filter(|receipt| !receipt.current_phase().is_terminal())
            .count();
        let archive_incomplete = self
            .archive_receipts
            .receipts()
            .iter()
            .filter(|receipt| !receipt.current_phase().is_terminal())
            .count();
        if ordinary_incomplete
            .checked_add(retire_incomplete)
            .ok_or_else(limit_exceeded)?
            .checked_add(migration_incomplete)
            .ok_or_else(limit_exceeded)?
            .checked_add(archive_incomplete)
            .ok_or_else(limit_exceeded)?
            > 1
        {
            return Err(invariant());
        }
        self.migration_receipts = migration_receipts;
        Ok(self)
    }

    /// Borrows every checksum-validated receipt in canonical operation order.
    #[must_use]
    pub const fn receipts(&self) -> &OfflineMaintenanceReceiptInventoryV1 {
        &self.receipts
    }

    /// Borrows every checksum-validated retire-only V2 receipt.
    #[must_use]
    pub const fn retire_receipts(&self) -> &OfflineMaintenanceReceiptInventoryV2 {
        &self.retire_receipts
    }

    /// Borrows the exact archive selections and recovery states in canonical order.
    /// These receipts alone grant no staged validation or serving readiness.
    #[must_use]
    pub const fn archive_receipts(&self) -> &OfflineMaintenanceReceiptInventoryV3 {
        &self.archive_receipts
    }

    /// Borrows every checksum-validated migration receipt in operation order.
    #[must_use]
    pub fn migration_receipts(&self) -> &[ContractMigrationReceiptV1] {
        &self.migration_receipts
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
    retired_directory: PathBuf,
    migrations_directory: PathBuf,
    database_parent_guard: PinnedDirectory,
    backup_root_guard: PinnedDirectory,
    maintenance_directory_guard: PinnedDirectory,
    receipts_directory_guard: PinnedDirectory,
    staged_directory_guard: PinnedDirectory,
    retired_directory_guard: PinnedDirectory,
    migrations_directory_guard: PinnedDirectory,
    promotion_directory_guard: Option<PinnedDirectory>,
    reconciled_promotion: Option<promotion_reconciliation::ReconciledPromotionReceipt>,
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

impl Drop for RedbMaintenanceStorage {
    fn drop(&mut self) {
        // A fork may briefly retain a duplicate descriptor. Explicitly releasing
        // ownership prevents that descriptor from extending this owner's lifetime.
        let _ = self.ownership_lock.unlock();
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
        let mut storage = Self::acquire_owner(database_file, backup_root, test_controller)?;
        let reconciliation = storage.reconcile_for_startup()?;
        Ok((storage, reconciliation))
    }

    /// Completes the same whole-owner reconciliation as ordinary open without
    /// releasing its lock. Promotion recovery must first establish its exact
    /// successful join; this method cannot waive unresolved promotion evidence.
    pub fn reconcile_for_startup(&mut self) -> Result<RedbMaintenanceReconciliation, StorageError> {
        self.verify_path_ownership()?;
        let migration_receipts = self.validate_migration_inventory()?;
        self.reconcile()?
            .with_migration_receipts(migration_receipts)
    }

    fn acquire_owner(
        database_file: &Path,
        backup_root: &Path,
        test_controller: Option<RedbMaintenanceTestController>,
    ) -> Result<Self, StorageError> {
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
        let retired_directory = maintenance_directory.join(RETIRED_DIRECTORY_NAME);
        let migrations_directory = maintenance_directory.join(MIGRATIONS_DIRECTORY_NAME);
        create_checked_directory(&receipts_directory)?;
        create_checked_directory(&staged_directory)?;
        create_checked_directory(&retired_directory)?;
        create_checked_directory(&migrations_directory)?;
        maintenance_directory_guard.verify()?;
        let receipts_directory_guard = PinnedDirectory::open(&receipts_directory)?;
        let staged_directory_guard = PinnedDirectory::open(&staged_directory)?;
        let retired_directory_guard = PinnedDirectory::open(&retired_directory)?;
        let migrations_directory_guard = PinnedDirectory::open(&migrations_directory)?;
        let promotion_directory_guard =
            maintenance_directory_guard.child_directory(OsStr::new(PROMOTIONS_DIRECTORY_NAME))?;
        validate_reserved_inventory(&maintenance_directory)?;

        let storage = Self {
            database_file,
            backup_root: backup_root.to_path_buf(),
            receipts_directory,
            staged_directory,
            retired_directory,
            migrations_directory,
            database_parent_guard,
            backup_root_guard,
            maintenance_directory_guard,
            receipts_directory_guard,
            staged_directory_guard,
            retired_directory_guard,
            migrations_directory_guard,
            promotion_directory_guard,
            reconciled_promotion: None,
            ownership_lock_path,
            ownership_lock,
            test_controller,
        };
        storage.verify_path_custody()?;
        Ok(storage)
    }

    fn verify_path_ownership(&self) -> Result<(), StorageError> {
        self.verify_path_custody()?;
        self.require_no_unreconciled_promotion()
    }

    fn verify_path_custody(&self) -> Result<(), StorageError> {
        self.database_parent_guard.verify()?;
        self.backup_root_guard.verify()?;
        self.maintenance_directory_guard.verify()?;
        self.receipts_directory_guard.verify()?;
        self.staged_directory_guard.verify()?;
        self.retired_directory_guard.verify()?;
        self.migrations_directory_guard.verify()?;
        match &self.promotion_directory_guard {
            Some(directory) => directory.verify_private()?,
            None if self
                .maintenance_directory_guard
                .child_directory(OsStr::new(PROMOTIONS_DIRECTORY_NAME))?
                .is_some() =>
            {
                return Err(corrupt());
            }
            None => {}
        }
        verify_regular_file_path(&self.ownership_lock, &self.ownership_lock_path)
    }

    /// Returns the exact configured database file for server-private validation.
    #[must_use]
    pub fn configured_database_file(&self) -> &Path {
        &self.database_file
    }

    /// Durably accepts immutable canonical artifacts before database drain.
    pub fn accept_contract_migration(
        &self,
        receipt: &ContractMigrationReceiptV1,
        candidate_bundle: &[u8],
        migration_bundle: &[u8],
    ) -> Result<(), StorageError> {
        self.verify_path_ownership()?;
        if receipt.current_phase() != riffdb_storage_api::ContractMigrationReceiptPhaseV1::Accepted
            || !artifact_matches(receipt.operation_artifacts().candidate(), candidate_bundle)
            || !artifact_matches(receipt.operation_artifacts().migration(), migration_bundle)
        {
            return Err(invariant());
        }
        let directory = self.migration_operation_directory(receipt.operation_id());
        match fs::symlink_metadata(&directory) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let (v1, v2, v3, _) = self.read_complete_inventory(false)?;
                if v1
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
                create_checked_directory(&directory)?;
                write_new_synced(
                    &directory.join(MIGRATION_CANDIDATE_FILE_NAME),
                    candidate_bundle,
                )?;
                write_new_synced(
                    &directory.join(MIGRATION_BUNDLE_FILE_NAME),
                    migration_bundle,
                )?;
                write_new_synced(
                    &directory.join(MIGRATION_RECEIPT_FILE_NAME),
                    &encode_migration_receipt(receipt)?,
                )?;
                crate::backup::sync_directory(&directory)?;
                crate::backup::sync_directory(&self.migrations_directory)?;
            }
            Err(error) => return Err(io_unavailable(error)),
            Ok(metadata) if metadata.file_type().is_dir() => {
                let existing =
                    read_migration_receipt_file(&directory.join(MIGRATION_RECEIPT_FILE_NAME))?;
                let candidate = read_bounded_file(
                    &directory.join(MIGRATION_CANDIDATE_FILE_NAME),
                    receipt.operation_artifacts().candidate().length(),
                )?;
                let migration = read_bounded_file(
                    &directory.join(MIGRATION_BUNDLE_FILE_NAME),
                    receipt.operation_artifacts().migration().length(),
                )?;
                if existing != *receipt
                    || candidate != candidate_bundle
                    || migration != migration_bundle
                {
                    return Err(corrupt());
                }
            }
            Ok(_) => return Err(corrupt()),
        }
        self.verify_path_ownership()
    }

    /// Computes the exact concrete SHA-256 identities used by protected artifacts.
    pub fn contract_migration_operation_artifacts(
        candidate_bundle: &[u8],
        migration_bundle: &[u8],
    ) -> Result<riffdb_storage_api::ContractMigrationOperationArtifactsV1, StorageError> {
        use riffdb_storage_api::{
            ContractMigrationArtifactFileV1, ContractMigrationOperationArtifactsV1,
        };
        let candidate_length = u64::try_from(candidate_bundle.len()).map_err(|_| invariant())?;
        let migration_length = u64::try_from(migration_bundle.len()).map_err(|_| invariant())?;
        let candidate = ContractMigrationArtifactFileV1::new(
            candidate_length,
            <[u8; 32]>::from(Sha256::digest(candidate_bundle)),
        )
        .map_err(|_| invariant())?;
        let migration = ContractMigrationArtifactFileV1::new(
            migration_length,
            <[u8; 32]>::from(Sha256::digest(migration_bundle)),
        )
        .map_err(|_| invariant())?;
        Ok(ContractMigrationOperationArtifactsV1::new(
            candidate, migration,
        ))
    }

    /// Reads one checksummed protected migration receipt.
    pub fn read_contract_migration_receipt(
        &self,
        operation_id: ContractMigrationOperationId,
    ) -> Result<Option<ContractMigrationReceiptV1>, StorageError> {
        self.verify_path_ownership()?;
        let path = self
            .migration_operation_directory(operation_id)
            .join(MIGRATION_RECEIPT_FILE_NAME);
        let result = match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_unavailable(error)),
            Ok(metadata)
                if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
            {
                read_migration_receipt_file(&path).map(Some)
            }
            Ok(_) => Err(corrupt()),
        }?;
        self.verify_path_ownership()?;
        Ok(result)
    }

    /// Atomically advances a receipt from an exact current prefix.
    pub fn replace_contract_migration_receipt(
        &self,
        expected: &ContractMigrationReceiptV1,
        replacement: &ContractMigrationReceiptV1,
    ) -> Result<(), StorageError> {
        self.verify_path_ownership()?;
        if !migration_receipt_extends(replacement, expected) {
            return Err(invariant());
        }
        let directory = self.migration_operation_directory(expected.operation_id());
        let destination = directory.join(MIGRATION_RECEIPT_FILE_NAME);
        if read_migration_receipt_file(&destination)? != *expected {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let temporary = directory.join(MIGRATION_RECEIPT_TEMP_FILE_NAME);
        let mut temporary_file = TemporaryReceiptFile::create(&temporary)?;
        temporary_file.write_all(&encode_migration_receipt(replacement)?)?;
        self.hit(RedbMaintenanceFailpoint::BeforeReceiptFileSync, false)?;
        temporary_file.sync_all()?;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptFileSync, false)?;
        self.hit(RedbMaintenanceFailpoint::BeforeReceiptRename, false)?;
        fs::rename(&temporary, &destination).map_err(io_unavailable)?;
        temporary_file.published = true;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptRename, true)?;
        crate::backup::sync_directory(&directory)?;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptParentSync, true)?;
        self.verify_path_ownership()
    }

    /// Reads and rechecks the canonical artifact pair used by restart recovery.
    pub fn read_contract_migration_artifacts(
        &self,
        operation_id: ContractMigrationOperationId,
    ) -> Result<(Vec<u8>, Vec<u8>), StorageError> {
        let receipt = self
            .read_contract_migration_receipt(operation_id)?
            .ok_or_else(corrupt)?;
        let directory = self.migration_operation_directory(operation_id);
        let candidate = read_bounded_file(
            &directory.join(MIGRATION_CANDIDATE_FILE_NAME),
            receipt.operation_artifacts().candidate().length(),
        )?;
        let migration = read_bounded_file(
            &directory.join(MIGRATION_BUNDLE_FILE_NAME),
            receipt.operation_artifacts().migration().length(),
        )?;
        if !artifact_matches(receipt.operation_artifacts().candidate(), &candidate)
            || !artifact_matches(receipt.operation_artifacts().migration(), &migration)
        {
            return Err(corrupt());
        }
        Ok((candidate, migration))
    }

    /// Conservatively allocates backup, stage-growth, and scratch capacity.
    pub fn reserve_contract_migration_disk(
        &self,
        operation_id: ContractMigrationOperationId,
        semantic_write_bytes: u64,
    ) -> Result<RedbMigrationDiskReservation, StorageError> {
        self.verify_path_ownership()?;
        let receipt = self
            .read_contract_migration_receipt(operation_id)?
            .ok_or_else(corrupt)?;
        if receipt.current_phase() != riffdb_storage_api::ContractMigrationReceiptPhaseV1::Preflight
        {
            return Err(invariant());
        }
        let database = fs::symlink_metadata(&self.database_file).map_err(io_unavailable)?;
        if !database.file_type().is_file() || database.file_type().is_symlink() {
            return Err(corrupt());
        }
        let artifact_bytes = receipt
            .operation_artifacts()
            .candidate()
            .length()
            .checked_add(receipt.operation_artifacts().migration().length())
            .ok_or_else(limit_exceeded)?;
        let duplicated_semantic_bytes = semantic_write_bytes
            .checked_mul(2)
            .ok_or_else(limit_exceeded)?;
        let target_bytes = database
            .len()
            .checked_add(duplicated_semantic_bytes)
            .and_then(|bytes| bytes.checked_add(artifact_bytes))
            .ok_or_else(limit_exceeded)?;
        let target_path = self.contract_migration_reservation_path(operation_id)?;
        let backup_path = self
            .migrations_directory
            .join(format!(".{operation_id}.backup-reservation"));
        let target = reserve_disk_file(&target_path, target_bytes)?;
        let backup = match reserve_disk_file(&backup_path, database.len()) {
            Ok(file) => file,
            Err(error) => {
                let mut target = Some(target);
                let _ = release_reserved_file(&mut target);
                return Err(error);
            }
        };
        self.verify_path_ownership()?;
        Ok(RedbMigrationDiskReservation {
            operation_id,
            target: Some(target),
            backup: Some(backup),
        })
    }

    /// Creates or revalidates the deterministic immutable pre-migration backup.
    pub fn create_contract_migration_backup(
        &self,
        operation_id: ContractMigrationOperationId,
        build: &riffdb_storage_api::BackupBuildMetadataV1,
    ) -> Result<
        (
            BackupNameV1,
            OfflineBackupManifestV1,
            OfflineBackupManifestIdentityV1,
        ),
        StorageError,
    > {
        self.verify_path_ownership()?;
        let receipt = self
            .read_contract_migration_receipt(operation_id)?
            .ok_or_else(corrupt)?;
        if receipt.current_phase() != riffdb_storage_api::ContractMigrationReceiptPhaseV1::Preflight
        {
            return Err(invariant());
        }
        let name =
            BackupNameV1::new(format!("pre-migration-{operation_id}")).map_err(|_| invariant())?;
        let directory = self.named_backup_directory(&name);
        let (manifest, published_now) = match fs::symlink_metadata(&directory) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (
                RedbOfflineBackup::bind(&self.database_file, &directory)
                    .create_offline_backup(build)?,
                true,
            ),
            Err(error) => return Err(io_unavailable(error)),
            Ok(metadata) if metadata.file_type().is_dir() => {
                (validate_immutable_backup(&directory)?.0, false)
            }
            Ok(_) => return Err(corrupt()),
        };
        let (validated, identity) = validate_immutable_backup(&directory)?;
        if manifest != validated || identity.database_id() != receipt.database_id() {
            return Err(corrupt());
        }
        if published_now {
            self.hit(RedbMaintenanceFailpoint::AfterNamedBackupPublication, true)?;
        }
        self.verify_path_ownership()?;
        Ok((name, manifest, identity))
    }

    /// Materializes the immutable operation backup into a protected sibling stage.
    pub fn materialize_contract_migration_stage(
        &self,
        operation_id: ContractMigrationOperationId,
    ) -> Result<(PathBuf, [u8; 32]), StorageError> {
        self.verify_path_ownership()?;
        let receipt = self
            .read_contract_migration_receipt(operation_id)?
            .ok_or_else(corrupt)?;
        if !matches!(
            receipt.current_phase(),
            riffdb_storage_api::ContractMigrationReceiptPhaseV1::Staging
                | riffdb_storage_api::ContractMigrationReceiptPhaseV1::Transforming
                | riffdb_storage_api::ContractMigrationReceiptPhaseV1::RebuildingProjections
                | riffdb_storage_api::ContractMigrationReceiptPhaseV1::ValidatingStage
                | riffdb_storage_api::ContractMigrationReceiptPhaseV1::Publishing
        ) {
            return Err(invariant());
        }
        let backup_name = receipt.backup_name().ok_or_else(corrupt)?;
        let expected_manifest = receipt.backup_manifest().ok_or_else(corrupt)?;
        let (manifest, identity) =
            validate_immutable_backup(&self.named_backup_directory(backup_name))?;
        if &identity != expected_manifest {
            return Err(corrupt());
        }
        let source = self
            .named_backup_directory(backup_name)
            .join(crate::backup::DATABASE_ARTIFACT_FILE_NAME);
        let source_format_marker = self
            .named_backup_directory(backup_name)
            .join(crate::backup::FORMAT_MARKER_ARTIFACT_FILE_NAME);
        let stage = self.contract_migration_stage_path(operation_id)?;
        let stage_format_marker = crate::durable_format_marker_path(&stage);
        let materialized_now = match fs::symlink_metadata(&stage) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                copy_new_synced(&source, &stage)?;
                copy_new_synced(&source_format_marker, &stage_format_marker)?;
                self.database_parent_guard.sync()?;
                true
            }
            Err(error) => return Err(io_unavailable(error)),
            Ok(metadata)
                if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
            {
                false
            }
            Ok(_) => return Err(corrupt()),
        };
        if !materialized_now && !stage_format_marker.try_exists().map_err(io_unavailable)? {
            copy_new_synced(&source_format_marker, &stage_format_marker)?;
            self.database_parent_guard.sync()?;
        }
        if crate::preflight_durable_format_path(&stage)
            != Ok(crate::RedbDurableFormatPreflight::OpenCurrent)
        {
            return Err(corrupt());
        }
        let stage_identity = migration_stage_identity(operation_id, expected_manifest)?;
        if receipt
            .stage_identity()
            .is_some_and(|expected| expected != stage_identity)
        {
            return Err(corrupt());
        }
        drop(manifest);
        if materialized_now {
            self.hit(RedbMaintenanceFailpoint::AfterStagedMaterialization, false)?;
        }
        self.verify_path_ownership()?;
        Ok((stage, stage_identity))
    }

    /// Atomically publishes a closed, fully validated sibling stage.
    pub fn publish_contract_migration_stage(
        &self,
        operation_id: ContractMigrationOperationId,
    ) -> Result<(), StorageError> {
        self.verify_path_ownership()?;
        let receipt = self
            .read_contract_migration_receipt(operation_id)?
            .ok_or_else(corrupt)?;
        if receipt.current_phase()
            != riffdb_storage_api::ContractMigrationReceiptPhaseV1::Publishing
            || receipt.stage_identity().is_none()
        {
            return Err(invariant());
        }
        let stage = self.contract_migration_stage_path(operation_id)?;
        let metadata = fs::symlink_metadata(&stage).map_err(io_unavailable)?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(corrupt());
        }
        let stage_format_marker = crate::durable_format_marker_path(&stage);
        if crate::preflight_durable_format_path(&stage)
            != Ok(crate::RedbDurableFormatPreflight::OpenCurrent)
        {
            return Err(corrupt());
        }
        let target_format_marker = crate::durable_format_marker_path(&self.database_file);
        self.hit(RedbMaintenanceFailpoint::BeforeTargetPublication, false)?;
        remove_regular_file_if_present(&target_format_marker)?;
        self.database_parent_guard.sync()?;
        fs::rename(&stage, &self.database_file).map_err(io_unavailable)?;
        fs::rename(&stage_format_marker, &target_format_marker).map_err(io_unavailable)?;
        crate::store::discard_replaced_derived_sidecar(&self.database_file)?;
        self.hit(RedbMaintenanceFailpoint::AfterTargetPublication, true)?;
        self.database_parent_guard.sync()?;
        self.hit(RedbMaintenanceFailpoint::AfterTargetParentSync, true)?;
        self.verify_path_ownership()
    }

    /// Restores the exact operation backup and advances restore-rewind incarnation.
    pub fn rollback_contract_migration(
        &self,
        operation_id: ContractMigrationOperationId,
    ) -> Result<(), StorageError> {
        self.verify_path_ownership()?;
        let receipt = self
            .read_contract_migration_receipt(operation_id)?
            .ok_or_else(corrupt)?;
        if receipt.current_phase()
            != riffdb_storage_api::ContractMigrationReceiptPhaseV1::RollingBack
        {
            return Err(invariant());
        }
        let backup_name = receipt.backup_name().ok_or_else(corrupt)?;
        let expected_manifest = receipt.backup_manifest().ok_or_else(corrupt)?;
        let (manifest, identity) =
            validate_immutable_backup(&self.named_backup_directory(backup_name))?;
        if &identity != expected_manifest {
            return Err(corrupt());
        }
        let new_history_incarnation = manifest
            .history_incarnation()
            .unwrap_or(1)
            .checked_add(1)
            .ok_or_else(limit_exceeded)?;
        let source = self
            .named_backup_directory(backup_name)
            .join(crate::backup::DATABASE_ARTIFACT_FILE_NAME);
        let source_format_marker = self
            .named_backup_directory(backup_name)
            .join(crate::backup::FORMAT_MARKER_ARTIFACT_FILE_NAME);
        let rollback = self.contract_migration_rollback_path(operation_id)?;
        let rollback_format_marker = crate::durable_format_marker_path(&rollback);
        remove_regular_file_if_present(&rollback)?;
        remove_regular_file_if_present(&rollback_format_marker)?;
        copy_new_synced(&source, &rollback)?;
        copy_new_synced(&source_format_marker, &rollback_format_marker)?;
        crate::backup::stamp_history_incarnation(&rollback, new_history_incarnation)?;
        let target_format_marker = crate::durable_format_marker_path(&self.database_file);
        remove_regular_file_if_present(&target_format_marker)?;
        self.database_parent_guard.sync()?;
        fs::rename(&rollback, &self.database_file).map_err(io_unavailable)?;
        fs::rename(&rollback_format_marker, &target_format_marker).map_err(io_unavailable)?;
        crate::store::discard_replaced_derived_sidecar(&self.database_file)?;
        self.database_parent_guard.sync()?;
        self.verify_path_ownership()
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

    /// Resolves the one succeeded create receipt and exact immutable manifest eligible for retire.
    pub fn prepare_backup_retirement(
        &mut self,
        backup_name: &BackupNameV1,
    ) -> Result<
        (
            OfflineMaintenanceOperationId,
            OfflineBackupManifestIdentityV1,
        ),
        StorageError,
    > {
        self.verify_path_ownership()?;
        let (v1, v2, _, _) = self.read_complete_inventory(false)?;
        if v2
            .receipts()
            .iter()
            .any(|receipt| receipt.backup_name() == backup_name)
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let mut candidates = v1.receipts().iter().filter(|receipt| {
            receipt.operation_kind() == OfflineMaintenanceOperationKind::CreateBackup
                && receipt.backup_name() == backup_name
                && receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Succeeded
        });
        let create = candidates.next().ok_or_else(corrupt)?;
        if candidates.next().is_some() {
            return Err(corrupt());
        }
        let expected = create.manifest_identity().ok_or_else(corrupt)?;
        let (_, actual) = validate_immutable_backup(&self.named_backup_directory(backup_name))?;
        if &actual != expected {
            return Err(corrupt());
        }
        self.verify_path_ownership()?;
        Ok((create.operation_id(), actual))
    }

    /// Atomically moves one checked named backup into its operation-private retire stage.
    pub fn publish_backup_retirement(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError> {
        self.verify_path_ownership()?;
        let receipt = self
            .read_retire_receipt(operation_id)?
            .ok_or_else(corrupt)?;
        if receipt.current_phase() != OfflineMaintenanceReceiptPhaseV1::Offline {
            return Err(invariant());
        }
        let named = self.named_backup_directory(receipt.backup_name());
        let stage = self.retired_directory.join(operation_id.to_string());
        match (fs::symlink_metadata(&named), fs::symlink_metadata(&stage)) {
            (Ok(named_metadata), Err(stage_error))
                if named_metadata.file_type().is_dir()
                    && !named_metadata.file_type().is_symlink()
                    && stage_error.kind() == std::io::ErrorKind::NotFound =>
            {
                let (_, identity) = validate_immutable_backup(&named)?;
                if &identity != receipt.retirement().manifest_identity() {
                    return Err(corrupt());
                }
                self.hit(RedbMaintenanceFailpoint::BeforeRetirementPublication, false)?;
                fs::rename(&named, &stage).map_err(io_unavailable)?;
                self.hit(RedbMaintenanceFailpoint::AfterRetirementPublication, true)?;
                self.backup_root_guard.sync()?;
                self.hit(
                    RedbMaintenanceFailpoint::AfterRetirementNamedParentSync,
                    true,
                )?;
                self.retired_directory_guard.sync()?;
                self.hit(
                    RedbMaintenanceFailpoint::AfterRetirementStageParentSync,
                    true,
                )?;
            }
            (Err(named_error), Ok(stage_metadata))
                if named_error.kind() == std::io::ErrorKind::NotFound
                    && stage_metadata.file_type().is_dir()
                    && !stage_metadata.file_type().is_symlink() =>
            {
                let (_, identity) = validate_immutable_backup(&stage)?;
                if &identity != receipt.retirement().manifest_identity() {
                    return Err(corrupt());
                }
            }
            _ => return Err(corrupt()),
        }
        self.verify_path_ownership()
    }

    /// Removes only the closed immutable inventory from a published retire stage.
    pub fn delete_published_backup_retirement(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError> {
        self.verify_path_ownership()?;
        let receipt = self
            .read_retire_receipt(operation_id)?
            .ok_or_else(corrupt)?;
        if receipt.current_phase() != OfflineMaintenanceReceiptPhaseV1::Validating {
            return Err(invariant());
        }
        let stage = self.retired_directory.join(operation_id.to_string());
        match fs::symlink_metadata(&stage) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.verify_path_ownership()?;
                return Ok(());
            }
            Err(error) => return Err(io_unavailable(error)),
            Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            }
            Ok(_) => return Err(corrupt()),
        }
        validate_retired_stage_inventory(&stage, true)?;
        for (name, failpoint) in [
            (
                crate::backup::DATABASE_ARTIFACT_FILE_NAME,
                RedbMaintenanceFailpoint::AfterRetirementDatabaseDelete,
            ),
            (
                crate::backup::FORMAT_MARKER_ARTIFACT_FILE_NAME,
                RedbMaintenanceFailpoint::AfterRetirementFormatDelete,
            ),
            (
                crate::backup::JOURNAL_ARTIFACT_FILE_NAME,
                RedbMaintenanceFailpoint::AfterRetirementJournalDelete,
            ),
            (
                crate::backup::MANIFEST_FILE_NAME,
                RedbMaintenanceFailpoint::AfterRetirementManifestDelete,
            ),
        ] {
            let path = stage.join(name);
            match fs::symlink_metadata(&path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_unavailable(error)),
                Ok(metadata)
                    if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
                {
                    fs::remove_file(&path).map_err(io_unavailable)?;
                    self.hit(failpoint, true)?;
                }
                Ok(_) => return Err(corrupt()),
            }
        }
        crate::backup::sync_directory(&stage)?;
        fs::remove_dir(&stage).map_err(io_unavailable)?;
        self.hit(RedbMaintenanceFailpoint::AfterRetirementStageDelete, true)?;
        self.retired_directory_guard.sync()?;
        self.hit(
            RedbMaintenanceFailpoint::AfterRetirementDeleteParentSync,
            true,
        )?;
        self.verify_path_ownership()
    }

    /// Materializes and completely validates one operation-private restore.
    ///
    /// The supplied digest inventory is server configuration, never a public
    /// selector. This method ignores copied clean eligibility and returns only
    /// after both complete evidence streams reach exact end.
    pub fn stage_restore(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        backup_name: &BackupNameV1,
        validation_inputs: StartupValidationInputs,
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
            validation_inputs,
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
    /// returns only the same move-only, completely validated stage that still
    /// requires fresh staged authorization, sealing, and a durable source-less
    /// restore receipt before publication.
    pub fn stage_recovery_restore_candidate(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        backup_name: &BackupNameV1,
        validation_inputs: StartupValidationInputs,
    ) -> Result<RedbStagedRestore, StorageError> {
        self.verify_path_ownership()?;
        let stage = RedbStagedRestore::materialize(
            operation_id,
            backup_name.clone(),
            self.named_backup_directory(backup_name),
            self.staged_directory.join(operation_id.to_string()),
            self.database_file.clone(),
            validation_inputs,
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
        let (ordinary, retirement, archive, _) = self.read_complete_inventory(false)?;
        if ordinary
            .receipts()
            .iter()
            .any(|r| r.operation_id() == operation_id)
            || retirement
                .receipts()
                .iter()
                .any(|r| r.operation_id() == operation_id)
            || archive
                .receipts()
                .iter()
                .any(|r| r.operation_id() == operation_id)
        {
            return Err(invariant());
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
        let (inventory, retire_inventory, archive_inventory, removed_temps) =
            self.read_complete_inventory(true)?;
        let incomplete_count = inventory
            .receipts()
            .iter()
            .filter(|receipt| !receipt.current_phase().is_terminal())
            .count();
        let retire_incomplete_count = retire_inventory
            .receipts()
            .iter()
            .filter(|receipt| !receipt.current_phase().is_terminal())
            .count();
        let archive_incomplete_count = archive_inventory
            .receipts()
            .iter()
            .filter(|receipt| !receipt.current_phase().is_terminal())
            .count();
        if incomplete_count
            .checked_add(retire_incomplete_count)
            .ok_or_else(limit_exceeded)?
            .checked_add(archive_incomplete_count)
            .ok_or_else(limit_exceeded)?
            > 1
        {
            return Err(invariant());
        }

        let receipt_by_stage_name = inventory
            .receipts()
            .iter()
            .map(|receipt| (receipt.operation_id().to_string(), receipt))
            .collect::<BTreeMap<_, _>>();
        let (staged_names, removed_unadmitted_stages) = checked_staged_inventory(
            &self.staged_directory,
            &receipt_by_stage_name,
            &archive_inventory
                .receipts()
                .iter()
                .map(|r| r.operation_id().to_string())
                .collect(),
        )?;
        validate_retired_directory_inventory(&self.retired_directory, &retire_inventory)?;
        let removed_unpublished_target_temps = self
            .remove_unpublished_target_temps(inventory.receipts(), archive_inventory.receipts())?;
        let mut removed_incomplete_stages = removed_unadmitted_stages;
        let mut removed_terminal_stages = 0usize;
        let mut operations = Vec::with_capacity(inventory.receipts().len());
        let mut retirement_by_create = BTreeMap::new();
        for retirement in retire_inventory.receipts() {
            let create = inventory
                .receipts()
                .iter()
                .find(|receipt| {
                    receipt.operation_id()
                        == retirement.retirement().originating_create_operation_id()
                })
                .ok_or_else(corrupt)?;
            if create.operation_kind() != OfflineMaintenanceOperationKind::CreateBackup
                || create.current_phase() != OfflineMaintenanceReceiptPhaseV1::Succeeded
                || create.backup_name() != retirement.backup_name()
                || create.manifest_identity() != Some(retirement.retirement().manifest_identity())
                || retirement_by_create
                    .insert(create.operation_id(), retirement)
                    .is_some()
            {
                return Err(corrupt());
            }
            reconcile_retired_artifact(
                &self.named_backup_directory(retirement.backup_name()),
                &self
                    .retired_directory
                    .join(retirement.operation_id().to_string()),
                retirement,
            )?;
        }
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
            let matching_retirement = inventory
                .receipts()
                .iter()
                .find(|create| {
                    create.operation_kind() == OfflineMaintenanceOperationKind::CreateBackup
                        && create.backup_name() == receipt.backup_name()
                        && create.manifest_identity() == receipt.manifest_identity()
                })
                .and_then(|create| retirement_by_create.get(&create.operation_id()).copied())
                .is_some_and(|retirement| {
                    matches!(
                        retirement.current_phase(),
                        OfflineMaintenanceReceiptPhaseV1::Offline
                            | OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                            | OfflineMaintenanceReceiptPhaseV1::Validating
                            | OfflineMaintenanceReceiptPhaseV1::Succeeded
                    )
                });
            if receipt.manifest_identity().is_some()
                && !named_backup_matches
                && !matching_retirement
            {
                return Err(corrupt());
            }
            if receipt.operation_kind() == OfflineMaintenanceOperationKind::RestoreBackup
                && named.is_none()
                && !matching_retirement
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
                        let target_candidate_is_current = target_checksum.as_ref()
                            == Some(staged_checksum)
                            && crate::preflight_durable_format_path(&self.database_file)
                                == Ok(crate::RedbDurableFormatPreflight::OpenCurrent);
                        if !target_candidate_is_current {
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
            retire_receipts: retire_inventory,
            archive_receipts: archive_inventory,
            migration_receipts: Vec::new(),
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
        archives: &[OfflineMaintenanceReceiptV3],
    ) -> Result<usize, StorageError> {
        let mut removed = 0usize;
        let journal = crate::journal::journal_path(&self.database_file);
        let marker = crate::durable_format_marker_path(&self.database_file);
        let targets = [
            self.database_file.file_name().ok_or_else(invariant)?,
            journal.file_name().ok_or_else(invariant)?,
            marker.file_name().ok_or_else(invariant)?,
        ];
        let operations = receipts
            .iter()
            .filter(|receipt| {
                receipt.operation_kind() == OfflineMaintenanceOperationKind::RestoreBackup
            })
            .map(OfflineMaintenanceReceiptV1::operation_id)
            .chain(
                archives
                    .iter()
                    .map(OfflineMaintenanceReceiptV3::operation_id),
            );
        for operation_id in operations {
            for target_name in targets {
                let name = super::staged::target_temporary_name(target_name, operation_id);
                if self.database_parent_guard.remove_file_if_present(&name)? {
                    removed = removed.checked_add(1).ok_or_else(limit_exceeded)?;
                }
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
    ) -> Result<
        (
            OfflineMaintenanceReceiptInventoryV1,
            OfflineMaintenanceReceiptInventoryV2,
            OfflineMaintenanceReceiptInventoryV3,
            usize,
        ),
        StorageError,
    > {
        let mut receipts = Vec::new();
        let mut retire_receipts = Vec::new();
        let mut archive_receipts = Vec::new();
        let mut temporaries = Vec::new();
        let mut identities = BTreeSet::new();
        for entry in fs::read_dir(&self.receipts_directory).map_err(io_unavailable)? {
            let entry = entry.map_err(io_unavailable)?;
            let file_type = entry.file_type().map_err(io_unavailable)?;
            if !file_type.is_file() || file_type.is_symlink() {
                return Err(corrupt());
            }
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(corrupt)?;
            let temporary_operation = name.strip_prefix('.').and_then(|value| {
                value
                    .strip_suffix(RECEIPT_TEMP_SUFFIX)
                    .or_else(|| value.strip_suffix(RETIRE_RECEIPT_TEMP_SUFFIX))
                    .or_else(|| value.strip_suffix(ARCHIVE_RECEIPT_TEMP_SUFFIX))
            });
            if let Some(operation_text) = temporary_operation {
                if !remove_temps {
                    return Err(corrupt());
                }
                parse_operation_id(operation_text).ok_or_else(corrupt)?;
                if temporaries.len() == MAX_UNPUBLISHED_RECEIPT_TEMPS {
                    return Err(limit_exceeded());
                }
                temporaries.push(entry.path());
                continue;
            }
            if identities.len() == MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1 {
                return Err(limit_exceeded());
            }
            let id = if name.ends_with(RECEIPT_FILE_SUFFIX) {
                let receipt = read_receipt_file(&entry.path())?;
                if name != receipt_file_name(receipt.operation_id()) {
                    return Err(corrupt());
                }
                let id = receipt.operation_id();
                receipts.push(receipt);
                id
            } else if name.ends_with(RETIRE_RECEIPT_FILE_SUFFIX) {
                let receipt = read_retire_receipt_file(&entry.path())?;
                if name != retire_receipt_file_name(receipt.operation_id()) {
                    return Err(corrupt());
                }
                let id = receipt.operation_id();
                retire_receipts.push(receipt);
                id
            } else if name.ends_with(ARCHIVE_RECEIPT_FILE_SUFFIX) {
                let receipt = read_archive_receipt_file(&entry.path())?;
                if name != archive_receipt_file_name(receipt.operation_id()) {
                    return Err(corrupt());
                }
                let id = receipt.operation_id();
                archive_receipts.push(receipt);
                id
            } else {
                return Err(corrupt());
            };
            if !identities.insert(id) {
                return Err(corrupt());
            }
        }
        // Validate the entire inventory before removing even unpublished files.
        // Unknown/corrupt versions must never turn a refused open into cleanup.
        let v1 = OfflineMaintenanceReceiptInventoryV1::new(receipts).map_err(value_error)?;
        let v2 = OfflineMaintenanceReceiptInventoryV2::new(retire_receipts).map_err(value_error)?;
        let v3 =
            OfflineMaintenanceReceiptInventoryV3::new(archive_receipts).map_err(value_error)?;
        for path in &temporaries {
            fs::remove_file(path).map_err(io_unavailable)?;
        }
        if !temporaries.is_empty() {
            self.receipts_directory_guard.sync()?;
        }
        Ok((v1, v2, v3, temporaries.len()))
    }

    fn receipt_path(&self, operation_id: OfflineMaintenanceOperationId) -> PathBuf {
        self.receipts_directory
            .join(receipt_file_name(operation_id))
    }

    fn retire_receipt_path(&self, operation_id: OfflineMaintenanceOperationId) -> PathBuf {
        self.receipts_directory
            .join(retire_receipt_file_name(operation_id))
    }

    fn migration_operation_directory(&self, operation_id: ContractMigrationOperationId) -> PathBuf {
        self.migrations_directory.join(operation_id.to_string())
    }

    fn contract_migration_stage_path(
        &self,
        operation_id: ContractMigrationOperationId,
    ) -> Result<PathBuf, StorageError> {
        let target = self
            .database_file
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(invariant)?;
        Ok(self
            .database_file
            .with_file_name(format!(".{target}.migration-{operation_id}.stage")))
    }

    fn contract_migration_rollback_path(
        &self,
        operation_id: ContractMigrationOperationId,
    ) -> Result<PathBuf, StorageError> {
        let target = self
            .database_file
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(invariant)?;
        Ok(self
            .database_file
            .with_file_name(format!(".{target}.migration-{operation_id}.rollback")))
    }

    fn contract_migration_reservation_path(
        &self,
        operation_id: ContractMigrationOperationId,
    ) -> Result<PathBuf, StorageError> {
        let target = self
            .database_file
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(invariant)?;
        Ok(self
            .database_file
            .with_file_name(format!(".{target}.migration-{operation_id}.reservation")))
    }

    fn validate_migration_inventory(
        &self,
    ) -> Result<Vec<ContractMigrationReceiptV1>, StorageError> {
        let mut count = 0usize;
        let mut reservation_count = 0usize;
        let mut receipts = Vec::new();
        for entry in fs::read_dir(&self.migrations_directory).map_err(io_unavailable)? {
            let entry = entry.map_err(io_unavailable)?;
            let name = entry.file_name().into_string().map_err(|_| corrupt())?;
            if let Some(operation) = parse_migration_backup_reservation_name(&name) {
                reservation_count = reservation_count
                    .checked_add(1)
                    .ok_or_else(limit_exceeded)?;
                if reservation_count > MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1
                    || self.read_contract_migration_receipt(operation)?.is_none()
                {
                    return Err(corrupt());
                }
                remove_regular_file_if_present(&entry.path())?;
                continue;
            }
            count = count.checked_add(1).ok_or_else(limit_exceeded)?;
            if count > MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1 {
                return Err(limit_exceeded());
            }
            if !entry.file_type().map_err(io_unavailable)?.is_dir() {
                return Err(corrupt());
            }
            let operation = parse_contract_migration_operation_id(&name).ok_or_else(corrupt)?;
            let receipt =
                read_migration_receipt_file(&entry.path().join(MIGRATION_RECEIPT_FILE_NAME))?;
            if receipt.operation_id() != operation {
                return Err(corrupt());
            }
            let candidate = read_bounded_file(
                &entry.path().join(MIGRATION_CANDIDATE_FILE_NAME),
                receipt.operation_artifacts().candidate().length(),
            )?;
            let migration = read_bounded_file(
                &entry.path().join(MIGRATION_BUNDLE_FILE_NAME),
                receipt.operation_artifacts().migration().length(),
            )?;
            if !artifact_matches(receipt.operation_artifacts().candidate(), &candidate)
                || !artifact_matches(receipt.operation_artifacts().migration(), &migration)
            {
                return Err(corrupt());
            }
            receipts.push(receipt);
        }
        for receipt in &receipts {
            remove_regular_file_if_present(
                &self.contract_migration_reservation_path(receipt.operation_id())?,
            )?;
        }
        receipts.sort_unstable_by_key(ContractMigrationReceiptV1::operation_id);
        if receipts
            .windows(2)
            .any(|pair| pair[0].operation_id() == pair[1].operation_id())
            || receipts
                .iter()
                .filter(|receipt| !receipt.current_phase().is_terminal())
                .count()
                > 1
        {
            return Err(invariant());
        }
        Ok(receipts)
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

    fn write_retire_receipt(
        &self,
        receipt: &OfflineMaintenanceReceiptV2,
    ) -> Result<(), StorageError> {
        let destination = self.retire_receipt_path(receipt.operation_id());
        let temporary = self.receipts_directory.join(format!(
            ".{}{}",
            receipt.operation_id(),
            RETIRE_RECEIPT_TEMP_SUFFIX
        ));
        let bytes = encode_retire_receipt(receipt)?;
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
                let (inventory, retire_inventory, archive_inventory, _) =
                    self.read_complete_inventory(false)?;
                if inventory.receipts().len()
                    + retire_inventory.receipts().len()
                    + archive_inventory.receipts().len()
                    >= MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1
                {
                    return Err(limit_exceeded());
                }
                if retire_inventory
                    .receipts()
                    .iter()
                    .any(|r| r.operation_id() == receipt.operation_id())
                    || archive_inventory.receipts().iter().any(|r| {
                        r.operation_id() == receipt.operation_id()
                            || !r.current_phase().is_terminal()
                    })
                    || self
                        .validate_migration_inventory()?
                        .iter()
                        .any(|r| !r.current_phase().is_terminal())
                {
                    return Err(invariant());
                }
                if inventory
                    .receipts()
                    .iter()
                    .any(|existing| !existing.current_phase().is_terminal())
                {
                    return Err(invariant());
                }
                if retire_inventory
                    .receipts()
                    .iter()
                    .any(|existing| !existing.current_phase().is_terminal())
                {
                    return Err(invariant());
                }
                if receipt.operation_kind() == OfflineMaintenanceOperationKind::CreateBackup {
                    if inventory.receipts().iter().any(|existing| {
                        existing.operation_kind() == OfflineMaintenanceOperationKind::CreateBackup
                            && existing.backup_name() == receipt.backup_name()
                    }) || retire_inventory
                        .receipts()
                        .iter()
                        .any(|existing| existing.backup_name() == receipt.backup_name())
                    {
                        return Err(storage_error(StorageErrorKind::Unavailable));
                    }
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
        let (inventory, _, _, _) = self.read_complete_inventory(false)?;
        self.verify_path_ownership()?;
        Ok(inventory)
    }

    fn create_or_read_retire_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV2,
    ) -> Result<OfflineMaintenanceReceiptCreateResultV2, StorageError> {
        self.verify_path_ownership()?;
        if self.read_receipt(receipt.operation_id())?.is_some() {
            return Err(corrupt());
        }
        let path = self.retire_receipt_path(receipt.operation_id());
        let result = match fs::symlink_metadata(&path) {
            Ok(metadata)
                if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
            {
                Ok(OfflineMaintenanceReceiptCreateResultV2::Existing(Box::new(
                    read_retire_receipt_file(&path)?,
                )))
            }
            Ok(_) => Err(corrupt()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let (v1, v2, v3, _) = self.read_complete_inventory(false)?;
                if v1.receipts().len() + v2.receipts().len() + v3.receipts().len()
                    >= MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1
                {
                    return Err(limit_exceeded());
                }
                if v3.receipts().iter().any(|r| {
                    r.operation_id() == receipt.operation_id() || !r.current_phase().is_terminal()
                }) || self
                    .validate_migration_inventory()?
                    .iter()
                    .any(|r| !r.current_phase().is_terminal())
                {
                    return Err(invariant());
                }
                if v1
                    .receipts()
                    .iter()
                    .any(|existing| !existing.current_phase().is_terminal())
                    || v2
                        .receipts()
                        .iter()
                        .any(|existing| !existing.current_phase().is_terminal())
                {
                    return Err(invariant());
                }
                let create = v1
                    .receipts()
                    .iter()
                    .find(|candidate| {
                        candidate.operation_id()
                            == receipt.retirement().originating_create_operation_id()
                    })
                    .ok_or_else(corrupt)?;
                if create.operation_kind() != OfflineMaintenanceOperationKind::CreateBackup
                    || create.current_phase() != OfflineMaintenanceReceiptPhaseV1::Succeeded
                    || create.backup_name() != receipt.backup_name()
                    || create.manifest_identity() != Some(receipt.retirement().manifest_identity())
                    || v2.receipts().iter().any(|prior| {
                        prior.backup_name() == receipt.backup_name()
                            || prior.retirement().originating_create_operation_id()
                                == create.operation_id()
                    })
                {
                    return Err(corrupt());
                }
                let (_, identity) =
                    validate_immutable_backup(&self.named_backup_directory(receipt.backup_name()))?;
                if &identity != receipt.retirement().manifest_identity() {
                    return Err(corrupt());
                }
                self.write_retire_receipt(receipt)?;
                Ok(OfflineMaintenanceReceiptCreateResultV2::Created)
            }
            Err(error) => Err(io_unavailable(error)),
        }?;
        self.verify_path_ownership()?;
        Ok(result)
    }

    fn replace_retire_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV2,
    ) -> Result<OfflineMaintenanceReceiptReplaceResultV2, StorageError> {
        self.verify_path_ownership()?;
        let path = self.retire_receipt_path(receipt.operation_id());
        let current = read_retire_receipt_file(&path)?;
        if current == *receipt {
            return Ok(OfflineMaintenanceReceiptReplaceResultV2::AlreadyCurrent);
        }
        if !receipt.monotonically_extends(&current) {
            return Err(invariant());
        }
        self.write_retire_receipt(receipt)?;
        self.verify_path_ownership()?;
        Ok(OfflineMaintenanceReceiptReplaceResultV2::Replaced)
    }

    fn read_retire_receipt(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<Option<OfflineMaintenanceReceiptV2>, StorageError> {
        self.verify_path_ownership()?;
        match fs::symlink_metadata(self.receipt_path(operation_id)) {
            Ok(_) => return Err(corrupt()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_unavailable(error)),
        }
        let path = self.retire_receipt_path(operation_id);
        let result = match fs::symlink_metadata(&path) {
            Ok(metadata)
                if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
            {
                read_retire_receipt_file(&path).map(Some)
            }
            Ok(_) => Err(corrupt()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_unavailable(error)),
        }?;
        self.verify_path_ownership()?;
        Ok(result)
    }

    fn validate_retire_receipt_inventory(
        &mut self,
    ) -> Result<OfflineMaintenanceReceiptInventoryV2, StorageError> {
        self.verify_path_ownership()?;
        let (_, inventory, _, _) = self.read_complete_inventory(false)?;
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
            Some(
                RECEIPTS_DIRECTORY_NAME
                    | STAGED_DIRECTORY_NAME
                    | RETIRED_DIRECTORY_NAME
                    | MIGRATIONS_DIRECTORY_NAME
                    | PROMOTIONS_DIRECTORY_NAME
            )
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
        OsStr::new(RETIRED_DIRECTORY_NAME).to_os_string(),
        OsStr::new(MIGRATIONS_DIRECTORY_NAME).to_os_string(),
    ]);
    // Older stores have no promotion ledger. Create it only when the explicit
    // promotion owner first persists an attempt; ordinary opens stay unchanged.
    names.remove(OsStr::new(PROMOTIONS_DIRECTORY_NAME));
    if names != expected {
        return Err(corrupt());
    }
    Ok(())
}

fn checked_staged_inventory(
    staged_directory: &Path,
    receipts: &BTreeMap<String, &OfflineMaintenanceReceiptV1>,
    archive_operations: &BTreeSet<String>,
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
        if archive_operations.contains(&name) {
            // Replay changes the original backup head. Retain this admitted private
            // stage for its archive owner; ordinary backup validation cannot adopt it.
            continue;
        }
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

fn validate_retired_directory_inventory(
    retired_directory: &Path,
    receipts: &OfflineMaintenanceReceiptInventoryV2,
) -> Result<(), StorageError> {
    let by_name = receipts
        .receipts()
        .iter()
        .map(|receipt| (receipt.operation_id().to_string(), receipt))
        .collect::<BTreeMap<_, _>>();
    let mut count = 0usize;
    for entry in fs::read_dir(retired_directory).map_err(io_unavailable)? {
        count = count.checked_add(1).ok_or_else(limit_exceeded)?;
        if count > MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1 {
            return Err(limit_exceeded());
        }
        let entry = entry.map_err(io_unavailable)?;
        let name = entry.file_name().into_string().map_err(|_| corrupt())?;
        if !entry.file_type().map_err(io_unavailable)?.is_dir()
            || !by_name.contains_key(&name)
            || parse_operation_id(&name).is_none()
        {
            return Err(corrupt());
        }
    }
    Ok(())
}

fn validate_stage_inventory(stage_directory: &Path) -> Result<(), StorageError> {
    let format_marker_name =
        crate::durable_format_marker_path(Path::new(crate::backup::DATABASE_ARTIFACT_FILE_NAME))
            .file_name()
            .ok_or_else(invariant)?
            .to_os_string();
    let journal_name =
        crate::journal::journal_path(Path::new(crate::backup::DATABASE_ARTIFACT_FILE_NAME))
            .file_name()
            .ok_or_else(invariant)?
            .to_os_string();
    let mut expected = BTreeSet::from([
        OsString::from(crate::backup::DATABASE_ARTIFACT_FILE_NAME),
        format_marker_name,
        journal_name,
    ]);
    let mut seen = BTreeSet::new();
    for entry in fs::read_dir(stage_directory).map_err(io_unavailable)? {
        let entry = entry.map_err(io_unavailable)?;
        if !entry.file_type().map_err(io_unavailable)?.is_file()
            || !expected.remove(&entry.file_name())
            || !seen.insert(entry.file_name())
        {
            return Err(corrupt());
        }
    }
    if !expected.is_empty() || seen.len() != 3 {
        return Err(corrupt());
    }
    Ok(())
}

fn reconcile_retired_artifact(
    named_directory: &Path,
    retired_stage: &Path,
    receipt: &OfflineMaintenanceReceiptV2,
) -> Result<(), StorageError> {
    let named_exists = match fs::symlink_metadata(named_directory) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(io_unavailable(error)),
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            let (_, identity) = validate_immutable_backup(named_directory)?;
            if &identity != receipt.retirement().manifest_identity() {
                return Err(corrupt());
            }
            true
        }
        Ok(_) => return Err(corrupt()),
    };
    let stage_exists = match fs::symlink_metadata(retired_stage) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(io_unavailable(error)),
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Validating {
                validate_retired_stage_inventory(retired_stage, true)?;
            } else {
                let (_, identity) = validate_immutable_backup(retired_stage)?;
                if &identity != receipt.retirement().manifest_identity() {
                    return Err(corrupt());
                }
            }
            true
        }
        Ok(_) => return Err(corrupt()),
    };
    match receipt.current_phase() {
        OfflineMaintenanceReceiptPhaseV1::Accepted | OfflineMaintenanceReceiptPhaseV1::Draining => {
            if !named_exists || stage_exists {
                return Err(corrupt());
            }
        }
        OfflineMaintenanceReceiptPhaseV1::Offline => {
            if named_exists == stage_exists {
                return Err(corrupt());
            }
        }
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished => {
            if named_exists || !stage_exists {
                return Err(corrupt());
            }
        }
        OfflineMaintenanceReceiptPhaseV1::Validating => {
            if named_exists {
                return Err(corrupt());
            }
        }
        OfflineMaintenanceReceiptPhaseV1::Succeeded => {
            if named_exists || stage_exists {
                return Err(corrupt());
            }
        }
        OfflineMaintenanceReceiptPhaseV1::FailedClosed => {
            if !named_exists || stage_exists {
                return Err(corrupt());
            }
        }
    }
    Ok(())
}

fn validate_retired_stage_inventory(
    directory: &Path,
    allow_partial: bool,
) -> Result<(), StorageError> {
    let expected = BTreeSet::from([
        OsString::from(crate::backup::DATABASE_ARTIFACT_FILE_NAME),
        OsString::from(crate::backup::FORMAT_MARKER_ARTIFACT_FILE_NAME),
        OsString::from(crate::backup::JOURNAL_ARTIFACT_FILE_NAME),
        OsString::from(crate::backup::MANIFEST_FILE_NAME),
    ]);
    let mut seen = BTreeSet::new();
    for entry in fs::read_dir(directory).map_err(io_unavailable)? {
        let entry = entry.map_err(io_unavailable)?;
        let metadata = fs::symlink_metadata(entry.path()).map_err(io_unavailable)?;
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || !expected.contains(&entry.file_name())
            || !seen.insert(entry.file_name())
        {
            return Err(corrupt());
        }
    }
    if !allow_partial && seen.is_empty() {
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

fn read_retire_receipt_file(path: &Path) -> Result<OfflineMaintenanceReceiptV2, StorageError> {
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
    decode_retire_receipt(&bytes)
}

fn read_migration_receipt_file(path: &Path) -> Result<ContractMigrationReceiptV1, StorageError> {
    let metadata = fs::symlink_metadata(path).map_err(io_unavailable)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(corrupt());
    }
    let mut file = File::open(path).map_err(io_unavailable)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(u64::try_from(MAX_MIGRATION_RECEIPT_BYTES + 1).map_err(|_| limit_exceeded())?)
        .read_to_end(&mut bytes)
        .map_err(io_unavailable)?;
    decode_migration_receipt(&bytes)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io_unavailable)?;
    file.write_all(bytes).map_err(io_unavailable)?;
    file.sync_all().map_err(io_unavailable)
}

fn copy_new_synced(source: &Path, destination: &Path) -> Result<(), StorageError> {
    let mut source_file = File::open(source).map_err(io_unavailable)?;
    let mut destination_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .map_err(io_unavailable)?;
    std::io::copy(&mut source_file, &mut destination_file).map_err(io_unavailable)?;
    destination_file.sync_all().map_err(io_unavailable)
}

fn remove_regular_file_if_present(path: &Path) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_unavailable(error)),
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            fs::remove_file(path).map_err(io_unavailable)?;
            crate::backup::sync_parent(path)
        }
        Ok(_) => Err(corrupt()),
    }
}

fn reserve_disk_file(path: &Path, bytes: u64) -> Result<ReservedDiskFile, StorageError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io_unavailable)?;
    let zeros = [0_u8; MIGRATION_RESERVATION_CHUNK_BYTES];
    let mut remaining = bytes;
    while remaining != 0 {
        let chunk = usize::try_from(remaining.min(MIGRATION_RESERVATION_CHUNK_BYTES as u64))
            .map_err(|_| limit_exceeded())?;
        if let Err(error) = file.write_all(&zeros[..chunk]) {
            drop(file);
            let _ = fs::remove_file(path);
            return Err(io_unavailable(error));
        }
        remaining -= u64::try_from(chunk).map_err(|_| limit_exceeded())?;
    }
    if let Err(error) = file.sync_all() {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(io_unavailable(error));
    }
    crate::backup::sync_parent(path)?;
    Ok(ReservedDiskFile {
        path: path.to_path_buf(),
        file,
    })
}

fn release_reserved_file(file: &mut Option<ReservedDiskFile>) -> Result<(), StorageError> {
    let Some(reserved) = file.take() else {
        return Ok(());
    };
    let ReservedDiskFile { path, file } = reserved;
    drop(file);
    fs::remove_file(&path).map_err(io_unavailable)?;
    crate::backup::sync_parent(&path)
}

fn read_bounded_file(path: &Path, exact_length: u64) -> Result<Vec<u8>, StorageError> {
    let metadata = fs::symlink_metadata(path).map_err(io_unavailable)?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != exact_length
    {
        return Err(corrupt());
    }
    let mut file = File::open(path).map_err(io_unavailable)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(exact_length.checked_add(1).ok_or_else(limit_exceeded)?)
        .read_to_end(&mut bytes)
        .map_err(io_unavailable)?;
    if u64::try_from(bytes.len()).map_err(|_| limit_exceeded())? != exact_length {
        return Err(corrupt());
    }
    Ok(bytes)
}

fn artifact_matches(
    expected: riffdb_storage_api::ContractMigrationArtifactFileV1,
    bytes: &[u8],
) -> bool {
    u64::try_from(bytes.len()).ok() == Some(expected.length())
        && <[u8; 32]>::from(Sha256::digest(bytes)) == expected.sha256()
}

fn migration_stage_identity(
    operation_id: ContractMigrationOperationId,
    manifest: &OfflineBackupManifestIdentityV1,
) -> Result<[u8; 32], StorageError> {
    let mut digest = Sha256::new();
    digest.update(b"riffdb.contract-migration-stage/v1\0");
    digest.update(operation_id.as_bytes());
    digest.update(manifest.database_id().as_bytes());
    let checksum = manifest.manifest_checksum().as_bytes();
    digest.update(
        u32::try_from(checksum.len())
            .map_err(|_| limit_exceeded())?
            .to_be_bytes(),
    );
    digest.update(checksum);
    Ok(digest.finalize().into())
}

fn migration_receipt_extends(
    replacement: &ContractMigrationReceiptV1,
    expected: &ContractMigrationReceiptV1,
) -> bool {
    replacement.database_id() == expected.database_id()
        && replacement.operation_id() == expected.operation_id()
        && replacement.input_hash() == expected.input_hash()
        && replacement.artifacts() == expected.artifacts()
        && replacement.operation_artifacts() == expected.operation_artifacts()
        && replacement.admission() == expected.admission()
        && expected
            .backup_name()
            .is_none_or(|value| replacement.backup_name() == Some(value))
        && expected
            .backup_manifest()
            .is_none_or(|value| replacement.backup_manifest() == Some(value))
        && expected
            .stage_identity()
            .is_none_or(|value| replacement.stage_identity() == Some(value))
        && replacement
            .transitions()
            .starts_with(expected.transitions())
        && replacement.transitions().len() >= expected.transitions().len()
}

fn receipt_file_name(operation_id: OfflineMaintenanceOperationId) -> String {
    format!("{operation_id}{RECEIPT_FILE_SUFFIX}")
}

fn retire_receipt_file_name(operation_id: OfflineMaintenanceOperationId) -> String {
    format!("{operation_id}{RETIRE_RECEIPT_FILE_SUFFIX}")
}

fn parse_operation_id(value: &str) -> Option<OfflineMaintenanceOperationId> {
    let operation_id = OfflineMaintenanceOperationId::from_bytes(parse_uuid_bytes(value)?).ok()?;
    (operation_id.to_string() == value).then_some(operation_id)
}

fn parse_uuid_bytes(value: &str) -> Option<[u8; 16]> {
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
    Some(decoded)
}

fn parse_contract_migration_operation_id(value: &str) -> Option<ContractMigrationOperationId> {
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
    let operation_id = ContractMigrationOperationId::from_bytes(decoded).ok()?;
    (operation_id.to_string() == value).then_some(operation_id)
}

fn parse_migration_backup_reservation_name(value: &str) -> Option<ContractMigrationOperationId> {
    parse_contract_migration_operation_id(
        value
            .strip_prefix('.')?
            .strip_suffix(".backup-reservation")?,
    )
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

#[cfg(all(test, unix))]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            let ordinal = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
            let path = crate::test_path::root().join(format!(
                "riffdb-redb-maintenance-drop-lock-{}-{ordinal}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir(&path).expect("create test root");
            Self(path)
        }

        fn join(&self, value: &str) -> PathBuf {
            self.0.join(value)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn drop_releases_ownership_shared_with_an_inherited_descriptor() {
        let root = TestRoot::new();
        let database = root.join("database.redb");
        let backup_root = root.join("backups");
        let (storage, _) =
            RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
        let inherited = storage
            .ownership_lock
            .try_clone()
            .expect("model inherited lock descriptor");

        drop(storage);
        let (reopened, _) = RedbMaintenanceStorage::open(&database, &backup_root)
            .expect("drop must release ownership despite an inherited descriptor");
        drop(reopened);
        drop(inherited);
    }
}
