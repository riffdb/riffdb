#![expect(
    clippy::expect_used,
    reason = "verified backup manifests retain required path components and bounded sequence ranges"
)]

//! Offline redb backup artifacts and destructive restore.

use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use redb::{Database, Durability, ReadableDatabase, ReadableTable};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AlphaFormatEpoch, ApplicationSequenceAllocator,
    BackupArtifactChecksumV1, BackupBuildMetadataV1, BackupCatalogBundleV1,
    BackupFormatCompatibilityV1, BackupIntegrityChecksumV1, BackupManifestVersion,
    BackupSnapshotKindV1, CompatibilityFixtureDigest, DurableFormatIdentity, DurableFormatWriter,
    HISTORY_INCARNATION_INITIAL, MAX_BACKUP_BUILD_FEATURE_BYTES, MAX_BACKUP_BUILD_FEATURES,
    MAX_BACKUP_BUILD_VALUE_BYTES, MAX_BACKUP_CATALOG_BUNDLES, MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES,
    OfflineBackupManifestIdentityV1, OfflineBackupManifestV1, OfflineBackupPersistencePort,
    OfflineRestoreOverwritePolicyV1, OfflineRestorePersistencePort, OfflineRestoreResultV1,
    StorageError, StorageErrorKind, StorageFormatVersion, StorageValueError,
};
use riffdb_types::{
    CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId,
    MAX_CONTRACT_LINEAGE_BYTES,
};
use sha2::{Digest, Sha256};

use crate::codec;
use crate::command_authority::command_authority_head;
use crate::error::{
    commit_error, database_error, precommit_storage_error, storage_error, table_error,
    transaction_error,
};
use crate::hooks::{RedbTestController, RedbTestOperation};
use crate::keys::decode_contract_bundle_key;
use crate::layout::{
    CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, COMMITS, CONTRACT_BUNDLES, EVENTS, META,
    META_APPLICATION_SEQUENCE, META_DATABASE_ID, META_FORMAT_VERSION, META_HISTORY_INCARNATION,
    META_RETENTION_WATERMARK,
};

pub(crate) const MANIFEST_FILE_NAME: &str = "manifest.riffdb";
pub(crate) const DATABASE_ARTIFACT_FILE_NAME: &str = "database.redb";
pub(crate) const JOURNAL_ARTIFACT_FILE_NAME: &str = "journal.riffextent";
pub(crate) const FORMAT_MARKER_ARTIFACT_FILE_NAME: &str = "format.riffdb";
const MANIFEST_MAGIC: &[u8; 16] = b"RIFFDB-BACKUP\0\0\0";
const MANIFEST_MAX_BYTES: usize = 32 * 1024 * 1024;
const COPY_BUFFER_BYTES: usize = 64 * 1024;
const MAX_STAGING_ATTEMPTS: u16 = 256;
const SHA256_BYTES: usize = 32;
#[path = "backup_v3.rs"]
mod v3_restore;
const STORAGE_ENGINE_DATA_TAG: u8 = 1;

/// Reads the durable history incarnation from a closed database file.
///
/// Returns `None` when the key is absent (pre-fence artifact). Opens read-only
/// so a pure inspection never triggers redb recovery-on-open writes.
pub fn read_history_incarnation(path: impl AsRef<Path>) -> Result<Option<u64>, StorageError> {
    let database = redb::ReadOnlyDatabase::open(path.as_ref()).map_err(database_error)?;
    let transaction = database.begin_read().map_err(transaction_error)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    match meta
        .get(META_HISTORY_INCARNATION)
        .map_err(precommit_storage_error)?
    {
        None => Ok(None),
        Some(encoded) => {
            let incarnation = *codec::decode_history_incarnation_v1(encoded.value())?.value();
            Ok(Some(incarnation))
        }
    }
}

/// Idempotently stamps `history_incarnation/v1` on a closed database file.
///
/// Offline-only path used by restore publication. An active V3 lineage is fully
/// validated, then reanchored with replication-local state reset in this same
/// transaction. Equal-incarnation retries validate and perform no durable write.
pub fn stamp_history_incarnation(
    path: impl AsRef<Path>,
    incarnation: u64,
) -> Result<(), StorageError> {
    if incarnation < HISTORY_INCARNATION_INITIAL {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let database = Database::open(path.as_ref()).map_err(database_error)?;
    stamp_incarnation(database, incarnation, None)
}

pub(crate) fn stamp_private_archive_incarnation(
    file: &File,
    binding: crate::maintenance::PrivateArchiveValidationBinding,
    incarnation: u64,
) -> Result<(), StorageError> {
    let database = Database::builder()
        .create_file(
            file.try_clone()
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?,
        )
        .map_err(database_error)?;
    stamp_incarnation(database, incarnation, Some(binding))
}

fn stamp_incarnation(
    database: Database,
    incarnation: u64,
    private: Option<crate::maintenance::PrivateArchiveValidationBinding>,
) -> Result<(), StorageError> {
    let mut transaction = database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let current = {
        let meta = transaction.open_table(META).map_err(table_error)?;
        match meta
            .get(META_HISTORY_INCARNATION)
            .map_err(precommit_storage_error)?
        {
            None => None,
            Some(encoded) => Some(*codec::decode_history_incarnation_v1(encoded.value())?.value()),
        }
    };
    if current.is_some_and(|current| current > incarnation) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let anchor = match private {
        Some(binding) => Some(v3_restore::PreparedRestoreAnchor::prepare_private(
            &transaction,
            binding,
            incarnation,
        )?),
        None => v3_restore::PreparedRestoreAnchor::prepare(&transaction, incarnation)?,
    };
    if current == Some(incarnation) {
        return transaction.abort().map_err(precommit_storage_error);
    }
    v3_restore::edge("preflight");
    stamp_incarnation_metadata(&transaction, incarnation)?;
    v3_restore::edge("incarnation");
    if let Some(anchor) = anchor {
        anchor.stage(&transaction)?;
    }
    transaction.commit().map_err(commit_error)?;
    v3_restore::edge("committed");
    Ok(())
}

/// Shared private metadata portion of the ADR-0072 offline stamp. Callers own
/// full preflight and the single transaction containing the new lineage anchor.
pub(crate) fn stamp_incarnation_metadata(
    transaction: &redb::WriteTransaction,
    incarnation: u64,
) -> Result<(), StorageError> {
    {
        let mut meta = transaction.open_table(META).map_err(table_error)?;
        let encoded = codec::encode_history_incarnation_v1(incarnation)?;
        meta.insert(META_HISTORY_INCARNATION, encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        // Re-bind a present retention watermark to the new incarnation in the
        // same transaction so startup does not see a stale binding. The
        // recorded chain-root registry digest is carried forward verbatim:
        // rooting happened once, at prune time (ADR-0085 A2).
        let rebind = match meta
            .get(META_RETENTION_WATERMARK)
            .map_err(precommit_storage_error)?
        {
            None => None,
            Some(encoded) => {
                let existing =
                    riffdb_storage_api::proto_codec::decode_retention_watermark_v1(encoded.value())
                        .map_err(crate::error::codec_error)?
                        .into_parts()
                        .0;
                if existing.history_incarnation() == incarnation {
                    None
                } else {
                    Some((
                        existing.watermark_sequence(),
                        existing.chain_root_registry_digest(),
                    ))
                }
            }
        };
        if let Some((sequence, chain_root)) = rebind {
            let watermark = riffdb_storage_api::StoredRetentionWatermarkV1::new(
                sequence,
                incarnation,
                chain_root,
            )
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            let stamped =
                riffdb_storage_api::proto_codec::encode_retention_watermark_v1(&watermark)
                    .map_err(crate::error::codec_error)?;
            meta.insert(META_RETENTION_WATERMARK, stamped.as_bytes())
                .map_err(precommit_storage_error)?;
        }
    }
    Ok(())
}

/// Idempotently stamps `retention_watermark/v1` on a closed database file.
///
/// Offline-only path used after restore. Writes only when the stored value
/// differs. Sequence `0` writes an explicit zero watermark bound to the live
/// history incarnation.
///
/// A nonzero stamp requires an existing watermark row: its recorded
/// chain-root registry digest (the digest current when the tombstone chain
/// was rooted) is carried forward verbatim. A nonzero stamp with no existing
/// row would have to fabricate that binding, so it refuses as corrupt — the
/// database bytes and the manifest disagree about pruned history.
pub(crate) fn stamp_retention_watermark(
    path: impl AsRef<Path>,
    watermark_sequence: u64,
) -> Result<(), StorageError> {
    let database = Database::open(path.as_ref()).map_err(database_error)?;
    let mut transaction = database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let history = v3_restore::watermark_history(&transaction)?;
    let (incarnation, current) = {
        let meta = transaction.open_table(META).map_err(table_error)?;
        let incarnation = match meta
            .get(META_HISTORY_INCARNATION)
            .map_err(precommit_storage_error)?
        {
            None => HISTORY_INCARNATION_INITIAL,
            Some(encoded) => {
                let value = *codec::decode_history_incarnation_v1(encoded.value())?.value();
                if value < HISTORY_INCARNATION_INITIAL {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                value
            }
        };
        let current = match meta
            .get(META_RETENTION_WATERMARK)
            .map_err(precommit_storage_error)?
        {
            None => None,
            Some(encoded) => {
                let item =
                    riffdb_storage_api::proto_codec::decode_retention_watermark_v1(encoded.value())
                        .map_err(crate::error::codec_error)?;
                Some(item.into_parts().0)
            }
        };
        (incarnation, current)
    };
    if history.is_some()
        && current
            .as_ref()
            .is_some_and(|watermark| watermark.history_incarnation() != incarnation)
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    if current.as_ref().map(|w| w.watermark_sequence()) == Some(watermark_sequence) {
        return transaction.abort().map_err(precommit_storage_error);
    }
    let chain_root = if watermark_sequence == 0 {
        None
    } else {
        match current
            .as_ref()
            .and_then(|w| w.chain_root_registry_digest())
        {
            Some(digest) => Some(digest),
            // Nonzero target with no recorded rooting digest: refuse rather
            // than fabricate a chain-root binding (fail toward retention).
            None => return Err(storage_error(StorageErrorKind::CorruptData)),
        }
    };
    let watermark = riffdb_storage_api::StoredRetentionWatermarkV1::new(
        watermark_sequence,
        incarnation,
        chain_root,
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let encoded = riffdb_storage_api::proto_codec::encode_retention_watermark_v1(&watermark)
        .map_err(crate::error::codec_error)?;
    let receipt = v3_restore::prepare_watermark_receipt(&transaction, history, &encoded)?;
    v3_restore::edge("watermark-preflight");
    {
        let mut meta = transaction.open_table(META).map_err(table_error)?;
        meta.insert(META_RETENTION_WATERMARK, encoded.as_bytes())
            .map_err(precommit_storage_error)?;
    }
    v3_restore::edge("watermark-mutation");
    if let Some(receipt) = receipt {
        receipt.stage(&transaction)?;
    }
    v3_restore::edge("watermark-receipt");
    transaction.commit().map_err(commit_error)?;
    v3_restore::edge("watermark-committed");
    Ok(())
}

/// After restore: stamp watermark from the manifest when history was pruned,
/// then re-derive a fail-safe ceiling against restored fencing inputs.
///
/// Unpruned (`Some(0)` or absent) leaves the meta key absent so a subsequent
/// history-incarnation bump (restore publication) cannot leave a stale
/// watermark incarnation binding. Pruned watermarks (`> 0`) are stamped and
/// re-bound to the live incarnation.
///
/// Never advances the watermark past the fencing minimum. May lower the stored
/// watermark when fencing requires it **and** no tombstone chain claims a higher
/// pruned end (otherwise the pruned truth is retained so `HistoryPruned` stays
/// correct).
pub(crate) fn apply_restored_retention_watermark(
    path: impl AsRef<Path>,
    manifest_watermark: Option<u64>,
) -> Result<(), StorageError> {
    let path = path.as_ref();
    let Some(manifest_watermark) = manifest_watermark else {
        // Pre-RT-B backup: leave absent key (treat as 0).
        return Ok(());
    };
    if manifest_watermark == 0 {
        // Explicit unpruned: do not invent a zero-watermark meta row. A later
        // history-incarnation stamp on restore would otherwise desync the binding.
        return Ok(());
    }
    stamp_retention_watermark(path, manifest_watermark)?;

    let database = Database::open(path).map_err(database_error)?;
    let transaction = database.begin_read().map_err(transaction_error)?;
    let current = crate::retention::load_watermark(&transaction)?
        .map(|w| w.watermark_sequence())
        .unwrap_or(0);
    let holds = crate::retention::load_holds(&transaction)?;
    let fencing = crate::retention::collect_fencing_inputs(&transaction, &holds)?;
    let (max_perm, _) = riffdb_storage_api::compute_max_permissible_watermark(&fencing);
    let tombstone_end = {
        let table = transaction
            .open_table(crate::layout::HISTORY_TOMBSTONES)
            .map_err(table_error)?;
        match table.last().map_err(precommit_storage_error)? {
            None => 0,
            Some((_, value)) => {
                let item =
                    riffdb_storage_api::proto_codec::decode_history_tombstone_v1(value.value())
                        .map_err(crate::error::codec_error)?;
                item.into_parts().0.last_sequence()
            }
        }
    };
    drop(transaction);
    drop(database);

    // Fail-safe: never store a watermark above the fencing min when one exists,
    // but never lower below the tombstone chain end (pruned history truth).
    let mut target = current;
    if let Some(max_perm) = max_perm {
        target = target.min(max_perm);
    }
    target = target.max(tombstone_end);
    if target != current {
        stamp_retention_watermark(path, target)?;
    }
    Ok(())
}

/// Source- and destination-bound offline redb backup operation.
///
/// The source must not be open by a running RiffDB process. A successful call
/// publishes a new directory containing exactly `manifest.riffdb` and
/// `database.redb`; an existing destination is never replaced.
pub struct RedbOfflineBackup {
    source_database: PathBuf,
    backup_directory: PathBuf,
    test_controller: Option<RedbTestController>,
}

impl RedbOfflineBackup {
    /// Binds one offline database file to one not-yet-existing backup directory.
    pub fn bind(source_database: impl AsRef<Path>, backup_directory: impl AsRef<Path>) -> Self {
        Self {
            source_database: source_database.as_ref().to_path_buf(),
            backup_directory: backup_directory.as_ref().to_path_buf(),
            test_controller: None,
        }
    }

    /// Binds one backup operation to a closed process-test failpoint controller.
    #[doc(hidden)]
    pub fn bind_with_test_controller(
        source_database: impl AsRef<Path>,
        backup_directory: impl AsRef<Path>,
        controller: RedbTestController,
    ) -> Self {
        Self {
            source_database: source_database.as_ref().to_path_buf(),
            backup_directory: backup_directory.as_ref().to_path_buf(),
            test_controller: Some(controller),
        }
    }
}

/// Source- and target-bound offline redb restore operation.
///
/// The target is a database directory whose authoritative artifact is
/// `database.redb`. Its database must not be open by a running RiffDB process.
pub struct RedbOfflineRestore {
    backup_directory: PathBuf,
    target_directory: PathBuf,
    test_controller: Option<RedbTestController>,
}

impl RedbOfflineRestore {
    /// Binds one immutable backup directory to one offline target directory.
    pub fn bind(backup_directory: impl AsRef<Path>, target_directory: impl AsRef<Path>) -> Self {
        Self {
            backup_directory: backup_directory.as_ref().to_path_buf(),
            target_directory: target_directory.as_ref().to_path_buf(),
            test_controller: None,
        }
    }

    /// Binds one restore operation to a closed process-test failpoint controller.
    #[doc(hidden)]
    pub fn bind_with_test_controller(
        backup_directory: impl AsRef<Path>,
        target_directory: impl AsRef<Path>,
        controller: RedbTestController,
    ) -> Self {
        Self {
            backup_directory: backup_directory.as_ref().to_path_buf(),
            target_directory: target_directory.as_ref().to_path_buf(),
            test_controller: Some(controller),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct DatabaseFacts {
    storage_format_version: StorageFormatVersion,
    database_id: DatabaseId,
    catalog_bundles: Vec<BackupCatalogBundleV1>,
    active_catalog: Option<ActiveCatalogPointerV1>,
    last_commit_sequence: Option<CommitSequence>,
    history_incarnation: Option<u64>,
    /// Explicit post-RT-B watermark (`Some(0)` when unpruned / key absent).
    retention_watermark_sequence: Option<u64>,
}

impl DatabaseFacts {
    fn into_manifest(
        self,
        checksums: Vec<BackupArtifactChecksumV1>,
        build: BackupBuildMetadataV1,
    ) -> Result<OfflineBackupManifestV1, StorageError> {
        OfflineBackupManifestV1::new(
            self.storage_format_version,
            self.database_id,
            BackupSnapshotKindV1::StorageEngineData,
            self.catalog_bundles,
            self.active_catalog,
            self.last_commit_sequence,
            self.history_incarnation,
            // Prefer explicit Some(0) when the live DB has no key so new backups
            // always carry the post-RT-B watermark presence tag.
            Some(self.retention_watermark_sequence.unwrap_or(0)),
            checksums,
            build,
        )
        .map_err(value_error)
    }

    fn into_manifest_with_format_compatibility(
        self,
        checksums: Vec<BackupArtifactChecksumV1>,
        build: BackupBuildMetadataV1,
        format_compatibility: BackupFormatCompatibilityV1,
    ) -> Result<OfflineBackupManifestV1, StorageError> {
        OfflineBackupManifestV1::new_with_format_compatibility(
            self.storage_format_version,
            self.database_id,
            BackupSnapshotKindV1::StorageEngineData,
            self.catalog_bundles,
            self.active_catalog,
            self.last_commit_sequence,
            self.history_incarnation,
            Some(self.retention_watermark_sequence.unwrap_or(0)),
            format_compatibility,
            checksums,
            build,
        )
        .map_err(value_error)
    }

    #[cfg(test)]
    fn into_pre_format_compatibility_manifest(
        self,
        checksums: Vec<BackupArtifactChecksumV1>,
        build: BackupBuildMetadataV1,
    ) -> Result<OfflineBackupManifestV1, StorageError> {
        OfflineBackupManifestV1::new_pre_format_compatibility(
            self.storage_format_version,
            self.database_id,
            BackupSnapshotKindV1::StorageEngineData,
            self.catalog_bundles,
            self.active_catalog,
            self.last_commit_sequence,
            self.history_incarnation,
            Some(self.retention_watermark_sequence.unwrap_or(0)),
            checksums,
            build,
        )
        .map_err(value_error)
    }
}

impl OfflineBackupPersistencePort for RedbOfflineBackup {
    fn create_offline_backup(
        &mut self,
        build: &BackupBuildMetadataV1,
    ) -> Result<OfflineBackupManifestV1, StorageError> {
        require_absent(&self.backup_directory)?;
        let mut staging = StagingDirectory::create_sibling(&self.backup_directory, "backup")?;
        let artifact_path = staging.path().join(DATABASE_ARTIFACT_FILE_NAME);
        let journal_artifact_path = staging.path().join(JOURNAL_ARTIFACT_FILE_NAME);
        let format_marker_artifact_path = staging.path().join(FORMAT_MARKER_ARTIFACT_FILE_NAME);

        if crate::preflight_durable_format_path(&self.source_database)
            != Ok(crate::RedbDurableFormatPreflight::OpenCurrent)
        {
            return Err(incompatible());
        }

        let (source_database, source_facts) = open_database_with_facts(&self.source_database)?;
        copy_and_sync(&self.source_database, &artifact_path)?;
        let (artifact_database, artifact_facts) = open_database_with_facts(&artifact_path)?;
        drop(artifact_database);
        if artifact_facts != source_facts {
            return Err(corrupt());
        }
        stage_backup_journal(
            &self.source_database,
            &journal_artifact_path,
            source_facts.database_id,
            source_facts.last_commit_sequence,
        )?;
        copy_and_sync(
            &crate::durable_format_marker_path(&self.source_database),
            &format_marker_artifact_path,
        )?;
        validate_current_format_marker_artifact(&format_marker_artifact_path)?;
        let checksums = vec![
            BackupArtifactChecksumV1::new(NonZeroU32::MIN, sha256_file(&artifact_path)?),
            BackupArtifactChecksumV1::new(
                NonZeroU32::new(2).expect("journal artifact ordinal is nonzero"),
                sha256_file(&journal_artifact_path)?,
            ),
            BackupArtifactChecksumV1::new(
                NonZeroU32::new(3).expect("format marker artifact ordinal is nonzero"),
                sha256_file(&format_marker_artifact_path)?,
            ),
        ];
        let manifest = source_facts.into_manifest(checksums, build.clone())?;
        let manifest_bytes = encode_manifest(&manifest)?;
        write_new_and_sync(&staging.path().join(MANIFEST_FILE_NAME), &manifest_bytes)?;
        validate_backup_inventory(staging.path())?;
        validate_manifest_inventory(staging.path(), &manifest)?;
        sync_directory(staging.path())?;

        if let Some(controller) = &self.test_controller {
            controller.before_commit(RedbTestOperation::Backup)?;
        }
        fs::rename(staging.path(), &self.backup_directory).map_err(io_unavailable)?;
        staging.mark_published();
        if sync_parent(&self.backup_directory).is_err() {
            return Err(unknown());
        }
        if let Some(controller) = &self.test_controller {
            controller.after_commit(RedbTestOperation::Backup)?;
        }
        drop(source_database);
        Ok(manifest)
    }
}

impl OfflineRestorePersistencePort for RedbOfflineRestore {
    fn restore_offline_backup(
        &mut self,
        overwrite_policy: OfflineRestoreOverwritePolicyV1,
    ) -> Result<OfflineRestoreResultV1, StorageError> {
        validate_backup_inventory(&self.backup_directory)?;
        let manifest_bytes = read_bounded_file(
            &self.backup_directory.join(MANIFEST_FILE_NAME),
            MANIFEST_MAX_BYTES,
        )?;
        let manifest = decode_manifest(&manifest_bytes)?;
        validate_manifest_inventory(&self.backup_directory, &manifest)?;
        // Physical compatibility is decided from the retained exact range,
        // never inferred from a successful parse. This check intentionally
        // precedes target discovery, directory creation, locking, staging, or
        // replacement so an incompatible restore is non-mutating.
        if !manifest.is_physically_restorable_by_current_binary() {
            return Err(incompatible());
        }

        let target_state = target_directory_state(&self.target_directory)?;
        if target_state == TargetDirectoryState::NonEmpty
            && overwrite_policy == OfflineRestoreOverwritePolicyV1::RefuseNonEmpty
        {
            return Ok(OfflineRestoreResultV1::TargetNotEmpty);
        }
        if target_state == TargetDirectoryState::Missing {
            fs::create_dir_all(&self.target_directory).map_err(io_unavailable)?;
        }

        let target_artifact = self.target_directory.join(DATABASE_ARTIFACT_FILE_NAME);
        let target_journal = crate::journal::journal_path(&target_artifact);
        let target_format_marker = crate::durable_format_marker_path(&target_artifact);
        let target_lock = lock_existing_target(&target_artifact)?;
        let mut staged_artifact = StagedArtifact::create(&self.target_directory)?;
        let mut staged_journal = StagedArtifact::create_named(
            &self.target_directory,
            OsStr::new(JOURNAL_ARTIFACT_FILE_NAME),
        )?;
        let mut staged_format_marker = StagedArtifact::create_named(
            &self.target_directory,
            OsStr::new(FORMAT_MARKER_ARTIFACT_FILE_NAME),
        )?;
        copy_and_sync(
            &self.backup_directory.join(DATABASE_ARTIFACT_FILE_NAME),
            staged_artifact.path(),
        )?;
        let staged_lock = validate_artifact(staged_artifact.path(), &manifest)?;
        match manifest_journal_checksum(&manifest)? {
            Some(expected) => {
                copy_and_sync(
                    &self.backup_directory.join(JOURNAL_ARTIFACT_FILE_NAME),
                    staged_journal.path(),
                )?;
                if sha256_file(staged_journal.path())? != expected {
                    return Err(corrupt());
                }
            }
            None => {
                fs::remove_file(staged_journal.path()).map_err(io_unavailable)?;
                crate::journal::reset_journal(
                    staged_journal.path(),
                    &crate::journal::JournalFileHeader::with_frontiers(
                        manifest.database_id(),
                        manifest.last_commit_sequence(),
                        None,
                        [0; 32],
                    ),
                )
                .map_err(backup_journal_error)?;
            }
        }
        validate_backup_journal(
            staged_journal.path(),
            manifest.database_id(),
            manifest.last_commit_sequence(),
        )?;
        let expected_format_marker =
            manifest_format_marker_checksum(&manifest)?.ok_or_else(corrupt)?;
        copy_and_sync(
            &self.backup_directory.join(FORMAT_MARKER_ARTIFACT_FILE_NAME),
            staged_format_marker.path(),
        )?;
        if sha256_file(staged_format_marker.path())? != expected_format_marker {
            return Err(corrupt());
        }
        validate_manifest_format_marker(staged_format_marker.path(), &manifest)?;

        if let Some(controller) = &self.test_controller {
            controller.before_commit(RedbTestOperation::Restore)?;
        }
        match fs::remove_file(&target_format_marker) {
            Ok(()) => sync_directory(&self.target_directory)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_unavailable(error)),
        }
        fs::rename(staged_artifact.path(), &target_artifact).map_err(io_unavailable)?;
        staged_artifact.mark_published();
        fs::rename(staged_journal.path(), &target_journal).map_err(io_unavailable)?;
        staged_journal.mark_published();
        fs::rename(staged_format_marker.path(), &target_format_marker).map_err(io_unavailable)?;
        staged_format_marker.mark_published();
        if sync_directory(&self.target_directory).is_err() {
            return Err(unknown());
        }
        drop(target_lock);
        drop(staged_lock);
        if let Some(controller) = &self.test_controller {
            controller.after_commit(RedbTestOperation::Restore)?;
        }
        // Stamp/re-derive the retention watermark from the manifest so restore
        // of pre-stamp artifacts and fencing drift stay fail-safe (ADR-0085 A2).
        apply_restored_retention_watermark(
            &target_artifact,
            manifest.retention_watermark_sequence(),
        )?;
        Ok(OfflineRestoreResultV1::Restored {
            manifest: Box::new(manifest),
        })
    }
}

fn open_database_with_facts(path: &Path) -> Result<(Database, DatabaseFacts), StorageError> {
    let database = Database::open(path).map_err(database_error)?;
    let facts = read_database_facts(&database)?;
    Ok((database, facts))
}

fn read_database_facts(database: &impl ReadableDatabase) -> Result<DatabaseFacts, StorageError> {
    let transaction = database.begin_read().map_err(transaction_error)?;

    let meta = transaction.open_table(META).map_err(table_error)?;
    let format = meta
        .get(META_FORMAT_VERSION)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let storage_format_version = *codec::decode_storage_format_version_v1(format.value())?.value();
    let identity = meta
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let database_id = *codec::decode_database_identity_v1(identity.value())?.value();
    let allocator = meta
        .get(META_APPLICATION_SEQUENCE)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let allocator = *codec::decode_application_sequence_allocator_v1(allocator.value())?.value();
    let history_incarnation = match meta
        .get(META_HISTORY_INCARNATION)
        .map_err(precommit_storage_error)?
    {
        None => None,
        Some(encoded) => Some(*codec::decode_history_incarnation_v1(encoded.value())?.value()),
    };
    drop(meta);
    // Watermark: 0 when absent (pre-RT-B / unpruned). Exposed as Some for new
    // backup manifests; allocator fencing uses the effective sequence.
    let retention_watermark_sequence =
        crate::retention::load_watermark(&transaction)?.map(|w| w.watermark_sequence());
    let effective_watermark = retention_watermark_sequence.unwrap_or(0);

    let bundle_table = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    let mut catalog_bundles = Vec::new();
    for entry in bundle_table.iter().map_err(precommit_storage_error)? {
        if catalog_bundles.len() == MAX_BACKUP_CATALOG_BUNDLES {
            return Err(limit_exceeded());
        }
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let (lineage, version) = decode_contract_bundle_key(key.value()).map_err(|_| corrupt())?;
        let bundle = codec::decode_contract_bundle_v1(value.value())?
            .into_parts()
            .0;
        if bundle.lineage() != &lineage || bundle.contract_version() != version {
            return Err(corrupt());
        }
        catalog_bundles.push(BackupCatalogBundleV1::new(
            lineage,
            version,
            bundle.bundle_hash(),
        ));
    }
    drop(bundle_table);

    let active_table = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    let mut active_catalog = None;
    for entry in active_table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if key.value() != CATALOG_ACTIVE_KEY || active_catalog.is_some() {
            return Err(corrupt());
        }
        active_catalog = Some(
            codec::decode_active_catalog_pointer_v1(value.value())?
                .into_parts()
                .0,
        );
    }
    drop(active_table);

    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let last_commit_sequence = match command_authority_head(&commits, &events)? {
        None => {
            // Empty commits table: either never-written, or fully pruned.
            // Accept an advanced allocator only when the watermark covers the
            // pruned prefix exactly (allocator Next(n) ⇔ watermark == n-1;
            // Exhausted ⇔ watermark == u64::MAX).
            if allocator != ApplicationSequenceAllocator::initial() {
                let covered = match allocator {
                    ApplicationSequenceAllocator::Next(next) => {
                        let expected_watermark = next.get().saturating_sub(1);
                        effective_watermark == expected_watermark && effective_watermark > 0
                    }
                    ApplicationSequenceAllocator::Exhausted => effective_watermark == u64::MAX,
                };
                if !covered {
                    return Err(corrupt());
                }
            }
            None
        }
        Some(sequence) => {
            // Retained last commit must sit strictly above the pruned prefix
            // when any history was pruned (watermark covers [1, watermark]).
            if effective_watermark > 0 && sequence.get() <= effective_watermark {
                return Err(corrupt());
            }
            let expected_allocator = sequence.checked_next().map_or(
                ApplicationSequenceAllocator::Exhausted,
                ApplicationSequenceAllocator::next,
            );
            if allocator != expected_allocator {
                return Err(corrupt());
            }
            Some(sequence)
        }
    };
    drop(commits);
    drop(transaction);

    Ok(DatabaseFacts {
        storage_format_version,
        database_id,
        catalog_bundles,
        active_catalog,
        last_commit_sequence,
        history_incarnation,
        retention_watermark_sequence,
    })
}

fn validate_artifact(
    artifact_path: &Path,
    manifest: &OfflineBackupManifestV1,
) -> Result<Database, StorageError> {
    let expected_checksum = manifest_checksum(manifest)?;
    if sha256_file(artifact_path)? != expected_checksum {
        return Err(corrupt());
    }
    let (database, facts) = open_database_with_facts(artifact_path)?;
    // Opening a redb artifact may update engine-owned clean-open metadata. The
    // checksum authenticates the exact immutable backup bytes before that open;
    // semantic metadata is then validated from redb's recovered view.
    let reconstructed = facts.into_manifest_with_format_compatibility(
        manifest.checksums().to_vec(),
        manifest.build().clone(),
        manifest.format_compatibility().ok_or_else(incompatible)?,
    )?;
    if &reconstructed != manifest {
        return Err(corrupt());
    }
    Ok(database)
}

fn stage_backup_journal(
    source_database: &Path,
    destination: &Path,
    database_id: DatabaseId,
    checkpoint_sequence: Option<CommitSequence>,
) -> Result<(), StorageError> {
    let source = crate::journal::journal_path(source_database);
    match source.try_exists() {
        Ok(true) => {
            validate_backup_journal(&source, database_id, checkpoint_sequence)?;
            copy_and_sync(&source, destination)?;
        }
        Ok(false) => crate::journal::reset_journal(
            destination,
            &crate::journal::JournalFileHeader::with_frontiers(
                database_id,
                checkpoint_sequence,
                None,
                [0; 32],
            ),
        )
        .map_err(backup_journal_error)?,
        Err(error) => return Err(io_unavailable(error)),
    }
    validate_backup_journal(destination, database_id, checkpoint_sequence)
}

pub(crate) fn validate_backup_journal(
    path: &Path,
    database_id: DatabaseId,
    checkpoint_sequence: Option<CommitSequence>,
) -> Result<(), StorageError> {
    let (header, tail) = crate::journal::scan_journal(path, database_id, |_| Ok(()))
        .map_err(backup_journal_error)?
        .ok_or_else(corrupt)?;
    if header.checkpoint_sequence() != checkpoint_sequence
        || tail.last_sequence != checkpoint_sequence
        || tail.incomplete_tail
        || tail.transition_count != 0
    {
        return Err(corrupt());
    }
    Ok(())
}

pub(crate) fn backup_journal_error(error: crate::journal::JournalIoError) -> StorageError {
    match error {
        crate::journal::JournalIoError::Corrupt
        | crate::journal::JournalIoError::LegacyNonEmpty(_) => corrupt(),
        crate::journal::JournalIoError::Capacity => limit_exceeded(),
        crate::journal::JournalIoError::Io | crate::journal::JournalIoError::Stopped => {
            storage_error(StorageErrorKind::Unavailable)
        }
    }
}

fn validate_current_format_marker_artifact(path: &Path) -> Result<(), StorageError> {
    let bytes = read_bounded_file(path, riffdb_storage_api::DURABLE_FORMAT_MARKER_BYTES)?;
    if bytes.len() != riffdb_storage_api::DURABLE_FORMAT_MARKER_BYTES {
        return Err(corrupt());
    }
    let marker =
        riffdb_storage_api::decode_durable_format_marker(&bytes).map_err(|error| match error {
            riffdb_storage_api::DurableFormatMarkerError::Malformed
            | riffdb_storage_api::DurableFormatMarkerError::ChecksumMismatch => corrupt(),
            riffdb_storage_api::DurableFormatMarkerError::UnknownVersion
            | riffdb_storage_api::DurableFormatMarkerError::ManifestMismatch => incompatible(),
        })?;
    let current = riffdb_storage_api::current_durable_format_manifest();
    if marker.identity() != current.identity()
        || marker.registry_digest() != current.registry_digest()
        || marker.compatibility_fixture_digest() != current.compatibility_fixture_digest()
    {
        return Err(incompatible());
    }
    Ok(())
}

fn validate_manifest_format_marker(
    path: &Path,
    manifest: &OfflineBackupManifestV1,
) -> Result<(), StorageError> {
    let bytes = read_bounded_file(path, riffdb_storage_api::DURABLE_FORMAT_MARKER_BYTES)?;
    if bytes.len() != riffdb_storage_api::DURABLE_FORMAT_MARKER_BYTES {
        return Err(corrupt());
    }
    let marker = riffdb_storage_api::decode_durable_format_marker(&bytes).map_err(|_| corrupt())?;
    let range = manifest.format_compatibility().ok_or_else(incompatible)?;
    if !range.contains(marker.identity())
        || marker.compatibility_fixture_digest() != range.compatibility_fixture_digest()
    {
        return Err(incompatible());
    }
    let current = riffdb_storage_api::current_durable_format_manifest();
    if marker.identity() == current.identity()
        && marker.registry_digest() != current.registry_digest()
    {
        return Err(incompatible());
    }
    Ok(())
}

pub(crate) fn validate_immutable_backup(
    directory: &Path,
) -> Result<(OfflineBackupManifestV1, OfflineBackupManifestIdentityV1), StorageError> {
    validate_backup_inventory(directory)?;
    let manifest_bytes =
        read_bounded_file(&directory.join(MANIFEST_FILE_NAME), MANIFEST_MAX_BYTES)?;
    let manifest = decode_manifest(&manifest_bytes)?;
    validate_manifest_inventory(directory, &manifest)?;
    let artifact = directory.join(DATABASE_ARTIFACT_FILE_NAME);
    if sha256_file(&artifact)? != manifest_checksum(&manifest)? {
        return Err(corrupt());
    }
    if let Some(expected) = manifest_journal_checksum(&manifest)? {
        let journal = directory.join(JOURNAL_ARTIFACT_FILE_NAME);
        if sha256_file(&journal)? != expected {
            return Err(corrupt());
        }
        validate_backup_journal(
            &journal,
            manifest.database_id(),
            manifest.last_commit_sequence(),
        )?;
    }
    if let Some(expected) = manifest_format_marker_checksum(&manifest)? {
        let marker = directory.join(FORMAT_MARKER_ARTIFACT_FILE_NAME);
        if sha256_file(&marker)? != expected {
            return Err(corrupt());
        }
        validate_manifest_format_marker(&marker, &manifest)?;
    }
    let manifest_checksum =
        BackupIntegrityChecksumV1::new(Sha256::digest(&manifest_bytes).to_vec())
            .map_err(value_error)?;
    let identity = OfflineBackupManifestIdentityV1::new(
        manifest_checksum,
        manifest.database_id(),
        manifest.last_commit_sequence(),
    );
    Ok((manifest, identity))
}

/// Proves that a closed predecessor database and journal are exactly the
/// immutable artifacts authenticated by a verified backup. This check performs
/// bounded-memory streaming hashes and never opens or mutates the source.
pub(crate) fn validate_exact_source_matches_backup(
    source_database: &Path,
    manifest: &OfflineBackupManifestV1,
) -> Result<(), StorageError> {
    if sha256_file(source_database)? != manifest_checksum(manifest)? {
        return Err(corrupt());
    }
    let source_journal = crate::journal::journal_path(source_database);
    match manifest_journal_checksum(manifest)? {
        Some(expected) => {
            match source_journal.try_exists() {
                Ok(true) => {
                    if sha256_file(&source_journal)? != expected {
                        return Err(corrupt());
                    }
                    validate_backup_journal(
                        &source_journal,
                        manifest.database_id(),
                        manifest.last_commit_sequence(),
                    )?;
                }
                // The predecessor backup adapter canonicalizes an absent,
                // empty suffix into a checked journal artifact. Absence on the
                // closed source is therefore semantically exact, not missing
                // durability, because the authenticated artifact was already
                // validated as an empty suffix at the same frontier.
                Ok(false) => {}
                Err(_) => return Err(storage_error(StorageErrorKind::Unavailable)),
            }
        }
        None => match source_journal.try_exists() {
            Ok(false) => {}
            Ok(true) => return Err(corrupt()),
            Err(_) => return Err(storage_error(StorageErrorKind::Unavailable)),
        },
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn create_pre_format_compatibility_backup_fixture(
    source_database: &Path,
    backup_directory: &Path,
    build: &BackupBuildMetadataV1,
) -> Result<OfflineBackupManifestV1, StorageError> {
    require_absent(backup_directory)?;
    fs::create_dir(backup_directory).map_err(io_unavailable)?;
    let artifact = backup_directory.join(DATABASE_ARTIFACT_FILE_NAME);
    let journal = backup_directory.join(JOURNAL_ARTIFACT_FILE_NAME);
    let source = redb::ReadOnlyDatabase::open(source_database).map_err(database_error)?;
    let facts = read_database_facts(&source)?;
    drop(source);
    copy_and_sync(source_database, &artifact)?;
    stage_backup_journal(
        source_database,
        &journal,
        facts.database_id,
        facts.last_commit_sequence,
    )?;
    let checksums = vec![
        BackupArtifactChecksumV1::new(NonZeroU32::MIN, sha256_file(&artifact)?),
        BackupArtifactChecksumV1::new(
            NonZeroU32::new(2).expect("journal artifact ordinal is nonzero"),
            sha256_file(&journal)?,
        ),
    ];
    let manifest = facts.into_pre_format_compatibility_manifest(checksums, build.clone())?;
    write_new_and_sync(
        &backup_directory.join(MANIFEST_FILE_NAME),
        &encode_manifest_pre_format_compatibility(&manifest)?,
    )?;
    sync_directory(backup_directory)?;
    Ok(manifest)
}

pub(crate) fn validate_database_semantics(
    artifact: &Path,
    manifest: &OfflineBackupManifestV1,
) -> Result<Database, StorageError> {
    // Preserve manifest refusal before the potentially writable engine open.
    let _ = manifest_checksum(manifest)?;
    let database = Database::open(artifact).map_err(database_error)?;
    validate_readable_database_semantics(&database, manifest)?;
    Ok(database)
}

pub(crate) fn validate_readable_database_semantics(
    database: &impl ReadableDatabase,
    manifest: &OfflineBackupManifestV1,
) -> Result<(), StorageError> {
    let expected_checksum = manifest_checksum(manifest)?;
    let facts = read_database_facts(database)?;
    // Startup-owned migrations may insert history_incarnation/v1 on pre-fence
    // artifacts. Compare non-history facts exactly, and require
    // manifest.history_incarnation ≤ staged when both are present.
    if !facts.semantically_match_manifest(manifest, &expected_checksum)? {
        return Err(corrupt());
    }
    Ok(())
}

impl DatabaseFacts {
    /// Exact non-history semantic match plus fence-compatible history/watermark rules.
    ///
    /// When both the manifest and artifact carry a history incarnation, the
    /// manifest value must be ≤ the artifact value (startup may only advance it
    /// via accepted migrations). A pre-fence manifest without the field remains
    /// acceptable. A manifest that claims an incarnation the artifact lacks is
    /// corrupt.
    ///
    /// Watermark: effective sequences (absent → 0) must match. A post-RT-B
    /// restore may re-derive a lower watermark under fencing; the artifact may
    /// therefore be ≤ the manifest claim, never greater without an equal stamp.
    fn semantically_match_manifest(
        &self,
        manifest: &OfflineBackupManifestV1,
        expected_checksum: &BackupIntegrityChecksumV1,
    ) -> Result<bool, StorageError> {
        if self.storage_format_version != manifest.storage_format_version()
            || self.database_id != manifest.database_id()
            || self.catalog_bundles != manifest.catalog_bundles()
            || self.active_catalog.as_ref() != manifest.active_catalog()
            || self.last_commit_sequence != manifest.last_commit_sequence()
            || manifest.snapshot_kind() != BackupSnapshotKindV1::StorageEngineData
        {
            return Ok(false);
        }
        let Some(checksum) = manifest.checksums().first() else {
            return Ok(false);
        };
        if checksum.artifact_ordinal() != NonZeroU32::MIN
            || checksum.checksum() != expected_checksum
        {
            return Ok(false);
        }
        match (manifest.history_incarnation(), self.history_incarnation) {
            (Some(from_manifest), Some(from_artifact)) if from_manifest > from_artifact => {
                return Ok(false);
            }
            (Some(_), None) => return Ok(false),
            _ => {}
        }
        let artifact_watermark = self.retention_watermark_sequence.unwrap_or(0);
        let manifest_watermark = manifest.effective_retention_watermark_sequence();
        // Artifact may be lower after fail-safe re-derive; never higher.
        Ok(artifact_watermark <= manifest_watermark)
    }
}

fn manifest_checksum(
    manifest: &OfflineBackupManifestV1,
) -> Result<BackupIntegrityChecksumV1, StorageError> {
    manifest_artifact_checksum(manifest, NonZeroU32::MIN)
}

pub(crate) fn manifest_journal_checksum(
    manifest: &OfflineBackupManifestV1,
) -> Result<Option<BackupIntegrityChecksumV1>, StorageError> {
    let ordinal = NonZeroU32::new(2).expect("journal artifact ordinal is nonzero");
    if manifest.checksums().len() == 1 {
        return Ok(None);
    }
    manifest_artifact_checksum(manifest, ordinal).map(Some)
}

fn manifest_format_marker_checksum(
    manifest: &OfflineBackupManifestV1,
) -> Result<Option<BackupIntegrityChecksumV1>, StorageError> {
    if manifest.checksums().len() < 3 {
        return Ok(None);
    }
    manifest_artifact_checksum(
        manifest,
        NonZeroU32::new(3).expect("format marker artifact ordinal is nonzero"),
    )
    .map(Some)
}

fn manifest_artifact_checksum(
    manifest: &OfflineBackupManifestV1,
    ordinal: NonZeroU32,
) -> Result<BackupIntegrityChecksumV1, StorageError> {
    if !matches!(manifest.checksums().len(), 1..=3) {
        return Err(corrupt());
    }
    let checksum = manifest
        .checksums()
        .iter()
        .find(|checksum| checksum.artifact_ordinal() == ordinal)
        .ok_or_else(corrupt)?;
    if checksum.checksum().as_bytes().len() != SHA256_BYTES {
        return Err(corrupt());
    }
    Ok(checksum.checksum().clone())
}

#[derive(Clone, Copy)]
enum ManifestWireEra {
    /// History, retention watermark, and physical-restore range (newest).
    Current,
    /// History + retention watermark presence tags, before format ranges.
    PostRetention,
    /// History presence tag only (post ADR-0072, pre ADR-0085 A2).
    PreRetention,
    /// Neither field (pre ADR-0072).
    PreFence,
}

fn encode_manifest(manifest: &OfflineBackupManifestV1) -> Result<Vec<u8>, StorageError> {
    encode_manifest_for_era(manifest, ManifestWireEra::Current)
}

/// Post-retention encoding from before physical restore ranges were retained.
fn encode_manifest_pre_format_compatibility(
    manifest: &OfflineBackupManifestV1,
) -> Result<Vec<u8>, StorageError> {
    if manifest.format_compatibility().is_some() {
        return Err(corrupt());
    }
    encode_manifest_for_era(manifest, ManifestWireEra::PostRetention)
}

/// History-only encoding (post-fence / pre-RT-B).
fn encode_manifest_pre_retention(
    manifest: &OfflineBackupManifestV1,
) -> Result<Vec<u8>, StorageError> {
    if manifest.retention_watermark_sequence().is_some() {
        return Err(corrupt());
    }
    encode_manifest_for_era(manifest, ManifestWireEra::PreRetention)
}

/// Pre-fence encoding omits history and watermark presence tags entirely.
fn encode_manifest_pre_fence(manifest: &OfflineBackupManifestV1) -> Result<Vec<u8>, StorageError> {
    if manifest.history_incarnation().is_some() || manifest.retention_watermark_sequence().is_some()
    {
        return Err(corrupt());
    }
    encode_manifest_for_era(manifest, ManifestWireEra::PreFence)
}

fn encode_manifest_for_era(
    manifest: &OfflineBackupManifestV1,
    era: ManifestWireEra,
) -> Result<Vec<u8>, StorageError> {
    if manifest.manifest_version() != BackupManifestVersion::V1
        || manifest.snapshot_kind() != BackupSnapshotKindV1::StorageEngineData
    {
        return Err(incompatible());
    }
    if !matches!(manifest.checksums().len(), 1..=3) {
        return Err(corrupt());
    }
    for (index, checksum) in manifest.checksums().iter().enumerate() {
        let expected = NonZeroU32::new(u32::try_from(index + 1).map_err(|_| corrupt())?)
            .ok_or_else(corrupt)?;
        if checksum.artifact_ordinal() != expected
            || checksum.checksum().as_bytes().len() != SHA256_BYTES
        {
            return Err(corrupt());
        }
    }

    let mut output = ManifestEncoder::new();
    output.bytes(MANIFEST_MAGIC)?;
    output.u32(manifest.manifest_version().get())?;
    output.u32(manifest.storage_format_version().get())?;
    output.bytes(manifest.database_id().as_bytes())?;
    output.u8(STORAGE_ENGINE_DATA_TAG)?;
    output.count(manifest.catalog_bundles().len())?;
    for bundle in manifest.catalog_bundles() {
        output.catalog_identity(
            bundle.lineage(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        )?;
    }
    match manifest.active_catalog() {
        None => output.u8(0)?,
        Some(active) => {
            output.u8(1)?;
            output.catalog_identity(
                active.lineage(),
                active.contract_version(),
                active.bundle_hash(),
            )?;
        }
    }
    match manifest.last_commit_sequence() {
        None => output.u8(0)?,
        Some(sequence) => {
            output.u8(1)?;
            output.u64(sequence.get())?;
        }
    }
    match era {
        ManifestWireEra::Current
        | ManifestWireEra::PostRetention
        | ManifestWireEra::PreRetention => match manifest.history_incarnation() {
            None => output.u8(0)?,
            Some(incarnation) => {
                output.u8(1)?;
                output.u64(incarnation)?;
            }
        },
        ManifestWireEra::PreFence => {}
    }
    if matches!(
        era,
        ManifestWireEra::Current | ManifestWireEra::PostRetention
    ) {
        match manifest.retention_watermark_sequence() {
            None => output.u8(0)?,
            Some(sequence) => {
                output.u8(1)?;
                output.u64(sequence)?;
            }
        }
    }
    if matches!(era, ManifestWireEra::Current) {
        let compatibility = manifest.format_compatibility().ok_or_else(corrupt)?;
        output.u8(1)?;
        output.u32(compatibility.minimum().epoch().get())?;
        output.u32(compatibility.minimum().writer().get())?;
        output.u32(compatibility.maximum().epoch().get())?;
        output.u32(compatibility.maximum().writer().get())?;
        output.bytes(compatibility.compatibility_fixture_digest().as_bytes())?;
    }
    output.count(manifest.checksums().len())?;
    for checksum in manifest.checksums() {
        output.u32(checksum.artifact_ordinal().get())?;
        output.framed(checksum.checksum().as_bytes())?;
    }
    output.framed(manifest.build().semantic_version().as_bytes())?;
    output.framed(manifest.build().git_revision().as_bytes())?;
    output.framed(manifest.build().rust_version().as_bytes())?;
    output.u32(manifest.build().executable_ir_version())?;
    output.count(manifest.build().enabled_features().len())?;
    for feature in manifest.build().enabled_features() {
        output.framed(feature.as_bytes())?;
    }
    Ok(output.finish())
}

fn decode_manifest(encoded: &[u8]) -> Result<OfflineBackupManifestV1, StorageError> {
    if encoded.len() > MANIFEST_MAX_BYTES {
        return Err(limit_exceeded());
    }
    // Four-path: range-bearing newest → history+watermark → history-only →
    // pre-fence. Prefer round-trip equality so an older era that partially
    // parses as a newer one falls through to its exact decoder.
    // Prefer round-trip equality so older eras that partially parse as newer
    // fall through to the correct decoder.
    if let Ok(manifest) = decode_manifest_body(encoded, ManifestWireEra::Current)
        && encode_manifest(&manifest)? == encoded
    {
        return Ok(manifest);
    }
    if let Ok(manifest) = decode_manifest_body(encoded, ManifestWireEra::PostRetention)
        && encode_manifest_pre_format_compatibility(&manifest)? == encoded
    {
        return Ok(manifest);
    }
    if let Ok(manifest) = decode_manifest_body(encoded, ManifestWireEra::PreRetention)
        && encode_manifest_pre_retention(&manifest)? == encoded
    {
        return Ok(manifest);
    }
    let manifest = decode_manifest_body(encoded, ManifestWireEra::PreFence)?;
    if encode_manifest_pre_fence(&manifest)? != encoded {
        return Err(corrupt());
    }
    Ok(manifest)
}

fn decode_manifest_body(
    encoded: &[u8],
    era: ManifestWireEra,
) -> Result<OfflineBackupManifestV1, StorageError> {
    let mut input = ManifestDecoder::new(encoded);
    if input.bytes(MANIFEST_MAGIC.len())? != MANIFEST_MAGIC {
        return Err(incompatible());
    }
    let version = input.u32()?;
    if BackupManifestVersion::new(version) != Some(BackupManifestVersion::V1) {
        return Err(incompatible());
    }
    let storage_format_version =
        StorageFormatVersion::from_supported(input.u32()?).ok_or_else(incompatible)?;
    let database_id = DatabaseId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    if input.u8()? != STORAGE_ENGINE_DATA_TAG {
        return Err(incompatible());
    }

    let bundle_count = input.count(MAX_BACKUP_CATALOG_BUNDLES)?;
    let mut catalog_bundles = Vec::with_capacity(bundle_count);
    for _ in 0..bundle_count {
        let (lineage, contract_version, bundle_hash) = input.catalog_identity()?;
        catalog_bundles.push(BackupCatalogBundleV1::new(
            lineage,
            contract_version,
            bundle_hash,
        ));
    }

    let active_catalog = match input.u8()? {
        0 => None,
        1 => {
            let (lineage, contract_version, bundle_hash) = input.catalog_identity()?;
            Some(ActiveCatalogPointerV1::new(
                lineage,
                contract_version,
                bundle_hash,
            ))
        }
        _ => return Err(corrupt()),
    };
    let last_commit_sequence = match input.u8()? {
        0 => None,
        1 => Some(CommitSequence::new(input.u64()?).ok_or_else(corrupt)?),
        _ => return Err(corrupt()),
    };
    let history_incarnation = match era {
        ManifestWireEra::Current
        | ManifestWireEra::PostRetention
        | ManifestWireEra::PreRetention => match input.u8()? {
            0 => None,
            1 => {
                let incarnation = input.u64()?;
                if incarnation < 1 {
                    return Err(corrupt());
                }
                Some(incarnation)
            }
            _ => return Err(corrupt()),
        },
        ManifestWireEra::PreFence => None,
    };
    let retention_watermark_sequence = match era {
        ManifestWireEra::Current | ManifestWireEra::PostRetention => match input.u8()? {
            0 => None,
            1 => Some(input.u64()?),
            _ => return Err(corrupt()),
        },
        ManifestWireEra::PreRetention | ManifestWireEra::PreFence => None,
    };
    let format_compatibility = match era {
        ManifestWireEra::Current => {
            if input.u8()? != 1 {
                return Err(corrupt());
            }
            let minimum_epoch = AlphaFormatEpoch::new(input.u32()?).ok_or_else(corrupt)?;
            let minimum_writer = DurableFormatWriter::new(input.u32()?);
            let maximum_epoch = AlphaFormatEpoch::new(input.u32()?).ok_or_else(corrupt)?;
            let maximum_writer = DurableFormatWriter::new(input.u32()?);
            let fixture_digest = CompatibilityFixtureDigest::from_bytes(input.array()?);
            Some(
                BackupFormatCompatibilityV1::new(
                    DurableFormatIdentity::new(minimum_epoch, minimum_writer),
                    DurableFormatIdentity::new(maximum_epoch, maximum_writer),
                    fixture_digest,
                )
                .map_err(value_error)?,
            )
        }
        ManifestWireEra::PostRetention
        | ManifestWireEra::PreRetention
        | ManifestWireEra::PreFence => None,
    };

    let checksum_count = usize::try_from(input.u32()?).map_err(|_| corrupt())?;
    if !matches!(checksum_count, 1..=3) {
        return Err(corrupt());
    }
    let mut checksums = Vec::with_capacity(checksum_count);
    for index in 0..checksum_count {
        let ordinal = NonZeroU32::new(input.u32()?).ok_or_else(corrupt)?;
        let expected = NonZeroU32::new(u32::try_from(index + 1).map_err(|_| corrupt())?)
            .ok_or_else(corrupt)?;
        if ordinal != expected {
            return Err(corrupt());
        }
        let checksum_bytes = input.framed(MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES)?;
        if checksum_bytes.len() != SHA256_BYTES {
            return Err(corrupt());
        }
        let checksum =
            BackupIntegrityChecksumV1::new(checksum_bytes.to_vec()).map_err(value_error)?;
        checksums.push(BackupArtifactChecksumV1::new(ordinal, checksum));
    }

    let semantic_version = input.text(MAX_BACKUP_BUILD_VALUE_BYTES)?;
    let git_revision = input.text(MAX_BACKUP_BUILD_VALUE_BYTES)?;
    let rust_version = input.text(MAX_BACKUP_BUILD_VALUE_BYTES)?;
    let executable_ir_version = input.u32()?;
    let feature_count = input.count(MAX_BACKUP_BUILD_FEATURES)?;
    let mut enabled_features = Vec::with_capacity(feature_count);
    for _ in 0..feature_count {
        enabled_features.push(input.text(MAX_BACKUP_BUILD_FEATURE_BYTES)?);
    }
    input.finish()?;

    let build = BackupBuildMetadataV1::new(
        semantic_version,
        git_revision,
        rust_version,
        executable_ir_version,
        enabled_features,
    )
    .map_err(value_error)?;
    match era {
        ManifestWireEra::Current => OfflineBackupManifestV1::new_with_format_compatibility(
            storage_format_version,
            database_id,
            BackupSnapshotKindV1::StorageEngineData,
            catalog_bundles,
            active_catalog,
            last_commit_sequence,
            history_incarnation,
            retention_watermark_sequence,
            format_compatibility.ok_or_else(corrupt)?,
            checksums,
            build,
        )
        .map_err(value_error),
        ManifestWireEra::PostRetention => OfflineBackupManifestV1::new_pre_format_compatibility(
            storage_format_version,
            database_id,
            BackupSnapshotKindV1::StorageEngineData,
            catalog_bundles,
            active_catalog,
            last_commit_sequence,
            history_incarnation,
            retention_watermark_sequence,
            checksums,
            build,
        )
        .map_err(value_error),
        ManifestWireEra::PreRetention => OfflineBackupManifestV1::new_pre_retention(
            storage_format_version,
            database_id,
            BackupSnapshotKindV1::StorageEngineData,
            catalog_bundles,
            active_catalog,
            last_commit_sequence,
            history_incarnation,
            checksums,
            build,
        )
        .map_err(value_error),
        ManifestWireEra::PreFence => OfflineBackupManifestV1::new_pre_fence(
            storage_format_version,
            database_id,
            BackupSnapshotKindV1::StorageEngineData,
            catalog_bundles,
            active_catalog,
            last_commit_sequence,
            checksums,
            build,
        )
        .map_err(value_error),
    }
}

struct ManifestEncoder {
    bytes: Vec<u8>,
}

impl ManifestEncoder {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn reserve(&self, additional: usize) -> Result<(), StorageError> {
        if self
            .bytes
            .len()
            .checked_add(additional)
            .is_none_or(|length| length > MANIFEST_MAX_BYTES)
        {
            return Err(limit_exceeded());
        }
        Ok(())
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), StorageError> {
        self.reserve(value.len())?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn u8(&mut self, value: u8) -> Result<(), StorageError> {
        self.bytes(&[value])
    }

    fn u32(&mut self, value: u32) -> Result<(), StorageError> {
        self.bytes(&value.to_be_bytes())
    }

    fn u64(&mut self, value: u64) -> Result<(), StorageError> {
        self.bytes(&value.to_be_bytes())
    }

    fn count(&mut self, value: usize) -> Result<(), StorageError> {
        self.u32(u32::try_from(value).map_err(|_| limit_exceeded())?)
    }

    fn framed(&mut self, value: &[u8]) -> Result<(), StorageError> {
        self.count(value.len())?;
        self.bytes(value)
    }

    fn catalog_identity(
        &mut self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Result<(), StorageError> {
        self.framed(lineage.as_bytes())?;
        self.u64(contract_version.get())?;
        self.bytes(bundle_hash.as_bytes())
    }
}

struct ManifestDecoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> ManifestDecoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn finish(self) -> Result<(), StorageError> {
        if self.offset == self.encoded.len() {
            Ok(())
        } else {
            Err(corrupt())
        }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], StorageError> {
        let end = self.offset.checked_add(length).ok_or_else(corrupt)?;
        let bytes = self.encoded.get(self.offset..end).ok_or_else(corrupt)?;
        self.offset = end;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], StorageError> {
        self.bytes(N)?.try_into().map_err(|_| corrupt())
    }

    fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn count(&mut self, maximum: usize) -> Result<usize, StorageError> {
        let value = usize::try_from(self.u32()?).map_err(|_| limit_exceeded())?;
        if value > maximum {
            return Err(limit_exceeded());
        }
        Ok(value)
    }

    fn framed(&mut self, maximum: usize) -> Result<&'a [u8], StorageError> {
        let length = self.count(maximum)?;
        self.bytes(length)
    }

    fn text(&mut self, maximum: usize) -> Result<String, StorageError> {
        let bytes = self.framed(maximum)?;
        let text = std::str::from_utf8(bytes).map_err(|_| corrupt())?;
        Ok(text.to_owned())
    }

    fn catalog_identity(
        &mut self,
    ) -> Result<(ContractLineage, ContractVersion, ContractBundleHash), StorageError> {
        let lineage =
            ContractLineage::new(self.text(MAX_CONTRACT_LINEAGE_BYTES)?).map_err(|_| corrupt())?;
        let contract_version = ContractVersion::new(self.u64()?).ok_or_else(corrupt)?;
        let bundle_hash = ContractBundleHash::from_bytes(self.array()?);
        Ok((lineage, contract_version, bundle_hash))
    }
}

pub(crate) fn sha256_file(path: &Path) -> Result<BackupIntegrityChecksumV1, StorageError> {
    let file = File::open(path).map_err(io_unavailable)?;
    sha256_reader(file)
}

pub(crate) fn sha256_reader(
    mut reader: impl Read,
) -> Result<BackupIntegrityChecksumV1, StorageError> {
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer).map_err(io_unavailable)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    BackupIntegrityChecksumV1::new(hasher.finalize().to_vec()).map_err(value_error)
}

pub(crate) fn copy_and_sync(source: &Path, destination: &Path) -> Result<(), StorageError> {
    fs::copy(source, destination).map_err(io_unavailable)?;
    File::open(destination)
        .and_then(|file| file.sync_all())
        .map_err(io_unavailable)
}

fn write_new_and_sync(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io_unavailable)?;
    file.write_all(bytes).map_err(io_unavailable)?;
    file.sync_all().map_err(io_unavailable)
}

fn read_bounded_file(path: &Path, maximum: usize) -> Result<Vec<u8>, StorageError> {
    let mut file = File::open(path).map_err(io_unavailable)?;
    let limit = u64::try_from(maximum)
        .map_err(|_| limit_exceeded())?
        .checked_add(1)
        .ok_or_else(limit_exceeded)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(limit)
        .read_to_end(&mut bytes)
        .map_err(io_unavailable)?;
    if bytes.len() > maximum {
        return Err(limit_exceeded());
    }
    Ok(bytes)
}

fn validate_backup_inventory(directory: &Path) -> Result<(), StorageError> {
    let metadata = fs::symlink_metadata(directory).map_err(io_unavailable)?;
    if !metadata.file_type().is_dir() {
        return Err(corrupt());
    }
    let mut names = Vec::with_capacity(4);
    for entry in fs::read_dir(directory).map_err(io_unavailable)? {
        let entry = entry.map_err(io_unavailable)?;
        if names.len() == 4 {
            return Err(corrupt());
        }
        let metadata = fs::symlink_metadata(entry.path()).map_err(io_unavailable)?;
        if !metadata.file_type().is_file() {
            return Err(corrupt());
        }
        names.push(entry.file_name());
    }
    names.sort_unstable();
    let mut legacy = [
        OsString::from(DATABASE_ARTIFACT_FILE_NAME),
        OsString::from(MANIFEST_FILE_NAME),
    ];
    legacy.sort_unstable();
    let mut post_journal = [
        OsString::from(DATABASE_ARTIFACT_FILE_NAME),
        OsString::from(JOURNAL_ARTIFACT_FILE_NAME),
        OsString::from(MANIFEST_FILE_NAME),
    ];
    post_journal.sort_unstable();
    let mut current = [
        OsString::from(DATABASE_ARTIFACT_FILE_NAME),
        OsString::from(FORMAT_MARKER_ARTIFACT_FILE_NAME),
        OsString::from(JOURNAL_ARTIFACT_FILE_NAME),
        OsString::from(MANIFEST_FILE_NAME),
    ];
    current.sort_unstable();
    if names != legacy && names != post_journal && names != current {
        return Err(corrupt());
    }
    Ok(())
}

fn validate_manifest_inventory(
    directory: &Path,
    manifest: &OfflineBackupManifestV1,
) -> Result<(), StorageError> {
    let journal_exists = directory
        .join(JOURNAL_ARTIFACT_FILE_NAME)
        .try_exists()
        .map_err(io_unavailable)?;
    if journal_exists != manifest_journal_checksum(manifest)?.is_some() {
        return Err(corrupt());
    }
    let marker_exists = directory
        .join(FORMAT_MARKER_ARTIFACT_FILE_NAME)
        .try_exists()
        .map_err(io_unavailable)?;
    if marker_exists != manifest_format_marker_checksum(manifest)?.is_some() {
        return Err(corrupt());
    }
    if marker_exists {
        validate_manifest_format_marker(
            &directory.join(FORMAT_MARKER_ARTIFACT_FILE_NAME),
            manifest,
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TargetDirectoryState {
    Missing,
    Empty,
    NonEmpty,
}

fn target_directory_state(path: &Path) -> Result<TargetDirectoryState, StorageError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(TargetDirectoryState::Missing);
        }
        Err(error) => return Err(io_unavailable(error)),
    };
    if !metadata.file_type().is_dir() {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    let mut entries = fs::read_dir(path).map_err(io_unavailable)?;
    match entries.next() {
        None => Ok(TargetDirectoryState::Empty),
        Some(Ok(_)) => Ok(TargetDirectoryState::NonEmpty),
        Some(Err(error)) => Err(io_unavailable(error)),
    }
}

pub(crate) fn lock_existing_target(path: &Path) -> Result<Option<Database>, StorageError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_unavailable(error)),
        Ok(metadata) if !metadata.file_type().is_file() => {
            Err(storage_error(StorageErrorKind::Unavailable))
        }
        Ok(_) => match Database::open(path) {
            Ok(database) => Ok(Some(database)),
            Err(redb::DatabaseError::Storage(redb::StorageError::Io(error)))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::InvalidData
                ) =>
            {
                Ok(None)
            }
            // An explicit destructive restore may replace a corrupt or old-
            // format target, but never one whose offline status is uncertain.
            Err(error) => {
                let mapped = database_error(error);
                match mapped.kind() {
                    StorageErrorKind::CorruptData | StorageErrorKind::IncompatibleFormat => {
                        Ok(None)
                    }
                    _ => Err(mapped),
                }
            }
        },
    }
}

fn require_absent(path: &Path) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_unavailable(error)),
        Ok(_) => Err(storage_error(StorageErrorKind::Unavailable)),
    }
}

struct StagingDirectory {
    path: PathBuf,
    published: bool,
}

impl StagingDirectory {
    fn create_sibling(destination: &Path, label: &str) -> Result<Self, StorageError> {
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(io_unavailable)?;
        let stem = destination
            .file_name()
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        for ordinal in 0..MAX_STAGING_ATTEMPTS {
            let candidate = parent.join(staging_name(stem, label, ordinal));
            match fs::create_dir(&candidate) {
                Ok(()) => {
                    return Ok(Self {
                        path: candidate,
                        published: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(io_unavailable(error)),
            }
        }
        Err(storage_error(StorageErrorKind::Unavailable))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn mark_published(&mut self) {
        self.published = true;
    }
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

struct StagedArtifact {
    path: PathBuf,
    published: bool,
}

impl StagedArtifact {
    fn create(target_directory: &Path) -> Result<Self, StorageError> {
        Self::create_named(target_directory, OsStr::new(DATABASE_ARTIFACT_FILE_NAME))
    }

    fn create_named(target_directory: &Path, stem: &OsStr) -> Result<Self, StorageError> {
        for ordinal in 0..MAX_STAGING_ATTEMPTS {
            let path = target_directory.join(staging_name(stem, "restore", ordinal));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    drop(file);
                    return Ok(Self {
                        path,
                        published: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(io_unavailable(error)),
            }
        }
        Err(storage_error(StorageErrorKind::Unavailable))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn mark_published(&mut self) {
        self.published = true;
    }
}

impl Drop for StagedArtifact {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn staging_name(stem: &OsStr, label: &str, ordinal: u16) -> OsString {
    let mut name = OsString::from(".");
    name.push(stem);
    name.push(format!(".riffdb-{label}-{}-{ordinal}", std::process::id()));
    name
}

pub(crate) fn sync_parent(path: &Path) -> Result<(), StorageError> {
    sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
}

pub(crate) fn sync_directory(path: &Path) -> Result<(), StorageError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(io_unavailable)
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

fn incompatible() -> StorageError {
    storage_error(StorageErrorKind::IncompatibleFormat)
}

fn limit_exceeded() -> StorageError {
    storage_error(StorageErrorKind::LimitExceeded)
}

fn unknown() -> StorageError {
    storage_error(StorageErrorKind::CommitStatusUnknown)
}

#[cfg(test)]
#[path = "backup_v3_tests.rs"]
mod v3_tests;

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom};
    use std::sync::atomic::{AtomicU64, Ordering};

    use riffdb_storage_api::{
        DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort,
        DatabaseInitializationResult, EvidencePageLimit, HistoricalEvidenceCursor,
        HistoricalEvidencePage, ReadableCapabilityDigestInventory, ReadableDigestKey,
        ReadableIdempotencyDigestInventory, StartupValidationInputs, StructuralEvidenceCursor,
        StructuralEvidenceOpen, StructuralEvidencePage, StructuralEvidenceSession,
    };
    use riffdb_types::{DigestKeyId, Timestamp};

    use super::*;
    use crate::store::RedbStore;

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let ordinal = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
            let path = crate::test_path::root().join(format!(
                "riffdb-redb-backup-{label}-{}-{ordinal}",
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

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .expect("database ID")
    }

    fn build_metadata() -> BackupBuildMetadataV1 {
        BackupBuildMetadataV1::new(
            "0.1.0",
            "0123456789abcdef",
            "rustc-1.97.0",
            1,
            vec!["alpha".to_owned(), "zeta".to_owned()],
        )
        .expect("build metadata")
    }

    fn initialize(path: &Path) {
        let mut store = RedbStore::open(path).expect("open database");
        assert_eq!(
            store.probe_database_identity().expect("probe database"),
            DatabaseIdentityProbe::NeedsInitialization
        );
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize database"),
            DatabaseInitializationResult::Installed(database_id())
        );
    }

    fn complete_structural_scan(store: RedbStore) -> DatabaseId {
        let digest_key = DigestKeyId::new(1).expect("digest key ID");
        let inputs = StartupValidationInputs::new(
            Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
            ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
                .expect("capability digest inventory"),
            ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
                .expect("idempotency digest inventory"),
        );
        let mut session = store
            .begin_structural_evidence(inputs)
            .expect("begin restored structural scan");
        let database_id = session.database_id();
        let open_session_id = session.open_session_id();
        let limit = EvidencePageLimit::new(64).expect("evidence page limit");

        let mut structural = StructuralEvidenceCursor::start(database_id, open_session_id);
        let structural_end = loop {
            match session
                .read_structural_evidence(structural, limit)
                .expect("read restored structural evidence")
            {
                StructuralEvidencePage::Page { next, .. } => structural = next,
                StructuralEvidencePage::ExactEnd(end) => break end,
            }
        };
        let mut historical = HistoricalEvidenceCursor::start(database_id, open_session_id);
        let historical_end = loop {
            match session
                .read_historical_evidence(historical, limit)
                .expect("read restored historical evidence")
            {
                HistoricalEvidencePage::Page { next, .. } => historical = next,
                HistoricalEvidencePage::ExactEnd(end) => break end,
            }
        };
        let outcome = session
            .finish(structural_end, historical_end)
            .expect("finish restored structural scan");
        let riffdb_storage_api::StructuralOpenOutcome::Clean(opened) = outcome else {
            panic!("V2 backup fixture must reopen cleanly");
        };
        assert_eq!(opened.database_id(), database_id);
        database_id
    }

    fn create_backup(root: &TestRoot) -> (PathBuf, PathBuf, OfflineBackupManifestV1) {
        let source = root.join("source.redb");
        let backup = root.join("backup");
        initialize(&source);
        let manifest = RedbOfflineBackup::bind(&source, &backup)
            .create_offline_backup(&build_metadata())
            .expect("create backup");
        (source, backup, manifest)
    }

    #[test]
    fn backup_and_restore_preserve_identity_and_publish_exact_inventory() {
        let root = TestRoot::new("round-trip");
        let (_source, backup, manifest) = create_backup(&root);
        assert_eq!(manifest.database_id(), database_id());
        assert_eq!(manifest.checksums().len(), 3);

        let mut names = fs::read_dir(&backup)
            .expect("read backup")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                OsString::from(DATABASE_ARTIFACT_FILE_NAME),
                OsString::from(FORMAT_MARKER_ARTIFACT_FILE_NAME),
                OsString::from(JOURNAL_ARTIFACT_FILE_NAME),
                OsString::from(MANIFEST_FILE_NAME),
            ]
        );

        let target = root.join("restored");
        let restored = RedbOfflineRestore::bind(&backup, &target)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
            .expect("restore backup");
        assert_eq!(
            restored,
            OfflineRestoreResultV1::Restored {
                manifest: Box::new(manifest)
            }
        );
        let reopened = RedbStore::open(target.join(DATABASE_ARTIFACT_FILE_NAME))
            .expect("open restored database");
        assert_eq!(
            reopened
                .probe_database_identity()
                .expect("probe restored database"),
            DatabaseIdentityProbe::Existing(database_id())
        );
        assert_eq!(complete_structural_scan(reopened), database_id());
        let restored_journal =
            crate::journal::journal_path(&target.join(DATABASE_ARTIFACT_FILE_NAME));
        assert!(restored_journal.is_file());
        let (_, tail) = crate::journal::scan_journal(&restored_journal, database_id(), |_| Ok(()))
            .expect("scan restored journal")
            .expect("restored journal exists");
        assert_eq!(tail.transition_count, 0);
        assert!(!tail.incomplete_tail);
    }

    #[test]
    fn nonempty_restore_requires_explicit_destructive_policy() {
        let root = TestRoot::new("destructive-policy");
        let (_source, backup, _) = create_backup(&root);
        let target = root.join("target");
        fs::create_dir(&target).expect("create target");
        fs::write(target.join("operator-file"), b"preserve until approved")
            .expect("write operator file");

        let refused = RedbOfflineRestore::bind(&backup, &target)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
            .expect("typed refusal");
        assert_eq!(refused, OfflineRestoreResultV1::TargetNotEmpty);
        assert!(!target.join(DATABASE_ARTIFACT_FILE_NAME).exists());

        let restored = RedbOfflineRestore::bind(&backup, &target)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive)
            .expect("explicit destructive restore");
        assert!(matches!(restored, OfflineRestoreResultV1::Restored { .. }));
        assert!(target.join(DATABASE_ARTIFACT_FILE_NAME).is_file());
    }

    #[test]
    fn incompatible_backup_range_refuses_before_creating_or_replacing_target() {
        let root = TestRoot::new("incompatible-format-restore");
        let (_source, backup, manifest) = create_backup(&root);
        let current = riffdb_storage_api::current_durable_format_manifest();
        let incompatible_writer = DurableFormatWriter::new(
            current
                .identity()
                .writer()
                .get()
                .checked_add(1)
                .expect("test writer increment"),
        );
        let incompatible_identity =
            DurableFormatIdentity::new(current.identity().epoch(), incompatible_writer);
        let incompatible_range = BackupFormatCompatibilityV1::new(
            incompatible_identity,
            incompatible_identity,
            current.compatibility_fixture_digest(),
        )
        .expect("valid disjoint range");
        let incompatible_manifest = OfflineBackupManifestV1::new_with_format_compatibility(
            manifest.storage_format_version(),
            manifest.database_id(),
            manifest.snapshot_kind(),
            manifest.catalog_bundles().to_vec(),
            manifest.active_catalog().cloned(),
            manifest.last_commit_sequence(),
            manifest.history_incarnation(),
            manifest.retention_watermark_sequence(),
            incompatible_range,
            manifest.checksums().to_vec(),
            manifest.build().clone(),
        )
        .expect("incompatible manifest remains structurally valid");
        fs::write(
            backup.join(MANIFEST_FILE_NAME),
            encode_manifest(&incompatible_manifest).expect("encode incompatible range"),
        )
        .expect("replace test manifest");

        let absent_target = root.join("absent-target");
        let error = RedbOfflineRestore::bind(&backup, &absent_target)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
            .expect_err("incompatible range must refuse");
        assert_eq!(error.kind(), StorageErrorKind::IncompatibleFormat);
        assert!(
            !absent_target.exists(),
            "restore must not create the target"
        );

        let existing_target = root.join("existing-target");
        fs::create_dir(&existing_target).expect("create existing target");
        let sentinel = existing_target.join("sentinel");
        fs::write(&sentinel, b"unchanged").expect("write sentinel");
        let error = RedbOfflineRestore::bind(&backup, &existing_target)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive)
            .expect_err("destructive policy cannot bypass compatibility");
        assert_eq!(error.kind(), StorageErrorKind::IncompatibleFormat);
        assert_eq!(fs::read(&sentinel).expect("read sentinel"), b"unchanged");
        assert_eq!(
            fs::read_dir(&existing_target).expect("read target").count(),
            1,
            "restore must not stage files in an incompatible target"
        );
    }

    #[test]
    fn corrupt_artifact_is_rejected_before_target_replacement() {
        let root = TestRoot::new("corrupt-before-replace");
        let (_source, backup, _) = create_backup(&root);
        let target = root.join("target");
        fs::create_dir(&target).expect("create target");
        let sentinel = target.join("sentinel");
        fs::write(&sentinel, b"unchanged").expect("write sentinel");

        let artifact = backup.join(DATABASE_ARTIFACT_FILE_NAME);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&artifact)
            .expect("open artifact");
        file.seek(SeekFrom::Start(0)).expect("seek artifact");
        file.write_all(&[0xff]).expect("corrupt artifact");
        file.sync_all().expect("sync corruption");

        let error = RedbOfflineRestore::bind(&backup, &target)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive)
            .expect_err("checksum mismatch must reject");
        assert_eq!(error.kind(), StorageErrorKind::CorruptData);
        assert_eq!(fs::read(sentinel).expect("read sentinel"), b"unchanged");
        assert!(!target.join(DATABASE_ARTIFACT_FILE_NAME).exists());
    }

    #[test]
    fn corrupt_journal_artifact_is_rejected_before_target_replacement() {
        let root = TestRoot::new("corrupt-journal-before-replace");
        let (_source, backup, _) = create_backup(&root);
        let target = root.join("target");
        fs::create_dir(&target).expect("create target");
        let sentinel = target.join("sentinel");
        fs::write(&sentinel, b"unchanged").expect("write sentinel");

        let journal = backup.join(JOURNAL_ARTIFACT_FILE_NAME);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&journal)
            .expect("open journal artifact");
        file.seek(SeekFrom::Start(0)).expect("seek journal");
        file.write_all(&[0xff]).expect("corrupt journal");
        file.sync_all().expect("sync corruption");

        let error = RedbOfflineRestore::bind(&backup, &target)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive)
            .expect_err("journal checksum mismatch must reject");
        assert_eq!(error.kind(), StorageErrorKind::CorruptData);
        assert_eq!(fs::read(sentinel).expect("read sentinel"), b"unchanged");
        assert!(!target.join(DATABASE_ARTIFACT_FILE_NAME).exists());
    }

    #[test]
    fn backup_publication_failpoints_distinguish_absence_from_uncertainty() {
        let root = TestRoot::new("backup-failpoints");
        let source = root.join("source.redb");
        initialize(&source);

        let before_destination = root.join("before");
        let before = RedbTestController::return_before_commit(RedbTestOperation::Backup);
        let error =
            RedbOfflineBackup::bind_with_test_controller(&source, &before_destination, before)
                .create_offline_backup(&build_metadata())
                .expect_err("pre-publication failure");
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        assert!(!before_destination.exists());

        let after_destination = root.join("after");
        let after = RedbTestController::return_unknown_after_commit(RedbTestOperation::Backup);
        let error =
            RedbOfflineBackup::bind_with_test_controller(&source, &after_destination, after)
                .create_offline_backup(&build_metadata())
                .expect_err("post-publication uncertainty");
        assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
        validate_backup_inventory(&after_destination).expect("published backup is complete");
    }

    #[test]
    fn backup_and_restore_require_offline_database_ownership() {
        let root = TestRoot::new("offline-ownership");
        let source = root.join("source.redb");
        initialize(&source);
        let live_source = RedbStore::open(&source).expect("hold source open");
        let backup = root.join("backup");
        let error = RedbOfflineBackup::bind(&source, &backup)
            .create_offline_backup(&build_metadata())
            .expect_err("live source must reject offline backup");
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        assert!(!backup.exists());
        drop(live_source);
        RedbOfflineBackup::bind(&source, &backup)
            .create_offline_backup(&build_metadata())
            .expect("create backup after source closes");

        let target = root.join("target");
        fs::create_dir(&target).expect("create target directory");
        let target_artifact = target.join(DATABASE_ARTIFACT_FILE_NAME);
        initialize(&target_artifact);
        let live_target = RedbStore::open(&target_artifact).expect("hold target open");
        let error = RedbOfflineRestore::bind(&backup, &target)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive)
            .expect_err("live target must reject offline restore");
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        drop(live_target);
        assert!(matches!(
            RedbOfflineRestore::bind(&backup, &target)
                .restore_offline_backup(
                    OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive,
                )
                .expect("restore after target closes"),
            OfflineRestoreResultV1::Restored { .. }
        ));
    }

    #[test]
    fn restore_publication_failpoints_distinguish_absence_from_uncertainty() {
        let root = TestRoot::new("restore-failpoints");
        let (_source, backup, _) = create_backup(&root);

        let before_target = root.join("before-target");
        let before = RedbTestController::return_before_commit(RedbTestOperation::Restore);
        let error = RedbOfflineRestore::bind_with_test_controller(&backup, &before_target, before)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
            .expect_err("pre-publication restore failure");
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        assert!(!before_target.join(DATABASE_ARTIFACT_FILE_NAME).exists());

        let after_target = root.join("after-target");
        let after = RedbTestController::return_unknown_after_commit(RedbTestOperation::Restore);
        let error = RedbOfflineRestore::bind_with_test_controller(&backup, &after_target, after)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
            .expect_err("post-publication restore uncertainty");
        assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
        let restored = RedbStore::open(after_target.join(DATABASE_ARTIFACT_FILE_NAME))
            .expect("uncertain restore was published");
        assert_eq!(
            restored
                .probe_database_identity()
                .expect("probe uncertain restore"),
            DatabaseIdentityProbe::Existing(database_id())
        );
    }

    #[test]
    fn manifest_encoding_is_canonical_and_versioned() {
        let checksum = BackupIntegrityChecksumV1::new(vec![0x5a; SHA256_BYTES]).expect("checksum");
        let manifest = DatabaseFacts {
            storage_format_version: StorageFormatVersion::V1,
            database_id: database_id(),
            catalog_bundles: Vec::new(),
            active_catalog: None,
            last_commit_sequence: None,
            history_incarnation: Some(1),
            retention_watermark_sequence: None,
        }
        .into_manifest_with_format_compatibility(
            vec![BackupArtifactChecksumV1::new(NonZeroU32::MIN, checksum)],
            build_metadata(),
            BackupFormatCompatibilityV1::new(
                riffdb_storage_api::DurableFormatIdentity::new(
                    riffdb_storage_api::AlphaFormatEpoch::new(1).unwrap(),
                    riffdb_storage_api::DurableFormatWriter::new(1),
                ),
                riffdb_storage_api::DurableFormatIdentity::new(
                    riffdb_storage_api::AlphaFormatEpoch::new(1).unwrap(),
                    riffdb_storage_api::DurableFormatWriter::new(1),
                ),
                riffdb_storage_api::CompatibilityFixtureDigest::from_bytes([
                    255, 96, 181, 248, 173, 42, 3, 29, 240, 245, 0, 250, 149, 117, 23, 35, 220, 34,
                    121, 248, 18, 168, 25, 188, 8, 143, 148, 87, 203, 102, 150, 41,
                ]),
            )
            .unwrap(),
        )
        .expect("manifest");
        let encoded = encode_manifest(&manifest).expect("encode manifest");
        assert_eq!(
            decode_manifest(&encoded).expect("decode manifest"),
            manifest
        );
        let golden_digest: [u8; SHA256_BYTES] = Sha256::digest(&encoded).into();
        // Freeze all input metadata, including the pre-V3 receipt corpus digest.
        // Adding a release fixture must not rotate this V1 encoding proof. This
        // tests byte compatibility, not permission to restore an older release.
        assert_eq!(
            golden_digest,
            [
                148, 175, 59, 244, 43, 183, 178, 87, 8, 154, 90, 103, 236, 56, 41, 240, 200, 136,
                208, 4, 126, 221, 80, 83, 99, 217, 228, 63, 74, 249, 74, 172,
            ],
            "manifest v1 encoding is a durable compatibility boundary"
        );

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(
            decode_manifest(&trailing)
                .expect_err("trailing byte")
                .kind(),
            StorageErrorKind::CorruptData
        );

        let mut unsupported = encoded;
        unsupported[MANIFEST_MAGIC.len() + 3] = 2;
        assert_eq!(
            decode_manifest(&unsupported)
                .expect_err("unsupported manifest version")
                .kind(),
            StorageErrorKind::IncompatibleFormat
        );
    }

    #[test]
    fn semantic_bytes_wire_eras_match_real_encoder_lengths() {
        // pre-fence / pre-retention / post-retention / current range-bearing
        let checksum = BackupIntegrityChecksumV1::new(vec![0x5a; SHA256_BYTES]).expect("checksum");
        let build = build_metadata();
        let pre = OfflineBackupManifestV1::new_pre_fence(
            StorageFormatVersion::V1,
            database_id(),
            BackupSnapshotKindV1::StorageEngineData,
            Vec::new(),
            None,
            None,
            vec![BackupArtifactChecksumV1::new(
                NonZeroU32::MIN,
                checksum.clone(),
            )],
            build.clone(),
        )
        .expect("pre-fence");
        let history_only = OfflineBackupManifestV1::new_pre_retention(
            StorageFormatVersion::V1,
            database_id(),
            BackupSnapshotKindV1::StorageEngineData,
            Vec::new(),
            None,
            None,
            Some(1),
            vec![BackupArtifactChecksumV1::new(
                NonZeroU32::MIN,
                checksum.clone(),
            )],
            build.clone(),
        )
        .expect("pre-retention");
        let post_wm_none = OfflineBackupManifestV1::new_pre_format_compatibility(
            StorageFormatVersion::V1,
            database_id(),
            BackupSnapshotKindV1::StorageEngineData,
            Vec::new(),
            None,
            None,
            Some(1),
            None,
            vec![BackupArtifactChecksumV1::new(
                NonZeroU32::MIN,
                checksum.clone(),
            )],
            build.clone(),
        )
        .expect("post-retention wm None");
        let post_wm_some = OfflineBackupManifestV1::new_pre_format_compatibility(
            StorageFormatVersion::V1,
            database_id(),
            BackupSnapshotKindV1::StorageEngineData,
            Vec::new(),
            None,
            None,
            Some(1),
            Some(0),
            vec![BackupArtifactChecksumV1::new(NonZeroU32::MIN, checksum)],
            build,
        )
        .expect("post-retention wm Some(0)");
        let current = OfflineBackupManifestV1::new(
            StorageFormatVersion::V1,
            database_id(),
            BackupSnapshotKindV1::StorageEngineData,
            Vec::new(),
            None,
            None,
            Some(1),
            Some(0),
            post_wm_some.checksums().to_vec(),
            post_wm_some.build().clone(),
        )
        .expect("current range-bearing manifest");

        assert!(!pre.history_wire_tagged());
        assert!(!pre.retention_watermark_wire_tagged());
        assert!(history_only.history_wire_tagged());
        assert!(!history_only.retention_watermark_wire_tagged());
        assert!(post_wm_none.retention_watermark_wire_tagged());
        assert!(post_wm_some.retention_watermark_wire_tagged());
        assert!(!post_wm_some.format_compatibility_wire_tagged());
        assert!(current.format_compatibility_wire_tagged());

        let encoded_pre = encode_manifest_pre_fence(&pre).expect("encode pre");
        let encoded_history =
            encode_manifest_pre_retention(&history_only).expect("encode history-only");
        let encoded_wm_none =
            encode_manifest_pre_format_compatibility(&post_wm_none).expect("encode wm none");
        let encoded_wm_some =
            encode_manifest_pre_format_compatibility(&post_wm_some).expect("encode wm some");
        let encoded_current = encode_manifest(&current).expect("encode current");

        // History presence(1)+u64(8) over pre-fence.
        assert_eq!(encoded_history.len() - encoded_pre.len(), 9);
        // Watermark presence only (None).
        assert_eq!(encoded_wm_none.len() - encoded_history.len(), 1);
        // Watermark Some adds 8 over presence-0.
        assert_eq!(encoded_wm_some.len() - encoded_wm_none.len(), 8);
        // Range presence + four u32 identity components + SHA-256 fixture digest.
        assert_eq!(encoded_current.len() - encoded_wm_some.len(), 49);
        assert_eq!(
            history_only.semantic_bytes() - pre.semantic_bytes(),
            encoded_history.len() - encoded_pre.len()
        );
        assert_eq!(
            post_wm_none.semantic_bytes() - history_only.semantic_bytes(),
            encoded_wm_none.len() - encoded_history.len()
        );
        assert_eq!(
            post_wm_some.semantic_bytes() - post_wm_none.semantic_bytes(),
            encoded_wm_some.len() - encoded_wm_none.len()
        );
        assert_eq!(
            current.semantic_bytes() - post_wm_some.semantic_bytes(),
            encoded_current.len() - encoded_wm_some.len()
        );
        assert_eq!(
            decode_manifest(&encoded_wm_some).expect("decode historical post-retention"),
            post_wm_some
        );
        assert_eq!(
            decode_manifest(&encoded_current).expect("decode current range"),
            current
        );
    }

    #[test]
    fn pre_fence_manifest_and_stamp_round_trip() {
        let checksum = BackupIntegrityChecksumV1::new(vec![0x5a; SHA256_BYTES]).expect("checksum");
        // Pre-fence constructor omits both wire tags.
        let pre_fence = OfflineBackupManifestV1::new_pre_fence(
            StorageFormatVersion::V1,
            database_id(),
            BackupSnapshotKindV1::StorageEngineData,
            Vec::new(),
            None,
            None,
            vec![BackupArtifactChecksumV1::new(
                NonZeroU32::MIN,
                checksum.clone(),
            )],
            build_metadata(),
        )
        .expect("pre-fence manifest");
        let encoded = encode_manifest_pre_fence(&pre_fence).expect("encode pre-fence");
        let decoded = decode_manifest(&encoded).expect("decode pre-fence via triple-path");
        assert_eq!(decoded.history_incarnation(), None);
        assert_eq!(decoded.retention_watermark_sequence(), None);
        assert_eq!(decoded.database_id(), pre_fence.database_id());

        // Newest encode with explicit watermark also round-trips.
        let with_fields = DatabaseFacts {
            storage_format_version: StorageFormatVersion::V1,
            database_id: database_id(),
            catalog_bundles: Vec::new(),
            active_catalog: None,
            last_commit_sequence: None,
            history_incarnation: Some(1),
            retention_watermark_sequence: None,
        }
        .into_manifest(
            vec![BackupArtifactChecksumV1::new(NonZeroU32::MIN, checksum)],
            build_metadata(),
        )
        .expect("post-retention manifest");
        assert_eq!(with_fields.retention_watermark_sequence(), Some(0));
        let post = encode_manifest(&with_fields).expect("encode post-retention");
        assert_eq!(
            decode_manifest(&post).expect("decode post-retention"),
            with_fields
        );
    }

    #[test]
    fn read_history_incarnation_absent_vs_unreadable() {
        let root = crate::test_path::ScopedDirectory::new("history-read");
        let db_path = root.join("database.redb");

        // Absent file → open fails (not silently 0).
        assert!(read_history_incarnation(&db_path).is_err());

        let mut store = crate::RedbStore::open(&db_path).expect("open");
        store
            .initialize_database(database_id())
            .expect("initialize");
        drop(store);
        assert_eq!(
            read_history_incarnation(&db_path).expect("read present"),
            Some(1)
        );

        // Truncate to corrupt: present-but-unreadable fails closed.
        std::fs::write(&db_path, b"not-a-redb-file").expect("corrupt");
        assert!(
            read_history_incarnation(&db_path).is_err(),
            "corrupt target must not be treated as incarnation 0"
        );
    }

    #[test]
    fn stamp_history_incarnation_is_idempotent() {
        let root = crate::test_path::ScopedDirectory::new("history-stamp");
        let db_path = root.join("database.redb");
        let mut store = crate::RedbStore::open(&db_path).expect("open");
        store
            .initialize_database(database_id())
            .expect("initialize");
        drop(store);

        stamp_history_incarnation(&db_path, 2).expect("stamp 2");
        assert_eq!(read_history_incarnation(&db_path).expect("read"), Some(2));
        stamp_history_incarnation(&db_path, 2).expect("idempotent stamp");
        assert_eq!(read_history_incarnation(&db_path).expect("read"), Some(2));
    }
}
