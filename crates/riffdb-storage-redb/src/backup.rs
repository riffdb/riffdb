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
/// Retained sealed journal extents (ADR-0263 A5) follow the three fixed
/// artifacts, one per extent in generation order.
const FIRST_SEALED_ARTIFACT_ORDINAL: u32 = 4;
const SEALED_ARTIFACT_PREFIX: &str = "journal.sealed.";
const SEALED_ARTIFACT_SUFFIX: &str = ".riffextent";
/// A backup never names more sealed extents than its manifest can checksum.
const MAX_SEALED_ARTIFACTS: usize = riffdb_storage_api::MAX_BACKUP_ARTIFACT_CHECKSUMS - 3;
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

/// Stamps only a closed, sealed private stage; ordinary recovery is separate.
pub(crate) fn stamp_sealed_history_incarnation(
    path: &Path,
    incarnation: u64,
) -> Result<(), StorageError> {
    if incarnation < HISTORY_INCARNATION_INITIAL {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    stamp_incarnation(open_unrepaired_private_database(path)?, incarnation, None)
}

pub(crate) fn stamp_private_archive_incarnation(
    file: &File,
    binding: crate::maintenance::PrivateArchiveValidationBinding,
    incarnation: u64,
) -> Result<(), StorageError> {
    let database = open_unrepaired_private_file(file, None)?;
    stamp_incarnation(database, incarnation, Some(binding))
}

fn stamp_incarnation(
    database: Database,
    incarnation: u64,
    private: Option<crate::maintenance::PrivateArchiveValidationBinding>,
) -> Result<(), StorageError> {
    stamp_incarnation_retaining_database(database, incarnation, private).map(drop)
}

fn stamp_incarnation_retaining_database(
    database: Database,
    incarnation: u64,
    private: Option<crate::maintenance::PrivateArchiveValidationBinding>,
) -> Result<Database, StorageError> {
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
        transaction.abort().map_err(precommit_storage_error)?;
        return Ok(database);
    }
    v3_restore::edge("preflight");
    stamp_incarnation_metadata(&transaction, incarnation)?;
    v3_restore::edge("incarnation");
    if let Some(anchor) = anchor {
        anchor.stage(&transaction)?;
    }
    transaction.commit().map_err(commit_error)?;
    v3_restore::edge("committed");
    Ok(database)
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
    stamp_retention_watermark_on_database(&database, watermark_sequence)
}

fn stamp_retention_watermark_on_database(
    database: &Database,
    watermark_sequence: u64,
) -> Result<(), StorageError> {
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
    let target = restored_retention_watermark_target(&database)?;
    drop(database);
    if let Some(target) = target {
        stamp_retention_watermark(path, target)?;
    }
    Ok(())
}

fn apply_restored_retention_watermark_on_database(
    database: &Database,
    manifest_watermark: Option<u64>,
) -> Result<(), StorageError> {
    let Some(watermark) = manifest_watermark.filter(|watermark| *watermark != 0) else {
        return Ok(());
    };
    stamp_retention_watermark_on_database(database, watermark)?;
    if let Some(target) = restored_retention_watermark_target(database)? {
        stamp_retention_watermark_on_database(database, target)?;
    }
    Ok(())
}

fn restored_retention_watermark_target(database: &Database) -> Result<Option<u64>, StorageError> {
    let transaction = database.begin_read().map_err(transaction_error)?;
    let current = crate::retention::load_watermark(&transaction)?
        .map(|w| w.watermark_sequence())
        .unwrap_or(0);
    let holds = crate::retention::load_holds(&transaction)?;
    // Restore discards the derived sidecar (ADR-0240), so no projection
    // frontier exists yet for the restored timeline.
    let fencing = crate::retention::collect_fencing_inputs(&transaction, None, &holds)?;
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

    // Preserve the same fencing minimum and tombstone-chain truth.
    let mut target = current;
    if let Some(max_perm) = max_perm {
        target = target.min(max_perm);
    }
    target = target.max(tombstone_end);
    Ok((target != current).then_some(target))
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
        let sealed_artifacts = stage_sealed_extents(
            &self.source_database,
            staging.path(),
            source_facts.database_id,
        )?;
        let mut checksums = vec![
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
        for (ordinal, path) in sealed_artifact_ordinals(&sealed_artifacts)? {
            checksums.push(BackupArtifactChecksumV1::new(ordinal, sha256_file(&path)?));
        }
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
        self.restore_offline_backup_with_layout(overwrite_policy, RestoredHistoryLayout::Retained)
            .map(|restored| restored.outcome)
    }
}

/// Backend-private representation choice. Ordinary offline restore retains its
/// reviewed extents and mutation order. Only the unpublished staged owner uses
/// the self-contained representation it already requires before validation.
enum RestoredHistoryLayout<'a> {
    Retained,
    SelfContained {
        configured_database_file: &'a Path,
        private_cancellation: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    },
}

enum MaterializationDatabase {
    Ordinary(Database),
    Owned(crate::store::OwnedEngineOpen),
}
impl std::ops::Deref for MaterializationDatabase {
    type Target = Database;
    fn deref(&self) -> &Database {
        match self {
            Self::Ordinary(database) => database,
            Self::Owned(engine) => engine.database(),
        }
    }
}

struct NativeRestoreResult {
    outcome: OfflineRestoreResultV1,
    owned: Option<OwnedMaterialization>,
}

/// Native construction only; complete validation and server join are separate.
/// Release the engine and its three fixed creation pins before outer custody.
pub(crate) struct OwnedMaterialization {
    database: Option<std::sync::Arc<MaterializationDatabase>>,
    primary: MaterializationArtifact,
    journal: MaterializationArtifact,
    marker: MaterializationArtifact,
    custody: crate::maintenance::RestoreCustody,
    validated_seal: Option<OwnedArtifactSeals>,
    manifest: OfflineBackupManifestV1,
}

/// Proof drops its immutable pin and engine before creation pins and custody.
/// It yields only immutable authorization evidence, retaining native custody.
pub(crate) struct OwnedValidatedMaterialization {
    logical: crate::startup::CompleteLogicalValidation,
    materialization: OwnedMaterialization,
}

/// Full content seals and operation-local continuity evidence. Metadata only
/// admits immutable handoffs; every mutation/publication boundary checks bytes.
struct OwnedArtifactSeals {
    primary: BackupIntegrityChecksumV1,
    journal: BackupIntegrityChecksumV1,
    marker: BackupIntegrityChecksumV1,
    continuity: [Option<ArtifactContinuity>; 3],
    mutation_generation: u64,
    sealed: Vec<OwnedExtentSeal>,
}

struct OwnedExtentSeal {
    generation: u64,
    checksum: BackupIntegrityChecksumV1,
    continuity: Option<ArtifactContinuity>,
}

/// Unix descriptor identity and write observations; never a content proof.
#[derive(Clone, Copy, PartialEq, Eq)]
struct ArtifactContinuity {
    device: u64,
    inode: u64,
    length: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl ArtifactContinuity {
    fn read(file: &File) -> Result<Option<Self>, StorageError> {
        let metadata = file.metadata().map_err(io_unavailable)?;
        if !metadata.is_file() {
            return Err(corrupt());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(Some(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
                length: metadata.len(),
                modified: (metadata.mtime(), metadata.mtime_nsec()),
                changed: (metadata.ctime(), metadata.ctime_nsec()),
            }))
        }
        #[cfg(not(unix))]
        Ok(None)
    }
}

impl OwnedArtifactSeals {
    fn capture(owner: &OwnedMaterialization) -> Result<Self, StorageError> {
        let mutation_generation = owner.custody.mutation_generation()?;
        let continuity = owner.artifact_continuity()?;
        let primary = owner.primary.checksum()?;
        let journal = owner.journal.checksum()?;
        let marker = owner.marker.checksum()?;
        let paths = sealed_extents_beside(owner.primary.path())?;
        // Known history hydration consumes backup extents before this proof.
        // Seal the actual post-preparation inventory, not its predecessor's
        // manifest ordinals. Subsequent handoffs must retain this exact set.
        let mut sealed = Vec::with_capacity(paths.len());
        for (generation, path) in &paths {
            let file = owner.open_extent(path)?;
            let before = ArtifactContinuity::read(&file)?;
            let checksum = sha256_reader(&file)?;
            crate::journal::read_sealed_extent_with_media(
                &crate::media::RealJournalMedia,
                path,
                owner.manifest.database_id(),
            )
            .map_err(|_| corrupt())?;
            if ArtifactContinuity::read(&file)? != before {
                return Err(corrupt());
            }
            sealed.push(OwnedExtentSeal {
                generation: *generation,
                checksum,
                continuity: before,
            });
        }
        if owner.artifact_continuity()? != continuity
            || owner.custody.mutation_generation()? != mutation_generation
        {
            return Err(corrupt());
        }
        Ok(Self {
            primary,
            journal,
            marker,
            continuity,
            mutation_generation,
            sealed,
        })
    }

    fn verify(&self, owner: &OwnedMaterialization) -> Result<(), StorageError> {
        if owner.custody.mutation_generation()? != self.mutation_generation {
            return Err(corrupt());
        }
        let actual = owner.artifact_continuity()?;
        for ((expected, actual), (artifact, checksum)) in self.continuity.iter().zip(actual).zip([
            (&owner.primary, &self.primary),
            (&owner.journal, &self.journal),
            (&owner.marker, &self.marker),
        ]) {
            match expected {
                Some(expected) if Some(*expected) != actual => return Err(corrupt()),
                None if artifact.checksum()? != *checksum => return Err(corrupt()),
                _ => {}
            }
        }
        self.verify_extents(owner, false)?;
        if owner.artifact_continuity()? != actual
            || owner.custody.mutation_generation()? != self.mutation_generation
        {
            return Err(corrupt());
        }
        Ok(())
    }

    fn verify_full(&self, owner: &OwnedMaterialization) -> Result<(), StorageError> {
        self.verify(owner)?;
        if owner.primary.checksum()? != self.primary {
            return Err(corrupt());
        }
        self.verify_immutable(owner)?;
        self.verify(owner)
    }

    /// A known primary transition cannot authorize changes to immutable files.
    fn verify_immutable(&self, owner: &OwnedMaterialization) -> Result<(), StorageError> {
        if owner.journal.checksum()? != self.journal || owner.marker.checksum()? != self.marker {
            return Err(corrupt());
        }
        self.verify_extents(owner, true)
    }

    fn verify_extents(&self, owner: &OwnedMaterialization, full: bool) -> Result<(), StorageError> {
        let paths = sealed_extents_beside(owner.primary.path())?;
        if paths.len() != self.sealed.len() {
            return Err(corrupt());
        }
        for ((generation, path), seal) in paths.iter().zip(&self.sealed) {
            if *generation != seal.generation {
                return Err(corrupt());
            }
            let file = owner.open_extent(path)?;
            let before = ArtifactContinuity::read(&file)?;
            if seal.continuity.is_some() && before != seal.continuity {
                return Err(corrupt());
            }
            if (full || seal.continuity.is_none()) && sha256_reader(&file)? != seal.checksum {
                return Err(corrupt());
            }
            if ArtifactContinuity::read(&file)? != before {
                return Err(corrupt());
            }
        }
        Ok(())
    }
}

/// Move-only native restore evidence for the server's ordinary startup joins.
/// Only the immutable-backup materialization owner constructs this value. It
/// carries one immutable reader with its native assurance and no operational ports.
#[doc(hidden)]
pub struct RedbPrivateRestoreAuthorization {
    parts: crate::startup::ValidatedAuthorizationParts,
    inputs: riffdb_storage_api::StartupValidationInputs,
    assurance: PrivateRestoreAssurance,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PrivateRestoreAssurance {
    Complete,
    Bounded,
}

impl RedbPrivateRestoreAuthorization {
    /// Matches the actual validation's sampled values and readable inventories.
    #[must_use]
    pub fn matches_inputs(&self, inputs: &riffdb_storage_api::StartupValidationInputs) -> bool {
        &self.inputs == inputs
    }

    pub(crate) fn archive_snapshot(&self) -> Result<crate::RedbOwnedSnapshot, StorageError> {
        if self.assurance != PrivateRestoreAssurance::Complete {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        self.parts.4.verify_validated_authorization_read()?;
        Ok(self.parts.4.clone())
    }

    /// Consumes the original same-owner proof for the unchanged server join.
    pub fn into_parts(self) -> Result<crate::startup::ValidatedAuthorizationParts, StorageError> {
        self.parts.4.verify_validated_authorization_read()?;
        Ok(self.parts)
    }
}

/// Retains the actual first engine while immutable authorization clones exist.
/// No writer, readiness, publication or source activation capability is exposed.
pub(crate) struct OwnedAuthorizationMaterialization {
    shared: std::sync::Arc<crate::store::SharedRedb>,
    materialization: OwnedMaterialization,
}

/// Exact closed bytes under the original continuous native exclusion.
pub(crate) struct OwnedClosedMaterialization {
    materialization: OwnedMaterialization,
    history_incarnation: u64,
    ordinary_source: bool,
}

impl OwnedValidatedMaterialization {
    pub(crate) fn into_archive_preparation(self) -> Result<OwnedArchivePreparation, StorageError> {
        self.materialization.verify()?;
        let shared = self.logical.retain_native_close_owner()?;
        // Archive conversion is bound to the immutable backup's exact history,
        // not a source-compatibility receipt appended to the private copy.
        validate_copied_database_facts(
            read_database_facts(&shared.database)?,
            &self.materialization.manifest,
        )?;
        drop(self.logical);
        OwnedAuthorizationMaterialization {
            shared,
            materialization: self.materialization,
        }
        .close()?
        .into_archive_preparation()
    }

    #[cfg(test)]
    pub(crate) fn into_server_authorization(
        self,
    ) -> Result<
        (
            OwnedAuthorizationMaterialization,
            RedbPrivateRestoreAuthorization,
        ),
        StorageError,
    > {
        let inputs = self.logical.native_authorization_inputs()?;
        let (owner, parts) = self.into_authorization()?;
        Ok((
            owner,
            RedbPrivateRestoreAuthorization {
                parts,
                inputs,
                assurance: PrivateRestoreAssurance::Complete,
            },
        ))
    }

    #[cfg(test)]
    pub(crate) fn into_authorization(
        self,
    ) -> Result<
        (
            OwnedAuthorizationMaterialization,
            crate::startup::ValidatedAuthorizationParts,
        ),
        StorageError,
    > {
        self.materialization.verify()?;
        let shared = self.logical.retain_native_authorization_owner()?;
        let parts = self.logical.into_authorization_parts()?;
        Ok((
            OwnedAuthorizationMaterialization {
                shared,
                materialization: self.materialization,
            },
            parts,
        ))
    }
}

impl OwnedAuthorizationMaterialization {
    pub(crate) fn verify_authorization(&self) -> Result<(), StorageError> {
        self.materialization.verify()?;
        self.shared.verify_private_restore_integrity()
    }

    pub(crate) fn close(self) -> Result<OwnedClosedMaterialization, StorageError> {
        self.materialization.verify()?;
        self.shared.verify_private_restore_integrity()?;
        // Every authorization clone retains this same SharedRedb. Refuse a
        // surviving read instead of treating backend close as read release.
        if std::sync::Arc::strong_count(&self.shared) != 1 {
            self.materialization.custody.invalidate();
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let ordinary_source = self.shared.is_offline_source_mode();
        let history_incarnation = {
            let read = self
                .shared
                .database
                .begin_read()
                .map_err(transaction_error)?;
            crate::startup::read_retained_metadata_pub(&read)?.history_incarnation()
        };
        let Self {
            shared,
            mut materialization,
        } = self;
        // Bytes checked immediately before the only known engine-close
        // transition. That transition may clean allocator/header state, never
        // substitute an earlier changed primary, journal or marker artifact.
        materialization.verify_full_seal()?;
        shared.verify_private_restore_integrity()?;
        drop(shared);
        materialization.custody.freeze_prepared_artifact()?;
        let prior = materialization
            .validated_seal
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Err(error) = prior.verify_immutable(&materialization) {
            materialization.custody.invalidate();
            return Err(error);
        }
        materialization.validated_seal = Some(OwnedArtifactSeals::capture(&materialization)?);
        materialization.verify()?;
        Ok(OwnedClosedMaterialization {
            materialization,
            history_incarnation,
            ordinary_source,
        })
    }
}

impl OwnedClosedMaterialization {
    /// Consume the closed source proof before the existing archive conversion.
    /// This retains only native custody, never authorization or publication
    /// evidence. Complete validation must follow the known archive writes.
    pub(crate) fn into_archive_preparation(self) -> Result<OwnedArchivePreparation, StorageError> {
        self.materialization.verify_full_seal()?;
        self.materialization
            .custody
            .begin_controlled_closed_mutation()?;
        let OwnedMaterialization {
            custody, primary, ..
        } = self.materialization;
        Ok(OwnedArchivePreparation { custody, primary })
    }

    pub(crate) fn stamp_history_incarnation(
        &mut self,
        incarnation: u64,
    ) -> Result<(), StorageError> {
        self.materialization.verify_full_seal()?;
        if incarnation < HISTORY_INCARNATION_INITIAL {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let result = (|| {
            self.materialization
                .custody
                .begin_controlled_closed_mutation()?;
            let old = self
                .materialization
                .validated_seal
                .take()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let engine = crate::store::OwnedEngineOpen::from_private_restore_backend(
                self.materialization.custody.lease()?,
            )?;
            // Reuse the original transaction-current history preflight, restore
            // anchor/reset and one Immediate two-phase transaction unchanged.
            let database = stamp_incarnation_retaining_database(
                engine.into_database_for_controlled_preparation()?,
                incarnation,
                None,
            )?;
            if self.ordinary_source {
                crate::startup::validate_restore_transition_roots(&database, incarnation)?;
            }
            drop(database);
            self.materialization.custody.freeze_prepared_artifact()?;
            old.verify_immutable(&self.materialization)?;
            self.materialization.validated_seal =
                Some(OwnedArtifactSeals::capture(&self.materialization)?);
            self.materialization.verify()?;
            self.history_incarnation = incarnation;
            Ok(())
        })();
        if result.is_err() {
            self.materialization.custody.invalidate();
        }
        result
    }

    pub(crate) fn verify(&self) -> Result<(), StorageError> {
        self.materialization.verify()
    }
    #[cfg(test)]
    pub(crate) fn verify_publication_seal(&self) -> Result<(), StorageError> {
        self.materialization.verify_full_seal()
    }
    pub(crate) fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
    pub(crate) fn checksums(
        &self,
    ) -> Result<
        (
            BackupIntegrityChecksumV1,
            BackupIntegrityChecksumV1,
            BackupIntegrityChecksumV1,
        ),
        StorageError,
    > {
        self.verify()?;
        let seals = self
            .materialization
            .validated_seal
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        Ok((
            seals.primary.clone(),
            seals.journal.clone(),
            seals.marker.clone(),
        ))
    }
}

/// A verified native materialization after its source proof has been consumed.
/// The archive owner retains the journal/marker identities across their existing
/// controlled removal and quarantine transitions. This value alone can neither
/// authenticate a principal nor seal or publish a restored database.
pub(crate) struct OwnedArchivePreparation {
    custody: crate::maintenance::RestoreCustody,
    primary: MaterializationArtifact,
}

/// Complete follower evidence still awaiting the archive owner's artifact
/// seals and server joins. It grants no public authorization or publication.
pub(crate) struct OwnedArchiveCompleteValidation {
    preparation: OwnedArchivePreparation,
    logical: crate::startup::CompleteLogicalValidation,
}

#[cfg(test)]
impl OwnedArchiveCompleteValidation {
    pub(crate) fn into_parts_for_test(
        self,
    ) -> (
        OwnedArchivePreparation,
        crate::startup::CompleteLogicalValidation,
    ) {
        (self.preparation, self.logical)
    }
}

impl OwnedArchiveCompleteValidation {
    pub(crate) fn close_after_source_validation(self) -> Result<OwnedArchiveClosed, StorageError> {
        self.preparation.verify()?;
        let inputs = self.logical.native_authorization_inputs()?;
        let cancellation = self.logical.native_authorization_cancellation()?;
        let shared = self.logical.retain_native_close_owner()?;
        drop(self.logical);
        OwnedArchiveAuthorization {
            shared,
            preparation: self.preparation,
            inputs,
            cancellation,
        }
        .close()
    }

    pub(crate) fn verify_follower_history(
        &self,
        expected: riffdb_storage_api::ChangelogHistoryStateV3,
    ) -> Result<(), StorageError> {
        self.logical.verify_native_archive_follower(expected)
    }

    pub(crate) fn verify_source_history(
        &self,
        database: DatabaseId,
        incarnation: u64,
        frontier: riffdb_types::DualFrontier,
    ) -> Result<(), StorageError> {
        self.logical
            .verify_native_archive_source(database, incarnation, frontier)
    }

    pub(crate) fn into_server_authorization(
        self,
    ) -> Result<(OwnedArchiveAuthorization, RedbPrivateRestoreAuthorization), StorageError> {
        self.preparation.verify()?;
        let inputs = self.logical.native_authorization_inputs()?;
        let cancellation = self.logical.native_authorization_cancellation()?;
        let shared = self.logical.retain_native_authorization_owner()?;
        let parts = self.logical.into_authorization_parts()?;
        Ok((
            OwnedArchiveAuthorization {
                shared,
                preparation: self.preparation,
                inputs: inputs.clone(),
                cancellation,
            },
            RedbPrivateRestoreAuthorization {
                parts,
                inputs,
                assurance: PrivateRestoreAssurance::Complete,
            },
        ))
    }
}

pub(crate) struct OwnedArchiveAuthorization {
    shared: std::sync::Arc<crate::store::SharedRedb>,
    preparation: OwnedArchivePreparation,
    inputs: riffdb_storage_api::StartupValidationInputs,
    cancellation: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

pub(crate) struct OwnedArchiveClosed {
    preparation: OwnedArchivePreparation,
    primary_checksum: BackupIntegrityChecksumV1,
}

impl OwnedArchiveAuthorization {
    pub(crate) fn invalidate(&self) {
        self.preparation.custody.invalidate();
    }
    pub(crate) fn matches_inputs(
        &self,
        inputs: &riffdb_storage_api::StartupValidationInputs,
    ) -> bool {
        &self.inputs == inputs
    }
    pub(crate) fn verify(&self) -> Result<(), StorageError> {
        if self.cancellation.load(std::sync::atomic::Ordering::Acquire) {
            self.preparation.custody.invalidate();
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        self.preparation.verify()?;
        self.shared.verify_private_restore_integrity()
    }
    pub(crate) fn close(self) -> Result<OwnedArchiveClosed, StorageError> {
        self.verify()?;
        if std::sync::Arc::strong_count(&self.shared) != 1 {
            self.preparation.custody.invalidate();
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        drop(self.shared);
        self.preparation.custody.freeze_prepared_artifact()?;
        let primary_checksum = self.preparation.primary.checksum()?;
        let closed = OwnedArchiveClosed {
            preparation: self.preparation,
            primary_checksum,
        };
        closed.verify()?;
        Ok(closed)
    }
}

impl OwnedArchiveClosed {
    pub(crate) fn invalidate(&self) {
        self.preparation.custody.invalidate();
    }
    pub(crate) fn verify(&self) -> Result<(), StorageError> {
        let result = (|| {
            self.preparation.verify()?;
            if self.preparation.primary.checksum()? != self.primary_checksum {
                return Err(corrupt());
            }
            Ok(())
        })();
        if result.is_err() {
            self.invalidate();
        }
        result
    }
    pub(crate) fn into_preparation(self) -> Result<OwnedArchivePreparation, StorageError> {
        self.verify()?;
        self.preparation
            .custody
            .begin_controlled_closed_mutation()?;
        Ok(self.preparation)
    }
    pub(crate) fn stamp_incarnation(
        &mut self,
        incarnation: u64,
        private: Option<crate::maintenance::PrivateArchiveValidationBinding>,
    ) -> Result<(), StorageError> {
        self.verify()?;
        let result = (|| {
            self.preparation
                .custody
                .begin_controlled_closed_mutation()?;
            let engine = crate::store::OwnedEngineOpen::from_private_restore_backend(
                self.preparation.custody.lease()?,
            )?;
            stamp_incarnation(
                engine.into_database_for_controlled_preparation()?,
                incarnation,
                private,
            )?;
            self.preparation.custody.freeze_prepared_artifact()?;
            self.primary_checksum = self.preparation.primary.checksum()?;
            self.verify()
        })();
        if result.is_err() {
            self.preparation.custody.invalidate();
        }
        result
    }
}

impl OwnedArchivePreparation {
    pub(crate) fn invalidate(&self) {
        self.custody.invalidate();
    }

    pub(crate) fn validate_complete_follower(
        self,
        path: &Path,
        inputs: riffdb_storage_api::StartupValidationInputs,
        cancellation: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<OwnedArchiveCompleteValidation, StorageError> {
        self.verify()?;
        if path != self.primary.path() {
            self.custody.invalidate();
            return Err(corrupt());
        }
        if cancellation.load(std::sync::atomic::Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let engine =
            crate::store::OwnedEngineOpen::from_private_restore_backend(self.custody.lease()?)?;
        let store = crate::RedbStore::from_owned_archive_integrity_engine(path, engine)?;
        let logical = crate::RedbOfflineIntegrityScrub::validate_owned_archive_follower(
            store,
            inputs,
            cancellation,
        )?;
        self.verify()?;
        Ok(OwnedArchiveCompleteValidation {
            preparation: self,
            logical,
        })
    }

    pub(crate) fn validate_complete_private(
        self,
        path: &Path,
        binding: crate::maintenance::PrivateArchiveValidationBinding,
        inputs: riffdb_storage_api::StartupValidationInputs,
        cancellation: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<OwnedArchiveCompleteValidation, StorageError> {
        self.verify()?;
        if path != self.primary.path() {
            self.custody.invalidate();
            return Err(corrupt());
        }
        if cancellation.load(std::sync::atomic::Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let engine =
            crate::store::OwnedEngineOpen::from_private_restore_backend(self.custody.lease()?)?;
        let store =
            crate::RedbStore::from_owned_private_archive_integrity_engine(path, engine, binding)?;
        let logical = crate::RedbOfflineIntegrityScrub::validate_owned_archive_private(
            store,
            binding,
            inputs,
            cancellation,
        )?;
        self.verify()?;
        Ok(OwnedArchiveCompleteValidation {
            preparation: self,
            logical,
        })
    }

    pub(crate) fn validate_complete_source(
        self,
        path: &Path,
        inputs: riffdb_storage_api::StartupValidationInputs,
        cancellation: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<OwnedArchiveCompleteValidation, StorageError> {
        self.verify()?;
        if path != self.primary.path() {
            self.custody.invalidate();
            return Err(corrupt());
        }
        if cancellation.load(std::sync::atomic::Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let engine =
            crate::store::OwnedEngineOpen::from_private_restore_backend(self.custody.lease()?)?;
        let store = crate::RedbStore::from_owned_archive_source_integrity_engine(path, engine)?;
        let logical = crate::RedbOfflineIntegrityScrub::validate_owned_archive_source(
            store,
            inputs,
            cancellation,
        )?;
        self.verify()?;
        Ok(OwnedArchiveCompleteValidation {
            preparation: self,
            logical,
        })
    }

    pub(crate) fn open_follower_store(
        &self,
        path: &Path,
    ) -> Result<crate::RedbFollowerStore, StorageError> {
        self.verify()?;
        if path != self.primary.path() {
            self.custody.invalidate();
            return Err(corrupt());
        }
        let result = self.custody.lease().and_then(|backend| {
            crate::RedbFollowerStore::from_owned_archive_preparation(path, backend)
        });
        if result.is_err() {
            self.custody.invalidate();
        }
        result
    }

    pub(crate) fn verify(&self) -> Result<(), StorageError> {
        let result = self
            .custody
            .verify_identity()
            .and_then(|()| self.primary.verify());
        if result.is_err() {
            self.custody.invalidate();
        }
        result
    }

    pub(crate) fn open_preparation_database(&self) -> Result<redb::Database, StorageError> {
        self.verify()?;
        let result = (|| {
            crate::store::OwnedEngineOpen::from_private_restore_backend(self.custody.lease()?)?
                .into_database_for_controlled_preparation()
        })();
        if result.is_err() {
            self.custody.invalidate();
        }
        result
    }
}

/// A move-only preparation read retains the original Database owner as well
/// as its actual read transaction. redb backend close alone does not establish
/// that every ReadTransaction has gone away. Drop the read before its owner.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "owned restore validation integration is pending")
)]
struct PreparedMaterializationRead {
    read: redb::ReadTransaction,
    _engine: std::sync::Arc<MaterializationDatabase>,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "owned restore validation integration is pending")
)]
impl OwnedMaterialization {
    fn verify(&self) -> Result<(), StorageError> {
        let result = (|| {
            self.custody.verify_identity()?;
            self.primary.verify()?;
            self.journal.verify()?;
            self.marker.verify()?;
            if let Some(seal) = &self.validated_seal {
                seal.verify(self)?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.custody.invalidate();
        }
        result
    }

    fn artifact_continuity(&self) -> Result<[Option<ArtifactContinuity>; 3], StorageError> {
        Ok([
            ArtifactContinuity::read(self.primary.private_file()?)?,
            ArtifactContinuity::read(self.journal.private_file()?)?,
            ArtifactContinuity::read(self.marker.private_file()?)?,
        ])
    }

    fn open_extent(&self, path: &Path) -> Result<File, StorageError> {
        let (parent, _) = self
            .primary
            .identity
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        parent.verify()?;
        let name = path.file_name().ok_or_else(corrupt)?;
        let file = parent.open_file(name)?.into_std();
        if !parent.regular_file_matches(name, &file)? {
            return Err(corrupt());
        }
        parent.verify()?;
        Ok(file)
    }

    fn verify_full_seal(&self) -> Result<(), StorageError> {
        let result = (|| {
            self.verify()?;
            self.validated_seal
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .verify_full(self)
        })();
        if result.is_err() {
            self.custody.invalidate();
        }
        result
    }

    fn pin_prepared_read(&mut self) -> Result<PreparedMaterializationRead, StorageError> {
        self.verify()?;
        let engine = self
            .database
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if std::sync::Arc::strong_count(engine) != 1 {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let read = engine.begin_read().map_err(transaction_error)?;
        Ok(PreparedMaterializationRead {
            read,
            _engine: std::sync::Arc::clone(engine),
        })
    }

    /// Complete original backup-image verification, without appending source
    /// compatibility transactions before archive replay. Ordinary restore keeps
    /// its existing required compatibility preparation in validate_complete.
    pub(crate) fn validate_archive_source(
        mut self,
        inputs: riffdb_storage_api::StartupValidationInputs,
    ) -> Result<OwnedValidatedMaterialization, StorageError> {
        self.verify()?;
        let engine = self
            .database
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let engine = std::sync::Arc::try_unwrap(engine)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let MaterializationDatabase::Owned(mut engine) = engine else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        engine.check_owned_restore_integrity()?;
        let store = crate::RedbStore::from_owned_restore_engine(
            self.primary.path(),
            engine,
            &self.manifest,
        )?;
        let logical = crate::RedbOfflineIntegrityScrub::validate_owned_archive_image(
            store,
            inputs,
            self.custody.cancellation(),
        )?;
        self.verify()?;
        self.validated_seal = Some(OwnedArtifactSeals::capture(&self)?);
        self.verify()?;
        Ok(OwnedValidatedMaterialization {
            logical,
            materialization: self,
        })
    }

    /// Ordinary self-contained restore readiness. Legacy compatibility and
    /// promotion retain their existing complete proof before authorization.
    pub(crate) fn validate_readiness(
        mut self,
        inputs: riffdb_storage_api::StartupValidationInputs,
    ) -> Result<
        (
            OwnedAuthorizationMaterialization,
            RedbPrivateRestoreAuthorization,
        ),
        StorageError,
    > {
        self.verify()?;
        let engine = self
            .database
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let engine = std::sync::Arc::try_unwrap(engine)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let MaterializationDatabase::Owned(engine) = engine else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        let mut store = crate::RedbStore::from_owned_restore_engine(
            self.primary.path(),
            engine,
            &self.manifest,
        )?;
        store.prepare_owned_restore_before_validation()?;
        self.verify()?;
        let (shared, parts, assurance) =
            if crate::startup::private_restore_supports_bounded_readiness(&store)? {
                let (shared, parts) = crate::startup::validate_private_restore_readiness(
                    store,
                    inputs.clone(),
                    self.custody.cancellation(),
                )?;
                (shared, parts, PrivateRestoreAssurance::Bounded)
            } else {
                let logical = crate::RedbOfflineIntegrityScrub::validate_owned_restore(
                    store,
                    inputs.clone(),
                    self.custody.cancellation(),
                )?;
                let shared = logical.retain_native_authorization_owner()?;
                let parts = logical.into_authorization_parts()?;
                (shared, parts, PrivateRestoreAssurance::Complete)
            };
        self.verify()?;
        self.validated_seal = Some(OwnedArtifactSeals::capture(&self)?);
        self.verify()?;
        Ok((
            OwnedAuthorizationMaterialization {
                shared,
                materialization: self,
            },
            RedbPrivateRestoreAuthorization {
                parts,
                inputs,
                assurance,
            },
        ))
    }

    /// Complete verification on the same first engine. This internal result
    /// still requires compatibility preparation and the server startup join.
    pub(crate) fn validate_complete(
        mut self,
        inputs: riffdb_storage_api::StartupValidationInputs,
    ) -> Result<OwnedValidatedMaterialization, StorageError> {
        self.verify()?;
        let engine = self
            .database
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let engine = std::sync::Arc::try_unwrap(engine)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let MaterializationDatabase::Owned(engine) = engine else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        // No read or savepoint survives preparation admission. The engine is
        // unique here; its original observer and repair latch travel with it.
        self.verify()?;
        #[cfg(test)]
        let observer = engine.checkpoint_observer();
        let mut store = crate::RedbStore::from_owned_restore_engine(
            self.primary.path(),
            engine,
            &self.manifest,
        )?;
        store.prepare_owned_restore_before_validation()?;
        self.verify()?;
        #[cfg(test)]
        assert!(
            std::sync::Arc::ptr_eq(&observer, &store.shared.checkpoint_rewrite),
            "the first engine's actual checkpoint observer survives the store join"
        );
        let logical = crate::RedbOfflineIntegrityScrub::validate_owned_restore(
            store,
            inputs,
            self.custody.cancellation(),
        )?;
        self.verify()?;
        self.validated_seal = Some(OwnedArtifactSeals::capture(&self)?);
        self.verify()?;
        Ok(OwnedValidatedMaterialization {
            logical,
            materialization: self,
        })
    }

    fn close_for_validation(&mut self) -> Result<u64, StorageError> {
        self.verify()?;
        if let Some(engine) = self.database.take() {
            match std::sync::Arc::try_unwrap(engine) {
                Ok(database) => drop(database),
                Err(engine) => {
                    self.database = Some(engine);
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
            }
        }
        let length = self.custody.freeze_prepared_artifact()?;
        self.verify()?;
        Ok(length)
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MaterializationTestBoundary {
    BeforeHydration,
    BeforeWatermark,
}

#[cfg(test)]
struct MaterializationTestBarrier {
    reached: std::sync::mpsc::SyncSender<MaterializationTestBoundary>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
std::thread_local! {
    static MATERIALIZATION_CHECKSUM_PASSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static MATERIALIZATION_TEST_BARRIER: std::cell::RefCell<Option<MaterializationTestBarrier>> = const {
        std::cell::RefCell::new(None)
    };
}

#[cfg(test)]
fn materialization_test_boundary(
    boundary: MaterializationTestBoundary,
) -> Result<(), StorageError> {
    MATERIALIZATION_TEST_BARRIER.with(|slot| {
        let barrier = slot.borrow_mut().take();
        if let Some(barrier) = barrier {
            barrier
                .reached
                .send(boundary)
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
            barrier
                .release
                .recv_timeout(std::time::Duration::from_secs(120))
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
            if boundary == MaterializationTestBoundary::BeforeHydration {
                *slot.borrow_mut() = Some(barrier);
            }
        }
        Ok(())
    })
}

impl RedbOfflineRestore {
    pub(crate) fn restore_staged_backup(
        &mut self,
        configured_database_file: &Path,
    ) -> Result<OfflineRestoreResultV1, StorageError> {
        self.restore_offline_backup_with_layout(
            OfflineRestoreOverwritePolicyV1::RefuseNonEmpty,
            RestoredHistoryLayout::SelfContained {
                configured_database_file,
                private_cancellation: None,
            },
        )
        .map(|restored| restored.outcome)
    }

    pub(crate) fn restore_owned_staged_backup(
        &mut self,
        configured_database_file: &Path,
        cancellation: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(OfflineBackupManifestV1, OwnedMaterialization), StorageError> {
        let restored = self.restore_offline_backup_with_layout(
            OfflineRestoreOverwritePolicyV1::RefuseNonEmpty,
            RestoredHistoryLayout::SelfContained {
                configured_database_file,
                private_cancellation: Some(cancellation),
            },
        )?;
        let OfflineRestoreResultV1::Restored { manifest } = restored.outcome else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        let owned = restored
            .owned
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        owned.verify()?;
        Ok((*manifest, owned))
    }

    fn restore_offline_backup_with_layout(
        &mut self,
        overwrite_policy: OfflineRestoreOverwritePolicyV1,
        layout: RestoredHistoryLayout<'_>,
    ) -> Result<NativeRestoreResult, StorageError> {
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
            return Ok(NativeRestoreResult {
                outcome: OfflineRestoreResultV1::TargetNotEmpty,
                owned: None,
            });
        }
        if target_state == TargetDirectoryState::Missing {
            fs::create_dir_all(&self.target_directory).map_err(io_unavailable)?;
        }

        let target_artifact = self.target_directory.join(DATABASE_ARTIFACT_FILE_NAME);
        let target_journal = crate::journal::journal_path(&target_artifact);
        let target_format_marker = crate::durable_format_marker_path(&target_artifact);
        let target_lock = lock_existing_target(&target_artifact)?;
        let private = matches!(&layout, RestoredHistoryLayout::SelfContained { .. });
        let mut staged_artifact = MaterializationArtifact::create(
            &self.target_directory,
            OsStr::new(DATABASE_ARTIFACT_FILE_NAME),
            private,
        )?;
        let mut staged_journal = MaterializationArtifact::create(
            &self.target_directory,
            OsStr::new(JOURNAL_ARTIFACT_FILE_NAME),
            private,
        )?;
        let mut staged_format_marker = MaterializationArtifact::create(
            &self.target_directory,
            OsStr::new(FORMAT_MARKER_ARTIFACT_FILE_NAME),
            private,
        )?;
        staged_artifact.copy_from(&self.backup_directory.join(DATABASE_ARTIFACT_FILE_NAME))?;
        let staged_lock = match &layout {
            RestoredHistoryLayout::Retained => {
                Some(validate_artifact(staged_artifact.path(), &manifest)?)
            }
            RestoredHistoryLayout::SelfContained { .. } => {
                // A native private stage cannot recover/repair its first copy
                // and then present the repaired bytes as a clean validation.
                // Authenticate every artifact before its first engine open.
                if staged_artifact.checksum()? != manifest_checksum(&manifest)? {
                    return Err(corrupt());
                }
                None
            }
        };
        match manifest_journal_checksum(&manifest)? {
            Some(expected) => {
                staged_journal
                    .copy_from(&self.backup_directory.join(JOURNAL_ARTIFACT_FILE_NAME))?;
                if staged_journal.checksum()? != expected {
                    return Err(corrupt());
                }
            }
            None => {
                staged_journal.create_legacy_journal(
                    &crate::journal::JournalFileHeader::with_frontiers(
                        manifest.database_id(),
                        manifest.last_commit_sequence(),
                        None,
                        [0; 32],
                    ),
                )?;
            }
        }
        validate_backup_journal(
            staged_journal.path(),
            manifest.database_id(),
            manifest.last_commit_sequence(),
        )?;
        let expected_format_marker =
            manifest_format_marker_checksum(&manifest)?.ok_or_else(corrupt)?;
        staged_format_marker
            .copy_from(&self.backup_directory.join(FORMAT_MARKER_ARTIFACT_FILE_NAME))?;
        if staged_format_marker.checksum()? != expected_format_marker {
            return Err(corrupt());
        }
        validate_manifest_format_marker(staged_format_marker.path(), &manifest)?;
        let staged_sealed = stage_restored_sealed_extents(
            &self.backup_directory,
            &self.target_directory,
            &manifest,
        )?;
        // Only this native manifest-verified copy path can mint the input to
        // production custody. It retains the original creation descriptor and
        // directory capability, rather than discovering them from a pathname.
        let custody = if let RestoredHistoryLayout::SelfContained {
            private_cancellation: Some(cancellation),
            ..
        } = &layout
        {
            staged_artifact.verify()?;
            staged_journal.verify()?;
            staged_format_marker.verify()?;
            let verified = staged_artifact.verified_primary()?;
            Some(crate::maintenance::RestoreCustody::from_verified_copy(
                verified,
                std::sync::Arc::clone(cancellation),
            )?)
        } else {
            None
        };
        let staged_lock = match staged_lock {
            Some(database) => MaterializationDatabase::Ordinary(database),
            None => {
                let database = match &custody {
                    Some(custody) => MaterializationDatabase::Owned(
                        crate::store::OwnedEngineOpen::from_private_restore_backend(
                            custody.lease()?,
                        )?,
                    ),
                    None => {
                        MaterializationDatabase::Ordinary(open_private_materialization_database(
                            &staged_artifact,
                            &staged_journal,
                            &staged_format_marker,
                        )?)
                    }
                };
                staged_artifact.verify()?;
                staged_journal.verify()?;
                staged_format_marker.verify()?;
                validate_copied_database_facts(read_database_facts(&*database)?, &manifest)?;
                database
            }
        };

        if let Some(controller) = &self.test_controller {
            controller.before_commit(RedbTestOperation::Restore)?;
        }
        match fs::remove_file(&target_format_marker) {
            Ok(()) => sync_directory(&self.target_directory)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_unavailable(error)),
        }
        if let Some(custody) = &custody {
            staged_artifact.rename_under_custody(&target_artifact, custody)?;
        } else {
            staged_artifact.rename_to(&target_artifact)?;
        }
        staged_journal.rename_to(&target_journal)?;
        // Sealed extents of whatever the target held before never survive, and
        // the backup's own become the restored database's (ADR-0263 A5).
        for (_, stale) in sealed_extents_beside(&target_artifact)? {
            fs::remove_file(stale).map_err(io_unavailable)?;
        }
        for (generation, mut staged) in staged_sealed {
            fs::rename(
                staged.path(),
                crate::journal::sealed_journal_path(&target_artifact, generation),
            )
            .map_err(io_unavailable)?;
            staged.mark_published();
        }
        staged_format_marker.rename_to(&target_format_marker)?;
        if sync_directory(&self.target_directory).is_err() {
            return Err(unknown());
        }
        drop(target_lock);
        // Ordinary offline restore retains its reviewed close/commit boundary.
        // A private materialization keeps its first native owner until both
        // hydration and watermark preparation have completed.
        let private_database = match &layout {
            RestoredHistoryLayout::Retained => {
                drop(staged_lock);
                None
            }
            RestoredHistoryLayout::SelfContained { .. } => Some(staged_lock),
        };
        if let Some(controller) = &self.test_controller {
            controller.after_commit(RedbTestOperation::Restore)?;
        }
        match (&layout, private_database.as_ref()) {
            (
                RestoredHistoryLayout::SelfContained {
                    configured_database_file,
                    ..
                },
                Some(database),
            ) => {
                #[cfg(test)]
                materialization_test_boundary(MaterializationTestBoundary::BeforeHydration)?;
                hydrate_staged_history_on_database(
                    database,
                    &self.target_directory,
                    manifest.database_id(),
                    configured_database_file,
                )?;
                #[cfg(test)]
                materialization_test_boundary(MaterializationTestBoundary::BeforeWatermark)?;
            }
            (RestoredHistoryLayout::Retained, None) => {}
            _ => return Err(storage_error(StorageErrorKind::InvariantViolation)),
        }
        // The restored copy's own verified extents remain available through
        // every retained-history stamp and fencing read.
        let _history_resolver = crate::history_stub::HistoryResolver::register(
            manifest.database_id(),
            &target_artifact,
            std::sync::Arc::new(crate::media::RealJournalMedia),
        );
        match private_database.as_ref() {
            Some(database) => apply_restored_retention_watermark_on_database(
                database,
                manifest.retention_watermark_sequence(),
            )?,
            None => apply_restored_retention_watermark(
                &target_artifact,
                manifest.retention_watermark_sequence(),
            )?,
        }
        let owned = match custody {
            Some(custody) => {
                let owned = OwnedMaterialization {
                    database: private_database.map(std::sync::Arc::new),
                    primary: staged_artifact,
                    journal: staged_journal,
                    marker: staged_format_marker,
                    custody,
                    validated_seal: None,
                    manifest: manifest.clone(),
                };
                owned.verify()?;
                Some(owned)
            }
            None => {
                drop(private_database);
                None
            }
        };
        Ok(NativeRestoreResult {
            outcome: OfflineRestoreResultV1::Restored {
                manifest: Box::new(manifest),
            },
            owned,
        })
    }
}

/// A repaired unpublished stage must never acquire publication authority.
/// Observe refusal before mapping the engine's RepairAborted error, since a
/// later clean open cannot prove that an earlier open did not repair the file.
fn open_unrepaired_private_database(path: &Path) -> Result<Database, StorageError> {
    open_unrepaired_private_location(
        PrivateDatabaseLocation::Path(path),
        Some(crate::store::PRIMARY_REDB_CACHE_SIZE_BYTES),
    )
}

/// Native descriptor-bound private opens share the same repair refusal while
/// preserving each existing caller's cache ceiling and redacted native errors.
pub(crate) fn open_unrepaired_private_file(
    file: &File,
    cache_bytes: Option<usize>,
) -> Result<Database, StorageError> {
    open_unrepaired_private_location(PrivateDatabaseLocation::File(file), cache_bytes)
}

#[cfg(test)]
fn open_unrepaired_private_backend(
    backend: Box<dyn redb::StorageBackend>,
    cache_bytes: Option<usize>,
) -> Result<Database, StorageError> {
    open_unrepaired_private_location(PrivateDatabaseLocation::Backend(backend), cache_bytes)
}

enum PrivateDatabaseLocation<'a> {
    Path(&'a Path),
    File(&'a File),
    #[cfg(test)]
    Backend(Box<dyn redb::StorageBackend>),
}

fn open_unrepaired_private_location(
    location: PrivateDatabaseLocation<'_>,
    cache_bytes: Option<usize>,
) -> Result<Database, StorageError> {
    let repair_observed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = std::sync::Arc::clone(&repair_observed);
    let mut builder = redb::Builder::new();
    if let Some(cache_bytes) = cache_bytes {
        builder.set_cache_size(cache_bytes);
    }
    builder.set_repair_callback(move |session| {
        observed.store(true, std::sync::atomic::Ordering::Release);
        session.abort();
    });
    let opened = match location {
        PrivateDatabaseLocation::Path(path) => builder.open(path),
        PrivateDatabaseLocation::File(file) => {
            builder.create_file(file.try_clone().map_err(io_unavailable)?)
        }
        #[cfg(test)]
        PrivateDatabaseLocation::Backend(backend) => {
            builder.create_with_backend(crate::media::DynStorageBackend(backend))
        }
    };
    if repair_observed.load(std::sync::atomic::Ordering::Acquire) {
        return Err(corrupt());
    }
    opened.map_err(database_error)
}

/// Expands the staged copy's history stubs into full rows, resolving them from
/// the sealed extents the restore placed beside it, then removes those extents:
/// the published restore carries only its database, journal, and marker
/// (ADR-0263 Amendment 1). Its journal is re-issued above every extent beside
/// the configured database, which it replaces, so no later seal collides.
#[cfg(test)]
fn hydrate_staged_history(
    staged_directory: &Path,
    database_id: DatabaseId,
    configured_database_file: &Path,
) -> Result<(), StorageError> {
    let database_file = staged_directory.join(DATABASE_ARTIFACT_FILE_NAME);
    let database = open_unrepaired_private_database(&database_file)?;
    hydrate_staged_history_on_database(
        &database,
        staged_directory,
        database_id,
        configured_database_file,
    )
}

fn hydrate_staged_history_on_database(
    database: &Database,
    staged_directory: &Path,
    database_id: DatabaseId,
    configured_database_file: &Path,
) -> Result<(), StorageError> {
    let database_file = staged_directory.join(DATABASE_ARTIFACT_FILE_NAME);
    let _resolver = crate::history_stub::HistoryResolver::register(
        database_id,
        &database_file,
        std::sync::Arc::new(crate::media::RealJournalMedia),
    );
    crate::history_stub::hydrate_history_stubs(database)?;
    crate::backup::remove_sealed_extents_beside(&database_file)?;
    let floor = crate::backup::highest_extent_generation_beside(configured_database_file)?;
    crate::journal::renew_extent_above_with_media(
        &crate::media::RealJournalMedia,
        &crate::journal::journal_path(&database_file),
        floor,
    )
    .map_err(crate::backup::backup_journal_error)?;
    crate::backup::sync_parent(&database_file)
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
    validate_copied_database_facts(facts, manifest)?;
    Ok(database)
}

fn validate_copied_database_facts(
    facts: DatabaseFacts,
    manifest: &OfflineBackupManifestV1,
) -> Result<(), StorageError> {
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
    Ok(())
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
    riffdb_storage_api::preflight_durable_format_marker(marker).map_err(|_| incompatible())?;
    Ok(())
}

/// Reads the bounded canonical manifest binding, not artifact content evidence.
/// Only a receipt that already authorizes publication may use this for reopen.
pub(crate) fn read_backup_manifest_binding(
    directory: &Path,
) -> Result<(OfflineBackupManifestV1, OfflineBackupManifestIdentityV1), StorageError> {
    validate_backup_inventory(directory)?;
    let manifest_bytes =
        read_bounded_file(&directory.join(MANIFEST_FILE_NAME), MANIFEST_MAX_BYTES)?;
    let manifest = decode_manifest(&manifest_bytes)?;
    validate_manifest_inventory(directory, &manifest)?;
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

pub(crate) fn validate_immutable_backup(
    directory: &Path,
) -> Result<(OfflineBackupManifestV1, OfflineBackupManifestIdentityV1), StorageError> {
    let (manifest, identity) = read_backup_manifest_binding(directory)?;
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
    create_compatible_backup_fixture(source_database, backup_directory, build, None)
}

#[cfg(test)]
pub(crate) fn create_compatible_backup_fixture(
    source_database: &Path,
    backup_directory: &Path,
    build: &BackupBuildMetadataV1,
    marker: Option<riffdb_storage_api::DurableFormatMarker>,
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
    let mut checksums = vec![
        BackupArtifactChecksumV1::new(NonZeroU32::MIN, sha256_file(&artifact)?),
        BackupArtifactChecksumV1::new(
            NonZeroU32::new(2).expect("journal artifact ordinal is nonzero"),
            sha256_file(&journal)?,
        ),
    ];
    let (manifest, encoded) = if let Some(marker) = marker {
        let marker_artifact = backup_directory.join(FORMAT_MARKER_ARTIFACT_FILE_NAME);
        write_new_and_sync(
            &marker_artifact,
            &riffdb_storage_api::encode_durable_format_marker(marker),
        )?;
        checksums.push(BackupArtifactChecksumV1::new(
            NonZeroU32::new(3).expect("marker artifact ordinal"),
            sha256_file(&marker_artifact)?,
        ));
        let range = BackupFormatCompatibilityV1::new(
            marker.identity(),
            marker.identity(),
            marker.compatibility_fixture_digest(),
        )
        .map_err(value_error)?;
        let manifest =
            facts.into_manifest_with_format_compatibility(checksums, build.clone(), range)?;
        let encoded = encode_manifest(&manifest)?;
        (manifest, encoded)
    } else {
        let manifest = facts.into_pre_format_compatibility_manifest(checksums, build.clone())?;
        let encoded = encode_manifest_pre_format_compatibility(&manifest)?;
        (manifest, encoded)
    };
    write_new_and_sync(&backup_directory.join(MANIFEST_FILE_NAME), &encoded)?;
    sync_directory(backup_directory)?;
    Ok(manifest)
}

/// Final semantic reread may not repair the already validated private stage.
pub(crate) fn validate_sealed_database_semantics(
    artifact: &Path,
    manifest: &OfflineBackupManifestV1,
) -> Result<Database, StorageError> {
    let _ = manifest_checksum(manifest)?;
    let database = open_unrepaired_private_database(artifact)?;
    validate_readable_database_semantics(&database, manifest)?;
    Ok(database)
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

pub(crate) fn manifest_artifact_checksum(
    manifest: &OfflineBackupManifestV1,
    ordinal: NonZeroU32,
) -> Result<BackupIntegrityChecksumV1, StorageError> {
    if !(1..=riffdb_storage_api::MAX_BACKUP_ARTIFACT_CHECKSUMS)
        .contains(&manifest.checksums().len())
    {
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
    if !(1..=riffdb_storage_api::MAX_BACKUP_ARTIFACT_CHECKSUMS)
        .contains(&manifest.checksums().len())
    {
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
    // Three fixed artifacts, then one per retained sealed extent (ADR-0263 A5).
    if !(1..=riffdb_storage_api::MAX_BACKUP_ARTIFACT_CHECKSUMS).contains(&checksum_count) {
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
    let mut sealed = 0_usize;
    for entry in fs::read_dir(directory).map_err(io_unavailable)? {
        let entry = entry.map_err(io_unavailable)?;
        if sealed_artifact_generation(&entry.file_name()).is_some() {
            let metadata = fs::symlink_metadata(entry.path()).map_err(io_unavailable)?;
            sealed += 1;
            if !metadata.file_type().is_file() || sealed > MAX_SEALED_ARTIFACTS {
                return Err(corrupt());
            }
            continue;
        }
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
    // Sealed extents exist only beside the complete current artifact set.
    if sealed != 0 && names != current {
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
    // Every sealed artifact has exactly one checksum, at the ordinal its
    // generation order gives it, and no checksum names a missing extent.
    let sealed = sealed_artifacts_in(directory)?;
    let expected = sealed_artifact_ordinals(&sealed)?
        .into_iter()
        .map(|(ordinal, _)| ordinal)
        .collect::<Vec<_>>();
    let recorded = manifest
        .checksums()
        .iter()
        .map(BackupArtifactChecksumV1::artifact_ordinal)
        .filter(|ordinal| ordinal.get() >= FIRST_SEALED_ARTIFACT_ORDINAL)
        .collect::<Vec<_>>();
    if expected != recorded {
        return Err(corrupt());
    }
    Ok(())
}

/// The backup artifacts that can hold the extent of `generation`: its sealed
/// artifact, then the copied active journal.
pub(crate) fn extent_artifacts(directory: &Path, generation: u64) -> [PathBuf; 2] {
    [
        directory.join(sealed_artifact_name(generation)),
        directory.join(JOURNAL_ARTIFACT_FILE_NAME),
    ]
}

fn sealed_artifact_name(generation: u64) -> String {
    format!("{SEALED_ARTIFACT_PREFIX}{generation:020}{SEALED_ARTIFACT_SUFFIX}")
}

fn sealed_artifact_generation(name: &OsStr) -> Option<u64> {
    let digits = name
        .to_str()?
        .strip_prefix(SEALED_ARTIFACT_PREFIX)?
        .strip_suffix(SEALED_ARTIFACT_SUFFIX)?;
    (digits.len() == 20 && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
        .filter(|generation| {
            sealed_artifact_name(*generation).as_str() == name.to_str().unwrap_or("")
        })
}

/// Sealed backup artifacts in `directory`, in generation order.
/// The sealed extent artifact names of a backup directory, in artifact
/// ordinal order from the first sealed ordinal.
pub(crate) fn sealed_artifact_names(
    directory: &Path,
) -> Result<Vec<std::ffi::OsString>, StorageError> {
    sealed_artifacts_in(directory)?
        .into_iter()
        .map(|(_, path)| {
            path.file_name()
                .map(OsStr::to_os_string)
                .ok_or_else(corrupt)
        })
        .collect()
}

fn sealed_artifacts_in(directory: &Path) -> Result<Vec<(u64, PathBuf)>, StorageError> {
    let mut sealed = Vec::new();
    for entry in fs::read_dir(directory).map_err(io_unavailable)? {
        let entry = entry.map_err(io_unavailable)?;
        if let Some(generation) = sealed_artifact_generation(&entry.file_name()) {
            if sealed.len() == MAX_SEALED_ARTIFACTS {
                return Err(corrupt());
            }
            sealed.push((generation, entry.path()));
        }
    }
    sealed.sort_unstable();
    Ok(sealed)
}

/// Removes every sealed journal extent beside `database`, once nothing in it
/// names them (a hydrated staged restore), and syncs the directory.
pub(crate) fn remove_sealed_extents_beside(database: &Path) -> Result<(), StorageError> {
    let sealed = sealed_extents_beside(database)?;
    if sealed.is_empty() {
        return Ok(());
    }
    for (_, path) in sealed {
        fs::remove_file(&path).map_err(io_unavailable)?;
    }
    sync_parent(database)
}

/// The highest generation of any journal extent beside `database`: sealed,
/// active, checkpoint, sealing, or spare. Zero when there is none.
pub(crate) fn highest_extent_generation_beside(database: &Path) -> Result<u64, StorageError> {
    let media = crate::media::RealJournalMedia;
    let mut highest = sealed_extents_beside(database)?
        .last()
        .map_or(0, |(generation, _)| *generation);
    for path in [
        crate::journal::journal_path(database),
        crate::journal::checkpoint_journal_path(database),
        crate::journal::sealing_journal_path(database),
        crate::journal::spare_journal_path(database),
    ] {
        if let Some(generation) = crate::journal::extent_generation_with_media(&media, &path)
            .map_err(backup_journal_error)?
        {
            highest = highest.max(generation);
        }
    }
    Ok(highest)
}

/// Sealed journal extents retained beside `database`, in generation order.
fn sealed_extents_beside(database: &Path) -> Result<Vec<(u64, PathBuf)>, StorageError> {
    let directory = database.parent().ok_or_else(corrupt)?;
    let prefix = format!(
        "{}.riffjournal.sealed.",
        database
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(corrupt)?
    );
    let mut sealed = Vec::new();
    for entry in fs::read_dir(directory).map_err(io_unavailable)? {
        let entry = entry.map_err(io_unavailable)?;
        let name = entry.file_name();
        let Some(digits) = name
            .to_str()
            .and_then(|name| name.strip_prefix(prefix.as_str()))
        else {
            continue;
        };
        let generation = digits.parse::<u64>().map_err(|_| corrupt())?;
        if crate::journal::sealed_journal_path(database, generation) != entry.path()
            || sealed.len() == MAX_SEALED_ARTIFACTS
        {
            return Err(corrupt());
        }
        sealed.push((generation, entry.path()));
    }
    sealed.sort_unstable();
    Ok(sealed)
}

fn sealed_artifact_ordinals(
    sealed: &[(u64, PathBuf)],
) -> Result<Vec<(NonZeroU32, PathBuf)>, StorageError> {
    sealed
        .iter()
        .enumerate()
        .map(|(index, (_, path))| {
            let ordinal = u32::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(FIRST_SEALED_ARTIFACT_ORDINAL))
                .and_then(NonZeroU32::new)
                .ok_or_else(corrupt)?;
            Ok((ordinal, path.clone()))
        })
        .collect()
}

/// Verifies and copies every sealed extent beside the source into the backup.
/// A seal a crash interrupted must finish first: opening the database does.
fn stage_sealed_extents(
    source_database: &Path,
    staging: &Path,
    database_id: DatabaseId,
) -> Result<Vec<(u64, PathBuf)>, StorageError> {
    if crate::journal::sealing_journal_path(source_database)
        .try_exists()
        .map_err(io_unavailable)?
    {
        return Err(unknown());
    }
    let mut staged = Vec::new();
    for (generation, path) in sealed_extents_beside(source_database)? {
        crate::journal::read_sealed_extent_with_media(
            &crate::media::RealJournalMedia,
            &path,
            database_id,
        )
        .map_err(|_| corrupt())?;
        let artifact = staging.join(sealed_artifact_name(generation));
        copy_and_sync(&path, &artifact)?;
        staged.push((generation, artifact));
    }
    Ok(staged)
}

/// Copies each sealed backup artifact beside the restore target under a staged
/// name, verifying its checksum and its footer.
fn stage_restored_sealed_extents(
    backup_directory: &Path,
    target_directory: &Path,
    manifest: &OfflineBackupManifestV1,
) -> Result<Vec<(u64, StagedArtifact)>, StorageError> {
    let sealed = sealed_artifacts_in(backup_directory)?;
    let mut staged = Vec::with_capacity(sealed.len());
    for (ordinal, path) in sealed_artifact_ordinals(&sealed)? {
        let generation = sealed_artifact_generation(path.file_name().ok_or_else(corrupt)?)
            .ok_or_else(corrupt)?;
        let expected = manifest_artifact_checksum(manifest, ordinal)?;
        let artifact = StagedArtifact::create_named(
            target_directory,
            OsStr::new(&sealed_artifact_name(generation)),
        )?;
        copy_and_sync(&path, artifact.path())?;
        if sha256_file(artifact.path())? != expected {
            return Err(corrupt());
        }
        crate::journal::read_sealed_extent_with_media(
            &crate::media::RealJournalMedia,
            artifact.path(),
            manifest.database_id(),
        )
        .map_err(|_| corrupt())?;
        staged.push((generation, artifact));
    }
    Ok(staged)
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

/// Opens `path` with the default engine configuration. When this is the
/// process's first engine open after a crash, redb's whole-file repair runs
/// here; record it so the store's later open reports it as its own.
fn open_recording_repair(path: &Path) -> Result<Database, redb::DatabaseError> {
    let repaired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = std::sync::Arc::clone(&repaired);
    let mut builder = Database::builder();
    builder.set_repair_callback(move |_| {
        observed.store(true, std::sync::atomic::Ordering::Release);
    });
    let opened = builder.open(path);
    if repaired.load(std::sync::atomic::Ordering::Acquire) {
        crate::store::note_engine_repaired_by_prior_open(path);
    }
    opened
}

pub(crate) fn lock_existing_target(path: &Path) -> Result<Option<Database>, StorageError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_unavailable(error)),
        Ok(metadata) if !metadata.file_type().is_file() => {
            Err(storage_error(StorageErrorKind::Unavailable))
        }
        Ok(_) => match open_recording_repair(path) {
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

/// Unforgeable outside the native materializer. This grants copied-primary
/// identity only; it is never a physical, logical or publication certificate.
pub(crate) struct VerifiedCopiedPrimary {
    parent: std::sync::Arc<crate::maintenance::PinnedDirectory>,
    name: OsString,
    file: File,
}

impl VerifiedCopiedPrimary {
    pub(crate) fn into_parts(
        self,
    ) -> (
        std::sync::Arc<crate::maintenance::PinnedDirectory>,
        OsString,
        File,
    ) {
        (self.parent, self.name, self.file)
    }
}

/// Three fixed materialization artifacts retain their creation descriptors.
/// Sealed extents continue using the bounded, descriptor-free staging vector.
struct MaterializationArtifact {
    staged: StagedArtifact,
    identity: Option<(std::sync::Arc<crate::maintenance::PinnedDirectory>, File)>,
}

impl MaterializationArtifact {
    fn create(directory: &Path, stem: &OsStr, private: bool) -> Result<Self, StorageError> {
        let parent = if private {
            Some(std::sync::Arc::new(
                crate::maintenance::PinnedDirectory::open(directory)?,
            ))
        } else {
            None
        };
        let (staged, file) = StagedArtifact::create_named_with_file(directory, stem, private)?;
        let artifact = Self {
            staged,
            identity: parent.map(|parent| (parent, file)),
        };
        artifact.verify()?;
        Ok(artifact)
    }

    fn path(&self) -> &Path {
        self.staged.path()
    }

    fn verify(&self) -> Result<(), StorageError> {
        if let Some((parent, file)) = &self.identity {
            parent.verify()?;
            if !parent.regular_file_matches(self.path().file_name().ok_or_else(corrupt)?, file)? {
                return Err(corrupt());
            }
            parent.verify()?;
        }
        Ok(())
    }

    fn copy_from(&self, source: &Path) -> Result<(), StorageError> {
        self.verify()?;
        copy_and_sync(source, self.path())?;
        self.verify()
    }

    fn checksum(&self) -> Result<BackupIntegrityChecksumV1, StorageError> {
        self.verify()?;
        #[cfg(test)]
        MATERIALIZATION_CHECKSUM_PASSES.with(|passes| passes.set(passes.get() + 1));
        let before = self
            .identity
            .as_ref()
            .map(|(_, file)| ArtifactContinuity::read(file))
            .transpose()?;
        let checksum = match &self.identity {
            Some((_, file)) => {
                use std::io::{Seek, SeekFrom};
                let mut reader = file.try_clone().map_err(io_unavailable)?;
                reader.seek(SeekFrom::Start(0)).map_err(io_unavailable)?;
                sha256_reader(reader)?
            }
            None => sha256_file(self.path())?,
        };
        self.verify()?;
        if self
            .identity
            .as_ref()
            .map(|(_, file)| ArtifactContinuity::read(file))
            .transpose()?
            != before
        {
            return Err(corrupt());
        }
        Ok(checksum)
    }

    fn private_file(&self) -> Result<&File, StorageError> {
        self.verify()?;
        let (_, file) = self
            .identity
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if file.metadata().map_err(io_unavailable)?.len() == 0 {
            return Err(corrupt());
        }
        Ok(file)
    }

    fn verified_primary(&self) -> Result<VerifiedCopiedPrimary, StorageError> {
        self.verify()?;
        let (parent, file) = self
            .identity
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let file = file.try_clone().map_err(io_unavailable)?;
        if file.metadata().map_err(io_unavailable)?.len() == 0 {
            return Err(corrupt());
        }
        Ok(VerifiedCopiedPrimary {
            parent: std::sync::Arc::clone(parent),
            name: self.path().file_name().ok_or_else(corrupt)?.to_os_string(),
            file,
        })
    }

    fn rename_under_custody(
        &mut self,
        destination: &Path,
        custody: &crate::maintenance::RestoreCustody,
    ) -> Result<(), StorageError> {
        self.verify()?;
        custody.rename_created_primary(self.path(), destination)?;
        self.staged.mark_published();
        self.staged.path = destination.to_path_buf();
        self.verify()
    }

    fn rename_to(&mut self, destination: &Path) -> Result<(), StorageError> {
        self.verify()?;
        if self.path().parent() != destination.parent() {
            return Err(corrupt());
        }
        fs::rename(self.path(), destination).map_err(io_unavailable)?;
        self.staged.mark_published();
        self.staged.path = destination.to_path_buf();
        self.verify()
    }

    /// This is the existing no-journal backup compatibility path: reset_journal
    /// has deliberately replaced the original zero-length temporary file.
    /// No validation proof or engine has yet existed. Never use for ordinary
    /// identity verification or after a failed/uncertain materialization.
    fn create_legacy_journal(
        &mut self,
        header: &crate::journal::JournalFileHeader,
    ) -> Result<(), StorageError> {
        self.verify()?;
        if let Some((_, file)) = &self.identity
            && file.metadata().map_err(io_unavailable)?.len() != 0
        {
            return Err(corrupt());
        }
        fs::remove_file(self.path()).map_err(io_unavailable)?;
        crate::journal::reset_journal(self.path(), header).map_err(backup_journal_error)?;
        if let Some((parent, file)) = &mut self.identity {
            parent.verify()?;
            *file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.staged.path)
                .map_err(io_unavailable)?;
        }
        self.verify()
    }
}

fn open_private_materialization_database(
    primary: &MaterializationArtifact,
    journal: &MaterializationArtifact,
    marker: &MaterializationArtifact,
) -> Result<Database, StorageError> {
    // The caller has checked every copied artifact against the canonical
    // manifest and sealed extent inventory. Bind the first engine to the exact
    // created primary descriptor, never a freshly selected pathname.
    primary.verify()?;
    journal.verify()?;
    marker.verify()?;
    let database = open_unrepaired_private_file(
        primary.private_file()?,
        Some(crate::store::PRIMARY_REDB_CACHE_SIZE_BYTES),
    )?;
    primary.verify()?;
    journal.verify()?;
    marker.verify()?;
    Ok(database)
}

struct StagedArtifact {
    path: PathBuf,
    published: bool,
}

impl StagedArtifact {
    fn create_named(target_directory: &Path, stem: &OsStr) -> Result<Self, StorageError> {
        Self::create_named_with_file(target_directory, stem, false)
            .map(|(artifact, _file)| artifact)
    }

    fn create_named_with_file(
        target_directory: &Path,
        stem: &OsStr,
        readable: bool,
    ) -> Result<(Self, File), StorageError> {
        for ordinal in 0..MAX_STAGING_ATTEMPTS {
            let path = target_directory.join(staging_name(stem, "restore", ordinal));
            match OpenOptions::new()
                .read(readable)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    return Ok((
                        Self {
                            path,
                            published: false,
                        },
                        file,
                    ));
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

    fn native_materialization_copies(
        root: &TestRoot,
    ) -> (
        OfflineBackupManifestV1,
        MaterializationArtifact,
        MaterializationArtifact,
        MaterializationArtifact,
        PathBuf,
    ) {
        let source = root.join("source.redb");
        initialize(&source);
        let backup = root.join("backup");
        RedbOfflineBackup::bind(&source, &backup)
            .create_offline_backup(&build_metadata())
            .unwrap();
        let (manifest, _) = validate_immutable_backup(&backup).unwrap();
        let stage = root.join("stage");
        fs::create_dir(&stage).unwrap();
        let primary =
            MaterializationArtifact::create(&stage, OsStr::new(DATABASE_ARTIFACT_FILE_NAME), true)
                .unwrap();
        let journal =
            MaterializationArtifact::create(&stage, OsStr::new(JOURNAL_ARTIFACT_FILE_NAME), true)
                .unwrap();
        let marker = MaterializationArtifact::create(
            &stage,
            OsStr::new(FORMAT_MARKER_ARTIFACT_FILE_NAME),
            true,
        )
        .unwrap();
        for (artifact, source) in [
            (&primary, DATABASE_ARTIFACT_FILE_NAME),
            (&journal, JOURNAL_ARTIFACT_FILE_NAME),
            (&marker, FORMAT_MARKER_ARTIFACT_FILE_NAME),
        ] {
            artifact.copy_from(&backup.join(source)).unwrap();
        }
        assert_eq!(
            primary.checksum().unwrap(),
            manifest_checksum(&manifest).unwrap()
        );
        assert_eq!(
            Some(journal.checksum().unwrap()),
            manifest_journal_checksum(&manifest).unwrap()
        );
        assert_eq!(
            Some(marker.checksum().unwrap()),
            manifest_format_marker_checksum(&manifest).unwrap()
        );
        validate_backup_journal(
            journal.path(),
            manifest.database_id(),
            manifest.last_commit_sequence(),
        )
        .unwrap();
        validate_manifest_format_marker(marker.path(), &manifest).unwrap();
        (manifest, primary, journal, marker, backup)
    }

    fn assert_native_materialization_replacement_refused(replaced: usize) {
        let root = TestRoot::new("private-copy-native-identity");
        let (manifest, primary, journal, marker, backup) = native_materialization_copies(&root);
        let artifacts = [&primary, &journal, &marker];
        let before: Vec<_> = artifacts
            .iter()
            .map(|artifact| fs::read(artifact.path()).unwrap())
            .collect();
        let target = artifacts[replaced].path();
        fs::rename(target, root.join("original-copied-artifact")).unwrap();
        fs::write(target, &before[replaced]).unwrap();
        let result = open_private_materialization_database(&primary, &journal, &marker);
        let refused =
            matches!(&result, Err(error) if error.kind() == StorageErrorKind::CorruptData);
        drop(result);
        assert!(
            refused,
            "first private materialization open must refuse replaced copied artifacts"
        );
        for (artifact, bytes) in artifacts.iter().zip(before) {
            assert_eq!(
                fs::read(artifact.path()).unwrap(),
                bytes,
                "refusal must precede engine writes"
            );
        }
        assert_eq!(validate_immutable_backup(&backup).unwrap().0, manifest);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn private_materialization_first_open_refuses_replaced_primary_before_engine_write() {
        assert_native_materialization_replacement_refused(0);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn private_materialization_first_open_refuses_replaced_journal_before_engine_write() {
        assert_native_materialization_replacement_refused(1);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn private_materialization_first_open_refuses_replaced_marker_before_engine_write() {
        assert_native_materialization_replacement_refused(2);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn private_materialization_first_open_refuses_replaced_parent_before_engine_write() {
        let root = TestRoot::new("private-copy-parent-native-identity");
        let (manifest, primary, journal, marker, backup) = native_materialization_copies(&root);
        let artifacts = [&primary, &journal, &marker];
        let before: Vec<_> = artifacts
            .iter()
            .map(|artifact| fs::read(artifact.path()).unwrap())
            .collect();
        fs::rename(root.join("stage"), root.join("original-stage")).unwrap();
        fs::create_dir(root.join("stage")).unwrap();
        for (artifact, bytes) in artifacts.iter().zip(&before) {
            fs::write(artifact.path(), bytes).unwrap();
        }
        let result = open_private_materialization_database(&primary, &journal, &marker);
        let refused =
            matches!(&result, Err(error) if error.kind() == StorageErrorKind::CorruptData);
        drop(result);
        assert!(
            refused,
            "first private materialization open must refuse replaced copied artifacts"
        );
        for (artifact, bytes) in artifacts.iter().zip(before) {
            assert_eq!(fs::read(artifact.path()).unwrap(), bytes);
        }
        assert_eq!(validate_immutable_backup(&backup).unwrap().0, manifest);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn private_materialization_descriptor_open_preserves_manifest_facts_and_known_rename_identity()
    {
        let root = TestRoot::new("private-copy-native-identity-control");
        let (manifest, mut primary, mut journal, mut marker, backup) =
            native_materialization_copies(&root);
        let database = open_private_materialization_database(&primary, &journal, &marker).unwrap();
        validate_copied_database_facts(read_database_facts(&database).unwrap(), &manifest).unwrap();
        let target = root.join("stage").join(DATABASE_ARTIFACT_FILE_NAME);
        primary.rename_to(&target).unwrap();
        journal
            .rename_to(&crate::journal::journal_path(&target))
            .unwrap();
        marker
            .rename_to(&crate::durable_format_marker_path(&target))
            .unwrap();
        primary.verify().unwrap();
        journal.verify().unwrap();
        marker.verify().unwrap();
        validate_copied_database_facts(read_database_facts(&database).unwrap(), &manifest).unwrap();
        assert!(matches!(
            Database::open(&target),
            Err(redb::DatabaseError::DatabaseAlreadyOpen)
        ));
        drop(database);
        assert_eq!(validate_immutable_backup(&backup).unwrap().0, manifest);
        assert_eq!(
            journal.checksum().unwrap(),
            manifest_journal_checksum(&manifest).unwrap().unwrap()
        );
        assert_eq!(
            marker.checksum().unwrap(),
            manifest_format_marker_checksum(&manifest).unwrap().unwrap()
        );
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_keeps_native_custody_across_first_engine_close_and_physical_validation()
     {
        let root = TestRoot::new("owned-native-materialization-lifetime");
        let (_, backup, expected) = create_backup(&root);
        let before = validate_immutable_backup(&backup).unwrap();
        let stage = root.join("stage");
        let (manifest, mut owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        assert_eq!(manifest, expected);
        let path = stage.join(DATABASE_ARTIFACT_FILE_NAME);
        crate::maintenance::probe_restore_custody(&path, true);
        let original_read = owned.pin_prepared_read().unwrap();
        // Native backend close can precede ReadTransaction release. The RiffDB
        // pin keeps the SAME Database alive and refuses close while it exists.
        assert!(matches!(owned.pin_prepared_read(),
            Err(error) if error.kind() == StorageErrorKind::Unavailable));
        assert!(matches!(owned.close_for_validation(),
            Err(error) if error.kind() == StorageErrorKind::Unavailable));
        crate::maintenance::probe_restore_custody(&path, true);
        {
            let meta = original_read.read.open_table(META).unwrap();
            let identity = meta.get(META_DATABASE_ID).unwrap().unwrap();
            assert_eq!(
                *codec::decode_database_identity_v1(identity.value())
                    .unwrap()
                    .value(),
                manifest.database_id(),
                "the original native read remains usable after refused close"
            );
        }
        drop(original_read);
        let length = owned.close_for_validation().unwrap();
        assert_eq!(length, fs::metadata(&path).unwrap().len());
        crate::maintenance::probe_restore_custody(&path, true);
        let mut database = open_unrepaired_private_backend(
            Box::new(owned.custody.lease().unwrap()),
            Some(crate::store::PRIMARY_REDB_CACHE_SIZE_BYTES),
        )
        .unwrap();
        assert!(database.check_integrity().unwrap());
        validate_copied_database_facts(read_database_facts(&database).unwrap(), &manifest).unwrap();
        drop(database);
        owned.verify().unwrap();
        crate::maintenance::probe_restore_custody(&path, true);
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(!root.join("configured.redb").exists());
        drop(owned);
        crate::maintenance::probe_restore_custody(&path, false);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_refuses_primary_replacement_after_known_renames() {
        let root = TestRoot::new("owned-native-materialization-replacement");
        let (_, backup, _) = create_backup(&root);
        let before = validate_immutable_backup(&backup).unwrap();
        let stage = root.join("stage");
        let (_, mut owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let path = stage.join(DATABASE_ARTIFACT_FILE_NAME);
        fs::rename(&path, root.join("original-primary")).unwrap();
        fs::copy(root.join("original-primary"), &path).unwrap();
        assert!(matches!(owned.close_for_validation(),
            Err(error) if error.kind() == StorageErrorKind::CorruptData));
        assert!(owned.verify().is_err());
        fs::remove_file(&path).unwrap();
        fs::rename(root.join("original-primary"), &path).unwrap();
        assert!(
            owned.verify().is_err(),
            "restored identity cannot launder uncertain custody"
        );
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(!root.join("configured.redb").exists());
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_cancellation_cannot_restore_authority_after_reset() {
        let root = TestRoot::new("owned-native-materialization-cancellation");
        let (_, backup, _) = create_backup(&root);
        let before = validate_immutable_backup(&backup).unwrap();
        let cancellation = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (_, owned) = RedbOfflineRestore::bind(&backup, root.join("stage"))
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::clone(&cancellation),
            )
            .unwrap();
        cancellation.store(true, Ordering::Release);
        assert!(
            matches!(owned.verify(), Err(error) if error.kind() == StorageErrorKind::Unavailable)
        );
        cancellation.store(false, Ordering::Release);
        assert!(
            matches!(owned.verify(), Err(error) if error.kind() == StorageErrorKind::Unavailable)
        );
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(!root.join("configured.redb").exists());
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

    // req: REC-001, REC-004, REC-006, END-001
    #[test]
    fn published_restore_manifest_binding_is_not_artifact_validation() {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
        let root = TestRoot::new("published-manifest-binding");
        let source = root.join("source.redb");
        initialize(&source);
        let backup = root.join("backup");
        RedbOfflineBackup::bind(&source, &backup)
            .create_offline_backup(&build_metadata())
            .unwrap();
        let expected = validate_immutable_backup(&backup).unwrap();
        assert_eq!(read_backup_manifest_binding(&backup).unwrap(), expected);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(backup.join(DATABASE_ARTIFACT_FILE_NAME))
            .unwrap();
        let mut byte = [0];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 1;
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert_eq!(
            read_backup_manifest_binding(&backup).unwrap(),
            expected,
            "receipt identity lookup must not repeat complete artifact validation"
        );
        assert!(matches!(validate_immutable_backup(&backup), Err(error)
            if error.kind() == StorageErrorKind::CorruptData));
        fs::write(backup.join(MANIFEST_FILE_NAME), b"invalid manifest").unwrap();
        assert!(read_backup_manifest_binding(&backup).is_err());
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

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_preserves_required_native_locator_preparation() {
        let root = TestRoot::new("owned-native-required-locators");
        let source = root.join("source.redb");
        {
            let mut store = RedbStore::open(&source).unwrap();
            store.initialize_legacy_fixture(database_id()).unwrap();
        }
        {
            let database = Database::open(&source).unwrap();
            let write = database.begin_write().unwrap();
            assert!(
                write
                    .delete_table(crate::layout::IDEMPOTENCY_LOCATORS)
                    .unwrap()
            );
            assert!(
                write
                    .delete_table(crate::layout::PROVENANCE_LOCATORS)
                    .unwrap()
            );
            assert!(
                write
                    .delete_table(crate::layout::AUDIT_BY_REQUEST_LOCATORS)
                    .unwrap()
            );
            write.commit().unwrap();
        }
        let backup = root.join("backup");
        let expected = RedbOfflineBackup::bind(&source, &backup)
            .create_offline_backup(&build_metadata())
            .unwrap();
        let before = validate_immutable_backup(&backup).unwrap();
        let control = root.join("ordinary-control");
        RedbOfflineRestore::bind(&backup, &control)
            .restore_staged_backup(&root.join("configured.redb"))
            .unwrap();
        let control_database = control.join(DATABASE_ARTIFACT_FILE_NAME);
        // The very same immutable backup remains supported by ordinary native
        // preparation. The owned lifecycle must not narrow that behavior.
        assert_eq!(
            complete_structural_scan(RedbStore::open(&control_database).unwrap()),
            expected.database_id()
        );
        let stage = root.join("stage");
        let (manifest, owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        assert_eq!(manifest, expected);
        let path = stage.join(DATABASE_ARTIFACT_FILE_NAME);
        let validated = owned
            .validate_complete(owned_validation_inputs())
            .expect("owned restore must perform native preparation before complete validation");
        crate::maintenance::probe_restore_custody(&path, true);
        drop(validated);
        crate::maintenance::probe_restore_custody(&path, false);
        let database = Database::open(&path).unwrap();
        let read = database.begin_read().unwrap();
        // The actual derived tables must exist, as they do after ordinary
        // source startup. Complete scrub alone cannot replace preparation.
        assert!(
            read.open_table(crate::layout::IDEMPOTENCY_LOCATORS).is_ok(),
            "owned restore must perform native preparation before complete validation"
        );
        assert!(read.open_table(crate::layout::PROVENANCE_LOCATORS).is_ok());
        assert!(
            read.open_table(crate::layout::AUDIT_BY_REQUEST_LOCATORS)
                .is_ok()
        );
        validate_copied_database_facts(read_database_facts(&database).unwrap(), &manifest).unwrap();
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(!root.join("configured.redb").exists());
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_native_proof_refuses_an_untracked_engine_commit() {
        let root = TestRoot::new("owned-native-proof-raw-commit");
        let (_, backup, _) = create_backup(&root);
        let before = validate_immutable_backup(&backup).unwrap();
        let (_, owned) = RedbOfflineRestore::bind(&backup, root.join("stage"))
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let validated = owned.validate_complete(owned_validation_inputs()).unwrap();
        let shared = validated.logical.native_owner_for_test();
        let epoch = shared.durable_commit_epoch();
        let OwnedValidatedMaterialization {
            logical,
            materialization,
        } = validated;
        let (_, _, _, _, reader) = logical.into_authorization_parts().unwrap();
        let clone = reader.clone();
        reader.verify_validated_authorization_read().unwrap();
        assert!(
            reader
                .complete_logical_access_for_test()
                .read_value(
                    crate::journal::JournalTable::OutboxStatus,
                    b"native-proof-raw-write"
                )
                .unwrap()
                .is_none()
        );
        let write = shared.database.begin_write().unwrap();
        write
            .open_table(crate::layout::OUTBOX_STATUS)
            .unwrap()
            .insert(
                b"native-proof-raw-write".as_slice(),
                b"unclassified".as_slice(),
            )
            .unwrap();
        // Exercise the actual engine directly, bypassing SharedRedb's commit
        // epoch. Private native proof must observe every backend mutation.
        write.commit().unwrap();
        assert_eq!(shared.durable_commit_epoch(), epoch);
        assert!(
            reader
                .complete_logical_access_for_test()
                .read_value(
                    crate::journal::JournalTable::OutboxStatus,
                    b"native-proof-raw-write"
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(
            riffdb_storage_api::SnapshotFenceReader::application_frontier(&reader).is_err(),
            "native restore proof must refuse an unclassified engine commit"
        );
        assert!(riffdb_storage_api::CatalogRepository::read_active_catalog(&reader).is_err());
        assert!(
            riffdb_storage_api::CapabilityReader::resolve_capability_digests(&reader, &[]).is_err()
        );
        assert!(reader.verify_validated_authorization_read().is_err());
        assert!(clone.verify_validated_authorization_read().is_err());
        drop(reader);
        drop(clone);
        drop(shared);
        drop(materialization);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_native_proof_refuses_primary_replacement_on_cached_reads() {
        let root = TestRoot::new("owned-native-proof-cached-identity");
        let (_, backup, _) = create_backup(&root);
        let before = validate_immutable_backup(&backup).unwrap();
        let stage = root.join("stage");
        let (_, owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let validated = owned.validate_complete(owned_validation_inputs()).unwrap();
        let OwnedValidatedMaterialization {
            logical,
            materialization,
        } = validated;
        let (_, _, _, _, reader) = logical.into_authorization_parts().unwrap();
        let clone = reader.clone();
        reader.verify_validated_authorization_read().unwrap();
        assert!(
            reader
                .complete_logical_access_for_test()
                .read_value(
                    crate::journal::JournalTable::OutboxStatus,
                    b"native-proof-cache"
                )
                .unwrap()
                .is_none()
        );
        let path = stage.join(DATABASE_ARTIFACT_FILE_NAME);
        fs::rename(&path, root.join("original-primary")).unwrap();
        fs::copy(root.join("original-primary"), &path).unwrap();
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(
            riffdb_storage_api::SnapshotFenceReader::application_frontier(&reader).is_err(),
            "cached native restore proof must verify the original primary identity"
        );
        assert!(riffdb_storage_api::CatalogRepository::read_active_catalog(&reader).is_err());
        assert!(
            riffdb_storage_api::CapabilityReader::resolve_capability_digests(&reader, &[]).is_err()
        );
        assert!(reader.verify_validated_authorization_read().is_err());
        fs::remove_file(&path).unwrap();
        fs::rename(root.join("original-primary"), &path).unwrap();
        assert!(
            clone.verify_validated_authorization_read().is_err(),
            "restored pathname cannot launder an invalidated cached proof"
        );
        drop(reader);
        drop(clone);
        drop(materialization);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_compatibility_completes_before_authorization() {
        let root = TestRoot::new("owned-native-compatibility-completion");
        let source = root.join("source.redb");
        {
            let mut store = RedbStore::open(&source).unwrap();
            store.initialize_legacy_fixture(database_id()).unwrap();
        }
        let backup = root.join("backup");
        let expected = RedbOfflineBackup::bind(&source, &backup)
            .create_offline_backup(&build_metadata())
            .unwrap();
        let before = validate_immutable_backup(&backup).unwrap();
        let (_, owned) = RedbOfflineRestore::bind(&backup, root.join("stage"))
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let validated = owned.validate_complete(owned_validation_inputs()).unwrap();
        let shared = validated.logical.native_owner_for_test();
        let read = shared.database.begin_read().unwrap();
        assert!(
            crate::changelog_v3_roots::read_checkpoint_roots(&read)
                .unwrap()
                .is_some(),
            "owned restore must complete required native compatibility before authorization"
        );
        let meta = read.open_table(crate::layout::META).unwrap();
        let encoded = meta
            .get(crate::layout::META_RECORD_REGISTRY)
            .unwrap()
            .unwrap();
        let registry = riffdb_storage_api::decode_record_registry(encoded.value()).unwrap();
        assert!(
            registry.value().completed_for(
                expected.database_id(),
                expected.history_incarnation().unwrap_or(0),
                riffdb_storage_api::current_record_registry_digest()
            ),
            "owned restore must complete required native compatibility before authorization"
        );
        drop(encoded);
        drop(meta);
        drop(read);
        drop(shared);
        let OwnedValidatedMaterialization {
            logical,
            materialization,
        } = validated;
        let (database, session, retained, catalog, reader) =
            logical.into_authorization_parts().unwrap();
        assert!(catalog.matches(database, session));
        assert_eq!(retained.database_id(), expected.database_id());
        reader.verify_validated_authorization_read().unwrap();
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(!root.join("configured.redb").exists());
        drop(reader);
        drop(materialization);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_validated_bytes_refuse_changed_primary() {
        owned_materialization_validated_byte_change_refused(DATABASE_ARTIFACT_FILE_NAME);
    }
    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_validated_bytes_refuse_changed_journal() {
        owned_materialization_validated_byte_change_refused(JOURNAL_ARTIFACT_FILE_NAME);
    }
    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_validated_bytes_refuse_changed_marker() {
        owned_materialization_validated_byte_change_refused(FORMAT_MARKER_ARTIFACT_FILE_NAME);
    }
    // req: REC-001, REC-006, END-001
    #[cfg(unix)]
    #[test]
    fn owned_materialization_immutable_handoffs_reuse_seal_without_content_reads() {
        let root = TestRoot::new("owned-native-continuity");
        let (_, backup, _) = create_backup(&root);
        let (_, owned) = RedbOfflineRestore::bind(&backup, &root.join("stage"))
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let validated = owned.validate_complete(owned_validation_inputs()).unwrap();
        MATERIALIZATION_CHECKSUM_PASSES.with(|passes| passes.set(0));
        for _ in 0..10 {
            validated.materialization.verify().unwrap();
        }
        assert_eq!(
            MATERIALIZATION_CHECKSUM_PASSES.with(std::cell::Cell::get),
            0
        );
        validated.materialization.verify_full_seal().unwrap();
        assert_eq!(
            MATERIALIZATION_CHECKSUM_PASSES.with(std::cell::Cell::get),
            3
        );
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_seals_post_hydration_extent_inventory() {
        let root = TestRoot::new("owned-native-hydrated-inventory");
        let source = root.join("source.redb");
        initialize(&source);
        crate::journal::reset_journal(
            &crate::journal::journal_path(&source),
            &crate::journal::JournalFileHeader::new(database_id(), None, [0; 32]),
        )
        .unwrap();
        fs::rename(
            crate::journal::journal_path(&source),
            crate::journal::sealing_journal_path(&source),
        )
        .unwrap();
        crate::journal::seal_extent_with_media(
            &crate::media::RealJournalMedia,
            &source,
            database_id(),
        )
        .unwrap()
        .unwrap();
        crate::journal::recreate_missing_active_with_media(
            &crate::media::RealJournalMedia,
            &source,
        )
        .unwrap();
        let backup = root.join("backup");
        let manifest = RedbOfflineBackup::bind(&source, &backup)
            .create_offline_backup(&build_metadata())
            .unwrap();
        assert!(
            manifest.checksums().len() > 3,
            "the immutable backup includes a sealed extent"
        );
        let before = validate_immutable_backup(&backup).unwrap();
        let (_, owned) = RedbOfflineRestore::bind(&backup, &root.join("stage"))
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        assert!(
            sealed_extents_beside(owned.primary.path())
                .unwrap()
                .is_empty(),
            "known hydration consumes copied extents before validation"
        );
        let validated = owned
            .validate_complete(owned_validation_inputs())
            .expect("post-hydration inventory must admit unchanged validated materialization");
        let (owner, parts) = validated.into_authorization().unwrap();
        drop(parts);
        let mut closed = owner.close().unwrap();
        closed.stamp_history_incarnation(2).unwrap();
        closed.verify_publication_seal().unwrap();
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(!root.join("configured.redb").exists());
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_continuity_refuses_an_added_extent() {
        let root = TestRoot::new("owned-native-added-extent");
        let closed = closed_continuity_fixture(&root);
        fs::write(
            crate::journal::sealed_journal_path(closed.materialization.primary.path(), 99),
            b"unclassified-extent",
        )
        .unwrap();
        assert!(closed.verify().is_err());
        assert!(closed.verify_publication_seal().is_err());
        assert!(!root.join("configured.redb").exists());
    }

    fn closed_continuity_fixture(root: &TestRoot) -> OwnedClosedMaterialization {
        let (_, backup, _) = create_backup(root);
        let (_, owned) = RedbOfflineRestore::bind(&backup, &root.join("stage"))
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let validated = owned.validate_complete(owned_validation_inputs()).unwrap();
        let (owner, parts) = validated.into_authorization().unwrap();
        drop(parts);
        owner.close().unwrap()
    }

    fn change_same_length_preserving_mtime(path: &Path) {
        use std::io::{Seek, SeekFrom};
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let modified = file.metadata().unwrap().modified().unwrap();
        let mut byte = [0];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 1;
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&byte).unwrap();
        file.set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
        file.sync_all().unwrap();
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_unsupported_continuity_falls_back_to_content() {
        let root = TestRoot::new("owned-native-continuity-fallback");
        let mut closed = closed_continuity_fixture(&root);
        closed
            .materialization
            .validated_seal
            .as_mut()
            .unwrap()
            .continuity = [None; 3];
        MATERIALIZATION_CHECKSUM_PASSES.with(|passes| passes.set(0));
        closed.verify().unwrap();
        assert_eq!(
            MATERIALIZATION_CHECKSUM_PASSES.with(std::cell::Cell::get),
            3
        );
        change_same_length_preserving_mtime(closed.materialization.journal.path());
        assert!(closed.verify().is_err());
        assert!(!root.join("configured.redb").exists());
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_full_boundary_refuses_even_unchanged_metadata() {
        let root = TestRoot::new("owned-native-full-seal");
        let mut closed = closed_continuity_fixture(&root);
        change_same_length_preserving_mtime(closed.materialization.journal.path());
        // Model a platform whose metadata observations cannot distinguish the
        // write. This is deliberately not a content proof or a new seal.
        closed
            .materialization
            .validated_seal
            .as_mut()
            .unwrap()
            .continuity = closed.materialization.artifact_continuity().unwrap();
        assert!(closed.verify_publication_seal().is_err());
        assert!(
            closed.verify().is_err(),
            "a bad full seal permanently invalidates custody"
        );
        assert!(!root.join("configured.redb").exists());
    }

    // req: REC-001, REC-006, END-001
    #[cfg(unix)]
    #[test]
    fn owned_materialization_continuity_refuses_same_length_with_restored_mtime() {
        let root = TestRoot::new("owned-native-restored-mtime");
        let closed = closed_continuity_fixture(&root);
        change_same_length_preserving_mtime(closed.materialization.journal.path());
        assert!(closed.verify().is_err());
        assert!(!root.join("configured.redb").exists());
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_continuity_refuses_stale_generation() {
        let root = TestRoot::new("owned-native-stale-generation");
        let mut closed = closed_continuity_fixture(&root);
        closed
            .materialization
            .validated_seal
            .as_mut()
            .unwrap()
            .mutation_generation += 1;
        assert!(closed.verify().is_err());
        assert!(closed.verify_publication_seal().is_err());
    }

    fn owned_materialization_validated_byte_change_refused(artifact: &str) {
        use std::io::Write;
        {
            let root = TestRoot::new("owned-native-byte-seal");
            let (_, backup, _) = create_backup(&root);
            let before = validate_immutable_backup(&backup).unwrap();
            let stage = root.join("stage");
            let (_, owned) = RedbOfflineRestore::bind(&backup, &stage)
                .restore_owned_staged_backup(
                    &root.join("configured.redb"),
                    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                )
                .unwrap();
            let validated = owned.validate_complete(owned_validation_inputs()).unwrap();
            let path = match artifact {
                DATABASE_ARTIFACT_FILE_NAME => stage.join(DATABASE_ARTIFACT_FILE_NAME),
                JOURNAL_ARTIFACT_FILE_NAME => {
                    crate::journal::journal_path(&stage.join(DATABASE_ARTIFACT_FILE_NAME))
                }
                _ => crate::durable_format_marker_path(&stage.join(DATABASE_ARTIFACT_FILE_NAME)),
            };
            // This intentionally bypasses all engine wrappers while retaining
            // the same original inode. Native byte seals must still refuse it.
            let mut writer = fs::OpenOptions::new().append(true).open(&path).unwrap();
            writer.write_all(b"unclassified-after-validation").unwrap();
            writer.sync_all().unwrap();
            drop(writer);
            assert!(
                validated.materialization.verify().is_err(),
                "owned restore must refuse changed validated artifact bytes before resealing"
            );
            assert!(
                validated.logical.into_authorization_parts().is_err(),
                "byte-seal refusal must invalidate the actual native proof"
            );
            drop(validated.materialization);
            assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
            assert!(!root.join("configured.redb").exists());
        }
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_authorization_close_retains_original_native_custody() {
        let root = TestRoot::new("owned-native-auth-close");
        let (_, backup, manifest) = create_backup(&root);
        let before = validate_immutable_backup(&backup).unwrap();
        let stage = root.join("stage");
        let (_, owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let validated = owned.validate_complete(owned_validation_inputs()).unwrap();
        let (owner, (database, session, retained, catalog, reader)) =
            validated.into_authorization().unwrap();
        assert_eq!(database, manifest.database_id());
        assert!(catalog.matches(database, session));
        assert_eq!(retained.database_id(), database);
        reader.verify_validated_authorization_read().unwrap();
        let clone = reader.clone();
        drop(reader);
        clone.verify_validated_authorization_read().unwrap();
        drop(clone);
        let path = stage.join(DATABASE_ARTIFACT_FILE_NAME);
        let closed = owner.close().unwrap();
        closed.verify().unwrap();
        assert_eq!(
            closed.history_incarnation(),
            manifest.history_incarnation().unwrap_or(0)
        );
        let (primary, journal, marker) = closed.checksums().unwrap();
        assert_eq!(primary, sha256_file(&path).unwrap());
        assert_eq!(
            journal,
            sha256_file(&crate::journal::journal_path(&path)).unwrap()
        );
        assert_eq!(
            marker,
            sha256_file(&crate::durable_format_marker_path(&path)).unwrap()
        );
        crate::maintenance::probe_restore_custody(&path, true);
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        drop(closed);
        crate::maintenance::probe_restore_custody(&path, false);
        assert!(!root.join("configured.redb").exists());
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_authorization_close_refuses_a_surviving_reader_clone() {
        let root = TestRoot::new("owned-native-auth-close-pinned");
        let (_, backup, _) = create_backup(&root);
        let stage = root.join("stage");
        let (_, owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let (owner, (_, _, _, _, reader)) = owned
            .validate_complete(owned_validation_inputs())
            .unwrap()
            .into_authorization()
            .unwrap();
        let clone = reader.clone();
        drop(reader);
        assert!(
            matches!(owner.close(), Err(error) if error.kind() == StorageErrorKind::Unavailable)
        );
        assert!(clone.verify_validated_authorization_read().is_err());
        let path = stage.join(DATABASE_ARTIFACT_FILE_NAME);
        crate::maintenance::probe_restore_custody(&path, true);
        drop(clone);
        crate::maintenance::probe_restore_custody(&path, false);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_receipt_stamp_retains_native_custody_and_is_idempotent() {
        let root = TestRoot::new("owned-native-receipt-stamp");
        let (_, backup, _) = create_backup(&root);
        let before = validate_immutable_backup(&backup).unwrap();
        let stage = root.join("stage");
        let (_, owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let (owner, (_, _, _, _, reader)) = owned
            .validate_complete(owned_validation_inputs())
            .unwrap()
            .into_authorization()
            .unwrap();
        drop(reader);
        let mut closed = owner.close().unwrap();
        let incarnation = closed.history_incarnation().checked_add(1).unwrap();
        closed
            .stamp_history_incarnation(incarnation)
            .expect("owned closed stamp must use the retained native engine custody");
        closed.verify().unwrap();
        assert_eq!(closed.history_incarnation(), incarnation);
        let path = stage.join(DATABASE_ARTIFACT_FILE_NAME);
        crate::maintenance::probe_restore_custody(&path, true);
        let first = closed.checksums().unwrap();
        // A real original-routine control starts from exactly the same sealed
        // primary. Read-only observations cannot clean or rewrite its bytes.
        let control = root.join("original-equal-stamp-control.redb");
        fs::copy(&path, &control).unwrap();
        let observe = |path: &Path| {
            let database = redb::ReadOnlyDatabase::open(path).unwrap();
            let facts = read_database_facts(&database).unwrap();
            let read = database.begin_read().unwrap();
            let retained = crate::startup::read_retained_metadata_pub(&read).unwrap();
            let history = crate::changelog_v3_roots::validate_retained_history(&read)
                .unwrap()
                .unwrap();
            let table = read
                .open_table(crate::changelog_v3_activation::HISTORY)
                .unwrap();
            let rows = table
                .iter()
                .unwrap()
                .map(|row| {
                    let (key, value) = row.unwrap();
                    (key.value().to_vec(), value.value().to_vec())
                })
                .collect::<Vec<_>>();
            assert!(rows.len() <= 8, "bounded native stamp fixture");
            (facts, retained, history, rows)
        };
        let expected = observe(&control);
        assert_eq!(sha256_file(&control).unwrap(), first.0);
        stamp_sealed_history_incarnation(&control, incarnation).unwrap();
        assert_eq!(
            observe(&control),
            expected,
            "original equal retry preserves all retained facts and exact receipts"
        );
        let original_equal_checksum = sha256_file(&control).unwrap();
        assert_ne!(
            original_equal_checksum, first.0,
            "the original abort/open/close changes engine housekeeping bytes in this fixture"
        );
        closed.stamp_history_incarnation(incarnation).unwrap();
        let second = closed.checksums().unwrap();
        assert_eq!(
            second.0, original_equal_checksum,
            "owned equal retry preserves exact original native close behavior"
        );
        assert_eq!(second.1, first.1, "equal retry never changes journal");
        assert_eq!(second.2, first.2, "equal retry never changes marker");
        crate::maintenance::probe_restore_custody(&path, true);
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        drop(closed);
        crate::maintenance::probe_restore_custody(&path, false);
        assert_eq!(
            observe(&path),
            expected,
            "owned retry preserves original retained facts, allocators and receipt bytes"
        );
        assert_eq!(read_history_incarnation(&path).unwrap(), Some(incarnation));
        let database = Database::open(&path).unwrap();
        let read = database.begin_read().unwrap();
        let history = crate::changelog_v3_roots::validate_retained_history(&read)
            .unwrap()
            .unwrap();
        assert_eq!(history.lineage().history_incarnation(), incarnation);
        assert!(!root.join("configured.redb").exists());
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_receipt_stamp_refuses_changed_closed_artifacts() {
        use std::io::Write;
        for artifact in [
            DATABASE_ARTIFACT_FILE_NAME,
            JOURNAL_ARTIFACT_FILE_NAME,
            FORMAT_MARKER_ARTIFACT_FILE_NAME,
        ] {
            let root = TestRoot::new("owned-native-receipt-stamp-changed");
            let (_, backup, _) = create_backup(&root);
            let before = validate_immutable_backup(&backup).unwrap();
            let stage = root.join("stage");
            let (_, owned) = RedbOfflineRestore::bind(&backup, &stage)
                .restore_owned_staged_backup(
                    &root.join("configured.redb"),
                    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                )
                .unwrap();
            let (owner, (_, _, _, _, reader)) = owned
                .validate_complete(owned_validation_inputs())
                .unwrap()
                .into_authorization()
                .unwrap();
            drop(reader);
            let mut closed = owner.close().unwrap();
            let incarnation = closed.history_incarnation().checked_add(1).unwrap();
            let primary = stage.join(DATABASE_ARTIFACT_FILE_NAME);
            let path = match artifact {
                DATABASE_ARTIFACT_FILE_NAME => primary,
                JOURNAL_ARTIFACT_FILE_NAME => crate::journal::journal_path(&primary),
                _ => crate::durable_format_marker_path(&primary),
            };
            let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"changed-before-receipt-stamp").unwrap();
            file.sync_all().unwrap();
            drop(file);
            let damaged = fs::read(&path).unwrap();
            assert!(closed.stamp_history_incarnation(incarnation).is_err());
            assert!(closed.verify().is_err());
            assert_eq!(fs::read(&path).unwrap(), damaged);
            assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
            assert!(!root.join("configured.redb").exists());
        }
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_server_evidence_retains_exact_inputs_and_private_read() {
        let root = TestRoot::new("owned-native-server-evidence");
        let (_, backup, manifest) = create_backup(&root);
        let before = validate_immutable_backup(&backup).unwrap();
        let stage = root.join("stage");
        let (_, owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let inputs = owned_validation_inputs();
        let (owner, evidence) = owned
            .validate_complete(inputs.clone())
            .unwrap()
            .into_server_authorization()
            .unwrap();
        assert!(evidence.matches_inputs(&inputs));
        let mismatch = StartupValidationInputs::new(
            inputs.authorization_time(),
            ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(
                DigestKeyId::new(2).unwrap(),
            )])
            .unwrap(),
            inputs.idempotency_digests().clone(),
        );
        assert!(!evidence.matches_inputs(&mismatch));
        let (database, session, retained, catalog, reader) = evidence.into_parts().unwrap();
        assert_eq!(database, manifest.database_id());
        assert!(catalog.matches(database, session));
        assert_eq!(retained.database_id(), database);
        assert!(owner.shared.cannot_serve_command_commit());
        assert!(!owner.shared.startup_validation_clean());
        reader.verify_validated_authorization_read().unwrap();
        let path = stage.join(DATABASE_ARTIFACT_FILE_NAME);
        crate::maintenance::probe_restore_custody(&path, true);
        drop(reader);
        let closed = owner.close().unwrap();
        closed.verify().unwrap();
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(!root.join("configured.redb").exists());
        drop(closed);
        crate::maintenance::probe_restore_custody(&path, false);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_server_evidence_refuses_cancellation_before_the_join() {
        let root = TestRoot::new("owned-native-server-evidence-cancel");
        let (_, backup, _) = create_backup(&root);
        let cancellation = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (_, owned) = RedbOfflineRestore::bind(&backup, root.join("stage"))
            .restore_owned_staged_backup(&root.join("configured.redb"), cancellation.clone())
            .unwrap();
        let (owner, evidence) = owned
            .validate_complete(owned_validation_inputs())
            .unwrap()
            .into_server_authorization()
            .unwrap();
        cancellation.store(true, std::sync::atomic::Ordering::Release);
        assert!(
            matches!(evidence.into_parts(), Err(error) if error.kind() == StorageErrorKind::Unavailable)
        );
        assert!(owner.close().is_err());
        assert!(!root.join("configured.redb").exists());
    }

    // req: STO-023, REC-004, END-004, REC-001, REC-006
    #[test]
    fn owned_materialization_preserves_clean_certificate_bytes_without_clean_shortcut() {
        fn certificate(database: &impl redb::ReadableDatabase) -> Vec<u8> {
            let read = database.begin_read().unwrap();
            let meta = read.open_table(crate::layout::META).unwrap();
            meta.get(crate::layout::META_CLEAN_CLOSE_LIFECYCLE)
                .unwrap()
                .unwrap()
                .value()
                .to_vec()
        }
        let root = TestRoot::new("owned-native-clean-certificate");
        let source = root.join("source.redb");
        let backup = root.join("backup");
        initialize(&source);
        {
            let store = RedbStore::open(&source).unwrap();
            let mut session = store
                .begin_structural_evidence(owned_validation_inputs())
                .unwrap();
            let limit = EvidencePageLimit::new(64).unwrap();
            let mut cursor =
                StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
            let structural_end = loop {
                match session.read_structural_evidence(cursor, limit).unwrap() {
                    StructuralEvidencePage::Page { findings, next, .. } => {
                        assert!(findings.is_empty());
                        cursor = next;
                    }
                    StructuralEvidencePage::ExactEnd(end) => break end,
                }
            };
            let (catalog, historical_end) = session
                .validate_startup_catalog_history()
                .unwrap()
                .into_parts();
            assert!(matches!(
                catalog,
                riffdb_catalog::CatalogHistoryOutcome::Ready(_)
            ));
            let riffdb_storage_api::StructuralOpenOutcome::Clean(opened) =
                session.finish(structural_end, historical_end).unwrap()
            else {
                panic!("current source must not require migration");
            };
            let (_, _, _, dormant) = opened.into_parts();
            dormant
                .into_operational_after_catalog_validation()
                .unwrap()
                .write_clean_close_lifecycle()
                .unwrap();
        }
        let clean = certificate(&redb::ReadOnlyDatabase::open(&source).unwrap());
        RedbOfflineBackup::bind(&source, &backup)
            .create_offline_backup(&build_metadata())
            .unwrap();
        assert_eq!(
            certificate(
                &redb::ReadOnlyDatabase::open(backup.join(DATABASE_ARTIFACT_FILE_NAME)).unwrap()
            ),
            clean
        );
        let before = validate_immutable_backup(&backup).unwrap();
        let stage = root.join("stage");
        let (_, owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let (owner, (database, session, retained, catalog, reader)) = owned
            .validate_complete(owned_validation_inputs())
            .unwrap()
            .into_authorization()
            .unwrap();
        assert_eq!(
            certificate(&owner.shared.database),
            clean,
            "normal restore staging preserves exact immutable certificate bytes"
        );
        // A complete logical proof is issued only by finish_offline_integrity_scrub,
        // which rejects clean_close_fast, bounded_dirty and checkpoint shortcuts
        // and requires both exact ends and the original catalog/session match.
        assert!(catalog.matches(database, session));
        assert_eq!(retained.database_id(), database);
        reader.verify_validated_authorization_read().unwrap();
        assert!(!owner.shared.startup_validation_clean());
        crate::maintenance::probe_restore_custody(&stage.join(DATABASE_ARTIFACT_FILE_NAME), true);
        drop(reader);
        owner.close().unwrap().verify().unwrap();
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(!root.join("configured.redb").exists());
    }

    fn owned_validation_inputs() -> StartupValidationInputs {
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
        StartupValidationInputs::new(
            Timestamp::new(1_700_000_000, 0).unwrap(),
            ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
            ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
        )
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_same_first_engine_reaches_both_complete_validation_ends() {
        let root = TestRoot::new("owned-native-complete-validation");
        let (_, backup, expected) = create_backup(&root);
        let before = validate_immutable_backup(&backup).unwrap();
        let stage = root.join("stage");
        let (manifest, owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(
                &root.join("configured.redb"),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        assert_eq!(manifest, expected);
        let path = stage.join(DATABASE_ARTIFACT_FILE_NAME);
        crate::maintenance::probe_restore_custody(&path, true);
        let validated = owned.validate_complete(owned_validation_inputs()).unwrap();
        validated.materialization.verify().unwrap();
        crate::maintenance::probe_restore_custody(&path, true);
        let OwnedValidatedMaterialization {
            logical,
            materialization,
        } = validated;
        let (database_id, session, retained, catalog, reader) =
            logical.into_authorization_parts().unwrap();
        assert_eq!(database_id, manifest.database_id());
        assert_eq!(retained.database_id(), database_id);
        assert!(catalog.matches(database_id, session));
        // The immutable proof reader owns the SAME engine, even when its
        // materialization handle is released; no second dense copy exists.
        drop(materialization);
        crate::maintenance::probe_restore_custody(&path, true);
        assert_eq!(
            fs::read_dir(&stage)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_type().unwrap().is_dir())
                .count(),
            0
        );
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(!root.join("configured.redb").exists());
        drop(reader);
        crate::maintenance::probe_restore_custody(&path, false);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn owned_materialization_complete_validation_refuses_cancellation_after_exact_ends() {
        let root = TestRoot::new("owned-native-complete-cancel");
        let (_, backup, _) = create_backup(&root);
        let before = validate_immutable_backup(&backup).unwrap();
        let cancellation = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stage = root.join("stage");
        let (_, owned) = RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_staged_backup(&root.join("configured.redb"), cancellation.clone())
            .unwrap();
        let validated = owned.validate_complete(owned_validation_inputs()).unwrap();
        cancellation.store(true, std::sync::atomic::Ordering::Release);
        let OwnedValidatedMaterialization {
            logical,
            materialization,
        } = validated;
        assert!(matches!(logical.into_authorization_parts(),
            Err(error) if error.kind() == StorageErrorKind::Unavailable));
        assert!(materialization.verify().is_err());
        drop(materialization);
        crate::maintenance::probe_restore_custody(&stage.join(DATABASE_ARTIFACT_FILE_NAME), false);
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
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
    // req: REC-001, REC-006, END-001
    #[test]
    fn staged_materialization_refuses_repair_at_its_first_engine_open() {
        let root = TestRoot::new("staged-first-open-repair-refusal");
        let (_source, backup, manifest) = create_backup(&root);
        let artifact = backup.join(DATABASE_ARTIFACT_FILE_NAME);
        let database = Database::open(&artifact).unwrap();
        let facts = read_database_facts(&database).unwrap();
        drop(database);
        let mut primary = fs::read(&artifact).unwrap();
        assert_eq!(&primary[..9], b"redb\x1a\x0a\xa9\x0d\x0a");
        primary[9] = (primary[9] | 2) & !4;
        fs::write(&artifact, &primary).unwrap();
        // Authenticate a genuinely repair-requiring artifact. A checksum check
        // alone cannot authorize repairing an unpublished native restore.
        let checksums = manifest
            .checksums()
            .iter()
            .map(|entry| {
                BackupArtifactChecksumV1::new(
                    entry.artifact_ordinal(),
                    if entry.artifact_ordinal() == NonZeroU32::MIN {
                        sha256_file(&artifact).unwrap()
                    } else {
                        entry.checksum().clone()
                    },
                )
            })
            .collect();
        let manifest = facts
            .into_manifest_with_format_compatibility(
                checksums,
                manifest.build().clone(),
                manifest.format_compatibility().unwrap(),
            )
            .unwrap();
        fs::write(
            backup.join(MANIFEST_FILE_NAME),
            encode_manifest(&manifest).unwrap(),
        )
        .unwrap();
        validate_immutable_backup(&backup).unwrap();
        let original_inventory: Vec<_> = fs::read_dir(&backup)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let checksum = sha256_file(&path).unwrap();
                (path, checksum)
            })
            .collect();
        let stage = root.join("stage");
        let result = RedbOfflineRestore::bind(&backup, &stage)
            .restore_staged_backup(&root.join("configured.redb"));
        assert!(
            matches!(result, Err(ref error) if error.kind() == StorageErrorKind::CorruptData),
            "first private engine open must abort repair"
        );
        assert!(!stage.join(DATABASE_ARTIFACT_FILE_NAME).exists());
        assert!(!root.join("configured.redb").exists());
        assert_eq!(fs::read(&artifact).unwrap(), primary);
        for (path, checksum) in original_inventory {
            assert_eq!(sha256_file(&path).unwrap(), checksum);
        }
    }

    // req: REC-001, REC-002, REC-006, REC-008
    #[test]
    fn staged_history_hydration_refuses_engine_repair_before_scrub() {
        let root = TestRoot::new("staged-hydration-repair-refusal");
        let (_source, backup, _) = create_backup(&root);
        let stage = root.join("stage");
        RedbOfflineRestore::bind(&backup, &stage)
            .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
            .unwrap();
        let artifact = stage.join(DATABASE_ARTIFACT_FILE_NAME);
        // A healthy private stage remains hydratable; an existing native
        // engine owner is still excluded before the repair-only refusal.
        hydrate_staged_history(&stage, database_id(), &root.join("configured.redb")).unwrap();
        let held = Database::open(&artifact).unwrap();
        let locked = hydrate_staged_history(&stage, database_id(), &root.join("configured.redb"));
        assert!(matches!(locked, Err(ref error) if error.kind() == StorageErrorKind::Unavailable));
        drop(held);
        let mut primary = fs::read(&artifact).unwrap();
        assert_eq!(&primary[..9], b"redb\x1a\x0a\xa9\x0d\x0a");
        assert_eq!(primary[9] & 2, 0, "clean engine recovery flag");
        // Pinned redb fixture: require full allocator repair, rather than
        // loading an intact two-phase allocator after a normal dirty close.
        primary[9] = (primary[9] | 2) & !4;
        fs::write(&artifact, &primary).unwrap();
        let original_backup = fs::read(backup.join(DATABASE_ARTIFACT_FILE_NAME)).unwrap();
        let result = hydrate_staged_history(&stage, database_id(), &root.join("configured.redb"));
        assert_eq!(
            fs::read(backup.join(DATABASE_ARTIFACT_FILE_NAME)).unwrap(),
            original_backup
        );
        assert!(
            matches!(result, Err(ref error) if error.kind() == StorageErrorKind::CorruptData),
            "private stage must refuse engine repair before a later clean scrub: {result:?}",
        );
        assert_eq!(fs::read(&artifact).unwrap(), primary);
        // The incarnation helper must independently reject repair too, even
        // when reached without the sealed owner's prior checksum check.
        let stamp = stamp_sealed_history_incarnation(&artifact, 2);
        assert!(matches!(stamp, Err(ref error) if error.kind() == StorageErrorKind::CorruptData));
        assert_eq!(fs::read(&artifact).unwrap(), primary);
    }

    // req: REC-001, REC-006, END-001
    #[test]
    fn staged_materialization_keeps_native_exclusion_through_hydration_and_watermark() {
        use std::sync::mpsc::{RecvTimeoutError, sync_channel};
        use std::time::Duration;

        let root = TestRoot::new("materialization-owned-engine");
        let (_source, backup, _) = create_backup(&root);
        let original = sha256_file(&backup.join(DATABASE_ARTIFACT_FILE_NAME)).unwrap();
        let stage = root.join("stage");
        let configured = root.join("configured.redb");
        let (reached_tx, reached_rx) = sync_channel(1);
        let (release_tx, release_rx) = sync_channel(1);
        let restore_backup = backup.clone();
        let restore_stage = stage.clone();
        let worker = std::thread::spawn(move || {
            MATERIALIZATION_TEST_BARRIER.with(|slot| {
                *slot.borrow_mut() = Some(MaterializationTestBarrier {
                    reached: reached_tx,
                    release: release_rx,
                });
            });
            RedbOfflineRestore::bind(restore_backup, restore_stage)
                .restore_staged_backup(&configured)
        });
        let mut observations = Vec::new();
        for expected in [
            MaterializationTestBoundary::BeforeHydration,
            MaterializationTestBoundary::BeforeWatermark,
        ] {
            match reached_rx.recv_timeout(Duration::from_secs(120)) {
                Ok(actual) => {
                    let excluded = match Database::open(stage.join(DATABASE_ARTIFACT_FILE_NAME)) {
                        Err(redb::DatabaseError::DatabaseAlreadyOpen) => true,
                        Ok(competing) => {
                            drop(competing);
                            false
                        }
                        Err(_) => false,
                    };
                    observations.push((actual == expected, excluded));
                    if release_tx.send(()).is_err() {
                        break;
                    }
                }
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
            }
        }
        // Always release and join before asserting, including the red baseline.
        drop(release_tx);
        drop(reached_rx);
        let result = worker.join().expect("restore worker");
        assert_eq!(
            observations.len(),
            2,
            "both native boundaries must be observed"
        );
        assert!(
            observations.iter().all(|(phase, held)| *phase && *held),
            "private materialization must exclude a competing native engine at both boundaries"
        );
        assert!(matches!(
            result,
            Ok(OfflineRestoreResultV1::Restored { .. })
        ));
        assert_eq!(
            sha256_file(&backup.join(DATABASE_ARTIFACT_FILE_NAME)).unwrap(),
            original
        );
        // Ownership ends normally once the materialization operation has ended.
        drop(Database::open(stage.join(DATABASE_ARTIFACT_FILE_NAME)).unwrap());
    }
}
