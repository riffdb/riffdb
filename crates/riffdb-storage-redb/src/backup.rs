//! Offline redb backup artifacts and destructive restore.

use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use redb::{Database, Durability, ReadableDatabase, ReadableTable};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, ApplicationSequenceAllocator, BackupArtifactChecksumV1,
    BackupBuildMetadataV1, BackupCatalogBundleV1, BackupIntegrityChecksumV1, BackupManifestVersion,
    BackupSnapshotKindV1, HISTORY_INCARNATION_INITIAL, MAX_BACKUP_BUILD_FEATURE_BYTES,
    MAX_BACKUP_BUILD_FEATURES, MAX_BACKUP_BUILD_VALUE_BYTES, MAX_BACKUP_CATALOG_BUNDLES,
    MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES, OfflineBackupManifestIdentityV1, OfflineBackupManifestV1,
    OfflineBackupPersistencePort, OfflineRestoreOverwritePolicyV1, OfflineRestorePersistencePort,
    OfflineRestoreResultV1, StorageError, StorageErrorKind, StorageFormatVersion,
    StorageValueError,
};
use riffdb_types::{
    CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId,
    MAX_CONTRACT_LINEAGE_BYTES,
};
use sha2::{Digest, Sha256};

use crate::codec;
use crate::error::{
    commit_error, database_error, precommit_storage_error, storage_error, table_error,
    transaction_error,
};
use crate::hooks::{RedbTestController, RedbTestOperation};
use crate::keys::{decode_application_sequence_key, decode_contract_bundle_key};
use crate::layout::{
    CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, COMMITS, CONTRACT_BUNDLES, EVENTS, META,
    META_APPLICATION_SEQUENCE, META_DATABASE_ID, META_FORMAT_VERSION, META_HISTORY_INCARNATION,
};

pub(crate) const MANIFEST_FILE_NAME: &str = "manifest.riffdb";
pub(crate) const DATABASE_ARTIFACT_FILE_NAME: &str = "database.redb";
const MANIFEST_MAGIC: &[u8; 16] = b"RIFFDB-BACKUP\0\0\0";
const MANIFEST_MAX_BYTES: usize = 32 * 1024 * 1024;
const COPY_BUFFER_BYTES: usize = 64 * 1024;
const MAX_STAGING_ATTEMPTS: u16 = 256;
const SHA256_BYTES: usize = 32;
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
/// Offline-only path used after restore publication. Writes only when the
/// stored value differs from `incarnation`.
pub fn stamp_history_incarnation(
    path: impl AsRef<Path>,
    incarnation: u64,
) -> Result<(), StorageError> {
    if incarnation < HISTORY_INCARNATION_INITIAL {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let database = Database::open(path.as_ref()).map_err(database_error)?;
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
    if current == Some(incarnation) {
        return transaction.abort().map_err(precommit_storage_error);
    }
    {
        let mut meta = transaction.open_table(META).map_err(table_error)?;
        let encoded = codec::encode_history_incarnation_v1(incarnation)?;
        meta.insert(META_HISTORY_INCARNATION, encoded.as_bytes())
            .map_err(precommit_storage_error)?;
    }
    transaction.commit().map_err(commit_error)?;
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
}

impl DatabaseFacts {
    fn into_manifest(
        self,
        checksum: BackupIntegrityChecksumV1,
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
            vec![BackupArtifactChecksumV1::new(NonZeroU32::MIN, checksum)],
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

        let (source_database, source_facts) = open_database_with_facts(&self.source_database)?;
        copy_and_sync(&self.source_database, &artifact_path)?;
        let (artifact_database, artifact_facts) = open_database_with_facts(&artifact_path)?;
        drop(artifact_database);
        if artifact_facts != source_facts {
            return Err(corrupt());
        }
        let checksum = sha256_file(&artifact_path)?;
        let manifest = source_facts.into_manifest(checksum, build.clone())?;
        let manifest_bytes = encode_manifest(&manifest)?;
        write_new_and_sync(&staging.path().join(MANIFEST_FILE_NAME), &manifest_bytes)?;
        validate_backup_inventory(staging.path())?;
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
        let target_lock = lock_existing_target(&target_artifact)?;
        let mut staged_artifact = StagedArtifact::create(&self.target_directory)?;
        copy_and_sync(
            &self.backup_directory.join(DATABASE_ARTIFACT_FILE_NAME),
            staged_artifact.path(),
        )?;
        let staged_lock = validate_artifact(staged_artifact.path(), &manifest)?;

        if let Some(controller) = &self.test_controller {
            controller.before_commit(RedbTestOperation::Restore)?;
        }
        fs::rename(staged_artifact.path(), &target_artifact).map_err(io_unavailable)?;
        staged_artifact.mark_published();
        if sync_directory(&self.target_directory).is_err() {
            return Err(unknown());
        }
        drop(target_lock);
        drop(staged_lock);
        if let Some(controller) = &self.test_controller {
            controller.after_commit(RedbTestOperation::Restore)?;
        }
        Ok(OfflineRestoreResultV1::Restored {
            manifest: Box::new(manifest),
        })
    }
}

fn open_database_with_facts(path: &Path) -> Result<(Database, DatabaseFacts), StorageError> {
    let database = Database::open(path).map_err(database_error)?;
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
    let last_commit_sequence = match commits.last().map_err(precommit_storage_error)? {
        None => {
            if allocator != ApplicationSequenceAllocator::initial() {
                return Err(corrupt());
            }
            None
        }
        Some((key, value)) => {
            let sequence = decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;
            let commit = codec::decode_commit_with_event_table(value.value(), &events)?
                .into_parts()
                .0;
            if commit.commit_sequence() != sequence {
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

    Ok((
        database,
        DatabaseFacts {
            storage_format_version,
            database_id,
            catalog_bundles,
            active_catalog,
            last_commit_sequence,
            history_incarnation,
        },
    ))
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
    let reconstructed = facts.into_manifest(expected_checksum, manifest.build().clone())?;
    if &reconstructed != manifest {
        return Err(corrupt());
    }
    Ok(database)
}

pub(crate) fn validate_immutable_backup(
    directory: &Path,
) -> Result<(OfflineBackupManifestV1, OfflineBackupManifestIdentityV1), StorageError> {
    validate_backup_inventory(directory)?;
    let manifest_bytes =
        read_bounded_file(&directory.join(MANIFEST_FILE_NAME), MANIFEST_MAX_BYTES)?;
    let manifest = decode_manifest(&manifest_bytes)?;
    let artifact = directory.join(DATABASE_ARTIFACT_FILE_NAME);
    if sha256_file(&artifact)? != manifest_checksum(&manifest)? {
        return Err(corrupt());
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

pub(crate) fn validate_database_semantics(
    artifact: &Path,
    manifest: &OfflineBackupManifestV1,
) -> Result<Database, StorageError> {
    let expected_checksum = manifest_checksum(manifest)?;
    let (database, facts) = open_database_with_facts(artifact)?;
    // Startup-owned migrations may insert history_incarnation/v1 on pre-fence
    // artifacts. Compare non-history facts exactly, and require
    // manifest.history_incarnation ≤ staged when both are present.
    if !facts.semantically_match_manifest(manifest, &expected_checksum)? {
        return Err(corrupt());
    }
    Ok(database)
}

impl DatabaseFacts {
    /// Exact non-history semantic match plus the fence-compatible history rule.
    ///
    /// When both the manifest and artifact carry a history incarnation, the
    /// manifest value must be ≤ the artifact value (startup may only advance it
    /// via accepted migrations). A pre-fence manifest without the field remains
    /// acceptable. A manifest that claims an incarnation the artifact lacks is
    /// corrupt.
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
        let [checksum] = manifest.checksums() else {
            return Ok(false);
        };
        if checksum.checksum() != expected_checksum {
            return Ok(false);
        }
        match (manifest.history_incarnation(), self.history_incarnation) {
            (Some(from_manifest), Some(from_artifact)) if from_manifest > from_artifact => {
                Ok(false)
            }
            (Some(_), None) => Ok(false),
            _ => Ok(true),
        }
    }
}

fn manifest_checksum(
    manifest: &OfflineBackupManifestV1,
) -> Result<BackupIntegrityChecksumV1, StorageError> {
    let [checksum] = manifest.checksums() else {
        return Err(corrupt());
    };
    if checksum.artifact_ordinal() != NonZeroU32::MIN
        || checksum.checksum().as_bytes().len() != SHA256_BYTES
    {
        return Err(corrupt());
    }
    Ok(checksum.checksum().clone())
}

fn encode_manifest(manifest: &OfflineBackupManifestV1) -> Result<Vec<u8>, StorageError> {
    encode_manifest_with_history_field(manifest, true)
}

/// Pre-fence encoding omits the history-incarnation presence tag entirely.
fn encode_manifest_pre_fence(manifest: &OfflineBackupManifestV1) -> Result<Vec<u8>, StorageError> {
    if manifest.history_incarnation().is_some() {
        return Err(corrupt());
    }
    encode_manifest_with_history_field(manifest, false)
}

fn encode_manifest_with_history_field(
    manifest: &OfflineBackupManifestV1,
    include_history_field: bool,
) -> Result<Vec<u8>, StorageError> {
    if manifest.manifest_version() != BackupManifestVersion::V1
        || manifest.snapshot_kind() != BackupSnapshotKindV1::StorageEngineData
    {
        return Err(incompatible());
    }
    let [checksum] = manifest.checksums() else {
        return Err(corrupt());
    };
    if checksum.artifact_ordinal() != NonZeroU32::MIN
        || checksum.checksum().as_bytes().len() != SHA256_BYTES
    {
        return Err(corrupt());
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
    if include_history_field {
        match manifest.history_incarnation() {
            None => output.u8(0)?,
            Some(incarnation) => {
                output.u8(1)?;
                output.u64(incarnation)?;
            }
        }
    }
    output.u32(1)?;
    output.u32(checksum.artifact_ordinal().get())?;
    output.framed(checksum.checksum().as_bytes())?;
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
    // Prefer post-fence (presence-tagged history field). Fall back to pre-fence
    // bytes that omit the field entirely (accept-and-stamp at restore). On any
    // post-fence parse/canonicalization mismatch, try the pre-fence parser
    // rather than fail closed — pre-fence bytes can partially parse as post-fence.
    if let Ok(manifest) = decode_manifest_body(encoded, true)
        && encode_manifest(&manifest)? == encoded
    {
        return Ok(manifest);
    }
    let manifest = decode_manifest_body(encoded, false)?;
    if encode_manifest_pre_fence(&manifest)? != encoded {
        return Err(corrupt());
    }
    Ok(manifest)
}

fn decode_manifest_body(
    encoded: &[u8],
    include_history_field: bool,
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
    let history_incarnation = if include_history_field {
        match input.u8()? {
            0 => None,
            1 => {
                let incarnation = input.u64()?;
                if incarnation < 1 {
                    return Err(corrupt());
                }
                Some(incarnation)
            }
            _ => return Err(corrupt()),
        }
    } else {
        None
    };

    if input.u32()? != 1 {
        return Err(corrupt());
    }
    let ordinal = NonZeroU32::new(input.u32()?).ok_or_else(corrupt)?;
    if ordinal != NonZeroU32::MIN {
        return Err(corrupt());
    }
    let checksum_bytes = input.framed(MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES)?;
    if checksum_bytes.len() != SHA256_BYTES {
        return Err(corrupt());
    }
    let checksum = BackupIntegrityChecksumV1::new(checksum_bytes.to_vec()).map_err(value_error)?;

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
    OfflineBackupManifestV1::new(
        storage_format_version,
        database_id,
        BackupSnapshotKindV1::StorageEngineData,
        catalog_bundles,
        active_catalog,
        last_commit_sequence,
        history_incarnation,
        vec![BackupArtifactChecksumV1::new(ordinal, checksum)],
        build,
    )
    .map_err(value_error)
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
    let mut names = Vec::with_capacity(2);
    for entry in fs::read_dir(directory).map_err(io_unavailable)? {
        let entry = entry.map_err(io_unavailable)?;
        if names.len() == 2 {
            return Err(corrupt());
        }
        let metadata = fs::symlink_metadata(entry.path()).map_err(io_unavailable)?;
        if !metadata.file_type().is_file() {
            return Err(corrupt());
        }
        names.push(entry.file_name());
    }
    names.sort_unstable();
    let mut expected = [
        OsString::from(DATABASE_ARTIFACT_FILE_NAME),
        OsString::from(MANIFEST_FILE_NAME),
    ];
    expected.sort_unstable();
    if names != expected {
        return Err(corrupt());
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
        for ordinal in 0..MAX_STAGING_ATTEMPTS {
            let path = target_directory.join(staging_name(
                OsStr::new(DATABASE_ARTIFACT_FILE_NAME),
                "restore",
                ordinal,
            ));
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
            let path = std::env::temp_dir().join(format!(
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
        assert_eq!(manifest.checksums().len(), 1);

        let mut names = fs::read_dir(&backup)
            .expect("read backup")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                OsString::from(DATABASE_ARTIFACT_FILE_NAME),
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
        }
        .into_manifest(checksum, build_metadata())
        .expect("manifest");
        let encoded = encode_manifest(&manifest).expect("encode manifest");
        assert_eq!(
            decode_manifest(&encoded).expect("decode manifest"),
            manifest
        );
        let golden_digest: [u8; SHA256_BYTES] = Sha256::digest(&encoded).into();
        // Encoding grows by the presence-tagged history_incarnation field; digest is
        // recomputed whenever the durable layout of this fixture changes intentionally.
        assert_eq!(
            golden_digest,
            [
                98, 103, 172, 112, 38, 182, 112, 170, 150, 145, 173, 122, 22, 2, 234, 237, 12, 227,
                218, 76, 37, 220, 133, 3, 47, 44, 214, 244, 252, 204, 145, 19,
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
    fn pre_fence_manifest_and_stamp_round_trip() {
        let checksum = BackupIntegrityChecksumV1::new(vec![0x5a; SHA256_BYTES]).expect("checksum");
        let pre_fence = DatabaseFacts {
            storage_format_version: StorageFormatVersion::V1,
            database_id: database_id(),
            catalog_bundles: Vec::new(),
            active_catalog: None,
            last_commit_sequence: None,
            history_incarnation: None,
        }
        .into_manifest(checksum.clone(), build_metadata())
        .expect("pre-fence manifest");
        let encoded = encode_manifest_pre_fence(&pre_fence).expect("encode pre-fence");
        let decoded = decode_manifest(&encoded).expect("decode pre-fence via dual-path");
        assert_eq!(decoded.history_incarnation(), None);
        assert_eq!(decoded.database_id(), pre_fence.database_id());

        // Post-fence encode with presence tag also round-trips.
        let with_field = DatabaseFacts {
            storage_format_version: StorageFormatVersion::V1,
            database_id: database_id(),
            catalog_bundles: Vec::new(),
            active_catalog: None,
            last_commit_sequence: None,
            history_incarnation: Some(1),
        }
        .into_manifest(checksum, build_metadata())
        .expect("post-fence manifest");
        let post = encode_manifest(&with_field).expect("encode post-fence");
        assert_eq!(
            decode_manifest(&post).expect("decode post-fence"),
            with_field
        );
    }

    #[test]
    fn read_history_incarnation_absent_vs_unreadable() {
        let root = std::env::temp_dir().join(format!(
            "riffdb-history-read-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("root");
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
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn stamp_history_incarnation_is_idempotent() {
        let root = std::env::temp_dir().join(format!(
            "riffdb-history-stamp-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("root");
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
        let _ = std::fs::remove_dir_all(&root);
    }
}
