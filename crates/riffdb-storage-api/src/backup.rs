//! Checked value-level metadata for backend-private offline backup operations.
//!
//! This module defines no backup/restore handle, filesystem path, generic byte
//! sink, or on-disk encoding. ADR-0004 keeps execution handles private to the
//! concrete adapter; WP-070 owns the offline artifact and manifest encoding.

use std::num::NonZeroU32;

use riffdb_types::{
    CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId,
};

use crate::{ActiveCatalogPointerV1, StorageError, StorageFormatVersion, StorageValueError};

/// Maximum bytes in one build-metadata scalar.
pub const MAX_BACKUP_BUILD_VALUE_BYTES: usize = 256;
/// Maximum enabled feature names retained in build metadata.
pub const MAX_BACKUP_BUILD_FEATURES: usize = 128;
/// Maximum bytes in one enabled feature name.
pub const MAX_BACKUP_BUILD_FEATURE_BYTES: usize = 128;
/// Maximum catalog bundle descriptors retained in one manifest.
pub const MAX_BACKUP_CATALOG_BUNDLES: usize = 65_535;
/// Maximum artifact checksums retained in one manifest.
pub const MAX_BACKUP_ARTIFACT_CHECKSUMS: usize = 65_535;
/// Maximum opaque adapter-owned bytes in one integrity checksum value.
pub const MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES: usize = 256;

/// Nonzero semantic version of the offline backup manifest.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackupManifestVersion(NonZeroU32);

impl BackupManifestVersion {
    /// Initial POC semantic manifest version.
    pub const V1: Self = Self(NonZeroU32::MIN);

    /// Reconstructs a nonzero historical version for checked decoding.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric semantic manifest version.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// One immutable contract bundle identity included by an offline backup.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackupCatalogBundleV1 {
    lineage: ContractLineage,
    contract_version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

impl BackupCatalogBundleV1 {
    /// Constructs an exact immutable bundle descriptor.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        contract_version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Self {
        Self {
            lineage,
            contract_version,
            bundle_hash,
        }
    }

    /// Returns the exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the immutable contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Returns the hash of the canonical immutable bundle.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }

    fn matches_active(&self, active: &ActiveCatalogPointerV1) -> bool {
        self.lineage == *active.lineage()
            && self.contract_version == active.contract_version()
            && self.bundle_hash == active.bundle_hash()
    }
}

/// Closed snapshot representation included by an offline backup.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BackupSnapshotKindV1 {
    /// A complete consistent set of storage-engine data artifacts.
    StorageEngineData,
    /// A complete engine-independent logical snapshot verified by the adapter.
    VerifiedLogicalSnapshot,
}

/// Bounded adapter-owned checksum representation.
///
/// Storage API intentionally does not select a backup checksum algorithm or
/// byte encoding. The concrete offline backup format owns both and must verify
/// the exact value during restore.
#[derive(Clone, Eq, PartialEq)]
pub struct BackupIntegrityChecksumV1(Vec<u8>);

impl BackupIntegrityChecksumV1 {
    /// Constructs a nonempty bounded opaque checksum value.
    pub fn new(value: Vec<u8>) -> Result<Self, StorageValueError> {
        if value.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if value.len() > MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self(value))
    }

    /// Borrows the complete adapter-owned checksum representation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for BackupIntegrityChecksumV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackupIntegrityChecksumV1")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.0.len())
            .finish()
    }
}

/// One exact integrity checksum for a named backup artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupArtifactChecksumV1 {
    artifact_ordinal: NonZeroU32,
    checksum: BackupIntegrityChecksumV1,
}

impl BackupArtifactChecksumV1 {
    /// Associates one opaque checksum with its one-based snapshot artifact.
    #[must_use]
    pub const fn new(artifact_ordinal: NonZeroU32, checksum: BackupIntegrityChecksumV1) -> Self {
        Self {
            artifact_ordinal,
            checksum,
        }
    }

    /// Returns the one-based opaque artifact position in the complete snapshot.
    #[must_use]
    pub const fn artifact_ordinal(&self) -> NonZeroU32 {
        self.artifact_ordinal
    }

    /// Borrows the complete adapter-owned integrity checksum.
    #[must_use]
    pub const fn checksum(&self) -> &BackupIntegrityChecksumV1 {
        &self.checksum
    }
}

/// Bounded release/build identity retained in an offline backup manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupBuildMetadataV1 {
    semantic_version: String,
    git_revision: String,
    rust_version: String,
    executable_ir_version: u32,
    enabled_features: Vec<String>,
}

impl BackupBuildMetadataV1 {
    /// Constructs canonical build metadata from already captured values.
    pub fn new(
        semantic_version: impl Into<String>,
        git_revision: impl Into<String>,
        rust_version: impl Into<String>,
        executable_ir_version: u32,
        mut enabled_features: Vec<String>,
    ) -> Result<Self, StorageValueError> {
        let semantic_version = checked_build_value(semantic_version.into())?;
        let git_revision = checked_build_value(git_revision.into())?;
        let rust_version = checked_build_value(rust_version.into())?;
        if executable_ir_version == 0 {
            return Err(StorageValueError::InvalidShape);
        }
        if enabled_features.len() > MAX_BACKUP_BUILD_FEATURES {
            return Err(StorageValueError::LimitExceeded);
        }
        for feature in &enabled_features {
            checked_feature(feature)?;
        }
        enabled_features.sort_unstable();
        if enabled_features.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self {
            semantic_version,
            git_revision,
            rust_version,
            executable_ir_version,
            enabled_features,
        })
    }

    /// Returns the embedded RiffDB semantic version.
    #[must_use]
    pub fn semantic_version(&self) -> &str {
        &self.semantic_version
    }

    /// Returns the embedded source revision.
    #[must_use]
    pub fn git_revision(&self) -> &str {
        &self.git_revision
    }

    /// Returns the embedded Rust toolchain version.
    #[must_use]
    pub fn rust_version(&self) -> &str {
        &self.rust_version
    }

    /// Returns the executable contract-IR version without importing IR here.
    #[must_use]
    pub const fn executable_ir_version(&self) -> u32 {
        self.executable_ir_version
    }

    /// Returns enabled feature names in canonical order.
    #[must_use]
    pub fn enabled_features(&self) -> &[String] {
        &self.enabled_features
    }
}

/// Checked semantic summary for one complete offline consistent backup.
///
/// This is not the manifest byte encoding and does not make online backup or
/// point-in-time recovery available.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflineBackupManifestV1 {
    manifest_version: BackupManifestVersion,
    storage_format_version: StorageFormatVersion,
    database_id: DatabaseId,
    snapshot_kind: BackupSnapshotKindV1,
    catalog_bundles: Vec<BackupCatalogBundleV1>,
    active_catalog: Option<ActiveCatalogPointerV1>,
    last_commit_sequence: Option<CommitSequence>,
    checksums: Vec<BackupArtifactChecksumV1>,
    build: BackupBuildMetadataV1,
    semantic_bytes: usize,
}

impl OfflineBackupManifestV1 {
    /// Validates canonical bundle/checksum inventories and active linkage.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_format_version: StorageFormatVersion,
        database_id: DatabaseId,
        snapshot_kind: BackupSnapshotKindV1,
        mut catalog_bundles: Vec<BackupCatalogBundleV1>,
        active_catalog: Option<ActiveCatalogPointerV1>,
        last_commit_sequence: Option<CommitSequence>,
        mut checksums: Vec<BackupArtifactChecksumV1>,
        build: BackupBuildMetadataV1,
    ) -> Result<Self, StorageValueError> {
        if catalog_bundles.len() > MAX_BACKUP_CATALOG_BUNDLES
            || checksums.len() > MAX_BACKUP_ARTIFACT_CHECKSUMS
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if checksums.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if active_catalog.is_none()
            && (!catalog_bundles.is_empty() || last_commit_sequence.is_some())
        {
            return Err(StorageValueError::InvalidShape);
        }

        catalog_bundles.sort_unstable();
        if catalog_bundles.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        if active_catalog.as_ref().is_some_and(|active| {
            !catalog_bundles
                .iter()
                .any(|bundle| bundle.matches_active(active))
        }) {
            return Err(StorageValueError::IdentityMismatch);
        }

        checksums.sort_by_key(|checksum| checksum.artifact_ordinal);
        if checksums
            .windows(2)
            .any(|pair| pair[0].artifact_ordinal == pair[1].artifact_ordinal)
        {
            return Err(StorageValueError::Duplicate);
        }

        let semantic_bytes = backup_manifest_semantic_bytes(
            &catalog_bundles,
            active_catalog.as_ref(),
            last_commit_sequence,
            &checksums,
            &build,
        )?;

        Ok(Self {
            manifest_version: BackupManifestVersion::V1,
            storage_format_version,
            database_id,
            snapshot_kind,
            catalog_bundles,
            active_catalog,
            last_commit_sequence,
            checksums,
            build,
            semantic_bytes,
        })
    }

    /// Returns the semantic backup-manifest version.
    #[must_use]
    pub const fn manifest_version(&self) -> BackupManifestVersion {
        self.manifest_version
    }

    /// Returns the included database storage-format version.
    #[must_use]
    pub const fn storage_format_version(&self) -> StorageFormatVersion {
        self.storage_format_version
    }

    /// Returns the permanent database identity preserved by restore.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns whether this backup contains engine artifacts or a logical snapshot.
    #[must_use]
    pub const fn snapshot_kind(&self) -> BackupSnapshotKindV1 {
        self.snapshot_kind
    }

    /// Returns all immutable bundle hashes in canonical identity order.
    #[must_use]
    pub fn catalog_bundles(&self) -> &[BackupCatalogBundleV1] {
        &self.catalog_bundles
    }

    /// Borrows the exact active contract relation, if deployed.
    #[must_use]
    pub const fn active_catalog(&self) -> Option<&ActiveCatalogPointerV1> {
        self.active_catalog.as_ref()
    }

    /// Returns the last included authoritative application commit, if any.
    #[must_use]
    pub const fn last_commit_sequence(&self) -> Option<CommitSequence> {
        self.last_commit_sequence
    }

    /// Returns checksums in canonical artifact-name order.
    #[must_use]
    pub fn checksums(&self) -> &[BackupArtifactChecksumV1] {
        &self.checksums
    }

    /// Borrows the captured build metadata.
    #[must_use]
    pub const fn build(&self) -> &BackupBuildMetadataV1 {
        &self.build
    }

    /// Returns checked aggregate semantic bytes under the per-field maxima.
    ///
    /// This is overflow evidence, not a new normative artifact-size limit.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }
}

/// Restore overwrite policy selected only by explicit operator action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineRestoreOverwritePolicyV1 {
    /// Refuse any nonempty target directory.
    RefuseNonEmpty,
    /// Operator explicitly selected destructive replacement.
    ExplicitlyAllowDestructive,
}

/// Closed result of a backend-private, source/target-bound offline restore.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfflineRestoreResultV1 {
    /// The artifact was restored and its semantic manifest was decoded.
    ///
    /// This does not establish structural integrity, catalog validity, or
    /// readiness. The restored database must still enter the normal exclusive
    /// startup-evidence and catalog-validation path.
    Restored {
        /// Checked value-level summary decoded from the restored artifact.
        manifest: Box<OfflineBackupManifestV1>,
    },
    /// The target was nonempty and destructive replacement was not explicit.
    TargetNotEmpty,
}

/// Semantic operation implemented by a private, target-bound offline adapter.
///
/// The implementing receiver owns all destination and I/O details. No concrete
/// handle, path, sink, artifact bytes, encoding, or callback crosses this port.
pub trait OfflineBackupPersistencePort {
    /// Exclusively creates one complete consistent backup and returns its
    /// checked semantic manifest summary.
    fn create_offline_backup(
        &mut self,
        build: &BackupBuildMetadataV1,
    ) -> Result<OfflineBackupManifestV1, StorageError>;
}

/// Semantic operation implemented by a private, source/target-bound adapter.
///
/// A successful result still requires ordinary startup structural evidence and
/// catalog validation before any readiness claim or operational port release.
pub trait OfflineRestorePersistencePort {
    /// Exclusively restores the already-bound source into the already-bound
    /// target under the explicit overwrite policy.
    fn restore_offline_backup(
        &mut self,
        overwrite_policy: OfflineRestoreOverwritePolicyV1,
    ) -> Result<OfflineRestoreResultV1, StorageError>;
}

fn checked_build_value(value: String) -> Result<String, StorageValueError> {
    if value.is_empty() {
        return Err(StorageValueError::Empty);
    }
    if value.len() > MAX_BACKUP_BUILD_VALUE_BYTES {
        return Err(StorageValueError::LimitExceeded);
    }
    if value.bytes().any(|byte| !(0x21..=0x7e).contains(&byte)) {
        return Err(StorageValueError::InvalidShape);
    }
    Ok(value)
}

fn checked_feature(value: &str) -> Result<(), StorageValueError> {
    if value.is_empty() {
        return Err(StorageValueError::Empty);
    }
    if value.len() > MAX_BACKUP_BUILD_FEATURE_BYTES {
        return Err(StorageValueError::LimitExceeded);
    }
    if value
        .bytes()
        .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_'))
    {
        return Err(StorageValueError::InvalidShape);
    }
    Ok(())
}

fn backup_manifest_semantic_bytes(
    catalog_bundles: &[BackupCatalogBundleV1],
    active_catalog: Option<&ActiveCatalogPointerV1>,
    last_commit_sequence: Option<CommitSequence>,
    checksums: &[BackupArtifactChecksumV1],
    build: &BackupBuildMetadataV1,
) -> Result<usize, StorageValueError> {
    let bundles = catalog_bundles.iter().try_fold(4usize, |total, bundle| {
        total
            .checked_add(framed_backup_bytes(catalog_identity_semantic_bytes(
                bundle.lineage.as_bytes().len(),
                bundle.contract_version.to_be_bytes().len(),
            )?)?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let checksums = checksums.iter().try_fold(4usize, |total, checksum| {
        let value = 4usize
            .checked_add(framed_backup_bytes(checksum.checksum.as_bytes().len())?)
            .ok_or(StorageValueError::SizeOverflow)?;
        total
            .checked_add(framed_backup_bytes(value)?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let features = build
        .enabled_features
        .iter()
        .try_fold(4usize, |total, feature| {
            total
                .checked_add(framed_backup_bytes(feature.len())?)
                .ok_or(StorageValueError::SizeOverflow)
        })?;
    let active = match active_catalog {
        Some(pointer) => 1usize
            .checked_add(catalog_identity_semantic_bytes(
                pointer.lineage().as_bytes().len(),
                pointer.contract_version().to_be_bytes().len(),
            )?)
            .ok_or(StorageValueError::SizeOverflow)?,
        None => 1,
    };
    checked_backup_sum([
        4,  // manifest version
        4,  // storage-format version
        16, // database ID
        1,  // snapshot kind
        bundles,
        active,
        1 + last_commit_sequence.map_or(0, |_| 8),
        checksums,
        framed_backup_bytes(build.semantic_version.len())?,
        framed_backup_bytes(build.git_revision.len())?,
        framed_backup_bytes(build.rust_version.len())?,
        4,
        features,
    ])
}

fn catalog_identity_semantic_bytes(
    lineage_bytes: usize,
    version_bytes: usize,
) -> Result<usize, StorageValueError> {
    checked_backup_sum([framed_backup_bytes(lineage_bytes)?, version_bytes, 32])
}

fn framed_backup_bytes(content_bytes: usize) -> Result<usize, StorageValueError> {
    4usize
        .checked_add(content_bytes)
        .ok_or(StorageValueError::SizeOverflow)
}

fn checked_backup_sum(parts: impl IntoIterator<Item = usize>) -> Result<usize, StorageValueError> {
    parts.into_iter().try_fold(0usize, |total, part| {
        total
            .checked_add(part)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_artifact_identity_is_opaque_and_one_based() {
        let value = BackupIntegrityChecksumV1::new(vec![0x55; 32]).expect("checksum");
        let checksum = BackupArtifactChecksumV1::new(NonZeroU32::MIN, value);

        assert_eq!(checksum.artifact_ordinal(), NonZeroU32::MIN);
        assert_eq!(checksum.checksum().as_bytes(), &[0x55; 32]);
    }

    #[test]
    fn build_features_are_canonicalized() {
        let metadata = BackupBuildMetadataV1::new(
            "0.1.0",
            "0123456789abcdef",
            "rustc-1.97.0",
            1,
            vec!["zeta".to_owned(), "alpha".to_owned()],
        )
        .expect("build metadata");

        assert_eq!(metadata.enabled_features(), &["alpha", "zeta"]);
    }

    #[test]
    fn absent_active_catalog_requires_empty_history_and_no_commits() {
        let checksum = BackupArtifactChecksumV1::new(
            NonZeroU32::MIN,
            BackupIntegrityChecksumV1::new(vec![0x55; 32]).expect("checksum"),
        );
        let build =
            BackupBuildMetadataV1::new("0.1.0", "0123456789abcdef", "rustc-1.97.0", 1, Vec::new())
                .expect("build metadata");

        assert_eq!(
            OfflineBackupManifestV1::new(
                StorageFormatVersion::V1,
                DatabaseId::from_bytes([
                    0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x03,
                ])
                .expect("valid UUIDv7"),
                BackupSnapshotKindV1::StorageEngineData,
                Vec::new(),
                None,
                Some(CommitSequence::first()),
                vec![checksum],
                build,
            ),
            Err(StorageValueError::InvalidShape)
        );
    }
}
