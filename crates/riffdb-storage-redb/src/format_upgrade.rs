//! Restartable, backup-bound offline durable-format transition.

use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use riffdb_storage_api::{
    BackupIntegrityChecksumV1, CompatibilityFixtureDigest, DURABLE_FORMAT_UPGRADE_RECEIPT_VERSION,
    DurableFormatAction, DurableFormatIdentity, OfflineBackupManifestIdentityV1, StorageError,
    current_durable_format_manifest, preflight_durable_format,
};
use riffdb_types::DatabaseId;
use sha2::{Digest, Sha256};

use crate::backup::{validate_exact_source_matches_backup, validate_immutable_backup};
use crate::format_preflight::{
    RedbDurableFormatPreflight, RedbDurableFormatPreflightError, preflight_durable_format_path,
    publish_upgraded_current_marker,
};
use crate::store::RedbStore;

const RECEIPT_SUFFIX: &str = ".riffdb-format-upgrade-v1";
const RECEIPT_STAGING_SUFFIX: &str = ".riffdb-format-upgrade-v1.staging";
const RECEIPT_MAGIC: [u8; 8] = *b"RDBUPG01";
const RECEIPT_PREFIX_BYTES: usize = 8 + 2 + 1 + 4 + 4 + 4 + 4 + 16 + 32 + 32;
const RECEIPT_BYTES: usize = RECEIPT_PREFIX_BYTES + 32;

/// Durable progress through the one-way offline transition.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RedbDurableFormatUpgradePhase {
    /// Exact predecessor bytes and immutable backup were matched before mutation.
    Accepted = 1,
    /// Restartable storage migrations and journal recovery completed.
    Migrated = 2,
    /// The current marker was durably published and revalidated.
    Complete = 3,
}

/// Checksummed, source/target/backup-bound transition receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedbDurableFormatUpgradeReceipt {
    phase: RedbDurableFormatUpgradePhase,
    source: DurableFormatIdentity,
    target: DurableFormatIdentity,
    database_id: DatabaseId,
    backup_manifest_checksum: BackupIntegrityChecksumV1,
    compatibility_fixture_digest: CompatibilityFixtureDigest,
}

impl RedbDurableFormatUpgradeReceipt {
    /// Returns the durably reached transition phase.
    #[must_use]
    pub const fn phase(&self) -> RedbDurableFormatUpgradePhase {
        self.phase
    }

    /// Returns the exact predecessor identity.
    #[must_use]
    pub const fn source(&self) -> DurableFormatIdentity {
        self.source
    }

    /// Returns this binary's target identity.
    #[must_use]
    pub const fn target(&self) -> DurableFormatIdentity {
        self.target
    }

    /// Returns the database identity authenticated by the backup.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Borrows the exact immutable backup-manifest checksum.
    #[must_use]
    pub const fn backup_manifest_checksum(&self) -> &BackupIntegrityChecksumV1 {
        &self.backup_manifest_checksum
    }

    /// Returns the compatibility corpus authorizing the transition.
    #[must_use]
    pub const fn compatibility_fixture_digest(&self) -> CompatibilityFixtureDigest {
        self.compatibility_fixture_digest
    }

    fn with_phase(&self, phase: RedbDurableFormatUpgradePhase) -> Self {
        Self {
            phase,
            source: self.source,
            target: self.target,
            database_id: self.database_id,
            backup_manifest_checksum: self.backup_manifest_checksum.clone(),
            compatibility_fixture_digest: self.compatibility_fixture_digest,
        }
    }
}

/// Whether this invocation performed or merely observed the transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedbDurableFormatUpgradeDisposition {
    /// The database already carried the exact current marker.
    AlreadyCurrent,
    /// This invocation admitted and completed the transition.
    Upgraded,
    /// This invocation resumed or completed an earlier admitted transition.
    Reconciled,
}

/// Complete result of one offline transition attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedbDurableFormatUpgradeResult {
    disposition: RedbDurableFormatUpgradeDisposition,
    receipt: Option<RedbDurableFormatUpgradeReceipt>,
}

impl RedbDurableFormatUpgradeResult {
    /// Returns how this invocation related to the transition.
    #[must_use]
    pub const fn disposition(&self) -> RedbDurableFormatUpgradeDisposition {
        self.disposition
    }

    /// Borrows the durable receipt when a transition was admitted.
    #[must_use]
    pub const fn receipt(&self) -> Option<&RedbDurableFormatUpgradeReceipt> {
        self.receipt.as_ref()
    }
}

/// Safe closed failures; no arm authorizes force, ignore, reset, or downgrade.
#[derive(Debug)]
pub enum RedbDurableFormatUpgradeError {
    /// Source-free marker/path inspection failed before redb open.
    Preflight(RedbDurableFormatPreflightError),
    /// No manifest edge authorizes the requested transition.
    UnsupportedAction,
    /// The supplied immutable backup did not validate.
    BackupInvalid(StorageError),
    /// The backup is valid but does not authenticate the live predecessor.
    BackupDoesNotMatchSource(StorageError),
    /// A retained receipt is malformed or bound to another operation.
    ReceiptInvalid,
    /// Restartable storage migration or recovery failed closed.
    Storage(StorageError),
    /// Receipt or filesystem durability was unavailable.
    Unavailable,
}

impl fmt::Display for RedbDurableFormatUpgradeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Preflight(error) => write!(formatter, "durable-format preflight failed: {error}"),
            Self::UnsupportedAction => {
                formatter.write_str("the durable-format manifest does not authorize this upgrade")
            }
            Self::BackupInvalid(_) => formatter.write_str(
                "the required immutable backup is invalid or incompatible; no upgrade was authorized",
            ),
            Self::BackupDoesNotMatchSource(_) => formatter.write_str(
                "the immutable backup does not exactly match the closed predecessor database; no upgrade was authorized",
            ),
            Self::ReceiptInvalid => formatter.write_str(
                "the durable-format upgrade receipt is invalid or does not match this database and backup",
            ),
            Self::Storage(_) => formatter.write_str("the durable-format upgrade failed closed"),
            Self::Unavailable => formatter.write_str("the durable-format upgrade path is unavailable"),
        }
    }
}

impl Error for RedbDurableFormatUpgradeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Preflight(error) => Some(error),
            Self::BackupInvalid(error)
            | Self::BackupDoesNotMatchSource(error)
            | Self::Storage(error) => Some(error),
            Self::UnsupportedAction | Self::ReceiptInvalid | Self::Unavailable => None,
        }
    }
}

/// Offline operation bound to one database and one already-published backup.
pub struct RedbDurableFormatUpgrade {
    database_path: PathBuf,
    backup_directory: PathBuf,
}

impl RedbDurableFormatUpgrade {
    /// Binds one closed database and one immutable backup directory.
    #[must_use]
    pub fn bind(database_path: impl AsRef<Path>, backup_directory: impl AsRef<Path>) -> Self {
        Self {
            database_path: database_path.as_ref().to_path_buf(),
            backup_directory: backup_directory.as_ref().to_path_buf(),
        }
    }

    /// Executes or resumes the only manifest-authorized offline transition.
    pub fn run(&self) -> Result<RedbDurableFormatUpgradeResult, RedbDurableFormatUpgradeError> {
        let observed = preflight_durable_format_path(&self.database_path)
            .map_err(RedbDurableFormatUpgradeError::Preflight)?;
        let receipt_path = durable_format_upgrade_receipt_path(&self.database_path);
        let retained_receipt = read_optional_receipt(&receipt_path)?;

        if observed == RedbDurableFormatPreflight::OpenCurrent {
            let Some(receipt) = retained_receipt else {
                return Ok(RedbDurableFormatUpgradeResult {
                    disposition: RedbDurableFormatUpgradeDisposition::AlreadyCurrent,
                    receipt: None,
                });
            };
            let (_, backup_identity) = validate_immutable_backup(&self.backup_directory)
                .map_err(RedbDurableFormatUpgradeError::BackupInvalid)?;
            validate_receipt_for_current_marker(&receipt, &backup_identity)?;
            let complete = receipt.with_phase(RedbDurableFormatUpgradePhase::Complete);
            publish_receipt(&receipt_path, &complete)?;
            return Ok(RedbDurableFormatUpgradeResult {
                disposition: RedbDurableFormatUpgradeDisposition::Reconciled,
                receipt: Some(complete),
            });
        }

        let RedbDurableFormatPreflight::OfflineUpgradeRequired {
            current,
            binary,
            action,
        } = observed
        else {
            return Err(RedbDurableFormatUpgradeError::UnsupportedAction);
        };
        if !matches!(
            action,
            DurableFormatAction::OfflineInPlace {
                backup_required: true,
                downtime_required: true,
                one_way: true,
                ..
            }
        ) {
            return Err(RedbDurableFormatUpgradeError::UnsupportedAction);
        }

        let (backup_manifest, backup_identity) = validate_immutable_backup(&self.backup_directory)
            .map_err(RedbDurableFormatUpgradeError::BackupInvalid)?;
        let manifest = current_durable_format_manifest();
        if binary != manifest.identity()
            || backup_manifest.format_compatibility().is_some()
            || backup_identity.manifest_checksum().as_bytes().len() != 32
        {
            return Err(RedbDurableFormatUpgradeError::BackupInvalid(
                StorageError::new(
                    riffdb_storage_api::StorageErrorKind::IncompatibleFormat,
                    None,
                ),
            ));
        }

        let (receipt, resumed) = match retained_receipt {
            Some(receipt) => {
                validate_receipt_for_manifest(&receipt, &backup_identity)?;
                if receipt.source != current
                    || receipt.target != binary
                    || receipt.phase == RedbDurableFormatUpgradePhase::Complete
                {
                    return Err(RedbDurableFormatUpgradeError::ReceiptInvalid);
                }
                (receipt, true)
            }
            None => {
                validate_exact_source_matches_backup(&self.database_path, &backup_manifest)
                    .map_err(RedbDurableFormatUpgradeError::BackupDoesNotMatchSource)?;
                let receipt = RedbDurableFormatUpgradeReceipt {
                    phase: RedbDurableFormatUpgradePhase::Accepted,
                    source: current,
                    target: binary,
                    database_id: backup_identity.database_id(),
                    backup_manifest_checksum: backup_identity.manifest_checksum().clone(),
                    compatibility_fixture_digest: manifest.compatibility_fixture_digest(),
                };
                publish_receipt(&receipt_path, &receipt)?;
                (receipt, false)
            }
        };

        let store = RedbStore::open_for_durable_format_upgrade(&self.database_path, observed)
            .map_err(RedbDurableFormatUpgradeError::Storage)?;
        drop(store);
        let migrated = receipt.with_phase(RedbDurableFormatUpgradePhase::Migrated);
        publish_receipt(&receipt_path, &migrated)?;
        publish_upgraded_current_marker(&self.database_path, observed)
            .map_err(RedbDurableFormatUpgradeError::Preflight)?;
        if preflight_durable_format_path(&self.database_path)
            .map_err(RedbDurableFormatUpgradeError::Preflight)?
            != RedbDurableFormatPreflight::OpenCurrent
        {
            return Err(RedbDurableFormatUpgradeError::ReceiptInvalid);
        }
        let complete = migrated.with_phase(RedbDurableFormatUpgradePhase::Complete);
        publish_receipt(&receipt_path, &complete)?;
        Ok(RedbDurableFormatUpgradeResult {
            disposition: if resumed {
                RedbDurableFormatUpgradeDisposition::Reconciled
            } else {
                RedbDurableFormatUpgradeDisposition::Upgraded
            },
            receipt: Some(complete),
        })
    }
}

/// Returns the fixed sibling receipt path for one database.
#[must_use]
pub fn durable_format_upgrade_receipt_path(database_path: &Path) -> PathBuf {
    sibling_with_suffix(database_path, RECEIPT_SUFFIX)
}

fn validate_receipt_binding(
    receipt: &RedbDurableFormatUpgradeReceipt,
    backup: &OfflineBackupManifestIdentityV1,
) -> Result<(), RedbDurableFormatUpgradeError> {
    if receipt.database_id != backup.database_id()
        || &receipt.backup_manifest_checksum != backup.manifest_checksum()
    {
        return Err(RedbDurableFormatUpgradeError::ReceiptInvalid);
    }
    Ok(())
}

fn validate_receipt_for_manifest(
    receipt: &RedbDurableFormatUpgradeReceipt,
    backup: &OfflineBackupManifestIdentityV1,
) -> Result<(), RedbDurableFormatUpgradeError> {
    validate_receipt_binding(receipt, backup)?;
    let manifest = current_durable_format_manifest();
    if receipt.target != manifest.identity()
        || receipt.compatibility_fixture_digest != manifest.compatibility_fixture_digest()
        || !matches!(
            preflight_durable_format(receipt.source),
            Ok(DurableFormatAction::OfflineInPlace {
                backup_required: true,
                downtime_required: true,
                one_way: true,
                ..
            })
        )
    {
        return Err(RedbDurableFormatUpgradeError::ReceiptInvalid);
    }
    Ok(())
}

fn validate_receipt_for_current_marker(
    receipt: &RedbDurableFormatUpgradeReceipt,
    backup: &OfflineBackupManifestIdentityV1,
) -> Result<(), RedbDurableFormatUpgradeError> {
    validate_receipt_for_manifest(receipt, backup)?;
    if receipt.phase == RedbDurableFormatUpgradePhase::Accepted {
        return Err(RedbDurableFormatUpgradeError::ReceiptInvalid);
    }
    Ok(())
}

fn read_optional_receipt(
    path: &Path,
) -> Result<Option<RedbDurableFormatUpgradeReceipt>, RedbDurableFormatUpgradeError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(RedbDurableFormatUpgradeError::Unavailable),
    };
    let metadata = file
        .metadata()
        .map_err(|_| RedbDurableFormatUpgradeError::Unavailable)?;
    if metadata.len() != u64::try_from(RECEIPT_BYTES).expect("receipt length fits u64") {
        return Err(RedbDurableFormatUpgradeError::ReceiptInvalid);
    }
    let mut encoded = [0_u8; RECEIPT_BYTES];
    file.read_exact(&mut encoded)
        .map_err(|_| RedbDurableFormatUpgradeError::Unavailable)?;
    decode_receipt(&encoded).map(Some)
}

fn publish_receipt(
    path: &Path,
    receipt: &RedbDurableFormatUpgradeReceipt,
) -> Result<(), RedbDurableFormatUpgradeError> {
    let staging = sibling_with_suffix(path, RECEIPT_STAGING_SUFFIX);
    match fs::remove_file(&staging) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(RedbDurableFormatUpgradeError::Unavailable),
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&staging)
        .map_err(|_| RedbDurableFormatUpgradeError::Unavailable)?;
    let result = (|| {
        file.write_all(&encode_receipt(receipt)?)
            .map_err(|_| RedbDurableFormatUpgradeError::Unavailable)?;
        file.sync_all()
            .map_err(|_| RedbDurableFormatUpgradeError::Unavailable)?;
        drop(file);
        fs::rename(&staging, path).map_err(|_| RedbDurableFormatUpgradeError::Unavailable)?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(staging);
    }
    result
}

fn encode_receipt(
    receipt: &RedbDurableFormatUpgradeReceipt,
) -> Result<[u8; RECEIPT_BYTES], RedbDurableFormatUpgradeError> {
    let backup = receipt.backup_manifest_checksum.as_bytes();
    if backup.len() != 32 {
        return Err(RedbDurableFormatUpgradeError::ReceiptInvalid);
    }
    let mut encoded = [0_u8; RECEIPT_BYTES];
    encoded[..8].copy_from_slice(&RECEIPT_MAGIC);
    encoded[8..10].copy_from_slice(&DURABLE_FORMAT_UPGRADE_RECEIPT_VERSION.to_be_bytes());
    encoded[10] = receipt.phase as u8;
    encoded[11..15].copy_from_slice(&receipt.source.epoch().get().to_be_bytes());
    encoded[15..19].copy_from_slice(&receipt.source.writer().get().to_be_bytes());
    encoded[19..23].copy_from_slice(&receipt.target.epoch().get().to_be_bytes());
    encoded[23..27].copy_from_slice(&receipt.target.writer().get().to_be_bytes());
    encoded[27..43].copy_from_slice(receipt.database_id.as_bytes());
    encoded[43..75].copy_from_slice(backup);
    encoded[75..107].copy_from_slice(receipt.compatibility_fixture_digest.as_bytes());
    let checksum: [u8; 32] = Sha256::digest(&encoded[..RECEIPT_PREFIX_BYTES]).into();
    encoded[RECEIPT_PREFIX_BYTES..].copy_from_slice(&checksum);
    Ok(encoded)
}

fn decode_receipt(
    encoded: &[u8; RECEIPT_BYTES],
) -> Result<RedbDurableFormatUpgradeReceipt, RedbDurableFormatUpgradeError> {
    let expected_checksum: [u8; 32] = Sha256::digest(&encoded[..RECEIPT_PREFIX_BYTES]).into();
    if encoded[..8] != RECEIPT_MAGIC
        || u16::from_be_bytes([encoded[8], encoded[9]]) != DURABLE_FORMAT_UPGRADE_RECEIPT_VERSION
        || encoded[RECEIPT_PREFIX_BYTES..] != expected_checksum
    {
        return Err(RedbDurableFormatUpgradeError::ReceiptInvalid);
    }
    let phase = match encoded[10] {
        1 => RedbDurableFormatUpgradePhase::Accepted,
        2 => RedbDurableFormatUpgradePhase::Migrated,
        3 => RedbDurableFormatUpgradePhase::Complete,
        _ => return Err(RedbDurableFormatUpgradeError::ReceiptInvalid),
    };
    let epoch = |start: usize| {
        riffdb_storage_api::AlphaFormatEpoch::new(u32::from_be_bytes(
            encoded[start..start + 4]
                .try_into()
                .map_err(|_| RedbDurableFormatUpgradeError::ReceiptInvalid)?,
        ))
        .ok_or(RedbDurableFormatUpgradeError::ReceiptInvalid)
    };
    let writer = |start: usize| {
        Ok::<_, RedbDurableFormatUpgradeError>(riffdb_storage_api::DurableFormatWriter::new(
            u32::from_be_bytes(
                encoded[start..start + 4]
                    .try_into()
                    .map_err(|_| RedbDurableFormatUpgradeError::ReceiptInvalid)?,
            ),
        ))
    };
    let source = DurableFormatIdentity::new(epoch(11)?, writer(15)?);
    let target = DurableFormatIdentity::new(epoch(19)?, writer(23)?);
    let database_id = DatabaseId::from_bytes(
        encoded[27..43]
            .try_into()
            .map_err(|_| RedbDurableFormatUpgradeError::ReceiptInvalid)?,
    )
    .map_err(|_| RedbDurableFormatUpgradeError::ReceiptInvalid)?;
    let backup_manifest_checksum = BackupIntegrityChecksumV1::new(encoded[43..75].to_vec())
        .map_err(|_| RedbDurableFormatUpgradeError::ReceiptInvalid)?;
    let compatibility_fixture_digest = CompatibilityFixtureDigest::from_bytes(
        encoded[75..107]
            .try_into()
            .map_err(|_| RedbDurableFormatUpgradeError::ReceiptInvalid)?,
    );
    Ok(RedbDurableFormatUpgradeReceipt {
        phase,
        source,
        target,
        database_id,
        backup_manifest_checksum,
        compatibility_fixture_digest,
    })
}

fn sibling_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn sync_parent(path: &Path) -> Result<(), RedbDurableFormatUpgradeError> {
    let parent = path
        .parent()
        .ok_or(RedbDurableFormatUpgradeError::Unavailable)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| RedbDurableFormatUpgradeError::Unavailable)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use riffdb_storage_api::{
        BackupBuildMetadataV1, DatabaseIdentityProbePort, DatabaseInitializationPort,
    };

    use super::*;

    static NEXT: AtomicU64 = AtomicU64::new(1);

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let ordinal = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "riffdb-format-upgrade-{label}-{}-{ordinal}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("create test root");
            Self(path)
        }

        fn join(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn database_id(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10])
            .expect("database ID")
    }

    fn build() -> BackupBuildMetadataV1 {
        BackupBuildMetadataV1::new(
            "0.0.0-pre-manifest",
            "predecessor",
            "rustc-1.97.0",
            1,
            Vec::new(),
        )
        .expect("build metadata")
    }

    fn predecessor_fixture(root: &TestRoot, seed: u8) -> (PathBuf, PathBuf) {
        let database = root.join("application.redb");
        let backup = root.join("verified-backup");
        let mut store = RedbStore::open(&database).expect("create current database");
        store
            .initialize_database(database_id(seed))
            .expect("initialize database");
        assert_eq!(
            store.probe_database_identity().expect("probe identity"),
            riffdb_storage_api::DatabaseIdentityProbe::Existing(database_id(seed))
        );
        drop(store);
        fs::remove_file(crate::durable_format_marker_path(&database))
            .expect("remove marker to model predecessor release");
        crate::backup::create_pre_format_compatibility_backup_fixture(&database, &backup, &build())
            .expect("create verified predecessor backup");
        (database, backup)
    }

    #[test]
    fn exact_predecessor_upgrade_is_backup_bound_receipted_and_reopenable() {
        let root = TestRoot::new("complete");
        let (database, backup) = predecessor_fixture(&root, 0x91);

        let result = RedbDurableFormatUpgrade::bind(&database, &backup)
            .run()
            .expect("upgrade predecessor");
        assert_eq!(
            result.disposition(),
            RedbDurableFormatUpgradeDisposition::Upgraded
        );
        let receipt = result.receipt().expect("upgrade receipt");
        assert_eq!(receipt.phase(), RedbDurableFormatUpgradePhase::Complete);
        assert_eq!(receipt.database_id(), database_id(0x91));
        assert_eq!(
            preflight_durable_format_path(&database),
            Ok(RedbDurableFormatPreflight::OpenCurrent)
        );
        assert!(durable_format_upgrade_receipt_path(&database).is_file());

        let reopened = RedbStore::open(&database).expect("reopen upgraded database");
        assert_eq!(
            reopened
                .probe_database_identity()
                .expect("probe upgraded database"),
            riffdb_storage_api::DatabaseIdentityProbe::Existing(database_id(0x91))
        );
    }

    #[test]
    fn mismatched_backup_refuses_before_receipt_marker_or_source_mutation() {
        let root = TestRoot::new("mismatch");
        let (database, _) = predecessor_fixture(&root, 0x91);
        let other_root = TestRoot::new("other");
        let (_, other_backup) = predecessor_fixture(&other_root, 0x92);
        let before = fs::read(&database).expect("read predecessor bytes");

        assert!(matches!(
            RedbDurableFormatUpgrade::bind(&database, &other_backup).run(),
            Err(RedbDurableFormatUpgradeError::BackupDoesNotMatchSource(_))
                | Err(RedbDurableFormatUpgradeError::ReceiptInvalid)
        ));
        assert_eq!(fs::read(&database).expect("re-read predecessor"), before);
        assert!(!crate::durable_format_marker_path(&database).exists());
        assert!(!durable_format_upgrade_receipt_path(&database).exists());
    }

    #[test]
    fn accepted_receipt_resumes_after_storage_migration_before_marker() {
        let root = TestRoot::new("resume");
        let (database, backup) = predecessor_fixture(&root, 0x91);
        let observed = preflight_durable_format_path(&database).expect("preflight predecessor");
        let RedbDurableFormatPreflight::OfflineUpgradeRequired {
            current, binary, ..
        } = observed
        else {
            panic!("predecessor transition expected");
        };
        let (_, backup_identity) =
            validate_immutable_backup(&backup).expect("validate predecessor backup");
        let receipt = RedbDurableFormatUpgradeReceipt {
            phase: RedbDurableFormatUpgradePhase::Accepted,
            source: current,
            target: binary,
            database_id: backup_identity.database_id(),
            backup_manifest_checksum: backup_identity.manifest_checksum().clone(),
            compatibility_fixture_digest: current_durable_format_manifest()
                .compatibility_fixture_digest(),
        };
        publish_receipt(&durable_format_upgrade_receipt_path(&database), &receipt)
            .expect("publish admitted receipt");
        drop(
            RedbStore::open_for_durable_format_upgrade(&database, observed)
                .expect("perform migration before simulated crash"),
        );
        assert!(!crate::durable_format_marker_path(&database).exists());

        let result = RedbDurableFormatUpgrade::bind(&database, &backup)
            .run()
            .expect("resume admitted transition");
        assert_eq!(
            result.disposition(),
            RedbDurableFormatUpgradeDisposition::Reconciled
        );
        assert_eq!(
            result.receipt().map(RedbDurableFormatUpgradeReceipt::phase),
            Some(RedbDurableFormatUpgradePhase::Complete)
        );
        assert_eq!(
            preflight_durable_format_path(&database),
            Ok(RedbDurableFormatPreflight::OpenCurrent)
        );
    }

    #[test]
    fn current_marker_reconciles_a_pre_complete_receipt() {
        let root = TestRoot::new("marker-before-complete");
        let (database, backup) = predecessor_fixture(&root, 0x91);
        let upgraded = RedbDurableFormatUpgrade::bind(&database, &backup)
            .run()
            .expect("complete initial upgrade");
        let migrated = upgraded
            .receipt()
            .expect("retained receipt")
            .with_phase(RedbDurableFormatUpgradePhase::Migrated);
        publish_receipt(&durable_format_upgrade_receipt_path(&database), &migrated)
            .expect("model crash before complete receipt");

        let reconciled = RedbDurableFormatUpgrade::bind(&database, &backup)
            .run()
            .expect("reconcile current marker");
        assert_eq!(
            reconciled.disposition(),
            RedbDurableFormatUpgradeDisposition::Reconciled
        );
        assert_eq!(
            reconciled
                .receipt()
                .map(RedbDurableFormatUpgradeReceipt::phase),
            Some(RedbDurableFormatUpgradePhase::Complete)
        );
    }

    #[test]
    fn receipt_phase_must_match_the_observed_marker_state() {
        let root = TestRoot::new("phase-state");
        let (database, backup) = predecessor_fixture(&root, 0x91);
        let upgraded = RedbDurableFormatUpgrade::bind(&database, &backup)
            .run()
            .expect("complete initial upgrade");
        let accepted = upgraded
            .receipt()
            .expect("retained receipt")
            .with_phase(RedbDurableFormatUpgradePhase::Accepted);
        publish_receipt(&durable_format_upgrade_receipt_path(&database), &accepted)
            .expect("model invalid accepted/current pairing");

        assert!(matches!(
            RedbDurableFormatUpgrade::bind(&database, &backup).run(),
            Err(RedbDurableFormatUpgradeError::ReceiptInvalid)
        ));

        let other_root = TestRoot::new("complete-without-marker");
        let (other_database, other_backup) = predecessor_fixture(&other_root, 0x92);
        let observed =
            preflight_durable_format_path(&other_database).expect("preflight predecessor");
        let RedbDurableFormatPreflight::OfflineUpgradeRequired {
            current, binary, ..
        } = observed
        else {
            panic!("predecessor transition expected");
        };
        let (_, backup_identity) =
            validate_immutable_backup(&other_backup).expect("validate predecessor backup");
        let complete = RedbDurableFormatUpgradeReceipt {
            phase: RedbDurableFormatUpgradePhase::Complete,
            source: current,
            target: binary,
            database_id: backup_identity.database_id(),
            backup_manifest_checksum: backup_identity.manifest_checksum().clone(),
            compatibility_fixture_digest: current_durable_format_manifest()
                .compatibility_fixture_digest(),
        };
        publish_receipt(
            &durable_format_upgrade_receipt_path(&other_database),
            &complete,
        )
        .expect("model invalid complete/predecessor pairing");
        let before = fs::read(&other_database).expect("read predecessor bytes");

        assert!(matches!(
            RedbDurableFormatUpgrade::bind(&other_database, &other_backup).run(),
            Err(RedbDurableFormatUpgradeError::ReceiptInvalid)
        ));
        assert_eq!(
            fs::read(&other_database).expect("re-read predecessor bytes"),
            before
        );
        assert!(!crate::durable_format_marker_path(&other_database).exists());
    }
}
