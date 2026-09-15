//! Read-only binding of an archive to an independently verified physical backup.
use super::{RedbArchiveRepository, path_guard::PinnedDirectory};
use crate::{backup, error::storage_error};
use redb::ReadableDatabase;
use riffdb_storage_api::{
    ArchiveConsumerErrorV1, ArchiveEncryptionPostureV1, ChangelogHistoryStateV3,
    OfflineBackupManifestIdentityV1, OfflineBackupManifestV1, StorageError, StorageErrorKind,
};
use std::{ffi::OsStr, fs::File, path::Path};

const NAMES: [&str; 4] = [
    backup::MANIFEST_FILE_NAME,
    backup::DATABASE_ARTIFACT_FILE_NAME,
    backup::JOURNAL_ARTIFACT_FILE_NAME,
    backup::FORMAT_MARKER_ARTIFACT_FILE_NAME,
];

/// Exact manifest/artifact checksums, compatible format, catalog inventory and
/// retained V3 receipt-chain binding under a read-only engine lock. This is not
/// a complete catalog-semantic startup proof or a serving/write capability.
pub struct RedbVerifiedArchiveBackup {
    directory: PinnedDirectory,
    files: [File; 4],
    _database: redb::ReadOnlyDatabase,
    manifest: OfflineBackupManifestV1,
    identity: OfflineBackupManifestIdentityV1,
    history: ChangelogHistoryStateV3,
    digest: [u8; 32],
}

impl RedbVerifiedArchiveBackup {
    /// Opens only existing regular files. The backup is never repaired, migrated
    /// or journal-replayed, and no source lifecycle or receipt is originated.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let directory = PinnedDirectory::open(path)?;
        let files = [
            directory.open_file(OsStr::new(NAMES[0]))?.into_std(),
            directory.open_file(OsStr::new(NAMES[1]))?.into_std(),
            directory.open_file(OsStr::new(NAMES[2]))?.into_std(),
            directory.open_file(OsStr::new(NAMES[3]))?.into_std(),
        ];
        let database = read_only_database(&files[1])?;
        let (manifest, identity) = backup::validate_immutable_backup(path)?;
        if !manifest.is_physically_restorable_by_current_binary() {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        backup::validate_readable_database_semantics(&database, &manifest)?;
        let read = database.begin_read().map_err(unavailable)?;
        let history =
            crate::changelog_v3_roots::validate_retained_history(&read)?.ok_or_else(corrupt)?;
        if crate::follower_lifecycle::is_attached(&read)?
            || manifest.history_incarnation() != Some(history.lineage().history_incarnation())
            || manifest.database_id() != history.lineage().database_id()
        {
            return Err(corrupt());
        }
        // The manifest names the last retained command, which can be absent
        // after complete pruning. Its facts were checked above; the V3 roots
        // independently bind the physical allocator and complete applied fence.
        drop(read);
        let digest = identity
            .manifest_checksum()
            .as_bytes()
            .try_into()
            .map_err(|_| corrupt())?;
        let value = Self {
            directory,
            files,
            _database: database,
            manifest,
            identity,
            history,
            digest,
        };
        value.verify()?;
        Ok(value)
    }

    /// Returns the source receipt fence derived from the verified backup engine.
    #[must_use]
    pub const fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }

    /// Returns the original unmodified full-backup manifest.
    #[must_use]
    pub const fn manifest(&self) -> &OfflineBackupManifestV1 {
        &self.manifest
    }

    /// SHA-256 of the exact verified V1 manifest bytes.
    #[must_use]
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Rechecks retained files before touching the archive. Encryption posture
    /// is an operator declaration; this method performs no encryption.
    pub fn open_archive(
        &self,
        path: &Path,
        encryption: ArchiveEncryptionPostureV1,
    ) -> Result<RedbArchiveRepository, ArchiveConsumerErrorV1> {
        self.verify().map_err(|error| match error.kind() {
            StorageErrorKind::Unavailable => ArchiveConsumerErrorV1::SinkUnavailable,
            _ => ArchiveConsumerErrorV1::InvalidManifest,
        })?;
        RedbArchiveRepository::open(
            path,
            self.history.lineage(),
            self.history.tail(),
            self.digest,
            encryption,
        )
    }

    fn verify(&self) -> Result<(), StorageError> {
        self.directory.verify()?;
        if self.manifest.checksums().len() != 3 {
            return Err(corrupt());
        }
        for (index, file) in self.files.iter().enumerate() {
            let name = OsStr::new(NAMES[index]);
            if !self.directory.regular_file_matches(name, file)? {
                return Err(corrupt());
            }
            let expected = if index == 0 {
                self.identity.manifest_checksum()
            } else {
                self.manifest.checksums()[index - 1].checksum()
            };
            // A fresh capability-opened descriptor has its own offset; cloned
            // descriptors would race concurrent verification reads.
            let input = self.directory.open_file(name)?.into_std();
            if !self.directory.regular_file_matches(name, &input)?
                || &backup::sha256_reader(input)? != expected
                || !self.directory.regular_file_matches(name, file)?
            {
                return Err(corrupt());
            }
        }
        self.directory.verify()
    }
}

#[cfg(target_os = "linux")]
pub(in crate::maintenance) fn read_only_database(
    file: &File,
) -> Result<redb::ReadOnlyDatabase, StorageError> {
    use std::os::fd::AsRawFd;
    // Same retained-inode read-only open used by bootstrap publication.
    redb::Database::builder()
        .set_cache_size(64 * 1024 * 1024)
        .open_read_only(format!("/proc/self/fd/{}", file.as_raw_fd()))
        .map_err(unavailable)
}

#[cfg(not(target_os = "linux"))]
pub(in crate::maintenance) fn read_only_database(
    _: &File,
) -> Result<redb::ReadOnlyDatabase, StorageError> {
    Err(storage_error(StorageErrorKind::IncompatibleFormat))
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn unavailable<T>(_: T) -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
