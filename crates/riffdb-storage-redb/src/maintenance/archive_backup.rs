//! Read-only binding of an archive to an independently verified physical backup.
use super::{RedbArchiveRepository, path_guard::PinnedDirectory};
use crate::{backup, error::storage_error};
use redb::ReadableDatabase;
use riffdb_storage_api::{
    ArchiveConsumerErrorV1, ArchiveEncryptionPostureV1, ChangelogHistoryStateV3,
    OfflineBackupManifestIdentityV1, OfflineBackupManifestV1, StorageError, StorageErrorKind,
};
use std::{ffi::OsString, fs::File, path::Path};

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
    /// The fixed artifacts, then every sealed extent artifact (ADR-0263), by
    /// artifact ordinal after the manifest.
    files: Vec<(OsString, File)>,
    _database: redb::ReadOnlyDatabase,
    manifest: OfflineBackupManifestV1,
    identity: OfflineBackupManifestIdentityV1,
    history: ChangelogHistoryStateV3,
    fence_commit_timestamp: Option<riffdb_types::Timestamp>,
    digest: [u8; 32],
}

impl RedbVerifiedArchiveBackup {
    /// Opens only existing regular files. The backup is never repaired, migrated
    /// or journal-replayed, and no source lifecycle or receipt is originated.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let directory = PinnedDirectory::open(path)?;
        let names = NAMES
            .iter()
            .map(OsString::from)
            .chain(backup::sealed_artifact_names(path)?)
            .collect::<Vec<_>>();
        let files = names
            .into_iter()
            .map(|name| {
                let file = directory.open_file(&name)?.into_std();
                Ok((name, file))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        let database = read_only_database(&files[1].1)?;
        let (manifest, identity) = backup::validate_immutable_backup(path)?;
        // History stubs in the backup resolve from its own verified journal
        // and sealed extent artifacts (ADR-0263), for as long as it is read.
        let _history =
            crate::history_stub::HistoryResolver::register_backup(manifest.database_id(), path);
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
        let fence_commit_timestamp = match manifest.last_commit_sequence() {
            None => None,
            Some(fence_sequence) => {
                let commits = read
                    .open_table(crate::layout::COMMITS)
                    .map_err(|_| corrupt())?;
                let events = read
                    .open_table(crate::layout::EVENTS)
                    .map_err(|_| corrupt())?;
                let member =
                    crate::command_authority::command_member_at(&commits, &events, fence_sequence)?
                        .ok_or_else(corrupt)?;
                Some(member.base().commit().logical_time().timestamp())
            }
        };
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
            fence_commit_timestamp,
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

    /// Returns the coordinator commit timestamp of the backup's last retained
    /// application command, when present in the verified backup.
    #[must_use]
    pub const fn fence_commit_timestamp(&self) -> Option<riffdb_types::Timestamp> {
        self.fence_commit_timestamp
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

    pub(in crate::maintenance) fn verify_for_archive(&self) -> Result<(), ArchiveConsumerErrorV1> {
        self.verify().map_err(|error| match error.kind() {
            StorageErrorKind::Unavailable => ArchiveConsumerErrorV1::SinkUnavailable,
            _ => ArchiveConsumerErrorV1::InvalidManifest,
        })
    }

    /// Rechecks retained files before touching the archive. Encryption posture
    /// is an operator declaration; this method performs no encryption.
    pub fn open_archive(
        &self,
        path: &Path,
        encryption: ArchiveEncryptionPostureV1,
    ) -> Result<RedbArchiveRepository, ArchiveConsumerErrorV1> {
        self.verify_for_archive()?;
        RedbArchiveRepository::open(
            path,
            self.history.lineage(),
            self.history.tail(),
            self.digest,
            encryption,
        )
    }

    /// Opens a collector with cancellation between bounded archive validation
    /// steps. The original full-backup binding is rechecked before any sink I/O.
    pub fn open_archive_cancellable(
        &self,
        path: &Path,
        encryption: ArchiveEncryptionPostureV1,
        cancellation: &std::sync::atomic::AtomicBool,
    ) -> Result<RedbArchiveRepository, ArchiveConsumerErrorV1> {
        self.verify().map_err(|error| match error.kind() {
            StorageErrorKind::Unavailable => ArchiveConsumerErrorV1::SinkUnavailable,
            _ => ArchiveConsumerErrorV1::InvalidManifest,
        })?;
        RedbArchiveRepository::open_cancellable(
            path,
            self.history.lineage(),
            self.history.tail(),
            self.digest,
            encryption,
            cancellation,
        )
    }

    /// Restore-only counterpart: never creates a missing archive or lock file.
    pub(in crate::maintenance) fn open_existing_archive(
        &self,
        path: &Path,
        encryption: ArchiveEncryptionPostureV1,
    ) -> Result<RedbArchiveRepository, ArchiveConsumerErrorV1> {
        self.verify().map_err(|error| match error.kind() {
            StorageErrorKind::Unavailable => ArchiveConsumerErrorV1::SinkUnavailable,
            _ => ArchiveConsumerErrorV1::InvalidManifest,
        })?;
        RedbArchiveRepository::open_existing(
            path,
            self.history.lineage(),
            self.history.tail(),
            self.digest,
            encryption,
        )
    }

    fn verify(&self) -> Result<(), StorageError> {
        self.directory.verify()?;
        if self.manifest.checksums().len() + 1 != self.files.len() {
            return Err(corrupt());
        }
        for (index, (name, file)) in self.files.iter().enumerate() {
            let name = name.as_os_str();
            if !self.directory.regular_file_matches(name, file)? {
                return Err(corrupt());
            }
            let expected =
                match std::num::NonZeroU32::new(u32::try_from(index).map_err(|_| corrupt())?) {
                    None => self.identity.manifest_checksum().clone(),
                    Some(ordinal) => backup::manifest_artifact_checksum(&self.manifest, ordinal)?,
                };
            let expected = &expected;
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
