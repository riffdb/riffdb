use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use riffdb_storage_api::{
    BackupIntegrityChecksumV1, OfflineBackupManifestIdentityV1, OfflineBackupManifestV1,
    OfflineRestoreOverwritePolicyV1, OfflineRestorePersistencePort, OfflineRestoreResultV1,
    StartupValidationInputs, StorageError, StorageErrorKind,
};
use riffdb_types::{BackupNameV1, DatabaseId, OfflineMaintenanceOperationId};

use super::failpoint::{RedbMaintenanceFailpoint, RedbMaintenanceTestController};
use super::path_guard::PinnedDirectory;
use crate::backup::{
    DATABASE_ARTIFACT_FILE_NAME, RedbOfflineRestore, sha256_file, sha256_reader, sync_parent,
    validate_backup_journal, validate_database_semantics, validate_immutable_backup,
};
use crate::error::storage_error;

const COPY_BUFFER_BYTES: usize = 64 * 1024;

#[path = "archive_restore.rs"]
mod archive;
pub use archive::{RedbArchiveRestoreStage, RedbReplayedArchiveRestore, RedbSealedArchiveRestore};

#[cfg(test)]
#[path = "archive_restore_tests.rs"]
mod archive_tests;

/// Move-only private staged restore that has passed complete exact-end validation.
pub struct RedbStagedRestore {
    operation_id: OfflineMaintenanceOperationId,
    backup_name: BackupNameV1,
    backup_directory: PathBuf,
    staged_database_file: PathBuf,
    staged_journal_file: PathBuf,
    staged_format_marker_file: PathBuf,
    configured_database_file: PathBuf,
    manifest: OfflineBackupManifestV1,
    manifest_identity: OfflineBackupManifestIdentityV1,
    backup_directory_guard: PinnedDirectory,
    configured_parent_guard: PinnedDirectory,
    stage_cleanup: StagedDirectoryCleanup,
    test_controller: Option<RedbMaintenanceTestController>,
}

impl RedbStagedRestore {
    pub(super) fn materialize(
        operation_id: OfflineMaintenanceOperationId,
        backup_name: BackupNameV1,
        backup_directory: PathBuf,
        staged_directory: PathBuf,
        configured_database_file: PathBuf,
        validation_inputs: StartupValidationInputs,
        test_controller: Option<RedbMaintenanceTestController>,
    ) -> Result<Self, StorageError> {
        let backup_directory_guard = PinnedDirectory::open(&backup_directory)?;
        let configured_parent_guard =
            PinnedDirectory::open(configured_database_file.parent().ok_or_else(invariant)?)?;
        let staged_parent = staged_directory.parent().ok_or_else(invariant)?;
        let staged_parent_guard = PinnedDirectory::open(staged_parent)?;
        backup_directory_guard.verify()?;
        configured_parent_guard.verify()?;
        staged_parent_guard.verify()?;

        remove_prior_stage(&staged_directory)?;
        staged_parent_guard.verify()?;
        let (source_manifest, manifest_identity) = validate_immutable_backup(&backup_directory)?;
        backup_directory_guard.verify()?;
        fs::create_dir(&staged_directory).map_err(io_unavailable)?;
        sync_parent(&staged_directory)?;
        staged_parent_guard.verify()?;
        let mut stage_cleanup =
            match StagedDirectoryCleanup::new(staged_directory.clone(), staged_parent_guard) {
                Ok(cleanup) => cleanup,
                Err(error) => {
                    remove_prior_stage(&staged_directory)?;
                    return Err(error);
                }
            };

        let materialized = (|| {
            let result = RedbOfflineRestore::bind(&backup_directory, &staged_directory)
                .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)?;
            let OfflineRestoreResultV1::Restored { manifest } = result else {
                return Err(invariant());
            };
            if *manifest != source_manifest {
                return Err(corrupt());
            }
            backup_directory_guard.verify()?;
            configured_parent_guard.verify()?;
            stage_cleanup.verify()?;
            if let Some(controller) = &test_controller {
                controller.hit(RedbMaintenanceFailpoint::AfterStagedMaterialization, false)?;
            }
            Ok(*manifest)
        })();

        match materialized {
            Ok(manifest) => {
                let staged_database_file = staged_directory.join(DATABASE_ARTIFACT_FILE_NAME);
                let staged_journal_file = crate::journal::journal_path(&staged_database_file);
                let staged_format_marker_file =
                    crate::durable_format_marker_path(&staged_database_file);
                crate::startup::RedbOfflineIntegrityScrub::from_inputs(
                    &staged_database_file,
                    validation_inputs,
                )
                .run()?;
                Ok(Self {
                    operation_id,
                    backup_name,
                    backup_directory,
                    staged_database_file,
                    staged_journal_file,
                    staged_format_marker_file,
                    configured_database_file,
                    manifest,
                    manifest_identity,
                    backup_directory_guard,
                    configured_parent_guard,
                    stage_cleanup,
                    test_controller,
                })
            }
            Err(error) => {
                stage_cleanup.remove_now()?;
                Err(error)
            }
        }
    }

    /// Returns the completely scrubbed private database for staged authorization.
    #[must_use]
    pub fn staged_database_file(&self) -> &Path {
        &self.staged_database_file
    }

    /// Borrows the source manifest proven during private materialization.
    #[must_use]
    pub const fn manifest(&self) -> &OfflineBackupManifestV1 {
        &self.manifest
    }

    /// Borrows the exact immutable source-manifest identity.
    #[must_use]
    pub const fn manifest_identity(&self) -> &OfflineBackupManifestIdentityV1 {
        &self.manifest_identity
    }

    /// Explicitly discards this unpublished private stage.
    ///
    /// Dropping the move-only stage has the same best-effort cleanup behavior;
    /// this method is available when the server must observe cleanup failure.
    pub fn discard(mut self) -> Result<(), StorageError> {
        self.stage_cleanup.remove_now()
    }

    /// Seals the already-scrubbed stage after auth-owned handles are closed.
    ///
    /// The server-supplied identity is reread by its staged authorization open;
    /// this final check freezes exact post-open bytes for publication.
    pub fn seal_after_validation(
        self,
        validated_database_id: DatabaseId,
    ) -> Result<RedbSealedStagedRestore, StorageError> {
        self.verify_paths()?;
        if validated_database_id != self.manifest.database_id() {
            return Err(corrupt());
        }
        let database = validate_database_semantics(&self.staged_database_file, &self.manifest)?;
        drop(database);
        let staged_history_incarnation =
            crate::backup::read_history_incarnation(&self.staged_database_file)?.unwrap_or(0);
        if let Some(manifest_incarnation) = self.manifest.history_incarnation()
            && manifest_incarnation > staged_history_incarnation
        {
            return Err(corrupt());
        }
        let sealed_artifact_checksum = sha256_file(&self.staged_database_file)?;
        validate_backup_journal(
            &self.staged_journal_file,
            self.manifest.database_id(),
            self.manifest.last_commit_sequence(),
        )?;
        let sealed_journal_checksum = sha256_file(&self.staged_journal_file)?;
        if crate::preflight_durable_format_path(&self.staged_database_file)
            != Ok(crate::RedbDurableFormatPreflight::OpenCurrent)
        {
            return Err(corrupt());
        }
        let sealed_format_marker_checksum = sha256_file(&self.staged_format_marker_file)?;
        self.verify_paths()?;
        Ok(RedbSealedStagedRestore {
            operation_id: self.operation_id,
            backup_name: self.backup_name,
            backup_directory: self.backup_directory,
            configured_database_file: self.configured_database_file,
            staged_database_file: self.staged_database_file,
            staged_journal_file: self.staged_journal_file,
            staged_format_marker_file: self.staged_format_marker_file,
            manifest: self.manifest,
            manifest_identity: self.manifest_identity,
            staged_history_incarnation,
            sealed_artifact_checksum,
            sealed_journal_checksum,
            sealed_format_marker_checksum,
            backup_directory_guard: self.backup_directory_guard,
            configured_parent_guard: self.configured_parent_guard,
            stage_cleanup: self.stage_cleanup,
            test_controller: self.test_controller,
        })
    }

    fn verify_paths(&self) -> Result<(), StorageError> {
        self.backup_directory_guard.verify()?;
        self.configured_parent_guard.verify()?;
        self.stage_cleanup.verify()
    }
}

impl std::fmt::Debug for RedbStagedRestore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedbStagedRestore")
            .field("operation_id", &self.operation_id)
            .field("backup_name", &self.backup_name)
            .field("paths", &"[REDACTED]")
            .finish()
    }
}

/// Move-only checksum seal for one fully validated private staged database.
pub struct RedbSealedStagedRestore {
    operation_id: OfflineMaintenanceOperationId,
    backup_name: BackupNameV1,
    backup_directory: PathBuf,
    configured_database_file: PathBuf,
    staged_database_file: PathBuf,
    staged_journal_file: PathBuf,
    staged_format_marker_file: PathBuf,
    manifest: OfflineBackupManifestV1,
    manifest_identity: OfflineBackupManifestIdentityV1,
    staged_history_incarnation: u64,
    sealed_artifact_checksum: BackupIntegrityChecksumV1,
    sealed_journal_checksum: BackupIntegrityChecksumV1,
    sealed_format_marker_checksum: BackupIntegrityChecksumV1,
    backup_directory_guard: PinnedDirectory,
    configured_parent_guard: PinnedDirectory,
    stage_cleanup: StagedDirectoryCleanup,
    test_controller: Option<RedbMaintenanceTestController>,
}

impl RedbSealedStagedRestore {
    /// Borrows the exact immutable source-manifest identity.
    #[must_use]
    pub const fn manifest_identity(&self) -> &OfflineBackupManifestIdentityV1 {
        &self.manifest_identity
    }

    /// Borrows the sealed offline backup manifest.
    #[must_use]
    pub const fn manifest(&self) -> &OfflineBackupManifestV1 {
        &self.manifest
    }

    /// Returns the staged META history incarnation (0 when the key was absent).
    #[must_use]
    pub const fn staged_history_incarnation(&self) -> u64 {
        self.staged_history_incarnation
    }

    /// Stamps the published history incarnation into the staged artifact and
    /// reseals the byte checksum so rename publication stays byte-identical.
    ///
    /// Called only during the offline restore phase before publish. Crash resume
    /// re-applies from the receipt; stamping is idempotent.
    pub fn apply_published_history_incarnation(
        &mut self,
        incarnation: u64,
    ) -> Result<(), StorageError> {
        // Receipt authority is already durable before this stamp; a crash here
        // resumes from the receipt value without double-advancing.
        self.hit(
            RedbMaintenanceFailpoint::BetweenReceiptWriteAndStagedStamp,
            true,
        )?;
        crate::backup::stamp_history_incarnation(&self.staged_database_file, incarnation)?;
        self.staged_history_incarnation = incarnation;
        self.sealed_artifact_checksum = sha256_file(&self.staged_database_file)?;
        self.verify_paths()?;
        Ok(())
    }

    pub(super) const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        self.operation_id
    }

    pub(super) const fn backup_name(&self) -> &BackupNameV1 {
        &self.backup_name
    }

    pub(super) fn configured_database_file(&self) -> &Path {
        &self.configured_database_file
    }

    pub(super) fn publish_to_configured_database(
        mut self,
        overwrite_policy: OfflineRestoreOverwritePolicyV1,
    ) -> Result<OfflineRestoreResultV1, StorageError> {
        self.verify_paths()?;
        let target_name = self
            .configured_database_file
            .file_name()
            .ok_or_else(invariant)?;
        let target_state = target_file_state(&self.configured_parent_guard, target_name)?;
        if target_state == TargetFileState::NonEmpty
            && overwrite_policy == OfflineRestoreOverwritePolicyV1::RefuseNonEmpty
        {
            return Ok(OfflineRestoreResultV1::TargetNotEmpty);
        }

        let (source_manifest, source_identity) = validate_immutable_backup(&self.backup_directory)?;
        if source_manifest != self.manifest || source_identity != self.manifest_identity {
            return Err(corrupt());
        }
        self.backup_directory_guard.verify()?;
        let staged_file = self
            .stage_cleanup
            .open_file(std::ffi::OsStr::new(DATABASE_ARTIFACT_FILE_NAME))?;
        if sha256_reader(staged_file)? != self.sealed_artifact_checksum {
            return Err(corrupt());
        }
        let staged_journal_name = self.staged_journal_file.file_name().ok_or_else(invariant)?;
        let staged_journal = self.stage_cleanup.open_file(staged_journal_name)?;
        if sha256_reader(staged_journal)? != self.sealed_journal_checksum {
            return Err(corrupt());
        }
        let staged_format_marker_name = self
            .staged_format_marker_file
            .file_name()
            .ok_or_else(invariant)?;
        let staged_format_marker = self.stage_cleanup.open_file(staged_format_marker_name)?;
        if sha256_reader(staged_format_marker)? != self.sealed_format_marker_checksum {
            return Err(corrupt());
        }
        self.stage_cleanup.verify()?;

        let target_lock = lock_existing_target(&self.configured_parent_guard, target_name)?;
        let configured_journal = crate::journal::journal_path(&self.configured_database_file);
        let target_journal_name = configured_journal.file_name().ok_or_else(invariant)?;
        let target_journal_lock =
            lock_existing_target(&self.configured_parent_guard, target_journal_name)?;
        let configured_format_marker =
            crate::durable_format_marker_path(&self.configured_database_file);
        let target_format_marker_name =
            configured_format_marker.file_name().ok_or_else(invariant)?;
        let target_format_marker_lock =
            lock_existing_target(&self.configured_parent_guard, target_format_marker_name)?;
        let (mut temporary, mut temporary_file) = TargetTemporaryFile::create(
            &self.configured_parent_guard,
            target_name,
            self.operation_id,
        )?;
        let (mut journal_temporary, mut journal_temporary_file) = TargetTemporaryFile::create(
            &self.configured_parent_guard,
            target_journal_name,
            self.operation_id,
        )?;
        let (mut format_marker_temporary, mut format_marker_temporary_file) =
            TargetTemporaryFile::create(
                &self.configured_parent_guard,
                target_format_marker_name,
                self.operation_id,
            )?;
        let mut staged_file = self
            .stage_cleanup
            .open_file(std::ffi::OsStr::new(DATABASE_ARTIFACT_FILE_NAME))?;
        copy_and_sync(&mut staged_file, &mut temporary_file)?;
        drop(temporary_file);
        if sha256_reader(temporary.reopen()?)? != self.sealed_artifact_checksum {
            return Err(corrupt());
        }
        let mut staged_journal = self.stage_cleanup.open_file(staged_journal_name)?;
        copy_and_sync(&mut staged_journal, &mut journal_temporary_file)?;
        drop(journal_temporary_file);
        if sha256_reader(journal_temporary.reopen()?)? != self.sealed_journal_checksum {
            return Err(corrupt());
        }
        let mut staged_format_marker = self.stage_cleanup.open_file(staged_format_marker_name)?;
        copy_and_sync(&mut staged_format_marker, &mut format_marker_temporary_file)?;
        drop(format_marker_temporary_file);
        if sha256_reader(format_marker_temporary.reopen()?)? != self.sealed_format_marker_checksum {
            return Err(corrupt());
        }
        if target_file_state(&self.configured_parent_guard, target_name)?
            == TargetFileState::NonEmpty
            && overwrite_policy == OfflineRestoreOverwritePolicyV1::RefuseNonEmpty
        {
            return Ok(OfflineRestoreResultV1::TargetNotEmpty);
        }
        self.verify_paths()?;
        self.hit(RedbMaintenanceFailpoint::BeforeTargetPublication, false)?;
        if !target_lock_matches(
            &self.configured_parent_guard,
            target_name,
            target_lock.as_ref(),
        )? || !target_lock_matches(
            &self.configured_parent_guard,
            target_journal_name,
            target_journal_lock.as_ref(),
        )? {
            return Err(corrupt());
        }
        if !target_lock_matches(
            &self.configured_parent_guard,
            target_format_marker_name,
            target_format_marker_lock.as_ref(),
        )? {
            return Err(corrupt());
        }
        if target_file_state(&self.configured_parent_guard, target_name)?
            == TargetFileState::NonEmpty
            && overwrite_policy == OfflineRestoreOverwritePolicyV1::RefuseNonEmpty
        {
            return Ok(OfflineRestoreResultV1::TargetNotEmpty);
        }
        // Remove and durably publish absence of the old marker before any new
        // database bytes. A crash from here until the final marker rename is a
        // typed startup refusal, never a stale-marker acceptance.
        self.configured_parent_guard
            .remove_file_if_present(target_format_marker_name)?;
        self.configured_parent_guard.sync()?;
        temporary.publish(target_name)?;
        journal_temporary.publish(target_journal_name)?;
        // Publish the marker last. A crash before this rename leaves no new
        // marker for new bytes and therefore forces startup to refuse rather
        // than trusting a partially published restore.
        format_marker_temporary.publish(target_format_marker_name)?;
        self.stage_cleanup.disarm();
        self.hit(RedbMaintenanceFailpoint::AfterTargetPublication, true)?;
        if self.configured_parent_guard.sync().is_err()
            || self.configured_parent_guard.verify().is_err()
        {
            return Err(unknown());
        }
        drop(target_lock);
        drop(target_journal_lock);
        drop(target_format_marker_lock);
        self.hit(RedbMaintenanceFailpoint::AfterTargetParentSync, true)?;
        Ok(OfflineRestoreResultV1::Restored {
            manifest: Box::new(self.manifest),
        })
    }

    fn verify_paths(&self) -> Result<(), StorageError> {
        self.backup_directory_guard.verify()?;
        self.configured_parent_guard.verify()?;
        self.stage_cleanup.verify()
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

impl std::fmt::Debug for RedbSealedStagedRestore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedbSealedStagedRestore")
            .field("operation_id", &self.operation_id)
            .field("backup_name", &self.backup_name)
            .field("paths", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TargetFileState {
    Missing,
    Empty,
    NonEmpty,
}

fn target_file_state(
    parent: &PinnedDirectory,
    name: &std::ffi::OsStr,
) -> Result<TargetFileState, StorageError> {
    match parent.regular_file_length(name)? {
        None => Ok(TargetFileState::Missing),
        Some(0) => Ok(TargetFileState::Empty),
        Some(_) => Ok(TargetFileState::NonEmpty),
    }
}

fn lock_existing_target(
    parent: &PinnedDirectory,
    name: &std::ffi::OsStr,
) -> Result<Option<File>, StorageError> {
    if parent.regular_file_length(name)?.is_none() {
        return Ok(None);
    }
    let file = parent.open_file_read_write(name)?.into_std();
    file.try_lock()
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    Ok(Some(file))
}

fn target_lock_matches(
    parent: &PinnedDirectory,
    name: &std::ffi::OsStr,
    target_lock: Option<&File>,
) -> Result<bool, StorageError> {
    match target_lock {
        None => Ok(parent.regular_file_length(name)?.is_none()),
        Some(file) => parent.regular_file_matches(name, file),
    }
}

struct StagedDirectoryCleanup {
    parent_guard: PinnedDirectory,
    directory_guard: PinnedDirectory,
    armed: bool,
}

impl StagedDirectoryCleanup {
    fn new(path: PathBuf, parent_guard: PinnedDirectory) -> Result<Self, StorageError> {
        let directory_guard = PinnedDirectory::open(&path)?;
        Ok(Self {
            parent_guard,
            directory_guard,
            armed: true,
        })
    }

    fn verify(&self) -> Result<(), StorageError> {
        self.parent_guard.verify()?;
        self.directory_guard.verify()
    }

    fn remove_now(&mut self) -> Result<(), StorageError> {
        if !self.armed {
            return Ok(());
        }
        self.directory_guard.remove_self_all()?;
        self.parent_guard.sync()?;
        self.armed = false;
        Ok(())
    }

    fn open_file(&self, name: &std::ffi::OsStr) -> Result<cap_std::fs::File, StorageError> {
        self.directory_guard.open_file(name)
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StagedDirectoryCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.remove_now();
        }
    }
}

fn remove_prior_stage(path: &Path) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_unavailable(error)),
        Ok(metadata) if metadata.file_type().is_dir() => {
            fs::remove_dir_all(path).map_err(io_unavailable)?;
            sync_parent(path)
        }
        Ok(_) => Err(corrupt()),
    }
}

struct TargetTemporaryFile<'a> {
    parent: &'a PinnedDirectory,
    name: OsString,
    published: bool,
}

impl<'a> TargetTemporaryFile<'a> {
    fn create(
        parent: &'a PinnedDirectory,
        target_name: &std::ffi::OsStr,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<(Self, cap_std::fs::File), StorageError> {
        let name = target_temporary_name(target_name, operation_id);
        let file = parent.create_new_file(&name)?;
        Ok((
            Self {
                parent,
                name,
                published: false,
            },
            file,
        ))
    }

    fn reopen(&self) -> Result<cap_std::fs::File, StorageError> {
        self.parent.open_file(&self.name)
    }

    fn publish(&mut self, target_name: &std::ffi::OsStr) -> Result<(), StorageError> {
        self.parent.rename(&self.name, target_name)?;
        self.published = true;
        Ok(())
    }
}

impl Drop for TargetTemporaryFile<'_> {
    fn drop(&mut self) {
        if !self.published {
            let _ = self.parent.remove_file_if_present(&self.name);
        }
    }
}

pub(super) fn target_temporary_name(
    target_name: &std::ffi::OsStr,
    operation_id: OfflineMaintenanceOperationId,
) -> OsString {
    let mut name = OsString::from(".");
    name.push(target_name);
    name.push(format!(".riffdb-maintenance-publish-{operation_id}.tmp"));
    name
}

fn copy_and_sync(
    source: &mut cap_std::fs::File,
    destination: &mut cap_std::fs::File,
) -> Result<(), StorageError> {
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    loop {
        let read = source.read(&mut buffer).map_err(io_unavailable)?;
        if read == 0 {
            break;
        }
        destination
            .write_all(&buffer[..read])
            .map_err(io_unavailable)?;
    }
    destination.sync_all().map_err(io_unavailable)
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

fn unknown() -> StorageError {
    storage_error(StorageErrorKind::CommitStatusUnknown)
}

#[cfg(all(test, unix))]
mod tests {
    use std::ffi::OsStr;
    use std::fs;
    use std::io::Write as _;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use riffdb_types::OfflineMaintenanceOperationId;

    use super::{
        StagedDirectoryCleanup, TargetTemporaryFile, lock_existing_target, target_lock_matches,
        target_temporary_name,
    };
    use crate::maintenance::path_guard::PinnedDirectory;

    static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(1);

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let ordinal = NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed);
            let path = crate::test_path::root().join(format!(
                "riffdb-redb-capability-{label}-{}-{ordinal}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir(&path).expect("create capability test root");
            Self(path)
        }

        fn join(&self, value: impl AsRef<Path>) -> PathBuf {
            self.0.join(value)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn operation_id(seed: u8) -> OfflineMaintenanceOperationId {
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [seed; 10])
            .expect("operation ID")
    }

    #[test]
    fn retained_target_parent_never_publishes_into_an_ambient_replacement() {
        let root = TestRoot::new("publish-parent-swap");
        let parent = root.join("data");
        let original = root.join("data-original");
        let target_name = OsStr::new("database.redb");
        fs::create_dir(&parent).expect("create target parent");
        fs::write(parent.join(target_name), b"old").expect("write old target");
        let guard = PinnedDirectory::open(&parent).expect("pin target parent");
        let (mut temporary, mut temporary_file) =
            TargetTemporaryFile::create(&guard, target_name, operation_id(0x71))
                .expect("create target temporary");
        temporary_file
            .write_all(b"restored")
            .expect("write restored bytes");
        temporary_file.sync_all().expect("sync restored bytes");
        drop(temporary_file);

        fs::rename(&parent, &original).expect("move pinned parent");
        fs::create_dir(&parent).expect("install replacement parent");
        fs::write(parent.join(target_name), b"replacement").expect("write replacement sentinel");

        temporary
            .publish(target_name)
            .expect("publish through retained parent");
        guard.sync().expect("sync retained parent");

        assert_eq!(
            fs::read(original.join(target_name)).expect("read original target"),
            b"restored"
        );
        assert_eq!(
            fs::read(parent.join(target_name)).expect("read replacement target"),
            b"replacement",
            "publication must not follow the replaced ambient parent name"
        );
        assert!(
            guard.verify().is_err(),
            "ambient replacement remains a fail-closed integrity signal"
        );
    }

    #[test]
    fn retained_target_parent_never_cleans_a_replacement_temporary() {
        let root = TestRoot::new("cleanup-parent-swap");
        let parent = root.join("data");
        let original = root.join("data-original");
        let target_name = OsStr::new("database.redb");
        let operation_id = operation_id(0x72);
        let temporary_name = target_temporary_name(target_name, operation_id);
        fs::create_dir(&parent).expect("create target parent");
        let guard = PinnedDirectory::open(&parent).expect("pin target parent");
        let (temporary, mut temporary_file) =
            TargetTemporaryFile::create(&guard, target_name, operation_id)
                .expect("create target temporary");
        temporary_file
            .write_all(b"unpublished")
            .expect("write unpublished bytes");
        temporary_file.sync_all().expect("sync unpublished bytes");
        drop(temporary_file);

        fs::rename(&parent, &original).expect("move pinned parent");
        fs::create_dir(&parent).expect("install replacement parent");
        fs::write(parent.join(&temporary_name), b"replacement-temp")
            .expect("write replacement temporary sentinel");
        drop(temporary);

        assert!(
            !original.join(&temporary_name).exists(),
            "cleanup must remove the unpublished file from the retained parent"
        );
        assert_eq!(
            fs::read(parent.join(&temporary_name)).expect("read replacement temporary"),
            b"replacement-temp",
            "cleanup must not follow the replaced ambient parent name"
        );
    }

    #[test]
    fn staged_cleanup_never_removes_a_same_name_substitute() {
        let root = TestRoot::new("stage-name-swap");
        let parent = root.join("staged");
        let stage = parent.join("operation");
        let moved_stage = parent.join("operation-original");
        fs::create_dir(&parent).expect("create stage parent");
        fs::create_dir(&stage).expect("create private stage");
        fs::write(stage.join("owned"), b"owned").expect("write private-stage sentinel");
        let parent_guard = PinnedDirectory::open(&parent).expect("pin stage parent");
        let mut cleanup =
            StagedDirectoryCleanup::new(stage.clone(), parent_guard).expect("pin private stage");

        fs::rename(&stage, &moved_stage).expect("move pinned stage");
        fs::create_dir(&stage).expect("install same-name substitute");
        fs::write(stage.join("substitute"), b"substitute")
            .expect("write substitute-stage sentinel");

        cleanup.remove_now().expect("remove exact pinned stage");

        assert!(
            !moved_stage.exists(),
            "cleanup must find and remove the retained stage by identity"
        );
        assert_eq!(
            fs::read(stage.join("substitute")).expect("read substitute-stage sentinel"),
            b"substitute",
            "cleanup must not select a reused child name"
        );
    }

    #[test]
    fn target_identity_recheck_rejects_a_same_name_substitute() {
        let root = TestRoot::new("target-name-swap");
        let parent = root.join("data");
        let moved_target = parent.join("database-original.redb");
        let target_name = OsStr::new("database.redb");
        fs::create_dir(&parent).expect("create target parent");
        fs::write(parent.join(target_name), []).expect("write empty target");
        let guard = PinnedDirectory::open(&parent).expect("pin target parent");
        let target_lock =
            lock_existing_target(&guard, target_name).expect("lock exact empty target");

        fs::rename(parent.join(target_name), &moved_target).expect("move locked target");
        fs::write(parent.join(target_name), b"same-name substitute")
            .expect("write same-name target substitute");

        assert!(
            !target_lock_matches(&guard, target_name, target_lock.as_ref())
                .expect("recheck target identity"),
            "publication must fail closed when the configured entry no longer names the locked file"
        );
        assert_eq!(
            fs::read(parent.join(target_name)).expect("read target substitute"),
            b"same-name substitute"
        );
        assert_eq!(
            fs::read(moved_target).expect("read moved locked target"),
            b""
        );
    }
}
