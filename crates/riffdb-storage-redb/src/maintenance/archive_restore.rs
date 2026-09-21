//! Private verified-backup conversion and exact archived-frame replay. Publication
//! requires a separate complete validation seal and matching durable V3 receipt.
#[path = "archive_preparation.rs"]
mod preparation;
pub use preparation::{RedbArchiveProjectionRebuild, RedbPreparedArchiveRestore};
#[path = "archive_prefix.rs"]
mod prefix;
#[path = "archive_publication.rs"]
mod publication;
pub(crate) use prefix::PrivateArchiveValidationBinding;
pub use prefix::{RedbPrivateArchiveRestoreCandidate, RedbValidatedPrivateArchiveRestore};
pub use publication::RedbSealedArchiveRestore;

use super::*;
use crate::maintenance::{RedbArchiveRepository, archive_backup::read_only_database};
use redb::ReadableDatabase;
use riffdb_storage_api::{
    ArchiveRestoreSelectionV3, ArchiveRestoreSuffixV3, AuthoritativeNamespaceV1 as N,
    ChangelogFrameV3, ChangelogHistoryStateV3, ReplicationAuthorityClassV1,
    ReplicationFollowerStateV3, ReplicationTransferV1,
    proto_codec::encode_replication_follower_state_v3,
};
use std::{
    ffi::OsStr,
    sync::atomic::{AtomicBool, Ordering},
};

/// Move-only unpublished stage, bound to one already verified archive. The
/// caller must run ordinary follower startup on this file before replay.
pub struct RedbArchiveRestoreStage {
    stage: RedbStagedRestore,
    file: File,
    history: ChangelogHistoryStateV3,
    selection: ArchiveRestoreSelectionV3,
    archive: RedbArchiveRepository,
}

/// Exact replay result, still private and not publishable. Full post-replay
/// validation, staged authorization and the existing incarnation ceremony remain
/// required. Dropping it discards the stage.
pub struct RedbReplayedArchiveRestore {
    archive: RedbArchiveRepository,
    stage: RedbStagedRestore,
    file: File,
    history: ChangelogHistoryStateV3,
    selection: ArchiveRestoreSelectionV3,
}

impl RedbStagedRestore {
    /// Rechecks the complete archive before changing the copied backup. Only
    /// source-local control state and the validated empty source journal are
    /// removed. The original lineage, allocators and replicated bytes survive.
    pub fn begin_archive_replay(
        self,
        archive: RedbArchiveRepository,
        cancellation: &AtomicBool,
    ) -> Result<RedbArchiveRestoreStage, StorageError> {
        self.begin_archive_replay_inner(archive, None, cancellation)
    }

    /// Replays only the independently checked selection retained by an archive
    /// receipt. Later archive frames never replace its terminal manifest or empty
    /// suffix. The complete selected prefix is verified before stage conversion.
    pub fn begin_selected_archive_replay(
        self,
        archive: RedbArchiveRepository,
        selection: ArchiveRestoreSelectionV3,
        cancellation: &AtomicBool,
    ) -> Result<RedbArchiveRestoreStage, StorageError> {
        self.begin_archive_replay_inner(archive, Some(selection), cancellation)
    }

    fn begin_archive_replay_inner(
        self,
        archive: RedbArchiveRepository,
        selection: Option<ArchiveRestoreSelectionV3>,
        cancellation: &AtomicBool,
    ) -> Result<RedbArchiveRestoreStage, StorageError> {
        cancelled(cancellation)?;
        self.verify_paths()?;
        let file = self
            .stage_cleanup
            .directory_guard
            .open_file_read_write(OsStr::new(DATABASE_ARTIFACT_FILE_NAME))?
            .into_std();
        let database = read_only_database(&file)?;
        let read = database.begin_read().map_err(unavailable)?;
        let history =
            crate::changelog_v3_roots::validate_retained_history(&read)?.ok_or_else(corrupt)?;
        let digest = self
            .manifest_identity
            .manifest_checksum()
            .as_bytes()
            .try_into()
            .map_err(|_| corrupt())?;
        if crate::follower_lifecycle::is_attached(&read)?
            || !archive.bound_to(history.lineage(), history.tail(), digest)
        {
            return Err(corrupt());
        }
        crate::primary_admission_roots::require_unfenced(
            &read.open_table(crate::layout::META).map_err(unavailable)?,
        )?;
        let selection = match selection {
            Some(selection) => selection,
            None => ArchiveRestoreSelectionV3::new(
                self.manifest_identity.clone(),
                history.lineage(),
                history.tail(),
                archive
                    .head()
                    .map_or(ArchiveRestoreSuffixV3::Empty, |head| {
                        ArchiveRestoreSuffixV3::Terminal(Box::new(head))
                    }),
            )
            .map_err(|_| corrupt())?,
        };
        if selection.backup() != &self.manifest_identity
            || selection.lineage() != history.lineage()
            || selection.backup_fence() != history.tail()
        {
            return Err(corrupt());
        }
        for frame in archive
            .frames_for_selection(&selection)
            .map_err(invalid_archive)?
        {
            cancelled(cancellation)?;
            drop(frame.map_err(invalid_archive)?);
        }
        drop(read);
        drop(database);
        cancelled(cancellation)?;
        self.verify_paths()?;
        let directory = &self.stage_cleanup.directory_guard;
        if !directory.regular_file_matches(OsStr::new(DATABASE_ARTIFACT_FILE_NAME), &file)? {
            return Err(corrupt());
        }
        validate_backup_journal(
            &self.staged_journal_file,
            self.manifest.database_id(),
            self.manifest.last_commit_sequence(),
        )?;
        for sidecar in [
            crate::journal::checkpoint_journal_path(&self.staged_database_file),
            crate::journal::spare_journal_path(&self.staged_database_file),
        ] {
            if directory
                .regular_file_length(sidecar.file_name().ok_or_else(corrupt)?)?
                .is_some()
            {
                return Err(corrupt());
            }
        }
        let database = redb::Database::builder()
            .set_cache_size(64 * 1024 * 1024)
            .create_file(file.try_clone().map_err(unavailable)?)
            .map_err(unavailable)?;
        let mut write = database.begin_write().map_err(unavailable)?;
        write.set_two_phase_commit(true);
        write
            .set_durability(redb::Durability::Immediate)
            .map_err(unavailable)?;
        if crate::changelog_v3_roots::validate_retained_history_for_write(&write)? != Some(history)
        {
            return Err(corrupt());
        }
        crate::primary_admission_roots::require_unfenced(
            &write.open_table(crate::layout::META).map_err(unavailable)?,
        )?;
        for table in [
            crate::changelog_v3_activation::HISTORY,
            crate::changelog_v3_activation::SOURCE_HOLDS,
        ] {
            if !write.delete_table(table).map_err(unavailable)? {
                return Err(corrupt());
            }
            drop(write.open_table(table).map_err(unavailable)?);
        }
        {
            let mut meta = write.open_table(crate::layout::META).map_err(unavailable)?;
            for namespace in N::ALL {
                if namespace.class()
                    == ReplicationAuthorityClassV1::ReplicationControl(
                        ReplicationTransferV1::SourceOnly,
                    )
                    && let Some(key) = namespace.metadata_key()
                {
                    meta.remove(key).map_err(unavailable)?;
                }
            }
            // This validated, unfenced source becomes a private attached
            // candidate. V2 admission is SourceOnly and cannot follow it.
            meta.remove(crate::primary_admission_roots::key()?)
                .map_err(unavailable)?;
            let state =
                ReplicationFollowerStateV3::attached(history.lineage(), history.tail(), None)
                    .map_err(|_| corrupt())?;
            let encoded = encode_replication_follower_state_v3(state).map_err(|_| corrupt())?;
            meta.insert(
                N::ReplicationFollowerState
                    .metadata_key()
                    .ok_or_else(corrupt)?,
                encoded.as_bytes(),
            )
            .map_err(unavailable)?;
        }
        if crate::changelog_v3_roots::validate_retained_history_for_write(&write)? != Some(history)
        {
            return Err(corrupt());
        }
        cancelled(cancellation)?;
        edge("conversion-staged");
        write.commit().map_err(unavailable)?;
        edge("conversion-committed");
        drop(database);
        directory
            .remove_file_if_present(self.staged_journal_file.file_name().ok_or_else(corrupt)?)?;
        directory.sync()?;
        edge("journal-removed");
        let value = RedbArchiveRestoreStage {
            stage: self,
            file,
            history,
            selection,
            archive,
        };
        value.verify()?;
        Ok(value)
    }
}

impl RedbArchiveRestoreStage {
    /// Complete verified original selection to freeze durably before replay.
    #[must_use]
    pub const fn selection(&self) -> &ArchiveRestoreSelectionV3 {
        &self.selection
    }

    /// Exact private file to open through the ordinary complete follower startup.
    #[must_use]
    pub fn staged_database_file(&self) -> &Path {
        self.stage.staged_database_file()
    }

    fn verify(&self) -> Result<(), StorageError> {
        verify_file(&self.stage, &self.file)
    }

    /// Consumes a separately validated follower applier only if it owns this
    /// exact retained inode and original backup fence. Buffers one archived frame
    /// at a time and never rewrites source receipts or advances acknowledgements.
    pub fn replay(
        self,
        mut applier: crate::RedbFollowerApplier,
        cancellation: &AtomicBool,
    ) -> Result<RedbReplayedArchiveRestore, StorageError> {
        let result = self.replay_inner(&mut applier, cancellation);
        // Release the engine before any success transfer or failed-stage cleanup.
        let closed = applier.close();
        let history = result?;
        closed?;
        Ok(RedbReplayedArchiveRestore {
            archive: self.archive,
            stage: self.stage,
            file: self.file,
            history,
            selection: self.selection,
        })
    }

    fn replay_inner(
        &self,
        applier: &mut crate::RedbFollowerApplier,
        cancellation: &AtomicBool,
    ) -> Result<ChangelogHistoryStateV3, StorageError> {
        cancelled(cancellation)?;
        self.verify()?;
        applier.verify_database_file(&self.file)?;
        if applier.durable_history()? != self.history {
            return Err(corrupt());
        }
        for item in self
            .archive
            .frames_for_selection(&self.selection)
            .map_err(invalid_archive)?
        {
            cancelled(cancellation)?;
            self.verify()?;
            let (manifest, bytes) = item.map_err(invalid_archive)?;
            if applier.durable_position()? != manifest.before() {
                return Err(corrupt());
            }
            let reconnect = ChangelogFrameV3::decode(&bytes)
                .map_err(|_| corrupt())?
                .binding()
                .prior_frame_hash()
                == manifest.before().history_hash();
            if reconnect && applier.resume_stream()? != manifest.before() {
                return Err(corrupt());
            }
            if applier.apply_frame(&bytes)? != manifest.covered() {
                return Err(corrupt());
            }
            edge("frame-applied");
        }
        cancelled(cancellation)?;
        self.verify()?;
        applier.verify_database_file(&self.file)?;
        let history = applier.durable_history()?;
        let selected_end = match self.selection.suffix() {
            ArchiveRestoreSuffixV3::Empty => self.selection.backup_fence(),
            ArchiveRestoreSuffixV3::Terminal(terminal) => terminal.covered(),
        };
        if history.tail() != selected_end {
            return Err(corrupt());
        }
        Ok(history)
    }
}

impl RedbReplayedArchiveRestore {
    /// Private replay result for complete validation and staged authorization.
    #[must_use]
    pub fn staged_database_file(&self) -> &Path {
        self.stage.staged_database_file()
    }
    /// Exact immutable backup/archive selection replayed by this private stage.
    #[must_use]
    pub const fn selection(&self) -> &ArchiveRestoreSelectionV3 {
        &self.selection
    }
    /// Original lineage and actual replayed physical/dual frontier.
    #[must_use]
    pub const fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }
    /// Discards the private result; never removes a configured database file.
    pub fn discard(self) -> Result<(), StorageError> {
        verify_file(&self.stage, &self.file)?;
        drop(self.file);
        self.stage.discard()
    }
}

fn verify_file(stage: &RedbStagedRestore, file: &File) -> Result<(), StorageError> {
    stage.verify_paths()?;
    if !stage
        .stage_cleanup
        .directory_guard
        .regular_file_matches(OsStr::new(DATABASE_ARTIFACT_FILE_NAME), file)?
    {
        return Err(corrupt());
    }
    Ok(())
}
fn cancelled(flag: &AtomicBool) -> Result<(), StorageError> {
    if flag.load(Ordering::Acquire) {
        Err(storage_error(StorageErrorKind::Unavailable))
    } else {
        Ok(())
    }
}
fn unavailable<T>(_: T) -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
fn invalid_archive(error: riffdb_storage_api::ArchiveConsumerErrorV1) -> StorageError {
    match error {
        riffdb_storage_api::ArchiveConsumerErrorV1::SinkUnavailable => unavailable(()),
        _ => corrupt(),
    }
}
fn edge(_name: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_ARCHIVE_REPLAY_CRASH").as_deref() == Ok(_name) {
        std::process::exit(98);
    }
}
