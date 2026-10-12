//! Complete selected replay followed by an exact physical or private stop.
use super::*;
use riffdb_types::{ArchiveRestoreStopV1, DualFrontier};
use std::sync::Arc;

/// Composition-owned pure projection replay over a private follower candidate.
/// The supplied owner permits historical validation and derived writes only;
/// complete structural validation and publication remain separate gates.
pub trait RedbArchiveProjectionRebuild {
    /// Validates historical plans and restores every required local generation.
    fn rebuild(
        &self,
        session: crate::RedbFollowerRecoveryCatalogSession,
        cancellation: Arc<AtomicBool>,
    ) -> Result<crate::RedbFollowerProjectionRecovery, StorageError>;
}

pub(super) fn rebuild_projection_state(
    stage: &RedbStagedRestore,
    file: &File,
    expected: ChangelogHistoryStateV3,
    inputs: StartupValidationInputs,
    cancellation: Arc<AtomicBool>,
    rebuilder: Option<&dyn RedbArchiveProjectionRebuild>,
) -> Result<(), StorageError> {
    let Some(rebuilder) = rebuilder else {
        return Ok(());
    };
    cancelled(&cancellation)?;
    verify_file(stage, file)?;
    let store = stage.open_archive_follower_store()?;
    let session = store.begin_projection_recovery(inputs, Arc::clone(&cancellation))?;
    let owner = rebuilder.rebuild(session, Arc::clone(&cancellation))?;
    if owner.durable_history()? != expected {
        return Err(corrupt());
    }
    drop(owner.finish_rebuild()?);
    cancelled(&cancellation)?;
    verify_file(stage, file)?;
    let database = stage.open_archive_preparation_read(file)?;
    let read = database.begin_read()?;
    if crate::changelog_v3_roots::validate_retained_history(&read)? != Some(expected) {
        return Err(corrupt());
    }
    Ok(())
}

enum PreparedKind {
    Physical {
        replayed: Box<RedbReplayedArchiveRestore>,
        checksum: BackupIntegrityChecksumV1,
    },
    Private(Box<RedbValidatedPrivateArchiveRestore>),
}

/// Fully validated exact restore state. Its immutable authorization snapshot
/// must pass current policy before its readers close and it is sealed. Only a
/// matching durable V3 receipt can authorize later incarnation publication.
pub struct RedbPreparedArchiveRestore {
    kind: PreparedKind,
    stop: ArchiveRestoreStopV1,
    inputs: StartupValidationInputs,
}

impl RedbArchiveRestoreStage {
    /// Replays and validates the complete original selection first, including
    /// receipts beyond an earlier requested stop. Explicit stops retain the
    /// first exact application boundary at or after the backup; LastArchived
    /// includes the selected terminal administration frontier as well.
    pub fn prepare_restore(
        self,
        stop: ArchiveRestoreStopV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<RedbPreparedArchiveRestore, StorageError> {
        self.prepare_restore_inner(stop, inputs, cancellation, None)
    }

    /// Restores derived projection generations through the existing composition
    /// owner before each unchanged complete scrub. It grants no serving authority.
    pub fn prepare_restore_with_projection_rebuild(
        self,
        stop: ArchiveRestoreStopV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
        rebuilder: &dyn RedbArchiveProjectionRebuild,
    ) -> Result<RedbPreparedArchiveRestore, StorageError> {
        self.prepare_restore_inner(stop, inputs, cancellation, Some(rebuilder))
    }

    fn prepare_restore_inner(
        self,
        stop: ArchiveRestoreStopV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
        rebuilder: Option<&dyn RedbArchiveProjectionRebuild>,
    ) -> Result<RedbPreparedArchiveRestore, StorageError> {
        cancelled(&cancellation)?;
        self.selection
            .target_application(stop)
            .map_err(|_| corrupt())?;
        let applier = self.open_preparation_applier(inputs.clone(), &cancellation)?;
        let mut replayed = self.replay(applier, &cancellation)?;
        rebuild_projection_state(
            &replayed.stage,
            &replayed.file,
            replayed.history,
            inputs.clone(),
            Arc::clone(&cancellation),
            rebuilder,
        )?;
        validate_physical(&mut replayed, inputs.clone(), Arc::clone(&cancellation))?;
        let kind = match stop {
            ArchiveRestoreStopV1::LastArchived => physical_kind(replayed)?,
            ArchiveRestoreStopV1::AtApplicationSequence(sequence) => {
                match replayed.rebuild_to_stop_with_projection_rebuild(
                    sequence,
                    inputs.clone(),
                    &cancellation,
                    rebuilder.map(|owner| (owner, Arc::clone(&cancellation))),
                )? {
                    prefix::RebuiltArchiveStop::Physical(mut replayed) => {
                        rebuild_projection_state(
                            &replayed.stage,
                            &replayed.file,
                            replayed.history,
                            inputs.clone(),
                            Arc::clone(&cancellation),
                            rebuilder,
                        )?;
                        validate_physical(
                            &mut replayed,
                            inputs.clone(),
                            Arc::clone(&cancellation),
                        )?;
                        physical_kind(*replayed)?
                    }
                    prefix::RebuiltArchiveStop::Private(candidate) => PreparedKind::Private(
                        Box::new(candidate.validate(inputs.clone(), Arc::clone(&cancellation))?),
                    ),
                }
            }
        };
        let prepared = RedbPreparedArchiveRestore { kind, stop, inputs };
        prepared
            .selection()
            .validate_restored_frontier(stop, prepared.restored_frontier())
            .map_err(|_| corrupt())?;
        cancelled(&cancellation)?;
        Ok(prepared)
    }
}

pub(super) fn validate_physical(
    replayed: &mut RedbReplayedArchiveRestore,
    inputs: StartupValidationInputs,
    cancellation: Arc<AtomicBool>,
) -> Result<(), StorageError> {
    verify_file(&replayed.stage, &replayed.file)?;
    if let Some(owner) = replayed.stage.native_archive_preparation.take() {
        let complete = owner.validate_complete_follower(
            replayed.staged_database_file(),
            inputs,
            cancellation,
        )?;
        complete.verify_follower_history(replayed.history)?;
        replayed.stage.install_native_archive_validation(complete)?;
        return verify_file(&replayed.stage, &replayed.file);
    }
    crate::startup::RedbOfflineIntegrityScrub::from_inputs(replayed.staged_database_file(), inputs)
        .with_cancellation(cancellation)
        .run_follower()?;
    verify_file(&replayed.stage, &replayed.file)?;
    let database = read_only_database(&replayed.file)?;
    let read = database.begin_read().map_err(unavailable)?;
    if crate::changelog_v3_roots::validate_retained_history(&read)? != Some(replayed.history)
        || !crate::follower_lifecycle::is_attached(&read)?
    {
        return Err(corrupt());
    }
    Ok(())
}

fn physical_kind(replayed: RedbReplayedArchiveRestore) -> Result<PreparedKind, StorageError> {
    let checksum = sha256_file(replayed.staged_database_file())?;
    verify_file(&replayed.stage, &replayed.file)?;
    Ok(PreparedKind::Physical {
        replayed: Box::new(replayed),
        checksum,
    })
}

impl RedbPreparedArchiveRestore {
    /// Immutable original artifact selection, separate from the stopped state.
    #[must_use]
    pub fn selection(&self) -> &ArchiveRestoreSelectionV3 {
        match &self.kind {
            PreparedKind::Physical { replayed, .. } => replayed.selection(),
            PreparedKind::Private(candidate) => candidate.selection(),
        }
    }
    /// Actual validated application and administration frontier.
    #[must_use]
    pub fn restored_frontier(&self) -> DualFrontier {
        match &self.kind {
            PreparedKind::Physical { replayed, .. } => replayed.history.tail().frontier(),
            PreparedKind::Private(candidate) => candidate.restored_frontier(),
        }
    }
    /// Consumes the original native proof for the server's ordinary startup joins.
    #[doc(hidden)]
    pub fn take_owned_authorization_evidence(
        &mut self,
    ) -> Result<crate::RedbPrivateRestoreAuthorization, StorageError> {
        match &mut self.kind {
            PreparedKind::Physical { replayed, .. } => {
                replayed.stage.take_owned_authorization_evidence()
            }
            PreparedKind::Private(candidate) => candidate.take_owned_authorization_evidence(),
        }
    }

    /// One immutable exact-state pin for staged authentication and policy.
    pub fn authorization_snapshot(&self) -> Result<crate::RedbOwnedSnapshot, StorageError> {
        match &self.kind {
            PreparedKind::Private(candidate) => candidate.authorization_snapshot(),
            PreparedKind::Physical { replayed, checksum } => {
                verify_physical_seal(replayed, checksum)?;
                if let Some(snapshot) = replayed.stage.native_archive_authorization_snapshot()? {
                    return Ok(snapshot);
                }
                let database = read_only_database(&replayed.file)?;
                let read = database.begin_read().map_err(unavailable)?;
                if crate::changelog_v3_roots::validate_retained_history(&read)?
                    != Some(replayed.history)
                    || !crate::follower_lifecycle::is_attached(&read)?
                {
                    return Err(corrupt());
                }
                verify_physical_seal(replayed, checksum)?;
                let root = Arc::new(crate::checkpoint_root::CheckpointRoot::new(read, 0));
                Ok(crate::RedbOwnedSnapshot::from_read_access(
                    crate::store::RedbReadAccess::Durable(root),
                ))
            }
        }
    }
    /// Close all authentication/policy readers before sealing. Publication still
    /// requires the exact durable Offline V3 receipt and recorded incarnation.
    pub fn seal_after_authorization(
        self,
        database_id: DatabaseId,
    ) -> Result<RedbSealedArchiveRestore, StorageError> {
        match self.kind {
            PreparedKind::Physical { replayed, checksum } => {
                verify_physical_seal(&replayed, &checksum)?;
                replayed.seal_after_validation(database_id, self.stop, self.inputs)
            }
            PreparedKind::Private(candidate) => {
                candidate.seal_after_validation(database_id, self.inputs)
            }
        }
    }
    /// Discard only this private stage, preserving the configured target.
    pub fn discard(self) -> Result<(), StorageError> {
        match self.kind {
            PreparedKind::Physical { replayed, .. } => replayed.discard(),
            PreparedKind::Private(candidate) => candidate.discard(),
        }
    }
}

fn verify_physical_seal(
    replayed: &RedbReplayedArchiveRestore,
    checksum: &BackupIntegrityChecksumV1,
) -> Result<(), StorageError> {
    verify_file(&replayed.stage, &replayed.file)?;
    if &sha256_file(replayed.staged_database_file())? != checksum {
        return Err(corrupt());
    }
    verify_file(&replayed.stage, &replayed.file)
}
