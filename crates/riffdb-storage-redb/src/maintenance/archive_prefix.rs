//! Offline construction after complete original selection replay. A cut has no
//! source history identity and cannot be opened or published by ordinary owners.
use super::*;
use redb::ReadableTable;
use riffdb_storage_api::ChangelogFrameBindingV3;
use riffdb_types::{CommitSequence, DualFrontier};

#[path = "archive_prefix_graph.rs"]
mod graph;
#[path = "archive_prefix_validation.rs"]
mod validation;
pub(crate) use validation::PrivateArchiveValidationBinding;
pub use validation::RedbValidatedPrivateArchiveRestore;
const PRIVATE_MARKER: &str = "private-restore-format.riffdb";

/// Unpublished command-prefix artifact. Its original format marker is quarantined
/// and its follower attachment removed. Complete private validation and staged
/// authorization are required before a separate publication owner can exist.
pub struct RedbPrivateArchiveRestoreCandidate {
    construction_checksum: BackupIntegrityChecksumV1,
    binding: PrivateArchiveValidationBinding,
    stage: RedbStagedRestore,
    file: File,
    selection: ArchiveRestoreSelectionV3,
    frontier: DualFrontier,
}

impl RedbPrivateArchiveRestoreCandidate {
    /// Offline inspection path; ordinary source and follower opens refuse it.
    #[must_use]
    pub fn staged_database_file(&self) -> &Path {
        self.stage.staged_database_file()
    }
    /// Original selection, independent of the reconstructed logical frontier.
    #[must_use]
    pub const fn selection(&self) -> &ArchiveRestoreSelectionV3 {
        &self.selection
    }
    /// Actual application/audit boundary; never a replication position.
    #[must_use]
    pub const fn restored_frontier(&self) -> DualFrontier {
        self.frontier
    }
    /// Discard this artifact without touching the configured database.
    pub fn discard(self) -> Result<(), StorageError> {
        verify_file(&self.stage, &self.file)?;
        drop(self.file);
        self.stage.discard()
    }
}

impl RedbReplayedArchiveRestore {
    /// Rebuilds from the verified backup after validating the complete selected
    /// history. Only an interior application boundary is accepted here. The
    /// result has no serving, follower, seal or publication capability.
    pub fn reconstruct_inside_group(
        self,
        stop: CommitSequence,
        inputs: StartupValidationInputs,
        cancellation: &AtomicBool,
    ) -> Result<RedbPrivateArchiveRestoreCandidate, StorageError> {
        cancelled(cancellation)?;
        verify_file(&self.stage, &self.file)?;
        if self.selection.backup_fence().frontier().application() >= Some(stop)
            || self.history.tail().frontier().application() <= Some(stop)
        {
            return Err(corrupt());
        }
        crate::startup::RedbOfflineIntegrityScrub::from_inputs(
            self.staged_database_file(),
            inputs.clone(),
        )
        .run_follower()?;
        verify_file(&self.stage, &self.file)?;
        let Self {
            stage,
            file,
            selection,
            archive,
            ..
        } = self;
        drop(file);
        cancelled(cancellation)?;
        let stage = rebuild(stage, inputs.clone())?.begin_selected_archive_replay(
            archive,
            selection,
            cancellation,
        )?;
        let mut applier = crate::startup::open_validated_archive_follower(
            stage.staged_database_file(),
            inputs,
            cancellation,
        )?;
        applier.verify_database_file(&stage.file)?;
        let result = replay_predecessor(&stage, &mut applier, stop, cancellation);
        let closed = applier.close();
        let receipt = result?;
        closed?;
        construct(stage, receipt, stop, cancellation)
    }
}

fn rebuild(
    mut stage: RedbStagedRestore,
    inputs: StartupValidationInputs,
) -> Result<RedbStagedRestore, StorageError> {
    stage.verify_paths()?;
    let directory = stage
        .staged_database_file
        .parent()
        .ok_or_else(corrupt)?
        .to_path_buf();
    stage.stage_cleanup.remove_now()?;
    let RedbStagedRestore {
        operation_id,
        backup_name,
        backup_directory,
        configured_database_file,
        manifest_identity,
        test_controller,
        ..
    } = stage;
    let rebuilt = RedbStagedRestore::materialize(
        operation_id,
        backup_name,
        backup_directory,
        directory,
        configured_database_file,
        inputs,
        test_controller,
    )?;
    if rebuilt.manifest_identity != manifest_identity {
        return Err(corrupt());
    }
    Ok(rebuilt)
}

fn replay_predecessor(
    stage: &RedbArchiveRestoreStage,
    applier: &mut crate::RedbFollowerApplier,
    stop: CommitSequence,
    cancellation: &AtomicBool,
) -> Result<riffdb_storage_api::AuthoritativeTransactionV3, StorageError> {
    if applier.durable_history()? != stage.history {
        return Err(corrupt());
    }
    for item in stage
        .archive
        .frames_for_selection(&stage.selection)
        .map_err(invalid_archive)?
    {
        cancelled(cancellation)?;
        stage.verify()?;
        let (_, bytes) = item.map_err(invalid_archive)?;
        let frame = ChangelogFrameV3::decode(&bytes).map_err(|_| corrupt())?;
        for receipt in frame.receipts() {
            cancelled(cancellation)?;
            let binding = receipt.binding();
            if binding.covered_frontier.application() >= Some(stop) {
                if binding.predecessor_frontier.application() >= Some(stop)
                    || binding.covered_frontier.application() == Some(stop)
                {
                    return Err(corrupt());
                }
                // Prove the crossing receipt's original predecessor before
                // releasing the only follower mutation capability.
                applier
                    .durable_history()?
                    .advance(receipt)
                    .map_err(|_| corrupt())?;
                return Ok(receipt.clone());
            }
            let before = applier.resume_stream()?;
            let lineage = stage.history.lineage();
            let wrapper = ChangelogFrameV3::new(
                ChangelogFrameBindingV3::new(
                    lineage.database_id(),
                    lineage.history_incarnation(),
                    lineage.leadership_epoch().get(),
                    lineage.catalog_digest(),
                    before.history_hash(),
                )
                .map_err(|_| corrupt())?,
                vec![receipt.clone()],
            )
            .map_err(|_| corrupt())?
            .encode()
            .map_err(|_| corrupt())?;
            applier.apply_frame(&wrapper)?;
        }
    }
    Err(corrupt())
}

fn construct(
    mut stage: RedbArchiveRestoreStage,
    receipt: riffdb_storage_api::AuthoritativeTransactionV3,
    stop: CommitSequence,
    cancellation: &AtomicBool,
) -> Result<RedbPrivateArchiveRestoreCandidate, StorageError> {
    cancelled(cancellation)?;
    stage.verify()?;
    let directory = &stage.stage.stage_cleanup.directory_guard;
    // Quarantine the existing marker before any partial authority is written.
    // A crash leaves an unopenable private stage, rebuilt from the selection.
    let marker = stage
        .stage
        .staged_format_marker_file
        .file_name()
        .ok_or_else(corrupt)?;
    let mut marker_file = directory.open_file(marker)?.into_std();
    crate::maintenance::path_guard::check_current_marker(&mut marker_file)?;
    if directory
        .regular_file_length(OsStr::new(PRIVATE_MARKER))?
        .is_some()
    {
        return Err(corrupt());
    }
    directory.rename(marker, OsStr::new(PRIVATE_MARKER))?;
    directory.sync()?;
    stage.stage.staged_format_marker_file = stage
        .stage
        .staged_database_file
        .parent()
        .ok_or_else(corrupt)?
        .join(PRIVATE_MARKER);
    prefix_edge("prefix-quarantined");
    let database = redb::Database::builder()
        .set_cache_size(64 * 1024 * 1024)
        .create_file(stage.file.try_clone().map_err(unavailable)?)
        .map_err(unavailable)?;
    let mut write = database.begin_write().map_err(unavailable)?;
    write.set_two_phase_commit(true);
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(unavailable)?;
    let history = crate::changelog_v3_roots::validate_retained_history_for_write(&write)?
        .ok_or_else(corrupt)?;
    history.advance(&receipt).map_err(|_| corrupt())?;
    let tables = crate::changelog_v3_write::table_inventory(&write)?;
    crate::command_prefix::validate_received_prefixes(&write, &tables, &receipt)?;
    for mutation in receipt.mutations() {
        if !tables.contains(mutation.namespace().table()) {
            return Err(corrupt());
        }
        crate::changelog_v3_write::check_predecessor(&write, mutation)?;
    }
    let (frontier, mutations) = graph::derive(&receipt, stop)?;
    for mutation in &mutations {
        crate::changelog_v3_write::check_predecessor(&write, mutation)?;
    }
    for mutation in &mutations {
        crate::changelog_v3_write::apply_mutation(&write, mutation)?;
    }
    {
        let mut meta = write.open_table(crate::layout::META).map_err(unavailable)?;
        if meta
            .remove(
                N::ReplicationFollowerState
                    .metadata_key()
                    .ok_or_else(corrupt)?,
            )
            .map_err(unavailable)?
            .is_none()
        {
            return Err(corrupt());
        }
        let app = meta
            .get(
                N::NextApplicationSequence
                    .metadata_key()
                    .ok_or_else(corrupt)?,
            )
            .map_err(unavailable)?
            .ok_or_else(corrupt)?;
        let admin = meta
            .get(
                N::NextAdministrationSequence
                    .metadata_key()
                    .ok_or_else(corrupt)?,
            )
            .map_err(unavailable)?
            .ok_or_else(corrupt)?;
        if crate::changelog_v3_roots::decode_physical_frontier(app.value(), admin.value())?
            != frontier
        {
            return Err(corrupt());
        }
    }
    cancelled(cancellation)?;
    prefix_edge("prefix-staged");
    write.commit().map_err(unavailable)?;
    prefix_edge("prefix-committed");
    drop(database);
    stage.verify()?;
    let construction_checksum = sha256_file(stage.staged_database_file())?;
    stage.verify()?;
    Ok(RedbPrivateArchiveRestoreCandidate {
        construction_checksum,
        binding: PrivateArchiveValidationBinding::new(history, frontier),
        stage: stage.stage,
        file: stage.file,
        selection: stage.selection,
        frontier,
    })
}

fn prefix_edge(_name: &str) {
    #[cfg(any(test, feature = "test-fixtures"))]
    if std::env::var("RIFFDB_PRIVATE_PREFIX_CRASH").as_deref() == Ok(_name) {
        std::process::exit(98);
    }
}
