//! Existing shared-store journal runtime and checkpoint extent rotation.
//! Extraction keeps the original publication, media and writer ownership intact.

use super::*;

enum JournalRuntimePurpose {
    Writer,
    GracefulClose,
}

impl SharedRedb {
    pub(super) fn journal_runtime(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Option<JournalRuntime>>, StorageError> {
        self.initialize_journal_runtime(JournalRuntimePurpose::Writer)
    }

    /// Closing still needs the exact active extent, but no capacity for future
    /// writes. Existing sealing jobs are joined by the ordinary close barrier.
    pub(super) fn journal_runtime_for_graceful_close(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Option<JournalRuntime>>, StorageError> {
        self.initialize_journal_runtime(JournalRuntimePurpose::GracefulClose)
    }

    fn initialize_journal_runtime(
        &self,
        purpose: JournalRuntimePurpose,
    ) -> Result<std::sync::MutexGuard<'_, Option<JournalRuntime>>, StorageError> {
        let mut runtime = self
            .journal_runtime
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if runtime.is_none() {
            let frontier = self
                .durable_read_frontier
                .read()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            let predecessor = frontier
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let database_id = read_identity_from_read_transaction(predecessor)?;
            let checkpoint_sequence = read_commit_tail(predecessor)?;
            let checkpoint_administration_sequence = read_administration_tail(predecessor)?;
            let changelog_history = if crate::changelog_v3_journal::has_recovery_roots(predecessor)?
            {
                Some(
                    crate::changelog_v3_roots::read_checkpoint_roots(predecessor)?
                        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?,
                )
            } else {
                None
            };
            drop(frontier);
            let path = crate::journal::journal_path(&self.path);
            let (mut header, mut tail) = match crate::journal::scan_journal_with_media(
                self.journal_media.as_ref(),
                &path,
                database_id,
                |_| Ok(()),
            )
            .map_err(journal_io_error)?
            {
                Some((header, tail)) => (header, tail),
                None => (
                    crate::journal::JournalFileHeader::with_frontiers(
                        database_id,
                        checkpoint_sequence,
                        checkpoint_administration_sequence,
                        [0; 32],
                    ),
                    crate::journal::JournalScanTail {
                        last_sequence: checkpoint_sequence,
                        last_administration_sequence: checkpoint_administration_sequence,
                        last_hash: [0; 32],
                        incomplete_tail: false,
                        complete_bytes: 0,
                        transition_count: 0,
                        command_count: 0,
                        audit_count: 0,
                    },
                ),
            };
            let empty_tail_matches_header = header.database_id() == database_id
                && tail.last_sequence == header.checkpoint_sequence()
                && tail.last_administration_sequence == header.checkpoint_administration_sequence()
                && !tail.incomplete_tail
                && tail.transition_count == 0
                && tail.command_count == 0
                && tail.audit_count == 0;
            if empty_tail_matches_header
                && header.checkpoint_sequence() <= checkpoint_sequence
                && header.checkpoint_administration_sequence() <= checkpoint_administration_sequence
                && (header.checkpoint_sequence() != checkpoint_sequence
                    || header.checkpoint_administration_sequence()
                        != checkpoint_administration_sequence)
            {
                let checkpoint_hash = tail.last_hash;
                header = crate::journal::JournalFileHeader::with_frontiers(
                    database_id,
                    checkpoint_sequence,
                    checkpoint_administration_sequence,
                    checkpoint_hash,
                );
                crate::journal::advance_journal_anchor_with_media(
                    self.journal_media.as_ref(),
                    &path,
                    &header,
                )
                .map_err(journal_io_error)?;
                tail = crate::journal::JournalScanTail {
                    last_sequence: checkpoint_sequence,
                    last_administration_sequence: checkpoint_administration_sequence,
                    last_hash: checkpoint_hash,
                    incomplete_tail: false,
                    complete_bytes: tail.complete_bytes,
                    transition_count: 0,
                    command_count: 0,
                    audit_count: 0,
                };
            }
            if header.database_id() != database_id
                || header.checkpoint_sequence() != checkpoint_sequence
                || header.checkpoint_administration_sequence() != checkpoint_administration_sequence
                || tail.last_sequence != checkpoint_sequence
                || tail.last_administration_sequence != checkpoint_administration_sequence
                || tail.incomplete_tail
                || tail.transition_count != 0
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let spare_header = crate::journal::JournalFileHeader::with_frontiers(
                database_id,
                tail.last_sequence,
                tail.last_administration_sequence,
                tail.last_hash,
            );
            let idle = self
                .idle_journal_lane
                .lock()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .take();
            let physical_bytes = crate::journal::extent_used_bytes(tail.complete_bytes);
            let lane = match idle {
                Some(idle)
                    if idle.database_id == database_id
                        && idle.physical_bytes == physical_bytes
                        && Arc::strong_count(&idle.lane) == 1
                        && idle
                            .lane
                            .can_reuse_cursor(
                                self.journal_media.as_ref(),
                                &path,
                                &header,
                                physical_bytes,
                            )
                            .map_err(journal_io_error)? =>
                {
                    idle.lane
                }
                other => {
                    drop(other);
                    Arc::new(
                        crate::journal::JournalLane::open_with_media_and_capacity(
                            self.journal_media.as_ref(),
                            &path,
                            &header,
                            self.extent_profile.extent_data_bytes(),
                            #[cfg(feature = "test-fixtures")]
                            self.test_controller.clone(),
                        )
                        .map_err(journal_io_error)?,
                    )
                }
            };
            if matches!(purpose, JournalRuntimePurpose::Writer) {
                // Refresh an existing spare's generation now; a missing one (the
                // last install consumed it) is zero-filled off the writer's path
                // once the active extent profile's preparation threshold is reached.
                let spare_path = crate::journal::spare_journal_path(&self.path);
                let spare_guard = self
                    .journal_spare
                    .lock()
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                if self
                    .journal_media
                    .try_exists(&spare_path)
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
                {
                    crate::journal::reset_journal_after_with_media_and_capacity(
                        self.journal_media.as_ref(),
                        &spare_path,
                        &spare_header,
                        &path,
                        self.extent_profile.extent_data_bytes(),
                    )
                    .map_err(journal_io_error)?;
                    drop(spare_guard);
                } else {
                    drop(spare_guard);
                    if self.extent_profile.should_prepare_spare(physical_bytes, 0) {
                        self.ensure_journal_spare(spare_header)?;
                    }
                }
            }
            *runtime = Some(JournalRuntime {
                changelog_history,
                lane,
                database_id,
                last_sequence: checkpoint_sequence,
                last_administration_sequence: checkpoint_administration_sequence,
                last_hash: header.checkpoint_frame_hash(),
                published_sequence: checkpoint_sequence,
                published_administration_sequence: checkpoint_administration_sequence,
                published_hash: header.checkpoint_frame_hash(),
                suffix_transitions: 0,
                suffix_commands: 0,
                suffix_audits: 0,
                suffix_bytes: 0,
                // Frames before the anchor stay in the extent (ADR-0263), so
                // physical capacity starts from the retained prefix.
                suffix_physical_bytes: crate::journal::extent_used_bytes(tail.complete_bytes),
                suffix_frames: Vec::new(),
                unpublished_transitions: 0,
                unpublished_commands: 0,
                unpublished_audits: 0,
                unpublished_bytes: 0,
                reanchor_required: false,
            });
        }
        Ok(runtime)
    }

    pub(super) fn maybe_start_async_checkpoint(self: &Arc<Self>) -> Result<(), StorageError> {
        self.start_async_checkpoint(false)
    }

    pub(super) fn start_async_checkpoint_for_direct_write(
        self: &Arc<Self>,
    ) -> Result<(), StorageError> {
        self.start_async_checkpoint(true)
    }

    fn start_async_checkpoint(self: &Arc<Self>, direct_write: bool) -> Result<(), StorageError> {
        if self.mutation_gate.source_publication_requested()? {
            // The ordinary writer owns this lease. Complete only its existing
            // submitted FIFO prefix, retaining results for original owners.
            self.publish_pending_journal_prefix()?;
        }
        let start_physical_bytes = self
            .extent_profile
            .checkpoint_start_physical_bytes()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;

        let mut checkpoint_guard = self
            .journal_checkpoint
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if checkpoint_guard.is_some() {
            return Ok(());
        }
        let mut runtime_guard = self
            .journal_runtime
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let Some(runtime) = runtime_guard.as_ref() else {
            return Ok(());
        };
        if runtime.unpublished_transitions != 0 || runtime.unpublished_bytes != 0 {
            return Ok(());
        }
        #[cfg(feature = "test-fixtures")]
        let requested = runtime.suffix_transitions != 0
            && self
                .test_controller
                .as_ref()
                .is_some_and(RedbTestController::take_checkpoint_test_request);
        #[cfg(not(feature = "test-fixtures"))]
        let requested = false;
        let requested = requested
            || (direct_write && runtime.suffix_transitions != 0)
            || (runtime.suffix_transitions != 0
                && self.mutation_gate.source_checkpoint_requested()?);
        if !requested
            && !runtime.reanchor_required
            && runtime.suffix_transitions < JOURNAL_CHECKPOINT_START_TRANSITIONS
            && runtime.suffix_bytes < JOURNAL_CHECKPOINT_START_BYTES
            && runtime.suffix_physical_bytes < start_physical_bytes
        {
            if self
                .extent_profile
                .should_prepare_spare(runtime.suffix_physical_bytes, runtime.suffix_transitions)
            {
                let spare_header = crate::journal::JournalFileHeader::with_frontiers(
                    runtime.database_id,
                    runtime.last_sequence,
                    runtime.last_administration_sequence,
                    runtime.last_hash,
                );
                drop(runtime_guard);
                drop(checkpoint_guard);
                let spare_path = crate::journal::spare_journal_path(&self.path);
                let has_spare = {
                    let _spare_guard = self
                        .journal_spare
                        .lock()
                        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                    self.journal_media
                        .try_exists(&spare_path)
                        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
                };
                if !has_spare {
                    self.ensure_journal_spare(spare_header)?;
                }
            }
            return Ok(());
        }
        let checkpoint_triggers = [
            runtime.suffix_transitions >= JOURNAL_CHECKPOINT_START_TRANSITIONS,
            runtime.suffix_bytes >= JOURNAL_CHECKPOINT_START_BYTES,
            runtime.suffix_physical_bytes >= start_physical_bytes,
            runtime.reanchor_required,
        ];
        let covered_view = self
            .composite_publication
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .capture()?;
        if covered_view.overlay().published_application() != runtime.last_sequence
            || covered_view.overlay().published_administration()
                != runtime.last_administration_sequence
            || covered_view.overlay().terminal_frame_hash() != runtime.last_hash
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let runtime = runtime_guard
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        drop(runtime_guard);
        if Arc::strong_count(&runtime.lane) != 1 {
            self.restore_journal_runtime(runtime)?;
            return Ok(());
        }
        drop(runtime.lane);

        let active_path = crate::journal::journal_path(&self.path);
        let checkpoint_path = crate::journal::checkpoint_journal_path(&self.path);
        let spare_path = crate::journal::spare_journal_path(&self.path);
        if self
            .journal_media
            .try_exists(&checkpoint_path)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let covered_overlay = covered_view.overlay();
        let overlay_charge = covered_overlay.charged_bytes();
        if covered_overlay.transition_count() != runtime.suffix_transitions
            || covered_overlay.encoded_frame_bytes() != runtime.suffix_bytes
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let batch = JournalCheckpointBatch {
            database_id: runtime.database_id,
            checkpoint_sequence: covered_overlay.checkpoint().application_frontier(),
            checkpoint_administration_sequence: covered_overlay
                .checkpoint()
                .administration_frontier(),
            checkpoint_hash: covered_overlay.checkpoint().terminal_frame_hash(),
            last_sequence: runtime.last_sequence,
            last_administration_sequence: runtime.last_administration_sequence,
            last_hash: runtime.last_hash,
            transition_count: runtime.suffix_transitions,
            command_count: runtime.suffix_commands,
            audit_count: runtime.suffix_audits,
            encoded_bytes: runtime.suffix_bytes,
            frames: runtime.suffix_frames,
        };
        let next_header = crate::journal::JournalFileHeader::with_frontiers(
            runtime.database_id,
            runtime.last_sequence,
            runtime.last_administration_sequence,
            runtime.last_hash,
        );
        // The previous checkpoint extent must have left the checkpoint name.
        self.free_checkpoint_name(runtime.database_id)?;
        let media = self.journal_media.as_ref();
        // The spare name moves under the lock the background preparation takes,
        // so a preparation that finishes now never replaces the spare being used.
        let spare_guard = self
            .journal_spare
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        crate::journal::reset_journal_after_with_media_and_capacity(
            media,
            &spare_path,
            &next_header,
            &active_path,
            self.extent_profile.extent_data_bytes(),
        )
        .map_err(journal_io_error)?;
        media
            .rename(&active_path, &checkpoint_path)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        // Persist removal of the old active name before the prepared spare is
        // allowed to replace it. A crash between these two syncs leaves the
        // complete checkpoint extent authoritative and no active suffix.
        crate::journal::sync_parent_directory_with_media(media, &active_path)
            .map_err(journal_io_error)?;
        media
            .rename(&spare_path, &active_path)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        crate::journal::sync_parent_directory_with_media(media, &active_path)
            .map_err(journal_io_error)?;
        drop(spare_guard);
        let lane = Arc::new(
            crate::journal::JournalLane::open_with_media_and_capacity(
                media,
                &active_path,
                &next_header,
                self.extent_profile.extent_data_bytes(),
                #[cfg(feature = "test-fixtures")]
                self.test_controller.clone(),
            )
            .map_err(journal_io_error)?,
        );
        *self
            .journal_runtime
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))? =
            Some(JournalRuntime {
                changelog_history: runtime.changelog_history,
                lane,
                database_id: runtime.database_id,
                last_sequence: runtime.last_sequence,
                last_administration_sequence: runtime.last_administration_sequence,
                last_hash: runtime.last_hash,
                published_sequence: runtime.published_sequence,
                published_administration_sequence: runtime.published_administration_sequence,
                published_hash: runtime.published_hash,
                suffix_transitions: 0,
                suffix_commands: 0,
                suffix_audits: 0,
                suffix_bytes: 0,
                suffix_physical_bytes: 0,
                suffix_frames: Vec::new(),
                unpublished_transitions: 0,
                unpublished_commands: 0,
                unpublished_audits: 0,
                unpublished_bytes: 0,
                reanchor_required: false,
            });

        let receiver = Arc::new(CheckpointCompletion::default());
        let sender = CheckpointPublisher(Arc::clone(&receiver));
        *checkpoint_guard = Some(AsyncJournalCheckpoint {
            batch: batch.clone(),
            covered_view,
            completion: Some(receiver),
            result: None,
        });
        crate::checkpoint_census::overlay_charge(overlay_charge);
        crate::checkpoint_census::CHECKPOINTS.started(
            batch.transition_count,
            batch.command_count,
            batch.encoded_bytes,
            checkpoint_triggers,
        );
        let worker_shared = Arc::clone(self);
        let worker_batch = batch.clone();
        if std::thread::Builder::new()
            .name("riffdb-checkpoint".to_string())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                let result = worker_shared.materialize_checkpoint_batch(&worker_batch);
                if result.is_ok() {
                    crate::checkpoint_census::CHECKPOINTS.materialized();
                } else {
                    crate::checkpoint_census::CHECKPOINTS.failed();
                }
                #[cfg(feature = "test-fixtures")]
                let controller = worker_shared.test_controller.clone();
                sender.publish(result);
                drop(worker_shared);
                #[cfg(feature = "test-fixtures")]
                if let Some(controller) = controller {
                    controller
                        .checkpoint_test_pause(crate::hooks::RedbCheckpointTestPoint::Materialized);
                }
            })
            .is_err()
        {
            crate::checkpoint_census::CHECKPOINTS.failed();
            let error = storage_error(StorageErrorKind::Unavailable);
            if let Some(checkpoint) = checkpoint_guard.as_mut() {
                checkpoint.completion = None;
                checkpoint.result = Some(Err(error.clone()));
            }
            self.fence_writes();
            return Err(error);
        }
        self.mutation_gate.clear_source_checkpoint_request()?;
        Ok(())
    }
}
