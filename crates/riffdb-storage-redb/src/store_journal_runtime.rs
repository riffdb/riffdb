//! Existing shared-store journal runtime and checkpoint extent rotation.
//! Extraction keeps the original publication, media and writer ownership intact.

use super::*;

impl SharedRedb {
    pub(super) fn journal_runtime(
        &self,
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
                crate::journal::reset_journal_with_media(
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
            let lane = Arc::new(
                crate::journal::JournalLane::open_with_media(
                    self.journal_media.as_ref(),
                    &path,
                    &header,
                )
                .map_err(journal_io_error)?,
            );
            crate::journal::reset_journal_after_with_media(
                self.journal_media.as_ref(),
                &crate::journal::spare_journal_path(&self.path),
                &spare_header,
                &path,
            )
            .map_err(journal_io_error)?;
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
                suffix_physical_bytes: 0,
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
        let start_physical_bytes = journal_checkpoint_start_physical_bytes()
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
        if !runtime.reanchor_required
            && runtime.suffix_transitions < JOURNAL_CHECKPOINT_START_TRANSITIONS
            && runtime.suffix_bytes < JOURNAL_CHECKPOINT_START_BYTES
            && runtime.suffix_physical_bytes < start_physical_bytes
        {
            return Ok(());
        }
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
        let media = self.journal_media.as_ref();
        crate::journal::reset_journal_after_with_media(
            media,
            &spare_path,
            &next_header,
            &active_path,
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
        let lane = Arc::new(
            crate::journal::JournalLane::open_with_media(media, &active_path, &next_header)
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

        let (sender, receiver) = std::sync::mpsc::channel();
        *checkpoint_guard = Some(AsyncJournalCheckpoint {
            batch: batch.clone(),
            covered_view,
            completion: Some(receiver),
            result: None,
        });
        let worker_shared = Arc::clone(self);
        let worker_batch = batch.clone();
        if std::thread::Builder::new()
            .name("riffdb-checkpoint".to_string())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                let result = worker_shared.materialize_checkpoint_batch(&worker_batch);
                let _ = sender.send(result);
            })
            .is_err()
        {
            let error = storage_error(StorageErrorKind::Unavailable);
            if let Some(checkpoint) = checkpoint_guard.as_mut() {
                checkpoint.completion = None;
                checkpoint.result = Some(Err(error.clone()));
            }
            self.fence_writes();
            return Err(error);
        }
        Ok(())
    }
}
