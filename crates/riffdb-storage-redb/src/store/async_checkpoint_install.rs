//! Installs a completed asynchronous checkpoint once unpublished work is quiescent.
use super::*;

impl SharedRedb {
    pub(super) fn finish_async_checkpoint_if_quiescent(
        self: &Arc<Self>,
    ) -> Result<(), StorageError> {
        let mut checkpoint = match self.journal_checkpoint.lock() {
            Ok(checkpoint) => checkpoint,
            Err(poisoned) => {
                drop(poisoned.into_inner());
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
            }
        };
        let Some(state) = checkpoint.as_mut() else {
            return Ok(());
        };
        let runtime = match self.journal_runtime.lock() {
            Ok(runtime) => runtime,
            Err(poisoned) => {
                drop(poisoned.into_inner());
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
            }
        };
        let Some(runtime) = runtime.as_ref() else {
            return Ok(());
        };
        if runtime.unpublished_transitions != 0 || runtime.unpublished_bytes != 0 {
            return Ok(());
        }
        let Some(result) = state.result.as_ref() else {
            return Ok(());
        };
        if let Err(error) = result {
            self.fence_writes();
            return Err(error.clone());
        }
        let batch = state.batch.clone();
        let covered_view = state.covered_view.clone();
        let mut publication_began = false;
        let rebase_result = (|| -> Result<(), StorageError> {
            let current = self
                .composite_publication
                .read()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .capture()?;
            let root = self.capture_checkpoint_root()?;
            if read_commit_tail(&root)? != batch.last_sequence
                || read_administration_tail(&root)? != batch.last_administration_sequence
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            if covered_view.overlay().published_application() != batch.last_sequence
                || covered_view.overlay().published_administration()
                    != batch.last_administration_sequence
                || covered_view.overlay().terminal_frame_hash() != batch.last_hash
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            // Capacity refusal while preparing the exact rebase leaves the
            // completed I/O result and its original descriptor retryable.
            let prepared = current.prepare_rebase_after(&covered_view, root, batch.last_hash)?;
            let mut private = self
                .private_composite_frontier
                .lock()
                .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
            if private
                .as_ref()
                .is_none_or(|private| !private.same_allocation(&current))
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let successor = {
                let publication = self
                    .composite_publication
                    .read()
                    .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
                let publication = publication
                    .as_ref()
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                // All ordinary fallible preparation has finished. Move the
                // single admitted descriptor only into this publication owner.
                let descriptor = checkpoint
                    .as_mut()
                    .and_then(|state| state.locator_descriptor.take())
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                let successor =
                    riffdb_types::OwnedCapacitySharedV1::new(prepared.finish(descriptor));
                publication.publish_rebased(&current, successor.clone())?;
                publication_began = true;
                successor
            };
            *private = Some(successor.clone());
            *self
                .durable_read_frontier
                .write()
                .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))? =
                Some(successor.checkpoint_root_shared());
            #[cfg(feature = "test-fixtures")]
            if let Some(controller) = &self.test_controller {
                controller
                    .checkpoint_test_pause(crate::hooks::RedbCheckpointTestPoint::BeforeReclaim);
            }
            // The installed extent is retained history now (ADR-0263): it is
            // sealed, not recycled. The sealer retires and seals it off the
            // writer's path; the next rotation waits for the checkpoint name.
            self.start_journal_sealer(crate::journal::JournalFileHeader::with_frontiers(
                runtime.database_id,
                runtime.last_sequence,
                runtime.last_administration_sequence,
                runtime.last_hash,
            ))?;
            #[cfg(feature = "test-fixtures")]
            if let Some(controller) = &self.test_controller {
                controller.checkpoint_test_pause(crate::hooks::RedbCheckpointTestPoint::Reclaimed);
            }
            *checkpoint = None;
            crate::checkpoint_census::CHECKPOINTS.installed();
            self.note_journal_checkpoint_finished();
            Ok(())
        })();
        match rebase_result {
            Ok(()) => Ok(()),
            Err(error) => {
                if publication_began
                    || !matches!(
                        error.kind(),
                        StorageErrorKind::Unavailable | StorageErrorKind::LimitExceeded
                    )
                {
                    self.fence_writes();
                }
                Err(error)
            }
        }
    }
}
