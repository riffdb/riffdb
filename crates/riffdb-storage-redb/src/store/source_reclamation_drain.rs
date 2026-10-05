//! Closed admission preparation under an already granted source writer turn.
use super::*;

pub(crate) enum Readiness {
    Ready,
    PendingJournal,
    CheckpointPublication,
}

impl SharedRedb {
    pub(crate) fn retire_online_source_admission(&self) {
        self.mutation_gate.retire_source_admission();
    }

    pub(super) fn prepare_online_source_root(self: &Arc<Self>) -> Result<Readiness, StorageError> {
        if self.write_fenced.load(Ordering::Acquire) || self.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        // Check before capturing a root: that operation can otherwise wait for
        // native publication. No receiver or sealer is joined under this lease.
        // Foreground checkpoint installation keeps its existing protocol.
        if self.async_checkpoint_in_flight()?
            || !self
                .journal_sealer
                .lock()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .as_ref()
                .is_none_or(std::thread::JoinHandle::is_finished)
        {
            return Ok(Readiness::CheckpointPublication);
        }
        {
            let root = self.capture_checkpoint_root()?;
            let meta = root.open_table(META).map_err(table_error)?;
            if let Some(row) = meta
                .get(META_CLEAN_CLOSE_LIFECYCLE)
                .map_err(precommit_storage_error)?
            {
                let lifecycle = crate::clean_close::CleanCloseLifecycle::decode(row.value())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                if matches!(
                    lifecycle.state(),
                    crate::clean_close::CleanCloseState::Clean(_)
                ) {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
            }
            // An unactivated source has no source-control mutation authority.
            // Preserve its previous no-op capture without forcing any drain.
            if crate::changelog_v3_roots::read_checkpoint_roots(&root)?.is_none() {
                return Ok(Readiness::Ready);
            }
        }
        let database_id = {
            // Inspect the existing publication metadata only. The general
            // accessor can create a journal lane; absence here proves no
            // in-memory suffix and must not initialize one during admission.
            let runtime = self
                .journal_runtime
                .lock()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            let Some(runtime) = runtime.as_ref() else {
                return Ok(Readiness::Ready);
            };
            if !suffix_is_bounded(runtime) {
                self.mutation_gate.note_source_drain_yield()?;
                return Ok(Readiness::PendingJournal);
            }
            if runtime.suffix_transitions == 0 {
                return Ok(Readiness::Ready);
            }
            runtime.database_id
        };
        {
            let root = self.capture_checkpoint_root()?;
            if read_identity_from_read_transaction(&root)? != database_id {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
        self.materialize_published_suffix_for_barrier(true)?;
        Ok(Readiness::Ready)
    }
}

fn suffix_is_bounded(runtime: &JournalRuntime) -> bool {
    use crate::journal::{SOURCE_SUFFIX_BYTES, SOURCE_SUFFIX_TRANSITIONS};
    if runtime.unpublished_transitions != 0
        || runtime.unpublished_bytes != 0
        || runtime.unpublished_commands != 0
        || runtime.unpublished_audits != 0
        || runtime.last_sequence != runtime.published_sequence
        || runtime.last_administration_sequence != runtime.published_administration_sequence
        || runtime.last_hash != runtime.published_hash
        || runtime.suffix_transitions > SOURCE_SUFFIX_TRANSITIONS
        || runtime.suffix_bytes > SOURCE_SUFFIX_BYTES
        || runtime.suffix_frames.len() > SOURCE_SUFFIX_TRANSITIONS
    {
        return false;
    }
    let mut counts = [0_usize; 4];
    for frame in &runtime.suffix_frames {
        let length = frame.encoded.as_bytes().len();
        if frame.transition_count == 0
            || length == 0
            || length > SOURCE_SUFFIX_BYTES
            || crate::journal::extent_frame_bytes(length) != Some(frame.placement.padded_len)
        {
            return false;
        }
        for (total, added) in counts.iter_mut().zip([
            usize::from(frame.transition_count),
            usize::from(frame.command_count),
            usize::from(frame.audit_count),
            length,
        ]) {
            let Some(next) = total.checked_add(added) else {
                return false;
            };
            *total = next;
        }
    }
    counts
        == [
            runtime.suffix_transitions,
            runtime.suffix_commands,
            runtime.suffix_audits,
            runtime.suffix_bytes,
        ]
}

#[cfg(test)]
#[path = "source_reclamation_drain_tests.rs"]
mod tests;
