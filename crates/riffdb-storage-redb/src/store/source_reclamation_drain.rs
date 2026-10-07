//! Closed admission preparation under an already granted source writer turn.
use super::*;

pub(crate) enum Readiness {
    Ready,
    PendingJournal,
    CheckpointPublication,
}

impl SharedRedb {
    pub(crate) fn source_drain_observation(&self) -> Result<[u64; 11], StorageError> {
        self.mutation_gate.source_drain_observation()
    }

    pub(crate) fn source_checkpoint_publication_pending(&self) -> Result<bool, StorageError> {
        Ok(self.async_checkpoint_in_flight()?
            || !self
                .journal_sealer
                .lock()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .as_ref()
                .is_none_or(std::thread::JoinHandle::is_finished))
    }

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
        if self.source_checkpoint_publication_pending()? {
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
                self.mutation_gate.clear_source_publication_request()?;
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
                self.mutation_gate.clear_source_publication_request()?;
                return Ok(Readiness::Ready);
            };
            let counts = published_suffix_counts(runtime);
            if !counts.is_some_and(|counts| {
                counts[0] <= crate::journal::SOURCE_SUFFIX_TRANSITIONS
                    && counts[3] <= crate::journal::SOURCE_SUFFIX_BYTES
            }) {
                if unpublished_suffix_is_accounted(runtime) || counts.is_some() {
                    // Only suggest the existing ordinary checkpoint. This
                    // refused source turn does not materialize, rotate, join
                    // or alter the authority of any acknowledged frame.
                    self.mutation_gate.note_source_publication_request()?;
                }
                let unpublished = runtime.unpublished_transitions != 0
                    || runtime.unpublished_bytes != 0
                    || runtime.unpublished_commands != 0
                    || runtime.unpublished_audits != 0;
                let reason = if unpublished {
                    crate::gate::SourceDrainRefusal::Unpublished
                } else if counts.is_none() {
                    crate::gate::SourceDrainRefusal::Unaccounted
                } else {
                    crate::gate::SourceDrainRefusal::Oversized
                };
                self.mutation_gate.note_source_drain_yield(
                    reason,
                    [
                        u64::try_from(runtime.suffix_transitions).unwrap_or(u64::MAX),
                        u64::try_from(runtime.suffix_bytes).unwrap_or(u64::MAX),
                        u64::try_from(runtime.unpublished_transitions).unwrap_or(u64::MAX),
                        u64::try_from(runtime.unpublished_bytes).unwrap_or(u64::MAX),
                        u64::from(runtime.last_sequence == runtime.published_sequence),
                        u64::from(
                            runtime.last_administration_sequence
                                == runtime.published_administration_sequence,
                        ),
                        u64::from(runtime.last_hash == runtime.published_hash),
                        u64::try_from(runtime.suffix_frames.len()).unwrap_or(u64::MAX),
                    ],
                )?;
                return Ok(Readiness::PendingJournal);
            }
            if runtime.suffix_transitions == 0 {
                self.mutation_gate.clear_source_publication_request()?;
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
        self.mutation_gate.clear_source_publication_request()?;
        Ok(Readiness::Ready)
    }
}

fn published_suffix_counts(runtime: &JournalRuntime) -> Option<[usize; 4]> {
    use crate::journal::{
        MAX_JOURNAL_FRAME_BYTES, MAX_JOURNAL_SUFFIX_BYTES, MAX_JOURNAL_SUFFIX_TRANSITIONS,
    };
    if runtime.unpublished_transitions != 0
        || runtime.unpublished_bytes != 0
        || runtime.unpublished_commands != 0
        || runtime.unpublished_audits != 0
        || runtime.last_sequence != runtime.published_sequence
        || runtime.last_administration_sequence != runtime.published_administration_sequence
        || runtime.last_hash != runtime.published_hash
        || runtime.suffix_transitions > MAX_JOURNAL_SUFFIX_TRANSITIONS
        || runtime.suffix_bytes > MAX_JOURNAL_SUFFIX_BYTES
        || runtime.suffix_frames.len() > MAX_JOURNAL_SUFFIX_TRANSITIONS
    {
        return None;
    }
    let mut counts = [0_usize; 4];
    for frame in &runtime.suffix_frames {
        let length = frame.encoded.as_bytes().len();
        if frame.transition_count == 0
            || length == 0
            || length > MAX_JOURNAL_FRAME_BYTES
            || frame.database_id != runtime.database_id
            || crate::journal::extent_frame_bytes(length) != Some(frame.placement.padded_len)
        {
            return None;
        }
        for (total, added) in counts.iter_mut().zip([
            usize::from(frame.transition_count),
            usize::from(frame.command_count),
            usize::from(frame.audit_count),
            length,
        ]) {
            let next = total.checked_add(added)?;
            *total = next;
        }
    }
    (counts
        == [
            runtime.suffix_transitions,
            runtime.suffix_commands,
            runtime.suffix_audits,
            runtime.suffix_bytes,
        ])
    .then_some(counts)
}

/// A scheduling hint only. Admission still refuses every unpublished frame.
/// Prove the exact published boundary and complete native pending tail, without
/// decoding, consuming a completion ticket or treating it as acknowledged.
fn unpublished_suffix_is_accounted(runtime: &JournalRuntime) -> bool {
    if runtime.unpublished_transitions == 0
        || runtime.unpublished_transitions > crate::journal::MAX_JOURNAL_TRANSITIONS
        || runtime.unpublished_bytes == 0
        || runtime.unpublished_bytes > crate::journal::MAX_JOURNAL_FRAME_BYTES
        || runtime.suffix_transitions > crate::journal::MAX_JOURNAL_SUFFIX_TRANSITIONS
        || runtime.suffix_bytes > crate::journal::MAX_JOURNAL_SUFFIX_BYTES
        || runtime.suffix_frames.len() > crate::journal::MAX_JOURNAL_SUFFIX_TRANSITIONS
    {
        return false;
    }
    let Some(first) = runtime.suffix_frames.first() else {
        return false;
    };
    let published = (
        runtime.published_sequence,
        runtime.published_administration_sequence,
        runtime.published_hash,
    );
    let mut tip = (
        first.predecessor_sequence,
        first.predecessor_administration_sequence,
        first.previous_hash,
    );
    let mut past_publication = tip == published;
    let mut total = [0usize; 4];
    let mut pending = [0usize; 4];
    for frame in &runtime.suffix_frames {
        let length = frame.encoded.as_bytes().len();
        if frame.transition_count == 0
            || length == 0
            || length > crate::journal::MAX_JOURNAL_FRAME_BYTES
            || frame.database_id != runtime.database_id
            || frame.frame_hash != frame.encoded.frame_hash()
            || (
                frame.predecessor_sequence,
                frame.predecessor_administration_sequence,
                frame.previous_hash,
            ) != tip
            || crate::journal::extent_frame_bytes(length) != Some(frame.placement.padded_len)
        {
            return false;
        }
        for (index, added) in [
            usize::from(frame.transition_count),
            usize::from(frame.command_count),
            usize::from(frame.audit_count),
            length,
        ]
        .into_iter()
        .enumerate()
        {
            let Some(next) = total[index].checked_add(added) else {
                return false;
            };
            total[index] = next;
            if past_publication {
                let Some(next) = pending[index].checked_add(added) else {
                    return false;
                };
                pending[index] = next;
            }
        }
        tip = (
            frame.covered_sequence,
            frame.covered_administration_sequence,
            frame.frame_hash,
        );
        if !past_publication && tip == published {
            past_publication = true;
        }
    }
    past_publication
        && tip
            == (
                runtime.last_sequence,
                runtime.last_administration_sequence,
                runtime.last_hash,
            )
        && total
            == [
                runtime.suffix_transitions,
                runtime.suffix_commands,
                runtime.suffix_audits,
                runtime.suffix_bytes,
            ]
        && pending
            == [
                runtime.unpublished_transitions,
                runtime.unpublished_commands,
                runtime.unpublished_audits,
                runtime.unpublished_bytes,
            ]
}

#[cfg(test)]
fn suffix_is_bounded(runtime: &JournalRuntime) -> bool {
    published_suffix_counts(runtime).is_some_and(|counts| {
        counts[0] <= crate::journal::SOURCE_SUFFIX_TRANSITIONS
            && counts[3] <= crate::journal::SOURCE_SUFFIX_BYTES
    })
}

#[cfg(test)]
#[path = "source_reclamation_drain_tests.rs"]
mod tests;
