//! Same-process materialization of already validated journal checkpoint batches.
//! Receipt integration must retain this existing single durable transaction and
//! its exact retained mutation proofs; it must not reconstruct from latest rows.

use redb::{Durability, ReadableTable};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3, ChangelogAttributionV3,
    CompositeTableV1, StorageError, StorageErrorKind,
};
use riffdb_types::DualFrontier;

use super::{JournalCheckpointBatch, SharedRedb, journal_io_error};
use crate::changelog_v3_write::{PreparedHistoryAdvance, value_error};
use crate::error::{storage_error, transaction_error};

#[cfg(test)]
#[path = "store_journal_checkpoint_tests.rs"]
mod tests;

impl SharedRedb {
    pub(super) fn materialize_checkpoint_batch(
        &self,
        batch: &JournalCheckpointBatch,
    ) -> Result<(), StorageError> {
        #[cfg(feature = "test-fixtures")]
        if let Some(controller) = &self.test_controller {
            controller.checkpoint_test_pause(
                crate::hooks::RedbCheckpointTestPoint::BeforeMaterialization,
            );
        }
        self.register_history_resolver(batch.database_id);
        let mut transaction = self.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(false);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let v3 = crate::changelog_v3_journal::has_write_recovery_roots(&transaction)?;
        let mut footprint = if v3 {
            crate::source_continuation::SourceContinuationFootprint::fresh()
        } else {
            crate::source_continuation::SourceContinuationFootprint::unknown()
        };
        let mut last_sequence = batch.checkpoint_sequence;
        let mut last_administration_sequence = batch.checkpoint_administration_sequence;
        let mut last_hash = batch.checkpoint_hash;
        let mut transition_count = 0_usize;
        let mut command_count = 0_usize;
        let mut audit_count = 0_usize;
        let mut encoded_bytes = 0_usize;
        // The batch is one redb transaction: no state between two of its frames
        // is durable or visible. The physical roots, including the terminal
        // receipt the history index resolves, are therefore validated in full
        // once before the first frame and once after the last. Between them the
        // history state is the successor the previous frame staged here, and
        // `prepare_chained` re-checks every stored root row against it.
        let mut chain = if v3 {
            Some(
                crate::changelog_v3_roots::read_checkpoint_roots_for_write(&transaction)?
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?,
            )
        } else {
            None
        };
        for frame in &batch.frames {
            if frame.database_id != batch.database_id
                || frame.predecessor_sequence != last_sequence
                || frame.predecessor_administration_sequence != last_administration_sequence
                || frame.previous_hash != last_hash
                || frame.encoded.frame_hash() != frame.frame_hash
                || frame.encoded.transition_count() != frame.transition_count
                || frame.encoded.command_count() != frame.command_count
                || frame.encoded.audit_count() != frame.audit_count
                || frame.encoded.covered_sequence() != frame.covered_sequence
                || frame.encoded.covered_administration_sequence()
                    != frame.covered_administration_sequence
            {
                let _ = transaction.abort();
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let advance = if let Some(history) = chain {
                let binding = AuthoritativeTransactionBindingV3 {
                    database_id: frame.database_id,
                    history_incarnation: history.lineage().history_incarnation(),
                    predecessor: Some(history.tail().sequence()),
                    sequence: history
                        .expected_allocator()
                        .allocate_one()
                        .map_err(value_error)?
                        .0,
                    predecessor_frontier: DualFrontier::new(
                        frame.predecessor_sequence,
                        frame.predecessor_administration_sequence,
                    ),
                    covered_frontier: DualFrontier::new(
                        frame.covered_sequence,
                        frame.covered_administration_sequence,
                    ),
                    prior_history_hash: history.tail().history_hash(),
                };
                let source = match frame.encoded.kind() {
                    crate::journal::JournalFrameKind::Command => {
                        ChangelogAttributionV3::JournaledApplicationGroup
                    }
                    crate::journal::JournalFrameKind::ServiceAudit => {
                        ChangelogAttributionV3::JournaledServiceAudit
                    }
                    crate::journal::JournalFrameKind::RetentionPrune => {
                        ChangelogAttributionV3::RetentionPrune
                    }
                };
                let receipt = crate::changelog_v3::receipt_from_validated_mutations_for_catalog(
                    // The same binding was proven before admission to the lane.
                    binding,
                    source,
                    &frame.mutations,
                    history.lineage().catalog_digest(),
                )
                .map_err(value_error)?;
                if frame.changelog_binding != Some(binding) {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                Some(PreparedHistoryAdvance::prepare_chained(
                    &transaction,
                    history,
                    &receipt,
                )?)
            } else {
                if frame.changelog_binding.is_some() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                None
            };
            footprint.combine(
                crate::source_continuation::SourceContinuationFootprint::composite_checked(
                    last_administration_sequence,
                    &frame.mutations,
                    |mutation| {
                        let table = transaction
                            .open_table(crate::layout::AUDIT)
                            .map_err(crate::error::table_error)?;
                        Ok(table
                            .get(mutation.key())
                            .map_err(crate::error::precommit_storage_error)?
                            .map(|row| row.value().to_vec()))
                    },
                    |sequence| {
                        let table = transaction
                            .open_table(crate::layout::COMMITS)
                            .map_err(crate::error::table_error)?;
                        let key = crate::keys::encode_application_sequence_key(sequence);
                        let mut rows = table
                            .range::<&[u8]>((
                                std::ops::Bound::Unbounded,
                                std::ops::Bound::Included(key.as_slice()),
                            ))
                            .map_err(crate::error::precommit_storage_error)?;
                        rows.next_back()
                            .transpose()
                            .map_err(crate::error::precommit_storage_error)
                            .map(|row| {
                                row.map(|(key, value)| {
                                    (key.value().to_vec(), value.value().to_vec())
                                })
                            })
                    },
                )?,
            );
            for (index, mutation) in frame.mutations.iter().enumerate() {
                if mutation.table() == CompositeTableV1::Meta
                    && Some(mutation.key())
                        == N::NextChangelogTransaction
                            .metadata_key()
                            .map(str::as_bytes)
                {
                    if advance.is_none() {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    // The shared receipt fold proved this one checked allocation.
                    // Only the non-recursive history writer may stage it.
                    continue;
                }
                // A command segment is written as a history stub naming the
                // retained frame (ADR-0263 Amendment 3); every other row as is.
                if let Some(stub) = segment_stub(frame, index, mutation)? {
                    crate::journal::apply_byte_mutation_parts(
                        &transaction,
                        crate::layout::COMMITS,
                        mutation.key(),
                        Some(&stub),
                        None,
                    )
                    .map_err(journal_io_error)?;
                } else {
                    crate::journal::apply_validated_composite_mutation(&transaction, mutation)
                        .map_err(journal_io_error)?;
                }
            }
            if let Some(advance) = advance {
                chain = Some(advance.successor());
                if frame.encoded.records_changelog() {
                    let stub = frame_locator(frame).receipt_stub()?;
                    advance.stage_within_batch(&transaction, Some(&stub))?;
                } else {
                    // Only a frame that records its binding can re-derive its
                    // receipt, so any other keeps the receipt bytes.
                    advance.stage_within_batch(&transaction, None)?;
                }
            }
            transition_count = transition_count
                .checked_add(usize::from(frame.transition_count))
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            command_count = command_count
                .checked_add(usize::from(frame.command_count))
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            audit_count = audit_count
                .checked_add(usize::from(frame.audit_count))
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            encoded_bytes = encoded_bytes
                .checked_add(frame.encoded.as_bytes().len())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            last_sequence = frame.covered_sequence;
            last_administration_sequence = frame.covered_administration_sequence;
            last_hash = frame.frame_hash;
        }
        if last_sequence != batch.last_sequence
            || last_administration_sequence != batch.last_administration_sequence
            || last_hash != batch.last_hash
            || transition_count != batch.transition_count
            || command_count != batch.command_count
            || audit_count != batch.audit_count
            || encoded_bytes != batch.encoded_bytes
        {
            let _ = transaction.abort();
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        // Bind the actual physical post-images of the whole batch, including
        // the terminal receipt resolved through the history index.
        if v3 && crate::changelog_v3_roots::read_checkpoint_roots_for_write(&transaction)? != chain
        {
            let _ = transaction.abort();
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        #[cfg(test)]
        crate::changelog_v3_journal::recovery_crash_edge("checkpoint-staged");
        #[cfg(feature = "test-fixtures")]
        if let Some(controller) = &self.test_controller {
            controller.checkpoint_test_pause(
                crate::hooks::RedbCheckpointTestPoint::BeforeCheckpointCommit,
            );
            controller.before_commit(crate::hooks::RedbTestOperation::JournalCheckpoint)?;
        }
        // This changes backend write selection only. redb still finalizes all
        // checksums and performs every growth and final Immediate sync.
        let _rewrite = self
            .checkpoint_rewrite
            .enter()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        self.commit_with_source_footprint(transaction, footprint)?;
        #[cfg(test)]
        crate::changelog_v3_journal::recovery_crash_edge("checkpoint-committed");
        Ok(())
    }
}

/// The history stub for a command segment the frame inserts, or `None` for
/// any other mutation (and for a COMMITS change that is not a fresh insert,
/// which keeps its exact bytes).
fn segment_stub(
    frame: &super::ValidatedCheckpointFrame,
    index: usize,
    mutation: &riffdb_storage_api::CompositeMutationV1,
) -> Result<Option<Vec<u8>>, StorageError> {
    let (riffdb_storage_api::CompositeTableV1::Commits, Some(value), None) =
        (mutation.table(), mutation.value(), mutation.expected_hash())
    else {
        return Ok(None);
    };
    // The writer recorded the summary of each segment it built; any other
    // fresh COMMITS value is decoded to learn whether it is one.
    match frame
        .segment_summaries
        .iter()
        .find(|(staged, _)| *staged == index)
    {
        Some((_, summary)) => {
            // A summary names its segment by the COMMITS key it is stored under.
            if summary
                .first_commit_sequence
                .map(crate::keys::encode_application_sequence_key)
                .is_none_or(|key| key.as_slice() != mutation.key())
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            frame_locator(frame).summary_stub(*summary).map(Some)
        }
        None => frame_locator(frame).segment_stub(value),
    }
}

fn frame_locator(frame: &super::ValidatedCheckpointFrame) -> crate::history_stub::StubLocator {
    crate::history_stub::StubLocator {
        database_id: frame.database_id,
        generation: frame.extent_generation,
        placement: frame.placement,
    }
}
