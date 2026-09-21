//! Same-process materialization of already validated journal checkpoint batches.
//! Receipt integration must retain this existing single durable transaction and
//! its exact retained mutation proofs; it must not reconstruct from latest rows.

use redb::Durability;
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
        let mut transaction = self.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(false);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let v3 = crate::changelog_v3_journal::has_write_recovery_roots(&transaction)?;
        let mut last_sequence = batch.checkpoint_sequence;
        let mut last_administration_sequence = batch.checkpoint_administration_sequence;
        let mut last_hash = batch.checkpoint_hash;
        let mut transition_count = 0_usize;
        let mut command_count = 0_usize;
        let mut audit_count = 0_usize;
        let mut encoded_bytes = 0_usize;
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
            let advance = if v3 {
                let history =
                    crate::changelog_v3_roots::read_checkpoint_roots_for_write(&transaction)?
                        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
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
                let source = if frame.command_count == 0 {
                    ChangelogAttributionV3::JournaledServiceAudit
                } else {
                    ChangelogAttributionV3::JournaledApplicationGroup
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
                Some(PreparedHistoryAdvance::prepare(&transaction, &receipt)?)
            } else {
                if frame.changelog_binding.is_some() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                None
            };
            for mutation in frame.mutations.iter() {
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
                crate::journal::apply_validated_composite_mutation(&transaction, mutation)
                    .map_err(journal_io_error)?;
            }
            if let Some(advance) = advance {
                advance.stage(&transaction)?;
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
        #[cfg(test)]
        crate::changelog_v3_journal::recovery_crash_edge("checkpoint-staged");
        self.commit_durable(transaction)?;
        #[cfg(test)]
        crate::changelog_v3_journal::recovery_crash_edge("checkpoint-committed");
        Ok(())
    }
}
