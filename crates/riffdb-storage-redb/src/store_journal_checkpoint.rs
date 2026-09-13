//! Same-process materialization of already validated journal checkpoint batches.
//! Receipt integration must retain this existing single durable transaction and
//! its exact retained mutation proofs; it must not reconstruct from latest rows.

use redb::Durability;
use riffdb_storage_api::{StorageError, StorageErrorKind};

use super::{JournalCheckpointBatch, SharedRedb, journal_io_error};
use crate::error::{storage_error, transaction_error};

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
            for mutation in frame.mutations.iter() {
                crate::journal::apply_validated_composite_mutation(&transaction, mutation)
                    .map_err(journal_io_error)?;
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
        self.commit_durable(transaction)
    }
}
