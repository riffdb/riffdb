//! Complete retained-history evidence for one continuously owned restore engine.
//! No serialization, replay permission, public selector or durable completion.

use crate::{
    changelog_v3_roots,
    error::storage_error,
    maintenance::{NativeRestoreValidatedGeneration, RestoreIntegrityBinding},
};
use redb::{ReadTransaction, WriteTransaction};
use riffdb_storage_api::{ChangelogHistoryStateV3, StorageError, StorageErrorKind};

/// Constructor-owned evidence: a root decode alone cannot construct this value.
/// Retains native custody, but neither a read transaction nor an engine Arc.
pub(crate) struct CompleteRestoreHistory {
    generation: NativeRestoreValidatedGeneration,
    history: ChangelogHistoryStateV3,
}

impl CompleteRestoreHistory {
    #[cfg(test)]
    pub(crate) fn capture(
        read: &ReadTransaction,
        binding: &RestoreIntegrityBinding,
    ) -> Result<Option<Self>, StorageError> {
        Self::capture_with_resolver(read, binding, None)
    }

    pub(crate) fn capture_with_resolver(
        read: &ReadTransaction,
        binding: &RestoreIntegrityBinding,
        resolver: Option<&crate::history_stub::HistoryResolver>,
    ) -> Result<Option<Self>, StorageError> {
        let generation = binding.validated_generation()?;
        // Includes the entire retained chain, source holds, registration links,
        // primary admission and cutover. Do not replace with bounded roots.
        let history = match resolver {
            Some(resolver) => crate::history_stub::validate_owned_history(read, binding, resolver)?,
            None => changelog_v3_roots::validate_retained_history(read)?,
        };
        let Some(history) = history else {
            return Ok(None);
        };
        generation.verify(binding)?;
        let proof = Self {
            generation,
            history,
        };
        proof.verify_read(read, binding)?;
        Ok(Some(proof))
    }

    pub(crate) fn verify_read(
        &self,
        read: &ReadTransaction,
        binding: &RestoreIntegrityBinding,
    ) -> Result<(), StorageError> {
        self.generation.verify(binding)?;
        if changelog_v3_roots::read_checkpoint_roots(read)? != Some(self.history) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        self.generation.verify(binding)
    }

    pub(crate) fn history_for_stamp(
        &self,
        transaction: &WriteTransaction,
        binding: &RestoreIntegrityBinding,
    ) -> Result<ChangelogHistoryStateV3, StorageError> {
        self.generation.verify(binding)?;
        if changelog_v3_roots::read_checkpoint_roots_for_write(transaction)? != Some(self.history) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        self.generation.verify(binding)?;
        Ok(self.history)
    }
}
