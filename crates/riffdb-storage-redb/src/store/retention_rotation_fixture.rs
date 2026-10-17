//! Test-only fixture methods remain inside the real store privacy boundary.
use super::*;
impl RedbOperationalPorts {
    pub(crate) fn materialize_and_capture_retention_fixture(
        &self,
    ) -> Result<crate::source_validation_pin::ValidationSource, StorageError> {
        let gate = self.shared.mutation_gate.acquire()?;
        self.shared
            .checkpoint_published_journal_suffix_for_barrier()?;
        let source = self.shared.capture_source_validation_pin()?;
        drop(gate);
        Ok(source)
    }
}
#[cfg(test)]
impl RedbOperationalPorts {
    pub(crate) fn checkpoint_retention_relocation_fixture_for_test(
        &self,
    ) -> Result<(), StorageError> {
        let gate = self.shared.mutation_gate.acquire()?;
        self.shared.publish_pending_journal_prefix()?;
        // Direct materialization only advances the active anchor. Exercise
        // the real asynchronous rotation/installation/sealing protocol instead.
        self.shared.start_async_checkpoint_for_direct_write()?;
        let completion = self
            .shared
            .journal_checkpoint
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .as_ref()
            .and_then(|checkpoint| checkpoint.completion.clone())
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        drop(gate);
        // Retain only its existing completion capability while the actual
        // materializer runs; neither writer gate nor state mutex is held.
        completion
            .poll(true)
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))??;
        let gate = self.shared.mutation_gate.acquire()?;
        self.shared.poll_async_checkpoint_locked(false)?;
        drop(gate);
        self.shared.join_journal_sealer();
        Ok(())
    }
}
