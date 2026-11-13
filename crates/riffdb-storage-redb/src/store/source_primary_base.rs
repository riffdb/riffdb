//! Clone-only selection of an already existing actual primary read base.
//! The new Current-registration path needs its own pre-begin native plan.
use super::*;
impl SharedRedb {
    #[cfg(test)]
    pub(crate) fn source_publication_epoch_for_test(&self) -> u64 {
        self.durable_root_publication.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub(crate) fn source_live_tracked_root_count_for_test(&self) -> Result<usize, StorageError> {
        let roots = self
            .extent_roots
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        Ok(roots.iter().filter(|root| root.strong_count() != 0).count())
    }

    #[cfg(test)]
    pub(crate) fn source_current_required_journal_cache_for_test(
        &self,
    ) -> Result<Option<(u64, u64, bool, bool)>, StorageError> {
        let current = self
            .current_read_root
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        Ok(current.as_ref().map(|(epoch, root)| {
            let (commits, events) = root.source_required_journal_cache_bits_for_test();
            (*epoch, root.identity(), commits, events)
        }))
    }

    // FIRST Source capture never adopts a shared cached Current as baseline.
    // Only an actual already-published Composite/Durable owner may be cloned.
    pub(super) fn try_clone_published_source_base(
        &self,
    ) -> Result<Option<RedbReadAccess>, StorageError> {
        let composite = self
            .composite_publication
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Some(publication) = composite.as_ref() {
            return Ok(Some(RedbReadAccess::Composite(publication.capture()?)));
        }
        drop(composite);
        let frontier = self
            .durable_read_frontier
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        Ok(frontier
            .as_ref()
            .map(|root| RedbReadAccess::Durable(root.clone())))
    }

    pub(super) fn try_clone_existing_source_base(
        &self,
    ) -> Result<Option<RedbReadAccess>, StorageError> {
        let composite = self
            .composite_publication
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Some(publication) = composite.as_ref() {
            return Ok(Some(RedbReadAccess::Composite(publication.capture()?)));
        }
        drop(composite);
        // Match the ordinary selector's frontier/root lock order, while never
        // falling through to begin_read or publishing a source-owned root.
        let frontier = self
            .durable_read_frontier
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Some(root) = frontier.as_ref() {
            return Ok(Some(RedbReadAccess::Durable(root.clone())));
        }
        let epoch = self.stable_durable_root_version()?;
        let current = self
            .current_read_root
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        Ok(current
            .as_ref()
            .filter(|(captured, _)| *captured == epoch)
            .map(|(_, root)| RedbReadAccess::Current(root.clone())))
    }
}

// Owning test observations only; no transaction, root, mutation or warmup escape.
#[cfg(test)]
impl SharedRedb {
    pub(crate) fn source_read_publications_for_test(
        &self,
    ) -> Result<(bool, bool, bool), StorageError> {
        let composite = self
            .composite_publication
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .is_some();
        let durable = self
            .durable_read_frontier
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .is_some();
        let current = self
            .current_read_root
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .is_some();
        Ok((composite, durable, current))
    }
}
