//! Actual physical root publication and private Source usage custody.
#![cfg_attr(not(feature = "projection-source-driver"), allow(dead_code))]
use super::*;

#[cfg(not(test))]
static PROCESS_DURABLE_ROOT_PUBLICATION_FLOOR: AtomicU64 = AtomicU64::new(0);

enum SourceCommitCustody<'guard, 'engine> {
    SourceAbsent,
    Unaccounted(crate::source_usage_domain::SourceUsagePublication<'engine>),
    Prepared(crate::source_usage_domain::SourceUsagePhysicalCommitBorrow<'guard, 'engine>),
}
impl SourceCommitCustody<'_, '_> {
    fn require_for_shared(&self, shared: &SharedRedb) -> Result<(), StorageError> {
        match self {
            Self::SourceAbsent => Ok(()),
            Self::Unaccounted(publication) => publication.require_for_shared(shared),
            Self::Prepared(borrowed) => borrowed.require_for_shared(shared),
        }
    }
    #[cfg(feature = "test-fixtures")]
    fn source_changed(&self) -> bool {
        match self {
            Self::SourceAbsent => false,
            Self::Unaccounted(publication) => {
                // Retain the owned publication through the entire physical turn.
                let _ = publication;
                true
            }
            Self::Prepared(borrowed) => {
                let _ = borrowed;
                true
            }
        }
    }
}

impl SharedRedb {
    pub(crate) fn commit_with_source_footprint_and_locator(
        &self,
        transaction: WriteTransaction,
        footprint: crate::source_continuation::SourceContinuationFootprint,
        capability: crate::locator_capability::LocatorCapability,
    ) -> Result<(), StorageError> {
        // Closed legacy/raw producer inventory: these writers do not mutate
        // Source44..47/114. Fresh retains its separately prepared exact guard.
        // Complete typed receipts/follower Source mutations use the distinct
        // method below; unknown continuation is not Source absence evidence.
        self.commit_with_source_usage_footprint_and_locator(
            transaction,
            footprint,
            crate::source_usage_footprint::SourceUsageFootprint::fixed_source_absent(),
            capability,
        )
    }

    pub(crate) fn commit_with_source_usage_footprint_and_locator(
        &self,
        transaction: WriteTransaction,
        footprint: crate::source_continuation::SourceContinuationFootprint,
        source_usage: crate::source_usage_footprint::SourceUsageFootprint,
        capability: crate::locator_capability::LocatorCapability,
    ) -> Result<(), StorageError> {
        self.bind_source_commit_locator(&transaction, &capability)?;
        // Ordinary complete receipts retain the existing unaccounted fence.
        // The actual prepared producer uses its distinct typed seam below.
        let custody = if source_usage.changed() {
            SourceCommitCustody::Unaccounted(
                self.database
                    .source_usage_domain()
                    .begin_unaccounted_publication()?,
            )
        } else {
            SourceCommitCustody::SourceAbsent
        };
        self.commit_with_source_custody_and_locator(transaction, footprint, capability, custody)
    }

    pub(crate) fn commit_with_prepared_source_usage_and_locator(
        &self,
        transaction: WriteTransaction,
        footprint: crate::source_continuation::SourceContinuationFootprint,
        capability: crate::locator_capability::LocatorCapability,
        prepared: crate::source_usage_domain::SourceUsagePhysicalCommitBorrow<'_, '_>,
    ) -> Result<(), StorageError> {
        prepared.require_for_shared(self)?;
        self.bind_source_commit_locator(&transaction, &capability)?;
        // The underlying prepared mutation remains owned by the native producer
        // through finish_publication, composite refresh and exact read-back.
        self.commit_with_source_custody_and_locator(
            transaction,
            footprint,
            capability,
            SourceCommitCustody::Prepared(prepared),
        )
    }

    fn bind_source_commit_locator(
        &self,
        transaction: &WriteTransaction,
        capability: &crate::locator_capability::LocatorCapability,
    ) -> Result<(), StorageError> {
        // These fixed metadata roots bind the admitted directory to the staged
        // database incarnation and registered durable schema. Exact membership
        // is established by the captured predecessor plus its complete delta.
        if let Ok(generation) = capability.prepared_generation() {
            let metadata = transaction.open_table(META).map_err(table_error)?;
            let database = metadata
                .get(META_DATABASE_ID)
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let incarnation = metadata
                .get(META_HISTORY_INCARNATION)
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let registry = metadata
                .get(META_RECORD_REGISTRY)
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let database = *decode_database_identity_v1(database.value())
                .map_err(crate::error::codec_error)?
                .value();
            let incarnation = *decode_history_incarnation_v1(incarnation.value())
                .map_err(crate::error::codec_error)?
                .value();
            let registry = *decode_record_registry_digest(registry.value())
                .map_err(crate::error::codec_error)?
                .value();
            generation.validate_database_incarnation(database, incarnation)?;
            capability.bind_source(database, incarnation, registry)?;
        }
        Ok(())
    }

    fn commit_with_source_custody_and_locator(
        &self,
        transaction: WriteTransaction,
        footprint: crate::source_continuation::SourceContinuationFootprint,
        capability: crate::locator_capability::LocatorCapability,
        custody: SourceCommitCustody<'_, '_>,
    ) -> Result<(), StorageError> {
        custody.require_for_shared(self)?;
        #[cfg(feature = "test-fixtures")]
        if custody.source_changed()
            && let Some(controller) = &self.test_controller
        {
            controller.checkpoint_test_pause(
                crate::hooks::RedbCheckpointTestPoint::BeforeSourceUsagePublication,
            );
        }
        // This binding preserves every owned guard/borrow through all exits.
        let _source_commit_custody = custody;
        if !footprint.is_append_only() {
            self.invalidate_source_continuation();
        }
        if self.durable_commit_epoch.load(Ordering::Acquire) == u64::MAX {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::SequenceExhausted));
        }
        let publication = self.durable_root_publication.load(Ordering::Acquire);
        if !publication.is_multiple_of(2) {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let base_publication = {
            #[cfg(not(test))]
            {
                publication.max(PROCESS_DURABLE_ROOT_PUBLICATION_FLOOR.load(Ordering::Acquire))
            }
            #[cfg(test)]
            {
                publication
            }
        };
        let Some(stable_successor) = base_publication.checked_add(2) else {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::SequenceExhausted));
        };
        let in_progress = stable_successor - 1;
        if self
            .durable_root_publication
            .compare_exchange(
                publication,
                in_progress,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let committed = transaction.commit();
        #[cfg(test)]
        if committed.is_ok()
            && let Some(controller) = &self.test_controller
        {
            controller.wait_after_engine_commit_before_root_publication();
        }
        // This slot is installed while engine publication is odd: no reader
        // may capture the new physical root with an older locator directory.
        let locator_publication = self.durable_locator_capability.write();
        let locator_poisoned = match locator_publication {
            Ok(mut slot) => {
                *slot = Some((stable_successor, capability));
                false
            }
            Err(poisoned) => {
                *poisoned.into_inner() = None;
                self.fence_writes();
                true
            }
        };
        let publication_wait = self.durable_root_publication_wait.lock();
        #[cfg(not(test))]
        PROCESS_DURABLE_ROOT_PUBLICATION_FLOOR.fetch_max(stable_successor, Ordering::AcqRel);
        self.durable_root_publication
            .store(stable_successor, Ordering::Release);
        self.durable_root_publication_changed.notify_all();
        let publication_wait_poisoned = match publication_wait {
            Ok(guard) => {
                drop(guard);
                false
            }
            Err(poisoned) => {
                drop(poisoned.into_inner());
                self.fence_writes();
                true
            }
        };
        if let Err(error) = committed {
            self.invalidate_source_continuation();
            self.fence_writes();
            return Err(commit_error(error));
        }
        if publication_wait_poisoned || locator_poisoned {
            return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
        }
        if self
            .durable_commit_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .is_err()
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::SequenceExhausted));
        }
        Ok(())
    }
}

// Structural native fixture writes use the same exact retirement-aware Source
// grant. The gate itself stays private; no production admission API is widened.
#[cfg(test)]
impl SharedRedb {
    pub(crate) fn acquire_source_usage_fixture_lease(
        &self,
    ) -> Result<ExclusiveLease, StorageError> {
        if !self.is_source_mode() || self.write_fenced.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        self.mutation_gate
            .acquire_source()?
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))
    }
}
