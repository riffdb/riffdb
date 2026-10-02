//! Bounded retained-history validation over the scrub's original immutable pin.

use super::*;
use crate::changelog_v3_retained_validation::RetainedHistoryValidation;
use crate::checkpoint_root::CheckpointRoot;

/// Closed proof of all three exact-end passes over one original immutable pin.
/// No constructor, callback, or publication control crosses the public boundary.
pub(crate) struct ValidatedPrefixRefreshState {
    shared: Arc<crate::store::SharedRedb>,
    read: ReadTransaction,
    retained: RetainedMetadataV1,
    execution_failed_rows: u64,
    cancellation: Option<Arc<AtomicBool>>,
    // Last: keep files protected until the read transaction has been dropped.
    _extent_pin: crate::store::extent_reclamation::ScrubPin,
}

impl ValidatedPrefixRefreshState {
    pub(crate) fn shared(&self) -> &Arc<crate::store::SharedRedb> {
        &self.shared
    }
    pub(crate) fn read(&self) -> &ReadTransaction {
        &self.read
    }
    pub(crate) fn retained(&self) -> &RetainedMetadataV1 {
        &self.retained
    }
    pub(crate) fn execution_failed_rows(&self) -> u64 {
        self.execution_failed_rows
    }
    pub(crate) fn cancelled(&self) -> bool {
        self.cancellation
            .as_ref()
            .is_some_and(|stop| stop.load(Ordering::Acquire))
    }
}

impl RedbBackgroundScrubSession {
    /// Validates the complete retained V3 history and retained-fence evidence
    /// over the original pin. It yields between bounded pages and observes
    /// cancellation before every page. Exact end is required before the scrub
    /// can finish; failure preserves ADR-0260's source-authority refusal.
    pub fn validate_retained_history(&mut self) -> Result<(), StorageError> {
        self.validate_retained_history_pages(
            EvidencePageLimit::new(64).ok_or_else(invariant)?,
            || {
                std::thread::yield_now();
                true
            },
        )
    }

    pub(crate) fn validate_retained_history_pages(
        &mut self,
        limit: EvidencePageLimit,
        mut proceed: impl FnMut() -> bool,
    ) -> Result<(), StorageError> {
        self.retained_history_validated = false;
        self.session.check_cancellation()?;
        let transaction = self.session.validation_read.take().ok_or_else(invariant)?;
        // The scrub's existing extent pin protects this original read for the
        // complete pass. The local identity is never a publication permit.
        let root = Arc::new(CheckpointRoot::new(transaction, 0));
        if let Some(mut walk) = RetainedHistoryValidation::new(Arc::clone(&root))? {
            loop {
                self.session.check_cancellation()?;
                if !proceed() {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
                if walk.advance(limit)? {
                    drop(walk.complete()?);
                    break;
                }
            }
        } else if !proceed() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let root = Arc::try_unwrap(root).map_err(|_| invariant())?;
        self.session.validation_read = Some(root.into_transaction());
        self.retained_history_validated = true;
        Ok(())
    }

    pub(crate) fn finish_for_prefix_refresh(
        mut self,
        structural_end: RedbStructuralEvidenceEnd,
        historical_end: RedbHistoricalEvidenceEnd,
    ) -> Result<ValidatedPrefixRefreshState, StorageError> {
        if !self.retained_history_validated || !self.catalog_history_validated {
            return Err(invariant());
        }
        self.session
            .verify_background_completion(&structural_end, &historical_end)?;
        self.session.historical_tables = None;
        let read = self.session.validation_read.take().ok_or_else(invariant)?;
        Ok(ValidatedPrefixRefreshState {
            shared: Arc::clone(&self.session.shared),
            read,
            retained: self.session.retained_metadata,
            execution_failed_rows: self.session.terminal_execution_failure_rows,
            cancellation: self.session.cancellation,
            _extent_pin: self._extent_pin,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use riffdb_storage_api::DatabaseInitializationPort;

    fn initialized() -> (crate::test_path::ScopedDirectory, crate::RedbStore) {
        let scope = crate::test_path::ScopedDirectory::new("online-prefix-proof");
        let mut store = crate::RedbStore::open(scope.join("db.redb")).unwrap();
        store
            .initialize_database(
                riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [0x61; 10]).unwrap(),
            )
            .unwrap();
        (scope, store)
    }

    fn validated_candidate(
        store: &crate::RedbStore,
        cancellation: Option<Arc<AtomicBool>>,
    ) -> crate::store::PendingPrefixRefresh {
        let mut scrub = begin_background_scrub_on_shared(&store.shared, inputs()).unwrap();
        if let Some(stop) = cancellation {
            scrub.set_cancellation(stop);
        }
        scrub.validate_retained_history().unwrap();
        let limit = EvidencePageLimit::new(64).unwrap();
        let structural = loop {
            match scrub.read_structural_page(limit).unwrap() {
                BackgroundScrubPage::Page { findings, .. } => assert!(findings.is_empty()),
                BackgroundScrubPage::ExactEnd(end) => break end,
            }
        };
        let historical = scrub.validate_catalog_history(limit, || true).unwrap();
        let proof = scrub
            .finish_for_prefix_refresh(structural, historical)
            .unwrap();
        crate::store::PendingPrefixRefresh::prepare(proof).unwrap()
    }

    // req: STO-032, REC-001, REC-005
    #[test]
    fn complete_scrub_creates_a_bound_prefix_candidate_and_publishes_once() {
        let (_scope, store) = initialized();
        let candidate = validated_candidate(&store, None);
        let before = store.shared.durable_commit_epoch();
        let (result, _) = candidate.publish().unwrap();
        assert_eq!(result, crate::store::PrefixRefreshPublication::Published);
        assert_eq!(store.shared.durable_commit_epoch(), before + 1);
        let read = store.shared.database.begin_read().unwrap();
        assert!(
            read.open_table(crate::layout::META)
                .unwrap()
                .get(crate::layout::META_VALIDATED_PREFIX_CHECKPOINT)
                .unwrap()
                .is_some()
        );
    }

    // req: STO-032, REC-001, REC-005
    #[test]
    fn maintenance_refresh_requires_complete_validation_and_does_not_repeat_publication() {
        let (_scope, store) = initialized();
        let ports = crate::RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let lane = ports.online_retention_lane();
        let before = store.shared.durable_commit_epoch();
        lane.refresh_validated_prefix(inputs(), Arc::new(AtomicBool::new(false)))
            .unwrap();
        assert_eq!(store.shared.durable_commit_epoch(), before + 1);
        lane.refresh_validated_prefix(inputs(), Arc::new(AtomicBool::new(false)))
            .unwrap();
        assert_eq!(store.shared.durable_commit_epoch(), before + 1);

        let (_scope, store) = initialized();
        let ports = crate::RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let lane = ports.online_retention_lane();
        let before = store.shared.durable_commit_epoch();
        let error = lane
            .refresh_validated_prefix(inputs(), Arc::new(AtomicBool::new(true)))
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        assert_eq!(store.shared.durable_commit_epoch(), before);
    }

    // req: STO-032, REC-001, REC-005
    #[test]
    fn stale_prefix_candidate_cannot_replace_newer_evidence_or_advance_an_allocator() {
        let (_scope, store) = initialized();
        let stale = validated_candidate(&store, None);
        validated_candidate(&store, None).publish().unwrap();
        let before = store.shared.durable_commit_epoch();
        let history = crate::changelog_v3_roots::read_checkpoint_roots(
            &store.shared.database.begin_read().unwrap(),
        )
        .unwrap();
        assert_eq!(
            stale.publish().unwrap().0,
            crate::store::PrefixRefreshPublication::Stale
        );
        assert_eq!(store.shared.durable_commit_epoch(), before);
        assert_eq!(
            crate::changelog_v3_roots::read_checkpoint_roots(
                &store.shared.database.begin_read().unwrap(),
            )
            .unwrap(),
            history
        );
    }

    // req: STO-032, REC-001, REC-005
    #[test]
    fn completed_prefix_candidate_still_observes_cancellation_and_writer_admission() {
        let (_scope, store) = initialized();
        let stop = Arc::new(AtomicBool::new(false));
        let cancelled = validated_candidate(&store, Some(Arc::clone(&stop)));
        let yielded = validated_candidate(&store, None);
        let before = store.shared.durable_commit_epoch();
        stop.store(true, Ordering::Release);
        assert_eq!(
            cancelled.publish().unwrap().0,
            crate::store::PrefixRefreshPublication::Cancelled
        );
        let lease = store.acquire_mutation_lease().unwrap();
        assert_eq!(
            yielded.publish().unwrap().0,
            crate::store::PrefixRefreshPublication::Yielded
        );
        drop(lease);
        assert_eq!(store.shared.durable_commit_epoch(), before);
    }

    fn inputs() -> StartupValidationInputs {
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
        StartupValidationInputs::new(
            Timestamp::new(0, 0).unwrap(),
            ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
            ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
        )
    }

    // req: STO-032, REC-001, REC-005
    #[test]
    fn an_unfinished_scrub_cannot_create_prefix_refresh_authority() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let scrub = begin_background_scrub_on_shared(&ports.shared, inputs()).unwrap();
        let structural_end = RedbStructuralEvidenceEnd {
            cursor: scrub.session.next_structural,
        };
        let historical_end = RedbHistoricalEvidenceEnd {
            cursor: scrub.session.next_historical,
        };
        let epoch = ports.shared.durable_commit_epoch();
        let error = scrub
            .finish_for_prefix_refresh(structural_end, historical_end)
            .err()
            .unwrap();
        assert_eq!(error.kind(), StorageErrorKind::InvariantViolation);
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    }

    // req: STO-032, REC-001, REC-005
    #[test]
    fn cancelled_history_pages_never_complete_scrub_or_mutate_source_state() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let epoch = ports.shared.durable_commit_epoch();
        let mut scrub = begin_background_scrub_on_shared(&ports.shared, inputs()).unwrap();
        let mut pages = 0;
        let error = scrub
            .validate_retained_history_pages(EvidencePageLimit::new(1).unwrap(), || {
                pages += 1;
                pages <= 1
            })
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        assert_eq!(pages, 2);
        assert!(!scrub.retained_history_validated);
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    }

    // req: STO-032, REC-001, REC-005
    #[test]
    fn complete_history_pages_keep_the_original_scrub_pin_and_prove_exact_end() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let epoch = ports.shared.durable_commit_epoch();
        let mut scrub = begin_background_scrub_on_shared(&ports.shared, inputs()).unwrap();
        let before = crate::changelog_v3_roots::read_checkpoint_roots(
            scrub.session.validation_read.as_ref().unwrap(),
        )
        .unwrap();
        let mut pages = 0;
        scrub
            .validate_retained_history_pages(EvidencePageLimit::new(1).unwrap(), || {
                pages += 1;
                true
            })
            .unwrap();
        assert!(pages >= 8);
        assert!(scrub.retained_history_validated);
        assert_eq!(
            crate::changelog_v3_roots::read_checkpoint_roots(
                scrub.session.validation_read.as_ref().unwrap()
            )
            .unwrap(),
            before
        );
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    }
}
