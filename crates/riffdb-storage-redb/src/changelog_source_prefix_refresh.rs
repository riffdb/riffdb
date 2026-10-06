//! Sealed publication owner for complete online validation. No public controls.

use super::*;
use crate::startup::ValidatedPrefixRefreshState;

pub(crate) struct PendingPrefixRefresh {
    proof: ValidatedPrefixRefreshState,
    history: History,
    receipt: Option<AuthoritativeTransactionV3>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrefixRefreshPublication {
    Published,
    Unchanged,
    Yielded,
    Stale,
    Cancelled,
}

impl PendingPrefixRefresh {
    pub(crate) fn begin_if_needed(
        shared: &Arc<SharedRedb>,
        inputs: riffdb_storage_api::StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<(Option<crate::startup::RedbBackgroundScrubSession>, u64), StorageError> {
        Self::begin_if_needed_impl(
            shared,
            inputs,
            cancellation,
            crate::startup::begin_background_scrub_on_shared,
        )
    }

    #[cfg(test)]
    pub(crate) fn begin_if_needed_with_open(
        shared: &Arc<SharedRedb>,
        inputs: riffdb_storage_api::StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
        open: impl FnOnce(
            &Arc<SharedRedb>,
            riffdb_storage_api::StartupValidationInputs,
        ) -> Result<crate::startup::RedbBackgroundScrubSession, StorageError>,
    ) -> Result<(Option<crate::startup::RedbBackgroundScrubSession>, u64), StorageError> {
        Self::begin_if_needed_impl(shared, inputs, cancellation, open)
    }

    fn begin_if_needed_impl(
        shared: &Arc<SharedRedb>,
        inputs: riffdb_storage_api::StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
        open: impl FnOnce(
            &Arc<SharedRedb>,
            riffdb_storage_api::StartupValidationInputs,
        ) -> Result<crate::startup::RedbBackgroundScrubSession, StorageError>,
    ) -> Result<(Option<crate::startup::RedbBackgroundScrubSession>, u64), StorageError> {
        let started = std::time::Instant::now();
        let Some(lease) = shared.mutation_gate.try_acquire()? else {
            return Ok((None, 0));
        };
        if !super::transaction::drained_for_validation(shared)? {
            drop(lease);
            return Ok((None, elapsed_us(started, std::time::Instant::now())));
        }
        let read = shared.capture_checkpoint_root()?;
        let publication = read.identity();
        let meta = read.open_table(META).map_err(table_error)?;
        if meta
            .get(META_VALIDATED_PREFIX_CHECKPOINT)
            .map_err(precommit_storage_error)?
            .is_some()
        {
            drop(meta);
            drop(read);
            drop(lease);
            return Ok((None, elapsed_us(started, std::time::Instant::now())));
        }
        drop(meta);
        drop(read);
        // Session construction scans the audit stream and retained tombstone
        // chain. It belongs outside the ordered writer just like the later
        // page walks. Keep only the original publication binding: any writer
        // that runs before or during setup makes this candidate stale, never
        // a validation or publication permit for a different snapshot.
        drop(lease);
        let held_us = elapsed_us(started, std::time::Instant::now());
        let mut scrub = open(shared, inputs)?;
        scrub.set_cancellation(cancellation);
        scrub.bind_prefix_refresh(publication);
        Ok((Some(scrub), held_us))
    }

    /// Building and all entity-head planning remain outside the writer gate.
    pub(crate) fn prepare(proof: ValidatedPrefixRefreshState) -> Result<Self, StorageError> {
        if proof.cancelled() || !proof.publication_current() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let history = crate::changelog_v3_roots::read_checkpoint_roots(proof.read())?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
        let mut rows = 0;
        let checkpoint = crate::validated_prefix::build_checkpoint_from_snapshot(
            proof.read(),
            proof.retained(),
            crate::validated_prefix::CheckpointCountSource::DurableLengths {
                execution_failed_rows: proof.execution_failed_rows(),
            },
            &mut rows,
        )?;
        let receipt = crate::validated_prefix::plan_checkpoint_receipt(proof.read(), &checkpoint)?;
        Ok(Self {
            proof,
            history,
            receipt,
        })
    }

    /// Publication is a closed existing Immediate owner. It never drains,
    /// repairs, manufactures a completion, or waits for another writer.
    pub(crate) fn publish(self) -> Result<(PrefixRefreshPublication, u64), StorageError> {
        if self.proof.cancelled() {
            return Ok((PrefixRefreshPublication::Cancelled, 0));
        }
        if !self.proof.publication_current() {
            return Ok((PrefixRefreshPublication::Stale, 0));
        }
        let shared = self.proof.shared();
        let start = std::time::Instant::now();
        let Some(_lease) = shared.mutation_gate.try_acquire()? else {
            return Ok((PrefixRefreshPublication::Yielded, 0));
        };
        if shared.write_fenced.load(Ordering::Acquire) || shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        if !super::transaction::drained_for_validation(shared)? {
            return Ok((
                PrefixRefreshPublication::Yielded,
                elapsed_us(start, std::time::Instant::now()),
            ));
        }
        let current = shared.capture_checkpoint_root()?;
        if !self.proof.publication_current()
            || crate::changelog_v3_roots::read_checkpoint_roots(&current)? != Some(self.history)
        {
            return Ok((
                PrefixRefreshPublication::Stale,
                elapsed_us(start, std::time::Instant::now()),
            ));
        }
        let meta = current.open_table(META).map_err(table_error)?;
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
        drop(meta);
        if self.proof.cancelled() {
            return Ok((
                PrefixRefreshPublication::Cancelled,
                elapsed_us(start, std::time::Instant::now()),
            ));
        }
        shared.clear_composite_publication()?;
        let disposition = if let Some(receipt) = self.receipt {
            let prepared = crate::changelog_v3_write::PreparedImmediateReceipt::apply(
                &shared.database,
                crate::store::RedbCommitProfile::Hardened,
                &receipt,
            )?;
            shared.before_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)?;
            prepared.commit(shared)?;
            shared.after_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)?;
            shared.refresh_durable_read_frontier()?;
            let successor = self.history.advance(&receipt).map_err(value_error)?;
            shared.refresh_drained_journal_source(self.history, successor)?;
            PrefixRefreshPublication::Published
        } else {
            PrefixRefreshPublication::Unchanged
        };
        Ok((disposition, elapsed_us(start, std::time::Instant::now())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_storage_api::{
        ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
        StartupValidationInputs,
    };
    use riffdb_types::{DigestKeyId, Timestamp};

    fn inputs() -> StartupValidationInputs {
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
        StartupValidationInputs::new(
            Timestamp::new(0, 0).unwrap(),
            ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
            ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
        )
    }

    // req: STO-030, STO-031, END-008
    #[test]
    fn prefix_refresh_session_setup_releases_the_ordered_writer() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let mut opened = false;
        let (scrub, _) = crate::store::PendingPrefixRefresh::begin_if_needed_with_open(
            &ports.shared,
            inputs(),
            Arc::new(AtomicBool::new(false)),
            |shared, inputs| {
                let writer = shared
                    .mutation_gate
                    .try_acquire()
                    .unwrap()
                    .expect("background session setup must not retain the ordered writer");
                drop(writer);
                opened = true;
                crate::startup::begin_background_scrub_on_shared(shared, inputs)
            },
        )
        .unwrap();
        assert!(opened);
        assert!(scrub.is_some());
    }

    // req: STO-030, STO-032, REC-001, END-008
    #[test]
    fn prefix_refresh_publication_during_session_setup_discards_the_candidate() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let (scrub, _) = crate::store::PendingPrefixRefresh::begin_if_needed_with_open(
            &ports.shared,
            inputs(),
            Arc::new(AtomicBool::new(false)),
            |shared, inputs| {
                let writer = shared
                    .mutation_gate
                    .try_acquire()
                    .unwrap()
                    .expect("ordinary publication must progress during background setup");
                shared
                    .commit_durable(shared.database.begin_write().unwrap())
                    .unwrap();
                drop(writer);
                crate::startup::begin_background_scrub_on_shared(shared, inputs)
            },
        )
        .unwrap();
        let mut scrub = scrub.unwrap();
        let epoch = ports.shared.durable_commit_epoch();
        let stale = scrub.prefix_refresh_staleness().unwrap();
        assert_eq!(
            scrub.validate_retained_history().unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
        assert!(stale.load(Ordering::Acquire));
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    }
}
