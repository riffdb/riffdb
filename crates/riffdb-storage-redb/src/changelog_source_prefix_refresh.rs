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
        let started = std::time::Instant::now();
        let Some(lease) = shared.mutation_gate.try_acquire()? else {
            return Ok((None, 0));
        };
        if !super::transaction::drained_for_validation(shared)? {
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
            return Ok((None, elapsed_us(started, std::time::Instant::now())));
        }
        drop(meta);
        drop(read);
        let mut scrub = crate::startup::begin_background_scrub_on_shared(shared, inputs)?;
        scrub.set_cancellation(cancellation);
        scrub.bind_prefix_refresh(publication);
        let held_us = elapsed_us(started, std::time::Instant::now());
        drop(lease);
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
