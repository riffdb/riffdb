//! ADR-0182 section 6: online retention steps through the sole ordered writer.
//!
//! Each step is one administration-class transition on the existing
//! attributed write lane, so it holds the shared mutation gate exactly as long
//! as its one bounded transaction runs: no exclusive lease, no second writer,
//! and no journal rebase (the process has already recovered its journal). A
//! step deletes whole command rows only, ending on a row boundary, so it never
//! rewrites a straddled segment or its successors, and it reads every fence
//! from a snapshot taken while it holds the gate.

use std::num::NonZeroU64;
use std::sync::Arc;

use riffdb_storage_api::StoredCommandSegmentV1;

use super::*;
use crate::store::{RedbOperationalPorts, WriteBeginTiming, elapsed_us};
use crate::transient::TransientIndexDelta;

#[path = "online/prefix_refresh.rs"]
mod prefix_refresh;

/// A busy database's lane steps only while at most this many journaled
/// transitions await materialization, which is shortly after each background
/// checkpoint, so a step never drains a large suffix under the writer gate.
const ONLINE_RETENTION_SMALL_SUFFIX_TRANSITIONS: usize = 256;

/// ADR-0182 section 7: an online step covers at most this many sequences.
pub const ONLINE_RETENTION_STEP_SEQUENCES: u64 = 256;

/// Commit-row bytes one step may remove, so its captured receipt stays well
/// inside the changelog frame ceiling even for unusually large commands.
pub(super) const ONLINE_RETENTION_STEP_ROW_BYTES: usize = 8 * 1024 * 1024;

/// Why an online step made no progress. Closed and payload-free.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnlineRetentionHold {
    /// The watermark already meets the configured retained-sequence target.
    TargetReached,
    /// A fencing input stops advancement below the target.
    Fenced(RetentionFenceBinding),
    /// The retained-sequence target falls inside the next whole command row,
    /// or the budget was spent before it.
    RowBoundary,
    /// An asynchronous journal checkpoint is in flight. The step yields to it
    /// rather than wait for it while holding the writer gate; retry shortly.
    CheckpointInFlight,
    /// A busy database's journal suffix is large. The lane waits for the next
    /// background checkpoint to materialize it rather than drain it under the
    /// writer gate; retry shortly.
    AwaitingCheckpoint,
}

/// Microsecond phase durations of one online retention step (WP-811
/// evidence). A phase the step did not reach stays zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OnlineRetentionPhases {
    /// Planning from an engine snapshot, without the writer gate.
    pub preplan_us: u64,
    /// Off-gate physical reclamation time, including bounded footer validation.
    pub reclaim_us: u64,
    /// Whole unreferenced files deleted in this pass.
    pub deleted_extents: u64,
    /// Whole unreferenced files placed in the one-file recycling pool.
    pub recycled_extents: u64,
    /// Passes held by durable command or receipt references.
    pub extent_reference_holds: u64,
    /// Passes held by older snapshots or a background scrub.
    pub extent_reader_holds: u64,
    /// Retained source receipts actually removed in this pass (at most 256).
    pub reclaimed_receipts: u64,
    /// Under the gate: drained-root source-receipt reclamation.
    pub receipt_gate_us: u64,
    /// Another bounded off-gate receipt page remains before exact end.
    pub receipt_validation_pending: bool,
    /// Receipt/audit rows streamed during the complete initial source pass.
    pub source_initial_rows: u64,
    /// Receipt/audit rows streamed after closed publication qualification.
    pub source_continuation_rows: u64,
    /// Existing source candidates/certificates invalidated by native publication.
    pub source_validation_invalidations: u64,
    /// Bounded source admissions refused without deleting any receipt.
    pub source_admission_refusals: u64,
    /// FIFO admission expired, was retired or lacked its fixed request slot.
    pub source_admission_busy_writer: u64,
    /// Admission found unknown, unpublished or excessive journal work.
    pub source_admission_pending_journal: u64,
    /// A checkpoint or its extent sealing was still publishing.
    pub source_admission_checkpoint: u64,
    /// The candidate no longer qualified for the captured source state.
    pub source_admission_qualification: u64,
    /// Catch-up exceeded one existing receipt/audit validation page.
    pub source_admission_page_bound: u64,
    /// One source-control attempt, including attempts without a new floor plan.
    pub source_reclamation_attempts: u64,
    /// Actual barrier plan: resume, tail, prior checkpoint, derived fence,
    /// durable fence, uncapped ceiling, live durable holds, presence bits.
    /// Bit zero marks a derived fence; bit one marks a durable fence. This
    /// plans eligibility only and never proves deletion or file retirement.
    pub source_reclamation_sample: Option<[u64; 8]>,
    /// Attempt index associated with the last actual floor-plan measurement.
    pub source_reclamation_sample_attempt: u64,
    /// First observation, unusable prior progress, or a held floor, respectively.
    pub source_reclamation_yields: [u64; 3],
    /// Queued, granted, expired, cancelled, capacity refusals, wait microseconds,
    /// actual source lease microseconds and bounded-drain yields, respectively.
    /// Per-step diagnostics, never source authority.
    pub source_admission_census: [u64; 8],
    /// Under the gate: validated-prefix pin capture and publication.
    pub prefix_gate_us: u64,
    /// Waiting for the first-come writer gate.
    pub gate_wait_us: u64,
    /// Under the gate: maintenance setup and any direct-path journal drain.
    pub drain_us: u64,
    /// Under the gate: reading the fences and planning the sub-range.
    pub plan_us: u64,
    /// Under the gate: hashing the tombstone preimage.
    pub digest_us: u64,
    /// Under the gate: writing retained views and removing locators.
    pub materialize_us: u64,
    /// Under the gate: deleting rows and writing the tombstone and watermark.
    pub delete_us: u64,
    /// Under the gate: the durable commit.
    pub commit_us: u64,
}

impl OnlineRetentionPhases {
    /// Microseconds this step held the writer gate.
    #[must_use]
    pub const fn gate_held_us(&self) -> u64 {
        self.drain_us
            .saturating_add(self.receipt_gate_us)
            .saturating_add(self.prefix_gate_us)
            .saturating_add(self.plan_us)
            .saturating_add(self.digest_us)
            .saturating_add(self.materialize_us)
            .saturating_add(self.delete_us)
            .saturating_add(self.commit_us)
    }
}

/// The durable result of one online retention step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnlineRetentionStep {
    /// One sub-range was pruned and the watermark advanced to `through`.
    Advanced {
        /// Inclusive new watermark.
        through: u64,
        /// Sequences this step removed (at most 256).
        sequences: u64,
    },
    /// Nothing was pruned; the reason is closed.
    Held(OnlineRetentionHold),
}

/// A planned sub-range and what it removes.
struct StepPlan {
    first: u64,
    last: u64,
    pruned_segments: Vec<Arc<StoredCommandSegmentV1>>,
}

/// The least-authority handle a background retention lane holds: it runs
/// online retention steps and nothing else. Each step queues on the same
/// first-come mutation gate as every other writer, so the lane is never a
/// second writer and a writer that queued behind a step is served before the
/// lane's next one.
#[derive(Clone)]
pub struct RedbOnlineRetentionLane {
    shared: Arc<crate::store::SharedRedb>,
    /// The journal's last application sequence at the previous step, so the
    /// lane materializes the journal suffix only once the database is idle.
    last_journal_sequence: Arc<std::sync::atomic::AtomicU64>,
    source_control: Arc<std::sync::Mutex<crate::store::ReplicationSourceControl>>,
    logical_bytes: Arc<std::sync::Mutex<super::eligible_bytes::Observer>>,
    physical_bytes:
        Arc<std::sync::Mutex<crate::store::extent_reclamation::physical_bytes::Observer>>,
}

impl RedbOnlineRetentionLane {
    #[cfg(test)]
    pub(crate) fn physical_observations_for_test(&self) -> u64 {
        self.physical_bytes.lock().unwrap().completed_for_test
    }

    /// First-party worker retirement cancels pending source requests under the
    /// gate lock, leaving ordinary writers and granted atomic turns intact.
    #[doc(hidden)]
    pub fn retire_source_admission(&self) {
        self.shared.retire_online_source_admission();
    }

    /// Releases the retired owner's nondurable validation and byte-observation pins after
    /// its worker has joined. No durable hold or history is changed.
    #[doc(hidden)]
    pub fn release_source_validation(&self) {
        self.source_control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retire_online_validation();
        self.physical_bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retire();
        self.logical_bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retire();
    }

    /// Continues receipt/audit and logical-byte pages while waiting for a checkpoint,
    /// and ticks the bounded physical-byte observer outside the writer gate.
    /// Existing capture, page, qualification and exact-end checks still apply.
    #[doc(hidden)]
    pub fn advance_source_validation_off_gate(
        &self,
    ) -> Result<OnlineRetentionPhases, StorageError> {
        self.physical_bytes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .tick(&self.shared);
        let mut logical = self
            .logical_bytes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Err(error) = logical.advance(
            &RedbOperationalPorts {
                shared: Arc::clone(&self.shared),
            },
            std::time::Instant::now(),
        ) {
            logical.report_refusal(&error);
        }
        drop(logical);
        let mut source = self
            .source_control
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let [initial, continued, invalidated] = source.advance_validation_off_gate()?;
        Ok(OnlineRetentionPhases {
            source_initial_rows: initial,
            source_continuation_rows: continued,
            source_validation_invalidations: invalidated,
            receipt_validation_pending: source.receipt_validation_pending(),
            ..OnlineRetentionPhases::default()
        })
    }
    /// Runs one step, as [`RedbOperationalPorts::online_retention_step`] does,
    /// except that history still in the journal suffix is materialized only
    /// when no transition was journaled since this lane's previous step: a busy
    /// database's own checkpoints materialize it instead.
    pub fn step(
        &self,
        retain_sequences: NonZeroU64,
        max_sequences: NonZeroU64,
    ) -> Result<OnlineRetentionStep, StorageError> {
        self.step_timed(retain_sequences, max_sequences, false)
            .map(|(step, _)| step)
    }

    /// How many journal checkpoints this process has finished.
    #[must_use]
    pub fn journal_checkpoints_finished(&self) -> u64 {
        self.shared.journal_checkpoints_finished()
    }

    /// Waits up to `timeout` for a journal checkpoint to finish after `seen`
    /// (see [`Self::journal_checkpoints_finished`]), returning the current
    /// count. A lane held for `AwaitingCheckpoint` or `CheckpointInFlight`
    /// steps right after one, while the journal suffix is nearly empty.
    pub fn wait_for_journal_checkpoint(&self, seen: u64, timeout: std::time::Duration) -> u64 {
        self.shared.wait_for_journal_checkpoint(seen, timeout)
    }

    /// [`Self::step`] together with the step's phase durations. A lane woken
    /// by a finished journal checkpoint passes `after_checkpoint`: that is the
    /// smallest journal suffix it will see, so the step proceeds whatever the
    /// suffix size, and the lane's gate-held pacing charges any drain.
    pub fn step_timed(
        &self,
        retain_sequences: NonZeroU64,
        max_sequences: NonZeroU64,
        after_checkpoint: bool,
    ) -> Result<(OnlineRetentionStep, OnlineRetentionPhases), StorageError> {
        self.physical_bytes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .tick(&self.shared);
        let mut logical = self
            .logical_bytes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Err(error) = logical.tick(
            &RedbOperationalPorts {
                shared: Arc::clone(&self.shared),
            },
            retain_sequences,
        ) {
            logical.report_refusal(&error);
        }
        drop(logical);
        let mut phases = OnlineRetentionPhases::default();
        let reclaiming = std::time::Instant::now();
        let reclamation = self.shared.reclaim_one_extent()?;
        phases.reclaim_us = elapsed_us(reclaiming, std::time::Instant::now());
        phases.deleted_extents = reclamation.deleted;
        phases.recycled_extents = reclamation.recycled;
        phases.extent_reference_holds = reclamation.held_references;
        phases.extent_reader_holds = reclamation.held_readers;
        let mut source_control = self
            .source_control
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let (removed, held_us) = source_control.reclaim_history_online()?;
        phases.receipt_validation_pending = source_control.receipt_validation_pending();
        [
            phases.source_initial_rows,
            phases.source_continuation_rows,
            phases.source_validation_invalidations,
            phases.source_admission_refusals,
        ] = source_control.validation_phases();
        [
            phases.source_admission_busy_writer,
            phases.source_admission_pending_journal,
            phases.source_admission_checkpoint,
            phases.source_admission_qualification,
            phases.source_admission_page_bound,
        ] = source_control.admission_yields();
        phases.source_reclamation_attempts = 1;
        phases.source_reclamation_sample = source_control.reclamation_observation();
        phases.source_reclamation_yields = source_control.reclamation_yields();
        phases.source_admission_census = source_control.source_admission_census();
        drop(source_control);
        phases.reclaimed_receipts = removed;
        phases.receipt_gate_us = held_us;
        let (journal_sequence, suffix_transitions) = self.shared.journal_suffix_activity()?;
        let idle = self
            .last_journal_sequence
            .swap(journal_sequence, std::sync::atomic::Ordering::AcqRel)
            == journal_sequence;
        if !self.shared.uses_journaled_retention()
            && !idle
            && !after_checkpoint
            && suffix_transitions > ONLINE_RETENTION_SMALL_SUFFIX_TRANSITIONS
        {
            return Ok((
                OnlineRetentionStep::Held(OnlineRetentionHold::AwaitingCheckpoint),
                phases,
            ));
        }
        let step = RedbOperationalPorts {
            shared: Arc::clone(&self.shared),
        }
        .online_retention_step_inner(retain_sequences, max_sequences, idle, &mut phases)?;
        Ok((step, phases))
    }
}

impl std::fmt::Debug for RedbOnlineRetentionLane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RedbOnlineRetentionLane([STORAGE])")
    }
}

impl RedbOperationalPorts {
    /// The retention lane's step-only handle over this store.
    #[must_use]
    pub fn online_retention_lane(&self) -> RedbOnlineRetentionLane {
        RedbOnlineRetentionLane {
            shared: Arc::clone(&self.shared),
            last_journal_sequence: Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX)),
            source_control: Arc::new(std::sync::Mutex::new(self.replication_source_control())),
            physical_bytes: Arc::new(std::sync::Mutex::new(
                crate::store::extent_reclamation::physical_bytes::Observer::default(),
            )),
            logical_bytes: Arc::new(std::sync::Mutex::new(
                super::eligible_bytes::Observer::default(),
            )),
        }
    }

    /// Runs one bounded online retention step toward retaining
    /// `retain_sequences` sequences below the durable application head.
    ///
    /// At most `max_sequences` (capped at 256) sequences are removed, except
    /// that a single whole command segment larger than `max_sequences` is
    /// removed alone (never more than 256 sequences), in one
    /// administration-class transaction that deletes whole command rows,
    /// appends the chained tombstone, advances the watermark, and deletes any
    /// validated-prefix checkpoint. Every fence (projection frontiers in the
    /// primary and the derived sidecar, operator holds, consumer low water,
    /// staged migration frontier, registered follower acknowledgements, and
    /// delivery of every outbox event in the sub-range) is read while this
    /// step holds the ordered writer gate. An unreadable, unvalidated, or
    /// incarnation-mismatched source refuses with an error.
    pub fn online_retention_step(
        &self,
        retain_sequences: NonZeroU64,
        max_sequences: NonZeroU64,
    ) -> Result<OnlineRetentionStep, StorageError> {
        self.online_retention_step_inner(
            retain_sequences,
            max_sequences,
            true,
            &mut OnlineRetentionPhases::default(),
        )
    }

    fn online_retention_step_inner(
        &self,
        retain_sequences: NonZeroU64,
        max_sequences: NonZeroU64,
        may_materialize_suffix: bool,
        phases: &mut OnlineRetentionPhases,
    ) -> Result<OnlineRetentionStep, StorageError> {
        let allowance = max_sequences.get().min(ONLINE_RETENTION_STEP_SEQUENCES);
        if self.shared.uses_journaled_retention() {
            return self.journaled_retention_step(retain_sequences, allowance, phases);
        }
        let preplan = std::time::Instant::now();
        // Plan first without the writer gate, from an engine snapshot: most
        // checks find nothing to prune, and they must not drain the journal
        // suffix or delay a writer to learn that.
        let hold = {
            let read = self
                .shared
                .database
                .begin_read()
                .map_err(transaction_error)?;
            let derived_read = self.begin_derived_read()?;
            plan_step(
                &self.shared,
                &read,
                &derived_read,
                retain_sequences,
                allowance,
            )?
            .err()
        };
        phases.preplan_us = elapsed_us(preplan, std::time::Instant::now());
        let materialize_suffix = match hold {
            None => false,
            Some(hold) => {
                // Held only because the snapshot's head is stale (history in
                // the journal suffix): materialize it under the gate and plan
                // again, but only when the caller allows it.
                let head_bound = matches!(
                    hold,
                    OnlineRetentionHold::TargetReached
                        | OnlineRetentionHold::RowBoundary
                        | OnlineRetentionHold::Fenced(
                            RetentionFenceBinding::DurableApplicationHead
                        )
                );
                if !(may_materialize_suffix
                    && head_bound
                    && self.shared.journal_suffix_activity()?.1 > 0)
                {
                    return Ok(OnlineRetentionStep::Held(hold));
                }
                true
            }
        };
        let mut begin = WriteBeginTiming::default();
        let opened = self.begin_attributed_write_unless_checkpointing(
            riffdb_storage_api::ChangelogAttributionV3::RetentionPrune,
            materialize_suffix,
            &mut begin,
        );
        phases.gate_wait_us = begin.gate_wait_us;
        phases.drain_us = begin.drain_us;
        let Some(access) = opened? else {
            return Ok(OnlineRetentionStep::Held(
                OnlineRetentionHold::CheckpointInFlight,
            ));
        };
        self.online_retention_step_with(access, retain_sequences, allowance, phases)
    }

    fn journaled_retention_step(
        &self,
        retain: NonZeroU64,
        allowance: u64,
        phases: &mut OnlineRetentionPhases,
    ) -> Result<OnlineRetentionStep, StorageError> {
        let preparing = std::time::Instant::now();
        let prepared = match super::prepared::PreparedPrune::prepare(self, retain, allowance)? {
            Ok(prepared) => prepared,
            Err(hold) => {
                phases.preplan_us = elapsed_us(preparing, std::time::Instant::now());
                return Ok(OnlineRetentionStep::Held(hold));
            }
        };
        let waiting = std::time::Instant::now();
        phases.preplan_us = elapsed_us(preparing, waiting);
        let mut begin = WriteBeginTiming::default();
        let access = self.begin_deferred_maintenance_write(&mut begin)?;
        let validating = std::time::Instant::now();
        phases.gate_wait_us = begin.gate_wait_us;
        phases.drain_us = begin.drain_us;
        let read = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let derived = self.begin_derived_read()?;
        if load_history_incarnation(&read)? != prepared.incarnation {
            phases.plan_us = elapsed_us(validating, std::time::Instant::now());
            access.abort()?;
            return Ok(OnlineRetentionStep::Held(OnlineRetentionHold::RowBoundary));
        }
        let holds = load_holds(&read)?;
        let mut fences = collect_online_fencing_inputs(&read, &derived, &holds)?;
        let head = access
            .read_command_value(
                crate::journal::JournalTable::Meta,
                META_APPLICATION_SEQUENCE.as_bytes(),
            )?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        fences.durable_application_head = Some(
            match codec::decode_application_sequence_allocator_v1(&head)?
                .into_parts()
                .0
            {
                riffdb_storage_api::ApplicationSequenceAllocator::Next(next) => next.get() - 1,
                riffdb_storage_api::ApplicationSequenceAllocator::Exhausted => u64::MAX,
            },
        );
        let (limit, binding) = compute_max_permissible_watermark(&fences);
        if limit.unwrap_or(0) < prepared.last
            || fences
                .durable_application_head
                .unwrap_or(0)
                .saturating_sub(retain.get())
                < prepared.last
        {
            phases.plan_us = elapsed_us(validating, std::time::Instant::now());
            access.abort()?;
            return Ok(OnlineRetentionStep::Held(OnlineRetentionHold::Fenced(
                binding,
            )));
        }
        if !prepared.validate_and_stage(&access)? {
            phases.plan_us = elapsed_us(validating, std::time::Instant::now());
            access.abort()?;
            return Ok(OnlineRetentionStep::Held(OnlineRetentionHold::RowBoundary));
        }
        drop(derived);
        drop(read);
        let submitting = std::time::Instant::now();
        phases.plan_us = elapsed_us(validating, submitting);
        let through = CommitSequence::new(prepared.last)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let fence = access.submit_retention_prune(through, prepared.segments)?;
        let released = std::time::Instant::now();
        phases.commit_us = elapsed_us(submitting, released);
        // Submission released the sole gate. The existing ordered durability
        // fence publishes the watermark before its composite deletion view.
        riffdb_storage_api::DeferredServiceAuditFence::wait(Box::new(fence))?;
        Ok(OnlineRetentionStep::Advanced {
            through: prepared.last,
            sequences: prepared.last - prepared.first + 1,
        })
    }

    fn online_retention_step_with(
        &self,
        access: crate::store::RedbWriteAccess,
        retain_sequences: NonZeroU64,
        allowance: u64,
        phases: &mut OnlineRetentionPhases,
    ) -> Result<OnlineRetentionStep, StorageError> {
        let planning = std::time::Instant::now();
        // Opened while the gate is held: every fence row is a direct
        // administration transition that commits under the same gate, so this
        // snapshot is the fence state this step commits against. It excludes
        // only the journal suffix this write drained, whose application
        // commands lie above the head it reports and touch no fencing input.
        let read = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        // A sidecar that was never created holds no projection frontier; the
        // step reads it as empty rather than creating it, as offline does.
        let derived_read = match self.begin_derived_read() {
            Ok(read) => read,
            Err(error) => {
                let _ = access.abort();
                return Err(error);
            }
        };

        let plan = match plan_step(
            &self.shared,
            &read,
            &derived_read,
            retain_sequences,
            allowance,
        ) {
            Ok(Ok(plan)) => plan,
            Ok(Err(hold)) => {
                drop(derived_read);
                drop(read);
                access.abort()?;
                return Ok(OnlineRetentionStep::Held(hold));
            }
            Err(error) => {
                let _ = access.abort();
                return Err(error);
            }
        };
        let chain = match chain_link(&read, plan.first) {
            Ok(chain) => chain,
            Err(error) => {
                let _ = access.abort();
                return Err(error);
            }
        };
        let checkpoint_present = match read.open_table(META).map_err(table_error).and_then(|meta| {
            meta.get(META_VALIDATED_PREFIX_CHECKPOINT)
                .map(|value| value.is_some())
                .map_err(precommit_storage_error)
        }) {
            Ok(present) => present,
            Err(error) => {
                let _ = access.abort();
                return Err(error);
            }
        };
        drop(derived_read);
        drop(read);
        let digesting = std::time::Instant::now();
        phases.plan_us = elapsed_us(planning, digesting);

        let write = access.transaction()?;
        let mut materializing = digesting;
        let mut deleting = digesting;
        let result = (|| {
            let (content_digest, counts) = digest_and_count_range(write, plan.first, plan.last)?;
            materializing = std::time::Instant::now();
            let range_size = plan
                .last
                .checked_sub(plan.first)
                .and_then(|span| span.checked_add(1))
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            // Every sequence below the head committed, so exactly one command
            // per sequence must be present; a shortfall is corruption.
            if counts.commits != range_size {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            if checkpoint_present {
                let mut meta = write.open_table(META).map_err(table_error)?;
                let _ = meta
                    .remove(META_VALIDATED_PREFIX_CHECKPOINT)
                    .map_err(precommit_storage_error)?;
            }
            materialize_retained_command_views(write, plan.first, plan.last)?;
            deleting = std::time::Instant::now();
            delete_whole_command_rows(write, plan.first, plan.last)?;
            delete_event_keyed_range(write, EVENTS, plan.first, plan.last)?;
            delete_event_keyed_range(write, OUTBOX, plan.first, plan.last)?;
            delete_event_keyed_range(write, OUTBOX_STATUS, plan.first, plan.last)?;
            let tombstone = StoredHistoryTombstoneV1::new(
                plan.first,
                plan.last,
                counts.commits,
                counts.events,
                counts.outbox,
                counts.outbox_status,
                content_digest,
                Some(chain.previous),
                chain.incarnation,
            )
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            {
                let mut tombstones = write.open_table(HISTORY_TOMBSTONES).map_err(table_error)?;
                let key = encode_application_sequence_key(
                    CommitSequence::new(plan.first)
                        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
                );
                let encoded =
                    encode_history_tombstone_v1(&tombstone).map_err(crate::error::codec_error)?;
                tombstones
                    .insert(key.as_slice(), encoded.as_bytes())
                    .map_err(precommit_storage_error)?;
            }
            {
                let mut meta = write.open_table(META).map_err(table_error)?;
                let watermark = StoredRetentionWatermarkV1::new(
                    plan.last,
                    chain.incarnation,
                    Some(chain.root_digest),
                )
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                let encoded =
                    encode_retention_watermark_v1(&watermark).map_err(crate::error::codec_error)?;
                meta.insert(META_RETENTION_WATERMARK, encoded.as_bytes())
                    .map_err(precommit_storage_error)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            let _ = access.abort();
            return Err(error);
        }
        let committing = std::time::Instant::now();
        phases.digest_us = elapsed_us(digesting, materializing);
        phases.materialize_us = elapsed_us(materializing, deleting);
        phases.delete_us = elapsed_us(deleting, committing);
        let through = CommitSequence::new(plan.last)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        // Publish the new watermark to live readers BEFORE the deletion can
        // become visible: a reader that captures a post-commit snapshot then
        // loads this value, so an absent pruned row always reads as
        // HistoryPruned, never as absence. One step runs at a time (it holds
        // the gate), so restoring on failure is exact.
        let previous = self.shared.publish_retention_watermark(plan.last);
        if let Err(error) = access.commit_for_with_delta(
            RedbTestOperation::OnlineRetentionStep,
            Some(TransientIndexDelta::HistoryPruned {
                through,
                segments: plan.pruned_segments,
            }),
        ) {
            self.shared.restore_retention_watermark(previous);
            return Err(error);
        }
        phases.commit_us = elapsed_us(committing, std::time::Instant::now());
        Ok(OnlineRetentionStep::Advanced {
            through: plan.last,
            sequences: plan.last - plan.first + 1,
        })
    }
}

/// The chain state a step extends: the verified tail link, the tombstone
/// chain's recorded root, and the current history incarnation.
struct ChainLink {
    previous: HistoryTombstoneHash,
    root_digest: SchemaHash,
    incarnation: u64,
}

fn chain_link(read: &redb::ReadTransaction, first: u64) -> Result<ChainLink, StorageError> {
    let incarnation = load_history_incarnation(read)?;
    let watermark = load_watermark(read)?;
    let (current, recorded_root) = match watermark.as_ref() {
        None => (0, None),
        Some(watermark) => {
            // A watermark from another history incarnation is not this
            // history's retention state; refuse rather than extend it.
            if watermark.history_incarnation() != incarnation {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            (
                watermark.watermark_sequence(),
                watermark.chain_root_registry_digest(),
            )
        }
    };
    if current.checked_add(1) != Some(first) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let root_digest = recorded_root.unwrap_or_else(current_record_registry_digest);
    let tombstones = read.open_table(HISTORY_TOMBSTONES).map_err(table_error)?;
    let previous = match tombstones.last().map_err(precommit_storage_error)? {
        None if current == 0 => history_tombstone_chain_root(root_digest),
        None => return Err(storage_error(StorageErrorKind::CorruptData)),
        Some((_, value)) => {
            let tombstone = decode_history_tombstone_v1(value.value())
                .map_err(crate::error::codec_error)?
                .into_parts()
                .0;
            // The tail must end exactly at the watermark, in this incarnation.
            if tombstone.last_sequence() != current
                || tombstone.history_incarnation() != incarnation
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            tombstone.tombstone_hash()
        }
    };
    Ok(ChainLink {
        previous,
        root_digest,
        incarnation,
    })
}

/// Plans one step: the fenced limit, then the largest whole-row prefix of the
/// remaining history inside it whose outbox events are all delivered.
fn plan_step(
    shared: &crate::store::SharedRedb,
    read: &redb::ReadTransaction,
    derived: &redb::ReadTransaction,
    retain_sequences: NonZeroU64,
    allowance: u64,
) -> Result<Result<StepPlan, OnlineRetentionHold>, StorageError> {
    let current = load_watermark(read)?
        .map(|watermark| watermark.watermark_sequence())
        .unwrap_or(0);
    let holds = load_holds(read)?;
    let fencing = collect_online_fencing_inputs(read, derived, &holds)?;
    let head = fencing.durable_application_head.unwrap_or(0);
    let target = head.saturating_sub(retain_sequences.get());
    if current >= target {
        return Ok(Err(OnlineRetentionHold::TargetReached));
    }
    let (max_permissible, binding) = compute_max_permissible_watermark(&fencing);
    let max_permissible = max_permissible.unwrap_or(0);
    if max_permissible <= current {
        return Ok(Err(OnlineRetentionHold::Fenced(binding)));
    }
    // Fences and the target bound every step; the budget binds too, except
    // that one whole segment larger than the budget is removed alone (still
    // within the 256-sequence hard bound), so a small budget cannot wedge
    // the lane behind a large group commit.
    let limit = target.min(max_permissible);
    let budget_end = current.saturating_add(allowance);
    let first = current + 1;

    let commits = read.open_table(COMMITS).map_err(table_error)?;
    let intents = read.open_table(OUTBOX).map_err(table_error)?;
    let statuses = read.open_table(OUTBOX_STATUS).map_err(table_error)?;
    let first_key = encode_application_sequence_key(
        CommitSequence::new(first)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
    );
    // The watermark is a row boundary: no row that starts below `first` may
    // still hold a command at or above it.
    if let Some(entry) = commits
        .range::<&[u8]>(..first_key.as_slice())
        .map_err(precommit_storage_error)?
        .next_back()
    {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = decode_application_sequence_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if row_last(value.value(), physical)? >= first {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }

    let mut last = current;
    let mut bytes = 0_usize;
    let mut pruned_segments = Vec::new();
    // A row that crosses the limit leaves the step at a row boundary unless
    // a fence, not the target, is what the row crosses.
    let crossed_limit = if max_permissible < target {
        OnlineRetentionHold::Fenced(binding)
    } else {
        OnlineRetentionHold::RowBoundary
    };
    let mut stopped = crossed_limit;
    for entry in commits
        .range::<&[u8]>(first_key.as_slice()..)
        .map_err(precommit_storage_error)?
    {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = decode_application_sequence_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if physical.get() != last + 1 {
            // Every sequence below the head committed: a gap is corruption.
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        if !shared.segment_extent_is_sealed(value.value())? {
            stopped = OnlineRetentionHold::RowBoundary;
            break;
        }
        let resolved = crate::history_stub::resolve_commits_value(value.value())?;
        let segment = decode_segment_row(&resolved)?;
        let end = match &segment {
            Some(segment) => segment.last_commit_sequence().get(),
            None => physical.get(),
        };
        if end > limit {
            stopped = crossed_limit;
            break;
        }
        if end > budget_end && (last > current || end - current > ONLINE_RETENTION_STEP_SEQUENCES) {
            // The budget is spent, or a single row exceeds the hard bound.
            stopped = OnlineRetentionHold::RowBoundary;
            break;
        }
        bytes = bytes.saturating_add(resolved.len());
        if bytes > ONLINE_RETENTION_STEP_ROW_BYTES && last > current {
            break;
        }
        let delivered = match &segment {
            Some(segment) => segment
                .commands()
                .iter()
                .flat_map(|command| command.events())
                .map(|event| event_delivered(&statuses, event.event_id()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .all(|delivered| delivered),
            None => true,
        } && intents_delivered(&intents, &statuses, physical.get(), end)?;
        if !delivered {
            stopped = OnlineRetentionHold::Fenced(RetentionFenceBinding::UndeliveredOutboxLowWater);
            break;
        }
        last = end;
        if let Some(segment) = segment {
            pruned_segments.push(Arc::new(segment));
        }
        if last >= limit.min(budget_end) {
            break;
        }
    }
    if last == current {
        return Ok(Err(stopped));
    }
    Ok(Ok(StepPlan {
        first,
        last,
        pruned_segments,
    }))
}

/// The fencing set without the whole-history undelivered-outbox scan: an
/// online step instead proves delivery of every event inside its own
/// sub-range (`plan_step`), which bounds its read cost to that sub-range.
pub(super) fn collect_online_fencing_inputs(
    read: &redb::ReadTransaction,
    derived: &redb::ReadTransaction,
    holds: &StoredRetentionHoldsV1,
) -> Result<RetentionFencingInputs, StorageError> {
    collect_fencing_inputs_with(read, Some(derived), holds, OutboxFence::StepLocal)
}

pub(super) fn decode_segment_row(
    row: &[u8],
) -> Result<Option<StoredCommandSegmentV1>, StorageError> {
    match crate::history_stub::decode_segment_value(row) {
        Ok(segment) => Ok(Some(segment.into_parts().0)),
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
        {
            Ok(None)
        }
        Err(error) => Err(crate::error::codec_error(error)),
    }
}

pub(super) fn row_last(row: &[u8], physical: CommitSequence) -> Result<u64, StorageError> {
    Ok(decode_segment_row(row)?.map_or(physical.get(), |segment| {
        segment.last_commit_sequence().get()
    }))
}

fn event_delivered(
    statuses: &impl ReadableTable<&'static [u8], &'static [u8]>,
    event_id: riffdb_types::EventId,
) -> Result<bool, StorageError> {
    let key = event_id.to_be_bytes();
    Ok(
        match statuses
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
        {
            None => false,
            Some(value) => {
                let status: StoredOutboxStatusV1 = codec::decode_outbox_status_v1(value.value())
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                    .into_parts()
                    .0;
                matches!(
                    status.state(),
                    riffdb_storage_api::OutboxDeliveryStateV1::Delivered { .. }
                )
            }
        },
    )
}

fn intents_delivered(
    intents: &impl ReadableTable<&'static [u8], &'static [u8]>,
    statuses: &impl ReadableTable<&'static [u8], &'static [u8]>,
    first: u64,
    last: u64,
) -> Result<bool, StorageError> {
    let (lower, upper) = event_keyed_range_bounds(first, last);
    for entry in intents
        .range::<&[u8]>(lower.as_slice()..=upper.as_slice())
        .map_err(precommit_storage_error)?
    {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        let event_id = decode_event_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if !event_delivered(statuses, event_id)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Deletes every whole command row inside `[first, last]`. The plan ended the
/// range on a row boundary, so no segment is rewritten.
fn delete_whole_command_rows(
    write: &WriteTransaction,
    first: u64,
    last: u64,
) -> Result<(), StorageError> {
    let mut table = write.open_table(COMMITS).map_err(table_error)?;
    let lower = encode_application_sequence_key(
        CommitSequence::new(first)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
    );
    let upper = encode_application_sequence_key(
        CommitSequence::new(last)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
    );
    let mut keys = Vec::new();
    for entry in table
        .range::<&[u8]>(lower.as_slice()..=upper.as_slice())
        .map_err(precommit_storage_error)?
    {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = decode_application_sequence_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if row_last(value.value(), physical)? > last {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        keys.push(key.value().to_vec());
    }
    for key in keys {
        let _ = table
            .remove(key.as_slice())
            .map_err(precommit_storage_error)?;
    }
    Ok(())
}
