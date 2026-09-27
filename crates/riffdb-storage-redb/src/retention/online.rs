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
use crate::store::RedbOperationalPorts;
use crate::transient::TransientIndexDelta;

/// ADR-0182 section 7: an online step covers at most this many sequences.
pub const ONLINE_RETENTION_STEP_SEQUENCES: u64 = 256;

/// Commit-row bytes one step may remove, so its captured receipt stays well
/// inside the changelog frame ceiling even for unusually large commands.
const ONLINE_RETENTION_STEP_ROW_BYTES: usize = 8 * 1024 * 1024;

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
}

impl RedbOnlineRetentionLane {
    /// Runs one step; see [`RedbOperationalPorts::online_retention_step`].
    pub fn step(
        &self,
        retain_sequences: NonZeroU64,
        max_sequences: NonZeroU64,
    ) -> Result<OnlineRetentionStep, StorageError> {
        RedbOperationalPorts {
            shared: Arc::clone(&self.shared),
        }
        .online_retention_step(retain_sequences, max_sequences)
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
        let allowance = max_sequences.get().min(ONLINE_RETENTION_STEP_SEQUENCES);
        let access = self
            .begin_attributed_write(riffdb_storage_api::ChangelogAttributionV3::RetentionPrune)?;
        // Opened while the gate is held: every fence row is an administration
        // transition that commits under the same gate, so this snapshot is the
        // fence state this step commits against. It excludes only the
        // not-yet-applied journal suffix, whose commands lie above the head it
        // reports and touch no fencing input.
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

        let plan = match plan_step(&read, &derived_read, retain_sequences, allowance) {
            Ok(Ok(plan)) => plan,
            Ok(Err(hold)) => {
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

        let write = access.transaction()?;
        let result = (|| {
            let (content_digest, counts) = digest_and_count_range(write, plan.first, plan.last)?;
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
        let segment = decode_segment_row(value.value())?;
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
        bytes = bytes.saturating_add(value.value().len());
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
fn collect_online_fencing_inputs(
    read: &redb::ReadTransaction,
    derived: &redb::ReadTransaction,
    holds: &StoredRetentionHoldsV1,
) -> Result<RetentionFencingInputs, StorageError> {
    collect_fencing_inputs_with(read, Some(derived), holds, OutboxFence::StepLocal)
}

fn decode_segment_row(row: &[u8]) -> Result<Option<StoredCommandSegmentV1>, StorageError> {
    match riffdb_storage_api::decode_command_segment_v1(row) {
        Ok(segment) => Ok(Some(segment.into_parts().0)),
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
        {
            Ok(None)
        }
        Err(error) => Err(crate::error::codec_error(error)),
    }
}

fn row_last(row: &[u8], physical: CommitSequence) -> Result<u64, StorageError> {
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
