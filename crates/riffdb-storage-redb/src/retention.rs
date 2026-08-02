//! Offline retention watermark, fencing, prune, and tombstone verification (ADR-0085 A2).

use std::collections::BTreeSet;
use std::ops::Bound::Included;
use std::path::{Path, PathBuf};

use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
    WriteTransaction,
};
use riffdb_storage_api::{
    HISTORY_INCARNATION_INITIAL, HistoryTombstoneContentDigest, HistoryTombstoneHash,
    RetentionFenceBinding, RetentionFencingInputs, RetentionHoldV1, StorageError, StorageErrorKind,
    StoredHistoryTombstoneV1, StoredOutboxStatusV1, StoredRetentionHoldsV1,
    StoredRetentionWatermarkV1, compute_max_permissible_watermark, history_tombstone_chain_root,
    proto_codec::{
        current_record_registry_digest, decode_history_tombstone_v1, decode_projection_control_v1,
        decode_retention_holds_v1, decode_retention_watermark_v1, encode_history_tombstone_v1,
        encode_retention_holds_v1, encode_retention_watermark_v1,
    },
};
use riffdb_types::{CommitSequence, FrontierPosition, HashDomain, SchemaHash, hash};

use crate::codec;
use crate::error::{
    database_error, precommit_storage_error, storage_error, table_error, transaction_error,
};
use crate::hooks::{RedbTestController, RedbTestOperation};
use crate::keys::{
    decode_application_sequence_key, decode_event_key, encode_application_sequence_key,
};
use crate::layout::{
    COMMITS, CONTRACT_MIGRATION_JOURNAL, EVENTS, HISTORY_TOMBSTONES, META,
    META_HISTORY_INCARNATION, META_RETENTION_HOLDS, META_RETENTION_WATERMARK,
    META_VALIDATED_PREFIX_CHECKPOINT, OUTBOX, OUTBOX_STATUS, PROJECTION_FRONTIER,
};

/// Inclusive commit sequences pruned per offline sub-range transaction.
pub const RETENTION_PRUNE_SUBRANGE_SEQUENCES: u64 = 1_024;

/// Prefix for hold_ids that mark a projection identity as detached from fencing.
pub const PROJECTION_DETACH_HOLD_PREFIX: &str = "detach/";

/// Status snapshot for offline retention inspection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionStatusV1 {
    /// Current watermark sequence (0 = unpruned).
    pub watermark_sequence: u64,
    /// History incarnation bound on the watermark (or live incarnation when absent).
    pub history_incarnation: u64,
    /// Max permissible inclusive watermark under current fencing, if computable.
    pub max_permissible_watermark: Option<u64>,
    /// Which fencing input binds the max, when any.
    pub fence_binding: RetentionFenceBinding,
    /// Operator holds currently stored.
    pub holds: StoredRetentionHoldsV1,
    /// Number of history tombstone rows.
    pub tombstone_count: u64,
}

/// Offline exclusive retention maintenance over a closed database file.
pub struct RedbOfflineRetention {
    database_path: PathBuf,
    test_controller: Option<RedbTestController>,
}

impl RedbOfflineRetention {
    /// Binds one offline database file for exclusive retention maintenance.
    pub fn bind(database_path: impl AsRef<Path>) -> Self {
        Self {
            database_path: database_path.as_ref().to_path_buf(),
            test_controller: None,
        }
    }

    /// Binds with a process-test failpoint controller.
    #[doc(hidden)]
    pub fn bind_with_test_controller(
        database_path: impl AsRef<Path>,
        controller: RedbTestController,
    ) -> Self {
        Self {
            database_path: database_path.as_ref().to_path_buf(),
            test_controller: Some(controller),
        }
    }

    /// Reads retention status without mutating.
    pub fn status(&self) -> Result<RetentionStatusV1, StorageError> {
        let database = Database::open(&self.database_path).map_err(database_error)?;
        let transaction = database.begin_read().map_err(transaction_error)?;
        let watermark = load_watermark(&transaction)?;
        let holds = load_holds(&transaction)?;
        let incarnation = load_history_incarnation(&transaction)?;
        let fencing = collect_fencing_inputs(&transaction, &holds)?;
        let (max_perm, binding) = compute_max_permissible_watermark(&fencing);
        let tombstones = transaction
            .open_table(HISTORY_TOMBSTONES)
            .map_err(table_error)?;
        let tombstone_count = tombstones.len().map_err(precommit_storage_error)?;
        Ok(RetentionStatusV1 {
            watermark_sequence: watermark
                .as_ref()
                .map(|w| w.watermark_sequence())
                .unwrap_or(0),
            history_incarnation: watermark
                .as_ref()
                .map(|w| w.history_incarnation())
                .unwrap_or(incarnation),
            max_permissible_watermark: max_perm,
            fence_binding: binding,
            holds,
            tombstone_count,
        })
    }

    /// Adds or replaces one operator hold (canonical order preserved).
    pub fn add_hold(
        &self,
        hold_id: impl Into<String>,
        sequence: u64,
        reason: impl Into<String>,
    ) -> Result<(), StorageError> {
        let hold = RetentionHoldV1::new(hold_id, sequence, reason)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let database = Database::open(&self.database_path).map_err(database_error)?;
        let write = begin_durable_write(&database)?;
        {
            let mut meta = write.open_table(META).map_err(table_error)?;
            let existing = read_holds_meta(&meta)?;
            let mut list = existing.holds().to_vec();
            if let Some(slot) = list.iter_mut().find(|h| h.hold_id() == hold.hold_id()) {
                *slot = hold;
            } else {
                list.push(hold);
                list.sort_by(|a, b| a.hold_id().cmp(b.hold_id()));
            }
            let holds = StoredRetentionHoldsV1::new(list)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            let encoded = encode_retention_holds_v1(&holds).map_err(crate::error::codec_error)?;
            meta.insert(META_RETENTION_HOLDS, encoded.as_bytes())
                .map_err(precommit_storage_error)?;
        }
        self.before_commit(RedbTestOperation::RetentionHold)?;
        commit_durable(write)?;
        self.after_commit(RedbTestOperation::RetentionHold)
    }

    /// Removes one operator hold by id. Missing is a no-op success.
    pub fn remove_hold(&self, hold_id: &str) -> Result<(), StorageError> {
        let database = Database::open(&self.database_path).map_err(database_error)?;
        let write = begin_durable_write(&database)?;
        {
            let mut meta = write.open_table(META).map_err(table_error)?;
            let existing = read_holds_meta(&meta)?;
            let list: Vec<_> = existing
                .holds()
                .iter()
                .filter(|h| h.hold_id() != hold_id)
                .cloned()
                .collect();
            let holds = StoredRetentionHoldsV1::new(list)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            let encoded = encode_retention_holds_v1(&holds).map_err(crate::error::codec_error)?;
            meta.insert(META_RETENTION_HOLDS, encoded.as_bytes())
                .map_err(precommit_storage_error)?;
        }
        self.before_commit(RedbTestOperation::RetentionHold)?;
        commit_durable(write)?;
        self.after_commit(RedbTestOperation::RetentionHold)
    }

    /// Marks a projection identity as detached from the fencing minimum.
    pub fn detach_projection(&self, projection_id: &str) -> Result<(), StorageError> {
        if projection_id.is_empty() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let hold_id = format!("{PROJECTION_DETACH_HOLD_PREFIX}{projection_id}");
        self.add_hold(
            hold_id,
            u64::MAX,
            "projection detached from retention fencing",
        )
    }

    /// Offline prune of commits/events/outbox/outbox_status up to `target_inclusive`.
    pub fn prune_to(&self, target_inclusive: u64) -> Result<RetentionStatusV1, StorageError> {
        if target_inclusive == 0 {
            return self.status();
        }
        let database = Database::open(&self.database_path).map_err(database_error)?;

        // First exclusive transaction: delete validated-prefix checkpoint.
        {
            let write = begin_durable_write(&database)?;
            {
                let mut meta = write.open_table(META).map_err(table_error)?;
                let _ = meta
                    .remove(META_VALIDATED_PREFIX_CHECKPOINT)
                    .map_err(precommit_storage_error)?;
            }
            self.before_commit(RedbTestOperation::RetentionPruneCheckpointDelete)?;
            commit_durable(write)?;
            self.after_commit(RedbTestOperation::RetentionPruneCheckpointDelete)?;
        }

        loop {
            let transaction = database.begin_read().map_err(transaction_error)?;
            let current = load_watermark(&transaction)?
                .map(|w| w.watermark_sequence())
                .unwrap_or(0);
            if current >= target_inclusive {
                break;
            }
            let holds = load_holds(&transaction)?;
            let fencing = collect_fencing_inputs(&transaction, &holds)?;
            let (max_perm, _) = compute_max_permissible_watermark(&fencing);
            let Some(max_perm) = max_perm else {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            };
            if target_inclusive > max_perm {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let incarnation = load_history_incarnation(&transaction)?;
            let first = current.saturating_add(1);
            if first > target_inclusive {
                break;
            }
            let last = first
                .saturating_add(RETENTION_PRUNE_SUBRANGE_SEQUENCES.saturating_sub(1))
                .min(target_inclusive)
                .min(max_perm);
            if last < first {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let previous_hash = load_last_tombstone_hash(&transaction)?;
            let registry = current_record_registry_digest();
            drop(transaction);

            let mut write = begin_durable_write(&database)?;
            let (content_digest, counts) = digest_and_count_range(&write, first, last)?;
            delete_commit_range(&mut write, first, last)?;
            delete_event_keyed_range(&mut write, EVENTS, first, last)?;
            delete_event_keyed_range(&mut write, OUTBOX, first, last)?;
            delete_event_keyed_range(&mut write, OUTBOX_STATUS, first, last)?;

            let prev = match previous_hash {
                Some(hash) => Some(hash),
                None if first == 1 => Some(history_tombstone_chain_root(registry)),
                None => return Err(storage_error(StorageErrorKind::InvariantViolation)),
            };
            let tombstone = StoredHistoryTombstoneV1::new(
                first,
                last,
                counts.commits,
                counts.events,
                counts.outbox,
                counts.outbox_status,
                content_digest,
                prev,
                incarnation,
            )
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            {
                let mut tombstones = write.open_table(HISTORY_TOMBSTONES).map_err(table_error)?;
                let key = encode_application_sequence_key(
                    CommitSequence::new(first)
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
                let watermark = StoredRetentionWatermarkV1::new(last, incarnation)
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                let encoded =
                    encode_retention_watermark_v1(&watermark).map_err(crate::error::codec_error)?;
                meta.insert(META_RETENTION_WATERMARK, encoded.as_bytes())
                    .map_err(precommit_storage_error)?;
            }
            self.before_commit(RedbTestOperation::RetentionPruneSubrange)?;
            commit_durable(write)?;
            self.after_commit(RedbTestOperation::RetentionPruneSubrange)?;
        }
        // Drop the exclusive database handle before reopening for status.
        drop(database);
        self.status()
    }

    fn before_commit(&self, operation: RedbTestOperation) -> Result<(), StorageError> {
        if let Some(controller) = &self.test_controller {
            controller.before_commit(operation)?;
        }
        Ok(())
    }

    fn after_commit(&self, operation: RedbTestOperation) -> Result<(), StorageError> {
        if let Some(controller) = &self.test_controller {
            controller.after_commit(operation)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
struct RangeCounts {
    commits: u64,
    events: u64,
    outbox: u64,
    outbox_status: u64,
}

fn begin_durable_write(database: &Database) -> Result<WriteTransaction, StorageError> {
    let mut write = database.begin_write().map_err(transaction_error)?;
    write.set_two_phase_commit(true);
    write
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    Ok(write)
}

fn commit_durable(write: WriteTransaction) -> Result<(), StorageError> {
    write
        .commit()
        .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))
}

pub(crate) fn load_watermark(
    transaction: &redb::ReadTransaction,
) -> Result<Option<StoredRetentionWatermarkV1>, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let Some(encoded) = meta
        .get(META_RETENTION_WATERMARK)
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let item = decode_retention_watermark_v1(encoded.value()).map_err(crate::error::codec_error)?;
    Ok(Some(item.into_parts().0))
}

pub(crate) fn load_holds(
    transaction: &redb::ReadTransaction,
) -> Result<StoredRetentionHoldsV1, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    read_holds_meta(&meta)
}

fn read_holds_meta(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<StoredRetentionHoldsV1, StorageError> {
    let Some(encoded) = meta
        .get(META_RETENTION_HOLDS)
        .map_err(precommit_storage_error)?
    else {
        return Ok(StoredRetentionHoldsV1::empty());
    };
    let item = decode_retention_holds_v1(encoded.value()).map_err(crate::error::codec_error)?;
    Ok(item.into_parts().0)
}

fn load_history_incarnation(transaction: &redb::ReadTransaction) -> Result<u64, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let encoded = meta
        .get(META_HISTORY_INCARNATION)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let incarnation = *codec::decode_history_incarnation_v1(encoded.value())?.value();
    if incarnation < HISTORY_INCARNATION_INITIAL {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(incarnation)
}

fn load_last_tombstone_hash(
    transaction: &redb::ReadTransaction,
) -> Result<Option<HistoryTombstoneHash>, StorageError> {
    let table = transaction
        .open_table(HISTORY_TOMBSTONES)
        .map_err(table_error)?;
    let Some((_, value)) = table.last().map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let item = decode_history_tombstone_v1(value.value()).map_err(crate::error::codec_error)?;
    Ok(Some(item.into_parts().0.tombstone_hash()))
}

/// Collect fencing inputs. Any unreadable fencing source refuses (error).
pub(crate) fn collect_fencing_inputs(
    transaction: &redb::ReadTransaction,
    holds: &StoredRetentionHoldsV1,
) -> Result<RetentionFencingInputs, StorageError> {
    let detached = detached_projection_ids(holds);
    let min_projection = min_projection_durable_frontier(transaction, &detached)?;
    let min_hold = holds
        .holds()
        .iter()
        .filter(|h| !h.hold_id().starts_with(PROJECTION_DETACH_HOLD_PREFIX))
        .map(RetentionHoldV1::sequence)
        .min();
    let undelivered = undelivered_outbox_low_water(transaction)?;
    let staged = staged_migration_frozen_frontier(transaction)?;
    Ok(RetentionFencingInputs {
        min_projection_durable_frontier: min_projection,
        min_operator_hold_sequence: min_hold,
        undelivered_outbox_low_water: undelivered,
        staged_migration_frozen_frontier: staged,
    })
}

fn detached_projection_ids(holds: &StoredRetentionHoldsV1) -> BTreeSet<String> {
    holds
        .holds()
        .iter()
        .filter_map(|h| {
            h.hold_id()
                .strip_prefix(PROJECTION_DETACH_HOLD_PREFIX)
                .map(str::to_owned)
        })
        .collect()
}

fn min_projection_durable_frontier(
    transaction: &redb::ReadTransaction,
    detached: &BTreeSet<String>,
) -> Result<Option<u64>, StorageError> {
    let controls = transaction
        .open_table(PROJECTION_FRONTIER)
        .map_err(table_error)?;
    let mut min_frontier: Option<u64> = None;
    let mut any = false;
    for entry in controls.iter().map_err(precommit_storage_error)? {
        let (_, encoded) = entry.map_err(precommit_storage_error)?;
        let control = decode_projection_control_v1(encoded.value())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .into_parts()
            .0;
        let id_key = control.identity().projection_id().get().to_string();
        if detached.contains(&id_key) {
            continue;
        }
        any = true;
        let frontier = control
            .published()
            .map(|p| match p.frontier() {
                FrontierPosition::BeforeFirst => 0,
                FrontierPosition::AppliedThrough(seq) => seq.get(),
            })
            .unwrap_or(0);
        min_frontier = Some(match min_frontier {
            None => frontier,
            Some(existing) => existing.min(frontier),
        });
    }
    Ok(if any { min_frontier } else { None })
}

fn undelivered_outbox_low_water(
    transaction: &redb::ReadTransaction,
) -> Result<Option<u64>, StorageError> {
    let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
    let mut low: Option<u64> = None;
    for entry in statuses.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let event_id = decode_event_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let status: StoredOutboxStatusV1 = codec::decode_outbox_status_v1(value.value())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .into_parts()
            .0;
        if !outbox_status_is_terminal(&status) {
            let seq = event_id.commit_sequence().get();
            let bind = seq.saturating_sub(1);
            low = Some(match low {
                None => bind,
                Some(existing) => existing.min(bind),
            });
        }
    }
    Ok(low)
}

fn outbox_status_is_terminal(status: &StoredOutboxStatusV1) -> bool {
    use riffdb_storage_api::OutboxDeliveryStateV1;
    matches!(
        status.state(),
        OutboxDeliveryStateV1::Delivered { .. } | OutboxDeliveryStateV1::DeadLetter { .. }
    )
}

fn staged_migration_frozen_frontier(
    transaction: &redb::ReadTransaction,
) -> Result<Option<u64>, StorageError> {
    let journal = transaction
        .open_table(CONTRACT_MIGRATION_JOURNAL)
        .map_err(table_error)?;
    let mut min_frontier: Option<u64> = None;
    for entry in journal.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        let item =
            riffdb_storage_api::proto_codec::decode_contract_migration_journal_v1(value.value())
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let row = item.into_parts().0;
        if let Some(frontier) = row.frozen_application_frontier() {
            let seq = frontier.get();
            min_frontier = Some(match min_frontier {
                None => seq,
                Some(existing) => existing.min(seq),
            });
        }
    }
    Ok(min_frontier)
}

fn digest_and_count_range(
    write: &WriteTransaction,
    first: u64,
    last: u64,
) -> Result<(HistoryTombstoneContentDigest, RangeCounts), StorageError> {
    let mut preimage = Vec::new();
    preimage.extend_from_slice(b"riffdb.history-tombstone-content/v1\0");
    preimage.extend_from_slice(&first.to_be_bytes());
    preimage.extend_from_slice(&last.to_be_bytes());
    let mut counts = RangeCounts::default();

    {
        let commits = write.open_table(COMMITS).map_err(table_error)?;
        let first_key = encode_application_sequence_key(
            CommitSequence::new(first)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        );
        let last_key = encode_application_sequence_key(
            CommitSequence::new(last)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        );
        for entry in commits
            .range::<&[u8]>((
                Included(first_key.as_slice()),
                Included(last_key.as_slice()),
            ))
            .map_err(precommit_storage_error)?
        {
            let (key, value) = entry.map_err(precommit_storage_error)?;
            preimage.extend_from_slice(b"commits\0");
            preimage.extend_from_slice(key.value());
            preimage.extend_from_slice(&(value.value().len() as u64).to_be_bytes());
            preimage.extend_from_slice(value.value());
            counts.commits = counts
                .commits
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }

    for (label, definition, counter) in [
        ("events", EVENTS, &mut counts.events),
        ("outbox", OUTBOX, &mut counts.outbox),
        ("outbox_status", OUTBOX_STATUS, &mut counts.outbox_status),
    ] {
        let table = write.open_table(definition).map_err(table_error)?;
        let mut lower = [0u8; 16];
        lower[..8].copy_from_slice(&first.to_be_bytes());
        let mut upper = [0xffu8; 16];
        upper[..8].copy_from_slice(&last.to_be_bytes());
        for entry in table
            .range::<&[u8]>((Included(lower.as_slice()), Included(upper.as_slice())))
            .map_err(precommit_storage_error)?
        {
            let (key, value) = entry.map_err(precommit_storage_error)?;
            preimage.extend_from_slice(label.as_bytes());
            preimage.extend_from_slice(b"\0");
            preimage.extend_from_slice(key.value());
            preimage.extend_from_slice(&(value.value().len() as u64).to_be_bytes());
            preimage.extend_from_slice(value.value());
            *counter = counter
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }

    let digest = hash(HashDomain::Schema, &preimage);
    Ok((
        HistoryTombstoneContentDigest::from_bytes(*digest.as_bytes()),
        counts,
    ))
}

fn delete_commit_range(
    write: &mut WriteTransaction,
    first: u64,
    last: u64,
) -> Result<(), StorageError> {
    let mut table = write.open_table(COMMITS).map_err(table_error)?;
    let first_key = encode_application_sequence_key(
        CommitSequence::new(first)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
    );
    let last_key = encode_application_sequence_key(
        CommitSequence::new(last)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
    );
    let mut keys = Vec::new();
    for entry in table
        .range::<&[u8]>((
            Included(first_key.as_slice()),
            Included(last_key.as_slice()),
        ))
        .map_err(precommit_storage_error)?
    {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        keys.push(key.value().to_vec());
    }
    for key in keys {
        let _ = table
            .remove(key.as_slice())
            .map_err(precommit_storage_error)?;
    }
    Ok(())
}

fn delete_event_keyed_range(
    write: &mut WriteTransaction,
    definition: TableDefinition<'_, &[u8], &[u8]>,
    first: u64,
    last: u64,
) -> Result<(), StorageError> {
    let mut table = write.open_table(definition).map_err(table_error)?;
    let mut lower = [0u8; 16];
    lower[..8].copy_from_slice(&first.to_be_bytes());
    let mut upper = [0xffu8; 16];
    upper[..8].copy_from_slice(&last.to_be_bytes());
    let mut keys = Vec::new();
    for entry in table
        .range::<&[u8]>((Included(lower.as_slice()), Included(upper.as_slice())))
        .map_err(precommit_storage_error)?
    {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        keys.push(key.value().to_vec());
    }
    for key in keys {
        let _ = table
            .remove(key.as_slice())
            .map_err(precommit_storage_error)?;
    }
    Ok(())
}

/// Verifies the tombstone chain covers `[1, watermark]` contiguously.
pub(crate) fn verify_tombstone_chain(
    transaction: &redb::ReadTransaction,
    watermark: u64,
    registry_digest: SchemaHash,
) -> Result<(), StorageError> {
    if watermark == 0 {
        let table = transaction
            .open_table(HISTORY_TOMBSTONES)
            .map_err(table_error)?;
        if table.len().map_err(precommit_storage_error)? != 0 {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(());
    }
    let table = transaction
        .open_table(HISTORY_TOMBSTONES)
        .map_err(table_error)?;
    let mut expected_previous = Some(history_tombstone_chain_root(registry_digest));
    let mut expected_first = 1u64;
    let mut last_seen = 0u64;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let first_key = decode_application_sequence_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let tombstone = decode_history_tombstone_v1(value.value())
            .map_err(crate::error::codec_error)?
            .into_parts()
            .0;
        if tombstone.first_sequence() != first_key.get()
            || tombstone.first_sequence() != expected_first
            || tombstone.previous_tombstone_hash() != expected_previous
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let computed = tombstone
            .computed_hash()
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if computed != tombstone.tombstone_hash() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        expected_previous = Some(tombstone.tombstone_hash());
        expected_first = tombstone
            .last_sequence()
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        last_seen = tombstone.last_sequence();
    }
    if last_seen != watermark {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(())
}

/// Returns true when `sequence` is covered by the watermark (below-or-equal).
#[must_use]
pub(crate) const fn sequence_covered_by_watermark(sequence: u64, watermark: u64) -> bool {
    sequence > 0 && sequence <= watermark
}
