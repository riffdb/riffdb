//! Validated-prefix startup checkpoint build/write (ADR-0085 Amendment 1).

use redb::{Durability, ReadTransaction, ReadableDatabase, ReadableTable};

use riffdb_storage_api::{
    ApplicationSequenceAllocator, EntityChainFingerprint, EntityTarget, StorageError,
    StorageErrorKind, StoredValidatedPrefixCheckpointV1, ValidatedPrefixRetainedSnapshot,
    ValidatedPrefixSequenceCounts,
    proto_codec::{
        current_record_registry_digest, decode_validated_prefix_checkpoint_v1,
        encode_validated_prefix_checkpoint_v1,
    },
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, EntityVersion, EventId};

use crate::codec;
use crate::error::{precommit_storage_error, storage_error, table_error, transaction_error};
use crate::hooks::RedbTestOperation;
use crate::keys;
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, COMMITS, ENTITIES, EVENT_ROUTES, EVENTS, IDEMPOTENCY, META,
    META_VALIDATED_PREFIX_CHECKPOINT, OUTBOX, OUTBOX_STATUS,
};
use crate::store::SharedRedb;

/// Number of deterministic sample windows derived from the checkpoint self-hash.
pub(crate) const SAMPLE_WINDOW_COUNT: usize = 8;
/// Rows inspected per sample window (clipped to ≤ S).
pub(crate) const SAMPLE_WINDOW_SIZE: u64 = 128;

/// Active checkpoint binding used by a checkpointed structural evidence session.
#[derive(Clone, Debug)]
pub(crate) struct ActiveCheckpoint {
    pub checkpoint_commit_sequence: u64,
    pub audit_sequence_bound: u64,
    pub counts: ValidatedPrefixSequenceCounts,
    /// Fingerprint-verified `(target, version)` map at S; consumed once to seed
    /// suffix entity-chain advancement (`None` after consumption).
    pub entities_at_s: Option<std::collections::BTreeMap<EntityTarget, EntityVersion>>,
    pub checkpoint_hash: [u8; 32],
}

/// Reasons a checkpoint was ignored (fail-closed → full validation). Never blocks open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointIgnoreReason {
    Absent,
    DecodeFailed,
    SelfHashMismatch,
    DatabaseIdMismatch,
    IncarnationMismatch,
    RegistryDigestMismatch,
    SequenceBeyondHead,
    CountImpossible,
    EntityChainMismatch,
}

impl CheckpointIgnoreReason {
    /// Every reason, in counter-index order (see [`Self::index`]).
    pub(crate) const ALL: [Self; 9] = [
        Self::Absent,
        Self::DecodeFailed,
        Self::SelfHashMismatch,
        Self::DatabaseIdMismatch,
        Self::IncarnationMismatch,
        Self::RegistryDigestMismatch,
        Self::SequenceBeyondHead,
        Self::CountImpossible,
        Self::EntityChainMismatch,
    ];

    /// Stable counter index for per-store ignore-reason counting.
    #[must_use]
    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Absent => 0,
            Self::DecodeFailed => 1,
            Self::SelfHashMismatch => 2,
            Self::DatabaseIdMismatch => 3,
            Self::IncarnationMismatch => 4,
            Self::RegistryDigestMismatch => 5,
            Self::SequenceBeyondHead => 6,
            Self::CountImpossible => 7,
            Self::EntityChainMismatch => 8,
        }
    }

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::DecodeFailed => "decode_failed",
            Self::SelfHashMismatch => "self_hash_mismatch",
            Self::DatabaseIdMismatch => "database_id_mismatch",
            Self::IncarnationMismatch => "incarnation_mismatch",
            Self::RegistryDigestMismatch => "registry_digest_mismatch",
            Self::SequenceBeyondHead => "sequence_beyond_head",
            Self::CountImpossible => "count_impossible",
            Self::EntityChainMismatch => "entity_chain_mismatch",
        }
    }
}

/// Builds and durably writes one validated-prefix checkpoint under an exclusive writer.
pub(crate) fn write_validated_prefix_checkpoint(
    shared: &SharedRedb,
    retained: &riffdb_storage_api::RetainedMetadataV1,
) -> Result<(), StorageError> {
    let transaction = shared.database.begin_read().map_err(transaction_error)?;
    let checkpoint = build_checkpoint_from_snapshot(&transaction, retained)?;
    drop(transaction);

    let encoded =
        encode_validated_prefix_checkpoint_v1(&checkpoint).map_err(crate::error::codec_error)?;
    let mut write = shared.database.begin_write().map_err(transaction_error)?;
    write.set_two_phase_commit(true);
    write
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    {
        let mut meta = write.open_table(META).map_err(table_error)?;
        meta.insert(META_VALIDATED_PREFIX_CHECKPOINT, encoded.as_bytes())
            .map_err(precommit_storage_error)?;
    }
    shared.before_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)?;
    shared.commit_durable(write)?;
    shared.after_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)
}

fn build_checkpoint_from_snapshot(
    transaction: &ReadTransaction,
    retained: &riffdb_storage_api::RetainedMetadataV1,
) -> Result<StoredValidatedPrefixCheckpointV1, StorageError> {
    let database_id = retained.database_id();
    let history_incarnation = retained.history_incarnation();
    let registry_digest = current_record_registry_digest();

    let head_commit = last_commit_sequence(transaction)?;
    let checkpoint_commit_sequence = head_commit.map(CommitSequence::get).unwrap_or(0);
    let audit_sequence_bound = last_audit_sequence(transaction)?
        .map(AdministrationSequence::get)
        .unwrap_or(0);

    let s = checkpoint_commit_sequence;
    let counts = ValidatedPrefixSequenceCounts {
        commits_count: count_commits_le(transaction, s)?,
        events_count: count_event_keys_le(transaction, EVENTS, s)?,
        event_routes_count: count_event_routes_le(transaction, s)?,
        outbox_count: count_event_keys_le(transaction, OUTBOX, s)?,
        outbox_status_count: count_event_keys_le(transaction, OUTBOX_STATUS, s)?,
        idempotency_count: count_idempotency_le(transaction, s)?,
        audit_count: count_audit_le(transaction, audit_sequence_bound)?,
        audit_by_request_count: count_audit_by_request_le(transaction, audit_sequence_bound)?,
    };
    let entity_chain_fingerprint = entity_chain_fingerprint_from_entities(transaction)?;
    let retained_snap = retained_snapshot(retained);

    let previous = load_previous_hash(transaction)?;

    StoredValidatedPrefixCheckpointV1::new(
        database_id,
        history_incarnation,
        registry_digest,
        checkpoint_commit_sequence,
        audit_sequence_bound,
        counts,
        entity_chain_fingerprint,
        retained_snap,
        previous,
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
}

fn retained_snapshot(
    retained: &riffdb_storage_api::RetainedMetadataV1,
) -> ValidatedPrefixRetainedSnapshot {
    match retained.application_sequence() {
        ApplicationSequenceAllocator::Next(seq) => ValidatedPrefixRetainedSnapshot {
            next_application_sequence: seq.get(),
            application_sequence_exhausted: false,
            next_administration_sequence: match retained.administration_sequence() {
                riffdb_storage_api::AdministrationSequenceAllocator::Next(s) => s.get(),
                riffdb_storage_api::AdministrationSequenceAllocator::Exhausted => 0,
            },
            administration_sequence_exhausted: matches!(
                retained.administration_sequence(),
                riffdb_storage_api::AdministrationSequenceAllocator::Exhausted
            ),
        },
        ApplicationSequenceAllocator::Exhausted => ValidatedPrefixRetainedSnapshot {
            next_application_sequence: 0,
            application_sequence_exhausted: true,
            next_administration_sequence: match retained.administration_sequence() {
                riffdb_storage_api::AdministrationSequenceAllocator::Next(s) => s.get(),
                riffdb_storage_api::AdministrationSequenceAllocator::Exhausted => 0,
            },
            administration_sequence_exhausted: matches!(
                retained.administration_sequence(),
                riffdb_storage_api::AdministrationSequenceAllocator::Exhausted
            ),
        },
    }
}

fn load_previous_hash(
    transaction: &ReadTransaction,
) -> Result<Option<riffdb_storage_api::ValidatedPrefixCheckpointHash>, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let Some(value) = meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    match decode_validated_prefix_checkpoint_v1(value.value()) {
        Ok(item) => Ok(Some(item.into_parts().0.checkpoint_hash())),
        Err(_) => Ok(None),
    }
}

/// Loads and verifies a checkpoint. On any failure returns `Err(reason)` for ignore → full validation.
pub(crate) fn load_active_checkpoint(
    transaction: &ReadTransaction,
    database_id: DatabaseId,
    history_incarnation: u64,
    full_counts: &[u64; 27],
) -> Result<ActiveCheckpoint, CheckpointIgnoreReason> {
    let meta = transaction
        .open_table(META)
        .map_err(|_| CheckpointIgnoreReason::DecodeFailed)?;
    let Some(value) = meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(|_| CheckpointIgnoreReason::DecodeFailed)?
    else {
        return Err(CheckpointIgnoreReason::Absent);
    };
    let checkpoint = match decode_validated_prefix_checkpoint_v1(value.value()) {
        Ok(item) => item.into_parts().0,
        Err(_) => return Err(CheckpointIgnoreReason::DecodeFailed),
    };
    // from_stored_parts already rechecked self-hash; recompute for belt-and-suspenders.
    let computed = checkpoint
        .computed_hash()
        .map_err(|_| CheckpointIgnoreReason::SelfHashMismatch)?;
    if computed != checkpoint.checkpoint_hash() {
        return Err(CheckpointIgnoreReason::SelfHashMismatch);
    }
    if checkpoint.database_id() != database_id {
        return Err(CheckpointIgnoreReason::DatabaseIdMismatch);
    }
    if checkpoint.history_incarnation() != history_incarnation {
        return Err(CheckpointIgnoreReason::IncarnationMismatch);
    }
    if checkpoint.registry_digest() != current_record_registry_digest() {
        return Err(CheckpointIgnoreReason::RegistryDigestMismatch);
    }
    let head = last_commit_sequence(transaction)
        .map_err(|_| CheckpointIgnoreReason::DecodeFailed)?
        .map(CommitSequence::get)
        .unwrap_or(0);
    if checkpoint.checkpoint_commit_sequence() > head {
        return Err(CheckpointIgnoreReason::SequenceBeyondHead);
    }
    let counts = checkpoint.counts();
    // Phase indices: 10 COMMITS, 12 EVENTS, 13 EVENT_ROUTES, 14 OUTBOX, 15 OUTBOX_STATUS,
    // 8 IDEMPOTENCY, 21 AUDIT, 22 AUDIT_BY_REQUEST.
    if counts.commits_count > full_counts[10]
        || counts.events_count > full_counts[12]
        || counts.event_routes_count > full_counts[13]
        || counts.outbox_count > full_counts[14]
        || counts.outbox_status_count > full_counts[15]
        || counts.idempotency_count > full_counts[8]
        || counts.audit_count > full_counts[21]
        || counts.audit_by_request_count > full_counts[22]
    {
        return Err(CheckpointIgnoreReason::CountImpossible);
    }
    // Entity-chain fingerprint must match the reconstructed-at-S map from current
    // ENTITIES adjusted by suffix. Mismatch → full validation. Contract-migration
    // version jumps between write and open also land here (reconstruction cannot
    // see migration bumps): the fallback is counted so migrated databases lose
    // the fast path visibly, never silently.
    let entities_at_s =
        match reconstruct_entities_at_s(transaction, checkpoint.checkpoint_commit_sequence()) {
            Ok(map) => map,
            Err(_) => return Err(CheckpointIgnoreReason::EntityChainMismatch),
        };
    let fingerprint =
        EntityChainFingerprint::from_sorted_pairs(entities_at_s.iter().map(|(t, v)| (t, *v)));
    if fingerprint != checkpoint.entity_chain_fingerprint() {
        return Err(CheckpointIgnoreReason::EntityChainMismatch);
    }
    Ok(ActiveCheckpoint {
        checkpoint_commit_sequence: checkpoint.checkpoint_commit_sequence(),
        audit_sequence_bound: checkpoint.audit_sequence_bound(),
        counts,
        entities_at_s: Some(entities_at_s),
        checkpoint_hash: *checkpoint.checkpoint_hash().as_bytes(),
    })
}

/// Derives W=8 window start sequences in [1, S] from the checkpoint self-hash.
pub(crate) fn sample_window_starts(
    checkpoint_hash: &[u8; 32],
    s: u64,
) -> [u64; SAMPLE_WINDOW_COUNT] {
    let mut starts = [0_u64; SAMPLE_WINDOW_COUNT];
    if s == 0 {
        return starts;
    }
    for (i, start) in starts.iter_mut().enumerate() {
        let off = (i * 4) % 32;
        let word = u32::from_be_bytes([
            checkpoint_hash[off],
            checkpoint_hash[(off + 1) % 32],
            checkpoint_hash[(off + 2) % 32],
            checkpoint_hash[(off + 3) % 32],
        ]);
        // Uniform in [1, S].
        *start = (u64::from(word) % s).saturating_add(1);
    }
    starts
}

fn last_commit_sequence(
    transaction: &ReadTransaction,
) -> Result<Option<CommitSequence>, StorageError> {
    let table = transaction.open_table(COMMITS).map_err(table_error)?;
    let Some((key, _)) = table.last().map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    Ok(Some(
        keys::decode_application_sequence_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?,
    ))
}

fn last_audit_sequence(
    transaction: &ReadTransaction,
) -> Result<Option<AdministrationSequence>, StorageError> {
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let Some((key, _)) = table.last().map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    Ok(Some(keys::decode_audit_key(key.value()).map_err(|_| {
        storage_error(StorageErrorKind::CorruptData)
    })?))
}

fn count_commits_le(transaction: &ReadTransaction, s: u64) -> Result<u64, StorageError> {
    if s == 0 {
        return Ok(0);
    }
    let table = transaction.open_table(COMMITS).map_err(table_error)?;
    let upper =
        CommitSequence::new(s).ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let upper_key = keys::encode_application_sequence_key(upper);
    let mut count = 0_u64;
    for entry in table
        .range::<&[u8]>(..=upper_key.as_slice())
        .map_err(precommit_storage_error)?
    {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        let seq = keys::decode_application_sequence_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if seq.get() <= s {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn count_event_keys_le(
    transaction: &ReadTransaction,
    definition: redb::TableDefinition<'_, &'static [u8], &'static [u8]>,
    s: u64,
) -> Result<u64, StorageError> {
    if s == 0 {
        return Ok(0);
    }
    let table = transaction.open_table(definition).map_err(table_error)?;
    let mut count = 0_u64;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        let Ok(id) = keys::decode_event_key(key.value()) else {
            continue;
        };
        if id.commit_sequence().get() <= s {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn count_event_routes_le(transaction: &ReadTransaction, s: u64) -> Result<u64, StorageError> {
    if s == 0 {
        return Ok(0);
    }
    let table = transaction.open_table(EVENT_ROUTES).map_err(table_error)?;
    let mut count = 0_u64;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        let Ok((_, id)) = keys::decode_event_route_key(key.value()) else {
            continue;
        };
        if id.commit_sequence().get() <= s {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn count_idempotency_le(transaction: &ReadTransaction, s: u64) -> Result<u64, StorageError> {
    if s == 0 {
        return Ok(0);
    }
    let table = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
    let mut count = 0_u64;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        let Ok(record) = codec::decode_idempotency_record_v1(value.value()) else {
            continue;
        };
        let seq = match record.into_parts().0 {
            crate::codec::IdempotencyRecordV1::StoredOutcome(outcome) => {
                outcome.commit_sequence().get()
            }
            crate::codec::IdempotencyRecordV1::ExecutionFailed(_) => continue,
        };
        if seq <= s {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn count_audit_le(transaction: &ReadTransaction, bound: u64) -> Result<u64, StorageError> {
    if bound == 0 {
        return Ok(0);
    }
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let upper = AdministrationSequence::new(bound)
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let upper_key = keys::encode_audit_key(upper);
    let mut count = 0_u64;
    for entry in table
        .range::<&[u8]>(..=upper_key.as_slice())
        .map_err(precommit_storage_error)?
    {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        let seq = keys::decode_audit_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if seq.get() <= bound {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn count_audit_by_request_le(
    transaction: &ReadTransaction,
    bound: u64,
) -> Result<u64, StorageError> {
    if bound == 0 {
        return Ok(0);
    }
    let table = transaction
        .open_table(AUDIT_BY_REQUEST)
        .map_err(table_error)?;
    let mut count = 0_u64;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        let Ok((_, seq)) = keys::decode_audit_by_request_key(key.value()) else {
            continue;
        };
        if seq.get() <= bound {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn entity_chain_fingerprint_from_entities(
    transaction: &ReadTransaction,
) -> Result<EntityChainFingerprint, StorageError> {
    let table = transaction.open_table(ENTITIES).map_err(table_error)?;
    let mut pairs = Vec::new();
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        let record = codec::decode_entity_record_v1(value.value())?
            .into_parts()
            .0;
        pairs.push((record.target().clone(), record.entity_version()));
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(EntityChainFingerprint::from_sorted_pairs(
        pairs.iter().map(|(t, v)| (t, *v)),
    ))
}

/// Reconstructs the `(target, version)` map at S from current ENTITIES adjusted
/// by suffix commits. The caller fingerprints the map and, when the fingerprint
/// binds, seeds suffix entity-chain advancement from it.
fn reconstruct_entities_at_s(
    transaction: &ReadTransaction,
    s: u64,
) -> Result<std::collections::BTreeMap<EntityTarget, EntityVersion>, StorageError> {
    use std::collections::BTreeMap;
    use std::ops::Bound::{Included, Unbounded};

    let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
    let mut map: BTreeMap<EntityTarget, EntityVersion> = BTreeMap::new();
    for entry in entities.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        let record = codec::decode_entity_record_v1(value.value())?
            .into_parts()
            .0;
        map.insert(record.target().clone(), record.entity_version());
    }
    // Walk suffix commits forward; track first-touch version per target.
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let mut first_suffix: BTreeMap<EntityTarget, EntityVersion> = BTreeMap::new();
    if s == u64::MAX {
        return Ok(map);
    }
    // Collect suffix commit payloads first to avoid heterogeneous range types.
    let mut suffix_values = Vec::new();
    if s == 0 {
        for entry in commits.iter().map_err(precommit_storage_error)? {
            let (_, value) = entry.map_err(precommit_storage_error)?;
            suffix_values.push(value.value().to_vec());
        }
    } else if let Some(next) = CommitSequence::new(s.saturating_add(1)) {
        let key = keys::encode_application_sequence_key(next);
        for entry in commits
            .range::<&[u8]>((Included(key.as_slice()), Unbounded))
            .map_err(precommit_storage_error)?
        {
            let (_, value) = entry.map_err(precommit_storage_error)?;
            suffix_values.push(value.value().to_vec());
        }
    }
    for value in suffix_values {
        let Ok(item) = codec::decode_commit_entity_references(&value) else {
            continue;
        };
        let references = item.into_parts().0;
        for reference in references {
            first_suffix
                .entry(reference.target().clone())
                .or_insert_with(|| reference.entity_version());
        }
    }
    // Adjust: first suffix touch version 1 ⇒ absent at S; else version_at_S = first - 1.
    for (target, first_ver) in first_suffix {
        if first_ver == EntityVersion::first() {
            map.remove(&target);
        } else {
            let at_s = EntityVersion::new(first_ver.get() - 1)
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            map.insert(target, at_s);
        }
    }
    Ok(map)
}

/// Lower-bound event key for rows with commit sequence > S.
pub(crate) fn first_event_key_after(s: u64) -> Option<[u8; 12]> {
    let next = CommitSequence::new(s.checked_add(1)?)?;
    Some(keys::encode_event_key(EventId::new(next, 0)))
}

/// Whether a sequence-keyed structural phase uses exclusive range-skip at S.
pub(crate) fn is_range_skipped_phase(phase: usize) -> bool {
    matches!(phase, 10 | 12 | 14 | 15 | 21)
}

/// Whether inspect may be skipped for a prefix row of a full-scan seq-linked table.
pub(crate) fn skip_inspect_for_prefix_row(
    phase: usize,
    key: &[u8],
    value: &[u8],
    s: u64,
    audit_bound: u64,
) -> bool {
    match phase {
        13 => {
            keys::decode_event_route_key(key).is_ok_and(|(_, id)| id.commit_sequence().get() <= s)
        }
        8 => match codec::decode_idempotency_record_v1(value) {
            Ok(item) => match item.into_parts().0 {
                crate::codec::IdempotencyRecordV1::StoredOutcome(outcome) => {
                    outcome.commit_sequence().get() <= s
                }
                crate::codec::IdempotencyRecordV1::ExecutionFailed(_) => false,
            },
            Err(_) => false,
        },
        22 => keys::decode_audit_by_request_key(key).is_ok_and(|(_, seq)| seq.get() <= audit_bound),
        _ => false,
    }
}
