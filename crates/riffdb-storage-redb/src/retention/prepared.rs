//! Off-gate, bounded private prune preparation; every observation is revalidated.

use super::*;
use crate::journal::{JournalMutation, JournalTable};
use crate::store::{RedbOperationalPorts, RedbReadAccess};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;

type ObservedRows = BTreeMap<(JournalTable, Vec<u8>), Option<Vec<u8>>>;

pub(crate) struct PreparedPrune {
    pub(super) first: u64,
    pub(super) last: u64,
    pub(super) segments: Vec<Arc<riffdb_storage_api::StoredCommandSegmentV1>>,
    pub(super) incarnation: u64,
    observed: ObservedRows,
    mutations: Vec<JournalMutation>,
}

struct PreparedViews<'a> {
    read: &'a RedbReadAccess,
    observed: RefCell<ObservedRows>,
    observed_bytes: RefCell<usize>,
    changed: ObservedRows,
    mutations: Vec<JournalMutation>,
    bytes: usize,
}

impl<'a> PreparedViews<'a> {
    fn new(read: &'a RedbReadAccess) -> Self {
        Self {
            read,
            observed: RefCell::default(),
            observed_bytes: RefCell::default(),
            changed: BTreeMap::new(),
            mutations: Vec::new(),
            bytes: 0,
        }
    }

    fn observe(&self, table: JournalTable, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let index = (table, key.to_vec());
        if let Some(value) = self.changed.get(&index) {
            return Ok(value.clone());
        }
        if let Some(value) = self.observed.borrow().get(&index) {
            return Ok(value.clone());
        }
        let mut value = self.read.read_value(table, key)?;
        if table == JournalTable::Commits {
            value = value
                .map(|value| {
                    crate::history_stub::resolve_commits_value(&value)
                        .map(|value| value.into_owned())
                })
                .transpose()?;
        }
        let mut bytes = self.observed_bytes.borrow_mut();
        *bytes = bytes
            .saturating_add(key.len())
            .saturating_add(value.as_ref().map_or(0, Vec::len))
            .saturating_add(64);
        if *bytes > crate::journal::MAX_JOURNAL_FRAME_BYTES
            || self.observed.borrow().len() >= riffdb_storage_api::MAX_CHANGELOG_FRAME_ENTRIES
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        self.observed.borrow_mut().insert(index, value.clone());
        Ok(value)
    }

    fn stage(
        &mut self,
        table: JournalTable,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let before = self.observe(table, key)?;
        let mutation = match (before.as_deref(), value) {
            (None, Some(value)) => JournalMutation::put(table, key, value),
            (Some(before), Some(value)) => JournalMutation::replace(table, key, before, value),
            (Some(before), None) => JournalMutation::delete_matching(table, key, before),
            (None, None) => return Ok(None),
        }
        .map_err(prune_codec_error)?;
        self.bytes = self
            .bytes
            .checked_add(mutation.key().len())
            .and_then(|bytes| bytes.checked_add(mutation.value().map_or(0, <[u8]>::len)))
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        if self.bytes > crate::journal::MAX_JOURNAL_FRAME_BYTES / 2
            || self.mutations.len() >= riffdb_storage_api::MAX_CHANGELOG_FRAME_ENTRIES
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        self.changed
            .insert((table, key.to_vec()), value.map(<[u8]>::to_vec));
        self.mutations.push(mutation);
        Ok(before)
    }
}

impl RetainedViewAccess for PreparedViews<'_> {
    fn get(&self, table: JournalTable, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.observe(table, key)
    }
    fn put(
        &mut self,
        table: JournalTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.stage(table, key, Some(value))
    }
    fn remove(&mut self, table: JournalTable, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.stage(table, key, None)
    }
}

impl PreparedPrune {
    pub(crate) fn prepare(
        ports: &RedbOperationalPorts,
        retain: NonZeroU64,
        allowance: u64,
    ) -> Result<Result<Self, OnlineRetentionHold>, StorageError> {
        let read = ports.begin_composite_read()?;
        let derived = ports.begin_derived_read()?;
        let mut views = PreparedViews::new(&read);
        let watermark = views
            .observe(JournalTable::Meta, META_RETENTION_WATERMARK.as_bytes())?
            .map(|bytes| {
                decode_retention_watermark_v1(&bytes)
                    .map(|value| value.into_parts().0)
                    .map_err(crate::error::codec_error)
            })
            .transpose()?;
        let incarnation = load_history_incarnation(&read)?;
        if watermark
            .as_ref()
            .is_some_and(|watermark| watermark.history_incarnation() != incarnation)
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let current = watermark
            .as_ref()
            .map_or(0, StoredRetentionWatermarkV1::watermark_sequence);
        let root_digest = watermark
            .as_ref()
            .and_then(StoredRetentionWatermarkV1::chain_root_registry_digest)
            .unwrap_or_else(current_record_registry_digest);
        let holds = load_holds(&read)?;
        let mut fences = online::collect_online_fencing_inputs(&read, &derived, &holds)?;
        fences.durable_application_head = Some(access_head(&read)?);
        let target = fences
            .durable_application_head
            .unwrap_or(0)
            .saturating_sub(retain.get());
        if current >= target {
            return Ok(Err(OnlineRetentionHold::TargetReached));
        }
        let (limit, binding) = compute_max_permissible_watermark(&fences);
        let limit = limit.unwrap_or(0).min(target);
        if limit <= current {
            return Ok(Err(OnlineRetentionHold::Fenced(binding)));
        }
        let first = current
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let first_key = first.to_be_bytes();
        // A watermark is a whole-row boundary. Check the preceding visible row
        // through this exact composite pin rather than through its checkpoint.
        for (key, value) in read.read_range_reverse(JournalTable::Commits, &[], &first_key, 1)? {
            let end = online::row_last(
                &value,
                decode_application_sequence_key(&key)
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?,
            )?;
            if end >= first {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
        let mut last = current;
        let mut command_bytes = 0_usize;
        let mut segments = Vec::new();
        let mut commits = Vec::new();
        let mut next_key = first_key.to_vec();
        for _ in 0..257 {
            let Some((key, value)) = read
                .read_range_to(JournalTable::Commits, &next_key, None, 1)?
                .pop()
            else {
                break;
            };
            next_key = increment_key(key.to_vec())?;
            let physical = decode_application_sequence_key(&key)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            if physical.get() != last + 1 {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            if !ports.shared.segment_extent_is_sealed(&value)? {
                break;
            }
            let canonical = crate::history_stub::resolve_commits_value(&value)?.into_owned();
            let segment = online::decode_segment_row(&canonical)?;
            let end = segment.as_ref().map_or(physical.get(), |segment| {
                segment.last_commit_sequence().get()
            });
            if end > limit {
                if last == current && limit < target {
                    return Ok(Err(OnlineRetentionHold::Fenced(binding)));
                }
                break;
            }
            if end > current.saturating_add(allowance)
                && (last > current || end - current > ONLINE_RETENTION_STEP_SEQUENCES)
            {
                break;
            }
            command_bytes = command_bytes.saturating_add(canonical.len());
            if command_bytes > online::ONLINE_RETENTION_STEP_ROW_BYTES {
                break;
            }
            if let Some(segment) = &segment {
                for event in segment
                    .commands()
                    .iter()
                    .flat_map(|command| command.events())
                {
                    if !delivered(&views, event.event_id())? {
                        return Ok(Err(OnlineRetentionHold::Fenced(
                            RetentionFenceBinding::UndeliveredOutboxLowWater,
                        )));
                    }
                }
            }
            views.observe(JournalTable::Commits, &key)?;
            commits.push((key.into_vec(), canonical));
            if let Some(segment) = segment {
                segments.push(Arc::new(segment));
            }
            last = end;
            if last >= limit.min(current.saturating_add(allowance)) {
                break;
            }
        }
        if last == current {
            return Ok(Err(OnlineRetentionHold::RowBoundary));
        }
        let mut additional = Vec::new();
        let (lower, upper) = event_keyed_range_bounds(first, last);
        let exclusive = increment_key(upper.to_vec())?;
        for (label, table) in [
            ("events", JournalTable::Events),
            ("outbox", JournalTable::Outbox),
            ("outbox_status", JournalTable::OutboxStatus),
        ] {
            let rows = bounded_rows(&read, table, &lower, &exclusive)?;
            if rows.len() > riffdb_storage_api::MAX_CHANGELOG_FRAME_ENTRIES {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            for (key, value) in rows {
                if table == JournalTable::Outbox
                    && !delivered(
                        &views,
                        decode_event_key(&key)
                            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?,
                    )?
                {
                    return Ok(Err(OnlineRetentionHold::Fenced(
                        RetentionFenceBinding::UndeliveredOutboxLowWater,
                    )));
                }
                views.observe(table, &key)?;
                additional.push((label, key.into_vec(), value.into_vec()));
            }
        }
        let previous = match read
            .read_range_reverse(JournalTable::HistoryTombstones, &[], &first_key, 1)?
            .pop()
        {
            None if current == 0 => history_tombstone_chain_root(root_digest),
            None => return Err(storage_error(StorageErrorKind::CorruptData)),
            Some((key, value)) => {
                let tombstone = decode_history_tombstone_v1(&value)
                    .map_err(crate::error::codec_error)?
                    .into_parts()
                    .0;
                if tombstone.last_sequence() != current
                    || tombstone.history_incarnation() != incarnation
                    || key.as_ref() != tombstone.first_sequence().to_be_bytes()
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                views.observe(JournalTable::HistoryTombstones, &key)?;
                tombstone.tombstone_hash()
            }
        };
        let mut length = 0;
        let counts = walk_tombstone_preimage_rows(
            first,
            last,
            commits.iter().cloned().map(Ok),
            additional.iter().cloned().map(Ok),
            TombstoneSink::Measure(&mut length),
        )?;
        let mut hasher = ContentHasher::new(HashDomain::Schema, length);
        let hashed = walk_tombstone_preimage_rows(
            first,
            last,
            commits.iter().cloned().map(Ok),
            additional.iter().cloned().map(Ok),
            TombstoneSink::Hash(&mut hasher),
        )?;
        if counts != hashed || counts.commits != last - first + 1 {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let digest = HistoryTombstoneContentDigest::from_bytes(
            *hasher
                .finish()
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?
                .as_bytes(),
        );
        for (_, value) in &commits {
            match online::decode_segment_row(value)? {
                Some(segment) => {
                    for command in segment.commands() {
                        materialize_retained_capsule_views(
                            &mut views,
                            command.base(),
                            command.events(),
                        )?;
                    }
                }
                None => {
                    let is_capsule = match riffdb_storage_api::decode_command_capsule_v2(value) {
                        Ok(_) => true,
                        Err(error) if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => match riffdb_storage_api::decode_command_capsule_event_references(value) {
                            Ok(_) => true,
                            Err(error) if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => false,
                            Err(error) => return Err(crate::error::codec_error(error)),
                        },
                        Err(error) => return Err(crate::error::codec_error(error)),
                    };
                    if is_capsule {
                        let events = read.open_table(EVENTS).map_err(table_error)?;
                        let capsule =
                            codec::decode_command_capsule_with_event_table(value, &events)?
                                .into_parts()
                                .0;
                        materialize_retained_capsule_views(&mut views, &capsule, &[])?;
                    }
                }
            }
        }
        for (key, _) in commits {
            views.remove(JournalTable::Commits, &key)?;
        }
        for (label, key, _) in additional {
            views.remove(
                match label {
                    "events" => JournalTable::Events,
                    "outbox" => JournalTable::Outbox,
                    _ => JournalTable::OutboxStatus,
                },
                &key,
            )?;
        }
        views.remove(
            JournalTable::Meta,
            META_VALIDATED_PREFIX_CHECKPOINT.as_bytes(),
        )?;
        let tombstone = StoredHistoryTombstoneV1::new(
            first,
            last,
            counts.commits,
            counts.events,
            counts.outbox,
            counts.outbox_status,
            digest,
            Some(previous),
            incarnation,
        )
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        views.put(
            JournalTable::HistoryTombstones,
            &first_key,
            encode_history_tombstone_v1(&tombstone)
                .map_err(crate::error::codec_error)?
                .as_bytes(),
        )?;
        let watermark = StoredRetentionWatermarkV1::new(last, incarnation, Some(root_digest))
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        views.put(
            JournalTable::Meta,
            META_RETENTION_WATERMARK.as_bytes(),
            encode_retention_watermark_v1(&watermark)
                .map_err(crate::error::codec_error)?
                .as_bytes(),
        )?;
        Ok(Ok(Self {
            first,
            last,
            segments,
            incarnation,
            observed: views.observed.into_inner(),
            mutations: views.mutations,
        }))
    }

    pub(crate) fn validate_and_stage(
        &self,
        access: &crate::store::RedbWriteAccess,
    ) -> Result<bool, StorageError> {
        for ((table, key), expected) in &self.observed {
            let mut current = access.read_command_value(*table, key)?;
            if *table == JournalTable::Commits {
                current = current
                    .map(|value| {
                        crate::history_stub::resolve_commits_value(&value)
                            .map(|value| value.into_owned())
                    })
                    .transpose()?;
            }
            if &current != expected {
                return Ok(false);
            }
        }
        for mutation in &self.mutations {
            access.stage_retention_mutation(mutation)?;
        }
        Ok(true)
    }
}

pub(super) fn access_head(read: &RedbReadAccess) -> Result<u64, StorageError> {
    let bytes = read
        .read_value(JournalTable::Meta, META_APPLICATION_SEQUENCE.as_bytes())?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    Ok(
        match codec::decode_application_sequence_allocator_v1(&bytes)?
            .into_parts()
            .0
        {
            riffdb_storage_api::ApplicationSequenceAllocator::Next(next) => next.get() - 1,
            riffdb_storage_api::ApplicationSequenceAllocator::Exhausted => u64::MAX,
        },
    )
}

fn delivered(
    views: &PreparedViews<'_>,
    event: riffdb_types::EventId,
) -> Result<bool, StorageError> {
    views
        .observe(JournalTable::OutboxStatus, &event.to_be_bytes())?
        .map(|value| {
            codec::decode_outbox_status_v1(&value).map(|status| {
                matches!(
                    status.value().state(),
                    riffdb_storage_api::OutboxDeliveryStateV1::Delivered { .. }
                )
            })
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

fn increment_key(mut key: Vec<u8>) -> Result<Vec<u8>, StorageError> {
    for byte in key.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return Ok(key);
        }
        *byte = 0;
    }
    Err(storage_error(StorageErrorKind::LimitExceeded))
}

fn prune_codec_error(error: crate::journal::JournalCodecError) -> StorageError {
    storage_error(match error {
        crate::journal::JournalCodecError::LimitExceeded => StorageErrorKind::LimitExceeded,
        _ => StorageErrorKind::CorruptData,
    })
}

fn bounded_rows(
    read: &RedbReadAccess,
    table: JournalTable,
    lower: &[u8],
    upper: &[u8],
) -> Result<Vec<riffdb_storage_api::CompositeRow>, StorageError> {
    let mut rows = Vec::new();
    let mut next = lower.to_vec();
    let mut bytes = 0_usize;
    while let Some(row) = read.read_range(table, &next, upper, 1)?.pop() {
        bytes = bytes
            .saturating_add(row.0.len())
            .saturating_add(row.1.len());
        if bytes > online::ONLINE_RETENTION_STEP_ROW_BYTES
            || rows.len() >= riffdb_storage_api::MAX_CHANGELOG_FRAME_ENTRIES
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        next = increment_key(row.0.to_vec())?;
        rows.push(row);
    }
    Ok(rows)
}
