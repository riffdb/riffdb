//! Rebuildable operational accelerators derived from authoritative tables.

#[path = "transient_follower.rs"]
pub(crate) mod follower;
#[path = "transient_payload.rs"]
mod payload;
use payload::{PayloadCache, SegmentMetadata};

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use redb::{ReadTransaction, ReadableTable};
use riffdb_storage_api::{
    CommandDerivedIndexKindV1, CommandDerivedMemberV1, CommandSegmentDigestV1,
    EventRouteUpperFenceV1, OutboxDeliveryStateV1, StorageError, StorageErrorKind,
    StoredCommandCapsuleV2, StoredCommandSegmentV1, StoredEventRouteV1,
};
use riffdb_types::{CommitSequence, EventId, PartitionKeyHash};

use crate::codec::{decode_event_route_v1, decode_outbox_status_v1};
use crate::error::{precommit_storage_error, table_error};
use crate::keys::{
    decode_application_sequence_key, decode_audit_by_request_key, decode_event_key,
    encode_audit_by_request_key, encode_audit_by_request_prefix, encode_audit_key,
    encode_event_key, encode_event_route_key, encode_idempotency_key, encode_provenance_key,
};
use crate::layout::{COMMITS, EVENT_ROUTES, OUTBOX, OUTBOX_STATUS};

pub(crate) struct TransientIndexes {
    event_routes: Option<BTreeMap<Vec<u8>, StoredEventRouteV1>>,
    outbox_intents: Option<BTreeSet<EventId>>,
    pending_outbox: Option<BTreeSet<EventId>>,
    undelivered_outbox: Option<BTreeSet<EventId>>,
    command_derived: Option<CommandDerivedIndexes>,
}

type EventRoutePage = Result<(EventRouteUpperFenceV1, Vec<StoredEventRouteV1>, bool), StorageError>;

/// Exact command identities whose redb roots have been sealed but whose
/// durability fences have not yet advanced the public read frontier.
///
/// Later writers must observe these identities to preserve idempotency across
/// pipelined epochs. Public readers deliberately never consult this overlay.
#[derive(Default)]
pub(crate) struct UnpublishedCommandIndexes {
    segments: BTreeMap<CommitSequence, Arc<StoredCommandSegmentV1>>,
    exact: BTreeMap<(CommandDerivedIndexKindV1, Vec<u8>), CommandDerivedLocator>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommandDerivedLocator {
    pub(crate) segment_first: CommitSequence,
    pub(crate) command_ordinal: u16,
    pub(crate) member_ordinal: u16,
    pub(crate) member: CommandDerivedMemberV1,
}

#[derive(Default)]
struct CommandDerivedIndexes {
    segments: BTreeMap<CommitSequence, SegmentMetadata>,
    payloads: PayloadCache,
    idempotency: BTreeMap<Vec<u8>, CommandDerivedLocator>,
    provenance: BTreeMap<Vec<u8>, CommandDerivedLocator>,
    audit_sequence: BTreeMap<Vec<u8>, CommandDerivedLocator>,
    audit_request: BTreeMap<Vec<u8>, CommandDerivedLocator>,
    event_route: BTreeMap<Vec<u8>, CommandDerivedLocator>,
    pending_outbox: BTreeMap<Vec<u8>, CommandDerivedLocator>,
}

struct RebuiltOutboxIndexes {
    intents: Option<BTreeSet<EventId>>,
    pending: Option<BTreeSet<EventId>>,
    undelivered: Option<BTreeSet<EventId>>,
}

#[derive(Default)]
#[allow(clippy::large_enum_variant)] // Ready is the one long-lived state; boxing adds read indirection.
pub(crate) enum TransientIndexState {
    #[default]
    Dormant,
    Ready(TransientIndexes),
    Invalid,
}

pub(crate) enum TransientIndexDelta {
    PendingOutboxInserted(Vec<EventId>),
    CommandSegmentPublished(Arc<StoredCommandSegmentV1>),
    PendingOutboxMembership {
        event_id: EventId,
        was_pending: bool,
        pending: bool,
        was_undelivered: bool,
        undelivered: bool,
    },
}

impl TransientIndexDelta {
    pub(crate) fn command_segment(&self) -> Option<&Arc<StoredCommandSegmentV1>> {
        match self {
            Self::CommandSegmentPublished(segment) => Some(segment),
            Self::PendingOutboxInserted(_) | Self::PendingOutboxMembership { .. } => None,
        }
    }

    pub(crate) fn command_derived_member(
        &self,
        kind: CommandDerivedIndexKindV1,
        exact_key: &[u8],
    ) -> Option<(Arc<StoredCommandSegmentV1>, CommandDerivedLocator)> {
        let segment = self.command_segment()?;
        let entry = segment
            .manifest()
            .entries()
            .iter()
            .find(|entry| entry.kind() == kind && entry.exact_key() == exact_key)?;
        Some((
            Arc::clone(segment),
            CommandDerivedLocator {
                segment_first: entry.segment_first_commit_sequence(),
                command_ordinal: entry.command_ordinal(),
                member_ordinal: entry.member_ordinal(),
                member: entry.member(),
            },
        ))
    }

    pub(crate) fn command_audit_sequences_for(
        &self,
        request_ids: &BTreeSet<riffdb_types::RequestId>,
        output: &mut BTreeMap<riffdb_types::RequestId, Vec<riffdb_types::AdministrationSequence>>,
    ) -> Result<(), StorageError> {
        let Self::CommandSegmentPublished(segment) = self else {
            return Ok(());
        };
        for entry in segment.manifest().entries() {
            if entry.kind() != CommandDerivedIndexKindV1::AuditRequest {
                continue;
            }
            let (request_id, sequence) =
                decode_audit_by_request_key(entry.exact_key()).map_err(|_| corrupt())?;
            if request_ids.contains(&request_id) {
                let sequences = output.entry(request_id).or_default();
                if !sequences.contains(&sequence) {
                    sequences.push(sequence);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn command_audit_record(
        &self,
        sequence: riffdb_types::AdministrationSequence,
    ) -> Option<riffdb_storage_api::StoredServiceAuditRecordV1> {
        command_audit_from_segment(self.command_segment()?, sequence)
    }

    pub(crate) fn command_audit_tail(
        &self,
    ) -> Option<&riffdb_storage_api::StoredServiceAuditRecordV1> {
        self.command_segment()?
            .commands()
            .last()
            .map(|command| command.base().terminal_audit())
    }
}

impl TransientIndexes {
    /// Rebuilds every transient population index from durable tables.
    ///
    /// Walks the whole `COMMITS` table, decoding each command segment and
    /// re-deriving each manifest key, and retains every decoded segment. Cost
    /// is linear in retained commands with a large per-command constant, and it
    /// is the single most expensive operation in an open. Returns the rows
    /// walked so the caller can count what it paid.
    pub(crate) fn rebuild_counted(
        transaction: &ReadTransaction,
    ) -> Result<(Self, u64), StorageError> {
        let commit_rows = table_row_count(transaction, COMMITS)?;
        Ok((Self::rebuild(transaction)?, commit_rows))
    }

    pub(crate) fn rebuild_counted_cancellable(
        transaction: &ReadTransaction,
        cancellation: &std::sync::atomic::AtomicBool,
    ) -> Result<(Self, u64), StorageError> {
        let commit_rows = table_row_count(transaction, COMMITS)?;
        Ok((
            Self::rebuild_with_cancellation(transaction, Some(cancellation))?,
            commit_rows,
        ))
    }

    pub(crate) fn rebuild(transaction: &ReadTransaction) -> Result<Self, StorageError> {
        Self::rebuild_with_cancellation(transaction, None)
    }

    fn rebuild_with_cancellation(
        transaction: &ReadTransaction,
        cancellation: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Self, StorageError> {
        check_rebuild_cancellation(cancellation)?;
        let command_derived = rebuild_command_derived_indexes(transaction, cancellation)?;
        let event_routes =
            rebuild_event_routes(transaction, command_derived.as_ref(), cancellation)?;
        let rebuilt_outbox =
            rebuild_outbox_indexes(transaction, command_derived.as_ref(), cancellation)?;
        Ok(Self {
            event_routes,
            outbox_intents: rebuilt_outbox.intents,
            pending_outbox: rebuilt_outbox.pending,
            undelivered_outbox: rebuilt_outbox.undelivered,
            command_derived,
        })
    }

    pub(crate) fn undelivered_outbox_page(
        &self,
        after: Option<EventId>,
        limit: usize,
    ) -> Option<(Vec<EventId>, bool)> {
        page_event_index(self.undelivered_outbox.as_ref()?, after, limit)
    }

    pub(crate) fn pending_outbox_page(
        &self,
        after: Option<EventId>,
        limit: usize,
    ) -> Option<(Vec<EventId>, bool)> {
        let pending_outbox = self.pending_outbox.as_ref()?;
        let mut events = match after {
            Some(after) => pending_outbox
                .range((Excluded(after), Unbounded))
                .copied()
                .take(limit.saturating_add(1))
                .collect::<Vec<_>>(),
            None => pending_outbox
                .iter()
                .copied()
                .take(limit.saturating_add(1))
                .collect::<Vec<_>>(),
        };
        let has_more = events.len() > limit;
        events.truncate(limit);
        Some((events, has_more))
    }

    pub(crate) fn outbox_intent_last(&self) -> Option<Option<EventId>> {
        Some(self.outbox_intents.as_ref()?.last().copied())
    }

    pub(crate) fn partition_event_route_page(
        &self,
        partition_hash: PartitionKeyHash,
        after: Option<EventId>,
        requested_upper: Option<EventId>,
        limit: usize,
    ) -> Option<EventRoutePage> {
        let routes = self.event_routes.as_ref()?;
        let mut lower = [0_u8; 44];
        lower[..32].copy_from_slice(partition_hash.as_bytes());
        let mut upper = [0xff_u8; 44];
        upper[..32].copy_from_slice(partition_hash.as_bytes());
        let inclusive_upper = match requested_upper {
            Some(event_id) => {
                let key = encode_event_route_key(partition_hash, event_id);
                let Some(route) = routes.get(key.as_slice()) else {
                    return Some(Err(corrupt()));
                };
                if route.event_id() != event_id {
                    return Some(Err(corrupt()));
                }
                EventRouteUpperFenceV1::Inclusive(event_id)
            }
            None => match routes.range(lower.to_vec()..=upper.to_vec()).next_back() {
                Some((_, route)) => EventRouteUpperFenceV1::Inclusive(route.event_id()),
                None => EventRouteUpperFenceV1::BeforeFirst,
            },
        };
        let EventRouteUpperFenceV1::Inclusive(fence) = inclusive_upper else {
            return Some(Ok((inclusive_upper, Vec::new(), false)));
        };
        if after.is_some_and(|after| after >= fence) {
            return Some(Ok((inclusive_upper, Vec::new(), false)));
        }
        let start = after.map_or(lower.to_vec(), |event_id| {
            encode_event_route_key(partition_hash, event_id).to_vec()
        });
        let end = encode_event_route_key(partition_hash, fence).to_vec();
        let mut page = Vec::with_capacity(limit);
        let mut has_more = false;
        for (_, route) in routes.range(start..=end) {
            if after.is_some_and(|after| route.event_id() <= after) {
                continue;
            }
            if page.len() == limit {
                has_more = true;
                break;
            }
            page.push(*route);
        }
        Some(Ok((inclusive_upper, page, has_more)))
    }

    pub(crate) fn command_audit_sequences_for(
        &self,
        request_ids: &BTreeSet<riffdb_types::RequestId>,
    ) -> Option<
        Result<
            BTreeMap<riffdb_types::RequestId, Vec<riffdb_types::AdministrationSequence>>,
            StorageError,
        >,
    > {
        let indexes = self.command_derived.as_ref()?;
        let mut output = BTreeMap::new();
        for &request_id in request_ids {
            let prefix = encode_audit_by_request_prefix(request_id);
            for (exact_key, _) in indexes.audit_request.range(prefix.to_vec()..) {
                if !exact_key.starts_with(prefix.as_slice()) {
                    break;
                }
                let (decoded_request, sequence) =
                    match decode_audit_by_request_key(exact_key).map_err(|_| corrupt()) {
                        Ok(value) => value,
                        Err(error) => return Some(Err(error)),
                    };
                if decoded_request != request_id {
                    return Some(Err(corrupt()));
                }
                output
                    .entry(request_id)
                    .or_insert_with(Vec::new)
                    .push(sequence);
            }
        }
        Some(Ok(output))
    }

    pub(crate) fn has_command_member(
        &self,
        kind: CommandDerivedIndexKindV1,
        key: &[u8],
    ) -> Result<bool, StorageError> {
        let indexes = self.command_derived.as_ref().ok_or_else(corrupt)?;
        Ok(indexes.index(kind).contains_key(key))
    }

    pub(crate) fn has_command_at(&self, sequence: CommitSequence) -> Result<bool, StorageError> {
        let indexes = self.command_derived.as_ref().ok_or_else(corrupt)?;
        Ok(indexes
            .segments
            .range(..=sequence)
            .next_back()
            .is_some_and(|(_, metadata)| sequence <= metadata.last))
    }

    pub(crate) fn command_audit_record(
        &self,
        sequence: riffdb_types::AdministrationSequence,
        load: impl FnOnce(CommitSequence) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Option<riffdb_storage_api::StoredServiceAuditRecordV1>, StorageError> {
        self.command_audit_record_at_or_before(
            sequence,
            Some(CommitSequence::new(u64::MAX).ok_or_else(corrupt)?),
            load,
        )
    }

    pub(crate) fn command_audit_record_at_or_before(
        &self,
        sequence: riffdb_types::AdministrationSequence,
        frontier: Option<CommitSequence>,
        load: impl FnOnce(CommitSequence) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Option<riffdb_storage_api::StoredServiceAuditRecordV1>, StorageError> {
        let indexes = self.command_derived.as_ref().ok_or_else(corrupt)?;
        let key = encode_audit_key(sequence);
        let Some(locator) = indexes.audit_sequence.get(key.as_slice()) else {
            return Ok(None);
        };
        let metadata = indexes
            .segments
            .get(&locator.segment_first)
            .ok_or_else(corrupt)?;
        if frontier.is_none_or(|frontier| metadata.last > frontier) {
            return Ok(None);
        }
        let segment = indexes.payloads.resolve(metadata, load)?;
        command_audit_from_locator(&segment, *locator, sequence)
            .map(Some)
            .ok_or_else(corrupt)
    }

    pub(crate) fn command_derived_member(
        &self,
        kind: CommandDerivedIndexKindV1,
        exact_key: &[u8],
        load: impl FnOnce(CommitSequence) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Option<(Arc<StoredCommandSegmentV1>, CommandDerivedLocator)>, StorageError> {
        let indexes = self.command_derived.as_ref().ok_or_else(corrupt)?;
        let Some(locator) = indexes.index(kind).get(exact_key).copied() else {
            return Ok(None);
        };
        let metadata = indexes
            .segments
            .get(&locator.segment_first)
            .ok_or_else(corrupt)?;
        let segment = indexes.payloads.resolve(metadata, load)?;
        let command = segment
            .commands()
            .get(usize::from(locator.command_ordinal))
            .ok_or_else(corrupt)?;
        if expected_manifest_key(command, kind, locator.member, locator.member_ordinal)?.as_slice()
            != exact_key
        {
            return Err(corrupt());
        }
        Ok(Some((segment, locator)))
    }

    pub(crate) fn command_segment_tail(
        &self,
    ) -> Option<Option<(CommitSequence, CommandSegmentDigestV1)>> {
        Some(
            self.command_derived
                .as_ref()?
                .segments
                .last_key_value()
                .map(|(_, segment)| (segment.first, segment.digest)),
        )
    }

    pub(crate) fn command_segment_coverage(&self) -> Option<Option<CommitSequence>> {
        Some(
            self.command_derived
                .as_ref()?
                .segments
                .last_key_value()
                .map(|(_, segment)| segment.last),
        )
    }

    pub(crate) fn command_at(
        &self,
        sequence: CommitSequence,
        load: impl FnOnce(CommitSequence) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Option<StoredCommandCapsuleV2>, StorageError> {
        let indexes = self.command_derived.as_ref().ok_or_else(corrupt)?;
        let Some((_, metadata)) = indexes.segments.range(..=sequence).next_back() else {
            return Ok(None);
        };
        if sequence > metadata.last {
            return Ok(None);
        }
        let segment = indexes.payloads.resolve(metadata, load)?;
        let ordinal =
            usize::try_from(sequence.get() - metadata.first.get()).map_err(|_| corrupt())?;
        segment
            .commands()
            .get(ordinal)
            .cloned()
            .map(Some)
            .ok_or_else(corrupt)
    }

    #[cfg(test)]
    pub(crate) fn evict_payloads(&self) {
        self.command_derived.as_ref().unwrap().payloads.clear();
    }

    #[cfg(test)]
    pub(crate) fn payload_stats(&self) -> (usize, usize) {
        self.command_derived.as_ref().unwrap().payloads.stats()
    }

    pub(crate) fn apply(&mut self, delta: TransientIndexDelta) {
        match delta {
            TransientIndexDelta::PendingOutboxInserted(events) => {
                let (Some(pending), Some(undelivered)) = (
                    self.pending_outbox.as_mut(),
                    self.undelivered_outbox.as_mut(),
                ) else {
                    return;
                };
                for event_id in events {
                    if !pending.insert(event_id) || !undelivered.insert(event_id) {
                        self.invalidate_outbox();
                        return;
                    }
                }
            }
            TransientIndexDelta::CommandSegmentPublished(segment) => {
                let Some(command_derived) = self.command_derived.as_mut() else {
                    return;
                };
                if command_derived
                    .apply_segment_arc(Arc::clone(&segment))
                    .is_err()
                {
                    self.command_derived = None;
                    self.invalidate_outbox();
                    return;
                }
                let Some(routes) = self.event_routes.as_mut() else {
                    return;
                };
                for command in segment.commands() {
                    for event in command.events() {
                        let key = encode_event_route_key(
                            command.base().commit().partition_hash(),
                            event.event_id(),
                        );
                        let route = StoredEventRouteV1::new(
                            event.event_id(),
                            event.event_type_id(),
                            event.event_hash(),
                        );
                        if routes.insert(key.to_vec(), route).is_some() {
                            self.event_routes = None;
                            self.command_derived = None;
                            self.invalidate_outbox();
                            return;
                        }
                    }
                }
                let events = segment
                    .commands()
                    .iter()
                    .flat_map(|command| command.events().iter().map(|event| event.event_id()));
                let (Some(intents), Some(pending), Some(undelivered)) = (
                    self.outbox_intents.as_mut(),
                    self.pending_outbox.as_mut(),
                    self.undelivered_outbox.as_mut(),
                ) else {
                    return;
                };
                for event_id in events {
                    if !intents.insert(event_id)
                        || !pending.insert(event_id)
                        || !undelivered.insert(event_id)
                    {
                        self.invalidate_outbox();
                        return;
                    }
                }
            }
            TransientIndexDelta::PendingOutboxMembership {
                event_id,
                was_pending,
                pending,
                was_undelivered,
                undelivered,
            } => {
                let (Some(pending_index), Some(undelivered_index)) = (
                    self.pending_outbox.as_mut(),
                    self.undelivered_outbox.as_mut(),
                ) else {
                    return;
                };
                if update_event_membership(pending_index, event_id, was_pending, pending).is_err()
                    || update_event_membership(
                        undelivered_index,
                        event_id,
                        was_undelivered,
                        undelivered,
                    )
                    .is_err()
                {
                    self.invalidate_outbox();
                }
            }
        }
    }

    fn invalidate_outbox(&mut self) {
        self.outbox_intents = None;
        self.pending_outbox = None;
        self.undelivered_outbox = None;
    }
}

impl UnpublishedCommandIndexes {
    pub(crate) fn command_segment_tail(&self) -> Option<(CommitSequence, CommandSegmentDigestV1)> {
        self.segments
            .last_key_value()
            .map(|(_, segment)| (segment.first_commit_sequence(), segment.segment_digest()))
    }

    pub(crate) fn insert_segment(
        &mut self,
        segment: Arc<StoredCommandSegmentV1>,
    ) -> Result<(), StorageError> {
        let first = segment.first_commit_sequence();
        if self.segments.contains_key(&first) {
            return Err(corrupt());
        }
        let mut inserted = Vec::new();
        for entry in segment.manifest().entries() {
            if entry.segment_first_commit_sequence() != first
                || usize::from(entry.command_ordinal()) >= segment.commands().len()
            {
                return Err(corrupt());
            }
            let command = &segment.commands()[usize::from(entry.command_ordinal())];
            let expected = expected_manifest_key(
                command,
                entry.kind(),
                entry.member(),
                entry.member_ordinal(),
            )?;
            if expected.as_slice() != entry.exact_key()
                || self
                    .exact
                    .contains_key(&(entry.kind(), entry.exact_key().to_vec()))
            {
                return Err(corrupt());
            }
            inserted.push((
                entry.kind(),
                entry.exact_key().to_vec(),
                CommandDerivedLocator {
                    segment_first: first,
                    command_ordinal: entry.command_ordinal(),
                    member_ordinal: entry.member_ordinal(),
                    member: entry.member(),
                },
            ));
        }
        self.segments.insert(first, Arc::clone(&segment));
        for (kind, key, locator) in inserted {
            if self.exact.insert((kind, key), locator).is_some() {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    pub(crate) fn remove_segment(
        &mut self,
        segment: &StoredCommandSegmentV1,
    ) -> Result<(), StorageError> {
        let first = segment.first_commit_sequence();
        let removed = self.segments.remove(&first).ok_or_else(corrupt)?;
        if removed.segment_digest() != segment.segment_digest() {
            return Err(corrupt());
        }
        for entry in segment.manifest().entries() {
            let removed = self
                .exact
                .remove(&(entry.kind(), entry.exact_key().to_vec()))
                .ok_or_else(corrupt)?;
            if removed.segment_first != first
                || removed.command_ordinal != entry.command_ordinal()
                || removed.member != entry.member()
            {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    pub(crate) fn command_derived_member(
        &self,
        kind: CommandDerivedIndexKindV1,
        exact_key: &[u8],
    ) -> Option<(Arc<StoredCommandSegmentV1>, CommandDerivedLocator)> {
        let locator = self.exact.get(&(kind, exact_key.to_vec())).copied()?;
        let segment = self.segments.get(&locator.segment_first).cloned()?;
        Some((segment, locator))
    }

    pub(crate) fn command_audit_record(
        &self,
        sequence: riffdb_types::AdministrationSequence,
    ) -> Option<riffdb_storage_api::StoredServiceAuditRecordV1> {
        let exact_key = encode_audit_key(sequence);
        let (segment, locator) = self.command_derived_member(
            CommandDerivedIndexKindV1::AuditSequence,
            exact_key.as_slice(),
        )?;
        command_audit_from_locator(&segment, locator, sequence)
    }

    pub(crate) fn command_audit_sequences_for(
        &self,
        request_ids: &BTreeSet<riffdb_types::RequestId>,
    ) -> Result<
        BTreeMap<riffdb_types::RequestId, Vec<riffdb_types::AdministrationSequence>>,
        StorageError,
    > {
        let mut output = BTreeMap::new();
        for segment in self.segments.values() {
            TransientIndexDelta::CommandSegmentPublished(Arc::clone(segment))
                .command_audit_sequences_for(request_ids, &mut output)?;
        }
        Ok(output)
    }
}

fn command_audit_from_segment(
    segment: &StoredCommandSegmentV1,
    sequence: riffdb_types::AdministrationSequence,
) -> Option<riffdb_storage_api::StoredServiceAuditRecordV1> {
    let key = encode_audit_key(sequence);
    let entry = segment.manifest().entries().iter().find(|entry| {
        entry.kind() == CommandDerivedIndexKindV1::AuditSequence
            && entry.exact_key() == key.as_slice()
    })?;
    let locator = CommandDerivedLocator {
        segment_first: entry.segment_first_commit_sequence(),
        command_ordinal: entry.command_ordinal(),
        member_ordinal: entry.member_ordinal(),
        member: entry.member(),
    };
    command_audit_from_locator(segment, locator, sequence)
}

fn command_audit_from_locator(
    segment: &StoredCommandSegmentV1,
    locator: CommandDerivedLocator,
    sequence: riffdb_types::AdministrationSequence,
) -> Option<riffdb_storage_api::StoredServiceAuditRecordV1> {
    let command = segment
        .commands()
        .get(usize::from(locator.command_ordinal))?;
    let record = match locator.member {
        CommandDerivedMemberV1::AuditStarted => command.base().started_audit(),
        CommandDerivedMemberV1::AuditTerminal => command.base().terminal_audit(),
        CommandDerivedMemberV1::Command | CommandDerivedMemberV1::Event => return None,
    };
    (record.administration_sequence() == sequence).then(|| record.clone())
}

impl Default for TransientIndexes {
    fn default() -> Self {
        Self {
            event_routes: Some(BTreeMap::new()),
            outbox_intents: Some(BTreeSet::new()),
            pending_outbox: Some(BTreeSet::new()),
            undelivered_outbox: Some(BTreeSet::new()),
            command_derived: Some(CommandDerivedIndexes::default()),
        }
    }
}

fn rebuild_event_routes(
    transaction: &ReadTransaction,
    command_derived: Option<&CommandDerivedIndexes>,
    cancellation: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Option<BTreeMap<Vec<u8>, StoredEventRouteV1>>, StorageError> {
    let mut routes = BTreeMap::new();
    if let Some(command_derived) = command_derived {
        for segment in command_derived.segments.values() {
            check_rebuild_cancellation(cancellation)?;
            for (event_id, (partition, route)) in &segment.events {
                check_rebuild_cancellation(cancellation)?;
                let key = encode_event_route_key(*partition, *event_id);
                if routes.insert(key.to_vec(), *route).is_some() {
                    return Err(corrupt());
                }
            }
        }
    }
    let table = transaction.open_table(EVENT_ROUTES).map_err(table_error)?;
    for entry in table.iter().map_err(precommit_storage_error)? {
        check_rebuild_cancellation(cancellation)?;
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let decoded = decode_event_route_v1(value.value())?.into_parts().0;
        let exact_key = key.value().to_vec();
        if let Some(existing) = routes.get(&exact_key) {
            if existing != &decoded {
                return Err(corrupt());
            }
        } else {
            routes.insert(exact_key, decoded);
        }
    }
    Ok(Some(routes))
}

impl CommandDerivedIndexes {
    fn event_ids(&self) -> impl Iterator<Item = EventId> + '_ {
        self.segments
            .values()
            .flat_map(|segment| segment.events.keys().copied())
    }

    fn index(&self, kind: CommandDerivedIndexKindV1) -> &BTreeMap<Vec<u8>, CommandDerivedLocator> {
        match kind {
            CommandDerivedIndexKindV1::Idempotency => &self.idempotency,
            CommandDerivedIndexKindV1::Provenance => &self.provenance,
            CommandDerivedIndexKindV1::AuditSequence => &self.audit_sequence,
            CommandDerivedIndexKindV1::AuditRequest => &self.audit_request,
            CommandDerivedIndexKindV1::EventRoute => &self.event_route,
            CommandDerivedIndexKindV1::PendingOutbox => &self.pending_outbox,
        }
    }

    fn apply_segment_arc(
        &mut self,
        segment: Arc<StoredCommandSegmentV1>,
    ) -> Result<(), StorageError> {
        if self
            .segments
            .insert(
                segment.first_commit_sequence(),
                SegmentMetadata::new(&segment)?,
            )
            .is_some()
        {
            return Err(corrupt());
        }
        for entry in segment.manifest().entries() {
            if entry.segment_first_commit_sequence() != segment.first_commit_sequence()
                || usize::from(entry.command_ordinal()) >= segment.commands().len()
            {
                return Err(corrupt());
            }
            let command = &segment.commands()[usize::from(entry.command_ordinal())];
            let expected_key = expected_manifest_key(
                command,
                entry.kind(),
                entry.member(),
                entry.member_ordinal(),
            )?;
            let expected_member = match entry.kind() {
                CommandDerivedIndexKindV1::Idempotency | CommandDerivedIndexKindV1::Provenance => {
                    CommandDerivedMemberV1::Command
                }
                CommandDerivedIndexKindV1::AuditSequence
                | CommandDerivedIndexKindV1::AuditRequest => entry.member(),
                CommandDerivedIndexKindV1::EventRoute
                | CommandDerivedIndexKindV1::PendingOutbox => CommandDerivedMemberV1::Event,
            };
            if entry.member() != expected_member || entry.exact_key() != expected_key.as_slice() {
                return Err(corrupt());
            }
            let locator = CommandDerivedLocator {
                segment_first: entry.segment_first_commit_sequence(),
                command_ordinal: entry.command_ordinal(),
                member_ordinal: entry.member_ordinal(),
                member: entry.member(),
            };
            let index = self.index_mut(entry.kind());
            if index.insert(entry.exact_key().to_vec(), locator).is_some() {
                return Err(corrupt());
            }
        }

        Ok(())
    }
}

fn expected_manifest_key(
    command: &StoredCommandCapsuleV2,
    kind: CommandDerivedIndexKindV1,
    member: CommandDerivedMemberV1,
    member_ordinal: u16,
) -> Result<Vec<u8>, StorageError> {
    let base = command.base();
    match kind {
        CommandDerivedIndexKindV1::Idempotency => base
            .outcome()
            .identity()
            .storage_key()
            .map(|key| encode_idempotency_key(&key).to_vec())
            .map_err(|_| corrupt()),
        CommandDerivedIndexKindV1::Provenance => {
            Ok(encode_provenance_key(base.provenance().provenance_id()).to_vec())
        }
        CommandDerivedIndexKindV1::AuditSequence => {
            if member_ordinal != 0 {
                return Err(corrupt());
            }
            let record = match member {
                CommandDerivedMemberV1::AuditStarted => base.started_audit(),
                CommandDerivedMemberV1::AuditTerminal => base.terminal_audit(),
                CommandDerivedMemberV1::Command | CommandDerivedMemberV1::Event => {
                    return Err(corrupt());
                }
            };
            Ok(encode_audit_key(record.administration_sequence()).to_vec())
        }
        CommandDerivedIndexKindV1::AuditRequest => {
            if member_ordinal != 0 {
                return Err(corrupt());
            }
            let record = match member {
                CommandDerivedMemberV1::AuditStarted => base.started_audit(),
                CommandDerivedMemberV1::AuditTerminal => base.terminal_audit(),
                CommandDerivedMemberV1::Command | CommandDerivedMemberV1::Event => {
                    return Err(corrupt());
                }
            };
            Ok(
                encode_audit_by_request_key(record.request_id(), record.administration_sequence())
                    .to_vec(),
            )
        }
        CommandDerivedIndexKindV1::EventRoute => {
            let event = command
                .events()
                .get(usize::from(member_ordinal))
                .ok_or_else(corrupt)?;
            Ok(encode_event_route_key(base.commit().partition_hash(), event.event_id()).to_vec())
        }
        CommandDerivedIndexKindV1::PendingOutbox => {
            let event = command
                .events()
                .get(usize::from(member_ordinal))
                .ok_or_else(corrupt)?;
            Ok(encode_event_key(event.event_id()).to_vec())
        }
    }
}

/// Row count from redb table metadata (never a walk).
fn table_row_count(
    transaction: &ReadTransaction,
    definition: redb::TableDefinition<'static, &'static [u8], &'static [u8]>,
) -> Result<u64, StorageError> {
    use redb::ReadableTableMetadata;

    let table = transaction.open_table(definition).map_err(table_error)?;
    table.len().map_err(precommit_storage_error)
}

fn rebuild_command_derived_indexes(
    transaction: &ReadTransaction,
    cancellation: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Option<CommandDerivedIndexes>, StorageError> {
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let mut indexes = CommandDerivedIndexes::default();
    for entry in commits.iter().map_err(precommit_storage_error)? {
        check_rebuild_cancellation(cancellation)?;
        let (key, value) = entry.map_err(precommit_storage_error)?;
        match crate::command_prefix::decode_segment(value.value()) {
            Ok(segment) => {
                let segment = segment.into_parts().0;
                let physical =
                    decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;
                if physical != segment.first_commit_sequence() {
                    return Err(corrupt());
                }
                // The replay loop owns each decoded segment and does not read it
                // again, so it moves into the index instead of being deep-cloned
                // once per segment. This loop dominates restart recovery.
                indexes.apply_segment_arc(Arc::new(segment))?;
            }
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
            {
                // Historical command rows retain their durable locator tables;
                // the directory still covers every segment and readers fall
                // back to those rows only when no segment key matches.
            }
            Err(_) => return Err(corrupt()),
        }
    }
    Ok(Some(indexes))
}

fn rebuild_outbox_indexes(
    transaction: &ReadTransaction,
    command_derived: Option<&CommandDerivedIndexes>,
    cancellation: Option<&std::sync::atomic::AtomicBool>,
) -> Result<RebuiltOutboxIndexes, StorageError> {
    // Segment-owned intents are reconstructed from their immutable events.
    // Historical standalone intents are unioned during the pre-alpha format
    // transition. OUTBOX_STATUS remains the independently mutable authority.
    let mut pending = BTreeSet::new();
    let mut undelivered = BTreeSet::new();
    let mut all_intents = BTreeSet::new();
    if let Some(command_derived) = command_derived {
        for event_id in command_derived.event_ids() {
            check_rebuild_cancellation(cancellation)?;
            if !all_intents.insert(event_id)
                || !pending.insert(event_id)
                || !undelivered.insert(event_id)
            {
                return Err(corrupt());
            }
        }
    }
    let intents = transaction.open_table(OUTBOX).map_err(table_error)?;
    let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
    for entry in intents.iter().map_err(precommit_storage_error)? {
        check_rebuild_cancellation(cancellation)?;
        let (key, _) = entry.map_err(precommit_storage_error)?;
        let event_id = decode_event_key(key.value()).map_err(|_| corrupt())?;
        // A transitional standalone row may duplicate the segment-owned fact.
        all_intents.insert(event_id);
        pending.insert(event_id);
        undelivered.insert(event_id);
    }
    for entry in statuses.iter().map_err(precommit_storage_error)? {
        check_rebuild_cancellation(cancellation)?;
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let event_id = decode_event_key(key.value()).map_err(|_| corrupt())?;
        if !all_intents.contains(&event_id) {
            return Ok(RebuiltOutboxIndexes {
                intents: None,
                pending: None,
                undelivered: None,
            });
        }
        let Ok(status) = decode_outbox_status_v1(value.value()) else {
            return Ok(RebuiltOutboxIndexes {
                intents: None,
                pending: None,
                undelivered: None,
            });
        };
        if status.value().event_id() != event_id {
            return Ok(RebuiltOutboxIndexes {
                intents: None,
                pending: None,
                undelivered: None,
            });
        }
        if !status.value().state().is_pending() {
            pending.remove(&event_id);
        }
        if matches!(
            status.value().state(),
            OutboxDeliveryStateV1::Delivered { .. }
        ) {
            undelivered.remove(&event_id);
        }
    }
    Ok(RebuiltOutboxIndexes {
        intents: Some(all_intents),
        pending: Some(pending),
        undelivered: Some(undelivered),
    })
}

fn page_event_index(
    index: &BTreeSet<EventId>,
    after: Option<EventId>,
    limit: usize,
) -> Option<(Vec<EventId>, bool)> {
    let mut events = match after {
        Some(after) => index
            .range((Excluded(after), Unbounded))
            .copied()
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>(),
        None => index
            .iter()
            .copied()
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>(),
    };
    let has_more = events.len() > limit;
    events.truncate(limit);
    Some((events, has_more))
}

fn update_event_membership(
    index: &mut BTreeSet<EventId>,
    event_id: EventId,
    expected: bool,
    updated: bool,
) -> Result<(), ()> {
    if index.contains(&event_id) != expected {
        return Err(());
    }
    if updated {
        index.insert(event_id);
    } else {
        index.remove(&event_id);
    }
    Ok(())
}

const fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}

fn check_rebuild_cancellation(
    flag: Option<&std::sync::atomic::AtomicBool>,
) -> Result<(), StorageError> {
    if flag.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire)) {
        return Err(StorageError::new(StorageErrorKind::Unavailable, None));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use riffdb_types::CommitSequence;

    use super::*;

    #[test]
    fn pending_outbox_accelerator_pages_in_event_order_and_tracks_membership() {
        let first = EventId::new(CommitSequence::first(), 0);
        let second = EventId::new(CommitSequence::first(), 1);
        let third = EventId::new(CommitSequence::first(), 2);
        let mut indexes = TransientIndexes::default();
        indexes.apply(TransientIndexDelta::PendingOutboxInserted(vec![
            third, first, second,
        ]));

        assert_eq!(
            indexes.pending_outbox_page(None, 2),
            Some((vec![first, second], true))
        );
        indexes.apply(TransientIndexDelta::PendingOutboxMembership {
            event_id: second,
            was_pending: true,
            pending: false,
            was_undelivered: true,
            undelivered: true,
        });
        assert_eq!(
            indexes.pending_outbox_page(Some(first), 2),
            Some((vec![third], false))
        );
        assert_eq!(
            indexes.undelivered_outbox_page(Some(first), 2),
            Some((vec![second, third], false))
        );
    }
}
