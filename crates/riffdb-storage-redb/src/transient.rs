//! Rebuildable operational accelerators derived from authoritative tables.

#[path = "transient_follower.rs"]
pub(crate) mod follower;
#[path = "transient_payload.rs"]
mod payload;
use crate::locator_memory::{
    ChargedLocatorAllocation, ChargedLocatorKey, LocatorAllocationClass, LocatorOwner,
    LocatorRetireContents, NativeLocatorPool,
};
use crate::locator_tree::{LocatorFamilyMember, LocatorTree};
#[cfg(test)]
pub(crate) use payload::PAYLOAD_COLD_DECODES;
#[cfg(test)]
pub(crate) use payload::PAYLOAD_SEED_VERIFY_EVERY;
pub(crate) use payload::{CapturedCommandPayload, ResolvedCommandPayload};
use payload::{PayloadCache, SegmentMetadata, SegmentMetadataRef};

#[cfg(test)]
thread_local! {
    pub(crate) static PAYLOAD_READ_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    static LOCATOR_CONSTRUCTION_DECODE_ATTEMPTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn before_payload_load() {
    let hook = PAYLOAD_READ_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

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
    // A failed ancillary population scan never invalidates exact command
    // membership. Its own readers retain the original typed failure instead
    // of treating a partial route map as complete or empty.
    event_route_failure: Option<StorageErrorKind>,
    outbox_intents: Option<BTreeSet<EventId>>,
    pending_outbox: Option<BTreeSet<EventId>>,
    undelivered_outbox: Option<BTreeSet<EventId>>,
    command_derived: Option<CommandDerivedIndexes>,
}

type EventRoutePage = Result<(EventRouteUpperFenceV1, Vec<StoredEventRouteV1>, bool), StorageError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommandDerivedLocator {
    pub(crate) segment_first: CommitSequence,
    pub(crate) command_ordinal: u16,
    pub(crate) member_ordinal: u16,
    pub(crate) member: CommandDerivedMemberV1,
}

impl LocatorFamilyMember for CommandDerivedLocator {
    fn validate_family(&self, _: &NativeLocatorPool) -> Result<(), StorageError> {
        Ok(()) // Copy-only scalar locator identity owns no allocation.
    }
}
impl LocatorFamilyMember for SegmentMetadataRef {
    fn validate_family(&self, pool: &NativeLocatorPool) -> Result<(), StorageError> {
        self.validate_family_pool(pool)
    }
}

type ExactLocatorTree = LocatorTree<ChargedLocatorKey, CommandDerivedLocator>;

/// One complete immutable directory for exactly one captured command authority.
/// Cloning shares admitted ownership; successors admit a distinct owner before
/// any path copies and never mutate a directory already visible to a reader.
#[derive(Clone)]
pub(crate) struct LocatorGeneration(Arc<LocatorGenerationState>);

pub(crate) struct LocatorGenerationState {
    pool: Arc<NativeLocatorPool>,
    class: LocatorAllocationClass,
    database_incarnation: Option<(riffdb_types::DatabaseId, u64)>,
    metadata_ownership_bytes: usize,
    segments: LocatorTree<CommitSequence, SegmentMetadataRef>,
    idempotency: ExactLocatorTree,
    provenance: ExactLocatorTree,
    audit_sequence: ExactLocatorTree,
    audit_request: ExactLocatorTree,
    event_route: ExactLocatorTree,
    pending_outbox: ExactLocatorTree,
    // Last: node/key release is enqueued before this preadmitted owner barrier.
    _owner: LocatorOwner,
}

impl std::ops::Deref for LocatorGeneration {
    type Target = LocatorGenerationState;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct CommandDerivedIndexes {
    generation: LocatorGeneration,
    payloads: PayloadCache,
}
impl std::ops::Deref for CommandDerivedIndexes {
    type Target = LocatorGeneration;
    fn deref(&self) -> &LocatorGeneration {
        &self.generation
    }
}

impl LocatorGeneration {
    pub(crate) const fn descriptor_bytes() -> usize {
        std::mem::size_of::<LocatorGenerationState>() + 64 + 256
    }

    pub(crate) fn owner_handle(&self) -> LocatorOwner {
        self._owner.clone()
    }

    pub(crate) fn try_empty(class: LocatorAllocationClass) -> Result<Self, StorageError> {
        let pool = NativeLocatorPool::global();
        Self::try_empty_in_pool(pool, class)
    }

    pub(crate) fn try_empty_in_pool(
        pool: Arc<NativeLocatorPool>,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        let pool = pool.fresh_source_family(class)?;
        let owner = LocatorOwner::try_new_with_bytes(&pool, class, Self::descriptor_bytes())?;
        Ok(Self::empty_with_owner(pool, owner, class))
    }

    pub(crate) fn try_empty_with_owner(
        owner: LocatorOwner,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        let pool = owner.family_pool();
        Ok(Self::empty_with_owner(pool, owner, class))
    }

    fn empty_with_owner(
        pool: Arc<NativeLocatorPool>,
        owner: LocatorOwner,
        class: LocatorAllocationClass,
    ) -> Self {
        Self(Arc::new(LocatorGenerationState {
            _owner: owner,
            pool: Arc::clone(&pool),
            class,
            database_incarnation: None,
            metadata_ownership_bytes: 0,
            segments: LocatorTree::empty(Arc::clone(&pool), class),
            idempotency: LocatorTree::empty(Arc::clone(&pool), class),
            provenance: LocatorTree::empty(Arc::clone(&pool), class),
            audit_sequence: LocatorTree::empty(Arc::clone(&pool), class),
            audit_request: LocatorTree::empty(Arc::clone(&pool), class),
            event_route: LocatorTree::empty(Arc::clone(&pool), class),
            pending_outbox: LocatorTree::empty(pool, class),
        }))
    }

    fn successor(&self, class: LocatorAllocationClass) -> Result<Self, StorageError> {
        let owner = LocatorOwner::try_new_with_bytes(&self.pool, class, Self::descriptor_bytes())?;
        Ok(Self(Arc::new(LocatorGenerationState {
            _owner: owner,
            pool: Arc::clone(&self.pool),
            class,
            database_incarnation: self.database_incarnation,
            metadata_ownership_bytes: self.metadata_ownership_bytes,
            segments: self.segments.with_allocation_class(class),
            idempotency: self.idempotency.with_allocation_class(class),
            provenance: self.provenance.with_allocation_class(class),
            audit_sequence: self.audit_sequence.with_allocation_class(class),
            audit_request: self.audit_request.with_allocation_class(class),
            event_route: self.event_route.with_allocation_class(class),
            pending_outbox: self.pending_outbox.with_allocation_class(class),
        })))
    }

    pub(crate) fn prepare_delta(
        &self,
        delta: &TransientIndexDelta,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        match delta {
            TransientIndexDelta::CommandSegmentPublished(segment, canonical) => {
                if let Some(canonical) = canonical
                    && segment
                        .first_commit_sequence()
                        .get()
                        .is_multiple_of(payload::PAYLOAD_SEED_VERIFY_EVERY)
                {
                    let decoded =
                        decode_native_locator_construction_segment(canonical, None, class, None)?
                            .ok_or_else(corrupt)?;
                    if *decoded != **segment {
                        return Err(corrupt());
                    }
                }
                let mut next = self.successor(class)?;
                next.insert_segment(segment)?;
                Ok(next)
            }
            TransientIndexDelta::HistoryPruned { segments, .. } => {
                self.prepare_history_prune(segments, class)
            }
            _ => Ok(self.clone()),
        }
    }

    pub(crate) fn prepare_history_prune(
        &self,
        segments: &[Arc<StoredCommandSegmentV1>],
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        let mut next = self.successor(class)?;
        for segment in segments {
            let removed = next
                .remove_segment(segment.first_commit_sequence())?
                .ok_or_else(corrupt)?;
            removed.validate(segment)?;
        }
        Ok(next)
    }

    /// The caller supplies only segments decoded from its one captured source.
    /// This builder is private until the entire iterator has validated.
    #[cfg(test)]
    pub(crate) fn rebuild_from_segments(
        segments: impl IntoIterator<Item = Result<Arc<StoredCommandSegmentV1>, StorageError>>,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        let mut result = Self::try_empty(class)?;
        for segment in segments {
            let segment = segment?;
            result.insert_segment(segment.as_ref())?;
        }
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn rebuild_test_segments_with_owner(
        segments: impl IntoIterator<Item = Arc<StoredCommandSegmentV1>>,
        owner: LocatorOwner,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        let mut generation = Self::try_empty_with_owner(owner, class)?;
        for segment in segments {
            generation.insert_segment(&segment)?;
        }
        Ok(generation)
    }

    pub(crate) fn rebuild_from_access_with_owner(
        access: &crate::store::RedbReadAccess,
        owner: LocatorOwner,
        class: LocatorAllocationClass,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<Self, StorageError> {
        Self::rebuild_captured_counted(access, owner, class, &mut cancelled)
            .map(|(generation, _)| generation)
    }

    fn rebuild_captured_counted(
        access: &crate::store::RedbReadAccess,
        owner: LocatorOwner,
        class: LocatorAllocationClass,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<(Self, u64), StorageError> {
        let mut generation = Self::try_empty_with_owner(owner, class)?;
        let mut after = None;
        let mut rows = 0u64;
        loop {
            if cancelled() {
                return Err(unavailable());
            }
            let next = access.visit_next_command_row(after, |physical, bytes| {
                if cancelled() {
                    return Err(unavailable());
                }
                if let Some(segment) = decode_locator_construction_segment(bytes, Some(access))? {
                    if segment.first_commit_sequence() != physical {
                        return Err(corrupt());
                    }
                    generation.insert_segment(&segment)?;
                }
                rows = rows.checked_add(1).ok_or_else(unavailable)?;
                Ok(physical)
            })?;
            let Some(sequence) = next else {
                break;
            };
            after = Some(sequence);
        }
        if cancelled() {
            return Err(unavailable());
        }
        generation.validate_source(access)?;
        Ok((generation, rows))
    }

    pub(crate) fn validate_database_incarnation(
        &self,
        database: riffdb_types::DatabaseId,
        incarnation: u64,
    ) -> Result<(), StorageError> {
        if self
            .database_incarnation
            .is_some_and(|binding| binding.0 != database || binding.1 > incarnation)
        {
            return Err(corrupt());
        }
        Ok(())
    }

    pub(crate) fn validate_source(
        &self,
        access: &crate::store::RedbReadAccess,
    ) -> Result<(), StorageError> {
        let (database, incarnation, _) = access.locator_capability().source_binding()?;
        self.validate_database_incarnation(database, incarnation)?;
        let frontier = access.application_frontier()?;
        if self
            .coverage()
            .is_some_and(|last| frontier.is_none_or(|frontier| last > frontier))
        {
            return Err(corrupt());
        }
        Ok(())
    }

    pub(crate) fn provider_family_pool(&self) -> &Arc<NativeLocatorPool> {
        &self.pool
    }

    #[cfg(test)]
    pub(crate) fn admit_provider_ownership(
        &self,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<Arc<riffdb_types::exact_provider_budget_v1::OwnershipCharge>, StorageError> {
        let mut family = self.pool.lock_provider_family()?;
        let needed = family.provider_ownership_needed(budget)?;
        // Keep the caller's nonzero parent allowance while splitting the exact
        // family plan. This private allowance is released after publication.
        let mut work = budget
            .reserve(needed.checked_add(128).ok_or_else(unavailable)?)
            .map_err(|_| unavailable())?;
        let prepared = family.prepare_provider_ownership(&mut work, budget)?;
        Ok(prepared.commit())
    }

    pub(crate) fn provider_ownership_needed(
        &self,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<u64, StorageError> {
        self.pool
            .lock_provider_family()?
            .provider_ownership_needed(budget)
    }

    pub(crate) fn admit_provider_ownership_from(
        &self,
        reservation: &mut riffdb_types::exact_provider_budget_v1::Reservation,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<Arc<riffdb_types::exact_provider_budget_v1::OwnershipCharge>, StorageError> {
        let mut family = self.pool.lock_provider_family()?;
        Ok(family
            .prepare_provider_ownership(reservation, budget)?
            .commit())
    }

    #[cfg(test)]
    pub(crate) fn retained_ownership_bound(&self) -> Result<u64, StorageError> {
        let mut bytes = self
            ._owner
            .retained_charge_bound()
            .checked_add(self.metadata_ownership_bytes)
            .ok_or_else(unavailable)?;
        for structural in [
            self.segments.structural_charge_bound(),
            self.idempotency.structural_charge_bound(),
            self.provenance.structural_charge_bound(),
            self.audit_sequence.structural_charge_bound(),
            self.audit_request.structural_charge_bound(),
            self.event_route.structural_charge_bound(),
            self.pending_outbox.structural_charge_bound(),
        ] {
            bytes = bytes
                .checked_add(structural.ok_or_else(unavailable)?)
                .ok_or_else(unavailable)?;
        }
        u64::try_from(bytes).map_err(|_| unavailable())
    }

    pub(crate) fn coverage(&self) -> Option<CommitSequence> {
        self.segments.last().map(|(_, m)| m.last)
    }
    pub(crate) fn tail(&self) -> Option<(CommitSequence, CommandSegmentDigestV1)> {
        self.segments.last().map(|(_, m)| (m.first, m.digest))
    }

    pub(crate) fn capture_command_member(
        &self,
        kind: CommandDerivedIndexKindV1,
        key: &[u8],
    ) -> Result<Option<(CapturedCommandPayload, CommandDerivedLocator)>, StorageError> {
        let Some(locator) = self.index(kind).get(key).copied() else {
            return Ok(None);
        };
        let metadata = self
            .segments
            .get(&locator.segment_first)
            .ok_or_else(corrupt)?;
        Ok(Some((
            CapturedCommandPayload::without_cache(metadata.clone()),
            locator,
        )))
    }

    pub(crate) fn capture_command_at(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<CapturedCommandPayload>, StorageError> {
        let Some((_, metadata)) = self.segments.predecessor(&sequence) else {
            return Ok(None);
        };
        Ok((sequence <= metadata.last)
            .then(|| CapturedCommandPayload::without_cache(metadata.clone())))
    }

    pub(crate) fn command_audit_sequences_for(
        &self,
        request_ids: &BTreeSet<riffdb_types::RequestId>,
    ) -> Result<
        BTreeMap<riffdb_types::RequestId, Vec<riffdb_types::AdministrationSequence>>,
        StorageError,
    > {
        let mut output = BTreeMap::new();
        for &request_id in request_ids {
            let prefix = encode_audit_by_request_prefix(request_id);
            for (exact_key, _) in self.audit_request.iter_from(prefix.as_slice()) {
                if !exact_key.as_ref().starts_with(prefix.as_slice()) {
                    break;
                }
                let (decoded_request, sequence) =
                    decode_audit_by_request_key(exact_key.as_ref()).map_err(|_| corrupt())?;
                if decoded_request != request_id {
                    return Err(corrupt());
                }
                let sequences = output.entry(request_id).or_insert_with(Vec::new);
                // A command owns one Started/Terminal pair. Refuse corrupt
                // oversized membership before growing a response collection.
                if sequences.len() >= 2 {
                    return Err(corrupt());
                }
                sequences.push(sequence);
            }
        }
        Ok(output)
    }

    pub(crate) fn has_command_member(
        &self,
        kind: CommandDerivedIndexKindV1,
        key: &[u8],
    ) -> Result<bool, StorageError> {
        Ok(self.index(kind).contains_key(key))
    }

    pub(crate) fn has_command_at(&self, sequence: CommitSequence) -> Result<bool, StorageError> {
        Ok(self
            .segments
            .predecessor(&sequence)
            .is_some_and(|(_, metadata)| sequence <= metadata.last))
    }

    fn index(&self, kind: CommandDerivedIndexKindV1) -> &ExactLocatorTree {
        match kind {
            CommandDerivedIndexKindV1::Idempotency => &self.idempotency,
            CommandDerivedIndexKindV1::Provenance => &self.provenance,
            CommandDerivedIndexKindV1::AuditSequence => &self.audit_sequence,
            CommandDerivedIndexKindV1::AuditRequest => &self.audit_request,
            CommandDerivedIndexKindV1::EventRoute => &self.event_route,
            CommandDerivedIndexKindV1::PendingOutbox => &self.pending_outbox,
        }
    }
    fn insert_segment(&mut self, segment: &StoredCommandSegmentV1) -> Result<(), StorageError> {
        let state = Arc::get_mut(&mut self.0).ok_or_else(unavailable)?;
        let first = segment.first_commit_sequence();
        let binding = (segment.database_id(), segment.history_incarnation());
        if state
            .database_incarnation
            .is_some_and(|expected| expected.0 != binding.0)
            || state
                .segments
                .predecessor(&first)
                .is_some_and(|(_, previous)| previous.last >= first)
            || state
                .segments
                .iter_from(&first)
                .next()
                .is_some_and(|(next, _)| *next <= segment.last_commit_sequence())
        {
            return Err(corrupt());
        }
        let metadata = SegmentMetadata::try_new(&state.pool, state.class, segment)?;
        let metadata_bytes = metadata
            .retained_charge_bound()
            .checked_add(metadata.key_ownership_bytes)
            .ok_or_else(unavailable)?;
        let ownership_bytes = state
            .metadata_ownership_bytes
            .checked_add(metadata_bytes)
            .ok_or_else(unavailable)?;
        for entry in metadata.manifest.entries() {
            let index = state.index_mut(entry.kind());
            if index.contains_key(entry.exact_key()) {
                return Err(corrupt());
            }
            *index = index.try_insert(entry.charged_key(), entry.locator())?;
        }
        state.segments = state.segments.try_insert(first, metadata)?;
        state.metadata_ownership_bytes = ownership_bytes;
        state.database_incarnation = Some((
            binding.0,
            state
                .database_incarnation
                .map_or(binding.1, |previous| previous.1.max(binding.1)),
        ));
        Ok(())
    }
    fn remove_segment(
        &mut self,
        first: CommitSequence,
    ) -> Result<Option<SegmentMetadataRef>, StorageError> {
        let state = Arc::get_mut(&mut self.0).ok_or_else(unavailable)?;
        let Some(segment) = state.segments.get(&first).cloned() else {
            return Ok(None);
        };
        let metadata_bytes = segment
            .retained_charge_bound()
            .checked_add(segment.key_ownership_bytes)
            .ok_or_else(unavailable)?;
        let ownership_bytes = state
            .metadata_ownership_bytes
            .checked_sub(metadata_bytes)
            .ok_or_else(corrupt)?;
        for entry in segment.manifest.entries() {
            let index = state.index_mut(entry.kind());
            if index.get(entry.exact_key()) != Some(&entry.locator()) {
                return Err(corrupt());
            }
            *index = index.try_remove(entry.exact_key())?;
        }
        state.segments = state.segments.try_remove(&first)?;
        state.metadata_ownership_bytes = ownership_bytes;
        Ok(Some(segment))
    }
}

impl LocatorGenerationState {
    fn index_mut(&mut self, kind: CommandDerivedIndexKindV1) -> &mut ExactLocatorTree {
        match kind {
            CommandDerivedIndexKindV1::Idempotency => &mut self.idempotency,
            CommandDerivedIndexKindV1::Provenance => &mut self.provenance,
            CommandDerivedIndexKindV1::AuditSequence => &mut self.audit_sequence,
            CommandDerivedIndexKindV1::AuditRequest => &mut self.audit_request,
            CommandDerivedIndexKindV1::EventRoute => &mut self.event_route,
            CommandDerivedIndexKindV1::PendingOutbox => &mut self.pending_outbox,
        }
    }
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
    /// At most sixteen exact Delivering-to-Delivered transitions committed
    /// together under the sole writer's mutation lease.
    OutboxSucceeded(Vec<EventId>),
    /// At most sixteen exact pending-to-Delivering claims committed together
    /// under the sole writer's mutation lease (ADR-0284).
    OutboxClaimed(Vec<EventId>),
    /// A published command segment, with the exact canonical `COMMITS` row
    /// bytes the writer encoded when it has them. Those bytes seed the
    /// byte-checked payload cache, so the next writer's tail lookup does not
    /// re-decode a segment it just built.
    CommandSegmentPublished(Arc<StoredCommandSegmentV1>, Option<Arc<[u8]>>),
    PendingOutboxMembership {
        event_id: EventId,
        was_pending: bool,
        pending: bool,
        was_undelivered: bool,
        undelivered: bool,
    },
    /// ADR-0182 online retention removed every command row through `through`,
    /// including these whole segments. Their derived locators go with them;
    /// the retained views the prune materialized answer those lookups.
    HistoryPruned {
        through: CommitSequence,
        segments: Vec<Arc<StoredCommandSegmentV1>>,
    },
}

impl TransientIndexDelta {
    pub(crate) fn command_segment(&self) -> Option<&Arc<StoredCommandSegmentV1>> {
        match self {
            Self::CommandSegmentPublished(segment, _) => Some(segment),
            Self::PendingOutboxInserted(_)
            | Self::OutboxSucceeded(_)
            | Self::OutboxClaimed(_)
            | Self::PendingOutboxMembership { .. }
            | Self::HistoryPruned { .. } => None,
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
        let Self::CommandSegmentPublished(segment, _) = self else {
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
}

impl TransientIndexes {
    pub(crate) fn try_empty_with_owner(
        owner: LocatorOwner,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            event_routes: Some(BTreeMap::new()),
            event_route_failure: None,
            outbox_intents: Some(BTreeSet::new()),
            pending_outbox: Some(BTreeSet::new()),
            undelivered_outbox: Some(BTreeSet::new()),
            command_derived: Some(CommandDerivedIndexes {
                generation: LocatorGeneration::try_empty_with_owner(owner, class)?,
                payloads: PayloadCache::default(),
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn try_empty(class: LocatorAllocationClass) -> Result<Self, StorageError> {
        Ok(Self {
            event_routes: Some(BTreeMap::new()),
            event_route_failure: None,
            outbox_intents: Some(BTreeSet::new()),
            pending_outbox: Some(BTreeSet::new()),
            undelivered_outbox: Some(BTreeSet::new()),
            command_derived: Some(CommandDerivedIndexes::try_empty(class)?),
        })
    }
    pub(crate) fn locator_generation(&self) -> Result<LocatorGeneration, StorageError> {
        Ok(self
            .command_derived
            .as_ref()
            .ok_or_else(unavailable)?
            .generation
            .clone())
    }
    pub(crate) fn prepare_locator_delta(
        &self,
        delta: &TransientIndexDelta,
        class: LocatorAllocationClass,
    ) -> Result<LocatorGeneration, StorageError> {
        self.command_derived
            .as_ref()
            .ok_or_else(unavailable)?
            .generation
            .prepare_delta(delta, class)
    }
    pub(crate) fn install_locator_generation(&mut self, generation: LocatorGeneration) {
        if let Some(indexes) = self.command_derived.as_mut() {
            indexes.generation = generation;
        } else {
            self.command_derived = Some(CommandDerivedIndexes {
                generation,
                payloads: PayloadCache::default(),
            });
        }
    }

    pub(crate) fn rebuild_from_access_with_owner(
        access: &crate::store::RedbReadAccess,
        owner: LocatorOwner,
        class: LocatorAllocationClass,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<(Self, u64), StorageError> {
        let (generation, rows) =
            LocatorGeneration::rebuild_captured_counted(access, owner, class, &mut cancelled)?;
        let _scratch = NativeLocatorPool::global().admit(16 * 1024, class)?;
        let mut routes = BTreeMap::new();
        let mut intents = BTreeSet::new();
        let mut pending = BTreeSet::new();
        let mut undelivered = BTreeSet::new();
        for (_, segment) in generation.segments.iter() {
            if cancelled() {
                return Err(unavailable());
            }
            for (id, (partition, event)) in &segment.events {
                if routes
                    .insert(encode_event_route_key(*partition, *id).to_vec(), *event)
                    .is_some()
                    || !intents.insert(*id)
                    || !pending.insert(*id)
                    || !undelivered.insert(*id)
                {
                    return Err(corrupt());
                }
            }
        }
        let mut decoded_route_failure = None;
        let route_scan = visit_captured_locator_rows(
            access,
            crate::journal::JournalTable::EventRoutes,
            &mut cancelled,
            |key, bytes| {
                let decoded = (|| {
                    let (partition, id) =
                        crate::keys::decode_event_route_key(key).map_err(|_| corrupt())?;
                    let route = decode_event_route_v1(bytes)?.into_parts().0;
                    if route.event_id() != id {
                        return Err(corrupt());
                    }
                    let encoded = encode_event_route_key(partition, id).to_vec();
                    if routes
                        .get(&encoded)
                        .is_some_and(|previous| previous != &route)
                    {
                        return Err(corrupt());
                    }
                    Ok((encoded, route))
                })();
                match decoded {
                    Ok((encoded, route)) => {
                        routes.insert(encoded, route);
                        Ok(())
                    }
                    Err(error) => {
                        // Only decoded ancillary-record corruption is isolated.
                        // A source/page traversal error never sets this witness.
                        decoded_route_failure = Some(error.kind());
                        Err(error)
                    }
                }
            },
        );
        let (event_routes, event_route_failure) = match route_scan {
            Ok(()) => (Some(routes), None),
            Err(error)
                if error.kind() == StorageErrorKind::CorruptData
                    && decoded_route_failure == Some(StorageErrorKind::CorruptData) =>
            {
                // Drop the whole private accelerator. The generation above was
                // independently validated against canonical command authority;
                // no route-prefix result may establish completeness or absence.
                (None, Some(error.kind()))
            }
            Err(error) => return Err(error),
        };
        visit_captured_locator_rows(
            access,
            crate::journal::JournalTable::Outbox,
            &mut cancelled,
            |key, _bytes| {
                let id = decode_event_key(key).map_err(|_| corrupt())?;
                // Membership accepts both supported standalone intent encodings.
                // Ordinary authority reads and scrub perform their full decode.
                intents.insert(id);
                pending.insert(id);
                undelivered.insert(id);
                Ok(())
            },
        )?;
        let mut outbox_valid = true;
        visit_captured_locator_rows(
            access,
            crate::journal::JournalTable::OutboxStatus,
            &mut cancelled,
            |key, bytes| {
                let Ok(id) = decode_event_key(key) else {
                    outbox_valid = false;
                    return Ok(());
                };
                let Ok(status) = decode_outbox_status_v1(bytes) else {
                    outbox_valid = false;
                    return Ok(());
                };
                let status = status.into_parts().0;
                if status.event_id() != id || !intents.contains(&id) {
                    outbox_valid = false;
                    return Ok(());
                }
                if !status.state().is_pending() {
                    pending.remove(&id);
                }
                if matches!(status.state(), OutboxDeliveryStateV1::Delivered { .. }) {
                    undelivered.remove(&id);
                }
                Ok(())
            },
        )?;
        if cancelled() {
            return Err(unavailable());
        }
        Ok((
            Self {
                event_routes,
                event_route_failure,
                outbox_intents: outbox_valid.then_some(intents),
                pending_outbox: outbox_valid.then_some(pending),
                undelivered_outbox: outbox_valid.then_some(undelivered),
                command_derived: Some(CommandDerivedIndexes {
                    generation,
                    payloads: PayloadCache::default(),
                }),
            },
            rows,
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
            event_route_failure: None,
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

    /// Cardinality of commit-coupled undelivered membership, without a scan.
    /// Missing/degraded membership remains unknown rather than zero.
    pub(crate) fn undelivered_outbox_count(&self) -> Option<u64> {
        u64::try_from(self.undelivered_outbox.as_ref()?.len()).ok()
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
        if let Some(kind) = self.event_route_failure {
            return Some(Err(StorageError::new(kind, None)));
        }
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

    #[cfg(test)]
    pub(crate) fn command_derived_locator(
        &self,
        kind: CommandDerivedIndexKindV1,
        exact_key: &[u8],
    ) -> Result<Option<CommandDerivedLocator>, StorageError> {
        let indexes = self.command_derived.as_ref().ok_or_else(corrupt)?;
        Ok(indexes.index(kind).get(exact_key).copied())
    }

    #[cfg(test)]
    pub(crate) fn capture_command_at(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<CapturedCommandPayload>, StorageError> {
        let indexes = self.command_derived.as_ref().ok_or_else(corrupt)?;
        let Some((_, metadata)) = indexes.segments.predecessor(&sequence) else {
            return Ok(None);
        };
        if sequence > metadata.last {
            return Ok(None);
        }
        indexes.payloads.capture(metadata.clone()).map(Some)
    }

    pub(crate) fn capture_payload_candidate(
        &self,
        payload: CapturedCommandPayload,
    ) -> Result<CapturedCommandPayload, StorageError> {
        match self.command_derived.as_ref() {
            Some(indexes) => indexes.payloads.capture(payload.metadata),
            None => Ok(payload),
        }
    }

    pub(crate) fn retain_resolved_payload(
        &self,
        resolved: &mut ResolvedCommandPayload,
    ) -> Result<(), StorageError> {
        let Some(indexes) = &self.command_derived else {
            return Ok(());
        };
        if let Some(metadata) = indexes
            .segments
            .get(&resolved.segment.first_commit_sequence())
        {
            indexes.payloads.retain_resolved(metadata, resolved)?;
        }
        Ok(())
    }

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
    pub(crate) fn command_segment_tail(
        &self,
    ) -> Option<Option<(CommitSequence, CommandSegmentDigestV1)>> {
        Some(
            self.command_derived
                .as_ref()?
                .segments
                .last()
                .map(|(_, segment)| (segment.first, segment.digest)),
        )
    }

    pub(crate) fn command_segment_coverage(&self) -> Option<Option<CommitSequence>> {
        Some(
            self.command_derived
                .as_ref()?
                .segments
                .last()
                .map(|(_, segment)| segment.last),
        )
    }

    #[cfg(test)]
    pub(crate) fn command_at(
        &self,
        sequence: CommitSequence,
        load: impl FnOnce(CommitSequence) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Option<StoredCommandCapsuleV2>, StorageError> {
        let indexes = self.command_derived.as_ref().ok_or_else(corrupt)?;
        let Some((_, metadata)) = indexes.segments.predecessor(&sequence) else {
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

    /// Compatibility helper for tests. Live publication
    /// prepares this successor before durability, then installs it atomically
    /// with the matching source and calls only `apply_disposable_delta`.
    #[cfg(test)]
    pub(crate) fn apply(&mut self, delta: TransientIndexDelta) {
        let class = if matches!(&delta, TransientIndexDelta::HistoryPruned { .. }) {
            LocatorAllocationClass::Maintenance
        } else {
            LocatorAllocationClass::Ordinary
        };
        match self.prepare_locator_delta(&delta, class) {
            Ok(generation) => self.install_locator_generation(generation),
            Err(_) => {
                self.command_derived = None;
                self.invalidate_outbox();
                return;
            }
        }
        self.apply_disposable_delta(delta);
    }

    pub(crate) fn apply_disposable_delta(&mut self, delta: TransientIndexDelta) {
        match delta {
            TransientIndexDelta::OutboxSucceeded(events) => {
                for event_id in events {
                    self.apply_disposable_delta(TransientIndexDelta::PendingOutboxMembership {
                        event_id,
                        was_pending: false,
                        pending: false,
                        was_undelivered: true,
                        undelivered: false,
                    });
                }
            }
            TransientIndexDelta::OutboxClaimed(events) => {
                for event_id in events {
                    self.apply_disposable_delta(TransientIndexDelta::PendingOutboxMembership {
                        event_id,
                        was_pending: true,
                        pending: false,
                        was_undelivered: true,
                        undelivered: true,
                    });
                }
            }
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
            TransientIndexDelta::CommandSegmentPublished(segment, canonical) => {
                let Some(command_derived) = self.command_derived.as_mut() else {
                    return;
                };
                if let Some(canonical) = canonical {
                    // A cache admission failure never invalidates the complete
                    // immutable locator directory already published with source.
                    let _ = command_derived.seed_payload(&segment, canonical);
                }
                // Route degradation must not leave otherwise valid outbox
                // membership stale when a later command publishes more events.
                if let Some(routes) = self.event_routes.as_mut() {
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
                                self.invalidate_outbox();
                                return;
                            }
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
            TransientIndexDelta::HistoryPruned { through, segments } => {
                self.prune_history(through, &segments);
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

    fn prune_history(&mut self, through: CommitSequence, segments: &[Arc<StoredCommandSegmentV1>]) {
        if let Some(command_derived) = self.command_derived.as_mut() {
            for segment in segments {
                let _ = command_derived
                    .payloads
                    .remove(segment.first_commit_sequence());
            }
        }
        let Some(after) = through.checked_next() else {
            self.invalidate_outbox();
            return;
        };
        let boundary = EventId::new(after, 0);
        for set in [
            self.outbox_intents.as_mut(),
            self.pending_outbox.as_mut(),
            self.undelivered_outbox.as_mut(),
        ]
        .into_iter()
        .flatten()
        {
            *set = set.split_off(&boundary);
        }
    }

    fn invalidate_outbox(&mut self) {
        self.outbox_intents = None;
        self.pending_outbox = None;
        self.undelivered_outbox = None;
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

pub(crate) fn command_audit_from_locator(
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
            event_route_failure: None,
            outbox_intents: Some(BTreeSet::new()),
            pending_outbox: Some(BTreeSet::new()),
            undelivered_outbox: Some(BTreeSet::new()),
            command_derived: CommandDerivedIndexes::try_empty(LocatorAllocationClass::Ordinary)
                .ok(),
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
        for segment in command_derived.segments.iter().map(|(_, segment)| segment) {
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
    fn try_empty(class: LocatorAllocationClass) -> Result<Self, StorageError> {
        Ok(Self {
            generation: LocatorGeneration::try_empty(class)?,
            payloads: PayloadCache::default(),
        })
    }
    fn event_ids(&self) -> impl Iterator<Item = EventId> + '_ {
        self.segments
            .iter()
            .flat_map(|(_, segment)| segment.events.keys().copied())
    }
    fn seed_payload(
        &mut self,
        segment: &Arc<StoredCommandSegmentV1>,
        canonical: Arc<[u8]>,
    ) -> Result<(), StorageError> {
        let metadata = self
            .segments
            .get(&segment.first_commit_sequence())
            .ok_or_else(corrupt)?;
        self.payloads.seed(metadata, Arc::clone(segment), canonical)
    }
}

pub(crate) fn expected_manifest_key(
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

fn rebuild_command_derived_indexes(
    transaction: &ReadTransaction,
    cancellation: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Option<CommandDerivedIndexes>, StorageError> {
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let mut indexes = CommandDerivedIndexes::try_empty(LocatorAllocationClass::Ordinary)?;
    for entry in commits.iter().map_err(precommit_storage_error)? {
        check_rebuild_cancellation(cancellation)?;
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if let Some(segment) = decode_locator_construction_segment(value.value(), None)? {
            let physical = decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;
            if physical != segment.first_commit_sequence() {
                return Err(corrupt());
            }
            indexes.generation.insert_segment(&segment)?;
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
        let Ok(event_id) = decode_event_key(key.value()) else {
            return Ok(RebuiltOutboxIndexes {
                intents: None,
                pending: None,
                undelivered: None,
            });
        };
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

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

struct LocatorDecodeScratchOwner(Option<crate::locator_memory::LocatorCharge>);

impl Drop for LocatorDecodeScratchOwner {
    fn drop(&mut self) {
        if !riffdb_proto::envelope::release_locator_decode_scratch_v1() {
            // A still-borrowed buffer cannot outlive its accounting. This is an
            // internal ownership violation: conservatively retain the charge,
            // bounded by the global ceiling, rather than grant unsafe capacity.
            if let Some(charge) = self.0.take() {
                std::mem::forget(charge);
            }
        }
    }
}

struct AdmittedLocatorSource<'bytes> {
    bytes: std::borrow::Cow<'bytes, [u8]>,
    _work: crate::locator_memory::LocatorCharge,
}

struct AdmittedLocatorSegment {
    // Fields drop in declaration order: retain the work charge until every
    // decoded semantic value has been released after metadata construction.
    segment: StoredCommandSegmentV1,
    _work: LocatorDecodeScratchOwner,
}

impl std::ops::Deref for AdmittedLocatorSegment {
    type Target = StoredCommandSegmentV1;
    fn deref(&self) -> &Self::Target {
        &self.segment
    }
}

impl std::fmt::Debug for AdmittedLocatorSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmittedLocatorSegment")
            .finish_non_exhaustive()
    }
}

fn decode_locator_construction_segment(
    bytes: &[u8],
    access: Option<&crate::store::RedbReadAccess>,
) -> Result<Option<AdmittedLocatorSegment>, StorageError> {
    decode_native_locator_construction_segment(
        bytes,
        access,
        LocatorAllocationClass::Ordinary,
        Some(crate::startup::MAX_RECOVERY_OWNED_STATE_BYTES),
    )
}

fn decode_native_locator_construction_segment(
    bytes: &[u8],
    access: Option<&crate::store::RedbReadAccess>,
    class: LocatorAllocationClass,
    recovery_window: Option<usize>,
) -> Result<Option<AdmittedLocatorSegment>, StorageError> {
    let pool = NativeLocatorPool::global();
    decode_native_locator_construction_segment_in_pool(&pool, bytes, access, class, recovery_window)
}

fn decode_native_locator_construction_segment_in_pool(
    pool: &NativeLocatorPool,
    bytes: &[u8],
    access: Option<&crate::store::RedbReadAccess>,
    class: LocatorAllocationClass,
    recovery_window: Option<usize>,
) -> Result<Option<AdmittedLocatorSegment>, StorageError> {
    // Adopt and clear any previous TLS scratch before computing this row's
    // allowance. Its destructor order is independent of TLS registration order.
    let mut owner = LocatorDecodeScratchOwner(Some(pool.admit(
        riffdb_proto::envelope::MAX_STORED_ENVELOPE_BYTES + 256,
        class,
    )?));
    if !riffdb_proto::envelope::release_locator_decode_scratch_v1() {
        return Err(unavailable());
    }
    drop(owner.0.take());
    // A frame mutation has a 44-byte header. Sixteen times padded length covers
    // physical/canonical copies, fourfold descriptor capacity, allocation
    // headers and the resolved segment; the inventory has a fixed extra window.
    let frame_work = match crate::history_stub::HistoryStub::decode(bytes)? {
        Some(stub) => {
            if stub.kind != crate::history_stub::StubKind::Segment {
                return Err(corrupt());
            }
            let needed = stub
                .placement
                .padded_len
                .checked_mul(16)
                .and_then(|bytes| bytes.checked_add(1024 * 1024))
                .ok_or_else(unavailable)?;
            if recovery_window.is_some_and(|window| needed > window) {
                return Err(unavailable());
            }
            Some(pool.admit(needed, class)?)
        }
        None => None,
    };
    let resolved = match access {
        Some(access) => access.resolve_command_segment_value(bytes)?,
        None => crate::history_stub::resolve_commits_value(bytes)?,
    };
    // Transfer the surviving copied segment out of frame-phase ownership only
    // after admitting its capacity. All source frame buffers have now died.
    let resolved_bytes = match &resolved {
        std::borrow::Cow::Owned(bytes) => {
            bytes.capacity().checked_add(64).ok_or_else(unavailable)?
        }
        std::borrow::Cow::Borrowed(_) => 0,
    };
    let resolved_work = pool.admit(resolved_bytes, class)?;
    let resolved = AdmittedLocatorSource {
        bytes: resolved,
        _work: resolved_work,
    };
    drop(frame_work);
    let work = riffdb_storage_api::locator_segment_decode_work_bound_v1(&resolved.bytes).map_err(
        |error| {
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::LimitExceeded {
                unavailable()
            } else {
                corrupt()
            }
        },
    )?;
    let Some(work) = work else {
        return Ok(None);
    };
    let work = usize::try_from(work).map_err(|_| unavailable())?;
    let peak = work.checked_add(resolved_bytes).ok_or_else(unavailable)?;
    if recovery_window.is_some_and(|window| peak > window) {
        return Err(unavailable());
    }
    owner.0 = Some(pool.admit(work, class)?);
    #[cfg(test)]
    LOCATOR_CONSTRUCTION_DECODE_ATTEMPTS.with(|attempts| attempts.set(attempts.get() + 1));
    let decoded = crate::history_stub::decode_retained_segment(&resolved.bytes)
        .map_err(|_| corrupt())?
        .into_parts()
        .0;
    Ok(Some(AdmittedLocatorSegment {
        segment: decoded,
        _work: owner,
    }))
}

fn visit_captured_locator_rows(
    access: &crate::store::RedbReadAccess,
    table: crate::journal::JournalTable,
    cancelled: &mut impl FnMut() -> bool,
    mut visit: impl FnMut(&[u8], &[u8]) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let mut after: Option<Vec<u8>> = None;
    loop {
        if cancelled() {
            return Err(unavailable());
        }
        let next = access.visit_next_locator_row(table, after.as_deref(), |key, bytes| {
            if key.len() > riffdb_types::MAX_KEY_BYTES {
                return Err(corrupt());
            }
            visit(key, bytes)?;
            Ok(key.to_vec())
        })?;
        let Some(next) = next else {
            return Ok(());
        };
        after = Some(next);
    }
}

#[cfg(test)]
#[path = "transient_seed_admission_tests.rs"]
mod seed_admission_tests;

#[cfg(test)]
#[path = "transient_provider_sharing_tests.rs"]
mod provider_sharing_tests;

#[cfg(test)]
mod tests {
    use riffdb_types::CommitSequence;

    use super::*;

    // req: TXN-042, STO-032, REC-001
    #[test]
    fn immutable_locator_generation_retirement_preserves_original_exact_membership() {
        let command = crate::derived::follower_indexes::capsule(CommitSequence::first());
        let (segment, encoded) =
            crate::derived::follower_indexes::segment(vec![command.clone()], None);
        let segment = Arc::new(segment);
        let original = LocatorGeneration::rebuild_from_segments(
            [Ok(Arc::clone(&segment))],
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        let next = original
            .prepare_delta(
                &TransientIndexDelta::HistoryPruned {
                    through: segment.last_commit_sequence(),
                    segments: vec![Arc::clone(&segment)],
                },
                LocatorAllocationClass::Maintenance,
            )
            .unwrap();
        assert_eq!(next.coverage(), None);
        let original_bound = original.retained_ownership_bound().unwrap();
        assert_eq!(
            original.clone().retained_ownership_bound().unwrap(),
            original_bound,
            "sharing a generation shares one immutable descriptor and byte census"
        );
        assert!(
            next.retained_ownership_bound().unwrap() < original_bound,
            "retirement removes metadata, key and tree ownership from the successor"
        );
        for entry in segment.manifest().entries() {
            assert!(
                next.capture_command_member(entry.kind(), entry.exact_key())
                    .unwrap()
                    .is_none()
            );
            let (payload, locator) = original
                .capture_command_member(entry.kind(), entry.exact_key())
                .unwrap()
                .expect("captured generation keeps every original exact member");
            assert_eq!(locator.command_ordinal, entry.command_ordinal());
            let resolved = payload
                .resolve(|first| {
                    assert_eq!(first, segment.first_commit_sequence());
                    Ok(Some(encoded.as_bytes().to_vec()))
                })
                .unwrap();
            assert_eq!(resolved.segment.commands(), std::slice::from_ref(&command));
        }
        // Cancellation discards a separately prepared successor, never mutates
        // the original, and leaves no partial directory masquerading as empty.
        assert!(
            original
                .prepare_delta(
                    &TransientIndexDelta::CommandSegmentPublished(Arc::clone(&segment), None),
                    LocatorAllocationClass::Ordinary,
                )
                .is_err()
        );
        assert_eq!(original.coverage(), Some(segment.last_commit_sequence()));
    }

    // req: TXN-042, STO-032, OQ-040
    #[test]
    fn provider_families_share_one_generation_charge_through_native_retirement() {
        use riffdb_types::exact_provider_budget_v1::ProviderBudget;
        let pool = NativeLocatorPool::isolated_for_tests();
        let command = crate::derived::follower_indexes::capsule(CommitSequence::first());
        let (segment, _) = crate::derived::follower_indexes::segment(vec![command], None);
        let mut generation = LocatorGeneration::try_empty_in_pool(
            Arc::clone(&pool),
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        generation.insert_segment(&segment).unwrap();
        let budget = ProviderBudget::new();
        let barrier = Arc::new(std::sync::Barrier::new(9));
        let joins: Vec<_> = (0..8)
            .map(|_| {
                let generation = generation.clone();
                let budget = budget.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    generation.admit_provider_ownership(&budget).unwrap()
                })
            })
            .collect();
        barrier.wait();
        let owners: Vec<_> = joins.into_iter().map(|join| join.join().unwrap()).collect();
        assert!(owners.iter().all(|owner| Arc::ptr_eq(owner, &owners[0])));
        let bound = budget.observation().unwrap().owned_bytes;
        assert!(bound > owners[0].bytes());
        let foreign = ProviderBudget::new();
        assert_eq!(
            generation
                .provider_ownership_needed(&foreign)
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(foreign.observation().unwrap().owned_bytes, 0);
        drop(owners);
        let protected = generation.clone();
        drop(generation);
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            bound,
            "discovery removal cannot release ownership retained by a snapshot"
        );
        drop(protected);
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            bound,
            "queued physical native ownership keeps its provider charge"
        );
        while pool.drain_retired_step() {}
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    // req: TXN-042, STO-032, REC-001
    #[test]
    fn locator_construction_preflight_counts_dense_canonical_values_before_decoding() {
        use riffdb_storage_api::{StoredCommandCapsuleV1, StoredCommandCapsuleV2};
        use riffdb_types::{CanonicalList, CanonicalRecord, CanonicalValue, FieldId};
        let original = crate::derived::follower_indexes::capsule(CommitSequence::first());
        let (_, sparse) = crate::derived::follower_indexes::segment(vec![original.clone()], None);
        let values = CanonicalRecord::new(vec![(
            FieldId::first(),
            CanonicalValue::List(CanonicalList::new(vec![CanonicalValue::Null; 4096]).unwrap()),
        )])
        .unwrap();
        let base = original.base();
        let outcome = base
            .outcome()
            .clone()
            .with_service_values(values.clone())
            .unwrap();
        let base = StoredCommandCapsuleV1::new(
            outcome,
            base.provenance().clone(),
            base.commit().clone(),
            base.started_audit().clone(),
            base.terminal_audit().clone(),
        )
        .unwrap();
        let dense = StoredCommandCapsuleV2::from_base(base, vec![]).unwrap();
        let (_, dense) = crate::derived::follower_indexes::segment(vec![dense], None);
        let sparse_work =
            riffdb_storage_api::locator_segment_decode_work_bound_v1(sparse.as_bytes())
                .unwrap()
                .unwrap();
        let dense_work = riffdb_storage_api::locator_segment_decode_work_bound_v1(dense.as_bytes())
            .unwrap()
            .unwrap();
        assert!(
            dense_work - sparse_work >= 4096 * std::mem::size_of::<CanonicalValue>() as u64,
            "one-byte canonical values require their actual enum/vector ownership"
        );
        let decoded = decode_locator_construction_segment(dense.as_bytes(), None)
            .unwrap()
            .unwrap();
        assert_eq!(
            decoded.commands()[0].base().outcome().service_values(),
            &values
        );
        let mut corrupt = dense.as_bytes().to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert_eq!(
            decode_locator_construction_segment(&corrupt, None)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );
    }

    // req: TXN-042, STO-032
    #[test]
    fn missing_locator_directory_never_proves_an_empty_command_set() {
        let mut indexes = TransientIndexes::try_empty(LocatorAllocationClass::Ordinary).unwrap();
        indexes.command_derived = None;
        assert_eq!(
            indexes.locator_generation().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        assert!(indexes.capture_command_at(CommitSequence::first()).is_err());
        assert!(
            indexes
                .command_derived_locator(CommandDerivedIndexKindV1::AuditSequence, &[0; 8])
                .is_err()
        );
    }

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
    // req: TXN-042, STO-032, REC-001
    #[test]
    fn legal_dense_segment_refuses_recovery_window_before_decode_but_fits_native_admission() {
        use riffdb_storage_api::{StoredCommandCapsuleV1, StoredCommandCapsuleV2};
        use riffdb_types::{
            CanonicalList, CanonicalRecord, CanonicalValue, FieldId, MAX_LIST_ENTRIES,
        };
        let original = crate::derived::follower_indexes::capsule(CommitSequence::first());
        let values = CanonicalRecord::new(vec![
            (
                FieldId::first(),
                CanonicalValue::List(
                    CanonicalList::new(vec![CanonicalValue::Null; MAX_LIST_ENTRIES]).unwrap(),
                ),
            ),
            (
                FieldId::new(2).unwrap(),
                CanonicalValue::List(
                    CanonicalList::new(vec![CanonicalValue::Null; MAX_LIST_ENTRIES]).unwrap(),
                ),
            ),
        ])
        .unwrap();
        let base = original.base();
        let outcome = base
            .outcome()
            .clone()
            .with_service_values(values.clone())
            .unwrap();
        let capsule = StoredCommandCapsuleV2::from_base(
            StoredCommandCapsuleV1::new(
                outcome,
                base.provenance().clone(),
                base.commit().clone(),
                base.started_audit().clone(),
                base.terminal_audit().clone(),
            )
            .unwrap(),
            vec![],
        )
        .unwrap();
        let (segment, encoded) = crate::derived::follower_indexes::segment(vec![capsule], None);
        let canonical = crate::history_stub::decode_retained_segment(encoded.as_bytes())
            .unwrap()
            .into_parts()
            .0;
        assert_eq!(
            canonical, segment,
            "the large layout remains a valid durable segment"
        );
        drop(canonical);
        let work = usize::try_from(
            riffdb_storage_api::locator_segment_decode_work_bound_v1(encoded.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(
            work > crate::startup::MAX_RECOVERY_OWNED_STATE_BYTES,
            "two maximum-cardinality canonical lists exceed the unchanged recovery work allowance"
        );
        assert!(
            work < crate::locator_memory::LOCATOR_ORDINARY_BYTES,
            "the legal directory construction fits the separate ordinary native allowance"
        );
        let pool = NativeLocatorPool::isolated_for_tests();
        // The process-lifetime retirement directory is real pool-owned
        // capacity, admitted before the first reconstruction allocation.
        let baseline = pool.charged_bytes();
        let before = LOCATOR_CONSTRUCTION_DECODE_ATTEMPTS.with(std::cell::Cell::get);
        let refused = decode_native_locator_construction_segment_in_pool(
            &pool,
            encoded.as_bytes(),
            None,
            LocatorAllocationClass::Ordinary,
            Some(crate::startup::MAX_RECOVERY_OWNED_STATE_BYTES),
        )
        .unwrap_err();
        assert_eq!(refused.kind(), StorageErrorKind::Unavailable);
        assert_eq!(
            LOCATOR_CONSTRUCTION_DECODE_ATTEMPTS.with(std::cell::Cell::get),
            before
        );
        assert_eq!(pool.charged_bytes(), baseline);
        let pressure = pool.exhaust_ordinary_for_tests();
        let refused = decode_native_locator_construction_segment_in_pool(
            &pool,
            encoded.as_bytes(),
            None,
            LocatorAllocationClass::Ordinary,
            None,
        )
        .unwrap_err();
        assert_eq!(refused.kind(), StorageErrorKind::Unavailable);
        assert_eq!(
            LOCATOR_CONSTRUCTION_DECODE_ATTEMPTS.with(std::cell::Cell::get),
            before
        );
        drop(pressure);
        let decoded = decode_native_locator_construction_segment_in_pool(
            &pool,
            encoded.as_bytes(),
            None,
            LocatorAllocationClass::Ordinary,
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            LOCATOR_CONSTRUCTION_DECODE_ATTEMPTS.with(std::cell::Cell::get),
            before + 1
        );
        assert_eq!(
            decoded.commands()[0].base().outcome().service_values(),
            &values
        );
        assert!(
            pool.charged_bytes() >= baseline + work,
            "the semantic graph remains owned by its admitted scratch guard"
        );
        drop(decoded);
        assert_eq!(
            pool.charged_bytes(),
            baseline,
            "decoded graph and TLS scratch die before numerical release, while the physical retirement directory stays charged"
        );
        println!(
            "locator-legal-dense-segment-v1 lists=2 entries_per_list={MAX_LIST_ENTRIES} wire_bytes={} admitted_work={work} recovery_window={}",
            encoded.as_bytes().len(),
            crate::startup::MAX_RECOVERY_OWNED_STATE_BYTES
        );
    }
}
