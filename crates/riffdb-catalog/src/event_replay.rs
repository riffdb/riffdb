//! Bounded symbolic replay over partition-local authoritative event routes.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use riffdb_query_module::{
    CompiledReactiveOperationV1, ReactiveOperationPlanV1, ReactivePredicateNodeV1,
};
use riffdb_storage_api::{
    AuthoritativePointReader, EventRouteContinuationV1, EventRoutePageLimit,
    EventRouteScanRequestV1, EventRouteUpperFenceV1, PartitionEventRouteReader,
    PolicyAuthorizedEventReplayItemV1, StorageErrorKind, StoredCommitRecordV1,
};
use riffdb_types::{
    ActorKind, CanonicalValue, ContractVersion, EventId, EventTypeId, PartitionKey,
    PartitionKeyHash, PlanHash, ProvenanceId, RequestId, Timestamp, decode_canonical_value,
    hash_partition_key,
};

use crate::{
    ActiveCatalogSnapshot, EventMaterializationErrorKind, ResolvedEventMaterializer,
    SymbolicEventView,
};

/// Closed safe classification for symbolic replay failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventReplayErrorKind {
    /// Symbol resolution, historical materialization, or partition proof failed.
    Materialization(EventMaterializationErrorKind),
    /// A bounded storage observation failed with its safe closed class.
    Storage(StorageErrorKind),
    /// Cross-record route, event, and commit evidence did not agree.
    Integrity,
}

/// Redaction-safe symbolic replay failure.
#[derive(Clone, Eq, PartialEq)]
pub struct EventReplayError {
    kind: EventReplayErrorKind,
}

impl EventReplayError {
    const fn integrity() -> Self {
        Self {
            kind: EventReplayErrorKind::Integrity,
        }
    }

    /// Returns the stable closed failure classification.
    #[must_use]
    pub const fn kind(&self) -> EventReplayErrorKind {
        self.kind
    }
}

impl fmt::Debug for EventReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventReplayError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for EventReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            EventReplayErrorKind::Materialization(kind) => formatter.write_str(kind.safe_message()),
            EventReplayErrorKind::Storage(kind) => formatter.write_str(kind.safe_message()),
            EventReplayErrorKind::Integrity => {
                formatter.write_str("event replay integrity failure")
            }
        }
    }
}

impl Error for EventReplayError {}

impl From<crate::EventMaterializationError> for EventReplayError {
    fn from(error: crate::EventMaterializationError) -> Self {
        Self {
            kind: EventReplayErrorKind::Materialization(error.kind()),
        }
    }
}

/// Internal replay position retained behind the public opaque cursor boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventReplayPosition {
    /// Start a newly upper-fenced replay after an optional prior event.
    Initial {
        /// Last event already held by the caller, when resuming initial replay.
        after: Option<EventId>,
    },
    /// Continue the exact prior frozen route scan.
    Continue(EventRouteContinuationV1),
}

/// One resolved event symbol, exact symbolic partition, and field selection.
pub struct ResolvedEventReplay {
    materializer: ResolvedEventMaterializer,
    partition_hash: PartitionKeyHash,
}

/// One exact immutable reactive stream resolved to a single partition-route scan.
pub struct ResolvedReactiveEventStream {
    materializers: Vec<ResolvedEventMaterializer>,
    partition_key: PartitionKey,
    partition_hash: PartitionKeyHash,
    parameters: BTreeMap<String, CanonicalValue>,
    predicate: Vec<ReactivePredicateNodeV1>,
}

impl ResolvedReactiveEventStream {
    /// Returns the complete policy-visible partition key.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Returns the storage routing hash derived from that complete key.
    #[must_use]
    pub const fn partition_hash(&self) -> PartitionKeyHash {
        self.partition_hash
    }

    /// Returns the exact active-contract source entities whose row policies
    /// govern this stream's compiler-anchored event types.
    #[doc(hidden)]
    #[must_use]
    pub fn policy_anchor_entities(&self) -> Vec<riffdb_types::EntityTypeId> {
        let mut entities = self
            .materializers
            .iter()
            .filter_map(ResolvedEventMaterializer::policy_anchor_entity)
            .collect::<Vec<_>>();
        entities.sort_unstable();
        entities.dedup();
        entities
    }

    /// Reads one bounded route page and materializes every selected event type in route order.
    pub fn replay_page<R>(
        &self,
        reader: &R,
        position: EventReplayPosition,
        limit: EventRoutePageLimit,
        history_incarnation: u64,
    ) -> Result<SymbolicEventReplayPage, EventReplayError>
    where
        R: PartitionEventRouteReader + AuthoritativePointReader,
    {
        let request = match position {
            EventReplayPosition::Initial { after } => {
                EventRouteScanRequestV1::initial(self.partition_hash, after, limit)
            }
            EventReplayPosition::Continue(continuation) => {
                if continuation.partition_hash() != self.partition_hash {
                    return Err(EventReplayError::integrity());
                }
                EventRouteScanRequestV1::continuing(continuation, limit)
            }
        };
        let routes = reader
            .scan_partition_event_routes(request)
            .map_err(|error| EventReplayError {
                kind: EventReplayErrorKind::Storage(error.kind()),
            })?;
        let mut items = Vec::new();
        for encoded_route in routes.items() {
            let route = *encoded_route.value();
            let Some(materializer) = self
                .materializers
                .iter()
                .find(|candidate| candidate.event_type_id() == route.event_type_id())
            else {
                continue;
            };
            let event = reader
                .read_durable_event(route.event_id())
                .map_err(|error| EventReplayError {
                    kind: EventReplayErrorKind::Storage(error.kind()),
                })?
                .ok_or_else(EventReplayError::integrity)?;
            let commit = reader
                .read_commit(route.event_id().commit_sequence())
                .map_err(|error| EventReplayError {
                    kind: EventReplayErrorKind::Storage(error.kind()),
                })?
                .ok_or_else(EventReplayError::integrity)?;
            let (root_request_id, causing_event_id) = read_event_correlation(reader, &commit)?;
            if route.event_id() != event.event_id()
                || route.event_type_id() != event.event_type_id()
                || route.event_hash() != event.event_hash()
                || commit.commit_sequence() != route.event_id().commit_sequence()
                || commit.partition_hash() != self.partition_hash
                || !commit.events().iter().any(|candidate| candidate == &event)
            {
                return Err(EventReplayError::integrity());
            }
            let view = materializer.materialize_routed_event(
                commit.plan(),
                self.partition_hash,
                route,
                &event,
            )?;
            if !predicate_matches(&self.predicate, &self.parameters, &view)? {
                continue;
            }
            items.push(SymbolicEventEnvelope {
                view,
                occurred_at: commit.logical_time().timestamp(),
                request_id: commit.admission_request_id(),
                root_request_id,
                causing_event_id,
                actor_kind: commit.actor().actor_kind(),
                provenance_id: commit.provenance_id(),
                history_incarnation,
                policy_anchor: event.policy_anchor().cloned(),
            });
        }
        Ok(SymbolicEventReplayPage {
            items,
            continuation: routes.continuation(),
            inclusive_upper: routes.inclusive_upper(),
        })
    }
}

impl fmt::Debug for ResolvedReactiveEventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResolvedReactiveEventStream([CHECKED])")
    }
}

impl ResolvedEventReplay {
    /// Returns the exact event symbol without exposing its stable numeric ID.
    #[must_use]
    pub fn event_name(&self) -> &str {
        self.materializer.event_name()
    }

    /// Returns the exact storage route selected by the resolved symbolic
    /// replay. This is a first-party adapter handoff, not an application ID.
    #[doc(hidden)]
    #[must_use]
    pub const fn partition_hash(&self) -> PartitionKeyHash {
        self.partition_hash
    }

    /// Returns the stable event identity selected by the resolved symbolic
    /// replay for the storage-owned candidate filter.
    #[doc(hidden)]
    #[must_use]
    pub fn event_type_id(&self) -> EventTypeId {
        self.materializer.event_type_id()
    }

    /// Materializes one immutable event graph only after storage has made the
    /// transaction-current row-policy decision. Storage owns reciprocal graph
    /// validation; catalog still owns symbolic names, selected fields, and
    /// historical schema interpretation.
    #[doc(hidden)]
    pub fn materialize_policy_authorized_item(
        &self,
        item: PolicyAuthorizedEventReplayItemV1,
        history_incarnation: u64,
    ) -> Result<SymbolicEventEnvelope, EventReplayError> {
        let (route, event, commit, provenance) = item.into_parts();
        if event.event_type_id() != self.materializer.event_type_id() {
            return Err(EventReplayError::integrity());
        }
        let view = self.materializer.materialize_routed_event(
            commit.plan(),
            self.partition_hash,
            route,
            &event,
        )?;
        let (root_request_id, causing_event_id) = match provenance.causation() {
            Some(causation) => (
                causation.root_request_id(),
                Some(causation.causing_event_id()),
            ),
            None => (commit.admission_request_id(), None),
        };
        Ok(SymbolicEventEnvelope {
            view,
            occurred_at: commit.logical_time().timestamp(),
            request_id: commit.admission_request_id(),
            root_request_id,
            causing_event_id,
            actor_kind: commit.actor().actor_kind(),
            provenance_id: commit.provenance_id(),
            history_incarnation,
            policy_anchor: event.policy_anchor().cloned(),
        })
    }

    /// Reads one bounded physical route page and returns only matching events.
    ///
    /// Every route is joined back to its immutable event and originating commit.
    /// Missing or contradictory reciprocal evidence fails the complete page.
    pub fn replay_page<R>(
        &self,
        reader: &R,
        position: EventReplayPosition,
        limit: EventRoutePageLimit,
        history_incarnation: u64,
    ) -> Result<SymbolicEventReplayPage, EventReplayError>
    where
        R: PartitionEventRouteReader + AuthoritativePointReader,
    {
        let request = match position {
            EventReplayPosition::Initial { after } => {
                EventRouteScanRequestV1::initial(self.partition_hash, after, limit)
            }
            EventReplayPosition::Continue(continuation) => {
                if continuation.partition_hash() != self.partition_hash {
                    return Err(EventReplayError::integrity());
                }
                EventRouteScanRequestV1::continuing(continuation, limit)
            }
        };
        let routes = reader
            .scan_partition_event_routes(request)
            .map_err(|error| EventReplayError {
                kind: EventReplayErrorKind::Storage(error.kind()),
            })?;
        let mut items = Vec::new();
        for encoded_route in routes.items() {
            let route = *encoded_route.value();
            let event = reader
                .read_durable_event(route.event_id())
                .map_err(|error| EventReplayError {
                    kind: EventReplayErrorKind::Storage(error.kind()),
                })?
                .ok_or_else(EventReplayError::integrity)?;
            let commit = reader
                .read_commit(route.event_id().commit_sequence())
                .map_err(|error| EventReplayError {
                    kind: EventReplayErrorKind::Storage(error.kind()),
                })?
                .ok_or_else(EventReplayError::integrity)?;
            let (root_request_id, causing_event_id) = read_event_correlation(reader, &commit)?;
            if route.event_id() != event.event_id()
                || route.event_type_id() != event.event_type_id()
                || route.event_hash() != event.event_hash()
                || commit.commit_sequence() != route.event_id().commit_sequence()
                || commit.partition_hash() != self.partition_hash
                || !commit.events().iter().any(|candidate| candidate == &event)
            {
                return Err(EventReplayError::integrity());
            }
            if event.event_type_id() == self.materializer.event_type_id() {
                let view = self.materializer.materialize_routed_event(
                    commit.plan(),
                    self.partition_hash,
                    route,
                    &event,
                )?;
                items.push(SymbolicEventEnvelope {
                    view,
                    occurred_at: commit.logical_time().timestamp(),
                    request_id: commit.admission_request_id(),
                    root_request_id,
                    causing_event_id,
                    actor_kind: commit.actor().actor_kind(),
                    provenance_id: commit.provenance_id(),
                    history_incarnation,
                    policy_anchor: event.policy_anchor().cloned(),
                });
            }
        }
        Ok(SymbolicEventReplayPage {
            items,
            continuation: routes.continuation(),
            inclusive_upper: routes.inclusive_upper(),
        })
    }
}

impl fmt::Debug for ResolvedEventReplay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResolvedEventReplay([CHECKED])")
    }
}

fn read_event_correlation<R>(
    reader: &R,
    commit: &StoredCommitRecordV1,
) -> Result<(RequestId, Option<EventId>), EventReplayError>
where
    R: AuthoritativePointReader,
{
    let provenance = reader
        .read_provenance(commit.provenance_id())
        .map_err(|error| EventReplayError {
            kind: EventReplayErrorKind::Storage(error.kind()),
        })?
        .ok_or_else(EventReplayError::integrity)?;
    let matches_commit = provenance.provenance_id() == commit.provenance_id()
        && provenance.commit_sequence() == commit.commit_sequence()
        && provenance.admission_request_id() == commit.admission_request_id()
        && provenance.plan() == commit.plan()
        && provenance.canonical_input_hash() == commit.canonical_input_hash()
        && provenance.actor() == commit.actor()
        && provenance.logical_time() == commit.logical_time()
        && provenance.partition_hash() == commit.partition_hash()
        && provenance.conflict_hashes() == commit.conflict_hashes()
        && provenance.outcome_id() == commit.declared_outcome().outcome_id()
        && provenance
            .event_ids()
            .iter()
            .copied()
            .eq(commit.events().iter().map(|event| event.event_id()));
    if !matches_commit {
        return Err(EventReplayError::integrity());
    }
    Ok(match provenance.causation() {
        Some(causation) => (
            causation.root_request_id(),
            Some(causation.causing_event_id()),
        ),
        None => (commit.admission_request_id(), None),
    })
}

/// One bounded page of symbolic matching events from a frozen route scan.
pub struct SymbolicEventReplayPage {
    items: Vec<SymbolicEventEnvelope>,
    continuation: Option<EventRouteContinuationV1>,
    inclusive_upper: EventRouteUpperFenceV1,
}

impl SymbolicEventReplayPage {
    /// Reconstitutes one page after a lower authoritative policy filter.
    ///
    /// The caller must preserve the original frozen route continuation and
    /// upper fence. This constructor is intentionally hidden from application
    /// APIs; it exists only for first-party storage adapters that filter a
    /// page inside the same authoritative snapshot that supplied its events.
    #[doc(hidden)]
    #[must_use]
    pub fn from_policy_filtered_parts(
        items: Vec<SymbolicEventEnvelope>,
        continuation: Option<EventRouteContinuationV1>,
        inclusive_upper: EventRouteUpperFenceV1,
    ) -> Self {
        Self {
            items,
            continuation,
            inclusive_upper,
        }
    }

    /// Borrows matching symbolic events in exact partition order.
    #[must_use]
    pub fn items(&self) -> &[SymbolicEventEnvelope] {
        &self.items
    }

    /// Consumes the page into matching symbolic events.
    #[must_use]
    pub fn into_items(self) -> Vec<SymbolicEventEnvelope> {
        self.items
    }

    /// Returns the internal continuation for opaque service-cursor issuance.
    #[must_use]
    pub const fn continuation(&self) -> Option<EventRouteContinuationV1> {
        self.continuation
    }

    /// Returns the frozen authoritative route frontier.
    #[must_use]
    pub const fn inclusive_upper(&self) -> EventRouteUpperFenceV1 {
        self.inclusive_upper
    }

    /// Returns the captured route frontier without exposing storage cursor types.
    #[must_use]
    pub const fn observed_upper(&self) -> Option<EventId> {
        match self.inclusive_upper {
            EventRouteUpperFenceV1::BeforeFirst => None,
            EventRouteUpperFenceV1::Inclusive(event_id) => Some(event_id),
        }
    }
}

/// Safe symbolic application envelope joined from event and enclosing commit.
///
/// Raw principal/session identity, partition/conflict keys, unselected payload,
/// credentials, and process-local tracing data are deliberately absent.
pub struct SymbolicEventEnvelope {
    view: SymbolicEventView,
    occurred_at: Timestamp,
    request_id: RequestId,
    root_request_id: RequestId,
    causing_event_id: Option<EventId>,
    actor_kind: ActorKind,
    provenance_id: ProvenanceId,
    history_incarnation: u64,
    policy_anchor: Option<riffdb_storage_api::StoredEventPolicyAnchorV1>,
}

impl SymbolicEventEnvelope {
    /// Returns the unchanged authoritative event identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.view.event_id()
    }

    /// Stable event type used to validate the retained policy-anchor schema.
    #[doc(hidden)]
    #[must_use]
    pub const fn event_type_id(&self) -> EventTypeId {
        self.view.event_type_id()
    }

    /// Returns the exact symbolic event type.
    #[must_use]
    pub fn event_name(&self) -> &str {
        self.view.event_name()
    }

    /// Returns the originating contract version.
    #[must_use]
    pub const fn writer_contract_version(&self) -> ContractVersion {
        self.view.writer_contract_version()
    }

    /// Returns the originating command-plan identity.
    #[must_use]
    pub const fn writer_plan_hash(&self) -> PlanHash {
        self.view.writer_plan_hash()
    }

    /// Returns the exact symbolic originating command.
    #[must_use]
    pub fn command_name(&self) -> &str {
        self.view.command_name()
    }

    /// Returns the exact command request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the inherited root request, or the direct command request.
    #[must_use]
    pub const fn root_request_id(&self) -> RequestId {
        self.root_request_id
    }

    /// Returns the causing event for a contextual reaction.
    #[must_use]
    pub const fn causing_event_id(&self) -> Option<EventId> {
        self.causing_event_id
    }

    /// Returns the deterministic occurrence time of the enclosing commit.
    #[must_use]
    pub const fn occurred_at(&self) -> Timestamp {
        self.occurred_at
    }

    /// Returns only the non-identifying admitted actor class.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns the opaque immutable provenance locator.
    #[must_use]
    pub const fn provenance_id(&self) -> ProvenanceId {
        self.provenance_id
    }

    /// Returns the restore fence bound to the replay response.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Compiler-owned current-row anchor retained by a V2 event.
    #[doc(hidden)]
    #[must_use]
    pub const fn policy_anchor(&self) -> Option<&riffdb_storage_api::StoredEventPolicyAnchorV1> {
        self.policy_anchor.as_ref()
    }

    /// Borrows only the explicitly selected symbolic payload fields.
    #[must_use]
    pub fn fields(&self) -> &[crate::SymbolicEventField] {
        self.view.fields()
    }

    /// Resolves one canonical enum identity through the active contract schema.
    #[must_use]
    pub fn enum_variant_name(&self, type_id: u32, variant_id: u32) -> Option<&str> {
        self.view.enum_variant_name(type_id, variant_id)
    }

    /// Borrows the shared enum display-name table for protocol presentation.
    #[must_use]
    pub const fn enum_variant_names(&self) -> &crate::ContractEnumVariantNames {
        self.view.enum_variant_names()
    }
}

impl fmt::Debug for SymbolicEventEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SymbolicEventEnvelope")
            .field("event", &self.view.event_name())
            .field("actor_kind", &self.actor_kind)
            .field("payload", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for SymbolicEventReplayPage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SymbolicEventReplayPage")
            .field("item_count", &self.items.len())
            .field("has_more", &self.continuation.is_some())
            .field("items", &"[REDACTED]")
            .finish()
    }
}

impl ActiveCatalogSnapshot {
    /// Resolves symbolic event, partition, and selected payload fields once.
    pub fn resolve_event_replay<'a, 'b>(
        &self,
        event_name: &str,
        partition: impl IntoIterator<Item = (&'a str, CanonicalValue)>,
        selected_field_names: impl IntoIterator<Item = &'b str>,
    ) -> Result<ResolvedEventReplay, EventReplayError> {
        let materializer = self.resolve_event_materializer(event_name, selected_field_names)?;
        let partition_hash = materializer.derive_partition_hash(partition)?;
        Ok(ResolvedEventReplay {
            materializer,
            partition_hash,
        })
    }

    /// Resolves one compiled event-stream operation and its exact canonical parameters.
    pub fn resolve_reactive_event_stream(
        &self,
        operation: &CompiledReactiveOperationV1,
        parameters: BTreeMap<String, CanonicalValue>,
    ) -> Result<ResolvedReactiveEventStream, EventReplayError> {
        let ReactiveOperationPlanV1::Stream {
            parameters: declared,
            partition,
            events,
            predicate,
        } = operation.plan()
        else {
            return Err(invalid_reactive_selection());
        };
        if declared.len() != parameters.len()
            || declared
                .iter()
                .any(|parameter| !parameters.contains_key(parameter.name()))
        {
            return Err(invalid_reactive_selection());
        }
        let partition_values = partition
            .iter()
            .map(|binding| {
                parameters
                    .get(binding.parameter())
                    .cloned()
                    .map(|value| (binding.field(), value))
                    .ok_or_else(invalid_reactive_selection)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut materializers = Vec::with_capacity(events.len());
        let mut partition_key = None;
        for event in events {
            let materializer = self.resolve_event_materializer(
                event.name(),
                event.fields().iter().map(|field| field.name()),
            )?;
            let key = materializer.derive_partition_key(
                partition_values
                    .iter()
                    .map(|(name, value)| (*name, value.clone())),
            )?;
            if partition_key
                .as_ref()
                .is_some_and(|expected| expected != &key)
            {
                return Err(EventReplayError::integrity());
            }
            partition_key = Some(key);
            materializers.push(materializer);
        }
        let partition_key = partition_key.ok_or_else(invalid_reactive_selection)?;
        let partition_hash = hash_partition_key(partition_key.as_bytes());
        Ok(ResolvedReactiveEventStream {
            materializers,
            partition_key,
            partition_hash,
            parameters,
            predicate: predicate.clone(),
        })
    }
}

fn invalid_reactive_selection() -> EventReplayError {
    EventReplayError {
        kind: EventReplayErrorKind::Materialization(EventMaterializationErrorKind::UnknownSymbol),
    }
}

enum PredicateValue {
    Scalar(CanonicalValue),
    Boolean(bool),
}

fn predicate_matches(
    predicate: &[ReactivePredicateNodeV1],
    parameters: &BTreeMap<String, CanonicalValue>,
    event: &SymbolicEventView,
) -> Result<bool, EventReplayError> {
    if predicate.is_empty() {
        return Ok(true);
    }
    let mut stack = Vec::new();
    for node in predicate {
        match node {
            ReactivePredicateNodeV1::EventField(name, _) => {
                let value = event
                    .fields()
                    .iter()
                    .find(|field| field.name() == name)
                    .map(|field| field.value().clone())
                    .ok_or_else(EventReplayError::integrity)?;
                stack.push(PredicateValue::Scalar(value));
            }
            ReactivePredicateNodeV1::Parameter(name, _) => {
                let value = parameters
                    .get(name)
                    .cloned()
                    .ok_or_else(EventReplayError::integrity)?;
                stack.push(PredicateValue::Scalar(value));
            }
            ReactivePredicateNodeV1::Literal(_, encoded) => {
                let value =
                    decode_canonical_value(encoded).map_err(|_| EventReplayError::integrity())?;
                stack.push(PredicateValue::Scalar(value));
            }
            ReactivePredicateNodeV1::Equal
            | ReactivePredicateNodeV1::NotEqual
            | ReactivePredicateNodeV1::Less
            | ReactivePredicateNodeV1::LessEqual
            | ReactivePredicateNodeV1::Greater
            | ReactivePredicateNodeV1::GreaterEqual => {
                let right = pop_scalar(&mut stack)?;
                let left = pop_scalar(&mut stack)?;
                let result = match node {
                    ReactivePredicateNodeV1::Equal => left == right,
                    ReactivePredicateNodeV1::NotEqual => left != right,
                    ReactivePredicateNodeV1::Less => {
                        scalar_order(&left, &right) == Some(Ordering::Less)
                    }
                    ReactivePredicateNodeV1::LessEqual => scalar_order(&left, &right)
                        .is_some_and(|ordering| ordering != Ordering::Greater),
                    ReactivePredicateNodeV1::Greater => {
                        scalar_order(&left, &right) == Some(Ordering::Greater)
                    }
                    ReactivePredicateNodeV1::GreaterEqual => scalar_order(&left, &right)
                        .is_some_and(|ordering| ordering != Ordering::Less),
                    _ => unreachable!(),
                };
                stack.push(PredicateValue::Boolean(result));
            }
            ReactivePredicateNodeV1::And | ReactivePredicateNodeV1::Or => {
                let right = pop_boolean(&mut stack)?;
                let left = pop_boolean(&mut stack)?;
                stack.push(PredicateValue::Boolean(
                    if matches!(node, ReactivePredicateNodeV1::And) {
                        left && right
                    } else {
                        left || right
                    },
                ));
            }
        }
    }
    match stack.pop() {
        Some(PredicateValue::Boolean(value)) if stack.is_empty() => Ok(value),
        _ => Err(EventReplayError::integrity()),
    }
}

fn pop_scalar(stack: &mut Vec<PredicateValue>) -> Result<CanonicalValue, EventReplayError> {
    match stack.pop() {
        Some(PredicateValue::Scalar(value)) => Ok(value),
        _ => Err(EventReplayError::integrity()),
    }
}

fn pop_boolean(stack: &mut Vec<PredicateValue>) -> Result<bool, EventReplayError> {
    match stack.pop() {
        Some(PredicateValue::Boolean(value)) => Ok(value),
        _ => Err(EventReplayError::integrity()),
    }
}

fn scalar_order(left: &CanonicalValue, right: &CanonicalValue) -> Option<Ordering> {
    match (left, right) {
        (CanonicalValue::I64(left), CanonicalValue::I64(right)) => left.partial_cmp(right),
        (CanonicalValue::U64(left), CanonicalValue::U64(right)) => left.partial_cmp(right),
        (CanonicalValue::String(left), CanonicalValue::String(right)) => {
            left.as_str().partial_cmp(right.as_str())
        }
        (CanonicalValue::Timestamp(left), CanonicalValue::Timestamp(right)) => {
            left.partial_cmp(right)
        }
        (CanonicalValue::Date(left), CanonicalValue::Date(right)) => left.partial_cmp(right),
        (CanonicalValue::Uuid(left), CanonicalValue::Uuid(right)) => left.partial_cmp(right),
        (
            CanonicalValue::Enum {
                type_id: left_type,
                variant_id: left_variant,
            },
            CanonicalValue::Enum {
                type_id: right_type,
                variant_id: right_variant,
            },
        ) if left_type == right_type => left_variant.partial_cmp(right_variant),
        _ => None,
    }
}
