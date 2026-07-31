//! Bounded symbolic replay over partition-local authoritative event routes.

use std::error::Error;
use std::fmt;

use riffdb_storage_api::{
    AuthoritativePointReader, EventRouteContinuationV1, EventRoutePageLimit,
    EventRouteScanRequestV1, EventRouteUpperFenceV1, PartitionEventRouteReader, StorageErrorKind,
};
use riffdb_types::{
    ActorKind, CanonicalValue, ContractVersion, EventId, PartitionKeyHash, PlanHash, ProvenanceId,
    RequestId, Timestamp,
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

impl ResolvedEventReplay {
    /// Returns the exact event symbol without exposing its stable numeric ID.
    #[must_use]
    pub fn event_name(&self) -> &str {
        self.materializer.event_name()
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
                    actor_kind: commit.actor().actor_kind(),
                    provenance_id: commit.provenance_id(),
                    history_incarnation,
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

/// One bounded page of symbolic matching events from a frozen route scan.
pub struct SymbolicEventReplayPage {
    items: Vec<SymbolicEventEnvelope>,
    continuation: Option<EventRouteContinuationV1>,
    inclusive_upper: EventRouteUpperFenceV1,
}

impl SymbolicEventReplayPage {
    /// Borrows matching symbolic events in exact partition order.
    #[must_use]
    pub fn items(&self) -> &[SymbolicEventEnvelope] {
        &self.items
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
}

/// Safe symbolic application envelope joined from event and enclosing commit.
///
/// Raw principal/session identity, partition/conflict keys, unselected payload,
/// credentials, and process-local tracing data are deliberately absent.
pub struct SymbolicEventEnvelope {
    view: SymbolicEventView,
    occurred_at: Timestamp,
    request_id: RequestId,
    actor_kind: ActorKind,
    provenance_id: ProvenanceId,
    history_incarnation: u64,
}

impl SymbolicEventEnvelope {
    /// Returns the unchanged authoritative event identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.view.event_id()
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

    /// Returns the original command request and root correlation identity.
    ///
    /// Direct commands use their own request as the root. A future contextual
    /// reaction successor may persist and expose an inherited root separately.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the root request correlation retained by current direct commands.
    #[must_use]
    pub const fn root_request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the causing event when persisted by a contextual-reaction format.
    ///
    /// Existing direct-command provenance has no causing event.
    #[must_use]
    pub const fn causing_event_id(&self) -> Option<EventId> {
        None
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

    /// Borrows only the explicitly selected symbolic payload fields.
    #[must_use]
    pub fn fields(&self) -> &[crate::SymbolicEventField] {
        self.view.fields()
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
}
