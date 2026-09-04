//! Safe structured trace records and a bounded tracing-subscriber layer.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use riffdb_types::{
    CommandId, CommitSequence, ContractVersion, IncidentId, MAX_COMMAND_CONFLICT_KEYS_V1,
    OutcomeId, PartitionKeyHash, PlanHash, RequestId, ServiceIngressKindV1, ServiceOperationV1,
};
use tracing::field::{self, Field, Visit};
use tracing::{Event, Instrument};

use crate::{
    AuthenticationDefect, AuthenticationRejection, AuthoritativeReadinessFailure,
    AuthorizationDefect, AuthorizationDenial, CatalogTelemetryEvent, CommandPipelineStage,
    CommitCallTerminal, CommitCommandTerminal, CommitExecutionFailureKind,
    CommitGroupDispatchReason, CommitIdempotencyObservation, CommitTelemetryEvent,
    CommitUncertaintyResolution, CommitUncertaintyStage, CompletionLanePhase, ConflictEventKind,
    IncidentClass, McpListChangeKind, McpRiskClass, McpSchemaFailurePhase, McpTelemetryEvent,
    McpTransportKind, PreparedEpochRollbackReason, ServiceTerminalClass,
};

const MAX_READ_DEPENDENCIES: usize = 4_096;
const MAX_ENTITY_MUTATIONS: usize = 4_096;
const MAX_EVENT_INTENTS: usize = 4_096;

/// The only tracing target emitted by this crate.
pub const SAFE_TRACE_TARGET: &str = "riffdb_observability::safe";

/// Maximum trace records retained by one collector.
pub const MAX_TRACE_RECORDS: usize = 4_096;

/// Required command-span fields from `SPEC.md` section 16.4.
///
/// The service operation is also recorded as a fixed local field so the same
/// wrapper can cover non-command entry points without accepting a dynamic name.
pub const REQUIRED_COMMAND_SPAN_FIELDS: [&str; 18] = [
    "request_id",
    "transport",
    "principal_id_hash",
    "agent_session_id_hash",
    "command_id",
    "contract_version",
    "plan_hash",
    "partition_key_hash",
    "conflict_key_count",
    "lock_wait_ms",
    "read_dependency_count",
    "mutation_count",
    "outbox_event_count",
    "commit_sequence",
    "storage_queue_ms",
    "commit_ms",
    "outcome_type",
    "replayed",
];

/// Closed structured trace kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceKind {
    /// Required audit was unavailable.
    ServiceAuditUnavailable,
    /// Service detected an integrity failure.
    ServiceInternalIntegrity,
    /// Cursor support failed closed.
    ServiceCursorUnavailable,
    /// Current policy closed a stream.
    ServiceStreamClosedByPolicy,
    /// Initial authentication rejected.
    AuthenticationRejected,
    /// Initial authentication hit an internal defect.
    AuthenticationDefect,
    /// Current authorization denied.
    AuthorizationDenied,
    /// Current authorization hit an internal defect.
    AuthorizationDefect,
    /// Conflict-manager event.
    Conflict,
    /// Catalog lifecycle event.
    Catalog,
    /// A redacted internal incident was retained.
    Incident,
    /// The injected incident source failed.
    IncidentSourceUnavailable,
    /// Authoritative readiness was failed.
    AuthoritativeReadinessFailed,
    /// One API-neutral service operation reached a caller-visible disposition.
    ServiceOperationTerminal,
    /// One commit-owner semantic transition completed.
    Commit,
    /// One MCP-owner semantic transition completed.
    Mcp,
}

impl TraceKind {
    const fn tag(self) -> u8 {
        match self {
            Self::ServiceAuditUnavailable => 1,
            Self::ServiceInternalIntegrity => 2,
            Self::ServiceCursorUnavailable => 3,
            Self::ServiceStreamClosedByPolicy => 4,
            Self::AuthenticationRejected => 5,
            Self::AuthenticationDefect => 6,
            Self::AuthorizationDenied => 7,
            Self::AuthorizationDefect => 8,
            Self::Conflict => 9,
            Self::Catalog => 10,
            Self::Incident => 11,
            Self::IncidentSourceUnavailable => 12,
            Self::AuthoritativeReadinessFailed => 13,
            Self::ServiceOperationTerminal => 14,
            Self::Commit => 15,
            Self::Mcp => 16,
        }
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::ServiceAuditUnavailable),
            2 => Some(Self::ServiceInternalIntegrity),
            3 => Some(Self::ServiceCursorUnavailable),
            4 => Some(Self::ServiceStreamClosedByPolicy),
            5 => Some(Self::AuthenticationRejected),
            6 => Some(Self::AuthenticationDefect),
            7 => Some(Self::AuthorizationDenied),
            8 => Some(Self::AuthorizationDefect),
            9 => Some(Self::Conflict),
            10 => Some(Self::Catalog),
            11 => Some(Self::Incident),
            12 => Some(Self::IncidentSourceUnavailable),
            13 => Some(Self::AuthoritativeReadinessFailed),
            14 => Some(Self::ServiceOperationTerminal),
            15 => Some(Self::Commit),
            16 => Some(Self::Mcp),
            _ => None,
        }
    }
}

/// One closed, value-free structured trace record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraceRecord {
    kind: TraceKind,
    operation: Option<ServiceOperationV1>,
    detail_tag: u8,
    value: u64,
    incident_id: Option<IncidentId>,
}

impl TraceRecord {
    const fn closed(
        kind: TraceKind,
        operation: Option<ServiceOperationV1>,
        detail_tag: u8,
        value: u64,
        incident_id: Option<IncidentId>,
    ) -> Self {
        Self {
            kind,
            operation,
            detail_tag,
            value,
            incident_id,
        }
    }

    pub(crate) const fn service_audit_unavailable(operation: ServiceOperationV1) -> Self {
        Self::closed(
            TraceKind::ServiceAuditUnavailable,
            Some(operation),
            0,
            1,
            None,
        )
    }

    pub(crate) const fn service_internal_integrity(operation: ServiceOperationV1) -> Self {
        Self::closed(
            TraceKind::ServiceInternalIntegrity,
            Some(operation),
            0,
            1,
            None,
        )
    }

    pub(crate) const fn service_cursor_unavailable() -> Self {
        Self::closed(TraceKind::ServiceCursorUnavailable, None, 0, 1, None)
    }

    pub(crate) const fn service_stream_closed_by_policy() -> Self {
        Self::closed(TraceKind::ServiceStreamClosedByPolicy, None, 0, 1, None)
    }

    pub(crate) const fn service_operation_terminal(
        operation: ServiceOperationV1,
        ingress: ServiceIngressKindV1,
        terminal: ServiceTerminalClass,
        elapsed_microseconds: u64,
    ) -> Self {
        Self::closed(
            TraceKind::ServiceOperationTerminal,
            Some(operation),
            service_terminal_detail_tag(ingress, terminal),
            elapsed_microseconds,
            None,
        )
    }

    pub(crate) const fn authentication_rejected(reason: AuthenticationRejection) -> Self {
        Self::closed(
            TraceKind::AuthenticationRejected,
            None,
            authentication_rejection_tag(reason),
            1,
            None,
        )
    }

    pub(crate) const fn authentication_defect(reason: AuthenticationDefect) -> Self {
        Self::closed(
            TraceKind::AuthenticationDefect,
            None,
            authentication_defect_tag(reason),
            1,
            None,
        )
    }

    pub(crate) const fn authorization_denied(code: AuthorizationDenial) -> Self {
        Self::closed(TraceKind::AuthorizationDenied, None, code.tag(), 1, None)
    }

    pub(crate) const fn authorization_defect(reason: AuthorizationDefect) -> Self {
        Self::closed(
            TraceKind::AuthorizationDefect,
            None,
            authorization_defect_tag(reason),
            1,
            None,
        )
    }

    pub(crate) const fn conflict(kind: ConflictEventKind, wait_microseconds: u64) -> Self {
        Self::closed(
            TraceKind::Conflict,
            None,
            conflict_event_tag(kind),
            wait_microseconds,
            None,
        )
    }

    pub(crate) const fn catalog(event: CatalogTelemetryEvent) -> Self {
        Self::closed(TraceKind::Catalog, None, catalog_event_tag(event), 1, None)
    }

    pub(crate) const fn commit(event: CommitTelemetryEvent) -> Self {
        let (detail_tag, value) = match event {
            CommitTelemetryEvent::CompletionLaneObserved { phase, elapsed, .. } => (
                92 + completion_lane_phase_tag(phase),
                saturating_duration_microseconds(elapsed),
            ),
            CommitTelemetryEvent::PreparationPoolDepthObserved { depth } => (84, depth as u64),
            CommitTelemetryEvent::ReorderBufferOccupancyObserved { occupancy } => {
                (85, occupancy as u64)
            }
            CommitTelemetryEvent::PreparedEpochRolledBack { reason } => {
                (85 + prepared_epoch_rollback_reason_tag(reason), 1)
            }
            CommitTelemetryEvent::FrontierEquivalenceChecked { equivalent } => {
                (if equivalent { 90 } else { 91 }, 1)
            }
            CommitTelemetryEvent::CommandPipelineStageCompleted { stage, elapsed, .. } => (
                74 + command_pipeline_stage_tag(stage),
                saturating_duration_microseconds(elapsed),
            ),
            CommitTelemetryEvent::CommandGroupDispatched {
                reason, elapsed, ..
            } => (
                70 + commit_group_dispatch_reason_tag(reason),
                saturating_duration_microseconds(elapsed),
            ),
            CommitTelemetryEvent::CommandGroupPartitioned {
                completion_groups, ..
            } => (81, completion_groups as u64),
            CommitTelemetryEvent::StorageQueueCompleted {
                ingress, elapsed, ..
            } => (ingress.tag(), saturating_duration_microseconds(elapsed)),
            CommitTelemetryEvent::CommandTerminal {
                ingress,
                terminal,
                elapsed,
                ..
            } => (
                3 + (ingress.tag() - 1) * 14 + commit_command_terminal_tag(terminal),
                saturating_duration_microseconds(elapsed),
            ),
            CommitTelemetryEvent::IdempotencyObserved { observation } => {
                (68 + commit_idempotency_observation_tag(observation), 1)
            }
            CommitTelemetryEvent::CommitCallCompleted {
                terminal, elapsed, ..
            } => (
                45 + commit_call_terminal_tag(terminal),
                saturating_duration_microseconds(elapsed),
            ),
            CommitTelemetryEvent::CommitApplicationCompleted { elapsed, .. } => {
                (83, saturating_duration_microseconds(elapsed))
            }
            CommitTelemetryEvent::CommitSubmissionCompleted { elapsed, .. } => {
                (82, saturating_duration_microseconds(elapsed))
            }
            CommitTelemetryEvent::UncertaintyResolved { stage, resolution } => (
                53 + (commit_uncertainty_stage_tag(stage) - 1) * 5
                    + commit_uncertainty_resolution_tag(resolution),
                1,
            ),
            CommitTelemetryEvent::WriterUnitCompleted { busy, .. } => {
                (80, saturating_duration_microseconds(busy))
            }
        };
        Self::closed(TraceKind::Commit, None, detail_tag, value, None)
    }

    pub(crate) const fn mcp(event: McpTelemetryEvent) -> Self {
        let detail_tag = match event {
            McpTelemetryEvent::SessionOpened { transport } => mcp_transport_tag(transport),
            McpTelemetryEvent::SessionClosed { transport } => 2 + mcp_transport_tag(transport),
            McpTelemetryEvent::ToolCall { risk } => 4 + mcp_risk_tag(risk),
            McpTelemetryEvent::SchemaFailure { phase } => 12 + mcp_schema_phase_tag(phase),
            McpTelemetryEvent::AuthorizationDenied => 15,
            McpTelemetryEvent::ListChangeNotification { kind } => 15 + mcp_list_change_tag(kind),
        };
        Self::closed(TraceKind::Mcp, None, detail_tag, 1, None)
    }

    pub(crate) const fn incident(class: IncidentClass, incident_id: IncidentId) -> Self {
        Self::closed(TraceKind::Incident, None, class.tag(), 1, Some(incident_id))
    }

    pub(crate) const fn incident_source_unavailable(class: IncidentClass) -> Self {
        Self::closed(
            TraceKind::IncidentSourceUnavailable,
            None,
            class.tag(),
            1,
            None,
        )
    }

    pub(crate) const fn authoritative_readiness_failed(
        reason: AuthoritativeReadinessFailure,
    ) -> Self {
        Self::closed(
            TraceKind::AuthoritativeReadinessFailed,
            None,
            readiness_reason_tag(reason),
            1,
            None,
        )
    }

    /// Returns the event classification.
    #[must_use]
    pub const fn kind(self) -> TraceKind {
        self.kind
    }

    /// Returns the closed operation, when one applies.
    #[must_use]
    pub const fn operation(self) -> Option<ServiceOperationV1> {
        self.operation
    }

    /// Returns a kind-specific closed numeric tag.
    #[must_use]
    pub const fn detail_tag(self) -> u8 {
        self.detail_tag
    }

    /// Returns the bounded numeric observation.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.value
    }

    /// Returns the opaque incident correlation identity, when present.
    #[must_use]
    pub const fn incident_id(self) -> Option<IncidentId> {
        self.incident_id
    }
}

struct TraceBufferState {
    records: VecDeque<TraceRecord>,
}

struct TraceBufferInner {
    capacity: usize,
    state: Mutex<TraceBufferState>,
    dropped: AtomicU64,
}

/// Bounded in-memory trace collector used by the process layer and tests.
#[derive(Clone)]
pub struct TraceCollector {
    inner: Arc<TraceBufferInner>,
}

impl TraceCollector {
    /// Creates a collector with a checked positive capacity.
    pub fn new(capacity: usize) -> Result<Self, TraceCollectorBuildError> {
        if capacity == 0 || capacity > MAX_TRACE_RECORDS {
            return Err(TraceCollectorBuildError);
        }
        Ok(Self {
            inner: Arc::new(TraceBufferInner {
                capacity,
                state: Mutex::new(TraceBufferState {
                    records: VecDeque::with_capacity(capacity),
                }),
                dropped: AtomicU64::new(0),
            }),
        })
    }

    /// Records one already-redacted trace, dropping rather than blocking at capacity.
    pub fn record(&self, record: TraceRecord) -> bool {
        let mut state = self.lock_state();
        if state.records.len() == self.inner.capacity {
            saturating_increment(&self.inner.dropped);
            return false;
        }
        state.records.push_back(record);
        true
    }

    /// Validates and records one tracing event from an adapter-owned subscriber.
    #[doc(hidden)]
    pub fn record_event(&self, event: &Event<'_>) {
        if !allows_safe_trace_target(event.metadata().target()) {
            return;
        }
        let mut visitor = TraceFieldVisitor::default();
        event.record(&mut visitor);
        if let Some(record) = visitor.finish() {
            self.record(record);
        }
    }

    /// Returns retained records in observation order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<TraceRecord> {
        self.lock_state().records.iter().copied().collect()
    }

    /// Returns the number of capacity-rejected records.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    fn lock_state(&self) -> MutexGuard<'_, TraceBufferState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl fmt::Debug for TraceCollector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TraceCollector([REDACTED])")
    }
}

/// Invalid trace collector configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraceCollectorBuildError;

impl fmt::Display for TraceCollectorBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("trace collector capacity is outside the accepted bound")
    }
}

impl std::error::Error for TraceCollectorBuildError {}

/// Returns whether metadata belongs to the exact safe first-party target.
#[must_use]
pub fn allows_safe_trace_target(target: &str) -> bool {
    target == SAFE_TRACE_TARGET
}

macro_rules! telemetry_hash {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Creates a telemetry hash from a previously domain-separated digest.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            /// Borrows the digest bytes.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!($name))?;
                formatter.write_str("(")?;
                write_hex(&self.0, formatter)?;
                formatter.write_str(")")
            }
        }
    };
}

telemetry_hash!(
    /// Domain-separated digest of the admitted principal identity.
    PrincipalIdTelemetryHash
);
telemetry_hash!(
    /// Domain-separated digest of an admitted agent-session identity.
    AgentSessionTelemetryHash
);

/// Checked bounded command-work counts accepted by the request-span vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandWorkCounts {
    conflict_keys: u64,
    read_dependencies: u64,
    mutations: u64,
    outbox_events: u64,
}

impl CommandWorkCounts {
    /// Validates counts against their semantic owner constants.
    pub fn new(
        conflict_keys: usize,
        read_dependencies: usize,
        mutations: usize,
        outbox_events: usize,
    ) -> Result<Self, CommandWorkCountError> {
        if conflict_keys > MAX_COMMAND_CONFLICT_KEYS_V1 {
            return Err(CommandWorkCountError::ConflictKeys);
        }
        if read_dependencies > MAX_READ_DEPENDENCIES {
            return Err(CommandWorkCountError::ReadDependencies);
        }
        if mutations > MAX_ENTITY_MUTATIONS {
            return Err(CommandWorkCountError::Mutations);
        }
        if outbox_events > MAX_EVENT_INTENTS {
            return Err(CommandWorkCountError::OutboxEvents);
        }
        Ok(Self {
            conflict_keys: u64::try_from(conflict_keys)
                .map_err(|_| CommandWorkCountError::ConflictKeys)?,
            read_dependencies: u64::try_from(read_dependencies)
                .map_err(|_| CommandWorkCountError::ReadDependencies)?,
            mutations: u64::try_from(mutations).map_err(|_| CommandWorkCountError::Mutations)?,
            outbox_events: u64::try_from(outbox_events)
                .map_err(|_| CommandWorkCountError::OutboxEvents)?,
        })
    }
}

/// A command work count exceeded the semantic owner bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandWorkCountError {
    /// Conflict-key count exceeded `MAX_COMMAND_CONFLICT_KEYS_V1`.
    ConflictKeys,
    /// Read-dependency count exceeded `MAX_READ_DEPENDENCIES`.
    ReadDependencies,
    /// Mutation count exceeded `MAX_ENTITY_MUTATIONS`.
    Mutations,
    /// Outbox-event count exceeded `MAX_EVENT_INTENTS`.
    OutboxEvents,
}

impl fmt::Display for CommandWorkCountError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("command trace count exceeds its semantic bound")
    }
}

impl std::error::Error for CommandWorkCountError {}

/// Opaque request span whose public methods accept only the fixed safe vocabulary.
pub struct SafeRequestSpan {
    span: tracing::Span,
}

impl SafeRequestSpan {
    /// Records previously domain-separated principal and optional session hashes.
    pub fn record_actor(
        &self,
        principal: PrincipalIdTelemetryHash,
        agent_session: Option<AgentSessionTelemetryHash>,
    ) {
        self.span
            .record("principal_id_hash", field::debug(principal));
        if let Some(agent_session) = agent_session {
            self.span
                .record("agent_session_id_hash", field::debug(agent_session));
        }
    }

    /// Records the exact checked command-plan identity.
    pub fn record_command(
        &self,
        command_id: CommandId,
        contract_version: ContractVersion,
        plan_hash: PlanHash,
    ) {
        self.span.record("command_id", u64::from(command_id.get()));
        self.span.record("contract_version", contract_version.get());
        self.span.record("plan_hash", field::debug(plan_hash));
    }

    /// Records the hashed partition and checked command-work counts.
    pub fn record_work(&self, partition: PartitionKeyHash, counts: CommandWorkCounts) {
        self.span
            .record("partition_key_hash", field::debug(partition));
        self.span.record("conflict_key_count", counts.conflict_keys);
        self.span
            .record("read_dependency_count", counts.read_dependencies);
        self.span.record("mutation_count", counts.mutations);
        self.span.record("outbox_event_count", counts.outbox_events);
    }

    /// Records a measured logical-lock wait without acquiring a clock.
    pub fn record_lock_wait(&self, duration: Duration) {
        self.span
            .record("lock_wait_ms", duration_milliseconds(duration));
    }

    /// Records storage-queue and commit durations measured by their owning layer.
    pub fn record_commit_timing(&self, storage_queue: Duration, commit: Duration) {
        self.span
            .record("storage_queue_ms", duration_milliseconds(storage_queue));
        self.span.record("commit_ms", duration_milliseconds(commit));
    }

    /// Records a terminal command result.
    pub fn record_outcome(
        &self,
        commit_sequence: CommitSequence,
        outcome: OutcomeId,
        replayed: bool,
    ) {
        self.span.record("commit_sequence", commit_sequence.get());
        self.span.record("outcome_type", u64::from(outcome.get()));
        self.span.record("replayed", replayed);
    }

    /// Runs synchronous work inside the span without exposing the raw span.
    pub fn in_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        self.span.in_scope(operation)
    }

    /// Instruments one future while keeping the raw span private.
    pub fn instrument<F>(&self, future: F) -> impl Future<Output = F::Output>
    where
        F: Future,
    {
        future.instrument(self.span.clone())
    }
}

impl fmt::Debug for SafeRequestSpan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SafeRequestSpan([REDACTED])")
    }
}

/// Creates a request span with a fixed, typed, nonsecret field vocabulary.
#[must_use]
pub fn request_span(
    request_id: RequestId,
    transport: ServiceIngressKindV1,
    operation: ServiceOperationV1,
) -> SafeRequestSpan {
    let span = tracing::info_span!(
        target: SAFE_TRACE_TARGET,
        "riffdb.request",
        request_id = %request_id,
        transport = u64::from(transport.tag()),
        service_operation = u64::from(operation.tag()),
        principal_id_hash = field::Empty,
        agent_session_id_hash = field::Empty,
        command_id = field::Empty,
        contract_version = field::Empty,
        plan_hash = field::Empty,
        partition_key_hash = field::Empty,
        conflict_key_count = field::Empty,
        lock_wait_ms = field::Empty,
        read_dependency_count = field::Empty,
        mutation_count = field::Empty,
        outbox_event_count = field::Empty,
        commit_sequence = field::Empty,
        storage_queue_ms = field::Empty,
        commit_ms = field::Empty,
        outcome_type = field::Empty,
        replayed = field::Empty
    );
    SafeRequestSpan { span }
}

pub(crate) fn emit(record: TraceRecord) {
    let (incident_present, incident_id_high, incident_id_low) = record
        .incident_id
        .map(|incident| {
            let (high, low) = split_uuid_bytes(*incident.as_bytes());
            (true, high, low)
        })
        .unwrap_or((false, 0, 0));
    tracing::event!(
        target: SAFE_TRACE_TARGET,
        tracing::Level::INFO,
        kind_tag = u64::from(record.kind.tag()),
        operation_tag = u64::from(record.operation.map_or(0, ServiceOperationV1::tag)),
        detail_tag = u64::from(record.detail_tag),
        value = record.value,
        incident_present,
        incident_id_high,
        incident_id_low
    );
}

fn split_uuid_bytes(bytes: [u8; 16]) -> (u64, u64) {
    let mut high = [0_u8; 8];
    let mut low = [0_u8; 8];
    high.copy_from_slice(&bytes[..8]);
    low.copy_from_slice(&bytes[8..]);
    (u64::from_be_bytes(high), u64::from_be_bytes(low))
}

#[derive(Default)]
struct TraceFieldVisitor {
    kind_tag: Option<u64>,
    operation_tag: Option<u64>,
    detail_tag: Option<u64>,
    value: Option<u64>,
    incident_present: Option<bool>,
    incident_id_high: Option<u64>,
    incident_id_low: Option<u64>,
    invalid: bool,
}

impl TraceFieldVisitor {
    fn finish(self) -> Option<TraceRecord> {
        if self.invalid {
            return None;
        }
        let kind = TraceKind::from_tag(u8::try_from(self.kind_tag?).ok()?)?;
        let operation_tag = u8::try_from(self.operation_tag?).ok()?;
        let operation = if operation_tag == 0 {
            None
        } else {
            ServiceOperationV1::from_tag(operation_tag)
        };
        if operation_tag != 0 && operation.is_none() {
            return None;
        }
        let detail_tag = u8::try_from(self.detail_tag?).ok()?;
        let incident_id = if self.incident_present? {
            let high = self.incident_id_high?.to_be_bytes();
            let low = self.incident_id_low?.to_be_bytes();
            let mut bytes = [0_u8; 16];
            bytes[..8].copy_from_slice(&high);
            bytes[8..].copy_from_slice(&low);
            IncidentId::from_bytes(bytes).ok()
        } else {
            None
        };
        if self.incident_present == Some(true) && incident_id.is_none() {
            return None;
        }
        Some(TraceRecord::closed(
            kind,
            operation,
            detail_tag,
            self.value?,
            incident_id,
        ))
    }
}

impl Visit for TraceFieldVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        let slot = match field.name() {
            "kind_tag" => &mut self.kind_tag,
            "operation_tag" => &mut self.operation_tag,
            "detail_tag" => &mut self.detail_tag,
            "value" => &mut self.value,
            "incident_id_high" => &mut self.incident_id_high,
            "incident_id_low" => &mut self.incident_id_low,
            _ => {
                self.invalid = true;
                return;
            }
        };
        if slot.replace(value).is_some() {
            self.invalid = true;
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() != "incident_present" || self.incident_present.replace(value).is_some() {
            self.invalid = true;
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {
        self.invalid = true;
    }
}

fn saturating_increment(counter: &AtomicU64) {
    let _result = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

const fn readiness_reason_tag(reason: AuthoritativeReadinessFailure) -> u8 {
    match reason {
        AuthoritativeReadinessFailure::AuditUnavailable => 1,
        AuthoritativeReadinessFailure::CoordinatorFenced => 2,
        AuthoritativeReadinessFailure::Integrity => 3,
    }
}

const fn authentication_rejection_tag(reason: AuthenticationRejection) -> u8 {
    match reason {
        AuthenticationRejection::MalformedCredential => 1,
        AuthenticationRejection::NoMatch => 2,
        AuthenticationRejection::InactiveCapability => 3,
        AuthenticationRejection::BoundaryMismatch => 4,
        AuthenticationRejection::OutsideValidityInterval => 5,
    }
}

const fn authentication_defect_tag(reason: AuthenticationDefect) -> u8 {
    match reason {
        AuthenticationDefect::ClockUnavailable => 1,
        AuthenticationDefect::RepositoryUnavailable => 2,
        AuthenticationDefect::RepositoryIntegrity => 3,
        AuthenticationDefect::MultipleMatches => 4,
        AuthenticationDefect::ReciprocalLinkMismatch => 5,
    }
}

const fn authorization_defect_tag(reason: AuthorizationDefect) -> u8 {
    match reason {
        AuthorizationDefect::CurrentCapabilityUnavailable => 1,
        AuthorizationDefect::ClockUnavailable => 2,
    }
}

const fn conflict_event_tag(kind: ConflictEventKind) -> u8 {
    match kind {
        ConflictEventKind::Queued => 1,
        ConflictEventKind::Acquired => 2,
        ConflictEventKind::Cancelled => 3,
        ConflictEventKind::DeadlineExceeded => 4,
        ConflictEventKind::Released => 5,
        ConflictEventKind::CapacityRejected => 6,
    }
}

const fn catalog_event_tag(event: CatalogTelemetryEvent) -> u8 {
    match event {
        CatalogTelemetryEvent::ActivationPrepared => 1,
        CatalogTelemetryEvent::ActivationDurable => 2,
        CatalogTelemetryEvent::NotificationDelivered => 3,
        CatalogTelemetryEvent::NotificationFailed => 4,
        CatalogTelemetryEvent::NoCatalogChange => 5,
    }
}

const fn commit_group_dispatch_reason_tag(reason: CommitGroupDispatchReason) -> u8 {
    match reason {
        CommitGroupDispatchReason::Full => 1,
        CommitGroupDispatchReason::Barrier => 2,
        CommitGroupDispatchReason::QueueDrained => 3,
        CommitGroupDispatchReason::ReceiverClosed => 4,
    }
}

const fn service_terminal_detail_tag(
    ingress: ServiceIngressKindV1,
    terminal: ServiceTerminalClass,
) -> u8 {
    (service_terminal_tag(terminal) - 1) * 3 + ingress.tag()
}

const fn service_terminal_tag(terminal: ServiceTerminalClass) -> u8 {
    match terminal {
        ServiceTerminalClass::Succeeded => 1,
        ServiceTerminalClass::Validation => 2,
        ServiceTerminalClass::IdempotencyMismatch => 3,
        ServiceTerminalClass::AuthorizationDenied => 4,
        ServiceTerminalClass::ConcurrencyDeadlineExceeded => 5,
        ServiceTerminalClass::ContractMismatch => 6,
        ServiceTerminalClass::StorageUnavailable => 7,
        ServiceTerminalClass::OutcomeUnknown => 8,
        ServiceTerminalClass::InternalDefect => 9,
        ServiceTerminalClass::CommandExecutionFailed => 10,
        ServiceTerminalClass::Cancelled => 11,
        ServiceTerminalClass::DeadlineExceeded => 12,
        ServiceTerminalClass::ResponseTooLarge => 13,
        ServiceTerminalClass::EmergencyInternal => 14,
        ServiceTerminalClass::HistoryIncarnationMismatch => 15,
        ServiceTerminalClass::HistoryPruned => 17,
        ServiceTerminalClass::Overloaded => 16,
    }
}

const fn commit_command_terminal_tag(terminal: CommitCommandTerminal) -> u8 {
    match terminal {
        CommitCommandTerminal::ReadOnlySucceeded => 1,
        CommitCommandTerminal::FirstCommit => 2,
        CommitCommandTerminal::OutcomeReplay => 3,
        CommitCommandTerminal::ExecutionFailed => 4,
        CommitCommandTerminal::PreparationChanged => 5,
        CommitCommandTerminal::InputMismatch => 6,
        CommitCommandTerminal::Failed(kind) => 6 + command_execution_error_tag(kind),
    }
}

const fn prepared_epoch_rollback_reason_tag(reason: PreparedEpochRollbackReason) -> u8 {
    match reason {
        PreparedEpochRollbackReason::WorkerFailure => 1,
        PreparedEpochRollbackReason::ProofMismatch => 2,
        PreparedEpochRollbackReason::CurrentStateChanged => 3,
        PreparedEpochRollbackReason::Cancelled => 4,
    }
}

const fn commit_idempotency_observation_tag(observation: CommitIdempotencyObservation) -> u8 {
    match observation {
        CommitIdempotencyObservation::Hit => 1,
        CommitIdempotencyObservation::Mismatch => 2,
    }
}

const fn command_execution_error_tag(kind: CommitExecutionFailureKind) -> u8 {
    match kind {
        CommitExecutionFailureKind::Cancelled => 1,
        CommitExecutionFailureKind::DeadlineExceeded => 2,
        CommitExecutionFailureKind::RetryBudgetExhausted => 3,
        CommitExecutionFailureKind::StorageUnavailable => 4,
        CommitExecutionFailureKind::OutcomeUnknown => 5,
        CommitExecutionFailureKind::InternalDefect => 6,
        CommitExecutionFailureKind::CoordinatorStopped => 7,
        CommitExecutionFailureKind::CoordinatorFenced => 8,
        CommitExecutionFailureKind::AuthorizationDenied => 9,
    }
}

const fn commit_call_terminal_tag(terminal: CommitCallTerminal) -> u8 {
    match terminal {
        CommitCallTerminal::Committed => 1,
        CommitCallTerminal::ProvenAbort => 2,
        CommitCallTerminal::StatusUnknown => 3,
        CommitCallTerminal::Integrity => 4,
    }
}

const fn command_pipeline_stage_tag(stage: CommandPipelineStage) -> u8 {
    match stage {
        CommandPipelineStage::Admission => 1,
        CommandPipelineStage::Compatibility => 2,
        CommandPipelineStage::Evaluation => 3,
        CommandPipelineStage::ValidationEncodingStaging => 4,
        CommandPipelineStage::Publication => 5,
    }
}

const fn completion_lane_phase_tag(phase: CompletionLanePhase) -> u8 {
    match phase {
        CompletionLanePhase::Submitted => 0,
        CompletionLanePhase::Published => 1,
        CompletionLanePhase::Drained => 2,
        CompletionLanePhase::Shutdown => 3,
    }
}

const fn commit_uncertainty_stage_tag(stage: CommitUncertaintyStage) -> u8 {
    match stage {
        CommitUncertaintyStage::Admission => 1,
        CommitUncertaintyStage::CommandCommit => 2,
        CommitUncertaintyStage::ExecutionFailure => 3,
    }
}

const fn commit_uncertainty_resolution_tag(resolution: CommitUncertaintyResolution) -> u8 {
    match resolution {
        CommitUncertaintyResolution::Outcome => 1,
        CommitUncertaintyResolution::ExecutionFailure => 2,
        CommitUncertaintyResolution::ProvenPending => 3,
        CommitUncertaintyResolution::StillUnknown => 4,
        CommitUncertaintyResolution::Integrity => 5,
    }
}

const fn mcp_transport_tag(transport: McpTransportKind) -> u8 {
    match transport {
        McpTransportKind::Stdio => 1,
        McpTransportKind::StreamableHttp => 2,
    }
}

const fn mcp_risk_tag(risk: McpRiskClass) -> u8 {
    match risk {
        McpRiskClass::AdministrativeMutation => 1,
        McpRiskClass::AdministrativeRead => 2,
        McpRiskClass::BoundedAdministrativeRead => 3,
        McpRiskClass::BoundedRead => 4,
        McpRiskClass::ReadOnly => 5,
        McpRiskClass::ReadOnlyCompute => 6,
        McpRiskClass::ReadOnlyData => 7,
        McpRiskClass::SymbolicRead => 8,
        McpRiskClass::ReactiveApplication => 9,
        McpRiskClass::ConsumerControl => 10,
        McpRiskClass::ApplicationMutation => 11,
        McpRiskClass::DynamicCommand => 12,
    }
}

const fn mcp_schema_phase_tag(phase: McpSchemaFailurePhase) -> u8 {
    match phase {
        McpSchemaFailurePhase::Input => 1,
        McpSchemaFailurePhase::Output => 2,
    }
}

const fn mcp_list_change_tag(kind: McpListChangeKind) -> u8 {
    match kind {
        McpListChangeKind::Tools => 1,
        McpListChangeKind::Resources => 2,
    }
}

const fn saturating_duration_microseconds(duration: Duration) -> u64 {
    let micros = duration.as_micros();
    if micros > u64::MAX as u128 {
        u64::MAX
    } else {
        micros as u64
    }
}

fn duration_milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn write_hex(bytes: &[u8], formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tracing::{Event, Subscriber};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    #[derive(Clone)]
    struct SafeTraceLayer {
        collector: TraceCollector,
    }

    impl SafeTraceLayer {
        fn new(capacity: usize) -> Result<Self, TraceCollectorBuildError> {
            TraceCollector::new(capacity).map(|collector| Self { collector })
        }

        fn collector(&self) -> TraceCollector {
            self.collector.clone()
        }
    }

    impl<S> Layer<S> for SafeTraceLayer
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            self.collector.record_event(event);
        }
    }

    #[test]
    fn layer_accepts_only_the_exact_numeric_schema() {
        let layer = SafeTraceLayer::new(4).expect("bounded layer");
        let collector = layer.collector();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            emit(TraceRecord::service_cursor_unavailable());
            tracing::event!(
                target: SAFE_TRACE_TARGET,
                tracing::Level::INFO,
                secret = "secret-canary"
            );
            tracing::event!(
                target: "rmcp::transport",
                tracing::Level::INFO,
                kind_tag = 3_u64,
                operation_tag = 0_u64,
                detail_tag = 0_u64,
                value = 1_u64,
                incident_present = false,
                incident_id_high = 0_u64,
                incident_id_low = 0_u64
            );
        });
        assert_eq!(collector.snapshot().len(), 1);
    }

    /// ADR-0118 telemetry sweep: an event attempting to carry a
    /// secret-classified field's value — as a string field, or riding an
    /// otherwise-valid numeric record — never reaches the captured
    /// telemetry, and the raw rendering of everything that WAS captured
    /// contains no trace of it. The accepted numeric record proves the
    /// channel is live (non-empty triggering set).
    #[test]
    fn secret_field_values_cannot_enter_captured_telemetry() {
        const SECRET_CANARY: &str = "wp597-telemetry-canary-b6e3";
        let layer = SafeTraceLayer::new(8).expect("bounded layer");
        let collector = layer.collector();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            emit(TraceRecord::service_cursor_unavailable());
            tracing::event!(
                target: SAFE_TRACE_TARGET,
                tracing::Level::INFO,
                token_hash = SECRET_CANARY
            );
            tracing::event!(
                target: SAFE_TRACE_TARGET,
                tracing::Level::INFO,
                kind_tag = 3_u64,
                operation_tag = 0_u64,
                detail_tag = 0_u64,
                value = 1_u64,
                incident_present = false,
                incident_id_high = 0_u64,
                incident_id_low = 0_u64,
                token_hash = SECRET_CANARY
            );
        });
        let captured = collector.snapshot();
        assert_eq!(
            captured.len(),
            1,
            "only the closed numeric record may be captured"
        );
        let rendered = format!("{captured:?}");
        assert!(
            !rendered.is_empty(),
            "the captured channel must be observable"
        );
        assert!(
            !rendered.contains(SECRET_CANARY),
            "no captured telemetry rendering may carry the secret value"
        );
    }
}
