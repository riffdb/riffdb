//! Integration over closed upstream telemetry and health hooks.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use riffdb_api_mcp::{McpTelemetry, McpTelemetryEvent};
use riffdb_auth::{AuthenticationTelemetry, AuthenticationTelemetryEvent};
use riffdb_catalog::{
    CatalogDeploymentFailpoint, CatalogDeploymentHooks, CatalogFailpointTriggered,
    CatalogTelemetryEvent,
};
use riffdb_commit::{
    CommitGroupDispatchReason, CommitIdempotencyObservation, CommitTelemetry, CommitTelemetryEvent,
    CommitUncertaintyResolution, CommitUncertaintyStage,
};
use riffdb_conflict::{ConflictEvent, ConflictObserver};
use riffdb_errors::{IncidentIdSource, InternalError};
use riffdb_policy::{AuthorizationTelemetry, AuthorizationTelemetryEvent};
use riffdb_service::{
    AuthoritativeReadinessFailure, ServiceDiagnostics, ServiceHealthHooks, ServiceTelemetry,
    ServiceTelemetryEvent,
};
use riffdb_types::{ConflictKeyHash, IncidentId};

use crate::{
    HealthRegistry, MetricKey, MetricLabel, MetricRegistry, RequiredCounter, RequiredGauge,
    RequiredHistogram, TraceCollector, TraceRecord,
};

/// Maximum redacted incidents retained for operator correlation.
pub const MAX_RETAINED_INCIDENTS: usize = 256;
/// Maximum distinct queued conflict-key hashes retained for hot-key cardinality.
pub const MAX_HOT_CONFLICT_KEYS: usize = 1_024;
/// Closed production completion-group sizes.
pub const MAX_WRITE_GROUP_SIZE: usize = 64;
/// Closed scheduler dispatch-reason cardinality.
pub const COMMAND_GROUP_DISPATCH_REASON_COUNT: usize = 4;

/// Closed internal incident classifications.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum IncidentClass {
    /// An upstream internal error already had an incident identity.
    InternalError,
    /// A containable panic was observed.
    PanicContained,
    /// A required injected provider failed.
    ProviderFailure,
    /// A checked semantic or durable join failed.
    Integrity,
    /// A required durable audit operation failed.
    Audit,
}

impl IncidentClass {
    pub(crate) const ALL: [Self; 5] = [
        Self::InternalError,
        Self::PanicContained,
        Self::ProviderFailure,
        Self::Integrity,
        Self::Audit,
    ];

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::InternalError => 0,
            Self::PanicContained => 1,
            Self::ProviderFailure => 2,
            Self::Integrity => 3,
            Self::Audit => 4,
        }
    }

    pub(crate) const fn tag(self) -> u8 {
        (self.index() + 1) as u8
    }

    pub(crate) const fn label(self) -> MetricLabel {
        MetricLabel {
            key: "class",
            value: match self {
                Self::InternalError => "internal_error",
                Self::PanicContained => "panic_contained",
                Self::ProviderFailure => "provider_failure",
                Self::Integrity => "integrity",
                Self::Audit => "audit",
            },
        }
    }
}

/// One redacted incident observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncidentRecord {
    incident_id: IncidentId,
    class: IncidentClass,
}

impl IncidentRecord {
    /// Returns the opaque incident identity.
    #[must_use]
    pub const fn incident_id(self) -> IncidentId {
        self.incident_id
    }

    /// Returns the closed incident class.
    #[must_use]
    pub const fn class(self) -> IncidentClass {
        self.class
    }
}

struct IncidentState {
    records: VecDeque<IncidentRecord>,
}

/// Process-local aggregation of safe telemetry and incident correlation.
///
/// The injected source is synchronous and is the only way this type creates an
/// incident ID. Source failure returns no correlation value and monotonically
/// fails authoritative readiness.
pub struct Observability {
    incident_ids: Arc<dyn IncidentIdSource>,
    incidents: Mutex<IncidentState>,
    hot_conflict_keys: Mutex<BTreeSet<ConflictKeyHash>>,
    write_completion_groups: [AtomicU64; MAX_WRITE_GROUP_SIZE],
    command_group_dispatch_reasons: [AtomicU64; COMMAND_GROUP_DISPATCH_REASON_COUNT],
    command_group_selected_total: AtomicU64,
    command_group_deferred_total: AtomicU64,
    metrics: MetricRegistry,
    traces: TraceCollector,
    health: HealthRegistry,
}

impl Observability {
    /// Creates the bounded process integration.
    pub fn new(
        incident_ids: Arc<dyn IncidentIdSource>,
        trace_capacity: usize,
    ) -> Result<Self, ObservabilityBuildError> {
        let traces = TraceCollector::new(trace_capacity)
            .map_err(|_| ObservabilityBuildError::TraceCapacity)?;
        Ok(Self {
            incident_ids,
            incidents: Mutex::new(IncidentState {
                records: VecDeque::with_capacity(MAX_RETAINED_INCIDENTS),
            }),
            hot_conflict_keys: Mutex::new(BTreeSet::new()),
            write_completion_groups: std::array::from_fn(|_| AtomicU64::new(0)),
            command_group_dispatch_reasons: std::array::from_fn(|_| AtomicU64::new(0)),
            command_group_selected_total: AtomicU64::new(0),
            command_group_deferred_total: AtomicU64::new(0),
            metrics: MetricRegistry::new(),
            traces,
            health: HealthRegistry::new(),
        })
    }

    /// Borrows the fixed-cardinality registry.
    #[must_use]
    pub const fn metrics(&self) -> &MetricRegistry {
        &self.metrics
    }

    /// Borrows the health registry.
    #[must_use]
    pub const fn health(&self) -> &HealthRegistry {
        &self.health
    }

    /// Returns exact successful completion-commit counts for sizes 1 through 64.
    #[must_use]
    pub fn write_completion_group_snapshot(&self) -> [u64; MAX_WRITE_GROUP_SIZE] {
        std::array::from_fn(|index| self.write_completion_groups[index].load(Ordering::Relaxed))
    }

    /// Returns dispatch counts in full, barrier, queue-drained, receiver-closed order.
    #[must_use]
    pub fn command_group_dispatch_snapshot(
        &self,
    ) -> ([u64; COMMAND_GROUP_DISPATCH_REASON_COUNT], u64, u64) {
        (
            std::array::from_fn(|index| {
                self.command_group_dispatch_reasons[index].load(Ordering::Relaxed)
            }),
            self.command_group_selected_total.load(Ordering::Relaxed),
            self.command_group_deferred_total.load(Ordering::Relaxed),
        )
    }

    /// Returns a cloneable bounded trace collector.
    #[must_use]
    pub fn traces(&self) -> TraceCollector {
        self.traces.clone()
    }

    /// Returns redacted incidents in observation order.
    ///
    /// UUID byte order is deliberately not used as event order.
    #[must_use]
    pub fn incident_snapshot(&self) -> Vec<IncidentRecord> {
        self.lock_incidents().records.iter().copied().collect()
    }

    /// Sources and retains one new opaque incident.
    pub fn report_incident(&self, class: IncidentClass) -> Result<IncidentId, IncidentReportError> {
        let incident_id = self.incident_ids.next_incident_id().map_err(|_| {
            self.metrics.increment(MetricKey::IncidentSourceFailure);
            self.health.fail_authoritative_readiness();
            self.record_trace(TraceRecord::incident_source_unavailable(class));
            IncidentReportError::SourceUnavailable
        })?;
        self.retain_incident(IncidentRecord { incident_id, class })?;
        Ok(incident_id)
    }

    fn retain_incident(&self, record: IncidentRecord) -> Result<(), IncidentReportError> {
        let mut incidents = self.lock_incidents();
        if incidents.records.len() == MAX_RETAINED_INCIDENTS {
            self.metrics.increment(MetricKey::TelemetryDropped);
            self.health.fail_authoritative_readiness();
            return Err(IncidentReportError::CapacityExceeded);
        }
        incidents.records.push_back(record);
        drop(incidents);
        self.metrics.increment(MetricKey::Incident(record.class));
        self.record_trace(TraceRecord::incident(record.class, record.incident_id));
        Ok(())
    }

    fn record_trace(&self, record: TraceRecord) {
        if !self.traces.record(record) {
            self.metrics.increment(MetricKey::TelemetryDropped);
        }
        crate::tracing_layer::emit(record);
    }

    fn lock_incidents(&self) -> MutexGuard<'_, IncidentState> {
        self.incidents.lock().unwrap_or_else(|poisoned| {
            self.health.fail_authoritative_readiness();
            poisoned.into_inner()
        })
    }
}

impl fmt::Debug for Observability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Observability([REDACTED])")
    }
}

/// Invalid process-observability construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservabilityBuildError {
    /// The trace retention capacity is zero or above the fixed bound.
    TraceCapacity,
}

impl fmt::Display for ObservabilityBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("observability configuration is outside the accepted bound")
    }
}

impl std::error::Error for ObservabilityBuildError {}

/// A failure to obtain or retain a trustworthy incident correlation value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncidentReportError {
    /// The injected incident source returned its closed failure.
    SourceUnavailable,
    /// The bounded redacted incident registry is full.
    CapacityExceeded,
}

impl fmt::Display for IncidentReportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SourceUnavailable => "incident identifier source is unavailable",
            Self::CapacityExceeded => "incident registry capacity is exhausted",
        })
    }
}

impl std::error::Error for IncidentReportError {}

impl ServiceTelemetry for Observability {
    fn record(&self, event: ServiceTelemetryEvent) {
        let (metric, trace) = match event {
            ServiceTelemetryEvent::OperationTerminal {
                operation,
                ingress,
                terminal,
                elapsed,
            } => (
                MetricKey::ServiceOperationTerminal(terminal),
                TraceRecord::service_operation_terminal(
                    operation,
                    ingress,
                    terminal,
                    duration_micros(elapsed),
                ),
            ),
            ServiceTelemetryEvent::AuditUnavailable { operation } => (
                MetricKey::ServiceAuditUnavailable(operation),
                TraceRecord::service_audit_unavailable(operation),
            ),
            ServiceTelemetryEvent::InternalIntegrity { operation } => (
                MetricKey::ServiceInternalIntegrity(operation),
                TraceRecord::service_internal_integrity(operation),
            ),
            ServiceTelemetryEvent::CursorUnavailable => (
                MetricKey::ServiceCursorUnavailable,
                TraceRecord::service_cursor_unavailable(),
            ),
            ServiceTelemetryEvent::StreamClosedByPolicy => (
                MetricKey::ServiceStreamClosedByPolicy,
                TraceRecord::service_stream_closed_by_policy(),
            ),
            // Package R events: retained by production counters; fixed-cardinality
            // metric series expansion is deferred until the next metrics layout cut.
            ServiceTelemetryEvent::CursorEvicted
            | ServiceTelemetryEvent::ReadRetryAttempt { .. }
            | ServiceTelemetryEvent::ReadRetryExhausted { .. } => return,
        };
        self.metrics.increment(metric);
        self.record_trace(trace);
    }
}

impl ServiceDiagnostics for Observability {
    fn record_internal(&self, error: InternalError) {
        let incident_id = *error.incident_id();
        drop(error);
        if self
            .retain_incident(IncidentRecord {
                incident_id,
                class: IncidentClass::InternalError,
            })
            .is_err()
        {
            self.health.fail_authoritative_readiness();
        }
    }
}

impl ServiceHealthHooks for Observability {
    fn fail_authoritative_readiness(&self, reason: AuthoritativeReadinessFailure) {
        self.metrics
            .increment(MetricKey::AuthoritativeReadinessFailure(reason));
        self.health.fail_authoritative_readiness();
        self.record_trace(TraceRecord::authoritative_readiness_failed(reason));
    }
}

impl AuthenticationTelemetry for Observability {
    fn record(&self, event: AuthenticationTelemetryEvent) {
        let (metric, trace) = match event {
            AuthenticationTelemetryEvent::Rejected(reason) => (
                MetricKey::AuthenticationRejected(reason),
                TraceRecord::authentication_rejected(reason),
            ),
            AuthenticationTelemetryEvent::Defect(reason) => (
                MetricKey::AuthenticationDefect(reason),
                TraceRecord::authentication_defect(reason),
            ),
        };
        self.metrics.increment(metric);
        self.record_trace(trace);
    }
}

impl AuthorizationTelemetry for Observability {
    fn record(&self, event: AuthorizationTelemetryEvent) {
        let (metric, trace) = match event {
            AuthorizationTelemetryEvent::Denied(code) => (
                MetricKey::AuthorizationDenied(code),
                TraceRecord::authorization_denied(code),
            ),
            AuthorizationTelemetryEvent::Defect(reason) => (
                MetricKey::AuthorizationDefect(reason),
                TraceRecord::authorization_defect(reason),
            ),
        };
        self.metrics.increment(metric);
        self.record_trace(trace);
    }
}

impl ConflictObserver for Observability {
    fn observe(&self, event: ConflictEvent) {
        let wait_microseconds = duration_micros(event.wait_duration());
        self.metrics
            .increment(MetricKey::ConflictEvent(event.kind()));
        self.metrics
            .add(MetricKey::ConflictWaitMicroseconds, wait_microseconds);
        self.metrics.add(
            MetricKey::ConflictQueueDepth,
            u64::try_from(event.total_queue_depth()).unwrap_or(u64::MAX),
        );
        self.metrics
            .observe_required_histogram(RequiredHistogram::LockWaitMicroseconds, wait_microseconds);
        self.metrics.set_required_gauge(
            RequiredGauge::ConflictQueueDepth,
            u64::try_from(event.total_queue_depth()).unwrap_or(u64::MAX),
        );
        self.observe_hot_conflict_key(&event);
        if event.kind() == riffdb_conflict::ConflictEventKind::DeadlineExceeded {
            self.metrics
                .increment_required_counter(RequiredCounter::LockTimeouts);
        }
        self.record_trace(TraceRecord::conflict(event.kind(), wait_microseconds));
    }
}

impl CommitTelemetry for Observability {
    fn record(&self, event: CommitTelemetryEvent) {
        match event {
            CommitTelemetryEvent::CommandPipelineStageCompleted { .. } => {}
            CommitTelemetryEvent::CommandGroupDispatched {
                reason,
                selected,
                deferred,
                ..
            } => {
                saturating_increment(
                    &self.command_group_dispatch_reasons
                        [command_group_dispatch_reason_index(reason)],
                );
                saturating_add(&self.command_group_selected_total, u64::from(selected));
                saturating_add(&self.command_group_deferred_total, u64::from(deferred));
            }
            CommitTelemetryEvent::StorageQueueCompleted { elapsed, .. } => {
                self.metrics.observe_required_histogram(
                    RequiredHistogram::StorageQueueLatencyMicroseconds,
                    duration_micros(elapsed),
                );
            }
            CommitTelemetryEvent::CommandTerminal {
                command_id,
                ingress,
                terminal,
                elapsed,
            } => {
                let elapsed = duration_micros(elapsed);
                self.metrics
                    .increment_required_counter(RequiredCounter::CommandRequests);
                self.metrics.observe_required_histogram(
                    RequiredHistogram::CommandLatencyMicroseconds,
                    elapsed,
                );
                if !self
                    .metrics
                    .observe_command(command_id, ingress, terminal, elapsed)
                {
                    self.metrics.increment(MetricKey::TelemetryDropped);
                }
            }
            CommitTelemetryEvent::IdempotencyObserved { observation } => match observation {
                CommitIdempotencyObservation::Hit => self
                    .metrics
                    .increment_required_counter(RequiredCounter::IdempotencyHits),
                CommitIdempotencyObservation::Mismatch => self
                    .metrics
                    .increment_required_counter(RequiredCounter::IdempotencyMismatches),
            },
            CommitTelemetryEvent::CommitCallCompleted {
                terminal,
                elapsed,
                batch_size,
                synchronous,
            } => {
                let elapsed = duration_micros(elapsed);
                self.metrics.observe_required_histogram(
                    RequiredHistogram::CommitDurationMicroseconds,
                    elapsed,
                );
                if synchronous {
                    self.metrics.observe_required_histogram(
                        RequiredHistogram::DurableFlushDurationMicroseconds,
                        elapsed,
                    );
                }
                self.metrics.observe_required_histogram(
                    RequiredHistogram::CommitBatchSize,
                    u64::from(batch_size),
                );
                if terminal == riffdb_commit::CommitCallTerminal::Committed {
                    if let Some(counter) = usize::from(batch_size)
                        .checked_sub(1)
                        .and_then(|index| self.write_completion_groups.get(index))
                    {
                        saturating_increment(counter);
                    } else {
                        self.metrics.increment(MetricKey::TelemetryDropped);
                    }
                    self.metrics
                        .increment_required_counter(RequiredCounter::Commits);
                }
            }
            CommitTelemetryEvent::UncertaintyResolved { stage, resolution } => {
                self.metrics
                    .increment_required_counter(RequiredCounter::IdempotencyUncertaintyRecoveries);
                if stage == CommitUncertaintyStage::CommandCommit
                    && resolution == CommitUncertaintyResolution::Outcome
                {
                    self.metrics
                        .increment_required_counter(RequiredCounter::Commits);
                }
            }
        }
        self.record_trace(TraceRecord::commit(event));
    }
}

const fn command_group_dispatch_reason_index(reason: CommitGroupDispatchReason) -> usize {
    match reason {
        CommitGroupDispatchReason::Full => 0,
        CommitGroupDispatchReason::Barrier => 1,
        CommitGroupDispatchReason::QueueDrained => 2,
        CommitGroupDispatchReason::ReceiverClosed => 3,
    }
}

fn saturating_increment(counter: &AtomicU64) {
    saturating_add(counter, 1);
}

fn saturating_add(counter: &AtomicU64, addend: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(addend))
    });
}

impl McpTelemetry for Observability {
    fn record(&self, event: McpTelemetryEvent) {
        match event {
            McpTelemetryEvent::SessionOpened { .. } => self
                .metrics
                .adjust_required_gauge(RequiredGauge::McpSessions, 1),
            McpTelemetryEvent::SessionClosed { .. } => self
                .metrics
                .adjust_required_gauge(RequiredGauge::McpSessions, -1),
            McpTelemetryEvent::ToolCall { risk } => {
                self.metrics.increment_mcp_tool_call(risk);
            }
            McpTelemetryEvent::SchemaFailure { .. } => self
                .metrics
                .increment_required_counter(RequiredCounter::McpSchemaFailures),
            McpTelemetryEvent::AuthorizationDenied => self
                .metrics
                .increment_required_counter(RequiredCounter::McpAuthorizationDenials),
            McpTelemetryEvent::ListChangeNotification { .. } => self
                .metrics
                .increment_required_counter(RequiredCounter::McpListChangeNotifications),
        }
        self.record_trace(TraceRecord::mcp(event));
    }
}

impl CatalogDeploymentHooks for Observability {
    fn record(&mut self, event: CatalogTelemetryEvent) {
        self.metrics.increment(MetricKey::CatalogEvent(event));
        if event == CatalogTelemetryEvent::ActivationDurable {
            self.metrics
                .increment_required_counter(RequiredCounter::ContractDeployments);
        }
        self.record_trace(TraceRecord::catalog(event));
    }

    fn reach(
        &mut self,
        _failpoint: CatalogDeploymentFailpoint,
    ) -> Result<(), CatalogFailpointTriggered> {
        Ok(())
    }
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

impl Observability {
    fn observe_hot_conflict_key(&self, event: &ConflictEvent) {
        let mut hot = self
            .hot_conflict_keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let hash = event.conflict_key_hash();
        if event.queue_depth() == 0 {
            hot.remove(&hash);
        } else if !hot.contains(&hash) {
            if hot.len() == MAX_HOT_CONFLICT_KEYS {
                drop(hot);
                self.metrics.increment(MetricKey::TelemetryDropped);
                return;
            }
            hot.insert(hash);
        }
        self.metrics.set_required_gauge(
            RequiredGauge::HotConflictKeyCardinality,
            u64::try_from(hot.len()).unwrap_or(u64::MAX),
        );
    }
}
