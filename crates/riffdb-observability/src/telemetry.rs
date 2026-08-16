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
    CommandPipelineStage, CommitGroupDispatchReason, CommitIdempotencyObservation, CommitTelemetry,
    CommitTelemetryEvent, CommitUncertaintyResolution, CommitUncertaintyStage,
};
use riffdb_conflict::{ConflictEvent, ConflictObserver};
use riffdb_errors::{IncidentIdSource, InternalError};
use riffdb_policy::{AuthorizationTelemetry, AuthorizationTelemetryEvent};
use riffdb_service::{
    AuthoritativeReadinessFailure, ReadPipelineStage, ServiceDiagnostics, ServiceHealthHooks,
    ServiceTelemetry, ServiceTelemetryEvent, WriteServiceStage,
};
use riffdb_types::{ConflictKeyHash, IncidentId};

use crate::{
    HISTOGRAM_UPPER_BOUNDS, HealthRegistry, HistogramSnapshot, MetricKey, MetricLabel,
    MetricRegistry, READ_PIPELINE_STAGE_COUNT, RequiredCounter, RequiredGauge, RequiredHistogram,
    TraceCollector, TraceRecord, WRITE_SERVICE_STAGE_COUNT, read_pipeline_stage_index,
    write_service_stage_index,
};

/// Maximum redacted incidents retained for operator correlation.
pub const MAX_RETAINED_INCIDENTS: usize = 256;
/// Maximum distinct queued conflict-key hashes retained for hot-key cardinality.
pub const MAX_HOT_CONFLICT_KEYS: usize = 1_024;
/// Closed production completion-group sizes.
pub const MAX_WRITE_GROUP_SIZE: usize = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;
/// Closed scheduler dispatch-reason cardinality.
pub const COMMAND_GROUP_DISPATCH_REASON_COUNT: usize = 4;
/// Closed coordinator command-stage cardinality.
pub const COMMAND_PIPELINE_STAGE_COUNT: usize = 5;

const COMMAND_PIPELINE_STAGES: [(CommandPipelineStage, &str); COMMAND_PIPELINE_STAGE_COUNT] = [
    (CommandPipelineStage::Admission, "admission"),
    (CommandPipelineStage::Compatibility, "compatibility"),
    (CommandPipelineStage::Evaluation, "evaluation"),
    (
        CommandPipelineStage::ValidationEncodingStaging,
        "validation_encoding_staging",
    ),
    (CommandPipelineStage::Publication, "publication"),
];

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
    command_group_deferred_max: AtomicU64,
    compatibility_selected_total: AtomicU64,
    compatibility_group_total: AtomicU64,
    compatibility_conflict_key_split_total: AtomicU64,
    compatibility_exact_access_split_total: AtomicU64,
    compatibility_commutative_shared_group_total: AtomicU64,
    prepared_epoch_rollbacks: AtomicU64,
    prepared_epoch_proof_mismatches: AtomicU64,
    frontier_equivalence_checks: AtomicU64,
    frontier_equivalence_failures: AtomicU64,
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
            command_group_deferred_max: AtomicU64::new(0),
            compatibility_selected_total: AtomicU64::new(0),
            compatibility_group_total: AtomicU64::new(0),
            compatibility_conflict_key_split_total: AtomicU64::new(0),
            compatibility_exact_access_split_total: AtomicU64::new(0),
            compatibility_commutative_shared_group_total: AtomicU64::new(0),
            prepared_epoch_rollbacks: AtomicU64::new(0),
            prepared_epoch_proof_mismatches: AtomicU64::new(0),
            frontier_equivalence_checks: AtomicU64::new(0),
            frontier_equivalence_failures: AtomicU64::new(0),
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

    /// Returns exact successful completion-commit counts for sizes 1 through 256.
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

    /// Returns the complete fixed-cardinality writer evidence snapshot.
    #[must_use]
    pub fn writer_evidence_snapshot(&self) -> WriterEvidenceSnapshotV1 {
        WriterEvidenceSnapshotV1 {
            writer_busy_us: self
                .metrics
                .required_counter(RequiredCounter::WriterBusyMicroseconds),
            writer_idle_us: self
                .metrics
                .required_counter(RequiredCounter::WriterIdleMicroseconds),
            dispatch_selected: self.command_group_selected_total.load(Ordering::Relaxed),
            dispatch_deferred: self.command_group_deferred_total.load(Ordering::Relaxed),
            dispatch_deferred_max: self.command_group_deferred_max.load(Ordering::Relaxed),
            compatibility_selected: self.compatibility_selected_total.load(Ordering::Relaxed),
            compatibility_groups: self.compatibility_group_total.load(Ordering::Relaxed),
            compatibility_conflict_key_splits: self
                .compatibility_conflict_key_split_total
                .load(Ordering::Relaxed),
            compatibility_exact_access_splits: self
                .compatibility_exact_access_split_total
                .load(Ordering::Relaxed),
            compatibility_commutative_shared_groups: self
                .compatibility_commutative_shared_group_total
                .load(Ordering::Relaxed),
            queue_delay_estimate_us: self
                .metrics
                .required_gauge(RequiredGauge::CommandQueueDelayEstimateMicroseconds),
            commit_duration_us: self
                .metrics
                .required_histogram(RequiredHistogram::CommitDurationMicroseconds),
            durable_flush_duration_us: self
                .metrics
                .required_histogram(RequiredHistogram::DurableFlushDurationMicroseconds),
            commit_batch_size: self
                .metrics
                .required_histogram(RequiredHistogram::CommitBatchSize),
            storage_queue_duration_us: self
                .metrics
                .required_histogram(RequiredHistogram::StorageQueueLatencyMicroseconds),
            command_application_duration_us: self.metrics.command_application_duration(),
            command_submission_duration_us: self.metrics.command_submission_duration(),
            preparation_pool_depth: self.metrics.preparation_pool_depth(),
            reorder_buffer_occupancy: self.metrics.reorder_buffer_occupancy(),
            prepared_epoch_rollbacks: self.prepared_epoch_rollbacks.load(Ordering::Relaxed),
            prepared_epoch_proof_mismatches: self
                .prepared_epoch_proof_mismatches
                .load(Ordering::Relaxed),
            frontier_equivalence_checks: self.frontier_equivalence_checks.load(Ordering::Relaxed),
            frontier_equivalence_failures: self
                .frontier_equivalence_failures
                .load(Ordering::Relaxed),
        }
    }

    /// Returns per-stage read-pipeline histogram snapshots in [`ReadPipelineStage::ALL`] order.
    ///
    /// Each entry is `(count, sum_us, cumulative_buckets)`.
    #[must_use]
    pub fn read_stage_snapshot(
        &self,
    ) -> [(u64, u64, [u64; HISTOGRAM_UPPER_BOUNDS.len()]); READ_PIPELINE_STAGE_COUNT] {
        std::array::from_fn(|index| {
            let stage = ReadPipelineStage::ALL[index];
            debug_assert_eq!(read_pipeline_stage_index(stage), index);
            let snapshot = self.metrics.read_stage_duration(stage);
            (snapshot.count, snapshot.sum, snapshot.cumulative_buckets)
        })
    }

    /// Returns per-stage mutating-command service histograms in stable order.
    #[must_use]
    pub fn write_service_stage_snapshot(
        &self,
    ) -> [(u64, u64, [u64; HISTOGRAM_UPPER_BOUNDS.len()]); WRITE_SERVICE_STAGE_COUNT] {
        std::array::from_fn(|index| {
            let stage = WriteServiceStage::ALL[index];
            debug_assert_eq!(write_service_stage_index(stage), index);
            let snapshot = self.metrics.write_service_stage_duration(stage);
            (snapshot.count, snapshot.sum, snapshot.cumulative_buckets)
        })
    }

    /// Returns per-stage command-pipeline histogram snapshots in closed order.
    #[must_use]
    pub fn command_stage_snapshot(
        &self,
    ) -> [(u64, u64, [u64; HISTOGRAM_UPPER_BOUNDS.len()]); COMMAND_PIPELINE_STAGE_COUNT] {
        std::array::from_fn(|index| {
            let stage = COMMAND_PIPELINE_STAGES[index].0;
            let snapshot = self.metrics.command_stage_duration(stage);
            (snapshot.count, snapshot.sum, snapshot.cumulative_buckets)
        })
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
            ServiceTelemetryEvent::CursorEvicted => {
                self.metrics.increment(MetricKey::ServiceCursorEvicted);
                return;
            }
            ServiceTelemetryEvent::ReadRetryAttempt { .. } => {
                self.metrics.increment(MetricKey::ServiceReadRetryAttempt);
                return;
            }
            ServiceTelemetryEvent::ReadRetryExhausted { .. } => {
                self.metrics.increment(MetricKey::ServiceReadRetryExhausted);
                return;
            }
            ServiceTelemetryEvent::CapacityRejected { stage, .. } => {
                // Stage-only aggregate counter (bounded cardinality). There is
                // no operation-level capacity metric: ServiceOperationTerminal
                // is keyed only by terminal class. Operation/ingress live on
                // the per-request trace path only.
                self.metrics
                    .increment(MetricKey::ServiceCapacityRejected(stage));
                return;
            }
            ServiceTelemetryEvent::ReadPipelineStageCompleted { stage, elapsed } => {
                self.metrics
                    .observe_read_stage_duration(stage, duration_micros(elapsed));
                return;
            }
            ServiceTelemetryEvent::WriteServiceStageCompleted { stage, elapsed } => {
                self.metrics
                    .observe_write_service_stage_duration(stage, duration_micros(elapsed));
                return;
            }
        };
        self.metrics.increment(metric);
        self.record_trace(trace);
    }
}

/// Renders the process-shutdown evidence line for service-side read stages.
///
/// Format:
/// `riffdb-read-stages-v1\t<stage>:<count>:<sum_us>:<b0,...,b15>;...`
#[must_use]
pub fn format_read_stages_v1_line(
    snapshot: &[(u64, u64, [u64; HISTOGRAM_UPPER_BOUNDS.len()]); READ_PIPELINE_STAGE_COUNT],
) -> String {
    let mut parts = Vec::with_capacity(READ_PIPELINE_STAGE_COUNT);
    for (index, (count, sum_us, buckets)) in snapshot.iter().enumerate() {
        let stage = ReadPipelineStage::ALL[index];
        let buckets = buckets
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!(
            "{}:{count}:{sum_us}:{buckets}",
            stage.metric_label()
        ));
    }
    format!("riffdb-read-stages-v1\t{}", parts.join(";"))
}

/// Renders fixed-cardinality mutating-command service-stage shutdown evidence.
///
/// Format mirrors read-stage evidence and carries no command identity or value.
#[must_use]
pub fn format_write_service_stages_v1_line(
    snapshot: &[(u64, u64, [u64; HISTOGRAM_UPPER_BOUNDS.len()]); WRITE_SERVICE_STAGE_COUNT],
) -> String {
    let mut parts = Vec::with_capacity(WRITE_SERVICE_STAGE_COUNT);
    for (index, (count, sum_us, buckets)) in snapshot.iter().enumerate() {
        let bucket_text = buckets
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!(
            "{}:{count}:{sum_us}:{bucket_text}",
            WriteServiceStage::ALL[index].metric_label()
        ));
    }
    format!("riffdb-write-service-stages-v1\t{}", parts.join(";"))
}

/// Renders the process-shutdown evidence line for coordinator command stages.
#[must_use]
pub fn format_command_stages_v1_line(
    snapshot: &[(u64, u64, [u64; HISTOGRAM_UPPER_BOUNDS.len()]); COMMAND_PIPELINE_STAGE_COUNT],
) -> String {
    let mut parts = Vec::with_capacity(COMMAND_PIPELINE_STAGE_COUNT);
    for (index, (count, sum_us, buckets)) in snapshot.iter().enumerate() {
        let buckets = buckets
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!(
            "{}:{count}:{sum_us}:{buckets}",
            COMMAND_PIPELINE_STAGES[index].1
        ));
    }
    format!("riffdb-command-stages-v1\t{}", parts.join(";"))
}

/// Complete fixed-cardinality writer evidence captured from one process generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterEvidenceSnapshotV1 {
    /// Cumulative writer-unit busy time.
    pub writer_busy_us: u64,
    /// Cumulative writer-unit idle time.
    pub writer_idle_us: u64,
    /// Commands selected by intake groups.
    pub dispatch_selected: u64,
    /// Messages left behind after intake selection, summed at dispatch edges.
    pub dispatch_deferred: u64,
    /// Largest exact deferred queue observed at a dispatch edge.
    pub dispatch_deferred_max: u64,
    /// Commands presented to exact compatibility partitioning.
    pub compatibility_selected: u64,
    /// Compatible durable groups emitted by partitioning.
    pub compatibility_groups: u64,
    /// Boundaries caused by overlapping declared conflict keys.
    pub compatibility_conflict_key_splits: u64,
    /// Boundaries caused by exact entity read/write overlap.
    pub compatibility_exact_access_splits: u64,
    /// Groups admitted under the compiler-proved shared conflict lease.
    pub compatibility_commutative_shared_groups: u64,
    /// Latest bounded command-queue EWMA, absent before the first writer unit.
    pub queue_delay_estimate_us: Option<u64>,
    /// Storage commit-call duration.
    pub commit_duration_us: HistogramSnapshot,
    /// Durable flush duration under the configured profile.
    pub durable_flush_duration_us: HistogramSnapshot,
    /// Logical commands per successful physical commit call.
    pub commit_batch_size: HistogramSnapshot,
    /// Accepted command queue latency.
    pub storage_queue_duration_us: HistogramSnapshot,
    /// Final apply to writer-private authoritative state.
    pub command_application_duration_us: HistogramSnapshot,
    /// Final apply, frame encoding, and journal receipt creation duration.
    pub command_submission_duration_us: HistogramSnapshot,
    /// Bounded tasks queued or executing in preparation workers.
    pub preparation_pool_depth: HistogramSnapshot,
    /// Completed preparations waiting for an earlier admission ordinal.
    pub reorder_buffer_occupancy: HistogramSnapshot,
    /// Complete unpublished prepared epochs rolled back.
    pub prepared_epoch_rollbacks: u64,
    /// Rollbacks caused by a proof mismatch.
    pub prepared_epoch_proof_mismatches: u64,
    /// Private-frontier/application equivalence checks completed.
    pub frontier_equivalence_checks: u64,
    /// Private-frontier/application equivalence failures.
    pub frontier_equivalence_failures: u64,
}

/// Renders one process-generation writer evidence line.
///
/// The line is closed and numeric: it carries no command, tenant, key, or principal labels.
#[must_use]
pub fn format_writer_evidence_v1_line(snapshot: &WriterEvidenceSnapshotV1) -> String {
    let scalar = format!(
        "busy_us={};idle_us={};dispatch_selected={};dispatch_deferred={};dispatch_deferred_max={};compatibility_selected={};compatibility_groups={};compatibility_conflict_key_splits={};compatibility_exact_access_splits={};compatibility_commutative_shared_groups={};queue_delay_estimate_us={};prepared_epoch_rollbacks={};prepared_epoch_proof_mismatches={};frontier_equivalence_checks={};frontier_equivalence_failures={}",
        snapshot.writer_busy_us,
        snapshot.writer_idle_us,
        snapshot.dispatch_selected,
        snapshot.dispatch_deferred,
        snapshot.dispatch_deferred_max,
        snapshot.compatibility_selected,
        snapshot.compatibility_groups,
        snapshot.compatibility_conflict_key_splits,
        snapshot.compatibility_exact_access_splits,
        snapshot.compatibility_commutative_shared_groups,
        snapshot
            .queue_delay_estimate_us
            .map_or_else(|| "none".to_owned(), |value| value.to_string()),
        snapshot.prepared_epoch_rollbacks,
        snapshot.prepared_epoch_proof_mismatches,
        snapshot.frontier_equivalence_checks,
        snapshot.frontier_equivalence_failures,
    );
    let histograms = [
        ("commit_us", snapshot.commit_duration_us),
        ("flush_us", snapshot.durable_flush_duration_us),
        ("batch_size", snapshot.commit_batch_size),
        ("storage_queue_us", snapshot.storage_queue_duration_us),
        ("final_apply_us", snapshot.command_application_duration_us),
        ("journal_submit_us", snapshot.command_submission_duration_us),
        ("preparation_pool_depth", snapshot.preparation_pool_depth),
        (
            "reorder_buffer_occupancy",
            snapshot.reorder_buffer_occupancy,
        ),
    ]
    .map(|(name, histogram)| {
        let buckets = histogram
            .cumulative_buckets
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        format!("{name}:{}:{}:{buckets}", histogram.count, histogram.sum)
    })
    .join(";");
    format!("riffdb-writer-evidence-v1\t{scalar}\t{histograms}")
}

/// One parsed stage entry from a `riffdb-read-stages-v1` payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedReadStageV1 {
    /// Snake_case stage label.
    pub name: String,
    /// Observation count.
    pub count: u64,
    /// Saturating sum of observed microseconds.
    pub sum_us: u64,
    /// Cumulative fixed histogram buckets.
    pub buckets: Vec<u64>,
}

/// Parses one [`format_read_stages_v1_line`] payload after the prefix tab.
///
/// Returns per-stage entries in emission order.
pub fn parse_read_stages_v1_payload(payload: &str) -> Result<Vec<ParsedReadStageV1>, &'static str> {
    let mut stages = Vec::with_capacity(READ_PIPELINE_STAGE_COUNT);
    for part in payload.split(';') {
        let mut fields = part.splitn(4, ':');
        let name = fields.next().ok_or("missing stage name")?.to_owned();
        let count = fields
            .next()
            .ok_or("missing count")?
            .parse()
            .map_err(|_| "invalid count")?;
        let sum_us = fields
            .next()
            .ok_or("missing sum")?
            .parse()
            .map_err(|_| "invalid sum")?;
        let buckets = fields
            .next()
            .ok_or("missing buckets")?
            .split(',')
            .map(|value| value.parse().map_err(|_| "invalid bucket"))
            .collect::<Result<Vec<_>, _>>()?;
        if buckets.len() != HISTOGRAM_UPPER_BOUNDS.len() {
            return Err("bucket cardinality");
        }
        stages.push(ParsedReadStageV1 {
            name,
            count,
            sum_us,
            buckets,
        });
    }
    if stages.len() != READ_PIPELINE_STAGE_COUNT {
        return Err("stage cardinality");
    }
    Ok(stages)
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
            CommitTelemetryEvent::PreparationPoolDepthObserved { depth } => {
                self.metrics
                    .observe_preparation_pool_depth(u64::from(depth));
            }
            CommitTelemetryEvent::ReorderBufferOccupancyObserved { occupancy } => {
                self.metrics
                    .observe_reorder_buffer_occupancy(u64::from(occupancy));
            }
            CommitTelemetryEvent::PreparedEpochRolledBack { reason } => {
                saturating_increment(&self.prepared_epoch_rollbacks);
                if reason == riffdb_commit::PreparedEpochRollbackReason::ProofMismatch {
                    saturating_increment(&self.prepared_epoch_proof_mismatches);
                }
            }
            CommitTelemetryEvent::FrontierEquivalenceChecked { equivalent } => {
                saturating_increment(&self.frontier_equivalence_checks);
                if !equivalent {
                    saturating_increment(&self.frontier_equivalence_failures);
                }
            }
            CommitTelemetryEvent::CommandPipelineStageCompleted { stage, elapsed, .. } => {
                self.metrics
                    .observe_command_stage_duration(stage, duration_micros(elapsed));
            }
            CommitTelemetryEvent::CommandGroupDispatched {
                reason,
                selected,
                deferred,
                ..
            } => {
                let reason_index = command_group_dispatch_reason_index(reason);
                saturating_increment(&self.command_group_dispatch_reasons[reason_index]);
                saturating_add(&self.command_group_selected_total, u64::from(selected));
                saturating_add(&self.command_group_deferred_total, u64::from(deferred));
                self.command_group_deferred_max
                    .fetch_max(u64::from(deferred), Ordering::Relaxed);
                self.metrics.record_command_group_dispatch(
                    reason,
                    u64::from(selected),
                    u64::from(deferred),
                );
            }
            CommitTelemetryEvent::CommandGroupPartitioned {
                selected,
                completion_groups,
                conflict_key_splits,
                exact_access_splits,
                commutative_shared_groups,
            } => {
                saturating_add(&self.compatibility_selected_total, u64::from(selected));
                saturating_add(
                    &self.compatibility_group_total,
                    u64::from(completion_groups),
                );
                saturating_add(
                    &self.compatibility_conflict_key_split_total,
                    u64::from(conflict_key_splits),
                );
                saturating_add(
                    &self.compatibility_exact_access_split_total,
                    u64::from(exact_access_splits),
                );
                saturating_add(
                    &self.compatibility_commutative_shared_group_total,
                    u64::from(commutative_shared_groups),
                );
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
            } => {
                let elapsed = duration_micros(elapsed);
                self.metrics.observe_required_histogram(
                    RequiredHistogram::CommitDurationMicroseconds,
                    elapsed,
                );
                // Always record flush duration: production Group durability still
                // performs a durable commit; the previous `synchronous` gate
                // left this histogram empty under CoordinatorDurability::Group.
                self.metrics.observe_required_histogram(
                    RequiredHistogram::DurableFlushDurationMicroseconds,
                    elapsed,
                );
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
            CommitTelemetryEvent::CommitApplicationCompleted { elapsed, .. } => {
                self.metrics
                    .observe_command_application_duration(duration_micros(elapsed));
            }
            CommitTelemetryEvent::CommitSubmissionCompleted { elapsed, .. } => {
                self.metrics
                    .observe_command_submission_duration(duration_micros(elapsed));
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
            CommitTelemetryEvent::WriterUnitCompleted {
                busy,
                idle,
                queue_delay_estimate_micros,
            } => {
                self.metrics
                    .add_writer_busy_microseconds(duration_micros(busy));
                self.metrics
                    .add_writer_idle_microseconds(duration_micros(idle));
                self.metrics
                    .set_command_queue_delay_estimate_microseconds(queue_delay_estimate_micros);
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
