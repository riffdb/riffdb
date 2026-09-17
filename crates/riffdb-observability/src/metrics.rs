//! Fixed-cardinality process metrics.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use riffdb_types::{CommandId, ServiceIngressKindV1, ServiceOperationV1};

use crate::{
    AuthenticationDefect, AuthenticationRejection, AuthoritativeReadinessFailure,
    AuthorizationDefect, AuthorizationDenial, CapacityRejectionStage, CatalogTelemetryEvent,
    CommandPipelineStage, CommitCommandTerminal, CommitGroupDispatchReason, ConflictEventKind,
    IncidentClass, McpRiskClass, ReadPipelineStage, ServiceTerminalClass, WriteServiceStage,
};

const SERVICE_OPERATION_COUNT: usize = ServiceOperationV1::ALL.len();
const SERVICE_TERMINAL_COUNT: usize = ServiceTerminalClass::ALL.len();
const AUTH_REJECTION_COUNT: usize = 5;
const AUTH_DEFECT_COUNT: usize = 5;
const POLICY_CODE_COUNT: usize = AuthorizationDenial::ALL.len();
const POLICY_DEFECT_COUNT: usize = 2;
const CONFLICT_EVENT_COUNT: usize = 6;
const CATALOG_EVENT_COUNT: usize = 5;
const INCIDENT_CLASS_COUNT: usize = 5;
const READINESS_FAILURE_COUNT: usize = 3;

const SERVICE_AUDIT_OFFSET: usize = 0;
const SERVICE_INTEGRITY_OFFSET: usize = SERVICE_AUDIT_OFFSET + SERVICE_OPERATION_COUNT;
const SERVICE_CURSOR_OFFSET: usize = SERVICE_INTEGRITY_OFFSET + SERVICE_OPERATION_COUNT;
const SERVICE_STREAM_OFFSET: usize = SERVICE_CURSOR_OFFSET + 1;
const SERVICE_CURSOR_EVICTED_OFFSET: usize = SERVICE_STREAM_OFFSET + 1;
const SERVICE_READ_RETRY_ATTEMPT_OFFSET: usize = SERVICE_CURSOR_EVICTED_OFFSET + 1;
const SERVICE_READ_RETRY_EXHAUSTED_OFFSET: usize = SERVICE_READ_RETRY_ATTEMPT_OFFSET + 1;
const CAPACITY_REJECTION_STAGE_COUNT: usize = CapacityRejectionStage::ALL.len();
const SERVICE_CAPACITY_REJECTED_OFFSET: usize = SERVICE_READ_RETRY_EXHAUSTED_OFFSET + 1;
const SERVICE_TERMINAL_OFFSET: usize =
    SERVICE_CAPACITY_REJECTED_OFFSET + CAPACITY_REJECTION_STAGE_COUNT;
const AUTH_REJECTION_OFFSET: usize = SERVICE_TERMINAL_OFFSET + SERVICE_TERMINAL_COUNT;
const AUTH_DEFECT_OFFSET: usize = AUTH_REJECTION_OFFSET + AUTH_REJECTION_COUNT;
const POLICY_DENIAL_OFFSET: usize = AUTH_DEFECT_OFFSET + AUTH_DEFECT_COUNT;
const POLICY_DEFECT_OFFSET: usize = POLICY_DENIAL_OFFSET + POLICY_CODE_COUNT;
const CONFLICT_EVENT_OFFSET: usize = POLICY_DEFECT_OFFSET + POLICY_DEFECT_COUNT;
const CONFLICT_WAIT_MICROS_OFFSET: usize = CONFLICT_EVENT_OFFSET + CONFLICT_EVENT_COUNT;
const CONFLICT_QUEUE_DEPTH_OFFSET: usize = CONFLICT_WAIT_MICROS_OFFSET + 1;
const CATALOG_EVENT_OFFSET: usize = CONFLICT_QUEUE_DEPTH_OFFSET + 1;
const INCIDENT_OFFSET: usize = CATALOG_EVENT_OFFSET + CATALOG_EVENT_COUNT;
const INCIDENT_SOURCE_FAILURE_OFFSET: usize = INCIDENT_OFFSET + INCIDENT_CLASS_COUNT;
const READINESS_FAILURE_OFFSET: usize = INCIDENT_SOURCE_FAILURE_OFFSET + 1;
const TELEMETRY_DROPPED_OFFSET: usize = READINESS_FAILURE_OFFSET + READINESS_FAILURE_COUNT;
const UNPROVEN_CORRUPT_TARGET_BUMP_OFFSET: usize = TELEMETRY_DROPPED_OFFSET + 1;

/// Exact maximum number of metric series exported by the POC registry.
pub const MAX_METRIC_SERIES: usize = UNPROVEN_CORRUPT_TARGET_BUMP_OFFSET + 1;

/// Maximum distinct typed command metric dimensions retained in-process.
pub const MAX_COMMAND_METRIC_SERIES: usize = 1_024;

/// Fixed upper bounds for cumulative histogram buckets.
pub const HISTOGRAM_UPPER_BOUNDS: [u64; 16] = [
    0,
    1,
    2,
    4,
    8,
    16,
    32,
    64,
    128,
    256,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    u64::MAX,
];

/// Storage semantics of one required metric family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricSemantics {
    /// Monotonic saturating total.
    Counter,
    /// Cumulative fixed-bucket observation.
    Histogram,
    /// Replaceable current observation that may be absent.
    CurrentGauge,
}

/// Required metric family from `SPEC.md` section 16.5.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RequiredMetricFamily {
    /// Command request count by checked dimensions.
    CommandRequests,
    /// Command end-to-end duration.
    CommandLatencyMicroseconds,
    /// Time admitted work spent queued for storage.
    StorageQueueLatencyMicroseconds,
    /// Logical conflict-lock wait duration.
    LockWaitMicroseconds,
    /// Logical conflict-lock deadline count.
    LockTimeouts,
    /// Current bounded conflict queue depth.
    ConflictQueueDepth,
    /// Current bounded hot-key cardinality.
    HotConflictKeyCardinality,
    /// Durable application commit count.
    Commits,
    /// Durable application commit duration.
    CommitDurationMicroseconds,
    /// Number of commands in a durable batch.
    CommitBatchSize,
    /// Durable flush duration.
    DurableFlushDurationMicroseconds,
    /// Idempotency terminal replay hits.
    IdempotencyHits,
    /// Idempotency key/input mismatches.
    IdempotencyMismatches,
    /// Uncertain-result recovery count.
    IdempotencyUncertaintyRecoveries,
    /// Current active contract version.
    ActiveContractVersion,
    /// Durable contract activation count.
    ContractDeployments,
    /// Current pending outbox rows.
    OutboxPending,
    /// Outbox delivery attempt count.
    OutboxAttempts,
    /// Current age of the oldest pending outbox row.
    OutboxOldestPendingAgeMilliseconds,
    /// Current dead-letter row count.
    OutboxDeadLetters,
    /// Current projection frontier.
    ProjectionFrontier,
    /// Current projection lag in commits.
    ProjectionLagCommits,
    /// Projection rebuild count.
    ProjectionRebuilds,
    /// Projection closed error count.
    ProjectionErrors,
    /// Current MCP session count.
    McpSessions,
    /// MCP tool call count.
    McpToolCalls,
    /// MCP schema rejection count.
    McpSchemaFailures,
    /// MCP authorization denial count.
    McpAuthorizationDenials,
    /// MCP list-change notification count.
    McpListChangeNotifications,
    /// Current storage bytes.
    StorageBytes,
    /// Current authoritative commit-log record count.
    CommitLogRecords,
    /// Startup recovery duration.
    StartupRecoveryDurationMilliseconds,
    /// Command-group dispatch count by closed selection reason.
    CommandGroupDispatch,
    /// Commands selected into dispatched groups.
    CommandGroupSelected,
    /// Messages deferred after group selection.
    CommandGroupDeferred,
    /// Coordinator CPU stage duration by closed stage identity.
    CommandStageDurationMicroseconds,
    /// Service-side symbolic read pipeline stage duration by closed stage identity.
    ReadStageDurationMicroseconds,
    /// Cumulative time the pipelined writer spent executing units.
    WriterBusyMicroseconds,
    /// Cumulative time the pipelined writer spent idle between units.
    WriterIdleMicroseconds,
    /// EWMA estimate of coordinator queue delay observed by the writer.
    CommandQueueDelayEstimateMicroseconds,
}

/// Frozen name, semantics, and closed dimension names for a required family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequiredMetricDescriptor {
    /// Typed family identity.
    pub family: RequiredMetricFamily,
    /// Stable exporter name.
    pub name: &'static str,
    /// Counter, fixed histogram, or current gauge.
    pub semantics: MetricSemantics,
    /// Fixed label keys. Producers must supply typed, bounded values.
    pub label_keys: &'static [&'static str],
}

const NO_LABELS: &[&str] = &[];
const COMMAND_LABELS: &[&str] = &["command_id", "transport", "terminal_class"];
const PROJECTION_LABELS: &[&str] = &["projection_identity"];
const MCP_RISK_LABELS: &[&str] = &["risk_class"];
const DISPATCH_REASON_LABELS: &[&str] = &["reason"];
const COMMAND_STAGE_LABELS: &[&str] = &["stage"];
const READ_STAGE_LABELS: &[&str] = &["stage"];

/// Exact local inventory required before WP-185 composition.
pub const REQUIRED_METRIC_INVENTORY: [RequiredMetricDescriptor; 40] = [
    descriptor(
        RequiredMetricFamily::CommandRequests,
        "riffdb_command_requests_total",
        MetricSemantics::Counter,
        COMMAND_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::CommandLatencyMicroseconds,
        "riffdb_command_latency_microseconds",
        MetricSemantics::Histogram,
        COMMAND_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::StorageQueueLatencyMicroseconds,
        "riffdb_storage_queue_latency_microseconds",
        MetricSemantics::Histogram,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::LockWaitMicroseconds,
        "riffdb_lock_wait_microseconds",
        MetricSemantics::Histogram,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::LockTimeouts,
        "riffdb_lock_timeouts_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::ConflictQueueDepth,
        "riffdb_conflict_queue_depth",
        MetricSemantics::CurrentGauge,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::HotConflictKeyCardinality,
        "riffdb_hot_conflict_key_cardinality",
        MetricSemantics::CurrentGauge,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::Commits,
        "riffdb_commits_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::CommitDurationMicroseconds,
        "riffdb_commit_duration_microseconds",
        MetricSemantics::Histogram,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::CommitBatchSize,
        "riffdb_commit_batch_size",
        MetricSemantics::Histogram,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::DurableFlushDurationMicroseconds,
        "riffdb_durable_flush_duration_microseconds",
        MetricSemantics::Histogram,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::IdempotencyHits,
        "riffdb_idempotency_hits_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::IdempotencyMismatches,
        "riffdb_idempotency_mismatches_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::IdempotencyUncertaintyRecoveries,
        "riffdb_idempotency_uncertainty_recoveries_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::ActiveContractVersion,
        "riffdb_active_contract_version",
        MetricSemantics::CurrentGauge,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::ContractDeployments,
        "riffdb_contract_deployments_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::OutboxPending,
        "riffdb_outbox_pending",
        MetricSemantics::CurrentGauge,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::OutboxAttempts,
        "riffdb_outbox_attempts_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::OutboxOldestPendingAgeMilliseconds,
        "riffdb_outbox_oldest_pending_age_milliseconds",
        MetricSemantics::CurrentGauge,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::OutboxDeadLetters,
        "riffdb_outbox_dead_letters",
        MetricSemantics::CurrentGauge,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::ProjectionFrontier,
        "riffdb_projection_frontier",
        MetricSemantics::CurrentGauge,
        PROJECTION_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::ProjectionLagCommits,
        "riffdb_projection_lag_commits",
        MetricSemantics::CurrentGauge,
        PROJECTION_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::ProjectionRebuilds,
        "riffdb_projection_rebuilds_total",
        MetricSemantics::Counter,
        PROJECTION_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::ProjectionErrors,
        "riffdb_projection_errors_total",
        MetricSemantics::Counter,
        PROJECTION_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::McpSessions,
        "riffdb_mcp_sessions",
        MetricSemantics::CurrentGauge,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::McpToolCalls,
        "riffdb_mcp_tool_calls_total",
        MetricSemantics::Counter,
        MCP_RISK_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::McpSchemaFailures,
        "riffdb_mcp_schema_failures_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::McpAuthorizationDenials,
        "riffdb_mcp_authorization_denials_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::McpListChangeNotifications,
        "riffdb_mcp_list_change_notifications_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::StorageBytes,
        "riffdb_storage_bytes",
        MetricSemantics::CurrentGauge,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::CommitLogRecords,
        "riffdb_commit_log_records",
        MetricSemantics::CurrentGauge,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::StartupRecoveryDurationMilliseconds,
        "riffdb_startup_recovery_duration_milliseconds",
        MetricSemantics::Histogram,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::CommandGroupDispatch,
        "riffdb_command_group_dispatch_total",
        MetricSemantics::Counter,
        DISPATCH_REASON_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::CommandGroupSelected,
        "riffdb_command_group_selected_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::CommandGroupDeferred,
        "riffdb_command_group_deferred_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::CommandStageDurationMicroseconds,
        "riffdb_command_stage_duration_microseconds",
        MetricSemantics::Histogram,
        COMMAND_STAGE_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::ReadStageDurationMicroseconds,
        "riffdb_read_stage_duration_microseconds",
        MetricSemantics::Histogram,
        READ_STAGE_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::WriterBusyMicroseconds,
        "riffdb_writer_busy_microseconds_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::WriterIdleMicroseconds,
        "riffdb_writer_idle_microseconds_total",
        MetricSemantics::Counter,
        NO_LABELS,
    ),
    descriptor(
        RequiredMetricFamily::CommandQueueDelayEstimateMicroseconds,
        "riffdb_command_queue_delay_estimate_microseconds",
        MetricSemantics::CurrentGauge,
        NO_LABELS,
    ),
];

const fn descriptor(
    family: RequiredMetricFamily,
    name: &'static str,
    semantics: MetricSemantics,
    label_keys: &'static [&'static str],
) -> RequiredMetricDescriptor {
    RequiredMetricDescriptor {
        family,
        name,
        semantics,
        label_keys,
    }
}

/// One static metric label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricLabel {
    /// Closed label key.
    pub key: &'static str,
    /// Closed label value.
    pub value: &'static str,
}

/// One fixed-cardinality counter sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricSample {
    /// Stable metric family.
    pub name: &'static str,
    /// Zero, one, or two static labels.
    pub labels: [Option<MetricLabel>; 2],
    /// Saturating process-local value.
    pub value: u64,
}

/// Closed keys for every metric series.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricKey {
    /// Required audit was unavailable for an operation.
    ServiceAuditUnavailable(ServiceOperationV1),
    /// Service detected a closed integrity failure.
    ServiceInternalIntegrity(ServiceOperationV1),
    /// Cursor support failed closed.
    ServiceCursorUnavailable,
    /// Policy closed a live stream.
    ServiceStreamClosedByPolicy,
    /// A live cursor was evicted under capacity pressure.
    ServiceCursorEvicted,
    /// An internal read retry attempt after a transient failure.
    ServiceReadRetryAttempt,
    /// Internal read retry budget was exhausted.
    ServiceReadRetryExhausted,
    /// Command capacity admission rejected a request before accept.
    ServiceCapacityRejected(CapacityRejectionStage),
    /// One API-neutral operation reached a closed caller-visible disposition.
    ServiceOperationTerminal(ServiceTerminalClass),
    /// Initial credential authentication rejected.
    AuthenticationRejected(AuthenticationRejection),
    /// Initial credential authentication hit an internal defect.
    AuthenticationDefect(AuthenticationDefect),
    /// Current policy denied an operation.
    AuthorizationDenied(AuthorizationDenial),
    /// Current policy hit an internal defect.
    AuthorizationDefect(AuthorizationDefect),
    /// Conflict-manager lifecycle event.
    ConflictEvent(ConflictEventKind),
    /// Total observed conflict wait duration in microseconds.
    ConflictWaitMicroseconds,
    /// Sum of observed bounded queue depths.
    ConflictQueueDepth,
    /// Contract-catalog lifecycle event.
    CatalogEvent(CatalogTelemetryEvent),
    /// A redacted internal incident was retained.
    Incident(IncidentClass),
    /// The injected incident source failed.
    IncidentSourceFailure,
    /// Authoritative readiness was failed by an upstream hook.
    AuthoritativeReadinessFailure(AuthoritativeReadinessFailure),
    /// A bounded telemetry collector rejected an additional record.
    TelemetryDropped,
    /// Destructive restore bumped history incarnation without proving target
    /// monotonicity (corrupt/unreadable target with no retained or receipt floor).
    UnprovenCorruptTargetHistoryBump,
}

impl MetricKey {
    fn index(self) -> usize {
        match self {
            Self::ServiceAuditUnavailable(operation) => {
                SERVICE_AUDIT_OFFSET + service_operation_index(operation)
            }
            Self::ServiceInternalIntegrity(operation) => {
                SERVICE_INTEGRITY_OFFSET + service_operation_index(operation)
            }
            Self::ServiceCursorUnavailable => SERVICE_CURSOR_OFFSET,
            Self::ServiceStreamClosedByPolicy => SERVICE_STREAM_OFFSET,
            Self::ServiceCursorEvicted => SERVICE_CURSOR_EVICTED_OFFSET,
            Self::ServiceReadRetryAttempt => SERVICE_READ_RETRY_ATTEMPT_OFFSET,
            Self::ServiceReadRetryExhausted => SERVICE_READ_RETRY_EXHAUSTED_OFFSET,
            Self::ServiceCapacityRejected(stage) => {
                SERVICE_CAPACITY_REJECTED_OFFSET + capacity_rejection_stage_index(stage)
            }
            Self::ServiceOperationTerminal(terminal) => {
                SERVICE_TERMINAL_OFFSET + service_terminal_index(terminal)
            }
            Self::AuthenticationRejected(reason) => {
                AUTH_REJECTION_OFFSET + authentication_rejection_index(reason)
            }
            Self::AuthenticationDefect(reason) => {
                AUTH_DEFECT_OFFSET + authentication_defect_index(reason)
            }
            Self::AuthorizationDenied(code) => POLICY_DENIAL_OFFSET + usize::from(code.tag() - 1),
            Self::AuthorizationDefect(reason) => {
                POLICY_DEFECT_OFFSET + authorization_defect_index(reason)
            }
            Self::ConflictEvent(kind) => CONFLICT_EVENT_OFFSET + conflict_event_index(kind),
            Self::ConflictWaitMicroseconds => CONFLICT_WAIT_MICROS_OFFSET,
            Self::ConflictQueueDepth => CONFLICT_QUEUE_DEPTH_OFFSET,
            Self::CatalogEvent(event) => CATALOG_EVENT_OFFSET + catalog_event_index(event),
            Self::Incident(class) => INCIDENT_OFFSET + class.index(),
            Self::IncidentSourceFailure => INCIDENT_SOURCE_FAILURE_OFFSET,
            Self::AuthoritativeReadinessFailure(reason) => {
                READINESS_FAILURE_OFFSET + readiness_failure_index(reason)
            }
            Self::TelemetryDropped => TELEMETRY_DROPPED_OFFSET,
            Self::UnprovenCorruptTargetHistoryBump => UNPROVEN_CORRUPT_TARGET_BUMP_OFFSET,
        }
    }

    fn sample(self, value: u64) -> MetricSample {
        let none = [None, None];
        match self {
            Self::ServiceAuditUnavailable(operation) => MetricSample {
                name: "riffdb_service_events_total",
                labels: [
                    Some(MetricLabel {
                        key: "kind",
                        value: "audit_unavailable",
                    }),
                    Some(operation_label(operation)),
                ],
                value,
            },
            Self::ServiceInternalIntegrity(operation) => MetricSample {
                name: "riffdb_service_events_total",
                labels: [
                    Some(MetricLabel {
                        key: "kind",
                        value: "internal_integrity",
                    }),
                    Some(operation_label(operation)),
                ],
                value,
            },
            Self::ServiceCursorUnavailable => MetricSample {
                name: "riffdb_service_events_total",
                labels: [
                    Some(MetricLabel {
                        key: "kind",
                        value: "cursor_unavailable",
                    }),
                    None,
                ],
                value,
            },
            Self::ServiceStreamClosedByPolicy => MetricSample {
                name: "riffdb_service_events_total",
                labels: [
                    Some(MetricLabel {
                        key: "kind",
                        value: "stream_closed_by_policy",
                    }),
                    None,
                ],
                value,
            },
            Self::ServiceCursorEvicted => MetricSample {
                name: "riffdb_service_events_total",
                labels: [
                    Some(MetricLabel {
                        key: "kind",
                        value: "cursor_evicted",
                    }),
                    None,
                ],
                value,
            },
            Self::ServiceReadRetryAttempt => MetricSample {
                name: "riffdb_service_events_total",
                labels: [
                    Some(MetricLabel {
                        key: "kind",
                        value: "read_retry_attempt",
                    }),
                    None,
                ],
                value,
            },
            Self::ServiceReadRetryExhausted => MetricSample {
                name: "riffdb_service_events_total",
                labels: [
                    Some(MetricLabel {
                        key: "kind",
                        value: "read_retry_exhausted",
                    }),
                    None,
                ],
                value,
            },
            Self::ServiceCapacityRejected(stage) => MetricSample {
                name: "riffdb_service_events_total",
                labels: [
                    Some(MetricLabel {
                        key: "kind",
                        value: "capacity_rejected",
                    }),
                    Some(MetricLabel {
                        key: "stage",
                        value: capacity_rejection_stage_label(stage),
                    }),
                ],
                value,
            },
            Self::ServiceOperationTerminal(terminal) => MetricSample {
                name: "riffdb_service_operation_terminals_total",
                labels: [Some(service_terminal_label(terminal)), None],
                value,
            },
            Self::AuthenticationRejected(reason) => MetricSample {
                name: "riffdb_authentication_events_total",
                labels: [Some(authentication_rejection_label(reason)), None],
                value,
            },
            Self::AuthenticationDefect(reason) => MetricSample {
                name: "riffdb_authentication_defects_total",
                labels: [Some(authentication_defect_label(reason)), None],
                value,
            },
            Self::AuthorizationDenied(code) => MetricSample {
                name: "riffdb_authorization_denials_total",
                labels: [Some(policy_code_label(code)), None],
                value,
            },
            Self::AuthorizationDefect(reason) => MetricSample {
                name: "riffdb_authorization_defects_total",
                labels: [Some(authorization_defect_label(reason)), None],
                value,
            },
            Self::ConflictEvent(kind) => MetricSample {
                name: "riffdb_conflict_events_total",
                labels: [Some(conflict_event_label(kind)), None],
                value,
            },
            Self::ConflictWaitMicroseconds => MetricSample {
                name: "riffdb_conflict_wait_microseconds_total",
                labels: none,
                value,
            },
            Self::ConflictQueueDepth => MetricSample {
                name: "riffdb_conflict_queue_depth_total",
                labels: none,
                value,
            },
            Self::CatalogEvent(event) => MetricSample {
                name: "riffdb_catalog_events_total",
                labels: [Some(catalog_event_label(event)), None],
                value,
            },
            Self::Incident(class) => MetricSample {
                name: "riffdb_internal_incidents_total",
                labels: [Some(class.label()), None],
                value,
            },
            Self::IncidentSourceFailure => MetricSample {
                name: "riffdb_incident_source_failures_total",
                labels: none,
                value,
            },
            Self::AuthoritativeReadinessFailure(reason) => MetricSample {
                name: "riffdb_authoritative_readiness_failures_total",
                labels: [Some(readiness_failure_label(reason)), None],
                value,
            },
            Self::TelemetryDropped => MetricSample {
                name: "riffdb_telemetry_dropped_total",
                labels: none,
                value,
            },
            Self::UnprovenCorruptTargetHistoryBump => MetricSample {
                name: "riffdb_unproven_corrupt_target_history_bumps_total",
                labels: none,
                value,
            },
        }
    }
}

/// Counter families whose values may only increase.
///
/// Variant meanings are identical to the corresponding documented
/// [`RequiredMetricFamily`] variants.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredCounter {
    CommandRequests,
    LockTimeouts,
    Commits,
    IdempotencyHits,
    IdempotencyMismatches,
    IdempotencyUncertaintyRecoveries,
    ContractDeployments,
    OutboxAttempts,
    ProjectionRebuilds,
    ProjectionErrors,
    McpToolCalls,
    McpSchemaFailures,
    McpAuthorizationDenials,
    McpListChangeNotifications,
    CommandGroupDispatch,
    CommandGroupSelected,
    CommandGroupDeferred,
    WriterBusyMicroseconds,
    WriterIdleMicroseconds,
}

impl RequiredCounter {
    /// All required counters in stable storage order.
    pub const ALL: [Self; 19] = [
        Self::CommandRequests,
        Self::LockTimeouts,
        Self::Commits,
        Self::IdempotencyHits,
        Self::IdempotencyMismatches,
        Self::IdempotencyUncertaintyRecoveries,
        Self::ContractDeployments,
        Self::OutboxAttempts,
        Self::ProjectionRebuilds,
        Self::ProjectionErrors,
        Self::McpToolCalls,
        Self::McpSchemaFailures,
        Self::McpAuthorizationDenials,
        Self::McpListChangeNotifications,
        Self::CommandGroupDispatch,
        Self::CommandGroupSelected,
        Self::CommandGroupDeferred,
        Self::WriterBusyMicroseconds,
        Self::WriterIdleMicroseconds,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

/// Required fixed-bucket histogram families.
///
/// Variant meanings are identical to the corresponding documented
/// [`RequiredMetricFamily`] variants.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredHistogram {
    CommandLatencyMicroseconds,
    StorageQueueLatencyMicroseconds,
    LockWaitMicroseconds,
    CommitDurationMicroseconds,
    CommitBatchSize,
    DurableFlushDurationMicroseconds,
    StartupRecoveryDurationMilliseconds,
    CommandStageDurationMicroseconds,
    ReadStageDurationMicroseconds,
}

impl RequiredHistogram {
    /// All required histograms in stable storage order.
    pub const ALL: [Self; 9] = [
        Self::CommandLatencyMicroseconds,
        Self::StorageQueueLatencyMicroseconds,
        Self::LockWaitMicroseconds,
        Self::CommitDurationMicroseconds,
        Self::CommitBatchSize,
        Self::DurableFlushDurationMicroseconds,
        Self::StartupRecoveryDurationMilliseconds,
        Self::CommandStageDurationMicroseconds,
        Self::ReadStageDurationMicroseconds,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

/// Required current-gauge families.
///
/// Variant meanings are identical to the corresponding documented
/// [`RequiredMetricFamily`] variants.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredGauge {
    ConflictQueueDepth,
    HotConflictKeyCardinality,
    ActiveContractVersion,
    OutboxPending,
    OutboxOldestPendingAgeMilliseconds,
    OutboxDeadLetters,
    ProjectionFrontier,
    ProjectionLagCommits,
    McpSessions,
    StorageBytes,
    CommitLogRecords,
    CommandQueueDelayEstimateMicroseconds,
}

impl RequiredGauge {
    /// All required current gauges in stable storage order.
    pub const ALL: [Self; 12] = [
        Self::ConflictQueueDepth,
        Self::HotConflictKeyCardinality,
        Self::ActiveContractVersion,
        Self::OutboxPending,
        Self::OutboxOldestPendingAgeMilliseconds,
        Self::OutboxDeadLetters,
        Self::ProjectionFrontier,
        Self::ProjectionLagCommits,
        Self::McpSessions,
        Self::StorageBytes,
        Self::CommitLogRecords,
        Self::CommandQueueDelayEstimateMicroseconds,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

/// One immutable cumulative histogram observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistogramSnapshot {
    /// Number of observations.
    pub count: u64,
    /// Saturating sum of observed values.
    pub sum: u64,
    /// Cumulative counts corresponding to [`HISTOGRAM_UPPER_BOUNDS`].
    pub cumulative_buckets: [u64; HISTOGRAM_UPPER_BOUNDS.len()],
}

/// One typed, bounded command metric dimension set.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CommandMetricDimensions {
    command_id: CommandId,
    ingress: ServiceIngressKindV1,
    terminal: CommitCommandTerminal,
}

impl CommandMetricDimensions {
    /// Returns the checked command identity.
    #[must_use]
    pub const fn command_id(self) -> CommandId {
        self.command_id
    }

    /// Returns the trusted transport classification.
    #[must_use]
    pub const fn ingress(self) -> ServiceIngressKindV1 {
        self.ingress
    }

    /// Returns the closed coordinator disposition.
    #[must_use]
    pub const fn terminal(self) -> CommitCommandTerminal {
        self.terminal
    }
}

/// One immutable bounded command-series observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandMetricSnapshot {
    /// Typed dimensions; no text label is accepted from a producer.
    pub dimensions: CommandMetricDimensions,
    /// Saturating request total.
    pub requests: u64,
    /// Fixed-bucket latency observation.
    pub latency_microseconds: HistogramSnapshot,
}

struct FixedHistogram {
    count: AtomicU64,
    sum: AtomicU64,
    /// Per-bucket (non-cumulative) counts; cumulated only at snapshot time so
    /// one observation touches exactly one bucket. The final bound is
    /// `u64::MAX`, so every value has a bucket.
    bucket_counts: [AtomicU64; HISTOGRAM_UPPER_BOUNDS.len()],
}

/// Returns the first bucket whose upper bound covers the value.
const fn histogram_bucket_index(value: u64) -> usize {
    let mut index = 0;
    while index < HISTOGRAM_UPPER_BOUNDS.len() {
        if value <= HISTOGRAM_UPPER_BOUNDS[index] {
            return index;
        }
        index += 1;
    }
    HISTOGRAM_UPPER_BOUNDS.len() - 1
}

impl FixedHistogram {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            bucket_counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Hot path: three uncontended relaxed adds, no compare-and-swap loops.
    /// Plain wrapping adds are sound here: a `u64` of microseconds saturates
    /// after ~585,000 years of accumulated duration and the count after
    /// 1.8e19 observations — wrap is unreachable, and the previous
    /// saturating CAS loops were the dominant cost of the whole metrics
    /// surface under profile.
    fn observe(&self, value: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.bucket_counts[histogram_bucket_index(value)].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> HistogramSnapshot {
        let mut running = 0_u64;
        let mut cumulative = [0_u64; HISTOGRAM_UPPER_BOUNDS.len()];
        let mut index = 0;
        while index < HISTOGRAM_UPPER_BOUNDS.len() {
            running = running.saturating_add(self.bucket_counts[index].load(Ordering::Relaxed));
            cumulative[index] = running;
            index += 1;
        }
        HistogramSnapshot {
            count: self.count.load(Ordering::Relaxed),
            sum: self.sum.load(Ordering::Relaxed),
            cumulative_buckets: cumulative,
        }
    }
}

struct CommandMetricSeries {
    requests: u64,
    latency_microseconds: FixedHistogram,
}

impl CommandMetricSeries {
    fn new() -> Self {
        Self {
            requests: 0,
            latency_microseconds: FixedHistogram::new(),
        }
    }
}

const COMMAND_GROUP_DISPATCH_REASON_COUNT: usize = 4;
const COMMAND_PIPELINE_STAGE_COUNT: usize = 5;
/// Closed end-to-end symbolic read pipeline stage cardinality.
pub const READ_PIPELINE_STAGE_COUNT: usize = 15;
/// Closed mutating-command service-stage cardinality.
pub const WRITE_SERVICE_STAGE_COUNT: usize = 5;

struct MetricRegistryInner {
    counters: [AtomicU64; MAX_METRIC_SERIES],
    required_counters: [AtomicU64; RequiredCounter::ALL.len()],
    required_histograms: [FixedHistogram; RequiredHistogram::ALL.len()],
    required_gauges: Mutex<[Option<u64>; RequiredGauge::ALL.len()]>,
    command_series: Mutex<BTreeMap<CommandMetricDimensions, CommandMetricSeries>>,
    mcp_tool_calls: [AtomicU64; McpRiskClass::ALL.len()],
    command_group_dispatch_reasons: [AtomicU64; COMMAND_GROUP_DISPATCH_REASON_COUNT],
    command_group_residence_durations: FixedHistogram,
    command_stage_durations: [FixedHistogram; COMMAND_PIPELINE_STAGE_COUNT],
    read_stage_durations: [FixedHistogram; READ_PIPELINE_STAGE_COUNT],
    write_service_stage_durations: [FixedHistogram; WRITE_SERVICE_STAGE_COUNT],
    command_application_durations: FixedHistogram,
    command_submission_durations: FixedHistogram,
    preparation_pool_depths: FixedHistogram,
    reorder_buffer_occupancies: FixedHistogram,
}

/// Cloneable fixed-cardinality counter registry.
#[derive(Clone)]
pub struct MetricRegistry {
    inner: Arc<MetricRegistryInner>,
}

impl MetricRegistry {
    /// Creates a zeroed registry with no dynamic series allocation.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricRegistryInner {
                counters: std::array::from_fn(|_| AtomicU64::new(0)),
                required_counters: std::array::from_fn(|_| AtomicU64::new(0)),
                required_histograms: std::array::from_fn(|_| FixedHistogram::new()),
                required_gauges: Mutex::new([None; RequiredGauge::ALL.len()]),
                command_series: Mutex::new(BTreeMap::new()),
                mcp_tool_calls: std::array::from_fn(|_| AtomicU64::new(0)),
                command_group_dispatch_reasons: std::array::from_fn(|_| AtomicU64::new(0)),
                command_group_residence_durations: FixedHistogram::new(),
                command_stage_durations: std::array::from_fn(|_| FixedHistogram::new()),
                read_stage_durations: std::array::from_fn(|_| FixedHistogram::new()),
                write_service_stage_durations: std::array::from_fn(|_| FixedHistogram::new()),
                command_application_durations: FixedHistogram::new(),
                command_submission_durations: FixedHistogram::new(),
                preparation_pool_depths: FixedHistogram::new(),
                reorder_buffer_occupancies: FixedHistogram::new(),
            }),
        }
    }

    /// Saturating-increments one closed series.
    pub fn increment(&self, key: MetricKey) {
        self.add(key, 1);
    }

    /// Saturating-adds a nonnegative observation to one closed series.
    pub fn add(&self, key: MetricKey, amount: u64) {
        saturating_add(&self.inner.counters[key.index()], amount);
    }

    /// Returns one series value.
    #[must_use]
    pub fn value(&self, key: MetricKey) -> u64 {
        self.inner.counters[key.index()].load(Ordering::Relaxed)
    }

    /// Returns all fixed series in canonical registry order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<MetricSample> {
        metric_keys()
            .into_iter()
            .map(|key| key.sample(self.value(key)))
            .collect()
    }

    /// Saturating-increments one typed required counter.
    pub fn increment_required_counter(&self, counter: RequiredCounter) {
        saturating_add(&self.inner.required_counters[counter.index()], 1);
    }

    /// Returns one typed required counter.
    #[must_use]
    pub fn required_counter(&self, counter: RequiredCounter) -> u64 {
        self.inner.required_counters[counter.index()].load(Ordering::Relaxed)
    }

    /// Adds one nonnegative observation to a typed fixed histogram.
    pub fn observe_required_histogram(&self, histogram: RequiredHistogram, value: u64) {
        self.inner.required_histograms[histogram.index()].observe(value);
    }

    /// Returns a cumulative fixed-bucket histogram observation.
    #[must_use]
    pub fn required_histogram(&self, histogram: RequiredHistogram) -> HistogramSnapshot {
        self.inner.required_histograms[histogram.index()].snapshot()
    }

    /// Replaces the current value of one typed gauge.
    pub fn set_required_gauge(&self, gauge: RequiredGauge, value: u64) {
        self.lock_gauges()[gauge.index()] = Some(value);
    }

    /// Clears a current gauge when no trustworthy observation exists.
    pub fn clear_required_gauge(&self, gauge: RequiredGauge) {
        self.lock_gauges()[gauge.index()] = None;
    }

    /// Returns the current gauge value without fabricating zero for absence.
    #[must_use]
    pub fn required_gauge(&self, gauge: RequiredGauge) -> Option<u64> {
        self.lock_gauges()[gauge.index()]
    }

    /// Records one command request in the bounded typed dimension registry.
    ///
    /// Returns `false` only when a new dimension set would exceed the hard
    /// process cap. Existing series remain observable at capacity.
    pub fn observe_command(
        &self,
        command_id: CommandId,
        ingress: ServiceIngressKindV1,
        terminal: CommitCommandTerminal,
        latency_microseconds: u64,
    ) -> bool {
        let dimensions = CommandMetricDimensions {
            command_id,
            ingress,
            terminal,
        };
        let mut series = self
            .inner
            .command_series
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !series.contains_key(&dimensions) && series.len() == MAX_COMMAND_METRIC_SERIES {
            return false;
        }
        let entry = series
            .entry(dimensions)
            .or_insert_with(CommandMetricSeries::new);
        entry.requests = entry.requests.saturating_add(1);
        entry.latency_microseconds.observe(latency_microseconds);
        true
    }

    /// Returns command series in canonical typed-dimension order.
    #[must_use]
    pub fn command_snapshot(&self) -> Vec<CommandMetricSnapshot> {
        self.inner
            .command_series
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(dimensions, series)| CommandMetricSnapshot {
                dimensions: *dimensions,
                requests: series.requests,
                latency_microseconds: series.latency_microseconds.snapshot(),
            })
            .collect()
    }

    /// Increments one fixed MCP risk-class series and the aggregate counter.
    pub fn increment_mcp_tool_call(&self, risk: McpRiskClass) {
        saturating_add(&self.inner.mcp_tool_calls[mcp_risk_index(risk)], 1);
        self.increment_required_counter(RequiredCounter::McpToolCalls);
    }

    /// Returns one fixed MCP risk-class counter.
    #[must_use]
    pub fn mcp_tool_calls(&self, risk: McpRiskClass) -> u64 {
        self.inner.mcp_tool_calls[mcp_risk_index(risk)].load(Ordering::Relaxed)
    }

    /// Records one command-group dispatch observation with closed reason labels.
    pub fn record_command_group_dispatch(
        &self,
        reason: CommitGroupDispatchReason,
        selected: u64,
        deferred: u64,
    ) {
        saturating_add(
            &self.inner.command_group_dispatch_reasons[command_group_dispatch_reason_index(reason)],
            1,
        );
        self.increment_required_counter(RequiredCounter::CommandGroupDispatch);
        saturating_add(
            &self.inner.required_counters[RequiredCounter::CommandGroupSelected.index()],
            selected,
        );
        saturating_add(
            &self.inner.required_counters[RequiredCounter::CommandGroupDeferred.index()],
            deferred,
        );
    }

    /// Observes time from receiving the oldest groupable item through dispatch.
    pub fn observe_command_group_residence_duration(&self, value: u64) {
        self.inner.command_group_residence_durations.observe(value);
    }

    /// Returns the fixed command-group formation histogram.
    #[must_use]
    pub fn command_group_residence_duration(&self) -> HistogramSnapshot {
        self.inner.command_group_residence_durations.snapshot()
    }

    /// Returns dispatch counts in full, barrier, queue-drained, receiver-closed order.
    #[must_use]
    pub fn command_group_dispatch_reasons(&self) -> [u64; COMMAND_GROUP_DISPATCH_REASON_COUNT] {
        std::array::from_fn(|index| {
            self.inner.command_group_dispatch_reasons[index].load(Ordering::Relaxed)
        })
    }

    /// Observes one coordinator CPU stage duration under its closed stage label.
    pub fn observe_command_stage_duration(&self, stage: CommandPipelineStage, value: u64) {
        self.inner.command_stage_durations[command_pipeline_stage_index(stage)].observe(value);
        self.observe_required_histogram(RequiredHistogram::CommandStageDurationMicroseconds, value);
    }

    /// Returns the fixed histogram for one closed pipeline stage.
    #[must_use]
    pub fn command_stage_duration(&self, stage: CommandPipelineStage) -> HistogramSnapshot {
        self.inner.command_stage_durations[command_pipeline_stage_index(stage)].snapshot()
    }

    /// Observes one service-side read pipeline stage duration under its closed stage label.
    pub fn observe_read_stage_duration(&self, stage: ReadPipelineStage, value: u64) {
        self.inner.read_stage_durations[read_pipeline_stage_index(stage)].observe(value);
        self.observe_required_histogram(RequiredHistogram::ReadStageDurationMicroseconds, value);
    }

    /// Returns the fixed histogram for one closed read pipeline stage.
    #[must_use]
    pub fn read_stage_duration(&self, stage: ReadPipelineStage) -> HistogramSnapshot {
        self.inner.read_stage_durations[read_pipeline_stage_index(stage)].snapshot()
    }

    /// Observes one non-overlapping mutating-command service stage.
    pub fn observe_write_service_stage_duration(&self, stage: WriteServiceStage, value: u64) {
        self.inner.write_service_stage_durations[write_service_stage_index(stage)].observe(value);
    }

    /// Returns the fixed histogram for one mutating-command service stage.
    #[must_use]
    pub fn write_service_stage_duration(&self, stage: WriteServiceStage) -> HistogramSnapshot {
        self.inner.write_service_stage_durations[write_service_stage_index(stage)].snapshot()
    }

    /// Observes final apply to writer-private authoritative state.
    pub fn observe_command_application_duration(&self, value: u64) {
        self.inner.command_application_durations.observe(value);
    }

    /// Returns final-apply duration.
    #[must_use]
    pub fn command_application_duration(&self) -> HistogramSnapshot {
        self.inner.command_application_durations.snapshot()
    }

    /// Observes final apply through deferred-journal receipt creation.
    pub fn observe_command_submission_duration(&self, value: u64) {
        self.inner.command_submission_durations.observe(value);
    }

    /// Returns final apply through deferred-journal receipt duration.
    #[must_use]
    pub fn command_submission_duration(&self) -> HistogramSnapshot {
        self.inner.command_submission_durations.snapshot()
    }

    /// Observes bounded preparation-pool depth.
    pub fn observe_preparation_pool_depth(&self, value: u64) {
        self.inner.preparation_pool_depths.observe(value);
    }

    /// Returns bounded preparation-pool depth observations.
    #[must_use]
    pub fn preparation_pool_depth(&self) -> HistogramSnapshot {
        self.inner.preparation_pool_depths.snapshot()
    }

    /// Observes bounded admission-ordinal reorder-buffer occupancy.
    pub fn observe_reorder_buffer_occupancy(&self, value: u64) {
        self.inner.reorder_buffer_occupancies.observe(value);
    }

    /// Returns bounded reorder-buffer occupancy observations.
    #[must_use]
    pub fn reorder_buffer_occupancy(&self) -> HistogramSnapshot {
        self.inner.reorder_buffer_occupancies.snapshot()
    }

    /// Adds busy time spent on the pipelined writer thread.
    pub fn add_writer_busy_microseconds(&self, micros: u64) {
        saturating_add(
            &self.inner.required_counters[RequiredCounter::WriterBusyMicroseconds.index()],
            micros,
        );
    }

    /// Adds idle time between units on the pipelined writer thread.
    pub fn add_writer_idle_microseconds(&self, micros: u64) {
        saturating_add(
            &self.inner.required_counters[RequiredCounter::WriterIdleMicroseconds.index()],
            micros,
        );
    }

    /// Publishes the writer's EWMA queue-delay estimate as a current gauge.
    pub fn set_command_queue_delay_estimate_microseconds(&self, micros: u64) {
        self.set_required_gauge(RequiredGauge::CommandQueueDelayEstimateMicroseconds, micros);
    }

    /// Applies a signed delta to a present-or-zero current gauge.
    pub fn adjust_required_gauge(&self, gauge: RequiredGauge, delta: i64) {
        let slot = &mut self.lock_gauges()[gauge.index()];
        let current = slot.unwrap_or(0);
        *slot = Some(if delta >= 0 {
            current.saturating_add(delta.unsigned_abs())
        } else {
            current.saturating_sub(delta.unsigned_abs())
        });
    }

    fn lock_gauges(&self) -> MutexGuard<'_, [Option<u64>; RequiredGauge::ALL.len()]> {
        self.inner
            .required_gauges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for MetricRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MetricRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MetricRegistry([REDACTED])")
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

const fn command_pipeline_stage_index(stage: CommandPipelineStage) -> usize {
    match stage {
        CommandPipelineStage::Admission => 0,
        CommandPipelineStage::Compatibility => 1,
        CommandPipelineStage::Evaluation => 2,
        CommandPipelineStage::ValidationEncodingStaging => 3,
        CommandPipelineStage::Publication => 4,
    }
}

/// Stable registry index for one closed read pipeline stage.
#[must_use]
pub const fn read_pipeline_stage_index(stage: ReadPipelineStage) -> usize {
    match stage {
        ReadPipelineStage::TransportAdapt => 0,
        ReadPipelineStage::Authn => 1,
        ReadPipelineStage::AdmissionContext => 2,
        ReadPipelineStage::SpawnDispatch => 3,
        ReadPipelineStage::PlanLookup => 4,
        ReadPipelineStage::ParamMaterialize => 5,
        ReadPipelineStage::AuthorizeBegin => 6,
        ReadPipelineStage::AuthorizePre => 7,
        ReadPipelineStage::Execute => 8,
        ReadPipelineStage::AuthorizePost => 9,
        ReadPipelineStage::ResponseBuild => 10,
        ReadPipelineStage::EncodeConvert => 11,
        ReadPipelineStage::AuditFinish => 12,
        ReadPipelineStage::ServiceAwait => 13,
        ReadPipelineStage::ServerHandler => 14,
    }
}

/// Stable registry index for one closed mutating-command service stage.
#[must_use]
pub const fn write_service_stage_index(stage: WriteServiceStage) -> usize {
    match stage {
        WriteServiceStage::TransportAdapt => 0,
        WriteServiceStage::ServicePrepare => 1,
        WriteServiceStage::CoordinatorAwait => 2,
        WriteServiceStage::ServiceFinish => 3,
        WriteServiceStage::EncodeConvert => 4,
    }
}

fn metric_keys() -> Vec<MetricKey> {
    let mut keys = Vec::with_capacity(MAX_METRIC_SERIES);
    keys.extend(
        ServiceOperationV1::ALL
            .into_iter()
            .map(MetricKey::ServiceAuditUnavailable),
    );
    keys.extend(
        ServiceOperationV1::ALL
            .into_iter()
            .map(MetricKey::ServiceInternalIntegrity),
    );
    keys.push(MetricKey::ServiceCursorUnavailable);
    keys.push(MetricKey::ServiceStreamClosedByPolicy);
    keys.push(MetricKey::ServiceCursorEvicted);
    keys.push(MetricKey::ServiceReadRetryAttempt);
    keys.push(MetricKey::ServiceReadRetryExhausted);
    keys.extend(
        CapacityRejectionStage::ALL
            .into_iter()
            .map(MetricKey::ServiceCapacityRejected),
    );
    keys.extend(
        ServiceTerminalClass::ALL
            .into_iter()
            .map(MetricKey::ServiceOperationTerminal),
    );
    keys.extend(authentication_rejections().map(MetricKey::AuthenticationRejected));
    keys.extend(authentication_defects().map(MetricKey::AuthenticationDefect));
    keys.extend(
        AuthorizationDenial::ALL
            .into_iter()
            .map(MetricKey::AuthorizationDenied),
    );
    keys.extend(authorization_defects().map(MetricKey::AuthorizationDefect));
    keys.extend(conflict_events().map(MetricKey::ConflictEvent));
    keys.push(MetricKey::ConflictWaitMicroseconds);
    keys.push(MetricKey::ConflictQueueDepth);
    keys.extend(catalog_events().map(MetricKey::CatalogEvent));
    keys.extend(IncidentClass::ALL.into_iter().map(MetricKey::Incident));
    keys.push(MetricKey::IncidentSourceFailure);
    keys.extend(readiness_failures().map(MetricKey::AuthoritativeReadinessFailure));
    keys.push(MetricKey::TelemetryDropped);
    keys.push(MetricKey::UnprovenCorruptTargetHistoryBump);
    debug_assert_eq!(keys.len(), MAX_METRIC_SERIES);
    keys
}

const fn service_operation_index(operation: ServiceOperationV1) -> usize {
    (operation.tag() - 1) as usize
}

const fn capacity_rejection_stage_index(stage: CapacityRejectionStage) -> usize {
    match stage {
        CapacityRejectionStage::QueueDepth => 0,
        CapacityRejectionStage::RetainedBytes => 1,
    }
}

const fn capacity_rejection_stage_label(stage: CapacityRejectionStage) -> &'static str {
    match stage {
        CapacityRejectionStage::QueueDepth => "queue_depth",
        CapacityRejectionStage::RetainedBytes => "retained_bytes",
    }
}

fn service_terminal_index(terminal: ServiceTerminalClass) -> usize {
    ServiceTerminalClass::ALL
        .iter()
        .position(|candidate| *candidate == terminal)
        .expect("closed service terminal class")
}

const fn authentication_rejection_index(reason: AuthenticationRejection) -> usize {
    match reason {
        AuthenticationRejection::MalformedCredential => 0,
        AuthenticationRejection::NoMatch => 1,
        AuthenticationRejection::InactiveCapability => 2,
        AuthenticationRejection::BoundaryMismatch => 3,
        AuthenticationRejection::OutsideValidityInterval => 4,
    }
}

const fn authentication_defect_index(reason: AuthenticationDefect) -> usize {
    match reason {
        AuthenticationDefect::ClockUnavailable => 0,
        AuthenticationDefect::RepositoryUnavailable => 1,
        AuthenticationDefect::RepositoryIntegrity => 2,
        AuthenticationDefect::MultipleMatches => 3,
        AuthenticationDefect::ReciprocalLinkMismatch => 4,
    }
}

const fn authorization_defect_index(reason: AuthorizationDefect) -> usize {
    match reason {
        AuthorizationDefect::CurrentCapabilityUnavailable => 0,
        AuthorizationDefect::ClockUnavailable => 1,
    }
}

const fn conflict_event_index(kind: ConflictEventKind) -> usize {
    match kind {
        ConflictEventKind::Queued => 0,
        ConflictEventKind::Acquired => 1,
        ConflictEventKind::Cancelled => 2,
        ConflictEventKind::DeadlineExceeded => 3,
        ConflictEventKind::Released => 4,
        ConflictEventKind::CapacityRejected => 5,
    }
}

const fn catalog_event_index(event: CatalogTelemetryEvent) -> usize {
    match event {
        CatalogTelemetryEvent::ActivationPrepared => 0,
        CatalogTelemetryEvent::ActivationDurable => 1,
        CatalogTelemetryEvent::NotificationDelivered => 2,
        CatalogTelemetryEvent::NotificationFailed => 3,
        CatalogTelemetryEvent::NoCatalogChange => 4,
    }
}

const fn readiness_failure_index(reason: AuthoritativeReadinessFailure) -> usize {
    match reason {
        AuthoritativeReadinessFailure::AuditUnavailable => 0,
        AuthoritativeReadinessFailure::CoordinatorFenced => 1,
        AuthoritativeReadinessFailure::Integrity => 2,
    }
}

fn authentication_rejections() -> impl Iterator<Item = AuthenticationRejection> {
    [
        AuthenticationRejection::MalformedCredential,
        AuthenticationRejection::NoMatch,
        AuthenticationRejection::InactiveCapability,
        AuthenticationRejection::BoundaryMismatch,
        AuthenticationRejection::OutsideValidityInterval,
    ]
    .into_iter()
}

fn authentication_defects() -> impl Iterator<Item = AuthenticationDefect> {
    [
        AuthenticationDefect::ClockUnavailable,
        AuthenticationDefect::RepositoryUnavailable,
        AuthenticationDefect::RepositoryIntegrity,
        AuthenticationDefect::MultipleMatches,
        AuthenticationDefect::ReciprocalLinkMismatch,
    ]
    .into_iter()
}

fn authorization_defects() -> impl Iterator<Item = AuthorizationDefect> {
    [
        AuthorizationDefect::CurrentCapabilityUnavailable,
        AuthorizationDefect::ClockUnavailable,
    ]
    .into_iter()
}

fn conflict_events() -> impl Iterator<Item = ConflictEventKind> {
    [
        ConflictEventKind::Queued,
        ConflictEventKind::Acquired,
        ConflictEventKind::Cancelled,
        ConflictEventKind::DeadlineExceeded,
        ConflictEventKind::Released,
        ConflictEventKind::CapacityRejected,
    ]
    .into_iter()
}

fn catalog_events() -> impl Iterator<Item = CatalogTelemetryEvent> {
    [
        CatalogTelemetryEvent::ActivationPrepared,
        CatalogTelemetryEvent::ActivationDurable,
        CatalogTelemetryEvent::NotificationDelivered,
        CatalogTelemetryEvent::NotificationFailed,
        CatalogTelemetryEvent::NoCatalogChange,
    ]
    .into_iter()
}

fn saturating_add(counter: &AtomicU64, amount: u64) {
    let _result = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(amount))
    });
}

fn readiness_failures() -> impl Iterator<Item = AuthoritativeReadinessFailure> {
    [
        AuthoritativeReadinessFailure::AuditUnavailable,
        AuthoritativeReadinessFailure::CoordinatorFenced,
        AuthoritativeReadinessFailure::Integrity,
    ]
    .into_iter()
}

const fn operation_label(operation: ServiceOperationV1) -> MetricLabel {
    MetricLabel {
        key: "operation",
        value: operation_name(operation),
    }
}

const fn service_terminal_label(terminal: ServiceTerminalClass) -> MetricLabel {
    MetricLabel {
        key: "terminal_class",
        value: match terminal {
            ServiceTerminalClass::Succeeded => "succeeded",
            ServiceTerminalClass::Validation => "validation",
            ServiceTerminalClass::IdempotencyMismatch => "idempotency_mismatch",
            ServiceTerminalClass::AuthorizationDenied => "authorization_denied",
            ServiceTerminalClass::ConcurrencyDeadlineExceeded => "concurrency_deadline_exceeded",
            ServiceTerminalClass::ContractMismatch => "contract_mismatch",
            ServiceTerminalClass::StorageUnavailable => "storage_unavailable",
            ServiceTerminalClass::OutcomeUnknown => "outcome_unknown",
            ServiceTerminalClass::InternalDefect => "internal_defect",
            ServiceTerminalClass::CommandExecutionFailed => "command_execution_failed",
            ServiceTerminalClass::Cancelled => "cancelled",
            ServiceTerminalClass::DeadlineExceeded => "deadline_exceeded",
            ServiceTerminalClass::ResponseTooLarge => "response_too_large",
            ServiceTerminalClass::EmergencyInternal => "emergency_internal",
            ServiceTerminalClass::HistoryIncarnationMismatch => "history_incarnation_mismatch",
            ServiceTerminalClass::HistoryPruned => "history_pruned",
            ServiceTerminalClass::Overloaded => "overloaded",
        },
    }
}

const fn operation_name(operation: ServiceOperationV1) -> &'static str {
    match operation {
        ServiceOperationV1::ValidateContract => "validate_contract",
        ServiceOperationV1::ExplainCommand => "explain_command",
        ServiceOperationV1::DeployContract => "deploy_contract",
        ServiceOperationV1::GetActiveContract => "get_active_contract",
        ServiceOperationV1::GetContractVersion => "get_contract_version",
        ServiceOperationV1::ExecuteCommand => "execute_command",
        ServiceOperationV1::ResolveCommandOutcome => "resolve_command_outcome",
        ServiceOperationV1::GetEntity => "get_entity",
        ServiceOperationV1::ScanIndex => "scan_index",
        ServiceOperationV1::QueryProjection => "query_projection",
        ServiceOperationV1::GetProjectionStatus => "get_projection_status",
        ServiceOperationV1::GetCommit => "get_commit",
        ServiceOperationV1::ScanCommits => "scan_commits",
        ServiceOperationV1::SubscribeToCommits => "subscribe_to_commits",
        ServiceOperationV1::TraceProvenance => "trace_provenance",
        ServiceOperationV1::GetHealth => "get_health",
        ServiceOperationV1::GetStatistics => "get_statistics",
        ServiceOperationV1::CreateCapability => "create_capability",
        ServiceOperationV1::RevokeCapability => "revoke_capability",
        ServiceOperationV1::ListPendingOutboxDeliveries => "list_pending_outbox_deliveries",
        ServiceOperationV1::DiscoverCommandTools => "discover_command_tools",
        ServiceOperationV1::DiscoverResources => "discover_resources",
        ServiceOperationV1::DescribeContract => "describe_contract",
        ServiceOperationV1::CheckQuery => "check_query",
        ServiceOperationV1::ExplainQuery => "explain_query",
        ServiceOperationV1::ExecuteQuery => "execute_query",
        ServiceOperationV1::DeployQueryModule => "deploy_query_module",
        ServiceOperationV1::ApplyContractMigration => "apply_contract_migration",
        ServiceOperationV1::DescribeEvent => "describe_event",
        ServiceOperationV1::ReplayEvents => "replay_events",
        ServiceOperationV1::TailEvents => "tail_events",
        ServiceOperationV1::ExecuteProjectedQuery => "execute_projected_query",
        ServiceOperationV1::DeployReactiveModule => "deploy_reactive_module",
        ServiceOperationV1::ConsumeEventStream => "consume_event_stream",
        ServiceOperationV1::AcknowledgeEventStream => "acknowledge_event_stream",
        ServiceOperationV1::NegativeAcknowledgeEventStream => "negative_acknowledge_event_stream",
        ServiceOperationV1::SeekEventStreamConsumer => "seek_event_stream_consumer",
        ServiceOperationV1::RetireEventStreamConsumer => "retire_event_stream_consumer",
        ServiceOperationV1::GetEventStreamConsumerStatus => "get_event_stream_consumer_status",
        ServiceOperationV1::WatchNamedQuery => "watch_named_query",
        ServiceOperationV1::ConsumeContextualSubscription => "consume_contextual_subscription",
        ServiceOperationV1::AcknowledgeContextualSubscription => {
            "acknowledge_contextual_subscription"
        }
        ServiceOperationV1::NegativeAcknowledgeContextualSubscription => {
            "negative_acknowledge_contextual_subscription"
        }
        ServiceOperationV1::GetContextualSubscriptionStatus => "get_contextual_subscription_status",
        ServiceOperationV1::ExecuteContextualReaction => "execute_contextual_reaction",
        ServiceOperationV1::GetReactiveWakeup => "get_reactive_wakeup",
        ServiceOperationV1::StartApplicationInstallation => "start_application_installation",
        ServiceOperationV1::GetApplicationInstallation => "get_application_installation",
        ServiceOperationV1::StartApplicationExport => "start_application_export",
        ServiceOperationV1::GetApplicationExportPage => "get_application_export_page",
        ServiceOperationV1::GetApplicationExport => "get_application_export",
        ServiceOperationV1::CancelApplicationExport => "cancel_application_export",
        ServiceOperationV1::StartApplicationReimport => "start_application_reimport",
        ServiceOperationV1::ApplyApplicationReimportPage => "apply_application_reimport_page",
        ServiceOperationV1::GetApplicationReimport => "get_application_reimport",
        ServiceOperationV1::CancelApplicationReimport => "cancel_application_reimport",
        ServiceOperationV1::InspectVectorState => "inspect_vector_state",
        ServiceOperationV1::RegisterFollower => "register_follower",
        ServiceOperationV1::RetireFollower => "retire_follower",
    }
}

const fn authentication_rejection_label(reason: AuthenticationRejection) -> MetricLabel {
    MetricLabel {
        key: "reason",
        value: match reason {
            AuthenticationRejection::MalformedCredential => "malformed_credential",
            AuthenticationRejection::NoMatch => "no_match",
            AuthenticationRejection::InactiveCapability => "inactive_capability",
            AuthenticationRejection::BoundaryMismatch => "boundary_mismatch",
            AuthenticationRejection::OutsideValidityInterval => "outside_validity_interval",
        },
    }
}

const fn authentication_defect_label(reason: AuthenticationDefect) -> MetricLabel {
    MetricLabel {
        key: "reason",
        value: match reason {
            AuthenticationDefect::ClockUnavailable => "clock_unavailable",
            AuthenticationDefect::RepositoryUnavailable => "repository_unavailable",
            AuthenticationDefect::RepositoryIntegrity => "repository_integrity",
            AuthenticationDefect::MultipleMatches => "multiple_matches",
            AuthenticationDefect::ReciprocalLinkMismatch => "reciprocal_link_mismatch",
        },
    }
}

const fn policy_code_label(code: AuthorizationDenial) -> MetricLabel {
    MetricLabel {
        key: "reason",
        value: match code {
            AuthorizationDenial::MissingPermission => "missing_permission",
            AuthorizationDenial::TenantScopeMismatch => "tenant_scope_mismatch",
            AuthorizationDenial::PartitionScopeMismatch => "partition_scope_mismatch",
            AuthorizationDenial::FieldVisibilityDenied => "field_visibility_denied",
            AuthorizationDenial::ApprovalRequired => "approval_required",
            AuthorizationDenial::DelegationExceedsAuthority => "delegation_exceeds_authority",
            AuthorizationDenial::InactiveOrStaleCapability => "inactive_or_stale_capability",
        },
    }
}

const fn authorization_defect_label(reason: AuthorizationDefect) -> MetricLabel {
    MetricLabel {
        key: "reason",
        value: match reason {
            AuthorizationDefect::CurrentCapabilityUnavailable => "current_capability_unavailable",
            AuthorizationDefect::ClockUnavailable => "clock_unavailable",
        },
    }
}

const fn conflict_event_label(kind: ConflictEventKind) -> MetricLabel {
    MetricLabel {
        key: "kind",
        value: match kind {
            ConflictEventKind::Queued => "queued",
            ConflictEventKind::Acquired => "acquired",
            ConflictEventKind::Cancelled => "cancelled",
            ConflictEventKind::DeadlineExceeded => "deadline_exceeded",
            ConflictEventKind::Released => "released",
            ConflictEventKind::CapacityRejected => "capacity_rejected",
        },
    }
}

const fn catalog_event_label(event: CatalogTelemetryEvent) -> MetricLabel {
    MetricLabel {
        key: "kind",
        value: match event {
            CatalogTelemetryEvent::ActivationPrepared => "activation_prepared",
            CatalogTelemetryEvent::ActivationDurable => "activation_durable",
            CatalogTelemetryEvent::NotificationDelivered => "notification_delivered",
            CatalogTelemetryEvent::NotificationFailed => "notification_failed",
            CatalogTelemetryEvent::NoCatalogChange => "no_catalog_change",
        },
    }
}

const fn readiness_failure_label(reason: AuthoritativeReadinessFailure) -> MetricLabel {
    MetricLabel {
        key: "reason",
        value: match reason {
            AuthoritativeReadinessFailure::AuditUnavailable => "audit_unavailable",
            AuthoritativeReadinessFailure::CoordinatorFenced => "coordinator_fenced",
            AuthoritativeReadinessFailure::Integrity => "integrity",
        },
    }
}

const fn mcp_risk_index(risk: McpRiskClass) -> usize {
    match risk {
        McpRiskClass::AdministrativeMutation => 0,
        McpRiskClass::AdministrativeRead => 1,
        McpRiskClass::BoundedAdministrativeRead => 2,
        McpRiskClass::BoundedRead => 3,
        McpRiskClass::ReadOnly => 4,
        McpRiskClass::ReadOnlyCompute => 5,
        McpRiskClass::ReadOnlyData => 6,
        McpRiskClass::SymbolicRead => 7,
        McpRiskClass::ReactiveApplication => 8,
        McpRiskClass::ConsumerControl => 9,
        McpRiskClass::ApplicationMutation => 10,
        McpRiskClass::DynamicCommand => 11,
    }
}

#[cfg(test)]
mod histogram_hot_path_tests {
    use super::*;

    /// Boundary placement: the snapshot's cumulative buckets must be
    /// identical to the previous cumulative-at-observe encoding.
    #[test]
    fn snapshot_cumulative_buckets_match_boundary_placement_exactly() {
        let histogram = FixedHistogram::new();
        // Values chosen on exact bounds and just past them.
        for value in [0, 0, 1, 2, 3, u64::MAX] {
            histogram.observe(value);
        }
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.count, 6);
        assert_eq!(snapshot.sum, u64::MAX.wrapping_add(6));
        // bounds start [0, 1, 2, ...]; cumulative: <=0 → 2, <=1 → 3, <=2 → 4.
        assert_eq!(snapshot.cumulative_buckets[0], 2);
        assert_eq!(snapshot.cumulative_buckets[1], 3);
        assert_eq!(snapshot.cumulative_buckets[2], 4);
        // Everything lands somewhere; the final cumulative equals the count.
        assert_eq!(
            snapshot.cumulative_buckets[HISTOGRAM_UPPER_BOUNDS.len() - 1],
            6
        );
        // Cumulative sequence is monotone by construction.
        for pair in snapshot.cumulative_buckets.windows(2) {
            assert!(pair[0] <= pair[1], "cumulative buckets must be monotone");
        }
    }

    /// Concurrency: hammered from many threads, count must equal the bucket
    /// total exactly once writers stop (no lost updates on the single-bucket
    /// path).
    #[test]
    fn concurrent_observations_lose_nothing() {
        let histogram = std::sync::Arc::new(FixedHistogram::new());
        let threads: Vec<_> = (0..8)
            .map(|worker| {
                let histogram = std::sync::Arc::clone(&histogram);
                std::thread::spawn(move || {
                    for i in 0..10_000_u64 {
                        histogram.observe((worker * 131 + i * 7) % 5_000);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("worker");
        }
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.count, 80_000);
        assert_eq!(
            snapshot.cumulative_buckets[HISTOGRAM_UPPER_BOUNDS.len() - 1],
            80_000
        );
    }
}
