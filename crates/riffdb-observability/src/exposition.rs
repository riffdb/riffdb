//! Pure-Rust Prometheus v0.0.4, OTLP JSON v1, and soak flight-recorder NDJSON exposition.
//!
//! All encoders format closed, fixed-cardinality observations using `std::fmt::Write`
//! so `riffdb-observability` remains a strict leaf crate with zero serialization or
//! network dependencies.

use std::fmt::Write as _;

use riffdb_types::ServiceIngressKindV1;

use crate::{
    CommandPipelineStage, CommitCommandTerminal, CommitGroupDispatchReason, HISTOGRAM_UPPER_BOUNDS,
    HealthClassification, HistogramSnapshot, McpRiskClass, MetricLabel, MetricRegistry,
    Observability, REQUIRED_METRIC_INVENTORY, ReadPipelineStage, RequiredCounter, RequiredGauge,
    RequiredHistogram, WriteServiceStage,
};

impl HistogramSnapshot {
    /// Returns a deterministic upper-bound approximation for the requested permille (`0..=1000`).
    ///
    /// For example, `permille = 500` approximates `p50`, `950` approximates `p95`, and
    /// `990` approximates `p99`. Returns `0` when `count == 0`.
    #[must_use]
    pub const fn approx_percentile_upper_bound(&self, permille: u16) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let clamped = if permille > 1_000 {
            1_000_u128
        } else {
            permille as u128
        };
        let numerator = (self.count as u128) * clamped;
        let rank = numerator.div_ceil(1_000);
        let target_rank = if rank == 0 { 1 } else { rank as u64 };

        let mut index = 0;
        while index < HISTOGRAM_UPPER_BOUNDS.len() {
            if self.cumulative_buckets[index] >= target_rank {
                let bound = HISTOGRAM_UPPER_BOUNDS[index];
                if bound == u64::MAX {
                    let mean = self.sum / self.count;
                    return if mean > 10_000_000 { mean } else { 10_000_000 };
                }
                return bound;
            }
            index += 1;
        }
        10_000_000
    }

    /// Converts cumulative bucket counts into per-bucket non-cumulative counts.
    #[must_use]
    pub const fn non_cumulative_buckets(&self) -> [u64; HISTOGRAM_UPPER_BOUNDS.len()] {
        let mut out = [0_u64; HISTOGRAM_UPPER_BOUNDS.len()];
        let mut index = 0;
        let mut previous = 0_u64;
        while index < HISTOGRAM_UPPER_BOUNDS.len() {
            let current = self.cumulative_buckets[index];
            out[index] = current.saturating_sub(previous);
            previous = current;
            index += 1;
        }
        out
    }
}

impl MetricRegistry {
    /// Encodes the current fixed-cardinality registry snapshot in Prometheus text exposition
    /// format (`text/plain; version=0.0.4; charset=utf-8`).
    #[must_use]
    pub fn encode_prometheus_v1(&self) -> String {
        let mut out = String::with_capacity(16_384);
        self.write_prometheus_v1(&mut out);
        out
    }

    /// Encodes the current fixed-cardinality registry snapshot as an OTLP JSON
    /// `ExportMetricsServiceRequest` payload.
    #[must_use]
    pub fn encode_otel_json_v1(&self, timestamp_unix_nanos: u64) -> String {
        let mut out = String::with_capacity(32_768);
        let _ = write!(
            out,
            "{{\"resourceMetrics\":[{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"riffdbd\"}}}}]}},\"scopeMetrics\":[{{\"scope\":{{\"name\":\"riffdb-observability\",\"version\":\"v1\"}},\"metrics\":["
        );
        let mut first = true;
        self.write_otel_metrics_v1(&mut out, timestamp_unix_nanos, &mut first);
        let _ = write!(out, "]}}]}}]}}");
        out
    }

    pub(crate) fn write_prometheus_v1(&self, out: &mut String) {
        // 1. Fixed MetricKey counter families, grouped by metric family name in canonical order.
        let samples = self.snapshot();
        let mut idx = 0;
        while idx < samples.len() {
            let family_name = samples[idx].name;
            let _ = writeln!(out, "# HELP {family_name} Closed RiffDB process counter.");
            let _ = writeln!(out, "# TYPE {family_name} counter");
            while idx < samples.len() && samples[idx].name == family_name {
                let sample = samples[idx];
                write_prometheus_sample(out, sample.name, &sample.labels, sample.value);
                idx += 1;
            }
        }

        // 2. All 40 REQUIRED_METRIC_INVENTORY families in canonical inventory order.
        for descriptor in REQUIRED_METRIC_INVENTORY {
            let name = descriptor.name;
            match descriptor.family {
                crate::RequiredMetricFamily::CommandRequests => {
                    let _ = writeln!(
                        out,
                        "# HELP {name} Total command requests by closed command, transport, and terminal class."
                    );
                    let _ = writeln!(out, "# TYPE {name} counter");
                    let command_series = self.command_snapshot();
                    if command_series.is_empty() {
                        let total = self.required_counter(RequiredCounter::CommandRequests);
                        let _ = writeln!(out, "{name} {total}");
                    } else {
                        for entry in &command_series {
                            let cmd_id = entry.dimensions.command_id().get();
                            let transport = ingress_label(entry.dimensions.ingress());
                            let terminal = command_terminal_label(entry.dimensions.terminal());
                            let _ = writeln!(
                                out,
                                "{name}{{command_id=\"{cmd_id}\",transport=\"{transport}\",terminal_class=\"{terminal}\"}} {}",
                                entry.requests
                            );
                        }
                    }
                }
                crate::RequiredMetricFamily::CommandLatencyMicroseconds => {
                    let _ = writeln!(
                        out,
                        "# HELP {name} Command latency histogram in microseconds."
                    );
                    let _ = writeln!(out, "# TYPE {name} histogram");
                    let command_series = self.command_snapshot();
                    if command_series.is_empty() {
                        let snapshot =
                            self.required_histogram(RequiredHistogram::CommandLatencyMicroseconds);
                        write_prometheus_histogram(out, name, &[], &snapshot);
                    } else {
                        for entry in &command_series {
                            let cmd_id = entry.dimensions.command_id().get();
                            let transport = ingress_label(entry.dimensions.ingress());
                            let terminal = command_terminal_label(entry.dimensions.terminal());
                            let prefix = format!(
                                "command_id=\"{cmd_id}\",transport=\"{transport}\",terminal_class=\"{terminal}\""
                            );
                            write_prometheus_histogram_with_raw_labels(
                                out,
                                name,
                                &prefix,
                                &entry.latency_microseconds,
                            );
                        }
                    }
                }
                crate::RequiredMetricFamily::StorageQueueLatencyMicroseconds => {
                    write_prometheus_required_histogram(
                        out,
                        name,
                        &self
                            .required_histogram(RequiredHistogram::StorageQueueLatencyMicroseconds),
                    );
                }
                crate::RequiredMetricFamily::LockWaitMicroseconds => {
                    write_prometheus_required_histogram(
                        out,
                        name,
                        &self.required_histogram(RequiredHistogram::LockWaitMicroseconds),
                    );
                }
                crate::RequiredMetricFamily::LockTimeouts => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::LockTimeouts),
                    );
                }
                crate::RequiredMetricFamily::ConflictQueueDepth => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::ConflictQueueDepth),
                    );
                }
                crate::RequiredMetricFamily::HotConflictKeyCardinality => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::HotConflictKeyCardinality),
                    );
                }
                crate::RequiredMetricFamily::Commits => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::Commits),
                    );
                }
                crate::RequiredMetricFamily::CommitDurationMicroseconds => {
                    write_prometheus_required_histogram(
                        out,
                        name,
                        &self.required_histogram(RequiredHistogram::CommitDurationMicroseconds),
                    );
                }
                crate::RequiredMetricFamily::CommitBatchSize => {
                    write_prometheus_required_histogram(
                        out,
                        name,
                        &self.required_histogram(RequiredHistogram::CommitBatchSize),
                    );
                }
                crate::RequiredMetricFamily::DurableFlushDurationMicroseconds => {
                    write_prometheus_required_histogram(
                        out,
                        name,
                        &self.required_histogram(
                            RequiredHistogram::DurableFlushDurationMicroseconds,
                        ),
                    );
                }
                crate::RequiredMetricFamily::IdempotencyHits => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::IdempotencyHits),
                    );
                }
                crate::RequiredMetricFamily::IdempotencyMismatches => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::IdempotencyMismatches),
                    );
                }
                crate::RequiredMetricFamily::IdempotencyUncertaintyRecoveries => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::IdempotencyUncertaintyRecoveries),
                    );
                }
                crate::RequiredMetricFamily::ActiveContractVersion => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::ActiveContractVersion),
                    );
                }
                crate::RequiredMetricFamily::ContractDeployments => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::ContractDeployments),
                    );
                }
                crate::RequiredMetricFamily::OutboxPending => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::OutboxPending),
                    );
                }
                crate::RequiredMetricFamily::OutboxAttempts => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::OutboxAttempts),
                    );
                }
                crate::RequiredMetricFamily::OutboxOldestPendingAgeMilliseconds => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::OutboxOldestPendingAgeMilliseconds),
                    );
                }
                crate::RequiredMetricFamily::OutboxDeadLetters => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::OutboxDeadLetters),
                    );
                }
                crate::RequiredMetricFamily::ProjectionFrontier => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::ProjectionFrontier),
                    );
                }
                crate::RequiredMetricFamily::ProjectionLagCommits => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::ProjectionLagCommits),
                    );
                }
                crate::RequiredMetricFamily::ProjectionRebuilds => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::ProjectionRebuilds),
                    );
                }
                crate::RequiredMetricFamily::ProjectionErrors => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::ProjectionErrors),
                    );
                }
                crate::RequiredMetricFamily::McpSessions => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::McpSessions),
                    );
                }
                crate::RequiredMetricFamily::McpToolCalls => {
                    let _ = writeln!(
                        out,
                        "# HELP {name} MCP tool calls by closed risk classification."
                    );
                    let _ = writeln!(out, "# TYPE {name} counter");
                    for risk in McpRiskClass::ALL {
                        let risk_str = mcp_risk_label(risk);
                        let val = self.mcp_tool_calls(risk);
                        let _ = writeln!(out, "{name}{{risk_class=\"{risk_str}\"}} {val}");
                    }
                }
                crate::RequiredMetricFamily::McpSchemaFailures => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::McpSchemaFailures),
                    );
                }
                crate::RequiredMetricFamily::McpAuthorizationDenials => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::McpAuthorizationDenials),
                    );
                }
                crate::RequiredMetricFamily::McpListChangeNotifications => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::McpListChangeNotifications),
                    );
                }
                crate::RequiredMetricFamily::StorageBytes => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::StorageBytes),
                    );
                }
                crate::RequiredMetricFamily::CommitLogRecords => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::CommitLogRecords),
                    );
                }
                crate::RequiredMetricFamily::StartupRecoveryDurationMilliseconds => {
                    write_prometheus_required_histogram(
                        out,
                        name,
                        &self.required_histogram(
                            RequiredHistogram::StartupRecoveryDurationMilliseconds,
                        ),
                    );
                }
                crate::RequiredMetricFamily::CommandGroupDispatch => {
                    let _ = writeln!(
                        out,
                        "# HELP {name} Command-group dispatches by closed selection reason."
                    );
                    let _ = writeln!(out, "# TYPE {name} counter");
                    let reasons = self.command_group_dispatch_reasons();
                    for (reason, count) in [
                        (CommitGroupDispatchReason::Full, reasons[0]),
                        (CommitGroupDispatchReason::Barrier, reasons[1]),
                        (CommitGroupDispatchReason::QueueDrained, reasons[2]),
                        (CommitGroupDispatchReason::ReceiverClosed, reasons[3]),
                    ] {
                        let label = dispatch_reason_label(reason);
                        let _ = writeln!(out, "{name}{{reason=\"{label}\"}} {count}");
                    }
                }
                crate::RequiredMetricFamily::CommandGroupSelected => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::CommandGroupSelected),
                    );
                }
                crate::RequiredMetricFamily::CommandGroupDeferred => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::CommandGroupDeferred),
                    );
                }
                crate::RequiredMetricFamily::CommandStageDurationMicroseconds => {
                    let _ = writeln!(
                        out,
                        "# HELP {name} Coordinator command pipeline stage duration in microseconds."
                    );
                    let _ = writeln!(out, "# TYPE {name} histogram");
                    for stage in COMMAND_STAGES {
                        let label = command_stage_label(stage);
                        let snap = self.command_stage_duration(stage);
                        write_prometheus_histogram(
                            out,
                            name,
                            &[MetricLabel {
                                key: "stage",
                                value: label,
                            }],
                            &snap,
                        );
                    }
                }
                crate::RequiredMetricFamily::ReadStageDurationMicroseconds => {
                    let _ = writeln!(
                        out,
                        "# HELP {name} Symbolic read pipeline stage duration in microseconds."
                    );
                    let _ = writeln!(out, "# TYPE {name} histogram");
                    for stage in ReadPipelineStage::ALL {
                        let label = stage.metric_label();
                        let snap = self.read_stage_duration(stage);
                        write_prometheus_histogram(
                            out,
                            name,
                            &[MetricLabel {
                                key: "stage",
                                value: label,
                            }],
                            &snap,
                        );
                    }
                }
                crate::RequiredMetricFamily::WriterBusyMicroseconds => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::WriterBusyMicroseconds),
                    );
                }
                crate::RequiredMetricFamily::WriterIdleMicroseconds => {
                    write_prometheus_unlabeled_counter(
                        out,
                        name,
                        self.required_counter(RequiredCounter::WriterIdleMicroseconds),
                    );
                }
                crate::RequiredMetricFamily::CommandQueueDelayEstimateMicroseconds => {
                    write_prometheus_optional_gauge(
                        out,
                        name,
                        self.required_gauge(RequiredGauge::CommandQueueDelayEstimateMicroseconds),
                    );
                }
            }
        }

        // 3. Additional stage and pool histograms owned by MetricRegistry.
        let _ = writeln!(
            out,
            "# HELP riffdb_write_service_stage_duration_microseconds Write service stage duration in microseconds."
        );
        let _ = writeln!(
            out,
            "# TYPE riffdb_write_service_stage_duration_microseconds histogram"
        );
        for stage in WriteServiceStage::ALL {
            let snap = self.write_service_stage_duration(stage);
            write_prometheus_histogram(
                out,
                "riffdb_write_service_stage_duration_microseconds",
                &[MetricLabel {
                    key: "stage",
                    value: stage.metric_label(),
                }],
                &snap,
            );
        }

        write_prometheus_required_histogram(
            out,
            "riffdb_command_group_residence_duration_microseconds",
            &self.command_group_residence_duration(),
        );
        write_prometheus_required_histogram(
            out,
            "riffdb_command_application_duration_microseconds",
            &self.command_application_duration(),
        );
        write_prometheus_required_histogram(
            out,
            "riffdb_command_submission_duration_microseconds",
            &self.command_submission_duration(),
        );
        write_prometheus_required_histogram(
            out,
            "riffdb_preparation_pool_depth",
            &self.preparation_pool_depth(),
        );
        write_prometheus_required_histogram(
            out,
            "riffdb_reorder_buffer_occupancy",
            &self.reorder_buffer_occupancy(),
        );
    }

    pub(crate) fn write_otel_metrics_v1(
        &self,
        out: &mut String,
        timestamp_unix_nanos: u64,
        first: &mut bool,
    ) {
        // Required counters
        for descriptor in REQUIRED_METRIC_INVENTORY {
            match descriptor.family {
                crate::RequiredMetricFamily::LockTimeouts => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::LockTimeouts),
                ),
                crate::RequiredMetricFamily::Commits => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::Commits),
                ),
                crate::RequiredMetricFamily::IdempotencyHits => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::IdempotencyHits),
                ),
                crate::RequiredMetricFamily::IdempotencyMismatches => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::IdempotencyMismatches),
                ),
                crate::RequiredMetricFamily::IdempotencyUncertaintyRecoveries => {
                    write_otel_counter(
                        out,
                        first,
                        descriptor.name,
                        timestamp_unix_nanos,
                        &[],
                        self.required_counter(RequiredCounter::IdempotencyUncertaintyRecoveries),
                    );
                }
                crate::RequiredMetricFamily::ContractDeployments => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::ContractDeployments),
                ),
                crate::RequiredMetricFamily::OutboxAttempts => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::OutboxAttempts),
                ),
                crate::RequiredMetricFamily::ProjectionRebuilds => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::ProjectionRebuilds),
                ),
                crate::RequiredMetricFamily::ProjectionErrors => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::ProjectionErrors),
                ),
                crate::RequiredMetricFamily::McpSchemaFailures => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::McpSchemaFailures),
                ),
                crate::RequiredMetricFamily::McpAuthorizationDenials => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::McpAuthorizationDenials),
                ),
                crate::RequiredMetricFamily::McpListChangeNotifications => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::McpListChangeNotifications),
                ),
                crate::RequiredMetricFamily::CommandGroupSelected => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::CommandGroupSelected),
                ),
                crate::RequiredMetricFamily::CommandGroupDeferred => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::CommandGroupDeferred),
                ),
                crate::RequiredMetricFamily::WriterBusyMicroseconds => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::WriterBusyMicroseconds),
                ),
                crate::RequiredMetricFamily::WriterIdleMicroseconds => write_otel_counter(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    self.required_counter(RequiredCounter::WriterIdleMicroseconds),
                ),
                crate::RequiredMetricFamily::CommandRequests => {
                    write_otel_counter(
                        out,
                        first,
                        descriptor.name,
                        timestamp_unix_nanos,
                        &[],
                        self.required_counter(RequiredCounter::CommandRequests),
                    );
                }
                crate::RequiredMetricFamily::McpToolCalls => {
                    for risk in McpRiskClass::ALL {
                        write_otel_counter(
                            out,
                            first,
                            descriptor.name,
                            timestamp_unix_nanos,
                            &[MetricLabel {
                                key: "risk_class",
                                value: mcp_risk_label(risk),
                            }],
                            self.mcp_tool_calls(risk),
                        );
                    }
                }
                crate::RequiredMetricFamily::CommandGroupDispatch => {
                    let reasons = self.command_group_dispatch_reasons();
                    for (reason, count) in [
                        (CommitGroupDispatchReason::Full, reasons[0]),
                        (CommitGroupDispatchReason::Barrier, reasons[1]),
                        (CommitGroupDispatchReason::QueueDrained, reasons[2]),
                        (CommitGroupDispatchReason::ReceiverClosed, reasons[3]),
                    ] {
                        write_otel_counter(
                            out,
                            first,
                            descriptor.name,
                            timestamp_unix_nanos,
                            &[MetricLabel {
                                key: "reason",
                                value: dispatch_reason_label(reason),
                            }],
                            count,
                        );
                    }
                }
                crate::RequiredMetricFamily::CommandLatencyMicroseconds => write_otel_histogram(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    &self.required_histogram(RequiredHistogram::CommandLatencyMicroseconds),
                ),
                crate::RequiredMetricFamily::StorageQueueLatencyMicroseconds => {
                    write_otel_histogram(
                        out,
                        first,
                        descriptor.name,
                        timestamp_unix_nanos,
                        &[],
                        &self
                            .required_histogram(RequiredHistogram::StorageQueueLatencyMicroseconds),
                    );
                }
                crate::RequiredMetricFamily::LockWaitMicroseconds => write_otel_histogram(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    &self.required_histogram(RequiredHistogram::LockWaitMicroseconds),
                ),
                crate::RequiredMetricFamily::CommitDurationMicroseconds => write_otel_histogram(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    &self.required_histogram(RequiredHistogram::CommitDurationMicroseconds),
                ),
                crate::RequiredMetricFamily::CommitBatchSize => write_otel_histogram(
                    out,
                    first,
                    descriptor.name,
                    timestamp_unix_nanos,
                    &[],
                    &self.required_histogram(RequiredHistogram::CommitBatchSize),
                ),
                crate::RequiredMetricFamily::DurableFlushDurationMicroseconds => {
                    write_otel_histogram(
                        out,
                        first,
                        descriptor.name,
                        timestamp_unix_nanos,
                        &[],
                        &self.required_histogram(
                            RequiredHistogram::DurableFlushDurationMicroseconds,
                        ),
                    );
                }
                crate::RequiredMetricFamily::StartupRecoveryDurationMilliseconds => {
                    write_otel_histogram(
                        out,
                        first,
                        descriptor.name,
                        timestamp_unix_nanos,
                        &[],
                        &self.required_histogram(
                            RequiredHistogram::StartupRecoveryDurationMilliseconds,
                        ),
                    );
                }
                crate::RequiredMetricFamily::CommandStageDurationMicroseconds => {
                    for stage in COMMAND_STAGES {
                        let snap = self.command_stage_duration(stage);
                        write_otel_histogram(
                            out,
                            first,
                            descriptor.name,
                            timestamp_unix_nanos,
                            &[MetricLabel {
                                key: "stage",
                                value: command_stage_label(stage),
                            }],
                            &snap,
                        );
                    }
                }
                crate::RequiredMetricFamily::ReadStageDurationMicroseconds => {
                    for stage in ReadPipelineStage::ALL {
                        let snap = self.read_stage_duration(stage);
                        write_otel_histogram(
                            out,
                            first,
                            descriptor.name,
                            timestamp_unix_nanos,
                            &[MetricLabel {
                                key: "stage",
                                value: stage.metric_label(),
                            }],
                            &snap,
                        );
                    }
                }
                crate::RequiredMetricFamily::ConflictQueueDepth => {
                    if let Some(val) = self.required_gauge(RequiredGauge::ConflictQueueDepth) {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::HotConflictKeyCardinality => {
                    if let Some(val) = self.required_gauge(RequiredGauge::HotConflictKeyCardinality)
                    {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::ActiveContractVersion => {
                    if let Some(val) = self.required_gauge(RequiredGauge::ActiveContractVersion) {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::OutboxPending => {
                    if let Some(val) = self.required_gauge(RequiredGauge::OutboxPending) {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::OutboxOldestPendingAgeMilliseconds => {
                    if let Some(val) =
                        self.required_gauge(RequiredGauge::OutboxOldestPendingAgeMilliseconds)
                    {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::OutboxDeadLetters => {
                    if let Some(val) = self.required_gauge(RequiredGauge::OutboxDeadLetters) {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::ProjectionFrontier => {
                    if let Some(val) = self.required_gauge(RequiredGauge::ProjectionFrontier) {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::ProjectionLagCommits => {
                    if let Some(val) = self.required_gauge(RequiredGauge::ProjectionLagCommits) {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::McpSessions => {
                    if let Some(val) = self.required_gauge(RequiredGauge::McpSessions) {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::StorageBytes => {
                    if let Some(val) = self.required_gauge(RequiredGauge::StorageBytes) {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::CommitLogRecords => {
                    if let Some(val) = self.required_gauge(RequiredGauge::CommitLogRecords) {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
                crate::RequiredMetricFamily::CommandQueueDelayEstimateMicroseconds => {
                    if let Some(val) =
                        self.required_gauge(RequiredGauge::CommandQueueDelayEstimateMicroseconds)
                    {
                        write_otel_gauge(out, first, descriptor.name, timestamp_unix_nanos, val);
                    }
                }
            }
        }
    }
}

impl Observability {
    /// Encodes all process metrics, completion-lane counters, and health gauges in
    /// Prometheus v0.0.4 text exposition format.
    #[must_use]
    pub fn encode_prometheus_v1(&self) -> String {
        let mut out = String::with_capacity(20_480);
        self.metrics().write_prometheus_v1(&mut out);

        let health = self.health().snapshot();
        let status_code = match health.classification() {
            HealthClassification::NotReady => 0_u64,
            HealthClassification::Ready => 1_u64,
            HealthClassification::Degraded => 2_u64,
        };
        let _ = writeln!(
            out,
            "# HELP riffdb_health_status Process health classification (0=not_ready, 1=ready, 2=degraded)."
        );
        let _ = writeln!(out, "# TYPE riffdb_health_status gauge");
        let _ = writeln!(out, "riffdb_health_status {status_code}");

        let liveness = u64::from(health.liveness());
        let _ = writeln!(out, "# HELP riffdb_health_liveness Process liveness gauge.");
        let _ = writeln!(out, "# TYPE riffdb_health_liveness gauge");
        let _ = writeln!(out, "riffdb_health_liveness {liveness}");

        let auth_ready = u64::from(health.authoritative_ready());
        let _ = writeln!(
            out,
            "# HELP riffdb_health_authoritative_ready Authoritative prerequisites readiness gauge."
        );
        let _ = writeln!(out, "# TYPE riffdb_health_authoritative_ready gauge");
        let _ = writeln!(out, "riffdb_health_authoritative_ready {auth_ready}");

        if let Some(seq) = health.last_commit_sequence() {
            let _ = writeln!(
                out,
                "# HELP riffdb_health_last_commit_sequence Last authoritative application commit sequence."
            );
            let _ = writeln!(out, "# TYPE riffdb_health_last_commit_sequence gauge");
            let _ = writeln!(out, "riffdb_health_last_commit_sequence {}", seq.get());
        }

        let completion = self.completion_lane_evidence_snapshot();
        let _ = writeln!(
            out,
            "# HELP riffdb_completion_lane_max_depth Maximum observed completion-lane depth."
        );
        let _ = writeln!(out, "# TYPE riffdb_completion_lane_max_depth gauge");
        let _ = writeln!(
            out,
            "riffdb_completion_lane_max_depth {}",
            completion.max_depth
        );

        let _ = writeln!(
            out,
            "# HELP riffdb_completion_lane_max_reorder_occupancy Maximum observed completion-lane reorder occupancy."
        );
        let _ = writeln!(
            out,
            "# TYPE riffdb_completion_lane_max_reorder_occupancy gauge"
        );
        let _ = writeln!(
            out,
            "riffdb_completion_lane_max_reorder_occupancy {}",
            completion.max_reorder_occupancy
        );

        let retention = self.online_retention_snapshot();
        let _ = writeln!(
            out,
            "# HELP riffdb_online_retention_state Online retention lane state (0=holding, 1=idle, 2=advancing, 3=fenced, 4=refused, 5=cancelled)."
        );
        let _ = writeln!(out, "# TYPE riffdb_online_retention_state gauge");
        let _ = writeln!(
            out,
            "riffdb_online_retention_state {}",
            retention.state.code()
        );

        let _ = writeln!(
            out,
            "# HELP riffdb_online_retention_fence Active online retention binding fence (0=none, 1=durable_application_head, 2=projection_durable_frontier, 3=operator_hold, 4=undelivered_outbox, 5=staged_migration, 6=consumer_low_water, 7=follower_low_water, 8=unbounded)."
        );
        let _ = writeln!(out, "# TYPE riffdb_online_retention_fence gauge");
        let _ = writeln!(
            out,
            "riffdb_online_retention_fence {}",
            retention.fence.code()
        );

        let _ = writeln!(
            out,
            "# HELP riffdb_online_retention_steps_total Total online retention pruning steps completed."
        );
        let _ = writeln!(out, "# TYPE riffdb_online_retention_steps_total counter");
        let _ = writeln!(
            out,
            "riffdb_online_retention_steps_total {}",
            retention.steps_total
        );

        let _ = writeln!(
            out,
            "# HELP riffdb_online_retention_sequences_total Total commit sequences logically pruned by online retention."
        );
        let _ = writeln!(
            out,
            "# TYPE riffdb_online_retention_sequences_total counter"
        );
        let _ = writeln!(
            out,
            "riffdb_online_retention_sequences_total {}",
            retention.sequences_total
        );

        let _ = writeln!(
            out,
            "# HELP riffdb_online_retention_watermark_sequence Latest inclusive commit sequence pruned by online retention."
        );
        let _ = writeln!(
            out,
            "# TYPE riffdb_online_retention_watermark_sequence gauge"
        );
        let _ = writeln!(
            out,
            "riffdb_online_retention_watermark_sequence {}",
            retention.watermark_sequence
        );

        out
    }

    /// Encodes all process metrics and health gauges as an OTLP JSON
    /// `ExportMetricsServiceRequest` payload.
    #[must_use]
    pub fn encode_otel_json_v1(&self, timestamp_unix_nanos: u64) -> String {
        let mut out = String::with_capacity(36_864);
        let _ = write!(
            out,
            "{{\"resourceMetrics\":[{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"riffdbd\"}}}}]}},\"scopeMetrics\":[{{\"scope\":{{\"name\":\"riffdb-observability\",\"version\":\"v1\"}},\"metrics\":["
        );
        let mut first = true;
        self.metrics()
            .write_otel_metrics_v1(&mut out, timestamp_unix_nanos, &mut first);

        let health = self.health().snapshot();
        let status_code = match health.classification() {
            HealthClassification::NotReady => 0_u64,
            HealthClassification::Ready => 1_u64,
            HealthClassification::Degraded => 2_u64,
        };
        write_otel_gauge(
            &mut out,
            &mut first,
            "riffdb_health_status",
            timestamp_unix_nanos,
            status_code,
        );
        write_otel_gauge(
            &mut out,
            &mut first,
            "riffdb_health_authoritative_ready",
            timestamp_unix_nanos,
            u64::from(health.authoritative_ready()),
        );
        if let Some(seq) = health.last_commit_sequence() {
            write_otel_gauge(
                &mut out,
                &mut first,
                "riffdb_health_last_commit_sequence",
                timestamp_unix_nanos,
                seq.get(),
            );
        }
        let completion = self.completion_lane_evidence_snapshot();
        write_otel_gauge(
            &mut out,
            &mut first,
            "riffdb_completion_lane_max_depth",
            timestamp_unix_nanos,
            completion.max_depth,
        );
        let retention = self.online_retention_snapshot();
        write_otel_gauge(
            &mut out,
            &mut first,
            "riffdb_online_retention_state",
            timestamp_unix_nanos,
            retention.state.code(),
        );
        write_otel_gauge(
            &mut out,
            &mut first,
            "riffdb_online_retention_fence",
            timestamp_unix_nanos,
            retention.fence.code(),
        );
        write_otel_counter(
            &mut out,
            &mut first,
            "riffdb_online_retention_steps_total",
            timestamp_unix_nanos,
            &[],
            retention.steps_total,
        );
        write_otel_counter(
            &mut out,
            &mut first,
            "riffdb_online_retention_sequences_total",
            timestamp_unix_nanos,
            &[],
            retention.sequences_total,
        );
        write_otel_gauge(
            &mut out,
            &mut first,
            "riffdb_online_retention_watermark_sequence",
            timestamp_unix_nanos,
            retention.watermark_sequence,
        );
        let _ = write!(out, "]}}]}}]}}");
        out
    }

    /// Encodes a single-line bounded NDJSON flight-recorder snapshot for long-running soak and
    /// endurance runs (`END-007`, `END-008`).
    #[must_use]
    pub fn encode_flight_record_ndjson_v1(
        &self,
        timestamp_unix_nanos: u64,
        storage_bytes: u64,
        journal_bytes: u64,
    ) -> String {
        let mut out = String::with_capacity(4_096);
        let m = self.metrics();
        let health = self.health().snapshot();
        let health_str = match health.classification() {
            HealthClassification::NotReady => "not_ready",
            HealthClassification::Ready => "ready",
            HealthClassification::Degraded => "degraded",
        };
        let auth_ready = health.authoritative_ready();
        let last_seq = health.last_commit_sequence().map(|s| s.get());
        let effective_storage_bytes = if storage_bytes > 0 {
            storage_bytes
        } else {
            m.required_gauge(RequiredGauge::StorageBytes).unwrap_or(0)
        };
        let commits_total = m.required_counter(RequiredCounter::Commits);
        let command_requests_total = {
            let snapshots = m.command_snapshot();
            if snapshots.is_empty() {
                m.required_counter(RequiredCounter::CommandRequests)
            } else {
                snapshots
                    .iter()
                    .fold(0_u64, |acc, s| acc.saturating_add(s.requests))
                    .max(m.required_counter(RequiredCounter::CommandRequests))
            }
        };
        let lock_timeouts_total = m.required_counter(RequiredCounter::LockTimeouts);
        let idempotency_hits_total = m.required_counter(RequiredCounter::IdempotencyHits);
        let idempotency_mismatches_total =
            m.required_counter(RequiredCounter::IdempotencyMismatches);
        let telemetry_dropped_total = m.value(crate::MetricKey::TelemetryDropped);
        let writer_busy_us_total = m.required_counter(RequiredCounter::WriterBusyMicroseconds);
        let writer_idle_us_total = m.required_counter(RequiredCounter::WriterIdleMicroseconds);
        let queue_delay_estimate_us = m
            .required_gauge(RequiredGauge::CommandQueueDelayEstimateMicroseconds)
            .unwrap_or(0);
        let conflict_queue_depth = m
            .required_gauge(RequiredGauge::ConflictQueueDepth)
            .unwrap_or(0);
        let hot_conflict_key_cardinality = m
            .required_gauge(RequiredGauge::HotConflictKeyCardinality)
            .unwrap_or(0);
        let outbox_pending = m.required_gauge(RequiredGauge::OutboxPending).unwrap_or(0);
        let outbox_dead_letters = m
            .required_gauge(RequiredGauge::OutboxDeadLetters)
            .unwrap_or(0);

        let _ = write!(
            out,
            "{{\"schema\":\"riffdb.telemetry.flight-record.v1\",\"schema_version\":1,\"timestamp_unix_nanos\":{timestamp_unix_nanos},\"health_classification\":\"{health_str}\",\"authoritative_ready\":{auth_ready},\"last_commit_sequence\":"
        );
        match last_seq {
            Some(seq) => {
                let _ = write!(out, "{seq}");
            }
            None => {
                let _ = write!(out, "null");
            }
        }
        let _ = write!(
            out,
            ",\"storage_bytes\":{effective_storage_bytes},\"journal_bytes\":{journal_bytes},\"commits_total\":{commits_total},\"command_requests_total\":{command_requests_total},\"lock_timeouts_total\":{lock_timeouts_total},\"idempotency_hits_total\":{idempotency_hits_total},\"idempotency_mismatches_total\":{idempotency_mismatches_total},\"telemetry_dropped_total\":{telemetry_dropped_total},\"writer_busy_us_total\":{writer_busy_us_total},\"writer_idle_us_total\":{writer_idle_us_total},\"queue_delay_estimate_us\":{queue_delay_estimate_us},\"conflict_queue_depth\":{conflict_queue_depth},\"hot_conflict_key_cardinality\":{hot_conflict_key_cardinality},\"outbox_pending\":{outbox_pending},\"outbox_dead_letters\":{outbox_dead_letters}"
        );
        let retention = self.online_retention_snapshot();
        let _ = write!(
            out,
            ",\"online_retention_state\":\"{}\",\"online_retention_fence\":\"{}\",\"online_retention_steps_total\":{},\"online_retention_sequences_total\":{},\"online_retention_watermark_sequence\":{}",
            retention.state.as_str(),
            retention.fence.as_str(),
            retention.steps_total,
            retention.sequences_total,
            retention.watermark_sequence,
        );

        let _ = write!(out, ",\"commit_duration_us\":");
        write_flight_histogram_summary(
            &mut out,
            &m.required_histogram(RequiredHistogram::CommitDurationMicroseconds),
        );
        let _ = write!(out, ",\"durable_flush_duration_us\":");
        write_flight_histogram_summary(
            &mut out,
            &m.required_histogram(RequiredHistogram::DurableFlushDurationMicroseconds),
        );
        let _ = write!(out, ",\"storage_queue_latency_us\":");
        write_flight_histogram_summary(
            &mut out,
            &m.required_histogram(RequiredHistogram::StorageQueueLatencyMicroseconds),
        );
        let _ = write!(out, ",\"lock_wait_us\":");
        write_flight_histogram_summary(
            &mut out,
            &m.required_histogram(RequiredHistogram::LockWaitMicroseconds),
        );
        let _ = write!(out, ",\"commit_batch_size\":");
        write_flight_histogram_summary(
            &mut out,
            &m.required_histogram(RequiredHistogram::CommitBatchSize),
        );

        let _ = write!(out, ",\"command_stages_us\":{{");
        for (idx, stage) in COMMAND_STAGES.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            let _ = write!(out, "\"{}\":", command_stage_label(*stage));
            write_flight_histogram_summary(&mut out, &m.command_stage_duration(*stage));
        }
        out.push('}');

        let _ = write!(out, ",\"read_stages_us\":{{");
        for (idx, stage) in ReadPipelineStage::ALL.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            let _ = write!(out, "\"{}\":", stage.metric_label());
            write_flight_histogram_summary(&mut out, &m.read_stage_duration(*stage));
        }
        let _ = writeln!(out, "}}}}");
        out
    }
}

const COMMAND_STAGES: [CommandPipelineStage; 5] = [
    CommandPipelineStage::Admission,
    CommandPipelineStage::Compatibility,
    CommandPipelineStage::Evaluation,
    CommandPipelineStage::ValidationEncodingStaging,
    CommandPipelineStage::Publication,
];

const fn command_stage_label(stage: CommandPipelineStage) -> &'static str {
    match stage {
        CommandPipelineStage::Admission => "admission",
        CommandPipelineStage::Compatibility => "compatibility",
        CommandPipelineStage::Evaluation => "evaluation",
        CommandPipelineStage::ValidationEncodingStaging => "validation_encoding_staging",
        CommandPipelineStage::Publication => "publication",
    }
}

const fn dispatch_reason_label(reason: CommitGroupDispatchReason) -> &'static str {
    match reason {
        CommitGroupDispatchReason::Full => "full",
        CommitGroupDispatchReason::Barrier => "barrier",
        CommitGroupDispatchReason::QueueDrained => "queue_drained",
        CommitGroupDispatchReason::ReceiverClosed => "receiver_closed",
    }
}

const fn mcp_risk_label(risk: McpRiskClass) -> &'static str {
    match risk {
        McpRiskClass::AdministrativeMutation => "administrative_mutation",
        McpRiskClass::AdministrativeRead => "administrative_read",
        McpRiskClass::BoundedAdministrativeRead => "bounded_administrative_read",
        McpRiskClass::BoundedRead => "bounded_read",
        McpRiskClass::ReadOnly => "read_only",
        McpRiskClass::ReadOnlyCompute => "read_only_compute",
        McpRiskClass::ReadOnlyData => "read_only_data",
        McpRiskClass::SymbolicRead => "symbolic_read",
        McpRiskClass::ReactiveApplication => "reactive_application",
        McpRiskClass::ConsumerControl => "consumer_control",
        McpRiskClass::ApplicationMutation => "application_mutation",
        McpRiskClass::DynamicCommand => "dynamic_command",
    }
}

const fn ingress_label(ingress: ServiceIngressKindV1) -> &'static str {
    match ingress {
        ServiceIngressKindV1::Grpc => "grpc",
        ServiceIngressKindV1::McpHttp => "mcp_http",
        ServiceIngressKindV1::InProcessTestComparison => "in_process_test_comparison",
    }
}

const fn command_terminal_label(terminal: CommitCommandTerminal) -> &'static str {
    match terminal {
        CommitCommandTerminal::ReadOnlySucceeded => "read_only_succeeded",
        CommitCommandTerminal::FirstCommit => "first_commit",
        CommitCommandTerminal::OutcomeReplay => "outcome_replay",
        CommitCommandTerminal::ExecutionFailed => "execution_failed",
        CommitCommandTerminal::PreparationChanged => "preparation_changed",
        CommitCommandTerminal::InputMismatch => "input_mismatch",
        CommitCommandTerminal::Failed(kind) => match kind {
            crate::CommitExecutionFailureKind::AuthorizationDenied => "authorization_denied",
            crate::CommitExecutionFailureKind::Cancelled => "cancelled",
            crate::CommitExecutionFailureKind::DeadlineExceeded => "deadline_exceeded",
            crate::CommitExecutionFailureKind::RetryBudgetExhausted => "retry_budget_exhausted",
            crate::CommitExecutionFailureKind::StorageUnavailable => "storage_unavailable",
            crate::CommitExecutionFailureKind::OutcomeUnknown => "outcome_unknown",
            crate::CommitExecutionFailureKind::InternalDefect => "internal_defect",
            crate::CommitExecutionFailureKind::CoordinatorStopped => "coordinator_stopped",
            crate::CommitExecutionFailureKind::CoordinatorFenced => "coordinator_fenced",
        },
    }
}

fn write_prometheus_sample(
    out: &mut String,
    name: &'static str,
    labels: &[Option<MetricLabel>; 2],
    value: u64,
) {
    let _ = write!(out, "{name}");
    if labels[0].is_some() || labels[1].is_some() {
        out.push('{');
        let mut wrote_any = false;
        for label in labels.iter().flatten() {
            if wrote_any {
                out.push(',');
            }
            let _ = write!(out, "{}=\"{}\"", label.key, label.value);
            wrote_any = true;
        }
        out.push('}');
    }
    let _ = writeln!(out, " {value}");
}

fn write_prometheus_unlabeled_counter(out: &mut String, name: &'static str, value: u64) {
    let _ = writeln!(out, "# HELP {name} Closed RiffDB required counter.");
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

fn write_prometheus_optional_gauge(out: &mut String, name: &'static str, value: Option<u64>) {
    if let Some(val) = value {
        let _ = writeln!(out, "# HELP {name} Closed RiffDB current gauge.");
        let _ = writeln!(out, "# TYPE {name} gauge");
        let _ = writeln!(out, "{name} {val}");
    }
}

fn write_prometheus_required_histogram(
    out: &mut String,
    name: &'static str,
    snapshot: &HistogramSnapshot,
) {
    let _ = writeln!(out, "# HELP {name} Closed RiffDB fixed-bucket histogram.");
    let _ = writeln!(out, "# TYPE {name} histogram");
    write_prometheus_histogram(out, name, &[], snapshot);
}

fn write_prometheus_histogram(
    out: &mut String,
    name: &'static str,
    labels: &[MetricLabel],
    snapshot: &HistogramSnapshot,
) {
    let mut prefix = String::new();
    for (idx, label) in labels.iter().enumerate() {
        if idx > 0 {
            prefix.push(',');
        }
        let _ = write!(prefix, "{}=\"{}\"", label.key, label.value);
    }
    write_prometheus_histogram_with_raw_labels(out, name, &prefix, snapshot);
}

fn write_prometheus_histogram_with_raw_labels(
    out: &mut String,
    name: &'static str,
    label_prefix: &str,
    snapshot: &HistogramSnapshot,
) {
    for (bound, cumulative) in HISTOGRAM_UPPER_BOUNDS
        .iter()
        .zip(snapshot.cumulative_buckets.iter())
    {
        if label_prefix.is_empty() {
            if *bound == u64::MAX {
                let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
            } else {
                let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
            }
        } else if *bound == u64::MAX {
            let _ = writeln!(
                out,
                "{name}_bucket{{{label_prefix},le=\"+Inf\"}} {cumulative}"
            );
        } else {
            let _ = writeln!(
                out,
                "{name}_bucket{{{label_prefix},le=\"{bound}\"}} {cumulative}"
            );
        }
    }
    if label_prefix.is_empty() {
        let _ = writeln!(out, "{name}_sum {}", snapshot.sum);
        let _ = writeln!(out, "{name}_count {}", snapshot.count);
    } else {
        let _ = writeln!(out, "{name}_sum{{{label_prefix}}} {}", snapshot.sum);
        let _ = writeln!(out, "{name}_count{{{label_prefix}}} {}", snapshot.count);
    }
}

fn write_otel_attributes(out: &mut String, labels: &[MetricLabel]) {
    out.push('[');
    for (idx, label) in labels.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"key\":\"{}\",\"value\":{{\"stringValue\":\"{}\"}}}}",
            label.key, label.value
        );
    }
    out.push(']');
}

fn write_otel_counter(
    out: &mut String,
    first: &mut bool,
    name: &'static str,
    timestamp_unix_nanos: u64,
    labels: &[MetricLabel],
    value: u64,
) {
    if !*first {
        out.push(',');
    }
    *first = false;
    let _ = write!(
        out,
        "{{\"name\":\"{name}\",\"sum\":{{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[{{\"attributes\":"
    );
    write_otel_attributes(out, labels);
    let _ = write!(
        out,
        ",\"timeUnixNano\":\"{timestamp_unix_nanos}\",\"asInt\":\"{value}\"}}]}}}}"
    );
}

fn write_otel_gauge(
    out: &mut String,
    first: &mut bool,
    name: &'static str,
    timestamp_unix_nanos: u64,
    value: u64,
) {
    if !*first {
        out.push(',');
    }
    *first = false;
    let _ = write!(
        out,
        "{{\"name\":\"{name}\",\"gauge\":{{\"dataPoints\":[{{\"attributes\":[],\"timeUnixNano\":\"{timestamp_unix_nanos}\",\"asInt\":\"{value}\"}}]}}}}"
    );
}

fn write_otel_histogram(
    out: &mut String,
    first: &mut bool,
    name: &'static str,
    timestamp_unix_nanos: u64,
    labels: &[MetricLabel],
    snapshot: &HistogramSnapshot,
) {
    if !*first {
        out.push(',');
    }
    *first = false;
    let non_cumulative = snapshot.non_cumulative_buckets();
    let _ = write!(
        out,
        "{{\"name\":\"{name}\",\"histogram\":{{\"aggregationTemporality\":2,\"dataPoints\":[{{\"attributes\":"
    );
    write_otel_attributes(out, labels);
    let _ = write!(
        out,
        ",\"timeUnixNano\":\"{timestamp_unix_nanos}\",\"count\":\"{}\",\"sum\":{},\"bucketCounts\":[",
        snapshot.count, snapshot.sum
    );
    for (idx, bucket) in non_cumulative.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        let _ = write!(out, "\"{bucket}\"");
    }
    let _ = write!(
        out,
        "],\"explicitBounds\":[0,1,2,4,8,16,32,64,128,256,1000,10000,100000,1000000,10000000]}}]}}}}"
    );
}

fn write_flight_histogram_summary(out: &mut String, snapshot: &HistogramSnapshot) {
    let p50 = snapshot.approx_percentile_upper_bound(500);
    let p95 = snapshot.approx_percentile_upper_bound(950);
    let p99 = snapshot.approx_percentile_upper_bound(990);
    let _ = write!(
        out,
        "{{\"count\":{},\"sum\":{},\"p50\":{p50},\"p95\":{p95},\"p99\":{p99}}}",
        snapshot.count, snapshot.sum
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use riffdb_errors::{IncidentIdSource, IncidentIdSourceError};
    use riffdb_types::{CommandId, IncidentId, ServiceIngressKindV1};

    use super::*;
    use crate::{
        AuthoritativeComponent, AuthoritativeCondition, CommandPipelineStage,
        CommitCommandTerminal, CommitGroupDispatchReason, McpRiskClass, ReadPipelineStage,
        RequiredCounter, RequiredGauge, RequiredHistogram,
    };

    struct FixedIncidentIds;
    impl IncidentIdSource for FixedIncidentIds {
        fn next_incident_id(&self) -> Result<IncidentId, IncidentIdSourceError> {
            let mut bytes = [0x11_u8; 16];
            bytes[6] = (bytes[6] & 0x0f) | 0x70;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            IncidentId::from_bytes(bytes).map_err(|_| IncidentIdSourceError)
        }
    }

    #[test]
    // req: ID-005, PERF-001, END-007
    fn prometheus_and_otel_exposition_cover_required_inventory_and_preserve_redaction() {
        let obs = Observability::new(Arc::new(FixedIncidentIds), 16).expect("observability");
        let m = obs.metrics();
        m.increment_required_counter(RequiredCounter::Commits);
        m.observe_required_histogram(RequiredHistogram::CommitDurationMicroseconds, 32);
        m.observe_required_histogram(RequiredHistogram::CommitDurationMicroseconds, 250);
        m.set_required_gauge(RequiredGauge::StorageBytes, 65_536);
        m.set_required_gauge(RequiredGauge::ActiveContractVersion, 2);
        m.observe_command(
            CommandId::new(7).expect("nonzero"),
            ServiceIngressKindV1::Grpc,
            CommitCommandTerminal::FirstCommit,
            120,
        );
        m.increment_mcp_tool_call(McpRiskClass::ApplicationMutation);
        m.record_command_group_dispatch(CommitGroupDispatchReason::QueueDrained, 4, 1);
        m.observe_command_stage_duration(CommandPipelineStage::Evaluation, 48);
        m.observe_read_stage_duration(ReadPipelineStage::Execute, 15);
        m.add_writer_busy_microseconds(900);
        m.add_writer_idle_microseconds(100);

        obs.health().set_authoritative(
            AuthoritativeComponent::Storage,
            AuthoritativeCondition::Healthy,
        );
        obs.health().set_authoritative(
            AuthoritativeComponent::Catalog,
            AuthoritativeCondition::Healthy,
        );
        obs.health().set_authoritative(
            AuthoritativeComponent::CommitCoordinator,
            AuthoritativeCondition::Healthy,
        );

        let prom = obs.encode_prometheus_v1();
        assert!(prom.contains("riffdb_commits_total 1\n"));
        assert!(prom.contains("riffdb_storage_bytes 65536\n"));
        assert!(prom.contains("riffdb_active_contract_version 2\n"));
        assert!(prom.contains(
            "riffdb_command_requests_total{command_id=\"7\",transport=\"grpc\",terminal_class=\"first_commit\"} 1\n"
        ));
        assert!(
            prom.contains("riffdb_mcp_tool_calls_total{risk_class=\"application_mutation\"} 1\n")
        );
        assert!(prom.contains("riffdb_command_group_dispatch_total{reason=\"queue_drained\"} 1\n"));
        assert!(prom.contains(
            "riffdb_command_stage_duration_microseconds_bucket{stage=\"evaluation\",le=\"64\"} 1\n"
        ));
        assert!(prom.contains(
            "riffdb_read_stage_duration_microseconds_bucket{stage=\"execute\",le=\"16\"} 1\n"
        ));
        assert!(prom.contains("riffdb_health_authoritative_ready 1\n"));

        let otel = obs.encode_otel_json_v1(1_700_000_000_000_000_000);
        assert!(otel.starts_with("{\"resourceMetrics\":["));
        assert!(otel.contains("\"name\":\"riffdb_commits_total\""));
        assert!(otel.contains("\"name\":\"riffdb_commit_duration_microseconds\""));
        assert!(otel.contains("\"name\":\"riffdb_health_status\""));

        let flight = obs.encode_flight_record_ndjson_v1(1_700_000_000_000_000_000, 65_536, 8_192);
        assert!(flight.ends_with('\n'));
        assert_eq!(flight.lines().count(), 1);
        assert!(flight.contains("\"schema_version\":1"));
        assert!(flight.contains("\"storage_bytes\":65536"));
        assert!(flight.contains("\"journal_bytes\":8192"));
        assert!(flight.contains("\"commits_total\":1"));
        assert!(flight.contains("\"writer_busy_us_total\":900"));
        assert!(flight.contains(
            "\"commit_duration_us\":{\"count\":2,\"sum\":282,\"p50\":32,\"p95\":256,\"p99\":256}"
        ));
    }

    #[test]
    // req: STO-030, STO-031, ID-005, PERF-001, END-007, END-008, DX-014
    fn online_retention_telemetry_exposes_state_fence_and_counters_in_prometheus_otel_and_flight_recorder()
     {
        use crate::{OnlineRetentionTelemetryFence, OnlineRetentionTelemetryState};

        let obs = Observability::new(Arc::new(FixedIncidentIds), 16).expect("observability");
        let initial = obs.online_retention_snapshot();
        assert_eq!(initial.state, OnlineRetentionTelemetryState::Holding);
        assert_eq!(initial.fence, OnlineRetentionTelemetryFence::None);
        assert_eq!(initial.steps_total, 0);
        assert_eq!(initial.sequences_total, 0);
        assert_eq!(initial.watermark_sequence, 0);

        obs.set_online_retention_state(OnlineRetentionTelemetryState::Advancing);
        obs.record_online_retention_step(128, 256);
        obs.record_online_retention_step(64, 320);
        obs.set_online_retention_state(OnlineRetentionTelemetryState::Fenced);
        obs.set_online_retention_fence(OnlineRetentionTelemetryFence::UndeliveredOutbox);

        let snap = obs.online_retention_snapshot();
        assert_eq!(snap.state, OnlineRetentionTelemetryState::Fenced);
        assert_eq!(snap.fence, OnlineRetentionTelemetryFence::UndeliveredOutbox);
        assert_eq!(snap.steps_total, 2);
        assert_eq!(snap.sequences_total, 192);
        assert_eq!(snap.watermark_sequence, 320);

        let prom = obs.encode_prometheus_v1();
        assert!(prom.contains("riffdb_online_retention_state 3\n"));
        assert!(prom.contains("riffdb_online_retention_fence 4\n"));
        assert!(prom.contains("riffdb_online_retention_steps_total 2\n"));
        assert!(prom.contains("riffdb_online_retention_sequences_total 192\n"));
        assert!(prom.contains("riffdb_online_retention_watermark_sequence 320\n"));

        let otel = obs.encode_otel_json_v1(1_700_000_000_000_000_000);
        assert!(otel.contains("\"name\":\"riffdb_online_retention_state\""));
        assert!(otel.contains("\"name\":\"riffdb_online_retention_fence\""));
        assert!(otel.contains("\"name\":\"riffdb_online_retention_steps_total\""));
        assert!(otel.contains("\"name\":\"riffdb_online_retention_sequences_total\""));
        assert!(otel.contains("\"name\":\"riffdb_online_retention_watermark_sequence\""));

        let flight = obs.encode_flight_record_ndjson_v1(1_700_000_000_000_000_000, 65_536, 8_192);
        assert!(flight.contains("\"online_retention_state\":\"fenced\""));
        assert!(flight.contains("\"online_retention_fence\":\"undelivered_outbox\""));
        assert!(flight.contains("\"online_retention_steps_total\":2"));
        assert!(flight.contains("\"online_retention_sequences_total\":192"));
        assert!(flight.contains("\"online_retention_watermark_sequence\":320"));
    }
}
