//! Closed process signal identity; endpoints and request identities cannot become labels.
/// Diagnostic signal identity for explicit exporter configuration and fixed metrics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetrySignal {
    /// Checked operational records.
    Logs,
    /// Bounded request spans.
    Traces,
    /// Existing cumulative metric snapshots.
    Metrics,
}
impl TelemetrySignal {
    /// Closed label; never a caller-selected string.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::Traces => "traces",
            Self::Metrics => "metrics",
        }
    }
}

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
impl TelemetrySignal {
    /// Complete closed process signal inventory.
    pub const ALL: [Self; 3] = [Self::Logs, Self::Traces, Self::Metrics];
    const fn index(self) -> usize {
        match self {
            Self::Logs => 0,
            Self::Traces => 1,
            Self::Metrics => 2,
        }
    }
}
/// Observable signal outcomes with fixed disposition/reason pairs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetryDisposition {
    /// Entered a bounded queue; not delivery.
    Enqueued,
    /// Collector response confirmed ingestion.
    Accepted,
    /// Collector reported aggregate partial rejection.
    Rejected,
    /// Queue or severity reserve was full.
    QueueFull,
    /// Admission had stopped.
    Closing,
    /// Encoder could not produce an admitted request.
    Encoding,
    /// Request exceeded one MiB.
    RequestLimit,
    /// Resolver failed.
    Dns,
    /// Connection failed.
    Connect,
    /// Verified TLS failed.
    Tls,
    /// HTTP transport failed.
    Transport,
    /// Response exceeded 64 KiB.
    ResponseLimit,
    /// Complete valid OTLP response was not available.
    ResponseInvalid,
    /// Collector returned a terminal HTTP failure.
    Http,
    /// Whole-export deadline expired.
    Timeout,
    /// Disposition was still unknown at shutdown deadline.
    Unflushed,
    /// Local sampling did not record this span.
    SampledOut,
    /// Process span bound refused an observation.
    SpanLimit,
    /// Protected collector credentials could not be loaded.
    Credential,
}
const DISPOSITIONS: [TelemetryDisposition; 19] = [
    TelemetryDisposition::Enqueued,
    TelemetryDisposition::Accepted,
    TelemetryDisposition::Rejected,
    TelemetryDisposition::QueueFull,
    TelemetryDisposition::Closing,
    TelemetryDisposition::Encoding,
    TelemetryDisposition::RequestLimit,
    TelemetryDisposition::Dns,
    TelemetryDisposition::Connect,
    TelemetryDisposition::Tls,
    TelemetryDisposition::Transport,
    TelemetryDisposition::ResponseLimit,
    TelemetryDisposition::ResponseInvalid,
    TelemetryDisposition::Http,
    TelemetryDisposition::Timeout,
    TelemetryDisposition::Unflushed,
    TelemetryDisposition::SampledOut,
    TelemetryDisposition::SpanLimit,
    TelemetryDisposition::Credential,
];
impl TelemetryDisposition {
    const fn index(self) -> usize {
        self as usize
    }
    /// Closed outcome and reason for local delivery diagnostics.
    #[must_use]
    pub const fn labels(self) -> (&'static str, &'static str) {
        match self {
            Self::Enqueued => ("enqueued", "none"),
            Self::Accepted => ("accepted", "none"),
            Self::Rejected => ("rejected", "partial"),
            Self::QueueFull => ("dropped", "capacity"),
            Self::Closing => ("dropped", "closing"),
            Self::Encoding => ("failed", "encoding"),
            Self::RequestLimit => ("failed", "request_limit"),
            Self::Dns => ("failed", "dns"),
            Self::Connect => ("failed", "connect"),
            Self::Tls => ("failed", "tls"),
            Self::Transport => ("failed", "transport"),
            Self::ResponseLimit => ("failed", "response_limit"),
            Self::ResponseInvalid => ("failed", "response_invalid"),
            Self::Http => ("failed", "http"),
            Self::Timeout => ("failed", "timeout"),
            Self::Unflushed => ("unflushed", "deadline"),
            Self::SampledOut => ("dropped", "sampling"),
            Self::SpanLimit => ("dropped", "span_limit"),
            Self::Credential => ("failed", "credential"),
        }
    }
}
/// Number of fixed counter series, independent of endpoints or database population.
pub const MAX_TELEMETRY_COUNTER_SERIES: usize = 3 * DISPOSITIONS.len();
/// Process exporter observations. This type performs no I/O or clock acquisition.
pub struct TelemetryDeliveryMetrics {
    counts: [AtomicU64; MAX_TELEMETRY_COUNTER_SERIES],
    enabled: [AtomicU64; 3],
    depth: [AtomicU64; 3],
    inflight: [AtomicU64; 3],
    exports: [AtomicU64; 3],
    duration_ms: [AtomicU64; 3],
    last_duration_ms: [AtomicU64; 3],
    last_success_ns: [AtomicU64; 3],
}
fn add(counter: &AtomicU64, count: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(count))
    });
}
impl TelemetryDeliveryMetrics {
    const fn new() -> Self {
        Self {
            counts: [const { AtomicU64::new(0) }; MAX_TELEMETRY_COUNTER_SERIES],
            enabled: [const { AtomicU64::new(0) }; 3],
            depth: [const { AtomicU64::new(0) }; 3],
            inflight: [const { AtomicU64::new(0) }; 3],
            exports: [const { AtomicU64::new(0) }; 3],
            duration_ms: [const { AtomicU64::new(0) }; 3],
            last_duration_ms: [const { AtomicU64::new(0) }; 3],
            last_success_ns: [const { AtomicU64::new(0) }; 3],
        }
    }
    /// Marks a configured process sink; no endpoint or path is retained.
    pub fn set_enabled(&self, signal: TelemetrySignal, enabled: bool) {
        self.enabled[signal.index()].store(u64::from(enabled), Ordering::Relaxed);
    }
    /// Records a monotonic count by closed signal, sink and disposition.
    pub fn record(&self, signal: TelemetrySignal, disposition: TelemetryDisposition, count: u64) {
        add(
            &self.counts[signal.index() * DISPOSITIONS.len() + disposition.index()],
            count,
        );
    }
    /// Reports waiting records independently of the one in-flight batch.
    pub fn set_depth(&self, signal: TelemetrySignal, depth: usize) {
        if depth <= 4096 {
            self.depth[signal.index()].store(depth as u64, Ordering::Relaxed);
        }
    }
    /// Reports at most one admitted batch, bounded to 512 records.
    pub fn set_inflight(&self, signal: TelemetrySignal, count: usize) {
        if count <= 512 {
            self.inflight[signal.index()].store(count as u64, Ordering::Relaxed);
        }
    }
    /// Records a completed export duration measured by the owning worker.
    pub fn export_completed(&self, signal: TelemetrySignal, duration_ms: u64) {
        let index = signal.index();
        add(&self.exports[index], 1);
        add(&self.duration_ms[index], duration_ms);
        self.last_duration_ms[index].store(duration_ms, Ordering::Relaxed);
    }
    /// Records a confirmed accepted export using the caller's bounded wall timestamp.
    pub fn accepted_at(&self, signal: TelemetrySignal, timestamp_unix_nanos: u64) {
        self.last_success_ns[signal.index()].fetch_max(timestamp_unix_nanos, Ordering::Relaxed);
    }
    fn gauges(&self, signal: TelemetrySignal, timestamp: u64) -> [(&'static str, i128); 7] {
        let index = signal.index();
        let success = self.last_success_ns[index].load(Ordering::Relaxed);
        let age = if success == 0 || timestamp == 0 {
            -1
        } else {
            i128::from(timestamp.saturating_sub(success) / 1_000_000_000)
        };
        [
            (
                "riffdb_telemetry_queue_depth",
                i128::from(self.depth[index].load(Ordering::Relaxed)),
            ),
            (
                "riffdb_telemetry_inflight_records",
                i128::from(self.inflight[index].load(Ordering::Relaxed)),
            ),
            (
                "riffdb_telemetry_export_batches_total",
                i128::from(self.exports[index].load(Ordering::Relaxed)),
            ),
            (
                "riffdb_telemetry_export_duration_milliseconds_total",
                i128::from(self.duration_ms[index].load(Ordering::Relaxed)),
            ),
            (
                "riffdb_telemetry_export_duration_milliseconds",
                i128::from(self.last_duration_ms[index].load(Ordering::Relaxed)),
            ),
            ("riffdb_telemetry_last_success_age_seconds", age),
            (
                "riffdb_telemetry_last_success_timestamp_seconds",
                i128::from(success / 1_000_000_000),
            ),
        ]
    }
    pub(crate) fn write_prometheus(&self, out: &mut String, timestamp: u64) {
        let signals = TelemetrySignal::ALL
            .into_iter()
            .filter(|signal| self.enabled[signal.index()].load(Ordering::Relaxed) != 0)
            .collect::<Vec<_>>();
        if signals.is_empty() {
            return;
        }
        let _ = writeln!(
            out,
            "# HELP riffdb_telemetry_records_total Closed collector evidence; enqueue and writing do not imply acceptance.\n# TYPE riffdb_telemetry_records_total counter"
        );
        for signal in &signals {
            for disposition in DISPOSITIONS {
                let (outcome, reason) = disposition.labels();
                let count = self.counts[signal.index() * DISPOSITIONS.len() + disposition.index()]
                    .load(Ordering::Relaxed);
                let _ = writeln!(
                    out,
                    "riffdb_telemetry_records_total{{signal=\"{}\",sink=\"otlp\",outcome=\"{outcome}\",reason=\"{reason}\"}} {count}",
                    signal.name()
                );
            }
        }
        for index in 0..7 {
            let name = self.gauges(signals[0], timestamp)[index].0;
            let _ = writeln!(
                out,
                "# TYPE {name} {}",
                if name.ends_with("_total") {
                    "counter"
                } else {
                    "gauge"
                }
            );
            for signal in &signals {
                let (_, value) = self.gauges(*signal, timestamp)[index];
                let _ = writeln!(
                    out,
                    "{name}{{signal=\"{}\",sink=\"otlp\"}} {value}",
                    signal.name()
                );
            }
        }
    }
    pub(crate) fn write_otel(&self, out: &mut String, timestamp: u64, first: &mut bool) {
        let signals = TelemetrySignal::ALL
            .into_iter()
            .filter(|signal| self.enabled[signal.index()].load(Ordering::Relaxed) != 0)
            .collect::<Vec<_>>();
        if signals.is_empty() {
            return;
        }
        if !*first {
            out.push(',');
        }
        *first = false;
        out.push_str("{\"name\":\"riffdb_telemetry_records_total\",\"sum\":{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[");
        let mut point_first = true;
        for signal in &signals {
            for disposition in DISPOSITIONS {
                let labels = disposition.labels();
                let count = self.counts[signal.index() * DISPOSITIONS.len() + disposition.index()]
                    .load(Ordering::Relaxed);
                write_point(
                    out,
                    *signal,
                    timestamp,
                    i128::from(count),
                    Some(labels),
                    &mut point_first,
                );
            }
        }
        out.push_str("]}}");
        for index in 0..7 {
            let name = self.gauges(signals[0], timestamp)[index].0;
            out.push(',');
            let counter = name.ends_with("_total");
            let _ = write!(
                out,
                "{{\"name\":\"{name}\",\"{}\":{{{}\"dataPoints\":[",
                if counter { "sum" } else { "gauge" },
                if counter {
                    "\"aggregationTemporality\":2,\"isMonotonic\":true,"
                } else {
                    ""
                }
            );
            let mut point_first = true;
            for signal in &signals {
                write_point(
                    out,
                    *signal,
                    timestamp,
                    self.gauges(*signal, timestamp)[index].1,
                    None,
                    &mut point_first,
                );
            }
            out.push_str("]}}");
        }
    }
}
fn write_point(
    out: &mut String,
    signal: TelemetrySignal,
    timestamp: u64,
    value: i128,
    labels: Option<(&str, &str)>,
    first: &mut bool,
) {
    if !*first {
        out.push(',');
    }
    *first = false;
    let value = value.clamp(i128::from(i64::MIN), i128::from(i64::MAX));
    let _ = write!(
        out,
        "{{\"attributes\":[{{\"key\":\"signal\",\"value\":{{\"stringValue\":\"{}\"}}}},{{\"key\":\"sink\",\"value\":{{\"stringValue\":\"otlp\"}}}}",
        signal.name()
    );
    if let Some((outcome, reason)) = labels {
        let _ = write!(
            out,
            ",{{\"key\":\"outcome\",\"value\":{{\"stringValue\":\"{outcome}\"}}}},{{\"key\":\"reason\",\"value\":{{\"stringValue\":\"{reason}\"}}}}"
        );
    }
    let _ = write!(
        out,
        "],\"timeUnixNano\":\"{timestamp}\",\"asInt\":\"{value}\"}}"
    );
}
static DELIVERY: TelemetryDeliveryMetrics = TelemetryDeliveryMetrics::new();
/// Bounded process counters shared by collector workers and metric exposition.
#[must_use]
pub fn process_telemetry_delivery_metrics() -> &'static TelemetryDeliveryMetrics {
    &DELIVERY
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: ID-005, END-007, END-008, PERF-001
    #[test]
    fn delivery_series_are_closed_and_acceptance_age_uses_only_supplied_timestamps() {
        let metrics = TelemetryDeliveryMetrics::new();
        metrics.set_enabled(TelemetrySignal::Logs, true);
        metrics.record(TelemetrySignal::Logs, TelemetryDisposition::Accepted, 3);
        metrics.record(TelemetrySignal::Logs, TelemetryDisposition::Rejected, 2);
        metrics.accepted_at(TelemetrySignal::Logs, 2_000_000_000);
        let mut output = String::new();
        metrics.write_prometheus(&mut output, 5_000_000_000);
        assert_eq!(
            output
                .lines()
                .filter(|line| line.starts_with("riffdb_telemetry_records_total"))
                .count(),
            19
        );
        assert!(output.contains(
            "riffdb_telemetry_last_success_age_seconds{signal=\"logs\",sink=\"otlp\"} 3"
        ));
        let mut first = true;
        let mut otel = String::new();
        metrics.write_otel(&mut otel, 5_000_000_000, &mut first);
        assert!(!otel.contains("endpoint"));
        assert!(!otel.contains("request_id"));
        assert_eq!(MAX_TELEMETRY_COUNTER_SERIES, 57);
    }
    // req: ID-005, PERF-001, END-007, END-008
    #[test]
    fn delivery_otlp_families_have_unique_names_fixed_labels_and_native_numeric_values() {
        let metrics = TelemetryDeliveryMetrics::new();
        for signal in TelemetrySignal::ALL {
            metrics.set_enabled(signal, true);
            metrics.record(signal, TelemetryDisposition::Accepted, u64::MAX);
        }
        let mut first = true;
        let mut encoded = String::new();
        metrics.write_otel(&mut encoded, 5_000_000_000, &mut first);
        let families: serde_json::Value = serde_json::from_str(&format!("[{encoded}]")).unwrap();
        let mut names = std::collections::BTreeSet::new();
        let mut points = 0;
        for family in families.as_array().unwrap() {
            assert!(names.insert(family["name"].as_str().unwrap()));
            let data = if family.get("sum").is_some() {
                assert_eq!(family["sum"]["aggregationTemporality"], 2);
                assert_eq!(family["sum"]["isMonotonic"], true);
                &family["sum"]
            } else {
                &family["gauge"]
            };
            for point in data["dataPoints"].as_array().unwrap() {
                points += 1;
                assert!(point["asInt"].as_str().unwrap().parse::<i64>().is_ok());
                assert_eq!(point["timeUnixNano"], "5000000000");
                for attribute in point["attributes"].as_array().unwrap() {
                    assert!(matches!(
                        attribute["key"].as_str().unwrap(),
                        "signal" | "sink" | "outcome" | "reason"
                    ));
                }
            }
        }
        assert_eq!(names.len(), 8);
        assert_eq!(points, 57 + 21);
    }
}
