//! Fixed-cardinality process logging observations. No dynamic labels or I/O.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::Level;

/// Operator output destinations; never an endpoint or path label.
#[derive(Clone, Copy, Debug)]
pub enum LogDestination {
    /// Process stderr collector.
    Stderr,
    /// Protected rotating file.
    File,
    /// Explicit OTLP collector.
    Otlp,
}
impl LogDestination {
    const ALL: [Self; 3] = [Self::Stderr, Self::File, Self::Otlp];
    const fn index(self) -> usize {
        match self {
            Self::Stderr => 0,
            Self::File => 1,
            Self::Otlp => 2,
        }
    }
    /// Closed metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stderr => "stderr",
            Self::File => "file",
            Self::Otlp => "otlp",
        }
    }
}

/// A closed disposition/reason, independent of diagnostic error prose.
#[derive(Clone, Copy, Debug)]
pub enum LogObservation {
    /// Accepted by the bounded queue, not yet written.
    Enqueued,
    /// The complete record was written.
    Written,
    /// Queue capacity or the severity reserve rejected a record.
    QueueFull,
    /// Admission had already stopped.
    Closing,
    /// The queue could not prove valid state.
    QueueUnavailable,
    /// The record exceeded its fixed encoding bound.
    RecordLimit,
    /// The output has no remaining space.
    StorageFull,
    /// Output permissions refused a write.
    PermissionDenied,
    /// The downstream output closed.
    BrokenPipe,
    /// A different bounded I/O failure occurred.
    WriteFailure,
    /// Pending at the final deadline; later physical delivery is unknown.
    Unflushed,
    /// An output worker failed independently of a record write.
    WorkerFailed,
}
const OBSERVATIONS: [LogObservation; 12] = [
    LogObservation::Enqueued,
    LogObservation::Written,
    LogObservation::QueueFull,
    LogObservation::Closing,
    LogObservation::QueueUnavailable,
    LogObservation::RecordLimit,
    LogObservation::StorageFull,
    LogObservation::PermissionDenied,
    LogObservation::BrokenPipe,
    LogObservation::WriteFailure,
    LogObservation::Unflushed,
    LogObservation::WorkerFailed,
];
const LEVELS: [Level; 5] = [
    Level::ERROR,
    Level::WARN,
    Level::INFO,
    Level::DEBUG,
    Level::TRACE,
];
const LEVEL_NAMES: [&str; 5] = ["error", "warn", "info", "debug", "trace"];
/// Maximum fixed logging counter series; independent of entity population.
pub const MAX_LOG_COUNTER_SERIES: usize = 3 * 5 * 12;
impl LogObservation {
    const fn index(self) -> usize {
        match self {
            Self::Enqueued => 0,
            Self::Written => 1,
            Self::QueueFull => 2,
            Self::Closing => 3,
            Self::QueueUnavailable => 4,
            Self::RecordLimit => 5,
            Self::StorageFull => 6,
            Self::PermissionDenied => 7,
            Self::BrokenPipe => 8,
            Self::WriteFailure => 9,
            Self::Unflushed => 10,
            Self::WorkerFailed => 11,
        }
    }
    const fn labels(self) -> (&'static str, &'static str) {
        match self {
            Self::Enqueued => ("enqueued", "none"),
            Self::Written => ("written", "none"),
            Self::QueueFull => ("dropped", "capacity"),
            Self::Closing => ("dropped", "closing"),
            Self::QueueUnavailable => ("dropped", "queue_unavailable"),
            Self::RecordLimit => ("dropped", "record_limit"),
            Self::StorageFull => ("write_failed", "storage_full"),
            Self::PermissionDenied => ("write_failed", "permission_denied"),
            Self::BrokenPipe => ("write_failed", "broken_pipe"),
            Self::WriteFailure => ("write_failed", "io"),
            Self::Unflushed => ("unflushed", "deadline"),
            Self::WorkerFailed => ("worker_failed", "state"),
        }
    }
}
fn level_index(level: Level) -> usize {
    match level {
        Level::ERROR => 0,
        Level::WARN => 1,
        Level::INFO => 2,
        Level::DEBUG => 3,
        Level::TRACE => 4,
    }
}

/// Bounded logging counters shared by process composition and encoders.
pub struct LoggingMetrics {
    counters: [AtomicU64; MAX_LOG_COUNTER_SERIES],
    depth: [AtomicU64; 3],
    enabled: [AtomicU64; 3],
    rejected: AtomicU64,
}
impl LoggingMetrics {
    const fn new() -> Self {
        Self {
            counters: [const { AtomicU64::new(0) }; MAX_LOG_COUNTER_SERIES],
            depth: [const { AtomicU64::new(0) }; 3],
            enabled: [const { AtomicU64::new(0) }; 3],
            rejected: AtomicU64::new(0),
        }
    }
    /// Counts only closed sink, severity and result labels; totals saturate.
    pub fn record(
        &self,
        sink: LogDestination,
        level: Level,
        observation: LogObservation,
        count: u64,
    ) {
        let index = (sink.index() * 5 + level_index(level)) * 12 + observation.index();
        let _ = self.counters[index].try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_add(count))
        });
    }
    /// Records rejected adapter input without retaining its contents.
    pub fn reject_input(&self) {
        let _ = self
            .rejected
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            });
    }
    /// Replaces one sink's bounded queued-record count; invalid observations are refused.
    pub fn set_depth(&self, sink: LogDestination, depth: usize) {
        if depth > 4096 {
            self.reject_input();
            return;
        }
        self.depth[sink.index()].store(depth as u64, Ordering::Relaxed);
    }
    /// Reports whether a process-owned sink accepts records.
    pub fn set_enabled(&self, sink: LogDestination, enabled: bool) {
        self.enabled[sink.index()].store(u64::from(enabled), Ordering::Relaxed);
    }
    pub(crate) fn write_prometheus(&self, out: &mut String) {
        let _ = writeln!(
            out,
            "# HELP riffdb_logging_records_total Closed logging dispositions; enqueued is not delivery, unflushed is unknown at deadline.\n# TYPE riffdb_logging_records_total counter"
        );
        for sink in LogDestination::ALL {
            for (index, level) in LEVELS.iter().enumerate() {
                for observation in OBSERVATIONS {
                    let value = self.counters
                        [(sink.index() * 5 + level_index(*level)) * 12 + observation.index()]
                    .load(Ordering::Relaxed);
                    let (outcome, reason) = observation.labels();
                    let _ = writeln!(
                        out,
                        "riffdb_logging_records_total{{sink=\"{}\",severity=\"{}\",outcome=\"{outcome}\",reason=\"{reason}\"}} {value}",
                        sink.as_str(),
                        LEVEL_NAMES[index]
                    );
                }
            }
        }
        let _ = writeln!(
            out,
            "# TYPE riffdb_logging_queue_depth gauge\n# TYPE riffdb_logging_sink_enabled gauge"
        );
        for sink in LogDestination::ALL {
            let _ = writeln!(
                out,
                "riffdb_logging_queue_depth{{sink=\"{}\"}} {}",
                sink.as_str(),
                self.depth[sink.index()].load(Ordering::Relaxed)
            );
            let _ = writeln!(
                out,
                "riffdb_logging_sink_enabled{{sink=\"{}\"}} {}",
                sink.as_str(),
                self.enabled[sink.index()].load(Ordering::Relaxed)
            );
        }
        let _ = writeln!(
            out,
            "# TYPE riffdb_logging_input_rejected_total counter\nriffdb_logging_input_rejected_total {}",
            self.rejected.load(Ordering::Relaxed)
        );
    }
    pub(crate) fn write_otel(&self, out: &mut String, timestamp: u64, first: &mut bool) {
        if !*first {
            out.push(',');
        }
        *first = false;
        out.push_str("{\"name\":\"riffdb_logging_records_total\",\"sum\":{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[");
        let mut comma = false;
        for sink in LogDestination::ALL {
            for (index, level) in LEVELS.iter().enumerate() {
                for observation in OBSERVATIONS {
                    if comma {
                        out.push(',');
                    }
                    comma = true;
                    let (outcome, reason) = observation.labels();
                    let value = self.counters
                        [(sink.index() * 5 + level_index(*level)) * 12 + observation.index()]
                    .load(Ordering::Relaxed);
                    let _ = write!(
                        out,
                        "{{\"attributes\":[{{\"key\":\"sink\",\"value\":{{\"stringValue\":\"{}\"}}}},{{\"key\":\"severity\",\"value\":{{\"stringValue\":\"{}\"}}}},{{\"key\":\"outcome\",\"value\":{{\"stringValue\":\"{outcome}\"}}}},{{\"key\":\"reason\",\"value\":{{\"stringValue\":\"{reason}\"}}}}],\"timeUnixNano\":\"{timestamp}\",\"asInt\":\"{value}\"}}",
                        sink.as_str(),
                        LEVEL_NAMES[index]
                    );
                }
            }
        }
        out.push_str("]}}");
        for (name, values) in [
            ("riffdb_logging_queue_depth", &self.depth),
            ("riffdb_logging_sink_enabled", &self.enabled),
        ] {
            let _ = write!(out, ",{{\"name\":\"{name}\",\"gauge\":{{\"dataPoints\":[");
            for (index, sink) in LogDestination::ALL.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                let value = values[sink.index()].load(Ordering::Relaxed);
                let _ = write!(
                    out,
                    "{{\"attributes\":[{{\"key\":\"sink\",\"value\":{{\"stringValue\":\"{}\"}}}}],\"timeUnixNano\":\"{timestamp}\",\"asInt\":\"{value}\"}}",
                    sink.as_str()
                );
            }
            out.push_str("]}}");
        }
        let value = self.rejected.load(Ordering::Relaxed);
        let _ = write!(
            out,
            ",{{\"name\":\"riffdb_logging_input_rejected_total\",\"sum\":{{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[{{\"attributes\":[],\"timeUnixNano\":\"{timestamp}\",\"asInt\":\"{value}\"}}]}}}}"
        );
    }
}
static PROCESS_LOGGING: LoggingMetrics = LoggingMetrics::new();
/// Process-global diagnostics only; never command state or audit evidence.
#[must_use]
pub fn process_logging_metrics() -> &'static LoggingMetrics {
    &PROCESS_LOGGING
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: ID-005, END-007, END-008
    #[test]
    fn logging_metrics_are_closed_bounded_and_separate_enqueue_from_delivery() {
        let metrics = LoggingMetrics::new();
        metrics.record(
            LogDestination::File,
            Level::WARN,
            LogObservation::Enqueued,
            3,
        );
        metrics.record(
            LogDestination::File,
            Level::WARN,
            LogObservation::Written,
            1,
        );
        metrics.record(
            LogDestination::File,
            Level::WARN,
            LogObservation::StorageFull,
            1,
        );
        metrics.set_depth(LogDestination::File, 4096);
        metrics.set_depth(LogDestination::File, 4097);
        let mut prom = String::new();
        metrics.write_prometheus(&mut prom);
        assert!(
            prom.contains(
                "sink=\"file\",severity=\"warn\",outcome=\"enqueued\",reason=\"none\"} 3\n"
            )
        );
        assert!(
            prom.contains(
                "sink=\"file\",severity=\"warn\",outcome=\"written\",reason=\"none\"} 1\n"
            )
        );
        assert!(prom.contains("reason=\"storage_full\"} 1\n"));
        assert!(prom.contains("riffdb_logging_queue_depth{sink=\"file\"} 4096\n"));
        assert!(prom.contains("riffdb_logging_input_rejected_total 1\n"));
        let mut json = String::new();
        metrics.write_otel(&mut json, 42, &mut true);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&format!("[{json}]")).unwrap();
        assert_eq!(
            parsed[0]["sum"]["dataPoints"].as_array().unwrap().len(),
            MAX_LOG_COUNTER_SERIES
        );
        assert!(json.len() < 128 * 1024);
    }
}
