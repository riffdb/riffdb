//! Offline soak and flight-recorder NDJSON summary (`ADR-0306`).

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read as _};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::cli::TelemetryCommand;
use crate::output::{CommandIdentity, Terminal, local_error, success};

/// Maximum flight-recorder NDJSON file size accepted by `riffdb telemetry summarize` (64 MiB).
pub(crate) const MAX_FLIGHT_RECORD_FILE_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum byte length of a single flight-recorder NDJSON line (64 KiB).
const MAX_FLIGHT_RECORD_LINE_BYTES: usize = 64 * 1024;
/// Maximum number of samples aggregated in a single summary invocation.
const MAX_FLIGHT_RECORD_SAMPLES: usize = 100_000;
const EXPECTED_SCHEMA: &str = "riffdb.telemetry.flight-record.v1";

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
struct FlightHistogramSummary {
    count: u64,
    sum: u64,
    p50: u64,
    p95: u64,
    p99: u64,
}

#[derive(Debug, Deserialize)]
struct FlightRecordSample {
    #[serde(default)]
    schema: Option<String>,
    schema_version: u32,
    timestamp_unix_nanos: u64,
    health_classification: String,
    #[allow(dead_code)]
    authoritative_ready: bool,
    last_commit_sequence: Option<u64>,
    storage_bytes: u64,
    journal_bytes: u64,
    commits_total: u64,
    command_requests_total: u64,
    lock_timeouts_total: u64,
    idempotency_hits_total: u64,
    idempotency_mismatches_total: u64,
    telemetry_dropped_total: u64,
    writer_busy_us_total: u64,
    writer_idle_us_total: u64,
    queue_delay_estimate_us: u64,
    conflict_queue_depth: u64,
    hot_conflict_key_cardinality: u64,
    outbox_pending: u64,
    outbox_dead_letters: u64,
    #[serde(default)]
    online_retention_state: Option<String>,
    #[serde(default)]
    online_retention_fence: Option<String>,
    #[serde(default)]
    online_retention_steps_total: u64,
    #[serde(default)]
    online_retention_sequences_total: u64,
    #[serde(default)]
    online_retention_watermark_sequence: u64,
    commit_duration_us: FlightHistogramSummary,
    durable_flush_duration_us: FlightHistogramSummary,
    storage_queue_latency_us: FlightHistogramSummary,
    lock_wait_us: FlightHistogramSummary,
    #[allow(dead_code)]
    commit_batch_size: FlightHistogramSummary,
    command_stages_us: BTreeMap<String, FlightHistogramSummary>,
    read_stages_us: BTreeMap<String, FlightHistogramSummary>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
pub(crate) struct ByteGrowthSummary {
    pub(crate) initial_bytes: u64,
    pub(crate) final_bytes: u64,
    pub(crate) peak_bytes: u64,
    pub(crate) growth_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
pub(crate) struct LatencyHighWaterSummary {
    pub(crate) max_p50_us: u64,
    pub(crate) max_p95_us: u64,
    pub(crate) max_p99_us: u64,
}

impl LatencyHighWaterSummary {
    fn observe(&mut self, sample: FlightHistogramSummary) {
        self.max_p50_us = self.max_p50_us.max(sample.p50);
        self.max_p95_us = self.max_p95_us.max(sample.p95);
        self.max_p99_us = self.max_p99_us.max(sample.p99);
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct TelemetrySummaryReport {
    pub(crate) status: &'static str,
    pub(crate) format: &'static str,
    pub(crate) samples: u64,
    pub(crate) first_timestamp_unix_nanos: u64,
    pub(crate) last_timestamp_unix_nanos: u64,
    pub(crate) window_duration_ms: u64,
    pub(crate) initial_health: String,
    pub(crate) final_health: String,
    pub(crate) health_transitions: u64,
    pub(crate) degraded_samples: u64,
    pub(crate) last_commit_sequence: Option<u64>,
    pub(crate) commits_delta: u64,
    pub(crate) command_requests_delta: u64,
    pub(crate) lock_timeouts_delta: u64,
    pub(crate) idempotency_hits_delta: u64,
    pub(crate) idempotency_mismatches_delta: u64,
    pub(crate) telemetry_dropped_delta: u64,
    pub(crate) writer_busy_permille: u64,
    pub(crate) peak_queue_delay_estimate_us: u64,
    pub(crate) peak_conflict_queue_depth: u64,
    pub(crate) peak_hot_conflict_key_cardinality: u64,
    pub(crate) peak_outbox_pending: u64,
    pub(crate) peak_outbox_dead_letters: u64,
    pub(crate) final_online_retention_state: Option<String>,
    pub(crate) final_online_retention_fence: Option<String>,
    pub(crate) online_retention_fenced_samples: u64,
    pub(crate) online_retention_steps_delta: u64,
    pub(crate) online_retention_sequences_delta: u64,
    pub(crate) online_retention_watermark_sequence: u64,
    pub(crate) storage_bytes: ByteGrowthSummary,
    pub(crate) journal_bytes: ByteGrowthSummary,
    pub(crate) commit_duration_us_high_water: LatencyHighWaterSummary,
    pub(crate) durable_flush_duration_us_high_water: LatencyHighWaterSummary,
    pub(crate) storage_queue_latency_us_high_water: LatencyHighWaterSummary,
    pub(crate) lock_wait_us_high_water: LatencyHighWaterSummary,
    pub(crate) command_stages_us_high_water: BTreeMap<String, LatencyHighWaterSummary>,
    pub(crate) read_stages_us_high_water: BTreeMap<String, LatencyHighWaterSummary>,
}

pub(crate) fn summarize(command: &TelemetryCommand) -> Terminal {
    let TelemetryCommand::Summarize { path } = command;
    let identity = CommandIdentity::TelemetrySummarize;
    match summarize_path(Path::new(path)) {
        Ok(report) => success(identity, report.status, &report),
        Err((code, message)) => local_error(identity, code, message),
    }
}

pub(crate) fn summarize_path(
    path: &Path,
) -> Result<TelemetrySummaryReport, (&'static str, &'static str)> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| {
        (
            "telemetry_flight_record_unreadable",
            "failed to inspect telemetry flight-recorder path",
        )
    })?;
    if !metadata.file_type().is_file() {
        return Err((
            "telemetry_flight_record_invalid_path",
            "telemetry flight-recorder path must name a regular file",
        ));
    }
    if metadata.len() > MAX_FLIGHT_RECORD_FILE_BYTES {
        return Err((
            "telemetry_flight_record_too_large",
            "telemetry flight-recorder file exceeds the 64 MiB CLI limit",
        ));
    }
    let file = File::open(path).map_err(|_| {
        (
            "telemetry_flight_record_unreadable",
            "failed to open telemetry flight-recorder file",
        )
    })?;
    summarize_reader(file.take(MAX_FLIGHT_RECORD_FILE_BYTES + 1))
}

fn summarize_reader<R: std::io::Read>(
    reader: R,
) -> Result<TelemetrySummaryReport, (&'static str, &'static str)> {
    let mut buf = BufReader::new(reader);
    let mut line = String::new();
    let mut total_bytes: u64 = 0;
    let mut samples: u64 = 0;

    let mut first_timestamp_unix_nanos = 0_u64;
    let mut last_timestamp_unix_nanos = 0_u64;
    let mut initial_health = String::new();
    let mut prev_health = String::new();
    let mut health_transitions = 0_u64;
    let mut degraded_samples = 0_u64;
    let mut last_commit_sequence = None;

    let mut prev_commits = 0_u64;
    let mut prev_requests = 0_u64;
    let mut prev_lock_timeouts = 0_u64;
    let mut prev_idempotency_hits = 0_u64;
    let mut prev_idempotency_mismatches = 0_u64;
    let mut prev_telemetry_dropped = 0_u64;
    let mut prev_writer_busy = 0_u64;
    let mut prev_writer_idle = 0_u64;
    let mut prev_retention_steps = 0_u64;
    let mut prev_retention_sequences = 0_u64;

    let mut commits_delta = 0_u64;
    let mut command_requests_delta = 0_u64;
    let mut lock_timeouts_delta = 0_u64;
    let mut idempotency_hits_delta = 0_u64;
    let mut idempotency_mismatches_delta = 0_u64;
    let mut telemetry_dropped_delta = 0_u64;
    let mut writer_busy_delta = 0_u64;
    let mut writer_idle_delta = 0_u64;
    let mut online_retention_steps_delta = 0_u64;
    let mut online_retention_sequences_delta = 0_u64;

    let mut final_online_retention_state = None;
    let mut final_online_retention_fence = None;
    let mut online_retention_fenced_samples = 0_u64;
    let mut online_retention_watermark_sequence = 0_u64;

    let mut peak_queue_delay_estimate_us = 0_u64;
    let mut peak_conflict_queue_depth = 0_u64;
    let mut peak_hot_conflict_key_cardinality = 0_u64;
    let mut peak_outbox_pending = 0_u64;
    let mut peak_outbox_dead_letters = 0_u64;

    let mut initial_storage_bytes = 0_u64;
    let mut final_storage_bytes = 0_u64;
    let mut peak_storage_bytes = 0_u64;

    let mut initial_journal_bytes = 0_u64;
    let mut final_journal_bytes = 0_u64;
    let mut peak_journal_bytes = 0_u64;

    let mut commit_duration_us_high_water = LatencyHighWaterSummary::default();
    let mut durable_flush_duration_us_high_water = LatencyHighWaterSummary::default();
    let mut storage_queue_latency_us_high_water = LatencyHighWaterSummary::default();
    let mut lock_wait_us_high_water = LatencyHighWaterSummary::default();
    let mut command_stages_us_high_water: BTreeMap<String, LatencyHighWaterSummary> =
        BTreeMap::new();
    let mut read_stages_us_high_water: BTreeMap<String, LatencyHighWaterSummary> = BTreeMap::new();

    loop {
        line.clear();
        let bytes_read = buf.read_line(&mut line).map_err(|_| {
            (
                "telemetry_flight_record_unreadable",
                "failed while reading telemetry flight-recorder file",
            )
        })?;
        if bytes_read == 0 {
            break;
        }
        total_bytes = total_bytes.saturating_add(bytes_read as u64);
        if total_bytes > MAX_FLIGHT_RECORD_FILE_BYTES {
            return Err((
                "telemetry_flight_record_too_large",
                "telemetry flight-recorder file exceeds the 64 MiB CLI limit",
            ));
        }
        if bytes_read > MAX_FLIGHT_RECORD_LINE_BYTES {
            return Err((
                "telemetry_flight_record_line_too_large",
                "telemetry flight-recorder line exceeds the 64 KiB line limit",
            ));
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if (samples as usize) >= MAX_FLIGHT_RECORD_SAMPLES {
            return Err((
                "telemetry_flight_record_too_many_samples",
                "telemetry flight-recorder file exceeds the 100000 sample limit",
            ));
        }
        let sample: FlightRecordSample = serde_json::from_str(trimmed).map_err(|_| {
            (
                "telemetry_flight_record_invalid_json",
                "telemetry flight-recorder line is not valid v1 NDJSON",
            )
        })?;
        if sample.schema_version != 1
            || sample
                .schema
                .as_deref()
                .is_some_and(|schema| schema != EXPECTED_SCHEMA)
        {
            return Err((
                "telemetry_flight_record_unsupported_version",
                "unsupported telemetry flight-recorder schema version",
            ));
        }
        if !matches!(
            sample.health_classification.as_str(),
            "not_ready" | "ready" | "degraded"
        ) {
            return Err((
                "telemetry_flight_record_invalid_health",
                "telemetry flight-recorder sample contains an unknown health classification",
            ));
        }

        if samples == 0 {
            first_timestamp_unix_nanos = sample.timestamp_unix_nanos;
            initial_health.clone_from(&sample.health_classification);
            initial_storage_bytes = sample.storage_bytes;
            initial_journal_bytes = sample.journal_bytes;
        } else {
            if sample.health_classification != prev_health {
                health_transitions = health_transitions.saturating_add(1);
            }
            commits_delta =
                commits_delta.saturating_add(counter_delta(prev_commits, sample.commits_total));
            command_requests_delta = command_requests_delta
                .saturating_add(counter_delta(prev_requests, sample.command_requests_total));
            lock_timeouts_delta = lock_timeouts_delta.saturating_add(counter_delta(
                prev_lock_timeouts,
                sample.lock_timeouts_total,
            ));
            idempotency_hits_delta = idempotency_hits_delta.saturating_add(counter_delta(
                prev_idempotency_hits,
                sample.idempotency_hits_total,
            ));
            idempotency_mismatches_delta =
                idempotency_mismatches_delta.saturating_add(counter_delta(
                    prev_idempotency_mismatches,
                    sample.idempotency_mismatches_total,
                ));
            telemetry_dropped_delta = telemetry_dropped_delta.saturating_add(counter_delta(
                prev_telemetry_dropped,
                sample.telemetry_dropped_total,
            ));
            writer_busy_delta = writer_busy_delta
                .saturating_add(counter_delta(prev_writer_busy, sample.writer_busy_us_total));
            writer_idle_delta = writer_idle_delta
                .saturating_add(counter_delta(prev_writer_idle, sample.writer_idle_us_total));
            online_retention_steps_delta = online_retention_steps_delta.saturating_add(
                counter_delta(prev_retention_steps, sample.online_retention_steps_total),
            );
            online_retention_sequences_delta =
                online_retention_sequences_delta.saturating_add(counter_delta(
                    prev_retention_sequences,
                    sample.online_retention_sequences_total,
                ));
        }

        if sample.health_classification == "degraded" {
            degraded_samples = degraded_samples.saturating_add(1);
        }
        prev_health.clone_from(&sample.health_classification);
        last_timestamp_unix_nanos = last_timestamp_unix_nanos.max(sample.timestamp_unix_nanos);
        if sample.last_commit_sequence.is_some() {
            last_commit_sequence = sample.last_commit_sequence;
        }
        if sample.online_retention_state.as_deref() == Some("fenced") {
            online_retention_fenced_samples = online_retention_fenced_samples.saturating_add(1);
        }
        if let Some(state) = sample.online_retention_state {
            final_online_retention_state = Some(state);
        }
        if let Some(fence) = sample.online_retention_fence {
            final_online_retention_fence = Some(fence);
        }
        online_retention_watermark_sequence =
            online_retention_watermark_sequence.max(sample.online_retention_watermark_sequence);

        prev_commits = sample.commits_total;
        prev_requests = sample.command_requests_total;
        prev_lock_timeouts = sample.lock_timeouts_total;
        prev_idempotency_hits = sample.idempotency_hits_total;
        prev_idempotency_mismatches = sample.idempotency_mismatches_total;
        prev_telemetry_dropped = sample.telemetry_dropped_total;
        prev_writer_busy = sample.writer_busy_us_total;
        prev_writer_idle = sample.writer_idle_us_total;
        prev_retention_steps = sample.online_retention_steps_total;
        prev_retention_sequences = sample.online_retention_sequences_total;

        peak_queue_delay_estimate_us =
            peak_queue_delay_estimate_us.max(sample.queue_delay_estimate_us);
        peak_conflict_queue_depth = peak_conflict_queue_depth.max(sample.conflict_queue_depth);
        peak_hot_conflict_key_cardinality =
            peak_hot_conflict_key_cardinality.max(sample.hot_conflict_key_cardinality);
        peak_outbox_pending = peak_outbox_pending.max(sample.outbox_pending);
        peak_outbox_dead_letters = peak_outbox_dead_letters.max(sample.outbox_dead_letters);

        final_storage_bytes = sample.storage_bytes;
        peak_storage_bytes = peak_storage_bytes.max(sample.storage_bytes);
        final_journal_bytes = sample.journal_bytes;
        peak_journal_bytes = peak_journal_bytes.max(sample.journal_bytes);

        commit_duration_us_high_water.observe(sample.commit_duration_us);
        durable_flush_duration_us_high_water.observe(sample.durable_flush_duration_us);
        storage_queue_latency_us_high_water.observe(sample.storage_queue_latency_us);
        lock_wait_us_high_water.observe(sample.lock_wait_us);

        for (stage, hist) in sample.command_stages_us {
            command_stages_us_high_water
                .entry(stage)
                .or_default()
                .observe(hist);
        }
        for (stage, hist) in sample.read_stages_us {
            read_stages_us_high_water
                .entry(stage)
                .or_default()
                .observe(hist);
        }

        samples = samples.saturating_add(1);
    }

    if samples == 0 {
        return Err((
            "telemetry_flight_record_empty",
            "telemetry flight-recorder file contains no samples",
        ));
    }

    if samples == 1 {
        commits_delta = prev_commits;
        command_requests_delta = prev_requests;
        lock_timeouts_delta = prev_lock_timeouts;
        idempotency_hits_delta = prev_idempotency_hits;
        idempotency_mismatches_delta = prev_idempotency_mismatches;
        telemetry_dropped_delta = prev_telemetry_dropped;
        writer_busy_delta = prev_writer_busy;
        writer_idle_delta = prev_writer_idle;
        online_retention_steps_delta = prev_retention_steps;
        online_retention_sequences_delta = prev_retention_sequences;
    }

    let writer_total_us = writer_busy_delta.saturating_add(writer_idle_delta);
    let writer_busy_permille = if writer_total_us == 0 {
        0
    } else {
        ((writer_busy_delta as u128) * 1_000 / (writer_total_us as u128)) as u64
    };

    let window_duration_ms =
        last_timestamp_unix_nanos.saturating_sub(first_timestamp_unix_nanos) / 1_000_000;

    Ok(TelemetrySummaryReport {
        status: "summarized",
        format: EXPECTED_SCHEMA,
        samples,
        first_timestamp_unix_nanos,
        last_timestamp_unix_nanos,
        window_duration_ms,
        initial_health,
        final_health: prev_health,
        health_transitions,
        degraded_samples,
        last_commit_sequence,
        commits_delta,
        command_requests_delta,
        lock_timeouts_delta,
        idempotency_hits_delta,
        idempotency_mismatches_delta,
        telemetry_dropped_delta,
        writer_busy_permille,
        peak_queue_delay_estimate_us,
        peak_conflict_queue_depth,
        peak_hot_conflict_key_cardinality,
        peak_outbox_pending,
        peak_outbox_dead_letters,
        final_online_retention_state,
        final_online_retention_fence,
        online_retention_fenced_samples,
        online_retention_steps_delta,
        online_retention_sequences_delta,
        online_retention_watermark_sequence,
        storage_bytes: ByteGrowthSummary {
            initial_bytes: initial_storage_bytes,
            final_bytes: final_storage_bytes,
            peak_bytes: peak_storage_bytes,
            growth_bytes: final_storage_bytes.saturating_sub(initial_storage_bytes),
        },
        journal_bytes: ByteGrowthSummary {
            initial_bytes: initial_journal_bytes,
            final_bytes: final_journal_bytes,
            peak_bytes: peak_journal_bytes,
            growth_bytes: final_journal_bytes.saturating_sub(initial_journal_bytes),
        },
        commit_duration_us_high_water,
        durable_flush_duration_us_high_water,
        storage_queue_latency_us_high_water,
        lock_wait_us_high_water,
        command_stages_us_high_water,
        read_stages_us_high_water,
    })
}

const fn counter_delta(previous: u64, current: u64) -> u64 {
    if current >= previous {
        current - previous
    } else {
        // Counter reset across server restart in multi-incarnation soak campaign.
        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::OutputMode;

    #[allow(clippy::too_many_arguments)]
    fn sample_line(
        ts_ns: u64,
        health: &str,
        storage_bytes: u64,
        journal_bytes: u64,
        commits: u64,
        busy_us: u64,
        idle_us: u64,
        commit_p99_us: u64,
    ) -> String {
        let retention_state = if commits >= 60 { "fenced" } else { "advancing" };
        let retention_fence = if commits >= 60 {
            "undelivered_outbox"
        } else {
            "none"
        };
        let retention_steps = commits / 10;
        let retention_seqs = commits / 2;
        format!(
            "{{\"schema\":\"riffdb.telemetry.flight-record.v1\",\"schema_version\":1,\"timestamp_unix_nanos\":{ts_ns},\"health_classification\":\"{health}\",\"authoritative_ready\":true,\"last_commit_sequence\":{commits},\"storage_bytes\":{storage_bytes},\"journal_bytes\":{journal_bytes},\"commits_total\":{commits},\"command_requests_total\":{commits},\"lock_timeouts_total\":0,\"idempotency_hits_total\":2,\"idempotency_mismatches_total\":0,\"telemetry_dropped_total\":0,\"writer_busy_us_total\":{busy_us},\"writer_idle_us_total\":{idle_us},\"queue_delay_estimate_us\":45,\"conflict_queue_depth\":2,\"hot_conflict_key_cardinality\":1,\"outbox_pending\":3,\"outbox_dead_letters\":0,\"online_retention_state\":\"{retention_state}\",\"online_retention_fence\":\"{retention_fence}\",\"online_retention_steps_total\":{retention_steps},\"online_retention_sequences_total\":{retention_seqs},\"online_retention_watermark_sequence\":{retention_seqs},\"commit_duration_us\":{{\"count\":{commits},\"sum\":1000,\"p50\":32,\"p95\":128,\"p99\":{commit_p99_us}}},\"durable_flush_duration_us\":{{\"count\":{commits},\"sum\":800,\"p50\":16,\"p95\":64,\"p99\":128}},\"storage_queue_latency_us\":{{\"count\":{commits},\"sum\":100,\"p50\":4,\"p95\":8,\"p99\":16}},\"lock_wait_us\":{{\"count\":{commits},\"sum\":50,\"p50\":2,\"p95\":4,\"p99\":8}},\"commit_batch_size\":{{\"count\":{commits},\"sum\":{commits},\"p50\":1,\"p95\":2,\"p99\":4}},\"command_stages_us\":{{\"evaluation\":{{\"count\":{commits},\"sum\":500,\"p50\":16,\"p95\":32,\"p99\":64}}}},\"read_stages_us\":{{\"execute\":{{\"count\":{commits},\"sum\":200,\"p50\":8,\"p95\":16,\"p99\":32}}}}}}\n"
        )
    }

    #[test]
    // req: STO-030, STO-031, ID-005, PERF-001, END-007, END-008, DX-014
    fn telemetry_summarize_aggregates_window_deltas_high_water_and_renders_human_and_json() {
        let mut ndjson = String::new();
        ndjson.push_str(&sample_line(
            1_700_000_000_000_000_000,
            "ready",
            4096,
            1024,
            10,
            1_000,
            3_000,
            256,
        ));
        ndjson.push_str(&sample_line(
            1_700_000_010_000_000_000,
            "degraded",
            8192,
            2048,
            60,
            6_000,
            8_000,
            1000,
        ));
        ndjson.push_str(&sample_line(
            1_700_000_020_000_000_000,
            "ready",
            12288,
            1536,
            110,
            11_000,
            13_000,
            512,
        ));

        let report = summarize_reader(ndjson.as_bytes()).expect("summarize valid ndjson");
        assert_eq!(report.samples, 3);
        assert_eq!(report.window_duration_ms, 20_000);
        assert_eq!(report.initial_health, "ready");
        assert_eq!(report.final_health, "ready");
        assert_eq!(report.health_transitions, 2);
        assert_eq!(report.degraded_samples, 1);
        assert_eq!(report.commits_delta, 100);
        assert_eq!(report.writer_busy_permille, 500);
        assert_eq!(
            report.final_online_retention_state.as_deref(),
            Some("fenced")
        );
        assert_eq!(
            report.final_online_retention_fence.as_deref(),
            Some("undelivered_outbox")
        );
        assert_eq!(report.online_retention_fenced_samples, 2);
        assert_eq!(report.online_retention_steps_delta, 10);
        assert_eq!(report.online_retention_sequences_delta, 50);
        assert_eq!(report.online_retention_watermark_sequence, 55);
        assert_eq!(
            report.storage_bytes,
            ByteGrowthSummary {
                initial_bytes: 4096,
                final_bytes: 12288,
                peak_bytes: 12288,
                growth_bytes: 8192,
            }
        );
        assert_eq!(
            report.journal_bytes,
            ByteGrowthSummary {
                initial_bytes: 1024,
                final_bytes: 1536,
                peak_bytes: 2048,
                growth_bytes: 512,
            }
        );
        assert_eq!(report.commit_duration_us_high_water.max_p99_us, 1000);
        assert_eq!(
            report.command_stages_us_high_water["evaluation"].max_p99_us,
            64
        );
        assert_eq!(report.read_stages_us_high_water["execute"].max_p99_us, 32);

        let dir = tempfile::TempDir::with_prefix("riffdb-cli-telemetry-").expect("tempdir");
        let path = dir.path().join("flight.ndjson");
        std::fs::write(&path, &ndjson).expect("write flight file");

        let terminal = summarize(&TelemetryCommand::Summarize {
            path: path.clone().into_os_string(),
        });
        let mut json_out = Vec::new();
        let mut json_err = Vec::new();
        let code = terminal.emit(OutputMode::Json, &mut json_out, &mut json_err);
        assert_eq!(code, std::process::ExitCode::from(0));
        assert!(json_err.is_empty());
        let parsed: serde_json::Value = serde_json::from_slice(&json_out).expect("cli json");
        assert_eq!(parsed["command"], "telemetry.summarize");
        assert_eq!(parsed["ok"], true);
        assert_eq!(parsed["result"]["samples"], 3);
        assert_eq!(parsed["result"]["commits_delta"], 100);
        assert_eq!(
            parsed["result"]["final_online_retention_fence"],
            "undelivered_outbox"
        );
        assert_eq!(parsed["result"]["storage_bytes"]["growth_bytes"], 8192);

        let terminal_human = summarize(&TelemetryCommand::Summarize {
            path: path.into_os_string(),
        });
        let mut human_out = Vec::new();
        let mut human_err = Vec::new();
        let code_human = terminal_human.emit(OutputMode::Human, &mut human_out, &mut human_err);
        assert_eq!(code_human, std::process::ExitCode::from(0));
        assert!(human_err.is_empty());
        let human_str = String::from_utf8(human_out).expect("utf8 human output");
        assert!(human_str.starts_with("telemetry.summarize: summarized\n"));
        assert!(human_str.contains("commits_delta: 100"));
    }

    #[test]
    // req: END-007, DX-014
    fn telemetry_summarize_rejects_empty_and_malformed_inputs_fail_closed() {
        let empty = summarize_reader(&b""[..]).expect_err("empty file must fail closed");
        assert_eq!(empty.0, "telemetry_flight_record_empty");

        let bad_json =
            summarize_reader(&b"{not-json}\n"[..]).expect_err("malformed json must fail closed");
        assert_eq!(bad_json.0, "telemetry_flight_record_invalid_json");
    }
}
