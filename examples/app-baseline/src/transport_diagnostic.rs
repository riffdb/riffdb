//! WP-632 diagnostic-only generated-client transport attribution.
//!
//! This binary deliberately has a distinct report schema and cannot produce a
//! PERF-018 qualification receipt. The ordinary `riffdb-app-baseline` binary
//! remains the sole frozen comparator.

#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use riffdb_app_baseline_core::{AppBackend, LatencyHistogram, Scale, SeedDataset, TicketRow};
use riffdb_app_baseline_postgres::{PostgresAppBackend, PostgresComparisonProfile};
use riffdb_app_baseline_riffdb::{
    DirectDiagnosticOpenTiming, DirectDiagnosticTiming, PairedClientTiming, RiffDbPublicBackend,
    RiffDbQueryExecuteEvidence, RiffDbReadStageEvidence, RiffDbServerSession,
    RiffDbShutdownEvidence, ServerStartOptions,
};
use serde_json::{Value, json};

const CLIENT_POINTS: &[usize] = &[1, 8, 32];
const DEFAULT_SAMPLES_PER_CLIENT: usize = 500;
const DEFAULT_WARMUP_PER_CLIENT: usize = 32;
const MAX_SAMPLES_PER_CLIENT: usize = 10_000;
const MAX_WARMUP_PER_CLIENT: usize = 1_000;

#[derive(Debug)]
struct Args {
    riffdbd_bin: PathBuf,
    output: Option<PathBuf>,
    scale: Scale,
    samples_per_client: usize,
    warmup_per_client: usize,
    clients: Vec<usize>,
    postgres_url: Option<String>,
    bounded_session_shadow: bool,
    bounded_session_first: bool,
    direct_exclusive_probe: bool,
    direct_exclusive_tls_probe: bool,
    direct_first: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SynchronousTransportShape {
    Unary,
    BoundedSession,
}

#[derive(Debug, Default)]
struct PairedHistograms {
    outer: LatencyHistogram,
    runtime_entry: LatencyHistogram,
    asynchronous_call: LatencyHistogram,
    runtime_exit: LatencyHistogram,
    bridge_only: LatencyHistogram,
}

impl PairedHistograms {
    fn record(&mut self, timing: PairedClientTiming) {
        self.outer.record(timing.outer);
        self.runtime_entry.record(timing.runtime_entry);
        self.asynchronous_call.record(timing.asynchronous_call);
        self.runtime_exit.record(timing.runtime_exit);
        self.bridge_only.record(timing.bridge_only());
    }

    fn merge(&mut self, other: &Self) {
        self.outer.merge(&other.outer);
        self.runtime_entry.merge(&other.runtime_entry);
        self.asynchronous_call.merge(&other.asynchronous_call);
        self.runtime_exit.merge(&other.runtime_exit);
        self.bridge_only.merge(&other.bridge_only);
    }

    fn json(&self) -> Value {
        json!({
            "outer": self.outer.summary_json(),
            "runtime_entry": self.runtime_entry.summary_json(),
            "asynchronous_call": self.asynchronous_call.summary_json(),
            "runtime_exit": self.runtime_exit.summary_json(),
            "bridge_only_measurement_artifact": self.bridge_only.summary_json(),
        })
    }
}

#[derive(Debug)]
struct ShapeResult {
    throughput_ops_s: u64,
    elapsed: Duration,
    latency: LatencyHistogram,
    paired: Option<PairedHistograms>,
}

#[derive(Debug, Default)]
struct DirectHistograms {
    outer: LatencyHistogram,
    runtime_entry: LatencyHistogram,
    client_encode: LatencyHistogram,
    stream_write: LatencyHistogram,
    server_decode_adapt: LatencyHistogram,
    application_service: LatencyHistogram,
    server_encode: LatencyHistogram,
    server_write: LatencyHistogram,
    stream_poll_read: LatencyHistogram,
    client_decode: LatencyHistogram,
    caller_wakeup: LatencyHistogram,
    ledger_error: LatencyHistogram,
}

impl DirectHistograms {
    fn record(&mut self, timing: DirectDiagnosticTiming) {
        self.outer.record(timing.outer);
        self.runtime_entry.record(timing.runtime_entry);
        self.client_encode.record(timing.client_encode);
        self.stream_write.record(timing.stream_write);
        self.server_decode_adapt.record(timing.server_decode_adapt);
        self.application_service.record(timing.application_service);
        self.server_encode.record(timing.server_encode);
        self.server_write.record(timing.server_write);
        self.stream_poll_read.record(timing.stream_poll_read);
        self.client_decode.record(timing.client_decode);
        self.caller_wakeup.record(timing.caller_wakeup);
        self.ledger_error
            .record(timing.outer.abs_diff(timing.ledger_total()));
    }

    fn json(&self) -> Value {
        json!({
            "outer": self.outer.summary_json(),
            "runtime_entry": self.runtime_entry.summary_json(),
            "client_encode": self.client_encode.summary_json(),
            "stream_write": self.stream_write.summary_json(),
            "server_decode_adapt": self.server_decode_adapt.summary_json(),
            "application_service": self.application_service.summary_json(),
            "server_encode": self.server_encode.summary_json(),
            "server_write": self.server_write.summary_json(),
            "stream_poll_read": self.stream_poll_read.summary_json(),
            "client_decode": self.client_decode.summary_json(),
            "caller_wakeup": self.caller_wakeup.summary_json(),
            "ledger_error": self.ledger_error.summary_json(),
        })
    }
}

#[derive(Debug)]
struct DirectShapeResult {
    throughput_ops_s: u64,
    elapsed: Duration,
    timings: DirectHistograms,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct LifecycleSample {
    trust_establishment: Duration,
    session_establishment: Duration,
    first_operation: Duration,
    total: Duration,
}

impl LifecycleSample {
    fn ledger_total(self) -> Duration {
        self.trust_establishment
            .saturating_add(self.session_establishment)
            .saturating_add(self.first_operation)
    }
}

#[derive(Debug, Default)]
struct LifecycleHistograms {
    trust_establishment: LatencyHistogram,
    session_establishment: LatencyHistogram,
    first_operation: LatencyHistogram,
    total: LatencyHistogram,
    ledger_error: LatencyHistogram,
}

impl LifecycleHistograms {
    fn record(&mut self, sample: LifecycleSample) {
        self.trust_establishment.record(sample.trust_establishment);
        self.session_establishment.record(sample.session_establishment);
        self.first_operation.record(sample.first_operation);
        self.total.record(sample.total);
        self.ledger_error
            .record(sample.total.abs_diff(sample.ledger_total()));
    }

    fn json(&self) -> Value {
        json!({
            "trust_establishment": self.trust_establishment.summary_json(),
            "session_establishment": self.session_establishment.summary_json(),
            "first_operation": self.first_operation.summary_json(),
            "total": self.total.summary_json(),
            "ledger_error": self.ledger_error.summary_json(),
        })
    }
}

#[derive(Debug, Default)]
struct OpenHistograms {
    trust_establishment: LatencyHistogram,
    session_establishment: LatencyHistogram,
    total: LatencyHistogram,
    ledger_error: LatencyHistogram,
}

impl OpenHistograms {
    fn record(&mut self, timing: DirectDiagnosticOpenTiming) {
        self.trust_establishment.record(timing.trust_establishment);
        self.session_establishment.record(timing.session_establishment);
        self.total.record(timing.total);
        self.ledger_error
            .record(timing.total.abs_diff(timing.ledger_total()));
    }

    fn json(&self) -> Value {
        json!({
            "trust_establishment": self.trust_establishment.summary_json(),
            "session_establishment": self.session_establishment.summary_json(),
            "total": self.total.summary_json(),
            "ledger_error": self.ledger_error.summary_json(),
        })
    }
}

fn main() -> Result<(), String> {
    let args = parse_args()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let dataset = SeedDataset::generate(args.scale);
    let expected = dataset
        .tickets
        .iter()
        .find(|ticket| ticket.ticket_id == dataset.probes().ticket_id)
        .cloned()
        .ok_or_else(|| "probe ticket absent from generated dataset".to_owned())?;

    let mut session = runtime
        .block_on(RiffDbServerSession::start_with_options(
            &args.riffdbd_bin,
            ServerStartOptions {
                query_execute_diagnostics: true,
                direct_stream_diagnostic: args.direct_exclusive_probe,
                direct_stream_diagnostic_tls: args.direct_exclusive_tls_probe,
                ..ServerStartOptions::default()
            },
        ))
        .map_err(|error| error.to_string())?;
    session.backend.reset().map_err(|error| error.to_string())?;
    session
        .backend
        .seed(&dataset)
        .map_err(|error| error.to_string())?;
    let (restarted, _) = runtime
        .block_on(session.restart_for_measurement())
        .map_err(|error| error.to_string())?;
    session = restarted;

    let mut cells = Vec::new();
    for &clients in &args.clients {
        let direct_setup = if args.direct_exclusive_probe && clients == 1 {
            let measured = runtime.block_on(measure_connection_lifecycle(
                &session,
                &expected,
                args.direct_first,
            ))?;
            let (restarted, setup_server) = runtime
                .block_on(session.restart_for_measurement())
                .map_err(|error| error.to_string())?;
            session = restarted;
            Some(json!({
                "timing": measured,
                "server_read_stages": read_stages_json(&setup_server),
            }))
        } else {
            None
        };
        let direct_exclusive_first = if args.direct_exclusive_probe
            && clients == 1
            && args.direct_first
        {
            let (restarted, measured, server) = measure_direct_shape(
                &runtime,
                session,
                &expected,
                args.samples_per_client,
                args.warmup_per_client,
            )?;
            session = restarted;
            Some((measured, server))
        } else {
            None
        };
        let bounded_session_first = if args.bounded_session_shadow && args.bounded_session_first {
            let (restarted, measured) = measure_bounded_session_shape(
                &runtime,
                session,
                &expected,
                clients,
                args.samples_per_client,
                args.warmup_per_client,
            )?;
            session = restarted;
            Some(measured)
        } else {
            None
        };
        let synchronous_memory_before = process_memory_json(session.child_pid())?;
        let synchronous = run_synchronous_shape(
            &session.backend,
            &expected,
            clients,
            args.samples_per_client,
            args.warmup_per_client,
        )?;
        let synchronous_memory_after = process_memory_json(session.child_pid())?;
        let (restarted, synchronous_server) = runtime
            .block_on(session.restart_for_measurement())
            .map_err(|error| error.to_string())?;
        session = restarted;

        let asynchronous_memory_before = process_memory_json(session.child_pid())?;
        let asynchronous = runtime.block_on(run_asynchronous_shape(
            &session.backend,
            &expected,
            clients,
            args.samples_per_client,
            args.warmup_per_client,
        ))?;
        let asynchronous_memory_after = process_memory_json(session.child_pid())?;
        let (restarted, asynchronous_server) = runtime
            .block_on(session.restart_for_measurement())
            .map_err(|error| error.to_string())?;
        session = restarted;

        let bounded_session = if let Some(measured) = bounded_session_first {
            Some(measured)
        } else if args.bounded_session_shadow {
            let (restarted, measured) = measure_bounded_session_shape(
                &runtime,
                session,
                &expected,
                clients,
                args.samples_per_client,
                args.warmup_per_client,
            )?;
            session = restarted;
            Some(measured)
        } else {
            None
        };

        let direct_exclusive = if args.direct_exclusive_probe && clients == 1 {
            let (measured, server) = if let Some(measured) = direct_exclusive_first {
                measured
            } else {
                let (restarted, measured, server) = measure_direct_shape(
                    &runtime,
                    session,
                    &expected,
                    args.samples_per_client,
                    args.warmup_per_client,
                )?;
                session = restarted;
                (measured, server)
            };
            let unary_complete_ns = synchronous
                .paired
                .as_ref()
                .map(|paired| histogram_mean_ns(&paired.asynchronous_call))
                .unwrap_or(0);
            let unary_server_ns = read_stage_mean_ns(&synchronous_server);
            let direct_complete_ns = histogram_mean_ns(&measured.timings.outer);
            let direct_server_ns = histogram_mean_ns(&measured.timings.application_service);
            let unary_outside_ns = unary_complete_ns.saturating_sub(unary_server_ns);
            let direct_outside_ns = direct_complete_ns.saturating_sub(direct_server_ns);
            Some(json!({
                "throughput_ops_s": measured.throughput_ops_s,
                "elapsed_ns": u64::try_from(measured.elapsed.as_nanos()).unwrap_or(u64::MAX),
                "paired_stage_ledger": measured.timings.json(),
                "server_read_stages": read_stages_json(&server),
                "mechanics_gate": {
                    "unary_complete_mean_ns": unary_complete_ns,
                    "direct_complete_mean_ns": direct_complete_ns,
                    "complete_reduction_percent": reduction_percent(unary_complete_ns, direct_complete_ns),
                    "complete_threshold_percent": 40,
                    "unary_outside_service_mean_ns": unary_outside_ns,
                    "direct_outside_service_mean_ns": direct_outside_ns,
                    "outside_service_reduction_percent": reduction_percent(unary_outside_ns, direct_outside_ns),
                    "outside_service_threshold_percent": 55,
                    "ledger_error_percent": ratio_percent(
                        histogram_mean_ns(&measured.timings.ledger_error),
                        direct_complete_ns,
                    ),
                    "ledger_error_threshold_percent": 5,
                },
            }))
        } else {
            None
        };

        cells.push(json!({
            "clients": clients,
            "operation": "generated.GetTicket",
            "samples_per_client": args.samples_per_client,
            "warmup_per_client": args.warmup_per_client,
            "frozen_synchronous_bridge": shape_json(&synchronous),
            "asynchronous_shadow": shape_json(&asynchronous),
            "synchronous_server_read_stages": read_stages_json(&synchronous_server),
            "synchronous_server_query_execute_windows": query_execute_json(
                synchronous_server.query_execute.as_ref(),
            ),
            "synchronous_server_memory_kib": {
                "before": synchronous_memory_before,
                "after": synchronous_memory_after,
            },
            "asynchronous_server_read_stages": read_stages_json(&asynchronous_server),
            "asynchronous_server_query_execute_windows": query_execute_json(
                asynchronous_server.query_execute.as_ref(),
            ),
            "asynchronous_server_memory_kib": {
                "before": asynchronous_memory_before,
                "after": asynchronous_memory_after,
            },
            "bounded_session_shadow": bounded_session,
                "direct_exclusive_probe": direct_exclusive,
                "connection_lifecycle": direct_setup,
                "bookkeeping": {
                "direct_exclusive_order": if args.direct_first { "before_unary" } else { "after_async" },
                "bounded_session_order": if args.bounded_session_first { "before_unary" } else { "after_async" },
                "bridge_only_classification": "measurement_artifact_not_product_gain",
                "asynchronous_call_classification": "customer_paid_product_path",
                "server_stage_note": "same operation/process generation; includes declared warmup samples",
            },
        }));
    }

    session
        .shutdown_with_evidence()
        .map_err(|error| error.to_string())?;
    let postgres_safe_app_twin = args
        .postgres_url
        .as_deref()
        .map(|url| {
            run_postgres_safe_app_twin(
                url,
                &dataset,
                &expected,
                args.samples_per_client,
                args.warmup_per_client,
            )
        })
        .transpose()?;

    let report = json!({
        "schema": "riffdb.client-transport-attribution/v2",
        "evidentiary": false,
        "perf_018_eligible": false,
        "release_comparator_changed": false,
        "frozen_release_binary": "riffdb-app-baseline",
        "diagnostic_binary": "riffdb-client-transport-diagnostic",
        "direct_exclusive_probe_enabled": args.direct_exclusive_probe,
        "direct_exclusive_transport": if args.direct_exclusive_tls_probe { "verified_direct_tls" } else { "loopback_cleartext" },
        "scale": args.scale.name(),
        "tcp_nodelay": {
            "client": true,
            "server": true,
            "proof": "tonic-0.14.6 defaults plus riffdb-client-rust executable assertion",
        },
        "predeclared_expected_gain": {
            "seed_percent": [0, 0],
            "c1_percent": [20, 35],
            "c8_percent": [10, 20],
            "c32_percent": [0, 8],
        },
        "activation_threshold": {
            "c1_minimum_percent": 15,
            "c8_minimum_percent": 10,
            "c32": "no_regression",
        },
        "cells": cells,
        "postgres_safe_app_get_ticket_twin": postgres_safe_app_twin,
        "notes": [
            "The synchronous shape is observed but not modified; ordinary PERF-018 evidence remains frozen.",
            "Every paired tuple is captured around one generated GetTicket call; no p50-minus-mean attribution is used.",
            "The asynchronous shadow uses the same exact generated operation, credential, persistent HTTP/2 channel, and server path.",
            "Removing bridge-only time is a measurement correction and cannot be claimed as a RiffDB product improvement.",
            "This report cannot encode a release pass.",
            "The direct-exclusive cell is feature-gated diagnostic carriage and is not a production listener or selector.",
            "Cold trust, exact application-session acceptance, and first generated operation are disjoint customer-paid intervals; resumed reconnect is reported separately.",
        ],
    });
    let encoded = serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?;
    if let Some(path) = args.output {
        fs::write(&path, format!("{encoded}\n"))
            .map_err(|error| format!("write {}: {error}", path.display()))?;
    } else {
        println!("{encoded}");
    }
    Ok(())
}

fn measure_direct_shape(
    runtime: &tokio::runtime::Runtime,
    session: RiffDbServerSession,
    expected: &TicketRow,
    samples_per_client: usize,
    warmup_per_client: usize,
) -> Result<
    (
        RiffDbServerSession,
        DirectShapeResult,
        RiffDbShutdownEvidence,
    ),
    String,
> {
    let address = session
        .direct_diagnostic_address()
        .ok_or_else(|| "direct diagnostic listener was not published".to_owned())?;
    let measured = run_direct_shape(
        runtime,
        &session.backend,
        address,
        expected,
        samples_per_client,
        warmup_per_client,
    )?;
    let (restarted, server) = runtime
        .block_on(session.restart_for_measurement())
        .map_err(|error| error.to_string())?;
    Ok((restarted, measured, server))
}

fn measure_bounded_session_shape(
    runtime: &tokio::runtime::Runtime,
    session: RiffDbServerSession,
    expected: &TicketRow,
    clients: usize,
    samples_per_client: usize,
    warmup_per_client: usize,
) -> Result<(RiffDbServerSession, Value), String> {
    let memory_before = process_memory_json(session.child_pid())?;
    let measured = run_synchronous_transport_shape(
        &session.backend,
        expected,
        clients,
        samples_per_client,
        warmup_per_client,
        SynchronousTransportShape::BoundedSession,
    )?;
    let memory_after = process_memory_json(session.child_pid())?;
    let (restarted, server) = runtime
        .block_on(session.restart_for_measurement())
        .map_err(|error| error.to_string())?;
    Ok((
        restarted,
        json!({
            "shape": shape_json(&measured),
            "server_read_stages": read_stages_json(&server),
            "server_query_execute_windows": query_execute_json(server.query_execute.as_ref()),
            "server_memory_kib": {
                "before": memory_before,
                "after": memory_after,
            },
            "bookkeeping": {
                "classification": "accepted_adr_0127_diagnostic_shape_not_perf_018_evidence",
                "session_open_note": "one catalog authentication precedes measured operations; no query-stage sample is attributed to session establishment",
            },
        }),
    ))
}

fn run_direct_shape(
    runtime: &tokio::runtime::Runtime,
    prototype: &RiffDbPublicBackend,
    address: std::net::SocketAddr,
    expected: &TicketRow,
    samples: usize,
    warmup: usize,
) -> Result<DirectShapeResult, String> {
    let mut client = runtime
        .block_on(async {
            let generation = prototype.direct_diagnostic_transport_generation(address)?;
            prototype
                .open_direct_diagnostic_on(&generation)
                .await
                .map(|(client, _)| client)
        })
        .map_err(|error| error.to_string())?;
    for _ in 0..warmup {
        let (observed, _) = client
            .get_ticket_paired(expected.organization_id, expected.ticket_id)
            .map_err(|error| error.to_string())?;
        require_expected(observed.as_ref(), expected)?;
    }
    let started = Instant::now();
    let mut timings = DirectHistograms::default();
    for _ in 0..samples {
        let (observed, timing) = client
            .get_ticket_paired(expected.organization_id, expected.ticket_id)
            .map_err(|error| error.to_string())?;
        require_expected(observed.as_ref(), expected)?;
        timings.record(timing);
    }
    let elapsed = started.elapsed();
    Ok(DirectShapeResult {
        throughput_ops_s: throughput(1, samples, elapsed),
        elapsed,
        timings,
    })
}

async fn measure_connection_lifecycle(
    session: &RiffDbServerSession,
    expected: &TicketRow,
    direct_first: bool,
) -> Result<Value, String> {
    async fn unary(
        backend: &RiffDbPublicBackend,
        expected: &TicketRow,
    ) -> Result<LifecycleSample, String> {
        let total_started = Instant::now();
        let trust_started = Instant::now();
        let mut fresh = backend
            .fresh_session_async()
            .await
            .map_err(|error| error.to_string())?;
        let trust_establishment = trust_started.elapsed();
        let session_started = Instant::now();
        fresh
            .open_bounded_session_async()
            .await
            .map_err(|error| error.to_string())?;
        let session_establishment = session_started.elapsed();
        let operation_started = Instant::now();
        let observed = fresh
            .get_ticket_generated_async(expected.organization_id, expected.ticket_id)
            .await
            .map_err(|error| error.to_string())?;
        let first_operation = operation_started.elapsed();
        require_expected(observed.as_ref(), expected)?;
        Ok(LifecycleSample {
            trust_establishment,
            session_establishment,
            first_operation,
            total: total_started.elapsed(),
        })
    }

    async fn direct(
        session: &RiffDbServerSession,
        expected: &TicketRow,
    ) -> Result<LifecycleSample, String> {
        let address = session
            .direct_diagnostic_address()
            .ok_or_else(|| "direct diagnostic listener was not published".to_owned())?;
        // A new generation for every cold sample structurally prevents TLS
        // resumption from being mislabeled as a full handshake.
        let generation = session
            .backend
            .direct_diagnostic_transport_generation(address)
            .map_err(|error| error.to_string())?;
        let total_started = Instant::now();
        let (mut fresh, open) = session
            .backend
            .open_direct_diagnostic_on(&generation)
            .await
            .map_err(|error| error.to_string())?;
        let operation_started = Instant::now();
        let (observed, _) = fresh
            .get_ticket(expected.organization_id, expected.ticket_id)
            .await
            .map_err(|error| error.to_string())?;
        let first_operation = operation_started.elapsed();
        require_expected(observed.as_ref(), expected)?;
        Ok(LifecycleSample {
            trust_establishment: open.trust_establishment,
            session_establishment: open.session_establishment,
            first_operation,
            total: total_started.elapsed(),
        })
    }

    const SETUP_SAMPLES: usize = 32;
    // Warm code and plans on discarded connections. Every measured direct
    // connection still uses a fresh TLS generation and therefore cannot
    // resume a prior diagnostic TLS session.
    if direct_first {
        unary(&session.backend, expected).await?;
        direct(session, expected).await?;
    } else {
        direct(session, expected).await?;
        unary(&session.backend, expected).await?;
    }
    let mut unary_latency = LifecycleHistograms::default();
    let mut direct_latency = LifecycleHistograms::default();
    for sample in 0..SETUP_SAMPLES {
        let candidate_first = (sample % 2 == 0) == direct_first;
        let (unary_sample, direct_sample) = if candidate_first {
            let direct_sample = direct(session, expected).await?;
            let unary_sample = unary(&session.backend, expected).await?;
            (unary_sample, direct_sample)
        } else {
            let unary_sample = unary(&session.backend, expected).await?;
            let direct_sample = direct(session, expected).await?;
            (unary_sample, direct_sample)
        };
        unary_latency.record(unary_sample);
        direct_latency.record(direct_sample);
    }

    // Resumed reconnect is reported separately and never substitutes for the
    // cold full-handshake gate above.
    let address = session
        .direct_diagnostic_address()
        .ok_or_else(|| "direct diagnostic listener was not published".to_owned())?;
    let generation = session
        .backend
        .direct_diagnostic_transport_generation(address)
        .map_err(|error| error.to_string())?;
    let (priming, _) = session
        .backend
        .open_direct_diagnostic_on(&generation)
        .await
        .map_err(|error| error.to_string())?;
    drop(priming);
    let mut resumed = OpenHistograms::default();
    for _ in 0..SETUP_SAMPLES {
        let (client, timing) = session
            .backend
            .open_direct_diagnostic_on(&generation)
            .await
            .map_err(|error| error.to_string())?;
        resumed.record(timing);
        drop(client);
    }

    let unary_setup_ns = histogram_mean_ns(&unary_latency.trust_establishment)
        .saturating_add(histogram_mean_ns(&unary_latency.session_establishment));
    let direct_setup_ns = histogram_mean_ns(&direct_latency.trust_establishment)
        .saturating_add(histogram_mean_ns(&direct_latency.session_establishment));
    let unary_first_ns = histogram_mean_ns(&unary_latency.first_operation);
    let direct_first_ns = histogram_mean_ns(&direct_latency.first_operation);
    let unary_total_ns = histogram_mean_ns(&unary_latency.total);
    let direct_total_ns = histogram_mean_ns(&direct_latency.total);
    Ok(json!({
        "samples": SETUP_SAMPLES,
        "counterbalanced_start": if direct_first { "direct" } else { "unary" },
        "cold_full_handshake": {
            "unary_authenticated_session": unary_latency.json(),
            "direct_authenticated_session": direct_latency.json(),
            "setup_ratio_percent": ratio_percent(direct_setup_ns, unary_setup_ns),
            "setup_maximum_percent": 105,
            "first_operation_reduction_percent": reduction_percent(unary_first_ns, direct_first_ns),
            "first_operation_threshold_percent": 25,
            "unary_ledger_error_percent": ratio_percent(
                histogram_mean_ns(&unary_latency.ledger_error),
                unary_total_ns,
            ),
            "direct_ledger_error_percent": ratio_percent(
                histogram_mean_ns(&direct_latency.ledger_error),
                direct_total_ns,
            ),
            "ledger_error_threshold_percent": 5,
        },
        "resumed_reconnect": {
            "direct_authenticated_session": resumed.json(),
            "classification": "reported_separately_not_a_cold_setup_substitute",
        },
    }))
}

fn reduction_percent(control: u64, candidate: u64) -> f64 {
    if control == 0 {
        return 0.0;
    }
    100.0 * (control.saturating_sub(candidate) as f64) / control as f64
}

fn ratio_percent(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    100.0 * numerator as f64 / denominator as f64
}

fn histogram_mean_ns(histogram: &LatencyHistogram) -> u64 {
    histogram
        .summary_json()
        .get("mean_ns")
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn read_stage_mean_ns(evidence: &RiffDbShutdownEvidence) -> u64 {
    evidence.read_stages.iter().fold(0_u64, |total, stage| {
        total.saturating_add(
            stage
                .sum_us
                .saturating_mul(1_000)
                .checked_div(stage.count)
                .unwrap_or(0),
        )
    })
}

fn run_postgres_safe_app_twin(
    url: &str,
    dataset: &SeedDataset,
    expected: &TicketRow,
    samples: usize,
    warmup: usize,
) -> Result<Value, String> {
    let mut backend = PostgresAppBackend::new_with_profile(url, PostgresComparisonProfile::SafeApp)
        .map_err(|error| error.to_string())?;
    backend.reset().map_err(|error| error.to_string())?;
    backend.seed(dataset).map_err(|error| error.to_string())?;
    for _ in 0..warmup {
        let observed = backend
            .point_get_ticket(expected.organization_id, expected.ticket_id)
            .map_err(|error| error.to_string())?;
        require_expected(observed.as_ref(), expected)?;
    }
    let mut latency = LatencyHistogram::default();
    let started = Instant::now();
    for _ in 0..samples {
        let call_started = Instant::now();
        let observed = backend
            .point_get_ticket(expected.organization_id, expected.ticket_id)
            .map_err(|error| error.to_string())?;
        require_expected(observed.as_ref(), expected)?;
        latency.record(call_started.elapsed());
    }
    let elapsed = started.elapsed();
    Ok(json!({
        "profile": PostgresComparisonProfile::SafeApp.backend_id(),
        "operation": "get_ticket",
        "clients": 1,
        "samples": samples,
        "warmup": warmup,
        "throughput_ops_s": throughput(1, samples, elapsed),
        "elapsed_ns": u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
        "latency": latency.summary_json(),
    }))
}

fn run_synchronous_shape(
    prototype: &RiffDbPublicBackend,
    expected: &TicketRow,
    clients: usize,
    samples_per_client: usize,
    warmup_per_client: usize,
) -> Result<ShapeResult, String> {
    run_synchronous_transport_shape(
        prototype,
        expected,
        clients,
        samples_per_client,
        warmup_per_client,
        SynchronousTransportShape::Unary,
    )
}

fn run_synchronous_transport_shape(
    prototype: &RiffDbPublicBackend,
    expected: &TicketRow,
    clients: usize,
    samples_per_client: usize,
    warmup_per_client: usize,
    transport_shape: SynchronousTransportShape,
) -> Result<ShapeResult, String> {
    let sessions = (0..clients)
        .map(|_| {
            let result = match transport_shape {
                SynchronousTransportShape::Unary => prototype.fresh_session(),
                SynchronousTransportShape::BoundedSession => prototype.fresh_bounded_session(),
            };
            result.map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ready = Arc::new(Barrier::new(clients + 1));
    let expected = Arc::new(expected.clone());
    let mut workers = Vec::with_capacity(clients);
    for mut backend in sessions {
        let ready = Arc::clone(&ready);
        let expected = Arc::clone(&expected);
        workers.push(thread::spawn(
            move || -> Result<PairedHistograms, String> {
                for _ in 0..warmup_per_client {
                    let result = backend
                        .get_ticket_generated_paired(expected.organization_id, expected.ticket_id)
                        .map_err(|error| error.to_string())?;
                    require_expected(result.value.as_ref(), &expected)?;
                }
                ready.wait();
                let mut histograms = PairedHistograms::default();
                for _ in 0..samples_per_client {
                    let result = backend
                        .get_ticket_generated_paired(expected.organization_id, expected.ticket_id)
                        .map_err(|error| error.to_string())?;
                    require_expected(result.value.as_ref(), &expected)?;
                    histograms.record(result.timing);
                }
                match transport_shape {
                    SynchronousTransportShape::Unary => {}
                    SynchronousTransportShape::BoundedSession => {
                        backend.close_bounded_session();
                    }
                }
                Ok(histograms)
            },
        ));
    }
    ready.wait();
    let started = Instant::now();
    let mut paired = PairedHistograms::default();
    for worker in workers {
        paired.merge(
            &worker
                .join()
                .map_err(|_| "synchronous diagnostic worker panicked".to_owned())??,
        );
    }
    let elapsed = started.elapsed();
    let throughput_ops_s = throughput(clients, samples_per_client, elapsed);
    let latency = paired.outer.clone();
    Ok(ShapeResult {
        throughput_ops_s,
        elapsed,
        latency,
        paired: Some(paired),
    })
}

async fn run_asynchronous_shape(
    prototype: &RiffDbPublicBackend,
    expected: &TicketRow,
    clients: usize,
    samples_per_client: usize,
    warmup_per_client: usize,
) -> Result<ShapeResult, String> {
    let mut sessions = Vec::with_capacity(clients);
    for _ in 0..clients {
        sessions.push(
            prototype
                .fresh_session_async()
                .await
                .map_err(|error| error.to_string())?,
        );
    }
    let ready = Arc::new(tokio::sync::Barrier::new(clients + 1));
    let expected = Arc::new(expected.clone());
    let mut workers = Vec::with_capacity(clients);
    for backend in sessions {
        let ready = Arc::clone(&ready);
        let expected = Arc::clone(&expected);
        workers.push(tokio::spawn(async move {
            for _ in 0..warmup_per_client {
                let result = backend
                    .get_ticket_generated_async(expected.organization_id, expected.ticket_id)
                    .await
                    .map_err(|error| error.to_string())?;
                require_expected(result.as_ref(), &expected)?;
            }
            ready.wait().await;
            let mut latency = LatencyHistogram::default();
            for _ in 0..samples_per_client {
                let started = Instant::now();
                let result = backend
                    .get_ticket_generated_async(expected.organization_id, expected.ticket_id)
                    .await
                    .map_err(|error| error.to_string())?;
                latency.record(started.elapsed());
                require_expected(result.as_ref(), &expected)?;
            }
            Ok::<LatencyHistogram, String>(latency)
        }));
    }
    ready.wait().await;
    let started = Instant::now();
    let mut latency = LatencyHistogram::default();
    for worker in workers {
        latency.merge(
            &worker
                .await
                .map_err(|_| "asynchronous diagnostic worker panicked".to_owned())??,
        );
    }
    let elapsed = started.elapsed();
    Ok(ShapeResult {
        throughput_ops_s: throughput(clients, samples_per_client, elapsed),
        elapsed,
        latency,
        paired: None,
    })
}

fn require_expected(observed: Option<&TicketRow>, expected: &TicketRow) -> Result<(), String> {
    match observed {
        Some(observed) if observed == expected => Ok(()),
        Some(_) => Err("generated GetTicket returned unequal row".to_owned()),
        None => Err("generated GetTicket returned NotFound".to_owned()),
    }
}

fn throughput(clients: usize, samples_per_client: usize, elapsed: Duration) -> u64 {
    let operations = u64::try_from(clients.saturating_mul(samples_per_client)).unwrap_or(u64::MAX);
    let elapsed_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX).max(1);
    operations
        .saturating_mul(1_000_000_000)
        .checked_div(elapsed_ns)
        .unwrap_or(0)
}

fn shape_json(result: &ShapeResult) -> Value {
    json!({
        "throughput_ops_s": result.throughput_ops_s,
        "elapsed_ns": u64::try_from(result.elapsed.as_nanos()).unwrap_or(u64::MAX),
        "latency": result.latency.summary_json(),
        "paired_timing": result.paired.as_ref().map(PairedHistograms::json),
    })
}

fn read_stages_json(evidence: &RiffDbShutdownEvidence) -> Value {
    Value::Array(evidence.read_stages.iter().map(read_stage_json).collect())
}

fn read_stage_json(stage: &RiffDbReadStageEvidence) -> Value {
    json!({
        "name": stage.name,
        "count": stage.count,
        "sum_us": stage.sum_us,
        "mean_us": stage.sum_us.checked_div(stage.count).unwrap_or(0),
        "cumulative_buckets": stage.buckets,
    })
}

fn query_execute_json(evidence: Option<&RiffDbQueryExecuteEvidence>) -> Value {
    let Some(evidence) = evidence else {
        return Value::Null;
    };
    json!({
        "window_width": evidence.window_width,
        "stage_names": evidence.stage_names,
        "total_count": evidence.total_count,
        "windows": evidence.windows.iter().enumerate().map(|(index, window)| json!({
            "index": index,
            "sample_start": u64::try_from(index).unwrap_or(u64::MAX)
                .saturating_mul(evidence.window_width),
            "count": window.count,
            "stage_mean_ns": window.stage_ns.iter().map(|sum| {
                sum.checked_div(window.count).unwrap_or(0)
            }).collect::<Vec<_>>(),
            "stage_sum_ns": window.stage_ns,
            "overlay_transitions_mean": window.overlay_transitions_sum
                .checked_div(window.count).unwrap_or(0),
            "overlay_transitions_max": window.overlay_transitions_max,
            "overlay_bytes_mean": window.overlay_bytes_sum
                .checked_div(window.count).unwrap_or(0),
            "overlay_bytes_max": window.overlay_bytes_max,
            "authority_tail_bytes_mean": window.authority_tail_bytes_sum
                .checked_div(window.count).unwrap_or(0),
            "authority_tail_bytes_max": window.authority_tail_bytes_max,
            "authority_tail_commands_mean": window.authority_tail_commands_sum
                .checked_div(window.count).unwrap_or(0),
            "authority_tail_commands_max": window.authority_tail_commands_max,
        })).collect::<Vec<_>>(),
    })
}

fn process_memory_json(pid: u32) -> Result<Value, String> {
    let path = PathBuf::from(format!("/proc/{pid}/smaps_rollup"));
    let encoded = fs::read_to_string(&path)
        .map_err(|error| format!("read process memory snapshot: {error}"))?;
    let mut values = serde_json::Map::new();
    for line in encoded.lines() {
        let Some((name, tail)) = line.split_once(':') else {
            continue;
        };
        if !matches!(name, "Rss" | "Pss" | "Private_Dirty" | "Anonymous" | "Swap") {
            continue;
        }
        let value = tail
            .split_ascii_whitespace()
            .next()
            .ok_or_else(|| format!("missing process memory value for {name}"))?
            .parse::<u64>()
            .map_err(|_| format!("invalid process memory value for {name}"))?;
        values.insert(name.to_owned(), Value::from(value));
    }
    if values.len() != 5 {
        return Err("process memory snapshot omitted a required field".to_owned());
    }
    Ok(Value::Object(values))
}

fn parse_args() -> Result<Args, String> {
    let mut riffdbd_bin = None;
    let mut output = None;
    let mut scale = Scale::smoke();
    let mut samples_per_client = DEFAULT_SAMPLES_PER_CLIENT;
    let mut warmup_per_client = DEFAULT_WARMUP_PER_CLIENT;
    let mut clients = CLIENT_POINTS.to_vec();
    let mut postgres_url = None;
    let mut bounded_session_shadow = false;
    let mut bounded_session_first = false;
    let mut direct_exclusive_probe = false;
    let mut direct_exclusive_tls_probe = false;
    let mut direct_first = false;
    let mut arguments = env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        let argument = argument
            .into_string()
            .map_err(|_| "arguments must be UTF-8".to_owned())?;
        match argument.as_str() {
            "--riffdbd-bin" => {
                riffdbd_bin = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| "--riffdbd-bin requires a path".to_owned())?,
                ));
            }
            "--output" => {
                output = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| "--output requires a path".to_owned())?,
                ));
            }
            "--scale" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "--scale requires smoke or full".to_owned())?
                    .into_string()
                    .map_err(|_| "--scale must be UTF-8".to_owned())?;
                scale = match value.as_str() {
                    "smoke" => Scale::smoke(),
                    "full" => Scale::full(),
                    _ => return Err("--scale must be smoke or full".to_owned()),
                };
            }
            "--samples-per-client" => {
                samples_per_client = parse_bounded_usize(
                    arguments.next(),
                    "--samples-per-client",
                    1,
                    MAX_SAMPLES_PER_CLIENT,
                )?;
            }
            "--warmup-per-client" => {
                warmup_per_client = parse_bounded_usize(
                    arguments.next(),
                    "--warmup-per-client",
                    0,
                    MAX_WARMUP_PER_CLIENT,
                )?;
            }
            "--clients" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "--clients requires comma-separated values".to_owned())?
                    .into_string()
                    .map_err(|_| "--clients must be UTF-8".to_owned())?;
                clients = value
                    .split(',')
                    .map(|part| {
                        part.parse::<usize>()
                            .ok()
                            .filter(|value| matches!(value, 1 | 8 | 32))
                            .ok_or_else(|| "--clients values must be 1, 8, or 32".to_owned())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                clients.sort_unstable();
                clients.dedup();
                if clients.is_empty() {
                    return Err("--clients must select at least one cell".to_owned());
                }
            }
            "--postgres-url" => {
                postgres_url = Some(
                    arguments
                        .next()
                        .ok_or_else(|| "--postgres-url requires a URL".to_owned())?
                        .into_string()
                        .map_err(|_| "--postgres-url must be UTF-8".to_owned())?,
                );
            }
            "--bounded-session-shadow" => bounded_session_shadow = true,
            "--bounded-session-first" => bounded_session_first = true,
            "--direct-exclusive-probe" => direct_exclusive_probe = true,
            "--direct-exclusive-tls-probe" => {
                direct_exclusive_probe = true;
                direct_exclusive_tls_probe = true;
            }
            "--direct-first" => direct_first = true,
            "--help" | "-h" => {
                return Err(
                    "usage: riffdb-client-transport-diagnostic --riffdbd-bin PATH \
                     [--output PATH] [--scale smoke|full] \
                     [--clients 1,8,32] \
                     [--bounded-session-shadow] \
                     [--bounded-session-first] \
                     [--direct-exclusive-probe|--direct-exclusive-tls-probe] [--direct-first] \
                     [--postgres-url URL] \
                     [--samples-per-client 1..10000] [--warmup-per-client 0..1000]"
                        .to_owned(),
                );
            }
            _ => return Err(format!("unknown argument: {argument}")),
        }
    }
    let riffdbd_bin = riffdbd_bin.ok_or_else(|| "--riffdbd-bin is required".to_owned())?;
    if bounded_session_first && !bounded_session_shadow {
        return Err("--bounded-session-first requires --bounded-session-shadow".to_owned());
    }
    if direct_first && !direct_exclusive_probe {
        return Err("--direct-first requires --direct-exclusive-probe".to_owned());
    }
    if !riffdbd_bin.is_file() {
        return Err(format!(
            "riffdbd binary not found: {}",
            riffdbd_bin.display()
        ));
    }
    Ok(Args {
        riffdbd_bin,
        output,
        scale,
        samples_per_client,
        warmup_per_client,
        clients,
        postgres_url,
        bounded_session_shadow,
        bounded_session_first,
        direct_exclusive_probe,
        direct_exclusive_tls_probe,
        direct_first,
    })
}

fn parse_bounded_usize(
    value: Option<std::ffi::OsString>,
    flag: &str,
    minimum: usize,
    maximum: usize,
) -> Result<usize, String> {
    let value = value
        .ok_or_else(|| format!("{flag} requires a value"))?
        .into_string()
        .map_err(|_| format!("{flag} must be UTF-8"))?;
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be an integer"))?;
    if !(minimum..=maximum).contains(&parsed) {
        return Err(format!("{flag} must be {minimum}..={maximum}"));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::{LifecycleHistograms, LifecycleSample, histogram_mean_ns};
    use std::time::Duration;

    #[test]
    fn lifecycle_ledger_closes_only_over_three_customer_paid_intervals() {
        let sample = LifecycleSample {
            trust_establishment: Duration::from_micros(300),
            session_establishment: Duration::from_micros(200),
            first_operation: Duration::from_micros(100),
            total: Duration::from_micros(607),
        };
        let mut histograms = LifecycleHistograms::default();
        histograms.record(sample);
        assert_eq!(sample.ledger_total(), Duration::from_micros(600));
        assert_eq!(histogram_mean_ns(&histograms.ledger_error), 7_000);
        let encoded = histograms.json();
        assert!(encoded.get("trust_establishment").is_some());
        assert!(encoded.get("session_establishment").is_some());
        assert!(encoded.get("first_operation").is_some());
        assert!(encoded.get("total").is_some());
    }
}
