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
    PairedClientTiming, RiffDbPublicBackend, RiffDbQueryExecuteEvidence, RiffDbReadStageEvidence,
    RiffDbServerSession, RiffDbShutdownEvidence, ServerStartOptions,
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
                ..ServerStartOptions::default()
            },
        ))
        .map_err(|error| error.to_string())?;
    session.backend.reset().map_err(|error| error.to_string())?;
    session
        .backend
        .seed(&dataset)
        .map_err(|error| error.to_string())?;
    let (restarted, initial_server) = runtime
        .block_on(session.restart_for_measurement())
        .map_err(|error| error.to_string())?;
    session = restarted;

    let mut cells = Vec::new();
    for &clients in &args.clients {
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

        cells.push(json!({
            "clients": clients,
            "operation": "generated.GetTicket",
            "samples_per_client": args.samples_per_client,
            "warmup_per_client": args.warmup_per_client,
            "frozen_synchronous_bridge": shape_json(&synchronous),
            "asynchronous_shadow": shape_json(&asynchronous),
            "synchronous_server_read_stages": read_stages_json(&synchronous_server),
            "synchronous_server_shutdown": shutdown_stages_json(&synchronous_server),
            "synchronous_server_query_execute_windows": query_execute_json(
                synchronous_server.query_execute.as_ref(),
            ),
            "synchronous_server_memory_kib": {
                "before": synchronous_memory_before,
                "after": synchronous_memory_after,
            },
            "asynchronous_server_read_stages": read_stages_json(&asynchronous_server),
            "asynchronous_server_shutdown": shutdown_stages_json(&asynchronous_server),
            "asynchronous_server_query_execute_windows": query_execute_json(
                asynchronous_server.query_execute.as_ref(),
            ),
            "asynchronous_server_memory_kib": {
                "before": asynchronous_memory_before,
                "after": asynchronous_memory_after,
            },
            "bounded_session_shadow": bounded_session,
            "bookkeeping": {
                "bounded_session_order": if args.bounded_session_first { "before_unary" } else { "after_async" },
                "bridge_only_classification": "measurement_artifact_not_product_gain",
                "asynchronous_call_classification": "customer_paid_product_path",
                "server_stage_note": "same operation/process generation; includes declared warmup samples",
            },
        }));
    }

    let final_server = session
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
        "schema": "riffdb.client-transport-attribution/v1",
        "evidentiary": false,
        "perf_018_eligible": false,
        "release_comparator_changed": false,
        "frozen_release_binary": "riffdb-app-baseline",
        "diagnostic_binary": "riffdb-client-transport-diagnostic",
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
        "initial_seed_server_shutdown": shutdown_stages_json(&initial_server),
        "final_server_shutdown": shutdown_stages_json(&final_server),
        "postgres_safe_app_get_ticket_twin": postgres_safe_app_twin,
        "notes": [
            "The synchronous shape is observed but not modified; ordinary PERF-018 evidence remains frozen.",
            "Every paired tuple is captured around one generated GetTicket call; no p50-minus-mean attribution is used.",
            "The asynchronous shadow uses the same exact generated operation, credential, persistent HTTP/2 channel, and server path.",
            "Removing bridge-only time is a measurement correction and cannot be claimed as a RiffDB product improvement.",
            "This report cannot encode a release pass.",
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
            "server_shutdown": shutdown_stages_json(&server),
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

fn shutdown_stages_json(evidence: &RiffDbShutdownEvidence) -> Value {
    const NAMES: [&str; 8] = [
        "service_jobs_idle",
        "exact_text_worker",
        "columnar_worker",
        "projection_worker",
        "notifications",
        "coordinator",
        "blocking_ports",
        "validated_prefix_checkpoint",
    ];
    let stages = evidence.shutdown_stages_us.map(|values| {
        NAMES
            .iter()
            .zip(values)
            .map(|(name, elapsed_us)| (name.to_string(), json!(elapsed_us)))
            .collect::<serde_json::Map<String, Value>>()
    });
    let stage_sum_us = evidence
        .shutdown_stages_us
        .map(|values| values.into_iter().sum::<u64>());
    json!({
        "stages_us": stages,
        "stage_sum_us": stage_sum_us,
        "graph_shutdown_elapsed_us": evidence.graph_shutdown_elapsed_us,
        "harness_shutdown_elapsed_us": evidence.harness_shutdown_elapsed_us,
        "graph_closure_ratio": stage_sum_us.zip(evidence.graph_shutdown_elapsed_us).map(
            |(sum, graph)| sum as f64 / graph.max(1) as f64,
        ),
        "external_process_residual_us": evidence.graph_shutdown_elapsed_us.map(|graph| {
            evidence.harness_shutdown_elapsed_us.saturating_sub(graph)
        }),
    })
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
            "--help" | "-h" => {
                return Err(
                    "usage: riffdb-client-transport-diagnostic --riffdbd-bin PATH \
                     [--output PATH] [--scale smoke|full] \
                     [--clients 1,8,32] \
                     [--bounded-session-shadow] \
                     [--bounded-session-first] \
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
