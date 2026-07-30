//! TicketDesk app-baseline runner: live Postgres vs live `riffdbd` over public gRPC.

#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use riffdb_app_baseline_core::{
    AppBackend, BackendReport, Scale, SeedDataset, build_report, run_scenarios,
};
use riffdb_app_baseline_postgres::PostgresAppBackend;
use riffdb_app_baseline_riffdb::RiffDbServerSession;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("app-baseline failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args = Args::parse(env::args().skip(1))?;
    let dataset = SeedDataset::generate(args.scale);
    let mut backends = Vec::new();
    let mut concurrent_reads = Vec::new();

    if !args.skip_postgres {
        let url = args
            .postgres_url
            .clone()
            .or_else(|| env::var("RIFFDB_APP_BASELINE_POSTGRES_URL").ok())
            .ok_or_else(|| {
                "PostgreSQL required: pass --postgres-url or set RIFFDB_APP_BASELINE_POSTGRES_URL"
                    .to_owned()
            })?;
        let mut postgres = PostgresAppBackend::new(&url).map_err(|error| error.to_string())?;
        postgres.reset().map_err(|error| error.to_string())?;
        let seed_started = Instant::now();
        postgres.seed(&dataset).map_err(|error| error.to_string())?;
        let seed_ns = u64::try_from(seed_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let scenarios = run_scenarios(&mut postgres, &dataset, args.warmups, args.samples)
            .map_err(|error| error.to_string())?;
        if let Some(clients) = args.concurrent_clients {
            let url = Arc::new(url);
            concurrent_reads.push(run_concurrent_point_reads(
                "postgres_sql",
                clients,
                args.concurrent_operations,
                &dataset,
                move || {
                    PostgresAppBackend::new(url.as_str()).map_err(|error| error.to_string())
                },
            )?);
        }
        backends.push(BackendReport {
            backend_id: "postgres_sql",
            description: "Live PostgreSQL 18 via SQL (joins, filters, indexes)".to_owned(),
            guarantee_notes: vec![
                "READ COMMITTED SQL transactions".to_owned(),
                "Relational joins for ticket_detail_page".to_owned(),
                "Not RiffDB command/idempotency semantics".to_owned(),
            ],
            seed_ns,
            seed_rows: args.scale.approximate_row_count(),
            scenarios,
            write_completion_groups: None,
        });
    }

    if !args.skip_riffdb {
        let riffdbd = args
            .riffdbd_bin
            .clone()
            .or_else(|| env::var_os("RIFFDB_APP_BASELINE_RIFFDBD_BIN").map(PathBuf::from))
            .ok_or_else(|| {
                "riffdbd required: pass --riffdbd-bin or set RIFFDB_APP_BASELINE_RIFFDBD_BIN"
                    .to_owned()
            })?;
        if !riffdbd.is_file() {
            return Err(format!("riffdbd binary not found: {}", riffdbd.display()));
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;

        let mut session = runtime
            .block_on(RiffDbServerSession::start(&riffdbd))
            .map_err(|error| error.to_string())?;

        // reset is a no-op for fresh process
        session.backend.reset().map_err(|error| error.to_string())?;
        let seed_started = Instant::now();
        if let Err(error) = session.backend.seed(&dataset) {
            if let Ok(groups) = session.shutdown() {
                eprintln!("riffdb write completion groups 1..64: {groups:?}");
            }
            return Err(error.to_string());
        }
        let seed_ns = u64::try_from(seed_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let scenarios = run_scenarios(&mut session.backend, &dataset, args.warmups, args.samples);
        let scenarios = match scenarios {
            Ok(scenarios) => scenarios,
            Err(error) => {
                if let Ok(groups) = session.shutdown() {
                    eprintln!("riffdb write completion groups 1..64: {groups:?}");
                }
                return Err(error.to_string());
            }
        };
        if let Some(clients) = args.concurrent_clients {
            let prototype = session.backend.clone();
            concurrent_reads.push(run_concurrent_point_reads(
                "riffdb_public_grpc",
                clients,
                args.concurrent_operations,
                &dataset,
                move || Ok(prototype.clone()),
            )?);
        }
        let write_completion_groups = session.shutdown().map_err(|error| error.to_string())?;
        backends.push(BackendReport {
            backend_id: "riffdb_public_grpc",
            description: "Live riffdbd over public gRPC (symbolic commands + named RiffQL)"
                .to_owned(),
            guarantee_notes: vec![
                "Symbolic TicketDesk commands for seed/writes".to_owned(),
                "Reads via named RiffQL queries (one public RPC per page)".to_owned(),
                "ticket_detail_page is one TicketPage query with dependent key batches".to_owned(),
                "Synchronous durable command commits".to_owned(),
            ],
            seed_ns,
            seed_rows: args.scale.approximate_row_count(),
            scenarios,
            write_completion_groups: Some(write_completion_groups.to_vec()),
        });
    }

    if backends.is_empty() {
        return Err("no backends selected".to_owned());
    }

    let mut report = build_report(args.scale, args.warmups, args.samples, &backends);
    if !concurrent_reads.is_empty() {
        report["concurrent_point_reads"] = serde_json::Value::Array(concurrent_reads);
    }
    let encoded = serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?;
    if let Some(path) = &args.output {
        fs::write(path, format!("{encoded}\n")).map_err(|error| error.to_string())?;
        println!("wrote {}", path.display());
    } else {
        println!("{encoded}");
    }
    print_summary(&report);
    if args.assert_write_parity {
        assert_write_parity(&report)?;
    }
    if args.assert_all_parity {
        assert_all_parity(&report)?;
    }
    Ok(())
}

fn run_concurrent_point_reads<B, Factory>(
    backend_id: &'static str,
    clients: usize,
    operations_per_client: usize,
    dataset: &SeedDataset,
    factory: Factory,
) -> Result<serde_json::Value, String>
where
    B: AppBackend + Send + 'static,
    B::Error: Send + 'static,
    Factory: Fn() -> Result<B, String> + Send + Sync + 'static,
{
    let probes = dataset.probes();
    let organization_id = probes.organization_id;
    let ticket_id = probes.ticket_id;
    let ready = Arc::new(Barrier::new(clients + 1));
    let start = Arc::new(Barrier::new(clients + 1));
    let factory = Arc::new(factory);
    let mut workers = Vec::with_capacity(clients);
    for _ in 0..clients {
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        let factory = Arc::clone(&factory);
        workers.push(thread::spawn(move || -> Result<Vec<u64>, String> {
            let mut backend = factory()?;
            backend
                .point_get_ticket(organization_id, ticket_id)
                .map_err(|error| error.to_string())?;
            ready.wait();
            start.wait();
            let mut samples = Vec::with_capacity(operations_per_client);
            for _ in 0..operations_per_client {
                let began = Instant::now();
                let found = backend
                    .point_get_ticket(organization_id, ticket_id)
                    .map_err(|error| error.to_string())?;
                if found.is_none() {
                    return Err("concurrent point read returned no ticket".to_owned());
                }
                samples.push(u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX));
            }
            Ok(samples)
        }));
    }
    ready.wait();
    let began = Instant::now();
    start.wait();
    let mut samples = Vec::with_capacity(clients.saturating_mul(operations_per_client));
    for worker in workers {
        samples.extend(
            worker
                .join()
                .map_err(|_| "concurrent point-read worker panicked".to_owned())??,
        );
    }
    let elapsed_ns = u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX);
    samples.sort_unstable();
    let operation_count = samples.len();
    let p95_index = operation_count.saturating_sub(1).saturating_mul(95) / 100;
    let p95_ns = samples.get(p95_index).copied().unwrap_or(u64::MAX);
    let throughput_ops_s = u64::try_from(operation_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(1_000_000_000)
        / elapsed_ns.max(1);
    Ok(serde_json::json!({
        "backend_id": backend_id,
        "clients": clients,
        "operations_per_client": operations_per_client,
        "operation_count": operation_count,
        "elapsed_ns": elapsed_ns,
        "throughput_ops_s": throughput_ops_s,
        "p95_ns": p95_ns,
    }))
}

fn assert_write_parity(report: &serde_json::Value) -> Result<(), String> {
    const MAX_RATIO: f64 = 2.0;
    const WRITE_SCENARIOS: [&str; 4] = [
        "create_comment",
        "close_ticket_with_comment",
        "swap_member_roles",
        "open_ticket_with_labels",
    ];

    if report["comparisons"]["available"] != true {
        return Err("write-parity assertion requires both backends".to_owned());
    }
    let seed_ratio = report["comparisons"]["seed"]["ratio_riffdb_over_postgres"]
        .as_f64()
        .ok_or("write-parity assertion is missing the seed ratio")?;
    if !seed_ratio.is_finite() || seed_ratio > MAX_RATIO {
        return Err(format!(
            "write-parity seed gate failed: RiffDB/PostgreSQL is {seed_ratio:.2}x, limit is {MAX_RATIO:.2}x"
        ));
    }
    let scenarios = report["comparisons"]["scenarios"]
        .as_array()
        .ok_or("write-parity assertion is missing scenario ratios")?;
    for expected in WRITE_SCENARIOS {
        let row = scenarios
            .iter()
            .find(|row| row["scenario"].as_str() == Some(expected))
            .ok_or_else(|| format!("write-parity assertion is missing {expected}"))?;
        let ratio = row["ratio_riffdb_over_postgres"]
            .as_f64()
            .ok_or_else(|| format!("write-parity assertion has no ratio for {expected}"))?;
        if !ratio.is_finite() || ratio > MAX_RATIO {
            return Err(format!(
                "write-parity scenario gate failed for {expected}: RiffDB/PostgreSQL is {ratio:.2}x, limit is {MAX_RATIO:.2}x"
            ));
        }
    }
    println!("write-parity gate passed: seed and all write p50 ratios are <= {MAX_RATIO:.2}x");
    Ok(())
}

fn assert_all_parity(report: &serde_json::Value) -> Result<(), String> {
    const MAX_RATIO: f64 = 1.10;
    if report["comparisons"]["available"] != true {
        return Err("all-parity assertion requires both backends".to_owned());
    }
    let seed_ratio = report["comparisons"]["seed"]["ratio_riffdb_over_postgres"]
        .as_f64()
        .ok_or("all-parity assertion is missing the seed ratio")?;
    if !seed_ratio.is_finite() || seed_ratio > MAX_RATIO {
        return Err(format!(
            "all-parity seed gate failed: RiffDB/PostgreSQL is {seed_ratio:.2}x, limit is {MAX_RATIO:.2}x"
        ));
    }
    let scenarios = report["comparisons"]["scenarios"]
        .as_array()
        .ok_or("all-parity assertion is missing scenario ratios")?;
    for expected in riffdb_app_baseline_core::ScenarioId::all() {
        let expected = expected.as_str();
        let row = scenarios
            .iter()
            .find(|row| row["scenario"].as_str() == Some(expected))
            .ok_or_else(|| format!("all-parity assertion is missing {expected}"))?;
        let ratio = row["ratio_riffdb_over_postgres"]
            .as_f64()
            .ok_or_else(|| format!("all-parity assertion has no ratio for {expected}"))?;
        if !ratio.is_finite() || ratio > MAX_RATIO {
            return Err(format!(
                "all-parity scenario gate failed for {expected}: RiffDB/PostgreSQL is {ratio:.2}x, limit is {MAX_RATIO:.2}x"
            ));
        }
    }
    println!("all-parity gate passed: seed and every p50 ratio are <= {MAX_RATIO:.2}x");
    Ok(())
}

fn print_summary(report: &serde_json::Value) {
    println!("\n== TicketDesk app baseline summary ==");
    if let Some(backends) = report["backends"].as_array() {
        for backend in backends {
            let id = backend["backend_id"].as_str().unwrap_or("?");
            let seed_ms = backend["seed_ns"].as_u64().unwrap_or(0) as f64 / 1_000_000.0;
            println!("backend {id}: seed={seed_ms:.1}ms");
            if let Some(scenarios) = backend["scenarios"].as_array() {
                for scenario in scenarios {
                    let name = scenario["scenario"].as_str().unwrap_or("?");
                    let p50 =
                        scenario["timing"]["p50_ns"].as_u64().unwrap_or(0) as f64 / 1_000_000.0;
                    println!("  {name}: p50={p50:.3}ms");
                }
            }
        }
    }
    if report["comparisons"]["available"] == true {
        println!("\nratios (riffdb/postgres p50):");
        if let Some(rows) = report["comparisons"]["scenarios"].as_array() {
            for row in rows {
                let name = row["scenario"].as_str().unwrap_or("?");
                let ratio = row["ratio_riffdb_over_postgres"].as_f64().unwrap_or(0.0);
                println!("  {name}: {ratio:.2}x");
            }
        }
    }
}

struct Args {
    scale: Scale,
    samples: usize,
    warmups: usize,
    postgres_url: Option<String>,
    riffdbd_bin: Option<PathBuf>,
    output: Option<PathBuf>,
    skip_postgres: bool,
    skip_riffdb: bool,
    assert_write_parity: bool,
    assert_all_parity: bool,
    concurrent_clients: Option<usize>,
    concurrent_operations: usize,
}

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut scale = Scale::smoke();
        let mut samples = 3;
        let mut warmups = 1;
        let mut postgres_url = None;
        let mut riffdbd_bin = None;
        let mut output = None;
        let mut skip_postgres = false;
        let mut skip_riffdb = false;
        let mut assert_write_parity = false;
        let mut assert_all_parity = false;
        let mut concurrent_clients = None;
        let mut concurrent_operations = 200;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--smoke" => scale = Scale::smoke(),
                "--full" => {
                    scale = Scale::full();
                    samples = 9;
                    warmups = 2;
                }
                "--samples" => {
                    samples = args
                        .next()
                        .ok_or("--samples needs a value")?
                        .parse()
                        .map_err(|_| "--samples must be usize")?;
                }
                "--warmup" => {
                    warmups = args
                        .next()
                        .ok_or("--warmup needs a value")?
                        .parse()
                        .map_err(|_| "--warmup must be usize")?;
                }
                "--postgres-url" => {
                    postgres_url = Some(args.next().ok_or("--postgres-url needs a value")?);
                }
                "--riffdbd-bin" => {
                    riffdbd_bin = Some(PathBuf::from(
                        args.next().ok_or("--riffdbd-bin needs a value")?,
                    ));
                }
                "--output" => {
                    output = Some(PathBuf::from(args.next().ok_or("--output needs a value")?));
                }
                "--skip-postgres" => skip_postgres = true,
                "--skip-riffdb" => skip_riffdb = true,
                "--assert-write-parity" => assert_write_parity = true,
                "--assert-all-parity" => assert_all_parity = true,
                "--concurrent-clients" => {
                    concurrent_clients = Some(
                        args.next()
                            .ok_or("--concurrent-clients needs a value")?
                            .parse()
                            .map_err(|_| "--concurrent-clients must be usize")?,
                    );
                }
                "--concurrent-operations" => {
                    concurrent_operations = args
                        .next()
                        .ok_or("--concurrent-operations needs a value")?
                        .parse()
                        .map_err(|_| "--concurrent-operations must be usize")?;
                }
                "--help" | "-h" => {
                    return Err(
                        "usage: riffdb-app-baseline [--smoke|--full] [--samples N] [--warmup N] \
                         [--postgres-url URL] [--riffdbd-bin PATH] [--output PATH] \
                         [--assert-write-parity|--assert-all-parity] \
                         [--concurrent-clients N] [--concurrent-operations N] \
                         [--skip-postgres] [--skip-riffdb]"
                            .to_owned(),
                    );
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        if !(1..=100).contains(&samples) {
            return Err("--samples must be 1..=100".to_owned());
        }
        if warmups > 20 {
            return Err("--warmup must be <= 20".to_owned());
        }
        if concurrent_clients.is_some_and(|clients| !(1..=64).contains(&clients)) {
            return Err("--concurrent-clients must be 1..=64".to_owned());
        }
        if !(1..=1_000).contains(&concurrent_operations) {
            return Err("--concurrent-operations must be 1..=1000".to_owned());
        }
        Ok(Self {
            scale,
            samples,
            warmups,
            postgres_url,
            riffdbd_bin,
            output,
            skip_postgres,
            skip_riffdb,
            assert_write_parity,
            assert_all_parity,
            concurrent_clients,
            concurrent_operations,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{assert_all_parity, assert_write_parity};

    fn parity_report(seed_ratio: f64, write_ratio: f64) -> Value {
        let scenarios = [
            "create_comment",
            "close_ticket_with_comment",
            "swap_member_roles",
            "open_ticket_with_labels",
        ]
        .into_iter()
        .map(|scenario| {
            json!({
                "scenario": scenario,
                "ratio_riffdb_over_postgres": write_ratio,
            })
        })
        .collect::<Vec<_>>();
        json!({
            "comparisons": {
                "available": true,
                "seed": {
                    "ratio_riffdb_over_postgres": seed_ratio,
                },
                "scenarios": scenarios,
            }
        })
    }

    #[test]
    fn parity_gate_accepts_its_exact_boundary() {
        assert_write_parity(&parity_report(2.0, 2.0)).expect("exact boundary");
    }

    #[test]
    fn parity_gate_rejects_seed_and_interactive_write_misses() {
        assert!(assert_write_parity(&parity_report(2.01, 0.5)).is_err());
        assert!(assert_write_parity(&parity_report(0.5, 2.01)).is_err());
    }

    #[test]
    fn parity_gate_requires_complete_same_run_evidence() {
        let mut report = parity_report(1.0, 1.0);
        report["comparisons"]["scenarios"]
            .as_array_mut()
            .expect("scenarios")
            .pop();
        assert!(assert_write_parity(&report).is_err());
        assert!(assert_write_parity(&json!({"comparisons": {"available": false}})).is_err());
    }

    #[test]
    fn all_parity_gate_uses_the_strict_application_threshold() {
        assert!(assert_all_parity(&parity_report(1.10, 1.10)).is_err());
        let scenarios = riffdb_app_baseline_core::ScenarioId::all()
            .into_iter()
            .map(|scenario| {
                json!({
                    "scenario": scenario.as_str(),
                    "ratio_riffdb_over_postgres": 1.10,
                })
            })
            .collect::<Vec<_>>();
        let report = json!({
            "comparisons": {
                "available": true,
                "seed": {"ratio_riffdb_over_postgres": 1.10},
                "scenarios": scenarios,
            }
        });
        assert_all_parity(&report).expect("exact strict boundary");
    }
}
