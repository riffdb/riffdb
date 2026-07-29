//! TicketDesk app-baseline runner: live Postgres vs live `riffdbd` over public gRPC.

#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
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

    if !args.skip_postgres {
        let url = args
            .postgres_url
            .clone()
            .or_else(|| env::var("RIFFDB_APP_BASELINE_POSTGRES_URL").ok())
            .ok_or_else(|| {
                "PostgreSQL required: pass --postgres-url or set RIFFDB_APP_BASELINE_POSTGRES_URL"
                    .to_owned()
            })?;
        let mut postgres = PostgresAppBackend::new(url).map_err(|error| error.to_string())?;
        postgres.reset().map_err(|error| error.to_string())?;
        let seed_started = Instant::now();
        postgres
            .seed(&dataset)
            .map_err(|error| error.to_string())?;
        let seed_ns = u64::try_from(seed_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let scenarios = run_scenarios(&mut postgres, &dataset, args.warmups, args.samples)
            .map_err(|error| error.to_string())?;
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
        session
            .backend
            .reset()
            .map_err(|error| error.to_string())?;
        let seed_started = Instant::now();
        session
            .backend
            .seed(&dataset)
            .map_err(|error| error.to_string())?;
        let seed_ns = u64::try_from(seed_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let scenarios = run_scenarios(
            &mut session.backend,
            &dataset,
            args.warmups,
            args.samples,
        )
        .map_err(|error| error.to_string())?;
        backends.push(BackendReport {
            backend_id: "riffdb_public_grpc",
            description: "Live riffdbd over public gRPC (commands + GetEntity + ScanIndex)"
                .to_owned(),
            guarantee_notes: vec![
                "Compiled TicketDesk contract commands for seed/writes".to_owned(),
                "Reads via public GetEntity and ScanIndex RPCs".to_owned(),
                "ticket_detail_page is multi-RPC application composition, not SQL join".to_owned(),
                "Synchronous durable command commits".to_owned(),
            ],
            seed_ns,
            seed_rows: args.scale.approximate_row_count(),
            scenarios,
        });
        session.shutdown().map_err(|error| error.to_string())?;
    }

    if backends.is_empty() {
        return Err("no backends selected".to_owned());
    }

    let report = build_report(args.scale, args.warmups, args.samples, &backends);
    let encoded = serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?;
    if let Some(path) = &args.output {
        fs::write(path, format!("{encoded}\n")).map_err(|error| error.to_string())?;
        println!("wrote {}", path.display());
    } else {
        println!("{encoded}");
    }
    print_summary(&report);
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
                    let p50 = scenario["timing"]["p50_ns"].as_u64().unwrap_or(0) as f64 / 1_000_000.0;
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
                "--help" | "-h" => {
                    return Err(
                        "usage: riffdb-app-baseline [--smoke|--full] [--samples N] [--warmup N] \
                         [--postgres-url URL] [--riffdbd-bin PATH] [--output PATH] \
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
        Ok(Self {
            scale,
            samples,
            warmups,
            postgres_url,
            riffdbd_bin,
            output,
            skip_postgres,
            skip_riffdb,
        })
    }
}
