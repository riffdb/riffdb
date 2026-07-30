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
        postgres.seed(&dataset).map_err(|error| error.to_string())?;
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
        session
            .backend
            .seed(&dataset)
            .map_err(|error| error.to_string())?;
        let seed_ns = u64::try_from(seed_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let scenarios = run_scenarios(&mut session.backend, &dataset, args.warmups, args.samples);
        let scenarios = match scenarios {
            Ok(scenarios) => scenarios,
            Err(error) => {
                if let Ok(groups) = session.shutdown() {
                    eprintln!("riffdb write completion groups 1..16: {groups:?}");
                }
                return Err(error.to_string());
            }
        };
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
            write_completion_groups: Some(write_completion_groups),
        });
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
    if args.assert_write_parity {
        assert_write_parity(&report)?;
    }
    Ok(())
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
                "--help" | "-h" => {
                    return Err(
                        "usage: riffdb-app-baseline [--smoke|--full] [--samples N] [--warmup N] \
                         [--postgres-url URL] [--riffdbd-bin PATH] [--output PATH] \
                         [--assert-write-parity] [--skip-postgres] [--skip-riffdb]"
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
            assert_write_parity,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::assert_write_parity;

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
}
