//! TicketDesk app-baseline runner: live Postgres vs live `riffdbd` over public gRPC.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use riffdb_app_baseline_core::{
    AppBackend, BackendReport, LOAD_CONCURRENCY_SWEEP_CLIENTS, LoadConfig, LoadExecutionShape,
    RIFFDB_MAX_LOAD_CLIENTS, RIFFDB_SATURATE_LOAD_CLIENTS, SATURATE_COORDINATOR_WORKLOAD_CAPACITY,
    SEED_GENERATION, Scale, SeedDataset, WorkloadProfile, assert_board_last_row_counts_equal,
    assert_board_ticket_sequences_equal, board_marginal_from_results, build_report,
    concurrency_curve_point, print_concurrency_sweep_summary, print_load_summary,
    run_closed_loop_load, run_closed_loop_load_with_abort, run_open_loop_load, run_scenarios,
    run_scenarios_with_options, run_stateful_journeys,
};
use riffdb_app_baseline_postgres::{
    PostgresAppBackend, PostgresComparisonProfile, PostgresDurabilitySettings,
    PostgresResourceSnapshot,
};
use riffdb_app_baseline_riffdb::{
    RiffDbServerSession, RiffDbShutdownEvidence, ServerStartOptions, min_free_bytes_for_full,
};
use riffdb_bench_root::{
    BenchRoot, BenchRootOptions, DeviceBaseline, StorageMedium, classify_medium,
    run_device_baseline_for,
};
use serde_json::json;

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
    if args.load_profile.is_some() {
        return run_load(args);
    }
    let dataset = SeedDataset::generate(args.scale);
    let mut backends = Vec::new();
    let mut concurrent_reads = Vec::new();
    let mut postgres_durability: Option<PostgresDurabilitySettings> = None;
    let mut riffdb_database_root: Option<PathBuf> = None;
    let mut riffdb_medium: Option<String> = None;
    let mut device_baseline_json: Option<serde_json::Value> = None;
    let mut environment_json: Option<serde_json::Value> = None;
    let mut integrity_notes: Vec<String> = Vec::new();
    let full_mode = args.scale.name() == "full";
    let min_free = min_free_bytes_for_full(full_mode);

    // Device baseline once per invocation (short for smoke; full for --full).
    if let Some(root) = args.database_root.clone().or_else(|| {
        Some(PathBuf::from(
            riffdb_app_baseline_riffdb::DEFAULT_DATABASE_ROOT,
        ))
    }) {
        let _ = fs::create_dir_all(&root);
        let probe_for = if full_mode {
            std::time::Duration::from_secs(10)
        } else {
            std::time::Duration::from_millis(200)
        };
        if let Ok(baseline) = run_device_baseline_for(&root, probe_for) {
            device_baseline_json = Some(device_baseline_value(&baseline));
        }
    }

    let postgres_host_path = args.postgres_data_host_path.clone().or_else(|| {
        env::var_os("RIFFDB_APP_BASELINE_POSTGRES_DATA_HOST")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    });
    let mut postgres_host_medium: Option<StorageMedium> = None;
    if let Some(path) = &postgres_host_path {
        let _ = fs::create_dir_all(path);
        match classify_medium(path) {
            Ok(medium) => {
                if medium.is_ram_backed() {
                    return Err(format!(
                        "PostgreSQL host data path is RAM-backed ({}); refuse parity on tmpfs",
                        path.display()
                    ));
                }
                postgres_host_medium = Some(medium);
            }
            Err(error) => {
                integrity_notes.push(format!(
                    "could not classify postgres host data path {}: {error}",
                    path.display()
                ));
            }
        }
    }

    // Interleaved reps: PG1, R1, PG2, R2, …
    let mut pg_rep_seed_ns: Vec<u64> = Vec::new();
    let mut pg_rep_scenarios: Vec<Vec<riffdb_app_baseline_core::ScenarioResult>> = Vec::new();
    let mut rd_rep_seed_ns: Vec<u64> = Vec::new();
    let mut rd_rep_scenarios: Vec<Vec<riffdb_app_baseline_core::ScenarioResult>> = Vec::new();
    let mut rd_write_groups: Option<Vec<u64>> = None;
    // Live order-sensitive board page cross-check (rep 0): all static sizes
    // the dense cell can fill (50/200/450 → BoardPage50/200/450 on RiffDB).
    let board_crosscheck_limits: Vec<u32> = [50_u32, 200, 450]
        .into_iter()
        .filter(|&limit| dataset.board_dense_open_count() >= limit as usize)
        .collect();
    let mut board_crosscheck_pg_pages: Option<Vec<(u32, Vec<[u8; 16]>)>> = None;
    let mut board_crosscheck_done = false;

    for rep in 0..args.reps {
        if !args.skip_postgres {
            let url = args
                .postgres_url
                .clone()
                .or_else(|| env::var("RIFFDB_APP_BASELINE_POSTGRES_URL").ok())
                .ok_or_else(|| {
                    "PostgreSQL required: pass --postgres-url or set RIFFDB_APP_BASELINE_POSTGRES_URL"
                        .to_owned()
                })?;
            let mut postgres = PostgresAppBackend::new_with_profile(&url, args.postgres_comparator)
                .map_err(|error| error.to_string())?;
            if postgres_durability.is_none() {
                let settings = postgres
                    .durability_settings()
                    .map_err(|error| error.to_string())?;
                settings.assert_durable_for_parity()?;
                postgres_durability = Some(settings);
            }
            postgres.reset().map_err(|error| error.to_string())?;
            let seed_started = Instant::now();
            postgres.seed(&dataset).map_err(|error| error.to_string())?;
            let seed_ns = u64::try_from(seed_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let scenarios = run_scenarios(&mut postgres, &dataset, args.warmups, args.samples)
                .map_err(|error| error.to_string())?;
            if rep == 0 && !board_crosscheck_limits.is_empty() {
                let probes = dataset.probes();
                let mut pages = Vec::with_capacity(board_crosscheck_limits.len());
                for &limit in &board_crosscheck_limits {
                    let rows = postgres
                        .board_page(
                            probes.board_organization_id,
                            probes.board_project_id,
                            probes.open_status,
                            limit,
                        )
                        .map_err(|error| error.to_string())?;
                    if rows.len() != limit as usize {
                        return Err(format!(
                            "measurement-integrity: postgres board_page({limit}) returned {} rows",
                            rows.len()
                        ));
                    }
                    pages.push((limit, rows.into_iter().map(|row| row.ticket_id).collect()));
                }
                board_crosscheck_pg_pages = Some(pages);
            }
            if rep == 0
                && let Some(clients) = args.concurrent_clients
            {
                let url = Arc::new(url);
                concurrent_reads.push(run_concurrent_point_reads(
                    args.postgres_comparator.backend_id(),
                    clients,
                    args.concurrent_operations,
                    &dataset,
                    move || {
                        PostgresAppBackend::new_with_profile(url.as_str(), args.postgres_comparator)
                            .map_err(|error| error.to_string())
                    },
                )?);
            }
            pg_rep_seed_ns.push(seed_ns);
            pg_rep_scenarios.push(scenarios);
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
                .block_on(RiffDbServerSession::start_with_options(
                    &riffdbd,
                    ServerStartOptions {
                        database_root: args.database_root.clone(),
                        allow_tmpfs: args.allow_tmpfs,
                        min_free_bytes: min_free,
                        ..ServerStartOptions::default()
                    },
                ))
                .map_err(|error| error.to_string())?;

            if riffdb_database_root.is_none() {
                riffdb_database_root = Some(session.bench_root.path().to_path_buf());
                riffdb_medium = Some(session.bench_root.medium().to_report_json());
                if environment_json.is_none() {
                    environment_json =
                        serde_json::from_str(&session.bench_root.environment_report_json()).ok();
                }
            }

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
            // Catch-up + compiled-vs-projected equivalence must pass before timing.
            if let Err(error) = session.backend.prepare_projected_board_gates(&dataset) {
                if let Ok(groups) = session.shutdown() {
                    eprintln!("riffdb write completion groups 1..64: {groups:?}");
                }
                return Err(error.to_string());
            }
            let scenarios = run_scenarios_with_options(
                &mut session.backend,
                &dataset,
                args.warmups,
                args.samples,
                true,
            );
            let scenarios = match scenarios {
                Ok(scenarios) => scenarios,
                Err(error) => {
                    if let Ok(groups) = session.shutdown() {
                        eprintln!("riffdb write completion groups 1..64: {groups:?}");
                    }
                    return Err(error.to_string());
                }
            };
            if rep == 0
                && let Some(pg_pages) = board_crosscheck_pg_pages.as_ref()
            {
                let probes = dataset.probes();
                for (limit, pg_ids) in pg_pages {
                    let rows = session
                        .backend
                        .board_page(
                            probes.board_organization_id,
                            probes.board_project_id,
                            probes.open_status,
                            *limit,
                        )
                        .map_err(|error| error.to_string())?;
                    let rd_ids: Vec<[u8; 16]> = rows.into_iter().map(|row| row.ticket_id).collect();
                    assert_board_ticket_sequences_equal(pg_ids, &rd_ids, *limit)?;
                }
                board_crosscheck_done = true;
                eprintln!(
                    "board-page cross-check ok: live PG and RiffDB returned identical \
                     ticket_id sequences for static board sizes {board_crosscheck_limits:?}"
                );
            }
            if rep == 0
                && let Some(clients) = args.concurrent_clients
            {
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
            rd_write_groups = Some(write_completion_groups.to_vec());
            rd_rep_seed_ns.push(seed_ns);
            rd_rep_scenarios.push(scenarios);
        }
    }

    if !board_crosscheck_limits.is_empty()
        && !args.skip_postgres
        && !args.skip_riffdb
        && !board_crosscheck_done
    {
        return Err(
            "measurement-integrity: board_page live cross-check was required but did not run"
                .to_owned(),
        );
    }

    if !pg_rep_scenarios.is_empty() {
        let seed_ns = median_u64(&pg_rep_seed_ns);
        let scenarios = median_scenarios(&pg_rep_scenarios);
        let mut notes_pg = vec![
            "READ COMMITTED SQL transactions".to_owned(),
            "Relational joins for ticket_detail_page".to_owned(),
        ];
        match args.postgres_comparator {
            PostgresComparisonProfile::Minimal => notes_pg.push(
                "optimized conventional floor; does not reproduce RiffDB audit/provenance/event obligations"
                    .to_owned(),
            ),
            PostgresComparisonProfile::SafeApp => notes_pg.push(
                "application authorization, idempotency, audit/provenance, domain event, and outbox intent are atomic with each mutation"
                    .to_owned(),
            ),
        }
        if let Some(settings) = &postgres_durability {
            notes_pg.push(format!(
                "durability: synchronous_commit={} fsync={} full_page_writes={} wal_sync_method={} data_directory={}",
                settings.synchronous_commit,
                settings.fsync,
                settings.full_page_writes,
                settings.wal_sync_method,
                settings.data_directory
            ));
        }
        backends.push(BackendReport {
            backend_id: args.postgres_comparator.backend_id(),
            description: format!(
                "Live PostgreSQL 18 via SQL (profile={})",
                args.postgres_comparator.backend_id()
            ),
            guarantee_notes: notes_pg,
            seed_ns,
            seed_rows: args.scale.approximate_row_count(),
            scenarios,
            write_completion_groups: None,
        });
    }

    if !rd_rep_scenarios.is_empty() {
        let seed_ns = median_u64(&rd_rep_seed_ns);
        let scenarios = median_scenarios(&rd_rep_scenarios);
        backends.push(BackendReport {
            backend_id: "riffdb_public_grpc",
            description: "Live riffdbd over public gRPC (symbolic commands + named RiffQL + projected board)"
                .to_owned(),
            guarantee_notes: vec![
                "cross-era comparability: since CP3, compiled named reads execute via \
                 NamedQuery with the deployed module hash (client decode differs from \
                 the pre-CP3 generated-client era) and all scenarios run on a \
                 projection-enabled server with background apply — compare compiled \
                 p50s across eras with this disclosure, not raw"
                    .to_owned(),
                "Symbolic TicketDesk commands for seed/writes".to_owned(),
                "Reads via named RiffQL queries (one public RPC per page)".to_owned(),
                "ticket_detail_page is one TicketPage query with dependent key batches".to_owned(),
                "board_page_projected_* via ExecuteProjectedQuery (generated tonic client) after Causal catch-up".to_owned(),
                "board_page_packed_* via ExecuteProjectedQuery response_encoding=PACKED (column-major canonical cells)".to_owned(),
                "Synchronous durable command commits".to_owned(),
            ],
            seed_ns,
            seed_rows: args.scale.approximate_row_count(),
            scenarios,
            write_completion_groups: rd_write_groups.clone(),
        });
    }

    if backends.is_empty() {
        return Err("no backends selected".to_owned());
    }

    assert_board_last_row_counts_equal(&backends)?;

    let mut report = build_report(args.scale, args.warmups, args.samples, &backends);
    report["scale_shape"] = json!({
        "organizations": args.scale.organizations,
        "projects_per_org": args.scale.projects_per_org,
        "tickets_per_project": args.scale.tickets_per_project,
        "comments_per_ticket": args.scale.comments_per_ticket,
        "labels_per_ticket": args.scale.labels_per_ticket,
        "board_dense_open": args.scale.board_dense_open,
        "payload_bytes": args.scale.payload_bytes,
        "comment_payload_bytes": args.scale.payload_bytes,
        "ticket_title_payload_bytes": args.scale.payload_bytes.min(128),
    });
    if !concurrent_reads.is_empty() {
        report["concurrent_point_reads"] = serde_json::Value::Array(concurrent_reads);
    }
    // Gated comparison scalars become rep summaries. When reps==1 the summaries
    // are still attached so stability machinery is uniform.
    attach_rep_summaries(
        &mut report,
        &pg_rep_seed_ns,
        &pg_rep_scenarios,
        &rd_rep_seed_ns,
        &rd_rep_scenarios,
    );
    if board_crosscheck_done {
        report["board_page_live_crosscheck"] = json!({
            "status": "ok",
            "limits": board_crosscheck_limits,
            "limit_mode": "static_compiled",
            "order_sensitive": true,
        });
    }
    if let Some(baseline) = device_baseline_json {
        report["device_baseline"] = baseline;
    }
    if let Some(environment) = environment_json {
        report["environment"] = environment;
    }
    if let Some(settings) = &postgres_durability {
        report["postgres_durability"] = json!({
            "server_version_num": settings.server_version_num,
            "synchronous_commit": settings.synchronous_commit,
            "fsync": settings.fsync,
            "full_page_writes": settings.full_page_writes,
            "wal_sync_method": settings.wal_sync_method,
            "data_directory": settings.data_directory,
        });
    }
    if let Some(path) = &postgres_host_path {
        report["postgres_data_host_path"] = json!(path.display().to_string());
    }
    if let Some(medium) = &postgres_host_medium {
        report["postgres_storage_medium"] = serde_json::from_str(&medium.to_report_json())
            .unwrap_or(json!(medium.to_report_json()));
    }
    if let Some(path) = &riffdb_database_root {
        report["riffdb_database_root"] = json!(path.display().to_string());
    }
    if let Some(medium) = &riffdb_medium {
        report["riffdb_storage_medium"] = serde_json::from_str(medium).unwrap_or(json!(medium));
    }
    // Same-device check: compare mount device models / mount paths when both known.
    let same_device = match (
        postgres_host_medium.as_ref(),
        riffdb_medium
            .as_ref()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()),
    ) {
        (Some(pg), Some(rd_json)) => {
            let pg_json: serde_json::Value =
                serde_json::from_str(&pg.to_report_json()).unwrap_or(json!({}));
            let pg_mount = pg_json["mount"].as_str().unwrap_or("");
            let rd_mount = rd_json["mount"].as_str().unwrap_or("");
            let pg_model = pg_json["device_model"].as_str();
            let rd_model = rd_json["device_model"].as_str();
            let same = match (pg_model, rd_model) {
                (Some(a), Some(b)) if a != "null" && b != "null" => a == b,
                _ => !pg_mount.is_empty() && pg_mount == rd_mount,
            };
            if !same {
                let msg = format!(
                    "same-device warning: postgres mount/device ({pg_mount}/{pg_model:?}) \
                     differs from riffdb ({rd_mount}/{rd_model:?})"
                );
                eprintln!("{msg}");
                integrity_notes.push(msg);
            }
            same
        }
        _ => {
            integrity_notes.push(
                "same-device check incomplete: missing postgres host path or riffdb medium"
                    .to_owned(),
            );
            false
        }
    };
    report["same_device"] = json!(same_device);
    if !integrity_notes.is_empty() {
        report["integrity_notes"] = json!(integrity_notes);
    }
    report["reps"] = json!(args.reps);
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
    if args.require_stable {
        require_stable(&report)?;
    }
    Ok(())
}

#[derive(Debug)]
struct LoadEvidenceContext {
    database_root: PathBuf,
    environment: serde_json::Value,
    riffdb_medium: serde_json::Value,
    device_baseline: Option<serde_json::Value>,
    postgres_host_path: Option<PathBuf>,
    postgres_medium: Option<serde_json::Value>,
    postgres_durability: Option<PostgresDurabilitySettings>,
    backend_execution_order: Vec<&'static str>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ProcessResourceSnapshot {
    cpu_ticks: u64,
    rss_bytes: u64,
    read_bytes: u64,
    write_bytes: u64,
    durable_bytes: u64,
}

impl ProcessResourceSnapshot {
    fn delta_json(self, before: Self) -> serde_json::Value {
        json!({
            "cpu_ticks": self.cpu_ticks.saturating_sub(before.cpu_ticks),
            "rss_bytes_before": before.rss_bytes,
            "rss_bytes_after": self.rss_bytes,
            "rss_bytes_peak_sampled": before.rss_bytes.max(self.rss_bytes),
            "process_read_bytes": self.read_bytes.saturating_sub(before.read_bytes),
            "process_write_bytes": self.write_bytes.saturating_sub(before.write_bytes),
            "durable_bytes_before": before.durable_bytes,
            "durable_bytes_after": self.durable_bytes,
            "durable_bytes_growth": self.durable_bytes.saturating_sub(before.durable_bytes),
            "scope": "before_after_point",
        })
    }
}

fn process_resource_snapshot(
    pid: u32,
    durable_root: &Path,
) -> Result<ProcessResourceSnapshot, String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|error| format!("read process stat for {pid}: {error}"))?;
    let close = stat
        .rfind(')')
        .ok_or_else(|| format!("process stat for {pid} has no command terminator"))?;
    let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
    let parse_field = |index: usize, name: &str| -> Result<u64, String> {
        fields
            .get(index)
            .ok_or_else(|| format!("process stat for {pid} lacks {name}"))?
            .parse::<u64>()
            .map_err(|_| format!("process stat for {pid} has invalid {name}"))
    };
    // `fields` starts at proc field 3 (state); utime/stime are fields 14/15.
    let cpu_ticks = parse_field(11, "utime")?.saturating_add(parse_field(12, "stime")?);
    let status = fs::read_to_string(format!("/proc/{pid}/status"))
        .map_err(|error| format!("read process status for {pid}: {error}"))?;
    let rss_kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let io = fs::read_to_string(format!("/proc/{pid}/io"))
        .map_err(|error| format!("read process io for {pid}: {error}"))?;
    let io_counter = |name: &str| -> u64 {
        io.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    Ok(ProcessResourceSnapshot {
        cpu_ticks,
        rss_bytes: rss_kib.saturating_mul(1024),
        read_bytes: io_counter("read_bytes:"),
        write_bytes: io_counter("write_bytes:"),
        durable_bytes: directory_bytes_bounded(durable_root, 100_000)?,
    })
}

fn directory_bytes_bounded(root: &Path, max_entries: usize) -> Result<u64, String> {
    let mut stack = vec![root.to_path_buf()];
    let mut entries = 0_usize;
    let mut bytes = 0_u64;
    while let Some(path) = stack.pop() {
        let read = fs::read_dir(&path)
            .map_err(|error| format!("read durable directory {}: {error}", path.display()))?;
        for entry in read {
            let entry = entry.map_err(|error| format!("read durable entry: {error}"))?;
            entries = entries.saturating_add(1);
            if entries > max_entries {
                return Err(format!(
                    "durable directory accounting exceeded bounded entry limit {max_entries}"
                ));
            }
            let metadata = entry
                .metadata()
                .map_err(|error| format!("stat durable entry: {error}"))?;
            if metadata.is_dir() {
                stack.push(entry.path());
            } else if metadata.is_file() {
                bytes = bytes.saturating_add(metadata.len());
            }
        }
    }
    Ok(bytes)
}

fn postgres_resource_delta_json(
    before: &PostgresResourceSnapshot,
    after: &PostgresResourceSnapshot,
) -> serde_json::Value {
    json!({
        "committed_transactions": after.committed_transactions.saturating_sub(before.committed_transactions),
        "blocks_read": after.blocks_read.saturating_sub(before.blocks_read),
        "blocks_hit": after.blocks_hit.saturating_sub(before.blocks_hit),
        "temporary_bytes": after.temporary_bytes.saturating_sub(before.temporary_bytes),
        "database_bytes_before": before.database_bytes,
        "database_bytes_after": after.database_bytes,
        "database_bytes_growth": after.database_bytes.saturating_sub(before.database_bytes),
        "wal_bytes": after.wal_bytes.saturating_sub(before.wal_bytes),
        "scope": "before_after_point",
    })
}

fn riffdb_shutdown_evidence_json(evidence: &RiffDbShutdownEvidence) -> serde_json::Value {
    json!({
        "write_completion_groups_by_size": evidence.write_completion_groups.to_vec(),
        "dispatch_reasons": {
            "full": evidence.dispatch_reasons[0],
            "barrier": evidence.dispatch_reasons[1],
            "queue_drained": evidence.dispatch_reasons[2],
            "receiver_closed": evidence.dispatch_reasons[3],
        },
        "read_pipeline_stages": evidence.read_stages.iter().map(|stage| json!({
            "stage": stage.name,
            "count": stage.count,
            "sum_us": stage.sum_us,
            "cumulative_buckets": stage.buckets,
        })).collect::<Vec<_>>(),
        "command_pipeline_stages": evidence.command_stages.iter().map(|stage| json!({
            "stage": stage.name,
            "count": stage.count,
            "sum_us": stage.sum_us,
            "cumulative_buckets": stage.buckets,
        })).collect::<Vec<_>>(),
        "labels": "closed_fixed_cardinality",
    })
}

impl LoadEvidenceContext {
    fn collect(args: &Args) -> Result<Self, String> {
        let full_mode = args.scale.name() == "full";
        let root = BenchRoot::resolve(BenchRootOptions {
            harness: "app-baseline-load",
            cli_override: args.database_root.clone(),
            default_root: PathBuf::from(riffdb_app_baseline_riffdb::DEFAULT_DATABASE_ROOT),
            allow_tmpfs: args.allow_tmpfs,
            min_free_bytes: min_free_bytes_for_full(full_mode),
        })
        .map_err(|error| error.to_string())?;
        let environment = serde_json::from_str(&root.environment_report_json())
            .map_err(|error| format!("decode benchmark environment report: {error}"))?;
        let riffdb_medium = serde_json::from_str(&root.medium().to_report_json())
            .map_err(|error| format!("decode RiffDB storage-medium report: {error}"))?;
        let probe_for = if full_mode {
            Duration::from_secs(10)
        } else {
            Duration::from_millis(200)
        };
        let device_baseline = run_device_baseline_for(root.path(), probe_for)
            .ok()
            .map(|baseline| device_baseline_value(&baseline));

        let postgres_host_path = args.postgres_data_host_path.clone().or_else(|| {
            env::var_os("RIFFDB_APP_BASELINE_POSTGRES_DATA_HOST")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        });
        let postgres_medium = if let Some(path) = &postgres_host_path {
            fs::create_dir_all(path).map_err(|error| {
                format!(
                    "create PostgreSQL benchmark data root {}: {error}",
                    path.display()
                )
            })?;
            let medium = classify_medium(path).map_err(|error| error.to_string())?;
            if medium.is_ram_backed() && !args.allow_tmpfs {
                return Err(format!(
                    "PostgreSQL host data path is RAM-backed ({}); refuse load comparison on tmpfs",
                    path.display()
                ));
            }
            Some(
                serde_json::from_str(&medium.to_report_json())
                    .map_err(|error| format!("decode PostgreSQL storage-medium report: {error}"))?,
            )
        } else {
            None
        };

        Ok(Self {
            database_root: root.path().to_path_buf(),
            environment,
            riffdb_medium,
            device_baseline,
            postgres_host_path,
            postgres_medium,
            postgres_durability: None,
            backend_execution_order: Vec::new(),
        })
    }
}

fn attach_load_evidence(
    suite: &mut serde_json::Value,
    evidence: &LoadEvidenceContext,
    curve: &[serde_json::Value],
    reps: usize,
    non_evidentiary_window: bool,
) -> Result<(), String> {
    suite["reps"] = json!(reps);
    suite["environment"] = evidence.environment.clone();
    suite["device_baseline"] = evidence
        .device_baseline
        .clone()
        .unwrap_or(serde_json::Value::Null);
    suite["riffdb_database_root"] = json!(evidence.database_root.display().to_string());
    suite["riffdb_storage_medium"] = evidence.riffdb_medium.clone();
    suite["backend_execution_order"] = json!(evidence.backend_execution_order);
    if let Some(path) = &evidence.postgres_host_path {
        suite["postgres_data_host_path"] = json!(path.display().to_string());
    }
    if let Some(medium) = &evidence.postgres_medium {
        suite["postgres_storage_medium"] = medium.clone();
    }
    if let Some(settings) = &evidence.postgres_durability {
        suite["postgres_durability"] = json!({
            "server_version_num": settings.server_version_num,
            "synchronous_commit": settings.synchronous_commit,
            "fsync": settings.fsync,
            "full_page_writes": settings.full_page_writes,
            "wal_sync_method": settings.wal_sync_method,
            "data_directory": settings.data_directory,
        });
    }

    let same_device = evidence
        .postgres_medium
        .as_ref()
        .map(|postgres| storage_media_match(postgres, &evidence.riffdb_medium));
    suite["same_device"] = same_device.map_or(serde_json::Value::Null, serde_json::Value::Bool);

    let summaries = load_rep_summaries(curve);
    let unstable = summaries.values().any(summary_has_unstable_metric);
    suite["load_rep_summaries"] = serde_json::to_value(&summaries)
        .map_err(|error| format!("encode load repetition summaries: {error}"))?;

    let contains_postgres = suite["backends"].as_array().is_some_and(|reports| {
        reports.iter().any(|report| {
            report["backend_id"]
                .as_str()
                .is_some_and(|backend| backend.starts_with("postgres_"))
        })
    });
    let contains_riffdb = suite["backends"].as_array().is_some_and(|reports| {
        reports
            .iter()
            .any(|report| report["backend_id"] == "riffdb_public_grpc")
    });
    let mut missing = Vec::new();
    if evidence.device_baseline.is_none() {
        missing.push("device_baseline");
    }
    if contains_postgres && evidence.postgres_durability.is_none() {
        missing.push("postgres_durability");
    }
    if contains_postgres && evidence.postgres_medium.is_none() {
        missing.push("postgres_storage_medium");
    }
    if contains_postgres && contains_riffdb && same_device.is_none() {
        missing.push("same_device");
    }
    let correctness_clean = suite["correctness"]["clean"].as_bool().unwrap_or(false);
    let resources_complete = suite["backends"].as_array().is_some_and(|reports| {
        !reports.is_empty()
            && reports
                .iter()
                .all(|report| report["resource_delta"].is_object())
    });
    if !resources_complete {
        missing.push("resource_delta");
    }
    let comparable_device = !contains_postgres || !contains_riffdb || same_device == Some(true);
    suite["evidence_eligibility"] = json!({
        "eligible": !non_evidentiary_window && reps >= 2 && !unstable && missing.is_empty()
            && correctness_clean && comparable_device,
        "non_evidentiary_window": non_evidentiary_window,
        "stable": !unstable,
        "correctness_clean": correctness_clean,
        "resource_attribution_complete": resources_complete,
        "same_device_comparable": comparable_device,
        "missing_required_fields": missing,
        "comparison_complete": contains_postgres && contains_riffdb,
    });
    Ok(())
}

fn storage_media_match(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (
        left["device_model"].as_str(),
        right["device_model"].as_str(),
    ) {
        (Some(a), Some(b)) if !a.is_empty() && !b.is_empty() => a == b,
        _ => {
            let left_mount = left["mount"].as_str().unwrap_or("");
            let right_mount = right["mount"].as_str().unwrap_or("");
            !left_mount.is_empty() && left_mount == right_mount
        }
    }
}

fn load_rep_summaries(curve: &[serde_json::Value]) -> BTreeMap<String, serde_json::Value> {
    let mut grouped: BTreeMap<(String, u64), Vec<&serde_json::Value>> = BTreeMap::new();
    for point in curve {
        let Some(backend) = point["backend_id"].as_str() else {
            continue;
        };
        let Some(clients) = point["clients"].as_u64() else {
            continue;
        };
        grouped
            .entry((backend.to_owned(), clients))
            .or_default()
            .push(point);
    }
    grouped
        .into_iter()
        .map(|((backend, clients), points)| {
            let metric = |name: &str| {
                scalar_summary(
                    &points
                        .iter()
                        .filter_map(|point| point[name].as_f64())
                        .collect::<Vec<_>>(),
                )
            };
            (
                format!("{backend}@clients={clients}"),
                json!({
                    "backend_id": backend,
                    "clients": clients,
                    "throughput_ops_s": metric("throughput_ops_s"),
                    "aggregate_p50_ns": metric("aggregate_p50_ns"),
                    "aggregate_p95_ns": metric("aggregate_p95_ns"),
                    "aggregate_p99_ns": metric("aggregate_p99_ns"),
                    "create_comment_p50_ns": metric("create_comment_p50_ns"),
                    "close_ticket_with_comment_p50_ns": metric("close_ticket_with_comment_p50_ns"),
                    "open_ticket_with_labels_p50_ns": metric("open_ticket_with_labels_p50_ns"),
                }),
            )
        })
        .collect()
}

fn summary_has_unstable_metric(summary: &serde_json::Value) -> bool {
    summary.as_object().is_some_and(|fields| {
        fields.values().any(|value| {
            value["stability"]
                .as_str()
                .is_some_and(|stability| stability == "unstable")
        })
    })
}

fn require_load_stable(report: &serde_json::Value) -> Result<(), String> {
    if report["reps"].as_u64().unwrap_or(1) < 2 {
        return Err(
            "--require-stable requires --reps >= 2 (single-rep load stability is trivial)"
                .to_owned(),
        );
    }
    let unstable = report["load_rep_summaries"]
        .as_object()
        .map(|summaries| {
            summaries
                .iter()
                .filter(|(_, summary)| summary_has_unstable_metric(summary))
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if unstable.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "--require-stable failed; unstable load points: {}",
            unstable.join(", ")
        ))
    }
}

fn run_load(args: Args) -> Result<(), String> {
    let mut evidence = LoadEvidenceContext::collect(&args)?;
    let profile = args
        .load_profile
        .ok_or_else(|| "internal: load profile required".to_owned())?;
    let mut base_config = if args.scale.approximate_row_count() < 1_000 {
        LoadConfig::smoke(profile)
    } else {
        LoadConfig::standard(profile, args.load_clients.unwrap_or(32))
    };
    if let Some(clients) = args.load_clients {
        base_config.clients = clients.clamp(1, 128);
    }
    if let Some(seconds) = args.load_duration_secs {
        base_config.duration = Duration::from_secs(seconds.max(1));
    }
    if let Some(seconds) = args.load_warmup_secs {
        base_config.warmup = Duration::from_secs(seconds);
    }
    if let Some(zipf_s) = args.load_zipf_s {
        base_config.zipf_s = zipf_s;
    }
    base_config.open_loop_rate_per_sec = args.load_open_loop_rate;
    base_config.open_loop_queue_depth = args.load_open_loop_queue_depth;
    base_config.tenant_count = args.load_tenants;
    base_config.hot_tenant_percent = args.load_hot_tenant_percent;
    base_config.contended = args.load_contended;
    base_config.saturate = args.load_saturate;
    // Default 250 ms = 150 ms admission budget + accepted group-turn + bucket
    // slack; success-only p99 excludes Overloaded waits that burn the full cap.
    base_config.saturate_p99_ceiling = Duration::from_millis(args.load_saturate_p99_ms.max(1));
    if args.load_saturate {
        // Long-lived fan-out multiplies per-client concurrency; do not force
        // client count (knee sweeps must control --load-clients). PostgreSQL is
        // skipped: saturate is a RiffDB coordinator-depth probe.
        base_config.saturate_fanout = riffdb_app_baseline_core::SATURATE_DEFAULT_FANOUT;
    }

    let client_points: Vec<usize> = if args.load_concurrency_sweep {
        LOAD_CONCURRENCY_SWEEP_CLIENTS.to_vec()
    } else {
        vec![base_config.clients]
    };

    let dataset = SeedDataset::generate(args.scale);
    let mut reports = Vec::new();
    let mut curve = Vec::new();
    let mut deferred_failure: Option<String> = None;
    let mut journey_reports = Vec::new();
    let mut non_evidentiary = false;
    if base_config.duration < Duration::from_secs(60) {
        non_evidentiary = true;
    }

    // Saturate is riffdb-only; never force 512 PG sessions.
    let skip_postgres = args.skip_postgres || args.load_saturate;
    let sweep_isolation = if args.load_sweep_per_level_daemon {
        "per_level_daemon"
    } else {
        "shared_daemon_accumulated_history"
    };

    // Sequential exclusive backends: complete every PostgreSQL rep/point first,
    // then every RiffDB rep/point. Never interleave — concurrent dual load
    // competes for CPU/IO (and with harness Docker, docker-proxy). Prefer the
    // outer `run-app-baseline` dual-phase path, which also tears down Postgres
    // before starting RiffDB.
    if !skip_postgres {
        evidence
            .backend_execution_order
            .push(args.postgres_comparator.backend_id());
        eprintln!("load phase: PostgreSQL only (all reps/points before RiffDB)");
        let url = args
            .postgres_url
            .clone()
            .or_else(|| env::var("RIFFDB_APP_BASELINE_POSTGRES_URL").ok())
            .ok_or_else(|| {
                "PostgreSQL required: pass --postgres-url or set RIFFDB_APP_BASELINE_POSTGRES_URL"
                    .to_owned()
            })?;
        for rep in 0..args.reps {
            let mut postgres = PostgresAppBackend::new_with_profile(&url, args.postgres_comparator)
                .map_err(|error| error.to_string())?;
            if rep == 0 {
                let settings = postgres
                    .durability_settings()
                    .map_err(|error| error.to_string())?;
                settings.assert_durable_for_parity()?;
                evidence.postgres_durability = Some(settings.clone());
            }
            let capacity = postgres
                .load_session_capacity()
                .map_err(|error| error.to_string())?;
            let max_clients = *client_points.iter().max().unwrap_or(&1);
            if max_clients > capacity {
                return Err(format!(
                    "concurrency sweep/load clients max {max_clients} exceeds live PostgreSQL safe \
                     session capacity {capacity}; raise max_connections (e.g. docker -c max_connections=200) \
                     or lower the client set"
                ));
            }
            let shared_seed_ns = if args.load_sweep_per_level_daemon {
                None
            } else {
                postgres.reset().map_err(|error| error.to_string())?;
                let seed_started = Instant::now();
                postgres.seed(&dataset).map_err(|error| error.to_string())?;
                let seed_ns = u64::try_from(seed_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                if args.load_journeys {
                    let mut journey = run_stateful_journeys(
                        &mut postgres,
                        &dataset,
                        (rep as u64).saturating_mul(1_000_000),
                    )?;
                    journey["backend_id"] = json!(args.postgres_comparator.backend_id());
                    journey["rep"] = json!(rep);
                    journey["sweep_scope"] = json!("shared_history");
                    journey_reports.push(journey);
                }
                Some(seed_ns)
            };
            drop(postgres);
            let worker_url = Arc::new(url.clone());
            const SAMPLE_GAP: u64 = 1_000_000_000;
            for (point_index, &clients) in client_points.iter().enumerate() {
                let seed_ns = if let Some(seed_ns) = shared_seed_ns {
                    seed_ns
                } else {
                    let mut control =
                        PostgresAppBackend::new_with_profile(&url, args.postgres_comparator)
                            .map_err(|error| error.to_string())?;
                    control.reset().map_err(|error| error.to_string())?;
                    let seed_started = Instant::now();
                    control.seed(&dataset).map_err(|error| error.to_string())?;
                    let seed_ns =
                        u64::try_from(seed_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    if args.load_journeys {
                        let mut journey = run_stateful_journeys(
                            &mut control,
                            &dataset,
                            (rep as u64)
                                .saturating_mul(1_000_000)
                                .saturating_add((point_index as u64).saturating_mul(1_000)),
                        )?;
                        journey["backend_id"] = json!(args.postgres_comparator.backend_id());
                        journey["rep"] = json!(rep);
                        journey["clients"] = json!(clients);
                        journey_reports.push(journey);
                    }
                    seed_ns
                };
                let mut config = base_config.clone();
                config.clients = clients;
                config.sample_id_base = (point_index as u64)
                    .saturating_mul(SAMPLE_GAP)
                    .saturating_add((rep as u64).saturating_mul(SAMPLE_GAP / 16));
                let mut resource_control =
                    PostgresAppBackend::new_with_profile(&url, args.postgres_comparator)
                        .map_err(|error| error.to_string())?;
                let resources_before = resource_control
                    .resource_snapshot()
                    .map_err(|error| error.to_string())?;
                let reconciliation_sample = config.sample_id_base.saturating_add(SAMPLE_GAP / 32);
                let report = if config.open_loop_rate_per_sec.is_some() {
                    let factory_url = Arc::clone(&worker_url);
                    run_open_loop_load(
                        args.postgres_comparator.backend_id(),
                        config,
                        LoadExecutionShape {
                            transport_topology: "per_session_tcp",
                            command_attempt_budget: 1,
                        },
                        &dataset,
                        seed_ns,
                        move || {
                            let mut backend = PostgresAppBackend::new_with_profile(
                                factory_url.as_str(),
                                args.postgres_comparator,
                            )
                            .map_err(|error| error.to_string())?;
                            backend.prewarm().map_err(|error| error.to_string())?;
                            Ok(backend)
                        },
                    )?
                } else {
                    let factory_url = Arc::clone(&worker_url);
                    run_closed_loop_load(
                        args.postgres_comparator.backend_id(),
                        config,
                        LoadExecutionShape {
                            transport_topology: "per_session_tcp",
                            command_attempt_budget: 1,
                        },
                        &dataset,
                        seed_ns,
                        move || {
                            let mut backend = PostgresAppBackend::new_with_profile(
                                factory_url.as_str(),
                                args.postgres_comparator,
                            )
                            .map_err(|error| error.to_string())?;
                            backend.prewarm().map_err(|error| error.to_string())?;
                            Ok(backend)
                        },
                    )?
                };
                // Join alone is not enough: prove the server sees zero load
                // sessions before we record the point as complete.
                assert_postgres_load_quiesced(&mut resource_control, clients, rep)?;
                if args.load_journeys {
                    let mut journey = run_stateful_journeys(
                        &mut resource_control,
                        &dataset,
                        reconciliation_sample,
                    )?;
                    journey["backend_id"] = json!(args.postgres_comparator.backend_id());
                    journey["rep"] = json!(rep);
                    journey["clients"] = json!(clients);
                    journey["phase"] = json!("post_load_reconciliation_canary");
                    journey_reports.push(journey);
                }
                let resources_after = resource_control
                    .resource_snapshot()
                    .map_err(|error| error.to_string())?;
                print_load_summary(&report);
                let mut point = concurrency_curve_point(&report);
                point["rep"] = json!(rep);
                point["postgres_quiesced"] = json!(true);
                point["sweep_isolation"] = json!(sweep_isolation);
                point["resource_delta"] =
                    postgres_resource_delta_json(&resources_before, &resources_after);
                curve.push(point);
                let mut json_report = report.to_json();
                json_report["rep"] = json!(rep);
                json_report["postgres_quiesced"] = json!(true);
                json_report["sweep_isolation"] = json!(sweep_isolation);
                json_report["resource_delta"] =
                    postgres_resource_delta_json(&resources_before, &resources_after);
                reports.push(json_report);
            }
        }
        // Final phase gate: no foreign clients before we hand off (or before the
        // outer harness tears down Docker PG).
        let mut phase_control =
            PostgresAppBackend::new_with_profile(&url, args.postgres_comparator)
                .map_err(|error| error.to_string())?;
        assert_postgres_load_quiesced(&mut phase_control, 0, usize::MAX)?;
        eprintln!(
            "PostgreSQL load phase complete and server-verified quiesced \
             (pg_stat_activity: no foreign client backends). \
             RiffDB phase begins next (outer harness should stop Docker PG first)."
        );
    }

    if !args.skip_riffdb {
        evidence.backend_execution_order.push("riffdb_public_grpc");
        eprintln!("load phase: RiffDB only (PostgreSQL phase already finished)");
        for rep in 0..args.reps {
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

            let start_options = ServerStartOptions {
                coordinator_workload_capacity: args
                    .load_saturate
                    .then_some(SATURATE_COORDINATOR_WORKLOAD_CAPACITY),
                database_root: args.database_root.clone(),
                allow_tmpfs: args.allow_tmpfs,
                min_free_bytes: min_free_bytes_for_full(args.scale.name() == "full"),
            };
            let transport_topology = if args.load_saturate {
                RiffDbTransport::PerSession
            } else {
                args.riffdb_transport
            };
            const SAMPLE_GAP: u64 = 1_000_000_000;

            if args.load_sweep_per_level_daemon {
                for (point_index, &clients) in client_points.iter().enumerate() {
                    let session = runtime
                        .block_on(RiffDbServerSession::start_with_options(
                            &riffdbd,
                            start_options.clone(),
                        ))
                        .map_err(|error| error.to_string())?;
                    let session = Arc::new(std::sync::Mutex::new(session));
                    let seed_ns;
                    {
                        let mut guard = session.lock().map_err(|_| "session lock".to_owned())?;
                        guard.backend.reset().map_err(|error| error.to_string())?;
                        let seed_started = Instant::now();
                        if let Err(error) = guard.backend.seed(&dataset) {
                            drop(guard);
                            if let Ok(owned) = Arc::try_unwrap(session) {
                                let _ = owned.into_inner().map(|s| s.shutdown());
                            }
                            return Err(error.to_string());
                        }
                        seed_ns =
                            u64::try_from(seed_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                        if args.load_journeys {
                            let mut journey = run_stateful_journeys(
                                &mut guard.backend,
                                &dataset,
                                (rep as u64)
                                    .saturating_mul(1_000_000)
                                    .saturating_add((point_index as u64).saturating_mul(1_000)),
                            )?;
                            journey["backend_id"] = json!("riffdb_public_grpc");
                            journey["rep"] = json!(rep);
                            journey["clients"] = json!(clients);
                            journey_reports.push(journey);
                        }
                    }
                    let mut config = base_config.clone().with_client_cap(if args.load_saturate {
                        RIFFDB_SATURATE_LOAD_CLIENTS
                    } else {
                        RIFFDB_MAX_LOAD_CLIENTS
                    });
                    config.clients = clients.clamp(
                        1,
                        if args.load_saturate {
                            RIFFDB_SATURATE_LOAD_CLIENTS
                        } else {
                            RIFFDB_MAX_LOAD_CLIENTS
                        },
                    );
                    config.sample_id_base = (point_index as u64)
                        .saturating_mul(SAMPLE_GAP)
                        .saturating_add((rep as u64).saturating_mul(SAMPLE_GAP / 16));
                    let prototype = {
                        let guard = session.lock().map_err(|_| "session lock".to_owned())?;
                        guard.backend.clone().with_command_attempt_budget(1)
                    };
                    let resources_before = {
                        let guard = session.lock().map_err(|_| "session lock".to_owned())?;
                        process_resource_snapshot(guard.child_pid(), guard.session_directory())?
                    };
                    let reconciliation_sample =
                        config.sample_id_base.saturating_add(SAMPLE_GAP / 32);
                    let load_result = if config.open_loop_rate_per_sec.is_some() {
                        run_open_loop_load(
                            "riffdb_public_grpc",
                            config,
                            LoadExecutionShape {
                                transport_topology: transport_topology.as_report_str(),
                                command_attempt_budget: 1,
                            },
                            &dataset,
                            seed_ns,
                            {
                                let prototype = prototype.clone();
                                move || {
                                    let mut backend = match transport_topology {
                                        RiffDbTransport::PerSession => prototype.fresh_session(),
                                        RiffDbTransport::Shared => Ok(prototype.clone()),
                                    }
                                    .map_err(|error| error.to_string())?;
                                    backend.prewarm().map_err(|error| error.to_string())?;
                                    Ok(backend)
                                }
                            },
                        )
                    } else {
                        let abort = {
                            let session = Arc::clone(&session);
                            Arc::new(move || {
                                let mut guard = session.lock().ok()?;
                                guard.server_alive().err()
                            })
                                as Arc<dyn Fn() -> Option<String> + Send + Sync>
                        };
                        run_closed_loop_load_with_abort(
                            "riffdb_public_grpc",
                            config,
                            LoadExecutionShape {
                                transport_topology: transport_topology.as_report_str(),
                                command_attempt_budget: 1,
                            },
                            &dataset,
                            seed_ns,
                            {
                                let prototype = prototype.clone();
                                move || {
                                    let mut backend = match transport_topology {
                                        RiffDbTransport::PerSession => prototype.fresh_session(),
                                        RiffDbTransport::Shared => Ok(prototype.clone()),
                                    }
                                    .map_err(|error| error.to_string())?;
                                    backend.prewarm().map_err(|error| error.to_string())?;
                                    Ok(backend)
                                }
                            },
                            Some(abort),
                        )
                    };
                    if load_result.is_ok() && args.load_journeys {
                        let mut guard = session.lock().map_err(|_| "session lock".to_owned())?;
                        let mut journey = run_stateful_journeys(
                            &mut guard.backend,
                            &dataset,
                            reconciliation_sample,
                        )?;
                        journey["backend_id"] = json!("riffdb_public_grpc");
                        journey["rep"] = json!(rep);
                        journey["clients"] = json!(clients);
                        journey["phase"] = json!("post_load_reconciliation_canary");
                        journey_reports.push(journey);
                    }
                    let resources_after = {
                        let guard = session.lock().map_err(|_| "session lock".to_owned())?;
                        process_resource_snapshot(guard.child_pid(), guard.session_directory())
                    };
                    let owned = Arc::try_unwrap(session)
                        .map_err(|_| "session still shared after load".to_owned())?
                        .into_inner()
                        .map_err(|_| "session mutex poisoned".to_owned())?;
                    match load_result {
                        Ok(report) => {
                            let resources_after = resources_after?;
                            print_load_summary(&report);
                            let mut point = concurrency_curve_point(&report);
                            point["rep"] = json!(rep);
                            point["sweep_isolation"] = json!(sweep_isolation);
                            point["resource_delta"] = resources_after.delta_json(resources_before);
                            let mut json_report = report.to_json();
                            json_report["rep"] = json!(rep);
                            json_report["sweep_isolation"] = json!(sweep_isolation);
                            json_report["resource_delta"] =
                                resources_after.delta_json(resources_before);
                            match owned.shutdown_with_evidence() {
                                Ok(evidence) => {
                                    let groups = evidence.write_completion_groups;
                                    point["write_completion_groups_by_size"] =
                                        json!(groups.to_vec());
                                    point["histogram_scope"] = json!("per_level");
                                    point["server_stage_evidence"] =
                                        riffdb_shutdown_evidence_json(&evidence);
                                    json_report["write_completion_groups_by_size"] =
                                        json!(groups.to_vec());
                                    json_report["histogram_scope"] = json!("per_level");
                                    json_report["server_stage_evidence"] =
                                        riffdb_shutdown_evidence_json(&evidence);
                                }
                                Err(error) => {
                                    eprintln!("riffdbd shutdown diagnostic: {error}");
                                    json_report["server_shutdown_error"] = json!(error.to_string());
                                    deferred_failure = Some(error.to_string());
                                }
                            }
                            curve.push(point);
                            reports.push(json_report);
                        }
                        Err(error) => {
                            let _ = owned.shutdown();
                            return Err(error);
                        }
                    }
                }
            } else {
                let session = runtime
                    .block_on(RiffDbServerSession::start_with_options(
                        &riffdbd,
                        start_options.clone(),
                    ))
                    .map_err(|error| error.to_string())?;
                let session = Arc::new(std::sync::Mutex::new(session));
                let seed_ns;
                {
                    let mut guard = session.lock().map_err(|_| "session lock".to_owned())?;
                    guard.backend.reset().map_err(|error| error.to_string())?;
                    let seed_started = Instant::now();
                    if let Err(error) = guard.backend.seed(&dataset) {
                        drop(guard);
                        if let Ok(owned) = Arc::try_unwrap(session) {
                            let _ = owned.into_inner().map(|s| s.shutdown());
                        }
                        return Err(error.to_string());
                    }
                    seed_ns = u64::try_from(seed_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    if args.load_journeys {
                        let mut journey = run_stateful_journeys(
                            &mut guard.backend,
                            &dataset,
                            (rep as u64).saturating_mul(1_000_000),
                        )?;
                        journey["backend_id"] = json!("riffdb_public_grpc");
                        journey["rep"] = json!(rep);
                        journey["sweep_scope"] = json!("shared_history");
                        journey_reports.push(journey);
                    }
                }
                let prototype = {
                    let guard = session.lock().map_err(|_| "session lock".to_owned())?;
                    guard.backend.clone().with_command_attempt_budget(1)
                };
                for (point_index, &clients) in client_points.iter().enumerate() {
                    let mut config = base_config.clone().with_client_cap(if args.load_saturate {
                        RIFFDB_SATURATE_LOAD_CLIENTS
                    } else {
                        RIFFDB_MAX_LOAD_CLIENTS
                    });
                    config.clients = clients.clamp(
                        1,
                        if args.load_saturate {
                            RIFFDB_SATURATE_LOAD_CLIENTS
                        } else {
                            RIFFDB_MAX_LOAD_CLIENTS
                        },
                    );
                    config.sample_id_base = (point_index as u64)
                        .saturating_mul(SAMPLE_GAP)
                        .saturating_add((rep as u64).saturating_mul(SAMPLE_GAP / 16));
                    let prototype = prototype.clone();
                    let resources_before = {
                        let guard = session.lock().map_err(|_| "session lock".to_owned())?;
                        process_resource_snapshot(guard.child_pid(), guard.session_directory())?
                    };
                    let reconciliation_sample =
                        config.sample_id_base.saturating_add(SAMPLE_GAP / 32);
                    let report = if config.open_loop_rate_per_sec.is_some() {
                        run_open_loop_load(
                            "riffdb_public_grpc",
                            config,
                            LoadExecutionShape {
                                transport_topology: transport_topology.as_report_str(),
                                command_attempt_budget: 1,
                            },
                            &dataset,
                            seed_ns,
                            move || {
                                let mut backend = match transport_topology {
                                    RiffDbTransport::PerSession => prototype.fresh_session(),
                                    RiffDbTransport::Shared => Ok(prototype.clone()),
                                }
                                .map_err(|error| error.to_string())?;
                                backend.prewarm().map_err(|error| error.to_string())?;
                                Ok(backend)
                            },
                        )
                    } else {
                        let abort = {
                            let session = Arc::clone(&session);
                            Arc::new(move || {
                                let mut guard = session.lock().ok()?;
                                guard.server_alive().err()
                            })
                                as Arc<dyn Fn() -> Option<String> + Send + Sync>
                        };
                        run_closed_loop_load_with_abort(
                            "riffdb_public_grpc",
                            config,
                            LoadExecutionShape {
                                transport_topology: transport_topology.as_report_str(),
                                command_attempt_budget: 1,
                            },
                            &dataset,
                            seed_ns,
                            move || {
                                let mut backend = match transport_topology {
                                    RiffDbTransport::PerSession => prototype.fresh_session(),
                                    RiffDbTransport::Shared => Ok(prototype.clone()),
                                }
                                .map_err(|error| error.to_string())?;
                                backend.prewarm().map_err(|error| error.to_string())?;
                                Ok(backend)
                            },
                            Some(abort),
                        )
                    };
                    if report.is_ok() && args.load_journeys {
                        let mut guard = session.lock().map_err(|_| "session lock".to_owned())?;
                        let mut journey = run_stateful_journeys(
                            &mut guard.backend,
                            &dataset,
                            reconciliation_sample,
                        )?;
                        journey["backend_id"] = json!("riffdb_public_grpc");
                        journey["rep"] = json!(rep);
                        journey["clients"] = json!(clients);
                        journey["phase"] = json!("post_load_reconciliation_canary");
                        journey_reports.push(journey);
                    }
                    let resources_after = {
                        let guard = session.lock().map_err(|_| "session lock".to_owned())?;
                        process_resource_snapshot(guard.child_pid(), guard.session_directory())
                    };
                    match report {
                        Ok(report) => {
                            let resources_after = resources_after?;
                            print_load_summary(&report);
                            let mut point = concurrency_curve_point(&report);
                            point["rep"] = json!(rep);
                            point["sweep_isolation"] = json!(sweep_isolation);
                            point["resource_delta"] = resources_after.delta_json(resources_before);
                            curve.push(point);
                            let mut json_report = report.to_json();
                            json_report["rep"] = json!(rep);
                            json_report["sweep_isolation"] = json!(sweep_isolation);
                            json_report["resource_delta"] =
                                resources_after.delta_json(resources_before);
                            reports.push(json_report);
                        }
                        Err(error) => {
                            if let Ok(owned) = Arc::try_unwrap(session) {
                                let _ = owned.into_inner().map(|s| s.shutdown());
                            }
                            return Err(error);
                        }
                    }
                }
                let owned = Arc::try_unwrap(session)
                    .map_err(|_| "session still shared after load".to_owned())?
                    .into_inner()
                    .map_err(|_| "session mutex poisoned".to_owned())?;
                match owned.shutdown_with_evidence() {
                    Ok(evidence) => {
                        let groups = evidence.write_completion_groups;
                        if let Some(last) = reports.iter_mut().rev().find(|report| {
                            report["backend_id"].as_str() == Some("riffdb_public_grpc")
                        }) {
                            last["write_completion_groups_by_size"] = json!(groups.to_vec());
                            last["histogram_scope"] = json!("cumulative_final");
                            last["server_stage_evidence"] =
                                riffdb_shutdown_evidence_json(&evidence);
                        }
                        if let Some(last) = curve.iter_mut().rev().find(|report| {
                            report["backend_id"].as_str() == Some("riffdb_public_grpc")
                        }) {
                            last["write_completion_groups_by_size"] = json!(groups.to_vec());
                            last["histogram_scope"] = json!("cumulative_final");
                            last["server_stage_evidence"] =
                                riffdb_shutdown_evidence_json(&evidence);
                        }
                    }
                    Err(error) => {
                        eprintln!("riffdbd shutdown diagnostic: {error}");
                        if let Some(last) = reports.iter_mut().rev().find(|report| {
                            report["backend_id"].as_str() == Some("riffdb_public_grpc")
                        }) {
                            last["server_shutdown_error"] = json!(error.to_string());
                        }
                        deferred_failure = Some(error.to_string());
                    }
                }
            }
        }
    }

    if non_evidentiary {
        for report in &mut reports {
            report["non_evidentiary_window"] = json!(true);
            report["non_evidentiary_note"] =
                json!("load duration < 60s; results are smoke/debug only, not evidentiary");
        }
    }
    for report in &mut reports {
        report["reps_requested"] = json!(args.reps);
    }

    if reports.is_empty() {
        return Err("no backends selected".to_owned());
    }
    if args.load_concurrency_sweep {
        print_concurrency_sweep_summary(&curve);
    }
    let correctness_failures = reports
        .iter()
        .filter_map(|report| {
            let backend = report["backend_id"].as_str()?;
            let clients = report["clients"].as_u64().unwrap_or(0);
            let label = if args.load_concurrency_sweep {
                format!("{backend}@c={clients}")
            } else {
                backend.to_owned()
            };
            let unavailable = report["aggregate"]["outcomes"]["unavailable"]
                .as_u64()
                .unwrap_or(0);
            let idempotency_mismatch = report["aggregate"]["outcomes"]["idempotency_mismatch"]
                .as_u64()
                .unwrap_or(0);
            let error = report["aggregate"]["outcomes"]["error"]
                .as_u64()
                .unwrap_or(0);
            let mut parts = Vec::new();
            if args.load_saturate && backend.starts_with("riffdb") {
                let overloaded = report["aggregate"]["outcomes"]["overloaded"]
                    .as_u64()
                    .unwrap_or(0);
                let conflict = report["aggregate"]["outcomes"]["conflict"]
                    .as_u64()
                    .unwrap_or(0);
                let history = report["aggregate"]["outcomes"]["history_incarnation_mismatch"]
                    .as_u64()
                    .unwrap_or(0);
                let non_success = conflict
                    .saturating_add(unavailable)
                    .saturating_add(error)
                    .saturating_add(idempotency_mismatch)
                    .saturating_add(history)
                    .saturating_add(overloaded);
                if overloaded == 0 {
                    parts.push(
                        "saturate profile observed zero Overloaded outcomes (profile broken)"
                            .to_owned(),
                    );
                }
                if non_success > 0 && overloaded != non_success {
                    parts.push(format!(
                        "saturate profile requires 100% non-success outcomes to be Overloaded \
                         (overloaded={overloaded}, non_success={non_success})"
                    ));
                }
                // Success-only p99 (ADR: accepted-work latency bounded).
                if let Some(p99_ns) = report["aggregate"]["success_latency"]["p99_ns"].as_u64() {
                    let ceiling_ns = u64::try_from(base_config.saturate_p99_ceiling.as_nanos())
                        .unwrap_or(u64::MAX);
                    let success = report["aggregate"]["outcomes"]["success"]
                        .as_u64()
                        .unwrap_or(0);
                    if success > 0 && p99_ns > ceiling_ns {
                        parts.push(format!(
                            "saturate success-only p99 {p99_ns}ns exceeds ceiling {ceiling_ns}ns"
                        ));
                    }
                }
            } else if unavailable > 0 || idempotency_mismatch > 0 || error > 0 {
                parts.push(format!(
                    "unavailable={unavailable}, \
                     idempotency_mismatch={idempotency_mismatch}, error={error}"
                ));
            }
            if args.load_contended {
                // Contended profile requires every weighted op to land at least
                // one success against the shared hot ticket.
                if let Some(by_op) = report["operations"].as_object() {
                    for (op, stats) in by_op {
                        let success = stats["outcomes"]["success"].as_u64().unwrap_or(0);
                        if success == 0 {
                            parts.push(format!("op {op} has zero successes"));
                        }
                    }
                }
            }
            (!parts.is_empty()).then(|| format!("{label}: {}", parts.join("; ")))
        })
        .collect::<Vec<_>>();
    let mut suite = json!({
        "schema": "riffdb.app-baseline-load-suite/v1",
        "scale": {
            "profile": if args.scale.approximate_row_count() < 1_000 { "smoke" } else { "full" },
            "approximate_row_count": args.scale.approximate_row_count(),
            "organizations": args.scale.organizations,
            "projects_per_org": args.scale.projects_per_org,
            "tickets_per_project": args.scale.tickets_per_project,
            "comments_per_ticket": args.scale.comments_per_ticket,
            "labels_per_ticket": args.scale.labels_per_ticket,
            "board_dense_open": args.scale.board_dense_open,
            "payload_bytes": args.scale.payload_bytes,
            "comment_payload_bytes": args.scale.payload_bytes,
            "ticket_title_payload_bytes": args.scale.payload_bytes.min(128),
        },
        "comparison": {
            "requested_clients": if args.load_concurrency_sweep {
                serde_json::Value::Null
            } else {
                json!(base_config.clients)
            },
            "concurrency_sweep": args.load_concurrency_sweep,
            "client_points": client_points,
            "riffdb_transport": args.riffdb_transport.as_report_str(),
            "postgres_comparator": args.postgres_comparator.backend_id(),
            "automatic_command_retries": false,
            "contended": args.load_contended,
            "saturate": args.load_saturate,
            "saturate_p99_ceiling_ms": args.load_saturate_p99_ms,
            "tenant_count": base_config.tenant_count,
            "hot_tenant_percent": base_config.hot_tenant_percent,
            "history_mode": sweep_isolation,
            "notes": if args.load_concurrency_sweep {
                if args.load_sweep_per_level_daemon {
                    json!([
                        "Concurrency sweep: same workload mix, warmup, and measure window at each client_points entry.",
                        "Every point starts from an identically generated fresh seed and process/storage state.",
                        "Curve is the defensible scaling evidence (throughput and latency vs clients), not a two-point inference."
                    ])
                } else {
                    json!([
                        "Concurrency sweep: same workload mix, warmup, and measure window at each client_points entry.",
                        "Explicit history-growth mode: points run low→high against one daemon and later points include earlier writes.",
                        "Do not interpret this curve as concurrency-only scaling evidence."
                    ])
                }
            } else {
                json!([])
            },
        },
        "query_shape_tiers": {
            "board_page_sizes": [50, 200, 450],
            "comment_page_limit": 50,
            "cursor_depth": "first_page_in_load; cursor resume is covered by generated-client conformance",
            "bounded_shape_note": "RiffQL v1 application pages use deployed static bounds; unsupported arbitrary depths are not silently simulated by the harness."
        },
        "curve": curve,
        "correctness": {
            "clean": correctness_failures.is_empty() && deferred_failure.is_none(),
            "failures": &correctness_failures,
            "server_shutdown_error": deferred_failure.as_ref(),
        },
        "stateful_journeys": journey_reports,
        "backends": reports,
    });
    attach_load_evidence(&mut suite, &evidence, &curve, args.reps, non_evidentiary)?;
    if args.require_stable {
        require_load_stable(&suite)?;
    }
    let encoded = serde_json::to_string_pretty(&suite).map_err(|error| error.to_string())?;
    if let Some(path) = &args.output {
        fs::write(path, format!("{encoded}\n")).map_err(|error| error.to_string())?;
        println!("wrote {}", path.display());
    } else {
        println!("{encoded}");
    }
    if !correctness_failures.is_empty() {
        return Err(format!(
            "load completed with public correctness failures: {}",
            correctness_failures.join("; ")
        ));
    }
    if let Some(detail) = deferred_failure {
        return Err(detail);
    }
    Ok(())
}

/// Server-side proof that a Postgres load point/phase is finished.
///
/// Joining worker threads is necessary but not sufficient: if a client is still
/// connected or executing SQL, measurement boundaries and sequential isolation
/// are lies. Fail closed with a `pg_stat_activity` sample.
fn assert_postgres_load_quiesced(
    control: &mut PostgresAppBackend,
    clients: usize,
    rep: usize,
) -> Result<(), String> {
    // Long tail: a single stuck multi-entity write under c=128 can take seconds;
    // 30s is well above expected TCP close after join.
    const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);
    match control.wait_until_load_clients_gone(QUIESCE_TIMEOUT) {
        Ok(()) => {
            if clients == 0 && rep == usize::MAX {
                eprintln!(
                    "postgres-quiesce: phase gate ok (no foreign client backends on database)"
                );
            } else {
                eprintln!(
                    "postgres-quiesce: point ok rep={rep} clients={clients} \
                     (no foreign client backends on database)"
                );
            }
            Ok(())
        }
        Err(error) => Err(format!(
            "measurement-integrity: PostgreSQL load claimed complete but server still has \
             client activity (rep={rep} clients={clients}): {error}"
        )),
    }
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

fn gated_ratio(value: &serde_json::Value) -> Result<f64, String> {
    if let Some(object) = value.as_object() {
        if object
            .get("stability")
            .and_then(|s| s.as_str())
            .is_some_and(|s| s == "unstable")
        {
            return Err(
                "gated metric is unstable (spread_ratio > 2.0); refusing parity pass".to_owned(),
            );
        }
        return object
            .get("median")
            .and_then(|v| v.as_f64())
            .or_else(|| value.as_f64())
            .ok_or_else(|| "gated ratio missing median".to_owned());
    }
    value
        .as_f64()
        .ok_or_else(|| "gated ratio is not a number".to_owned())
}

fn refuse_if_postgres_ram_backed(report: &serde_json::Value) -> Result<(), String> {
    if let Some(medium) = report.get("postgres_storage_medium")
        && medium["kind"].as_str() == Some("ram_backed")
    {
        return Err(format!(
            "parity refused: PostgreSQL data medium is RAM-backed ({medium})"
        ));
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
    refuse_if_postgres_ram_backed(report)?;
    if let Some(settings) = report.get("postgres_durability") {
        for key in ["synchronous_commit", "fsync", "full_page_writes"] {
            let value = settings[key].as_str().unwrap_or("");
            if value != "on" {
                return Err(format!(
                    "write-parity refused: PostgreSQL {key}={value:?} (must be on)"
                ));
            }
        }
    }
    let seed_ratio = gated_ratio(&report["comparisons"]["seed"]["ratio_riffdb_over_postgres"])
        .map_err(|error| format!("write-parity seed gate: {error}"))?;
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
        let ratio = gated_ratio(&row["ratio_riffdb_over_postgres"])
            .map_err(|error| format!("write-parity {expected}: {error}"))?;
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
    refuse_if_postgres_ram_backed(report)?;
    if let Some(settings) = report.get("postgres_durability") {
        for key in ["synchronous_commit", "fsync", "full_page_writes"] {
            let value = settings[key].as_str().unwrap_or("");
            if value != "on" {
                return Err(format!(
                    "all-parity refused: PostgreSQL {key}={value:?} (must be on)"
                ));
            }
        }
    }
    let seed_ratio = gated_ratio(&report["comparisons"]["seed"]["ratio_riffdb_over_postgres"])
        .map_err(|error| format!("all-parity seed gate: {error}"))?;
    if !seed_ratio.is_finite() || seed_ratio > MAX_RATIO {
        return Err(format!(
            "all-parity seed gate failed: RiffDB/PostgreSQL is {seed_ratio:.2}x, limit is {MAX_RATIO:.2}x"
        ));
    }
    let scenarios = report["comparisons"]["scenarios"]
        .as_array()
        .ok_or("all-parity assertion is missing scenario ratios")?;
    // Gate every *measured* scenario present in the report. Smoke omits
    // board_page_* (board_dense_open=0); full includes them. An empty
    // measured set is refuse, not a free pass.
    if scenarios.is_empty() {
        return Err("all-parity assertion is missing scenario ratios".to_owned());
    }
    for row in scenarios {
        let expected = row["scenario"]
            .as_str()
            .ok_or("all-parity scenario row missing name")?;
        let ratio = gated_ratio(&row["ratio_riffdb_over_postgres"])
            .map_err(|error| format!("all-parity {expected}: {error}"))?;
        if !ratio.is_finite() || ratio > MAX_RATIO {
            return Err(format!(
                "all-parity scenario gate failed for {expected}: RiffDB/PostgreSQL is {ratio:.2}x, limit is {MAX_RATIO:.2}x"
            ));
        }
    }
    println!(
        "all-parity gate passed: seed and every measured p50 ratio ({} scenarios) are <= {MAX_RATIO:.2}x",
        scenarios.len()
    );
    Ok(())
}

fn require_stable(report: &serde_json::Value) -> Result<(), String> {
    // Single-rep stability is trivially "stable" (spread undefined/1.0); refuse
    // rather than pass a meaningless gate.
    let reps = report["reps"].as_u64().unwrap_or(1);
    if reps < 2 {
        return Err(
            "--require-stable requires --reps >= 2 (single-rep stability is trivial)".to_owned(),
        );
    }
    let mut unstable = Vec::new();
    if let Some(summaries) = report["comparisons"]["rep_summaries"].as_object() {
        for (name, summary) in summaries {
            if summary["stability"].as_str() == Some("unstable") {
                unstable.push(name.clone());
            }
        }
    }
    // Also walk scenario ratio fields directly (gated field is the summary object).
    if let Some(scenarios) = report["comparisons"]["scenarios"].as_array() {
        for row in scenarios {
            let name = row["scenario"].as_str().unwrap_or("?");
            if row["ratio_riffdb_over_postgres"]["stability"].as_str() == Some("unstable") {
                let key = format!("scenario:{name}");
                if !unstable.contains(&key) {
                    unstable.push(key);
                }
            }
        }
    }
    if report["comparisons"]["seed"]["ratio_riffdb_over_postgres"]["stability"].as_str()
        == Some("unstable")
    {
        let key = "seed_ratio".to_owned();
        if !unstable.contains(&key) {
            unstable.push(key);
        }
    }
    if unstable.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "--require-stable failed; unstable gated metrics: {}",
            unstable.join(", ")
        ))
    }
}

fn median_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

fn scalar_summary(values: &[f64]) -> serde_json::Value {
    if values.is_empty() {
        return json!({
            "median": 0.0,
            "min": 0.0,
            "max": 0.0,
            "reps": 0,
            "spread_ratio": 0.0,
            "stability": "stable",
        });
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    let median = sorted[sorted.len() / 2];
    let spread_ratio = if min <= 0.0 { f64::INFINITY } else { max / min };
    let stability = if spread_ratio > 2.0 {
        "unstable"
    } else {
        "stable"
    };
    json!({
        "median": median,
        "min": min,
        "max": max,
        "reps": values.len(),
        "spread_ratio": spread_ratio,
        "stability": stability,
    })
}

/// Per scenario, select the median-ranking rep by p50 and carry that rep's
/// **real** [`SampleSet`] (genuine percentiles). Never synthesizes a single-sample set.
///
/// Cross-rep median p50 for ratio gates lives in `attach_rep_summaries` objects,
/// not in fabricated timing samples.
fn median_scenarios(
    reps: &[Vec<riffdb_app_baseline_core::ScenarioResult>],
) -> Vec<riffdb_app_baseline_core::ScenarioResult> {
    if reps.is_empty() {
        return Vec::new();
    }
    // --reps 1: pass through the only real distribution unchanged.
    if reps.len() == 1 {
        return reps[0].clone();
    }
    let template = &reps[0];
    template
        .iter()
        .map(|proto| {
            let mut ranked: Vec<(u64, usize)> = Vec::with_capacity(reps.len());
            for (rep_index, rep) in reps.iter().enumerate() {
                if let Some(row) = rep.iter().find(|r| r.scenario == proto.scenario) {
                    ranked.push((row.samples.summary().p50_ns, rep_index));
                }
            }
            if ranked.is_empty() {
                return proto.clone();
            }
            ranked.sort_by_key(|(p50, _)| *p50);
            let median_rep = ranked[ranked.len() / 2].1;
            reps[median_rep]
                .iter()
                .find(|r| r.scenario == proto.scenario)
                .cloned()
                .unwrap_or_else(|| proto.clone())
        })
        .collect()
}

fn attach_rep_summaries(
    report: &mut serde_json::Value,
    pg_seed: &[u64],
    pg_scenarios: &[Vec<riffdb_app_baseline_core::ScenarioResult>],
    rd_seed: &[u64],
    rd_scenarios: &[Vec<riffdb_app_baseline_core::ScenarioResult>],
) {
    if pg_seed.is_empty() || rd_seed.is_empty() {
        return;
    }
    let mut rep_summaries = serde_json::Map::new();

    let mut seed_ratios = Vec::new();
    for (pg, rd) in pg_seed.iter().zip(rd_seed.iter()) {
        let ratio = if *pg == 0 {
            f64::INFINITY
        } else {
            *rd as f64 / *pg as f64
        };
        seed_ratios.push(ratio);
    }
    let seed_summary = scalar_summary(&seed_ratios);
    // Gated field is the full summary object so stability is visible to gates.
    report["comparisons"]["seed"]["ratio_riffdb_over_postgres"] = seed_summary.clone();
    report["comparisons"]["seed"]["ratio_riffdb_over_postgres_median"] =
        seed_summary["median"].clone();
    rep_summaries.insert("seed_ratio".to_owned(), seed_summary);

    // Per-scenario ratio summaries across matching reps.
    if !pg_scenarios.is_empty() && !rd_scenarios.is_empty() {
        let scenario_ids: Vec<_> = pg_scenarios[0]
            .iter()
            .map(|s| s.scenario.as_str().to_owned())
            .collect();
        if let Some(scenarios_json) = report["comparisons"]["scenarios"].as_array_mut() {
            for name in scenario_ids {
                let mut ratios = Vec::new();
                let n = pg_scenarios.len().min(rd_scenarios.len());
                for i in 0..n {
                    let pg_p50 = pg_scenarios[i]
                        .iter()
                        .find(|s| s.scenario.as_str() == name)
                        .map(|s| s.samples.summary().p50_ns)
                        .unwrap_or(0);
                    let rd_p50 = rd_scenarios[i]
                        .iter()
                        .find(|s| s.scenario.as_str() == name)
                        .map(|s| s.samples.summary().p50_ns)
                        .unwrap_or(0);
                    let ratio = if pg_p50 == 0 {
                        f64::INFINITY
                    } else {
                        rd_p50 as f64 / pg_p50 as f64
                    };
                    ratios.push(ratio);
                }
                let summary = scalar_summary(&ratios);
                if let Some(row) = scenarios_json
                    .iter_mut()
                    .find(|row| row["scenario"].as_str() == Some(name.as_str()))
                {
                    row["ratio_riffdb_over_postgres"] = summary.clone();
                    row["ratio_riffdb_over_postgres_median"] = summary["median"].clone();
                }
                rep_summaries.insert(format!("scenario:{name}"), summary);
            }
        }
    }

    // Board marginal cost is the package headline: compute per rep and emit a
    // gated-style stability summary (median/min/max/spread_ratio).
    let n = pg_scenarios.len().min(rd_scenarios.len());
    if n > 0 {
        let mut pg_marginals = Vec::new();
        let mut rd_marginals = Vec::new();
        for i in 0..n {
            if let Some(m) = board_marginal_from_results(&pg_scenarios[i]) {
                pg_marginals.push(m as f64);
            }
            if let Some(m) = board_marginal_from_results(&rd_scenarios[i]) {
                rd_marginals.push(m as f64);
            }
        }
        if !pg_marginals.is_empty() && !rd_marginals.is_empty() {
            let pg_summary = scalar_summary(&pg_marginals);
            let rd_summary = scalar_summary(&rd_marginals);
            report["comparisons"]["board_marginal_ns_per_row"] = json!({
                "postgres": pg_summary.clone(),
                "riffdb": rd_summary.clone(),
            });
            // Mirror onto backend objects when present.
            if let Some(backends) = report["backends"].as_array_mut() {
                for backend in backends {
                    match backend["backend_id"].as_str() {
                        Some(id) if id.starts_with("postgres_") => {
                            backend["board_marginal_ns_per_row"] = pg_summary.clone();
                        }
                        Some("riffdb_public_grpc") => {
                            backend["board_marginal_ns_per_row"] = rd_summary.clone();
                        }
                        _ => {}
                    }
                }
            }
            rep_summaries.insert("board_marginal:postgres".to_owned(), pg_summary);
            rep_summaries.insert("board_marginal:riffdb".to_owned(), rd_summary);
        }
    }

    report["comparisons"]["rep_summaries"] = serde_json::Value::Object(rep_summaries);
}

fn device_baseline_value(baseline: &DeviceBaseline) -> serde_json::Value {
    json!({
        "fdatasync_p50_us": baseline.fdatasync_p50_us,
        "fdatasync_p99_us": baseline.fdatasync_p99_us,
        "fsyncs_per_s": baseline.fsyncs_per_s,
        "sequential_write_mib_s": baseline.sequential_write_mib_s,
    })
}

fn print_summary(report: &serde_json::Value) {
    println!("\n== TicketDesk app baseline summary ==");
    let seed_gen = report["configuration"]["seed_generation"]
        .as_u64()
        .unwrap_or(u64::from(SEED_GENERATION));
    println!(
        "seed_generation={seed_gen} (pre-B1 full baselines superseded; do not compare across generations)"
    );
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
                    let rows = scenario["last_row_count"].as_u64().unwrap_or(0);
                    println!("  {name}: p50={p50:.3}ms rows={rows}");
                }
            }
            print_board_marginal_line(
                "  board_marginal_ns_per_row",
                &backend["board_marginal_ns_per_row"],
            );
        }
    }
    if report["comparisons"]["available"] == true {
        println!("\nratios (riffdb/postgres p50):");
        if let Some(rows) = report["comparisons"]["scenarios"].as_array() {
            for row in rows {
                let name = row["scenario"].as_str().unwrap_or("?");
                let ratio = gated_ratio(&row["ratio_riffdb_over_postgres"]).unwrap_or(0.0);
                println!("  {name}: {ratio:.2}x");
            }
        }
        if let Some(obj) = report["comparisons"]["board_marginal_ns_per_row"].as_object() {
            print_board_marginal_line(
                "board marginal ns/row postgres",
                obj.get("postgres").unwrap_or(&json!(null)),
            );
            print_board_marginal_line(
                "board marginal ns/row riffdb",
                obj.get("riffdb").unwrap_or(&json!(null)),
            );
        }
    }
}

fn print_board_marginal_line(label: &str, value: &serde_json::Value) {
    if let Some(median) = value.as_u64() {
        println!("{label}={median}  ( (p50_450 − p50_50) / 400 )");
        return;
    }
    if let Some(median) = value["median"].as_f64() {
        let min = value["min"].as_f64().unwrap_or(median);
        let max = value["max"].as_f64().unwrap_or(median);
        let spread = value["spread_ratio"].as_f64().unwrap_or(1.0);
        let stability = value["stability"].as_str().unwrap_or("?");
        println!(
            "{label}: median={median:.0} min={min:.0} max={max:.0} spread_ratio={spread:.2} ({stability})"
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RiffDbTransport {
    PerSession,
    Shared,
}

impl RiffDbTransport {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "per-session" => Some(Self::PerSession),
            "shared" => Some(Self::Shared),
            _ => None,
        }
    }

    const fn as_report_str(self) -> &'static str {
        match self {
            Self::PerSession => "per_session_http2_connection",
            Self::Shared => "shared_http2_connection",
        }
    }
}

struct Args {
    scale: Scale,
    samples: usize,
    warmups: usize,
    postgres_url: Option<String>,
    postgres_comparator: PostgresComparisonProfile,
    riffdbd_bin: Option<PathBuf>,
    output: Option<PathBuf>,
    skip_postgres: bool,
    skip_riffdb: bool,
    assert_write_parity: bool,
    assert_all_parity: bool,
    concurrent_clients: Option<usize>,
    concurrent_operations: usize,
    load_profile: Option<WorkloadProfile>,
    load_clients: Option<usize>,
    load_duration_secs: Option<u64>,
    load_warmup_secs: Option<u64>,
    load_zipf_s: Option<f64>,
    load_open_loop_rate: Option<u64>,
    load_open_loop_queue_depth: usize,
    load_tenants: usize,
    load_hot_tenant_percent: u8,
    load_journeys: bool,
    load_contended: bool,
    load_saturate: bool,
    load_saturate_p99_ms: u64,
    /// Same mix at client points 1/8/32/128 (curve evidence).
    load_concurrency_sweep: bool,
    /// Fresh daemon per sweep client point (identical retained history each level).
    load_sweep_per_level_daemon: bool,
    riffdb_transport: RiffDbTransport,
    /// On-disk root for riffdbd session DBs (default `target/perf-db/app-baseline`).
    database_root: Option<PathBuf>,
    /// Host path bind-mounted as PostgreSQL's data directory (same-device check).
    postgres_data_host_path: Option<PathBuf>,
    /// Permit tmpfs/ramfs roots (tests only).
    allow_tmpfs: bool,
    /// Independent measurement repetitions (default 3 for --full, 1 for smoke).
    reps: usize,
    /// Exit nonzero when any gated metric is unstable (spread_ratio > 2.0).
    require_stable: bool,
}

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut scale = Scale::smoke();
        let mut samples = 3;
        let mut warmups = 1;
        let mut postgres_url = None;
        let mut postgres_comparator = PostgresComparisonProfile::Minimal;
        let mut riffdbd_bin = None;
        let mut output = None;
        let mut skip_postgres = false;
        let mut skip_riffdb = false;
        let mut assert_write_parity = false;
        let mut assert_all_parity = false;
        let mut concurrent_clients = None;
        let mut concurrent_operations = 200;
        let mut load_profile = None;
        let mut load_clients = None;
        let mut load_duration_secs = None;
        let mut load_warmup_secs = None;
        let mut load_zipf_s = None;
        let mut load_open_loop_rate = None;
        let mut load_open_loop_queue_depth = 4_096;
        let mut load_tenants = 1;
        let mut load_hot_tenant_percent = 0;
        let mut load_journeys = false;
        let mut load_contended = false;
        let mut load_saturate = false;
        let mut load_saturate_p99_ms = 250;
        let mut load_concurrency_sweep = false;
        // Concurrency comparisons must start from identical history. Accumulated
        // history remains an explicit growth experiment, never the default.
        let mut load_sweep_per_level_daemon = true;
        let mut riffdb_transport = RiffDbTransport::PerSession;
        let mut database_root = None;
        let mut postgres_data_host_path = None;
        let mut allow_tmpfs = false;
        let mut reps: Option<usize> = None;
        let mut require_stable = false;
        let mut full = false;
        let mut board_density: Option<u32> = None;
        let mut organizations: Option<u32> = None;
        let mut tickets_per_project: Option<u32> = None;
        let mut comments_per_ticket: Option<u32> = None;
        let mut labels_per_ticket: Option<u32> = None;
        let mut payload_bytes: Option<u32> = None;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--smoke" => scale = Scale::smoke(),
                "--full" => {
                    scale = Scale::full();
                    samples = 9;
                    warmups = 2;
                    full = true;
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
                "--postgres-comparator" => {
                    let value = args.next().ok_or("--postgres-comparator needs a value")?;
                    postgres_comparator =
                        PostgresComparisonProfile::parse(&value).ok_or_else(|| {
                            "--postgres-comparator must be minimal or safe-app".to_owned()
                        })?;
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
                "--load" => {
                    let name = args.next().ok_or("--load needs a profile name")?;
                    load_profile = Some(WorkloadProfile::parse(&name).ok_or_else(|| {
                        format!(
                            "unknown load profile '{name}' (interactive|agent|membership_contention)"
                        )
                    })?);
                }
                "--load-clients" => {
                    load_clients = Some(
                        args.next()
                            .ok_or("--load-clients needs a value")?
                            .parse()
                            .map_err(|_| "--load-clients must be usize")?,
                    );
                }
                "--load-duration-secs" => {
                    load_duration_secs = Some(
                        args.next()
                            .ok_or("--load-duration-secs needs a value")?
                            .parse()
                            .map_err(|_| "--load-duration-secs must be u64")?,
                    );
                }
                "--load-warmup-secs" => {
                    load_warmup_secs = Some(
                        args.next()
                            .ok_or("--load-warmup-secs needs a value")?
                            .parse()
                            .map_err(|_| "--load-warmup-secs must be u64")?,
                    );
                }
                "--load-zipf-s" => {
                    load_zipf_s = Some(
                        args.next()
                            .ok_or("--load-zipf-s needs a value")?
                            .parse()
                            .map_err(|_| "--load-zipf-s must be f64")?,
                    );
                }
                "--load-open-loop-rate" => {
                    load_open_loop_rate = Some(
                        args.next()
                            .ok_or("--load-open-loop-rate needs a value")?
                            .parse()
                            .map_err(|_| "--load-open-loop-rate must be u64")?,
                    );
                }
                "--load-open-loop-queue-depth" => {
                    load_open_loop_queue_depth = args
                        .next()
                        .ok_or("--load-open-loop-queue-depth needs a value")?
                        .parse()
                        .map_err(|_| "--load-open-loop-queue-depth must be usize")?;
                }
                "--load-tenants" => {
                    load_tenants = args
                        .next()
                        .ok_or("--load-tenants needs a value")?
                        .parse()
                        .map_err(|_| "--load-tenants must be usize")?;
                }
                "--load-hot-tenant-percent" => {
                    load_hot_tenant_percent = args
                        .next()
                        .ok_or("--load-hot-tenant-percent needs a value")?
                        .parse()
                        .map_err(|_| "--load-hot-tenant-percent must be u8")?;
                }
                "--load-journeys" => load_journeys = true,
                "--load-contended" => load_contended = true,
                "--load-saturate" => load_saturate = true,
                "--load-concurrency-sweep" => load_concurrency_sweep = true,
                "--load-sweep-per-level-daemon" => load_sweep_per_level_daemon = true,
                "--load-accumulate-history" => load_sweep_per_level_daemon = false,
                "--load-saturate-p99-ms" => {
                    load_saturate_p99_ms = args
                        .next()
                        .ok_or("--load-saturate-p99-ms needs a value")?
                        .parse()
                        .map_err(|_| "--load-saturate-p99-ms must be u64")?;
                }
                "--load-riffdb-transport" => {
                    let value = args.next().ok_or("--load-riffdb-transport needs a value")?;
                    riffdb_transport = RiffDbTransport::parse(&value).ok_or_else(|| {
                        "--load-riffdb-transport must be per-session or shared".to_owned()
                    })?;
                }
                "--database-root" => {
                    database_root = Some(PathBuf::from(
                        args.next().ok_or("--database-root needs a value")?,
                    ));
                }
                "--postgres-data-host-path" => {
                    postgres_data_host_path = Some(PathBuf::from(
                        args.next()
                            .ok_or("--postgres-data-host-path needs a value")?,
                    ));
                }
                "--allow-tmpfs" => allow_tmpfs = true,
                "--reps" => {
                    reps = Some(
                        args.next()
                            .ok_or("--reps needs a value")?
                            .parse()
                            .map_err(|_| "--reps must be usize")?,
                    );
                }
                "--require-stable" => require_stable = true,
                "--board-density" => {
                    let value = args
                        .next()
                        .ok_or("--board-density needs a value")?
                        .parse::<u32>()
                        .map_err(|_| "--board-density must be u32")?;
                    if value > 10_000 {
                        return Err("--board-density must be <= 10000".to_owned());
                    }
                    board_density = Some(value);
                }
                "--organizations" => {
                    organizations = Some(
                        args.next()
                            .ok_or("--organizations needs a value")?
                            .parse()
                            .map_err(|_| "--organizations must be u32")?,
                    );
                }
                "--tickets-per-project" => {
                    tickets_per_project = Some(
                        args.next()
                            .ok_or("--tickets-per-project needs a value")?
                            .parse()
                            .map_err(|_| "--tickets-per-project must be u32")?,
                    );
                }
                "--comments-per-ticket" => {
                    comments_per_ticket = Some(
                        args.next()
                            .ok_or("--comments-per-ticket needs a value")?
                            .parse()
                            .map_err(|_| "--comments-per-ticket must be u32")?,
                    );
                }
                "--labels-per-ticket" => {
                    labels_per_ticket = Some(
                        args.next()
                            .ok_or("--labels-per-ticket needs a value")?
                            .parse()
                            .map_err(|_| "--labels-per-ticket must be u32")?,
                    );
                }
                "--payload-bytes" => {
                    payload_bytes = Some(
                        args.next()
                            .ok_or("--payload-bytes needs a value")?
                            .parse()
                            .map_err(|_| "--payload-bytes must be u32")?,
                    );
                }
                "--help" | "-h" => {
                    return Err(
                        "usage: riffdb-app-baseline [--smoke|--full] [--samples N] [--warmup N] \
                         [--board-density N] [--organizations N] [--tickets-per-project N] \
                         [--comments-per-ticket N] [--labels-per-ticket N] [--payload-bytes N] \
                         [--postgres-url URL] [--riffdbd-bin PATH] \
                         [--postgres-comparator minimal|safe-app] \
                         [--output PATH] [--assert-write-parity|--assert-all-parity] \
                         [--concurrent-clients N] [--concurrent-operations N] \
                         [--load interactive|agent|membership_contention] [--load-clients N] \
                         [--load-duration-secs N] [--load-warmup-secs N] [--load-zipf-s F] \
                         [--load-open-loop-rate OPS] [--load-open-loop-queue-depth N] \
                         [--load-tenants N] [--load-hot-tenant-percent PERCENT] \
                         [--load-journeys] \
                         [--load-contended] [--load-saturate] [--load-saturate-p99-ms N] \
                         [--load-concurrency-sweep] [--load-sweep-per-level-daemon|--load-accumulate-history] \
                         [--load-riffdb-transport per-session|shared] \
                         [--database-root PATH] [--postgres-data-host-path PATH] \
                         [--allow-tmpfs] [--reps N] [--require-stable] \
                         [--skip-postgres] [--skip-riffdb]"
                            .to_owned(),
                    );
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        if let Some(density) = board_density {
            scale.board_dense_open = density;
        }
        if let Some(value) = organizations {
            if !(1..=64).contains(&value) {
                return Err("--organizations must be 1..=64".to_owned());
            }
            scale.organizations = value;
        }
        if let Some(value) = tickets_per_project {
            if !(1..=1_000).contains(&value) {
                return Err("--tickets-per-project must be 1..=1000".to_owned());
            }
            scale.tickets_per_project = value;
        }
        if let Some(value) = comments_per_ticket {
            if value > 50 {
                return Err("--comments-per-ticket must be <= 50".to_owned());
            }
            scale.comments_per_ticket = value;
        }
        if let Some(value) = labels_per_ticket {
            if value == 0 || value > scale.labels_per_org {
                return Err(format!(
                    "--labels-per-ticket must be 1..={} for this scale",
                    scale.labels_per_org
                ));
            }
            scale.labels_per_ticket = value;
        }
        if let Some(value) = payload_bytes {
            if value > 200 {
                return Err("--payload-bytes must be <= 200".to_owned());
            }
            scale.payload_bytes = value;
        }
        let reps = reps.unwrap_or(if full { 3 } else { 1 });
        if !(1..=32).contains(&reps) {
            return Err("--reps must be 1..=32".to_owned());
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
        // Structural parser bound; run_load also checks live PostgreSQL capacity
        // and RiffDB's fixed session ceiling without silently reducing parity.
        // Saturate mode may raise clients well above coordinator depth 128.
        let client_ceiling = if load_saturate {
            RIFFDB_SATURATE_LOAD_CLIENTS
        } else {
            128
        };
        if load_clients.is_some_and(|clients| !(1..=client_ceiling).contains(&clients)) {
            return Err(format!("--load-clients must be 1..={client_ceiling}"));
        }
        if load_profile.is_none()
            && (load_clients.is_some()
                || load_duration_secs.is_some()
                || load_warmup_secs.is_some()
                || load_zipf_s.is_some()
                || load_open_loop_rate.is_some()
                || load_tenants != 1
                || load_hot_tenant_percent != 0
                || load_journeys
                || load_contended
                || load_saturate
                || load_concurrency_sweep
                || riffdb_transport != RiffDbTransport::PerSession)
        {
            return Err("--load-* options require --load".to_owned());
        }
        if load_concurrency_sweep && load_clients.is_some() {
            return Err(
                "--load-concurrency-sweep is incompatible with --load-clients (points are 1/8/32/128)"
                    .to_owned(),
            );
        }
        if load_concurrency_sweep && load_saturate {
            return Err(
                "--load-concurrency-sweep is for the ordinary interactive/agent mix, not --load-saturate"
                    .to_owned(),
            );
        }
        if load_open_loop_rate.is_some_and(|rate| rate == 0 || rate > 1_000_000) {
            return Err("--load-open-loop-rate must be 1..=1000000".to_owned());
        }
        if !(1..=65_536).contains(&load_open_loop_queue_depth) {
            return Err("--load-open-loop-queue-depth must be 1..=65536".to_owned());
        }
        if !(1..=64).contains(&load_tenants) {
            return Err("--load-tenants must be 1..=64".to_owned());
        }
        if load_tenants > scale.organizations as usize {
            return Err(format!(
                "--load-tenants {load_tenants} requires --organizations of at least {load_tenants}"
            ));
        }
        if load_hot_tenant_percent > 100 {
            return Err("--load-hot-tenant-percent must be <= 100".to_owned());
        }
        if load_open_loop_rate.is_some() && load_saturate {
            return Err("--load-open-loop-rate is incompatible with --load-saturate".to_owned());
        }
        if assert_write_parity && load_profile.is_some() {
            return Err(
                "--assert-write-parity is only for the parity suite, not --load".to_owned(),
            );
        }
        if assert_all_parity && load_profile.is_some() {
            return Err("--assert-all-parity is only for the parity suite, not --load".to_owned());
        }
        Ok(Self {
            scale,
            samples,
            warmups,
            postgres_url,
            postgres_comparator,
            riffdbd_bin,
            output,
            skip_postgres,
            skip_riffdb,
            assert_write_parity,
            assert_all_parity,
            concurrent_clients,
            concurrent_operations,
            load_profile,
            load_clients,
            load_duration_secs,
            load_warmup_secs,
            load_zipf_s,
            load_open_loop_rate,
            load_open_loop_queue_depth,
            load_tenants,
            load_hot_tenant_percent,
            load_journeys,
            load_contended,
            load_saturate,
            load_saturate_p99_ms,
            load_concurrency_sweep,
            load_sweep_per_level_daemon,
            riffdb_transport,
            database_root,
            postgres_data_host_path,
            allow_tmpfs,
            reps,
            require_stable,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        Args, RiffDbTransport, Scale, SeedDataset, WorkloadProfile, assert_all_parity,
        assert_write_parity, gated_ratio, load_rep_summaries, median_scenarios,
        require_load_stable, require_stable, scalar_summary,
    };

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
    fn load_flags_select_explicit_contention_and_transport_shape() {
        let args = Args::parse(
            [
                "--load",
                "agent",
                "--load-clients",
                "32",
                "--load-contended",
                "--load-saturate",
                "--load-riffdb-transport",
                "shared",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("load flags");
        assert_eq!(args.load_profile, Some(WorkloadProfile::Agent));
        assert_eq!(args.load_clients, Some(32));
        assert!(args.load_contended);
        assert!(args.load_saturate);
        assert_eq!(args.riffdb_transport, RiffDbTransport::Shared);
    }

    #[test]
    fn load_specific_flags_require_load_mode() {
        let error = Args::parse(["--load-contended"].into_iter().map(str::to_owned))
            .err()
            .expect("must reject");
        assert_eq!(error, "--load-* options require --load");
    }

    #[test]
    fn concurrency_sweep_rejects_load_clients() {
        let error = Args::parse(
            [
                "--load",
                "interactive",
                "--load-concurrency-sweep",
                "--load-clients",
                "16",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .err()
        .expect("must reject");
        assert!(error.contains("--load-concurrency-sweep"));
    }

    #[test]
    fn concurrency_sweep_flag_parses() {
        let args = Args::parse(
            ["--load", "interactive", "--load-concurrency-sweep"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect("sweep");
        assert!(args.load_concurrency_sweep);
    }

    #[test]
    fn tenant_and_open_loop_flags_require_seeded_partitions() {
        let args = Args::parse(
            [
                "--organizations",
                "8",
                "--load",
                "agent",
                "--load-tenants",
                "8",
                "--load-hot-tenant-percent",
                "70",
                "--load-open-loop-rate",
                "2000",
                "--load-open-loop-queue-depth",
                "4096",
                "--load-journeys",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("tenant open-loop flags");
        assert_eq!(args.load_tenants, 8);
        assert_eq!(args.load_hot_tenant_percent, 70);
        assert_eq!(args.load_open_loop_rate, Some(2_000));
        assert_eq!(args.load_open_loop_queue_depth, 4_096);
        assert!(args.load_journeys);

        let error = Args::parse(
            ["--load", "interactive", "--load-tenants", "8"]
                .into_iter()
                .map(str::to_owned),
        )
        .err()
        .expect("missing organizations must fail");
        assert!(error.contains("requires --organizations"));
    }

    #[test]
    fn process_resource_delta_is_saturating_and_redaction_safe() {
        let before = super::ProcessResourceSnapshot {
            cpu_ticks: 10,
            rss_bytes: 100,
            read_bytes: 30,
            write_bytes: 40,
            durable_bytes: 1_000,
        };
        let after = super::ProcessResourceSnapshot {
            cpu_ticks: 25,
            rss_bytes: 80,
            read_bytes: 50,
            write_bytes: 90,
            durable_bytes: 900,
        };
        let delta = after.delta_json(before);
        assert_eq!(delta["cpu_ticks"], 15);
        assert_eq!(delta["rss_bytes_peak_sampled"], 100);
        assert_eq!(delta["process_write_bytes"], 50);
        assert_eq!(delta["durable_bytes_growth"], 0);
        assert!(delta.get("path").is_none());
        assert!(delta.get("pid").is_none());
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
        // Over-threshold measured scenario fails.
        assert!(assert_all_parity(&parity_report(1.11, 1.10)).is_err());
        assert!(assert_all_parity(&parity_report(1.10, 1.11)).is_err());
        // Write-only measured set at the exact boundary passes (smoke-shaped).
        assert_all_parity(&parity_report(1.10, 1.10)).expect("write-only measured set at boundary");
        // Full measured set (all scenarios including board) at 1.10 passes.
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

    #[test]
    fn smoke_measured_set_passes_all_parity() {
        // --smoke measures 11 scenarios (board_page_* skipped). all-parity must
        // gate the measured set, not ScenarioId::all().
        let smoke = SeedDataset::generate(Scale::smoke());
        let measured = riffdb_app_baseline_core::ScenarioId::for_dataset(&smoke);
        assert_eq!(measured.len(), 11);
        assert!(!measured.iter().any(|id| id.board_page_limit().is_some()));
        let scenarios = measured
            .into_iter()
            .map(|scenario| {
                json!({
                    "scenario": scenario.as_str(),
                    "ratio_riffdb_over_postgres": 1.05,
                })
            })
            .collect::<Vec<_>>();
        let report = json!({
            "comparisons": {
                "available": true,
                "seed": {"ratio_riffdb_over_postgres": 1.05},
                "scenarios": scenarios,
            }
        });
        assert_all_parity(&report).expect("smoke measured set must pass all-parity");
    }

    #[test]
    fn integrity_flags_parse_and_default_reps() {
        let smoke = Args::parse(["--smoke".to_owned()].into_iter()).expect("smoke");
        assert_eq!(smoke.reps, 1);
        assert!(!smoke.allow_tmpfs);
        assert!(smoke.load_sweep_per_level_daemon);
        assert!(!smoke.require_stable);

        let full = Args::parse(["--full".to_owned()].into_iter()).expect("full");
        assert_eq!(full.reps, 3);

        let custom = Args::parse(
            [
                "--smoke".to_owned(),
                "--reps".to_owned(),
                "5".to_owned(),
                "--allow-tmpfs".to_owned(),
                "--require-stable".to_owned(),
                "--load".to_owned(),
                "interactive".to_owned(),
                "--load-sweep-per-level-daemon".to_owned(),
            ]
            .into_iter(),
        )
        .expect("custom");
        assert_eq!(custom.reps, 5);
        assert!(custom.allow_tmpfs);
        assert!(custom.require_stable);
        assert!(custom.load_sweep_per_level_daemon);

        let accumulated = Args::parse(
            ["--load", "interactive", "--load-accumulate-history"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect("explicit accumulated history");
        assert!(!accumulated.load_sweep_per_level_daemon);
    }

    #[test]
    fn load_rep_summaries_group_backend_and_client_and_detect_instability() {
        let curve = vec![
            json!({
                "backend_id": "riffdb_public_grpc",
                "clients": 32,
                "throughput_ops_s": 1000,
                "aggregate_p50_ns": 100,
                "aggregate_p95_ns": 200,
                "aggregate_p99_ns": 300,
            }),
            json!({
                "backend_id": "riffdb_public_grpc",
                "clients": 32,
                "throughput_ops_s": 4000,
                "aggregate_p50_ns": 110,
                "aggregate_p95_ns": 210,
                "aggregate_p99_ns": 310,
            }),
        ];
        let summaries = load_rep_summaries(&curve);
        let point = &summaries["riffdb_public_grpc@clients=32"];
        assert_eq!(point["throughput_ops_s"]["median"], 4000.0);
        assert_eq!(point["throughput_ops_s"]["stability"], "unstable");

        let report = json!({
            "reps": 2,
            "load_rep_summaries": summaries,
        });
        assert!(require_load_stable(&report).is_err());
    }

    #[test]
    fn gated_ratio_refuses_unstable_summaries() {
        let unstable = json!({
            "median": 1.0,
            "min": 1.0,
            "max": 3.0,
            "reps": 3,
            "spread_ratio": 3.0,
            "stability": "unstable",
        });
        assert!(gated_ratio(&unstable).is_err());
        let stable = json!({
            "median": 1.2,
            "min": 1.0,
            "max": 1.4,
            "reps": 3,
            "spread_ratio": 1.4,
            "stability": "stable",
        });
        assert!((gated_ratio(&stable).expect("stable") - 1.2).abs() < 1e-9);
        assert!((gated_ratio(&json!(1.5)).expect("plain") - 1.5).abs() < 1e-9);
    }

    #[test]
    fn unstable_scenario_ratio_refuses_parity_and_require_stable() {
        let unstable = scalar_summary(&[1.0, 1.0, 4.0]);
        assert_eq!(unstable["stability"], "unstable");
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
                "ratio_riffdb_over_postgres": if scenario == "create_comment" {
                    unstable.clone()
                } else {
                    json!(1.0)
                },
            })
        })
        .collect::<Vec<_>>();
        let report = json!({
            "reps": 3,
            "comparisons": {
                "available": true,
                "seed": {"ratio_riffdb_over_postgres": 1.0},
                "scenarios": scenarios,
                "rep_summaries": {
                    "seed_ratio": {"stability": "stable", "spread_ratio": 1.0},
                    "scenario:create_comment": unstable,
                },
            }
        });
        let err = assert_write_parity(&report).expect_err("must refuse unstable scenario");
        assert!(
            err.contains("unstable") || err.contains("create_comment"),
            "unexpected: {err}"
        );
        let req = require_stable(&report).expect_err("require-stable must fail");
        assert!(req.contains("create_comment") || req.contains("unstable"));
    }

    #[test]
    fn median_scenarios_carries_real_distribution_from_median_ranking_rep() {
        use riffdb_app_baseline_core::{SampleSet, ScenarioId, ScenarioResult};
        // Distinct multi-sample distributions; rank by p50, carry real SampleSet.
        // rep0: high p50 (~500), 3 samples
        // rep1: low p50 (~100), 4 samples
        // rep2: median p50 (~200), 5 samples with p99 != p50
        let mk = |samples: Vec<u64>| {
            let count = samples.len();
            ScenarioResult {
                scenario: ScenarioId::CreateComment,
                samples: SampleSet::from_nanos(samples),
                last_row_count: count,
            }
        };
        let rep_high = vec![mk(vec![400, 500, 600])]; // p50 ≈ 500
        let rep_low = vec![mk(vec![50, 100, 110, 120])]; // p50 ≈ 100
        let rep_mid = vec![mk(vec![180, 190, 200, 250, 900])]; // p50 ≈ 200, p99 high
        // Execution order high, low, mid — median-by-p50 is mid (200), not execution middle (low).
        let reps = vec![rep_high, rep_low, rep_mid.clone()];
        let selected = median_scenarios(&reps);
        assert_eq!(selected.len(), 1);
        let summary = selected[0].samples.summary();
        assert_eq!(
            summary.sample_count, 5,
            "must carry the median-ranking rep's real sample count, not fabricate 1"
        );
        assert_eq!(summary.p50_ns, rep_mid[0].samples.summary().p50_ns);
        assert_ne!(
            summary.p99_ns, summary.p50_ns,
            "real distribution must keep p99 distinct from p50"
        );
        assert_eq!(summary.max_ns, 900);
    }

    #[test]
    fn median_scenarios_reps_one_passes_through_unchanged() {
        use riffdb_app_baseline_core::{SampleSet, ScenarioId, ScenarioResult};
        let only = vec![ScenarioResult {
            scenario: ScenarioId::CreateComment,
            samples: SampleSet::from_nanos(vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]),
            last_row_count: 10,
        }];
        let out = median_scenarios(std::slice::from_ref(&only));
        let summary = out[0].samples.summary();
        assert_eq!(summary.sample_count, 10);
        assert_eq!(summary.p50_ns, only[0].samples.summary().p50_ns);
        assert_ne!(summary.p99_ns, summary.p50_ns);
    }

    #[test]
    fn require_stable_refuses_single_rep() {
        let report = json!({
            "reps": 1,
            "comparisons": {
                "rep_summaries": {
                    "seed_ratio": {"stability": "stable", "spread_ratio": 1.0},
                },
                "seed": {"ratio_riffdb_over_postgres": 1.0},
                "scenarios": [],
            }
        });
        let err = require_stable(&report).expect_err("single-rep must refuse");
        assert!(
            err.contains("--reps >= 2") || err.contains("trivial"),
            "{err}"
        );
    }
}
