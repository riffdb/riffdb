#![forbid(unsafe_code)]

//! Dependency-free repeated-run Fjall substrate benchmark.
//!
//! Results are deliberately marked non-publishable while the unchanged RiffDB
//! semantic conformance report remains failed.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use riffdb_storage_fjall_comparison::{
    ComparisonDurability, ComparisonMutation, FjallComparisonStore, RiffdbTable,
};

const DEFAULT_ITERATIONS: usize = 20;
const DEFAULT_WARMUP: usize = 3;
const DEFAULT_HISTORY_ROWS: usize = 500;
const MAX_ITERATIONS: usize = 10_000;
const MAX_HISTORY_ROWS: usize = 1_000;
const GROUP_SIZE: usize = 8;
const FIXTURE_CONTRACT_VERSION: &str =
    "none (substrate-only explicit semantic conformance failure)";

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

fn main() {
    if let Err(error) = run() {
        eprintln!("Fjall comparison benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let configuration = Configuration::from_process()?;
    if configuration.help {
        print_help();
        return Ok(());
    }

    let root = TempRoot::new()?;
    let database_path = root.path().join("comparison");
    let store = FjallComparisonStore::open(&database_path).map_err(display_adapter("open"))?;
    correctness_preflight(&store)?;
    populate_history(&store, configuration.history_rows)?;

    println!("riffdb_storage_benchmark_format=1");
    println!("engine=fjall");
    println!("engine_version=3.1.8");
    println!("engine_default_features=false");
    println!("semantic_contract=unchanged");
    println!("semantic_conformance=failed");
    println!("performance_eligibility=not_publishable_conformance_failure");
    println!("conformance_report=reports/conformance-v1.tsv");
    println!("dependency_inventory=reports/dependency-inventory-v1.tsv");
    println!("reference_harness=crates/riffdb-storage-redb/benches/storage_baseline.rs");
    println!("correctness_preflight=substrate_only_passed");
    println!("iterations={}", configuration.iterations);
    println!("warmup_iterations={}", configuration.warmup);
    println!("populated_history_records={}", configuration.history_rows);
    println!("durability_modes=sync_all,buffer_then_sync_all_group");
    println!(
        "database_size_bytes={}",
        store
            .disk_space_bytes()
            .map_err(display_adapter("read database size"))?
    );
    print_host_metadata(root.path());

    let point_samples = measure_repeated(configuration.warmup, configuration.iterations, || {
        let value = store
            .snapshot()
            .get(RiffdbTable::Audit, &0_u64.to_be_bytes())
            .map_err(display_adapter("point snapshot"))?;
        if value.is_none() {
            return Err("point snapshot missed preflight row".to_owned());
        }
        Ok(())
    })?;
    print_result(
        "point_read_snapshot",
        "one owned snapshot and exact key read",
        "read-only owned Fjall snapshot",
        store
            .disk_space_bytes()
            .map_err(display_adapter("read point benchmark database size"))?,
        &point_samples,
    );

    let range_samples = measure_repeated(configuration.warmup, configuration.iterations, || {
        let page = store
            .snapshot()
            .scan(RiffdbTable::Audit, None, 500, 4 * 1024 * 1024)
            .map_err(display_adapter("range snapshot"))?;
        if page.rows().is_empty() {
            return Err("range snapshot returned no fixture rows".to_owned());
        }
        Ok(())
    })?;
    print_result(
        "range_scan_up_to_500",
        "one owned physical-order page under 500-row and 4-MiB bounds",
        "read-only owned Fjall snapshot",
        store
            .disk_space_bytes()
            .map_err(display_adapter("read range benchmark database size"))?,
        &range_samples,
    );

    let next_sync_key = AtomicU64::new(1_000_000);
    let sync_samples = measure_repeated(configuration.warmup, configuration.iterations, || {
        let key = next_sync_key.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        store
            .apply(
                &[ComparisonMutation::put(RiffdbTable::Audit, key, b"sync")
                    .map_err(display_adapter("build sync mutation"))?],
                ComparisonDurability::Sync,
            )
            .map_err(display_adapter("synchronous atomic write"))
    })?;
    print_result(
        "synchronous_atomic_write",
        "one cross-keyspace-capable transaction committed with SyncAll",
        "Fjall SyncAll per transaction",
        store
            .disk_space_bytes()
            .map_err(display_adapter("read sync benchmark database size"))?,
        &sync_samples,
    );

    let next_group_key = AtomicU64::new(2_000_000);
    let group_samples = measure_repeated(configuration.warmup, configuration.iterations, || {
        for _ in 0..GROUP_SIZE {
            let key = next_group_key.fetch_add(1, Ordering::Relaxed).to_be_bytes();
            store
                .apply(
                    &[ComparisonMutation::put(RiffdbTable::Audit, key, b"group")
                        .map_err(display_adapter("build grouped mutation"))?],
                    ComparisonDurability::Buffered,
                )
                .map_err(display_adapter("buffer grouped transaction"))?;
        }
        store
            .flush_group()
            .map_err(display_adapter("flush grouped transactions"))
    })?;
    print_result(
        "buffered_eight_then_group_flush",
        "eight buffered transactions followed by one SyncAll journal flush",
        "eight Fjall Buffer commits followed by one SyncAll persist",
        store
            .disk_space_bytes()
            .map_err(display_adapter("read group benchmark database size"))?,
        &group_samples,
    );

    let reopen_database_size = store
        .disk_space_bytes()
        .map_err(display_adapter("read reopen benchmark database size"))?;
    drop(store);
    let reopen_samples = measure_repeated(configuration.warmup, configuration.iterations, || {
        let reopened =
            FjallComparisonStore::open(&database_path).map_err(display_adapter("reopen"))?;
        let value = reopened
            .snapshot()
            .get(RiffdbTable::Audit, &0_u64.to_be_bytes())
            .map_err(display_adapter("reopen probe"))?;
        if value.is_none() {
            return Err("reopen probe missed durable fixture row".to_owned());
        }
        drop(reopened);
        Ok(())
    })?;
    print_result(
        "reopen_recovery_probe",
        "recover engine state and read one synchronized row",
        "open existing Fjall files after synchronized clean close",
        reopen_database_size,
        &reopen_samples,
    );

    println!(
        "result_notice=substrate timings are not redb-versus-Fjall decision evidence until semantic conformance passes"
    );
    Ok(())
}

#[derive(Clone, Copy)]
struct Configuration {
    iterations: usize,
    warmup: usize,
    history_rows: usize,
    help: bool,
}

impl Configuration {
    fn from_process() -> Result<Self, String> {
        let mut iterations = environment_count("RIFFDB_STORAGE_BENCH_ITERATIONS", MAX_ITERATIONS)?
            .unwrap_or(DEFAULT_ITERATIONS);
        let mut warmup = environment_count("RIFFDB_STORAGE_BENCH_WARMUP", MAX_ITERATIONS)?
            .unwrap_or(DEFAULT_WARMUP);
        let mut history_rows =
            environment_count("RIFFDB_STORAGE_BENCH_HISTORY_RECORDS", MAX_HISTORY_ROWS)?
                .unwrap_or(DEFAULT_HISTORY_ROWS);
        let mut help = false;
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--bench" => {}
                "--help" | "-h" => help = true,
                "--iterations" => {
                    iterations = parse_count(
                        "--iterations",
                        next_value(&mut arguments, "--iterations")?,
                        MAX_ITERATIONS,
                    )?;
                }
                "--warmup" => {
                    warmup = parse_count(
                        "--warmup",
                        next_value(&mut arguments, "--warmup")?,
                        MAX_ITERATIONS,
                    )?;
                }
                "--history-records" => {
                    history_rows = parse_count(
                        "--history-records",
                        next_value(&mut arguments, "--history-records")?,
                        MAX_HISTORY_ROWS,
                    )?;
                }
                _ if argument.starts_with("--iterations=") => {
                    iterations = parse_count(
                        "--iterations",
                        argument.trim_start_matches("--iterations=").to_owned(),
                        MAX_ITERATIONS,
                    )?;
                }
                _ if argument.starts_with("--warmup=") => {
                    warmup = parse_count(
                        "--warmup",
                        argument.trim_start_matches("--warmup=").to_owned(),
                        MAX_ITERATIONS,
                    )?;
                }
                _ if argument.starts_with("--history-records=") => {
                    history_rows = parse_count(
                        "--history-records",
                        argument.trim_start_matches("--history-records=").to_owned(),
                        MAX_HISTORY_ROWS,
                    )?;
                }
                _ => return Err(format!("unrecognized argument: {argument}")),
            }
        }
        Ok(Self {
            iterations,
            warmup,
            history_rows,
            help,
        })
    }
}

fn next_value(arguments: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("{name} requires a value"))
}

fn environment_count(name: &str, maximum: usize) -> Result<Option<usize>, String> {
    env::var(name)
        .ok()
        .map(|value| parse_count(name, value, maximum))
        .transpose()
}

fn parse_count(name: &str, value: String, maximum: usize) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 || parsed > maximum {
        return Err(format!("{name} must be in 1..={maximum}"));
    }
    Ok(parsed)
}

fn print_help() {
    println!("storage_comparison [--iterations N] [--warmup N] [--history-records N]");
    println!(
        "RIFFDB_STORAGE_BENCH_ITERATIONS, RIFFDB_STORAGE_BENCH_WARMUP, and RIFFDB_STORAGE_BENCH_HISTORY_RECORDS set defaults"
    );
}

fn correctness_preflight(store: &FjallComparisonStore) -> Result<(), String> {
    let mutations = [
        ComparisonMutation::put(RiffdbTable::Entities, b"preflight/entity", b"entity")
            .map_err(display_adapter("build entity preflight"))?,
        ComparisonMutation::put(RiffdbTable::Commits, b"preflight/commit", b"commit")
            .map_err(display_adapter("build commit preflight"))?,
        ComparisonMutation::put(RiffdbTable::Events, b"preflight/event", b"event")
            .map_err(display_adapter("build event preflight"))?,
        ComparisonMutation::put(RiffdbTable::Outbox, b"preflight/outbox", b"event")
            .map_err(display_adapter("build outbox preflight"))?,
    ];
    store
        .apply(&mutations, ComparisonDurability::Sync)
        .map_err(display_adapter("preflight atomic write"))?;
    let snapshot = store.snapshot();
    for (table, key) in [
        (RiffdbTable::Entities, b"preflight/entity".as_slice()),
        (RiffdbTable::Commits, b"preflight/commit".as_slice()),
        (RiffdbTable::Events, b"preflight/event".as_slice()),
        (RiffdbTable::Outbox, b"preflight/outbox".as_slice()),
    ] {
        if snapshot
            .get(table, key)
            .map_err(display_adapter("preflight read"))?
            .is_none()
        {
            return Err("atomic preflight row missing".to_owned());
        }
    }
    Ok(())
}

fn populate_history(store: &FjallComparisonStore, rows: usize) -> Result<(), String> {
    let mutations = (0..rows)
        .map(|index| {
            let key = u64::try_from(index)
                .map_err(|_| "history key conversion failed".to_owned())?
                .to_be_bytes();
            ComparisonMutation::put(RiffdbTable::Audit, key, b"history")
                .map_err(display_adapter("build history row"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    store
        .apply(&mutations, ComparisonDurability::Sync)
        .map_err(display_adapter("populate history"))
}

fn measure_repeated(
    warmup: usize,
    iterations: usize,
    mut operation: impl FnMut() -> Result<(), String>,
) -> Result<Vec<Duration>, String> {
    for _ in 0..warmup {
        operation()?;
    }
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        operation()?;
        samples.push(started.elapsed());
    }
    Ok(samples)
}

fn print_result(
    name: &str,
    workload: &str,
    durability_mode: &str,
    database_size_bytes: u64,
    samples: &[Duration],
) {
    let mut nanos = samples.iter().map(Duration::as_nanos).collect::<Vec<_>>();
    nanos.sort_unstable();
    let total = nanos.iter().copied().sum::<u128>();
    let mean = total / u128::try_from(nanos.len()).expect("non-empty sample count");
    let distribution = nanos
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>()
        .join(",");

    println!("benchmark={name}");
    println!("workload={workload}");
    println!("workload_distribution=deterministic single-operation component samples");
    println!("contract_version={FIXTURE_CONTRACT_VERSION}");
    println!("durability_mode={durability_mode}");
    println!("database_size_bytes={database_size_bytes}");
    println!("sample_count={}", nanos.len());
    println!("sample_ns=[{distribution}]");
    println!("min_ns={}", nanos[0]);
    println!("p50_ns={}", percentile(&nanos, 50));
    println!("p95_ns={}", percentile(&nanos, 95));
    println!("p99_ns={}", percentile(&nanos, 99));
    println!("max_ns={}", nanos[nanos.len() - 1]);
    println!("mean_ns={mean}");
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    let numerator = (sorted.len() - 1)
        .checked_mul(percentile)
        .expect("sample percentile index");
    let index = numerator.div_ceil(100);
    sorted[index]
}

fn print_host_metadata(root: &Path) {
    println!(
        "git_revision={}",
        command_line("git", &["rev-parse", "HEAD"])
    );
    println!("git_dirty={}", git_dirty());
    let rust_verbose = command_output("rustc", &["-vV"]);
    println!(
        "rust_version={}",
        metadata_line(&rust_verbose, "release:").unwrap_or("unavailable")
    );
    println!(
        "rust_target={}",
        metadata_line(&rust_verbose, "host:").unwrap_or("unavailable")
    );
    println!("build_profile=bench");
    println!("enabled_features=local default empty; fjall default-features=false");
    println!("operating_system={}", command_line("uname", &["-srmo"]));
    println!("cpu={}", linux_metadata("/proc/cpuinfo", "model name"));
    println!("memory={}", linux_metadata("/proc/meminfo", "MemTotal"));
    println!(
        "storage_medium={}",
        env::var("RIFFDB_BENCH_STORAGE_MEDIUM").unwrap_or_else(|_| "unspecified".to_owned())
    );
    println!("filesystem={}", filesystem_metadata(root));
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|output| !output.is_empty())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn command_line(program: &str, arguments: &[&str]) -> String {
    command_output(program, arguments).replace('\n', " | ")
}

fn git_dirty() -> String {
    match Command::new("git").args(["status", "--porcelain"]).output() {
        Ok(output) if output.status.success() => {
            if output.stdout.is_empty() {
                "false".to_owned()
            } else {
                "true".to_owned()
            }
        }
        _ => "unavailable".to_owned(),
    }
}

fn metadata_line<'a>(metadata: &'a str, prefix: &str) -> Option<&'a str> {
    metadata
        .lines()
        .find_map(|line| line.strip_prefix(prefix).map(str::trim))
}

fn linux_metadata(path: &str, field: &str) -> String {
    fs::read_to_string(path)
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key.trim() == field).then(|| value.trim().to_owned())
            })
        })
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn filesystem_metadata(path: &Path) -> String {
    let path = path.to_string_lossy();
    let output = command_output("df", &["-T", path.as_ref()]);
    output
        .lines()
        .nth(1)
        .map(str::trim)
        .unwrap_or("unavailable")
        .to_owned()
}

fn display_adapter(
    operation: &'static str,
) -> impl FnOnce(riffdb_storage_fjall_comparison::AdapterError) -> String {
    move |error| format!("{operation}: {error}")
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Result<Self, String> {
        let path = env::temp_dir().join(format!(
            "riffdb-storage-fjall-benchmark-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).map_err(|error| format!("create benchmark root: {error}"))?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
