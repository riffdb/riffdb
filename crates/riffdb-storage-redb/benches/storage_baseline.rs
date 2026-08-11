#![forbid(unsafe_code)]

//! Dependency-free repeated-run component benchmarks for the redb adapter.

use std::env;
use std::fs;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    AuditPrincipalV1, BootstrapDigestCandidatesV1, BootstrapServiceAuditStartV1,
    CapabilityBootstrapAdministrationRepository, CapabilityBootstrapIntentV1,
    CapabilityBootstrapResult, CapabilityGrantV1, CapabilityPermissionKindV1,
    CapabilityPermissionV1, CapabilityPermissionsV1, CapabilityReader, CapabilityRequestedRecordV1,
    DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort,
    DatabaseInitializationResult, EvidencePageLimit, HistoricalEvidenceCursor,
    HistoricalEvidencePage, PartitionScopeV1, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, ServiceAuditAppendIntentV1, ServiceAuditAppendRepository,
    ServiceAuditAppendResult, StartupValidationInputs, StorageScanLimit, StructuralEvidenceCursor,
    StructuralEvidenceOpen, StructuralEvidencePage, StructuralEvidenceSession,
    StructuralOpenOutcome,
};
use riffdb_storage_redb::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
use riffdb_types::{
    ActorId, ActorKind, Audience, CapabilityId, CapabilityTokenDigest, DatabaseId, DigestKeyId,
    Environment, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1,
    ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, TenantScope, Timestamp,
};

const DEFAULT_ITERATIONS: usize = 20;
const DEFAULT_WARMUP_ITERATIONS: usize = 3;
const DEFAULT_HISTORY_RECORDS: usize = 64;
const MAX_ITERATIONS: usize = 10_000;
const MAX_HISTORY_RECORDS: usize = 1_000;
const STARTUP_PAGE_ITEMS: u32 = 64;
const FIXTURE_CONTRACT_VERSION: &str = "none (storage-only WP-070 fixture)";
const DURABILITY_MODE: &str = "redb immediate, two-phase commit";

static NEXT_TEMP_ROOT: AtomicU64 = AtomicU64::new(1);

fn main() {
    if let Err(error) = run() {
        eprintln!("riffdb storage benchmark failed: {error}");
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
    let initialized_path = root.path().join("initialized.redb");
    initialize(&initialized_path)?;
    correctness_preflight(&root, &initialized_path)?;
    let populated_path = root.path().join("populated-history.redb");
    let populated_bytes = populate_history(&populated_path, configuration.history_records)?;

    println!("riffdb_storage_benchmark_format=1");
    print_host_metadata(root.path());
    println!("correctness_preflight=passed");
    println!("iterations={}", configuration.iterations);
    println!("warmup_iterations={}", configuration.warmup_iterations);
    println!(
        "populated_history_records={}",
        configuration.history_records
    );
    println!(
        "populated_history_total_audit_records={}",
        configuration.history_records + 2
    );

    let initialized_bytes = file_size(&initialized_path)?;
    let open_samples = measure_repeated(
        configuration.warmup_iterations,
        configuration.iterations,
        || initialized_open_probe(&initialized_path),
    )?;
    print_result(
        "initialized_open_probe",
        "open one initialized database and verify its durable DatabaseId",
        initialized_bytes,
        &open_samples,
    );

    let startup_samples = measure_repeated(
        configuration.warmup_iterations,
        configuration.iterations,
        || {
            let dormant = complete_startup_evidence(&initialized_path)?;
            drop(dormant);
            Ok(())
        },
    )?;
    print_result(
        "full_startup_evidence",
        "open one initialized database and consume structural and historical evidence to exact end",
        initialized_bytes,
        &startup_samples,
    );

    let populated_startup_samples = measure_repeated(
        configuration.warmup_iterations,
        configuration.iterations,
        || {
            let ports = complete_startup_evidence(&populated_path)?
                .into_operational_after_catalog_validation()
                .map_err(display_error("rebuild populated transient indexes"))?;
            drop(ports);
            Ok(())
        },
    )?;
    print_result(
        "populated_history_full_startup",
        "open a populated database, consume structural and historical evidence to exact end, and rebuild transient operational indexes",
        populated_bytes,
        &populated_startup_samples,
    );

    let (bootstrap_samples, bootstrap_database_bytes) = measure_bootstrap_repeated(
        &root,
        configuration.warmup_iterations,
        configuration.iterations,
    )?;
    print_result(
        "durable_capability_bootstrap",
        "one atomic capability, token lookup, service-start audit, control-plane audit, and allocator commit per fresh initialized database",
        bootstrap_database_bytes,
        &bootstrap_samples,
    );

    Ok(())
}

#[derive(Clone, Copy)]
struct Configuration {
    iterations: usize,
    warmup_iterations: usize,
    history_records: usize,
    help: bool,
}

impl Configuration {
    fn from_process() -> Result<Self, String> {
        let mut iterations = environment_count("RIFFDB_STORAGE_BENCH_ITERATIONS", MAX_ITERATIONS)?
            .unwrap_or(DEFAULT_ITERATIONS);
        let mut warmup_iterations =
            environment_count("RIFFDB_STORAGE_BENCH_WARMUP", MAX_ITERATIONS)?
                .unwrap_or(DEFAULT_WARMUP_ITERATIONS);
        let mut history_records =
            environment_count("RIFFDB_STORAGE_BENCH_HISTORY_RECORDS", MAX_HISTORY_RECORDS)?
                .unwrap_or(DEFAULT_HISTORY_RECORDS);
        let mut help = false;
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--bench" => {}
                "--help" | "-h" => help = true,
                "--iterations" => {
                    iterations = parse_count(
                        "--iterations",
                        arguments
                            .next()
                            .ok_or_else(|| "--iterations requires a value".to_owned())?,
                        MAX_ITERATIONS,
                    )?;
                }
                "--warmup" => {
                    warmup_iterations = parse_count(
                        "--warmup",
                        arguments
                            .next()
                            .ok_or_else(|| "--warmup requires a value".to_owned())?,
                        MAX_ITERATIONS,
                    )?;
                }
                "--history-records" => {
                    history_records = parse_count(
                        "--history-records",
                        arguments
                            .next()
                            .ok_or_else(|| "--history-records requires a value".to_owned())?,
                        MAX_HISTORY_RECORDS,
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
                    warmup_iterations = parse_count(
                        "--warmup",
                        argument.trim_start_matches("--warmup=").to_owned(),
                        MAX_ITERATIONS,
                    )?;
                }
                _ if argument.starts_with("--history-records=") => {
                    history_records = parse_count(
                        "--history-records",
                        argument.trim_start_matches("--history-records=").to_owned(),
                        MAX_HISTORY_RECORDS,
                    )?;
                }
                _ => return Err(format!("unrecognized argument: {argument}")),
            }
        }
        Ok(Self {
            iterations,
            warmup_iterations,
            history_records,
            help,
        })
    }
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
    if parsed == 0 {
        return Err(format!("{name} must be greater than zero"));
    }
    if parsed > maximum {
        return Err(format!("{name} must not exceed {maximum}"));
    }
    Ok(parsed)
}

fn print_help() {
    println!("storage_baseline [--iterations N] [--warmup N] [--history-records N]");
    println!(
        "RIFFDB_STORAGE_BENCH_ITERATIONS, RIFFDB_STORAGE_BENCH_WARMUP, and RIFFDB_STORAGE_BENCH_HISTORY_RECORDS set defaults"
    );
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Result<Self, String> {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "riffdb-storage-benchmark-{}-{}",
            std::process::id(),
            NEXT_TEMP_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).map_err(|error| format!("create {}: {error}", path.display()))?;
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

fn initialize(path: &Path) -> Result<(), String> {
    let mut store = RedbStore::open(path).map_err(display_error("open initialization fixture"))?;
    let expected = database_id();
    match store
        .initialize_database(expected)
        .map_err(display_error("initialize fixture"))?
    {
        DatabaseInitializationResult::Installed(actual) if actual == expected => Ok(()),
        result => Err(format!("unexpected initialization result: {result:?}")),
    }
}

fn initialized_open_probe(path: &Path) -> Result<(), String> {
    let store = RedbStore::open(path).map_err(display_error("open initialized database"))?;
    match store
        .probe_database_identity()
        .map_err(display_error("probe initialized database"))?
    {
        DatabaseIdentityProbe::Existing(actual) if actual == database_id() => Ok(()),
        probe => Err(format!("unexpected identity probe: {probe:?}")),
    }
}

fn complete_startup_evidence(path: &Path) -> Result<RedbDormantPorts, String> {
    let store = RedbStore::open(path).map_err(display_error("open startup fixture"))?;
    let mut session = store
        .begin_structural_evidence(startup_inputs())
        .map_err(display_error("begin startup evidence"))?;
    let found_database_id = session.database_id();
    if found_database_id != database_id() {
        return Err("startup evidence returned the wrong database ID".to_owned());
    }
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(STARTUP_PAGE_ITEMS)
        .ok_or_else(|| "construct evidence page limit: out of range".to_owned())?;

    let mut structural = StructuralEvidenceCursor::start(found_database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural, limit)
            .map_err(display_error("read structural evidence"))?
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                if !findings.is_empty() {
                    return Err(format!(
                        "structural correctness preflight found {} finding(s)",
                        findings.len()
                    ));
                }
                structural = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };

    let mut historical = HistoricalEvidenceCursor::start(found_database_id, open_session_id);
    let historical_end = loop {
        match session
            .read_historical_evidence(historical, limit)
            .map_err(display_error("read historical evidence"))?
        {
            HistoricalEvidencePage::Page { next, .. } => historical = next,
            HistoricalEvidencePage::ExactEnd(end) => break end,
        }
    };
    let outcome = session
        .finish(structural_end, historical_end)
        .map_err(display_error("finish startup evidence"))?;
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        return Err("V2-only benchmark fixture unexpectedly requires index migration".to_owned());
    };
    let (opened_database_id, _, _, dormant) = opened.into_parts();
    if opened_database_id != database_id() {
        return Err("startup handoff returned the wrong database ID".to_owned());
    }
    Ok(dormant)
}

fn operational_fixture(path: &Path) -> Result<RedbOperationalPorts, String> {
    initialize(path)?;
    complete_startup_evidence(path)?
        .into_operational_after_catalog_validation()
        .map_err(display_error("activate empty benchmark fixture"))
}

fn correctness_preflight(root: &TempRoot, initialized_path: &Path) -> Result<(), String> {
    initialized_open_probe(initialized_path)?;
    drop(complete_startup_evidence(initialized_path)?);

    let bootstrap_path = root.path().join("bootstrap-preflight.redb");
    let mut ports = operational_fixture(&bootstrap_path)?;
    let intent = bootstrap_intent()?;
    let result = commit_bootstrap(&mut ports, &intent)?;
    verify_bootstrap(&ports, result)?;
    drop(ports);
    let reopened = complete_startup_evidence(&bootstrap_path)?
        .into_operational_after_catalog_validation()
        .map_err(display_error("reactivate preflight fixture after reopen"))?;
    verify_durable_bootstrap_state(&reopened)?;
    Ok(())
}

fn populate_history(path: &Path, history_records: usize) -> Result<u64, String> {
    let mut ports = operational_fixture(path)?;
    let bootstrap = bootstrap_intent()?;
    let result = commit_bootstrap(&mut ports, &bootstrap)?;
    verify_bootstrap(&ports, result)?;

    for index in 0..history_records {
        let intent = history_audit_intent(index)?;
        match ports
            .append_service_audit(&intent)
            .map_err(display_error("append populated-history service audit"))?
        {
            ServiceAuditAppendResult::Appended(record)
                if record.request_id() == intent.request_id() => {}
            result => {
                return Err(format!(
                    "unexpected populated-history append result: {result:?}"
                ));
            }
        }
    }
    let expected = history_records
        .checked_add(2)
        .ok_or_else(|| "populated audit count overflow".to_owned())?;
    verify_audit_record_count(&ports, expected)?;
    drop(ports);

    let reopened = complete_startup_evidence(path)?
        .into_operational_after_catalog_validation()
        .map_err(display_error("reactivate populated-history preflight"))?;
    verify_durable_capability(&reopened)?;
    verify_audit_record_count(&reopened, expected)?;
    drop(reopened);
    file_size(path)
}

fn commit_bootstrap(
    ports: &mut RedbOperationalPorts,
    intent: &CapabilityBootstrapIntentV1,
) -> Result<CapabilityBootstrapResult, String> {
    ports
        .bootstrap_capability(intent)
        .map_err(display_error("commit capability bootstrap"))
}

fn verify_bootstrap(
    ports: &RedbOperationalPorts,
    result: CapabilityBootstrapResult,
) -> Result<(), String> {
    let capability_id = capability_id();
    match result {
        CapabilityBootstrapResult::BootstrapCreated {
            capability_id: actual,
            ..
        } if actual == capability_id => {}
        result => {
            return Err(format!(
                "unexpected capability bootstrap result: {result:?}"
            ));
        }
    }
    verify_durable_bootstrap_state(ports)
}

fn verify_durable_bootstrap_state(ports: &RedbOperationalPorts) -> Result<(), String> {
    verify_durable_capability(ports)?;
    verify_audit_record_count(ports, 2)
}

fn verify_durable_capability(ports: &RedbOperationalPorts) -> Result<(), String> {
    let capability_id = capability_id();
    let stored = ports
        .read_capability(capability_id)
        .map_err(display_error("read bootstrapped capability"))?
        .ok_or_else(|| "bootstrapped capability was not durable".to_owned())?;
    if stored.capability_id() != capability_id {
        return Err("durable capability ID did not match the request".to_owned());
    }
    Ok(())
}

fn verify_audit_record_count(ports: &RedbOperationalPorts, expected: usize) -> Result<(), String> {
    let limit = StorageScanLimit::new(500)
        .ok_or_else(|| "construct audit scan limit: out of range".to_owned())?;
    let mut after = None;
    let mut count = 0usize;
    loop {
        let scan = ports
            .scan_administration_audit(AdministrationAuditScanRequest::new(after, limit))
            .map_err(display_error("scan durable administration audit"))?;
        let (records, next) = match scan {
            AdministrationAuditScan::Page {
                records,
                next_after,
            } => (records, Some(next_after)),
            AdministrationAuditScan::ExactEnd { records } => (records, None),
        };
        if records
            .iter()
            .any(|record| record.encoded_content_charge().get() == 0)
        {
            return Err("durable administration audit contained a zero charge".to_owned());
        }
        count = count
            .checked_add(records.len())
            .ok_or_else(|| "durable administration audit count overflow".to_owned())?;
        match next {
            Some(next_after) => after = Some(next_after),
            None => break,
        }
    }
    if count == expected {
        Ok(())
    } else {
        Err(format!(
            "expected {expected} durable administration audit records, found {count}"
        ))
    }
}

fn measure_repeated<F>(
    warmup_iterations: usize,
    iterations: usize,
    mut operation: F,
) -> Result<Vec<Duration>, String>
where
    F: FnMut() -> Result<(), String>,
{
    for _ in 0..warmup_iterations {
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

fn measure_bootstrap_repeated(
    root: &TempRoot,
    warmup_iterations: usize,
    iterations: usize,
) -> Result<(Vec<Duration>, u64), String> {
    let total = warmup_iterations
        .checked_add(iterations)
        .ok_or_else(|| "benchmark iteration count overflow".to_owned())?;
    let mut samples = Vec::with_capacity(iterations);
    let mut last_database_bytes = 0;
    for index in 0..total {
        let path = root.path().join(format!("bootstrap-{index}.redb"));
        let mut ports = operational_fixture(&path)?;
        let intent = bootstrap_intent()?;
        let started = Instant::now();
        let result = commit_bootstrap(&mut ports, &intent)?;
        let elapsed = started.elapsed();
        verify_bootstrap(&ports, result)?;
        drop(ports);
        last_database_bytes = file_size(&path)?;
        if index >= warmup_iterations {
            samples.push(elapsed);
        }
        fs::remove_file(&path)
            .map_err(|error| format!("remove bootstrap fixture {}: {error}", path.display()))?;
    }
    Ok((samples, last_database_bytes))
}

fn print_result(name: &str, workload: &str, database_size_bytes: u64, samples: &[Duration]) {
    let mut nanoseconds = samples.iter().map(Duration::as_nanos).collect::<Vec<_>>();
    nanoseconds.sort_unstable();
    let total = nanoseconds.iter().copied().sum::<u128>();
    let mean = total / u128::try_from(nanoseconds.len()).expect("non-empty sample count");
    let distribution = nanoseconds
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>()
        .join(",");

    println!("benchmark={name}");
    println!("workload={workload}");
    println!("workload_distribution=deterministic single-operation component samples");
    println!("contract_version={FIXTURE_CONTRACT_VERSION}");
    println!("durability_mode={DURABILITY_MODE}");
    println!("database_size_bytes={database_size_bytes}");
    println!("sample_count={}", nanoseconds.len());
    println!("sample_ns=[{distribution}]");
    println!("min_ns={}", nanoseconds[0]);
    println!("p50_ns={}", percentile(&nanoseconds, 50));
    println!("p95_ns={}", percentile(&nanoseconds, 95));
    println!("p99_ns={}", percentile(&nanoseconds, 99));
    println!("max_ns={}", nanoseconds[nanoseconds.len() - 1]);
    println!("mean_ns={mean}");
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    let numerator = (sorted.len() - 1)
        .checked_mul(percentile)
        .expect("sample percentile index");
    let index = numerator.div_ceil(100);
    sorted[index]
}

fn print_host_metadata(database_root: &Path) {
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
    println!("enabled_features=default (empty)");
    println!("operating_system={}", command_line("uname", &["-srmo"]));
    println!("cpu={}", linux_metadata("/proc/cpuinfo", "model name"));
    println!("memory={}", linux_metadata("/proc/meminfo", "MemTotal"));
    println!(
        "storage_medium={}",
        env::var("RIFFDB_BENCH_STORAGE_MEDIUM").unwrap_or_else(|_| "unspecified".to_owned())
    );
    println!("filesystem={}", filesystem_metadata(database_root));
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

fn file_size(path: &Path) -> Result<u64, String> {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|error| format!("read database size {}: {error}", path.display()))
}

fn startup_inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(digest_key_id());
    StartupValidationInputs::new(
        Timestamp::new(100, 0).expect("valid benchmark timestamp"),
        ReadableCapabilityDigestInventory::new(vec![key])
            .expect("valid capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![key])
            .expect("valid idempotency digest inventory"),
    )
}

fn bootstrap_intent() -> Result<CapabilityBootstrapIntentV1, String> {
    let issued_at = Timestamp::new(10, 0).map_err(display_error("construct issued timestamp"))?;
    let start = BootstrapServiceAuditStartV1::new(
        request_id(),
        issued_at,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(capability_id())])
            .map_err(display_error("construct bootstrap audit targets"))?,
        None,
    )
    .map_err(display_error("construct bootstrap audit start"))?;
    CapabilityBootstrapIntentV1::new(
        capability_id(),
        requested_record()?,
        BootstrapDigestCandidatesV1::new(vec![token_digest()], token_digest())
            .map_err(display_error("construct digest candidates"))?,
        issued_at,
        Timestamp::new(1_000_010, 0).map_err(display_error("construct expiry timestamp"))?,
        start,
    )
    .map_err(display_error("construct bootstrap intent"))
}

fn history_audit_intent(index: usize) -> Result<ServiceAuditAppendIntentV1, String> {
    let seconds = i64::try_from(index)
        .ok()
        .and_then(|value| value.checked_add(20))
        .ok_or_else(|| "populated-history timestamp overflow".to_owned())?;
    ServiceAuditAppendIntentV1::new(
        history_request_id(index)?,
        Timestamp::new(seconds, 0).map_err(display_error("construct history timestamp"))?,
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        AuditPrincipalV1::new(
            ActorId::new("benchmark-operator")
                .map_err(display_error("construct history principal"))?,
            ActorKind::Human,
            capability_id(),
            NonZeroU64::MIN,
        ),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .map_err(display_error("construct populated-history audit intent"))
}

fn requested_record() -> Result<CapabilityRequestedRecordV1, String> {
    let permissions = CapabilityPermissionsV1::new(vec![
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::AdministerCapabilities)
            .map_err(display_error("construct bootstrap permission"))?,
    ])
    .map_err(display_error("construct bootstrap permissions"))?;
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        permissions,
        Vec::new(),
        NonZeroU16::MIN,
        Vec::new(),
    )
    .map_err(display_error("construct bootstrap grant"))?;
    CapabilityRequestedRecordV1::new(
        database_id(),
        Environment::new("benchmark").map_err(display_error("construct environment"))?,
        ActorId::new("benchmark-operator").map_err(display_error("construct actor"))?,
        ActorKind::Human,
        NonZeroU32::new(1_000_000).expect("nonzero duration"),
        vec![Audience::new("riffdb-benchmark").map_err(display_error("construct audience"))?],
        grant,
    )
    .map_err(display_error("construct requested capability"))
}

fn database_id() -> DatabaseId {
    DatabaseId::from_bytes(uuid_bytes(0x11)).expect("valid UUIDv7 database ID")
}

fn capability_id() -> CapabilityId {
    CapabilityId::from_bytes(uuid_bytes(0x22)).expect("valid UUIDv7 capability ID")
}

fn request_id() -> RequestId {
    RequestId::from_bytes(uuid_bytes(0x33)).expect("valid UUIDv7 request ID")
}

fn history_request_id(index: usize) -> Result<RequestId, String> {
    let counter = u64::try_from(index)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| "populated-history request ID overflow".to_owned())?;
    RequestId::from_bytes(indexed_uuid_bytes(0x55, counter))
        .map_err(display_error("construct populated-history request ID"))
}

fn digest_key_id() -> DigestKeyId {
    DigestKeyId::new(1).expect("nonzero digest key ID")
}

fn token_digest() -> CapabilityTokenDigest {
    CapabilityTokenDigest::from_hmac_bytes(digest_key_id(), [0x44; 32])
}

fn uuid_bytes(seed: u8) -> [u8; 16] {
    let mut bytes = [seed; 16];
    bytes[6] = 0x70 | (seed & 0x0f);
    bytes[8] = 0x80 | (seed & 0x3f);
    bytes
}

fn indexed_uuid_bytes(domain: u8, counter: u64) -> [u8; 16] {
    let mut bytes = [domain; 16];
    bytes[8..].copy_from_slice(&counter.to_be_bytes());
    bytes[6] = 0x70 | (domain & 0x0f);
    bytes[8] = 0x80 | (bytes[8] & 0x3f);
    bytes
}

fn display_error<E: std::fmt::Display>(context: &'static str) -> impl FnOnce(E) -> String {
    move |error| format!("{context}: {error}")
}
