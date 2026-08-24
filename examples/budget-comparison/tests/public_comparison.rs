#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! External-process evidence for the public gRPC comparison runner.

use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, BufReader, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::str;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_client_rust::{
    BearerCredential, BootstrapCallMetadata, BootstrapCredential as TransportBootstrapCredential,
    CallMetadata, RiffDbClient, generate_capability_id, generate_request_id, v1,
};
use tokio::time::timeout;
use tonic::transport::Endpoint;

const RIFFDBD_ENV: &str = "RIFFDB_BUDGET_RIFFDBD_BIN";
const RUNNER_ENV: &str = "RIFFDB_BUDGET_RUNNER_BIN";
const PROTOCOL: &str = "riffdb.budget.public-run/v1";
const ADAPTER: &str = "riffdb-public-grpc-v1";
const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "wp135-public-comparison";
const CONTRACT_LINEAGE: &str = "LegalSpend";
const CONTRACT_VERSION: u64 = 1;
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const SHUTDOWN_COMMAND: &[u8] = b"shutdown\n";
const MAX_READY_LINE_BYTES: usize = 256;
const MAX_RUNNER_OUTPUT_BYTES: usize = 4_096;
const PROCESS_START_TIMEOUT: Duration = Duration::from_secs(15);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESS_KILL_TIMEOUT: Duration = Duration::from_secs(5);
const PROCESS_REAPER_POLL: Duration = Duration::from_millis(10);
const RUNNER_TIMEOUT: Duration = Duration::from_secs(180);
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_000_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;

const CAPABILITY_KEY_DOCUMENT: &[u8] = b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] = b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";
const BUDGET_CONTRACT: &str = include_str!("../../../contracts/examples/budget.riff");
const SUCCESS_FIXTURE: &[u8] =
    include_bytes!("../riffdb-grpc/fixtures/public-run-v1-success.jsonl");
const CHECKED_ERROR_FIXTURE: &[u8] =
    include_bytes!("../riffdb-grpc/fixtures/public-run-v1-checked-error.txt");
const INVALID_INVOCATION_FIXTURE: &[u8] =
    include_bytes!("../riffdb-grpc/fixtures/public-run-v1-invalid-invocation.txt");

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
fn public_comparison_harness_is_registered() {
    assert_eq!(
        SUCCESS_FIXTURE,
        b"{\"schema\":\"riffdb.budget.public-run/v1\",\"adapter\":\"riffdb-public-grpc-v1\",\"case\":\"sequential\",\"workload_version\":1,\"status\":\"passed\"}\n"
    );
    assert_eq!(CHECKED_ERROR_FIXTURE, b"riffdb budget public run failed\n");
    assert_eq!(
        INVALID_INVOCATION_FIXTURE,
        b"riffdb budget public invocation invalid\n"
    );
}

#[test]
fn public_comparison_adapter_dependency_boundary_is_frozen() {
    let manifest = include_str!("../riffdb-grpc/Cargo.toml");
    let adapter_source = include_str!("../riffdb-grpc/src/lib.rs");
    let runner_source = include_str!("../riffdb-grpc/src/bin/riffdb-budget-public.rs");
    let dependencies = manifest
        .split_once("[dependencies]\n")
        .expect("adapter manifest has a dependency section")
        .1
        .split_once("\n[lints]")
        .expect("adapter dependency section has one exact end")
        .0;
    assert_eq!(
        dependencies,
        concat!(
            "riffdb-budget-comparison-core = { path = \"../core\", version = \"0.1.0\" }\n",
            "riffdb-client-rust = { path = \"../../../crates/riffdb-client-rust\", version = \"0.1.0\", default-features = false }\n",
            "riffdb-proto = { path = \"../../../crates/riffdb-proto\", version = \"0.1.0\", default-features = false }\n",
            "riffdb-types = { path = \"../../../crates/riffdb-types\", version = \"0.1.0\", default-features = false }\n",
            "tokio = { version = \"=1.52.0\", default-features = false, features = [\"macros\", \"rt-multi-thread\"] }\n",
            "tonic = { version = \"=0.14.6\", default-features = false, features = [\"channel\", \"codegen\"] }\n",
        )
    );

    for banned in [
        "riffdb-api-mcp",
        "riffdb-auth",
        "riffdb-catalog",
        "riffdb-commit",
        "riffdb-conflict",
        "riffdb-contract-compiler",
        "riffdb-idempotency",
        "riffdb-policy",
        "riffdb-runtime",
        "riffdb-server",
        "riffdb-service",
        "riffdb-storage-api",
        "riffdb-storage-redb",
    ] {
        assert!(!manifest.contains(banned), "adapter depends on {banned}");
        let source_name = banned.replace('-', "_");
        for (owner, source) in [("adapter", adapter_source), ("runner", runner_source)] {
            assert!(
                !source.contains(&source_name),
                "{owner} imports {source_name}"
            );
        }
    }
    assert!(runner_source.contains("load_protected_bearer_credential"));
    for durability_reconciliation in [
        "pub enum PublicCommandDurability",
        "PublicCommandDurability::from_response",
        "PublicCommandDurability::from_commit(commit.durability)",
        "commit_matches_response_metadata(&notified_commit, &replay.metadata)",
    ] {
        assert!(
            adapter_source.contains(durability_reconciliation),
            "adapter lost fail-closed durability reconciliation: {durability_reconciliation}"
        );
    }
    assert!(!adapter_source.contains("response.durability_mode != \"sync\""));
    assert!(!adapter_source.contains(
        "commit.durability != v1::CommandDurability::Synchronous as i32"
    ));
    for bypass in ["std::fs", "std::env::var(", "std::env::var_os("] {
        assert!(
            !runner_source.contains(bypass),
            "runner contains credential or ambient-configuration bypass {bypass}"
        );
    }
}

#[test]
fn public_comparison_server_uses_an_explicit_backup_root() {
    let process_source = include_str!("public_comparison.rs");
    assert!(process_source.contains(".arg(\"--backup-root\")"));
}

#[test]
fn public_comparison_process_runner_matches_the_shared_oracle() -> TestResult<()> {
    let Some(binaries) = ProcessBinaries::from_environment()? else {
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for case in ["sequential", "contention", "same_key_replay"] {
            run_fresh_case(&binaries, case).await?;
        }
        assert_invalid_invocation(&binaries.runner)?;
        Ok(())
    })
}

/// Public gRPC process-path phase timings for optimization triage.
///
/// Each sample starts a fresh `riffdbd`, so process lifecycle costs are explicit
/// and comparable to the frozen suite wall-clock. Run via
/// `benchmarks/run-budget-diagnostics`.
#[test]
#[ignore = "run through benchmarks/run-budget-diagnostics"]
fn public_path_phase_diagnostics_report() -> TestResult<()> {
    let Some(binaries) = ProcessBinaries::from_environment()? else {
        return Err(test_failure(
            "public path diagnostics require RIFFDB_BUDGET_RIFFDBD_BIN and RIFFDB_BUDGET_RUNNER_BIN",
        ));
    };
    let output = required_diagnostics_output_path()?;
    let samples = diagnostics_sample_count();
    let warmups = diagnostics_warmup_count();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let report = runtime.block_on(async {
        for _ in 0..warmups {
            let _ = measure_public_path_phases(&binaries).await?;
        }
        let mut phases: std::collections::BTreeMap<&'static str, Vec<u64>> =
            std::collections::BTreeMap::new();
        for _ in 0..samples {
            let sample = measure_public_path_phases(&binaries).await?;
            for (name, elapsed_ns) in sample {
                phases.entry(name).or_default().push(elapsed_ns);
            }
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(phases)
    })?;

    let mut phase_rows = Vec::new();
    let mut total_mean = 0_u128;
    for (name, samples_ns) in &report {
        let summary = summarize_samples(samples_ns);
        // Exclude aggregate rows from the share denominator.
        if *name != "full_case_cycle_mean" {
            total_mean = total_mean.saturating_add(u128::from(summary.mean_ns));
        }
        phase_rows.push((name, summary));
    }
    let mut phases_json = Vec::new();
    let mut shares = Vec::new();
    for (name, summary) in &phase_rows {
        let share_bps = if total_mean == 0 || **name == "full_case_cycle_mean" {
            0
        } else {
            (u128::from(summary.mean_ns) * 10_000) / total_mean
        };
        if **name != "full_case_cycle_mean" {
            shares.push(((*name).to_owned(), share_bps, summary.mean_ns));
        }
        phases_json.push(format!(
            concat!(
                "{{\"phase\":\"{}\",\"sample_count\":{},\"min_ns\":{},\"p50_ns\":{},",
                "\"p95_ns\":{},\"p99_ns\":{},\"max_ns\":{},\"mean_ns\":{},",
                "\"share_basis_points\":{}}}"
            ),
            name,
            summary.sample_count,
            summary.min_ns,
            summary.p50_ns,
            summary.p95_ns,
            summary.p99_ns,
            summary.max_ns,
            summary.mean_ns,
            share_bps
        ));
    }
    shares.sort_by_key(|entry| std::cmp::Reverse(entry.1));
    let mut hint = String::from(
        "Inspect the largest share_basis_points phase first; if server_start_ready or bootstrap_deploy_credentials dominate, whole-suite process timings are lifecycle-dominated.",
    );
    if let Some((name, share_bps, _)) = shares.first()
        && *share_bps >= 2_500
    {
        hint = format!(
            "Phase `{name}` is {:.1}% of mean sample time; prioritize that surface before command-kernel tuning.",
            *share_bps as f64 / 100.0
        );
    }

    let body = format!(
        concat!(
            "{{\n",
            "  \"schema\": \"riffdb.budget.public-path-diagnostics/v1\",\n",
            "  \"report_id\": \"budget-public-path-diagnostics\",\n",
            "  \"sample_count\": {},\n",
            "  \"warmup_count\": {},\n",
            "  \"timing_source\": \"std::time::Instant\",\n",
            "  \"phases\": [\n    {}\n  ],\n",
            "  \"bottleneck_hint\": {},\n",
            "  \"limitations\": [\n",
            "    \"Each sample starts a fresh riffdbd and temporary database.\",\n",
            "    \"Case timings include runner child-process overhead for sequential, contention, and same_key_replay.\",\n",
            "    \"Diagnostic only; does not replace the frozen budget-comparison publication report.\"\n",
            "  ]\n",
            "}}\n"
        ),
        samples,
        warmups,
        phases_json.join(",\n    "),
        json_string(&hint)
    );
    fs::write(&output, body)?;
    println!(
        "RIFFDB_BUDGET_PUBLIC_PATH_DIAGNOSTICS_REPORT={}",
        output.display()
    );
    Ok(())
}

struct PhaseSummary {
    sample_count: usize,
    min_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    max_ns: u64,
    mean_ns: u64,
}

fn summarize_samples(samples_ns: &[u64]) -> PhaseSummary {
    let mut sorted = samples_ns.to_vec();
    sorted.sort_unstable();
    let sample_count = sorted.len();
    assert!(sample_count > 0);
    let sum: u128 = sorted.iter().map(|sample| u128::from(*sample)).sum();
    PhaseSummary {
        sample_count,
        min_ns: sorted[0],
        p50_ns: nearest_rank(&sorted, 50),
        p95_ns: nearest_rank(&sorted, 95),
        p99_ns: nearest_rank(&sorted, 99),
        max_ns: *sorted.last().expect("nonempty"),
        mean_ns: u64::try_from(sum / sample_count as u128).expect("mean fits u64"),
    }
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    let numerator = sorted.len().saturating_mul(percentile);
    let rank = numerator.div_ceil(100).max(1);
    sorted[rank - 1]
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("phase sample fits u64")
}

fn json_string(value: &str) -> String {
    let mut encoded = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => encoded.push_str("\\\""),
            '\\' => encoded.push_str("\\\\"),
            '\n' => encoded.push_str("\\n"),
            c if c.is_control() => encoded.push_str(&format!("\\u{:04x}", c as u32)),
            c => encoded.push(c),
        }
    }
    encoded.push('"');
    encoded
}

fn required_diagnostics_output_path() -> TestResult<PathBuf> {
    let raw = std::env::var("RIFFDB_BUDGET_PUBLIC_PATH_DIAGNOSTICS_OUTPUT")
        .map_err(|_| test_failure("RIFFDB_BUDGET_PUBLIC_PATH_DIAGNOSTICS_OUTPUT is required"))?;
    if raw.is_empty() || raw.len() > 4_096 {
        return Err(test_failure(
            "RIFFDB_BUDGET_PUBLIC_PATH_DIAGNOSTICS_OUTPUT is empty or too long",
        ));
    }
    let path = PathBuf::from(raw);
    if !path.is_absolute() {
        return Err(test_failure(
            "RIFFDB_BUDGET_PUBLIC_PATH_DIAGNOSTICS_OUTPUT must be absolute",
        ));
    }
    Ok(path)
}

fn diagnostics_sample_count() -> usize {
    parse_bounded_usize_env("RIFFDB_BUDGET_DIAGNOSTICS_SAMPLES", 3, 1, 100)
}

fn diagnostics_warmup_count() -> usize {
    parse_bounded_usize_env("RIFFDB_BUDGET_DIAGNOSTICS_WARMUPS", 0, 0, 20)
}

fn parse_bounded_usize_env(name: &str, default: usize, minimum: usize, maximum: usize) -> usize {
    let value = std::env::var(name).ok().map_or(default, |raw| {
        raw.parse()
            .unwrap_or_else(|_| panic!("{name} must be a usize"))
    });
    assert!(
        (minimum..=maximum).contains(&value),
        "{name} must be in {minimum}..={maximum}"
    );
    value
}

async fn measure_public_path_phases(
    binaries: &ProcessBinaries,
) -> TestResult<Vec<(&'static str, u64)>> {
    // Canonical cases share budget keys, so each case needs a fresh database.
    let sequential = measure_public_case_cycle(binaries, "sequential").await?;
    let contention = measure_public_case_cycle(binaries, "contention").await?;
    let replay = measure_public_case_cycle(binaries, "same_key_replay").await?;

    let mean3 = |a: u64, b: u64, c: u64| a.saturating_add(b).saturating_add(c) / 3;
    Ok(vec![
        (
            "server_start_ready",
            mean3(
                sequential.server_start_ready_ns,
                contention.server_start_ready_ns,
                replay.server_start_ready_ns,
            ),
        ),
        (
            "grpc_connect",
            mean3(
                sequential.grpc_connect_ns,
                contention.grpc_connect_ns,
                replay.grpc_connect_ns,
            ),
        ),
        (
            "bootstrap_deploy_credentials",
            mean3(
                sequential.bootstrap_deploy_credentials_ns,
                contention.bootstrap_deploy_credentials_ns,
                replay.bootstrap_deploy_credentials_ns,
            ),
        ),
        ("runner_sequential", sequential.runner_case_ns),
        ("runner_contention", contention.runner_case_ns),
        ("runner_same_key_replay", replay.runner_case_ns),
        (
            "server_shutdown",
            mean3(
                sequential.server_shutdown_ns,
                contention.server_shutdown_ns,
                replay.server_shutdown_ns,
            ),
        ),
        (
            "full_case_cycle_mean",
            mean3(
                sequential.full_cycle_ns,
                contention.full_cycle_ns,
                replay.full_cycle_ns,
            ),
        ),
    ])
}

struct PublicCaseCycle {
    server_start_ready_ns: u64,
    grpc_connect_ns: u64,
    bootstrap_deploy_credentials_ns: u64,
    runner_case_ns: u64,
    server_shutdown_ns: u64,
    full_cycle_ns: u64,
}

async fn measure_public_case_cycle(
    binaries: &ProcessBinaries,
    case: &'static str,
) -> TestResult<PublicCaseCycle> {
    let full_started = Instant::now();
    let temporary = TemporaryDirectory::new()?;
    let database_path = temporary.path().join("riffdb.redb");
    let backup_root = temporary.path().join("backups");
    let capability_keys_path = temporary.path().join("capability.keys");
    let idempotency_keys_path = temporary.path().join("idempotency.keys");
    let bootstrap_path = temporary.path().join("bootstrap.credential");
    let bearer_path = temporary.path().join("runner.credential");

    write_protected_file(&capability_keys_path, CAPABILITY_KEY_DOCUMENT)?;
    write_protected_file(&idempotency_keys_path, IDEMPOTENCY_KEY_DOCUMENT)?;
    let generated = generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)?;
    write_protected_file(&bootstrap_path, generated.render_document().expose_secret())?;
    drop(generated);
    let retained = load_bootstrap_credential_file(&bootstrap_path)?;

    let start_ready = Instant::now();
    let mut process = ServerProcess::spawn(
        &binaries.riffdbd,
        &database_path,
        &backup_root,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let address = process.wait_for_ready_address()?;
    let server_start_ready_ns = duration_ns(start_ready.elapsed());
    let endpoint = format!("http://{address}");

    let connect_started = Instant::now();
    let mut client = connect(&endpoint).await?;
    let grpc_connect_ns = duration_ns(connect_started.elapsed());

    let bootstrap_started = Instant::now();
    let credentials = bootstrap_deploy_and_issue(&mut client, &retained).await?;
    let bootstrap_deploy_credentials_ns = duration_ns(bootstrap_started.elapsed());
    write_protected_file(&bearer_path, credentials.runner.as_bytes())?;

    let case_started = Instant::now();
    let output = invoke_runner(&binaries.runner, case, &endpoint, &bearer_path)?;
    assert_runner_output(
        &output,
        0,
        &expected_success(case),
        b"",
        &format!("diagnostics {case}"),
    )?;
    let runner_case_ns = duration_ns(case_started.elapsed());

    let shutdown_started = Instant::now();
    process.shutdown_cleanly()?;
    let server_shutdown_ns = duration_ns(shutdown_started.elapsed());

    Ok(PublicCaseCycle {
        server_start_ready_ns,
        grpc_connect_ns,
        bootstrap_deploy_credentials_ns,
        runner_case_ns,
        server_shutdown_ns,
        full_cycle_ns: duration_ns(full_started.elapsed()),
    })
}

async fn run_fresh_case(binaries: &ProcessBinaries, case: &'static str) -> TestResult<()> {
    let temporary = TemporaryDirectory::new()?;
    let database_path = temporary.path().join("riffdb.redb");
    let backup_root = temporary.path().join("backups");
    let capability_keys_path = temporary.path().join("capability.keys");
    let idempotency_keys_path = temporary.path().join("idempotency.keys");
    let bootstrap_path = temporary.path().join("bootstrap.credential");
    let bearer_path = temporary.path().join("runner.credential");
    let checked_bearer_path = temporary.path().join("checked-runner.credential");

    write_protected_file(&capability_keys_path, CAPABILITY_KEY_DOCUMENT)?;
    write_protected_file(&idempotency_keys_path, IDEMPOTENCY_KEY_DOCUMENT)?;
    let generated = generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)?;
    write_protected_file(&bootstrap_path, generated.render_document().expose_secret())?;
    drop(generated);
    let retained = load_bootstrap_credential_file(&bootstrap_path)?;

    let mut process = ServerProcess::spawn(
        &binaries.riffdbd,
        &database_path,
        &backup_root,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let address = process.wait_for_ready_address()?;
    let endpoint = format!("http://{address}");
    let mut client = connect(&endpoint).await?;
    let credentials = bootstrap_deploy_and_issue(&mut client, &retained).await?;
    write_protected_file(&bearer_path, credentials.runner.as_bytes())?;
    write_protected_file(&checked_bearer_path, credentials.checked_failure.as_bytes())?;

    let output = invoke_runner(&binaries.runner, case, &endpoint, &bearer_path)?;
    assert_runner_output(
        &output,
        0,
        &expected_success(case),
        b"",
        "successful public comparison",
    )?;

    if case == "sequential" {
        let checked = invoke_runner(
            &binaries.runner,
            "same_key_replay",
            &endpoint,
            &checked_bearer_path,
        )?;
        assert_runner_output(
            &checked,
            1,
            b"",
            CHECKED_ERROR_FIXTURE,
            "checked public comparison failure",
        )?;
    }

    process.shutdown_cleanly()?;
    Ok(())
}

struct IssuedCredentials {
    runner: String,
    checked_failure: String,
}

async fn bootstrap_deploy_and_issue(
    client: &mut RiffDbClient,
    credential: &RetainedBootstrapCredential,
) -> TestResult<IssuedCredentials> {
    let token = bootstrap_token_text(credential)?;
    let bootstrap_metadata = BootstrapCallMetadata::new(TransportBootstrapCredential::new(token)?);
    let authenticated = CallMetadata::authenticated(BearerCredential::new(token)?);

    let created = bounded_rpc(
        "bootstrap capability creation",
        client.create_bootstrap_capability(bootstrap_request(credential)?, &bootstrap_metadata),
    )
    .await?;
    let Some(v1::create_capability_response::Result::Bootstrap(result)) = created.result else {
        return Err(test_failure(
            "bootstrap response used the wrong result family",
        ));
    };
    let Some(v1::bootstrap_create_capability_result::Result::Created(transition)) = result.result
    else {
        return Err(test_failure(
            "fresh database bootstrap was not newly created",
        ));
    };
    if transition.administration_sequence == 0 || transition.identity.is_none() {
        return Err(test_failure("bootstrap transition was incomplete"));
    }

    let deployed = bounded_rpc(
        "Budget contract deployment",
        client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: BUDGET_CONTRACT.to_owned(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            },
            &authenticated,
        ),
    )
    .await?;
    let Some(v1::deploy_contract_response::Result::Activated(contract)) = deployed.result else {
        return Err(test_failure("Budget contract was not newly activated"));
    };
    if contract.contract_lineage != CONTRACT_LINEAGE
        || contract.contract_version != CONTRACT_VERSION
    {
        return Err(test_failure(
            "activated Budget contract identity was unexpected",
        ));
    }

    let health = bounded_rpc(
        "post-deployment Health",
        client.health(
            v1::HealthRequest {
                request_id: Some(fresh_request_id_bytes()?),
            },
            &authenticated,
        ),
    )
    .await?;
    let Some(v1::health_response::Result::Authenticated(report)) = health.result else {
        return Err(test_failure("Health did not use the authenticated result"));
    };
    if report.status != v1::HealthStatus::Ready as i32
        || report.active_contract_version != Some(CONTRACT_VERSION)
    {
        return Err(test_failure(
            "fresh database did not become ready with the Budget contract",
        ));
    }

    let runner =
        create_normal_credential(client, &authenticated, "wp135-budget-runner", true).await?;
    let checked_failure = create_normal_credential(
        client,
        &authenticated,
        "wp135-budget-checked-failure",
        false,
    )
    .await?;
    Ok(IssuedCredentials {
        runner,
        checked_failure,
    })
}

fn bootstrap_request(
    credential: &RetainedBootstrapCredential,
) -> TestResult<v1::CreateCapabilityRequest> {
    use v1::capability_permission::Permission;

    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: CONTRACT_LINEAGE.to_owned(),
        stable_id,
    };
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "wp135-budget-runner".to_owned(),
        actor_kind: v1::ActorKind::Human as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
        audiences: vec![AUDIENCE.to_owned()],
        grant: Some(v1::CapabilityGrant {
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            partition_scope: Some(v1::PartitionScope {
                scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
            }),
            permissions: vec![
                v1::CapabilityPermission {
                    permission: Some(Permission::DeployContract(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::InvokeCommand(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::InvokeCommand(scoped(2))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadEntity(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::SubscribeCommits(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadHealth(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::AdministerCapabilities(v1::Unit {})),
                },
            ],
            field_visibility: vec![v1::EntityFieldVisibility {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                entity_type_id: 1,
                field_ids: vec![1, 3, 5],
                secret_field_ids: Vec::new(),
            }],
            max_scan_rows: 1,
            approval_required: Vec::new(),
            row_policy: None,
            export: None,
            reimport: None,
            vector_inspection: None,
        }),
    })
}

async fn create_normal_credential(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    principal_id: &str,
    include_subscribe: bool,
) -> TestResult<String> {
    let response = bounded_rpc(
        "normal runner capability creation",
        client.create_capability(
            normal_capability_request(principal_id, include_subscribe)?,
            metadata,
        ),
    )
    .await?;
    let Some(v1::create_capability_response::Result::Normal(result)) = response.result else {
        return Err(test_failure(
            "normal runner capability used the wrong result family",
        ));
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = result.result else {
        return Err(test_failure("normal runner capability was not created"));
    };
    if created.transition.is_none() || created.token.len() != 43 {
        return Err(test_failure(
            "normal runner capability response was incomplete",
        ));
    }
    Ok(created.token)
}

fn normal_capability_request(
    principal_id: &str,
    include_subscribe: bool,
) -> TestResult<v1::CreateCapabilityRequest> {
    use v1::capability_permission::Permission;

    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: CONTRACT_LINEAGE.to_owned(),
        stable_id,
    };
    let mut permissions = vec![
        v1::CapabilityPermission {
            permission: Some(Permission::InvokeCommand(scoped(1))),
        },
        v1::CapabilityPermission {
            permission: Some(Permission::InvokeCommand(scoped(2))),
        },
        v1::CapabilityPermission {
            permission: Some(Permission::ReadEntity(scoped(1))),
        },
    ];
    if include_subscribe {
        permissions.push(v1::CapabilityPermission {
            permission: Some(Permission::SubscribeCommits(v1::Unit {})),
        });
    }
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Normal as i32,
        capability_id: generate_capability_id()?.into_bytes().to_vec(),
        principal_id: principal_id.to_owned(),
        actor_kind: v1::ActorKind::Human as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
        audiences: vec![AUDIENCE.to_owned()],
        grant: Some(v1::CapabilityGrant {
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            partition_scope: Some(v1::PartitionScope {
                scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
            }),
            permissions,
            field_visibility: vec![v1::EntityFieldVisibility {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                entity_type_id: 1,
                field_ids: vec![1, 3, 5],
                secret_field_ids: Vec::new(),
            }],
            max_scan_rows: 1,
            approval_required: Vec::new(),
            row_policy: None,
            export: None,
            reimport: None,
            vector_inspection: None,
        }),
    })
}

async fn connect(endpoint: &str) -> TestResult<RiffDbClient> {
    let endpoint = Endpoint::from_shared(endpoint.to_owned())?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    bounded_rpc("gRPC connection", RiffDbClient::connect(endpoint)).await
}

async fn bounded_rpc<T, E>(
    label: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T>
where
    E: std::fmt::Display,
{
    match timeout(RPC_TIMEOUT, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(test_failure(format!("{label} failed: {error}"))),
        Err(_) => Err(test_failure(format!("{label} exceeded its deadline"))),
    }
}

fn bootstrap_token_text(credential: &RetainedBootstrapCredential) -> TestResult<&str> {
    str::from_utf8(credential.token().expose_secret()).map_err(Into::into)
}

fn fresh_request_id_bytes() -> TestResult<Vec<u8>> {
    Ok(generate_request_id()?.into_bytes().to_vec())
}

fn expected_success(case: &str) -> Vec<u8> {
    format!(
        "{{\"schema\":\"{PROTOCOL}\",\"adapter\":\"{ADAPTER}\",\"case\":\"{case}\",\"workload_version\":1,\"status\":\"passed\"}}\n"
    )
    .into_bytes()
}

fn invoke_runner(
    binary: &Path,
    case: &str,
    endpoint: &str,
    credential_path: &Path,
) -> TestResult<ProcessOutput> {
    let mut command = Command::new(binary);
    command
        .env_clear()
        .arg("--protocol")
        .arg(PROTOCOL)
        .arg("--case")
        .arg(case)
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--credential-file")
        .arg(credential_path);
    run_bounded_process(command)
}

fn assert_invalid_invocation(binary: &Path) -> TestResult<()> {
    let mut command = Command::new(binary);
    command.env_clear();
    let output = run_bounded_process(command)?;
    assert_runner_output(
        &output,
        2,
        b"",
        INVALID_INVOCATION_FIXTURE,
        "invalid public comparison invocation",
    )
}

fn assert_runner_output(
    output: &ProcessOutput,
    expected_code: i32,
    expected_stdout: &[u8],
    expected_stderr: &[u8],
    label: &str,
) -> TestResult<()> {
    if output.status.code() != Some(expected_code)
        || output.stdout != expected_stdout
        || output.stderr != expected_stderr
    {
        return Err(test_failure(format!(
            "{label} violated the closed exit/output contract"
        )));
    }
    Ok(())
}

struct ProcessBinaries {
    riffdbd: PathBuf,
    runner: PathBuf,
}

impl ProcessBinaries {
    fn from_environment() -> TestResult<Option<Self>> {
        match (std::env::var_os(RIFFDBD_ENV), std::env::var_os(RUNNER_ENV)) {
            (None, None) => Ok(None),
            (Some(riffdbd), Some(runner)) => {
                let riffdbd = checked_binary_path(riffdbd, RIFFDBD_ENV)?;
                let runner = checked_binary_path(runner, RUNNER_ENV)?;
                Ok(Some(Self { riffdbd, runner }))
            }
            _ => Err(test_failure(
                "both WP-135 process binary variables must be supplied together",
            )),
        }
    }
}

fn checked_binary_path(value: OsString, label: &str) -> TestResult<PathBuf> {
    if value.is_empty() {
        return Err(test_failure(format!("{label} was empty")));
    }
    let path = PathBuf::from(value);
    if !path.is_file() {
        return Err(test_failure(format!("{label} did not name a file")));
    }
    Ok(path)
}

struct ProcessOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

struct BoundedOutput {
    bytes: Vec<u8>,
    overflowed: bool,
}

fn run_bounded_process(mut command: Command) -> TestResult<ProcessOutput> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| test_failure("runner stdout pipe was unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| test_failure("runner stderr pipe was unavailable"))?;
    let stdout = thread::spawn(move || drain_bounded(stdout, MAX_RUNNER_OUTPUT_BYTES));
    let stderr = thread::spawn(move || drain_bounded(stderr, MAX_RUNNER_OUTPUT_BYTES));
    let (commands, reaper_commands) = mpsc::sync_channel(1);
    let (exit_sender, exited) = mpsc::sync_channel(1);
    let reaper = thread::spawn(move || reap_child(child, reaper_commands, exit_sender));

    let status = match exited.recv_timeout(RUNNER_TIMEOUT) {
        Ok(result) => result?,
        Err(RecvTimeoutError::Timeout) => {
            let _ = commands.send(ReaperCommand::Kill);
            let _ = exited.recv_timeout(PROCESS_KILL_TIMEOUT);
            let _ = reaper.join();
            let _ = stdout.join();
            let _ = stderr.join();
            return Err(test_failure("public comparison runner timed out"));
        }
        Err(RecvTimeoutError::Disconnected) => {
            let _ = reaper.join();
            let _ = stdout.join();
            let _ = stderr.join();
            return Err(test_failure(
                "public comparison process reaper disconnected",
            ));
        }
    };
    reaper
        .join()
        .map_err(|_| test_failure("public comparison process reaper panicked"))?;
    let stdout = stdout
        .join()
        .map_err(|_| test_failure("public comparison stdout reader panicked"))??;
    let stderr = stderr
        .join()
        .map_err(|_| test_failure("public comparison stderr reader panicked"))??;
    if stdout.overflowed || stderr.overflowed {
        return Err(test_failure(
            "public comparison runner exceeded its output bound",
        ));
    }
    Ok(ProcessOutput {
        status,
        stdout: stdout.bytes,
        stderr: stderr.bytes,
    })
}

fn drain_bounded(mut reader: impl Read, maximum: usize) -> io::Result<BoundedOutput> {
    let mut bytes = Vec::with_capacity(maximum.min(1_024));
    let mut overflowed = false;
    let mut buffer = [0_u8; 1_024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            return Ok(BoundedOutput { bytes, overflowed });
        }
        let remaining = maximum.saturating_sub(bytes.len());
        let retained = remaining.min(count);
        bytes.extend_from_slice(&buffer[..retained]);
        overflowed |= retained != count;
    }
}

fn write_protected_file(path: &Path, document: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(document)?;
    file.sync_all()?;
    drop(file);
    File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("secret path has no parent"))?,
    )?
    .sync_all()
}

mod bench_root_support;

fn test_failure(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new() -> io::Result<Self> {
        let dir = bench_root_support::unique_bench_dir("wp135-public");
        // Leak ownership into path; Drop of TemporaryDirectory still cleans.
        let path = dir.path().to_path_buf();
        std::mem::forget(dir);
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

enum ReaperCommand {
    Kill,
}

struct ServerProcess {
    stdin: Option<ChildStdin>,
    ready: Receiver<io::Result<String>>,
    reaper_commands: SyncSender<ReaperCommand>,
    exited: Receiver<io::Result<ExitStatus>>,
    reaper: Option<JoinHandle<()>>,
    stdout: Option<JoinHandle<usize>>,
    stderr: Option<JoinHandle<usize>>,
    exit_observed: bool,
}

impl ServerProcess {
    fn spawn(
        binary: &Path,
        database_path: &Path,
        backup_root: &Path,
        capability_keys_path: &Path,
        idempotency_keys_path: &Path,
    ) -> io::Result<Self> {
        let mut command = Command::new(binary);
        command
            .arg("--database")
            .arg(database_path)
            .arg("--backup-root")
            .arg(backup_root)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--environment")
            .arg(ENVIRONMENT)
            .arg("--audience")
            .arg(AUDIENCE)
            .arg("--capability-keys")
            .arg(capability_keys_path)
            .arg("--idempotency-keys")
            .arg(idempotency_keys_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("riffdbd stdin missing"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("riffdbd stdout missing"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("riffdbd stderr missing"))?;

        let (ready_sender, ready) = mpsc::sync_channel(1);
        let stdout = thread::spawn(move || read_ready_then_drain(stdout, ready_sender));
        let stderr = thread::spawn(move || drain_stream(stderr));
        let (reaper_commands, commands) = mpsc::sync_channel(1);
        let (exit_sender, exited) = mpsc::sync_channel(1);
        let reaper = thread::spawn(move || reap_child(child, commands, exit_sender));

        Ok(Self {
            stdin: Some(stdin),
            ready,
            reaper_commands,
            exited,
            reaper: Some(reaper),
            stdout: Some(stdout),
            stderr: Some(stderr),
            exit_observed: false,
        })
    }

    fn wait_for_ready_address(&self) -> TestResult<SocketAddr> {
        let line = match self.ready.recv_timeout(PROCESS_START_TIMEOUT) {
            Ok(result) => result?,
            Err(RecvTimeoutError::Timeout) => {
                return Err(test_failure("riffdbd readiness line timed out"));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(test_failure("riffdbd readiness reader disconnected"));
            }
        };
        let address = line
            .strip_prefix(READY_PREFIX)
            .ok_or_else(|| test_failure("riffdbd emitted an unknown readiness line"))?
            .parse::<SocketAddr>()?;
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(test_failure(
                "riffdbd readiness address was not bound loopback",
            ));
        }
        Ok(address)
    }

    fn shutdown_cleanly(&mut self) -> TestResult<()> {
        let mut stdin = self
            .stdin
            .take()
            .ok_or_else(|| test_failure("riffdbd stdin was already closed"))?;
        stdin.write_all(SHUTDOWN_COMMAND)?;
        stdin.flush()?;
        drop(stdin);
        self.wait_for_successful_exit(PROCESS_STOP_TIMEOUT)
    }

    fn wait_for_successful_exit(&mut self, deadline: Duration) -> TestResult<()> {
        match self.exited.recv_timeout(deadline) {
            Ok(result) => {
                self.exit_observed = true;
                let (stdout_bytes, stderr_bytes) = self.join_threads()?;
                let status = result?;
                if !status.success() {
                    return Err(test_failure(format!(
                        "riffdbd exited with {status}; drained {stdout_bytes} stdout and {stderr_bytes} stderr bytes"
                    )));
                }
                Ok(())
            }
            Err(RecvTimeoutError::Timeout) => {
                let _ = self.reaper_commands.send(ReaperCommand::Kill);
                if self.exited.recv_timeout(PROCESS_KILL_TIMEOUT).is_ok() {
                    self.exit_observed = true;
                    let _ = self.join_threads();
                }
                Err(test_failure("riffdbd clean shutdown timed out"))
            }
            Err(RecvTimeoutError::Disconnected) => {
                Err(test_failure("riffdbd process reaper disconnected"))
            }
        }
    }

    fn join_threads(&mut self) -> TestResult<(usize, usize)> {
        if let Some(reaper) = self.reaper.take() {
            reaper
                .join()
                .map_err(|_| test_failure("riffdbd process reaper panicked"))?;
        }
        let stdout_bytes = self
            .stdout
            .take()
            .ok_or_else(|| test_failure("riffdbd stdout reader was already joined"))?
            .join()
            .map_err(|_| test_failure("riffdbd stdout reader panicked"))?;
        let stderr_bytes = self
            .stderr
            .take()
            .ok_or_else(|| test_failure("riffdbd stderr reader was already joined"))?
            .join()
            .map_err(|_| test_failure("riffdbd stderr reader panicked"))?;
        Ok((stdout_bytes, stderr_bytes))
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stdin.take();
        if !self.exit_observed {
            let _ = self.reaper_commands.send(ReaperCommand::Kill);
            if self.exited.recv_timeout(PROCESS_KILL_TIMEOUT).is_ok() {
                self.exit_observed = true;
                let _ = self.join_threads();
            }
        }
    }
}

fn reap_child(
    mut child: Child,
    commands: Receiver<ReaperCommand>,
    exited: SyncSender<io::Result<ExitStatus>>,
) {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let _ = exited.send(Ok(status));
                return;
            }
            Ok(None) => {}
            Err(error) => {
                let _ = exited.send(Err(error));
                return;
            }
        }
        match commands.recv_timeout(PROCESS_REAPER_POLL) {
            Ok(ReaperCommand::Kill) | Err(RecvTimeoutError::Disconnected) => {
                let _ = child.kill();
                let _ = exited.send(child.wait());
                return;
            }
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

fn read_ready_then_drain(stdout: ChildStdout, ready: SyncSender<io::Result<String>>) -> usize {
    let mut reader = BufReader::new(stdout);
    let line = read_bounded_line(&mut reader, MAX_READY_LINE_BYTES);
    let line_bytes = line.as_ref().map_or(0, String::len);
    let _ = ready.send(line);
    line_bytes.saturating_add(drain_reader(&mut reader))
}

fn read_bounded_line(reader: &mut impl Read, maximum: usize) -> io::Result<String> {
    let mut bytes = Vec::with_capacity(maximum.min(64));
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte)? {
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "readiness line missing",
                ));
            }
            1 if byte[0] == b'\n' => break,
            1 if bytes.len() < maximum => bytes.push(byte[0]),
            1 => {
                drain_through_newline(reader)?;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "readiness line exceeded its bound",
                ));
            }
            _ => unreachable!("one-byte read returned more than one byte"),
        }
    }
    String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "readiness line was not UTF-8"))
}

fn drain_through_newline(reader: &mut impl Read) -> io::Result<()> {
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte)? {
            0 => return Ok(()),
            1 if byte[0] == b'\n' => return Ok(()),
            1 => {}
            _ => unreachable!("one-byte read returned more than one byte"),
        }
    }
}

fn drain_stream(stderr: ChildStderr) -> usize {
    drain_reader(&mut BufReader::new(stderr))
}

fn drain_reader(reader: &mut impl Read) -> usize {
    let mut total = 0_usize;
    let mut buffer = [0_u8; 1_024];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => return total,
            Ok(read) => total = total.saturating_add(read),
        }
    }
}
