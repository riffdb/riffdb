#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Required-live process evidence for the WP-139 budget safety report.

mod bench_root_support;

use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, BufReader, Read, Write};
use std::net::SocketAddr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::str;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_budget_comparison_postgres::live_database_url;
use riffdb_client_rust::{
    BearerCredential, BootstrapCallMetadata, BootstrapCredential as TransportBootstrapCredential,
    CallMetadata, RiffDbClient, generate_capability_id, generate_request_id, v1,
};
use tokio::time::timeout;
use tonic::transport::Endpoint;

const RIFFDBD_ENV: &str = "RIFFDB_BUDGET_RIFFDBD_BIN";
const RUNNER_ENV: &str = "RIFFDB_BUDGET_SAFETY_BIN";
const REPORT_PATH_ENV: &str = "RIFFDB_BUDGET_SAFETY_REPORT_PATH";
const PROTOCOL: &str = "riffdb.budget.safety-evidence/v1";
const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "wp139-safety-evidence";
const CONTRACT_LINEAGE: &str = "LegalSpend";
const CONTRACT_VERSION: u64 = 1;
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const SHUTDOWN_COMMAND: &[u8] = b"shutdown\n";
const MAX_READY_LINE_BYTES: usize = 256;
const MAX_RUNNER_OUTPUT_BYTES: usize = 32_768;
const MAX_REPORT_PATH_BYTES: usize = 4_096;
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
const SUCCESS_FIXTURE: &[u8] = include_bytes!("../fixtures/safety/report-v1.jsonl");
const CHECKED_ERROR_FIXTURE: &[u8] = include_bytes!("../fixtures/safety/checked-error.txt");
const INVALID_INVOCATION_FIXTURE: &[u8] =
    include_bytes!("../fixtures/safety/invalid-invocation.txt");

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
fn safety_evidence_runner_proves_all_four_live_contrasts() -> TestResult<()> {
    let Some(inputs) = LiveInputs::from_environment()? else {
        eprintln!(
            "WP-139 safety process evidence skipped; both {RIFFDBD_ENV} and {RUNNER_ENV} \
             are required to enable it"
        );
        return Ok(());
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(run_live_evidence(inputs))
}

async fn run_live_evidence(inputs: LiveInputs) -> TestResult<()> {
    let temporary = TemporaryDirectory::new()?;
    let database_path = temporary.path().join("riffdb.redb");
    let backup_root = temporary.path().join("backups");
    let capability_keys_path = temporary.path().join("capability.keys");
    let idempotency_keys_path = temporary.path().join("idempotency.keys");
    let bootstrap_path = temporary.path().join("bootstrap.credential");
    let bearer_path = temporary.path().join("runner.credential");
    let postgres_url_path = temporary.path().join("postgres.url");

    write_protected_file(&capability_keys_path, CAPABILITY_KEY_DOCUMENT)?;
    write_protected_file(&idempotency_keys_path, IDEMPOTENCY_KEY_DOCUMENT)?;
    write_protected_file(&postgres_url_path, inputs.postgres_url.as_bytes())?;
    let generated = generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)?;
    write_protected_file(&bootstrap_path, generated.render_document().expose_secret())?;
    drop(generated);
    let retained = load_bootstrap_credential_file(&bootstrap_path)?;

    let mut process = ServerProcess::spawn(
        &inputs.riffdbd,
        &database_path,
        &backup_root,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let address = process.wait_for_ready_address()?;
    let endpoint = format!("http://{address}");
    let mut client = connect(&endpoint).await?;
    let bearer = bootstrap_deploy_and_issue(&mut client, &retained).await?;
    write_protected_file(&bearer_path, bearer.as_bytes())?;
    drop(bearer);

    let output = invoke_runner(&inputs.runner, &postgres_url_path, &endpoint, &bearer_path)?;
    assert_runner_output(
        &output,
        0,
        SUCCESS_FIXTURE,
        b"",
        "successful safety evidence",
    )?;

    assert_invalid_invocation(&inputs.runner)?;
    process.shutdown_cleanly()?;
    let unavailable = invoke_runner(&inputs.runner, &postgres_url_path, &endpoint, &bearer_path)?;
    assert_runner_output(
        &unavailable,
        1,
        b"",
        CHECKED_ERROR_FIXTURE,
        "checked safety evidence failure",
    )?;

    if let Some(report_path) = inputs.report_path.as_deref() {
        write_protected_file(report_path, &output.stdout)?;
    } else {
        io::stdout().lock().write_all(&output.stdout)?;
    }
    Ok(())
}

struct LiveInputs {
    postgres_url: String,
    riffdbd: PathBuf,
    runner: PathBuf,
    report_path: Option<PathBuf>,
}

impl LiveInputs {
    fn from_environment() -> TestResult<Option<Self>> {
        let required = required_live_mode()?;
        let riffdbd = std::env::var_os(RIFFDBD_ENV);
        let runner = std::env::var_os(RUNNER_ENV);
        let (riffdbd, runner) = match (riffdbd, runner) {
            (None, None) if !required => return Ok(None),
            (Some(riffdbd), Some(runner)) => (riffdbd, runner),
            _ => {
                return Err(test_failure(
                    "both WP-139 process binaries are required in live mode",
                ));
            }
        };
        let postgres_url = live_database_url()
            .map_err(|_| test_failure("WP-139 PostgreSQL configuration is invalid"))?
            .ok_or_else(|| test_failure("WP-139 PostgreSQL URL is required in live mode"))?;
        if postgres_url
            .as_bytes()
            .iter()
            .any(|byte| matches!(*byte, 0 | b'\n' | b'\r'))
        {
            return Err(test_failure("WP-139 PostgreSQL URL is invalid"));
        }
        Ok(Some(Self {
            postgres_url,
            riffdbd: checked_binary_path(riffdbd, RIFFDBD_ENV)?,
            runner: checked_binary_path(runner, RUNNER_ENV)?,
            report_path: checked_report_path(std::env::var_os(REPORT_PATH_ENV))?,
        }))
    }
}

fn checked_report_path(candidate: Option<OsString>) -> TestResult<Option<PathBuf>> {
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let path = PathBuf::from(candidate);
    if path.as_os_str().as_bytes().len() > MAX_REPORT_PATH_BYTES
        || !path.is_absolute()
        || path.file_name().is_none()
        || !path.parent().is_some_and(Path::is_dir)
        || path.exists()
    {
        return Err(test_failure(
            "WP-139 safety report path must be a new file in an existing absolute directory",
        ));
    }
    Ok(Some(path))
}

fn assert_authoritatively_ready(report: &v1::AuthenticatedHealth) -> TestResult<()> {
    if report.active_contract_version != Some(CONTRACT_VERSION) {
        return Err(test_failure(
            "fresh database reported the wrong active Budget contract",
        ));
    }
    let expected_kinds = [
        v1::HealthComponentKind::AuthoritativeStorage,
        v1::HealthComponentKind::Catalog,
        v1::HealthComponentKind::CommitCoordinator,
        v1::HealthComponentKind::Projection,
        v1::HealthComponentKind::Outbox,
    ];
    let actual_kinds = report
        .components
        .iter()
        .map(|component| v1::HealthComponentKind::try_from(component.component))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| test_failure("Health reported an unknown component kind"))?;
    if actual_kinds != expected_kinds {
        return Err(test_failure(
            "Health omitted or reordered its authoritative and derived components",
        ));
    }
    for component in &report.components[..3] {
        if v1::HealthComponentStatus::try_from(component.status)
            != Ok(v1::HealthComponentStatus::Healthy)
        {
            return Err(test_failure(
                "fresh database reported an unhealthy authoritative component",
            ));
        }
    }

    let mut derived_degraded = false;
    for component in &report.components[3..] {
        match v1::HealthComponentStatus::try_from(component.status) {
            Ok(v1::HealthComponentStatus::Healthy) => {}
            Ok(v1::HealthComponentStatus::Degraded) => derived_degraded = true,
            _ => {
                return Err(test_failure(
                    "fresh database reported an unavailable derived component",
                ));
            }
        }
    }
    let expected_status = if derived_degraded {
        v1::HealthStatus::Degraded
    } else {
        v1::HealthStatus::Ready
    };
    if v1::HealthStatus::try_from(report.status) != Ok(expected_status) {
        return Err(test_failure(
            "aggregate Health did not match its authoritative and derived components",
        ));
    }
    Ok(())
}

#[test]
fn report_path_is_new_absolute_bounded_and_non_overwriting() -> TestResult<()> {
    let root = TemporaryDirectory::new()?;
    let report_path = root.path().join("report-v1.jsonl");
    assert_eq!(
        checked_report_path(Some(report_path.clone().into_os_string()))?,
        Some(report_path.clone())
    );
    assert!(checked_report_path(Some(OsString::from("relative-report.jsonl"))).is_err());
    assert!(
        checked_report_path(Some(
            root.path()
                .join("x".repeat(MAX_REPORT_PATH_BYTES))
                .into_os_string()
        ))
        .is_err()
    );
    assert!(
        checked_report_path(Some(
            root.path()
                .join("missing")
                .join("report.jsonl")
                .into_os_string()
        ))
        .is_err()
    );

    write_protected_file(&report_path, b"existing")?;
    assert!(
        checked_report_path(Some(report_path.clone().into_os_string())).is_err(),
        "an existing report must be rejected before execution"
    );
    assert!(
        write_protected_file(&report_path, b"replacement").is_err(),
        "create_new must remain the overwrite boundary"
    );
    assert_eq!(fs::read(&report_path)?, b"existing");
    assert_eq!(
        fs::metadata(&report_path)?.permissions().mode() & 0o777,
        0o600
    );
    Ok(())
}

fn required_live_mode() -> TestResult<bool> {
    match std::env::var("RIFFDB_BUDGET_POSTGRES_REQUIRED") {
        Ok(value) if value == "1" => Ok(true),
        Ok(value) if value == "0" || value.is_empty() => Ok(false),
        Ok(_) | Err(std::env::VarError::NotUnicode(_)) => {
            Err(test_failure("WP-139 required-live flag is invalid"))
        }
        Err(std::env::VarError::NotPresent) => Ok(false),
    }
}

async fn bootstrap_deploy_and_issue(
    client: &mut RiffDbClient,
    credential: &RetainedBootstrapCredential,
) -> TestResult<String> {
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
    assert_authoritatively_ready(&report)?;

    let response = bounded_rpc(
        "normal safety capability creation",
        client.create_capability(normal_capability_request()?, &authenticated),
    )
    .await?;
    let Some(v1::create_capability_response::Result::Normal(result)) = response.result else {
        return Err(test_failure(
            "normal safety capability used the wrong result family",
        ));
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = result.result else {
        return Err(test_failure("normal safety capability was not created"));
    };
    if created.transition.is_none() || created.token.len() != 43 {
        return Err(test_failure(
            "normal safety capability response was incomplete",
        ));
    }
    Ok(created.token)
}

fn bootstrap_request(
    credential: &RetainedBootstrapCredential,
) -> TestResult<v1::CreateCapabilityRequest> {
    use v1::capability_permission::Permission;

    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "wp139-safety-bootstrap".to_owned(),
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
                    permission: Some(Permission::ReadHealth(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::AdministerCapabilities(v1::Unit {})),
                },
            ],
            field_visibility: Vec::new(),
            max_scan_rows: 1,
            approval_required: Vec::new(),
        }),
    })
}

fn normal_capability_request() -> TestResult<v1::CreateCapabilityRequest> {
    use v1::capability_permission::Permission;

    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: CONTRACT_LINEAGE.to_owned(),
        stable_id,
    };
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Normal as i32,
        capability_id: generate_capability_id()?.into_bytes().to_vec(),
        principal_id: "wp139-safety-runner".to_owned(),
        actor_kind: v1::ActorKind::Agent as i32,
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
                    permission: Some(Permission::InvokeCommand(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::InvokeCommand(scoped(2))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadEntity(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ScanCommits(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::SubscribeCommits(v1::Unit {})),
                },
            ],
            field_visibility: vec![v1::EntityFieldVisibility {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                entity_type_id: 1,
                field_ids: vec![1, 3, 5],
            }],
            max_scan_rows: 100,
            approval_required: Vec::new(),
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

fn invoke_runner(
    binary: &Path,
    postgres_url_path: &Path,
    endpoint: &str,
    credential_path: &Path,
) -> TestResult<ProcessOutput> {
    let mut command = Command::new(binary);
    command
        .env_clear()
        .arg("--protocol")
        .arg(PROTOCOL)
        .arg("--postgres-url-file")
        .arg(postgres_url_path)
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
        "invalid safety evidence invocation",
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
            "{label} violated the closed exit/output contract: \
             exit={:?}/{expected_code}, stdout_bytes={}/{}, stdout_matches={}, \
             stderr_bytes={}/{}, stderr_matches={}",
            output.status.code(),
            output.stdout.len(),
            expected_stdout.len(),
            output.stdout == expected_stdout,
            output.stderr.len(),
            expected_stderr.len(),
            output.stderr == expected_stderr,
        )));
    }
    Ok(())
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
            return Err(test_failure("safety evidence runner timed out"));
        }
        Err(RecvTimeoutError::Disconnected) => {
            let _ = reaper.join();
            let _ = stdout.join();
            let _ = stderr.join();
            return Err(test_failure("safety evidence process reaper disconnected"));
        }
    };
    reaper
        .join()
        .map_err(|_| test_failure("safety evidence process reaper panicked"))?;
    let stdout = stdout
        .join()
        .map_err(|_| test_failure("safety evidence stdout reader panicked"))??;
    let stderr = stderr
        .join()
        .map_err(|_| test_failure("safety evidence stderr reader panicked"))??;
    if stdout.overflowed || stderr.overflowed {
        return Err(test_failure(
            "safety evidence runner exceeded its output bound",
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

fn test_failure(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new() -> io::Result<Self> {
        let dir = bench_root_support::unique_bench_dir("wp139-safety");
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
                        "riffdbd exited with {status}; drained {stdout_bytes} stdout and \
                         {stderr_bytes} stderr bytes"
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
