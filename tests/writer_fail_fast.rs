#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Real-process proof that a writer-thread panic fences command admission and
//! makes `riffdbd` exit nonzero.

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::process::{Child, ChildStderr, ChildStdout, Command, ExitStatus, Stdio};
use std::str;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_client_rust::generated::legal_spend::{Amount, CONTRACT_LINEAGE, CreateBudget};
use riffdb_client_rust::{
    ApplicationErrorCode, AttemptBudget, BearerCredential, BootstrapCallMetadata,
    BootstrapCredential as TransportBootstrapCredential, CallMetadata, ClientError,
    GeneratedExecutionError, RiffDbClient, generate_request_id,
};
use riffdb_errors::PublicErrorKind;
use riffdb_proto::v1;
use tokio::time::timeout;
use tonic::transport::Endpoint;

const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "writer-fail-fast-test";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const WRITER_FAILPOINT: &str = "RIFFDB_TEST_WRITER_FAILPOINT";
const WRITER_FAILPOINT_ARM: &str = "RIFFDB_TEST_WRITER_FAILPOINT_ARM";
const MAX_READY_LINE_BYTES: usize = 256;
const MAX_RETAINED_STDERR_LINES: usize = 256;
const PROCESS_START_TIMEOUT: Duration = Duration::from_secs(15);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESS_KILL_TIMEOUT: Duration = Duration::from_secs(5);
const PROCESS_REAPER_POLL: Duration = Duration::from_millis(10);
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_000_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;
const CAPABILITY_KEY_DOCUMENT: &[u8] = b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] = b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";
const BUDGET_CONTRACT: &str = include_str!("../contracts/examples/budget.riff");

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_thread_panic_stops_the_coordinator_and_exits_the_daemon_nonzero() -> TestResult<()>
{
    let temporary = riffdb_testkit::scratch::ScratchDir::new("writer-fail-fast")?;
    let database = temporary.path().join("riffdb.redb");
    let capability_keys = temporary.path().join("capability.keys");
    let idempotency_keys = temporary.path().join("idempotency.keys");
    let bootstrap_path = temporary.path().join("bootstrap.credential");
    let arm = temporary.path().join("writer-panic.arm");

    write_protected_file(&capability_keys, CAPABILITY_KEY_DOCUMENT)?;
    write_protected_file(&idempotency_keys, IDEMPOTENCY_KEY_DOCUMENT)?;
    let generated = generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)?;
    write_protected_file(&bootstrap_path, generated.render_document().expose_secret())?;
    drop(generated);
    let retained = load_bootstrap_credential_file(&bootstrap_path)?;

    let mut process = ServerProcess::spawn(&database, &capability_keys, &idempotency_keys, &arm)?;
    let address = process.wait_for_ready_address()?;
    let mut client = connect(address).await?;

    rpc(
        "bootstrap capability creation",
        client.create_bootstrap_capability(
            bootstrap_request(&retained)?,
            &bootstrap_metadata(&retained)?,
        ),
    )
    .await?;
    let authenticated =
        CallMetadata::authenticated(BearerCredential::new(bootstrap_token_text(&retained)?)?);
    let deployment = rpc(
        "contract deployment",
        client.deploy_contract(
            v1::DeployContractRequest {
                request_id: request_id()?,
                source: BUDGET_CONTRACT.to_owned(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            },
            &authenticated,
        ),
    )
    .await?;
    if !matches!(
        deployment.result,
        Some(v1::deploy_contract_response::Result::Activated(_))
    ) {
        return Err(test_failure("fresh contract deployment did not activate"));
    }

    write_arm_file(&arm)?;
    let command = |key: &str, organization_id: [u8; 16]| CreateBudget {
        idempotency_key: key.to_owned(),
        organization_id,
        fiscal_year: 2027,
        approved_amount: Amount::from_minor_units(10_000)
            .expect("the fixed amount fits the generated decimal"),
    };
    let mut first = client.clone();
    let mut later = client;
    let first_command = command("writer-panic-first", [0x11; 16]);
    let later_command = command("writer-panic-later", [0x22; 16]);
    let (first_result, later_result) = tokio::join!(
        first.execute_generated(&first_command, one_attempt(), &authenticated),
        later.execute_generated(&later_command, one_attempt(), &authenticated),
    );

    let first_error = first_result
        .err()
        .ok_or_else(|| test_failure("the panicking writer acknowledged the first command"))?;
    let later_error = later_result
        .err()
        .ok_or_else(|| test_failure("the stopped coordinator acknowledged the later command"))?;
    if !is_typed_unavailable(&first_error) || !is_typed_unavailable(&later_error) {
        return Err(test_failure(format!(
            "writer panic did not return typed unavailability: first={first_error:?}, later={later_error:?}"
        )));
    }

    let status = process.wait_for_exit(PROCESS_STOP_TIMEOUT)?;
    if status.success() {
        return Err(test_failure(
            "riffdbd exited successfully after its writer panicked",
        ));
    }
    let diagnostics = process.stderr_lines();
    if !diagnostics
        .iter()
        .any(|line| line.contains("riffdb-command-writer"))
    {
        return Err(test_failure(
            "riffdbd diagnostics did not attribute the panic to its writer thread",
        ));
    }
    Ok(())
}

fn is_typed_unavailable(error: &GeneratedExecutionError) -> bool {
    let GeneratedExecutionError::Client(error) = error else {
        return false;
    };
    matches!(
        error,
        ClientError::Public(public) if public.kind() == PublicErrorKind::StorageUnavailable
    ) || matches!(
        error,
        ClientError::Application(application)
            if application.code() == ApplicationErrorCode::StorageUnavailable
    )
}

async fn rpc<T>(
    label: &str,
    future: impl Future<Output = Result<T, ClientError>>,
) -> TestResult<T> {
    match timeout(RPC_TIMEOUT, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(test_failure(format!("{label} failed: {error:?}"))),
        Err(_) => Err(test_failure(format!("{label} exceeded its deadline"))),
    }
}

async fn connect(address: SocketAddr) -> TestResult<RiffDbClient> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    rpc("gRPC connection", RiffDbClient::connect(endpoint)).await
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
        request_id: request_id()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "writer-fail-fast-maintainer".to_owned(),
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
                    permission: Some(Permission::ReadHealth(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::AdministerCapabilities(v1::Unit {})),
                },
            ],
            field_visibility: vec![v1::EntityFieldVisibility {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                entity_type_id: 1,
                field_ids: vec![1, 2, 3, 4, 5],
                secret_field_ids: Vec::new(),
            }],
            max_scan_rows: 100,
            approval_required: Vec::new(),
            row_policy: None,
            export: None,
            reimport: None,
            vector_inspection: None,
        }),
    })
}

fn bootstrap_metadata(
    credential: &RetainedBootstrapCredential,
) -> TestResult<BootstrapCallMetadata> {
    Ok(BootstrapCallMetadata::new(
        TransportBootstrapCredential::new(bootstrap_token_text(credential)?)?,
    ))
}

fn bootstrap_token_text(credential: &RetainedBootstrapCredential) -> TestResult<&str> {
    str::from_utf8(credential.token().expose_secret()).map_err(Into::into)
}

fn request_id() -> TestResult<Vec<u8>> {
    Ok(generate_request_id()?.into_bytes().to_vec())
}

fn one_attempt() -> AttemptBudget {
    AttemptBudget::new(1).expect("one is a nonzero submission bound")
}

fn write_arm_file(path: &Path) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(b"armed\n")?;
    file.sync_all()
}

fn write_protected_file(path: &Path, document: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(document)?;
    file.sync_all()?;
    File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("secret path has no parent"))?,
    )?
    .sync_all()
}

fn test_failure(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}

enum ReaperCommand {
    Kill,
}

struct ServerProcess {
    ready: Receiver<io::Result<String>>,
    commands: SyncSender<ReaperCommand>,
    exited: Receiver<io::Result<ExitStatus>>,
    reaper: Option<JoinHandle<()>>,
    stdout: Option<JoinHandle<usize>>,
    stderr: Option<JoinHandle<usize>>,
    retained_stderr: Arc<Mutex<Vec<String>>>,
    exit_observed: bool,
}

impl ServerProcess {
    fn spawn(
        database: &Path,
        capability_keys: &Path,
        idempotency_keys: &Path,
        arm: &Path,
    ) -> io::Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_riffdbd"));
        command
            .arg("--database")
            .arg(database)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--environment")
            .arg(ENVIRONMENT)
            .arg("--audience")
            .arg(AUDIENCE)
            .arg("--backup-root")
            .arg(
                database
                    .parent()
                    .ok_or_else(|| io::Error::other("database has no parent"))?
                    .join("backups"),
            )
            .arg("--capability-keys")
            .arg(capability_keys)
            .arg("--idempotency-keys")
            .arg(idempotency_keys)
            .env(WRITER_FAILPOINT, "after_command_dispatch")
            .env(WRITER_FAILPOINT_ARM, arm)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("stdout missing"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("stderr missing"))?;
        let (ready_tx, ready) = mpsc::sync_channel(1);
        let stdout = thread::spawn(move || read_ready_then_drain(stdout, ready_tx));
        let retained_stderr = Arc::new(Mutex::new(Vec::new()));
        let stderr_lines = Arc::clone(&retained_stderr);
        let stderr = thread::spawn(move || drain_stderr(stderr, stderr_lines));
        let (commands, reaper_commands) = mpsc::sync_channel(1);
        let (exit_tx, exited) = mpsc::sync_channel(1);
        let reaper = thread::spawn(move || reap_child(child, reaper_commands, exit_tx));
        Ok(Self {
            ready,
            commands,
            exited,
            reaper: Some(reaper),
            stdout: Some(stdout),
            stderr: Some(stderr),
            retained_stderr,
            exit_observed: false,
        })
    }

    fn wait_for_ready_address(&self) -> TestResult<SocketAddr> {
        let line = self
            .ready
            .recv_timeout(PROCESS_START_TIMEOUT)
            .map_err(|_| test_failure("riffdbd readiness timed out"))??;
        line.strip_prefix(READY_PREFIX)
            .ok_or_else(|| test_failure("unknown readiness line"))?
            .parse()
            .map_err(Into::into)
    }

    fn wait_for_exit(&mut self, deadline: Duration) -> TestResult<ExitStatus> {
        match self.exited.recv_timeout(deadline) {
            Ok(result) => {
                self.exit_observed = true;
                self.join_threads()?;
                result.map_err(Into::into)
            }
            Err(_) => Err(test_failure(
                "riffdbd did not exit within its drain deadline",
            )),
        }
    }

    fn stderr_lines(&self) -> Vec<String> {
        self.retained_stderr
            .lock()
            .map(|lines| lines.clone())
            .unwrap_or_default()
    }

    fn join_threads(&mut self) -> TestResult<()> {
        self.reaper
            .take()
            .ok_or_else(|| test_failure("process reaper already joined"))?
            .join()
            .map_err(|_| test_failure("process reaper panicked"))?;
        self.stdout
            .take()
            .ok_or_else(|| test_failure("stdout reader already joined"))?
            .join()
            .map_err(|_| test_failure("stdout reader panicked"))?;
        self.stderr
            .take()
            .ok_or_else(|| test_failure("stderr reader already joined"))?
            .join()
            .map_err(|_| test_failure("stderr reader panicked"))?;
        Ok(())
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if !self.exit_observed {
            let _ = self.commands.send(ReaperCommand::Kill);
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
    let bytes = line.as_ref().map_or(0, String::len);
    let _ = ready.send(line);
    bytes.saturating_add(io::copy(&mut reader, &mut io::sink()).unwrap_or(0) as usize)
}

fn read_bounded_line(reader: &mut impl Read, maximum: usize) -> io::Result<String> {
    let mut bytes = Vec::with_capacity(maximum.min(64));
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte)? {
            0 => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            1 if byte[0] == b'\n' => break,
            1 if bytes.len() < maximum => bytes.push(byte[0]),
            1 => return Err(io::Error::other("readiness line exceeded its bound")),
            _ => return Err(io::Error::other("invalid bounded read count")),
        }
    }
    String::from_utf8(bytes).map_err(|_| io::Error::other("readiness was not UTF-8"))
}

fn drain_stderr(stderr: ChildStderr, retained: Arc<Mutex<Vec<String>>>) -> usize {
    let mut bytes = 0_usize;
    for line in BufReader::new(stderr).lines() {
        let Ok(line) = line else { break };
        bytes = bytes.saturating_add(line.len());
        if let Ok(mut lines) = retained.lock()
            && lines.len() < MAX_RETAINED_STDERR_LINES
        {
            lines.push(line);
        }
    }
    bytes
}
