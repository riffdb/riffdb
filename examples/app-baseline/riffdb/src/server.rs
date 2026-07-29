//! Process harness that starts a real `riffdbd` for the app baseline.

use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_client_rust::{
    BearerCredential, BootstrapCallMetadata, BootstrapCredential as TransportBootstrapCredential,
    CallMetadata, RiffDbClient, app_v1, generate_capability_id, generate_request_id, v1,
};
use tokio::time::timeout;
use tonic::transport::Endpoint;

use crate::{RiffDbError, RiffDbPublicBackend};

const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "app-baseline";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const PROCESS_START_TIMEOUT: Duration = Duration::from_secs(30);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(15);
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_000_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;
const CONTRACT_LINEAGE: &str = "TicketDesk";
const CONTRACT_VERSION: u64 = 1;
const TICKETDESK_CONTRACT: &str = include_str!("../../contracts/ticketdesk.riff");
const MODULE_NAME: &str = "ticketdesk";
const MODULE_VERSION: u64 = 1;
const QUERY_SOURCES: &[(&str, &str)] = &[
    (
        "GetTicket",
        include_str!("../../../../queries/ticketdesk/get_ticket.riffq"),
    ),
    (
        "GetUser",
        include_str!("../../../../queries/ticketdesk/get_user.riffq"),
    ),
    (
        "ListComments",
        include_str!("../../../../queries/ticketdesk/list_comments.riffq"),
    ),
    (
        "ListTickets",
        include_str!("../../../../queries/ticketdesk/list_tickets.riffq"),
    ),
    (
        "ListTicketsByAssignee",
        include_str!("../../../../queries/ticketdesk/list_tickets_by_assignee.riffq"),
    ),
    (
        "ProjectMembers",
        include_str!("../../../../queries/ticketdesk/project_members.riffq"),
    ),
    (
        "ProjectSummary",
        include_str!("../../../../queries/ticketdesk/project_summary.riffq"),
    ),
    (
        "TicketPage",
        include_str!("../../../../queries/ticketdesk/ticket_page.riffq"),
    ),
];
const CAPABILITY_KEY_DOCUMENT: &[u8] =
    b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] =
    b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";

static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

/// Owns one live `riffdbd` process and a ready public client backend.
pub struct RiffDbServerSession {
    _temporary: TemporaryDirectory,
    process: ServerProcess,
    /// Public application backend.
    pub backend: RiffDbPublicBackend,
}

impl RiffDbServerSession {
    /// Spawns `riffdbd`, bootstraps, deploys TicketDesk + query module, issues a runner capability.
    pub async fn start(riffdbd_bin: &Path) -> Result<Self, RiffDbError> {
        let temporary = TemporaryDirectory::new().map_err(|_| RiffDbError::Io)?;
        let database_path = temporary.path().join("riffdb.redb");
        let backup_root = temporary.path().join("backups");
        let capability_keys_path = temporary.path().join("capability.keys");
        let idempotency_keys_path = temporary.path().join("idempotency.keys");
        let bootstrap_path = temporary.path().join("bootstrap.credential");

        write_protected_file(&capability_keys_path, CAPABILITY_KEY_DOCUMENT)
            .map_err(|_| RiffDbError::Io)?;
        write_protected_file(&idempotency_keys_path, IDEMPOTENCY_KEY_DOCUMENT)
            .map_err(|_| RiffDbError::Io)?;
        let generated = generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)
            .map_err(|_| RiffDbError::Bootstrap)?;
        write_protected_file(&bootstrap_path, generated.render_document().expose_secret())
            .map_err(|_| RiffDbError::Io)?;
        drop(generated);
        let retained =
            load_bootstrap_credential_file(&bootstrap_path).map_err(|_| RiffDbError::Bootstrap)?;

        let process = ServerProcess::spawn(
            riffdbd_bin,
            &database_path,
            &backup_root,
            &capability_keys_path,
            &idempotency_keys_path,
        )
        .map_err(|_| RiffDbError::Server)?;
        let address = process
            .wait_for_ready_address()
            .map_err(|_| RiffDbError::Server)?;
        let endpoint = format!("http://{address}");
        let mut client = connect(&endpoint).await?;
        let token = bootstrap_deploy_and_issue(&mut client, &retained).await?;
        let backend = RiffDbPublicBackend::connect(&endpoint, &token).await?;
        Ok(Self {
            _temporary: temporary,
            process,
            backend,
        })
    }

    /// Stops the server cleanly.
    pub fn shutdown(mut self) -> Result<(), RiffDbError> {
        self.process.shutdown_cleanly().map_err(|_| RiffDbError::Server)
    }
}

async fn bootstrap_deploy_and_issue(
    client: &mut RiffDbClient,
    credential: &RetainedBootstrapCredential,
) -> Result<String, RiffDbError> {
    let token = std::str::from_utf8(credential.token().expose_secret())
        .map_err(|_| RiffDbError::Bootstrap)?;
    let bootstrap_metadata = BootstrapCallMetadata::new(
        TransportBootstrapCredential::new(token).map_err(|_| RiffDbError::Bootstrap)?,
    );
    let authenticated = CallMetadata::authenticated(
        BearerCredential::new(token).map_err(|_| RiffDbError::Bootstrap)?,
    );

    let created = bounded_rpc(
        "bootstrap_create_capability",
        client.create_bootstrap_capability(bootstrap_request(credential)?, &bootstrap_metadata),
    )
    .await?;
    let Some(v1::create_capability_response::Result::Bootstrap(result)) = created.result else {
        return Err(RiffDbError::Rpc(
            "bootstrap response was not Bootstrap/Created".into(),
        ));
    };
    let Some(v1::bootstrap_create_capability_result::Result::Created(_)) = result.result else {
        return Err(RiffDbError::Rpc(
            "bootstrap capability was not newly created".into(),
        ));
    };

    let deployed = bounded_rpc(
        "deploy_contract",
        client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: TICKETDESK_CONTRACT.to_owned(),
                expected_active_version: None,
            },
            &authenticated,
        ),
    )
    .await?;
    let Some(v1::deploy_contract_response::Result::Activated(contract)) = deployed.result else {
        return Err(RiffDbError::Rpc(format!(
            "deploy_contract did not activate: {deployed:?}"
        )));
    };
    if contract.contract_lineage != CONTRACT_LINEAGE
        || contract.contract_version != CONTRACT_VERSION
    {
        return Err(RiffDbError::Rpc(format!(
            "unexpected contract identity {}/{}",
            contract.contract_lineage, contract.contract_version
        )));
    }

    let module = bounded_rpc(
        "deploy_query_module",
        client.deploy_query_module(
            app_v1::DeployQueryModuleRequest {
                contract: Some(app_v1::ContractSelector {
                    lineage: CONTRACT_LINEAGE.to_owned(),
                    version: CONTRACT_VERSION,
                    bundle_hash: Vec::new(),
                }),
                module_name: MODULE_NAME.to_owned(),
                module_version: MODULE_VERSION,
                queries: QUERY_SOURCES
                    .iter()
                    .map(|(name, source)| app_v1::NamedQuerySource {
                        name: (*name).to_owned(),
                        source: (*source).to_owned(),
                    })
                    .collect(),
                request_id: fresh_request_id_bytes()?,
                expected_active: Some(
                    app_v1::deploy_query_module_request::ExpectedActive::AnyActive(true),
                ),
            },
            &authenticated,
        ),
    )
    .await
    .map_err(|error| RiffDbError::Rpc(format!("deploy_query_module: {error}")))?;
    let Some(module) = module.module else {
        return Err(RiffDbError::Rpc(format!(
            "deploy_query_module did not activate: {module:?}"
        )));
    };

    let response = bounded_rpc(
        "create_runner_capability",
        client.create_capability(
            normal_capability_request(&module.module_hash)?,
            &authenticated,
        ),
    )
    .await?;
    let Some(v1::create_capability_response::Result::Normal(result)) = response.result else {
        return Err(RiffDbError::Rpc(
            "runner capability response was not Normal".into(),
        ));
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = result.result else {
        return Err(RiffDbError::Rpc(
            "runner capability was not created".into(),
        ));
    };
    Ok(created.token)
}

fn bootstrap_request(
    credential: &RetainedBootstrapCredential,
) -> Result<v1::CreateCapabilityRequest, RiffDbError> {
    use v1::capability_permission::Permission;
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "app-baseline-bootstrap".to_owned(),
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

fn normal_capability_request(module_hash: &[u8]) -> Result<v1::CreateCapabilityRequest, RiffDbError> {
    use v1::capability_permission::Permission;
    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: CONTRACT_LINEAGE.to_owned(),
        stable_id,
    };
    if module_hash.len() != 32 {
        return Err(RiffDbError::Rpc(
            "deployed query module returned an invalid identity".into(),
        ));
    }
    // The stable application profile contains only exact command and immutable
    // named-query permissions. Compiler-derived entity/index access never
    // becomes reusable kernel authority.
    let mut permissions = Vec::new();
    for stable_id in 1_u32..=8 {
        permissions.push(v1::CapabilityPermission {
            permission: Some(Permission::InvokeCommand(scoped(stable_id))),
        });
    }
    for query_name in [
        "GetUser",
        "GetTicket",
        "TicketPage",
        "ListTickets",
        "ListComments",
        "ProjectMembers",
        "ProjectSummary",
        "ListTicketsByAssignee",
    ] {
        permissions.push(v1::CapabilityPermission {
            permission: Some(Permission::ExecuteNamedQuery(v1::NamedQueryPermission {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                query_module_hash: module_hash.to_vec(),
                query_name: query_name.to_owned(),
            })),
        });
    }

    // Non-key field visibility required for policy-filtered application data.
    let field_visibility = vec![
        field_visibility(1, &[1, 3]),
        field_visibility(2, &[1, 2, 4, 5, 6, 7, 8]),
        field_visibility(3, &[1, 3, 4]),
        field_visibility(4, &[1, 2, 3, 5]),
        field_visibility(5, &[1, 2]),
        field_visibility(6, &[3]),
        field_visibility(7, &[1, 2]),
        field_visibility(8, &[1, 3]),
    ];

    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Normal as i32,
        capability_id: generate_capability_id()
            .map_err(|_| RiffDbError::Bootstrap)?
            .into_bytes()
            .to_vec(),
        principal_id: "app-baseline-runner".to_owned(),
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
            field_visibility,
            max_scan_rows: 500,
            approval_required: Vec::new(),
        }),
    })
}

fn field_visibility(entity_type_id: u32, field_ids: &[u32]) -> v1::EntityFieldVisibility {
    v1::EntityFieldVisibility {
        contract_lineage: CONTRACT_LINEAGE.to_owned(),
        entity_type_id,
        field_ids: field_ids.to_vec(),
    }
}

async fn connect(endpoint: &str) -> Result<RiffDbClient, RiffDbError> {
    let endpoint = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|_| RiffDbError::Connection)?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    bounded_rpc("connect", RiffDbClient::connect(endpoint)).await
}

async fn bounded_rpc<T, E>(
    step: &str,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, RiffDbError>
where
    E: std::fmt::Display,
{
    match timeout(RPC_TIMEOUT, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(RiffDbError::Rpc(format!("{step}: {error}"))),
        Err(_) => Err(RiffDbError::Rpc(format!("{step}: timeout"))),
    }
}

fn fresh_request_id_bytes() -> Result<Vec<u8>, RiffDbError> {
    Ok(generate_request_id()
        .map_err(|_| RiffDbError::Bootstrap)?
        .into_bytes()
        .to_vec())
}

fn write_protected_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new() -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "riffdb-app-baseline-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
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
    stdin: Option<std::process::ChildStdin>,
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
        let stdin = child.stdin.take().ok_or_else(|| io::Error::other("stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| io::Error::other("stdout"))?;
        let stderr = child.stderr.take().ok_or_else(|| io::Error::other("stderr"))?;
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

    fn wait_for_ready_address(&self) -> io::Result<SocketAddr> {
        let line = match self.ready.recv_timeout(PROCESS_START_TIMEOUT) {
            Ok(result) => result?,
            Err(RecvTimeoutError::Timeout) => {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "ready timeout"));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(io::Error::other("ready disconnected"));
            }
        };
        let address = line
            .strip_prefix(READY_PREFIX)
            .ok_or_else(|| io::Error::other("bad ready line"))?;
        address
            .parse()
            .map_err(|_| io::Error::other("bad ready address"))
    }

    fn shutdown_cleanly(&mut self) -> io::Result<()> {
        if let Some(mut stdin) = self.stdin.take() {
            let _ = stdin.write_all(b"shutdown\n");
            let _ = stdin.flush();
        }
        match self.exited.recv_timeout(PROCESS_STOP_TIMEOUT) {
            Ok(status) => {
                self.exit_observed = true;
                let status = status?;
                if status.success() {
                    Ok(())
                } else {
                    Err(io::Error::other(format!("exit {status}")))
                }
            }
            Err(_) => {
                let _ = self.reaper_commands.send(ReaperCommand::Kill);
                Err(io::Error::other("shutdown timeout"))
            }
        }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if !self.exit_observed {
            let _ = self.reaper_commands.send(ReaperCommand::Kill);
        }
        if let Some(handle) = self.reaper.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.stdout.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.stderr.take() {
            let _ = handle.join();
        }
    }
}

fn read_ready_then_drain(
    stream: impl Read,
    ready_sender: SyncSender<io::Result<String>>,
) -> usize {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let ready = reader.read_line(&mut line).map(|_| line.trim_end().to_owned());
    let _ = ready_sender.send(ready);
    drain_stream(reader)
}

fn drain_stream(mut stream: impl Read) -> usize {
    let mut total = 0_usize;
    let mut buffer = [0_u8; 4_096];
    while let Ok(read) = stream.read(&mut buffer) {
        if read == 0 {
            break;
        }
        total = total.saturating_add(read);
    }
    total
}

fn reap_child(
    mut child: Child,
    commands: Receiver<ReaperCommand>,
    exit_sender: SyncSender<io::Result<ExitStatus>>,
) {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let _ = exit_sender.send(Ok(status));
                return;
            }
            Ok(None) => match commands.recv_timeout(Duration::from_millis(10)) {
                Ok(ReaperCommand::Kill) => {
                    let _ = child.kill();
                    let _ = exit_sender.send(child.wait());
                    return;
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return;
                }
            },
            Err(error) => {
                let _ = exit_sender.send(Err(error));
                return;
            }
        }
    }
}
