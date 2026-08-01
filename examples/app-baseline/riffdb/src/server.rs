//! Process harness that starts a real `riffdbd` for the app baseline.

use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
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
use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    ApplicationManifest, CompiledApplicationRole, NamedQuerySource, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion, compile_application_role,
};
use riffdb_types::{CapabilityGrantV1, CapabilityPermissionV1, PartitionScopeV1, TenantScope};
use tokio::time::timeout;
use tonic::transport::Endpoint;

use crate::{RiffDbError, RiffDbPublicBackend};

const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "app-baseline";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const WRITE_GROUP_PREFIX: &str = "riffdb-write-completion-groups-v1\t";
const PROCESS_START_TIMEOUT: Duration = Duration::from_secs(30);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(15);
/// Keep the last N stderr lines for crash diagnosis (panic / OOM messages).
const STDERR_RING_LINES: usize = 200;
/// Soft byte cap for the retained stderr ring (in addition to the line cap).
const STDERR_RING_BYTES: usize = 64 * 1024;
const STDERR_DETAIL_CHARS: usize = 4_000;
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_000_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;
const CONTRACT_LINEAGE: &str = "TicketDesk";
const CONTRACT_VERSION: u64 = 1;
const APPLICATION_ROLE_NAME: &str = "TicketDeskApplication";
const TICKETDESK_CONTRACT: &str = include_str!("../../contracts/ticketdesk.riff");
const TICKETDESK_MANIFEST: &str =
    include_str!("../../../../fixtures/application-manifests/ticketdesk-v1.json");
const MODULE_NAME: &str = "ticketdesk";
const MODULE_VERSION: u64 = 1;
const QUERY_SOURCES: &[(&str, &str)] = &[
    (
        "BoardPage",
        include_str!("../../../../queries/ticketdesk/board_page.riffq"),
    ),
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

/// Env override for the on-disk app-baseline database root (not `/tmp`).
///
/// Prefer [`riffdb_bench_root::BENCH_DB_ROOT_ENV`] (`RIFFDB_BENCH_DB_ROOT`); this
/// legacy name remains as a secondary fallback for one transition release.
pub const DATABASE_ROOT_ENV: &str = "RIFFDB_APP_BASELINE_DB_ROOT";
/// Default database root under the repo (real disk; gitignored via `/target/`).
pub const DEFAULT_DATABASE_ROOT: &str = "target/perf-db/app-baseline";
/// Minimum free bytes required before spawning `riffdbd` in smoke mode (256 MiB).
pub const MIN_FREE_BYTES_SMOKE: u64 = 256 * 1024 * 1024;
/// Minimum free bytes required for evidentiary `--full` runs (8 GiB).
pub const MIN_FREE_BYTES_FULL: u64 = 8 * 1024 * 1024 * 1024;
/// Backward-compatible alias for smoke floor.
pub const MIN_FREE_BYTES: u64 = MIN_FREE_BYTES_SMOKE;

/// Free-space floor for a harness mode.
#[must_use]
pub const fn min_free_bytes_for_full(full: bool) -> u64 {
    if full {
        MIN_FREE_BYTES_FULL
    } else {
        MIN_FREE_BYTES_SMOKE
    }
}

/// Optional process overrides for a baseline `riffdbd` session.
#[derive(Clone, Debug, Default)]
pub struct ServerStartOptions {
    /// When set, exported as `RIFFDB_P1_COORDINATOR_WORKLOAD_CAPACITY` for the child.
    ///
    /// Used by `--load-saturate` so the live profile can reach typed overload
    /// under continuous concurrency without shortening the 150 ms admission wait.
    pub coordinator_workload_capacity: Option<u16>,
    /// Root directory for per-run database/session dirs (real disk by default).
    ///
    /// Resolution order: this field → `RIFFDB_BENCH_DB_ROOT` →
    /// `RIFFDB_APP_BASELINE_DB_ROOT` → [`DEFAULT_DATABASE_ROOT`].
    /// Does **not** use `/tmp` (often a small ramdisk) unless `allow_tmpfs`.
    pub database_root: Option<PathBuf>,
    /// Permit resolving onto tmpfs/ramfs (tests only; never for published numbers).
    pub allow_tmpfs: bool,
    /// Free-space floor before spawn (smoke 256 MiB / full 8 GiB).
    ///
    /// `0` means use [`MIN_FREE_BYTES_SMOKE`].
    pub min_free_bytes: u64,
}

/// Owns one live `riffdbd` process and a ready public client backend.
pub struct RiffDbServerSession {
    _temporary: riffdb_bench_root::BenchDir,
    process: ServerProcess,
    /// Resolved real-disk root used for this session.
    pub bench_root: riffdb_bench_root::BenchRoot,
    /// Public application backend.
    pub backend: RiffDbPublicBackend,
}

impl RiffDbServerSession {
    /// Spawns `riffdbd`, bootstraps, deploys TicketDesk + query module, issues a runner capability.
    pub async fn start(riffdbd_bin: &Path) -> Result<Self, RiffDbError> {
        Self::start_with_options(riffdbd_bin, ServerStartOptions::default()).await
    }

    /// Like [`start`](Self::start) with optional process overrides (saturation capacity).
    pub async fn start_with_options(
        riffdbd_bin: &Path,
        options: ServerStartOptions,
    ) -> Result<Self, RiffDbError> {
        let bench_root = resolve_bench_root(&options).map_err(|error| RiffDbError::Server {
            detail: error.to_string(),
        })?;
        let _ = riffdb_bench_root::sweep_stale(&bench_root);
        let temporary =
            riffdb_bench_root::BenchDir::create(&bench_root, "app-baseline").map_err(|error| {
                RiffDbError::Server {
                    detail: error.to_string(),
                }
            })?;
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
            options.coordinator_workload_capacity,
        )
        .map_err(|error| RiffDbError::Server {
            detail: error.to_string(),
        })?;
        let address = process.wait_for_ready_address().map_err(|error| {
            let detail = process.diagnostic_detail(&error.to_string());
            RiffDbError::Server { detail }
        })?;
        let endpoint = format!("http://{address}");
        let mut client = connect(&endpoint).await?;
        let token = bootstrap_deploy_and_issue(&mut client, &retained).await?;
        let backend = RiffDbPublicBackend::connect(&endpoint, &token).await?;
        Ok(Self {
            _temporary: temporary,
            process,
            bench_root,
            backend,
        })
    }

    /// Stops the server cleanly.
    pub fn shutdown(mut self) -> Result<[u64; 64], RiffDbError> {
        self.process
            .shutdown_cleanly()
            .map_err(|error| RiffDbError::Server {
                detail: error.to_string(),
            })
    }

    /// Last stderr lines captured from `riffdbd` (for crash diagnosis).
    #[must_use]
    pub fn stderr_tail(&self) -> String {
        self.process.stderr_tail()
    }

    /// Non-blocking liveness check: `Ok` while the child is still running.
    pub fn server_alive(&mut self) -> Result<(), String> {
        match self.process.poll_exit() {
            None => Ok(()),
            Some(Ok(status)) => {
                let mut detail = format!("riffdbd exited mid-load: {status}");
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    if let Some(signal) = status.signal() {
                        detail.push_str(&format!(" signal={signal}"));
                    }
                }
                let tail = self.process.stderr_tail();
                if !tail.is_empty() {
                    detail.push_str("; stderr_tail=\n");
                    detail.push_str(&tail);
                }
                detail.push_str(&format!(
                    "; database_root={} free_bytes_at_start={}",
                    self.bench_root.path().display(),
                    self.bench_root.free_bytes_at_start()
                ));
                Err(detail)
            }
            Some(Err(error)) => Err(format!(
                "riffdbd wait error mid-load: {error}; database_root={} free_bytes_at_start={}",
                self.bench_root.path().display(),
                self.bench_root.free_bytes_at_start()
            )),
        }
    }

    /// OS process id of the `riffdbd` child (for integration kill tests).
    #[must_use]
    pub fn child_pid(&self) -> u32 {
        self.process.child_id
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
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
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

    // Compile the exact TicketDeskApplication role from the same sources that
    // were deployed. Field visibility, command IDs, and scan ceilings remain
    // compiler-private consequences of named operations (WP-310).
    let role = compile_ticketdesk_application_role()?;
    let expected_module_hash = role
        .module_hashes()
        .first()
        .map(|hash| hash.as_bytes())
        .ok_or(RiffDbError::Deploy)?;
    if module.module_hash.as_slice() != expected_module_hash.as_slice() {
        return Err(RiffDbError::Rpc(
            "deployed query-module hash does not match compiled role module identity".into(),
        ));
    }

    let response = bounded_rpc(
        "create_runner_capability",
        client.create_capability(
            role_capability_request(role.internal_grant())?,
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
        return Err(RiffDbError::Rpc("runner capability was not created".into()));
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

/// Compiles the checked-in TicketDesk application role from the same contract
/// and query sources the harness deploys to `riffdbd`.
fn compile_ticketdesk_application_role() -> Result<CompiledApplicationRole, RiffDbError> {
    let manifest = ApplicationManifest::decode_canonical(TICKETDESK_MANIFEST.as_bytes())
        .map_err(|_| RiffDbError::Deploy)?;
    let contract = compile_contract_source(TICKETDESK_CONTRACT).map_err(|_| RiffDbError::Deploy)?;
    let queries = QUERY_SOURCES
        .iter()
        .map(|(name, source)| {
            NamedQuerySource::new(*name, *source).map_err(|_| RiffDbError::Deploy)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new(MODULE_NAME).map_err(|_| RiffDbError::Deploy)?,
        QueryModuleVersion::new(MODULE_VERSION).ok_or(RiffDbError::Deploy)?,
        queries,
    )
    .map_err(|_| RiffDbError::Deploy)?;
    let module = QueryModule::compile(candidate, &contract).map_err(|_| RiffDbError::Deploy)?;
    compile_application_role(&manifest, APPLICATION_ROLE_NAME, None, &contract, &[module])
        .map_err(|_| RiffDbError::Deploy)
}

/// Lowers a compiler-private role grant into the public create-capability request.
fn role_capability_request(
    grant: &CapabilityGrantV1,
) -> Result<v1::CreateCapabilityRequest, RiffDbError> {
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Normal as i32,
        capability_id: generate_capability_id()
            .map_err(|_| RiffDbError::Bootstrap)?
            .into_bytes()
            .to_vec(),
        principal_id: "ticketdesk-application".to_owned(),
        actor_kind: v1::ActorKind::Service as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
        audiences: vec![AUDIENCE.to_owned()],
        grant: Some(application_role_grant_to_proto(grant)?),
    })
}

fn application_role_grant_to_proto(
    grant: &CapabilityGrantV1,
) -> Result<v1::CapabilityGrant, RiffDbError> {
    let tenant_scope = match grant.tenant_scope() {
        TenantScope::Global => v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
        },
        TenantScope::Tenant(tenant) => v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::TenantId(
                tenant.as_str().to_owned(),
            )),
        },
    };
    let partition_scope = match grant.partition_scope() {
        PartitionScopeV1::All => v1::PartitionScope {
            scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
        },
        PartitionScopeV1::Explicit(_) => {
            return Err(RiffDbError::Rpc(
                "application role lowered to explicit partition scope".into(),
            ));
        }
    };
    let permissions = grant
        .permissions()
        .as_slice()
        .iter()
        .map(application_role_permission_to_proto)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(v1::CapabilityGrant {
        tenant_scope: Some(tenant_scope),
        partition_scope: Some(partition_scope),
        permissions,
        field_visibility: grant
            .field_visibility()
            .iter()
            .map(|visibility| v1::EntityFieldVisibility {
                contract_lineage: visibility.lineage().as_str().to_owned(),
                entity_type_id: visibility.entity_type().get(),
                field_ids: visibility
                    .fields()
                    .iter()
                    .map(|field| field.get())
                    .collect(),
            })
            .collect(),
        max_scan_rows: u32::from(grant.max_scan_rows().get()),
        approval_required: Vec::new(),
    })
}

fn application_role_permission_to_proto(
    permission: &CapabilityPermissionV1,
) -> Result<v1::CapabilityPermission, RiffDbError> {
    use v1::capability_permission::Permission;
    let permission = match permission {
        CapabilityPermissionV1::InvokeCommand(lineage, command) => {
            Permission::InvokeCommand(v1::LineageScopedStableId {
                contract_lineage: lineage.as_str().to_owned(),
                stable_id: command.get(),
            })
        }
        CapabilityPermissionV1::ExecuteNamedQuery(lineage, module_hash, query_name) => {
            Permission::ExecuteNamedQuery(v1::NamedQueryPermission {
                contract_lineage: lineage.as_str().to_owned(),
                query_module_hash: module_hash.as_bytes().to_vec(),
                query_name: query_name.as_str().to_owned(),
            })
        }
        CapabilityPermissionV1::ApplicationRoleIdentity(role_hash) => {
            Permission::ApplicationRoleIdentity(role_hash.as_bytes().to_vec())
        }
        _ => {
            return Err(RiffDbError::Rpc(
                "application role compiler emitted non-application authority".into(),
            ));
        }
    };
    Ok(v1::CapabilityPermission {
        permission: Some(permission),
    })
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

/// Resolves the on-disk root for app-baseline `riffdbd` session directories.
///
/// Order: explicit `override_root` → `RIFFDB_BENCH_DB_ROOT` →
/// `RIFFDB_APP_BASELINE_DB_ROOT` → [`DEFAULT_DATABASE_ROOT`].
/// Never falls back to `std::env::temp_dir()`.
#[must_use]
pub fn resolve_database_root(override_root: Option<&Path>) -> PathBuf {
    if let Some(path) = override_root {
        return path.to_path_buf();
    }
    if let Some(from_env) = std::env::var_os(riffdb_bench_root::BENCH_DB_ROOT_ENV)
        && !from_env.is_empty()
    {
        return PathBuf::from(from_env);
    }
    if let Some(from_env) = std::env::var_os(DATABASE_ROOT_ENV)
        && !from_env.is_empty()
    {
        return PathBuf::from(from_env);
    }
    PathBuf::from(DEFAULT_DATABASE_ROOT)
}

/// Resolves a classified [`riffdb_bench_root::BenchRoot`] for session dirs.
#[allow(clippy::result_large_err)]
pub fn resolve_bench_root(
    options: &ServerStartOptions,
) -> Result<riffdb_bench_root::BenchRoot, riffdb_bench_root::BenchRootError> {
    let default_root = if let Some(legacy) = std::env::var_os(DATABASE_ROOT_ENV) {
        if !legacy.is_empty() && options.database_root.is_none() {
            // Legacy env only fills default when unified env/cli absent; BenchRoot
            // still prefers RIFFDB_BENCH_DB_ROOT over this default.
            PathBuf::from(legacy)
        } else {
            PathBuf::from(DEFAULT_DATABASE_ROOT)
        }
    } else {
        PathBuf::from(DEFAULT_DATABASE_ROOT)
    };
    let min_free_bytes = if options.min_free_bytes == 0 {
        MIN_FREE_BYTES_SMOKE
    } else {
        options.min_free_bytes
    };
    riffdb_bench_root::BenchRoot::resolve(riffdb_bench_root::BenchRootOptions {
        harness: "app-baseline",
        cli_override: options.database_root.clone(),
        default_root,
        allow_tmpfs: options.allow_tmpfs,
        min_free_bytes,
    })
}

/// Removes orphaned session dirs left by killed/hung harness runs.
///
/// Prefers [`riffdb_bench_root::sweep_stale_path`] when the root contains
/// `perf-db`; otherwise best-effort removes legacy `riffdb-app-baseline-*`
/// children only (no broad recursive wipe).
pub fn sweep_stale_session_dirs(root: &Path) {
    if riffdb_bench_root::path_has_perf_db_component(root) {
        let _ = riffdb_bench_root::sweep_stale_path(root);
        return;
    }
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !(name.starts_with("riffdb-app-baseline-") || name.starts_with("riffdb-bench-")) {
            continue;
        }
        if path.is_dir() {
            let _ = fs::remove_dir_all(&path);
        }
    }
}

enum ReaperCommand {
    Kill,
}

struct ServerProcess {
    child_id: u32,
    stdin: Option<std::process::ChildStdin>,
    ready: Receiver<io::Result<String>>,
    write_groups: Receiver<io::Result<[u64; 64]>>,
    reaper_commands: SyncSender<ReaperCommand>,
    exited: Receiver<io::Result<ExitStatus>>,
    reaper: Option<JoinHandle<()>>,
    stdout: Option<JoinHandle<usize>>,
    stderr: Option<JoinHandle<usize>>,
    stderr_ring: Arc<Mutex<VecDeque<String>>>,
    exit_observed: bool,
    /// Cached exit once observed; subsequent [`poll_exit`] returns this.
    cached_exit: Option<Result<ExitStatus, String>>,
}

impl ServerProcess {
    fn spawn(
        binary: &Path,
        database_path: &Path,
        backup_root: &Path,
        capability_keys_path: &Path,
        idempotency_keys_path: &Path,
        coordinator_workload_capacity: Option<u16>,
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
            // Surface panics / allocator errors in the harness tail.
            .env("RUST_BACKTRACE", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(capacity) = coordinator_workload_capacity {
            command.env(
                "RIFFDB_P1_COORDINATOR_WORKLOAD_CAPACITY",
                capacity.to_string(),
            );
        }
        let mut child = command.spawn()?;
        let child_id = child.id();
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("stderr"))?;
        let (ready_sender, ready) = mpsc::sync_channel(1);
        let (write_group_sender, write_groups) = mpsc::sync_channel(1);
        let stdout =
            thread::spawn(move || read_server_stdout(stdout, ready_sender, write_group_sender));
        let stderr_ring = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_RING_LINES)));
        let stderr_ring_worker = Arc::clone(&stderr_ring);
        let stderr = thread::spawn(move || drain_server_stderr(stderr, stderr_ring_worker));
        let (reaper_commands, commands) = mpsc::sync_channel(1);
        let (exit_sender, exited) = mpsc::sync_channel(1);
        let reaper = thread::spawn(move || reap_child(child, commands, exit_sender));
        Ok(Self {
            child_id,
            stdin: Some(stdin),
            ready,
            write_groups,
            reaper_commands,
            exited,
            reaper: Some(reaper),
            stdout: Some(stdout),
            stderr: Some(stderr),
            stderr_ring,
            exit_observed: false,
            cached_exit: None,
        })
    }

    fn wait_for_ready_address(&self) -> io::Result<SocketAddr> {
        let line = match self.ready.recv_timeout(PROCESS_START_TIMEOUT) {
            Ok(result) => result?,
            Err(RecvTimeoutError::Timeout) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    self.diagnostic_detail("ready timeout"),
                ));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(io::Error::other(
                    self.diagnostic_detail("ready disconnected"),
                ));
            }
        };
        let address = line
            .strip_prefix(READY_PREFIX)
            .ok_or_else(|| io::Error::other("bad ready line"))?;
        address
            .parse()
            .map_err(|_| io::Error::other("bad ready address"))
    }

    fn stderr_tail(&self) -> String {
        self.stderr_ring
            .lock()
            .map(|ring| ring.iter().cloned().collect::<Vec<_>>().join(""))
            .unwrap_or_default()
    }

    /// Non-blocking poll of the child exit channel (caches observed status).
    ///
    /// Once an exit is observed, subsequent calls return the **same** cached
    /// status so [`RiffDbServerSession::server_alive`] stays truthful.
    fn poll_exit(&mut self) -> Option<io::Result<ExitStatus>> {
        if let Some(cached) = &self.cached_exit {
            return Some(match cached {
                Ok(status) => Ok(*status),
                Err(message) => Err(io::Error::other(message.clone())),
            });
        }
        match self.exited.try_recv() {
            Ok(status) => {
                self.exit_observed = true;
                let cached = status
                    .as_ref()
                    .map(|s| *s)
                    .map_err(|error| error.to_string());
                self.cached_exit = Some(cached);
                Some(status)
            }
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                self.exit_observed = true;
                let message = "riffdbd reaper disconnected".to_owned();
                self.cached_exit = Some(Err(message.clone()));
                Some(Err(io::Error::other(message)))
            }
        }
    }

    fn diagnostic_detail(&self, headline: &str) -> String {
        let tail = self.stderr_tail();
        let mut detail = headline.to_owned();
        if !tail.is_empty() {
            let clipped: String = tail.chars().rev().take(STDERR_DETAIL_CHARS).collect();
            let clipped: String = clipped.chars().rev().collect();
            detail.push_str("; stderr_tail=\n");
            detail.push_str(&clipped);
        }
        detail
    }

    fn shutdown_cleanly(&mut self) -> io::Result<[u64; 64]> {
        if let Some(mut stdin) = self.stdin.take() {
            let _ = stdin.write_all(b"shutdown\n");
            let _ = stdin.flush();
        }
        match self.exited.recv_timeout(PROCESS_STOP_TIMEOUT) {
            Ok(status) => {
                self.exit_observed = true;
                let status = status?;
                self.cached_exit = Some(Ok(status));
                if status.success() {
                    self.write_groups
                        .recv_timeout(PROCESS_STOP_TIMEOUT)
                        .map_err(|_| {
                            io::Error::other(
                                self.diagnostic_detail(
                                    "write-group report missing after clean exit",
                                ),
                            )
                        })?
                } else {
                    let code = status
                        .code()
                        .map(|c| format!("exit_code={c}"))
                        .unwrap_or_else(|| format!("exit_status={status}"));
                    // Unix signal when available.
                    #[cfg(unix)]
                    let code = {
                        use std::os::unix::process::ExitStatusExt;
                        match status.signal() {
                            Some(sig) => format!("{code} signal={sig}"),
                            None => code,
                        }
                    };
                    Err(io::Error::other(self.diagnostic_detail(&code)))
                }
            }
            Err(_) => {
                let _ = self.reaper_commands.send(ReaperCommand::Kill);
                // Give the reaper a moment to deposit an exit status if kill works.
                let after_kill = self.exited.recv_timeout(Duration::from_secs(2));
                let headline = match after_kill {
                    Ok(Ok(status)) => {
                        self.exit_observed = true;
                        format!("shutdown timeout; killed child status={status}")
                    }
                    Ok(Err(error)) => format!("shutdown timeout; kill wait error={error}"),
                    Err(_) => "shutdown timeout; child unresponsive after kill".to_owned(),
                };
                Err(io::Error::other(self.diagnostic_detail(&headline)))
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

fn read_server_stdout(
    stream: impl Read,
    ready_sender: SyncSender<io::Result<String>>,
    write_group_sender: SyncSender<io::Result<[u64; 64]>>,
) -> usize {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let ready = reader
        .read_line(&mut line)
        .map(|_| line.trim_end().to_owned());
    let _ = ready_sender.send(ready);
    let mut total = line.len();
    loop {
        line.clear();
        let Ok(read) = reader.read_line(&mut line) else {
            break;
        };
        if read == 0 {
            break;
        }
        eprint!("{line}");
        total = total.saturating_add(read);
        if let Some(encoded) = line.trim_end().strip_prefix(WRITE_GROUP_PREFIX) {
            let _ = write_group_sender.send(parse_write_groups(encoded));
        }
    }
    total
}

fn parse_write_groups(encoded: &str) -> io::Result<[u64; 64]> {
    let values = encoded
        .split(',')
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| io::Error::other("invalid write-group count"))
        })
        .collect::<io::Result<Vec<_>>>()?;
    values
        .try_into()
        .map_err(|_| io::Error::other("invalid write-group count length"))
}

fn drain_server_stderr(stream: impl Read, ring: Arc<Mutex<VecDeque<String>>>) -> usize {
    let mut reader = BufReader::new(stream);
    let mut total = 0_usize;
    let mut ring_bytes = 0_usize;
    let mut line = String::new();
    loop {
        line.clear();
        let Ok(read) = reader.read_line(&mut line) else {
            break;
        };
        if read == 0 {
            break;
        }
        // Mirror to parent stderr so panics are visible during a hung load.
        eprint!("[riffdbd-stderr] {line}");
        if let Ok(mut guard) = ring.lock() {
            push_stderr_line(&mut guard, &mut ring_bytes, line.clone());
        }
        total = total.saturating_add(read);
    }
    total
}

fn push_stderr_line(ring: &mut VecDeque<String>, ring_bytes: &mut usize, line: String) {
    *ring_bytes = ring_bytes.saturating_add(line.len());
    ring.push_back(line);
    while ring.len() > STDERR_RING_LINES || (*ring_bytes > STDERR_RING_BYTES && ring.len() > 1) {
        if let Some(front) = ring.pop_front() {
            *ring_bytes = ring_bytes.saturating_sub(front.len());
        } else {
            break;
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_root_prefers_explicit_override() {
        let override_root = PathBuf::from("/data/riffdb-baseline");
        assert_eq!(
            resolve_database_root(Some(override_root.as_path())),
            override_root
        );
    }

    #[test]
    fn default_database_root_constant_is_not_tmp() {
        let root = PathBuf::from(DEFAULT_DATABASE_ROOT);
        assert!(
            !root.starts_with("/tmp"),
            "default root must not be under /tmp: {}",
            root.display()
        );
        assert_eq!(root.as_os_str(), "target/perf-db/app-baseline");
        // Explicit override always wins over env/default.
        let forced = PathBuf::from("target/perf-db/app-baseline/forced-db");
        assert_eq!(resolve_database_root(Some(forced.as_path())), forced);
    }

    #[test]
    fn sweep_removes_only_session_prefix_dirs() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("perf-db")
            .join(format!("riffdb-baseline-sweep-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let stale = root.join("riffdb-app-baseline-999999-1");
        let keep = root.join("keep-me");
        fs::create_dir_all(&stale).expect("stale");
        fs::create_dir_all(&keep).expect("keep");
        sweep_stale_session_dirs(&root);
        assert!(!stale.exists());
        assert!(keep.exists());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn min_free_bytes_scales_with_mode() {
        assert_eq!(min_free_bytes_for_full(false), MIN_FREE_BYTES_SMOKE);
        assert_eq!(min_free_bytes_for_full(true), MIN_FREE_BYTES_FULL);
        const {
            assert!(MIN_FREE_BYTES_FULL > MIN_FREE_BYTES_SMOKE);
        }
    }

    #[test]
    fn stderr_ring_keeps_last_200_within_byte_cap() {
        let mut ring = VecDeque::new();
        let mut ring_bytes = 0_usize;
        for i in 0..10_000 {
            // Short lines so the line cap (200) binds before the byte cap.
            push_stderr_line(&mut ring, &mut ring_bytes, format!("line-{i}\n"));
        }
        assert_eq!(ring.len(), STDERR_RING_LINES);
        assert!(ring.front().unwrap().starts_with("line-9800"));
        assert!(ring.back().unwrap().starts_with("line-9999"));
        assert!(ring_bytes <= STDERR_RING_BYTES + 64);

        // Byte-cap eviction: huge lines shrink the ring below the line cap.
        let mut ring = VecDeque::new();
        let mut ring_bytes = 0_usize;
        let huge = "x".repeat(8_000) + "\n";
        for _ in 0..20 {
            push_stderr_line(&mut ring, &mut ring_bytes, huge.clone());
        }
        assert!(ring.len() < STDERR_RING_LINES);
        assert!(ring_bytes <= STDERR_RING_BYTES + huge.len());
    }
}
