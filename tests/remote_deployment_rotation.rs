#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Real-process resumable application-credential rotation for WP-554.

use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::net::TcpListener;
use std::num::NonZeroU32;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{ExitCode, ExitStatus};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use riffdb_client_rust::{
    CallMetadata, DatabaseAlias, RiffDbClient, generate_request_id,
    load_protected_bearer_credential, v1,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};
use riffdb_testkit_server::process::{ChildProcessController, ChildProcessSpec};

const SERVER_CHILD: &str = "RIFFDB_ROTATION_SERVER_CHILD";
const CLI_CHILD: &str = "RIFFDB_ROTATION_CLI_CHILD";
const INTERRUPT_AFTER: &str = "RIFFDB_APPLICATION_TEST_INTERRUPT_AFTER";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const START_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(30);
const INTERRUPTED_EXIT: i32 = 86;
const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEYS: &[u8] = b"riffdb-idempotency-digest-keys-v1\n1:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> ExitCode {
    if std::env::var_os(SERVER_CHILD).is_some() {
        return riffdb_server::riffdbd_main();
    }
    if std::env::var_os(CLI_CHILD).is_some() {
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(_) => return ExitCode::FAILURE,
        };
        return runtime.block_on(riffdb_cli::run());
    }
    match remote_rotation_matrix() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("remote deployment rotation acceptance failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn remote_rotation_matrix() -> TestResult<()> {
    let root = TestRoot::new()?;
    let port = reserve_loopback_port()?;
    let certificate = root.write_protected(
        "server.pem",
        include_bytes!("../crates/riffdb-server/tests/fixtures/localhost-cert.pem"),
        0o444,
    )?;
    let private_key = root.write_protected(
        "server.key",
        include_bytes!("../crates/riffdb-server/tests/fixtures/localhost-key.pem"),
        0o600,
    )?;
    let trust_root = root.write_protected(
        "ca.pem",
        include_bytes!("../crates/riffdb-server/tests/fixtures/test-ca.pem"),
        0o444,
    )?;
    let capability_keys = root.write_protected("capability.keys", CAPABILITY_KEYS, 0o600)?;
    let idempotency_keys = root.write_protected("idempotency.keys", IDEMPOTENCY_KEYS, 0o600)?;
    let backup_root = root.path().join("backups");
    fs::create_dir(&backup_root)?;
    fs::set_permissions(&backup_root, fs::Permissions::from_mode(0o700))?;
    let server_config = root.path().join("riffdbd.toml");
    write_server_config(
        &server_config,
        &root.path().join("riffdb.redb"),
        &backup_root,
        &capability_keys,
        &idempotency_keys,
        &certificate,
        &private_key,
        port,
    )?;
    let mut server = ChildProcessController::spawn(&server_spec(server_config)?)?;
    let ready = server.wait_for_readiness(READY_PREFIX, START_TIMEOUT)?;
    if ready != format!("{READY_PREFIX}127.0.0.1:{port}") {
        return Err("TLS server published an unexpected readiness endpoint".into());
    }

    let bootstrap_request = root.path().join("bootstrap.json");
    write_bootstrap_request(&bootstrap_request)?;
    let bootstrap_config = root.path().join("bootstrap-client.toml");
    write_client_config(&bootstrap_config, port, &trust_root, None)?;
    let bootstrap_material = root.path().join("bootstrap.credential");
    let operator_credential = root.path().join("operator.credential");
    require_success(
        run_cli(
            [
                "--config".into(),
                bootstrap_config.as_os_str().to_owned(),
                "capability".into(),
                "bootstrap".into(),
                "--request".into(),
                bootstrap_request.as_os_str().to_owned(),
                "--generate".into(),
                bootstrap_material.as_os_str().to_owned(),
                "--bearer-output".into(),
                operator_credential.as_os_str().to_owned(),
            ],
            None,
            None,
        )?,
        "operator bootstrap",
    )?;
    require_mode(&operator_credential, 0o600)?;

    let operator_config = root.path().join("operator-client.toml");
    write_client_config(
        &operator_config,
        port,
        &trust_root,
        Some(&operator_credential),
    )?;
    let application = root.path().join("application");
    copy_application_fixture(&application)?;
    let deploy_args = || {
        vec![
            "--config".into(),
            operator_config.as_os_str().to_owned(),
            "application".into(),
            "deploy".into(),
            "riffdb.application.json".into(),
            "--lock".into(),
            "riffdb.application.lock.json".into(),
            "--provision-role".into(),
            "AgentAlphaApplication".into(),
            "--lifetime-seconds".into(),
            "3600".into(),
        ]
    };
    require_success(
        run_cli(deploy_args(), None, Some(&application))?,
        "initial application deploy",
    )?;

    let deployment_root = application.join(".riffdb/deployments/default");
    let state_path = deployment_root.join("deployment-state.json");
    let tls = tls_client_config(port, trust_root.clone())?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    for stage in [
        "rotation_intent_retained",
        "rotation_successor_created_remote",
        "rotation_successor_bound",
        "rotation_successor_proven",
        "rotation_configs_switched",
        "rotation_predecessor_revoked",
    ] {
        let before = read_role_state(&state_path, &deployment_root)?;
        let mut predecessor =
            runtime.block_on(application_client(&tls, &before.credential_path))?;
        runtime.block_on(assert_application_handshake(
            &mut predecessor.0,
            &predecessor.1,
        ))?;

        let mut rotation_args = deploy_args();
        rotation_args.push("--replace-role-credential".into());
        let interrupted = run_cli(rotation_args.clone(), Some(stage), Some(&application))?;
        if interrupted.status.code() != Some(INTERRUPTED_EXIT) {
            return Err(format!("rotation stage {stage} did not stop at its crash arm").into());
        }

        let resumed = run_cli(rotation_args.clone(), None, Some(&application))?;
        if stage == "rotation_successor_created_remote" {
            if resumed.status.success() {
                return Err("lost successor secret was not surfaced before retry".into());
            }
            require_success(
                run_cli(rotation_args, None, Some(&application))?,
                "rotation retry after secret loss",
            )?;
        } else {
            require_success(resumed, "rotation resume")?;
        }

        let after = read_role_state(&state_path, &deployment_root)?;
        if before.capability_id == after.capability_id
            || !after.credential_path.is_file()
            || after.rotation_present
        {
            return Err(
                format!("rotation stage {stage} did not publish one terminal successor").into(),
            );
        }
        if before.credential_path.exists() {
            return Err(
                format!("rotation stage {stage} retained the revoked predecessor secret").into(),
            );
        }
        if runtime
            .block_on(assert_application_handshake(
                &mut predecessor.0,
                &predecessor.1,
            ))
            .is_ok()
        {
            return Err(
                format!("rotation stage {stage} left the pooled predecessor authorized").into(),
            );
        }
        let mut successor = runtime.block_on(application_client(&tls, &after.credential_path))?;
        runtime.block_on(assert_application_handshake(&mut successor.0, &successor.1))?;
        let client_config = fs::read_to_string(deployment_root.join("client.toml"))?;
        if !client_config.contains(after.credential_path.to_str().ok_or("credential path")?)
            || client_config.contains("capability_token")
        {
            return Err(format!(
                "application config did not switch by protected credential path: expected={} config={client_config}",
                after.credential_path.display()
            )
            .into());
        }
    }

    let stopped = server.shutdown_cleanly(b"shutdown\n", STOP_TIMEOUT)?;
    if !stopped.status.success() {
        return Err("TLS server did not drain after the rotation matrix".into());
    }
    Ok(())
}

async fn application_client(
    tls: &TlsClientConfig,
    credential_path: &Path,
) -> TestResult<(RiffDbClient, CallMetadata)> {
    let credential = load_protected_bearer_credential(credential_path)?;
    let metadata =
        CallMetadata::authenticated(credential).with_database(DatabaseAlias::default_alias());
    let client = RiffDbClient::connect_verified_tls(tls).await?;
    Ok((client, metadata))
}

async fn assert_application_handshake(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
) -> TestResult<()> {
    let request_id = generate_request_id()?.into_bytes().to_vec();
    let response = client
        .discover_command_tools(
            v1::DiscoverCommandToolsRequest {
                request_id,
                page: Some(v1::PageRequest {
                    limit: Some(32),
                    cursor: None,
                }),
                prior_fence: None,
                representation: v1::DiscoveryRepresentation::CompactObservation as i32,
            },
            metadata,
        )
        .await?;
    let Some(v1::discover_command_tools_response::Result::CompactPage(page)) = response.result
    else {
        return Err("application handshake did not return a compact catalog".into());
    };
    if page.items.is_empty() || page.observed_fence.is_none() {
        return Err("application handshake omitted its authorized catalog identity".into());
    }
    Ok(())
}

struct RoleState {
    capability_id: String,
    credential_path: PathBuf,
    rotation_present: bool,
}

fn read_role_state(path: &Path, deployment_root: &Path) -> TestResult<RoleState> {
    let document: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
    let role = document
        .get("role")
        .and_then(|value| value.as_object())
        .ok_or("role state")?;
    let capability_id = role
        .get("capability_id")
        .and_then(|value| value.as_str())
        .ok_or("capability identity")?
        .to_owned();
    let file = role
        .get("credential_file")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("application.credential");
    Ok(RoleState {
        capability_id,
        credential_path: deployment_root.join(file),
        rotation_present: document
            .get("credential_rotation")
            .is_some_and(|value| !value.is_null()),
    })
}

struct CliExit {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn run_cli(
    arguments: impl IntoIterator<Item = OsString>,
    interruption: Option<&str>,
    working_directory: Option<&Path>,
) -> TestResult<CliExit> {
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command.env_clear().env(CLI_CHILD, "1").args(arguments);
    if let Some(directory) = working_directory {
        command.current_dir(directory);
    }
    if let Some(stage) = interruption {
        command.env(INTERRUPT_AFTER, stage);
    }
    let output = command.output()?;
    if output.stdout.len() > 65_536 || output.stderr.len() > 65_536 {
        return Err("CLI child exceeded its public output bound".into());
    }
    Ok(CliExit {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

fn require_success(exit: CliExit, operation: &str) -> TestResult<()> {
    if !exit.status.success() {
        return Err(format!(
            "{operation} failed with {}: stdout={} stderr={}",
            exit.status,
            String::from_utf8_lossy(&exit.stdout),
            String::from_utf8_lossy(&exit.stderr)
        )
        .into());
    }
    Ok(())
}

fn write_bootstrap_request(path: &Path) -> TestResult<()> {
    let request = serde_json::json!({
        "principal_id": "remote-operator",
        "actor_kind": "human",
        "requested_lifetime_seconds": 86400,
        "audiences": ["riffdb-application-test"],
        "grant": {
            "tenant_scope": {"type": "global"},
            "partition_scope": {"type": "all"},
            "permissions": [
                {"type": "validate_contract"},
                {"type": "read_contract"},
                {"type": "deploy_contract"},
                {"type": "read_health"},
                {"type": "create_capability"},
                {"type": "revoke_capability"},
                {"type": "administer_capabilities"}
            ],
            "field_visibility": [],
            "max_scan_rows": 50,
            "approval_required": []
        }
    });
    fs::write(path, serde_json::to_vec(&request)?)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn write_client_config(
    path: &Path,
    port: u16,
    trust_root: &Path,
    credential: Option<&Path>,
) -> TestResult<()> {
    let credential = credential.map_or_else(String::new, |credential| {
        format!("credential_file = {credential:?}\n")
    });
    fs::write(
        path,
        format!(
            "[client]\nendpoint = \"https://127.0.0.1:{port}\"\ndatabase = \"default\"\noutput = \"json\"\nmax_attempts = 3\n{credential}tls_trust_root = {trust_root:?}\ntls_server_name = \"127.0.0.1\"\n"
        ),
    )?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_server_config(
    path: &Path,
    database: &Path,
    backup_root: &Path,
    capability_keys: &Path,
    idempotency_keys: &Path,
    certificate: &Path,
    private_key: &Path,
    port: u16,
) -> TestResult<()> {
    fs::write(
        path,
        format!(
            "[server]\ndatabase = {database:?}\nenvironment = \"development\"\naudience = \"riffdb-application-test\"\ncapability_keys = {capability_keys:?}\nidempotency_keys = {idempotency_keys:?}\n\n[server.application_listener]\nmode = \"direct_tls\"\nlisten = \"127.0.0.1:{port}\"\npublic_endpoint = \"https://127.0.0.1:{port}\"\ncertificate_chain = {certificate:?}\nprivate_key = {private_key:?}\n\n[server.application_listener.bounds]\nmax_connections = 64\nmax_streams_per_connection = 128\nhandshake_timeout_seconds = 5\ndrain_timeout_seconds = 5\n\n[maintenance]\nbackup_root = {backup_root:?}\n"
        ),
    )?;
    Ok(())
}

fn server_spec(config: PathBuf) -> TestResult<ChildProcessSpec> {
    Ok(ChildProcessSpec::new(std::env::current_exe()?)?
        .clear_environment()
        .arg("--config")?
        .arg(config.into_os_string())?
        .env(SERVER_CHILD, "1")?)
}

fn tls_client_config(port: u16, trust_root: PathBuf) -> TestResult<TlsClientConfig> {
    Ok(TlsClientConfig::new(
        CanonicalHttpsEndpoint::parse(&format!("https://127.0.0.1:{port}"))?,
        ProtectedFilePath::new(trust_root)?,
        TlsServerIdentity::parse("127.0.0.1")?,
        Duration::from_secs(5),
        Duration::from_secs(30),
        NonZeroU32::new(4).ok_or("pool bound")?,
        NonZeroU32::new(64).ok_or("stream bound")?,
    )?)
}

fn copy_application_fixture(destination: &Path) -> TestResult<()> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/agent-alpha");
    for relative in [
        "riffdb.application.json",
        "riffdb.application.lock.json",
        "riffdb/contract.riff",
        "riffdb/queries/item_page.riffq",
        "riffdb/seed/01-CreateItem.jsonl",
        "generated/mcp/tools.json",
        "generated/riffdb.application.exact.json",
        "generated/riffdb.contract.bundle",
        "generated/rust/client.rs",
        "web/src/generated/client.ts",
    ] {
        let target = destination.join(relative);
        fs::create_dir_all(target.parent().ok_or("application fixture parent")?)?;
        fs::copy(source.join(relative), target)?;
    }
    Ok(())
}

fn require_mode(path: &Path, mode: u32) -> TestResult<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o777 != mode
    {
        return Err("protected file mode was not exact".into());
    }
    Ok(())
}

fn reserve_loopback_port() -> TestResult<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> TestResult<Self> {
        let unique = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::current_dir()?
            .join("target/remote-deployment-rotation")
            .join(format!("{}-{unique}", std::process::id()));
        fs::create_dir_all(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        Ok(Self(root))
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn write_protected(&self, name: &str, bytes: &[u8], mode: u32) -> TestResult<PathBuf> {
        let path = self.0.join(name);
        fs::write(&path, bytes)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
        Ok(path)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
