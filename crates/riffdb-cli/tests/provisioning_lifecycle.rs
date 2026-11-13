#![cfg(any(target_os = "linux", target_os = "macos"))]
#![forbid(unsafe_code)]

//! Stateful CLI provisioning, redeploy, live reload, and restart lifecycle proof.

// req: DX-004, DX-006

use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use riffdb_auth::bootstrap_secret::load_bootstrap_credential_file;
use serde_json::{Value, json};

const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const START_TIMEOUT: Duration = Duration::from_secs(20);
const STOP_TIMEOUT: Duration = Duration::from_secs(15);
const SERVER_AUDIENCE: &str = "riffdb-provisioning-lifecycle";
const DRIVER_PROTOCOL_VERSION: u32 = 4;
const DRIVER_VALUE_REGISTRY_HASH: &str =
    "8e1681ddf5e6a82e7fa646f9737128ad7e36f54f8b5846ac6e33e732125407e5";
const DRIVER_ERROR_REGISTRY_HASH: &str =
    "b94d685ecbc18f2369a2bfa1a53139d06100699c4ee41b31c86d6a7e17039850";
const CREATE_ITEM_SCHEMA_HASH: &str =
    "5c7a3a44a20ddd450af975c763a45de00e5bbf246598171dcc2a556089935dc2";
const ITEM_PAGE_SCHEMA_HASH: &str =
    "e80f7b5c93c2a56b94d286b35377068aea67449dcb2a8f243a00375ffe92e47b";
const ITEM_1_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b11";
const ITEM_2_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b22";
const RETENTION_ABORT_ENV: &str = "RIFFDB_PROVISIONING_RETENTION_ABORT_TARGET";

const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEYS: &[u8] = b"riffdb-idempotency-digest-keys-v1\n1:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[cfg(feature = "test-fixtures")]
#[test]
fn bootstrap_retention_abort_child_entrypoint() -> TestResult<()> {
    let Some(target) = std::env::var_os(RETENTION_ABORT_ENV) else {
        return Ok(());
    };
    let _ = riffdb_cli::test_fixtures::run_bootstrap_retention_fixture(
        Path::new(&target),
        riffdb_cli::test_fixtures::BootstrapRetentionTestPoint::CredentialFileSynced,
    );
    Err(err("armed retention fixture returned without aborting"))
}

// req: DX-004, DX-006
#[test]
fn stateful_provisioning_redeploy_live_reload_and_restart_lifecycle() -> TestResult<()> {
    let bins = WorkspaceBinaries::resolve()?;
    let work = tempfile::Builder::new()
        .prefix("riffdb-prov-lifecycle-")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()?;
    // Keep Unix domain socket path strictly below 107 bytes (`sockaddr_un`).
    let sock_dir = tempfile::Builder::new()
        .prefix("rdb-pl-")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/tmp")?;

    let server_port = reserve_loopback_port()?;
    let closed_port = reserve_loopback_port()?;
    let certs = TlsMaterial::copy_from_fixtures(work.path())?;
    let server_config = write_multi_db_server_config(work.path(), server_port, &certs)?;

    let mut server = DaemonProcess::spawn(&bins.riffdbd, &server_config)?;
    let bound = server.wait_ready()?;
    assert_eq!(bound.port(), server_port);

    let endpoint = format!("https://127.0.0.1:{server_port}");
    let closed_endpoint = format!("https://127.0.0.1:{closed_port}");

    // 1. Definitive bootstrap failure cleans up newly created bearer & automatic files.
    let failed_dir = work.path().join("failed-bootstrap");
    fs::create_dir_all(&failed_dir)?;
    fs::set_permissions(&failed_dir, fs::Permissions::from_mode(0o700))?;
    let failed_cred = failed_dir.join("operator-failed.credential");
    let failed_out = run_cli(
        &bins.riffdb,
        work.path(),
        &[
            "--output",
            "json",
            "--endpoint",
            &closed_endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "--max-attempts",
            "1",
            "capability",
            "bootstrap",
            "--operator",
            "--bearer-output",
            path_str(&failed_cred)?,
        ],
    )?;
    assert!(!failed_out.status.success());
    assert!(
        !failed_cred.exists(),
        "definitive bootstrap failure left bearer output behind"
    );
    assert_eq!(
        fs::read_dir(&failed_dir)?.count(),
        0,
        "definitive bootstrap failure left automatic bootstrap document behind"
    );

    // 2. Initial operator bootstrap creates a mode-0600 credential that authenticates.
    let creds_dir = work.path().join("creds");
    fs::create_dir_all(&creds_dir)?;
    fs::set_permissions(&creds_dir, fs::Permissions::from_mode(0o700))?;
    let op_cred = creds_dir.join("operator-default.credential");
    let boot_json = run_cli_json(
        &bins.riffdb,
        work.path(),
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "capability",
            "bootstrap",
            "--operator",
            "--bearer-output",
            path_str(&op_cred)?,
        ],
    )?;
    assert_eq!(boot_json["ok"], true);
    assert_eq!(boot_json["result"]["status"], "created");
    assert_eq!(boot_json["result"]["bearer_retained"], true);
    assert_mode_0600(&op_cred)?;
    let original_op_bytes = fs::read(&op_cred)?;
    let original_op_ino = fs::symlink_metadata(&op_cred)?.ino();

    let health_after_boot = run_cli_json(
        &bins.riffdb,
        work.path(),
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "--credential-file",
            path_str(&op_cred)?,
            "server",
            "health",
        ],
    )?;
    assert_eq!(health_after_boot["ok"], true);
    assert_ne!(health_after_boot["result"]["status"], "pre_bootstrap");

    // 3. Idempotent rerun without --force refuses locally and never clobbers working credential;
    //    rerun against a fresh target path reports bootstrap_conflict while working credential stays valid.
    let rerun_same = run_cli(
        &bins.riffdb,
        work.path(),
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "capability",
            "bootstrap",
            "--operator",
            "--bearer-output",
            path_str(&op_cred)?,
        ],
    )?;
    assert_eq!(rerun_same.status.code(), Some(2));
    let rerun_same_json: Value = serde_json::from_slice(&rerun_same.stdout)?;
    assert_eq!(
        rerun_same_json["error"]["code"],
        "credential_retention_failed"
    );
    assert_eq!(fs::read(&op_cred)?, original_op_bytes);
    assert_eq!(fs::symlink_metadata(&op_cred)?.ino(), original_op_ino);

    let conflict_cred = creds_dir.join("operator-conflict.credential");
    let conflict_json = run_cli_json(
        &bins.riffdb,
        work.path(),
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "capability",
            "bootstrap",
            "--operator",
            "--bearer-output",
            path_str(&conflict_cred)?,
        ],
    )?;
    assert_eq!(conflict_json["ok"], true);
    assert_eq!(conflict_json["result"]["status"], "bootstrap_conflict");
    assert_eq!(fs::read(&op_cred)?, original_op_bytes);

    // 4. Pre-validation & pre-connect failures with --force preserve the existing working credential.
    let invalid_boot_doc = work.path().join("invalid-bootstrap.credential");
    write_mode_0600(&invalid_boot_doc, b"not-a-valid-bootstrap-document\n")?;
    for bad_args in [
        vec![
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "capability",
            "bootstrap",
            "--operator",
            "--bootstrap-file",
            path_str(&invalid_boot_doc)?,
            "--bearer-output",
            path_str(&op_cred)?,
            "--force",
        ],
        vec![
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "capability",
            "bootstrap",
            "--operator",
            "--bootstrap-file",
            path_str(&op_cred)?,
            "--bearer-output",
            path_str(&op_cred)?,
            "--force",
        ],
        vec![
            "--output",
            "json",
            "--endpoint",
            &closed_endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "--max-attempts",
            "1",
            "capability",
            "bootstrap",
            "--operator",
            "--bearer-output",
            path_str(&op_cred)?,
            "--force",
        ],
    ] {
        let out = run_cli(&bins.riffdb, work.path(), &bad_args)?;
        assert!(!out.status.success(), "expected failure for {bad_args:?}");
        assert_eq!(
            fs::read(&op_cred)?,
            original_op_bytes,
            "--force pre-send failure clobbered existing working credential"
        );
    }

    // 5. OutcomeUnknown recovery retention (both process-abort boundary & post-send TLS sever).
    #[cfg(feature = "test-fixtures")]
    verify_abort_retention_fixture(work.path())?;

    verify_uncertain_bootstrap_replay(&bins.riffdb, work.path(), server_port, &certs)?;

    // 6. External-cwd `application deploy` with `--deployment-dir` and `--driver-config-output`.
    let app_src = work.path().join("agent-alpha-src");
    copy_dir_recursive(&workspace_root().join("examples/agent-alpha"), &app_src)?;
    let unrelated_cwd = work.path().join("unrelated-empty-cwd");
    fs::create_dir_all(&unrelated_cwd)?;
    let ext_deployments = work.path().join("external-deployments");
    fs::create_dir_all(&ext_deployments)?;
    fs::set_permissions(&ext_deployments, fs::Permissions::from_mode(0o700))?;
    let driver_toml = work.path().join("driverd.toml");
    let driver_env = work.path().join("driver.env");
    let driver_sock = sock_dir.path().join("d.sock");
    let manifest_path = app_src.join("riffdb.application.json");
    let lock_path = app_src.join("riffdb.application.lock.json");

    let deploy_1 = run_cli_json(
        &bins.riffdb,
        &unrelated_cwd,
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "--credential-file",
            path_str(&op_cred)?,
            "application",
            "deploy",
            path_str(&manifest_path)?,
            "--lock",
            path_str(&lock_path)?,
            "--provision-role",
            "AgentAlphaApplication",
            "--deployment-dir",
            path_str(&ext_deployments)?,
            "--driver-config-output",
            path_str(&driver_toml)?,
            "--driver-env-output",
            path_str(&driver_env)?,
            "--driver-socket",
            path_str(&driver_sock)?,
        ],
    )?;
    assert_eq!(deploy_1["ok"], true);
    assert_mode_0600(&driver_toml)?;
    assert_mode_0600(&driver_env)?;

    let health_ready = run_cli_json(
        &bins.riffdb,
        &unrelated_cwd,
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "--credential-file",
            path_str(&op_cred)?,
            "server",
            "health",
        ],
    )?;
    assert_eq!(health_ready["result"]["status"], "ready");

    // 7. Inject `driver.allowed_peer_uids` and verify idempotent redeploy preserves it.
    let current_uid = {
        let uid = fs::metadata(work.path())?.uid();
        if uid == 0 { 1000 } else { uid }
    };
    let initial_toml = fs::read_to_string(&driver_toml)?;
    let with_peer_uids = initial_toml.replacen(
        "[driver]\n",
        &format!("[driver]\nallowed_peer_uids = [{current_uid}, 65532]\n"),
        1,
    );
    assert!(with_peer_uids.contains("allowed_peer_uids"));
    fs::write(&driver_toml, with_peer_uids.as_bytes())?;
    fs::set_permissions(&driver_toml, fs::Permissions::from_mode(0o600))?;

    let deploy_2 = run_cli_json(
        &bins.riffdb,
        &unrelated_cwd,
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "--credential-file",
            path_str(&op_cred)?,
            "application",
            "deploy",
            path_str(&manifest_path)?,
            "--lock",
            path_str(&lock_path)?,
            "--provision-role",
            "AgentAlphaApplication",
            "--deployment-dir",
            path_str(&ext_deployments)?,
            "--driver-config-output",
            path_str(&driver_toml)?,
            "--driver-env-output",
            path_str(&driver_env)?,
            "--driver-socket",
            path_str(&driver_sock)?,
        ],
    )?;
    assert_eq!(deploy_2["ok"], true);
    let parsed_redeployed: toml::Value = toml::from_str(&fs::read_to_string(&driver_toml)?)?;
    let preserved_uids = parsed_redeployed["driver"]["allowed_peer_uids"]
        .as_array()
        .ok_or_else(|| err("allowed_peer_uids missing after redeploy"))?
        .iter()
        .map(|v| v.as_integer().unwrap_or(-1))
        .collect::<Vec<_>>();
    assert_eq!(preserved_uids, vec![i64::from(current_uid), 65532]);

    // 8. Live `riffdb-driverd` credential rotation + predecessor revocation without restart.
    let state_json: Value = serde_json::from_slice(&fs::read(
        ext_deployments.join("default/deployment-state.json"),
    )?)?;
    let predecessor_cap_id = state_json["role"]["capability_id"]
        .as_str()
        .ok_or_else(|| err("missing predecessor capability_id"))?
        .to_owned();
    let role_cred_path = parsed_redeployed["application"]["credential_file"]
        .as_str()
        .map(PathBuf::from)
        .ok_or_else(|| err("missing application.credential_file in driverd.toml"))?;
    let predecessor_cred_copy = work.path().join("predecessor-role.credential");
    write_mode_0600(&predecessor_cred_copy, &fs::read(&role_cred_path)?)?;

    let env_map = parse_env_file(&driver_env)?;
    let mut driverd = DriverProcess::spawn(&bins.driverd, &driver_toml, &driver_sock)?;
    let driverd_pid = driverd.pid();

    let mut session = DriverSession::connect_and_handshake(&driver_sock, &env_map)?;
    let created_1 = session.create_item("prov-item-1", ITEM_1_ID, "Before live rotation")?;
    assert_eq!(
        created_1["value"]["value"]["outcome"]["value"], "Created",
        "unexpected create_item response: {created_1}"
    );
    let commit_1 = created_1["application_head"]
        .as_u64()
        .ok_or_else(|| err(format!("missing application_head in {created_1}")))?;
    let page_1 = session.item_page(ITEM_1_ID, Some(commit_1))?;
    assert_eq!(
        page_1["value"]["value"]["outcome"]["value"], "Found",
        "unexpected item_page response: {page_1}"
    );
    assert_eq!(
        page_1["value"]["value"]["item"]["value"]["title"]["value"],
        "Before live rotation"
    );

    // Rotate role credential on disk via `--replace-role-credential`, then revoke predecessor.
    let rotate_deploy = run_cli_json(
        &bins.riffdb,
        &unrelated_cwd,
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "--credential-file",
            path_str(&op_cred)?,
            "application",
            "deploy",
            path_str(&manifest_path)?,
            "--lock",
            path_str(&lock_path)?,
            "--provision-role",
            "AgentAlphaApplication",
            "--replace-role-credential",
            "--deployment-dir",
            path_str(&ext_deployments)?,
            "--driver-config-output",
            path_str(&driver_toml)?,
            "--driver-env-output",
            path_str(&driver_env)?,
            "--driver-socket",
            path_str(&driver_sock)?,
        ],
    )?;
    assert_eq!(rotate_deploy["ok"], true);
    let rotated_state: Value = serde_json::from_slice(&fs::read(
        ext_deployments.join("default/deployment-state.json"),
    )?)?;
    let successor_cap_id = rotated_state["role"]["capability_id"]
        .as_str()
        .ok_or_else(|| err("missing successor capability_id"))?;
    assert_ne!(successor_cap_id, predecessor_cap_id);

    let revoke_out = run_cli_json(
        &bins.riffdb,
        &unrelated_cwd,
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "--credential-file",
            path_str(&op_cred)?,
            "capability",
            "revoke",
            &predecessor_cap_id,
            "--reason",
            "replaced",
        ],
    )?;
    assert_eq!(revoke_out["ok"], true);
    assert_eq!(revoke_out["result"]["status"], "already_revoked");

    // Predecessor credential fails closed directly against riffdbd (`RDB-AUTH-0215` / revoked).
    let revoked_direct = run_cli(
        &bins.riffdb,
        &app_src,
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "--credential-file",
            path_str(&predecessor_cred_copy)?,
            "query",
            "run-named",
            "ItemPage",
            "--contract-lineage",
            "AgentAlpha",
            "--contract-version",
            "1",
            "--params-json",
            &format!(r#"{{"item_id":{{"type":"uuid","value":"{ITEM_1_ID}"}}}}"#),
        ],
    )?;
    assert!(!revoked_direct.status.success());
    let revoked_json: Value = serde_json::from_slice(&revoked_direct.stdout).map_err(|e| {
        err(format!(
            "failed to parse revoked_direct stdout ({e}): stdout={} stderr={}",
            String::from_utf8_lossy(&revoked_direct.stdout),
            String::from_utf8_lossy(&revoked_direct.stderr),
        ))
    })?;
    assert_eq!(revoked_json["ok"], false);
    assert_eq!(revoked_json["error"]["type"], "application");
    assert_eq!(revoked_json["error"]["code"], "RDB-AUTH-0215");

    // Running `riffdb-driverd` (same PID, zero restart!) reloads rotated credential immediately.
    assert_eq!(driverd.pid(), driverd_pid);
    let created_2 = session.create_item("prov-item-2", ITEM_2_ID, "After live rotation")?;
    assert_eq!(created_2["value"]["value"]["outcome"]["value"], "Created");
    let commit_2 = created_2["application_head"]
        .as_u64()
        .ok_or_else(|| err("missing application_head after rotation"))?;
    let page_2 = session.item_page(ITEM_2_ID, Some(commit_2))?;
    assert_eq!(page_2["value"]["value"]["outcome"]["value"], "Found");
    assert_eq!(
        page_2["value"]["value"]["item"]["value"]["title"]["value"],
        "After live rotation"
    );

    // 9. Graceful restart of both `riffdb-driverd` and `riffdbd` preserves data and credentials.
    drop(session);
    driverd.stop()?;
    server.stop()?;

    let mut restarted_server = DaemonProcess::spawn(&bins.riffdbd, &server_config)?;
    let restarted_bound = restarted_server.wait_ready()?;
    assert_eq!(restarted_bound.port(), server_port);

    let health_after_restart = run_cli_json(
        &bins.riffdb,
        &unrelated_cwd,
        &[
            "--output",
            "json",
            "--endpoint",
            &endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "default",
            "--credential-file",
            path_str(&op_cred)?,
            "server",
            "health",
        ],
    )?;
    assert_eq!(health_after_restart["ok"], true);
    assert_eq!(
        health_after_restart["result"]["active_contract_version"],
        "1"
    );
    assert_eq!(health_after_restart["result"]["last_commit_sequence"], "2");
    for required_healthy in ["authoritative_storage", "catalog", "commit_coordinator"] {
        let comp = health_after_restart["result"]["components"]
            .as_array()
            .and_then(|arr| arr.iter().find(|c| c["component"] == required_healthy))
            .ok_or_else(|| err(format!("missing component {required_healthy}")))?;
        assert_eq!(comp["status"], "healthy");
    }

    let mut restarted_driverd = DriverProcess::spawn(&bins.driverd, &driver_toml, &driver_sock)?;
    let mut post_restart_session = DriverSession::connect_and_handshake(&driver_sock, &env_map)?;
    for (id, expected_title) in [
        (ITEM_1_ID, "Before live rotation"),
        (ITEM_2_ID, "After live rotation"),
    ] {
        let page = post_restart_session.item_page(id, Some(commit_2))?;
        assert_eq!(page["value"]["value"]["outcome"]["value"], "Found");
        assert_eq!(
            page["value"]["value"]["item"]["value"]["title"]["value"],
            expected_title
        );
    }

    drop(post_restart_session);
    restarted_driverd.stop()?;
    restarted_server.stop()?;
    Ok(())
}

#[cfg(feature = "test-fixtures")]
fn verify_abort_retention_fixture(work: &Path) -> TestResult<()> {
    let abort_dir = work.join("abort-retention");
    fs::create_dir_all(&abort_dir)?;
    fs::set_permissions(&abort_dir, fs::Permissions::from_mode(0o700))?;
    let abort_doc = abort_dir.join("aborted-bootstrap.credential");
    let current_exe = std::env::current_exe()?;
    let output = Command::new(current_exe)
        .args([
            "--exact",
            "bootstrap_retention_abort_child_entrypoint",
            "--nocapture",
        ])
        .env(RETENTION_ABORT_ENV, &abort_doc)
        .output()?;
    assert!(
        !output.status.success() && output.status.signal().is_some(),
        "expected SIGABRT from armed retention fixture, got status {:?}",
        output.status
    );
    assert_mode_0600(&abort_doc)?;
    let parsed = load_bootstrap_credential_file(&abort_doc)?;
    assert!(!parsed.capability_id().to_string().is_empty());
    Ok(())
}

fn verify_uncertain_bootstrap_replay(
    riffdb_bin: &Path,
    work: &Path,
    upstream_port: u16,
    certs: &TlsMaterial,
) -> TestResult<()> {
    let uncertain_dir = work.join("uncertain-bootstrap");
    fs::create_dir_all(&uncertain_dir)?;
    fs::set_permissions(&uncertain_dir, fs::Permissions::from_mode(0o700))?;
    let uncertain_bearer = uncertain_dir.join("operator-recovery.credential");

    let proxy = PostSendSeverProxy::start(upstream_port)?;
    let proxy_endpoint = format!("https://127.0.0.1:{}", proxy.port());
    let real_endpoint = format!("https://127.0.0.1:{upstream_port}");

    let uncertain_out = run_cli(
        riffdb_bin,
        work,
        &[
            "--output",
            "json",
            "--endpoint",
            &proxy_endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "recovery_db",
            "--max-attempts",
            "1",
            "capability",
            "bootstrap",
            "--operator",
            "--bearer-output",
            path_str(&uncertain_bearer)?,
        ],
    )?;
    proxy.join();

    assert_eq!(
        uncertain_out.status.code(),
        Some(3),
        "expected uncertain exit code 3, stdout={}",
        String::from_utf8_lossy(&uncertain_out.stdout)
    );
    let uncertain_json: Value = serde_json::from_slice(&uncertain_out.stdout)?;
    assert_eq!(uncertain_json["ok"], false);
    assert_eq!(uncertain_json["error"]["type"], "uncertain");
    assert_eq!(uncertain_json["error"]["code"], "bootstrap_outcome_unknown");
    let cap_id = uncertain_json["error"]["capability_id"]
        .as_str()
        .ok_or_else(|| err("missing capability_id on bootstrap_outcome_unknown"))?;
    let retained_doc = uncertain_dir.join(format!(".riffdb-bootstrap-{cap_id}.credential"));
    assert_mode_0600(&retained_doc)?;
    assert_mode_0600(&uncertain_bearer)?;

    let replay_json = run_cli_json(
        riffdb_bin,
        work,
        &[
            "--output",
            "json",
            "--endpoint",
            &real_endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "recovery_db",
            "capability",
            "bootstrap",
            "--operator",
            "--bootstrap-file",
            path_str(&retained_doc)?,
            "--bearer-output",
            path_str(&uncertain_bearer)?,
            "--force",
        ],
    )?;
    assert_eq!(replay_json["ok"], true);
    let replay_status = replay_json["result"]["status"]
        .as_str()
        .ok_or_else(|| err("missing replay status"))?;
    assert!(
        matches!(replay_status, "replayed" | "created"),
        "unexpected bootstrap replay status: {replay_status}"
    );

    let health = run_cli_json(
        riffdb_bin,
        work,
        &[
            "--output",
            "json",
            "--endpoint",
            &real_endpoint,
            "--tls-trust-root",
            path_str(&certs.ca_pem)?,
            "--database",
            "recovery_db",
            "--credential-file",
            path_str(&uncertain_bearer)?,
            "server",
            "health",
        ],
    )?;
    assert_eq!(health["ok"], true);
    assert_ne!(health["result"]["status"], "pre_bootstrap");
    Ok(())
}

struct PostSendSeverProxy {
    port: u16,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl PostSendSeverProxy {
    fn start(upstream_port: u16) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut severed = false;
            while !stop_flag.load(Ordering::Relaxed) && Instant::now() < deadline {
                match listener.accept() {
                    Ok((downstream, _)) => {
                        if severed {
                            let _ = downstream.shutdown(std::net::Shutdown::Both);
                            break;
                        }
                        if let Ok(upstream) = TcpStream::connect(("127.0.0.1", upstream_port)) {
                            sever_after_post_handshake_request(downstream, upstream);
                            severed = true;
                        }
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        thread::yield_now();
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            port,
            stop,
            handle: Some(handle),
        })
    }

    const fn port(&self) -> u16 {
        self.port
    }

    fn join(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for PostSendSeverProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn sever_after_post_handshake_request(mut downstream: TcpStream, mut upstream: TcpStream) {
    let _ = downstream.set_nonblocking(true);
    let _ = upstream.set_nonblocking(true);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut upstream_flights = 0_usize;
    let mut post_flight_client_bytes = 0_usize;
    let mut buf = [0_u8; 16_384];

    while Instant::now() < deadline {
        let mut progressed = false;
        match downstream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                progressed = true;
                let _ = upstream.write_all(&buf[..n]);
                let _ = upstream.flush();
                if upstream_flights >= 1 {
                    post_flight_client_bytes = post_flight_client_bytes.saturating_add(n);
                    // Wait until H2 client preface/SETTINGS (~220B) + CreateCapabilityRequest
                    // (>350B encrypted) have been forwarded to riffdbd, then sever downstream
                    // before forwarding CreateCapabilityResponse.
                    if post_flight_client_bytes >= 350 {
                        // Drain one upstream read window briefly so riffdbd finishes committing
                        // before we drop the proxy sockets.
                        let drain_until = Instant::now() + Duration::from_millis(150);
                        while Instant::now() < drain_until {
                            match upstream.read(&mut buf) {
                                Ok(count) if count > 0 => break,
                                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                                    thread::yield_now();
                                }
                                _ => break,
                            }
                        }
                        let _ = downstream.shutdown(std::net::Shutdown::Both);
                        let _ = upstream.shutdown(std::net::Shutdown::Both);
                        return;
                    }
                }
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => break,
        }

        match upstream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                progressed = true;
                upstream_flights = upstream_flights.saturating_add(1);
                let _ = downstream.write_all(&buf[..n]);
                let _ = downstream.flush();
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => break,
        }

        if !progressed {
            thread::yield_now();
        }
    }
}

struct DriverSession {
    stream: UnixStream,
    next_id: u64,
}

impl DriverSession {
    fn connect_and_handshake(
        socket_path: &Path,
        env_map: &BTreeMap<String, String>,
    ) -> TestResult<Self> {
        let stream = UnixStream::connect(socket_path)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        let mut session = Self { stream, next_id: 1 };
        let handshake = json!({
            "type": "handshake",
            "request_id": session.alloc_id(),
            "protocol_version": DRIVER_PROTOCOL_VERSION,
            "application_manifest_hash": env_val(env_map, "RIFFDB_MANIFEST_HASH")?,
            "operation_catalog_hash": env_val(env_map, "RIFFDB_CATALOG_HASH")?,
            "contract_lineage": env_val(env_map, "RIFFDB_LINEAGE")?,
            "contract_version": env_val(env_map, "RIFFDB_CONTRACT_VERSION")?.parse::<u64>()?,
            "contract_bundle_hash": env_val(env_map, "RIFFDB_BUNDLE_HASH")?,
            "value_registry_hash": DRIVER_VALUE_REGISTRY_HASH,
            "error_registry_hash": DRIVER_ERROR_REGISTRY_HASH,
            "database": env_val(env_map, "RIFFDB_DATABASE")?,
            "role": env_val(env_map, "RIFFDB_ROLE")?,
            "role_definition_hash": env_val(env_map, "RIFFDB_ROLE_HASH")?,
            "remote_identity_hash": env_val(env_map, "RIFFDB_REMOTE_HASH")?,
        });
        let resp = session.roundtrip(&handshake)?;
        if resp["type"] != "handshake" {
            return Err(err(format!("unexpected handshake response: {resp}")));
        }
        Ok(session)
    }

    fn create_item(&mut self, idem_key: &str, item_id: &str, title: &str) -> TestResult<Value> {
        let req = json!({
            "type": "invoke",
            "request_id": self.alloc_id(),
            "operation": "agent_alpha_create_item",
            "input_schema_hash": CREATE_ITEM_SCHEMA_HASH,
            "input": {
                "idempotency_key": {"type": "string", "value": idem_key},
                "item_id": {"type": "uuid", "value": item_id},
                "title": {"type": "string", "value": title},
            },
            "options": {
                "deadline_millis": 10_000,
                "maximum_attempts": 3,
                "read_after_commit": null,
                "cursor": null,
            }
        });
        let resp = self.roundtrip(&req)?;
        if resp["type"] != "result" {
            return Err(err(format!("create_item failed: {resp}")));
        }
        Ok(resp)
    }

    fn item_page(&mut self, item_id: &str, read_after_commit: Option<u64>) -> TestResult<Value> {
        let req = json!({
            "type": "invoke",
            "request_id": self.alloc_id(),
            "operation": "agent_alpha_item_page",
            "input_schema_hash": ITEM_PAGE_SCHEMA_HASH,
            "input": {
                "item_id": {"type": "uuid", "value": item_id},
            },
            "options": {
                "deadline_millis": 10_000,
                "maximum_attempts": 3,
                "read_after_commit": read_after_commit,
                "cursor": null,
            }
        });
        let resp = self.roundtrip(&req)?;
        if resp["type"] != "result" {
            return Err(err(format!("item_page failed: {resp}")));
        }
        Ok(resp)
    }

    fn alloc_id(&mut self) -> String {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        format!("req-{id}")
    }

    fn roundtrip(&mut self, payload: &Value) -> TestResult<Value> {
        let encoded = serde_json::to_vec(payload)?;
        let len = u32::try_from(encoded.len())?;
        self.stream.write_all(&len.to_be_bytes())?;
        self.stream.write_all(&encoded)?;
        self.stream.flush()?;

        let mut header = [0_u8; 4];
        self.stream.read_exact(&mut header)?;
        let resp_len = usize::try_from(u32::from_be_bytes(header))?;
        if resp_len > 8 * 1024 * 1024 {
            return Err(err("driver frame exceeded 8 MiB bound"));
        }
        let mut body = vec![0_u8; resp_len];
        self.stream.read_exact(&mut body)?;
        Ok(serde_json::from_slice(&body)?)
    }
}

struct WorkspaceBinaries {
    riffdb: PathBuf,
    riffdbd: PathBuf,
    driverd: PathBuf,
}

impl WorkspaceBinaries {
    fn resolve() -> TestResult<Self> {
        let riffdb = PathBuf::from(env!("CARGO_BIN_EXE_riffdb"));
        let profile_dir = riffdb
            .parent()
            .ok_or_else(|| err("CARGO_BIN_EXE_riffdb has no parent"))?;
        let riffdbd = profile_dir.join("riffdbd");
        let driverd = profile_dir.join("riffdb-driverd");
        if !riffdbd.is_file() || !driverd.is_file() {
            let status = Command::new(env!("CARGO"))
                .current_dir(workspace_root())
                .args([
                    "build",
                    "--locked",
                    "--quiet",
                    "-p",
                    "riffdb-server",
                    "--bin",
                    "riffdbd",
                    "-p",
                    "riffdb-driver-host",
                    "--bin",
                    "riffdb-driverd",
                ])
                .status()?;
            if !status.success() {
                return Err(err("failed to build riffdbd and riffdb-driverd"));
            }
        }
        Ok(Self {
            riffdb,
            riffdbd,
            driverd,
        })
    }
}

struct TlsMaterial {
    server_pem: PathBuf,
    server_key: PathBuf,
    ca_pem: PathBuf,
}

impl TlsMaterial {
    fn copy_from_fixtures(work: &Path) -> TestResult<Self> {
        let fixtures = workspace_root().join("crates/riffdb-server/tests/fixtures");
        let server_pem = work.join("server.pem");
        let server_key = work.join("server.key");
        let ca_pem = work.join("ca.pem");
        fs::copy(fixtures.join("localhost-cert.pem"), &server_pem)?;
        fs::copy(fixtures.join("localhost-key.pem"), &server_key)?;
        fs::copy(fixtures.join("test-ca.pem"), &ca_pem)?;
        fs::set_permissions(&server_pem, fs::Permissions::from_mode(0o444))?;
        fs::set_permissions(&ca_pem, fs::Permissions::from_mode(0o444))?;
        fs::set_permissions(&server_key, fs::Permissions::from_mode(0o600))?;
        Ok(Self {
            server_pem,
            server_key,
            ca_pem,
        })
    }
}

fn write_multi_db_server_config(
    work: &Path,
    port: u16,
    certs: &TlsMaterial,
) -> TestResult<PathBuf> {
    let cap_keys = work.join("capability.keys");
    let idem_keys = work.join("idempotency.keys");
    write_mode_0600(&cap_keys, CAPABILITY_KEYS)?;
    write_mode_0600(&idem_keys, IDEMPOTENCY_KEYS)?;

    let data_dir = work.join("data");
    let backups_default = work.join("backups-default");
    let backups_recovery = work.join("backups-recovery");
    let projections_default = work.join("projections-default");
    let projections_recovery = work.join("projections-recovery");
    for dir in [
        &data_dir,
        &backups_default,
        &backups_recovery,
        &projections_default,
        &projections_recovery,
    ] {
        fs::create_dir_all(dir)?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    }

    let config_path = work.join("riffdbd.toml");
    let toml_body = format!(
        r#"[server]
audience = "{SERVER_AUDIENCE}"
capability_keys = "{cap_keys}"
idempotency_keys = "{idem_keys}"

[server.application_listener]
mode = "direct_tls"
listen = "127.0.0.1:{port}"
public_endpoint = "https://127.0.0.1:{port}"
certificate_chain = "{server_pem}"
private_key = "{server_key}"

[server.application_listener.bounds]
max_connections = 64
max_streams_per_connection = 128
handshake_timeout_seconds = 5
drain_timeout_seconds = 5

[databases.default]
path = "{default_db}"
backup_root = "{backups_default}"
projections_root = "{projections_default}"
environment = "development"

[databases.recovery_db]
path = "{recovery_db}"
backup_root = "{backups_recovery}"
projections_root = "{projections_recovery}"
environment = "development"
"#,
        cap_keys = path_str(&cap_keys)?,
        idem_keys = path_str(&idem_keys)?,
        server_pem = path_str(&certs.server_pem)?,
        server_key = path_str(&certs.server_key)?,
        default_db = path_str(&data_dir.join("default.redb"))?,
        backups_default = path_str(&backups_default)?,
        projections_default = path_str(&projections_default)?,
        recovery_db = path_str(&data_dir.join("recovery.redb"))?,
        backups_recovery = path_str(&backups_recovery)?,
        projections_recovery = path_str(&projections_recovery)?,
    );
    write_mode_0600(&config_path, toml_body.as_bytes())?;
    Ok(config_path)
}

struct DaemonProcess {
    stdin: Option<ChildStdin>,
    child: Option<Child>,
    ready: Receiver<io::Result<String>>,
    stdout: Option<JoinHandle<io::Result<()>>>,
    stderr: Option<JoinHandle<io::Result<String>>>,
}

impl DaemonProcess {
    fn spawn(binary: &Path, config: &Path) -> io::Result<Self> {
        let mut child = Command::new(binary)
            .arg("--config")
            .arg(config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
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
        let (tx, ready) = mpsc::sync_channel(1);
        let stdout_handle = thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut first = String::new();
            let res = reader.read_line(&mut first);
            let _ = tx.send(res.map(|_| first.trim_end().to_owned()));
            let mut sink = [0_u8; 1024];
            while reader.read(&mut sink)? > 0 {}
            Ok(())
        });
        let stderr_handle = thread::spawn(move || {
            let mut text = String::new();
            BufReader::new(stderr).read_to_string(&mut text)?;
            Ok(text)
        });
        Ok(Self {
            stdin: Some(stdin),
            child: Some(child),
            ready,
            stdout: Some(stdout_handle),
            stderr: Some(stderr_handle),
        })
    }

    fn wait_ready(&mut self) -> TestResult<SocketAddr> {
        let line = match self.ready.recv_timeout(START_TIMEOUT) {
            Ok(Ok(line)) if !line.is_empty() => line,
            other => {
                let stderr = self
                    .stderr
                    .take()
                    .and_then(|h| h.join().ok())
                    .and_then(Result::ok)
                    .unwrap_or_default();
                return Err(err(format!(
                    "riffdbd failed to report readiness ({other:?}): {stderr}"
                )));
            }
        };
        let addr_str = line
            .strip_prefix(READY_PREFIX)
            .ok_or_else(|| err(format!("unexpected readiness line: {line}")))?;
        Ok(addr_str.parse()?)
    }

    fn stop(&mut self) -> TestResult<()> {
        if let Some(mut stdin) = self.stdin.take() {
            let _ = stdin.write_all(b"shutdown\n");
            let _ = stdin.flush();
        }
        if let Some(mut child) = self.child.take() {
            let (tx, rx) = mpsc::sync_channel(1);
            thread::spawn(move || {
                let _ = tx.send(child.wait());
            });
            let status = rx
                .recv_timeout(STOP_TIMEOUT)
                .map_err(|_| err("riffdbd shutdown timed out"))??;
            if !status.success() {
                return Err(err(format!("riffdbd exited with {status}")));
            }
        }
        if let Some(h) = self.stdout.take() {
            let _ = h.join();
        }
        if let Some(h) = self.stderr.take() {
            let _ = h.join();
        }
        Ok(())
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        self.stdin.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct DriverProcess {
    child: Option<Child>,
    stderr: Option<JoinHandle<String>>,
}

impl DriverProcess {
    fn spawn(binary: &Path, config: &Path, socket_path: &Path) -> TestResult<Self> {
        if socket_path.exists() {
            let _ = fs::remove_file(socket_path);
        }
        let mut child = Command::new(binary)
            .arg(config)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let stderr_pipe = child
            .stderr
            .take()
            .ok_or_else(|| err("driverd missing stderr"))?;
        let stderr = thread::spawn(move || {
            let mut buf = String::new();
            let _ = BufReader::new(stderr_pipe).read_to_string(&mut buf);
            buf
        });
        let deadline = Instant::now() + START_TIMEOUT;
        while Instant::now() < deadline {
            if socket_path.exists() && UnixStream::connect(socket_path).is_ok() {
                return Ok(Self {
                    child: Some(child),
                    stderr: Some(stderr),
                });
            }
            if let Some(status) = child.try_wait()? {
                let err_text = stderr.join().unwrap_or_default();
                return Err(err(format!(
                    "riffdb-driverd exited early ({status}): {err_text}"
                )));
            }
            thread::yield_now();
        }
        let _ = child.kill();
        let _ = child.wait();
        let err_text = stderr.join().unwrap_or_default();
        Err(err(format!(
            "riffdb-driverd socket did not become ready: {err_text}"
        )))
    }

    fn pid(&self) -> u32 {
        self.child.as_ref().map_or(0, Child::id)
    }

    fn stop(&mut self) -> TestResult<()> {
        if let Some(mut child) = self.child.take() {
            let pid = child.id().to_string();
            let _ = Command::new("kill").args(["-TERM", &pid]).status();
            let (tx, rx) = mpsc::sync_channel(1);
            thread::spawn(move || {
                let _ = tx.send(child.wait());
            });
            let _ = rx
                .recv_timeout(STOP_TIMEOUT)
                .map_err(|_| err("riffdb-driverd stop timed out"))??;
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
        Ok(())
    }
}

impl Drop for DriverProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn run_cli(binary: &Path, cwd: &Path, args: &[&str]) -> io::Result<Output> {
    Command::new(binary)
        .current_dir(cwd)
        .args(args)
        .env_remove("RIFFDB_ENDPOINT")
        .env_remove("RIFFDB_CONFIG")
        .env_remove("RIFFDB_CREDENTIAL_FILE")
        .env_remove("RIFFDB_DATABASE")
        .output()
}

fn run_cli_json(binary: &Path, cwd: &Path, args: &[&str]) -> TestResult<Value> {
    let out = run_cli(binary, cwd, args)?;
    if !out.status.success() {
        return Err(err(format!(
            "CLI command {args:?} failed ({:?}): stdout={} stderr={}",
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        )));
    }
    Ok(serde_json::from_slice(&out.stdout)?)
}

fn reserve_loopback_port() -> io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn write_mode_0600(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn assert_mode_0600(path: &Path) -> TestResult<()> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(err(format!("{} is not a regular file", path.display())));
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode != 0o600 {
        return Err(err(format!(
            "expected mode 0600 on {}, got {:04o}",
            path.display(),
            mode
        )));
    }
    Ok(())
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "node_modules" || name == "target" || name == ".riffdb" {
            continue;
        }
        let file_type = entry.file_type()?;
        let target = dst.join(name);
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn parse_env_file(path: &Path) -> TestResult<BTreeMap<String, String>> {
    let text = fs::read_to_string(path)?;
    let mut map = BTreeMap::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let (k, v) = trimmed
            .split_once('=')
            .ok_or_else(|| err(format!("malformed env line: {trimmed}")))?;
        map.insert(k.to_owned(), v.to_owned());
    }
    Ok(map)
}

fn env_val<'a>(map: &'a BTreeMap<String, String>, key: &str) -> TestResult<&'a str> {
    map.get(key)
        .map(String::as_str)
        .ok_or_else(|| err(format!("missing {key} in driver.env")))
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn path_str(path: &Path) -> TestResult<&str> {
    path.to_str()
        .ok_or_else(|| err(format!("non-UTF-8 path: {}", path.display())))
}

fn err(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
