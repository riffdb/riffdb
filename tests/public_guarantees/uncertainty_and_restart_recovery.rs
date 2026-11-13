// req: TXN-002, DRV-005, DX-003
//! End-to-end proof that post-send network / socket disconnects after request
//! bytes reach `riffdbd` / `riffdb-driverd` classify as `OutcomeUnknown`
//! (never silently auto-retrying as non-idempotent or reporting definitive
//! rollback), and that restarting `riffdbd` + `riffdb-driverd` on the same
//! `redb` database replays the durable outcome cleanly under the original
//! `idempotency_key`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use riffdb_client_rust::{ApplicationClientError, ApplicationValue, ClientError};
use riffdb_driver_host::{DriverResponse, DriverSocketState, DriverValue, FrameCodec};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

use crate::support::{
    CommitProfile, PRIMARY_ROLE, ServerOptions, TestResult, TestWorkspace, app_uuid,
    execute_driver_command, execute_grpc_command, execute_grpc_query, uuid_str,
};

pub(crate) async fn run() -> TestResult<()> {
    for profile in [CommitProfile::StandardGroup, CommitProfile::HardenedSync] {
        run_for_profile(profile).await?;
    }
    Ok(())
}

async fn run_for_profile(profile: CommitProfile) -> TestResult<()> {
    let ws = TestWorkspace::new("pg-uncertainty-restart")?;
    let server_opts = ServerOptions {
        commit_profile: profile,
        environment: "development",
        authorization_diagnostics: None,
    };
    let mut server = ws.spawn_server(&server_opts)?;
    ws.bootstrap_operator()?;
    let app = ws.lock_and_deploy("development")?;
    let mut driver = ws
        .spawn_driver_host(&app.role_credential_path, PRIMARY_ROLE, "d1.sock")
        .await?;

    let org_id = 9_000_u32;
    let account_id = 9_001_u32;

    // 1. Start a deterministic TCP fault proxy in front of `riffdbd`'s TLS port.
    let tcp_proxy = TcpDropResponseProxy::spawn(ws.grpc_port).await?;
    let (mut proxied_grpc, proxied_meta) = ws
        .connect_app_client_on_port(tcp_proxy.listen_port, &app.role_credential_path)
        .await?;

    // Warm up the TLS 1.3 + HTTP/2 connection through the proxy by opening the account.
    let open_cmd = app.build_grpc_command(
        "OpenAccount",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String("uncertain-open-v1".to_owned()),
            ),
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(account_id)),
            ("credit_limit".to_owned(), ApplicationValue::I64(10_000)),
            (
                "title".to_owned(),
                ApplicationValue::String("Uncertain Recovery Account".to_owned()),
            ),
        ]),
    )?;
    let open_res = execute_grpc_command(&mut proxied_grpc, &proxied_meta, open_cmd, 3).await?;
    assert!(!open_res.replayed);

    // Arm the TCP proxy: forward the client's HTTP/2 command frame to `riffdbd`,
    // wait until `riffdbd` sends back the first TLS response bytes (proving the
    // commit succeeded on `redb`), then sever the client TCP stream without
    // delivering those response bytes.
    tcp_proxy.arm_drop_next_server_reply();

    let grpc_uncertain_key = "uncertain-grpc-charge-v1";
    let grpc_charge_cmd = app.build_grpc_command(
        "PostCharge",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String(grpc_uncertain_key.to_owned()),
            ),
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(account_id)),
            ("charge_id".to_owned(), app_uuid(9_101)),
            ("amount".to_owned(), ApplicationValue::I64(400)),
            (
                "memo".to_owned(),
                ApplicationValue::String("Post-send TCP drop".to_owned()),
            ),
        ]),
    )?;

    // With a single attempt (`attempts = 1`), `StableApplicationClient` MUST classify
    // the severed post-send TLS stream as `OutcomeUnknown` (never definitive rollback
    // or non-uncertain transport failure), leaving retry recovery to replay the same
    // `idempotency_key` after `riffdbd` restarts below!
    let grpc_err =
        execute_grpc_command(&mut proxied_grpc, &proxied_meta, grpc_charge_cmd.clone(), 1)
            .await
            .expect_err("severed post-send TLS stream must surface OutcomeUnknown");
    assert!(
        matches!(
            grpc_err,
            ApplicationClientError::Client(ClientError::OutcomeUnknown(_))
        ),
        "expected ClientError::OutcomeUnknown from StableApplicationClient, got: {grpc_err:?}"
    );

    // 2. Start a deterministic Unix-socket fault proxy in front of `riffdb-driverd`.
    let proxy_sock_path = ws.root().join("p1.sock");
    let unix_proxy =
        UnixDropResponseProxy::spawn(proxy_sock_path.clone(), driver.socket_path.clone())?;
    let mut proxied_driver_sock = DriverSocketState::connect(
        proxy_sock_path.to_str().ok_or("proxy socket UTF-8")?,
        Some("default"),
        Some(PRIMARY_ROLE),
    )
    .await
    .map_err(|e| format!("proxy socket connect failed: {:?}", e.kind))?;

    // Warm up the verified handshake session on the Unix-socket proxy using a read query.
    let warmup_query = crate::support::execute_driver_query(
        &mut proxied_driver_sock,
        &app,
        "GetAccount",
        BTreeMap::from([
            ("org_id".to_owned(), DriverValue::Uuid(uuid_str(org_id))),
            (
                "account_id".to_owned(),
                DriverValue::Uuid(uuid_str(account_id)),
            ),
        ]),
        None,
        None,
    )
    .await
    .map_err(|e| format!("driver warmup query failed: {:?}", e.kind))?;
    assert!(matches!(warmup_query, DriverResponse::Result { .. }));

    // Arm the Unix-socket proxy to forward the next `DriverRequest::Invoke` frame
    // to `riffdb-driverd`, read `riffdb-driverd`'s `DriverResponse::Result` frame
    // (proving commit), and close the client socket before writing any response byte.
    unix_proxy.arm_drop_next_invoke_reply();

    let driver_uncertain_key = "uncertain-driver-charge-v1";
    let driver_input = BTreeMap::from([
        (
            "req_key".to_owned(),
            DriverValue::String(driver_uncertain_key.to_owned()),
        ),
        ("org_id".to_owned(), DriverValue::Uuid(uuid_str(org_id))),
        (
            "account_id".to_owned(),
            DriverValue::Uuid(uuid_str(account_id)),
        ),
        ("charge_id".to_owned(), DriverValue::Uuid(uuid_str(9_102))),
        ("amount".to_owned(), DriverValue::I64("350".to_owned())),
        (
            "memo".to_owned(),
            DriverValue::String("Post-send Unix drop".to_owned()),
        ),
    ]);
    let driver_err = execute_driver_command(
        &mut proxied_driver_sock,
        &app,
        "PostCharge",
        driver_input.clone(),
        3,
    )
    .await
    .expect_err("severed post-send Unix socket must surface outcome_unknown");
    assert_eq!(
        driver_err.kind, "outcome_unknown",
        "DriverSocketState must classify post-send command EOF as outcome_unknown"
    );

    drop(proxied_grpc);
    drop(proxied_driver_sock);
    drop(unix_proxy);
    drop(tcp_proxy);

    // 3. Shut down BOTH `riffdb-driverd` AND `riffdbd`, then restart both on
    // the exact same `redb` database file (`ws.database_path`).
    driver.shutdown().await?;
    server.shutdown()?;

    let mut restarted_server = ws.spawn_server(&server_opts)?;
    let mut restarted_driver = ws
        .spawn_driver_host(&app.role_credential_path, PRIMARY_ROLE, "d2.sock")
        .await?;

    // 4. Replay both uncertain commands with their original `idempotency_key`s
    // and payloads against the restarted processes: both MUST return `replayed = true`
    // and the account balance MUST equal `400 + 350 = 750` (single application each).
    let (mut recovery_grpc, recovery_meta) =
        ws.connect_app_client(&app.role_credential_path).await?;
    let grpc_replay =
        execute_grpc_command(&mut recovery_grpc, &recovery_meta, grpc_charge_cmd, 3).await?;
    assert!(
        grpc_replay.replayed,
        "replaying post-send uncertain gRPC command after restart must replay stored outcome"
    );
    let grpc_seq = grpc_replay
        .commit_sequence
        .ok_or("missing replayed gRPC seq")?;

    let mut recovery_sock = restarted_driver.connect(PRIMARY_ROLE).await?;
    let driver_replay =
        execute_driver_command(&mut recovery_sock, &app, "PostCharge", driver_input, 3)
            .await
            .map_err(|e| {
                format!(
                    "driver replay after restart failed: {:?} {:?}",
                    e.kind, e.details
                )
            })?;
    let DriverResponse::Result {
        application_head: Some(driver_seq),
        replayed: driver_replayed,
        ..
    } = driver_replay
    else {
        return Err(format!("unexpected driver replay response: {driver_replay:?}").into());
    };
    assert!(
        driver_replayed,
        "replaying post-send uncertain driver command after restart must replay stored outcome"
    );

    let max_seq = grpc_seq.max(driver_seq);
    let check_q = app.build_grpc_query(
        "GetAccount",
        BTreeMap::from([
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(account_id)),
        ]),
        None,
        Some(max_seq),
    )?;
    let check_res = execute_grpc_query(&mut recovery_grpc, &recovery_meta, check_q).await?;
    assert_eq!(check_res.outcome, "Found");
    assert_eq!(
        check_res.fields["account"].records[0]
            .fields
            .get("posted_total"),
        Some(&ApplicationValue::I64(750)),
        "exact once-only debit across restart recovery: 400 + 350 == 750"
    );

    restarted_driver.shutdown().await?;
    restarted_server.shutdown()?;
    Ok(())
}

struct TcpDropResponseProxy {
    listen_port: u16,
    arm_drop: Arc<AtomicBool>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl TcpDropResponseProxy {
    async fn spawn(upstream_port: u16) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listen_port = listener.local_addr()?.port();
        let arm_drop = Arc::new(AtomicBool::new(false));
        let arm_ref = Arc::clone(&arm_drop);

        let accept_task = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(upstream) = TcpStream::connect(("127.0.0.1", upstream_port)).await else {
                    return;
                };
                let arm_conn = Arc::clone(&arm_ref);
                tokio::spawn(async move {
                    let _ = relay_tcp_until_armed_drop(client, upstream, arm_conn).await;
                });
            }
        });

        Ok(Self {
            listen_port,
            arm_drop,
            accept_task,
        })
    }

    fn arm_drop_next_server_reply(&self) {
        self.arm_drop.store(true, Ordering::SeqCst);
    }
}

impl Drop for TcpDropResponseProxy {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

async fn relay_tcp_until_armed_drop(
    mut client: TcpStream,
    mut upstream: TcpStream,
    arm_drop: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let mut c2u_buf = vec![0_u8; 16 * 1024];
    let mut u2c_buf = vec![0_u8; 16 * 1024];
    let mut drop_next_upstream_chunk = false;

    loop {
        tokio::select! {
            read_client = client.read(&mut c2u_buf) => {
                let n = read_client?;
                if n == 0 {
                    return Ok(());
                }
                if arm_drop.swap(false, Ordering::SeqCst) {
                    drop_next_upstream_chunk = true;
                }
                upstream.write_all(&c2u_buf[..n]).await?;
                upstream.flush().await?;
            }
            read_upstream = upstream.read(&mut u2c_buf) => {
                let n = read_upstream?;
                if n == 0 {
                    return Ok(());
                }
                if drop_next_upstream_chunk {
                    // Upstream `riffdbd` processed the request and sent response
                    // bytes; sever client socket without delivering them!
                    let _ = client.shutdown().await;
                    return Ok(());
                }
                client.write_all(&u2c_buf[..n]).await?;
                client.flush().await?;
            }
        }
    }
}

struct UnixDropResponseProxy {
    socket_path: PathBuf,
    arm_drop: Arc<AtomicBool>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl UnixDropResponseProxy {
    fn spawn(listen_path: PathBuf, upstream_path: PathBuf) -> TestResult<Self> {
        let _ = std::fs::remove_file(&listen_path);
        let listener = UnixListener::bind(&listen_path)?;
        let arm_drop = Arc::new(AtomicBool::new(false));
        let arm_ref = Arc::clone(&arm_drop);

        let accept_task = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(upstream) = UnixStream::connect(&upstream_path).await else {
                    return;
                };
                let arm_conn = Arc::clone(&arm_ref);
                tokio::spawn(async move {
                    let _ = relay_unix_frames(client, upstream, arm_conn).await;
                });
            }
        });

        Ok(Self {
            socket_path: listen_path,
            arm_drop,
            accept_task,
        })
    }

    fn arm_drop_next_invoke_reply(&self) {
        self.arm_drop.store(true, Ordering::SeqCst);
    }
}

impl Drop for UnixDropResponseProxy {
    fn drop(&mut self) {
        self.accept_task.abort();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

async fn relay_unix_frames(
    mut client: UnixStream,
    mut upstream: UnixStream,
    arm_drop: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let req = FrameCodec::read_request(&mut client)
            .await
            .map_err(|_| "client closed or frame error")?;
        let drop_reply = matches!(req, riffdb_driver_host::DriverRequest::Invoke { .. })
            && arm_drop.swap(false, Ordering::SeqCst);
        let encoded_req = FrameCodec::encode_request(&req).map_err(|_| "encode_request")?;
        upstream.write_all(&encoded_req).await?;
        upstream.flush().await?;

        let resp = FrameCodec::read_response(&mut upstream)
            .await
            .map_err(|_| "upstream read_response")?;
        if drop_reply {
            // `riffdb-driverd` finished executing the command and returned `resp`;
            // drop the client socket immediately without writing a single byte!
            let _ = client.shutdown().await;
            return Ok(());
        }
        let encoded_resp = FrameCodec::encode_response(&resp).map_err(|_| "encode_response")?;
        client.write_all(&encoded_resp).await?;
        client.flush().await?;
    }
}
