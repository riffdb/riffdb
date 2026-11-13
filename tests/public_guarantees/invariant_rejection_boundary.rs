// req: TEST-001, CMP-001, DX-003
//! End-to-end proof that a post-mutation `.riff` aggregate `invariant` failure
//! rolls back candidate mutations, exhausts evaluation attempts without
//! poisoning the coordinator or terminalizing the idempotency key, and maps to
//! `RDB-INPUT-0101` (`category: "input"`, `recovery: "correct_request"`) —
//! never `RDB-APP-0003` (`deadline_exceeded`) — across gRPC, `riffdb-driverd`,
//! hosted MCP, and `riffdb` CLI JSON.

use std::collections::BTreeMap;

use riffdb_client_rust::{ApplicationErrorCode, ApplicationValue};
use riffdb_driver_host::DriverValue;

use crate::support::{
    CommitProfile, McpSession, PRIMARY_ROLE, ServerOptions, TestResult, TestWorkspace, app_uuid,
    execute_driver_command, execute_grpc_command, execute_grpc_query, run_cli, uuid_str,
};

pub(crate) async fn run() -> TestResult<()> {
    for profile in [CommitProfile::StandardGroup, CommitProfile::HardenedSync] {
        run_for_profile(profile).await?;
    }
    Ok(())
}

async fn run_for_profile(profile: CommitProfile) -> TestResult<()> {
    let ws = TestWorkspace::new("pg-invariant-boundary")?;
    let mut server = ws.spawn_server(&ServerOptions {
        commit_profile: profile,
        environment: "development",
        authorization_diagnostics: None,
    })?;
    ws.bootstrap_operator()?;
    let app = ws.lock_and_deploy("development")?;
    let mut driver = ws
        .spawn_driver_host(&app.role_credential_path, PRIMARY_ROLE, "driver-inv.sock")
        .await?;

    let (mut grpc_client, grpc_meta) = ws.connect_app_client(&app.role_credential_path).await?;
    let org_id = 5_000_u32;
    let account_id = 5_001_u32;

    // Seed an account with credit_limit = 500 at sequence S.
    let open_cmd = app.build_grpc_command(
        "OpenAccount",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String("inv-open-v1".to_owned()),
            ),
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(account_id)),
            ("credit_limit".to_owned(), ApplicationValue::I64(500)),
            (
                "title".to_owned(),
                ApplicationValue::String("Invariant Account".to_owned()),
            ),
        ]),
    )?;
    let open_res = execute_grpc_command(&mut grpc_client, &grpc_meta, open_cmd, 3).await?;
    let seq_before = open_res.commit_sequence.ok_or("missing open seq")?;

    let shared_rejected_key = "inv-rejected-reusable-key";

    // 1. Direct verified-TLS gRPC surface (`StableApplicationClient`):
    let bad_grpc = app.build_grpc_command(
        "PostCharge",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String(shared_rejected_key.to_owned()),
            ),
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(account_id)),
            ("charge_id".to_owned(), app_uuid(5_101)),
            ("amount".to_owned(), ApplicationValue::I64(600)),
            (
                "memo".to_owned(),
                ApplicationValue::String("Over limit gRPC".to_owned()),
            ),
        ]),
    )?;
    let grpc_err = execute_grpc_command(&mut grpc_client, &grpc_meta, bad_grpc, 3)
        .await
        .expect_err("violating within_credit_limit must fail on gRPC");
    let semantic = grpc_err
        .semantic_error()
        .ok_or("gRPC invariant failure omitted semantic error")?;
    assert_eq!(semantic.code(), ApplicationErrorCode::InputInvalid);
    assert_eq!(semantic.code().as_str(), "RDB-INPUT-0101");
    assert_ne!(semantic.code().as_str(), "RDB-APP-0003");
    assert_eq!(semantic.category().as_str(), "input");
    assert_eq!(semantic.recovery_action().as_str(), "correct_request");

    // 2. `riffdb-driverd` Unix-socket surface (`DriverSocketState`):
    let mut driver_sock = driver.connect(PRIMARY_ROLE).await?;
    let driver_err = execute_driver_command(
        &mut driver_sock,
        &app,
        "PostCharge",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                DriverValue::String("inv-rejected-driver".to_owned()),
            ),
            ("org_id".to_owned(), DriverValue::Uuid(uuid_str(org_id))),
            (
                "account_id".to_owned(),
                DriverValue::Uuid(uuid_str(account_id)),
            ),
            ("charge_id".to_owned(), DriverValue::Uuid(uuid_str(5_102))),
            ("amount".to_owned(), DriverValue::I64("750".to_owned())),
            (
                "memo".to_owned(),
                DriverValue::String("Over limit driver".to_owned()),
            ),
        ]),
        3,
    )
    .await
    .expect_err("violating within_credit_limit must fail on riffdb-driverd");
    assert_eq!(driver_err.kind, "application");
    let driver_details = driver_err
        .details
        .as_ref()
        .ok_or("driver application error omitted details")?;
    assert_eq!(driver_details["code"].as_str(), Some("RDB-INPUT-0101"));
    assert_eq!(driver_details["category"].as_str(), Some("input"));
    assert_eq!(
        driver_details["recovery_action"].as_str(),
        Some("correct_request")
    );

    // 3. Hosted MCP Streamable HTTP surface (`POST /mcp` `tools/call`):
    let mcp = McpSession::connect(ws.mcp_address(), &app.role_credential_path)?;
    let post_charge_tool = app.mcp_tool_name("PostCharge")?;
    let mcp_resp = mcp.call_tool(
        &post_charge_tool,
        serde_json::json!({
            "req_key": "inv-rejected-mcp",
            "org_id": uuid_str(org_id),
            "account_id": uuid_str(account_id),
            "charge_id": uuid_str(5_103),
            "amount": 900,
            "memo": "Over limit MCP"
        }),
    )?;
    assert_eq!(
        mcp_resp["result"]["isError"].as_bool(),
        Some(true),
        "MCP tools/call must report tool error on invariant violation: {mcp_resp}"
    );
    let mcp_text = mcp_resp["result"]["content"][0]["text"]
        .as_str()
        .ok_or_else(|| format!("MCP error omitted content[0].text: {mcp_resp}"))?;
    let mcp_err_json: serde_json::Value = serde_json::from_str(mcp_text)?;
    assert_eq!(
        mcp_err_json["code"].as_str(),
        Some("validation_failed"),
        "MCP invariant error must surface validation_failed: {mcp_err_json}"
    );
    assert_eq!(
        mcp_err_json["class"].as_str(),
        Some("invalid_argument"),
        "MCP invariant error must classify as invalid_argument: {mcp_err_json}"
    );
    assert_eq!(
        mcp_err_json["recovery_action"].as_str(),
        Some("correct_request"),
        "MCP invariant error must advise correct_request: {mcp_err_json}"
    );
    assert_ne!(
        mcp_err_json["code"].as_str(),
        Some("deadline_exceeded"),
        "MCP invariant error must never surface deadline_exceeded: {mcp_err_json}"
    );

    // 4. `riffdb` CLI JSON surface (`command run`):
    let cli_input = serde_json::json!({
        "req_key": "inv-rejected-cli",
        "org_id": uuid_str(org_id),
        "account_id": uuid_str(account_id),
        "charge_id": uuid_str(5_104),
        "amount": 650,
        "memo": "Over limit CLI"
    });
    let cli_exit = run_cli(
        [
            "--config".into(),
            app.role_client_config_path.clone().into_os_string(),
            "command".into(),
            "run".into(),
            "PostCharge".into(),
            "--input-json".into(),
            cli_input.to_string().into(),
        ],
        Some(&ws.app_dir),
    )?;
    assert!(
        !cli_exit.status.success(),
        "CLI command run must exit non-zero on invariant violation"
    );
    let cli_combined = format!(
        "{}{}",
        String::from_utf8_lossy(&cli_exit.stdout),
        String::from_utf8_lossy(&cli_exit.stderr)
    );
    assert!(
        cli_combined.contains("RDB-INPUT-0101"),
        "CLI invariant output must report RDB-INPUT-0101: {cli_combined}"
    );
    assert!(
        !cli_combined.contains("RDB-APP-0003"),
        "CLI invariant output must never report RDB-APP-0003: {cli_combined}"
    );

    // 5. Prove candidate mutations rolled back AND the rejected idempotency key
    // was NOT persisted as a terminal failure/commit: submitting a corrected
    // valid command (`amount = 300 <= 500`) with `shared_rejected_key` commits
    // at `seq_before + 1` with zero sequence gap!
    let corrected_cmd = app.build_grpc_command(
        "PostCharge",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String(shared_rejected_key.to_owned()),
            ),
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(account_id)),
            ("charge_id".to_owned(), app_uuid(5_101)),
            ("amount".to_owned(), ApplicationValue::I64(300)),
            (
                "memo".to_owned(),
                ApplicationValue::String("Within limit".to_owned()),
            ),
        ]),
    )?;
    let corrected_res =
        execute_grpc_command(&mut grpc_client, &grpc_meta, corrected_cmd, 3).await?;
    assert!(!corrected_res.replayed);
    assert_eq!(
        corrected_res.commit_sequence,
        Some(seq_before + 1),
        "rejected invariant checks must not consume commit sequences"
    );

    let verify_query = app.build_grpc_query(
        "GetAccount",
        BTreeMap::from([
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(account_id)),
        ]),
        None,
        corrected_res.commit_sequence,
    )?;
    let verify = execute_grpc_query(&mut grpc_client, &grpc_meta, verify_query).await?;
    assert_eq!(
        verify.fields["account"].records[0]
            .fields
            .get("posted_total"),
        Some(&ApplicationValue::I64(300))
    );

    driver.shutdown().await?;
    server.shutdown()?;
    Ok(())
}
