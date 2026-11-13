// req: TEST-001, TEST-003, TXN-001, TXN-002
//! End-to-end proof that concurrent duplicate idempotent commands mixed with
//! distinct sibling writes across unpublished group-commit epochs coalesce and
//! replay cleanly across gRPC, `riffdb-driverd`, and hosted MCP without
//! withdrawing the database (`StandardGroup` and `HardenedSync`).

use std::collections::BTreeMap;
use std::sync::Arc;

use riffdb_client_rust::{ApplicationErrorCode, ApplicationValue};
use riffdb_driver_host::{DriverResponse, DriverValue};
use tokio::sync::Barrier;

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
    let ws = TestWorkspace::new("pg-idempotency-epochs")?;
    let mut server = ws.spawn_server(&ServerOptions {
        commit_profile: profile,
        environment: "development",
        authorization_diagnostics: None,
    })?;
    ws.bootstrap_operator()?;
    let app = Arc::new(ws.lock_and_deploy("development")?);
    let mut driver = ws
        .spawn_driver_host(&app.role_credential_path, PRIMARY_ROLE, "driver-idem.sock")
        .await?;

    let (mut setup_client, setup_meta) = ws.connect_app_client(&app.role_credential_path).await?;
    let org_id = 1_000_u32;
    let shared_account_id = 1_001_u32;

    let open_shared = app.build_grpc_command(
        "OpenAccount",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String("open-shared-v1".to_owned()),
            ),
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(shared_account_id)),
            ("credit_limit".to_owned(), ApplicationValue::I64(100_000)),
            (
                "title".to_owned(),
                ApplicationValue::String("Primary Ledger".to_owned()),
            ),
        ]),
    )?;
    let opened = execute_grpc_command(&mut setup_client, &setup_meta, open_shared, 3).await?;
    let base_seq = opened
        .commit_sequence
        .ok_or("OpenAccount omitted commit_sequence")?;

    // Open 6 sibling accounts so distinct concurrent writes populate multi-unit
    // group-commit epochs simultaneously with the duplicate burst.
    for sibling_offset in 0..6_u32 {
        let cmd = app.build_grpc_command(
            "OpenAccount",
            BTreeMap::from([
                (
                    "req_key".to_owned(),
                    ApplicationValue::String(format!("open-sib-{sibling_offset}")),
                ),
                ("org_id".to_owned(), app_uuid(org_id)),
                ("account_id".to_owned(), app_uuid(1_100 + sibling_offset)),
                ("credit_limit".to_owned(), ApplicationValue::I64(50_000)),
                (
                    "title".to_owned(),
                    ApplicationValue::String(format!("Sibling {sibling_offset}")),
                ),
            ]),
        )?;
        execute_grpc_command(&mut setup_client, &setup_meta, cmd, 3).await?;
    }

    // Reuse one persistent MCP session across the 3 waves so observer startups do not
    // exhaust the per-principal 16-burst MCP discovery admission bucket (`MCP_POST_AUTH_MAX_BURST`).
    let shared_mcp = Arc::new(crate::support::McpSession::connect(
        ws.mcp_address(),
        &app.role_credential_path,
    )?);

    // Run 3 waves of concurrent duplicate PostCharge submissions across:
    // - 6 independent gRPC connections
    // - 5 independent riffdb-driverd Unix-socket sessions
    // - 1 hosted MCP HTTP session
    // - 6 distinct sibling PostCharge writes (forcing concurrent group epochs)
    for wave in 1..=3_u32 {
        let dup_key = format!("dup-wave-{wave}");
        let charge_id = 2_000 + wave;
        let amount = 250_i64;
        let memo = format!("Wave {wave} settlement");

        let total_participants = 6 + 5 + 1 + 6;
        let barrier = Arc::new(Barrier::new(total_participants));

        let mut dup_grpc_handles = Vec::with_capacity(6);
        for _ in 0..6 {
            let (mut client, meta) = ws.connect_app_client(&app.role_credential_path).await?;
            let app_ref = Arc::clone(&app);
            let barrier_ref = Arc::clone(&barrier);
            let key_clone = dup_key.clone();
            let memo_clone = memo.clone();
            dup_grpc_handles.push(tokio::spawn(async move {
                let cmd = app_ref
                    .build_grpc_command(
                        "PostCharge",
                        BTreeMap::from([
                            ("req_key".to_owned(), ApplicationValue::String(key_clone)),
                            ("org_id".to_owned(), app_uuid(org_id)),
                            ("account_id".to_owned(), app_uuid(shared_account_id)),
                            ("charge_id".to_owned(), app_uuid(charge_id)),
                            ("amount".to_owned(), ApplicationValue::I64(amount)),
                            ("memo".to_owned(), ApplicationValue::String(memo_clone)),
                        ]),
                    )
                    .map_err(|e| e.to_string())?;
                barrier_ref.wait().await;
                let res = execute_grpc_command(&mut client, &meta, cmd, 3)
                    .await
                    .map_err(|e| format!("gRPC duplicate failed: {e}"))?;
                Ok::<(u64, bool), String>((res.commit_sequence.ok_or("missing seq")?, res.replayed))
            }));
        }

        let mut dup_driver_handles = Vec::with_capacity(5);
        for _ in 0..5 {
            let mut sock = driver.connect(PRIMARY_ROLE).await?;
            let app_ref = Arc::clone(&app);
            let barrier_ref = Arc::clone(&barrier);
            let key_clone = dup_key.clone();
            let memo_clone = memo.clone();
            dup_driver_handles.push(tokio::spawn(async move {
                let input = BTreeMap::from([
                    ("req_key".to_owned(), DriverValue::String(key_clone)),
                    ("org_id".to_owned(), DriverValue::Uuid(uuid_str(org_id))),
                    (
                        "account_id".to_owned(),
                        DriverValue::Uuid(uuid_str(shared_account_id)),
                    ),
                    (
                        "charge_id".to_owned(),
                        DriverValue::Uuid(uuid_str(charge_id)),
                    ),
                    ("amount".to_owned(), DriverValue::I64(amount.to_string())),
                    ("memo".to_owned(), DriverValue::String(memo_clone)),
                ]);
                barrier_ref.wait().await;
                let resp = execute_driver_command(&mut sock, &app_ref, "PostCharge", input, 3)
                    .await
                    .map_err(|e| {
                        format!("driver duplicate failed: {:?} {:?}", e.kind, e.details)
                    })?;
                match resp {
                    DriverResponse::Result {
                        application_head: Some(seq),
                        replayed,
                        ..
                    } => Ok::<(u64, bool), String>((seq, replayed)),
                    other => Err(format!("unexpected driver response: {other:?}")),
                }
            }));
        }

        let mcp_tool = app.mcp_tool_name("PostCharge")?;
        let mut dup_mcp_handles = Vec::with_capacity(1);
        {
            let mcp = Arc::clone(&shared_mcp);
            let barrier_ref = Arc::clone(&barrier);
            let tool_name = mcp_tool.clone();
            let key_clone = dup_key.clone();
            let memo_clone = memo.clone();
            dup_mcp_handles.push(tokio::spawn(async move {
                barrier_ref.wait().await;
                tokio::task::spawn_blocking(move || {
                    let resp = mcp
                        .call_tool(
                            &tool_name,
                            serde_json::json!({
                                "req_key": key_clone,
                                "org_id": uuid_str(org_id),
                                "account_id": uuid_str(shared_account_id),
                                "charge_id": uuid_str(charge_id),
                                "amount": amount,
                                "memo": memo_clone,
                            }),
                        )
                        .map_err(|e| e.to_string())?;
                    if resp["result"]["isError"].as_bool() == Some(true) {
                        return Err(format!("MCP duplicate returned tool error: {resp}"));
                    }
                    let structured = &resp["result"]["structuredContent"];
                    let seq_str = structured["commit_sequence"]
                        .as_str()
                        .ok_or_else(|| format!("MCP missing commit_sequence: {resp}"))?;
                    let seq = seq_str.parse::<u64>().map_err(|e| e.to_string())?;
                    let status = structured["status"]
                        .as_str()
                        .ok_or_else(|| format!("MCP missing status: {resp}"))?;
                    let replayed = match status {
                        "committed" => false,
                        "replayed" => true,
                        other => return Err(format!("unexpected MCP status {other}: {resp}")),
                    };
                    Ok::<(u64, bool), String>((seq, replayed))
                })
                .await
                .map_err(|e| e.to_string())?
            }));
        }

        let mut sibling_handles = Vec::with_capacity(6);
        for sib in 0..6_u32 {
            let (mut client, meta) = ws.connect_app_client(&app.role_credential_path).await?;
            let app_ref = Arc::clone(&app);
            let barrier_ref = Arc::clone(&barrier);
            sibling_handles.push(tokio::spawn(async move {
                let cmd = app_ref
                    .build_grpc_command(
                        "PostCharge",
                        BTreeMap::from([
                            (
                                "req_key".to_owned(),
                                ApplicationValue::String(format!("sib-wave-{wave}-{sib}")),
                            ),
                            ("org_id".to_owned(), app_uuid(org_id)),
                            ("account_id".to_owned(), app_uuid(1_100 + sib)),
                            ("charge_id".to_owned(), app_uuid(3_000 + wave * 10 + sib)),
                            ("amount".to_owned(), ApplicationValue::I64(100)),
                            (
                                "memo".to_owned(),
                                ApplicationValue::String("Sibling charge".to_owned()),
                            ),
                        ]),
                    )
                    .map_err(|e| e.to_string())?;
                barrier_ref.wait().await;
                let res = execute_grpc_command(&mut client, &meta, cmd, 3)
                    .await
                    .map_err(|e| format!("sibling write failed: {e}"))?;
                if res.replayed {
                    return Err("distinct sibling write unexpectedly replayed".to_owned());
                }
                res.commit_sequence
                    .ok_or_else(|| "missing sibling seq".to_owned())
            }));
        }

        let mut observations = Vec::with_capacity(12);
        for handle in dup_grpc_handles {
            observations.push(handle.await??);
        }
        for handle in dup_driver_handles {
            observations.push(handle.await??);
        }
        for handle in dup_mcp_handles {
            observations.push(handle.await??);
        }
        for handle in sibling_handles {
            let sib_seq = handle.await??;
            assert!(sib_seq > base_seq);
        }

        let first_seq = observations[0].0;
        let mut committed_count = 0_usize;
        let mut replayed_count = 0_usize;
        for &(seq, replayed) in &observations {
            assert_eq!(
                seq, first_seq,
                "all 12 duplicate callers in wave {wave} must observe identical commit_sequence"
            );
            if replayed {
                replayed_count += 1;
            } else {
                committed_count += 1;
            }
        }
        assert_eq!(
            committed_count, 1,
            "wave {wave} ({profile:?}): expected exactly 1 initial commit across 12 duplicates"
        );
        assert_eq!(
            replayed_count, 11,
            "wave {wave} ({profile:?}): expected 11 replays across 12 duplicates"
        );
    }

    // Reusing an existing idempotency key with a mutated payload MUST fail closed
    // with RDB-COMMAND-0101 across public gRPC and leave the account state intact.
    let mismatch_cmd = app.build_grpc_command(
        "PostCharge",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String("dup-wave-1".to_owned()),
            ),
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(shared_account_id)),
            ("charge_id".to_owned(), app_uuid(2_001)),
            ("amount".to_owned(), ApplicationValue::I64(999)),
            (
                "memo".to_owned(),
                ApplicationValue::String("Mutated".to_owned()),
            ),
        ]),
    )?;
    let mismatch_err = execute_grpc_command(&mut setup_client, &setup_meta, mismatch_cmd, 1)
        .await
        .expect_err("mutated payload on existing idempotency key must fail");
    let semantic = mismatch_err
        .semantic_error()
        .ok_or("key-reuse error omitted semantic ApplicationError")?;
    assert_eq!(semantic.code(), ApplicationErrorCode::IdempotencyKeyReuse);
    assert_eq!(semantic.code().as_str(), "RDB-COMMAND-0101");

    // Subsequent fresh writes and fenced reads succeed cleanly (database was never withdrawn).
    let final_charge = app.build_grpc_command(
        "PostCharge",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String("post-storm-check".to_owned()),
            ),
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(shared_account_id)),
            ("charge_id".to_owned(), app_uuid(2_999)),
            ("amount".to_owned(), ApplicationValue::I64(50)),
            (
                "memo".to_owned(),
                ApplicationValue::String("Post storm".to_owned()),
            ),
        ]),
    )?;
    let final_res = execute_grpc_command(&mut setup_client, &setup_meta, final_charge, 3).await?;
    assert!(!final_res.replayed);
    let final_seq = final_res.commit_sequence.ok_or("missing final seq")?;

    let get_query = app.build_grpc_query(
        "GetAccount",
        BTreeMap::from([
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(shared_account_id)),
        ]),
        None,
        Some(final_seq),
    )?;
    let queried = execute_grpc_query(&mut setup_client, &setup_meta, get_query).await?;
    assert_eq!(queried.outcome, "Found");
    let account_record = &queried.fields["account"].records[0];
    assert_eq!(
        account_record.fields.get("posted_total"),
        Some(&ApplicationValue::I64(3 * 250 + 50)),
        "3 duplicate waves * 250 + 50 post-storm charge must equal 800"
    );

    driver.shutdown().await?;
    server.shutdown()?;
    Ok(())
}
