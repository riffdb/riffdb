// req: QRY-001, QRY-004, AAA-003, AAA-004, DX-003
//! End-to-end proof of agreement across query planning, compiled role scan
//! budgets, runtime capability admission, dedicated `authorization_diagnostic`
//! propagation (`AAA-004` `symbol_path` preservation), and environment-default
//! `authorization_diagnostics` redaction (`development` vs `production`).

use std::collections::BTreeMap;
use std::fs;

use riffdb_client_rust::{
    ApplicationErrorCode, ApplicationValue, CallMetadata, DatabaseAlias, generate_request_id,
    load_protected_bearer_credential, v1,
};
use riffdb_driver_host::DriverValue;
use riffdb_errors::AuthorizationDiagnosticCode;

use crate::support::{
    CONTRACT_LINEAGE, CommitProfile, GRPC_AUDIENCE, McpSession, PK_SET_ONLY_ROLE, PRIMARY_ROLE,
    ServerOptions, TestResult, TestWorkspace, app_uuid, execute_driver_query, execute_grpc_command,
    execute_grpc_query, run_cli, uuid_str,
};

pub(crate) async fn run() -> TestResult<()> {
    verify_compile_time_over_budget_role_refusal()?;
    verify_development_query_and_diagnostic_agreement().await?;
    verify_production_default_redaction_and_override().await?;
    Ok(())
}

fn verify_compile_time_over_budget_role_refusal() -> TestResult<()> {
    let ws = TestWorkspace::new("pg-role-compile-refusal")?;
    ws.write_guarantee_application("development")?;

    // First prove the baseline application manifest and queries lock cleanly,
    // so the subsequent failure can only come from the over-ceiling query/role.
    let baseline = run_cli(
        [
            "application".into(),
            "lock".into(),
            "riffdb.application.json".into(),
            "--write".into(),
        ],
        Some(&ws.app_dir),
    )?;
    assert!(
        baseline.status.success(),
        "baseline application lock must succeed: {}",
        String::from_utf8_lossy(&baseline.stderr)
    );
    fs::remove_file(ws.app_dir.join("riffdb.application.lock.json"))?;

    // Replace `SearchTaggedAccounts` with a multi-step candidate query whose
    // candidate ceiling (`within 70000`) exceeds the role u16 scan ceiling;
    // `riffdb application lock --write` MUST fail closed before producing a lock.
    let over_ceiling_query = r#"
query SearchTaggedAccounts(
    $org_id: Account.org_id,
    $k1: AccountTag.tag_key,
) {
    candidates matching_ids: Account.account_id from intersect {
        AccountTag.account_id using by_tag_score
            where org_id == $org_id && tag_key == $k1,
        AccountTag.account_id using by_tag_score
            where org_id == $org_id && tag_key == $k1,
    } within 70000 else WindowExceeded

    many accounts from Account
        where org_id == $org_id && account_id in matching_ids
        order by account_id asc
        take 20 else WindowExceeded

    return Found { accounts: accounts { account_id title posted_total } }
    outcomes Found | WindowExceeded
}
"#;
    fs::write(
        ws.app_dir
            .join("riffdb/queries/search_tagged_accounts.riffq"),
        over_ceiling_query,
    )?;

    let exit = run_cli(
        [
            "application".into(),
            "lock".into(),
            "riffdb.application.json".into(),
            "--write".into(),
        ],
        Some(&ws.app_dir),
    )?;
    assert!(
        !exit.status.success(),
        "over-ceiling query/role must fail closed at compile time"
    );
    let stderr = String::from_utf8_lossy(&exit.stderr);
    assert!(
        !stderr.contains("RDB-AS003"),
        "over-ceiling refusal must come from query/role compilation, not manifest syntax: {stderr}"
    );
    assert!(
        !ws.app_dir.join("riffdb.application.lock.json").exists(),
        "failed compile-time role check must not emit a lockfile"
    );
    Ok(())
}

async fn verify_development_query_and_diagnostic_agreement() -> TestResult<()> {
    let ws = TestWorkspace::new("pg-query-role-dev")?;
    let mut server = ws.spawn_server(&ServerOptions {
        commit_profile: CommitProfile::StandardGroup,
        environment: "development",
        authorization_diagnostics: None,
    })?;
    ws.bootstrap_operator()?;
    let app = ws.lock_and_deploy("development")?;
    let mut driver = ws
        .spawn_driver_host(&app.role_credential_path, PRIMARY_ROLE, "driver-auth.sock")
        .await?;

    let (mut grpc_client, grpc_meta) = ws.connect_app_client(&app.role_credential_path).await?;
    let org_id = 7_000_u32;

    // Seed 20 accounts and tag 3 of them to exercise PK Get, PK `in $set` (20 keys),
    // covering index scan, and composable optional EAV `candidates` + joined ORDER BY NULLS LAST.
    let mut last_seq = 0_u64;
    for idx in 1..=20_u32 {
        let cmd = app.build_grpc_command(
            "OpenAccount",
            BTreeMap::from([
                (
                    "req_key".to_owned(),
                    ApplicationValue::String(format!("qa-open-{idx}")),
                ),
                ("org_id".to_owned(), app_uuid(org_id)),
                ("account_id".to_owned(), app_uuid(7_100 + idx)),
                ("credit_limit".to_owned(), ApplicationValue::I64(10_000)),
                (
                    "title".to_owned(),
                    ApplicationValue::String(format!("Acct-{idx:02}")),
                ),
            ]),
        )?;
        let res = execute_grpc_command(&mut grpc_client, &grpc_meta, cmd, 3).await?;
        last_seq = res.commit_sequence.ok_or("missing seq")?;
    }

    // Tag accounts 7101, 7102, 7103 with env=prod; give 7101 score 10, 7103 score 50,
    // and omit rank on 7102 so `order by rank_tags.sort_score desc nulls last` orders 7103, 7101, 7102!
    for (acct_offset, rank_score) in [(1_u32, Some(10_i64)), (2, None), (3, Some(50))] {
        let env_tag = app.build_grpc_command(
            "UpsertAccountTag",
            BTreeMap::from([
                (
                    "req_key".to_owned(),
                    ApplicationValue::String(format!("tag-env-{acct_offset}")),
                ),
                ("org_id".to_owned(), app_uuid(org_id)),
                ("account_id".to_owned(), app_uuid(7_100 + acct_offset)),
                (
                    "tag_key".to_owned(),
                    ApplicationValue::String("env".to_owned()),
                ),
                (
                    "tag_val".to_owned(),
                    ApplicationValue::String("prod".to_owned()),
                ),
                ("sort_score".to_owned(), ApplicationValue::I64(0)),
            ]),
        )?;
        last_seq = execute_grpc_command(&mut grpc_client, &grpc_meta, env_tag, 3)
            .await?
            .commit_sequence
            .ok_or("missing tag seq")?;

        if let Some(score) = rank_score {
            let rank_tag = app.build_grpc_command(
                "UpsertAccountTag",
                BTreeMap::from([
                    (
                        "req_key".to_owned(),
                        ApplicationValue::String(format!("tag-rank-{acct_offset}")),
                    ),
                    ("org_id".to_owned(), app_uuid(org_id)),
                    ("account_id".to_owned(), app_uuid(7_100 + acct_offset)),
                    (
                        "tag_key".to_owned(),
                        ApplicationValue::String("rank".to_owned()),
                    ),
                    (
                        "tag_val".to_owned(),
                        ApplicationValue::String("score".to_owned()),
                    ),
                    ("sort_score".to_owned(), ApplicationValue::I64(score)),
                ]),
            )?;
            last_seq = execute_grpc_command(&mut grpc_client, &grpc_meta, rank_tag, 3)
                .await?
                .commit_sequence
                .ok_or("missing rank seq")?;
        }
    }

    // (A) Prove `PkSetOnlyRole` (which has ONLY `GetAccountsBySet` and NO index scans)
    // derived `max_scan_rows >= 25` rather than `1`, and executes a 20-key PK `in $set`
    // query end-to-end without `RDB-AUTH-0214`!
    let pk_role_cred = ws.bind_role_credential(
        PK_SET_ONLY_ROLE,
        "pg-pk-set-reader",
        "pk-set-only.credential",
    )?;
    let (mut pk_client, pk_meta) = ws.connect_app_client(&pk_role_cred).await?;
    let twenty_ids = (1..=20_u32)
        .map(|idx| app_uuid(7_100 + idx))
        .collect::<Vec<_>>();
    let set_query = app.build_grpc_query(
        "GetAccountsBySet",
        BTreeMap::from([
            ("org_id".to_owned(), app_uuid(org_id)),
            ("ids".to_owned(), ApplicationValue::List(twenty_ids)),
        ]),
        None,
        Some(last_seq),
    )?;
    let set_res = execute_grpc_query(&mut pk_client, &pk_meta, set_query.clone()).await?;
    assert_eq!(set_res.outcome, "Found");
    assert_eq!(
        set_res.fields["accounts"].records.len(),
        20,
        "PkSetOnlyRole must authorize and return all 20 PK-set rows"
    );

    // Execute covering scan and composable optional EAV + joined ORDER BY NULLS LAST.
    let scan_q = app.build_grpc_query(
        "ScanAccountsByTitle",
        BTreeMap::from([("org_id".to_owned(), app_uuid(org_id))]),
        None,
        Some(last_seq),
    )?;
    let scan_res = execute_grpc_query(&mut grpc_client, &grpc_meta, scan_q).await?;
    assert_eq!(scan_res.fields["accounts"].records.len(), 20);

    let eav_q = app.build_grpc_query(
        "SearchTaggedAccounts",
        BTreeMap::from([
            ("org_id".to_owned(), app_uuid(org_id)),
            ("k1".to_owned(), ApplicationValue::String("env".to_owned())),
            ("v1".to_owned(), ApplicationValue::String("prod".to_owned())),
            ("k2".to_owned(), ApplicationValue::Null),
            ("v2".to_owned(), ApplicationValue::Null),
            (
                "order".to_owned(),
                ApplicationValue::Enum("ScoreDesc".to_owned()),
            ),
            ("limit".to_owned(), ApplicationValue::U64(20)),
        ]),
        None,
        Some(last_seq),
    )?;
    let eav_res = execute_grpc_query(&mut grpc_client, &grpc_meta, eav_q).await?;
    let eav_ids = eav_res.fields["accounts"]
        .records
        .iter()
        .map(|rec| match rec.fields.get("account_id") {
            Some(ApplicationValue::Uuid(id)) => id.as_str().to_owned(),
            other => panic!("unexpected account_id value: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        eav_ids,
        vec![uuid_str(7_103), uuid_str(7_101), uuid_str(7_102)],
        "joined ORDER BY sort_score DESC NULLS LAST must rank 50, then 10, then NULL"
    );

    // (B) Issue a custom runtime capability with `ExecuteNamedQuery(GetAccountsBySet)`
    // and field visibility, but `max_scan_rows = 5` (< 25). Calling `GetAccountsBySet`
    // MUST return `RDB-AUTH-0214` with `authorization_diagnostic == Some(ScanBudgetExceeded)`
    // (NOT `missing_permission`), while preserving `symbol_path` strictly for symbols (`AAA-004`).
    let under_budget_cred = create_under_budgeted_query_capability(&ws, &app, 5).await?;
    let (mut narrow_client, narrow_meta) = ws.connect_app_client(&under_budget_cred).await?;
    let budget_err = execute_grpc_query(&mut narrow_client, &narrow_meta, set_query)
        .await
        .expect_err("max_scan_rows=5 capability must reject take-25 PK-set query");
    let budget_sem = budget_err
        .semantic_error()
        .ok_or("scan-budget denial omitted semantic error")?;
    assert_eq!(budget_sem.code(), ApplicationErrorCode::AuthorizationDenied);
    assert_eq!(
        budget_sem.context().authorization_diagnostic(),
        Some(AuthorizationDiagnosticCode::ScanBudgetExceeded)
    );
    assert!(
        !budget_sem
            .context()
            .symbol_path()
            .iter()
            .any(|segment| segment == "scan_budget_exceeded"),
        "AAA-004: symbol_path must never carry diagnostic reason tokens"
    );

    // (C) Calling an ungranted command (`AdminOnlyPing` with `GuaranteeRole`) fails closed
    // with `AuthorizationDenied`, and calling an ungranted named query (`GetAccount` with
    // `PkSetOnlyRole`, which only grants `GetAccountsBySet`) surfaces `missing_permission`
    // on `riffdb.app.v1.ApplicationError.authorization_diagnostic` in development mode.
    let ungranted_grpc = app.build_grpc_command(
        "AdminOnlyPing",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String("ungranted-1".to_owned()),
            ),
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(7_101)),
        ]),
    )?;
    let ungranted_cmd_err = execute_grpc_command(&mut grpc_client, &grpc_meta, ungranted_grpc, 1)
        .await
        .expect_err("ungranted command must fail on gRPC");
    let ungranted_cmd_sem = ungranted_cmd_err
        .semantic_error()
        .ok_or("ungranted gRPC command error omitted semantic details")?;
    assert_eq!(
        ungranted_cmd_sem.code(),
        ApplicationErrorCode::AuthorizationDenied
    );

    let ungranted_query = app.build_grpc_query(
        "GetAccount",
        BTreeMap::from([
            ("org_id".to_owned(), app_uuid(org_id)),
            ("account_id".to_owned(), app_uuid(7_101)),
        ]),
        None,
        Some(last_seq),
    )?;
    let ungranted_err = execute_grpc_query(&mut pk_client, &pk_meta, ungranted_query)
        .await
        .expect_err("ungranted query must fail on gRPC");
    let ungranted_sem = ungranted_err
        .semantic_error()
        .ok_or("ungranted gRPC query error omitted semantic details")?;
    assert_eq!(
        ungranted_sem.code(),
        ApplicationErrorCode::AuthorizationDenied
    );
    assert_eq!(
        ungranted_sem.context().authorization_diagnostic(),
        Some(AuthorizationDiagnosticCode::MissingPermission)
    );
    assert!(
        !ungranted_sem
            .context()
            .symbol_path()
            .iter()
            .any(|segment| segment == "missing_permission")
    );

    //Also verify narrowed capability over `riffdb-driverd`, MCP, and CLI JSON!
    let mut narrow_driver = ws
        .spawn_driver_host(&under_budget_cred, PK_SET_ONLY_ROLE, "driver-narrow.sock")
        .await?;
    let mut narrow_sock = narrow_driver.connect(PK_SET_ONLY_ROLE).await?;
    let driver_budget_err = execute_driver_query(
        &mut narrow_sock,
        &app,
        "GetAccountsBySet",
        BTreeMap::from([
            ("org_id".to_owned(), DriverValue::Uuid(uuid_str(org_id))),
            (
                "ids".to_owned(),
                DriverValue::List(vec![DriverValue::Uuid(uuid_str(7_101))]),
            ),
        ]),
        None,
        Some(last_seq),
    )
    .await
    .expect_err("narrowed capability on riffdb-driverd must fail");
    let driver_details = driver_budget_err
        .details
        .as_ref()
        .ok_or("driver error missing details")?;
    assert_eq!(driver_details["code"].as_str(), Some("RDB-AUTH-0214"));
    assert_eq!(
        driver_details["authorization_diagnostic"].as_str(),
        Some("scan_budget_exceeded")
    );

    let mcp_narrow = McpSession::connect(ws.mcp_address(), &under_budget_cred)?;
    let get_set_mcp_tool = app.mcp_tool_name("GetAccountsBySet")?;
    let mcp_denied = mcp_narrow.call_tool(
        &get_set_mcp_tool,
        serde_json::json!({
            "org_id": uuid_str(org_id),
            "ids": [uuid_str(7_101)]
        }),
    )?;
    assert_eq!(mcp_denied["result"]["isError"].as_bool(), Some(true));
    let mcp_denied_text = mcp_denied["result"]["content"][0]["text"]
        .as_str()
        .ok_or("MCP authorization denial omitted text block")?;
    let mcp_denied_json: serde_json::Value = serde_json::from_str(mcp_denied_text)?;
    assert_eq!(
        mcp_denied_json["code"].as_str(),
        Some("authorization_denied")
    );
    assert_eq!(
        mcp_denied_json["authorization_diagnostic"].as_str(),
        Some("scan_budget_exceeded")
    );

    let narrow_cli_toml = ws.root().join("narrow-client.toml");
    ws.write_client_config(&narrow_cli_toml, Some(&under_budget_cred))?;
    let cli_denied = run_cli(
        [
            "--output".into(),
            "json".into(),
            "--config".into(),
            narrow_cli_toml.into_os_string(),
            "query".into(),
            "run-named".into(),
            "GetAccountsBySet".into(),
            "--params-json".into(),
            serde_json::json!({
                "org_id": uuid_str(org_id),
                "ids": [uuid_str(7_101)]
            })
            .to_string()
            .into(),
        ],
        Some(&ws.app_dir),
    )?;
    assert!(!cli_denied.status.success());
    let cli_denied_out = format!(
        "{}{}",
        String::from_utf8_lossy(&cli_denied.stdout),
        String::from_utf8_lossy(&cli_denied.stderr)
    );
    assert!(
        cli_denied_out.contains("RDB-AUTH-0214"),
        "expected RDB-AUTH-0214 in CLI output, got: {cli_denied_out}"
    );
    assert!(
        cli_denied_out.contains("scan_budget_exceeded"),
        "expected scan_budget_exceeded in CLI output, got: {cli_denied_out}"
    );

    narrow_driver.shutdown().await?;
    driver.shutdown().await?;
    server.shutdown()?;
    Ok(())
}

async fn verify_production_default_redaction_and_override() -> TestResult<()> {
    let ws = TestWorkspace::new("pg-query-role-prod")?;
    let mut server = ws.spawn_server(&ServerOptions {
        commit_profile: CommitProfile::StandardGroup,
        environment: "production",
        authorization_diagnostics: None,
    })?;
    ws.bootstrap_operator()?;
    let app = ws.lock_and_deploy("production")?;

    let prod_pk_cred = ws.bind_role_credential(
        PK_SET_ONLY_ROLE,
        "pg-prod-pk-set-reader",
        "prod-pk-set-only.credential",
    )?;
    let (mut prod_client, prod_meta) = ws.connect_app_client(&prod_pk_cred).await?;
    let denied_query = app.build_grpc_query(
        "GetAccount",
        BTreeMap::from([
            ("org_id".to_owned(), app_uuid(7_900)),
            ("account_id".to_owned(), app_uuid(7_901)),
        ]),
        None,
        None,
    )?;
    let prod_err = execute_grpc_query(&mut prod_client, &prod_meta, denied_query.clone())
        .await
        .expect_err("ungranted query in production must fail");
    let prod_sem = prod_err
        .semantic_error()
        .ok_or("production denial omitted semantic error")?;
    assert_eq!(prod_sem.code(), ApplicationErrorCode::AuthorizationDenied);
    assert_eq!(
        prod_sem.context().authorization_diagnostic(),
        None,
        "production environment must default authorization_diagnostics to None (redacted)"
    );

    // Restart the same production database with explicit `authorization_diagnostics = true`
    // in `riffdbd.toml` and verify the override surfaces `MissingPermission`.
    server.shutdown()?;
    let mut overridden_server = ws.spawn_server(&ServerOptions {
        commit_profile: CommitProfile::StandardGroup,
        environment: "production",
        authorization_diagnostics: Some(true),
    })?;
    let (mut override_client, override_meta) = ws.connect_app_client(&prod_pk_cred).await?;
    let override_err = execute_grpc_query(&mut override_client, &override_meta, denied_query)
        .await
        .expect_err("ungranted query must still fail with override");
    let override_sem = override_err
        .semantic_error()
        .ok_or("override denial omitted semantic error")?;
    assert_eq!(
        override_sem.context().authorization_diagnostic(),
        Some(AuthorizationDiagnosticCode::MissingPermission),
        "explicit authorization_diagnostics = true in production must surface diagnostic"
    );

    overridden_server.shutdown()?;
    Ok(())
}

async fn create_under_budgeted_query_capability(
    ws: &TestWorkspace,
    app: &crate::support::DeployedApp,
    max_scan_rows: u32,
) -> TestResult<std::path::PathBuf> {
    use v1::capability_permission::Permission;

    let exact_bytes = fs::read(ws.app_dir.join("generated/riffdb.application.exact.json"))?;
    let contract_bytes = fs::read(ws.app_dir.join("generated/riffdb.contract.bundle"))?;
    let contract = riffdb_contract_ir::ContractBundle::decode(&contract_bytes)?;
    let module = riffdb_query_module::QueryModule::compile(
        riffdb_query_module::QueryModuleCandidate::new(
            riffdb_query_module::QueryModuleName::new("guarantee_queries")?,
            riffdb_query_module::QueryModuleVersion::new(1).ok_or("invalid module version")?,
            vec![
                riffdb_query_module::NamedQuerySource::new(
                    "GetAccount",
                    crate::support::QUERY_GET_ACCOUNT,
                )?,
                riffdb_query_module::NamedQuerySource::new(
                    "GetAccountsBySet",
                    crate::support::QUERY_GET_ACCOUNTS_BY_SET,
                )?,
                riffdb_query_module::NamedQuerySource::new(
                    "ScanAccountsByTitle",
                    crate::support::QUERY_SCAN_ACCOUNTS_BY_TITLE,
                )?,
                riffdb_query_module::NamedQuerySource::new(
                    "SearchTaggedAccounts",
                    crate::support::QUERY_SEARCH_TAGGED_ACCOUNTS,
                )?,
                riffdb_query_module::NamedQuerySource::new(
                    "PageWideSpansWithHeader",
                    crate::support::QUERY_PAGE_WIDE_SPANS,
                )?,
                riffdb_query_module::NamedQuerySource::new(
                    "PageMediumSpansWithGroupedRollup",
                    crate::support::QUERY_PAGE_MEDIUM_SPANS_WITH_GROUPED_ROLLUP,
                )?,
            ],
        )?,
        &contract,
    )?;
    let exact_manifest =
        riffdb_query_module::ApplicationManifest::parse(std::str::from_utf8(&exact_bytes)?)?;
    let compiled_role = riffdb_query_module::compile_application_role(
        &exact_manifest,
        PK_SET_ONLY_ROLE,
        None,
        &contract,
        std::slice::from_ref(&module),
    )?;
    // Verify the compiler-derived scan budget for `PkSetOnlyRole` is indeed 25!
    assert_eq!(compiled_role.internal_grant().max_scan_rows().get(), 25);

    let (mut op_client, op_meta) = ws.connect_operator_grpc().await?;
    let mut audiences = vec![GRPC_AUDIENCE.to_owned(), ws.mcp_audience()];
    audiences.sort_unstable();

    let account_entity = contract
        .schema()
        .entities()
        .iter()
        .find(|e| e.name() == "Account")
        .ok_or("missing Account entity")?;
    let field_ids = account_entity
        .record()
        .fields()
        .iter()
        .map(|f| f.id().get())
        .collect::<Vec<_>>();

    let cap_id = generate_request_id()?.into_bytes().to_vec();
    let create_req = v1::CreateCapabilityRequest {
        request_id: generate_request_id()?.into_bytes().to_vec(),
        mode: v1::CapabilityCreateMode::Normal as i32,
        capability_id: cap_id,
        principal_id: "pg-under-budgeted-agent".to_owned(),
        actor_kind: v1::ActorKind::Agent as i32,
        requested_lifetime_seconds: 3_600,
        audiences,
        grant: Some(v1::CapabilityGrant {
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            partition_scope: Some(v1::PartitionScope {
                scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
            }),
            permissions: vec![
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadContract(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ExecuteNamedQuery(v1::NamedQueryPermission {
                        contract_lineage: CONTRACT_LINEAGE.to_owned(),
                        query_module_hash: app.query_module_hash.to_vec(),
                        query_name: "GetAccountsBySet".to_owned(),
                    })),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ApplicationRoleIdentity(
                        compiled_role.identity().as_bytes().to_vec(),
                    )),
                },
            ],
            field_visibility: vec![v1::EntityFieldVisibility {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                entity_type_id: account_entity.id().get(),
                field_ids,
                secret_field_ids: Vec::new(),
            }],
            max_scan_rows,
            approval_required: Vec::new(),
            row_policy: None,
            export: None,
            reimport: None,
            vector_inspection: None,
        }),
    };

    let op_bearer = load_protected_bearer_credential(&ws.operator_credential_path)?;
    let call_meta =
        CallMetadata::authenticated(op_bearer).with_database(DatabaseAlias::default_alias());
    let resp = op_client.create_capability(create_req, &call_meta).await?;
    let Some(v1::create_capability_response::Result::Normal(normal)) = resp.result else {
        return Err("expected normal CreateCapabilityResponse".into());
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = normal.result else {
        return Err("expected Created capability transition".into());
    };
    let token = created.token;
    let out_path = crate::support::write_protected(
        ws.root(),
        "under-budgeted.credential",
        token.as_bytes(),
        0o600,
    )?;
    let _ = op_meta;
    Ok(out_path)
}
