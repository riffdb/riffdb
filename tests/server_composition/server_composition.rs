#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Real-process WP-185 composition evidence for gRPC and hosted MCP.

use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::str;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_api_mcp::{MCP_PROTOCOL_VERSION, MCP_ROUTE};
use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_client_rust::generated::legal_spend::{
    AllocateBudget, Amount, CONTRACT_LINEAGE, CONTRACT_VERSION, CreateBudget,
};
use riffdb_client_rust::{
    AttemptBudget, BearerCredential, BootstrapCallMetadata,
    BootstrapCredential as TransportBootstrapCredential, CallMetadata, RiffDbClient,
    generate_request_id,
};
use riffdb_proto::{decimal_from_proto, v1};
use riffdb_types::DecimalSpec;
use tokio::time::timeout;
use tonic::transport::Endpoint;

const GRPC_AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "p2-server-composition-test";
const ALLOWED_ORIGIN: &str = "http://127.0.0.1:43119";
const FORBIDDEN_ORIGIN: &str = "http://untrusted.invalid";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const MAX_READY_LINE_BYTES: usize = 256;
const MAX_HTTP_LINE_BYTES: usize = 16_384;
const MAX_HTTP_HEADERS: usize = 64;
const MAX_HTTP_BODY_BYTES: usize = 4_194_304;
const MAX_CAPTURED_STDERR_BYTES: usize = 65_536;
const PROCESS_START_TIMEOUT: Duration = Duration::from_secs(15);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(40);
const PROCESS_KILL_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_100_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;
const INVALID_CAPABILITY_TOKEN: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh4";
const CREATE_BUDGET_TOOL: &str = "riffdb_cmd_legalspend_createbudget";
const ALLOCATE_BUDGET_TOOL: &str = "riffdb_cmd_legalspend_allocatebudget";
const FISCAL_YEAR: i64 = 2027;
const ORGANIZATION_ID: [u8; 16] = [0x11; 16];
const MATTER_ID: [u8; 16] = [0x22; 16];
const APPROVED_MINOR_UNITS: i128 = 10_000;
const ALLOCATED_MINOR_UNITS: i128 = 2_500;
const PROJECTION_WAIT_NANOS: u64 = 5_000_000_000;

const CAPABILITY_KEY_DOCUMENT: &[u8] = b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] = b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";
const BUDGET_CONTRACT: &str = include_str!("../../contracts/examples/budget.riff");

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_process_hosts_policy_filtered_mcp_and_stops_on_sigterm() -> TestResult<()> {
    let temporary = TemporaryDirectory::new()?;
    let database_path = temporary.path().join("riffdb.redb");
    let capability_keys_path = temporary.path().join("capability.keys");
    let idempotency_keys_path = temporary.path().join("idempotency.keys");
    let bootstrap_path = temporary.path().join("bootstrap.credential");

    write_protected_file(&capability_keys_path, CAPABILITY_KEY_DOCUMENT)?;
    write_protected_file(&idempotency_keys_path, IDEMPOTENCY_KEY_DOCUMENT)?;
    let generated_bootstrap =
        generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)?;
    write_protected_file(
        &bootstrap_path,
        generated_bootstrap.render_document().expose_secret(),
    )?;
    drop(generated_bootstrap);
    let retained_bootstrap = load_bootstrap_credential_file(&bootstrap_path)?;

    let reservation = TcpListener::bind("127.0.0.1:0")?;
    let mcp_address = reservation.local_addr()?;
    if !mcp_address.ip().is_loopback() || mcp_address.port() == 0 {
        return Err(test_failure("MCP test reservation was not loopback"));
    }
    drop(reservation);
    let mcp_audience = format!("http://{mcp_address}{MCP_ROUTE}");

    let mut process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
        mcp_address,
    )?;
    let grpc_address = process.wait_for_ready_address()?;

    // Readiness is emitted only after both listeners exist. Closing stdin now
    // proves EOF disables the test-control watcher without stopping the daemon.
    process.close_stdin()?;

    let initialize_body = initialize_request();
    let missing = mcp_post(mcp_address, None, None, None, initialize_body.as_bytes())
        .map_err(|error| test_failure(format!("missing-auth MCP request failed: {error}")))?;
    assert_eq!(missing.status, 401);
    assert_eq!(missing.single_header("www-authenticate")?, Some("Bearer"));
    assert!(missing.body.is_empty());

    let invalid = mcp_post(
        mcp_address,
        Some(ALLOWED_ORIGIN),
        Some(INVALID_CAPABILITY_TOKEN),
        None,
        initialize_body.as_bytes(),
    )
    .map_err(|error| test_failure(format!("invalid-auth MCP request failed: {error}")))?;
    assert_eq!(invalid.status, 401);
    assert_eq!(
        invalid.single_header("access-control-allow-origin")?,
        Some(ALLOWED_ORIGIN)
    );
    assert_response_omits(&invalid, INVALID_CAPABILITY_TOKEN)?;

    let forbidden = mcp_post(
        mcp_address,
        Some(FORBIDDEN_ORIGIN),
        Some(INVALID_CAPABILITY_TOKEN),
        None,
        initialize_body.as_bytes(),
    )
    .map_err(|error| test_failure(format!("forbidden-origin MCP request failed: {error}")))?;
    assert_eq!(forbidden.status, 403);
    assert_response_omits(&forbidden, INVALID_CAPABILITY_TOKEN)?;

    let mut client = connect(grpc_address).await?;
    let bootstrap = bounded_rpc(
        "bootstrap capability creation",
        client.create_bootstrap_capability(
            bootstrap_request(&retained_bootstrap, &mcp_audience)?,
            &bootstrap_metadata(&retained_bootstrap)?,
        ),
    )
    .await?;
    assert_created_bootstrap(bootstrap)?;

    let bearer = bearer_credential(&retained_bootstrap)?;
    let authenticated = CallMetadata::authenticated(bearer);
    let deployment = bounded_rpc(
        "budget contract deployment",
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
    assert_activated_budget_contract(deployment)?;

    let health = bounded_rpc(
        "authenticated Health after stdin EOF",
        client.health(
            v1::HealthRequest {
                request_id: Some(fresh_request_id_bytes()?),
            },
            &authenticated,
        ),
    )
    .await?;
    assert_derived_health_shape(&health)?;

    let grpc_dynamic_tools = discover_dynamic_tools(&mut client, &authenticated).await?;
    let expected_dynamic_tools = expected_dynamic_tools();
    assert_eq!(grpc_dynamic_tools, expected_dynamic_tools);

    execute_projection_fixture(&mut client, &authenticated).await?;
    let projection = await_projection_ready(&mut client, &authenticated).await?;
    assert_projection_ready(&projection)?;

    let degraded_health = bounded_rpc(
        "derived-degraded Health",
        client.health(
            v1::HealthRequest {
                request_id: Some(fresh_request_id_bytes()?),
            },
            &authenticated,
        ),
    )
    .await?;
    assert_pending_outbox_degrades_only_derived_health(&degraded_health)?;

    let credential = bootstrap_token_text(&retained_bootstrap)?;
    let initialized = mcp_post(
        mcp_address,
        Some(ALLOWED_ORIGIN),
        Some(credential),
        None,
        initialize_body.as_bytes(),
    )
    .map_err(|error| test_failure(format!("authenticated MCP initialize failed: {error}")))?;
    assert_eq!(initialized.status, 200);
    assert_eq!(
        initialized.single_header("access-control-allow-origin")?,
        Some(ALLOWED_ORIGIN)
    );
    let session_id = initialized
        .single_header("mcp-session-id")?
        .ok_or_else(|| test_failure("MCP initialization omitted its session ID"))?
        .to_owned();
    assert_valid_session_id(&session_id)?;
    let initialized_text = initialized.body_text()?;
    assert_eq!(
        json_string_property_values(initialized_text, "protocolVersion")?,
        vec![MCP_PROTOCOL_VERSION.to_owned()]
    );

    let notification = mcp_post(
        mcp_address,
        Some(ALLOWED_ORIGIN),
        Some(credential),
        Some(&session_id),
        br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
    )
    .map_err(|error| test_failure(format!("MCP initialized notification failed: {error}")))?;
    if !(200..300).contains(&notification.status) {
        return Err(test_failure(
            "MCP initialized notification was not accepted",
        ));
    }

    let tools = mcp_post(
        mcp_address,
        Some(ALLOWED_ORIGIN),
        Some(credential),
        Some(&session_id),
        br#"{"id":2,"jsonrpc":"2.0","method":"tools/list"}"#,
    )
    .map_err(|error| test_failure(format!("MCP tools/list failed: {error}")))?;
    assert_eq!(tools.status, 200);
    let tools_text = tools.body_text()?;
    let mut mcp_dynamic_tools: Vec<_> = json_string_property_values(tools_text, "name")?
        .into_iter()
        .filter(|name| name.starts_with("riffdb_cmd_"))
        .collect();
    mcp_dynamic_tools.sort_unstable();
    assert_eq!(
        mcp_dynamic_tools, grpc_dynamic_tools,
        "hosted MCP tools/list response: {tools_text}"
    );
    assert!(
        json_string_property_values(tools_text, "name")?
            .iter()
            .any(|name| name == "riffdb_server_health"),
        "hosted MCP omitted the policy-visible fixed Health tool"
    );

    let projection_call = concat!(
        r#"{"id":3,"jsonrpc":"2.0","method":"tools/call","params":{"#,
        r#""name":"riffdb_projection_query","arguments":{"#,
        r#""contract":{"active":{}},"leading_components":[],"page":{"limit":10},"#,
        r#""projection_id":1,"required_sequence":"2","wait_nanos":"0"}}}"#
    );
    let mcp_projection = mcp_post(
        mcp_address,
        Some(ALLOWED_ORIGIN),
        Some(credential),
        Some(&session_id),
        projection_call.as_bytes(),
    )
    .map_err(|error| test_failure(format!("MCP projection query failed: {error}")))?;
    assert_eq!(mcp_projection.status, 200);
    assert_mcp_projection_ready(mcp_projection.body_text()?)?;

    drop(client);
    process.signal_sigterm()?;
    process.wait_for_successful_exit(PROCESS_STOP_TIMEOUT)?;
    Ok(())
}

async fn execute_projection_fixture(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
) -> TestResult<()> {
    let create = CreateBudget {
        idempotency_key: "p2-create-budget".to_owned(),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        approved_amount: amount(APPROVED_MINOR_UNITS)?,
    };
    let created = bounded_rpc(
        "CreateBudget",
        client.execute_generated(&create, one_attempt(), metadata),
    )
    .await?;
    if created.response().commit_sequence != 1
        || created.response().status
            != v1::execute_command_response::CompletionStatus::Committed as i32
    {
        return Err(test_failure(
            "CreateBudget did not produce commit sequence 1",
        ));
    }

    let allocate = AllocateBudget {
        idempotency_key: "p2-allocate-budget".to_owned(),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        matter_id: MATTER_ID,
        amount: amount(ALLOCATED_MINOR_UNITS)?,
    };
    let allocated = bounded_rpc(
        "AllocateBudget",
        client.execute_generated(&allocate, one_attempt(), metadata),
    )
    .await?;
    if allocated.response().commit_sequence != 2
        || allocated.response().status
            != v1::execute_command_response::CompletionStatus::Committed as i32
    {
        return Err(test_failure(
            "AllocateBudget did not produce commit sequence 2",
        ));
    }
    Ok(())
}

async fn await_projection_ready(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
) -> TestResult<v1::QueryProjectionResponse> {
    const MAX_BUILD_OBSERVATIONS: usize = 256;

    for _ in 0..MAX_BUILD_OBSERVATIONS {
        let response = bounded_rpc(
            "gRPC read-after-sequence projection query",
            client.query_projection(projection_request(PROJECTION_WAIT_NANOS)?, metadata),
        )
        .await?;
        let building = matches!(
            response.result.as_ref(),
            Some(v1::query_projection_response::Result::Degraded(degraded))
                if matches!(
                    degraded.reason.as_ref().and_then(|reason| reason.reason.as_ref()),
                    Some(
                        v1::projection_unavailable_reason::Reason::Building(_)
                            | v1::projection_unavailable_reason::Reason::Rebuilding(_)
                    )
                )
        );
        if !building {
            return Ok(response);
        }
    }
    Err(test_failure(
        "projection remained in a build lifecycle after 256 explicit observations",
    ))
}

fn projection_request(wait_nanos: u64) -> TestResult<v1::QueryProjectionRequest> {
    Ok(v1::QueryProjectionRequest {
        request_id: fresh_request_id_bytes()?,
        contract: Some(active_contract()),
        projection_id: 1,
        leading_components: Vec::new(),
        required_sequence: Some(2),
        wait_nanos,
        page: Some(v1::PageRequest {
            limit: Some(10),
            cursor: None,
        }),
    })
}

fn active_contract() -> v1::ContractSelection {
    v1::ContractSelection {
        selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
    }
}

fn assert_projection_ready(response: &v1::QueryProjectionResponse) -> TestResult<()> {
    let Some(v1::query_projection_response::Result::Ready(ready)) = response.result.as_ref() else {
        let branch = match response.result.as_ref() {
            None => "missing",
            Some(v1::query_projection_response::Result::Ready(_)) => "ready",
            Some(v1::query_projection_response::Result::WaitTimedOut(_)) => "wait_timed_out",
            Some(v1::query_projection_response::Result::Degraded(_)) => "degraded",
            Some(v1::query_projection_response::Result::Invalid(_)) => "invalid",
        };
        return Err(test_failure(format!(
            "projection did not satisfy read-after-sequence through commit 2: {branch}"
        )));
    };
    assert_applied_through(ready.frontier.as_ref(), 2)?;
    let page = ready
        .data
        .as_ref()
        .ok_or_else(|| test_failure("ready projection omitted its page"))?;
    if page.items.len() != 1 || page.next_cursor.is_some() {
        return Err(test_failure(
            "ready projection did not return one exact-end aggregate",
        ));
    }
    let fence = page
        .observed_fence
        .as_ref()
        .ok_or_else(|| test_failure("ready projection omitted its fence"))?;
    if fence.generation != 1 {
        return Err(test_failure(
            "first projection generation was not published",
        ));
    }
    let identity = fence
        .identity
        .as_ref()
        .ok_or_else(|| test_failure("projection fence omitted its identity"))?;
    if identity.contract_lineage != CONTRACT_LINEAGE
        || identity.projection_id != 1
        || identity.projection_plan_hash.len() != 32
    {
        return Err(test_failure("projection fence identity was unexpected"));
    }
    assert_applied_through(fence.frontier.as_ref(), 2)?;

    let row = &page.items[0];
    if row.group.len() != 3
        || !matches!(
            row.group[0].kind.as_ref(),
            Some(v1::value::Kind::UuidValue(value)) if value == &ORGANIZATION_ID
        )
        || !matches!(
            row.group[1].kind.as_ref(),
            Some(v1::value::Kind::I64Value(FISCAL_YEAR))
        )
        || !matches!(
            row.group[2].kind.as_ref(),
            Some(v1::value::Kind::DateValue(_))
        )
    {
        return Err(test_failure(
            "projection returned an unexpected checked group key",
        ));
    }
    let values = row
        .values
        .as_ref()
        .ok_or_else(|| test_failure("projection row omitted its measures"))?;
    let [measure] = values.fields.as_slice() else {
        return Err(test_failure(
            "projection row did not return one allocated measure",
        ));
    };
    let Some(v1::Value {
        kind: Some(v1::value::Kind::DecimalValue(value)),
    }) = measure.value.as_ref()
    else {
        return Err(test_failure("projection allocated measure was not decimal"));
    };
    if measure.field_id != Some(1)
        || decimal_from_proto(value, DecimalSpec::new(28, 2)?)?.coefficient()
            != ALLOCATED_MINOR_UNITS
    {
        return Err(test_failure(
            "projection allocated measure did not equal the durable event",
        ));
    }
    Ok(())
}

fn assert_applied_through(
    frontier: Option<&v1::FrontierPosition>,
    expected: u64,
) -> TestResult<()> {
    if !matches!(
        frontier.and_then(|frontier| frontier.position.as_ref()),
        Some(v1::frontier_position::Position::AppliedThrough(sequence)) if *sequence == expected
    ) {
        return Err(test_failure(
            "projection frontier did not reach the required sequence",
        ));
    }
    Ok(())
}

fn assert_pending_outbox_degrades_only_derived_health(
    response: &v1::HealthResponse,
) -> TestResult<()> {
    let Some(v1::health_response::Result::Authenticated(report)) = response.result.as_ref() else {
        return Err(test_failure(
            "degraded Health did not return an authenticated report",
        ));
    };
    if report.status != v1::HealthStatus::Degraded as i32
        || report.active_contract_version != Some(CONTRACT_VERSION)
        || report.last_commit_sequence != Some(2)
    {
        return Err(test_failure(
            "pending outbox intent weakened authoritative Health evidence",
        ));
    }
    for required in [
        v1::HealthComponentKind::AuthoritativeStorage,
        v1::HealthComponentKind::Catalog,
        v1::HealthComponentKind::CommitCoordinator,
        v1::HealthComponentKind::Projection,
    ] {
        if component_status(report, required)? != v1::HealthComponentStatus::Healthy {
            return Err(test_failure(
                "pending outbox intent degraded a non-outbox component",
            ));
        }
    }
    if component_status(report, v1::HealthComponentKind::Outbox)?
        != v1::HealthComponentStatus::Degraded
    {
        return Err(test_failure(
            "pending no-destination outbox intent was not reported as degraded",
        ));
    }
    Ok(())
}

fn component_status(
    report: &v1::AuthenticatedHealth,
    kind: v1::HealthComponentKind,
) -> TestResult<v1::HealthComponentStatus> {
    let component = report
        .components
        .iter()
        .find(|component| component.component == kind as i32)
        .ok_or_else(|| test_failure("Health omitted a required component"))?;
    v1::HealthComponentStatus::try_from(component.status)
        .map_err(|_| test_failure("Health returned an unknown component status"))
}

fn assert_mcp_projection_ready(body: &str) -> TestResult<()> {
    if body.contains(r#""error":"#) || !body.contains(r#""structuredContent":{"ready":"#) {
        return Err(test_failure(
            "MCP projection query did not return the ready structured branch",
        ));
    }
    let coefficients = json_string_property_values(body, "coefficient")?;
    if coefficients != [ALLOCATED_MINOR_UNITS.to_string()] {
        return Err(test_failure(
            "MCP projection measure diverged from the gRPC projection",
        ));
    }
    let applied = json_string_property_values(body, "applied_through")?;
    if applied.len() < 2 || applied.iter().any(|sequence| sequence != "2") {
        return Err(test_failure(
            "MCP projection did not preserve the read-after-sequence fence",
        ));
    }
    if json_string_property_values(body, "generation")? != ["1"] {
        return Err(test_failure(
            "MCP projection reported an unexpected generation",
        ));
    }
    Ok(())
}

async fn discover_dynamic_tools(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
) -> TestResult<Vec<String>> {
    let response = bounded_rpc(
        "gRPC command-tool discovery",
        client.discover_command_tools(
            v1::DiscoverCommandToolsRequest {
                request_id: fresh_request_id_bytes()?,
                page: Some(v1::PageRequest {
                    limit: Some(100),
                    cursor: None,
                }),
                prior_fence: None,
                representation: v1::DiscoveryRepresentation::Full as i32,
            },
            metadata,
        ),
    )
    .await?;
    let Some(v1::discover_command_tools_response::Result::Page(page)) = response.result else {
        return Err(test_failure(
            "gRPC command-tool discovery did not return a full page",
        ));
    };
    if page.next_cursor.is_some() {
        return Err(test_failure(
            "gRPC command-tool discovery unexpectedly paginated the fixture",
        ));
    }
    let mut tools = Vec::new();
    for item in page.items {
        if let Some(v1::command_tool_discovery_item::Item::CommandTool(tool)) = item.item {
            tools.push(tool.tool_name);
        }
    }
    tools.sort_unstable();
    Ok(tools)
}

fn expected_dynamic_tools() -> Vec<String> {
    let mut tools = vec![
        CREATE_BUDGET_TOOL.to_owned(),
        ALLOCATE_BUDGET_TOOL.to_owned(),
    ];
    tools.sort_unstable();
    tools
}

fn assert_derived_health_shape(response: &v1::HealthResponse) -> TestResult<()> {
    let Some(v1::health_response::Result::Authenticated(report)) = response.result.as_ref() else {
        return Err(test_failure(
            "Health did not return an authenticated report",
        ));
    };
    if report.active_contract_version != Some(CONTRACT_VERSION)
        || report.last_commit_sequence.is_some()
    {
        return Err(test_failure(
            "empty deployed database reported the wrong authoritative fence",
        ));
    }
    let expected_kinds = [
        v1::HealthComponentKind::AuthoritativeStorage,
        v1::HealthComponentKind::Catalog,
        v1::HealthComponentKind::CommitCoordinator,
        v1::HealthComponentKind::Projection,
        v1::HealthComponentKind::Outbox,
    ];
    let actual_kinds: Vec<_> = report
        .components
        .iter()
        .map(|component| v1::HealthComponentKind::try_from(component.component))
        .collect::<Result<_, _>>()
        .map_err(|_| test_failure("Health returned an unknown component kind"))?;
    if actual_kinds != expected_kinds {
        return Err(test_failure(
            "Health omitted or reordered the authoritative/derived component shape",
        ));
    }
    for component in &report.components[..3] {
        if v1::HealthComponentStatus::try_from(component.status)
            != Ok(v1::HealthComponentStatus::Healthy)
        {
            return Err(test_failure("authoritative Health component was not ready"));
        }
    }
    let mut derived_degraded = false;
    for component in &report.components[3..] {
        match v1::HealthComponentStatus::try_from(component.status) {
            Ok(v1::HealthComponentStatus::Healthy) => {}
            Ok(v1::HealthComponentStatus::Degraded) => derived_degraded = true,
            _ => {
                return Err(test_failure(
                    "started derived Health component was unavailable",
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
            "aggregate Health did not preserve authoritative readiness classification",
        ));
    }
    let build = report
        .build
        .as_ref()
        .ok_or_else(|| test_failure("Health omitted build information"))?;
    if build.mcp_protocol_baseline != MCP_PROTOCOL_VERSION {
        return Err(test_failure(
            "Health did not report the hosted MCP protocol baseline",
        ));
    }
    Ok(())
}

fn initialize_request() -> String {
    format!(
        concat!(
            r#"{{"id":1,"jsonrpc":"2.0","method":"initialize","params":{{"#,
            r#""capabilities":{{}},"clientInfo":{{"name":"riffdb-process-test","version":"1"}},"#,
            r#""protocolVersion":"{}"}}}}"#
        ),
        MCP_PROTOCOL_VERSION
    )
}

fn bootstrap_request(
    credential: &RetainedBootstrapCredential,
    mcp_audience: &str,
) -> TestResult<v1::CreateCapabilityRequest> {
    use v1::capability_permission::Permission;

    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: CONTRACT_LINEAGE.to_owned(),
        stable_id,
    };
    let mut audiences = vec![GRPC_AUDIENCE.to_owned(), mcp_audience.to_owned()];
    audiences.sort_unstable();
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "p2-maintainer".to_owned(),
        actor_kind: v1::ActorKind::Human as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
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
                    permission: Some(Permission::DeployContract(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::InvokeCommand(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::InvokeCommand(scoped(2))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::QueryProjection(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadProjectionStatus(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadHealth(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::AdministerCapabilities(v1::Unit {})),
                },
            ],
            field_visibility: Vec::new(),
            max_scan_rows: 100,
            approval_required: Vec::new(),
            row_policy: None,
        }),
    })
}

fn assert_created_bootstrap(response: v1::CreateCapabilityResponse) -> TestResult<()> {
    let Some(v1::create_capability_response::Result::Bootstrap(result)) = response.result else {
        return Err(test_failure(
            "bootstrap response used the wrong result family",
        ));
    };
    let Some(v1::bootstrap_create_capability_result::Result::Created(transition)) = result.result
    else {
        return Err(test_failure("initial bootstrap capability was not created"));
    };
    if transition.identity.is_none() || transition.administration_sequence == 0 {
        return Err(test_failure("bootstrap transition was incomplete"));
    }
    Ok(())
}

fn assert_activated_budget_contract(response: v1::DeployContractResponse) -> TestResult<()> {
    let Some(v1::deploy_contract_response::Result::Activated(contract)) = response.result else {
        return Err(test_failure("budget contract was not activated"));
    };
    if contract.contract_lineage != CONTRACT_LINEAGE
        || contract.contract_version != CONTRACT_VERSION
    {
        return Err(test_failure("activated contract identity was unexpected"));
    }
    Ok(())
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

async fn connect(address: SocketAddr) -> TestResult<RiffDbClient> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    bounded_rpc("gRPC connection", RiffDbClient::connect(endpoint)).await
}

fn bearer_credential(credential: &RetainedBootstrapCredential) -> TestResult<BearerCredential> {
    BearerCredential::new(bootstrap_token_text(credential)?).map_err(Into::into)
}

fn bootstrap_metadata(
    credential: &RetainedBootstrapCredential,
) -> TestResult<BootstrapCallMetadata> {
    let transport = TransportBootstrapCredential::new(bootstrap_token_text(credential)?)?;
    Ok(BootstrapCallMetadata::new(transport))
}

fn bootstrap_token_text(credential: &RetainedBootstrapCredential) -> TestResult<&str> {
    str::from_utf8(credential.token().expose_secret()).map_err(Into::into)
}

fn fresh_request_id_bytes() -> TestResult<Vec<u8>> {
    Ok(generate_request_id()?.into_bytes().to_vec())
}

fn amount(minor_units: i128) -> TestResult<Amount> {
    Amount::from_minor_units(minor_units)
        .ok_or_else(|| test_failure("projection fixture amount exceeded its generated type"))
}

fn one_attempt() -> AttemptBudget {
    AttemptBudget::new(1).expect("one is a nonzero submission bound")
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
        static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

        for _ in 0..1_024 {
            let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "riffdb-p2-server-composition-{}-{ordinal}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique P2 test directory",
        ))
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

struct ServerProcess {
    pid: u32,
    stdin: Option<ChildStdin>,
    ready: Receiver<io::Result<String>>,
    exited: Receiver<io::Result<ExitStatus>>,
    waiter: Option<JoinHandle<()>>,
    stdout: Option<JoinHandle<usize>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
    exit_observed: bool,
}

impl ServerProcess {
    fn spawn(
        database_path: &Path,
        capability_keys_path: &Path,
        idempotency_keys_path: &Path,
        mcp_address: SocketAddr,
    ) -> io::Result<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_riffdbd"))
            .arg("--database")
            .arg(database_path)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--environment")
            .arg(ENVIRONMENT)
            .arg("--audience")
            .arg(GRPC_AUDIENCE)
            .arg("--mcp-listen")
            .arg(mcp_address.to_string())
            .arg("--mcp-origin")
            .arg(ALLOWED_ORIGIN)
            .arg("--backup-root")
            .arg(
                database_path
                    .parent()
                    .expect("test database has parent")
                    .join("backups"),
            )
            .arg("--capability-keys")
            .arg(capability_keys_path)
            .arg("--idempotency-keys")
            .arg(idempotency_keys_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let pid = child.id();
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
        let stderr = thread::spawn(move || capture_stderr(stderr));
        let (exit_sender, exited) = mpsc::sync_channel(1);
        let waiter = thread::spawn(move || {
            let _ = exit_sender.send(child.wait());
        });
        Ok(Self {
            pid,
            stdin: Some(stdin),
            ready,
            exited,
            waiter: Some(waiter),
            stdout: Some(stdout),
            stderr: Some(stderr),
            exit_observed: false,
        })
    }

    fn wait_for_ready_address(&mut self) -> TestResult<SocketAddr> {
        let line = match self.ready.recv_timeout(PROCESS_START_TIMEOUT) {
            Ok(Ok(line)) => line,
            Ok(Err(source)) => return self.report_startup_exit(source),
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

    fn report_startup_exit(&mut self, source: io::Error) -> TestResult<SocketAddr> {
        let status = match self.exited.recv_timeout(PROCESS_KILL_TIMEOUT) {
            Ok(result) => {
                self.exit_observed = true;
                result?
            }
            Err(_) => {
                return Err(test_failure(format!(
                    "riffdbd closed readiness output before startup: {source}"
                )));
            }
        };
        let (_, stderr) = self.join_threads()?;
        Err(test_failure(format!(
            "riffdbd exited before readiness with {status}: {}",
            String::from_utf8_lossy(&stderr)
        )))
    }

    fn close_stdin(&mut self) -> TestResult<()> {
        self.stdin
            .take()
            .ok_or_else(|| test_failure("riffdbd stdin was already closed"))?;
        Ok(())
    }

    fn signal_sigterm(&self) -> TestResult<()> {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(self.pid.to_string())
            .status()?;
        if !status.success() {
            return Err(test_failure("could not deliver SIGTERM to riffdbd"));
        }
        Ok(())
    }

    fn wait_for_successful_exit(&mut self, deadline: Duration) -> TestResult<()> {
        match self.exited.recv_timeout(deadline) {
            Ok(result) => {
                self.exit_observed = true;
                let status = result?;
                let (stdout_bytes, stderr) = self.join_threads()?;
                if !status.success() {
                    return Err(test_failure(format!(
                        "riffdbd exited with {status}; drained {stdout_bytes} stdout bytes; stderr: {}",
                        String::from_utf8_lossy(&stderr)
                    )));
                }
                Ok(())
            }
            Err(RecvTimeoutError::Timeout) => {
                let _ = signal_process(self.pid, "-KILL");
                if self.exited.recv_timeout(PROCESS_KILL_TIMEOUT).is_ok() {
                    self.exit_observed = true;
                    let _ = self.join_threads();
                }
                Err(test_failure("riffdbd graceful SIGTERM shutdown timed out"))
            }
            Err(RecvTimeoutError::Disconnected) => {
                Err(test_failure("riffdbd process waiter disconnected"))
            }
        }
    }

    fn join_threads(&mut self) -> TestResult<(usize, Vec<u8>)> {
        if let Some(waiter) = self.waiter.take() {
            waiter
                .join()
                .map_err(|_| test_failure("riffdbd process waiter panicked"))?;
        }
        let stdout_bytes = self
            .stdout
            .take()
            .ok_or_else(|| test_failure("riffdbd stdout reader was already joined"))?
            .join()
            .map_err(|_| test_failure("riffdbd stdout reader panicked"))?;
        let stderr = self
            .stderr
            .take()
            .ok_or_else(|| test_failure("riffdbd stderr reader was already joined"))?
            .join()
            .map_err(|_| test_failure("riffdbd stderr reader panicked"))?;
        Ok((stdout_bytes, stderr))
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stdin.take();
        if !self.exit_observed {
            let _ = signal_process(self.pid, "-KILL");
            if self.exited.recv_timeout(PROCESS_KILL_TIMEOUT).is_ok() {
                self.exit_observed = true;
                let _ = self.join_threads();
            }
        }
    }
}

fn signal_process(pid: u32, signal: &str) -> io::Result<()> {
    let status = Command::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other("process signal command failed"))
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
            0 if bytes.is_empty() => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "bounded line missing",
                ));
            }
            0 => break,
            1 if byte[0] == b'\n' => break,
            1 if bytes.len() < maximum => bytes.push(byte[0]),
            1 => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bounded line exceeded its limit",
                ));
            }
            _ => unreachable!("one-byte read returned more than one byte"),
        }
    }
    if bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bounded line was not UTF-8"))
}

fn drain_reader(reader: &mut impl Read) -> usize {
    let mut total = 0_usize;
    let mut buffer = [0_u8; 4_096];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => return total,
            Ok(read) => total = total.saturating_add(read),
        }
    }
}

fn capture_stderr(stderr: ChildStderr) -> Vec<u8> {
    let mut reader = BufReader::new(stderr);
    let mut captured = Vec::new();
    let mut buffer = [0_u8; 4_096];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => return captured,
            Ok(read) => {
                let remaining = MAX_CAPTURED_STDERR_BYTES.saturating_sub(captured.len());
                captured.extend_from_slice(&buffer[..read.min(remaining)]);
            }
        }
    }
}

struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpResponse {
    fn single_header(&self, name: &str) -> TestResult<Option<&str>> {
        let values: Vec<_> = self
            .headers
            .iter()
            .filter_map(|(header, value)| (header == name).then_some(value.as_str()))
            .collect();
        match values.as_slice() {
            [] => Ok(None),
            [value] => Ok(Some(*value)),
            _ => Err(test_failure("HTTP response repeated a singleton header")),
        }
    }

    fn body_text(&self) -> TestResult<&str> {
        str::from_utf8(&self.body).map_err(|_| test_failure("MCP response body was not UTF-8"))
    }
}

fn mcp_post(
    address: SocketAddr,
    origin: Option<&str>,
    credential: Option<&str>,
    session_id: Option<&str>,
    body: &[u8],
) -> TestResult<HttpResponse> {
    if body.len() > MAX_HTTP_BODY_BYTES {
        return Err(test_failure("test MCP request exceeded its bound"));
    }
    for value in [origin, credential, session_id].into_iter().flatten() {
        if value.is_empty() || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
            return Err(test_failure("test MCP header value was invalid"));
        }
    }
    let mut stream = TcpStream::connect_timeout(&address, HTTP_TIMEOUT)?;
    stream.set_read_timeout(Some(HTTP_TIMEOUT))?;
    stream.set_write_timeout(Some(HTTP_TIMEOUT))?;
    write!(
        stream,
        "POST {MCP_ROUTE} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nMcp-Protocol-Version: {MCP_PROTOCOL_VERSION}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    )?;
    if let Some(origin) = origin {
        write!(stream, "Origin: {origin}\r\n")?;
    }
    if let Some(credential) = credential {
        write!(stream, "Authorization: Bearer {credential}\r\n")?;
    }
    if let Some(session_id) = session_id {
        write!(stream, "Mcp-Session-Id: {session_id}\r\n")?;
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(body)?;
    stream.flush()?;
    read_http_response(BufReader::new(stream))
}

fn read_http_response(mut reader: BufReader<TcpStream>) -> TestResult<HttpResponse> {
    let status_line = read_bounded_line(&mut reader, MAX_HTTP_LINE_BYTES)?;
    let mut status_parts = status_line.split_whitespace();
    if status_parts.next() != Some("HTTP/1.1") {
        return Err(test_failure("MCP response was not HTTP/1.1"));
    }
    let status = status_parts
        .next()
        .ok_or_else(|| test_failure("MCP response omitted its status"))?
        .parse::<u16>()?;

    let mut headers = Vec::new();
    for _ in 0..MAX_HTTP_HEADERS {
        let line = read_bounded_line(&mut reader, MAX_HTTP_LINE_BYTES)?;
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| test_failure("MCP response contained an invalid header"))?;
        if name.is_empty() || name.bytes().any(|byte| !byte.is_ascii_graphic()) {
            return Err(test_failure(
                "MCP response contained an invalid header name",
            ));
        }
        headers.push((name.to_ascii_lowercase(), value.trim().to_owned()));
    }
    if headers.len() == MAX_HTTP_HEADERS {
        return Err(test_failure("MCP response exceeded its header-count bound"));
    }

    let content_lengths: Vec<_> = headers
        .iter()
        .filter_map(|(name, value)| (name == "content-length").then_some(value.as_str()))
        .collect();
    let transfer_encodings: Vec<_> = headers
        .iter()
        .filter_map(|(name, value)| (name == "transfer-encoding").then_some(value.as_str()))
        .collect();
    let body = match (content_lengths.as_slice(), transfer_encodings.as_slice()) {
        ([length], []) => {
            let length = length.parse::<usize>()?;
            if length > MAX_HTTP_BODY_BYTES {
                return Err(test_failure("MCP response exceeded its body bound"));
            }
            let mut body = vec![0_u8; length];
            reader.read_exact(&mut body)?;
            body
        }
        ([], [encoding]) if encoding.eq_ignore_ascii_case("chunked") => {
            read_chunked_body(&mut reader)?
        }
        ([], []) => {
            let mut body = Vec::new();
            reader
                .take((MAX_HTTP_BODY_BYTES + 1) as u64)
                .read_to_end(&mut body)?;
            if body.len() > MAX_HTTP_BODY_BYTES {
                return Err(test_failure("MCP response exceeded its body bound"));
            }
            body
        }
        _ => return Err(test_failure("MCP response used ambiguous body framing")),
    };
    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

fn read_chunked_body(reader: &mut impl Read) -> TestResult<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let size_line = read_bounded_line(reader, 128)?;
        let size_text = size_line
            .split_once(';')
            .map_or(size_line.as_str(), |(size, _)| size);
        let size = usize::from_str_radix(size_text, 16)?;
        if size == 0 {
            for _ in 0..MAX_HTTP_HEADERS {
                if read_bounded_line(reader, MAX_HTTP_LINE_BYTES)?.is_empty() {
                    return Ok(body);
                }
            }
            return Err(test_failure("MCP response exceeded its trailer bound"));
        }
        if body.len().saturating_add(size) > MAX_HTTP_BODY_BYTES {
            return Err(test_failure("MCP response exceeded its body bound"));
        }
        let start = body.len();
        body.resize(start + size, 0);
        reader.read_exact(&mut body[start..])?;
        let mut delimiter = [0_u8; 2];
        reader.read_exact(&mut delimiter)?;
        if delimiter != *b"\r\n" {
            return Err(test_failure("MCP response used invalid chunk framing"));
        }
    }
}

fn assert_response_omits(response: &HttpResponse, secret: &str) -> TestResult<()> {
    if response.body_text()?.contains(secret) {
        return Err(test_failure(
            "MCP authentication rejection reflected credential material",
        ));
    }
    Ok(())
}

fn assert_valid_session_id(session_id: &str) -> TestResult<()> {
    if session_id.len() > 256
        || session_id.contains(',')
        || !session_id.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(test_failure("MCP returned an invalid session ID"));
    }
    Ok(())
}

fn json_string_property_values(document: &str, property: &str) -> TestResult<Vec<String>> {
    let needle = format!("\"{property}\":\"");
    let bytes = document.as_bytes();
    let mut offset = 0;
    let mut values = Vec::new();
    while let Some(relative) = find_bytes(&bytes[offset..], needle.as_bytes()) {
        let start = offset + relative + needle.len();
        let mut end = start;
        while end < bytes.len() && bytes[end] != b'"' {
            if bytes[end] == b'\\' || bytes[end] < 0x20 {
                return Err(test_failure(
                    "MCP response escaped a closed ASCII identifier",
                ));
            }
            end += 1;
        }
        if end == bytes.len() {
            return Err(test_failure(
                "MCP response contained an unterminated string property",
            ));
        }
        values.push(str::from_utf8(&bytes[start..end])?.to_owned());
        offset = end + 1;
    }
    Ok(values)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
