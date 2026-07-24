#![forbid(unsafe_code)]

//! External protocol, authorization, redaction, and transport-parity conformance.

#[allow(dead_code)]
#[path = "../../crates/riffdb-api-mcp/examples/conformance_support/auth.rs"]
mod auth;
#[allow(dead_code)]
#[path = "../../crates/riffdb-api-mcp/examples/conformance_support/backend.rs"]
mod backend;
#[allow(dead_code)]
#[path = "../../crates/riffdb-api-mcp/examples/conformance_support/hosted.rs"]
mod hosted;

use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

use backend::{
    ACTIVE_CONTRACT_URI, CAPABILITY_TOKEN, COMMAND_PLAN_URI, ConformanceBackend, INTERNAL_CANARY,
    PROJECTION_STATUS_URI, STABLE_TOOL, STALE_TOOL, UNKNOWN_TOOL,
};
use bytes::Bytes;
use hosted::{HostedConformanceConnection, hosted_registration};
use http::{
    HeaderMap, HeaderValue, Method, Request, Response, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HOST, ORIGIN},
};
use http_body_util::{BodyExt, Full};
use riffdb_api_mcp::{MCP_PROTOCOL_VERSION, MCP_ROUTE};
use serde_json::{Value, json};
use tower_service::Service;

const IO_TIMEOUT: Duration = Duration::from_secs(10);
const TEST_AUTHORITY: &str = "127.0.0.1:17443";
const WRONG_CAPABILITY_TOKEN: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh4";

#[derive(Debug)]
struct Transcript {
    initialize: Value,
    tools: Value,
    resources: Value,
    templates: Value,
    stable_call: Value,
    stale_call: Value,
    unknown_call: Value,
    invalid_call: Value,
    active_contract: Value,
    command_plan: Value,
    projection_status: Value,
}

struct StdioClient {
    child: Child,
    input: Option<ChildStdin>,
    output: Option<Receiver<String>>,
    reader: Option<thread::JoinHandle<()>>,
}

impl StdioClient {
    fn spawn() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_riffdb-mcp-conformance-stdio"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start bounded stdio conformance server");
        let input = child.stdin.take().expect("child stdin");
        let output = child.stdout.take().expect("child stdout");
        let (sender, receiver) = mpsc::sync_channel(32);
        let reader = thread::spawn(move || {
            let mut lines = BufReader::new(output).lines();
            while let Some(Ok(line)) = lines.next() {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            input: Some(input),
            output: Some(receiver),
            reader: Some(reader),
        }
    }

    fn notify(&mut self, method: &str, params: Option<Value>) {
        let mut message = json!({"jsonrpc": "2.0", "method": method});
        if let Some(params) = params {
            message["params"] = params;
        }
        self.write(&message);
    }

    fn request(&mut self, id: u64, method: &str, params: Option<Value>) -> Value {
        let mut message = json!({"id": id, "jsonrpc": "2.0", "method": method});
        if let Some(params) = params {
            message["params"] = params;
        }
        self.write(&message);
        loop {
            let line = self
                .output
                .as_ref()
                .expect("open child stdout")
                .recv_timeout(IO_TIMEOUT)
                .expect("bounded stdio response");
            let response: Value =
                serde_json::from_str(&line).expect("stdout contains only JSON-RPC messages");
            if response.get("id") == Some(&json!(id)) {
                return response;
            }
        }
    }

    fn write(&mut self, value: &Value) {
        let input = self.input.as_mut().expect("open child stdin");
        serde_json::to_writer(&mut *input, value).expect("serialize JSON-RPC request");
        input.write_all(b"\n").expect("frame JSON-RPC request");
        input.flush().expect("flush JSON-RPC request");
    }
}

impl Drop for StdioClient {
    fn drop(&mut self) {
        self.input.take();
        let _ = self.child.kill();
        self.child.wait().expect("reap stdio fixture");
        self.output.take();
        if let Some(reader) = self.reader.take() {
            reader.join().expect("join stdio fixture reader");
        }
    }
}

struct HostedClient {
    connection: HostedConformanceConnection,
    session_id: Option<HeaderValue>,
}

struct HostedResponse {
    status: StatusCode,
    headers: HeaderMap,
    wire: String,
    message: Option<Value>,
}

impl HostedClient {
    fn new(connection: HostedConformanceConnection) -> Self {
        Self {
            connection,
            session_id: None,
        }
    }

    async fn initialize(&mut self, protocol_version: &str) -> HostedResponse {
        let response = self
            .post_with_protocol(
                Some(CAPABILITY_TOKEN),
                json!({
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "initialize",
                    "params": {
                        "capabilities": {},
                        "clientInfo": {
                            "name": "riffdb-conformance",
                            "version": "1"
                        },
                        "protocolVersion": protocol_version
                    }
                }),
                false,
                None,
                protocol_version,
            )
            .await;
        if response.status == StatusCode::OK {
            self.session_id = response.headers.get("mcp-session-id").cloned();
        }
        response
    }

    async fn notify_initialized(&mut self) {
        let response = self
            .post(
                Some(CAPABILITY_TOKEN),
                json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized"
                }),
                true,
                None,
            )
            .await;
        assert!(
            response.status.is_success(),
            "initialized notification failed: {response:?}"
        );
    }

    async fn request(&mut self, id: u64, method: &str, params: Option<Value>) -> Value {
        let mut message = json!({"id": id, "jsonrpc": "2.0", "method": method});
        if let Some(params) = params {
            message["params"] = params;
        }
        let response = self.post(Some(CAPABILITY_TOKEN), message, true, None).await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "hosted request failed: {response:?}"
        );
        response.message.expect("hosted JSON-RPC response")
    }

    async fn open_event_stream(
        &mut self,
        credential: &str,
    ) -> Response<riffdb_api_mcp::HostedMcpHttpResponseBody> {
        let request = Request::builder()
            .method(Method::GET)
            .uri(MCP_ROUTE)
            .header(HOST, TEST_AUTHORITY)
            .header(ACCEPT, "text/event-stream")
            .header(AUTHORIZATION, format!("Bearer {credential}"))
            .header("mcp-protocol-version", MCP_PROTOCOL_VERSION)
            .header(
                "mcp-session-id",
                self.session_id
                    .as_ref()
                    .expect("active hosted session")
                    .clone(),
            )
            .body(Full::new(Bytes::new()))
            .expect("hosted GET request");
        self.connection
            .call(request)
            .await
            .expect("infallible hosted wrapper")
    }

    async fn post(
        &mut self,
        credential: Option<&str>,
        message: Value,
        active_session: bool,
        origin: Option<&str>,
    ) -> HostedResponse {
        self.post_with_protocol(
            credential,
            message,
            active_session,
            origin,
            MCP_PROTOCOL_VERSION,
        )
        .await
    }

    async fn post_with_protocol(
        &mut self,
        credential: Option<&str>,
        message: Value,
        active_session: bool,
        origin: Option<&str>,
        protocol_version: &str,
    ) -> HostedResponse {
        let body = serde_json::to_vec(&message).expect("serialize hosted JSON-RPC request");
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(MCP_ROUTE)
            .header(HOST, TEST_AUTHORITY)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream")
            .header("mcp-protocol-version", protocol_version);
        if let Some(credential) = credential {
            builder = builder.header(AUTHORIZATION, format!("Bearer {credential}"));
        }
        if let Some(origin) = origin {
            builder = builder.header(ORIGIN, origin);
        }
        if active_session {
            builder = builder.header(
                "mcp-session-id",
                self.session_id
                    .as_ref()
                    .expect("active hosted session")
                    .clone(),
            );
        }
        let request = builder
            .body(Full::new(Bytes::from(body)))
            .expect("hosted HTTP request");
        let response = self
            .connection
            .call(request)
            .await
            .expect("infallible hosted wrapper");
        let (parts, body) = response.into_parts();
        let bytes = tokio::time::timeout(IO_TIMEOUT, body.collect())
            .await
            .expect("bounded hosted response")
            .expect("read hosted response")
            .to_bytes();
        let wire = String::from_utf8(bytes.to_vec()).expect("hosted response is UTF-8");
        let message = parse_hosted_message(&wire);
        HostedResponse {
            status: parts.status,
            headers: parts.headers,
            wire,
            message,
        }
    }
}

impl std::fmt::Debug for HostedResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostedResponse")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .field("wire", &self.wire)
            .finish()
    }
}

fn parse_hosted_message(wire: &str) -> Option<Value> {
    if wire.trim().is_empty() {
        return None;
    }
    if let Ok(value) = serde_json::from_str(wire) {
        return Some(value);
    }
    wire.lines()
        .find_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str(data).expect("valid JSON-RPC SSE data"))
}

fn stdio_transcript() -> Transcript {
    let mut client = StdioClient::spawn();
    let initialize = client.request(
        1,
        "initialize",
        Some(json!({
            "capabilities": {},
            "clientInfo": {"name": "riffdb-conformance", "version": "1"},
            "protocolVersion": MCP_PROTOCOL_VERSION
        })),
    );
    client.notify("notifications/initialized", None);
    let tools = client.request(2, "tools/list", None);
    let resources = client.request(3, "resources/list", None);
    let templates = client.request(4, "resources/templates/list", None);
    let stable_call = client.request(5, "tools/call", Some(stable_call_arguments()));
    let stale_call = client.request(
        6,
        "tools/call",
        Some(json!({"arguments": valid_command_input(), "name": STALE_TOOL})),
    );
    let unknown_call = client.request(
        7,
        "tools/call",
        Some(json!({"arguments": valid_command_input(), "name": UNKNOWN_TOOL})),
    );
    let invalid_call = client.request(
        8,
        "tools/call",
        Some(json!({
            "arguments": {
                "allocation": {
                    "account_name": "Operations",
                    "priority": "High",
                    "secret": INTERNAL_CANARY
                },
                "idempotency_key": "conformance-1"
            },
            "name": STABLE_TOOL
        })),
    );
    let active_contract = client.request(
        9,
        "resources/read",
        Some(json!({"uri": ACTIVE_CONTRACT_URI})),
    );
    let command_plan = client.request(10, "resources/read", Some(json!({"uri": COMMAND_PLAN_URI})));
    let projection_status = client.request(
        11,
        "resources/read",
        Some(json!({"uri": PROJECTION_STATUS_URI})),
    );
    Transcript {
        initialize,
        tools,
        resources,
        templates,
        stable_call,
        stale_call,
        unknown_call,
        invalid_call,
        active_contract,
        command_plan,
        projection_status,
    }
}

async fn hosted_transcript(client: &mut HostedClient) -> Transcript {
    let initialize = client
        .initialize(MCP_PROTOCOL_VERSION)
        .await
        .message
        .expect("hosted initialize response");
    client.notify_initialized().await;
    Transcript {
        tools: client.request(2, "tools/list", None).await,
        resources: client.request(3, "resources/list", None).await,
        templates: client.request(4, "resources/templates/list", None).await,
        stable_call: client
            .request(5, "tools/call", Some(stable_call_arguments()))
            .await,
        stale_call: client
            .request(
                6,
                "tools/call",
                Some(json!({"arguments": valid_command_input(), "name": STALE_TOOL})),
            )
            .await,
        unknown_call: client
            .request(
                7,
                "tools/call",
                Some(json!({"arguments": valid_command_input(), "name": UNKNOWN_TOOL})),
            )
            .await,
        invalid_call: client
            .request(
                8,
                "tools/call",
                Some(json!({
                    "arguments": {
                        "allocation": {
                            "account_name": "Operations",
                            "priority": "High",
                            "secret": INTERNAL_CANARY
                        },
                        "idempotency_key": "conformance-1"
                    },
                    "name": STABLE_TOOL
                })),
            )
            .await,
        active_contract: client
            .request(
                9,
                "resources/read",
                Some(json!({"uri": ACTIVE_CONTRACT_URI})),
            )
            .await,
        command_plan: client
            .request(10, "resources/read", Some(json!({"uri": COMMAND_PLAN_URI})))
            .await,
        projection_status: client
            .request(
                11,
                "resources/read",
                Some(json!({"uri": PROJECTION_STATUS_URI})),
            )
            .await,
        initialize,
    }
}

fn valid_command_input() -> Value {
    json!({
        "allocation": {
            "account_name": "Operations",
            "priority": "High"
        },
        "idempotency_key": "conformance-1"
    })
}

fn stable_call_arguments() -> Value {
    json!({"arguments": valid_command_input(), "name": STABLE_TOOL})
}

fn result(response: &Value) -> &Value {
    response
        .get("result")
        .unwrap_or_else(|| panic!("successful JSON-RPC result: {response}"))
}

fn error(response: &Value) -> &Value {
    response.get("error").expect("JSON-RPC error")
}

fn assert_transcript_semantics(transcript: &Transcript) {
    assert_eq!(
        result(&transcript.initialize)["protocolVersion"],
        MCP_PROTOCOL_VERSION
    );
    let tools = result(&transcript.tools)["tools"]
        .as_array()
        .expect("tool array");
    for tool in tools {
        let name = tool["name"].as_str().expect("tool name");
        assert_eq!(
            tool["inputSchema"]["type"], "object",
            "{name} input schema must have the MCP-required object root"
        );
        assert_eq!(
            tool["outputSchema"]["type"], "object",
            "{name} output schema must have the MCP-required object root"
        );
    }
    let stable = tools
        .iter()
        .find(|tool| tool["name"] == STABLE_TOOL)
        .expect("stable generated command tool");
    assert_eq!(
        stable["inputSchema"]["properties"]["allocation"]["properties"]["priority"]["enum"],
        json!(["High", "Normal"]),
        "generated input uses exact name-only enum variants"
    );
    assert!(
        stable["inputSchema"]["properties"]["allocation"]["properties"]["account_name"].is_object(),
        "generated input retains the service-owned named record field"
    );
    assert!(
        stable["outputSchema"]["oneOf"]
            .as_array()
            .is_some_and(|branches| !branches.is_empty()),
        "generated result schema mechanically includes the declared outcome"
    );
    assert!(tools.iter().any(|tool| tool["name"] == STALE_TOOL));
    assert!(
        tools
            .iter()
            .any(|tool| tool["name"] == "riffdb.server.health")
    );

    let resources = result(&transcript.resources)["resources"]
        .as_array()
        .expect("resource array");
    for expected in [ACTIVE_CONTRACT_URI, COMMAND_PLAN_URI, PROJECTION_STATUS_URI] {
        assert!(resources.iter().any(|resource| resource["uri"] == expected));
    }
    let templates = result(&transcript.templates)["resourceTemplates"]
        .as_array()
        .expect("resource template array");
    assert!(templates.iter().any(|template| {
        template["uriTemplate"]
            == concat!(
                "riffdb://outcome/{principal}/LegalSpend/2/",
                "riffdb.cmd.legalspend.allocatebudget/{key_hash}"
            )
    }));

    let structured = &result(&transcript.stable_call)["structuredContent"];
    assert_eq!(structured["outcome"]["type"], "Allocated");
    assert_eq!(
        structured["outcome"]["allocation"]["account_name"],
        "Operations"
    );
    assert_eq!(structured["outcome"]["allocation"]["priority"], "High");

    assert_eq!(
        error(&transcript.stale_call),
        error(&transcript.unknown_call),
        "stale and unknown compiled command names are existence-blind"
    );
    let invalid_wire = serde_json::to_string(&transcript.invalid_call).expect("serialize error");
    assert!(!invalid_wire.contains(INTERNAL_CANARY));
    assert!(!invalid_wire.contains(CAPABILITY_TOKEN));

    let active = resource_json(&transcript.active_contract);
    assert_eq!(active["compatibility"]["overall"], "compatible");
    let plan = resource_json(&transcript.command_plan);
    assert_eq!(plan["source_command"], "AllocateBudget");
    assert!(plan["input_schema"]["properties"]["allocation"].is_object());
    assert!(plan["outcome_schema"]["oneOf"].is_array());
    let projection = resource_json(&transcript.projection_status);
    assert_eq!(projection["lag"], "5");
}

fn resource_json(response: &Value) -> Value {
    let text = result(response)["contents"][0]["text"]
        .as_str()
        .expect("JSON resource text");
    serde_json::from_str(text).expect("resource JSON")
}

#[test]
fn stdio_uses_exact_protocol_and_policy_filtered_common_handler() {
    let transcript = stdio_transcript();
    assert_transcript_semantics(&transcript);
}

#[tokio::test(flavor = "current_thread")]
async fn hosted_http_matches_stdio_and_rejects_authentication_bypasses() {
    let stdio = stdio_transcript();
    let backend = ConformanceBackend::default();
    let (registration, authenticator) = hosted_registration(
        TEST_AUTHORITY.parse().expect("test address"),
        backend.clone(),
    );
    let connection = registration
        .service_for_peer("127.0.0.1:40123".parse().expect("loopback peer"))
        .expect("hosted loopback connection");

    let mut unauthenticated = HostedClient::new(connection.clone());
    let missing = unauthenticated
        .post(
            None,
            json!({"id": 1, "jsonrpc": "2.0", "method": "initialize", "params": {
                "capabilities": {},
                "clientInfo": {"name": "missing", "version": "1"},
                "protocolVersion": MCP_PROTOCOL_VERSION
            }}),
            false,
            None,
        )
        .await;
    assert_eq!(missing.status, StatusCode::UNAUTHORIZED);
    let malformed = unauthenticated
        .post(
            Some(INTERNAL_CANARY),
            json!({"id": 1, "jsonrpc": "2.0", "method": "initialize", "params": {
                "capabilities": {},
                "clientInfo": {"name": "malformed", "version": "1"},
                "protocolVersion": MCP_PROTOCOL_VERSION
            }}),
            false,
            None,
        )
        .await;
    assert_eq!(malformed.status, StatusCode::UNAUTHORIZED);
    let wrong = unauthenticated
        .post(
            Some(WRONG_CAPABILITY_TOKEN),
            json!({"id": 1, "jsonrpc": "2.0", "method": "initialize", "params": {
                "capabilities": {},
                "clientInfo": {"name": "wrong", "version": "1"},
                "protocolVersion": MCP_PROTOCOL_VERSION
            }}),
            false,
            None,
        )
        .await;
    assert_eq!(wrong.status, StatusCode::UNAUTHORIZED);
    let forbidden_origin = unauthenticated
        .post(
            Some(CAPABILITY_TOKEN),
            json!({"id": 1, "jsonrpc": "2.0", "method": "initialize", "params": {
                "capabilities": {},
                "clientInfo": {"name": "origin", "version": "1"},
                "protocolVersion": MCP_PROTOCOL_VERSION
            }}),
            false,
            Some("http://untrusted.invalid"),
        )
        .await;
    assert_eq!(forbidden_origin.status, StatusCode::FORBIDDEN);
    for response in [&missing, &malformed, &wrong, &forbidden_origin] {
        assert!(!response.wire.contains(CAPABILITY_TOKEN));
        assert!(!response.wire.contains(WRONG_CAPABILITY_TOKEN));
        assert!(!response.wire.contains(INTERNAL_CANARY));
    }
    assert_eq!(backend.counts().begin_http, 0);

    let mut hosted_client = HostedClient::new(connection);
    let http = hosted_transcript(&mut hosted_client).await;
    assert_transcript_semantics(&http);
    for (stdio_response, http_response) in [
        (&stdio.tools, &http.tools),
        (&stdio.resources, &http.resources),
        (&stdio.templates, &http.templates),
        (&stdio.stable_call, &http.stable_call),
        (&stdio.stale_call, &http.stale_call),
        (&stdio.unknown_call, &http.unknown_call),
        (&stdio.invalid_call, &http.invalid_call),
        (&stdio.active_contract, &http.active_contract),
        (&stdio.command_plan, &http.command_plan),
        (&stdio.projection_status, &http.projection_status),
    ] {
        assert_eq!(
            stdio_response.get("result"),
            http_response.get("result"),
            "transport result parity"
        );
        assert_eq!(
            stdio_response.get("error"),
            http_response.get("error"),
            "transport error parity"
        );
    }
    let counts = backend.counts();
    assert!(counts.begin_http > 0);
    assert_eq!(counts.begin_stdio, 0);
    assert!(counts.tool_discovery > 0);
    assert!(counts.dynamic_resolution >= 3);
    assert_eq!(counts.invocation, 1);
    assert!(counts.resource_discovery >= 2);
    assert!(counts.resource_read >= 3);
    assert!(authenticator.calls() > 0);
    registration.shutdown();
}

#[tokio::test(flavor = "current_thread")]
async fn hosted_get_stream_is_authenticated_single_owner_and_session_bounded() {
    let backend = ConformanceBackend::default();
    let (registration, authenticator) =
        hosted_registration(TEST_AUTHORITY.parse().expect("test address"), backend);
    let connection = registration
        .service_for_peer("127.0.0.1:40125".parse().expect("loopback peer"))
        .expect("hosted loopback connection");
    let mut client = HostedClient::new(connection);
    let initialize = client.initialize(MCP_PROTOCOL_VERSION).await;
    assert_eq!(initialize.status, StatusCode::OK);
    client.notify_initialized().await;

    let wrong_credential = client.open_event_stream(WRONG_CAPABILITY_TOKEN).await;
    assert_eq!(wrong_credential.status(), StatusCode::UNAUTHORIZED);
    drop(wrong_credential);

    let authentication_calls = authenticator.calls();
    let event_stream = client.open_event_stream(CAPABILITY_TOKEN).await;
    assert_eq!(event_stream.status(), StatusCode::OK);
    assert!(
        authenticator.calls() > authentication_calls,
        "standalone GET repeats credential authentication"
    );
    let duplicate_stream = client.open_event_stream(CAPABILITY_TOKEN).await;
    assert_eq!(
        duplicate_stream.status(),
        StatusCode::NOT_FOUND,
        "a second standalone stream is rejected existence-blind"
    );
    drop(duplicate_stream);
    drop(event_stream);
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    let closed_session = client
        .post(
            Some(CAPABILITY_TOKEN),
            json!({"id": 12, "jsonrpc": "2.0", "method": "tools/list"}),
            true,
            None,
        )
        .await;
    assert_eq!(
        closed_session.status,
        StatusCode::NOT_FOUND,
        "dropping the standalone stream tears down its session"
    );
    registration.shutdown();
}

#[tokio::test(flavor = "current_thread")]
async fn hosted_http_does_not_negotiate_a_nonbaseline_protocol() {
    let backend = ConformanceBackend::default();
    let (registration, _) =
        hosted_registration(TEST_AUTHORITY.parse().expect("test address"), backend);
    let connection = registration
        .service_for_peer("127.0.0.1:40124".parse().expect("loopback peer"))
        .expect("hosted loopback connection");
    let mut client = HostedClient::new(connection);
    let response = client.initialize("2025-06-18").await;
    assert_ne!(
        response
            .message
            .as_ref()
            .and_then(|message| message.get("result"))
            .and_then(|result| result.get("protocolVersion"))
            .and_then(Value::as_str),
        Some("2025-06-18")
    );
    assert!(!response.wire.contains(INTERNAL_CANARY));
    registration.shutdown();
}
