#![forbid(unsafe_code)]

//! Source V4, Manifest V2, Lock V5, and least-authority role acceptance.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    APPLICATION_LOCK_SCHEMA_V5, APPLICATION_MANIFEST_SCHEMA_V2, APPLICATION_SOURCE_SCHEMA_V4,
    ApplicationLock, ApplicationSourceManifest, GeneratedApplicationArtifact,
    GeneratedApplicationArtifactKind, NamedQuerySource, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion, compile_application_role_v2, compile_reactive_source,
    generate_mcp_reactive_tools, generate_python_application_client,
    generate_rust_application_client, generate_typescript_application_client,
};
use riffdb_types::{CapabilityPermissionKindV1, CapabilityPermissionV1};
use serde_json::Value;

const CONTRACT: &str = r#"
contract ReactiveRows version 1 {
  entity Row { key (organization_id: uuid, row_id: uuid) field value: i64 }
  event RowChanged {
    partition_by (organization_id)
    organization_id: uuid
    row_id: uuid
    value: i64
  }
  aggregate Rows { root Row partition_by organization_id conflict_key (organization_id, row_id) }
  command ChangeRow {
    input idempotency_key: string<128>
    input organization_id: uuid
    input row_id: uuid
    idempotency_key idempotency_key
    mutate Row(organization_id, row_id) as row else Missing {}
    set row.value = 1
    emit RowChanged { organization_id: organization_id, row_id: row_id, value: 1 }
    return Changed {}
  }
}
"#;

const QUERY: &str = r#"
query GetRow($organization_id: Row.organization_id, $row_id: Row.row_id) {
  one row from Row where organization_id == $organization_id && row_id == $row_id else Missing
  return Found { row: row { organization_id row_id value } }
  outcomes Found | Missing
}
"#;

const REACTIVE: &str = r#"
reactive RowActivity version 1 {
  stream RowChanges($organization_id: Row.organization_id) {
    partition (organization_id = $organization_id);
    event RowChanged select (organization_id, row_id, value);
  }
  watch RowWatch($organization_id: Row.organization_id, $row_id: Row.row_id) query GetRow updates patch;
  subscription RowAgent($organization_id: Row.organization_id) {
    stream RowChanges(organization_id = $organization_id);
    reaction change command ChangeRow;
    limits { batch 4; in_flight 4; lease_seconds 60; }
  }
}
"#;

const SOURCE: &str = r#"{
  "schema":"riffdb.application-source/v4",
  "application":"rows",
  "contract":{"source":"riffdb/contract.riff","lineage":"ReactiveRows","version":1},
  "generation":{"rust":"generated/rust/client.rs","typescript":"generated/typescript/client.ts","python":"generated/python/client.py","mcp":"generated/mcp/tools.json"},
  "migrations":[],
  "query_modules":[{"name":"rows","version":1,"queries":[{"name":"GetRow","source":"riffdb/queries/get_row.riffq"}]}],
  "reactive_modules":[{"name":"RowActivity","version":1,"source":"riffdb/reactive/row_activity.riffr"}],
  "roles":[{"name":"RowsAgent","environment":"development","tenant_scope":"global","queries":[],"commands":[],"event_streams":["RowChanges"],"watch_queries":["RowWatch"],"agent_subscriptions":["RowAgent"]}],
  "seed_inputs":[]
}"#;

#[test]
fn v5_lock_and_role_bind_every_reactive_identity_without_implicit_seek() {
    let source = ApplicationSourceManifest::parse(SOURCE).expect("source V4");
    assert_eq!(source.schema(), APPLICATION_SOURCE_SCHEMA_V4);
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let query_module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("rows").expect("name"),
            QueryModuleVersion::new(1).expect("version"),
            vec![NamedQuerySource::new("GetRow", QUERY).expect("query")],
        )
        .expect("candidate"),
        &contract,
    )
    .expect("query module");
    let reactive =
        compile_reactive_source(REACTIVE, &contract, std::slice::from_ref(&query_module))
            .expect("reactive module");
    let manifest = source
        .exact_manifest_v2(
            &contract,
            std::slice::from_ref(&query_module),
            std::slice::from_ref(&reactive),
        )
        .expect("manifest V2");
    assert_eq!(manifest.schema(), APPLICATION_MANIFEST_SCHEMA_V2);

    let artifact = |kind, path, bytes: &[u8]| {
        GeneratedApplicationArtifact::new(kind, path, bytes).expect("artifact")
    };
    let artifacts = vec![
        artifact(
            GeneratedApplicationArtifactKind::Rust,
            source.generation().rust(),
            b"rust",
        ),
        artifact(
            GeneratedApplicationArtifactKind::TypeScript,
            source.generation().typescript(),
            b"ts",
        ),
        artifact(
            GeneratedApplicationArtifactKind::Python,
            source.generation().python().expect("python"),
            b"py",
        ),
        artifact(
            GeneratedApplicationArtifactKind::Mcp,
            source.generation().mcp(),
            b"mcp",
        ),
        artifact(
            GeneratedApplicationArtifactKind::ContractBundle,
            riffdb_query_module::CONTRACT_BUNDLE_ARTIFACT_PATH,
            contract.canonical_bytes(),
        ),
        artifact(
            GeneratedApplicationArtifactKind::ReactiveModule,
            "generated/reactive/RowActivity.riffdb.reactive.module",
            reactive.canonical_bytes(),
        ),
    ];
    let lock = ApplicationLock::compile_v5(
        &source,
        &manifest,
        &contract,
        std::slice::from_ref(&query_module),
        std::slice::from_ref(&reactive),
        &artifacts,
        &[],
    )
    .expect("lock V5");
    assert_eq!(lock.schema(), APPLICATION_LOCK_SCHEMA_V5);
    assert_eq!(
        ApplicationLock::decode_canonical(lock.canonical_bytes())
            .expect("decode")
            .identity(),
        lock.identity()
    );

    let role = compile_application_role_v2(
        &manifest,
        "RowsAgent",
        None,
        &contract,
        std::slice::from_ref(&query_module),
        std::slice::from_ref(&reactive),
    )
    .expect("role");
    let permissions = role.internal_grant().permissions();
    for kind in [
        CapabilityPermissionKindV1::ConsumeEventStream,
        CapabilityPermissionKindV1::WatchNamedQuery,
        CapabilityPermissionKindV1::ConsumeContextualSubscription,
    ] {
        assert!(permissions.contains_kind(kind));
    }
    assert!(!permissions.contains_kind(CapabilityPermissionKindV1::SeekEventStreamConsumer));
    assert!(permissions.as_slice().iter().all(|permission| !matches!(
        permission,
        CapabilityPermissionV1::SeekEventStreamConsumer(..)
    )));
}

#[test]
fn reactive_generation_is_exact_typed_and_transport_neutral() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let query_module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("rows").expect("name"),
            QueryModuleVersion::new(1).expect("version"),
            vec![NamedQuerySource::new("GetRow", QUERY).expect("query")],
        )
        .expect("candidate"),
        &contract,
    )
    .expect("query module");
    let reactive =
        compile_reactive_source(REACTIVE, &contract, std::slice::from_ref(&query_module))
            .expect("reactive module");

    let rust =
        generate_rust_application_client(&query_module, &contract, std::slice::from_ref(&reactive));
    for required in [
        "pub struct RowChangesConsumer",
        "pub enum RowChangesEvent",
        "pub enum RowWatchUpdate",
        "pub async fn watch_row_watch",
        "pub async fn next_row_changes",
        "pub struct RowAgentConsumer",
        "pub async fn next_row_agent",
        "pub async fn react_change",
    ] {
        assert!(rust.contains(required), "missing Rust surface: {required}");
    }
    assert!(!rust.contains("riffdb_proto"));

    let typescript = generate_typescript_application_client(
        &query_module,
        &contract,
        std::slice::from_ref(&reactive),
    );
    for required in [
        "export type RowChangesEvent",
        "export type RowWatchUpdate",
        "AsyncIterable<ReactiveConsumerBatch<RowChangesEvent>>",
        "createRowWatchStore",
        "createRowWatchSseRelay",
        "export type ReactiveParameterSchema",
        "parameterSchema: RowChangesParameterSchema",
        "ReactiveEventMutationResult",
        "applyLivePatch",
        "export type RowAgentItem",
        "reactChange",
    ] {
        assert!(
            typescript.contains(required),
            "missing TypeScript surface: {required}"
        );
    }
    assert!(!typescript.contains("Authorization: Bearer"));

    let python = generate_python_application_client(
        &query_module,
        &contract,
        std::slice::from_ref(&reactive),
    )
    .expect("Python client");
    for required in [
        "RowChangesEvent: TypeAlias",
        "class RowWatchSnapshot",
        "async def row_changes",
        "async def watch_row_watch",
        "AsyncIterator[RowWatchUpdate]",
        "encode_reactive_record(parameters, RowChanges_PARAMETER_SCHEMA)",
        "RowChanges_PARAMETER_SCHEMA",
        "class RowAgentItem",
        "async def next_row_agent",
        "async def react_change",
    ] {
        assert!(
            python.contains(required),
            "missing Python surface: {required}"
        );
    }
    assert!(!python.contains("grpc"));

    let tools = generate_mcp_reactive_tools(&reactive, &contract).expect("MCP reactive tools");
    let names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "row_activity_row_agent_ack",
            "row_activity_row_agent_nack",
            "row_activity_row_agent_next",
            "row_activity_row_agent_react_change",
            "row_activity_row_agent_status",
            "row_activity_row_changes_ack",
            "row_activity_row_changes_nack",
            "row_activity_row_changes_next",
            "row_activity_row_changes_seek",
            "row_activity_row_changes_status",
            "row_activity_row_watch_watch",
        ]
    );
    assert!(names.iter().all(|name| !name.contains('.')));
    let next_schema: Value = serde_json::from_str(
        &tools
            .iter()
            .find(|tool| tool.name.ends_with("_next"))
            .expect("next tool")
            .input_schema,
    )
    .expect("MCP input schema");
    assert_eq!(
        next_schema["properties"]["parameters"]["properties"]["organization_id"]["properties"]["type"]
            ["const"],
        "uuid"
    );
}

#[test]
fn cross_language_reactive_fixture_freezes_cursor_tools_and_payload_free_wakeup() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/application-parity/reactive-observation-v1.json"
    ))
    .expect("canonical reactive parity fixture");
    assert_eq!(fixture["schema"], "riffdb.application-reactive-parity/v1");
    assert_eq!(fixture["cursor"]["external_base64"], "AQIDBA==");
    assert_eq!(fixture["terminal_requires_clear"], true);
    let notification = fixture["mcp"]["notification"]
        .as_object()
        .expect("notification object");
    assert_eq!(notification.len(), 2);
    let params = notification["params"]
        .as_object()
        .expect("notification parameters");
    assert_eq!(params.keys().collect::<Vec<_>>(), vec!["uri"]);
    for forbidden in fixture["mcp"]["notification_forbidden_keys"]
        .as_array()
        .expect("forbidden key inventory")
    {
        assert!(!params.contains_key(forbidden.as_str().expect("key")));
    }
}
