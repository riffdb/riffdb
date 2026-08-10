#![forbid(unsafe_code)]

//! Generates the receipted RiffQL v1-to-operational-v2-to-aggregate-v3 identity rotation.

use std::env;
use std::fs;
use std::path::PathBuf;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_ir::{
    QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1, QUERY_IR_VERSION_OPERATIONAL_V1, QUERY_IR_VERSION_V1,
};
use riffdb_query_module::{
    ApplicationLock, ApplicationSourceManifest, GeneratedApplicationArtifact,
    GeneratedApplicationArtifactKind, NamedQuerySource, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion, compile_application_role, generate_go_client,
    generate_mcp_tools, generate_python_client, generate_rust_client, generate_typescript_client,
};
use riffdb_riffql_syntax::{RIFFQL_LANGUAGE_VERSION, RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1};
use serde_json::{Value, json};

const CONTRACT: &str = r#"
contract OperationalRotation version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: string<32>
    field updated_at: timestamp
    index by_status (organization_id, status, updated_at, ticket_id)
    index all_tickets (organization_id, updated_at, ticket_id)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
}
"#;

const QUERY_V1: &str = r#"
query SearchTickets($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by updated_at asc, ticket_id asc
        take 25
    return Found { tickets: tickets { ticket_id status updated_at } }
    outcomes Found
}
"#;

const QUERY_OPERATIONAL_V1: &str = r#"
query SearchTickets(
    $organization_id: Ticket.organization_id,
    $status: Ticket.status?
) {
    many tickets from Ticket
        where organization_id == $organization_id
          && when $status { status == $status }
        order by updated_at asc, ticket_id asc
        take 25
    return Found { tickets: tickets { ticket_id status updated_at } }
    outcomes Found
}
"#;

const QUERY_OPERATIONAL_AGGREGATE_V1: &str = r#"
query SearchTickets(
    $organization_id: Ticket.organization_id,
    $status: Ticket.status?
) {
    many tickets from Ticket
        where organization_id == $organization_id
          && when $status { status == $status }
        order by updated_at asc, ticket_id asc
        take 25
    aggregate summary from tickets {
        group by status
        count() as ticket_count
        min(updated_at) as earliest
    }
    return Found { summary: summary { status ticket_count earliest } }
    outcomes Found
}
"#;

struct Closure {
    source: ApplicationSourceManifest,
    module: QueryModule,
    manifest_hash: riffdb_types::ApplicationManifestHash,
    role_hash: riffdb_types::ApplicationRoleHash,
    lock: ApplicationLock,
    artifacts: Vec<GeneratedApplicationArtifact>,
}

fn main() {
    let output = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("."), PathBuf::from);
    let contract = compile_contract_source(CONTRACT).expect("operational rotation contract");
    let before = compile_closure(1, QUERY_V1, &contract);
    let after = compile_closure(2, QUERY_OPERATIONAL_V1, &contract);
    let aggregate = compile_closure(3, QUERY_OPERATIONAL_AGGREGATE_V1, &contract);

    let before_query = before.module.query("SearchTickets").expect("before query");
    let after_query = after.module.query("SearchTickets").expect("after query");
    let after_family = after_query
        .operational_family()
        .expect("operational query family");
    let aggregate_query = aggregate
        .module
        .query("SearchTickets")
        .expect("aggregate query");
    let aggregate_family = aggregate_query
        .operational_family()
        .expect("aggregate query family");

    assert_eq!(before.module.format_version(), 1);
    assert_eq!(after.module.format_version(), 2);
    assert_eq!(aggregate.module.format_version(), 3);
    assert_eq!(
        before_query.document().language_version,
        RIFFQL_LANGUAGE_VERSION
    );
    assert_eq!(
        after_query.document().language_version,
        RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1
    );

    let receipt = json!({
        "schema": "riffdb.operational-riffql-identity-rotation/v1",
        "work_package": "WP-563",
        "decision": "ADR-0108",
        "contract": {
            "lineage": contract.lineage().as_str(),
            "version": contract.contract_version().get(),
            "bundle_hash": hex(contract.bundle_hash().as_bytes()),
        },
        "before": closure_receipt(
            &before,
            before_query,
            RIFFQL_LANGUAGE_VERSION,
            QUERY_IR_VERSION_V1,
        ),
        "after": closure_receipt(
            &after,
            after_query,
            RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1,
            QUERY_IR_VERSION_OPERATIONAL_V1,
        ),
        "aggregate_after": closure_receipt(
            &aggregate,
            aggregate_query,
            RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1,
            QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1,
        ),
        "operational_family": {
            "presence_parameters": after_family.presence_parameters(),
            "member_count": after_family.members().len(),
            "member_plan_hashes": after_family.members().iter()
                .map(|member| json!({
                    "presence_mask": member.presence_mask(),
                    "plan_hash": hex(member.program().identity().hash().as_bytes()),
                }))
                .collect::<Vec<_>>(),
            "authorization_entities": after_family.authorization_union().len(),
            "maximum_cost": {
                "steps": after_family.maximum_cost().access_steps(),
                "scan_rows": after_family.maximum_cost().scanned_index_rows(),
                "point_reads": after_family.maximum_cost().point_reads(),
                "dependent_keys": after_family.maximum_cost().dependent_keys(),
                "intermediate_rows": after_family.maximum_cost().intermediate_rows(),
                "projected_values": after_family.maximum_cost().projected_values(),
                "result_bytes": after_family.maximum_cost().encoded_result_bytes(),
            },
        },
        "operational_aggregates": aggregate_family.aggregates().iter().map(|aggregate| json!({
            "name": aggregate.name(),
            "source": aggregate.source_binding(),
            "entity": aggregate.source_entity(),
            "maximum_groups": format!("{:?}", aggregate.maximum_groups()),
            "group_keys": aggregate.group_keys().iter().map(|key| key.field()).collect::<Vec<_>>(),
            "measures": aggregate.measures().iter().map(|measure| json!({
                "alias": measure.alias(),
                "function": format!("{:?}", measure.function()),
                "input_field": measure.input_field(),
                "result_type": format!("{:?}", measure.result_type()),
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "repository_closure": [
            retained_application("examples/agent-alpha/riffdb.application.lock.json"),
            retained_application("examples/ticketdesk/riffdb.application.lock.json"),
        ],
        "verification": [
            "scripts/generate-query-clients --check",
            "scripts/check-application-bindings",
            "scripts/check-generated",
        ],
        "classification": {
            "ordinary_v1_bytes_preserved": true,
            "operational_v2_additive": true,
            "operational_aggregate_v3_additive": true,
            "partial_rotation_is_success": false,
        },
    });

    let destination = output.join("fixtures/riffql/operational-identity-rotation-v1.json");
    fs::create_dir_all(destination.parent().expect("receipt parent"))
        .expect("create receipt directory");
    fs::write(
        destination,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&receipt).expect("canonical receipt JSON")
        ),
    )
    .expect("write operational rotation receipt");
}

fn compile_closure(
    module_version: u64,
    query_source: &str,
    contract: &riffdb_contract_ir::ContractBundle,
) -> Closure {
    let source_json = json!({
        "schema": "riffdb.application-source/v5",
        "application": "operational-rotation",
        "contract": {
            "source": "riffdb/contract.riff",
            "lineage": "OperationalRotation",
            "version": 1,
        },
        "generation": {
            "go": "generated/go/client.go",
            "mcp": "generated/mcp/tools.json",
            "python": "generated/python/client.py",
            "rust": "generated/rust/client.rs",
            "typescript": "generated/typescript/client.ts",
        },
        "migrations": [],
        "query_modules": [{
            "name": "operational",
            "version": module_version,
            "queries": [{
                "name": "SearchTickets",
                "source": "riffdb/queries/search_tickets.riffq",
            }],
        }],
        "reactive_modules": [],
        "roles": [{
            "name": "OperationalReader",
            "environment": "development",
            "tenant_scope": "global",
            "queries": ["SearchTickets"],
            "commands": [],
            "event_streams": [],
            "watch_queries": [],
            "agent_subscriptions": [],
        }],
        "seed_inputs": [],
    });
    let source = ApplicationSourceManifest::parse(
        &serde_json::to_string(&source_json).expect("application source JSON"),
    )
    .expect("application source");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("operational").expect("module name"),
            QueryModuleVersion::new(module_version).expect("module version"),
            vec![NamedQuerySource::new("SearchTickets", query_source).expect("query source")],
        )
        .expect("query module candidate"),
        contract,
    )
    .expect("query module");
    let manifest = source
        .exact_manifest_v2(contract, std::slice::from_ref(&module), &[])
        .expect("exact manifest");
    let role = compile_application_role(
        &manifest,
        "OperationalReader",
        None,
        contract,
        std::slice::from_ref(&module),
    )
    .expect("application role");
    let artifacts = generated_artifacts(&module, contract, &manifest);
    let lock = ApplicationLock::compile_v6(
        &source,
        &manifest,
        contract,
        std::slice::from_ref(&module),
        &[],
        &artifacts,
        &[],
    )
    .expect("application lock");
    Closure {
        source,
        module,
        manifest_hash: manifest.identity(),
        role_hash: role.identity(),
        lock,
        artifacts,
    }
}

fn generated_artifacts(
    module: &QueryModule,
    contract: &riffdb_contract_ir::ContractBundle,
    manifest: &riffdb_query_module::ApplicationManifest,
) -> Vec<GeneratedApplicationArtifact> {
    let rust = generate_rust_client(module, contract);
    let typescript = generate_typescript_client(module, contract);
    let python = generate_python_client(module, contract).expect("Python client");
    let go = generate_go_client(module, contract);
    let mcp_tools = generate_mcp_tools(module).expect("MCP tools");
    let mcp = serde_json::to_vec(&json!({
        "schema": "riffdb-operational-rotation-mcp/v1",
        "tools": mcp_tools.iter().map(|tool| json!({
            "name": tool.name,
            "operation_name": tool.operation_name,
            "module_hash": hex(&tool.module_hash),
            "input_schema": serde_json::from_str::<Value>(&tool.input_schema)
                .expect("MCP input schema"),
            "result_schema": serde_json::from_str::<Value>(&tool.result_schema)
                .expect("MCP result schema"),
        })).collect::<Vec<_>>(),
    }))
    .expect("MCP artifact");
    [
        (
            GeneratedApplicationArtifactKind::Manifest,
            "generated/riffdb.application.exact.json",
            manifest.canonical_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::ContractBundle,
            riffdb_query_module::CONTRACT_BUNDLE_ARTIFACT_PATH,
            contract.canonical_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Rust,
            "generated/rust/client.rs",
            rust.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::TypeScript,
            "generated/typescript/client.ts",
            typescript.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Python,
            "generated/python/client.py",
            python.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Go,
            "generated/go/client.go",
            go.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Mcp,
            "generated/mcp/tools.json",
            mcp.as_slice(),
        ),
    ]
    .into_iter()
    .map(|(kind, path, bytes)| {
        GeneratedApplicationArtifact::new(kind, path, bytes).expect("generated artifact")
    })
    .collect()
}

fn closure_receipt(
    closure: &Closure,
    query: &riffdb_query_module::CompiledNamedQuery,
    language_version: u32,
    ir_version: u32,
) -> Value {
    json!({
        "language_version": language_version,
        "query_ir_version": ir_version,
        "query_module_format_version": closure.module.format_version(),
        "application_source_hash": hex(closure.source.identity().as_bytes()),
        "query_source_hash": hex(query.source_hash().as_bytes()),
        "plan_or_family_hash": hex(query.plan().identity().as_bytes()),
        "module_hash": hex(closure.module.identity().as_bytes()),
        "manifest_hash": hex(closure.manifest_hash.as_bytes()),
        "role_hash": hex(closure.role_hash.as_bytes()),
        "lock_hash": hex(closure.lock.identity().as_bytes()),
        "artifacts": closure.artifacts.iter().map(|artifact| json!({
            "kind": artifact_kind(artifact.kind()),
            "path": artifact.path(),
            "content_hash": hex(artifact.content_hash().as_bytes()),
        })).collect::<Vec<_>>(),
    })
}

fn retained_application(path: &str) -> Value {
    let bytes = fs::read(path).unwrap_or_else(|error| panic!("read {path}: {error}"));
    let lock: Value =
        serde_json::from_slice(&bytes).unwrap_or_else(|error| panic!("parse {path}: {error}"));
    json!({
        "path": path,
        "lock_hash": hex(riffdb_types::hash_application_lock(&bytes).as_bytes()),
        "source_hash": lock["source_hash"],
        "manifest_hash": lock["exact_manifest_hash"],
        "contract_bundle_hash": lock["contract"]["bundle_hash"],
        "query_module_format": lock["compiler_formats"]["query_module"],
        "module_hashes": lock["modules"].as_array().expect("module array").iter()
            .map(|module| module["module_hash"].clone()).collect::<Vec<_>>(),
        "role_hashes": lock["roles"].as_array().expect("role array").iter()
            .map(|role| role["definition_hash"].clone()).collect::<Vec<_>>(),
        "artifact_hashes": lock["artifacts"].as_array().expect("artifact array").iter()
            .map(|artifact| artifact["content_hash"].clone()).collect::<Vec<_>>(),
        "rotation": "unchanged_ordinary_v1",
    })
}

const fn artifact_kind(kind: GeneratedApplicationArtifactKind) -> &'static str {
    match kind {
        GeneratedApplicationArtifactKind::Manifest => "manifest",
        GeneratedApplicationArtifactKind::Rust => "rust",
        GeneratedApplicationArtifactKind::TypeScript => "typescript",
        GeneratedApplicationArtifactKind::Python => "python",
        GeneratedApplicationArtifactKind::Go => "go",
        GeneratedApplicationArtifactKind::Mcp => "mcp",
        GeneratedApplicationArtifactKind::ContractBundle => "contract_bundle",
        GeneratedApplicationArtifactKind::ReactiveModule => "reactive_module",
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("string");
    }
    output
}
