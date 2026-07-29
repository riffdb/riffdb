#![forbid(unsafe_code)]

//! Regenerates the checked TicketDesk Rust and TypeScript clients.

use std::env;
use std::fs;
use std::path::PathBuf;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    ApplicationManifest, NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName,
    QueryModuleVersion, compile_application_role, generate_mcp_commands, generate_mcp_tools,
    generate_rust_client, generate_typescript_client,
};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const APPLICATION_MANIFEST: &str =
    include_str!("../../../fixtures/application-manifests/ticketdesk-v1.json");
const QUERIES: [(&str, &str); 8] = [
    (
        "GetTicket",
        include_str!("../../../queries/ticketdesk/get_ticket.riffq"),
    ),
    (
        "GetUser",
        include_str!("../../../queries/ticketdesk/get_user.riffq"),
    ),
    (
        "ListComments",
        include_str!("../../../queries/ticketdesk/list_comments.riffq"),
    ),
    (
        "ListTickets",
        include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
    ),
    (
        "ListTicketsByAssignee",
        include_str!("../../../queries/ticketdesk/list_tickets_by_assignee.riffq"),
    ),
    (
        "ProjectMembers",
        include_str!("../../../queries/ticketdesk/project_members.riffq"),
    ),
    (
        "ProjectSummary",
        include_str!("../../../queries/ticketdesk/project_summary.riffq"),
    ),
    (
        "TicketPage",
        include_str!("../../../queries/ticketdesk/ticket_page.riffq"),
    ),
];

fn main() {
    let output = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("."), PathBuf::from);
    let contract = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let application = ApplicationManifest::decode_canonical(APPLICATION_MANIFEST.as_bytes())
        .expect("canonical TicketDesk application manifest");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("ticketdesk").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        QUERIES
            .into_iter()
            .map(|(name, source)| NamedQuerySource::new(name, source).expect("query source"))
            .collect(),
    )
    .expect("module candidate");
    let module = QueryModule::compile(candidate, &contract).expect("compile query module");
    assert_eq!(
        application.contract().lineage(),
        contract.lineage().as_str()
    );
    assert_eq!(
        application.contract().version(),
        contract.contract_version().get()
    );
    assert_eq!(application.contract().bundle_hash(), contract.bundle_hash());
    assert_eq!(
        application.query_modules()[0].module_hash(),
        module.identity()
    );
    let role = compile_application_role(
        &application,
        "TicketDeskAgent",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("compile TicketDesk role");
    fs::create_dir_all(output.join("fixtures/query-modules")).expect("fixture directory");
    fs::create_dir_all(output.join("fixtures/application-manifests"))
        .expect("manifest fixture directory");
    fs::create_dir_all(output.join("clients/typescript/ticketdesk")).expect("client directory");
    fs::write(
        output.join("fixtures/query-modules/ticketdesk.rs"),
        generate_rust_client(&module, &contract),
    )
    .expect("Rust fixture");
    fs::write(
        output.join("clients/typescript/ticketdesk/client.ts"),
        generate_typescript_client(&module, &contract),
    )
    .expect("TypeScript fixture");
    let tools = generate_mcp_tools(&module).expect("generate MCP tools");
    let commands = generate_mcp_commands(&module, &contract).expect("generate MCP commands");
    let manifest = serde_json::json!({
        "schema": "riffdb-generated-mcp-tools-v1",
        "application_manifest_hash": hex(application.identity().as_bytes()),
        "tools": tools
            .iter()
            .map(|tool| serde_json::json!({
                "name": tool.name,
                "title": tool.title,
                "description": tool.description,
                "module_hash": hex(&tool.module_hash),
                "input_schema": serde_json::from_str::<serde_json::Value>(&tool.input_schema)
                    .expect("input schema"),
                "result_schema": serde_json::from_str::<serde_json::Value>(&tool.result_schema)
                    .expect("result schema"),
                "annotations": {
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false,
                },
            }))
            .collect::<Vec<_>>(),
        "commands": commands
            .iter()
            .map(|command| serde_json::json!({
                "name": command.name,
                "title": command.title,
                "description": command.description,
                "contract_bundle_hash": hex(&command.contract_bundle_hash),
                "plan_hash": hex(&command.plan_hash),
                "input_schema": serde_json::from_str::<serde_json::Value>(&command.input_schema)
                    .expect("input schema"),
                "result_schema": serde_json::from_str::<serde_json::Value>(&command.result_schema)
                    .expect("result schema"),
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": true,
                    "idempotentHint": true,
                    "openWorldHint": false,
                },
            }))
            .collect::<Vec<_>>(),
    });
    fs::write(
        output.join("fixtures/query-modules/ticketdesk.mcp.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&manifest).expect("MCP manifest")
        ),
    )
    .expect("MCP fixture");
    fs::write(
        output.join("fixtures/application-manifests/ticketdesk-v1.identity"),
        format!("{}\n", hex(application.identity().as_bytes())),
    )
    .expect("manifest identity fixture");
    fs::write(
        output.join("fixtures/application-manifests/ticketdesk-agent-role-v1.identity"),
        format!("{}\n", hex(role.identity().as_bytes())),
    )
    .expect("role identity fixture");
    let role_description = serde_json::json!({
        "application": role.application_name(),
        "application_manifest_hash": hex(role.manifest_hash().as_bytes()),
        "contract": {
            "bundle_hash": hex(role.contract_hash().as_bytes()),
            "lineage": role.contract_lineage().as_str(),
            "version": role.contract_version().get(),
        },
        "environment": role.environment().as_str(),
        "query_module_hashes": role.module_hashes().iter()
            .map(|hash| hex(hash.as_bytes()))
            .collect::<Vec<_>>(),
        "role": role.role_name(),
        "role_hash": hex(role.identity().as_bytes()),
        "tenant_scope": "global",
        "operations": role.operations().iter().map(|operation| serde_json::json!({
            "kind": match operation.kind() {
                riffdb_query_module::ApplicationRoleOperationKind::Query => "query",
                riffdb_query_module::ApplicationRoleOperationKind::Command => "command",
            },
            "name": operation.name(),
        })).collect::<Vec<_>>(),
    });
    fs::write(
        output.join("fixtures/application-manifests/ticketdesk-agent-role-v1.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&role_description).expect("role description")
        ),
    )
    .expect("role description fixture");
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("string");
    }
    output
}
