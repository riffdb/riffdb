#![forbid(unsafe_code)]

//! Regenerates the checked TicketDesk Rust and TypeScript clients.

use std::env;
use std::fs;
use std::path::PathBuf;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    generate_mcp_tools, generate_rust_client, generate_typescript_client,
};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
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
    fs::create_dir_all(output.join("fixtures/query-modules")).expect("fixture directory");
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
    let manifest = serde_json::json!({
        "schema": "riffdb-generated-mcp-tools-v1",
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
    });
    fs::write(
        output.join("fixtures/query-modules/ticketdesk.mcp.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&manifest).expect("MCP manifest")
        ),
    )
    .expect("MCP fixture");
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("string");
    }
    output
}
