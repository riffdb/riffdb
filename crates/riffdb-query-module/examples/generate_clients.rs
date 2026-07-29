#![forbid(unsafe_code)]

//! Regenerates the checked TicketDesk Rust and TypeScript clients.

use std::env;
use std::fs;
use std::path::PathBuf;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    generate_rust_client, generate_typescript_client,
};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const QUERIES: [(&str, &str); 4] = [
    (
        "ListTickets",
        include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
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
}
