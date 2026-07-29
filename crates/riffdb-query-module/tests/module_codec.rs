//! Query-module identity and strict-codec acceptance tests.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleErrorKind, QueryModuleName,
    QueryModuleVersion, generate_mcp_tools, generate_rust_client, generate_typescript_client,
};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");

fn candidate(reversed: bool) -> QueryModuleCandidate {
    let mut queries = vec![
        NamedQuerySource::new(
            "ListTickets",
            include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
        )
        .expect("list source"),
        NamedQuerySource::new(
            "TicketPage",
            include_str!("../../../queries/ticketdesk/ticket_page.riffq"),
        )
        .expect("page source"),
    ];
    if reversed {
        queries.reverse();
    }
    QueryModuleCandidate::new(
        QueryModuleName::new("ticketdesk").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        queries,
    )
    .expect("module candidate")
}

#[test]
fn generated_mcp_tools_are_module_pinned_name_addressed_and_domain_shaped() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(candidate(false), &bundle).expect("module");
    let tools = generate_mcp_tools(&module).expect("tools");

    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0].name, "ticketdesk.list_tickets");
    assert_eq!(tools[1].name, "ticketdesk.ticket_page");
    assert!(
        tools
            .iter()
            .all(|tool| tool.module_hash == *module.identity().as_bytes())
    );
    let input: serde_json::Value =
        serde_json::from_str(&tools[0].input_schema).expect("input schema");
    let result: serde_json::Value =
        serde_json::from_str(&tools[1].result_schema).expect("result schema");
    assert_eq!(input["properties"]["organization_id"]["type"], "string");
    assert_eq!(input["properties"]["statuses"]["uniqueItems"], true);
    assert_eq!(
        result["oneOf"][0]["properties"]["outcome"]["const"],
        "Found"
    );
    assert_eq!(
        result["oneOf"][0]["properties"]["ticket"]["properties"]["title"]["type"],
        "string"
    );
    assert!(!tools[0].input_schema.contains("field_id"));
    assert!(!tools[0].result_schema.contains("entity_type_id"));
}

#[test]
fn identity_and_bytes_are_independent_of_input_order() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let first = QueryModule::compile(candidate(false), &bundle).expect("first module");
    let second = QueryModule::compile(candidate(true), &bundle).expect("second module");

    assert_eq!(first.identity(), second.identity());
    assert_eq!(first.canonical_bytes(), second.canonical_bytes());
    assert_eq!(first.queries()[0].name(), "ListTickets");
    assert_eq!(first.queries()[1].name(), "TicketPage");
}

#[test]
fn strict_decode_recompiles_against_the_exact_contract() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(candidate(false), &bundle).expect("module");
    let decoded =
        QueryModule::decode_and_validate(module.canonical_bytes(), &bundle).expect("decode");
    assert_eq!(decoded.identity(), module.identity());
    assert_eq!(
        decoded.query("TicketPage").expect("named query").name(),
        "TicketPage"
    );

    let mut damaged = module.canonical_bytes().to_vec();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    let error = QueryModule::decode_and_validate(&damaged, &bundle).expect_err("damage rejects");
    assert!(matches!(
        error.kind(),
        QueryModuleErrorKind::InvalidEncoding | QueryModuleErrorKind::IdentityMismatch
    ));
}

#[test]
fn same_version_with_changed_source_has_a_distinct_identity() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let original = QueryModule::compile(candidate(false), &bundle).expect("module");
    let changed = QueryModuleCandidate::new(
        QueryModuleName::new("ticketdesk").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![
            NamedQuerySource::new(
                "ListTickets",
                include_str!("../../../queries/ticketdesk/list_tickets.riffq")
                    .replace("$limit: Limit = 25", "$limit: Limit = 26"),
            )
            .expect("changed source"),
        ],
    )
    .expect("changed candidate");
    let changed = QueryModule::compile(changed, &bundle).expect("changed module");
    assert_ne!(changed.identity(), original.identity());
}

#[test]
fn generated_clients_are_reproducible_name_addressed_and_identity_pinned() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(candidate(false), &bundle).expect("module");
    let rust = generate_rust_client(&module, &bundle);
    let typescript = generate_typescript_client(&module, &bundle);

    assert_eq!(rust, generate_rust_client(&module, &bundle));
    assert_eq!(typescript, generate_typescript_client(&module, &bundle));
    assert!(rust.contains("pub struct ListTicketsParams"));
    assert!(rust.contains("query_name: \"TicketPage\""));
    assert!(rust.contains("pub struct CreateTicketInput"));
    assert!(rust.contains("pub const CONTRACT_BUNDLE_HASH"));
    assert!(rust.contains("pub fn accepts_identity"));
    assert!(typescript.contains("export interface TicketPageParams"));
    assert!(typescript.contains("queryName: \"ListTickets\""));
    assert!(typescript.contains("export interface CreateTicketInput"));
    assert!(typescript.contains("export const CONTRACT_BUNDLE_HASH"));
    assert!(typescript.contains("export function acceptsIdentity"));
    assert!(!rust.contains("field_id"));
    assert!(!typescript.contains("entity_type_id"));
}
