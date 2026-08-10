//! Query-module identity and strict-codec acceptance tests.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    ApplicationManifest, ManifestErrorKind, NamedQuerySource, QueryModule, QueryModuleCandidate,
    QueryModuleErrorKind, QueryModuleName, QueryModuleVersion, generate_go_client,
    generate_mcp_commands, generate_mcp_tools, generate_python_client, generate_rust_client,
    generate_typescript_client,
};
use std::sync::Arc;

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
    assert_eq!(tools[0].name, "ticketdesk_list_tickets");
    assert_eq!(tools[1].name, "ticketdesk_ticket_page");
    assert_eq!(tools[0].operation_name, "ListTickets");
    assert_eq!(tools[1].operation_name, "TicketPage");
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

    let commands = generate_mcp_commands(&module, &bundle).expect("commands");
    assert_eq!(commands.len(), 11);
    assert_eq!(commands[0].name, "ticketdesk_add_project_member");
    for expected in [
        "ticketdesk_close_ticket_with_comment",
        "ticketdesk_open_ticket_with_labels",
        "ticketdesk_swap_member_roles",
    ] {
        assert!(commands.iter().any(|command| command.name == expected));
    }
    assert!(commands[0].input_schema.contains("idempotency_key"));
    assert!(commands[0].result_schema.contains("\"outcome\""));
    assert!(
        commands
            .iter()
            .all(|command| command.contract_bundle_hash == *bundle.bundle_hash().as_bytes())
    );
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
fn cloned_named_query_handles_share_immutable_checked_artifacts() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(candidate(false), &bundle).expect("module");
    let query = module.query("TicketPage").expect("named query");

    let first_document = query.shared_document();
    let second_document = query.shared_document();
    let first_program = query.shared_program();
    let second_program = query.shared_program();

    assert!(Arc::ptr_eq(&first_document, &second_document));
    assert!(Arc::ptr_eq(&first_program, &second_program));
    assert_eq!(query.document(), first_document.as_ref());
    assert_eq!(query.program(), first_program.as_ref());
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
    let python = generate_python_client(&module, &bundle).expect("Python");
    let go = generate_go_client(&module, &bundle);

    assert_eq!(rust, generate_rust_client(&module, &bundle));
    assert_eq!(typescript, generate_typescript_client(&module, &bundle));
    assert_eq!(
        python,
        generate_python_client(&module, &bundle).expect("Python")
    );
    assert_eq!(go, generate_go_client(&module, &bundle));
    assert!(rust.contains("pub struct ListTicketsParams"));
    assert!(rust.contains("\"TicketPage\",\n            Some(QUERY_MODULE_HASH)"));
    assert!(rust.contains("pub struct CreateTicketInput"));
    assert!(rust.contains("pub const CONTRACT_BUNDLE_HASH"));
    assert!(rust.contains("impl GeneratedQuery for TicketPageQuery"));
    assert!(rust.contains("pub struct TicketDeskClient"));
    assert!(rust.contains("execute_generated_command"));
    assert!(rust.contains("fn decode_outcome"));
    assert!(rust.contains("with_options(options)"));
    assert!(typescript.contains("export interface TicketPageParams"));
    assert!(typescript.contains("queryName: \"ListTickets\""));
    assert!(typescript.contains("export interface CreateTicketInput"));
    assert!(typescript.contains("export const CONTRACT_BUNDLE_HASH"));
    assert!(typescript.contains("export function acceptsIdentity"));
    assert!(typescript.contains("export class TicketDeskClient"));
    assert!(typescript.contains("executeNamedQuery"));
    assert!(typescript.contains("executeCommand"));
    assert!(
        typescript
            .contains("driverOperation: { name: \"ticketdesk_list_tickets\", inputSchemaHash: \"")
    );
    assert!(
        typescript
            .contains("driverOperation: { name: \"ticketdesk_create_ticket\", inputSchemaHash: \"")
    );
    assert!(typescript.contains("executeCommandBatch?<I, R>"));
    assert!(typescript.contains("this.transport.executeCommandBatch"));
    assert!(typescript.contains("export class RiffDbApplicationError"));
    assert!(typescript.contains("export function decodeApplicationError"));
    assert!(typescript.contains("\"RDB-AUTH-0214\""));
    assert!(typescript.contains("RiffDB application identity mismatch"));
    assert!(go.contains("package ticketdesk"));
    assert!(go.contains("func (client *Client) TicketPage("));
    assert!(go.contains("func (client *Client) CreateTicketBatch("));
    assert!(go.contains("ticketdesk_create_ticket"));
    assert!(python.contains("class TicketDeskClient:"));
    assert!(python.contains("class AsyncTicketDeskClient:"));
    assert!(python.contains("class CreateTicketInput:"));
    assert!(python.contains("def create_ticket_batch("));
    assert!(python.contains("CONTRACT_BUNDLE_HASH: Final[str]"));
    assert!(!rust.contains("pub field_id"));
    assert!(!typescript.contains("entity_type_id"));
}

#[test]
fn application_manifest_is_canonical_bounded_and_identity_bearing() {
    let source = include_str!("../../../fixtures/application-manifests/ticketdesk-v1.json");
    let manifest = ApplicationManifest::parse(source).expect("manifest");

    assert_eq!(manifest.application_name(), "ticketdesk");
    assert_eq!(manifest.contract().lineage(), "TicketDesk");
    assert_eq!(manifest.query_modules().len(), 1);
    assert_eq!(manifest.roles().len(), 2);
    assert_eq!(
        ApplicationManifest::decode_canonical(manifest.canonical_bytes())
            .expect("canonical round trip")
            .identity(),
        manifest.identity()
    );
    assert!(
        manifest
            .source_map()
            .span("contract.source")
            .is_some_and(|span| span.start() < span.end())
    );
    assert_ne!(
        manifest
            .source_map()
            .span("roles.TicketDeskAgent.environment"),
        manifest
            .source_map()
            .span("roles.TicketDeskApplication.environment")
    );

    let noncanonical = format!("\n{source}");
    let error = ApplicationManifest::decode_canonical(noncanonical.as_bytes())
        .expect_err("strict decoder rejects noncanonical source");
    assert_eq!(error.kind(), ManifestErrorKind::NonCanonical);
}

#[test]
fn unsupported_application_manifest_fixture_fails_closed() {
    let source = include_str!("../../../fixtures/application-manifests/pre-v1-unsupported.json");
    let error = ApplicationManifest::parse(source).expect_err("v0 must remain unsupported");
    assert!(matches!(
        error.kind(),
        ManifestErrorKind::InvalidShape | ManifestErrorKind::UnsupportedVersion
    ));
}
