//! Exact TicketDesk contract symbol and public-schema resolution evidence.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_ir::{
    NamedTypeSchema, QueryDiagnosticCode, SourceSymbolKind, SymbolicCatalog, resolve_query_surface,
};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");

const QUERIES: &[(&str, &str)] = &[
    (
        "get_ticket",
        include_str!("../../../queries/ticketdesk/get_ticket.riffq"),
    ),
    (
        "get_user",
        include_str!("../../../queries/ticketdesk/get_user.riffq"),
    ),
    (
        "list_comments",
        include_str!("../../../queries/ticketdesk/list_comments.riffq"),
    ),
    (
        "list_tickets",
        include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
    ),
    (
        "list_tickets_by_assignee",
        include_str!("../../../queries/ticketdesk/list_tickets_by_assignee.riffq"),
    ),
    (
        "ticket_page",
        include_str!("../../../queries/ticketdesk/ticket_page.riffq"),
    ),
    (
        "project_members",
        include_str!("../../../queries/ticketdesk/project_members.riffq"),
    ),
    (
        "project_summary",
        include_str!("../../../queries/ticketdesk/project_summary.riffq"),
    ),
];

#[test]
fn ticketdesk_queries_resolve_to_exact_name_addressed_schemas_and_source_maps() {
    let bundle = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let ticket = catalog.entity("Ticket").expect("Ticket symbol");
    let project = ticket
        .relationship("project")
        .expect("project relationship");
    assert_eq!(project.source_fields(), ["organization_id", "project_id"]);
    assert_eq!(project.target_entity(), "Project");
    assert_eq!(project.target_fields(), ["organization_id", "project_id"]);

    for (name, source) in QUERIES {
        let document = parse_query(source).expect("parse TicketDesk query");
        let resolved = resolve_query_surface(&document, &catalog)
            .unwrap_or_else(|diagnostics| panic!("{name}: {diagnostics:?}"));
        assert_eq!(resolved.contract().lineage().as_str(), "TicketDesk");
        assert!(!resolved.canonical_bytes().is_empty());
        assert!(!resolved.schemas().parameters().is_empty());
        assert!(
            resolved
                .source_map()
                .entries()
                .iter()
                .any(|entry| entry.kind() == SourceSymbolKind::Field),
            "{name}"
        );
        let schema_debug = format!("{:?}", resolved.schemas());
        assert!(!schema_debug.contains("EntityTypeId"));
        assert!(!schema_debug.contains("FieldId"));
        assert!(!schema_debug.contains("IndexId"));
    }
}

#[test]
fn list_query_schema_uses_contract_names_and_declared_page_bound() {
    let bundle = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let list_query = QUERIES
        .iter()
        .find_map(|(name, source)| (*name == "list_tickets").then_some(*source))
        .expect("ListTickets query is present");
    let document = parse_query(list_query).expect("parse list query");
    let resolved = resolve_query_surface(&document, &catalog).expect("resolve list query");

    assert_eq!(
        resolved.schemas().parameters()[0].value_type(),
        &NamedTypeSchema::Scalar("uuid".to_owned())
    );
    let tickets = &resolved.schemas().results()[0].fields()[0];
    assert_eq!(tickets.name(), "tickets");
    assert!(matches!(tickets.value_type(), NamedTypeSchema::List { .. }));
}

#[test]
fn unknown_and_ambiguous_symbols_are_source_spanned_and_value_free() {
    let bundle = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let source = "query Broken($organization_id: Missing.id) { return { secret } }";
    let document = parse_query(source).expect("syntax is valid");
    let diagnostics = resolve_query_surface(&document, &catalog).expect_err("unknown symbol");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), QueryDiagnosticCode::UnknownSymbol);
    assert!(diagnostic.primary().end > diagnostic.primary().start);
    assert!(!diagnostic.summary().contains("secret"));
    let rendered = format!(
        "{}|{:?}|{}..{}|{}|{}\n",
        diagnostic.code().as_str(),
        diagnostic.stage(),
        diagnostic.primary().start,
        diagnostic.primary().end,
        diagnostic.symbol_path().join("."),
        diagnostic.summary()
    );
    assert_eq!(
        rendered,
        include_str!("../../../fixtures/riffql/unknown_symbol.snapshot")
    );

    let ambiguous = r#"
query Ambiguous($organization_id: Organization.organization_id) {
    many TicketStatus from Ticket
        where TicketStatus.Open == status
        take 1
    return Found {
        tickets: TicketStatus {
            ticket_id
        }
    }
    outcomes Found
}
"#;
    let document = parse_query(ambiguous).expect("ambiguous source is syntactically valid");
    let diagnostics =
        resolve_query_surface(&document, &catalog).expect_err("ambiguous path must reject");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        QueryDiagnosticCode::AmbiguousSymbol
    );
}
