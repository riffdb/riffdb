//! Exact TicketDesk contract symbol and public-schema resolution evidence.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_ir::{
    MAX_QUERY_SCANNED_ROWS, NamedTypeSchema, QUERY_CONTINUATION_PROBE_ROWS, QueryDiagnosticCode,
    SourceSymbolKind, SymbolicCatalog, max_query_page_take, page_take_within_scan_bound,
    resolve_query_surface, scanned_rows_budget_for_page_take,
};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");

const QUERIES: &[(&str, &str)] = &[
    (
        "board_page_50",
        include_str!("../../../queries/ticketdesk/board_page_50.riffq"),
    ),
    (
        "board_page_200",
        include_str!("../../../queries/ticketdesk/board_page_200.riffq"),
    ),
    (
        "board_page_450",
        include_str!("../../../queries/ticketdesk/board_page_450.riffq"),
    ),
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

/// Runtime-`Limit` BoardPage shape (Limit max is continuation-aware page take).
const RUNTIME_LIMIT_BOARD_PAGE: &str = r#"query BoardPage(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $status: TicketStatus,
    $limit: Limit<499> = 50,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take $limit

    return Found {
        tickets: tickets {
            ticket_id
            project_id
            title
            status
            reporter_id
            assignee_id
        }
    }

    outcomes Found
}
"#;

const STATIC_TAKE_500: &str = r#"query BoardPage500(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $status: TicketStatus,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take 500

    return Found {
        tickets: tickets {
            ticket_id
            project_id
            title
            status
            reporter_id
            assignee_id
        }
    }

    outcomes Found
}
"#;

const STATIC_TAKE_MAX: &str = r#"query BoardPageMax(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $status: TicketStatus,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take 499

    return Found {
        tickets: tickets {
            ticket_id
            project_id
            title
            status
            reporter_id
            assignee_id
        }
    }

    outcomes Found
}
"#;

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
fn board_page_50_static_resolves_wide_row_without_runtime_limit() {
    let bundle = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let source = QUERIES
        .iter()
        .find_map(|(name, source)| (*name == "board_page_50").then_some(*source))
        .expect("BoardPage50 query is present");
    let document = parse_query(source).expect("parse BoardPage50");
    let resolved = resolve_query_surface(&document, &catalog).expect("resolve BoardPage50");
    let params = resolved.schemas().parameters();
    assert!(
        !params.iter().any(|param| param.name() == "limit"),
        "static BoardPage50 must not expose a runtime Limit parameter"
    );
    let tickets = &resolved.schemas().results()[0].fields()[0];
    assert_eq!(tickets.name(), "tickets");
    assert!(matches!(tickets.value_type(), NamedTypeSchema::List { .. }));
}

#[test]
fn scan_bound_constants_encode_continuation_probe_relationship() {
    assert_eq!(MAX_QUERY_SCANNED_ROWS, 500);
    assert_eq!(QUERY_CONTINUATION_PROBE_ROWS, 1);
    assert_eq!(max_query_page_take(), 499);
    assert_eq!(scanned_rows_budget_for_page_take(499), Some(500));
    assert_eq!(scanned_rows_budget_for_page_take(500), Some(501));
    assert!(page_take_within_scan_bound(499));
    assert!(!page_take_within_scan_bound(500));
    assert!(!page_take_within_scan_bound(0));
}

/// Incidents 019fbf5b-1a64… / 019fbf60-aaac…: take 500 + continuation probe (501)
/// used to deploy then fail as RDB-INTERNAL-0001. Static take 500 is now rejected
/// at resolve with a diagnostic that names the continuation-aware page bound.
#[test]
fn static_take_500_is_rejected_at_resolve_with_bound_diagnostic() {
    let bundle = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let document = parse_query(STATIC_TAKE_500).expect("parse static take 500");
    let diagnostics =
        resolve_query_surface(&document, &catalog).expect_err("take 500 must not resolve");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), QueryDiagnosticCode::ArtifactLimit);
    assert!(
        diagnostic.summary().contains("499"),
        "diagnostic must name the max page take: {}",
        diagnostic.summary()
    );
    assert!(
        diagnostic.summary().contains("continuation probe")
            || diagnostic.summary().contains("scan ceiling"),
        "diagnostic must explain the probe reserve: {}",
        diagnostic.summary()
    );
}

#[test]
fn static_take_at_max_page_take_resolves() {
    let bundle = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let document = parse_query(STATIC_TAKE_MAX).expect("parse static take 499");
    let resolved = resolve_query_surface(&document, &catalog).expect("take 499 resolves");
    let tickets = &resolved.schemas().results()[0].fields()[0];
    assert!(matches!(
        tickets.value_type(),
        NamedTypeSchema::List {
            maximum: riffdb_query_ir::PageBound::Literal(499),
            ..
        }
    ));
}

#[test]
fn runtime_limit_board_page_still_resolves() {
    let bundle = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let document = parse_query(RUNTIME_LIMIT_BOARD_PAGE).expect("parse runtime BoardPage");
    let resolved = resolve_query_surface(&document, &catalog).expect("runtime BoardPage resolves");
    assert!(
        resolved
            .schemas()
            .parameters()
            .iter()
            .any(|param| param.name() == "limit"),
        "runtime BoardPage exposes a Limit parameter"
    );
}

#[test]
fn bounded_limit_maximum_is_retained_in_parameter_and_result_schemas() {
    let bundle = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let source = RUNTIME_LIMIT_BOARD_PAGE.replace("Limit<499> = 50", "Limit<100> = 50");
    let document = parse_query(&source).expect("parse bounded BoardPage");
    let resolved = resolve_query_surface(&document, &catalog).expect("bounded BoardPage resolves");
    let limit = resolved
        .schemas()
        .parameters()
        .iter()
        .find(|parameter| parameter.name() == "limit")
        .expect("limit parameter");
    assert!(matches!(
        limit.value_type(),
        NamedTypeSchema::BoundedLimit { maximum: 100 }
    ));
    assert!(matches!(
        resolved.schemas().results()[0].fields()[0].value_type(),
        NamedTypeSchema::List {
            maximum: riffdb_query_ir::PageBound::BoundedParameter {
                name,
                maximum: 100,
            },
            ..
        } if name == "limit"
    ));
}

#[test]
fn bounded_limit_default_above_declared_maximum_is_rejected() {
    let bundle = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let source = RUNTIME_LIMIT_BOARD_PAGE.replace("Limit<499> = 50", "Limit<49> = 50");
    let document = parse_query(&source).expect("parse bounded BoardPage");
    let diagnostics = resolve_query_surface(&document, &catalog)
        .expect_err("default above declared maximum must fail");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        QueryDiagnosticCode::ArtifactLimit
    );
    assert_eq!(
        diagnostics.as_slice()[0].summary(),
        "Limit default exceeds its compiler-declared maximum"
    );
}

#[test]
fn limit_default_over_max_page_take_is_rejected_at_resolve() {
    let bundle = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let source = r#"query OverDefault(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $limit: Limit<499> = 500,
) {
    many tickets from Ticket
        where organization_id == $organization_id && project_id == $project_id
        order by ticket_id asc
        take $limit
    return Found { tickets: tickets { ticket_id } }
    outcomes Found
}
"#;
    let document = parse_query(source).expect("parse");
    let diagnostics =
        resolve_query_surface(&document, &catalog).expect_err("Limit default 500 must fail");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        QueryDiagnosticCode::ArtifactLimit
    );
    // The summary does not quote 499. Bounded-limit diagnostics are still
    // value-free, which is the same gap RDB-QP010 closed for cost ceilings and
    // which has not yet been extended to declared maxima.
    assert!(
        diagnostics.as_slice()[0]
            .summary()
            .contains("bounded limit default")
            || diagnostics.as_slice()[0].code() == QueryDiagnosticCode::ArtifactLimit
    );
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
