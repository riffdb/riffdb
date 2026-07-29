//! Stable TicketDesk planner acceptance fixtures.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_query;
use riffdb_query_ir::{QueryAccessKind, QueryPredicateValue, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");

fn query(name: &str) -> &'static str {
    QUERIES
        .iter()
        .find_map(|(query_name, source)| (*query_name == name).then_some(*source))
        .unwrap_or_else(|| panic!("missing query fixture {name}"))
}

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
fn every_ticketdesk_query_has_one_stable_bounded_same_partition_program() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    for (name, source) in QUERIES {
        let document = parse_query(source).expect("parse");
        let first = compile_query(&document, &catalog)
            .unwrap_or_else(|diagnostics| panic!("{name}: {diagnostics:?}"));
        let second = compile_query(&document, &catalog).expect("deterministic second compile");
        assert_eq!(first, second, "{name}");
        assert_eq!(first.partition_parameter(), "organization_id", "{name}");
        assert!(!first.steps().is_empty(), "{name}");
        assert!(first.steps().iter().all(|step| step.maximum_rows() <= 500));
        assert_eq!(first.identity(), second.identity(), "{name}");
        assert!(
            !first
                .explain()
                .lines()
                .iter()
                .any(|line| line.contains("Id("))
        );
    }
}

#[test]
fn list_and_detail_choose_expected_physical_accesses() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let list = compile_query(
        &parse_query(query("list_tickets")).expect("parse"),
        &catalog,
    )
    .expect("plan");
    assert_eq!(
        format!("{}\n", list.explain().lines().join("\n")),
        include_str!("../../../fixtures/riffql/list_tickets.plan")
    );
    assert!(matches!(
        list.steps()[0].access(),
        QueryAccessKind::Index { index, .. } if index == "by_project_status"
    ));

    let detail =
        compile_query(&parse_query(query("ticket_page")).expect("parse"), &catalog).expect("plan");
    let step = |binding: &str| {
        detail
            .steps()
            .iter()
            .find(|step| step.binding() == binding)
            .unwrap_or_else(|| panic!("missing {binding}"))
    };
    assert!(matches!(
        step("ticket").access(),
        QueryAccessKind::Point { .. }
    ));
    assert!(matches!(
        step("comments").access(),
        QueryAccessKind::Index { index, .. } if index == "by_ticket"
    ));
    assert!(matches!(
        step("ticket_labels").access(),
        QueryAccessKind::Index { index, .. } if index == "by_ticket"
    ));
    assert!(matches!(
        step("labels").access(),
        QueryAccessKind::DependentPointBatch {
            source_binding,
            source_field,
            ..
        } if source_binding == "ticket_labels" && source_field == "label_id"
    ));
    let ticket = &detail.steps()[0];
    assert!(
        ["project_id", "reporter_id", "assignee_id"]
            .iter()
            .all(|field| ticket.predicate_fields().iter().any(|read| read == field)),
        "downstream join fields must be fetched from the source binding"
    );
    assert!(
        ["reporter_id", "assignee_id"].iter().all(|field| !ticket
            .selected_fields()
            .iter()
            .any(|selected| selected == field)),
        "dependency-only fields must not leak into the public result"
    );
}

#[test]
fn collection_dependencies_are_not_implicitly_scalar_or_unbounded() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let ticket_page = query("ticket_page");
    let scalar = ticket_page.replace(
        "label_id in ticket_labels.label_id",
        "label_id == ticket_labels.label_id",
    );
    let diagnostics =
        compile_query(&parse_query(&scalar).expect("parse"), &catalog).expect_err("reject scalar");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        riffdb_query_compiler::PlannerDiagnosticCode::Cardinality
    );
    assert_eq!(
        diagnostics.as_slice()[0].symbol_path(),
        &["ticket_labels", "label_id"]
    );
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(
        format!(
            "{}|{}..{}|{}|{}|{}\n",
            diagnostic.code().as_str(),
            diagnostic.primary().start,
            diagnostic.primary().end,
            diagnostic.symbol_path().join("."),
            diagnostic.summary(),
            diagnostic.suggested_index().unwrap_or("")
        ),
        include_str!("../../../fixtures/riffql/cardinality.snapshot")
    );

    let singular_set = ticket_page.replace(
        "label_id in ticket_labels.label_id",
        "label_id in ticket.ticket_id",
    );
    let diagnostics = compile_query(&parse_query(&singular_set).expect("parse"), &catalog)
        .expect_err("reject singular set");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        riffdb_query_compiler::PlannerDiagnosticCode::Cardinality
    );

    let no_outcome = ticket_page.replace(
        "        take 50\n        else IntegrityFailure\n\n    return Found",
        "        take 50\n\n    return Found",
    );
    let diagnostics = compile_query(&parse_query(&no_outcome).expect("parse"), &catalog)
        .expect_err("reject implicit missing targets");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        riffdb_query_compiler::PlannerDiagnosticCode::Cardinality
    );

    let target_exceeds_source = ticket_page.replacen(
        "        take 50\n\n    many labels",
        "        take 25\n\n    many labels",
        1,
    );
    let diagnostics = compile_query(
        &parse_query(&target_exceeds_source).expect("parse"),
        &catalog,
    )
    .expect_err("reject target bound above source");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        riffdb_query_compiler::PlannerDiagnosticCode::Cardinality
    );

    let noncanonical_source = ticket_page.replace(
        "order by label_id asc\n        take 50\n\n    many labels",
        "order by label_id desc\n        take 50\n\n    many labels",
    );
    let diagnostics = compile_query(&parse_query(&noncanonical_source).expect("parse"), &catalog)
        .expect_err("reject reverse collection source");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        riffdb_query_compiler::PlannerDiagnosticCode::Cardinality
    );
}

#[test]
fn predicate_inputs_are_closed_and_participate_in_plan_identity() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let open = query("project_summary");
    let closed = open.replace("status == $status", "status == TicketStatus.Closed");
    let open = compile_query(&parse_query(open).expect("parse"), &catalog).expect("plan");
    let closed = compile_query(&parse_query(&closed).expect("parse"), &catalog).expect("plan");
    assert_ne!(open.identity(), closed.identity());
    assert!(matches!(
        open.steps()[1].predicates()[2].value(),
        QueryPredicateValue::Parameter(name) if name == "status"
    ));
    assert!(matches!(
        closed.steps()[1].predicates()[2].value(),
        QueryPredicateValue::EnumVariant {
            enumeration,
            variant,
            ..
        }
            if enumeration == "TicketStatus" && variant == "Closed"
    ));
}
