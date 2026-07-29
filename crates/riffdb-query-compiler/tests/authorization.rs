//! Complete authorization analysis and fail-closed planner diagnostics.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{PlannerDiagnosticCode, compile_query};
use riffdb_query_ir::SymbolicCatalog;
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");

#[test]
fn authorization_set_contains_predicate_order_key_and_selected_fields() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let source = include_str!("../../../queries/ticketdesk/list_tickets.riffq");
    let program = compile_query(&parse_query(source).expect("parse"), &catalog).expect("plan");
    let ticket = program
        .authorization()
        .iter()
        .find(|access| access.entity() == "Ticket")
        .expect("Ticket access");
    for field in [
        "organization_id",
        "project_id",
        "status",
        "ticket_id",
        "title",
        "updated_at",
        "reporter_id",
        "assignee_id",
    ] {
        assert!(
            ticket.fields().iter().any(|candidate| candidate == field),
            "{field}"
        );
    }
    assert_eq!(ticket.indexes(), &["by_project_status"]);
}

#[test]
fn unindexed_or_nonlocal_query_fails_with_named_spanned_diagnostic() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let unindexed = r#"
query Bad($organization_id: Organization.organization_id, $title: Ticket.title) {
    many tickets from Ticket
        where organization_id == $organization_id && title == $title
        order by updated_at desc
        take 10
    return Found { tickets: tickets { ticket_id } }
    outcomes Found
}
"#;
    let diagnostics =
        compile_query(&parse_query(unindexed).expect("parse"), &catalog).expect_err("reject");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        PlannerDiagnosticCode::Unindexed
    );
    assert!(diagnostics.as_slice()[0].primary().end > diagnostics.as_slice()[0].primary().start);
    assert!(diagnostics.as_slice()[0].suggested_index().is_some());
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
        include_str!("../../../fixtures/riffql/unindexed.snapshot")
    );

    let nonlocal = r#"
query Bad($organization_id: Organization.organization_id, $project_id: Project.project_id) {
    one project from Project
        where project_id == $project_id
        else NotFound
    return Found { project: project { project_id } }
    outcomes Found | NotFound
}
"#;
    let diagnostics =
        compile_query(&parse_query(nonlocal).expect("parse"), &catalog).expect_err("reject");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        PlannerDiagnosticCode::NonLocal
    );
}
