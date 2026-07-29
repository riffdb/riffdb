//! Stable TicketDesk planner acceptance fixtures.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_query;
use riffdb_query_ir::{QueryAccessKind, QueryPredicateValue, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const QUERIES: &[(&str, &str)] = &[
    (
        "list_tickets",
        include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
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
    let list = compile_query(&parse_query(QUERIES[0].1).expect("parse"), &catalog).expect("plan");
    assert_eq!(
        format!("{}\n", list.explain().lines().join("\n")),
        include_str!("../../../fixtures/riffql/list_tickets.plan")
    );
    assert!(matches!(
        list.steps()[0].access(),
        QueryAccessKind::Index { index, .. } if index == "by_project_status"
    ));

    let detail = compile_query(&parse_query(QUERIES[1].1).expect("parse"), &catalog).expect("plan");
    assert!(matches!(
        detail.steps()[0].access(),
        QueryAccessKind::Point { .. }
    ));
    assert!(matches!(
        detail.steps()[3].access(),
        QueryAccessKind::Index { index, .. } if index == "by_ticket"
    ));
}

#[test]
fn predicate_inputs_are_closed_and_participate_in_plan_identity() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let open = QUERIES[3].1;
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
        QueryPredicateValue::EnumVariant { enumeration, variant }
            if enumeration == "TicketStatus" && variant == "Closed"
    ));
}
