//! ADR-0185 compiler proofs for one-level bounded expansion.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{
    OperationalCardinalityClass, OperationalOperatorKind, OperationalPartitionClass,
    PlannerDiagnosticCode, compile_query,
};
use riffdb_query_ir::{QUERY_IR_VERSION_RELATIONAL_OPERATORS_V1, QueryAccessKind, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");

const EXPANSION: &str = r#"query TicketComments(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $status: TicketStatus,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take 25

    many comments from Comment
        for each ticket in tickets
        where organization_id == $organization_id
            && ticket_id == ticket.ticket_id
        order by created_at asc, comment_id asc
        take 8 per ticket

    return Found {
        tickets: tickets {
            ticket_id
            comments: comments { comment_id created_at }
        }
    }
    outcomes Found
}
"#;

fn catalog() -> SymbolicCatalog {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    SymbolicCatalog::from_bundle(&bundle).expect("catalog")
}

// req: RQL-008, OQ-113, OQ-116
#[test]
fn expansion_plan_is_least_sufficient_and_legacy_bytes_are_unchanged() {
    let document = parse_query(EXPANSION).expect("expansion source");
    let plan = compile_query(&document, &catalog()).expect("expansion plan");
    assert_eq!(plan.ir_version(), QUERY_IR_VERSION_RELATIONAL_OPERATORS_V1);
    let step = plan
        .steps()
        .iter()
        .find(|step| step.binding() == "comments")
        .expect("comments step");
    assert!(matches!(
        step.access(),
        QueryAccessKind::ExpansionIndex {
            driver_binding,
            driver_item,
            per_driver_maximum: 8,
            product_maximum: 200,
            index,
            ..
        } if driver_binding == "tickets" && driver_item == "ticket" && index == "by_ticket"
    ));

    let legacy_source = EXPANSION
        .replace("        for each ticket in tickets\n", "")
        .replace("take 8 per ticket", "take 8")
        .replace("ticket.ticket_id", "$ticket_id")
        .replace(
            "    $status: TicketStatus,\n",
            "    $status: TicketStatus,\n    $ticket_id: Ticket.ticket_id,\n",
        )
        .replace(
            "            comments: comments { comment_id created_at }\n",
            "",
        );
    let legacy = compile_query(
        &parse_query(&legacy_source).expect("legacy source"),
        &catalog(),
    )
    .expect("legacy plan");
    assert_ne!(
        legacy.ir_version(),
        QUERY_IR_VERSION_RELATIONAL_OPERATORS_V1
    );
    assert_eq!(
        legacy.identity().hash().into_bytes(),
        [
            183, 148, 22, 237, 225, 232, 37, 108, 178, 210, 172, 202, 189, 174, 220, 90, 253, 26,
            135, 22, 31, 191, 7, 63, 146, 69, 134, 122, 255, 152, 59, 30,
        ],
        "the predecessor program bytes and cursor-bound plan identity are frozen",
    );
}

fn refusal(source: &str) -> riffdb_query_compiler::PlannerDiagnostic {
    let document = parse_query(source).expect("refusal source parses");
    compile_query(&document, &catalog())
        .expect_err("shape must be refused")
        .as_slice()[0]
        .clone()
}

// req: RQL-008, OQ-118
#[test]
fn expansion_refusals_pin_exact_clauses_for_product_partition_prefix_and_depth() {
    let over_product = EXPANSION.replace("take 25", "take 9000");
    let diagnostic = refusal(&over_product);
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::Unbounded);
    assert_eq!(
        &over_product[diagnostic.primary().start as usize..diagnostic.primary().end as usize],
        "for each ticket in tickets"
    );

    let cross_partition = EXPANSION.replace(
        "for each ticket in tickets\n        where organization_id == $organization_id",
        "for each ticket in tickets\n        where organization_id == ticket.ticket_id",
    );
    let diagnostic = refusal(&cross_partition);
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::NonLocal);
    assert_eq!(
        &cross_partition[diagnostic.primary().start as usize..diagnostic.primary().end as usize],
        "for each ticket in tickets"
    );

    let non_prefix = EXPANSION.replace(
        "ticket_id == ticket.ticket_id",
        "comment_id == ticket.ticket_id",
    );
    let diagnostic = refusal(&non_prefix);
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::Unindexed);
    assert_eq!(
        &non_prefix[diagnostic.primary().start as usize..diagnostic.primary().end as usize],
        "for each ticket in tickets"
    );

    let depth_two = EXPANSION.replace(
        "    return Found {",
        "    many labels from TicketLabel\n        for each comment in comments\n        where organization_id == $organization_id\n            && ticket_id == comment.ticket_id\n        order by label_id asc\n        take 2 per comment\n\n    return Found {",
    );
    let diagnostic = refusal(&depth_two);
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::TypeMismatch);
    assert_eq!(
        &depth_two[diagnostic.primary().start as usize..diagnostic.primary().end as usize],
        "comments"
    );
}

// req: OQ-118
#[test]
fn refused_shapes_are_recorded_as_anonymized_refusal_classes() {
    let source = EXPANSION.replace("take 25", "take 9000");
    let diagnostic = refusal(&source);
    let class = diagnostic.refusal_class().expect("refusal class");
    assert_eq!(class.operator(), OperationalOperatorKind::Expansion);
    assert_eq!(
        class.cardinality(),
        OperationalCardinalityClass::BoundedCollection
    );
    assert_eq!(class.partition(), OperationalPartitionClass::SamePartition);
    let exported = format!(
        "operator={} cardinality={} partition={}",
        class.operator().as_str(),
        class.cardinality().as_str(),
        class.partition().as_str(),
    );
    assert_eq!(
        exported,
        "operator=expansion cardinality=bounded_collection partition=same_partition"
    );
    for forbidden in [
        "TicketComments",
        "tickets",
        "comments",
        "organization_id",
        "9000",
    ] {
        assert!(!exported.contains(forbidden));
    }
}
