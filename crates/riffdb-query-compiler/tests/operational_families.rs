//! Finite operational plan-family safety and identity acceptance.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{
    PlannerDiagnosticCode, compile_operational_query_family, compile_query,
};
use riffdb_query_ir::{MAX_OPERATIONAL_PRESENCE_PARAMETERS, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = r#"
contract Operational version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: string<32>
    field priority: string<32>
    field updated_at: timestamp
    index a_status_priority (organization_id, status, priority, updated_at, ticket_id)
    index b_status (organization_id, status, updated_at, ticket_id)
    index c_priority (organization_id, priority, updated_at, ticket_id)
    index z_all (organization_id, updated_at, ticket_id)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
}
"#;

const QUERY: &str = r#"
query SearchTickets(
    $organization_id: Ticket.organization_id,
    $status: Ticket.status?,
    $priority: Ticket.priority?
) {
    many tickets from Ticket
        where organization_id == $organization_id
          && when $status { status == $status }
          && when $priority { priority == $priority }
        order by updated_at asc, ticket_id asc
        take 25
    return Found { tickets: tickets { ticket_id status priority updated_at } }
    outcomes Found
}
"#;

fn catalog(source: &str) -> SymbolicCatalog {
    let bundle = compile_contract_source(source).expect("contract");
    SymbolicCatalog::from_bundle(&bundle).expect("catalog")
}

#[test]
fn every_presence_mask_selects_one_precompiled_bounded_member() {
    let catalog = catalog(CONTRACT);
    let document = parse_query(QUERY).expect("query");
    let first = compile_operational_query_family(&document, &catalog).expect("family");
    let second = compile_operational_query_family(&document, &catalog).expect("same family");

    assert_eq!(first, second);
    assert_eq!(first.identity(), second.identity());
    assert_eq!(first.presence_parameters(), &["priority", "status"]);
    assert_eq!(first.members().len(), 4);
    assert_eq!(
        first.select(&[false, false]).expect("00").presence_mask(),
        0
    );
    assert_eq!(first.select(&[true, false]).expect("01").presence_mask(), 1);
    assert_eq!(first.select(&[false, true]).expect("10").presence_mask(), 2);
    assert_eq!(first.select(&[true, true]).expect("11").presence_mask(), 3);
    assert!(first.select(&[true]).is_none());

    let indexes = first.authorization_union()[0].indexes();
    let member_indexes = first
        .members()
        .iter()
        .map(|member| {
            (
                member.presence_mask(),
                member.program().authorization()[0].indexes().to_vec(),
                member.program().steps()[0].predicates().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    for expected in ["a_status_priority", "b_status", "c_priority", "z_all"] {
        assert!(
            indexes.iter().any(|candidate| candidate == expected),
            "{expected}: {indexes:?}; {member_indexes:?}"
        );
    }
    for member in first.members() {
        assert!(first.maximum_cost().covers(member.program().cost()));
        assert_eq!(member.program().partition_parameter(), "organization_id");
        assert_eq!(
            member.program().surface().schemas(),
            first.members()[0].program().surface().schemas()
        );
    }
}

#[test]
fn ordinary_compiler_rejects_operational_source_instead_of_ignoring_it() {
    let catalog = catalog(CONTRACT);
    let document = parse_query(QUERY).expect("query");
    let diagnostics = compile_query(&document, &catalog).expect_err("closed V1 compiler");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        PlannerDiagnosticCode::OperationalFamilyRequired
    );
}

#[test]
fn one_unindexed_presence_member_rejects_the_complete_family() {
    let contract = CONTRACT.replace(
        "    index z_all (organization_id, updated_at, ticket_id)\n",
        "",
    );
    let catalog = catalog(&contract);
    let document = parse_query(QUERY).expect("query");
    let diagnostics =
        compile_operational_query_family(&document, &catalog).expect_err("mask zero unindexed");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::Unindexed);
    assert!(diagnostic.primary().start < diagnostic.primary().end);
    assert!(diagnostic.suggested_index().is_some());
}

#[test]
fn optional_inputs_are_usable_only_in_matching_top_level_guards() {
    let catalog = catalog(CONTRACT);
    let cases = [
        (
            QUERY.replace("when $status { status == $status }", "status == $status"),
            PlannerDiagnosticCode::OperationalFamilyRequired,
        ),
        (
            QUERY.replace(
                "when $status { status == $status }",
                "true || when $status { status == $status }",
            ),
            PlannerDiagnosticCode::OperationalFamilyRequired,
        ),
        (
            QUERY.replace("$status: Ticket.status?", "$status: Ticket.status"),
            PlannerDiagnosticCode::TypeMismatch,
        ),
        (
            QUERY.replace(
                "when $status { status == $status }",
                "when $status { status == \"Open\" }",
            ),
            PlannerDiagnosticCode::OperationalFamilyRequired,
        ),
    ];
    for (source, expected) in cases {
        let document = parse_query(&source).expect("syntax");
        let diagnostics = compile_operational_query_family(&document, &catalog)
            .expect_err("unsafe optional shape");
        assert_eq!(diagnostics.as_slice()[0].code(), expected, "{source}");
        assert!(
            diagnostics.as_slice()[0].primary().start < diagnostics.as_slice()[0].primary().end
        );
    }
}

#[test]
fn presence_dimension_count_is_hard_bounded_before_enumeration() {
    let catalog = catalog(CONTRACT);
    let parameter_count = MAX_OPERATIONAL_PRESENCE_PARAMETERS + 1;
    let parameters = (0..parameter_count)
        .map(|index| format!("$status_{index}: Ticket.status?"))
        .collect::<Vec<_>>()
        .join(",\n    ");
    let guards = (0..parameter_count)
        .map(|index| format!("&& when $status_{index} {{ status == $status_{index} }}"))
        .collect::<Vec<_>>()
        .join("\n          ");
    let source = format!(
        r#"query TooMany(
    $organization_id: Ticket.organization_id,
    {parameters}
) {{
    many tickets from Ticket
        where organization_id == $organization_id
          {guards}
        order by updated_at asc, ticket_id asc
        take 25
    return Found {{ tickets: tickets {{ ticket_id }} }}
    outcomes Found
}}"#
    );
    let document = parse_query(&source).expect("syntax");
    let diagnostics =
        compile_operational_query_family(&document, &catalog).expect_err("too many members");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        PlannerDiagnosticCode::Unbounded
    );
}
