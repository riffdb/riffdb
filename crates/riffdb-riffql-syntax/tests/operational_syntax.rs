//! Operational-predicate grammar and canonical-format acceptance.

use riffdb_riffql_syntax::{
    BinaryOperator, Expression, RIFFQL_LANGUAGE_VERSION, RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1,
    UnaryOperator, format_query, parse_query,
};

const OPERATIONAL_QUERY: &str = r#"
query SearchTickets(
    $organization_id: Ticket.organization_id,
    $status: Ticket.status?,
    $title_prefix: String?
) {
    many tickets from Ticket
        where organization_id == $organization_id
          && when $status { status == $status }
          && when $title_prefix { title prefix $title_prefix }
          && exists assignee_id
          && deleted_at is null
        order by updated_at desc, ticket_id desc
        take 50

    return Found {
        tickets: tickets {
            ticket_id
            title
        }
    }

    outcomes Found
}
"#;

#[test]
fn operational_predicates_parse_and_format_canonically() {
    let parsed = parse_query(OPERATIONAL_QUERY).expect("operational query parses");
    assert_eq!(
        parsed.language_version,
        RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1
    );
    let canonical = format_query(&parsed);
    let reparsed = parse_query(&canonical).expect("canonical operational query parses");
    assert_eq!(format_query(&reparsed), canonical);
    assert!(canonical.contains("when $status { status == $status }"));
    assert!(canonical.contains("title prefix $title_prefix"));
    assert!(canonical.contains("exists assignee_id"));
    assert!(canonical.contains("deleted_at is null"));

    let mut guards = 0;
    let mut prefixes = 0;
    let mut existence = 0;
    let mut nulls = 0;
    visit(
        &parsed.body.bindings[0].predicate.value,
        &mut |expression| match expression {
            Expression::PresenceGuard { .. } => guards += 1,
            Expression::Binary { operator, .. } if operator.value == BinaryOperator::Prefix => {
                prefixes += 1;
            }
            Expression::Unary { operator, .. } if operator.value == UnaryOperator::Exists => {
                existence += 1;
            }
            Expression::Unary { operator, .. } if operator.value == UnaryOperator::IsNull => {
                nulls += 1;
            }
            _ => {}
        },
    );
    assert_eq!((guards, prefixes, existence, nulls), (2, 1, 1, 1));
}

#[test]
fn legacy_query_bytes_retain_the_v1_language_identity() {
    let parsed = parse_query(
        r#"query One($id: Ticket.ticket_id) {
    one ticket from Ticket where ticket_id == $id else NotFound
    return Found { ticket: ticket { ticket_id } }
    outcomes Found | NotFound
}"#,
    )
    .expect("legacy syntax");
    assert_eq!(parsed.language_version, RIFFQL_LANGUAGE_VERSION);
}

#[test]
fn malformed_presence_guard_reports_the_guard_span() {
    let source = r#"query Broken($org: uuid, $status: Ticket.status?) {
    many tickets from Ticket
        where organization_id == $org && when $status status == $status
        order by ticket_id asc
        take 10
    return Found { tickets: tickets { ticket_id } }
    outcomes Found
}"#;
    let diagnostics = parse_query(source).expect_err("guard braces are mandatory");
    let diagnostic = &diagnostics.as_slice()[0];
    let status = source.find("status ==").expect("predicate spelling");
    assert!(diagnostic.span().start as usize <= status);
    assert_eq!(diagnostic.summary(), "unexpected RiffQL token");
}

fn visit(expression: &Expression, visitor: &mut impl FnMut(&Expression)) {
    visitor(expression);
    match expression {
        Expression::PresenceGuard { predicate, .. } => visit(&predicate.value, visitor),
        Expression::Unary { operand, .. } => visit(&operand.value, visitor),
        Expression::Binary { left, right, .. } => {
            visit(&left.value, visitor);
            visit(&right.value, visitor);
        }
        Expression::Parameter(_) | Expression::Path(_) | Expression::Literal(_) => {}
    }
}
