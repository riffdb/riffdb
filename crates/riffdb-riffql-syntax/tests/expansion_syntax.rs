//! ADR-0185 bounded one-to-many expansion syntax.

use riffdb_riffql_syntax::{
    DiagnosticCode, RIFFQL_LANGUAGE_VERSION_RELATIONAL_OPERATORS_V1, format_query, parse_query,
};

const QUERY: &str = r#"query TicketPage($tenant: TenantId) {
    many tickets from Ticket
        where tenant_id == $tenant
        order by ticket_id asc
        take 25

    many comments from Comment
        for each ticket in tickets
        where tenant_id == $tenant && ticket_id == ticket.ticket_id
        order by ticket_id asc, comment_id asc
        take 8 per ticket

    return Found {
        tickets: tickets {
            ticket_id
            comments: comments { comment_id }
        }
    }
    outcomes Found
}
"#;

// req: RQL-008, OQ-116
#[test]
fn bounded_expansion_is_canonical_and_selects_v14() {
    let document = parse_query(QUERY).expect("bounded expansion parses");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_RELATIONAL_OPERATORS_V1
    );
    let expansion = document.body.bindings[1]
        .expansion
        .as_ref()
        .expect("expansion driver");
    assert_eq!(expansion.item.value.as_str(), "ticket");
    assert_eq!(expansion.binding.value.as_str(), "tickets");
    assert_eq!(
        document.body.bindings[1]
            .take
            .as_ref()
            .and_then(|take| take.per.as_ref())
            .map(|per| per.value.as_str()),
        Some("ticket")
    );

    let canonical = format_query(&document);
    assert_eq!(
        format_query(&parse_query(&canonical).expect("canonical query reparses")),
        canonical
    );
}

// req: RQL-008
#[test]
fn expansion_requires_matching_per_driver_and_forbids_cursor_paging() {
    let missing_per = QUERY.replace("take 8 per ticket", "take 8");
    let diagnostics = parse_query(&missing_per).expect_err("missing per-driver bound must fail");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        DiagnosticCode::UnsupportedForm
    );
    assert!(diagnostics.as_slice()[0].span().start < diagnostics.as_slice()[0].span().end);

    let wrong_per = QUERY.replace("take 8 per ticket", "take 8 per other");
    let diagnostics = parse_query(&wrong_per).expect_err("wrong driver must fail");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        DiagnosticCode::UnsupportedForm
    );

    let cursor = QUERY.replace("take 8 per ticket", "take 8 per ticket after $after");
    let diagnostics = parse_query(&cursor).expect_err("per-driver cursor must fail");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        DiagnosticCode::UnsupportedForm
    );
}

// req: OQ-116
#[test]
fn legacy_query_keeps_its_predecessor_language_identity() {
    let legacy = QUERY
        .replace("        for each ticket in tickets\n", "")
        .replace("take 8 per ticket", "take 8")
        .replace("ticket.ticket_id", "tickets.ticket_id");
    let document = parse_query(&legacy).expect("legacy query parses");
    assert_ne!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_RELATIONAL_OPERATORS_V1
    );
}
