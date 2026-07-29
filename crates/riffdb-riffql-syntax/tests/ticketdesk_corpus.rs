//! TicketDesk positive and negative RiffQL source corpus.

use riffdb_riffql_syntax::{DiagnosticCode, format_query, parse_query};

const CORPUS: &[(&str, &str)] = &[
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
fn complete_ticketdesk_corpus_is_canonical_parse_format_parse_stable() {
    for (name, source) in CORPUS {
        let parsed = parse_query(source).unwrap_or_else(|diagnostics| {
            panic!("{name} did not parse: {diagnostics:?}");
        });
        let canonical = format_query(&parsed);
        let reparsed = parse_query(&canonical).unwrap_or_else(|diagnostics| {
            panic!("{name} canonical output did not parse: {diagnostics:?}\n{canonical}");
        });
        assert_eq!(format_query(&reparsed), canonical, "{name}");
        assert!(
            !canonical
                .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                .any(|word| word.ends_with("_id") && word.bytes().all(|byte| byte.is_ascii_digit())),
            "{name} contains a caller-visible numeric storage ID"
        );
    }
}

#[test]
fn negative_corpus_rejects_mutation_recursion_unbounded_many_and_unindexed_shape() {
    let cases = [
        (
            "run CreateTicket { title: \"x\" }",
            DiagnosticCode::UnsupportedForm,
        ),
        (
            "query Recursive() { many rows from Ticket where recurse(rows) take 1 return { rows } }",
            DiagnosticCode::UnsupportedForm,
        ),
        (
            "query Unbounded($tenant: TenantId) { many rows from Ticket where tenant_id == $tenant return { rows } }",
            DiagnosticCode::UnboundedMany,
        ),
        (
            "query FullScan() { many rows from Ticket take 10 return { rows } }",
            DiagnosticCode::UnexpectedToken,
        ),
        ("SELECT * FROM Ticket", DiagnosticCode::UnsupportedForm),
    ];
    for (source, expected) in cases {
        let diagnostics = parse_query(source).expect_err("negative source must reject");
        assert_eq!(diagnostics.as_slice()[0].code(), expected, "{source}");
    }
}
