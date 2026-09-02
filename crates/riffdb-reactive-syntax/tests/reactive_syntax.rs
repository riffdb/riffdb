#![forbid(unsafe_code)]

//! Reactive grammar-v1 acceptance tests.

use riffdb_reactive_syntax::{DiagnosticCode, format_module, parse_module};

const SOURCE: &str = r#"
reactive TicketDeskReactive version 1 {
  stream TicketActivity($organization_id: UUID, $minimum_priority: I64) {
    partition (organization_id = $organization_id);
    event TicketCreated select (ticket_id, title, priority);
    event TicketReprioritized select (ticket_id, priority);
    where event.priority >= $minimum_priority;
  }
  watch OpenTickets($organization_id: UUID) query OpenTicketsPage updates patch;
  subscription TriageTicket($organization_id: UUID) {
    stream TicketActivity(organization_id = $organization_id, minimum_priority = 0);
    hydrate ticket query TicketById(organization_id = $organization_id, ticket_id = event.ticket_id);
    reaction assign command AssignTicket;
    limits {
      batch 4;
      in_flight 4;
      lease_seconds 60;
    }
  }
}
"#;

#[test]
fn canonical_format_is_parse_format_parse_stable() {
    let parsed = parse_module(SOURCE).expect("reactive source");
    let canonical = format_module(&parsed);
    let reparsed = parse_module(&canonical).expect("canonical reactive source");
    assert_eq!(format_module(&reparsed), canonical);
    assert_eq!(parsed.streams()[0].events().len(), 2);
    assert_eq!(parsed.subscriptions()[0].limits().lease_seconds(), 60);
}

#[test]
fn duplicate_operation_and_unsafe_limits_fail_with_spans() {
    let duplicate = SOURCE.replace("watch OpenTickets", "watch TicketActivity");
    let diagnostic = parse_module(&duplicate).expect_err("duplicate operation")[0];
    assert_eq!(diagnostic.code(), DiagnosticCode::DuplicateName);
    assert!(diagnostic.span().end() > diagnostic.span().start());

    let excessive = SOURCE.replace("batch 4", "batch 9");
    assert_eq!(
        parse_module(&excessive).expect_err("bounded batch")[0].code(),
        DiagnosticCode::LimitExceeded
    );
}

#[test]
fn unsupported_forms_do_not_fall_through_to_an_ambient_language() {
    let source = "reactive X version 1 { sql Anything() {} }";
    assert_eq!(
        parse_module(source).expect_err("no SQL escape")[0].code(),
        DiagnosticCode::UnsupportedForm
    );
}
