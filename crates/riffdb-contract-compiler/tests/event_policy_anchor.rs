//! Compiler gate for current-row event policy anchors.

use riffdb_contract_compiler::{CompilerDiagnosticCode, validate_contract_source};

fn anchored_source(anchor: &str, event_fields: &str, policies: &str) -> String {
    format!(
        r#"contract EventPolicyAnchor version 1 {{
  entity Ticket {{
    key (organization_id: uuid, ticket_id: uuid)
    field owner_id: uuid
  }}
  event TicketCreated {{
    partition_by (organization_id)
    {anchor}
    {event_fields}
  }}
  aggregate Tickets {{
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }}
  {policies}
}}
"#
    )
}

fn semantic_diagnostics(source: &str) -> Vec<(CompilerDiagnosticCode, String)> {
    let error = validate_contract_source(source)
        .expect_err("anchored event is not deployable before the IR rotation");
    error
        .semantic()
        .unwrap_or_else(|| panic!("expected semantic diagnostics, got {error:?}"))
        .as_slice()
        .iter()
        .map(|diagnostic| {
            let span = diagnostic.primary_span();
            (
                diagnostic.code(),
                source[span.start() as usize..span.end() as usize].to_owned(),
            )
        })
        .collect()
}

#[test]
fn complete_current_row_anchor_is_retained_in_checked_ir() {
    let source = r#"contract EventPolicyAnchor version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field owner_id: uuid
  }
  event TicketCreated {
    partition_by (organization_id)
    policy_anchor current Ticket(
      organization_id: organization_id,
      ticket_id: ticket_id,
    )
    organization_id: uuid
    ticket_id: uuid
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
  row policy TicketAccess on Ticket {
    allow read when true
  }
  command TouchTicket {
    input request_id: string<128>
    input organization_id: uuid
    input ticket_id: uuid
    idempotency_key request_id
    mutate Ticket(organization_id, ticket_id) as ticket else Missing {}
    set ticket.owner_id = organization_id
    emit TicketCreated {
      organization_id: organization_id,
      ticket_id: ticket_id,
    }
    return Touched { ticket: ticket }
  }
}
"#;
    let bundle = riffdb_contract_compiler::compile_contract_source(source)
        .expect("a complete current-row anchor compiles");
    let event = bundle
        .schema()
        .events()
        .iter()
        .find(|event| event.name() == "TicketCreated")
        .expect("event schema");
    let anchor = event.policy_anchor().expect("checked policy anchor");
    assert_eq!(anchor.source_entity().get(), 1);
    assert_eq!(anchor.read_policy(), "TicketAccess");
    assert_eq!(anchor.key_fields().len(), 2);
}

#[test]
fn anchor_requires_the_complete_ordered_non_optional_primary_key() {
    let policy = r#"row policy TicketAccess on Ticket {
    allow read when true
  }"#;
    for (anchor, event_fields, selected) in [
        (
            "policy_anchor current Ticket(organization_id: organization_id)",
            "organization_id: uuid\n    ticket_id: uuid",
            "policy_anchor current Ticket(organization_id: organization_id)",
        ),
        (
            "policy_anchor current Ticket(ticket_id: ticket_id, organization_id: organization_id)",
            "organization_id: uuid\n    ticket_id: uuid",
            "policy_anchor current Ticket(ticket_id: ticket_id, organization_id: organization_id)",
        ),
        (
            "policy_anchor current Ticket(organization_id: organization_id, ticket_id: ticket_id)",
            "organization_id: uuid\n    ticket_id: optional<uuid>",
            "ticket_id",
        ),
        (
            "policy_anchor current Ticket(organization_id: organization_id, owner_id: owner_id)",
            "organization_id: uuid\n    owner_id: uuid",
            "policy_anchor current Ticket(organization_id: organization_id, owner_id: owner_id)",
        ),
    ] {
        let source = anchored_source(anchor, event_fields, policy);
        assert!(
            semantic_diagnostics(&source)
                .iter()
                .any(|(code, span)| *code == CompilerDiagnosticCode::InvalidEvent
                    && span == selected),
            "missing expected diagnostic for {anchor}"
        );
    }
}

#[test]
fn anchor_requires_the_event_partition_prefix_and_one_read_policy() {
    let complete =
        "policy_anchor current Ticket(organization_id: organization_id, ticket_id: ticket_id)";
    let fields = "organization_id: uuid\n    ticket_id: uuid";
    for policies in [
        "",
        r#"row policy TicketAccess on Ticket {
    allow update when true
  }"#,
        r#"row policy TicketAccess on Ticket { allow read when true }
  row policy TicketAuditAccess on Ticket { allow read when true }"#,
    ] {
        let source = anchored_source(complete, fields, policies);
        assert!(semantic_diagnostics(&source).iter().any(|(code, span)| {
            *code == CompilerDiagnosticCode::InvalidEvent && span == "Ticket"
        }));
    }

    let source = anchored_source(
        "policy_anchor current Ticket(organization_id: ticket_id, ticket_id: organization_id)",
        fields,
        "row policy TicketAccess on Ticket { allow read when true }",
    );
    assert!(semantic_diagnostics(&source).iter().any(|(code, span)| {
        *code == CompilerDiagnosticCode::InvalidEvent && span.starts_with("policy_anchor")
    }));
}
