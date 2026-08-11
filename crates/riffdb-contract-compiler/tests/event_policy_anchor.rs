//! Fail-closed compiler gate for current-row event policy anchors.

use riffdb_contract_compiler::{CompilerDiagnosticCode, validate_contract_source};

#[test]
fn parsed_anchor_is_rejected_until_the_checked_ir_successor_is_present() {
    let source = r#"contract EventPolicyAnchor version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
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
}
"#;
    let error = validate_contract_source(source).expect_err("anchor must not be discarded");
    let diagnostic = error
        .semantic()
        .expect("semantic diagnostic")
        .as_slice()
        .iter()
        .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidEvent)
        .expect("fail-closed anchor diagnostic");
    let selected = &source
        [diagnostic.primary_span().start() as usize..diagnostic.primary_span().end() as usize];
    assert!(selected.starts_with("policy_anchor current Ticket("));
    assert!(selected.ends_with(')'));
}
