//! Current-row event policy-anchor grammar and source-span tests.

use riffdb_contract_syntax::ast::Declaration;
use riffdb_contract_syntax::parse_contract;

#[test]
fn parses_one_current_row_anchor_with_exact_field_mappings() {
    let source = concat!(
        "contract TicketDesk version 1 { ",
        "event TicketCreated { ",
        "partition_by (organization_id) ",
        "policy_anchor current Ticket(",
        "organization_id: organization_id, ticket_id: ticket_id) ",
        "organization_id: uuid ticket_id: uuid ",
        "} }",
    );
    let document = parse_contract(source).expect("anchored event parses");
    let Declaration::Event(event) = &document.contract.value.declarations[0].value else {
        panic!("first declaration must be an event");
    };
    let anchor = event.policy_anchor.as_ref().expect("policy anchor");
    assert_eq!(anchor.value.entity.value, "Ticket");
    assert_eq!(anchor.value.fields.len(), 2);
    assert_eq!(
        anchor
            .value
            .fields
            .iter()
            .map(|field| (
                field.value.entity_field.value.as_str(),
                field.value.payload_field.value.as_str(),
            ))
            .collect::<Vec<_>>(),
        [
            ("organization_id", "organization_id"),
            ("ticket_id", "ticket_id"),
        ]
    );
    assert_eq!(
        &source[anchor.span.start() as usize..anchor.span.end() as usize],
        concat!(
            "policy_anchor current Ticket(",
            "organization_id: organization_id, ticket_id: ticket_id)",
        )
    );
}

#[test]
fn legacy_events_remain_unanchored() {
    let document = parse_contract("contract C version 1 { event Changed { id: uuid } }")
        .expect("legacy event parses");
    let Declaration::Event(event) = &document.contract.value.declarations[0].value else {
        panic!("event declaration");
    };
    assert!(event.policy_anchor.is_none());
}

#[test]
fn empty_duplicate_or_late_policy_anchors_fail_syntax() {
    for source in [
        concat!(
            "contract C version 1 { event E { ",
            "policy_anchor current Row() id: uuid } }",
        ),
        concat!(
            "contract C version 1 { event E { ",
            "policy_anchor current Row(id: id) ",
            "policy_anchor current Row(id: id) id: uuid } }",
        ),
        concat!(
            "contract C version 1 { event E { id: uuid ",
            "policy_anchor current Row(id: id) } }",
        ),
    ] {
        parse_contract(source).expect_err("invalid event policy-anchor shape must fail");
    }
}
