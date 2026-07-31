//! Application-stream event partition grammar and source-span tests.

use riffdb_contract_syntax::ast::Declaration;
use riffdb_contract_syntax::parse_contract;

#[test]
fn parses_one_ordered_event_partition_tuple_with_exact_spans() {
    let source = concat!(
        "contract TicketDesk version 1 { ",
        "event TicketCreated { ",
        "partition_by (organization_id, project_id) ",
        "organization_id: uuid project_id: uuid ticket_id: uuid ",
        "} }",
    );
    let document = parse_contract(source).expect("partitioned event parses");
    let Declaration::Event(event) = &document.contract.value.declarations[0].value else {
        panic!("first declaration must be an event");
    };
    let partition = event.partition_by.as_ref().expect("partition declaration");
    assert_eq!(
        partition
            .value
            .iter()
            .map(|field| field.value.as_str())
            .collect::<Vec<_>>(),
        ["organization_id", "project_id"]
    );
    assert_eq!(
        &source[partition.span.start() as usize..partition.span.end() as usize],
        "partition_by (organization_id, project_id)"
    );
    assert_eq!(event.fields.len(), 3);
}

#[test]
fn existing_events_remain_unpartitioned_and_byte_compatible_at_the_ast_boundary() {
    let document = parse_contract("contract C version 1 { event Changed { id: uuid } }")
        .expect("legacy event parses");
    let Declaration::Event(event) = &document.contract.value.declarations[0].value else {
        panic!("event declaration");
    };
    assert!(event.partition_by.is_none());
    assert_eq!(event.fields.len(), 1);
}

#[test]
fn empty_duplicate_or_late_partition_clauses_fail_syntax() {
    for source in [
        "contract C version 1 { event E { partition_by () id: uuid } }",
        concat!(
            "contract C version 1 { event E { ",
            "partition_by (id) partition_by (id) id: uuid } }",
        ),
        "contract C version 1 { event E { id: uuid partition_by (id) } }",
    ] {
        parse_contract(source).expect_err("invalid event partition shape must fail");
    }
}
