//! Cross-language wire-vector fixture validation.

use std::collections::BTreeMap;

use riffdb_proto::{
    decode_execute_request, decode_execute_response, decode_public_error, decode_value,
};

const VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/wire-vectors.txt"
));

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0, "hex fixture length");
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("ASCII hex");
            u8::from_str_radix(text, 16).expect("valid hex")
        })
        .collect()
}

#[test]
fn every_checked_in_wire_vector_decodes_through_its_strict_boundary() {
    let mut lines = VECTORS.lines();
    assert_eq!(lines.next(), Some("riffdb-proto-wire-vectors-v1"));
    let vectors = lines
        .map(|line| {
            let (name, encoded) = line.split_once(' ').expect("name and hex bytes");
            (name, decode_hex(encoded))
        })
        .collect::<BTreeMap<_, _>>();

    let expected = [
        "error.authorization",
        "error.concurrency",
        "error.contract",
        "error.execution-arithmetic",
        "error.execution-resource-limit",
        "error.execution-unique-conflict",
        "error.idempotency",
        "error.internal",
        "error.outcome-unknown",
        "error.storage",
        "error.validation",
        "execute.request-active",
        "execute.request-versioned",
        "execute.response-committed",
        "execute.response-read-only",
        "execute.response-replayed",
        "value.bool-false",
        "value.bytes",
        "value.date",
        "value.decimal",
        "value.enum",
        "value.i64-minus-one",
        "value.list",
        "value.money",
        "value.null",
        "value.record",
        "value.string",
        "value.timestamp",
        "value.u64-max",
        "value.uuid",
    ];
    assert_eq!(vectors.keys().copied().collect::<Vec<_>>(), expected);

    for (name, encoded) in vectors {
        if name.starts_with("value.") {
            decode_value(&encoded).expect("valid Value vector");
        } else if name.starts_with("error.") {
            decode_public_error(&encoded).expect("valid public error vector");
        } else if name.starts_with("execute.request-") {
            decode_execute_request(&encoded).expect("valid Execute request vector");
        } else if name.starts_with("execute.response-") {
            decode_execute_response(&encoded).expect("valid Execute response vector");
        } else {
            panic!("unknown vector class: {name}");
        }
    }
}
