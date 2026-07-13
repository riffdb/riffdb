#![forbid(unsafe_code)]

//! Bounded property and malformed-input tests for canonical values.

use std::collections::BTreeMap;

use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use riffdb_types::{
    CanonicalCodecError, CanonicalRecord, CanonicalValue, CurrencyCode, Date, Decimal, DecimalSpec,
    EnumTypeId, EnumVariantId, FieldId, MAX_LIST_ENTRIES, MAX_NESTING_DEPTH, MAX_STRING_BYTES,
    Money, Timestamp, decode_canonical_value, encode_canonical_value,
};

fn decimal(precision: u8, scale: u8, coefficient: i128) -> Decimal {
    Decimal::new(
        DecimalSpec::new(precision, scale).expect("valid decimal specification"),
        coefficient,
    )
    .expect("coefficient fits precision")
}

fn leaf_value() -> BoxedStrategy<CanonicalValue> {
    prop_oneof![
        Just(CanonicalValue::Null),
        any::<bool>().prop_map(CanonicalValue::Bool),
        any::<i64>().prop_map(CanonicalValue::I64),
        any::<u64>().prop_map(CanonicalValue::U64),
        any::<i64>().prop_map(|value| CanonicalValue::Decimal(decimal(19, 2, value as i128))),
        any::<i64>().prop_map(|value| CanonicalValue::Money(Money::new(
            CurrencyCode::new("USD").expect("valid currency"),
            decimal(19, 2, value as i128),
        ))),
        vec(any::<char>(), 0..24).prop_map(|chars| {
            CanonicalValue::string(chars.into_iter().collect::<String>()).expect("bounded")
        }),
        vec(any::<u8>(), 0..64).prop_map(|bytes| CanonicalValue::bytes(bytes).expect("bounded")),
        (any::<i64>(), 0u32..1_000_000_000).prop_map(|(seconds, nanos)| {
            CanonicalValue::Timestamp(Timestamp::new(seconds, nanos).expect("bounded nanos"))
        }),
        any::<i32>().prop_map(|days| CanonicalValue::Date(Date::from_days_since_unix_epoch(days))),
        any::<[u8; 16]>().prop_map(CanonicalValue::Uuid),
        (any::<u32>(), any::<u32>()).prop_map(|(type_id, variant_id)| CanonicalValue::Enum {
            type_id: EnumTypeId::new(type_id),
            variant_id: EnumVariantId::new(variant_id),
        }),
    ]
    .boxed()
}

fn canonical_value() -> BoxedStrategy<CanonicalValue> {
    let leaf = leaf_value();
    prop_oneof![
        leaf.clone(),
        vec(leaf.clone(), 0..8).prop_map(|values| CanonicalValue::list(values).expect("bounded")),
        btree_map(0u32..100, leaf, 0..8).prop_map(|fields| {
            CanonicalValue::Record(
                CanonicalRecord::new(
                    fields
                        .into_iter()
                        .map(|(id, value)| (FieldId::new(id), value))
                        .collect(),
                )
                .expect("BTreeMap field IDs are unique"),
            )
        }),
    ]
    .boxed()
}

proptest! {
    #[test]
    fn canonical_encoding_round_trips(value in canonical_value()) {
        let bytes = encode_canonical_value(&value).expect("generated value is bounded");
        prop_assert_eq!(decode_canonical_value(&bytes), Ok(value));
    }

    #[test]
    fn canonical_reencoding_is_byte_identical(value in canonical_value()) {
        let first = encode_canonical_value(&value).expect("generated value is bounded");
        let decoded = decode_canonical_value(&first).expect("self encoding decodes");
        let second = encode_canonical_value(&decoded).expect("decoded value reencodes");
        prop_assert_eq!(first, second);
    }

    #[test]
    fn record_constructor_is_permutation_independent(fields in btree_map(0u32..100, leaf_value(), 0..12)) {
        let forward = fields
            .iter()
            .map(|(id, value)| (FieldId::new(*id), value.clone()))
            .collect::<Vec<_>>();
        let reverse = forward.iter().cloned().rev().collect::<Vec<_>>();
        let forward = CanonicalValue::record(forward).expect("unique fields");
        let reverse = CanonicalValue::record(reverse).expect("unique fields");
        prop_assert_eq!(
            encode_canonical_value(&forward),
            encode_canonical_value(&reverse)
        );
    }

    #[test]
    fn arbitrary_bounded_bytes_never_panic(input in vec(any::<u8>(), 0..2048)) {
        let _ = decode_canonical_value(&input);
    }
}

#[test]
fn decoder_rejects_noncanonical_and_malformed_documents() {
    let cases = [
        vec![],
        vec![0x02, 0x00],
        vec![0x01, 0xff],
        vec![0x01, 0x01, 0x02],
        vec![0x01, 0x06, 0, 0, 0, 1, 0xff],
        vec![0x01, 0x00, 0x00],
        // Duplicate record field ID 2.
        vec![
            0x01, 0x0d, 0, 0, 0, 2, 0, 0, 0, 2, 0x01, 0x00, 0, 0, 0, 2, 0x01, 0x00,
        ],
        // Descending record IDs 9 then 2.
        vec![
            0x01, 0x0d, 0, 0, 0, 2, 0, 0, 0, 9, 0x01, 0x00, 0, 0, 0, 2, 0x01, 0x00,
        ],
        // Invalid timestamp nanoseconds.
        vec![0x01, 0x08, 0, 0, 0, 0, 0, 0, 0, 0, 0x3b, 0x9a, 0xca, 0x00],
        // Lowercase currency.
        vec![
            0x01, 0x05, b'u', b's', b'd', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
    ];

    for case in cases {
        assert!(decode_canonical_value(&case).is_err(), "accepted {case:?}");
    }
}

#[test]
fn decoder_checks_declared_limits_before_allocating_or_reading_payloads() {
    let string_too_long = [
        &[0x01, 0x06][..],
        &((MAX_STRING_BYTES as u32) + 1).to_be_bytes(),
    ]
    .concat();
    assert_eq!(
        decode_canonical_value(&string_too_long),
        Err(CanonicalCodecError::StringTooLarge {
            actual: MAX_STRING_BYTES + 1,
            maximum: MAX_STRING_BYTES,
        })
    );

    let list_too_long = [
        &[0x01, 0x0c][..],
        &((MAX_LIST_ENTRIES as u32) + 1).to_be_bytes(),
    ]
    .concat();
    assert!(matches!(
        decode_canonical_value(&list_too_long),
        Err(CanonicalCodecError::TooManyEntries { .. })
    ));
}

#[test]
fn nesting_limit_is_enforced_on_encode_and_decode() {
    let mut allowed = CanonicalValue::Null;
    for _ in 0..MAX_NESTING_DEPTH {
        allowed = CanonicalValue::list(vec![allowed]).expect("one entry");
    }
    let encoded = encode_canonical_value(&allowed).expect("depth 32 is accepted");
    assert_eq!(decode_canonical_value(&encoded), Ok(allowed));

    let mut at_limit = CanonicalValue::Null;
    for _ in 0..MAX_NESTING_DEPTH {
        at_limit = CanonicalValue::list(vec![at_limit]).expect("within depth limit");
    }
    assert!(CanonicalValue::list(vec![at_limit]).is_err());

    // A decoder must independently enforce the limit on untrusted bytes.
    let mut too_deep = Vec::new();
    for _ in 0..=MAX_NESTING_DEPTH {
        too_deep.extend_from_slice(&[0x01, 0x0c, 0, 0, 0, 1]);
    }
    too_deep.extend_from_slice(&[0x01, 0x00]);
    assert!(matches!(
        decode_canonical_value(&too_deep),
        Err(CanonicalCodecError::NestingTooDeep { .. })
    ));
}

#[test]
fn record_constructor_rejects_duplicate_fields() {
    let duplicate_fields = vec![
        (FieldId::new(7), CanonicalValue::Null),
        (FieldId::new(7), CanonicalValue::Bool(true)),
    ];
    assert!(CanonicalValue::record(duplicate_fields).is_err());
}

#[test]
fn btree_map_strategy_documents_stable_order() {
    let map = BTreeMap::from([(9, CanonicalValue::Null), (2, CanonicalValue::Bool(true))]);
    let record = CanonicalRecord::new(
        map.into_iter()
            .map(|(id, value)| (FieldId::new(id), value))
            .collect(),
    )
    .expect("unique fields");
    assert_eq!(
        record
            .fields()
            .iter()
            .map(|(id, _)| id.get())
            .collect::<Vec<_>>(),
        [2, 9]
    );
}
