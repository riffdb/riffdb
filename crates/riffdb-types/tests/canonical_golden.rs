#![forbid(unsafe_code)]

//! Golden compatibility vectors for canonical value encoding v1.

use riffdb_types::{
    CanonicalValue, CanonicalVector, CurrencyCode, Date, Decimal, DecimalSpec, EnumTypeId,
    EnumVariantId, FieldId, Money, Timestamp, decode_canonical_value, encode_canonical_value,
};

/// Maps a value to its registered canonical tag through an exhaustive match.
///
/// Adding a `CanonicalValue` variant fails compilation here until the new
/// tag is mapped, and the golden test asserts every mapped tag has a pinned
/// row — previously a new durable tag (`0x0e`) landed with no fixture and
/// nothing redded.
fn registered_tag(value: &CanonicalValue) -> u8 {
    match value {
        CanonicalValue::Null => 0x00,
        CanonicalValue::Bool(_) => 0x01,
        CanonicalValue::I64(_) => 0x02,
        CanonicalValue::U64(_) => 0x03,
        CanonicalValue::Decimal(_) => 0x04,
        CanonicalValue::Money(_) => 0x05,
        CanonicalValue::String(_) => 0x06,
        CanonicalValue::Bytes(_) => 0x07,
        CanonicalValue::Timestamp(_) => 0x08,
        CanonicalValue::Date(_) => 0x09,
        CanonicalValue::Uuid(_) => 0x0a,
        CanonicalValue::Enum { .. } => 0x0b,
        CanonicalValue::List(_) => 0x0c,
        CanonicalValue::Record(_) => 0x0d,
        CanonicalValue::Vector(_) => 0x0e,
    }
}

/// Highest tag in canonical encoding v1 as amended (ADR-0011: `0x00..=0x0e`).
const MAX_REGISTERED_TAG: u8 = 0x0e;

fn decimal(precision: u8, scale: u8, coefficient: i128) -> Decimal {
    Decimal::new(
        DecimalSpec::new(precision, scale).expect("valid decimal specification"),
        coefficient,
    )
    .expect("coefficient fits precision")
}

fn hex(input: &str) -> Vec<u8> {
    assert_eq!(input.len() % 2, 0);
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("ASCII hex");
            u8::from_str_radix(pair, 16).expect("valid hex")
        })
        .collect()
}

#[test]
fn canonical_v1_tag_and_payload_vectors_are_stable() {
    let record = CanonicalValue::record(vec![
        (FieldId::new(9).expect("nonzero"), CanonicalValue::Null),
        (FieldId::new(2).expect("nonzero"), CanonicalValue::U64(1)),
    ])
    .expect("valid record");
    let values = [
        (CanonicalValue::Null, "0100"),
        (CanonicalValue::Bool(false), "010100"),
        (CanonicalValue::Bool(true), "010101"),
        (CanonicalValue::I64(-1), "0102ffffffffffffffff"),
        (CanonicalValue::U64(u64::MAX), "0103ffffffffffffffff"),
        (
            CanonicalValue::Decimal(decimal(5, 2, -1234)),
            "01040502fffffffffffffffffffffffffffffb2e",
        ),
        (
            CanonicalValue::Money(Money::new(
                CurrencyCode::new("USD").expect("valid currency"),
                decimal(5, 2, -1234),
            )),
            "01055553440502fffffffffffffffffffffffffffffb2e",
        ),
        (
            CanonicalValue::string("é").expect("bounded"),
            "010600000002c3a9",
        ),
        (
            CanonicalValue::bytes(vec![0, 255]).expect("bounded"),
            "01070000000200ff",
        ),
        (
            CanonicalValue::Timestamp(Timestamp::new(-1, 999_999_999).expect("valid timestamp")),
            "0108ffffffffffffffff3b9ac9ff",
        ),
        (
            CanonicalValue::Date(Date::from_days_since_unix_epoch(-1)),
            "0109ffffffff",
        ),
        (
            CanonicalValue::Uuid([
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ]),
            "010a000102030405060708090a0b0c0d0e0f",
        ),
        (
            CanonicalValue::Enum {
                type_id: EnumTypeId::new(42).expect("nonzero"),
                variant_id: EnumVariantId::new(7).expect("nonzero"),
            },
            "010b0000002a00000007",
        ),
        (
            CanonicalValue::list(vec![CanonicalValue::Null, CanonicalValue::Bool(true)])
                .expect("bounded"),
            "010c000000020100010101",
        ),
        (
            record,
            "010d000000020000000201030000000000000001000000090100",
        ),
        (
            CanonicalValue::Vector(CanonicalVector::new(vec![1.0f32]).expect("finite")),
            "010e000000013f800000",
        ),
        (
            CanonicalValue::Vector(CanonicalVector::new(vec![0.0f32, -2.5, 3.25]).expect("finite")),
            "010e0000000300000000c020000040500000",
        ),
    ];

    let mut pinned_tags = std::collections::BTreeSet::new();
    for (value, expected_hex) in values {
        let expected = hex(expected_hex);
        assert_eq!(
            expected[1],
            registered_tag(&value),
            "golden row tag byte disagrees with the registry for {value:?}"
        );
        pinned_tags.insert(expected[1]);
        assert_eq!(encode_canonical_value(&value), Ok(expected.clone()));
        assert_eq!(decode_canonical_value(&expected), Ok(value));
    }
    let expected_tags: std::collections::BTreeSet<u8> = (0..=MAX_REGISTERED_TAG).collect();
    assert_eq!(
        pinned_tags, expected_tags,
        "every registered canonical tag must have a pinned golden row"
    );
}

#[test]
fn text_is_exact_utf8_without_implicit_normalization() {
    let composed = CanonicalValue::string("é").expect("bounded");
    let decomposed = CanonicalValue::string("e\u{301}").expect("bounded");
    assert_ne!(composed, decomposed);
    assert_ne!(
        encode_canonical_value(&composed).expect("encodes"),
        encode_canonical_value(&decomposed).expect("encodes")
    );
}
