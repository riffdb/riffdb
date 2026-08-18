//! Frozen binary UTF-8 exact-text truth table.

use riffdb_types::{
    ExactTextBindErrorV1, ExactTextFieldValueV1, ExactTextOperatorV1, ExactTextProfileV1,
    MAX_EXACT_TEXT_NEEDLE_BYTES_V1, MAX_EXACT_TEXT_VALUE_BYTES_V1,
};

const _: () = assert!(MAX_EXACT_TEXT_VALUE_BYTES_V1 > MAX_EXACT_TEXT_NEEDLE_BYTES_V1);

#[test]
fn binary_utf8_v1_truth_table_is_exact_and_null_safe() {
    let profile = ExactTextProfileV1::BinaryUtf8V1;
    let cases = [
        (ExactTextOperatorV1::Equals, "café", "café", true),
        (ExactTextOperatorV1::Equals, "café", "CAFÉ", false),
        (ExactTextOperatorV1::StartsWith, "éclair", "é", true),
        (ExactTextOperatorV1::StartsWith, "éclair", "clair", false),
        (ExactTextOperatorV1::EndsWith, "東京駅", "駅", true),
        (ExactTextOperatorV1::Contains, "a🙂b", "🙂", true),
        (ExactTextOperatorV1::Contains, "Straße", "strasse", false),
        (ExactTextOperatorV1::Contains, "wild*card", "*", true),
    ];
    for (operator, value, needle, expected) in cases {
        let needle = profile.bind_needle(needle).unwrap();
        assert_eq!(
            profile.matches(operator, ExactTextFieldValueV1::Value(value), &needle),
            expected
        );
    }
    let needle = profile.bind_needle("x").unwrap();
    assert!(!profile.matches(
        ExactTextOperatorV1::Contains,
        ExactTextFieldValueV1::Missing,
        &needle
    ));
    assert!(!profile.matches(
        ExactTextOperatorV1::Contains,
        ExactTextFieldValueV1::Null,
        &needle
    ));
}

#[test]
fn indexed_value_maximum_is_explicit_and_independent_from_needle_maximum() {
    assert_eq!(MAX_EXACT_TEXT_VALUE_BYTES_V1, 256);
}

#[test]
fn binding_is_canonical_bounded_and_rejects_empty_needles() {
    let profile = ExactTextProfileV1::BinaryUtf8V1;
    assert_eq!(
        profile.bind_needle(""),
        Err(ExactTextBindErrorV1::EmptyNeedle)
    );
    let maximum = "a".repeat(MAX_EXACT_TEXT_NEEDLE_BYTES_V1);
    let bound = profile.bind_needle(&maximum).unwrap();
    assert_eq!(&bound.to_canonical_bytes()[..4], b"RXTN");
    assert_eq!(
        profile.bind_needle(&(maximum + "a")),
        Err(ExactTextBindErrorV1::NeedleTooLong)
    );
}
