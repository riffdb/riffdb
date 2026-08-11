#![forbid(unsafe_code)]

//! Bounded property and malformed-input tests for canonical values.

use std::collections::BTreeMap;

use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use riffdb_types::{
    CanonicalCodecError, CanonicalRecord, CanonicalValue, CanonicalVector, CurrencyCode, Date,
    Decimal, DecimalSpec, EnumTypeId, EnumVariantId, FieldId, MAX_LIST_ENTRIES, MAX_NESTING_DEPTH,
    MAX_STRING_BYTES, Money, Timestamp, decode_canonical_value, encode_canonical_value,
};

/// Finite components across magnitudes, including exact zero and subnormals.
/// Construction canonicalizes -0.0, so the generated vector is always in
/// canonical form.
fn finite_component() -> BoxedStrategy<f32> {
    prop_oneof![
        Just(0.0_f32),
        Just(-0.0_f32),
        // Subnormals (positive and negative): denormals are accepted as
        // canonical, so the trait-square property must cover them.
        Just(f32::from_bits(1)),
        any::<u32>().prop_map(|bits| f32::from_bits(bits % 0x0080_0000)),
        any::<u32>().prop_map(|bits| -f32::from_bits(bits % 0x0080_0000)),
        -1.0e30_f32..1.0e30_f32,
        -1.0_f32..1.0_f32,
    ]
    .boxed()
}

fn canonical_vector() -> BoxedStrategy<CanonicalVector> {
    vec(finite_component(), 1..16)
        .prop_map(|components| CanonicalVector::new(components).expect("finite components"))
        .boxed()
}

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
        (1u32..=u32::MAX, 1u32..=u32::MAX).prop_map(|(type_id, variant_id)| CanonicalValue::Enum {
            type_id: EnumTypeId::new(type_id).expect("generated nonzero enum type ID"),
            variant_id: EnumVariantId::new(variant_id).expect("generated nonzero enum variant ID"),
        }),
        canonical_vector().prop_map(CanonicalValue::Vector),
    ]
    .boxed()
}

fn canonical_value() -> BoxedStrategy<CanonicalValue> {
    let leaf = leaf_value();
    prop_oneof![
        leaf.clone(),
        vec(leaf.clone(), 0..8).prop_map(|values| CanonicalValue::list(values).expect("bounded")),
        btree_map(1u32..100, leaf, 0..8).prop_map(|fields| {
            CanonicalValue::Record(
                CanonicalRecord::new(
                    fields
                        .into_iter()
                        .map(|(id, value)| {
                            (FieldId::new(id).expect("generated nonzero field ID"), value)
                        })
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
    fn record_constructor_is_permutation_independent(fields in btree_map(1u32..100, leaf_value(), 0..12)) {
        let forward = fields
            .iter()
            .map(|(id, value)| (FieldId::new(*id).expect("generated nonzero field ID"), value.clone()))
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

    /// Canonical-form invariants (ADR-0011 vector amendment): within the
    /// canonical domain, equality, ordering, hashing, and durable bytes must
    /// all agree — `a == b` iff `cmp == Equal` iff equal hashes iff equal
    /// encoded documents.
    ///
    /// `right` is DERIVED from `left` half the time (an equal pair via clone,
    /// possibly re-canonicalized through zero-sign flips) rather than drawn
    /// independently: two independent draws are essentially never equal, so
    /// the equal-pair arms of the biconditionals — the positive direction of
    /// hash and byte agreement — previously never executed. Measured over the
    /// shipped strategy, the `if equal` branch ran on 0 of 256 cases.
    #[test]
    fn vector_equality_ordering_hash_and_digest_agree(
        left in canonical_vector(),
        independent in canonical_vector(),
        derive_equal in any::<bool>(),
        flip_zero_signs in any::<bool>(),
    ) {
        use std::hash::{Hash, Hasher};
        let hash_of = |vector: &CanonicalVector| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            vector.hash(&mut hasher);
            hasher.finish()
        };
        // Reflexivity always holds in the canonical domain.
        prop_assert_eq!(&left, &left.clone());
        prop_assert_eq!(left.cmp(&left.clone()), std::cmp::Ordering::Equal);

        let right = if derive_equal {
            // An equal pair, optionally rebuilt through the constructor with
            // every zero's sign flipped: canonicalization makes the results
            // equal AND bit-identical, so the positive hash/byte legs run.
            if flip_zero_signs {
                CanonicalVector::new(
                    left.components()
                        .iter()
                        .map(|component| if *component == 0.0 { -0.0 } else { *component })
                        .collect(),
                )
                .expect("canonical components stay canonical")
            } else {
                left.clone()
            }
        } else {
            independent
        };

        let equal = left == right;
        // The derived pair really is equal: the positive hash/byte legs
        // below run on roughly half of all cases, never on none.
        if derive_equal {
            prop_assert!(equal, "derived pair must compare equal");
        }
        prop_assert_eq!(equal, left.cmp(&right) == std::cmp::Ordering::Equal);
        if equal {
            prop_assert_eq!(hash_of(&left), hash_of(&right));
        }
        let left_bytes =
            encode_canonical_value(&CanonicalValue::Vector(left)).expect("bounded");
        let right_bytes =
            encode_canonical_value(&CanonicalValue::Vector(right)).expect("bounded");
        prop_assert_eq!(equal, left_bytes == right_bytes);
    }
}

/// The canonical-form predicate exists twice — the constructor canonicalizes
/// (`CanonicalVector::new`) and the strict decoder rejects (`codec.rs`) —
/// and nothing but this test forces the two to stay coupled. The invariant:
/// the decoder accepts a component bit pattern IF AND ONLY IF the
/// constructor preserves that exact bit pattern. If either side drifts
/// (constructor accepts a new pattern the decoder rejects, or the decoder
/// admits bytes the constructor would rewrite), decode/encode stops being an
/// isomorphism and this test reds.
#[test]
fn decoder_acceptance_coincides_with_constructor_bit_preservation() {
    let probes: [(u32, &str); 12] = [
        (0x7FC0_0000, "quiet NaN"),
        (0x7FA0_0000, "signaling NaN"),
        (0xFFC0_0000, "negative NaN"),
        (0x7F80_0000, "+infinity"),
        (0xFF80_0000, "-infinity"),
        (0x8000_0000, "-0.0"),
        (0x0000_0000, "+0.0"),
        (0x3F80_0000, "1.0"),
        (0x0000_0001, "min positive subnormal"),
        (0x807F_FFFF, "max negative subnormal"),
        (0x7F7F_FFFF, "f32::MAX"),
        (0xFF7F_FFFF, "f32::MIN"),
    ];
    for (bits, label) in probes {
        // Craft the exact durable bytes: version, vector tag 0x0e,
        // dimension 1, one big-endian component.
        let mut bytes = vec![0x01, 0x0e, 0, 0, 0, 1];
        bytes.extend_from_slice(&bits.to_be_bytes());
        let decoded = decode_canonical_value(&bytes);

        let constructor_preserves_bits = CanonicalVector::new(vec![f32::from_bits(bits)])
            .is_ok_and(|vector| vector.components()[0].to_bits() == bits);

        assert_eq!(
            decoded.is_ok(),
            constructor_preserves_bits,
            "{label}: decoder acceptance must coincide with the constructor \
             preserving the bit pattern (decoded = {decoded:?})"
        );
        if let Ok(CanonicalValue::Vector(vector)) = &decoded {
            assert_eq!(
                vector.components()[0].to_bits(),
                bits,
                "{label}: accepted bytes must decode bit-identically"
            );
        }
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
fn decoder_rejects_zero_assigned_ids() {
    assert_eq!(
        decode_canonical_value(&[0x01, 0x0b, 0, 0, 0, 0, 0, 0, 0, 1]),
        Err(CanonicalCodecError::ZeroEnumTypeId)
    );
    assert_eq!(
        decode_canonical_value(&[0x01, 0x0b, 0, 0, 0, 1, 0, 0, 0, 0]),
        Err(CanonicalCodecError::ZeroEnumVariantId)
    );
    assert_eq!(
        decode_canonical_value(&[0x01, 0x0d, 0, 0, 0, 1, 0, 0, 0, 0, 0x01, 0x00]),
        Err(CanonicalCodecError::ZeroFieldId)
    );
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
        (FieldId::new(7).expect("nonzero"), CanonicalValue::Null),
        (
            FieldId::new(7).expect("nonzero"),
            CanonicalValue::Bool(true),
        ),
    ];
    assert!(CanonicalValue::record(duplicate_fields).is_err());
}

#[test]
fn btree_map_strategy_documents_stable_order() {
    let map = BTreeMap::from([(9, CanonicalValue::Null), (2, CanonicalValue::Bool(true))]);
    let record = CanonicalRecord::new(
        map.into_iter()
            .map(|(id, value)| (FieldId::new(id).expect("nonzero"), value))
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
