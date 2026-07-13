#![forbid(unsafe_code)]

//! Golden and property coverage for v1 digest frames and durable keys.

use proptest::prelude::*;
use riffdb_types::{
    AggregateTypeId, CONFLICT_KEY_V1_PREFIX, ConflictKeyBuilder, DIGEST_SCHEME_V1, DigestKey,
    DigestKeyId, ENTITY_KEY_V1_PREFIX, EntityKeyBuilder, EntityTypeId, HashDomain, KeyedHashDomain,
    MAX_KEY_BYTES, hash, hash_plan, keyed_hash,
};

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
fn hash_v1_domain_vectors_are_stable() {
    let vectors = [
        (
            HashDomain::CanonicalValue,
            "c562dc2f8188979bcf16db08d51a8b0aaf53a278486c00098688261553dc254b",
        ),
        (
            HashDomain::Source,
            "2001684d405dfa12725fc58a79156e382aee12f06230029eae3b51baa11df61f",
        ),
        (
            HashDomain::ContractBundle,
            "5446a1489d9c6e5d7f2a91314deeda8a4f33566001fc4732185d71473beb67da",
        ),
        (
            HashDomain::Plan,
            "38405dbcdcb4e467b6e9a92f26d7d8e2be8e457057c4bddce6930edefe3fb87c",
        ),
        (
            HashDomain::CommandInput,
            "bf0ca571e3c0022491aaf0c0c1357730ba2075c54309f883f6d53c490d927b3f",
        ),
        (
            HashDomain::Event,
            "a1e41c3c893298e590add962eed4dad1991364e6e551b810696564b669e7bc96",
        ),
        (
            HashDomain::EntityKey,
            "86e8af6a7bb7ed197032691bec9fbe68ef72dc18b50b323e3bcf99c038352069",
        ),
        (
            HashDomain::ConflictKey,
            "4758dc6f251a8e8032180e421c8c39db02c90a79a32b2006cd43e67446788c4a",
        ),
        (
            HashDomain::Schema,
            "4289634dbb8dd918bbd4f664b550c3db4b6b0ea8754ff2128c33b0b41e44e431",
        ),
    ];

    for (domain, expected) in vectors {
        let digest = hash(domain, b"abc");
        assert_eq!(digest.scheme(), DIGEST_SCHEME_V1);
        assert_eq!(digest.as_bytes().as_slice(), hex(expected));
    }
}

#[test]
fn typed_hash_helpers_preserve_semantic_output_types() {
    let typed: riffdb_types::PlanHash = hash_plan(b"abc");
    let generic = hash(HashDomain::Plan, b"abc");

    assert_eq!(typed.as_bytes(), generic.as_bytes());
    assert_eq!(generic.domain(), HashDomain::Plan);
}

#[test]
fn hmac_v1_idempotency_vector_and_key_metadata_are_stable() {
    let key = DigestKey::from_bytes(std::array::from_fn(|index| index as u8));
    let key_id = DigestKeyId::new(7);
    let digest = keyed_hash(KeyedHashDomain::IdempotencyKey, key_id, &key, b"abc");
    assert_eq!(digest.scheme(), DIGEST_SCHEME_V1);
    assert_eq!(digest.key_id(), key_id);
    assert_eq!(
        digest.as_bytes().as_slice(),
        hex("1f4efc4b2b126d651bcadff9d0db50cc3a356ee64281cb8ac421b190f3830f0c")
    );
}

#[test]
fn key_v1_vectors_freeze_namespace_version_and_component_encoding() {
    let mut entity = EntityKeyBuilder::new(EntityTypeId::new(0x0102_0304));
    entity
        .push_u32(0x0506_0708)
        .expect("bounded")
        .push_i64(-2)
        .expect("bounded")
        .push_bytes(b"ab")
        .expect("bounded");
    assert_eq!(ENTITY_KEY_V1_PREFIX, [0x45, 0x01]);
    assert_eq!(
        entity.as_bytes(),
        hex("450101020304050607087ffffffffffffffe000000026162")
    );

    let mut conflict = ConflictKeyBuilder::new(AggregateTypeId::new(0x1112_1314));
    conflict
        .push_u32(0x1516_1718)
        .expect("bounded")
        .push_i64(-2)
        .expect("bounded")
        .push_bytes(b"ab")
        .expect("bounded");
    assert_eq!(CONFLICT_KEY_V1_PREFIX, [0x43, 0x01]);
    assert_eq!(
        conflict.as_bytes(),
        hex("430111121314151617187ffffffffffffffe000000026162")
    );
}

#[test]
fn uuid_key_components_are_fixed_width_network_bytes() {
    let uuid = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];
    let mut key = EntityKeyBuilder::new(EntityTypeId::new(0x0102_0304));
    key.push_uuid(&uuid).expect("fixed component fits");

    assert_eq!(
        key.as_bytes(),
        hex("450101020304000102030405060708090a0b0c0d0e0f")
    );
}

#[test]
fn compiled_type_identity_separates_equal_components() {
    let mut first = EntityKeyBuilder::new(EntityTypeId::new(1));
    first.push_str("same").expect("bounded");
    let mut second = EntityKeyBuilder::new(EntityTypeId::new(2));
    second.push_str("same").expect("bounded");

    assert_ne!(first.as_bytes(), second.as_bytes());
    assert_eq!(first.as_bytes().cmp(second.as_bytes()), 1u32.cmp(&2));
}

proptest! {
    #[test]
    fn entity_type_ids_preserve_key_order(left in any::<u32>(), right in any::<u32>()) {
        let left_key = EntityKeyBuilder::new(EntityTypeId::new(left));
        let right_key = EntityKeyBuilder::new(EntityTypeId::new(right));
        prop_assert_eq!(left.cmp(&right), left_key.as_bytes().cmp(right_key.as_bytes()));
    }

    #[test]
    fn aggregate_type_ids_preserve_conflict_key_order(left in any::<u32>(), right in any::<u32>()) {
        let left_key = ConflictKeyBuilder::new(AggregateTypeId::new(left));
        let right_key = ConflictKeyBuilder::new(AggregateTypeId::new(right));
        prop_assert_eq!(left.cmp(&right), left_key.as_bytes().cmp(right_key.as_bytes()));
    }

    #[test]
    fn signed_i64_key_bytes_preserve_numeric_order(left in any::<i64>(), right in any::<i64>()) {
        let mut left_key = EntityKeyBuilder::new(EntityTypeId::new(1));
        left_key.push_i64(left).expect("fixed-size key");
        let mut right_key = EntityKeyBuilder::new(EntityTypeId::new(1));
        right_key.push_i64(right).expect("fixed-size key");
        prop_assert_eq!(left.cmp(&right), left_key.as_bytes().cmp(right_key.as_bytes()));
    }

    #[test]
    fn signed_i32_key_bytes_preserve_numeric_order(left in any::<i32>(), right in any::<i32>()) {
        let mut left_key = EntityKeyBuilder::new(EntityTypeId::new(1));
        left_key.push_i32(left).expect("fixed-size key");
        let mut right_key = EntityKeyBuilder::new(EntityTypeId::new(1));
        right_key.push_i32(right).expect("fixed-size key");
        prop_assert_eq!(left.cmp(&right), left_key.as_bytes().cmp(right_key.as_bytes()));
    }

    #[test]
    fn variable_components_are_unambiguous(left in proptest::collection::vec(any::<u8>(), 0..32), right in proptest::collection::vec(any::<u8>(), 0..32)) {
        let mut separate = EntityKeyBuilder::new(EntityTypeId::new(1));
        separate.push_bytes(&left).expect("bounded");
        separate.push_bytes(&right).expect("bounded");
        let mut joined = EntityKeyBuilder::new(EntityTypeId::new(1));
        let mut bytes = left;
        bytes.extend_from_slice(&right);
        joined.push_bytes(&bytes).expect("bounded");
        prop_assert_ne!(separate.as_bytes(), joined.as_bytes());
    }
}

#[test]
fn key_limit_is_exact_and_failed_append_is_atomic() {
    let mut exact = EntityKeyBuilder::new(EntityTypeId::new(1));
    exact
        .push_bytes(&vec![0; MAX_KEY_BYTES - 2 - 4 - 4])
        .expect("prefix, type identity, length, and bytes total exactly 4 KiB");
    assert_eq!(exact.as_bytes().len(), MAX_KEY_BYTES);
    assert!(exact.finish().is_ok());

    let mut too_large = EntityKeyBuilder::new(EntityTypeId::new(1));
    let before = too_large.as_bytes().to_vec();
    assert!(
        too_large
            .push_bytes(&vec![0; MAX_KEY_BYTES - 2 - 4 - 4 + 1])
            .is_err()
    );
    assert_eq!(too_large.as_bytes(), before);
}
