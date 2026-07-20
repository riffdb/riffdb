#![forbid(unsafe_code)]

//! Golden and property coverage for v1 digest frames and durable keys.

use proptest::prelude::*;
use riffdb_types::{
    AggregateTypeId, CONFLICT_KEY_V1_PREFIX, CapabilityTokenDigest, ConflictKeyBuilder,
    DIGEST_SCHEME_V1, Date, DigestKey, DigestKeyId, ENTITY_KEY_V1_PREFIX, EntityKeyBuilder,
    EntityTypeId, EnumVariantId, HashDomain, INDEX_ENTRY_KEY_V1_PREFIX, IndexEntryKeyBuilder,
    IndexId, KeyedHashDomain, MAX_KEY_BYTES, PARTITION_KEY_V1_PREFIX, PartitionKeyBuilder,
    ProjectionApplyHash, Timestamp, hash, hash_capability_token, hash_capability_token_secret,
    hash_contract_plan_root, hash_partition_key, hash_plan, hash_projection_apply,
    hash_projection_plan, keyed_hash, keyed_hash_secret,
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
            HashDomain::ProjectionPlan,
            "44887bd72c1e081a9c3499ad47f35a2c090e5176552c2a7ad3324902db3705f0",
        ),
        (
            HashDomain::ContractPlanRoot,
            "c2376efb994722621ff5ba006b7a2090e977e954e906aef61eb4b7d4047f0cbc",
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
            HashDomain::PartitionKey,
            "6f1bf670b12c3a846389a18141dc69c6cc7e9a5abfe06b62e38cc2323eb80b8d",
        ),
        (
            HashDomain::Schema,
            "4289634dbb8dd918bbd4f664b550c3db4b6b0ea8754ff2128c33b0b41e44e431",
        ),
        (
            HashDomain::ProjectionApply,
            "406205600e6ec6208bd990827d9348fb37302d9d1ccd55a2846e8bc8ea6937c2",
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

    let _: riffdb_types::ProjectionPlanHash = hash_projection_plan(b"abc");
    let _: riffdb_types::ContractPlanRootHash = hash_contract_plan_root(b"abc");
    let _: riffdb_types::PartitionKeyHash = hash_partition_key(b"abc");
    let _: ProjectionApplyHash = hash_projection_apply(b"abc");
}

#[test]
fn hmac_v1_idempotency_vector_and_key_metadata_are_stable() {
    let key_bytes = std::array::from_fn(|index| index as u8);
    let key = DigestKey::from_bytes(key_bytes);
    let key_id = DigestKeyId::new(7).expect("nonzero key ID");
    let digest = keyed_hash(KeyedHashDomain::IdempotencyKey, key_id, &key, b"abc");
    let borrowed_digest =
        keyed_hash_secret(KeyedHashDomain::IdempotencyKey, key_id, &key_bytes, b"abc");
    assert_eq!(digest.scheme(), DIGEST_SCHEME_V1);
    assert_eq!(digest.key_id(), key_id);
    assert_eq!(borrowed_digest, digest);
    assert_eq!(
        digest.as_bytes().as_slice(),
        hex("1f4efc4b2b126d651bcadff9d0db50cc3a356ee64281cb8ac421b190f3830f0c")
    );
}

#[test]
fn hmac_v1_capability_token_vector_uses_raw_token_bytes_and_redacts() {
    let key_bytes = std::array::from_fn(|index| index as u8);
    let key = DigestKey::from_bytes(key_bytes);
    let key_id = DigestKeyId::new(7).expect("nonzero key ID");
    let raw_token = std::array::from_fn(|index| index as u8);
    let digest: CapabilityTokenDigest = hash_capability_token(key_id, &key, &raw_token);
    let borrowed_digest = hash_capability_token_secret(key_id, &key_bytes, &raw_token);
    let idempotency_digest = keyed_hash(KeyedHashDomain::IdempotencyKey, key_id, &key, &raw_token);
    let borrowed_idempotency_digest = keyed_hash_secret(
        KeyedHashDomain::IdempotencyKey,
        key_id,
        &key_bytes,
        &raw_token,
    );

    assert_eq!(digest.scheme(), DIGEST_SCHEME_V1);
    assert_eq!(digest.key_id(), key_id);
    assert_eq!(borrowed_digest, digest);
    assert_eq!(borrowed_idempotency_digest, idempotency_digest);
    assert_eq!(
        digest.as_bytes().as_slice(),
        hex("836b1036b35f59f04efaace126446cbf0f81ff113e6e768ae7b3c88e72a8e042")
    );
    assert_ne!(digest.as_bytes(), idempotency_digest.as_bytes());

    let encoded_text = b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    let text_digest = keyed_hash(KeyedHashDomain::CapabilityToken, key_id, &key, encoded_text);
    assert_ne!(digest.as_bytes(), text_digest.as_bytes());
    assert!(!format!("{digest:?}").contains("836b1036"));
    assert!(!format!("{text_digest:?}").contains("836b1036"));
}

#[test]
fn key_v1_vectors_freeze_namespace_version_and_component_encoding() {
    let mut entity = EntityKeyBuilder::new(EntityTypeId::new(0x0102_0304).expect("nonzero"));
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

    let mut conflict = ConflictKeyBuilder::new(AggregateTypeId::new(0x1112_1314).expect("nonzero"));
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

    let mut partition =
        PartitionKeyBuilder::new(AggregateTypeId::new(0x2122_2324).expect("nonzero"));
    partition.push_i64(-2).expect("bounded");
    assert_eq!(PARTITION_KEY_V1_PREFIX, [0x50, 0x01]);
    assert_eq!(partition.as_bytes(), hex("5001212223247ffffffffffffffe"));

    let entity = entity.finish().expect("bounded entity key");
    let mut index = IndexEntryKeyBuilder::new(IndexId::new(0x3132_3334).expect("nonzero"));
    index.push_str("ab").expect("bounded");
    let index = index.finish(entity).expect("bounded complete index key");
    assert_eq!(INDEX_ENTRY_KEY_V1_PREFIX, [0x49, 0x01]);
    assert_eq!(
        index.as_bytes(),
        hex("49013132333400000002616200000018450101020304050607087ffffffffffffffe000000026162")
    );
}

#[test]
fn closed_scalar_component_helpers_have_exact_bytes() {
    let mut key = PartitionKeyBuilder::new(AggregateTypeId::first());
    key.push_bool(true)
        .expect("bounded")
        .push_timestamp(Timestamp::new(-2, 3).expect("valid timestamp"))
        .expect("bounded")
        .push_date(Date::new(-4))
        .expect("bounded")
        .push_enum_variant(EnumVariantId::new(5).expect("nonzero"))
        .expect("bounded");

    assert_eq!(
        key.as_bytes(),
        hex("500100000001017ffffffffffffffe000000037ffffffc00000005")
    );
}

#[test]
fn uuid_key_components_are_fixed_width_network_bytes() {
    let uuid = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];
    let mut key = EntityKeyBuilder::new(EntityTypeId::new(0x0102_0304).expect("nonzero"));
    key.push_uuid(&uuid).expect("fixed component fits");

    assert_eq!(
        key.as_bytes(),
        hex("450101020304000102030405060708090a0b0c0d0e0f")
    );
}

#[test]
fn compiled_type_identity_separates_equal_components() {
    let mut first = EntityKeyBuilder::new(EntityTypeId::first());
    first.push_str("same").expect("bounded");
    let mut second = EntityKeyBuilder::new(EntityTypeId::new(2).expect("nonzero"));
    second.push_str("same").expect("bounded");

    assert_ne!(first.as_bytes(), second.as_bytes());
    assert_eq!(first.as_bytes().cmp(second.as_bytes()), 1u32.cmp(&2));
}

proptest! {
    #[test]
    fn entity_type_ids_preserve_key_order(left in 1u32..=u32::MAX, right in 1u32..=u32::MAX) {
        let left_key = EntityKeyBuilder::new(EntityTypeId::new(left).expect("generated nonzero"));
        let right_key = EntityKeyBuilder::new(EntityTypeId::new(right).expect("generated nonzero"));
        prop_assert_eq!(left.cmp(&right), left_key.as_bytes().cmp(right_key.as_bytes()));
    }

    #[test]
    fn aggregate_type_ids_preserve_conflict_key_order(left in 1u32..=u32::MAX, right in 1u32..=u32::MAX) {
        let left_key = ConflictKeyBuilder::new(AggregateTypeId::new(left).expect("generated nonzero"));
        let right_key = ConflictKeyBuilder::new(AggregateTypeId::new(right).expect("generated nonzero"));
        prop_assert_eq!(left.cmp(&right), left_key.as_bytes().cmp(right_key.as_bytes()));
    }

    #[test]
    fn aggregate_type_ids_preserve_partition_key_order(left in 1u32..=u32::MAX, right in 1u32..=u32::MAX) {
        let left_key = PartitionKeyBuilder::new(AggregateTypeId::new(left).expect("generated nonzero"));
        let right_key = PartitionKeyBuilder::new(AggregateTypeId::new(right).expect("generated nonzero"));
        prop_assert_eq!(left.cmp(&right), left_key.as_bytes().cmp(right_key.as_bytes()));
    }

    #[test]
    fn index_ids_preserve_index_entry_prefix_order(left in 1u32..=u32::MAX, right in 1u32..=u32::MAX) {
        let left_key = IndexEntryKeyBuilder::new(IndexId::new(left).expect("generated nonzero"));
        let right_key = IndexEntryKeyBuilder::new(IndexId::new(right).expect("generated nonzero"));
        prop_assert_eq!(left.cmp(&right), left_key.as_bytes().cmp(right_key.as_bytes()));
    }

    #[test]
    fn signed_i64_key_bytes_preserve_numeric_order(left in any::<i64>(), right in any::<i64>()) {
        let mut left_key = EntityKeyBuilder::new(EntityTypeId::first());
        left_key.push_i64(left).expect("fixed-size key");
        let mut right_key = EntityKeyBuilder::new(EntityTypeId::first());
        right_key.push_i64(right).expect("fixed-size key");
        prop_assert_eq!(left.cmp(&right), left_key.as_bytes().cmp(right_key.as_bytes()));
    }

    #[test]
    fn signed_i32_key_bytes_preserve_numeric_order(left in any::<i32>(), right in any::<i32>()) {
        let mut left_key = EntityKeyBuilder::new(EntityTypeId::first());
        left_key.push_i32(left).expect("fixed-size key");
        let mut right_key = EntityKeyBuilder::new(EntityTypeId::first());
        right_key.push_i32(right).expect("fixed-size key");
        prop_assert_eq!(left.cmp(&right), left_key.as_bytes().cmp(right_key.as_bytes()));
    }

    #[test]
    fn variable_components_are_unambiguous(left in proptest::collection::vec(any::<u8>(), 0..32), right in proptest::collection::vec(any::<u8>(), 0..32)) {
        let mut separate = EntityKeyBuilder::new(EntityTypeId::first());
        separate.push_bytes(&left).expect("bounded");
        separate.push_bytes(&right).expect("bounded");
        let mut joined = EntityKeyBuilder::new(EntityTypeId::first());
        let mut bytes = left;
        bytes.extend_from_slice(&right);
        joined.push_bytes(&bytes).expect("bounded");
        prop_assert_ne!(separate.as_bytes(), joined.as_bytes());
    }
}

#[test]
fn key_limit_is_exact_and_failed_append_is_atomic() {
    let mut exact = EntityKeyBuilder::new(EntityTypeId::first());
    exact
        .push_bytes(&vec![0; MAX_KEY_BYTES - 2 - 4 - 4])
        .expect("prefix, type identity, length, and bytes total exactly 4 KiB");
    assert_eq!(exact.as_bytes().len(), MAX_KEY_BYTES);
    assert!(exact.finish().is_ok());

    let mut too_large = EntityKeyBuilder::new(EntityTypeId::first());
    let before = too_large.as_bytes().to_vec();
    assert!(
        too_large
            .push_bytes(&vec![0; MAX_KEY_BYTES - 2 - 4 - 4 + 1])
            .is_err()
    );
    assert_eq!(too_large.as_bytes(), before);

    let entity = EntityKeyBuilder::new(EntityTypeId::first())
        .finish()
        .expect("minimal entity key");
    let mut exact_index = IndexEntryKeyBuilder::new(IndexId::first());
    exact_index
        .push_bytes(&vec![0; MAX_KEY_BYTES - 6 - 4 - 4 - 6])
        .expect("index payload leaves room for nested entity framing");
    let exact_index = exact_index
        .finish(entity.clone())
        .expect("complete index key is exactly 4 KiB");
    assert_eq!(exact_index.as_bytes().len(), MAX_KEY_BYTES);

    let mut too_large_index = IndexEntryKeyBuilder::new(IndexId::first());
    too_large_index
        .push_bytes(&vec![0; MAX_KEY_BYTES - 6 - 4 - 4 - 6 + 1])
        .expect("partial index key alone fits");
    assert!(matches!(
        too_large_index.finish(entity),
        Err(riffdb_types::KeyEncodingError::TooLong { .. })
    ));
}
