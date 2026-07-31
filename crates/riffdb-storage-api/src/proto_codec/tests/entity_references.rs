//! Entity-reference commit-record codec and hash contract tests.

use riffdb_types::EntityVersion;

use super::super::{
    decode_commit_entity_references, decode_commit_record_legacy_v1, decode_commit_record_v2,
    decode_commit_record_v3, encode_commit_record_legacy_v1, encode_commit_record_v1,
    encode_commit_record_v2_fixture,
};
use super::sample;
use crate::{CommittedEntityReferenceV2, derive_entity_record_hash_v1};

#[test]
fn v3_commit_round_trips_entity_references() {
    let atomic = sample::atomic_record_set();
    let encoded = encode_commit_record_v1(atomic.commit()).expect("encode v3");
    let decoded =
        decode_commit_record_v3(encoded.as_bytes(), atomic.events().to_vec()).expect("decode v3");
    assert_eq!(decoded.value(), atomic.commit());
    assert_eq!(
        decoded.value().entity_references().len(),
        atomic.entities().len()
    );
}

#[test]
fn rev2_decode_hashes_embedded_images_into_references() {
    let atomic = sample::atomic_record_set();
    let encoded =
        encode_commit_record_v2_fixture(atomic.commit(), atomic.entities()).expect("encode v2");
    let decoded =
        decode_commit_record_v2(encoded.as_bytes(), atomic.events().to_vec()).expect("decode v2");
    for (reference, mutation) in decoded
        .value()
        .entity_references()
        .iter()
        .zip(atomic.entities())
    {
        let rehashed = derive_entity_record_hash_v1(mutation.post_image()).expect("rehash");
        assert_eq!(reference.post_image_hash(), rehashed);
        assert!(reference.matches(mutation.post_image()));
        assert_eq!(
            reference.entity_version(),
            mutation.post_image().entity_version()
        );
    }
}

#[test]
fn rev1_legacy_decode_derives_entity_references() {
    let atomic = sample::atomic_record_set();
    let encoded =
        encode_commit_record_legacy_v1(atomic.commit(), atomic.entities()).expect("encode v1");
    let decoded = decode_commit_record_legacy_v1(encoded.as_bytes()).expect("decode v1");
    assert_eq!(
        decoded.value().entity_references().len(),
        atomic.entities().len()
    );
    for (reference, mutation) in decoded
        .value()
        .entity_references()
        .iter()
        .zip(atomic.entities())
    {
        assert!(reference.matches(mutation.post_image()));
    }
}

#[test]
fn reference_only_decode_avoids_event_join() {
    let atomic = sample::atomic_record_set();
    let encoded = encode_commit_record_v1(atomic.commit()).expect("encode v3");
    let references = decode_commit_entity_references(encoded.as_bytes()).expect("refs");
    assert_eq!(references.value(), atomic.commit().entity_references());
}

#[test]
fn expected_from_version_matches_mutation_constructor_inverse() {
    assert_eq!(
        CommittedEntityReferenceV2::expected_from_version(EntityVersion::first()),
        crate::ExpectedEntityState::Absent
    );
    let second = EntityVersion::first().checked_next().expect("second");
    assert_eq!(
        CommittedEntityReferenceV2::expected_from_version(second),
        crate::ExpectedEntityState::Present(EntityVersion::first())
    );
}

#[test]
fn entity_reference_lens_dominates_encoded_reference_length() {
    use prost::Message as _;
    use riffdb_proto::storage::v1 as wire;

    let atomic = sample::atomic_record_set();
    let encoded = encode_commit_record_v1(atomic.commit()).expect("encode");
    let payload = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .expect("decode envelope")
        .payload()
        .to_vec();
    let message = wire::StoredCommitRecordV3::decode(payload.as_slice()).expect("prost");
    for reference in &message.entity_references {
        let actual = reference.encoded_len();
        // Conservative varint + target + 32-byte hash charge used by bounds.
        assert!(actual <= 512, "actual={actual}");
    }
}
