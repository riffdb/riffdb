//! Entity-reference commit-record codec and hash contract tests.

use riffdb_types::EntityVersion;

use super::super::{
    CommitRecordRevisionV1, DurableCodecErrorKind, decode_commit_entity_references,
    decode_commit_event_references_with_revision, decode_commit_record_for_revision,
    decode_commit_record_legacy_v1, decode_commit_record_v2, decode_commit_record_v3,
    decode_commit_record_with_events, encode_commit_record_legacy_v1, encode_commit_record_v1,
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
fn revision_witness_dispatch_matches_the_compatibility_decoder() {
    let atomic = sample::atomic_record_set();
    let cases = [
        (
            CommitRecordRevisionV1::V3,
            encode_commit_record_v1(atomic.commit()).expect("encode v3"),
        ),
        (
            CommitRecordRevisionV1::V2,
            encode_commit_record_v2_fixture(atomic.commit(), atomic.entities()).expect("encode v2"),
        ),
        (
            CommitRecordRevisionV1::LegacyV1,
            encode_commit_record_legacy_v1(atomic.commit(), atomic.entities()).expect("encode v1"),
        ),
    ];

    for (expected_revision, encoded) in cases {
        let witnessed = decode_commit_event_references_with_revision(encoded.as_bytes())
            .expect("decode revision witness");
        assert_eq!(witnessed.value().revision(), expected_revision);
        assert_eq!(
            witnessed.value().references(),
            atomic.commit().event_references()
        );
        assert_eq!(
            witnessed.encoded_content_charge(),
            encoded.encoded_content_charge()
        );
        assert_eq!(
            format!("{:?}", witnessed.value()),
            format!(
                "DecodedCommitEventReferencesV1 {{ revision: {expected_revision:?}, reference_count: {} }}",
                atomic.events().len()
            )
        );

        let direct = decode_commit_record_for_revision(
            encoded.as_bytes(),
            witnessed.value().revision(),
            atomic.events().to_vec(),
        )
        .expect("direct revision materialization");
        let compatibility =
            decode_commit_record_with_events(encoded.as_bytes(), atomic.events().to_vec())
                .expect("compatibility materialization");
        assert_eq!(direct, compatibility);
    }
}

#[test]
fn revision_dispatch_rejects_a_witness_from_different_bytes() {
    let atomic = sample::atomic_record_set();
    let encoded = encode_commit_record_v1(atomic.commit()).expect("encode v3");
    let error = decode_commit_record_for_revision(
        encoded.as_bytes(),
        CommitRecordRevisionV1::V2,
        atomic.events().to_vec(),
    )
    .expect_err("a V2 witness cannot decode V3 bytes");
    assert_eq!(error.kind(), DurableCodecErrorKind::UnexpectedRecordType);
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
    use riffdb_types::{EntityKeyBuilder, EntityTypeId};

    use crate::conservative_entity_reference_payload_len;

    let atomic = sample::atomic_record_set();
    let encoded = encode_commit_record_v1(atomic.commit()).expect("encode");
    let payload = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .expect("decode envelope")
        .payload()
        .to_vec();
    let message = wire::StoredCommitRecordV3::decode(payload.as_slice()).expect("prost");
    for (reference, mutation) in message.entity_references.iter().zip(atomic.entities()) {
        let actual = reference.encoded_len();
        let charge = conservative_entity_reference_payload_len(mutation.post_image().target())
            .expect("charge");
        assert!(
            charge >= actual,
            "charge={charge} actual={actual} for sample entity reference"
        );
    }

    // Varied target sizes: empty-ish key (minimal u64 component) and max-ish key.
    let entity_type = EntityTypeId::new(9).expect("type");
    let mut short = EntityKeyBuilder::new(entity_type);
    short.push_u64(1).expect("key");
    let short_target =
        crate::EntityTarget::new(entity_type, short.finish().expect("finish")).expect("target");
    let mut long = EntityKeyBuilder::new(entity_type);
    for i in 0..8 {
        long.push_u64(i).expect("key component");
    }
    let long_target =
        crate::EntityTarget::new(entity_type, long.finish().expect("finish")).expect("target");
    for target in [&short_target, &long_target] {
        let proto = wire::CommittedEntityReferenceV2 {
            target: Some(wire::EntityTargetV1 {
                entity_type_id: target.entity_type_id().get(),
                entity_key: target.key().as_bytes().to_vec(),
            }),
            entity_version: 1,
            post_image_hash: vec![0xab; 32],
        };
        let actual = proto.encoded_len();
        let charge = conservative_entity_reference_payload_len(target).expect("charge");
        assert!(
            charge >= actual,
            "charge={charge} actual={actual} for varied key"
        );
    }
}
