//! Source-only admission preserves complete existing canonical/semantic decoding.
// req: PRJ-004, STO-012, STO-020, STO-032
use super::*;
use prost::Message;
use riffdb_types::{DualFrontier, ProjectionSourceWorkingBudgetV1};

fn finish(
    bytes: &[u8],
    events: &[crate::StoredDurableEventV1],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> ProjectionSourceCommandReadV1 {
    let mut value = decode_projection_source_command_admitted_v1(bytes, budget).unwrap();
    for event in events {
        if !value.required_events().is_empty() {
            let encoded = encode_durable_event_v1(event).unwrap();
            value.accept_event_canonical(encoded.as_bytes()).unwrap();
        }
    }
    value.finish_event_join().unwrap();
    value
}
fn prefix_capsule() -> crate::StoredCommandCapsuleV2 {
    let (base, atomic) = sample_command_capsule_v1();
    let capsule =
        crate::StoredCommandCapsuleV2::from_base(base, atomic.index_epochs().to_vec()).unwrap();
    let entity = encode_entity_record_v1(atomic.entities()[0].post_image()).unwrap();
    capsule
        .with_prefix_evidence(
            crate::CommandPrefixEvidenceV1::new(
                DualFrontier::new(None, None),
                DualFrontier::new(
                    Some(CommitSequence::first()),
                    Some(AdministrationSequence::new(2).unwrap()),
                ),
                vec![
                    crate::AuthoritativeMutationV3::put(
                        crate::AuthoritativeNamespaceV1::Entities,
                        b"key",
                        None,
                        entity.as_bytes(),
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        )
        .unwrap()
}
fn segment(capsule: crate::StoredCommandCapsuleV2) -> CanonicalStoredEnvelopeV1 {
    let first = capsule.commit_sequence();
    let manifest = crate::CommandSegmentManifestV1::new(vec![
        crate::CommandDerivedIndexManifestEntryV1::new(
            crate::CommandDerivedIndexKindV1::EventRoute,
            crate::CommandDerivedMemberV1::Event,
            vec![0x31],
            0,
            0,
            first,
        )
        .unwrap(),
    ])
    .unwrap();
    let draft = crate::StoredCommandSegmentV1::new(
        sample::database_id(),
        1,
        None,
        vec![capsule],
        manifest,
        crate::CommandSegmentDigestV1::from_bytes([0; 32]),
    )
    .unwrap();
    seal_and_encode_command_segment_v1(draft).unwrap().1
}
#[test]
fn source_command_reader_matches_complete_existing_semantics_for_current_and_historical_graphs() {
    let (base, atomic) = sample_command_capsule_v1();
    let modern =
        crate::StoredCommandCapsuleV2::from_base(base.clone(), atomic.index_epochs().to_vec())
            .unwrap();
    let anchored = sample_anchored_command_capsule_v2();
    let prefixed = prefix_capsule();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    for capsule in [&modern, &anchored, &prefixed] {
        let encoded = encode_command_capsule_v2(capsule).unwrap();
        for bytes in [
            encoded.clone(),
            transcode_durable_record_to_v1(encoded.as_bytes()).unwrap(),
        ] {
            let decoded = finish(bytes.as_bytes(), &[], &budget);
            assert_eq!(decoded.commit_count(), 1);
            assert_eq!(decoded.commit(0), Some(capsule.base().commit()));
            assert!(decoded.commit(1).is_none());
        }
    }
    for capsule in [modern, anchored, prefixed] {
        let expected = capsule.base().commit().clone();
        let bytes = segment(capsule);
        let old = decode_command_segment_v1(bytes.as_bytes()).unwrap();
        let decoded = finish(bytes.as_bytes(), &[], &budget);
        assert_eq!(decoded.commit(0), Some(&expected));
        assert_eq!(
            decoded.commit(0),
            Some(old.value().commands()[0].base().commit())
        );
    }
    for bytes in [
        encode_command_capsule_v1(&base).unwrap(),
        encode_commit_record_v1(atomic.commit()).unwrap(),
    ] {
        let decoded = finish(bytes.as_bytes(), atomic.events(), &budget);
        assert_eq!(decoded.commit(0), Some(atomic.commit()));
    }
    assert_eq!(budget.observation().unwrap(), before);
}
#[test]
fn source_command_reader_refuses_worker_pressure_before_allocating_without_gating_ordinary_decode()
{
    let capsule = prefix_capsule();
    let encoded = encode_command_capsule_v2(&capsule).unwrap();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let block = budget
        .reserve(
            riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
                - before.fixed_owner_bytes
                - before.owned_bytes
                - before.reserved_bytes,
        )
        .unwrap();
    let blocked = budget.observation().unwrap();
    assert_eq!(
        decode_command_capsule_v2(encoded.as_bytes())
            .unwrap()
            .value(),
        &capsule
    );
    assert!(riffdb_proto::envelope::release_locator_decode_scratch_v1());
    assert!(matches!(
        decode_projection_source_command_admitted_v1(encoded.as_bytes(), &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
    ));
    assert_eq!(budget.observation().unwrap(), blocked);
    drop(block);
    let decoded = finish(encoded.as_bytes(), &[], &budget);
    assert_eq!(decoded.commit(0), Some(capsule.base().commit()));
    drop(decoded);
    assert_eq!(budget.observation().unwrap(), before);
}
#[test]
fn source_command_reader_event_join_refuses_missing_unequal_and_unadmitted_events() {
    let (base, atomic) = sample_command_capsule_v1();
    for bytes in [
        encode_commit_record_v1(atomic.commit()).unwrap(),
        encode_command_capsule_v1(&base).unwrap(),
    ] {
        let good = encode_durable_event_v1(&atomic.events()[0]).unwrap();
        let other = sample::atomic_record_set_at(
            CommitSequence::new(2).unwrap(),
            riffdb_types::RequestId::from_bytes(sample::uuid_v7(0x51)).unwrap(),
            riffdb_types::ProvenanceId::from_bytes(sample::uuid_v7(0x52)).unwrap(),
        );
        let wrong = encode_durable_event_v1(&other.events()[0]).unwrap();
        let budget = ProjectionSourceWorkingBudgetV1::new();
        let before = budget.observation().unwrap();
        let mut value =
            decode_projection_source_command_admitted_v1(bytes.as_bytes(), &budget).unwrap();
        assert!(value.commit(0).is_none());
        assert!(value.finish_event_join().is_err());
        let prepared = budget.observation().unwrap();
        assert!(value.accept_event_canonical(wrong.as_bytes()).is_err());
        assert_eq!(budget.observation().unwrap(), prepared);
        let block = budget
            .reserve(
                riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
                    - prepared.fixed_owner_bytes
                    - prepared.owned_bytes
                    - prepared.reserved_bytes,
            )
            .unwrap();
        assert!(matches!(
            value.accept_event_canonical(good.as_bytes()),
            Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
        ));
        drop(block);
        value.accept_event_canonical(good.as_bytes()).unwrap();
        assert!(budget.observation().unwrap().reserved_bytes > prepared.reserved_bytes);
        value.finish_event_join().unwrap();
        assert_eq!(value.commit(0), Some(atomic.commit()));
        drop(value);
        assert_eq!(budget.observation().unwrap(), before);
    }
}
#[test]
fn source_command_reader_rejects_unknown_unused_fields_and_checksum_damage() {
    let capsule = prefix_capsule();
    let encoded = encode_command_capsule_v2(&capsule).unwrap();
    let decoded = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .unwrap();
    let role = decoded.record_type();
    let mut payload = decoded.payload().to_vec();
    payload.extend_from_slice(&[0x98, 0x06, 1]);
    let schema = riffdb_proto::durable::readable_record_schema(role).unwrap();
    let mut unknown = encoded.as_bytes().to_vec();
    unknown.extend_from_slice(&[0x98, 0x06, 1]);
    unknown[8..12].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    let crc = riffdb_proto::envelope::payload_crc32c(&unknown[16..]);
    unknown[12..16].copy_from_slice(&crc.to_be_bytes());
    let mut damaged = encoded.as_bytes().to_vec();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    let mut message =
        riffdb_proto::storage::v1::StoredCommandCapsuleV7::decode(decoded.payload()).unwrap();
    message.canonical_service_values.push(0);
    let malformed = riffdb_proto::envelope::encode(schema, &message.encode_to_vec()).unwrap();
    assert!(riffdb_proto::envelope::release_locator_decode_scratch_v1());
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    for bytes in [&unknown, &damaged, &malformed] {
        assert!(decode_projection_source_command_admitted_v1(bytes, &budget).is_err());
        assert_eq!(budget.observation().unwrap(), before);
    }
}
