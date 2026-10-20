use super::*;
use riffdb_types::{
    CanonicalRecord, CanonicalValue, ContractLineage, DatabaseId, FieldId,
    PROJECTION_SOURCE_WORKING_MEMORY_BYTES, ProjectionId, ProjectionIdentity, ProjectionPlanHash,
    encode_canonical_record, encode_canonical_value,
};
fn encoded_row(value: u64) -> Vec<u8> {
    let identity = ProjectionIdentity::new(
        ContractLineage::new("proto_source_proof").unwrap(),
        ProjectionId::first(),
        ProjectionPlanHash::from_bytes([9; 32]),
    );
    let message = wire::StoredProjectionSourceRowV1 {
        identity: Some(wire::ProjectionIdentityV1 {
            contract_lineage: identity.contract_lineage().as_str().to_owned(),
            projection_id: identity.projection_id().get(),
            projection_plan_hash: identity.plan_hash().as_bytes().to_vec(),
        }),
        locator: Some(wire::ProjectionSourceLocatorV1 {
            database_id: DatabaseId::from_unix_milliseconds_and_random(42, [7; 10])
                .unwrap()
                .as_bytes()
                .to_vec(),
            allocation_history_incarnation: 2,
            checkpoint_ordinal: 1,
        }),
        canonical_group_values: vec![encode_canonical_value(&CanonicalValue::U64(value)).unwrap()],
        canonical_measures: encode_canonical_record(
            &CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(value))]).unwrap(),
        )
        .unwrap(),
        last_changed_sequence: 2,
    };
    crate::durable::encode_current_message(&message).unwrap()
}
// req: PRJ-004, STO-020
#[test]
fn source_wire_decode_uses_one_source_ledger_and_retains_charge_until_final_reference() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let original = encoded_row(7);
    let input = inspect_projection_source_decode_v1::<wire::StoredProjectionSourceRowV1>(&original)
        .unwrap();
    assert_eq!(input.canonical_group_values().count(), 1);
    assert!(input.canonical_measures().is_some());
    let mut scratch = ProjectionSourceDecodeScratchV1::new(original.len(), &budget).unwrap();
    let before = budget.observation().unwrap();
    let message = input.decode_with_scratch(&mut scratch).unwrap();
    assert!(message.belongs_to(&budget));
    let kept = message.clone();
    drop(message);
    assert!(budget.observation().unwrap().owned_bytes > before.owned_bytes);
    let second = encoded_row(8);
    let next = inspect_projection_source_decode_v1::<wire::StoredProjectionSourceRowV1>(&second)
        .unwrap()
        .decode_with_scratch(&mut scratch)
        .unwrap();
    assert_ne!(kept.canonical_measures, next.canonical_measures);
    drop(next);
    drop(kept);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    drop(scratch);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
// req: PRJ-004, STO-020
#[test]
fn source_wire_decode_pressure_refuses_before_message_construction_and_releases_reservations() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let original = encoded_row(7);
    let input = inspect_projection_source_decode_v1::<wire::StoredProjectionSourceRowV1>(&original)
        .unwrap();
    let mut scratch = ProjectionSourceDecodeScratchV1::new(original.len(), &budget).unwrap();
    let before = budget.observation().unwrap();
    let held = budget
        .reserve(
            PROJECTION_SOURCE_WORKING_MEMORY_BYTES
                - before.fixed_owner_bytes
                - before.reserved_bytes
                - before.owned_bytes,
        )
        .unwrap();
    assert!(matches!(
        input.decode_with_scratch(&mut scratch),
        Err(ProjectionSourceDecodeErrorV1::AdmissionUnavailable)
    ));
    drop(held);
    assert_eq!(budget.observation().unwrap(), before);
    drop(scratch);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

// req: STO-012, STO-020, PRJ-004
#[test]
fn source_inline_allocator_canonicality_matches_both_framings_and_refuses_substitutions() {
    use crate::durable::ReadableRecordMessage;
    type Allocator = wire::StoredProjectionCheckpointOrdinalAllocatorV1;
    let schema = Allocator::record_schema();
    let envelope = |payload: Vec<u8>| {
        wire::StoredEnvelope {
            storage_format_version: 1,
            record_type: schema.record_type().to_owned(),
            payload_crc32c: super::super::payload_crc32c(&payload),
            payload,
            schema_hash: schema.schema_hash().as_bytes().to_vec(),
        }
        .encode_to_vec()
    };
    for incarnation in [1, 127, 128, u64::MAX] {
        for ordinal in [1, 127, 128, u64::MAX] {
            let expected = Allocator {
                allocation_history_incarnation: incarnation,
                position: Some(
                    wire::stored_projection_checkpoint_ordinal_allocator_v1::Position::NextOrdinal(
                        ordinal,
                    ),
                ),
            };
            let payload = expected.encode_to_vec();
            for encoded in [
                crate::durable::encode_current_message(&expected).unwrap(),
                envelope(payload),
            ] {
                assert_eq!(
                    decode_projection_checkpoint_ordinal_allocator_inline_v1(&encoded).unwrap(),
                    expected
                );
            }
        }
    }
    for payload in [
        vec![8, 0x80],
        vec![8, 1, 16, 0x80],
        vec![8, 1, 16, 1, 0x98, 6, 1],
        vec![8, 1, 16, 1, 26, 0],
        vec![8, 1, 16, 1, 16, 2],
        vec![8, 1, 26, 2, 8, 1],
        vec![8, 0x81, 0, 16, 1],
    ] {
        assert!(
            decode_projection_checkpoint_ordinal_allocator_inline_v1(&envelope(payload)).is_err()
        );
    }
    let expected = Allocator {
        allocation_history_incarnation: 1,
        position: Some(
            wire::stored_projection_checkpoint_ordinal_allocator_v1::Position::Exhausted(
                wire::UnitV1 {},
            ),
        ),
    };
    let compact = crate::durable::encode_current_message(&expected).unwrap();
    assert_eq!(
        decode_projection_checkpoint_ordinal_allocator_inline_v1(&compact).unwrap(),
        expected
    );
    assert_eq!(
        decode_projection_checkpoint_ordinal_allocator_inline_v1(&encoded_row(7)).unwrap_err(),
        EnvelopeError::UnknownRecordType
    );
    let mut foreign_revision = compact.clone();
    foreign_revision[7] ^= 1;
    assert!(decode_projection_checkpoint_ordinal_allocator_inline_v1(&foreign_revision).is_err());
    let mut corrupt_compact = compact;
    *corrupt_compact.last_mut().unwrap() ^= 1;
    assert_eq!(
        decode_projection_checkpoint_ordinal_allocator_inline_v1(&corrupt_compact).unwrap_err(),
        EnvelopeError::ChecksumMismatch
    );
    let canonical = envelope(expected.encode_to_vec());
    assert_eq!(
        decode_projection_checkpoint_ordinal_allocator_inline_v1(&canonical).unwrap(),
        expected
    );
    let mut unknown = canonical.clone();
    unknown.extend_from_slice(&[0x98, 6, 1]);
    assert_eq!(
        decode_projection_checkpoint_ordinal_allocator_inline_v1(&unknown).unwrap_err(),
        EnvelopeError::NonCanonicalEnvelope
    );
    // Reordered known fields have equivalent values but differ from the exact
    // ordinary Prost envelope order and must be refused by the inline path.
    let mut reordered = canonical[2..].to_vec();
    reordered.extend_from_slice(&canonical[..2]);
    assert_eq!(
        decode_projection_checkpoint_ordinal_allocator_inline_v1(&reordered).unwrap_err(),
        EnvelopeError::NonCanonicalEnvelope
    );
    let mut nonminimal = canonical.clone();
    nonminimal.splice(1..2, [0x81, 0]);
    assert!(decode_projection_checkpoint_ordinal_allocator_inline_v1(&nonminimal).is_err());
    let mut checksum = wire::StoredEnvelope::decode(canonical.as_slice()).unwrap();
    checksum.payload_crc32c ^= 1;
    assert_eq!(
        decode_projection_checkpoint_ordinal_allocator_inline_v1(&checksum.encode_to_vec())
            .unwrap_err(),
        EnvelopeError::ChecksumMismatch
    );
    let mut schema_hash = wire::StoredEnvelope::decode(canonical.as_slice()).unwrap();
    schema_hash.schema_hash[0] ^= 1;
    assert_eq!(
        decode_projection_checkpoint_ordinal_allocator_inline_v1(&schema_hash.encode_to_vec())
            .unwrap_err(),
        EnvelopeError::UnsupportedSchemaHash
    );
    const {
        assert!(!<Allocator as riffdb_types::InspectOwnedCapacityV1>::HAS_NESTED_ALLOCATIONS);
    }
}
