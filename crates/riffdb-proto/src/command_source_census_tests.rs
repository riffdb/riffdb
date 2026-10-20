// req: PRJ-004, STO-012, STO-032
use super::*;
use crate::durable::ReadableRecordMessage;
use crate::storage::v1;
use prost::Message;

fn commit(fields: riffdb_types::CanonicalRecord) -> Vec<u8> {
    let message = v1::StoredCommitRecordV1 {
        mutations: vec![v1::CommittedEntityMutationV1 {
            post_image: Some(v1::StoredEntityRecordV1 {
                canonical_fields: riffdb_types::encode_canonical_record(&fields).unwrap(),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    crate::envelope::encode(
        v1::StoredCommitRecordV1::record_schema(),
        &message.encode_to_vec(),
    )
    .unwrap()
}

#[test]
fn source_census_accounts_historical_dense_entity_postimages_before_decode() {
    use riffdb_types::{CanonicalList, CanonicalRecord, CanonicalValue, FieldId};
    let sparse = commit(CanonicalRecord::new(vec![]).unwrap());
    let dense = commit(
        CanonicalRecord::new(vec![(
            FieldId::first(),
            CanonicalValue::List(CanonicalList::new(vec![CanonicalValue::Null; 4096]).unwrap()),
        )])
        .unwrap(),
    );
    assert!(
        inspect_locator_segment_decode_shape_v1(&dense, |_| Ok(0))
            .unwrap()
            .is_none()
    );
    let sparse_shape = inspect_command_source_decode_shape_v1(&sparse, |_| Ok(0))
        .unwrap()
        .unwrap();
    let dense_shape = inspect_command_source_decode_shape_v1(&dense, |_| Ok(0))
        .unwrap()
        .unwrap();
    assert_eq!(dense_shape.semantic_entity_mutations, 1);
    assert!(
        dense_shape.canonical_heap_bytes - sparse_shape.canonical_heap_bytes
            >= 4096 * std::mem::size_of::<CanonicalValue>() as u64
    );
}

#[test]
fn source_census_uses_real_generated_sizes_for_every_new_historical_root() {
    // Structural admission is not semantic authority. Defaults deliberately
    // exercise only the census; the full source decoder still rejects missing
    // mandatory semantic identifiers and checks canonicality after admission.
    macro_rules! check {
        ($ty:ty) => {{
            let message = <$ty>::default();
            let bytes =
                crate::envelope::encode(<$ty>::record_schema(), &message.encode_to_vec()).unwrap();
            let shape = inspect_command_source_decode_shape_v1(&bytes, |_| Ok(0))
                .unwrap()
                .unwrap();
            assert!(shape.wire_inline_bytes >= std::mem::size_of::<$ty>() as u64);
            assert!(
                inspect_locator_segment_decode_shape_v1(&bytes, |_| Ok(0))
                    .unwrap()
                    .is_none()
            );
        }};
    }
    check!(v1::StoredCommandCapsuleV1);
    check!(v1::StoredCommandCapsuleV2);
    check!(v1::StoredCommandCapsuleV3);
    check!(v1::StoredCommandCapsuleV4);
    check!(v1::StoredCommandCapsuleV5);
    check!(v1::StoredCommandCapsuleV6);
    check!(v1::StoredCommandCapsuleV7);
    check!(v1::StoredCommitRecordV1);
    check!(v1::StoredCommitRecordV2);
    check!(v1::StoredCommitRecordV3);
    check!(v1::StoredDurableEventV1);
    check!(v1::StoredDurableEventV2);
}
