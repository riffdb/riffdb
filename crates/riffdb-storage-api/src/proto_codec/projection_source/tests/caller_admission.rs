//! Shared canonical parser controls; values here are structural fixtures only.
use super::*;

// req: PRJ-004, STO-020, REC-003
#[test]
fn caller_source_row_matches_worker_for_both_framings_without_growing_scratch() {
    let row = row();
    let compact = encode_projection_source_row_v1(&row).unwrap();
    let legacy = raw_payload(&row_to_wire(&row).unwrap());
    let budget = ProjectionSourceWorkingBudgetV1::new();
    for encoded in [compact.as_bytes(), legacy.as_slice()] {
        let plan = inspect_projection_source_row_caller_decode_v1(encoded).unwrap();
        assert!(plan.maximum_work_bytes() > plan.scratch_capacity() as u64);
        let mut scratch = Vec::with_capacity(plan.scratch_capacity());
        let capacity = scratch.capacity();
        let decoded =
            decode_projection_source_row_with_caller_scratch_v1(encoded, &mut scratch).unwrap();
        let worker = decode_projection_source_row_structural_admitted_v1(encoded, &budget).unwrap();
        assert_eq!(decoded.value(), &**worker.value());
        assert_eq!(decoded.value(), &row);
        assert_eq!(scratch.capacity(), capacity);
        assert_eq!(
            decoded.encoded_content_charge(),
            worker.encoded_content_charge()
        );
    }
}

// req: PRJ-004, STO-020, REC-003
#[test]
fn caller_source_record_refuses_short_scratch_wrong_role_and_noncanonical_payload() {
    let encoded = encode_projection_source_row_v1(&row()).unwrap();
    let mut scratch = Vec::new();
    let before = scratch.capacity();
    assert_eq!(
        decode_projection_source_row_with_caller_scratch_v1(encoded.as_bytes(), &mut scratch)
            .unwrap_err(),
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable
    );
    assert_eq!(scratch.capacity(), before);
    let mut scratch = Vec::with_capacity(encoded.as_bytes().len());
    assert!(
        decode_projection_source_root_with_caller_scratch_v1(encoded.as_bytes(), &mut scratch)
            .is_err()
    );
    let mut message = row_to_wire(&row()).unwrap();
    message.canonical_group_values[0].push(0);
    let malformed = raw_payload(&message);
    let mut scratch = Vec::with_capacity(malformed.len());
    let capacity = scratch.capacity();
    assert!(decode_projection_source_row_with_caller_scratch_v1(&malformed, &mut scratch).is_err());
    assert_eq!(scratch.capacity(), capacity);
}

// req: PRJ-004, STO-020, REC-003
#[test]
fn caller_source_controls_reuse_exact_existing_semantic_mappers() {
    let object = ProjectionSourceObjectV1::new(descriptor(), ProjectionSourceLifecycleV1::Copying);
    let object_bytes = encode_projection_source_object_v1(&object).unwrap();
    let mut scratch = Vec::with_capacity(object_bytes.as_bytes().len());
    assert_eq!(
        decode_projection_source_object_with_caller_scratch_v1(
            object_bytes.as_bytes(),
            &mut scratch
        )
        .unwrap()
        .value(),
        &object
    );
    let root = root();
    let root_bytes = encode_projection_source_root_v1(&root).unwrap();
    let mut scratch = Vec::with_capacity(root_bytes.as_bytes().len());
    assert_eq!(
        decode_projection_source_root_with_caller_scratch_v1(root_bytes.as_bytes(), &mut scratch)
            .unwrap()
            .value(),
        &root
    );
    let maintenance = ProjectionSourceMaintenanceV1::initial(descriptor(), None).unwrap();
    let bytes = encode_projection_source_maintenance_v1(&maintenance).unwrap();
    let mut scratch = Vec::with_capacity(bytes.as_bytes().len());
    assert_eq!(
        decode_projection_source_maintenance_with_caller_scratch_v1(bytes.as_bytes(), &mut scratch)
            .unwrap()
            .value(),
        &maintenance
    );
}
