//! Codec-owned preflight proofs; constructing a row grants no source authority.
use super::*;
use crate::proto_codec::projection_source::row::row_to_wire;
use prost::Message;
use riffdb_types::{
    CanonicalList, CanonicalRecord, CanonicalValue, CommitSequence, ContractLineage, DatabaseId,
    FieldId, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionV1,
    ProjectionCheckpointOrdinal, ProjectionId, ProjectionIdentity, ProjectionPlanHash,
    ProjectionSourceLocatorV1,
};

fn row(number: u64, projection: u32, wide: bool) -> ProjectionSourceRowV1 {
    row_with_groups(number, projection, if wide { 16 } else { 1 }, wide)
}
fn row_with_groups(
    number: u64,
    projection: u32,
    groups: usize,
    wide: bool,
) -> ProjectionSourceRowV1 {
    ProjectionSourceRowV1::from_stored_parts(
        ProjectionIdentity::new(
            ContractLineage::new("source_encode_preflight").unwrap(),
            ProjectionId::new(projection).unwrap(),
            ProjectionPlanHash::from_bytes([9; 32]),
        ),
        ProjectionSourceLocatorV1::new(
            DatabaseId::from_unix_milliseconds_and_random(42, [7; 10]).unwrap(),
            number,
            ProjectionCheckpointOrdinal::new(number).unwrap(),
        )
        .unwrap(),
        vec![CanonicalValue::U64(number); groups],
        CanonicalRecord::new(vec![(
            FieldId::first(),
            if wide {
                CanonicalValue::List(CanonicalList::new(vec![CanonicalValue::Null; 4096]).unwrap())
            } else {
                CanonicalValue::U64(number)
            },
        )])
        .unwrap(),
        CommitSequence::new(number).unwrap(),
    )
    .unwrap()
}

// req: STO-012, PRJ-004
#[test]
fn source_row_borrowed_charge_matches_canonical_wire_at_all_varint_boundaries() {
    for number in [1, 127, 128, 16_383, 16_384, u32::MAX as u64, u64::MAX] {
        for projection in [1, 127, 128, 16_383, 16_384, u32::MAX] {
            for wide in [false, true] {
                let row = row(number, projection, wide);
                let message = row_to_wire(&row).unwrap();
                let encoded = super::super::encode_projection_source_row_v1(&row).unwrap();
                let charge = projection_source_row_encoded_charge_v1(&row).unwrap();
                assert_eq!(charge.get(), encoded.as_bytes().len());
                assert_eq!(charge.get(), 16 + message.encoded_len());
                assert_eq!(encoded.encoded_content_charge(), charge);
            }
        }
    }
    assert!(
        projection_source_row_max_encoded_bytes_v1().unwrap()
            >= projection_source_row_encoded_charge_v1(&row(u64::MAX, u32::MAX, true))
                .unwrap()
                .get()
    );
}

// req: STO-020, PRJ-004
#[test]
fn source_row_encode_work_admits_real_wire_and_output_before_allocation() {
    // Ordinary borrowed row fixtures are prepared before the Source ledger.
    // This is the additional encoding-work proof, not Source publication.
    let values = [1, 3, 5, 9, 16].map(|groups| row_with_groups(u64::MAX, u32::MAX, groups, true));
    let budget = riffdb_types::ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    for value in values {
        let work = projection_source_row_encode_work_bound_v1(&value).unwrap();
        let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
            - before.fixed_owner_bytes
            - before.reserved_bytes
            - before.owned_bytes;
        let blocker = budget.reserve(free - work + 1).unwrap();
        assert!(
            budget.reserve(work).is_err(),
            "one missing byte refuses before encoding"
        );
        drop(blocker);
        let blocker = budget.reserve(free - work).unwrap();
        let admitted = budget.reserve(work).unwrap();
        assert!(budget.reserve(1).is_err());
        // This proof's independent ownership census has its own prior admission.
        // Production encoding never constructs the census scratch.
        drop(blocker);
        let mut census = budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let wire = row_to_wire(&value).unwrap();
        let actual_group_capacity = wire.canonical_group_values.capacity();
        let encoded = crate::proto_codec::encode_message(super::super::ROW, &wire)
            .unwrap()
            .into_bytes();
        let wire_report =
            OwnedCapacityInspectionV1::preview(&wire, census.inspection_scope().unwrap()).unwrap();
        assert!(wire_report.shared_allocations().is_empty());
        let wire_bytes = wire_report.unshared_bytes();
        drop(wire_report);
        let encoded_report =
            OwnedCapacityInspectionV1::preview(&encoded, census.inspection_scope().unwrap())
                .unwrap();
        assert!(encoded_report.shared_allocations().is_empty());
        let observed = wire_bytes + encoded_report.unshared_bytes();
        assert!(
            observed <= work,
            "groups={} actual_group_capacity={} observed_bytes={} prepaid_bytes={}",
            value.group_values().len(),
            actual_group_capacity,
            observed,
            work
        );
        drop(encoded_report);
        drop((wire, encoded, census, admitted));
        assert_eq!(budget.observation().unwrap(), before);
    }
}

// req: STO-020
#[test]
fn source_encode_allocation_allowance_reuses_inspector_and_refuses_invalid_shapes() {
    let budget = riffdb_types::ProjectionSourceWorkingBudgetV1::new();
    let mut census = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    for (bytes, alignment) in [(0, 1), (1, 1), (4096, 8), (8192, 16)] {
        let expected = OwnedCapacityInspectionV1::preview_with(
            census.inspection_scope().unwrap(),
            |inspection| inspection.allocation(bytes, alignment),
        )
        .unwrap()
        .unshared_bytes();
        assert_eq!(
            riffdb_types::owned_allocation_capacity_bound_v1(bytes, alignment).unwrap(),
            expected
        );
    }
    assert!(riffdb_types::owned_allocation_capacity_bound_v1(1, 0).is_err());
    assert!(riffdb_types::owned_allocation_capacity_bound_v1(1, 3).is_err());
    assert!(riffdb_types::owned_allocation_capacity_bound_v1(usize::MAX, 8).is_err());
}
