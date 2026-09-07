#[cfg(test)]
mod tests {
    use super::{CanonicalGroupBounds, CanonicalGroupLeafBuilder, GroupStateError};
    use crate::batch::aggregate::CanonicalPartialIdentity;
    use crate::segment_v2::{SegmentV2Cell, SegmentV2LogicalType, SegmentV2SegmentId};
    use riffdb_types::CanonicalValue;

    fn identity(batch: u32) -> CanonicalPartialIdentity {
        CanonicalPartialIdentity::new(0, SegmentV2SegmentId::from_bytes([7; 16]), batch)
    }

    #[test]
    fn canonical_group_leaf_normalizes_missing_and_null_without_colliding_with_values() {
        let bounds = CanonicalGroupBounds::new(3, 64, 256).expect("bounded plan");
        let mut leaf = CanonicalGroupLeafBuilder::new(
            identity(0),
            &[SegmentV2LogicalType::U64],
            bounds,
        )
        .expect("preallocated owner");

        leaf.push_row(0, &[SegmentV2Cell::Missing])
            .expect("missing");
        leaf.push_row(1, &[SegmentV2Cell::Null])
            .expect("null joins NoValue");
        leaf.push_row(2, &[SegmentV2Cell::Value(CanonicalValue::U64(0))])
            .expect("typed value");

        let groups = leaf.finish().expect("complete leaf");
        assert_eq!(groups.group_count(), 2);
        assert_eq!(groups.row_count_for_test(0), Some(2));
        assert_eq!(groups.row_count_for_test(1), Some(1));

        let mut wrong_type = CanonicalGroupLeafBuilder::new(
            identity(0),
            &[SegmentV2LogicalType::U64],
            bounds,
        )
        .expect("preallocated owner");
        assert_eq!(
            wrong_type.push_row(0, &[SegmentV2Cell::Value(CanonicalValue::I64(0))]),
            Err(GroupStateError::TypeMismatch)
        );
        assert!(wrong_type.is_poisoned());
    }
}
