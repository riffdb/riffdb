//! Allocation-free owning-field bounds for ADR-0316 source payloads.
//! Canonical scalar/oneof form is additionally checked by the sealed codec;
//! the ordinary metadata allocator path closes those fields without decoding.

use super::*;

static SOURCE_LOCATOR: Shape = Shape::new(&[fixed_bytes(1, 16)]);
static SOURCE_DIGEST: Shape = Shape::new(&[fixed_bytes(2, 32)]);
static SOURCE_REFERENCE: Shape = Shape::new(&[
    message(1, &PROJECTION_IDENTITY),
    message(2, &SOURCE_LOCATOR),
    message(3, &SOURCE_DIGEST),
    message(4, &SOURCE_DIGEST),
]);
static SOURCE_DESCRIPTOR: Shape = Shape::new(&[
    message(1, &PROJECTION_IDENTITY),
    message(2, &SOURCE_LOCATOR),
    message(4, &CHANGELOG_HISTORY_STATE_V3),
    message(5, &SOURCE_DIGEST),
    message(6, &SOURCE_DIGEST),
    message(9, &SOURCE_DIGEST),
    message(10, &SOURCE_DIGEST),
]);
static SOURCE_DUAL_FRONTIER: Shape = Shape::new(&[]);
pub(super) static SOURCE_PUBLICATION_BINDING: Shape = Shape::new(&[
    fixed_bytes(1, 16),
    message(5, &SOURCE_DUAL_FRONTIER),
    message(6, &SOURCE_DUAL_FRONTIER),
    fixed_bytes(7, 32),
]);

pub(super) static PROJECTION_SOURCE_ROW_V1: Shape = Shape::new(&[
    message(1, &PROJECTION_IDENTITY),
    message(2, &SOURCE_LOCATOR),
    repeated_bytes(
        3,
        riffdb_types::MAX_PROJECTION_GROUP_COMPONENTS,
        MAX_DOCUMENT_BYTES,
    ),
    bytes(4, MAX_DOCUMENT_BYTES),
]);
pub(super) static PROJECTION_SOURCE_OBJECT_V1: Shape =
    Shape::new(&[message(1, &SOURCE_DESCRIPTOR)]);
pub(super) static PROJECTION_SOURCE_ROOT_V1: Shape = Shape::new(&[
    message(1, &SOURCE_REFERENCE),
    message(2, &SOURCE_PUBLICATION_BINDING),
]);
pub(super) static PROJECTION_SOURCE_MAINTENANCE_V1: Shape = Shape::new(&[
    message(1, &SOURCE_DESCRIPTOR),
    message(2, &PROJECTION_SOURCE_ROOT_V1),
    bytes(5, riffdb_types::MAX_PROJECTION_SOURCE_CONTROL_BYTES),
    message(6, &SOURCE_DIGEST),
]);
pub(super) static GENERATION_SOURCE_BASE_V1: Shape = Shape::new(&[message(1, &SOURCE_REFERENCE)]);
pub(super) static PROJECTION_CHECKPOINT_ORDINAL_ALLOCATOR_V1: Shape =
    Shape::new(&[message(3, &UNIT)]);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::v1 as wire;
    use prost::Message;

    fn inspect<M: Message>(message: &M, shape: &Shape) -> Result<(), DurablePreflightError> {
        let mut budget = Budget {
            visited_fields: 0,
            packed_items: 0,
        };
        preflight(&message.encode_to_vec(), shape, 0, &mut budget)
    }

    // req: STO-012, STO-020, PRJ-004
    #[test]
    fn source_wire_shapes_bound_nested_owners_repeated_groups_and_control_keys() {
        let row = wire::StoredProjectionSourceRowV1 {
            identity: Some(wire::ProjectionIdentityV1 {
                contract_lineage: "source_shape_proof".to_owned(),
                projection_id: 1,
                projection_plan_hash: vec![9; 32],
            }),
            locator: Some(wire::ProjectionSourceLocatorV1 {
                database_id: riffdb_types::DatabaseId::from_unix_milliseconds_and_random(
                    42, [7; 10],
                )
                .unwrap()
                .as_bytes()
                .to_vec(),
                allocation_history_incarnation: 2,
                checkpoint_ordinal: 1,
            }),
            canonical_group_values: vec![vec![7]; riffdb_types::MAX_PROJECTION_GROUP_COMPONENTS],
            canonical_measures: vec![8],
            last_changed_sequence: 1,
        };
        assert_eq!(inspect(&row, &PROJECTION_SOURCE_ROW_V1), Ok(()));
        let mut repeated = row.clone();
        repeated.canonical_group_values.push(vec![7]);
        assert_eq!(
            inspect(&repeated, &PROJECTION_SOURCE_ROW_V1),
            Err(DurablePreflightError::LimitExceeded)
        );
        let mut oversized = row.clone();
        oversized.canonical_measures = vec![8; MAX_DOCUMENT_BYTES + 1];
        assert_eq!(
            inspect(&oversized, &PROJECTION_SOURCE_ROW_V1),
            Err(DurablePreflightError::LimitExceeded)
        );
        let mut short_identity = row.clone();
        short_identity
            .identity
            .as_mut()
            .unwrap()
            .projection_plan_hash
            .pop();
        assert_eq!(
            inspect(&short_identity, &PROJECTION_SOURCE_ROW_V1),
            Err(DurablePreflightError::Malformed)
        );
        let mut short_locator = row;
        short_locator.locator.as_mut().unwrap().database_id.pop();
        assert_eq!(
            inspect(&short_locator, &PROJECTION_SOURCE_ROW_V1),
            Err(DurablePreflightError::Malformed)
        );
        let mut maintenance = wire::StoredProjectionSourceMaintenanceV1 {
            descriptor: None,
            expected_prior_root: None,
            copied_rows: 0,
            copied_canonical_row_bytes: 0,
            last_copied_key: Some(vec![7; riffdb_types::MAX_PROJECTION_SOURCE_CONTROL_BYTES]),
            rolling_hash: None,
        };
        assert_eq!(
            inspect(&maintenance, &PROJECTION_SOURCE_MAINTENANCE_V1),
            Ok(())
        );
        maintenance.last_copied_key.as_mut().unwrap().push(7);
        assert_eq!(
            inspect(&maintenance, &PROJECTION_SOURCE_MAINTENANCE_V1),
            Err(DurablePreflightError::LimitExceeded)
        );
        // Shape inspection does not certify an object: these incomplete
        // records still fail the later ordinary semantic reconstruction.
        let mut budget = Budget {
            visited_fields: 0,
            packed_items: 0,
        };
        assert_eq!(
            preflight(
                &[0x0a, 0, 0x0a, 0],
                &PROJECTION_SOURCE_OBJECT_V1,
                0,
                &mut budget
            ),
            Err(DurablePreflightError::NonCanonical)
        );
    }
}
