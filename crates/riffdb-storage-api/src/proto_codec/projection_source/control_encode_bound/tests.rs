//! Future source-control length is syntax only, never publication authority.
use super::*;
use crate::{AuthoritativeTransactionBindingV3, ChangelogTransactionSequence};
use riffdb_types::{
    AdministrationSequence, CommitSequence, ContentDigest, ContractLineage, DatabaseId,
    DualFrontier, HashDomain, ProjectionCheckpointOrdinal, ProjectionId, ProjectionIdentity,
    ProjectionPlanHash, ProjectionSourceLocatorV1,
};
fn reference(width: u64, lineage: &str, projection: u32) -> ProjectionSourceReferenceV1 {
    let database = DatabaseId::from_unix_milliseconds_and_random(42, [7; 10]).unwrap();
    ProjectionSourceReferenceV1::new(
        ProjectionIdentity::new(
            ContractLineage::new(lineage).unwrap(),
            ProjectionId::new(projection).unwrap(),
            ProjectionPlanHash::from_bytes([9; 32]),
        ),
        ProjectionSourceLocatorV1::new(
            database,
            width,
            ProjectionCheckpointOrdinal::new(width).unwrap(),
        )
        .unwrap(),
        ContentDigest::from_stored_parts(1, HashDomain::ProjectionSourceDescriptor, [4; 32])
            .unwrap(),
        ContentDigest::from_stored_parts(1, HashDomain::ProjectionSourceOrigin, [5; 32]).unwrap(),
    )
    .unwrap()
}
fn root(reference: &ProjectionSourceReferenceV1, maximum: bool) -> ProjectionSourceRootV1 {
    let (predecessor, sequence, application, prior_admin, assigned) = if maximum {
        (
            u64::MAX - 1,
            u64::MAX,
            u64::MAX,
            Some(AdministrationSequence::new(u64::MAX - 1).unwrap()),
            AdministrationSequence::new(u64::MAX).unwrap(),
        )
    } else {
        (1, 2, 1, None, AdministrationSequence::first())
    };
    let application = Some(CommitSequence::new(application).unwrap());
    ProjectionSourceRootV1::new(
        reference.clone(),
        AuthoritativeTransactionBindingV3 {
            database_id: reference.locator().database_id(),
            history_incarnation: reference.locator().allocation_history_incarnation(),
            predecessor: Some(ChangelogTransactionSequence::new(predecessor).unwrap()),
            sequence: ChangelogTransactionSequence::new(sequence).unwrap(),
            predecessor_frontier: DualFrontier::new(application, prior_admin),
            covered_frontier: DualFrontier::new(application, Some(assigned)),
            prior_history_hash: [6; 32],
        },
        Some(assigned),
    )
    .unwrap()
}
// req: STO-012, PRJ-004
#[test]
fn future_source_root_bound_equals_maximum_legal_publication_and_covers_narrow_scalars() {
    let wide = "x".repeat(riffdb_types::MAX_CONTRACT_LINEAGE_BYTES);
    for width in [1, 127, 128, 16_383, 16_384, u64::MAX] {
        for (lineage, projection) in [("x", 1), (wide.as_str(), u32::MAX)] {
            let reference = reference(width, lineage, projection);
            let bound = projection_source_root_future_encoded_bound_v1(&reference).unwrap();
            let maximum = encode_projection_source_root_v1(&root(&reference, true)).unwrap();
            assert_eq!(
                bound.get(),
                maximum.as_bytes().len(),
                "exact same reference and every legal future publication/admin scalar at its largest width"
            );
            let narrow = encode_projection_source_root_v1(&root(&reference, false)).unwrap();
            assert!(narrow.as_bytes().len() <= bound.get());
            assert!(
                bound.get() < 1024,
                "actual full reference bound avoids a blanket4KiB root reservation"
            );
        }
    }
}
// req: STO-020, PRJ-004
#[test]
fn future_source_root_bound_is_quota_independent_and_arithmetic_refuses_overflow() {
    let reference = reference(
        u64::MAX,
        &"x".repeat(riffdb_types::MAX_CONTRACT_LINEAGE_BYTES),
        u32::MAX,
    );
    let expected = projection_source_root_future_encoded_bound_v1(&reference).unwrap();
    let budget = riffdb_types::ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.owned_bytes
        - before.reserved_bytes;
    let pressure = budget.reserve(free).unwrap();
    let saturated = budget.observation().unwrap();
    assert_eq!(
        projection_source_root_future_encoded_bound_v1(&reference).unwrap(),
        expected
    );
    assert_eq!(budget.observation().unwrap(), saturated);
    assert!(framed_len(usize::MAX).is_err());
    let mut value = usize::MAX;
    assert!(add(&mut value, 1).is_err());
    assert!(
        ContractLineage::new("x".repeat(riffdb_types::MAX_CONTRACT_LINEAGE_BYTES + 1)).is_err()
    );
    drop(pressure);
    assert_eq!(budget.observation().unwrap(), before);
}
