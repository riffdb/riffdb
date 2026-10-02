// req: OQ-040
use crate::{
    ExactPredicatePartitionIndexV4, ExactPredicatePartitionIndexV5, ExactTextPartitionIndexV2,
    ExactTextPartitionIndexV3, LongPatternPartitionV1, TokenizedTextPartitionIndexV1,
};
use riffdb_types::exact_provider_budget_v1::ProviderBudget;
use riffdb_types::{
    ExactTextProfileV1, InspectOwnedCapacityV1, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    OwnedCapacityInspectionV1, PartitionKeyHash, ProjectionGeneration,
};

#[test]
fn every_physical_exact_provider_has_an_exhaustive_capacity_inventory() {
    fn inventory<T: InspectOwnedCapacityV1>() {}
    inventory::<ExactTextPartitionIndexV2>();
    inventory::<ExactTextPartitionIndexV3>();
    inventory::<ExactPredicatePartitionIndexV4>();
    inventory::<ExactPredicatePartitionIndexV5>();
    inventory::<TokenizedTextPartitionIndexV1>();
    inventory::<LongPatternPartitionV1>();
}

#[test]
fn provider_capacity_includes_retained_tree_roots_without_mutating_checkpoint_identity() {
    let mut provider = ExactTextPartitionIndexV2::new(
        PartitionKeyHash::from_bytes([1; 32]),
        ProjectionGeneration::new(1).unwrap(),
        ExactTextProfileV1::BinaryUtf8V1,
    );
    provider
        .apply(riffdb_types::CommitSequence::new(1).unwrap(), &[])
        .unwrap();
    let before = provider.to_checkpoint_bytes().unwrap();
    let budget = ProviderBudget::new();
    let scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let report = OwnedCapacityInspectionV1::inspect(&provider).unwrap();
    assert!(report.unshared_bytes() > u64::try_from(before.len()).unwrap());
    assert!(report.shared_allocations().is_empty());
    assert_eq!(provider.to_checkpoint_bytes().unwrap(), before);
    drop(report);
    drop(scratch);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn a_small_checkpoint_still_accounts_for_derived_substring_postings() {
    let mut provider = ExactTextPartitionIndexV2::new(
        PartitionKeyHash::from_bytes([1; 32]),
        ProjectionGeneration::new(1).unwrap(),
        ExactTextProfileV1::BinaryUtf8V1,
    );
    let budget = ProviderBudget::new();
    let scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let empty = OwnedCapacityInspectionV1::inspect(&provider)
        .unwrap()
        .unshared_bytes();
    let mutation = crate::ExactTextIndexMutationV2::upsert(
        key(),
        "abcdefghijklmnopqrstuvwx",
        riffdb_types::CanonicalRecord::new(vec![]).unwrap(),
    )
    .unwrap();
    provider
        .apply(riffdb_types::CommitSequence::new(1).unwrap(), &[mutation])
        .unwrap();
    let bytes = provider.to_checkpoint_bytes().unwrap();
    let report = OwnedCapacityInspectionV1::inspect(&provider).unwrap();
    assert!(report.unshared_bytes() > empty + u64::try_from(bytes.len()).unwrap() * 10);
    let recovered = ExactTextPartitionIndexV2::from_checkpoint_bytes(&bytes).unwrap();
    let recovered_report = OwnedCapacityInspectionV1::inspect(&recovered).unwrap();
    assert!(recovered_report.unshared_bytes() > u64::try_from(bytes.len()).unwrap() * 10);
    assert_eq!(recovered.to_checkpoint_bytes().unwrap(), bytes);
    assert_eq!(provider, recovered);
    drop(recovered_report);
    drop(report);
    drop(scratch);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

fn key() -> riffdb_types::EntityKey {
    let mut builder =
        riffdb_types::EntityKeyBuilder::new(riffdb_types::EntityTypeId::new(1).unwrap());
    builder.push_u32(1).unwrap();
    builder.finish().unwrap()
}

#[test]
fn predicate_cells_account_for_empty_retained_scalar_buffers() {
    let text = String::with_capacity(4096);
    let bytes = Vec::<u8>::with_capacity(8192);
    let cells = [
        riffdb_query_ir::ExactReferenceCellV1::Value(riffdb_query_ir::ExactScalarV1::String(text)),
        riffdb_query_ir::ExactReferenceCellV1::Value(riffdb_query_ir::ExactScalarV1::Bytes(bytes)),
    ];
    let budget = ProviderBudget::new();
    let scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let report = OwnedCapacityInspectionV1::inspect(&cells).unwrap();
    assert!(report.unshared_bytes() >= 4096 + 8192);
    assert!(report.shared_allocations().is_empty());
    assert_eq!(
        cells[0],
        riffdb_query_ir::ExactReferenceCellV1::Value(riffdb_query_ir::ExactScalarV1::String(
            String::new()
        ))
    );
    assert_eq!(
        cells[1],
        riffdb_query_ir::ExactReferenceCellV1::Value(riffdb_query_ir::ExactScalarV1::Bytes(
            Vec::new()
        ))
    );
    drop(report);
    drop(scratch);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
