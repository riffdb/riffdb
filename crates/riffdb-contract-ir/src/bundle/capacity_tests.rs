//! Contract ownership witnesses use real canonical compiler fixtures.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::ProviderBudget;
use riffdb_types::{OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionV1};

#[test]
fn bundle_capacity_preserves_all_fixture_bytes_and_shared_lineage_identity() {
    let budget = ProviderBudget::new();
    let _scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/production-embedding/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/cascade/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/vector-ann/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/text-index/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/initialized-state-transition/bundle.bin")
            .as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret-reveal/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/indexed-selection/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/command-decision/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/aggregate-collection-budget/bundle.bin")
            .as_slice(),
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let before = bundle.clone();
        let report = OwnedCapacityInspectionV1::inspect(&bundle).unwrap();
        assert!(report.unshared_bytes() >= 2 * bundle.canonical_bytes.capacity() as u64);
        assert!(!report.shared_allocations().is_empty());
        let clone_report = OwnedCapacityInspectionV1::inspect(&before).unwrap();
        let pair = OwnedCapacityInspectionV1::inspect_with(|inspection| {
            inspection.visit(&bundle)?;
            inspection.visit(&before)
        })
        .unwrap();
        assert_eq!(
            pair.unshared_bytes(),
            report.unshared_bytes() + clone_report.unshared_bytes()
        );
        assert_eq!(
            pair.shared_allocations().len(),
            report.shared_allocations().len()
        );
        assert_eq!(bundle, before);
        assert_eq!(bundle.canonical_bytes(), bytes);
    }
}

#[test]
fn bundle_capacity_counts_unused_canonical_buffer_and_command_vector_capacity() {
    let budget = ProviderBudget::new();
    let _scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let mut bundle = super::tests::empty_bundle();
    let before = OwnedCapacityInspectionV1::inspect(&bundle)
        .unwrap()
        .unshared_bytes();
    let old_capacity = bundle.canonical_bytes.capacity();
    bundle.canonical_bytes.reserve(8192);
    let added_capacity = bundle.canonical_bytes.capacity() - old_capacity;
    let after = OwnedCapacityInspectionV1::inspect(&bundle)
        .unwrap()
        .unshared_bytes();
    assert!(after >= before + 2 * added_capacity as u64);
    let bytes = bundle.canonical_bytes().to_vec();
    bundle.commands.reserve(16);
    let with_commands = OwnedCapacityInspectionV1::inspect(&bundle)
        .unwrap()
        .unshared_bytes();
    assert!(
        with_commands
            >= after + 2 * (bundle.commands.capacity() * std::mem::size_of::<CommandPlan>()) as u64
    );
    assert_eq!(bundle.canonical_bytes(), bytes);
}
