//! Provider plan hashes require admission before dependency-set construction.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

#[test]
fn provider_plan_hash_validation_preserves_frozen_hashes_and_releases_workspace() {
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
        let budget = ProviderBudget::new();
        {
            let _inspection = budget
                .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
                .unwrap();
            for command in bundle.commands() {
                let maximum =
                    plan_hash_admission::closure_bound(bundle.schema(), Some(command)).unwrap();
                let _work = budget.reserve(maximum).unwrap();
                let forest = collect_command_enum_forest(command, bundle.schema()).unwrap();
                let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&forest).unwrap();
                assert!(inventory.shared_allocations().is_empty());
                assert!(inventory.unshared_bytes() <= maximum);
            }
            for projection in bundle.projections() {
                let maximum = plan_hash_admission::closure_bound(bundle.schema(), None).unwrap();
                let _work = budget.reserve(maximum).unwrap();
                let source = bundle.schema().event(projection.source_event()).unwrap();
                let forest =
                    collect_projection_enum_forest(projection, source, bundle.schema()).unwrap();
                let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&forest).unwrap();
                assert!(inventory.shared_allocations().is_empty());
                assert!(inventory.unshared_bytes() <= maximum);
            }
        }
        bundle
            .validate_provider_plan_hashes_admitted_v1(&budget)
            .unwrap();
        let observation = budget.observation().unwrap();
        assert_eq!(observation.reserved_bytes, 0);
        assert_eq!(observation.owned_bytes, 0);
    }
}

#[test]
fn provider_plan_hash_validation_refuses_before_dependency_traversal() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let budget = ProviderBudget::new();
    let fixed = budget.observation().unwrap().fixed_owner_bytes;
    let pressure = budget
        .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - 128)
        .unwrap();
    let before = budget.observation().unwrap();
    assert!(matches!(
        bundle.validate_provider_plan_hashes_admitted_v1(&budget),
        Err(ProviderBundleEncodingErrorV1::Unavailable)
    ));
    assert_eq!(budget.observation().unwrap(), before);
    drop(pressure);
}

#[test]
fn provider_plan_hash_validation_refuses_after_dependency_forest_fits() {
    let mut bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let command_maximum = plan_hash_admission::closure_bound(
        bundle.schema(),
        Some(bundle.commands().first().unwrap()),
    )
    .unwrap();
    let projection_maximum = plan_hash_admission::closure_bound(bundle.schema(), None).unwrap();
    for (projection_only, maximum) in [(false, command_maximum), (true, projection_maximum)] {
        if projection_only {
            bundle.commands.clear();
            assert!(!bundle.projections.is_empty());
        }
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - maximum)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            bundle.validate_provider_plan_hashes_admitted_v1(&budget),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
        bundle
            .validate_provider_plan_hashes_admitted_v1(&budget)
            .unwrap();
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
