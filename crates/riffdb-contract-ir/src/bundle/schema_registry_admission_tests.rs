//! Schema registry revalidation preserves ordinary meanings under admission.
// req: OQ-040, PRJ-004
use super::*;
use crate::ProviderSchemaArtifactErrorV1;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

#[test]
fn provider_schema_registry_validation_preserves_frozen_artifacts_and_releases_work() {
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
        bundle
            .validate_provider_schema_artifacts_admitted_v1(&budget)
            .unwrap();
        assert_eq!(bundle.canonical_bytes(), bytes);
        let observation = budget.observation().unwrap();
        assert_eq!(observation.reserved_bytes, 0);
        assert_eq!(observation.owned_bytes, 0);
        assert_eq!(
            observation.released_allocations,
            bundle.schema_artifacts().len() as u64
        );
    }
}

#[test]
fn provider_schema_registry_validation_refuses_pressure_and_preserves_registry_errors() {
    let mut bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    for remaining in [128, 64 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            bundle.validate_provider_schema_artifacts_admitted_v1(&budget),
            Err(ProviderSchemaArtifactErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
    }
    let budget = ProviderBudget::new();
    let expected = IrValidationError::HashMismatch {
        kind: "generated schema artifact registry",
    };
    let count = bundle.schema_artifacts.len();
    assert!(count > 2);
    bundle.schema_artifacts.swap(count - 1, count - 2);
    assert_eq!(
        validate_schema_artifacts(
            &bundle.schema,
            &bundle.commands,
            &bundle.projections,
            &bundle.schema_artifacts
        ),
        Err(expected.clone())
    );
    assert_eq!(
        bundle.validate_provider_schema_artifacts_admitted_v1(&budget),
        Err(ProviderSchemaArtifactErrorV1::Invalid(expected.clone()))
    );
    let observation = budget.observation().unwrap();
    assert_eq!(observation.reserved_bytes, 0);
    assert_eq!(observation.owned_bytes, 0);
    assert!(observation.released_allocations > 1);
    bundle.schema_artifacts.pop();
    let before = budget.observation().unwrap();
    assert_eq!(
        bundle.validate_provider_schema_artifacts_admitted_v1(&budget),
        Err(ProviderSchemaArtifactErrorV1::Invalid(expected))
    );
    assert_eq!(budget.observation().unwrap(), before);
}
