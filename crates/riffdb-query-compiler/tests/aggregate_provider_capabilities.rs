//! Exact aggregate provider subsets remain closed and provider-specific.

use std::num::NonZeroU32;

use riffdb_types::{
    AggregateSemanticIdentityV1, ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1,
    ProjectionProviderKindV1, ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1,
    ProjectionProviderStateIdentityV1, ProjectionProviderStaticBoundsV1,
    aggregate_semantic_registry_v1,
};

fn descriptor(
    kind: ProjectionProviderKindV1,
    capabilities: ProjectionProviderCapabilitiesV1,
    max_measures: u16,
) -> ProjectionProviderDescriptorV1 {
    ProjectionProviderDescriptorV1::new(
        kind,
        ProjectionProviderPostureV1::Exact,
        capabilities,
        ProjectionProviderPolicyModeV1::PartitionAligned,
        ProjectionProviderStaticBoundsV1 {
            max_candidates: 500,
            max_output_rows: 50,
            max_measures,
            max_input_bytes: 1_024,
            max_work_units: 50_000,
            max_state_bytes_per_row: 1_024,
            max_diagnostic_bytes: 512,
            retained_epochs: 100,
            max_catchup_lag: 10,
            max_epoch_lease_steps: 100,
        },
        ProjectionProviderStateIdentityV1::new(NonZeroU32::new(1).unwrap(), [kind as u8; 32]),
    )
    .unwrap()
}

#[test]
fn real_provider_subsets_never_become_their_union() {
    let base = ProjectionProviderCapabilitiesV1::CANDIDATE
        | ProjectionProviderCapabilitiesV1::FILTER
        | ProjectionProviderCapabilitiesV1::WINDOW
        | ProjectionProviderCapabilitiesV1::OUTPUT;
    let columnar = descriptor(
        ProjectionProviderKindV1::Columnar,
        base | ProjectionProviderCapabilitiesV1::ORDER | ProjectionProviderCapabilitiesV1::MEASURE,
        16,
    );
    let exact = descriptor(
        ProjectionProviderKindV1::ExactText,
        base | ProjectionProviderCapabilitiesV1::ORDER | ProjectionProviderCapabilitiesV1::MEASURE,
        1,
    );
    let vector = descriptor(
        ProjectionProviderKindV1::Vector,
        base | ProjectionProviderCapabilitiesV1::RANK,
        0,
    );

    for semantic in aggregate_semantic_registry_v1()
        .iter()
        .map(|entry| entry.identity())
    {
        assert_eq!(
            columnar.aggregate_semantics().contains(semantic),
            semantic != AggregateSemanticIdentityV1::ExactCount
        );
        assert_eq!(
            exact.aggregate_semantics().contains(semantic),
            semantic == AggregateSemanticIdentityV1::ExactCount
        );
        assert!(!vector.aggregate_semantics().contains(semantic));
    }
    assert_eq!(columnar.aggregate_semantics().len(), 10);
    assert_eq!(exact.aggregate_semantics().len(), 1);
    assert!(vector.aggregate_semantics().is_empty());
}

#[test]
fn aggregate_advertisement_requires_the_sealed_measure_stage() {
    let columnar = descriptor(
        ProjectionProviderKindV1::Columnar,
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::ORDER
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        0,
    );
    assert!(columnar.aggregate_semantics().is_empty());

    let bytes = columnar.to_canonical_bytes();
    let decoded = ProjectionProviderDescriptorV1::from_canonical_bytes(&bytes).unwrap();
    assert_eq!(decoded, columnar);
    assert_eq!(decoded.digest(), columnar.digest());
    assert!(decoded.aggregate_semantics().is_empty());
}
