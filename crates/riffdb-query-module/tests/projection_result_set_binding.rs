//! Named result-set binding framing and strict decode tests.

use std::num::{NonZeroU16, NonZeroU32};

use riffdb_query_module::{
    ProjectionResultSetBindingV1, ProjectionResultSetPlanV1, ResultSetOutputShapeV1,
    ResultSetWindowV1,
};
use riffdb_types::{
    ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1, ProjectionProviderKindV1,
    ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1, QueryOperationName,
};

#[test]
fn named_binding_v1_is_canonical_complete_and_strict() {
    let provider = ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::Vector,
        ProjectionProviderPostureV1::Exact,
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::RANK
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        ProjectionProviderPolicyModeV1::BoundedRowAdmission,
        ProjectionProviderStaticBoundsV1 {
            max_candidates: 100,
            max_output_rows: 10,
            max_measures: 0,
            max_input_bytes: 1_024,
            max_work_units: 10_000,
            max_state_bytes_per_row: 1_024,
            max_diagnostic_bytes: 512,
            retained_epochs: 20,
            max_catchup_lag: 5,
            max_epoch_lease_steps: 50,
        },
        ProjectionProviderStateIdentityV1::new(NonZeroU32::new(1).unwrap(), [0x72; 32]),
    )
    .unwrap();
    let plan = ProjectionResultSetPlanV1::new(
        provider,
        false,
        true,
        false,
        ResultSetWindowV1::Top {
            limit: NonZeroU16::new(10).unwrap(),
        },
        ResultSetOutputShapeV1::TypedRows,
    )
    .unwrap();
    let binding = ProjectionResultSetBindingV1::new(
        QueryOperationName::new("NearestDocuments").unwrap(),
        plan,
    );
    let bytes = binding.to_canonical_bytes();
    assert_eq!(&bytes[..6], b"RPRB\0\x01");
    assert_eq!(&bytes[6..8], &16_u16.to_be_bytes());
    assert_eq!(&bytes[8..24], b"NearestDocuments");
    assert_eq!(
        ProjectionResultSetBindingV1::from_canonical_bytes(&bytes).unwrap(),
        binding
    );

    let mut trailing = bytes;
    trailing.push(0);
    assert!(ProjectionResultSetBindingV1::from_canonical_bytes(&trailing).is_err());
}
