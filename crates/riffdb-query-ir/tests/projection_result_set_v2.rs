//! Runtime-window bounds retain exact plan identity without hashing values.

use std::num::{NonZeroU16, NonZeroU32};

use riffdb_query_ir::{
    ProjectionResultSetPlanV2, ProjectionResultSetPlanV2Error, ResultSetOutputShapeV1,
    ResultSetWindowBoundsV2,
};
use riffdb_types::{
    ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1, ProjectionProviderKindV1,
    ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1,
};

fn descriptor() -> ProjectionProviderDescriptorV1 {
    ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::ExactText,
        ProjectionProviderPostureV1::Exact,
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::FILTER
            | ProjectionProviderCapabilitiesV1::ORDER
            | ProjectionProviderCapabilitiesV1::MEASURE
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        ProjectionProviderPolicyModeV1::PartitionAligned,
        ProjectionProviderStaticBoundsV1 {
            max_candidates: 4_096,
            max_output_rows: 500,
            max_measures: 1,
            max_input_bytes: 64,
            max_work_units: 100_000,
            max_state_bytes_per_row: 4_194_304,
            max_diagnostic_bytes: 4_096,
            retained_epochs: 8_192,
            max_catchup_lag: 100,
            max_epoch_lease_steps: 1_024,
        },
        ProjectionProviderStateIdentityV1::new(NonZeroU32::new(1).unwrap(), [0x51; 32]),
    )
    .unwrap()
}

#[test]
fn v2_binds_runtime_maxima_without_changing_v1_bytes() {
    let plan = ProjectionResultSetPlanV2::new(
        descriptor(),
        true,
        true,
        true,
        ResultSetWindowBoundsV2::Ordinal {
            max_offset: 4_096,
            max_limit: NonZeroU16::new(500).unwrap(),
        },
        ResultSetOutputShapeV1::TypedRows,
    )
    .unwrap();
    assert_eq!(&plan.to_canonical_bytes()[..6], b"RPRS\0\x02");
    assert_eq!(
        ProjectionResultSetPlanV2::from_canonical_bytes(&plan.to_canonical_bytes()).unwrap(),
        plan
    );
    assert!(
        plan.bind_window(4_096, NonZeroU16::new(500).unwrap())
            .is_ok()
    );
    assert_eq!(
        plan.bind_window(4_097, NonZeroU16::new(1).unwrap()),
        Err(ProjectionResultSetPlanV2Error::WindowExceedsProviderBound)
    );
    assert_eq!(
        plan.bind_window(0, NonZeroU16::new(501).unwrap()),
        Err(ProjectionResultSetPlanV2Error::WindowExceedsProviderBound)
    );
}
