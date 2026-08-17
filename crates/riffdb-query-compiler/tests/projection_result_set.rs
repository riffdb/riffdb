//! Compiler pinning test for one sealed result-set provider.

use std::num::{NonZeroU16, NonZeroU32};

use riffdb_query_compiler::{
    ProjectionResultSetRequirementsV1, pin_projection_result_set_provider_v1,
};
use riffdb_query_ir::{ResultSetOutputShapeV1, ResultSetWindowV1};
use riffdb_types::{
    ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1, ProjectionProviderKindV1,
    ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1,
};

#[test]
fn compiler_pins_the_exact_descriptor_without_a_runtime_choice() {
    let descriptor = ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::Columnar,
        ProjectionProviderPostureV1::Exact,
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::ORDER
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        ProjectionProviderPolicyModeV1::PartitionAligned,
        ProjectionProviderStaticBoundsV1 {
            max_candidates: 500,
            max_output_rows: 50,
            max_measures: 0,
            max_input_bytes: 1_024,
            max_work_units: 50_000,
            max_state_bytes_per_row: 1_024,
            max_diagnostic_bytes: 512,
            retained_epochs: 64,
            max_catchup_lag: 10,
            max_epoch_lease_steps: 100,
        },
        ProjectionProviderStateIdentityV1::new(NonZeroU32::new(1).unwrap(), [0x31; 32]),
    )
    .unwrap();
    let expected = descriptor.digest();
    let plan = pin_projection_result_set_provider_v1(
        descriptor,
        ProjectionResultSetRequirementsV1 {
            filtering: false,
            rank_or_order: true,
            whole_set_measures: false,
            window: ResultSetWindowV1::Top {
                limit: NonZeroU16::new(50).unwrap(),
            },
            output: ResultSetOutputShapeV1::TypedRows,
        },
    )
    .unwrap();
    assert_eq!(plan.provider_digest(), expected);
}
