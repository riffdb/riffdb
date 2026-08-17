//! Compiler-owned result-set plan compatibility tests.

use riffdb_query_ir::{ProjectionResultSetPlanV1, ResultSetOutputShapeV1, ResultSetWindowV1};
use riffdb_types::{
    ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1, ProjectionProviderKindV1,
    ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1, hash_query_plan,
};
use std::num::{NonZeroU16, NonZeroU32};

fn descriptor() -> ProjectionProviderDescriptorV1 {
    ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::Columnar,
        ProjectionProviderPostureV1::Exact,
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::FILTER
            | ProjectionProviderCapabilitiesV1::ORDER
            | ProjectionProviderCapabilitiesV1::MEASURE
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        ProjectionProviderPolicyModeV1::PartitionAligned,
        bounds(4),
        ProjectionProviderStateIdentityV1::new(NonZeroU32::new(1).unwrap(), [3; 32]),
    )
    .unwrap()
}

fn bounds(max_measures: u16) -> ProjectionProviderStaticBoundsV1 {
    ProjectionProviderStaticBoundsV1 {
        max_candidates: 1_000,
        max_output_rows: 100,
        max_measures,
        max_input_bytes: 4_096,
        max_work_units: 100_000,
        max_state_bytes_per_row: 2_048,
        max_diagnostic_bytes: 1_024,
        retained_epochs: 2_000,
        max_catchup_lag: 100,
        max_epoch_lease_steps: 500,
    }
}

#[test]
fn result_set_plan_pins_one_provider_and_the_six_stage_order() {
    let descriptor = descriptor();
    let plan = ProjectionResultSetPlanV1::new(
        descriptor.clone(),
        true,
        true,
        true,
        ResultSetWindowV1::Ordinal {
            offset: 50,
            limit: NonZeroU16::new(50).unwrap(),
        },
        ResultSetOutputShapeV1::TypedRows,
    )
    .unwrap();

    assert_eq!(plan.provider_digest(), descriptor.digest());
    assert_eq!(
        plan.stage_names(),
        [
            "candidates",
            "policy_filter",
            "rank_order",
            "whole_set_measures",
            "window",
            "typed_output"
        ]
    );
    assert_eq!(
        ProjectionResultSetPlanV1::from_canonical_bytes(&plan.to_canonical_bytes()).unwrap(),
        plan
    );
    let bytes = plan.to_canonical_bytes();
    assert_eq!(plan.identity(), hash_query_plan(&bytes));
    assert_eq!(&bytes[..6], b"RPRS\0\x01");
    assert_eq!(&bytes[6..118], &descriptor.to_canonical_bytes());
    assert_eq!(&bytes[118..150], descriptor.digest().as_bytes());
    assert_eq!(bytes[150], 0b0000_0111);
    assert_eq!(bytes[151], 2);
    assert_eq!(&bytes[152..156], &50_u32.to_be_bytes());
    assert_eq!(&bytes[156..158], &50_u16.to_be_bytes());
    assert_eq!(bytes[158], 1);
    assert_eq!(bytes[159], 0);
}

#[test]
fn unsupported_stage_is_rejected_before_execution() {
    let descriptor = ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::Vector,
        ProjectionProviderPostureV1::approximate(9_000).unwrap(),
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::RANK
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        ProjectionProviderPolicyModeV1::BoundedRowAdmission,
        bounds(1),
        ProjectionProviderStateIdentityV1::new(NonZeroU32::new(1).unwrap(), [4; 32]),
    )
    .unwrap();

    assert!(
        ProjectionResultSetPlanV1::new(
            descriptor,
            true,
            false,
            true,
            ResultSetWindowV1::Top {
                limit: NonZeroU16::new(10).unwrap(),
            },
            ResultSetOutputShapeV1::TypedRows,
        )
        .is_err()
    );
}
