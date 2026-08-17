//! Projection-provider descriptor compatibility and fail-closed tests.

use std::num::NonZeroU32;

use riffdb_types::{
    ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1, ProjectionProviderKindV1,
    ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1, ProjectionProviderValidationError,
    hash_projection_provider_descriptor,
};

fn descriptor() -> ProjectionProviderDescriptorV1 {
    ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::Columnar,
        ProjectionProviderPostureV1::Exact,
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::FILTER
            | ProjectionProviderCapabilitiesV1::ORDER
            | ProjectionProviderCapabilitiesV1::MEASURE
            | ProjectionProviderCapabilitiesV1::FACET
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        ProjectionProviderPolicyModeV1::PartitionAligned,
        ProjectionProviderStaticBoundsV1 {
            max_candidates: 10_000,
            max_output_rows: 500,
            max_measures: 16,
            max_input_bytes: 4_096,
            max_work_units: 200_000,
            max_state_bytes_per_row: 2_048,
            max_diagnostic_bytes: 1_024,
            retained_epochs: 8_192,
            max_catchup_lag: 10_000,
            max_epoch_lease_steps: 250,
        },
        ProjectionProviderStateIdentityV1::new(NonZeroU32::new(1).unwrap(), [0x5a; 32]),
    )
    .unwrap()
}

#[test]
fn descriptor_v1_is_canonical_and_digest_bound() {
    let descriptor = descriptor();
    let bytes = descriptor.to_canonical_bytes();
    assert_eq!(bytes.len(), 112, "V1 is a fixed-size sealed descriptor");
    assert_eq!(
        bytes,
        [
            0x52, 0x50, 0x50, 0x44, 0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0xfb, 0x01, 0x01,
            0x01, 0x00, 0x00, 0x00, 0x27, 0x10, 0x00, 0x00, 0x01, 0xf4, 0x00, 0x10, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x27, 0x10, 0x00, 0x00, 0x00, 0x01, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
            0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
            0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x00, 0x00, 0x10, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x0d, 0x40, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfa, 0x00, 0x00, 0x00, 0x00,
        ]
    );
    assert_eq!(
        descriptor.digest().as_bytes(),
        &[
            0x59, 0x42, 0xf4, 0x18, 0x6c, 0x78, 0xec, 0x84, 0x72, 0x1b, 0xdf, 0x80, 0x8d, 0x5e,
            0x9b, 0xae, 0x4a, 0xde, 0x33, 0x15, 0xa3, 0x77, 0x42, 0x23, 0x18, 0xdb, 0x4b, 0xa8,
            0x8b, 0x95, 0x0f, 0x3b,
        ]
    );
    assert_eq!(
        ProjectionProviderDescriptorV1::from_canonical_bytes(&bytes).unwrap(),
        descriptor
    );
    assert_eq!(
        hash_projection_provider_descriptor(&bytes),
        descriptor.digest()
    );

    let mut missing_epoch_lease = bytes;
    missing_epoch_lease[100..108].fill(0);
    assert_eq!(
        ProjectionProviderDescriptorV1::from_canonical_bytes(&missing_epoch_lease),
        Err(ProjectionProviderValidationError::InvalidBound)
    );
    let mut noncanonical_reserved = bytes;
    noncanonical_reserved[108] = 1;
    assert_eq!(
        ProjectionProviderDescriptorV1::from_canonical_bytes(&noncanonical_reserved),
        Err(ProjectionProviderValidationError::NonCanonicalReservedBytes)
    );

    let mut trailing = bytes.to_vec();
    trailing.push(0);
    assert_eq!(
        ProjectionProviderDescriptorV1::from_canonical_bytes(&trailing),
        Err(ProjectionProviderValidationError::InvalidLength)
    );
}

#[test]
fn approximation_and_provider_capabilities_fail_closed() {
    assert_eq!(
        ProjectionProviderPostureV1::approximate(0),
        Err(ProjectionProviderValidationError::InvalidRecallTarget)
    );
    assert_eq!(
        ProjectionProviderPostureV1::approximate(10_001),
        Err(ProjectionProviderValidationError::InvalidRecallTarget)
    );

    let result = ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::Vector,
        ProjectionProviderPostureV1::Exact,
        ProjectionProviderCapabilitiesV1::FACET,
        ProjectionProviderPolicyModeV1::BoundedRowAdmission,
        ProjectionProviderStaticBoundsV1 {
            max_candidates: 100,
            max_output_rows: 10,
            max_measures: 1,
            max_input_bytes: 100,
            max_work_units: 1_000,
            max_state_bytes_per_row: 100,
            max_diagnostic_bytes: 100,
            retained_epochs: 100,
            max_catchup_lag: 100,
            max_epoch_lease_steps: 100,
        },
        ProjectionProviderStateIdentityV1::new(NonZeroU32::new(1).unwrap(), [7; 32]),
    );
    assert_eq!(
        result,
        Err(ProjectionProviderValidationError::UnsupportedCapability)
    );
}
