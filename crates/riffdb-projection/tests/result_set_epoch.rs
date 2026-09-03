//! Result-set epoch negotiation and deterministic health conformance.

use riffdb_projection::{
    ProviderEpochObservationV1, ProviderLifecycleV1, ResultSetEpochContextV1, ResultSetEpochError,
    ResultSetEpochRequirementV1, ResultSetLagHealthV1, ResultSetLagMonitorV1,
    negotiate_result_set_epoch_v1,
};
use riffdb_types::{
    ApplicationRoleHash, CommitSequence, ProjectionGeneration, ProjectionProviderDescriptorHash,
    QueryPlanHash,
};

fn seq(value: u64) -> CommitSequence {
    CommitSequence::new(value).unwrap()
}

fn context() -> ResultSetEpochContextV1 {
    ResultSetEpochContextV1::new(
        QueryPlanHash::from_bytes([0xa1; 32]),
        ApplicationRoleHash::from_bytes([0xb2; 32]),
    )
}

fn observation(
    descriptor: u8,
    incarnation: u64,
    generation: u64,
    floor: u64,
    ceiling: u64,
) -> ProviderEpochObservationV1 {
    observation_with_lifecycle(
        descriptor,
        incarnation,
        generation,
        floor,
        ceiling,
        ProviderLifecycleV1::Ready,
    )
}

fn observation_with_lifecycle(
    descriptor: u8,
    incarnation: u64,
    generation: u64,
    floor: u64,
    ceiling: u64,
    lifecycle: ProviderLifecycleV1,
) -> ProviderEpochObservationV1 {
    ProviderEpochObservationV1::new(
        ProjectionProviderDescriptorHash::from_bytes([descriptor; 32]),
        [descriptor.wrapping_add(1); 32],
        incarnation,
        ProjectionGeneration::new(generation).unwrap(),
        seq(floor),
        seq(ceiling),
        lifecycle,
    )
    .unwrap()
}

#[test]
// req: OQ-020
fn newest_common_epoch_is_bound_to_every_provider_generation() {
    let providers = [observation(1, 7, 2, 20, 90), observation(2, 7, 4, 40, 80)];
    let proof = negotiate_result_set_epoch_v1(
        context(),
        &providers,
        ResultSetEpochRequirementV1::AtLeast(seq(60)),
    )
    .unwrap();
    assert_eq!(proof.selected_epoch(), seq(80));
    assert_eq!(proof.participants().len(), 2);
    assert_eq!(proof.plan_identity(), QueryPlanHash::from_bytes([0xa1; 32]));
    assert_eq!(
        proof.policy_shape_identity(),
        ApplicationRoleHash::from_bytes([0xb2; 32])
    );
    let resumed = negotiate_result_set_epoch_v1(
        context(),
        &providers,
        ResultSetEpochRequirementV1::Exact(seq(60)),
    )
    .unwrap();
    assert_eq!(resumed.selected_epoch(), seq(60));

    let no_intersection = [observation(1, 7, 2, 81, 90), observation(2, 7, 4, 40, 80)];
    assert_eq!(
        negotiate_result_set_epoch_v1(
            context(),
            &no_intersection,
            ResultSetEpochRequirementV1::Latest
        ),
        Err(ResultSetEpochError::Diverged)
    );
}

#[test]
fn incarnation_retention_and_lifecycle_fail_closed() {
    let mismatch = [observation(1, 7, 2, 20, 90), observation(2, 8, 4, 40, 80)];
    assert_eq!(
        negotiate_result_set_epoch_v1(context(), &mismatch, ResultSetEpochRequirementV1::Latest),
        Err(ResultSetEpochError::IncarnationMismatch)
    );

    let retired = observation_with_lifecycle(1, 7, 2, 20, 90, ProviderLifecycleV1::Retired);
    assert_eq!(
        negotiate_result_set_epoch_v1(context(), &[retired], ResultSetEpochRequirementV1::Latest),
        Err(ResultSetEpochError::Retired)
    );

    assert_eq!(
        negotiate_result_set_epoch_v1(
            context(),
            &[observation(1, 7, 2, 40, 80)],
            ResultSetEpochRequirementV1::Exact(seq(30))
        ),
        Err(ResultSetEpochError::EpochExpired)
    );
}

#[test]
fn sustained_admitted_lag_degrades_then_refuses_normal_success() {
    let mut monitor = ResultSetLagMonitorV1::new(10, 2, 4).unwrap();
    assert_eq!(monitor.observe(true, 11), ResultSetLagHealthV1::Ready);
    assert_eq!(monitor.observe(true, 11), ResultSetLagHealthV1::Degraded);
    assert_eq!(monitor.observe(false, 100), ResultSetLagHealthV1::Degraded);
    assert_eq!(monitor.observe(true, 11), ResultSetLagHealthV1::Degraded);
    assert_eq!(monitor.observe(true, 11), ResultSetLagHealthV1::Unavailable);
    assert_eq!(monitor.observe(true, 2), ResultSetLagHealthV1::Ready);
}
