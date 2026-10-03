// req: OQ-040, PRJ-004
//! Runtime-only partition generation proofs preserve ordinary epoch negotiation.

use riffdb_projection::{
    ResultSetEpochContextV1, ResultSetEpochRequirementV1, negotiate_result_set_epoch_v1,
};
use riffdb_query_executor::LongPatternCandidateBatch;
use riffdb_types::{
    ApplicationRoleHash, CommitSequence, ProjectionGeneration, ProjectionProviderDescriptorHash,
    QueryPlanHash,
};

fn atomic(generation: u64, floor: u64, ceiling: u64) -> LongPatternCandidateBatch {
    LongPatternCandidateBatch::checked(
        "source".to_owned(),
        vec![],
        ProjectionProviderDescriptorHash::from_bytes([1; 32]),
        [2; 32],
        1,
        ProjectionGeneration::new(generation).unwrap(),
        CommitSequence::new(floor).unwrap(),
        CommitSequence::new(ceiling).unwrap(),
        0,
        0,
    )
    .unwrap()
}

fn merge(batches: Vec<LongPatternCandidateBatch>) -> LongPatternCandidateBatch {
    LongPatternCandidateBatch::merge_partition_batches(batches, 0, 0, 0).unwrap()
}

#[test]
fn nested_partition_generation_proofs_bind_every_constituent() {
    let expected = merge(vec![
        merge(vec![atomic(1, 1, 7), atomic(2, 4, 9)]),
        atomic(3, 2, 8),
    ]);
    let substituted = merge(vec![
        merge(vec![atomic(4, 1, 7), atomic(2, 4, 9)]),
        atomic(3, 2, 8),
    ]);
    let expected = expected.observation().unwrap();
    let substituted = substituted.observation().unwrap();
    assert_eq!(expected.generation(), substituted.generation());
    assert_eq!(expected.floor(), substituted.floor());
    assert_eq!(expected.ceiling(), substituted.ceiling());
    assert_ne!(
        expected.partition_generations(),
        substituted.partition_generations()
    );
    assert_ne!(expected, substituted);
    let proof = negotiate_result_set_epoch_v1(
        ResultSetEpochContextV1::new(
            QueryPlanHash::from_bytes([3; 32]),
            ApplicationRoleHash::from_bytes([4; 32]),
        ),
        &[expected],
        ResultSetEpochRequirementV1::Latest,
    )
    .unwrap();
    assert_eq!(proof.selected_epoch(), CommitSequence::new(7).unwrap());
    assert_eq!(proof.participants().next(), Some(&expected));
    assert_ne!(proof.participants().next(), Some(&substituted));
}

#[test]
fn one_partition_preserves_atomic_or_complete_merged_observation() {
    for batch in [
        atomic(1, 1, 7),
        merge(vec![atomic(1, 1, 7), atomic(2, 1, 7)]),
    ] {
        let observation = batch.observation().unwrap();
        assert_eq!(merge(vec![batch]).observation().unwrap(), observation);
    }
}

#[test]
fn independent_generations_still_require_a_common_servable_epoch() {
    assert!(
        LongPatternCandidateBatch::merge_partition_batches(
            vec![atomic(1, 1, 2), atomic(2, 3, 4)],
            0,
            0,
            0,
        )
        .is_none()
    );
    assert!(LongPatternCandidateBatch::merge_partition_batches(vec![], 0, 0, 0).is_none());
}
