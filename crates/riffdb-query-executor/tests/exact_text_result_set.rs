//! One-proof exact count and ordinal execution conformance.

use std::num::NonZeroU16;

use riffdb_projection::{
    ExactTextIndexMutationV2, ExactTextPartitionIndexV2, ProviderEpochObservationV1,
    ProviderLifecycleV1, ResultSetEpochContextV1, ResultSetEpochRequirementV1,
    negotiate_result_set_epoch_v1,
};
use riffdb_query_compiler::{
    ExactTextCompilerDeclarationV1, ProjectionResultSetRequirementsV2,
    compile_exact_text_result_family_v1, pin_projection_result_set_provider_v2,
};
use riffdb_query_executor::{ExactTextResultSetErrorV1, execute_exact_text_result_set_v1};
use riffdb_query_ir::{ResultSetOutputShapeV1, ResultSetWindowBoundsV2};
use riffdb_riffql_syntax::Span;
use riffdb_types::{
    ApplicationRoleHash, CanonicalRecord, CanonicalValue, CommitSequence, EntityKey,
    EntityKeyBuilder, EntityTypeId, ExactTextOperatorV1, ExactTextOrderV1, ExactTextProfileV1,
    FieldId, PartitionKeyHash, ProjectionGeneration, ProjectionProviderPolicyModeV1,
};

fn seq(value: u64) -> CommitSequence {
    CommitSequence::new(value).unwrap()
}

fn row(value: u8) -> EntityKey {
    let mut builder = EntityKeyBuilder::new(EntityTypeId::first());
    builder.push_u32(u32::from(value)).unwrap();
    builder.finish().unwrap()
}

fn output(value: u64) -> CanonicalRecord {
    CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(value))]).unwrap()
}

#[test]
fn count_page_and_epoch_identity_are_one_atomic_result_contract() {
    let family =
        compile_exact_text_result_family_v1(ExactTextCompilerDeclarationV1::bounded_binary_utf8(
            FieldId::new(3).unwrap(),
            [true, true, true, true],
            ProjectionProviderPolicyModeV1::PartitionAligned,
            Span { start: 10, end: 30 },
        ))
        .unwrap();
    let plan = pin_projection_result_set_provider_v2(
        family.descriptor().clone(),
        ProjectionResultSetRequirementsV2 {
            filtering: true,
            rank_or_order: true,
            whole_set_measures: true,
            window: ResultSetWindowBoundsV2::Ordinal {
                max_offset: family.max_candidates(),
                max_limit: NonZeroU16::new(500).unwrap(),
            },
            output: ResultSetOutputShapeV1::TypedRows,
        },
    )
    .unwrap();
    let partition = PartitionKeyHash::from_bytes([0x44; 32]);
    let generation = ProjectionGeneration::new(7).unwrap();
    let mut index =
        ExactTextPartitionIndexV2::new(partition, generation, ExactTextProfileV1::BinaryUtf8V1);
    index
        .apply(
            seq(20),
            &[
                ExactTextIndexMutationV2::upsert(row(1), "gamma needle", output(1)).unwrap(),
                ExactTextIndexMutationV2::upsert(row(2), "alpha needle", output(2)).unwrap(),
                ExactTextIndexMutationV2::upsert(row(3), "beta needle", output(3)).unwrap(),
            ],
        )
        .unwrap();
    let role = ApplicationRoleHash::from_bytes([0x55; 32]);
    let observation = ProviderEpochObservationV1::new(
        plan.provider_digest(),
        plan.provider().state_identity().schema_hash(),
        9,
        generation,
        seq(1),
        seq(20),
        ProviderLifecycleV1::Ready,
    )
    .unwrap();
    let proof = negotiate_result_set_epoch_v1(
        ResultSetEpochContextV1::new(plan.identity(), role),
        &[observation],
        ResultSetEpochRequirementV1::Latest,
    )
    .unwrap();
    let needle = ExactTextProfileV1::BinaryUtf8V1
        .bind_needle("needle")
        .unwrap();
    let result = execute_exact_text_result_set_v1(
        &plan,
        &family,
        &proof,
        &index,
        ExactTextOperatorV1::Contains,
        ExactTextOrderV1::ValueAscEntityKey,
        &needle,
        1,
        NonZeroU16::new(2).unwrap(),
    )
    .unwrap();
    assert_eq!(result.exact_total(), 3);
    assert_eq!(result.rows()[0].key(), &row(3));
    assert_eq!(result.rows()[0].output(), &output(3));
    assert_eq!(result.rows()[1].key(), &row(1));
    assert_eq!(result.rows()[1].output(), &output(1));
    assert_eq!(result.partition(), partition);
    assert_eq!(result.plan_identity(), plan.identity());
    assert_eq!(result.policy_shape_identity(), role);
    assert_eq!(result.generation(), generation);
    assert_eq!(result.history_incarnation(), 9);
    assert_eq!(result.epoch(), seq(20));

    index
        .apply(
            seq(21),
            &[ExactTextIndexMutationV2::upsert(row(4), "delta needle", output(4)).unwrap()],
        )
        .unwrap();
    assert_eq!(
        execute_exact_text_result_set_v1(
            &plan,
            &family,
            &proof,
            &index,
            ExactTextOperatorV1::Contains,
            ExactTextOrderV1::ValueAscEntityKey,
            &needle,
            1,
            NonZeroU16::new(2).unwrap(),
        ),
        Err(ExactTextResultSetErrorV1::SnapshotChanged)
    );
}
