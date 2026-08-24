//! Pay-once plan/epoch proof for ADR-0134 indexed result execution.

use std::collections::BTreeMap;
use std::num::NonZeroU16;

use riffdb_projection::{
    ExactPredicatePartitionIndexV4, ExactPredicatePartitionIndexV5,
    ExactPredicateProviderBindingV1, ExactPredicateProviderBindingV2, ExactPredicateProviderRowV1,
    ProviderEpochObservationV1, ProviderLifecycleV1, ResultSetEpochContextV1,
    ResultSetEpochRequirementV1, negotiate_result_set_epoch_v1,
};
use riffdb_query_executor::{
    ExactPredicateResultSetErrorV1, execute_exact_predicate_result_set_v1,
    execute_nullable_exact_predicate_result_set_v1,
};
use riffdb_query_ir::{
    ExactComparisonProfileV1, ExactOrderDirectionV1, ExactOrderProgramV1, ExactOrderProgramV2,
    ExactOrderTermV1, ExactOrderTermV2, ExactParameterValueV1, ExactPredicateLeafV1,
    ExactPredicateNodeV1, ExactPredicateOperatorV1, ExactPredicateProgramV1,
    ExactPredicateProgramV2, ExactProviderRequirementV1, ExactReferenceCellV1, ExactScalarV1,
    ExactStatePlacementV1, ExactValueSlotV1,
};
use riffdb_types::{
    ApplicationRoleHash, CanonicalRecord, CanonicalValue, CommitSequence, EntityKeyBuilder,
    EntityTypeId, FieldId, PartitionKeyHash, ProjectionGeneration, ProjectionProviderPolicyModeV1,
    QueryPlanHash,
};

fn sequence(value: u64) -> CommitSequence {
    CommitSequence::new(value).expect("sequence")
}

fn program() -> ExactPredicateProgramV1 {
    ExactPredicateProgramV1::new(
        ExactPredicateNodeV1::Leaf(
            ExactPredicateLeafV1::new(
                FieldId::new(2).expect("field"),
                ExactPredicateOperatorV1::GreaterEqual,
                ExactComparisonProfileV1::U64,
                Some(ExactValueSlotV1::Scalar(0)),
            )
            .expect("leaf"),
        ),
        vec![
            ExactOrderProgramV1::new(vec![
                ExactOrderTermV1::new(
                    FieldId::new(2).expect("field"),
                    ExactComparisonProfileV1::U64,
                    ExactOrderDirectionV1::Descending,
                    false,
                ),
                ExactOrderTermV1::new(
                    FieldId::first(),
                    ExactComparisonProfileV1::U64,
                    ExactOrderDirectionV1::Ascending,
                    true,
                ),
            ])
            .expect("order"),
        ],
        0,
        true,
        100,
        20,
        ExactProviderRequirementV1::new(
            ProjectionProviderPolicyModeV1::PartitionAligned,
            100,
            10_000,
            4_096,
        )
        .expect("requirement"),
    )
    .expect("program")
}

fn provider(plan: QueryPlanHash, policy: ApplicationRoleHash) -> ExactPredicatePartitionIndexV4 {
    let program = program();
    let rows = (1..=3_u32)
        .map(|id| {
            let mut key = EntityKeyBuilder::new(EntityTypeId::first());
            key.push_u32(id).expect("key");
            ExactPredicateProviderRowV1::new(
                key.finish().expect("key"),
                BTreeMap::from([
                    (
                        FieldId::first(),
                        ExactReferenceCellV1::Value(ExactScalarV1::U64(u64::from(id))),
                    ),
                    (
                        FieldId::new(2).expect("field"),
                        ExactReferenceCellV1::Value(ExactScalarV1::U64(u64::from(id * 10))),
                    ),
                ]),
                CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(u64::from(id)))])
                    .expect("output"),
            )
        })
        .collect();
    let binding = ExactPredicateProviderBindingV1::new(
        plan,
        &program,
        policy,
        PartitionKeyHash::from_bytes([3; 32]),
        7,
        ProjectionGeneration::new(2).expect("generation"),
        sequence(9),
    )
    .expect("binding");
    ExactPredicatePartitionIndexV4::rebuild(binding, program, rows).expect("provider")
}

#[test]
fn plan_policy_generation_history_and_epoch_are_one_exact_proof() {
    let plan = QueryPlanHash::from_bytes([1; 32]);
    let policy = ApplicationRoleHash::from_bytes([2; 32]);
    let provider = provider(plan, policy);
    let descriptor = provider
        .program()
        .provider_descriptor()
        .expect("descriptor");
    let observation = ProviderEpochObservationV1::new(
        descriptor.digest(),
        descriptor.state_identity().schema_hash(),
        7,
        ProjectionGeneration::new(2).expect("generation"),
        sequence(9),
        sequence(9),
        ProviderLifecycleV1::Ready,
    )
    .expect("observation");
    let proof = negotiate_result_set_epoch_v1(
        ResultSetEpochContextV1::new(plan, policy),
        &[observation],
        ResultSetEpochRequirementV1::Latest,
    )
    .expect("proof");
    let parameters = BTreeMap::from([(0, ExactParameterValueV1::Scalar(ExactScalarV1::U64(15)))]);
    let result = execute_exact_predicate_result_set_v1(
        plan,
        provider.program(),
        &proof,
        &provider,
        &parameters,
        provider.program().members()[0],
        0,
        NonZeroU16::new(10).expect("limit"),
    )
    .expect("result");
    assert_eq!(result.exact_total(), 2);

    assert_eq!(
        execute_exact_predicate_result_set_v1(
            QueryPlanHash::from_bytes([9; 32]),
            provider.program(),
            &proof,
            &provider,
            &parameters,
            provider.program().members()[0],
            0,
            NonZeroU16::new(10).expect("limit"),
        ),
        Err(ExactPredicateResultSetErrorV1::PlanMismatch)
    );
}

#[test]
fn nullable_provider_requires_the_exact_v5_epoch_identity_once() {
    let plan = QueryPlanHash::from_bytes([0x31; 32]);
    let policy = ApplicationRoleHash::from_bytes([0x32; 32]);
    let program = ExactPredicateProgramV2::new(
        ExactPredicateNodeV1::Leaf(
            ExactPredicateLeafV1::new(
                FieldId::first(),
                ExactPredicateOperatorV1::Exists,
                ExactComparisonProfileV1::U64,
                None,
            )
            .expect("predicate"),
        ),
        vec![
            ExactOrderProgramV2::new(vec![
                ExactOrderTermV2::new(
                    FieldId::new(2).expect("field"),
                    ExactComparisonProfileV1::U64,
                    ExactOrderDirectionV1::Descending,
                    ExactStatePlacementV1::NullsLastV1,
                    false,
                ),
                ExactOrderTermV2::new(
                    FieldId::first(),
                    ExactComparisonProfileV1::U64,
                    ExactOrderDirectionV1::Ascending,
                    ExactStatePlacementV1::PresentOnlyV1,
                    true,
                ),
            ])
            .expect("order"),
        ],
        0,
        true,
        100,
        20,
        ExactProviderRequirementV1::new(
            ProjectionProviderPolicyModeV1::PartitionAligned,
            100,
            10_000,
            4_096,
        )
        .expect("requirement"),
    )
    .expect("program");
    let rows = (1..=3_u32)
        .map(|id| {
            let mut key = EntityKeyBuilder::new(EntityTypeId::first());
            key.push_u32(id).expect("key");
            ExactPredicateProviderRowV1::new(
                key.finish().expect("key"),
                BTreeMap::from([
                    (
                        FieldId::first(),
                        ExactReferenceCellV1::Value(ExactScalarV1::U64(u64::from(id))),
                    ),
                    (
                        FieldId::new(2).expect("field"),
                        if id == 1 {
                            ExactReferenceCellV1::Null
                        } else {
                            ExactReferenceCellV1::Value(ExactScalarV1::U64(u64::from(id)))
                        },
                    ),
                ]),
                CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(u64::from(id)))])
                    .expect("output"),
            )
        })
        .collect();
    let binding = ExactPredicateProviderBindingV2::new(
        plan,
        &program,
        policy,
        PartitionKeyHash::from_bytes([0x33; 32]),
        7,
        ProjectionGeneration::new(5).expect("generation"),
        sequence(9),
    )
    .expect("binding");
    let provider =
        ExactPredicatePartitionIndexV5::rebuild(binding, program.clone(), rows).expect("provider");
    let descriptor = program.provider_descriptor().expect("descriptor");
    let observation = ProviderEpochObservationV1::new(
        descriptor.digest(),
        descriptor.state_identity().schema_hash(),
        7,
        ProjectionGeneration::new(5).expect("generation"),
        sequence(9),
        sequence(9),
        ProviderLifecycleV1::Ready,
    )
    .expect("observation");
    let proof = negotiate_result_set_epoch_v1(
        ResultSetEpochContextV1::new(plan, policy),
        &[observation],
        ResultSetEpochRequirementV1::Latest,
    )
    .expect("proof");
    let page = execute_nullable_exact_predicate_result_set_v1(
        plan,
        &program,
        &proof,
        &provider,
        &BTreeMap::new(),
        program.members()[0],
        0,
        NonZeroU16::new(20).expect("limit"),
    )
    .expect("page");
    assert_eq!(page.exact_total(), 3);
    assert_eq!(
        page.rows()[2]
            .fields()
            .get(&FieldId::new(2).expect("field")),
        Some(&ExactReferenceCellV1::Null)
    );
}
