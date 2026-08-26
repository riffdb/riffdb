//! Completeness checks for the transient aggregate semantic registry.

use riffdb_types::{
    AggregateArithmeticV1, AggregateBudgetBoundV1, AggregateEmptyResultV1, AggregateInputClassV1,
    AggregateNoValueRuleV1, AggregatePartialStateV1, AggregateResultSchemaV1,
    AggregateSemanticIdentityV1, aggregate_semantic_registry_v1,
};

#[test]
fn registry_freezes_every_existing_aggregate_semantic() {
    let registry = aggregate_semantic_registry_v1();
    assert_eq!(registry.len(), 5);

    let identities = registry
        .iter()
        .map(|descriptor| descriptor.identity())
        .collect::<Vec<_>>();
    assert_eq!(
        identities,
        vec![
            AggregateSemanticIdentityV1::Count,
            AggregateSemanticIdentityV1::ExactCount,
            AggregateSemanticIdentityV1::Sum,
            AggregateSemanticIdentityV1::Min,
            AggregateSemanticIdentityV1::Max,
        ]
    );

    for descriptor in registry {
        assert_eq!(
            AggregateSemanticIdentityV1::from_source_spelling(descriptor.source_spelling()),
            Some(descriptor.identity())
        );
        assert!(descriptor.budgets().charges_input_rows());
        assert!(descriptor.budgets().charges_output_bytes());
        assert!(descriptor.policy().admits_rows_before_aggregation());
        assert!(descriptor.policy().withholds_partial_results());
    }
}

#[test]
fn existing_empty_null_result_and_partial_state_rules_are_exact() {
    let count = AggregateSemanticIdentityV1::Count.descriptor();
    assert_eq!(count.input_class(), AggregateInputClassV1::NoField);
    assert_eq!(count.no_value_rule(), AggregateNoValueRuleV1::CountsRow);
    assert_eq!(count.empty_result(), AggregateEmptyResultV1::UnsignedZero);
    assert_eq!(count.result_schema(), AggregateResultSchemaV1::U64);
    assert_eq!(count.partial_state(), AggregatePartialStateV1::CheckedU64);
    assert_eq!(count.arithmetic(), AggregateArithmeticV1::CheckedU64);
    assert_eq!(count.operational_ir_tag(), Some(1));
    assert!(count.eligibility().ordinary_bounded_fold());
    assert!(count.eligibility().whole_result_provider());
    assert_eq!(
        count.budgets().groups(),
        AggregateBudgetBoundV1::CompilerPlanWhenGrouped
    );
    assert_eq!(
        count.budgets().distinct_values(),
        AggregateBudgetBoundV1::NotApplicable
    );

    let exact_count = AggregateSemanticIdentityV1::ExactCount.descriptor();
    assert_eq!(exact_count.input_class(), AggregateInputClassV1::NoField);
    assert_eq!(exact_count.result_schema(), AggregateResultSchemaV1::U64);
    assert!(!exact_count.eligibility().ordinary_bounded_fold());
    assert!(exact_count.eligibility().whole_result_provider());
    assert_eq!(exact_count.operational_ir_tag(), None);
    assert_eq!(
        exact_count.budgets().groups(),
        AggregateBudgetBoundV1::NotApplicable
    );

    let sum = AggregateSemanticIdentityV1::Sum.descriptor();
    assert_eq!(sum.input_class(), AggregateInputClassV1::ExactNumericField);
    assert_eq!(
        sum.no_value_rule(),
        AggregateNoValueRuleV1::InvalidInputType
    );
    assert_eq!(sum.empty_result(), AggregateEmptyResultV1::ExactNumericZero);
    assert_eq!(
        sum.result_schema(),
        AggregateResultSchemaV1::ExactDecimalAtInputScale
    );
    assert_eq!(
        sum.partial_state(),
        AggregatePartialStateV1::CheckedI128AtInputScale
    );
    assert_eq!(sum.arithmetic(), AggregateArithmeticV1::CheckedI128);
    assert_eq!(sum.operational_ir_tag(), Some(2));

    for (identity, tag) in [
        (AggregateSemanticIdentityV1::Min, 3),
        (AggregateSemanticIdentityV1::Max, 4),
    ] {
        let descriptor = identity.descriptor();
        assert_eq!(
            descriptor.input_class(),
            AggregateInputClassV1::OrderedScalarField
        );
        assert_eq!(
            descriptor.no_value_rule(),
            AggregateNoValueRuleV1::ComparableState
        );
        assert_eq!(descriptor.empty_result(), AggregateEmptyResultV1::Absent);
        assert_eq!(
            descriptor.result_schema(),
            AggregateResultSchemaV1::OptionalInputScalar
        );
        assert_eq!(
            descriptor.partial_state(),
            AggregatePartialStateV1::OptionalOrderedScalar
        );
        assert_eq!(
            descriptor.arithmetic(),
            AggregateArithmeticV1::FrozenTypedComparator
        );
        assert_eq!(descriptor.operational_ir_tag(), Some(tag));
    }
}
