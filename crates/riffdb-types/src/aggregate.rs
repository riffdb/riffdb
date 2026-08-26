//! Closed compiler-owned aggregate semantic vocabulary.
//!
//! These descriptors are transient compilation metadata. They are not encoded
//! into query IR, provider state, plan hashes, wire messages, or generated SDK
//! artifacts. Existing durable tags remain owned by their original formats.

/// Maximum distinct canonical values retained by one exact measure/group.
pub const MAX_AGGREGATE_DISTINCT_VALUES_V1: u16 = 256;
/// Maximum transient aggregate partial-state bytes in one query result.
pub const MAX_AGGREGATE_STATE_BYTES_V1: u32 = 1_048_576;
/// Maximum exact contribution/merge arithmetic operations in one aggregate.
pub const MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1: u32 = 8_192;

/// Stable semantic identity for the aggregate functions available before
/// ADR-0152 expansion.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AggregateSemanticIdentityV1 {
    /// Exact count of rows in an earlier bounded source.
    Count,
    /// Exact count of the complete provider-admitted population.
    ExactCount,
    /// Checked exact sum of one numeric field.
    Sum,
    /// Minimum under the field's frozen typed comparator.
    Min,
    /// Maximum under the field's frozen typed comparator.
    Max,
    /// Exact count of present values.
    CountPresent,
    /// Exact distinct count including `NoValue`.
    CountDistinct,
    /// Exact distinct count excluding `NoValue`.
    CountDistinctPresent,
    /// Exact total and contributing count without division.
    Mean,
    /// Boolean disjunction.
    Any,
    /// Boolean conjunction.
    All,
}

impl AggregateSemanticIdentityV1 {
    /// Looks up the semantic identity for one compiler-owned source spelling.
    #[must_use]
    pub fn from_source_spelling(spelling: &str) -> Option<Self> {
        aggregate_semantic_registry_v1()
            .iter()
            .find(|descriptor| descriptor.source_spelling == spelling)
            .map(|descriptor| descriptor.identity)
    }

    /// Returns this identity's complete v1 descriptor.
    #[must_use]
    pub const fn descriptor(self) -> &'static AggregateSemanticDescriptorV1 {
        match self {
            Self::Count => &AGGREGATE_SEMANTIC_REGISTRY_V1[0],
            Self::ExactCount => &AGGREGATE_SEMANTIC_REGISTRY_V1[1],
            Self::Sum => &AGGREGATE_SEMANTIC_REGISTRY_V1[2],
            Self::Min => &AGGREGATE_SEMANTIC_REGISTRY_V1[3],
            Self::Max => &AGGREGATE_SEMANTIC_REGISTRY_V1[4],
            Self::CountPresent => &AGGREGATE_SEMANTIC_REGISTRY_V1[5],
            Self::CountDistinct => &AGGREGATE_SEMANTIC_REGISTRY_V1[6],
            Self::CountDistinctPresent => &AGGREGATE_SEMANTIC_REGISTRY_V1[7],
            Self::Mean => &AGGREGATE_SEMANTIC_REGISTRY_V1[8],
            Self::Any => &AGGREGATE_SEMANTIC_REGISTRY_V1[9],
            Self::All => &AGGREGATE_SEMANTIC_REGISTRY_V1[10],
        }
    }
}

/// Compiler-accepted input shape for one aggregate function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateInputClassV1 {
    /// The aggregate accepts no field argument.
    NoField,
    /// One required exact `i64`, `u64`, or fixed-decimal field.
    ExactNumericField,
    /// One ordered scalar field, including an optional field's `NoValue` state.
    OrderedScalarField,
    /// One scalar field with canonical typed equality.
    CanonicalScalarField,
    /// One required Boolean field.
    RequiredBooleanField,
}

/// How the aggregate treats the canonical optional `NoValue` state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateNoValueRuleV1 {
    /// Field values are irrelevant and every admitted row contributes.
    CountsRow,
    /// An optional field is not an accepted input type.
    InvalidInputType,
    /// `NoValue` is a real value under the frozen typed comparator.
    ComparableState,
    /// `NoValue` does not contribute.
    Excluded,
    /// `NoValue` contributes as one canonical distinct value.
    DistinctValue,
}

/// Exact result returned for an empty input population.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateEmptyResultV1 {
    /// Unsigned cardinality zero.
    UnsignedZero,
    /// Exact additive zero at the compiler-resolved input scale.
    ExactNumericZero,
    /// Outer absence, distinct from a contributed `NoValue`.
    Absent,
    /// Exact mean state with zero total and zero count.
    ExactMeanZero,
    /// Boolean false.
    BooleanFalse,
    /// Boolean true.
    BooleanTrue,
}

/// Compiler-resolved public result schema family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateResultSchemaV1 {
    /// Exact unsigned 64-bit cardinality.
    U64,
    /// Widened exact decimal retaining the input's fixed scale.
    ExactDecimalAtInputScale,
    /// Optional copy of the compiler-resolved input scalar type.
    OptionalInputScalar,
    /// Exact `ExactMeanV1 { total, count }` record.
    ExactMeanV1,
    /// Required Boolean scalar.
    Bool,
}

/// Mergeable partial-state family admitted by an exact provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregatePartialStateV1 {
    /// Checked unsigned 64-bit counter.
    CheckedU64,
    /// Checked signed 128-bit coefficient plus compiler-resolved scale.
    CheckedI128AtInputScale,
    /// Optional scalar selected using the frozen typed comparator.
    OptionalOrderedScalar,
    /// Bounded canonical typed value set.
    BoundedCanonicalSet,
    /// Checked exact total and unsigned count.
    ExactMeanV1,
    /// Boolean disjunction state.
    BooleanAny,
    /// Boolean conjunction state.
    BooleanAll,
}

/// Arithmetic or comparison rule used by the exact evaluator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateArithmeticV1 {
    /// Checked unsigned 64-bit addition; overflow refuses the whole result.
    CheckedU64,
    /// Checked signed 128-bit addition; overflow refuses the whole result.
    CheckedI128,
    /// The input field's canonical frozen typed comparator.
    FrozenTypedComparator,
    /// Canonical typed equality with checked bounded-set growth.
    CanonicalDistinctSet,
    /// Checked exact total and checked unsigned count.
    CheckedExactMean,
    /// Boolean disjunction.
    BooleanOr,
    /// Boolean conjunction.
    BooleanAnd,
}

/// Static execution-family eligibility for one aggregate semantic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AggregateEligibilityV1 {
    ordinary_bounded_fold: bool,
    grouped_bounded_fold: bool,
    whole_result_provider: bool,
}

impl AggregateEligibilityV1 {
    /// Whether the function may fold an earlier compiler-bounded collection.
    #[must_use]
    pub const fn ordinary_bounded_fold(self) -> bool {
        self.ordinary_bounded_fold
    }

    /// Whether the function may fold compiler-bounded groups.
    #[must_use]
    pub const fn grouped_bounded_fold(self) -> bool {
        self.grouped_bounded_fold
    }

    /// Whether an exact whole-result provider may advertise the function.
    #[must_use]
    pub const fn whole_result_provider(self) -> bool {
        self.whole_result_provider
    }
}

/// Where the maximum for one aggregate resource dimension is frozen.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateBudgetBoundV1 {
    /// The function does not retain or consume this resource dimension.
    NotApplicable,
    /// The compiler seals a finite maximum into the complete plan.
    CompilerPlan,
    /// The compiler seals a finite maximum when the aggregate is grouped.
    CompilerPlanWhenGrouped,
}

/// Budget dimensions which every plan using an aggregate must account for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AggregateBudgetDimensionsV1 {
    input_rows: AggregateBudgetBoundV1,
    distinct_values: AggregateBudgetBoundV1,
    groups: AggregateBudgetBoundV1,
    state_bytes: AggregateBudgetBoundV1,
    output_bytes: AggregateBudgetBoundV1,
    diagnostic_work: AggregateBudgetBoundV1,
}

impl AggregateBudgetDimensionsV1 {
    /// Whether admitted input rows require a compiler/runtime charge.
    #[must_use]
    pub const fn charges_input_rows(self) -> bool {
        !matches!(self.input_rows, AggregateBudgetBoundV1::NotApplicable)
    }

    /// Source of the maximum admitted input-row count.
    #[must_use]
    pub const fn input_rows(self) -> AggregateBudgetBoundV1 {
        self.input_rows
    }

    /// Source of the maximum retained distinct-value count.
    #[must_use]
    pub const fn distinct_values(self) -> AggregateBudgetBoundV1 {
        self.distinct_values
    }

    /// Whether group cardinality requires an independent bound when grouped.
    #[must_use]
    pub const fn charges_groups(self) -> bool {
        !matches!(self.groups, AggregateBudgetBoundV1::NotApplicable)
    }

    /// Source of the maximum group count.
    #[must_use]
    pub const fn groups(self) -> AggregateBudgetBoundV1 {
        self.groups
    }

    /// Whether partial aggregate state bytes require an independent bound.
    #[must_use]
    pub const fn charges_state_bytes(self) -> bool {
        !matches!(self.state_bytes, AggregateBudgetBoundV1::NotApplicable)
    }

    /// Source of the maximum partial-state bytes.
    #[must_use]
    pub const fn state_bytes(self) -> AggregateBudgetBoundV1 {
        self.state_bytes
    }

    /// Whether encoded aggregate output bytes require an independent bound.
    #[must_use]
    pub const fn charges_output_bytes(self) -> bool {
        !matches!(self.output_bytes, AggregateBudgetBoundV1::NotApplicable)
    }

    /// Source of the maximum encoded output bytes.
    #[must_use]
    pub const fn output_bytes(self) -> AggregateBudgetBoundV1 {
        self.output_bytes
    }

    /// Whether bounded diagnostic work must be accounted for.
    #[must_use]
    pub const fn charges_diagnostic_work(self) -> bool {
        !matches!(self.diagnostic_work, AggregateBudgetBoundV1::NotApplicable)
    }

    /// Source of the maximum bounded diagnostic work.
    #[must_use]
    pub const fn diagnostic_work(self) -> AggregateBudgetBoundV1 {
        self.diagnostic_work
    }
}

/// Policy and inference posture shared by every existing exact aggregate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AggregatePolicyV1 {
    admits_rows_before_aggregation: bool,
    partition_scoped_primary: bool,
    withholds_partial_results: bool,
}

impl AggregatePolicyV1 {
    /// Whether row policy is applied before any fold, count, group, or limit.
    #[must_use]
    pub const fn admits_rows_before_aggregation(self) -> bool {
        self.admits_rows_before_aggregation
    }

    /// Whether policy-aligned provider partitioning is the primary mode.
    #[must_use]
    pub const fn partition_scoped_primary(self) -> bool {
        self.partition_scoped_primary
    }

    /// Whether refusal or budget exhaustion releases no partial aggregate.
    #[must_use]
    pub const fn withholds_partial_results(self) -> bool {
        self.withholds_partial_results
    }
}

/// Complete transient semantic descriptor for one exact aggregate function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AggregateSemanticDescriptorV1 {
    identity: AggregateSemanticIdentityV1,
    source_spelling: &'static str,
    input_class: AggregateInputClassV1,
    no_value_rule: AggregateNoValueRuleV1,
    empty_result: AggregateEmptyResultV1,
    result_schema: AggregateResultSchemaV1,
    partial_state: AggregatePartialStateV1,
    arithmetic: AggregateArithmeticV1,
    operational_ir_tag: Option<u8>,
    eligibility: AggregateEligibilityV1,
    budgets: AggregateBudgetDimensionsV1,
    policy: AggregatePolicyV1,
}

impl AggregateSemanticDescriptorV1 {
    /// Stable semantic identity.
    #[must_use]
    pub const fn identity(self) -> AggregateSemanticIdentityV1 {
        self.identity
    }

    /// Exact RiffQL source spelling.
    #[must_use]
    pub const fn source_spelling(self) -> &'static str {
        self.source_spelling
    }

    /// Compiler-accepted input class and arity.
    #[must_use]
    pub const fn input_class(self) -> AggregateInputClassV1 {
        self.input_class
    }

    /// Canonical `NoValue` rule.
    #[must_use]
    pub const fn no_value_rule(self) -> AggregateNoValueRuleV1 {
        self.no_value_rule
    }

    /// Empty-population result.
    #[must_use]
    pub const fn empty_result(self) -> AggregateEmptyResultV1 {
        self.empty_result
    }

    /// Compiler-resolved public result schema family.
    #[must_use]
    pub const fn result_schema(self) -> AggregateResultSchemaV1 {
        self.result_schema
    }

    /// Mergeable exact partial-state family.
    #[must_use]
    pub const fn partial_state(self) -> AggregatePartialStateV1 {
        self.partial_state
    }

    /// Exact arithmetic or comparison rule.
    #[must_use]
    pub const fn arithmetic(self) -> AggregateArithmeticV1 {
        self.arithmetic
    }

    /// Existing durable operational query-IR tag, when this semantic is
    /// carried by that format. These values are inventory only and unchanged.
    #[must_use]
    pub const fn operational_ir_tag(self) -> Option<u8> {
        self.operational_ir_tag
    }

    /// Static execution-family eligibility.
    #[must_use]
    pub const fn eligibility(self) -> AggregateEligibilityV1 {
        self.eligibility
    }

    /// Required independent budget dimensions.
    #[must_use]
    pub const fn budgets(self) -> AggregateBudgetDimensionsV1 {
        self.budgets
    }

    /// Policy and inference posture.
    #[must_use]
    pub const fn policy(self) -> AggregatePolicyV1 {
        self.policy
    }
}

const BOUNDED_AND_PROVIDER: AggregateEligibilityV1 = AggregateEligibilityV1 {
    ordinary_bounded_fold: true,
    grouped_bounded_fold: true,
    whole_result_provider: true,
};

const WHOLE_RESULT_ONLY: AggregateEligibilityV1 = AggregateEligibilityV1 {
    ordinary_bounded_fold: false,
    grouped_bounded_fold: false,
    whole_result_provider: true,
};

const EXACT_BUDGETS: AggregateBudgetDimensionsV1 = AggregateBudgetDimensionsV1 {
    input_rows: AggregateBudgetBoundV1::CompilerPlan,
    distinct_values: AggregateBudgetBoundV1::NotApplicable,
    groups: AggregateBudgetBoundV1::CompilerPlanWhenGrouped,
    state_bytes: AggregateBudgetBoundV1::CompilerPlan,
    output_bytes: AggregateBudgetBoundV1::CompilerPlan,
    diagnostic_work: AggregateBudgetBoundV1::CompilerPlan,
};

const EXACT_COUNT_BUDGETS: AggregateBudgetDimensionsV1 = AggregateBudgetDimensionsV1 {
    input_rows: AggregateBudgetBoundV1::CompilerPlan,
    distinct_values: AggregateBudgetBoundV1::NotApplicable,
    groups: AggregateBudgetBoundV1::NotApplicable,
    state_bytes: AggregateBudgetBoundV1::CompilerPlan,
    output_bytes: AggregateBudgetBoundV1::CompilerPlan,
    diagnostic_work: AggregateBudgetBoundV1::CompilerPlan,
};

const EXACT_DISTINCT_BUDGETS: AggregateBudgetDimensionsV1 = AggregateBudgetDimensionsV1 {
    input_rows: AggregateBudgetBoundV1::CompilerPlan,
    distinct_values: AggregateBudgetBoundV1::CompilerPlan,
    groups: AggregateBudgetBoundV1::CompilerPlanWhenGrouped,
    state_bytes: AggregateBudgetBoundV1::CompilerPlan,
    output_bytes: AggregateBudgetBoundV1::CompilerPlan,
    diagnostic_work: AggregateBudgetBoundV1::CompilerPlan,
};

const EXACT_POLICY: AggregatePolicyV1 = AggregatePolicyV1 {
    admits_rows_before_aggregation: true,
    partition_scoped_primary: true,
    withholds_partial_results: true,
};

const AGGREGATE_SEMANTIC_REGISTRY_V1: [AggregateSemanticDescriptorV1; 11] = [
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::Count,
        source_spelling: "count",
        input_class: AggregateInputClassV1::NoField,
        no_value_rule: AggregateNoValueRuleV1::CountsRow,
        empty_result: AggregateEmptyResultV1::UnsignedZero,
        result_schema: AggregateResultSchemaV1::U64,
        partial_state: AggregatePartialStateV1::CheckedU64,
        arithmetic: AggregateArithmeticV1::CheckedU64,
        operational_ir_tag: Some(1),
        eligibility: BOUNDED_AND_PROVIDER,
        budgets: EXACT_BUDGETS,
        policy: EXACT_POLICY,
    },
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::ExactCount,
        source_spelling: "exact_count",
        input_class: AggregateInputClassV1::NoField,
        no_value_rule: AggregateNoValueRuleV1::CountsRow,
        empty_result: AggregateEmptyResultV1::UnsignedZero,
        result_schema: AggregateResultSchemaV1::U64,
        partial_state: AggregatePartialStateV1::CheckedU64,
        arithmetic: AggregateArithmeticV1::CheckedU64,
        operational_ir_tag: None,
        eligibility: WHOLE_RESULT_ONLY,
        budgets: EXACT_COUNT_BUDGETS,
        policy: EXACT_POLICY,
    },
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::Sum,
        source_spelling: "sum",
        input_class: AggregateInputClassV1::ExactNumericField,
        no_value_rule: AggregateNoValueRuleV1::InvalidInputType,
        empty_result: AggregateEmptyResultV1::ExactNumericZero,
        result_schema: AggregateResultSchemaV1::ExactDecimalAtInputScale,
        partial_state: AggregatePartialStateV1::CheckedI128AtInputScale,
        arithmetic: AggregateArithmeticV1::CheckedI128,
        operational_ir_tag: Some(2),
        eligibility: BOUNDED_AND_PROVIDER,
        budgets: EXACT_BUDGETS,
        policy: EXACT_POLICY,
    },
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::Min,
        source_spelling: "min",
        input_class: AggregateInputClassV1::OrderedScalarField,
        no_value_rule: AggregateNoValueRuleV1::ComparableState,
        empty_result: AggregateEmptyResultV1::Absent,
        result_schema: AggregateResultSchemaV1::OptionalInputScalar,
        partial_state: AggregatePartialStateV1::OptionalOrderedScalar,
        arithmetic: AggregateArithmeticV1::FrozenTypedComparator,
        operational_ir_tag: Some(3),
        eligibility: BOUNDED_AND_PROVIDER,
        budgets: EXACT_BUDGETS,
        policy: EXACT_POLICY,
    },
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::Max,
        source_spelling: "max",
        input_class: AggregateInputClassV1::OrderedScalarField,
        no_value_rule: AggregateNoValueRuleV1::ComparableState,
        empty_result: AggregateEmptyResultV1::Absent,
        result_schema: AggregateResultSchemaV1::OptionalInputScalar,
        partial_state: AggregatePartialStateV1::OptionalOrderedScalar,
        arithmetic: AggregateArithmeticV1::FrozenTypedComparator,
        operational_ir_tag: Some(4),
        eligibility: BOUNDED_AND_PROVIDER,
        budgets: EXACT_BUDGETS,
        policy: EXACT_POLICY,
    },
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::CountPresent,
        source_spelling: "count_present",
        input_class: AggregateInputClassV1::CanonicalScalarField,
        no_value_rule: AggregateNoValueRuleV1::Excluded,
        empty_result: AggregateEmptyResultV1::UnsignedZero,
        result_schema: AggregateResultSchemaV1::U64,
        partial_state: AggregatePartialStateV1::CheckedU64,
        arithmetic: AggregateArithmeticV1::CheckedU64,
        operational_ir_tag: Some(5),
        eligibility: BOUNDED_AND_PROVIDER,
        budgets: EXACT_BUDGETS,
        policy: EXACT_POLICY,
    },
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::CountDistinct,
        source_spelling: "count_distinct",
        input_class: AggregateInputClassV1::CanonicalScalarField,
        no_value_rule: AggregateNoValueRuleV1::DistinctValue,
        empty_result: AggregateEmptyResultV1::UnsignedZero,
        result_schema: AggregateResultSchemaV1::U64,
        partial_state: AggregatePartialStateV1::BoundedCanonicalSet,
        arithmetic: AggregateArithmeticV1::CanonicalDistinctSet,
        operational_ir_tag: Some(6),
        eligibility: BOUNDED_AND_PROVIDER,
        budgets: EXACT_DISTINCT_BUDGETS,
        policy: EXACT_POLICY,
    },
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::CountDistinctPresent,
        source_spelling: "count_distinct_present",
        input_class: AggregateInputClassV1::CanonicalScalarField,
        no_value_rule: AggregateNoValueRuleV1::Excluded,
        empty_result: AggregateEmptyResultV1::UnsignedZero,
        result_schema: AggregateResultSchemaV1::U64,
        partial_state: AggregatePartialStateV1::BoundedCanonicalSet,
        arithmetic: AggregateArithmeticV1::CanonicalDistinctSet,
        operational_ir_tag: Some(7),
        eligibility: BOUNDED_AND_PROVIDER,
        budgets: EXACT_DISTINCT_BUDGETS,
        policy: EXACT_POLICY,
    },
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::Mean,
        source_spelling: "mean",
        input_class: AggregateInputClassV1::ExactNumericField,
        no_value_rule: AggregateNoValueRuleV1::InvalidInputType,
        empty_result: AggregateEmptyResultV1::ExactMeanZero,
        result_schema: AggregateResultSchemaV1::ExactMeanV1,
        partial_state: AggregatePartialStateV1::ExactMeanV1,
        arithmetic: AggregateArithmeticV1::CheckedExactMean,
        operational_ir_tag: Some(8),
        eligibility: BOUNDED_AND_PROVIDER,
        budgets: EXACT_BUDGETS,
        policy: EXACT_POLICY,
    },
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::Any,
        source_spelling: "any",
        input_class: AggregateInputClassV1::RequiredBooleanField,
        no_value_rule: AggregateNoValueRuleV1::InvalidInputType,
        empty_result: AggregateEmptyResultV1::BooleanFalse,
        result_schema: AggregateResultSchemaV1::Bool,
        partial_state: AggregatePartialStateV1::BooleanAny,
        arithmetic: AggregateArithmeticV1::BooleanOr,
        operational_ir_tag: Some(9),
        eligibility: BOUNDED_AND_PROVIDER,
        budgets: EXACT_BUDGETS,
        policy: EXACT_POLICY,
    },
    AggregateSemanticDescriptorV1 {
        identity: AggregateSemanticIdentityV1::All,
        source_spelling: "all",
        input_class: AggregateInputClassV1::RequiredBooleanField,
        no_value_rule: AggregateNoValueRuleV1::InvalidInputType,
        empty_result: AggregateEmptyResultV1::BooleanTrue,
        result_schema: AggregateResultSchemaV1::Bool,
        partial_state: AggregatePartialStateV1::BooleanAll,
        arithmetic: AggregateArithmeticV1::BooleanAnd,
        operational_ir_tag: Some(10),
        eligibility: BOUNDED_AND_PROVIDER,
        budgets: EXACT_BUDGETS,
        policy: EXACT_POLICY,
    },
];

/// Returns the closed v1 aggregate semantic registry in stable identity order.
#[must_use]
pub const fn aggregate_semantic_registry_v1() -> &'static [AggregateSemanticDescriptorV1] {
    &AGGREGATE_SEMANTIC_REGISTRY_V1
}
