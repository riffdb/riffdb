//! Closed finite plan families for operational RiffQL.

use std::sync::Arc;

use riffdb_riffql_syntax::AggregateFunction;
use riffdb_types::{
    AggregateInputClassV1, AggregateSemanticIdentityV1, QueryCostVectorV1, QueryPlanHash,
    hash_query_plan,
};

pub use riffdb_types::{
    MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1, MAX_AGGREGATE_DISTINCT_VALUES_V1,
    MAX_AGGREGATE_STATE_BYTES_V1,
};

use crate::{
    AuthorizationEntityAccess, MAX_QUERY_ARTIFACT_BYTES, NamedTypeSchema, PageBound,
    QUERY_IR_VERSION_EXACT_AGGREGATE_V1, QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1,
    QUERY_IR_VERSION_OPERATIONAL_V1, QueryAccessProgramV1, QueryPredicate, QueryRootOrderTermV1,
    ResolvedQueryV1,
};

const FAMILY_MAGIC: &[u8] = b"RIFFDB-OPERATIONAL-QUERY-FAMILY\0";

/// Maximum optional-presence dimensions in one operational query.
pub const MAX_OPERATIONAL_PRESENCE_PARAMETERS: usize = 8;
/// Maximum compiler-enumerated access plans in one operational family.
pub const MAX_OPERATIONAL_PLAN_MEMBERS: usize = 1 << MAX_OPERATIONAL_PRESENCE_PARAMETERS;
/// Maps the unchanged source syntax arms to the shared semantic registry.
#[must_use]
pub const fn source_aggregate_semantic_identity(
    function: AggregateFunction,
) -> AggregateSemanticIdentityV1 {
    match function {
        AggregateFunction::Count => AggregateSemanticIdentityV1::Count,
        AggregateFunction::ExactCount => AggregateSemanticIdentityV1::ExactCount,
        AggregateFunction::Sum | AggregateFunction::Stddev => AggregateSemanticIdentityV1::Sum,
        AggregateFunction::Min
        | AggregateFunction::Median
        | AggregateFunction::P90
        | AggregateFunction::P95
        | AggregateFunction::P99
        | AggregateFunction::First => AggregateSemanticIdentityV1::Min,
        AggregateFunction::Max | AggregateFunction::Last => AggregateSemanticIdentityV1::Max,
        AggregateFunction::CountPresent | AggregateFunction::Collect => {
            AggregateSemanticIdentityV1::CountPresent
        }
        AggregateFunction::CountDistinct => AggregateSemanticIdentityV1::CountDistinct,
        AggregateFunction::CountDistinctPresent | AggregateFunction::CollectDistinct => {
            AggregateSemanticIdentityV1::CountDistinctPresent
        }
        AggregateFunction::Mean => AggregateSemanticIdentityV1::Mean,
        AggregateFunction::Any => AggregateSemanticIdentityV1::Any,
        AggregateFunction::All => AggregateSemanticIdentityV1::All,
    }
}

/// Deterministic UTC temporal truncation bucket for `group by`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalAggregateTimeBucketV1 {
    /// Truncate timestamp to UTC hour start (`YYYY-MM-DDTHH:00:00Z`).
    Hour,
    /// Truncate timestamp or date to UTC day start (`YYYY-MM-DDT00:00:00Z` or `Date`).
    Day,
    /// Truncate timestamp or date to ISO-8601 Monday UTC week start.
    Week,
    /// Truncate timestamp or date to first day of UTC month (`YYYY-MM-01`).
    Month,
    /// Truncate timestamp or date to first day of UTC year (`YYYY-01-01`).
    Year,
}

impl OperationalAggregateTimeBucketV1 {
    pub(crate) const fn durable_tag(self) -> u8 {
        match self {
            Self::Hour => 1,
            Self::Day => 2,
            Self::Week => 3,
            Self::Month => 4,
            Self::Year => 5,
        }
    }
}

/// Closed aggregate functions shared with the projected exact evaluator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalAggregateFunctionV1 {
    /// Exact matching-row count.
    Count,
    /// Checked exact numeric sum.
    Sum,
    /// Minimum contributed value, absent for an empty input.
    Min,
    /// Maximum contributed value, absent for an empty input.
    Max,
    /// Exact count of present values.
    CountPresent,
    /// Exact distinct count including `NoValue`.
    CountDistinct,
    /// Exact distinct count excluding `NoValue`.
    CountDistinctPresent,
    /// Exact total and contributing count.
    Mean,
    /// Boolean disjunction.
    Any,
    /// Boolean conjunction.
    All,
    /// Exact nearest-rank 50th order statistic over non-null ordered scalars.
    Median,
    /// Exact nearest-rank 90th order statistic over non-null ordered scalars.
    P90,
    /// Exact nearest-rank 95th order statistic over non-null ordered scalars.
    P95,
    /// Exact nearest-rank 99th order statistic over non-null ordered scalars.
    P99,
    /// First non-null scalar in source order.
    First,
    /// Last non-null scalar in source order.
    Last,
    /// Exact fixed-scale population standard deviation.
    Stddev,
    /// Bounded list of non-null scalar values in source order.
    Collect,
    /// Bounded list of distinct non-null scalar values in canonical order.
    CollectDistinct,
}

impl OperationalAggregateFunctionV1 {
    /// Returns the shared semantic identity represented by this durable IR arm.
    #[must_use]
    pub const fn semantic_identity(self) -> AggregateSemanticIdentityV1 {
        match self {
            Self::Count => AggregateSemanticIdentityV1::Count,
            Self::Sum | Self::Stddev => AggregateSemanticIdentityV1::Sum,
            Self::Min | Self::Median | Self::P90 | Self::P95 | Self::P99 | Self::First => {
                AggregateSemanticIdentityV1::Min
            }
            Self::Max | Self::Last => AggregateSemanticIdentityV1::Max,
            Self::CountPresent | Self::Collect => AggregateSemanticIdentityV1::CountPresent,
            Self::CountDistinct => AggregateSemanticIdentityV1::CountDistinct,
            Self::CountDistinctPresent | Self::CollectDistinct => {
                AggregateSemanticIdentityV1::CountDistinctPresent
            }
            Self::Mean => AggregateSemanticIdentityV1::Mean,
            Self::Any => AggregateSemanticIdentityV1::Any,
            Self::All => AggregateSemanticIdentityV1::All,
        }
    }

    pub(crate) fn durable_tag(self) -> u8 {
        match self {
            Self::Median => 11,
            Self::P90 => 12,
            Self::P95 => 13,
            Self::P99 => 14,
            Self::First => 15,
            Self::Last => 16,
            Self::Stddev => 17,
            Self::Collect => 18,
            Self::CollectDistinct => 19,
            _ => self
                .semantic_identity()
                .descriptor()
                .operational_ir_tag()
                .expect("every operational aggregate has its frozen v1 tag"),
        }
    }
}

/// One resolved grouping field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalAggregateGroupKeyV1 {
    field: String,
    source_field: String,
    time_bucket: Option<OperationalAggregateTimeBucketV1>,
    value_type: NamedTypeSchema,
}

impl OperationalAggregateGroupKeyV1 {
    /// Output field name in the aggregate result record.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Underlying source entity field name.
    #[must_use]
    pub fn source_field(&self) -> &str {
        &self.source_field
    }

    /// Optional deterministic UTC temporal truncation bucket.
    #[must_use]
    pub const fn time_bucket(&self) -> Option<OperationalAggregateTimeBucketV1> {
        self.time_bucket
    }

    /// Name-addressed result type.
    #[must_use]
    pub const fn value_type(&self) -> &NamedTypeSchema {
        &self.value_type
    }

    #[doc(hidden)]
    #[must_use]
    pub fn checked(field: String, value_type: NamedTypeSchema) -> Option<Self> {
        (!field.is_empty()).then_some(Self {
            source_field: field.clone(),
            field,
            time_bucket: None,
            value_type,
        })
    }

    #[doc(hidden)]
    #[must_use]
    pub fn checked_with_bucket_and_alias(
        field: String,
        source_field: String,
        time_bucket: Option<OperationalAggregateTimeBucketV1>,
        value_type: NamedTypeSchema,
    ) -> Option<Self> {
        (!field.is_empty() && !source_field.is_empty()).then_some(Self {
            field,
            source_field,
            time_bucket,
            value_type,
        })
    }
}

/// One resolved exact aggregate measure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalAggregateMeasureV1 {
    alias: String,
    function: OperationalAggregateFunctionV1,
    input_field: Option<String>,
    filter: Vec<QueryPredicate>,
    result_type: NamedTypeSchema,
}

impl OperationalAggregateMeasureV1 {
    /// Declared result field name.
    #[must_use]
    pub fn alias(&self) -> &str {
        &self.alias
    }

    /// Closed aggregate function.
    #[must_use]
    pub const fn function(&self) -> OperationalAggregateFunctionV1 {
        self.function
    }

    /// Exact source field for non-count measures.
    #[must_use]
    pub fn input_field(&self) -> Option<&str> {
        self.input_field.as_deref()
    }

    /// Optional conjunctive per-measure row filter (`where <predicate>`).
    #[must_use]
    pub fn filter(&self) -> &[QueryPredicate] {
        &self.filter
    }

    /// Name-addressed result type.
    #[must_use]
    pub const fn result_type(&self) -> &NamedTypeSchema {
        &self.result_type
    }

    #[doc(hidden)]
    #[must_use]
    pub fn checked(
        alias: String,
        function: OperationalAggregateFunctionV1,
        input_field: Option<String>,
        result_type: NamedTypeSchema,
    ) -> Option<Self> {
        Self::checked_with_filter(alias, function, input_field, Vec::new(), result_type)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn checked_with_filter(
        alias: String,
        function: OperationalAggregateFunctionV1,
        input_field: Option<String>,
        filter: Vec<QueryPredicate>,
        result_type: NamedTypeSchema,
    ) -> Option<Self> {
        let input_is_valid = match function.semantic_identity().descriptor().input_class() {
            AggregateInputClassV1::NoField => input_field.is_none(),
            AggregateInputClassV1::ExactNumericField
            | AggregateInputClassV1::OrderedScalarField
            | AggregateInputClassV1::CanonicalScalarField
            | AggregateInputClassV1::RequiredBooleanField => {
                input_field.as_ref().is_some_and(|field| !field.is_empty())
            }
        };
        (!alias.is_empty() && input_is_valid).then_some(Self {
            alias,
            function,
            input_field,
            filter,
            result_type,
        })
    }
}

/// One compiler-resolved bounded aggregate over an earlier collection binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalAggregateV1 {
    name: String,
    source_binding: String,
    source_entity: String,
    parent_binding: Option<String>,
    parent_child_fields: Vec<String>,
    parent_driver_fields: Vec<String>,
    group_keys: Vec<OperationalAggregateGroupKeyV1>,
    measures: Vec<OperationalAggregateMeasureV1>,
    having: Vec<QueryPredicate>,
    order_by: Vec<QueryRootOrderTermV1>,
    maximum_groups: PageBound,
    execution_budget: AggregateExecutionBudgetV1,
}

/// Independent compiler-sealed runtime bounds for exact aggregate state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AggregateExecutionBudgetV1 {
    maximum_distinct_values_per_measure: u16,
    maximum_state_bytes: u32,
    maximum_arithmetic_operations: u32,
}

impl AggregateExecutionBudgetV1 {
    /// Constructs one bounded aggregate budget.
    #[must_use]
    pub const fn checked(
        maximum_distinct_values_per_measure: u16,
        maximum_state_bytes: u32,
        maximum_arithmetic_operations: u32,
    ) -> Option<Self> {
        if maximum_distinct_values_per_measure == 0
            || maximum_distinct_values_per_measure > MAX_AGGREGATE_DISTINCT_VALUES_V1
            || maximum_state_bytes == 0
            || maximum_state_bytes > MAX_AGGREGATE_STATE_BYTES_V1
            || maximum_arithmetic_operations == 0
            || maximum_arithmetic_operations > MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1
        {
            return None;
        }
        Some(Self {
            maximum_distinct_values_per_measure,
            maximum_state_bytes,
            maximum_arithmetic_operations,
        })
    }

    /// Maximum distinct values retained by one measure in one group.
    #[must_use]
    pub const fn maximum_distinct_values_per_measure(self) -> u16 {
        self.maximum_distinct_values_per_measure
    }

    /// Maximum aggregate partial-state bytes across the query result.
    #[must_use]
    pub const fn maximum_state_bytes(self) -> u32 {
        self.maximum_state_bytes
    }

    /// Maximum exact contribution and merge operations.
    #[must_use]
    pub const fn maximum_arithmetic_operations(self) -> u32 {
        self.maximum_arithmetic_operations
    }
}

impl OperationalAggregateV1 {
    /// Query-local aggregate result name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Earlier bounded collection binding being folded.
    #[must_use]
    pub fn source_binding(&self) -> &str {
        &self.source_binding
    }

    /// Exact contract entity supplying rows.
    #[must_use]
    pub fn source_entity(&self) -> &str {
        &self.source_entity
    }

    /// Optional parent binding when this aggregate is a per-parent rollup (`per <parent>`).
    #[must_use]
    pub fn parent_binding(&self) -> Option<&str> {
        self.parent_binding.as_deref()
    }

    /// Child-entity join fields pairing with `parent_driver_fields()` for per-parent rollups.
    #[must_use]
    pub fn parent_child_fields(&self) -> &[String] {
        &self.parent_child_fields
    }

    /// Parent-entity join fields pairing with `parent_child_fields()` for per-parent rollups.
    #[must_use]
    pub fn parent_driver_fields(&self) -> &[String] {
        &self.parent_driver_fields
    }

    /// Grouping keys in declaration/result order.
    #[must_use]
    pub fn group_keys(&self) -> &[OperationalAggregateGroupKeyV1] {
        &self.group_keys
    }

    /// Measures in declaration/result order.
    #[must_use]
    pub fn measures(&self) -> &[OperationalAggregateMeasureV1] {
        &self.measures
    }

    /// Post-aggregation group predicates (`having <predicate>`).
    #[must_use]
    pub fn having(&self) -> &[QueryPredicate] {
        &self.having
    }

    /// Post-aggregation group ordering (`order by ...`).
    #[must_use]
    pub fn order_by(&self) -> &[QueryRootOrderTermV1] {
        &self.order_by
    }

    /// Compiler-proven maximum returned group count.
    #[must_use]
    pub const fn maximum_groups(&self) -> &PageBound {
        &self.maximum_groups
    }

    /// Independent compiler-sealed aggregate execution bounds.
    #[must_use]
    pub const fn execution_budget(&self) -> AggregateExecutionBudgetV1 {
        self.execution_budget
    }

    #[doc(hidden)]
    #[must_use]
    pub fn checked(
        name: String,
        source_binding: String,
        source_entity: String,
        group_keys: Vec<OperationalAggregateGroupKeyV1>,
        measures: Vec<OperationalAggregateMeasureV1>,
        maximum_groups: PageBound,
        execution_budget: AggregateExecutionBudgetV1,
    ) -> Option<Self> {
        let group_names = group_keys
            .iter()
            .map(|key| key.field.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let measure_names = measures
            .iter()
            .map(|measure| measure.alias.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if name.is_empty()
            || source_binding.is_empty()
            || source_entity.is_empty()
            || measures.is_empty()
            || group_keys.len() > riffdb_riffql_syntax::MAX_AGGREGATE_GROUP_KEYS
            || measures.len() > riffdb_riffql_syntax::MAX_AGGREGATE_MEASURES
            || group_names.len() != group_keys.len()
            || measure_names.len() != measures.len()
            || group_names.iter().any(|name| measure_names.contains(name))
            || (group_keys.is_empty() && !matches!(maximum_groups, PageBound::Literal(1)))
        {
            return None;
        }
        Some(Self {
            name,
            source_binding,
            source_entity,
            parent_binding: None,
            parent_child_fields: Vec::new(),
            parent_driver_fields: Vec::new(),
            group_keys,
            measures,
            having: Vec::new(),
            order_by: Vec::new(),
            maximum_groups,
            execution_budget,
        })
    }

    #[doc(hidden)]
    #[must_use]
    pub fn with_saas_extensions(
        mut self,
        parent_binding: Option<String>,
        parent_child_fields: Vec<String>,
        parent_driver_fields: Vec<String>,
        having: Vec<QueryPredicate>,
        order_by: Vec<QueryRootOrderTermV1>,
    ) -> Option<Self> {
        if parent_child_fields.len() != parent_driver_fields.len() {
            return None;
        }
        if parent_binding.is_some() && parent_child_fields.is_empty() {
            return None;
        }
        self.parent_binding = parent_binding;
        self.parent_child_fields = parent_child_fields;
        self.parent_driver_fields = parent_driver_fields;
        self.having = having;
        self.order_by = order_by;
        Some(self)
    }
}

/// Stable identity of one complete finite operational plan family.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationalQueryFamilyIdentity(QueryPlanHash);

impl OperationalQueryFamilyIdentity {
    /// Domain-separated digest over the complete canonical family.
    #[must_use]
    pub const fn hash(self) -> QueryPlanHash {
        self.0
    }
}

/// One compiler-enumerated member selected by an exact presence bit mask.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalPlanMemberV1 {
    presence_mask: u16,
    program: Arc<QueryAccessProgramV1>,
}

impl OperationalPlanMemberV1 {
    /// Presence mask in the family's ordered parameter domain.
    #[must_use]
    pub const fn presence_mask(&self) -> u16 {
        self.presence_mask
    }

    /// Complete ordinary bounded access plan for this member.
    #[must_use]
    pub fn program(&self) -> &QueryAccessProgramV1 {
        &self.program
    }

    /// Shared immutable access program for request execution.
    #[must_use]
    pub fn shared_program(&self) -> Arc<QueryAccessProgramV1> {
        Arc::clone(&self.program)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn checked(presence_mask: u16, program: QueryAccessProgramV1) -> Self {
        Self {
            presence_mask,
            program: Arc::new(program),
        }
    }
}

/// Complete finite compiler-owned access family for one operational query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalQueryFamilyV1 {
    surface: ResolvedQueryV1,
    presence_parameters: Vec<String>,
    members: Vec<OperationalPlanMemberV1>,
    authorization_union: Vec<AuthorizationEntityAccess>,
    maximum_cost: QueryCostVectorV1,
    canonical_bytes: Vec<u8>,
    identity: OperationalQueryFamilyIdentity,
    // Last: attached allocation charges outlive all semantic data fields.
    ownership_lifecycle: riffdb_types::OwnedCapacityLifecycleV1,
}

impl OperationalQueryFamilyV1 {
    /// Constructs and canonically seals one completely enumerated family.
    #[doc(hidden)]
    pub fn checked(
        surface: ResolvedQueryV1,
        presence_parameters: Vec<String>,
        members: Vec<OperationalPlanMemberV1>,
    ) -> Option<Self> {
        let expected_members =
            1_usize.checked_shl(u32::try_from(presence_parameters.len()).ok()?)?;
        let first = members.first()?;
        if presence_parameters.len() > MAX_OPERATIONAL_PRESENCE_PARAMETERS
            || presence_parameters.iter().any(String::is_empty)
            || presence_parameters
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || members.len() != expected_members
            || members.len() > MAX_OPERATIONAL_PLAN_MEMBERS
            || members
                .iter()
                .enumerate()
                .any(|(mask, member)| usize::from(member.presence_mask) != mask)
            || members.iter().any(|member| {
                member.program.contract() != surface.contract()
                    || member.program.contract() != first.program.contract()
                    || member.program.name() != first.program.name()
                    || member.program.partition_route() != first.program.partition_route()
                    || member.program.surface().schemas() != surface.schemas()
            })
        {
            return None;
        }
        let authorization_union = authorization_union(&members)?;
        let maximum_cost = maximum_cost(&members)?;
        let canonical_bytes = encode_family(
            surface.ir_version(),
            surface.canonical_bytes(),
            !surface.aggregates().is_empty(),
            &presence_parameters,
            &members,
            &authorization_union,
            maximum_cost,
        )?;
        let identity = OperationalQueryFamilyIdentity(hash_query_plan(&canonical_bytes));
        Some(Self {
            ownership_lifecycle: Default::default(),
            surface,
            presence_parameters,
            members,
            authorization_union,
            maximum_cost,
            canonical_bytes,
            identity,
        })
    }

    /// Original canonical typed operation surface shared by every member.
    #[must_use]
    pub const fn surface(&self) -> &ResolvedQueryV1 {
        &self.surface
    }

    /// One parameter routing every member to the same partition.
    #[must_use]
    pub fn partition_parameter(&self) -> &str {
        self.members[0].program().partition_parameter()
    }

    /// Optional parameters in canonical name order; positions define mask bits.
    #[must_use]
    pub fn presence_parameters(&self) -> &[String] {
        &self.presence_parameters
    }

    /// Exact aggregate descriptors shared by every family member.
    #[must_use]
    pub fn aggregates(&self) -> &[OperationalAggregateV1] {
        self.surface.aggregates()
    }

    /// Every selectable member in ascending mask order.
    #[must_use]
    pub fn members(&self) -> &[OperationalPlanMemberV1] {
        &self.members
    }

    /// Resolves one member from presence choices in exact parameter order.
    #[must_use]
    pub fn select(&self, presence: &[bool]) -> Option<&OperationalPlanMemberV1> {
        if presence.len() != self.presence_parameters.len() {
            return None;
        }
        let mask = presence
            .iter()
            .enumerate()
            .fold(
                0_usize,
                |mask, (bit, set)| {
                    if *set { mask | (1_usize << bit) } else { mask }
                },
            );
        self.members.get(mask)
    }

    /// Authorization union that must be allowed before any member executes.
    #[must_use]
    pub fn authorization_union(&self) -> &[AuthorizationEntityAccess] {
        &self.authorization_union
    }

    /// Componentwise maximum cost across the finite family.
    #[must_use]
    pub const fn maximum_cost(&self) -> QueryCostVectorV1 {
        self.maximum_cost
    }

    /// Canonical family bytes covered by the identity.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Stable family identity.
    #[must_use]
    pub const fn identity(&self) -> OperationalQueryFamilyIdentity {
        self.identity
    }
}

fn authorization_union(
    members: &[OperationalPlanMemberV1],
) -> Option<Vec<AuthorizationEntityAccess>> {
    let mut by_entity = std::collections::BTreeMap::<String, Vec<AuthorizationEntityAccess>>::new();
    for access in members
        .iter()
        .flat_map(|member| member.program().authorization())
    {
        by_entity
            .entry(access.entity().to_owned())
            .or_default()
            .push(access.clone());
    }
    by_entity
        .into_values()
        .map(|accesses| AuthorizationEntityAccess::internal_union(&accesses))
        .collect()
}

fn maximum_cost(members: &[OperationalPlanMemberV1]) -> Option<QueryCostVectorV1> {
    let mut maximum = QueryCostVectorV1::zero();
    for cost in members.iter().map(|member| member.program().cost()) {
        maximum = QueryCostVectorV1::new(
            maximum.access_steps().max(cost.access_steps()),
            maximum.scanned_index_rows().max(cost.scanned_index_rows()),
            maximum.point_reads().max(cost.point_reads()),
            maximum.dependent_keys().max(cost.dependent_keys()),
            maximum.intermediate_rows().max(cost.intermediate_rows()),
            maximum.projected_values().max(cost.projected_values()),
            maximum
                .encoded_result_bytes()
                .max(cost.encoded_result_bytes()),
        )?;
    }
    Some(maximum)
}

fn encode_family(
    ir_version: u32,
    surface: &[u8],
    has_aggregates: bool,
    parameters: &[String],
    members: &[OperationalPlanMemberV1],
    authorization: &[AuthorizationEntityAccess],
    maximum_cost: QueryCostVectorV1,
) -> Option<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(FAMILY_MAGIC);
    output.extend_from_slice(
        &if ir_version == QUERY_IR_VERSION_EXACT_AGGREGATE_V1 {
            QUERY_IR_VERSION_EXACT_AGGREGATE_V1
        } else if ir_version == crate::QUERY_IR_VERSION_SECRET_OUTPUT_V1 {
            crate::QUERY_IR_VERSION_SECRET_OUTPUT_V1
        } else if has_aggregates {
            QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1
        } else {
            QUERY_IR_VERSION_OPERATIONAL_V1
        }
        .to_be_bytes(),
    );
    write_bytes(&mut output, surface)?;
    write_strings(&mut output, parameters)?;
    write_count(&mut output, members.len())?;
    for member in members {
        output.extend_from_slice(&member.presence_mask.to_be_bytes());
        write_bytes(&mut output, member.program.canonical_bytes())?;
        output.extend_from_slice(member.program.identity().hash().as_bytes());
    }
    write_count(&mut output, authorization.len())?;
    for access in authorization {
        write_text(&mut output, access.entity())?;
        write_strings(&mut output, access.fields())?;
        write_strings(&mut output, access.indexes())?;
        output.extend_from_slice(&access.maximum_rows().to_be_bytes());
    }
    for value in [
        maximum_cost.access_steps(),
        maximum_cost.scanned_index_rows(),
        maximum_cost.point_reads(),
        maximum_cost.dependent_keys(),
        maximum_cost.intermediate_rows(),
        maximum_cost.projected_values(),
        maximum_cost.encoded_result_bytes(),
    ] {
        output.extend_from_slice(&value.to_be_bytes());
    }
    (output.len() <= MAX_QUERY_ARTIFACT_BYTES).then_some(output)
}

fn write_count(output: &mut Vec<u8>, value: usize) -> Option<()> {
    output.extend_from_slice(&u32::try_from(value).ok()?.to_be_bytes());
    Some(())
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> Option<()> {
    write_count(output, value.len())?;
    output.extend_from_slice(value);
    Some(())
}

fn write_text(output: &mut Vec<u8>, value: &str) -> Option<()> {
    write_bytes(output, value.as_bytes())
}

fn write_strings(output: &mut Vec<u8>, values: &[String]) -> Option<()> {
    write_count(output, values.len())?;
    for value in values {
        write_text(output, value)?;
    }
    Some(())
}

// Exhaustive non-durable ownership inventories for exact-provider admission.
riffdb_types::inspect_owned_capacity_struct_v1!(OperationalAggregateV1 {
    owned: [
        name,
        source_binding,
        source_entity,
        parent_binding,
        parent_child_fields,
        parent_driver_fields,
        group_keys,
        measures,
        having,
        order_by,
        maximum_groups
    ],
    inline: [execution_budget]
});

riffdb_types::inspect_owned_capacity_struct_v1!(OperationalAggregateGroupKeyV1 {
    owned: [field, source_field, value_type],
    inline: [time_bucket]
});

riffdb_types::inspect_owned_capacity_struct_v1!(OperationalAggregateMeasureV1 {
    owned: [alias, input_field, filter, result_type],
    inline: [function]
});

riffdb_types::inspect_owned_capacity_copy_v1!(OperationalAggregateTimeBucketV1);

riffdb_types::inspect_owned_capacity_copy_v1!(OperationalAggregateFunctionV1);

riffdb_types::inspect_owned_capacity_copy_v1!(AggregateExecutionBudgetV1);

riffdb_types::inspect_owned_capacity_struct_v1!(OperationalQueryFamilyV1 {
    owned: [
        surface,
        presence_parameters,
        members,
        authorization_union,
        canonical_bytes
    ],
    inline: [maximum_cost, identity],
    lifecycle: ownership_lifecycle
});

riffdb_types::inspect_owned_capacity_struct_v1!(OperationalPlanMemberV1 {
    owned: [program],
    inline: [presence_mask]
});

riffdb_types::inspect_owned_capacity_copy_v1!(OperationalQueryFamilyIdentity);
