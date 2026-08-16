use riffdb_contract_ir::KeySchema;
use riffdb_riffql_syntax::Cardinality;
use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, EntityTypeId, EnumTypeId, EnumVariantId,
    FieldId, IndexId, QueryCostVectorV1, QueryPlanHash, hash_query_plan,
};

use crate::{
    ExactContractIdentity, MAX_QUERY_ARTIFACT_BYTES, ResolvedQueryV1, max_query_page_take,
};

const PROGRAM_MAGIC: &[u8] = b"RIFFDB-QUERY-ACCESS-PROGRAM\0";
const INDEX_GENERATION_MODEL_V2: &[u8] = b"PARTITION-INDEX-GENERATION-V2\0";

/// Direction of one complete ordered index walk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessDirection {
    /// Declared index order.
    Forward,
    /// Complete reverse of declared index order.
    Reverse,
}

/// Runtime row-limit source for one access step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryRowLimit {
    /// Fixed positive source literal.
    Literal(u64),
    /// Typed `Limit` parameter with an optional source default.
    Parameter {
        /// Parameter name without `$`.
        name: String,
        /// Positive default, when declared.
        default: Option<u64>,
    },
}

/// Closed normalized predicate operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryPredicateOperator {
    /// Equality.
    Equal,
    /// Inequality.
    NotEqual,
    /// Less than.
    Less,
    /// Less than or equal.
    LessEqual,
    /// Greater than.
    Greater,
    /// Greater than or equal.
    GreaterEqual,
    /// Membership in one canonical submitted set.
    In,
    /// Logical field is explicitly null.
    IsNull,
    /// Logical field is present and non-null.
    IsNotNull,
    /// Logical field is present, including explicit null.
    Exists,
    /// Leading-byte match through a declared text-key profile.
    Prefix,
}

/// Literal retained in a compiled predicate.
#[derive(Clone, Eq, PartialEq)]
pub enum QueryLiteral {
    /// Unsigned integer source spelling.
    Unsigned(String),
    /// UTF-8 string.
    String(String),
    /// Boolean.
    Boolean(bool),
    /// Null.
    Null,
}

impl std::fmt::Debug for QueryLiteral {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED LITERAL]")
    }
}

/// Closed right-hand input for one normalized predicate.
#[derive(Clone, Eq, PartialEq)]
pub enum QueryPredicateValue {
    /// Typed submitted parameter.
    Parameter(String),
    /// Field from one earlier binding.
    BindingField {
        /// Query-local binding.
        binding: String,
        /// Contract field.
        field: String,
    },
    /// Ordered field collection from one earlier bounded `many` binding.
    BindingFieldSet {
        /// Query-local bounded source binding.
        binding: String,
        /// Contract field supplying one target-key component.
        field: String,
    },
    /// Exact contract enum variant.
    EnumVariant {
        /// Enum declaration name.
        enumeration: String,
        /// Variant name.
        variant: String,
        /// Compiler-internal enum identity.
        #[doc(hidden)]
        type_id: EnumTypeId,
        /// Compiler-internal variant identity.
        #[doc(hidden)]
        variant_id: EnumVariantId,
    },
    /// Plan literal; debug and explain representations redact its value.
    Literal(QueryLiteral),
}

impl std::fmt::Debug for QueryPredicateValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parameter(name) => formatter.debug_tuple("Parameter").field(name).finish(),
            Self::BindingField { binding, field } => formatter
                .debug_struct("BindingField")
                .field("binding", binding)
                .field("field", field)
                .finish(),
            Self::BindingFieldSet { binding, field } => formatter
                .debug_struct("BindingFieldSet")
                .field("binding", binding)
                .field("field", field)
                .finish(),
            Self::EnumVariant {
                enumeration,
                variant,
                ..
            } => formatter
                .debug_struct("EnumVariant")
                .field("enumeration", enumeration)
                .field("variant", variant)
                .finish(),
            Self::Literal(_) => formatter.write_str("Literal([REDACTED])"),
        }
    }
}

/// One normalized conjunctive predicate term.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPredicate {
    field: String,
    operator: QueryPredicateOperator,
    value: QueryPredicateValue,
}

impl QueryPredicate {
    /// Target field on the accessed entity.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Typed comparison operator.
    #[must_use]
    pub const fn operator(&self) -> QueryPredicateOperator {
        self.operator
    }

    /// Typed parameter, dependency, enum, or literal input.
    #[must_use]
    pub const fn value(&self) -> &QueryPredicateValue {
        &self.value
    }

    #[doc(hidden)]
    pub fn checked(
        field: String,
        operator: QueryPredicateOperator,
        value: QueryPredicateValue,
    ) -> Option<Self> {
        (!field.is_empty()).then_some(Self {
            field,
            operator,
            value,
        })
    }
}

/// Closed physical access selected for one binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryAccessKind {
    /// Exact primary-key lookup.
    Point {
        /// Primary-key fields in contract order.
        key_fields: Vec<String>,
    },
    /// Ordered bounded primary-key lookups derived from one earlier `many`.
    DependentPointBatch {
        /// Complete primary-key fields in contract order.
        key_fields: Vec<String>,
        /// Earlier bounded collection binding.
        source_binding: String,
        /// Source field supplying the collection key component.
        source_field: String,
    },
    /// Bounded secondary-index range.
    Index {
        /// Exact contract index name.
        index: String,
        /// Index fields in contract order.
        fields: Vec<String>,
        /// Whole-index traversal direction.
        direction: AccessDirection,
    },
    /// Nearest-neighbor search on a declared vector field (ADR-0091).
    ///
    /// Returns the top-K entities closest to a query vector by the field's
    /// declared distance metric. The exact KNN scan is the reference path;
    /// approximate structures engage only at WP-594.
    Nearest {
        /// The declared vector field name on the entity.
        vector_field: String,
        /// The query vector parameter name.
        vector_parameter: String,
        /// Compiler-proven maximum K. For a literal this is exact; for a
        /// `Limit` parameter this is the shared page-take ceiling.
        k: u32,
    },
}

/// One ordered, bounded access in a closed program.
#[derive(Clone, Eq, PartialEq)]
pub struct QueryAccessStep {
    binding: String,
    entity: String,
    cardinality: Cardinality,
    maximum_rows: u64,
    row_limit: QueryRowLimit,
    access: QueryAccessKind,
    predicates: Vec<QueryPredicate>,
    predicate_fields: Vec<String>,
    selected_fields: Vec<String>,
    result_names: Vec<String>,
    absence_outcome: Option<String>,
    cursor_parameter: Option<String>,
    dependencies: Vec<String>,
    entity_id: EntityTypeId,
    index_id: Option<IndexId>,
    partition_key_schema: KeySchema,
    entity_key_schema: KeySchema,
    index_key_schema: Option<KeySchema>,
}

impl std::fmt::Debug for QueryAccessStep {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryAccessStep")
            .field("binding", &self.binding)
            .field("entity", &self.entity)
            .field("cardinality", &self.cardinality)
            .field("maximum_rows", &self.maximum_rows)
            .field("row_limit", &self.row_limit)
            .field("access", &self.access)
            .field("predicates", &self.predicates)
            .field("predicate_fields", &self.predicate_fields)
            .field("selected_fields", &self.selected_fields)
            .field("result_names", &self.result_names)
            .field("dependencies", &self.dependencies)
            .field("absence_outcome", &self.absence_outcome)
            .field("cursor_parameter", &self.cursor_parameter)
            .finish()
    }
}

impl QueryAccessStep {
    /// Query-local binding name.
    #[must_use]
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Contract entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Expected result cardinality.
    #[must_use]
    pub const fn cardinality(&self) -> Cardinality {
        self.cardinality
    }

    /// Compiler-proven maximum rows.
    #[must_use]
    pub const fn maximum_rows(&self) -> u64 {
        self.maximum_rows
    }

    /// Source of the request-specific row limit.
    #[must_use]
    pub const fn row_limit(&self) -> &QueryRowLimit {
        &self.row_limit
    }

    /// Selected physical access.
    #[must_use]
    pub const fn access(&self) -> &QueryAccessKind {
        &self.access
    }

    /// Normalized conjunctive predicates in source evaluation order.
    #[must_use]
    pub fn predicates(&self) -> &[QueryPredicate] {
        &self.predicates
    }

    /// Fields read while evaluating predicates.
    #[must_use]
    pub fn predicate_fields(&self) -> &[String] {
        &self.predicate_fields
    }

    /// Fields returned from this binding.
    #[must_use]
    pub fn selected_fields(&self) -> &[String] {
        &self.selected_fields
    }

    /// Top-level result field names populated from this binding.
    #[must_use]
    pub fn result_names(&self) -> &[String] {
        &self.result_names
    }

    /// Declared result selected when an exact-one binding is absent.
    #[must_use]
    pub fn absence_outcome(&self) -> Option<&str> {
        self.absence_outcome.as_deref()
    }

    /// Optional cursor parameter associated with this access.
    #[must_use]
    pub fn cursor_parameter(&self) -> Option<&str> {
        self.cursor_parameter.as_deref()
    }

    /// Compiler-internal stable entity identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_entity_id(&self) -> EntityTypeId {
        self.entity_id
    }

    /// Compiler-internal selected index identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_index_id(&self) -> Option<IndexId> {
        self.index_id
    }

    /// Compiler-internal complete entity-key schema.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_entity_key_schema(&self) -> &KeySchema {
        &self.entity_key_schema
    }

    /// Compiler-proven aggregate partition-key schema for this access.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_partition_key_schema(&self) -> &KeySchema {
        &self.partition_key_schema
    }

    /// Compiler-internal complete selected-index key schema.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_index_key_schema(&self) -> Option<&KeySchema> {
        self.index_key_schema.as_ref()
    }

    /// Earlier bindings supplying key values.
    #[must_use]
    pub fn dependencies(&self) -> &[String] {
        &self.dependencies
    }

    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn checked(
        binding: String,
        entity: String,
        cardinality: Cardinality,
        maximum_rows: u64,
        row_limit: QueryRowLimit,
        access: QueryAccessKind,
        predicates: Vec<QueryPredicate>,
        predicate_fields: Vec<String>,
        selected_fields: Vec<String>,
        result_names: Vec<String>,
        absence_outcome: Option<String>,
        cursor_parameter: Option<String>,
        dependencies: Vec<String>,
        entity_id: EntityTypeId,
        index_id: Option<IndexId>,
        partition_key_schema: KeySchema,
        entity_key_schema: KeySchema,
        index_key_schema: Option<KeySchema>,
    ) -> Option<Self> {
        let row_limit_is_valid = match &row_limit {
            QueryRowLimit::Literal(value) => {
                *value > 0 && *value <= maximum_rows && *value <= max_query_page_take()
            }
            QueryRowLimit::Parameter { name, default } => {
                !name.is_empty()
                    && default.as_ref().is_none_or(|value| {
                        *value > 0 && *value <= maximum_rows && *value <= max_query_page_take()
                    })
            }
        };
        let access_is_valid = match &access {
            QueryAccessKind::Point { key_fields } => {
                !key_fields.is_empty()
                    && !predicates.iter().any(|predicate| {
                        matches!(predicate.value, QueryPredicateValue::BindingFieldSet { .. })
                    })
            }
            QueryAccessKind::Index { fields, .. } => {
                !fields.is_empty()
                    && !predicates.iter().any(|predicate| {
                        matches!(predicate.value, QueryPredicateValue::BindingFieldSet { .. })
                    })
            }
            QueryAccessKind::DependentPointBatch {
                key_fields,
                source_binding,
                source_field,
            } => {
                let matching_sets = predicates
                    .iter()
                    .filter(|predicate| {
                        predicate.operator == QueryPredicateOperator::In
                            && matches!(
                                &predicate.value,
                                QueryPredicateValue::BindingFieldSet { binding, field }
                                    if binding == source_binding && field == source_field
                            )
                    })
                    .count();
                !key_fields.is_empty()
                    && !source_binding.is_empty()
                    && !source_field.is_empty()
                    && cardinality == Cardinality::Many
                    && absence_outcome.is_some()
                    && cursor_parameter.is_none()
                    && dependencies.binary_search(source_binding).is_ok()
                    && matching_sets == 1
                    && predicates
                        .iter()
                        .filter(|predicate| {
                            matches!(predicate.value, QueryPredicateValue::BindingFieldSet { .. })
                        })
                        .count()
                        == 1
            }
            QueryAccessKind::Nearest {
                vector_field,
                vector_parameter,
                k,
            } => {
                !vector_field.is_empty()
                    && !vector_parameter.is_empty()
                    && *k > 0
                    && u64::from(*k) <= max_query_page_take()
                    && maximum_rows == u64::from(*k)
                    && match &row_limit {
                        QueryRowLimit::Literal(value) => *value == u64::from(*k),
                        QueryRowLimit::Parameter { .. } => true,
                    }
                    && cardinality == Cardinality::Many
            }
        };
        if binding.is_empty()
            || entity.is_empty()
            || maximum_rows == 0
            || !row_limit_is_valid
            || !access_is_valid
            || predicate_fields.windows(2).any(|pair| pair[0] >= pair[1])
            || selected_fields.windows(2).any(|pair| pair[0] >= pair[1])
            || result_names.windows(2).any(|pair| pair[0] >= pair[1])
            || dependencies.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return None;
        }
        Some(Self {
            binding,
            entity,
            cardinality,
            maximum_rows,
            row_limit,
            access,
            predicates,
            predicate_fields,
            selected_fields,
            result_names,
            absence_outcome,
            cursor_parameter,
            dependencies,
            entity_id,
            index_id,
            partition_key_schema,
            entity_key_schema,
            index_key_schema,
        })
    }
}

/// Complete compiler-derived read authority for one entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationEntityAccess {
    entity: String,
    fields: Vec<String>,
    indexes: Vec<String>,
    maximum_rows: u64,
    entity_id: EntityTypeId,
    field_ids: Vec<FieldId>,
    index_ids: Vec<IndexId>,
}

impl AuthorizationEntityAccess {
    /// Contract entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Complete visible/read field set in exact name order.
    #[must_use]
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Required index names in exact order.
    #[must_use]
    pub fn indexes(&self) -> &[String] {
        &self.indexes
    }

    /// Aggregate maximum rows for the request.
    #[must_use]
    pub const fn maximum_rows(&self) -> u64 {
        self.maximum_rows
    }

    /// Resolves one compiler-internal field identity by its exact public name.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_field_id(&self, name: &str) -> Option<FieldId> {
        self.fields
            .binary_search_by(|candidate| candidate.as_str().cmp(name))
            .ok()
            .map(|index| self.field_ids[index])
    }

    /// Compiler-internal name/field-ID pairs in canonical name order.
    #[doc(hidden)]
    pub fn internal_fields(&self) -> impl ExactSizeIterator<Item = (&str, FieldId)> {
        self.fields
            .iter()
            .map(String::as_str)
            .zip(self.field_ids.iter().copied())
    }

    /// Forms the least authority covering every supplied access for one exact entity.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_union(accesses: &[Self]) -> Option<Self> {
        let first = accesses.first()?;
        let mut fields = std::collections::BTreeMap::new();
        let mut indexes = std::collections::BTreeMap::new();
        let mut maximum_rows = 0_u64;
        for access in accesses {
            if access.entity != first.entity || access.entity_id != first.entity_id {
                return None;
            }
            maximum_rows = maximum_rows.max(access.maximum_rows);
            for (name, id) in access.fields.iter().zip(access.field_ids.iter().copied()) {
                if fields
                    .insert(name.clone(), id)
                    .is_some_and(|prior| prior != id)
                {
                    return None;
                }
            }
            for (name, id) in access.indexes.iter().zip(access.index_ids.iter().copied()) {
                if indexes
                    .insert(name.clone(), id)
                    .is_some_and(|prior| prior != id)
                {
                    return None;
                }
            }
        }
        Self::checked(
            first.entity.clone(),
            fields.keys().cloned().collect(),
            indexes.keys().cloned().collect(),
            maximum_rows,
            first.entity_id,
            fields.into_values().collect(),
            indexes.into_values().collect(),
        )
    }

    #[doc(hidden)]
    pub fn checked(
        entity: String,
        fields: Vec<String>,
        indexes: Vec<String>,
        maximum_rows: u64,
        entity_id: EntityTypeId,
        field_ids: Vec<FieldId>,
        index_ids: Vec<IndexId>,
    ) -> Option<Self> {
        if entity.is_empty()
            || maximum_rows == 0
            || fields.len() != field_ids.len()
            || indexes.len() != index_ids.len()
            || fields.windows(2).any(|pair| pair[0] >= pair[1])
            || indexes.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return None;
        }
        Some(Self {
            entity,
            fields,
            indexes,
            maximum_rows,
            entity_id,
            field_ids,
            index_ids,
        })
    }
}

/// Stable identity of one canonical query access program.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct QueryPlanIdentity(QueryPlanHash);

impl QueryPlanIdentity {
    /// Typed domain-separated digest.
    #[must_use]
    pub const fn hash(self) -> QueryPlanHash {
        self.0
    }
}

/// Bounded name-only explain representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPlanExplain {
    lines: Vec<String>,
}

impl QueryPlanExplain {
    /// Stable explain lines.
    #[must_use]
    pub fn lines(&self) -> &[String] {
        &self.lines
    }
}

/// Closed, ordered, same-partition RiffQL v1 access program.
#[derive(Clone, Eq, PartialEq)]
pub struct QueryAccessProgramV1 {
    contract: ExactContractIdentity,
    surface: ResolvedQueryV1,
    name: Option<String>,
    partition_parameter: String,
    steps: Vec<QueryAccessStep>,
    authorization: Vec<AuthorizationEntityAccess>,
    cost: QueryCostVectorV1,
    canonical_bytes: Vec<u8>,
    identity: QueryPlanIdentity,
    explain: QueryPlanExplain,
}

impl std::fmt::Debug for QueryAccessProgramV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryAccessProgramV1")
            .field("contract_lineage", &self.contract.lineage().as_str())
            .field("contract_version", &self.contract.version())
            .field("name", &self.name)
            .field("partition_parameter", &self.partition_parameter)
            .field("steps", &self.steps)
            .field("authorization", &self.authorization)
            .field("cost", &self.cost)
            .field("identity", &self.identity)
            .finish()
    }
}

impl QueryAccessProgramV1 {
    #[doc(hidden)]
    pub fn checked(
        contract: ExactContractIdentity,
        surface: ResolvedQueryV1,
        name: Option<String>,
        partition_parameter: String,
        steps: Vec<QueryAccessStep>,
        authorization: Vec<AuthorizationEntityAccess>,
        cost: QueryCostVectorV1,
    ) -> Option<Self> {
        if surface.contract() != &contract
            || partition_parameter.is_empty()
            || steps.is_empty()
            || authorization.is_empty()
            || authorization
                .windows(2)
                .any(|pair| pair[0].entity >= pair[1].entity)
            || cost.access_steps() != steps.len() as u64
        {
            return None;
        }
        let canonical_bytes = encode_program(
            ProgramSurface {
                contract: &contract,
                ir_version: surface.ir_version(),
                canonical_bytes: surface.canonical_bytes(),
                name: name.as_deref(),
            },
            &partition_parameter,
            &steps,
            &authorization,
            cost,
        )?;
        let identity = QueryPlanIdentity(hash_query_plan(&canonical_bytes));
        let explain = build_explain(&partition_parameter, &steps, &authorization, cost);
        Some(Self {
            contract,
            surface,
            name,
            partition_parameter,
            steps,
            authorization,
            cost,
            canonical_bytes,
            identity,
            explain,
        })
    }

    /// Exact contract identity.
    #[must_use]
    pub const fn contract(&self) -> &ExactContractIdentity {
        &self.contract
    }

    /// Complete resolved parameter/result schema and canonical symbolic surface.
    #[must_use]
    pub const fn surface(&self) -> &ResolvedQueryV1 {
        &self.surface
    }

    /// Optional declared query name.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// One parameter routing every access to the same partition.
    #[must_use]
    pub fn partition_parameter(&self) -> &str {
        &self.partition_parameter
    }

    /// Ordered access steps.
    #[must_use]
    pub fn steps(&self) -> &[QueryAccessStep] {
        &self.steps
    }

    /// Complete authorization requirement set.
    #[must_use]
    pub fn authorization(&self) -> &[AuthorizationEntityAccess] {
        &self.authorization
    }

    /// Canonical whole-request maximum work covered by the plan identity.
    #[must_use]
    pub const fn cost(&self) -> QueryCostVectorV1 {
        self.cost
    }

    /// Resolves the compiler-derived access record for one exact entity name.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_entity_access(&self, name: &str) -> Option<&AuthorizationEntityAccess> {
        self.authorization
            .binary_search_by(|candidate| candidate.entity.as_str().cmp(name))
            .ok()
            .map(|index| &self.authorization[index])
    }

    /// Canonical bytes covered by the plan identity.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Stable domain-separated plan identity.
    #[must_use]
    pub const fn identity(&self) -> QueryPlanIdentity {
        self.identity
    }

    /// Bounded name-only explain form.
    #[must_use]
    pub const fn explain(&self) -> &QueryPlanExplain {
        &self.explain
    }
}

struct ProgramSurface<'a> {
    contract: &'a ExactContractIdentity,
    ir_version: u32,
    canonical_bytes: &'a [u8],
    name: Option<&'a str>,
}

fn encode_program(
    surface: ProgramSurface<'_>,
    partition_parameter: &str,
    steps: &[QueryAccessStep],
    authorization: &[AuthorizationEntityAccess],
    cost: QueryCostVectorV1,
) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(PROGRAM_MAGIC);
    out.extend_from_slice(&surface.ir_version.to_be_bytes());
    // Public cursor envelopes bind the plan hash. This reviewed marker makes
    // pre-WP-375 prefix-epoch cursors fail closed across the format cut.
    out.extend_from_slice(INDEX_GENERATION_MODEL_V2);
    write_text(&mut out, surface.contract.lineage().as_str())?;
    out.extend_from_slice(&surface.contract.version().get().to_be_bytes());
    out.extend_from_slice(surface.contract.bundle_hash().as_bytes());
    write_count(&mut out, surface.canonical_bytes.len())?;
    out.extend_from_slice(surface.canonical_bytes);
    write_text(&mut out, surface.name.unwrap_or(""))?;
    write_text(&mut out, partition_parameter)?;
    write_count(&mut out, steps.len())?;
    for step in steps {
        write_text(&mut out, &step.binding)?;
        write_text(&mut out, &step.entity)?;
        out.push(match step.cardinality {
            Cardinality::One => 1,
            Cardinality::Maybe => 2,
            Cardinality::Many => 3,
        });
        out.extend_from_slice(&step.maximum_rows.to_be_bytes());
        match &step.row_limit {
            QueryRowLimit::Literal(value) => {
                out.push(1);
                out.extend_from_slice(&value.to_be_bytes());
            }
            QueryRowLimit::Parameter { name, default } => {
                out.push(2);
                write_text(&mut out, name)?;
                out.extend_from_slice(&default.unwrap_or(0).to_be_bytes());
            }
        }
        match &step.access {
            QueryAccessKind::Point { key_fields } => {
                out.push(1);
                write_strings(&mut out, key_fields)?;
            }
            QueryAccessKind::Index {
                index,
                fields,
                direction,
            } => {
                out.push(2);
                write_text(&mut out, index)?;
                write_strings(&mut out, fields)?;
                out.push(match direction {
                    AccessDirection::Forward => 1,
                    AccessDirection::Reverse => 2,
                });
            }
            QueryAccessKind::DependentPointBatch {
                key_fields,
                source_binding,
                source_field,
            } => {
                out.push(3);
                write_strings(&mut out, key_fields)?;
                write_text(&mut out, source_binding)?;
                write_text(&mut out, source_field)?;
            }
            QueryAccessKind::Nearest {
                vector_field,
                vector_parameter,
                k,
            } => {
                out.push(4);
                write_text(&mut out, vector_field)?;
                write_text(&mut out, vector_parameter)?;
                out.extend_from_slice(&k.to_be_bytes());
            }
        }
        write_count(&mut out, step.predicates.len())?;
        for predicate in &step.predicates {
            write_text(&mut out, &predicate.field)?;
            out.push(predicate_operator_tag(predicate.operator));
            encode_predicate_value(&mut out, &predicate.value)?;
        }
        write_strings(&mut out, &step.predicate_fields)?;
        write_strings(&mut out, &step.selected_fields)?;
        write_strings(&mut out, &step.result_names)?;
        write_text(&mut out, step.absence_outcome.as_deref().unwrap_or(""))?;
        write_text(&mut out, step.cursor_parameter.as_deref().unwrap_or(""))?;
        write_strings(&mut out, &step.dependencies)?;
    }
    write_count(&mut out, authorization.len())?;
    for access in authorization {
        write_text(&mut out, &access.entity)?;
        write_strings(&mut out, &access.fields)?;
        write_strings(&mut out, &access.indexes)?;
        out.extend_from_slice(&access.maximum_rows.to_be_bytes());
    }
    out.extend_from_slice(&cost.access_steps().to_be_bytes());
    out.extend_from_slice(&cost.scanned_index_rows().to_be_bytes());
    out.extend_from_slice(&cost.point_reads().to_be_bytes());
    out.extend_from_slice(&cost.dependent_keys().to_be_bytes());
    out.extend_from_slice(&cost.intermediate_rows().to_be_bytes());
    out.extend_from_slice(&cost.projected_values().to_be_bytes());
    out.extend_from_slice(&cost.encoded_result_bytes().to_be_bytes());
    (out.len() <= MAX_QUERY_ARTIFACT_BYTES).then_some(out)
}

fn predicate_operator_tag(operator: QueryPredicateOperator) -> u8 {
    match operator {
        QueryPredicateOperator::Equal => 1,
        QueryPredicateOperator::NotEqual => 2,
        QueryPredicateOperator::Less => 3,
        QueryPredicateOperator::LessEqual => 4,
        QueryPredicateOperator::Greater => 5,
        QueryPredicateOperator::GreaterEqual => 6,
        QueryPredicateOperator::In => 7,
        QueryPredicateOperator::IsNull => 8,
        QueryPredicateOperator::IsNotNull => 9,
        QueryPredicateOperator::Exists => 10,
        QueryPredicateOperator::Prefix => 11,
    }
}

fn encode_predicate_value(out: &mut Vec<u8>, value: &QueryPredicateValue) -> Option<()> {
    match value {
        QueryPredicateValue::Parameter(name) => {
            out.push(1);
            write_text(out, name)
        }
        QueryPredicateValue::BindingField { binding, field } => {
            out.push(2);
            write_text(out, binding)?;
            write_text(out, field)
        }
        QueryPredicateValue::EnumVariant {
            enumeration,
            variant,
            type_id,
            variant_id,
        } => {
            out.push(3);
            write_text(out, enumeration)?;
            write_text(out, variant)?;
            out.extend_from_slice(&type_id.get().to_be_bytes());
            out.extend_from_slice(&variant_id.get().to_be_bytes());
            Some(())
        }
        QueryPredicateValue::Literal(literal) => {
            out.push(4);
            match literal {
                QueryLiteral::Unsigned(value) => {
                    out.push(1);
                    write_text(out, value)
                }
                QueryLiteral::String(value) => {
                    out.push(2);
                    write_text(out, value)
                }
                QueryLiteral::Boolean(value) => {
                    out.push(3);
                    out.push(u8::from(*value));
                    Some(())
                }
                QueryLiteral::Null => {
                    out.push(4);
                    Some(())
                }
            }
        }
        QueryPredicateValue::BindingFieldSet { binding, field } => {
            out.push(5);
            write_text(out, binding)?;
            write_text(out, field)
        }
    }
}

fn write_count(out: &mut Vec<u8>, value: usize) -> Option<()> {
    out.extend_from_slice(&u32::try_from(value).ok()?.to_be_bytes());
    Some(())
}

fn write_text(out: &mut Vec<u8>, value: &str) -> Option<()> {
    write_count(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Some(())
}

fn write_strings(out: &mut Vec<u8>, values: &[String]) -> Option<()> {
    write_count(out, values.len())?;
    for value in values {
        write_text(out, value)?;
    }
    Some(())
}

fn build_explain(
    partition_parameter: &str,
    steps: &[QueryAccessStep],
    authorization: &[AuthorizationEntityAccess],
    cost: QueryCostVectorV1,
) -> QueryPlanExplain {
    let mut lines = vec![format!("partition ${partition_parameter}")];
    for step in steps {
        let access = match &step.access {
            QueryAccessKind::Point { .. } => "primary-key".to_owned(),
            QueryAccessKind::DependentPointBatch {
                source_binding,
                source_field,
                ..
            } => format!("dependent primary-key batch from {source_binding}.{source_field}"),
            QueryAccessKind::Index {
                index, direction, ..
            } => format!(
                "index {index} {}",
                match direction {
                    AccessDirection::Forward => "forward",
                    AccessDirection::Reverse => "reverse",
                }
            ),
            QueryAccessKind::Nearest {
                vector_field, k, ..
            } => format!("nearest({vector_field}, k={k})"),
        };
        lines.push(format!(
            "{}: {} via {} max {}",
            step.binding, step.entity, access, step.maximum_rows
        ));
    }
    for access in authorization {
        lines.push(format!(
            "authorize {} fields [{}] indexes [{}] max {}",
            access.entity,
            access.fields.join(","),
            access.indexes.join(","),
            access.maximum_rows
        ));
    }
    lines.push(format!(
        "cost steps={} scans={} points={} dependent_keys={} intermediates={} projected_values={} result_bytes={}",
        cost.access_steps(),
        cost.scanned_index_rows(),
        cost.point_reads(),
        cost.dependent_keys(),
        cost.intermediate_rows(),
        cost.projected_values(),
        cost.encoded_result_bytes()
    ));
    QueryPlanExplain { lines }
}

#[allow(dead_code)]
fn _identity_type_guard(
    _: ContractLineage,
    _: ContractVersion,
    _: ContractBundleHash,
    _: EntityTypeId,
    _: FieldId,
    _: IndexId,
) {
}
