//! Org-scoped query executor over a published snapshot (D6).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use riffdb_contract_ir::{ValueType, ValueTypeTag};
use riffdb_policy::{AuthorizedProjectedRowAdmissionV1, MAX_PROJECTED_POLICY_CANDIDATES_V1};
use riffdb_types::{
    CanonicalValue, EntityKey, EntityTypeId, EntityVersion, FieldId, encode_canonical_value,
};

use crate::definition::RegisteredDefinition;
use crate::store::{ColumnarSnapshot, MergedRow, OrgKey, PrimaryKeyBytes};

/// Hard budgets for scan-based prototype execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryBudget {
    /// Maximum rows examined while scanning an org partition.
    pub max_scanned_rows: usize,
    /// Maximum group cardinality for group-by queries.
    pub max_group_cardinality: usize,
}

impl Default for QueryBudget {
    fn default() -> Self {
        Self {
            max_scanned_rows: 100_000,
            max_group_cardinality: 10_000,
        }
    }
}

/// Equality or range predicate on a projected column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnPredicate {
    /// Column equals value.
    Eq {
        /// Projected field id.
        field: FieldId,
        /// Comparison value.
        value: CanonicalValue,
    },
    /// Inclusive lower / exclusive upper range (either bound optional).
    Range {
        /// Projected field id.
        field: FieldId,
        /// Inclusive lower bound.
        low: Option<CanonicalValue>,
        /// Exclusive upper bound.
        high: Option<CanonicalValue>,
    },
}

/// Sort direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortDirection {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
}

/// One sort key; primary key is always the final tie-break.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderSpec {
    /// Projected field or primary-key component to sort by.
    pub field: FieldId,
    /// Direction.
    pub direction: SortDirection,
}

/// Aggregate function over a projected column (or count-star).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateOp {
    /// Count of matching rows.
    Count,
    /// Sum of a numeric column.
    Sum {
        /// Field to sum.
        field: FieldId,
    },
    /// Minimum of a column.
    Min {
        /// Field to minimize.
        field: FieldId,
    },
    /// Maximum of a column.
    Max {
        /// Field to maximize.
        field: FieldId,
    },
}

/// Optional group-by specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupBySpec {
    /// Grouping columns (projected fields).
    pub keys: Vec<FieldId>,
    /// Aggregates computed per group.
    pub aggregates: Vec<AggregateOp>,
}

/// Typed query request. Org scope is a required field — omitting it is unrepresentable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnarQueryRequest {
    /// Exactly one organization scope value (ADR-0086 §9 / ADR-0087).
    pub org_scope: CanonicalValue,
    /// Selected projected fields. Empty means all projected fields (CP1 compat).
    ///
    /// Select narrowing applies to [`QueryResult::Rows`] only. For requests
    /// with `aggregate` or `group_by` set, the select list is still validated
    /// (duplicates and unprojected fields are rejected typed) but otherwise
    /// ignored by design: aggregate and group results carry no row cells, so
    /// there is nothing for the select list to narrow.
    pub select: Vec<FieldId>,
    /// Conjunctive predicates over projected columns.
    pub predicates: Vec<ColumnPredicate>,
    /// Optional sort keys (primary key tie-break always applied).
    pub order: Vec<OrderSpec>,
    /// Optional limit after sort.
    pub limit: Option<usize>,
    /// When set, returns aggregates (optionally grouped) instead of rows.
    pub group_by: Option<GroupBySpec>,
    /// When set without group_by, compute a single aggregate over matching rows.
    pub aggregate: Option<AggregateOp>,
    /// Scan and group budgets.
    pub budget: QueryBudget,
}

/// One result row: selected cells plus field-id-addressable primary-key values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRow {
    /// Selected projected cells aligned with [`QueryRows::fields`].
    pub cells: Vec<CanonicalValue>,
    /// Primary-key component values aligned with [`QueryRows::primary_key_fields`].
    pub primary_key: Vec<CanonicalValue>,
}

impl QueryRow {
    /// Returns the primary-key component for `field`, when it is part of the entity key.
    #[must_use]
    pub fn primary_key_value<'a>(
        &'a self,
        fields: &[FieldId],
        field: FieldId,
    ) -> Option<&'a CanonicalValue> {
        fields
            .iter()
            .position(|id| *id == field)
            .map(|idx| &self.primary_key[idx])
    }

    /// Returns the selected cell for `field`, when it is part of the select list.
    #[must_use]
    pub fn cell_value<'a>(
        &'a self,
        fields: &[FieldId],
        field: FieldId,
    ) -> Option<&'a CanonicalValue> {
        fields
            .iter()
            .position(|id| *id == field)
            .map(|idx| &self.cells[idx])
    }
}

/// Row-shaped query result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRows {
    /// Selected projected field order for each row's cells.
    pub fields: Vec<FieldId>,
    /// Entity primary-key field order for each row's primary_key vector.
    pub primary_key_fields: Vec<FieldId>,
    /// Matching rows.
    pub rows: Vec<QueryRow>,
}

/// Aggregate or group result cell.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateValue {
    /// Count result.
    Count(u64),
    /// Sum result (i64/u64 promoted to i128 for safety in prototype).
    Sum(i128),
    /// Min/max or missing.
    Scalar(Option<CanonicalValue>),
}

/// Full query result variants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryResult {
    /// Row projection.
    Rows(QueryRows),
    /// Single aggregate over the matching set.
    Aggregate(AggregateValue),
    /// Grouped aggregates: each entry is (group key cells, aggregate values).
    Groups {
        /// Group key field order.
        key_fields: Vec<FieldId>,
        /// Groups in group-key order.
        groups: Vec<(Vec<CanonicalValue>, Vec<AggregateValue>)>,
    },
}

/// Query execution failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryError {
    /// Predicate, order, select, or group references a field not addressable.
    UnknownField {
        /// Field id.
        field_id: FieldId,
    },
    /// Select list contains a duplicate field.
    DuplicateSelectField {
        /// Field id.
        field_id: FieldId,
    },
    /// Select references a field that is not in the projection definition.
    UnprojectedSelectField {
        /// Field id.
        field_id: FieldId,
    },
    /// OrderSpec field is a primary-key component whose encoding does not
    /// preserve CanonicalValue order (length-prefixed string/bytes).
    OrderNotValueOrderPreserving {
        /// Field id.
        field_id: FieldId,
        /// Rejected value type tag.
        tag: ValueTypeTag,
    },
    /// Primary-key bytes could not be decoded with the registered key schema.
    PrimaryKeyDecode,
    /// The authoritative admission proof did not cover this exact entity and
    /// complete projection candidate set.
    PolicyAdmissionMismatch,
    /// Scan budget exceeded.
    ScanBudgetExceeded {
        /// Configured max.
        max: usize,
    },
    /// Group cardinality budget exceeded.
    GroupCardinalityExceeded {
        /// Configured max.
        max: usize,
    },
    /// Aggregate not defined for the column type.
    InvalidAggregate(&'static str),
    /// Org scope encoding failed.
    InvalidOrgScope,
    /// Org scope value does not match the registered org field's type.
    OrgScopeTypeMismatch {
        /// Type tag the registered org scope field requires.
        expected: ValueTypeTag,
    },
    /// A vector's dimension does not match the declared field dimension.
    ///
    /// Raised for a query vector that does not match the contract-declared
    /// dimension, and for a stored cell whose dimension has skewed from the
    /// declaration (typed error, never a panic).
    VectorDimensionMismatch {
        /// The contract-declared dimension of the vector field.
        expected: u32,
        /// The observed dimension.
        actual: u32,
    },
    /// A nearest query names a projected field that exists but is not
    /// vector-typed (previously mislabeled as `UnknownField`).
    NotAVectorField {
        /// Field id.
        field_id: FieldId,
    },
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownField { field_id } => {
                write!(f, "unknown field {}", field_id.get())
            }
            Self::DuplicateSelectField { field_id } => {
                write!(f, "duplicate select field {}", field_id.get())
            }
            Self::UnprojectedSelectField { field_id } => {
                write!(f, "select field {} is not projected", field_id.get())
            }
            Self::OrderNotValueOrderPreserving { field_id, tag } => {
                write!(
                    f,
                    "order field {} type {tag:?} is not value-order-preserving in the entity key",
                    field_id.get()
                )
            }
            Self::PrimaryKeyDecode => f.write_str("primary key decode failed"),
            Self::PolicyAdmissionMismatch => {
                f.write_str("projected row-policy admission did not cover the candidate set")
            }
            Self::ScanBudgetExceeded { max } => write!(f, "scan budget exceeded (max {max})"),
            Self::GroupCardinalityExceeded { max } => {
                write!(f, "group cardinality exceeded (max {max})")
            }
            Self::InvalidAggregate(message) => write!(f, "invalid aggregate: {message}"),
            Self::InvalidOrgScope => f.write_str("invalid org scope value"),
            Self::OrgScopeTypeMismatch { expected } => {
                write!(f, "org scope value type mismatch (expected {expected:?})")
            }
            Self::VectorDimensionMismatch { expected, actual } => {
                write!(
                    f,
                    "vector dimension {actual} does not match the declared dimension {expected}"
                )
            }
            Self::NotAVectorField { field_id } => {
                write!(f, "field {} is not vector-typed", field_id.get())
            }
        }
    }
}

impl std::error::Error for QueryError {}

/// Public snapshot-query entry: serve reads from an already-published snapshot
/// without holding the engine apply lock (CP2b read path).
pub fn query_snapshot(
    definition: &RegisteredDefinition,
    snapshot: &ColumnarSnapshot,
    request: &ColumnarQueryRequest,
) -> Result<QueryResult, QueryError> {
    execute_query(definition, snapshot, request, None)
}

/// Executes a protected projected query using one opaque authoritative
/// admission proof.
///
/// Admission is checked against the complete org-partition candidate set and
/// applied before caller predicates, scan charging, limits, grouping, or
/// aggregation. This is an internal first-party boundary used by the
/// authoritative query adapters; application transports never receive the
/// proof or an editable allow list.
#[doc(hidden)]
pub fn query_snapshot_with_policy_admission(
    definition: &RegisteredDefinition,
    snapshot: &ColumnarSnapshot,
    request: &ColumnarQueryRequest,
    admission: &AuthorizedProjectedRowAdmissionV1,
) -> Result<QueryResult, QueryError> {
    execute_query(definition, snapshot, request, Some(admission))
}

/// Nearest-neighbor query request for the columnar vector projection.
#[derive(Clone, Debug)]
pub struct NearestQueryRequest {
    /// Exactly one organization scope value (ADR-0086 §9 / ADR-0087).
    pub org_scope: CanonicalValue,
    /// The projected vector field to search.
    pub vector_field: FieldId,
    /// The query vector.
    pub query_vector: riffdb_types::CanonicalVector,
    /// Maximum results (K). Mandatory and positive.
    pub k: u32,
    /// Distance metric for scoring.
    pub metric: riffdb_types::DistanceMetric,
    /// Row filters applied BEFORE distance ranking (VEC-006/VEC-007).
    ///
    /// A row failing any predicate never enters the candidate set: it is
    /// not scored, not ranked, and cannot influence distances, ordering, or
    /// the result count.
    ///
    /// The guarantee is conditional on the caller: an EMPTY vector is valid
    /// and means "no filtering". The engine enforces that supplied
    /// predicates filter before ranking; it cannot know whether the RIGHT
    /// predicates were supplied. Binding compiled row policies into this set
    /// is the caller's obligation (WP-572 runtime evaluator; recorded in
    /// WP-593's deferred entry).
    pub predicates: Vec<ColumnPredicate>,
    /// Scan budget.
    pub budget: QueryBudget,
}

/// One exact nearest candidate presented to principal-policy admission before
/// vector validation, distance computation, or ranking.
///
/// Fields borrow one captured columnar snapshot. The admission implementation
/// may inspect values but cannot mutate the candidate or projection state.
#[derive(Clone, Copy, Debug)]
pub struct NearestCandidate<'a> {
    entity_type_id: EntityTypeId,
    entity_version: EntityVersion,
    primary_key_fields: &'a [FieldId],
    primary_key: &'a [CanonicalValue],
    projected_fields: &'a [FieldId],
    cells: &'a [CanonicalValue],
}

impl<'a> NearestCandidate<'a> {
    /// Projected entity type.
    #[must_use]
    pub const fn entity_type_id(&self) -> EntityTypeId {
        self.entity_type_id
    }

    /// Entity version captured in the projection row.
    #[must_use]
    pub const fn entity_version(&self) -> EntityVersion {
        self.entity_version
    }

    /// Primary-key field order.
    #[must_use]
    pub const fn primary_key_fields(&self) -> &'a [FieldId] {
        self.primary_key_fields
    }

    /// Primary-key values aligned with [`Self::primary_key_fields`].
    #[must_use]
    pub const fn primary_key(&self) -> &'a [CanonicalValue] {
        self.primary_key
    }

    /// Projected field order.
    #[must_use]
    pub const fn projected_fields(&self) -> &'a [FieldId] {
        self.projected_fields
    }

    /// Projected values aligned with [`Self::projected_fields`].
    #[must_use]
    pub const fn cells(&self) -> &'a [CanonicalValue] {
        self.cells
    }

    /// Resolves one field from the entity key or projected cells.
    ///
    /// Malformed row shapes fail closed as `None`; they never panic through
    /// unchecked indexing while policy admission is in progress.
    #[must_use]
    pub fn field_value(&self, field: FieldId) -> Option<&'a CanonicalValue> {
        self.primary_key_fields
            .iter()
            .position(|candidate| *candidate == field)
            .and_then(|position| self.primary_key.get(position))
            .or_else(|| {
                self.projected_fields
                    .iter()
                    .position(|candidate| *candidate == field)
                    .and_then(|position| self.cells.get(position))
            })
    }
}

/// Narrow infrastructure admission port for exact nearest candidates.
///
/// This is not an application policy language, request predicate, or bypass.
/// Production composition must adapt the compiler-owned closed row-policy
/// evaluator and current principal facts to this port; application callbacks
/// and request-supplied authorization predicates remain prohibited by
/// ADR-0111. The columnar engine invokes the port after ordinary predicates
/// but before reading or validating the vector cell and before scoring.
/// `Ok(false)` excludes the row completely; an error aborts the complete query
/// without partial output.
pub trait NearestCandidateAdmission {
    /// Caller-owned policy/backend failure retained for internal handling.
    type Error;

    /// Decides whether one candidate may enter distance scoring.
    fn admit(&mut self, candidate: NearestCandidate<'_>) -> Result<bool, Self::Error>;
}

/// Exact-nearest failure preserving columnar validation and caller-owned
/// admission failures as distinct closed branches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NearestQueryAdmissionError<E> {
    /// Columnar request, bound, or snapshot-integrity failure.
    Query(QueryError),
    /// Principal-policy admission failed.
    Admission(E),
}

impl<E> From<QueryError> for NearestQueryAdmissionError<E> {
    fn from(error: QueryError) -> Self {
        Self::Query(error)
    }
}

impl<E> fmt::Display for NearestQueryAdmissionError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(error) => error.fmt(formatter),
            Self::Admission(_) => formatter.write_str("nearest candidate admission failed"),
        }
    }
}

impl<E: fmt::Debug + 'static> std::error::Error for NearestQueryAdmissionError<E> {}

/// One nearest-neighbor result row: primary key values and distance score.
#[derive(Clone, Debug, PartialEq)]
pub struct NearestResultRow {
    /// Primary-key component values aligned with
    /// [`RegisteredDefinition::primary_key_fields`].
    pub primary_key: Vec<CanonicalValue>,
    /// All projected cell values for this entity row.
    pub cells: Vec<CanonicalValue>,
    /// Distance from the query vector (lower is closer for all metrics).
    pub distance: f32,
}

/// Nearest-neighbor query result.
#[derive(Clone, Debug)]
pub struct NearestQueryResult {
    /// Primary-key field order.
    pub primary_key_fields: Vec<FieldId>,
    /// Projected field order.
    pub projected_fields: Vec<FieldId>,
    /// Matching rows ordered by distance ascending (closest first).
    pub rows: Vec<NearestResultRow>,
    /// Rows examined while scanning the org partition — every merged row
    /// visited, including rows the predicates excluded, and never rows from
    /// other organizations. This is the honest scan-work count an executor
    /// adapter must report as `QueryNearestPage::scanned_rows` for fuel
    /// accounting (ADR-0087); without it the adapter would have to fabricate
    /// the number the executor demands.
    pub scanned_rows: u64,
}

/// Executes a nearest-neighbor query against a published snapshot using only
/// the request's scalar predicates.
///
/// This compatibility entry point does not perform principal-policy
/// admission. Production composition must use
/// [`nearest_query_snapshot_with_admission`] when a principal policy applies.
/// Returns up to `k` rows ordered by distance ascending (closest first).
pub fn nearest_query_snapshot(
    definition: &RegisteredDefinition,
    snapshot: &ColumnarSnapshot,
    request: &NearestQueryRequest,
) -> Result<NearestQueryResult, QueryError> {
    struct AdmitAll;

    impl NearestCandidateAdmission for AdmitAll {
        type Error = std::convert::Infallible;

        fn admit(&mut self, _candidate: NearestCandidate<'_>) -> Result<bool, Self::Error> {
            Ok(true)
        }
    }

    match nearest_query_snapshot_with_admission(definition, snapshot, request, &mut AdmitAll) {
        Ok(result) => Ok(result),
        Err(NearestQueryAdmissionError::Query(error)) => Err(error),
        Err(NearestQueryAdmissionError::Admission(never)) => match never {},
    }
}

/// Executes exact nearest search with mandatory principal-policy admission.
///
/// Ordinary predicates and `admission` both run before vector validation,
/// distance computation, and ranking. A denied row therefore influences no
/// score, ordering, or result count. Every merged row still consumes scan
/// budget whether predicates or policy exclude it.
pub fn nearest_query_snapshot_with_admission<A: NearestCandidateAdmission>(
    definition: &RegisteredDefinition,
    snapshot: &ColumnarSnapshot,
    request: &NearestQueryRequest,
    admission: &mut A,
) -> Result<NearestQueryResult, NearestQueryAdmissionError<A::Error>> {
    // Validate org scope type.
    if !org_value_matches_type(&request.org_scope, definition.org_scope_type()) {
        return Err(QueryError::OrgScopeTypeMismatch {
            expected: definition.org_scope_type().tag(),
        }
        .into());
    }
    let org = OrgKey::from_value(&request.org_scope).map_err(|_| QueryError::InvalidOrgScope)?;

    // Find the vector field index in the projected fields, and validate the
    // query vector against the declared dimension before any scan work.
    let vector_field_index = projected_field_index(definition, request.vector_field)?;
    let declared_dimension = definition.projected_types()[vector_field_index]
        .vector_dimension()
        .map(riffdb_types::VectorDimension::get)
        .ok_or(QueryError::NotAVectorField {
            field_id: request.vector_field,
        })?;
    if request.query_vector.dimension() != declared_dimension {
        return Err(QueryError::VectorDimensionMismatch {
            expected: declared_dimension,
            actual: request.query_vector.dimension(),
        }
        .into());
    }

    let merged = snapshot.merged_org(&org);
    let mut scanned = 0usize;

    // Collect only candidates admitted by both scalar predicates and the
    // domain-specific principal-policy gate. Admission happens before the
    // vector cell is read or validated, so a denied row cannot affect exact
    // scoring even when its stored vector is malformed.
    let mut candidate_rows: Vec<(PrimaryKeyBytes, Vec<CanonicalValue>, MergedRow)> = Vec::new();
    let mut candidate_vectors: Vec<riffdb_types::CanonicalVector> = Vec::new();

    for (key, row) in merged {
        scanned = scanned.saturating_add(1);
        if scanned > request.budget.max_scanned_rows {
            return Err(QueryError::ScanBudgetExceeded {
                max: request.budget.max_scanned_rows,
            }
            .into());
        }
        if !predicates_match(definition, &row, &request.predicates)? {
            continue;
        }

        let pk_values = decode_primary_key(definition, &key)?;
        let candidate = NearestCandidate {
            entity_type_id: definition.entity_type_id(),
            entity_version: row.entity_version,
            primary_key_fields: definition.primary_key_fields(),
            primary_key: &pk_values,
            projected_fields: definition.projected_fields(),
            cells: &row.cells,
        };
        if !admission
            .admit(candidate)
            .map_err(NearestQueryAdmissionError::Admission)?
        {
            continue;
        }

        // Only admitted candidates reach vector extraction and integrity
        // validation. Null and non-vector values retain compatibility by not
        // entering the exact candidate set.
        let cell = &row.cells[vector_field_index];
        if let CanonicalValue::Vector(vec_val) = cell {
            if vec_val.dimension() != declared_dimension {
                return Err(QueryError::VectorDimensionMismatch {
                    expected: declared_dimension,
                    actual: vec_val.dimension(),
                }
                .into());
            }
            candidate_vectors.push(vec_val.clone());
            candidate_rows.push((key, pk_values, row));
        }
    }

    // Run exact KNN. Every candidate and the query vector were validated
    // against the declared dimension above, so a mismatch here is unreachable
    // in practice; it still maps to the typed error, never a panic.
    let candidate_refs: Vec<&riffdb_types::CanonicalVector> = candidate_vectors.iter().collect();
    let scored = crate::nearest::exact_knn(
        &request.query_vector,
        &candidate_refs,
        request.metric,
        request.k,
    )
    .map_err(
        |crate::nearest::NearestError::DimensionMismatch {
             query, candidate, ..
         }| {
            QueryError::VectorDimensionMismatch {
                expected: query,
                actual: candidate,
            }
        },
    )?;

    let primary_key_fields = definition.primary_key_fields().to_vec();
    let projected_fields = definition.projected_fields().to_vec();

    let rows = scored
        .into_iter()
        .map(|scored_candidate| {
            let (_, pk_values, merged_row) = &candidate_rows[scored_candidate.index];
            NearestResultRow {
                primary_key: pk_values.clone(),
                cells: merged_row.cells.clone(),
                distance: scored_candidate.distance,
            }
        })
        .collect();

    Ok(NearestQueryResult {
        primary_key_fields,
        projected_fields,
        rows,
        scanned_rows: scanned as u64,
    })
}

/// Executes `request` against `snapshot` under `definition`.
pub(crate) fn execute_query(
    definition: &RegisteredDefinition,
    snapshot: &ColumnarSnapshot,
    request: &ColumnarQueryRequest,
    admission: Option<&AuthorizedProjectedRowAdmissionV1>,
) -> Result<QueryResult, QueryError> {
    // A wrong-typed org value can never name a real partition; fail typed
    // instead of silently returning an empty result.
    if !org_value_matches_type(&request.org_scope, definition.org_scope_type()) {
        return Err(QueryError::OrgScopeTypeMismatch {
            expected: definition.org_scope_type().tag(),
        });
    }
    let org = OrgKey::from_value(&request.org_scope).map_err(|_| QueryError::InvalidOrgScope)?;

    let select_fields = resolve_select(definition, &request.select)?;

    // Validate field references.
    for predicate in &request.predicates {
        let field = predicate_field(predicate);
        projected_field_index(definition, field)?;
    }
    for order in &request.order {
        validate_order_field(definition, order.field)?;
    }
    if let Some(group) = &request.group_by {
        for key in &group.keys {
            projected_field_index(definition, *key)?;
        }
        for agg in &group.aggregates {
            validate_aggregate_field(definition, agg)?;
        }
    }
    if let Some(agg) = &request.aggregate {
        validate_aggregate_field(definition, agg)?;
    }

    let merged = match admission {
        Some(_) => snapshot
            .merged_org_bounded(&org, MAX_PROJECTED_POLICY_CANDIDATES_V1)
            .ok_or(QueryError::ScanBudgetExceeded {
                max: MAX_PROJECTED_POLICY_CANDIDATES_V1,
            })?,
        None => snapshot.merged_org(&org),
    };
    let candidate_keys = admission
        .map(|_| {
            merged
                .keys()
                .map(|key| {
                    EntityKey::from_bytes(key.as_bytes().to_vec())
                        .map_err(|_| QueryError::PolicyAdmissionMismatch)
                })
                .collect::<Result<BTreeSet<_>, _>>()
        })
        .transpose()?;
    if let (Some(admission), Some(candidate_keys)) = (admission, candidate_keys.as_ref())
        && !admission.covers(definition.entity_type_id(), candidate_keys)
    {
        return Err(QueryError::PolicyAdmissionMismatch);
    }
    let mut scanned = 0usize;
    let mut matched: Vec<(PrimaryKeyBytes, Vec<CanonicalValue>, MergedRow)> = Vec::new();
    for (key, row) in merged {
        if let Some(admission) = admission {
            let candidate = EntityKey::from_bytes(key.as_bytes().to_vec())
                .map_err(|_| QueryError::PolicyAdmissionMismatch)?;
            if !admission.admits(&candidate) {
                continue;
            }
        }
        scanned = scanned.saturating_add(1);
        if scanned > request.budget.max_scanned_rows {
            return Err(QueryError::ScanBudgetExceeded {
                max: request.budget.max_scanned_rows,
            });
        }
        if predicates_match(definition, &row, &request.predicates)? {
            let pk_values = decode_primary_key(definition, &key)?;
            matched.push((key, pk_values, row));
        }
    }

    if let Some(group) = &request.group_by {
        return execute_group_by(definition, &matched, group, &request.budget);
    }
    if let Some(agg) = &request.aggregate {
        let value = compute_aggregate(definition, matched.iter().map(|(_, _, row)| row), agg)?;
        return Ok(QueryResult::Aggregate(value));
    }

    // Sort with primary-key tie-break (PrimaryKeyBytes total order).
    matched.sort_by(
        |(left_key, left_pk, left_row), (right_key, right_pk, right_row)| {
            for order in &request.order {
                let cmp = compare_order_field(
                    definition,
                    order.field,
                    left_pk,
                    left_row,
                    right_pk,
                    right_row,
                );
                let cmp = match order.direction {
                    SortDirection::Asc => cmp,
                    SortDirection::Desc => cmp.reverse(),
                };
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            left_key.cmp(right_key)
        },
    );

    if let Some(limit) = request.limit {
        matched.truncate(limit);
    }

    let primary_key_fields = definition.primary_key_fields().to_vec();
    let select_indexes: Vec<usize> = select_fields
        .iter()
        .map(|field| projected_field_index(definition, *field).expect("validated"))
        .collect();

    Ok(QueryResult::Rows(QueryRows {
        fields: select_fields,
        primary_key_fields,
        rows: matched
            .into_iter()
            .map(|(_, pk_values, row)| QueryRow {
                cells: project_selected_cells(&row.cells, &select_indexes),
                primary_key: pk_values,
            })
            .collect(),
    }))
}

/// Resolves the select list: empty means all projected fields (CP1 compat).
fn resolve_select(
    definition: &RegisteredDefinition,
    select: &[FieldId],
) -> Result<Vec<FieldId>, QueryError> {
    if select.is_empty() {
        return Ok(definition.projected_fields().to_vec());
    }
    let mut seen = std::collections::BTreeSet::new();
    for field in select {
        if !seen.insert(*field) {
            return Err(QueryError::DuplicateSelectField { field_id: *field });
        }
        if projected_field_index(definition, *field).is_err() {
            return Err(QueryError::UnprojectedSelectField { field_id: *field });
        }
    }
    Ok(select.to_vec())
}

/// Narrows full projected cells to the select indexes.
///
/// Extracted so falsifiability can neuter select-narrowing in one place.
#[inline]
pub(crate) fn project_selected_cells(
    full_cells: &[CanonicalValue],
    select_indexes: &[usize],
) -> Vec<CanonicalValue> {
    select_indexes
        .iter()
        .map(|idx| full_cells[*idx].clone())
        .collect()
}

fn execute_group_by(
    definition: &RegisteredDefinition,
    matched: &[(PrimaryKeyBytes, Vec<CanonicalValue>, MergedRow)],
    group: &GroupBySpec,
    budget: &QueryBudget,
) -> Result<QueryResult, QueryError> {
    let key_indexes: Vec<usize> = group
        .keys
        .iter()
        .map(|field| projected_field_index(definition, *field))
        .collect::<Result<_, _>>()?;

    let mut groups: BTreeMap<Vec<u8>, (Vec<CanonicalValue>, Vec<MergedRow>)> = BTreeMap::new();
    for (_, _, row) in matched {
        let key_cells: Vec<CanonicalValue> = key_indexes
            .iter()
            .map(|idx| row.cells[*idx].clone())
            .collect();
        let encoded = encode_group_key(&key_cells)?;
        let entry = groups
            .entry(encoded)
            .or_insert_with(|| (key_cells, Vec::new()));
        entry.1.push(row.clone());
        if groups.len() > budget.max_group_cardinality {
            return Err(QueryError::GroupCardinalityExceeded {
                max: budget.max_group_cardinality,
            });
        }
    }

    let mut out = Vec::with_capacity(groups.len());
    for (_, (key_cells, rows)) in groups {
        let mut aggregates = Vec::with_capacity(group.aggregates.len());
        for agg in &group.aggregates {
            aggregates.push(compute_aggregate(definition, rows.iter(), agg)?);
        }
        out.push((key_cells, aggregates));
    }
    Ok(QueryResult::Groups {
        key_fields: group.keys.clone(),
        groups: out,
    })
}

fn compute_aggregate<'a, I>(
    definition: &RegisteredDefinition,
    rows: I,
    agg: &AggregateOp,
) -> Result<AggregateValue, QueryError>
where
    I: Iterator<Item = &'a MergedRow>,
{
    match agg {
        AggregateOp::Count => {
            let count = rows.count() as u64;
            Ok(AggregateValue::Count(count))
        }
        AggregateOp::Sum { field } => {
            let idx = projected_field_index(definition, *field)?;
            let mut sum: i128 = 0;
            for row in rows {
                sum = sum
                    .checked_add(numeric_as_i128(&row.cells[idx])?)
                    .ok_or(QueryError::InvalidAggregate("sum overflow"))?;
            }
            Ok(AggregateValue::Sum(sum))
        }
        AggregateOp::Min { field } => {
            let idx = projected_field_index(definition, *field)?;
            let mut min: Option<CanonicalValue> = None;
            for row in rows {
                let value = &row.cells[idx];
                min = Some(match min {
                    None => value.clone(),
                    Some(current) => {
                        if compare_values(value, &current) == Ordering::Less {
                            value.clone()
                        } else {
                            current
                        }
                    }
                });
            }
            Ok(AggregateValue::Scalar(min))
        }
        AggregateOp::Max { field } => {
            let idx = projected_field_index(definition, *field)?;
            let mut max: Option<CanonicalValue> = None;
            for row in rows {
                let value = &row.cells[idx];
                max = Some(match max {
                    None => value.clone(),
                    Some(current) => {
                        if compare_values(value, &current) == Ordering::Greater {
                            value.clone()
                        } else {
                            current
                        }
                    }
                });
            }
            Ok(AggregateValue::Scalar(max))
        }
    }
}

fn predicates_match(
    definition: &RegisteredDefinition,
    row: &MergedRow,
    predicates: &[ColumnPredicate],
) -> Result<bool, QueryError> {
    for predicate in predicates {
        let field = predicate_field(predicate);
        let idx = projected_field_index(definition, field)?;
        let cell = &row.cells[idx];
        let ok = match predicate {
            ColumnPredicate::Eq { value, .. } => cell == value,
            ColumnPredicate::Range { low, high, .. } => {
                let ge_low = low
                    .as_ref()
                    .is_none_or(|bound| compare_values(cell, bound) != Ordering::Less);
                let lt_high = high
                    .as_ref()
                    .is_none_or(|bound| compare_values(cell, bound) == Ordering::Less);
                ge_low && lt_high
            }
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

fn predicate_field(predicate: &ColumnPredicate) -> FieldId {
    match predicate {
        ColumnPredicate::Eq { field, .. } | ColumnPredicate::Range { field, .. } => *field,
    }
}

fn projected_field_index(
    definition: &RegisteredDefinition,
    field: FieldId,
) -> Result<usize, QueryError> {
    definition
        .projected_fields()
        .iter()
        .position(|id| *id == field)
        .ok_or(QueryError::UnknownField { field_id: field })
}

fn primary_key_field_index(
    definition: &RegisteredDefinition,
    field: FieldId,
) -> Result<usize, QueryError> {
    definition
        .primary_key_fields()
        .iter()
        .position(|id| *id == field)
        .ok_or(QueryError::UnknownField { field_id: field })
}

/// Whether a key-component type preserves CanonicalValue order in entity-key bytes.
///
/// Fixed-width ordered components (ADR-0011) do: bool, u64, i64 (sign-bit flip),
/// timestamp, date, enum (variant id BE), uuid (network-order 16 bytes).
/// Length-prefixed string/bytes do **not**: `u32_be length || payload` sorts by
/// length first, which disagrees with lexicographic CanonicalValue order.
///
/// Enum ordering sense: a key component encodes **only** the variant id
/// (u32 BE), while [`CanonicalValue`] comparison falls back to canonical
/// bytes `(tag, type_id BE, variant_id BE)`. The two agree exactly because a
/// key column's enum type is constant across every row of the entity, so
/// both reduce to variant-id order — the sense an OrderSpec needs. This is
/// proven against the real encoder in
/// `order_proof_tests::enum_key_encoding_preserves_variant_order_at_constant_type`.
///
/// Extracted so falsifiability can neuter the order-proof validation.
#[inline]
pub(crate) fn key_type_preserves_value_order(value_type: &ValueType) -> bool {
    match value_type.tag() {
        ValueTypeTag::Bool
        | ValueTypeTag::U64
        | ValueTypeTag::I64
        | ValueTypeTag::Timestamp
        | ValueTypeTag::Date
        | ValueTypeTag::Enum
        | ValueTypeTag::Uuid => true,
        ValueTypeTag::String
        | ValueTypeTag::Bytes
        | ValueTypeTag::Decimal
        | ValueTypeTag::Money
        | ValueTypeTag::Optional
        | ValueTypeTag::List
        | ValueTypeTag::Record
        | ValueTypeTag::Vector => false,
    }
}

fn validate_order_field(
    definition: &RegisteredDefinition,
    field: FieldId,
) -> Result<(), QueryError> {
    if projected_field_index(definition, field).is_ok() {
        return Ok(());
    }
    let pk_idx = primary_key_field_index(definition, field)?;
    let value_type = &definition.primary_key_types()[pk_idx];
    if !key_type_preserves_value_order(value_type) {
        return Err(QueryError::OrderNotValueOrderPreserving {
            field_id: field,
            tag: value_type.tag(),
        });
    }
    Ok(())
}

fn compare_order_field(
    definition: &RegisteredDefinition,
    field: FieldId,
    left_pk: &[CanonicalValue],
    left_row: &MergedRow,
    right_pk: &[CanonicalValue],
    right_row: &MergedRow,
) -> Ordering {
    if let Ok(idx) = projected_field_index(definition, field) {
        return compare_values(&left_row.cells[idx], &right_row.cells[idx]);
    }
    let idx = primary_key_field_index(definition, field).expect("validated");
    compare_values(&left_pk[idx], &right_pk[idx])
}

/// Decodes entity-key envelope bytes into primary-key field values.
///
/// Extracted so falsifiability can corrupt field order in one place.
pub(crate) fn decode_primary_key(
    definition: &RegisteredDefinition,
    key: &PrimaryKeyBytes,
) -> Result<Vec<CanonicalValue>, QueryError> {
    let entity_key =
        EntityKey::from_bytes(key.as_bytes().to_vec()).map_err(|_| QueryError::PrimaryKeyDecode)?;
    let values = definition
        .primary_key_schema()
        .decode_entity(&entity_key)
        .map_err(|_| QueryError::PrimaryKeyDecode)?;
    if values.len() != definition.primary_key_fields().len() {
        return Err(QueryError::PrimaryKeyDecode);
    }
    Ok(values)
}

fn validate_aggregate_field(
    definition: &RegisteredDefinition,
    agg: &AggregateOp,
) -> Result<(), QueryError> {
    match agg {
        AggregateOp::Count => Ok(()),
        AggregateOp::Sum { field } | AggregateOp::Min { field } | AggregateOp::Max { field } => {
            projected_field_index(definition, *field).map(|_| ())
        }
    }
}

/// Whether a query-supplied org value conforms to the registered org type.
fn org_value_matches_type(value: &CanonicalValue, expected: &ValueType) -> bool {
    match expected.tag() {
        ValueTypeTag::Bool => matches!(value, CanonicalValue::Bool(_)),
        ValueTypeTag::I64 => matches!(value, CanonicalValue::I64(_)),
        ValueTypeTag::U64 => matches!(value, CanonicalValue::U64(_)),
        ValueTypeTag::Decimal => matches!(value, CanonicalValue::Decimal(_)),
        ValueTypeTag::Money => matches!(value, CanonicalValue::Money(_)),
        ValueTypeTag::String => matches!(value, CanonicalValue::String(_)),
        ValueTypeTag::Bytes => matches!(value, CanonicalValue::Bytes(_)),
        ValueTypeTag::Timestamp => matches!(value, CanonicalValue::Timestamp(_)),
        ValueTypeTag::Date => matches!(value, CanonicalValue::Date(_)),
        ValueTypeTag::Uuid => matches!(value, CanonicalValue::Uuid(_)),
        ValueTypeTag::Enum => match value {
            CanonicalValue::Enum { type_id, .. } => expected.enum_type_id() == Some(*type_id),
            _ => false,
        },
        // Optional org scopes are rejected at registration; List/Record are
        // never supported column types. Nothing conforms to them here.
        ValueTypeTag::Optional | ValueTypeTag::List | ValueTypeTag::Record => false,
        // Vectors are stored as entity field values, not as org-scope keys;
        // registration rejects a vector org scope, so nothing conforms here.
        ValueTypeTag::Vector => false,
    }
}

fn numeric_as_i128(value: &CanonicalValue) -> Result<i128, QueryError> {
    match value {
        CanonicalValue::I64(v) => Ok(i128::from(*v)),
        CanonicalValue::U64(v) => Ok(i128::from(*v)),
        _ => Err(QueryError::InvalidAggregate("sum requires i64/u64 column")),
    }
}

fn compare_values(left: &CanonicalValue, right: &CanonicalValue) -> Ordering {
    // Prefer typed comparison for common scalars; fall back to canonical bytes.
    match (left, right) {
        (CanonicalValue::Bool(a), CanonicalValue::Bool(b)) => a.cmp(b),
        (CanonicalValue::I64(a), CanonicalValue::I64(b)) => a.cmp(b),
        (CanonicalValue::U64(a), CanonicalValue::U64(b)) => a.cmp(b),
        (CanonicalValue::Uuid(a), CanonicalValue::Uuid(b)) => a.cmp(b),
        (CanonicalValue::String(a), CanonicalValue::String(b)) => a.as_str().cmp(b.as_str()),
        (CanonicalValue::Bytes(a), CanonicalValue::Bytes(b)) => a.as_bytes().cmp(b.as_bytes()),
        (CanonicalValue::Timestamp(a), CanonicalValue::Timestamp(b)) => a.cmp(b),
        (CanonicalValue::Date(a), CanonicalValue::Date(b)) => a.cmp(b),
        (CanonicalValue::Null, CanonicalValue::Null) => Ordering::Equal,
        (CanonicalValue::Null, _) => Ordering::Less,
        (_, CanonicalValue::Null) => Ordering::Greater,
        _ => {
            let left_bytes = encode_canonical_value(left).unwrap_or_default();
            let right_bytes = encode_canonical_value(right).unwrap_or_default();
            left_bytes.cmp(&right_bytes)
        }
    }
}

fn encode_group_key(cells: &[CanonicalValue]) -> Result<Vec<u8>, QueryError> {
    let mut out = Vec::new();
    for cell in cells {
        let encoded =
            encode_canonical_value(cell).map_err(|_| QueryError::InvalidAggregate("group key"))?;
        out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        out.extend_from_slice(&encoded);
    }
    Ok(out)
}

#[cfg(test)]
mod order_proof_tests {
    use super::{compare_values, key_type_preserves_value_order};
    use riffdb_contract_ir::ValueType;
    use riffdb_types::{
        CanonicalValue, Date, EntityKeyBuilder, EntityTypeId, EnumTypeId, EnumVariantId, Timestamp,
    };
    use std::cmp::Ordering;

    /// Encodes one component into an entity key and returns the key bytes.
    ///
    /// The envelope prefix is constant across calls, so lexicographic
    /// comparison of the returned bytes is comparison of the component
    /// encoding alone.
    fn encoded_component(push: impl FnOnce(&mut EntityKeyBuilder)) -> Vec<u8> {
        let mut builder = EntityKeyBuilder::new(EntityTypeId::first());
        push(&mut builder);
        builder.as_bytes().to_vec()
    }

    /// Core A3 proof obligation: for every ordered pair of values, the
    /// lexicographic order of the REAL `EntityKeyBuilder` encoding equals the
    /// [`CanonicalValue`] order used by `compare_order_field`.
    fn assert_key_bytes_track_value_order(encoded: &[(Vec<u8>, CanonicalValue)], context: &str) {
        for (i, (left_bytes, left_value)) in encoded.iter().enumerate() {
            for (j, (right_bytes, right_value)) in encoded.iter().enumerate() {
                assert_eq!(
                    left_bytes.cmp(right_bytes),
                    compare_values(left_value, right_value),
                    "{context}: key byte order diverges from value order at pair ({i}, {j})"
                );
                // The list is given in ascending value order; pin that too so
                // a broken compare_values cannot vacuously agree with broken
                // byte order.
                assert_eq!(
                    compare_values(left_value, right_value),
                    i.cmp(&j),
                    "{context}: value order diverges from the declared ascending order at ({i}, {j})"
                );
            }
        }
    }

    #[test]
    fn bool_key_encoding_preserves_value_order() {
        assert!(
            key_type_preserves_value_order(&ValueType::bool()),
            "classifier must admit bool so the encoder proof below is the load-bearing check"
        );
        let encoded: Vec<(Vec<u8>, CanonicalValue)> = [false, true]
            .into_iter()
            .map(|value| {
                (
                    encoded_component(|builder| {
                        builder.push_bool(value).expect("push");
                    }),
                    CanonicalValue::Bool(value),
                )
            })
            .collect();
        assert_key_bytes_track_value_order(&encoded, "bool");
    }

    #[test]
    fn u64_key_encoding_preserves_value_order() {
        assert!(key_type_preserves_value_order(&ValueType::u64()));
        let encoded: Vec<(Vec<u8>, CanonicalValue)> = [0u64, 1, 7, 9, 256, u64::MAX]
            .into_iter()
            .map(|value| {
                (
                    encoded_component(|builder| {
                        builder.push_u64(value).expect("push");
                    }),
                    CanonicalValue::U64(value),
                )
            })
            .collect();
        assert_key_bytes_track_value_order(&encoded, "u64");
    }

    #[test]
    fn i64_key_encoding_preserves_value_order_across_sign() {
        assert!(key_type_preserves_value_order(&ValueType::i64()));
        let encoded: Vec<(Vec<u8>, CanonicalValue)> = [i64::MIN, -3, -1, 0, 1, i64::MAX]
            .into_iter()
            .map(|value| {
                (
                    encoded_component(|builder| {
                        builder.push_i64(value).expect("push");
                    }),
                    CanonicalValue::I64(value),
                )
            })
            .collect();
        assert_key_bytes_track_value_order(&encoded, "i64");
    }

    /// Timestamp keys sign-flip the seconds and append nanos BE; the proof
    /// includes negative seconds and nano tie-breaks around the epoch, where
    /// a naive two's-complement encoding would invert the order.
    #[test]
    fn timestamp_key_encoding_preserves_value_order_including_negative_seconds() {
        assert!(key_type_preserves_value_order(&ValueType::timestamp()));
        let ascending = [
            (-5i64, 0u32),
            (-5, 999_999_999),
            (-4, 0),
            (-1, 999_999_999),
            (0, 0),
            (0, 1),
            (3, 500),
            (i64::MAX, 999_999_999),
        ];
        let encoded: Vec<(Vec<u8>, CanonicalValue)> = ascending
            .into_iter()
            .map(|(seconds, nanoseconds)| {
                let timestamp = Timestamp::new(seconds, nanoseconds).expect("canonical nanos");
                (
                    encoded_component(|builder| {
                        builder.push_timestamp(timestamp).expect("push");
                    }),
                    CanonicalValue::Timestamp(timestamp),
                )
            })
            .collect();
        assert_key_bytes_track_value_order(&encoded, "timestamp");
    }

    #[test]
    fn date_key_encoding_preserves_value_order_including_pre_epoch() {
        assert!(key_type_preserves_value_order(&ValueType::date()));
        let encoded: Vec<(Vec<u8>, CanonicalValue)> = [i32::MIN, -400, -1, 0, 1, 400, i32::MAX]
            .into_iter()
            .map(|days| {
                let date = Date::from_days_since_unix_epoch(days);
                (
                    encoded_component(|builder| {
                        builder.push_date(date).expect("push");
                    }),
                    CanonicalValue::Date(date),
                )
            })
            .collect();
        assert_key_bytes_track_value_order(&encoded, "date");
    }

    /// Enum keys encode only the variant id (u32 BE); CanonicalValue
    /// comparison falls back to canonical bytes `(tag, type_id, variant_id)`.
    /// With the type id held constant — always true for a key column — both
    /// reduce to variant-id order, which is the sense OrderSpec relies on.
    #[test]
    fn enum_key_encoding_preserves_variant_order_at_constant_type() {
        let type_id = EnumTypeId::new(7).expect("nonzero");
        assert!(key_type_preserves_value_order(&ValueType::enumeration(
            type_id
        )));
        let encoded: Vec<(Vec<u8>, CanonicalValue)> = [1u32, 2, 300, 70_000]
            .into_iter()
            .map(|raw| {
                let variant_id = EnumVariantId::new(raw).expect("nonzero");
                (
                    encoded_component(|builder| {
                        builder.push_enum_variant(variant_id).expect("push");
                    }),
                    CanonicalValue::Enum {
                        type_id,
                        variant_id,
                    },
                )
            })
            .collect();
        assert_key_bytes_track_value_order(&encoded, "enum");
    }

    #[test]
    fn uuid_key_encoding_preserves_value_order() {
        fn uuid_with(first: u8, last: u8) -> [u8; 16] {
            let mut bytes = [0u8; 16];
            bytes[0] = first;
            bytes[15] = last;
            bytes
        }
        assert!(key_type_preserves_value_order(&ValueType::uuid()));
        // Ascending network-order byte patterns exercising both ends of the
        // 16-byte width (first byte dominates; last byte tie-breaks).
        let ascending = [
            uuid_with(0x00, 0x00),
            uuid_with(0x00, 0x01),
            uuid_with(0x01, 0x00),
            uuid_with(0x01, 0x02),
            [0xff; 16],
        ];
        let encoded: Vec<(Vec<u8>, CanonicalValue)> = ascending
            .into_iter()
            .map(|value| {
                (
                    encoded_component(|builder| {
                        builder.push_uuid(&value).expect("push");
                    }),
                    CanonicalValue::Uuid(value),
                )
            })
            .collect();
        assert_key_bytes_track_value_order(&encoded, "uuid");
    }

    /// Length-prefixed string counterexample: "b" < "aa" in key bytes (length
    /// first) but "aa" < "b" lexicographically — the divergence class that
    /// forces the OrderSpec rejection. Bytes share the same encoding shape.
    #[test]
    fn string_key_encoding_is_not_value_order_preserving() {
        assert!(!key_type_preserves_value_order(
            &ValueType::string(32).expect("bound")
        ));
        assert!(!key_type_preserves_value_order(
            &ValueType::bytes(32).expect("bound")
        ));
        let short = encoded_component(|builder| {
            builder.push_str("b").expect("push");
        });
        let long = encoded_component(|builder| {
            builder.push_str("aa").expect("push");
        });
        assert_eq!(
            short.cmp(&long),
            Ordering::Less,
            "length prefix sorts first"
        );
        assert_eq!(
            compare_values(
                &CanonicalValue::string("b").expect("value"),
                &CanonicalValue::string("aa").expect("value"),
            ),
            Ordering::Greater,
            "lexicographic value order disagrees with the key byte order above"
        );
    }
}
