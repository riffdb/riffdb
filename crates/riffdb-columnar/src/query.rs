//! Org-scoped query executor over a published snapshot (D6).

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

use riffdb_types::{CanonicalValue, FieldId, encode_canonical_value};

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
    /// Projected field to sort by.
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

/// Row-shaped query result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRows {
    /// Projected field order for each row's cells.
    pub fields: Vec<FieldId>,
    /// Matching rows (cells aligned with `fields`).
    pub rows: Vec<Vec<CanonicalValue>>,
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
    /// Predicate or order references a field not in the projection.
    UnknownField {
        /// Field id.
        field_id: FieldId,
    },
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
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownField { field_id } => {
                write!(f, "unknown projected field {}", field_id.get())
            }
            Self::ScanBudgetExceeded { max } => write!(f, "scan budget exceeded (max {max})"),
            Self::GroupCardinalityExceeded { max } => {
                write!(f, "group cardinality exceeded (max {max})")
            }
            Self::InvalidAggregate(message) => write!(f, "invalid aggregate: {message}"),
            Self::InvalidOrgScope => f.write_str("invalid org scope value"),
        }
    }
}

impl std::error::Error for QueryError {}

/// Executes `request` against `snapshot` under `definition`.
pub(crate) fn execute_query(
    definition: &RegisteredDefinition,
    snapshot: &ColumnarSnapshot,
    request: &ColumnarQueryRequest,
) -> Result<QueryResult, QueryError> {
    let org = OrgKey::from_value(&request.org_scope).map_err(|_| QueryError::InvalidOrgScope)?;
    // Validate field references.
    for predicate in &request.predicates {
        let field = predicate_field(predicate);
        field_index(definition, field)?;
    }
    for order in &request.order {
        field_index(definition, order.field)?;
    }
    if let Some(group) = &request.group_by {
        for key in &group.keys {
            field_index(definition, *key)?;
        }
        for agg in &group.aggregates {
            validate_aggregate_field(definition, agg)?;
        }
    }
    if let Some(agg) = &request.aggregate {
        validate_aggregate_field(definition, agg)?;
    }

    let merged = snapshot.merged_org(&org);
    let mut scanned = 0usize;
    let mut matched: Vec<(PrimaryKeyBytes, MergedRow)> = Vec::new();
    for (key, row) in merged {
        scanned = scanned.saturating_add(1);
        if scanned > request.budget.max_scanned_rows {
            return Err(QueryError::ScanBudgetExceeded {
                max: request.budget.max_scanned_rows,
            });
        }
        if predicates_match(definition, &row, &request.predicates)? {
            matched.push((key, row));
        }
    }

    if let Some(group) = &request.group_by {
        return execute_group_by(definition, &matched, group, &request.budget);
    }
    if let Some(agg) = &request.aggregate {
        let value = compute_aggregate(definition, matched.iter().map(|(_, row)| row), agg)?;
        return Ok(QueryResult::Aggregate(value));
    }

    // Sort with primary-key tie-break.
    matched.sort_by(|(left_key, left_row), (right_key, right_row)| {
        for order in &request.order {
            let idx = field_index(definition, order.field).expect("validated");
            let cmp = compare_values(&left_row.cells[idx], &right_row.cells[idx]);
            let cmp = match order.direction {
                SortDirection::Asc => cmp,
                SortDirection::Desc => cmp.reverse(),
            };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        left_key.cmp(right_key)
    });

    if let Some(limit) = request.limit {
        matched.truncate(limit);
    }

    Ok(QueryResult::Rows(QueryRows {
        fields: definition.projected_fields().to_vec(),
        rows: matched.into_iter().map(|(_, row)| row.cells).collect(),
    }))
}

fn execute_group_by(
    definition: &RegisteredDefinition,
    matched: &[(PrimaryKeyBytes, MergedRow)],
    group: &GroupBySpec,
    budget: &QueryBudget,
) -> Result<QueryResult, QueryError> {
    let key_indexes: Vec<usize> = group
        .keys
        .iter()
        .map(|field| field_index(definition, *field))
        .collect::<Result<_, _>>()?;

    let mut groups: BTreeMap<Vec<u8>, (Vec<CanonicalValue>, Vec<MergedRow>)> = BTreeMap::new();
    for (_, row) in matched {
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
            let idx = field_index(definition, *field)?;
            let mut sum: i128 = 0;
            for row in rows {
                sum = sum
                    .checked_add(numeric_as_i128(&row.cells[idx])?)
                    .ok_or(QueryError::InvalidAggregate("sum overflow"))?;
            }
            Ok(AggregateValue::Sum(sum))
        }
        AggregateOp::Min { field } => {
            let idx = field_index(definition, *field)?;
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
            let idx = field_index(definition, *field)?;
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
        let idx = field_index(definition, field)?;
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

fn field_index(definition: &RegisteredDefinition, field: FieldId) -> Result<usize, QueryError> {
    definition
        .projected_fields()
        .iter()
        .position(|id| *id == field)
        .ok_or(QueryError::UnknownField { field_id: field })
}

fn validate_aggregate_field(
    definition: &RegisteredDefinition,
    agg: &AggregateOp,
) -> Result<(), QueryError> {
    match agg {
        AggregateOp::Count => Ok(()),
        AggregateOp::Sum { field } | AggregateOp::Min { field } | AggregateOp::Max { field } => {
            field_index(definition, *field).map(|_| ())
        }
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
