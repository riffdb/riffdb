#![forbid(unsafe_code)]

//! Closed-program execution over one engine-owned authoritative read view.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use riffdb_query_ir::{
    QueryAccessProgramV1, QueryAccessStep, QueryLiteral, QueryPredicateOperator,
    QueryPredicateValue,
};
use riffdb_riffql_syntax::Cardinality;
use riffdb_types::CanonicalValue;

/// Maximum checked submitted parameters.
pub const MAX_QUERY_PARAMETERS: usize = 1_024;
/// Maximum fields copied into one owned row.
pub const MAX_QUERY_ROW_FIELDS: usize = 1_024;
/// Maximum physical candidates inspected by one access step.
pub const MAX_QUERY_SCANNED_ROWS: u64 = 500;
/// Maximum opaque continuation bytes supplied by one engine adapter.
pub const MAX_QUERY_CONTINUATION_BYTES: usize = 4_096;

/// Checked name-addressed canonical parameters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryParameters(BTreeMap<String, CanonicalValue>);

impl QueryParameters {
    /// Validates bounded unique parameter names.
    pub fn checked(values: BTreeMap<String, CanonicalValue>) -> Option<Self> {
        (values.len() <= MAX_QUERY_PARAMETERS && values.keys().all(|name| !name.is_empty()))
            .then_some(Self(values))
    }

    /// Resolves one exact parameter.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&CanonicalValue> {
        self.0.get(name)
    }
}

/// One owned, name-addressed authoritative entity row.
#[derive(Clone, Eq, PartialEq)]
pub struct QueryRow {
    entity: String,
    fields: BTreeMap<String, CanonicalValue>,
}

impl std::fmt::Debug for QueryRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryRow")
            .field("entity", &self.entity)
            .field("field_names", &self.fields.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl QueryRow {
    /// Constructs one bounded row with unique field names.
    pub fn checked(entity: String, fields: BTreeMap<String, CanonicalValue>) -> Option<Self> {
        (!entity.is_empty()
            && fields.len() <= MAX_QUERY_ROW_FIELDS
            && fields.keys().all(|name| !name.is_empty()))
        .then_some(Self { entity, fields })
    }

    /// Contract entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Resolves one exact field.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&CanonicalValue> {
        self.fields.get(name)
    }

    fn project(&self, selected: &[String]) -> Result<Self, QueryExecutionError> {
        let fields = selected
            .iter()
            .map(|name| {
                self.field(name)
                    .cloned()
                    .map(|value| (name.clone(), value))
                    .ok_or_else(|| QueryExecutionError::MissingField {
                        entity: self.entity.clone(),
                        field: name.clone(),
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Self::checked(self.entity.clone(), fields).ok_or(QueryExecutionError::BoundExceeded)
    }
}

/// One resolved predicate passed to the closed engine view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundPredicate {
    field: String,
    operator: QueryPredicateOperator,
    value: CanonicalValue,
}

impl BoundPredicate {
    /// Target field.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Operator.
    #[must_use]
    pub const fn operator(&self) -> QueryPredicateOperator {
        self.operator
    }

    /// Canonical comparison value.
    #[must_use]
    pub const fn value(&self) -> &CanonicalValue {
        &self.value
    }
}

/// Owned result of one bounded index access inside the current read view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryScanPage {
    rows: Vec<QueryRow>,
    epoch: u64,
    scanned_rows: u64,
    continuation: Option<Vec<u8>>,
}

impl QueryScanPage {
    /// Constructs an exact-end page.
    #[must_use]
    pub fn exact_end(rows: Vec<QueryRow>, epoch: u64) -> Self {
        Self {
            scanned_rows: rows.len() as u64,
            rows,
            epoch,
            continuation: None,
        }
    }

    /// Constructs a non-final page with explicit physical progress.
    pub fn continued(
        rows: Vec<QueryRow>,
        epoch: u64,
        scanned_rows: u64,
        continuation: Vec<u8>,
    ) -> Option<Self> {
        (!continuation.is_empty()
            && continuation.len() <= MAX_QUERY_CONTINUATION_BYTES
            && scanned_rows > 0
            && scanned_rows <= MAX_QUERY_SCANNED_ROWS)
            .then_some(Self {
                rows,
                epoch,
                scanned_rows,
                continuation: Some(continuation),
            })
    }
}

/// The only operations available while a concrete adapter owns one read transaction.
pub trait QueryReadView {
    /// Adapter-internal error retained below the safe public boundary.
    type Error;

    /// Application commit head observed by this exact snapshot.
    fn application_head(&self) -> u64;

    /// Executes one exact primary-key step.
    fn point(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
    ) -> Result<Option<QueryRow>, Self::Error>;

    /// Executes one bounded declared-index step.
    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
    ) -> Result<QueryScanPage, Self::Error>;
}

/// Engine-owned one-snapshot execution boundary.
///
/// Implementations open one read transaction, invoke the closed executor, copy
/// the owned result, and close the transaction before returning.
pub trait QueryExecutionPort {
    /// Executes one compiler-produced program against one checked parameter set.
    fn execute_query(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError>;
}

/// One returned root field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryResultValue {
    /// Exact row.
    One(QueryRow),
    /// Optional row.
    Maybe(Option<QueryRow>),
    /// Bounded ordered rows.
    Many(Vec<QueryRow>),
}

/// Complete owned snapshot result; no engine handle or iterator escapes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryOwnedSnapshot {
    application_head: u64,
    index_epochs: BTreeMap<String, u64>,
    outcome: String,
    fields: BTreeMap<String, QueryResultValue>,
    continuation: Option<Vec<u8>>,
}

impl QueryOwnedSnapshot {
    /// Exact application head observed by the engine snapshot.
    #[must_use]
    pub const fn application_head(&self) -> u64 {
        self.application_head
    }

    /// Relevant index epochs keyed by `Entity.index`.
    #[must_use]
    pub const fn index_epochs(&self) -> &BTreeMap<String, u64> {
        &self.index_epochs
    }

    /// Declared result branch.
    #[must_use]
    pub fn outcome(&self) -> &str {
        &self.outcome
    }

    /// Name-addressed result fields.
    #[must_use]
    pub const fn fields(&self) -> &BTreeMap<String, QueryResultValue> {
        &self.fields
    }

    /// Opaque lower continuation to be bound into the public cursor.
    #[must_use]
    pub fn continuation(&self) -> Option<&[u8]> {
        self.continuation.as_deref()
    }
}

/// Closed, safe executor failure classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryExecutionError {
    /// Required submitted parameter is absent.
    MissingParameter {
        /// Safe parameter name.
        parameter: String,
    },
    /// A returned row is missing a compiler-required field.
    MissingField {
        /// Safe entity name.
        entity: String,
        /// Safe field name.
        field: String,
    },
    /// Backend row names or closed-program structure disagree.
    InvalidProgram,
    /// Engine adapter failed; its source is retained by the adapter, not here.
    BackendUnavailable,
    /// A row, byte, scan, or result ceiling was exceeded.
    BoundExceeded,
    /// A `one` or `maybe` binding returned too many rows.
    UnexpectedCardinality {
        /// Safe binding name.
        binding: String,
    },
    /// A predicate uses a value/operator unavailable in v1.
    UnsupportedPredicate,
}

/// Executes the complete program through one already-open engine view.
///
/// Concrete adapters open and close the transaction around this call. The view
/// is neither returned nor retained.
pub fn execute_in_snapshot<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    view: &mut V,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    let mut bindings = BTreeMap::<String, Vec<QueryRow>>::new();
    let mut result_fields = BTreeMap::new();
    let mut index_epochs = BTreeMap::new();
    let mut continuation = None;
    let default_outcome = program
        .surface()
        .schemas()
        .results()
        .first()
        .map(|branch| branch.name().to_owned())
        .unwrap_or_else(|| "Result".to_owned());

    for step in program.steps() {
        let predicates = bind_predicates(step, parameters, &bindings)?;
        let mut rows = match step.access() {
            riffdb_query_ir::QueryAccessKind::Point { .. } => view
                .point(step, &predicates)
                .map_err(|_| QueryExecutionError::BackendUnavailable)?
                .into_iter()
                .collect::<Vec<_>>(),
            riffdb_query_ir::QueryAccessKind::Index { index, .. } => {
                let page = view
                    .scan(step, &predicates)
                    .map_err(|_| QueryExecutionError::BackendUnavailable)?;
                if page.scanned_rows > MAX_QUERY_SCANNED_ROWS
                    || page.rows.len() as u64 > step.maximum_rows()
                    || page
                        .continuation
                        .as_ref()
                        .is_some_and(|value| value.len() > MAX_QUERY_CONTINUATION_BYTES)
                {
                    return Err(QueryExecutionError::BoundExceeded);
                }
                index_epochs.insert(format!("{}.{}", step.entity(), index), page.epoch);
                if page.continuation.is_some() {
                    if continuation.is_some() {
                        return Err(QueryExecutionError::InvalidProgram);
                    }
                    continuation = page.continuation;
                }
                page.rows
            }
        };
        if rows.iter().any(|row| row.entity() != step.entity()) {
            return Err(QueryExecutionError::InvalidProgram);
        }
        rows.retain(|row| predicates_match(row, &predicates).unwrap_or(false));
        if rows.len() as u64 > step.maximum_rows() {
            return Err(QueryExecutionError::BoundExceeded);
        }
        if matches!(step.cardinality(), Cardinality::One | Cardinality::Maybe) && rows.len() > 1 {
            return Err(QueryExecutionError::UnexpectedCardinality {
                binding: step.binding().to_owned(),
            });
        }
        if step.cardinality() == Cardinality::One && rows.is_empty() {
            let outcome = step
                .absence_outcome()
                .ok_or(QueryExecutionError::InvalidProgram)?
                .to_owned();
            return Ok(QueryOwnedSnapshot {
                application_head: view.application_head(),
                index_epochs,
                outcome,
                fields: BTreeMap::new(),
                continuation: None,
            });
        }
        let projected = rows
            .iter()
            .map(|row| row.project(step.selected_fields()))
            .collect::<Result<Vec<_>, _>>()?;
        let value = match step.cardinality() {
            Cardinality::One => QueryResultValue::One(
                projected
                    .first()
                    .cloned()
                    .ok_or(QueryExecutionError::InvalidProgram)?,
            ),
            Cardinality::Maybe => QueryResultValue::Maybe(projected.first().cloned()),
            Cardinality::Many => QueryResultValue::Many(projected),
        };
        for name in step.result_names() {
            if result_fields.insert(name.clone(), value.clone()).is_some() {
                return Err(QueryExecutionError::InvalidProgram);
            }
        }
        bindings.insert(step.binding().to_owned(), rows);
    }

    Ok(QueryOwnedSnapshot {
        application_head: view.application_head(),
        index_epochs,
        outcome: default_outcome,
        fields: result_fields,
        continuation,
    })
}

fn bind_predicates(
    step: &QueryAccessStep,
    parameters: &QueryParameters,
    bindings: &BTreeMap<String, Vec<QueryRow>>,
) -> Result<Vec<BoundPredicate>, QueryExecutionError> {
    step.predicates()
        .iter()
        .map(|predicate| {
            let value = match predicate.value() {
                QueryPredicateValue::Parameter(name) => {
                    parameters.get(name).cloned().ok_or_else(|| {
                        QueryExecutionError::MissingParameter {
                            parameter: name.clone(),
                        }
                    })?
                }
                QueryPredicateValue::BindingField { binding, field } => bindings
                    .get(binding)
                    .and_then(|rows| rows.first())
                    .and_then(|row| row.field(field))
                    .cloned()
                    .unwrap_or(CanonicalValue::Null),
                QueryPredicateValue::Literal(literal) => literal_value(literal)?,
                QueryPredicateValue::EnumVariant { .. } => {
                    return Err(QueryExecutionError::UnsupportedPredicate);
                }
            };
            Ok(BoundPredicate {
                field: predicate.field().to_owned(),
                operator: predicate.operator(),
                value,
            })
        })
        .collect()
}

fn literal_value(literal: &QueryLiteral) -> Result<CanonicalValue, QueryExecutionError> {
    match literal {
        QueryLiteral::Unsigned(value) => value
            .parse::<u64>()
            .map(CanonicalValue::U64)
            .map_err(|_| QueryExecutionError::InvalidProgram),
        QueryLiteral::String(value) => {
            CanonicalValue::string(value.clone()).map_err(|_| QueryExecutionError::BoundExceeded)
        }
        QueryLiteral::Boolean(value) => Ok(CanonicalValue::Bool(*value)),
        QueryLiteral::Null => Ok(CanonicalValue::Null),
    }
}

fn predicates_match(
    row: &QueryRow,
    predicates: &[BoundPredicate],
) -> Result<bool, QueryExecutionError> {
    for predicate in predicates {
        let actual =
            row.field(&predicate.field)
                .ok_or_else(|| QueryExecutionError::MissingField {
                    entity: row.entity.clone(),
                    field: predicate.field.clone(),
                })?;
        let matches = match predicate.operator {
            QueryPredicateOperator::Equal => actual == &predicate.value,
            QueryPredicateOperator::NotEqual => actual != &predicate.value,
            QueryPredicateOperator::In => match &predicate.value {
                CanonicalValue::List(values) => values.values().iter().any(|value| value == actual),
                _ => return Err(QueryExecutionError::InvalidProgram),
            },
            operator => {
                let ordering = scalar_order(actual, &predicate.value)
                    .ok_or(QueryExecutionError::UnsupportedPredicate)?;
                match operator {
                    QueryPredicateOperator::Less => ordering == Ordering::Less,
                    QueryPredicateOperator::LessEqual => ordering != Ordering::Greater,
                    QueryPredicateOperator::Greater => ordering == Ordering::Greater,
                    QueryPredicateOperator::GreaterEqual => ordering != Ordering::Less,
                    QueryPredicateOperator::Equal
                    | QueryPredicateOperator::NotEqual
                    | QueryPredicateOperator::In => unreachable!(),
                }
            }
        };
        if !matches {
            return Ok(false);
        }
    }
    Ok(true)
}

fn scalar_order(left: &CanonicalValue, right: &CanonicalValue) -> Option<Ordering> {
    match (left, right) {
        (CanonicalValue::I64(left), CanonicalValue::I64(right)) => left.partial_cmp(right),
        (CanonicalValue::U64(left), CanonicalValue::U64(right)) => left.partial_cmp(right),
        (CanonicalValue::String(left), CanonicalValue::String(right)) => {
            left.as_str().partial_cmp(right.as_str())
        }
        (CanonicalValue::Timestamp(left), CanonicalValue::Timestamp(right)) => {
            left.partial_cmp(right)
        }
        (CanonicalValue::Date(left), CanonicalValue::Date(right)) => left.partial_cmp(right),
        (CanonicalValue::Uuid(left), CanonicalValue::Uuid(right)) => left.partial_cmp(right),
        (
            CanonicalValue::Enum {
                type_id: left_type,
                variant_id: left_variant,
            },
            CanonicalValue::Enum {
                type_id: right_type,
                variant_id: right_variant,
            },
        ) if left_type == right_type => left_variant.partial_cmp(right_variant),
        _ => None,
    }
}

#[allow(dead_code)]
fn _bounded_set_guard(_: BTreeSet<String>) {}
