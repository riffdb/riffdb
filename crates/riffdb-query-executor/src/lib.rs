#![forbid(unsafe_code)]

//! Closed-program execution over one engine-owned authoritative read view.

use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use riffdb_query_ir::{
    QueryAccessProgramV1, QueryAccessStep, QueryLiteral, QueryPredicateOperator,
    QueryPredicateValue, QueryRowLimit,
};
use riffdb_riffql_syntax::Cardinality;
use riffdb_types::{
    CanonicalValue, QueryCostVectorV1, canonical_value_encoded_len, encode_canonical_value,
};

/// Maximum checked submitted parameters.
pub const MAX_QUERY_PARAMETERS: usize = 1_024;
/// Maximum fields copied into one owned row.
pub const MAX_QUERY_ROW_FIELDS: usize = 1_024;
/// Scan-bound constants and helpers shared with deploy-time resolution.
///
/// `MAX_QUERY_SCANNED_ROWS` is the physical scan ceiling. Adapters may inspect
/// one extra row (`QUERY_CONTINUATION_PROBE_ROWS`) when minting a continuation;
/// legal page takes are therefore bounded by `max_query_page_take()`.
pub use riffdb_query_ir::{
    MAX_QUERY_SCANNED_ROWS, QUERY_CONTINUATION_PROBE_ROWS, max_query_page_take,
    page_take_within_scan_bound, scanned_rows_budget_for_page_take,
};
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

    /// Iterates parameters in canonical name order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &CanonicalValue)> {
        self.0.iter().map(|(name, value)| (name.as_str(), value))
    }
}

/// One owned, name-addressed authoritative entity row.
///
/// Field **names** and the entity name are shared `Arc<str>` handles (per-query
/// constants). Field **values** are owned exactly once at materialization and
/// then moved through projection, service assembly, and transport conversion.
#[derive(Clone, Eq, PartialEq)]
pub struct QueryRow {
    entity: Arc<str>,
    fields: BTreeMap<Arc<str>, CanonicalValue>,
}

impl std::fmt::Debug for QueryRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryRow")
            .field("entity", &self.entity.as_ref())
            .field(
                "field_names",
                &self
                    .fields
                    .keys()
                    .map(|name| name.as_ref())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl QueryRow {
    /// Constructs one bounded row with unique field names.
    ///
    /// Accepts owned strings for harness and adapter convenience; hot paths
    /// should prefer [`Self::from_shared`] with interned `Arc<str>` names.
    pub fn checked(entity: String, fields: BTreeMap<String, CanonicalValue>) -> Option<Self> {
        let fields = fields
            .into_iter()
            .map(|(name, value)| (Arc::<str>::from(name), value))
            .collect::<BTreeMap<_, _>>();
        Self::from_shared(Arc::<str>::from(entity), fields)
    }

    /// Constructs one bounded row from already-shared name handles.
    pub fn from_shared(
        entity: Arc<str>,
        fields: BTreeMap<Arc<str>, CanonicalValue>,
    ) -> Option<Self> {
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

    /// Shared entity name handle.
    #[must_use]
    pub fn entity_arc(&self) -> &Arc<str> {
        &self.entity
    }

    /// Resolves one exact field.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&CanonicalValue> {
        self.fields.get(name)
    }

    /// Iterates returned fields in canonical name order.
    pub fn fields(&self) -> impl ExactSizeIterator<Item = (&str, &CanonicalValue)> {
        self.fields
            .iter()
            .map(|(name, value)| (name.as_ref(), value))
    }

    /// Shared field map (names are interned handles).
    #[must_use]
    pub const fn field_map(&self) -> &BTreeMap<Arc<str>, CanonicalValue> {
        &self.fields
    }

    /// Consumes the row into entity name and fields.
    #[must_use]
    pub fn into_parts(self) -> (Arc<str>, BTreeMap<Arc<str>, CanonicalValue>) {
        (self.entity, self.fields)
    }

    /// Moves selected fields into a projected row (no value clones).
    fn into_project(mut self, selected: &[Arc<str>]) -> Result<Self, QueryExecutionError> {
        let mut fields = BTreeMap::new();
        for name in selected {
            let value = self.fields.remove(name.as_ref()).ok_or_else(|| {
                QueryExecutionError::MissingField {
                    entity: self.entity.to_string(),
                    field: name.to_string(),
                }
            })?;
            fields.insert(Arc::clone(name), value);
        }
        Self::from_shared(self.entity, fields).ok_or(QueryExecutionError::BoundExceeded)
    }

    /// Clones selected fields into a projected row (used only when the full row
    /// must also be retained for a later dependent step).
    fn project_clone(&self, selected: &[Arc<str>]) -> Result<Self, QueryExecutionError> {
        let mut fields = BTreeMap::new();
        for name in selected {
            let value =
                self.field(name.as_ref())
                    .ok_or_else(|| QueryExecutionError::MissingField {
                        entity: self.entity.to_string(),
                        field: name.to_string(),
                    })?;
            note_pipeline_value_clone();
            fields.insert(Arc::clone(name), value.clone());
        }
        Self::from_shared(Arc::clone(&self.entity), fields)
            .ok_or(QueryExecutionError::BoundExceeded)
    }
}

// Thread-local clone probe for the result-row pipeline after storage
// materialization. Thread-local (not process-global) so parallel test threads
// cannot race — the redb table-open lesson applied to a multi-threaded suite.
thread_local! {
    static PIPELINE_CLONE_COUNTING: Cell<bool> = const { Cell::new(false) };
    static PIPELINE_VALUE_CLONES: Cell<u64> = const { Cell::new(0) };
}

fn note_pipeline_value_clone() {
    PIPELINE_CLONE_COUNTING.with(|enabled| {
        if enabled.get() {
            PIPELINE_VALUE_CLONES.with(|count| count.set(count.get().saturating_add(1)));
        }
    });
}

/// Enables clone counting on the current thread after reset.
pub fn enable_pipeline_clone_counting() {
    PIPELINE_VALUE_CLONES.with(|count| count.set(0));
    PIPELINE_CLONE_COUNTING.with(|enabled| enabled.set(true));
}

/// Disables counting on the current thread and returns the observed total.
pub fn disable_pipeline_clone_counting() -> u64 {
    PIPELINE_CLONE_COUNTING.with(|enabled| enabled.set(false));
    PIPELINE_VALUE_CLONES.with(|count| count.replace(0))
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
    point_reads: u64,
    continuation: Option<Vec<u8>>,
}

impl QueryScanPage {
    /// Constructs an exact-end page.
    #[must_use]
    pub fn exact_end(rows: Vec<QueryRow>, epoch: u64) -> Self {
        let work = rows.len() as u64;
        Self {
            scanned_rows: work,
            point_reads: work,
            rows,
            epoch,
            continuation: None,
        }
    }

    /// Constructs a non-final page with explicit physical progress.
    ///
    /// Construction does not enforce `MAX_QUERY_SCANNED_ROWS`; the closed
    /// executor classifies an over-scan as [`QueryExecutionError::BoundExceeded`]
    /// so adapters never map a knowable page bound into an integrity fault.
    pub fn continued(
        rows: Vec<QueryRow>,
        epoch: u64,
        scanned_rows: u64,
        continuation: Vec<u8>,
    ) -> Option<Self> {
        let point_reads = rows.len() as u64;
        (!continuation.is_empty()
            && continuation.len() <= MAX_QUERY_CONTINUATION_BYTES
            && scanned_rows > 0)
            .then_some(Self {
                rows,
                epoch,
                scanned_rows,
                point_reads,
                continuation: Some(continuation),
            })
    }

    /// Constructs one backend-reported page for conformance and fault tests.
    ///
    /// The executor independently reconciles these counts with the returned
    /// rows and the plan fuel; this constructor intentionally performs only
    /// structural bounds checks (not the scan ceiling).
    #[doc(hidden)]
    pub fn reported(
        rows: Vec<QueryRow>,
        epoch: u64,
        scanned_rows: u64,
        point_reads: u64,
        continuation: Option<Vec<u8>>,
    ) -> Option<Self> {
        (continuation
            .as_ref()
            .is_none_or(|value| !value.is_empty() && value.len() <= MAX_QUERY_CONTINUATION_BYTES)
            && (continuation.is_none() || scanned_rows > 0))
            .then_some(Self {
                rows,
                epoch,
                scanned_rows,
                point_reads,
                continuation,
            })
    }
}

/// Move-only decrementing work allowance for one exact compiled query.
///
/// Fuel is created from the plan-hashed cost vector inside the closed executor
/// and is never serialized into a cursor or returned with a result.
pub struct QueryExecutionFuel {
    access_steps: u64,
    scanned_index_rows: u64,
    point_reads: u64,
    dependent_keys: u64,
    intermediate_rows: u64,
    projected_values: u64,
    encoded_result_bytes: u64,
}

impl std::fmt::Debug for QueryExecutionFuel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("QueryExecutionFuel([REDACTED])")
    }
}

impl QueryExecutionFuel {
    fn from_cost(cost: QueryCostVectorV1) -> Self {
        // Runtime headroom: each access step may observe one extra index row when
        // deciding whether a continuation is minted (scan limit+1). This is not
        // part of the plan-hashed cost vector (returned-row budget stays stable).
        let scan_headroom = cost.access_steps();
        Self {
            access_steps: cost.access_steps(),
            scanned_index_rows: cost.scanned_index_rows().saturating_add(scan_headroom),
            point_reads: cost.point_reads(),
            dependent_keys: cost.dependent_keys(),
            intermediate_rows: cost.intermediate_rows(),
            projected_values: cost.projected_values(),
            encoded_result_bytes: cost.encoded_result_bytes(),
        }
    }

    fn step(&mut self) -> Result<(), QueryExecutionError> {
        burn(&mut self.access_steps, 1)
    }

    fn scans(&mut self, amount: u64) -> Result<(), QueryExecutionError> {
        burn(&mut self.scanned_index_rows, amount)
    }

    fn points(&mut self, amount: u64) -> Result<(), QueryExecutionError> {
        burn(&mut self.point_reads, amount)
    }

    fn dependent_keys(&mut self, amount: u64) -> Result<(), QueryExecutionError> {
        burn(&mut self.dependent_keys, amount)
    }

    fn intermediates(&mut self, amount: u64) -> Result<(), QueryExecutionError> {
        burn(&mut self.intermediate_rows, amount)
    }

    fn projected_values(&mut self, amount: u64) -> Result<(), QueryExecutionError> {
        burn(&mut self.projected_values, amount)
    }

    fn encoded_result(&mut self, amount: u64) -> Result<(), QueryExecutionError> {
        burn(&mut self.encoded_result_bytes, amount)
    }
}

fn burn(remaining: &mut u64, amount: u64) -> Result<(), QueryExecutionError> {
    *remaining = remaining
        .checked_sub(amount)
        .ok_or(QueryExecutionError::FuelExhausted)?;
    Ok(())
}

/// Closed backend fault classification retained below the safe public boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryBackendFault {
    /// Transient unavailability that may clear without client restart.
    Unavailable,
    /// Corrupt or invariant-violating durable state.
    Integrity,
    /// A backend resource or semantic ceiling was exceeded.
    LimitExceeded,
}

/// The only operations available while a concrete adapter owns one read transaction.
pub trait QueryReadView {
    /// Adapter-internal error retained below the safe public boundary.
    type Error;

    /// Classifies an adapter error without exposing diagnostics.
    fn fault(&self, error: &Self::Error) -> QueryBackendFault;

    /// Application commit head observed by this exact snapshot.
    fn application_head(&self) -> u64;

    /// Executes one exact primary-key step.
    fn point(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
    ) -> Result<Option<QueryRow>, Self::Error>;

    /// Executes one ordered bounded set of complete primary-key steps.
    ///
    /// The output preserves input position and represents each missing target
    /// explicitly. Concrete adapters keep the whole batch inside this view's
    /// one authoritative snapshot.
    fn dependent_point_batch(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
    ) -> Result<Vec<Option<QueryRow>>, Self::Error>;

    /// Executes one bounded declared-index step.
    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
    ) -> Result<QueryScanPage, Self::Error>;
}

fn map_view_error<V: QueryReadView>(view: &V, error: &V::Error) -> QueryExecutionError {
    match view.fault(error) {
        QueryBackendFault::Unavailable => QueryExecutionError::BackendUnavailable,
        QueryBackendFault::Integrity => QueryExecutionError::BackendIntegrity,
        QueryBackendFault::LimitExceeded => QueryExecutionError::BackendLimitExceeded,
    }
}

/// Checked lower continuation and epoch fence resolved from one opaque cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryContinuation {
    binding: String,
    lower: Vec<u8>,
    index_epochs: BTreeMap<String, u64>,
}

impl QueryContinuation {
    /// Constructs one bounded continuation.
    pub fn checked(
        binding: String,
        lower: Vec<u8>,
        index_epochs: BTreeMap<String, u64>,
    ) -> Option<Self> {
        (!binding.is_empty()
            && !lower.is_empty()
            && lower.len() <= MAX_QUERY_CONTINUATION_BYTES
            && !index_epochs.is_empty())
        .then_some(Self {
            binding,
            lower,
            index_epochs,
        })
    }
}

/// Engine-owned one-snapshot execution boundary.
///
/// Implementations open one read transaction, invoke the closed executor, copy
/// the owned result, and close the transaction before returning.
pub trait QueryExecutionPort: Send + Sync {
    /// Executes one compiler-produced program page against one checked
    /// parameter set and optional validated continuation.
    fn execute_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError>;

    /// Executes one compiler-produced program against one checked parameter set.
    fn execute_query(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        self.execute_query_page(program, parameters, None)
    }
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
    continuation_binding: Option<String>,
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

    /// Binding whose ordered access produced the continuation.
    #[must_use]
    pub fn continuation_binding(&self) -> Option<&str> {
        self.continuation_binding.as_deref()
    }

    /// Consumes the snapshot into its name-addressed result fields.
    #[must_use]
    pub fn into_fields(self) -> BTreeMap<String, QueryResultValue> {
        self.fields
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
    /// A submitted parameter is not in its required canonical representation.
    InvalidParameter {
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
    /// Engine adapter reported corrupt or invariant-violating durable state.
    BackendIntegrity,
    /// Engine adapter reported a resource or semantic ceiling was exceeded.
    BackendLimitExceeded,
    /// A row, byte, scan, or result ceiling was exceeded.
    BoundExceeded,
    /// Backend work or result shaping exhausted the admitted whole-query fuel.
    FuelExhausted,
    /// A `one` or `maybe` binding returned too many rows.
    UnexpectedCardinality {
        /// Safe binding name.
        binding: String,
    },
    /// A predicate uses a value/operator unavailable in v1.
    UnsupportedPredicate,
    /// A dependent collection key is null, duplicate, out of order, or malformed.
    InvalidDependentKey {
        /// Safe source binding name.
        binding: String,
        /// Safe source field name.
        field: String,
    },
    /// Cursor binding or an observed index epoch is stale.
    StaleCursor,
    /// Cursor structure does not match this query.
    InvalidContinuation,
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
    execute_page_in_snapshot(program, parameters, None, view)
}

/// Executes one page with an optional previously validated lower continuation.
pub fn execute_page_in_snapshot<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    view: &mut V,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    let mut fuel = QueryExecutionFuel::from_cost(program.cost());
    let mut bindings = BTreeMap::<String, Vec<QueryRow>>::new();
    let mut result_fields = BTreeMap::new();
    let mut index_epochs = BTreeMap::new();
    let mut continuation_binding = None;
    let mut continuation = None;
    // Hoist once: outcome name is a program constant, not per-step work.
    let default_outcome = program
        .surface()
        .schemas()
        .results()
        .first()
        .map(|branch| branch.name())
        .unwrap_or("Result");

    for (step_index, step) in program.steps().iter().enumerate() {
        fuel.step()?;
        let limit = resolve_row_limit(step, parameters)?;
        let after = prior
            .filter(|cursor| cursor.binding == step.binding())
            .map(|cursor| cursor.lower.as_slice());
        // Shared selected field names for this step (per-query constants).
        let selected_names: Vec<Arc<str>> = step
            .selected_fields()
            .iter()
            .map(|name| Arc::<str>::from(name.as_str()))
            .collect();
        let (mut rows, scalar_predicates) = match step.access() {
            riffdb_query_ir::QueryAccessKind::Point { .. } => {
                let predicates = bind_predicates(step, parameters, &bindings)?;
                fuel.points(1)?;
                (
                    view.point(step, &predicates)
                        .map_err(|error| map_view_error(view, &error))?
                        .into_iter()
                        .collect::<Vec<_>>(),
                    Some(predicates),
                )
            }
            riffdb_query_ir::QueryAccessKind::DependentPointBatch {
                source_binding,
                source_field,
                ..
            } => {
                if after.is_some() {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                let predicates = bind_dependent_point_batch(
                    step,
                    parameters,
                    &bindings,
                    source_binding,
                    source_field,
                    limit,
                )?;
                let key_count = u64::try_from(predicates.len())
                    .map_err(|_| QueryExecutionError::BoundExceeded)?;
                fuel.dependent_keys(key_count)?;
                let observations = view
                    .dependent_point_batch(step, &predicates)
                    .map_err(|error| map_view_error(view, &error))?;
                if observations.len() != predicates.len() {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                fuel.points(
                    u64::try_from(observations.len())
                        .map_err(|_| QueryExecutionError::BoundExceeded)?,
                )?;
                if observations.iter().any(Option::is_none) {
                    let outcome = step
                        .absence_outcome()
                        .ok_or(QueryExecutionError::InvalidProgram)?
                        .to_owned();
                    return finish_snapshot(
                        &mut fuel,
                        QueryOwnedSnapshot {
                            application_head: view.application_head(),
                            index_epochs,
                            outcome,
                            fields: BTreeMap::new(),
                            continuation_binding: None,
                            continuation: None,
                        },
                    );
                }
                let rows = observations
                    .into_iter()
                    .zip(&predicates)
                    .map(|(row, predicates)| {
                        let row = row.ok_or(QueryExecutionError::InvalidProgram)?;
                        if row.entity() != step.entity() || !predicates_match(&row, predicates)? {
                            return Err(QueryExecutionError::InvalidProgram);
                        }
                        Ok(row)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (rows, None)
            }
            riffdb_query_ir::QueryAccessKind::Index { index, .. } => {
                let predicates = bind_predicates(step, parameters, &bindings)?;
                let page = view
                    .scan(step, &predicates, limit, after)
                    .map_err(|error| map_view_error(view, &error))?;
                if page.scanned_rows > MAX_QUERY_SCANNED_ROWS
                    || page.rows.len() as u64 > limit
                    || page.point_reads != page.rows.len() as u64
                    || page.rows.len() as u64 > page.scanned_rows
                    || page
                        .continuation
                        .as_ref()
                        .is_some_and(|value| value.len() > MAX_QUERY_CONTINUATION_BYTES)
                {
                    return Err(QueryExecutionError::BoundExceeded);
                }
                fuel.scans(page.scanned_rows)?;
                fuel.points(page.point_reads)?;
                // Borrow entity/index names into one key; avoid format! per scan step.
                let mut epoch_key = String::with_capacity(step.entity().len() + 1 + index.len());
                epoch_key.push_str(step.entity());
                epoch_key.push('.');
                epoch_key.push_str(index);
                index_epochs.insert(epoch_key, page.epoch);
                if page.continuation.is_some() {
                    if continuation.is_some() {
                        return Err(QueryExecutionError::InvalidProgram);
                    }
                    continuation_binding = Some(step.binding().to_owned());
                    continuation = page.continuation;
                }
                (page.rows, Some(predicates))
            }
        };
        fuel.intermediates(
            u64::try_from(rows.len()).map_err(|_| QueryExecutionError::BoundExceeded)?,
        )?;
        if rows.iter().any(|row| row.entity() != step.entity()) {
            return Err(QueryExecutionError::InvalidProgram);
        }
        if let Some(predicates) = &scalar_predicates {
            rows.retain(|row| predicates_match(row, predicates).unwrap_or(false));
        }
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
            return finish_snapshot(
                &mut fuel,
                QueryOwnedSnapshot {
                    application_head: view.application_head(),
                    index_epochs,
                    outcome,
                    fields: BTreeMap::new(),
                    continuation_binding: None,
                    continuation: None,
                },
            );
        }
        let retain_for_dependents = binding_referenced_later(program, step_index, step.binding());
        let projected = if retain_for_dependents {
            // Later steps still need the full intermediate rows; clone only the
            // selected result projection (not the dual path when unused).
            rows.iter()
                .map(|row| row.project_clone(&selected_names))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            // Move selected values into the result path; drop intermediate fields.
            let mut projected = Vec::with_capacity(rows.len());
            for row in std::mem::take(&mut rows) {
                projected.push(row.into_project(&selected_names)?);
            }
            projected
        };
        let projected_count = u64::try_from(projected.len())
            .ok()
            .and_then(|rows| rows.checked_mul(step.selected_fields().len() as u64))
            .and_then(|values| values.checked_mul(step.result_names().len() as u64))
            .ok_or(QueryExecutionError::BoundExceeded)?;
        fuel.projected_values(projected_count)?;
        let value = match step.cardinality() {
            Cardinality::One => QueryResultValue::One(
                projected
                    .into_iter()
                    .next()
                    .ok_or(QueryExecutionError::InvalidProgram)?,
            ),
            Cardinality::Maybe => QueryResultValue::Maybe(projected.into_iter().next()),
            Cardinality::Many => QueryResultValue::Many(projected),
        };
        // result_names is almost always one entry; move into the last slot.
        let mut result_names = step.result_names().iter();
        if let Some(first) = result_names.next() {
            for name in result_names {
                if result_fields.insert(name.clone(), value.clone()).is_some() {
                    return Err(QueryExecutionError::InvalidProgram);
                }
            }
            if result_fields.insert(first.clone(), value).is_some() {
                return Err(QueryExecutionError::InvalidProgram);
            }
        }
        if retain_for_dependents {
            bindings.insert(step.binding().to_owned(), rows);
        }
    }

    if let Some(prior) = prior {
        if !program
            .steps()
            .iter()
            .any(|step| step.binding() == prior.binding && step.cursor_parameter().is_some())
        {
            return Err(QueryExecutionError::InvalidContinuation);
        }
        if prior.index_epochs != index_epochs {
            return Err(QueryExecutionError::StaleCursor);
        }
    }

    finish_snapshot(
        &mut fuel,
        QueryOwnedSnapshot {
            application_head: view.application_head(),
            index_epochs,
            outcome: default_outcome.to_owned(),
            fields: result_fields,
            continuation_binding,
            continuation,
        },
    )
}

fn binding_referenced_later(
    program: &QueryAccessProgramV1,
    step_index: usize,
    binding: &str,
) -> bool {
    program.steps().iter().skip(step_index + 1).any(|later| {
        later.dependencies().iter().any(|name| name == binding)
            || later
                .predicates()
                .iter()
                .any(|predicate| match predicate.value() {
                    QueryPredicateValue::BindingField {
                        binding: source, ..
                    }
                    | QueryPredicateValue::BindingFieldSet {
                        binding: source, ..
                    } => source == binding,
                    QueryPredicateValue::Parameter(_)
                    | QueryPredicateValue::Literal(_)
                    | QueryPredicateValue::EnumVariant { .. } => false,
                })
    })
}

fn finish_snapshot(
    fuel: &mut QueryExecutionFuel,
    snapshot: QueryOwnedSnapshot,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    fuel.encoded_result(encoded_snapshot_bytes(&snapshot)?)?;
    Ok(snapshot)
}

fn encoded_snapshot_bytes(snapshot: &QueryOwnedSnapshot) -> Result<u64, QueryExecutionError> {
    let mut bytes = 512_u64
        .checked_add(snapshot.outcome.len() as u64)
        .ok_or(QueryExecutionError::BoundExceeded)?;
    for (name, value) in &snapshot.fields {
        bytes = bytes
            .checked_add(name.len() as u64)
            .and_then(|value| value.checked_add(64))
            .ok_or(QueryExecutionError::BoundExceeded)?;
        let rows: &[QueryRow] = match value {
            QueryResultValue::One(row) => std::slice::from_ref(row),
            QueryResultValue::Maybe(Some(row)) => std::slice::from_ref(row),
            QueryResultValue::Maybe(None) => &[],
            QueryResultValue::Many(rows) => rows,
        };
        for row in rows {
            bytes = bytes
                .checked_add(row.entity.len() as u64)
                .and_then(|value| value.checked_add(64))
                .ok_or(QueryExecutionError::BoundExceeded)?;
            for (field, value) in &row.fields {
                let encoded_len = canonical_value_encoded_len(value)
                    .map_err(|_| QueryExecutionError::InvalidProgram)?;
                bytes = bytes
                    .checked_add(field.len() as u64)
                    .and_then(|value| value.checked_add(encoded_len as u64))
                    .and_then(|value| value.checked_add(160))
                    .ok_or(QueryExecutionError::BoundExceeded)?;
            }
        }
    }
    if snapshot.continuation.is_some() {
        bytes = bytes
            .checked_add(48)
            .ok_or(QueryExecutionError::BoundExceeded)?;
    }
    Ok(bytes)
}

fn resolve_row_limit(
    step: &QueryAccessStep,
    parameters: &QueryParameters,
) -> Result<u64, QueryExecutionError> {
    let limit = match step.row_limit() {
        QueryRowLimit::Literal(value) => *value,
        QueryRowLimit::Parameter { name, default } => {
            let value = match parameters.get(name) {
                Some(CanonicalValue::U64(value)) => *value,
                Some(_) => return Err(QueryExecutionError::InvalidProgram),
                None => default.ok_or_else(|| QueryExecutionError::MissingParameter {
                    parameter: name.clone(),
                })?,
            };
            // Parameterized Limit is validated before any backend scan so an
            // over-bound runtime value is InputInvalid, never INTERNAL.
            if value == 0 || !page_take_within_scan_bound(value) || value > step.maximum_rows() {
                return Err(QueryExecutionError::InvalidParameter {
                    parameter: name.clone(),
                });
            }
            value
        }
    };
    if limit == 0 || limit > step.maximum_rows() || !page_take_within_scan_bound(limit) {
        return Err(QueryExecutionError::BoundExceeded);
    }
    Ok(limit)
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
                QueryPredicateValue::BindingFieldSet { .. } => {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                QueryPredicateValue::Literal(literal) => literal_value(literal)?,
                QueryPredicateValue::EnumVariant {
                    type_id,
                    variant_id,
                    ..
                } => CanonicalValue::Enum {
                    type_id: *type_id,
                    variant_id: *variant_id,
                },
            };
            if predicate.operator() == QueryPredicateOperator::In {
                let parameter = match predicate.value() {
                    QueryPredicateValue::Parameter(name) => name,
                    _ => return Err(QueryExecutionError::InvalidProgram),
                };
                validate_canonical_set(&value).map_err(|()| {
                    QueryExecutionError::InvalidParameter {
                        parameter: parameter.clone(),
                    }
                })?;
            }
            Ok(BoundPredicate {
                field: predicate.field().to_owned(),
                operator: predicate.operator(),
                value,
            })
        })
        .collect()
}

fn bind_dependent_point_batch(
    step: &QueryAccessStep,
    parameters: &QueryParameters,
    bindings: &BTreeMap<String, Vec<QueryRow>>,
    source_binding: &str,
    source_field: &str,
    limit: u64,
) -> Result<Vec<Vec<BoundPredicate>>, QueryExecutionError> {
    let source_rows = bindings
        .get(source_binding)
        .ok_or(QueryExecutionError::InvalidProgram)?;
    if source_rows.len() as u64 > limit || source_rows.len() as u64 > step.maximum_rows() {
        return Err(QueryExecutionError::BoundExceeded);
    }
    let values = source_rows
        .iter()
        .map(|row| {
            row.field(source_field)
                .cloned()
                .ok_or_else(|| QueryExecutionError::MissingField {
                    entity: row.entity().to_owned(),
                    field: source_field.to_owned(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values
        .iter()
        .any(|value| matches!(value, CanonicalValue::Null))
    {
        return Err(QueryExecutionError::InvalidDependentKey {
            binding: source_binding.to_owned(),
            field: source_field.to_owned(),
        });
    }
    let encoded = values
        .iter()
        .map(encode_canonical_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| QueryExecutionError::InvalidDependentKey {
            binding: source_binding.to_owned(),
            field: source_field.to_owned(),
        })?;
    if encoded.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(QueryExecutionError::InvalidDependentKey {
            binding: source_binding.to_owned(),
            field: source_field.to_owned(),
        });
    }

    values
        .into_iter()
        .map(|dependent_value| {
            step.predicates()
                .iter()
                .map(|predicate| {
                    let (operator, value) = match predicate.value() {
                        QueryPredicateValue::Parameter(name) => (
                            predicate.operator(),
                            parameters.get(name).cloned().ok_or_else(|| {
                                QueryExecutionError::MissingParameter {
                                    parameter: name.clone(),
                                }
                            })?,
                        ),
                        QueryPredicateValue::BindingField { binding, field } => (
                            predicate.operator(),
                            bindings
                                .get(binding)
                                .and_then(|rows| rows.first())
                                .and_then(|row| row.field(field))
                                .cloned()
                                .unwrap_or(CanonicalValue::Null),
                        ),
                        QueryPredicateValue::BindingFieldSet { binding, field }
                            if binding == source_binding && field == source_field =>
                        {
                            (QueryPredicateOperator::Equal, dependent_value.clone())
                        }
                        QueryPredicateValue::BindingFieldSet { .. } => {
                            return Err(QueryExecutionError::InvalidProgram);
                        }
                        QueryPredicateValue::Literal(literal) => {
                            (predicate.operator(), literal_value(literal)?)
                        }
                        QueryPredicateValue::EnumVariant {
                            type_id,
                            variant_id,
                            ..
                        } => (
                            predicate.operator(),
                            CanonicalValue::Enum {
                                type_id: *type_id,
                                variant_id: *variant_id,
                            },
                        ),
                    };
                    Ok(BoundPredicate {
                        field: predicate.field().to_owned(),
                        operator,
                        value,
                    })
                })
                .collect()
        })
        .collect()
}

fn validate_canonical_set(value: &CanonicalValue) -> Result<(), ()> {
    let CanonicalValue::List(values) = value else {
        return Err(());
    };
    if values.values().is_empty() || values.values().len() > MAX_QUERY_PARAMETERS {
        return Err(());
    }
    let encoded = values
        .values()
        .iter()
        .map(encode_canonical_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ())?;
    if encoded.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(());
    }
    Ok(())
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
                    entity: row.entity.to_string(),
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
