#![forbid(unsafe_code)]

//! Closed-program execution over one engine-owned authoritative read view.

#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use riffdb_policy::AuthorizedQueryRowPolicyContextV1;
use riffdb_query_ir::{
    NamedTypeSchema, OperationalAggregateFunctionV1, OperationalAggregateV1, PageBound,
    QueryAccessKind, QueryAccessProgramV1, QueryAccessStep, QueryLiteral, QueryPredicateOperator,
    QueryPredicateValue, QueryRowLimit,
};
use riffdb_riffql_syntax::Cardinality;
use riffdb_types::{
    CanonicalValue, EntityKey, EntityTypeId, PartitionKeyHash, QueryCostVectorV1,
    canonical_value_encoded_len, encode_canonical_value, hash_partition_key,
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
/// Maximum named queries in one contextual shared snapshot.
pub const MAX_CONTEXTUAL_HYDRATION_QUERIES: usize = 16;

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

/// One parameter-bound invalidation target for an exact live query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundLiveQueryDependency {
    entity_type_id: EntityTypeId,
    partition_hash: PartitionKeyHash,
    entity_key: Option<EntityKey>,
}

impl BoundLiveQueryDependency {
    /// Stable entity identity.
    #[must_use]
    pub const fn entity_type_id(&self) -> EntityTypeId {
        self.entity_type_id
    }

    /// Exact aggregate-owned partition identity for this access step.
    #[must_use]
    pub const fn partition_hash(&self) -> PartitionKeyHash {
        self.partition_hash
    }

    /// Complete point key when it can be bound before execution.
    #[must_use]
    pub const fn entity_key(&self) -> Option<&EntityKey> {
        self.entity_key.as_ref()
    }
}

/// Binds compiler-derived live invalidation facts to one checked parameter set.
///
/// Dependent point batches and index scans deliberately remain conservative at
/// entity-type/partition precision. Independent complete point predicates are
/// reduced to exact canonical entity keys.
pub fn bind_live_query_dependencies(
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
) -> Result<Vec<BoundLiveQueryDependency>, QueryExecutionError> {
    let partition_value = parameters
        .get(program.partition_parameter())
        .ok_or_else(|| QueryExecutionError::MissingParameter {
            parameter: program.partition_parameter().to_owned(),
        })?;
    let empty_bindings = BTreeMap::new();
    program
        .steps()
        .iter()
        .map(|step| {
            let partition = step
                .internal_partition_key_schema()
                .encode_partition(std::slice::from_ref(partition_value))
                .map_err(|_| QueryExecutionError::InvalidProgram)?;
            let entity_key = match step.access() {
                QueryAccessKind::Point { key_fields }
                    if step.predicates().iter().all(|predicate| {
                        !matches!(
                            predicate.value(),
                            QueryPredicateValue::BindingField { .. }
                                | QueryPredicateValue::BindingFieldSet { .. }
                        )
                    }) =>
                {
                    let predicates = bind_predicates(step, parameters, &empty_bindings)?;
                    let values = key_fields
                        .iter()
                        .map(|field| {
                            predicates
                                .iter()
                                .find(|predicate| {
                                    predicate.field() == field
                                        && predicate.operator() == QueryPredicateOperator::Equal
                                })
                                .map(|predicate| predicate.value().clone())
                                .ok_or(QueryExecutionError::InvalidProgram)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Some(
                        step.internal_entity_key_schema()
                            .encode_entity(&values)
                            .map_err(|_| QueryExecutionError::InvalidProgram)?,
                    )
                }
                QueryAccessKind::Point { .. }
                | QueryAccessKind::DependentPointBatch { .. }
                | QueryAccessKind::Index { .. }
                | QueryAccessKind::Nearest { .. } => None,
            };
            Ok(BoundLiveQueryDependency {
                entity_type_id: step.internal_entity_id(),
                partition_hash: hash_partition_key(partition.as_bytes()),
                entity_key,
            })
        })
        .collect()
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

// Test-only clone probe (same gating lesson as redb table-open counters).
// Thread-local so parallel unit-test threads cannot race.
#[cfg(test)]
thread_local! {
    static PIPELINE_CLONE_COUNTING: Cell<bool> = const { Cell::new(false) };
    static PIPELINE_VALUE_CLONES: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
fn note_pipeline_value_clone() {
    PIPELINE_CLONE_COUNTING.with(|enabled| {
        if enabled.get() {
            PIPELINE_VALUE_CLONES.with(|count| count.set(count.get().saturating_add(1)));
        }
    });
}

#[cfg(not(test))]
const fn note_pipeline_value_clone() {}

#[cfg(test)]
fn enable_pipeline_clone_counting() {
    PIPELINE_VALUE_CLONES.with(|count| count.set(0));
    PIPELINE_CLONE_COUNTING.with(|enabled| enabled.set(true));
}

#[cfg(test)]
fn disable_pipeline_clone_counting() -> u64 {
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
    /// Constructs one resolved predicate.
    ///
    /// Production code builds predicates through binding; this constructor
    /// exists for cross-crate tests only.
    #[cfg(feature = "test-fixtures")]
    #[must_use]
    pub fn new(
        field: impl Into<String>,
        operator: QueryPredicateOperator,
        value: CanonicalValue,
    ) -> Self {
        Self {
            field: field.into(),
            operator,
            value,
        }
    }

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

/// Encodes the exact finite set of physical index prefixes selected by predicates.
#[doc(hidden)]
pub fn bound_index_prefix_bytes_v1(
    step: &QueryAccessStep,
    predicates: &[BoundPredicate],
) -> Result<Vec<Vec<u8>>, QueryExecutionError> {
    let QueryAccessKind::Index { fields, .. } = step.access() else {
        return Err(QueryExecutionError::InvalidProgram);
    };
    let schema = step
        .internal_index_key_schema()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let mut leading = vec![Vec::<CanonicalValue>::new()];
    for field in fields {
        let Some(predicate) = predicates
            .iter()
            .find(|predicate| predicate.field() == field)
        else {
            return encode_complete_prefixes(schema, leading);
        };
        match predicate.operator() {
            QueryPredicateOperator::Equal => {
                for values in &mut leading {
                    values.push(predicate.value().clone());
                }
            }
            QueryPredicateOperator::In => {
                let CanonicalValue::List(items) = predicate.value() else {
                    return Err(QueryExecutionError::InvalidProgram);
                };
                if items.values().is_empty() || items.values().len() > MAX_QUERY_PARAMETERS {
                    return Err(QueryExecutionError::BoundExceeded);
                }
                let prior = std::mem::take(&mut leading);
                for values in prior {
                    for item in items.values() {
                        let mut expanded = values.clone();
                        expanded.push(item.clone());
                        leading.push(expanded);
                    }
                }
                return encode_complete_prefixes(schema, leading);
            }
            QueryPredicateOperator::IsNull => {
                for values in &mut leading {
                    let payload = schema
                        .components()
                        .get(values.len() + 1)
                        .ok_or(QueryExecutionError::InvalidProgram)
                        .and_then(|component| {
                            riffdb_contract_ir::presence_placeholder_v1(component)
                                .map_err(|_| QueryExecutionError::InvalidProgram)
                        })?;
                    values.push(CanonicalValue::U64(riffdb_contract_ir::PRESENCE_NULL_V1));
                    values.push(payload);
                }
            }
            QueryPredicateOperator::IsNotNull => {
                for values in &mut leading {
                    values.push(CanonicalValue::U64(riffdb_contract_ir::PRESENCE_VALUE_V1));
                }
                return encode_complete_prefixes(schema, leading);
            }
            QueryPredicateOperator::Exists => {
                let prior = std::mem::take(&mut leading);
                for values in prior {
                    for state in [
                        riffdb_contract_ir::PRESENCE_NULL_V1,
                        riffdb_contract_ir::PRESENCE_VALUE_V1,
                    ] {
                        let mut expanded = values.clone();
                        expanded.push(CanonicalValue::U64(state));
                        leading.push(expanded);
                    }
                }
                return encode_complete_prefixes(schema, leading);
            }
            QueryPredicateOperator::Prefix => {
                let CanonicalValue::String(prefix) = predicate.value() else {
                    return Err(QueryExecutionError::InvalidProgram);
                };
                return leading
                    .into_iter()
                    .map(|values| {
                        schema
                            .encode_index_ordered_prefix(&values, prefix.as_str().as_bytes())
                            .map(|prefix| prefix.as_bytes().to_vec())
                            .map_err(|_| QueryExecutionError::InvalidProgram)
                    })
                    .collect();
            }
            QueryPredicateOperator::NotEqual
            | QueryPredicateOperator::Less
            | QueryPredicateOperator::LessEqual
            | QueryPredicateOperator::Greater
            | QueryPredicateOperator::GreaterEqual => {
                return encode_complete_prefixes(schema, leading);
            }
        }
    }
    encode_complete_prefixes(schema, leading)
}

fn encode_complete_prefixes(
    schema: &riffdb_contract_ir::KeySchema,
    prefixes: Vec<Vec<CanonicalValue>>,
) -> Result<Vec<Vec<u8>>, QueryExecutionError> {
    prefixes
        .into_iter()
        .map(|values| {
            schema
                .encode_index_prefix(&values)
                .map(|prefix| prefix.as_bytes().to_vec())
                .map_err(|_| QueryExecutionError::InvalidProgram)
        })
        .collect()
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

    /// Constructs an exact-end policy-filtered page while retaining physical
    /// candidate work. Denied rows never enter `rows`, but they remain charged
    /// to the compiler-bounded scan ceiling.
    #[doc(hidden)]
    #[must_use]
    pub fn policy_exact_end(rows: Vec<QueryRow>, epoch: u64, scanned_rows: u64) -> Option<Self> {
        let point_reads = rows.len() as u64;
        (scanned_rows >= point_reads).then_some(Self {
            rows,
            epoch,
            scanned_rows,
            point_reads,
            continuation: None,
        })
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

/// One executed nearest-neighbor step: ranked rows plus honest scan work.
///
/// `scanned_rows` reports every row the adapter examined while scanning the
/// org partition, not the rows returned — exact KNN examines the whole
/// partition, and the fuel accounting must charge that examination
/// (previously nothing was charged on the nearest path).
#[derive(Clone, Debug)]
pub struct QueryNearestPage {
    /// Up to `k` rows ordered by ascending distance (closest first).
    pub rows: Vec<QueryRow>,
    /// Rows examined while scanning the org partition.
    ///
    /// Load-bearing for ADR-0087 fuel accounting: the executor charges this
    /// self-reported count against the plan's scan fuel. It bounds the value
    /// above by the scan ceiling and below by `rows.len()`, but has no
    /// independent measure of the adapter's real work — an under-report is a
    /// first-party adapter bug, not a tenant-reachable bypass (the trait has
    /// no plugin surface). The columnar engine supplies the honest count as
    /// `NearestQueryResult::scanned_rows`.
    pub scanned_rows: u64,
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
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
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
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, Self::Error>;

    /// Executes one bounded declared-index step.
    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error>;

    /// Executes one nearest-neighbor search step (ADR-0091, VEC-005).
    ///
    /// The adapter scans the org-partitioned rows for the vector field,
    /// applies the bound predicates BEFORE distance computation and top-K
    /// selection (VEC-006/VEC-007 — the columnar engine's
    /// `nearest_query_snapshot` enforces this order structurally), runs
    /// exact KNN, and returns up to `k` rows ordered by distance (closest
    /// first) together with the honest count of rows examined.
    ///
    /// Implementations that do not support vector queries (row-store adapters)
    /// return an integrity/invariant error.
    fn nearest(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        k: u32,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error>;
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

    /// Executes one compiler-produced operational page and its sealed exact
    /// aggregate descriptors in the same authoritative snapshot.
    fn execute_operational_query_page(
        &self,
        program: &QueryAccessProgramV1,
        aggregates: &[OperationalAggregateV1],
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        if aggregates.is_empty() {
            self.execute_query_page(program, parameters, prior)
        } else {
            Err(QueryExecutionError::InvalidProgram)
        }
    }

    /// Executes a query page with compiler-selected transaction-current row policy.
    ///
    /// The default denies so an adapter cannot accidentally enable protected
    /// execution by implementing only the pre-policy port.
    fn execute_policy_query_page(
        &self,
        _program: &QueryAccessProgramV1,
        _parameters: &QueryParameters,
        _prior: Option<&QueryContinuation>,
        _policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        Err(QueryExecutionError::InvalidProgram)
    }

    /// Executes an operational page with policy applied before aggregation.
    fn execute_policy_operational_query_page(
        &self,
        _program: &QueryAccessProgramV1,
        _aggregates: &[OperationalAggregateV1],
        _parameters: &QueryParameters,
        _prior: Option<&QueryContinuation>,
        _policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        Err(QueryExecutionError::InvalidProgram)
    }

    /// Executes one bounded group while the engine's same read view remains open.
    ///
    /// Implementations must not emulate this with independent transactions.
    fn execute_query_group(
        &self,
        requests: &[QueryExecutionRequest<'_>],
    ) -> Result<Vec<QueryOwnedSnapshot>, QueryExecutionError>;

    /// Executes one bounded hydration group with row policy applied in the
    /// same authoritative read view before any result is shaped.
    ///
    /// The default denies so an adapter that implements only the historical
    /// unprotected group operation cannot accidentally release protected
    /// contextual data.
    fn execute_policy_query_group(
        &self,
        _requests: &[QueryExecutionRequest<'_>],
        _policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<Vec<QueryOwnedSnapshot>, QueryExecutionError> {
        Err(QueryExecutionError::InvalidProgram)
    }

    /// Executes one compiler-produced program against one checked parameter set.
    fn execute_query(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        self.execute_query_page(program, parameters, None)
    }
}

/// One borrowed exact program and parameter set in a contextual hydration group.
#[derive(Clone, Copy)]
pub struct QueryExecutionRequest<'a> {
    program: &'a QueryAccessProgramV1,
    parameters: &'a QueryParameters,
}

impl<'a> QueryExecutionRequest<'a> {
    /// Binds one compiler-owned program to one checked parameter set.
    #[must_use]
    pub const fn new(program: &'a QueryAccessProgramV1, parameters: &'a QueryParameters) -> Self {
        Self {
            program,
            parameters,
        }
    }

    /// Exact program.
    #[must_use]
    pub const fn program(self) -> &'a QueryAccessProgramV1 {
        self.program
    }
    /// Checked parameters.
    #[must_use]
    pub const fn parameters(self) -> &'a QueryParameters {
        self.parameters
    }
}

/// Validates the fixed contextual group bound before an engine opens a view.
pub fn validate_query_execution_group(
    requests: &[QueryExecutionRequest<'_>],
) -> Result<(), QueryExecutionError> {
    if requests.is_empty() || requests.len() > MAX_CONTEXTUAL_HYDRATION_QUERIES {
        Err(QueryExecutionError::BoundExceeded)
    } else {
        Ok(())
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
    /// Exactly one whole-set aggregate record.
    AggregateOne(QueryAggregateRow),
    /// Canonically ordered bounded aggregate groups.
    AggregateMany(Vec<QueryAggregateRow>),
}

/// One scalar cell produced by the exact operational aggregate evaluator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryAggregateCell {
    /// Ordinary canonical group key, count, or min/max value.
    Canonical(CanonicalValue),
    /// Full-width checked decimal coefficient. `CanonicalValue::Decimal`
    /// cannot carry the result-only 39-digit sum domain.
    ExactDecimal {
        /// Signed fixed-scale coefficient.
        coefficient: i128,
        /// Declared decimal scale.
        scale: u8,
    },
}

/// One name-addressed aggregate result record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryAggregateRow {
    entity: Arc<str>,
    fields: BTreeMap<Arc<str>, QueryAggregateCell>,
}

impl QueryAggregateRow {
    /// Query-local aggregate symbol.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Canonically ordered returned cells. A missing field represents the
    /// outer absence of a nested optional min/max result.
    #[must_use]
    pub const fn fields(&self) -> &BTreeMap<Arc<str>, QueryAggregateCell> {
        &self.fields
    }

    /// Consumes the record without cloning names or cells.
    #[must_use]
    pub fn into_parts(self) -> (Arc<str>, BTreeMap<Arc<str>, QueryAggregateCell>) {
        (self.entity, self.fields)
    }
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
    /// Exact aggregate arithmetic overflowed; no partial result is returned.
    AggregateOverflow,
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
    execute_operational_page_in_snapshot_with_policy(program, &[], parameters, prior, view, None)
}

/// Executes one policy-protected page in an already-open authoritative view.
pub fn execute_policy_page_in_snapshot<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    view: &mut V,
    policy: &AuthorizedQueryRowPolicyContextV1,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    execute_operational_page_in_snapshot_with_policy(
        program,
        &[],
        parameters,
        prior,
        view,
        Some(policy),
    )
}

/// Executes one page and its compiler-sealed exact aggregate descriptors in
/// the same already-open engine view.
pub fn execute_operational_page_in_snapshot<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    aggregates: &[OperationalAggregateV1],
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    view: &mut V,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    execute_operational_page_in_snapshot_with_policy(
        program, aggregates, parameters, prior, view, None,
    )
}

/// Executes one operational page with row policy enforced before every shape.
pub fn execute_policy_operational_page_in_snapshot<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    aggregates: &[OperationalAggregateV1],
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    view: &mut V,
    policy: &AuthorizedQueryRowPolicyContextV1,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    execute_operational_page_in_snapshot_with_policy(
        program,
        aggregates,
        parameters,
        prior,
        view,
        Some(policy),
    )
}

fn execute_operational_page_in_snapshot_with_policy<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    aggregates: &[OperationalAggregateV1],
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    view: &mut V,
    policy: Option<&AuthorizedQueryRowPolicyContextV1>,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    if aggregates != program.surface().aggregates() {
        return Err(QueryExecutionError::InvalidProgram);
    }
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
                    view.point(step, &predicates, policy)
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
                    .dependent_point_batch(step, &predicates, policy)
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
                    .scan(step, &predicates, limit, after, policy)
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
            riffdb_query_ir::QueryAccessKind::Nearest { k, .. } => {
                // WP-593: exact KNN execution path. The adapter applies the
                // bound predicates BEFORE ranking (filter-before-rank is
                // enforced structurally in the columnar engine), then runs
                // exact KNN. Scan work is charged against the same fuel the
                // plan cost vector funded (the static charge is the
                // partition-scan ceiling, not K).
                let predicates = bind_predicates(step, parameters, &bindings)?;
                let page = view
                    .nearest(step, &predicates, *k, policy)
                    .map_err(|error| map_view_error(&*view, &error))?;
                if page.scanned_rows > MAX_QUERY_SCANNED_ROWS
                    || page.rows.len() as u64 > u64::from(*k)
                    || page.rows.len() as u64 > page.scanned_rows
                {
                    return Err(QueryExecutionError::BoundExceeded);
                }
                fuel.scans(page.scanned_rows)?;
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
        for aggregate in aggregates
            .iter()
            .filter(|aggregate| aggregate.source_binding() == step.binding())
        {
            let selected = selected_aggregate_fields(program, aggregate)?;
            if selected.is_empty() {
                continue;
            }
            let value = evaluate_operational_aggregate(aggregate, &selected, &rows, parameters)?;
            let groups = match &value {
                QueryResultValue::AggregateOne(_) => 1,
                QueryResultValue::AggregateMany(rows) => rows.len(),
                QueryResultValue::One(_)
                | QueryResultValue::Maybe(_)
                | QueryResultValue::Many(_) => return Err(QueryExecutionError::InvalidProgram),
            };
            let projected = u64::try_from(groups)
                .ok()
                .and_then(|groups| groups.checked_mul(selected.len() as u64))
                .ok_or(QueryExecutionError::BoundExceeded)?;
            fuel.projected_values(projected)?;
            if result_fields
                .insert(aggregate.name().to_owned(), value)
                .is_some()
            {
                return Err(QueryExecutionError::InvalidProgram);
            }
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

fn selected_aggregate_fields(
    program: &QueryAccessProgramV1,
    aggregate: &OperationalAggregateV1,
) -> Result<BTreeSet<String>, QueryExecutionError> {
    let branch = program
        .surface()
        .schemas()
        .results()
        .first()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let Some(field) = branch
        .fields()
        .iter()
        .find(|field| field.name() == aggregate.name())
    else {
        return Ok(BTreeSet::new());
    };
    let record = match field.value_type() {
        NamedTypeSchema::Record(fields) => fields,
        NamedTypeSchema::List { element, .. } => match element.as_ref() {
            NamedTypeSchema::Record(fields) => fields,
            _ => return Err(QueryExecutionError::InvalidProgram),
        },
        _ => return Err(QueryExecutionError::InvalidProgram),
    };
    let available = aggregate
        .group_keys()
        .iter()
        .map(|key| key.field())
        .chain(aggregate.measures().iter().map(|measure| measure.alias()))
        .collect::<BTreeSet<_>>();
    let selected = record
        .iter()
        .map(|field| field.name().to_owned())
        .collect::<BTreeSet<_>>();
    if selected.len() != record.len()
        || selected
            .iter()
            .any(|name| !available.contains(name.as_str()))
    {
        return Err(QueryExecutionError::InvalidProgram);
    }
    Ok(selected)
}

fn evaluate_operational_aggregate(
    aggregate: &OperationalAggregateV1,
    selected: &BTreeSet<String>,
    rows: &[QueryRow],
    parameters: &QueryParameters,
) -> Result<QueryResultValue, QueryExecutionError> {
    let maximum_groups = resolve_page_bound(aggregate.maximum_groups(), parameters)?;
    let mut groups = BTreeMap::<Vec<u8>, (Vec<CanonicalValue>, Vec<&QueryRow>)>::new();
    if aggregate.group_keys().is_empty() {
        groups.insert(Vec::new(), (Vec::new(), rows.iter().collect()));
    } else {
        for row in rows {
            let keys = aggregate
                .group_keys()
                .iter()
                .map(|key| {
                    row.field(key.field()).cloned().ok_or_else(|| {
                        QueryExecutionError::MissingField {
                            entity: aggregate.source_entity().to_owned(),
                            field: key.field().to_owned(),
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let encoded = encode_group_key(&keys)?;
            groups
                .entry(encoded)
                .or_insert_with(|| (keys, Vec::new()))
                .1
                .push(row);
            if groups.len() as u64 > maximum_groups {
                return Err(QueryExecutionError::BoundExceeded);
            }
        }
    }

    let mut output = Vec::with_capacity(groups.len());
    for (_, (keys, grouped_rows)) in groups {
        let mut fields = BTreeMap::new();
        for (descriptor, value) in aggregate.group_keys().iter().zip(keys) {
            if selected.contains(descriptor.field()) {
                fields.insert(
                    Arc::<str>::from(descriptor.field()),
                    QueryAggregateCell::Canonical(value),
                );
            }
        }
        for measure in aggregate.measures() {
            if !selected.contains(measure.alias()) {
                continue;
            }
            let cell = evaluate_aggregate_measure(aggregate, measure, &grouped_rows)?;
            if let Some(cell) = cell {
                fields.insert(Arc::<str>::from(measure.alias()), cell);
            }
        }
        output.push(QueryAggregateRow {
            entity: Arc::<str>::from(aggregate.name()),
            fields,
        });
    }
    if aggregate.group_keys().is_empty() {
        let row = output
            .into_iter()
            .next()
            .ok_or(QueryExecutionError::InvalidProgram)?;
        Ok(QueryResultValue::AggregateOne(row))
    } else {
        Ok(QueryResultValue::AggregateMany(output))
    }
}

fn evaluate_aggregate_measure(
    aggregate: &OperationalAggregateV1,
    measure: &riffdb_query_ir::OperationalAggregateMeasureV1,
    rows: &[&QueryRow],
) -> Result<Option<QueryAggregateCell>, QueryExecutionError> {
    match measure.function() {
        OperationalAggregateFunctionV1::Count => {
            Ok(Some(QueryAggregateCell::Canonical(CanonicalValue::U64(
                u64::try_from(rows.len()).map_err(|_| QueryExecutionError::BoundExceeded)?,
            ))))
        }
        OperationalAggregateFunctionV1::Sum => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let scale = aggregate_sum_scale(measure.result_type())?;
            let mut coefficient = 0_i128;
            for row in rows {
                let value = row
                    .field(field)
                    .ok_or_else(|| QueryExecutionError::MissingField {
                        entity: aggregate.source_entity().to_owned(),
                        field: field.to_owned(),
                    })?;
                let contribution = match value {
                    CanonicalValue::I64(value) => i128::from(*value),
                    CanonicalValue::U64(value) => i128::from(*value),
                    CanonicalValue::Decimal(value) if value.spec().scale() == scale => {
                        value.coefficient()
                    }
                    _ => return Err(QueryExecutionError::InvalidProgram),
                };
                coefficient = coefficient
                    .checked_add(contribution)
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
            }
            Ok(Some(QueryAggregateCell::ExactDecimal {
                coefficient,
                scale,
            }))
        }
        OperationalAggregateFunctionV1::Min | OperationalAggregateFunctionV1::Max => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let mut selected_value: Option<&CanonicalValue> = None;
            for row in rows {
                let value = row
                    .field(field)
                    .ok_or_else(|| QueryExecutionError::MissingField {
                        entity: aggregate.source_entity().to_owned(),
                        field: field.to_owned(),
                    })?;
                selected_value = Some(match selected_value {
                    None => value,
                    Some(current) => {
                        let ordering = aggregate_scalar_order(value, current)?;
                        let replace = match measure.function() {
                            OperationalAggregateFunctionV1::Min => ordering == Ordering::Less,
                            OperationalAggregateFunctionV1::Max => ordering == Ordering::Greater,
                            OperationalAggregateFunctionV1::Count
                            | OperationalAggregateFunctionV1::Sum => unreachable!(),
                        };
                        if replace { value } else { current }
                    }
                });
            }
            match selected_value {
                Some(value) => Ok(Some(QueryAggregateCell::Canonical(value.clone()))),
                None if matches!(
                    measure.result_type(),
                    NamedTypeSchema::Optional(inner)
                        if matches!(inner.as_ref(), NamedTypeSchema::Optional(_))
                ) =>
                {
                    Ok(None)
                }
                None => Ok(Some(QueryAggregateCell::Canonical(CanonicalValue::Null))),
            }
        }
    }
}

fn aggregate_sum_scale(result_type: &NamedTypeSchema) -> Result<u8, QueryExecutionError> {
    let NamedTypeSchema::Scalar(name) = result_type else {
        return Err(QueryExecutionError::InvalidProgram);
    };
    let body = name
        .strip_prefix("decimal<")
        .and_then(|name| name.strip_suffix('>'))
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let (_, scale) = body
        .split_once(',')
        .ok_or(QueryExecutionError::InvalidProgram)?;
    scale
        .parse::<u8>()
        .map_err(|_| QueryExecutionError::InvalidProgram)
}

fn resolve_page_bound(
    bound: &PageBound,
    parameters: &QueryParameters,
) -> Result<u64, QueryExecutionError> {
    match bound {
        PageBound::Literal(value) if *value > 0 => Ok(*value),
        PageBound::Parameter(name) => match parameters.get(name) {
            Some(CanonicalValue::U64(value))
                if *value > 0 && page_take_within_scan_bound(*value) =>
            {
                Ok(*value)
            }
            Some(_) => Err(QueryExecutionError::InvalidParameter {
                parameter: name.clone(),
            }),
            None => Err(QueryExecutionError::MissingParameter {
                parameter: name.clone(),
            }),
        },
        PageBound::Literal(_) => Err(QueryExecutionError::InvalidProgram),
    }
}

fn encode_group_key(values: &[CanonicalValue]) -> Result<Vec<u8>, QueryExecutionError> {
    let mut output = Vec::new();
    for value in values {
        let encoded =
            encode_canonical_value(value).map_err(|_| QueryExecutionError::InvalidProgram)?;
        output.extend_from_slice(
            &u32::try_from(encoded.len())
                .map_err(|_| QueryExecutionError::BoundExceeded)?
                .to_be_bytes(),
        );
        output.extend_from_slice(&encoded);
    }
    Ok(output)
}

fn aggregate_scalar_order(
    left: &CanonicalValue,
    right: &CanonicalValue,
) -> Result<Ordering, QueryExecutionError> {
    if let Some(ordering) = scalar_order(left, right) {
        return Ok(ordering);
    }
    match (left, right) {
        (CanonicalValue::Null, CanonicalValue::Null) => Ok(Ordering::Equal),
        (CanonicalValue::Null, _) => Ok(Ordering::Less),
        (_, CanonicalValue::Null) => Ok(Ordering::Greater),
        (CanonicalValue::Bool(left), CanonicalValue::Bool(right)) => Ok(left.cmp(right)),
        (CanonicalValue::Bytes(left), CanonicalValue::Bytes(right)) => {
            Ok(left.as_bytes().cmp(right.as_bytes()))
        }
        (CanonicalValue::Decimal(left), CanonicalValue::Decimal(right))
            if left.spec() == right.spec() =>
        {
            Ok(left.coefficient().cmp(&right.coefficient()))
        }
        (CanonicalValue::Money(left), CanonicalValue::Money(right))
            if left.currency() == right.currency() =>
        {
            Ok(left
                .amount()
                .coefficient()
                .cmp(&right.amount().coefficient()))
        }
        _ => {
            let left =
                encode_canonical_value(left).map_err(|_| QueryExecutionError::InvalidProgram)?;
            let right =
                encode_canonical_value(right).map_err(|_| QueryExecutionError::InvalidProgram)?;
            Ok(left.cmp(&right))
        }
    }
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
        match value {
            QueryResultValue::One(row) => {
                bytes = encoded_query_rows(bytes, std::slice::from_ref(row))?;
            }
            QueryResultValue::Maybe(Some(row)) => {
                bytes = encoded_query_rows(bytes, std::slice::from_ref(row))?;
            }
            QueryResultValue::Maybe(None) => {}
            QueryResultValue::Many(rows) => {
                bytes = encoded_query_rows(bytes, rows)?;
            }
            QueryResultValue::AggregateOne(row) => {
                bytes = encoded_aggregate_rows(bytes, std::slice::from_ref(row))?;
            }
            QueryResultValue::AggregateMany(rows) => {
                bytes = encoded_aggregate_rows(bytes, rows)?;
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

fn encoded_query_rows(mut bytes: u64, rows: &[QueryRow]) -> Result<u64, QueryExecutionError> {
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
    Ok(bytes)
}

fn encoded_aggregate_rows(
    mut bytes: u64,
    rows: &[QueryAggregateRow],
) -> Result<u64, QueryExecutionError> {
    for row in rows {
        bytes = bytes
            .checked_add(row.entity.len() as u64)
            .and_then(|value| value.checked_add(64))
            .ok_or(QueryExecutionError::BoundExceeded)?;
        for (field, value) in &row.fields {
            let encoded_len = match value {
                QueryAggregateCell::Canonical(value) => canonical_value_encoded_len(value)
                    .map_err(|_| QueryExecutionError::InvalidProgram)?,
                QueryAggregateCell::ExactDecimal { .. } => 18,
            };
            bytes = bytes
                .checked_add(field.len() as u64)
                .and_then(|value| value.checked_add(encoded_len as u64))
                .and_then(|value| value.checked_add(160))
                .ok_or(QueryExecutionError::BoundExceeded)?;
        }
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
                QueryPredicateValue::BindingField { binding, field } => {
                    // Compiler invariant: every binding read is in dependencies()
                    // and was retained by a prior step.
                    debug_assert!(
                        bindings.contains_key(binding),
                        "binding {binding:?} is read but missing from retained bindings"
                    );
                    bindings
                        .get(binding)
                        .and_then(|rows| rows.first())
                        .and_then(|row| row.field(field))
                        .cloned()
                        .unwrap_or(CanonicalValue::Null)
                }
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
                        QueryPredicateValue::BindingField { binding, field } => {
                            debug_assert!(
                                bindings.contains_key(binding),
                                "binding {binding:?} is read but missing from retained bindings"
                            );
                            (
                                predicate.operator(),
                                bindings
                                    .get(binding)
                                    .and_then(|rows| rows.first())
                                    .and_then(|row| row.field(field))
                                    .cloned()
                                    .unwrap_or(CanonicalValue::Null),
                            )
                        }
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
        let actual = row.field(&predicate.field);
        let unary = match predicate.operator {
            QueryPredicateOperator::IsNull => Some(matches!(actual, Some(CanonicalValue::Null))),
            QueryPredicateOperator::IsNotNull => {
                Some(actual.is_some_and(|value| !matches!(value, CanonicalValue::Null)))
            }
            QueryPredicateOperator::Exists => Some(actual.is_some()),
            _ => None,
        };
        if let Some(matches) = unary {
            if !matches {
                return Ok(false);
            }
            continue;
        }
        let actual = actual.ok_or_else(|| QueryExecutionError::MissingField {
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
            QueryPredicateOperator::Prefix => match (actual, &predicate.value) {
                (CanonicalValue::String(actual), CanonicalValue::String(prefix)) => {
                    actual.as_str().starts_with(prefix.as_str())
                }
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
                    | QueryPredicateOperator::In
                    | QueryPredicateOperator::IsNull
                    | QueryPredicateOperator::IsNotNull
                    | QueryPredicateOperator::Exists
                    | QueryPredicateOperator::Prefix => unreachable!(),
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

#[cfg(test)]
mod pipeline_clone_tests {
    use super::*;
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_query_compiler::{compile_operational_query_family, compile_query};
    use riffdb_query_ir::{QueryAccessStep, SymbolicCatalog};
    use riffdb_riffql_syntax::parse_query;
    use riffdb_types::CanonicalRecord;

    const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
    const OPEN_TICKETS: &str = r#"
query OpenTickets(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == TicketStatus.Open
        order by ticket_id asc
        take 5
    return Found { tickets: tickets { ticket_id status } }
    outcomes Found
}
"#;

    const AGGREGATE_CONTRACT: &str = r#"
contract OperationalAggregate version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: string<32>
    field story_points: i64
    index by_status (organization_id, status, ticket_id)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
}
"#;

    const GROUPED_SUMMARY: &str = r#"
query GroupedSummary($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by status asc, ticket_id asc
        take 5
    aggregate summary from tickets {
        group by status
        count() as ticket_count
        sum(story_points) as total_points
    }
    return Found { summary: summary { status ticket_count total_points } }
    outcomes Found
}
"#;

    const PARAMETER_BOUNDED_GROUPED_SUMMARY: &str = r#"
query ParameterBoundedGroupedSummary(
    $organization_id: Ticket.organization_id,
    $limit: Limit = 5,
) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by status asc, ticket_id asc
        take $limit
    aggregate summary from tickets {
        group by status
        count() as ticket_count
    }
    return Found { summary: summary { status ticket_count } }
    outcomes Found
}
"#;

    const EMPTY_SUMMARY: &str = r#"
query EmptySummary($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by status asc, ticket_id asc
        take 5
    aggregate summary from tickets {
        count() as ticket_count
        sum(story_points) as total_points
        min(story_points) as minimum_points
    }
    return Found { summary: summary { ticket_count total_points minimum_points } }
    outcomes Found
}
"#;

    const DECIMAL_AGGREGATE_CONTRACT: &str = r#"
contract DecimalAggregate version 1 {
  entity Entry {
    key (organization_id: uuid, entry_id: uuid)
    field amount: decimal<38,0>
    index by_entry (organization_id, entry_id)
  }
  aggregate Entries {
    root Entry
    partition_by organization_id
    conflict_key (organization_id, entry_id)
  }
}
"#;

    const OVERFLOWING_SUMMARY: &str = r#"
query OverflowingSummary($organization_id: Entry.organization_id) {
    many entries from Entry
        where organization_id == $organization_id
        order by entry_id asc
        take 5
    aggregate summary from entries { sum(amount) as total }
    return Found { summary: summary { total } }
    outcomes Found
}
"#;

    const OPERATIONAL_INDEX_CONTRACT: &str = r#"
contract OperationalIndexQueries version 1 {
  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field deleted_at: optional<timestamp>
    field title: string<64>
    index by_deleted (organization_id, deleted_at, document_id) presence(deleted_at)
    index by_title (organization_id, title, document_id) text_key(title, binary_utf8_v1)
  }
  aggregate Documents {
    root Document
    partition_by organization_id
    conflict_key (organization_id, document_id)
  }
}
"#;

    const NULL_DOCUMENTS: &str = r#"
query NullDocuments($organization_id: Document.organization_id) {
    many documents from Document
        where organization_id == $organization_id && deleted_at is null
        order by document_id asc
        take 10
    return Found { documents: documents { document_id deleted_at } }
    outcomes Found
}
"#;

    const EXISTING_DOCUMENTS: &str = r#"
query ExistingDocuments($organization_id: Document.organization_id) {
    many documents from Document
        where organization_id == $organization_id && exists deleted_at
        order by deleted_at asc, document_id asc
        take 10
    return Found { documents: documents { document_id deleted_at } }
    outcomes Found
}
"#;

    const PREFIX_DOCUMENTS: &str = r#"
query PrefixDocuments(
    $organization_id: Document.organization_id,
    $prefix: Document.title,
) {
    many documents from Document
        where organization_id == $organization_id && title prefix $prefix
        order by title asc, document_id asc
        take 10
    return Found { documents: documents { document_id title } }
    outcomes Found
}
"#;

    const OPTIONAL_MINIMUM_SUMMARY: &str = r#"
query OptionalMinimumSummary($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by status asc, ticket_id asc
        take 5
    aggregate summary from tickets { min(story_points) as minimum_points }
    return Found { summary: summary { minimum_points } }
    outcomes Found
}
"#;

    struct FakeView {
        rows: BTreeMap<String, Vec<QueryRow>>,
    }

    impl QueryReadView for FakeView {
        type Error = ();

        fn fault(&self, _error: &Self::Error) -> QueryBackendFault {
            QueryBackendFault::Unavailable
        }

        fn application_head(&self) -> u64 {
            1
        }

        fn point(
            &mut self,
            step: &QueryAccessStep,
            _predicates: &[BoundPredicate],
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<Option<QueryRow>, Self::Error> {
            Ok(self
                .rows
                .get(step.binding())
                .and_then(|rows| rows.first())
                .cloned())
        }

        fn dependent_point_batch(
            &mut self,
            step: &QueryAccessStep,
            predicates: &[Vec<BoundPredicate>],
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
            Ok(self
                .rows
                .get(step.binding())
                .into_iter()
                .flatten()
                .cloned()
                .map(Some)
                .take(predicates.len())
                .collect())
        }

        fn scan(
            &mut self,
            step: &QueryAccessStep,
            _predicates: &[BoundPredicate],
            _limit: u64,
            _after: Option<&[u8]>,
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<QueryScanPage, Self::Error> {
            let rows = self.rows.get(step.binding()).cloned().unwrap_or_default();
            Ok(QueryScanPage::exact_end(rows, 1))
        }

        fn nearest(
            &mut self,
            step: &QueryAccessStep,
            _predicates: &[BoundPredicate],
            _k: u32,
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<QueryNearestPage, Self::Error> {
            let rows = self.rows.get(step.binding()).cloned().unwrap_or_default();
            let scanned_rows = rows.len() as u64;
            Ok(QueryNearestPage { rows, scanned_rows })
        }
    }

    fn row(entity: &str, fields: &[(&str, CanonicalValue)]) -> QueryRow {
        QueryRow::checked(
            entity.to_owned(),
            fields
                .iter()
                .map(|(name, value)| ((*name).to_owned(), value.clone()))
                .collect(),
        )
        .expect("row")
    }

    fn compiled_operational_step(
        source: &str,
    ) -> (riffdb_contract_ir::ContractBundle, QueryAccessStep) {
        let bundle = compile_contract_source(OPERATIONAL_INDEX_CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let family = compile_operational_query_family(
            &parse_query(source).expect("operational query"),
            &catalog,
        )
        .expect("operational family");
        (
            bundle,
            family.select(&[]).expect("sole member").program().steps()[0].clone(),
        )
    }

    fn matching_prefix(prefixes: &[Vec<u8>], key: &[u8]) -> bool {
        prefixes.iter().any(|prefix| key.starts_with(prefix))
    }

    #[test]
    fn presence_prefixes_and_row_filters_distinguish_missing_null_and_value() {
        let organization = CanonicalValue::Uuid([1; 16]);
        let parameters = QueryParameters::checked(BTreeMap::from([(
            "organization_id".to_owned(),
            organization.clone(),
        )]))
        .expect("parameters");
        let (bundle, null_step) = compiled_operational_step(NULL_DOCUMENTS);
        let null_predicates = bind_predicates(&null_step, &parameters, &BTreeMap::new())
            .expect("bound null predicates");
        let null_prefixes =
            bound_index_prefix_bytes_v1(&null_step, &null_predicates).expect("null prefixes");
        let entity = &bundle.schema().entities()[0];
        let index = entity
            .indexes()
            .iter()
            .find(|index| index.name() == "by_deleted")
            .expect("presence index");
        let deleted_field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "deleted_at")
            .expect("deleted field")
            .id();
        let field_id = |name: &str| {
            entity
                .record()
                .fields()
                .iter()
                .find(|field| field.name() == name)
                .expect("field")
                .id()
        };
        for (ordinal, (presence, expected_null)) in [
            None,
            Some(CanonicalValue::Null),
            Some(CanonicalValue::Timestamp(
                riffdb_types::Timestamp::new(1, 0).expect("timestamp"),
            )),
        ]
        .into_iter()
        .zip([false, true, false])
        .enumerate()
        {
            let document_id = CanonicalValue::Uuid([ordinal as u8 + 2; 16]);
            let mut fields = vec![
                (field_id("organization_id"), organization.clone()),
                (field_id("document_id"), document_id.clone()),
            ];
            if let Some(value) = presence {
                fields.push((deleted_field, value));
            }
            let record = CanonicalRecord::new(fields).expect("presence record");
            let values = riffdb_contract_ir::encode_operational_index_values_v1(index, &record)
                .expect("physical presence values");
            let key = index
                .key_schema()
                .encode_index(
                    &values,
                    entity
                        .primary_key()
                        .encode_entity(&[organization.clone(), document_id])
                        .expect("entity key"),
                )
                .expect("index key");
            assert_eq!(
                matching_prefix(&null_prefixes, key.as_bytes()),
                expected_null
            );
        }

        let (_, exists_step) = compiled_operational_step(EXISTING_DOCUMENTS);
        let exists_predicates = bind_predicates(&exists_step, &parameters, &BTreeMap::new())
            .expect("bound exists predicates");
        let missing_row = row("Document", &[("organization_id", organization.clone())]);
        let null_row = row(
            "Document",
            &[
                ("organization_id", organization.clone()),
                ("deleted_at", CanonicalValue::Null),
            ],
        );
        let value_row = row(
            "Document",
            &[
                ("organization_id", organization),
                (
                    "deleted_at",
                    CanonicalValue::Timestamp(
                        riffdb_types::Timestamp::new(1, 0).expect("timestamp"),
                    ),
                ),
            ],
        );
        assert!(!predicates_match(&missing_row, &exists_predicates).expect("missing filter"));
        assert!(predicates_match(&null_row, &exists_predicates).expect("null filter"));
        assert!(predicates_match(&value_row, &exists_predicates).expect("value filter"));
    }

    #[test]
    fn binary_text_prefix_selects_only_the_exact_ordered_byte_range() {
        let organization = CanonicalValue::Uuid([7; 16]);
        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization.clone()),
            (
                "prefix".to_owned(),
                CanonicalValue::string("ab").expect("prefix"),
            ),
        ]))
        .expect("parameters");
        let (bundle, step) = compiled_operational_step(PREFIX_DOCUMENTS);
        let predicates =
            bind_predicates(&step, &parameters, &BTreeMap::new()).expect("bound predicates");
        let prefixes = bound_index_prefix_bytes_v1(&step, &predicates).expect("prefix range");
        assert_eq!(prefixes.len(), 1);
        let entity = &bundle.schema().entities()[0];
        let index = entity
            .indexes()
            .iter()
            .find(|index| index.name() == "by_title")
            .expect("text index");
        let title_field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "title")
            .expect("title field")
            .id();
        for (title, expected) in [("a", false), ("ab", true), ("abacus", true), ("ac", false)] {
            let document_id = CanonicalValue::Uuid([title.len() as u8; 16]);
            let record = CanonicalRecord::new(vec![
                (
                    entity
                        .record()
                        .fields()
                        .iter()
                        .find(|field| field.name() == "organization_id")
                        .expect("organization field")
                        .id(),
                    organization.clone(),
                ),
                (
                    entity
                        .record()
                        .fields()
                        .iter()
                        .find(|field| field.name() == "document_id")
                        .expect("document field")
                        .id(),
                    document_id.clone(),
                ),
                (title_field, CanonicalValue::string(title).expect("title")),
            ])
            .expect("record");
            let values = riffdb_contract_ir::encode_operational_index_values_v1(index, &record)
                .expect("physical text values");
            let entity_key = entity
                .primary_key()
                .encode_entity(&[organization.clone(), document_id])
                .expect("entity key");
            let key = index
                .key_schema()
                .encode_index(&values, entity_key)
                .expect("index key");
            assert_eq!(
                matching_prefix(&prefixes, key.as_bytes()),
                expected,
                "{title}"
            );
        }
    }

    /// Falsifiability: leaf many-query projection must not clone selected values.
    /// Transcript: force project_clone → observed 6 → restore move path → 0.
    #[test]
    fn leaf_many_projection_does_not_clone_selected_values() {
        enable_pipeline_clone_counting();
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program =
            compile_query(&parse_query(OPEN_TICKETS).expect("query"), &catalog).expect("program");
        let status = catalog.enumeration("TicketStatus").expect("status");
        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
            ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
        ]))
        .expect("parameters");
        let tickets: Vec<QueryRow> = (0u8..3)
            .map(|n| {
                row(
                    "Ticket",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("project_id", CanonicalValue::Uuid([2; 16])),
                        ("ticket_id", CanonicalValue::Uuid([n; 16])),
                        (
                            "status",
                            CanonicalValue::Enum {
                                type_id: status.internal_id(),
                                variant_id: status.variant("Open").expect("Open"),
                            },
                        ),
                    ],
                )
            })
            .collect();
        let mut view = FakeView {
            rows: BTreeMap::from([("tickets".to_owned(), tickets)]),
        };
        let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("execute");
        let clones = disable_pipeline_clone_counting();
        match snapshot.fields().get("tickets") {
            Some(QueryResultValue::Many(rows)) => assert_eq!(rows.len(), 3),
            other => panic!("expected 3 tickets, got {other:?}"),
        }
        assert_eq!(
            clones, 0,
            "per-row result-path clone count must be 0 for a leaf 3-row query (observed {clones})"
        );
    }

    #[test]
    fn operational_aggregates_are_exact_grouped_and_canonically_ordered() {
        let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let family = compile_operational_query_family(
            &parse_query(GROUPED_SUMMARY).expect("query"),
            &catalog,
        )
        .expect("family");
        let program = family.select(&[]).expect("sole member").program();
        let parameters = QueryParameters::checked(BTreeMap::from([(
            "organization_id".to_owned(),
            CanonicalValue::Uuid([1; 16]),
        )]))
        .expect("parameters");
        let tickets = vec![
            row(
                "Ticket",
                &[
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("ticket_id", CanonicalValue::Uuid([2; 16])),
                    ("status", CanonicalValue::string("Open").expect("status")),
                    ("story_points", CanonicalValue::I64(3)),
                ],
            ),
            row(
                "Ticket",
                &[
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("ticket_id", CanonicalValue::Uuid([3; 16])),
                    ("status", CanonicalValue::string("Closed").expect("status")),
                    ("story_points", CanonicalValue::I64(5)),
                ],
            ),
            row(
                "Ticket",
                &[
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("ticket_id", CanonicalValue::Uuid([4; 16])),
                    ("status", CanonicalValue::string("Open").expect("status")),
                    ("story_points", CanonicalValue::I64(7)),
                ],
            ),
        ];
        let mut view = FakeView {
            rows: BTreeMap::from([("tickets".to_owned(), tickets)]),
        };
        let snapshot = execute_operational_page_in_snapshot(
            program,
            family.aggregates(),
            &parameters,
            None,
            &mut view,
        )
        .expect("execute exact aggregate");
        let Some(QueryResultValue::AggregateMany(groups)) = snapshot.fields().get("summary") else {
            panic!("grouped aggregate result missing: {:?}", snapshot.fields());
        };
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0].fields().get("status"),
            Some(&QueryAggregateCell::Canonical(
                CanonicalValue::string("Open").expect("status")
            ))
        );
        assert_eq!(
            groups[0].fields().get("ticket_count"),
            Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(2)))
        );
        assert_eq!(
            groups[0].fields().get("total_points"),
            Some(&QueryAggregateCell::ExactDecimal {
                coefficient: 10,
                scale: 0,
            })
        );
    }

    #[test]
    fn aggregate_execution_requires_the_programs_exact_sealed_descriptors() {
        let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let family = compile_operational_query_family(
            &parse_query(GROUPED_SUMMARY).expect("query"),
            &catalog,
        )
        .expect("family");
        let parameters = QueryParameters::checked(BTreeMap::from([(
            "organization_id".to_owned(),
            CanonicalValue::Uuid([1; 16]),
        )]))
        .expect("parameters");
        let mut view = FakeView {
            rows: BTreeMap::from([("tickets".to_owned(), Vec::new())]),
        };

        assert_eq!(
            execute_operational_page_in_snapshot(
                family.select(&[]).expect("sole member").program(),
                &[],
                &parameters,
                None,
                &mut view,
            ),
            Err(QueryExecutionError::InvalidProgram),
            "an executor caller cannot omit or substitute compiler-sealed aggregate work"
        );
    }

    #[test]
    fn runtime_limit_clamps_group_cardinality_before_backend_access() {
        let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let family = compile_operational_query_family(
            &parse_query(PARAMETER_BOUNDED_GROUPED_SUMMARY).expect("query"),
            &catalog,
        )
        .expect("family");
        assert_eq!(
            family.aggregates()[0].maximum_groups(),
            &PageBound::Parameter("limit".to_owned())
        );
        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
            ("limit".to_owned(), CanonicalValue::U64(501)),
        ]))
        .expect("parameters");
        let mut view = FakeView {
            rows: BTreeMap::from([("tickets".to_owned(), Vec::new())]),
        };

        assert_eq!(
            execute_operational_page_in_snapshot(
                family.select(&[]).expect("sole member").program(),
                family.aggregates(),
                &parameters,
                None,
                &mut view,
            ),
            Err(QueryExecutionError::InvalidParameter {
                parameter: "limit".to_owned(),
            })
        );
    }

    #[test]
    fn whole_set_empty_aggregate_preserves_wp492_identities() {
        let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let family =
            compile_operational_query_family(&parse_query(EMPTY_SUMMARY).expect("query"), &catalog)
                .expect("family");
        let program = family.select(&[]).expect("sole member").program();
        let parameters = QueryParameters::checked(BTreeMap::from([(
            "organization_id".to_owned(),
            CanonicalValue::Uuid([1; 16]),
        )]))
        .expect("parameters");
        let mut view = FakeView {
            rows: BTreeMap::from([("tickets".to_owned(), Vec::new())]),
        };
        let snapshot = execute_operational_page_in_snapshot(
            program,
            family.aggregates(),
            &parameters,
            None,
            &mut view,
        )
        .expect("execute empty aggregate");
        let Some(QueryResultValue::AggregateOne(summary)) = snapshot.fields().get("summary") else {
            panic!(
                "whole-set aggregate result missing: {:?}",
                snapshot.fields()
            );
        };
        assert_eq!(
            summary.fields().get("ticket_count"),
            Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(0)))
        );
        assert_eq!(
            summary.fields().get("total_points"),
            Some(&QueryAggregateCell::ExactDecimal {
                coefficient: 0,
                scale: 0,
            })
        );
        assert_eq!(
            summary.fields().get("minimum_points"),
            Some(&QueryAggregateCell::Canonical(CanonicalValue::Null))
        );
    }

    #[test]
    fn optional_minimum_distinguishes_empty_input_from_present_null() {
        let contract = AGGREGATE_CONTRACT.replace(
            "    field story_points: i64",
            "    field story_points: optional<i64>",
        );
        let bundle = compile_contract_source(&contract).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let family = compile_operational_query_family(
            &parse_query(OPTIONAL_MINIMUM_SUMMARY).expect("query"),
            &catalog,
        )
        .expect("family");
        let program = family.select(&[]).expect("sole member").program();
        let parameters = QueryParameters::checked(BTreeMap::from([(
            "organization_id".to_owned(),
            CanonicalValue::Uuid([1; 16]),
        )]))
        .expect("parameters");

        let mut empty_view = FakeView {
            rows: BTreeMap::from([("tickets".to_owned(), Vec::new())]),
        };
        let empty = execute_operational_page_in_snapshot(
            program,
            family.aggregates(),
            &parameters,
            None,
            &mut empty_view,
        )
        .expect("empty aggregate");
        let Some(QueryResultValue::AggregateOne(empty)) = empty.fields().get("summary") else {
            panic!("empty summary")
        };
        assert_eq!(empty.fields().get("minimum_points"), None);

        let mut null_view = FakeView {
            rows: BTreeMap::from([(
                "tickets".to_owned(),
                vec![row(
                    "Ticket",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("ticket_id", CanonicalValue::Uuid([2; 16])),
                        ("status", CanonicalValue::string("Open").expect("status")),
                        ("story_points", CanonicalValue::Null),
                    ],
                )],
            )]),
        };
        let present_null = execute_operational_page_in_snapshot(
            program,
            family.aggregates(),
            &parameters,
            None,
            &mut null_view,
        )
        .expect("present null aggregate");
        let Some(QueryResultValue::AggregateOne(present_null)) =
            present_null.fields().get("summary")
        else {
            panic!("present-null summary")
        };
        assert_eq!(
            present_null.fields().get("minimum_points"),
            Some(&QueryAggregateCell::Canonical(CanonicalValue::Null))
        );
    }

    #[test]
    fn aggregate_overflow_withholds_the_complete_result() {
        let bundle = compile_contract_source(DECIMAL_AGGREGATE_CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let family = compile_operational_query_family(
            &parse_query(OVERFLOWING_SUMMARY).expect("query"),
            &catalog,
        )
        .expect("family");
        let program = family.select(&[]).expect("sole member").program();
        let parameters = QueryParameters::checked(BTreeMap::from([(
            "organization_id".to_owned(),
            CanonicalValue::Uuid([1; 16]),
        )]))
        .expect("parameters");
        let spec = riffdb_types::DecimalSpec::new(38, 0).expect("decimal spec");
        let amount = riffdb_types::Decimal::new(spec, 10_i128.pow(38) - 1)
            .expect("maximum contract decimal");
        let entries = [2_u8, 3_u8]
            .into_iter()
            .map(|id| {
                row(
                    "Entry",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("entry_id", CanonicalValue::Uuid([id; 16])),
                        ("amount", CanonicalValue::Decimal(amount)),
                    ],
                )
            })
            .collect();
        let mut view = FakeView {
            rows: BTreeMap::from([("entries".to_owned(), entries)]),
        };
        assert_eq!(
            execute_operational_page_in_snapshot(
                program,
                family.aggregates(),
                &parameters,
                None,
                &mut view,
            ),
            Err(QueryExecutionError::AggregateOverflow),
            "overflow must release no partial count/group/result"
        );
    }
}
