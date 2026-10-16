#![forbid(unsafe_code)]

//! Closed-program execution over one engine-owned authoritative read view.

mod event_candidate_prefix;
mod exact_result_set;
mod storage_executor;
#[doc(hidden)]
pub use storage_executor::authorize_provider_projected_candidates_admitted_v1;
mod tokenized_text;

pub use exact_result_set::*;
pub use storage_executor::*;
pub use tokenized_text::*;

#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;

use riffdb_contract_ir::KeyComponentCodecV1;
use riffdb_policy::{AuthorizedProjectedRowAdmissionV1, AuthorizedQueryRowPolicyContextV1};
use riffdb_projection::ResultSetEpochProofV1;
use riffdb_query_ir::{
    AccessDirection, CandidateBindingV1, CoveredResultLayoutV1, NamedTypeSchema,
    OperationalAggregateFunctionV1, OperationalAggregateTimeBucketV1, OperationalAggregateV1,
    PageBound, QueryAccessKind, QueryAccessProgramV1, QueryAccessStep, QueryLiteral,
    QueryPlanIdentity, QueryPredicate, QueryPredicateOperator, QueryPredicateValue,
    QueryRootOrderTermV1, QueryRowLimit, candidate_source_binding_name, evaluate_candidate_set_v1,
};
use riffdb_riffql_syntax::Cardinality;
use riffdb_types::{
    AggregateSemanticIdentityV1, ApplicationRoleHash, CanonicalList, CanonicalValue,
    CommitSequence, CompiledLongPatternV1, ContractLineage, Date, EmbeddingMetadata, EntityKey,
    EntityTypeId, FieldId, PartitionKey, PartitionKeyHash, ProjectionGeneration,
    ProjectionProviderDescriptorHash, QueryCostVectorV1, Timestamp, canonical_value_encoded_len,
    decode_canonical_value, encode_canonical_value, encode_canonical_value_into,
    hash_partition_key, hash_partition_set_observation,
};

#[cfg(feature = "test-fixtures")]
mod pre_inversion_fixture;
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub use pre_inversion_fixture::{encode_query_error_fixture_v1, encode_query_snapshot_fixture_v1};

/// Maximum checked submitted parameters.
pub const MAX_QUERY_PARAMETERS: usize = 1_024;
/// Maximum pay-once physical schedule produced for one index step.
///
/// The historical prefix-oriented name remains stable; WP-688 charges both
/// endpoints of every normalized interval against this same ceiling.
pub const MAX_OPERATIONAL_PREFIX_SCHEDULE_BYTES: usize = 4 * 1_024 * 1_024;
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

/// One compiler-resolved authoritative vector evidence target.
///
/// This is an engine boundary, never an application request: symbolic names,
/// partition values, limits, and cursors are resolved by the application
/// service before construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorInspectionTargetV1 {
    lineage: ContractLineage,
    partition: PartitionKey,
    entity: EntityTypeId,
    field: FieldId,
    after: Option<EntityKey>,
    limit: NonZeroU16,
}

impl VectorInspectionTargetV1 {
    /// Binds one exact evidence-prefix scan.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        partition: PartitionKey,
        entity: EntityTypeId,
        field: FieldId,
        after: Option<EntityKey>,
        limit: NonZeroU16,
    ) -> Self {
        Self {
            lineage,
            partition,
            entity,
            field,
            after,
            limit,
        }
    }

    /// Contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }
    /// Exact aggregate partition.
    #[must_use]
    pub const fn partition(&self) -> &PartitionKey {
        &self.partition
    }
    /// Entity type.
    #[must_use]
    pub const fn entity(&self) -> EntityTypeId {
        self.entity
    }
    /// Vector field.
    #[must_use]
    pub const fn field(&self) -> FieldId {
        self.field
    }
    /// Exclusive lower key.
    #[must_use]
    pub const fn after(&self) -> Option<&EntityKey> {
        self.after.as_ref()
    }
    /// Maximum physical candidates returned by this snapshot.
    #[must_use]
    pub const fn limit(&self) -> NonZeroU16 {
        self.limit
    }
}

/// One authoritative evidence row copied from a single inspection snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorInspectionCandidateV1 {
    entity_key: EntityKey,
    newest_source_write: Option<CommitSequence>,
    embedding_write: Option<(CommitSequence, EmbeddingMetadata)>,
}

impl VectorInspectionCandidateV1 {
    /// Retains storage-validated evidence.
    #[must_use]
    pub const fn new(
        entity_key: EntityKey,
        newest_source_write: Option<CommitSequence>,
        embedding_write: Option<(CommitSequence, EmbeddingMetadata)>,
    ) -> Self {
        Self {
            entity_key,
            newest_source_write,
            embedding_write,
        }
    }
    /// Entity identity.
    #[must_use]
    pub const fn entity_key(&self) -> &EntityKey {
        &self.entity_key
    }
    /// Newest declared source-field write.
    #[must_use]
    pub const fn newest_source_write(&self) -> Option<CommitSequence> {
        self.newest_source_write
    }
    /// Current embedding write and metadata.
    #[must_use]
    pub const fn embedding_write(&self) -> Option<&(CommitSequence, EmbeddingMetadata)> {
        self.embedding_write.as_ref()
    }
}

/// One policy-admitted authoritative vector inspection snapshot.
///
/// Counts, evidence rows, current-row policy observations, and the application
/// frontier are read from one engine-owned transaction. The optional proof is
/// bound to the complete returned candidate set and cannot be edited by the
/// service before filtering.
pub struct VectorInspectionSnapshotV1 {
    total_entities: u64,
    stale_entities: u64,
    model_counts: BTreeMap<EmbeddingMetadata, u64>,
    revision: Option<CommitSequence>,
    frontier: Option<CommitSequence>,
    candidates: Vec<VectorInspectionCandidateV1>,
    continuation: Option<EntityKey>,
    exact_end: bool,
    admission: Option<AuthorizedProjectedRowAdmissionV1>,
}

impl VectorInspectionSnapshotV1 {
    /// Constructs an owned result at the storage boundary.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        total_entities: u64,
        stale_entities: u64,
        model_counts: BTreeMap<EmbeddingMetadata, u64>,
        revision: Option<CommitSequence>,
        frontier: Option<CommitSequence>,
        candidates: Vec<VectorInspectionCandidateV1>,
        continuation: Option<EntityKey>,
        exact_end: bool,
        admission: Option<AuthorizedProjectedRowAdmissionV1>,
    ) -> Self {
        Self {
            total_entities,
            stale_entities,
            model_counts,
            revision,
            frontier,
            candidates,
            continuation,
            exact_end,
            admission,
        }
    }
    /// Whole-partition entity count.
    #[must_use]
    pub const fn total_entities(&self) -> u64 {
        self.total_entities
    }
    /// Whole-partition stale count.
    #[must_use]
    pub const fn stale_entities(&self) -> u64 {
        self.stale_entities
    }
    /// Counts by exact embedding metadata.
    pub fn model_counts(&self) -> impl Iterator<Item = (&EmbeddingMetadata, u64)> {
        self.model_counts
            .iter()
            .map(|(metadata, count)| (metadata, *count))
    }
    /// Authoritative observation revision.
    #[must_use]
    pub const fn revision(&self) -> Option<CommitSequence> {
        self.revision
    }
    /// Application frontier of the same authoritative read transaction.
    #[must_use]
    pub const fn frontier(&self) -> Option<CommitSequence> {
        self.frontier
    }
    /// Complete physical candidate page.
    #[must_use]
    pub fn candidates(&self) -> &[VectorInspectionCandidateV1] {
        &self.candidates
    }
    /// Exclusive lower continuation.
    #[must_use]
    pub const fn continuation(&self) -> Option<&EntityKey> {
        self.continuation.as_ref()
    }
    /// Exact prefix end marker.
    #[must_use]
    pub const fn exact_end(&self) -> bool {
        self.exact_end
    }
    /// Candidate-bound row-policy admission, when the target is protected.
    #[must_use]
    pub const fn admission(&self) -> Option<&AuthorizedProjectedRowAdmissionV1> {
        self.admission.as_ref()
    }
}

/// Checked name-addressed canonical parameters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryParameters(BTreeMap<String, CanonicalValue>);

impl QueryParameters {
    /// Validates bounded unique parameter names and canonicalizes set values once.
    pub fn checked(mut values: BTreeMap<String, CanonicalValue>) -> Option<Self> {
        if values.len() > MAX_QUERY_PARAMETERS || values.keys().any(|name| name.is_empty()) {
            return None;
        }
        for value in values.values_mut() {
            let CanonicalValue::List(items) = value else {
                continue;
            };
            let mut normalized = items
                .values()
                .iter()
                .map(|item| encode_canonical_value(item).map(|encoded| (encoded, item.clone())))
                .collect::<Result<Vec<_>, _>>()
                .ok()?;
            normalized.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            normalized.dedup_by(|left, right| left.0 == right.0);
            *value = CanonicalValue::list(normalized.into_iter().map(|(_, item)| item).collect())
                .ok()?;
        }
        Some(Self(values))
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
    let partition_values = match program.partition_route() {
        riffdb_query_ir::QueryPartitionRouteV1::Exact { .. } => {
            std::slice::from_ref(partition_value)
        }
        riffdb_query_ir::QueryPartitionRouteV1::FiniteSet { maximum, .. } => {
            let CanonicalValue::List(values) = partition_value else {
                return Err(QueryExecutionError::InvalidParameter {
                    parameter: program.partition_parameter().to_owned(),
                });
            };
            if values.values().len() > usize::from(*maximum) {
                return Err(QueryExecutionError::BoundExceeded);
            }
            values.values()
        }
    };
    let empty_bindings = BTreeMap::new();
    let mut dependencies = Vec::with_capacity(
        program
            .steps()
            .len()
            .checked_mul(partition_values.len())
            .ok_or(QueryExecutionError::BoundExceeded)?,
    );
    for step in program.steps() {
        let entity_key = match step.access() {
            QueryAccessKind::Point { key_fields }
                if step.predicates().iter().all(|predicate| {
                    !matches!(
                        predicate.value(),
                        QueryPredicateValue::BindingField { .. }
                            | QueryPredicateValue::BindingFieldSet { .. }
                            | QueryPredicateValue::CandidateBinding { .. }
                    )
                }) =>
            {
                let predicates = bind_predicates(step, parameters, &empty_bindings, &[])?;
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
            | QueryAccessKind::ExpansionIndex { .. }
            | QueryAccessKind::PartitionSetIndex { .. }
            | QueryAccessKind::LongPatternCandidate { .. }
            | QueryAccessKind::Nearest { .. }
            | QueryAccessKind::CandidateRootHydration { .. } => None,
        };
        for partition_value in partition_values {
            let partition = step
                .internal_partition_key_schema()
                .encode_partition(std::slice::from_ref(partition_value))
                .map_err(|_| QueryExecutionError::InvalidProgram)?;
            dependencies.push(BoundLiveQueryDependency {
                entity_type_id: step.internal_entity_id(),
                partition_hash: hash_partition_key(partition.as_bytes()),
                entity_key: entity_key.clone(),
            });
        }
    }
    Ok(dependencies)
}

pub(crate) type SharedRowFieldMap = Arc<BTreeMap<Arc<str>, CanonicalValue>>;

/// One owned, name-addressed authoritative entity row.
///
/// Field **names** and the entity name are shared `Arc<str>` handles (per-query
/// constants). Field **values** are owned exactly once at materialization and
/// then moved through projection, service assembly, and transport conversion.
#[derive(Clone)]
pub struct QueryRow {
    entity: Arc<str>,
    fields: SharedRowFieldMap,
    fields_encoded_bytes: Option<u64>,
    values: Option<Arc<[CanonicalValue]>>,
    projected_fields: Option<(SharedRowFieldMap, u64)>,
    selected_prevalidated: bool,
    nested: BTreeMap<Arc<str>, Vec<QueryRow>>,
    nested_aggregates: BTreeMap<Arc<str>, Vec<QueryAggregateRow>>,
}

impl PartialEq for QueryRow {
    fn eq(&self, other: &Self) -> bool {
        self.entity == other.entity
            && self.fields == other.fields
            && self.nested == other.nested
            && self.nested_aggregates == other.nested_aggregates
    }
}

impl Eq for QueryRow {}

/// Move-only components of one projected query row, including nested expansion rows.
pub type QueryRowParts = (
    Arc<str>,
    BTreeMap<Arc<str>, CanonicalValue>,
    BTreeMap<Arc<str>, Vec<QueryRow>>,
);
/// Move-only components of one projected query row, including nested expansions and aggregates.
pub type QueryRowAggregateParts = (
    Arc<str>,
    BTreeMap<Arc<str>, CanonicalValue>,
    BTreeMap<Arc<str>, Vec<QueryRow>>,
    BTreeMap<Arc<str>, Vec<QueryAggregateRow>>,
);
/// Shared-field components of one projected query row, avoiding cloning shared field maps.
pub type QueryRowSharedAggregateParts = (
    Arc<str>,
    SharedRowFieldMap,
    BTreeMap<Arc<str>, Vec<QueryRow>>,
    BTreeMap<Arc<str>, Vec<QueryAggregateRow>>,
);

impl std::fmt::Debug for QueryRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names = self
            .fields
            .keys()
            .map(|name| name.as_ref())
            .collect::<Vec<_>>();
        formatter
            .debug_struct("QueryRow")
            .field("entity", &self.entity.as_ref())
            .field("field_names", &names)
            .finish()
    }
}

impl QueryRow {
    /// Constructs one bounded row with unique field names.
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
        Self::from_shared_arc(entity, Arc::new(fields))
    }

    /// Constructs one bounded row from a shared field map.
    pub fn from_shared_arc(entity: Arc<str>, fields: SharedRowFieldMap) -> Option<Self> {
        (!entity.is_empty()
            && fields.len() <= MAX_QUERY_ROW_FIELDS
            && fields.keys().all(|name| !name.is_empty()))
        .then_some(Self {
            entity,
            fields,
            fields_encoded_bytes: None,
            values: None,
            projected_fields: None,
            selected_prevalidated: false,
            nested: BTreeMap::new(),
            nested_aggregates: BTreeMap::new(),
        })
    }

    pub(crate) fn from_materialized_plan(
        entity: Arc<str>,
        fields: SharedRowFieldMap,
        fields_encoded_bytes: Option<u64>,
        values: Option<Arc<[CanonicalValue]>>,
        projected_fields: Option<(SharedRowFieldMap, u64)>,
        selected_prevalidated: bool,
    ) -> Option<Self> {
        (!entity.is_empty() && fields.len() <= MAX_QUERY_ROW_FIELDS).then_some(Self {
            entity,
            fields,
            fields_encoded_bytes,
            values,
            projected_fields,
            selected_prevalidated,
            nested: BTreeMap::new(),
            nested_aggregates: BTreeMap::new(),
        })
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
    #[inline]
    pub(crate) fn field_at(&self, pos: Option<usize>, name: &str) -> Option<&CanonicalValue> {
        if let (Some(pos), Some(values)) = (pos, &self.values)
            && values.len() == self.fields.len()
        {
            return values.get(pos);
        }
        self.fields.get(name)
    }
    #[inline]
    pub(crate) fn field_pos(&self, name: &str) -> Option<usize> {
        if let Some(values) = &self.values
            && values.len() == self.fields.len()
        {
            self.fields.keys().position(|k| k.as_ref() == name)
        } else {
            None
        }
    }
    /// Iterates returned fields in canonical name order.
    pub fn fields(&self) -> impl ExactSizeIterator<Item = (&str, &CanonicalValue)> {
        self.fields
            .iter()
            .map(|(name, value)| (name.as_ref(), value))
    }
    /// Resolves one compiler-sealed nested expansion field.
    #[must_use]
    pub fn nested_rows(&self, name: &str) -> Option<&[QueryRow]> {
        self.nested.get(name).map(Vec::as_slice)
    }
    /// Iterates nested expansion fields in canonical name order.
    pub fn nested_fields(&self) -> impl ExactSizeIterator<Item = (&str, &[QueryRow])> {
        self.nested
            .iter()
            .map(|(name, rows)| (name.as_ref(), rows.as_slice()))
    }
    /// Resolves one compiler-sealed per-parent ungrouped aggregate record.
    #[must_use]
    pub fn nested_aggregate(&self, name: &str) -> Option<&QueryAggregateRow> {
        self.nested_aggregates
            .get(name)
            .and_then(|rows| rows.first())
    }
    /// Resolves compiler-sealed per-parent aggregate rows (single record or grouped rows).
    #[must_use]
    pub fn nested_aggregate_rows(&self, name: &str) -> Option<&[QueryAggregateRow]> {
        self.nested_aggregates.get(name).map(Vec::as_slice)
    }
    /// Consumes the row into entity name and fields.
    #[must_use]
    pub fn into_parts(self) -> QueryRowParts {
        (self.entity, Arc::unwrap_or_clone(self.fields), self.nested)
    }
    /// Consumes the row into entity name, fields, nested expansions, and nested aggregates.
    #[must_use]
    pub fn into_parts_with_aggregates(self) -> QueryRowAggregateParts {
        (
            self.entity,
            Arc::unwrap_or_clone(self.fields),
            self.nested,
            self.nested_aggregates,
        )
    }
    /// Consumes the row into entity name, shared fields handle, nested expansions, and nested aggregates.
    #[must_use]
    pub fn into_shared_parts_with_aggregates(self) -> QueryRowSharedAggregateParts {
        (
            self.entity,
            self.fields,
            self.nested,
            self.nested_aggregates,
        )
    }
    fn attach_nested(
        &mut self,
        name: Arc<str>,
        rows: Vec<QueryRow>,
    ) -> Result<(), QueryExecutionError> {
        (!name.is_empty()
            && rows.len() <= MAX_QUERY_SCANNED_ROWS as usize
            && self.nested.insert(name, rows).is_none())
        .then_some(())
        .ok_or(QueryExecutionError::InvalidProgram)
    }
    fn attach_nested_aggregate(
        &mut self,
        name: Arc<str>,
        rows: Vec<QueryAggregateRow>,
    ) -> Result<(), QueryExecutionError> {
        (!name.is_empty()
            && rows.len() <= MAX_QUERY_SCANNED_ROWS as usize
            && self.nested_aggregates.insert(name, rows).is_none())
        .then_some(())
        .ok_or(QueryExecutionError::InvalidProgram)
    }
    /// Validates that all selected fields exist on this row without allocating a new map.
    fn validate_selected(&self, selected: &[Arc<str>]) -> Result<(), QueryExecutionError> {
        if self.selected_prevalidated {
            return Ok(());
        }
        if self.entity.is_empty() || selected.len() > MAX_QUERY_ROW_FIELDS {
            return Err(QueryExecutionError::BoundExceeded);
        }
        for (idx, name) in selected.iter().enumerate() {
            if name.is_empty() {
                return Err(QueryExecutionError::BoundExceeded);
            }
            if !self.fields.contains_key(name.as_ref())
                || selected[..idx].iter().any(|prior| prior == name)
            {
                return Err(QueryExecutionError::MissingField {
                    entity: self.entity.to_string(),
                    field: name.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Mutates this row in place to retain only selected fields (no value or row Vec clones).
    fn project_in_place(&mut self, selected: &[Arc<str>]) -> Result<(), QueryExecutionError> {
        if self.selected_prevalidated {
            if let Some((projected, projected_bytes)) = self.projected_fields.take()
                && projected.len() == selected.len()
            {
                self.fields = projected;
                self.fields_encoded_bytes = Some(projected_bytes);
                self.values = None;
                return Ok(());
            }
            if self.fields.len() == selected.len() {
                return Ok(());
            }
        }
        if self.fields.len() == selected.len() {
            self.validate_selected(selected)?;
            return Ok(());
        }
        let mut fields = BTreeMap::new();
        let old_fields = std::mem::take(&mut self.fields);
        match Arc::try_unwrap(old_fields) {
            Ok(mut owned) => {
                for name in selected {
                    let value = owned.remove(name.as_ref()).ok_or_else(|| {
                        QueryExecutionError::MissingField {
                            entity: self.entity.to_string(),
                            field: name.to_string(),
                        }
                    })?;
                    fields.insert(Arc::clone(name), value);
                }
            }
            Err(shared) => {
                for (idx, name) in selected.iter().enumerate() {
                    if selected[..idx].iter().any(|prior| prior == name) {
                        return Err(QueryExecutionError::MissingField {
                            entity: self.entity.to_string(),
                            field: name.to_string(),
                        });
                    }
                    let value = shared.get(name.as_ref()).cloned().ok_or_else(|| {
                        QueryExecutionError::MissingField {
                            entity: self.entity.to_string(),
                            field: name.to_string(),
                        }
                    })?;
                    fields.insert(Arc::clone(name), value);
                }
            }
        }
        if fields.len() > MAX_QUERY_ROW_FIELDS {
            return Err(QueryExecutionError::BoundExceeded);
        }
        self.fields = Arc::new(fields);
        self.fields_encoded_bytes = None;
        self.projected_fields = None;
        self.values = None;
        Ok(())
    }

    /// Clones selected fields into a projected row (used only when the full row
    /// must also be retained for a later dependent step).
    fn project_clone(&self, selected: &[Arc<str>]) -> Result<Self, QueryExecutionError> {
        if self.selected_prevalidated {
            if let Some((projected, projected_bytes)) = &self.projected_fields
                && projected.len() == selected.len()
            {
                return Ok(Self {
                    entity: Arc::clone(&self.entity),
                    fields: Arc::clone(projected),
                    fields_encoded_bytes: Some(*projected_bytes),
                    values: None,
                    projected_fields: None,
                    selected_prevalidated: true,
                    nested: self.nested.clone(),
                    nested_aggregates: self.nested_aggregates.clone(),
                });
            }
            if self.fields.len() == selected.len() {
                return Ok(Self {
                    entity: Arc::clone(&self.entity),
                    fields: Arc::clone(&self.fields),
                    fields_encoded_bytes: self.fields_encoded_bytes,
                    values: None,
                    projected_fields: None,
                    selected_prevalidated: true,
                    nested: self.nested.clone(),
                    nested_aggregates: self.nested_aggregates.clone(),
                });
            }
        }
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
        let mut projected = Self::from_shared(Arc::clone(&self.entity), fields)
            .ok_or(QueryExecutionError::BoundExceeded)?;
        projected.nested = self.nested.clone();
        projected.nested_aggregates = self.nested_aggregates.clone();
        Ok(projected)
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
fn is_pipeline_clone_counting() -> bool {
    PIPELINE_CLONE_COUNTING.with(Cell::get)
}

#[cfg(not(test))]
const fn is_pipeline_clone_counting() -> bool {
    false
}

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

mod index_range;
pub use index_range::*;
/// Owned result of one bounded index access inside the current read view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoveredResultBatch {
    layout: CoveredResultLayoutV1,
    rows: Vec<Vec<CanonicalValue>>,
    epoch: u64,
    scanned_rows: u64,
    point_reads: u64,
    continuation: Option<Vec<u8>>,
}

impl CoveredResultBatch {
    /// Constructs one adapter-owned sealed positional result.
    #[doc(hidden)]
    pub fn checked(
        layout: CoveredResultLayoutV1,
        rows: Vec<Vec<CanonicalValue>>,
        epoch: u64,
        scanned_rows: u64,
        point_reads: u64,
        continuation: Option<Vec<u8>>,
    ) -> Option<Self> {
        let width = layout.fields().len();
        (width > 0
            && width <= MAX_QUERY_ROW_FIELDS
            && rows.len() as u64 <= scanned_rows
            && rows.iter().all(|row| row.len() == width)
            && continuation.as_ref().is_none_or(|value| {
                !value.is_empty() && value.len() <= MAX_QUERY_CONTINUATION_BYTES
            }))
        .then_some(Self {
            layout,
            rows,
            epoch,
            scanned_rows,
            point_reads,
            continuation,
        })
    }
}

/// Owned or snapshot-shared rows of one executed query step.
pub(crate) enum QueryStepRows {
    Owned(Vec<QueryRow>),
    Shared(Arc<[QueryRow]>),
}

impl std::ops::Deref for QueryStepRows {
    type Target = [QueryRow];

    #[inline]
    fn deref(&self) -> &[QueryRow] {
        match self {
            Self::Owned(rows) => rows,
            Self::Shared(rows) => rows,
        }
    }
}

impl<'a> IntoIterator for &'a QueryStepRows {
    type Item = &'a QueryRow;
    type IntoIter = std::slice::Iter<'a, QueryRow>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl QueryStepRows {
    #[inline]
    fn shared_arc(&self) -> Option<&Arc<[QueryRow]>> {
        match self {
            Self::Owned(_) => None,
            Self::Shared(shared) => Some(shared),
        }
    }

    #[inline]
    fn retain_owned(&mut self, f: impl FnMut(&QueryRow) -> bool) {
        let vec = match self {
            Self::Owned(rows) => rows,
            Self::Shared(shared) => {
                let owned = shared.as_ref().to_vec();
                *self = Self::Owned(owned);
                match self {
                    Self::Owned(rows) => rows,
                    Self::Shared(_) => unreachable!(),
                }
            }
        };
        vec.retain(f);
    }
}

/// Owned result of one bounded index access inside the current read view.
#[derive(Clone, Debug)]
pub struct QueryScanPage {
    rows: Vec<QueryRow>,
    pub(crate) shared_rows: Option<Arc<[QueryRow]>>,
    epoch: u64,
    scanned_rows: u64,
    point_reads: u64,
    continuation: Option<Vec<u8>>,
    pub(crate) predicates_verified: bool,
}

impl PartialEq for QueryScanPage {
    fn eq(&self, other: &Self) -> bool {
        self.rows_slice() == other.rows_slice()
            && self.epoch == other.epoch
            && self.scanned_rows == other.scanned_rows
            && self.point_reads == other.point_reads
            && self.continuation == other.continuation
    }
}

impl Eq for QueryScanPage {}

/// One complete, policy-filtered long-pattern candidate population observed at an exact epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LongPatternCandidateBatch {
    binding: String,
    rows: Vec<QueryRow>,
    descriptor: ProjectionProviderDescriptorHash,
    state_schema_hash: [u8; 32],
    history_incarnation: u64,
    generation: ProjectionGeneration,
    partition_generations: Option<riffdb_types::PartitionSetObservationHash>,
    floor: CommitSequence,
    ceiling: CommitSequence,
    scanned_rows: u64,
    verification_bytes: u64,
}

impl LongPatternCandidateBatch {
    /// Constructs one bounded provider observation. Semantic ceilings are rechecked by the executor.
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    pub fn checked(
        binding: String,
        rows: Vec<QueryRow>,
        descriptor: ProjectionProviderDescriptorHash,
        state_schema_hash: [u8; 32],
        history_incarnation: u64,
        generation: ProjectionGeneration,
        floor: CommitSequence,
        ceiling: CommitSequence,
        scanned_rows: u64,
        verification_bytes: u64,
    ) -> Option<Self> {
        (!binding.is_empty() && floor <= ceiling && rows.len() as u64 <= scanned_rows).then_some(
            Self {
                binding,
                rows,
                descriptor,
                state_schema_hash,
                history_incarnation,
                generation,
                partition_generations: None,
                floor,
                ceiling,
                scanned_rows,
                verification_bytes,
            },
        )
    }

    /// Query-local provider source binding.
    #[must_use]
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Exact provider observation represented by this batch.
    pub fn observation(
        &self,
    ) -> Result<riffdb_projection::ProviderEpochObservationV1, riffdb_projection::ResultSetEpochError>
    {
        riffdb_projection::ProviderEpochObservationV1::new(
            self.descriptor,
            self.state_schema_hash,
            self.history_incarnation,
            self.generation,
            self.floor,
            self.ceiling,
            riffdb_projection::ProviderLifecycleV1::Ready,
        )
        .map(|observation| observation.with_partition_generations(self.partition_generations))
    }

    /// Combines one provider invocation per selected partition into one bounded source batch.
    #[doc(hidden)]
    pub fn merge_partition_batches(
        mut batches: Vec<Self>,
        maximum_candidates: u64,
        maximum_scanned_rows: u64,
        maximum_verification_bytes: u64,
    ) -> Option<Self> {
        // Commit every actual generation before merging populations. Independent
        // partition rebuilds need not share a generation number; their exact
        // epoch intersection and immutable identity are still mandatory.
        let partition_generations = if batches.len() > 1 {
            const PREFIX: &[u8] = b"native-provider-partition-generations-v1";
            let count = u16::try_from(batches.len()).ok()?;
            let payload_length = u64::try_from(PREFIX.len())
                .ok()?
                .checked_add(2)?
                .checked_add(u64::from(count).checked_mul(41)?)?;
            let mut hasher = riffdb_types::ContentHasher::new(
                riffdb_types::HashDomain::PartitionSetObservation,
                payload_length,
            );
            hasher.update(PREFIX).ok()?;
            hasher.update(&count.to_be_bytes()).ok()?;
            for batch in &batches {
                hasher.update(&batch.generation.get().to_be_bytes()).ok()?;
                hasher
                    .update(&[u8::from(batch.partition_generations.is_some())])
                    .ok()?;
                hasher
                    .update(
                        batch
                            .partition_generations
                            .as_ref()
                            .map_or(&[0; 32], |digest| digest.as_bytes().as_slice()),
                    )
                    .ok()?;
            }
            Some(riffdb_types::PartitionSetObservationHash::from_bytes(
                *hasher.finish().ok()?.as_bytes(),
            ))
        } else {
            batches.first()?.partition_generations
        };
        let mut merged = batches.pop()?;
        merged.partition_generations = partition_generations;
        for batch in batches {
            if batch.binding != merged.binding
                || batch.descriptor != merged.descriptor
                || batch.state_schema_hash != merged.state_schema_hash
                || batch.history_incarnation != merged.history_incarnation
            {
                return None;
            }
            merged.floor = merged.floor.max(batch.floor);
            merged.ceiling = merged.ceiling.min(batch.ceiling);
            merged.scanned_rows = merged.scanned_rows.checked_add(batch.scanned_rows)?;
            merged.verification_bytes = merged
                .verification_bytes
                .checked_add(batch.verification_bytes)?;
            merged.rows.extend(batch.rows);
            if merged.floor > merged.ceiling
                || merged.rows.len() as u64 > maximum_candidates
                || merged.scanned_rows > maximum_scanned_rows
                || merged.verification_bytes > maximum_verification_bytes
            {
                return None;
            }
        }
        (merged.rows.len() as u64 <= maximum_candidates
            && merged.scanned_rows <= maximum_scanned_rows
            && merged.verification_bytes <= maximum_verification_bytes)
            .then_some(merged)
    }
}

impl QueryScanPage {
    #[inline]
    pub(crate) fn rows_slice(&self) -> &[QueryRow] {
        match &self.shared_rows {
            Some(shared) => shared,
            None => &self.rows,
        }
    }

    #[inline]
    pub(crate) fn rows_len(&self) -> usize {
        self.rows_slice().len()
    }

    #[inline]
    pub(crate) fn into_step_rows(self) -> QueryStepRows {
        match self.shared_rows {
            Some(shared) => QueryStepRows::Shared(shared),
            None => QueryStepRows::Owned(self.rows),
        }
    }

    #[inline]
    pub(crate) fn into_rows_vec(self) -> Vec<QueryRow> {
        match self.shared_rows {
            Some(shared) => shared.as_ref().to_vec(),
            None => self.rows,
        }
    }

    #[inline]
    pub(crate) fn shared_verified(
        shared_rows: Arc<[QueryRow]>,
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
                rows: Vec::new(),
                shared_rows: Some(shared_rows),
                epoch,
                scanned_rows,
                point_reads,
                continuation,
                predicates_verified: true,
            })
    }

    /// Constructs an exact-end page.
    #[must_use]
    pub fn exact_end(rows: Vec<QueryRow>, epoch: u64) -> Self {
        let work = rows.len() as u64;
        Self {
            scanned_rows: work,
            point_reads: work,
            rows,
            shared_rows: None,
            epoch,
            continuation: None,
            predicates_verified: false,
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
            shared_rows: None,
            epoch,
            scanned_rows,
            point_reads,
            continuation: None,
            predicates_verified: false,
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
                shared_rows: None,
                epoch,
                scanned_rows,
                point_reads,
                continuation: Some(continuation),
                predicates_verified: false,
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
                shared_rows: None,
                epoch,
                scanned_rows,
                point_reads,
                continuation,
                predicates_verified: false,
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
        after_inclusive: bool,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error>;

    /// Executes one compiler-sealed covered positional index step.
    ///
    /// `None` means this adapter cannot prove the exact cover. The executor
    /// treats that as an integrity failure for a marked plan; it never falls
    /// back to entity hydration.
    fn scan_covered(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _limit: u64,
        _after: Option<&[u8]>,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<CoveredResultBatch>, Self::Error> {
        Ok(None)
    }

    /// Optional fast path for covered scans that materialize [`QueryRow`]s
    /// directly from a snapshot-cached projection map.
    fn scan_covered_rows(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _limit: u64,
        _after: Option<&[u8]>,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<Option<QueryScanPage>>, Self::Error> {
        Ok(None)
    }

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

    /// Exact lower binding retained by the opaque service cursor.
    #[doc(hidden)]
    #[must_use]
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Compiler-sealed ordinary and provider epoch identities.
    #[doc(hidden)]
    #[must_use]
    pub const fn index_epochs(&self) -> &BTreeMap<String, u64> {
        &self.index_epochs
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

    /// Executes a provider-participating page against one exact authoritative
    /// snapshot and one service-negotiated provider epoch proof.
    fn execute_provider_query_page(
        &self,
        _program: &QueryAccessProgramV1,
        _parameters: &QueryParameters,
        _prior: Option<&QueryContinuation>,
        _policy_shape: ApplicationRoleHash,
        _proof: &ResultSetEpochProofV1,
        _batches: &[LongPatternCandidateBatch],
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        Err(QueryExecutionError::InvalidProgram)
    }

    /// Policy-protected provider-participating execution. The default denies.
    #[allow(clippy::too_many_arguments)]
    fn execute_policy_provider_query_page(
        &self,
        _program: &QueryAccessProgramV1,
        _parameters: &QueryParameters,
        _prior: Option<&QueryContinuation>,
        _policy: &AuthorizedQueryRowPolicyContextV1,
        _policy_shape: ApplicationRoleHash,
        _proof: &ResultSetEpochProofV1,
        _batches: &[LongPatternCandidateBatch],
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

    /// Evaluates all candidate entity keys against current authoritative rows
    /// and relationship evidence in one read snapshot.
    ///
    /// The default denies. Concrete first-party adapters return a move-only,
    /// entity/candidate-bound proof; they never return a caller-editable allow
    /// list and must reject candidate sets above the fixed policy ceiling.
    fn authorize_projected_candidates(
        &self,
        _entity: EntityTypeId,
        _candidates: &[EntityKey],
        _policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<AuthorizedProjectedRowAdmissionV1, QueryExecutionError> {
        Err(QueryExecutionError::InvalidProgram)
    }

    /// Reads vector observation, evidence, and current-row policy in one snapshot.
    ///
    /// The default denies so a policy-neutral evidence repository cannot be
    /// composed into an application disclosure by accident.
    fn inspect_vector_evidence(
        &self,
        _target: &VectorInspectionTargetV1,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<VectorInspectionSnapshotV1, QueryExecutionError> {
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
    /// Exact mean partial state returned without division or rounding.
    ExactMean {
        /// Checked signed total coefficient at the declared scale.
        coefficient: i128,
        /// Declared decimal scale.
        scale: u8,
        /// Exact contributing row count.
        count: u64,
    },
}

/// Pre-split aggregate cells for zero-allocation service conversion.
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct QueryAggregateSplitCells {
    /// Canonical cells in canonical field-name order.
    pub canonical_fields: Arc<BTreeMap<Arc<str>, CanonicalValue>>,
    /// Exact decimal cells (`(coefficient, scale)`).
    pub exact_decimals: BTreeMap<Arc<str>, (i128, u8)>,
    /// Exact mean cells (`(coefficient, scale, count)`).
    pub exact_means: BTreeMap<Arc<str>, (i128, u8, u64)>,
}

/// One name-addressed aggregate result record.
#[derive(Clone, Debug)]
pub struct QueryAggregateRow {
    entity: Arc<str>,
    fields: Arc<BTreeMap<Arc<str>, QueryAggregateCell>>,
    split_cells: Option<Arc<QueryAggregateSplitCells>>,
    fields_encoded_bytes: Option<u64>,
}

impl PartialEq for QueryAggregateRow {
    fn eq(&self, other: &Self) -> bool {
        self.entity == other.entity && self.fields == other.fields
    }
}

impl Eq for QueryAggregateRow {}

/// One whole compiler-sealed named result retained positionally until the
/// service selects legacy or compact public carriage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoveredQueryResultV1 {
    result_name: String,
    entity: Arc<str>,
    fields: Vec<Arc<str>>,
    rows: Vec<Vec<CanonicalValue>>,
}

/// Owned compiler-sealed compact result components in protocol order.
pub type CoveredQueryResultPartsV1 = (String, Arc<str>, Vec<Arc<str>>, Vec<Vec<CanonicalValue>>);

impl CoveredQueryResultV1 {
    /// Exact top-level result field name.
    #[must_use]
    pub fn result_name(&self) -> &str {
        &self.result_name
    }

    /// Exact contract entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Ordered selected field names carried once.
    #[must_use]
    pub fn fields(&self) -> impl ExactSizeIterator<Item = &str> {
        self.fields.iter().map(Arc::as_ref)
    }

    /// Bounded positional canonical rows.
    #[must_use]
    pub fn rows(&self) -> &[Vec<CanonicalValue>] {
        &self.rows
    }

    /// Consumes the sealed result without constructing name-addressed rows.
    #[must_use]
    pub fn into_parts(self) -> CoveredQueryResultPartsV1 {
        (self.result_name, self.entity, self.fields, self.rows)
    }

    /// Converts to the legacy named-map value only for a legacy public caller.
    #[doc(hidden)]
    pub fn into_named_value(self) -> (String, QueryResultValue) {
        let rows = self
            .rows
            .into_iter()
            .map(|values| QueryRow {
                entity: Arc::clone(&self.entity),
                fields: Arc::new(self.fields.iter().cloned().zip(values).collect()),
                fields_encoded_bytes: None,
                values: None,
                projected_fields: None,
                selected_prevalidated: false,
                nested: BTreeMap::new(),
                nested_aggregates: BTreeMap::new(),
            })
            .collect::<Vec<_>>();
        (self.result_name, QueryResultValue::Many(rows))
    }
}

impl QueryAggregateRow {
    fn from_fields(entity: Arc<str>, fields: BTreeMap<Arc<str>, QueryAggregateCell>) -> Self {
        let mut bytes = 0_u64;
        let mut bytes_ok = true;
        let mut canonical_fields = BTreeMap::new();
        let mut exact_decimals = BTreeMap::new();
        let mut exact_means = BTreeMap::new();
        for (field, value) in &fields {
            if bytes_ok {
                let encoded_len = match value {
                    QueryAggregateCell::Canonical(v) => canonical_value_encoded_len(v).ok(),
                    QueryAggregateCell::ExactDecimal { .. } => Some(18),
                    QueryAggregateCell::ExactMean { .. } => Some(80),
                };
                if let Some(len) = encoded_len
                    && let Some(next) = bytes
                        .checked_add(field.len() as u64)
                        .and_then(|v| v.checked_add(len as u64))
                        .and_then(|v| v.checked_add(160))
                {
                    bytes = next;
                } else {
                    bytes_ok = false;
                }
            }
            match value {
                QueryAggregateCell::Canonical(v) => {
                    canonical_fields.insert(Arc::clone(field), v.clone());
                }
                &QueryAggregateCell::ExactDecimal { coefficient, scale } => {
                    exact_decimals.insert(Arc::clone(field), (coefficient, scale));
                }
                &QueryAggregateCell::ExactMean {
                    coefficient,
                    scale,
                    count,
                } => {
                    exact_means.insert(Arc::clone(field), (coefficient, scale, count));
                }
            }
        }
        Self {
            entity,
            fields: Arc::new(fields),
            split_cells: Some(Arc::new(QueryAggregateSplitCells {
                canonical_fields: Arc::new(canonical_fields),
                exact_decimals,
                exact_means,
            })),
            fields_encoded_bytes: bytes_ok.then_some(bytes),
        }
    }

    /// Query-local aggregate symbol.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Canonically ordered returned cells. A missing field represents the
    /// outer absence of a nested optional min/max result.
    #[must_use]
    pub fn fields(&self) -> &BTreeMap<Arc<str>, QueryAggregateCell> {
        &self.fields
    }

    /// Consumes the record without cloning names or cells.
    #[must_use]
    pub fn into_parts(self) -> (Arc<str>, BTreeMap<Arc<str>, QueryAggregateCell>) {
        (self.entity, Arc::unwrap_or_clone(self.fields))
    }

    /// Consumes the record returning shared pre-split cell maps when available.
    #[doc(hidden)]
    #[must_use]
    pub fn into_shared_parts(self) -> QueryAggregateSharedParts {
        (self.entity, self.fields, self.split_cells)
    }
}

/// Shared components of a [`QueryAggregateRow`].
#[doc(hidden)]
pub type QueryAggregateSharedParts = (
    Arc<str>,
    Arc<BTreeMap<Arc<str>, QueryAggregateCell>>,
    Option<Arc<QueryAggregateSplitCells>>,
);

/// Complete owned snapshot result; no engine handle or iterator escapes.
#[derive(Clone, Debug)]
pub struct QueryOwnedSnapshot {
    application_head: u64,
    index_epochs: BTreeMap<String, u64>,
    outcome: String,
    fields: Arc<BTreeMap<String, QueryResultValue>>,
    covered_result: Option<CoveredQueryResultV1>,
    continuation_binding: Option<String>,
    continuation: Option<Vec<u8>>,
    cache_pinned: bool,
}

impl PartialEq for QueryOwnedSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.application_head == other.application_head
            && self.index_epochs == other.index_epochs
            && self.outcome == other.outcome
            && self.fields == other.fields
            && self.covered_result == other.covered_result
            && self.continuation_binding == other.continuation_binding
            && self.continuation == other.continuation
    }
}

impl Eq for QueryOwnedSnapshot {}

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
    pub fn fields(&self) -> &BTreeMap<String, QueryResultValue> {
        &self.fields
    }

    /// Shared handle to name-addressed result fields.
    #[doc(hidden)]
    #[must_use]
    pub const fn shared_fields(&self) -> &Arc<BTreeMap<String, QueryResultValue>> {
        &self.fields
    }

    /// Whether this snapshot's `fields` allocation is pinned in the local snapshot cache.
    #[doc(hidden)]
    #[must_use]
    pub const fn is_cache_pinned(&self) -> bool {
        self.cache_pinned
    }

    /// Compiler-sealed positional result, when retained without generic maps.
    #[must_use]
    pub const fn covered_result(&self) -> Option<&CoveredQueryResultV1> {
        self.covered_result.as_ref()
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
    pub fn into_fields(mut self) -> BTreeMap<String, QueryResultValue> {
        let mut fields = Arc::unwrap_or_clone(self.fields);
        if let Some(covered) = self.covered_result.take() {
            let (name, value) = covered.into_named_value();
            fields.insert(name, value);
        }
        fields
    }

    /// Consumes the snapshot while retaining a positional result without map conversion.
    #[doc(hidden)]
    #[must_use]
    pub fn into_result_parts(
        self,
    ) -> (
        BTreeMap<String, QueryResultValue>,
        Option<CoveredQueryResultV1>,
    ) {
        (Arc::unwrap_or_clone(self.fields), self.covered_result)
    }

    /// Consumes the snapshot while retaining the shared fields handle.
    #[doc(hidden)]
    #[must_use]
    pub fn into_shared_result_parts(
        self,
    ) -> (
        Arc<BTreeMap<String, QueryResultValue>>,
        Option<CoveredQueryResultV1>,
    ) {
        (self.fields, self.covered_result)
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
    execute_operational_page_in_snapshot_with_policy(
        program,
        &[],
        parameters,
        prior,
        view,
        None,
        None,
    )
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
        None,
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
        program, aggregates, parameters, prior, view, None, None,
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
        None,
    )
}

/// Executes one provider-participating page in the same authoritative view as
/// root hydration and final ordering.
#[allow(clippy::too_many_arguments)]
pub fn execute_provider_page_in_snapshot<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    view: &mut V,
    policy_shape: ApplicationRoleHash,
    proof: &ResultSetEpochProofV1,
    batches: &[LongPatternCandidateBatch],
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    let mut providers = ProviderExecutionContext::new(program, policy_shape, proof, batches, view)?;
    execute_operational_page_in_snapshot_with_policy(
        program,
        &[],
        parameters,
        prior,
        view,
        None,
        Some(&mut providers),
    )
}

/// Policy-protected provider-participating execution in one authoritative view.
#[allow(clippy::too_many_arguments)]
pub fn execute_policy_provider_page_in_snapshot<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    view: &mut V,
    policy: &AuthorizedQueryRowPolicyContextV1,
    policy_shape: ApplicationRoleHash,
    proof: &ResultSetEpochProofV1,
    batches: &[LongPatternCandidateBatch],
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    let mut providers = ProviderExecutionContext::new(program, policy_shape, proof, batches, view)?;
    execute_operational_page_in_snapshot_with_policy(
        program,
        &[],
        parameters,
        prior,
        view,
        Some(policy),
        Some(&mut providers),
    )
}

struct ProviderExecutionContext<'a> {
    proof: &'a ResultSetEpochProofV1,
    batches: &'a [LongPatternCandidateBatch],
    used: BTreeSet<String>,
}

impl<'a> ProviderExecutionContext<'a> {
    fn new<V: QueryReadView>(
        program: &QueryAccessProgramV1,
        policy_shape: ApplicationRoleHash,
        proof: &'a ResultSetEpochProofV1,
        batches: &'a [LongPatternCandidateBatch],
        view: &V,
    ) -> Result<Self, QueryExecutionError> {
        let provider_steps = program
            .steps()
            .iter()
            .filter(|step| matches!(step.access(), QueryAccessKind::LongPatternCandidate { .. }))
            .count();
        if provider_steps == 0
            || batches.len() != provider_steps
            || proof.plan_identity() != program.identity().hash()
            || proof.policy_shape_identity() != policy_shape
            || proof.selected_epoch().get() != view.application_head()
        {
            return Err(QueryExecutionError::BackendUnavailable);
        }
        Ok(Self {
            proof,
            batches,
            used: BTreeSet::new(),
        })
    }

    fn batch(
        &mut self,
        step: &QueryAccessStep,
        descriptor: ProjectionProviderDescriptorHash,
    ) -> Result<LongPatternCandidateBatch, QueryExecutionError> {
        if !self.used.insert(step.binding().to_owned()) {
            return Err(QueryExecutionError::InvalidProgram);
        }
        let batch = self
            .batches
            .iter()
            .find(|batch| batch.binding == step.binding())
            .ok_or(QueryExecutionError::BackendIntegrity)?;
        let participant = self
            .proof
            .participants()
            .find(|participant| participant.descriptor() == descriptor)
            .ok_or(QueryExecutionError::BackendIntegrity)?;
        if batch.descriptor != descriptor
            || batch.observation().ok().as_ref() != Some(participant)
            || self.proof.selected_epoch() < batch.floor
            || self.proof.selected_epoch() > batch.ceiling
        {
            return Err(QueryExecutionError::BackendIntegrity);
        }
        Ok(batch.clone())
    }

    fn is_complete(&self) -> bool {
        self.used.len() == self.batches.len()
    }
}

fn execute_operational_page_in_snapshot_with_policy<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    aggregates: &[OperationalAggregateV1],
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    view: &mut V,
    policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    mut providers: Option<&mut ProviderExecutionContext<'_>>,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    if !std::ptr::eq(aggregates, program.surface().aggregates())
        && aggregates != program.surface().aggregates()
    {
        return Err(QueryExecutionError::InvalidProgram);
    }
    if program.steps().len() == 1
        && aggregates.is_empty()
        && program.steps()[0].covered_result_layout().is_some()
        && program.steps()[0].cardinality() == Cardinality::Many
        && program.steps()[0].result_names().len() == 1
        && program.steps()[0].dependencies().is_empty()
    {
        return execute_covered_page_in_snapshot(program, parameters, prior, view, policy);
    }
    let prepared_plan = prepared_executor_program_plan(program);
    let mut fuel = QueryExecutionFuel::from_cost(program.cost());
    let mut bindings = BTreeMap::<String, QueryStepRows>::new();
    let mut result_fields = BTreeMap::new();
    let mut index_epochs = BTreeMap::new();
    let mut continuation_binding = None;
    let mut continuation = None;
    let mut provider_history_incarnation = None;
    // Hoist once: outcome name is a program constant, not per-step work.
    let default_outcome = program
        .surface()
        .schemas()
        .results()
        .first()
        .map(|branch| branch.name())
        .unwrap_or("Result");

    let mut whole_snapshot_probe: Option<WholeSnapshotCacheProbe> = None;

    for (step_index, step) in program.steps().iter().enumerate() {
        fuel.step()?;
        let limit = resolve_row_limit(step, parameters)?;
        let after = prior
            .filter(|cursor| cursor.binding == step.binding())
            .map(|cursor| cursor.lower.as_slice());
        let prepared_step = prepared_plan.steps.get(step_index);
        let fallback_selected_names;
        let selected_names: &[Arc<str>] = if let Some(prepared_step) = prepared_step {
            &prepared_step.selected_names
        } else {
            fallback_selected_names = step
                .selected_fields()
                .iter()
                .map(|name| Arc::<str>::from(name.as_str()))
                .collect::<Vec<_>>();
            &fallback_selected_names
        };
        if matches!(step.access(), QueryAccessKind::ExpansionIndex { .. }) {
            execute_expansion_step(
                program,
                &prepared_plan,
                prepared_step,
                aggregates,
                step,
                parameters,
                after,
                limit,
                view,
                policy,
                &mut fuel,
                &bindings,
                &mut result_fields,
                &mut index_epochs,
                selected_names,
            )?;
            continue;
        }
        let (mut rows, scalar_predicates, predicates_must_match) = match step.access() {
            riffdb_query_ir::QueryAccessKind::ExpansionIndex { .. } => {
                return Err(QueryExecutionError::InvalidProgram);
            }
            riffdb_query_ir::QueryAccessKind::Point { .. } => {
                let predicates =
                    bind_predicates(step, parameters, &bindings, program.surface().candidates())?;
                fuel.points(1)?;
                (
                    QueryStepRows::Owned(
                        view.point(step, &predicates, policy)
                            .map_err(|error| map_view_error(view, &error))?
                            .into_iter()
                            .collect::<Vec<_>>(),
                    ),
                    Some(predicates),
                    false,
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
                    if let Some(outcome) = step.absence_outcome() {
                        let outcome = outcome.to_owned();
                        return finish_snapshot(
                            &mut fuel,
                            QueryOwnedSnapshot {
                                application_head: view.application_head(),
                                index_epochs,
                                outcome,
                                fields: Arc::new(BTreeMap::new()),
                                covered_result: None,
                                continuation_binding: None,
                                continuation: None,
                                cache_pinned: false,
                            },
                        );
                    }
                    if !step.distinct_dependent_keys() {
                        return Err(QueryExecutionError::InvalidProgram);
                    }
                }
                let pred_positions = prepared_step.and_then(|s| s.predicate_positions.as_deref());
                let rows = observations
                    .into_iter()
                    .zip(&predicates)
                    .filter_map(|(row, predicates)| row.map(|row| (row, predicates)))
                    .map(|(row, predicates)| {
                        if row.entity() != step.entity()
                            || !predicates_match_at_positions(&row, predicates, pred_positions)?
                        {
                            return Err(QueryExecutionError::InvalidProgram);
                        }
                        Ok(row)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (QueryStepRows::Owned(rows), None, false)
            }
            riffdb_query_ir::QueryAccessKind::Index { index, .. }
                if program.partition_route().is_finite_set()
                    && matches!(step.row_limit(), QueryRowLimit::CandidateComplete { .. }) =>
            {
                if after.is_some() || step.covered_result_layout().is_some() {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                let predicates =
                    bind_predicates(step, parameters, &bindings, program.surface().candidates())?;
                let routes = partition_set_routes(program, parameters)?;
                let pred_positions = prepared_step.and_then(|s| s.predicate_positions.as_deref());
                let mut rows = Vec::new();
                let mut observations = Vec::new();
                for route in routes {
                    let local = partition_local_predicates(
                        step,
                        &predicates,
                        Some(program.partition_parameter()),
                        route,
                    )?;
                    let page = view
                        .scan(step, &local, limit, None, false, policy)
                        .map_err(|error| map_view_error(view, &error))?;
                    if page.scanned_rows > MAX_QUERY_SCANNED_ROWS
                        || page.rows_len() as u64 > limit
                        || page.point_reads != page.rows_len() as u64
                        || page.rows_len() as u64 > page.scanned_rows
                        || page.continuation.is_some()
                        || page.rows_slice().iter().any(|row| {
                            row.entity() != step.entity()
                                || predicates_match_at_positions(row, &local, pred_positions)
                                    != Ok(true)
                        })
                    {
                        return Err(QueryExecutionError::BoundExceeded);
                    }
                    fuel.scans(page.scanned_rows)?;
                    fuel.points(page.point_reads)?;
                    append_partition_observation(&mut observations, route, page.epoch)?;
                    rows.extend(page.into_rows_vec());
                    if rows.len() as u64 > limit {
                        return Err(QueryExecutionError::BoundExceeded);
                    }
                }
                insert_partition_set_observation(
                    &mut index_epochs,
                    step.entity(),
                    index,
                    &observations,
                )?;
                (QueryStepRows::Owned(rows), Some(predicates), true)
            }
            riffdb_query_ir::QueryAccessKind::Index { index, .. } => {
                let predicates =
                    bind_predicates(step, parameters, &bindings, program.surface().candidates())?;
                let (mut page, covered_scan) =
                    if let Some(expected_layout) = step.covered_result_layout() {
                        if let Some(covered_page) = view
                            .scan_covered_rows(step, &predicates, limit, after, policy)
                            .map_err(|error| map_view_error(view, &error))?
                        {
                            let page = covered_page.ok_or(QueryExecutionError::BackendIntegrity)?;
                            (page, true)
                        } else {
                            let batch = view
                                .scan_covered(step, &predicates, limit, after, policy)
                                .map_err(|error| map_view_error(view, &error))?
                                .ok_or(QueryExecutionError::BackendIntegrity)?;
                            if &batch.layout != expected_layout
                                || batch.rows.len() as u64 > limit
                                || batch.scanned_rows > MAX_QUERY_SCANNED_ROWS
                                || batch.point_reads != 0
                                || batch
                                    .continuation
                                    .as_ref()
                                    .is_some_and(|value| value.len() > MAX_QUERY_CONTINUATION_BYTES)
                            {
                                return Err(QueryExecutionError::InvalidProgram);
                            }
                            let entity = Arc::<str>::from(expected_layout.entity());
                            let field_names: Vec<Arc<str>> = expected_layout
                                .fields()
                                .iter()
                                .map(|field| Arc::<str>::from(field.name()))
                                .collect();
                            let rows = batch
                                .rows
                                .into_iter()
                                .map(|values| {
                                    if values.len() != field_names.len() {
                                        return Err(QueryExecutionError::InvalidProgram);
                                    }
                                    let fields = field_names
                                        .iter()
                                        .zip(values)
                                        .map(|(name, value)| (Arc::clone(name), value))
                                        .collect::<BTreeMap<_, _>>();
                                    QueryRow::from_shared(Arc::clone(&entity), fields)
                                        .ok_or(QueryExecutionError::InvalidProgram)
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                            (
                                QueryScanPage {
                                    rows,
                                    shared_rows: None,
                                    epoch: batch.epoch,
                                    scanned_rows: batch.scanned_rows,
                                    point_reads: batch.point_reads,
                                    continuation: batch.continuation,
                                    predicates_verified: true,
                                },
                                true,
                            )
                        }
                    } else {
                        (
                            view.scan(step, &predicates, limit, after, false, policy)
                                .map_err(|error| map_view_error(view, &error))?,
                            false,
                        )
                    };
                let complete_candidate_source =
                    matches!(step.row_limit(), QueryRowLimit::CandidateComplete { .. });
                let page_rows_len = page.rows_len();
                if page.scanned_rows > MAX_QUERY_SCANNED_ROWS
                    || page_rows_len as u64 > limit
                    || page.point_reads
                        != if covered_scan {
                            0
                        } else {
                            page_rows_len as u64
                        }
                    || page_rows_len as u64 > page.scanned_rows
                    || page
                        .continuation
                        .as_ref()
                        .is_some_and(|value| value.len() > MAX_QUERY_CONTINUATION_BYTES)
                {
                    return Err(QueryExecutionError::BoundExceeded);
                }
                if complete_candidate_source && page.continuation.is_some() {
                    return Err(QueryExecutionError::BoundExceeded);
                }
                if step.cardinality() != Cardinality::Many && page.continuation.is_some() {
                    return Err(QueryExecutionError::UnexpectedCardinality {
                        binding: step.binding().to_owned(),
                    });
                }
                fuel.scans(page.scanned_rows)?;
                fuel.points(page.point_reads)?;
                let epoch_key =
                    if let Some(key) = prepared_step.and_then(|s| s.index_epoch_key.as_ref()) {
                        key.clone()
                    } else {
                        let mut epoch_key =
                            String::with_capacity(step.entity().len() + 1 + index.len());
                        epoch_key.push_str(step.entity());
                        epoch_key.push('.');
                        epoch_key.push_str(index);
                        epoch_key
                    };
                index_epochs.insert(epoch_key, page.epoch);
                if page.continuation.is_some() && !complete_candidate_source {
                    if continuation.is_some() {
                        return Err(QueryExecutionError::InvalidProgram);
                    }
                    continuation_binding = Some(step.binding().to_owned());
                    continuation = page.continuation.take();
                }
                let predicates_verified = page.predicates_verified;
                (
                    page.into_step_rows(),
                    (!covered_scan && !predicates_verified).then_some(predicates),
                    true,
                )
            }
            riffdb_query_ir::QueryAccessKind::PartitionSetIndex {
                index,
                fields,
                direction,
                order,
                scan_ceiling,
                key_fields,
            } => {
                if step.covered_result_layout().is_some() || fields.len() < 2 {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                let predicates =
                    bind_predicates(step, parameters, &bindings, program.surface().candidates())?;
                let (route_field, route_parameter, routes) =
                    partition_set_step_routes(program, step, fields, parameters, &predicates)?;
                let prior_marker = after
                    .map(|lower| {
                        decode_partition_set_marker(lower, order.len() + 1 + key_fields.len())
                    })
                    .transpose()?;
                let merge_limit =
                    usize::try_from(limit).map_err(|_| QueryExecutionError::BoundExceeded)?;
                let uniform_order = order.iter().all(|term| term.direction() == *direction);
                let (merged, observations, has_more) = if uniform_order {
                    execute_partition_set_uniform_merge(
                        view,
                        &mut fuel,
                        step,
                        prepared_step,
                        &predicates,
                        route_field,
                        route_parameter,
                        routes,
                        fields,
                        order,
                        key_fields,
                        *scan_ceiling,
                        limit,
                        prior_marker.as_deref(),
                        policy,
                    )?
                } else {
                    let mut streams = Vec::with_capacity(routes.len());
                    let mut has_more = false;
                    let mut observations = Vec::new();
                    for route in routes {
                        let local_predicates =
                            partition_local_predicates(step, &predicates, route_parameter, route)?;
                        let (stream, epoch, stream_has_more) = execute_partition_set_stream(
                            view,
                            &mut fuel,
                            step,
                            prepared_step,
                            &local_predicates,
                            route_field,
                            route,
                            fields,
                            order,
                            key_fields,
                            *direction,
                            *scan_ceiling,
                            limit,
                            prior_marker.as_deref(),
                            policy,
                        )?;
                        append_partition_observation(&mut observations, route, epoch)?;
                        has_more |= stream_has_more;
                        streams.push(stream);
                    }
                    let (merged, discarded) =
                        merge_partition_runs(streams, merge_limit, route_field, order, key_fields)?;
                    has_more |= discarded;
                    (merged, observations, has_more)
                };
                insert_partition_set_observation(
                    &mut index_epochs,
                    step.entity(),
                    index,
                    &observations,
                )?;
                if has_more {
                    let lower = merged
                        .last()
                        .ok_or(QueryExecutionError::InvalidProgram)
                        .and_then(|row| {
                            partition_set_marker(row, route_field, order, key_fields)
                        })?;
                    if lower.len() > MAX_QUERY_CONTINUATION_BYTES || continuation.is_some() {
                        return Err(QueryExecutionError::BoundExceeded);
                    }
                    continuation_binding = Some(step.binding().to_owned());
                    continuation = Some(lower);
                }
                (QueryStepRows::Owned(merged), Some(predicates), true)
            }
            riffdb_query_ir::QueryAccessKind::LongPatternCandidate { provider, pattern } => {
                if after.is_some() {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                let route_count = if program.partition_route().is_finite_set() {
                    u64::try_from(partition_set_routes(program, parameters)?.len())
                        .map_err(|_| QueryExecutionError::BoundExceeded)?
                } else {
                    1
                };
                if route_count == 0 {
                    (QueryStepRows::Owned(Vec::new()), None, false)
                } else {
                    let CanonicalValue::String(source_pattern) = parameters
                        .get(pattern.pattern_parameter())
                        .ok_or_else(|| QueryExecutionError::MissingParameter {
                            parameter: pattern.pattern_parameter().to_owned(),
                        })?
                    else {
                        return Err(QueryExecutionError::InvalidParameter {
                            parameter: pattern.pattern_parameter().to_owned(),
                        });
                    };
                    let compiled = CompiledLongPatternV1::compile_bounded(
                        pattern.operator(),
                        pattern.profile(),
                        source_pattern.as_str(),
                        pattern.bounds(),
                    )
                    .map_err(|_| QueryExecutionError::InvalidParameter {
                        parameter: pattern.pattern_parameter().to_owned(),
                    })?;
                    let predicates = bind_predicates(
                        step,
                        parameters,
                        &bindings,
                        program.surface().candidates(),
                    )?;
                    let batch = providers
                        .as_deref_mut()
                        .ok_or(QueryExecutionError::BackendUnavailable)?
                        .batch(step, pattern.descriptor().digest())?;
                    let head = view.application_head();
                    let bounds = pattern.bounds();
                    if batch.descriptor != pattern.descriptor().digest()
                        || batch.state_schema_hash
                            != pattern.descriptor().state_identity().schema_hash()
                        || batch.floor.get() > head
                        || batch.ceiling.get() < head
                        || batch.rows.len() as u64 > limit
                        || batch.rows.len() as u32 > bounds.candidates()
                        || batch.scanned_rows
                            > u64::from(bounds.rows())
                                .checked_mul(route_count)
                                .ok_or(QueryExecutionError::BoundExceeded)?
                        || batch.verification_bytes
                            > bounds
                                .verification_bytes()
                                .checked_mul(route_count)
                                .ok_or(QueryExecutionError::BoundExceeded)?
                        || batch.rows.iter().any(|row| {
                            row.entity() != step.entity()
                                || predicates_match(row, &predicates) != Ok(true)
                                || row
                                    .field(pattern.field())
                                    .and_then(|value| match value {
                                        CanonicalValue::String(value) => Some(value.as_str()),
                                        _ => None,
                                    })
                                    .is_none_or(|value| !compiled.matches_source(value))
                        })
                    {
                        return Err(QueryExecutionError::BoundExceeded);
                    }
                    match provider_history_incarnation {
                        None => provider_history_incarnation = Some(batch.history_incarnation),
                        Some(existing) if existing == batch.history_incarnation => {}
                        Some(_) => return Err(QueryExecutionError::StaleCursor),
                    }
                    fuel.scans(batch.scanned_rows)?;
                    fuel.points(batch.rows.len() as u64)?;
                    let epoch_key = format!(
                        "provider.{}.{}.{}",
                        step.entity(),
                        provider,
                        batch.generation.get()
                    );
                    index_epochs.insert(epoch_key, head);
                    (QueryStepRows::Owned(batch.rows), None, false)
                }
            }
            riffdb_query_ir::QueryAccessKind::Nearest { .. } => {
                // WP-593: exact KNN execution path. The adapter applies the
                // bound predicates BEFORE ranking (filter-before-rank is
                // enforced structurally in the columnar engine), then runs
                // exact KNN. Scan work is charged against the same fuel the
                // plan cost vector funded (the static charge is the
                // partition-scan ceiling, not K). Parameterized K is resolved
                // and bounded before this backend call.
                let runtime_k =
                    u32::try_from(limit).map_err(|_| QueryExecutionError::BoundExceeded)?;
                let predicates =
                    bind_predicates(step, parameters, &bindings, program.surface().candidates())?;
                let page = view
                    .nearest(step, &predicates, runtime_k, policy)
                    .map_err(|error| map_view_error(&*view, &error))?;
                if page.scanned_rows > MAX_QUERY_SCANNED_ROWS
                    || page.rows.len() as u64 > limit
                    || page.rows.len() as u64 > page.scanned_rows
                {
                    return Err(QueryExecutionError::BoundExceeded);
                }
                fuel.scans(page.scanned_rows)?;
                (QueryStepRows::Owned(page.rows), Some(predicates), false)
            }
            riffdb_query_ir::QueryAccessKind::CandidateRootHydration {
                candidate,
                maximum_candidates,
                key_fields,
                order,
            } => {
                let hydration_cache_probe = if after.is_none()
                    && policy.is_none()
                    && !program.partition_route().is_finite_set()
                    && let Some(cand_def) = program
                        .surface()
                        .candidates()
                        .iter()
                        .find(|c| c.name() == candidate)
                    && !cand_def.sources().is_empty()
                {
                    let mut source_pins = Vec::with_capacity(cand_def.sources().len());
                    let mut source_ptrs = Vec::with_capacity(cand_def.sources().len());
                    let mut sources_ok = true;
                    for source_index in 0..cand_def.sources().len() {
                        if let Some(binding_name) =
                            candidate_source_binding_name(candidate, source_index)
                            && let Some(shared_arc) = bindings
                                .get(&binding_name)
                                .and_then(QueryStepRows::shared_arc)
                            && !shared_arc.is_empty()
                        {
                            source_ptrs.push((
                                Arc::as_ptr(shared_arc).cast::<QueryRow>() as usize,
                                shared_arc.len(),
                            ));
                            source_pins.push(Arc::clone(shared_arc));
                        } else {
                            sources_ok = false;
                            break;
                        }
                    }
                    let mut param_values = Vec::new();
                    if sources_ok {
                        for pred in step.predicates() {
                            match pred.value() {
                                QueryPredicateValue::CandidateBinding { name }
                                    if name == candidate => {}
                                QueryPredicateValue::Literal(_)
                                | QueryPredicateValue::EnumVariant { .. } => {}
                                QueryPredicateValue::Parameter(name) => {
                                    if let Some(val) = parameters.get(name) {
                                        param_values.push(val.clone());
                                    } else {
                                        sources_ok = false;
                                        break;
                                    }
                                }
                                _ => {
                                    sources_ok = false;
                                    break;
                                }
                            }
                        }
                    }
                    sources_ok.then(|| {
                        (
                            Arc::as_ptr(&prepared_plan) as usize,
                            step_index,
                            source_pins,
                            source_ptrs,
                            param_values,
                            view.application_head(),
                            limit,
                        )
                    })
                } else {
                    None
                };
                if let Some((
                    plan_ptr,
                    step_idx,
                    _,
                    ref source_ptrs,
                    ref param_values,
                    head,
                    req_limit,
                )) = hydration_cache_probe
                    && let Some(hit) = LOCAL_CANDIDATE_HYDRATION_CACHE.with(|cell| {
                        let guard = cell.try_borrow().ok()?;
                        guard
                            .entries
                            .iter()
                            .find(|entry| {
                                entry.plan_ptr == plan_ptr
                                    && entry.step_index == step_idx
                                    && entry.head == head
                                    && entry.limit == req_limit
                                    && entry.source_ptrs.as_slice() == source_ptrs.as_slice()
                                    && entry.param_values.as_slice() == param_values.as_slice()
                            })
                            .map(|entry| {
                                (
                                    entry.dependent_keys,
                                    entry.points,
                                    entry.intermediates,
                                    Arc::clone(&entry.hydrated_rows),
                                    entry.continuation.clone(),
                                )
                            })
                    })
                {
                    let (dep_keys, pts, inter, hydrated_rows, cached_cont) = hit;
                    fuel.dependent_keys(dep_keys)?;
                    fuel.points(pts)?;
                    fuel.intermediates(inter)?;
                    if let Some((binding, lower)) = cached_cont {
                        if lower.len() > MAX_QUERY_CONTINUATION_BYTES || continuation.is_some() {
                            return Err(QueryExecutionError::BoundExceeded);
                        }
                        continuation_binding = Some(binding);
                        continuation = Some(lower);
                    }
                    (QueryStepRows::Shared(hydrated_rows), None, false)
                } else {
                    let predicates = bind_candidate_root_hydrations(
                        step,
                        program,
                        parameters,
                        &bindings,
                        program.surface().candidates(),
                        candidate,
                    )?;
                    if predicates.len() > usize::from(*maximum_candidates) {
                        return Err(QueryExecutionError::BoundExceeded);
                    }
                    let key_count = u64::try_from(predicates.len())
                        .map_err(|_| QueryExecutionError::BoundExceeded)?;
                    fuel.dependent_keys(key_count)?;
                    let observations = view
                        .dependent_point_batch(step, &predicates, policy)
                        .map_err(|error| map_view_error(view, &error))?;
                    if observations.len() != predicates.len() {
                        return Err(QueryExecutionError::InvalidProgram);
                    }
                    let points_count = u64::try_from(observations.len())
                        .map_err(|_| QueryExecutionError::BoundExceeded)?;
                    fuel.points(points_count)?;
                    let pred_positions =
                        prepared_step.and_then(|s| s.predicate_positions.as_deref());
                    let pred_is_key = prepared_step.and_then(|s| s.predicate_is_key.as_deref());
                    let order_positions = prepared_step.and_then(|s| s.order_positions.as_deref());
                    let key_positions = prepared_step.and_then(|s| s.key_positions.as_deref());
                    let mut hydrated = Vec::with_capacity(observations.len());
                    for (row, predicates) in observations
                        .into_iter()
                        .zip(&predicates)
                        .filter_map(|(row, predicates)| row.map(|row| (row, predicates)))
                    {
                        if row.entity() != step.entity() {
                            return Err(QueryExecutionError::InvalidProgram);
                        }
                        let mut all_match = true;
                        for (pred_idx, predicate) in predicates.iter().enumerate() {
                            let pos = pred_positions.and_then(|p| p.get(pred_idx).copied());
                            let is_key = pred_is_key
                                .and_then(|flags| flags.get(pred_idx).copied())
                                .unwrap_or_else(|| {
                                    key_fields.iter().any(|key| key == predicate.field())
                                });
                            let matched = predicate_values_match(
                                step.entity(),
                                std::slice::from_ref(predicate),
                                |field| row.field_at(pos, field),
                            );
                            if is_key {
                                if matched != Ok(true) {
                                    return Err(QueryExecutionError::InvalidProgram);
                                }
                            } else if !matched? {
                                all_match = false;
                            }
                        }
                        if all_match {
                            hydrated.push(row);
                        }
                    }
                    let intermediates_count = u64::try_from(hydrated.len())
                        .map_err(|_| QueryExecutionError::BoundExceeded)?;
                    fuel.intermediates(intermediates_count)?;
                    let partition_field = candidate_root_partition_field(step, program)?;
                    let partition_pos = partition_field
                        .and_then(|pf| hydrated.first().and_then(|row| row.field_pos(pf)));
                    hydrated.sort_by(|left, right| {
                        candidate_root_row_order_at_positions(
                            left,
                            right,
                            order,
                            partition_field,
                            key_fields,
                            order_positions,
                            partition_pos,
                            key_positions,
                        )
                    });
                    if let Some(lower) = after {
                        let position = hydrated
                            .iter()
                            .position(|row| {
                                candidate_root_marker_at_positions(
                                    row,
                                    partition_field,
                                    key_fields,
                                    partition_pos,
                                    key_positions,
                                )
                                .is_ok_and(|marker| marker == lower)
                            })
                            .ok_or(QueryExecutionError::InvalidContinuation)?;
                        hydrated.drain(..=position);
                    }
                    let limit_usize =
                        usize::try_from(limit).map_err(|_| QueryExecutionError::BoundExceeded)?;
                    let mut step_cont = None;
                    if hydrated.len() > limit_usize {
                        hydrated.truncate(limit_usize);
                        let lower = hydrated
                            .last()
                            .ok_or(QueryExecutionError::InvalidProgram)
                            .and_then(|row| {
                                candidate_root_marker_at_positions(
                                    row,
                                    partition_field,
                                    key_fields,
                                    partition_pos,
                                    key_positions,
                                )
                            })?;
                        if lower.len() > MAX_QUERY_CONTINUATION_BYTES || continuation.is_some() {
                            return Err(QueryExecutionError::BoundExceeded);
                        }
                        let binding_owned = step.binding().to_owned();
                        step_cont = Some((binding_owned.clone(), lower.clone()));
                        continuation_binding = Some(binding_owned);
                        continuation = Some(lower);
                    }
                    if let Some((
                        plan_ptr,
                        step_idx,
                        source_pins,
                        source_ptrs,
                        param_values,
                        head,
                        req_limit,
                    )) = hydration_cache_probe
                    {
                        let shared_hydrated: Arc<[QueryRow]> = Arc::from(hydrated);
                        LOCAL_CANDIDATE_HYDRATION_CACHE.with(|cell| {
                            if let Ok(mut guard) = cell.try_borrow_mut() {
                                guard.insert(CachedCandidateHydration {
                                    _plan_pin: Arc::clone(&prepared_plan),
                                    _sources_pin: source_pins,
                                    plan_ptr,
                                    step_index: step_idx,
                                    source_ptrs,
                                    param_values,
                                    head,
                                    limit: req_limit,
                                    dependent_keys: key_count,
                                    points: points_count,
                                    intermediates: intermediates_count,
                                    hydrated_rows: Arc::clone(&shared_hydrated),
                                    continuation: step_cont,
                                });
                            }
                        });
                        (QueryStepRows::Shared(shared_hydrated), None, false)
                    } else {
                        (QueryStepRows::Owned(hydrated), None, false)
                    }
                }
            }
        };
        fuel.intermediates(
            u64::try_from(rows.len()).map_err(|_| QueryExecutionError::BoundExceeded)?,
        )?;
        if !rows.first().is_some_and(|row| row.selected_prevalidated)
            && rows.iter().any(|row| row.entity() != step.entity())
        {
            return Err(QueryExecutionError::InvalidProgram);
        }
        if let Some(predicates) = &scalar_predicates {
            let pred_positions = prepared_step.and_then(|s| s.predicate_positions.as_deref());
            if predicates_must_match {
                if rows.iter().any(|row| {
                    predicates_match_at_positions(row, predicates, pred_positions) != Ok(true)
                }) {
                    return Err(QueryExecutionError::InvalidProgram);
                }
            } else {
                rows.retain_owned(|row| {
                    predicates_match_at_positions(row, predicates, pred_positions).unwrap_or(false)
                });
            }
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
                    fields: Arc::new(BTreeMap::new()),
                    covered_result: None,
                    continuation_binding: None,
                    continuation: None,
                    cache_pinned: false,
                },
            );
        }
        if step_index == 0
            && prior.is_none()
            && policy.is_none()
            && providers.is_none()
            && continuation.is_none()
            && scalar_predicates.is_none()
            && !program.partition_route().is_finite_set()
            && view.application_head() > 0
            && !is_pipeline_clone_counting()
            && let Some(step0_arc) = rows.shared_arc()
            && !step0_arc.is_empty()
        {
            let plan_ptr = Arc::as_ptr(&prepared_plan) as usize;
            let program_hash = program.identity().hash();
            let aggregates_len = aggregates.len();
            let step0_ptr = Arc::as_ptr(step0_arc).cast::<QueryRow>() as usize;
            let step0_len = step0_arc.len();
            let head = view.application_head();
            if let Some(cached_snapshot) = LOCAL_WHOLE_SNAPSHOT_CACHE.with(|cell| {
                let guard = cell.try_borrow().ok()?;
                guard
                    .entries
                    .iter()
                    .find(|entry| {
                        entry.plan_ptr == plan_ptr
                            && entry.step0_ptr == step0_ptr
                            && entry.step0_len == step0_len
                            && entry.head == head
                            && entry.aggregates_len == aggregates_len
                            && entry.program_hash == program_hash
                            && &entry.parameters == parameters
                    })
                    .map(|entry| entry.snapshot.clone())
            }) {
                return Ok(cached_snapshot);
            }
            if let Some(shared_entry) = lookup_shared_whole_snapshot_cache(
                plan_ptr,
                step0_ptr,
                step0_len,
                head,
                aggregates_len,
                program_hash,
                parameters,
            ) {
                let cached_snapshot = shared_entry.snapshot.clone();
                LOCAL_WHOLE_SNAPSHOT_CACHE.with(|cell| {
                    if let Ok(mut guard) = cell.try_borrow_mut() {
                        guard.insert(shared_entry);
                    }
                });
                return Ok(cached_snapshot);
            }
            whole_snapshot_probe = Some(WholeSnapshotCacheProbe {
                plan_pin: Arc::clone(&prepared_plan),
                step0_pin: Arc::clone(step0_arc),
                plan_ptr,
                program_hash,
                aggregates_len,
                step0_ptr,
                step0_len,
                head,
            });
        }
        for (agg_idx, aggregate) in aggregates.iter().enumerate() {
            if aggregate.source_binding() != step.binding() || aggregate.parent_binding().is_some()
            {
                continue;
            }
            let prepared_agg = prepared_plan
                .aggregates
                .get(agg_idx)
                .and_then(Option::as_ref);
            let fallback_selected;
            let selected = if let Some(agg_plan) = prepared_agg {
                &agg_plan.selected
            } else {
                fallback_selected = selected_aggregate_fields(program, aggregate)?;
                &fallback_selected
            };
            if selected.is_empty() {
                continue;
            }
            let agg_rows = evaluate_operational_aggregate(
                program,
                Some(&prepared_plan),
                aggregate,
                selected,
                prepared_agg,
                &rows,
                rows.shared_arc(),
                parameters,
                &bindings,
                None,
            )?;
            let groups = agg_rows.len();
            let value = if aggregate.group_keys().is_empty() {
                QueryResultValue::AggregateOne(
                    agg_rows
                        .into_iter()
                        .next()
                        .ok_or(QueryExecutionError::InvalidProgram)?,
                )
            } else {
                QueryResultValue::AggregateMany(agg_rows)
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
        let retain_for_dependents = prepared_step.map_or_else(
            || binding_referenced_later(program, step_index, step.binding()),
            |s| s.retained_later,
        );
        if step.result_names().is_empty() {
            if !rows.first().is_some_and(|row| row.selected_prevalidated) {
                for row in &rows {
                    row.validate_selected(selected_names)?;
                }
            }
            let _ = u64::try_from(rows.len())
                .ok()
                .and_then(|rows| rows.checked_mul(step.selected_fields().len() as u64))
                .ok_or(QueryExecutionError::BoundExceeded)?;
            fuel.projected_values(0)?;
            if step.cardinality() == Cardinality::One && rows.is_empty() {
                return Err(QueryExecutionError::InvalidProgram);
            }
            if retain_for_dependents {
                bindings.insert(step.binding().to_owned(), rows);
            }
            continue;
        }
        let output_names = prepared_step
            .and_then(|s| s.output_selected_names.as_deref())
            .unwrap_or(selected_names);
        if output_names.len() != selected_names.len()
            && !rows.first().is_some_and(|row| row.selected_prevalidated)
        {
            for row in &rows {
                row.validate_selected(selected_names)?;
            }
        }
        let projected = match (&rows, retain_for_dependents) {
            (_, true) | (QueryStepRows::Shared(_), false) => rows
                .iter()
                .map(|row| row.project_clone(output_names))
                .collect::<Result<Vec<_>, _>>()?,
            (QueryStepRows::Owned(_), false) => {
                let QueryStepRows::Owned(mut owned) =
                    std::mem::replace(&mut rows, QueryStepRows::Owned(Vec::new()))
                else {
                    unreachable!()
                };
                for row in &mut owned {
                    row.project_in_place(output_names)?;
                }
                owned
            }
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

    if providers
        .as_deref()
        .is_some_and(|providers| !providers.is_complete())
    {
        return Err(QueryExecutionError::InvalidProgram);
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

    let mut snapshot = finish_snapshot(
        &mut fuel,
        QueryOwnedSnapshot {
            application_head: view.application_head(),
            index_epochs,
            outcome: default_outcome.to_owned(),
            fields: Arc::new(result_fields),
            covered_result: None,
            continuation_binding,
            continuation,
            cache_pinned: false,
        },
    )?;
    if snapshot.continuation.is_none()
        && snapshot.continuation_binding.is_none()
        && let Some(probe) = whole_snapshot_probe
    {
        snapshot.cache_pinned = true;
        let entry = CachedWholeQuerySnapshot {
            _plan_pin: probe.plan_pin,
            _step0_pin: probe.step0_pin,
            plan_ptr: probe.plan_ptr,
            program_hash: probe.program_hash,
            aggregates_len: probe.aggregates_len,
            step0_ptr: probe.step0_ptr,
            step0_len: probe.step0_len,
            head: probe.head,
            parameters: parameters.clone(),
            snapshot: snapshot.clone(),
        };
        insert_shared_whole_snapshot_cache(entry.clone());
        LOCAL_WHOLE_SNAPSHOT_CACHE.with(|cell| {
            if let Ok(mut guard) = cell.try_borrow_mut() {
                guard.insert(entry);
            }
        });
    }
    Ok(snapshot)
}

fn execute_covered_page_in_snapshot<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
    view: &mut V,
    policy: Option<&AuthorizedQueryRowPolicyContextV1>,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    let [step] = program.steps() else {
        return Err(QueryExecutionError::InvalidProgram);
    };
    let layout = step
        .covered_result_layout()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let QueryAccessKind::Index { index, .. } = step.access() else {
        return Err(QueryExecutionError::InvalidProgram);
    };
    if step.cardinality() != Cardinality::Many
        || step.result_names().len() != 1
        || !step.dependencies().is_empty()
        || !program.surface().aggregates().is_empty()
    {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let mut fuel = QueryExecutionFuel::from_cost(program.cost());
    fuel.step()?;
    let limit = resolve_row_limit(step, parameters)?;
    let after = prior
        .filter(|cursor| cursor.binding == step.binding())
        .map(|cursor| cursor.lower.as_slice());
    let predicates = bind_predicates(step, parameters, &BTreeMap::new(), &[])?;
    let batch = view
        .scan_covered(step, &predicates, limit, after, policy)
        .map_err(|error| map_view_error(view, &error))?
        .ok_or(QueryExecutionError::BackendIntegrity)?;
    if &batch.layout != layout
        || batch.rows.len() as u64 > limit
        || batch.rows.len() as u64 > batch.scanned_rows
        || batch.scanned_rows > MAX_QUERY_SCANNED_ROWS
        || batch.point_reads != 0
        || batch
            .continuation
            .as_ref()
            .is_some_and(|value| value.len() > MAX_QUERY_CONTINUATION_BYTES)
        || batch
            .rows
            .iter()
            .any(|row| row.len() != layout.fields().len())
    {
        return Err(QueryExecutionError::InvalidProgram);
    }
    fuel.scans(batch.scanned_rows)?;
    fuel.intermediates(
        u64::try_from(batch.rows.len()).map_err(|_| QueryExecutionError::BoundExceeded)?,
    )?;
    let selected_positions = step
        .selected_fields()
        .iter()
        .map(|selected| {
            layout
                .fields()
                .binary_search_by(|field| field.name().cmp(selected))
                .map_err(|_| QueryExecutionError::InvalidProgram)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let rows = batch
        .rows
        .into_iter()
        .map(|values| {
            let mut values = values.into_iter().map(Some).collect::<Vec<_>>();
            selected_positions
                .iter()
                .map(|position| {
                    values
                        .get_mut(*position)
                        .and_then(Option::take)
                        .ok_or(QueryExecutionError::InvalidProgram)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let projected = u64::try_from(rows.len())
        .ok()
        .and_then(|rows| rows.checked_mul(step.selected_fields().len() as u64))
        .ok_or(QueryExecutionError::BoundExceeded)?;
    fuel.projected_values(projected)?;

    let mut index_epochs = BTreeMap::new();
    let mut epoch_key = String::with_capacity(step.entity().len() + 1 + index.len());
    epoch_key.push_str(step.entity());
    epoch_key.push('.');
    epoch_key.push_str(index);
    index_epochs.insert(epoch_key, batch.epoch);
    if let Some(prior) = prior
        && (step.cursor_parameter().is_none() || prior.index_epochs != index_epochs)
    {
        return Err(if step.cursor_parameter().is_none() {
            QueryExecutionError::InvalidContinuation
        } else {
            QueryExecutionError::StaleCursor
        });
    }
    let continuation = batch.continuation;
    let continuation_binding = continuation.as_ref().map(|_| step.binding().to_owned());
    let default_outcome = program
        .surface()
        .schemas()
        .results()
        .first()
        .map(|branch| branch.name())
        .unwrap_or("Result")
        .to_owned();
    finish_snapshot(
        &mut fuel,
        QueryOwnedSnapshot {
            application_head: view.application_head(),
            index_epochs,
            outcome: default_outcome,
            fields: Arc::new(BTreeMap::new()),
            covered_result: Some(CoveredQueryResultV1 {
                result_name: step.result_names()[0].clone(),
                entity: Arc::<str>::from(step.entity()),
                fields: step
                    .selected_fields()
                    .iter()
                    .map(|field| Arc::<str>::from(field.as_str()))
                    .collect(),
                rows,
            }),
            continuation_binding,
            continuation,
            cache_pinned: false,
        },
    )
}

fn find_aggregate_result_field<'a>(
    fields: &'a [riffdb_query_ir::NamedFieldSchema],
    aggregate: &OperationalAggregateV1,
) -> Option<&'a riffdb_query_ir::NamedFieldSchema> {
    if aggregate.parent_binding().is_none() {
        return fields.iter().find(|field| field.name() == aggregate.name());
    }
    for field in fields {
        let nested_fields = match field.value_type() {
            NamedTypeSchema::Record(nested) => Some(nested.as_slice()),
            NamedTypeSchema::Optional(inner) => match inner.as_ref() {
                NamedTypeSchema::Record(nested) => Some(nested.as_slice()),
                _ => None,
            },
            NamedTypeSchema::List { element, .. } => match element.as_ref() {
                NamedTypeSchema::Record(nested) => Some(nested.as_slice()),
                _ => None,
            },
            _ => None,
        };
        if let Some(nested_fields) = nested_fields
            && let Some(found) = nested_fields
                .iter()
                .find(|candidate| candidate.name() == aggregate.name())
        {
            return Some(found);
        }
    }
    None
}

fn selected_aggregate_fields(
    program: &QueryAccessProgramV1,
    aggregate: &OperationalAggregateV1,
) -> Result<BTreeMap<String, String>, QueryExecutionError> {
    let branch = program
        .surface()
        .schemas()
        .results()
        .first()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let Some(field) = find_aggregate_result_field(branch.fields(), aggregate) else {
        return Ok(BTreeMap::new());
    };
    let record = match field.value_type() {
        NamedTypeSchema::Record(fields) => fields,
        NamedTypeSchema::List { element, .. } => match element.as_ref() {
            NamedTypeSchema::Record(fields) => fields,
            _ => return Err(QueryExecutionError::InvalidProgram),
        },
        _ => return Err(QueryExecutionError::InvalidProgram),
    };
    let mut available = aggregate
        .group_keys()
        .iter()
        .map(|key| (key.field(), key.value_type(), false))
        .chain(
            aggregate
                .measures()
                .iter()
                .map(|measure| (measure.alias(), measure.result_type(), false)),
        )
        .collect::<Vec<_>>();
    let mut output_names = BTreeSet::new();
    let mut selected = BTreeMap::new();
    let mut unmatched = Vec::new();
    for field in record {
        if !output_names.insert(field.name()) {
            return Err(QueryExecutionError::InvalidProgram);
        }
        if let Some((source_name, _, used)) = available
            .iter_mut()
            .find(|(name, ty, used)| !*used && *name == field.name() && *ty == field.value_type())
        {
            *used = true;
            selected.insert((*source_name).to_owned(), field.name().to_owned());
        } else {
            unmatched.push(field);
        }
    }
    for field in unmatched {
        let Some((source_name, _, used)) = available
            .iter_mut()
            .find(|(_, ty, used)| !*used && *ty == field.value_type())
        else {
            return Err(QueryExecutionError::InvalidProgram);
        };
        *used = true;
        selected.insert((*source_name).to_owned(), field.name().to_owned());
    }
    Ok(selected)
}

#[inline]
fn fnv1a_hash64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

#[derive(Default)]
struct AggregateScratch {
    groups: Vec<(Vec<u8>, Vec<CanonicalValue>, Vec<u32>)>,
    group_order: Vec<usize>,
    encoded_buf: Vec<u8>,
    group_hash_table: Vec<u32>,
    filter_cache: Vec<(bool, Vec<u32>)>,
    cells: Vec<Option<QueryAggregateCell>>,
    surviving_cells: Vec<Vec<Option<QueryAggregateCell>>>,
    surviving_order: Vec<(usize, usize)>,
}

struct CachedStaticAggregate {
    _plan_pin: Arc<PreparedExecutorProgramPlan>,
    _rows_pin: Arc<[QueryRow]>,
    agg_plan_ptr: usize,
    rows_ptr: usize,
    rows_len: usize,
    maximum_groups: u64,
    dynamic_having: Vec<BoundPredicate>,
    dynamic_measure_filters: Vec<Option<Vec<BoundPredicate>>>,
    output: Vec<QueryAggregateRow>,
}

const LOCAL_STATIC_AGGREGATE_CACHE_CAP: usize = 256;

#[derive(Default)]
struct LocalStaticAggregateCache {
    entries: Vec<CachedStaticAggregate>,
    next_slot: usize,
}

impl LocalStaticAggregateCache {
    fn insert(&mut self, entry: CachedStaticAggregate) {
        if self.entries.len() < LOCAL_STATIC_AGGREGATE_CACHE_CAP {
            self.entries.push(entry);
        } else {
            self.entries[self.next_slot] = entry;
            self.next_slot = (self.next_slot + 1) % LOCAL_STATIC_AGGREGATE_CACHE_CAP;
        }
    }
}

struct CachedExpansionStep {
    _plan_pin: Arc<PreparedExecutorProgramPlan>,
    _drivers_pin: Arc<[QueryRow]>,
    plan_ptr: usize,
    step_ptr: usize,
    drivers_ptr: usize,
    drivers_len: usize,
    head: u64,
    limit: u64,
    template_predicates: Vec<BoundPredicate>,
    total_scanned_rows: u64,
    total_point_reads: u64,
    total_agg_projected: u64,
    total_rows: u64,
    observed_epoch: Option<u64>,
    completed_drivers: Arc<[QueryRow]>,
}

const LOCAL_EXPANSION_STEP_CACHE_CAP: usize = 64;

#[derive(Default)]
struct LocalExpansionStepCache {
    entries: Vec<CachedExpansionStep>,
    next_slot: usize,
}

impl LocalExpansionStepCache {
    fn insert(&mut self, entry: CachedExpansionStep) {
        if self.entries.len() < LOCAL_EXPANSION_STEP_CACHE_CAP {
            self.entries.push(entry);
        } else {
            self.entries[self.next_slot] = entry;
            self.next_slot = (self.next_slot + 1) % LOCAL_EXPANSION_STEP_CACHE_CAP;
        }
    }
}

struct CachedCandidateHydration {
    _plan_pin: Arc<PreparedExecutorProgramPlan>,
    _sources_pin: Vec<Arc<[QueryRow]>>,
    plan_ptr: usize,
    step_index: usize,
    source_ptrs: Vec<(usize, usize)>,
    param_values: Vec<CanonicalValue>,
    head: u64,
    limit: u64,
    dependent_keys: u64,
    points: u64,
    intermediates: u64,
    hydrated_rows: Arc<[QueryRow]>,
    continuation: Option<(String, Vec<u8>)>,
}

const LOCAL_CANDIDATE_HYDRATION_CACHE_CAP: usize = 32;

#[derive(Default)]
struct LocalCandidateHydrationCache {
    entries: Vec<CachedCandidateHydration>,
    next_slot: usize,
}

impl LocalCandidateHydrationCache {
    fn insert(&mut self, entry: CachedCandidateHydration) {
        if self.entries.len() < LOCAL_CANDIDATE_HYDRATION_CACHE_CAP {
            self.entries.push(entry);
        } else {
            self.entries[self.next_slot] = entry;
            self.next_slot = (self.next_slot + 1) % LOCAL_CANDIDATE_HYDRATION_CACHE_CAP;
        }
    }
}

struct WholeSnapshotCacheProbe {
    plan_pin: Arc<PreparedExecutorProgramPlan>,
    step0_pin: Arc<[QueryRow]>,
    plan_ptr: usize,
    program_hash: riffdb_types::QueryPlanHash,
    aggregates_len: usize,
    step0_ptr: usize,
    step0_len: usize,
    head: u64,
}

#[derive(Clone)]
struct CachedWholeQuerySnapshot {
    _plan_pin: Arc<PreparedExecutorProgramPlan>,
    _step0_pin: Arc<[QueryRow]>,
    plan_ptr: usize,
    program_hash: riffdb_types::QueryPlanHash,
    aggregates_len: usize,
    step0_ptr: usize,
    step0_len: usize,
    head: u64,
    parameters: QueryParameters,
    snapshot: QueryOwnedSnapshot,
}

const LOCAL_WHOLE_SNAPSHOT_CACHE_CAP: usize = 64;
const SHARED_WHOLE_SNAPSHOT_CACHE_SHARDS: usize = 64;

#[derive(Default)]
struct LocalWholeSnapshotCache {
    entries: Vec<CachedWholeQuerySnapshot>,
    next_slot: usize,
}

impl LocalWholeSnapshotCache {
    fn insert(&mut self, entry: CachedWholeQuerySnapshot) {
        if self.entries.len() < LOCAL_WHOLE_SNAPSHOT_CACHE_CAP {
            self.entries.push(entry);
        } else {
            self.entries[self.next_slot] = entry;
            self.next_slot = (self.next_slot + 1) % LOCAL_WHOLE_SNAPSHOT_CACHE_CAP;
        }
    }
}

static SHARED_WHOLE_SNAPSHOT_CACHE: std::sync::OnceLock<
    [std::sync::Mutex<LocalWholeSnapshotCache>; SHARED_WHOLE_SNAPSHOT_CACHE_SHARDS],
> = std::sync::OnceLock::new();

fn shared_whole_snapshot_cache()
-> &'static [std::sync::Mutex<LocalWholeSnapshotCache>; SHARED_WHOLE_SNAPSHOT_CACHE_SHARDS] {
    SHARED_WHOLE_SNAPSHOT_CACHE.get_or_init(|| {
        std::array::from_fn(|_| std::sync::Mutex::new(LocalWholeSnapshotCache::default()))
    })
}

fn lookup_shared_whole_snapshot_cache(
    plan_ptr: usize,
    step0_ptr: usize,
    step0_len: usize,
    head: u64,
    aggregates_len: usize,
    program_hash: riffdb_types::QueryPlanHash,
    parameters: &QueryParameters,
) -> Option<CachedWholeQuerySnapshot> {
    let shard = ((step0_ptr >> 4) ^ (plan_ptr >> 4)) & (SHARED_WHOLE_SNAPSHOT_CACHE_SHARDS - 1);
    let guard = shared_whole_snapshot_cache()[shard].lock().ok()?;
    guard
        .entries
        .iter()
        .find(|entry| {
            entry.plan_ptr == plan_ptr
                && entry.step0_ptr == step0_ptr
                && entry.step0_len == step0_len
                && entry.head == head
                && entry.aggregates_len == aggregates_len
                && entry.program_hash == program_hash
                && &entry.parameters == parameters
        })
        .cloned()
}

fn insert_shared_whole_snapshot_cache(entry: CachedWholeQuerySnapshot) {
    let shard =
        ((entry.step0_ptr >> 4) ^ (entry.plan_ptr >> 4)) & (SHARED_WHOLE_SNAPSHOT_CACHE_SHARDS - 1);
    if let Ok(mut guard) = shared_whole_snapshot_cache()[shard].lock() {
        guard.insert(entry);
    }
}

thread_local! {
    static LOCAL_AGGREGATE_SCRATCH: std::cell::RefCell<AggregateScratch> =
        std::cell::RefCell::new(AggregateScratch::default());
    static LOCAL_STATIC_AGGREGATE_CACHE: std::cell::RefCell<LocalStaticAggregateCache> =
        std::cell::RefCell::new(LocalStaticAggregateCache::default());
    static LOCAL_EXPANSION_STEP_CACHE: std::cell::RefCell<LocalExpansionStepCache> =
        std::cell::RefCell::new(LocalExpansionStepCache::default());
    static LOCAL_CANDIDATE_HYDRATION_CACHE: std::cell::RefCell<LocalCandidateHydrationCache> =
        std::cell::RefCell::new(LocalCandidateHydrationCache::default());
    static LOCAL_WHOLE_SNAPSHOT_CACHE: std::cell::RefCell<LocalWholeSnapshotCache> =
        std::cell::RefCell::new(LocalWholeSnapshotCache::default());
}

#[allow(clippy::too_many_arguments)]
fn evaluate_operational_aggregate(
    program: &QueryAccessProgramV1,
    prepared_plan: Option<&Arc<PreparedExecutorProgramPlan>>,
    aggregate: &OperationalAggregateV1,
    selected: &BTreeMap<String, String>,
    prepared_agg: Option<&PreparedAggregatePlan>,
    rows: &[QueryRow],
    shared_rows: Option<&Arc<[QueryRow]>>,
    parameters: &QueryParameters,
    bindings: &BTreeMap<String, QueryStepRows>,
    driver: Option<(&str, &QueryRow)>,
) -> Result<Vec<QueryAggregateRow>, QueryExecutionError> {
    LOCAL_AGGREGATE_SCRATCH.with(|cell| {
        if let Ok(mut scratch) = cell.try_borrow_mut() {
            evaluate_operational_aggregate_with_scratch(
                program,
                prepared_plan,
                aggregate,
                selected,
                prepared_agg,
                rows,
                shared_rows,
                parameters,
                bindings,
                driver,
                &mut scratch,
            )
        } else {
            let mut scratch = AggregateScratch::default();
            evaluate_operational_aggregate_with_scratch(
                program,
                prepared_plan,
                aggregate,
                selected,
                prepared_agg,
                rows,
                shared_rows,
                parameters,
                bindings,
                driver,
                &mut scratch,
            )
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn evaluate_operational_aggregate_with_scratch(
    program: &QueryAccessProgramV1,
    prepared_plan: Option<&Arc<PreparedExecutorProgramPlan>>,
    aggregate: &OperationalAggregateV1,
    selected: &BTreeMap<String, String>,
    prepared_agg: Option<&PreparedAggregatePlan>,
    rows: &[QueryRow],
    shared_rows: Option<&Arc<[QueryRow]>>,
    parameters: &QueryParameters,
    bindings: &BTreeMap<String, QueryStepRows>,
    driver: Option<(&str, &QueryRow)>,
    scratch: &mut AggregateScratch,
) -> Result<Vec<QueryAggregateRow>, QueryExecutionError> {
    let maximum_groups = resolve_page_bound(aggregate.maximum_groups(), parameters)?;
    let static_cache_keys = if let (Some(plan_pin), Some(agg_plan), Some(shared_rows)) =
        (prepared_plan, prepared_agg, shared_rows)
        && !shared_rows.is_empty()
    {
        let probe_having =
            if aggregate.having().is_empty() || agg_plan.static_bound_having.is_some() {
                Some(Vec::new())
            } else {
                bind_aggregate_predicates(
                    aggregate.having(),
                    parameters,
                    bindings,
                    program.surface().candidates(),
                    driver,
                )
                .ok()
            };
        let probe_filters = if agg_plan.all_measure_filters_static {
            Some(Vec::new())
        } else {
            let mut filters = Vec::with_capacity(agg_plan.active_measures.len());
            let mut all_ok = true;
            for prepared_measure in &agg_plan.active_measures {
                let measure = &aggregate.measures()[prepared_measure.measure_index];
                if measure.filter().is_empty() || prepared_measure.static_bound_filter.is_some() {
                    filters.push(None);
                } else if let Ok(bound) = bind_aggregate_predicates(
                    measure.filter(),
                    parameters,
                    bindings,
                    program.surface().candidates(),
                    driver,
                ) {
                    filters.push(Some(bound));
                } else {
                    all_ok = false;
                    break;
                }
            }
            all_ok.then_some(filters)
        };
        if let (Some(probe_having), Some(probe_filters)) = (probe_having, probe_filters) {
            let agg_plan_ptr = std::ptr::from_ref(agg_plan) as usize;
            let rows_ptr = Arc::as_ptr(shared_rows).cast::<QueryRow>() as usize;
            let rows_len = shared_rows.len();
            if let Some(cached) = LOCAL_STATIC_AGGREGATE_CACHE.with(|cell| {
                let guard = cell.try_borrow().ok()?;
                guard
                    .entries
                    .iter()
                    .find(|entry| {
                        entry.agg_plan_ptr == agg_plan_ptr
                            && entry.rows_ptr == rows_ptr
                            && entry.rows_len == rows_len
                            && entry.maximum_groups == maximum_groups
                            && entry.dynamic_having == probe_having
                            && entry.dynamic_measure_filters == probe_filters
                    })
                    .map(|entry| entry.output.clone())
            }) {
                return Ok(cached);
            }
            Some((
                plan_pin,
                shared_rows,
                agg_plan_ptr,
                rows_ptr,
                rows_len,
                probe_having,
                probe_filters,
            ))
        } else {
            None
        }
    } else {
        None
    };
    let accumulation_ceiling = if aggregate.having().is_empty() && aggregate.order_by().is_empty() {
        maximum_groups
    } else {
        u64::from(riffdb_types::MAX_AGGREGATE_DISTINCT_VALUES_V1)
    };
    let fallback_plan;
    let agg_plan = if let Some(plan) = prepared_agg {
        plan
    } else {
        fallback_plan = PreparedAggregatePlan::build(Some(program), aggregate, selected.clone());
        &fallback_plan
    };
    let mut fuel = AggregateExecutionFuel::new(aggregate);
    let is_ungrouped = aggregate.group_keys().is_empty();
    if is_ungrouped {
        fuel.consume_state(1)?;
    } else {
        scratch.group_order.clear();
        scratch.group_hash_table.clear();
        let encoded_buf = &mut scratch.encoded_buf;
        let groups = &mut scratch.groups;
        let group_order = &mut scratch.group_order;
        let group_hash_table = &mut scratch.group_hash_table;
        let mut last_group_idx = usize::MAX;
        let mut last_day_bucket: Option<(i64, i64, CanonicalValue, [u8; 17])> = None;
        for (row_idx, row) in rows.iter().enumerate() {
            let row_idx_u32 = row_idx as u32;
            encoded_buf.clear();
            for (key, &key_pos) in aggregate
                .group_keys()
                .iter()
                .zip(&agg_plan.group_key_positions)
            {
                let raw = row.field_at(key_pos, key.source_field()).ok_or_else(|| {
                    QueryExecutionError::MissingField {
                        entity: aggregate.source_entity().to_owned(),
                        field: key.source_field().to_owned(),
                    }
                })?;
                if key.time_bucket() == Some(OperationalAggregateTimeBucketV1::Day)
                    && let CanonicalValue::Timestamp(ts) = raw
                {
                    let secs = ts.seconds();
                    if let Some((start, end, _, ref encoded_bytes)) = last_day_bucket
                        && secs >= start
                        && secs < end
                    {
                        encoded_buf.extend_from_slice(encoded_bytes);
                        continue;
                    }
                    let truncated = truncate_temporal_group_key(
                        raw,
                        Some(OperationalAggregateTimeBucketV1::Day),
                    )?;
                    let len_pos = encoded_buf.len();
                    encoded_buf.extend_from_slice(&[0u8; 4]);
                    encode_canonical_value_into(&mut *encoded_buf, &truncated)
                        .map_err(|_| QueryExecutionError::InvalidProgram)?;
                    let value_len = u32::try_from(encoded_buf.len() - len_pos - 4)
                        .map_err(|_| QueryExecutionError::BoundExceeded)?;
                    encoded_buf[len_pos..len_pos + 4].copy_from_slice(&value_len.to_be_bytes());
                    if let CanonicalValue::Timestamp(day_ts) = &truncated
                        && encoded_buf.len() - len_pos == 17
                        && let Some(day_end) = day_ts.seconds().checked_add(86_400)
                    {
                        let mut fixed = [0u8; 17];
                        fixed.copy_from_slice(&encoded_buf[len_pos..len_pos + 17]);
                        last_day_bucket = Some((day_ts.seconds(), day_end, truncated, fixed));
                    }
                    continue;
                }
                let len_pos = encoded_buf.len();
                encoded_buf.extend_from_slice(&[0u8; 4]);
                if let Some(bucket) = key.time_bucket() {
                    let truncated = truncate_temporal_group_key(raw, Some(bucket))?;
                    encode_canonical_value_into(&mut *encoded_buf, &truncated)
                        .map_err(|_| QueryExecutionError::InvalidProgram)?;
                } else {
                    encode_canonical_value_into(&mut *encoded_buf, raw)
                        .map_err(|_| QueryExecutionError::InvalidProgram)?;
                }
                let value_len = u32::try_from(encoded_buf.len() - len_pos - 4)
                    .map_err(|_| QueryExecutionError::BoundExceeded)?;
                encoded_buf[len_pos..len_pos + 4].copy_from_slice(&value_len.to_be_bytes());
            }
            let active_groups = group_order.len();
            if last_group_idx < active_groups && groups[last_group_idx].0 == *encoded_buf {
                groups[last_group_idx].2.push(row_idx_u32);
                continue;
            }
            let tail_start = active_groups.saturating_sub(4);
            if let Some(rel) = groups[tail_start..active_groups]
                .iter()
                .rposition(|(k, _, _)| k.as_slice() == encoded_buf.as_slice())
            {
                let idx = tail_start + rel;
                last_group_idx = idx;
                groups[idx].2.push(row_idx_u32);
                continue;
            }
            let existing_idx = if group_hash_table.is_empty() {
                groups[..tail_start]
                    .iter()
                    .rposition(|(k, _, _)| k.as_slice() == encoded_buf.as_slice())
            } else {
                let mask = group_hash_table.len() - 1;
                let mut slot = (fnv1a_hash64(encoded_buf) as usize) & mask;
                let mut found = None;
                loop {
                    let entry = group_hash_table[slot];
                    if entry == u32::MAX {
                        break;
                    }
                    let idx = entry as usize;
                    if groups[idx].0.as_slice() == encoded_buf.as_slice() {
                        found = Some(idx);
                        break;
                    }
                    slot = (slot + 1) & mask;
                }
                found
            };
            if let Some(idx) = existing_idx {
                last_group_idx = idx;
                groups[idx].2.push(row_idx_u32);
                continue;
            }
            let state_bytes = encoded_buf
                .len()
                .checked_add(32)
                .ok_or(QueryExecutionError::BoundExceeded)?;
            fuel.consume_state(state_bytes)?;
            let new_idx = active_groups;
            if (new_idx as u64) >= accumulation_ceiling {
                return Err(QueryExecutionError::BoundExceeded);
            }
            if new_idx < groups.len() {
                let (slot_encoded, slot_keys, slot_rows) = &mut groups[new_idx];
                slot_encoded.clear();
                slot_encoded.extend_from_slice(encoded_buf);
                slot_keys.clear();
                for (key, &key_pos) in aggregate
                    .group_keys()
                    .iter()
                    .zip(&agg_plan.group_key_positions)
                {
                    let raw = row
                        .field_at(key_pos, key.source_field())
                        .expect("validated while encoding group key");
                    let val = if key.time_bucket() == Some(OperationalAggregateTimeBucketV1::Day)
                        && let CanonicalValue::Timestamp(ts) = raw
                        && let Some((start, end, ref cached_val, _)) = last_day_bucket
                        && ts.seconds() >= start
                        && ts.seconds() < end
                    {
                        cached_val.clone()
                    } else {
                        truncate_temporal_group_key(raw, key.time_bucket())?
                    };
                    slot_keys.push(val);
                }
                slot_rows.clear();
                slot_rows.push(row_idx_u32);
            } else {
                let mut keys = Vec::with_capacity(aggregate.group_keys().len());
                for (key, &key_pos) in aggregate
                    .group_keys()
                    .iter()
                    .zip(&agg_plan.group_key_positions)
                {
                    let raw = row
                        .field_at(key_pos, key.source_field())
                        .expect("validated while encoding group key");
                    let val = if key.time_bucket() == Some(OperationalAggregateTimeBucketV1::Day)
                        && let CanonicalValue::Timestamp(ts) = raw
                        && let Some((start, end, ref cached_val, _)) = last_day_bucket
                        && ts.seconds() >= start
                        && ts.seconds() < end
                    {
                        cached_val.clone()
                    } else {
                        truncate_temporal_group_key(raw, key.time_bucket())?
                    };
                    keys.push(val);
                }
                let mut group_rows = Vec::with_capacity(2);
                group_rows.push(row_idx_u32);
                groups.push((encoded_buf.clone(), keys, group_rows));
            }
            group_order.push(new_idx);
            let active_len = group_order.len();
            if new_idx == 16 && group_hash_table.is_empty() {
                group_hash_table.resize(64, u32::MAX);
                let mask = group_hash_table.len() - 1;
                for (i, (k, _, _)) in groups[..active_len].iter().enumerate() {
                    let mut slot = (fnv1a_hash64(k) as usize) & mask;
                    while group_hash_table[slot] != u32::MAX {
                        slot = (slot + 1) & mask;
                    }
                    group_hash_table[slot] = i as u32;
                }
            } else if !group_hash_table.is_empty() {
                if active_len * 2 > group_hash_table.len() {
                    let new_cap = group_hash_table.len() * 2;
                    group_hash_table.clear();
                    group_hash_table.resize(new_cap, u32::MAX);
                    let mask = new_cap - 1;
                    for (i, (k, _, _)) in groups[..active_len].iter().enumerate() {
                        let mut slot = (fnv1a_hash64(k) as usize) & mask;
                        while group_hash_table[slot] != u32::MAX {
                            slot = (slot + 1) & mask;
                        }
                        group_hash_table[slot] = i as u32;
                    }
                } else {
                    let mask = group_hash_table.len() - 1;
                    let mut slot = (fnv1a_hash64(&groups[new_idx].0) as usize) & mask;
                    while group_hash_table[slot] != u32::MAX {
                        slot = (slot + 1) & mask;
                    }
                    group_hash_table[slot] = new_idx as u32;
                }
            }
            last_group_idx = new_idx;
        }
        if aggregate.order_by().is_empty() && group_order.len() > 1 {
            group_order.sort_unstable_by(|&a, &b| groups[a].0.cmp(&groups[b].0));
        }
    }

    let dynamic_having;
    let bound_having: &[BoundPredicate] = if aggregate.having().is_empty() {
        &[]
    } else if let Some(static_having) = &agg_plan.static_bound_having {
        static_having
    } else {
        dynamic_having = bind_aggregate_predicates(
            aggregate.having(),
            parameters,
            bindings,
            program.surface().candidates(),
            driver,
        )?;
        &dynamic_having
    };
    let dynamic_measure_filters = if agg_plan.all_measure_filters_static {
        Vec::new()
    } else {
        let mut filters = Vec::with_capacity(agg_plan.active_measures.len());
        for prepared_measure in &agg_plan.active_measures {
            let measure = &aggregate.measures()[prepared_measure.measure_index];
            let filter =
                if measure.filter().is_empty() || prepared_measure.static_bound_filter.is_some() {
                    None
                } else {
                    Some(bind_aggregate_predicates(
                        measure.filter(),
                        parameters,
                        bindings,
                        program.surface().candidates(),
                        driver,
                    )?)
                };
            filters.push(filter);
        }
        filters
    };

    let num_group_keys = agg_plan.group_key_arcs.len();
    let num_slots = num_group_keys + agg_plan.active_measures.len();
    if scratch.filter_cache.len() < agg_plan.num_unique_filters {
        scratch
            .filter_cache
            .resize_with(agg_plan.num_unique_filters, || (false, Vec::new()));
    }
    let mut quantile_cache = Vec::new();
    let mut seen_values_buf: Vec<&CanonicalValue> = Vec::new();
    scratch.cells.clear();
    if scratch.cells.capacity() < num_slots {
        scratch.cells.reserve(num_slots - scratch.cells.len());
    }

    let output = if is_ungrouped {
        for (populated, buf) in &mut scratch.filter_cache[..agg_plan.num_unique_filters] {
            *populated = false;
            buf.clear();
        }
        for (idx, prepared_measure) in agg_plan.active_measures.iter().enumerate() {
            let measure = &aggregate.measures()[prepared_measure.measure_index];
            let bound_filter = if let Some(static_filter) = &prepared_measure.static_bound_filter {
                Some(static_filter.as_ref())
            } else {
                dynamic_measure_filters.get(idx).and_then(Option::as_deref)
            };
            let cell = evaluate_aggregate_measure(
                aggregate,
                measure,
                prepared_measure.scale,
                prepared_measure.input_field_pos,
                prepared_measure.filter_slot,
                prepared_measure.filter_positions.as_deref(),
                prepared_measure.shares_quantile_input,
                rows,
                None,
                bound_filter,
                &mut scratch.filter_cache[..agg_plan.num_unique_filters],
                &mut quantile_cache,
                &mut seen_values_buf,
                &mut fuel,
            )?;
            scratch.cells.push(cell);
        }
        if !bound_having.is_empty()
            && !aggregate_row_matches_having_slots(
                aggregate.name(),
                &scratch.cells,
                bound_having,
                &agg_plan.having_slots,
            )?
        {
            return Err(QueryExecutionError::InvalidProgram);
        }
        let mut fields = BTreeMap::new();
        for &(slot_idx, ref output_arc) in &agg_plan.output_slots {
            if let Some(Some(cell)) = scratch.cells.get_mut(slot_idx).map(Option::take) {
                fields.insert(Arc::clone(output_arc), cell);
            }
        }
        vec![QueryAggregateRow::from_fields(
            Arc::clone(&agg_plan.aggregate_entity),
            fields,
        )]
    } else if bound_having.is_empty() && aggregate.order_by().is_empty() {
        let mut output = Vec::with_capacity(scratch.group_order.len());
        for &group_idx in &scratch.group_order {
            let (_, keys, group_indices) = &mut scratch.groups[group_idx];
            scratch.cells.clear();
            for (arc_name, value) in agg_plan.group_key_arcs.iter().zip(keys.drain(..)) {
                if arc_name.is_some() {
                    scratch
                        .cells
                        .push(Some(QueryAggregateCell::Canonical(value)));
                } else {
                    scratch.cells.push(None);
                }
            }
            for (populated, buf) in &mut scratch.filter_cache[..agg_plan.num_unique_filters] {
                *populated = false;
                buf.clear();
            }
            for (populated, _, _, values, _) in &mut quantile_cache {
                *populated = false;
                values.clear();
            }
            for (idx, prepared_measure) in agg_plan.active_measures.iter().enumerate() {
                let measure = &aggregate.measures()[prepared_measure.measure_index];
                let bound_filter =
                    if let Some(static_filter) = &prepared_measure.static_bound_filter {
                        Some(static_filter.as_ref())
                    } else {
                        dynamic_measure_filters.get(idx).and_then(Option::as_deref)
                    };
                let cell = evaluate_aggregate_measure(
                    aggregate,
                    measure,
                    prepared_measure.scale,
                    prepared_measure.input_field_pos,
                    prepared_measure.filter_slot,
                    prepared_measure.filter_positions.as_deref(),
                    prepared_measure.shares_quantile_input,
                    rows,
                    Some(group_indices.as_slice()),
                    bound_filter,
                    &mut scratch.filter_cache[..agg_plan.num_unique_filters],
                    &mut quantile_cache,
                    &mut seen_values_buf,
                    &mut fuel,
                )?;
                scratch.cells.push(cell);
            }
            let mut fields = BTreeMap::new();
            for &(slot_idx, ref output_arc) in &agg_plan.output_slots {
                if let Some(Some(cell)) = scratch.cells.get_mut(slot_idx).map(Option::take) {
                    fields.insert(Arc::clone(output_arc), cell);
                }
            }
            output.push(QueryAggregateRow::from_fields(
                Arc::clone(&agg_plan.aggregate_entity),
                fields,
            ));
        }
        output
    } else {
        scratch.surviving_order.clear();
        if scratch.surviving_order.capacity() < scratch.group_order.len() {
            scratch
                .surviving_order
                .reserve(scratch.group_order.len() - scratch.surviving_order.len());
        }
        for &group_idx in &scratch.group_order {
            let (_, keys, group_indices) = &mut scratch.groups[group_idx];
            scratch.cells.clear();
            for (arc_name, value) in agg_plan.group_key_arcs.iter().zip(keys.drain(..)) {
                if arc_name.is_some() {
                    scratch
                        .cells
                        .push(Some(QueryAggregateCell::Canonical(value)));
                } else {
                    scratch.cells.push(None);
                }
            }
            for (populated, buf) in &mut scratch.filter_cache[..agg_plan.num_unique_filters] {
                *populated = false;
                buf.clear();
            }
            for (populated, _, _, values, _) in &mut quantile_cache {
                *populated = false;
                values.clear();
            }
            for (idx, prepared_measure) in agg_plan.active_measures.iter().enumerate() {
                let measure = &aggregate.measures()[prepared_measure.measure_index];
                let bound_filter =
                    if let Some(static_filter) = &prepared_measure.static_bound_filter {
                        Some(static_filter.as_ref())
                    } else {
                        dynamic_measure_filters.get(idx).and_then(Option::as_deref)
                    };
                let cell = evaluate_aggregate_measure(
                    aggregate,
                    measure,
                    prepared_measure.scale,
                    prepared_measure.input_field_pos,
                    prepared_measure.filter_slot,
                    prepared_measure.filter_positions.as_deref(),
                    prepared_measure.shares_quantile_input,
                    rows,
                    Some(group_indices.as_slice()),
                    bound_filter,
                    &mut scratch.filter_cache[..agg_plan.num_unique_filters],
                    &mut quantile_cache,
                    &mut seen_values_buf,
                    &mut fuel,
                )?;
                scratch.cells.push(cell);
            }
            if !bound_having.is_empty()
                && !aggregate_row_matches_having_slots(
                    aggregate.name(),
                    &scratch.cells,
                    bound_having,
                    &agg_plan.having_slots,
                )?
            {
                continue;
            }
            let surviving_idx = scratch.surviving_order.len();
            if surviving_idx < scratch.surviving_cells.len() {
                scratch.surviving_cells[surviving_idx].clear();
                std::mem::swap(
                    &mut scratch.cells,
                    &mut scratch.surviving_cells[surviving_idx],
                );
                if scratch.cells.capacity() < num_slots {
                    scratch.cells.reserve(num_slots - scratch.cells.len());
                }
            } else {
                let group_cells =
                    std::mem::replace(&mut scratch.cells, Vec::with_capacity(num_slots));
                scratch.surviving_cells.push(group_cells);
            }
            scratch.surviving_order.push((group_idx, surviving_idx));
        }

        if !aggregate.order_by().is_empty() {
            let mut sort_error = None;
            let groups = &scratch.groups;
            let surviving_cells = &scratch.surviving_cells;
            scratch.surviving_order.sort_unstable_by(
                |&(left_group, left_surv), &(right_group, right_surv)| {
                    match compare_aggregate_group_row_slots(
                        &surviving_cells[left_surv],
                        &surviving_cells[right_surv],
                        aggregate.order_by(),
                        &agg_plan.order_slots,
                        &groups[left_group].0,
                        &groups[right_group].0,
                    ) {
                        Ok(ordering) => ordering,
                        Err(error) => {
                            if sort_error.is_none() {
                                sort_error = Some(error);
                            }
                            Ordering::Equal
                        }
                    }
                },
            );
            if let Some(error) = sort_error {
                return Err(error);
            }
        }
        let limit =
            usize::try_from(maximum_groups).map_err(|_| QueryExecutionError::BoundExceeded)?;
        scratch.surviving_order.truncate(limit);

        let mut output = Vec::with_capacity(scratch.surviving_order.len());
        for &(_, surviving_idx) in &scratch.surviving_order {
            let cells = &mut scratch.surviving_cells[surviving_idx];
            let mut fields = BTreeMap::new();
            for &(slot_idx, ref output_arc) in &agg_plan.output_slots {
                if let Some(Some(cell)) = cells.get_mut(slot_idx).map(Option::take) {
                    fields.insert(Arc::clone(output_arc), cell);
                }
            }
            output.push(QueryAggregateRow::from_fields(
                Arc::clone(&agg_plan.aggregate_entity),
                fields,
            ));
        }
        output
    };
    if let Some((
        plan_pin,
        shared_rows,
        agg_plan_ptr,
        rows_ptr,
        rows_len,
        dynamic_having,
        dynamic_measure_filters,
    )) = static_cache_keys
    {
        LOCAL_STATIC_AGGREGATE_CACHE.with(|cell| {
            if let Ok(mut guard) = cell.try_borrow_mut() {
                guard.insert(CachedStaticAggregate {
                    _plan_pin: Arc::clone(plan_pin),
                    _rows_pin: Arc::clone(shared_rows),
                    agg_plan_ptr,
                    rows_ptr,
                    rows_len,
                    maximum_groups,
                    dynamic_having,
                    dynamic_measure_filters,
                    output: output.clone(),
                });
            }
        });
    }
    Ok(output)
}

const fn civil_from_days_utc(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

const fn days_from_civil_utc(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m = month as u64;
    let d = day as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + (doe as i64) - 719_468
}

fn truncate_days_bucket(
    days: i64,
    bucket: OperationalAggregateTimeBucketV1,
) -> Result<i64, QueryExecutionError> {
    match bucket {
        OperationalAggregateTimeBucketV1::Hour => Err(QueryExecutionError::InvalidProgram),
        OperationalAggregateTimeBucketV1::Day => Ok(days),
        OperationalAggregateTimeBucketV1::Week => Ok(days - (days + 3).rem_euclid(7)),
        OperationalAggregateTimeBucketV1::Month => {
            let (year, month, _) = civil_from_days_utc(days);
            Ok(days_from_civil_utc(year, month, 1))
        }
        OperationalAggregateTimeBucketV1::Year => {
            let (year, _, _) = civil_from_days_utc(days);
            Ok(days_from_civil_utc(year, 1, 1))
        }
    }
}

fn truncate_temporal_group_key(
    value: &CanonicalValue,
    bucket: Option<OperationalAggregateTimeBucketV1>,
) -> Result<CanonicalValue, QueryExecutionError> {
    let Some(bucket) = bucket else {
        return Ok(value.clone());
    };
    match value {
        CanonicalValue::Null => Ok(CanonicalValue::Null),
        CanonicalValue::Timestamp(ts) => {
            let bucket_seconds = match bucket {
                OperationalAggregateTimeBucketV1::Hour => {
                    ts.seconds() - ts.seconds().rem_euclid(3_600)
                }
                _ => {
                    let days = ts.seconds().div_euclid(86_400);
                    let bucket_days = truncate_days_bucket(days, bucket)?;
                    bucket_days
                        .checked_mul(86_400)
                        .ok_or(QueryExecutionError::AggregateOverflow)?
                }
            };
            Timestamp::new(bucket_seconds, 0)
                .map(CanonicalValue::Timestamp)
                .map_err(|_| QueryExecutionError::InvalidProgram)
        }
        CanonicalValue::Date(date) => {
            let days = i64::from(date.days_since_unix_epoch());
            let bucket_days = truncate_days_bucket(days, bucket)?;
            let bucket_days_i32 =
                i32::try_from(bucket_days).map_err(|_| QueryExecutionError::AggregateOverflow)?;
            Ok(CanonicalValue::Date(Date::new(bucket_days_i32)))
        }
        _ => Err(QueryExecutionError::InvalidProgram),
    }
}

fn bind_aggregate_predicates(
    predicates: &[QueryPredicate],
    parameters: &QueryParameters,
    bindings: &BTreeMap<String, QueryStepRows>,
    candidates: &[CandidateBindingV1],
    driver: Option<(&str, &QueryRow)>,
) -> Result<Vec<BoundPredicate>, QueryExecutionError> {
    predicates
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
                QueryPredicateValue::BindingField { binding, field } => driver
                    .filter(|(driver_binding, _)| *driver_binding == binding)
                    .map(|(_, row)| row)
                    .or_else(|| bindings.get(binding).and_then(|rows| rows.first()))
                    .and_then(|row| row.field(field))
                    .cloned()
                    .unwrap_or(CanonicalValue::Null),
                QueryPredicateValue::BindingFieldSet { .. } => {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                QueryPredicateValue::CandidateBinding { name } => {
                    bind_candidate_value(name, candidates, bindings)?
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
                match predicate.value() {
                    QueryPredicateValue::Parameter(parameter) => {
                        validate_canonical_set(&value).map_err(|()| {
                            QueryExecutionError::InvalidParameter {
                                parameter: parameter.clone(),
                            }
                        })?;
                    }
                    QueryPredicateValue::CandidateBinding { .. } => {}
                    _ => return Err(QueryExecutionError::InvalidProgram),
                }
            }
            Ok(BoundPredicate {
                field: predicate.field().to_owned(),
                operator: predicate.operator(),
                value,
            })
        })
        .collect()
}

fn compare_exact_decimal_with_canonical(
    coefficient: i128,
    scale: u8,
    rhs: &CanonicalValue,
) -> Option<Ordering> {
    match rhs {
        CanonicalValue::Decimal(decimal) => {
            let rhs_scale = decimal.spec().scale();
            if scale == rhs_scale {
                Some(coefficient.cmp(&decimal.coefficient()))
            } else if scale > rhs_scale {
                let factor = 10_i128.checked_pow(u32::from(scale - rhs_scale))?;
                let scaled = decimal.coefficient().checked_mul(factor)?;
                Some(coefficient.cmp(&scaled))
            } else {
                let factor = 10_i128.checked_pow(u32::from(rhs_scale - scale))?;
                let scaled = coefficient.checked_mul(factor)?;
                Some(scaled.cmp(&decimal.coefficient()))
            }
        }
        CanonicalValue::Money(money) => {
            let rhs_scale = money.amount().spec().scale();
            if scale == rhs_scale {
                Some(coefficient.cmp(&money.amount().coefficient()))
            } else if scale > rhs_scale {
                let factor = 10_i128.checked_pow(u32::from(scale - rhs_scale))?;
                let scaled = money.amount().coefficient().checked_mul(factor)?;
                Some(coefficient.cmp(&scaled))
            } else {
                let factor = 10_i128.checked_pow(u32::from(rhs_scale - scale))?;
                let scaled = coefficient.checked_mul(factor)?;
                Some(scaled.cmp(&money.amount().coefficient()))
            }
        }
        CanonicalValue::U64(value) => {
            let factor = 10_i128.checked_pow(u32::from(scale))?;
            let scaled = i128::from(*value).checked_mul(factor)?;
            Some(coefficient.cmp(&scaled))
        }
        CanonicalValue::I64(value) => {
            let factor = 10_i128.checked_pow(u32::from(scale))?;
            let scaled = i128::from(*value).checked_mul(factor)?;
            Some(coefficient.cmp(&scaled))
        }
        _ => None,
    }
}

fn aggregate_row_matches_having_slots(
    aggregate_name: &str,
    cells: &[Option<QueryAggregateCell>],
    predicates: &[BoundPredicate],
    having_slots: &[usize],
) -> Result<bool, QueryExecutionError> {
    for (predicate, &slot) in predicates.iter().zip(having_slots) {
        let cell = cells.get(slot).and_then(Option::as_ref);
        let is_null = matches!(
            cell,
            None | Some(QueryAggregateCell::Canonical(CanonicalValue::Null))
        );
        let unary = match predicate.operator {
            QueryPredicateOperator::IsNull => Some(is_null),
            QueryPredicateOperator::IsNotNull => Some(!is_null),
            QueryPredicateOperator::Exists => Some(cell.is_some()),
            _ => None,
        };
        if let Some(matches) = unary {
            if !matches {
                return Ok(false);
            }
            continue;
        }
        let Some(cell) = cell else {
            return Err(QueryExecutionError::MissingField {
                entity: aggregate_name.to_owned(),
                field: predicate.field.clone(),
            });
        };
        let matches = match cell {
            QueryAggregateCell::Canonical(actual) => match predicate.operator {
                QueryPredicateOperator::Equal => {
                    actual == &predicate.value
                        || matches!(
                            (actual, &predicate.value),
                            (CanonicalValue::I64(i), CanonicalValue::U64(u))
                                if *i >= 0 && (*i as u64) == *u
                        )
                }
                QueryPredicateOperator::NotEqual => {
                    actual != &predicate.value
                        && !matches!(
                            (actual, &predicate.value),
                            (CanonicalValue::I64(i), CanonicalValue::U64(u))
                                if *i >= 0 && (*i as u64) == *u
                        )
                }
                QueryPredicateOperator::In => match &predicate.value {
                    CanonicalValue::List(values) => {
                        values.values().iter().any(|value| value == actual)
                    }
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
                        .or_else(|| match (actual, &predicate.value) {
                            (CanonicalValue::I64(i), CanonicalValue::U64(u)) => {
                                if *i < 0 {
                                    Some(Ordering::Less)
                                } else {
                                    Some((*i as u64).cmp(u))
                                }
                            }
                            (CanonicalValue::Decimal(d), rhs) => {
                                compare_exact_decimal_with_canonical(
                                    d.coefficient(),
                                    d.spec().scale(),
                                    rhs,
                                )
                            }
                            (CanonicalValue::Money(m), rhs) => {
                                compare_exact_decimal_with_canonical(
                                    m.amount().coefficient(),
                                    m.amount().spec().scale(),
                                    rhs,
                                )
                            }
                            _ => None,
                        })
                        .ok_or(QueryExecutionError::UnsupportedPredicate)?;
                    match operator {
                        QueryPredicateOperator::Less => ordering == Ordering::Less,
                        QueryPredicateOperator::LessEqual => ordering != Ordering::Greater,
                        QueryPredicateOperator::Greater => ordering == Ordering::Greater,
                        QueryPredicateOperator::GreaterEqual => ordering != Ordering::Less,
                        _ => unreachable!(),
                    }
                }
            },
            QueryAggregateCell::ExactDecimal { coefficient, scale } => {
                let ordering =
                    compare_exact_decimal_with_canonical(*coefficient, *scale, &predicate.value)
                        .ok_or(QueryExecutionError::UnsupportedPredicate)?;
                match predicate.operator {
                    QueryPredicateOperator::Equal => ordering == Ordering::Equal,
                    QueryPredicateOperator::NotEqual => ordering != Ordering::Equal,
                    QueryPredicateOperator::Less => ordering == Ordering::Less,
                    QueryPredicateOperator::LessEqual => ordering != Ordering::Greater,
                    QueryPredicateOperator::Greater => ordering == Ordering::Greater,
                    QueryPredicateOperator::GreaterEqual => ordering != Ordering::Less,
                    _ => return Err(QueryExecutionError::UnsupportedPredicate),
                }
            }
            QueryAggregateCell::ExactMean { .. } => {
                return Err(QueryExecutionError::UnsupportedPredicate);
            }
        };
        if !matches {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compare_aggregate_cells(
    left: &QueryAggregateCell,
    right: &QueryAggregateCell,
) -> Result<Ordering, QueryExecutionError> {
    match (left, right) {
        (QueryAggregateCell::Canonical(l), QueryAggregateCell::Canonical(r)) => {
            aggregate_scalar_order(l, r)
        }
        (
            QueryAggregateCell::ExactDecimal {
                coefficient: l,
                scale: ls,
            },
            QueryAggregateCell::ExactDecimal {
                coefficient: r,
                scale: rs,
            },
        ) if ls == rs => Ok(l.cmp(r)),
        (
            QueryAggregateCell::ExactMean {
                coefficient: l,
                scale: ls,
                count: lc,
            },
            QueryAggregateCell::ExactMean {
                coefficient: r,
                scale: rs,
                count: rc,
            },
        ) if ls == rs => match (*lc, *rc) {
            (0, 0) => Ok(Ordering::Equal),
            (0, _) => Ok(Ordering::Less),
            (_, 0) => Ok(Ordering::Greater),
            (lc, rc) => {
                let lhs = l
                    .checked_mul(i128::from(rc))
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
                let rhs = r
                    .checked_mul(i128::from(lc))
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
                Ok(lhs.cmp(&rhs))
            }
        },
        _ => Err(QueryExecutionError::InvalidProgram),
    }
}

fn compare_aggregate_group_row_slots(
    left_cells: &[Option<QueryAggregateCell>],
    right_cells: &[Option<QueryAggregateCell>],
    order_by: &[QueryRootOrderTermV1],
    order_slots: &[usize],
    left_key: &[u8],
    right_key: &[u8],
) -> Result<Ordering, QueryExecutionError> {
    for (term, &slot) in order_by.iter().zip(order_slots) {
        let left = left_cells.get(slot).and_then(Option::as_ref);
        let right = right_cells.get(slot).and_then(Option::as_ref);
        let left_null = matches!(
            left,
            None | Some(QueryAggregateCell::Canonical(CanonicalValue::Null))
        );
        let right_null = matches!(
            right,
            None | Some(QueryAggregateCell::Canonical(CanonicalValue::Null))
        );
        let ordering = match (left_null, right_null) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if term.nulls_first() {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if term.nulls_first() {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let left = left.expect("non-null checked");
                let right = right.expect("non-null checked");
                let ord = compare_aggregate_cells(left, right)?;
                if term.direction() == AccessDirection::Reverse {
                    ord.reverse()
                } else {
                    ord
                }
            }
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(left_key.cmp(right_key))
}

struct AggregateExecutionFuel {
    remaining_state_bytes: usize,
    remaining_arithmetic_operations: usize,
    maximum_distinct_values_per_measure: usize,
}

impl AggregateExecutionFuel {
    fn new(aggregate: &OperationalAggregateV1) -> Self {
        let budget = aggregate.execution_budget();
        Self {
            remaining_state_bytes: budget.maximum_state_bytes() as usize,
            remaining_arithmetic_operations: budget.maximum_arithmetic_operations() as usize,
            maximum_distinct_values_per_measure: usize::from(
                budget.maximum_distinct_values_per_measure(),
            ),
        }
    }

    fn consume_state(&mut self, bytes: usize) -> Result<(), QueryExecutionError> {
        self.remaining_state_bytes = self
            .remaining_state_bytes
            .checked_sub(bytes)
            .ok_or(QueryExecutionError::BoundExceeded)?;
        Ok(())
    }

    fn consume_operations(&mut self, operations: usize) -> Result<(), QueryExecutionError> {
        self.remaining_arithmetic_operations = self
            .remaining_arithmetic_operations
            .checked_sub(operations)
            .ok_or(QueryExecutionError::BoundExceeded)?;
        Ok(())
    }
}

enum ActiveRows<'a, 'b> {
    All(std::slice::Iter<'a, QueryRow>),
    Indexed(&'a [QueryRow], std::slice::Iter<'b, u32>),
}

impl<'a> Iterator for ActiveRows<'a, '_> {
    type Item = &'a QueryRow;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::All(iter) => iter.next(),
            Self::Indexed(rows, indices) => indices.next().map(|&idx| &rows[idx as usize]),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl DoubleEndedIterator for ActiveRows<'_, '_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::All(iter) => iter.next_back(),
            Self::Indexed(rows, indices) => indices.next_back().map(|&idx| &rows[idx as usize]),
        }
    }
}

impl ExactSizeIterator for ActiveRows<'_, '_> {
    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::All(iter) => iter.len(),
            Self::Indexed(_, indices) => indices.len(),
        }
    }
}

#[inline]
fn active_rows<'a, 'b>(
    all_rows: &'a [QueryRow],
    active_indices: Option<&'b [u32]>,
) -> ActiveRows<'a, 'b> {
    match active_indices {
        Some(indices) => ActiveRows::Indexed(all_rows, indices.iter()),
        None => ActiveRows::All(all_rows.iter()),
    }
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn evaluate_aggregate_measure<'a>(
    aggregate: &OperationalAggregateV1,
    measure: &'a riffdb_query_ir::OperationalAggregateMeasureV1,
    preparsed_scale: Option<u8>,
    input_field_pos: Option<usize>,
    filter_slot: Option<usize>,
    filter_positions: Option<&[usize]>,
    shares_quantile_input: bool,
    all_rows: &'a [QueryRow],
    group_indices: Option<&[u32]>,
    bound_filter: Option<&[BoundPredicate]>,
    filter_cache: &mut [(bool, Vec<u32>)],
    quantile_cache: &mut Vec<(bool, &'a str, Option<usize>, Vec<&'a CanonicalValue>, usize)>,
    seen_values_buf: &mut Vec<&'a CanonicalValue>,
    fuel: &mut AggregateExecutionFuel,
) -> Result<Option<QueryAggregateCell>, QueryExecutionError> {
    let semantic = measure.function().semantic_identity();
    let input_len = group_indices.map_or(all_rows.len(), <[u32]>::len);
    fuel.consume_operations(input_len)?;
    let active_indices: Option<&[u32]> = if let Some(bound_filter) = bound_filter {
        let slot = filter_slot.expect("non-empty filter has slot");
        let (ref mut populated, ref mut matched) = filter_cache[slot];
        if !*populated {
            matched.clear();
            if let Some(indices) = group_indices {
                matched.reserve(indices.len());
                for &row_idx in indices {
                    let row = &all_rows[row_idx as usize];
                    if predicates_match_at_positions(row, bound_filter, filter_positions)? {
                        matched.push(row_idx);
                    }
                }
            } else {
                matched.reserve(all_rows.len());
                for (row_idx, row) in all_rows.iter().enumerate() {
                    if predicates_match_at_positions(row, bound_filter, filter_positions)? {
                        matched.push(row_idx as u32);
                    }
                }
            }
            *populated = true;
        }
        Some(matched.as_slice())
    } else {
        group_indices
    };
    let active_len = active_indices.map_or(all_rows.len(), <[u32]>::len);
    match measure.function() {
        OperationalAggregateFunctionV1::Median
        | OperationalAggregateFunctionV1::P90
        | OperationalAggregateFunctionV1::P95
        | OperationalAggregateFunctionV1::P99 => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let percentile: usize = match measure.function() {
                OperationalAggregateFunctionV1::Median => 50,
                OperationalAggregateFunctionV1::P90 => 90,
                OperationalAggregateFunctionV1::P95 => 95,
                OperationalAggregateFunctionV1::P99 => 99,
                _ => unreachable!(),
            };
            if !shares_quantile_input {
                let mut stack_buf = [&CanonicalValue::Null; 64];
                let values_slice: &mut [&'a CanonicalValue] = if active_len <= 64 {
                    let mut count = 0usize;
                    for row in active_rows(all_rows, active_indices) {
                        let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                        if matches!(value, CanonicalValue::Null) {
                            continue;
                        }
                        let encoded_len = canonical_value_encoded_len(value)
                            .map_err(|_| QueryExecutionError::InvalidProgram)?;
                        fuel.consume_state(encoded_len)?;
                        stack_buf[count] = value;
                        count += 1;
                    }
                    &mut stack_buf[..count]
                } else {
                    seen_values_buf.clear();
                    seen_values_buf.reserve(active_len);
                    for row in active_rows(all_rows, active_indices) {
                        let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                        if matches!(value, CanonicalValue::Null) {
                            continue;
                        }
                        let encoded_len = canonical_value_encoded_len(value)
                            .map_err(|_| QueryExecutionError::InvalidProgram)?;
                        fuel.consume_state(encoded_len)?;
                        seen_values_buf.push(value);
                    }
                    seen_values_buf.as_mut_slice()
                };
                if values_slice.is_empty() {
                    return if matches!(
                        measure.result_type(),
                        NamedTypeSchema::Optional(inner)
                            if matches!(inner.as_ref(), NamedTypeSchema::Optional(_))
                    ) {
                        Ok(None)
                    } else {
                        Ok(Some(QueryAggregateCell::Canonical(CanonicalValue::Null)))
                    };
                }
                let rank = (percentile * values_slice.len()).div_ceil(100);
                let index = rank.saturating_sub(1).min(values_slice.len() - 1);
                let mut order_err = None;
                let (_, selected, _) = values_slice.select_nth_unstable_by(index, |left, right| {
                    match aggregate_scalar_order(left, right) {
                        Ok(ord) => ord,
                        Err(err) => {
                            if order_err.is_none() {
                                order_err = Some(err);
                            }
                            Ordering::Equal
                        }
                    }
                });
                if let Some(err) = order_err {
                    return Err(err);
                }
                return Ok(Some(QueryAggregateCell::Canonical((*selected).clone())));
            }
            let existing_pos = quantile_cache
                .iter()
                .position(|(_, f, flt_slot, _, _)| *f == field && *flt_slot == filter_slot);
            let values: &[&'a CanonicalValue] = match existing_pos {
                Some(pos) if quantile_cache[pos].0 => {
                    let (_, _, _, ref cached_values, state_bytes) = quantile_cache[pos];
                    fuel.consume_state(state_bytes)?;
                    cached_values
                }
                Some(pos) => {
                    let (ref mut populated, _, _, ref mut values, ref mut cached_state_bytes) =
                        quantile_cache[pos];
                    values.clear();
                    values.reserve(active_len);
                    let mut state_bytes = 0usize;
                    for row in active_rows(all_rows, active_indices) {
                        let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                        if matches!(value, CanonicalValue::Null) {
                            continue;
                        }
                        let encoded_len = canonical_value_encoded_len(value)
                            .map_err(|_| QueryExecutionError::InvalidProgram)?;
                        fuel.consume_state(encoded_len)?;
                        state_bytes = state_bytes
                            .checked_add(encoded_len)
                            .ok_or(QueryExecutionError::BoundExceeded)?;
                        values.push(value);
                    }
                    let mut order_err = None;
                    values.sort_unstable_by(|left, right| {
                        match aggregate_scalar_order(left, right) {
                            Ok(ord) => ord,
                            Err(err) => {
                                if order_err.is_none() {
                                    order_err = Some(err);
                                }
                                Ordering::Equal
                            }
                        }
                    });
                    if let Some(err) = order_err {
                        return Err(err);
                    }
                    *populated = true;
                    *cached_state_bytes = state_bytes;
                    values
                }
                None => {
                    let mut values = Vec::with_capacity(active_len);
                    let mut state_bytes = 0usize;
                    for row in active_rows(all_rows, active_indices) {
                        let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                        if matches!(value, CanonicalValue::Null) {
                            continue;
                        }
                        let encoded_len = canonical_value_encoded_len(value)
                            .map_err(|_| QueryExecutionError::InvalidProgram)?;
                        fuel.consume_state(encoded_len)?;
                        state_bytes = state_bytes
                            .checked_add(encoded_len)
                            .ok_or(QueryExecutionError::BoundExceeded)?;
                        values.push(value);
                    }
                    let mut order_err = None;
                    values.sort_unstable_by(|left, right| {
                        match aggregate_scalar_order(left, right) {
                            Ok(ord) => ord,
                            Err(err) => {
                                if order_err.is_none() {
                                    order_err = Some(err);
                                }
                                Ordering::Equal
                            }
                        }
                    });
                    if let Some(err) = order_err {
                        return Err(err);
                    }
                    quantile_cache.push((true, field, filter_slot, values, state_bytes));
                    &quantile_cache.last().expect("just pushed").3
                }
            };
            if values.is_empty() {
                return if matches!(
                    measure.result_type(),
                    NamedTypeSchema::Optional(inner)
                        if matches!(inner.as_ref(), NamedTypeSchema::Optional(_))
                ) {
                    Ok(None)
                } else {
                    Ok(Some(QueryAggregateCell::Canonical(CanonicalValue::Null)))
                };
            }
            let rank = (percentile * values.len()).div_ceil(100);
            let index = rank.saturating_sub(1).min(values.len() - 1);
            return Ok(Some(QueryAggregateCell::Canonical(values[index].clone())));
        }
        OperationalAggregateFunctionV1::First | OperationalAggregateFunctionV1::Last => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let selected_value = if measure.function() == OperationalAggregateFunctionV1::First {
                let mut found = None;
                for row in active_rows(all_rows, active_indices) {
                    let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                    if !matches!(value, CanonicalValue::Null) {
                        found = Some(value);
                        break;
                    }
                }
                found
            } else {
                let mut found = None;
                for row in active_rows(all_rows, active_indices).rev() {
                    let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                    if !matches!(value, CanonicalValue::Null) {
                        found = Some(value);
                        break;
                    }
                }
                found
            };
            if let Some(value) = selected_value {
                fuel.consume_state(
                    canonical_value_encoded_len(value)
                        .map_err(|_| QueryExecutionError::InvalidProgram)?,
                )?;
                return Ok(Some(QueryAggregateCell::Canonical(value.clone())));
            }
            return if matches!(
                measure.result_type(),
                NamedTypeSchema::Optional(inner)
                    if matches!(inner.as_ref(), NamedTypeSchema::Optional(_))
            ) {
                Ok(None)
            } else {
                Ok(Some(QueryAggregateCell::Canonical(CanonicalValue::Null)))
            };
        }
        OperationalAggregateFunctionV1::Stddev => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let scale = match preparsed_scale {
                Some(scale) => scale,
                None => aggregate_sum_scale(measure.result_type())?,
            };
            let mut sum = 0_i128;
            let mut sum_sq = 0_u128;
            let mut count = 0_u128;
            for row in active_rows(all_rows, active_indices) {
                let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                let contribution = match value {
                    CanonicalValue::Null => continue,
                    CanonicalValue::I64(value) => i128::from(*value),
                    CanonicalValue::U64(value) => i128::from(*value),
                    CanonicalValue::Decimal(value) if value.spec().scale() == scale => {
                        value.coefficient()
                    }
                    CanonicalValue::Money(value) if value.amount().spec().scale() == scale => {
                        value.amount().coefficient()
                    }
                    _ => return Err(QueryExecutionError::InvalidProgram),
                };
                sum = sum
                    .checked_add(contribution)
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
                let abs = contribution.unsigned_abs();
                let sq = abs
                    .checked_mul(abs)
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
                sum_sq = sum_sq
                    .checked_add(sq)
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
                count = count
                    .checked_add(1)
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
            }
            fuel.consume_state(std::mem::size_of::<i128>() * 2 + std::mem::size_of::<u64>())?;
            let coefficient = if count <= 1 {
                0_i128
            } else {
                let n_sum_sq = count
                    .checked_mul(sum_sq)
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
                let sum_abs = sum.unsigned_abs();
                let sum_sq_total = sum_abs
                    .checked_mul(sum_abs)
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
                let numerator = n_sum_sq.saturating_sub(sum_sq_total);
                let std_coeff = numerator.isqrt() / count;
                i128::try_from(std_coeff).map_err(|_| QueryExecutionError::AggregateOverflow)?
            };
            return Ok(Some(QueryAggregateCell::ExactDecimal {
                coefficient,
                scale,
            }));
        }
        OperationalAggregateFunctionV1::Collect
        | OperationalAggregateFunctionV1::CollectDistinct => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let distinct_only =
                measure.function() == OperationalAggregateFunctionV1::CollectDistinct;
            let mut seen = BTreeSet::new();
            let mut values = Vec::with_capacity(active_len.min(64));
            for row in active_rows(all_rows, active_indices) {
                let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                if matches!(value, CanonicalValue::Null) {
                    continue;
                }
                if distinct_only {
                    if seen.is_empty() {
                        if values.contains(value) {
                            continue;
                        }
                        if values.len() < 64 {
                            let encoded_len = canonical_value_encoded_len(value)
                                .map_err(|_| QueryExecutionError::InvalidProgram)?;
                            if values.len() >= fuel.maximum_distinct_values_per_measure {
                                return Err(QueryExecutionError::BoundExceeded);
                            }
                            fuel.consume_state(
                                encoded_len
                                    .checked_add(32)
                                    .ok_or(QueryExecutionError::BoundExceeded)?,
                            )?;
                            values.push(value.clone());
                            continue;
                        }
                        for prior in &values {
                            let prior_encoded = encode_canonical_value(prior)
                                .map_err(|_| QueryExecutionError::InvalidProgram)?;
                            seen.insert(prior_encoded);
                        }
                    }
                    let encoded = encode_canonical_value(value)
                        .map_err(|_| QueryExecutionError::InvalidProgram)?;
                    if !seen.insert(encoded.clone()) {
                        continue;
                    }
                    if values.len() >= fuel.maximum_distinct_values_per_measure {
                        return Err(QueryExecutionError::BoundExceeded);
                    }
                    fuel.consume_state(
                        encoded
                            .len()
                            .checked_add(32)
                            .ok_or(QueryExecutionError::BoundExceeded)?,
                    )?;
                    values.push(value.clone());
                } else {
                    let encoded_len = canonical_value_encoded_len(value)
                        .map_err(|_| QueryExecutionError::InvalidProgram)?;
                    if values.len() >= fuel.maximum_distinct_values_per_measure {
                        return Err(QueryExecutionError::BoundExceeded);
                    }
                    fuel.consume_state(
                        encoded_len
                            .checked_add(32)
                            .ok_or(QueryExecutionError::BoundExceeded)?,
                    )?;
                    values.push(value.clone());
                }
            }
            let list =
                CanonicalList::new(values).map_err(|_| QueryExecutionError::BoundExceeded)?;
            return Ok(Some(QueryAggregateCell::Canonical(CanonicalValue::List(
                list,
            ))));
        }
        _ => {}
    }
    match semantic {
        AggregateSemanticIdentityV1::Count => {
            fuel.consume_state(std::mem::size_of::<u64>())?;
            Ok(Some(QueryAggregateCell::Canonical(CanonicalValue::U64(
                u64::try_from(active_len).map_err(|_| QueryExecutionError::BoundExceeded)?,
            ))))
        }
        AggregateSemanticIdentityV1::Sum => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let scale = match preparsed_scale {
                Some(scale) => scale,
                None => aggregate_sum_scale(measure.result_type())?,
            };
            let mut coefficient = 0_i128;
            for row in active_rows(all_rows, active_indices) {
                let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                let contribution = match value {
                    CanonicalValue::Null => continue,
                    CanonicalValue::I64(value) => i128::from(*value),
                    CanonicalValue::U64(value) => i128::from(*value),
                    CanonicalValue::Decimal(value) if value.spec().scale() == scale => {
                        value.coefficient()
                    }
                    CanonicalValue::Money(value) if value.amount().spec().scale() == scale => {
                        value.amount().coefficient()
                    }
                    _ => return Err(QueryExecutionError::InvalidProgram),
                };
                coefficient = coefficient
                    .checked_add(contribution)
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
            }
            fuel.consume_state(std::mem::size_of::<i128>() + std::mem::size_of::<u8>())?;
            Ok(Some(QueryAggregateCell::ExactDecimal {
                coefficient,
                scale,
            }))
        }
        AggregateSemanticIdentityV1::Min | AggregateSemanticIdentityV1::Max => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let mut selected_value: Option<&CanonicalValue> = None;
            for row in active_rows(all_rows, active_indices) {
                let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                selected_value = Some(match selected_value {
                    None => value,
                    Some(current) => {
                        let ordering = aggregate_scalar_order(value, current)?;
                        let replace = match semantic {
                            AggregateSemanticIdentityV1::Min => ordering == Ordering::Less,
                            AggregateSemanticIdentityV1::Max => ordering == Ordering::Greater,
                            AggregateSemanticIdentityV1::Count
                            | AggregateSemanticIdentityV1::ExactCount
                            | AggregateSemanticIdentityV1::Sum
                            | AggregateSemanticIdentityV1::CountPresent
                            | AggregateSemanticIdentityV1::CountDistinct
                            | AggregateSemanticIdentityV1::CountDistinctPresent
                            | AggregateSemanticIdentityV1::Mean
                            | AggregateSemanticIdentityV1::Any
                            | AggregateSemanticIdentityV1::All => unreachable!(),
                        };
                        if replace { value } else { current }
                    }
                });
            }
            if let Some(value) = selected_value {
                fuel.consume_state(
                    canonical_value_encoded_len(value)
                        .map_err(|_| QueryExecutionError::InvalidProgram)?,
                )?;
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
        AggregateSemanticIdentityV1::CountPresent => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let mut count = 0_u64;
            for row in active_rows(all_rows, active_indices) {
                let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                if !matches!(value, CanonicalValue::Null) {
                    count = count
                        .checked_add(1)
                        .ok_or(QueryExecutionError::AggregateOverflow)?;
                }
            }
            fuel.consume_state(std::mem::size_of::<u64>())?;
            Ok(Some(QueryAggregateCell::Canonical(CanonicalValue::U64(
                count,
            ))))
        }
        AggregateSemanticIdentityV1::CountDistinct
        | AggregateSemanticIdentityV1::CountDistinctPresent => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let mut stack_seen = [&CanonicalValue::Null; 64];
            let mut stack_len = 0usize;
            let mut distinct = BTreeSet::new();
            let mut distinct_count = 0usize;
            for row in active_rows(all_rows, active_indices) {
                let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                if semantic == AggregateSemanticIdentityV1::CountDistinctPresent
                    && matches!(value, CanonicalValue::Null)
                {
                    continue;
                }
                if distinct.is_empty() {
                    if stack_seen[..stack_len].contains(&value) {
                        continue;
                    }
                    if stack_len < 64 {
                        let encoded_len = canonical_value_encoded_len(value)
                            .map_err(|_| QueryExecutionError::InvalidProgram)?;
                        if distinct_count >= fuel.maximum_distinct_values_per_measure {
                            return Err(QueryExecutionError::BoundExceeded);
                        }
                        fuel.consume_state(
                            encoded_len
                                .checked_add(32)
                                .ok_or(QueryExecutionError::BoundExceeded)?,
                        )?;
                        stack_seen[stack_len] = value;
                        stack_len += 1;
                        distinct_count += 1;
                        continue;
                    }
                    for prior in &stack_seen[..stack_len] {
                        let prior_encoded = encode_canonical_value(prior)
                            .map_err(|_| QueryExecutionError::InvalidProgram)?;
                        distinct.insert(prior_encoded);
                    }
                }
                let encoded = encode_canonical_value(value)
                    .map_err(|_| QueryExecutionError::InvalidProgram)?;
                if !distinct.contains(&encoded) {
                    if distinct_count >= fuel.maximum_distinct_values_per_measure {
                        return Err(QueryExecutionError::BoundExceeded);
                    }
                    fuel.consume_state(
                        encoded
                            .len()
                            .checked_add(32)
                            .ok_or(QueryExecutionError::BoundExceeded)?,
                    )?;
                    distinct.insert(encoded);
                    distinct_count += 1;
                }
            }
            let count =
                u64::try_from(distinct_count).map_err(|_| QueryExecutionError::BoundExceeded)?;
            Ok(Some(QueryAggregateCell::Canonical(CanonicalValue::U64(
                count,
            ))))
        }
        AggregateSemanticIdentityV1::Mean => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let scale = match preparsed_scale {
                Some(scale) => scale,
                None => aggregate_mean_scale(measure.result_type())?,
            };
            let mut coefficient = 0_i128;
            let mut count = 0_u64;
            for row in active_rows(all_rows, active_indices) {
                let value = aggregate_input_value(aggregate, row, input_field_pos, field)?;
                let contribution = match value {
                    CanonicalValue::Null => continue,
                    CanonicalValue::I64(value) => i128::from(*value),
                    CanonicalValue::U64(value) => i128::from(*value),
                    CanonicalValue::Decimal(value) if value.spec().scale() == scale => {
                        value.coefficient()
                    }
                    CanonicalValue::Money(value) if value.amount().spec().scale() == scale => {
                        value.amount().coefficient()
                    }
                    _ => return Err(QueryExecutionError::InvalidProgram),
                };
                coefficient = coefficient
                    .checked_add(contribution)
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
                count = count
                    .checked_add(1)
                    .ok_or(QueryExecutionError::AggregateOverflow)?;
            }
            fuel.consume_state(
                std::mem::size_of::<i128>()
                    + std::mem::size_of::<u8>()
                    + std::mem::size_of::<u64>(),
            )?;
            Ok(Some(QueryAggregateCell::ExactMean {
                coefficient,
                scale,
                count,
            }))
        }
        AggregateSemanticIdentityV1::Any | AggregateSemanticIdentityV1::All => {
            let field = measure
                .input_field()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let mut result = semantic == AggregateSemanticIdentityV1::All;
            for row in active_rows(all_rows, active_indices) {
                let value = match aggregate_input_value(aggregate, row, input_field_pos, field)? {
                    CanonicalValue::Null => continue,
                    CanonicalValue::Bool(value) => *value,
                    _ => return Err(QueryExecutionError::InvalidProgram),
                };
                if semantic == AggregateSemanticIdentityV1::Any {
                    result |= value;
                } else {
                    result &= value;
                }
            }
            fuel.consume_state(std::mem::size_of::<bool>())?;
            Ok(Some(QueryAggregateCell::Canonical(CanonicalValue::Bool(
                result,
            ))))
        }
        AggregateSemanticIdentityV1::ExactCount => Err(QueryExecutionError::InvalidProgram),
    }
}

#[inline]
fn aggregate_input_value<'a>(
    aggregate: &OperationalAggregateV1,
    row: &'a QueryRow,
    pos: Option<usize>,
    field: &str,
) -> Result<&'a CanonicalValue, QueryExecutionError> {
    row.field_at(pos, field)
        .ok_or_else(|| QueryExecutionError::MissingField {
            entity: aggregate.source_entity().to_owned(),
            field: field.to_owned(),
        })
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

fn aggregate_mean_scale(result_type: &NamedTypeSchema) -> Result<u8, QueryExecutionError> {
    let NamedTypeSchema::Record(fields) = result_type else {
        return Err(QueryExecutionError::InvalidProgram);
    };
    let total = fields
        .iter()
        .find(|field| field.name() == "total")
        .ok_or(QueryExecutionError::InvalidProgram)?;
    aggregate_sum_scale(total.value_type())
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
        PageBound::BoundedParameter { name, maximum } => match parameters.get(name) {
            Some(CanonicalValue::U64(value))
                if *value > 0 && *value <= *maximum && page_take_within_scan_bound(*value) =>
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
                    QueryPredicateValue::CandidateBinding { name } => program
                        .surface()
                        .candidates()
                        .iter()
                        .find(|candidate| candidate.name() == name)
                        .is_some_and(|candidate| {
                            (0..candidate.sources().len()).any(|source_index| {
                                candidate_source_binding_name(name, source_index).as_deref()
                                    == Some(binding)
                            })
                        }),
                    QueryPredicateValue::Parameter(_)
                    | QueryPredicateValue::Literal(_)
                    | QueryPredicateValue::EnumVariant { .. } => false,
                })
            || program
                .surface()
                .aggregates()
                .iter()
                .filter(|agg| agg.source_binding() == later.binding())
                .any(|agg| {
                    agg.parent_binding() == Some(binding)
                        || agg.having().iter().any(|pred| {
                            matches!(
                                pred.value(),
                                QueryPredicateValue::BindingField {
                                    binding: source,
                                    ..
                                } if source == binding
                            )
                        })
                        || agg.measures().iter().any(|measure| {
                            measure.filter().iter().any(|pred| {
                                matches!(
                                    pred.value(),
                                    QueryPredicateValue::BindingField {
                                        binding: source,
                                        ..
                                    } if source == binding
                                )
                            })
                        })
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
    for (name, value) in snapshot.fields.as_ref() {
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
    if let Some(covered) = &snapshot.covered_result {
        bytes = bytes
            .checked_add(covered.result_name.len() as u64)
            .and_then(|value| value.checked_add(covered.entity.len() as u64))
            .and_then(|value| value.checked_add(128))
            .ok_or(QueryExecutionError::BoundExceeded)?;
        for field in &covered.fields {
            bytes = bytes
                .checked_add(field.len() as u64)
                .and_then(|value| value.checked_add(32))
                .ok_or(QueryExecutionError::BoundExceeded)?;
        }
        for row in &covered.rows {
            for value in row {
                bytes = bytes
                    .checked_add(
                        canonical_value_encoded_len(value)
                            .map_err(|_| QueryExecutionError::InvalidProgram)?
                            as u64,
                    )
                    .and_then(|value| value.checked_add(32))
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

fn encoded_query_rows(mut bytes: u64, rows: &[QueryRow]) -> Result<u64, QueryExecutionError> {
    for row in rows {
        bytes = bytes
            .checked_add(row.entity.len() as u64)
            .and_then(|value| value.checked_add(64))
            .ok_or(QueryExecutionError::BoundExceeded)?;
        if let Some(fields_bytes) = row.fields_encoded_bytes {
            bytes = bytes
                .checked_add(fields_bytes)
                .ok_or(QueryExecutionError::BoundExceeded)?;
        } else {
            for (field, value) in row.fields.as_ref() {
                let encoded_len = canonical_value_encoded_len(value)
                    .map_err(|_| QueryExecutionError::InvalidProgram)?;
                bytes = bytes
                    .checked_add(field.len() as u64)
                    .and_then(|value| value.checked_add(encoded_len as u64))
                    .and_then(|value| value.checked_add(160))
                    .ok_or(QueryExecutionError::BoundExceeded)?;
            }
        }
        for (field, nested) in &row.nested {
            bytes = bytes
                .checked_add(field.len() as u64)
                .and_then(|value| value.checked_add(96))
                .ok_or(QueryExecutionError::BoundExceeded)?;
            bytes = encoded_query_rows(bytes, nested)?;
        }
        for nested in row.nested_aggregates.values() {
            bytes = encoded_aggregate_rows(bytes, nested)?;
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
        if let Some(fields_bytes) = row.fields_encoded_bytes {
            bytes = bytes
                .checked_add(fields_bytes)
                .ok_or(QueryExecutionError::BoundExceeded)?;
        } else {
            for (field, value) in row.fields.as_ref() {
                let encoded_len = match value {
                    QueryAggregateCell::Canonical(value) => canonical_value_encoded_len(value)
                        .map_err(|_| QueryExecutionError::InvalidProgram)?,
                    QueryAggregateCell::ExactDecimal { .. } => 18,
                    // Includes the structural `total`/`count` record framing in
                    // addition to the 25 payload bytes.
                    QueryAggregateCell::ExactMean { .. } => 80,
                };
                bytes = bytes
                    .checked_add(field.len() as u64)
                    .and_then(|value| value.checked_add(encoded_len as u64))
                    .and_then(|value| value.checked_add(160))
                    .ok_or(QueryExecutionError::BoundExceeded)?;
            }
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
                Some(_) => {
                    return Err(QueryExecutionError::InvalidParameter {
                        parameter: name.clone(),
                    });
                }
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
        QueryRowLimit::BoundedParameter {
            name,
            maximum,
            default,
        } => {
            let value = match parameters.get(name) {
                Some(CanonicalValue::U64(value)) => *value,
                Some(_) => {
                    return Err(QueryExecutionError::InvalidParameter {
                        parameter: name.clone(),
                    });
                }
                None => default.ok_or_else(|| QueryExecutionError::MissingParameter {
                    parameter: name.clone(),
                })?,
            };
            if value == 0
                || value > *maximum
                || !page_take_within_scan_bound(value)
                || value > step.maximum_rows()
            {
                return Err(QueryExecutionError::InvalidParameter {
                    parameter: name.clone(),
                });
            }
            value
        }
        QueryRowLimit::CandidateComplete { maximum } => u64::from(*maximum),
    };
    if limit == 0
        || limit > step.maximum_rows()
        || (!matches!(step.row_limit(), QueryRowLimit::CandidateComplete { .. })
            && !page_take_within_scan_bound(limit))
        || limit > MAX_QUERY_SCANNED_ROWS
    {
        return Err(QueryExecutionError::BoundExceeded);
    }
    Ok(limit)
}

#[allow(clippy::too_many_arguments)]
fn execute_expansion_step<V: QueryReadView>(
    program: &QueryAccessProgramV1,
    prepared_plan: &Arc<PreparedExecutorProgramPlan>,
    prepared_step: Option<&PreparedExecutorStepPlan>,
    aggregates: &[OperationalAggregateV1],
    step: &QueryAccessStep,
    parameters: &QueryParameters,
    after: Option<&[u8]>,
    limit: u64,
    view: &mut V,
    policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    fuel: &mut QueryExecutionFuel,
    bindings: &BTreeMap<String, QueryStepRows>,
    result_fields: &mut BTreeMap<String, QueryResultValue>,
    index_epochs: &mut BTreeMap<String, u64>,
    selected_names: &[Arc<str>],
) -> Result<(), QueryExecutionError> {
    let QueryAccessKind::ExpansionIndex {
        index,
        driver_binding,
        per_driver_maximum,
        product_maximum,
        ..
    } = step.access()
    else {
        return Err(QueryExecutionError::InvalidProgram);
    };
    let mut parent_aggregates = Vec::new();
    for (agg_idx, aggregate) in aggregates.iter().enumerate() {
        if aggregate.source_binding() != step.binding()
            || aggregate.parent_binding() != Some(driver_binding.as_str())
        {
            continue;
        }
        let prepared_agg = prepared_plan
            .aggregates
            .get(agg_idx)
            .and_then(Option::as_ref);
        let fallback_selected = if prepared_agg.is_none() {
            Some(selected_aggregate_fields(program, aggregate)?)
        } else {
            None
        };
        parent_aggregates.push((aggregate, prepared_agg, fallback_selected));
    }
    if after.is_some()
        || limit != u64::from(*per_driver_maximum)
        || *product_maximum != step.maximum_rows()
        || (step.result_names().is_empty() && parent_aggregates.is_empty())
    {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let drivers = bindings
        .get(driver_binding)
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let admitted_product = u64::try_from(drivers.len())
        .ok()
        .and_then(|drivers| drivers.checked_mul(limit))
        .ok_or(QueryExecutionError::BoundExceeded)?;
    if admitted_product > *product_maximum {
        return Err(QueryExecutionError::BoundExceeded);
    }

    let (template_predicates, driver_slots) = if drivers.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        bind_driver_template_predicates(
            step,
            parameters,
            bindings,
            program.surface().candidates(),
            driver_binding,
            drivers.first(),
        )?
    };
    let driver_step = program
        .steps()
        .iter()
        .find(|candidate| candidate.binding() == driver_binding)
        .ok_or(QueryExecutionError::InvalidProgram)?;
    if driver_step.result_names().is_empty() {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let head = view.application_head();
    let expansion_cache_probe = if policy.is_none()
        && head > 0
        && driver_step.result_names().len() == 1
        && let Some(drivers_arc) = drivers.shared_arc()
        && !drivers_arc.is_empty()
        && parent_aggregates.iter().all(|(aggregate, prepared_agg, _)| {
            matches!(aggregate.maximum_groups(), PageBound::Literal(v) if *v > 0)
                && prepared_agg.is_some_and(|plan| {
                    plan.all_measure_filters_static
                        && (aggregate.having().is_empty() || plan.static_bound_having.is_some())
                })
        })
        && result_fields
            .get(&driver_step.result_names()[0])
            .is_some_and(|val| {
                matches!(
                    val,
                    QueryResultValue::Many(rows)
                        if rows.len() == drivers_arc.len()
                            && rows.iter().all(|d| d.nested.is_empty() && d.nested_aggregates.is_empty())
                )
            })
    {
        let plan_ptr = Arc::as_ptr(prepared_plan) as usize;
        let step_ptr = std::ptr::from_ref(step) as usize;
        let drivers_ptr = Arc::as_ptr(drivers_arc).cast::<QueryRow>() as usize;
        let drivers_len = drivers_arc.len();
        if let Some((
            total_scanned_rows,
            total_point_reads,
            total_agg_projected,
            total_rows,
            observed_epoch,
            completed_drivers,
        )) = LOCAL_EXPANSION_STEP_CACHE.with(|cell| {
            let guard = cell.try_borrow().ok()?;
            guard
                .entries
                .iter()
                .find(|entry| {
                    entry.plan_ptr == plan_ptr
                        && entry.step_ptr == step_ptr
                        && entry.drivers_ptr == drivers_ptr
                        && entry.drivers_len == drivers_len
                        && entry.head == head
                        && entry.limit == limit
                        && entry.template_predicates == template_predicates
                })
                .map(|entry| {
                    (
                        entry.total_scanned_rows,
                        entry.total_point_reads,
                        entry.total_agg_projected,
                        entry.total_rows,
                        entry.observed_epoch,
                        Arc::clone(&entry.completed_drivers),
                    )
                })
        }) {
            fuel.scans(total_scanned_rows)?;
            fuel.points(total_point_reads)?;
            fuel.projected_values(total_agg_projected)?;
            fuel.intermediates(total_rows)?;
            let projected_values = total_rows
                .checked_mul(step.selected_fields().len() as u64)
                .and_then(|value| value.checked_mul(step.result_names().len() as u64))
                .ok_or(QueryExecutionError::BoundExceeded)?;
            fuel.projected_values(projected_values)?;
            if let Some(epoch) = observed_epoch {
                let epoch_key =
                    if let Some(key) = prepared_step.and_then(|s| s.index_epoch_key.as_ref()) {
                        key.clone()
                    } else {
                        let mut epoch_key =
                            String::with_capacity(step.entity().len() + 1 + index.len());
                        epoch_key.push_str(step.entity());
                        epoch_key.push('.');
                        epoch_key.push_str(index);
                        epoch_key
                    };
                if index_epochs.insert(epoch_key, epoch).is_some() {
                    return Err(QueryExecutionError::InvalidProgram);
                }
            }
            let driver_result_name = &driver_step.result_names()[0];
            let Some(QueryResultValue::Many(result_drivers)) =
                result_fields.get_mut(driver_result_name)
            else {
                return Err(QueryExecutionError::InvalidProgram);
            };
            *result_drivers = completed_drivers.as_ref().to_vec();
            return Ok(());
        }
        Some((
            Arc::clone(drivers_arc),
            plan_ptr,
            step_ptr,
            drivers_ptr,
            drivers_len,
            template_predicates.clone(),
        ))
    } else {
        None
    };

    let mut groups = Vec::with_capacity(drivers.len());
    let mut per_driver_aggregates = Vec::with_capacity(drivers.len());
    let mut total_rows = 0_u64;
    let mut total_scanned_rows = 0_u64;
    let mut total_point_reads = 0_u64;
    let mut total_agg_projected = 0_u64;
    let mut observed_epoch = None;
    let pred_positions = prepared_step.and_then(|s| s.predicate_positions.as_deref());
    let mut predicates = template_predicates;
    let mut agg_scratch = if !parent_aggregates.is_empty() {
        LOCAL_AGGREGATE_SCRATCH.with(|cell| {
            cell.try_borrow_mut()
                .map(|mut s| std::mem::take(&mut *s))
                .unwrap_or_default()
        })
    } else {
        AggregateScratch::default()
    };
    let loop_result = (|| -> Result<(), QueryExecutionError> {
        for driver in drivers {
            for &(slot_index, driver_field, field_pos) in &driver_slots {
                predicates[slot_index].value = driver
                    .field_at(field_pos, driver_field)
                    .cloned()
                    .unwrap_or(CanonicalValue::Null);
            }
            let mut page = view
                .scan(step, &predicates, limit, None, false, policy)
                .map_err(|error| map_view_error(view, &error))?;
            let page_rows_len = page.rows_len() as u64;
            if page_rows_len > limit
                || page.scanned_rows > MAX_QUERY_SCANNED_ROWS
                || page.point_reads != page_rows_len
                || page_rows_len > page.scanned_rows
                || page.continuation.is_some()
                || (!page.predicates_verified
                    && page.rows_slice().iter().any(|row| {
                        row.entity() != step.entity()
                            || predicates_match_at_positions(row, &predicates, pred_positions)
                                != Ok(true)
                    }))
            {
                return Err(QueryExecutionError::BoundExceeded);
            }
            match observed_epoch {
                None => observed_epoch = Some(page.epoch),
                Some(epoch) if epoch == page.epoch => {}
                Some(_) => return Err(QueryExecutionError::BackendIntegrity),
            }
            fuel.scans(page.scanned_rows)?;
            fuel.points(page.point_reads)?;
            total_scanned_rows = total_scanned_rows.saturating_add(page.scanned_rows);
            total_point_reads = total_point_reads.saturating_add(page.point_reads);
            total_rows = total_rows
                .checked_add(page_rows_len)
                .ok_or(QueryExecutionError::BoundExceeded)?;
            if total_rows > *product_maximum {
                return Err(QueryExecutionError::BoundExceeded);
            }
            let page_rows = page.rows_slice();
            let mut driver_agg_entries = Vec::with_capacity(parent_aggregates.len());
            for (aggregate, prepared_agg, fallback_selected) in &parent_aggregates {
                let selected = match (prepared_agg, fallback_selected) {
                    (Some(agg_plan), _) => &agg_plan.selected,
                    (None, Some(selected)) => selected,
                    (None, None) => unreachable!(),
                };
                if selected.is_empty() {
                    continue;
                }
                let agg_rows = evaluate_operational_aggregate_with_scratch(
                    program,
                    Some(prepared_plan),
                    aggregate,
                    selected,
                    *prepared_agg,
                    page_rows,
                    page.shared_rows.as_ref(),
                    parameters,
                    bindings,
                    Some((driver_binding.as_str(), driver)),
                    &mut agg_scratch,
                )?;
                let projected = u64::try_from(agg_rows.len())
                    .ok()
                    .and_then(|groups| groups.checked_mul(selected.len() as u64))
                    .ok_or(QueryExecutionError::BoundExceeded)?;
                fuel.projected_values(projected)?;
                total_agg_projected = total_agg_projected.saturating_add(projected);
                let agg_name = match prepared_agg {
                    Some(agg_plan) => Arc::clone(&agg_plan.aggregate_entity),
                    None => Arc::<str>::from(aggregate.name()),
                };
                driver_agg_entries.push((agg_name, agg_rows));
            }
            per_driver_aggregates.push(driver_agg_entries);
            if step.result_names().is_empty() {
                if !page
                    .rows_slice()
                    .first()
                    .is_some_and(|row| row.selected_prevalidated)
                {
                    for row in page.rows_slice() {
                        row.validate_selected(selected_names)?;
                    }
                }
                groups.push(Vec::new());
            } else {
                let target_names = prepared_step
                    .and_then(|s| s.output_selected_names.as_deref())
                    .unwrap_or(selected_names);
                let need_validate_selected = target_names.len() != selected_names.len()
                    && !page
                        .rows_slice()
                        .first()
                        .is_some_and(|row| row.selected_prevalidated);
                if let Some(shared) = page.shared_rows.as_ref() {
                    let mut group = Vec::with_capacity(shared.len());
                    for row in shared.iter() {
                        if need_validate_selected {
                            row.validate_selected(selected_names)?;
                        }
                        group.push(row.project_clone(target_names)?);
                    }
                    groups.push(group);
                } else {
                    for row in &mut page.rows {
                        if need_validate_selected {
                            row.validate_selected(selected_names)?;
                        }
                        row.project_in_place(target_names)?;
                    }
                    groups.push(page.rows);
                }
            }
        }
        Ok(())
    })();
    if !parent_aggregates.is_empty() {
        LOCAL_AGGREGATE_SCRATCH.with(|cell| {
            if let Ok(mut slot) = cell.try_borrow_mut() {
                *slot = agg_scratch;
            }
        });
    }
    loop_result?;
    fuel.intermediates(total_rows)?;
    let projected_values = total_rows
        .checked_mul(step.selected_fields().len() as u64)
        .and_then(|value| value.checked_mul(step.result_names().len() as u64))
        .ok_or(QueryExecutionError::BoundExceeded)?;
    fuel.projected_values(projected_values)?;

    if let Some(epoch) = observed_epoch {
        let epoch_key = if let Some(key) = prepared_step.and_then(|s| s.index_epoch_key.as_ref()) {
            key.clone()
        } else {
            let mut epoch_key = String::with_capacity(step.entity().len() + 1 + index.len());
            epoch_key.push_str(step.entity());
            epoch_key.push('.');
            epoch_key.push_str(index);
            epoch_key
        };
        if index_epochs.insert(epoch_key, epoch).is_some() {
            return Err(QueryExecutionError::InvalidProgram);
        }
    }

    if driver_step.result_names().len() == 1 {
        let driver_result_name = &driver_step.result_names()[0];
        let QueryResultValue::Many(result_drivers) = result_fields
            .get_mut(driver_result_name)
            .ok_or(QueryExecutionError::InvalidProgram)?
        else {
            return Err(QueryExecutionError::InvalidProgram);
        };
        if result_drivers.len() != groups.len()
            || result_drivers.len() != per_driver_aggregates.len()
        {
            return Err(QueryExecutionError::InvalidProgram);
        }
        let fallback_nested_arcs;
        let nested_arcs: &[Arc<str>] = if let Some(prepared_step) = prepared_step {
            &prepared_step.nested_result_arcs
        } else {
            fallback_nested_arcs = step
                .result_names()
                .iter()
                .map(|n| Arc::<str>::from(n.as_str()))
                .collect::<Vec<_>>();
            &fallback_nested_arcs
        };
        for (driver, (targets, agg_entries)) in result_drivers
            .iter_mut()
            .zip(groups.into_iter().zip(per_driver_aggregates))
        {
            if let Some((last_name, prior_names)) = nested_arcs.split_last() {
                for nested_name in prior_names {
                    driver.attach_nested(Arc::clone(nested_name), targets.clone())?;
                }
                driver.attach_nested(Arc::clone(last_name), targets)?;
            }
            for (agg_name, agg_rows) in agg_entries {
                driver.attach_nested_aggregate(agg_name, agg_rows)?;
            }
        }
        if let Some((drivers_pin, plan_ptr, step_ptr, drivers_ptr, drivers_len, probe_template)) =
            expansion_cache_probe
        {
            let completed_drivers: Arc<[QueryRow]> = Arc::from(result_drivers.as_slice());
            LOCAL_EXPANSION_STEP_CACHE.with(|cell| {
                if let Ok(mut guard) = cell.try_borrow_mut() {
                    guard.insert(CachedExpansionStep {
                        _plan_pin: Arc::clone(prepared_plan),
                        _drivers_pin: drivers_pin,
                        plan_ptr,
                        step_ptr,
                        drivers_ptr,
                        drivers_len,
                        head,
                        limit,
                        template_predicates: probe_template,
                        total_scanned_rows,
                        total_point_reads,
                        total_agg_projected,
                        total_rows,
                        observed_epoch,
                        completed_drivers,
                    });
                }
            });
        }
        return Ok(());
    }
    for driver_result_name in driver_step.result_names() {
        let QueryResultValue::Many(result_drivers) = result_fields
            .get_mut(driver_result_name)
            .ok_or(QueryExecutionError::InvalidProgram)?
        else {
            return Err(QueryExecutionError::InvalidProgram);
        };
        if result_drivers.len() != groups.len()
            || result_drivers.len() != per_driver_aggregates.len()
        {
            return Err(QueryExecutionError::InvalidProgram);
        }
        for (driver, (targets, agg_entries)) in result_drivers
            .iter_mut()
            .zip(groups.iter().zip(&per_driver_aggregates))
        {
            for nested_name in step.result_names() {
                driver.attach_nested(Arc::<str>::from(nested_name.as_str()), targets.clone())?;
            }
            for (agg_name, agg_rows) in agg_entries {
                driver.attach_nested_aggregate(Arc::clone(agg_name), agg_rows.clone())?;
            }
        }
    }
    Ok(())
}

struct PreparedAggregateMeasurePlan {
    measure_index: usize,
    scale: Option<u8>,
    input_field_pos: Option<usize>,
    filter_slot: Option<usize>,
    filter_positions: Option<Vec<usize>>,
    static_bound_filter: Option<Arc<[BoundPredicate]>>,
    shares_quantile_input: bool,
}

struct PreparedAggregatePlan {
    selected: BTreeMap<String, String>,
    aggregate_entity: Arc<str>,
    group_key_arcs: Vec<Option<Arc<str>>>,
    group_key_positions: Vec<Option<usize>>,
    active_measures: Vec<PreparedAggregateMeasurePlan>,
    num_unique_filters: usize,
    having_slots: Vec<usize>,
    order_slots: Vec<usize>,
    output_slots: Vec<(usize, Arc<str>)>,
    static_bound_having: Option<Arc<[BoundPredicate]>>,
    all_measure_filters_static: bool,
}

fn step_row_field_names(
    program: &QueryAccessProgramV1,
    step: &QueryAccessStep,
) -> Option<Vec<String>> {
    if let Some(layout) = step.covered_result_layout() {
        return Some(
            layout
                .fields()
                .iter()
                .map(|field| field.name().to_owned())
                .collect(),
        );
    }
    let access = program.internal_entity_access(step.entity())?;
    let mut names = access
        .internal_fields()
        .map(|(name, _)| name.to_owned())
        .collect::<Vec<_>>();
    names.sort();
    Some(names)
}

impl PreparedAggregatePlan {
    fn build(
        program: Option<&QueryAccessProgramV1>,
        aggregate: &OperationalAggregateV1,
        selected: BTreeMap<String, String>,
    ) -> Self {
        let source_field_names = program.and_then(|prog| {
            let source_step = prog
                .steps()
                .iter()
                .find(|s| s.binding() == aggregate.source_binding())?;
            step_row_field_names(prog, source_step)
        });
        let resolve_pos = |field_name: &str| -> Option<usize> {
            source_field_names
                .as_ref()?
                .binary_search_by(|candidate| candidate.as_str().cmp(field_name))
                .ok()
        };
        let needed_measures_in_having: BTreeSet<&str> = aggregate
            .having()
            .iter()
            .map(QueryPredicate::field)
            .collect();
        let needed_in_order: BTreeSet<&str> = aggregate
            .order_by()
            .iter()
            .map(QueryRootOrderTermV1::field)
            .collect();
        let mut slot_by_name = BTreeMap::<String, usize>::new();
        let group_key_arcs = aggregate
            .group_keys()
            .iter()
            .enumerate()
            .map(|(slot_idx, key)| {
                let field = key.field();
                if selected.contains_key(field)
                    || needed_measures_in_having.contains(field)
                    || needed_in_order.contains(field)
                {
                    slot_by_name.insert(field.to_owned(), slot_idx);
                    Some(Arc::<str>::from(field))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let group_key_positions = aggregate
            .group_keys()
            .iter()
            .map(|key| resolve_pos(key.source_field()))
            .collect::<Vec<_>>();
        let num_group_keys = group_key_arcs.len();
        let mut active_measures = Vec::with_capacity(aggregate.measures().len());
        let mut unique_filters: Vec<&[QueryPredicate]> = Vec::new();
        let mut all_measure_filters_static = true;
        for (measure_index, measure) in aggregate.measures().iter().enumerate() {
            let alias = measure.alias();
            if !selected.contains_key(alias)
                && !needed_measures_in_having.contains(alias)
                && !needed_in_order.contains(alias)
            {
                continue;
            }
            let scale = match measure.function() {
                OperationalAggregateFunctionV1::Stddev => {
                    aggregate_sum_scale(measure.result_type()).ok()
                }
                _ => match measure.function().semantic_identity() {
                    AggregateSemanticIdentityV1::Sum => {
                        aggregate_sum_scale(measure.result_type()).ok()
                    }
                    AggregateSemanticIdentityV1::Mean => {
                        aggregate_mean_scale(measure.result_type()).ok()
                    }
                    _ => None,
                },
            };
            let input_field_pos = measure.input_field().and_then(&resolve_pos);
            let (filter_slot, filter_positions, static_bound_filter) = if measure
                .filter()
                .is_empty()
            {
                (None, None, None)
            } else {
                let f_slot =
                    if let Some(pos) = unique_filters.iter().position(|f| *f == measure.filter()) {
                        pos
                    } else {
                        let pos = unique_filters.len();
                        unique_filters.push(measure.filter());
                        pos
                    };
                let positions = measure
                    .filter()
                    .iter()
                    .map(|pred| resolve_pos(pred.field()))
                    .collect::<Option<Vec<usize>>>();
                let static_bound =
                    if let Some(bound) = try_bind_static_aggregate_predicates(measure.filter()) {
                        Some(Arc::from(bound))
                    } else {
                        all_measure_filters_static = false;
                        None
                    };
                (Some(f_slot), positions, static_bound)
            };
            let slot_idx = num_group_keys + active_measures.len();
            slot_by_name.insert(alias.to_owned(), slot_idx);
            active_measures.push(PreparedAggregateMeasurePlan {
                measure_index,
                scale,
                input_field_pos,
                filter_slot,
                filter_positions,
                static_bound_filter,
                shares_quantile_input: false,
            });
        }
        for i in 0..active_measures.len() {
            let m_i = &aggregate.measures()[active_measures[i].measure_index];
            if matches!(
                m_i.function(),
                OperationalAggregateFunctionV1::Median
                    | OperationalAggregateFunctionV1::P90
                    | OperationalAggregateFunctionV1::P95
                    | OperationalAggregateFunctionV1::P99
            ) {
                let shares = active_measures.iter().enumerate().any(|(j, other)| {
                    if i == j {
                        return false;
                    }
                    let m_j = &aggregate.measures()[other.measure_index];
                    matches!(
                        m_j.function(),
                        OperationalAggregateFunctionV1::Median
                            | OperationalAggregateFunctionV1::P90
                            | OperationalAggregateFunctionV1::P95
                            | OperationalAggregateFunctionV1::P99
                    ) && m_i.input_field() == m_j.input_field()
                        && active_measures[i].filter_slot == other.filter_slot
                });
                active_measures[i].shares_quantile_input = shares;
            }
        }
        let num_unique_filters = unique_filters.len();
        let having_slots = aggregate
            .having()
            .iter()
            .map(|pred| {
                slot_by_name
                    .get(pred.field())
                    .copied()
                    .unwrap_or(usize::MAX)
            })
            .collect();
        let order_slots = aggregate
            .order_by()
            .iter()
            .map(|term| {
                slot_by_name
                    .get(term.field())
                    .copied()
                    .unwrap_or(usize::MAX)
            })
            .collect();
        let output_slots = selected
            .iter()
            .filter_map(|(source_name, output_name)| {
                slot_by_name
                    .get(source_name.as_str())
                    .map(|&slot| (slot, Arc::<str>::from(output_name.as_str())))
            })
            .collect();
        let static_bound_having = if aggregate.having().is_empty() {
            None
        } else {
            try_bind_static_aggregate_predicates(aggregate.having()).map(Arc::from)
        };
        Self {
            selected,
            aggregate_entity: Arc::<str>::from(aggregate.name()),
            group_key_arcs,
            group_key_positions,
            active_measures,
            num_unique_filters,
            having_slots,
            order_slots,
            output_slots,
            static_bound_having,
            all_measure_filters_static,
        }
    }
}

struct PreparedExecutorStepPlan {
    selected_names: Arc<[Arc<str>]>,
    output_selected_names: Option<Arc<[Arc<str>]>>,
    predicate_positions: Option<Vec<usize>>,
    predicate_is_key: Option<Vec<bool>>,
    order_positions: Option<Vec<usize>>,
    key_positions: Option<Vec<usize>>,
    retained_later: bool,
    index_epoch_key: Option<String>,
    nested_result_arcs: Vec<Arc<str>>,
}

struct PreparedExecutorProgramPlan {
    steps: Vec<PreparedExecutorStepPlan>,
    aggregates: Vec<Option<PreparedAggregatePlan>>,
}

#[doc(hidden)]
pub fn step_output_selected_fields_v1(
    program: &QueryAccessProgramV1,
    step: &QueryAccessStep,
) -> Option<Vec<String>> {
    if step.result_names().is_empty() {
        return None;
    }
    let branch = program.surface().schemas().results().first()?;
    fn unwrap_record(ty: &NamedTypeSchema) -> Option<&[riffdb_query_ir::NamedFieldSchema]> {
        match ty {
            NamedTypeSchema::Record(fields) => Some(fields.as_slice()),
            NamedTypeSchema::Optional(inner) => match inner.as_ref() {
                NamedTypeSchema::Record(fields) => Some(fields.as_slice()),
                _ => None,
            },
            NamedTypeSchema::List { element, .. } => match element.as_ref() {
                NamedTypeSchema::Record(fields) => Some(fields.as_slice()),
                _ => None,
            },
            _ => None,
        }
    }
    let mut needed_output = BTreeSet::new();
    match step.access() {
        QueryAccessKind::ExpansionIndex { driver_binding, .. } => {
            let driver_step = program
                .steps()
                .iter()
                .find(|candidate| candidate.binding() == driver_binding)?;
            if driver_step.result_names().is_empty() {
                return None;
            }
            for driver_res in driver_step.result_names() {
                let driver_field = branch
                    .fields()
                    .iter()
                    .find(|field| field.name() == driver_res)?;
                let parent_record = unwrap_record(driver_field.value_type())?;
                for res_name in step.result_names() {
                    let nested_field = parent_record
                        .iter()
                        .find(|field| field.name() == res_name)?;
                    let record_fields = unwrap_record(nested_field.value_type())?;
                    for field in record_fields {
                        if step
                            .selected_fields()
                            .iter()
                            .any(|selected| selected == field.name())
                        {
                            needed_output.insert(field.name().to_owned());
                        } else {
                            return None;
                        }
                    }
                }
            }
        }
        _ => {
            for res_name in step.result_names() {
                let root_field = branch
                    .fields()
                    .iter()
                    .find(|field| field.name() == res_name)?;
                let record_fields = unwrap_record(root_field.value_type())?;
                for field in record_fields {
                    if step
                        .selected_fields()
                        .iter()
                        .any(|selected| selected == field.name())
                    {
                        needed_output.insert(field.name().to_owned());
                    } else {
                        let is_nested_step = program.steps().iter().any(|candidate| {
                            matches!(
                                candidate.access(),
                                QueryAccessKind::ExpansionIndex { driver_binding, .. }
                                    if driver_binding == step.binding()
                            ) && candidate
                                .result_names()
                                .iter()
                                .any(|nested_res| nested_res == field.name())
                        });
                        let is_nested_agg = program.surface().aggregates().iter().any(|agg| {
                            agg.parent_binding() == Some(step.binding())
                                && agg.name() == field.name()
                        });
                        if !is_nested_step && !is_nested_agg {
                            return None;
                        }
                    }
                }
            }
        }
    }
    if !needed_output.is_empty() && needed_output.len() < step.selected_fields().len() {
        Some(needed_output.into_iter().collect())
    } else {
        None
    }
}

fn try_bind_static_aggregate_predicates(
    predicates: &[QueryPredicate],
) -> Option<Vec<BoundPredicate>> {
    let mut bound = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        if predicate.operator() == QueryPredicateOperator::In {
            return None;
        }
        let value = match predicate.value() {
            QueryPredicateValue::Literal(literal) => literal_value(literal).ok()?,
            QueryPredicateValue::EnumVariant {
                type_id,
                variant_id,
                ..
            } => CanonicalValue::Enum {
                type_id: *type_id,
                variant_id: *variant_id,
            },
            QueryPredicateValue::Parameter(_)
            | QueryPredicateValue::BindingField { .. }
            | QueryPredicateValue::BindingFieldSet { .. }
            | QueryPredicateValue::CandidateBinding { .. } => return None,
        };
        bound.push(BoundPredicate {
            field: predicate.field().to_owned(),
            operator: predicate.operator(),
            value,
        });
    }
    Some(bound)
}

fn prepared_executor_program_plan(
    program: &QueryAccessProgramV1,
) -> Arc<PreparedExecutorProgramPlan> {
    std::thread_local! {
        static CACHE: std::cell::RefCell<BTreeMap<QueryPlanIdentity, Arc<PreparedExecutorProgramPlan>>> =
            const { std::cell::RefCell::new(BTreeMap::new()) };
    }
    static SHARED_CACHE: std::sync::OnceLock<
        std::sync::RwLock<BTreeMap<QueryPlanIdentity, Arc<PreparedExecutorProgramPlan>>>,
    > = std::sync::OnceLock::new();
    let identity = program.identity();
    if let Some(plan) = CACHE.with(|cache| cache.try_borrow().ok()?.get(&identity).cloned()) {
        return plan;
    }
    let shared = SHARED_CACHE.get_or_init(|| std::sync::RwLock::new(BTreeMap::new()));
    if let Ok(guard) = shared.read()
        && let Some(plan) = guard.get(&identity).cloned()
    {
        drop(guard);
        CACHE.with(|cache| {
            if let Ok(mut local) = cache.try_borrow_mut() {
                if local.len() >= 4096 {
                    local.clear();
                }
                local.insert(identity, Arc::clone(&plan));
            }
        });
        return plan;
    }
    let surface_aggregates = program.surface().aggregates();
    let aggregates = surface_aggregates
        .iter()
        .map(|aggregate| {
            selected_aggregate_fields(program, aggregate)
                .ok()
                .map(|selected| PreparedAggregatePlan::build(Some(program), aggregate, selected))
        })
        .collect::<Vec<_>>();
    let steps = program
        .steps()
        .iter()
        .enumerate()
        .map(|(step_index, step)| {
            let selected_names: Arc<[Arc<str>]> = step
                .selected_fields()
                .iter()
                .map(|name| Arc::<str>::from(name.as_str()))
                .collect::<Vec<_>>()
                .into();
            let output_selected_names: Option<Arc<[Arc<str>]>> =
                step_output_selected_fields_v1(program, step).map(|fields| {
                    fields
                        .into_iter()
                        .map(|name| {
                            selected_names
                                .iter()
                                .find(|candidate| candidate.as_ref() == name.as_str())
                                .cloned()
                                .unwrap_or_else(|| Arc::<str>::from(name.as_str()))
                        })
                        .collect::<Vec<_>>()
                        .into()
                });
            let row_field_names = step_row_field_names(program, step);
            let predicate_positions = row_field_names.as_ref().and_then(|field_names| {
                step.predicates()
                    .iter()
                    .map(|pred| {
                        field_names
                            .binary_search_by(|candidate| candidate.as_str().cmp(pred.field()))
                            .ok()
                    })
                    .collect::<Option<Vec<usize>>>()
            });
            let predicate_is_key = match step.access() {
                QueryAccessKind::CandidateRootHydration { key_fields, .. } => Some(
                    step.predicates()
                        .iter()
                        .map(|pred| key_fields.iter().any(|k| k == pred.field()))
                        .collect::<Vec<bool>>(),
                ),
                _ => None,
            };
            let order_positions = row_field_names.as_ref().and_then(|field_names| {
                let order = match step.access() {
                    QueryAccessKind::PartitionSetIndex { order, .. }
                    | QueryAccessKind::CandidateRootHydration { order, .. } => order,
                    _ => return None,
                };
                order
                    .iter()
                    .map(|term| {
                        field_names
                            .binary_search_by(|candidate| candidate.as_str().cmp(term.field()))
                            .ok()
                    })
                    .collect::<Option<Vec<usize>>>()
            });
            let key_positions = row_field_names.as_ref().and_then(|field_names| {
                let key_fields = match step.access() {
                    QueryAccessKind::PartitionSetIndex { key_fields, .. }
                    | QueryAccessKind::CandidateRootHydration { key_fields, .. }
                    | QueryAccessKind::Point { key_fields }
                    | QueryAccessKind::DependentPointBatch { key_fields, .. } => key_fields,
                    _ => return None,
                };
                key_fields
                    .iter()
                    .map(|field| {
                        field_names
                            .binary_search_by(|candidate| candidate.as_str().cmp(field.as_str()))
                            .ok()
                    })
                    .collect::<Option<Vec<usize>>>()
            });
            let retained_later = binding_referenced_later(program, step_index, step.binding());
            let index_epoch_key = match step.access() {
                QueryAccessKind::Index { index, .. }
                | QueryAccessKind::PartitionSetIndex { index, .. }
                | QueryAccessKind::ExpansionIndex { index, .. } => {
                    let mut epoch_key =
                        String::with_capacity(step.entity().len() + 1 + index.len());
                    epoch_key.push_str(step.entity());
                    epoch_key.push('.');
                    epoch_key.push_str(index);
                    Some(epoch_key)
                }
                _ => None,
            };
            let nested_result_arcs = step
                .result_names()
                .iter()
                .map(|name| Arc::<str>::from(name.as_str()))
                .collect::<Vec<_>>();
            PreparedExecutorStepPlan {
                selected_names,
                output_selected_names,
                predicate_positions,
                predicate_is_key,
                order_positions,
                key_positions,
                retained_later,
                index_epoch_key,
                nested_result_arcs,
            }
        })
        .collect::<Vec<_>>();
    let mut built = Arc::new(PreparedExecutorProgramPlan { steps, aggregates });
    if let Ok(mut guard) = shared.write() {
        if let Some(existing) = guard.get(&identity) {
            built = Arc::clone(existing);
        } else {
            if guard.len() >= 4096 {
                guard.clear();
            }
            guard.insert(identity, Arc::clone(&built));
        }
    }
    CACHE.with(|cache| {
        if let Ok(mut guard) = cache.try_borrow_mut() {
            if guard.len() >= 4096 {
                guard.clear();
            }
            guard.insert(identity, Arc::clone(&built));
        }
    });
    built
}

#[allow(clippy::type_complexity)]
fn bind_driver_template_predicates<'a>(
    step: &'a QueryAccessStep,
    parameters: &QueryParameters,
    bindings: &BTreeMap<String, QueryStepRows>,
    candidates: &[CandidateBindingV1],
    driver_binding: &str,
    first_driver: Option<&QueryRow>,
) -> Result<(Vec<BoundPredicate>, Vec<(usize, &'a str, Option<usize>)>), QueryExecutionError> {
    let mut template = Vec::with_capacity(step.predicates().len());
    let mut driver_slots = Vec::new();
    for (idx, predicate) in step.predicates().iter().enumerate() {
        let value = match predicate.value() {
            QueryPredicateValue::Parameter(name) => {
                parameters.get(name).cloned().ok_or_else(|| {
                    QueryExecutionError::MissingParameter {
                        parameter: name.clone(),
                    }
                })?
            }
            QueryPredicateValue::BindingField { binding, field } => {
                debug_assert!(
                    bindings.contains_key(binding),
                    "binding {binding:?} is read but missing from retained bindings"
                );
                if binding == driver_binding {
                    let field_pos = first_driver
                        .and_then(|d| d.fields.keys().position(|k| k.as_ref() == field.as_str()));
                    driver_slots.push((idx, field.as_str(), field_pos));
                    CanonicalValue::Null
                } else {
                    bindings
                        .get(binding)
                        .and_then(|rows| rows.first())
                        .and_then(|row| row.field(field))
                        .cloned()
                        .unwrap_or(CanonicalValue::Null)
                }
            }
            QueryPredicateValue::BindingFieldSet { .. } => {
                return Err(QueryExecutionError::InvalidProgram);
            }
            QueryPredicateValue::CandidateBinding { name } => {
                bind_candidate_value(name, candidates, bindings)?
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
            match predicate.value() {
                QueryPredicateValue::Parameter(parameter) => {
                    validate_canonical_set(&value).map_err(|()| {
                        QueryExecutionError::InvalidParameter {
                            parameter: parameter.clone(),
                        }
                    })?;
                }
                QueryPredicateValue::CandidateBinding { .. } => {}
                _ => return Err(QueryExecutionError::InvalidProgram),
            }
        }
        if let QueryPredicateValue::Parameter(parameter) = predicate.value() {
            validate_binary_text_interval_parameter(
                step,
                predicate.field(),
                predicate.operator(),
                parameter,
                &value,
            )?;
        }
        template.push(BoundPredicate {
            field: predicate.field().to_owned(),
            operator: predicate.operator(),
            value,
        });
    }
    Ok((template, driver_slots))
}

fn bind_predicates(
    step: &QueryAccessStep,
    parameters: &QueryParameters,
    bindings: &BTreeMap<String, QueryStepRows>,
    candidates: &[CandidateBindingV1],
) -> Result<Vec<BoundPredicate>, QueryExecutionError> {
    bind_predicates_with_driver(step, parameters, bindings, candidates, None)
}

fn bind_predicates_with_driver(
    step: &QueryAccessStep,
    parameters: &QueryParameters,
    bindings: &BTreeMap<String, QueryStepRows>,
    candidates: &[CandidateBindingV1],
    driver: Option<(&str, &QueryRow)>,
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
                    driver
                        .filter(|(driver_binding, _)| *driver_binding == binding)
                        .map(|(_, row)| row)
                        .or_else(|| bindings.get(binding).and_then(|rows| rows.first()))
                        .and_then(|row| row.field(field))
                        .cloned()
                        .unwrap_or(CanonicalValue::Null)
                }
                QueryPredicateValue::BindingFieldSet { .. } => {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                QueryPredicateValue::CandidateBinding { name } => {
                    bind_candidate_value(name, candidates, bindings)?
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
                match predicate.value() {
                    QueryPredicateValue::Parameter(parameter) => {
                        validate_canonical_set(&value).map_err(|()| {
                            QueryExecutionError::InvalidParameter {
                                parameter: parameter.clone(),
                            }
                        })?;
                    }
                    QueryPredicateValue::CandidateBinding { .. } => {}
                    _ => return Err(QueryExecutionError::InvalidProgram),
                }
            }
            if let QueryPredicateValue::Parameter(parameter) = predicate.value() {
                validate_binary_text_interval_parameter(
                    step,
                    predicate.field(),
                    predicate.operator(),
                    parameter,
                    &value,
                )?;
            }
            Ok(BoundPredicate {
                field: predicate.field().to_owned(),
                operator: predicate.operator(),
                value,
            })
        })
        .collect()
}

fn validate_binary_text_interval_parameter(
    step: &QueryAccessStep,
    field: &str,
    operator: QueryPredicateOperator,
    parameter: &str,
    value: &CanonicalValue,
) -> Result<(), QueryExecutionError> {
    if !matches!(
        operator,
        QueryPredicateOperator::NotEqual
            | QueryPredicateOperator::Less
            | QueryPredicateOperator::LessEqual
            | QueryPredicateOperator::Greater
            | QueryPredicateOperator::GreaterEqual
    ) {
        return Ok(());
    }
    let fields = match step.access() {
        QueryAccessKind::Index { fields, .. }
        | QueryAccessKind::PartitionSetIndex { fields, .. }
        | QueryAccessKind::ExpansionIndex { fields, .. } => fields,
        _ => return Ok(()),
    };
    let Some(position) = fields.iter().position(|candidate| candidate == field) else {
        return Err(QueryExecutionError::InvalidProgram);
    };
    let Some(component) = step
        .internal_operational_index_descriptor()
        .and_then(|descriptor| descriptor.component(position, field))
    else {
        return Ok(());
    };
    if component.encoding()
        != riffdb_contract_ir::IndexFieldEncodingV1::TextKey(
            riffdb_contract_ir::TextKeyProfileV1::BinaryUtf8,
        )
    {
        return Ok(());
    }
    let [physical] = component.physical() else {
        return Err(QueryExecutionError::InvalidProgram);
    };
    match value {
        CanonicalValue::String(value)
            if physical
                .value_type()
                .byte_bound()
                .is_some_and(|maximum| value.as_str().len() <= maximum) =>
        {
            Ok(())
        }
        _ => Err(QueryExecutionError::InvalidParameter {
            parameter: parameter.to_owned(),
        }),
    }
}

#[inline]
fn canonical_scalar_encoded_cmp(a: &CanonicalValue, b: &CanonicalValue) -> Option<Ordering> {
    match (a, b) {
        (CanonicalValue::String(sa), CanonicalValue::String(sb)) => {
            Some((sa.len(), sa.as_str().as_bytes()).cmp(&(sb.len(), sb.as_str().as_bytes())))
        }
        (CanonicalValue::Uuid(ua), CanonicalValue::Uuid(ub)) => Some(ua.cmp(ub)),
        (CanonicalValue::U64(ua), CanonicalValue::U64(ub)) => Some(ua.cmp(ub)),
        (CanonicalValue::I64(ia), CanonicalValue::I64(ib)) => {
            Some(ia.to_be_bytes().cmp(&ib.to_be_bytes()))
        }
        _ => None,
    }
}

#[inline]
fn canonical_scalar_encoded_len(v: &CanonicalValue) -> Option<u64> {
    match v {
        CanonicalValue::String(s) if s.len() <= riffdb_types::MAX_CANONICAL_DOCUMENT_BYTES => {
            Some(5 + s.len() as u64)
        }
        CanonicalValue::Uuid(_) => Some(17),
        CanonicalValue::U64(_) | CanonicalValue::I64(_) => Some(9),
        _ => None,
    }
}

fn check_scalar_candidate_bound(
    slice: &[&CanonicalValue],
    maximum_distinct_keys: u16,
    maximum_key_bytes: u64,
) -> Result<(), QueryExecutionError> {
    if slice.len() > usize::from(maximum_distinct_keys) {
        return Err(QueryExecutionError::BoundExceeded);
    }
    let bytes = slice.iter().try_fold(0_u64, |total, val| {
        total.checked_add(canonical_scalar_encoded_len(val)?)
    });
    if bytes.is_none_or(|b| b > maximum_key_bytes) {
        return Err(QueryExecutionError::BoundExceeded);
    }
    Ok(())
}

fn try_evaluate_scalar_candidate_refs<'a>(
    name: &str,
    candidate: &CandidateBindingV1,
    bindings: &'a BTreeMap<String, QueryStepRows>,
    maximum_key_bytes: u64,
) -> Option<Result<Vec<&'a CanonicalValue>, QueryExecutionError>> {
    let operator = candidate.operator();
    let max_distinct = candidate.maximum_distinct_keys();
    let sources = candidate.sources();
    if max_distinct == 0
        || sources.is_empty()
        || sources.len() > 8
        || (matches!(operator, riffdb_query_ir::CandidateSetOperatorV1::Single)
            && sources.len() != 1)
        || (matches!(
            operator,
            riffdb_query_ir::CandidateSetOperatorV1::Intersection
                | riffdb_query_ir::CandidateSetOperatorV1::Union
                | riffdb_query_ir::CandidateSetOperatorV1::Difference
        ) && sources.len() < 2)
    {
        return Some(Err(QueryExecutionError::BoundExceeded));
    }
    let mut first_discriminant: Option<std::mem::Discriminant<CanonicalValue>> = None;
    let mut raw_sources = Vec::with_capacity(sources.len());
    for (source_index, source) in sources.iter().enumerate() {
        let Some(binding) = candidate_source_binding_name(name, source_index) else {
            return Some(Err(QueryExecutionError::InvalidProgram));
        };
        let Some(rows) = bindings.get(&binding) else {
            return Some(Err(QueryExecutionError::InvalidProgram));
        };
        if rows.len() > usize::from(max_distinct) {
            return Some(Err(QueryExecutionError::BoundExceeded));
        }
        let first_key_pos = rows
            .first()
            .and_then(|row| row.field_pos(source.projected_key()));
        let mut refs = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(val) = row
                .field_at(first_key_pos, source.projected_key())
                .filter(|value| !matches!(value, CanonicalValue::Null))
            else {
                return Some(Err(QueryExecutionError::InvalidProgram));
            };
            canonical_scalar_encoded_len(val)?;
            let disc = std::mem::discriminant(val);
            match first_discriminant {
                None => first_discriminant = Some(disc),
                Some(prev) if prev == disc => {}
                Some(_) => return None,
            }
            refs.push(val);
        }
        raw_sources.push(refs);
    }
    let cmp = |a: &&CanonicalValue, b: &&CanonicalValue| {
        canonical_scalar_encoded_cmp(a, b).unwrap_or(Ordering::Equal)
    };
    let mut complete = Vec::with_capacity(raw_sources.len());
    for mut refs in raw_sources {
        refs.sort_unstable_by(cmp);
        refs.dedup();
        if let Err(err) = check_scalar_candidate_bound(&refs, max_distinct, maximum_key_bytes) {
            return Some(Err(err));
        }
        complete.push(refs);
    }
    let result = match operator {
        riffdb_query_ir::CandidateSetOperatorV1::Single => complete.remove(0),
        riffdb_query_ir::CandidateSetOperatorV1::Intersection => {
            let mut result = complete.remove(0);
            for source in complete {
                result.retain(|key| source.binary_search_by(|probe| cmp(probe, key)).is_ok());
            }
            result
        }
        riffdb_query_ir::CandidateSetOperatorV1::Union => {
            let mut result = Vec::new();
            for source in complete {
                result.extend(source);
                result.sort_unstable_by(cmp);
                result.dedup();
                if let Err(err) =
                    check_scalar_candidate_bound(&result, max_distinct, maximum_key_bytes)
                {
                    return Some(Err(err));
                }
            }
            result
        }
        riffdb_query_ir::CandidateSetOperatorV1::Difference => {
            let mut result = complete.remove(0);
            for source in complete {
                result.retain(|key| source.binary_search_by(|probe| cmp(probe, key)).is_err());
            }
            result
        }
    };
    if let Err(err) = check_scalar_candidate_bound(&result, max_distinct, maximum_key_bytes) {
        return Some(Err(err));
    }
    Some(Ok(result))
}

fn bind_candidate_value(
    name: &str,
    candidates: &[CandidateBindingV1],
    bindings: &BTreeMap<String, QueryStepRows>,
) -> Result<CanonicalValue, QueryExecutionError> {
    let candidate = candidates
        .iter()
        .find(|candidate| candidate.name() == name)
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let maximum_key_bytes = u64::from(candidate.maximum_distinct_keys())
        .checked_mul(riffdb_types::MAX_KEY_BYTES as u64)
        .ok_or(QueryExecutionError::BoundExceeded)?;
    if let Some(fast_res) =
        try_evaluate_scalar_candidate_refs(name, candidate, bindings, maximum_key_bytes)
    {
        let refs = fast_res?;
        let values = refs.into_iter().cloned().collect::<Vec<_>>();
        return CanonicalList::new(values)
            .map(CanonicalValue::List)
            .map_err(|_| QueryExecutionError::BoundExceeded);
    }
    let is_intersection = matches!(
        candidate.operator(),
        riffdb_query_ir::CandidateSetOperatorV1::Intersection
    );
    let mut source_keys = Vec::with_capacity(candidate.sources().len());
    let mut canonical_values = BTreeMap::<Vec<u8>, &CanonicalValue>::new();
    for (source_index, source) in candidate.sources().iter().enumerate() {
        let binding = candidate_source_binding_name(name, source_index)
            .ok_or(QueryExecutionError::InvalidProgram)?;
        let rows = bindings
            .get(&binding)
            .ok_or(QueryExecutionError::InvalidProgram)?;
        if rows.len() > usize::from(candidate.maximum_distinct_keys()) {
            return Err(QueryExecutionError::BoundExceeded);
        }
        let first_key_pos = rows
            .first()
            .and_then(|row| row.field_pos(source.projected_key()));
        let record_values = !is_intersection || source_index == 0;
        let keys = rows
            .iter()
            .map(|row| {
                let value = row
                    .field_at(first_key_pos, source.projected_key())
                    .filter(|value| !matches!(value, CanonicalValue::Null))
                    .ok_or(QueryExecutionError::InvalidProgram)?;
                let encoded = encode_canonical_value(value)
                    .map_err(|_| QueryExecutionError::InvalidProgram)?;
                if record_values && !canonical_values.contains_key(&encoded) {
                    canonical_values.insert(encoded.clone(), value);
                }
                Ok(encoded)
            })
            .collect::<Result<Vec<_>, QueryExecutionError>>()?;
        source_keys.push(keys);
    }
    let selected = evaluate_candidate_set_v1(
        candidate.operator(),
        &source_keys,
        candidate.maximum_distinct_keys(),
        maximum_key_bytes,
    )
    .map_err(|_| QueryExecutionError::BoundExceeded)?;
    let values = selected
        .into_iter()
        .map(|encoded| {
            canonical_values
                .get(&encoded)
                .copied()
                .cloned()
                .ok_or(QueryExecutionError::InvalidProgram)
        })
        .collect::<Result<Vec<_>, _>>()?;
    CanonicalList::new(values)
        .map(CanonicalValue::List)
        .map_err(|_| QueryExecutionError::BoundExceeded)
}

fn bind_candidate_root_hydrations(
    step: &QueryAccessStep,
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    bindings: &BTreeMap<String, QueryStepRows>,
    candidates: &[CandidateBindingV1],
    candidate_name: &str,
) -> Result<Vec<Vec<BoundPredicate>>, QueryExecutionError> {
    let keys = bind_scoped_candidate_keys(candidate_name, candidates, bindings, program)?;
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let first_key = &keys[0];
    let mut template_predicates = Vec::with_capacity(step.predicates().len());
    let mut candidate_slots = Vec::new();
    let mut partition_slots = Vec::new();
    for (idx, predicate) in step.predicates().iter().enumerate() {
        let (operator, value) = match predicate.value() {
            QueryPredicateValue::CandidateBinding { name } if name == candidate_name => {
                candidate_slots.push(idx);
                (QueryPredicateOperator::Equal, first_key.key.clone())
            }
            QueryPredicateValue::CandidateBinding { .. }
            | QueryPredicateValue::BindingFieldSet { .. } => {
                return Err(QueryExecutionError::InvalidProgram);
            }
            QueryPredicateValue::Parameter(name)
                if name == program.partition_parameter()
                    && predicate.operator() == QueryPredicateOperator::In =>
            {
                partition_slots.push(idx);
                (
                    QueryPredicateOperator::Equal,
                    first_key
                        .partition
                        .clone()
                        .ok_or(QueryExecutionError::InvalidProgram)?,
                )
            }
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
        template_predicates.push(BoundPredicate {
            field: predicate.field().to_owned(),
            operator,
            value,
        });
    }
    let mut result = Vec::with_capacity(keys.len());
    result.push(template_predicates);
    for candidate_key in &keys[1..] {
        let mut predicates = result[0].clone();
        for &slot in &candidate_slots {
            predicates[slot].value = candidate_key.key.clone();
        }
        for &slot in &partition_slots {
            predicates[slot].value = candidate_key
                .partition
                .clone()
                .ok_or(QueryExecutionError::InvalidProgram)?;
        }
        result.push(predicates);
    }
    Ok(result)
}

#[derive(Clone)]
struct ScopedCandidateKey {
    partition: Option<CanonicalValue>,
    key: CanonicalValue,
}

fn bind_scoped_candidate_keys(
    name: &str,
    candidates: &[CandidateBindingV1],
    bindings: &BTreeMap<String, QueryStepRows>,
    program: &QueryAccessProgramV1,
) -> Result<Vec<ScopedCandidateKey>, QueryExecutionError> {
    let candidate = candidates
        .iter()
        .find(|candidate| candidate.name() == name)
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let finite = program.partition_route().is_finite_set();
    let maximum_key_bytes = u64::from(candidate.maximum_distinct_keys())
        .checked_mul(riffdb_types::MAX_KEY_BYTES as u64)
        .and_then(|value| value.checked_mul(if finite { 2 } else { 1 }))
        .ok_or(QueryExecutionError::BoundExceeded)?;
    if !finite {
        let all_steps_present = (0..candidate.sources().len()).all(|source_index| {
            candidate_source_binding_name(name, source_index)
                .is_some_and(|binding| program.steps().iter().any(|step| step.binding() == binding))
        });
        if !all_steps_present {
            return Err(QueryExecutionError::InvalidProgram);
        }
        if let Some(fast_res) =
            try_evaluate_scalar_candidate_refs(name, candidate, bindings, maximum_key_bytes)
        {
            let refs = fast_res?;
            return Ok(refs
                .into_iter()
                .map(|key| ScopedCandidateKey {
                    partition: None,
                    key: key.clone(),
                })
                .collect());
        }
    }
    let is_intersection = matches!(
        candidate.operator(),
        riffdb_query_ir::CandidateSetOperatorV1::Intersection
    );
    let mut source_keys = Vec::with_capacity(candidate.sources().len());
    let mut canonical_values = BTreeMap::<Vec<u8>, ScopedCandidateKey>::new();
    let mut canonical_refs = BTreeMap::<Vec<u8>, &CanonicalValue>::new();
    for (source_index, source) in candidate.sources().iter().enumerate() {
        let binding = candidate_source_binding_name(name, source_index)
            .ok_or(QueryExecutionError::InvalidProgram)?;
        let rows = bindings
            .get(&binding)
            .ok_or(QueryExecutionError::InvalidProgram)?;
        if rows.len() > usize::from(candidate.maximum_distinct_keys()) {
            return Err(QueryExecutionError::BoundExceeded);
        }
        let source_step = program
            .steps()
            .iter()
            .find(|step| step.binding() == binding)
            .ok_or(QueryExecutionError::InvalidProgram)?;
        let partition_field = finite
            .then(|| {
                source_step.predicates().iter().find_map(|predicate| {
                    (predicate.operator() == QueryPredicateOperator::In
                        && matches!(
                            predicate.value(),
                            QueryPredicateValue::Parameter(parameter)
                                if parameter == program.partition_parameter()
                        ))
                    .then_some(predicate.field())
                })
            })
            .flatten()
            .ok_or(QueryExecutionError::InvalidProgram)
            .or_else(|error| if finite { Err(error) } else { Ok("") })?;
        let first_key_pos = rows
            .first()
            .and_then(|row| row.field_pos(source.projected_key()));
        let first_part_pos = if finite {
            rows.first().and_then(|row| row.field_pos(partition_field))
        } else {
            None
        };
        let record_values = !is_intersection || source_index == 0;
        let keys = rows
            .iter()
            .map(|row| {
                let key_ref = row
                    .field_at(first_key_pos, source.projected_key())
                    .filter(|value| !matches!(value, CanonicalValue::Null))
                    .ok_or(QueryExecutionError::InvalidProgram)?;
                if finite {
                    let partition = row
                        .field_at(first_part_pos, partition_field)
                        .cloned()
                        .ok_or(QueryExecutionError::InvalidProgram)?;
                    let scoped = ScopedCandidateKey {
                        partition: Some(partition.clone()),
                        key: key_ref.clone(),
                    };
                    let identity = CanonicalValue::list(vec![partition, key_ref.clone()])
                        .map_err(|_| QueryExecutionError::BoundExceeded)?;
                    let encoded = encode_canonical_value(&identity)
                        .map_err(|_| QueryExecutionError::InvalidProgram)?;
                    if record_values {
                        canonical_values.entry(encoded.clone()).or_insert(scoped);
                    }
                    Ok(encoded)
                } else {
                    let encoded = encode_canonical_value(key_ref)
                        .map_err(|_| QueryExecutionError::InvalidProgram)?;
                    if record_values && !canonical_refs.contains_key(&encoded) {
                        canonical_refs.insert(encoded.clone(), key_ref);
                    }
                    Ok(encoded)
                }
            })
            .collect::<Result<Vec<_>, QueryExecutionError>>()?;
        source_keys.push(keys);
    }
    let selected = evaluate_candidate_set_v1(
        candidate.operator(),
        &source_keys,
        candidate.maximum_distinct_keys(),
        maximum_key_bytes,
    )
    .map_err(|_| QueryExecutionError::BoundExceeded)?;
    if finite {
        selected
            .into_iter()
            .map(|encoded| {
                canonical_values
                    .remove(&encoded)
                    .ok_or(QueryExecutionError::InvalidProgram)
            })
            .collect()
    } else {
        selected
            .into_iter()
            .map(|encoded| {
                canonical_refs
                    .get(&encoded)
                    .copied()
                    .map(|key| ScopedCandidateKey {
                        partition: None,
                        key: key.clone(),
                    })
                    .ok_or(QueryExecutionError::InvalidProgram)
            })
            .collect()
    }
}

fn candidate_root_partition_field<'a>(
    step: &'a QueryAccessStep,
    program: &QueryAccessProgramV1,
) -> Result<Option<&'a str>, QueryExecutionError> {
    if !program.partition_route().is_finite_set() {
        return Ok(None);
    }
    step.predicates()
        .iter()
        .find_map(|predicate| {
            (predicate.operator() == QueryPredicateOperator::In
                && matches!(
                    predicate.value(),
                    QueryPredicateValue::Parameter(parameter)
                        if parameter == program.partition_parameter()
                ))
            .then_some(predicate.field())
        })
        .map(Some)
        .ok_or(QueryExecutionError::InvalidProgram)
}

#[allow(clippy::too_many_arguments)]
fn candidate_root_row_order_at_positions(
    left: &QueryRow,
    right: &QueryRow,
    order: &[QueryRootOrderTermV1],
    partition_field: Option<&str>,
    key_fields: &[String],
    order_positions: Option<&[usize]>,
    partition_pos: Option<usize>,
    key_positions: Option<&[usize]>,
) -> Ordering {
    for (idx, term) in order.iter().enumerate() {
        let pos = order_positions.and_then(|p| p.get(idx).copied());
        let left = left
            .field_at(pos, term.field())
            .unwrap_or(&CanonicalValue::Null);
        let right = right
            .field_at(pos, term.field())
            .unwrap_or(&CanonicalValue::Null);
        let ordering = match (
            matches!(left, CanonicalValue::Null),
            matches!(right, CanonicalValue::Null),
        ) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if term.nulls_first() {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if term.nulls_first() {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = aggregate_scalar_order(left, right).unwrap_or(Ordering::Equal);
                if term.direction() == AccessDirection::Reverse {
                    ordering.reverse()
                } else {
                    ordering
                }
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    if let Some(field) = partition_field {
        let Some(left) = left.field_at(partition_pos, field) else {
            return Ordering::Equal;
        };
        let Some(right) = right.field_at(partition_pos, field) else {
            return Ordering::Equal;
        };
        let ordering = aggregate_scalar_order(left, right).unwrap_or(Ordering::Equal);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    for (idx, field) in key_fields.iter().enumerate() {
        let pos = key_positions.and_then(|p| p.get(idx).copied());
        let Some(left) = left.field_at(pos, field.as_str()) else {
            return Ordering::Equal;
        };
        let Some(right) = right.field_at(pos, field.as_str()) else {
            return Ordering::Equal;
        };
        let ordering = aggregate_scalar_order(left, right).unwrap_or(Ordering::Equal);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn candidate_root_marker_at_positions(
    row: &QueryRow,
    partition_field: Option<&str>,
    key_fields: &[String],
    partition_pos: Option<usize>,
    key_positions: Option<&[usize]>,
) -> Result<Vec<u8>, QueryExecutionError> {
    let mut marker = Vec::new();
    if let Some(field) = partition_field {
        let value = row.field_at(partition_pos, field).ok_or_else(|| {
            QueryExecutionError::MissingField {
                entity: row.entity().to_owned(),
                field: field.to_owned(),
            }
        })?;
        let encoded =
            encode_canonical_value(value).map_err(|_| QueryExecutionError::InvalidProgram)?;
        marker.extend_from_slice(
            &u32::try_from(encoded.len())
                .map_err(|_| QueryExecutionError::BoundExceeded)?
                .to_be_bytes(),
        );
        marker.extend_from_slice(&encoded);
    }
    for (idx, field) in key_fields.iter().enumerate() {
        let pos = key_positions.and_then(|p| p.get(idx).copied());
        let value =
            row.field_at(pos, field.as_str())
                .ok_or_else(|| QueryExecutionError::MissingField {
                    entity: row.entity().to_owned(),
                    field: field.clone(),
                })?;
        let encoded =
            encode_canonical_value(value).map_err(|_| QueryExecutionError::InvalidProgram)?;
        marker.extend_from_slice(
            &u32::try_from(encoded.len())
                .map_err(|_| QueryExecutionError::BoundExceeded)?
                .to_be_bytes(),
        );
        marker.extend_from_slice(&encoded);
    }
    Ok(marker)
}

fn partition_set_routes<'a>(
    program: &QueryAccessProgramV1,
    parameters: &'a QueryParameters,
) -> Result<&'a [CanonicalValue], QueryExecutionError> {
    let value = parameters
        .get(program.partition_parameter())
        .ok_or_else(|| QueryExecutionError::MissingParameter {
            parameter: program.partition_parameter().to_owned(),
        })?;
    let CanonicalValue::List(routes) = value else {
        return Err(QueryExecutionError::InvalidParameter {
            parameter: program.partition_parameter().to_owned(),
        });
    };
    if !program.partition_route().is_finite_set()
        || routes.values().len() as u32 > program.partition_route().maximum_partitions()
        || routes.values().iter().any(|value| {
            matches!(
                value,
                CanonicalValue::Null
                    | CanonicalValue::List(_)
                    | CanonicalValue::Record(_)
                    | CanonicalValue::Vector(_)
            )
        })
    {
        return Err(QueryExecutionError::InvalidParameter {
            parameter: program.partition_parameter().to_owned(),
        });
    }
    Ok(routes.values())
}

fn partition_set_step_routes<'a>(
    program: &'a QueryAccessProgramV1,
    step: &'a QueryAccessStep,
    fields: &'a [String],
    parameters: &'a QueryParameters,
    predicates: &'a [BoundPredicate],
) -> Result<(&'a str, Option<&'a str>, &'a [CanonicalValue]), QueryExecutionError> {
    if program.partition_route().is_finite_set() {
        let route_field = fields
            .first()
            .ok_or(QueryExecutionError::InvalidProgram)?
            .as_str();
        let routes = partition_set_routes(program, parameters)?;
        return Ok((route_field, Some(program.partition_parameter()), routes));
    }
    if let Some((route_field, route_param)) =
        step.predicates()
            .iter()
            .find_map(|predicate| match predicate.value() {
                QueryPredicateValue::Parameter(name)
                    if predicate.operator() == QueryPredicateOperator::In =>
                {
                    Some((predicate.field(), name.as_str()))
                }
                _ => None,
            })
    {
        let value =
            parameters
                .get(route_param)
                .ok_or_else(|| QueryExecutionError::MissingParameter {
                    parameter: route_param.to_owned(),
                })?;
        let CanonicalValue::List(routes) = value else {
            return Err(QueryExecutionError::InvalidParameter {
                parameter: route_param.to_owned(),
            });
        };
        if routes.values().len() > MAX_QUERY_PARAMETERS
            || routes.values().iter().any(|value| {
                matches!(
                    value,
                    CanonicalValue::Null
                        | CanonicalValue::List(_)
                        | CanonicalValue::Record(_)
                        | CanonicalValue::Vector(_)
                )
            })
        {
            return Err(QueryExecutionError::InvalidParameter {
                parameter: route_param.to_owned(),
            });
        }
        return Ok((route_field, Some(route_param), routes.values()));
    }
    let route_field = fields
        .first()
        .ok_or(QueryExecutionError::InvalidProgram)?
        .as_str();
    let route = predicates
        .iter()
        .find(|predicate| {
            predicate.field() == route_field
                && predicate.operator() == QueryPredicateOperator::Equal
        })
        .map(BoundPredicate::value)
        .ok_or(QueryExecutionError::InvalidProgram)?;
    Ok((route_field, None, std::slice::from_ref(route)))
}

fn partition_local_predicates(
    step: &QueryAccessStep,
    predicates: &[BoundPredicate],
    route_parameter: Option<&str>,
    route: &CanonicalValue,
) -> Result<Vec<BoundPredicate>, QueryExecutionError> {
    if step.predicates().len() != predicates.len() {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let Some(route_parameter) = route_parameter else {
        return Ok(predicates.to_vec());
    };
    let mut found = false;
    let local = step
        .predicates()
        .iter()
        .zip(predicates)
        .map(|(source, bound)| {
            let is_route = source.operator() == QueryPredicateOperator::In
                && matches!(source.value(), QueryPredicateValue::Parameter(name) if name == route_parameter);
            if is_route {
                if found {
                    return Err(QueryExecutionError::InvalidProgram);
                }
                found = true;
                Ok(BoundPredicate {
                    field: bound.field.clone(),
                    operator: QueryPredicateOperator::Equal,
                    value: route.clone(),
                })
            } else {
                Ok(bound.clone())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    found
        .then_some(local)
        .ok_or(QueryExecutionError::InvalidProgram)
}

fn partition_set_marker_values(
    row: &QueryRow,
    route_field: &str,
    order: &[QueryRootOrderTermV1],
    key_fields: &[String],
) -> Result<Vec<CanonicalValue>, QueryExecutionError> {
    partition_set_marker_values_at_positions(row, route_field, None, order, None, key_fields, None)
}

fn partition_set_marker_values_at_positions(
    row: &QueryRow,
    route_field: &str,
    route_pos: Option<usize>,
    order: &[QueryRootOrderTermV1],
    order_positions: Option<&[usize]>,
    key_fields: &[String],
    key_positions: Option<&[usize]>,
) -> Result<Vec<CanonicalValue>, QueryExecutionError> {
    let mut values = Vec::with_capacity(order.len() + 1 + key_fields.len());
    for (idx, term) in order.iter().enumerate() {
        let pos = order_positions.and_then(|p| p.get(idx).copied());
        let val = row.field_at(pos, term.field()).cloned().ok_or_else(|| {
            QueryExecutionError::MissingField {
                entity: row.entity().to_owned(),
                field: term.field().to_owned(),
            }
        })?;
        values.push(val);
    }
    let route_val = row
        .field_at(route_pos, route_field)
        .cloned()
        .ok_or_else(|| QueryExecutionError::MissingField {
            entity: row.entity().to_owned(),
            field: route_field.to_owned(),
        })?;
    values.push(route_val);
    for (idx, field) in key_fields.iter().enumerate() {
        let pos = key_positions.and_then(|p| p.get(idx).copied());
        let val = row.field_at(pos, field.as_str()).cloned().ok_or_else(|| {
            QueryExecutionError::MissingField {
                entity: row.entity().to_owned(),
                field: field.clone(),
            }
        })?;
        values.push(val);
    }
    Ok(values)
}

fn partition_set_marker(
    row: &QueryRow,
    route_field: &str,
    order: &[QueryRootOrderTermV1],
    key_fields: &[String],
) -> Result<Vec<u8>, QueryExecutionError> {
    let values = partition_set_marker_values(row, route_field, order, key_fields)?;
    let value = CanonicalValue::list(values).map_err(|_| QueryExecutionError::BoundExceeded)?;
    encode_canonical_value(&value).map_err(|_| QueryExecutionError::BoundExceeded)
}

fn decode_partition_set_marker(
    marker: &[u8],
    expected: usize,
) -> Result<Vec<CanonicalValue>, QueryExecutionError> {
    let CanonicalValue::List(values) =
        decode_canonical_value(marker).map_err(|_| QueryExecutionError::InvalidContinuation)?
    else {
        return Err(QueryExecutionError::InvalidContinuation);
    };
    (values.values().len() == expected)
        .then(|| values.values().to_vec())
        .ok_or(QueryExecutionError::InvalidContinuation)
}

fn partition_set_value_order(
    left: &[CanonicalValue],
    right: &[CanonicalValue],
    order: &[QueryRootOrderTermV1],
    key_fields: &[String],
) -> Result<Ordering, QueryExecutionError> {
    if left.len() != right.len() || left.len() != order.len() + 1 + key_fields.len() {
        return Err(QueryExecutionError::InvalidProgram);
    }
    for (position, term) in order.iter().enumerate() {
        let left = &left[position];
        let right = &right[position];
        let ordering = match (
            matches!(left, CanonicalValue::Null),
            matches!(right, CanonicalValue::Null),
        ) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if term.nulls_first() {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if term.nulls_first() {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = aggregate_scalar_order(left, right)?;
                if term.direction() == AccessDirection::Reverse {
                    ordering.reverse()
                } else {
                    ordering
                }
            }
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    for (left, right) in left[order.len()..].iter().zip(&right[order.len()..]) {
        let ordering = aggregate_scalar_order(left, right)?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn partition_set_row_order(
    left: &QueryRow,
    right: &QueryRow,
    route_field: &str,
    order: &[QueryRootOrderTermV1],
    key_fields: &[String],
) -> Result<Ordering, QueryExecutionError> {
    partition_set_row_order_at_positions(
        left,
        right,
        route_field,
        None,
        order,
        None,
        key_fields,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn partition_set_row_order_at_positions(
    left: &QueryRow,
    right: &QueryRow,
    route_field: &str,
    route_pos: Option<usize>,
    order: &[QueryRootOrderTermV1],
    order_positions: Option<&[usize]>,
    key_fields: &[String],
    key_positions: Option<&[usize]>,
) -> Result<Ordering, QueryExecutionError> {
    for (idx, term) in order.iter().enumerate() {
        let pos = order_positions.and_then(|p| p.get(idx).copied());
        let left_value = left
            .field_at(pos, term.field())
            .unwrap_or(&CanonicalValue::Null);
        let right_value = right
            .field_at(pos, term.field())
            .unwrap_or(&CanonicalValue::Null);
        let ordering = match (
            matches!(left_value, CanonicalValue::Null),
            matches!(right_value, CanonicalValue::Null),
        ) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if term.nulls_first() {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if term.nulls_first() {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = aggregate_scalar_order(left_value, right_value)?;
                if term.direction() == AccessDirection::Reverse {
                    ordering.reverse()
                } else {
                    ordering
                }
            }
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    let left_route = left
        .field_at(route_pos, route_field)
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let right_route = right
        .field_at(route_pos, route_field)
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let route_ordering = aggregate_scalar_order(left_route, right_route)?;
    if route_ordering != Ordering::Equal {
        return Ok(route_ordering);
    }
    for (idx, field) in key_fields.iter().enumerate() {
        let pos = key_positions.and_then(|p| p.get(idx).copied());
        let left_value = left
            .field_at(pos, field.as_str())
            .ok_or(QueryExecutionError::InvalidProgram)?;
        let right_value = right
            .field_at(pos, field.as_str())
            .ok_or(QueryExecutionError::InvalidProgram)?;
        let ordering = aggregate_scalar_order(left_value, right_value)?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn partition_set_physical_row_order(
    left: &QueryRow,
    right: &QueryRow,
    fields: &[String],
    key_fields: &[String],
    direction: AccessDirection,
) -> Result<Ordering, QueryExecutionError> {
    for field in fields.iter().skip(1).chain(key_fields) {
        let left = left
            .field(field)
            .ok_or(QueryExecutionError::InvalidProgram)?;
        let right = right
            .field(field)
            .ok_or(QueryExecutionError::InvalidProgram)?;
        let ordering = aggregate_scalar_order(left, right)?;
        if ordering != Ordering::Equal {
            return Ok(if direction == AccessDirection::Reverse {
                ordering.reverse()
            } else {
                ordering
            });
        }
    }
    Ok(Ordering::Equal)
}

fn merge_partition_runs(
    streams: Vec<Vec<QueryRow>>,
    limit: usize,
    route_field: &str,
    order: &[QueryRootOrderTermV1],
    key_fields: &[String],
) -> Result<(Vec<QueryRow>, bool), QueryExecutionError> {
    let first_row = streams.iter().find_map(|rows| rows.first());
    let route_pos = first_row.and_then(|row| row.field_pos(route_field));
    let order_positions: Option<Vec<usize>> = order
        .iter()
        .map(|term| first_row.and_then(|row| row.field_pos(term.field())))
        .collect();
    let key_positions: Option<Vec<usize>> = key_fields
        .iter()
        .map(|field| first_row.and_then(|row| row.field_pos(field.as_str())))
        .collect();
    let order_pos_slice = order_positions.as_deref();
    let key_pos_slice = key_positions.as_deref();
    let mut heap = Vec::with_capacity(streams.len());
    for rows in streams {
        let mut buffered = rows.into_iter();
        let row = buffered.next();
        if row.is_some() {
            partition_heap_push(
                &mut heap,
                PartitionMergeStream {
                    route: CanonicalValue::Null,
                    predicates: Vec::new(),
                    row,
                    resume: None,
                    after_inclusive: false,
                    started: true,
                    epoch: None,
                    scanned_rows: 0,
                    buffered,
                },
                route_field,
                route_pos,
                order,
                order_pos_slice,
                key_fields,
                key_pos_slice,
            )?;
        }
    }
    let mut result = Vec::with_capacity(limit);
    while result.len() < limit {
        let Some(mut stream) = partition_heap_pop(
            &mut heap,
            route_field,
            route_pos,
            order,
            order_pos_slice,
            key_fields,
            key_pos_slice,
        )?
        else {
            break;
        };
        result.push(
            stream
                .row
                .take()
                .ok_or(QueryExecutionError::BackendIntegrity)?,
        );
        stream.row = stream.buffered.next();
        if stream.row.is_some() {
            partition_heap_push(
                &mut heap,
                stream,
                route_field,
                route_pos,
                order,
                order_pos_slice,
                key_fields,
                key_pos_slice,
            )?;
        }
    }
    Ok((result, !heap.is_empty()))
}

fn sort_partition_rows(
    rows: &mut [QueryRow],
    route_field: &str,
    order: &[QueryRootOrderTermV1],
    key_fields: &[String],
) -> Result<(), QueryExecutionError> {
    fn sift(
        rows: &mut [QueryRow],
        mut root: usize,
        route_field: &str,
        order: &[QueryRootOrderTermV1],
        keys: &[String],
    ) -> Result<(), QueryExecutionError> {
        while root < rows.len() / 2 {
            let mut child = root * 2 + 1;
            if child + 1 < rows.len()
                && partition_set_row_order(
                    &rows[child],
                    &rows[child + 1],
                    route_field,
                    order,
                    keys,
                )? == Ordering::Less
            {
                child += 1;
            }
            if partition_set_row_order(&rows[root], &rows[child], route_field, order, keys)?
                != Ordering::Less
            {
                break;
            }
            rows.swap(root, child);
            root = child;
        }
        Ok(())
    }
    for root in (0..rows.len() / 2).rev() {
        sift(rows, root, route_field, order, key_fields)?;
    }
    for end in (1..rows.len()).rev() {
        rows.swap(0, end);
        sift(&mut rows[..end], 0, route_field, order, key_fields)?;
    }
    Ok(())
}

fn merge_partition_streams(
    left: Vec<QueryRow>,
    right: Vec<QueryRow>,
    limit: usize,
    route_field: &str,
    order: &[QueryRootOrderTermV1],
    key_fields: &[String],
) -> Result<(Vec<QueryRow>, bool), QueryExecutionError> {
    let total = left
        .len()
        .checked_add(right.len())
        .ok_or(QueryExecutionError::BoundExceeded)?;
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();
    let mut merged = Vec::with_capacity(total.min(limit));
    while merged.len() < limit {
        let take_left = match (left.peek(), right.peek()) {
            (Some(left), Some(right)) => {
                partition_set_row_order(left, right, route_field, order, key_fields)?
                    != Ordering::Greater
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        merged.push(if take_left {
            left.next().ok_or(QueryExecutionError::InvalidProgram)?
        } else {
            right.next().ok_or(QueryExecutionError::InvalidProgram)?
        });
    }
    let discarded = left.peek().is_some() || right.peek().is_some();
    Ok((merged, discarded))
}

#[allow(clippy::too_many_arguments)]
fn partition_set_physical_resume(
    step: &QueryAccessStep,
    predicates: &[BoundPredicate],
    fields: &[String],
    route_field: &str,
    key_fields: &[String],
    order: &[QueryRootOrderTermV1],
    route: &CanonicalValue,
    marker: &[CanonicalValue],
) -> Result<(Vec<u8>, bool), QueryExecutionError> {
    if marker.len() != order.len() + 1 + key_fields.len() {
        return Err(QueryExecutionError::InvalidContinuation);
    }
    let schema = step
        .internal_index_key_schema()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let mut physical_index = partition_set_exact_physical_prefix(
        step,
        schema,
        fields,
        route_field,
        order,
        predicates,
        route,
    )?;
    let descriptor = step
        .internal_operational_index_descriptor()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let order_start = fields
        .len()
        .checked_sub(order.len())
        .ok_or(QueryExecutionError::InvalidProgram)?;
    for (offset, value) in marker[..order.len()].iter().enumerate() {
        let field_position = order_start
            .checked_add(offset)
            .ok_or(QueryExecutionError::BoundExceeded)?;
        let component = descriptor
            .component(
                field_position,
                fields
                    .get(field_position)
                    .ok_or(QueryExecutionError::InvalidProgram)?,
            )
            .ok_or(QueryExecutionError::InvalidProgram)?;
        push_exact_index_prefix_value(schema, component, &mut physical_index, value)?;
    }
    if physical_index.len() != schema.components().len() {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let key_offset = order.len() + 1;
    let mut entity_values = marker[key_offset..].to_vec();
    let mut synthetic = marker.to_vec();
    synthetic[order.len()] = route.clone();
    for (offset, field) in key_fields.iter().enumerate() {
        if field == route_field {
            entity_values[offset] = route.clone();
            synthetic[key_offset + offset] = route.clone();
        }
    }
    let entity_key = step
        .internal_entity_key_schema()
        .encode_entity(&entity_values)
        .map_err(|_| QueryExecutionError::InvalidProgram)?;
    let lower = schema
        .encode_index(&physical_index, entity_key)
        .map_err(|_| QueryExecutionError::InvalidProgram)?
        .as_bytes()
        .to_vec();
    let inclusive =
        partition_set_value_order(&synthetic, marker, order, key_fields)? == Ordering::Greater;
    Ok((lower, inclusive))
}

fn partition_set_exact_physical_prefix(
    step: &QueryAccessStep,
    schema: &riffdb_contract_ir::KeySchema,
    fields: &[String],
    route_field: &str,
    order: &[QueryRootOrderTermV1],
    predicates: &[BoundPredicate],
    route: &CanonicalValue,
) -> Result<Vec<CanonicalValue>, QueryExecutionError> {
    let order_start = fields
        .len()
        .checked_sub(order.len())
        .filter(|start| *start > 0)
        .ok_or(QueryExecutionError::InvalidProgram)?;
    if !fields[order_start..]
        .iter()
        .map(String::as_str)
        .eq(order.iter().map(QueryRootOrderTermV1::field))
    {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let mut physical = Vec::with_capacity(fields.len());
    let descriptor = step
        .internal_operational_index_descriptor()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    for (field_position, field) in fields.iter().enumerate().take(order_start) {
        let value = if field == route_field {
            route
        } else {
            let mut matching = predicates.iter().filter(|predicate| {
                predicate.field() == field && predicate.operator() == QueryPredicateOperator::Equal
            });
            let predicate = matching
                .next()
                .filter(|_| matching.next().is_none())
                .ok_or(QueryExecutionError::InvalidProgram)?;
            predicate.value()
        };
        let component = descriptor
            .component(field_position, field)
            .ok_or(QueryExecutionError::InvalidProgram)?;
        push_exact_index_prefix_value(schema, component, &mut physical, value)?;
    }
    Ok(physical)
}

fn append_partition_observation(
    output: &mut Vec<u8>,
    route: &CanonicalValue,
    epoch: u64,
) -> Result<(), QueryExecutionError> {
    let route = encode_canonical_value(route).map_err(|_| QueryExecutionError::InvalidProgram)?;
    output.extend_from_slice(
        &u32::try_from(route.len())
            .map_err(|_| QueryExecutionError::BoundExceeded)?
            .to_be_bytes(),
    );
    output.extend_from_slice(&route);
    output.extend_from_slice(&epoch.to_be_bytes());
    Ok(())
}

fn insert_partition_set_observation(
    index_epochs: &mut BTreeMap<String, u64>,
    entity: &str,
    index: &str,
    observations: &[u8],
) -> Result<(), QueryExecutionError> {
    let digest = hash_partition_set_observation(observations);
    let mut epoch_key =
        String::with_capacity(16 + entity.len() + index.len() + digest.as_bytes().len() * 2);
    use std::fmt::Write as _;
    write!(&mut epoch_key, "partition-set.{entity}.{index}.")
        .map_err(|_| QueryExecutionError::BoundExceeded)?;
    for byte in digest.as_bytes() {
        write!(&mut epoch_key, "{byte:02x}").map_err(|_| QueryExecutionError::BoundExceeded)?;
    }
    index_epochs.insert(epoch_key, 1);
    Ok(())
}

struct PartitionMergeStream {
    route: CanonicalValue,
    predicates: Vec<BoundPredicate>,
    row: Option<QueryRow>,
    resume: Option<Vec<u8>>,
    after_inclusive: bool,
    started: bool,
    epoch: Option<u64>,
    scanned_rows: u64,
    buffered: std::vec::IntoIter<QueryRow>,
}

#[allow(clippy::too_many_arguments)]
fn execute_partition_set_uniform_merge<V: QueryReadView>(
    view: &mut V,
    fuel: &mut QueryExecutionFuel,
    step: &QueryAccessStep,
    prepared_step: Option<&PreparedExecutorStepPlan>,
    predicates: &[BoundPredicate],
    route_field: &str,
    route_parameter: Option<&str>,
    routes: &[CanonicalValue],
    fields: &[String],
    order: &[QueryRootOrderTermV1],
    key_fields: &[String],
    scan_ceiling: u32,
    limit: u64,
    prior_marker: Option<&[CanonicalValue]>,
    policy: Option<&AuthorizedQueryRowPolicyContextV1>,
) -> Result<(Vec<QueryRow>, Vec<u8>, bool), QueryExecutionError> {
    let output_limit = usize::try_from(limit).map_err(|_| QueryExecutionError::BoundExceeded)?;
    // The compiler grants maximum_rows + one head per route. Only the
    // request's unused row allowance may fund speculative prefetch. At the
    // maximum bound, retain lazy heads until just one stream remains.
    let mut prefetch_spare = step.maximum_rows().saturating_sub(limit);
    let mut heap = Vec::<PartitionMergeStream>::with_capacity(routes.len());
    let mut observations = Vec::new();
    let pred_positions = prepared_step.and_then(|ps| ps.predicate_positions.as_deref());
    let order_positions = prepared_step.and_then(|ps| ps.order_positions.as_deref());
    let key_positions = prepared_step.and_then(|ps| ps.key_positions.as_deref());
    let mut route_pos: Option<usize> = None;
    for route in routes {
        let local = partition_local_predicates(step, predicates, route_parameter, route)?;
        let resume = prior_marker
            .map(|marker| {
                partition_set_physical_resume(
                    step,
                    &local,
                    fields,
                    route_field,
                    key_fields,
                    order,
                    route,
                    marker,
                )
            })
            .transpose()?;
        let mut stream = PartitionMergeStream {
            route: route.clone(),
            predicates: local,
            row: None,
            resume: resume.as_ref().map(|value| value.0.clone()),
            after_inclusive: resume.is_some_and(|value| value.1),
            started: false,
            epoch: None,
            scanned_rows: 0,
            buffered: Vec::new().into_iter(),
        };
        load_partition_merge_row(
            view,
            fuel,
            step,
            pred_positions,
            &mut stream,
            route_field,
            &mut route_pos,
            order,
            order_positions,
            key_fields,
            key_positions,
            scan_ceiling,
            prior_marker,
            policy,
            &mut prefetch_spare,
            1,
            limit.saturating_add(1),
        )?;
        append_partition_observation(
            &mut observations,
            &stream.route,
            stream.epoch.ok_or(QueryExecutionError::BackendIntegrity)?,
        )?;
        if stream.row.is_some() {
            partition_heap_push(
                &mut heap,
                stream,
                route_field,
                route_pos,
                order,
                order_positions,
                key_fields,
                key_positions,
            )?;
        }
    }

    let mut rows = Vec::with_capacity(output_limit);
    while rows.len() < output_limit {
        let Some(mut stream) = partition_heap_pop(
            &mut heap,
            route_field,
            route_pos,
            order,
            order_positions,
            key_fields,
            key_positions,
        )?
        else {
            break;
        };
        let row = stream
            .row
            .take()
            .ok_or(QueryExecutionError::InvalidProgram)?;
        if let Some(previous) = rows.last()
            && partition_set_row_order_at_positions(
                previous,
                &row,
                route_field,
                route_pos,
                order,
                order_positions,
                key_fields,
                key_positions,
            )? != Ordering::Less
        {
            return Err(QueryExecutionError::BackendIntegrity);
        }
        rows.push(row);
        load_partition_merge_row(
            view,
            fuel,
            step,
            pred_positions,
            &mut stream,
            route_field,
            &mut route_pos,
            order,
            order_positions,
            key_fields,
            key_positions,
            scan_ceiling,
            prior_marker,
            policy,
            &mut prefetch_spare,
            if heap.is_empty() {
                (output_limit - rows.len()) as u64 + 1
            } else {
                1
            },
            limit.saturating_add(1),
        )?;
        if stream.row.is_some() {
            partition_heap_push(
                &mut heap,
                stream,
                route_field,
                route_pos,
                order,
                order_positions,
                key_fields,
                key_positions,
            )?;
        }
    }
    Ok((rows, observations, !heap.is_empty()))
}

#[allow(clippy::too_many_arguments)]
fn load_partition_merge_row<V: QueryReadView>(
    view: &mut V,
    fuel: &mut QueryExecutionFuel,
    step: &QueryAccessStep,
    pred_positions: Option<&[usize]>,
    stream: &mut PartitionMergeStream,
    route_field: &str,
    route_pos: &mut Option<usize>,
    order: &[QueryRootOrderTermV1],
    order_positions: Option<&[usize]>,
    key_fields: &[String],
    key_positions: Option<&[usize]>,
    scan_ceiling: u32,
    prior_marker: Option<&[CanonicalValue]>,
    policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    prefetch_spare: &mut u64,
    guaranteed_rows: u64,
    page_target: u64,
) -> Result<(), QueryExecutionError> {
    stream.row = None;
    loop {
        if let Some(row) = stream.buffered.next() {
            if route_pos.is_none() {
                *route_pos = row.field_pos(route_field);
            }
            if prior_marker.is_some_and(|marker| {
                partition_set_marker_values_at_positions(
                    &row,
                    route_field,
                    *route_pos,
                    order,
                    order_positions,
                    key_fields,
                    key_positions,
                )
                .and_then(|values| partition_set_value_order(&values, marker, order, key_fields))
                    != Ok(Ordering::Greater)
            }) {
                continue;
            }
            stream.row = Some(row);
            return Ok(());
        }
        if stream.started && stream.resume.is_none() {
            return Ok(());
        }
        let remaining = u64::from(scan_ceiling)
            .checked_sub(stream.scanned_rows)
            .ok_or(QueryExecutionError::BoundExceeded)?;
        if remaining == 0 {
            return Err(QueryExecutionError::BoundExceeded);
        }
        let page_limit = guaranteed_rows
            .saturating_add(*prefetch_spare)
            .min(64)
            .min(page_target)
            .min(remaining.saturating_sub(1).max(1))
            .min(fuel.point_reads)
            .min(fuel.intermediate_rows)
            .max(1);
        let mut page = view
            .scan(
                step,
                &stream.predicates,
                page_limit,
                stream.resume.as_deref(),
                stream.after_inclusive,
                policy,
            )
            .map_err(|error| map_view_error(view, &error))?;
        let page_rows_len = page.rows_len() as u64;
        if page.scanned_rows > remaining
            || page_rows_len > page_limit
            || page.point_reads != page_rows_len
            || page_rows_len > page.scanned_rows
            || page
                .continuation
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > MAX_QUERY_CONTINUATION_BYTES)
            || page.rows_slice().iter().any(|row| {
                row.entity() != step.entity()
                    || predicates_match_at_positions(row, &stream.predicates, pred_positions)
                        != Ok(true)
            })
        {
            return Err(QueryExecutionError::BoundExceeded);
        }
        match stream.epoch {
            None => stream.epoch = Some(page.epoch),
            Some(existing) if existing == page.epoch => {}
            Some(_) => return Err(QueryExecutionError::BackendIntegrity),
        }
        stream.started = true;
        stream.after_inclusive = false;
        stream.scanned_rows = stream
            .scanned_rows
            .checked_add(page.scanned_rows)
            .ok_or(QueryExecutionError::BoundExceeded)?;
        fuel.scans(page.scanned_rows)?;
        fuel.points(page.point_reads)?;
        fuel.intermediates(page_rows_len)?;
        *prefetch_spare =
            prefetch_spare.saturating_sub(page_rows_len.saturating_sub(guaranteed_rows));
        stream.resume = page.continuation.take();
        stream.buffered = page.into_rows_vec().into_iter();
    }
}

#[allow(clippy::too_many_arguments)]
fn partition_heap_push(
    heap: &mut Vec<PartitionMergeStream>,
    stream: PartitionMergeStream,
    route_field: &str,
    route_pos: Option<usize>,
    order: &[QueryRootOrderTermV1],
    order_positions: Option<&[usize]>,
    key_fields: &[String],
    key_positions: Option<&[usize]>,
) -> Result<(), QueryExecutionError> {
    heap.push(stream);
    let mut child = heap.len() - 1;
    while child > 0 {
        let parent = (child - 1) / 2;
        let child_row = heap[child]
            .row
            .as_ref()
            .ok_or(QueryExecutionError::InvalidProgram)?;
        let parent_row = heap[parent]
            .row
            .as_ref()
            .ok_or(QueryExecutionError::InvalidProgram)?;
        if partition_set_row_order_at_positions(
            child_row,
            parent_row,
            route_field,
            route_pos,
            order,
            order_positions,
            key_fields,
            key_positions,
        )? != Ordering::Less
        {
            break;
        }
        heap.swap(child, parent);
        child = parent;
    }
    Ok(())
}

fn partition_heap_pop(
    heap: &mut Vec<PartitionMergeStream>,
    route_field: &str,
    route_pos: Option<usize>,
    order: &[QueryRootOrderTermV1],
    order_positions: Option<&[usize]>,
    key_fields: &[String],
    key_positions: Option<&[usize]>,
) -> Result<Option<PartitionMergeStream>, QueryExecutionError> {
    let Some(last) = heap.pop() else {
        return Ok(None);
    };
    if heap.is_empty() {
        return Ok(Some(last));
    }
    let result = std::mem::replace(&mut heap[0], last);
    let mut parent = 0;
    loop {
        let left = parent * 2 + 1;
        if left >= heap.len() {
            break;
        }
        let right = left + 1;
        let child = if right < heap.len() {
            let left_row = heap[left]
                .row
                .as_ref()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let right_row = heap[right]
                .row
                .as_ref()
                .ok_or(QueryExecutionError::InvalidProgram)?;
            if partition_set_row_order_at_positions(
                right_row,
                left_row,
                route_field,
                route_pos,
                order,
                order_positions,
                key_fields,
                key_positions,
            )? == Ordering::Less
            {
                right
            } else {
                left
            }
        } else {
            left
        };
        let parent_row = heap[parent]
            .row
            .as_ref()
            .ok_or(QueryExecutionError::InvalidProgram)?;
        let child_row = heap[child]
            .row
            .as_ref()
            .ok_or(QueryExecutionError::InvalidProgram)?;
        if partition_set_row_order_at_positions(
            parent_row,
            child_row,
            route_field,
            route_pos,
            order,
            order_positions,
            key_fields,
            key_positions,
        )? != Ordering::Greater
        {
            break;
        }
        heap.swap(parent, child);
        parent = child;
    }
    Ok(Some(result))
}

#[allow(clippy::too_many_arguments)]
fn execute_partition_set_stream<V: QueryReadView>(
    view: &mut V,
    fuel: &mut QueryExecutionFuel,
    step: &QueryAccessStep,
    prepared_step: Option<&PreparedExecutorStepPlan>,
    predicates: &[BoundPredicate],
    route_field: &str,
    route: &CanonicalValue,
    fields: &[String],
    order: &[QueryRootOrderTermV1],
    key_fields: &[String],
    direction: AccessDirection,
    scan_ceiling: u32,
    limit: u64,
    prior_marker: Option<&[CanonicalValue]>,
    policy: Option<&AuthorizedQueryRowPolicyContextV1>,
) -> Result<(Vec<QueryRow>, u64, bool), QueryExecutionError> {
    let pred_positions = prepared_step.and_then(|ps| ps.predicate_positions.as_deref());
    let mixed = order.iter().any(|term| term.direction() != direction);
    let mut resume = prior_marker
        .map(|marker| {
            if mixed {
                partition_set_group_resume(
                    step,
                    predicates,
                    fields,
                    route_field,
                    order,
                    direction,
                    route,
                    marker,
                )
            } else {
                partition_set_physical_resume(
                    step,
                    predicates,
                    fields,
                    route_field,
                    key_fields,
                    order,
                    route,
                    marker,
                )
            }
        })
        .transpose()?;
    let mut rows = Vec::new();
    let mut total_scanned = 0_u64;
    let mut epoch = None;
    let mut stopped_with_more = false;
    loop {
        let remaining = u64::from(scan_ceiling)
            .checked_sub(total_scanned)
            .ok_or(QueryExecutionError::BoundExceeded)?;
        if remaining <= QUERY_CONTINUATION_PROBE_ROWS {
            return Err(QueryExecutionError::BoundExceeded);
        }
        let page_limit = limit.min(
            remaining
                .checked_sub(QUERY_CONTINUATION_PROBE_ROWS)
                .ok_or(QueryExecutionError::BoundExceeded)?,
        );
        let mut page = view
            .scan(
                step,
                predicates,
                page_limit,
                resume.as_ref().map(|resume| resume.0.as_slice()),
                resume.as_ref().is_some_and(|resume| resume.1),
                policy,
            )
            .map_err(|error| map_view_error(view, &error))?;
        let page_rows_len = page.rows_len() as u64;
        if page.scanned_rows > remaining
            || page_rows_len > page_limit
            || page.point_reads != page_rows_len
            || page_rows_len > page.scanned_rows
            || page
                .continuation
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > MAX_QUERY_CONTINUATION_BYTES)
            || page.rows_slice().iter().any(|row| {
                row.entity() != step.entity()
                    || predicates_match_at_positions(row, predicates, pred_positions) != Ok(true)
            })
        {
            return Err(QueryExecutionError::BoundExceeded);
        }
        match epoch {
            None => epoch = Some(page.epoch),
            Some(existing) if existing == page.epoch => {}
            Some(_) => return Err(QueryExecutionError::BackendIntegrity),
        }
        total_scanned = total_scanned
            .checked_add(page.scanned_rows)
            .ok_or(QueryExecutionError::BoundExceeded)?;
        fuel.scans(page.scanned_rows)?;
        fuel.points(page.point_reads)?;
        fuel.intermediates(page_rows_len)?;
        let next = page.continuation.take();
        let page_rows_vec = page.into_rows_vec();
        if mixed {
            let mut incoming = page_rows_vec;
            sort_partition_rows(&mut incoming, route_field, order, key_fields)?;
            let total = rows
                .len()
                .checked_add(incoming.len())
                .ok_or(QueryExecutionError::BoundExceeded)?;
            rows =
                merge_partition_streams(rows, incoming, total, route_field, order, key_fields)?.0;
        } else {
            rows.extend(page_rows_vec);
        }

        if !mixed {
            stopped_with_more = next.is_some();
            break;
        }
        if rows.windows(2).any(|pair| {
            partition_set_row_order(&pair[0], &pair[1], route_field, order, key_fields)
                != Ok(Ordering::Less)
        }) {
            return Err(QueryExecutionError::BackendIntegrity);
        }
        let visible = rows
            .iter()
            .filter(|row| {
                prior_marker.is_none_or(|marker| {
                    partition_set_marker_values(row, route_field, order, key_fields).and_then(
                        |values| partition_set_value_order(&values, marker, order, key_fields),
                    ) == Ok(Ordering::Greater)
                })
            })
            .collect::<Vec<_>>();
        let Some(next_physical) = next else {
            break;
        };
        if visible.len() > limit as usize {
            let cutoff = visible
                .get(limit as usize)
                .and_then(|row| row.field(order[0].field()))
                .ok_or(QueryExecutionError::InvalidProgram)?;
            let physical_tail = rows
                .last()
                .and_then(|row| row.field(order[0].field()))
                .ok_or(QueryExecutionError::InvalidProgram)?;
            if aggregate_scalar_order(cutoff, physical_tail)? != Ordering::Equal {
                stopped_with_more = true;
                break;
            }
        }
        resume = Some((next_physical, false));
    }

    if !mixed
        && rows.windows(2).any(|pair| {
            partition_set_physical_row_order(&pair[0], &pair[1], fields, key_fields, direction)
                != Ok(Ordering::Less)
        })
    {
        return Err(QueryExecutionError::BackendIntegrity);
    }
    if let Some(marker) = prior_marker {
        rows.retain(|row| {
            partition_set_marker_values(row, route_field, order, key_fields)
                .and_then(|values| partition_set_value_order(&values, marker, order, key_fields))
                == Ok(Ordering::Greater)
        });
    }
    let limit = usize::try_from(limit).map_err(|_| QueryExecutionError::BoundExceeded)?;
    if rows.len() > limit {
        rows.truncate(limit);
        stopped_with_more = true;
    }
    Ok((rows, epoch.unwrap_or(0), stopped_with_more))
}

#[allow(clippy::too_many_arguments)]
fn partition_set_group_resume(
    step: &QueryAccessStep,
    predicates: &[BoundPredicate],
    fields: &[String],
    route_field: &str,
    order: &[QueryRootOrderTermV1],
    direction: AccessDirection,
    route: &CanonicalValue,
    marker: &[CanonicalValue],
) -> Result<(Vec<u8>, bool), QueryExecutionError> {
    let first_order = marker
        .first()
        .ok_or(QueryExecutionError::InvalidContinuation)?;
    let schema = step
        .internal_index_key_schema()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let mut physical = partition_set_exact_physical_prefix(
        step,
        schema,
        fields,
        route_field,
        order,
        predicates,
        route,
    )?;
    let order_start = fields
        .len()
        .checked_sub(order.len())
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let descriptor = step
        .internal_operational_index_descriptor()
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let component = descriptor
        .component(
            order_start,
            fields
                .get(order_start)
                .ok_or(QueryExecutionError::InvalidProgram)?,
        )
        .ok_or(QueryExecutionError::InvalidProgram)?;
    push_exact_index_prefix_value(schema, component, &mut physical, first_order)?;
    if order.is_empty() || physical.len() > schema.components().len() {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let prefix = schema
        .encode_index_prefix(&physical)
        .map_err(|_| QueryExecutionError::InvalidProgram)?
        .as_bytes()
        .to_vec();
    match direction {
        AccessDirection::Forward => Ok((prefix, true)),
        AccessDirection::Reverse => Ok((
            exclusive_prefix_end_v1(&prefix).ok_or(QueryExecutionError::InvalidProgram)?,
            false,
        )),
    }
}

fn bind_dependent_point_batch(
    step: &QueryAccessStep,
    parameters: &QueryParameters,
    bindings: &BTreeMap<String, QueryStepRows>,
    source_binding: &str,
    source_field: &str,
    limit: u64,
) -> Result<Vec<Vec<BoundPredicate>>, QueryExecutionError> {
    let source_rows = bindings
        .get(source_binding)
        .ok_or(QueryExecutionError::InvalidProgram)?;
    let source_field_pos = source_rows
        .first()
        .and_then(|row| row.field_pos(source_field));
    let values = if step.distinct_dependent_keys() {
        let mut refs = Vec::<&CanonicalValue>::with_capacity(source_rows.len());
        let mut first_disc: Option<std::mem::Discriminant<CanonicalValue>> = None;
        let mut all_scalar_fast = true;
        for row in source_rows {
            let value_ref = row
                .field_at(source_field_pos, source_field)
                .ok_or_else(|| QueryExecutionError::MissingField {
                    entity: row.entity().to_owned(),
                    field: source_field.to_owned(),
                })?;
            if matches!(value_ref, CanonicalValue::Null) {
                continue;
            }
            if all_scalar_fast {
                if canonical_scalar_encoded_len(value_ref).is_some() {
                    let disc = std::mem::discriminant(value_ref);
                    match first_disc {
                        None => first_disc = Some(disc),
                        Some(prev) if prev == disc => {}
                        Some(_) => all_scalar_fast = false,
                    }
                } else {
                    all_scalar_fast = false;
                }
            }
            refs.push(value_ref);
        }
        if all_scalar_fast {
            refs.sort_unstable_by(|a, b| {
                canonical_scalar_encoded_cmp(a, b).unwrap_or(Ordering::Equal)
            });
            refs.dedup();
            if refs.len() as u64 > limit || refs.len() as u64 > step.maximum_rows() {
                return Err(QueryExecutionError::BoundExceeded);
            }
            refs.into_iter().cloned().collect::<Vec<_>>()
        } else {
            let mut seen_values = Vec::<&CanonicalValue>::new();
            let mut keyed = Vec::with_capacity(refs.len());
            for value_ref in refs {
                if seen_values.len() <= 64 {
                    if seen_values.contains(&value_ref) {
                        continue;
                    }
                    seen_values.push(value_ref);
                }
                let encoded = encode_canonical_value(value_ref).map_err(|_| {
                    QueryExecutionError::InvalidDependentKey {
                        binding: source_binding.to_owned(),
                        field: source_field.to_owned(),
                    }
                })?;
                keyed.push((encoded, value_ref));
            }
            keyed.sort_by(|left, right| left.0.cmp(&right.0));
            keyed.dedup_by(|left, right| left.0 == right.0);
            if keyed.len() as u64 > limit || keyed.len() as u64 > step.maximum_rows() {
                return Err(QueryExecutionError::BoundExceeded);
            }
            keyed
                .into_iter()
                .map(|(_, value)| value.clone())
                .collect::<Vec<_>>()
        }
    } else {
        if source_rows.len() as u64 > limit || source_rows.len() as u64 > step.maximum_rows() {
            return Err(QueryExecutionError::BoundExceeded);
        }
        let values = source_rows
            .iter()
            .map(|row| {
                row.field_at(source_field_pos, source_field)
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
        let all_scalar_fast = values.first().is_some_and(|first| {
            canonical_scalar_encoded_len(first).is_some()
                && values.iter().all(|v| {
                    canonical_scalar_encoded_len(v).is_some()
                        && std::mem::discriminant(v) == std::mem::discriminant(first)
                })
        });
        if all_scalar_fast {
            if values.windows(2).any(|pair| {
                canonical_scalar_encoded_cmp(&pair[0], &pair[1]) != Some(Ordering::Less)
            }) {
                return Err(QueryExecutionError::InvalidDependentKey {
                    binding: source_binding.to_owned(),
                    field: source_field.to_owned(),
                });
            }
        } else {
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
        }
        values
    };

    if values.is_empty() {
        return Ok(Vec::new());
    }
    let first_dependent_value = &values[0];
    let mut template_predicates = Vec::with_capacity(step.predicates().len());
    let mut dependent_slots = Vec::new();
    for (idx, predicate) in step.predicates().iter().enumerate() {
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
                dependent_slots.push(idx);
                (QueryPredicateOperator::Equal, first_dependent_value.clone())
            }
            QueryPredicateValue::BindingFieldSet { .. } => {
                return Err(QueryExecutionError::InvalidProgram);
            }
            QueryPredicateValue::CandidateBinding { .. } => {
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
        template_predicates.push(BoundPredicate {
            field: predicate.field().to_owned(),
            operator,
            value,
        });
    }
    let mut result = Vec::with_capacity(values.len());
    result.push(template_predicates);
    for dependent_value in &values[1..] {
        let mut predicates = result[0].clone();
        for &slot in &dependent_slots {
            predicates[slot].value = dependent_value.clone();
        }
        result.push(predicates);
    }
    Ok(result)
}

fn validate_canonical_set(value: &CanonicalValue) -> Result<(), ()> {
    let CanonicalValue::List(values) = value else {
        return Err(());
    };
    if values.values().len() > MAX_QUERY_PARAMETERS {
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
    predicate_values_match(row.entity(), predicates, |name| row.field(name))
}

fn predicates_match_at_positions(
    row: &QueryRow,
    predicates: &[BoundPredicate],
    positions: Option<&[usize]>,
) -> Result<bool, QueryExecutionError> {
    if let Some(positions) = positions
        && positions.len() == predicates.len()
        && let Some(values) = &row.values
        && values.len() == row.fields.len()
    {
        for (predicate, &pos) in predicates.iter().zip(positions) {
            let Some(actual) = values.get(pos) else {
                let mut idx = 0;
                return predicate_values_match(row.entity(), predicates, |_name| {
                    let pos = positions[idx];
                    idx += 1;
                    values.get(pos)
                });
            };
            match predicate.operator {
                QueryPredicateOperator::Equal => {
                    if actual != &predicate.value {
                        return Ok(false);
                    }
                }
                QueryPredicateOperator::NotEqual => {
                    if actual == &predicate.value {
                        return Ok(false);
                    }
                }
                QueryPredicateOperator::IsNotNull => {
                    if matches!(actual, CanonicalValue::Null) {
                        return Ok(false);
                    }
                }
                QueryPredicateOperator::IsNull => {
                    if !matches!(actual, CanonicalValue::Null) {
                        return Ok(false);
                    }
                }
                QueryPredicateOperator::Exists => {}
                _ => {
                    let mut idx = 0;
                    return predicate_values_match(row.entity(), predicates, |_name| {
                        let pos = positions[idx];
                        idx += 1;
                        values.get(pos)
                    });
                }
            }
        }
        return Ok(true);
    }
    predicates_match(row, predicates)
}

/// Applies the one authoritative predicate implementation to a sealed
/// positional covered row before storage admits it to a batch.
#[doc(hidden)]
pub fn covered_row_matches_predicates_v1(
    layout: &CoveredResultLayoutV1,
    values: &[CanonicalValue],
    predicates: &[BoundPredicate],
) -> Result<bool, QueryExecutionError> {
    if values.len() != layout.fields().len() {
        return Err(QueryExecutionError::InvalidProgram);
    }
    predicate_values_match(layout.entity(), predicates, |name| {
        layout
            .fields()
            .binary_search_by(|field| field.name().cmp(name))
            .ok()
            .and_then(|position| values.get(position))
    })
}

#[doc(hidden)]
pub fn covered_row_matches_predicates_at_positions_v1(
    layout: &CoveredResultLayoutV1,
    values: &[CanonicalValue],
    predicates: &[BoundPredicate],
    positions: Option<&[usize]>,
) -> Result<bool, QueryExecutionError> {
    if values.len() != layout.fields().len() {
        return Err(QueryExecutionError::InvalidProgram);
    }
    if let Some(positions) = positions
        && positions.len() == predicates.len()
    {
        let mut idx = 0;
        return predicate_values_match(layout.entity(), predicates, |_name| {
            let pos = positions[idx];
            idx += 1;
            values.get(pos)
        });
    }
    covered_row_matches_predicates_v1(layout, values, predicates)
}

fn predicate_values_match<'a>(
    entity: &str,
    predicates: &[BoundPredicate],
    mut field: impl FnMut(&str) -> Option<&'a CanonicalValue>,
) -> Result<bool, QueryExecutionError> {
    for predicate in predicates {
        let actual = field(&predicate.field);
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
            entity: entity.to_owned(),
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
        (CanonicalValue::Bool(left), CanonicalValue::Bool(right)) => Some(left.cmp(right)),
        (CanonicalValue::Bytes(left), CanonicalValue::Bytes(right)) => {
            Some(left.as_bytes().cmp(right.as_bytes()))
        }
        (CanonicalValue::Decimal(left), CanonicalValue::Decimal(right))
            if left.spec() == right.spec() =>
        {
            Some(left.coefficient().cmp(&right.coefficient()))
        }
        (CanonicalValue::Money(left), CanonicalValue::Money(right))
            if left.currency() == right.currency()
                && left.amount().spec() == right.amount().spec() =>
        {
            Some(
                left.amount()
                    .coefficient()
                    .cmp(&right.amount().coefficient()),
            )
        }
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
mod pipeline_clone_tests;

#[cfg(test)]
mod review_regressions {
    use super::*;
    use riffdb_types::{CurrencyCode, Decimal, DecimalSpec, Money};

    // req: OQ-019
    #[test]
    fn review_scalar_ranges_use_typed_business_order() {
        let spec = DecimalSpec::new(12, 2).unwrap();
        let negative = Decimal::new(spec, -100).unwrap();
        let positive = Decimal::new(spec, 200).unwrap();
        let currency = CurrencyCode::new("USD").unwrap();
        for (low, high) in [
            (CanonicalValue::Bool(false), CanonicalValue::Bool(true)),
            (
                CanonicalValue::bytes(vec![1]).unwrap(),
                CanonicalValue::bytes(vec![2]).unwrap(),
            ),
            (
                CanonicalValue::Decimal(negative),
                CanonicalValue::Decimal(positive),
            ),
            (
                CanonicalValue::Money(Money::new(currency, negative)),
                CanonicalValue::Money(Money::new(currency, positive)),
            ),
        ] {
            for (operator, expected) in [
                (QueryPredicateOperator::Less, true),
                (QueryPredicateOperator::LessEqual, true),
                (QueryPredicateOperator::Greater, false),
                (QueryPredicateOperator::GreaterEqual, false),
            ] {
                let predicate = BoundPredicate {
                    field: "value".into(),
                    operator,
                    value: high.clone(),
                };
                assert_eq!(
                    predicate_values_match("Entity", &[predicate], |_| Some(&low)),
                    Ok(expected)
                );
            }
            assert_eq!(scalar_order(&low, &high), Some(Ordering::Less));
            assert_eq!(scalar_order(&high, &low), Some(Ordering::Greater));
            assert_eq!(scalar_order(&low, &low), Some(Ordering::Equal));
        }
        assert_eq!(
            scalar_order(&CanonicalValue::Null, &CanonicalValue::I64(1)),
            None
        );
        assert_eq!(
            scalar_order(
                &CanonicalValue::Decimal(negative),
                &CanonicalValue::Decimal(
                    Decimal::new(DecimalSpec::new(12, 3).unwrap(), 200).unwrap()
                )
            ),
            None
        );
    }
}
