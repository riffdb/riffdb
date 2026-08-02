//! Columnar projected-query application surface (ADR-0086 / ADR-0087 / CP2b).
//!
//! The trait lives outside the symbolic-query methods of
//! [`crate::ApplicationService`]; it is composed into `ApplicationService` as a
//! separate supertrait so adapters opt in without a generic `fn execute(`.

use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::{Duration, Instant};

use riffdb_columnar::{
    AggregateOp, ColumnPredicate, ColumnarQueryRequest, DefinitionFingerprint, DegradedReason,
    GroupBySpec, OrderSpec, QueryBudget, QueryError, QueryResult, QueryRow, QueryRows,
    RebuildingReason, SortDirection, query_snapshot,
};

pub use riffdb_columnar::{
    DegradedReason as ProjectedDegradedReason, RebuildingReason as ProjectedRebuildingReason,
    SortDirection as ProjectedSortDirection,
};
use riffdb_errors::{
    ApplicationErrorCode, PublicError, ValidationCode, ValidationIssue, ValidationIssues,
    ValidationPath,
};
use riffdb_policy::{
    ApplicationQueryAccessRequirement, ApplicationQueryTarget, OperationRequest,
    OperationTenantScope,
};
use riffdb_types::{
    CanonicalValue, CommitToken, FieldId, FreshnessPolicy, FrontierPosition, ProjectionFrontier,
    QueryCostVectorV1, QueryPlanHash, RequestId, ServiceOperationV1, hash_query_plan,
};

use crate::columnar_notification::ColumnarWake;
use crate::orchestration::AuditScope;
use crate::query_discovery_operations::{
    finish_failure, finish_success, prepare_selected_contract,
};
use crate::symbolic_query::SymbolicContractSelector;
use crate::{
    ColumnarLifecycle, ColumnarObservation, ColumnarPortError, InternalDefect, RequestContext,
    RiffDbService, RiffDbServiceInner, ServiceAuditTargetMap, ServiceFailure, ServiceFuture,
    ServiceResult,
};

/// Maximum register/observe cycles for one causal projected-query wait.
const MAX_COLUMNAR_WAIT_OBSERVATIONS: usize = 64;

/// Default hard row bound when the request omits `limit`.
const DEFAULT_PROJECTED_ROW_LIMIT: u16 = 1_024;

/// Default encoded-byte cost estimate per returned field value.
const DEFAULT_BYTES_PER_VALUE: u64 = 64;

/// Application surface for columnar projected queries.
pub trait ProjectedQueryApplication: Send + Sync {
    /// Executes one org-scoped projected query under freshness policy.
    fn execute_projected_query(
        &self,
        context: RequestContext,
        request: ExecuteProjectedQueryRequest,
    ) -> ServiceFuture<'_, ExecuteProjectedQueryResult>;
}

impl ProjectedQueryApplication for RiffDbService {
    fn execute_projected_query(
        &self,
        context: RequestContext,
        request: ExecuteProjectedQueryRequest,
    ) -> ServiceFuture<'_, ExecuteProjectedQueryResult> {
        let inner = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::ExecuteProjectedQuery,
            ingress,
            async move { execute_projected_query(&inner, context, request).await },
        )
    }
}

/// One exact projected-query request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecuteProjectedQueryRequest {
    contract: SymbolicContractSelector,
    projection_name: String,
    body: ProjectedQueryBody,
    freshness: FreshnessPolicy,
    request_id: Option<RequestId>,
}

impl ExecuteProjectedQueryRequest {
    /// Constructs one complete projected-query request.
    #[must_use]
    pub fn new(
        contract: SymbolicContractSelector,
        projection_name: impl Into<String>,
        body: ProjectedQueryBody,
        freshness: FreshnessPolicy,
    ) -> Self {
        Self {
            contract,
            projection_name: projection_name.into(),
            body,
            freshness,
            request_id: None,
        }
    }

    /// Attaches an optional correlating request identity.
    #[must_use]
    pub const fn with_request_id(mut self, request_id: RequestId) -> Self {
        self.request_id = Some(request_id);
        self
    }

    /// Contract selection used for preparation and audit.
    #[must_use]
    pub const fn contract(&self) -> &SymbolicContractSelector {
        &self.contract
    }

    /// Columnar projection name.
    #[must_use]
    pub fn projection_name(&self) -> &str {
        &self.projection_name
    }

    /// Query body (org scope, select, predicates, …).
    #[must_use]
    pub const fn body(&self) -> &ProjectedQueryBody {
        &self.body
    }

    /// Freshness policy.
    #[must_use]
    pub const fn freshness(&self) -> &FreshnessPolicy {
        &self.freshness
    }

    /// Optional correlating request identity.
    #[must_use]
    pub const fn request_id(&self) -> Option<RequestId> {
        self.request_id
    }
}

/// Name-addressed projected query body. Field names resolve against the entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedQueryBody {
    select: Vec<String>,
    org_scope: CanonicalValue,
    predicates: Vec<ProjectedColumnPredicate>,
    order: Vec<ProjectedOrderSpec>,
    limit: Option<usize>,
    group_by: Option<ProjectedGroupBySpec>,
    aggregate: Option<ProjectedAggregateOp>,
    budget: QueryBudget,
}

impl ProjectedQueryBody {
    /// Constructs a body with required org scope and empty optional clauses.
    #[must_use]
    pub fn new(org_scope: CanonicalValue) -> Self {
        Self {
            select: Vec::new(),
            org_scope,
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            group_by: None,
            aggregate: None,
            budget: QueryBudget::default(),
        }
    }

    /// Sets the selected field names (empty = all projected fields).
    #[must_use]
    pub fn with_select(mut self, select: Vec<String>) -> Self {
        self.select = select;
        self
    }

    /// Sets conjunctive predicates.
    #[must_use]
    pub fn with_predicates(mut self, predicates: Vec<ProjectedColumnPredicate>) -> Self {
        self.predicates = predicates;
        self
    }

    /// Sets sort keys (primary-key tie-break always applied by the engine).
    #[must_use]
    pub fn with_order(mut self, order: Vec<ProjectedOrderSpec>) -> Self {
        self.order = order;
        self
    }

    /// Sets the post-sort limit.
    #[must_use]
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Sets optional group-by.
    #[must_use]
    pub fn with_group_by(mut self, group_by: Option<ProjectedGroupBySpec>) -> Self {
        self.group_by = group_by;
        self
    }

    /// Sets optional whole-set aggregate (without group-by).
    #[must_use]
    pub fn with_aggregate(mut self, aggregate: Option<ProjectedAggregateOp>) -> Self {
        self.aggregate = aggregate;
        self
    }

    /// Sets scan/group budgets.
    #[must_use]
    pub fn with_budget(mut self, budget: QueryBudget) -> Self {
        self.budget = budget;
        self
    }

    /// Organization scope value (single tenant partition).
    #[must_use]
    pub const fn org_scope(&self) -> &CanonicalValue {
        &self.org_scope
    }

    /// Selected field names.
    #[must_use]
    pub fn select(&self) -> &[String] {
        &self.select
    }

    /// Predicates.
    #[must_use]
    pub fn predicates(&self) -> &[ProjectedColumnPredicate] {
        &self.predicates
    }

    /// Order specs.
    #[must_use]
    pub fn order(&self) -> &[ProjectedOrderSpec] {
        &self.order
    }

    /// Optional limit.
    #[must_use]
    pub const fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Optional group-by.
    #[must_use]
    pub const fn group_by(&self) -> Option<&ProjectedGroupBySpec> {
        self.group_by.as_ref()
    }

    /// Optional aggregate.
    #[must_use]
    pub const fn aggregate(&self) -> Option<&ProjectedAggregateOp> {
        self.aggregate.as_ref()
    }

    /// Query budgets.
    #[must_use]
    pub const fn budget(&self) -> QueryBudget {
        self.budget
    }
}

/// Equality or range predicate addressed by field name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectedColumnPredicate {
    /// Field equals value.
    Eq {
        /// Field name on the projected entity.
        field: String,
        /// Comparison value.
        value: CanonicalValue,
    },
    /// Inclusive lower / exclusive upper range (either bound optional).
    Range {
        /// Field name on the projected entity.
        field: String,
        /// Inclusive lower bound.
        low: Option<CanonicalValue>,
        /// Exclusive upper bound.
        high: Option<CanonicalValue>,
    },
}

/// Sort key addressed by field name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedOrderSpec {
    /// Field name (projected field or primary-key component).
    pub field: String,
    /// Sort direction.
    pub direction: SortDirection,
}

/// Name-addressed aggregate operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectedAggregateOp {
    /// Count of matching rows.
    Count,
    /// Sum of a numeric column.
    Sum {
        /// Field name to sum.
        field: String,
    },
    /// Minimum of a column.
    Min {
        /// Field name to minimize.
        field: String,
    },
    /// Maximum of a column.
    Max {
        /// Field name to maximize.
        field: String,
    },
}

/// Name-addressed group-by specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedGroupBySpec {
    /// Grouping columns.
    pub keys: Vec<String>,
    /// Aggregates computed per group.
    pub aggregates: Vec<ProjectedAggregateOp>,
}

/// Typed projected-query outcomes (ADR-0086 §7).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecuteProjectedQueryResult {
    /// Published snapshot served under the requested freshness policy.
    Ready {
        /// Selected projected field names (wire order for each row's cells).
        fields: Vec<String>,
        /// Selected projected field ids aligned with [`Self::Ready::fields`].
        field_ids: Vec<FieldId>,
        /// Entity primary-key field names aligned with each row's primary_key vector.
        primary_key_fields: Vec<String>,
        /// Matching rows (empty when the engine returned aggregates/groups only).
        rows: Vec<QueryRow>,
        /// Optional non-row result (aggregate / groups).
        result: Option<QueryResult>,
        /// Served projection frontier.
        frontier: ProjectionFrontier,
        /// Application head known at serve time.
        head: ProjectionFrontier,
        /// Commit token corresponding to the served frontier when sequenced.
        commit_token: Option<CommitToken>,
    },
    /// Freshness policy not yet satisfied.
    Lagging {
        /// Required frontier derived from the causal token or bounded policy.
        required: ProjectionFrontier,
        /// Current projection frontier.
        current: ProjectionFrontier,
        /// Application head.
        head: ProjectionFrontier,
        /// Sequence distance backlog (head − current), when both are sequenced.
        lag_sequences: Option<u64>,
        /// Optional retry hint.
        retry_after: Option<Duration>,
    },
    /// Catch-up before first publication.
    Building {
        /// Processed position during catch-up.
        applied_through: ProjectionFrontier,
        /// Known application head.
        head: ProjectionFrontier,
    },
    /// Rebuild in progress.
    Rebuilding {
        /// Closed reason code.
        reason: RebuildingReason,
        /// Progress numerator.
        progress_applied: u64,
        /// Progress denominator (0 = unknown).
        progress_total: u64,
    },
    /// Degraded health while still identified.
    Degraded {
        /// Closed reason code.
        reason: DegradedReason,
        /// Current frontier while degraded.
        current_frontier: ProjectionFrontier,
    },
    /// Durable definition fingerprint mismatch.
    Invalid {
        /// Fingerprint expected by the registered definition.
        expected_fingerprint: DefinitionFingerprint,
        /// Fingerprint found in durable state.
        found_fingerprint: DefinitionFingerprint,
    },
}

async fn execute_projected_query(
    service: &RiffDbServiceInner,
    context: RequestContext,
    request: ExecuteProjectedQueryRequest,
) -> ServiceResult<ExecuteProjectedQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteProjectedQuery;

    let Some(columnar) = service.providers.columnar.as_ref() else {
        return Err(PublicError::storage_unavailable().into());
    };

    let bundle =
        prepare_selected_contract(service, &context, request.contract().selection(), OPERATION)
            .await?;
    if !request.contract().matches(&bundle) {
        return Err(PublicError::contract_mismatch(bundle.contract_version()).into());
    }

    let definition = match columnar.definition(request.projection_name()) {
        Some(definition) => definition,
        None => {
            return Err(application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::QueryInvalid,
            ));
        }
    };

    let entity = bundle
        .bundle()
        .schema()
        .entity(definition.entity_type_id())
        .ok_or_else(|| {
            application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::QueryInvalid,
            )
        })?;

    let resolved = resolve_body_fields(entity, &definition, request.body())?;

    // B7 value-level shared derivation: partition authz and engine org_scope
    // both derive from this single CanonicalValue. Codecs are distinct framings
    // (partition key envelope vs ADR-0011 document); never re-parse wire org twice.
    let org_scope = request.body().org_scope().clone();
    let partition = {
        let aggregate = bundle
            .bundle()
            .schema()
            .aggregate_for_entity(definition.entity_type_id())
            .ok_or_else(|| {
                application_validation_failure(
                    ValidationCode::InvalidValue,
                    ApplicationErrorCode::QueryInvalid,
                )
            })?;
        aggregate
            .keys()
            .partition_schema()
            .encode_partition(std::slice::from_ref(&org_scope))
            .map_err(|_| {
                application_validation_failure(
                    ValidationCode::InvalidValue,
                    ApplicationErrorCode::QueryInvalid,
                )
            })?
    };

    let maximum_rows = row_limit(request.body().limit());
    let non_key_fields = resolved.authorization_fields(entity);
    let access = ApplicationQueryAccessRequirement::new(
        definition.entity_type_id(),
        None,
        non_key_fields,
        maximum_rows,
    )
    .map_err(|_| {
        application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryInvalid,
        )
    })?;

    let cost = projected_cost_vector(maximum_rows.get(), resolved.referenced_field_count())
        .ok_or_else(|| {
            application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::QueryInvalid,
            )
        })?;
    let plan_hash = projected_plan_hash(request.projection_name(), &resolved.plan_shape_fields);
    let target = ApplicationQueryTarget::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        plan_hash,
        context.ingress(),
        OperationTenantScope::global_only(),
        partition,
        vec![access],
        cost,
    )
    .map_err(|_| {
        application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryInvalid,
        )
    })?;

    let operation_request = OperationRequest::execute_projected_query(target);
    let targets =
        ServiceAuditTargetMap::symbolic_query(bundle.lineage().clone(), bundle.contract_version())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let begun = service
        .begin_invocation(
            &context,
            operation_request,
            targets,
            AuditScope::StandardRead,
        )
        .await?;

    // Read safe point 2 (pre-execute).
    begun
        .reauthorize_read(service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;

    let engine_request = resolved.into_engine_request(org_scope, request.body().budget());
    let outcome = match request.freshness() {
        FreshnessPolicy::Available => {
            serve_available(
                service,
                &context,
                &begun,
                columnar.as_ref(),
                request.projection_name(),
                &definition,
                entity,
                &engine_request,
            )
            .await
        }
        FreshnessPolicy::Bounded { max_lag_sequences } => {
            serve_bounded(
                service,
                &context,
                &begun,
                columnar.as_ref(),
                request.projection_name(),
                &definition,
                entity,
                &engine_request,
                *max_lag_sequences,
            )
            .await
        }
        FreshnessPolicy::Causal { token, max_wait } => {
            serve_causal(
                service,
                &context,
                &begun,
                columnar.as_ref(),
                request.projection_name(),
                &definition,
                entity,
                &engine_request,
                token,
                *max_wait,
            )
            .await
        }
    };

    match outcome {
        Ok(result) => {
            // Read safe point 3 (pre-release).
            begun.reauthorize_read(service, &context).await?;
            finish_success(service, &context, &begun).await?;
            Ok(result)
        }
        Err(failure) => Err(finish_failure(service, &context, &begun, failure).await),
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_available(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &crate::orchestration::BegunInvocation,
    columnar: &dyn crate::ColumnarProjectionPort,
    projection_name: &str,
    definition: &riffdb_columnar::RegisteredDefinition,
    entity: &riffdb_contract_ir::EntitySchema,
    engine_request: &ColumnarQueryRequest,
) -> ServiceResult<ExecuteProjectedQueryResult> {
    let observation = observe_projection(columnar, projection_name)?;
    if let Some(result) = lifecycle_outcome(&observation) {
        return Ok(result);
    }
    query_ready(
        service,
        context,
        begun,
        definition,
        entity,
        engine_request,
        &observation,
    )
}

#[allow(clippy::too_many_arguments)]
async fn serve_bounded(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &crate::orchestration::BegunInvocation,
    columnar: &dyn crate::ColumnarProjectionPort,
    projection_name: &str,
    definition: &riffdb_columnar::RegisteredDefinition,
    entity: &riffdb_contract_ir::EntitySchema,
    engine_request: &ColumnarQueryRequest,
    max_lag_sequences: u64,
) -> ServiceResult<ExecuteProjectedQueryResult> {
    let observation = observe_projection(columnar, projection_name)?;
    if let Some(result) = lifecycle_outcome(&observation) {
        return Ok(result);
    }
    let lag = observation
        .published_frontier()
        .lag_sequences(observation.head());
    match lag {
        Some(distance) if distance <= max_lag_sequences => {}
        _ => {
            // Bounded lagging reports the application head as the required fence.
            let required = ProjectionFrontier::new(
                observation.head().history_incarnation(),
                observation.head().position(),
            );
            return Ok(ExecuteProjectedQueryResult::Lagging {
                required,
                current: observation.published_frontier().clone(),
                head: observation.head().clone(),
                lag_sequences: lag,
                retry_after: None,
            });
        }
    }
    query_ready(
        service,
        context,
        begun,
        definition,
        entity,
        engine_request,
        &observation,
    )
}

#[allow(clippy::too_many_arguments)]
/// CAPACITY NOTE: the causal wait parks on a synchronous condvar
/// (`wait_controlled`) from within an async context, holding one runtime
/// worker thread for up to `max_wait`. Waits are deadline-bounded and
/// registrations are capped (`MAX_COLUMNAR_WAITERS`), so this is safe but
/// not free: sustained high-fanout causal reads should move this wait onto
/// a dedicated blocking pool or an async notifier before production load.
async fn serve_causal(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &crate::orchestration::BegunInvocation,
    columnar: &dyn crate::ColumnarProjectionPort,
    projection_name: &str,
    definition: &riffdb_columnar::RegisteredDefinition,
    entity: &riffdb_contract_ir::EntitySchema,
    engine_request: &ColumnarQueryRequest,
    token: &CommitToken,
    max_wait: Duration,
) -> ServiceResult<ExecuteProjectedQueryResult> {
    let wait_deadline = {
        let policy_deadline = Instant::now() + max_wait;
        let request_deadline = context.control().deadline();
        policy_deadline.min(request_deadline)
    };
    let cancellation = columnar.notifier().cancellation();
    let mut observation_count = 0usize;
    let mut last_observation: Option<ColumnarObservation> = None;

    loop {
        if context.control().is_cancelled() {
            return Err(ServiceFailure::Cancelled);
        }
        if context.control().is_deadline_exceeded() || Instant::now() >= wait_deadline {
            break;
        }

        let registration = columnar
            .notifier()
            .register(projection_name.to_owned())
            .map_err(|error| match error.kind() {
                crate::columnar_notification::ColumnarNotificationErrorKind::WaiterCapacityExceeded => {
                    PublicError::storage_unavailable().into()
                }
                crate::columnar_notification::ColumnarNotificationErrorKind::Integrity => {
                    service.internal_failure(
                        ServiceOperationV1::ExecuteProjectedQuery,
                        InternalDefect::LowerIntegrity,
                    )
                }
            })?;

        let observation = observe_projection(columnar, projection_name)?;
        observation_count = observation_count.saturating_add(1);
        if let Some(result) = lifecycle_outcome(&observation) {
            drop(registration);
            return Ok(result);
        }
        if observation.published_frontier().satisfies(token) {
            drop(registration);
            return query_ready(
                service,
                context,
                begun,
                definition,
                entity,
                engine_request,
                &observation,
            );
        }

        last_observation = Some(observation);
        if observation_count >= MAX_COLUMNAR_WAIT_OBSERVATIONS {
            break;
        }
        if Instant::now() >= wait_deadline {
            drop(registration);
            break;
        }

        let wake = registration
            .wait_controlled(wait_deadline, &cancellation)
            .map_err(|_| {
                service.internal_failure(
                    ServiceOperationV1::ExecuteProjectedQuery,
                    InternalDefect::LowerIntegrity,
                )
            })?;
        match wake {
            ColumnarWake::Cancelled => return Err(ServiceFailure::Cancelled),
            ColumnarWake::TimedOut => break,
            ColumnarWake::Notified => {
                // Mandatory post-wake safe point before the next observation.
                begun.reauthorize_read(service, context).await?;
            }
        }
    }

    let observation = match last_observation {
        Some(observation) => observation,
        None => observe_projection(columnar, projection_name)?,
    };
    if let Some(result) = lifecycle_outcome(&observation) {
        return Ok(result);
    }
    if observation.published_frontier().satisfies(token) {
        return query_ready(
            service,
            context,
            begun,
            definition,
            entity,
            engine_request,
            &observation,
        );
    }
    let required = ProjectionFrontier::new(
        token.history_incarnation(),
        FrontierPosition::AppliedThrough(token.commit_sequence()),
    );
    let remaining = wait_deadline.saturating_duration_since(Instant::now());
    Ok(ExecuteProjectedQueryResult::Lagging {
        required,
        current: observation.published_frontier().clone(),
        head: observation.head().clone(),
        lag_sequences: observation
            .published_frontier()
            .lag_sequences(observation.head()),
        retry_after: Some(remaining).filter(|duration| !duration.is_zero()),
    })
}

fn observe_projection(
    columnar: &dyn crate::ColumnarProjectionPort,
    projection_name: &str,
) -> ServiceResult<ColumnarObservation> {
    columnar
        .observe(projection_name)
        .map_err(|error| match error {
            ColumnarPortError::Unavailable => PublicError::storage_unavailable().into(),
            ColumnarPortError::Integrity => PublicError::storage_unavailable().into(),
        })
}

fn lifecycle_outcome(observation: &ColumnarObservation) -> Option<ExecuteProjectedQueryResult> {
    if !observation.has_published() {
        return Some(ExecuteProjectedQueryResult::Building {
            applied_through: observation.published_frontier().clone(),
            head: observation.head().clone(),
        });
    }
    match observation.lifecycle() {
        Some(ColumnarLifecycle::Building) => Some(ExecuteProjectedQueryResult::Building {
            applied_through: observation.published_frontier().clone(),
            head: observation.head().clone(),
        }),
        Some(ColumnarLifecycle::Invalid {
            expected_fingerprint,
            found_fingerprint,
        }) => Some(ExecuteProjectedQueryResult::Invalid {
            expected_fingerprint: *expected_fingerprint,
            found_fingerprint: *found_fingerprint,
        }),
        Some(ColumnarLifecycle::Rebuilding {
            reason,
            progress_applied,
            progress_total,
        }) => Some(ExecuteProjectedQueryResult::Rebuilding {
            reason: *reason,
            progress_applied: *progress_applied,
            progress_total: *progress_total,
        }),
        Some(ColumnarLifecycle::Degraded { reason }) => {
            Some(ExecuteProjectedQueryResult::Degraded {
                reason: *reason,
                current_frontier: observation.published_frontier().clone(),
            })
        }
        Some(ColumnarLifecycle::Ready) | None => None,
    }
}

fn query_ready(
    service: &RiffDbServiceInner,
    _context: &RequestContext,
    _begun: &crate::orchestration::BegunInvocation,
    definition: &riffdb_columnar::RegisteredDefinition,
    entity: &riffdb_contract_ir::EntitySchema,
    engine_request: &ColumnarQueryRequest,
    observation: &ColumnarObservation,
) -> ServiceResult<ExecuteProjectedQueryResult> {
    if !observation.has_published() {
        return Ok(ExecuteProjectedQueryResult::Building {
            applied_through: observation.published_frontier().clone(),
            head: observation.head().clone(),
        });
    }
    // Snapshot Arc was taken under the port; never hold an engine lock here.
    let snapshot = observation.snapshot_arc();
    let result = query_snapshot(definition, snapshot.as_ref(), engine_request)
        .map_err(|error| map_query_error(service, error))?;
    let frontier = observation.published_frontier().clone();
    let head = observation.head().clone();
    let commit_token = match frontier.position() {
        FrontierPosition::AppliedThrough(sequence) => {
            Some(CommitToken::new(frontier.history_incarnation(), sequence))
        }
        FrontierPosition::BeforeFirst => None,
    };
    Ok(match result {
        QueryResult::Rows(QueryRows {
            fields,
            primary_key_fields,
            rows,
        }) => ExecuteProjectedQueryResult::Ready {
            fields: field_ids_to_names(entity, &fields)?,
            field_ids: fields,
            primary_key_fields: field_ids_to_names(entity, &primary_key_fields)?,
            rows,
            result: None,
            frontier,
            head,
            commit_token,
        },
        other => ExecuteProjectedQueryResult::Ready {
            fields: Vec::new(),
            field_ids: Vec::new(),
            primary_key_fields: Vec::new(),
            rows: Vec::new(),
            result: Some(other),
            frontier,
            head,
            commit_token,
        },
    })
}

fn field_ids_to_names(
    entity: &riffdb_contract_ir::EntitySchema,
    fields: &[FieldId],
) -> ServiceResult<Vec<String>> {
    fields
        .iter()
        .map(|field_id| {
            entity
                .record()
                .field(*field_id)
                .map(|field| field.name().to_owned())
                .ok_or_else(|| {
                    application_validation_failure(
                        ValidationCode::InvalidValue,
                        ApplicationErrorCode::QueryInvalid,
                    )
                })
        })
        .collect()
}

fn map_query_error(service: &RiffDbServiceInner, error: QueryError) -> ServiceFailure {
    match error {
        QueryError::UnknownField { .. }
        | QueryError::DuplicateSelectField { .. }
        | QueryError::UnprojectedSelectField { .. }
        | QueryError::OrderNotValueOrderPreserving { .. }
        | QueryError::InvalidAggregate(_)
        | QueryError::InvalidOrgScope
        | QueryError::OrgScopeTypeMismatch { .. } => application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryInvalid,
        ),
        QueryError::ScanBudgetExceeded { .. } | QueryError::GroupCardinalityExceeded { .. } => {
            application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::QueryUnavailable,
            )
        }
        QueryError::PrimaryKeyDecode => service.internal_failure(
            ServiceOperationV1::ExecuteProjectedQuery,
            InternalDefect::LowerIntegrity,
        ),
    }
}

struct ResolvedBody {
    select: Vec<FieldId>,
    predicates: Vec<ColumnPredicate>,
    order: Vec<OrderSpec>,
    limit: Option<usize>,
    group_by: Option<GroupBySpec>,
    aggregate: Option<AggregateOp>,
    plan_shape_fields: Vec<FieldId>,
    authz_fields: Vec<FieldId>,
}

impl ResolvedBody {
    fn authorization_fields(&self, entity: &riffdb_contract_ir::EntitySchema) -> Vec<FieldId> {
        let pk = entity.primary_key_fields();
        let mut fields = self
            .authz_fields
            .iter()
            .copied()
            .filter(|field| !pk.contains(field))
            .collect::<Vec<_>>();
        fields.sort_unstable();
        fields.dedup();
        fields
    }

    fn referenced_field_count(&self) -> u64 {
        self.authz_fields.len() as u64
    }

    fn into_engine_request(
        self,
        org_scope: CanonicalValue,
        budget: QueryBudget,
    ) -> ColumnarQueryRequest {
        ColumnarQueryRequest {
            org_scope,
            select: self.select,
            predicates: self.predicates,
            order: self.order,
            limit: self.limit,
            group_by: self.group_by,
            aggregate: self.aggregate,
            budget,
        }
    }
}

fn resolve_body_fields(
    entity: &riffdb_contract_ir::EntitySchema,
    definition: &riffdb_columnar::RegisteredDefinition,
    body: &ProjectedQueryBody,
) -> ServiceResult<ResolvedBody> {
    let mut authz_fields = Vec::new();
    let mut plan_shape_fields = Vec::new();

    let select = if body.select().is_empty() {
        let fields = definition.projected_fields().to_vec();
        authz_fields.extend(fields.iter().copied());
        plan_shape_fields.extend(fields.iter().copied());
        fields
    } else {
        let mut fields = Vec::with_capacity(body.select().len());
        for name in body.select() {
            let field = resolve_field_name(entity, name)?;
            ensure_projected(definition, field)?;
            fields.push(field);
            authz_fields.push(field);
            plan_shape_fields.push(field);
        }
        fields
    };

    let mut predicates = Vec::with_capacity(body.predicates().len());
    for predicate in body.predicates() {
        match predicate {
            ProjectedColumnPredicate::Eq { field, value } => {
                let field_id = resolve_field_name(entity, field)?;
                ensure_projected(definition, field_id)?;
                authz_fields.push(field_id);
                plan_shape_fields.push(field_id);
                predicates.push(ColumnPredicate::Eq {
                    field: field_id,
                    value: value.clone(),
                });
            }
            ProjectedColumnPredicate::Range { field, low, high } => {
                let field_id = resolve_field_name(entity, field)?;
                ensure_projected(definition, field_id)?;
                authz_fields.push(field_id);
                plan_shape_fields.push(field_id);
                predicates.push(ColumnPredicate::Range {
                    field: field_id,
                    low: low.clone(),
                    high: high.clone(),
                });
            }
        }
    }

    let mut order = Vec::with_capacity(body.order().len());
    for spec in body.order() {
        let field_id = resolve_field_name(entity, &spec.field)?;
        ensure_orderable(definition, field_id)?;
        authz_fields.push(field_id);
        plan_shape_fields.push(field_id);
        order.push(OrderSpec {
            field: field_id,
            direction: spec.direction,
        });
    }

    let aggregate = match body.aggregate() {
        None => None,
        Some(ProjectedAggregateOp::Count) => Some(AggregateOp::Count),
        Some(ProjectedAggregateOp::Sum { field }) => {
            let field_id = resolve_field_name(entity, field)?;
            ensure_projected(definition, field_id)?;
            authz_fields.push(field_id);
            plan_shape_fields.push(field_id);
            Some(AggregateOp::Sum { field: field_id })
        }
        Some(ProjectedAggregateOp::Min { field }) => {
            let field_id = resolve_field_name(entity, field)?;
            ensure_projected(definition, field_id)?;
            authz_fields.push(field_id);
            plan_shape_fields.push(field_id);
            Some(AggregateOp::Min { field: field_id })
        }
        Some(ProjectedAggregateOp::Max { field }) => {
            let field_id = resolve_field_name(entity, field)?;
            ensure_projected(definition, field_id)?;
            authz_fields.push(field_id);
            plan_shape_fields.push(field_id);
            Some(AggregateOp::Max { field: field_id })
        }
    };

    let group_by = match body.group_by() {
        None => None,
        Some(spec) => {
            let mut keys = Vec::with_capacity(spec.keys.len());
            for name in &spec.keys {
                let field_id = resolve_field_name(entity, name)?;
                ensure_projected(definition, field_id)?;
                authz_fields.push(field_id);
                plan_shape_fields.push(field_id);
                keys.push(field_id);
            }
            let mut aggregates = Vec::with_capacity(spec.aggregates.len());
            for op in &spec.aggregates {
                match op {
                    ProjectedAggregateOp::Count => aggregates.push(AggregateOp::Count),
                    ProjectedAggregateOp::Sum { field } => {
                        let field_id = resolve_field_name(entity, field)?;
                        ensure_projected(definition, field_id)?;
                        authz_fields.push(field_id);
                        plan_shape_fields.push(field_id);
                        aggregates.push(AggregateOp::Sum { field: field_id });
                    }
                    ProjectedAggregateOp::Min { field } => {
                        let field_id = resolve_field_name(entity, field)?;
                        ensure_projected(definition, field_id)?;
                        authz_fields.push(field_id);
                        plan_shape_fields.push(field_id);
                        aggregates.push(AggregateOp::Min { field: field_id });
                    }
                    ProjectedAggregateOp::Max { field } => {
                        let field_id = resolve_field_name(entity, field)?;
                        ensure_projected(definition, field_id)?;
                        authz_fields.push(field_id);
                        plan_shape_fields.push(field_id);
                        aggregates.push(AggregateOp::Max { field: field_id });
                    }
                }
            }
            Some(GroupBySpec { keys, aggregates })
        }
    };

    plan_shape_fields.sort_unstable();
    plan_shape_fields.dedup();
    authz_fields.sort_unstable();
    authz_fields.dedup();

    Ok(ResolvedBody {
        select,
        predicates,
        order,
        limit: body.limit(),
        group_by,
        aggregate,
        plan_shape_fields,
        authz_fields,
    })
}

fn resolve_field_name(
    entity: &riffdb_contract_ir::EntitySchema,
    name: &str,
) -> ServiceResult<FieldId> {
    entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .map(riffdb_contract_ir::FieldSchema::id)
        .ok_or_else(|| {
            application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::QueryInvalid,
            )
        })
}

fn ensure_projected(
    definition: &riffdb_columnar::RegisteredDefinition,
    field: FieldId,
) -> ServiceResult<()> {
    if definition.projected_fields().contains(&field) {
        Ok(())
    } else {
        Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryInvalid,
        ))
    }
}

fn ensure_orderable(
    definition: &riffdb_columnar::RegisteredDefinition,
    field: FieldId,
) -> ServiceResult<()> {
    if definition.projected_fields().contains(&field)
        || definition.primary_key_fields().contains(&field)
    {
        Ok(())
    } else {
        Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryInvalid,
        ))
    }
}

fn row_limit(limit: Option<usize>) -> NonZeroU16 {
    let raw = limit
        .and_then(|value| u16::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_PROJECTED_ROW_LIMIT);
    NonZeroU16::new(raw).expect("DEFAULT_PROJECTED_ROW_LIMIT is nonzero")
}

fn projected_cost_vector(rows: u16, fields: u64) -> Option<QueryCostVectorV1> {
    let rows = u64::from(rows);
    let projected_values = rows.saturating_mul(fields.max(1));
    let encoded_bytes = projected_values
        .saturating_mul(DEFAULT_BYTES_PER_VALUE)
        .max(1);
    QueryCostVectorV1::new(1, rows, 0, 0, rows, projected_values, encoded_bytes)
}

fn projected_plan_hash(projection_name: &str, fields: &[FieldId]) -> QueryPlanHash {
    let mut payload = Vec::with_capacity(8 + projection_name.len() + fields.len() * 4);
    payload.extend_from_slice(b"cp2b-projected\0");
    payload.extend_from_slice(&(projection_name.len() as u32).to_be_bytes());
    payload.extend_from_slice(projection_name.as_bytes());
    for field in fields {
        payload.extend_from_slice(&field.get().to_be_bytes());
    }
    hash_query_plan(&payload)
}

fn application_validation_failure(
    code: ValidationCode,
    application_code: ApplicationErrorCode,
) -> ServiceFailure {
    let error = PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        code,
        ValidationPath::root(),
    )));
    match error.with_application_code_hint(application_code) {
        Ok(error) => error.into(),
        Err(_) => ServiceFailure::from(PublicError::validation(ValidationIssues::one(
            ValidationIssue::new(ValidationCode::InvalidValue, ValidationPath::root()),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_columnar::{OrgKey, encode_org_scope_key};
    use riffdb_contract_ir::{KeyComponentSchema, KeyPurpose, KeySchema, ValueType};
    use riffdb_types::{
        AggregateTypeId, CanonicalBytes, CanonicalString, Date, Timestamp, encode_canonical_value,
    };

    /// B7: org-scope scalar value-level equivalence between partition codec and OrgKey.
    ///
    /// Does **not** claim `OrgKey.as_bytes() == PartitionKey.as_bytes()` — those are
    /// distinct framings of the same `CanonicalValue`.
    #[test]
    fn org_partition_value_level_equivalence() {
        let aggregate = AggregateTypeId::first();
        let samples: Vec<(ValueType, Vec<CanonicalValue>)> = vec![
            (
                ValueType::bool(),
                vec![CanonicalValue::Bool(false), CanonicalValue::Bool(true)],
            ),
            (
                ValueType::u64(),
                vec![
                    CanonicalValue::U64(0),
                    CanonicalValue::U64(1),
                    CanonicalValue::U64(u64::MAX),
                ],
            ),
            (
                ValueType::i64(),
                vec![
                    CanonicalValue::I64(0),
                    CanonicalValue::I64(-1),
                    CanonicalValue::I64(i64::MAX),
                ],
            ),
            (
                ValueType::uuid(),
                vec![
                    CanonicalValue::Uuid([0; 16]),
                    CanonicalValue::Uuid([0xff; 16]),
                    CanonicalValue::Uuid([
                        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76,
                        0x98, 0xba, 0xdc, 0xfe,
                    ]),
                ],
            ),
            (
                ValueType::string(64).expect("string type"),
                vec![
                    CanonicalValue::String(CanonicalString::new("a").expect("string")),
                    CanonicalValue::String(CanonicalString::new("org-b").expect("string")),
                ],
            ),
            (
                ValueType::bytes(64).expect("bytes type"),
                vec![
                    CanonicalValue::Bytes(CanonicalBytes::new(vec![0x00]).expect("bytes")),
                    CanonicalValue::Bytes(CanonicalBytes::new(vec![0xde, 0xad]).expect("bytes")),
                ],
            ),
            (
                ValueType::timestamp(),
                vec![
                    CanonicalValue::Timestamp(Timestamp::new(0, 0).expect("ts")),
                    CanonicalValue::Timestamp(Timestamp::new(1, 1_000_000).expect("ts")),
                ],
            ),
            (
                ValueType::date(),
                vec![
                    CanonicalValue::Date(Date::new(0)),
                    CanonicalValue::Date(Date::new(19_000)),
                ],
            ),
        ];

        for (value_type, values) in samples {
            let component =
                KeyComponentSchema::new(value_type.clone(), vec![]).expect("key component");
            let partition_schema =
                KeySchema::new(KeyPurpose::Partition(aggregate), vec![component])
                    .expect("partition schema");

            let mut org_keys = Vec::with_capacity(values.len());
            for value in &values {
                // (a) partition codec round-trips the value.
                let partition = partition_schema
                    .encode_partition(std::slice::from_ref(value))
                    .expect("encode_partition");
                let decoded = partition_schema
                    .decode_partition(&partition)
                    .expect("decode_partition");
                assert_eq!(
                    decoded.as_slice(),
                    std::slice::from_ref(value),
                    "partition decode must recover the exact CanonicalValue for {value_type:?}"
                );

                // (b) OrgKey::from_value equals encode_canonical_value / encode_org_scope_key.
                let org = OrgKey::from_value(value).expect("OrgKey::from_value");
                let via_hook = encode_org_scope_key(value).expect("encode_org_scope_key");
                let direct = encode_canonical_value(value).expect("encode_canonical_value");
                assert_eq!(org, via_hook);
                assert_eq!(org.as_bytes(), direct.as_slice());

                // (c) Document codecs are distinct framings — full bytes need not match.
                // Inequality is expected and acceptable; equality would also be fine for
                // some accidental encodings, so we only assert both succeed.
                let _ = (partition.as_bytes(), org.as_bytes());
                org_keys.push(org);
            }

            // Distinct values never collide on OrgKey.
            for (i, left) in org_keys.iter().enumerate() {
                for (j, right) in org_keys.iter().enumerate() {
                    if i != j {
                        assert_ne!(
                            left, right,
                            "distinct org-scope values must not share an OrgKey for {value_type:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn projected_plan_hash_is_stable_for_same_shape() {
        let a = projected_plan_hash("board", &[FieldId::first()]);
        let b = projected_plan_hash("board", &[FieldId::first()]);
        let c = projected_plan_hash("other", &[FieldId::first()]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn max_columnar_waiters_matches_notification_bound() {
        assert_eq!(crate::columnar_notification::MAX_COLUMNAR_WAITERS, 256);
    }
}
