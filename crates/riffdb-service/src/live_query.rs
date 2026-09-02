#![expect(
    clippy::expect_used,
    reason = "a nonterminal live-query state retains its initial update and last published result"
)]

//! API-neutral live named-query contracts and orchestration.

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use riffdb_catalog::{ActiveCatalogSnapshot, ValidatedQueryModule};
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
};
use riffdb_policy::{
    AuthorizedApplicationQuery, AuthorizedQueryRowPolicyContextV1, Decision, OperationRequest,
    resolve_authorized_query_row_policy_context,
};
use riffdb_query_executor::{
    BoundLiveQueryDependency, QueryExecutionError, bind_live_query_dependencies,
};
use riffdb_query_ir::{LiveQueryPlanV1, LiveUpdateStrategyV1, ReactiveOperationPlanV1};
use riffdb_types::{
    CanonicalValue, CommitSequence, ContractBundleHash, ContractLineage, ContractVersion,
    FrontierPosition, PartitionKeyHash, QueryModuleHash, QueryOperationName, QueryParameterHash,
    QueryPlanHash, ReactiveModuleHash, ReactiveOperationHash, ReactiveOperationName,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceOperationV1, Timestamp,
    canonical_value_encoded_len,
};

use crate::orchestration::AuditScope;
use crate::service::CommitSubscriberLease;
use crate::symbolic_query::{
    application_query_target, execute_authorized_query_page, execution_failure, load_query_module,
    query_parameter_hash,
};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AuthoritativeCommitNotification, AuthoritativeCommitSnapshot,
    AuthoritativeCommitSubscriptionRequest, AuthoritativeReadError, CommitNotificationSource,
    ExecuteSymbolicQueryResult, InternalDefect, PageLimit, PortAdmissionError, PortDriverStopped,
    QueryParameters, RequestContext, RiffDbService, RiffDbServiceInner, ServiceAuditTargetMap,
    ServiceFailure, ServiceFuture, ServiceResult, SharedEnumVariantNames, SymbolicResultField,
    SymbolicResultRecord, ensure_response_budget,
};

/// Maximum simultaneous live watches owned by one selected database.
pub const MAX_LIVE_QUERY_WATCHES_PER_DATABASE: usize = 128;
/// Maximum pending public updates retained by one watch.
pub const MAX_LIVE_QUERY_BUFFERED_UPDATES: usize = 32;
/// Maximum lifetime of one established watch before generated reconnect.
pub const MAX_LIVE_QUERY_LIFETIME: Duration = Duration::from_secs(15 * 60);
/// Maximum operations in one public patch before a reset is required.
pub const MAX_LIVE_QUERY_PATCH_OPERATIONS: usize = 500;
/// Maximum estimated public patch bytes before a reset is required.
pub const MAX_LIVE_QUERY_PATCH_BYTES: usize = 4 * 1_024 * 1_024;
/// Maximum self-contained cursor bytes.
pub const MAX_LIVE_QUERY_CURSOR_BYTES: usize = 4_096;
/// Maximum delay before an idle watch revalidates definition and authority.
pub const LIVE_QUERY_REVALIDATION_INTERVAL: Duration = Duration::from_secs(1);

/// Why a complete bounded result replaced retained client state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveQueryResetReason {
    /// The query selected another declared result branch.
    OutcomeChanged,
    /// A complete-key patch exceeded its item or byte ceiling.
    DiffLimitExceeded,
    /// The immutable reactive/query definition no longer matches the cursor.
    DefinitionChanged,
    /// Destructive restore changed the database history incarnation.
    HistoryChanged,
    /// A supplied self-contained cursor exceeded its bounded validity window.
    CursorExpired,
}

/// Why a watch closed without retaining protected state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveQueryTerminalReason {
    /// Current capability authority was revoked, expired, or changed.
    AuthorizationChanged,
    /// The bounded public update buffer could not accept another update.
    BufferPressure,
    /// The fixed watch lifetime elapsed.
    LifetimeExpired,
    /// An authoritative dependency became unavailable.
    ServiceUnavailable,
    /// Durable or compiler-owned evidence failed a semantic check.
    IntegrityFailure,
    /// The active contract or immutable module operation no longer matches.
    DefinitionChanged,
}

/// Closed injected wall-clock failure for cursor expiry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveQueryClockError;

/// Service-owned wall-clock boundary used only for bounded cursor validity.
pub trait LiveQueryClock: Send + Sync {
    /// Samples one canonical cursor-validation instant.
    fn now(&self) -> Result<Timestamp, LiveQueryClockError>;
}

/// Exact immutable reactive watch selection shared by every transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveNamedQuerySelection {
    module_hash: ReactiveModuleHash,
    operation_name: ReactiveOperationName,
    parameters: QueryParameters,
}

impl LiveNamedQuerySelection {
    /// Joins one immutable watch operation and canonical parameter map.
    #[must_use]
    pub const fn new(
        module_hash: ReactiveModuleHash,
        operation_name: ReactiveOperationName,
        parameters: QueryParameters,
    ) -> Self {
        Self {
            module_hash,
            operation_name,
            parameters,
        }
    }

    /// Immutable reactive module identity.
    #[must_use]
    pub const fn module_hash(&self) -> ReactiveModuleHash {
        self.module_hash
    }

    /// Exact watch operation name.
    #[must_use]
    pub const fn operation_name(&self) -> &ReactiveOperationName {
        &self.operation_name
    }

    /// Canonically name-ordered query parameters.
    #[must_use]
    pub const fn parameters(&self) -> &QueryParameters {
        &self.parameters
    }
}

/// One initial or resumed live named-query request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchLiveNamedQueryRequest {
    selection: LiveNamedQuerySelection,
    cursor: Option<LiveQueryCursor>,
}

impl WatchLiveNamedQueryRequest {
    /// Starts a new watch from a fresh complete snapshot.
    #[must_use]
    pub const fn new(selection: LiveNamedQuerySelection) -> Self {
        Self {
            selection,
            cursor: None,
        }
    }

    /// Supplies opaque resume evidence. It grants no authority.
    #[must_use]
    pub fn with_cursor(mut self, cursor: LiveQueryCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Exact selected watch.
    #[must_use]
    pub const fn selection(&self) -> &LiveNamedQuerySelection {
        &self.selection
    }

    /// Optional self-contained consistency fence.
    #[must_use]
    pub const fn cursor(&self) -> Option<&LiveQueryCursor> {
        self.cursor.as_ref()
    }
}

/// Move-only API-neutral live update source.
pub trait LiveQuerySubscription: Send {
    /// Returns the next bounded update or a repeated terminal value.
    fn next(&mut self)
    -> Pin<Box<dyn Future<Output = ServiceResult<LiveQueryUpdate>> + Send + '_>>;
}

/// Established live query. The first `next` always returns Snapshot or Reset.
pub struct WatchLiveNamedQueryResult {
    subscription: Box<dyn LiveQuerySubscription>,
}

impl WatchLiveNamedQueryResult {
    pub(crate) fn new(subscription: Box<dyn LiveQuerySubscription>) -> Self {
        Self { subscription }
    }

    /// Consumes the establishment result into its update source.
    #[must_use]
    pub fn into_subscription(self) -> Box<dyn LiveQuerySubscription> {
        self.subscription
    }

    /// Constructs one closed transport-only stream without semantic providers.
    #[cfg(feature = "test-fixtures")]
    #[doc(hidden)]
    #[must_use]
    pub fn transport_terminal_fixture() -> Self {
        struct TerminalFixtureSubscription {
            terminal: LiveQueryUpdate,
        }

        impl LiveQuerySubscription for TerminalFixtureSubscription {
            fn next(
                &mut self,
            ) -> Pin<Box<dyn Future<Output = ServiceResult<LiveQueryUpdate>> + Send + '_>>
            {
                let terminal = self.terminal.clone();
                Box::pin(async move { Ok(terminal) })
            }
        }

        let frontier = LiveQueryFrontier::new(1, 7).expect("test frontier is valid");
        Self::new(Box::new(TerminalFixtureSubscription {
            terminal: LiveQueryUpdate::Terminal(LiveQueryTerminal {
                reason: LiveQueryTerminalReason::AuthorizationChanged,
                last_frontier: frontier,
            }),
        }))
    }
}

impl std::fmt::Debug for WatchLiveNamedQueryResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("WatchLiveNamedQueryResult([SUBSCRIPTION])")
    }
}

/// API-neutral live named-query application surface.
pub trait LiveNamedQueryApplication: Send + Sync {
    /// Establishes a race-free exact named query watch.
    fn watch_live_named_query(
        &self,
        context: RequestContext,
        request: WatchLiveNamedQueryRequest,
    ) -> ServiceFuture<'_, WatchLiveNamedQueryResult>;
}

impl LiveNamedQueryApplication for RiffDbService {
    fn watch_live_named_query(
        &self,
        context: RequestContext,
        request: WatchLiveNamedQueryRequest,
    ) -> ServiceFuture<'_, WatchLiveNamedQueryResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::WatchNamedQuery, ingress, async move {
            establish_live_query(service, context, request).await
        })
    }
}

#[derive(Clone)]
struct PreparedLiveQuery {
    catalog: ActiveCatalogSnapshot,
    reactive_module_hash: ReactiveModuleHash,
    reactive_operation_hash: ReactiveOperationHash,
    reactive_operation_name: ReactiveOperationName,
    query_module: ValidatedQueryModule,
    query_name: QueryOperationName,
    program: Arc<riffdb_query_ir::QueryAccessProgramV1>,
    parameters: QueryParameters,
    parameter_hash: QueryParameterHash,
    dependencies: Vec<BoundLiveQueryDependency>,
    live_plan: LiveQueryPlanV1,
    policy_request: OperationRequest,
    row_policy_protected: bool,
}

impl PreparedLiveQuery {
    fn cursor_binding(&self) -> LiveCursorBinding {
        let pointer = self.catalog.pointer();
        let mut partitions = self
            .dependencies
            .iter()
            .map(BoundLiveQueryDependency::partition_hash)
            .collect::<Vec<_>>();
        partitions.sort_unstable();
        partitions.dedup();
        LiveCursorBinding {
            lineage: pointer.lineage().clone(),
            contract_version: pointer.contract_version(),
            contract_hash: pointer.bundle_hash(),
            reactive_module_hash: self.reactive_module_hash,
            reactive_operation_hash: self.reactive_operation_hash,
            reactive_operation_name: self.reactive_operation_name.clone(),
            query_module_hash: self.query_module.identity(),
            query_plan_hash: self.program.identity().hash(),
            query_name: self.query_name.clone(),
            parameter_hash: self.parameter_hash,
            partitions,
        }
    }

    fn commit_is_relevant(&self, commit: &AuthoritativeCommitSnapshot) -> bool {
        if row_policy_partition_might_be_affected(
            self.row_policy_protected,
            self.dependencies
                .iter()
                .map(BoundLiveQueryDependency::partition_hash),
            commit.partition_hash(),
        ) {
            // A policy can depend on a local relationship entity that is not
            // part of the query's ordinary result dependencies. Re-executing
            // on every same-partition commit ensures an ACL-only mutation can
            // remove a previously visible value before another delivery.
            return true;
        }
        self.dependencies.iter().any(|dependency| {
            dependency.partition_hash() == commit.partition_hash()
                && commit.affected_entities().iter().any(|affected| {
                    affected.key().entity_type_id() == dependency.entity_type_id()
                        && dependency
                            .entity_key()
                            .is_none_or(|key| key == affected.key())
                })
        })
    }
}

fn row_policy_partition_might_be_affected(
    protected: bool,
    dependency_partitions: impl IntoIterator<Item = PartitionKeyHash>,
    commit_partition: PartitionKeyHash,
) -> bool {
    protected
        && dependency_partitions
            .into_iter()
            .any(|partition| partition == commit_partition)
}

struct ServiceLiveQuerySubscription {
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    prepared: PreparedLiveQuery,
    source: Option<Box<dyn CommitNotificationSource>>,
    subscriber_lease: Option<CommitSubscriberLease>,
    lifetime_deadline: Instant,
    cursor_expires_at: Timestamp,
    last_result: Option<ExecuteSymbolicQueryResult>,
    last_frontier: LiveQueryFrontier,
    last_acknowledged: Option<CommitSequence>,
    pending_upper: Option<CommitSequence>,
    initial: Option<LiveQueryUpdate>,
    terminal: Option<LiveQueryTerminal>,
}

impl LiveQuerySubscription for ServiceLiveQuerySubscription {
    fn next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = ServiceResult<LiveQueryUpdate>> + Send + '_>> {
        Box::pin(async move {
            let update = self.next_update().await;
            if let Err(failure) = ensure_response_budget(&update) {
                self.release();
                return Err(failure);
            }
            Ok(update)
        })
    }
}

impl ServiceLiveQuerySubscription {
    async fn next_update(&mut self) -> LiveQueryUpdate {
        if let Some(terminal) = self.terminal {
            return LiveQueryUpdate::Terminal(terminal);
        }
        if self.initial.is_some() {
            match self.current_definition_matches().await {
                Ok(true) => {}
                Ok(false) => return self.end(LiveQueryTerminalReason::DefinitionChanged),
                Err(reason) => return self.end(reason),
            }
            if !self.authorized_now() {
                return self.end(LiveQueryTerminalReason::AuthorizationChanged);
            }
            return self
                .initial
                .take()
                .expect("initial update was checked as present");
        }
        loop {
            match self.next_sequence().await {
                Ok(Some(sequence)) => {
                    let commit = match self.read_commit(sequence).await {
                        Ok(commit) => commit,
                        Err(reason) => return self.end(reason),
                    };
                    if !self.prepared.commit_is_relevant(&commit) {
                        if !self.acknowledge(sequence) {
                            return self.end(LiveQueryTerminalReason::IntegrityFailure);
                        }
                        if sequence.get() <= self.last_frontier.application_head() {
                            continue;
                        }
                        let frontier = match LiveQueryFrontier::new(
                            self.service.identity.history_incarnation(),
                            sequence.get(),
                        ) {
                            Some(frontier) => frontier,
                            None => return self.end(LiveQueryTerminalReason::IntegrityFailure),
                        };
                        let cursor = match encode_live_cursor(
                            &self.prepared.cursor_binding(),
                            frontier,
                            self.cursor_expires_at,
                        ) {
                            Some(cursor) => cursor,
                            None => return self.end(LiveQueryTerminalReason::IntegrityFailure),
                        };
                        self.last_frontier = frontier;
                        return LiveQueryUpdate::Checkpoint(LiveQueryCheckpoint {
                            frontier,
                            cursor,
                        });
                    }
                    let next = match self.execute_current().await {
                        Ok(next) => next,
                        Err(reason) => return self.end(reason),
                    };
                    if !self.acknowledge(sequence) {
                        return self.end(LiveQueryTerminalReason::IntegrityFailure);
                    }
                    if next.application_head() < sequence.get() {
                        return self.end(LiveQueryTerminalReason::IntegrityFailure);
                    }
                    let frontier = match LiveQueryFrontier::new(
                        self.service.identity.history_incarnation(),
                        next.application_head(),
                    ) {
                        Some(frontier) => frontier,
                        None => return self.end(LiveQueryTerminalReason::IntegrityFailure),
                    };
                    let cursor = match encode_live_cursor(
                        &self.prepared.cursor_binding(),
                        frontier,
                        self.cursor_expires_at,
                    ) {
                        Some(cursor) => cursor,
                        None => return self.end(LiveQueryTerminalReason::IntegrityFailure),
                    };
                    let update = diff_or_reset(
                        self.last_result
                            .as_ref()
                            .expect("nonterminal live query retains its last result"),
                        next.clone(),
                        self.prepared.live_plan.update_strategy(),
                        frontier,
                        cursor,
                    );
                    self.last_result = Some(next);
                    self.last_frontier = frontier;
                    return update;
                }
                Ok(None) => continue,
                Err(reason) => return self.end(reason),
            }
        }
    }

    async fn next_sequence(&mut self) -> Result<Option<CommitSequence>, LiveQueryTerminalReason> {
        if let Some(upper) = self.pending_upper {
            let next = self
                .last_acknowledged
                .map_or(Some(CommitSequence::first()), CommitSequence::checked_next)
                .ok_or(LiveQueryTerminalReason::IntegrityFailure)?;
            if next <= upper {
                return Ok(Some(next));
            }
            self.pending_upper = None;
        }
        let scheduler = Arc::clone(&self.service.providers.deadline_scheduler);
        let source = self
            .source
            .as_mut()
            .ok_or(LiveQueryTerminalReason::IntegrityFailure)?;
        match wait_live_notification(
            self.context.control(),
            scheduler.as_ref(),
            self.lifetime_deadline,
            source.next(),
        )
        .await
        {
            Ok(LiveNotificationWait::Revalidate) => {
                if !self.current_definition_matches().await? {
                    return Err(LiveQueryTerminalReason::DefinitionChanged);
                }
                if !self.authorized_now() {
                    return Err(LiveQueryTerminalReason::AuthorizationChanged);
                }
                Ok(None)
            }
            Ok(LiveNotificationWait::Notification(notification)) => match notification {
                Ok(AuthoritativeCommitNotification::Advanced(upper)) => {
                    let expected = self
                        .last_acknowledged
                        .map_or(Some(CommitSequence::first()), CommitSequence::checked_next)
                        .ok_or(LiveQueryTerminalReason::IntegrityFailure)?;
                    if upper >= expected {
                        self.pending_upper = Some(upper);
                    }
                    Ok(None)
                }
                Ok(AuthoritativeCommitNotification::Lagged { resume_after }) => {
                    if resume_after != self.acknowledged_frontier() {
                        Err(LiveQueryTerminalReason::IntegrityFailure)
                    } else {
                        Err(LiveQueryTerminalReason::BufferPressure)
                    }
                }
                Ok(AuthoritativeCommitNotification::Gap { .. }) => {
                    Err(LiveQueryTerminalReason::IntegrityFailure)
                }
                Ok(AuthoritativeCommitNotification::Closed) => {
                    Err(LiveQueryTerminalReason::ServiceUnavailable)
                }
                Err(AuthoritativeReadError::Integrity) => {
                    Err(LiveQueryTerminalReason::IntegrityFailure)
                }
                Err(_) => Err(LiveQueryTerminalReason::ServiceUnavailable),
            },
            Err(LiveStreamWaitError::Lifetime) => Err(LiveQueryTerminalReason::LifetimeExpired),
            Err(LiveStreamWaitError::Cancelled | LiveStreamWaitError::RequestDeadline) => {
                Err(LiveQueryTerminalReason::ServiceUnavailable)
            }
        }
    }

    async fn read_commit(
        &mut self,
        sequence: CommitSequence,
    ) -> Result<AuthoritativeCommitSnapshot, LiveQueryTerminalReason> {
        if !self.current_definition_matches().await? {
            return Err(LiveQueryTerminalReason::DefinitionChanged);
        }
        if !self.authorized_now() {
            return Err(LiveQueryTerminalReason::AuthorizationChanged);
        }
        let permit = wait_live_stream(
            self.context.control(),
            self.service.providers.deadline_scheduler.as_ref(),
            self.lifetime_deadline,
            self.service
                .providers
                .authoritative
                .reserve_read_commit(self.context.control()),
        )
        .await
        .map_err(map_live_wait)?
        .map_err(map_live_admission)?;
        if !self.authorized_now() {
            return Err(LiveQueryTerminalReason::AuthorizationChanged);
        }
        let receipt = permit.submit(sequence).map_err(map_live_admission)?;
        let snapshot = wait_live_stream(
            self.context.control(),
            self.service.providers.deadline_scheduler.as_ref(),
            self.lifetime_deadline,
            receipt,
        )
        .await
        .map_err(map_live_wait)?
        .map_err(|_| LiveQueryTerminalReason::ServiceUnavailable)?
        .map_err(map_live_read)?
        .ok_or(LiveQueryTerminalReason::IntegrityFailure)?;
        if snapshot.sequence() != sequence {
            return Err(LiveQueryTerminalReason::IntegrityFailure);
        }
        if !self.current_definition_matches().await? {
            return Err(LiveQueryTerminalReason::DefinitionChanged);
        }
        if !self.authorized_now() {
            return Err(LiveQueryTerminalReason::AuthorizationChanged);
        }
        Ok(snapshot)
    }

    async fn execute_current(
        &mut self,
    ) -> Result<ExecuteSymbolicQueryResult, LiveQueryTerminalReason> {
        if !self.current_definition_matches().await? {
            return Err(LiveQueryTerminalReason::DefinitionChanged);
        }
        let authorization = self
            .authorize_application_query()
            .ok_or(LiveQueryTerminalReason::AuthorizationChanged)?;
        let row_policy = self
            .resolve_row_policy(&authorization)
            .map_err(|_| LiveQueryTerminalReason::IntegrityFailure)?;
        let executor = self
            .service
            .providers
            .query_executor
            .as_ref()
            .ok_or(LiveQueryTerminalReason::ServiceUnavailable)?;
        let snapshot = execute_authorized_query_page(
            &authorization,
            executor.as_ref(),
            &self.prepared.program,
            &[],
            &self.prepared.parameters,
            None,
            row_policy.as_ref(),
        )
        .map_err(map_live_execution)?;
        // A static top-N query is one complete watched value even when the
        // engine proves that later scan pages exist. Reactive compilation
        // rejects caller-paginated `after` queries, and every invalidation
        // re-executes this same first bounded page rather than following or
        // exposing the engine continuation.
        if !self.authorized_now() {
            return Err(LiveQueryTerminalReason::AuthorizationChanged);
        }
        Ok(ExecuteSymbolicQueryResult::from_named_snapshot(
            &self.prepared.program,
            self.prepared.query_module.identity(),
            snapshot,
            Arc::clone(self.prepared.catalog.bundle().enum_variant_names()),
        ))
    }

    async fn current_definition_matches(&mut self) -> Result<bool, LiveQueryTerminalReason> {
        let current = wait_live_stream(
            self.context.control(),
            self.service.providers.deadline_scheduler.as_ref(),
            self.lifetime_deadline,
            self.service
                .providers
                .catalog
                .prepare_active_catalog(self.context.control()),
        )
        .await
        .map_err(map_live_wait)?
        .map_err(|_| LiveQueryTerminalReason::ServiceUnavailable)?;
        Ok(current.is_some_and(|catalog| catalog.pointer() == self.prepared.catalog.pointer()))
    }

    fn authorize_application_query(&self) -> Option<riffdb_policy::AuthorizedApplicationQuery> {
        match self.service.providers.policy.authorize(
            self.context.principal(),
            self.prepared.policy_request.clone(),
        ) {
            Ok(Decision::Allow(authorization)) => authorization.into_application_query(),
            Ok(Decision::Deny(_) | Decision::PrepareCapabilityMutation(_)) | Err(_) => None,
        }
    }

    fn authorized_now(&self) -> bool {
        self.authorize_application_query()
            .is_some_and(|authorization| self.resolve_row_policy(&authorization).is_ok())
    }

    fn resolve_row_policy(
        &self,
        authorization: &AuthorizedApplicationQuery,
    ) -> Result<Option<AuthorizedQueryRowPolicyContextV1>, ()> {
        resolve_authorized_query_row_policy_context(
            authorization,
            self.prepared.catalog.bundle().bundle(),
        )
        .map_err(|_| ())
    }

    fn acknowledge(&mut self, sequence: CommitSequence) -> bool {
        let Some(source) = self.source.as_mut() else {
            return false;
        };
        if source.acknowledge(sequence).is_err() {
            return false;
        }
        self.last_acknowledged = Some(sequence);
        if self.pending_upper == Some(sequence) {
            self.pending_upper = None;
        }
        true
    }

    fn acknowledged_frontier(&self) -> FrontierPosition {
        self.last_acknowledged.map_or(
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough,
        )
    }

    fn end(&mut self, reason: LiveQueryTerminalReason) -> LiveQueryUpdate {
        let terminal = LiveQueryTerminal {
            reason,
            last_frontier: self.last_frontier,
        };
        self.terminal = Some(terminal);
        self.initial = None;
        self.last_result = None;
        self.release();
        LiveQueryUpdate::Terminal(terminal)
    }

    fn release(&mut self) {
        drop(self.source.take());
        drop(self.subscriber_lease.take());
    }
}

async fn establish_live_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: WatchLiveNamedQueryRequest,
) -> ServiceResult<WatchLiveNamedQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::WatchNamedQuery;
    let prepared = prepare_live_query(&service, &context, request.selection).await?;
    let targets = ServiceAuditTargetMap::symbolic_query(
        prepared.catalog.pointer().lineage().clone(),
        prepared.catalog.pointer().contract_version(),
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let begun = service
        .begin_invocation(
            &context,
            prepared.policy_request.clone(),
            targets,
            AuditScope::StandardRead,
        )
        .await?;
    let authorization = begun
        .reauthorize_read(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let row_policy = resolve_authorized_query_row_policy_context(
        &authorization,
        prepared.catalog.bundle().bundle(),
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let executor = service
        .providers
        .query_executor
        .as_ref()
        .ok_or_else(PublicError::storage_unavailable)?;
    let snapshot = execute_authorized_query_page(
        &authorization,
        executor.as_ref(),
        &prepared.program,
        &[],
        &prepared.parameters,
        None,
        row_policy.as_ref(),
    )
    .map_err(|error| execution_failure(&service, OPERATION, error))?;
    // The engine continuation describes rows beyond this statically bounded
    // top-N result. It is neither a partial-result signal nor public watch
    // state; caller-paginated queries are rejected by reactive compilation.
    let initial_result = ExecuteSymbolicQueryResult::from_named_snapshot(
        &prepared.program,
        prepared.query_module.identity(),
        snapshot,
        Arc::clone(prepared.catalog.bundle().enum_variant_names()),
    );
    let initial_frontier = LiveQueryFrontier::new(
        service.identity.history_incarnation(),
        initial_result.application_head(),
    )
    .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let subscriber_lease = service
        .reserve_commit_subscriber()
        .map_err(|_| PublicError::storage_unavailable())?;
    let source = establish_live_source(
        &service,
        &context,
        &begun,
        initial_result.application_head(),
    )
    .await?;
    let now = service
        .providers
        .live_query_clock
        .as_ref()
        .ok_or_else(PublicError::storage_unavailable)?
        .now()
        .map_err(|_| PublicError::storage_unavailable())?;
    let cursor_expires_at = timestamp_after(now, MAX_LIVE_QUERY_LIFETIME)
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let cursor = encode_live_cursor(
        &prepared.cursor_binding(),
        initial_frontier,
        cursor_expires_at,
    )
    .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let initial = match request.cursor {
        None => initial_snapshot(initial_result.clone(), initial_frontier, cursor),
        Some(prior) => {
            match classify_resume_cursor(
                &prior,
                &prepared.cursor_binding(),
                service.identity.history_incarnation(),
                now,
            ) {
                ResumeCursorClassification::Valid => {
                    initial_snapshot(initial_result.clone(), initial_frontier, cursor)
                }
                ResumeCursorClassification::Reset(reason) => {
                    LiveQueryUpdate::Reset(LiveQueryReset {
                        reason,
                        result: initial_result.clone(),
                        frontier: initial_frontier,
                        cursor,
                    })
                }
            }
        }
    };
    begun.reauthorize_read(&service, &context).await?;
    ensure_response_budget(&initial)?;
    begun
        .finish(
            &service,
            &context,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditLinkV1::None,
        )
        .await
        .map_err(|_| PublicError::storage_unavailable())?;
    let lifetime_deadline = Instant::now()
        .checked_add(MAX_LIVE_QUERY_LIFETIME)
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let last_acknowledged = CommitSequence::new(initial_result.application_head());
    let subscription = ServiceLiveQuerySubscription {
        service,
        context,
        prepared,
        source: Some(source),
        subscriber_lease: Some(subscriber_lease),
        lifetime_deadline,
        cursor_expires_at,
        last_result: Some(initial_result),
        last_frontier: initial_frontier,
        last_acknowledged,
        pending_upper: None,
        initial: Some(initial),
        terminal: None,
    };
    Ok(WatchLiveNamedQueryResult::new(Box::new(subscription)))
}

async fn prepare_live_query(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    selection: LiveNamedQuerySelection,
) -> ServiceResult<PreparedLiveQuery> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::WatchNamedQuery;
    let catalog = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .prepare_active_catalog(context.control()),
    )
    .await
    .map_err(controlled_failure)?
    .map_err(|_| PublicError::storage_unavailable())?
    .ok_or_else(PublicError::storage_unavailable)?;
    let reactive_modules = service
        .providers
        .reactive_modules
        .as_ref()
        .ok_or_else(PublicError::storage_unavailable)?;
    let reactive_module = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        reactive_modules.prepare_reactive_module(
            context.control(),
            catalog.bundle().clone(),
            selection.module_hash(),
        ),
    )
    .await
    .map_err(controlled_failure)?
    .map_err(|error| match error {
        crate::ReactiveModuleReadError::Unavailable => PublicError::storage_unavailable().into(),
        crate::ReactiveModuleReadError::Integrity => {
            service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
        }
    })?
    .ok_or_else(invalid_live_query_request)?;
    let operation = reactive_module
        .plan()
        .operation(selection.operation_name().as_str())
        .cloned()
        .ok_or_else(invalid_live_query_request)?;
    let ReactiveOperationPlanV1::Watch {
        parameters: declared_parameters,
        query,
        update_mode,
        patch_key,
    } = operation.plan()
    else {
        return Err(invalid_live_query_request());
    };
    if declared_parameters.len() != selection.parameters().iter().len()
        || declared_parameters
            .iter()
            .zip(selection.parameters().iter())
            .any(|(declared, (submitted, _))| declared.name() != submitted)
    {
        return Err(invalid_live_query_request());
    }
    let query_name = QueryOperationName::new(query.query_name().to_owned())
        .map_err(|_| invalid_live_query_request())?;
    let query_module = load_query_module(
        service,
        context,
        catalog.bundle().clone(),
        Some(query.module_hash()),
        OPERATION,
    )
    .await?
    .ok_or_else(invalid_live_query_request)?;
    let named_query = query_module
        .module()
        .query(query.query_name())
        .ok_or_else(invalid_live_query_request)?;
    if named_query.plan().identity() != query.plan_hash()
        || named_query.plan().cost() != query.cost()
    {
        return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
    }
    let program = named_query
        .shared_ordinary_program()
        .ok_or_else(invalid_live_query_request)?;
    let live_plan = LiveQueryPlanV1::derive(&program, *update_mode, patch_key)
        .map_err(|_| invalid_live_query_request())?;
    let dependencies = bind_live_query_dependencies(&program, selection.parameters())
        .map_err(|_| invalid_live_query_request())?;
    let parameter_hash =
        query_parameter_hash(selection.parameters()).ok_or_else(invalid_live_query_request)?;
    let target = application_query_target(
        catalog.bundle().bundle(),
        &program,
        selection.parameters(),
        context.ingress(),
    )
    .ok_or_else(invalid_live_query_request)?;
    let policy_request = OperationRequest::watch_named_query(
        catalog.pointer().lineage().clone(),
        selection.module_hash(),
        selection.operation_name().clone(),
        target,
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let row_policy_protected = program.steps().iter().any(|step| {
        catalog
            .bundle()
            .bundle()
            .row_policies()
            .policies()
            .iter()
            .any(|policy| policy.entity() == step.internal_entity_id())
    });
    Ok(PreparedLiveQuery {
        catalog,
        reactive_module_hash: selection.module_hash(),
        reactive_operation_hash: operation.identity(),
        reactive_operation_name: selection.operation_name().clone(),
        query_module,
        query_name,
        program,
        parameters: selection.parameters,
        parameter_hash,
        dependencies,
        live_plan,
        policy_request,
        row_policy_protected,
    })
}

async fn establish_live_source(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &crate::orchestration::BegunInvocation,
    after: u64,
) -> ServiceResult<Box<dyn CommitNotificationSource>> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::WatchNamedQuery;
    let permit = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_subscribe_to_commits(context.control()),
    )
    .await
    .map_err(controlled_failure)?
    .map_err(map_establishment_admission)?;
    begun.reauthorize_read(service, context).await?;
    let after = CommitSequence::new(after);
    let request = AuthoritativeCommitSubscriptionRequest::new(after, PageLimit::default())
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let receipt = permit
        .submit(request)
        .map_err(map_establishment_admission)?;
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(source))) => Ok(source),
        Ok(Ok(Err(_))) | Ok(Err(PortDriverStopped)) => {
            Err(PublicError::storage_unavailable().into())
        }
        Err(error) => Err(controlled_failure(error)),
    }
}

fn invalid_live_query_request() -> ServiceFailure {
    PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        ValidationCode::InvalidValue,
        ValidationPath::root(),
    )))
    .into()
}

fn controlled_failure(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

fn map_establishment_admission(error: PortAdmissionError) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => ServiceFailure::Cancelled,
        PortAdmissionError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            PublicError::storage_unavailable().into()
        }
    }
}

fn map_live_admission(error: PortAdmissionError) -> LiveQueryTerminalReason {
    match error {
        PortAdmissionError::Cancelled
        | PortAdmissionError::DeadlineExceeded
        | PortAdmissionError::Unavailable
        | PortAdmissionError::Stopped => LiveQueryTerminalReason::ServiceUnavailable,
    }
}

fn map_live_read(error: AuthoritativeReadError) -> LiveQueryTerminalReason {
    match error {
        AuthoritativeReadError::Integrity
        | AuthoritativeReadError::InvalidContinuation
        | AuthoritativeReadError::HistoryPruned => LiveQueryTerminalReason::IntegrityFailure,
        AuthoritativeReadError::Unavailable
        | AuthoritativeReadError::Cancelled
        | AuthoritativeReadError::DeadlineExceeded => LiveQueryTerminalReason::ServiceUnavailable,
    }
}

fn map_live_execution(error: QueryExecutionError) -> LiveQueryTerminalReason {
    match error {
        QueryExecutionError::BackendUnavailable => LiveQueryTerminalReason::ServiceUnavailable,
        QueryExecutionError::BackendIntegrity
        | QueryExecutionError::MissingParameter { .. }
        | QueryExecutionError::InvalidParameter { .. }
        | QueryExecutionError::StaleCursor
        | QueryExecutionError::InvalidContinuation
        | QueryExecutionError::BackendLimitExceeded
        | QueryExecutionError::BoundExceeded
        | QueryExecutionError::AggregateOverflow
        | QueryExecutionError::FuelExhausted
        | QueryExecutionError::MissingField { .. }
        | QueryExecutionError::InvalidDependentKey { .. }
        | QueryExecutionError::InvalidProgram
        | QueryExecutionError::UnexpectedCardinality { .. }
        | QueryExecutionError::UnsupportedPredicate => LiveQueryTerminalReason::IntegrityFailure,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiveStreamWaitError {
    Cancelled,
    RequestDeadline,
    Lifetime,
}

fn map_live_wait(error: LiveStreamWaitError) -> LiveQueryTerminalReason {
    match error {
        LiveStreamWaitError::Lifetime => LiveQueryTerminalReason::LifetimeExpired,
        LiveStreamWaitError::Cancelled | LiveStreamWaitError::RequestDeadline => {
            LiveQueryTerminalReason::ServiceUnavailable
        }
    }
}

async fn wait_live_stream<F>(
    control: &crate::RequestControl,
    deadline_scheduler: &dyn crate::RequestDeadlineScheduler,
    lifetime_deadline: Instant,
    future: F,
) -> Result<F::Output, LiveStreamWaitError>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let mut cancelled = Box::pin(control.cancelled());
    let mut request_deadline = deadline_scheduler.wait_until(control.deadline());
    let mut lifetime = deadline_scheduler.wait_until(lifetime_deadline);
    poll_fn(|context| {
        if control.is_cancelled() || Pin::as_mut(&mut cancelled).poll(context).is_ready() {
            return Poll::Ready(Err(LiveStreamWaitError::Cancelled));
        }
        if control.is_deadline_exceeded()
            || Pin::as_mut(&mut request_deadline).poll(context).is_ready()
        {
            return Poll::Ready(Err(LiveStreamWaitError::RequestDeadline));
        }
        if Pin::as_mut(&mut lifetime).poll(context).is_ready() {
            return Poll::Ready(Err(LiveStreamWaitError::Lifetime));
        }
        Pin::as_mut(&mut future).poll(context).map(Ok)
    })
    .await
}

enum LiveNotificationWait<T> {
    Notification(T),
    Revalidate,
}

async fn wait_live_notification<F>(
    control: &crate::RequestControl,
    deadline_scheduler: &dyn crate::RequestDeadlineScheduler,
    lifetime_deadline: Instant,
    future: F,
) -> Result<LiveNotificationWait<F::Output>, LiveStreamWaitError>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let mut cancelled = Box::pin(control.cancelled());
    let mut request_deadline = deadline_scheduler.wait_until(control.deadline());
    let mut lifetime = deadline_scheduler.wait_until(lifetime_deadline);
    let revalidate_at = Instant::now()
        .checked_add(LIVE_QUERY_REVALIDATION_INTERVAL)
        .ok_or(LiveStreamWaitError::Lifetime)?;
    let mut revalidate = deadline_scheduler.wait_until(revalidate_at);
    poll_fn(|context| {
        if control.is_cancelled() || Pin::as_mut(&mut cancelled).poll(context).is_ready() {
            return Poll::Ready(Err(LiveStreamWaitError::Cancelled));
        }
        if control.is_deadline_exceeded()
            || Pin::as_mut(&mut request_deadline).poll(context).is_ready()
        {
            return Poll::Ready(Err(LiveStreamWaitError::RequestDeadline));
        }
        if Pin::as_mut(&mut lifetime).poll(context).is_ready() {
            return Poll::Ready(Err(LiveStreamWaitError::Lifetime));
        }
        if Pin::as_mut(&mut revalidate).poll(context).is_ready() {
            return Poll::Ready(Ok(LiveNotificationWait::Revalidate));
        }
        Pin::as_mut(&mut future)
            .poll(context)
            .map(|value| Ok(LiveNotificationWait::Notification(value)))
    })
    .await
}

const LIVE_CURSOR_MAGIC: &[u8; 8] = b"RDBLIVE1";

#[derive(Clone, Debug, Eq, PartialEq)]
struct LiveCursorBinding {
    lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_hash: ContractBundleHash,
    reactive_module_hash: ReactiveModuleHash,
    reactive_operation_hash: ReactiveOperationHash,
    reactive_operation_name: ReactiveOperationName,
    query_module_hash: QueryModuleHash,
    query_plan_hash: QueryPlanHash,
    query_name: QueryOperationName,
    parameter_hash: QueryParameterHash,
    partitions: Vec<PartitionKeyHash>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DecodedLiveCursor {
    history_incarnation: u64,
    binding: LiveCursorBinding,
    frontier: u64,
    expires_at: Timestamp,
}

enum ResumeCursorClassification {
    Valid,
    Reset(LiveQueryResetReason),
}

fn classify_resume_cursor(
    cursor: &LiveQueryCursor,
    binding: &LiveCursorBinding,
    history_incarnation: u64,
    now: Timestamp,
) -> ResumeCursorClassification {
    let Some(decoded) = decode_live_cursor(cursor.as_bytes()) else {
        return ResumeCursorClassification::Reset(LiveQueryResetReason::DefinitionChanged);
    };
    if decoded.history_incarnation != history_incarnation {
        return ResumeCursorClassification::Reset(LiveQueryResetReason::HistoryChanged);
    }
    if now >= decoded.expires_at {
        return ResumeCursorClassification::Reset(LiveQueryResetReason::CursorExpired);
    }
    if &decoded.binding != binding {
        return ResumeCursorClassification::Reset(LiveQueryResetReason::DefinitionChanged);
    }
    ResumeCursorClassification::Valid
}

fn encode_live_cursor(
    binding: &LiveCursorBinding,
    frontier: LiveQueryFrontier,
    expires_at: Timestamp,
) -> Option<LiveQueryCursor> {
    let mut bytes = Vec::with_capacity(512);
    bytes.extend_from_slice(LIVE_CURSOR_MAGIC);
    bytes.extend_from_slice(&frontier.history_incarnation().to_be_bytes());
    push_cursor_text(&mut bytes, binding.lineage.as_str())?;
    bytes.extend_from_slice(&binding.contract_version.to_be_bytes());
    bytes.extend_from_slice(binding.contract_hash.as_bytes());
    bytes.extend_from_slice(binding.reactive_module_hash.as_bytes());
    bytes.extend_from_slice(binding.reactive_operation_hash.as_bytes());
    push_cursor_text(&mut bytes, binding.reactive_operation_name.as_str())?;
    bytes.extend_from_slice(binding.query_module_hash.as_bytes());
    bytes.extend_from_slice(binding.query_plan_hash.as_bytes());
    push_cursor_text(&mut bytes, binding.query_name.as_str())?;
    bytes.extend_from_slice(binding.parameter_hash.as_bytes());
    let partition_count = u16::try_from(binding.partitions.len()).ok()?;
    bytes.extend_from_slice(&partition_count.to_be_bytes());
    for partition in &binding.partitions {
        bytes.extend_from_slice(partition.as_bytes());
    }
    bytes.extend_from_slice(&frontier.application_head().to_be_bytes());
    bytes.extend_from_slice(&expires_at.seconds().to_be_bytes());
    bytes.extend_from_slice(&expires_at.nanoseconds().to_be_bytes());
    LiveQueryCursor::new(bytes).ok()
}

fn push_cursor_text(bytes: &mut Vec<u8>, value: &str) -> Option<()> {
    let length = u16::try_from(value.len()).ok()?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Some(())
}

fn decode_live_cursor(bytes: &[u8]) -> Option<DecodedLiveCursor> {
    let mut reader = LiveCursorReader::new(bytes);
    if reader.take(8)? != LIVE_CURSOR_MAGIC {
        return None;
    }
    let history_incarnation = reader.u64()?;
    if history_incarnation == 0 {
        return None;
    }
    let lineage = ContractLineage::new(reader.text()?).ok()?;
    let contract_version = ContractVersion::new(reader.u64()?)?;
    let contract_hash = ContractBundleHash::from_bytes(reader.array_32()?);
    let reactive_module_hash = ReactiveModuleHash::from_bytes(reader.array_32()?);
    let reactive_operation_hash = ReactiveOperationHash::from_bytes(reader.array_32()?);
    let reactive_operation_name = ReactiveOperationName::new(reader.text()?).ok()?;
    let query_module_hash = QueryModuleHash::from_bytes(reader.array_32()?);
    let query_plan_hash = QueryPlanHash::from_bytes(reader.array_32()?);
    let query_name = QueryOperationName::new(reader.text()?).ok()?;
    let parameter_hash = QueryParameterHash::from_bytes(reader.array_32()?);
    let partition_count = usize::from(reader.u16()?);
    if partition_count == 0 || partition_count > 64 {
        return None;
    }
    let mut partitions = Vec::with_capacity(partition_count);
    for _ in 0..partition_count {
        partitions.push(PartitionKeyHash::from_bytes(reader.array_32()?));
    }
    if partitions.windows(2).any(|pair| pair[0] >= pair[1]) {
        return None;
    }
    let frontier = reader.u64()?;
    let expires_at = Timestamp::new(reader.i64()?, reader.u32()?).ok()?;
    if !reader.is_empty() {
        return None;
    }
    Some(DecodedLiveCursor {
        history_incarnation,
        binding: LiveCursorBinding {
            lineage,
            contract_version,
            contract_hash,
            reactive_module_hash,
            reactive_operation_hash,
            reactive_operation_name,
            query_module_hash,
            query_plan_hash,
            query_name,
            parameter_hash,
            partitions,
        },
        frontier,
        expires_at,
    })
}

struct LiveCursorReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> LiveCursorReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        let end = self.offset.checked_add(length)?;
        let value = self.bytes.get(self.offset..end)?;
        self.offset = end;
        Some(value)
    }

    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn i64(&mut self) -> Option<i64> {
        Some(i64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn array_32(&mut self) -> Option<[u8; 32]> {
        self.take(32)?.try_into().ok()
    }

    fn text(&mut self) -> Option<String> {
        let length = usize::from(self.u16()?);
        std::str::from_utf8(self.take(length)?)
            .ok()
            .map(str::to_owned)
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn timestamp_after(now: Timestamp, duration: Duration) -> Option<Timestamp> {
    let seconds = i64::try_from(duration.as_secs()).ok()?;
    Timestamp::new(now.seconds().checked_add(seconds)?, now.nanoseconds()).ok()
}

/// Opaque self-contained live-query resume state.
///
/// The cursor is a consistency fence, not authority. Every use resolves the
/// exact immutable definitions and reauthorizes the complete query again.
#[derive(Clone, Eq, PartialEq)]
pub struct LiveQueryCursor(Vec<u8>);

impl LiveQueryCursor {
    /// Retains one bounded opaque cursor produced by the service or transport.
    pub fn new(bytes: Vec<u8>) -> Result<Self, LiveQueryInputError> {
        if bytes.is_empty() || bytes.len() > MAX_LIVE_QUERY_CURSOR_BYTES {
            return Err(LiveQueryInputError::InvalidCursor);
        }
        Ok(Self(bytes))
    }

    /// Exact opaque bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for LiveQueryCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LiveQueryCursor([REDACTED])")
    }
}

/// Safe live-query request construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveQueryInputError {
    /// Cursor bytes are absent or exceed their fixed bound.
    InvalidCursor,
}

/// Restore-fenced authoritative application frontier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveQueryFrontier {
    history_incarnation: u64,
    application_head: u64,
}

impl LiveQueryFrontier {
    /// Joins a positive history incarnation and one snapshot/commit head.
    #[must_use]
    pub const fn new(history_incarnation: u64, application_head: u64) -> Option<Self> {
        if history_incarnation == 0 {
            return None;
        }
        Some(Self {
            history_incarnation,
            application_head,
        })
    }

    /// Durable restore fence.
    #[must_use]
    pub const fn history_incarnation(self) -> u64 {
        self.history_incarnation
    }

    /// Last authoritative application sequence incorporated into this view.
    #[must_use]
    pub const fn application_head(self) -> u64 {
        self.application_head
    }
}

/// One complete-key public row identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveQueryPublicKey {
    fields: Vec<(String, CanonicalValue)>,
}

impl LiveQueryPublicKey {
    fn from_record(record: &SymbolicResultRecord, key_fields: &[String]) -> Option<Self> {
        let fields = key_fields
            .iter()
            .map(|name| {
                record
                    .fields()
                    .get(name.as_str())
                    .cloned()
                    .map(|value| (name.clone(), value))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { fields })
    }

    /// Complete key fields in compiler-proved canonical name order.
    #[must_use]
    pub fn fields(&self) -> &[(String, CanonicalValue)] {
        &self.fields
    }
}

/// One deterministic operation over a keyed top-level collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveQueryPatchOperation {
    /// Inserts one complete authorized record at the selected index.
    Insert {
        /// Zero-based target index.
        index: u16,
        /// Complete selected record.
        record: SymbolicResultRecord,
    },
    /// Removes one exact public key from the selected index.
    Remove {
        /// Zero-based prior index.
        index: u16,
        /// Complete authorized primary key.
        key: LiveQueryPublicKey,
    },
    /// Replaces one same-key record at the selected index.
    Replace {
        /// Zero-based stable index.
        index: u16,
        /// Complete selected replacement record.
        record: SymbolicResultRecord,
    },
    /// Moves one exact public key inside the collection.
    Move {
        /// Zero-based prior index.
        from: u16,
        /// Zero-based target index.
        to: u16,
        /// Complete authorized primary key.
        key: LiveQueryPublicKey,
    },
}

/// Initial complete result for one live watch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveQuerySnapshot {
    result: ExecuteSymbolicQueryResult,
    frontier: LiveQueryFrontier,
    cursor: LiveQueryCursor,
}

impl LiveQuerySnapshot {
    /// Complete authorized named-query result.
    #[must_use]
    pub const fn result(&self) -> &ExecuteSymbolicQueryResult {
        &self.result
    }
    /// Snapshot frontier.
    #[must_use]
    pub const fn frontier(&self) -> LiveQueryFrontier {
        self.frontier
    }
    /// Resume cursor at this exact view.
    #[must_use]
    pub const fn cursor(&self) -> &LiveQueryCursor {
        &self.cursor
    }
}

/// Bounded complete-key changes from one authorized view to the next.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveQueryPatch {
    result_field: String,
    operations: Vec<LiveQueryPatchOperation>,
    frontier: LiveQueryFrontier,
    cursor: LiveQueryCursor,
    enum_variant_names: SharedEnumVariantNames,
}

impl LiveQueryPatch {
    /// Top-level collection field changed by every operation.
    #[must_use]
    pub fn result_field(&self) -> &str {
        &self.result_field
    }
    /// Ordered operations that transform the prior view exactly.
    #[must_use]
    pub fn operations(&self) -> &[LiveQueryPatchOperation] {
        &self.operations
    }
    /// Result frontier after the complete patch.
    #[must_use]
    pub const fn frontier(&self) -> LiveQueryFrontier {
        self.frontier
    }
    /// Resume cursor after the complete patch.
    #[must_use]
    pub const fn cursor(&self) -> &LiveQueryCursor {
        &self.cursor
    }
    /// Resolves one canonical enum identity for public symbolic conversion.
    #[must_use]
    pub fn enum_variant_name(&self, type_id: u32, variant_id: u32) -> Option<&str> {
        self.enum_variant_names
            .get(&(type_id, variant_id))
            .map(String::as_str)
    }
}

/// Complete replacement view with a typed reason.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveQueryReset {
    reason: LiveQueryResetReason,
    result: ExecuteSymbolicQueryResult,
    frontier: LiveQueryFrontier,
    cursor: LiveQueryCursor,
}

impl LiveQueryReset {
    /// Reset classification.
    #[must_use]
    pub const fn reason(&self) -> LiveQueryResetReason {
        self.reason
    }
    /// Complete newly authorized result.
    #[must_use]
    pub const fn result(&self) -> &ExecuteSymbolicQueryResult {
        &self.result
    }
    /// Replacement frontier.
    #[must_use]
    pub const fn frontier(&self) -> LiveQueryFrontier {
        self.frontier
    }
    /// Replacement resume cursor.
    #[must_use]
    pub const fn cursor(&self) -> &LiveQueryCursor {
        &self.cursor
    }
}

/// Frontier-only convergence marker when relevant commits did not change output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveQueryCheckpoint {
    frontier: LiveQueryFrontier,
    cursor: LiveQueryCursor,
}

impl LiveQueryCheckpoint {
    /// Checked-through frontier.
    #[must_use]
    pub const fn frontier(&self) -> LiveQueryFrontier {
        self.frontier
    }
    /// Resume cursor at the checked-through frontier.
    #[must_use]
    pub const fn cursor(&self) -> &LiveQueryCursor {
        &self.cursor
    }
}

/// Terminal close that requires retained protected client state to be cleared.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveQueryTerminal {
    reason: LiveQueryTerminalReason,
    last_frontier: LiveQueryFrontier,
}

impl LiveQueryTerminal {
    /// Terminal classification.
    #[must_use]
    pub const fn reason(self) -> LiveQueryTerminalReason {
        self.reason
    }
    /// Last frontier known to have been released completely.
    #[must_use]
    pub const fn last_frontier(self) -> LiveQueryFrontier {
        self.last_frontier
    }
}

/// Closed public live-query update vocabulary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveQueryUpdate {
    /// Initial complete result.
    Snapshot(LiveQuerySnapshot),
    /// Complete-key incremental result change.
    Patch(LiveQueryPatch),
    /// Complete replacement result.
    Reset(LiveQueryReset),
    /// Checked-through frontier with no visible value change.
    Checkpoint(LiveQueryCheckpoint),
    /// Final clear-state instruction.
    Terminal(LiveQueryTerminal),
}

pub(crate) fn initial_snapshot(
    result: ExecuteSymbolicQueryResult,
    frontier: LiveQueryFrontier,
    cursor: LiveQueryCursor,
) -> LiveQueryUpdate {
    LiveQueryUpdate::Snapshot(LiveQuerySnapshot {
        result,
        frontier,
        cursor,
    })
}

pub(crate) fn diff_or_reset(
    previous: &ExecuteSymbolicQueryResult,
    next: ExecuteSymbolicQueryResult,
    strategy: &LiveUpdateStrategyV1,
    frontier: LiveQueryFrontier,
    cursor: LiveQueryCursor,
) -> LiveQueryUpdate {
    let reset = |reason, result, cursor| {
        LiveQueryUpdate::Reset(LiveQueryReset {
            reason,
            result,
            frontier,
            cursor,
        })
    };
    if previous.outcome() != next.outcome() {
        return reset(LiveQueryResetReason::OutcomeChanged, next, cursor);
    }
    let LiveUpdateStrategyV1::Patch {
        result_field,
        key_fields,
    } = strategy
    else {
        return reset(LiveQueryResetReason::DiffLimitExceeded, next, cursor);
    };
    if non_patch_fields_changed(previous.fields(), next.fields(), result_field) {
        return reset(LiveQueryResetReason::DiffLimitExceeded, next, cursor);
    }
    let (
        Some(SymbolicResultField::Many(previous_rows)),
        Some(SymbolicResultField::Many(next_rows)),
    ) = (
        previous.fields().get(result_field),
        next.fields().get(result_field),
    )
    else {
        return reset(LiveQueryResetReason::DiffLimitExceeded, next, cursor);
    };
    let Some(mut current) = keyed_rows(previous_rows, key_fields) else {
        return reset(LiveQueryResetReason::DiffLimitExceeded, next, cursor);
    };
    let Some(desired) = keyed_rows(next_rows, key_fields) else {
        return reset(LiveQueryResetReason::DiffLimitExceeded, next, cursor);
    };
    let mut operations = Vec::new();
    for (target, (desired_key, desired_row)) in desired.iter().enumerate() {
        if current
            .get(target)
            .is_some_and(|(key, _)| key == desired_key)
        {
            if current[target].1 != *desired_row {
                operations.push(LiveQueryPatchOperation::Replace {
                    index: target as u16,
                    record: desired_row.clone(),
                });
                current[target].1 = desired_row.clone();
            }
            continue;
        }
        if let Some(from) = current
            .iter()
            .enumerate()
            .skip(target + 1)
            .find_map(|(index, (key, _))| (key == desired_key).then_some(index))
        {
            let moved = current.remove(from);
            current.insert(target, moved);
            operations.push(LiveQueryPatchOperation::Move {
                from: from as u16,
                to: target as u16,
                key: desired_key.clone(),
            });
            if current[target].1 != *desired_row {
                operations.push(LiveQueryPatchOperation::Replace {
                    index: target as u16,
                    record: desired_row.clone(),
                });
                current[target].1 = desired_row.clone();
            }
        } else {
            current.insert(target, (desired_key.clone(), desired_row.clone()));
            operations.push(LiveQueryPatchOperation::Insert {
                index: target as u16,
                record: desired_row.clone(),
            });
        }
    }
    while current.len() > desired.len() {
        let index = current.len() - 1;
        let (key, _) = current.remove(index);
        operations.push(LiveQueryPatchOperation::Remove {
            index: index as u16,
            key,
        });
    }
    if operations.len() > MAX_LIVE_QUERY_PATCH_OPERATIONS
        || estimated_patch_bytes(&operations) > MAX_LIVE_QUERY_PATCH_BYTES
    {
        return reset(LiveQueryResetReason::DiffLimitExceeded, next, cursor);
    }
    if operations.is_empty() {
        return LiveQueryUpdate::Checkpoint(LiveQueryCheckpoint { frontier, cursor });
    }
    let patch = LiveQueryUpdate::Patch(LiveQueryPatch {
        result_field: result_field.clone(),
        operations,
        frontier,
        cursor: cursor.clone(),
        enum_variant_names: next.shared_enum_variant_names(),
    });
    if ensure_response_budget(&patch).is_err() {
        return reset(LiveQueryResetReason::DiffLimitExceeded, next, cursor);
    }
    patch
}

fn non_patch_fields_changed(
    previous: &BTreeMap<String, SymbolicResultField>,
    next: &BTreeMap<String, SymbolicResultField>,
    patch_field: &str,
) -> bool {
    previous.len() != next.len()
        || previous.iter().any(|(name, value)| {
            name != patch_field && next.get(name).is_none_or(|candidate| candidate != value)
        })
}

fn keyed_rows(
    rows: &[SymbolicResultRecord],
    key_fields: &[String],
) -> Option<Vec<(LiveQueryPublicKey, SymbolicResultRecord)>> {
    let keyed = rows
        .iter()
        .map(|record| {
            LiveQueryPublicKey::from_record(record, key_fields).map(|key| (key, record.clone()))
        })
        .collect::<Option<Vec<_>>>()?;
    for index in 0..keyed.len() {
        if keyed[..index].iter().any(|(key, _)| key == &keyed[index].0) {
            return None;
        }
    }
    Some(keyed)
}

fn estimated_patch_bytes(operations: &[LiveQueryPatchOperation]) -> usize {
    operations.iter().fold(0usize, |total, operation| {
        let values = match operation {
            LiveQueryPatchOperation::Insert { record, .. }
            | LiveQueryPatchOperation::Replace { record, .. } => record
                .fields()
                .values()
                .map(|value| canonical_value_encoded_len(value).unwrap_or(usize::MAX))
                .fold(0usize, usize::saturating_add),
            LiveQueryPatchOperation::Remove { key, .. }
            | LiveQueryPatchOperation::Move { key, .. } => key
                .fields()
                .iter()
                .map(|(_, value)| canonical_value_encoded_len(value).unwrap_or(usize::MAX))
                .fold(0usize, usize::saturating_add),
        };
        total.saturating_add(64).saturating_add(values)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use riffdb_types::{CanonicalValue, QueryPlanHash};

    use super::*;
    use crate::SymbolicQueryIdentity;

    fn cursor_binding() -> LiveCursorBinding {
        LiveCursorBinding {
            lineage: ContractLineage::new("LiveTest").expect("lineage"),
            contract_version: ContractVersion::new(3).expect("version"),
            contract_hash: ContractBundleHash::from_bytes([1; 32]),
            reactive_module_hash: ReactiveModuleHash::from_bytes([2; 32]),
            reactive_operation_hash: ReactiveOperationHash::from_bytes([3; 32]),
            reactive_operation_name: ReactiveOperationName::new("WatchItems").expect("name"),
            query_module_hash: QueryModuleHash::from_bytes([4; 32]),
            query_plan_hash: QueryPlanHash::from_bytes([5; 32]),
            query_name: QueryOperationName::new("Items").expect("name"),
            parameter_hash: QueryParameterHash::from_bytes([6; 32]),
            partitions: vec![PartitionKeyHash::from_bytes([7; 32])],
        }
    }

    #[test]
    fn cursor_round_trip_binds_identity_history_frontier_and_expiry() {
        let binding = cursor_binding();
        let frontier = LiveQueryFrontier::new(9, 17).expect("frontier");
        let expires = Timestamp::new(100, 5).expect("time");
        let cursor = encode_live_cursor(&binding, frontier, expires).expect("cursor");
        let decoded = decode_live_cursor(cursor.as_bytes()).expect("decoded");
        assert_eq!(decoded.history_incarnation, 9);
        assert_eq!(decoded.frontier, 17);
        assert_eq!(decoded.expires_at, expires);
        assert_eq!(decoded.binding, binding);
        assert!(matches!(
            classify_resume_cursor(&cursor, &binding, 9, Timestamp::new(99, 0).expect("time")),
            ResumeCursorClassification::Valid
        ));
        assert!(matches!(
            classify_resume_cursor(&cursor, &binding, 10, Timestamp::new(99, 0).expect("time")),
            ResumeCursorClassification::Reset(LiveQueryResetReason::HistoryChanged)
        ));
        assert!(matches!(
            classify_resume_cursor(&cursor, &binding, 9, expires),
            ResumeCursorClassification::Reset(LiveQueryResetReason::CursorExpired)
        ));
        let mut changed = binding.clone();
        changed.query_plan_hash = QueryPlanHash::from_bytes([8; 32]);
        assert!(matches!(
            classify_resume_cursor(&cursor, &changed, 9, Timestamp::new(99, 0).expect("time")),
            ResumeCursorClassification::Reset(LiveQueryResetReason::DefinitionChanged)
        ));
    }

    fn row(id: u64, revision: u64) -> SymbolicResultRecord {
        SymbolicResultRecord::from_shared_for_test(
            Arc::from("Item"),
            BTreeMap::from([
                (Arc::from("id"), CanonicalValue::U64(id)),
                (Arc::from("revision"), CanonicalValue::U64(revision)),
            ]),
        )
    }

    fn result_with_outcome(
        outcome: &str,
        rows: Vec<SymbolicResultRecord>,
    ) -> ExecuteSymbolicQueryResult {
        ExecuteSymbolicQueryResult::from_parts_for_test(
            SymbolicQueryIdentity::from_parts_for_test(
                ContractLineage::new("LiveTest").expect("lineage"),
                ContractVersion::new(1).expect("version"),
                ContractBundleHash::from_bytes([1; 32]),
                Some("Items".to_owned()),
                QueryPlanHash::from_bytes([2; 32]),
            ),
            outcome.to_owned(),
            1,
            BTreeMap::from([("items".to_owned(), SymbolicResultField::Many(rows))]),
            Arc::new(BTreeMap::new()),
        )
    }

    fn result(rows: Vec<SymbolicResultRecord>) -> ExecuteSymbolicQueryResult {
        result_with_outcome("Ready", rows)
    }

    #[test]
    fn keyed_diff_is_deterministic_and_never_omits_complete_keys() {
        let previous = result(vec![row(1, 1), row(2, 1)]);
        let next = result(vec![row(2, 2), row(3, 1)]);
        let update = diff_or_reset(
            &previous,
            next,
            &LiveUpdateStrategyV1::Patch {
                result_field: "items".to_owned(),
                key_fields: vec!["id".to_owned()],
            },
            LiveQueryFrontier::new(1, 2).expect("frontier"),
            LiveQueryCursor::new(vec![1]).expect("cursor"),
        );
        let LiveQueryUpdate::Patch(patch) = update else {
            panic!("expected patch");
        };
        assert!(matches!(
            patch.operations(),
            [
                LiveQueryPatchOperation::Move { from: 1, to: 0, key },
                LiveQueryPatchOperation::Replace { index: 0, .. },
                LiveQueryPatchOperation::Insert { index: 1, .. },
                LiveQueryPatchOperation::Remove { index: 2, key: removed },
            ] if key.fields() == [("id".to_owned(), CanonicalValue::U64(2))]
                && removed.fields() == [("id".to_owned(), CanonicalValue::U64(1))]
        ));
    }

    #[test]
    fn outcome_change_forces_complete_reset() {
        let previous = result(vec![row(1, 1)]);
        let next = result_with_outcome("Unavailable", vec![row(1, 2)]);
        let update = diff_or_reset(
            &previous,
            next,
            &LiveUpdateStrategyV1::Reset,
            LiveQueryFrontier::new(1, 2).expect("frontier"),
            LiveQueryCursor::new(vec![1]).expect("cursor"),
        );
        assert!(matches!(
            update,
            LiveQueryUpdate::Reset(LiveQueryReset {
                reason: LiveQueryResetReason::OutcomeChanged,
                ..
            })
        ));
    }

    #[test]
    fn excessive_keyed_diff_forces_complete_reset() {
        let previous = result((0..500).map(|id| row(id, 1)).collect());
        let next = result((0..500).rev().map(|id| row(id, 2)).collect());
        let update = diff_or_reset(
            &previous,
            next,
            &LiveUpdateStrategyV1::Patch {
                result_field: "items".to_owned(),
                key_fields: vec!["id".to_owned()],
            },
            LiveQueryFrontier::new(1, 2).expect("frontier"),
            LiveQueryCursor::new(vec![1]).expect("cursor"),
        );
        assert!(matches!(
            update,
            LiveQueryUpdate::Reset(LiveQueryReset {
                reason: LiveQueryResetReason::DiffLimitExceeded,
                ..
            })
        ));
    }

    #[test]
    fn protected_watch_reexecutes_for_any_same_partition_commit() {
        let watched = PartitionKeyHash::from_bytes([0x31; 32]);
        let unrelated = PartitionKeyHash::from_bytes([0x32; 32]);
        assert!(row_policy_partition_might_be_affected(
            true,
            [watched],
            watched,
        ));
        assert!(!row_policy_partition_might_be_affected(
            true,
            [watched],
            unrelated,
        ));
        assert!(!row_policy_partition_might_be_affected(
            false,
            [watched],
            watched,
        ));
    }
}
