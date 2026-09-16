#![expect(
    clippy::expect_used,
    reason = "a completed one-shot service port retains exactly one result until its sole receiver consumes it"
)]

//! Least-authority consumer ports used by service orchestration.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use riffdb_auth::{
    AuthenticatedPrincipal, CurrentCapabilityResolver, NewlyIssuedCapabilityToken,
    RetainedOpaqueCredential,
};
use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogError, CatalogPreparationResult, ResolvedExecutablePlan,
    ValidatedContractBundle, ValidatedQueryModule, ValidatedReactiveModule,
};
use riffdb_contract_ir::ContractBundle;
use riffdb_policy::{
    ApplicationExportAuthorizationRequestV1, ApplicationExportDecisionV1,
    ApplicationReimportAuthorizationRequestV1, ApplicationReimportDecisionV1, AuthorizationClock,
    AuthorizationError, AuthorizationTelemetry, AuthorizedContractMigration,
    AuthorizedOfflineMaintenance, AuthorizedQueryRowPolicyContextV1, CapabilityViewCheckpoint,
    ContractMigrationAuthorizationRequest, ContractMigrationDecision, CurrentAuthorizer, Decision,
    OfflineMaintenanceAuthorizationRequest, OfflineMaintenanceDecision, OperationRequest,
    ProvenanceSelector,
};
use riffdb_query_ir::{ExactParameterValueV1, ExactPredicateFamilyMemberV1, QueryAccessStep};
use riffdb_types::{
    ApplicationRoleHash, CanonicalRecord, CanonicalValue, CapabilityId, CommandId, CommitSequence,
    CompiledLongPatternV1, ContractBundleHash, ContractLineage, ContractMigrationOperationId,
    ContractVersion, DistanceMetric, EmbeddingMetadata, EntityKey, EntityTypeId, FieldId,
    FrontierPosition, OfflineMaintenanceInputHash, OfflineMaintenanceOperationId, PartitionKey,
    PlanHash, ProjectionFrontier, ProjectionGeneration, ProjectionProviderDescriptorHash,
    QueryModuleHash, QueryOperationName, ReactiveModuleHash, RequestId,
};

use crate::{
    ApplyContractMigrationRequest, AuthoritativeCommitPage, AuthoritativeCommitScanRequest,
    AuthoritativeCommitSnapshot, AuthoritativeCommitSubscriptionRequest,
    AuthoritativeEntityRequest, AuthoritativeEntitySnapshot, AuthoritativeEventReplayRequest,
    AuthoritativeIndexPage, AuthoritativeIndexRequest, AuthoritativeOutcomeRequest,
    AuthoritativeOutcomeSnapshot, AuthoritativeProvenanceSnapshot,
    AuthoritativeReactiveEventWindow, AuthoritativeReactiveEventWindowRequest,
    CapabilityRevokeTargetSnapshot, CheckContractMigrationRequest,
    ContractMigrationOperationObservation, ContractMigrationStartResult,
    CreateOfflineBackupRequest, EventConsumerPortError, EventConsumerPortRequest,
    EventConsumerPortResponse, GetContractMigrationOperationRequest,
    GetOfflineMaintenanceOperationRequest, OfflineMaintenanceOperationObservation,
    OfflineMaintenanceStartResult, OperationalHealthSnapshot, OperationalStatisticsSnapshot,
    OutboxStatusRequest, OutboxStatusSnapshot, PageLimit, ProjectionPortRequest,
    ProjectionPortResult, ProjectionStatusSnapshot, RequestControl, RestoreOfflineBackupRequest,
    RetireOfflineBackupRequest,
};

/// One boxed, sendable future returned by a service consumer port.
pub type PortFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// One complete independently driven service invocation.
pub type ServiceJob = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Injected process runtime boundary for independently owned service jobs.
pub trait ServiceJobSpawner: Send + Sync {
    /// Synchronously accepts one complete invocation for independent driving.
    ///
    /// Admission is infallible because the trusted adapter reserves bounded
    /// request capacity before authentication and `RequestContext` construction.
    /// Once accepted, the job must be driven until it publishes completion.
    /// A composition that cannot honor either contract must stop routing calls;
    /// the service cannot know whether an abandoned job performed lower work.
    /// WP-130 server composition must supervise these fail-fast breaches and
    /// stop routing through the affected service instance.
    fn spawn(&self, job: ServiceJob);
}

/// One cancellation-safe wait for an absolute process-local request deadline.
pub type RequestDeadlineFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Injected scheduler capability used while awaiting protected admission.
///
/// This port supplies only wakeup behavior. It does not supply command-visible
/// time, authorization time, durable timestamps, or ordering. A returned
/// future must not complete before `deadline`; dropping it cancels only that
/// process-local wait.
pub trait RequestDeadlineScheduler: Send + Sync {
    /// Waits until the supplied absolute monotonic deadline has elapsed.
    fn wait_until(&self, deadline: Instant) -> RequestDeadlineFuture<'_>;
}

/// Closed failure to issue one normal-create capability credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityTokenIssueError {
    /// The isolated entropy or digest-key provider is unavailable.
    Unavailable,
    /// Auth-owned token preparation violated a checked internal invariant.
    Integrity,
}

/// Auth-owned normal-create token issuance consumed without key or entropy access.
pub trait CapabilityTokenIssuer: Send + Sync {
    /// Generates exactly one token and its current-key digest as a move-only value.
    fn issue(&self) -> Result<NewlyIssuedCapabilityToken, CapabilityTokenIssueError>;
}

struct PortCompletionState<T, E> {
    result: Option<Result<T, E>>,
    sender_alive: bool,
    waiter: Option<Waker>,
}

/// Sender retained only by an independently owned consumer-port driver.
#[must_use = "the accepted port operation must publish or abandon its completion"]
pub struct PortCompletionSender<T, E> {
    state: Option<Arc<Mutex<PortCompletionState<T, E>>>>,
}

impl<T, E> PortCompletionSender<T, E> {
    /// Publishes the one terminal result and wakes the service-owned job.
    pub fn complete(mut self, result: Result<T, E>) {
        let state = self
            .state
            .take()
            .expect("move-only port completion sender is consumed once");
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.result = Some(result);
        state.sender_alive = false;
        let waiter = state.waiter.take();
        drop(state);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}

impl<T, E> Drop for PortCompletionSender<T, E> {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.sender_alive = false;
        let waiter = state.waiter.take();
        drop(state);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}

impl<T, E> std::fmt::Debug for PortCompletionSender<T, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PortCompletionSender([REDACTED])")
    }
}

/// Receiver-only completion for one synchronously admitted port request.
pub struct PortReceipt<T, E> {
    state: Arc<Mutex<PortCompletionState<T, E>>>,
}

impl<T, E> PortReceipt<T, E> {
    /// Waits for the already admitted bounded operation to finish.
    pub async fn completion(self) -> Result<Result<T, E>, PortDriverStopped> {
        self.await
    }
}

impl<T, E> Future for PortReceipt<T, E> {
    type Output = Result<Result<T, E>, PortDriverStopped>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(result) = state.result.take() {
            return Poll::Ready(Ok(result));
        }
        if !state.sender_alive {
            return Poll::Ready(Err(PortDriverStopped));
        }
        if state
            .waiter
            .as_ref()
            .is_none_or(|waiter| !waiter.will_wake(context.waker()))
        {
            state.waiter = Some(context.waker().clone());
        }
        Poll::Pending
    }
}

impl<T, E> std::fmt::Debug for PortReceipt<T, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PortReceipt([REDACTED])")
    }
}

/// Creates the one receiver-only completion pair used after synchronous admission.
pub fn port_completion_channel<T, E>() -> (PortCompletionSender<T, E>, PortReceipt<T, E>) {
    let state = Arc::new(Mutex::new(PortCompletionState {
        result: None,
        sender_alive: true,
        waiter: None,
    }));
    (
        PortCompletionSender {
            state: Some(Arc::clone(&state)),
        },
        PortReceipt { state },
    )
}

/// The accepted port driver's completion sender disappeared without a result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortDriverStopped;

/// Closed failure before a protected port request is accepted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortAdmissionError {
    /// Cancellation was observed while waiting for bounded capacity.
    Cancelled,
    /// The absolute request deadline elapsed before admission.
    DeadlineExceeded,
    /// Bounded capacity or the owning subsystem is temporarily unavailable.
    Unavailable,
    /// The owning subsystem has stopped accepting work.
    Stopped,
}

/// Move-only capacity for one exact typed consumer-port request.
pub trait PortCapacityPermit<Request, Response, Failure>: Send {
    /// Synchronously accepts work after the service's final policy check.
    fn submit(
        self: Box<Self>,
        request: Request,
    ) -> Result<PortReceipt<Response, Failure>, PortAdmissionError>;
}

/// Object-safe move-only capacity for one exact request/response pair.
pub type BoxPortCapacityPermit<Request, Response, Failure> =
    Box<dyn PortCapacityPermit<Request, Response, Failure>>;

/// The one non-caching current-policy entry point consumed by service code.
pub trait CurrentPolicyPort: Send + Sync {
    /// Reloads current administrative authority for one replication release.
    fn authorize_replication(
        &self,
        _principal: &AuthenticatedPrincipal,
    ) -> Result<riffdb_policy::ReplicationDecision, AuthorizationError> {
        Err(AuthorizationError::CurrentCapabilityUnavailable)
    }

    /// Reloads current capability state, samples fresh authorization time, and decides.
    fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OperationRequest,
    ) -> Result<Decision, AuthorizationError>;

    /// Reloads current capability state and decides one maintenance safe point.
    fn authorize_offline_maintenance(
        &self,
        _principal: &AuthenticatedPrincipal,
        _request: OfflineMaintenanceAuthorizationRequest,
    ) -> Result<OfflineMaintenanceDecision, AuthorizationError> {
        Err(AuthorizationError::CurrentCapabilityUnavailable)
    }

    /// Reloads current capability state and decides one migration safe point.
    fn authorize_contract_migration(
        &self,
        _principal: &AuthenticatedPrincipal,
        _request: ContractMigrationAuthorizationRequest,
    ) -> Result<ContractMigrationDecision, AuthorizationError> {
        Err(AuthorizationError::CurrentCapabilityUnavailable)
    }

    /// Reloads current V5 authority and decides one export release safe point.
    fn authorize_application_export(
        &self,
        _principal: &AuthenticatedPrincipal,
        _request: ApplicationExportAuthorizationRequestV1,
    ) -> Result<ApplicationExportDecisionV1, AuthorizationError> {
        Err(AuthorizationError::CurrentCapabilityUnavailable)
    }

    /// Reloads current V7 authority and decides one reimport campaign safe point.
    fn authorize_application_reimport(
        &self,
        _principal: &AuthenticatedPrincipal,
        _request: ApplicationReimportAuthorizationRequestV1,
    ) -> Result<ApplicationReimportDecisionV1, AuthorizationError> {
        Err(AuthorizationError::CurrentCapabilityUnavailable)
    }

    /// Returns the live capability-view generation without sampling the clock.
    ///
    /// Captured immediately *before* a full evaluation so a publication racing
    /// that evaluation cannot stamp a post-publication generation onto a proof.
    /// `None` disables revision-checked reauthorization for the invocation.
    fn capability_view_generation(&self) -> Option<u64> {
        None
    }

    /// Observes the live capability view and fresh authorization time together.
    ///
    /// This is the read-path reauthorization safe point's view of current
    /// state. Implementations MUST sample the same authorization clock
    /// [`CurrentPolicyPort::authorize`] uses, so the time clause checked
    /// against a retained validity window is the clause a full evaluation
    /// would apply. `None` forces full re-evaluation.
    fn capability_view_checkpoint(&self) -> Option<CapabilityViewCheckpoint> {
        None
    }
}

impl<R, C, T> CurrentPolicyPort for CurrentAuthorizer<'_, R, C, T>
where
    R: CurrentCapabilityResolver + Send + Sync + ?Sized,
    C: AuthorizationClock + Send + Sync + ?Sized,
    T: AuthorizationTelemetry + Send + Sync + ?Sized,
{
    fn authorize_replication(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<riffdb_policy::ReplicationDecision, AuthorizationError> {
        CurrentAuthorizer::authorize_replication(self, principal)
    }

    fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OperationRequest,
    ) -> Result<Decision, AuthorizationError> {
        CurrentAuthorizer::authorize(self, principal, request)
    }

    fn authorize_application_export(
        &self,
        principal: &AuthenticatedPrincipal,
        request: ApplicationExportAuthorizationRequestV1,
    ) -> Result<ApplicationExportDecisionV1, AuthorizationError> {
        CurrentAuthorizer::authorize_application_export(self, principal, request)
    }

    fn authorize_application_reimport(
        &self,
        principal: &AuthenticatedPrincipal,
        request: ApplicationReimportAuthorizationRequestV1,
    ) -> Result<ApplicationReimportDecisionV1, AuthorizationError> {
        CurrentAuthorizer::authorize_application_reimport(self, principal, request)
    }

    fn authorize_offline_maintenance(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OfflineMaintenanceAuthorizationRequest,
    ) -> Result<OfflineMaintenanceDecision, AuthorizationError> {
        CurrentAuthorizer::authorize_offline_maintenance(self, principal, request)
    }

    fn authorize_contract_migration(
        &self,
        principal: &AuthenticatedPrincipal,
        request: ContractMigrationAuthorizationRequest,
    ) -> Result<ContractMigrationDecision, AuthorizationError> {
        CurrentAuthorizer::authorize_contract_migration(self, principal, request)
    }
}

/// Complete checked identity required to resolve one executable plan.
#[derive(Clone, Eq, PartialEq)]
pub struct CatalogExecutablePlanRequest {
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
    command_id: CommandId,
    plan_hash: PlanHash,
}

impl CatalogExecutablePlanRequest {
    /// Joins the immutable plan identity without naming a storage-owned reference.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        version: ContractVersion,
        bundle_hash: ContractBundleHash,
        command_id: CommandId,
        plan_hash: PlanHash,
    ) -> Self {
        Self {
            lineage,
            version,
            bundle_hash,
            command_id,
            plan_hash,
        }
    }

    /// Borrows the contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the contract version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Returns the immutable bundle hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }

    /// Returns the stable command identity.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }

    /// Returns the exact plan hash.
    #[must_use]
    pub const fn plan_hash(&self) -> PlanHash {
        self.plan_hash
    }
}

impl std::fmt::Debug for CatalogExecutablePlanRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CatalogExecutablePlanRequest([REDACTED])")
    }
}

/// One bounded permit for an exact historical contract-version observation.
pub type ContractVersionReadPermit = BoxPortCapacityPermit<
    (ContractLineage, ContractVersion),
    Option<ValidatedContractBundle>,
    CatalogError,
>;

/// Checked catalog semantics needed by service operations.
pub trait CatalogReadPort: Send + Sync {
    /// Resolves the active checked catalog only for exact request preparation.
    ///
    /// The returned value is never caller output. It is used to resolve stable
    /// semantic IDs before the first exact authorization request is possible.
    fn prepare_active_catalog(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, Option<ActiveCatalogSnapshot>, CatalogError>;

    /// Resolves one checked historical bundle for exact request preparation or
    /// protected-output schema validation.
    ///
    /// The returned value is never caller output. It is used to derive stable
    /// schema and policy facts before a protected read is admitted, or to
    /// validate authoritative entity-key bytes before a result is released.
    fn prepare_contract_version<'a>(
        &'a self,
        control: &'a RequestControl,
        lineage: ContractLineage,
        version: ContractVersion,
    ) -> PortFuture<'a, Option<ValidatedContractBundle>, CatalogError>;

    /// Reserves cancellation-aware capacity for an active-catalog observation.
    fn reserve_active_catalog<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<(), Option<ActiveCatalogSnapshot>, CatalogError>,
        PortAdmissionError,
    >;

    /// Reserves cancellation-aware capacity for one historical-bundle observation.
    fn reserve_contract_version<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<'a, ContractVersionReadPermit, PortAdmissionError>;

    /// Resolves preparatory plan facts with bounded cancellation and deadline handling.
    ///
    /// This read returns no caller output and may run while a command-executor
    /// permit is held. The service performs a fresh policy check only after it
    /// obtains the resulting exact facts.
    fn executable_plan<'a>(
        &'a self,
        control: &'a RequestControl,
        request: CatalogExecutablePlanRequest,
    ) -> PortFuture<'a, ResolvedExecutablePlan, CatalogError>;

    /// Performs bounded preparatory catalog validation before coordinator admission.
    fn prepare_deployment<'a>(
        &'a self,
        control: &'a RequestControl,
        candidate: ContractBundle,
        expected_active_version: Option<ContractVersion>,
    ) -> PortFuture<'a, CatalogPreparationResult, CatalogError>;
}

/// Closed failure while reading and recompiling an immutable query module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryModuleReadError {
    /// Storage or bounded execution capacity was unavailable.
    Unavailable,
    /// Durable module bytes, identity, or exact-contract binding failed validation.
    Integrity,
}

/// Complete immutable identity for one generated exact named-query lookup.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExactNamedQueryRequest {
    lineage: ContractLineage,
    version: ContractVersion,
    contract_hash: ContractBundleHash,
    module_hash: QueryModuleHash,
    query_name: QueryOperationName,
}

impl ExactNamedQueryRequest {
    /// Creates one complete exact lookup key.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        version: ContractVersion,
        contract_hash: ContractBundleHash,
        module_hash: QueryModuleHash,
        query_name: QueryOperationName,
    ) -> Self {
        Self {
            lineage,
            version,
            contract_hash,
            module_hash,
            query_name,
        }
    }

    /// Exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact contract version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Exact contract bundle hash.
    #[must_use]
    pub const fn contract_hash(&self) -> ContractBundleHash {
        self.contract_hash
    }

    /// Exact query-module hash.
    #[must_use]
    pub const fn module_hash(&self) -> QueryModuleHash {
        self.module_hash
    }

    /// Exact public operation name.
    #[must_use]
    pub const fn query_name(&self) -> &QueryOperationName {
        &self.query_name
    }
}

/// Immutable contract and compiled operation selected by one exact lookup.
#[derive(Clone)]
pub struct ResolvedNamedQuery {
    contract: ValidatedContractBundle,
    module: ValidatedQueryModule,
    query_index: usize,
}

impl ResolvedNamedQuery {
    /// Creates one already-validated resolution artifact when the exact query exists.
    #[must_use]
    pub fn try_new(
        contract: ValidatedContractBundle,
        module: ValidatedQueryModule,
        query_name: &QueryOperationName,
    ) -> Option<Self> {
        let query_index = module
            .module()
            .queries()
            .binary_search_by(|query| query.name().cmp(query_name.as_str()))
            .ok()?;
        Some(Self {
            contract,
            module,
            query_index,
        })
    }

    /// Consumes the artifact into its exact shared parts.
    #[must_use]
    pub fn into_parts(self) -> (ValidatedContractBundle, ValidatedQueryModule, usize) {
        (self.contract, self.module, self.query_index)
    }
}

/// Exact query-module observations needed by named execution and inspection.
pub trait QueryModuleReadPort: Send + Sync {
    /// Resolves a warm generated operation through one complete immutable key.
    ///
    /// `None` means this optional fast path could not answer. The service then
    /// executes the established authoritative contract/module lookup, retaining
    /// its complete error taxonomy. Implementations MUST perform ordinary port
    /// admission before returning a cache hit.
    fn prepare_exact_named_query(
        &self,
        _control: &RequestControl,
        _request: ExactNamedQueryRequest,
    ) -> Result<Option<ResolvedNamedQuery>, QueryModuleReadError> {
        Ok(None)
    }

    /// Reads and recompiles the active module for one exact retained contract.
    fn prepare_active_query_module<'a>(
        &'a self,
        control: &'a RequestControl,
        contract: ValidatedContractBundle,
    ) -> PortFuture<'a, Option<ValidatedQueryModule>, QueryModuleReadError>;

    /// Reads and recompiles one content-addressed module for an exact contract.
    fn prepare_query_module<'a>(
        &'a self,
        control: &'a RequestControl,
        contract: ValidatedContractBundle,
        module_hash: QueryModuleHash,
    ) -> PortFuture<'a, Option<ValidatedQueryModule>, QueryModuleReadError>;
}

/// Closed failure while reading and recompiling an immutable reactive module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReactiveModuleReadError {
    /// Storage or bounded execution capacity was unavailable.
    Unavailable,
    /// Durable module bytes or an exact dependency binding failed validation.
    Integrity,
}

/// Exact content-addressed reactive-module observations used by consumer operations.
pub trait ReactiveModuleReadPort: Send + Sync {
    /// Reads and recompiles one module and every exact query dependency.
    fn prepare_reactive_module<'a>(
        &'a self,
        control: &'a RequestControl,
        contract: ValidatedContractBundle,
        module_hash: ReactiveModuleHash,
    ) -> PortFuture<'a, Option<ValidatedReactiveModule>, ReactiveModuleReadError>;
}

/// Narrow durable consumer mutation/inspection port owned by service orchestration.
pub trait EventConsumerPort: Send + Sync {
    /// Reserves one bounded exact consumer operation before final authorization.
    fn reserve_event_consumer<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            EventConsumerPortRequest,
            EventConsumerPortResponse,
            EventConsumerPortError,
        >,
        PortAdmissionError,
    >;
}

/// Closed authoritative-read failure with no storage diagnostic or handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoritativeReadError {
    /// A bounded authoritative read could not complete.
    Unavailable,
    /// Authoritative state failed semantic or reciprocal integrity.
    Integrity,
    /// The requested history existed and was retired by retention pruning
    /// (ADR-0085 A2) — a correct-request client outcome, never an integrity
    /// failure.
    HistoryPruned,
    /// A lower continuation or frozen fence no longer denotes a valid page.
    InvalidContinuation,
    /// Cancellation was observed while waiting for read-path admission.
    Cancelled,
    /// The absolute request deadline elapsed before the read completed admission.
    DeadlineExceeded,
}

/// A lower commit-notification source event.
pub enum AuthoritativeCommitNotification {
    /// The authoritative frontier advanced to this exact nonempty position.
    Advanced(riffdb_types::CommitSequence),
    /// The bounded source detected a gap; the service must terminate with this resume point.
    Gap {
        /// Last commit sequence known safely delivered before the gap.
        resume_after: FrontierPosition,
    },
    /// The bounded lower notification buffer overflowed without identifying a safe next item.
    Lagged {
        /// Last commit sequence known safely delivered before overflow.
        resume_after: FrontierPosition,
    },
    /// The source closed without dropping a known commit.
    Closed,
}

/// Exact lower notification-buffer capacity for each established commit subscription.
///
/// Implementations must terminate with [`AuthoritativeCommitNotification::Lagged`]
/// rather than overwrite or silently drop an item when this capacity is exhausted.
pub const MAX_COMMIT_SUBSCRIPTION_BUFFER_ITEMS: usize = 256;

/// Compiler-resolved exact target for authoritative vector observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoritativeVectorTarget {
    lineage: ContractLineage,
    partition_key: PartitionKey,
    entity_type: riffdb_types::EntityTypeId,
    vector_field: riffdb_types::FieldId,
}

impl AuthoritativeVectorTarget {
    /// Joins catalog-resolved identities; no public request constructs this value.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        partition_key: PartitionKey,
        entity_type: riffdb_types::EntityTypeId,
        vector_field: riffdb_types::FieldId,
    ) -> Self {
        Self {
            lineage,
            partition_key,
            entity_type,
            vector_field,
        }
    }

    /// Exact lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }
    /// Exact logical partition.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }
    /// Catalog-resolved entity identity.
    #[must_use]
    pub const fn entity_type(&self) -> riffdb_types::EntityTypeId {
        self.entity_type
    }
    /// Catalog-resolved vector-field identity.
    #[must_use]
    pub const fn vector_field(&self) -> riffdb_types::FieldId {
        self.vector_field
    }
}

/// Maintained whole-partition vector counts from one authoritative snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoritativeVectorObservation {
    target: AuthoritativeVectorTarget,
    total_entities: u64,
    stale_entities: u64,
    model_counts: Vec<(riffdb_types::EmbeddingMetadata, u64)>,
    revision: CommitSequence,
}

impl AuthoritativeVectorObservation {
    /// Retains one storage-validated observation.
    #[must_use]
    pub const fn new(
        target: AuthoritativeVectorTarget,
        total_entities: u64,
        stale_entities: u64,
        model_counts: Vec<(riffdb_types::EmbeddingMetadata, u64)>,
        revision: CommitSequence,
    ) -> Self {
        Self {
            target,
            total_entities,
            stale_entities,
            model_counts,
            revision,
        }
    }
    /// Exact target.
    #[must_use]
    pub const fn target(&self) -> &AuthoritativeVectorTarget {
        &self.target
    }
    /// Total rows.
    #[must_use]
    pub const fn total_entities(&self) -> u64 {
        self.total_entities
    }
    /// Source-stale rows.
    #[must_use]
    pub const fn stale_entities(&self) -> u64 {
        self.stale_entities
    }
    /// Exact model populations.
    #[must_use]
    pub fn model_counts(&self) -> &[(riffdb_types::EmbeddingMetadata, u64)] {
        &self.model_counts
    }
    /// Last incorporated command sequence.
    #[must_use]
    pub const fn revision(&self) -> CommitSequence {
        self.revision
    }
}

/// One policy-neutral authoritative evidence-index row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoritativeVectorEvidenceRow {
    entity_key: riffdb_types::EntityKey,
    newest_source_write: Option<CommitSequence>,
    embedding_write: Option<(CommitSequence, riffdb_types::EmbeddingMetadata)>,
}

impl AuthoritativeVectorEvidenceRow {
    /// Retains one storage-validated row for later policy admission.
    #[must_use]
    pub const fn new(
        entity_key: riffdb_types::EntityKey,
        newest_source_write: Option<CommitSequence>,
        embedding_write: Option<(CommitSequence, riffdb_types::EmbeddingMetadata)>,
    ) -> Self {
        Self {
            entity_key,
            newest_source_write,
            embedding_write,
        }
    }
    /// Opaque entity identity; never log or render without output authority.
    #[must_use]
    pub const fn entity_key(&self) -> &riffdb_types::EntityKey {
        &self.entity_key
    }
    /// Newest declared source write.
    #[must_use]
    pub const fn newest_source_write(&self) -> Option<CommitSequence> {
        self.newest_source_write
    }
    /// Exact embedding revision and model, when present.
    #[must_use]
    pub const fn embedding_write(
        &self,
    ) -> Option<&(CommitSequence, riffdb_types::EmbeddingMetadata)> {
        self.embedding_write.as_ref()
    }
}

/// Lower bounded evidence-index request; constructed only after symbolic resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoritativeVectorEvidenceRequest {
    target: AuthoritativeVectorTarget,
    after: Option<riffdb_types::EntityKey>,
    limit: PageLimit,
}

impl AuthoritativeVectorEvidenceRequest {
    /// Joins one resolved target and bounded lower continuation.
    #[must_use]
    pub const fn new(
        target: AuthoritativeVectorTarget,
        after: Option<riffdb_types::EntityKey>,
        limit: PageLimit,
    ) -> Self {
        Self {
            target,
            after,
            limit,
        }
    }
    /// Exact target.
    #[must_use]
    pub const fn target(&self) -> &AuthoritativeVectorTarget {
        &self.target
    }
    /// Exclusive lower continuation.
    #[must_use]
    pub const fn after(&self) -> Option<&riffdb_types::EntityKey> {
        self.after.as_ref()
    }
    /// Fixed service page bound.
    #[must_use]
    pub const fn limit(&self) -> PageLimit {
        self.limit
    }
}

/// One bounded policy-neutral lower page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoritativeVectorEvidencePage {
    rows: Vec<AuthoritativeVectorEvidenceRow>,
    continuation: Option<riffdb_types::EntityKey>,
    exact_end: bool,
}

impl AuthoritativeVectorEvidencePage {
    /// Retains a storage-validated bounded page.
    #[must_use]
    pub const fn new(
        rows: Vec<AuthoritativeVectorEvidenceRow>,
        continuation: Option<riffdb_types::EntityKey>,
        exact_end: bool,
    ) -> Self {
        Self {
            rows,
            continuation,
            exact_end,
        }
    }
    /// Policy-neutral candidates.
    #[must_use]
    pub fn rows(&self) -> &[AuthoritativeVectorEvidenceRow] {
        &self.rows
    }
    /// Exclusive continuation.
    #[must_use]
    pub const fn continuation(&self) -> Option<&riffdb_types::EntityKey> {
        self.continuation.as_ref()
    }
    /// Exact end marker.
    #[must_use]
    pub const fn exact_end(&self) -> bool {
        self.exact_end
    }
}

/// Move-only bounded source behind an established commit subscription.
///
/// Each implementation owns exactly one
/// [`MAX_COMMIT_SUBSCRIPTION_BUFFER_ITEMS`]-item notification buffer per
/// subscriber. The source reports overflow as a typed `Lagged` notification.
pub trait CommitNotificationSource: Send {
    /// Waits for one notification without exposing a storage iterator or snapshot.
    fn next(&mut self) -> PortFuture<'_, AuthoritativeCommitNotification, AuthoritativeReadError>;

    /// Records one contiguous commit only after the service has completed all
    /// checks that can still withhold the item from its transport caller.
    ///
    /// Implementations use this exact position when a later bounded-buffer
    /// overflow is reported. A successful acknowledgement must be monotonic
    /// and durable only for the lifetime of this process-local source.
    fn acknowledge(
        &mut self,
        delivered_through: riffdb_types::CommitSequence,
    ) -> Result<(), AuthoritativeReadError>;
}

/// Authoritative entity, outcome, commit, provenance, and capability observations.
pub trait AuthoritativeReadPort: Send + Sync {
    /// Reserves one exact maintained vector-observation read.
    fn reserve_vector_observation<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeVectorTarget,
            Option<AuthoritativeVectorObservation>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        Box::pin(async { Err(PortAdmissionError::Stopped) })
    }

    /// Reserves one bounded policy-neutral vector evidence page.
    fn reserve_vector_evidence<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeVectorEvidenceRequest,
            AuthoritativeVectorEvidencePage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        Box::pin(async { Err(PortAdmissionError::Stopped) })
    }
    /// Reserves capacity for one opaque-wakeup application-head observation.
    fn reserve_application_head<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<(), FrontierPosition, AuthoritativeReadError>,
        PortAdmissionError,
    > {
        Box::pin(async { Err(PortAdmissionError::Stopped) })
    }

    /// Reserves capacity for one catalog-resolved reactive event window.
    fn reserve_reactive_event_window<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeReactiveEventWindowRequest,
            AuthoritativeReactiveEventWindow,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        Box::pin(async { Err(PortAdmissionError::Stopped) })
    }

    /// Reserves capacity for one catalog-resolved partition-local event page.
    fn reserve_replay_events<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeEventReplayRequest,
            crate::AuthoritativeEventReplayPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        Box::pin(async { Err(PortAdmissionError::Stopped) })
    }

    /// Reserves capacity for one exact authoritative entity observation.
    fn reserve_read_entity<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeEntityRequest,
            Option<AuthoritativeEntitySnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity for one bounded, fenced authoritative index page.
    fn reserve_scan_index<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeIndexRequest,
            AuthoritativeIndexPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity for one exact durable command outcome observation.
    fn reserve_read_outcome<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeOutcomeRequest,
            Option<AuthoritativeOutcomeSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity for one exact application commit observation.
    fn reserve_read_commit<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            riffdb_types::CommitSequence,
            Option<AuthoritativeCommitSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity for one bounded upper-fenced commit page.
    fn reserve_scan_commits<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeCommitScanRequest,
            AuthoritativeCommitPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity to establish one bounded lower notification source.
    fn reserve_subscribe_to_commits<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeCommitSubscriptionRequest,
            Box<dyn CommitNotificationSource>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity for one provenance trace root and bounded graph observation.
    fn reserve_trace_provenance<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            ProvenanceSelector,
            Option<AuthoritativeProvenanceSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reads preparatory digest-free capability facts with bounded cancellation.
    ///
    /// The facts are never released as output. They are reloaded while a
    /// control-plane permit is held before the final policy safe point.
    fn read_capability_revoke_target<'a>(
        &'a self,
        control: &'a RequestControl,
        capability_id: CapabilityId,
    ) -> PortFuture<'a, CapabilityRevokeTargetSnapshot, AuthoritativeReadError>;
}

/// Closed derived-projection source failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionPortError {
    /// Projection state could not complete a bounded read or wait.
    Unavailable,
    /// Checked projection identity or state failed integrity.
    Integrity,
}

/// Derived projection query and status source.
pub trait ProjectionQueryPort: Send + Sync {
    /// Reserves capacity for one checked atomic projection observation.
    ///
    /// A behind-frontier observation completes as `PendingObservation`; the
    /// service performs its policy safe point and reserves fresh capacity before
    /// requesting another observation under the original absolute deadline.
    fn reserve_query_projection<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<ProjectionPortRequest, ProjectionPortResult, ProjectionPortError>,
        PortAdmissionError,
    >;

    /// Reserves capacity for one exact projection lifecycle snapshot.
    fn reserve_projection_status<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            riffdb_types::ProjectionIdentity,
            Option<ProjectionStatusSnapshot>,
            ProjectionPortError,
        >,
        PortAdmissionError,
    >;
}

/// Closed failure while observing a published columnar projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnarPortError {
    /// Projection observation capacity or engine state is temporarily unavailable.
    Unavailable,
    /// Checked projection identity or published state failed integrity.
    Integrity,
}

/// Optional lifecycle classification carried with a published observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnarLifecycle {
    /// Published snapshot is queryable.
    Ready,
    /// Catch-up before first publication.
    Building,
    /// Rebuild in progress after detach/rebuild policy.
    Rebuilding {
        /// Closed rebuild reason.
        reason: riffdb_columnar::RebuildingReason,
        /// Progress numerator.
        progress_applied: u64,
        /// Progress denominator (0 = unknown).
        progress_total: u64,
    },
    /// Degraded but still serving.
    Degraded {
        /// Closed degraded reason.
        reason: riffdb_columnar::DegradedReason,
    },
    /// Manifest fingerprint mismatch or other invalid durable state.
    Invalid {
        /// Fingerprint expected by the registered definition.
        expected_fingerprint: riffdb_columnar::DefinitionFingerprint,
        /// Fingerprint found in durable state.
        found_fingerprint: riffdb_columnar::DefinitionFingerprint,
    },
}

/// One published columnar observation with no engine lock retained.
///
/// Callers may hold the `Arc` snapshot and query it after `observe` returns.
#[derive(Clone, Debug)]
pub struct ColumnarObservation {
    definition: riffdb_columnar::RegisteredDefinition,
    snapshot: std::sync::Arc<riffdb_columnar::ColumnarSnapshot>,
    published_frontier: riffdb_types::ProjectionFrontier,
    head: riffdb_types::ProjectionFrontier,
    has_published: bool,
    lifecycle: Option<ColumnarLifecycle>,
}

impl ColumnarObservation {
    /// Attach the trusted process database identity to engine-local frontiers.
    pub(crate) fn scope_to_database(
        mut self,
        database_id: riffdb_types::DatabaseId,
    ) -> Result<Self, ColumnarPortError> {
        if [
            self.published_frontier.database_id(),
            self.head.database_id(),
        ]
        .into_iter()
        .flatten()
        .any(|existing| existing != database_id)
        {
            return Err(ColumnarPortError::Integrity);
        }
        self.published_frontier = riffdb_types::ProjectionFrontier::new_scoped(
            database_id,
            self.published_frontier.history_incarnation(),
            self.published_frontier.position(),
        );
        self.head = riffdb_types::ProjectionFrontier::new_scoped(
            database_id,
            self.head.history_incarnation(),
            self.head.position(),
        );
        Ok(self)
    }

    /// Constructs one complete observation after the engine lock is released.
    #[must_use]
    pub fn new(
        definition: riffdb_columnar::RegisteredDefinition,
        snapshot: std::sync::Arc<riffdb_columnar::ColumnarSnapshot>,
        published_frontier: riffdb_types::ProjectionFrontier,
        head: riffdb_types::ProjectionFrontier,
        has_published: bool,
        lifecycle: Option<ColumnarLifecycle>,
    ) -> Self {
        Self {
            definition,
            snapshot,
            published_frontier,
            head,
            has_published,
            lifecycle,
        }
    }

    /// Registered definition for field resolution and query execution.
    #[must_use]
    pub const fn definition(&self) -> &riffdb_columnar::RegisteredDefinition {
        &self.definition
    }

    /// Published snapshot (`Arc` so the engine lock need not be held).
    #[must_use]
    pub fn snapshot(&self) -> &std::sync::Arc<riffdb_columnar::ColumnarSnapshot> {
        &self.snapshot
    }

    /// Clones the published snapshot handle.
    #[must_use]
    pub fn snapshot_arc(&self) -> std::sync::Arc<riffdb_columnar::ColumnarSnapshot> {
        std::sync::Arc::clone(&self.snapshot)
    }

    /// Visible published frontier of the snapshot.
    #[must_use]
    pub const fn published_frontier(&self) -> &riffdb_types::ProjectionFrontier {
        &self.published_frontier
    }

    /// Application head known when the observation was taken.
    #[must_use]
    pub const fn head(&self) -> &riffdb_types::ProjectionFrontier {
        &self.head
    }

    /// Whether at least one snapshot has been published.
    #[must_use]
    pub const fn has_published(&self) -> bool {
        self.has_published
    }

    /// Optional lifecycle classification for Building/Invalid/etc.
    #[must_use]
    pub const fn lifecycle(&self) -> Option<&ColumnarLifecycle> {
        self.lifecycle.as_ref()
    }
}

/// Service-facing port over published columnar projection state.
///
/// Implementations must release any engine lock before returning
/// [`ColumnarObservation`] so callers can query the snapshot without
/// contending with apply.
pub trait ColumnarProjectionPort: Send + Sync {
    /// Observes one named projection's published snapshot and frontiers.
    fn observe(&self, projection_name: &str) -> Result<ColumnarObservation, ColumnarPortError>;

    /// Returns the registered definition for a known projection name.
    fn definition(&self, projection_name: &str) -> Option<riffdb_columnar::RegisteredDefinition>;

    /// Process-local notifier for register-before-read waits.
    fn notifier(&self) -> &crate::ColumnarNotifier;

    /// Current compiler-owned projection names in canonical order.
    fn known_names(&self) -> Vec<String>;
}

/// One compiler-owned exact-nearest request over a named vector projection.
#[derive(Clone)]
pub struct VectorProjectionRequest {
    source_name: String,
    lineage: ContractLineage,
    partition: PartitionKey,
    partition_value: CanonicalValue,
    entity: EntityTypeId,
    field: FieldId,
    current_model: EmbeddingMetadata,
    stale_entity_threshold: u64,
    query_vector: riffdb_types::CanonicalVector,
    k: u32,
    metric: DistanceMetric,
    predicates: Vec<riffdb_columnar::ColumnPredicate>,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
    minimum_epoch: Option<CommitSequence>,
    max_lag_ms: Option<u64>,
}

impl VectorProjectionRequest {
    /// Constructs a request from one exact compiled source and authorized partition.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        source_name: String,
        lineage: ContractLineage,
        partition: PartitionKey,
        partition_value: CanonicalValue,
        entity: EntityTypeId,
        field: FieldId,
        current_model: EmbeddingMetadata,
        stale_entity_threshold: u64,
        query_vector: riffdb_types::CanonicalVector,
        k: u32,
        metric: DistanceMetric,
        predicates: Vec<riffdb_columnar::ColumnPredicate>,
        row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
        minimum_epoch: Option<CommitSequence>,
        max_lag_ms: Option<u64>,
    ) -> Self {
        Self {
            source_name,
            lineage,
            partition,
            partition_value,
            entity,
            field,
            current_model,
            stale_entity_threshold,
            query_vector,
            k,
            metric,
            predicates,
            row_policy,
            minimum_epoch,
            max_lag_ms,
        }
    }

    /// Exact compiler-selected `Entity.field` source.
    pub fn source_name(&self) -> &str {
        &self.source_name
    }
    /// Contract lineage owning the evidence prefix.
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }
    /// Exact encoded aggregate partition.
    pub const fn partition(&self) -> &PartitionKey {
        &self.partition
    }
    /// Typed partition value used by the projected engine.
    pub const fn partition_value(&self) -> &CanonicalValue {
        &self.partition_value
    }
    /// Stable entity identity.
    pub const fn entity(&self) -> EntityTypeId {
        self.entity
    }
    /// Stable vector-field identity.
    pub const fn field(&self) -> FieldId {
        self.field
    }
    /// Exact current model identity and version.
    pub const fn current_model(&self) -> &EmbeddingMetadata {
        &self.current_model
    }
    /// Strict stale-entity health threshold.
    pub const fn stale_entity_threshold(&self) -> u64 {
        self.stale_entity_threshold
    }
    /// Submitted dimension-checked query vector.
    pub const fn query_vector(&self) -> &riffdb_types::CanonicalVector {
        &self.query_vector
    }
    /// Bound top-K result count.
    pub const fn k(&self) -> u32 {
        self.k
    }
    /// Contract-declared metric.
    pub const fn metric(&self) -> DistanceMetric {
        self.metric
    }
    /// Compiler-lowered scalar filters.
    pub fn predicates(&self) -> &[riffdb_columnar::ColumnPredicate] {
        &self.predicates
    }
    /// Current row-policy authority, when protected.
    pub const fn row_policy(&self) -> Option<&Arc<AuthorizedQueryRowPolicyContextV1>> {
        self.row_policy.as_ref()
    }
    /// Optional causal lower frontier.
    pub const fn minimum_epoch(&self) -> Option<CommitSequence> {
        self.minimum_epoch
    }
    /// Optional trusted duration-lag ceiling.
    pub const fn max_lag_ms(&self) -> Option<u64> {
        self.max_lag_ms
    }
}

impl fmt::Debug for VectorProjectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VectorProjectionRequest([REDACTED])")
    }
}

/// One exact-nearest result bound to a published projection frontier.
#[derive(Clone, Debug)]
pub struct VectorProjectionResult {
    nearest: riffdb_columnar::NearestQueryResult,
    frontier: ProjectionFrontier,
    head: ProjectionFrontier,
}

impl VectorProjectionResult {
    /// Constructs one checked provider result.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        nearest: riffdb_columnar::NearestQueryResult,
        frontier: ProjectionFrontier,
        head: ProjectionFrontier,
    ) -> Self {
        Self {
            nearest,
            frontier,
            head,
        }
    }
    /// Exact ranked rows and honest scan work.
    pub const fn nearest(&self) -> &riffdb_columnar::NearestQueryResult {
        &self.nearest
    }
    /// Consumes the result into its exact parts.
    #[doc(hidden)]
    pub fn into_parts(
        self,
    ) -> (
        riffdb_columnar::NearestQueryResult,
        ProjectionFrontier,
        ProjectionFrontier,
    ) {
        (self.nearest, self.frontier, self.head)
    }
}

/// Closed projected-vector lifecycle, capacity, or integrity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorProjectionPortError {
    /// Projection has no published generation yet.
    Building,
    /// Projection is rebuilding.
    Rebuilding,
    /// Required causal or exact-frontier freshness is unavailable.
    FreshnessUnsatisfied,
    /// Projection is degraded beyond its contract threshold.
    Degraded,
    /// Bounded capacity or provider state is temporarily unavailable.
    Unavailable,
    /// Source, evidence, policy proof, or snapshot failed integrity.
    Integrity,
}

/// Least-authority production vector projection execution boundary.
pub trait VectorProjectionPort: Send + Sync {
    /// Executes one compiler-owned exact-nearest request.
    fn execute(
        &self,
        request: VectorProjectionRequest,
    ) -> Result<VectorProjectionResult, VectorProjectionPortError>;
}

/// One compiler-owned exact-provider request constructed only after current authorization.
#[derive(Clone)]
pub struct ExactTextProjectionRequest {
    query: Arc<riffdb_query_module::CompiledExactTextResultSetV1>,
    partition_key: PartitionKey,
    partition_value: CanonicalValue,
    policy_shape: ApplicationRoleHash,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
    needle: riffdb_types::ExactTextNeedleV1,
    filter_value: Option<CanonicalValue>,
    offset: u32,
    limit: std::num::NonZeroU16,
    minimum_epoch: Option<CommitSequence>,
}

impl ExactTextProjectionRequest {
    /// Seals service-materialized values to one immutable compiled exact plan.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        query: Arc<riffdb_query_module::CompiledExactTextResultSetV1>,
        partition_key: PartitionKey,
        partition_value: CanonicalValue,
        policy_shape: ApplicationRoleHash,
        row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
        needle: riffdb_types::ExactTextNeedleV1,
        filter_value: Option<CanonicalValue>,
        offset: u32,
        limit: std::num::NonZeroU16,
        minimum_epoch: Option<CommitSequence>,
    ) -> Self {
        Self {
            query,
            partition_key,
            partition_value,
            policy_shape,
            row_policy,
            needle,
            filter_value,
            offset,
            limit,
            minimum_epoch,
        }
    }

    /// Immutable exact plan and provider descriptor.
    #[must_use]
    pub const fn query(&self) -> &Arc<riffdb_query_module::CompiledExactTextResultSetV1> {
        &self.query
    }
    /// Exact authorization-routed aggregate partition.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }
    /// Canonical partition value used to validate the compiler-owned index prefix.
    #[must_use]
    pub const fn partition_value(&self) -> &CanonicalValue {
        &self.partition_value
    }
    /// Current compiled role/policy identity checked for this request.
    #[must_use]
    pub const fn policy_shape(&self) -> ApplicationRoleHash {
        self.policy_shape
    }
    /// Current compiler-owned row-policy authority, when this query is protected.
    #[must_use]
    pub const fn row_policy(&self) -> Option<&Arc<AuthorizedQueryRowPolicyContextV1>> {
        self.row_policy.as_ref()
    }
    /// Bounded nonempty exact needle.
    #[must_use]
    pub const fn needle(&self) -> &riffdb_types::ExactTextNeedleV1 {
        &self.needle
    }
    /// Optional typed equality value for the compiler-bound filter dimension.
    #[must_use]
    pub const fn filter_value(&self) -> Option<&CanonicalValue> {
        self.filter_value.as_ref()
    }
    /// Checked zero-based ordinal.
    #[must_use]
    pub const fn offset(&self) -> u32 {
        self.offset
    }
    /// Checked bounded page size.
    #[must_use]
    pub const fn limit(&self) -> std::num::NonZeroU16 {
        self.limit
    }
    /// Optional causal lower frontier.
    #[must_use]
    pub const fn minimum_epoch(&self) -> Option<CommitSequence> {
        self.minimum_epoch
    }
}

impl fmt::Debug for ExactTextProjectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExactTextProjectionRequest([REDACTED])")
    }
}

/// One compiler-shaped exact-provider row; no remote hydration remains.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextProjectionRow {
    key: EntityKey,
    output: CanonicalRecord,
}

impl ExactTextProjectionRow {
    /// Constructs one already-validated derived output row.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(key: EntityKey, output: CanonicalRecord) -> Self {
        Self { key, output }
    }
    /// Canonical entity identity used as the total-order tie breaker.
    #[must_use]
    pub const fn key(&self) -> &EntityKey {
        &self.key
    }
    /// Compiler-shaped projected values.
    #[must_use]
    pub const fn output(&self) -> &CanonicalRecord {
        &self.output
    }

    /// Consumes the row without cloning its bounded projected values.
    #[doc(hidden)]
    #[must_use]
    pub fn into_parts(self) -> (EntityKey, CanonicalRecord) {
        (self.key, self.output)
    }
}

/// One exact page/count observation from a single provider epoch proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextProjectionResult {
    rows: Vec<ExactTextProjectionRow>,
    exact_total: u64,
    epoch: CommitSequence,
    generation: ProjectionGeneration,
    provider: ProjectionProviderDescriptorHash,
    history_incarnation: u64,
    statistics_identity: Option<[u8; 32]>,
}

impl ExactTextProjectionResult {
    /// Constructs a complete checked provider response.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        rows: Vec<ExactTextProjectionRow>,
        exact_total: u64,
        epoch: CommitSequence,
        generation: ProjectionGeneration,
        provider: ProjectionProviderDescriptorHash,
        history_incarnation: u64,
    ) -> Self {
        Self {
            rows,
            exact_total,
            epoch,
            generation,
            provider,
            history_incarnation,
            statistics_identity: None,
        }
    }

    /// Constructs a tokenized result with its authorized statistics identity.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_tokenized(
        rows: Vec<ExactTextProjectionRow>,
        exact_total: u64,
        epoch: CommitSequence,
        generation: ProjectionGeneration,
        provider: ProjectionProviderDescriptorHash,
        history_incarnation: u64,
        statistics_identity: [u8; 32],
    ) -> Self {
        Self {
            rows,
            exact_total,
            epoch,
            generation,
            provider,
            history_incarnation,
            statistics_identity: Some(statistics_identity),
        }
    }
    /// Bounded ordered output rows.
    #[must_use]
    pub fn rows(&self) -> &[ExactTextProjectionRow] {
        &self.rows
    }
    /// Exact admitted cardinality before the ordinal window.
    #[must_use]
    pub const fn exact_total(&self) -> u64 {
        self.exact_total
    }
    /// Shared count/page epoch.
    #[must_use]
    pub const fn epoch(&self) -> CommitSequence {
        self.epoch
    }
    /// Never-reused provider generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }
    /// Pinned provider descriptor digest.
    #[must_use]
    pub const fn provider(&self) -> ProjectionProviderDescriptorHash {
        self.provider
    }
    /// Authoritative history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
    /// Authorized corpus/statistics identity for ranked tokenized results.
    #[must_use]
    pub const fn statistics_identity(&self) -> Option<[u8; 32]> {
        self.statistics_identity
    }

    /// Consumes the checked result into response-assembly parts.
    #[doc(hidden)]
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Vec<ExactTextProjectionRow>,
        u64,
        CommitSequence,
        ProjectionGeneration,
        ProjectionProviderDescriptorHash,
        u64,
    ) {
        (
            self.rows,
            self.exact_total,
            self.epoch,
            self.generation,
            self.provider,
            self.history_incarnation,
        )
    }
}

/// Closed value-free exact-provider lifecycle or integrity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactTextProjectionPortError {
    /// Bounded submitted text is empty or exceeds its compiled term/input shape.
    InputInvalid,
    /// The complete candidate/result set exceeds a compiler-sealed public bound.
    ResponseTooLarge,
    /// Registration or initial rebuild is in progress.
    Building,
    /// The provider is rebuilding a new generation.
    Rebuilding,
    /// The requested snapshot has retired.
    SnapshotRetired,
    /// The provider cannot yet satisfy the causal/current frontier.
    FreshnessUnsatisfied,
    /// Provider intervals or histories cannot form one exact snapshot.
    Diverged,
    /// Bounded provider capacity is temporarily exhausted.
    Unavailable,
    /// Checked plan, state, epoch, or row integrity failed.
    Integrity,
}

/// Least-authority exact derived-result execution boundary.
pub trait ExactTextProjectionPort: Send + Sync {
    /// Executes one compiler-owned request without authoritative scans or row fetches.
    fn execute(
        &self,
        request: ExactTextProjectionRequest,
    ) -> Result<ExactTextProjectionResult, ExactTextProjectionPortError>;
}

/// One compiler-owned long-pattern candidate request formed only after authorization.
#[derive(Clone)]
pub struct LongPatternProjectionRequest {
    program: Arc<riffdb_query_ir::QueryAccessProgramV1>,
    step: QueryAccessStep,
    partition_key: PartitionKey,
    partition_value: CanonicalValue,
    policy_shape: ApplicationRoleHash,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
    pattern: CompiledLongPatternV1,
    minimum_epoch: Option<CommitSequence>,
    pinned_epoch: Option<CommitSequence>,
}

impl LongPatternProjectionRequest {
    /// Seals the checked step, pattern, route, policy, and epoch requirements.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        program: Arc<riffdb_query_ir::QueryAccessProgramV1>,
        step: QueryAccessStep,
        partition_key: PartitionKey,
        partition_value: CanonicalValue,
        policy_shape: ApplicationRoleHash,
        row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
        pattern: CompiledLongPatternV1,
        minimum_epoch: Option<CommitSequence>,
        pinned_epoch: Option<CommitSequence>,
    ) -> Self {
        Self {
            program,
            step,
            partition_key,
            partition_value,
            policy_shape,
            row_policy,
            pattern,
            minimum_epoch,
            pinned_epoch,
        }
    }

    /// Exact executable plan identity.
    #[must_use]
    pub fn plan(&self) -> riffdb_types::QueryPlanHash {
        self.program.identity().hash()
    }
    /// Complete immutable compiler-owned program used for field identity lookup.
    #[doc(hidden)]
    #[must_use]
    pub const fn program(&self) -> &Arc<riffdb_query_ir::QueryAccessProgramV1> {
        &self.program
    }
    /// Exact compiler-owned provider access step.
    #[must_use]
    pub const fn step(&self) -> &QueryAccessStep {
        &self.step
    }
    /// Authorization-routed partition key.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }
    /// Canonical route value used to rebuild the provider partition.
    #[must_use]
    pub const fn partition_value(&self) -> &CanonicalValue {
        &self.partition_value
    }
    /// Current role/policy shape identity.
    #[must_use]
    pub const fn policy_shape(&self) -> ApplicationRoleHash {
        self.policy_shape
    }
    /// Current compiler-owned row policy, when protected.
    #[must_use]
    pub const fn row_policy(&self) -> Option<&Arc<AuthorizedQueryRowPolicyContextV1>> {
        self.row_policy.as_ref()
    }
    /// Fully validated exact pattern machine.
    #[must_use]
    pub const fn pattern(&self) -> &CompiledLongPatternV1 {
        &self.pattern
    }
    /// Optional causal lower frontier.
    #[must_use]
    pub const fn minimum_epoch(&self) -> Option<CommitSequence> {
        self.minimum_epoch
    }
    /// Exact retained epoch required by a continuation.
    #[must_use]
    pub const fn pinned_epoch(&self) -> Option<CommitSequence> {
        self.pinned_epoch
    }
}

impl fmt::Debug for LongPatternProjectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LongPatternProjectionRequest([REDACTED])")
    }
}

/// Least-authority boundary for complete, policy-filtered pattern candidates.
pub trait LongPatternProjectionPort: Send + Sync {
    /// Returns one complete candidate population and exact provider observation.
    fn execute(
        &self,
        request: LongPatternProjectionRequest,
    ) -> Result<riffdb_query_executor::LongPatternCandidateBatch, ExactTextProjectionPortError>;
}

/// One compiler-owned tokenized request constructed only after current authorization.
#[derive(Clone)]
pub struct TokenizedTextProjectionRequest {
    query: Arc<riffdb_query_module::CompiledTokenizedTextResultSetV1>,
    partition_key: PartitionKey,
    partition_value: CanonicalValue,
    policy_shape: ApplicationRoleHash,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
    query_text: String,
    offset: u32,
    limit: std::num::NonZeroU16,
    minimum_epoch: Option<CommitSequence>,
    pinned_epoch: Option<CommitSequence>,
    pinned_generation: Option<ProjectionGeneration>,
}

impl TokenizedTextProjectionRequest {
    /// Seals service-materialized values to one immutable tokenized plan.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        query: Arc<riffdb_query_module::CompiledTokenizedTextResultSetV1>,
        partition_key: PartitionKey,
        partition_value: CanonicalValue,
        policy_shape: ApplicationRoleHash,
        row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
        query_text: String,
        offset: u32,
        limit: std::num::NonZeroU16,
        minimum_epoch: Option<CommitSequence>,
        pinned_snapshot: Option<(CommitSequence, ProjectionGeneration)>,
    ) -> Self {
        Self {
            query,
            partition_key,
            partition_value,
            policy_shape,
            row_policy,
            query_text,
            offset,
            limit,
            minimum_epoch,
            pinned_epoch: pinned_snapshot.map(|snapshot| snapshot.0),
            pinned_generation: pinned_snapshot.map(|snapshot| snapshot.1),
        }
    }

    /// Immutable tokenized plan and provider descriptor.
    #[must_use]
    pub const fn query(&self) -> &Arc<riffdb_query_module::CompiledTokenizedTextResultSetV1> {
        &self.query
    }
    /// Authorization-routed aggregate partition.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }
    /// Canonical partition value used to validate the compiler-owned index prefix.
    #[must_use]
    pub const fn partition_value(&self) -> &CanonicalValue {
        &self.partition_value
    }
    /// Current compiled role/policy identity checked for this request.
    #[must_use]
    pub const fn policy_shape(&self) -> ApplicationRoleHash {
        self.policy_shape
    }
    /// Current compiler-owned row-policy authority, when protected.
    #[must_use]
    pub const fn row_policy(&self) -> Option<&Arc<AuthorizedQueryRowPolicyContextV1>> {
        self.row_policy.as_ref()
    }
    /// Bounded text analyzed only by the compiled provider.
    #[must_use]
    pub fn query_text(&self) -> &str {
        &self.query_text
    }
    /// Checked zero-based ordinal.
    #[must_use]
    pub const fn offset(&self) -> u32 {
        self.offset
    }
    /// Checked bounded page size.
    #[must_use]
    pub const fn limit(&self) -> std::num::NonZeroU16 {
        self.limit
    }
    /// Optional causal lower frontier.
    #[must_use]
    pub const fn minimum_epoch(&self) -> Option<CommitSequence> {
        self.minimum_epoch
    }

    /// Exact retained snapshot required by a continuation.
    #[must_use]
    pub const fn pinned_snapshot(&self) -> Option<(CommitSequence, ProjectionGeneration)> {
        match (self.pinned_epoch, self.pinned_generation) {
            (Some(epoch), Some(generation)) => Some((epoch, generation)),
            _ => None,
        }
    }
}

impl fmt::Debug for TokenizedTextProjectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TokenizedTextProjectionRequest([REDACTED])")
    }
}

/// Least-authority tokenized derived-result execution boundary.
pub trait TokenizedTextProjectionPort: Send + Sync {
    /// Executes one compiler-owned request without authoritative scans or row fetches.
    fn execute(
        &self,
        request: TokenizedTextProjectionRequest,
    ) -> Result<ExactTextProjectionResult, ExactTextProjectionPortError>;
}

/// One compiler-owned exact-predicate request constructed after current authorization.
#[derive(Clone)]
pub struct ExactPredicateProjectionRequest {
    query: Arc<riffdb_query_module::CompiledExactPredicateResultSetV1>,
    partition_key: PartitionKey,
    partition_value: CanonicalValue,
    policy_shape: ApplicationRoleHash,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
    parameters: std::collections::BTreeMap<u16, ExactParameterValueV1>,
    member: ExactPredicateFamilyMemberV1,
    offset: u32,
    limit: std::num::NonZeroU16,
    minimum_epoch: Option<CommitSequence>,
}

impl ExactPredicateProjectionRequest {
    /// Seals service-materialized values to one immutable compiled predicate program.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        query: Arc<riffdb_query_module::CompiledExactPredicateResultSetV1>,
        partition_key: PartitionKey,
        partition_value: CanonicalValue,
        policy_shape: ApplicationRoleHash,
        row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
        parameters: std::collections::BTreeMap<u16, ExactParameterValueV1>,
        member: ExactPredicateFamilyMemberV1,
        offset: u32,
        limit: std::num::NonZeroU16,
        minimum_epoch: Option<CommitSequence>,
    ) -> Self {
        Self {
            query,
            partition_key,
            partition_value,
            policy_shape,
            row_policy,
            parameters,
            member,
            offset,
            limit,
            minimum_epoch,
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn query(&self) -> &Arc<riffdb_query_module::CompiledExactPredicateResultSetV1> {
        &self.query
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn partition_value(&self) -> &CanonicalValue {
        &self.partition_value
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn policy_shape(&self) -> ApplicationRoleHash {
        self.policy_shape
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn row_policy(&self) -> Option<&Arc<AuthorizedQueryRowPolicyContextV1>> {
        self.row_policy.as_ref()
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn parameters(&self) -> &std::collections::BTreeMap<u16, ExactParameterValueV1> {
        &self.parameters
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn member(&self) -> ExactPredicateFamilyMemberV1 {
        self.member
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn offset(&self) -> u32 {
        self.offset
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn limit(&self) -> std::num::NonZeroU16 {
        self.limit
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn minimum_epoch(&self) -> Option<CommitSequence> {
        self.minimum_epoch
    }
}

impl fmt::Debug for ExactPredicateProjectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExactPredicateProjectionRequest([REDACTED])")
    }
}

/// One compiler-owned nullable exact-order request after current authorization.
#[derive(Clone)]
pub struct NullableExactPredicateProjectionRequest {
    query: Arc<riffdb_query_module::CompiledNullableExactPredicateResultSetV1>,
    partition_key: PartitionKey,
    partition_value: CanonicalValue,
    policy_shape: ApplicationRoleHash,
    row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
    parameters: std::collections::BTreeMap<u16, ExactParameterValueV1>,
    member: ExactPredicateFamilyMemberV1,
    offset: u32,
    limit: std::num::NonZeroU16,
    minimum_epoch: Option<CommitSequence>,
}

impl NullableExactPredicateProjectionRequest {
    /// Seals service-materialized values to one immutable V5 nullable program.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        query: Arc<riffdb_query_module::CompiledNullableExactPredicateResultSetV1>,
        partition_key: PartitionKey,
        partition_value: CanonicalValue,
        policy_shape: ApplicationRoleHash,
        row_policy: Option<Arc<AuthorizedQueryRowPolicyContextV1>>,
        parameters: std::collections::BTreeMap<u16, ExactParameterValueV1>,
        member: ExactPredicateFamilyMemberV1,
        offset: u32,
        limit: std::num::NonZeroU16,
        minimum_epoch: Option<CommitSequence>,
    ) -> Self {
        Self {
            query,
            partition_key,
            partition_value,
            policy_shape,
            row_policy,
            parameters,
            member,
            offset,
            limit,
            minimum_epoch,
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn query(
        &self,
    ) -> &Arc<riffdb_query_module::CompiledNullableExactPredicateResultSetV1> {
        &self.query
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn partition_value(&self) -> &CanonicalValue {
        &self.partition_value
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn policy_shape(&self) -> ApplicationRoleHash {
        self.policy_shape
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn row_policy(&self) -> Option<&Arc<AuthorizedQueryRowPolicyContextV1>> {
        self.row_policy.as_ref()
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn parameters(&self) -> &std::collections::BTreeMap<u16, ExactParameterValueV1> {
        &self.parameters
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn member(&self) -> ExactPredicateFamilyMemberV1 {
        self.member
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn offset(&self) -> u32 {
        self.offset
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn limit(&self) -> std::num::NonZeroU16 {
        self.limit
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn minimum_epoch(&self) -> Option<CommitSequence> {
        self.minimum_epoch
    }
}

impl fmt::Debug for NullableExactPredicateProjectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NullableExactPredicateProjectionRequest([REDACTED])")
    }
}

/// One exact predicate page/count observation from one provider epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactPredicateProjectionResult {
    rows: Vec<ExactTextProjectionRow>,
    exact_total: u64,
    epoch: CommitSequence,
    generation: ProjectionGeneration,
    provider: ProjectionProviderDescriptorHash,
    history_incarnation: u64,
}

impl ExactPredicateProjectionResult {
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        rows: Vec<ExactTextProjectionRow>,
        exact_total: u64,
        epoch: CommitSequence,
        generation: ProjectionGeneration,
        provider: ProjectionProviderDescriptorHash,
        history_incarnation: u64,
    ) -> Self {
        Self {
            rows,
            exact_total,
            epoch,
            generation,
            provider,
            history_incarnation,
        }
    }

    /// Bounded ordered output rows.
    #[must_use]
    pub fn rows(&self) -> &[ExactTextProjectionRow] {
        &self.rows
    }

    /// Exact admitted cardinality before the ordinal window.
    #[must_use]
    pub const fn exact_total(&self) -> u64 {
        self.exact_total
    }

    /// Shared count/page epoch.
    #[must_use]
    pub const fn epoch(&self) -> CommitSequence {
        self.epoch
    }

    /// Never-reused provider generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Pinned provider descriptor digest.
    #[must_use]
    pub const fn provider(&self) -> ProjectionProviderDescriptorHash {
        self.provider
    }

    /// Authoritative history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Consumes the checked result into response-assembly parts.
    #[doc(hidden)]
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Vec<ExactTextProjectionRow>,
        u64,
        CommitSequence,
        ProjectionGeneration,
        ProjectionProviderDescriptorHash,
        u64,
    ) {
        (
            self.rows,
            self.exact_total,
            self.epoch,
            self.generation,
            self.provider,
            self.history_incarnation,
        )
    }
}

/// Least-authority exact predicate derived-result execution boundary.
pub trait ExactPredicateProjectionPort: Send + Sync {
    /// Executes one compiler-owned request without authoritative request-time reads.
    fn execute(
        &self,
        request: ExactPredicateProjectionRequest,
    ) -> Result<ExactPredicateProjectionResult, ExactTextProjectionPortError>;

    /// Executes one compiler-owned nullable-order request against V5 state.
    fn execute_nullable(
        &self,
        request: NullableExactPredicateProjectionRequest,
    ) -> Result<ExactPredicateProjectionResult, ExactTextProjectionPortError>;
}

/// Closed payload-free outbox-status source failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxStatusPortError {
    /// Outbox status is not installed or temporarily unavailable.
    Unavailable,
    /// Checked status state failed integrity.
    Integrity,
}

/// Optional payload-free outbox status source.
pub trait OutboxStatusPort: Send + Sync {
    /// Reserves capacity for one page without event payloads or connector secrets.
    fn reserve_pending_status<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<OutboxStatusRequest, OutboxStatusSnapshot, OutboxStatusPortError>,
        PortAdmissionError,
    >;
}

/// One fully authorized start command transferred to the maintenance controller.
///
/// The service constructs this only after its final current-policy safe point.
/// Restore retains the same ordinary bearer solely so the controller can
/// independently authenticate and authorize it against validated staging.
pub enum AuthorizedOfflineMaintenanceStart {
    /// Publish one immutable backup from the ready current database.
    CreateBackup {
        /// Checked semantic input and caller-stable receipt identity.
        request: CreateOfflineBackupRequest,
        /// Fresh current-database policy proof for this exact input hash.
        authorization: Box<AuthorizedOfflineMaintenance>,
    },
    /// Restore one immutable backup through independently authorized staging.
    RestoreBackup {
        /// Checked semantic input, confirmation, and receipt identity.
        request: RestoreOfflineBackupRequest,
        /// Fresh current-database policy proof for this exact input hash.
        authorization: Box<AuthorizedOfflineMaintenance>,
        /// Move-only bearer retained for fresh staged authentication.
        credential: RetainedOpaqueCredential,
    },
    /// Restore a backup and archived suffix under a distinct canonical request.
    RestoreArchivedBackup {
        /// Checked archive name, exact stop and caller-stable operation identity.
        request: crate::RestoreArchivedBackupRequest,
        /// Current global restore authority bound to every semantic input.
        authorization: Box<AuthorizedOfflineMaintenance>,
        /// The same bearer, retained only for independent staged authorization.
        credential: RetainedOpaqueCredential,
    },
    /// Retire one exact immutable backup under the same global authority.
    RetireBackup {
        /// Checked semantic input and caller-stable receipt identity.
        request: RetireOfflineBackupRequest,
        /// Fresh current-database policy proof for this exact input hash.
        authorization: Box<AuthorizedOfflineMaintenance>,
    },
}

impl std::fmt::Debug for AuthorizedOfflineMaintenanceStart {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::CreateBackup { .. } => {
                "AuthorizedOfflineMaintenanceStart::CreateBackup([REDACTED])"
            }
            Self::RestoreBackup { .. } => {
                "AuthorizedOfflineMaintenanceStart::RestoreBackup([REDACTED])"
            }
            Self::RestoreArchivedBackup { .. } => {
                "AuthorizedOfflineMaintenanceStart::RestoreArchivedBackup([REDACTED])"
            }
            Self::RetireBackup { .. } => {
                "AuthorizedOfflineMaintenanceStart::RetireBackup([REDACTED])"
            }
        })
    }
}

/// One fully authorized receipt observation request.
pub struct AuthorizedOfflineMaintenanceObservation {
    request: GetOfflineMaintenanceOperationRequest,
    authorization: Box<AuthorizedOfflineMaintenance>,
}

impl AuthorizedOfflineMaintenanceObservation {
    pub(crate) const fn new(
        request: GetOfflineMaintenanceOperationRequest,
        authorization: Box<AuthorizedOfflineMaintenance>,
    ) -> Self {
        Self {
            request,
            authorization,
        }
    }

    /// Returns the checked caller-stable operation selector.
    #[must_use]
    pub const fn request(&self) -> GetOfflineMaintenanceOperationRequest {
        self.request
    }

    /// Borrows the fresh current-policy proof.
    #[must_use]
    pub const fn authorization(&self) -> &AuthorizedOfflineMaintenance {
        &self.authorization
    }

    /// Separates the checked request from the move-only proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        GetOfflineMaintenanceOperationRequest,
        Box<AuthorizedOfflineMaintenance>,
    ) {
        (self.request, self.authorization)
    }
}

impl std::fmt::Debug for AuthorizedOfflineMaintenanceObservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthorizedOfflineMaintenanceObservation([REDACTED])")
    }
}

/// Closed failure after a maintenance start was submitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineMaintenanceStartPortError {
    /// The caller-stable operation ID names different semantic input.
    InputMismatch,
    /// The controller proved no durable receipt or work was admitted.
    Unavailable,
    /// The start may have been durably accepted but no result is known.
    OutcomeUnknown,
    /// Receipt-derived state violated a checked semantic invariant.
    Integrity,
}

/// Closed failure while reading one maintenance observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineMaintenanceObservationPortError {
    /// Receipt state could not be accessed safely.
    Unavailable,
    /// Receipt-derived state violated a checked semantic invariant.
    Integrity,
}

/// Permit to start or resolve one exact offline-maintenance operation.
pub type OfflineMaintenanceStartPermit = BoxPortCapacityPermit<
    AuthorizedOfflineMaintenanceStart,
    OfflineMaintenanceStartResult,
    OfflineMaintenanceStartPortError,
>;

/// Permit to read one exact receipt-backed operation observation.
pub type OfflineMaintenanceObservationPermit = BoxPortCapacityPermit<
    AuthorizedOfflineMaintenanceObservation,
    Option<OfflineMaintenanceOperationObservation>,
    OfflineMaintenanceObservationPortError,
>;

/// Server-private lifecycle and durable-receipt coordination boundary.
///
/// A successful start completion is published only after the external receipt
/// was durably created or exact-input-resolved. `Accepted` transfers ownership
/// of the newly admitted driver to this port's implementation; the service
/// never owns or launches that driver. Terminal results are derived only from
/// a validated terminal receipt. Restore must independently validate staging,
/// freshly authenticate and authorize the retained bearer there, and drop it
/// before publication or terminal receipt persistence; the current proof
/// cannot satisfy that staged safe point.
pub trait OfflineMaintenanceCoordinatorPort: Send + Sync {
    /// Reserves bounded capacity for one authorized receipt start/resolution.
    fn reserve_start(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, OfflineMaintenanceStartPermit, PortAdmissionError>;

    /// Reserves bounded capacity for one protected receipt observation.
    fn reserve_observation(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, OfflineMaintenanceObservationPermit, PortAdmissionError>;
}

/// One exact migration start paired with a fresh move-only policy proof.
pub enum AuthorizedContractMigrationStart {
    /// Complete read-only preflight.
    Check {
        /// Original transport request identity retained in the receipt.
        request_id: RequestId,
        /// Trusted ingress classification retained in the receipt.
        ingress: riffdb_types::ServiceIngressKindV1,
        /// Exact checked semantic request.
        request: CheckContractMigrationRequest,
        /// Fresh current-policy proof.
        authorization: Box<AuthorizedContractMigration>,
    },
    /// Complete offline staged migration.
    Apply {
        /// Original transport request identity retained in the receipt.
        request_id: RequestId,
        /// Trusted ingress classification retained in the receipt.
        ingress: riffdb_types::ServiceIngressKindV1,
        /// Exact checked semantic request.
        request: ApplyContractMigrationRequest,
        /// Fresh current-policy proof.
        authorization: Box<AuthorizedContractMigration>,
    },
}

impl AuthorizedContractMigrationStart {
    /// Returns the exact checked operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ContractMigrationOperationId {
        match self {
            Self::Check { request, .. } => request.operation_id(),
            Self::Apply { request, .. } => request.operation_id(),
        }
    }

    /// Separates a check request from its proof.
    #[must_use]
    pub fn into_check(
        self,
    ) -> Option<(
        RequestId,
        riffdb_types::ServiceIngressKindV1,
        CheckContractMigrationRequest,
        Box<AuthorizedContractMigration>,
    )> {
        match self {
            Self::Check {
                request_id,
                ingress,
                request,
                authorization,
            } => Some((request_id, ingress, request, authorization)),
            Self::Apply { .. } => None,
        }
    }

    /// Separates an apply request from its proof.
    #[must_use]
    pub fn into_apply(
        self,
    ) -> Option<(
        RequestId,
        riffdb_types::ServiceIngressKindV1,
        ApplyContractMigrationRequest,
        Box<AuthorizedContractMigration>,
    )> {
        match self {
            Self::Apply {
                request_id,
                ingress,
                request,
                authorization,
            } => Some((request_id, ingress, request, authorization)),
            Self::Check { .. } => None,
        }
    }
}

impl std::fmt::Debug for AuthorizedContractMigrationStart {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthorizedContractMigrationStart([REDACTED])")
    }
}

/// One exact receipt observation paired with current lineage-scoped authority.
pub struct AuthorizedContractMigrationObservation {
    request: GetContractMigrationOperationRequest,
    authorization: Box<AuthorizedContractMigration>,
}

impl AuthorizedContractMigrationObservation {
    pub(crate) const fn new(
        request: GetContractMigrationOperationRequest,
        authorization: Box<AuthorizedContractMigration>,
    ) -> Self {
        Self {
            request,
            authorization,
        }
    }

    /// Separates the exact selector from the fresh proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        GetContractMigrationOperationRequest,
        Box<AuthorizedContractMigration>,
    ) {
        (self.request, self.authorization)
    }
}

impl std::fmt::Debug for AuthorizedContractMigrationObservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthorizedContractMigrationObservation([REDACTED])")
    }
}

/// Closed failure after a migration start was submitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractMigrationStartPortError {
    /// The caller-stable operation ID names different semantic input.
    InputMismatch,
    /// The exact successor is already active with different durable evidence.
    AlreadyAppliedMismatch,
    /// The controller proved no durable receipt or work was admitted.
    Unavailable,
    /// The start may have been durably accepted but no result is known.
    OutcomeUnknown,
    /// Receipt-derived state violated a checked semantic invariant.
    Integrity,
}

/// Closed failure while resolving or reading a migration receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractMigrationObservationPortError {
    /// Receipt state could not be accessed safely.
    Unavailable,
    /// Receipt-derived state violated a checked semantic invariant.
    Integrity,
}

/// Permit to start or resolve one exact migration operation.
pub type ContractMigrationStartPermit = BoxPortCapacityPermit<
    AuthorizedContractMigrationStart,
    ContractMigrationStartResult,
    ContractMigrationStartPortError,
>;

/// Permit to read one exact receipt-backed migration observation.
pub type ContractMigrationObservationPermit = BoxPortCapacityPermit<
    AuthorizedContractMigrationObservation,
    Option<ContractMigrationOperationObservation>,
    ContractMigrationObservationPortError,
>;

/// Server-private migration receipt and selected-database lifecycle boundary.
pub trait ContractMigrationCoordinatorPort: Send + Sync {
    /// Resolves only the receipt lineage needed for exact observation policy.
    fn resolve_operation_lineage(
        &self,
        operation_id: ContractMigrationOperationId,
        control: &RequestControl,
    ) -> PortFuture<'_, Option<ContractLineage>, ContractMigrationObservationPortError>;

    /// Reserves bounded capacity for one authorized start or idempotent retry.
    fn reserve_start(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ContractMigrationStartPermit, PortAdmissionError>;

    /// Reserves bounded capacity for one protected receipt observation.
    fn reserve_observation(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ContractMigrationObservationPermit, PortAdmissionError>;
}

/// Closed ordinary/archive input for restricted restore coordinators.
#[derive(Debug)]
pub enum OfflineRestoreRequest {
    /// Frozen ordinary backup restore input.
    Ordinary(RestoreOfflineBackupRequest),
    /// Frozen archive restore input with its distinct canonical hash.
    Archived(crate::RestoreArchivedBackupRequest),
}
impl From<RestoreOfflineBackupRequest> for OfflineRestoreRequest {
    fn from(request: RestoreOfflineBackupRequest) -> Self {
        Self::Ordinary(request)
    }
}
impl From<crate::RestoreArchivedBackupRequest> for OfflineRestoreRequest {
    fn from(request: crate::RestoreArchivedBackupRequest) -> Self {
        Self::Archived(request)
    }
}
impl OfflineRestoreRequest {
    /// Returns the operation identity whose receipt is already durable.
    #[must_use]
    pub const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        match self {
            Self::Ordinary(request) => request.operation_id(),
            Self::Archived(request) => request.operation_id(),
        }
    }
    /// Returns the exact input identity, including the restore domain.
    #[must_use]
    pub const fn input_hash(&self) -> OfflineMaintenanceInputHash {
        match self {
            Self::Ordinary(request) => request.input_hash(),
            Self::Archived(request) => request.input_hash(),
        }
    }
}

/// One freshly authorized retry of an already-durable restore receipt.
///
/// This move-only command is distinct from [`AuthorizedOfflineMaintenanceStart`]
/// so a credential-retry host cannot submit backup creation or observe a
/// maintenance receipt through its coordinator capability.
pub struct AuthorizedRestoreRetryStart {
    request: OfflineRestoreRequest,
    authorization: Box<AuthorizedOfflineMaintenance>,
    credential: RetainedOpaqueCredential,
}

impl AuthorizedRestoreRetryStart {
    pub(crate) const fn new(
        request: RestoreOfflineBackupRequest,
        authorization: Box<AuthorizedOfflineMaintenance>,
        credential: RetainedOpaqueCredential,
    ) -> Self {
        Self {
            request: OfflineRestoreRequest::Ordinary(request),
            authorization,
            credential,
        }
    }

    pub(crate) const fn new_archive(
        request: crate::RestoreArchivedBackupRequest,
        authorization: Box<AuthorizedOfflineMaintenance>,
        credential: RetainedOpaqueCredential,
    ) -> Self {
        Self {
            request: OfflineRestoreRequest::Archived(request),
            authorization,
            credential,
        }
    }

    /// Returns the exact immutable restore input.
    #[must_use]
    pub const fn request(&self) -> &OfflineRestoreRequest {
        &self.request
    }

    /// Separates the restore input, fresh current-policy proof, and retained bearer.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        OfflineRestoreRequest,
        Box<AuthorizedOfflineMaintenance>,
        RetainedOpaqueCredential,
    ) {
        (self.request, self.authorization, self.credential)
    }
}

impl std::fmt::Debug for AuthorizedRestoreRetryStart {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthorizedRestoreRetryStart([REDACTED])")
    }
}

/// Permit for one exact, previously admitted restore retry.
pub type RestoreRetryOfflineMaintenancePermit = BoxPortCapacityPermit<
    AuthorizedRestoreRetryStart,
    OfflineMaintenanceStartResult,
    OfflineMaintenanceStartPortError,
>;

/// Least-authority coordinator capability for a frozen current-database retry.
pub trait RestoreRetryOfflineMaintenanceCoordinatorPort: Send + Sync {
    /// Reserves bounded capacity for the one exact restore operation.
    fn reserve_restore(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, RestoreRetryOfflineMaintenancePermit, PortAdmissionError>;
}

/// Recovery-only restore command with no current principal or policy proof.
///
/// The distinct recovery controller is responsible for private staging,
/// complete validation, fresh authentication of the retained bearer, and
/// fresh staged authorization before it may admit a receipt or publish.
pub struct RecoveryOfflineMaintenanceRestore {
    request_id: RequestId,
    request: OfflineRestoreRequest,
    credential: RetainedOpaqueCredential,
}

impl RecoveryOfflineMaintenanceRestore {
    pub(crate) const fn new(
        request_id: RequestId,
        request: OfflineRestoreRequest,
        credential: RetainedOpaqueCredential,
    ) -> Self {
        Self {
            request_id,
            request,
            credential,
        }
    }

    /// Returns the fresh transport request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Borrows the checked restore semantic input.
    #[must_use]
    pub const fn request(&self) -> &OfflineRestoreRequest {
        &self.request
    }

    /// Separates the request from the move-only staged-auth credential.
    #[must_use]
    pub fn into_parts(self) -> (RequestId, OfflineRestoreRequest, RetainedOpaqueCredential) {
        (self.request_id, self.request, self.credential)
    }
}

impl std::fmt::Debug for RecoveryOfflineMaintenanceRestore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RecoveryOfflineMaintenanceRestore([REDACTED])")
    }
}

/// Closed recovery-controller failure before a receipt-derived result exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryOfflineMaintenancePortError {
    /// The caller-stable operation ID names different semantic input.
    InputMismatch,
    /// Fresh staged authentication or authorization denied the bearer.
    AuthorizationDenied,
    /// The controller proved no receipt, publication, or work was admitted.
    Unavailable,
    /// The restore may have been admitted but no result is known.
    OutcomeUnknown,
    /// Recovery state violated a checked semantic invariant.
    Integrity,
}

/// Permit for the sole restricted recovery-mode restore operation.
pub type RecoveryOfflineMaintenanceRestorePermit = BoxPortCapacityPermit<
    RecoveryOfflineMaintenanceRestore,
    OfflineMaintenanceStartResult,
    RecoveryOfflineMaintenancePortError,
>;

/// Server-private staged-only recovery controller boundary.
///
/// The implementation must validate the immutable backup and complete staged
/// database before borrowing the retained credential for fresh authentication
/// and authorization. It must drop the credential before target publication
/// or terminal receipt persistence. It exposes no create or observation method.
pub trait RecoveryOfflineMaintenanceCoordinatorPort: Send + Sync {
    /// Reserves bounded capacity for one staged-only recovery restore.
    fn reserve_restore(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, RecoveryOfflineMaintenanceRestorePermit, PortAdmissionError>;
}

/// Closed operational-state source failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalStatusError {
    /// The bounded operational view is unavailable.
    Unavailable,
    /// The supplied operational snapshot failed service-owned invariants.
    Integrity,
}

/// Bounded process/lifecycle observations with no authority to mutate storage.
pub trait OperationalStatusPort: Send + Sync {
    /// Reserves capacity for bounded authenticated health facts.
    fn reserve_health<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<(), OperationalHealthSnapshot, OperationalStatusError>,
        PortAdmissionError,
    >;

    /// Reserves capacity for bounded authenticated operational counters.
    fn reserve_statistics<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<(), OperationalStatisticsSnapshot, OperationalStatusError>,
        PortAdmissionError,
    >;
}

pub use riffdb_observability::{
    AuthoritativeReadinessFailure, CapacityRejectionStage, NoopServiceTelemetry, ReadPipelineStage,
    ServiceDiagnostics, ServiceHealthHooks, ServiceTelemetry, ServiceTelemetryEvent,
    ServiceTerminalClass, WriteServiceStage,
};
#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::task::{Context, Poll, Wake, Waker};

    use super::{PortDriverStopped, PortReceipt, port_completion_channel};

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll_once<T, E>(
        receipt: Pin<&mut PortReceipt<T, E>>,
        waker: &Waker,
    ) -> Poll<Result<Result<T, E>, PortDriverStopped>> {
        let mut context = Context::from_waker(waker);
        receipt.poll(&mut context)
    }

    #[test]
    fn completion_before_first_poll_is_retained() {
        let (sender, receipt) = port_completion_channel::<u32, &'static str>();
        sender.complete(Ok(17));

        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&wake_counter));
        let mut receipt = Box::pin(receipt);

        assert_eq!(poll_once(receipt.as_mut(), &waker), Poll::Ready(Ok(Ok(17))));
        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn pending_receipt_is_woken_by_completion() {
        let (sender, receipt) = port_completion_channel::<u32, &'static str>();
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&wake_counter));
        let mut receipt = Box::pin(receipt);

        assert_eq!(poll_once(receipt.as_mut(), &waker), Poll::Pending);
        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 0);

        sender.complete(Err("closed failure"));

        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            poll_once(receipt.as_mut(), &waker),
            Poll::Ready(Ok(Err("closed failure")))
        );
    }

    #[test]
    fn sender_abandonment_wakes_with_driver_stopped() {
        let (sender, receipt) = port_completion_channel::<u32, &'static str>();
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&wake_counter));
        let mut receipt = Box::pin(receipt);

        assert_eq!(poll_once(receipt.as_mut(), &waker), Poll::Pending);
        drop(sender);

        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            poll_once(receipt.as_mut(), &waker),
            Poll::Ready(Err(PortDriverStopped))
        );
    }

    #[test]
    fn dropping_receiver_does_not_cancel_independently_owned_work() {
        let (sender, receipt) = port_completion_channel::<u32, &'static str>();
        let release_worker = Arc::new(Barrier::new(2));
        let work_completed = Arc::new(AtomicBool::new(false));

        let worker_barrier = Arc::clone(&release_worker);
        let worker_completed = Arc::clone(&work_completed);
        let worker = std::thread::spawn(move || {
            worker_barrier.wait();
            sender.complete(Ok(23));
            worker_completed.store(true, Ordering::SeqCst);
        });

        drop(receipt);
        release_worker.wait();
        worker.join().expect("independent port worker must finish");

        assert!(work_completed.load(Ordering::SeqCst));
    }
}
