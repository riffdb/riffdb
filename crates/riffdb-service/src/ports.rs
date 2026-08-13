//! Least-authority consumer ports used by service orchestration.

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
use riffdb_errors::InternalError;
use riffdb_policy::{
    ApplicationExportAuthorizationRequestV1, ApplicationExportDecisionV1,
    ApplicationReimportAuthorizationRequestV1, ApplicationReimportDecisionV1, AuthorizationClock,
    AuthorizationError, AuthorizationTelemetry, AuthorizedContractMigration,
    AuthorizedOfflineMaintenance, CapabilityViewCheckpoint, ContractMigrationAuthorizationRequest,
    ContractMigrationDecision, CurrentAuthorizer, Decision, OfflineMaintenanceAuthorizationRequest,
    OfflineMaintenanceDecision, OperationRequest, ProvenanceSelector,
};
use riffdb_types::{
    CapabilityId, CommandId, ContractBundleHash, ContractLineage, ContractMigrationOperationId,
    ContractVersion, FrontierPosition, PlanHash, QueryModuleHash, ReactiveModuleHash, RequestId,
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
    OutboxStatusRequest, OutboxStatusSnapshot, ProjectionPortRequest, ProjectionPortResult,
    ProjectionStatusSnapshot, RequestControl, RestoreOfflineBackupRequest,
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

/// Exact query-module observations needed by named execution and inspection.
pub trait QueryModuleReadPort: Send + Sync {
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

    /// Startup-fixed known projection names.
    fn known_names(&self) -> &[String];
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

/// One freshly authorized retry of an already-durable restore receipt.
///
/// This move-only command is distinct from [`AuthorizedOfflineMaintenanceStart`]
/// so a credential-retry host cannot submit backup creation or observe a
/// maintenance receipt through its coordinator capability.
pub struct AuthorizedRestoreRetryStart {
    request: RestoreOfflineBackupRequest,
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
            request,
            authorization,
            credential,
        }
    }

    /// Returns the exact immutable restore input.
    #[must_use]
    pub const fn request(&self) -> &RestoreOfflineBackupRequest {
        &self.request
    }

    /// Separates the restore input, fresh current-policy proof, and retained bearer.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        RestoreOfflineBackupRequest,
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
    request: RestoreOfflineBackupRequest,
    credential: RetainedOpaqueCredential,
}

impl RecoveryOfflineMaintenanceRestore {
    pub(crate) const fn new(
        request_id: RequestId,
        request: RestoreOfflineBackupRequest,
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
    pub const fn request(&self) -> &RestoreOfflineBackupRequest {
        &self.request
    }

    /// Separates the request from the move-only staged-auth credential.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        RequestId,
        RestoreOfflineBackupRequest,
        RetainedOpaqueCredential,
    ) {
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

/// Cached process/lifecycle status, disjoint from authoritative storage access.
pub trait OperationalStatusPort: Send + Sync {
    /// Reserves capacity for bounded authenticated health facts.
    fn reserve_health(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<(), OperationalHealthSnapshot, OperationalStatusError>,
        PortAdmissionError,
    >;

    /// Reserves capacity for bounded authenticated operational counters.
    fn reserve_statistics(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<(), OperationalStatisticsSnapshot, OperationalStatusError>,
        PortAdmissionError,
    >;
}

/// Closed stage at which command capacity admission rejected a request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CapacityRejectionStage {
    /// Coordinator queue depth is full.
    QueueDepth,
    /// Independent retained-byte budget is exhausted.
    RetainedBytes,
}

impl CapacityRejectionStage {
    /// Every rejection stage in stable metric order.
    pub const ALL: [Self; 2] = [Self::QueueDepth, Self::RetainedBytes];
}

/// Closed stages of the end-to-end symbolic read pipeline.
///
/// Stage identities are redaction-safe metric labels only. They never carry
/// application values, plan hashes, or request parameters.
///
/// Transport residual stages (`TransportAdapt`, `Authn`, `AdmissionContext`,
/// `SpawnDispatch`, `EncodeConvert`) decompose the client-visible gap outside
/// the original seven service-side stages. Codec internals stay uninstrumented.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReadPipelineStage {
    /// gRPC request split and protobuf → domain conversion.
    TransportAdapt,
    /// Credential authentication for a normal request.
    Authn,
    /// Lifecycle admission and request-context assembly excluding authentication.
    AdmissionContext,
    /// Service spawn submission until the job body first runs.
    SpawnDispatch,
    /// Named-query contract selection and module/query plan lookup.
    PlanLookup,
    /// Parameter materialization and cursor-lookup identity construction.
    ParamMaterialize,
    /// Audit begin for the symbolic query invocation.
    AuthorizeBegin,
    /// Pre-execution current-policy reauthorization.
    AuthorizePre,
    /// Authorized page execution (fence + snapshot + execute).
    Execute,
    /// Post-execution current-policy reauthorization.
    AuthorizePost,
    /// Snapshot-to-response projection assembly.
    ResponseBuild,
    /// Domain result → protobuf response conversion.
    EncodeConvert,
}

impl ReadPipelineStage {
    /// Every read-pipeline stage in stable metric and shutdown-line order.
    pub const ALL: [Self; 12] = [
        Self::TransportAdapt,
        Self::Authn,
        Self::AdmissionContext,
        Self::SpawnDispatch,
        Self::PlanLookup,
        Self::ParamMaterialize,
        Self::AuthorizeBegin,
        Self::AuthorizePre,
        Self::Execute,
        Self::AuthorizePost,
        Self::ResponseBuild,
        Self::EncodeConvert,
    ];

    /// Stable snake_case label value for the `{stage}` metric dimension.
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::TransportAdapt => "transport_adapt",
            Self::Authn => "authn",
            Self::AdmissionContext => "admission_context",
            Self::SpawnDispatch => "spawn_dispatch",
            Self::PlanLookup => "plan_lookup",
            Self::ParamMaterialize => "param_materialize",
            Self::AuthorizeBegin => "authorize_begin",
            Self::AuthorizePre => "authorize_pre",
            Self::Execute => "execute",
            Self::AuthorizePost => "authorize_post",
            Self::ResponseBuild => "response_build",
            Self::EncodeConvert => "encode_convert",
        }
    }
}

/// Redaction-safe service orchestration telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceTelemetryEvent {
    /// One API-neutral operation reached its final caller-visible disposition.
    OperationTerminal {
        /// Closed service operation.
        operation: riffdb_types::ServiceOperationV1,
        /// Trusted transport classification fixed by the request context.
        ingress: riffdb_types::ServiceIngressKindV1,
        /// Closed caller-visible terminal class.
        terminal: ServiceTerminalClass,
        /// Process-local elapsed time for the complete contained operation.
        elapsed: std::time::Duration,
    },
    /// A required service-audit operation was unavailable or uncertain.
    AuditUnavailable {
        /// Closed operation whose audit lifecycle failed.
        operation: riffdb_types::ServiceOperationV1,
    },
    /// A lower integrity or proof join failed without caller-controlled detail.
    InternalIntegrity {
        /// Closed operation being processed.
        operation: riffdb_types::ServiceOperationV1,
    },
    /// A cursor source, registry, or monotonic-clock operation failed closed.
    CursorUnavailable,
    /// A live cursor was evicted to make room for a newer registration.
    CursorEvicted,
    /// One internal read attempt failed with a closed transient and will retry.
    ReadRetryAttempt {
        /// Stable operation name for telemetry only.
        operation: riffdb_types::ServiceOperationV1,
        /// 1-based attempt index within the closed retry budget.
        attempt: u32,
    },
    /// The closed internal read-retry budget was exhausted.
    ReadRetryExhausted {
        /// Stable operation name for telemetry only.
        operation: riffdb_types::ServiceOperationV1,
    },
    /// A post-establishment stream was closed at a current-policy safe point.
    StreamClosedByPolicy,
    /// Command capacity admission rejected the request before accept.
    CapacityRejected {
        /// Closed service operation.
        operation: riffdb_types::ServiceOperationV1,
        /// Trusted transport classification fixed by the request context.
        ingress: riffdb_types::ServiceIngressKindV1,
        /// Closed capacity stage that rejected the request.
        stage: CapacityRejectionStage,
    },
    /// One bounded service-side read pipeline stage completed.
    ReadPipelineStageCompleted {
        /// Closed stage identity; no application values are retained.
        stage: ReadPipelineStage,
        /// Wall duration of this service stage.
        elapsed: std::time::Duration,
    },
}

/// Closed terminal classes for API-neutral service telemetry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ServiceTerminalClass {
    /// A complete result was released.
    Succeeded,
    /// Bounded request validation failed.
    Validation,
    /// An idempotency identity was reused with different input.
    IdempotencyMismatch,
    /// Current authorization denied the operation.
    AuthorizationDenied,
    /// Conflict acquisition or the retry budget reached its deadline.
    ConcurrencyDeadlineExceeded,
    /// The requested contract or plan was not current.
    ContractMismatch,
    /// A required authoritative dependency was unavailable.
    StorageUnavailable,
    /// Authoritative completion could not yet be determined.
    OutcomeUnknown,
    /// A checked internal invariant failed.
    InternalDefect,
    /// Deterministic command evaluation reached a declared failure.
    CommandExecutionFailed,
    /// Cancellation was proven at a safe point.
    Cancelled,
    /// The request deadline elapsed at a safe point.
    DeadlineExceeded,
    /// The complete response exceeded the service ceiling.
    ResponseTooLarge,
    /// Internal containment could not obtain an incident identity.
    EmergencyInternal,
    /// Observed history predates a database restore.
    HistoryIncarnationMismatch,
    /// Requested history was retired by retention prune.
    HistoryPruned,
    /// Admission rejected the request because the service is over capacity.
    Overloaded,
}

impl ServiceTerminalClass {
    /// Every terminal class in stable metric order.
    pub const ALL: [Self; 17] = [
        Self::Succeeded,
        Self::Validation,
        Self::IdempotencyMismatch,
        Self::AuthorizationDenied,
        Self::ConcurrencyDeadlineExceeded,
        Self::ContractMismatch,
        Self::StorageUnavailable,
        Self::OutcomeUnknown,
        Self::InternalDefect,
        Self::CommandExecutionFailed,
        Self::Cancelled,
        Self::DeadlineExceeded,
        Self::ResponseTooLarge,
        Self::EmergencyInternal,
        Self::HistoryIncarnationMismatch,
        Self::HistoryPruned,
        Self::Overloaded,
    ];
}

/// Trusted sink that receives only closed, payload-free service events.
pub trait ServiceTelemetry: Send + Sync {
    /// Records one bounded event without request values, credentials, or diagnostics.
    fn record(&self, event: ServiceTelemetryEvent);
}

/// Trusted diagnostic sink for owned internal sources correlated by incident ID.
///
/// This boundary is disjoint from public telemetry and transport output. Its
/// implementation must apply redaction before any external subscriber sees an
/// error source.
pub trait ServiceDiagnostics: Send + Sync {
    /// Retains one internal source under its already assigned incident identity.
    fn record_internal(&self, error: InternalError);
}

/// No-op telemetry for compositions that do not install an observer.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopServiceTelemetry;

impl ServiceTelemetry for NoopServiceTelemetry {
    fn record(&self, _event: ServiceTelemetryEvent) {}
}

/// Closed reasons the service must fail authoritative readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoritativeReadinessFailure {
    /// A mandatory audit clock or append could not be established safely.
    AuditUnavailable,
    /// Unknown authoritative write status fenced the coordinator.
    CoordinatorFenced,
    /// A checked semantic join or authoritative observation failed integrity.
    Integrity,
}

/// Lifecycle hook for fail-closed authoritative readiness transitions.
pub trait ServiceHealthHooks: Send + Sync {
    /// Records a monotonic readiness failure for the current process lifecycle.
    fn fail_authoritative_readiness(&self, reason: AuthoritativeReadinessFailure);
}

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
