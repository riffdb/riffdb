//! Least-authority consumer ports used by service orchestration.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use riffdb_auth::{AuthenticatedPrincipal, CurrentCapabilityResolver, NewlyIssuedCapabilityToken};
use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogError, CatalogPreparationResult, ResolvedExecutablePlan,
    ValidatedContractBundle,
};
use riffdb_contract_ir::ContractBundle;
use riffdb_errors::InternalError;
use riffdb_policy::{
    AuthorizationClock, AuthorizationError, AuthorizationTelemetry, CurrentAuthorizer, Decision,
    OperationRequest, ProvenanceSelector,
};
use riffdb_types::{
    CapabilityId, CommandId, ContractBundleHash, ContractLineage, ContractVersion,
    FrontierPosition, PlanHash,
};

use crate::{
    AuthoritativeCommitPage, AuthoritativeCommitScanRequest, AuthoritativeCommitSnapshot,
    AuthoritativeCommitSubscriptionRequest, AuthoritativeEntityRequest,
    AuthoritativeEntitySnapshot, AuthoritativeIndexPage, AuthoritativeIndexRequest,
    AuthoritativeOutcomeRequest, AuthoritativeOutcomeSnapshot, AuthoritativeProvenanceSnapshot,
    CapabilityRevokeTargetSnapshot, OperationalHealthSnapshot, OperationalStatisticsSnapshot,
    OutboxStatusRequest, OutboxStatusSnapshot, ProjectionPortRequest, ProjectionPortResult,
    ProjectionStatusSnapshot, RequestControl,
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
    fn prepare_contract_version(
        &self,
        control: &RequestControl,
        lineage: ContractLineage,
        version: ContractVersion,
    ) -> PortFuture<'_, Option<ValidatedContractBundle>, CatalogError>;

    /// Reserves cancellation-aware capacity for an active-catalog observation.
    fn reserve_active_catalog(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<(), Option<ActiveCatalogSnapshot>, CatalogError>,
        PortAdmissionError,
    >;

    /// Reserves cancellation-aware capacity for one historical-bundle observation.
    fn reserve_contract_version(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ContractVersionReadPermit, PortAdmissionError>;

    /// Resolves preparatory plan facts with bounded cancellation and deadline handling.
    ///
    /// This read returns no caller output and may run while a command-executor
    /// permit is held. The service performs a fresh policy check only after it
    /// obtains the resulting exact facts.
    fn executable_plan(
        &self,
        control: &RequestControl,
        request: CatalogExecutablePlanRequest,
    ) -> PortFuture<'_, ResolvedExecutablePlan, CatalogError>;

    /// Performs bounded preparatory catalog validation before coordinator admission.
    fn prepare_deployment(
        &self,
        control: &RequestControl,
        candidate: ContractBundle,
        expected_active_version: Option<ContractVersion>,
    ) -> PortFuture<'_, CatalogPreparationResult, CatalogError>;
}

/// Closed authoritative-read failure with no storage diagnostic or handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoritativeReadError {
    /// A bounded authoritative read could not complete.
    Unavailable,
    /// Authoritative state failed semantic or reciprocal integrity.
    Integrity,
    /// A lower continuation or frozen fence no longer denotes a valid page.
    InvalidContinuation,
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
}

/// Authoritative entity, outcome, commit, provenance, and capability observations.
pub trait AuthoritativeReadPort: Send + Sync {
    /// Reserves capacity for one exact authoritative entity observation.
    fn reserve_read_entity(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeEntityRequest,
            Option<AuthoritativeEntitySnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity for one bounded, fenced authoritative index page.
    fn reserve_scan_index(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeIndexRequest,
            AuthoritativeIndexPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity for one exact durable command outcome observation.
    fn reserve_read_outcome(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeOutcomeRequest,
            Option<AuthoritativeOutcomeSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity for one exact application commit observation.
    fn reserve_read_commit(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            riffdb_types::CommitSequence,
            Option<AuthoritativeCommitSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity for one bounded upper-fenced commit page.
    fn reserve_scan_commits(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeCommitScanRequest,
            AuthoritativeCommitPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity to establish one bounded lower notification source.
    fn reserve_subscribe_to_commits(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeCommitSubscriptionRequest,
            Box<dyn CommitNotificationSource>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    >;

    /// Reserves capacity for one provenance trace root and bounded graph observation.
    fn reserve_trace_provenance(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
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
    fn read_capability_revoke_target(
        &self,
        control: &RequestControl,
        capability_id: CapabilityId,
    ) -> PortFuture<'_, CapabilityRevokeTargetSnapshot, AuthoritativeReadError>;
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
    fn reserve_query_projection(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<ProjectionPortRequest, ProjectionPortResult, ProjectionPortError>,
        PortAdmissionError,
    >;

    /// Reserves capacity for one exact projection lifecycle snapshot.
    fn reserve_projection_status(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            riffdb_types::ProjectionIdentity,
            Option<ProjectionStatusSnapshot>,
            ProjectionPortError,
        >,
        PortAdmissionError,
    >;
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
    fn reserve_pending_status(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<OutboxStatusRequest, OutboxStatusSnapshot, OutboxStatusPortError>,
        PortAdmissionError,
    >;
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

/// Redaction-safe service orchestration telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceTelemetryEvent {
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
    /// A post-establishment stream was closed at a current-policy safe point.
    StreamClosedByPolicy,
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
