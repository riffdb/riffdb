//! Checked request contexts shared by every transport adapter.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use riffdb_auth::{AuthenticatedPrincipal, BootstrapDigestCandidates};
use riffdb_commit::{CommandCancellationHandle, CommandRequestControl};
use riffdb_policy::UntrustedInvocationClaims;
use riffdb_types::{RequestId, ServiceIngressKindV1};

/// Maximum trusted trace-propagation bytes retained for telemetry.
pub const MAX_TRACE_CONTEXT_BYTES: usize = 512;
/// Maximum command controls created for one bounded prepare/reprepare invocation.
pub const MAX_COMMAND_PREPARATION_ATTEMPTS: usize = 3;

/// Bounded trusted trace propagation that is never persisted or serialized.
#[derive(Clone, Eq, PartialEq)]
pub struct TraceContext(Vec<u8>);

impl TraceContext {
    /// Validates a nonempty bounded trace context.
    pub fn new(bytes: Vec<u8>) -> Result<Self, TraceContextError> {
        if bytes.is_empty() || bytes.len() > MAX_TRACE_CONTEXT_BYTES {
            return Err(TraceContextError);
        }
        Ok(Self(bytes))
    }

    /// Borrows the exact trusted propagation bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for TraceContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TraceContext([REDACTED])")
    }
}

/// Safe failure to construct bounded trace propagation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraceContextError;

impl fmt::Display for TraceContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("trace context is empty or exceeds its byte limit")
    }
}

impl std::error::Error for TraceContextError {}

struct RequestCancellationState {
    cancelled: AtomicBool,
    waiter: Mutex<Option<Waker>>,
    command_handles: Mutex<Vec<CommandCancellationHandle>>,
}

/// Cloneable authority to request cancellation of one service invocation.
#[derive(Clone)]
pub struct RequestCancellationHandle(Arc<RequestCancellationState>);

impl RequestCancellationHandle {
    /// Requests monotonic cancellation and propagates it to every admitted command attempt.
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        let waiter = self
            .0
            .waiter
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        let handles = self
            .0
            .command_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for handle in handles.iter() {
            handle.cancel();
        }
    }
}

impl fmt::Debug for RequestCancellationHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RequestCancellationHandle([REDACTED])")
    }
}

/// Process-local deadline and cancellation state for one service invocation.
///
/// The value is deliberately nonserializable and non-`Clone`. It can create at
/// most the fixed command-preparation attempt count of lower cancellation
/// controls; every handle remains connected to the request's monotonic signal.
pub struct RequestControl {
    deadline: Instant,
    cancellation: Arc<RequestCancellationState>,
}

impl RequestControl {
    /// Creates one request control and its externally retained cancellation handle.
    #[must_use]
    pub fn new(deadline: Instant) -> (Self, RequestCancellationHandle) {
        let cancellation = Arc::new(RequestCancellationState {
            cancelled: AtomicBool::new(false),
            waiter: Mutex::new(None),
            command_handles: Mutex::new(Vec::new()),
        });
        (
            Self {
                deadline,
                cancellation: Arc::clone(&cancellation),
            },
            RequestCancellationHandle(cancellation),
        )
    }

    /// Returns the exact absolute process-local request deadline.
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Reports whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.cancelled.load(Ordering::Acquire)
    }

    /// Reports whether the absolute process-local deadline has elapsed.
    #[must_use]
    pub fn is_deadline_exceeded(&self) -> bool {
        Instant::now() >= self.deadline
    }

    /// Returns the single cancellation wait used by sequential service admission.
    ///
    /// Lower adapters must also arrange a deadline wake using their scheduler
    /// and this request's absolute deadline.
    #[must_use]
    pub fn cancelled(&self) -> RequestCancellationFuture<'_> {
        RequestCancellationFuture {
            state: &self.cancellation,
        }
    }

    pub(crate) fn command_control(&self) -> Result<CommandRequestControl, RequestControlError> {
        let (control, handle) = CommandRequestControl::new(self.deadline);
        let mut handles = self
            .cancellation
            .command_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if handles.len() >= MAX_COMMAND_PREPARATION_ATTEMPTS {
            return Err(RequestControlError::CommandAttemptLimit);
        }
        handles.push(handle.clone());
        if self.cancellation.cancelled.load(Ordering::Acquire) {
            handle.cancel();
        }
        Ok(control)
    }

    fn fork(&self) -> Self {
        Self {
            deadline: self.deadline,
            cancellation: Arc::clone(&self.cancellation),
        }
    }
}

impl fmt::Debug for RequestControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RequestControl([REDACTED])")
    }
}

/// Borrowed cancellation notification for one sequential service wait.
pub struct RequestCancellationFuture<'a> {
    state: &'a Arc<RequestCancellationState>,
}

impl Future for RequestCancellationFuture<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.cancelled.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        let mut waiter = self
            .state
            .waiter
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.state.cancelled.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        if waiter
            .as_ref()
            .is_none_or(|registered| !registered.will_wake(context.waker()))
        {
            *waiter = Some(context.waker().clone());
        }
        Poll::Pending
    }
}

impl fmt::Debug for RequestCancellationFuture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RequestCancellationFuture([REDACTED])")
    }
}

/// Closed internal failure while deriving lower request control.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestControlError {
    /// The fixed prepare/reprepare attempt count has already been consumed.
    CommandAttemptLimit,
}

/// One authenticated, transport-neutral service invocation context.
pub struct RequestContext {
    request_id: RequestId,
    principal: AuthenticatedPrincipal,
    ingress: ServiceIngressKindV1,
    claims: UntrustedInvocationClaims,
    control: RequestControl,
    trace: Option<TraceContext>,
}

impl RequestContext {
    /// Joins checked adapter output without treating authentication as authorization.
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        principal: AuthenticatedPrincipal,
        ingress: ServiceIngressKindV1,
        claims: UntrustedInvocationClaims,
        control: RequestControl,
        trace: Option<TraceContext>,
    ) -> Self {
        Self {
            request_id,
            principal,
            ingress,
            claims,
            control,
            trace,
        }
    }

    /// Constructs an ordinary authenticated gRPC invocation with the POC's
    /// empty caller-claim set.
    ///
    /// Initial authentication does not authorize the request; every operation
    /// still passes through the shared current-policy safe points.
    #[must_use]
    pub const fn from_authenticated_grpc(
        request_id: RequestId,
        principal: AuthenticatedPrincipal,
        control: RequestControl,
        trace: Option<TraceContext>,
    ) -> Self {
        Self::new(
            request_id,
            principal,
            ServiceIngressKindV1::Grpc,
            UntrustedInvocationClaims::new(None, None, None, None, None),
            control,
            trace,
        )
    }

    /// Constructs an ordinary authenticated hosted MCP HTTP invocation with
    /// the POC's empty caller-claim set.
    ///
    /// Initial authentication does not authorize the request; every operation
    /// still passes through the shared current-policy safe points.
    #[must_use]
    pub const fn from_authenticated_mcp_http(
        request_id: RequestId,
        principal: AuthenticatedPrincipal,
        control: RequestControl,
        trace: Option<TraceContext>,
    ) -> Self {
        Self::new(
            request_id,
            principal,
            ServiceIngressKindV1::McpHttp,
            UntrustedInvocationClaims::new(None, None, None, None, None),
            control,
            trace,
        )
    }

    /// Returns the transport-submission identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Borrows the authenticated principal; current policy must still be checked.
    #[must_use]
    pub const fn principal(&self) -> &AuthenticatedPrincipal {
        &self.principal
    }

    /// Returns the trusted ingress classification.
    #[must_use]
    pub const fn ingress(&self) -> ServiceIngressKindV1 {
        self.ingress
    }

    /// Borrows bounded but untrusted invocation claims.
    #[must_use]
    pub const fn claims(&self) -> &UntrustedInvocationClaims {
        &self.claims
    }

    /// Borrows process-local request control.
    #[must_use]
    pub const fn control(&self) -> &RequestControl {
        &self.control
    }

    /// Borrows trusted trace propagation, when supplied.
    #[must_use]
    pub const fn trace(&self) -> Option<&TraceContext> {
        self.trace.as_ref()
    }

    pub(crate) fn child(&self, request_id: RequestId) -> Self {
        Self {
            request_id,
            principal: self.principal.clone(),
            ingress: self.ingress,
            claims: self.claims.clone(),
            control: self.control.fork(),
            trace: self.trace.clone(),
        }
    }
}

impl fmt::Debug for RequestContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RequestContext([REDACTED])")
    }
}

/// Principal-less checked context for the one loopback-gRPC bootstrap operation.
pub struct BootstrapRequestContext {
    request_id: RequestId,
    ingress: ServiceIngressKindV1,
    control: RequestControl,
    digests: BootstrapDigestCandidates,
}

impl BootstrapRequestContext {
    /// Constructs the production bootstrap context with gRPC ingress fixed by type.
    #[must_use]
    pub fn from_loopback_grpc(
        request_id: RequestId,
        control: RequestControl,
        digests: BootstrapDigestCandidates,
    ) -> Self {
        Self {
            request_id,
            ingress: ServiceIngressKindV1::Grpc,
            control,
            digests,
        }
    }

    /// Returns the transport-submission identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the fixed trusted bootstrap ingress.
    #[must_use]
    pub const fn ingress(&self) -> ServiceIngressKindV1 {
        self.ingress
    }

    /// Borrows process-local request control.
    #[must_use]
    pub const fn control(&self) -> &RequestControl {
        &self.control
    }

    /// Borrows checked digest candidates without exposing the raw credential.
    #[must_use]
    pub const fn digests(&self) -> &BootstrapDigestCandidates {
        &self.digests
    }
}

impl fmt::Debug for BootstrapRequestContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BootstrapRequestContext([REDACTED])")
    }
}

/// Closed lifecycle phases in which restricted principal-less health is legal.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PreBootstrapLifecycle {
    /// Durable identity and catalog/storage integrity validation is still running.
    InitializingValidation,
    /// Integrity validation passed and one-time capability bootstrap is pending.
    InitializingBootstrap,
}

/// Opaque authority for the restricted pre-bootstrap branch of Health.
///
/// The context contains no principal, policy proof, storage handle, audit
/// authority, or general service capability. Only a service-owned issuer can
/// construct it.
pub struct PreBootstrapHealthContext {
    lifecycle: PreBootstrapLifecycle,
    admission: Arc<PreBootstrapHealthAdmission>,
}

impl PreBootstrapHealthContext {
    /// Returns the exact pre-bootstrap lifecycle selected by the server router.
    #[must_use]
    pub const fn lifecycle(&self) -> PreBootstrapLifecycle {
        self.lifecycle
    }

    pub(crate) fn is_admitted_by(&self, admission: &Arc<PreBootstrapHealthAdmission>) -> bool {
        Arc::ptr_eq(&self.admission, admission) && admission.is_open()
    }
}

impl fmt::Debug for PreBootstrapHealthContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreBootstrapHealthContext([REDACTED])")
    }
}

/// Move-only capability issued with the application-service composition.
///
/// Its constructor is crate-private. WP-130 receives the one instance from the
/// composed service and retains it only in its pre-marker lifecycle router.
pub struct PreBootstrapHealthContextIssuer {
    admission: Arc<PreBootstrapHealthAdmission>,
}

impl PreBootstrapHealthContextIssuer {
    pub(crate) fn new(admission: Arc<PreBootstrapHealthAdmission>) -> Self {
        Self { admission }
    }

    /// Issues one restricted health context for the caller-selected closed phase.
    #[must_use]
    pub fn issue(&self, lifecycle: PreBootstrapLifecycle) -> Option<PreBootstrapHealthContext> {
        self.admission.is_open().then(|| PreBootstrapHealthContext {
            lifecycle,
            admission: Arc::clone(&self.admission),
        })
    }

    /// Atomically closes principal-less Health admission for this service.
    pub fn close(&self) {
        self.admission.close();
    }
}

impl Drop for PreBootstrapHealthContextIssuer {
    fn drop(&mut self) {
        self.close();
    }
}

impl fmt::Debug for PreBootstrapHealthContextIssuer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreBootstrapHealthContextIssuer([REDACTED])")
    }
}

pub(crate) struct PreBootstrapHealthAdmission {
    open: AtomicBool,
}

impl PreBootstrapHealthAdmission {
    pub(crate) fn open() -> Self {
        Self {
            open: AtomicBool::new(true),
        }
    }

    pub(crate) fn closed() -> Self {
        Self {
            open: AtomicBool::new(false),
        }
    }

    fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    fn close(&self) {
        self.open.store(false, Ordering::Release);
    }
}

impl fmt::Debug for PreBootstrapHealthAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreBootstrapHealthAdmission([REDACTED])")
    }
}

/// The one closed context accepted by `AdministrationApplication::health`.
pub enum HealthContext {
    /// Ordinary authenticated current-policy health.
    Authenticated(Box<RequestContext>),
    /// Restricted principal-less health before the durable bootstrap marker.
    PreBootstrap(PreBootstrapHealthContext),
}

impl HealthContext {
    /// Wraps one ordinary authenticated request context.
    #[must_use]
    pub fn authenticated(context: RequestContext) -> Self {
        Self::Authenticated(Box::new(context))
    }

    /// Wraps one service-issued restricted pre-bootstrap context.
    #[must_use]
    pub const fn pre_bootstrap(context: PreBootstrapHealthContext) -> Self {
        Self::PreBootstrap(context)
    }
}

impl fmt::Debug for HealthContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Authenticated(_) => "HealthContext::Authenticated([REDACTED])",
            Self::PreBootstrap(_) => "HealthContext::PreBootstrap([REDACTED])",
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::task::{Wake, Waker};
    use std::time::{Duration, Instant};

    use super::*;

    struct CountingWake(AtomicUsize);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn cancellation_reaches_every_bounded_command_attempt() {
        let (control, cancellation) = RequestControl::new(Instant::now() + Duration::from_secs(1));
        for _ in 0..MAX_COMMAND_PREPARATION_ATTEMPTS {
            control.command_control().expect("bounded command control");
        }
        assert_eq!(
            control.command_control().unwrap_err(),
            RequestControlError::CommandAttemptLimit
        );

        cancellation.cancel();
        assert!(control.is_cancelled());
    }

    #[test]
    fn forked_control_preserves_deadline_and_shares_cancellation() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let (control, cancellation) = RequestControl::new(deadline);
        let fork = control.fork();

        assert_eq!(fork.deadline(), deadline);
        cancellation.cancel();
        assert!(control.is_cancelled());
        assert!(fork.is_cancelled());
    }

    #[test]
    fn cancellation_wakes_the_registered_capacity_wait_without_sleeping() {
        let (control, cancellation) = RequestControl::new(Instant::now() + Duration::from_secs(1));
        let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let mut wait = std::pin::pin!(control.cancelled());

        assert!(matches!(wait.as_mut().poll(&mut context), Poll::Pending));
        cancellation.cancel();
        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        assert!(matches!(wait.as_mut().poll(&mut context), Poll::Ready(())));
    }

    #[test]
    fn prebootstrap_issuer_creates_only_closed_health_phases() {
        let admission = Arc::new(PreBootstrapHealthAdmission::open());
        let issuer = PreBootstrapHealthContextIssuer::new(Arc::clone(&admission));
        let context = issuer
            .issue(PreBootstrapLifecycle::InitializingBootstrap)
            .expect("open issuer");
        assert_eq!(
            context.lifecycle(),
            PreBootstrapLifecycle::InitializingBootstrap
        );
        assert!(context.is_admitted_by(&admission));

        issuer.close();
        assert!(
            issuer
                .issue(PreBootstrapLifecycle::InitializingValidation)
                .is_none()
        );
        assert!(!context.is_admitted_by(&admission));
    }

    #[test]
    fn prebootstrap_context_is_bound_to_one_admission_gate() {
        let admission = Arc::new(PreBootstrapHealthAdmission::open());
        let other = Arc::new(PreBootstrapHealthAdmission::open());
        let issuer = PreBootstrapHealthContextIssuer::new(Arc::clone(&admission));
        let context = issuer
            .issue(PreBootstrapLifecycle::InitializingValidation)
            .expect("open issuer");

        assert!(context.is_admitted_by(&admission));
        assert!(!context.is_admitted_by(&other));
    }

    #[test]
    fn dropping_issuer_revokes_every_context_it_already_issued() {
        let admission = Arc::new(PreBootstrapHealthAdmission::open());
        let context = {
            let issuer = PreBootstrapHealthContextIssuer::new(Arc::clone(&admission));
            issuer
                .issue(PreBootstrapLifecycle::InitializingValidation)
                .expect("open issuer")
        };

        assert!(!context.is_admitted_by(&admission));
    }
}
