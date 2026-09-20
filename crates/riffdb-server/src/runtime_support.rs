#![expect(
    clippy::panic,
    reason = "exhausting the bounded service-job supervision identity is an internal process invariant breach"
)]

//! Process-runtime adapters used by the production application service.

// These providers are consumed by the WP-130 production graph assembled in this crate.
#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use riffdb_errors::{DefectScope, InternalError};
use riffdb_observability::Observability;
use riffdb_service::{
    AuthoritativeReadinessFailure, RequestDeadlineFuture, RequestDeadlineScheduler,
    ServiceDiagnostics, ServiceHealthHooks, ServiceJob, ServiceJobSpawner, ServiceTelemetry,
    ServiceTelemetryEvent,
};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

/// Maximum trusted internal sources retained before diagnostics fail readiness.
pub(crate) const MAX_RETAINED_INTERNAL_DIAGNOSTICS: usize = 256;
/// Process-scoped defects the breaker tolerates back to back (ADR-0250 decision 4).
pub(crate) const DEFECT_BURST_CAPACITY: u32 = 16;
/// How often one unit of that burst comes back.
pub(crate) const DEFECT_REFILL_INTERVAL: Duration = Duration::from_secs(60);

const ROUTING_RUNNING: u8 = 0;

/// First fail-closed reason that stopped new routing through one service graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum RuntimeStopReason {
    AuditUnavailable = 1,
    CoordinatorFenced = 2,
    Integrity = 3,
    AcceptedServiceJobPanicked = 4,
    AcceptedServiceJobDisappeared = 5,
    SupervisionStateCorrupted = 6,
    DiagnosticCapacityExceeded = 7,
}

impl RuntimeStopReason {
    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::AuditUnavailable),
            2 => Some(Self::CoordinatorFenced),
            3 => Some(Self::Integrity),
            4 => Some(Self::AcceptedServiceJobPanicked),
            5 => Some(Self::AcceptedServiceJobDisappeared),
            6 => Some(Self::SupervisionStateCorrupted),
            7 => Some(Self::DiagnosticCapacityExceeded),
            _ => None,
        }
    }
}

struct RuntimeRoutingInner {
    reason: AtomicU8,
    stopped: watch::Sender<u8>,
}

/// Cloneable monotonic signal shared by routing, supervision, and health hooks.
#[derive(Clone)]
pub(crate) struct RuntimeRoutingState {
    inner: Arc<RuntimeRoutingInner>,
}

impl RuntimeRoutingState {
    pub(crate) fn new() -> Self {
        let (stopped, _unused_receiver) = watch::channel(ROUTING_RUNNING);
        Self {
            inner: Arc::new(RuntimeRoutingInner {
                reason: AtomicU8::new(ROUTING_RUNNING),
                stopped,
            }),
        }
    }

    /// Reports whether a transport may begin routing a new invocation.
    pub(crate) fn is_routing_allowed(&self) -> bool {
        self.inner.reason.load(Ordering::Acquire) == ROUTING_RUNNING
    }

    /// Returns the first failure that stopped routing, if any.
    pub(crate) fn stop_reason(&self) -> Option<RuntimeStopReason> {
        let code = self.inner.reason.load(Ordering::Acquire);
        if code == ROUTING_RUNNING {
            None
        } else {
            RuntimeStopReason::from_code(code)
                .or(Some(RuntimeStopReason::SupervisionStateCorrupted))
        }
    }

    /// Waits for the monotonic transition away from routable state.
    pub(crate) async fn stopped(&self) -> RuntimeStopReason {
        let mut receiver = self.inner.stopped.subscribe();
        loop {
            if let Some(reason) = self.stop_reason() {
                return reason;
            }
            if receiver.changed().await.is_err() {
                return RuntimeStopReason::SupervisionStateCorrupted;
            }
        }
    }

    fn stop(&self, reason: RuntimeStopReason) {
        if self
            .inner
            .reason
            .compare_exchange(
                ROUTING_RUNNING,
                reason as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.inner.stopped.send_replace(reason as u8);
        }
    }
}

impl Default for RuntimeRoutingState {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceHealthHooks for RuntimeRoutingState {
    fn fail_authoritative_readiness(&self, reason: AuthoritativeReadinessFailure) {
        self.stop(match reason {
            AuthoritativeReadinessFailure::AuditUnavailable => RuntimeStopReason::AuditUnavailable,
            AuthoritativeReadinessFailure::CoordinatorFenced => {
                RuntimeStopReason::CoordinatorFenced
            }
            AuthoritativeReadinessFailure::Integrity => RuntimeStopReason::Integrity,
        });
    }
}

impl fmt::Debug for RuntimeRoutingState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RuntimeRoutingState([REDACTED])")
    }
}

struct ServiceJobRegistry {
    handles: BTreeMap<u64, Option<JoinHandle<()>>>,
}

struct ServiceJobSupervisionInner {
    runtime: Handle,
    routing: RuntimeRoutingState,
    next_job_id: AtomicU64,
    registry: Mutex<ServiceJobRegistry>,
    active_count: watch::Sender<usize>,
}

impl ServiceJobSupervisionInner {
    fn lock_registry(&self) -> MutexGuard<'_, ServiceJobRegistry> {
        match self.registry.lock() {
            Ok(registry) => registry,
            Err(poisoned) => {
                self.routing
                    .stop(RuntimeStopReason::SupervisionStateCorrupted);
                poisoned.into_inner()
            }
        }
    }

    fn reserve_job(&self) -> u64 {
        let mut registry = self.lock_registry();
        let job_id =
            match self
                .next_job_id
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current.checked_add(1)
                }) {
                Ok(job_id) => job_id,
                Err(_) => {
                    drop(registry);
                    self.routing
                        .stop(RuntimeStopReason::SupervisionStateCorrupted);
                    panic!("service-job supervision identity exhausted");
                }
            };

        let replaced = registry.handles.insert(job_id, None);
        debug_assert!(replaced.is_none());
        let active = registry.handles.len();
        drop(registry);
        self.active_count.send_replace(active);
        job_id
    }

    fn install_handle(&self, job_id: u64, handle: JoinHandle<()>) -> bool {
        let mut registry = self.lock_registry();
        let Some(slot) = registry.handles.get_mut(&job_id) else {
            return false;
        };
        if slot.is_some() {
            self.routing
                .stop(RuntimeStopReason::SupervisionStateCorrupted);
            return false;
        }
        *slot = Some(handle);
        true
    }

    fn finish_job(&self, job_id: u64) -> bool {
        let mut registry = self.lock_registry();
        let removed = registry.handles.remove(&job_id).is_some();
        let active = registry.handles.len();
        drop(registry);
        self.active_count.send_replace(active);
        removed
    }

    fn active_jobs(&self) -> usize {
        self.lock_registry().handles.len()
    }
}

struct ActiveServiceJob {
    supervision: Arc<ServiceJobSupervisionInner>,
    job_id: u64,
    finished: bool,
}

impl ActiveServiceJob {
    fn new(supervision: Arc<ServiceJobSupervisionInner>, job_id: u64) -> Self {
        Self {
            supervision,
            job_id,
            finished: false,
        }
    }

    fn complete(mut self) {
        self.finished = true;
        if !self.supervision.finish_job(self.job_id) {
            self.supervision
                .routing
                .stop(RuntimeStopReason::SupervisionStateCorrupted);
        }
    }

    fn panicked(self) {
        self.supervision
            .routing
            .stop(RuntimeStopReason::AcceptedServiceJobPanicked);
        self.complete();
    }
}

impl Drop for ActiveServiceJob {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.supervision
            .routing
            .stop(RuntimeStopReason::AcceptedServiceJobDisappeared);
        let _removed = self.supervision.finish_job(self.job_id);
    }
}

struct CatchServiceJobPanic {
    job: ServiceJob,
}

impl Future for CatchServiceJobPanic {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let observed = catch_unwind(AssertUnwindSafe(|| self.job.as_mut().poll(context)));
        match observed {
            Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}

/// Tokio-backed owner of every independently driven application-service job.
///
/// The retained registry is bounded by the trusted transport admission permit,
/// which must remain held until the service future completes. This owner adds
/// no guessed second ceiling: after service admission it must retain and drive
/// every job it receives.
#[derive(Clone)]
pub(crate) struct SupervisedServiceJobSpawner {
    inner: Arc<ServiceJobSupervisionInner>,
}

impl SupervisedServiceJobSpawner {
    pub(crate) fn with_runtime(runtime: Handle, routing: RuntimeRoutingState) -> Self {
        let (active_count, _unused_receiver) = watch::channel(0);
        Self {
            inner: Arc::new(ServiceJobSupervisionInner {
                runtime,
                routing,
                next_job_id: AtomicU64::new(1),
                registry: Mutex::new(ServiceJobRegistry {
                    handles: BTreeMap::new(),
                }),
                active_count,
            }),
        }
    }

    pub(crate) fn from_current_runtime(
        routing: RuntimeRoutingState,
    ) -> Result<Self, RuntimeSupportError> {
        Handle::try_current()
            .map(|runtime| Self::with_runtime(runtime, routing))
            .map_err(|_| RuntimeSupportError)
    }

    pub(crate) fn routing(&self) -> &RuntimeRoutingState {
        &self.inner.routing
    }

    pub(crate) fn active_jobs(&self) -> usize {
        self.inner.active_jobs()
    }

    /// Waits until every retained accepted-job handle has completed.
    ///
    /// The caller must first close transport admission if it needs quiescence
    /// rather than a point-in-time idle observation.
    pub(crate) async fn wait_for_idle(&self) {
        let mut active = self.inner.active_count.subscribe();
        loop {
            if self.inner.active_jobs() == 0 {
                return;
            }
            if active.changed().await.is_err() {
                self.inner
                    .routing
                    .stop(RuntimeStopReason::SupervisionStateCorrupted);
                return;
            }
        }
    }

    #[cfg(test)]
    fn abort_all_for_test(&self) {
        let handles: Vec<_> = self
            .inner
            .lock_registry()
            .handles
            .values()
            .filter_map(|handle| handle.as_ref().map(JoinHandle::abort_handle))
            .collect();
        for handle in handles {
            handle.abort();
        }
    }
}

impl ServiceJobSpawner for SupervisedServiceJobSpawner {
    fn spawn(&self, job: ServiceJob) {
        let job_id = self.inner.reserve_job();
        let (start, started) = oneshot::channel();
        let active = ActiveServiceJob::new(Arc::clone(&self.inner), job_id);
        let supervised = async move {
            if started.await.is_err() {
                return;
            }
            match (CatchServiceJobPanic { job }).await {
                Ok(()) => active.complete(),
                Err(()) => active.panicked(),
            }
        };
        let handle = self.inner.runtime.spawn(supervised);
        if !self.inner.install_handle(job_id, handle) {
            self.inner
                .routing
                .stop(RuntimeStopReason::AcceptedServiceJobDisappeared);
            return;
        }
        if start.send(()).is_err() {
            self.inner
                .routing
                .stop(RuntimeStopReason::AcceptedServiceJobDisappeared);
        }
    }
}

impl fmt::Debug for SupervisedServiceJobSpawner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SupervisedServiceJobSpawner([REDACTED])")
    }
}

/// Closed failure to capture the Tokio runtime used by production composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeSupportError;

impl fmt::Display for RuntimeSupportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("process runtime is unavailable")
    }
}

impl std::error::Error for RuntimeSupportError {}

/// Process-local deadline wakeups with no command-visible time authority.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TokioRequestDeadlineScheduler;

impl RequestDeadlineScheduler for TokioRequestDeadlineScheduler {
    fn wait_until(&self, deadline: Instant) -> RequestDeadlineFuture<'_> {
        Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
            deadline,
        )))
    }
}

/// Fans authoritative health failures into routing and bounded observability.
pub(crate) struct ProductionObservabilityHealthHooks {
    routing: RuntimeRoutingState,
    observability: Arc<Observability>,
}

impl ProductionObservabilityHealthHooks {
    pub(crate) fn new(routing: RuntimeRoutingState, observability: Arc<Observability>) -> Self {
        Self {
            routing,
            observability,
        }
    }
}

impl ServiceHealthHooks for ProductionObservabilityHealthHooks {
    fn fail_authoritative_readiness(&self, reason: AuthoritativeReadinessFailure) {
        ServiceHealthHooks::fail_authoritative_readiness(self.observability.as_ref(), reason);
        ServiceHealthHooks::fail_authoritative_readiness(&self.routing, reason);
    }
}

impl fmt::Debug for ProductionObservabilityHealthHooks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionObservabilityHealthHooks([REDACTED])")
    }
}

/// Sends trusted internal incidents to observability and preserves routing failure.
pub(crate) struct ProductionObservabilityDiagnostics {
    routing: RuntimeRoutingState,
    observability: Arc<Observability>,
}

impl ProductionObservabilityDiagnostics {
    pub(crate) fn new(routing: RuntimeRoutingState, observability: Arc<Observability>) -> Self {
        Self {
            routing,
            observability,
        }
    }
}

impl ServiceDiagnostics for ProductionObservabilityDiagnostics {
    fn record_internal(&self, error: InternalError) {
        ServiceDiagnostics::record_internal(self.observability.as_ref(), error);
        if !self.observability.health().snapshot().authoritative_ready() {
            self.routing.stop(RuntimeStopReason::Integrity);
        }
    }
}

impl fmt::Debug for ProductionObservabilityDiagnostics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionObservabilityDiagnostics([REDACTED])")
    }
}

/// Aggregate payload-free service telemetry retained until WP-185 observation.
#[derive(Default)]
pub(crate) struct ProductionServiceTelemetry {
    operation_terminal: AtomicU64,
    audit_unavailable: AtomicU64,
    internal_integrity: AtomicU64,
    cursor_unavailable: AtomicU64,
    cursor_evicted: AtomicU64,
    read_retry_attempt: AtomicU64,
    read_retry_exhausted: AtomicU64,
    stream_closed_by_policy: AtomicU64,
    capacity_rejected: AtomicU64,
}

impl ProductionServiceTelemetry {
    pub(crate) fn snapshot(&self) -> ServiceTelemetrySnapshot {
        ServiceTelemetrySnapshot {
            operation_terminal: self.operation_terminal.load(Ordering::Relaxed),
            audit_unavailable: self.audit_unavailable.load(Ordering::Relaxed),
            internal_integrity: self.internal_integrity.load(Ordering::Relaxed),
            cursor_unavailable: self.cursor_unavailable.load(Ordering::Relaxed),
            cursor_evicted: self.cursor_evicted.load(Ordering::Relaxed),
            read_retry_attempt: self.read_retry_attempt.load(Ordering::Relaxed),
            read_retry_exhausted: self.read_retry_exhausted.load(Ordering::Relaxed),
            stream_closed_by_policy: self.stream_closed_by_policy.load(Ordering::Relaxed),
            capacity_rejected: self.capacity_rejected.load(Ordering::Relaxed),
        }
    }
}

impl ServiceTelemetry for ProductionServiceTelemetry {
    fn record(&self, event: ServiceTelemetryEvent) {
        let counter = match event {
            ServiceTelemetryEvent::OperationTerminal { .. } => &self.operation_terminal,
            ServiceTelemetryEvent::AuditUnavailable { .. } => &self.audit_unavailable,
            ServiceTelemetryEvent::InternalIntegrity { .. } => &self.internal_integrity,
            ServiceTelemetryEvent::CursorUnavailable => &self.cursor_unavailable,
            ServiceTelemetryEvent::CursorEvicted => &self.cursor_evicted,
            ServiceTelemetryEvent::ReadRetryAttempt { .. } => &self.read_retry_attempt,
            ServiceTelemetryEvent::ReadRetryExhausted { .. } => &self.read_retry_exhausted,
            ServiceTelemetryEvent::StreamClosedByPolicy => &self.stream_closed_by_policy,
            ServiceTelemetryEvent::CapacityRejected { .. } => &self.capacity_rejected,
            // Stage histograms are retained on Observability; this aggregate
            // counter surface does not expose stage labels.
            ServiceTelemetryEvent::ReadPipelineStageCompleted { .. }
            | ServiceTelemetryEvent::WriteServiceStageCompleted { .. } => return,
        };
        saturating_increment(counter);
    }
}

impl fmt::Debug for ProductionServiceTelemetry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionServiceTelemetry([REDACTED])")
    }
}

/// Fixed-shape redaction-safe telemetry counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ServiceTelemetrySnapshot {
    pub(crate) operation_terminal: u64,
    pub(crate) audit_unavailable: u64,
    pub(crate) internal_integrity: u64,
    pub(crate) cursor_unavailable: u64,
    pub(crate) cursor_evicted: u64,
    pub(crate) read_retry_attempt: u64,
    pub(crate) read_retry_exhausted: u64,
    pub(crate) stream_closed_by_policy: u64,
    pub(crate) capacity_rejected: u64,
}

fn saturating_increment(counter: &AtomicU64) {
    let _result = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

struct RetainedDiagnostics {
    errors: VecDeque<InternalError>,
}

/// Monotonic time for the defect budget, injectable so a test can advance it
/// rather than sleep for a refill interval.
pub(crate) trait DefectClock: Send + Sync {
    fn now(&self) -> Instant;
}

/// The process clock.
pub(crate) struct SystemDefectClock;

impl DefectClock for SystemDefectClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A refilling allowance of process-scoped defects (ADR-0250 decision 3).
///
/// A lifetime count cannot tell a broken process from a broken request: a
/// daemon that sees 256 unrelated defects across weeks of uptime reaches the
/// same total as one that sees them in a minute. The budget spends on a burst
/// and recovers with time, so only a rate trips it.
struct DefectBudget {
    remaining: u32,
    last_refill: Instant,
}

impl DefectBudget {
    fn new(now: Instant) -> Self {
        Self {
            remaining: DEFECT_BURST_CAPACITY,
            last_refill: now,
        }
    }

    /// Spends one unit, refilling first. Returns false when the budget is spent,
    /// which is the condition that trips the breaker.
    fn spend(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last_refill);
        let refills = u32::try_from(elapsed.as_secs() / DEFECT_REFILL_INTERVAL.as_secs().max(1))
            .unwrap_or(u32::MAX);
        if refills > 0 {
            self.remaining = self
                .remaining
                .saturating_add(refills)
                .min(DEFECT_BURST_CAPACITY);
            // Carry the remainder so a defect every 59 seconds still refills at
            // the stated rate rather than never.
            self.last_refill += DEFECT_REFILL_INTERVAL * refills;
        }
        if self.remaining == 0 {
            return false;
        }
        self.remaining -= 1;
        true
    }
}

/// Bounded trusted retention for internal sources before P2 observability exists.
pub(crate) struct ProductionServiceDiagnostics {
    routing: RuntimeRoutingState,
    retained: Mutex<RetainedDiagnostics>,
    dropped: AtomicU64,
    budget: Mutex<DefectBudget>,
    clock: Arc<dyn DefectClock>,
}

impl ProductionServiceDiagnostics {
    pub(crate) fn new(routing: RuntimeRoutingState) -> Self {
        Self::with_clock(routing, Arc::new(SystemDefectClock))
    }

    pub(crate) fn with_clock(routing: RuntimeRoutingState, clock: Arc<dyn DefectClock>) -> Self {
        let started = clock.now();
        Self {
            routing,
            retained: Mutex::new(RetainedDiagnostics {
                errors: VecDeque::with_capacity(MAX_RETAINED_INTERNAL_DIAGNOSTICS),
            }),
            dropped: AtomicU64::new(0),
            budget: Mutex::new(DefectBudget::new(started)),
            clock,
        }
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.lock_retained().errors.len()
    }

    pub(crate) fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Transfers retained sources only to a future trusted redaction layer.
    pub(crate) fn drain_for_trusted_observer(&self) -> Vec<InternalError> {
        self.lock_retained().errors.drain(..).collect()
    }

    fn lock_budget(&self) -> MutexGuard<'_, DefectBudget> {
        match self.budget.lock() {
            Ok(budget) => budget,
            Err(poisoned) => {
                self.routing
                    .stop(RuntimeStopReason::SupervisionStateCorrupted);
                poisoned.into_inner()
            }
        }
    }

    fn lock_retained(&self) -> MutexGuard<'_, RetainedDiagnostics> {
        match self.retained.lock() {
            Ok(retained) => retained,
            Err(poisoned) => {
                self.routing
                    .stop(RuntimeStopReason::SupervisionStateCorrupted);
                poisoned.into_inner()
            }
        }
    }
}

impl ServiceDiagnostics for ProductionServiceDiagnostics {
    fn record_internal(&self, error: InternalError) {
        // Retention and the breaker are separate concerns (ADR-0250 decision 5).
        // Conflating them is what let a request-scoped defect stop the process:
        // the ring rotated correctly and the stop fired anyway, on a count.
        let scope = error.scope();
        let mut retained = self.lock_retained();
        if retained.errors.len() == MAX_RETAINED_INTERNAL_DIAGNOSTICS {
            retained.errors.pop_front();
            saturating_increment(&self.dropped);
        }
        retained.errors.push_back(error);
        drop(retained);

        // Only a process-scoped defect draws on the budget. A request-scoped one
        // says this request could not be completed and nothing about the next,
        // so no number of them stops the runtime (ADR-0250 decision 2).
        if scope != DefectScope::Process {
            return;
        }
        let now = self.clock.now();
        let spent = {
            let mut budget = self.lock_budget();
            !budget.spend(now)
        };
        if spent {
            self.routing
                .stop(RuntimeStopReason::DiagnosticCapacityExceeded);
        }
    }
}

impl fmt::Debug for ProductionServiceDiagnostics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionServiceDiagnostics([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;
    use std::time::Duration;

    use riffdb_service::{
        ServiceHealthHooks, ServiceJobSpawner, ServiceTelemetry, ServiceTerminalClass,
    };
    use riffdb_types::{IncidentId, ServiceIngressKindV1, ServiceOperationV1};
    use tokio::sync::Barrier;

    use super::*;

    #[derive(Debug)]
    struct TestInternalSource;

    impl fmt::Display for TestInternalSource {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("sensitive test source")
        }
    }

    impl std::error::Error for TestInternalSource {}

    fn internal_error(seed: u8) -> InternalError {
        scoped_internal_error(seed, DefectScope::Process)
    }

    fn scoped_internal_error(seed: u8, scope: DefectScope) -> InternalError {
        let mut bytes = [0_u8; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        bytes[15] = seed;
        InternalError::new(
            IncidentId::from_bytes(bytes).expect("valid UUIDv7 incident fixture"),
            scope,
            TestInternalSource,
        )
    }

    /// A clock the test moves by hand, so a refill is asserted rather than
    /// waited for.
    struct ManualClock {
        now: Mutex<Instant>,
    }

    impl ManualClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                now: Mutex::new(Instant::now()),
            })
        }

        fn advance(&self, by: Duration) {
            let mut now = self.now.lock().expect("manual clock");
            *now += by;
        }
    }

    impl DefectClock for ManualClock {
        fn now(&self) -> Instant {
            *self.now.lock().expect("manual clock")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_scheduler_wakes_only_at_the_absolute_deadline() {
        let scheduler = TokioRequestDeadlineScheduler;
        let deadline = tokio::time::Instant::now().into_std() + Duration::from_secs(5);
        let waiter = tokio::spawn(async move {
            scheduler.wait_until(deadline).await;
        });

        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(!waiter.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        waiter.await.expect("deadline waiter must complete");
    }

    #[tokio::test(start_paused = true)]
    async fn expired_deadline_is_ready_and_dropping_a_wait_cancels_it() {
        let scheduler = TokioRequestDeadlineScheduler;
        let now = tokio::time::Instant::now().into_std();
        let expired = now
            .checked_sub(Duration::from_nanos(1))
            .expect("test instant supports one earlier nanosecond");
        scheduler.wait_until(expired).await;

        let cancelled = scheduler.wait_until(now + Duration::from_secs(30));
        drop(cancelled);
        tokio::time::advance(Duration::from_secs(30)).await;
    }

    #[tokio::test]
    async fn completed_service_job_is_driven_and_its_handle_is_released() {
        let routing = RuntimeRoutingState::new();
        let spawner = SupervisedServiceJobSpawner::with_runtime(Handle::current(), routing.clone());
        let (completed, observed) = oneshot::channel();

        spawner.spawn(Box::pin(async move {
            let _result = completed.send(());
        }));

        observed.await.expect("accepted job must run");
        spawner.wait_for_idle().await;
        assert_eq!(spawner.active_jobs(), 0);
        assert!(routing.is_routing_allowed());
        assert_eq!(routing.stop_reason(), None);
    }

    #[tokio::test]
    async fn accepted_job_cannot_start_before_its_handle_is_retained() {
        let routing = RuntimeRoutingState::new();
        let spawner = SupervisedServiceJobSpawner::with_runtime(Handle::current(), routing);
        let observing_spawner = spawner.clone();
        let (observed_count, observation) = oneshot::channel();

        spawner.spawn(Box::pin(async move {
            let _result = observed_count.send(observing_spawner.active_jobs());
        }));

        assert_eq!(observation.await.expect("job observation"), 1);
        spawner.wait_for_idle().await;
    }

    #[tokio::test]
    async fn idle_wait_observes_the_registry_without_a_lost_wakeup() {
        let routing = RuntimeRoutingState::new();
        let spawner = SupervisedServiceJobSpawner::with_runtime(Handle::current(), routing);
        let (started, observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        spawner.spawn(Box::pin(async move {
            let _result = started.send(());
            let _result = released.await;
        }));
        observed.await.expect("accepted job must start");

        let idle_spawner = spawner.clone();
        let idle = tokio::spawn(async move {
            idle_spawner.wait_for_idle().await;
        });
        tokio::task::yield_now().await;
        assert!(!idle.is_finished());

        let _result = release.send(());
        idle.await.expect("idle waiter must be notified");
        assert_eq!(spawner.active_jobs(), 0);
    }

    #[tokio::test]
    async fn accepted_job_panic_stops_routing_and_releases_its_handle() {
        let routing = RuntimeRoutingState::new();
        let spawner = SupervisedServiceJobSpawner::with_runtime(Handle::current(), routing.clone());

        spawner.spawn(Box::pin(async {
            panic!("injected accepted-job panic");
        }));

        assert_eq!(
            routing.stopped().await,
            RuntimeStopReason::AcceptedServiceJobPanicked
        );
        spawner.wait_for_idle().await;
        assert_eq!(spawner.active_jobs(), 0);
    }

    #[tokio::test]
    async fn simultaneous_job_panics_preserve_one_monotonic_stop_reason() {
        let routing = RuntimeRoutingState::new();
        let spawner = SupervisedServiceJobSpawner::with_runtime(Handle::current(), routing.clone());
        let barrier = Arc::new(Barrier::new(3));

        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            spawner.spawn(Box::pin(async move {
                barrier.wait().await;
                panic!("simultaneous injected panic");
            }));
        }
        barrier.wait().await;

        assert_eq!(
            routing.stopped().await,
            RuntimeStopReason::AcceptedServiceJobPanicked
        );
        spawner.wait_for_idle().await;
        assert_eq!(
            routing.stop_reason(),
            Some(RuntimeStopReason::AcceptedServiceJobPanicked)
        );
        assert_eq!(spawner.active_jobs(), 0);
    }

    #[tokio::test]
    async fn accepted_job_disappearance_stops_routing_without_sleeping() {
        let routing = RuntimeRoutingState::new();
        let spawner = SupervisedServiceJobSpawner::with_runtime(Handle::current(), routing.clone());
        let (started, observed) = oneshot::channel();

        spawner.spawn(Box::pin(async move {
            let _result = started.send(());
            pending::<()>().await;
        }));
        observed.await.expect("accepted job must start");
        spawner.abort_all_for_test();

        assert_eq!(
            routing.stopped().await,
            RuntimeStopReason::AcceptedServiceJobDisappeared
        );
        spawner.wait_for_idle().await;
        assert_eq!(spawner.active_jobs(), 0);
    }

    #[test]
    fn runtime_shutdown_marks_an_accepted_pending_job_disappeared() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");
        let routing = RuntimeRoutingState::new();
        let spawner =
            SupervisedServiceJobSpawner::with_runtime(runtime.handle().clone(), routing.clone());
        let (started, observed) = oneshot::channel();

        runtime.block_on(async {
            spawner.spawn(Box::pin(async move {
                let _result = started.send(());
                pending::<()>().await;
            }));
            observed.await.expect("accepted job must start");
        });
        assert_eq!(spawner.active_jobs(), 1);

        drop(runtime);
        assert_eq!(
            routing.stop_reason(),
            Some(RuntimeStopReason::AcceptedServiceJobDisappeared)
        );
        assert_eq!(spawner.active_jobs(), 0);
    }

    #[test]
    fn authoritative_health_failure_is_monotonic_and_stops_routing() {
        let routing = RuntimeRoutingState::new();
        routing.fail_authoritative_readiness(AuthoritativeReadinessFailure::AuditUnavailable);
        routing.fail_authoritative_readiness(AuthoritativeReadinessFailure::Integrity);

        assert!(!routing.is_routing_allowed());
        assert_eq!(
            routing.stop_reason(),
            Some(RuntimeStopReason::AuditUnavailable)
        );
    }

    #[test]
    fn telemetry_retains_only_closed_payload_free_counts() {
        let telemetry = ProductionServiceTelemetry::default();
        telemetry.record(ServiceTelemetryEvent::OperationTerminal {
            operation: ServiceOperationV1::GetHealth,
            ingress: ServiceIngressKindV1::Grpc,
            terminal: ServiceTerminalClass::Succeeded,
            elapsed: Duration::from_millis(1),
        });
        telemetry.record(ServiceTelemetryEvent::AuditUnavailable {
            operation: ServiceOperationV1::ExecuteCommand,
        });
        telemetry.record(ServiceTelemetryEvent::InternalIntegrity {
            operation: ServiceOperationV1::GetEntity,
        });
        telemetry.record(ServiceTelemetryEvent::CursorUnavailable);
        telemetry.record(ServiceTelemetryEvent::StreamClosedByPolicy);

        assert_eq!(
            telemetry.snapshot(),
            ServiceTelemetrySnapshot {
                operation_terminal: 1,
                audit_unavailable: 1,
                internal_integrity: 1,
                cursor_unavailable: 1,
                cursor_evicted: 0,
                read_retry_attempt: 0,
                read_retry_exhausted: 0,
                stream_closed_by_policy: 1,
                capacity_rejected: 0,
            }
        );
        assert_eq!(
            format!("{telemetry:?}"),
            "ProductionServiceTelemetry([REDACTED])"
        );
    }

    /// OBL-0250-1. A defect one client can reach, repeated far past both the
    /// retention bound and the burst capacity, leaves the runtime routing.
    ///
    /// This is the shape that killed a daemon on the bench host: a projected
    /// query against an unregistered columnar source failed as an internal
    /// defect, and repeating it stopped the process at demand 258.
    #[test]
    fn a_client_reachable_defect_path_cannot_stop_the_runtime() {
        let routing = RuntimeRoutingState::new();
        let diagnostics = ProductionServiceDiagnostics::new(routing.clone());

        for round in 0..2 {
            for seed in 0..=u8::MAX {
                diagnostics
                    .record_internal(scoped_internal_error(seed ^ round, DefectScope::Request));
            }
        }

        assert!(
            routing.is_routing_allowed(),
            "no number of request-scoped defects may stop the runtime"
        );
        assert_eq!(routing.stop_reason(), None);
        assert_eq!(
            diagnostics.retained_count(),
            MAX_RETAINED_INTERNAL_DIAGNOSTICS,
            "retention still bounds memory"
        );
        assert!(
            diagnostics.dropped_count() > 0,
            "the ring rotated rather than growing"
        );
    }

    /// OBL-0250-2. Narrowing what feeds the breaker must not make it fail open:
    /// a burst of process-scoped defects still stops the runtime.
    #[test]
    fn a_burst_of_process_scoped_defects_still_stops_the_runtime() {
        let routing = RuntimeRoutingState::new();
        let diagnostics = ProductionServiceDiagnostics::new(routing.clone());

        for seed in 0..DEFECT_BURST_CAPACITY {
            diagnostics.record_internal(internal_error(seed as u8));
            assert!(
                routing.is_routing_allowed(),
                "the budget covers exactly its burst capacity"
            );
        }

        diagnostics.record_internal(internal_error(0xff));
        assert_eq!(
            routing.stop_reason(),
            Some(RuntimeStopReason::DiagnosticCapacityExceeded),
            "the defect past the burst capacity trips the breaker"
        );
    }

    /// OBL-0250-3. A spent budget recovers with time, so defects spread across a
    /// long uptime never accumulate into a shutdown. The clock is advanced
    /// rather than slept on.
    #[test]
    fn the_defect_budget_refills_over_time() {
        let clock = ManualClock::new();
        let routing = RuntimeRoutingState::new();
        let diagnostics = ProductionServiceDiagnostics::with_clock(
            routing.clone(),
            Arc::clone(&clock) as Arc<dyn DefectClock>,
        );

        for seed in 0..DEFECT_BURST_CAPACITY {
            diagnostics.record_internal(internal_error(seed as u8));
        }
        assert!(routing.is_routing_allowed(), "the burst is exactly covered");

        clock.advance(DEFECT_REFILL_INTERVAL);
        diagnostics.record_internal(internal_error(0xfe));
        assert!(
            routing.is_routing_allowed(),
            "one refill interval returns one unit of budget"
        );

        // Spending the single refilled unit leaves the budget empty again, so
        // the next defect with no further time trips it. Without this the test
        // would also pass against a breaker that never trips at all.
        diagnostics.record_internal(internal_error(0xfd));
        assert_eq!(
            routing.stop_reason(),
            Some(RuntimeStopReason::DiagnosticCapacityExceeded),
            "the refill is one unit, not a reset"
        );
    }
}
