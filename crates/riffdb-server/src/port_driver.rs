#![expect(
    clippy::expect_used,
    reason = "the bounded blocking driver retains its named worker thread after successful spawn"
)]

//! Bounded blocking-work driver for production application-service ports.

// The driver is private composition infrastructure assembled during WP-130.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use riffdb_service::{
    AuthoritativeReadinessFailure, BoxPortCapacityPermit, PortAdmissionError, PortCapacityPermit,
    PortReceipt, RequestControl, ServiceHealthHooks, port_completion_channel,
};

use crate::runtime_support::RuntimeRoutingState;

/// Fixed number of threads dedicated to blocking P1 consumer-port operations.
///
/// This is deliberately independent of the 256-item commit-notification bound:
/// notifications and admitted blocking operations have different ownership and
/// backpressure semantics.
pub(crate) const P1_BLOCKING_PORT_WORKER_THREADS: usize = 32;

/// Maximum permits, queued jobs, and executing P1 blocking port operations.
///
/// The conservative bound limits synchronous storage/catalog pressure while
/// retaining enough capacity for the P1 public RPC surface. It is not a commit
/// subscription or notification capacity.
pub(crate) const P1_MAX_BLOCKING_PORT_OPERATIONS: usize = 256;

/// Maximum time a contended read waits for a free port permit.
pub(crate) const P1_PORT_ADMISSION_MAX_WAIT: Duration = Duration::from_millis(250);

/// Reserved margin so a waited admission still leaves room for the request body.
///
/// Must stay ≤ the service-layer `READ_RETRY_MIN_REMAINING` budget so a margin-
/// bound wait classifies as the client's deadline rather than storage unavailability.
pub(crate) const P1_PORT_ADMISSION_DEADLINE_MARGIN: Duration = Duration::from_millis(25);

/// Maximum concurrent waiters blocked on port admission.
///
/// Beyond this cap, admission fails immediately with
/// [`PortAdmissionError::Unavailable`] rather than unbounded queue growth.
pub(crate) const P1_MAX_BLOCKING_PORT_WAITERS: usize = 512;

type BlockingJob = Box<dyn FnOnce() + Send + 'static>;

/// RAII waiter-slot lease. Decrement is unconditional on drop so cancellation
/// or deadline races that drop a parked `reserve_async` future cannot leak slots.
struct WaiterSlot<'a> {
    waiters: &'a AtomicUsize,
    active: bool,
}

impl<'a> WaiterSlot<'a> {
    fn try_acquire(waiters: &'a AtomicUsize) -> Result<Self, PortAdmissionError> {
        let slot = waiters.fetch_add(1, Ordering::AcqRel);
        if slot >= P1_MAX_BLOCKING_PORT_WAITERS {
            waiters.fetch_sub(1, Ordering::AcqRel);
            return Err(PortAdmissionError::Unavailable);
        }
        Ok(Self {
            waiters,
            active: true,
        })
    }
}

impl Drop for WaiterSlot<'_> {
    fn drop(&mut self) {
        if self.active {
            self.waiters.fetch_sub(1, Ordering::AcqRel);
            self.active = false;
        }
    }
}

struct DriverState {
    accepting: bool,
}

struct BlockingPortDriverInner {
    routing: RuntimeRoutingState,
    state: Mutex<DriverState>,
    permits: Arc<Semaphore>,
    waiters: AtomicUsize,
    queue: JobQueue,
    max_in_flight: usize,
    installed_workers: AtomicUsize,
}

struct JobQueue {
    state: Mutex<JobQueueState>,
    available: Condvar,
    capacity: usize,
}

struct JobQueueState {
    jobs: VecDeque<BlockingJob>,
    closed: bool,
}

impl JobQueue {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(JobQueueState {
                jobs: VecDeque::with_capacity(capacity),
                closed: false,
            }),
            available: Condvar::new(),
            capacity,
        }
    }

    fn push(&self, job: BlockingJob) -> Result<(), BlockingJob> {
        let Ok(mut state) = self.state.lock() else {
            return Err(job);
        };
        if state.closed || state.jobs.len() == self.capacity {
            return Err(job);
        }
        state.jobs.push_back(job);
        self.available.notify_one();
        Ok(())
    }

    fn pop(&self) -> Result<Option<BlockingJob>, ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        loop {
            if let Some(job) = state.jobs.pop_front() {
                return Ok(Some(job));
            }
            if state.closed {
                return Ok(None);
            }
            state = self.available.wait(state).map_err(|_| ())?;
        }
    }

    fn close(&self) -> Result<(), ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        state.closed = true;
        drop(state);
        self.available.notify_all();
        Ok(())
    }
}

impl BlockingPortDriverInner {
    fn try_reserve(
        self: &Arc<Self>,
        control: &RequestControl,
    ) -> Result<Reservation, PortAdmissionError> {
        self.precheck_control(control)?;
        self.ensure_accepting()?;
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| PortAdmissionError::Unavailable)?;
        if !self.is_accepting() {
            drop(permit);
            return Err(PortAdmissionError::Stopped);
        }
        Ok(Reservation {
            permit: Some(permit),
        })
    }

    async fn reserve_async(
        self: &Arc<Self>,
        control: &RequestControl,
    ) -> Result<Reservation, PortAdmissionError> {
        self.precheck_control(control)?;
        self.ensure_accepting()?;

        if let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() {
            if !self.is_accepting() {
                drop(permit);
                return Err(PortAdmissionError::Stopped);
            }
            return Ok(Reservation {
                permit: Some(permit),
            });
        }

        // Slot is released on every path, including future drop during
        // wait_with_control cancel/deadline races.
        let _waiter_slot = WaiterSlot::try_acquire(&self.waiters)?;

        let now = Instant::now();
        let max_wait_deadline = now.checked_add(P1_PORT_ADMISSION_MAX_WAIT).unwrap_or(now);
        let request_wait_deadline = control
            .deadline()
            .checked_sub(P1_PORT_ADMISSION_DEADLINE_MARGIN)
            .unwrap_or(now);
        let wait_until = max_wait_deadline.min(request_wait_deadline);
        if wait_until <= now {
            // When the margin (not the absolute deadline clock alone) bounds the
            // wait to empty, the request cannot be admitted inside its own
            // deadline budget — that is a deadline outcome, not Unavailable.
            return if request_wait_deadline <= now
                || control.is_deadline_exceeded()
                || Instant::now() >= control.deadline()
            {
                Err(PortAdmissionError::DeadlineExceeded)
            } else {
                Err(PortAdmissionError::Unavailable)
            };
        }
        let timeout = wait_until.saturating_duration_since(Instant::now());

        let acquire = Arc::clone(&self.permits).acquire_owned();
        let outcome = tokio::select! {
            biased;
            () = control.cancelled() => Err(PortAdmissionError::Cancelled),
            result = tokio::time::timeout(timeout, acquire) => match result {
                Ok(Ok(permit)) => Ok(permit),
                Ok(Err(_)) => Err(PortAdmissionError::Stopped),
                Err(_) => {
                    if control.is_deadline_exceeded()
                        || Instant::now() >= control.deadline()
                        || Instant::now()
                            .checked_add(P1_PORT_ADMISSION_DEADLINE_MARGIN)
                            .is_some_and(|bound| bound >= control.deadline())
                    {
                        Err(PortAdmissionError::DeadlineExceeded)
                    } else {
                        Err(PortAdmissionError::Unavailable)
                    }
                }
            },
        };

        let permit = outcome?;
        if control.is_cancelled() {
            drop(permit);
            return Err(PortAdmissionError::Cancelled);
        }
        if control.is_deadline_exceeded() {
            drop(permit);
            return Err(PortAdmissionError::DeadlineExceeded);
        }
        if !self.is_accepting() {
            drop(permit);
            return Err(PortAdmissionError::Stopped);
        }
        Ok(Reservation {
            permit: Some(permit),
        })
    }

    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.waiters.load(Ordering::Acquire)
    }

    fn precheck_control(&self, control: &RequestControl) -> Result<(), PortAdmissionError> {
        if control.is_cancelled() {
            return Err(PortAdmissionError::Cancelled);
        }
        if control.is_deadline_exceeded() {
            return Err(PortAdmissionError::DeadlineExceeded);
        }
        if !self.routing.is_routing_allowed() {
            return Err(PortAdmissionError::Stopped);
        }
        Ok(())
    }

    fn ensure_accepting(&self) -> Result<(), PortAdmissionError> {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                self.close_poisoned(poisoned);
                return Err(PortAdmissionError::Stopped);
            }
        };
        if !state.accepting {
            return Err(PortAdmissionError::Stopped);
        }
        Ok(())
    }

    fn is_accepting(&self) -> bool {
        match self.state.lock() {
            Ok(state) => state.accepting && self.routing.is_routing_allowed(),
            Err(_) => false,
        }
    }

    fn ensure_submission_open(&self) -> Result<(), PortAdmissionError> {
        if !self.routing.is_routing_allowed() {
            return Err(PortAdmissionError::Stopped);
        }
        self.ensure_accepting()
    }

    fn close(&self) -> bool {
        let mut poisoned = false;
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(error) => {
                poisoned = true;
                error.into_inner()
            }
        };
        state.accepting = false;
        drop(state);
        if self.queue.close().is_err() {
            poisoned = true;
        }
        if poisoned {
            self.fail_integrity();
        }
        poisoned
    }

    fn close_poisoned(&self, error: std::sync::PoisonError<MutexGuard<'_, DriverState>>) {
        let mut state = error.into_inner();
        state.accepting = false;
        drop(state);
        let _ = self.queue.close();
        self.fail_integrity();
    }

    fn fail_integrity(&self) {
        self.routing
            .fail_authoritative_readiness(AuthoritativeReadinessFailure::Integrity);
    }

    fn stop_after_worker_failure(&self) {
        self.close();
        self.fail_integrity();
    }

    fn outstanding_permits_after_shutdown(&self) -> Result<usize, ()> {
        // available_permits is the free count; outstanding = max - free.
        Ok(self
            .max_in_flight
            .saturating_sub(self.permits.available_permits()))
    }
}

/// Retained owner of the fixed blocking-port worker set.
///
/// Call [`Self::shutdown_and_drain`] after transport and service admission are
/// closed. Dropping the owner closes its queue but intentionally does not hide
/// worker-join failures behind `Drop`.
pub(crate) struct BlockingPortDriver {
    inner: Arc<BlockingPortDriverInner>,
    workers: Vec<JoinHandle<()>>,
}

impl BlockingPortDriver {
    /// Starts the exact production worker set and reservation bound.
    pub(crate) fn new(routing: RuntimeRoutingState) -> Result<Self, BlockingPortDriverStartError> {
        Self::with_limits(
            routing,
            P1_BLOCKING_PORT_WORKER_THREADS,
            P1_MAX_BLOCKING_PORT_OPERATIONS,
        )
    }

    fn with_limits(
        routing: RuntimeRoutingState,
        worker_count: usize,
        max_in_flight: usize,
    ) -> Result<Self, BlockingPortDriverStartError> {
        if worker_count == 0 || max_in_flight == 0 {
            routing.fail_authoritative_readiness(AuthoritativeReadinessFailure::Integrity);
            return Err(BlockingPortDriverStartError::InvalidLimits);
        }

        let inner = Arc::new(BlockingPortDriverInner {
            routing,
            state: Mutex::new(DriverState { accepting: true }),
            permits: Arc::new(Semaphore::new(max_in_flight)),
            waiters: AtomicUsize::new(0),
            queue: JobQueue::new(max_in_flight),
            max_in_flight,
            installed_workers: AtomicUsize::new(0),
        });
        let mut workers = Vec::with_capacity(worker_count);
        let mut starts = Vec::with_capacity(worker_count);

        for worker_index in 0..worker_count {
            let (start, started) = mpsc::channel();
            let worker_inner = Arc::clone(&inner);
            let handle = thread::Builder::new()
                .name(format!("riffdb-port-{worker_index}"))
                .spawn(move || {
                    if started.recv().is_ok() {
                        run_worker(&worker_inner);
                    }
                });
            match handle {
                Ok(handle) => {
                    workers.push(handle);
                    inner.installed_workers.fetch_add(1, Ordering::Release);
                    starts.push(start);
                }
                Err(source) => {
                    inner.stop_after_worker_failure();
                    drop(starts);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(BlockingPortDriverStartError::ThreadSpawn(source));
                }
            }
        }

        // Every handle is retained before any worker may receive a job.
        let mut start_failed = false;
        for start in starts.drain(..) {
            if start.send(()).is_err() {
                start_failed = true;
            }
        }
        if start_failed {
            inner.stop_after_worker_failure();
            for worker in workers {
                let _ = worker.join();
            }
            return Err(BlockingPortDriverStartError::WorkerDisappeared);
        }

        Ok(Self { inner, workers })
    }

    /// Creates a typed cloneable executor sharing this driver's global bound.
    pub(crate) fn executor<Request, Response, Failure>(
        &self,
        operation: impl Fn(Request) -> Result<Response, Failure> + Send + Sync + 'static,
    ) -> BlockingPortExecutor<Request, Response, Failure>
    where
        Request: Send + 'static,
        Response: Send + 'static,
        Failure: Send + 'static,
    {
        BlockingPortExecutor {
            inner: Arc::clone(&self.inner),
            operation: Arc::new(operation),
        }
    }

    /// Closes admission, drains every accepted queued job, and joins all workers.
    pub(crate) fn shutdown_and_drain(mut self) -> Result<(), BlockingPortDriverShutdownError> {
        let state_was_poisoned = self.inner.close();
        let mut worker_panicked = false;
        for worker in self.workers.drain(..) {
            if worker.join().is_err() {
                worker_panicked = true;
                self.inner.fail_integrity();
            }
        }

        if state_was_poisoned || self.inner.outstanding_permits_after_shutdown().is_err() {
            return Err(BlockingPortDriverShutdownError::StateCorrupted);
        }
        if worker_panicked {
            return Err(BlockingPortDriverShutdownError::WorkerPanicked);
        }
        if self
            .inner
            .outstanding_permits_after_shutdown()
            .expect("state was checked immediately above")
            != 0
        {
            return Err(BlockingPortDriverShutdownError::OutstandingReservations);
        }
        Ok(())
    }
}

impl Drop for BlockingPortDriver {
    fn drop(&mut self) {
        self.inner.close();
    }
}

impl fmt::Debug for BlockingPortDriver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BlockingPortDriver([REDACTED])")
    }
}

fn run_worker(inner: &Arc<BlockingPortDriverInner>) {
    loop {
        let job = match inner.queue.pop() {
            Ok(Some(job)) => job,
            Ok(None) => return,
            Err(()) => {
                inner.stop_after_worker_failure();
                return;
            }
        };
        if catch_unwind(AssertUnwindSafe(job)).is_err() {
            // The unwinding job drops its completion sender. The service-owned
            // receipt therefore observes PortDriverStopped rather than a guessed
            // operation failure.
            inner.stop_after_worker_failure();
        }
    }
}

struct Reservation {
    /// Owned permit; dropping releases capacity. Taken on explicit release so
    /// the permit is never held across an await in the service job.
    permit: Option<OwnedSemaphorePermit>,
}

impl Reservation {
    fn release(mut self) {
        self.permit.take();
    }
}

/// Cloneable typed admission entry point for one blocking consumer operation.
pub(crate) struct BlockingPortExecutor<Request, Response, Failure> {
    inner: Arc<BlockingPortDriverInner>,
    operation: Arc<dyn Fn(Request) -> Result<Response, Failure> + Send + Sync + 'static>,
}

impl<Request, Response, Failure> Clone for BlockingPortExecutor<Request, Response, Failure> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            operation: Arc::clone(&self.operation),
        }
    }
}

impl<Request, Response, Failure> BlockingPortExecutor<Request, Response, Failure>
where
    Request: Send + 'static,
    Response: Send + 'static,
    Failure: Send + 'static,
{
    /// Applies the admission gates a reservation applies, without taking capacity.
    ///
    /// A caller that answers a request inline instead of dispatching to the
    /// pool MUST run this first. It is the same routing-allowed, cancellation,
    /// deadline, and accepting-state refusal set that [`Self::reserve`] and
    /// [`Self::reserve_async`] apply before acquiring a permit, so a drained,
    /// stopping, cancelled, or deadline-exceeded request is never served from
    /// process-local cache.
    pub(crate) fn precheck(&self, control: &RequestControl) -> Result<(), PortAdmissionError> {
        self.inner.precheck_control(control)?;
        self.inner.ensure_accepting()
    }

    /// Reserves one typed move-only permit without waiting (sync fast path).
    pub(crate) fn reserve(
        &self,
        control: &RequestControl,
    ) -> Result<BoxPortCapacityPermit<Request, Response, Failure>, PortAdmissionError> {
        let reservation = self.inner.try_reserve(control)?;
        Ok(Box::new(BlockingPortPermit {
            inner: Arc::clone(&self.inner),
            operation: Arc::clone(&self.operation),
            reservation,
        }))
    }

    /// Reserves one typed move-only permit with deadline-aware contention wait.
    pub(crate) async fn reserve_async(
        &self,
        control: &RequestControl,
    ) -> Result<BoxPortCapacityPermit<Request, Response, Failure>, PortAdmissionError> {
        let reservation = self.inner.reserve_async(control).await?;
        Ok(Box::new(BlockingPortPermit {
            inner: Arc::clone(&self.inner),
            operation: Arc::clone(&self.operation),
            reservation,
        }))
    }
}

impl<Request, Response, Failure> fmt::Debug for BlockingPortExecutor<Request, Response, Failure> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BlockingPortExecutor([REDACTED])")
    }
}

struct BlockingPortPermit<Request, Response, Failure> {
    inner: Arc<BlockingPortDriverInner>,
    operation: Arc<dyn Fn(Request) -> Result<Response, Failure> + Send + Sync + 'static>,
    reservation: Reservation,
}

impl<Request, Response, Failure> PortCapacityPermit<Request, Response, Failure>
    for BlockingPortPermit<Request, Response, Failure>
where
    Request: Send + 'static,
    Response: Send + 'static,
    Failure: Send + 'static,
{
    fn submit(
        self: Box<Self>,
        request: Request,
    ) -> Result<PortReceipt<Response, Failure>, PortAdmissionError> {
        let Self {
            inner,
            operation,
            reservation,
        } = *self;
        inner.ensure_submission_open()?;
        let (completion, receipt) = port_completion_channel();
        let job: BlockingJob = Box::new(move || {
            let result = operation(request);
            reservation.release();
            completion.complete(result);
        });

        if inner.queue.push(job).is_ok() {
            Ok(receipt)
        } else {
            inner.stop_after_worker_failure();
            Err(PortAdmissionError::Stopped)
        }
    }
}

/// Closed failure to start the retained production worker set.
#[derive(Debug)]
pub(crate) enum BlockingPortDriverStartError {
    InvalidLimits,
    ThreadSpawn(std::io::Error),
    WorkerDisappeared,
}

impl fmt::Display for BlockingPortDriverStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => formatter.write_str("blocking port driver limits are invalid"),
            Self::ThreadSpawn(_) => {
                formatter.write_str("a blocking port worker could not be started")
            }
            Self::WorkerDisappeared => {
                formatter.write_str("a blocking port worker disappeared during startup")
            }
        }
    }
}

impl std::error::Error for BlockingPortDriverStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ThreadSpawn(source) => Some(source),
            Self::InvalidLimits | Self::WorkerDisappeared => None,
        }
    }
}

/// Closed failure to complete an explicit driver drain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BlockingPortDriverShutdownError {
    StateCorrupted,
    WorkerPanicked,
    OutstandingReservations,
}

impl fmt::Display for BlockingPortDriverShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::StateCorrupted => "blocking port driver state is corrupted",
            Self::WorkerPanicked => "a blocking port worker panicked",
            Self::OutstandingReservations => {
                "blocking port driver stopped with outstanding unsubmitted reservations"
            }
        })
    }
}

impl std::error::Error for BlockingPortDriverShutdownError {}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, mpsc};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::{Duration, Instant};

    use riffdb_service::PortDriverStopped;

    use super::*;
    use crate::runtime_support::RuntimeStopReason;

    struct ThreadWake(thread::Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let wake = Arc::new(ThreadWake(thread::current()));
        let waker = Waker::from(wake);
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => thread::park(),
            }
        }
    }

    fn live_control() -> RequestControl {
        RequestControl::new(Instant::now() + Duration::from_secs(60)).0
    }

    fn admission_error<T>(result: Result<T, PortAdmissionError>) -> PortAdmissionError {
        match result {
            Ok(_) => panic!("admission unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    fn test_driver(workers: usize, capacity: usize) -> (RuntimeRoutingState, BlockingPortDriver) {
        let routing = RuntimeRoutingState::new();
        let driver = BlockingPortDriver::with_limits(routing.clone(), workers, capacity)
            .expect("test driver must start");
        (routing, driver)
    }

    #[test]
    fn permits_reserve_and_release_the_exact_global_capacity() {
        let (_routing, driver) = test_driver(1, 1);
        let executor = driver.executor(|value: u32| Ok::<_, ()>(value + 1));
        let control = live_control();

        let first = executor.reserve(&control).expect("first permit");
        assert_eq!(
            admission_error(executor.reserve(&control)),
            PortAdmissionError::Unavailable
        );
        drop(first);

        let second = executor.reserve(&control).expect("released permit");
        drop(second);
        driver.shutdown_and_drain().expect("clean shutdown");
    }

    #[test]
    fn contended_async_reserve_succeeds_when_permit_frees_within_wait() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");
        let (_routing, driver) = test_driver(1, 1);
        let executor = driver.executor(|value: u32| Ok::<_, ()>(value));
        let control = live_control();
        let held = executor.reserve(&control).expect("hold capacity");

        let waiter = executor.clone();
        let wait_control = live_control();
        let join = std::thread::spawn(move || {
            runtime.block_on(async move { waiter.reserve_async(&wait_control).await })
        });
        std::thread::sleep(Duration::from_millis(20));
        drop(held);
        join.join()
            .expect("waiter thread")
            .expect("contended wait succeeds");
        driver.shutdown_and_drain().expect("clean shutdown");
    }

    #[test]
    fn async_reserve_maps_deadline_and_cancellation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");
        let (_routing, driver) = test_driver(1, 1);
        let executor = driver.executor(|value: u32| Ok::<_, ()>(value));
        let live = live_control();
        let held = executor.reserve(&live).expect("hold capacity");

        let expired = RequestControl::new(Instant::now() - Duration::from_millis(1)).0;
        assert_eq!(
            admission_error(runtime.block_on(executor.reserve_async(&expired))),
            PortAdmissionError::DeadlineExceeded
        );

        let (control, cancel) = RequestControl::new(Instant::now() + Duration::from_secs(60));
        cancel.cancel();
        assert_eq!(
            admission_error(runtime.block_on(executor.reserve_async(&control))),
            PortAdmissionError::Cancelled
        );
        drop(held);
        driver.shutdown_and_drain().expect("clean shutdown");
    }

    #[test]
    fn margin_bound_empty_wait_is_deadline_exceeded_not_unavailable() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");
        let (_routing, driver) = test_driver(1, 1);
        let executor = driver.executor(|value: u32| Ok::<_, ()>(value));
        let held = executor
            .reserve(&live_control())
            .expect("hold the only permit");

        // Remaining budget is inside the margin window [0, MARGIN): the wait
        // cannot start, and that is the client's deadline outcome.
        let near = RequestControl::new(
            Instant::now() + P1_PORT_ADMISSION_DEADLINE_MARGIN - Duration::from_millis(5),
        )
        .0;
        assert_eq!(
            admission_error(runtime.block_on(executor.reserve_async(&near))),
            PortAdmissionError::DeadlineExceeded
        );
        drop(held);
        driver.shutdown_and_drain().expect("clean shutdown");
    }

    #[test]
    fn dropped_contended_waiters_release_slots_so_cap_recovers() {
        let (_routing, driver) = test_driver(1, 1);
        let executor = driver.executor(|value: u32| Ok::<_, ()>(value));
        let held = executor
            .reserve(&live_control())
            .expect("hold the only permit");

        // Park N waiters, drop half mid-wait, and assert the RAII slot guard
        // releases so the cap recovers for later contended reserves.
        let park_count = 16_usize;
        let early_count = park_count / 2;
        let late_count = park_count - early_count;
        let entered = Arc::new(AtomicUsize::new(0));
        let all_entered = Arc::new(Barrier::new(park_count + 1));
        let release_late = Arc::new(Barrier::new(late_count + 1));
        let mut joins = Vec::with_capacity(park_count);
        for index in 0..park_count {
            let waiter = executor.clone();
            let entered = Arc::clone(&entered);
            let all_entered = Arc::clone(&all_entered);
            let release_late = Arc::clone(&release_late);
            let drop_early = index < early_count;
            joins.push(thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .expect("waiter runtime");
                let control = RequestControl::new(Instant::now() + Duration::from_secs(60)).0;
                runtime.block_on(async {
                    let fut = waiter.reserve_async(&control);
                    let mut fut = std::pin::pin!(fut);
                    // Drive until the waiter slot is acquired (try_acquire failed
                    // and WaiterSlot is live).
                    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
                    let mut context = Context::from_waker(&waker);
                    match fut.as_mut().poll(&mut context) {
                        Poll::Pending => {}
                        Poll::Ready(_) => panic!("unexpected ready before release"),
                    }
                    entered.fetch_add(1, Ordering::SeqCst);
                    all_entered.wait();
                    if drop_early {
                        // Leave the async block without awaiting: dropping the
                        // pinned future runs WaiterSlot::Drop.
                        return;
                    }
                    release_late.wait();
                    // Completing after capacity frees returns a permit; drop it.
                    let _ = fut.await;
                });
            }));
        }

        all_entered.wait();
        assert_eq!(
            entered.load(Ordering::SeqCst),
            park_count,
            "every waiter acquired a slot"
        );
        // After all_entered, early half drops; give them a moment to run Drop.
        thread::sleep(Duration::from_millis(20));
        assert_eq!(
            driver.inner.waiter_count(),
            late_count,
            "early-dropped half must release; remaining half stay parked"
        );

        drop(held);
        release_late.wait();
        for join in joins {
            // Late waiters may complete with a permit; drop it inside the thread.
            join.join().expect("waiter thread");
        }
        assert_eq!(
            driver.inner.waiter_count(),
            0,
            "every waiter slot released after completion/drop"
        );

        let admitted = executor
            .reserve(&live_control())
            .expect("fresh reserve succeeds after full recovery");
        drop(admitted);
        driver.shutdown_and_drain().expect("clean shutdown");
    }

    #[test]
    fn every_worker_handle_is_installed_before_execution_begins() {
        let (_routing, driver) = test_driver(2, 4);
        let inner = Arc::clone(&driver.inner);
        let executor = driver.executor(move |(): ()| {
            assert_eq!(inner.installed_workers.load(Ordering::Acquire), 2);
            Ok::<_, ()>(())
        });

        let receipt = executor
            .reserve(&live_control())
            .expect("permit")
            .submit(())
            .expect("accepted operation");
        assert_eq!(block_on(receipt), Ok(Ok(())));
        driver.shutdown_and_drain().expect("clean shutdown");
    }

    #[test]
    fn dropping_receipt_does_not_cancel_accepted_work() {
        let (_routing, driver) = test_driver(1, 2);
        let gate = Arc::new(Barrier::new(2));
        let completed = Arc::new(AtomicBool::new(false));
        let worker_gate = Arc::clone(&gate);
        let worker_completed = Arc::clone(&completed);
        let executor = driver.executor(move |(): ()| {
            worker_gate.wait();
            worker_completed.store(true, Ordering::SeqCst);
            Ok::<_, ()>(())
        });

        let receipt = executor
            .reserve(&live_control())
            .expect("permit")
            .submit(())
            .expect("accepted operation");
        drop(receipt);
        gate.wait();
        driver.shutdown_and_drain().expect("clean shutdown");
        assert!(completed.load(Ordering::SeqCst));
    }

    #[test]
    fn panicking_operation_abandons_sender_and_stops_routing() {
        let (routing, driver) = test_driver(1, 2);
        let executor = driver
            .executor(|(): ()| -> Result<(), ()> { panic!("injected blocking operation panic") });
        let receipt = executor
            .reserve(&live_control())
            .expect("permit")
            .submit(())
            .expect("accepted operation");

        assert_eq!(block_on(receipt), Err(PortDriverStopped));
        assert_eq!(block_on(routing.stopped()), RuntimeStopReason::Integrity);
        assert_eq!(routing.stop_reason(), Some(RuntimeStopReason::Integrity));
        assert_eq!(
            admission_error(executor.reserve(&live_control())),
            PortAdmissionError::Stopped
        );
        driver
            .shutdown_and_drain()
            .expect("task panic is isolated from worker shutdown");
    }

    #[test]
    fn poisoned_and_stopped_admission_fail_closed() {
        let (routing, driver) = test_driver(1, 2);
        let executor = driver.executor(|(): ()| Ok::<_, ()>(()));
        let poisoned_inner = Arc::clone(&driver.inner);
        let poisoner = thread::spawn(move || {
            let _guard = poisoned_inner
                .state
                .lock()
                .expect("initially healthy state");
            panic!("inject driver-state poison");
        });
        assert!(poisoner.join().is_err());

        assert_eq!(
            admission_error(executor.reserve(&live_control())),
            PortAdmissionError::Stopped
        );
        assert_eq!(routing.stop_reason(), Some(RuntimeStopReason::Integrity));
        assert_eq!(
            driver.shutdown_and_drain(),
            Err(BlockingPortDriverShutdownError::StateCorrupted)
        );

        let (_routing, driver) = test_driver(1, 2);
        let executor = driver.executor(|(): ()| Ok::<_, ()>(()));
        driver.shutdown_and_drain().expect("clean shutdown");
        assert_eq!(
            admission_error(executor.reserve(&live_control())),
            PortAdmissionError::Stopped
        );
    }

    #[test]
    fn control_is_checked_before_capacity_admission() {
        let (_routing, driver) = test_driver(1, 1);
        let executor = driver.executor(|(): ()| Ok::<_, ()>(()));
        let held = executor.reserve(&live_control()).expect("capacity holder");

        let (cancelled, cancellation) =
            RequestControl::new(Instant::now() + Duration::from_secs(60));
        cancellation.cancel();
        assert_eq!(
            admission_error(executor.reserve(&cancelled)),
            PortAdmissionError::Cancelled
        );

        let expired = RequestControl::new(Instant::now()).0;
        assert_eq!(
            admission_error(executor.reserve(&expired)),
            PortAdmissionError::DeadlineExceeded
        );
        drop(held);
        driver.shutdown_and_drain().expect("clean shutdown");
    }

    #[test]
    fn shutdown_drains_all_accepted_jobs_and_joins_workers() {
        let (_routing, driver) = test_driver(2, 8);
        let completed = Arc::new(AtomicUsize::new(0));
        let operation_completed = Arc::clone(&completed);
        let executor = driver.executor(move |value: usize| {
            operation_completed.fetch_add(value, Ordering::SeqCst);
            Ok::<_, ()>(())
        });
        let mut receipts = Vec::new();
        for value in 1..=8 {
            receipts.push(
                executor
                    .reserve(&live_control())
                    .expect("bounded permit")
                    .submit(value)
                    .expect("accepted operation"),
            );
        }

        driver.shutdown_and_drain().expect("clean drain");
        assert_eq!(completed.load(Ordering::SeqCst), 36);
        for receipt in receipts {
            assert_eq!(block_on(receipt), Ok(Ok(())));
        }
    }

    #[test]
    fn reservation_is_released_before_completion_is_published() {
        let (_routing, driver) = test_driver(1, 1);
        let executor = driver.executor(|value: u32| Ok::<_, ()>(value));
        let first = executor
            .reserve(&live_control())
            .expect("first permit")
            .submit(7)
            .expect("first operation");
        assert_eq!(block_on(first), Ok(Ok(7)));

        let next = executor
            .reserve(&live_control())
            .expect("released capacity");
        drop(next);
        driver.shutdown_and_drain().expect("clean shutdown");
    }

    #[test]
    fn startup_rejects_zero_limits_and_fails_readiness() {
        let routing = RuntimeRoutingState::new();
        assert!(matches!(
            BlockingPortDriver::with_limits(routing.clone(), 0, 1),
            Err(BlockingPortDriverStartError::InvalidLimits)
        ));
        assert_eq!(routing.stop_reason(), Some(RuntimeStopReason::Integrity));
    }

    #[test]
    fn worker_is_independently_driven_before_receipt_is_polled() {
        let (_routing, driver) = test_driver(1, 1);
        let (completed, observed) = mpsc::sync_channel(0);
        let executor = driver.executor(move |value: u32| {
            completed.send(value).expect("test receiver retained");
            Ok::<_, ()>(value)
        });
        let receipt = executor
            .reserve(&live_control())
            .expect("permit")
            .submit(11)
            .expect("accepted operation");

        assert_eq!(observed.recv().expect("worker must run"), 11);
        assert_eq!(block_on(receipt), Ok(Ok(11)));
        driver.shutdown_and_drain().expect("clean shutdown");
    }

    #[test]
    fn distinct_workers_receive_jobs_before_either_job_completes() {
        let (_routing, driver) = test_driver(2, 4);
        let (entered, observations) = mpsc::sync_channel(2);
        let release = Arc::new(Barrier::new(3));
        let operation_release = Arc::clone(&release);
        let executor = driver.executor(move |value: usize| {
            entered.send(value).expect("entry observation");
            operation_release.wait();
            Ok::<_, ()>(value)
        });
        let control = RequestControl::new(
            Instant::now()
                .checked_add(Duration::from_secs(1))
                .expect("deadline"),
        )
        .0;
        let first = executor
            .reserve(&control)
            .expect("first permit")
            .submit(1)
            .expect("first submission");
        let second = executor
            .reserve(&control)
            .expect("second permit")
            .submit(2)
            .expect("second submission");

        let mut started = [
            observations
                .recv_timeout(Duration::from_secs(1))
                .expect("first worker entered"),
            observations
                .recv_timeout(Duration::from_secs(1))
                .expect("second worker entered before the first completed"),
        ];
        started.sort_unstable();
        assert_eq!(started, [1, 2]);
        release.wait();
        assert_eq!(block_on(first), Ok(Ok(1)));
        assert_eq!(block_on(second), Ok(Ok(2)));
        driver.shutdown_and_drain().expect("clean shutdown");
    }
}
