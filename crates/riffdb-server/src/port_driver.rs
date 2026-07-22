//! Bounded blocking-work driver for production application-service ports.

// The driver is private composition infrastructure assembled during WP-130.
#![allow(dead_code)]

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

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
pub(crate) const P1_BLOCKING_PORT_WORKER_THREADS: usize = 4;

/// Maximum permits, queued jobs, and executing P1 blocking port operations.
///
/// The conservative bound limits synchronous storage/catalog pressure while
/// retaining enough capacity for the P1 public RPC surface. It is not a commit
/// subscription or notification capacity.
pub(crate) const P1_MAX_BLOCKING_PORT_OPERATIONS: usize = 32;

type BlockingJob = Box<dyn FnOnce() + Send + 'static>;

struct DriverState {
    accepting: bool,
    in_flight: usize,
    sender: Option<SyncSender<BlockingJob>>,
}

struct BlockingPortDriverInner {
    routing: RuntimeRoutingState,
    state: Mutex<DriverState>,
    max_in_flight: usize,
    installed_workers: AtomicUsize,
}

impl BlockingPortDriverInner {
    fn reserve(
        self: &Arc<Self>,
        control: &RequestControl,
    ) -> Result<Reservation, PortAdmissionError> {
        if control.is_cancelled() {
            return Err(PortAdmissionError::Cancelled);
        }
        if control.is_deadline_exceeded() {
            return Err(PortAdmissionError::DeadlineExceeded);
        }
        if !self.routing.is_routing_allowed() {
            return Err(PortAdmissionError::Stopped);
        }

        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                self.close_poisoned(poisoned);
                return Err(PortAdmissionError::Stopped);
            }
        };
        if !state.accepting || state.sender.is_none() {
            return Err(PortAdmissionError::Stopped);
        }
        if state.in_flight == self.max_in_flight {
            return Err(PortAdmissionError::Unavailable);
        }
        state.in_flight += 1;
        drop(state);

        Ok(Reservation {
            driver: Arc::clone(self),
            released: false,
        })
    }

    fn sender_for_submission(&self) -> Result<SyncSender<BlockingJob>, PortAdmissionError> {
        if !self.routing.is_routing_allowed() {
            return Err(PortAdmissionError::Stopped);
        }
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
        state
            .sender
            .as_ref()
            .cloned()
            .ok_or(PortAdmissionError::Stopped)
    }

    fn release_reservation(&self) {
        let mut poisoned = false;
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(error) => {
                poisoned = true;
                error.into_inner()
            }
        };
        if state.in_flight == 0 {
            poisoned = true;
        } else {
            state.in_flight -= 1;
        }
        if poisoned {
            state.accepting = false;
            state.sender.take();
        }
        drop(state);
        if poisoned {
            self.fail_integrity();
        }
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
        state.sender.take();
        drop(state);
        if poisoned {
            self.fail_integrity();
        }
        poisoned
    }

    fn close_poisoned(&self, error: std::sync::PoisonError<MutexGuard<'_, DriverState>>) {
        let mut state = error.into_inner();
        state.accepting = false;
        state.sender.take();
        drop(state);
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

    fn in_flight_after_shutdown(&self) -> Result<usize, ()> {
        match self.state.lock() {
            Ok(state) => Ok(state.in_flight),
            Err(error) => {
                let in_flight = error.into_inner().in_flight;
                self.fail_integrity();
                let _ = in_flight;
                Err(())
            }
        }
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

        let (sender, receiver) = mpsc::sync_channel(max_in_flight);
        let receiver = Arc::new(Mutex::new(receiver));
        let inner = Arc::new(BlockingPortDriverInner {
            routing,
            state: Mutex::new(DriverState {
                accepting: true,
                in_flight: 0,
                sender: Some(sender),
            }),
            max_in_flight,
            installed_workers: AtomicUsize::new(0),
        });
        let mut workers = Vec::with_capacity(worker_count);
        let mut starts = Vec::with_capacity(worker_count);

        for worker_index in 0..worker_count {
            let (start, started) = mpsc::channel();
            let worker_inner = Arc::clone(&inner);
            let worker_receiver = Arc::clone(&receiver);
            let handle = thread::Builder::new()
                .name(format!("riffdb-port-{worker_index}"))
                .spawn(move || {
                    if started.recv().is_ok() {
                        run_worker(&worker_inner, &worker_receiver);
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

        if state_was_poisoned || self.inner.in_flight_after_shutdown().is_err() {
            return Err(BlockingPortDriverShutdownError::StateCorrupted);
        }
        if worker_panicked {
            return Err(BlockingPortDriverShutdownError::WorkerPanicked);
        }
        if self
            .inner
            .in_flight_after_shutdown()
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

fn run_worker(inner: &Arc<BlockingPortDriverInner>, receiver: &Arc<Mutex<Receiver<BlockingJob>>>) {
    loop {
        let job = {
            let receiver = match receiver.lock() {
                Ok(receiver) => receiver,
                Err(poisoned) => {
                    inner.stop_after_worker_failure();
                    poisoned.into_inner()
                }
            };
            receiver.recv()
        };
        let Ok(job) = job else {
            return;
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
    driver: Arc<BlockingPortDriverInner>,
    released: bool,
}

impl Reservation {
    fn release(mut self) {
        self.released = true;
        self.driver.release_reservation();
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.released {
            self.driver.release_reservation();
        }
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
    /// Reserves one typed move-only permit after synchronous control checks.
    pub(crate) fn reserve(
        &self,
        control: &RequestControl,
    ) -> Result<BoxPortCapacityPermit<Request, Response, Failure>, PortAdmissionError> {
        let reservation = self.inner.reserve(control)?;
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
        let queue = inner.sender_for_submission()?;
        let (completion, receipt) = port_completion_channel();
        let job: BlockingJob = Box::new(move || {
            let result = operation(request);
            reservation.release();
            completion.complete(result);
        });

        match queue.try_send(job) {
            Ok(()) => Ok(receipt),
            Err(TrySendError::Full(job)) | Err(TrySendError::Disconnected(job)) => {
                drop(job);
                inner.stop_after_worker_failure();
                Err(PortAdmissionError::Stopped)
            }
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
}
