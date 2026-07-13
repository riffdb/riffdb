//! Loom schedules over the production conflict-manager transition kernel.
//!
//! The feature-gated scheduler systematically explores interleavings at
//! production registration, grant consumption, waker, cancellation, release,
//! successor-promotion, and manual-deadline checkpoints. Standard-library
//! mutex/atomic memory models are not replaced by Loom primitives; this is a
//! bounded production state-machine interleaving proof, not a weak-memory proof.
//! Each focused scenario explores up to 5,000 permutations with a three-
//! preemption bound. No lock-state transition is reimplemented in this suite.

#![cfg(feature = "loom")]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc as StdArc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use loom::sync::Arc;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;
use riffdb_conflict::{
    CancellationToken, ConflictError, ConflictManager, ConflictManagerConfig,
    ConflictSchedulePoint, DeterministicConflictScheduler, ShardedConflictManager,
};
use riffdb_types::{AggregateTypeId, ConflictKey, ConflictKeyBuilder};

const LONG_DEADLINE: Duration = Duration::from_secs(60);

#[test]
fn production_fifo_grant_release_and_waker_paths_are_explored() {
    model(|| {
        let trace = StdArc::new(Mutex::new(Vec::new()));
        let scheduler = StdArc::new(YieldScheduler {
            trace: StdArc::clone(&trace),
        });
        let (manager, _) = test_manager(scheduler);
        let active = Arc::new(AtomicUsize::new(1));
        let holder =
            block_on(manager.acquire_mut(vec![key(0)], deadline(), CancellationToken::new()))
                .expect("initial holder");
        trace.lock().expect("trace").clear();

        let first = spawn_writer(manager.clone(), Arc::clone(&active), vec![key(0)]);
        let second = spawn_writer(manager.clone(), Arc::clone(&active), vec![key(0)]);
        thread::yield_now();
        assert_eq!(active.swap(0, Ordering::SeqCst), 1);
        holder.release();
        first.join().expect("first writer");
        second.join().expect("second writer");
        assert_eq!(active.load(Ordering::SeqCst), 0);

        let trace = trace.lock().expect("trace");
        let enqueued = waiters_at(&trace, ConflictSchedulePoint::Enqueued);
        let granted = waiters_at(&trace, ConflictSchedulePoint::Granted);
        assert_eq!(enqueued, granted, "one-key FIFO grant order changed");
        assert!(
            trace
                .iter()
                .any(|(point, _)| *point == ConflictSchedulePoint::WakerRegistered)
        );
        assert!(
            trace
                .iter()
                .any(|(point, _)| { *point == ConflictSchedulePoint::SuccessorPromotionComplete })
        );
        for expected in [
            ConflictSchedulePoint::BeforeGrantConsumption,
            ConflictSchedulePoint::GrantConsumed,
            ConflictSchedulePoint::BeforeWaiterWake,
            ConflictSchedulePoint::WaiterWakeComplete,
            ConflictSchedulePoint::BeforeRelease,
            ConflictSchedulePoint::Released,
        ] {
            assert!(trace.iter().any(|(point, _)| *point == expected));
        }
    });
}

#[test]
fn production_opposite_multi_key_order_is_all_or_nothing() {
    model(|| {
        let trace = StdArc::new(Mutex::new(Vec::new()));
        let scheduler = StdArc::new(YieldScheduler {
            trace: StdArc::clone(&trace),
        });
        let (manager, _) = test_manager(scheduler);
        let active = Arc::new(AtomicUsize::new(1));
        let holder = block_on(manager.acquire_mut(
            vec![key(0), key(1)],
            deadline(),
            CancellationToken::new(),
        ))
        .expect("initial multi-key holder");
        trace.lock().expect("trace").clear();

        let forward = spawn_writer(manager.clone(), Arc::clone(&active), vec![key(0), key(1)]);
        let reverse = spawn_writer(manager.clone(), Arc::clone(&active), vec![key(1), key(0)]);
        let single = spawn_writer(manager.clone(), Arc::clone(&active), vec![key(0)]);
        thread::yield_now();
        assert_eq!(active.swap(0, Ordering::SeqCst), 1);
        holder.release();
        forward.join().expect("forward writer");
        reverse.join().expect("reverse writer");
        single.join().expect("single-key writer");

        let trace = trace.lock().expect("trace");
        let enqueued = waiters_at(&trace, ConflictSchedulePoint::Enqueued);
        let granted = waiters_at(&trace, ConflictSchedulePoint::Granted);
        assert_eq!(enqueued.len(), 3);
        assert_eq!(
            enqueued, granted,
            "canonical multi-key queues must have one FIFO order"
        );
    });
}

#[test]
fn production_cancellation_concurrent_with_grant_cannot_leak() {
    model(|| {
        let trace = StdArc::new(Mutex::new(Vec::new()));
        let scheduler = StdArc::new(YieldScheduler {
            trace: StdArc::clone(&trace),
        });
        let (manager, _) = test_manager(scheduler);
        let holder =
            block_on(manager.acquire_mut(vec![key(0)], deadline(), CancellationToken::new()))
                .expect("holder");
        let cancellation = CancellationToken::new();
        let mut waiter = manager.acquire_mut(vec![key(0)], deadline(), cancellation.clone());
        let waker = Waker::from(StdArc::new(ThreadWaker(thread::current())));
        assert!(poll_with(waiter.as_mut(), &waker).is_pending());
        let cancel = thread::spawn(move || cancellation.cancel());
        holder.release();
        match block_on(waiter) {
            Ok(lease) => lease.release(),
            Err(error) => assert_eq!(error, ConflictError::Cancelled),
        }
        cancel.join().expect("canceller");

        block_on(manager.acquire_mut(vec![key(0)], deadline(), CancellationToken::new()))
            .expect("cancel/grant race leaves no holder")
            .release();
    });
}

#[test]
fn production_registered_cancellation_notifies_waker_and_removes_waiter() {
    model(|| {
        let trace = StdArc::new(Mutex::new(Vec::new()));
        let scheduler = StdArc::new(YieldScheduler {
            trace: StdArc::clone(&trace),
        });
        let (manager, _) = test_manager(scheduler);
        let holder =
            block_on(manager.acquire_mut(vec![key(0)], deadline(), CancellationToken::new()))
                .expect("holder");
        trace.lock().expect("trace").clear();
        let cancellation = CancellationToken::new();
        let wakes = StdArc::new(CountWaker(AtomicUsize::new(0)));
        let waker = Waker::from(StdArc::clone(&wakes));
        let mut waiter = manager.acquire_mut(vec![key(0)], deadline(), cancellation.clone());
        assert!(poll_with(waiter.as_mut(), &waker).is_pending());

        let cancel = thread::spawn(move || cancellation.cancel());
        cancel.join().expect("canceller");
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        assert_eq!(
            block_on(waiter).expect_err("registered cancellation rejects"),
            ConflictError::Cancelled
        );
        holder.release();
        block_on(manager.acquire_mut(vec![key(0)], deadline(), CancellationToken::new()))
            .expect("cancelled waiter leaves no capability")
            .release();

        let trace = trace.lock().expect("trace");
        for expected in [
            ConflictSchedulePoint::BeforeCancellationRegistration,
            ConflictSchedulePoint::CancellationRegistered,
            ConflictSchedulePoint::WakerRegistered,
            ConflictSchedulePoint::CancellationObserved,
            ConflictSchedulePoint::BeforeAbort,
            ConflictSchedulePoint::BeforeWaiterWake,
            ConflictSchedulePoint::WaiterWakeComplete,
            ConflictSchedulePoint::Aborted,
        ] {
            assert!(
                trace.iter().any(|(point, _)| *point == expected),
                "missing cancellation checkpoint {expected:?}"
            );
        }
    });
}

#[test]
fn production_manual_deadline_notifies_registered_waker_and_releases_queue() {
    model(|| {
        let trace = StdArc::new(Mutex::new(Vec::new()));
        let scheduler = StdArc::new(YieldScheduler {
            trace: StdArc::clone(&trace),
        });
        let (manager, driver) = test_manager(scheduler);
        let holder =
            block_on(manager.acquire_mut(vec![key(0)], deadline(), CancellationToken::new()))
                .expect("holder");
        trace.lock().expect("trace").clear();
        let wakes = StdArc::new(CountWaker(AtomicUsize::new(0)));
        let waker = Waker::from(StdArc::clone(&wakes));
        let mut waiter = manager.acquire_mut(vec![key(0)], deadline(), CancellationToken::new());
        assert!(poll_with(waiter.as_mut(), &waker).is_pending());
        let waiter_id = trace
            .lock()
            .expect("trace")
            .iter()
            .rev()
            .find_map(|(point, waiter)| {
                (*point == ConflictSchedulePoint::DeadlineRegistered).then_some(*waiter)
            })
            .expect("registered deadline");

        assert!(driver.notify_deadline(waiter_id));
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        assert_eq!(
            block_on(waiter).expect_err("deadline rejects"),
            ConflictError::DeadlineExceeded
        );
        holder.release();
        block_on(manager.acquire_mut(vec![key(0)], deadline(), CancellationToken::new()))
            .expect("deadline leaves no capability")
            .release();

        let trace = trace.lock().expect("trace");
        for expected in [
            ConflictSchedulePoint::DeadlineRegistered,
            ConflictSchedulePoint::DeadlineNotified,
            ConflictSchedulePoint::BeforeAbort,
            ConflictSchedulePoint::BeforeWaiterWake,
            ConflictSchedulePoint::WaiterWakeComplete,
            ConflictSchedulePoint::Aborted,
        ] {
            assert!(
                trace.iter().any(|(point, _)| *point == expected),
                "missing timeout checkpoint {expected:?}"
            );
        }
    });
}

fn model(check: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.max_threads = 4;
    builder.max_branches = 500;
    builder.max_permutations = Some(5_000);
    builder.preemption_bound = Some(3);
    builder.check(check);
}

fn test_manager(
    scheduler: StdArc<dyn DeterministicConflictScheduler>,
) -> (ShardedConflictManager, riffdb_conflict::ConflictTestDriver) {
    ShardedConflictManager::with_test_scheduler(
        ConflictManagerConfig::new(2, 8, 32, 32, 32).expect("test bounds"),
        scheduler,
    )
    .expect("manual production manager")
}

fn spawn_writer(
    manager: ShardedConflictManager,
    active: Arc<AtomicUsize>,
    keys: Vec<ConflictKey>,
) -> loom::thread::JoinHandle<()> {
    thread::spawn(move || {
        let lease = block_on(manager.acquire_mut(keys, deadline(), CancellationToken::new()))
            .expect("writer grant");
        assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
        thread::yield_now();
        assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
        lease.release();
    })
}

fn waiters_at(trace: &[(ConflictSchedulePoint, u64)], selected: ConflictSchedulePoint) -> Vec<u64> {
    trace
        .iter()
        .filter_map(|(point, waiter)| (*point == selected).then_some(*waiter))
        .collect()
}

struct YieldScheduler {
    trace: StdArc<Mutex<Vec<(ConflictSchedulePoint, u64)>>>,
}

impl DeterministicConflictScheduler for YieldScheduler {
    fn checkpoint(&self, point: ConflictSchedulePoint, waiter_id: u64) {
        self.trace.lock().expect("trace").push((point, waiter_id));
        if matches!(
            point,
            ConflictSchedulePoint::BeforeRegistration
                | ConflictSchedulePoint::RegistrationComplete
                | ConflictSchedulePoint::BeforeCancellationRegistration
                | ConflictSchedulePoint::CancellationRegistered
                | ConflictSchedulePoint::CancellationObserved
                | ConflictSchedulePoint::WakerRegistered
                | ConflictSchedulePoint::WaiterStateObserved
                | ConflictSchedulePoint::BeforeGrantConsumption
                | ConflictSchedulePoint::GrantConsumed
                | ConflictSchedulePoint::BeforeWaiterWake
                | ConflictSchedulePoint::WaiterWakeComplete
                | ConflictSchedulePoint::BeforeAbort
                | ConflictSchedulePoint::Aborted
                | ConflictSchedulePoint::BeforeRelease
                | ConflictSchedulePoint::Released
                | ConflictSchedulePoint::DeadlineRegistered
                | ConflictSchedulePoint::DeadlineNotified
                | ConflictSchedulePoint::BeforeSuccessorPromotion
                | ConflictSchedulePoint::SuccessorPromotionComplete
        ) {
            thread::yield_now();
        }
    }
}

struct CountWaker(AtomicUsize);

impl Wake for CountWaker {
    fn wake(self: StdArc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ThreadWaker(thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: StdArc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = Waker::from(StdArc::new(ThreadWaker(thread::current())));
    loop {
        match poll_with(future.as_mut(), &waker) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

fn poll_with<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

fn deadline() -> Instant {
    Instant::now() + LONG_DEADLINE
}

fn key(value: u64) -> ConflictKey {
    let mut builder = ConflictKeyBuilder::new(AggregateTypeId::new(1).expect("nonzero aggregate"));
    builder.push_u64(value).expect("bounded test key");
    builder.finish().expect("valid conflict key")
}
