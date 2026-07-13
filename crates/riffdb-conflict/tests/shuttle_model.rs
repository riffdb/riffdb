//! Shuttle schedules over the production sharded conflict manager.
//!
//! These larger histories use the same transition, queue, waker, cancellation,
//! release, and manual-deadline code as the default manager. The scheduler hook
//! only records/yields; this suite contains no duplicate lock-state model.

#![cfg(feature = "shuttle")]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc as StdArc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use riffdb_conflict::{
    CancellationToken, ConflictError, ConflictManager, ConflictManagerConfig,
    ConflictSchedulePoint, DeterministicConflictScheduler, ShardedConflictManager,
};
use riffdb_types::{AggregateTypeId, ConflictKey, ConflictKeyBuilder};
use shuttle::sync::Arc;
use shuttle::sync::atomic::{AtomicUsize, Ordering};
use shuttle::thread;

const LONG_DEADLINE: Duration = Duration::from_secs(60);

#[test]
fn production_larger_overlapping_histories_never_double_grant_or_leak() {
    shuttle::check_random(
        || {
            let trace = StdArc::new(Mutex::new(Vec::new()));
            let scheduler = StdArc::new(YieldScheduler {
                trace: StdArc::clone(&trace),
            });
            let (manager, _) = test_manager(scheduler);
            let active = Arc::new((0..3).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
            let holder =
                block_on(manager.acquire_mut(vec![key(2)], deadline(), CancellationToken::new()))
                    .expect("initial holder");
            active[2].store(1, Ordering::SeqCst);
            trace.lock().expect("trace").clear();

            let requests = [vec![1, 2], vec![0, 1], vec![0], vec![2, 0]];
            let tasks = requests
                .into_iter()
                .map(|keys| spawn_writer(manager.clone(), Arc::clone(&active), keys))
                .collect::<Vec<_>>();
            thread::yield_now();
            assert_eq!(active[2].swap(0, Ordering::SeqCst), 1);
            holder.release();

            for task in tasks {
                task.join().expect("writer");
            }
            assert!(active.iter().all(|count| count.load(Ordering::SeqCst) == 0));
            assert!(
                trace.lock().expect("trace").iter().any(|(point, _)| {
                    *point == ConflictSchedulePoint::SuccessorPromotionComplete
                })
            );
        },
        300,
    );
}

#[test]
fn production_cancel_deadline_and_release_schedules_preserve_progress() {
    shuttle::check_random(
        || {
            let trace = StdArc::new(Mutex::new(Vec::new()));
            let scheduler = StdArc::new(YieldScheduler {
                trace: StdArc::clone(&trace),
            });
            let (manager, driver) = test_manager(scheduler);
            let holder = block_on(manager.acquire_mut(
                vec![key(0), key(1)],
                deadline(),
                CancellationToken::new(),
            ))
            .expect("holder");
            trace.lock().expect("trace").clear();

            let cancellation = CancellationToken::new();
            let mut cancelled =
                manager.acquire_mut(vec![key(1), key(0)], deadline(), cancellation.clone());
            let mut timed_out =
                manager.acquire_mut(vec![key(0)], deadline(), CancellationToken::new());
            assert!(poll_once(cancelled.as_mut()).is_pending());
            assert!(poll_once(timed_out.as_mut()).is_pending());
            let timeout_id = trace
                .lock()
                .expect("trace")
                .iter()
                .rev()
                .find_map(|(point, waiter)| {
                    (*point == ConflictSchedulePoint::DeadlineRegistered).then_some(*waiter)
                })
                .expect("timeout waiter");

            let cancel_task = thread::spawn(move || cancellation.cancel());
            let timeout_task = thread::spawn(move || driver.notify_deadline(timeout_id));
            thread::yield_now();
            holder.release();
            cancel_task.join().expect("cancel task");
            assert!(timeout_task.join().expect("timeout task"));

            assert_eq!(
                block_on(cancelled).expect_err("cancelled waiter"),
                ConflictError::Cancelled
            );
            assert_eq!(
                block_on(timed_out).expect_err("timed out waiter"),
                ConflictError::DeadlineExceeded
            );
            block_on(manager.acquire_mut(
                vec![key(1), key(0)],
                deadline(),
                CancellationToken::new(),
            ))
            .expect("terminal waiters leave no capability")
            .release();
        },
        300,
    );
}

fn test_manager(
    scheduler: StdArc<dyn DeterministicConflictScheduler>,
) -> (ShardedConflictManager, riffdb_conflict::ConflictTestDriver) {
    ShardedConflictManager::with_test_scheduler(
        ConflictManagerConfig::new(4, 8, 64, 64, 64).expect("test bounds"),
        scheduler,
    )
    .expect("manual production manager")
}

fn spawn_writer(
    manager: ShardedConflictManager,
    active: Arc<Vec<AtomicUsize>>,
    key_ids: Vec<u64>,
) -> shuttle::thread::JoinHandle<()> {
    thread::spawn(move || {
        let keys = key_ids.iter().copied().map(key).collect::<Vec<_>>();
        let lease = block_on(manager.acquire_mut(keys, deadline(), CancellationToken::new()))
            .expect("writer grant");
        for key_id in &key_ids {
            assert_eq!(active[*key_id as usize].fetch_add(1, Ordering::SeqCst), 0);
        }
        thread::yield_now();
        for key_id in &key_ids {
            assert_eq!(active[*key_id as usize].fetch_sub(1, Ordering::SeqCst), 1);
        }
        lease.release();
    })
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
                | ConflictSchedulePoint::WakerRegistered
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

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::from(StdArc::new(ThreadWaker(thread::current())));
    poll_with(future, &waker)
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
