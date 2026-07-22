//! Bounded process-local first-commit notification hub.

// The hub is consumed by the WP-130 authoritative-read adapter assembled in this crate.
#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::poll_fn;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use riffdb_commit::{ApplicationCommitNotificationError, ApplicationCommitNotificationSink};
use riffdb_service::{
    AuthoritativeCommitNotification, AuthoritativeReadError, CommitNotificationSource,
    MAX_COMMIT_SUBSCRIPTION_BUFFER_ITEMS, MAX_LIVE_COMMIT_SUBSCRIBERS, PortFuture,
};
use riffdb_types::{CommitSequence, FrontierPosition};

const MAX_FIRST_COMMIT_SUBSCRIBERS: usize = 128;
const FIRST_COMMIT_BUFFER_ITEMS: usize = 256;
const _: () = assert!(MAX_FIRST_COMMIT_SUBSCRIBERS == MAX_LIVE_COMMIT_SUBSCRIBERS as usize);
const _: () = assert!(FIRST_COMMIT_BUFFER_ITEMS == MAX_COMMIT_SUBSCRIPTION_BUFFER_ITEMS);

#[cfg(test)]
type TestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
#[derive(Default)]
struct NotificationTestHooks {
    before_waiter_registration: Option<TestHook>,
    before_publish_progress_lock: Option<TestHook>,
    after_publish_enqueue: Option<TestHook>,
    after_lagged_pending: Option<TestHook>,
    after_shutdown_pending: Option<TestHook>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SubscriberLifecycle {
    Open,
    LaggedPending,
    ShutdownPending,
    TerminalConsumed,
}

struct SubscriberProgress {
    pending: VecDeque<CommitSequence>,
    acknowledged: FrontierPosition,
    lifecycle: SubscriberLifecycle,
    waiter: Option<Waker>,
    #[cfg(test)]
    hooks: Arc<NotificationTestHooks>,
}

impl SubscriberProgress {
    fn new(after: Option<CommitSequence>, #[cfg(test)] hooks: Arc<NotificationTestHooks>) -> Self {
        Self {
            pending: VecDeque::with_capacity(FIRST_COMMIT_BUFFER_ITEMS),
            acknowledged: after.map_or(
                FrontierPosition::BeforeFirst,
                FrontierPosition::AppliedThrough,
            ),
            lifecycle: SubscriberLifecycle::Open,
            waiter: None,
            #[cfg(test)]
            hooks,
        }
    }

    fn expected_acknowledgement(&self) -> Option<CommitSequence> {
        match self.acknowledged {
            FrontierPosition::BeforeFirst => Some(CommitSequence::first()),
            FrontierPosition::AppliedThrough(sequence) => sequence.checked_next(),
        }
    }

    fn take_waiter(&mut self) -> Option<Waker> {
        self.waiter.take()
    }
}

struct HubSubscriber {
    progress: Arc<Mutex<SubscriberProgress>>,
}

struct HubState {
    subscribers: BTreeMap<u64, HubSubscriber>,
    next_subscriber_id: u64,
    closed: bool,
    #[cfg(test)]
    hooks: Arc<NotificationTestHooks>,
}

/// One server-owned notification hub shared by the coordinator and authoritative reader.
#[derive(Clone)]
pub(crate) struct FirstCommitNotificationHub {
    state: Arc<Mutex<HubState>>,
}

impl FirstCommitNotificationHub {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(HubState {
                subscribers: BTreeMap::new(),
                next_subscriber_id: 1,
                closed: false,
                #[cfg(test)]
                hooks: Arc::new(NotificationTestHooks::default()),
            })),
        }
    }

    #[cfg(test)]
    fn with_test_hooks(hooks: NotificationTestHooks) -> Self {
        Self {
            state: Arc::new(Mutex::new(HubState {
                subscribers: BTreeMap::new(),
                next_subscriber_id: 1,
                closed: false,
                hooks: Arc::new(hooks),
            })),
        }
    }

    /// Installs the subscriber synchronously before its caller begins authoritative catch-up.
    pub(crate) fn subscribe(
        &self,
        after: Option<CommitSequence>,
    ) -> Result<Box<dyn CommitNotificationSource>, NotificationHubError> {
        let mut state = self.state.lock().map_err(|_| NotificationHubError)?;
        if state.closed || state.subscribers.len() >= MAX_FIRST_COMMIT_SUBSCRIBERS {
            return Err(NotificationHubError);
        }
        let subscriber_id = state.next_subscriber_id;
        state.next_subscriber_id = state
            .next_subscriber_id
            .checked_add(1)
            .ok_or(NotificationHubError)?;
        let progress = Arc::new(Mutex::new(SubscriberProgress::new(
            after,
            #[cfg(test)]
            Arc::clone(&state.hooks),
        )));
        if state
            .subscribers
            .insert(
                subscriber_id,
                HubSubscriber {
                    progress: Arc::clone(&progress),
                },
            )
            .is_some()
        {
            return Err(NotificationHubError);
        }
        Ok(Box::new(FirstCommitNotificationSource {
            hub: Arc::downgrade(&self.state),
            subscriber_id,
            progress,
        }))
    }

    /// Closes every source without draining stale process-local hints.
    pub(crate) fn shutdown(&self) -> Result<(), NotificationHubError> {
        let mut state = self.state.lock().map_err(|_| NotificationHubError)?;
        state.closed = true;
        let mut waiters = Vec::new();
        let mut failed = false;
        for subscriber in state.subscribers.values() {
            match subscriber.progress.lock() {
                Ok(mut progress) => {
                    if progress.lifecycle == SubscriberLifecycle::Open {
                        progress.lifecycle = SubscriberLifecycle::ShutdownPending;
                    }
                    progress.pending.clear();
                    #[cfg(test)]
                    if let Some(hook) = &progress.hooks.after_shutdown_pending {
                        hook();
                    }
                    waiters.extend(progress.take_waiter());
                }
                Err(_) => failed = true,
            }
        }
        state.subscribers.clear();
        drop(state);
        for waiter in waiters {
            waiter.wake();
        }
        if failed {
            Err(NotificationHubError)
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn active_subscribers(&self) -> Result<usize, NotificationHubError> {
        self.state
            .lock()
            .map(|state| state.subscribers.len())
            .map_err(|_| NotificationHubError)
    }
}

impl Default for FirstCommitNotificationHub {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for FirstCommitNotificationHub {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FirstCommitNotificationHub([REDACTED])")
    }
}

impl ApplicationCommitNotificationSink for FirstCommitNotificationHub {
    fn publish_first_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<(), ApplicationCommitNotificationError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ApplicationCommitNotificationError)?;
        if state.closed {
            return Err(ApplicationCommitNotificationError);
        }

        let mut lagged = Vec::new();
        let mut waiters = Vec::new();
        for (&subscriber_id, subscriber) in &state.subscribers {
            #[cfg(test)]
            if let Some(hook) = &state.hooks.before_publish_progress_lock {
                hook();
            }
            let mut progress = subscriber
                .progress
                .lock()
                .map_err(|_| ApplicationCommitNotificationError)?;
            if progress.lifecycle != SubscriberLifecycle::Open {
                lagged.push(subscriber_id);
                continue;
            }
            if progress.pending.len() == FIRST_COMMIT_BUFFER_ITEMS {
                progress.lifecycle = SubscriberLifecycle::LaggedPending;
                progress.pending.clear();
                #[cfg(test)]
                if let Some(hook) = &progress.hooks.after_lagged_pending {
                    hook();
                }
                waiters.extend(progress.take_waiter());
                lagged.push(subscriber_id);
                continue;
            }
            progress.pending.push_back(sequence);
            #[cfg(test)]
            if let Some(hook) = &progress.hooks.after_publish_enqueue {
                hook();
            }
            waiters.extend(progress.take_waiter());
        }
        for subscriber_id in lagged {
            state.subscribers.remove(&subscriber_id);
        }
        drop(state);
        for waiter in waiters {
            waiter.wake();
        }
        Ok(())
    }
}

struct FirstCommitNotificationSource {
    hub: Weak<Mutex<HubState>>,
    subscriber_id: u64,
    progress: Arc<Mutex<SubscriberProgress>>,
}

impl CommitNotificationSource for FirstCommitNotificationSource {
    fn next(&mut self) -> PortFuture<'_, AuthoritativeCommitNotification, AuthoritativeReadError> {
        let progress = Arc::clone(&self.progress);
        Box::pin(poll_fn(move |context| {
            let Ok(mut progress) = progress.lock() else {
                return Poll::Ready(Err(AuthoritativeReadError::Integrity));
            };
            match progress.lifecycle {
                SubscriberLifecycle::LaggedPending => {
                    progress.lifecycle = SubscriberLifecycle::TerminalConsumed;
                    progress.pending.clear();
                    return Poll::Ready(Ok(AuthoritativeCommitNotification::Lagged {
                        resume_after: progress.acknowledged,
                    }));
                }
                SubscriberLifecycle::ShutdownPending => {
                    progress.lifecycle = SubscriberLifecycle::TerminalConsumed;
                    progress.pending.clear();
                    return Poll::Ready(Ok(AuthoritativeCommitNotification::Closed));
                }
                SubscriberLifecycle::TerminalConsumed => {
                    return Poll::Ready(Ok(AuthoritativeCommitNotification::Closed));
                }
                SubscriberLifecycle::Open => {}
            }
            if let Some(sequence) = progress.pending.pop_front() {
                return Poll::Ready(Ok(AuthoritativeCommitNotification::Advanced(sequence)));
            }
            #[cfg(test)]
            if let Some(hook) = &progress.hooks.before_waiter_registration {
                hook();
            }
            if progress
                .waiter
                .as_ref()
                .is_none_or(|waiter| !waiter.will_wake(context.waker()))
            {
                progress.waiter = Some(context.waker().clone());
            }
            Poll::Pending
        }))
    }

    fn acknowledge(
        &mut self,
        delivered_through: CommitSequence,
    ) -> Result<(), AuthoritativeReadError> {
        let mut progress = self
            .progress
            .lock()
            .map_err(|_| AuthoritativeReadError::Integrity)?;
        if progress.lifecycle == SubscriberLifecycle::TerminalConsumed
            || progress.expected_acknowledgement() != Some(delivered_through)
        {
            return Err(AuthoritativeReadError::Integrity);
        }
        progress.acknowledged = FrontierPosition::AppliedThrough(delivered_through);
        Ok(())
    }
}

impl Drop for FirstCommitNotificationSource {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade()
            && let Ok(mut state) = hub.lock()
        {
            state.subscribers.remove(&self.subscriber_id);
        }

        let Ok(mut progress) = self.progress.lock() else {
            return;
        };
        progress.lifecycle = SubscriberLifecycle::TerminalConsumed;
        progress.pending.clear();
        let waiter = progress.take_waiter();
        drop(progress);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}

impl fmt::Debug for FirstCommitNotificationSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FirstCommitNotificationSource([REDACTED])")
    }
}

/// Closed server-composition failure to establish or stop a notification source.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct NotificationHubError;

impl fmt::Debug for NotificationHubError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NotificationHubError([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread;

    use super::*;

    #[derive(Clone)]
    struct BlockingHook {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl BlockingHook {
        fn new() -> Self {
            Self {
                reached: Arc::new(Barrier::new(2)),
                resume: Arc::new(Barrier::new(2)),
            }
        }

        fn callback(&self) -> TestHook {
            let hook = self.clone();
            Arc::new(move || {
                hook.reached.wait();
                hook.resume.wait();
            })
        }

        fn wait_until_reached(&self) {
            self.reached.wait();
        }

        fn release(&self) {
            self.resume.wait();
        }
    }

    struct WakeFlag(AtomicBool);

    impl Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn sequence(value: u64) -> CommitSequence {
        CommitSequence::new(value).expect("nonzero commit sequence")
    }

    fn poll_next(
        source: &mut dyn CommitNotificationSource,
    ) -> Poll<Result<AuthoritativeCommitNotification, AuthoritativeReadError>> {
        poll_next_with_waker(source, Waker::noop())
    }

    fn poll_next_with_waker(
        source: &mut dyn CommitNotificationSource,
        waker: &Waker,
    ) -> Poll<Result<AuthoritativeCommitNotification, AuthoritativeReadError>> {
        let mut future = source.next();
        let mut context = Context::from_waker(waker);
        future.as_mut().poll(&mut context)
    }

    #[test]
    fn subscriber_is_installed_before_publish_and_receives_the_exact_hint() {
        let hub = FirstCommitNotificationHub::new();
        let mut source = hub.subscribe(None).expect("subscriber");
        hub.publish_first_commit(sequence(7)).expect("publication");

        match poll_next(source.as_mut()) {
            Poll::Ready(Ok(AuthoritativeCommitNotification::Advanced(observed))) => {
                assert_eq!(observed, sequence(7));
            }
            _ => panic!("expected one advanced notification"),
        }
    }

    #[test]
    fn waiter_registration_racing_with_publish_cannot_miss_the_wake() {
        let waiter_hook = BlockingHook::new();
        let publish_hook = BlockingHook::new();
        let hub = FirstCommitNotificationHub::with_test_hooks(NotificationTestHooks {
            before_waiter_registration: Some(waiter_hook.callback()),
            before_publish_progress_lock: Some(publish_hook.callback()),
            ..NotificationTestHooks::default()
        });
        let mut source = hub.subscribe(None).expect("subscriber");
        let wake_flag = Arc::new(WakeFlag(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&wake_flag));

        thread::scope(|scope| {
            let poller = scope.spawn(|| poll_next_with_waker(source.as_mut(), &waker));
            waiter_hook.wait_until_reached();

            let publisher = scope.spawn(|| hub.publish_first_commit(sequence(1)));
            publish_hook.wait_until_reached();
            publish_hook.release();
            waiter_hook.release();

            assert!(matches!(poller.join().expect("poller task"), Poll::Pending));
            assert_eq!(publisher.join().expect("publisher task"), Ok(()));
        });

        assert!(wake_flag.0.load(Ordering::SeqCst));
        assert!(matches!(
            poll_next(source.as_mut()),
            Poll::Ready(Ok(AuthoritativeCommitNotification::Advanced(observed)))
                if observed == sequence(1)
        ));
    }

    #[test]
    fn subscriber_count_is_bounded_and_drop_releases_capacity() {
        let hub = FirstCommitNotificationHub::new();
        let mut subscribers = Vec::new();
        for _ in 0..MAX_FIRST_COMMIT_SUBSCRIBERS {
            subscribers.push(hub.subscribe(None).expect("bounded subscriber"));
        }
        assert_eq!(hub.active_subscribers(), Ok(MAX_FIRST_COMMIT_SUBSCRIBERS));
        assert!(hub.subscribe(None).is_err());

        drop(subscribers.pop());
        assert_eq!(
            hub.active_subscribers(),
            Ok(MAX_FIRST_COMMIT_SUBSCRIBERS - 1)
        );
        assert!(hub.subscribe(None).is_ok());
    }

    #[test]
    fn the_two_hundred_fifty_seventh_pending_hint_closes_as_lagged() {
        let hub = FirstCommitNotificationHub::new();
        let mut source = hub.subscribe(None).expect("subscriber");
        for value in 1..=FIRST_COMMIT_BUFFER_ITEMS + 1 {
            hub.publish_first_commit(sequence(value as u64))
                .expect("bounded publication");
        }

        assert_eq!(hub.active_subscribers(), Ok(0));
        match poll_next(source.as_mut()) {
            Poll::Ready(Ok(AuthoritativeCommitNotification::Lagged { resume_after })) => {
                assert_eq!(resume_after, FrontierPosition::BeforeFirst);
            }
            _ => panic!("expected one lagged notification"),
        }
    }

    #[test]
    fn acknowledgements_after_overflow_advance_the_lag_resume_position() {
        let hub = FirstCommitNotificationHub::new();
        let mut source = hub.subscribe(Some(sequence(3))).expect("subscriber");
        for value in 1..=FIRST_COMMIT_BUFFER_ITEMS + 1 {
            hub.publish_first_commit(sequence(value as u64))
                .expect("bounded publication");
        }
        source.acknowledge(sequence(4)).expect("contiguous ack");
        source.acknowledge(sequence(5)).expect("contiguous ack");

        match poll_next(source.as_mut()) {
            Poll::Ready(Ok(AuthoritativeCommitNotification::Lagged { resume_after })) => {
                assert_eq!(resume_after, FrontierPosition::AppliedThrough(sequence(5)));
            }
            _ => panic!("expected one lagged notification"),
        }
        assert_eq!(
            source.acknowledge(sequence(6)),
            Err(AuthoritativeReadError::Integrity)
        );
    }

    #[test]
    fn overflow_racing_with_an_in_flight_ack_preserves_the_safe_frontier() {
        let lagged_hook = BlockingHook::new();
        let hub = FirstCommitNotificationHub::with_test_hooks(NotificationTestHooks {
            after_lagged_pending: Some(lagged_hook.callback()),
            ..NotificationTestHooks::default()
        });
        let mut source = hub.subscribe(Some(sequence(3))).expect("subscriber");
        hub.publish_first_commit(sequence(4)).expect("publication");
        assert!(matches!(
            poll_next(source.as_mut()),
            Poll::Ready(Ok(AuthoritativeCommitNotification::Advanced(observed)))
                if observed == sequence(4)
        ));
        for value in 5..=FIRST_COMMIT_BUFFER_ITEMS + 4 {
            hub.publish_first_commit(sequence(value as u64))
                .expect("bounded publication");
        }
        let ack_started = Arc::new(Barrier::new(2));

        thread::scope(|scope| {
            let publisher = scope.spawn(|| {
                hub.publish_first_commit(sequence((FIRST_COMMIT_BUFFER_ITEMS + 5) as u64))
            });
            lagged_hook.wait_until_reached();

            let ack_started_in_task = Arc::clone(&ack_started);
            let source_for_ack = &mut source;
            let acknowledger = scope.spawn(move || {
                ack_started_in_task.wait();
                source_for_ack.acknowledge(sequence(4))
            });
            ack_started.wait();
            lagged_hook.release();

            assert_eq!(publisher.join().expect("publisher task"), Ok(()));
            assert_eq!(acknowledger.join().expect("acknowledger task"), Ok(()));
        });

        assert!(matches!(
            poll_next(source.as_mut()),
            Poll::Ready(Ok(AuthoritativeCommitNotification::Lagged {
                resume_after: FrontierPosition::AppliedThrough(observed),
            })) if observed == sequence(4)
        ));
        assert_eq!(
            source.acknowledge(sequence(5)),
            Err(AuthoritativeReadError::Integrity)
        );
    }

    #[test]
    fn acknowledgement_must_be_exactly_contiguous() {
        let hub = FirstCommitNotificationHub::new();
        let mut source = hub.subscribe(Some(sequence(8))).expect("subscriber");
        assert_eq!(
            source.acknowledge(sequence(10)),
            Err(AuthoritativeReadError::Integrity)
        );
        source.acknowledge(sequence(9)).expect("contiguous ack");
        assert_eq!(
            source.acknowledge(sequence(9)),
            Err(AuthoritativeReadError::Integrity)
        );
    }

    #[test]
    fn stale_and_duplicate_hints_remain_untrusted_hints() {
        let hub = FirstCommitNotificationHub::new();
        let mut source = hub.subscribe(Some(sequence(5))).expect("subscriber");
        hub.publish_first_commit(sequence(4)).expect("stale hint");
        hub.publish_first_commit(sequence(4))
            .expect("duplicate hint");

        for _ in 0..2 {
            match poll_next(source.as_mut()) {
                Poll::Ready(Ok(AuthoritativeCommitNotification::Advanced(observed))) => {
                    assert_eq!(observed, sequence(4));
                }
                _ => panic!("expected unchanged lower hint"),
            }
        }
    }

    #[test]
    fn shutdown_closes_sources_and_rejects_further_publication() {
        let hub = FirstCommitNotificationHub::new();
        let mut source = hub.subscribe(None).expect("subscriber");
        hub.publish_first_commit(sequence(1)).expect("publication");
        hub.shutdown().expect("shutdown");

        assert!(matches!(
            poll_next(source.as_mut()),
            Poll::Ready(Ok(AuthoritativeCommitNotification::Closed))
        ));
        assert_eq!(
            hub.publish_first_commit(sequence(2)),
            Err(ApplicationCommitNotificationError)
        );
        assert!(hub.subscribe(None).is_err());
    }

    #[test]
    fn shutdown_racing_with_an_in_flight_ack_allows_the_ack_before_closed_is_consumed() {
        let shutdown_hook = BlockingHook::new();
        let hub = FirstCommitNotificationHub::with_test_hooks(NotificationTestHooks {
            after_shutdown_pending: Some(shutdown_hook.callback()),
            ..NotificationTestHooks::default()
        });
        let mut source = hub.subscribe(None).expect("subscriber");
        hub.publish_first_commit(sequence(1)).expect("publication");
        assert!(matches!(
            poll_next(source.as_mut()),
            Poll::Ready(Ok(AuthoritativeCommitNotification::Advanced(observed)))
                if observed == sequence(1)
        ));
        let ack_started = Arc::new(Barrier::new(2));

        thread::scope(|scope| {
            let shutdown = scope.spawn(|| hub.shutdown());
            shutdown_hook.wait_until_reached();

            let ack_started_in_task = Arc::clone(&ack_started);
            let source_for_ack = &mut source;
            let acknowledger = scope.spawn(move || {
                ack_started_in_task.wait();
                source_for_ack.acknowledge(sequence(1))
            });
            ack_started.wait();
            shutdown_hook.release();

            assert_eq!(shutdown.join().expect("shutdown task"), Ok(()));
            assert_eq!(acknowledger.join().expect("acknowledger task"), Ok(()));
        });

        assert!(matches!(
            poll_next(source.as_mut()),
            Poll::Ready(Ok(AuthoritativeCommitNotification::Closed))
        ));
        assert_eq!(
            source.acknowledge(sequence(2)),
            Err(AuthoritativeReadError::Integrity)
        );
    }

    #[test]
    fn publish_racing_with_drop_releases_subscriber_capacity() {
        let publish_hook = BlockingHook::new();
        let hub = FirstCommitNotificationHub::with_test_hooks(NotificationTestHooks {
            after_publish_enqueue: Some(publish_hook.callback()),
            ..NotificationTestHooks::default()
        });
        let source = hub.subscribe(None).expect("subscriber");
        let drop_started = Arc::new(Barrier::new(2));

        thread::scope(|scope| {
            let publisher = scope.spawn(|| hub.publish_first_commit(sequence(1)));
            publish_hook.wait_until_reached();

            let drop_started_in_task = Arc::clone(&drop_started);
            let dropper = scope.spawn(move || {
                drop_started_in_task.wait();
                drop(source);
            });
            drop_started.wait();
            publish_hook.release();

            assert_eq!(publisher.join().expect("publisher task"), Ok(()));
            dropper.join().expect("dropper task");
        });

        assert_eq!(hub.active_subscribers(), Ok(0));
        assert!(hub.subscribe(None).is_ok());
    }

    #[test]
    fn pending_waiter_is_woken_by_shutdown_and_observes_closed() {
        let hub = FirstCommitNotificationHub::new();
        let mut source = hub.subscribe(None).expect("subscriber");
        let wake_flag = Arc::new(WakeFlag(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&wake_flag));
        assert!(matches!(
            poll_next_with_waker(source.as_mut(), &waker),
            Poll::Pending
        ));
        let shutdown_started = Arc::new(Barrier::new(2));

        thread::scope(|scope| {
            let shutdown_started_in_task = Arc::clone(&shutdown_started);
            let shutdown_hub = hub.clone();
            let shutdown = scope.spawn(move || {
                shutdown_started_in_task.wait();
                shutdown_hub.shutdown()
            });
            shutdown_started.wait();
            assert_eq!(shutdown.join().expect("shutdown task"), Ok(()));
        });

        assert!(wake_flag.0.load(Ordering::SeqCst));
        assert!(matches!(
            poll_next(source.as_mut()),
            Poll::Ready(Ok(AuthoritativeCommitNotification::Closed))
        ));
    }

    #[test]
    fn poisoned_hub_fails_the_commit_sink_closed() {
        let hub = FirstCommitNotificationHub::new();
        let poison = hub.clone();
        let result = catch_unwind(AssertUnwindSafe(move || {
            let _guard = poison.state.lock().expect("initial hub lock");
            panic!("poison test hub");
        }));
        assert!(result.is_err());
        assert_eq!(
            hub.publish_first_commit(sequence(1)),
            Err(ApplicationCommitNotificationError)
        );
    }

    #[test]
    fn debug_output_redacts_hub_state() {
        assert_eq!(
            format!("{:?}", FirstCommitNotificationHub::new()),
            "FirstCommitNotificationHub([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", NotificationHubError),
            "NotificationHubError([REDACTED])"
        );
    }
}
