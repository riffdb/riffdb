//! Bounded lost-wakeup-safe columnar projection waiter notification.
//!
//! Twin of the projection-core notifier, keyed by projection name rather than
//! [`riffdb_types::ProjectionIdentity`]. Fixed at process startup for CP2;
//! no registry synchronization is required.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

/// Maximum simultaneous columnar wait registrations in one process.
pub const MAX_COLUMNAR_WAITERS: usize = 256;

/// Closed, redaction-safe failure for columnar waiter operations.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ColumnarNotificationErrorKind {
    /// Durable or process-local waiter state is inconsistent.
    Integrity,
    /// The bounded waiter registry has no remaining capacity.
    WaiterCapacityExceeded,
}

impl ColumnarNotificationErrorKind {
    /// Returns fixed safe text without identities or values.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::Integrity => "columnar notification state integrity failure",
            Self::WaiterCapacityExceeded => "columnar waiter capacity exceeded",
        }
    }
}

/// A typed columnar notification failure that never retains free text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnarNotificationError {
    kind: ColumnarNotificationErrorKind,
}

impl ColumnarNotificationError {
    /// Constructs an error from its closed safe classification.
    #[must_use]
    pub const fn new(kind: ColumnarNotificationErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the closed safe classification.
    #[must_use]
    pub const fn kind(self) -> ColumnarNotificationErrorKind {
        self.kind
    }
}

impl fmt::Display for ColumnarNotificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for ColumnarNotificationError {}

struct WaitState {
    names: BTreeMap<String, NameWaitState>,
    active_waiters: usize,
    #[cfg(test)]
    blocking_wait_cycles: u64,
}

struct NameWaitState {
    epoch: u64,
    waiters: usize,
    active: bool,
}

struct WaitInner {
    state: Mutex<WaitState>,
    changed: Condvar,
}

/// Process-local notifier. Durable projection storage remains the authority after wakeup.
#[derive(Clone)]
pub struct ColumnarNotifier {
    inner: Arc<WaitInner>,
}

impl ColumnarNotifier {
    /// Builds a fixed, bounded name set from startup registration.
    #[must_use]
    pub fn from_names(names: impl IntoIterator<Item = String>) -> Self {
        Self {
            inner: Arc::new(WaitInner {
                state: Mutex::new(WaitState {
                    names: names
                        .into_iter()
                        .map(|name| {
                            (
                                name,
                                NameWaitState {
                                    epoch: 0,
                                    waiters: 0,
                                    active: true,
                                },
                            )
                        })
                        .collect(),
                    active_waiters: 0,
                    #[cfg(test)]
                    blocking_wait_cycles: 0,
                }),
                changed: Condvar::new(),
            }),
        }
    }

    /// Registers one bounded waiter before the caller performs its published observation.
    pub fn register(
        &self,
        projection_name: String,
    ) -> Result<ColumnarWaitRegistration, ColumnarNotificationError> {
        let mut state = self.lock_state()?;
        let observed_epoch = state
            .names
            .get(&projection_name)
            .filter(|tracked| tracked.active)
            .map(|tracked| tracked.epoch)
            .ok_or_else(|| {
                ColumnarNotificationError::new(ColumnarNotificationErrorKind::Integrity)
            })?;
        if state.active_waiters == MAX_COLUMNAR_WAITERS {
            return Err(ColumnarNotificationError::new(
                ColumnarNotificationErrorKind::WaiterCapacityExceeded,
            ));
        }
        state.active_waiters += 1;
        let tracked = state.names.get_mut(&projection_name).ok_or_else(|| {
            ColumnarNotificationError::new(ColumnarNotificationErrorKind::Integrity)
        })?;
        tracked.waiters = tracked.waiters.checked_add(1).ok_or_else(|| {
            ColumnarNotificationError::new(ColumnarNotificationErrorKind::Integrity)
        })?;
        drop(state);
        Ok(ColumnarWaitRegistration {
            notifier: self.clone(),
            projection_name,
            observed_epoch,
            active: true,
        })
    }

    /// Notifies waiters after a durable transition for one exact projection name.
    pub fn notify(&self, projection_name: &str) -> Result<(), ColumnarNotificationError> {
        let mut state = self.lock_state()?;
        let tracked = state.names.get_mut(projection_name).ok_or_else(|| {
            ColumnarNotificationError::new(ColumnarNotificationErrorKind::Integrity)
        })?;
        tracked.epoch = tracked.epoch.checked_add(1).ok_or_else(|| {
            ColumnarNotificationError::new(ColumnarNotificationErrorKind::Integrity)
        })?;
        drop(state);
        self.inner.changed.notify_all();
        Ok(())
    }

    /// Creates a process-local cancellation signal bound to this notifier.
    ///
    /// Cancelling the signal wakes every condition-variable waiter, but does
    /// not advance a projection epoch or impersonate a durable transition.
    #[must_use]
    pub fn cancellation(&self) -> ColumnarWaitCancellation {
        ColumnarWaitCancellation {
            notifier: self.clone(),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn shares_inner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, WaitState>, ColumnarNotificationError> {
        self.inner
            .state
            .lock()
            .map_err(|_| ColumnarNotificationError::new(ColumnarNotificationErrorKind::Integrity))
    }

    fn release(&self, projection_name: &str) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.active_waiters = state.active_waiters.saturating_sub(1);
            if let Some(tracked) = state.names.get_mut(projection_name) {
                tracked.waiters = tracked.waiters.saturating_sub(1);
                if !tracked.active && tracked.waiters == 0 {
                    state.names.remove(projection_name);
                }
            }
        }
    }
}

impl fmt::Debug for ColumnarNotifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ColumnarNotifier([PROCESS_LOCAL])")
    }
}

/// Result of one bounded notification wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnarWake {
    /// A durable-transition notification occurred; the caller must re-observe.
    Notified,
    /// The absolute process-local deadline elapsed.
    TimedOut,
    /// The process-local request was cancelled.
    Cancelled,
}

/// Process-local cancellation signal tied to one columnar notifier.
#[derive(Clone)]
pub struct ColumnarWaitCancellation {
    notifier: ColumnarNotifier,
    cancelled: Arc<AtomicBool>,
}

impl ColumnarWaitCancellation {
    /// Cancels the wait and wakes the notifier without changing any name epoch.
    pub fn cancel(&self) -> Result<(), ColumnarNotificationError> {
        let state = self.notifier.lock_state()?;
        self.cancelled.store(true, Ordering::Release);
        drop(state);
        self.notifier.inner.changed.notify_all();
        Ok(())
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn belongs_to(&self, notifier: &ColumnarNotifier) -> bool {
        self.notifier.shares_inner(notifier)
    }
}

impl fmt::Debug for ColumnarWaitCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ColumnarWaitCancellation([PROCESS_LOCAL])")
    }
}

/// Move-only process-local registration for one exact projection name.
pub struct ColumnarWaitRegistration {
    notifier: ColumnarNotifier,
    projection_name: String,
    observed_epoch: u64,
    active: bool,
}

impl ColumnarWaitRegistration {
    /// Blocks until the name epoch changes or the absolute deadline elapses.
    ///
    /// The caller must reauthorize and re-observe after `Notified`; a
    /// notification carries no row or frontier authority.
    pub fn wait(mut self, deadline: Instant) -> Result<ColumnarWake, ColumnarNotificationError> {
        self.wait_inner(deadline, None)
    }

    /// Blocks until transition notification, cancellation, or the deadline.
    ///
    /// The cancellation signal must have been created by this registration's
    /// notifier, which closes the wakeup race under the same mutex/condition
    /// variable pair.
    pub fn wait_controlled(
        mut self,
        deadline: Instant,
        cancellation: &ColumnarWaitCancellation,
    ) -> Result<ColumnarWake, ColumnarNotificationError> {
        if !cancellation.belongs_to(&self.notifier) {
            return Err(ColumnarNotificationError::new(
                ColumnarNotificationErrorKind::Integrity,
            ));
        }
        self.wait_inner(deadline, Some(cancellation))
    }

    fn wait_inner(
        &mut self,
        deadline: Instant,
        cancellation: Option<&ColumnarWaitCancellation>,
    ) -> Result<ColumnarWake, ColumnarNotificationError> {
        let mut state = self.notifier.lock_state()?;
        loop {
            if cancellation.is_some_and(ColumnarWaitCancellation::is_cancelled) {
                drop(state);
                self.release();
                return Ok(ColumnarWake::Cancelled);
            }
            let current = state
                .names
                .get(&self.projection_name)
                .map_or(0, |tracked| tracked.epoch);
            if current != self.observed_epoch {
                drop(state);
                self.release();
                return Ok(ColumnarWake::Notified);
            }
            let now = Instant::now();
            if now >= deadline {
                drop(state);
                self.release();
                return Ok(ColumnarWake::TimedOut);
            }
            let duration = deadline.saturating_duration_since(now);
            #[cfg(test)]
            {
                state.blocking_wait_cycles =
                    state.blocking_wait_cycles.checked_add(1).ok_or_else(|| {
                        ColumnarNotificationError::new(ColumnarNotificationErrorKind::Integrity)
                    })?;
            }
            let (next, timeout) = self
                .notifier
                .inner
                .changed
                .wait_timeout(state, duration)
                .map_err(|_| {
                    ColumnarNotificationError::new(ColumnarNotificationErrorKind::Integrity)
                })?;
            state = next;
            if timeout.timed_out()
                && !cancellation.is_some_and(ColumnarWaitCancellation::is_cancelled)
                && state
                    .names
                    .get(&self.projection_name)
                    .map_or(0, |tracked| tracked.epoch)
                    == self.observed_epoch
            {
                drop(state);
                self.release();
                return Ok(ColumnarWake::TimedOut);
            }
        }
    }

    fn release(&mut self) {
        if self.active {
            self.active = false;
            self.notifier.release(&self.projection_name);
        }
    }
}

impl Drop for ColumnarWaitRegistration {
    fn drop(&mut self) {
        self.release();
    }
}

impl fmt::Debug for ColumnarWaitRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ColumnarWaitRegistration([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn notifier_for(name: &str) -> ColumnarNotifier {
        ColumnarNotifier::from_names([name.to_owned()])
    }

    #[test]
    fn register_before_notify_cannot_lose_the_wakeup() {
        let name = "board".to_owned();
        let notifier = notifier_for(&name);
        let registration = notifier.register(name.clone()).expect("registration");
        notifier.notify(&name).expect("notify");
        assert_eq!(
            registration.wait(Instant::now()).expect("wait"),
            ColumnarWake::Notified
        );
    }

    #[test]
    fn timeout_and_drop_release_bounded_capacity() {
        let name = "board".to_owned();
        let notifier = notifier_for(&name);
        let mut registrations = (0..MAX_COLUMNAR_WAITERS)
            .map(|_| notifier.register(name.clone()).expect("registration"))
            .collect::<Vec<_>>();
        assert_eq!(
            notifier
                .register(name.clone())
                .expect_err("bounded capacity")
                .kind(),
            ColumnarNotificationErrorKind::WaiterCapacityExceeded
        );
        drop(registrations.pop());
        let registration = notifier.register(name).expect("released capacity");
        assert_eq!(
            registration.wait(Instant::now()).expect("timeout"),
            ColumnarWake::TimedOut
        );
    }

    #[test]
    fn unknown_names_fail_closed_without_growing_tracking_state() {
        let known = "known".to_owned();
        let unknown = "unknown".to_owned();
        let notifier = notifier_for(&known);

        assert_eq!(
            notifier
                .register(unknown.clone())
                .expect_err("unknown registration")
                .kind(),
            ColumnarNotificationErrorKind::Integrity
        );
        assert_eq!(
            notifier
                .notify(&unknown)
                .expect_err("unknown notification")
                .kind(),
            ColumnarNotificationErrorKind::Integrity
        );
        assert_eq!(
            notifier.lock_state().expect("state").names.len(),
            1,
            "public operations cannot expand the startup-derived name set"
        );

        let registration = notifier
            .register(known.clone())
            .expect("known registration");
        notifier.notify(&known).expect("known notification");
        assert_eq!(
            registration.wait(Instant::now()).expect("known wait"),
            ColumnarWake::Notified
        );
    }

    #[test]
    fn debug_output_contains_no_projection_name() {
        let name = "board".to_owned();
        let notifier = notifier_for(&name);
        let registration = notifier.register(name).expect("registration");
        let cancellation = notifier.cancellation();
        assert_eq!(
            format!("{registration:?}"),
            "ColumnarWaitRegistration([REDACTED])"
        );
        assert_eq!(format!("{notifier:?}"), "ColumnarNotifier([PROCESS_LOCAL])");
        assert_eq!(
            format!("{cancellation:?}"),
            "ColumnarWaitCancellation([PROCESS_LOCAL])"
        );
    }

    #[test]
    fn cancellation_wakes_without_impersonating_a_notification() {
        let name = "board".to_owned();
        let notifier = notifier_for(&name);
        let cancellation = notifier.cancellation();
        let registration = notifier.register(name).expect("registration");
        cancellation.cancel().expect("cancel");
        assert_eq!(
            registration
                .wait_controlled(Instant::now(), &cancellation)
                .expect("controlled wait"),
            ColumnarWake::Cancelled
        );
    }

    #[test]
    fn cancellation_from_another_notifier_fails_closed() {
        let name = "board".to_owned();
        let notifier = notifier_for(&name);
        let other = notifier_for(&name);
        let registration = notifier.register(name).expect("registration");
        assert_eq!(
            registration
                .wait_controlled(Instant::now(), &other.cancellation())
                .expect_err("notifier mismatch")
                .kind(),
            ColumnarNotificationErrorKind::Integrity
        );
    }

    #[test]
    fn condition_variable_wake_without_epoch_change_is_spurious() {
        let name = "board".to_owned();
        let notifier = notifier_for(&name);
        let registration = notifier.register(name.clone()).expect("registration");
        let waiter = thread::spawn(move || {
            registration
                .wait(Instant::now() + Duration::from_secs(30))
                .expect("wait")
        });

        assert!(wait_for_blocking_cycle(&notifier, 1));
        notifier.inner.changed.notify_all();
        let reentered_wait = wait_for_blocking_cycle(&notifier, 2);
        notifier.notify(&name).expect("real notification");

        assert!(
            reentered_wait,
            "a condition-variable wake without an epoch change must reenter the wait"
        );
        assert_eq!(
            waiter.join().expect("waiter thread"),
            ColumnarWake::Notified
        );
    }

    fn wait_for_blocking_cycle(notifier: &ColumnarNotifier, expected: u64) -> bool {
        for _ in 0..10_000 {
            if notifier
                .lock_state()
                .is_ok_and(|state| state.blocking_wait_cycles >= expected)
            {
                return true;
            }
            thread::yield_now();
        }
        false
    }
}
