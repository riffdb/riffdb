//! Bounded lost-wakeup-safe projection waiter notification.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

use riffdb_types::{FrontierPosition, ProjectionIdentity};

use crate::{ProjectionCoreError, ProjectionCoreErrorKind, ProjectionSchemaRegistry};

/// Maximum simultaneous projection wait registrations in one process.
pub const MAX_PROJECTION_WAITERS: usize = 256;

struct WaitState {
    identities: BTreeMap<ProjectionIdentity, IdentityWaitState>,
    active_waiters: usize,
    #[cfg(test)]
    blocking_wait_cycles: u64,
}

struct IdentityWaitState {
    epoch: u64,
    waiters: usize,
    active: bool,
}

struct WaitInner {
    state: Mutex<WaitState>,
    changed: Condvar,
}

/// Process-local notifier. Durable storage remains the authority after wakeup.
#[derive(Clone)]
pub struct ProjectionNotifier {
    inner: Arc<WaitInner>,
}

impl ProjectionNotifier {
    /// Builds a fixed, bounded identity set from the checked active registry.
    #[must_use]
    pub fn from_registry(registry: &ProjectionSchemaRegistry) -> Self {
        Self::from_identities(registry.iter().map(|schema| schema.identity().clone()))
    }

    fn from_identities(identities: impl IntoIterator<Item = ProjectionIdentity>) -> Self {
        Self {
            inner: Arc::new(WaitInner {
                state: Mutex::new(WaitState {
                    identities: identities
                        .into_iter()
                        .map(|identity| {
                            (
                                identity,
                                IdentityWaitState {
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

    /// Synchronizes active identities after a checked catalog activation.
    ///
    /// Existing registrations for retired identities are epoch-woken and their
    /// entries remain only until those registrations drain. New registrations
    /// use the fresh active registry immediately. The only temporary excess over
    /// the checked registry size is therefore bounded by
    /// [`MAX_PROJECTION_WAITERS`].
    pub fn synchronize_registry(
        &self,
        registry: &ProjectionSchemaRegistry,
    ) -> Result<ProjectionRegistrySync, ProjectionCoreError> {
        self.synchronize_identities(
            registry.iter().map(|schema| schema.identity().clone()),
            registry.len(),
        )
    }

    fn synchronize_identities(
        &self,
        identities: impl IntoIterator<Item = ProjectionIdentity>,
        expected_count: usize,
    ) -> Result<ProjectionRegistrySync, ProjectionCoreError> {
        let desired = identities.into_iter().collect::<BTreeSet<_>>();
        if desired.len() != expected_count {
            return Err(ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity));
        }
        let mut state = self.lock_state()?;
        let mut retired = 0usize;
        for (identity, tracked) in &mut state.identities {
            if desired.contains(identity) {
                tracked.active = true;
            } else if tracked.active {
                tracked.active = false;
                tracked.epoch = tracked
                    .epoch
                    .checked_add(1)
                    .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
                retired = retired
                    .checked_add(1)
                    .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
            }
        }
        state
            .identities
            .retain(|_, tracked| tracked.active || tracked.waiters != 0);

        let mut added = 0usize;
        for identity in desired {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                state.identities.entry(identity)
            {
                entry.insert(IdentityWaitState {
                    epoch: 0,
                    waiters: 0,
                    active: true,
                });
                added = added
                    .checked_add(1)
                    .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
            }
        }
        if state.identities.len()
            > expected_count
                .checked_add(state.active_waiters)
                .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?
        {
            return Err(ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity));
        }
        drop(state);
        if retired != 0 {
            self.inner.changed.notify_all();
        }
        Ok(ProjectionRegistrySync { added, retired })
    }

    /// Registers one bounded waiter before the caller performs its persisted read.
    pub fn register(
        &self,
        identity: ProjectionIdentity,
    ) -> Result<ProjectionWaitRegistration, ProjectionCoreError> {
        let mut state = self.lock_state()?;
        let observed_epoch = state
            .identities
            .get(&identity)
            .filter(|tracked| tracked.active)
            .map(|tracked| tracked.epoch)
            .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
        if state.active_waiters == MAX_PROJECTION_WAITERS {
            return Err(ProjectionCoreError::new(
                ProjectionCoreErrorKind::WaiterCapacityExceeded,
            ));
        }
        state.active_waiters += 1;
        let tracked = state
            .identities
            .get_mut(&identity)
            .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
        tracked.waiters = tracked
            .waiters
            .checked_add(1)
            .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
        drop(state);
        Ok(ProjectionWaitRegistration {
            notifier: self.clone(),
            identity,
            observed_epoch,
            active: true,
        })
    }

    /// Notifies waiters after a durable transition for one exact identity.
    pub fn notify(&self, identity: &ProjectionIdentity) -> Result<(), ProjectionCoreError> {
        let mut state = self.lock_state()?;
        let epoch = state
            .identities
            .get_mut(identity)
            .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
        epoch.epoch = epoch
            .epoch
            .checked_add(1)
            .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
        drop(state);
        self.inner.changed.notify_all();
        Ok(())
    }

    /// Creates a process-local cancellation signal bound to this notifier.
    ///
    /// Cancelling the signal wakes every condition-variable waiter, but does
    /// not advance a projection epoch or impersonate a durable transition.
    #[must_use]
    pub fn cancellation(&self) -> ProjectionWaitCancellation {
        ProjectionWaitCancellation {
            notifier: self.clone(),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn shares_inner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, WaitState>, ProjectionCoreError> {
        self.inner
            .state
            .lock()
            .map_err(|_| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))
    }

    fn release(&self, identity: &ProjectionIdentity) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.active_waiters = state.active_waiters.saturating_sub(1);
            if let Some(tracked) = state.identities.get_mut(identity) {
                tracked.waiters = tracked.waiters.saturating_sub(1);
                if !tracked.active && tracked.waiters == 0 {
                    state.identities.remove(identity);
                }
            }
        }
    }
}

impl std::fmt::Debug for ProjectionNotifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProjectionNotifier([PROCESS_LOCAL])")
    }
}

/// Payload-free result of one checked active-registry synchronization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionRegistrySync {
    added: usize,
    retired: usize,
}

impl ProjectionRegistrySync {
    /// Returns the number of newly tracked active identities.
    #[must_use]
    pub const fn added(self) -> usize {
        self.added
    }

    /// Returns the number of active identities retired and epoch-woken.
    #[must_use]
    pub const fn retired(self) -> usize {
        self.retired
    }
}

/// Result of one bounded notification wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionWake {
    /// A durable-transition notification occurred; the caller must reread storage.
    Notified,
    /// The absolute process-local deadline elapsed.
    TimedOut,
    /// The process-local request was cancelled.
    Cancelled,
}

/// Process-local cancellation signal tied to one projection notifier.
#[derive(Clone)]
pub struct ProjectionWaitCancellation {
    notifier: ProjectionNotifier,
    cancelled: Arc<AtomicBool>,
}

impl ProjectionWaitCancellation {
    /// Cancels the wait and wakes the notifier without changing any identity epoch.
    pub fn cancel(&self) -> Result<(), ProjectionCoreError> {
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

    pub(crate) fn belongs_to(&self, notifier: &ProjectionNotifier) -> bool {
        self.notifier.shares_inner(notifier)
    }
}

impl std::fmt::Debug for ProjectionWaitCancellation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProjectionWaitCancellation([PROCESS_LOCAL])")
    }
}

/// Move-only process-local registration for one exact projection identity.
pub struct ProjectionWaitRegistration {
    notifier: ProjectionNotifier,
    identity: ProjectionIdentity,
    observed_epoch: u64,
    active: bool,
}

impl ProjectionWaitRegistration {
    /// Blocks until the identity epoch changes or the absolute deadline elapses.
    ///
    /// The caller must reauthorize and reread one complete storage snapshot after
    /// `Notified`; a notification carries no row or frontier authority.
    pub fn wait(mut self, deadline: Instant) -> Result<ProjectionWake, ProjectionCoreError> {
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
        cancellation: &ProjectionWaitCancellation,
    ) -> Result<ProjectionWake, ProjectionCoreError> {
        if !cancellation.belongs_to(&self.notifier) {
            return Err(ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity));
        }
        self.wait_inner(deadline, Some(cancellation))
    }

    fn wait_inner(
        &mut self,
        deadline: Instant,
        cancellation: Option<&ProjectionWaitCancellation>,
    ) -> Result<ProjectionWake, ProjectionCoreError> {
        let mut state = self.notifier.lock_state()?;
        loop {
            if cancellation.is_some_and(ProjectionWaitCancellation::is_cancelled) {
                drop(state);
                self.release();
                return Ok(ProjectionWake::Cancelled);
            }
            let current = state
                .identities
                .get(&self.identity)
                .map_or(0, |tracked| tracked.epoch);
            if current != self.observed_epoch {
                drop(state);
                self.release();
                return Ok(ProjectionWake::Notified);
            }
            let now = Instant::now();
            if now >= deadline {
                drop(state);
                self.release();
                return Ok(ProjectionWake::TimedOut);
            }
            let duration = deadline.saturating_duration_since(now);
            #[cfg(test)]
            {
                state.blocking_wait_cycles = state
                    .blocking_wait_cycles
                    .checked_add(1)
                    .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
            }
            let (next, timeout) = self
                .notifier
                .inner
                .changed
                .wait_timeout(state, duration)
                .map_err(|_| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
            state = next;
            if timeout.timed_out()
                && !cancellation.is_some_and(ProjectionWaitCancellation::is_cancelled)
                && state
                    .identities
                    .get(&self.identity)
                    .map_or(0, |tracked| tracked.epoch)
                    == self.observed_epoch
            {
                drop(state);
                self.release();
                return Ok(ProjectionWake::TimedOut);
            }
        }
    }

    fn release(&mut self) {
        if self.active {
            self.active = false;
            self.notifier.release(&self.identity);
        }
    }
}

impl Drop for ProjectionWaitRegistration {
    fn drop(&mut self) {
        self.release();
    }
}

impl std::fmt::Debug for ProjectionWaitRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProjectionWaitRegistration([REDACTED])")
    }
}

/// One persisted observation paired with a pre-read waiter registration.
///
/// Register-before-read plus epoch comparison prevents a transition between
/// those steps from becoming a lost wakeup.
pub struct RegisteredProjectionObservation<T> {
    observation: T,
    registration: ProjectionWaitRegistration,
}

impl<T> RegisteredProjectionObservation<T> {
    /// Creates an observation from one pre-read registration.
    #[must_use]
    pub const fn new(observation: T, registration: ProjectionWaitRegistration) -> Self {
        Self {
            observation,
            registration,
        }
    }

    /// Borrows the persisted observation.
    #[must_use]
    pub const fn observation(&self) -> &T {
        &self.observation
    }

    /// Splits the persisted observation from its still-active registration.
    #[must_use]
    pub fn into_parts(self) -> (T, ProjectionWaitRegistration) {
        (self.observation, self.registration)
    }
}

#[allow(dead_code)]
fn _frontier_is_not_notification_authority(_: FrontierPosition) {}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{ContractLineage, ProjectionId, ProjectionPlanHash};

    fn identity(seed: u8) -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new("ProjectionTest").expect("lineage"),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([seed; 32]),
        )
    }

    fn notifier_for(identity: &ProjectionIdentity) -> ProjectionNotifier {
        ProjectionNotifier::from_identities([identity.clone()])
    }

    #[test]
    fn register_before_notify_cannot_lose_the_wakeup() {
        let selected = identity(1);
        let notifier = notifier_for(&selected);
        let registration = notifier.register(selected.clone()).expect("registration");
        notifier.notify(&selected).expect("notify");
        assert_eq!(
            registration.wait(Instant::now()).expect("wait"),
            ProjectionWake::Notified
        );
    }

    #[test]
    fn timeout_and_drop_release_bounded_capacity() {
        let selected = identity(2);
        let notifier = notifier_for(&selected);
        let mut registrations = (0..MAX_PROJECTION_WAITERS)
            .map(|_| notifier.register(selected.clone()).expect("registration"))
            .collect::<Vec<_>>();
        assert_eq!(
            notifier
                .register(selected.clone())
                .expect_err("bounded capacity")
                .kind(),
            ProjectionCoreErrorKind::WaiterCapacityExceeded
        );
        drop(registrations.pop());
        let registration = notifier.register(selected).expect("released capacity");
        assert_eq!(
            registration.wait(Instant::now()).expect("timeout"),
            ProjectionWake::TimedOut
        );
    }

    #[test]
    fn unknown_identities_fail_closed_without_growing_tracking_state() {
        let known = identity(7);
        let unknown = identity(8);
        let notifier = notifier_for(&known);

        assert_eq!(
            notifier
                .register(unknown.clone())
                .expect_err("unknown registration")
                .kind(),
            ProjectionCoreErrorKind::Integrity
        );
        assert_eq!(
            notifier
                .notify(&unknown)
                .expect_err("unknown notification")
                .kind(),
            ProjectionCoreErrorKind::Integrity
        );
        assert_eq!(
            notifier.lock_state().expect("state").identities.len(),
            1,
            "public operations cannot expand the registry-derived identity set"
        );

        let registration = notifier
            .register(known.clone())
            .expect("known registration");
        notifier.notify(&known).expect("known notification");
        assert_eq!(
            registration.wait(Instant::now()).expect("known wait"),
            ProjectionWake::Notified
        );
    }

    #[test]
    fn registry_sync_wakes_retired_waiters_and_admits_new_identities_in_place() {
        let retired = identity(9);
        let retained = identity(10);
        let added = identity(11);
        let notifier = ProjectionNotifier::from_identities([retired.clone(), retained.clone()]);
        let retired_wait = notifier
            .register(retired.clone())
            .expect("retired registration");
        let retained_wait = notifier
            .register(retained.clone())
            .expect("retained registration");

        assert_eq!(
            notifier
                .synchronize_identities([retained.clone(), added.clone()], 2)
                .expect("registry synchronization"),
            ProjectionRegistrySync {
                added: 1,
                retired: 1,
            }
        );
        assert_eq!(
            notifier
                .register(retired.clone())
                .expect_err("retired identity rejects new registrations")
                .kind(),
            ProjectionCoreErrorKind::Integrity
        );
        notifier
            .register(added.clone())
            .expect("new identity is immediately active");
        assert_eq!(
            retired_wait.wait(Instant::now()).expect("retirement wake"),
            ProjectionWake::Notified
        );
        assert!(
            !notifier
                .lock_state()
                .expect("state")
                .identities
                .contains_key(&retired),
            "retired identity is removed after its final registration drains"
        );

        notifier.notify(&retained).expect("retained notify");
        assert_eq!(
            retained_wait.wait(Instant::now()).expect("retained wake"),
            ProjectionWake::Notified
        );
        notifier.notify(&added).expect("added notify");
    }

    #[test]
    fn debug_output_contains_no_projection_identity() {
        let selected = identity(3);
        let notifier = notifier_for(&selected);
        let registration = notifier.register(selected).expect("registration");
        let cancellation = notifier.cancellation();
        assert_eq!(
            format!("{registration:?}"),
            "ProjectionWaitRegistration([REDACTED])"
        );
        assert_eq!(
            format!("{notifier:?}"),
            "ProjectionNotifier([PROCESS_LOCAL])"
        );
        assert_eq!(
            format!("{cancellation:?}"),
            "ProjectionWaitCancellation([PROCESS_LOCAL])"
        );
    }

    #[test]
    fn cancellation_wakes_without_impersonating_a_projection_notification() {
        let selected = identity(4);
        let notifier = notifier_for(&selected);
        let cancellation = notifier.cancellation();
        let registration = notifier.register(selected).expect("registration");
        cancellation.cancel().expect("cancel");
        assert_eq!(
            registration
                .wait_controlled(Instant::now(), &cancellation)
                .expect("controlled wait"),
            ProjectionWake::Cancelled
        );
    }

    #[test]
    fn cancellation_from_another_notifier_fails_closed() {
        let selected = identity(5);
        let notifier = notifier_for(&selected);
        let other = notifier_for(&selected);
        let registration = notifier.register(selected).expect("registration");
        assert_eq!(
            registration
                .wait_controlled(Instant::now(), &other.cancellation())
                .expect_err("notifier mismatch")
                .kind(),
            ProjectionCoreErrorKind::Integrity
        );
    }

    #[test]
    fn condition_variable_wake_without_epoch_change_is_spurious() {
        use std::thread;
        use std::time::Duration;

        let selected = identity(6);
        let notifier = notifier_for(&selected);
        let registration = notifier.register(selected.clone()).expect("registration");
        let waiter = thread::spawn(move || {
            registration
                .wait(Instant::now() + Duration::from_secs(30))
                .expect("wait")
        });

        assert!(wait_for_blocking_cycle(&notifier, 1));
        notifier.inner.changed.notify_all();
        let reentered_wait = wait_for_blocking_cycle(&notifier, 2);
        notifier.notify(&selected).expect("real notification");

        assert!(
            reentered_wait,
            "a condition-variable wake without an epoch change must reenter the wait"
        );
        assert_eq!(
            waiter.join().expect("waiter thread"),
            ProjectionWake::Notified
        );
    }

    fn wait_for_blocking_cycle(notifier: &ProjectionNotifier, expected: u64) -> bool {
        for _ in 0..10_000 {
            if notifier
                .lock_state()
                .is_ok_and(|state| state.blocking_wait_cycles >= expected)
            {
                return true;
            }
            std::thread::yield_now();
        }
        false
    }
}
