//! Feature-gated deterministic scheduling hooks for concurrency exploration.

/// A synchronization point reached by the production conflict manager.
///
/// The hook exposes only process-local waiter identifiers and never exposes a
/// raw conflict key. It is available only to crate tests and the `loom` or
/// `shuttle` feature suites.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConflictSchedulePoint {
    /// An acquisition is about to register its complete key set.
    BeforeRegistration,
    /// The waiter has been appended atomically to every requested FIFO.
    Enqueued,
    /// Registration has completed and no table mutex is held.
    RegistrationComplete,
    /// An eligible all-or-nothing grant is about to change state.
    BeforeGrant,
    /// The complete key set has changed to granted.
    Granted,
    /// The acquisition future installed its latest task waker.
    WakerRegistered,
    /// The future is about to register its task waker for cancellation.
    BeforeCancellationRegistration,
    /// The task waker is registered with the cancellation signal.
    CancellationRegistered,
    /// A poll observed the monotonic cancellation notification.
    CancellationObserved,
    /// A poll observed the current waiter state before dispatch.
    WaiterStateObserved,
    /// A granted waiter is about to transfer ownership to its lease.
    BeforeGrantConsumption,
    /// Grant ownership was transferred atomically to its lease.
    GrantConsumed,
    /// The manager is about to notify a registered waiter waker.
    BeforeWaiterWake,
    /// Waiter waker notification has returned.
    WaiterWakeComplete,
    /// A cancellation, deadline, or abandoned future is about to abort.
    BeforeAbort,
    /// Abort cleanup has removed the waiter or released its grant.
    Aborted,
    /// An owned capability is about to be released.
    BeforeRelease,
    /// The complete capability has changed to released.
    Released,
    /// A waiting acquisition was registered with the deadline driver.
    DeadlineRegistered,
    /// The deadline driver selected a waiter for timeout notification.
    DeadlineNotified,
    /// Eligible FIFO successors are about to be considered.
    BeforeSuccessorPromotion,
    /// Successor promotion and wakeups have completed.
    SuccessorPromotionComplete,
}

/// A deterministic scheduler callback used by Loom, Shuttle, and unit tests.
///
/// Implementations must not retain process locks or call back into the manager.
/// Yielding or recording the point is supported; production constructors never
/// install this hook.
pub trait DeterministicConflictScheduler: Send + Sync + 'static {
    /// Observes a production state-machine scheduling point.
    fn checkpoint(&self, point: ConflictSchedulePoint, waiter_id: u64);
}
