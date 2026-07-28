use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use riffdb_types::{ConflictKey, ConflictKeyHash, MAX_COMMAND_CONFLICT_KEYS_V1, hash_conflict_key};

use crate::cancellation::{CancellationRegistrationError, CancellationToken};
use crate::telemetry::{
    ConflictEvent, ConflictEventKind, ConflictObserver, ConflictTelemetry,
    DEFAULT_TELEMETRY_QUEUE_CAPACITY, NoopConflictObserver,
};
#[cfg(any(test, feature = "loom", feature = "shuttle"))]
use crate::testing::{ConflictSchedulePoint, DeterministicConflictScheduler};

const MAX_SHARDS: usize = 256;
const MAX_KEYS_PER_SHARD_LIMIT: usize = 4_096;
const MAX_WAITERS_LIMIT: usize = 65_536;
const MAX_WAITERS_PER_KEY_LIMIT: usize = 4_096;

macro_rules! checkpoint {
    ($inner:expr, $point:expr, $waiter_id:expr) => {
        #[cfg(any(test, feature = "loom", feature = "shuttle"))]
        $inner.checkpoint($point, $waiter_id)
    };
}

/// Runtime limits for one sharded conflict manager.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConflictManagerConfig {
    shard_count: usize,
    max_keys_per_acquisition: usize,
    max_keys_per_shard: usize,
    max_waiters: usize,
    max_waiters_per_key: usize,
}

impl ConflictManagerConfig {
    /// Constructs checked, process-local conflict-manager bounds.
    pub fn new(
        shard_count: usize,
        max_keys_per_acquisition: usize,
        max_keys_per_shard: usize,
        max_waiters: usize,
        max_waiters_per_key: usize,
    ) -> Result<Self, ConflictManagerConfigError> {
        check_config_bound("shard_count", shard_count, MAX_SHARDS)?;
        check_config_bound(
            "max_keys_per_acquisition",
            max_keys_per_acquisition,
            MAX_COMMAND_CONFLICT_KEYS_V1,
        )?;
        check_config_bound(
            "max_keys_per_shard",
            max_keys_per_shard,
            MAX_KEYS_PER_SHARD_LIMIT,
        )?;
        check_config_bound("max_waiters", max_waiters, MAX_WAITERS_LIMIT)?;
        check_config_bound(
            "max_waiters_per_key",
            max_waiters_per_key,
            MAX_WAITERS_PER_KEY_LIMIT,
        )?;
        if max_waiters_per_key > max_waiters {
            return Err(ConflictManagerConfigError::PerKeyWaitersExceedTotal {
                per_key: max_waiters_per_key,
                total: max_waiters,
            });
        }
        Ok(Self {
            shard_count,
            max_keys_per_acquisition,
            max_keys_per_shard,
            max_waiters,
            max_waiters_per_key,
        })
    }

    /// Returns the fixed number of table shards.
    #[must_use]
    pub const fn shard_count(self) -> usize {
        self.shard_count
    }

    /// Returns the maximum canonical keys in one acquisition.
    #[must_use]
    pub const fn max_keys_per_acquisition(self) -> usize {
        self.max_keys_per_acquisition
    }

    /// Returns the maximum live key entries in one shard.
    #[must_use]
    pub const fn max_keys_per_shard(self) -> usize {
        self.max_keys_per_shard
    }

    /// Returns the maximum number of queued acquisitions.
    #[must_use]
    pub const fn max_waiters(self) -> usize {
        self.max_waiters
    }

    /// Returns the maximum FIFO depth for one key.
    #[must_use]
    pub const fn max_waiters_per_key(self) -> usize {
        self.max_waiters_per_key
    }
}

impl Default for ConflictManagerConfig {
    fn default() -> Self {
        Self {
            shard_count: 64,
            max_keys_per_acquisition: MAX_COMMAND_CONFLICT_KEYS_V1,
            max_keys_per_shard: 1_024,
            max_waiters: 16_384,
            max_waiters_per_key: 1_024,
        }
    }
}

/// A rejected conflict-manager configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConflictManagerConfigError {
    /// A bound was zero or exceeded its hard process limit.
    InvalidBound {
        /// Stable field name.
        field: &'static str,
        /// Supplied value.
        actual: usize,
        /// Inclusive hard maximum.
        maximum: usize,
    },
    /// A per-key waiter limit exceeded the total waiter limit.
    PerKeyWaitersExceedTotal {
        /// Per-key limit.
        per_key: usize,
        /// Total limit.
        total: usize,
    },
}

impl fmt::Display for ConflictManagerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBound {
                field,
                actual,
                maximum,
            } => write!(
                formatter,
                "conflict-manager {field} must be in 1..={maximum}, received {actual}"
            ),
            Self::PerKeyWaitersExceedTotal { per_key, total } => write!(
                formatter,
                "per-key waiter limit {per_key} exceeds total waiter limit {total}"
            ),
        }
    }
}

impl Error for ConflictManagerConfigError {}

/// A failure to construct the process-local conflict manager.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConflictManagerBuildError {
    /// The operating environment could not start the one deadline worker.
    DeadlineWorkerUnavailable,
    /// The operating environment could not start the bounded telemetry worker.
    TelemetryWorkerUnavailable,
}

impl fmt::Display for ConflictManagerBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineWorkerUnavailable => {
                formatter.write_str("conflict deadline worker is unavailable")
            }
            Self::TelemetryWorkerUnavailable => {
                formatter.write_str("conflict telemetry worker is unavailable")
            }
        }
    }
}

impl Error for ConflictManagerBuildError {}

/// A typed, redaction-safe acquisition failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConflictError {
    /// Mutation acquisition requires at least one logical conflict key.
    EmptyKeySet,
    /// The raw input vector exceeded the hard preprocessing bound.
    InputKeyCountExceeded {
        /// Raw entry count before ordering or deduplication.
        actual: usize,
        /// Hard maximum raw entry count.
        maximum: usize,
    },
    /// The deduplicated key set exceeded the configured bound.
    TooManyKeys {
        /// Canonical deduplicated key count.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// The caller cancelled before it consumed a complete grant.
    Cancelled,
    /// The absolute monotonic acquisition deadline elapsed.
    DeadlineExceeded,
    /// The bounded global waiter capacity is full.
    WaiterCapacityExceeded {
        /// Configured maximum.
        maximum: usize,
    },
    /// One key's bounded FIFO is full.
    KeyQueueCapacityExceeded {
        /// Configured maximum.
        maximum: usize,
    },
    /// One shard's bounded live-key table is full.
    TableCapacityExceeded {
        /// Configured maximum entries per shard.
        maximum_per_shard: usize,
    },
    /// The process-local waiter identifier space was exhausted.
    IdentifierExhausted,
    /// One cancellation signal reached its checked pending-registration bound.
    CancellationRegistrationCapacityExceeded {
        /// Hard maximum pending acquisitions sharing one cancellation signal.
        maximum: usize,
    },
}

impl fmt::Display for ConflictError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyKeySet => {
                formatter.write_str("mutation acquisition requires a conflict key")
            }
            Self::InputKeyCountExceeded { actual, maximum } => write!(
                formatter,
                "acquisition input contains {actual} key entries but the hard maximum is {maximum}"
            ),
            Self::TooManyKeys { actual, maximum } => write!(
                formatter,
                "acquisition contains {actual} keys but the maximum is {maximum}"
            ),
            Self::Cancelled => formatter.write_str("conflict acquisition was cancelled"),
            Self::DeadlineExceeded => formatter.write_str("conflict acquisition deadline exceeded"),
            Self::WaiterCapacityExceeded { maximum } => {
                write!(formatter, "conflict waiter capacity {maximum} is full")
            }
            Self::KeyQueueCapacityExceeded { maximum } => {
                write!(formatter, "conflict-key queue capacity {maximum} is full")
            }
            Self::TableCapacityExceeded { maximum_per_shard } => write!(
                formatter,
                "conflict table shard capacity {maximum_per_shard} is full"
            ),
            Self::IdentifierExhausted => {
                formatter.write_str("conflict waiter identifier space exhausted")
            }
            Self::CancellationRegistrationCapacityExceeded { maximum } => write!(
                formatter,
                "cancellation registration capacity {maximum} is full"
            ),
        }
    }
}

impl Error for ConflictError {}

/// The runtime-neutral future returned by [`ConflictManager::acquire_mut`].
pub type AcquisitionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<MutationLease, ConflictError>> + Send + 'a>>;

/// Exclusive logical mutation-capability acquisition.
pub trait ConflictManager: Send + Sync {
    /// Acquires the complete canonical key set or exposes no capability.
    fn acquire_mut(
        &self,
        keys: Vec<ConflictKey>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> AcquisitionFuture<'_>;
}

/// A bounded conflict manager backed by independently synchronized table shards.
#[derive(Clone)]
pub struct ShardedConflictManager {
    inner: Arc<Inner>,
}

impl ShardedConflictManager {
    /// Creates a manager with checked bounds and no-op telemetry.
    pub fn new(config: ConflictManagerConfig) -> Result<Self, ConflictManagerBuildError> {
        Self::with_observer(config, Arc::new(NoopConflictObserver))
    }

    /// Creates a manager with a redaction-safe event observer.
    pub fn with_observer(
        config: ConflictManagerConfig,
        observer: Arc<dyn ConflictObserver>,
    ) -> Result<Self, ConflictManagerBuildError> {
        let telemetry = ConflictTelemetry::new(observer, DEFAULT_TELEMETRY_QUEUE_CAPACITY)
            .map_err(|_| ConflictManagerBuildError::TelemetryWorkerUnavailable)?;
        let inner = Arc::new(Inner {
            config,
            shards: (0..config.shard_count)
                .map(|_| Mutex::new(Shard::default()))
                .collect(),
            queued_waiters: AtomicUsize::new(0),
            next_waiter_id: Mutex::new(Some(1)),
            telemetry,
            #[cfg(any(test, feature = "loom", feature = "shuttle"))]
            scheduler: None,
            timer: TimerQueue::new(),
        });
        inner.timer.start(Arc::downgrade(&inner))?;
        Ok(Self { inner })
    }

    /// Returns the immutable process-local bounds.
    #[must_use]
    pub fn config(&self) -> ConflictManagerConfig {
        self.inner.config
    }

    /// Returns the number of redaction-safe observations dropped to preserve
    /// nonblocking lock progress after the bounded telemetry queue filled.
    #[must_use]
    pub fn dropped_telemetry_events(&self) -> usize {
        self.inner.telemetry.dropped()
    }

    /// Constructs the production manager with a deterministic scheduler and a
    /// manually driven deadline source for Loom, Shuttle, and unit tests.
    #[cfg(any(test, feature = "loom", feature = "shuttle"))]
    #[doc(hidden)]
    pub fn with_test_scheduler(
        config: ConflictManagerConfig,
        scheduler: Arc<dyn DeterministicConflictScheduler>,
    ) -> Result<(Self, ConflictTestDriver), ConflictManagerBuildError> {
        Self::build_test(config, ConflictTelemetry::disabled(), scheduler)
    }

    #[cfg(test)]
    fn with_test_scheduler_and_telemetry_capacity(
        config: ConflictManagerConfig,
        observer: Arc<dyn ConflictObserver>,
        scheduler: Arc<dyn DeterministicConflictScheduler>,
        telemetry_capacity: usize,
    ) -> Result<(Self, ConflictTestDriver), ConflictManagerBuildError> {
        let telemetry = ConflictTelemetry::new(observer, telemetry_capacity)
            .map_err(|_| ConflictManagerBuildError::TelemetryWorkerUnavailable)?;
        Self::build_test(config, telemetry, scheduler)
    }

    #[cfg(any(test, feature = "loom", feature = "shuttle"))]
    fn build_test(
        config: ConflictManagerConfig,
        telemetry: ConflictTelemetry,
        scheduler: Arc<dyn DeterministicConflictScheduler>,
    ) -> Result<(Self, ConflictTestDriver), ConflictManagerBuildError> {
        let inner = Arc::new(Inner {
            config,
            shards: (0..config.shard_count)
                .map(|_| Mutex::new(Shard::default()))
                .collect(),
            queued_waiters: AtomicUsize::new(0),
            next_waiter_id: Mutex::new(Some(1)),
            telemetry,
            scheduler: Some(scheduler),
            timer: TimerQueue::new(),
        });
        let driver = ConflictTestDriver {
            inner: Arc::downgrade(&inner),
        };
        Ok((Self { inner }, driver))
    }

    #[cfg(test)]
    fn queued_waiters(&self) -> usize {
        self.inner.queued_waiters.load(Ordering::Acquire)
    }
}

/// Manual deadline control for deterministic production-manager schedules.
#[cfg(any(test, feature = "loom", feature = "shuttle"))]
#[doc(hidden)]
#[derive(Clone)]
pub struct ConflictTestDriver {
    inner: Weak<Inner>,
}

#[cfg(any(test, feature = "loom", feature = "shuttle"))]
impl ConflictTestDriver {
    /// Delivers a timeout notification to a registered waiter.
    #[must_use]
    pub fn notify_deadline(&self, waiter_id: u64) -> bool {
        self.inner
            .upgrade()
            .is_some_and(|inner| inner.notify_deadline(waiter_id))
    }
}

impl fmt::Debug for ShardedConflictManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShardedConflictManager")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

impl ConflictManager for ShardedConflictManager {
    fn acquire_mut(
        &self,
        keys: Vec<ConflictKey>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> AcquisitionFuture<'_> {
        Box::pin(Acquisition::new(
            Arc::clone(&self.inner),
            keys,
            deadline,
            cancellation,
        ))
    }
}

/// An exclusive, non-cloneable, non-serializable mutation capability.
///
/// The `Rc` marker deliberately makes this value neither `Send` nor `Sync`:
/// the coordinator that consumes a grant cannot transfer it to another task or
/// thread. Dropping it releases every key exactly once.
///
/// ```compile_fail
/// use riffdb_conflict::MutationLease;
/// fn require_send<T: Send>() {}
/// require_send::<MutationLease>();
/// ```
///
/// ```compile_fail
/// use riffdb_conflict::MutationLease;
/// fn require_clone<T: Clone>() {}
/// require_clone::<MutationLease>();
/// ```
pub struct MutationLease {
    core: Option<LeaseCore>,
    not_transferable: PhantomData<Rc<()>>,
}

impl MutationLease {
    fn new(inner: Arc<Inner>, waiter: Arc<Waiter>) -> Self {
        Self {
            core: Some(LeaseCore { inner, waiter }),
            not_transferable: PhantomData,
        }
    }

    /// Returns the number of canonical deduplicated keys in this capability.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.core.as_ref().map_or(0, |core| core.waiter.keys.len())
    }

    /// Releases the complete capability before the end of its lexical scope.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if let Some(core) = self.core.take() {
            core.inner.release(&core.waiter);
        }
    }
}

impl fmt::Debug for MutationLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutationLease")
            .field("key_count", &self.key_count())
            .field("keys", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl Drop for MutationLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

struct LeaseCore {
    inner: Arc<Inner>,
    waiter: Arc<Waiter>,
}

struct Acquisition {
    inner: Arc<Inner>,
    waiter: Option<Arc<Waiter>>,
    cancellation: CancellationToken,
    cancellation_registration: Option<u64>,
    initial_error: Option<ConflictError>,
    registered: bool,
    completed: bool,
}

impl Acquisition {
    fn new(
        inner: Arc<Inner>,
        mut keys: Vec<ConflictKey>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Self {
        let raw_key_count = keys.len();
        let raw_count_error = (raw_key_count > MAX_COMMAND_CONFLICT_KEYS_V1).then_some(
            ConflictError::InputKeyCountExceeded {
                actual: raw_key_count,
                maximum: MAX_COMMAND_CONFLICT_KEYS_V1,
            },
        );
        if raw_count_error.is_none() {
            keys.sort();
            keys.dedup();
        }

        let initial_error = if let Some(error) = raw_count_error {
            Some(error)
        } else if keys.is_empty() {
            Some(ConflictError::EmptyKeySet)
        } else if keys.len() > inner.config.max_keys_per_acquisition {
            Some(ConflictError::TooManyKeys {
                actual: keys.len(),
                maximum: inner.config.max_keys_per_acquisition,
            })
        } else {
            None
        };

        let waiter = if initial_error.is_none() {
            match inner.next_waiter_id() {
                Ok(id) => Some(Arc::new(Waiter::new(
                    id,
                    keys,
                    deadline,
                    inner.config.shard_count,
                ))),
                Err(error) => {
                    return Self {
                        inner,
                        waiter: None,
                        cancellation,
                        cancellation_registration: None,
                        initial_error: Some(error),
                        registered: false,
                        completed: false,
                    };
                }
            }
        } else {
            None
        };

        Self {
            inner,
            waiter,
            cancellation,
            cancellation_registration: None,
            initial_error,
            registered: false,
            completed: false,
        }
    }

    fn finish(
        &mut self,
        result: Result<MutationLease, ConflictError>,
    ) -> Poll<Result<MutationLease, ConflictError>> {
        self.cancellation
            .unregister(&mut self.cancellation_registration);
        self.completed = true;
        Poll::Ready(result)
    }
}

impl Future for Acquisition {
    type Output = Result<MutationLease, ConflictError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.completed {
            panic!("conflict acquisition polled after completion");
        }
        if let Some(error) = self.initial_error.take() {
            return self.finish(Err(error));
        }

        let waiter = Arc::clone(
            self.waiter
                .as_ref()
                .expect("validated acquisition has a waiter"),
        );
        if self.cancellation.is_cancelled() {
            checkpoint!(
                self.inner,
                ConflictSchedulePoint::CancellationObserved,
                waiter.id
            );
            if self.registered {
                self.inner.abort(&waiter, AbortKind::Cancelled);
            }
            return self.finish(Err(ConflictError::Cancelled));
        }
        if Instant::now() >= waiter.deadline {
            if self.registered {
                self.inner.abort(&waiter, AbortKind::DeadlineExceeded);
            }
            return self.finish(Err(ConflictError::DeadlineExceeded));
        }

        if !self.registered {
            checkpoint!(
                self.inner,
                ConflictSchedulePoint::BeforeRegistration,
                waiter.id
            );
            match self.inner.register(&waiter) {
                Ok(registration) => {
                    self.registered = true;
                    checkpoint!(
                        self.inner,
                        ConflictSchedulePoint::RegistrationComplete,
                        waiter.id
                    );
                    self.inner.emit_many(
                        &waiter,
                        if registration.granted {
                            ConflictEventKind::Acquired
                        } else {
                            ConflictEventKind::Queued
                        },
                        &registration.queue_depths,
                    );
                    if !registration.granted {
                        self.inner.timer.insert(&waiter);
                        checkpoint!(
                            self.inner,
                            ConflictSchedulePoint::DeadlineRegistered,
                            waiter.id
                        );
                        if waiter.state() != WaiterState::Waiting {
                            self.inner.timer.remove(&waiter);
                        }
                    }
                }
                Err(error) => {
                    self.inner.emit_many(
                        &waiter,
                        ConflictEventKind::CapacityRejected,
                        &vec![0; waiter.keys.len()],
                    );
                    return self.finish(Err(error));
                }
            }
        }

        if self.cancellation.is_cancelled() {
            checkpoint!(
                self.inner,
                ConflictSchedulePoint::CancellationObserved,
                waiter.id
            );
            self.inner.abort(&waiter, AbortKind::Cancelled);
        } else if Instant::now() >= waiter.deadline {
            self.inner.abort(&waiter, AbortKind::DeadlineExceeded);
        }

        let waiter_state = waiter.state();
        checkpoint!(
            self.inner,
            ConflictSchedulePoint::WaiterStateObserved,
            waiter.id
        );
        match waiter_state {
            WaiterState::Granted => {
                checkpoint!(
                    self.inner,
                    ConflictSchedulePoint::BeforeGrantConsumption,
                    waiter.id
                );
                if waiter.transition(WaiterState::Granted, WaiterState::Consumed) {
                    checkpoint!(self.inner, ConflictSchedulePoint::GrantConsumed, waiter.id);
                    if self.cancellation.is_cancelled() {
                        checkpoint!(
                            self.inner,
                            ConflictSchedulePoint::CancellationObserved,
                            waiter.id
                        );
                        self.inner.release(&waiter);
                        return self.finish(Err(ConflictError::Cancelled));
                    }
                    if Instant::now() >= waiter.deadline {
                        self.inner.release(&waiter);
                        return self.finish(Err(ConflictError::DeadlineExceeded));
                    }
                    let inner = Arc::clone(&self.inner);
                    self.finish(Ok(MutationLease::new(inner, waiter)))
                } else {
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            WaiterState::Cancelled | WaiterState::Abandoned => {
                self.finish(Err(ConflictError::Cancelled))
            }
            WaiterState::TimedOut => self.finish(Err(ConflictError::DeadlineExceeded)),
            WaiterState::Waiting => {
                let cancellation = self.cancellation.clone();
                checkpoint!(
                    self.inner,
                    ConflictSchedulePoint::BeforeCancellationRegistration,
                    waiter.id
                );
                if let Err(CancellationRegistrationError::CapacityExceeded { maximum }) =
                    cancellation.register(&mut self.cancellation_registration, context.waker())
                {
                    self.inner.abort(&waiter, AbortKind::Abandoned);
                    return self.finish(Err(
                        ConflictError::CancellationRegistrationCapacityExceeded { maximum },
                    ));
                }
                checkpoint!(
                    self.inner,
                    ConflictSchedulePoint::CancellationRegistered,
                    waiter.id
                );
                waiter.register_waker(context.waker());
                checkpoint!(
                    self.inner,
                    ConflictSchedulePoint::WakerRegistered,
                    waiter.id
                );
                if self.cancellation.is_cancelled()
                    || Instant::now() >= waiter.deadline
                    || waiter.state() != WaiterState::Waiting
                {
                    context.waker().wake_by_ref();
                }
                Poll::Pending
            }
            WaiterState::Consumed | WaiterState::Released => {
                panic!("conflict acquisition lost ownership of its grant")
            }
        }
    }
}

impl Drop for Acquisition {
    fn drop(&mut self) {
        self.cancellation
            .unregister(&mut self.cancellation_registration);
        if self.registered
            && !self.completed
            && let Some(waiter) = &self.waiter
        {
            self.inner.abort(waiter, AbortKind::Abandoned);
        }
    }
}

struct Inner {
    config: ConflictManagerConfig,
    shards: Vec<Mutex<Shard>>,
    queued_waiters: AtomicUsize,
    next_waiter_id: Mutex<Option<u64>>,
    telemetry: ConflictTelemetry,
    #[cfg(any(test, feature = "loom", feature = "shuttle"))]
    scheduler: Option<Arc<dyn DeterministicConflictScheduler>>,
    timer: TimerQueue,
}

impl Inner {
    fn next_waiter_id(&self) -> Result<u64, ConflictError> {
        let mut next = lock_unpoisoned(&self.next_waiter_id);
        let id = next.ok_or(ConflictError::IdentifierExhausted)?;
        *next = id.checked_add(1);
        Ok(id)
    }

    fn register(&self, waiter: &Arc<Waiter>) -> Result<Registration, ConflictError> {
        let mut guards = self.lock_shards(&waiter.shards);
        let mut new_entries = BTreeMap::<usize, usize>::new();

        for wait_key in waiter.keys.iter() {
            let shard = shard_mut(&mut guards, wait_key.shard);
            match shard.keys.get(&wait_key.key) {
                Some(state) if state.waiters.len() >= self.config.max_waiters_per_key => {
                    return Err(ConflictError::KeyQueueCapacityExceeded {
                        maximum: self.config.max_waiters_per_key,
                    });
                }
                Some(_) => {}
                None => *new_entries.entry(wait_key.shard).or_default() += 1,
            }
        }

        for (shard_index, additional) in new_entries {
            if shard_mut(&mut guards, shard_index).keys.len() + additional
                > self.config.max_keys_per_shard
            {
                return Err(ConflictError::TableCapacityExceeded {
                    maximum_per_shard: self.config.max_keys_per_shard,
                });
            }
        }

        self.queued_waiters
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.config.max_waiters).then_some(current + 1)
            })
            .map_err(|_| ConflictError::WaiterCapacityExceeded {
                maximum: self.config.max_waiters,
            })?;
        waiter.queued.store(true, Ordering::Release);

        let mut queue_depths = Vec::with_capacity(waiter.keys.len());
        for wait_key in waiter.keys.iter() {
            let state = shard_mut(&mut guards, wait_key.shard)
                .keys
                .entry(wait_key.key.clone())
                .or_default();
            state.waiters.push_back(Arc::clone(waiter));
            queue_depths.push(state.waiters.len());
        }
        checkpoint!(self, ConflictSchedulePoint::Enqueued, waiter.id);

        let granted = self.try_grant_locked(waiter, &mut guards);
        if granted {
            for (depth, wait_key) in queue_depths.iter_mut().zip(waiter.keys.iter()) {
                *depth = shard_mut(&mut guards, wait_key.shard)
                    .keys
                    .get(&wait_key.key)
                    .map_or(0, |state| state.waiters.len());
            }
        }
        drop(guards);

        Ok(Registration {
            granted,
            queue_depths,
        })
    }

    fn try_grant(&self, waiter: &Arc<Waiter>) -> bool {
        if waiter.state() != WaiterState::Waiting {
            return false;
        }
        let mut guards = self.lock_shards(&waiter.shards);
        let granted = self.try_grant_locked(waiter, &mut guards);
        let depths = granted.then(|| {
            waiter
                .keys
                .iter()
                .map(|wait_key| {
                    shard_mut(&mut guards, wait_key.shard)
                        .keys
                        .get(&wait_key.key)
                        .map_or(0, |state| state.waiters.len())
                })
                .collect::<Vec<_>>()
        });
        drop(guards);

        if let Some(depths) = depths {
            self.timer.remove(waiter);
            checkpoint!(self, ConflictSchedulePoint::BeforeWaiterWake, waiter.id);
            waiter.wake();
            checkpoint!(self, ConflictSchedulePoint::WaiterWakeComplete, waiter.id);
            self.emit_many(waiter, ConflictEventKind::Acquired, &depths);
        }
        granted
    }

    fn try_grant_locked(
        &self,
        waiter: &Arc<Waiter>,
        guards: &mut [(usize, MutexGuard<'_, Shard>)],
    ) -> bool {
        if waiter.state() != WaiterState::Waiting {
            return false;
        }
        let eligible = waiter.keys.iter().all(|wait_key| {
            let state = shard_mut(guards, wait_key.shard)
                .keys
                .get(&wait_key.key)
                .expect("registered waiter key exists");
            state.holder.is_none()
                && state
                    .waiters
                    .front()
                    .is_some_and(|front| front.id == waiter.id)
        });
        if !eligible {
            return false;
        }

        checkpoint!(self, ConflictSchedulePoint::BeforeGrant, waiter.id);

        for wait_key in waiter.keys.iter() {
            let state = shard_mut(guards, wait_key.shard)
                .keys
                .get_mut(&wait_key.key)
                .expect("registered waiter key exists");
            let front = state
                .waiters
                .pop_front()
                .expect("eligible waiter is at the queue front");
            debug_assert_eq!(front.id, waiter.id);
            state.holder = Some(waiter.id);
        }
        let transitioned = waiter.transition(WaiterState::Waiting, WaiterState::Granted);
        debug_assert!(transitioned);
        self.finish_queued(waiter);
        checkpoint!(self, ConflictSchedulePoint::Granted, waiter.id);
        true
    }

    fn abort(&self, waiter: &Arc<Waiter>, kind: AbortKind) -> bool {
        checkpoint!(self, ConflictSchedulePoint::BeforeAbort, waiter.id);
        let mut guards = self.lock_shards(&waiter.shards);
        let state = waiter.state();
        let target = kind.waiter_state();
        let changed = match state {
            WaiterState::Waiting => {
                if waiter.transition(WaiterState::Waiting, target) {
                    for wait_key in waiter.keys.iter() {
                        if let Some(key_state) = shard_mut(&mut guards, wait_key.shard)
                            .keys
                            .get_mut(&wait_key.key)
                        {
                            key_state.waiters.retain(|queued| queued.id != waiter.id);
                        }
                    }
                    true
                } else {
                    false
                }
            }
            WaiterState::Granted => {
                if waiter.transition(WaiterState::Granted, target) {
                    for wait_key in waiter.keys.iter() {
                        if let Some(key_state) = shard_mut(&mut guards, wait_key.shard)
                            .keys
                            .get_mut(&wait_key.key)
                            && key_state.holder == Some(waiter.id)
                        {
                            key_state.holder = None;
                        }
                    }
                    true
                } else {
                    false
                }
            }
            WaiterState::Cancelled
            | WaiterState::TimedOut
            | WaiterState::Abandoned
            | WaiterState::Consumed
            | WaiterState::Released => false,
        };
        if !changed {
            return false;
        }
        if state == WaiterState::Waiting {
            self.finish_queued(waiter);
        }

        let (candidates, depths) = collect_candidates_and_cleanup(&mut guards, waiter);
        drop(guards);
        self.timer.remove(waiter);
        checkpoint!(self, ConflictSchedulePoint::BeforeWaiterWake, waiter.id);
        waiter.wake();
        checkpoint!(self, ConflictSchedulePoint::WaiterWakeComplete, waiter.id);
        checkpoint!(
            self,
            ConflictSchedulePoint::BeforeSuccessorPromotion,
            waiter.id
        );
        self.try_candidates(candidates);
        checkpoint!(
            self,
            ConflictSchedulePoint::SuccessorPromotionComplete,
            waiter.id
        );
        checkpoint!(self, ConflictSchedulePoint::Aborted, waiter.id);
        self.emit_many(waiter, kind.event_kind(), &depths);
        true
    }

    fn release(&self, waiter: &Arc<Waiter>) -> bool {
        checkpoint!(self, ConflictSchedulePoint::BeforeRelease, waiter.id);
        let mut guards = self.lock_shards(&waiter.shards);
        if !waiter.transition(WaiterState::Consumed, WaiterState::Released) {
            return false;
        }
        for wait_key in waiter.keys.iter() {
            if let Some(key_state) = shard_mut(&mut guards, wait_key.shard)
                .keys
                .get_mut(&wait_key.key)
                && key_state.holder == Some(waiter.id)
            {
                key_state.holder = None;
            }
        }
        let (candidates, depths) = collect_candidates_and_cleanup(&mut guards, waiter);
        drop(guards);
        checkpoint!(
            self,
            ConflictSchedulePoint::BeforeSuccessorPromotion,
            waiter.id
        );
        self.try_candidates(candidates);
        checkpoint!(
            self,
            ConflictSchedulePoint::SuccessorPromotionComplete,
            waiter.id
        );
        checkpoint!(self, ConflictSchedulePoint::Released, waiter.id);
        self.emit_many(waiter, ConflictEventKind::Released, &depths);
        true
    }

    fn try_candidates(&self, candidates: Vec<Arc<Waiter>>) {
        for candidate in candidates {
            self.try_grant(&candidate);
        }
    }

    fn finish_queued(&self, waiter: &Waiter) {
        if waiter.queued.swap(false, Ordering::AcqRel) {
            let previous = self.queued_waiters.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(previous > 0);
        }
    }

    fn emit_many(&self, waiter: &Waiter, kind: ConflictEventKind, depths: &[usize]) {
        let duration = Instant::now().saturating_duration_since(waiter.started_at);
        let total_queue_depth = self.queued_waiters.load(Ordering::Acquire);
        for (index, wait_key) in waiter.keys.iter().enumerate() {
            let event = ConflictEvent::new(
                kind,
                wait_key.hash,
                duration,
                depths.get(index).copied().unwrap_or(0),
                total_queue_depth,
                waiter.keys.len(),
            );
            self.telemetry.emit(event);
        }
    }

    #[cfg(any(test, feature = "loom", feature = "shuttle"))]
    fn checkpoint(&self, point: ConflictSchedulePoint, waiter_id: u64) {
        if let Some(scheduler) = &self.scheduler {
            scheduler.checkpoint(point, waiter_id);
        }
    }

    #[cfg(any(test, feature = "loom", feature = "shuttle"))]
    fn notify_deadline(&self, waiter_id: u64) -> bool {
        let Some(waiter) = self.timer.take_waiter(waiter_id) else {
            return false;
        };
        checkpoint!(self, ConflictSchedulePoint::DeadlineNotified, waiter.id);
        self.abort(&waiter, AbortKind::DeadlineExceeded)
    }

    fn lock_shards(&self, shard_indices: &[usize]) -> Vec<(usize, MutexGuard<'_, Shard>)> {
        shard_indices
            .iter()
            .map(|index| (*index, lock_unpoisoned(&self.shards[*index])))
            .collect()
    }
}

#[derive(Default)]
struct Shard {
    keys: BTreeMap<ConflictKey, KeyState>,
}

#[derive(Default)]
struct KeyState {
    holder: Option<u64>,
    waiters: VecDeque<Arc<Waiter>>,
}

struct Waiter {
    id: u64,
    keys: Box<[WaitKey]>,
    shards: Box<[usize]>,
    started_at: Instant,
    deadline: Instant,
    state: AtomicU8,
    queued: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Waiter {
    fn new(id: u64, keys: Vec<ConflictKey>, deadline: Instant, shard_count: usize) -> Self {
        let keys = keys
            .into_iter()
            .map(|key| {
                let hash = hash_conflict_key(key.as_bytes());
                let shard = shard_for_hash(hash, shard_count);
                WaitKey { key, hash, shard }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let shards = keys
            .iter()
            .map(|key| key.shard)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            id,
            keys,
            shards,
            started_at: Instant::now(),
            deadline,
            state: AtomicU8::new(WaiterState::Waiting as u8),
            queued: AtomicBool::new(false),
            waker: Mutex::new(None),
        }
    }

    fn state(&self) -> WaiterState {
        WaiterState::from_u8(self.state.load(Ordering::Acquire))
            .expect("conflict waiter state is internally valid")
    }

    fn transition(&self, from: WaiterState, to: WaiterState) -> bool {
        self.state
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn register_waker(&self, waker: &Waker) {
        let mut slot = lock_unpoisoned(&self.waker);
        if slot
            .as_ref()
            .is_none_or(|current| !current.will_wake(waker))
        {
            *slot = Some(waker.clone());
        }
    }

    fn wake(&self) {
        if let Some(waker) = lock_unpoisoned(&self.waker).take() {
            waker.wake();
        }
    }
}

impl fmt::Debug for Waiter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Waiter")
            .field("key_count", &self.keys.len())
            .field("keys", &"[REDACTED]")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

struct WaitKey {
    key: ConflictKey,
    hash: ConflictKeyHash,
    shard: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum WaiterState {
    Waiting = 1,
    Granted = 2,
    Consumed = 3,
    Cancelled = 4,
    TimedOut = 5,
    Abandoned = 6,
    Released = 7,
}

impl WaiterState {
    const fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Waiting),
            2 => Some(Self::Granted),
            3 => Some(Self::Consumed),
            4 => Some(Self::Cancelled),
            5 => Some(Self::TimedOut),
            6 => Some(Self::Abandoned),
            7 => Some(Self::Released),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
enum AbortKind {
    Cancelled,
    DeadlineExceeded,
    Abandoned,
}

impl AbortKind {
    const fn waiter_state(self) -> WaiterState {
        match self {
            Self::Cancelled => WaiterState::Cancelled,
            Self::DeadlineExceeded => WaiterState::TimedOut,
            Self::Abandoned => WaiterState::Abandoned,
        }
    }

    const fn event_kind(self) -> ConflictEventKind {
        match self {
            Self::Cancelled | Self::Abandoned => ConflictEventKind::Cancelled,
            Self::DeadlineExceeded => ConflictEventKind::DeadlineExceeded,
        }
    }
}

struct Registration {
    granted: bool,
    queue_depths: Vec<usize>,
}

fn collect_candidates_and_cleanup(
    guards: &mut [(usize, MutexGuard<'_, Shard>)],
    waiter: &Waiter,
) -> (Vec<Arc<Waiter>>, Vec<usize>) {
    let mut candidates = BTreeMap::new();
    let mut depths = Vec::with_capacity(waiter.keys.len());
    for wait_key in waiter.keys.iter() {
        let shard = shard_mut(guards, wait_key.shard);
        let mut remove = false;
        let depth = if let Some(state) = shard.keys.get(&wait_key.key) {
            if let Some(candidate) = state.waiters.front() {
                candidates
                    .entry(candidate.id)
                    .or_insert_with(|| Arc::clone(candidate));
            }
            remove = state.holder.is_none() && state.waiters.is_empty();
            state.waiters.len()
        } else {
            0
        };
        depths.push(depth);
        if remove {
            shard.keys.remove(&wait_key.key);
        }
    }
    (candidates.into_values().collect(), depths)
}

fn shard_mut<'a>(
    guards: &'a mut [(usize, MutexGuard<'_, Shard>)],
    shard_index: usize,
) -> &'a mut Shard {
    let position = guards
        .binary_search_by_key(&shard_index, |(index, _)| *index)
        .expect("requested shard is locked");
    &mut guards[position].1
}

fn shard_for_hash(hash: ConflictKeyHash, shard_count: usize) -> usize {
    let bytes = hash.as_bytes();
    let prefix = u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    (prefix % shard_count as u64) as usize
}

fn check_config_bound(
    field: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), ConflictManagerConfigError> {
    if actual == 0 || actual > maximum {
        Err(ConflictManagerConfigError::InvalidBound {
            field,
            actual,
            maximum,
        })
    } else {
        Ok(())
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct TimerQueue {
    shared: Arc<TimerShared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl TimerQueue {
    fn new() -> Self {
        Self {
            shared: Arc::new(TimerShared::default()),
            worker: Mutex::new(None),
        }
    }

    fn start(&self, inner: Weak<Inner>) -> Result<(), ConflictManagerBuildError> {
        let worker_shared = Arc::clone(&self.shared);
        let worker = thread::Builder::new()
            .name("riffdb-conflict-deadlines".to_owned())
            .spawn(move || timer_worker(worker_shared, inner))
            .map_err(|_| ConflictManagerBuildError::DeadlineWorkerUnavailable)?;
        *lock_unpoisoned(&self.worker) = Some(worker);
        Ok(())
    }

    fn insert(&self, waiter: &Arc<Waiter>) {
        let mut state = lock_unpoisoned(&self.shared.state);
        if !state.shutdown {
            state
                .deadlines
                .insert((waiter.deadline, waiter.id), Arc::downgrade(waiter));
            self.shared.changed.notify_one();
        }
    }

    fn remove(&self, waiter: &Waiter) {
        let removed = lock_unpoisoned(&self.shared.state)
            .deadlines
            .remove(&(waiter.deadline, waiter.id))
            .is_some();
        if removed {
            self.shared.changed.notify_one();
        }
    }

    #[cfg(any(test, feature = "loom", feature = "shuttle"))]
    fn take_waiter(&self, waiter_id: u64) -> Option<Arc<Waiter>> {
        let mut state = lock_unpoisoned(&self.shared.state);
        let key = state
            .deadlines
            .keys()
            .find(|(_, id)| *id == waiter_id)
            .copied()?;
        state.deadlines.remove(&key).and_then(|weak| weak.upgrade())
    }

    #[cfg(test)]
    fn expire_now(&self, waiter: &Arc<Waiter>) {
        let mut state = lock_unpoisoned(&self.shared.state);
        state.deadlines.remove(&(waiter.deadline, waiter.id));
        state
            .deadlines
            .insert((Instant::now(), waiter.id), Arc::downgrade(waiter));
        self.shared.changed.notify_one();
    }
}

impl Drop for TimerQueue {
    fn drop(&mut self) {
        {
            let mut state = lock_unpoisoned(&self.shared.state);
            state.shutdown = true;
            state.deadlines.clear();
        }
        self.shared.changed.notify_one();

        if let Some(worker) = lock_unpoisoned(&self.worker).take()
            && worker.thread().id() != thread::current().id()
        {
            let _ = worker.join();
        }
    }
}

#[derive(Default)]
struct TimerShared {
    state: Mutex<TimerState>,
    changed: Condvar,
}

#[derive(Default)]
struct TimerState {
    deadlines: BTreeMap<(Instant, u64), Weak<Waiter>>,
    shutdown: bool,
}

fn timer_worker(shared: Arc<TimerShared>, inner: Weak<Inner>) {
    loop {
        let waiter = {
            let mut state = lock_unpoisoned(&shared.state);
            loop {
                if state.shutdown {
                    return;
                }
                let Some((&(deadline, id), _)) = state.deadlines.first_key_value() else {
                    state = wait_unpoisoned(&shared.changed, state);
                    continue;
                };
                let now = Instant::now();
                if deadline <= now {
                    let waiter = state
                        .deadlines
                        .remove(&(deadline, id))
                        .and_then(|weak| weak.upgrade());
                    break waiter;
                }
                let duration = deadline.saturating_duration_since(now);
                let (next, _) = wait_timeout_unpoisoned(&shared.changed, state, duration);
                state = next;
            }
        };

        if let Some(waiter) = waiter {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            checkpoint!(inner, ConflictSchedulePoint::DeadlineNotified, waiter.id);
            inner.abort(&waiter, AbortKind::DeadlineExceeded);
        }
    }
}

fn wait_unpoisoned<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn wait_timeout_unpoisoned<'a, T>(
    condvar: &Condvar,
    guard: MutexGuard<'a, T>,
    duration: Duration,
) -> (MutexGuard<'a, T>, std::sync::WaitTimeoutResult) {
    condvar
        .wait_timeout(guard, duration)
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, Weak};
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread;
    use std::time::{Duration, Instant};

    use riffdb_types::{
        AggregateTypeId, ConflictKey, ConflictKeyBuilder, MAX_COMMAND_CONFLICT_KEYS_V1,
    };

    use super::{
        Acquisition, ConflictError, ConflictManager, ConflictManagerConfig,
        ConflictManagerConfigError, ShardedConflictManager, WaiterState, lock_unpoisoned,
        shard_for_hash,
    };
    use crate::{
        CancellationToken, ConflictEvent, ConflictEventKind, ConflictObserver,
        ConflictSchedulePoint, DeterministicConflictScheduler,
    };

    const LONG_DEADLINE: Duration = Duration::from_secs(60);

    #[test]
    fn configuration_is_checked_and_bounded() {
        let over_command_limit = MAX_COMMAND_CONFLICT_KEYS_V1 + 1;

        assert_eq!(
            ConflictManagerConfig::new(0, 1, 1, 1, 1),
            Err(ConflictManagerConfigError::InvalidBound {
                field: "shard_count",
                actual: 0,
                maximum: 256,
            })
        );
        assert_eq!(
            ConflictManagerConfig::new(1, 1, 1, 1, 2),
            Err(ConflictManagerConfigError::PerKeyWaitersExceedTotal {
                per_key: 2,
                total: 1,
            })
        );
        assert_eq!(
            ConflictManagerConfig::new(1, over_command_limit, 1, 1, 1),
            Err(ConflictManagerConfigError::InvalidBound {
                field: "max_keys_per_acquisition",
                actual: over_command_limit,
                maximum: MAX_COMMAND_CONFLICT_KEYS_V1,
            })
        );

        let lower = ConflictManagerConfig::new(1, 1, 1, 1, 1).expect("lower bound is allowed");
        assert_eq!(lower.max_keys_per_acquisition(), 1);
        assert_eq!(
            ConflictManagerConfig::default().max_keys_per_acquisition(),
            MAX_COMMAND_CONFLICT_KEYS_V1
        );
    }

    #[test]
    fn empty_and_oversized_sets_fail_before_registration() {
        let config = ConflictManagerConfig::new(1, 1, 8, 8, 8).expect("valid limits");
        let manager = ShardedConflictManager::new(config).expect("deadline worker");
        assert_eq!(
            block_on(manager.acquire_mut(Vec::new(), deadline(), CancellationToken::new()))
                .expect_err("empty set rejects"),
            ConflictError::EmptyKeySet
        );
        assert_eq!(
            block_on(manager.acquire_mut(
                vec![key(1), key(2)],
                deadline(),
                CancellationToken::new()
            ))
            .expect_err("oversized set rejects"),
            ConflictError::TooManyKeys {
                actual: 2,
                maximum: 1,
            }
        );
        assert_eq!(manager.queued_waiters(), 0);
    }

    #[test]
    fn raw_key_count_is_bounded_before_duplicate_canonicalization() {
        let manager = manager();
        let duplicate = key(1);
        let raw_count = MAX_COMMAND_CONFLICT_KEYS_V1 + 1;
        let input = (0..raw_count)
            .map(|_| duplicate.clone())
            .collect::<Vec<_>>();

        assert_eq!(
            block_on(manager.acquire_mut(input, deadline(), CancellationToken::new()))
                .expect_err("raw vector rejects even though it would deduplicate to one key"),
            ConflictError::InputKeyCountExceeded {
                actual: raw_count,
                maximum: MAX_COMMAND_CONFLICT_KEYS_V1,
            }
        );
        assert_eq!(manager.queued_waiters(), 0);
    }

    #[test]
    fn default_manager_accepts_the_shared_v1_key_limit() {
        let manager = manager();
        let input = (0..MAX_COMMAND_CONFLICT_KEYS_V1)
            .map(|value| key(value as u64))
            .collect();

        let lease = block_on(manager.acquire_mut(input, deadline(), CancellationToken::new()))
            .expect("the inclusive v1 limit grants under the default configuration");
        assert_eq!(lease.key_count(), MAX_COMMAND_CONFLICT_KEYS_V1);
    }

    #[test]
    fn canonicalization_sorts_and_deduplicates_before_grant() {
        let manager = manager();
        let lease = block_on(manager.acquire_mut(
            vec![key(2), key(1), key(2), key(1)],
            deadline(),
            CancellationToken::new(),
        ))
        .expect("canonical set grants");

        assert_eq!(lease.key_count(), 2);
    }

    #[test]
    fn one_key_is_never_granted_to_two_writers() {
        let manager = manager();
        let first =
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new()))
                .expect("first grant");
        let mut second = manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new());

        assert!(poll_once(second.as_mut()).is_pending());
        assert_eq!(manager.queued_waiters(), 1);
        first.release();

        let second = block_on(second).expect("second grant after release");
        assert_eq!(second.key_count(), 1);
    }

    #[test]
    fn multi_key_waiter_is_fifo_and_never_receives_a_partial_grant() {
        let manager = manager();
        let key_a = key(1);
        let key_b = key(2);
        let holder = block_on(manager.acquire_mut(
            vec![key_b.clone()],
            deadline(),
            CancellationToken::new(),
        ))
        .expect("key b holder");
        let mut both = manager.acquire_mut(
            vec![key_b, key_a.clone()],
            deadline(),
            CancellationToken::new(),
        );
        let mut only_a = manager.acquire_mut(vec![key_a], deadline(), CancellationToken::new());

        assert!(poll_once(both.as_mut()).is_pending());
        assert!(poll_once(only_a.as_mut()).is_pending());
        holder.release();

        let both = block_on(both).expect("front waiter receives complete grant");
        assert!(poll_once(only_a.as_mut()).is_pending());
        both.release();
        assert!(block_on(only_a).is_ok());
    }

    #[test]
    fn opposite_user_order_uses_one_canonical_acquisition_order() {
        let manager = manager();
        let first = block_on(manager.acquire_mut(
            vec![key(1), key(2)],
            deadline(),
            CancellationToken::new(),
        ))
        .expect("first grant");
        let mut second =
            manager.acquire_mut(vec![key(2), key(1)], deadline(), CancellationToken::new());
        assert!(poll_once(second.as_mut()).is_pending());

        first.release();
        assert_eq!(
            block_on(second).expect("ordered second grant").key_count(),
            2
        );
    }

    #[test]
    fn cancellation_removes_waiter_and_unblocks_its_fifo_successor() {
        let manager = manager();
        let holder =
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new()))
                .expect("holder");
        let cancellation = CancellationToken::new();
        let mut cancelled = manager.acquire_mut(vec![key(1)], deadline(), cancellation.clone());
        let mut successor = manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new());
        assert!(poll_once(cancelled.as_mut()).is_pending());
        assert!(poll_once(successor.as_mut()).is_pending());

        cancellation.cancel();
        assert_eq!(
            block_on(cancelled).expect_err("cancelled waiter rejects"),
            ConflictError::Cancelled
        );
        holder.release();
        assert!(block_on(successor).is_ok());
    }

    #[test]
    fn cancellation_after_internal_grant_releases_before_returning() {
        let manager = manager();
        let holder =
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new()))
                .expect("holder");
        let cancellation = CancellationToken::new();
        let mut cancelled = manager.acquire_mut(vec![key(1)], deadline(), cancellation.clone());
        assert!(poll_once(cancelled.as_mut()).is_pending());

        holder.release();
        cancellation.cancel();
        assert_eq!(
            block_on(cancelled).expect_err("cancelled grant rejects"),
            ConflictError::Cancelled
        );
        assert!(
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new(),))
                .is_ok()
        );
    }

    #[test]
    fn dropping_a_queued_future_cleans_every_queue() {
        let manager = manager();
        let holder =
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new()))
                .expect("holder");
        let mut abandoned =
            manager.acquire_mut(vec![key(1), key(2)], deadline(), CancellationToken::new());
        assert!(poll_once(abandoned.as_mut()).is_pending());
        assert_eq!(manager.queued_waiters(), 1);

        drop(abandoned);
        assert_eq!(manager.queued_waiters(), 0);
        holder.release();
        assert!(
            block_on(manager.acquire_mut(
                vec![key(1), key(2)],
                deadline(),
                CancellationToken::new(),
            ))
            .is_ok()
        );
    }

    #[test]
    fn panic_unwinding_drops_the_complete_capability() {
        let manager = manager();
        let panic_manager = manager.clone();
        let result = catch_unwind(AssertUnwindSafe(move || {
            let _lease = block_on(panic_manager.acquire_mut(
                vec![key(1), key(2)],
                deadline(),
                CancellationToken::new(),
            ))
            .expect("grant before panic");
            panic!("contained test panic");
        }));
        assert!(result.is_err());

        assert!(
            block_on(manager.acquire_mut(
                vec![key(2), key(1)],
                deadline(),
                CancellationToken::new(),
            ))
            .is_ok()
        );
    }

    #[test]
    fn production_deadline_worker_wakes_and_removes_a_registered_waiter() {
        let manager = manager();
        let holder =
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new()))
                .expect("holder");
        let mut acquisition = Box::pin(Acquisition::new(
            Arc::clone(&manager.inner),
            vec![key(1)],
            deadline(),
            CancellationToken::new(),
        ));
        assert!(poll_once(acquisition.as_mut()).is_pending());
        let waiter = Arc::clone(acquisition.waiter.as_ref().expect("registered waiter"));

        manager.inner.timer.expire_now(&waiter);
        assert_eq!(
            block_on(acquisition).expect_err("expired waiter rejects"),
            ConflictError::DeadlineExceeded
        );
        assert_eq!(waiter.state(), WaiterState::TimedOut);
        holder.release();
        assert_eq!(manager.queued_waiters(), 0);
    }

    #[test]
    fn actual_multi_shard_grant_is_all_or_nothing() {
        let config = ConflictManagerConfig::new(4, 8, 32, 32, 32).expect("valid limits");
        let manager = ShardedConflictManager::new(config).expect("deadline worker");
        let (left, right) = keys_on_different_shards(config.shard_count());
        let holder = block_on(manager.acquire_mut(
            vec![right.clone()],
            deadline(),
            CancellationToken::new(),
        ))
        .expect("right holder");
        let mut both = manager.acquire_mut(
            vec![left.clone(), right],
            deadline(),
            CancellationToken::new(),
        );
        assert!(poll_once(both.as_mut()).is_pending());

        let mut left_only = manager.acquire_mut(vec![left], deadline(), CancellationToken::new());
        assert!(poll_once(left_only.as_mut()).is_pending());
        holder.release();
        let both = block_on(both).expect("multi-shard complete grant");
        assert!(poll_once(left_only.as_mut()).is_pending());
        both.release();
        assert!(block_on(left_only).is_ok());
    }

    #[test]
    fn bounded_waiter_and_key_queue_capacity_fail_closed() {
        let config = ConflictManagerConfig::new(1, 4, 8, 1, 1).expect("valid limits");
        let manager = ShardedConflictManager::new(config).expect("deadline worker");
        let holder =
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new()))
                .expect("holder");
        let mut queued = manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new());
        assert!(poll_once(queued.as_mut()).is_pending());

        assert_eq!(
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new(),))
                .expect_err("per-key queue rejects"),
            ConflictError::KeyQueueCapacityExceeded { maximum: 1 }
        );
        assert_eq!(
            block_on(manager.acquire_mut(vec![key(2)], deadline(), CancellationToken::new(),))
                .expect_err("global queue rejects"),
            ConflictError::WaiterCapacityExceeded { maximum: 1 }
        );
        drop(queued);
        holder.release();
    }

    #[test]
    fn shared_cancellation_registration_overflow_is_typed_and_cleans_the_waiter() {
        let config = ConflictManagerConfig::new(1, 1, 8, 2_048, 2_048).expect("valid limits");
        let manager = ShardedConflictManager::new(config).expect("deadline worker");
        let holder =
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new()))
                .expect("holder");
        let cancellation = CancellationToken::new();
        let mut waiters = Vec::with_capacity(1_024);
        for _ in 0..1_024 {
            let mut waiter = manager.acquire_mut(vec![key(1)], deadline(), cancellation.clone());
            assert!(poll_once(waiter.as_mut()).is_pending());
            waiters.push(waiter);
        }
        let mut overflow = manager.acquire_mut(vec![key(1)], deadline(), cancellation);
        let Poll::Ready(Err(error)) = poll_once(overflow.as_mut()) else {
            panic!("registration beyond the checked bound must reject");
        };
        assert_eq!(
            error,
            ConflictError::CancellationRegistrationCapacityExceeded { maximum: 1_024 }
        );
        assert_eq!(manager.queued_waiters(), 1_024);

        drop(overflow);
        drop(waiters);
        assert_eq!(manager.queued_waiters(), 0);
        holder.release();
    }

    #[test]
    fn observer_panics_are_contained_and_events_never_carry_raw_keys() {
        let observer = Arc::new(RecordingObserver::default());
        let manager = ShardedConflictManager::with_observer(
            ConflictManagerConfig::default(),
            observer.clone(),
        )
        .expect("deadline worker");
        let raw = key(0xfeed_beef);
        let raw_debug = format!("{:?}", raw.as_bytes());
        let lease = block_on(manager.acquire_mut(vec![raw], deadline(), CancellationToken::new()))
            .expect("observer panic does not block grant");
        lease.release();

        let events = observer.wait_for_events(2);
        assert!(
            events
                .iter()
                .any(|event| event.kind() == ConflictEventKind::Acquired)
        );
        assert!(
            events
                .iter()
                .any(|event| event.kind() == ConflictEventKind::Released)
        );
        assert!(
            events
                .iter()
                .all(|event| !format!("{event:?}").contains(&raw_debug))
        );
    }

    #[test]
    fn stalled_observer_cannot_delay_grant_successor_progress_or_bounded_drop() {
        let observer = Arc::new(StallingObserver::default());
        let scheduler = Arc::new(RecordingScheduler::default());
        let (manager, _) = ShardedConflictManager::with_test_scheduler_and_telemetry_capacity(
            ConflictManagerConfig::default(),
            observer.clone(),
            scheduler,
            1,
        )
        .expect("manual manager");

        let first =
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new()))
                .expect("immediate grant is independent of the observer");
        observer.wait_until_stalled();

        let mut successor = manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new());
        assert!(poll_once(successor.as_mut()).is_pending());
        first.release();
        let successor = block_on(successor).expect("successor promoted before telemetry handoff");
        successor.release();

        for value in 2..8 {
            block_on(manager.acquire_mut(vec![key(value)], deadline(), CancellationToken::new()))
                .expect("telemetry pressure never blocks grants")
                .release();
        }
        assert!(manager.dropped_telemetry_events() > 0);
        observer.unblock();
    }

    #[test]
    fn observer_can_reenter_manager_without_delaying_the_original_grant() {
        let observer = Arc::new(ReentrantObserver::new(key(2)));
        let manager = ShardedConflictManager::with_observer(
            ConflictManagerConfig::default(),
            observer.clone(),
        )
        .expect("manager");
        observer.install(Arc::downgrade(&manager.inner));

        let lease =
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new()))
                .expect("original grant returns before observer code");
        observer.wait_until_reentered();
        lease.release();
    }

    #[test]
    fn deterministic_driver_reaches_production_deadline_and_waker_transitions() {
        let scheduler = Arc::new(RecordingScheduler::default());
        let (manager, driver) = ShardedConflictManager::with_test_scheduler(
            ConflictManagerConfig::default(),
            scheduler.clone(),
        )
        .expect("manual manager");
        let holder =
            block_on(manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new()))
                .expect("holder");
        let counter = Arc::new(CounterWaker(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut acquisition =
            manager.acquire_mut(vec![key(1)], deadline(), CancellationToken::new());
        assert!(poll_with(acquisition.as_mut(), &waker).is_pending());
        let waiter_id = scheduler
            .last_waiter(ConflictSchedulePoint::DeadlineRegistered)
            .expect("deadline registration hook");

        assert!(driver.notify_deadline(waiter_id));
        assert!(counter.0.load(Ordering::SeqCst) > 0);
        assert_eq!(
            block_on(acquisition).expect_err("manual deadline rejects"),
            ConflictError::DeadlineExceeded
        );
        holder.release();

        let points = scheduler.points();
        for expected in [
            ConflictSchedulePoint::Enqueued,
            ConflictSchedulePoint::WakerRegistered,
            ConflictSchedulePoint::DeadlineRegistered,
            ConflictSchedulePoint::DeadlineNotified,
            ConflictSchedulePoint::BeforeAbort,
            ConflictSchedulePoint::Aborted,
            ConflictSchedulePoint::BeforeRelease,
            ConflictSchedulePoint::Released,
        ] {
            assert!(
                points.iter().any(|(point, _)| *point == expected),
                "missing production checkpoint {expected:?}"
            );
        }
    }

    #[test]
    fn manager_drop_wakes_and_joins_the_deadline_worker() {
        let manager = manager();
        drop(manager);
    }

    #[derive(Default)]
    struct RecordingObserver {
        events: Mutex<Vec<ConflictEvent>>,
        changed: Condvar,
        calls: AtomicUsize,
    }

    impl RecordingObserver {
        fn wait_for_events(&self, count: usize) -> std::sync::MutexGuard<'_, Vec<ConflictEvent>> {
            let mut events = lock_unpoisoned(&self.events);
            while events.len() < count {
                events = self
                    .changed
                    .wait(events)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            events
        }
    }

    impl ConflictObserver for RecordingObserver {
        fn observe(&self, event: ConflictEvent) {
            lock_unpoisoned(&self.events).push(event);
            self.changed.notify_all();
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("observer failure is contained");
            }
        }
    }

    #[derive(Default)]
    struct RecordingScheduler {
        points: Mutex<Vec<(ConflictSchedulePoint, u64)>>,
    }

    impl RecordingScheduler {
        fn last_waiter(&self, point: ConflictSchedulePoint) -> Option<u64> {
            lock_unpoisoned(&self.points)
                .iter()
                .rev()
                .find_map(|(candidate, waiter)| (*candidate == point).then_some(*waiter))
        }

        fn points(&self) -> Vec<(ConflictSchedulePoint, u64)> {
            lock_unpoisoned(&self.points).clone()
        }
    }

    impl DeterministicConflictScheduler for RecordingScheduler {
        fn checkpoint(&self, point: ConflictSchedulePoint, waiter_id: u64) {
            lock_unpoisoned(&self.points).push((point, waiter_id));
        }
    }

    #[derive(Default)]
    struct StallingObserver {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
    }

    impl StallingObserver {
        fn wait_until_stalled(&self) {
            let mut state = lock_unpoisoned(&self.state);
            while !state.0 {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }

        fn unblock(&self) {
            let mut state = lock_unpoisoned(&self.state);
            state.1 = true;
            self.changed.notify_all();
        }
    }

    impl ConflictObserver for StallingObserver {
        fn observe(&self, _event: ConflictEvent) {
            let mut state = lock_unpoisoned(&self.state);
            state.0 = true;
            self.changed.notify_all();
            while !state.1 {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }

    struct ReentrantObserver {
        inner: Mutex<Option<Weak<super::Inner>>>,
        key: ConflictKey,
        entered: AtomicBool,
        completed: Mutex<bool>,
        changed: Condvar,
    }

    impl ReentrantObserver {
        fn new(key: ConflictKey) -> Self {
            Self {
                inner: Mutex::new(None),
                key,
                entered: AtomicBool::new(false),
                completed: Mutex::new(false),
                changed: Condvar::new(),
            }
        }

        fn install(&self, inner: Weak<super::Inner>) {
            *lock_unpoisoned(&self.inner) = Some(inner);
        }

        fn wait_until_reentered(&self) {
            let mut completed = lock_unpoisoned(&self.completed);
            while !*completed {
                completed = self
                    .changed
                    .wait(completed)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }

    impl ConflictObserver for ReentrantObserver {
        fn observe(&self, _event: ConflictEvent) {
            if self.entered.swap(true, Ordering::SeqCst) {
                return;
            }
            let weak = lock_unpoisoned(&self.inner).clone();
            if let Some(inner) = weak.and_then(|inner| inner.upgrade()) {
                let manager = ShardedConflictManager { inner };
                block_on(manager.acquire_mut(
                    vec![self.key.clone()],
                    deadline(),
                    CancellationToken::new(),
                ))
                .expect("reentrant acquisition on an unrelated key")
                .release();
            }
            *lock_unpoisoned(&self.completed) = true;
            self.changed.notify_all();
        }
    }

    struct CounterWaker(AtomicUsize);

    impl Wake for CounterWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn manager() -> ShardedConflictManager {
        ShardedConflictManager::new(ConflictManagerConfig::default()).expect("deadline worker")
    }

    fn deadline() -> Instant {
        Instant::now() + LONG_DEADLINE
    }

    fn key(value: u64) -> ConflictKey {
        let mut builder =
            ConflictKeyBuilder::new(AggregateTypeId::new(1).expect("nonzero aggregate"));
        builder.push_u64(value).expect("bounded test key");
        builder.finish().expect("valid conflict key")
    }

    fn keys_on_different_shards(shard_count: usize) -> (ConflictKey, ConflictKey) {
        let first = key(0);
        let first_shard = shard_for_hash(
            riffdb_types::hash_conflict_key(first.as_bytes()),
            shard_count,
        );
        let second = (1..u64::MAX)
            .map(key)
            .find(|candidate| {
                shard_for_hash(
                    riffdb_types::hash_conflict_key(candidate.as_bytes()),
                    shard_count,
                ) != first_shard
            })
            .expect("four shards produce a distinct test key");
        (first, second)
    }

    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
        poll_with(future, &waker)
    }

    fn poll_with<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        let mut context = Context::from_waker(waker);
        future.poll(&mut context)
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
        let mut context = Context::from_waker(&waker);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => thread::park(),
            }
        }
    }

    struct ThreadWaker(thread::Thread);

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
}
