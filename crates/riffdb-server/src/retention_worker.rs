//! ADR-0182 sections 6 and 7: the online retention lane.
//!
//! One background thread per primary graph advances the retention watermark
//! toward the configured retained-sequence target in bounded steps. Each step
//! is one administration-class transition through the sole ordered writer:
//! the storage handle it holds runs steps and nothing else, and it queues on
//! the same first-come mutation gate as every other writer, so a writer is
//! blocked by at most the one step in progress. The lane yields after every
//! step, never removes more than its per-epoch budget (one step), and
//! rechecks on a bounded idle interval once it reaches the target or a fence.
//!
//! Lane health is internal and closed: `holding`, `idle`, `advancing`,
//! `fenced`, `refused`, and `cancelled`. It appears in the shutdown census
//! only; no public health member, RPC, or application operation observes,
//! starts, stops, or targets it. A refused step (an unreadable, unvalidated,
//! or incarnation-mismatched fencing source, or any storage failure) records
//! one internal incident and stops the lane for the life of the process
//! without retrying past it.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_errors::{DefectScope, IncidentIdSource, InternalError};
use riffdb_observability::ServiceDiagnostics;
use riffdb_storage_api::RetentionFenceBinding;
use riffdb_storage_redb::{OnlineRetentionHold, OnlineRetentionStep, RedbOnlineRetentionLane};

use crate::config::{RetentionConfig, RetentionTarget};

/// How long an idle or fenced lane waits before it looks again. A bound on
/// how stale compaction may fall behind new commits, never a retention age.
const RETENTION_LANE_RECHECK: Duration = Duration::from_secs(1);

/// Closed online retention lane states.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum RetentionLaneState {
    /// Configured as a hold: the lane never runs.
    Holding,
    /// The watermark meets the target; the lane waits for new history.
    Idle,
    /// The last step removed history.
    Advancing,
    /// A fencing input stops advancement below the target.
    Fenced,
    /// A step failed; the incident is recorded and the lane has stopped.
    Refused,
    /// Shutdown stopped the lane.
    Cancelled,
}

impl RetentionLaneState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Holding => "holding",
            Self::Idle => "idle",
            Self::Advancing => "advancing",
            Self::Fenced => "fenced",
            Self::Refused => "refused",
            Self::Cancelled => "cancelled",
        }
    }

    const fn code(self) -> u8 {
        match self {
            Self::Holding => 0,
            Self::Idle => 1,
            Self::Advancing => 2,
            Self::Fenced => 3,
            Self::Refused => 4,
            Self::Cancelled => 5,
        }
    }

    const fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Holding,
            1 => Self::Idle,
            2 => Self::Advancing,
            3 => Self::Fenced,
            5 => Self::Cancelled,
            _ => Self::Refused,
        }
    }

    const fn is_terminal(self) -> bool {
        matches!(self, Self::Refused | Self::Cancelled)
    }
}

impl fmt::Display for RetentionLaneState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The closed name of the fence that last held the lane, or `none`. A
/// binding name only: never a frontier, sequence, or count.
pub(crate) const fn fence_name(binding: Option<RetentionFenceBinding>) -> &'static str {
    match binding {
        None => "none",
        Some(RetentionFenceBinding::DurableApplicationHead) => "durable_application_head",
        Some(RetentionFenceBinding::ProjectionDurableFrontier) => "projection_durable_frontier",
        Some(RetentionFenceBinding::OperatorHold) => "operator_hold",
        Some(RetentionFenceBinding::UndeliveredOutboxLowWater) => "undelivered_outbox",
        Some(RetentionFenceBinding::StagedMigrationFrozenFrontier) => "staged_migration",
        Some(RetentionFenceBinding::ConsumerLowWater) => "consumer_low_water",
        Some(RetentionFenceBinding::FollowerLowWater) => "follower_low_water",
        Some(RetentionFenceBinding::Unbounded) => "unbounded",
    }
}

const FENCE_BINDINGS: [RetentionFenceBinding; 8] = [
    RetentionFenceBinding::DurableApplicationHead,
    RetentionFenceBinding::ProjectionDurableFrontier,
    RetentionFenceBinding::OperatorHold,
    RetentionFenceBinding::UndeliveredOutboxLowWater,
    RetentionFenceBinding::StagedMigrationFrozenFrontier,
    RetentionFenceBinding::ConsumerLowWater,
    RetentionFenceBinding::FollowerLowWater,
    RetentionFenceBinding::Unbounded,
];

/// The lane's bounded, redaction-safe work observation for the shutdown
/// census: the closed state, the closed name of the last binding fence, and
/// counters only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetentionLaneObservation {
    pub(crate) state: RetentionLaneState,
    pub(crate) fence: Option<RetentionFenceBinding>,
    pub(crate) steps: u64,
    pub(crate) sequences: u64,
}

/// Process-local lane status shared by the worker and its owner.
#[derive(Clone)]
pub(crate) struct RetentionLaneStatus {
    state: Arc<AtomicU8>,
    fence: Arc<AtomicU8>,
    steps: Arc<AtomicU64>,
    sequences: Arc<AtomicU64>,
}

impl RetentionLaneStatus {
    fn new(state: RetentionLaneState) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(state.code())),
            fence: Arc::new(AtomicU8::new(0)),
            steps: Arc::new(AtomicU64::new(0)),
            sequences: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Moves to `to` unless the lane already ended; terminal states never
    /// transition.
    fn set(&self, to: RetentionLaneState) {
        let _ = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (!RetentionLaneState::from_code(current).is_terminal()).then_some(to.code())
            });
    }

    fn note_fence(&self, binding: Option<RetentionFenceBinding>) {
        let code = binding
            .and_then(|binding| FENCE_BINDINGS.iter().position(|known| *known == binding))
            .and_then(|index| u8::try_from(index + 1).ok())
            .unwrap_or(0);
        self.fence.store(code, Ordering::Release);
    }

    fn fence(&self) -> Option<RetentionFenceBinding> {
        usize::from(self.fence.load(Ordering::Acquire))
            .checked_sub(1)
            .and_then(|index| FENCE_BINDINGS.get(index).copied())
    }

    fn note_step(&self, sequences: u64) {
        let _ = self.steps.fetch_add(1, Ordering::AcqRel);
        let _ = self.sequences.fetch_add(sequences, Ordering::AcqRel);
    }

    pub(crate) fn state(&self) -> RetentionLaneState {
        RetentionLaneState::from_code(self.state.load(Ordering::Acquire))
    }

    pub(crate) fn observation(&self) -> RetentionLaneObservation {
        RetentionLaneObservation {
            state: self.state(),
            fence: self.fence(),
            steps: self.steps.load(Ordering::Acquire),
            sequences: self.sequences.load(Ordering::Acquire),
        }
    }
}

#[derive(Default)]
struct LaneSignals {
    stop: bool,
    wake: bool,
}

#[derive(Default)]
struct LaneControl {
    signals: Mutex<LaneSignals>,
    changed: Condvar,
}

impl LaneControl {
    fn stop(&self) {
        let mut signals = self
            .signals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        signals.stop = true;
        self.changed.notify_all();
    }

    #[cfg(test)]
    fn wake(&self) {
        let mut signals = self
            .signals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        signals.wake = true;
        self.changed.notify_all();
    }

    fn stop_requested(&self) -> bool {
        self.signals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stop
    }

    /// Waits for stop, an explicit wake, or `timeout`; true when stopping.
    fn pause(&self, timeout: Duration) -> bool {
        let signals = self
            .signals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut signals, _) = self
            .changed
            .wait_timeout_while(signals, timeout, |signals| !signals.stop && !signals.wake)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        signals.wake = false;
        signals.stop
    }
}

/// Where a refused step records itself: its incident identity and the
/// trusted diagnostic sink. Readiness and public health are untouched.
#[derive(Clone)]
pub(crate) struct RetentionLaneFailureSinks {
    pub(crate) incident_ids: Arc<dyn IncidentIdSource>,
    pub(crate) diagnostics: Arc<dyn ServiceDiagnostics>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetentionLaneRefused;

impl fmt::Display for RetentionLaneRefused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("online retention step refused")
    }
}

impl std::error::Error for RetentionLaneRefused {}

/// The owner of one online retention lane. Shutdown stops and joins it.
pub(crate) struct RunningRetentionLane {
    status: RetentionLaneStatus,
    control: Arc<LaneControl>,
    thread: Option<JoinHandle<()>>,
}

impl RunningRetentionLane {
    /// Starts the lane for `config`; a hold starts no thread.
    pub(crate) fn start(
        config: RetentionConfig,
        lane: RedbOnlineRetentionLane,
        failure: RetentionLaneFailureSinks,
    ) -> Result<Self, std::io::Error> {
        let control = Arc::new(LaneControl::default());
        let RetentionTarget::RetainSequences(retain) = config.target() else {
            return Ok(Self {
                status: RetentionLaneStatus::new(RetentionLaneState::Holding),
                control,
                thread: None,
            });
        };
        let status = RetentionLaneStatus::new(RetentionLaneState::Idle);
        let worker_status = status.clone();
        let worker_control = Arc::clone(&control);
        let budget = config.sequences_per_epoch();
        let thread = thread::Builder::new()
            .name("riffdb-retention".to_owned())
            .spawn(move || {
                run_lane(
                    &lane,
                    retain,
                    budget,
                    &worker_control,
                    &worker_status,
                    &failure,
                );
            })?;
        Ok(Self {
            status,
            control,
            thread: Some(thread),
        })
    }

    #[cfg(test)]
    pub(crate) fn status(&self) -> RetentionLaneStatus {
        self.status.clone()
    }

    pub(crate) fn observation(&self) -> RetentionLaneObservation {
        self.status.observation()
    }

    /// Ends the current idle pause so the lane looks again now.
    #[cfg(test)]
    fn wake(&self) {
        self.control.wake();
    }

    /// Requests cancellation and joins the lane. A step in progress finishes
    /// (it is one atomic transition); no further step starts.
    pub(crate) fn shutdown(&mut self) -> RetentionLaneState {
        self.control.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.status.state()
    }
}

impl Drop for RunningRetentionLane {
    fn drop(&mut self) {
        if self.thread.is_some() {
            let _ = self.shutdown();
        }
    }
}

fn run_lane(
    lane: &RedbOnlineRetentionLane,
    retain: NonZeroU64,
    budget: NonZeroU64,
    control: &LaneControl,
    status: &RetentionLaneStatus,
    sinks: &RetentionLaneFailureSinks,
) {
    loop {
        if control.stop_requested() {
            status.set(RetentionLaneState::Cancelled);
            return;
        }
        let pause = match lane.step(retain, budget) {
            Ok(OnlineRetentionStep::Advanced { sequences, .. }) => {
                status.note_fence(None);
                status.set(RetentionLaneState::Advancing);
                status.note_step(sequences);
                // Queue behind any writer that arrived during the step.
                thread::yield_now();
                false
            }
            Ok(OnlineRetentionStep::Held(
                OnlineRetentionHold::TargetReached | OnlineRetentionHold::RowBoundary,
            )) => {
                status.note_fence(None);
                status.set(RetentionLaneState::Idle);
                true
            }
            Ok(OnlineRetentionStep::Held(OnlineRetentionHold::Fenced(binding))) => {
                status.note_fence(Some(binding));
                status.set(RetentionLaneState::Fenced);
                true
            }
            Err(_) if control.stop_requested() => {
                status.set(RetentionLaneState::Cancelled);
                return;
            }
            Err(_) => {
                if let Ok(incident) = sinks.incident_ids.next_incident_id() {
                    sinks.diagnostics.record_internal(InternalError::new(
                        incident,
                        DefectScope::Process,
                        RetentionLaneRefused,
                    ));
                }
                status.set(RetentionLaneState::Refused);
                return;
            }
        };
        if pause && control.pause(RETENTION_LANE_RECHECK) {
            status.set(RetentionLaneState::Cancelled);
            return;
        }
    }
}

#[cfg(test)]
#[path = "retention_worker_tests.rs"]
mod tests;
