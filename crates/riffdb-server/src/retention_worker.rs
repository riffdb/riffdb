//! ADR-0182 sections 6 and 7: the online retention lane.
//!
//! One background thread per primary graph advances the retention watermark
//! toward the configured retained-sequence target in bounded steps. Each step
//! is one administration-class transition through the sole ordered writer:
//! the storage handle it holds runs steps and nothing else, and it queues on
//! the same first-come mutation gate as every other writer, so a writer is
//! blocked by at most the one step in progress. After every attempt that held
//! the writer gate the lane rests nineteen times that gate-held time, so it holds the writer
//! at most about five percent of the time; it never removes more than its
//! per-epoch budget (one step), yields to an in-flight journal checkpoint
//! instead of waiting for it under the gate, and rechecks on a bounded idle
//! interval once it reaches the target or a fence.
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
use riffdb_storage_redb::{
    OnlineRetentionHold, OnlineRetentionPhases, OnlineRetentionStep, RedbOnlineRetentionLane,
};

use crate::config::{RetentionConfig, RetentionTarget};

/// How long an idle or fenced lane waits before it looks again. A bound on
/// how stale compaction may fall behind new commits, never a retention age.
const RETENTION_LANE_RECHECK: Duration = Duration::from_secs(1);

/// How often a lane waiting for a journal checkpoint looks for shutdown.
const RETENTION_LANE_CHECKPOINT_SLICE: Duration = Duration::from_millis(100);

/// ADR-0182 section 7 interference bound: after any attempt that took the
/// writer gate the lane rests this many times the time it held the gate, so
/// it holds the ordered writer at most one part in twenty of the time. A backlog therefore
/// compacts at a bounded, not maximal, rate.
const RETENTION_LANE_REST_FACTOR: u32 = 19;

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
    /// Summed phase microseconds over every step attempt (WP-811 evidence).
    pub(crate) phases: OnlineRetentionPhases,
    /// Holds by reason: target or row boundary, fenced, checkpoint in
    /// flight, awaiting a checkpoint.
    pub(crate) holds: [u64; 4],
}

/// Process-local lane status shared by the worker and its owner.
#[derive(Clone)]
pub(crate) struct RetentionLaneStatus {
    state: Arc<AtomicU8>,
    fence: Arc<AtomicU8>,
    steps: Arc<AtomicU64>,
    sequences: Arc<AtomicU64>,
    phases: Arc<Mutex<OnlineRetentionPhases>>,
    holds: Arc<[AtomicU64; 4]>,
}

impl RetentionLaneStatus {
    fn new(state: RetentionLaneState) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(state.code())),
            fence: Arc::new(AtomicU8::new(0)),
            steps: Arc::new(AtomicU64::new(0)),
            sequences: Arc::new(AtomicU64::new(0)),
            phases: Arc::new(Mutex::new(OnlineRetentionPhases::default())),
            holds: Arc::new([const { AtomicU64::new(0) }; 4]),
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

    fn note_phases(&self, phases: &OnlineRetentionPhases) {
        let mut guard = self
            .phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let total = &mut *guard;
        for (sum, add) in [
            (&mut total.preplan_us, phases.preplan_us),
            (&mut total.gate_wait_us, phases.gate_wait_us),
            (&mut total.drain_us, phases.drain_us),
            (&mut total.plan_us, phases.plan_us),
            (&mut total.digest_us, phases.digest_us),
            (&mut total.materialize_us, phases.materialize_us),
            (&mut total.delete_us, phases.delete_us),
            (&mut total.commit_us, phases.commit_us),
        ] {
            *sum = sum.saturating_add(add);
        }
    }

    fn note_hold(&self, reason: usize) {
        if let Some(count) = self.holds.get(reason) {
            let _ = count.fetch_add(1, Ordering::AcqRel);
        }
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
            phases: *self
                .phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            holds: std::array::from_fn(|reason| self.holds[reason].load(Ordering::Acquire)),
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
        // Counted before the attempt, so a checkpoint that finishes during it
        // releases the wait below at once.
        let checkpoints_seen = lane.journal_checkpoints_finished();
        let mut await_checkpoint = false;
        let (pause, gate_held) = match lane.step_timed(retain, budget) {
            Ok((step, phases)) => {
                status.note_phases(&phases);
                let pause = match step {
                    OnlineRetentionStep::Advanced { sequences, .. } => {
                        status.note_fence(None);
                        status.set(RetentionLaneState::Advancing);
                        status.note_step(sequences);
                        Duration::ZERO
                    }
                    OnlineRetentionStep::Held(OnlineRetentionHold::CheckpointInFlight) => {
                        status.note_hold(2);
                        await_checkpoint = true;
                        Duration::ZERO
                    }
                    OnlineRetentionStep::Held(OnlineRetentionHold::AwaitingCheckpoint) => {
                        status.note_hold(3);
                        await_checkpoint = true;
                        Duration::ZERO
                    }
                    OnlineRetentionStep::Held(
                        OnlineRetentionHold::TargetReached | OnlineRetentionHold::RowBoundary,
                    ) => {
                        status.note_hold(0);
                        status.note_fence(None);
                        status.set(RetentionLaneState::Idle);
                        RETENTION_LANE_RECHECK
                    }
                    OnlineRetentionStep::Held(OnlineRetentionHold::Fenced(binding)) => {
                        status.note_hold(1);
                        status.note_fence(Some(binding));
                        status.set(RetentionLaneState::Fenced);
                        RETENTION_LANE_RECHECK
                    }
                };
                (pause, Duration::from_micros(phases.gate_held_us()))
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
        // Rest in proportion to the time the attempt held the writer gate,
        // never less than the outcome's own recheck interval.
        let rest = gate_held
            .saturating_mul(RETENTION_LANE_REST_FACTOR)
            .min(RETENTION_LANE_RECHECK.saturating_mul(RETENTION_LANE_REST_FACTOR));
        if await_checkpoint && rest.is_zero() {
            // Step right after the next journal checkpoint, while its suffix
            // is nearly empty; look for shutdown between short waits.
            let waiting = std::time::Instant::now();
            loop {
                if control.stop_requested() {
                    status.set(RetentionLaneState::Cancelled);
                    return;
                }
                if lane
                    .wait_for_journal_checkpoint(checkpoints_seen, RETENTION_LANE_CHECKPOINT_SLICE)
                    != checkpoints_seen
                    || waiting.elapsed() >= RETENTION_LANE_RECHECK
                {
                    break;
                }
            }
            continue;
        }
        if control.pause(pause.max(rest)) {
            status.set(RetentionLaneState::Cancelled);
            return;
        }
    }
}

#[cfg(test)]
#[path = "retention_worker_tests.rs"]
mod tests;
