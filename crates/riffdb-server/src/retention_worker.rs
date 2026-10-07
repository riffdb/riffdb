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
            (&mut total.reclaim_us, phases.reclaim_us),
            (&mut total.deleted_extents, phases.deleted_extents),
            (&mut total.recycled_extents, phases.recycled_extents),
            (
                &mut total.extent_reference_holds,
                phases.extent_reference_holds,
            ),
            (&mut total.extent_reader_holds, phases.extent_reader_holds),
            (&mut total.reclaimed_receipts, phases.reclaimed_receipts),
            (&mut total.receipt_gate_us, phases.receipt_gate_us),
            (&mut total.source_initial_rows, phases.source_initial_rows),
            (
                &mut total.source_continuation_rows,
                phases.source_continuation_rows,
            ),
            (
                &mut total.source_validation_invalidations,
                phases.source_validation_invalidations,
            ),
            (
                &mut total.source_admission_refusals,
                phases.source_admission_refusals,
            ),
            (
                &mut total.source_admission_busy_writer,
                phases.source_admission_busy_writer,
            ),
            (
                &mut total.source_admission_pending_journal,
                phases.source_admission_pending_journal,
            ),
            (
                &mut total.source_admission_checkpoint,
                phases.source_admission_checkpoint,
            ),
            (
                &mut total.source_admission_qualification,
                phases.source_admission_qualification,
            ),
            (
                &mut total.source_admission_page_bound,
                phases.source_admission_page_bound,
            ),
            (
                &mut total.source_reclamation_attempts,
                phases.source_reclamation_attempts,
            ),
            (&mut total.prefix_gate_us, phases.prefix_gate_us),
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
        for (sum, add) in total
            .source_reclamation_yields
            .iter_mut()
            .zip(phases.source_reclamation_yields)
        {
            *sum = sum.saturating_add(add);
        }
        if let Some(sample) = phases.source_reclamation_sample {
            total.source_reclamation_sample = Some(sample);
            total.source_reclamation_sample_attempt = total.source_reclamation_attempts;
        }
        for (sum, add) in total
            .source_admission_census
            .iter_mut()
            .zip(phases.source_admission_census)
        {
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
    cancellation: Arc<std::sync::atomic::AtomicBool>,
    source_retirement: Option<RedbOnlineRetentionLane>,
}

impl LaneControl {
    fn stop(&self) {
        if let Some(lane) = &self.source_retirement {
            lane.retire_source_admission();
        }
        self.cancellation.store(true, Ordering::Release);
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
    #[cfg(test)]
    fn pause(&self, timeout: Duration) -> bool {
        self.pause_observation_slice(timeout).0
    }

    fn pause_observation_slice(&self, timeout: Duration) -> (bool, bool) {
        let signals = self
            .signals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut signals, _) = self
            .changed
            .wait_timeout_while(signals, timeout, |signals| !signals.stop && !signals.wake)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let woke = signals.wake;
        signals.wake = false;
        (signals.stop, woke)
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

#[derive(Clone, Copy)]
enum RetentionRefusalPhase {
    Step,
    PrefixRefresh,
}

fn format_retention_refusal(
    phase: RetentionRefusalPhase,
    kind: riffdb_storage_api::StorageErrorKind,
) -> String {
    use riffdb_storage_api::StorageErrorKind as K;
    // Only closed discriminants enter this private observation: no engine
    // source, incident identity, path, payload or caller-provided text.
    let phase = match phase {
        RetentionRefusalPhase::Step => 1,
        RetentionRefusalPhase::PrefixRefresh => 2,
    };
    let kind = match kind {
        K::Unavailable => 1,
        K::CommitStatusUnknown => 2,
        K::CorruptData => 3,
        K::IncompatibleFormat => 4,
        K::LimitExceeded => 5,
        K::InvariantViolation => 6,
        K::SequenceExhausted => 7,
        K::HistoryPruned => 8,
    };
    format!("riffdb-retention-refusal-v1\tphase={phase}\tkind={kind}")
}

fn refuse_lane(
    status: &RetentionLaneStatus,
    sinks: &RetentionLaneFailureSinks,
    phase: RetentionRefusalPhase,
    kind: riffdb_storage_api::StorageErrorKind,
) {
    eprintln!("{}", format_retention_refusal(phase, kind));
    if let Ok(incident) = sinks.incident_ids.next_incident_id() {
        sinks.diagnostics.record_internal(InternalError::new(
            incident,
            DefectScope::Process,
            RetentionLaneRefused,
        ));
    }
    status.set(RetentionLaneState::Refused);
}

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
        validation_inputs: riffdb_storage_api::StartupValidationInputs,
    ) -> Result<Self, std::io::Error> {
        let control = Arc::new(LaneControl {
            source_retirement: Some(lane.clone()),
            ..LaneControl::default()
        });
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
            .stack_size(384 * 1024)
            .spawn(move || {
                let _source_retirement = SourceOwnerRetirement(&lane);
                run_lane(
                    &lane,
                    retain,
                    budget,
                    &worker_control,
                    &worker_status,
                    &failure,
                    &validation_inputs,
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

    #[cfg(feature = "test-fixtures")]
    pub(crate) fn is_joined(&self) -> bool {
        self.thread.is_none()
    }

    /// Requests cancellation and joins the lane. A step in progress finishes
    /// (it is one atomic transition); no further step starts.
    pub(crate) fn shutdown(&mut self) -> RetentionLaneState {
        self.control.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        if let Some(lane) = &self.control.source_retirement {
            lane.release_source_validation();
        }
        // Replace any live shutdown-trigger sample with the joined owner's
        // actual terminal result. In particular, preserve Refused as Refused.
        crate::shutdown_census::record_online_retention(self.observation());
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

struct SourceOwnerRetirement<'a>(&'a RedbOnlineRetentionLane);
impl Drop for SourceOwnerRetirement<'_> {
    fn drop(&mut self) {
        self.0.retire_source_admission();
        self.0.release_source_validation();
    }
}

fn idle_recheck_pause(phases: &OnlineRetentionPhases) -> Duration {
    // Actual durable source deletion is progress even when the separate
    // logical watermark is at its target or fenced. The full writer-residence
    // rest below still applies; zero-progress source attempts retain backoff.
    if phases.receipt_validation_pending || phases.reclaimed_receipts > 0 {
        Duration::ZERO
    } else {
        RETENTION_LANE_RECHECK
    }
}

fn gate_residence_rest(gate_held: Duration) -> Duration {
    gate_held
        .saturating_mul(RETENTION_LANE_REST_FACTOR)
        .min(RETENTION_LANE_RECHECK.saturating_mul(RETENTION_LANE_REST_FACTOR))
}

/// Decide the existing rest and checkpoint-wakeup policy from one native turn.
fn maintenance_wait_policy(
    pause: Duration,
    logical_checkpoint: bool,
    gate_held: Duration,
    phases: &OnlineRetentionPhases,
) -> (Duration, bool) {
    let rest = gate_residence_rest(gate_held);
    if SourceCheckpointWait::from_phases(phases) != SourceCheckpointWait::None
        || (logical_checkpoint && rest.is_zero())
    {
        (rest, true)
    } else {
        (pause.max(rest), false)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SourceCheckpointWait {
    None,
    Journal,
    Publication,
}

impl SourceCheckpointWait {
    fn from_phases(phases: &OnlineRetentionPhases) -> Self {
        if phases.source_admission_pending_journal > 0 {
            Self::Journal
        } else if phases.source_admission_checkpoint > 0 {
            Self::Publication
        } else {
            Self::None
        }
    }
}

/// Continues captured read-only pages inside the existing checkpoint wait.
/// Completion stops page work; only a checkpoint changes the next-step hint.
fn wait_with_source_validation(
    control: &LaneControl,
    seen: u64,
    mut pending: bool,
    source: (
        SourceCheckpointWait,
        impl FnMut() -> Result<bool, riffdb_storage_api::StorageError>,
    ),
    mut wait: impl FnMut(u64, Duration) -> u64,
    mut elapsed: impl FnMut() -> Duration,
    mut advance: impl FnMut() -> Result<bool, riffdb_storage_api::StorageError>,
) -> Result<bool, riffdb_storage_api::StorageError> {
    let (source_wait, mut publication_pending) = source;
    let mut checkpoint_observed = false;
    let mut waiting_seen = seen;
    loop {
        if control.stop_requested() {
            return Ok(false);
        }
        let observed = wait(waiting_seen, RETENTION_LANE_CHECKPOINT_SLICE);
        if observed != waiting_seen {
            waiting_seen = observed;
            checkpoint_observed = true;
            if source_wait == SourceCheckpointWait::None {
                return Ok(true);
            }
        }
        if elapsed() >= RETENTION_LANE_RECHECK || control.stop_requested() {
            return Ok(false);
        }
        // A journal yield first needs an actual ordinary checkpoint. A
        // publication yield can finish when its existing sealer finishes,
        // without inventing another checkpoint or joining under a source lease.
        if source_wait != SourceCheckpointWait::None
            && (source_wait == SourceCheckpointWait::Publication || checkpoint_observed)
            && !publication_pending()?
        {
            return Ok(checkpoint_observed);
        }
        if pending {
            pending = advance()?;
        }
    }
}

/// Continue only a captured physical census during the mandatory writer rest.
/// Read-only pages never authorize a new writer turn or shorten its rest.
fn pause_with_physical_observation(
    duration: Duration,
    mut pending: bool,
    mut elapsed: impl FnMut() -> Duration,
    mut wait: impl FnMut(Duration) -> (bool, bool),
    mut advance: impl FnMut() -> Result<bool, riffdb_storage_api::StorageError>,
) -> Result<bool, riffdb_storage_api::StorageError> {
    if duration.is_zero() {
        return Ok(wait(duration).0);
    }
    loop {
        let remaining = duration.saturating_sub(elapsed());
        if remaining.is_zero() {
            return Ok(false);
        }
        let slice = if pending {
            remaining.min(RETENTION_LANE_CHECKPOINT_SLICE)
        } else {
            remaining
        };
        let (stopped, woke) = wait(slice);
        if stopped || woke {
            return Ok(stopped);
        }
        if elapsed() >= duration {
            return Ok(false);
        }
        if pending {
            pending = advance()?;
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
    validation_inputs: &riffdb_storage_api::StartupValidationInputs,
) {
    let mut after_checkpoint = false;
    let diagnostic_start = std::time::Instant::now();
    let mut next_source_diagnostic = Some(Duration::ZERO);
    loop {
        std::thread::yield_now();
        if control.stop_requested() {
            status.set(RetentionLaneState::Cancelled);
            return;
        }
        // Counted before the attempt, so a checkpoint that finishes during it
        // releases the wait below at once.
        let checkpoints_seen = lane.journal_checkpoints_finished();
        let woke_on_checkpoint = std::mem::take(&mut after_checkpoint);
        let mut await_checkpoint = false;
        let receipt_validation_pending;
        let (pause, mut gate_held, step_phases) =
            match lane.step_timed(retain, budget, woke_on_checkpoint) {
                Ok((step, phases)) => {
                    receipt_validation_pending = phases.receipt_validation_pending;
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
                            idle_recheck_pause(&phases)
                        }
                        OnlineRetentionStep::Held(OnlineRetentionHold::Fenced(binding)) => {
                            status.note_hold(1);
                            status.note_fence(Some(binding));
                            status.set(RetentionLaneState::Fenced);
                            idle_recheck_pause(&phases)
                        }
                    };
                    (pause, Duration::from_micros(phases.gate_held_us()), phases)
                }
                Err(_) if control.stop_requested() => {
                    status.set(RetentionLaneState::Cancelled);
                    return;
                }
                Err(error) => {
                    refuse_lane(status, sinks, RetentionRefusalPhase::Step, error.kind());
                    return;
                }
            };
        if !receipt_validation_pending
            && matches!(
                status.state(),
                RetentionLaneState::Idle | RetentionLaneState::Fenced
            )
        {
            match lane.refresh_validated_prefix(
                validation_inputs.clone(),
                Arc::clone(&control.cancellation),
            ) {
                Ok(held_us) => {
                    gate_held = gate_held.saturating_add(Duration::from_micros(held_us));
                    status.note_phases(&OnlineRetentionPhases {
                        prefix_gate_us: held_us,
                        ..OnlineRetentionPhases::default()
                    });
                }
                Err(_) if control.stop_requested() => {
                    status.set(RetentionLaneState::Cancelled);
                    return;
                }
                Err(error) => {
                    refuse_lane(
                        status,
                        sinks,
                        RetentionRefusalPhase::PrefixRefresh,
                        error.kind(),
                    );
                    return;
                }
            }
        }
        let elapsed = diagnostic_start.elapsed();
        if next_source_diagnostic.is_some_and(|next| elapsed >= next) {
            eprintln!(
                "{}",
                crate::shutdown_census::format_source_reclamation_sample(
                    &status.observation().phases
                )
            );
            next_source_diagnostic = elapsed.checked_add(Duration::from_secs(60));
            eprintln!(
                "{}",
                crate::shutdown_census::format_source_admission_census(
                    status.observation().phases.source_admission_census,
                )
            );
            match lane.source_drain_observation() {
                Ok(
                    [
                        unpublished,
                        unaccounted,
                        oversized,
                        transitions,
                        bytes,
                        unpublished_transitions,
                        unpublished_bytes,
                        sequence_matches,
                        administration_matches,
                        hash_matches,
                        frames,
                    ],
                ) => eprintln!(
                    "riffdb-retention-source-drain-v1\tavailable=1\tunpublished={unpublished}\tunaccounted={unaccounted}\toversized={oversized}\tsuffix_transitions={transitions}\tsuffix_bytes={bytes}\tunpublished_transitions={unpublished_transitions}\tunpublished_bytes={unpublished_bytes}\tsequence_matches={sequence_matches}\tadministration_matches={administration_matches}\thash_matches={hash_matches}\tsuffix_frames={frames}"
                ),
                Err(_) => eprintln!("riffdb-retention-source-drain-v1\tavailable=0"),
            }
        }
        // Preserve complete writer-residence rest. Source checkpoint waits
        // replace idle backoff with bounded completion/publication rechecks;
        // ordinary idle and noncheckpoint refusals keep their prior backoff.
        let (rest, wait_for_checkpoint) =
            maintenance_wait_policy(pause, await_checkpoint, gate_held, &step_phases);
        let physical_pending = match lane.physical_observation_pending() {
            Ok(pending) => pending,
            Err(_) if control.stop_requested() => {
                status.set(RetentionLaneState::Cancelled);
                return;
            }
            Err(error) => {
                refuse_lane(status, sinks, RetentionRefusalPhase::Step, error.kind());
                return;
            }
        };
        let waiting = std::time::Instant::now();
        let paused = pause_with_physical_observation(
            rest,
            physical_pending,
            || waiting.elapsed(),
            |slice| control.pause_observation_slice(slice),
            || lane.advance_physical_observation_off_gate(),
        );
        match paused {
            Ok(true) => {
                status.set(RetentionLaneState::Cancelled);
                return;
            }
            Ok(false) => {}
            Err(_) if control.stop_requested() => {
                status.set(RetentionLaneState::Cancelled);
                return;
            }
            Err(error) => {
                refuse_lane(status, sinks, RetentionRefusalPhase::Step, error.kind());
                return;
            }
        }
        if wait_for_checkpoint {
            // Step right after the next journal checkpoint, while its suffix
            // is nearly empty; look for shutdown between short waits.
            let waiting = std::time::Instant::now();
            let checkpoint = lane
                .physical_observation_pending()
                .and_then(|physical_pending| {
                    wait_with_source_validation(
                        control,
                        checkpoints_seen,
                        receipt_validation_pending || physical_pending,
                        (SourceCheckpointWait::from_phases(&step_phases), || {
                            lane.source_checkpoint_publication_pending()
                        }),
                        |waiting_seen, slice| lane.wait_for_journal_checkpoint(waiting_seen, slice),
                        || waiting.elapsed(),
                        || {
                            let phases = lane.advance_source_validation_off_gate()?;
                            status.note_phases(&phases);
                            Ok(phases.receipt_validation_pending
                                || lane.physical_observation_pending()?)
                        },
                    )
                });
            match checkpoint {
                Ok(checkpoint) => {
                    // A source-only wake cannot grant the logical lane's
                    // existing after-checkpoint suffix-drain exception.
                    after_checkpoint =
                        checkpoint && await_checkpoint && gate_residence_rest(gate_held).is_zero();
                }
                Err(_) if control.stop_requested() => {
                    status.set(RetentionLaneState::Cancelled);
                    return;
                }
                Err(error) => {
                    refuse_lane(status, sinks, RetentionRefusalPhase::Step, error.kind());
                    return;
                }
            }
            continue;
        }
    }
}

#[cfg(test)]
#[path = "retention_worker_tests.rs"]
mod tests;
