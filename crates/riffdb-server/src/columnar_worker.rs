//! Bounded owner for columnar projection apply catch-up and checkpoints.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use riffdb_columnar::{
    ColumnarAmplification, ColumnarEngine, ColumnarError, ColumnarSnapshotRebuild, OpenOptions,
    frontier_lag_sequences,
};
use riffdb_observability::{MetricRegistry, RequiredGauge};
use riffdb_storage_api::{
    ApplicationExportSnapshotPort, ApplicationExportSourceRecordV1, AuthoritativeScanReader,
    CommitScanPageV1, CommitScanRequest, StorageError, StorageErrorKind, StorageScanLimit,
    StoredVectorProjectionControlV1, VectorProjectionControlRepository,
    VectorProjectionControlWriteResultV1, VectorProjectionLifecycleV1,
    VectorProjectionRebuildReasonV1,
};
use riffdb_types::FrontierPosition;

use crate::columnar_adapter::{
    ColumnarRuntime, VectorProjectionRegistration, is_holdback_active, vector_generation_directory,
};

const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(25);
const CHECKPOINT_COMMIT_CADENCE: u64 = 4096;
const CHECKPOINT_POLL_CADENCE: u32 = 30;

/// Columnar-owned process-local worker state used by aggregate health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnarWorkerReadiness {
    Starting,
    Ready,
    Degraded,
    Stopped,
}

/// Cloneable observation handle with no worker or storage authority.
#[derive(Clone)]
pub(crate) struct ColumnarWorkerStatus {
    state: Arc<AtomicU8>,
}

impl ColumnarWorkerStatus {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(0)),
        }
    }

    pub(crate) fn publish(&self, readiness: ColumnarWorkerReadiness) {
        self.state.store(
            match readiness {
                ColumnarWorkerReadiness::Starting => 0,
                ColumnarWorkerReadiness::Ready => 1,
                ColumnarWorkerReadiness::Degraded => 2,
                ColumnarWorkerReadiness::Stopped => 3,
            },
            Ordering::Release,
        );
    }

    /// Returns the most recent bounded worker classification.
    pub(crate) fn readiness(&self) -> ColumnarWorkerReadiness {
        match self.state.load(Ordering::Acquire) {
            0 => ColumnarWorkerReadiness::Starting,
            1 => ColumnarWorkerReadiness::Ready,
            2 => ColumnarWorkerReadiness::Degraded,
            _ => ColumnarWorkerReadiness::Stopped,
        }
    }
}

impl fmt::Debug for ColumnarWorkerStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ColumnarWorkerStatus")
            .field("readiness", &self.readiness())
            .finish()
    }
}

struct StopState {
    requested: Mutex<bool>,
    changed: Condvar,
}

/// Owning guard for the one columnar apply/checkpoint thread.
#[must_use = "the columnar worker must be explicitly stopped and joined"]
pub(crate) struct RunningColumnarWorker {
    stop: Arc<StopState>,
    task: Option<JoinHandle<()>>,
    status: ColumnarWorkerStatus,
}

impl RunningColumnarWorker {
    /// Starts one bounded worker over already registered columnar engines.
    pub(crate) fn start(
        runtime: Arc<ColumnarRuntime>,
        metrics: Option<MetricRegistry>,
    ) -> Result<Self, ColumnarWorkerStartError> {
        let stop = Arc::new(StopState {
            requested: Mutex::new(false),
            changed: Condvar::new(),
        });
        let status = ColumnarWorkerStatus::new();
        let worker_stop = Arc::clone(&stop);
        let worker_status = status.clone();
        let task = thread::Builder::new()
            .name("riffdb-columnar".to_owned())
            .spawn(move || {
                let mut state = ColumnarWorkerState::new(runtime.as_ref());
                loop {
                    if stop_requested(&worker_stop) {
                        break;
                    }
                    let pass_started = Instant::now();
                    worker_status.publish(
                        match run_columnar_pass(&runtime, metrics.as_ref(), &mut state) {
                            Ok(()) => ColumnarWorkerReadiness::Ready,
                            Err(_) => ColumnarWorkerReadiness::Degraded,
                        },
                    );
                    let pass_us = elapsed_microseconds(pass_started);
                    state.evidence.passes = state.evidence.passes.saturating_add(1);
                    state.evidence.last_pass_us = pass_us;
                    state.evidence.max_pass_us = state.evidence.max_pass_us.max(pass_us);
                    if wait_for_stop(&worker_stop, WORKER_POLL_INTERVAL) {
                        break;
                    }
                }
                emit_columnar_worker_evidence(&state.evidence);
                worker_status.publish(ColumnarWorkerReadiness::Stopped);
            })
            .map_err(|_| ColumnarWorkerStartError)?;
        Ok(Self {
            stop,
            task: Some(task),
            status,
        })
    }

    /// Returns a least-authority aggregate-health observation handle.
    pub(crate) fn status(&self) -> ColumnarWorkerStatus {
        self.status.clone()
    }

    /// Requests termination and joins the worker before storage can be dropped.
    pub(crate) fn shutdown(mut self) -> Result<(), ColumnarWorkerShutdownError> {
        request_stop(&self.stop)?;
        let task = self
            .task
            .take()
            .expect("a running columnar worker retains one task");
        task.join().map_err(|_| ColumnarWorkerShutdownError)?;
        if self.status.readiness() == ColumnarWorkerReadiness::Stopped {
            Ok(())
        } else {
            Err(ColumnarWorkerShutdownError)
        }
    }
}

impl fmt::Debug for RunningColumnarWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RunningColumnarWorker([DERIVED_AUTHORITY])")
    }
}

struct EngineCatchupState {
    last_published: FrontierPosition,
    commits_since_checkpoint: u64,
}

/// Cumulative per-process columnar apply and rewrite cost.
///
/// ADR-0086 §6 requires a sequence-distance backlog metric alongside time lag,
/// and its acceptance criteria require write/disk amplification and an apply
/// CPU split. This carries both, plus the per-pass duration distribution that
/// bounds how long a graceful stop waits for the worker: a stop request is only
/// observed between passes, so the tail of the current pass is the shutdown
/// cost.
#[derive(Clone, Copy, Debug, Default)]
struct ColumnarWorkerEvidence {
    passes: u64,
    apply_us: u64,
    checkpoint_us: u64,
    max_pass_us: u64,
    last_pass_us: u64,
    commits_applied: u64,
    amplification: ColumnarAmplification,
    resident_rows: u64,
    lag_at_exit: u64,
}

impl ColumnarWorkerEvidence {
    /// Stable process-evidence line consumed by benchmark harnesses.
    fn format_v1_line(&self) -> String {
        format!(
            "riffdb-columnar-worker-v1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            self.passes,
            self.apply_us,
            self.checkpoint_us,
            self.max_pass_us,
            self.last_pass_us,
            self.commits_applied,
            self.amplification.checkpoints,
            self.amplification.segments_written,
            self.amplification.rows_rewritten,
            self.amplification.rows_dirty,
            self.amplification.segment_bytes_written,
            self.resident_rows,
            self.lag_at_exit,
        )
    }
}

/// Field order of [`ColumnarWorkerEvidence::format_v1_line`].
const COLUMNAR_WORKER_EVIDENCE_LABELS: [&str; 13] = [
    "passes",
    "apply_us",
    "checkpoint_us",
    "max_pass_us",
    "last_pass_us",
    "commits_applied",
    "checkpoints",
    "segments_written",
    "rows_rewritten",
    "rows_dirty",
    "segment_bytes_written",
    "resident_rows",
    "lag_at_exit",
];

fn format_columnar_worker_labels_v1_line() -> String {
    format!(
        "riffdb-columnar-worker-labels-v1\t{}",
        COLUMNAR_WORKER_EVIDENCE_LABELS.join(",")
    )
}

struct ColumnarWorkerState {
    engines: BTreeMap<String, EngineCatchupState>,
    polls_since_checkpoint: u32,
    evidence: ColumnarWorkerEvidence,
}

impl ColumnarWorkerState {
    fn new(runtime: &ColumnarRuntime) -> Self {
        let mut engines = BTreeMap::new();
        for (name, slot) in runtime.engines().unwrap_or_default() {
            let published = slot
                .lock_engine()
                .map(|engine| engine.published_frontier_position())
                .unwrap_or(FrontierPosition::BeforeFirst);
            engines.insert(
                name.clone(),
                EngineCatchupState {
                    last_published: published,
                    commits_since_checkpoint: 0,
                },
            );
        }
        Self {
            engines,
            polls_since_checkpoint: 0,
            evidence: ColumnarWorkerEvidence::default(),
        }
    }
}

fn maintain_vector_generations(runtime: &ColumnarRuntime) -> Result<(), ColumnarWorkerError> {
    for (name, registration) in runtime.vector_registrations().map_err(map_port_error)? {
        let control = runtime
            .storage()
            .read_vector_projection_control(registration.source())
            .map_err(map_storage_error)?
            .ok_or(ColumnarWorkerError::Integrity)?;
        if control.definition_fingerprint() != registration.definition_fingerprint()
            || control.limits() != registration.limits()
        {
            return Err(ColumnarWorkerError::Integrity);
        }
        match control.lifecycle() {
            VectorProjectionLifecycleV1::Invalid => continue,
            VectorProjectionLifecycleV1::Building
            | VectorProjectionLifecycleV1::RebuildRequired
            | VectorProjectionLifecycleV1::Rebuilding => {
                rebuild_vector_generation(runtime, &name, &registration, control)?;
            }
            VectorProjectionLifecycleV1::Ready => {
                let slot = runtime
                    .engine(&name)
                    .map_err(map_port_error)?
                    .ok_or(ColumnarWorkerError::Integrity)?;
                let durable = slot
                    .lock_engine()
                    .map_err(map_port_error)?
                    .durable_frontier()
                    .position();
                if slot.generation() != Some(control.generation())
                    || durable != control.published_frontier()
                {
                    let detached = detached_control(
                        &control,
                        VectorProjectionRebuildReasonV1::DefinitionChanged,
                    )?;
                    compare_control(runtime, Some(&control), &detached)?;
                    rebuild_vector_generation(runtime, &name, &registration, detached)?;
                    continue;
                }
                if let Some(reason) = replay_budget_breach(runtime, &control)? {
                    let detached = detached_control(&control, reason)?;
                    compare_control(runtime, Some(&control), &detached)?;
                    rebuild_vector_generation(runtime, &name, &registration, detached)?;
                }
            }
        }
    }
    Ok(())
}

fn detached_control(
    control: &StoredVectorProjectionControlV1,
    reason: VectorProjectionRebuildReasonV1,
) -> Result<StoredVectorProjectionControlV1, ColumnarWorkerError> {
    StoredVectorProjectionControlV1::new(
        control.source().clone(),
        control
            .generation()
            .checked_next()
            .ok_or(ColumnarWorkerError::Integrity)?,
        *control.definition_fingerprint(),
        VectorProjectionLifecycleV1::RebuildRequired,
        control.published_frontier(),
        None,
        Some(reason),
        control.limits(),
    )
    .map_err(|_| ColumnarWorkerError::Integrity)
}

fn compare_control(
    runtime: &ColumnarRuntime,
    expected: Option<&StoredVectorProjectionControlV1>,
    replacement: &StoredVectorProjectionControlV1,
) -> Result<(), ColumnarWorkerError> {
    match runtime
        .storage()
        .compare_and_set_vector_projection_control(expected, replacement)
        .map_err(map_storage_error)?
    {
        VectorProjectionControlWriteResultV1::Applied
        | VectorProjectionControlWriteResultV1::Unchanged => Ok(()),
        VectorProjectionControlWriteResultV1::CompareMismatch => {
            Err(ColumnarWorkerError::Unavailable)
        }
    }
}

fn rebuild_vector_generation(
    runtime: &ColumnarRuntime,
    name: &str,
    registration: &VectorProjectionRegistration,
    mut control: StoredVectorProjectionControlV1,
) -> Result<(), ColumnarWorkerError> {
    let snapshot = runtime
        .storage()
        .capture_application_export_snapshot(registration.source().lineage())
        .map_err(map_storage_error)?;
    let snapshot_frontier = snapshot.binding().application_frontier().map_or(
        FrontierPosition::BeforeFirst,
        FrontierPosition::AppliedThrough,
    );
    if matches!(
        control.lifecycle(),
        VectorProjectionLifecycleV1::RebuildRequired | VectorProjectionLifecycleV1::Rebuilding
    ) {
        let rebuilding = StoredVectorProjectionControlV1::new(
            control.source().clone(),
            control.generation(),
            *control.definition_fingerprint(),
            VectorProjectionLifecycleV1::Rebuilding,
            control.published_frontier(),
            Some(snapshot_frontier),
            control.rebuild_reason(),
            control.limits(),
        )
        .map_err(|_| ColumnarWorkerError::Integrity)?;
        compare_control(runtime, Some(&control), &rebuilding)?;
        control = rebuilding;
    }

    let directory =
        vector_generation_directory(runtime.projections_root(), name, control.generation());
    let mut successor = ColumnarEngine::open(
        registration.definition().clone(),
        OpenOptions::new(directory).with_history_incarnation(runtime.history_incarnation()),
    )
    .map_err(ColumnarWorkerError::Apply)?;
    let mut rebuild = ColumnarSnapshotRebuild::new(registration.definition().clone());
    let limit = StorageScanLimit::new(500).ok_or(ColumnarWorkerError::Integrity)?;
    let mut continuation: Option<Box<[u8]>> = None;
    loop {
        let page = snapshot
            .read_application_export_entity_page(
                registration.source().entity_type(),
                continuation.as_deref(),
                limit,
            )
            .map_err(map_storage_error)?;
        let records = page
            .records()
            .iter()
            .map(|record| match record {
                ApplicationExportSourceRecordV1::Entity(entity) => Ok((**entity).clone()),
                _ => Err(ColumnarWorkerError::Integrity),
            })
            .collect::<Result<Vec<_>, _>>()?;
        rebuild
            .apply_page(&records)
            .map_err(ColumnarWorkerError::Apply)?;
        if page.exact_end() {
            break;
        }
        continuation = page.continuation().map(Into::into);
    }
    rebuild
        .install(&mut successor, snapshot_frontier)
        .map_err(ColumnarWorkerError::Apply)?;
    successor
        .apply_available(runtime.apply_source())
        .map_err(ColumnarWorkerError::Apply)?;
    successor.checkpoint().map_err(ColumnarWorkerError::Apply)?;
    let published = successor.durable_frontier().position();
    let ready = StoredVectorProjectionControlV1::new(
        control.source().clone(),
        control.generation(),
        *control.definition_fingerprint(),
        VectorProjectionLifecycleV1::Ready,
        published,
        None,
        None,
        control.limits(),
    )
    .map_err(|_| ColumnarWorkerError::Integrity)?;
    compare_control(runtime, Some(&control), &ready)?;
    runtime
        .replace_vector_engine(name, successor, control.generation())
        .map_err(map_port_error)?;
    let _ = runtime.notifier().notify(name);
    Ok(())
}

fn replay_budget_breach(
    runtime: &ColumnarRuntime,
    control: &StoredVectorProjectionControlV1,
) -> Result<Option<VectorProjectionRebuildReasonV1>, ColumnarWorkerError> {
    let head = read_head(runtime)?;
    let backlog = sequences_advanced(control.published_frontier(), head);
    if backlog > control.limits().backlog() {
        return Ok(Some(VectorProjectionRebuildReasonV1::ReplayBacklog));
    }
    if backlog == 0 {
        return Ok(None);
    }
    let limit = StorageScanLimit::new(64).ok_or(ColumnarWorkerError::Integrity)?;
    let mut request = match control.published_frontier() {
        FrontierPosition::BeforeFirst => CommitScanRequest::initial(limit),
        FrontierPosition::AppliedThrough(sequence) => {
            CommitScanRequest::initial_after(sequence, limit)
        }
    };
    let mut encoded_bytes = 0u64;
    let mut first_seconds = None;
    let mut last_seconds = None;
    loop {
        let page = runtime
            .apply_source()
            .scan_commits(request)
            .map_err(map_storage_error)?;
        for record in page.records() {
            encoded_bytes = encoded_bytes
                .checked_add(
                    u64::try_from(record.encoded_content_charge().get())
                        .map_err(|_| ColumnarWorkerError::Integrity)?,
                )
                .ok_or(ColumnarWorkerError::Integrity)?;
            if encoded_bytes > control.limits().bytes() {
                return Ok(Some(VectorProjectionRebuildReasonV1::ReplayBytes));
            }
            let seconds = record.value().logical_time().timestamp().seconds();
            first_seconds.get_or_insert(seconds);
            last_seconds = Some(seconds);
        }
        match page {
            CommitScanPageV1::Page {
                next_after,
                inclusive_upper,
                ..
            } => {
                let FrontierPosition::AppliedThrough(upper) = inclusive_upper else {
                    return Err(ColumnarWorkerError::Integrity);
                };
                request = CommitScanRequest::continuing(next_after, upper, limit)
                    .map_err(|_| ColumnarWorkerError::Integrity)?;
            }
            CommitScanPageV1::ExactEnd { .. } => break,
        }
    }
    let age = match (first_seconds, last_seconds) {
        (Some(first), Some(last)) => last
            .checked_sub(first)
            .and_then(|seconds| u64::try_from(seconds).ok())
            .ok_or(ColumnarWorkerError::Integrity)?,
        (None, None) => 0,
        _ => return Err(ColumnarWorkerError::Integrity),
    };
    if age > control.limits().age_seconds() {
        return Ok(Some(VectorProjectionRebuildReasonV1::ReplayAge));
    }
    Ok(None)
}

fn map_storage_error(error: StorageError) -> ColumnarWorkerError {
    match error.kind() {
        StorageErrorKind::Unavailable => ColumnarWorkerError::Unavailable,
        _ => ColumnarWorkerError::Integrity,
    }
}

const fn map_port_error(error: riffdb_service::ColumnarPortError) -> ColumnarWorkerError {
    match error {
        riffdb_service::ColumnarPortError::Unavailable => ColumnarWorkerError::Unavailable,
        riffdb_service::ColumnarPortError::Integrity => ColumnarWorkerError::Integrity,
    }
}

fn run_columnar_pass(
    runtime: &ColumnarRuntime,
    metrics: Option<&MetricRegistry>,
    state: &mut ColumnarWorkerState,
) -> Result<(), ColumnarWorkerError> {
    runtime
        .synchronize_active_vector_projections()
        .map_err(|_| ColumnarWorkerError::Registration)?;
    maintain_vector_generations(runtime)?;
    state.polls_since_checkpoint = state.polls_since_checkpoint.saturating_add(1);
    let force_checkpoint = state.polls_since_checkpoint >= CHECKPOINT_POLL_CADENCE;
    if force_checkpoint {
        state.polls_since_checkpoint = 0;
    }

    let apply_source = runtime.apply_source();
    let head = read_head(runtime)?;
    let mut max_lag = 0u64;
    let mut pass_apply_us = 0u64;
    let mut pass_checkpoint_us = 0u64;
    let mut pass_commits = 0u64;
    let mut pass_amplification = ColumnarAmplification::default();
    let mut pass_resident_rows = 0u64;
    let vector_registrations = runtime
        .vector_registrations()
        .map_err(map_port_error)?
        .into_iter()
        .collect::<BTreeMap<_, _>>();

    for (name, slot) in runtime
        .engines()
        .map_err(|_| ColumnarWorkerError::Unavailable)?
    {
        let catchup = state
            .engines
            .entry(name.clone())
            .or_insert(EngineCatchupState {
                last_published: FrontierPosition::BeforeFirst,
                commits_since_checkpoint: 0,
            });
        let published_after;
        {
            let mut engine = slot
                .lock_engine()
                .map_err(|_| ColumnarWorkerError::Integrity)?;
            let processed_before = engine.processed_frontier().position();
            let published_before = engine.published_frontier_position();
            let apply_started = Instant::now();
            let progress = engine
                .apply_available(apply_source)
                .map_err(ColumnarWorkerError::Apply)?;
            pass_apply_us = pass_apply_us.saturating_add(elapsed_microseconds(apply_started));
            published_after = progress.published_frontier;
            let commits_applied = sequences_advanced(processed_before, progress.processed);
            pass_commits = pass_commits.saturating_add(commits_applied);
            catchup.commits_since_checkpoint = catchup
                .commits_since_checkpoint
                .saturating_add(commits_applied);

            if published_after != published_before {
                let _ = runtime.notifier().notify(&name);
            }
            catchup.last_published = published_after;

            let should_checkpoint =
                force_checkpoint || catchup.commits_since_checkpoint >= CHECKPOINT_COMMIT_CADENCE;
            if should_checkpoint {
                let checkpoint_started = Instant::now();
                let outcome = engine.checkpoint();
                pass_checkpoint_us =
                    pass_checkpoint_us.saturating_add(elapsed_microseconds(checkpoint_started));
                match outcome {
                    Ok(manifest) => {
                        catchup.commits_since_checkpoint = 0;
                        if let Some(registration) = vector_registrations.get(&name) {
                            record_vector_durable_frontier(
                                runtime,
                                registration,
                                manifest.durable_frontier,
                            )?;
                        }
                    }
                    Err(error) if is_holdback_active(&error) => {
                        // HoldbackActive: retry after the next apply pull.
                    }
                    Err(error) => return Err(ColumnarWorkerError::Apply(error)),
                }
            }
            pass_amplification = accumulate(pass_amplification, engine.amplification());
            pass_resident_rows = pass_resident_rows.saturating_add(engine.resident_segment_rows());
        }

        if let Some(lag) = frontier_lag_sequences(published_after, head) {
            max_lag = max_lag.max(lag);
        }
    }

    state.evidence.apply_us = state.evidence.apply_us.saturating_add(pass_apply_us);
    state.evidence.checkpoint_us = state
        .evidence
        .checkpoint_us
        .saturating_add(pass_checkpoint_us);
    state.evidence.commits_applied = state.evidence.commits_applied.saturating_add(pass_commits);
    // Engine counters are already cumulative per engine, so the pass total
    // replaces rather than accumulates the process view.
    state.evidence.amplification = pass_amplification;
    state.evidence.resident_rows = pass_resident_rows;
    state.evidence.lag_at_exit = max_lag;

    if let Some(metrics) = metrics {
        metrics.set_required_gauge(RequiredGauge::ProjectionLagCommits, max_lag);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn run_one_test_pass(runtime: &ColumnarRuntime) -> bool {
    let mut state = ColumnarWorkerState::new(runtime);
    run_columnar_pass(runtime, None, &mut state).is_ok()
}

fn record_vector_durable_frontier(
    runtime: &ColumnarRuntime,
    registration: &VectorProjectionRegistration,
    frontier: FrontierPosition,
) -> Result<(), ColumnarWorkerError> {
    let current = runtime
        .storage()
        .read_vector_projection_control(registration.source())
        .map_err(map_storage_error)?
        .ok_or(ColumnarWorkerError::Integrity)?;
    if current.lifecycle() != VectorProjectionLifecycleV1::Ready
        || current.definition_fingerprint() != registration.definition_fingerprint()
    {
        return Err(ColumnarWorkerError::Integrity);
    }
    let replacement = StoredVectorProjectionControlV1::new(
        current.source().clone(),
        current.generation(),
        *current.definition_fingerprint(),
        VectorProjectionLifecycleV1::Ready,
        frontier,
        None,
        None,
        current.limits(),
    )
    .map_err(|_| ColumnarWorkerError::Integrity)?;
    compare_control(runtime, Some(&current), &replacement)
}

fn read_head(runtime: &ColumnarRuntime) -> Result<FrontierPosition, ColumnarWorkerError> {
    runtime
        .read_application_head()
        .map_err(|error| match error {
            riffdb_service::ColumnarPortError::Unavailable => ColumnarWorkerError::Unavailable,
            riffdb_service::ColumnarPortError::Integrity => ColumnarWorkerError::Integrity,
        })
}

/// Writes the once-per-process columnar cost receipt to stdout.
///
/// Emitted from the worker thread as it exits so every shutdown path — graceful,
/// maintenance, and failed-build cleanup — produces exactly one line.
fn emit_columnar_worker_evidence(evidence: &ColumnarWorkerEvidence) {
    use std::io::Write as _;
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    let _ = writeln!(stdout, "{}", format_columnar_worker_labels_v1_line());
    let _ = writeln!(stdout, "{}", evidence.format_v1_line());
    let _ = stdout.flush();
}

fn elapsed_microseconds(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

const fn accumulate(
    total: ColumnarAmplification,
    engine: ColumnarAmplification,
) -> ColumnarAmplification {
    ColumnarAmplification {
        checkpoints: total.checkpoints.saturating_add(engine.checkpoints),
        segments_written: total
            .segments_written
            .saturating_add(engine.segments_written),
        rows_rewritten: total.rows_rewritten.saturating_add(engine.rows_rewritten),
        rows_dirty: total.rows_dirty.saturating_add(engine.rows_dirty),
        segment_bytes_written: total
            .segment_bytes_written
            .saturating_add(engine.segment_bytes_written),
    }
}

const fn sequences_advanced(from: FrontierPosition, to: FrontierPosition) -> u64 {
    match (from, to) {
        (FrontierPosition::BeforeFirst, FrontierPosition::AppliedThrough(to_seq)) => to_seq.get(),
        (FrontierPosition::AppliedThrough(from_seq), FrontierPosition::AppliedThrough(to_seq)) => {
            to_seq.get().saturating_sub(from_seq.get())
        }
        _ => 0,
    }
}

fn stop_requested(stop: &StopState) -> bool {
    stop.requested.lock().map_or(true, |requested| *requested)
}

fn wait_for_stop(stop: &StopState, duration: Duration) -> bool {
    let Ok(requested) = stop.requested.lock() else {
        return true;
    };
    if *requested {
        return true;
    }
    match stop.changed.wait_timeout(requested, duration) {
        Ok((requested, _)) => *requested,
        Err(_) => true,
    }
}

fn request_stop(stop: &StopState) -> Result<(), ColumnarWorkerShutdownError> {
    let mut requested = stop
        .requested
        .lock()
        .map_err(|_| ColumnarWorkerShutdownError)?;
    *requested = true;
    stop.changed.notify_all();
    Ok(())
}

/// Closed worker construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ColumnarWorkerStartError;

impl fmt::Display for ColumnarWorkerStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the columnar worker could not start")
    }
}

impl std::error::Error for ColumnarWorkerStartError {}

/// Closed worker shutdown failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ColumnarWorkerShutdownError;

impl fmt::Display for ColumnarWorkerShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the columnar worker did not stop cleanly")
    }
}

impl std::error::Error for ColumnarWorkerShutdownError {}

enum ColumnarWorkerError {
    Apply(ColumnarError),
    Registration,
    Unavailable,
    Integrity,
}

impl fmt::Debug for ColumnarWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let class = match self {
            Self::Apply(error) => {
                let _ = error;
                "apply"
            }
            Self::Registration => "registration",
            Self::Unavailable => "unavailable",
            Self::Integrity => "integrity",
        };
        formatter
            .debug_struct("ColumnarWorkerError")
            .field("class", &class)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequences_advanced_counts_contiguous_catch_up() {
        assert_eq!(
            sequences_advanced(
                FrontierPosition::BeforeFirst,
                FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first())
            ),
            1
        );
        assert_eq!(
            sequences_advanced(
                FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
                FrontierPosition::AppliedThrough(
                    riffdb_types::CommitSequence::new(5).expect("sequence")
                )
            ),
            4
        );
    }

    #[test]
    fn worker_errors_never_render_sources() {
        assert_eq!(
            format!("{:?}", ColumnarWorkerError::Integrity),
            "ColumnarWorkerError { class: \"integrity\" }"
        );
    }
}
