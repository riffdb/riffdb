#![expect(
    clippy::expect_used,
    reason = "a running columnar worker retains exactly one owned task until shutdown"
)]

//! Bounded owner for columnar projection apply catch-up and checkpoints.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_columnar::{
    ColumnarEngine, ColumnarError, ColumnarSnapshotRebuild, OpenOptions, WorkerApplyOutcome,
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

/// Fixed, redaction-safe result of stopping the columnar worker.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum ColumnarWorkerShutdownObservation {
    BetweenPasses,
    AbandonedUnpublished,
    Failed,
}

impl ColumnarWorkerShutdownObservation {
    const fn code(self) -> u8 {
        match self {
            Self::BetweenPasses => 0,
            Self::AbandonedUnpublished => 1,
            Self::Failed => 2,
        }
    }

    const fn from_code(code: u8) -> Self {
        match code {
            0 => Self::BetweenPasses,
            1 => Self::AbandonedUnpublished,
            _ => Self::Failed,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BetweenPasses => "between_passes",
            Self::AbandonedUnpublished => "abandoned_unpublished",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Debug for ColumnarWorkerShutdownObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone)]
struct ColumnarWorkerShutdownStatus {
    state: Arc<AtomicU8>,
}

impl ColumnarWorkerShutdownStatus {
    fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(
                ColumnarWorkerShutdownObservation::BetweenPasses.code(),
            )),
        }
    }

    fn publish(&self, observation: ColumnarWorkerShutdownObservation) {
        self.state.store(observation.code(), Ordering::Release);
    }

    fn observation(&self) -> ColumnarWorkerShutdownObservation {
        ColumnarWorkerShutdownObservation::from_code(self.state.load(Ordering::Acquire))
    }
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
    shutdown_status: ColumnarWorkerShutdownStatus,
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
        let shutdown_status = ColumnarWorkerShutdownStatus::new();
        let worker_stop = Arc::clone(&stop);
        let worker_status = status.clone();
        let worker_shutdown_status = shutdown_status.clone();
        let task = thread::Builder::new()
            .name("riffdb-columnar".to_owned())
            .spawn(move || {
                let mut state = ColumnarWorkerState::new(runtime.as_ref());
                loop {
                    if stop_requested(&worker_stop) {
                        break;
                    }
                    match run_columnar_pass(
                        &runtime,
                        metrics.as_ref(),
                        &mut state,
                        || stop_requested(&worker_stop),
                        || stop_requested(&worker_stop),
                    ) {
                        Ok(ColumnarPassOutcome::Completed) => {
                            worker_status.publish(ColumnarWorkerReadiness::Ready);
                        }
                        Ok(ColumnarPassOutcome::StoppedBetweenEngines) => break,
                        Ok(ColumnarPassOutcome::AbandonedUnpublished) => {
                            worker_shutdown_status
                                .publish(ColumnarWorkerShutdownObservation::AbandonedUnpublished);
                            break;
                        }
                        Err(_) => worker_status.publish(ColumnarWorkerReadiness::Degraded),
                    }
                    if wait_for_stop(&worker_stop, WORKER_POLL_INTERVAL) {
                        break;
                    }
                }
                worker_status.publish(ColumnarWorkerReadiness::Stopped);
            })
            .map_err(|_| ColumnarWorkerStartError)?;
        Ok(Self {
            stop,
            task: Some(task),
            status,
            shutdown_status,
        })
    }

    /// Returns a least-authority aggregate-health observation handle.
    pub(crate) fn status(&self) -> ColumnarWorkerStatus {
        self.status.clone()
    }

    #[cfg(test)]
    fn start_boundary_barrier_test_worker(
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Self {
        let stop = Arc::new(StopState {
            requested: Mutex::new(false),
            changed: Condvar::new(),
        });
        let status = ColumnarWorkerStatus::new();
        let shutdown_status = ColumnarWorkerShutdownStatus::new();
        let worker_stop = Arc::clone(&stop);
        let worker_status = status.clone();
        let worker_shutdown_status = shutdown_status.clone();
        let task = thread::Builder::new()
            .name("riffdb-columnar-boundary-test".to_owned())
            .spawn(move || {
                let _ = entered.send(());
                let _ = release.recv();
                if stop_requested(&worker_stop) {
                    worker_shutdown_status
                        .publish(ColumnarWorkerShutdownObservation::AbandonedUnpublished);
                }
                worker_status.publish(ColumnarWorkerReadiness::Stopped);
            })
            .expect("start deterministic boundary worker");
        Self {
            stop,
            task: Some(task),
            status,
            shutdown_status,
        }
    }

    /// Requests termination and joins the worker before storage can be dropped.
    pub(crate) fn shutdown(
        mut self,
    ) -> Result<ColumnarWorkerShutdownObservation, ColumnarWorkerShutdownError> {
        if request_stop(&self.stop).is_err() {
            self.shutdown_status
                .publish(ColumnarWorkerShutdownObservation::Failed);
            return Err(ColumnarWorkerShutdownError);
        }
        let task = self
            .task
            .take()
            .expect("a running columnar worker retains one task");
        if task.join().is_err() {
            self.shutdown_status
                .publish(ColumnarWorkerShutdownObservation::Failed);
            return Err(ColumnarWorkerShutdownError);
        }
        if self.status.readiness() == ColumnarWorkerReadiness::Stopped {
            Ok(self.shutdown_status.observation())
        } else {
            self.shutdown_status
                .publish(ColumnarWorkerShutdownObservation::Failed);
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

struct ColumnarWorkerState {
    engines: BTreeMap<String, EngineCatchupState>,
    polls_since_checkpoint: u32,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnarPassOutcome {
    Completed,
    StoppedBetweenEngines,
    AbandonedUnpublished,
}

fn run_columnar_pass(
    runtime: &ColumnarRuntime,
    metrics: Option<&MetricRegistry>,
    state: &mut ColumnarWorkerState,
    mut stop_before_engine: impl FnMut() -> bool,
    mut stop_before_next_page: impl FnMut() -> bool,
) -> Result<ColumnarPassOutcome, ColumnarWorkerError> {
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
    let vector_registrations = runtime
        .vector_registrations()
        .map_err(map_port_error)?
        .into_iter()
        .collect::<BTreeMap<_, _>>();

    for (name, slot) in runtime
        .engines()
        .map_err(|_| ColumnarWorkerError::Unavailable)?
    {
        // The engine callback is intentionally reserved for nonterminal page
        // boundaries. This separate worker-owned check prevents a stop that
        // races with one engine's terminal page from starting another engine.
        if stop_before_engine() {
            return Ok(ColumnarPassOutcome::StoppedBetweenEngines);
        }
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
            let progress = match engine
                .apply_available_for_worker(apply_source, &mut stop_before_next_page)
                .map_err(ColumnarWorkerError::Apply)?
            {
                WorkerApplyOutcome::Completed(progress) => progress,
                WorkerApplyOutcome::AbandonedUnpublished => {
                    return Ok(ColumnarPassOutcome::AbandonedUnpublished);
                }
            };
            published_after = progress.published_frontier;
            let commits_applied = sequences_advanced(processed_before, progress.processed);
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
                match engine.checkpoint() {
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
        }

        if let Some(lag) = frontier_lag_sequences(published_after, head) {
            max_lag = max_lag.max(lag);
        }
    }

    if let Some(metrics) = metrics {
        metrics.set_required_gauge(RequiredGauge::ProjectionLagCommits, max_lag);
    }
    Ok(ColumnarPassOutcome::Completed)
}

#[cfg(test)]
pub(crate) fn run_one_test_pass(runtime: &ColumnarRuntime) -> bool {
    let mut state = ColumnarWorkerState::new(runtime);
    matches!(
        run_columnar_pass(runtime, None, &mut state, || false, || false),
        Ok(ColumnarPassOutcome::Completed)
    )
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

    // req: PERF-007, PERF-008, PERF-019
    #[test]
    fn real_worker_shutdown_observes_the_monotonic_stop_at_a_deterministic_boundary() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker =
            RunningColumnarWorker::start_boundary_barrier_test_worker(entered_tx, release_rx);
        entered_rx.recv().expect("worker reached page boundary");

        let stop = Arc::clone(&worker.stop);
        let releaser = thread::spawn(move || {
            let mut requested = stop.requested.lock().expect("stop lock");
            while !*requested {
                requested = stop.changed.wait(requested).expect("stop wait");
            }
            release_tx.send(()).expect("release page boundary");
        });

        assert_eq!(
            worker.shutdown().expect("bounded worker shutdown"),
            ColumnarWorkerShutdownObservation::AbandonedUnpublished
        );
        releaser.join().expect("join deterministic releaser");
    }

    // req: PERF-007, PERF-008, PERF-019
    #[test]
    fn shutdown_observation_is_fixed_and_redaction_safe() {
        let observations = [
            ColumnarWorkerShutdownObservation::BetweenPasses,
            ColumnarWorkerShutdownObservation::AbandonedUnpublished,
            ColumnarWorkerShutdownObservation::Failed,
        ];
        assert_eq!(
            observations.map(ColumnarWorkerShutdownObservation::as_str),
            ["between_passes", "abandoned_unpublished", "failed"]
        );
        assert_eq!(format!("{:?}", observations[0]), "between_passes");
        assert_eq!(format!("{:?}", observations[1]), "abandoned_unpublished");
        assert_eq!(format!("{:?}", observations[2]), "failed");
    }
}
