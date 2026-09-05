#![expect(
    clippy::expect_used,
    reason = "a running columnar worker retains exactly one owned task until shutdown"
)]

//! Bounded owner for columnar projection apply catch-up and checkpoints.

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_columnar::{
    ColumnarError, ColumnarSnapshotRebuild, WorkerApplyOutcome, frontier_lag_sequences,
};
use riffdb_observability::{MetricRegistry, RequiredGauge};
use riffdb_storage_api::{
    ApplicationExportSnapshotPort, ApplicationExportSourceRecordV1, ColumnarProjectionArtifactV1,
    ColumnarProjectionControlRepository, ColumnarProjectionControlWriteResultV1,
    ColumnarProjectionGenerationRoleV1, ColumnarProjectionLayoutV1, ColumnarProjectionLifecycleV1,
    PreparedColumnarGenerationV1, StorageError, StorageErrorKind, StorageScanLimit,
    StoredColumnarProjectionGenerationV1,
};
use riffdb_types::FrontierPosition;

use crate::columnar_adapter::{
    ColumnarControlBinding, ColumnarEngineSlot, ColumnarRuntime, ColumnarSlotLifecycle,
};

const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(25);

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
    runtime: Option<Arc<ColumnarRuntime>>,
    activation_wake: Arc<crate::columnar_adapter::ColumnarActivationWake>,
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
        let activation_wake = runtime.activation_wake();
        let worker_activation_wake = Arc::clone(&activation_wake);
        let retained_runtime = Arc::clone(&runtime);
        let task = thread::Builder::new()
            .name("riffdb-columnar".to_owned())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                let mut state = ColumnarWorkerState::new(runtime.as_ref());
                let mut activation_epoch = worker_activation_wake.current();
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
                            worker_status.publish(match runtime.aggregate_lifecycle() {
                                ColumnarSlotLifecycle::Active => ColumnarWorkerReadiness::Ready,
                                ColumnarSlotLifecycle::Cold | ColumnarSlotLifecycle::Activating => {
                                    ColumnarWorkerReadiness::Starting
                                }
                                ColumnarSlotLifecycle::Failed => ColumnarWorkerReadiness::Degraded,
                                ColumnarSlotLifecycle::Stopped => ColumnarWorkerReadiness::Stopped,
                            });
                        }
                        Ok(ColumnarPassOutcome::StoppedBetweenEngines) => break,
                        Ok(ColumnarPassOutcome::AbandonedUnpublished) => {
                            worker_shutdown_status
                                .publish(ColumnarWorkerShutdownObservation::AbandonedUnpublished);
                            break;
                        }
                        Err(_) => worker_status.publish(ColumnarWorkerReadiness::Degraded),
                    }
                    activation_epoch =
                        worker_activation_wake.wait(activation_epoch, WORKER_POLL_INTERVAL);
                    if stop_requested(&worker_stop) {
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
            runtime: Some(retained_runtime),
            activation_wake,
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
            runtime: None,
            activation_wake: Arc::new(crate::columnar_adapter::ColumnarActivationWake::default()),
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
        self.activation_wake.signal();
        let task = self
            .task
            .take()
            .expect("a running columnar worker retains one task");
        if task.join().is_err() {
            self.shutdown_status
                .publish(ColumnarWorkerShutdownObservation::Failed);
            return Err(ColumnarWorkerShutdownError);
        }
        if let Some(runtime) = &self.runtime {
            runtime.stop_slots();
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

struct ColumnarWorkerState;

impl ColumnarWorkerState {
    fn new(runtime: &ColumnarRuntime) -> Self {
        let _ = runtime;
        Self
    }
}

fn maintain_common_generations(
    runtime: &ColumnarRuntime,
    stop_before_engine: &mut impl FnMut() -> bool,
    stop_before_next_page: &mut impl FnMut() -> bool,
) -> Result<ColumnarPassOutcome, ColumnarWorkerError> {
    for binding in runtime.control_bindings() {
        if stop_before_engine() {
            return Ok(ColumnarPassOutcome::StoppedBetweenEngines);
        }
        let slot = runtime
            .engine(binding.name())
            .map_err(map_port_error)?
            .ok_or(ColumnarWorkerError::Integrity)?;
        if slot.lifecycle().map_err(map_port_error)? != ColumnarSlotLifecycle::Active {
            continue;
        }
        runtime.record_population_pass();
        let control = recover_control(runtime, &binding)?;
        if control.target_definition_fingerprint() != binding.spec().definition_fingerprint()
            || control.target_spec_hash() != binding.spec().hash()
            || control.replay_limits() != binding.spec().replay_limits()
        {
            return Err(ColumnarWorkerError::Integrity);
        }
        let completed = if let Some(candidate) = control.candidate()
            && candidate.layout() == ColumnarProjectionLayoutV1::V1
            && matches!(
                control.lifecycle(),
                ColumnarProjectionLifecycleV1::Building | ColumnarProjectionLifecycleV1::Rebuilding
            ) {
            advance_v1_candidate(runtime, &binding, &control, stop_before_next_page)?
        } else if let Some(published) = control.servable_generation()
            && published.layout() == ColumnarProjectionLayoutV1::V1
        {
            advance_published_v1(runtime, &binding, &control, stop_before_next_page)?
        } else {
            true
        };
        if !completed {
            return Ok(ColumnarPassOutcome::AbandonedUnpublished);
        }
    }
    Ok(ColumnarPassOutcome::Completed)
}

fn advance_v1_candidate(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    control: &riffdb_storage_api::StoredColumnarProjectionControlV1,
    stop_before_next_page: &mut impl FnMut() -> bool,
) -> Result<bool, ColumnarWorkerError> {
    let candidate = control.candidate().ok_or(ColumnarWorkerError::Integrity)?;
    let mut successor = runtime
        .open_controlled_generation(binding, candidate)
        .map_err(ColumnarWorkerError::Apply)?;
    let mut current = control.clone();
    if candidate.artifact().is_none() {
        let Some(recovered) = build_fresh_v1_snapshot(
            runtime,
            binding,
            current,
            &mut successor,
            stop_before_next_page,
        )?
        else {
            return Ok(false);
        };
        current = recovered;
    } else {
        match successor
            .apply_available_for_worker(runtime.apply_source(), stop_before_next_page)
            .map_err(ColumnarWorkerError::Apply)?
        {
            WorkerApplyOutcome::Completed(_) => {}
            WorkerApplyOutcome::AbandonedUnpublished => return Ok(false),
        }
        let frontier = successor.processed_frontier().position();
        if frontier != candidate.frontier() {
            let manifest = successor.checkpoint().map_err(ColumnarWorkerError::Apply)?;
            let replacement = prepared_candidate_from_manifest(
                runtime,
                binding,
                current.candidate().ok_or(ColumnarWorkerError::Integrity)?,
                candidate
                    .snapshot_frontier()
                    .ok_or(ColumnarWorkerError::Integrity)?,
                &manifest,
            )?;
            successor = reopen_prepared_v1(runtime, binding, replacement.generation())?;
            if runtime
                .storage()
                .record_candidate_frontier(&current, &replacement)
                .map_err(map_storage_error)?
                == ColumnarProjectionControlWriteResultV1::StateChanged
            {
                return Ok(true);
            }
            current = recover_control(runtime, binding)?;
        }
    }
    let prepared = PreparedColumnarGenerationV1::v1(
        binding.spec().source().clone(),
        binding.spec().definition_semantics_hash(),
        current
            .candidate()
            .ok_or(ColumnarWorkerError::Integrity)?
            .clone(),
        runtime.process_generation(),
    )
    .map_err(|_| ColumnarWorkerError::Integrity)?;
    if runtime
        .storage()
        .publish_prepared_generation(&current, &prepared)
        .map_err(map_storage_error)?
        == ColumnarProjectionControlWriteResultV1::Applied
    {
        let published = recover_control(runtime, binding)?;
        let selected = published
            .servable_generation()
            .ok_or(ColumnarWorkerError::Integrity)?;
        if selected.generation()
            != current
                .candidate()
                .ok_or(ColumnarWorkerError::Integrity)?
                .generation()
        {
            return Err(ColumnarWorkerError::Integrity);
        }
        runtime
            .replace_vector_engine(binding.name(), successor, selected.generation())
            .map_err(map_port_error)?;
        let _ = runtime.notifier().notify(binding.name());
    }
    Ok(true)
}

fn build_fresh_v1_snapshot(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    control: riffdb_storage_api::StoredColumnarProjectionControlV1,
    successor: &mut riffdb_columnar::ColumnarEngine,
    stop_before_next_page: &mut impl FnMut() -> bool,
) -> Result<Option<riffdb_storage_api::StoredColumnarProjectionControlV1>, ColumnarWorkerError> {
    let snapshot = runtime
        .storage()
        .capture_application_export_snapshot(binding.spec().source().lineage())
        .map_err(map_storage_error)?;
    let snapshot_frontier = snapshot.binding().application_frontier().map_or(
        FrontierPosition::BeforeFirst,
        FrontierPosition::AppliedThrough,
    );
    let mut rebuild = ColumnarSnapshotRebuild::new(binding.definition().clone());
    let limit = StorageScanLimit::new(500).ok_or(ColumnarWorkerError::Integrity)?;
    let mut continuation: Option<Box<[u8]>> = None;
    loop {
        if stop_before_next_page() {
            return Ok(None);
        }
        let page = snapshot
            .read_application_export_entity_page(
                binding.definition().entity_type_id(),
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
        .install(successor, snapshot_frontier)
        .map_err(ColumnarWorkerError::Apply)?;
    match successor
        .apply_available_for_worker(runtime.apply_source(), stop_before_next_page)
        .map_err(ColumnarWorkerError::Apply)?
    {
        WorkerApplyOutcome::Completed(_) => {}
        WorkerApplyOutcome::AbandonedUnpublished => return Ok(None),
    }
    let manifest = successor.checkpoint().map_err(ColumnarWorkerError::Apply)?;
    let candidate = control.candidate().ok_or(ColumnarWorkerError::Integrity)?;
    let prepared = prepared_candidate_from_manifest(
        runtime,
        binding,
        candidate,
        snapshot_frontier,
        &manifest,
    )?;
    *successor = reopen_prepared_v1(runtime, binding, prepared.generation())?;
    if runtime
        .storage()
        .record_durable_snapshot(&control, &prepared)
        .map_err(map_storage_error)?
        == ColumnarProjectionControlWriteResultV1::StateChanged
    {
        return Err(ColumnarWorkerError::Unavailable);
    }
    recover_control(runtime, binding).map(Some)
}

fn prepared_candidate_from_manifest(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    candidate: &StoredColumnarProjectionGenerationV1,
    snapshot_frontier: FrontierPosition,
    manifest: &riffdb_columnar::ManifestV1,
) -> Result<PreparedColumnarGenerationV1, ColumnarWorkerError> {
    let (length, checksum) = manifest.artifact_identity();
    let artifact = ColumnarProjectionArtifactV1::new(length, checksum)
        .ok_or(ColumnarWorkerError::Integrity)?;
    let generation = StoredColumnarProjectionGenerationV1::prepared_candidate(
        candidate.generation(),
        ColumnarProjectionLayoutV1::V1,
        snapshot_frontier,
        manifest.durable_frontier,
        candidate.history_incarnation(),
        artifact,
        candidate.definition_fingerprint(),
        candidate.spec_hash(),
        None,
    )
    .map_err(|_| ColumnarWorkerError::Integrity)?;
    PreparedColumnarGenerationV1::v1(
        binding.spec().source().clone(),
        binding.spec().definition_semantics_hash(),
        generation,
        runtime.process_generation(),
    )
    .map_err(|_| ColumnarWorkerError::Integrity)
}

fn reopen_prepared_v1(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    generation: &StoredColumnarProjectionGenerationV1,
) -> Result<riffdb_columnar::ColumnarEngine, ColumnarWorkerError> {
    runtime
        .open_controlled_generation(binding, generation)
        .map_err(ColumnarWorkerError::Apply)
}

fn advance_published_v1(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    control: &riffdb_storage_api::StoredColumnarProjectionControlV1,
    stop_before_next_page: &mut impl FnMut() -> bool,
) -> Result<bool, ColumnarWorkerError> {
    let published = control
        .servable_generation()
        .ok_or(ColumnarWorkerError::Integrity)?;
    let mut successor = runtime
        .open_controlled_generation(binding, published)
        .map_err(ColumnarWorkerError::Apply)?;
    match successor
        .apply_available_for_worker(runtime.apply_source(), stop_before_next_page)
        .map_err(ColumnarWorkerError::Apply)?
    {
        WorkerApplyOutcome::Completed(_) => {}
        WorkerApplyOutcome::AbandonedUnpublished => return Ok(false),
    }
    if successor.processed_frontier().position() == published.frontier() {
        return Ok(true);
    }
    let manifest = successor.checkpoint().map_err(ColumnarWorkerError::Apply)?;
    let (length, checksum) = manifest.artifact_identity();
    let artifact = ColumnarProjectionArtifactV1::new(length, checksum)
        .ok_or(ColumnarWorkerError::Integrity)?;
    let replacement = StoredColumnarProjectionGenerationV1::selected(
        published.generation(),
        ColumnarProjectionLayoutV1::V1,
        manifest.durable_frontier,
        published.history_incarnation(),
        artifact,
        published.definition_fingerprint(),
        published.spec_hash(),
        None,
        ColumnarProjectionGenerationRoleV1::Published,
    )
    .map_err(|_| ColumnarWorkerError::Integrity)?;
    let replacement = PreparedColumnarGenerationV1::v1(
        binding.spec().source().clone(),
        binding.spec().definition_semantics_hash(),
        replacement,
        runtime.process_generation(),
    )
    .map_err(|_| ColumnarWorkerError::Integrity)?;
    successor = reopen_prepared_v1(runtime, binding, replacement.generation())?;
    if runtime
        .storage()
        .advance_published_v1(control, &replacement)
        .map_err(map_storage_error)?
        == ColumnarProjectionControlWriteResultV1::Applied
    {
        runtime
            .replace_vector_engine(binding.name(), successor, published.generation())
            .map_err(map_port_error)?;
        let _ = runtime.notifier().notify(binding.name());
    }
    Ok(true)
}

fn recover_control(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
) -> Result<riffdb_storage_api::StoredColumnarProjectionControlV1, ColumnarWorkerError> {
    runtime
        .storage()
        .recover_expected_control(binding.spec().source())
        .map_err(map_storage_error)?
        .ok_or(ColumnarWorkerError::Integrity)
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

fn activate_requested_slot(
    runtime: &ColumnarRuntime,
    name: &str,
    slot: &ColumnarEngineSlot,
) -> Result<bool, ColumnarWorkerError> {
    let spec = match runtime.current_activation_spec(name, slot) {
        Ok(spec) => spec,
        Err(error) => {
            slot.fail_activation(slot.generation());
            return Err(map_port_error(error));
        }
    };
    let Some(spec) = spec else {
        return Ok(false);
    };
    let generation = spec.generation();
    let (engine, generation) = match spec.open(runtime.history_incarnation()) {
        Ok(opened) => opened,
        Err(error) => {
            slot.fail_activation(generation);
            return Err(ColumnarWorkerError::Apply(error));
        }
    };
    slot.complete_activation(engine, generation)
        .map_err(map_port_error)?;
    let _ = runtime.notifier().notify(name);
    Ok(true)
}

fn run_columnar_pass(
    runtime: &ColumnarRuntime,
    metrics: Option<&MetricRegistry>,
    state: &mut ColumnarWorkerState,
    mut stop_before_engine: impl FnMut() -> bool,
    mut stop_before_next_page: impl FnMut() -> bool,
) -> Result<ColumnarPassOutcome, ColumnarWorkerError> {
    let _ = state;
    for (name, slot) in runtime
        .engines()
        .map_err(|_| ColumnarWorkerError::Unavailable)?
    {
        if stop_before_engine() {
            return Ok(ColumnarPassOutcome::StoppedBetweenEngines);
        }
        let _ = activate_requested_slot(runtime, &name, &slot)?;
    }
    let outcome =
        maintain_common_generations(runtime, &mut stop_before_engine, &mut stop_before_next_page)?;
    if outcome != ColumnarPassOutcome::Completed {
        return Ok(outcome);
    }

    let head = read_head(runtime)?;
    let mut max_lag = 0u64;
    for (_, slot) in runtime
        .engines()
        .map_err(|_| ColumnarWorkerError::Unavailable)?
    {
        if stop_before_engine() {
            return Ok(ColumnarPassOutcome::StoppedBetweenEngines);
        }
        let published = slot
            .with_engine(riffdb_columnar::ColumnarEngine::published_frontier_position)
            .map_err(map_port_error)?;
        if let Some(published) = published
            && let Some(lag) = frontier_lag_sequences(published, head)
        {
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

fn read_head(runtime: &ColumnarRuntime) -> Result<FrontierPosition, ColumnarWorkerError> {
    runtime
        .read_application_head()
        .map_err(|error| match error {
            riffdb_service::ColumnarPortError::Unavailable => ColumnarWorkerError::Unavailable,
            riffdb_service::ColumnarPortError::Integrity => ColumnarWorkerError::Integrity,
        })
}

#[cfg(test)]
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
    fn columnar_activation_shutdown_abandons_without_stop_caused_publication() {
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
