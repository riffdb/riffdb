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

#[cfg(test)]
use riffdb_columnar::ColumnarV2GenerationError;
use riffdb_columnar::{
    ColumnarError, ColumnarSnapshotRebuild, ColumnarV2StreamingError,
    PhysicalGenerationFingerprintV1, PreparedColumnarGenerationRepository,
    PreparedColumnarGenerationV1, ValidatedColumnarV2Generation, WorkerApplyOutcome,
    frontier_lag_sequences,
};
use riffdb_observability::{MetricRegistry, RequiredGauge};
use riffdb_storage_api::{
    ApplicationExportSnapshotPort, ApplicationExportSourceRecordV1, ColumnarProjectionArtifactV1,
    ColumnarProjectionControlRepository, ColumnarProjectionControlWriteResultV1,
    ColumnarProjectionFailureReasonV1, ColumnarProjectionFailureTargetV1,
    ColumnarProjectionGenerationRoleV1, ColumnarProjectionLayoutV1, ColumnarProjectionLifecycleV1,
    StorageError, StorageErrorKind, StorageScanLimit, StoredColumnarProjectionControlV1,
    StoredColumnarProjectionGenerationV1,
};
use riffdb_types::{FrontierPosition, ProjectionGeneration};

use crate::columnar_adapter::{
    ColumnarControlBinding, ColumnarEngineSlot, ColumnarRuntime, ColumnarSlotLifecycle,
    controlled_source_directory,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnarPublicationAttempt {
    Applied,
    StateChanged,
    StorageFailure,
    UnknownCommit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnarPublicationMode {
    Ordinary,
    #[cfg(test)]
    StateChangedAfterApplied,
    #[cfg(test)]
    StorageFailureBeforeCommit,
    #[cfg(test)]
    UnknownAfterApplied,
    #[cfg(test)]
    UnknownBeforeCommit,
}

#[cfg(test)]
pub(crate) use ColumnarPublicationMode as ColumnarPublicationTestMode;

impl ColumnarPublicationAttempt {
    const fn from_result(
        result: &Result<ColumnarProjectionControlWriteResultV1, StorageError>,
    ) -> Self {
        match result {
            Ok(ColumnarProjectionControlWriteResultV1::Applied) => Self::Applied,
            Ok(ColumnarProjectionControlWriteResultV1::StateChanged) => Self::StateChanged,
            Err(error) if matches!(error.kind(), StorageErrorKind::CommitStatusUnknown) => {
                Self::UnknownCommit
            }
            Err(_) => Self::StorageFailure,
        }
    }

    const fn requires_error(self) -> bool {
        matches!(self, Self::StorageFailure | Self::UnknownCommit)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnarPublicationResolution {
    InstallSelectedAndAcknowledge,
    InstallPredecessorWithoutAcknowledgement,
    RemainClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishedV1AdvanceResolution {
    InstallSelectedAndAcknowledge,
    InstallSelectedWithoutAcknowledgement,
    RemainClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedV2HeadResolution {
    Publish,
    ReplaceCandidate,
}

fn prepared_v2_head_resolution(
    candidate_frontier: FrontierPosition,
    authoritative_head: FrontierPosition,
) -> PreparedV2HeadResolution {
    if candidate_frontier == authoritative_head {
        PreparedV2HeadResolution::Publish
    } else {
        PreparedV2HeadResolution::ReplaceCandidate
    }
}

fn publication_resolution(
    _attempt: ColumnarPublicationAttempt,
    durable: &StoredColumnarProjectionControlV1,
    prepared: &PreparedColumnarGenerationV1,
) -> ColumnarPublicationResolution {
    publication_resolution_for(_attempt, durable, prepared.source(), prepared.generation())
}

fn publication_resolution_for(
    _attempt: ColumnarPublicationAttempt,
    durable: &StoredColumnarProjectionControlV1,
    prepared_source: &riffdb_types::ColumnarProjectionSourceV1,
    prepared_generation: &StoredColumnarProjectionGenerationV1,
) -> ColumnarPublicationResolution {
    let Some(selected) = durable.servable_generation() else {
        return ColumnarPublicationResolution::RemainClosed;
    };
    if durable.source() == prepared_source
        && selected_matches_prepared(selected, prepared_generation)
    {
        ColumnarPublicationResolution::InstallSelectedAndAcknowledge
    } else {
        ColumnarPublicationResolution::InstallPredecessorWithoutAcknowledgement
    }
}

fn selected_matches_prepared(
    selected: &StoredColumnarProjectionGenerationV1,
    prepared: &StoredColumnarProjectionGenerationV1,
) -> bool {
    selected.role() == ColumnarProjectionGenerationRoleV1::Published
        && prepared.role() == ColumnarProjectionGenerationRoleV1::Candidate
        && selected.generation() == prepared.generation()
        && selected.layout() == prepared.layout()
        && selected.frontier() == prepared.frontier()
        && selected.history_incarnation() == prepared.history_incarnation()
        && selected.artifact() == prepared.artifact()
        && selected.definition_fingerprint() == prepared.definition_fingerprint()
        && selected.spec_hash() == prepared.spec_hash()
        && selected.physical_generation_fingerprint() == prepared.physical_generation_fingerprint()
}

fn published_v1_advance_resolution(
    durable: &StoredColumnarProjectionControlV1,
    replacement: &StoredColumnarProjectionGenerationV1,
) -> PublishedV1AdvanceResolution {
    if durable.servable_generation() == Some(replacement) {
        PublishedV1AdvanceResolution::InstallSelectedAndAcknowledge
    } else if durable.servable_generation().is_some() {
        PublishedV1AdvanceResolution::InstallSelectedWithoutAcknowledgement
    } else {
        PublishedV1AdvanceResolution::RemainClosed
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
        let control = recover_control(runtime, &binding)?;
        let lifecycle = slot.lifecycle().map_err(map_port_error)?;
        if lifecycle != ColumnarSlotLifecycle::Active
            && !(matches!(
                lifecycle,
                ColumnarSlotLifecycle::Activating | ColumnarSlotLifecycle::Failed
            ) && control.servable_generation().is_none())
        {
            continue;
        }
        runtime.record_population_pass();
        if control.target_definition_fingerprint() != binding.spec().definition_fingerprint()
            || control.target_spec_hash() != binding.spec().hash()
            || control.replay_limits() != binding.spec().replay_limits()
        {
            return Err(ColumnarWorkerError::Integrity);
        }
        let completed = if control.lifecycle() == ColumnarProjectionLifecycleV1::Degraded
            && control.candidate().is_none()
            && control.failure().is_some_and(|failure| {
                failure.target() == ColumnarProjectionFailureTargetV1::Published
                    && failure.reason() == ColumnarProjectionFailureReasonV1::ArtifactInvalid
            }) {
            let published = control.published().ok_or(ColumnarWorkerError::Integrity)?;
            let physical = match published.layout() {
                ColumnarProjectionLayoutV1::V1 => None,
                ColumnarProjectionLayoutV1::V2 => Some(
                    *PhysicalGenerationFingerprintV1::compute(binding.definition().fingerprint())
                        .as_bytes(),
                ),
            };
            let _ = runtime
                .storage()
                .allocate_unservable_rebuild_candidate(
                    &control,
                    binding.spec().definition_fingerprint(),
                    binding.spec().hash(),
                    binding.spec().replay_limits(),
                    published.layout(),
                    physical,
                )
                .map_err(map_storage_error)?;
            true
        } else if let Some(candidate) = control.candidate()
            && control.lifecycle() == ColumnarProjectionLifecycleV1::Degraded
            && control.failure().is_some_and(|failure| {
                failure.target() == ColumnarProjectionFailureTargetV1::Candidate
                    && failure.generation() == Some(candidate.generation())
            })
        {
            let physical = match candidate.layout() {
                ColumnarProjectionLayoutV1::V1 => None,
                ColumnarProjectionLayoutV1::V2 => Some(
                    *PhysicalGenerationFingerprintV1::compute(binding.definition().fingerprint())
                        .as_bytes(),
                ),
            };
            let _ = runtime
                .storage()
                .replace_failed_candidate(&control, candidate.layout(), physical)
                .map_err(map_storage_error)?;
            true
        } else if let Some(candidate) = control.candidate()
            && candidate.layout() == ColumnarProjectionLayoutV1::V1
            && matches!(
                control.lifecycle(),
                ColumnarProjectionLifecycleV1::Building | ColumnarProjectionLifecycleV1::Rebuilding
            )
        {
            advance_v1_candidate(runtime, &binding, &slot, &control, stop_before_next_page)?
        } else if let Some(candidate) = control.candidate()
            && candidate.layout() == ColumnarProjectionLayoutV1::V2
            && control.servable_generation().is_none()
            && control.lifecycle() == ColumnarProjectionLifecycleV1::Rebuilding
        {
            advance_v2_candidate(runtime, &binding, &slot, &control, stop_before_next_page)?
        } else if let Some(published) = control.servable_generation()
            && published.layout() == ColumnarProjectionLayoutV1::V1
        {
            if !advance_published_v1(runtime, &binding, &control, stop_before_next_page)? {
                false
            } else {
                let current = recover_control(runtime, &binding)?;
                match current.candidate() {
                    Some(candidate)
                        if candidate.layout() == ColumnarProjectionLayoutV1::V2
                            && matches!(
                                current.lifecycle(),
                                ColumnarProjectionLifecycleV1::CatchingUp
                                    | ColumnarProjectionLifecycleV1::Rebuilding
                            ) =>
                    {
                        advance_v2_candidate(
                            runtime,
                            &binding,
                            &slot,
                            &current,
                            stop_before_next_page,
                        )?
                    }
                    None if current.lifecycle() == ColumnarProjectionLifecycleV1::Ready
                        && ValidatedColumnarV2Generation::supports_definition(
                            binding.definition(),
                        ) =>
                    {
                        begin_v2_candidate(runtime, &binding, &current)?;
                        true
                    }
                    None | Some(_) => true,
                }
            }
        } else if let Some(published) = control.servable_generation()
            && published.layout() == ColumnarProjectionLayoutV1::V2
        {
            match control.candidate() {
                Some(candidate)
                    if candidate.layout() == ColumnarProjectionLayoutV1::V2
                        && control.lifecycle() == ColumnarProjectionLifecycleV1::Rebuilding =>
                {
                    advance_v2_candidate(runtime, &binding, &slot, &control, stop_before_next_page)?
                }
                None if control.lifecycle() == ColumnarProjectionLifecycleV1::Ready => {
                    let head = runtime.read_application_head().map_err(map_port_error)?;
                    if head < published.frontier() {
                        return Err(ColumnarWorkerError::Integrity);
                    }
                    if head > published.frontier() {
                        let physical = PhysicalGenerationFingerprintV1::compute(
                            binding.definition().fingerprint(),
                        );
                        let _ = runtime
                            .storage()
                            .allocate_same_spec_candidate(&control, *physical.as_bytes())
                            .map_err(map_storage_error)?;
                    }
                    true
                }
                None | Some(_) => true,
            }
        } else {
            true
        };
        if !completed {
            return Ok(ColumnarPassOutcome::AbandonedUnpublished);
        }
        let durable = recover_control(runtime, &binding)?;
        runtime
            .reclaim_unselected_generations(&binding, &durable)
            .map_err(map_port_error)?;
    }
    Ok(ColumnarPassOutcome::Completed)
}

fn begin_v2_candidate(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    control: &StoredColumnarProjectionControlV1,
) -> Result<(), ColumnarWorkerError> {
    let physical = PhysicalGenerationFingerprintV1::compute(binding.definition().fingerprint());
    let _ = runtime
        .storage()
        .begin_v2_candidate(control, *physical.as_bytes())
        .map_err(map_storage_error)?;
    Ok(())
}

fn advance_v2_candidate(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    slot: &ColumnarEngineSlot,
    control: &StoredColumnarProjectionControlV1,
    stop_before_next_page: &mut impl FnMut() -> bool,
) -> Result<bool, ColumnarWorkerError> {
    let candidate = control.candidate().ok_or(ColumnarWorkerError::Integrity)?;
    if candidate.layout() != ColumnarProjectionLayoutV1::V2 {
        return Err(ColumnarWorkerError::Integrity);
    }
    if candidate.artifact().is_none() {
        return prepare_v2_candidate(
            runtime,
            binding,
            control,
            candidate.generation(),
            stop_before_next_page,
        );
    }

    // A prepared V2 root is immutable. If the authoritative head advanced,
    // replace it through the accepted never-reused allocation transition;
    // the ordinary V1 published pointer remains selected and byte-exact.
    if prepared_v2_head_resolution(
        candidate.frontier(),
        runtime.read_application_head().map_err(map_port_error)?,
    ) == PreparedV2HeadResolution::ReplaceCandidate
    {
        let physical = PhysicalGenerationFingerprintV1::compute(binding.definition().fingerprint());
        let result = if control.published().is_none()
            && control.predecessor().is_some()
            && control.servable_generation().is_none()
        {
            runtime.storage().allocate_unservable_rebuild_candidate(
                control,
                binding.spec().definition_fingerprint(),
                binding.spec().hash(),
                binding.spec().replay_limits(),
                ColumnarProjectionLayoutV1::V2,
                Some(*physical.as_bytes()),
            )
        } else {
            runtime
                .storage()
                .allocate_same_spec_candidate(control, *physical.as_bytes())
        };
        let _ = result.map_err(map_storage_error)?;
        return Ok(true);
    }

    let (successor, prepared) = runtime
        .open_prepared_generation(binding, candidate)
        .map_err(ColumnarWorkerError::Apply)?;
    let _ =
        publish_prepared_under_capture_gate(runtime, binding, slot, control, &prepared, successor)?;
    Ok(true)
}

fn prepare_v2_candidate(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    control: &StoredColumnarProjectionControlV1,
    generation: ProjectionGeneration,
    stop_before_next_page: &mut impl FnMut() -> bool,
) -> Result<bool, ColumnarWorkerError> {
    if stop_before_next_page() {
        return Ok(false);
    }
    let snapshot = runtime
        .storage()
        .capture_application_export_snapshot(binding.spec().source().lineage())
        .map_err(map_storage_error)?;
    let snapshot_frontier = snapshot.binding().application_frontier().map_or(
        FrontierPosition::BeforeFirst,
        FrontierPosition::AppliedThrough,
    );
    let source_directory =
        controlled_source_directory(runtime.projections_root(), binding.spec().hash());
    let generation_view = match ValidatedColumnarV2Generation::prepare_streaming(
        &source_directory,
        binding.definition().clone(),
        runtime.history_incarnation(),
        generation,
        snapshot_frontier,
        snapshot.as_ref(),
        runtime.apply_source(),
        binding.spec().replay_limits(),
        || {
            runtime
                .sample_columnar_replay_utc()
                .map_err(|_| ColumnarV2StreamingError::Clock)
        },
        stop_before_next_page,
    ) {
        Ok(generation) => generation,
        Err(ColumnarV2StreamingError::Cancelled) => return Ok(false),
        Err(ColumnarV2StreamingError::Clock) => return Err(ColumnarWorkerError::Unavailable),
        Err(error) => {
            let reason =
                v2_streaming_failure_reason(error).ok_or(ColumnarWorkerError::Integrity)?;
            let result = runtime
                .storage()
                .record_candidate_failure(control, reason)
                .map_err(map_storage_error)?;
            let durable = recover_control(runtime, binding)?;
            if result == ColumnarProjectionControlWriteResultV1::Applied
                && durable.failure().is_none_or(|failure| {
                    failure.reason() != reason
                        || failure.generation()
                            != control.candidate().map(|value| value.generation())
                })
            {
                return Err(ColumnarWorkerError::Integrity);
            }
            return Ok(true);
        }
    };
    let frontier = generation_view.root().frontier();
    let (length, checksum) = generation_view.artifact_identity();
    let artifact = ColumnarProjectionArtifactV1::new(length, checksum)
        .ok_or(ColumnarWorkerError::Integrity)?;
    let physical = *generation_view
        .root()
        .physical_generation_fingerprint()
        .as_bytes();
    let pointer = StoredColumnarProjectionGenerationV1::prepared_candidate(
        generation,
        ColumnarProjectionLayoutV1::V2,
        snapshot_frontier,
        frontier,
        runtime.history_incarnation(),
        artifact,
        binding.definition().fingerprint(),
        binding.spec().hash(),
        Some(physical),
    )
    .map_err(|_| ColumnarWorkerError::Integrity)?;
    let prepared = generation_view
        .prepared_generation(binding.spec(), pointer, runtime.process_generation())
        .map_err(|_| ColumnarWorkerError::Integrity)?;
    let _ = runtime
        .storage()
        .record_durable_snapshot(control, &prepared, runtime.process_generation())
        .map_err(map_storage_error)?;
    Ok(true)
}

const fn v2_streaming_failure_reason(
    error: ColumnarV2StreamingError,
) -> Option<ColumnarProjectionFailureReasonV1> {
    match error {
        ColumnarV2StreamingError::Cancelled => Some(ColumnarProjectionFailureReasonV1::Cancelled),
        ColumnarV2StreamingError::ReplayAge => Some(ColumnarProjectionFailureReasonV1::ReplayAge),
        ColumnarV2StreamingError::ReplayBytes => {
            Some(ColumnarProjectionFailureReasonV1::ReplayBytes)
        }
        ColumnarV2StreamingError::ReplayBacklog => {
            Some(ColumnarProjectionFailureReasonV1::ReplayBacklog)
        }
        ColumnarV2StreamingError::Clock => None,
        ColumnarV2StreamingError::BoundExceeded => {
            Some(ColumnarProjectionFailureReasonV1::ResourceLimit)
        }
        ColumnarV2StreamingError::Invalid => {
            Some(ColumnarProjectionFailureReasonV1::ArtifactInvalid)
        }
        ColumnarV2StreamingError::Io => Some(ColumnarProjectionFailureReasonV1::Storage),
    }
}

#[cfg(test)]
const fn v2_candidate_failure_reason(
    error: ColumnarV2GenerationError,
) -> ColumnarProjectionFailureReasonV1 {
    match error {
        ColumnarV2GenerationError::Io => ColumnarProjectionFailureReasonV1::Storage,
        ColumnarV2GenerationError::BoundExceeded => {
            ColumnarProjectionFailureReasonV1::ResourceLimit
        }
        ColumnarV2GenerationError::Invalid
        | ColumnarV2GenerationError::LogicalMismatch
        | ColumnarV2GenerationError::UnsupportedDefinition => {
            ColumnarProjectionFailureReasonV1::ArtifactInvalid
        }
    }
}

fn advance_v1_candidate(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    slot: &ColumnarEngineSlot,
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
            let pointer = candidate_pointer_from_manifest(
                current.candidate().ok_or(ColumnarWorkerError::Integrity)?,
                candidate
                    .snapshot_frontier()
                    .ok_or(ColumnarWorkerError::Integrity)?,
                &manifest,
            )?;
            let (reopened, replacement) = reopen_prepared_v1(runtime, binding, &pointer)?;
            successor = reopened;
            if runtime
                .storage()
                .record_candidate_frontier(&current, &replacement, runtime.process_generation())
                .map_err(map_storage_error)?
                == ColumnarProjectionControlWriteResultV1::StateChanged
            {
                return Ok(true);
            }
            current = recover_control(runtime, binding)?;
        }
    }
    let prepared = successor
        .prepared_v1_generation(
            binding.spec(),
            current
                .candidate()
                .ok_or(ColumnarWorkerError::Integrity)?
                .clone(),
            runtime.process_generation(),
        )
        .map_err(|_| ColumnarWorkerError::Integrity)?;
    let _ = publish_prepared_under_capture_gate(
        runtime, binding, slot, &current, &prepared, successor,
    )?;
    Ok(true)
}

fn publish_prepared_under_capture_gate(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    slot: &ColumnarEngineSlot,
    expected: &StoredColumnarProjectionControlV1,
    prepared: &PreparedColumnarGenerationV1,
    successor: riffdb_columnar::ColumnarEngine,
) -> Result<bool, ColumnarWorkerError> {
    publish_prepared_under_capture_gate_controlled(
        runtime,
        binding,
        slot,
        expected,
        prepared,
        successor,
        ColumnarPublicationMode::Ordinary,
        || {},
    )
}

#[cfg(test)]
#[allow(
    clippy::too_many_arguments,
    reason = "the proof injects only result and schedule beside the exact production publication inputs"
)]
pub(crate) fn publish_prepared_under_capture_gate_for_test(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    slot: &ColumnarEngineSlot,
    expected: &StoredColumnarProjectionControlV1,
    prepared: &PreparedColumnarGenerationV1,
    successor: riffdb_columnar::ColumnarEngine,
    mode: ColumnarPublicationTestMode,
    after_gate_closed: impl FnOnce(),
) -> Result<bool, ()> {
    publish_prepared_under_capture_gate_controlled(
        runtime,
        binding,
        slot,
        expected,
        prepared,
        successor,
        mode,
        after_gate_closed,
    )
    .map_err(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn publish_prepared_under_capture_gate_controlled(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    slot: &ColumnarEngineSlot,
    expected: &StoredColumnarProjectionControlV1,
    prepared: &PreparedColumnarGenerationV1,
    successor: riffdb_columnar::ColumnarEngine,
    mode: ColumnarPublicationMode,
    after_gate_closed: impl FnOnce(),
) -> Result<bool, ColumnarWorkerError> {
    let mut gate = slot.close_capture_gate().map_err(map_port_error)?;
    after_gate_closed();
    abort_publication_test_process_at("before-cas");
    let publication = issue_prepared_publication(runtime, expected, prepared, mode);
    abort_publication_test_process_at("after-cas");
    let attempt = ColumnarPublicationAttempt::from_result(&publication);

    // The write result is never publication evidence. This exact durable
    // reread is unconditional and occurs while the query-capture gate remains
    // closed, including for storage failures with uncertain commit outcome.
    let durable = match runtime
        .storage()
        .recover_expected_control(binding.spec().source())
    {
        Ok(Some(control)) => control,
        Ok(None) => {
            gate.remain_closed(None);
            return Err(ColumnarWorkerError::Integrity);
        }
        Err(error) => {
            gate.remain_closed(Some(prepared.generation().generation()));
            return Err(map_storage_error(error));
        }
    };
    abort_publication_test_process_at("after-reread");
    if durable.source() != binding.spec().source()
        || durable.target_definition_fingerprint() != binding.spec().definition_fingerprint()
        || durable.target_spec_hash() != binding.spec().hash()
        || durable.replay_limits() != binding.spec().replay_limits()
    {
        gate.remain_closed(
            durable
                .servable_generation()
                .map(|value| value.generation()),
        );
        return Err(ColumnarWorkerError::Integrity);
    }

    match publication_resolution(attempt, &durable, prepared) {
        ColumnarPublicationResolution::InstallSelectedAndAcknowledge => {
            let selected = durable
                .servable_generation()
                .ok_or(ColumnarWorkerError::Integrity)?;
            if let Some(retired) = gate
                .install_selected(successor, selected.generation())
                .map_err(map_port_error)?
            {
                runtime.defer_retirement(retired).map_err(map_port_error)?;
            }
            abort_publication_test_process_at("after-view-install");
            drop(gate);
            let _ = runtime.notifier().notify(binding.name());
            Ok(true)
        }
        ColumnarPublicationResolution::InstallPredecessorWithoutAcknowledgement => {
            let selected = durable
                .servable_generation()
                .ok_or(ColumnarWorkerError::Integrity)?;
            let predecessor = match runtime.open_controlled_generation(binding, selected) {
                Ok(engine) => engine,
                Err(error) => {
                    gate.remain_closed(Some(selected.generation()));
                    return Err(ColumnarWorkerError::Apply(error));
                }
            };
            if let Some(retired) = gate
                .install_selected(predecessor, selected.generation())
                .map_err(map_port_error)?
            {
                runtime.defer_retirement(retired).map_err(map_port_error)?;
            }
            drop(gate);
            if attempt.requires_error() {
                match publication {
                    Err(error) => Err(map_storage_error(error)),
                    Ok(_) => Err(ColumnarWorkerError::Integrity),
                }
            } else {
                Ok(false)
            }
        }
        ColumnarPublicationResolution::RemainClosed => {
            gate.remain_closed(durable.published().map(|value| value.generation()));
            Err(ColumnarWorkerError::Integrity)
        }
    }
}

fn issue_prepared_publication(
    runtime: &ColumnarRuntime,
    expected: &StoredColumnarProjectionControlV1,
    prepared: &PreparedColumnarGenerationV1,
    mode: ColumnarPublicationMode,
) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
    #[cfg(test)]
    let mode = if mode == ColumnarPublicationMode::Ordinary
        && std::env::var("RIFFDB_COLUMNAR_PUBLICATION_RESULT").as_deref()
            == Ok("unknown-after-applied")
    {
        ColumnarPublicationMode::UnknownAfterApplied
    } else {
        mode
    };
    match mode {
        ColumnarPublicationMode::Ordinary => runtime.storage().publish_prepared_generation(
            expected,
            prepared,
            runtime.process_generation(),
        ),
        #[cfg(test)]
        ColumnarPublicationMode::StateChangedAfterApplied => {
            if runtime.storage().publish_prepared_generation(
                expected,
                prepared,
                runtime.process_generation(),
            )? != ColumnarProjectionControlWriteResultV1::Applied
            {
                return Err(StorageError::new(
                    StorageErrorKind::InvariantViolation,
                    None,
                ));
            }
            runtime.storage().publish_prepared_generation(
                expected,
                prepared,
                runtime.process_generation(),
            )
        }
        #[cfg(test)]
        ColumnarPublicationMode::StorageFailureBeforeCommit => {
            Err(StorageError::new(StorageErrorKind::Unavailable, None))
        }
        #[cfg(test)]
        ColumnarPublicationMode::UnknownAfterApplied => {
            if runtime.storage().publish_prepared_generation(
                expected,
                prepared,
                runtime.process_generation(),
            )? != ColumnarProjectionControlWriteResultV1::Applied
            {
                return Err(StorageError::new(
                    StorageErrorKind::InvariantViolation,
                    None,
                ));
            }
            Err(StorageError::new(
                StorageErrorKind::CommitStatusUnknown,
                None,
            ))
        }
        #[cfg(test)]
        ColumnarPublicationMode::UnknownBeforeCommit => Err(StorageError::new(
            StorageErrorKind::CommitStatusUnknown,
            None,
        )),
    }
}

#[cfg(test)]
fn abort_publication_test_process_at(boundary: &str) {
    if std::env::var("RIFFDB_COLUMNAR_PUBLICATION_ABORT_AT").as_deref() == Ok(boundary) {
        std::process::abort();
    }
}

#[cfg(not(test))]
fn abort_publication_test_process_at(_boundary: &str) {}

#[cfg(test)]
fn abort_v1_publication_test_process_at(boundary: &str) {
    if std::env::var("RIFFDB_COLUMNAR_V1_PUBLICATION_ABORT_AT").as_deref() == Ok(boundary) {
        std::process::abort();
    }
}

#[cfg(not(test))]
fn abort_v1_publication_test_process_at(_boundary: &str) {}

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
    let pointer = candidate_pointer_from_manifest(candidate, snapshot_frontier, &manifest)?;
    let (reopened, prepared) = reopen_prepared_v1(runtime, binding, &pointer)?;
    *successor = reopened;
    if runtime
        .storage()
        .record_durable_snapshot(&control, &prepared, runtime.process_generation())
        .map_err(map_storage_error)?
        == ColumnarProjectionControlWriteResultV1::StateChanged
    {
        return Err(ColumnarWorkerError::Unavailable);
    }
    recover_control(runtime, binding).map(Some)
}

fn candidate_pointer_from_manifest(
    candidate: &StoredColumnarProjectionGenerationV1,
    snapshot_frontier: FrontierPosition,
    manifest: &riffdb_columnar::ManifestV1,
) -> Result<StoredColumnarProjectionGenerationV1, ColumnarWorkerError> {
    let (length, checksum) = manifest.artifact_identity();
    let artifact = ColumnarProjectionArtifactV1::new(length, checksum)
        .ok_or(ColumnarWorkerError::Integrity)?;
    StoredColumnarProjectionGenerationV1::prepared_candidate(
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
    .map_err(|_| ColumnarWorkerError::Integrity)
}

fn reopen_prepared_v1(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    generation: &StoredColumnarProjectionGenerationV1,
) -> Result<
    (
        riffdb_columnar::ColumnarEngine,
        PreparedColumnarGenerationV1,
    ),
    ColumnarWorkerError,
> {
    runtime
        .open_prepared_generation(binding, generation)
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
    let (reopened, replacement) = reopen_prepared_v1(runtime, binding, &replacement)?;
    successor = reopened;
    advance_published_v1_under_capture_gate(runtime, binding, control, &replacement, successor)
}

fn advance_published_v1_under_capture_gate(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    expected: &StoredColumnarProjectionControlV1,
    replacement: &PreparedColumnarGenerationV1,
    successor: riffdb_columnar::ColumnarEngine,
) -> Result<bool, ColumnarWorkerError> {
    advance_published_v1_under_capture_gate_controlled(
        runtime,
        binding,
        expected,
        replacement,
        successor,
        ColumnarPublicationMode::Ordinary,
        || {},
    )
}

#[cfg(test)]
#[allow(
    clippy::too_many_arguments,
    reason = "the deterministic proof adds only a closed result class and gate hook to production inputs"
)]
pub(crate) fn advance_published_v1_under_capture_gate_for_test(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    expected: &StoredColumnarProjectionControlV1,
    replacement: &PreparedColumnarGenerationV1,
    successor: riffdb_columnar::ColumnarEngine,
    mode: ColumnarPublicationTestMode,
    after_gate_closed: impl FnOnce(),
) -> Result<bool, ()> {
    advance_published_v1_under_capture_gate_controlled(
        runtime,
        binding,
        expected,
        replacement,
        successor,
        mode,
        after_gate_closed,
    )
    .map_err(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn advance_published_v1_under_capture_gate_controlled(
    runtime: &ColumnarRuntime,
    binding: &ColumnarControlBinding,
    expected: &StoredColumnarProjectionControlV1,
    replacement: &PreparedColumnarGenerationV1,
    successor: riffdb_columnar::ColumnarEngine,
    mode: ColumnarPublicationMode,
    after_gate_closed: impl FnOnce(),
) -> Result<bool, ColumnarWorkerError> {
    let slot = runtime
        .engine(binding.name())
        .map_err(map_port_error)?
        .ok_or(ColumnarWorkerError::Integrity)?;
    let mut gate = slot.close_capture_gate().map_err(map_port_error)?;
    after_gate_closed();
    abort_v1_publication_test_process_at("before-cas");
    let result = issue_published_v1_advance(runtime, expected, replacement, mode);
    abort_v1_publication_test_process_at("after-cas");
    let attempt = ColumnarPublicationAttempt::from_result(&result);
    let durable = match runtime
        .storage()
        .recover_expected_control(binding.spec().source())
    {
        Ok(Some(durable)) => durable,
        Ok(None) => {
            gate.remain_closed(expected.published().map(|value| value.generation()));
            return Err(ColumnarWorkerError::Integrity);
        }
        Err(error) => {
            gate.remain_closed(expected.published().map(|value| value.generation()));
            return Err(map_storage_error(error));
        }
    };
    abort_v1_publication_test_process_at("after-reread");
    if durable.source() != binding.spec().source()
        || durable.target_definition_fingerprint() != binding.spec().definition_fingerprint()
        || durable.target_spec_hash() != binding.spec().hash()
        || durable.replay_limits() != binding.spec().replay_limits()
    {
        gate.remain_closed(durable.published().map(|value| value.generation()));
        return Err(ColumnarWorkerError::Integrity);
    }
    match published_v1_advance_resolution(&durable, replacement.generation()) {
        PublishedV1AdvanceResolution::InstallSelectedAndAcknowledge => {
            if let Some(retired) = gate
                .install_selected(successor, replacement.generation().generation())
                .map_err(map_port_error)?
            {
                runtime.defer_retirement(retired).map_err(map_port_error)?;
            }
            abort_v1_publication_test_process_at("after-view-install");
            drop(gate);
            let _ = runtime.notifier().notify(binding.name());
            Ok(true)
        }
        PublishedV1AdvanceResolution::InstallSelectedWithoutAcknowledgement => {
            let selected = durable
                .servable_generation()
                .ok_or(ColumnarWorkerError::Integrity)?;
            let durable_engine = match runtime.open_controlled_generation(binding, selected) {
                Ok(engine) => engine,
                Err(error) => {
                    gate.remain_closed(Some(selected.generation()));
                    return Err(ColumnarWorkerError::Apply(error));
                }
            };
            if let Some(retired) = gate
                .install_selected(durable_engine, selected.generation())
                .map_err(map_port_error)?
            {
                runtime.defer_retirement(retired).map_err(map_port_error)?;
            }
            drop(gate);
            if attempt.requires_error() {
                match result {
                    Err(error) => Err(map_storage_error(error)),
                    Ok(_) => Err(ColumnarWorkerError::Integrity),
                }
            } else {
                Ok(false)
            }
        }
        PublishedV1AdvanceResolution::RemainClosed => {
            gate.remain_closed(durable.published().map(|value| value.generation()));
            Err(ColumnarWorkerError::Integrity)
        }
    }
}

fn issue_published_v1_advance(
    runtime: &ColumnarRuntime,
    expected: &StoredColumnarProjectionControlV1,
    replacement: &PreparedColumnarGenerationV1,
    mode: ColumnarPublicationMode,
) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
    #[cfg(test)]
    let mode = if mode == ColumnarPublicationMode::Ordinary
        && std::env::var("RIFFDB_COLUMNAR_PUBLICATION_RESULT").as_deref()
            == Ok("unknown-after-applied")
    {
        ColumnarPublicationMode::UnknownAfterApplied
    } else {
        mode
    };
    let issue = || {
        runtime
            .storage()
            .advance_published_v1(expected, replacement, runtime.process_generation())
    };
    match mode {
        ColumnarPublicationMode::Ordinary => issue(),
        #[cfg(test)]
        ColumnarPublicationMode::StateChangedAfterApplied => {
            if issue()? != ColumnarProjectionControlWriteResultV1::Applied {
                return Err(StorageError::new(
                    StorageErrorKind::InvariantViolation,
                    None,
                ));
            }
            issue()
        }
        #[cfg(test)]
        ColumnarPublicationMode::StorageFailureBeforeCommit => {
            Err(StorageError::new(StorageErrorKind::Unavailable, None))
        }
        #[cfg(test)]
        ColumnarPublicationMode::UnknownAfterApplied => {
            if issue()? != ColumnarProjectionControlWriteResultV1::Applied {
                return Err(StorageError::new(
                    StorageErrorKind::InvariantViolation,
                    None,
                ));
            }
            Err(StorageError::new(
                StorageErrorKind::CommitStatusUnknown,
                None,
            ))
        }
        #[cfg(test)]
        ColumnarPublicationMode::UnknownBeforeCommit => Err(StorageError::new(
            StorageErrorKind::CommitStatusUnknown,
            None,
        )),
    }
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
            if let Some(generation) = generation {
                record_selected_open_failure(runtime, name, generation)?;
            }
            return Err(ColumnarWorkerError::Apply(error));
        }
    };
    slot.complete_activation(engine, generation)
        .map_err(map_port_error)?;
    let _ = runtime.notifier().notify(name);
    Ok(true)
}

fn record_selected_open_failure(
    runtime: &ColumnarRuntime,
    name: &str,
    generation: ProjectionGeneration,
) -> Result<(), ColumnarWorkerError> {
    let binding = runtime
        .control_bindings()
        .into_iter()
        .find(|binding| binding.name() == name)
        .ok_or(ColumnarWorkerError::Integrity)?;
    let control = recover_control(runtime, &binding)?;
    let Some(published) = control.published() else {
        return Ok(());
    };
    if published.generation() != generation {
        return Ok(());
    }
    let _ = runtime.storage().record_published_failure(&control);
    let durable = recover_control(runtime, &binding)?;
    if durable.published() == Some(published) && durable.servable_generation().is_none() {
        Ok(())
    } else {
        Err(ColumnarWorkerError::Integrity)
    }
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

#[cfg(test)]
pub(crate) fn run_one_test_pass_stopping_before_page(runtime: &ColumnarRuntime) -> bool {
    let mut state = ColumnarWorkerState::new(runtime);
    matches!(
        run_columnar_pass(runtime, None, &mut state, || false, || true),
        Ok(ColumnarPassOutcome::AbandonedUnpublished)
    )
}

#[cfg(test)]
pub(crate) fn activate_one_test_slot(runtime: &ColumnarRuntime, name: &str) -> bool {
    let Ok(Some(slot)) = runtime.engine(name) else {
        return false;
    };
    activate_requested_slot(runtime, name, &slot).unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn apply_available_for_worker_for_test(
    runtime: &ColumnarRuntime,
    engine: &mut riffdb_columnar::ColumnarEngine,
) -> Result<WorkerApplyOutcome, ColumnarError> {
    engine.apply_available_for_worker(runtime.apply_source(), || false)
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

#[cfg(test)]
mod publication_tests {
    use riffdb_columnar::{ColumnarV2GenerationError, ColumnarV2StreamingError};
    use riffdb_storage_api::{
        ColumnarProjectionArtifactV1, ColumnarProjectionLayoutV1, ColumnarProjectionReplayLimitsV1,
        StorageError, StorageErrorKind, StoredColumnarProjectionControlV1,
        StoredColumnarProjectionGenerationV1,
    };
    use riffdb_types::{
        ColumnarProjectionSourceV1, ColumnarProjectionSpecHashV1, ContractLineage,
        DefinitionFingerprint, FrontierPosition,
    };

    use super::{
        ColumnarPublicationAttempt, ColumnarPublicationResolution, PreparedV2HeadResolution,
        prepared_v2_head_resolution, publication_resolution_for, v2_candidate_failure_reason,
        v2_streaming_failure_reason,
    };

    fn prepared_v2() -> (
        StoredColumnarProjectionControlV1,
        ColumnarProjectionSourceV1,
        StoredColumnarProjectionGenerationV1,
        StoredColumnarProjectionControlV1,
    ) {
        let definition = DefinitionFingerprint::from_bytes([0x11; 32]);
        let source = ColumnarProjectionSourceV1::scalar(
            ContractLineage::new("gate-test").expect("lineage"),
            definition,
        );
        let spec = ColumnarProjectionSpecHashV1::from_bytes([0x22; 32]);
        let limits = ColumnarProjectionReplayLimitsV1::new(60, 1_024, 10).expect("limits");
        let initial = StoredColumnarProjectionControlV1::initialize_fresh_v1(
            source.clone(),
            definition,
            spec,
            limits,
            1,
        )
        .expect("initial");
        let initial_candidate = initial.candidate().expect("candidate");
        let matched_frontier = FrontierPosition::AppliedThrough(
            riffdb_types::CommitSequence::new(3).expect("matched frontier"),
        );
        let v1_pointer = StoredColumnarProjectionGenerationV1::prepared_candidate(
            initial_candidate.generation(),
            ColumnarProjectionLayoutV1::V1,
            FrontierPosition::BeforeFirst,
            matched_frontier,
            1,
            ColumnarProjectionArtifactV1::new(64, [0x31; 32]).expect("V1 artifact"),
            definition,
            spec,
            None,
        )
        .expect("V1 pointer");
        let ready_v1 = initial
            .record_durable_snapshot(v1_pointer)
            .expect("prepare V1")
            .publish_prepared_generation(matched_frontier)
            .expect("publish V1");
        let physical = [0x41; 32];
        let catching_up = ready_v1.begin_v2_candidate(physical).expect("begin V2");
        let candidate = catching_up.candidate().expect("V2 candidate");
        let v2_pointer = StoredColumnarProjectionGenerationV1::prepared_candidate(
            candidate.generation(),
            ColumnarProjectionLayoutV1::V2,
            FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
            matched_frontier,
            1,
            ColumnarProjectionArtifactV1::new(128, [0x51; 32]).expect("root artifact"),
            definition,
            spec,
            Some(physical),
        )
        .expect("V2 pointer");
        let prepared_control = catching_up
            .record_durable_snapshot(v2_pointer.clone())
            .expect("prepare V2");
        let published = prepared_control
            .clone()
            .publish_prepared_generation(matched_frontier)
            .expect("publish V2");
        (prepared_control, source, v2_pointer, published)
    }

    // req: PRJ-006, PRJ-008, PRJ-009, OQ-020, OQ-022, PERF-007
    #[test]
    fn publication_attempt_taxonomy_and_durable_resolution_are_closed() {
        let (prepared_control, source, prepared, published) = prepared_v2();
        assert_eq!(
            ColumnarPublicationAttempt::from_result(&Err(StorageError::new(
                StorageErrorKind::Unavailable,
                None,
            ))),
            ColumnarPublicationAttempt::StorageFailure,
            "a proved non-commit is not an unknown commit"
        );
        assert_eq!(
            ColumnarPublicationAttempt::from_result(&Err(StorageError::new(
                StorageErrorKind::CommitStatusUnknown,
                None,
            ))),
            ColumnarPublicationAttempt::UnknownCommit,
            "only the closed unknown-commit class is uncertain"
        );
        for attempt in [
            ColumnarPublicationAttempt::Applied,
            ColumnarPublicationAttempt::StateChanged,
            ColumnarPublicationAttempt::StorageFailure,
            ColumnarPublicationAttempt::UnknownCommit,
        ] {
            assert_eq!(
                publication_resolution_for(attempt, &published, &source, &prepared),
                ColumnarPublicationResolution::InstallSelectedAndAcknowledge,
                "durable exact selection, not the ambiguous attempt result, owns acknowledgement"
            );
            assert_eq!(
                publication_resolution_for(attempt, &prepared_control, &source, &prepared),
                ColumnarPublicationResolution::InstallPredecessorWithoutAcknowledgement,
                "an unselected prepared root cannot be acknowledged"
            );
        }

        let selected_corrupt = published
            .record_published_failure()
            .expect("selected corruption");
        assert_eq!(
            publication_resolution_for(
                ColumnarPublicationAttempt::StorageFailure,
                &selected_corrupt,
                &source,
                &prepared,
            ),
            ColumnarPublicationResolution::RemainClosed,
            "selected V2 corruption has no V1 fallback"
        );
    }

    // req: PRJ-008, PRJ-009
    #[test]
    fn corrupt_selected_control_has_no_servable_generation() {
        let (_, source, prepared, published) = prepared_v2();
        let selected_corrupt = published
            .record_published_failure()
            .expect("selected corruption");
        assert!(selected_corrupt.servable_generation().is_none());
        assert_eq!(
            publication_resolution_for(
                ColumnarPublicationAttempt::UnknownCommit,
                &selected_corrupt,
                &source,
                &prepared,
            ),
            ColumnarPublicationResolution::RemainClosed
        );
    }

    // req: PRJ-002, PRJ-004, PRJ-006, PRJ-010, OQ-020
    #[test]
    fn retained_tail_head_movement_replaces_v2_candidate_and_preserves_v1_selection() {
        let (prepared_control, _, _, _) = prepared_v2();
        let stale = prepared_control.candidate().expect("prepared V2").clone();
        let published = prepared_control.published().expect("selected V1").clone();
        let moved_head = FrontierPosition::AppliedThrough(
            riffdb_types::CommitSequence::new(4).expect("advanced head"),
        );
        assert_eq!(
            prepared_v2_head_resolution(stale.frontier(), moved_head),
            PreparedV2HeadResolution::ReplaceCandidate
        );

        let replacement = prepared_control
            .allocate_same_spec_candidate(
                stale
                    .physical_generation_fingerprint()
                    .expect("physical fingerprint"),
            )
            .expect("replace stale immutable candidate");
        assert_eq!(replacement.published(), Some(&published));
        let candidate = replacement.candidate().expect("replacement candidate");
        assert!(candidate.generation() > stale.generation());
        assert!(candidate.artifact().is_none());
        assert_eq!(candidate.frontier(), FrontierPosition::BeforeFirst);
        assert_eq!(
            replacement.retention_frontier(),
            Some(FrontierPosition::BeforeFirst),
            "new snapshot cannot release retained tail before durable preparation"
        );
    }

    // req: PRJ-002, PRJ-004, PRJ-006, PRJ-009, PRJ-010, OQ-020
    #[test]
    fn columnar_v2_enospc_before_publication_preserves_selected_generation() {
        let (prepared_control, _, _, _) = prepared_v2();
        let selected = prepared_control.published().expect("selected V1").clone();
        assert_eq!(
            v2_candidate_failure_reason(ColumnarV2GenerationError::Io),
            riffdb_storage_api::ColumnarProjectionFailureReasonV1::Storage
        );
        let degraded = prepared_control
            .record_candidate_failure(v2_candidate_failure_reason(ColumnarV2GenerationError::Io))
            .expect("record ENOSPC-class failure");
        assert_eq!(degraded.servable_generation(), Some(&selected));
        assert_eq!(degraded.published(), Some(&selected));
        assert!(degraded.candidate().is_some());
    }

    // req: PRJ-002, PRJ-004, PRJ-006, PRJ-009, PRJ-010, OQ-020
    #[test]
    fn replay_limit_failures_retain_selection_until_exact_replacement() {
        let (prepared_control, _, _, _) = prepared_v2();
        let selected = prepared_control.published().expect("selected V1").clone();
        for (error, reason) in [
            (
                ColumnarV2StreamingError::ReplayAge,
                riffdb_storage_api::ColumnarProjectionFailureReasonV1::ReplayAge,
            ),
            (
                ColumnarV2StreamingError::ReplayBytes,
                riffdb_storage_api::ColumnarProjectionFailureReasonV1::ReplayBytes,
            ),
            (
                ColumnarV2StreamingError::ReplayBacklog,
                riffdb_storage_api::ColumnarProjectionFailureReasonV1::ReplayBacklog,
            ),
        ] {
            assert_eq!(v2_streaming_failure_reason(error), Some(reason));
            let failed = prepared_control
                .clone()
                .record_candidate_failure(reason)
                .expect("accepted RecordCandidateFailure");
            assert_eq!(failed.published(), Some(&selected));
            assert_eq!(failed.servable_generation(), Some(&selected));
            assert_eq!(failed.retention_frontier(), Some(selected.frontier()));
            assert_eq!(failed.failure().expect("failure").reason(), reason);
            let failed_generation = failed.candidate().expect("failed candidate").generation();
            let replacement = failed
                .replace_failed_candidate(
                    ColumnarProjectionLayoutV1::V2,
                    prepared_control
                        .candidate()
                        .and_then(|candidate| candidate.physical_generation_fingerprint()),
                )
                .expect("exact replacement transition");
            assert_eq!(replacement.published(), Some(&selected));
            assert!(replacement.failure().is_none());
            assert!(replacement.candidate().expect("replacement").generation() > failed_generation);
        }

        let source = include_str!("columnar_worker.rs");
        let start = source
            .find("Err(error) => {")
            .expect("streaming failure arm");
        let arm = &source[start
            ..source[start..]
                .find("let frontier = generation_view")
                .map(|offset| start + offset)
                .expect("failure arm end")];
        let record = arm
            .find("record_candidate_failure")
            .expect("durable failure CAS");
        let reread = arm.find("recover_control").expect("durable reread");
        assert!(record < reread);
    }
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

    // req: PRJ-004, PRJ-009, OQ-020
    #[test]
    fn production_v2_worker_uses_streaming_rebuild_without_generation_wide_expected_maps() {
        let source = include_str!("columnar_worker.rs");
        let start = source
            .find("fn prepare_v2_candidate")
            .expect("V2 candidate worker");
        let end = source[start..]
            .find("const fn v2_streaming_failure_reason")
            .map(|offset| start + offset)
            .expect("V2 candidate boundary");
        let candidate = &source[start..end];
        assert!(candidate.contains("prepare_streaming"));
        assert!(!candidate.contains("ColumnarSnapshotRebuild"));
        assert!(!candidate.contains("published_snapshot"));
        assert!(!candidate.contains("let expected"));
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
