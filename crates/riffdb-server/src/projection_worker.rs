#![expect(
    clippy::expect_used,
    reason = "a running projection worker retains one task and its fixed recovery page is nonzero"
)]

//! Bounded owner for projection recovery and contiguous live catch-up.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogError, ResolvedProjectionPlan, ValidatedMigrationPlan,
};
use riffdb_commit::{
    MigrationProjectionBuildObservation, MigrationProjectionBuildPort, MigrationProjectionError,
};
use riffdb_projection::{
    ProjectionController, ProjectionCoreError, ProjectionCoreErrorKind, ProjectionEvaluationError,
    ProjectionInitializationResult, ProjectionNotifier, ProjectionRecoveryError,
    ProjectionRecoveryOutcome, ProjectionSchemaRegistry, evaluate_and_prepare_projection_commit,
    validate_and_recover_projection_generation,
};
use riffdb_storage_api::{
    AuthoritativeScanReader, CatalogRepository, CheckedProjectionSchema, CommitScanPageV1,
    CommitScanRequest, ProjectionApplyResult, ProjectionControlResult, ProjectionFailureCodeV1,
    ProjectionGenerationPosition, ProjectionLifecycleV1, ProjectionQueryReader,
    ProjectionRecoveryPageLimit, StorageError, StorageScanLimit, StoredProjectionControlV1,
};
use riffdb_storage_redb::RedbMigrationProjectionPorts;
use riffdb_types::{CommitSequence, FrontierPosition, ProjectionGeneration, ProjectionIdentity};

use crate::storage::SharedRedbOperationalPorts;

const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(25);
const COMMIT_SCAN_ROWS: u16 = 500;
const RECOVERY_PAGE_ROWS: u16 = 500;
const MAX_CONTROL_TRANSITIONS_PER_PASS: usize = 32;

/// Projection-owned fresh-generation builder for one private migration stage.
pub(crate) struct MigrationProjectionBuilder<'plan> {
    plan: &'plan ValidatedMigrationPlan,
    ports: Option<RedbMigrationProjectionPorts>,
}

impl<'plan> MigrationProjectionBuilder<'plan> {
    pub(crate) fn new(
        plan: &'plan ValidatedMigrationPlan,
        ports: RedbMigrationProjectionPorts,
    ) -> Self {
        Self {
            plan,
            ports: Some(ports),
        }
    }
}

impl MigrationProjectionBuildPort for MigrationProjectionBuilder<'_> {
    fn rebuild_projection(
        &mut self,
        projection: riffdb_types::ProjectionId,
        frontier: Option<CommitSequence>,
    ) -> Result<MigrationProjectionBuildObservation, MigrationProjectionError> {
        let resolved = self
            .plan
            .resolve_candidate_projection(projection)
            .map_err(|_| MigrationProjectionError::BuildFailed)?;
        let schema = self
            .plan
            .candidate()
            .bundle()
            .bound_projection_group_schema(projection)
            .map(CheckedProjectionSchema::new)
            .ok_or(MigrationProjectionError::BuildFailed)?;
        let ports = self
            .ports
            .take()
            .ok_or(MigrationProjectionError::BuildFailed)?;
        let registry = ProjectionSchemaRegistry::new(vec![schema.clone()])
            .map_err(|_| MigrationProjectionError::BuildFailed)?;
        let notifier = ProjectionNotifier::from_registry(&registry);
        let mut controller = ProjectionController::new(ports, notifier);
        let result = rebuild_migration_projection(&mut controller, &resolved, &schema, frontier);
        self.ports = Some(controller.into_parts().0);
        result
    }
}

/// Closed process-local worker state used by aggregate health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProjectionWorkerReadiness {
    Starting,
    Ready,
    Degraded,
    Stopped,
}

/// Cloneable observation handle with no worker or storage authority.
#[derive(Clone)]
pub(crate) struct ProjectionWorkerStatus {
    state: Arc<AtomicU8>,
}

impl ProjectionWorkerStatus {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(0)),
        }
    }

    pub(crate) fn publish(&self, readiness: ProjectionWorkerReadiness) {
        self.state.store(
            match readiness {
                ProjectionWorkerReadiness::Starting => 0,
                ProjectionWorkerReadiness::Ready => 1,
                ProjectionWorkerReadiness::Degraded => 2,
                ProjectionWorkerReadiness::Stopped => 3,
            },
            Ordering::Release,
        );
    }

    /// Returns the most recent bounded worker classification.
    pub(crate) fn readiness(&self) -> ProjectionWorkerReadiness {
        match self.state.load(Ordering::Acquire) {
            0 => ProjectionWorkerReadiness::Starting,
            1 => ProjectionWorkerReadiness::Ready,
            2 => ProjectionWorkerReadiness::Degraded,
            _ => ProjectionWorkerReadiness::Stopped,
        }
    }
}

impl fmt::Debug for ProjectionWorkerStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionWorkerStatus")
            .field("readiness", &self.readiness())
            .finish()
    }
}

struct StopState {
    requested: Mutex<bool>,
    changed: Condvar,
}

/// Owning guard for the one projection recovery/catch-up thread.
#[must_use = "the projection worker must be explicitly stopped and joined"]
pub(crate) struct RunningProjectionWorker {
    stop: Arc<StopState>,
    task: Option<JoinHandle<()>>,
    status: ProjectionWorkerStatus,
}

impl RunningProjectionWorker {
    /// Starts one bounded worker over the already validated storage authority.
    pub(crate) fn start(
        storage: SharedRedbOperationalPorts,
        notifier: ProjectionNotifier,
    ) -> Result<Self, ProjectionWorkerStartError> {
        let stop = Arc::new(StopState {
            requested: Mutex::new(false),
            changed: Condvar::new(),
        });
        let status = ProjectionWorkerStatus::new();
        let worker_stop = Arc::clone(&stop);
        let worker_status = status.clone();
        let task = thread::Builder::new()
            .name("riffdb-projection".to_owned())
            .spawn(move || {
                let mut state = ProjectionWorkerState::default();
                loop {
                    if stop_requested(&worker_stop) {
                        break;
                    }
                    match run_projection_pass(&storage, &notifier, &mut state) {
                        Ok(()) => worker_status.publish(ProjectionWorkerReadiness::Ready),
                        Err(error) if error.is_transient_writer_backpressure() => {
                            // A journal frame may be durable but not published for
                            // a bounded interval after the mutation gate becomes
                            // available. Derived work retries on its next pass;
                            // the authoritative application remains ready.
                        }
                        Err(_) => worker_status.publish(ProjectionWorkerReadiness::Degraded),
                    }
                    if wait_for_stop(&worker_stop, WORKER_POLL_INTERVAL) {
                        break;
                    }
                }
                worker_status.publish(ProjectionWorkerReadiness::Stopped);
            })
            .map_err(|_| ProjectionWorkerStartError)?;
        Ok(Self {
            stop,
            task: Some(task),
            status,
        })
    }

    /// Returns a least-authority aggregate-health observation handle.
    pub(crate) fn status(&self) -> ProjectionWorkerStatus {
        self.status.clone()
    }

    /// Requests termination and joins the worker before storage can be dropped.
    pub(crate) fn shutdown(mut self) -> Result<(), ProjectionWorkerShutdownError> {
        request_stop(&self.stop)?;
        let task = self
            .task
            .take()
            .expect("a running projection worker retains one task");
        task.join().map_err(|_| ProjectionWorkerShutdownError)?;
        if self.status.readiness() == ProjectionWorkerReadiness::Stopped {
            Ok(())
        } else {
            Err(ProjectionWorkerShutdownError)
        }
    }
}

impl fmt::Debug for RunningProjectionWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RunningProjectionWorker([DERIVED_AUTHORITY])")
    }
}

#[derive(Default)]
struct ProjectionWorkerState {
    validated: BTreeSet<ProjectionIdentity>,
    active: Option<ActiveCatalogSnapshot>,
}

fn run_projection_pass(
    storage: &SharedRedbOperationalPorts,
    notifier: &ProjectionNotifier,
    state: &mut ProjectionWorkerState,
) -> Result<(), ProjectionWorkerError> {
    let pointer = storage
        .read_active_catalog()
        .map_err(ProjectionWorkerError::Storage)?;
    let active = match (pointer.as_ref(), state.active.as_ref()) {
        (Some(pointer), Some(active)) if active.pointer() == pointer => Some(active.clone()),
        (None, None) => None,
        _ => {
            let active =
                ActiveCatalogSnapshot::read(storage).map_err(ProjectionWorkerError::Catalog)?;
            state.active = active.clone();
            active
        }
    };
    let Some(active) = active else {
        let registry =
            ProjectionSchemaRegistry::new(Vec::new()).map_err(ProjectionWorkerError::Projection)?;
        notifier
            .synchronize_registry(&registry)
            .map_err(ProjectionWorkerError::Projection)?;
        state.validated.clear();
        return Ok(());
    };
    let registry = registry_for(&active)?;
    notifier
        .synchronize_registry(&registry)
        .map_err(ProjectionWorkerError::Projection)?;
    state
        .validated
        .retain(|identity| registry.get(identity).is_some());

    for schema in registry.iter().cloned() {
        let resolved = active
            .resolve_projection(schema.identity())
            .map_err(ProjectionWorkerError::Catalog)?;
        drive_projection(storage.clone(), notifier.clone(), &resolved, schema, state)?;
    }
    Ok(())
}

fn registry_for(
    active: &ActiveCatalogSnapshot,
) -> Result<ProjectionSchemaRegistry, ProjectionWorkerError> {
    let bundle = active.bundle().bundle();
    let schemas = bundle
        .projections()
        .iter()
        .map(|projection| {
            bundle
                .bound_projection_group_schema(projection.projection_id())
                .map(CheckedProjectionSchema::new)
                .ok_or(ProjectionWorkerError::Integrity)
        })
        .collect::<Result<Vec<_>, _>>()?;
    ProjectionSchemaRegistry::new(schemas).map_err(ProjectionWorkerError::Projection)
}

fn drive_projection(
    storage: SharedRedbOperationalPorts,
    notifier: ProjectionNotifier,
    resolved: &ResolvedProjectionPlan,
    schema: CheckedProjectionSchema,
    state: &mut ProjectionWorkerState,
) -> Result<(), ProjectionWorkerError> {
    let identity = schema.identity().clone();
    let mut controller = ProjectionController::new(storage, notifier);
    for _ in 0..MAX_CONTROL_TRANSITIONS_PER_PASS {
        let status = controller
            .repository()
            .read_projection_status(&identity)
            .map_err(ProjectionWorkerError::Storage)?;
        if status.published().is_none() && status.candidate().is_none() {
            match controller
                .initialize(schema.clone())
                .map_err(ProjectionWorkerError::Projection)?
            {
                ProjectionInitializationResult::Initialized(_)
                | ProjectionInitializationResult::Existing(_)
                | ProjectionInitializationResult::StateChanged => continue,
            }
        }

        if !state.validated.contains(&identity) {
            match validate_retained_generations(&mut controller, resolved, &schema, &status)? {
                ValidationProgress::Clean => {
                    state.validated.insert(identity.clone());
                }
                ValidationProgress::StateChanged => continue,
            }
        }

        let status = controller
            .repository()
            .read_projection_status(&identity)
            .map_err(ProjectionWorkerError::Storage)?;
        match status.lifecycle() {
            ProjectionLifecycleV1::Building => {
                let _ = controller
                    .start_initial_catch_up(&identity)
                    .map_err(ProjectionWorkerError::Projection)?;
            }
            ProjectionLifecycleV1::CatchingUp => {
                let generation = status
                    .candidate()
                    .map(ProjectionGenerationPosition::generation)
                    .ok_or(ProjectionWorkerError::Integrity)?;
                if apply_generation(&mut controller, resolved, &schema, generation)?
                    == ApplyProgress::StateChanged
                {
                    continue;
                }
                let _ = controller
                    .publish_candidate(&identity)
                    .map_err(ProjectionWorkerError::Projection)?;
            }
            ProjectionLifecycleV1::Ready => {
                let generation = status
                    .published()
                    .map(ProjectionGenerationPosition::generation)
                    .ok_or(ProjectionWorkerError::Integrity)?;
                if apply_generation(&mut controller, resolved, &schema, generation)?
                    == ApplyProgress::CaughtUp
                {
                    return Ok(());
                }
            }
            ProjectionLifecycleV1::Rebuilding => {
                let published = status
                    .published()
                    .map(ProjectionGenerationPosition::generation)
                    .ok_or(ProjectionWorkerError::Integrity)?;
                let candidate = status
                    .candidate()
                    .map(ProjectionGenerationPosition::generation)
                    .ok_or(ProjectionWorkerError::Integrity)?;
                if apply_generation(&mut controller, resolved, &schema, published)?
                    == ApplyProgress::StateChanged
                    || apply_generation(&mut controller, resolved, &schema, candidate)?
                        == ApplyProgress::StateChanged
                {
                    continue;
                }
                let _ = controller
                    .publish_candidate(&identity)
                    .map_err(ProjectionWorkerError::Projection)?;
            }
            ProjectionLifecycleV1::Degraded => {
                state.validated.remove(&identity);
                let _ = controller
                    .recover_degraded(&identity)
                    .map_err(ProjectionWorkerError::Projection)?;
            }
            ProjectionLifecycleV1::Invalid => return Ok(()),
        }
    }
    Err(ProjectionWorkerError::TransitionLimit)
}

enum ValidationProgress {
    Clean,
    StateChanged,
}

fn validate_retained_generations(
    controller: &mut ProjectionController<SharedRedbOperationalPorts>,
    resolved: &ResolvedProjectionPlan,
    schema: &CheckedProjectionSchema,
    status: &riffdb_storage_api::ProjectionStatus,
) -> Result<ValidationProgress, ProjectionWorkerError> {
    if status.lifecycle() == ProjectionLifecycleV1::Degraded {
        let _ = controller
            .recover_degraded(status.identity())
            .map_err(ProjectionWorkerError::Projection)?;
        return Ok(ValidationProgress::StateChanged);
    }
    if status.lifecycle() == ProjectionLifecycleV1::Invalid {
        return Ok(ValidationProgress::Clean);
    }
    let control = control_from_status(status)?;
    let limit = ProjectionRecoveryPageLimit::new(
        NonZeroU16::new(RECOVERY_PAGE_ROWS).expect("the fixed recovery page is nonzero"),
    )
    .map_err(|_| ProjectionWorkerError::Integrity)?;
    for generation in [control.published(), control.candidate()]
        .into_iter()
        .flatten()
        .map(ProjectionGenerationPosition::generation)
    {
        match validate_and_recover_projection_generation(
            controller,
            resolved,
            schema.clone(),
            &control,
            status.authoritative_head(),
            generation,
            limit,
        )
        .map_err(ProjectionWorkerError::Recovery)?
        {
            ProjectionRecoveryOutcome::Clean(_) => {}
            ProjectionRecoveryOutcome::FenceChanged
            | ProjectionRecoveryOutcome::RebuildAllocated { .. } => {
                return Ok(ValidationProgress::StateChanged);
            }
        }
    }
    Ok(ValidationProgress::Clean)
}

fn control_from_status(
    status: &riffdb_storage_api::ProjectionStatus,
) -> Result<StoredProjectionControlV1, ProjectionWorkerError> {
    let highest = [status.published(), status.candidate()]
        .into_iter()
        .flatten()
        .map(ProjectionGenerationPosition::generation)
        .max()
        .ok_or(ProjectionWorkerError::Integrity)?;
    StoredProjectionControlV1::new(
        status.identity().clone(),
        highest,
        status.published(),
        status.candidate(),
        status.published_apply_mode(),
        status.lifecycle(),
        status.failure().cloned(),
    )
    .map_err(|_| ProjectionWorkerError::Integrity)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ApplyProgress {
    CaughtUp,
    StateChanged,
}

fn rebuild_migration_projection(
    controller: &mut ProjectionController<RedbMigrationProjectionPorts>,
    resolved: &ResolvedProjectionPlan,
    schema: &CheckedProjectionSchema,
    frontier: Option<CommitSequence>,
) -> Result<MigrationProjectionBuildObservation, MigrationProjectionError> {
    let expected = frontier.map_or(
        FrontierPosition::BeforeFirst,
        FrontierPosition::AppliedThrough,
    );
    let identity = schema.identity();
    let mut target_generation = None;
    for _ in 0..MAX_CONTROL_TRANSITIONS_PER_PASS {
        let status = controller
            .repository()
            .read_projection_status(identity)
            .map_err(|_| MigrationProjectionError::BuildFailed)?;
        if status.authoritative_head() != expected {
            return Err(MigrationProjectionError::EvidenceMismatch);
        }
        if status.published().is_none() && status.candidate().is_none() {
            match controller
                .initialize(schema.clone())
                .map_err(|_| MigrationProjectionError::BuildFailed)?
            {
                ProjectionInitializationResult::Initialized(control)
                | ProjectionInitializationResult::Existing(control) => {
                    target_generation = control
                        .candidate()
                        .map(ProjectionGenerationPosition::generation);
                }
                ProjectionInitializationResult::StateChanged => {}
            }
            continue;
        }
        match status.lifecycle() {
            ProjectionLifecycleV1::Ready if target_generation.is_none() => {
                match controller
                    .allocate_rebuild(identity)
                    .map_err(|_| MigrationProjectionError::BuildFailed)?
                {
                    ProjectionControlResult::Updated(control)
                    | ProjectionControlResult::AlreadyInitialized(control) => {
                        target_generation = control
                            .candidate()
                            .map(ProjectionGenerationPosition::generation);
                    }
                    ProjectionControlResult::StateChanged => {}
                    ProjectionControlResult::GenerationExhausted => {
                        return Err(MigrationProjectionError::BuildFailed);
                    }
                }
            }
            ProjectionLifecycleV1::Ready => {
                let published = status
                    .published()
                    .ok_or(MigrationProjectionError::EvidenceMismatch)?;
                if Some(published.generation()) == target_generation
                    && published.frontier() == expected
                {
                    return Ok(MigrationProjectionBuildObservation::ready(
                        projection_id(identity),
                        published.generation(),
                        frontier,
                    ));
                }
                return Err(MigrationProjectionError::EvidenceMismatch);
            }
            ProjectionLifecycleV1::Building => {
                target_generation = status
                    .candidate()
                    .map(ProjectionGenerationPosition::generation);
                let _ = controller
                    .start_initial_catch_up(identity)
                    .map_err(|_| MigrationProjectionError::BuildFailed)?;
            }
            ProjectionLifecycleV1::CatchingUp | ProjectionLifecycleV1::Rebuilding => {
                let generation = status
                    .candidate()
                    .map(ProjectionGenerationPosition::generation)
                    .ok_or(MigrationProjectionError::EvidenceMismatch)?;
                target_generation = Some(generation);
                if apply_migration_generation(controller, resolved, schema, generation, expected)?
                    == ApplyProgress::CaughtUp
                {
                    let _ = controller
                        .publish_candidate(identity)
                        .map_err(|_| MigrationProjectionError::BuildFailed)?;
                }
            }
            ProjectionLifecycleV1::Degraded | ProjectionLifecycleV1::Invalid => {
                return Err(MigrationProjectionError::BuildFailed);
            }
        }
    }
    Err(MigrationProjectionError::BuildFailed)
}

fn projection_id(identity: &ProjectionIdentity) -> riffdb_types::ProjectionId {
    identity.projection_id()
}

fn apply_migration_generation(
    controller: &mut ProjectionController<RedbMigrationProjectionPorts>,
    resolved: &ResolvedProjectionPlan,
    schema: &CheckedProjectionSchema,
    generation: ProjectionGeneration,
    expected_head: FrontierPosition,
) -> Result<ApplyProgress, MigrationProjectionError> {
    let status = controller
        .repository()
        .read_projection_status(schema.identity())
        .map_err(|_| MigrationProjectionError::BuildFailed)?;
    if status.authoritative_head() != expected_head {
        return Err(MigrationProjectionError::EvidenceMismatch);
    }
    let mut frontier = status
        .candidate()
        .filter(|position| position.generation() == generation)
        .map(ProjectionGenerationPosition::frontier)
        .ok_or(MigrationProjectionError::EvidenceMismatch)?;
    let limit =
        StorageScanLimit::new(COMMIT_SCAN_ROWS).ok_or(MigrationProjectionError::BuildFailed)?;
    let mut scan = match frontier {
        FrontierPosition::BeforeFirst => CommitScanRequest::initial(limit),
        FrontierPosition::AppliedThrough(sequence) => {
            CommitScanRequest::initial_after(sequence, limit)
        }
    };
    loop {
        let page = controller
            .repository()
            .scan_commits(scan)
            .map_err(|_| MigrationProjectionError::BuildFailed)?;
        if page.inclusive_upper() != expected_head || page.inclusive_upper() < frontier {
            return Err(MigrationProjectionError::EvidenceMismatch);
        }
        for charged in page.records() {
            let commit = charged.value();
            if FrontierPosition::AppliedThrough(commit.commit_sequence()) <= frontier {
                continue;
            }
            if !is_exact_successor(frontier, commit.commit_sequence()) {
                return Err(MigrationProjectionError::EvidenceMismatch);
            }
            let request = evaluate_and_prepare_projection_commit(
                resolved,
                schema.clone(),
                generation,
                commit,
                controller.repository(),
            )
            .map_err(|_| MigrationProjectionError::BuildFailed)?;
            match controller
                .apply(&request)
                .map_err(|_| MigrationProjectionError::BuildFailed)?
            {
                ProjectionApplyResult::Applied { .. }
                | ProjectionApplyResult::AlreadyApplied(_) => {
                    frontier = FrontierPosition::AppliedThrough(commit.commit_sequence());
                }
                ProjectionApplyResult::StateChanged => return Ok(ApplyProgress::StateChanged),
            }
        }
        match page {
            CommitScanPageV1::Page {
                next_after,
                inclusive_upper,
                ..
            } => {
                let FrontierPosition::AppliedThrough(upper) = inclusive_upper else {
                    return Err(MigrationProjectionError::EvidenceMismatch);
                };
                scan = CommitScanRequest::continuing(next_after, upper, limit)
                    .map_err(|_| MigrationProjectionError::BuildFailed)?;
            }
            CommitScanPageV1::ExactEnd { .. } => {
                return if frontier == expected_head {
                    Ok(ApplyProgress::CaughtUp)
                } else {
                    Err(MigrationProjectionError::EvidenceMismatch)
                };
            }
        }
    }
}

fn apply_generation(
    controller: &mut ProjectionController<SharedRedbOperationalPorts>,
    resolved: &ResolvedProjectionPlan,
    schema: &CheckedProjectionSchema,
    generation: ProjectionGeneration,
) -> Result<ApplyProgress, ProjectionWorkerError> {
    let status = controller
        .repository()
        .read_projection_status(schema.identity())
        .map_err(ProjectionWorkerError::Storage)?;
    let mut frontier = status
        .published()
        .filter(|position| position.generation() == generation)
        .or_else(|| {
            status
                .candidate()
                .filter(|position| position.generation() == generation)
        })
        .map(ProjectionGenerationPosition::frontier)
        .ok_or(ProjectionWorkerError::Integrity)?;
    let limit = StorageScanLimit::new(COMMIT_SCAN_ROWS).ok_or(ProjectionWorkerError::Integrity)?;
    let mut scan = match frontier {
        FrontierPosition::BeforeFirst => CommitScanRequest::initial(limit),
        FrontierPosition::AppliedThrough(sequence) => {
            CommitScanRequest::initial_after(sequence, limit)
        }
    };
    loop {
        let page = controller
            .repository()
            .scan_commits(scan)
            .map_err(ProjectionWorkerError::Storage)?;
        let inclusive_upper = page.inclusive_upper();
        if inclusive_upper < frontier {
            return Err(ProjectionWorkerError::Integrity);
        }
        for charged in page.records() {
            let commit = charged.value();
            if FrontierPosition::AppliedThrough(commit.commit_sequence()) <= frontier {
                continue;
            }
            if !is_exact_successor(frontier, commit.commit_sequence()) {
                return record_generation_failure(
                    controller,
                    schema.identity(),
                    generation,
                    ProjectionFailureCodeV1::MissingCommit,
                    commit.commit_sequence(),
                );
            }
            let request = match evaluate_and_prepare_projection_commit(
                resolved,
                schema.clone(),
                generation,
                commit,
                controller.repository(),
            ) {
                Ok(request) => request,
                Err(error) => {
                    return handle_evaluation_failure(
                        controller,
                        schema.identity(),
                        generation,
                        commit.commit_sequence(),
                        error,
                    );
                }
            };
            match controller
                .apply(&request)
                .map_err(ProjectionWorkerError::Projection)?
            {
                ProjectionApplyResult::Applied { .. }
                | ProjectionApplyResult::AlreadyApplied(_) => {
                    frontier = FrontierPosition::AppliedThrough(commit.commit_sequence());
                }
                ProjectionApplyResult::StateChanged => return Ok(ApplyProgress::StateChanged),
            }
        }
        match page {
            CommitScanPageV1::Page {
                next_after,
                inclusive_upper,
                ..
            } => {
                let FrontierPosition::AppliedThrough(upper) = inclusive_upper else {
                    return Err(ProjectionWorkerError::Integrity);
                };
                scan = CommitScanRequest::continuing(next_after, upper, limit)
                    .map_err(|_| ProjectionWorkerError::Integrity)?;
            }
            CommitScanPageV1::ExactEnd {
                inclusive_upper, ..
            } => {
                return if frontier == inclusive_upper {
                    Ok(ApplyProgress::CaughtUp)
                } else {
                    Err(ProjectionWorkerError::Integrity)
                };
            }
        }
    }
}

fn handle_evaluation_failure(
    controller: &mut ProjectionController<SharedRedbOperationalPorts>,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
    sequence: CommitSequence,
    error: ProjectionEvaluationError,
) -> Result<ApplyProgress, ProjectionWorkerError> {
    match error.kind().failure_code() {
        Some(code) => record_generation_failure(controller, identity, generation, code, sequence),
        None => Err(ProjectionWorkerError::Evaluation(error)),
    }
}

fn record_generation_failure(
    controller: &mut ProjectionController<SharedRedbOperationalPorts>,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
    code: ProjectionFailureCodeV1,
    sequence: CommitSequence,
) -> Result<ApplyProgress, ProjectionWorkerError> {
    match controller
        .record_failure(identity, generation, code, Some(sequence))
        .map_err(ProjectionWorkerError::Projection)?
    {
        ProjectionControlResult::Updated(_) | ProjectionControlResult::StateChanged => {
            Ok(ApplyProgress::StateChanged)
        }
        ProjectionControlResult::AlreadyInitialized(_)
        | ProjectionControlResult::GenerationExhausted => Err(ProjectionWorkerError::Integrity),
    }
}

const fn is_exact_successor(frontier: FrontierPosition, sequence: CommitSequence) -> bool {
    match frontier {
        FrontierPosition::BeforeFirst => sequence.get() == 1,
        FrontierPosition::AppliedThrough(previous) => match previous.checked_next() {
            Some(expected) => expected.get() == sequence.get(),
            None => false,
        },
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

fn request_stop(stop: &StopState) -> Result<(), ProjectionWorkerShutdownError> {
    let mut requested = stop
        .requested
        .lock()
        .map_err(|_| ProjectionWorkerShutdownError)?;
    *requested = true;
    stop.changed.notify_all();
    Ok(())
}

/// Closed worker construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProjectionWorkerStartError;

impl fmt::Display for ProjectionWorkerStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the projection worker could not start")
    }
}

impl std::error::Error for ProjectionWorkerStartError {}

/// Closed worker shutdown failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProjectionWorkerShutdownError;

impl fmt::Display for ProjectionWorkerShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the projection worker did not stop cleanly")
    }
}

impl std::error::Error for ProjectionWorkerShutdownError {}

enum ProjectionWorkerError {
    Catalog(CatalogError),
    Evaluation(ProjectionEvaluationError),
    Projection(ProjectionCoreError),
    Recovery(ProjectionRecoveryError),
    Storage(StorageError),
    Integrity,
    TransitionLimit,
}

impl ProjectionWorkerError {
    fn is_transient_writer_backpressure(&self) -> bool {
        matches!(
            self,
            Self::Projection(error)
                if error.kind() == ProjectionCoreErrorKind::StorageUnavailable
        ) || matches!(
            self,
            Self::Storage(error) if error.kind() == riffdb_storage_api::StorageErrorKind::Unavailable
        )
    }
}

impl fmt::Debug for ProjectionWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let class = match self {
            Self::Catalog(error) => {
                let _ = error.kind();
                "catalog"
            }
            Self::Evaluation(error) => {
                let _ = error.kind();
                "evaluation"
            }
            Self::Projection(error) => {
                let _ = error.kind();
                "projection"
            }
            Self::Recovery(error) => {
                let _ = error.kind();
                "recovery"
            }
            Self::Storage(error) => {
                let _ = error.kind();
                "storage"
            }
            Self::Integrity => "integrity",
            Self::TransitionLimit => "transition_limit",
        };
        formatter
            .debug_struct("ProjectionWorkerError")
            .field("class", &class)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successor_check_has_no_zero_sequence_case() {
        assert!(is_exact_successor(
            FrontierPosition::BeforeFirst,
            CommitSequence::first()
        ));
        assert!(!is_exact_successor(
            FrontierPosition::AppliedThrough(CommitSequence::first()),
            CommitSequence::first()
        ));
    }

    #[test]
    fn worker_errors_never_render_sources() {
        assert_eq!(
            format!("{:?}", ProjectionWorkerError::TransitionLimit),
            "ProjectionWorkerError { class: \"transition_limit\" }"
        );
    }

    #[test]
    fn transient_writer_backpressure_does_not_degrade_projection_readiness() {
        assert!(
            ProjectionWorkerError::Projection(ProjectionCoreError::new(
                ProjectionCoreErrorKind::StorageUnavailable,
            ))
            .is_transient_writer_backpressure()
        );
        assert!(
            ProjectionWorkerError::Storage(StorageError::new(
                riffdb_storage_api::StorageErrorKind::Unavailable,
                None,
            ))
            .is_transient_writer_backpressure()
        );
        assert!(
            !ProjectionWorkerError::Projection(ProjectionCoreError::new(
                ProjectionCoreErrorKind::CommitStatusUnknown,
            ))
            .is_transient_writer_backpressure()
        );
    }
}
