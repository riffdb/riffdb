//! Follower-only V2 views derived from completed immutable authority.
//! This owner has no primary storage, control writer or generation allocator.
use super::{
    ColumnarActivationWake, ColumnarControlBinding, ColumnarRegistrationError,
    admission::{resolve_columnar_bindings, validate_follower_columnar_controls},
};
use crate::{
    clocks::ServerColumnarReplayClock,
    columnar_worker::{ColumnarWorkerShutdownError, ColumnarWorkerStartError},
    config::ConfiguredProjection,
    replication_bootstrap::{FollowerReadSnapshots, FollowerReadView},
};
use riffdb_columnar::{
    ColumnarSnapshot, ColumnarV2StreamingError, RegisteredDefinition, V2_SNAPSHOT_BUILD_MAX_BYTES,
    V2_SNAPSHOT_BUILD_MAX_FILES, ValidatedColumnarV2Generation,
};
use riffdb_service::{
    ColumnarLifecycle, ColumnarNotifier, ColumnarObservation, ColumnarPortError,
    ColumnarProjectionPort,
};
use riffdb_storage_redb::RedbFollowerColumnarScratch;
use riffdb_types::{DatabaseId, FrontierPosition, ProjectionFrontier, ProjectionGeneration};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

struct LocalView {
    process: [u8; 16],
    ordinal: NonZeroU64,
    snapshot: Arc<ColumnarSnapshot>,
}
enum State {
    Cold,
    Activating,
    Active(LocalView),
    Failed,
    Stopped,
}
struct Inventory {
    states: Vec<State>,
    activations: u64,
    population_passes: u64,
}
pub(crate) struct FollowerColumnarRuntime {
    reads: FollowerReadSnapshots,
    projections: Vec<ConfiguredProjection>,
    bindings: Vec<ColumnarControlBinding>,
    database: DatabaseId,
    incarnation: u64,
    process: [u8; 16],
    root: PathBuf,
    clock: ServerColumnarReplayClock,
    inventory: Mutex<Inventory>,
    cold_snapshot: Arc<ColumnarSnapshot>,
    notifier: ColumnarNotifier,
    wake: ColumnarActivationWake,
    stopped: AtomicBool,
    worker_claimed: AtomicBool,
}
impl FollowerColumnarRuntime {
    pub(crate) fn open(
        reads: FollowerReadSnapshots,
        projections: &[ConfiguredProjection],
        root: &Path,
        process: [u8; 16],
        clock: ServerColumnarReplayClock,
    ) -> Result<Arc<Self>, ColumnarRegistrationError> {
        let view = reads
            .latest()
            .map_err(|_| ColumnarRegistrationError::synchronization())?;
        let bindings =
            resolve_columnar_bindings(projections, view.catalog().map(|c| c.bundle().bundle()))?;
        let controls = view
            .snapshot()
            .read_columnar_projection_controls()
            .map_err(|error| {
                ColumnarRegistrationError::control_storage(error, "columnar-control")
            })?;
        let lineage = view.history().lineage();
        validate_follower_columnar_controls(
            &bindings,
            &controls,
            lineage.history_incarnation(),
            frontier(&view),
        )?;
        if process == [0; 16] {
            return Err(ColumnarRegistrationError::synchronization());
        }
        let notifier = ColumnarNotifier::from_names(bindings.iter().map(|b| b.name.clone()));
        let states = bindings.iter().map(|_| State::Cold).collect();
        Ok(Arc::new(Self {
            reads,
            projections: projections.to_vec(),
            bindings,
            database: lineage.database_id(),
            incarnation: lineage.history_incarnation(),
            process,
            root: root.to_path_buf(),
            clock,
            inventory: Mutex::new(Inventory {
                states,
                activations: 0,
                population_passes: 0,
            }),
            cold_snapshot: Arc::new(ColumnarSnapshot::empty()),
            notifier,
            wake: ColumnarActivationWake::default(),
            stopped: AtomicBool::new(false),
            worker_claimed: AtomicBool::new(false),
        }))
    }

    fn current(&self) -> Result<Arc<FollowerReadView>, ColumnarPortError> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(ColumnarPortError::Unavailable);
        }
        let view = self
            .reads
            .latest()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        let lineage = view.history().lineage();
        if lineage.database_id() != self.database
            || lineage.history_incarnation() != self.incarnation
        {
            return Err(ColumnarPortError::Integrity);
        }
        // Re-resolve the complete bounded set. A changed source specification
        // cannot be served through a stale process-local registration.
        let bindings = resolve_columnar_bindings(
            &self.projections,
            view.catalog().map(|c| c.bundle().bundle()),
        )
        .map_err(|_| ColumnarPortError::Integrity)?;
        if bindings.len() != self.bindings.len()
            || bindings
                .iter()
                .zip(&self.bindings)
                .any(|(a, b)| a.name != b.name || a.spec != b.spec || a.is_vector != b.is_vector)
        {
            return Err(ColumnarPortError::Integrity);
        }
        let controls = view
            .snapshot()
            .read_columnar_projection_controls()
            .map_err(|_| ColumnarPortError::Integrity)?;
        validate_follower_columnar_controls(
            &self.bindings,
            &controls,
            self.incarnation,
            frontier(&view),
        )
        .map_err(|_| ColumnarPortError::Integrity)?;
        Ok(view)
    }

    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        if let Ok(mut inventory) = self.inventory.lock() {
            for state in &mut inventory.states {
                *state = State::Stopped;
            }
        }
        for binding in &self.bindings {
            let _ = self.notifier.notify(&binding.name);
        }
        self.wake.signal();
    }

    pub(crate) fn is_healthy(&self) -> bool {
        !self.stopped.load(Ordering::Acquire)
            && self.inventory.lock().is_ok_and(|inventory| {
                inventory
                    .states
                    .iter()
                    .all(|state| matches!(state, State::Active(_)))
            })
            && self.current().is_ok()
    }

    #[cfg(test)]
    pub(crate) fn lifecycle_observation(&self) -> Result<(usize, u64, u64), ColumnarPortError> {
        let inventory = self
            .inventory
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        Ok((
            inventory
                .states
                .iter()
                .filter(|s| matches!(s, State::Cold))
                .count(),
            inventory.activations,
            inventory.population_passes,
        ))
    }

    #[cfg(test)]
    pub(crate) fn worker_for_test(
        self: &Arc<Self>,
    ) -> Result<FollowerColumnarWorker, ColumnarPortError> {
        FollowerColumnarWorker::claim(self.clone())
    }
}
impl ColumnarProjectionPort for FollowerColumnarRuntime {
    fn observe(&self, name: &str) -> Result<ColumnarObservation, ColumnarPortError> {
        let view = self.current()?;
        let index = self
            .bindings
            .iter()
            .position(|b| b.name == name)
            .ok_or(ColumnarPortError::Integrity)?;
        let mut inventory = self
            .inventory
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        if matches!(inventory.states[index], State::Cold) {
            inventory.activations = inventory
                .activations
                .checked_add(1)
                .ok_or(ColumnarPortError::Integrity)?;
            inventory.states[index] = State::Activating;
            self.wake.signal();
        }
        let (snapshot, published, lifecycle) = match &inventory.states[index] {
            State::Activating => (
                self.cold_snapshot.clone(),
                false,
                Some(ColumnarLifecycle::Building),
            ),
            State::Active(local) => {
                if local.process != self.process
                    || local.ordinal.get() == 0
                    || local.snapshot.visible_frontier > frontier(&view)
                {
                    return Err(ColumnarPortError::Integrity);
                }
                (local.snapshot.clone(), true, Some(ColumnarLifecycle::Ready))
            }
            State::Cold | State::Failed | State::Stopped => {
                return Err(ColumnarPortError::Unavailable);
            }
        };
        Ok(ColumnarObservation::new(
            self.bindings[index].definition.clone(),
            snapshot.clone(),
            ProjectionFrontier::new(self.incarnation, snapshot.visible_frontier),
            ProjectionFrontier::new(self.incarnation, frontier(&view)),
            published,
            lifecycle,
        ))
    }
    fn definition(&self, name: &str) -> Option<RegisteredDefinition> {
        self.current().ok()?;
        self.bindings
            .iter()
            .find(|b| b.name == name)
            .map(|b| b.definition.clone())
    }
    fn notifier(&self) -> &ColumnarNotifier {
        &self.notifier
    }
    fn known_names(&self) -> Vec<String> {
        if self.current().is_err() {
            return Vec::new();
        }
        self.bindings.iter().map(|b| b.name.clone()).collect()
    }
}

impl riffdb_service::VectorProjectionPort for FollowerColumnarRuntime {
    fn execute(
        &self,
        request: riffdb_service::VectorProjectionRequest,
    ) -> Result<riffdb_service::VectorProjectionResult, riffdb_service::VectorProjectionPortError>
    {
        if !self
            .bindings
            .iter()
            .any(|binding| binding.name == request.source_name() && binding.is_vector)
        {
            return Err(riffdb_service::VectorProjectionPortError::Integrity);
        }
        let observation = self
            .observe(request.source_name())
            .map_err(super::map_vector_port_error)?;
        let source = crate::projection_read_source::ProjectionReadSource::new(self.reads.clone());
        super::vector_view::execute(
            request,
            observation,
            &source,
            &source.query_executor(),
            None,
        )
    }
}

fn frontier(view: &FollowerReadView) -> FrontierPosition {
    view.history().tail().frontier().application().map_or(
        FrontierPosition::BeforeFirst,
        FrontierPosition::AppliedThrough,
    )
}

pub(crate) struct FollowerColumnarWorker {
    runtime: Arc<FollowerColumnarRuntime>,
    scratch: Option<RedbFollowerColumnarScratch>,
    next_source: usize,
    next_view: u64,
    cleanup_failed: bool,
    #[cfg(test)]
    before_build: Option<(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
    #[cfg(test)]
    before_install: Option<(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
}
impl FollowerColumnarWorker {
    fn claim(runtime: Arc<FollowerColumnarRuntime>) -> Result<Self, ColumnarPortError> {
        runtime
            .worker_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| ColumnarPortError::Unavailable)?;
        Ok(Self {
            runtime,
            scratch: None,
            next_source: 0,
            next_view: 1,
            cleanup_failed: false,
            #[cfg(test)]
            before_build: None,
            #[cfg(test)]
            before_install: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn pause_before_build(
        &mut self,
        reached: std::sync::mpsc::SyncSender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    ) {
        self.before_build = Some((reached, resume));
    }

    #[cfg(test)]
    pub(crate) fn pause_before_install(
        &mut self,
        reached: std::sync::mpsc::SyncSender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    ) {
        self.before_install = Some((reached, resume));
    }

    /// One fair source pass. Cold slots never open or populate any material.
    pub(crate) fn advance(&mut self) -> Result<bool, ColumnarPortError> {
        let view = self.runtime.current()?;
        let selected = {
            let inventory = self
                .runtime
                .inventory
                .lock()
                .map_err(|_| ColumnarPortError::Unavailable)?;
            (0..inventory.states.len())
                .map(|offset| (self.next_source + offset) % inventory.states.len())
                .find(|index| match &inventory.states[*index] {
                    State::Activating => true,
                    State::Active(local) => local.snapshot.visible_frontier < frontier(&view),
                    State::Cold | State::Failed | State::Stopped => false,
                })
        };
        let Some(index) = selected else {
            return Ok(false);
        };
        self.next_source = (index + 1) % self.runtime.bindings.len();
        let result = self.materialize(index, &view);
        #[cfg(test)]
        if result.is_err() {
            self.before_install.take();
        }
        let mut inventory = self
            .runtime
            .inventory
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        if self.runtime.stopped.load(Ordering::Acquire) {
            return Err(ColumnarPortError::Unavailable);
        }
        // A failed local build is rowless. It never repairs or acknowledges the
        // source control, and requests never retry a failed slot.
        inventory.states[index] = match result {
            Ok(local) => State::Active(local),
            Err(_) => State::Failed,
        };
        drop(inventory);
        self.runtime
            .notifier
            .notify(&self.runtime.bindings[index].name)
            .map_err(|_| ColumnarPortError::Unavailable)?;
        Ok(true)
    }

    fn materialize(
        &mut self,
        index: usize,
        view: &FollowerReadView,
    ) -> Result<LocalView, ColumnarPortError> {
        if self.scratch.is_none() {
            self.scratch = Some(
                RedbFollowerColumnarScratch::open(
                    &self.runtime.root,
                    NonZeroUsize::new(V2_SNAPSHOT_BUILD_MAX_FILES)
                        .ok_or(ColumnarPortError::Integrity)?,
                    NonZeroU64::new(V2_SNAPSHOT_BUILD_MAX_BYTES)
                        .ok_or(ColumnarPortError::Integrity)?,
                )
                .map_err(|_| ColumnarPortError::Unavailable)?,
            );
        }
        let mut inventory = self
            .runtime
            .inventory
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        inventory.population_passes = inventory
            .population_passes
            .checked_add(1)
            .ok_or(ColumnarPortError::Integrity)?;
        drop(inventory);
        let lease = self
            .scratch
            .as_mut()
            .ok_or(ColumnarPortError::Integrity)?
            .begin()
            .map_err(|_| {
                self.cleanup_failed = true;
                ColumnarPortError::Unavailable
            })?;
        let binding = &self.runtime.bindings[index];
        #[cfg(test)]
        if let Some((reached, resume)) = self.before_build.take() {
            reached
                .send(())
                .map_err(|_| ColumnarPortError::Unavailable)?;
            resume.recv().map_err(|_| ColumnarPortError::Unavailable)?;
        }
        // Identical immutable input and exact snapshot frontier imply no tail.
        // Temporary format generation one is never a source-control identity.
        let built = ValidatedColumnarV2Generation::prepare_streaming(
            lease.path(),
            binding.definition.clone(),
            self.runtime.incarnation,
            ProjectionGeneration::first(),
            frontier(view),
            view.snapshot(),
            view.snapshot(),
            binding.spec.replay_limits(),
            || {
                self.runtime
                    .clock
                    .now()
                    .map_err(|_| ColumnarV2StreamingError::Clock)
            },
            || self.runtime.stopped.load(Ordering::Acquire) || self.runtime.reads.latest().is_err(),
        )
        .map(|generation| generation.snapshot().clone());
        // All builder handles have gone before bounded removal. Cleanup failure
        // discards the candidate just like validation/cancellation failure.
        lease.discard().map_err(|_| {
            self.cleanup_failed = true;
            ColumnarPortError::Unavailable
        })?;
        self.cleanup_failed = false;
        let snapshot = built.map_err(|_| ColumnarPortError::Unavailable)?;
        #[cfg(test)]
        if let Some((reached, resume)) = self.before_install.take() {
            reached
                .send(())
                .map_err(|_| ColumnarPortError::Unavailable)?;
            resume.recv().map_err(|_| ColumnarPortError::Unavailable)?;
        }
        let latest = self.runtime.current()?;
        if snapshot.visible_frontier != frontier(view)
            || snapshot.visible_frontier > frontier(&latest)
        {
            return Err(ColumnarPortError::Integrity);
        }
        let ordinal = NonZeroU64::new(self.next_view).ok_or(ColumnarPortError::Integrity)?;
        self.next_view = self
            .next_view
            .checked_add(1)
            .ok_or(ColumnarPortError::Integrity)?;
        Ok(LocalView {
            process: self.runtime.process,
            ordinal,
            snapshot,
        })
    }
}
impl Drop for FollowerColumnarWorker {
    fn drop(&mut self) {
        self.runtime.stop();
    }
}

impl FollowerColumnarWorker {
    fn drained_result(&self) -> Result<(), ColumnarWorkerShutdownError> {
        if self.cleanup_failed {
            Err(ColumnarWorkerShutdownError)
        } else {
            Ok(())
        }
    }
}

pub(crate) struct RunningFollowerColumnarWorker {
    runtime: Arc<FollowerColumnarRuntime>,
    thread: JoinHandle<Result<(), ColumnarWorkerShutdownError>>,
}
impl RunningFollowerColumnarWorker {
    pub(crate) fn stop_admission(&self) {
        self.runtime.stop();
    }

    pub(crate) fn start(
        runtime: Arc<FollowerColumnarRuntime>,
    ) -> Result<Self, ColumnarWorkerStartError> {
        let mut worker =
            FollowerColumnarWorker::claim(runtime.clone()).map_err(|_| ColumnarWorkerStartError)?;
        let thread = std::thread::Builder::new()
            .name("follower-columnar".to_owned())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                loop {
                    let epoch = worker.runtime.wake.current();
                    if worker.runtime.stopped.load(Ordering::Acquire) {
                        return worker.drained_result();
                    }
                    match worker.advance() {
                        Ok(true) => continue,
                        Ok(false) => {
                            worker.runtime.wake.wait(epoch, Duration::from_millis(25));
                        }
                        Err(_)
                            if worker.runtime.stopped.load(Ordering::Acquire)
                                || worker.runtime.reads.latest().is_err() =>
                        {
                            return worker.drained_result();
                        }
                        Err(_) => return Err(ColumnarWorkerShutdownError),
                    }
                }
            })
            .map_err(|_| ColumnarWorkerStartError)?;
        Ok(Self { runtime, thread })
    }
    pub(crate) fn shutdown(self) -> Result<(), ColumnarWorkerShutdownError> {
        self.runtime.stop();
        self.thread
            .join()
            .map_err(|_| ColumnarWorkerShutdownError)?
    }
}
