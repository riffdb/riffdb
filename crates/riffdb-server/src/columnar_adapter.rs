#![expect(
    clippy::expect_used,
    reason = "validated columnar batches retain the projected entity and generation selected for apply"
)]

//! Server-side columnar apply source and published projection port.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, Weak};
use std::time::Duration;

use riffdb_catalog::ActiveCatalogSnapshot;
use riffdb_columnar::{
    ColumnarEngine, ColumnarError, ColumnarOutcome, ColumnarProjectionDefinition,
    ColumnarProjectionSpecV1, NearestCandidate, NearestCandidateAdmission,
    NearestQueryAdmissionError, NearestQueryRequest, OpenOptions, PhysicalGenerationFingerprintV1,
    PreparedColumnarGenerationV1, QueryBudget, QueryError, RegisteredDefinition,
    ValidatedColumnarV2Generation,
};
use riffdb_contract_ir::{ContractBundle, ExpressionKind, ValueType, ValueTypeTag};
use riffdb_service::{
    ColumnarLifecycle, ColumnarNotifier, ColumnarObservation, ColumnarPortError,
    ColumnarProjectionPort, VectorProjectionPort, VectorProjectionPortError,
    VectorProjectionRequest, VectorProjectionResult,
};
use riffdb_storage_api::{
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, ColumnarProjectionControlRepository, ColumnarProjectionLayoutV1,
    CommitScanPageV1, CommitScanRequest, EntityTarget, FreshColumnarProjectionControlV1,
    IdempotencyIdentity, StorageError, StorageErrorKind, StorageScanLimit,
    StoredColumnarProjectionControlV1, StoredColumnarProjectionGenerationV1, StoredCommitRecordV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1,
};
use riffdb_types::{
    CommitSequence, EntityKey, EventId, FieldId, FrontierPosition, ProjectionFrontier,
    ProjectionGeneration, ProvenanceId,
};

use crate::clocks::ServerColumnarReplayClock;
use crate::config::ConfiguredProjection;
use crate::storage::SharedRedbOperationalPorts;

/// Storage-backed authoritative apply surface for columnar workers.
///
/// Co-location is confined here: the engine receives only the storage-reader
/// traits; `SharedRedbOperationalPorts` never crosses into service modules.
/// (Service cannot depend on `riffdb-storage-api`; this adapter is server-only.)
pub(crate) struct ServerColumnarApplySource {
    storage: SharedRedbOperationalPorts,
}

impl ServerColumnarApplySource {
    /// Wraps shared operational ports for apply reads only.
    #[must_use]
    pub(crate) fn new(storage: SharedRedbOperationalPorts) -> Self {
        Self { storage }
    }

    /// Borrows the underlying storage ports (catalog/head paths).
    #[must_use]
    pub(crate) fn storage(&self) -> &SharedRedbOperationalPorts {
        &self.storage
    }
}

impl AuthoritativePointReader for ServerColumnarApplySource {
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        AuthoritativePointReader::read_entity(&self.storage, target)
    }

    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        AuthoritativePointReader::read_stored_outcome(&self.storage, identity)
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        AuthoritativePointReader::read_commit(&self.storage, sequence)
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        AuthoritativePointReader::read_provenance(&self.storage, provenance_id)
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        AuthoritativePointReader::read_durable_event(&self.storage, event_id)
    }
}

impl AuthoritativeScanReader for ServerColumnarApplySource {
    fn scan_entity_partition(
        &self,
        request: riffdb_storage_api::AuthoritativeEntityPartitionScanRequest,
    ) -> Result<riffdb_storage_api::AuthoritativeEntityPartitionScanPage, StorageError> {
        AuthoritativeScanReader::scan_entity_partition(&self.storage, request)
    }

    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        AuthoritativeScanReader::scan_index(&self.storage, request)
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        AuthoritativeScanReader::scan_commits(&self.storage, request)
    }
}

impl fmt::Debug for ServerColumnarApplySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerColumnarApplySource([AUTHORITATIVE_READ])")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnarSlotLifecycle {
    Cold,
    Activating,
    Active,
    Failed,
    Stopped,
}

struct ActiveColumnarEngine {
    engine: ColumnarEngine,
    generation: Option<ProjectionGeneration>,
}

enum ColumnarSlotState {
    Cold,
    Activating,
    Active(Box<ActiveColumnarEngine>),
    Failed {
        generation: Option<ProjectionGeneration>,
    },
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnarActivationRequest {
    Building { started: bool },
    Active,
}

#[derive(Clone)]
pub(crate) struct ColumnarActivationSpec {
    definition: RegisteredDefinition,
    directory: PathBuf,
    generation: Option<ProjectionGeneration>,
    expected_durable_frontier: Option<FrontierPosition>,
    controlled_manifest: Option<Option<(u64, [u8; 32])>>,
    v2_selection: Option<(FrontierPosition, (u64, [u8; 32]))>,
}

impl ColumnarActivationSpec {
    pub(crate) fn with_control_selection(
        &self,
        directory: PathBuf,
        generation: ProjectionGeneration,
        manifest: Option<(u64, [u8; 32])>,
    ) -> Self {
        Self {
            definition: self.definition.clone(),
            directory,
            generation: Some(generation),
            expected_durable_frontier: None,
            controlled_manifest: Some(manifest),
            v2_selection: None,
        }
    }

    pub(crate) fn with_expected_durable_frontier(mut self, frontier: FrontierPosition) -> Self {
        self.expected_durable_frontier = Some(frontier);
        self
    }

    pub(crate) fn open(
        self,
        history_incarnation: u64,
    ) -> Result<(ColumnarEngine, Option<ProjectionGeneration>), ColumnarError> {
        if let Some((frontier, artifact)) = self.v2_selection {
            let generation = self.generation.ok_or(ColumnarError::Integrity(
                "selected V2 generation identity is absent",
            ))?;
            let physical = PhysicalGenerationFingerprintV1::compute(self.definition.fingerprint());
            let validated = ValidatedColumnarV2Generation::open(
                &self.directory,
                self.definition.clone(),
                history_incarnation,
                generation,
                frontier,
                artifact,
                physical,
            )
            .map_err(|_| ColumnarError::Integrity("selected V2 generation is invalid"))?;
            return ColumnarEngine::from_validated_v2(self.definition, validated)
                .map(|engine| (engine, Some(generation)));
        }
        let mut options =
            OpenOptions::new(self.directory).with_history_incarnation(history_incarnation);
        if let Some(selected) = self.controlled_manifest {
            options = options.with_controlled_manifest(selected);
        }
        let engine = ColumnarEngine::open(self.definition, options)?;
        if self
            .expected_durable_frontier
            .is_some_and(|expected| engine.durable_frontier().position() != expected)
        {
            return Err(ColumnarError::InvalidState(
                "selected durable frontier does not match the opened artifact",
            ));
        }
        Ok((engine, self.generation))
    }

    pub(crate) const fn generation(&self) -> Option<ProjectionGeneration> {
        self.generation
    }
}

/// One named engine retained cold until semantic demand.
pub(crate) struct ColumnarEngineSlot {
    state: Mutex<ColumnarSlotState>,
    #[cfg(test)]
    capture_attempt_probe: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
    definition: RegisteredDefinition,
    activation: ColumnarActivationSpec,
    cold_snapshot: Arc<riffdb_columnar::ColumnarSnapshot>,
    cold_frontier: FrontierPosition,
}

/// Exclusive publication guard over the same mutex used to capture query
/// snapshots. Captures completed before this guard was acquired retain their
/// immutable `Arc`; no later capture can start until the guard is dropped.
pub(crate) struct ColumnarCaptureGate<'a> {
    state: MutexGuard<'a, ColumnarSlotState>,
}

pub(crate) struct RetiredColumnarGeneration {
    directory: PathBuf,
    v1_artifact: Option<(u64, [u8; 32])>,
    captured_view: Weak<riffdb_columnar::ColumnarSnapshot>,
}

struct CapturedColumnarView {
    observation: ColumnarObservation,
    generation: Option<ProjectionGeneration>,
}

impl ColumnarCaptureGate<'_> {
    /// Installs the exact durable selection while capture remains closed.
    pub(crate) fn install_selected(
        &mut self,
        engine: ColumnarEngine,
        generation: ProjectionGeneration,
    ) -> Result<Option<RetiredColumnarGeneration>, ColumnarPortError> {
        if !matches!(
            &*self.state,
            ColumnarSlotState::Active(_)
                | ColumnarSlotState::Activating
                | ColumnarSlotState::Failed { .. }
        ) {
            return Err(ColumnarPortError::Unavailable);
        }
        let replacement = ColumnarSlotState::Active(Box::new(ActiveColumnarEngine {
            engine,
            generation: Some(generation),
        }));
        let previous = std::mem::replace(&mut *self.state, replacement);
        match previous {
            ColumnarSlotState::Active(previous) => {
                let previous_v1_artifact = previous.engine.controlled_v1_artifact_identity();
                let replacement_v1_artifact = match &*self.state {
                    ColumnarSlotState::Active(replacement) => {
                        replacement.engine.controlled_v1_artifact_identity()
                    }
                    _ => None,
                };
                if previous.generation == Some(generation)
                    && previous_v1_artifact == replacement_v1_artifact
                {
                    return Ok(None);
                }
                let snapshot = previous.engine.published_snapshot();
                Ok(Some(RetiredColumnarGeneration {
                    directory: previous.engine.directory().to_path_buf(),
                    v1_artifact: previous_v1_artifact,
                    captured_view: Arc::downgrade(&snapshot),
                }))
            }
            ColumnarSlotState::Activating | ColumnarSlotState::Failed { .. } => Ok(None),
            ColumnarSlotState::Cold | ColumnarSlotState::Stopped => {
                Err(ColumnarPortError::Unavailable)
            }
        }
    }

    /// Leaves the source closed when durable selection cannot be validated.
    pub(crate) fn remain_closed(&mut self, generation: Option<ProjectionGeneration>) {
        *self.state = ColumnarSlotState::Failed { generation };
    }
}

impl ColumnarEngineSlot {
    fn cold(
        definition: RegisteredDefinition,
        directory: PathBuf,
        generation: Option<ProjectionGeneration>,
        cold_frontier: FrontierPosition,
    ) -> Self {
        Self {
            state: Mutex::new(ColumnarSlotState::Cold),
            #[cfg(test)]
            capture_attempt_probe: Mutex::new(None),
            activation: ColumnarActivationSpec {
                definition: definition.clone(),
                directory,
                generation,
                expected_durable_frontier: None,
                controlled_manifest: None,
                v2_selection: None,
            },
            definition,
            cold_snapshot: Arc::new(riffdb_columnar::ColumnarSnapshot::empty()),
            cold_frontier,
        }
    }

    fn cold_controlled(
        definition: RegisteredDefinition,
        directory: PathBuf,
        generation: ProjectionGeneration,
        cold_frontier: FrontierPosition,
        manifest: Option<(u64, [u8; 32])>,
    ) -> Self {
        let mut slot = Self::cold(definition, directory, Some(generation), cold_frontier);
        slot.activation.controlled_manifest = Some(manifest);
        slot
    }

    /// Registered definition for this projection.
    #[must_use]
    pub(crate) fn definition(&self) -> &RegisteredDefinition {
        &self.definition
    }

    pub(crate) fn generation(&self) -> Option<ProjectionGeneration> {
        self.state.lock().ok().and_then(|state| match &*state {
            ColumnarSlotState::Active(active) => active.generation,
            ColumnarSlotState::Cold | ColumnarSlotState::Activating => self.activation.generation,
            ColumnarSlotState::Failed { generation } => *generation,
            ColumnarSlotState::Stopped => None,
        })
    }

    pub(crate) fn lifecycle(&self) -> Result<ColumnarSlotLifecycle, ColumnarPortError> {
        self.state
            .lock()
            .map(|state| match &*state {
                ColumnarSlotState::Cold => ColumnarSlotLifecycle::Cold,
                ColumnarSlotState::Activating => ColumnarSlotLifecycle::Activating,
                ColumnarSlotState::Active(_) => ColumnarSlotLifecycle::Active,
                ColumnarSlotState::Failed { .. } => ColumnarSlotLifecycle::Failed,
                ColumnarSlotState::Stopped => ColumnarSlotLifecycle::Stopped,
            })
            .map_err(|_| ColumnarPortError::Unavailable)
    }

    fn request_activation(&self) -> Result<ColumnarActivationRequest, ColumnarPortError> {
        #[cfg(test)]
        if let Ok(probe) = self.capture_attempt_probe.lock()
            && let Some(probe) = probe.as_ref()
        {
            let _ = probe.send(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        match &*state {
            ColumnarSlotState::Cold => {
                *state = ColumnarSlotState::Activating;
                Ok(ColumnarActivationRequest::Building { started: true })
            }
            ColumnarSlotState::Activating => {
                Ok(ColumnarActivationRequest::Building { started: false })
            }
            ColumnarSlotState::Active(_) => Ok(ColumnarActivationRequest::Active),
            ColumnarSlotState::Failed { .. } | ColumnarSlotState::Stopped => {
                Err(ColumnarPortError::Unavailable)
            }
        }
    }

    pub(crate) fn pending_activation(
        &self,
    ) -> Result<Option<ColumnarActivationSpec>, ColumnarPortError> {
        self.state
            .lock()
            .map(|state| match &*state {
                ColumnarSlotState::Activating => Some(self.activation.clone()),
                _ => None,
            })
            .map_err(|_| ColumnarPortError::Unavailable)
    }

    pub(crate) fn complete_activation(
        &self,
        engine: ColumnarEngine,
        generation: Option<ProjectionGeneration>,
    ) -> Result<(), ColumnarPortError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        if !matches!(&*state, ColumnarSlotState::Activating) {
            return Err(ColumnarPortError::Integrity);
        }
        *state = ColumnarSlotState::Active(Box::new(ActiveColumnarEngine { engine, generation }));
        Ok(())
    }

    pub(crate) fn fail_activation(&self, generation: Option<ProjectionGeneration>) {
        if let Ok(mut state) = self.state.lock()
            && matches!(&*state, ColumnarSlotState::Activating)
        {
            *state = ColumnarSlotState::Failed { generation };
        }
    }

    /// Re-enters only the exact activation whose durable failure classification
    /// could not be committed within the worker's bounded attempt.
    pub(crate) fn retry_failed_activation(
        &self,
        generation: ProjectionGeneration,
    ) -> Result<(), ColumnarPortError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        match &*state {
            ColumnarSlotState::Failed {
                generation: Some(failed),
            } if *failed == generation => {
                *state = ColumnarSlotState::Activating;
                Ok(())
            }
            _ => Err(ColumnarPortError::Integrity),
        }
    }

    /// Closes new query-view capture for one publication decision.
    pub(crate) fn close_capture_gate(&self) -> Result<ColumnarCaptureGate<'_>, ColumnarPortError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        if !matches!(
            &*state,
            ColumnarSlotState::Active(_)
                | ColumnarSlotState::Activating
                | ColumnarSlotState::Failed { .. }
        ) {
            return Err(ColumnarPortError::Unavailable);
        }
        Ok(ColumnarCaptureGate { state })
    }

    pub(crate) fn with_engine<T>(
        &self,
        operation: impl FnOnce(&ColumnarEngine) -> T,
    ) -> Result<Option<T>, ColumnarPortError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        match &*state {
            ColumnarSlotState::Active(active) => Ok(Some(operation(&active.engine))),
            ColumnarSlotState::Cold | ColumnarSlotState::Activating => Ok(None),
            ColumnarSlotState::Failed { .. } | ColumnarSlotState::Stopped => {
                Err(ColumnarPortError::Unavailable)
            }
        }
    }

    fn capture_active(
        &self,
        head: ProjectionFrontier,
    ) -> Result<Option<CapturedColumnarView>, ColumnarPortError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        match &*state {
            ColumnarSlotState::Active(active) => {
                let definition = active.engine.definition().clone();
                let snapshot = active.engine.published_snapshot();
                let published_frontier = active.engine.published_frontier();
                let outcome = active.engine.outcome(head.position());
                let (has_published, lifecycle) = map_outcome_lifecycle(&outcome);
                Ok(Some(CapturedColumnarView {
                    observation: ColumnarObservation::new(
                        definition,
                        snapshot,
                        published_frontier,
                        head,
                        has_published,
                        lifecycle,
                    ),
                    generation: active.generation,
                }))
            }
            ColumnarSlotState::Cold | ColumnarSlotState::Activating => Ok(None),
            ColumnarSlotState::Failed { .. } | ColumnarSlotState::Stopped => {
                Err(ColumnarPortError::Unavailable)
            }
        }
    }

    #[cfg(test)]
    fn set_capture_attempt_probe(&self, probe: Option<std::sync::mpsc::SyncSender<()>>) {
        if let Ok(mut installed) = self.capture_attempt_probe.lock() {
            *installed = probe;
        }
    }

    #[cfg(test)]
    fn try_capture_snapshot_for_test(
        &self,
    ) -> Result<Option<Arc<riffdb_columnar::ColumnarSnapshot>>, ColumnarPortError> {
        let state = self
            .state
            .try_lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        match &*state {
            ColumnarSlotState::Active(active) => Ok(Some(active.engine.published_snapshot())),
            ColumnarSlotState::Cold | ColumnarSlotState::Activating => Ok(None),
            ColumnarSlotState::Failed { .. } | ColumnarSlotState::Stopped => {
                Err(ColumnarPortError::Unavailable)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn with_engine_mut<T>(
        &self,
        operation: impl FnOnce(&mut ColumnarEngine) -> T,
    ) -> Result<Option<T>, ColumnarPortError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        match &mut *state {
            ColumnarSlotState::Active(active) => Ok(Some(operation(&mut active.engine))),
            ColumnarSlotState::Cold | ColumnarSlotState::Activating => Ok(None),
            ColumnarSlotState::Failed { .. } | ColumnarSlotState::Stopped => {
                Err(ColumnarPortError::Unavailable)
            }
        }
    }

    fn cold_observation(
        &self,
        head: ProjectionFrontier,
        frontier: FrontierPosition,
    ) -> ColumnarObservation {
        ColumnarObservation::new(
            self.definition.clone(),
            Arc::clone(&self.cold_snapshot),
            ProjectionFrontier::new(head.history_incarnation(), frontier),
            head,
            false,
            Some(ColumnarLifecycle::Building),
        )
    }

    pub(crate) fn stop(&self) {
        if let Ok(mut state) = self.state.lock() {
            *state = ColumnarSlotState::Stopped;
        }
    }
}

impl fmt::Debug for ColumnarEngineSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ColumnarEngineSlot")
            .field("name", &self.definition.name())
            .field("generation", &self.generation())
            .field("lifecycle", &self.lifecycle().ok())
            .finish()
    }
}

#[derive(Default)]
pub(crate) struct ColumnarActivationWake {
    epoch: Mutex<u64>,
    changed: Condvar,
}

impl ColumnarActivationWake {
    pub(crate) fn signal(&self) {
        if let Ok(mut epoch) = self.epoch.lock() {
            *epoch = epoch.saturating_add(1);
            self.changed.notify_all();
        }
    }

    pub(crate) fn current(&self) -> u64 {
        self.epoch.lock().map_or(u64::MAX, |epoch| *epoch)
    }

    pub(crate) fn wait(&self, observed: u64, duration: Duration) -> u64 {
        let Ok(epoch) = self.epoch.lock() else {
            return u64::MAX;
        };
        if *epoch != observed {
            return *epoch;
        }
        self.changed
            .wait_timeout(epoch, duration)
            .map_or(u64::MAX, |(epoch, _)| *epoch)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ColumnarRuntimeLifecycleObservation {
    cold_sources: u64,
    activations: u64,
    population_passes: u64,
}

impl ColumnarRuntimeLifecycleObservation {
    pub(crate) const fn cold_sources(self) -> u64 {
        self.cold_sources
    }

    pub(crate) const fn activations(self) -> u64 {
        self.activations
    }

    pub(crate) const fn population_passes(self) -> u64 {
        self.population_passes
    }
}

/// Process-local columnar engines and notifier fixed at startup registration.
pub(crate) struct ColumnarRuntime {
    engines: RwLock<BTreeMap<String, Arc<ColumnarEngineSlot>>>,
    notifier: ColumnarNotifier,
    names: RwLock<Vec<String>>,
    control_bindings: BTreeMap<String, ColumnarControlBinding>,
    projections_root: PathBuf,
    history_incarnation: u64,
    process_generation: [u8; 16],
    replay_clock: ServerColumnarReplayClock,
    apply_source: ServerColumnarApplySource,
    activation_wake: Arc<ColumnarActivationWake>,
    admitted_cold_sources: AtomicU64,
    activations: AtomicU64,
    population_passes: AtomicU64,
    /// Highest application commit sequence this process has observed, or 0
    /// before the first observation.
    ///
    /// The application frontier is monotonic within a history incarnation, so a
    /// head probe may resume from the last observation instead of rescanning
    /// the journal from sequence one.
    observed_head: AtomicU64,
    retired_generations: Mutex<Vec<RetiredColumnarGeneration>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ColumnarControlBinding {
    name: String,
    definition: RegisteredDefinition,
    spec: ColumnarProjectionSpecV1,
    is_vector: bool,
}

impl ColumnarControlBinding {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn definition(&self) -> &RegisteredDefinition {
        &self.definition
    }

    pub(crate) const fn spec(&self) -> &ColumnarProjectionSpecV1 {
        &self.spec
    }
}

impl ColumnarRuntime {
    /// Empty runtime when no columnar projections are configured.
    #[must_use]
    pub(crate) fn empty(
        storage: SharedRedbOperationalPorts,
        projections_root: PathBuf,
        history_incarnation: u64,
        process_generation: [u8; 16],
        replay_clock: ServerColumnarReplayClock,
    ) -> Arc<Self> {
        Arc::new(Self {
            engines: RwLock::new(BTreeMap::new()),
            notifier: ColumnarNotifier::from_names(Vec::new()),
            names: RwLock::new(Vec::new()),
            control_bindings: BTreeMap::new(),
            projections_root,
            history_incarnation,
            process_generation,
            replay_clock,
            apply_source: ServerColumnarApplySource::new(storage),
            activation_wake: Arc::new(ColumnarActivationWake::default()),
            admitted_cold_sources: AtomicU64::new(0),
            activations: AtomicU64::new(0),
            population_passes: AtomicU64::new(0),
            observed_head: AtomicU64::new(0),
            retired_generations: Mutex::new(Vec::new()),
        })
    }

    /// Test-only composition of pre-I/O resolution/control setup and cold open.
    #[cfg(test)]
    pub(crate) fn open(
        storage: SharedRedbOperationalPorts,
        projections: &[ConfiguredProjection],
        projections_root: &Path,
        history_incarnation: u64,
        process_generation: [u8; 16],
    ) -> Result<Arc<Self>, ColumnarRegistrationError> {
        let bindings = prepare_columnar_control_foundation(
            &storage,
            projections,
            projections_root,
            history_incarnation,
        )?;
        let replay_clock = crate::clocks::ProductionWallClocks::settable(Arc::new(
            std::sync::atomic::AtomicI64::new(1_700_000_000),
        ))
        .columnar_replay();
        Self::open_prepared(
            storage,
            bindings,
            projections_root,
            history_incarnation,
            process_generation,
            replay_clock,
        )
    }

    /// Admits schema-bound controls as cold slots without opening an artifact.
    pub(crate) fn open_prepared(
        storage: SharedRedbOperationalPorts,
        bindings: Vec<ColumnarControlBinding>,
        projections_root: &Path,
        history_incarnation: u64,
        process_generation: [u8; 16],
        replay_clock: ServerColumnarReplayClock,
    ) -> Result<Arc<Self>, ColumnarRegistrationError> {
        if bindings.is_empty() {
            return Ok(Self::empty(
                storage,
                projections_root.to_path_buf(),
                history_incarnation,
                process_generation,
                replay_clock,
            ));
        }
        let mut engines = BTreeMap::new();
        let mut names = Vec::with_capacity(bindings.len());
        let mut control_bindings = BTreeMap::new();
        for binding in bindings {
            let name = binding.name.clone();
            let control = storage
                .recover_expected_control(binding.spec.source())
                .map_err(|error| ColumnarRegistrationError::control_storage(error, &name))?
                .ok_or_else(ColumnarRegistrationError::synchronization)?;
            if !common_control_matches(&control, &binding.spec)
                || !common_control_history_incarnation_matches(&control, history_incarnation)
            {
                return Err(ColumnarRegistrationError::synchronization());
            }
            let allocation = control
                .servable_generation()
                .or_else(|| control.candidate())
                .ok_or_else(ColumnarRegistrationError::synchronization)?;
            let source_directory =
                controlled_source_directory(projections_root, binding.spec.hash());
            let selected = control.servable_generation();
            let (directory, manifest, v2_selection, cold_frontier) = match selected {
                None => (
                    controlled_generation_directory(
                        projections_root,
                        binding.spec.hash(),
                        allocation.generation(),
                    ),
                    None,
                    None,
                    FrontierPosition::BeforeFirst,
                ),
                Some(selected) => match selected.layout() {
                    ColumnarProjectionLayoutV1::V1 => (
                        controlled_generation_directory(
                            projections_root,
                            binding.spec.hash(),
                            selected.generation(),
                        ),
                        selected
                            .artifact()
                            .map(|artifact| (artifact.length(), artifact.checksum())),
                        None,
                        selected.frontier(),
                    ),
                    ColumnarProjectionLayoutV1::V2 => {
                        let artifact = selected
                            .artifact()
                            .ok_or_else(ColumnarRegistrationError::synchronization)?;
                        let expected_physical = PhysicalGenerationFingerprintV1::compute(
                            binding.definition.fingerprint(),
                        );
                        if selected.physical_generation_fingerprint()
                            != Some(*expected_physical.as_bytes())
                        {
                            return Err(ColumnarRegistrationError::synchronization());
                        }
                        (
                            source_directory,
                            None,
                            Some((
                                selected.frontier(),
                                (artifact.length(), artifact.checksum()),
                            )),
                            selected.frontier(),
                        )
                    }
                },
            };
            engines.insert(
                name.clone(),
                Arc::new(ColumnarEngineSlot::cold_controlled(
                    binding.definition.clone(),
                    directory,
                    allocation.generation(),
                    cold_frontier,
                    manifest,
                )),
            );
            if let Some(slot) = engines.get_mut(&name) {
                Arc::get_mut(slot)
                    .ok_or_else(ColumnarRegistrationError::synchronization)?
                    .activation
                    .v2_selection = v2_selection;
            }
            names.push(name.clone());
            control_bindings.insert(name, binding);
        }
        names.sort();
        let notifier = ColumnarNotifier::from_names(names.iter().cloned());
        let admitted_cold_sources = u64::try_from(names.len()).unwrap_or(u64::MAX);
        Ok(Arc::new(Self {
            engines: RwLock::new(engines),
            notifier,
            names: RwLock::new(names),
            control_bindings,
            projections_root: projections_root.to_path_buf(),
            history_incarnation,
            process_generation,
            replay_clock,
            apply_source: ServerColumnarApplySource::new(storage),
            activation_wake: Arc::new(ColumnarActivationWake::default()),
            admitted_cold_sources: AtomicU64::new(admitted_cold_sources),
            activations: AtomicU64::new(0),
            population_passes: AtomicU64::new(0),
            observed_head: AtomicU64::new(0),
            retired_generations: Mutex::new(Vec::new()),
        }))
    }

    /// Shared notifier for register-before-read waits.
    #[must_use]
    pub(crate) fn notifier(&self) -> &ColumnarNotifier {
        &self.notifier
    }

    /// Startup-fixed known projection names.
    pub(crate) fn names(&self) -> Result<Vec<String>, ColumnarPortError> {
        self.names
            .read()
            .map(|names| names.clone())
            .map_err(|_| ColumnarPortError::Unavailable)
    }

    /// History incarnation bound into published frontiers.
    #[must_use]
    pub(crate) const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    #[must_use]
    pub(crate) const fn process_generation(&self) -> [u8; 16] {
        self.process_generation
    }

    pub(crate) fn sample_columnar_replay_utc(
        &self,
    ) -> Result<riffdb_types::Timestamp, crate::clocks::ServerProcessClockError> {
        self.replay_clock.now()
    }

    #[must_use]
    pub(crate) fn projections_root(&self) -> &Path {
        &self.projections_root
    }

    pub(crate) fn defer_retirement(
        &self,
        generation: RetiredColumnarGeneration,
    ) -> Result<(), ColumnarPortError> {
        self.retired_generations
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?
            .push(generation);
        Ok(())
    }

    pub(crate) fn reclaim_unselected_generations(
        &self,
        binding: &ColumnarControlBinding,
        control: &StoredColumnarProjectionControlV1,
    ) -> Result<(), ColumnarPortError> {
        let source_directory =
            controlled_source_directory(&self.projections_root, binding.spec.hash());
        let mut retained = BTreeSet::new();
        let mut retained_v1_artifacts = BTreeMap::<PathBuf, BTreeSet<(u64, [u8; 32])>>::new();
        for pointer in [
            control.published(),
            control.candidate(),
            control.predecessor(),
        ]
        .into_iter()
        .flatten()
        {
            let directory = controlled_layout_generation_directory(
                &source_directory,
                pointer.layout(),
                pointer.generation(),
            );
            retained.insert(directory.clone());
            if pointer.layout() == ColumnarProjectionLayoutV1::V1
                && let Some(artifact) = pointer.artifact()
            {
                retained_v1_artifacts
                    .entry(directory.clone())
                    .or_default()
                    .insert((artifact.length(), artifact.checksum()));
            }
            if pointer.role() == riffdb_storage_api::ColumnarProjectionGenerationRoleV1::Candidate {
                retained.insert(directory.with_file_name(format!(
                    "{}.tmp",
                    directory
                        .file_name()
                        .and_then(|name| name.to_str())
                        .ok_or(ColumnarPortError::Integrity)?
                )));
            }
        }

        let mut retired = self
            .retired_generations
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        retired.retain(|generation| generation.captured_view.upgrade().is_some());
        for generation in retired.iter() {
            retained.insert(generation.directory.clone());
            if let Some(artifact) = generation.v1_artifact {
                retained_v1_artifacts
                    .entry(generation.directory.clone())
                    .or_default()
                    .insert(artifact);
            }
        }

        for (directory, artifacts) in &retained_v1_artifacts {
            let artifacts = artifacts.iter().copied().collect::<Vec<_>>();
            ColumnarEngine::reclaim_controlled_v1_artifacts(directory, &artifacts)
                .map_err(|_| ColumnarPortError::Unavailable)?;
        }

        let entries = match std::fs::read_dir(&source_directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(ColumnarPortError::Unavailable),
        };
        for entry in entries {
            let entry = entry.map_err(|_| ColumnarPortError::Unavailable)?;
            let file_type = entry
                .file_type()
                .map_err(|_| ColumnarPortError::Unavailable)?;
            if !file_type.is_dir() {
                continue;
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| ColumnarPortError::Integrity)?;
            let Some((layout, generation, temporary)) = managed_generation_directory_name(&name)
            else {
                continue;
            };
            if retained.contains(&entry.path()) {
                continue;
            }
            if layout == ColumnarProjectionLayoutV1::V2 && !temporary {
                ValidatedColumnarV2Generation::reclaim(&source_directory, generation)
                    .map_err(|_| ColumnarPortError::Unavailable)?;
            } else {
                std::fs::remove_dir_all(entry.path())
                    .map_err(|_| ColumnarPortError::Unavailable)?;
                std::fs::File::open(&source_directory)
                    .and_then(|parent| parent.sync_all())
                    .map_err(|_| ColumnarPortError::Unavailable)?;
            }
        }
        Ok(())
    }

    /// Registered engine slots in name order.
    pub(crate) fn engines(
        &self,
    ) -> Result<Vec<(String, Arc<ColumnarEngineSlot>)>, ColumnarPortError> {
        self.engines
            .read()
            .map(|engines| {
                engines
                    .iter()
                    .map(|(name, slot)| (name.clone(), Arc::clone(slot)))
                    .collect()
            })
            .map_err(|_| ColumnarPortError::Unavailable)
    }

    pub(crate) fn engine(
        &self,
        name: &str,
    ) -> Result<Option<Arc<ColumnarEngineSlot>>, ColumnarPortError> {
        self.engines
            .read()
            .map(|engines| engines.get(name).cloned())
            .map_err(|_| ColumnarPortError::Unavailable)
    }

    fn control_binding(&self, name: &str) -> Option<&ColumnarControlBinding> {
        self.control_bindings.get(name)
    }

    pub(crate) fn control_bindings(&self) -> Vec<ColumnarControlBinding> {
        self.control_bindings.values().cloned().collect()
    }

    pub(crate) fn open_controlled_generation(
        &self,
        binding: &ColumnarControlBinding,
        generation: &riffdb_storage_api::StoredColumnarProjectionGenerationV1,
    ) -> Result<ColumnarEngine, ColumnarError> {
        let artifact = generation
            .artifact()
            .map(|artifact| (artifact.length(), artifact.checksum()));
        if generation.layout() == ColumnarProjectionLayoutV1::V2 {
            let artifact = artifact.ok_or(ColumnarError::Integrity(
                "selected V2 artifact identity is absent",
            ))?;
            let expected_physical =
                PhysicalGenerationFingerprintV1::compute(binding.definition.fingerprint());
            if generation.physical_generation_fingerprint() != Some(*expected_physical.as_bytes()) {
                return Err(ColumnarError::Integrity(
                    "selected V2 physical fingerprint does not match",
                ));
            }
            return ColumnarActivationSpec {
                definition: binding.definition.clone(),
                directory: controlled_source_directory(&self.projections_root, binding.spec.hash()),
                generation: Some(generation.generation()),
                expected_durable_frontier: None,
                controlled_manifest: None,
                v2_selection: Some((generation.frontier(), artifact)),
            }
            .open(self.history_incarnation)
            .map(|(engine, _)| engine);
        }
        let mut spec = ColumnarActivationSpec {
            definition: binding.definition.clone(),
            directory: controlled_generation_directory(
                &self.projections_root,
                binding.spec.hash(),
                generation.generation(),
            ),
            generation: Some(generation.generation()),
            expected_durable_frontier: None,
            controlled_manifest: Some(artifact),
            v2_selection: None,
        };
        if artifact.is_some() {
            spec = spec.with_expected_durable_frontier(generation.frontier());
        }
        spec.open(self.history_incarnation)
            .map(|(engine, _)| engine)
    }

    pub(crate) fn open_prepared_generation(
        &self,
        binding: &ColumnarControlBinding,
        generation: &riffdb_storage_api::StoredColumnarProjectionGenerationV1,
    ) -> Result<(ColumnarEngine, PreparedColumnarGenerationV1), ColumnarError> {
        if generation.layout() == ColumnarProjectionLayoutV1::V2 {
            let validated = ValidatedColumnarV2Generation::open_prepared_candidate(
                &controlled_source_directory(&self.projections_root, binding.spec.hash()),
                binding.definition.clone(),
                generation,
            )
            .map_err(|_| ColumnarError::Integrity("prepared V2 generation is invalid"))?;
            let prepared = validated
                .prepared_generation(&binding.spec, generation.clone(), self.process_generation)
                .map_err(|_| ColumnarError::Integrity("prepared V2 witness does not match"))?;
            let engine = ColumnarEngine::from_validated_v2(binding.definition.clone(), validated)?;
            return Ok((engine, prepared));
        }
        let engine = self.open_controlled_generation(binding, generation)?;
        let prepared = engine
            .prepared_v1_generation(&binding.spec, generation.clone(), self.process_generation)
            .map_err(|_| ColumnarError::Integrity("prepared V1 witness does not match"))?;
        Ok((engine, prepared))
    }

    /// Apply source used by the worker (`AuthoritativeScanReader` + point reads).
    #[must_use]
    pub(crate) fn apply_source(&self) -> &ServerColumnarApplySource {
        &self.apply_source
    }

    /// Authoritative storage for catalog/head observation.
    #[must_use]
    pub(crate) fn storage(&self) -> &SharedRedbOperationalPorts {
        self.apply_source.storage()
    }

    /// Reads the frozen application commit head without holding an engine lock.
    ///
    /// Resumes the probe from the highest sequence this process has already
    /// observed. `scan_commits` derives the returned `inclusive_upper` from the
    /// snapshot's own application frontier before it reads any commit row, so a
    /// probe that starts above the head returns the identical frontier without
    /// touching the commit range at all.
    pub(crate) fn read_application_head(&self) -> Result<FrontierPosition, ColumnarPortError> {
        let observed = CommitSequence::new(self.observed_head.load(Ordering::Acquire));
        let head = read_application_head_after(self.storage(), observed)?;
        if let FrontierPosition::AppliedThrough(sequence) = head {
            self.observed_head
                .fetch_max(sequence.get(), Ordering::AcqRel);
        }
        Ok(head)
    }

    pub(crate) fn request_activation(
        &self,
        slot: &ColumnarEngineSlot,
    ) -> Result<bool, ColumnarPortError> {
        match slot.request_activation()? {
            ColumnarActivationRequest::Building { started } => {
                if started {
                    self.activations.fetch_add(1, Ordering::AcqRel);
                    crate::startup_census::record_columnar_activation();
                    self.activation_wake.signal();
                }
                Ok(true)
            }
            ColumnarActivationRequest::Active => Ok(false),
        }
    }

    pub(crate) fn activation_wake(&self) -> Arc<ColumnarActivationWake> {
        Arc::clone(&self.activation_wake)
    }

    pub(crate) fn current_activation_spec(
        &self,
        name: &str,
        slot: &ColumnarEngineSlot,
    ) -> Result<Option<ColumnarActivationSpec>, ColumnarPortError> {
        let Some(mut spec) = slot.pending_activation()? else {
            return Ok(None);
        };
        let binding = self
            .control_binding(name)
            .ok_or(ColumnarPortError::Integrity)?;
        let control = self
            .storage()
            .recover_expected_control(binding.spec.source())
            .map_err(map_port_storage)?
            .ok_or(ColumnarPortError::Integrity)?;
        if !common_control_matches(&control, &binding.spec)
            || !common_control_history_incarnation_matches(&control, self.history_incarnation)
        {
            return Err(ColumnarPortError::Integrity);
        }
        let Some(selected) = control.servable_generation() else {
            return Ok(None);
        };
        if selected.layout() == ColumnarProjectionLayoutV1::V2 {
            let artifact = selected.artifact().ok_or(ColumnarPortError::Integrity)?;
            let expected_physical =
                PhysicalGenerationFingerprintV1::compute(binding.definition.fingerprint());
            if selected.physical_generation_fingerprint() != Some(*expected_physical.as_bytes()) {
                return Err(ColumnarPortError::Integrity);
            }
            spec.directory =
                controlled_source_directory(&self.projections_root, binding.spec.hash());
            spec.generation = Some(selected.generation());
            spec.controlled_manifest = None;
            spec.v2_selection = Some((
                selected.frontier(),
                (artifact.length(), artifact.checksum()),
            ));
        } else {
            spec = spec.with_control_selection(
                controlled_generation_directory(
                    &self.projections_root,
                    binding.spec.hash(),
                    selected.generation(),
                ),
                selected.generation(),
                selected
                    .artifact()
                    .map(|artifact| (artifact.length(), artifact.checksum())),
            );
        }
        if selected.artifact().is_some() {
            spec = spec.with_expected_durable_frontier(selected.frontier());
        }
        Ok(Some(spec))
    }

    fn cold_observation(
        &self,
        name: &str,
        slot: &ColumnarEngineSlot,
        head: ProjectionFrontier,
    ) -> Result<ColumnarObservation, ColumnarPortError> {
        let binding = self
            .control_binding(name)
            .ok_or(ColumnarPortError::Integrity)?;
        let control = self
            .storage()
            .recover_expected_control(binding.spec.source())
            .map_err(map_port_storage)?
            .ok_or(ColumnarPortError::Integrity)?;
        if !common_control_matches(&control, &binding.spec)
            || !common_control_history_incarnation_matches(&control, self.history_incarnation)
        {
            return Err(ColumnarPortError::Integrity);
        }
        let frontier = control
            .servable_generation()
            .map_or(slot.cold_frontier, |generation| generation.frontier());
        Ok(slot.cold_observation(head, frontier))
    }

    pub(crate) fn record_population_pass(&self) {
        self.population_passes.fetch_add(1, Ordering::AcqRel);
        crate::startup_census::record_columnar_population_pass();
    }

    pub(crate) fn lifecycle_observation(&self) -> ColumnarRuntimeLifecycleObservation {
        ColumnarRuntimeLifecycleObservation {
            cold_sources: self.admitted_cold_sources.load(Ordering::Acquire),
            activations: self.activations.load(Ordering::Acquire),
            population_passes: self.population_passes.load(Ordering::Acquire),
        }
    }

    pub(crate) fn stop_slots(&self) {
        if let Ok(engines) = self.engines.read() {
            for slot in engines.values() {
                slot.stop();
            }
        }
    }

    pub(crate) fn aggregate_lifecycle(&self) -> ColumnarSlotLifecycle {
        let Ok(engines) = self.engines.read() else {
            return ColumnarSlotLifecycle::Failed;
        };
        let mut observed = ColumnarSlotLifecycle::Active;
        for slot in engines.values() {
            match slot.lifecycle() {
                Ok(ColumnarSlotLifecycle::Failed) | Err(_) => {
                    return ColumnarSlotLifecycle::Failed;
                }
                Ok(ColumnarSlotLifecycle::Stopped) => {
                    observed = ColumnarSlotLifecycle::Stopped;
                }
                Ok(ColumnarSlotLifecycle::Cold | ColumnarSlotLifecycle::Activating) => {
                    if observed == ColumnarSlotLifecycle::Active {
                        observed = ColumnarSlotLifecycle::Cold;
                    }
                }
                Ok(ColumnarSlotLifecycle::Active) => {}
            }
        }
        observed
    }
}

/// Resolves every configured/compiler-declared source before installing any
/// common control or touching the projection filesystem.
pub(crate) fn prepare_columnar_control_foundation(
    storage: &SharedRedbOperationalPorts,
    projections: &[ConfiguredProjection],
    projections_root: &Path,
    history_incarnation: u64,
) -> Result<Vec<ColumnarControlBinding>, ColumnarRegistrationError> {
    if projections.len() > 256
        || projections.iter().any(|projection| {
            let name = projection.name();
            name.is_empty()
                || name.len() > 256
                || name == "."
                || name == ".."
                || name.contains('/')
                || name.contains('\\')
        })
    {
        return Err(ColumnarRegistrationError::definition(
            first_projection_name(projections),
        ));
    }
    let active = ActiveCatalogSnapshot::read(storage).map_err(|error| {
        ColumnarRegistrationError::storage(error, first_projection_name(projections))
    })?;
    let Some(active) = active else {
        if projections.is_empty() {
            return Ok(Vec::new());
        }
        return Err(ColumnarRegistrationError::no_active_catalog(
            first_projection_name(projections),
        ));
    };
    let bundle = active.bundle().bundle();
    let mut bindings =
        Vec::with_capacity(projections.len() + bundle.schema().vector_production_specs().len());
    let mut names = BTreeSet::new();
    let mut sources = BTreeSet::new();
    for configured in projections {
        let name = configured.name().to_owned();
        let definition = resolve_configured_projection(configured, bundle)?;
        let spec = ColumnarProjectionSpecV1::for_scalar(&definition, bundle)
            .map_err(|_| ColumnarRegistrationError::definition(name.clone()))?;
        insert_control_binding(
            &mut bindings,
            &mut names,
            &mut sources,
            ColumnarControlBinding {
                name,
                definition,
                spec,
                is_vector: false,
            },
        )?;
    }
    for production in bundle.schema().vector_production_specs() {
        let entity = bundle
            .schema()
            .entity(production.entity())
            .ok_or_else(|| ColumnarRegistrationError::definition("production-vector"))?;
        let vector_field = entity
            .record()
            .field(production.field())
            .ok_or_else(|| ColumnarRegistrationError::definition("production-vector"))?;
        let name = format!("{}.{}", entity.name(), vector_field.name());
        let definition = resolve_vector_registration(bundle, entity, &name)?;
        let spec = ColumnarProjectionSpecV1::for_vector(&definition, production.field(), bundle)
            .map_err(|_| ColumnarRegistrationError::definition(name.clone()))?;
        insert_control_binding(
            &mut bindings,
            &mut names,
            &mut sources,
            ColumnarControlBinding {
                name,
                definition,
                spec,
                is_vector: true,
            },
        )?;
    }
    if bindings.len() > 256 {
        return Err(ColumnarRegistrationError::definition("columnar-control"));
    }

    let mut fresh = Vec::new();
    for binding in &bindings {
        let observed = storage
            .recover_expected_control(binding.spec.source())
            .map_err(|error| ColumnarRegistrationError::control_storage(error, &binding.name))?;
        if observed.is_none() {
            fresh.push(
                FreshColumnarProjectionControlV1::new(
                    binding.spec.source().clone(),
                    binding.spec.definition_fingerprint(),
                    binding.spec.hash(),
                    binding.spec.replay_limits(),
                    history_incarnation,
                )
                .map_err(|_| ColumnarRegistrationError::definition(&binding.name))?,
            );
        }
    }
    if !fresh.is_empty() {
        let _ = storage.initialize_fresh_v1(&fresh).map_err(|error| {
            ColumnarRegistrationError::control_storage(error, "columnar-control")
        })?;
    }
    // Applied and transaction-current mismatch both resolve by complete reread;
    // no initialization outcome is inferred from the attempted write.
    for binding in &bindings {
        let mut observed = storage
            .recover_expected_control(binding.spec.source())
            .map_err(|error| ColumnarRegistrationError::control_storage(error, &binding.name))?
            .ok_or_else(ColumnarRegistrationError::synchronization)?;
        if !common_control_history_incarnation_matches(&observed, history_incarnation) {
            let reset = storage.reset_for_current_history_incarnation(&observed);
            observed = storage
                .recover_expected_control(binding.spec.source())
                .map_err(|error| ColumnarRegistrationError::control_storage(error, &binding.name))?
                .ok_or_else(ColumnarRegistrationError::synchronization)?;
            if !common_control_history_incarnation_matches(&observed, history_incarnation) {
                return match reset {
                    Err(error) => Err(ColumnarRegistrationError::control_storage(
                        error,
                        &binding.name,
                    )),
                    Ok(_) => Err(ColumnarRegistrationError::synchronization()),
                };
            }
        }
        reconcile_common_control(storage, binding, observed)?;
        let reconciled = storage
            .recover_expected_control(binding.spec.source())
            .map_err(|error| ColumnarRegistrationError::control_storage(error, &binding.name))?
            .ok_or_else(ColumnarRegistrationError::synchronization)?;
        retire_unprepared_v1_candidate_paths(
            projections_root,
            binding,
            &reconciled,
            history_incarnation,
        )?;
    }
    bindings.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(bindings)
}

fn insert_control_binding(
    bindings: &mut Vec<ColumnarControlBinding>,
    names: &mut BTreeSet<String>,
    sources: &mut BTreeSet<Vec<u8>>,
    binding: ColumnarControlBinding,
) -> Result<(), ColumnarRegistrationError> {
    if !names.insert(binding.name.clone())
        || !sources.insert(binding.spec.source().to_canonical_bytes())
    {
        return Err(ColumnarRegistrationError::duplicate_name(binding.name));
    }
    bindings.push(binding);
    Ok(())
}

fn reconcile_common_control(
    storage: &SharedRedbOperationalPorts,
    binding: &ColumnarControlBinding,
    observed: StoredColumnarProjectionControlV1,
) -> Result<(), ColumnarRegistrationError> {
    if common_control_matches(&observed, &binding.spec) {
        let Some(published) = selected_corruption_without_rebuild_candidate(&observed) else {
            return Ok(());
        };
        let physical = match published.layout() {
            ColumnarProjectionLayoutV1::V1 => None,
            ColumnarProjectionLayoutV1::V2 => Some(
                *PhysicalGenerationFingerprintV1::compute(binding.definition.fingerprint())
                    .as_bytes(),
            ),
        };
        let _ = storage
            .allocate_unservable_rebuild_candidate(
                &observed,
                binding.spec.definition_fingerprint(),
                binding.spec.hash(),
                binding.spec.replay_limits(),
                published.layout(),
                physical,
            )
            .map_err(|error| ColumnarRegistrationError::control_storage(error, &binding.name))?;
        let recovered = storage
            .recover_expected_control(binding.spec.source())
            .map_err(|error| ColumnarRegistrationError::control_storage(error, &binding.name))?
            .ok_or_else(ColumnarRegistrationError::synchronization)?;
        return if common_control_matches(&recovered, &binding.spec)
            && (recovered.servable_generation().is_some() || recovered.candidate().is_some())
        {
            Ok(())
        } else {
            Err(ColumnarRegistrationError::synchronization())
        };
    }
    let _ = if observed.published().is_none() && observed.predecessor().is_none() {
        storage.retarget_initial_candidate(
            &observed,
            binding.spec.definition_fingerprint(),
            binding.spec.hash(),
            binding.spec.replay_limits(),
        )
    } else {
        storage.allocate_unservable_rebuild_candidate(
            &observed,
            binding.spec.definition_fingerprint(),
            binding.spec.hash(),
            binding.spec.replay_limits(),
            ColumnarProjectionLayoutV1::V1,
            None,
        )
    }
    .map_err(|error| ColumnarRegistrationError::control_storage(error, &binding.name))?;
    // Applied, mismatch, storage uncertainty, and a racing later transition
    // authorize nothing by inference; only this complete reread is consumed.
    let recovered = storage
        .recover_expected_control(binding.spec.source())
        .map_err(|error| ColumnarRegistrationError::control_storage(error, &binding.name))?
        .ok_or_else(ColumnarRegistrationError::synchronization)?;
    if common_control_matches(&recovered, &binding.spec) {
        Ok(())
    } else {
        Err(ColumnarRegistrationError::synchronization())
    }
}

const MAX_RESET_CANDIDATE_FILES: usize =
    riffdb_columnar::MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS + 2;
const MAX_RESET_CANDIDATE_BYTES: u64 =
    (riffdb_columnar::MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS as u64 + 2)
        * riffdb_columnar::MAX_SEGMENT_V2_BYTES as u64;

fn retire_unprepared_v1_candidate_paths(
    projections_root: &Path,
    binding: &ColumnarControlBinding,
    control: &StoredColumnarProjectionControlV1,
    history_incarnation: u64,
) -> Result<(), ColumnarRegistrationError> {
    let Some(candidate) = control.candidate() else {
        return Ok(());
    };
    if control.lifecycle() != riffdb_storage_api::ColumnarProjectionLifecycleV1::Building
        || control.published().is_some()
        || control.predecessor().is_some()
        || control.failure().is_some()
        || candidate.layout() != ColumnarProjectionLayoutV1::V1
        || candidate.history_incarnation() != history_incarnation
        || candidate.frontier() != FrontierPosition::BeforeFirst
        || candidate.snapshot_frontier().is_some()
        || candidate.artifact().is_some()
    {
        return Ok(());
    }
    let final_path = controlled_generation_directory(
        projections_root,
        binding.spec.hash(),
        candidate.generation(),
    );
    let temporary_path = final_path.with_file_name(format!(
        "{}.tmp",
        final_path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(ColumnarRegistrationError::synchronization)?
    ));
    for path in [&final_path, &temporary_path] {
        retire_exact_candidate_path(path, history_incarnation)
            .map_err(|_| ColumnarRegistrationError::synchronization())?;
    }
    Ok(())
}

fn retire_exact_candidate_path(path: &Path, history_incarnation: u64) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "candidate path has no parent",
        )
    })?;
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "candidate path is not UTF-8",
            )
        })?;
    let quarantine = parent.join(format!(
        "{name}.retired-before-history-{history_incarnation:016x}"
    ));
    if remove_bounded_candidate_directory(&quarantine)? {
        sync_directory(parent)?;
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "candidate path is not an ordinary directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    if std::fs::symlink_metadata(&quarantine).is_ok() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "candidate quarantine unexpectedly exists",
        ));
    }
    std::fs::rename(path, &quarantine)?;
    sync_directory(parent)
}

fn remove_bounded_candidate_directory(path: &Path) -> std::io::Result<bool> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "candidate quarantine is not an ordinary directory",
        ));
    }
    let mut files = Vec::new();
    let mut bytes = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if files.len() == MAX_RESET_CANDIDATE_FILES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "candidate quarantine exceeds file bound",
            ));
        }
        let file_type = entry.file_type()?;
        if !file_type.is_file() || file_type.is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "candidate quarantine contains non-file material",
            ));
        }
        let metadata = entry.metadata()?;
        bytes = bytes.checked_add(metadata.len()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "candidate byte bound overflow",
            )
        })?;
        if bytes > MAX_RESET_CANDIDATE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "candidate quarantine exceeds byte bound",
            ));
        }
        files.push(entry.path());
    }
    for file in files {
        std::fs::remove_file(file)?;
    }
    std::fs::remove_dir(path)?;
    Ok(true)
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

fn selected_corruption_without_rebuild_candidate(
    control: &StoredColumnarProjectionControlV1,
) -> Option<&StoredColumnarProjectionGenerationV1> {
    let published = control.published()?;
    (control.lifecycle() == riffdb_storage_api::ColumnarProjectionLifecycleV1::Degraded
        && control.candidate().is_none()
        && control.predecessor().is_none()
        && control.failure().is_some_and(|failure| {
            failure.target() == riffdb_storage_api::ColumnarProjectionFailureTargetV1::Published
                && failure.reason()
                    == riffdb_storage_api::ColumnarProjectionFailureReasonV1::ArtifactInvalid
                && failure.generation() == Some(published.generation())
        }))
    .then_some(published)
}

fn common_control_matches(
    control: &StoredColumnarProjectionControlV1,
    spec: &ColumnarProjectionSpecV1,
) -> bool {
    control.target_definition_fingerprint() == spec.definition_fingerprint()
        && control.target_spec_hash() == spec.hash()
        && control.replay_limits() == spec.replay_limits()
}

fn common_control_history_incarnation_matches(
    control: &StoredColumnarProjectionControlV1,
    history_incarnation: u64,
) -> bool {
    [
        control.published(),
        control.candidate(),
        control.predecessor(),
    ]
    .into_iter()
    .flatten()
    .all(|generation| generation.history_incarnation() == history_incarnation)
}

pub(crate) fn controlled_generation_directory(
    projections_root: &Path,
    spec_hash: riffdb_types::ColumnarProjectionSpecHashV1,
    generation: ProjectionGeneration,
) -> PathBuf {
    controlled_source_directory(projections_root, spec_hash)
        .join(format!("generation-{:020}", generation.get()))
}

pub(crate) fn controlled_source_directory(
    projections_root: &Path,
    spec_hash: riffdb_types::ColumnarProjectionSpecHashV1,
) -> PathBuf {
    projections_root.join(format!("source-{}", lower_hex(spec_hash.as_bytes())))
}

fn controlled_layout_generation_directory(
    source_directory: &Path,
    layout: ColumnarProjectionLayoutV1,
    generation: ProjectionGeneration,
) -> PathBuf {
    match layout {
        ColumnarProjectionLayoutV1::V1 => {
            source_directory.join(format!("generation-{:020}", generation.get()))
        }
        ColumnarProjectionLayoutV1::V2 => {
            source_directory.join(format!("generation-{:016x}", generation.get()))
        }
    }
}

fn managed_generation_directory_name(
    name: &str,
) -> Option<(ColumnarProjectionLayoutV1, ProjectionGeneration, bool)> {
    let (base, temporary) = name
        .strip_suffix(".tmp")
        .map_or((name, false), |base| (base, true));
    let suffix = base.strip_prefix("generation-")?;
    let (layout, value) = if suffix.len() == 20 && suffix.bytes().all(|byte| byte.is_ascii_digit())
    {
        (ColumnarProjectionLayoutV1::V1, suffix.parse::<u64>().ok()?)
    } else if suffix.len() == 16
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        (
            ColumnarProjectionLayoutV1::V2,
            u64::from_str_radix(suffix, 16).ok()?,
        )
    } else {
        return None;
    };
    ProjectionGeneration::new(value).map(|generation| (layout, generation, temporary))
}

fn lower_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut text, "{byte:02x}");
    }
    text
}

fn resolve_production_vector_projection(
    bundle: &ContractBundle,
    entity: &riffdb_contract_ir::EntitySchema,
    name: &str,
) -> Result<RegisteredDefinition, ColumnarRegistrationError> {
    let aggregate = bundle
        .schema()
        .aggregate_for_entity(entity.id())
        .ok_or_else(|| ColumnarRegistrationError::definition(name))?;
    let partition = aggregate
        .keys()
        .expressions()
        .get(aggregate.keys().partition_expression())
        .ok_or_else(|| ColumnarRegistrationError::definition(name))?;
    let ExpressionKind::SchemaField { entity_type, field } = partition.kind() else {
        return Err(ColumnarRegistrationError::definition(name));
    };
    if *entity_type != entity.id() || entity.record().field(*field).is_none() {
        return Err(ColumnarRegistrationError::definition(name));
    }
    let projected_fields = entity
        .record()
        .fields()
        .iter()
        .filter(|field| !entity.primary_key_fields().contains(&field.id()))
        .filter(|field| production_column_type_supported(field.value_type()))
        .map(|field| field.id())
        .collect::<Vec<_>>();
    RegisteredDefinition::register(
        ColumnarProjectionDefinition {
            name: name.to_owned(),
            entity_name: entity.name().to_owned(),
            projected_fields,
            org_scope_field: *field,
        },
        bundle,
    )
    .map_err(|_| ColumnarRegistrationError::definition(name))
}

fn resolve_vector_registration(
    bundle: &ContractBundle,
    entity: &riffdb_contract_ir::EntitySchema,
    name: &str,
) -> Result<RegisteredDefinition, ColumnarRegistrationError> {
    resolve_production_vector_projection(bundle, entity, name)
}

fn production_column_type_supported(value_type: &ValueType) -> bool {
    match value_type.tag() {
        ValueTypeTag::Bool
        | ValueTypeTag::I64
        | ValueTypeTag::U64
        | ValueTypeTag::String
        | ValueTypeTag::Uuid
        | ValueTypeTag::Enum
        | ValueTypeTag::Timestamp
        | ValueTypeTag::Date
        | ValueTypeTag::Decimal
        | ValueTypeTag::Money
        | ValueTypeTag::Bytes
        | ValueTypeTag::Vector => true,
        ValueTypeTag::Optional => value_type
            .optional_inner()
            .is_some_and(production_column_type_supported),
        ValueTypeTag::List | ValueTypeTag::Record => false,
    }
}

impl fmt::Debug for ColumnarRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ColumnarRuntime")
            .field(
                "projection_count",
                &self.names.read().map_or(0, |names| names.len()),
            )
            .field("history_incarnation", &self.history_incarnation)
            .finish()
    }
}

/// Published observation port over [`ColumnarRuntime`].
pub(crate) struct ServerColumnarProjectionPort {
    runtime: Arc<ColumnarRuntime>,
}

impl ServerColumnarProjectionPort {
    /// Shares one startup-fixed runtime with the apply worker.
    #[must_use]
    pub(crate) fn new(runtime: Arc<ColumnarRuntime>) -> Self {
        Self { runtime }
    }
}

impl ColumnarProjectionPort for ServerColumnarProjectionPort {
    fn observe(&self, projection_name: &str) -> Result<ColumnarObservation, ColumnarPortError> {
        let slot = self
            .runtime
            .engine(projection_name)
            .map_err(|_| ColumnarPortError::Unavailable)?
            .ok_or(ColumnarPortError::Integrity)?;
        let must_return_building = self.runtime.request_activation(&slot)?;
        // Head from storage without holding the engine lock.
        let head_position = self.runtime.read_application_head()?;
        let head = ProjectionFrontier::new(self.runtime.history_incarnation(), head_position);
        // Lock only long enough to clone Arc snapshot + frontiers + lifecycle.
        if must_return_building {
            return self.runtime.cold_observation(projection_name, &slot, head);
        }
        let observation = slot.capture_active(head.clone())?;
        // Engine lock dropped before return; callers query the Arc snapshot freely.
        match observation {
            Some(captured) => Ok(captured.observation),
            None => self.runtime.cold_observation(projection_name, &slot, head),
        }
    }

    fn definition(&self, projection_name: &str) -> Option<RegisteredDefinition> {
        self.runtime
            .engine(projection_name)
            .ok()
            .flatten()
            .map(|slot| slot.definition().clone())
    }

    fn notifier(&self) -> &ColumnarNotifier {
        self.runtime.notifier()
    }

    fn known_names(&self) -> Vec<String> {
        self.runtime.names().unwrap_or_default()
    }
}

impl VectorProjectionPort for ServerColumnarProjectionPort {
    fn execute(
        &self,
        request: VectorProjectionRequest,
    ) -> Result<VectorProjectionResult, VectorProjectionPortError> {
        let slot = self
            .runtime
            .engine(request.source_name())
            .map_err(map_vector_port_error)?
            .ok_or(VectorProjectionPortError::Integrity)?;
        let must_return_building = self
            .runtime
            .request_activation(&slot)
            .map_err(map_vector_port_error)?;
        if must_return_building {
            return Err(VectorProjectionPortError::Building);
        }
        let binding = self
            .runtime
            .control_binding(request.source_name())
            .ok_or(VectorProjectionPortError::Integrity)?;
        if !binding.is_vector {
            return Err(VectorProjectionPortError::Integrity);
        }
        let head_position = self
            .runtime
            .read_application_head()
            .map_err(map_vector_port_error)?;
        let head = ProjectionFrontier::new(self.runtime.history_incarnation(), head_position);
        let captured = slot
            .capture_active(head)
            .map_err(map_vector_port_error)?
            .ok_or(VectorProjectionPortError::Building)?;
        let _installed_generation = captured
            .generation
            .ok_or(VectorProjectionPortError::Integrity)?;
        let observation = captured.observation;
        match observation.lifecycle() {
            Some(ColumnarLifecycle::Building) => return Err(VectorProjectionPortError::Building),
            Some(ColumnarLifecycle::Rebuilding { .. }) => {
                return Err(VectorProjectionPortError::Rebuilding);
            }
            Some(ColumnarLifecycle::Degraded { .. }) => {
                return Err(VectorProjectionPortError::Degraded);
            }
            Some(ColumnarLifecycle::Invalid { .. }) => {
                return Err(VectorProjectionPortError::Integrity);
            }
            Some(ColumnarLifecycle::Ready) | None => {}
        }
        if !observation.has_published()
            || observation.definition().entity_type_id() != request.entity()
            || !observation
                .definition()
                .projected_fields()
                .contains(&request.field())
        {
            return Err(VectorProjectionPortError::Integrity);
        }
        let FrontierPosition::AppliedThrough(epoch) = observation.published_frontier().position()
        else {
            return Err(VectorProjectionPortError::Building);
        };
        if request
            .minimum_epoch()
            .is_some_and(|minimum| epoch < minimum)
        {
            return Err(VectorProjectionPortError::FreshnessUnsatisfied);
        }
        if let Some(max_lag_ms) = request.max_lag_ms() {
            let FrontierPosition::AppliedThrough(head) = observation.head().position() else {
                return Err(VectorProjectionPortError::FreshnessUnsatisfied);
            };
            if trusted_commit_lag_ms(self.runtime.storage(), epoch, head)? > max_lag_ms {
                return Err(VectorProjectionPortError::FreshnessUnsatisfied);
            }
        }
        let target = riffdb_query_executor::VectorInspectionTargetV1::new(
            request.lineage().clone(),
            request.partition().clone(),
            request.entity(),
            request.field(),
            None,
            std::num::NonZeroU16::new(500).expect("fixed inspection bound is nonzero"),
        );
        let evidence = riffdb_query_executor::QueryExecutionPort::inspect_vector_evidence(
            &self.runtime.storage().query_executor(),
            &target,
            request.row_policy().map(Arc::as_ref),
        )
        .map_err(map_vector_execution_error)?;
        if !evidence.exact_end()
            || evidence.frontier() != Some(epoch)
            || evidence.stale_entities() > request.stale_entity_threshold()
        {
            return Err(VectorProjectionPortError::FreshnessUnsatisfied);
        }
        let candidates = evidence
            .candidates()
            .iter()
            .map(|candidate| candidate.entity_key().clone())
            .collect::<BTreeSet<_>>();
        if let Some(admission) = evidence.admission()
            && !admission.covers(request.entity(), &candidates)
        {
            return Err(VectorProjectionPortError::Integrity);
        }
        let current = evidence
            .candidates()
            .iter()
            .filter(|candidate| {
                candidate
                    .embedding_write()
                    .is_some_and(|(_, metadata)| metadata == request.current_model())
            })
            .map(|candidate| candidate.entity_key().clone())
            .collect::<BTreeSet<_>>();
        let mut admission = ProductionVectorAdmission {
            entity: request.entity(),
            key_schema: observation.definition().primary_key_schema(),
            current,
            policy: evidence.admission(),
        };
        let nearest_request = NearestQueryRequest {
            org_scope: request.partition_value().clone(),
            vector_field: request.field(),
            query_vector: request.query_vector().clone(),
            k: request.k(),
            metric: request.metric(),
            predicates: request.predicates().to_vec(),
            budget: QueryBudget {
                max_scanned_rows: 500,
                max_group_cardinality: 1,
            },
        };
        let nearest = riffdb_columnar::nearest_query_snapshot_with_admission(
            observation.definition(),
            observation.snapshot().as_ref(),
            &nearest_request,
            &mut admission,
        )
        .map_err(map_vector_nearest_error)?;
        Ok(VectorProjectionResult::new(
            nearest,
            observation.published_frontier().clone(),
            observation.head().clone(),
        ))
    }
}

fn trusted_commit_lag_ms(
    storage: &SharedRedbOperationalPorts,
    frontier: CommitSequence,
    head: CommitSequence,
) -> Result<u64, VectorProjectionPortError> {
    if head < frontier {
        return Err(VectorProjectionPortError::Integrity);
    }
    if head == frontier {
        return Ok(0);
    }
    let frontier_time = AuthoritativePointReader::read_commit(storage, frontier)
        .map_err(|_| VectorProjectionPortError::Unavailable)?
        .ok_or(VectorProjectionPortError::Integrity)?
        .logical_time()
        .timestamp();
    let head_time = AuthoritativePointReader::read_commit(storage, head)
        .map_err(|_| VectorProjectionPortError::Unavailable)?
        .ok_or(VectorProjectionPortError::Integrity)?
        .logical_time()
        .timestamp();
    if head_time < frontier_time {
        return Err(VectorProjectionPortError::Integrity);
    }
    trusted_timestamp_lag_ms(frontier_time, head_time)
}

fn trusted_timestamp_lag_ms(
    frontier_time: riffdb_types::Timestamp,
    head_time: riffdb_types::Timestamp,
) -> Result<u64, VectorProjectionPortError> {
    let to_nanos = |timestamp: riffdb_types::Timestamp| {
        i128::from(timestamp.seconds())
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_add(i128::from(timestamp.nanoseconds())))
    };
    let total_nanos = to_nanos(head_time)
        .and_then(|head| to_nanos(frontier_time).and_then(|frontier| head.checked_sub(frontier)))
        .and_then(|value| u64::try_from(value).ok())
        .ok_or(VectorProjectionPortError::Integrity)?;
    Ok(total_nanos.div_ceil(1_000_000))
}

struct ProductionVectorAdmission<'a> {
    entity: riffdb_types::EntityTypeId,
    key_schema: &'a riffdb_contract_ir::KeySchema,
    current: BTreeSet<EntityKey>,
    policy: Option<&'a riffdb_policy::AuthorizedProjectedRowAdmissionV1>,
}

impl NearestCandidateAdmission for ProductionVectorAdmission<'_> {
    type Error = ();

    fn admit(&mut self, candidate: NearestCandidate<'_>) -> Result<bool, Self::Error> {
        if candidate.entity_type_id() != self.entity {
            return Err(());
        }
        let key = self
            .key_schema
            .encode_entity(candidate.primary_key())
            .map_err(|_| ())?;
        Ok(self.current.contains(&key) && self.policy.is_none_or(|policy| policy.admits(&key)))
    }
}

const fn map_vector_port_error(error: ColumnarPortError) -> VectorProjectionPortError {
    match error {
        ColumnarPortError::Unavailable => VectorProjectionPortError::Unavailable,
        ColumnarPortError::Integrity => VectorProjectionPortError::Integrity,
    }
}

fn map_vector_execution_error(
    error: riffdb_query_executor::QueryExecutionError,
) -> VectorProjectionPortError {
    match error {
        riffdb_query_executor::QueryExecutionError::BackendUnavailable => {
            VectorProjectionPortError::Unavailable
        }
        riffdb_query_executor::QueryExecutionError::BackendLimitExceeded
        | riffdb_query_executor::QueryExecutionError::BoundExceeded
        | riffdb_query_executor::QueryExecutionError::FuelExhausted => {
            VectorProjectionPortError::Unavailable
        }
        _ => VectorProjectionPortError::Integrity,
    }
}

fn map_vector_nearest_error(error: NearestQueryAdmissionError<()>) -> VectorProjectionPortError {
    match error {
        NearestQueryAdmissionError::Admission(()) => VectorProjectionPortError::Integrity,
        NearestQueryAdmissionError::Query(
            QueryError::ScanBudgetExceeded { .. } | QueryError::GroupCardinalityExceeded { .. },
        ) => VectorProjectionPortError::Unavailable,
        NearestQueryAdmissionError::Query(_) => VectorProjectionPortError::Integrity,
    }
}

impl fmt::Debug for ServerColumnarProjectionPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerColumnarProjectionPort([PUBLISHED_SNAPSHOT])")
    }
}

/// Closed startup failure while resolving or opening one columnar projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ColumnarRegistrationError {
    projection_name: String,
    kind: ColumnarRegistrationErrorKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ColumnarRegistrationErrorKind {
    NoActiveCatalog,
    DuplicateName,
    UnknownEntity,
    UnknownField { field_name: String },
    Definition,
    Storage,
    Synchronization,
}

impl ColumnarRegistrationError {
    fn no_active_catalog(projection_name: impl Into<String>) -> Self {
        Self {
            projection_name: projection_name.into(),
            kind: ColumnarRegistrationErrorKind::NoActiveCatalog,
        }
    }

    fn duplicate_name(projection_name: impl Into<String>) -> Self {
        Self {
            projection_name: projection_name.into(),
            kind: ColumnarRegistrationErrorKind::DuplicateName,
        }
    }

    fn unknown_entity(projection_name: impl Into<String>) -> Self {
        Self {
            projection_name: projection_name.into(),
            kind: ColumnarRegistrationErrorKind::UnknownEntity,
        }
    }

    fn unknown_field(projection_name: impl Into<String>, field_name: impl Into<String>) -> Self {
        Self {
            projection_name: projection_name.into(),
            kind: ColumnarRegistrationErrorKind::UnknownField {
                field_name: field_name.into(),
            },
        }
    }

    fn definition(projection_name: impl Into<String>) -> Self {
        Self {
            projection_name: projection_name.into(),
            kind: ColumnarRegistrationErrorKind::Definition,
        }
    }

    fn storage(error: riffdb_catalog::CatalogError, projection_name: impl Into<String>) -> Self {
        let _ = error;
        Self {
            projection_name: projection_name.into(),
            kind: ColumnarRegistrationErrorKind::Storage,
        }
    }

    fn control_storage(error: StorageError, projection_name: impl Into<String>) -> Self {
        let _ = error;
        Self {
            projection_name: projection_name.into(),
            kind: ColumnarRegistrationErrorKind::Storage,
        }
    }

    fn synchronization() -> Self {
        Self {
            projection_name: "production-vector".to_owned(),
            kind: ColumnarRegistrationErrorKind::Synchronization,
        }
    }

    /// Projection name that failed registration (config-visible identity).
    #[must_use]
    pub(crate) fn projection_name(&self) -> &str {
        &self.projection_name
    }
}

impl fmt::Display for ColumnarRegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ColumnarRegistrationErrorKind::NoActiveCatalog => write!(
                formatter,
                "columnar projection '{}' requires an active contract catalog",
                self.projection_name()
            ),
            ColumnarRegistrationErrorKind::DuplicateName => write!(
                formatter,
                "columnar projection '{}' is configured more than once",
                self.projection_name()
            ),
            ColumnarRegistrationErrorKind::UnknownEntity => write!(
                formatter,
                "columnar projection '{}' references an unknown entity",
                self.projection_name()
            ),
            ColumnarRegistrationErrorKind::UnknownField { field_name } => write!(
                formatter,
                "columnar projection '{}' references unknown field '{field_name}'",
                self.projection_name()
            ),
            ColumnarRegistrationErrorKind::Definition => write!(
                formatter,
                "columnar projection '{}' failed definition registration",
                self.projection_name()
            ),
            ColumnarRegistrationErrorKind::Storage => write!(
                formatter,
                "columnar projection '{}' could not read the active catalog",
                self.projection_name()
            ),
            ColumnarRegistrationErrorKind::Synchronization => {
                formatter.write_str("columnar projection registry could not be synchronized")
            }
        }
    }
}

impl std::error::Error for ColumnarRegistrationError {}

fn first_projection_name(projections: &[ConfiguredProjection]) -> String {
    projections
        .first()
        .map(|projection| projection.name().to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn resolve_configured_projection(
    configured: &ConfiguredProjection,
    bundle: &ContractBundle,
) -> Result<RegisteredDefinition, ColumnarRegistrationError> {
    let name = configured.name().to_owned();
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == configured.entity())
        .ok_or_else(|| ColumnarRegistrationError::unknown_entity(name.clone()))?;
    let mut projected_fields = Vec::with_capacity(configured.projected_fields().len());
    for field_name in configured.projected_fields() {
        projected_fields.push(resolve_field_id(entity, field_name).ok_or_else(|| {
            ColumnarRegistrationError::unknown_field(name.clone(), field_name.clone())
        })?);
    }
    let org_scope_field =
        resolve_field_id(entity, configured.org_scope_field()).ok_or_else(|| {
            ColumnarRegistrationError::unknown_field(name.clone(), configured.org_scope_field())
        })?;
    RegisteredDefinition::register(
        ColumnarProjectionDefinition {
            name: name.clone(),
            entity_name: configured.entity().to_owned(),
            projected_fields,
            org_scope_field,
        },
        bundle,
    )
    .map_err(|_| ColumnarRegistrationError::definition(name))
}

fn resolve_field_id(
    entity: &riffdb_contract_ir::EntitySchema,
    field_name: &str,
) -> Option<FieldId> {
    entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .map(|field| field.id())
}

/// Reads the current application head via a bounded empty-or-one-row commit scan.
pub(crate) fn read_application_head(
    storage: &(impl AuthoritativeScanReader + ?Sized),
) -> Result<FrontierPosition, ColumnarPortError> {
    read_application_head_after(storage, None)
}

/// Reads the current application head, resuming the probe after `observed`.
///
/// The returned frontier is independent of `observed`: `scan_commits` fixes the
/// page's `inclusive_upper` from the snapshot's application frontier before it
/// inspects any row. Passing the last observed head therefore returns the same
/// value while letting storage skip the commit range entirely once the probe
/// starts above the head.
pub(crate) fn read_application_head_after(
    storage: &(impl AuthoritativeScanReader + ?Sized),
    observed: Option<CommitSequence>,
) -> Result<FrontierPosition, ColumnarPortError> {
    let limit = StorageScanLimit::new(1).ok_or(ColumnarPortError::Integrity)?;
    let request = match observed {
        Some(after) => CommitScanRequest::initial_after(after, limit),
        None => CommitScanRequest::initial(limit),
    };
    let page = AuthoritativeScanReader::scan_commits(storage, request).map_err(map_port_storage)?;
    Ok(page.inclusive_upper())
}

fn map_outcome_lifecycle(outcome: &ColumnarOutcome) -> (bool, Option<ColumnarLifecycle>) {
    match outcome {
        ColumnarOutcome::Building(_) => (false, Some(ColumnarLifecycle::Building)),
        ColumnarOutcome::Ready(_) => (true, Some(ColumnarLifecycle::Ready)),
        ColumnarOutcome::Invalid(invalid) => (
            false,
            Some(ColumnarLifecycle::Invalid {
                expected_fingerprint: invalid.expected_fingerprint,
                found_fingerprint: invalid.found_fingerprint,
            }),
        ),
        ColumnarOutcome::Lagging(_) => (true, Some(ColumnarLifecycle::Ready)),
        ColumnarOutcome::Rebuilding(rebuilding) => (
            true,
            Some(ColumnarLifecycle::Rebuilding {
                reason: rebuilding.reason,
                progress_applied: rebuilding.progress_applied,
                progress_total: rebuilding.progress_total,
            }),
        ),
        ColumnarOutcome::Degraded(degraded) => (
            true,
            Some(ColumnarLifecycle::Degraded {
                reason: degraded.reason,
            }),
        ),
    }
}

const fn map_port_storage(error: StorageError) -> ColumnarPortError {
    match error.kind() {
        StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown => {
            ColumnarPortError::Unavailable
        }
        StorageErrorKind::CorruptData
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::LimitExceeded
        | StorageErrorKind::InvariantViolation
        | StorageErrorKind::SequenceExhausted
        | StorageErrorKind::HistoryPruned => ColumnarPortError::Integrity,
    }
}

/// Maps engine checkpoint failures that must not degrade the worker.
#[must_use]
#[cfg(test)]
pub(crate) fn is_holdback_active(error: &ColumnarError) -> bool {
    matches!(
        error,
        ColumnarError::Checkpoint(riffdb_columnar::CheckpointError::HoldbackActive { .. })
    )
}

#[cfg(test)]
mod tests {
    use std::hint::black_box;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    use riffdb_catalog::{
        ActiveCatalogSnapshot as CatalogSnapshotReadback, CatalogHistoryOutcome,
        ValidatedContractBundle, validate_catalog_history,
    };
    use riffdb_columnar::{
        AggregateOp, AggregateValue, ColumnarQueryRequest, ColumnarSnapshot, ColumnarTestBoundary,
        ColumnarTestController, LiveRow, OrgKey, PreparedColumnarGenerationRepository,
        PrimaryKeyBytes, QueryBudget, QueryResult, query_snapshot,
    };
    use riffdb_storage_api::{
        AffectedEntityV1, AuditPrincipalV1, CatalogActivationIntentV1, CatalogActivationResult,
        CatalogAdministrationRepository, ColumnarProjectionArtifactV1, CommittedEntityReferenceV2,
        DatabaseInitializationPort, DatabaseInitializationResult, DeclaredOutcome, DurabilityMode,
        DurableKeySchemaBindingV1, EvidencePageLimit, ExecutablePlanRef, ExpectedEntityState,
        IdempotencyIdentity, IdempotencyKeyDigest, ReadDependencies, ReadDependency,
        ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
        StartupValidationInputs, StoredAdmittedProvenanceClaimsV1,
        StoredColumnarProjectionControlV1, StoredColumnarProjectionGenerationV1,
        StoredCommitRecordV1, StoredEntityRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1,
        StoredReadDependenciesV1, StructuralEvidenceCursor, StructuralEvidenceOpen,
        StructuralEvidencePage, StructuralEvidenceSession, StructuralOpenOutcome,
    };
    use riffdb_storage_redb::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CanonicalValue, CanonicalVector, CapabilityId, CommitSequence,
        ContractLineage, DatabaseId, DigestKeyId, DistanceMetric, EmbeddingMetadata,
        EntityKeyBuilder, EntityVersion, Environment, LogicalTime, OutcomeId, PartitionKeyBuilder,
        PartitionKeyHash, ProvenanceId, RequestId, TenantScope, Timestamp, hash_partition_key,
    };

    use super::*;
    use crate::config::ConfiguredProjection;
    use riffdb_storage_api::{
        ColumnarProjectionControlWriteResultV1, ColumnarProjectionLifecycleV1,
    };

    const ADAPTER_BOARD_CONTRACT: &str = r#"
contract AdapterBoard version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: u64
    field title: string<64>
  }

  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }

  command CreateTicket {
    input idempotency_key: string<128>
    input organization_id: uuid
    input ticket_id: uuid
    input status: u64
    input title: string<64>
    idempotency_key idempotency_key
    create Ticket(organization_id, ticket_id) as ticket
      else TicketExists { ticket_id: ticket_id }
    set ticket.status = status
    set ticket.title = title
    return Created { ticket: ticket }
  }
}
"#;

    const PRODUCTION_VECTOR_CONTRACT: &str = r#"
contract VectorBoard version 1 {
  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field title: string<64>
    vector_field embedding(4, cosine, (title), staleness_slo 60, model "embed-v1", current_version "2026-08-21", replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000)
  }

  aggregate Documents {
    root Document
    partition_by organization_id
    conflict_key (organization_id, document_id)
  }
}
"#;

    /// Whole-directory scope for one adapter test: database, its journal
    /// side files, and the projections root all live inside it and are
    /// removed on `Drop` — pass, fail, or panic.
    fn adapter_scope(label: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("riffdb-columnar-adapter-{label}-"))
            .tempdir()
            .expect("create adapter test scope directory")
    }

    fn uuid_bytes(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn open_operational(store: RedbStore) -> RedbOperationalPorts {
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
        let inputs = StartupValidationInputs::new(
            Timestamp::new(1_700_200_000, 0).expect("timestamp"),
            ReadableCapabilityDigestInventory::new(vec![key]).expect("capability digests"),
            ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency digests"),
        );
        let mut session = store
            .begin_structural_evidence(inputs)
            .expect("begin structural validation");
        let database_id = session.database_id();
        let open_session_id = session.open_session_id();
        let limit = EvidencePageLimit::new(64).expect("evidence page limit");
        let mut cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
        let structural_end = loop {
            match session
                .read_structural_evidence(cursor, limit)
                .expect("read structural evidence")
            {
                StructuralEvidencePage::Page { findings, next, .. } => {
                    assert!(
                        findings.is_empty(),
                        "fresh adapter store has no findings: {findings:?}"
                    );
                    cursor = next;
                }
                StructuralEvidencePage::ExactEnd(end) => break end,
            }
        };
        let (history, historical_end) = validate_catalog_history(&mut session)
            .expect("validate empty catalog history")
            .into_parts();
        let opened = session
            .finish(structural_end, historical_end)
            .expect("finish structural validation");
        let CatalogHistoryOutcome::Ready(history) = history else {
            panic!("fresh adapter store cannot require migration");
        };
        let StructuralOpenOutcome::Clean(opened) = opened else {
            panic!("fresh adapter store cannot expose a migration port");
        };
        assert!(history.matches(opened.database_id(), opened.open_session_id()));
        let (_, _, _, dormant): (_, _, _, RedbDormantPorts) = opened.into_parts();
        dormant
            .into_operational_after_catalog_validation()
            .expect("activate checked adapter ports")
    }

    fn board_runtime(label: &str) -> (Arc<ColumnarRuntime>, tempfile::TempDir) {
        let scope = adapter_scope(label);
        let database_path = scope.path().join("db.redb");
        let projections_root = scope.path().join("projections");
        std::fs::create_dir_all(&projections_root).expect("create projections root");
        let mut store = RedbStore::open(&database_path).expect("create adapter database");
        let database_id = DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database id");
        assert_eq!(
            store
                .initialize_database(database_id)
                .expect("initialize adapter database"),
            DatabaseInitializationResult::Installed(database_id)
        );
        let checked = ValidatedContractBundle::from_compiler_bundle(
            riffdb_contract_compiler::compile_contract_source(ADAPTER_BOARD_CONTRACT)
                .expect("compile adapter board contract"),
        )
        .expect("validate adapter board contract");
        let mut ports = open_operational(store);
        let stored = checked.to_stored().expect("encode adapter bundle");
        let activation = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                stored,
                RequestId::from_bytes(uuid_bytes(0x21)).expect("request id"),
                AuditPrincipalV1::new(
                    ActorId::new("adapter-test").expect("actor"),
                    ActorKind::Human,
                    CapabilityId::from_bytes(uuid_bytes(0x31)).expect("capability"),
                    std::num::NonZeroU64::MIN,
                ),
                Timestamp::new(1_700_200_001, 0).expect("timestamp"),
                None,
            ))
            .expect("activate adapter board catalog");
        assert!(matches!(
            activation,
            CatalogActivationResult::Activated { .. }
        ));
        let storage =
            SharedRedbOperationalPorts::new(ports, None).expect("share adapter operational ports");
        // Sanity: the catalog readback the runtime performs must succeed.
        assert!(
            CatalogSnapshotReadback::read(&storage)
                .expect("read active adapter catalog")
                .is_some()
        );
        let projection = ConfiguredProjection::for_test(
            "ticket_board",
            "Ticket",
            &["status", "title"],
            "organization_id",
        );
        let runtime =
            ColumnarRuntime::open(storage, &[projection], &projections_root, 1, [0x5a; 16])
                .expect("open adapter columnar runtime");
        (runtime, scope)
    }

    fn empty_columnar_runtime(label: &str) -> (Arc<ColumnarRuntime>, tempfile::TempDir) {
        let scope = adapter_scope(label);
        let database_path = scope.path().join("db.redb");
        let projections_root = scope.path().join("projections");
        std::fs::create_dir_all(&projections_root).expect("create empty projections root");
        let mut store = RedbStore::open(&database_path).expect("create empty adapter database");
        let database_id = DatabaseId::from_bytes(uuid_bytes(0x14)).expect("database id");
        assert_eq!(
            store
                .initialize_database(database_id)
                .expect("initialize empty adapter database"),
            DatabaseInitializationResult::Installed(database_id)
        );
        let ports = open_operational(store);
        let storage =
            SharedRedbOperationalPorts::new(ports, None).expect("share empty operational ports");
        let runtime = ColumnarRuntime::open(storage, &[], &projections_root, 1, [0x5b; 16])
            .expect("open empty columnar runtime");
        (runtime, scope)
    }

    fn production_vector_runtime(label: &str) -> (Arc<ColumnarRuntime>, tempfile::TempDir) {
        let scope = adapter_scope(label);
        let database_path = scope.path().join("db.redb");
        let projections_root = scope.path().join("projections");
        std::fs::create_dir_all(&projections_root).expect("create projections root");
        let mut store = RedbStore::open(&database_path).expect("create adapter database");
        let database_id = DatabaseId::from_bytes(uuid_bytes(0x12)).expect("database id");
        assert_eq!(
            store
                .initialize_database(database_id)
                .expect("initialize adapter database"),
            DatabaseInitializationResult::Installed(database_id)
        );
        let checked = ValidatedContractBundle::from_compiler_bundle(
            riffdb_contract_compiler::compile_contract_source(PRODUCTION_VECTOR_CONTRACT)
                .expect("compile production vector contract"),
        )
        .expect("validate production vector contract");
        let mut ports = open_operational(store);
        let stored = checked.to_stored().expect("encode adapter bundle");
        let activation = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                stored,
                RequestId::from_bytes(uuid_bytes(0x22)).expect("request id"),
                AuditPrincipalV1::new(
                    ActorId::new("adapter-test").expect("actor"),
                    ActorKind::Human,
                    CapabilityId::from_bytes(uuid_bytes(0x32)).expect("capability"),
                    std::num::NonZeroU64::MIN,
                ),
                Timestamp::new(1_700_200_001, 0).expect("timestamp"),
                None,
            ))
            .expect("activate production vector catalog");
        assert!(matches!(
            activation,
            CatalogActivationResult::Activated { .. }
        ));
        let storage =
            SharedRedbOperationalPorts::new(ports, None).expect("share adapter operational ports");
        let runtime = ColumnarRuntime::open(storage, &[], &projections_root, 1, [0x5a; 16])
            .expect("open production vector runtime");
        (runtime, scope)
    }

    fn reopen_board_runtime(scope: &tempfile::TempDir) -> Arc<ColumnarRuntime> {
        open_board_runtime_at(scope.path())
    }

    fn open_board_runtime_at(scope: &Path) -> Arc<ColumnarRuntime> {
        open_board_runtime_at_incarnation(scope, 1).expect("reopen board columnar runtime")
    }

    fn open_board_runtime_at_incarnation(
        scope: &Path,
        history_incarnation: u64,
    ) -> Result<Arc<ColumnarRuntime>, ColumnarRegistrationError> {
        let store = RedbStore::open(scope.join("db.redb")).expect("reopen board database");
        let ports = open_operational(store);
        let storage =
            SharedRedbOperationalPorts::new(ports, None).expect("share reopened board ports");
        let projection = ConfiguredProjection::for_test(
            "ticket_board",
            "Ticket",
            &["status", "title"],
            "organization_id",
        );
        ColumnarRuntime::open(
            storage,
            &[projection],
            &scope.join("projections"),
            history_incarnation,
            [0x5a; 16],
        )
    }

    fn board_query() -> ColumnarQueryRequest {
        ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(uuid_bytes(0x41)),
            select: Vec::new(),
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            group_by: None,
            aggregate: None,
            budget: QueryBudget::default(),
        }
    }

    fn append_board_ticket_for_worker(runtime: &ColumnarRuntime) {
        append_board_ticket_for_worker_at(runtime, CommitSequence::first(), 0x42);
    }

    fn append_board_ticket_for_worker_at(
        runtime: &ColumnarRuntime,
        sequence: CommitSequence,
        ticket_seed: u8,
    ) {
        let bundle = riffdb_contract_compiler::compile_contract_source(ADAPTER_BOARD_CONTRACT)
            .expect("compile board contract");
        let entity = bundle
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Ticket")
            .expect("Ticket entity");
        let field = |name: &str| {
            entity
                .record()
                .fields()
                .iter()
                .find(|field| field.name() == name)
                .expect("Ticket field")
                .id()
        };
        let command = bundle.commands().first().expect("CreateTicket command");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            command.command_id(),
            command.plan_hash(),
        );
        let organization = uuid_bytes(0x41);
        let ticket = uuid_bytes(ticket_seed);
        let mut key = EntityKeyBuilder::new(entity.id());
        key.push_uuid(&organization).expect("organization key");
        key.push_uuid(&ticket).expect("ticket key");
        let target = EntityTarget::new(entity.id(), key.finish().expect("entity key"))
            .expect("entity target");
        let row = StoredEntityRecordV1::new(
            target.clone(),
            EntityVersion::first(),
            bundle.contract_version(),
            DurableKeySchemaBindingV1::from_plan(&plan),
            CanonicalRecord::new(vec![
                (field("organization_id"), CanonicalValue::Uuid(organization)),
                (field("ticket_id"), CanonicalValue::Uuid(ticket)),
                (field("status"), CanonicalValue::U64(1)),
                (
                    field("title"),
                    CanonicalValue::string("one").expect("title"),
                ),
            ])
            .expect("canonical row"),
        )
        .expect("stored row");
        let dependencies = StoredReadDependenciesV1::from_live(
            &ReadDependencies::new(vec![ReadDependency::EntityObservation {
                target,
                expected: ExpectedEntityState::Absent,
            }])
            .expect("read dependencies"),
        )
        .expect("stored dependencies");
        let actor = AdmittedActorContext::new(
            ActorId::new("columnar-worker-test").expect("actor"),
            ActorKind::Human,
            TenantScope::Global,
            None,
        );
        let logical_time =
            LogicalTime::new(Timestamp::new(1_700_200_002, 0).expect("logical time"));
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition
            .push_uuid(&organization)
            .expect("partition organization");
        let partition_key = partition.finish().expect("partition key");
        let partition_hash = hash_partition_key(partition_key.as_bytes());
        let declared_outcome = DeclaredOutcome::new(
            OutcomeId::first(),
            CanonicalRecord::new(Vec::new()).expect("outcome record"),
        )
        .expect("outcome");
        let provenance_id = ProvenanceId::from_bytes(uuid_bytes(ticket_seed.wrapping_add(0x12)))
            .expect("provenance");
        let request_id =
            RequestId::from_bytes(uuid_bytes(ticket_seed.wrapping_add(0x0f))).expect("request id");
        let canonical_input_hash =
            CanonicalInputHash::from_bytes([ticket_seed.wrapping_add(0x10); 32]);
        let commit = StoredCommitRecordV1::new(
            sequence,
            request_id,
            plan.clone(),
            canonical_input_hash,
            actor.clone(),
            logical_time,
            partition_hash,
            Vec::new(),
            dependencies,
            vec![CommittedEntityReferenceV2::from_post_image(&row).expect("row reference")],
            Vec::new(),
            declared_outcome.clone(),
            provenance_id,
            Vec::new(),
            DurabilityMode::Sync,
        )
        .expect("stored commit");
        let identity = IdempotencyIdentity::new(
            DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database id"),
            Environment::new("test").expect("environment"),
            TenantScope::Global,
            actor.principal_id().clone(),
            plan.contract_lineage().clone(),
            plan.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key"),
                [ticket_seed; 32],
            ),
        );
        let outcome = StoredOutcomeV1::new(
            identity.clone(),
            sequence,
            request_id,
            plan.clone(),
            canonical_input_hash,
            actor.clone(),
            logical_time,
            partition_key,
            partition_hash,
            Vec::new(),
            declared_outcome.clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
            provenance_id,
            DurabilityMode::Sync,
        )
        .expect("stored outcome");
        let provenance = StoredProvenanceRecordV1::new(
            provenance_id,
            sequence,
            identity,
            request_id,
            plan,
            canonical_input_hash,
            actor,
            logical_time,
            partition_hash,
            Vec::new(),
            declared_outcome.outcome_id(),
            vec![AffectedEntityV1::from_record(&row)],
            Vec::new(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("stored provenance");
        runtime
            .storage()
            .append_columnar_worker_commit_with_crosslinks_fixture(
                &[row],
                &[commit],
                &[outcome],
                &[provenance],
            )
            .expect("append worker commit fixture");
    }

    fn sequence_uuid(domain: u8, sequence: u64) -> [u8; 16] {
        let mut value = [0_u8; 16];
        value[0] = domain;
        value[8..].copy_from_slice(&sequence.to_be_bytes());
        value[6] = 0x70 | (domain & 0x0f);
        value[8] = 0x80 | (value[8] & 0x3f);
        value
    }

    fn columnar_receipt_rows_and_commits(
        rows_per_partition: usize,
    ) -> (Vec<StoredEntityRecordV1>, Vec<StoredCommitRecordV1>) {
        let bundle = riffdb_contract_compiler::compile_contract_source(ADAPTER_BOARD_CONTRACT)
            .expect("compile receipt board contract");
        let entity = bundle
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Ticket")
            .expect("Ticket entity");
        let field = |name: &str| {
            entity
                .record()
                .fields()
                .iter()
                .find(|field| field.name() == name)
                .expect("Ticket field")
                .id()
        };
        let command = bundle.commands().first().expect("CreateTicket command");
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            command.command_id(),
            command.plan_hash(),
        );
        let total = rows_per_partition.checked_mul(2).expect("bounded corpus");
        let mut rows = Vec::with_capacity(total);
        let mut commits = Vec::with_capacity(total);
        for index in 0..total {
            let partition = index / rows_per_partition;
            let partition_row = index % rows_per_partition;
            let organization = uuid_bytes(if partition == 0 { 0x63 } else { 0x64 });
            let ticket = sequence_uuid(
                0x70 + u8::try_from(partition).expect("partition"),
                u64::try_from(partition_row + 1).expect("ticket"),
            );
            let mut key = EntityKeyBuilder::new(entity.id());
            key.push_uuid(&organization).expect("organization key");
            key.push_uuid(&ticket).expect("ticket key");
            let target = EntityTarget::new(entity.id(), key.finish().expect("entity key"))
                .expect("entity target");
            let row = StoredEntityRecordV1::new(
                target.clone(),
                EntityVersion::first(),
                bundle.contract_version(),
                DurableKeySchemaBindingV1::from_plan(&plan),
                CanonicalRecord::new(vec![
                    (field("organization_id"), CanonicalValue::Uuid(organization)),
                    (field("ticket_id"), CanonicalValue::Uuid(ticket)),
                    (
                        field("status"),
                        CanonicalValue::U64(u64::try_from(partition_row % 4).expect("status")),
                    ),
                    (
                        field("title"),
                        CanonicalValue::string(match partition_row % 4 {
                            0 => "open",
                            1 => "closed",
                            2 => "queued",
                            _ => "running",
                        })
                        .expect("title"),
                    ),
                ])
                .expect("canonical row"),
            )
            .expect("stored row");
            let dependencies = StoredReadDependenciesV1::from_live(
                &ReadDependencies::new(vec![ReadDependency::EntityObservation {
                    target,
                    expected: ExpectedEntityState::Absent,
                }])
                .expect("read dependencies"),
            )
            .expect("stored dependencies");
            let sequence = CommitSequence::new(u64::try_from(index + 1).expect("sequence"))
                .expect("commit sequence");
            let commit = StoredCommitRecordV1::new(
                sequence,
                RequestId::from_bytes(sequence_uuid(0x80, sequence.get())).expect("request id"),
                plan.clone(),
                CanonicalInputHash::from_bytes([0x52; 32]),
                AdmittedActorContext::new(
                    ActorId::new("columnar-receipt").expect("actor"),
                    ActorKind::Human,
                    TenantScope::Global,
                    None,
                ),
                LogicalTime::new(Timestamp::new(1_700_200_002, 0).expect("logical time")),
                PartitionKeyHash::from_bytes(
                    [0x53 + u8::try_from(partition).expect("partition"); 32],
                ),
                Vec::new(),
                dependencies,
                vec![CommittedEntityReferenceV2::from_post_image(&row).expect("row reference")],
                Vec::new(),
                DeclaredOutcome::new(
                    OutcomeId::first(),
                    CanonicalRecord::new(Vec::new()).expect("outcome record"),
                )
                .expect("outcome"),
                ProvenanceId::from_bytes(sequence_uuid(0x90, sequence.get())).expect("provenance"),
                Vec::new(),
                DurabilityMode::Sync,
            )
            .expect("stored commit");
            rows.push(row);
            commits.push(commit);
        }
        (rows, commits)
    }

    fn request_projection(runtime: &ColumnarRuntime, name: &str) {
        let slot = runtime
            .engine(name)
            .expect("engine registry")
            .expect("known projection");
        runtime
            .request_activation(&slot)
            .expect("request projection activation");
    }

    struct PreparedV2Publication {
        binding: ColumnarControlBinding,
        expected: StoredColumnarProjectionControlV1,
        prepared: PreparedColumnarGenerationV1,
        successor: ColumnarEngine,
        successor_snapshot: Arc<ColumnarSnapshot>,
        organizations: [OrgKey; 2],
    }

    fn two_partition_snapshot() -> (ColumnarSnapshot, [OrgKey; 2]) {
        let organizations = [
            OrgKey::from_value(&CanonicalValue::Uuid(uuid_bytes(0x41))).expect("organization A"),
            OrgKey::from_value(&CanonicalValue::Uuid(uuid_bytes(0x42))).expect("organization B"),
        ];
        let mut snapshot = ColumnarSnapshot::empty();
        for (ordinal, organization) in organizations.iter().enumerate() {
            snapshot.delta.insert(
                organization.clone(),
                BTreeMap::from([(
                    PrimaryKeyBytes::from_entity_key_bytes(vec![ordinal as u8 + 1]),
                    LiveRow {
                        entity_version: EntityVersion::new(1).expect("entity version"),
                        cells: vec![
                            CanonicalValue::U64(ordinal as u64 + 1),
                            CanonicalValue::string(format!("partition-{ordinal}"))
                                .expect("bounded title"),
                        ],
                    },
                )]),
            );
        }
        (snapshot, organizations)
    }

    fn prepare_two_partition_v2_publication(
        runtime: &Arc<ColumnarRuntime>,
        scope: &tempfile::TempDir,
    ) -> PreparedV2Publication {
        prepare_two_partition_v2_publication_at_frontier(
            runtime,
            scope,
            FrontierPosition::BeforeFirst,
        )
    }

    fn prepare_two_partition_v2_publication_at_frontier(
        runtime: &Arc<ColumnarRuntime>,
        scope: &tempfile::TempDir,
        frontier: FrontierPosition,
    ) -> PreparedV2Publication {
        request_projection(runtime, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(runtime));
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let ready_v1 = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read selected V1")
            .expect("selected V1");
        let physical = PhysicalGenerationFingerprintV1::compute(binding.definition().fingerprint());
        assert_eq!(
            runtime
                .storage()
                .begin_v2_candidate(&ready_v1, *physical.as_bytes())
                .expect("begin V2 candidate"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let catching_up = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read catching-up control")
            .expect("catching-up control");
        let candidate = catching_up.candidate().expect("V2 candidate");
        let (mut snapshot, organizations) = two_partition_snapshot();
        snapshot.visible_frontier = frontier;
        let source_directory =
            controlled_source_directory(&scope.path().join("projections"), binding.spec().hash());
        let generation = ValidatedColumnarV2Generation::prepare(
            &source_directory,
            binding.definition().clone(),
            runtime.history_incarnation(),
            candidate.generation(),
            frontier,
            &snapshot,
        )
        .expect("prepare two-partition V2 root");
        let (length, checksum) = generation.artifact_identity();
        let pointer = StoredColumnarProjectionGenerationV1::prepared_candidate(
            candidate.generation(),
            ColumnarProjectionLayoutV1::V2,
            frontier,
            frontier,
            runtime.history_incarnation(),
            ColumnarProjectionArtifactV1::new(length, checksum).expect("root artifact"),
            binding.definition().fingerprint(),
            binding.spec().hash(),
            Some(*physical.as_bytes()),
        )
        .expect("prepared V2 pointer");
        let prepared = generation
            .prepared_generation(binding.spec(), pointer, runtime.process_generation())
            .expect("prepared V2 witness");
        let mismatched_process = runtime
            .storage()
            .record_durable_snapshot(&catching_up, &prepared, [0x99; 16])
            .expect_err("another process generation cannot consume the witness");
        assert_eq!(
            mismatched_process.kind(),
            StorageErrorKind::InvariantViolation
        );
        assert_eq!(
            runtime
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("reread after rejected process generation")
                .expect("control remains present"),
            catching_up,
            "process-generation refusal happens before the redb control transaction"
        );
        assert_eq!(
            runtime
                .storage()
                .record_durable_snapshot(&catching_up, &prepared, runtime.process_generation())
                .expect("record prepared V2 root"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let expected = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread prepared V2")
            .expect("prepared V2");
        let (successor, reopened_prepared) = runtime
            .open_prepared_generation(&binding, expected.candidate().expect("candidate"))
            .expect("open exact prepared V2");
        assert_eq!(reopened_prepared, prepared);
        let successor_snapshot = successor.published_snapshot();
        PreparedV2Publication {
            binding,
            expected,
            prepared,
            successor,
            successor_snapshot,
            organizations,
        }
    }

    fn controlled_directory(
        runtime: &ColumnarRuntime,
        scope: &tempfile::TempDir,
        name: &str,
    ) -> PathBuf {
        let binding = runtime.control_binding(name).expect("control binding");
        let control = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read control")
            .expect("control");
        let generation = control
            .servable_generation()
            .or_else(|| control.candidate())
            .expect("selected or candidate generation");
        controlled_generation_directory(
            &scope.path().join("projections"),
            binding.spec().hash(),
            generation.generation(),
        )
    }

    // req: PERF-019, PRJ-004, OQ-022, PERF-007, PERF-008
    #[test]
    fn configured_columnar_artifacts_remain_cold_through_readiness_and_clean_close() {
        let (runtime, scope) = board_runtime("cold-lifecycle");
        let directory = controlled_directory(&runtime, &scope, "ticket_board");

        assert_eq!(runtime.lifecycle_observation().cold_sources(), 1);
        assert_eq!(runtime.lifecycle_observation().activations(), 0);
        assert_eq!(runtime.lifecycle_observation().population_passes(), 0);
        assert!(
            !directory.exists(),
            "registration must not open the artifact"
        );

        let worker =
            crate::columnar_worker::RunningColumnarWorker::start(Arc::clone(&runtime), None)
                .expect("start cold worker");
        assert_eq!(
            worker.shutdown().expect("stop cold worker"),
            crate::columnar_worker::ColumnarWorkerShutdownObservation::BetweenPasses
        );
        assert_eq!(runtime.lifecycle_observation().activations(), 0);
        assert_eq!(runtime.lifecycle_observation().population_passes(), 0);
        assert!(!directory.exists(), "cold close must not open the artifact");
    }

    // req: PRJ-002, PRJ-004, PRJ-008, OQ-019, OQ-020, OQ-022, PERF-007, PERF-008
    #[test]
    fn first_columnar_demand_coalesces_one_activation_and_returns_building_without_rows() {
        const CALLERS: usize = 8;
        let (runtime, scope) = board_runtime("coalesced-activation");
        let directory = controlled_directory(&runtime, &scope, "ticket_board");
        let port = Arc::new(ServerColumnarProjectionPort::new(Arc::clone(&runtime)));
        let barrier = Arc::new(std::sync::Barrier::new(CALLERS));
        let mut callers = Vec::new();

        for _ in 0..CALLERS {
            let port = Arc::clone(&port);
            let barrier = Arc::clone(&barrier);
            callers.push(thread::spawn(move || {
                barrier.wait();
                let observation = port.observe("ticket_board").expect("cold observation");
                assert!(!observation.has_published());
                assert!(matches!(
                    observation.lifecycle(),
                    Some(ColumnarLifecycle::Building)
                ));
                assert!(observation.snapshot().segments.is_empty());
                assert!(observation.snapshot().delta.is_empty());
            }));
        }
        for caller in callers {
            caller.join().expect("join first-demand caller");
        }

        assert_eq!(runtime.lifecycle_observation().activations(), 1);
        assert_eq!(runtime.lifecycle_observation().population_passes(), 0);
        assert!(!directory.exists(), "requests never open the artifact");

        let activation_completion = runtime
            .notifier()
            .register("ticket_board".to_owned())
            .expect("register activation completion");
        let cancelled_wait = runtime
            .notifier()
            .register("ticket_board".to_owned())
            .expect("register cancelled request");
        let cancellation = runtime.notifier().cancellation();
        cancellation.cancel().expect("cancel triggering request");
        assert_eq!(
            cancelled_wait
                .wait_controlled(
                    std::time::Instant::now() + Duration::from_secs(1),
                    &cancellation
                )
                .expect("observe request cancellation"),
            riffdb_service::ColumnarWake::Cancelled
        );
        assert_eq!(runtime.lifecycle_observation().activations(), 1);

        let worker =
            crate::columnar_worker::RunningColumnarWorker::start(Arc::clone(&runtime), None)
                .expect("start activation owner");
        assert_eq!(
            activation_completion
                .wait(std::time::Instant::now() + Duration::from_secs(5))
                .expect("wait for shared activation"),
            riffdb_service::ColumnarWake::Notified
        );
        assert_eq!(
            runtime
                .engine("ticket_board")
                .expect("engine registry")
                .expect("activated projection")
                .lifecycle(),
            Ok(ColumnarSlotLifecycle::Active)
        );
        assert_eq!(runtime.lifecycle_observation().activations(), 1);
        assert!(directory.exists(), "the sole worker owns artifact opening");
        worker.shutdown().expect("stop activation owner");
    }

    // req: PRJ-002, PRJ-004, PRJ-008, PRJ-009, OQ-019, OQ-020, OQ-022, PERF-007, PERF-008
    #[test]
    fn columnar_activation_installs_only_the_validated_selected_immutable_view() {
        let (runtime, scope) = board_runtime("selected-view");
        request_projection(&runtime, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let selected_frontier = runtime
            .engine("ticket_board")
            .expect("engine registry")
            .expect("board slot")
            .with_engine_mut(|engine| {
                engine.checkpoint().expect("checkpoint selected view");
                engine.durable_frontier().position()
            })
            .expect("engine lock")
            .expect("active engine");
        drop(runtime);

        let reopened = reopen_board_runtime(&scope);
        let slot = reopened
            .engine("ticket_board")
            .expect("engine registry")
            .expect("cold board slot");
        assert_eq!(slot.lifecycle(), Ok(ColumnarSlotLifecycle::Cold));
        let port = ServerColumnarProjectionPort::new(Arc::clone(&reopened));
        let cold = port.observe("ticket_board").expect("cold observation");
        assert!(!cold.has_published());
        assert!(matches!(
            cold.lifecycle(),
            Some(ColumnarLifecycle::Building)
        ));
        assert_eq!(slot.lifecycle(), Ok(ColumnarSlotLifecycle::Activating));

        assert!(crate::columnar_worker::run_one_test_pass(&reopened));
        assert_eq!(slot.lifecycle(), Ok(ColumnarSlotLifecycle::Active));
        let ready = port.observe("ticket_board").expect("installed observation");
        assert!(ready.has_published());
        assert_eq!(ready.published_frontier().position(), selected_frontier);
    }

    // req: PRJ-002, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn columnar_v2_publication_requires_complete_root_and_transaction_current_head() {
        let (runtime, scope) = board_runtime("v2-root-publication");
        request_projection(&runtime, "ticket_board");

        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let ready_v1 = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read V1 control")
            .expect("V1 control");
        assert_eq!(
            ready_v1.published().expect("published V1").layout(),
            ColumnarProjectionLayoutV1::V1
        );

        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let catching_up = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read catching-up control")
            .expect("catching-up control");
        assert_eq!(
            catching_up.lifecycle(),
            riffdb_storage_api::ColumnarProjectionLifecycleV1::CatchingUp
        );
        assert!(
            catching_up
                .candidate()
                .expect("V2 candidate")
                .artifact()
                .is_none()
        );
        assert_eq!(catching_up.published(), ready_v1.published());

        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let prepared = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read prepared control")
            .expect("prepared control");
        let candidate = prepared.candidate().expect("prepared V2 candidate");
        assert_eq!(candidate.layout(), ColumnarProjectionLayoutV1::V2);
        assert!(candidate.artifact().is_some());
        assert_eq!(
            candidate.frontier(),
            runtime.read_application_head().expect("head")
        );
        assert_eq!(prepared.published(), ready_v1.published());

        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let selected = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read selected control")
            .expect("selected control");
        assert_eq!(
            selected.lifecycle(),
            riffdb_storage_api::ColumnarProjectionLifecycleV1::Ready
        );
        assert_eq!(
            selected.published().expect("published V2").layout(),
            ColumnarProjectionLayoutV1::V2
        );
        assert!(selected.candidate().is_none());

        drop(runtime);
        let reopened = reopen_board_runtime(&scope);
        request_projection(&reopened, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(&reopened));
        let slot = reopened
            .engine("ticket_board")
            .expect("engine registry")
            .expect("V2 slot");
        assert_eq!(
            slot.generation(),
            selected.published().map(|value| value.generation())
        );
        assert!(
            ServerColumnarProjectionPort::new(reopened)
                .observe("ticket_board")
                .expect("validated V2 observation")
                .has_published()
        );
    }

    // req: PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn prepared_initial_candidate_is_never_exposed_before_publication_cas() {
        let (runtime, scope) = board_runtime("prepared-initial-not-selected");
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let initial = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read initial control")
            .expect("initial control");
        assert!(initial.servable_generation().is_none());
        let candidate = initial.candidate().expect("initial candidate");
        let mut engine = runtime
            .open_controlled_generation(&binding, candidate)
            .expect("open unpublished candidate for worker construction");
        let manifest = engine.checkpoint().expect("prepare candidate manifest");
        let (length, checksum) = manifest.artifact_identity();
        let prepared_pointer = StoredColumnarProjectionGenerationV1::prepared_candidate(
            candidate.generation(),
            ColumnarProjectionLayoutV1::V1,
            FrontierPosition::BeforeFirst,
            manifest.durable_frontier,
            candidate.history_incarnation(),
            ColumnarProjectionArtifactV1::new(length, checksum).expect("manifest artifact"),
            candidate.definition_fingerprint(),
            candidate.spec_hash(),
            None,
        )
        .expect("prepared candidate pointer");
        let (prepared_engine, prepared) = runtime
            .open_prepared_generation(&binding, &prepared_pointer)
            .expect("open prepared candidate witness");
        drop(prepared_engine);
        assert_eq!(
            runtime
                .storage()
                .record_durable_snapshot(&initial, &prepared, runtime.process_generation())
                .expect("record prepared candidate"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        drop(runtime);

        let reopened = reopen_board_runtime(&scope);
        let port = ServerColumnarProjectionPort::new(Arc::clone(&reopened));
        let before = port.observe("ticket_board").expect("cold observation");
        assert!(!before.has_published());
        assert!(!crate::columnar_worker::activate_one_test_slot(
            &reopened,
            "ticket_board"
        ));
        let after_activation = port
            .observe("ticket_board")
            .expect("observation before publication CAS");
        assert!(
            !after_activation.has_published(),
            "opening a prepared Candidate cannot turn its manifest into request-visible authority"
        );
        let durable = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("durable reread")
            .expect("control remains present");
        assert!(durable.servable_generation().is_none());
        assert_eq!(durable.candidate(), Some(&prepared_pointer));
    }

    // req: PRJ-004, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn corrupt_prepared_candidates_record_failure_and_replace_across_process_reopen() {
        const CHILD_MODE: &str = "RIFFDB_SERVER_CORRUPT_CANDIDATE_CHILD";
        const CHILD_PATH: &str = "RIFFDB_SERVER_CORRUPT_CANDIDATE_PATH";
        const EXACT_TEST: &str = "columnar_adapter::tests::corrupt_prepared_candidates_record_failure_and_replace_across_process_reopen";

        if std::env::var_os(CHILD_MODE).is_some() {
            let path = PathBuf::from(std::env::var_os(CHILD_PATH).expect("child path"));
            let runtime = open_board_runtime_at(&path);
            request_projection(&runtime, "ticket_board");
            for _ in 0..5 {
                let _ = crate::columnar_worker::run_one_test_pass(&runtime);
            }
            panic!("candidate crash child did not abort at the requested boundary");
        }

        for layout in [
            ColumnarProjectionLayoutV1::V1,
            ColumnarProjectionLayoutV1::V2,
        ] {
            let label = match layout {
                ColumnarProjectionLayoutV1::V1 => "v1",
                ColumnarProjectionLayoutV1::V2 => "v2",
            };
            let (runtime, scope) = board_runtime(&format!("corrupt-prepared-candidate-{label}"));
            append_board_ticket_for_worker(&runtime);
            let binding = runtime
                .control_binding("ticket_board")
                .expect("control binding")
                .clone();
            if layout == ColumnarProjectionLayoutV1::V2 {
                request_projection(&runtime, "ticket_board");
                assert!(crate::columnar_worker::run_one_test_pass(&runtime));
                assert!(crate::columnar_worker::run_one_test_pass(&runtime));
            }
            drop(runtime);

            let status = std::process::Command::new(
                std::env::current_exe().expect("server test executable"),
            )
            .arg("--exact")
            .arg(EXACT_TEST)
            .arg("--nocapture")
            .env(CHILD_MODE, "prepare")
            .env(CHILD_PATH, scope.path())
            .env("RIFFDB_COLUMNAR_PUBLICATION_ABORT_AT", "before-cas")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("spawn candidate preparation child");
            assert_eq!(status.code(), None, "{label} preparation stops before CAS");

            let prepared_runtime = reopen_board_runtime(&scope);
            let prepared_control = prepared_runtime
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("read prepared candidate")
                .expect("prepared candidate control");
            let prepared = prepared_control
                .candidate()
                .expect("prepared candidate")
                .clone();
            assert_eq!(prepared.layout(), layout);
            let artifact = prepared.artifact().expect("prepared artifact");
            let candidate_path = match layout {
                ColumnarProjectionLayoutV1::V1 => controlled_generation_directory(
                    &scope.path().join("projections"),
                    binding.spec().hash(),
                    prepared.generation(),
                )
                .join(format!("MANIFEST-V1-{}", lower_hex(&artifact.checksum()))),
                ColumnarProjectionLayoutV1::V2 => controlled_source_directory(
                    &scope.path().join("projections"),
                    binding.spec().hash(),
                )
                .join(format!("generation-{:016x}", prepared.generation().get()))
                .join("ROOT-V1"),
            };
            drop(prepared_runtime);
            std::fs::write(&candidate_path, b"corrupt prepared candidate")
                .expect("corrupt prepared candidate artifact");

            let status = std::process::Command::new(
                std::env::current_exe().expect("server test executable"),
            )
            .arg("--exact")
            .arg(EXACT_TEST)
            .arg("--nocapture")
            .env(CHILD_MODE, "recover")
            .env(CHILD_PATH, scope.path())
            .env("RIFFDB_COLUMNAR_CANDIDATE_FAILURE_ABORT_AT", "after-record")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("spawn candidate failure-record child");
            assert_eq!(
                status.code(),
                None,
                "{label} failure record is crash durable"
            );

            let reopened = reopen_board_runtime(&scope);
            let failed = reopened
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("read failed candidate")
                .expect("failed candidate control");
            assert_eq!(failed.candidate(), Some(&prepared));
            assert_eq!(
                failed.failure().map(|failure| (
                    failure.target(),
                    failure.reason(),
                    failure.generation(),
                )),
                Some((
                    riffdb_storage_api::ColumnarProjectionFailureTargetV1::Candidate,
                    riffdb_storage_api::ColumnarProjectionFailureReasonV1::ArtifactInvalid,
                    Some(prepared.generation()),
                ))
            );
            request_projection(&reopened, "ticket_board");
            assert!(crate::columnar_worker::run_one_test_pass(&reopened));
            let replaced = reopened
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("read replacement candidate")
                .expect("replacement candidate control");
            let replacement = replaced.candidate().expect("replacement candidate");
            assert!(replacement.generation() > prepared.generation());
            assert_eq!(replacement.layout(), layout);
            assert!(replacement.artifact().is_none());
            let recovery_passes = match layout {
                ColumnarProjectionLayoutV1::V1 => 1,
                ColumnarProjectionLayoutV1::V2 => 2,
            };
            for _ in 0..recovery_passes {
                let _ = crate::columnar_worker::run_one_test_pass(&reopened);
            }
            let recovered = reopened
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("read recovered selection")
                .expect("recovered selection control");
            assert_eq!(
                recovered
                    .servable_generation()
                    .map(|selected| (selected.layout(), selected.generation())),
                Some((layout, replacement.generation()))
            );
            let rows = query_snapshot(
                binding.definition(),
                ServerColumnarProjectionPort::new(Arc::clone(&reopened))
                    .observe("ticket_board")
                    .expect("capture recovered candidate")
                    .snapshot()
                    .as_ref(),
                &board_query(),
            )
            .expect("query recovered data-bearing candidate");
            let QueryResult::Rows(rows) = rows else {
                panic!("board query returns rows");
            };
            assert_eq!(rows.rows.len(), 1, "{label} authoritative row survives");
        }
    }

    fn prepare_published_v1_frontier_advance(
        runtime: &Arc<ColumnarRuntime>,
    ) -> (
        ColumnarControlBinding,
        StoredColumnarProjectionControlV1,
        ColumnarEngine,
        PreparedColumnarGenerationV1,
    ) {
        request_projection(runtime, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(runtime));
        append_board_ticket_for_worker(runtime);
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let expected = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read selected V1")
            .expect("selected V1");
        let published = expected.published().expect("published V1");
        let mut successor = runtime
            .open_controlled_generation(&binding, published)
            .expect("open V1 successor");
        let _ =
            crate::columnar_worker::apply_available_for_worker_for_test(runtime, &mut successor)
                .expect("apply authoritative advancement");
        let manifest = successor.checkpoint().expect("checkpoint V1 successor");
        let (length, checksum) = manifest.artifact_identity();
        let replacement = StoredColumnarProjectionGenerationV1::selected(
            published.generation(),
            ColumnarProjectionLayoutV1::V1,
            manifest.durable_frontier,
            published.history_incarnation(),
            ColumnarProjectionArtifactV1::new(length, checksum).expect("manifest artifact"),
            published.definition_fingerprint(),
            published.spec_hash(),
            None,
            riffdb_storage_api::ColumnarProjectionGenerationRoleV1::Published,
        )
        .expect("replacement pointer");
        let (successor, prepared) = runtime
            .open_prepared_generation(&binding, &replacement)
            .expect("validate prepared V1 successor");
        (binding, expected, successor, prepared)
    }

    // req: PRJ-002, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022, PERF-007
    #[test]
    fn published_v1_frontier_cas_closes_capture_rereads_and_installs_before_ack() {
        use crate::columnar_worker::{
            ColumnarPublicationTestMode, advance_published_v1_under_capture_gate_for_test,
        };

        for (label, mode, advances) in [
            ("applied", ColumnarPublicationTestMode::Ordinary, true),
            (
                "state-changed-after-applied",
                ColumnarPublicationTestMode::StateChangedAfterApplied,
                true,
            ),
            (
                "storage-failure-before-commit",
                ColumnarPublicationTestMode::StorageFailureBeforeCommit,
                false,
            ),
            (
                "unknown-after-applied",
                ColumnarPublicationTestMode::UnknownAfterApplied,
                true,
            ),
            (
                "unknown-before-commit",
                ColumnarPublicationTestMode::UnknownBeforeCommit,
                false,
            ),
        ] {
            let (runtime, _scope) = board_runtime(&format!("v1-frontier-gate-{label}"));
            let (binding, expected, successor, prepared) =
                prepare_published_v1_frontier_advance(&runtime);
            let successor_snapshot = successor.published_snapshot();
            let port = ServerColumnarProjectionPort::new(Arc::clone(&runtime));
            let predecessor = port
                .observe("ticket_board")
                .expect("capture predecessor")
                .snapshot_arc();
            assert!(!Arc::ptr_eq(&predecessor, &successor_snapshot));
            let waiter = runtime
                .notifier()
                .register("ticket_board".to_owned())
                .expect("register acknowledgement");
            let slot = runtime
                .engine("ticket_board")
                .expect("engine registry")
                .expect("active slot");
            let (attempt_tx, attempt_rx) = mpsc::sync_channel(1);
            slot.set_capture_attempt_probe(Some(attempt_tx));
            let (gate_tx, gate_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let publisher_runtime = Arc::clone(&runtime);
            let publisher_binding = binding.clone();
            let publisher_expected = expected.clone();
            let publisher_prepared = prepared.clone();
            let publisher = thread::spawn(move || {
                advance_published_v1_under_capture_gate_for_test(
                    &publisher_runtime,
                    &publisher_binding,
                    &publisher_expected,
                    &publisher_prepared,
                    successor,
                    mode,
                    || {
                        gate_tx.send(()).expect("signal closed gate");
                        release_rx.recv().expect("release publication CAS");
                    },
                )
            });
            gate_rx.recv().expect("observe closed gate");
            let (captured_tx, captured_rx) = mpsc::channel();
            let capture_runtime = Arc::clone(&runtime);
            let capture = thread::spawn(move || {
                let snapshot = ServerColumnarProjectionPort::new(capture_runtime)
                    .observe("ticket_board")
                    .expect("capture after durable resolution")
                    .snapshot_arc();
                captured_tx
                    .send(Arc::clone(&snapshot))
                    .expect("send capture");
                snapshot
            });
            attempt_rx.recv().expect("capture reached held mutex");
            assert!(matches!(
                captured_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            slot.set_capture_attempt_probe(None);
            release_tx.send(()).expect("allow publication CAS");
            let result = publisher.join().expect("join publisher");
            let captured = capture.join().expect("join capture");
            let durable = runtime
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("durable reread")
                .expect("durable control");
            let wake = waiter.wait(Instant::now()).expect("ack observation");
            if advances {
                assert_eq!(result, Ok(true), "{label}");
                assert_eq!(
                    durable.published(),
                    Some(prepared.generation()),
                    "{label}: durable selection is the prepared frontier"
                );
                assert!(Arc::ptr_eq(&captured, &successor_snapshot), "{label}");
                assert_eq!(wake, riffdb_service::ColumnarWake::Notified, "{label}");
            } else {
                assert_eq!(result, Err(()), "{label}");
                assert_eq!(durable.published(), expected.published(), "{label}");
                assert!(!Arc::ptr_eq(&captured, &successor_snapshot), "{label}");
                assert_eq!(wake, riffdb_service::ColumnarWake::TimedOut, "{label}");
            }
        }
    }

    // req: PRJ-002, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn controlled_v1_prepare_is_side_effect_free_and_retires_after_capture_release() {
        use crate::columnar_worker::{
            ColumnarPublicationTestMode, advance_published_v1_under_capture_gate_for_test,
            apply_available_for_worker_for_test,
        };

        let (runtime, scope) = board_runtime("v1-artifact-retirement");
        request_projection(&runtime, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        append_board_ticket_for_worker_at(&runtime, CommitSequence::first(), 0x42);
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));

        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let expected = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read selected predecessor")
            .expect("selected predecessor");
        let published = expected.published().expect("published V1");
        let predecessor_artifact = published.artifact().expect("predecessor artifact");
        let generation_directory = controlled_generation_directory(
            &scope.path().join("projections"),
            binding.spec().hash(),
            published.generation(),
        );
        let predecessor_manifest = generation_directory.join(format!(
            "MANIFEST-V1-{}",
            lower_hex(&predecessor_artifact.checksum())
        ));
        let predecessor = riffdb_columnar::ManifestV1::decode(
            &std::fs::read(&predecessor_manifest).expect("read predecessor manifest"),
        )
        .expect("decode predecessor manifest");
        assert!(!predecessor.segments.is_empty());
        let predecessor_segments = predecessor
            .segments
            .iter()
            .map(|entry| generation_directory.join(&entry.file_name))
            .collect::<Vec<_>>();
        let captured_predecessor = ServerColumnarProjectionPort::new(Arc::clone(&runtime))
            .observe("ticket_board")
            .expect("capture predecessor")
            .snapshot_arc();

        append_board_ticket_for_worker_at(
            &runtime,
            CommitSequence::new(2).expect("second sequence"),
            0x43,
        );
        let mut successor = runtime
            .open_controlled_generation(&binding, published)
            .expect("open predecessor for advancement");
        apply_available_for_worker_for_test(&runtime, &mut successor).expect("apply second ticket");
        let successor_manifest = successor.checkpoint().expect("checkpoint successor V1");
        let (length, checksum) = successor_manifest.artifact_identity();
        let replacement = StoredColumnarProjectionGenerationV1::selected(
            published.generation(),
            ColumnarProjectionLayoutV1::V1,
            successor_manifest.durable_frontier,
            published.history_incarnation(),
            ColumnarProjectionArtifactV1::new(length, checksum).expect("successor artifact"),
            published.definition_fingerprint(),
            published.spec_hash(),
            None,
            riffdb_storage_api::ColumnarProjectionGenerationRoleV1::Published,
        )
        .expect("successor pointer");
        let (successor, prepared) = runtime
            .open_prepared_generation(&binding, &replacement)
            .expect("side-effect-free successor validation");

        assert!(predecessor_manifest.is_file());
        assert!(
            predecessor_segments.iter().all(|path| path.is_file()),
            "pre-CAS validation must not delete predecessor-selected segments"
        );
        assert_eq!(
            advance_published_v1_under_capture_gate_for_test(
                &runtime,
                &binding,
                &expected,
                &prepared,
                successor,
                ColumnarPublicationTestMode::Ordinary,
                || {},
            ),
            Ok(true)
        );
        let durable = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread successor selection")
            .expect("successor selection");
        runtime
            .reclaim_unselected_generations(&binding, &durable)
            .expect("retain captured predecessor artifact");
        assert!(predecessor_manifest.is_file());
        assert!(predecessor_segments.iter().all(|path| path.is_file()));

        drop(captured_predecessor);
        runtime
            .reclaim_unselected_generations(&binding, &durable)
            .expect("retire released predecessor artifact");
        assert!(!predecessor_manifest.exists());
        assert!(
            predecessor_segments.iter().all(|path| !path.exists()),
            "artifact-specific segment retirement follows durable selection and captured-view release"
        );
    }

    // req: PRJ-002, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn controlled_v1_reclaim_crash_reopens_the_exact_selected_artifact() {
        use crate::columnar_worker::{
            ColumnarPublicationTestMode, advance_published_v1_under_capture_gate_for_test,
        };
        use riffdb_columnar::{ColumnarTestBoundary, ColumnarTestController};

        const CHILD_MODE: &str = "RIFFDB_SERVER_V1_RECLAIM_CRASH_CHILD";
        const CHILD_PATH: &str = "RIFFDB_SERVER_V1_RECLAIM_CRASH_PATH";
        const EXACT_TEST: &str = "columnar_adapter::tests::controlled_v1_reclaim_crash_reopens_the_exact_selected_artifact";
        if std::env::var(CHILD_MODE).as_deref() == Ok("1") {
            let path = PathBuf::from(std::env::var_os(CHILD_PATH).expect("child path"));
            let runtime = open_board_runtime_at(&path);
            let binding = runtime
                .control_binding("ticket_board")
                .expect("child binding");
            let durable = runtime
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("child durable read")
                .expect("child durable control");
            let selected = durable.servable_generation().expect("child selected V1");
            let artifact = selected.artifact().expect("child selected artifact");
            let directory = controlled_generation_directory(
                &path.join("projections"),
                binding.spec().hash(),
                selected.generation(),
            );
            let controller = ColumnarTestController::new();
            controller.arm_abort_at(ColumnarTestBoundary::AfterV1ArtifactReclaim);
            ColumnarEngine::reclaim_controlled_v1_artifacts_with_test_controller(
                &directory,
                &[(artifact.length(), artifact.checksum())],
                &controller,
            )
            .expect("armed V1 reclamation must abort");
            panic!("child did not abort inside V1 artifact reclamation");
        }

        let (runtime, scope) = board_runtime("v1-reclaim-crash");
        let (binding, expected, successor, prepared) =
            prepare_published_v1_frontier_advance(&runtime);
        assert_eq!(
            advance_published_v1_under_capture_gate_for_test(
                &runtime,
                &binding,
                &expected,
                &prepared,
                successor,
                ColumnarPublicationTestMode::Ordinary,
                || {},
            ),
            Ok(true)
        );
        let expected_rows = query_snapshot(
            binding.definition(),
            ServerColumnarProjectionPort::new(Arc::clone(&runtime))
                .observe("ticket_board")
                .expect("capture selected successor")
                .snapshot()
                .as_ref(),
            &board_query(),
        )
        .expect("query selected successor");
        drop(runtime);

        let status =
            std::process::Command::new(std::env::current_exe().expect("server test executable"))
                .arg("--exact")
                .arg(EXACT_TEST)
                .arg("--nocapture")
                .env(CHILD_MODE, "1")
                .env(CHILD_PATH, scope.path())
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .expect("spawn V1 reclamation crash child");
        assert_eq!(
            status.code(),
            None,
            "child aborts after one retired V1 deletion"
        );

        let reopened = reopen_board_runtime(&scope);
        let reopened_port = ServerColumnarProjectionPort::new(Arc::clone(&reopened));
        let cold = reopened_port
            .observe("ticket_board")
            .expect("request selected successor reopen after reclaim crash");
        assert!(!cold.has_published());
        assert!(crate::columnar_worker::activate_one_test_slot(
            &reopened,
            "ticket_board"
        ));
        let actual_rows = query_snapshot(
            binding.definition(),
            reopened_port
                .observe("ticket_board")
                .expect("capture selected successor after reclaim crash")
                .snapshot()
                .as_ref(),
            &board_query(),
        )
        .expect("query selected successor after reclaim crash");
        assert_eq!(actual_rows, expected_rows);
        let durable = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread selected successor")
            .expect("selected successor control");
        reopened
            .reclaim_unselected_generations(&binding, &durable)
            .expect("retry artifact reclamation after crash");
    }

    // req: PRJ-002, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn published_v1_state_change_installs_the_later_durable_v1_selection() {
        use crate::columnar_worker::{
            ColumnarPublicationTestMode, advance_published_v1_under_capture_gate_for_test,
            apply_available_for_worker_for_test,
        };

        let (runtime, _scope) = board_runtime("v1-later-durable-selection");
        let (binding, expected, attempted_successor, attempted_prepared) =
            prepare_published_v1_frontier_advance(&runtime);
        append_board_ticket_for_worker_at(
            &runtime,
            CommitSequence::new(2).expect("second sequence"),
            0x43,
        );
        let published = expected.published().expect("selected predecessor");
        let mut later_successor = runtime
            .open_controlled_generation(&binding, published)
            .expect("open predecessor for later selection");
        apply_available_for_worker_for_test(&runtime, &mut later_successor)
            .expect("apply through later durable head");
        let manifest = later_successor.checkpoint().expect("checkpoint later V1");
        let (length, checksum) = manifest.artifact_identity();
        let later_pointer = StoredColumnarProjectionGenerationV1::selected(
            published.generation(),
            ColumnarProjectionLayoutV1::V1,
            manifest.durable_frontier,
            published.history_incarnation(),
            ColumnarProjectionArtifactV1::new(length, checksum).expect("later artifact"),
            published.definition_fingerprint(),
            published.spec_hash(),
            None,
            riffdb_storage_api::ColumnarProjectionGenerationRoleV1::Published,
        )
        .expect("later pointer");
        let (later_successor, later_prepared) = runtime
            .open_prepared_generation(&binding, &later_pointer)
            .expect("validate later V1");
        drop(later_successor);
        let storage = Arc::clone(&runtime);
        let hook_expected = expected.clone();
        let result = advance_published_v1_under_capture_gate_for_test(
            &runtime,
            &binding,
            &expected,
            &attempted_prepared,
            attempted_successor,
            ColumnarPublicationTestMode::Ordinary,
            move || {
                assert_eq!(
                    storage
                        .storage()
                        .advance_published_v1(
                            &hook_expected,
                            &later_prepared,
                            storage.process_generation(),
                        )
                        .expect("install racing later V1"),
                    ColumnarProjectionControlWriteResultV1::Applied
                );
            },
        );
        assert_eq!(result, Ok(false));
        let installed = ServerColumnarProjectionPort::new(Arc::clone(&runtime))
            .observe("ticket_board")
            .expect("capture exact later durable selection");
        assert_eq!(
            installed.published_frontier().position(),
            manifest.durable_frontier,
            "StateChanged must install the exact later durable V1 before reopening capture"
        );
    }

    // req: PRJ-002, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn published_v1_no_commit_installs_durable_predecessor_when_local_view_lags() {
        use crate::columnar_worker::{
            ColumnarPublicationTestMode, advance_published_v1_under_capture_gate_for_test,
            apply_available_for_worker_for_test,
        };

        let (runtime, _scope) = board_runtime("v1-local-view-lags-durable");
        let (binding, initial, first_successor, first_prepared) =
            prepare_published_v1_frontier_advance(&runtime);
        assert_eq!(
            runtime
                .storage()
                .advance_published_v1(&initial, &first_prepared, runtime.process_generation(),)
                .expect("advance durable control without installing local view"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let durable_predecessor = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread durable predecessor")
            .expect("durable predecessor control");
        let durable_pointer = durable_predecessor
            .published()
            .expect("durable predecessor pointer");
        assert_ne!(
            ServerColumnarProjectionPort::new(Arc::clone(&runtime))
                .observe("ticket_board")
                .expect("capture lagging local view")
                .published_frontier()
                .position(),
            durable_pointer.frontier()
        );
        drop(first_successor);

        append_board_ticket_for_worker_at(
            &runtime,
            CommitSequence::new(2).expect("second sequence"),
            0x43,
        );
        let mut second_successor = runtime
            .open_controlled_generation(&binding, durable_pointer)
            .expect("open durable predecessor for next advance");
        apply_available_for_worker_for_test(&runtime, &mut second_successor)
            .expect("apply second frontier");
        let second_manifest = second_successor
            .checkpoint()
            .expect("checkpoint second frontier");
        let (length, checksum) = second_manifest.artifact_identity();
        let second_pointer = StoredColumnarProjectionGenerationV1::selected(
            durable_pointer.generation(),
            ColumnarProjectionLayoutV1::V1,
            second_manifest.durable_frontier,
            durable_pointer.history_incarnation(),
            ColumnarProjectionArtifactV1::new(length, checksum).expect("second artifact"),
            durable_pointer.definition_fingerprint(),
            durable_pointer.spec_hash(),
            None,
            riffdb_storage_api::ColumnarProjectionGenerationRoleV1::Published,
        )
        .expect("second pointer");
        let (second_successor, second_prepared) = runtime
            .open_prepared_generation(&binding, &second_pointer)
            .expect("validate second successor");
        assert_eq!(
            advance_published_v1_under_capture_gate_for_test(
                &runtime,
                &binding,
                &durable_predecessor,
                &second_prepared,
                second_successor,
                ColumnarPublicationTestMode::StorageFailureBeforeCommit,
                || {},
            ),
            Err(())
        );
        assert_eq!(
            ServerColumnarProjectionPort::new(runtime)
                .observe("ticket_board")
                .expect("capture exact durable predecessor")
                .published_frontier()
                .position(),
            durable_pointer.frontier(),
            "a failed CAS must still install the exact durable artifact when the local view lagged"
        );
    }

    // req: PRJ-002, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn published_v1_state_change_installs_the_later_durable_v2_selection() {
        use crate::columnar_worker::{
            ColumnarPublicationTestMode, advance_published_v1_under_capture_gate_for_test,
        };

        let (runtime, _scope) = board_runtime("v1-later-durable-v2-selection");
        let (binding, expected, successor, prepared) =
            prepare_published_v1_frontier_advance(&runtime);
        assert_eq!(
            advance_published_v1_under_capture_gate_for_test(
                &runtime,
                &binding,
                &expected,
                &prepared,
                successor,
                ColumnarPublicationTestMode::Ordinary,
                || {},
            ),
            Ok(true)
        );
        for _ in 0..3 {
            assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        }
        let durable_v2 = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread later V2 control")
            .expect("later V2 control");
        let selected_v2 = durable_v2.servable_generation().expect("selected later V2");
        assert_eq!(selected_v2.layout(), ColumnarProjectionLayoutV1::V2);
        let unused_attempt_view = runtime
            .open_controlled_generation(&binding, selected_v2)
            .expect("open a valid view for the stale attempted call");

        assert_eq!(
            advance_published_v1_under_capture_gate_for_test(
                &runtime,
                &binding,
                &expected,
                &prepared,
                unused_attempt_view,
                ColumnarPublicationTestMode::Ordinary,
                || {},
            ),
            Ok(false)
        );
        let installed = ServerColumnarProjectionPort::new(Arc::clone(&runtime))
            .observe("ticket_board")
            .expect("capture exact later durable V2");
        assert_eq!(
            installed.published_frontier().position(),
            selected_v2.frontier()
        );
        assert_eq!(
            runtime
                .engine("ticket_board")
                .expect("engine registry")
                .expect("active slot")
                .generation(),
            Some(selected_v2.generation())
        );
    }

    // req: PRJ-002, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn published_v1_crash_reopens_the_exact_durable_artifact() {
        const CHILD_MODE: &str = "RIFFDB_SERVER_V1_ADVANCE_CRASH_CHILD";
        const CHILD_PATH: &str = "RIFFDB_SERVER_V1_ADVANCE_CRASH_PATH";
        const EXACT_TEST: &str =
            "columnar_adapter::tests::published_v1_crash_reopens_the_exact_durable_artifact";

        if std::env::var(CHILD_MODE).as_deref() == Ok("1") {
            let path = PathBuf::from(std::env::var_os(CHILD_PATH).expect("child path"));
            let runtime = open_board_runtime_at(&path);
            request_projection(&runtime, "ticket_board");
            for _ in 0..4 {
                let _ = crate::columnar_worker::run_one_test_pass(&runtime);
            }
            panic!("child did not abort at the requested V1 publication boundary");
        }

        for (label, boundary, unknown, expected_frontier) in [
            (
                "before-cas",
                "before-cas",
                false,
                FrontierPosition::AppliedThrough(CommitSequence::first()),
            ),
            (
                "after-cas",
                "after-cas",
                false,
                FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("second sequence")),
            ),
            (
                "unknown-after-cas",
                "after-cas",
                true,
                FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("second sequence")),
            ),
            (
                "after-reread",
                "after-reread",
                false,
                FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("second sequence")),
            ),
            (
                "after-view-install",
                "after-view-install",
                false,
                FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("second sequence")),
            ),
        ] {
            let (runtime, scope) = board_runtime(&format!("v1-crash-{label}"));
            request_projection(&runtime, "ticket_board");
            assert!(crate::columnar_worker::run_one_test_pass(&runtime));
            append_board_ticket_for_worker_at(&runtime, CommitSequence::first(), 0x42);
            assert!(crate::columnar_worker::run_one_test_pass(&runtime));
            let binding = runtime
                .control_binding("ticket_board")
                .expect("control binding")
                .clone();
            let predecessor = runtime
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("read predecessor control")
                .expect("predecessor control")
                .published()
                .expect("published predecessor")
                .clone();
            let predecessor_artifact = predecessor.artifact().expect("predecessor artifact");
            let predecessor_manifest = controlled_generation_directory(
                &scope.path().join("projections"),
                binding.spec().hash(),
                predecessor.generation(),
            )
            .join(format!(
                "MANIFEST-V1-{}",
                lower_hex(&predecessor_artifact.checksum())
            ));
            append_board_ticket_for_worker_at(
                &runtime,
                CommitSequence::new(2).expect("second sequence"),
                0x43,
            );
            assert_eq!(
                runtime
                    .storage()
                    .recover_expected_control(binding.spec().source())
                    .expect("reread predecessor before crash")
                    .expect("predecessor before crash")
                    .servable_generation()
                    .expect("selected predecessor before crash")
                    .frontier(),
                FrontierPosition::AppliedThrough(CommitSequence::first())
            );
            drop(runtime);

            let mut child = std::process::Command::new(
                std::env::current_exe().expect("server test executable"),
            );
            child
                .arg("--exact")
                .arg(EXACT_TEST)
                .arg("--nocapture")
                .env(CHILD_MODE, "1")
                .env(CHILD_PATH, scope.path())
                .env("RIFFDB_COLUMNAR_V1_PUBLICATION_ABORT_AT", boundary)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            if unknown {
                child.env(
                    "RIFFDB_COLUMNAR_PUBLICATION_RESULT",
                    "unknown-after-applied",
                );
            }
            let status = child.status().expect("spawn V1 publication crash child");
            assert_eq!(status.code(), None, "child must abort at {label}");
            if boundary == "before-cas" {
                assert!(
                    predecessor_manifest.is_file(),
                    "pre-CAS validation/crash preserves the selected predecessor artifact"
                );
            }

            let reopened = open_board_runtime_at(scope.path());
            let durable = reopened
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("reread durable V1 after crash")
                .expect("durable V1 after crash");
            assert_eq!(
                durable
                    .servable_generation()
                    .expect("servable V1 after crash")
                    .frontier(),
                expected_frontier,
                "{label}"
            );
            let port = ServerColumnarProjectionPort::new(Arc::clone(&reopened));
            let cold = port.observe("ticket_board").expect("request reopen");
            assert!(!cold.has_published(), "{label}");
            assert!(crate::columnar_worker::activate_one_test_slot(
                &reopened,
                "ticket_board"
            ));
            assert_eq!(
                port.observe("ticket_board")
                    .expect("capture exact crash-selected V1")
                    .published_frontier()
                    .position(),
                expected_frontier,
                "{label}"
            );
        }
    }

    // req: PRJ-002, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022, OQ-024
    #[test]
    fn columnar_control_publish_requires_prepared_root_and_transaction_current_head() {
        let (runtime, scope) = board_runtime("prepared-root-publication");
        let publication = prepare_two_partition_v2_publication(&runtime, &scope);
        let rejected = runtime
            .storage()
            .publish_prepared_generation(&publication.expected, &publication.prepared, [0x99; 16])
            .expect_err("another process cannot consume the validated root witness");
        assert_eq!(rejected.kind(), StorageErrorKind::InvariantViolation);
        assert_eq!(
            runtime
                .storage()
                .recover_expected_control(publication.binding.spec().source())
                .expect("reread after process-generation refusal")
                .expect("control remains selected"),
            publication.expected,
            "witness refusal occurs before the exact-control transaction"
        );
        assert_eq!(
            runtime
                .storage()
                .publish_prepared_generation(
                    &publication.expected,
                    &publication.prepared,
                    runtime.process_generation(),
                )
                .expect("publish exact validated root"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let selected = runtime
            .storage()
            .recover_expected_control(publication.binding.spec().source())
            .expect("reread exact selection")
            .expect("selected control");
        let selected_generation = selected.published().expect("selected V2");
        let witnessed_generation = publication.prepared.generation();
        assert_eq!(
            selected_generation.generation(),
            witnessed_generation.generation()
        );
        assert_eq!(selected_generation.layout(), witnessed_generation.layout());
        assert_eq!(
            selected_generation.frontier(),
            witnessed_generation.frontier()
        );
        assert_eq!(
            selected_generation.artifact(),
            witnessed_generation.artifact()
        );
        assert_eq!(
            selected_generation.physical_generation_fingerprint(),
            witnessed_generation.physical_generation_fingerprint(),
            "only the immutable root identity carried by the validated witness is selected"
        );

        let (racing_runtime, racing_scope) = board_runtime("prepared-root-head-race");
        let ahead = FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first());
        assert_eq!(
            racing_runtime.read_application_head().expect("empty head"),
            FrontierPosition::BeforeFirst
        );
        let racing =
            prepare_two_partition_v2_publication_at_frontier(&racing_runtime, &racing_scope, ahead);
        let retained_v1 = racing.expected.published().cloned();
        let rejected = racing_runtime
            .storage()
            .publish_prepared_generation(
                &racing.expected,
                &racing.prepared,
                racing_runtime.process_generation(),
            )
            .expect_err("a complete root ahead of the transaction-current head must refuse");
        assert_eq!(rejected.kind(), StorageErrorKind::InvariantViolation);
        let after_refusal = racing_runtime
            .storage()
            .recover_expected_control(racing.binding.spec().source())
            .expect("reread head-race refusal")
            .expect("control survives refusal");
        assert_eq!(after_refusal, racing.expected);
        assert_eq!(
            after_refusal.published().cloned(),
            retained_v1,
            "head-race refusal cannot change the independently selected projection"
        );
    }

    // req: PRJ-006, PRJ-008, PRJ-009, OQ-020, OQ-022, PERF-007
    #[test]
    fn columnar_control_gate_recovers_every_cas_result_before_acknowledgement() {
        use crate::columnar_worker::{
            ColumnarPublicationTestMode, publish_prepared_under_capture_gate_for_test,
        };

        for (label, mode, becomes_selected) in [
            ("applied", ColumnarPublicationTestMode::Ordinary, true),
            (
                "state-changed",
                ColumnarPublicationTestMode::StateChangedAfterApplied,
                true,
            ),
            (
                "storage-failure",
                ColumnarPublicationTestMode::StorageFailureBeforeCommit,
                false,
            ),
            (
                "unknown-applied",
                ColumnarPublicationTestMode::UnknownAfterApplied,
                true,
            ),
            (
                "unknown-uncommitted",
                ColumnarPublicationTestMode::UnknownBeforeCommit,
                false,
            ),
        ] {
            let (runtime, scope) = board_runtime(&format!("publication-{label}"));
            let publication = prepare_two_partition_v2_publication(&runtime, &scope);
            let port = ServerColumnarProjectionPort::new(Arc::clone(&runtime));
            let predecessor = port
                .observe("ticket_board")
                .expect("capture predecessor")
                .snapshot_arc();
            let waiter = runtime
                .notifier()
                .register("ticket_board".to_owned())
                .expect("register exact acknowledgement observer");
            let slot = runtime
                .engine("ticket_board")
                .expect("engine registry")
                .expect("active slot");
            let candidate_generation = publication
                .expected
                .candidate()
                .expect("prepared candidate")
                .generation();
            let result = publish_prepared_under_capture_gate_for_test(
                &runtime,
                &publication.binding,
                &slot,
                &publication.expected,
                &publication.prepared,
                publication.successor,
                mode,
                || {},
            );
            let durable = runtime
                .storage()
                .recover_expected_control(publication.binding.spec().source())
                .expect("durable reread after injected result")
                .expect("durable control");
            let installed = port
                .observe("ticket_board")
                .expect("capture durable selected view")
                .snapshot_arc();
            let wake = waiter
                .wait(Instant::now())
                .expect("bounded wake observation");

            if becomes_selected {
                assert_eq!(result, Ok(true), "{label}");
                assert_eq!(
                    durable.published().map(|value| value.generation()),
                    Some(candidate_generation),
                    "{label}: durable reread, not the result class, selects the candidate"
                );
                assert!(
                    Arc::ptr_eq(&installed, &publication.successor_snapshot),
                    "{label}: the gate installs the exact fully validated successor Arc"
                );
                for organization in &publication.organizations {
                    assert_eq!(installed.merged_org(organization).len(), 1, "{label}");
                }
                assert_eq!(wake, riffdb_service::ColumnarWake::Notified, "{label}");
            } else {
                assert_eq!(result, Err(()), "{label}");
                assert_eq!(
                    durable.published(),
                    publication.expected.published(),
                    "{label}"
                );
                assert!(
                    !Arc::ptr_eq(&installed, &publication.successor_snapshot),
                    "{label}: an unselected candidate Arc is never installed"
                );
                assert!(
                    !Arc::ptr_eq(&predecessor, &publication.successor_snapshot),
                    "{label}: predecessor and candidate capabilities are distinct"
                );
                assert_eq!(wake, riffdb_service::ColumnarWake::TimedOut, "{label}");
            }
        }
    }

    // req: PRJ-002, PRJ-004, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022, OQ-024
    #[test]
    fn columnar_v2_generation_root_publication_is_atomic_across_partitions() {
        const CAPTURES: usize = 4;
        use crate::columnar_worker::{
            ColumnarPublicationTestMode, publish_prepared_under_capture_gate_for_test,
        };

        let (runtime, scope) = board_runtime("multi-partition-capture-race");
        let publication = prepare_two_partition_v2_publication(&runtime, &scope);
        let port = ServerColumnarProjectionPort::new(Arc::clone(&runtime));
        let predecessor = port
            .observe("ticket_board")
            .expect("capture predecessor before gate")
            .snapshot_arc();
        assert!(
            publication
                .organizations
                .iter()
                .all(|organization| predecessor.merged_org(organization).is_empty())
        );

        let slot = runtime
            .engine("ticket_board")
            .expect("engine registry")
            .expect("active slot");
        let (attempt_tx, attempt_rx) = mpsc::sync_channel(CAPTURES);
        slot.set_capture_attempt_probe(Some(attempt_tx));
        let (gate_closed_tx, gate_closed_rx) = mpsc::sync_channel(0);
        let (publish_tx, publish_rx) = mpsc::sync_channel(0);
        let publish_runtime = Arc::clone(&runtime);
        let publish_slot = Arc::clone(&slot);
        let expected_snapshot = Arc::clone(&publication.successor_snapshot);
        let organizations = publication.organizations.clone();
        let binding = publication.binding.clone();
        let expected_generation = publication
            .expected
            .candidate()
            .expect("candidate")
            .generation();
        let publisher = thread::spawn(move || {
            publish_prepared_under_capture_gate_for_test(
                &publish_runtime,
                &publication.binding,
                &publish_slot,
                &publication.expected,
                &publication.prepared,
                publication.successor,
                ColumnarPublicationTestMode::Ordinary,
                || {
                    gate_closed_tx.send(()).expect("publish gate closed");
                    publish_rx.recv().expect("release exact publication");
                },
            )
        });
        gate_closed_rx.recv().expect("observe closed capture gate");

        let (captured_tx, captured_rx) = mpsc::channel();
        let mut captures = Vec::new();
        for _ in 0..CAPTURES {
            let capture_runtime = Arc::clone(&runtime);
            let captured_tx = captured_tx.clone();
            captures.push(thread::spawn(move || {
                let snapshot = ServerColumnarProjectionPort::new(capture_runtime)
                    .observe("ticket_board")
                    .expect("capture after publication")
                    .snapshot_arc();
                captured_tx
                    .send(Arc::clone(&snapshot))
                    .expect("send capture");
                snapshot
            }));
        }
        drop(captured_tx);
        for _ in 0..CAPTURES {
            attempt_rx
                .recv()
                .expect("real request reached the held engine-slot mutex");
        }
        assert!(
            matches!(captured_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "no request capture completes before durable selection and exact view installation"
        );
        slot.set_capture_attempt_probe(None);
        publish_tx.send(()).expect("allow publication CAS");
        assert_eq!(publisher.join().expect("join publisher"), Ok(true));

        for capture in captures {
            let snapshot = capture.join().expect("join request capture");
            assert!(
                Arc::ptr_eq(&snapshot, &expected_snapshot),
                "every capture after the gate receives the one exact selected Arc"
            );
            for organization in &organizations {
                assert_eq!(snapshot.merged_org(organization).len(), 1);
            }
        }
        assert!(
            organizations
                .iter()
                .all(|organization| predecessor.merged_org(organization).is_empty())
        );
        let durable = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread exact selection")
            .expect("selected control");
        assert_eq!(
            durable.published().map(|value| value.generation()),
            Some(expected_generation)
        );
    }

    // req: PRJ-006, PRJ-007, PRJ-010, OQ-020, OQ-022
    #[test]
    fn failed_initial_and_unservable_v1_candidates_are_replaced_before_reclamation() {
        let (initial_runtime, initial_scope) = board_runtime("failed-initial-v1-replacement");
        let initial_binding = initial_runtime
            .control_binding("ticket_board")
            .expect("initial binding")
            .clone();
        let initial = initial_runtime
            .storage()
            .recover_expected_control(initial_binding.spec().source())
            .expect("read initial control")
            .expect("initial control");
        let failed_initial = initial_runtime
            .storage()
            .record_candidate_failure(
                &initial,
                riffdb_storage_api::ColumnarProjectionFailureReasonV1::ReplayBytes,
            )
            .expect("record initial candidate failure");
        assert_eq!(
            failed_initial,
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let failed_initial_directory = controlled_generation_directory(
            &initial_scope.path().join("projections"),
            initial_binding.spec().hash(),
            ProjectionGeneration::first(),
        );
        std::fs::create_dir_all(&failed_initial_directory)
            .expect("create failed initial candidate directory");
        std::fs::write(failed_initial_directory.join("orphan"), b"failed candidate")
            .expect("write failed initial candidate member");

        request_projection(&initial_runtime, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(&initial_runtime));
        let replaced_initial = initial_runtime
            .storage()
            .recover_expected_control(initial_binding.spec().source())
            .expect("read replaced initial control")
            .expect("replaced initial control");
        assert_eq!(
            replaced_initial.lifecycle(),
            riffdb_storage_api::ColumnarProjectionLifecycleV1::Building
        );
        assert_eq!(replaced_initial.highest_generation().get(), 2);
        assert_eq!(
            replaced_initial.retention_frontier(),
            Some(FrontierPosition::BeforeFirst)
        );
        assert!(
            !failed_initial_directory.exists(),
            "the failed Candidate is reclaimed only after its replacement CAS"
        );

        let (unservable_runtime, unservable_scope) =
            board_runtime("failed-unservable-v1-replacement");
        let unservable_binding = unservable_runtime
            .control_binding("ticket_board")
            .expect("unservable binding")
            .clone();
        request_projection(&unservable_runtime, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(
            &unservable_runtime
        ));
        let ready = unservable_runtime
            .storage()
            .recover_expected_control(unservable_binding.spec().source())
            .expect("read ready control")
            .expect("ready control");
        let selected = ready.published().expect("selected V1").clone();
        assert_eq!(
            unservable_runtime
                .storage()
                .record_published_failure(&ready)
                .expect("record selected corruption"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let corrupt = unservable_runtime
            .storage()
            .recover_expected_control(unservable_binding.spec().source())
            .expect("read corrupt control")
            .expect("corrupt control");
        assert_eq!(
            unservable_runtime
                .storage()
                .allocate_unservable_rebuild_candidate(
                    &corrupt,
                    unservable_binding.spec().definition_fingerprint(),
                    unservable_binding.spec().hash(),
                    unservable_binding.spec().replay_limits(),
                    ColumnarProjectionLayoutV1::V1,
                    None,
                )
                .expect("allocate corruption rebuild"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let rebuilding = unservable_runtime
            .storage()
            .recover_expected_control(unservable_binding.spec().source())
            .expect("read corruption rebuild")
            .expect("corruption rebuild");
        let failed_generation = rebuilding.candidate().expect("V1 candidate").generation();
        assert_eq!(
            unservable_runtime
                .storage()
                .record_candidate_failure(
                    &rebuilding,
                    riffdb_storage_api::ColumnarProjectionFailureReasonV1::ReplayBacklog,
                )
                .expect("record unservable candidate failure"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let failed_unservable_directory = controlled_generation_directory(
            &unservable_scope.path().join("projections"),
            unservable_binding.spec().hash(),
            failed_generation,
        );
        std::fs::create_dir_all(&failed_unservable_directory)
            .expect("create failed unservable candidate directory");
        std::fs::write(
            failed_unservable_directory.join("orphan"),
            b"failed candidate",
        )
        .expect("write failed unservable candidate member");

        assert!(crate::columnar_worker::run_one_test_pass(
            &unservable_runtime
        ));
        let replaced_unservable = unservable_runtime
            .storage()
            .recover_expected_control(unservable_binding.spec().source())
            .expect("read replaced unservable control")
            .expect("replaced unservable control");
        assert_eq!(
            replaced_unservable.lifecycle(),
            riffdb_storage_api::ColumnarProjectionLifecycleV1::Rebuilding
        );
        let retained_predecessor = replaced_unservable
            .predecessor()
            .expect("retained corrupt predecessor");
        assert_eq!(retained_predecessor.generation(), selected.generation());
        assert_eq!(retained_predecessor.layout(), selected.layout());
        assert_eq!(retained_predecessor.frontier(), selected.frontier());
        assert_eq!(retained_predecessor.artifact(), selected.artifact());
        assert_eq!(
            retained_predecessor.definition_fingerprint(),
            selected.definition_fingerprint()
        );
        assert_eq!(retained_predecessor.spec_hash(), selected.spec_hash());
        assert_eq!(
            replaced_unservable.highest_generation().get(),
            failed_generation.get() + 1
        );
        assert_eq!(
            replaced_unservable.retention_frontier(),
            Some(FrontierPosition::BeforeFirst)
        );
        assert!(
            !failed_unservable_directory.exists(),
            "the failed unservable Candidate is reclaimed only after replacement"
        );
        assert!(
            controlled_generation_directory(
                &unservable_scope.path().join("projections"),
                unservable_binding.spec().hash(),
                selected.generation(),
            )
            .exists(),
            "the unservable predecessor remains retained byte-exact"
        );
    }

    // req: REC-001, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010
    #[test]
    fn stale_history_incarnation_resets_columnar_control_before_serving() {
        let (runtime, scope) = board_runtime("selected-v1-stale-incarnation");
        request_projection(&runtime, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let selected = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read selected V1 control")
            .expect("selected V1 control");
        let selected = selected
            .servable_generation()
            .expect("selected V1 generation")
            .clone();
        assert_eq!(selected.layout(), ColumnarProjectionLayoutV1::V1);
        assert_eq!(selected.history_incarnation(), 1);
        let generation_directory = controlled_generation_directory(
            &scope.path().join("projections"),
            binding.spec().hash(),
            selected.generation(),
        );
        let manifest_name = format!(
            "MANIFEST-V1-{}",
            lower_hex(
                &selected
                    .artifact()
                    .expect("selected V1 artifact")
                    .checksum()
            )
        );
        let manifest_before = std::fs::read(generation_directory.join(&manifest_name))
            .expect("read selected V1 manifest");
        drop(runtime);

        riffdb_storage_redb::stamp_history_incarnation(scope.path().join("db.redb"), 2)
            .expect("advance authoritative history incarnation");

        let reopened = open_board_runtime_at_incarnation(scope.path(), 2)
            .expect("stale derived control resets before runtime installation");
        let reset = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread reset control")
            .expect("reset control");
        assert_eq!(reset.lifecycle(), ColumnarProjectionLifecycleV1::Building);
        assert!(reset.published().is_none());
        assert!(reset.predecessor().is_none());
        assert!(reset.failure().is_none());
        let candidate = reset.candidate().expect("fresh current candidate");
        assert_eq!(candidate.history_incarnation(), 2);
        assert_eq!(
            candidate.generation().get(),
            selected.generation().get() + 1
        );
        assert_eq!(candidate.frontier(), FrontierPosition::BeforeFirst);
        assert!(candidate.artifact().is_none());
        assert_eq!(
            reset.retention_frontier(),
            Some(FrontierPosition::BeforeFirst),
            "only the fresh current-incarnation candidate supplies retention"
        );
        assert_eq!(
            reopened
                .engine("ticket_board")
                .expect("engine registry")
                .expect("cold reset slot")
                .lifecycle(),
            Ok(ColumnarSlotLifecycle::Cold),
            "startup cannot install the stale selected view"
        );
        let observation = ServerColumnarProjectionPort::new(Arc::clone(&reopened))
            .observe("ticket_board")
            .expect("cold reset observation");
        assert!(!observation.has_published());
        assert_eq!(
            std::fs::read(generation_directory.join(&manifest_name))
                .expect("reread stale V1 manifest"),
            manifest_before,
            "frozen stale V1 bytes are neither mutated nor relabeled"
        );

        request_projection(&reopened, "ticket_board");
        for _ in 0..4 {
            assert!(crate::columnar_worker::run_one_test_pass(&reopened));
            let durable = reopened
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("reread rebuilt control")
                .expect("rebuilt control");
            if durable.servable_generation().is_some() {
                assert!(
                    durable
                        .servable_generation()
                        .is_some_and(|value| value.history_incarnation() == 2)
                );
                return;
            }
        }
        panic!("fresh current-incarnation V1 candidate did not publish");
    }

    // req: REC-001, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010
    #[test]
    fn columnar_history_reset_retires_exact_colliding_candidate_paths() {
        let (runtime, scope) = board_runtime("history-reset-path-collision");
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let stale = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read stale control")
            .expect("stale control");
        let reset_generation = stale
            .highest_generation()
            .checked_next()
            .expect("reset generation");
        let final_path = controlled_generation_directory(
            &scope.path().join("projections"),
            binding.spec().hash(),
            reset_generation,
        );
        let temporary_path = final_path.with_file_name(format!(
            "{}.tmp",
            final_path
                .file_name()
                .expect("final name")
                .to_string_lossy()
        ));
        std::fs::create_dir_all(&final_path).expect("create colliding final path");
        std::fs::write(final_path.join("seg-stale"), b"stale-final")
            .expect("write colliding final path");
        std::fs::create_dir_all(&temporary_path).expect("create colliding temporary path");
        std::fs::write(
            temporary_path.join("MANIFEST.stale.tmp"),
            b"stale-temporary",
        )
        .expect("write colliding temporary path");
        let unrelated = final_path
            .parent()
            .expect("source directory")
            .join("unmanaged-sibling");
        std::fs::create_dir_all(&unrelated).expect("create unrelated sibling");
        std::fs::write(unrelated.join("keep"), b"keep").expect("write unrelated sibling");
        drop(runtime);
        riffdb_storage_redb::stamp_history_incarnation(scope.path().join("db.redb"), 2)
            .expect("advance authoritative history");

        let reopened = open_board_runtime_at_incarnation(scope.path(), 2)
            .expect("retire exact collisions before cold runtime");
        let quarantine_suffix = ".retired-before-history-0000000000000002";
        let final_quarantine = final_path.with_file_name(format!(
            "{}{quarantine_suffix}",
            final_path
                .file_name()
                .expect("final name")
                .to_string_lossy()
        ));
        let temporary_quarantine = temporary_path.with_file_name(format!(
            "{}{quarantine_suffix}",
            temporary_path
                .file_name()
                .expect("temporary name")
                .to_string_lossy()
        ));
        assert!(!final_path.exists());
        assert!(!temporary_path.exists());
        assert_eq!(
            std::fs::read(final_quarantine.join("seg-stale")).expect("quarantined final"),
            b"stale-final"
        );
        assert_eq!(
            std::fs::read(temporary_quarantine.join("MANIFEST.stale.tmp"))
                .expect("quarantined temporary"),
            b"stale-temporary"
        );
        assert_eq!(
            std::fs::read(unrelated.join("keep")).expect("unrelated sibling survives"),
            b"keep",
            "the handler scans or touches no sibling"
        );
        drop(reopened);

        let repeated = open_board_runtime_at_incarnation(scope.path(), 2)
            .expect("repeated startup completes quarantine cleanup");
        assert!(!final_quarantine.exists());
        assert!(!temporary_quarantine.exists());
        assert!(!final_path.exists());
        assert!(!temporary_path.exists());
        assert_eq!(
            repeated
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("reread repeat control")
                .expect("repeat control")
                .candidate()
                .expect("same candidate")
                .generation(),
            reset_generation,
            "repeated startup cannot allocate again"
        );
    }

    // req: REC-001, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010
    #[test]
    fn columnar_history_reset_precedes_spec_retarget() {
        let (runtime, scope) = board_runtime("history-reset-before-retarget");
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let initial = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read initial control")
            .expect("initial control");
        let stale_spec = riffdb_types::ColumnarProjectionSpecHashV1::from_bytes([0x8d; 32]);
        assert_eq!(
            runtime
                .storage()
                .retarget_initial_candidate(
                    &initial,
                    binding.spec().definition_fingerprint(),
                    stale_spec,
                    binding.spec().replay_limits(),
                )
                .expect("install stale specification"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let stale = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread stale specification")
            .expect("stale specification");
        assert_eq!(stale.highest_generation().get(), 2);
        assert_eq!(stale.target_spec_hash(), stale_spec);
        drop(runtime);
        riffdb_storage_redb::stamp_history_incarnation(scope.path().join("db.redb"), 2)
            .expect("advance authoritative history");

        let reopened = open_board_runtime_at_incarnation(scope.path(), 2)
            .expect("reset then ordinary retarget");
        let reconciled = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread reconciled control")
            .expect("reconciled control");
        assert_eq!(
            reconciled.highest_generation().get(),
            4,
            "reset allocates generation 3 before retarget allocates generation 4"
        );
        assert_eq!(reconciled.target_spec_hash(), binding.spec().hash());
        assert_eq!(
            reconciled.lifecycle(),
            ColumnarProjectionLifecycleV1::Building
        );
        let candidate = reconciled.candidate().expect("final retarget candidate");
        assert_eq!(candidate.history_incarnation(), 2);
        assert_eq!(candidate.generation().get(), 4);
        assert!(candidate.artifact().is_none());
        assert_eq!(candidate.frontier(), FrontierPosition::BeforeFirst);
        let observation = ServerColumnarProjectionPort::new(Arc::clone(&reopened))
            .observe("ticket_board")
            .expect("cold reconciled observation");
        assert!(!observation.has_published());
        assert_eq!(
            reconciled.retention_frontier(),
            Some(FrontierPosition::BeforeFirst)
        );
    }

    fn receipt_query(organization: [u8; 16]) -> ColumnarQueryRequest {
        ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(organization),
            select: Vec::new(),
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            group_by: None,
            aggregate: Some(AggregateOp::Count),
            budget: QueryBudget::default(),
        }
    }

    fn receipt_samples(samples: usize, mut operation: impl FnMut()) -> Vec<u128> {
        for _ in 0..5 {
            operation();
        }
        let mut measured = (0..samples)
            .map(|_| {
                let start = Instant::now();
                operation();
                start.elapsed().as_nanos()
            })
            .collect::<Vec<_>>();
        measured.sort_unstable();
        measured
    }

    fn receipt_percentile(samples: &[u128], percentile: usize) -> u128 {
        samples[(samples.len() - 1) * percentile / 100]
    }

    fn receipt_directory_bytes(directory: &Path) -> u64 {
        std::fs::read_dir(directory)
            .expect("read receipt directory")
            .map(|entry| {
                let entry = entry.expect("read receipt member");
                let metadata = entry.metadata().expect("receipt member metadata");
                if metadata.is_dir() {
                    receipt_directory_bytes(&entry.path())
                } else {
                    metadata.len()
                }
            })
            .sum()
    }

    fn receipt_query_via_port(
        port: &ServerColumnarProjectionPort,
        binding: &ColumnarControlBinding,
        query: &ColumnarQueryRequest,
    ) -> QueryResult {
        let observation = port
            .observe(binding.name())
            .expect("capture gate-installed receipt view");
        query_snapshot(binding.definition(), observation.snapshot().as_ref(), query)
            .expect("query gate-installed receipt view")
    }

    fn receipt_no_projection_control(
        label: &str,
        source: &riffdb_types::ColumnarProjectionSourceV1,
    ) -> (u64, usize, u64) {
        let (runtime, scope) = empty_columnar_runtime(label);
        let projections_root = scope.path().join("projections");
        assert!(runtime.names().expect("empty names").is_empty());
        assert!(runtime.control_bindings().is_empty());
        assert_eq!(
            runtime
                .storage()
                .recover_expected_control(source)
                .expect("read actual no-projection control"),
            None
        );
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let bytes = receipt_directory_bytes(&projections_root);
        let modeled_owned_allocations = runtime
            .names()
            .expect("empty names for modeled allocation count")
            .len()
            .saturating_mul(6);
        let population_passes = runtime.lifecycle_observation().population_passes();
        (bytes, modeled_owned_allocations, population_passes)
    }

    // req: PERF-007, PERF-008, PRJ-009
    #[test]
    fn no_projection_receipt_control_uses_an_actual_empty_runtime_and_durable_control() {
        let (runtime, _scope) = board_runtime("no-projection-control-source");
        let binding = runtime
            .control_binding("ticket_board")
            .expect("source binding");
        assert_eq!(
            receipt_no_projection_control(
                "no-projection-control-observation",
                binding.spec().source(),
            ),
            (0, 0, 0)
        );
    }

    // req: PRJ-002, PRJ-004, PRJ-009, PRJ-010, OQ-020, OQ-022, PERF-007, PERF-008
    #[test]
    #[ignore = "fixed WP-711 production worker/control/gate activation receipt; run explicitly in release mode"]
    fn wp711_production_v2_activation_receipt() {
        const ROWS: usize = 16_384;
        const PARTITIONS: usize = 2;
        const ROWS_PER_PARTITION: usize = ROWS / PARTITIONS;
        const SAMPLES: usize = 31;
        let (runtime, scope) = board_runtime("production-v2-activation-receipt");
        let projections_root = scope.path().join("projections");
        assert_eq!(receipt_directory_bytes(&projections_root), 0);
        assert_eq!(runtime.lifecycle_observation().population_passes(), 0);
        let (rows, commits) = columnar_receipt_rows_and_commits(ROWS_PER_PARTITION);
        runtime
            .storage()
            .append_columnar_worker_commit_fixture(&rows, &commits)
            .expect("seed exact authoritative receipt corpus");
        drop(rows);
        drop(commits);
        let binding = runtime
            .control_binding("ticket_board")
            .expect("receipt control binding")
            .clone();
        let (
            no_projection_bytes,
            no_projection_modeled_owned_allocations,
            no_projection_population_passes,
        ) = receipt_no_projection_control(
            "production-v2-activation-no-projection-control",
            binding.spec().source(),
        );
        let organizations = [uuid_bytes(0x63), uuid_bytes(0x64)];
        let queries = organizations.map(receipt_query);
        let expected = QueryResult::Aggregate(AggregateValue::Count(
            u64::try_from(ROWS_PER_PARTITION).expect("partition count"),
        ));
        let port = ServerColumnarProjectionPort::new(Arc::clone(&runtime));

        request_projection(&runtime, "ticket_board");
        let v1_waiter = runtime
            .notifier()
            .register("ticket_board".to_owned())
            .expect("register V1 publication acknowledgement");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        assert_eq!(
            v1_waiter.wait(Instant::now()).expect("V1 acknowledgement"),
            riffdb_service::ColumnarWake::Notified
        );
        let v1_control = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread selected V1")
            .expect("selected V1 control");
        let v1_pointer = v1_control
            .servable_generation()
            .expect("selected V1 pointer")
            .clone();
        assert_eq!(v1_pointer.layout(), ColumnarProjectionLayoutV1::V1);
        let v1_snapshot = port
            .observe("ticket_board")
            .expect("capture selected V1")
            .snapshot_arc();
        let v1_results = queries
            .iter()
            .map(|query| query_snapshot(binding.definition(), &v1_snapshot, query))
            .collect::<Result<Vec<_>, _>>()
            .expect("query selected V1");
        assert_eq!(v1_results, vec![expected.clone(); PARTITIONS]);
        let v1_directory = controlled_generation_directory(
            &projections_root,
            binding.spec().hash(),
            v1_pointer.generation(),
        );
        let v1_bytes = receipt_directory_bytes(&v1_directory);
        let v1_partition_query_ns = queries
            .iter()
            .map(|query| {
                receipt_samples(SAMPLES, || {
                    assert_eq!(
                        receipt_query_via_port(&port, &binding, black_box(query)),
                        expected
                    );
                })
            })
            .collect::<Vec<_>>();
        let v1_query_ns = receipt_samples(SAMPLES, || {
            let actual = queries
                .iter()
                .map(|query| receipt_query_via_port(&port, &binding, black_box(query)))
                .collect::<Vec<_>>();
            assert_eq!(actual, vec![expected.clone(); PARTITIONS]);
        });
        let v1_recovery_ns = receipt_samples(SAMPLES, || {
            let recovered = runtime
                .open_controlled_generation(&binding, &v1_pointer)
                .expect("V1 recovery");
            let recovered = recovered.published_snapshot();
            for query in &queries {
                assert_eq!(
                    query_snapshot(binding.definition(), &recovered, query)
                        .expect("recovered V1 query"),
                    expected
                );
            }
        });

        let rebuild_start = Instant::now();
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let v2_waiter = runtime
            .notifier()
            .register("ticket_board".to_owned())
            .expect("register V2 publication acknowledgement");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let rebuild_ns = rebuild_start.elapsed().as_nanos();
        assert_eq!(
            v2_waiter.wait(Instant::now()).expect("V2 acknowledgement"),
            riffdb_service::ColumnarWake::Notified
        );
        let v2_control = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread selected V2")
            .expect("selected V2 control");
        let v2_pointer = v2_control
            .servable_generation()
            .expect("selected V2 pointer")
            .clone();
        assert_eq!(v2_pointer.layout(), ColumnarProjectionLayoutV1::V2);
        let v2_snapshot = port
            .observe("ticket_board")
            .expect("capture selected V2")
            .snapshot_arc();
        let repeated_v2_snapshot = port
            .observe("ticket_board")
            .expect("recapture selected V2")
            .snapshot_arc();
        assert!(Arc::ptr_eq(&v2_snapshot, &repeated_v2_snapshot));
        let v2_results = queries
            .iter()
            .map(|query| query_snapshot(binding.definition(), &v2_snapshot, query))
            .collect::<Result<Vec<_>, _>>()
            .expect("query selected V2");
        assert_eq!(v2_results, v1_results);
        let source_directory =
            controlled_source_directory(&projections_root, binding.spec().hash());
        let v2_directory =
            source_directory.join(format!("generation-{:016x}", v2_pointer.generation().get()));
        let v2_bytes = receipt_directory_bytes(&v2_directory);
        let v2_partition_query_ns = queries
            .iter()
            .map(|query| {
                receipt_samples(SAMPLES, || {
                    assert_eq!(
                        receipt_query_via_port(&port, &binding, black_box(query)),
                        expected
                    );
                })
            })
            .collect::<Vec<_>>();
        let v2_query_ns = receipt_samples(SAMPLES, || {
            let actual = queries
                .iter()
                .map(|query| receipt_query_via_port(&port, &binding, black_box(query)))
                .collect::<Vec<_>>();
            assert_eq!(actual, v1_results);
        });
        let v2_recovery_ns = receipt_samples(SAMPLES, || {
            let recovered = runtime
                .open_controlled_generation(&binding, &v2_pointer)
                .expect("V2 recovery");
            let recovered = recovered.published_snapshot();
            for query in &queries {
                assert_eq!(
                    query_snapshot(binding.definition(), &recovered, query)
                        .expect("recovered V2 query"),
                    expected
                );
            }
        });

        let compaction_start = Instant::now();
        let physical = PhysicalGenerationFingerprintV1::compute(binding.definition().fingerprint());
        assert_eq!(
            runtime
                .storage()
                .allocate_same_spec_candidate(&v2_control, *physical.as_bytes())
                .expect("allocate production compaction candidate"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let compaction_waiter = runtime
            .notifier()
            .register("ticket_board".to_owned())
            .expect("register compaction acknowledgement");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let compaction_ns = compaction_start.elapsed().as_nanos();
        assert_eq!(
            compaction_waiter
                .wait(Instant::now())
                .expect("compaction acknowledgement"),
            riffdb_service::ColumnarWake::Notified
        );
        let compacted = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread compacted selection")
            .expect("compacted control");
        let compacted_pointer = compacted
            .servable_generation()
            .expect("selected compacted pointer");
        assert!(compacted_pointer.generation() > v2_pointer.generation());
        let compacted_snapshot = port
            .observe("ticket_board")
            .expect("capture compacted selected V2")
            .snapshot_arc();
        assert!(!Arc::ptr_eq(&compacted_snapshot, &v2_snapshot));
        let compacted_results = queries
            .iter()
            .map(|query| query_snapshot(binding.definition(), &compacted_snapshot, query))
            .collect::<Result<Vec<_>, _>>()
            .expect("query compacted selected V2");
        assert_eq!(compacted_results, v1_results);

        let matched_frontier = match compacted_pointer.frontier() {
            FrontierPosition::BeforeFirst => 0,
            FrontierPosition::AppliedThrough(sequence) => sequence.get(),
        };
        let projection_lag = u64::try_from(ROWS)
            .expect("row count")
            .saturating_sub(matched_frontier);
        let segment_count = v2_snapshot.segments.len();
        let v1_modeled_owned_allocations = ROWS * 6;
        let v2_modeled_owned_allocations = ROWS * 6 + segment_count * 17;
        println!(
            "WP711_ACTIVATION corpus=wp711-low-cardinality-v1-v2-v1 rows={ROWS} partitions={PARTITIONS} rows_per_partition={ROWS_PER_PARTITION} samples={SAMPLES} cpu_method=single-thread_elapsed_ns allocation_method=wp710_modeled_owned_allocations_not_allocator_calls matched_frontier={matched_frontier} matched_query_cases={PARTITIONS} partition_0_result_count={ROWS_PER_PARTITION} partition_1_result_count={ROWS_PER_PARTITION} projection_lag={projection_lag} no_projection_bytes={no_projection_bytes} no_projection_modeled_owned_allocations={no_projection_modeled_owned_allocations} no_projection_population_passes={no_projection_population_passes} no_v2_bytes={v1_bytes} no_v2_modeled_owned_allocations={v1_modeled_owned_allocations} v1_partition_0_query_p50_ns={} v1_partition_1_query_p50_ns={} v1_query_p50_ns={} v1_query_p95_ns={} v1_query_p99_ns={} v1_recovery_p50_ns={} v1_recovery_p95_ns={} v1_recovery_p99_ns={} v2_bytes={v2_bytes} v2_modeled_owned_allocations={v2_modeled_owned_allocations} v2_partition_0_query_p50_ns={} v2_partition_1_query_p50_ns={} v2_query_p50_ns={} v2_query_p95_ns={} v2_query_p99_ns={} v2_recovery_p50_ns={} v2_recovery_p95_ns={} v2_recovery_p99_ns={} rebuild_ns={rebuild_ns} compaction_ns={compaction_ns} result_count={ROWS} production_worker_passes=6 publication_acknowledgements=3 durable_v2_generation={} compaction_generation={} selected_arc_reused=1",
            receipt_percentile(&v1_partition_query_ns[0], 50),
            receipt_percentile(&v1_partition_query_ns[1], 50),
            receipt_percentile(&v1_query_ns, 50),
            receipt_percentile(&v1_query_ns, 95),
            receipt_percentile(&v1_query_ns, 99),
            receipt_percentile(&v1_recovery_ns, 50),
            receipt_percentile(&v1_recovery_ns, 95),
            receipt_percentile(&v1_recovery_ns, 99),
            receipt_percentile(&v2_partition_query_ns[0], 50),
            receipt_percentile(&v2_partition_query_ns[1], 50),
            receipt_percentile(&v2_query_ns, 50),
            receipt_percentile(&v2_query_ns, 95),
            receipt_percentile(&v2_query_ns, 99),
            receipt_percentile(&v2_recovery_ns, 50),
            receipt_percentile(&v2_recovery_ns, 95),
            receipt_percentile(&v2_recovery_ns, 99),
            v2_pointer.generation().get(),
            compacted_pointer.generation().get(),
        );
    }

    // req: PRJ-002, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn columnar_v2_compaction_reuses_generation_root_and_queries_validate_once() {
        let (runtime, scope) = board_runtime("v2-compaction-retirement");
        request_projection(&runtime, "ticket_board");
        for _ in 0..4 {
            assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        }
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let selected = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read V2 control")
            .expect("V2 control");
        let predecessor = selected.published().expect("published V2").clone();
        assert_eq!(predecessor.layout(), ColumnarProjectionLayoutV1::V2);
        let source_directory =
            controlled_source_directory(&scope.path().join("projections"), binding.spec().hash());
        let predecessor_directory = source_directory.join(format!(
            "generation-{:016x}",
            predecessor.generation().get()
        ));
        let predecessor_root =
            std::fs::read(predecessor_directory.join("ROOT-V1")).expect("read predecessor root");

        let port = ServerColumnarProjectionPort::new(Arc::clone(&runtime));
        let captured = port
            .observe("ticket_board")
            .expect("capture predecessor")
            .snapshot_arc();
        let root_path = predecessor_directory.join("ROOT-V1");
        let offline_path = predecessor_directory.join("ROOT-V1.offline");
        std::fs::rename(&root_path, &offline_path).expect("hide durable root after validation");
        assert!(matches!(
            query_snapshot(
                runtime
                    .engine("ticket_board")
                    .expect("engine registry")
                    .expect("slot")
                    .definition(),
                &captured,
                &board_query(),
            ),
            Ok(QueryResult::Rows(_))
        ));
        std::fs::rename(&offline_path, &root_path).expect("restore root for compaction");

        let physical = PhysicalGenerationFingerprintV1::compute(binding.definition().fingerprint());
        assert_eq!(
            runtime
                .storage()
                .allocate_same_spec_candidate(&selected, *physical.as_bytes())
                .expect("allocate immutable compaction candidate"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let rebuilding = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read rebuilding control")
            .expect("rebuilding control");
        assert!(crate::columnar_worker::run_one_test_pass_stopping_before_page(&runtime));
        let cancelled = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read cancelled rebuild")
            .expect("cancelled rebuild");
        assert_eq!(cancelled, rebuilding);
        assert_eq!(cancelled.published(), Some(&predecessor));
        assert!(
            predecessor_directory.exists(),
            "cancellation cannot reclaim the selected generation"
        );
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let prepared_compaction = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read prepared compaction")
            .expect("prepared compaction");
        let candidate = prepared_compaction.candidate().expect("prepared candidate");
        let candidate_directory =
            source_directory.join(format!("generation-{:016x}", candidate.generation().get()));
        assert!(candidate_directory.join("ROOT-V1").is_file());
        assert!(predecessor_directory.join("ROOT-V1").is_file());
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let compacted = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read compacted control")
            .expect("compacted control");
        let compacted_generation = compacted.published().expect("compacted V2").generation();
        assert!(compacted_generation > predecessor.generation());
        assert_eq!(
            std::fs::read(predecessor_directory.join("ROOT-V1"))
                .expect("captured predecessor remains durable"),
            predecessor_root,
            "compaction never mutates or reuses the predecessor root"
        );

        drop(captured);
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        assert!(
            !predecessor_directory.exists(),
            "an unselected generation is reclaimed after its last captured view"
        );

        let compacted_root = source_directory
            .join(format!("generation-{:016x}", compacted_generation.get()))
            .join("ROOT-V1");
        drop(port);
        drop(runtime);
        std::fs::write(&compacted_root, b"corrupt selected compacted root")
            .expect("corrupt selected compacted root");
        let reopened = reopen_board_runtime(&scope);
        request_projection(&reopened, "ticket_board");
        assert!(
            !crate::columnar_worker::run_one_test_pass(&reopened),
            "selected compaction corruption fails closed instead of rolling back"
        );
        let degraded = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read degraded selection")
            .expect("degraded selection");
        assert_eq!(degraded.published(), compacted.published());
        assert!(degraded.servable_generation().is_none());
        assert!(!predecessor_directory.exists());
    }

    // req: PRJ-008, PRJ-009, OQ-020, OQ-022, PERF-007
    #[test]
    fn columnar_capture_gate_retains_predecessor_and_exposes_only_installed_successor() {
        let (runtime, _scope) = board_runtime("capture-gate");
        request_projection(&runtime, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));

        let port = ServerColumnarProjectionPort::new(Arc::clone(&runtime));
        let predecessor = port
            .observe("ticket_board")
            .expect("capture predecessor")
            .snapshot_arc();
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding");
        let durable = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread durable selection")
            .expect("durable selection");
        let selected = durable.servable_generation().expect("selected generation");
        let successor = runtime
            .open_controlled_generation(binding, selected)
            .expect("open exact successor");
        let slot = runtime
            .engine("ticket_board")
            .expect("engine registry")
            .expect("active slot");

        let mut gate = slot.close_capture_gate().expect("close capture gate");
        let probe_slot = Arc::clone(&slot);
        let probe = thread::spawn(move || probe_slot.try_capture_snapshot_for_test());
        assert!(
            matches!(
                probe.join().expect("join gated capture"),
                Err(ColumnarPortError::Unavailable)
            ),
            "new capture must be closed while publication owns the slot mutex"
        );
        assert!(
            predecessor.segments.is_empty() && predecessor.delta.is_empty(),
            "a captured predecessor remains independently usable while the gate is closed"
        );

        gate.install_selected(successor, selected.generation())
            .expect("install exact selected successor");
        drop(gate);
        let installed = port
            .observe("ticket_board")
            .expect("capture installed successor")
            .snapshot_arc();
        assert!(
            !Arc::ptr_eq(&predecessor, &installed),
            "capture reopens only after a distinct validated successor Arc is installed"
        );
    }

    // req: PRJ-004, PRJ-008, PRJ-009, OQ-019, OQ-020, PERF-007, PERF-008
    #[test]
    fn corrupt_selected_v1_fails_closed_then_records_failure_and_rebuilds() {
        let (runtime, scope) = board_runtime("failed-activation");
        append_board_ticket_for_worker(&runtime);
        request_projection(&runtime, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        runtime
            .engine("ticket_board")
            .expect("engine registry")
            .expect("board slot")
            .with_engine_mut(|engine| engine.checkpoint().expect("checkpoint selected view"))
            .expect("engine lock")
            .expect("active engine");
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let ready_control = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read ready control")
            .expect("ready control");
        let published = ready_control
            .published()
            .expect("published generation")
            .clone();
        let selected_snapshot = ServerColumnarProjectionPort::new(Arc::clone(&runtime))
            .observe("ticket_board")
            .expect("capture selected data-bearing V1")
            .snapshot_arc();
        let expected_rows =
            query_snapshot(binding.definition(), &selected_snapshot, &board_query())
                .expect("query selected data-bearing V1");
        let artifact = published.artifact().expect("selected artifact");
        let manifest = controlled_generation_directory(
            &scope.path().join("projections"),
            binding.spec().hash(),
            published.generation(),
        )
        .join(format!("MANIFEST-V1-{}", lower_hex(&artifact.checksum())));
        drop(runtime);
        std::fs::write(&manifest, b"corrupt-selected-manifest").expect("corrupt selected manifest");
        let corrupt_bytes = std::fs::read(&manifest).expect("read corrupt manifest");

        let reopened = reopen_board_runtime(&scope);
        let port = ServerColumnarProjectionPort::new(Arc::clone(&reopened));
        let cold = port.observe("ticket_board").expect("cold observation");
        assert!(!cold.has_published());
        assert!(!crate::columnar_worker::run_one_test_pass(&reopened));
        let slot = reopened
            .engine("ticket_board")
            .expect("engine registry")
            .expect("failed board slot");
        assert_eq!(slot.lifecycle(), Ok(ColumnarSlotLifecycle::Failed));
        assert!(matches!(
            port.observe("ticket_board"),
            Err(ColumnarPortError::Unavailable)
        ));
        assert_eq!(
            std::fs::read(&manifest).expect("reread corrupt manifest"),
            corrupt_bytes,
            "failure must not overwrite or fall back from the selected artifact"
        );
        let degraded = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread degraded V1 control")
            .expect("degraded V1 control");
        assert_eq!(degraded.published(), Some(&published));
        assert!(degraded.servable_generation().is_none());
        assert_eq!(
            degraded.failure().map(|failure| failure.reason()),
            Some(riffdb_storage_api::ColumnarProjectionFailureReasonV1::ArtifactInvalid)
        );
        assert_eq!(reopened.lifecycle_observation().activations(), 1);

        assert!(
            !crate::columnar_worker::run_one_test_pass(&reopened),
            "V1 remains unavailable while the rebuild candidate is only allocated"
        );
        let rebuilding = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread V1 rebuilding control")
            .expect("V1 rebuilding control");
        assert_eq!(
            rebuilding
                .predecessor()
                .map(|generation| generation.generation()),
            Some(published.generation())
        );
        let rebuilt_generation = rebuilding
            .candidate()
            .expect("fresh V1 rebuild candidate")
            .generation();
        assert!(rebuilt_generation > published.generation());
        assert!(
            crate::columnar_worker::run_one_test_pass(&reopened),
            "the fresh V1 candidate rebuilds and publishes"
        );
        let recovered = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread rebuilt V1 control")
            .expect("rebuilt V1 control");
        assert_eq!(
            recovered
                .servable_generation()
                .map(|generation| generation.generation()),
            Some(rebuilt_generation)
        );
        let rebuilt = port.observe("ticket_board").expect("capture rebuilt V1");
        assert!(rebuilt.has_published());
        assert_eq!(
            query_snapshot(
                binding.definition(),
                rebuilt.snapshot().as_ref(),
                &board_query()
            )
            .expect("query rebuilt V1"),
            expected_rows,
            "selected-corrupt V1 recovery preserves the authoritative row population"
        );
    }

    // req: PRJ-004, PRJ-008, PRJ-009, OQ-019, OQ-020, OQ-022
    #[test]
    fn selected_failure_revision_race_and_repeated_no_commit_recover_without_wedge() {
        use crate::columnar_worker::{
            SelectedFailureRecordTestMode, activate_one_test_slot_with_selected_failure_mode,
        };

        for (label, mode, requires_later_pass) in [
            (
                "storage-no-commit",
                SelectedFailureRecordTestMode::StorageFailureBeforeCommit,
                false,
            ),
            (
                "same-selection-state-changed",
                SelectedFailureRecordTestMode::StateChangedBeforeCommit,
                false,
            ),
            (
                "concurrent-candidate-allocation",
                SelectedFailureRecordTestMode::CandidateAllocationBeforeCommit,
                false,
            ),
            (
                "repeated-storage-no-commit",
                SelectedFailureRecordTestMode::RepeatedStorageFailureBeforeCommit,
                true,
            ),
        ] {
            let (runtime, scope) = board_runtime(&format!("selected-failure-{label}"));
            append_board_ticket_for_worker(&runtime);
            request_projection(&runtime, "ticket_board");
            assert!(crate::columnar_worker::run_one_test_pass(&runtime));
            runtime
                .engine("ticket_board")
                .expect("engine registry")
                .expect("board slot")
                .with_engine_mut(|engine| engine.checkpoint().expect("checkpoint selected V1"))
                .expect("engine lock")
                .expect("active engine");
            let binding = runtime
                .control_binding("ticket_board")
                .expect("control binding")
                .clone();
            let ready = runtime
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("read selected V1")
                .expect("selected V1 control");
            let published = ready.published().expect("published V1").clone();
            let artifact = published.artifact().expect("selected artifact");
            let manifest = controlled_generation_directory(
                &scope.path().join("projections"),
                binding.spec().hash(),
                published.generation(),
            )
            .join(format!("MANIFEST-V1-{}", lower_hex(&artifact.checksum())));
            drop(runtime);
            std::fs::write(&manifest, b"corrupt selected V1")
                .expect("corrupt selected V1 manifest");

            let reopened = reopen_board_runtime(&scope);
            let port = ServerColumnarProjectionPort::new(Arc::clone(&reopened));
            assert!(
                !port
                    .observe("ticket_board")
                    .expect("request selected validation")
                    .has_published()
            );
            assert!(
                !activate_one_test_slot_with_selected_failure_mode(&reopened, "ticket_board", mode,),
                "the corrupt selected view is never activated for {label}"
            );
            let mut durable = reopened
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("reread classification result")
                .expect("classification control");
            if requires_later_pass {
                assert_eq!(durable, ready, "both injected writes are proven absent");
                assert_eq!(
                    reopened
                        .engine("ticket_board")
                        .expect("engine registry")
                        .expect("failed activation slot")
                        .lifecycle(),
                    Ok(ColumnarSlotLifecycle::Activating),
                    "only the exact failed activation is re-entered for a later bounded retry"
                );
                assert!(
                    !crate::columnar_worker::run_one_test_pass(&reopened),
                    "the later pass still refuses the corrupt selection while classifying it"
                );
                durable = reopened
                    .storage()
                    .recover_expected_control(binding.spec().source())
                    .expect("reread later classification result")
                    .expect("later classification control");
            }
            assert_eq!(durable.published(), Some(&published), "{label}");
            assert!(durable.servable_generation().is_none(), "{label}");
            if label == "concurrent-candidate-allocation" {
                assert!(
                    durable.highest_generation() > published.generation(),
                    "the real competing Candidate allocation changed the durable control revision"
                );
                assert!(
                    durable.candidate().is_none(),
                    "classification against the reread revision detaches the competing Candidate"
                );
            }
            assert_eq!(
                durable.failure().map(|failure| (
                    failure.target(),
                    failure.reason(),
                    failure.generation(),
                )),
                Some((
                    riffdb_storage_api::ColumnarProjectionFailureTargetV1::Published,
                    riffdb_storage_api::ColumnarProjectionFailureReasonV1::ArtifactInvalid,
                    Some(published.generation()),
                )),
                "the bounded retry durably classifies the unchanged selected artifact for {label}"
            );
            assert!(matches!(
                port.observe("ticket_board"),
                Err(ColumnarPortError::Unavailable)
            ));
        }
    }

    // req: PRJ-004, PRJ-008, PRJ-009, OQ-020, OQ-022
    #[test]
    fn columnar_control_recovery_refuses_corrupt_selected_v2_without_v1_fallback() {
        const CHILD_MODE: &str = "RIFFDB_SERVER_CORRUPT_V2_CRASH_CHILD";
        const CHILD_PATH: &str = "RIFFDB_SERVER_CORRUPT_V2_CRASH_PATH";
        const EXACT_TEST: &str = "columnar_adapter::tests::columnar_control_recovery_refuses_corrupt_selected_v2_without_v1_fallback";
        if std::env::var(CHILD_MODE).as_deref() == Ok("1") {
            let path = PathBuf::from(std::env::var_os(CHILD_PATH).expect("child path"));
            let runtime = open_board_runtime_at(&path);
            request_projection(&runtime, "ticket_board");
            let _ = crate::columnar_worker::run_one_test_pass(&runtime);
            std::process::exit(0);
        }

        let (runtime, scope) = board_runtime("corrupt-selected-v2-no-fallback");
        append_board_ticket_for_worker(&runtime);
        request_projection(&runtime, "ticket_board");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let port = ServerColumnarProjectionPort::new(Arc::clone(&runtime));
        let retained_v1 = port
            .observe("ticket_board")
            .expect("capture selected V1 predecessor")
            .snapshot_arc();
        for _ in 0..3 {
            assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        }
        let binding = runtime
            .control_binding("ticket_board")
            .expect("control binding")
            .clone();
        let selected = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read selected V2")
            .expect("selected V2");
        let published_v2 = selected.published().expect("published V2").clone();
        assert_eq!(published_v2.layout(), ColumnarProjectionLayoutV1::V2);
        let physical = PhysicalGenerationFingerprintV1::compute(binding.definition().fingerprint());
        assert_eq!(
            runtime
                .storage()
                .allocate_same_spec_candidate(&selected, *physical.as_bytes())
                .expect("allocate stale candidate"),
            riffdb_storage_api::ColumnarProjectionControlWriteResultV1::Applied
        );
        let selected = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread selected V2 with stale candidate")
            .expect("selected V2 with stale candidate");
        let stale_candidate = selected.candidate().expect("stale candidate").generation();
        let selected_v2 = port
            .observe("ticket_board")
            .expect("capture selected V2")
            .snapshot_arc();
        let expected_rows = query_snapshot(binding.definition(), &selected_v2, &board_query())
            .expect("query selected data-bearing V2");
        assert!(!Arc::ptr_eq(&retained_v1, &selected_v2));
        let root =
            controlled_source_directory(&scope.path().join("projections"), binding.spec().hash())
                .join(format!(
                    "generation-{:016x}",
                    published_v2.generation().get()
                ))
                .join("ROOT-V1");
        drop(port);
        drop(runtime);
        std::fs::write(&root, b"corrupt selected V2 root").expect("corrupt selected root");
        let corrupt_bytes = std::fs::read(&root).expect("read corrupt root");

        let status =
            std::process::Command::new(std::env::current_exe().expect("server test executable"))
                .arg("--exact")
                .arg(EXACT_TEST)
                .arg("--nocapture")
                .env(CHILD_MODE, "1")
                .env(CHILD_PATH, scope.path())
                .env("RIFFDB_COLUMNAR_SELECTED_FAILURE_ABORT_AT", "after-record")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .expect("spawn selected-corruption failure-record child");
        assert_eq!(
            status.code(),
            None,
            "the first recovery process must abort after the durable failure record"
        );

        let reopened = reopen_board_runtime(&scope);
        let reopened_port = ServerColumnarProjectionPort::new(Arc::clone(&reopened));
        let cold = reopened_port
            .observe("ticket_board")
            .expect("cold observation before validation");
        assert!(!cold.has_published());
        assert!(matches!(
            reopened_port.observe("ticket_board"),
            Ok(observation) if !observation.has_published()
        ));
        let allocated = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread startup-allocated recovery")
            .expect("startup-allocated recovery");
        assert!(allocated.servable_generation().is_none());
        assert_eq!(
            allocated
                .predecessor()
                .map(|generation| generation.generation()),
            Some(published_v2.generation())
        );
        assert!(allocated.candidate().is_some_and(|candidate| {
            candidate.generation() > stale_candidate && candidate.artifact().is_none()
        }));
        assert_eq!(std::fs::read(&root).expect("reread root"), corrupt_bytes);
        assert_eq!(
            query_snapshot(binding.definition(), &retained_v1, &board_query())
                .expect("query retained V1 capability"),
            expected_rows
        );
        assert!(
            !Arc::ptr_eq(&retained_v1, &selected_v2),
            "a still-live predecessor capability never becomes fallback authority"
        );

        drop(reopened_port);
        drop(reopened);
        let reopened = reopen_board_runtime(&scope);
        let reopened_port = ServerColumnarProjectionPort::new(Arc::clone(&reopened));
        assert!(
            !reopened_port
                .observe("ticket_board")
                .expect("request repeated-open recovery")
                .has_published()
        );
        assert!(
            crate::columnar_worker::run_one_test_pass(&reopened),
            "a second cold open prepares the startup-allocated recovery candidate"
        );
        let rebuilding = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread allocated recovery")
            .expect("recovery control");
        assert!(rebuilding.servable_generation().is_none());
        assert_eq!(
            rebuilding.predecessor().map(|value| value.generation()),
            Some(published_v2.generation())
        );
        let recovery_generation = rebuilding
            .candidate()
            .expect("fresh recovery candidate")
            .generation();
        assert!(recovery_generation > published_v2.generation());
        assert!(
            recovery_generation > stale_candidate,
            "published-corruption recovery allocates from Predecessor and never reuses the stale candidate"
        );
        let prepared_recovery = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread prepared recovery")
            .expect("prepared recovery control");
        assert!(
            prepared_recovery.candidate().is_some_and(|candidate| {
                candidate.generation() == recovery_generation && candidate.artifact().is_some()
            }),
            "recovery candidate was not prepared: {prepared_recovery:?}"
        );
        append_board_ticket_for_worker_at(
            &reopened,
            CommitSequence::new(2).expect("second sequence"),
            0x43,
        );
        let _ = crate::columnar_worker::run_one_test_pass(&reopened);
        let replaced_recovery = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread replacement recovery")
            .expect("replacement recovery control");
        assert!(replaced_recovery.servable_generation().is_none());
        assert_eq!(
            replaced_recovery
                .predecessor()
                .map(|value| value.generation()),
            Some(published_v2.generation())
        );
        let replacement_generation = replaced_recovery
            .candidate()
            .expect("replacement recovery candidate")
            .generation();
        assert!(replacement_generation > recovery_generation);
        assert!(
            replaced_recovery
                .candidate()
                .is_some_and(|candidate| candidate.artifact().is_none()),
            "head movement allocates a new unprepared recovery candidate"
        );
        let _ = crate::columnar_worker::run_one_test_pass(&reopened);
        let prepared_replacement = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread prepared replacement recovery")
            .expect("prepared replacement recovery control");
        assert!(prepared_replacement.candidate().is_some_and(|candidate| {
            candidate.generation() == replacement_generation && candidate.artifact().is_some()
        }));
        let _ = crate::columnar_worker::run_one_test_pass(&reopened);
        let recovered = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread recovered control")
            .expect("recovered control");
        assert_eq!(
            recovered
                .servable_generation()
                .map(|value| value.generation()),
            Some(replacement_generation)
        );
        assert_eq!(
            recovered.servable_generation().map(|value| value.layout()),
            Some(ColumnarProjectionLayoutV1::V2)
        );
        let rebuilt = reopened_port
            .observe("ticket_board")
            .expect("capture rebuilt V2");
        assert!(rebuilt.has_published());
        let QueryResult::Rows(rebuilt_rows) = query_snapshot(
            binding.definition(),
            rebuilt.snapshot().as_ref(),
            &board_query(),
        )
        .expect("query rebuilt V2") else {
            panic!("board query returns rows");
        };
        let QueryResult::Rows(expected_rows) = expected_rows else {
            panic!("board query returns rows");
        };
        assert_eq!(rebuilt_rows.rows.len(), expected_rows.rows.len() + 1);
        assert!(
            expected_rows
                .rows
                .iter()
                .all(|expected| rebuilt_rows.rows.contains(expected)),
            "every authoritative row selected before corruption survives the rebuilt V2"
        );

        for (label, abort_at, unknown) in [
            ("after-cas", "after-cas", false),
            ("unknown-after-cas", "after-cas", true),
            ("after-reread", "after-reread", false),
            ("after-view-install", "after-view-install", false),
        ] {
            let (runtime, crash_scope) = board_runtime(&format!("corrupt-after-{label}"));
            let publication = prepare_two_partition_v2_publication(&runtime, &crash_scope);
            let predecessor = publication
                .expected
                .published()
                .expect("V1 predecessor")
                .clone();
            let candidate = publication
                .expected
                .candidate()
                .expect("V2 candidate")
                .clone();
            let binding = publication.binding.clone();
            drop(publication);
            drop(runtime);
            let mut child = std::process::Command::new(
                std::env::current_exe().expect("server test executable"),
            );
            child
                .arg("--exact")
                .arg(EXACT_TEST)
                .arg("--nocapture")
                .env(CHILD_MODE, "1")
                .env(CHILD_PATH, crash_scope.path())
                .env("RIFFDB_COLUMNAR_PUBLICATION_ABORT_AT", abort_at)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            if unknown {
                child.env(
                    "RIFFDB_COLUMNAR_PUBLICATION_RESULT",
                    "unknown-after-applied",
                );
            }
            let status = child.status().expect("spawn selected-V2 crash child");
            assert!(!status.success(), "child must abort at {label}");

            let root = controlled_source_directory(
                &crash_scope.path().join("projections"),
                binding.spec().hash(),
            )
            .join(format!("generation-{:016x}", candidate.generation().get()))
            .join("ROOT-V1");
            assert!(
                root.is_file(),
                "{label}: durable CAS selected the complete V2 root"
            );
            std::fs::write(&root, b"corrupt selected V2 after crash")
                .expect("corrupt crash-selected root");
            let reopened = open_board_runtime_at(crash_scope.path());
            let durable = reopened
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("reread crash-selected V2")
                .expect("crash-selected V2");
            let selected_pointer = durable.published().expect("published V2").clone();
            assert_eq!(
                selected_pointer.generation(),
                candidate.generation(),
                "{label}"
            );
            assert_eq!(selected_pointer.layout(), candidate.layout(), "{label}");
            assert_eq!(selected_pointer.frontier(), candidate.frontier(), "{label}");
            assert_eq!(selected_pointer.artifact(), candidate.artifact(), "{label}");
            assert!(
                controlled_generation_directory(
                    &crash_scope.path().join("projections"),
                    binding.spec().hash(),
                    predecessor.generation(),
                )
                .exists(),
                "{label}: a physical V1 predecessor remains available to catch accidental fallback"
            );
            let crash_port = ServerColumnarProjectionPort::new(Arc::clone(&reopened));
            let cold = crash_port
                .observe("ticket_board")
                .expect("cold selected-V2 observation");
            assert!(!cold.has_published(), "{label}");
            assert!(
                !crate::columnar_worker::run_one_test_pass(&reopened),
                "{label}"
            );
            assert!(matches!(
                crash_port.observe("ticket_board"),
                Err(ColumnarPortError::Unavailable)
            ));
            let degraded = reopened
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("reread degraded crash selection")
                .expect("degraded crash selection");
            assert_eq!(degraded.published(), Some(&selected_pointer), "{label}");
            assert!(degraded.servable_generation().is_none(), "{label}");
        }
    }

    // req: PRJ-002, PRJ-004, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
    #[test]
    fn columnar_v2_crash_reopens_exactly_one_published_generation() {
        const MODE: &str = "RIFFDB_SERVER_V2_CRASH_MODE";
        const PATH: &str = "RIFFDB_SERVER_V2_CRASH_PATH";
        const BOUNDARY: &str = "RIFFDB_SERVER_V2_CRASH_BOUNDARY";
        const EXACT_TEST: &str =
            "columnar_adapter::tests::columnar_v2_crash_reopens_exactly_one_published_generation";

        if let Ok(mode) = std::env::var(MODE) {
            let path = PathBuf::from(std::env::var_os(PATH).expect("child path"));
            match mode.as_str() {
                "build" => {
                    let (definition_runtime, _definition_scope) = board_runtime("crash-definition");
                    let definition = definition_runtime
                        .control_binding("ticket_board")
                        .expect("definition binding")
                        .definition()
                        .clone();
                    let boundary = match std::env::var(BOUNDARY).expect("build boundary").as_str() {
                        "before-segment-sync" => ColumnarTestBoundary::BeforeV2SegmentSync,
                        "after-segment-rename" => ColumnarTestBoundary::AfterV2SegmentRename,
                        "after-manifest-rename" => ColumnarTestBoundary::AfterV2ManifestRename,
                        "after-root-rename" => ColumnarTestBoundary::AfterV2RootRename,
                        "after-generation-rename" => ColumnarTestBoundary::AfterV2GenerationRename,
                        other => panic!("unknown build boundary {other}"),
                    };
                    let controller = ColumnarTestController::new();
                    controller.arm_abort_at(boundary);
                    let (snapshot, _) = two_partition_snapshot();
                    let _ = ValidatedColumnarV2Generation::prepare_with_controller(
                        &path,
                        definition,
                        1,
                        ProjectionGeneration::new(7).expect("generation"),
                        FrontierPosition::BeforeFirst,
                        &snapshot,
                        &controller,
                    );
                }
                "publication" | "compaction" => {
                    let runtime = open_board_runtime_at(&path);
                    request_projection(&runtime, "ticket_board");
                    let _ = crate::columnar_worker::run_one_test_pass(&runtime);
                }
                "reclaim" => {
                    let boundary = match std::env::var(BOUNDARY).expect("reclaim boundary").as_str()
                    {
                        "before-reclaim" => ColumnarTestBoundary::BeforeV2GenerationReclaim,
                        "after-reclaim" => ColumnarTestBoundary::AfterV2GenerationReclaim,
                        other => panic!("unknown reclaim boundary {other}"),
                    };
                    let controller = ColumnarTestController::new();
                    controller.arm_abort_at(boundary);
                    let _ = ValidatedColumnarV2Generation::reclaim_with_controller(
                        &path,
                        ProjectionGeneration::new(9).expect("retired generation"),
                        &controller,
                    );
                }
                other => panic!("unknown crash mode {other}"),
            }
            std::process::exit(0);
        }

        for boundary in [
            "before-segment-sync",
            "after-segment-rename",
            "after-manifest-rename",
            "after-root-rename",
            "after-generation-rename",
        ] {
            let source_directory = adapter_scope(&format!("process-build-{boundary}"));
            let status = std::process::Command::new(
                std::env::current_exe().expect("server test executable"),
            )
            .arg("--exact")
            .arg(EXACT_TEST)
            .arg("--nocapture")
            .env(MODE, "build")
            .env(PATH, source_directory.path())
            .env(BOUNDARY, boundary)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("spawn build-crash child");
            assert!(!status.success(), "child must abort at {boundary}");
            let (definition_runtime, _definition_scope) = board_runtime("recover-definition");
            let definition = definition_runtime
                .control_binding("ticket_board")
                .expect("definition binding")
                .definition()
                .clone();
            let (snapshot, organizations) = two_partition_snapshot();
            let recovered = ValidatedColumnarV2Generation::prepare(
                source_directory.path(),
                definition,
                1,
                ProjectionGeneration::new(7).expect("generation"),
                FrontierPosition::BeforeFirst,
                &snapshot,
            )
            .unwrap_or_else(|error| panic!("recover {boundary}: {error}"));
            assert_eq!(recovered.root().partitions().len(), 2);
            assert!(
                organizations.iter().all(|organization| recovered
                    .snapshot()
                    .merged_org(organization)
                    .len()
                    == 1)
            );
        }

        for (label, abort_at, unknown, selected_layout) in [
            (
                "before-cas",
                "before-cas",
                false,
                ColumnarProjectionLayoutV1::V1,
            ),
            (
                "after-cas",
                "after-cas",
                false,
                ColumnarProjectionLayoutV1::V2,
            ),
            (
                "unknown-after-cas",
                "after-cas",
                true,
                ColumnarProjectionLayoutV1::V2,
            ),
            (
                "after-reread",
                "after-reread",
                false,
                ColumnarProjectionLayoutV1::V2,
            ),
            (
                "after-view-install",
                "after-view-install",
                false,
                ColumnarProjectionLayoutV1::V2,
            ),
        ] {
            let (runtime, scope) = board_runtime(&format!("process-publication-{label}"));
            let publication = prepare_two_partition_v2_publication(&runtime, &scope);
            let candidate_generation = publication
                .expected
                .candidate()
                .expect("prepared candidate")
                .generation();
            let predecessor_generation = publication
                .expected
                .published()
                .expect("published predecessor")
                .generation();
            drop(publication);
            drop(runtime);
            let mut child = std::process::Command::new(
                std::env::current_exe().expect("server test executable"),
            );
            child
                .arg("--exact")
                .arg(EXACT_TEST)
                .arg("--nocapture")
                .env(MODE, "publication")
                .env(PATH, scope.path())
                .env("RIFFDB_COLUMNAR_PUBLICATION_ABORT_AT", abort_at)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            if unknown {
                child.env(
                    "RIFFDB_COLUMNAR_PUBLICATION_RESULT",
                    "unknown-after-applied",
                );
            }
            let status = child.status().expect("spawn publication-crash child");
            assert!(!status.success(), "child must abort at {label}");

            let reopened = open_board_runtime_at(scope.path());
            let binding = reopened
                .control_binding("ticket_board")
                .expect("reopened binding")
                .clone();
            let durable = reopened
                .storage()
                .recover_expected_control(binding.spec().source())
                .expect("reread crash result")
                .expect("durable crash result");
            let selected = durable
                .servable_generation()
                .expect("one durable selection");
            assert_eq!(selected.layout(), selected_layout, "{label}");
            assert_eq!(
                selected.generation(),
                if selected_layout == ColumnarProjectionLayoutV1::V2 {
                    candidate_generation
                } else {
                    predecessor_generation
                },
                "{label}"
            );
            request_projection(&reopened, "ticket_board");
            assert!(crate::columnar_worker::activate_one_test_slot(
                &reopened,
                "ticket_board"
            ));
            assert_eq!(
                reopened
                    .engine("ticket_board")
                    .expect("engine registry")
                    .expect("selected slot")
                    .generation(),
                Some(selected.generation()),
                "{label}: reopen installs exactly the durable selection"
            );
        }

        let (runtime, compaction_scope) = board_runtime("process-compaction-publication");
        request_projection(&runtime, "ticket_board");
        for _ in 0..4 {
            assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        }
        let binding = runtime
            .control_binding("ticket_board")
            .expect("compaction binding")
            .clone();
        let selected = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read selected before compaction")
            .expect("selected before compaction");
        let predecessor_generation = selected.published().expect("selected V2").generation();
        let physical = PhysicalGenerationFingerprintV1::compute(binding.definition().fingerprint());
        assert_eq!(
            runtime
                .storage()
                .allocate_same_spec_candidate(&selected, *physical.as_bytes())
                .expect("allocate compaction candidate"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let prepared_compaction = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read prepared compaction")
            .expect("prepared compaction");
        let compacted_generation = prepared_compaction
            .candidate()
            .expect("compaction candidate")
            .generation();
        drop(runtime);
        let status =
            std::process::Command::new(std::env::current_exe().expect("server test executable"))
                .arg("--exact")
                .arg(EXACT_TEST)
                .arg("--nocapture")
                .env(MODE, "compaction")
                .env(PATH, compaction_scope.path())
                .env("RIFFDB_COLUMNAR_PUBLICATION_ABORT_AT", "after-view-install")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .expect("spawn compaction-crash child");
        assert!(
            !status.success(),
            "compaction child must abort after view install"
        );
        let reopened = open_board_runtime_at(compaction_scope.path());
        let durable = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("reread compacted selection")
            .expect("compacted selection");
        assert_eq!(
            durable.published().map(|value| value.generation()),
            Some(compacted_generation)
        );
        assert!(compacted_generation > predecessor_generation);
        request_projection(&reopened, "ticket_board");
        assert!(crate::columnar_worker::activate_one_test_slot(
            &reopened,
            "ticket_board"
        ));

        for boundary in ["before-reclaim", "after-reclaim"] {
            let source_directory = adapter_scope(&format!("process-{boundary}"));
            let (definition_runtime, _definition_scope) = board_runtime("reclaim-definition");
            let definition = definition_runtime
                .control_binding("ticket_board")
                .expect("definition binding")
                .definition()
                .clone();
            let (snapshot, _) = two_partition_snapshot();
            let retired = ValidatedColumnarV2Generation::prepare(
                source_directory.path(),
                definition.clone(),
                1,
                ProjectionGeneration::new(9).expect("retired generation"),
                FrontierPosition::BeforeFirst,
                &snapshot,
            )
            .expect("prepare retired generation");
            let selected = ValidatedColumnarV2Generation::prepare(
                source_directory.path(),
                definition.clone(),
                1,
                ProjectionGeneration::new(10).expect("selected generation"),
                FrontierPosition::BeforeFirst,
                &snapshot,
            )
            .expect("prepare selected generation");
            let selected_identity = selected.artifact_identity();
            let selected_physical = selected.root().physical_generation_fingerprint();
            drop(retired);
            drop(selected);
            let status = std::process::Command::new(
                std::env::current_exe().expect("server test executable"),
            )
            .arg("--exact")
            .arg(EXACT_TEST)
            .arg("--nocapture")
            .env(MODE, "reclaim")
            .env(PATH, source_directory.path())
            .env(BOUNDARY, boundary)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("spawn reclaim-crash child");
            assert!(!status.success(), "child must abort at {boundary}");
            let selected = ValidatedColumnarV2Generation::open(
                source_directory.path(),
                definition,
                1,
                ProjectionGeneration::new(10).expect("selected generation"),
                FrontierPosition::BeforeFirst,
                selected_identity,
                selected_physical,
            )
            .unwrap_or_else(|error| panic!("selected reopen after {boundary}: {error}"));
            assert_eq!(selected.root().generation().get(), 10);
            ValidatedColumnarV2Generation::reclaim(
                source_directory.path(),
                ProjectionGeneration::new(9).expect("retired generation"),
            )
            .unwrap_or_else(|error| panic!("retry reclaim after {boundary}: {error}"));
            assert!(
                !source_directory
                    .path()
                    .join("generation-0000000000000009")
                    .exists()
            );
        }
    }

    #[test]
    fn production_vector_contract_registers_exact_symbolic_projection_automatically() {
        let (runtime, _scope) = production_vector_runtime("production-vector");
        assert_eq!(
            runtime.names().expect("runtime names"),
            ["Document.embedding"]
        );
        let slot = runtime
            .engine("Document.embedding")
            .expect("engine registry")
            .expect("compiler-owned vector projection");
        assert_eq!(slot.definition().name(), "Document.embedding");
        assert_eq!(slot.definition().entity_name(), "Document");
        assert_eq!(slot.definition().projected_fields().len(), 2);
    }

    #[test]
    fn production_vector_snapshot_generation_becomes_ready_only_after_checkpoint() {
        let (runtime, _scope) = production_vector_runtime("production-vector-rebuild");
        let binding = runtime
            .control_bindings()
            .into_iter()
            .find(|binding| binding.name() == "Document.embedding")
            .expect("vector control binding");
        let building = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read building control")
            .expect("building control");
        assert_eq!(
            building.lifecycle(),
            riffdb_storage_api::ColumnarProjectionLifecycleV1::Building
        );
        assert_eq!(
            building.retention_frontier(),
            Some(FrontierPosition::BeforeFirst)
        );

        request_projection(&runtime, "Document.embedding");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));

        let ready = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read ready control")
            .expect("ready control");
        assert_eq!(
            ready.lifecycle(),
            riffdb_storage_api::ColumnarProjectionLifecycleV1::Ready
        );
        let published = ready.published().expect("published V1");
        assert!(published.artifact().is_some());
        let engine = runtime
            .engine("Document.embedding")
            .expect("engine registry")
            .expect("ready vector engine");
        assert_eq!(
            engine
                .with_engine(|engine| engine.durable_frontier().position())
                .expect("engine state")
                .expect("active engine"),
            published.frontier()
        );
    }

    // req: PERF-007, PERF-008, PRJ-008, PRJ-009, OQ-020, OQ-022
    #[test]
    fn production_vector_hot_path_performs_no_durable_control_reread() {
        let (runtime, _scope) = production_vector_runtime("production-vector-hot-control");
        request_projection(&runtime, "Document.embedding");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let binding = runtime
            .control_binding("Document.embedding")
            .expect("vector binding");
        let vector_field = binding
            .definition()
            .projected_fields()
            .iter()
            .zip(binding.definition().projected_types())
            .find_map(|(field, value_type)| {
                (value_type.tag() == ValueTypeTag::Vector).then_some(*field)
            })
            .expect("vector field");
        let organization = uuid_bytes(0x41);
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition
            .push_uuid(&organization)
            .expect("partition organization");
        let request = VectorProjectionRequest::new(
            "Document.embedding".to_owned(),
            ContractLineage::new("VectorBoard").expect("lineage"),
            partition.finish().expect("partition"),
            CanonicalValue::Uuid(organization),
            binding.definition().entity_type_id(),
            vector_field,
            EmbeddingMetadata::new("embed-v1", "2026-08-21").expect("model"),
            0,
            CanonicalVector::new(vec![1.0, 0.0, 0.0, 0.0]).expect("query vector"),
            1,
            DistanceMetric::Cosine,
            Vec::new(),
            None,
            None,
            None,
        );
        crate::storage::reset_columnar_control_recovery_reads();
        assert!(matches!(
            VectorProjectionPort::execute(&ServerColumnarProjectionPort::new(runtime), request),
            Err(VectorProjectionPortError::Building)
        ));
        assert_eq!(
            crate::storage::columnar_control_recovery_reads(),
            0,
            "the real vector port must use the gate-installed generation authority"
        );
    }

    // req: PRJ-002, PRJ-004, PRJ-006, PRJ-007, PRJ-010
    #[test]
    fn fresh_common_v1_uses_only_schema_bound_paths() {
        let (runtime, scope) = production_vector_runtime("legacy-inert");
        let binding = runtime
            .control_bindings()
            .into_iter()
            .next()
            .expect("vector binding");
        let legacy_directory = scope.path().join("projections/Document.embedding");
        std::fs::create_dir_all(&legacy_directory).expect("legacy name directory");
        std::fs::write(legacy_directory.join("MANIFEST"), b"not selected")
            .expect("legacy manifest fixture");

        let initial = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read common control")
            .expect("fresh common control");
        assert_eq!(initial.highest_generation(), ProjectionGeneration::first());
        assert_eq!(
            initial.retention_frontier(),
            Some(FrontierPosition::BeforeFirst)
        );
        request_projection(&runtime, "Document.embedding");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let ready = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read common result")
            .expect("common result");
        assert_eq!(
            ready.lifecycle(),
            riffdb_storage_api::ColumnarProjectionLifecycleV1::Ready
        );
        assert!(legacy_directory.join("MANIFEST").is_file());
    }

    // req: PRJ-005, PRJ-007, PRJ-010, OQ-021, OQ-022
    #[test]
    fn columnar_projection_symbolic_name_resolves_one_schema_bound_source() {
        let (runtime, scope) = board_runtime("symbolic-binding");
        let original = runtime
            .control_bindings()
            .into_iter()
            .next()
            .expect("binding");
        let renamed = ConfiguredProjection::for_test(
            "renamed_board",
            "Ticket",
            &["status", "title"],
            "organization_id",
        );
        let renamed_bindings = prepare_columnar_control_foundation(
            runtime.storage(),
            std::slice::from_ref(&renamed),
            runtime.projections_root(),
            runtime.history_incarnation(),
        )
        .expect("name-only replacement");
        assert_eq!(renamed_bindings[0].spec.source(), original.spec.source());
        assert_eq!(renamed_bindings[0].spec.hash(), original.spec.hash());

        let duplicate = ConfiguredProjection::for_test(
            "second_alias",
            "Ticket",
            &["status", "title"],
            "organization_id",
        );
        let error = prepare_columnar_control_foundation(
            runtime.storage(),
            &[renamed, duplicate],
            runtime.projections_root(),
            runtime.history_incarnation(),
        )
        .expect_err("two aliases for one source refuse");
        assert_eq!(error.kind, ColumnarRegistrationErrorKind::DuplicateName);
        assert!(!scope.path().join("projections/renamed_board").exists());

        let unsafe_name =
            ConfiguredProjection::for_test("../unsafe", "Ticket", &["status"], "organization_id");
        assert!(
            prepare_columnar_control_foundation(
                runtime.storage(),
                &[unsafe_name],
                runtime.projections_root(),
                runtime.history_incarnation(),
            )
            .is_err()
        );
    }

    // req: PRJ-004, PRJ-005, PRJ-006, PRJ-007, PRJ-010
    #[test]
    fn columnar_spec_hash_change_forces_closed_rebuild() {
        let (runtime, _scope) = production_vector_runtime("spec-drift");
        request_projection(&runtime, "Document.embedding");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let binding = runtime
            .control_bindings()
            .into_iter()
            .next()
            .expect("binding");
        let ready = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read control")
            .expect("ready control");
        let changed_bundle = riffdb_contract_compiler::compile_contract_source(
            &PRODUCTION_VECTOR_CONTRACT
                .replace("replay_age_seconds 86400", "replay_age_seconds 86401"),
        )
        .expect("changed replay contract");
        let entity = changed_bundle.schema().entities().first().expect("entity");
        let production = changed_bundle
            .schema()
            .vector_production_specs()
            .first()
            .expect("vector spec");
        let changed_registration =
            resolve_vector_registration(&changed_bundle, entity, "Document.embedding")
                .expect("changed registration");
        let changed_spec = ColumnarProjectionSpecV1::for_vector(
            &changed_registration,
            production.field(),
            &changed_bundle,
        )
        .expect("changed compiler-bound spec");
        assert_eq!(changed_spec.source(), binding.spec().source());
        assert_ne!(changed_spec.hash(), binding.spec().hash());
        assert_eq!(
            runtime
                .storage()
                .allocate_unservable_rebuild_candidate(
                    &ready,
                    changed_spec.definition_fingerprint(),
                    changed_spec.hash(),
                    changed_spec.replay_limits(),
                    ColumnarProjectionLayoutV1::V1,
                    None,
                )
                .expect("allocate drift rebuild"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let rebuilding = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read rebuild")
            .expect("rebuild control");
        assert!(rebuilding.published().is_none());
        assert!(rebuilding.predecessor().is_some());
        assert!(rebuilding.servable_generation().is_none());
        assert_eq!(
            rebuilding.candidate().expect("candidate").frontier(),
            FrontierPosition::BeforeFirst
        );
    }

    // req: PRJ-004, PRJ-006, PRJ-007, PRJ-010
    #[test]
    fn common_v1_selection_reopens_only_its_control_bound_artifact() {
        let (runtime, scope) = production_vector_runtime("production-vector-reopen");
        let binding = runtime
            .control_bindings()
            .into_iter()
            .next()
            .expect("vector control binding");
        request_projection(&runtime, "Document.embedding");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let ready = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read ready generation")
            .expect("ready generation");
        let published = ready.published().expect("published generation").clone();
        drop(runtime);

        let store = RedbStore::open(scope.path().join("db.redb")).expect("reopen database");
        let ports = open_operational(store);
        let storage =
            SharedRedbOperationalPorts::new(ports, None).expect("share reopened operational ports");
        let reopened = ColumnarRuntime::open(
            storage,
            &[],
            &scope.path().join("projections"),
            1,
            [0x5a; 16],
        )
        .expect("reopen vector runtime");
        let persisted = reopened
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read persisted control")
            .expect("persisted control");
        assert_eq!(persisted, ready);
        let engine = reopened
            .engine("Document.embedding")
            .expect("engine registry")
            .expect("recovered engine");
        assert_eq!(engine.generation(), Some(published.generation()));
        request_projection(&reopened, "Document.embedding");
        assert!(crate::columnar_worker::run_one_test_pass(&reopened));
        assert_eq!(
            engine
                .with_engine(|engine| engine.durable_frontier().position())
                .expect("engine state")
                .expect("active engine"),
            published.frontier()
        );
    }

    #[test]
    fn first_contract_activation_requires_reopen_before_admitting_a_new_source() {
        let scope = adapter_scope("dynamic-production-vector");
        let database_path = scope.path().join("db.redb");
        let projections_root = scope.path().join("projections");
        std::fs::create_dir_all(&projections_root).expect("create projections root");
        let mut store = RedbStore::open(&database_path).expect("create adapter database");
        let database_id = DatabaseId::from_bytes(uuid_bytes(0x13)).expect("database id");
        assert_eq!(
            store
                .initialize_database(database_id)
                .expect("initialize adapter database"),
            DatabaseInitializationResult::Installed(database_id)
        );
        let ports = open_operational(store);
        let storage =
            SharedRedbOperationalPorts::new(ports, None).expect("share operational ports");
        let runtime = ColumnarRuntime::open(storage.clone(), &[], &projections_root, 1, [0x5a; 16])
            .expect("open empty runtime");
        assert!(runtime.names().expect("empty names").is_empty());

        let checked = ValidatedContractBundle::from_compiler_bundle(
            riffdb_contract_compiler::compile_contract_source(PRODUCTION_VECTOR_CONTRACT)
                .expect("compile production vector contract"),
        )
        .expect("validate production vector contract");
        let mut administration = storage.clone();
        let activation = CatalogAdministrationRepository::activate_catalog(
            &mut administration,
            &CatalogActivationIntentV1::new(
                None,
                checked.to_stored().expect("encode adapter bundle"),
                RequestId::from_bytes(uuid_bytes(0x23)).expect("request id"),
                AuditPrincipalV1::new(
                    ActorId::new("adapter-test").expect("actor"),
                    ActorKind::Human,
                    CapabilityId::from_bytes(uuid_bytes(0x33)).expect("capability"),
                    std::num::NonZeroU64::MIN,
                ),
                Timestamp::new(1_700_200_002, 0).expect("timestamp"),
                None,
            ),
        )
        .expect("activate catalog after runtime startup");
        assert!(matches!(
            activation,
            CatalogActivationResult::Activated { .. }
        ));

        assert!(runtime.names().expect("runtime names").is_empty());
        drop(runtime);

        let reopened = ColumnarRuntime::open(storage, &[], &projections_root, 1, [0x5a; 16])
            .expect("reopen admits source behind BeforeFirst fence");
        assert_eq!(
            reopened.names().expect("runtime names"),
            ["Document.embedding"]
        );
        assert!(
            reopened
                .engine("Document.embedding")
                .expect("engine registry")
                .is_some()
        );
    }

    #[test]
    fn bounded_vector_freshness_uses_elapsed_time_across_second_boundaries() {
        let frontier = Timestamp::new(1, 900_000_000).expect("frontier time");
        let head = Timestamp::new(2, 100_000_001).expect("head time");
        assert_eq!(
            trusted_timestamp_lag_ms(frontier, head).expect("trusted lag"),
            201
        );
        assert_eq!(
            trusted_timestamp_lag_ms(head, frontier),
            Err(VectorProjectionPortError::Integrity)
        );
    }

    /// Transcript (b) — serve-under-lock is impossible, proven live in both
    /// directions against the REAL adapter and engine:
    ///
    /// 1. queries against a held [`ColumnarObservation`] complete WHILE the
    ///    worker path holds the engine lock (query execution needs no engine
    ///    access), and
    /// 2. the worker path acquires the engine lock and completes a full
    ///    apply + checkpoint pass WHILE an observation is held and being
    ///    queried (observations retain no lock).
    ///
    /// A re-ordered implementation that served from the engine under lock (or
    /// returned observations retaining the lock) deadlocks one of the two
    /// bounded handshakes below and fails on `recv_timeout`.
    #[test]
    fn held_observation_and_running_queries_never_block_apply_or_checkpoint() {
        let (runtime, _scope) = board_runtime("lockfree");
        let port = ServerColumnarProjectionPort::new(Arc::clone(&runtime));
        let cold = port
            .observe("ticket_board")
            .expect("cold board observation");
        assert!(!cold.has_published());
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let observation = port.observe("ticket_board").expect("board observation");
        assert!(observation.has_published());

        // Continuous query load against the held observation.
        let query_cycles = Arc::new(AtomicU64::new(0));
        let keep_querying = Arc::new(AtomicBool::new(true));
        let query_thread = {
            let observation = observation.clone();
            let query_cycles = Arc::clone(&query_cycles);
            let keep_querying = Arc::clone(&keep_querying);
            thread::spawn(move || {
                while keep_querying.load(Ordering::Acquire) {
                    let result = query_snapshot(
                        observation.definition(),
                        observation.snapshot(),
                        &board_query(),
                    )
                    .expect("query over held observation");
                    assert!(matches!(result, QueryResult::Rows(_)));
                    query_cycles.fetch_add(1, Ordering::AcqRel);
                }
            })
        };

        // Worker pass that holds the engine lock across a handshake window.
        let (locked_sender, locked_receiver) = mpsc::channel();
        let (proceed_sender, proceed_receiver) = mpsc::channel();
        let (done_sender, done_receiver) = mpsc::channel();
        let worker = {
            let runtime = Arc::clone(&runtime);
            thread::spawn(move || {
                let slot = runtime
                    .engine("ticket_board")
                    .expect("engine registry")
                    .expect("board slot");
                slot.with_engine_mut(|engine| {
                    locked_sender.send(()).expect("report lock acquisition");
                    proceed_receiver
                        .recv_timeout(Duration::from_secs(30))
                        .expect("queries must complete while the engine lock is held");
                    engine
                        .apply_available(runtime.apply_source())
                        .expect("apply under held observation");
                    match engine.checkpoint() {
                        Ok(_) => {}
                        Err(error) => assert!(is_holdback_active(&error), "checkpoint failed"),
                    }
                })
                .expect("worker engine lock")
                .expect("active worker engine");
                done_sender.send(()).expect("report pass completion");
            })
        };

        // Direction 2: the worker acquired the lock while the observation is
        // held and queried.
        locked_receiver
            .recv_timeout(Duration::from_secs(30))
            .expect("a held observation must not retain the engine lock");
        // Direction 1: at least two full query cycles complete while the
        // engine lock is held by the worker.
        let baseline = query_cycles.load(Ordering::Acquire);
        let spin_deadline = std::time::Instant::now() + Duration::from_secs(30);
        while query_cycles.load(Ordering::Acquire) < baseline + 2 {
            assert!(
                std::time::Instant::now() < spin_deadline,
                "queries over a held observation stalled while the engine lock was held"
            );
            thread::yield_now();
        }
        proceed_sender.send(()).expect("release the worker");
        done_receiver
            .recv_timeout(Duration::from_secs(30))
            .expect("apply/checkpoint must complete while queries execute");
        worker.join().expect("worker thread");
        keep_querying.store(false, Ordering::Release);
        query_thread.join().expect("query thread");

        // The held observation stays valid after the pass.
        let result = query_snapshot(
            observation.definition(),
            observation.snapshot(),
            &board_query(),
        )
        .expect("query after apply pass");
        assert!(matches!(result, QueryResult::Rows(_)));

        drop(port);
        drop(observation);
        drop(runtime);
    }

    #[test]
    fn registration_error_names_the_projection() {
        let error = ColumnarRegistrationError::no_active_catalog("ticket_board");
        assert_eq!(error.projection_name(), "ticket_board");
        assert!(error.to_string().contains("ticket_board"));
    }

    #[test]
    fn holdback_active_is_recognized_for_retry() {
        let error = ColumnarError::Checkpoint(riffdb_columnar::CheckpointError::HoldbackActive {
            published: FrontierPosition::BeforeFirst,
            processed: FrontierPosition::BeforeFirst,
            deferred: 1,
        });
        assert!(is_holdback_active(&error));
    }
}
