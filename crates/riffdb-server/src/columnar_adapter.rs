#![expect(
    clippy::expect_used,
    reason = "validated columnar batches retain the projected entity and generation selected for apply"
)]

//! Server-side columnar apply source and published projection port.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

use riffdb_catalog::ActiveCatalogSnapshot;
use riffdb_columnar::{
    ColumnarEngine, ColumnarError, ColumnarOutcome, ColumnarProjectionDefinition,
    ColumnarProjectionSpecV1, NearestCandidate, NearestCandidateAdmission,
    NearestQueryAdmissionError, NearestQueryRequest, OpenOptions, QueryBudget, QueryError,
    RegisteredDefinition,
};
use riffdb_contract_ir::{ContractBundle, ExpressionKind, ValueType, ValueTypeTag};
use riffdb_service::{
    ColumnarLifecycle, ColumnarNotifier, ColumnarObservation, ColumnarPortError,
    ColumnarProjectionPort, VectorProjectionPort, VectorProjectionPortError,
    VectorProjectionRequest, VectorProjectionResult,
};
use riffdb_storage_api::{
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, ColumnarProjectionControlRepository,
    ColumnarProjectionControlWriteResultV1, ColumnarProjectionLayoutV1, CommitScanPageV1,
    CommitScanRequest, EntityTarget, FreshColumnarProjectionControlV1, IdempotencyIdentity,
    StorageError, StorageErrorKind, StorageScanLimit, StoredColumnarProjectionControlV1,
    StoredCommitRecordV1, StoredDurableEventV1, StoredEntityRecordV1, StoredOutcomeV1,
    StoredProvenanceRecordV1,
};
use riffdb_types::{
    CommitSequence, EntityKey, EventId, FieldId, FrontierPosition, ProjectionFrontier,
    ProjectionGeneration, ProvenanceId,
};

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
    definition: RegisteredDefinition,
    activation: ColumnarActivationSpec,
    cold_snapshot: Arc<riffdb_columnar::ColumnarSnapshot>,
    cold_frontier: FrontierPosition,
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
            activation: ColumnarActivationSpec {
                definition: definition.clone(),
                directory,
                generation,
                expected_durable_frontier: None,
                controlled_manifest: None,
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

    pub(crate) fn replace_active(
        &self,
        engine: ColumnarEngine,
        generation: ProjectionGeneration,
    ) -> Result<(), ColumnarPortError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        if matches!(&*state, ColumnarSlotState::Stopped) {
            return Err(ColumnarPortError::Unavailable);
        }
        *state = ColumnarSlotState::Active(Box::new(ActiveColumnarEngine {
            engine,
            generation: Some(generation),
        }));
        Ok(())
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
    ) -> Arc<Self> {
        Arc::new(Self {
            engines: RwLock::new(BTreeMap::new()),
            notifier: ColumnarNotifier::from_names(Vec::new()),
            names: RwLock::new(Vec::new()),
            control_bindings: BTreeMap::new(),
            projections_root,
            history_incarnation,
            process_generation,
            apply_source: ServerColumnarApplySource::new(storage),
            activation_wake: Arc::new(ColumnarActivationWake::default()),
            admitted_cold_sources: AtomicU64::new(0),
            activations: AtomicU64::new(0),
            population_passes: AtomicU64::new(0),
            observed_head: AtomicU64::new(0),
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
        let bindings =
            prepare_columnar_control_foundation(&storage, projections, history_incarnation)?;
        Self::open_prepared(
            storage,
            bindings,
            projections_root,
            history_incarnation,
            process_generation,
        )
    }

    /// Admits schema-bound controls as cold slots without opening an artifact.
    pub(crate) fn open_prepared(
        storage: SharedRedbOperationalPorts,
        bindings: Vec<ColumnarControlBinding>,
        projections_root: &Path,
        history_incarnation: u64,
        process_generation: [u8; 16],
    ) -> Result<Arc<Self>, ColumnarRegistrationError> {
        if bindings.is_empty() {
            return Ok(Self::empty(
                storage,
                projections_root.to_path_buf(),
                history_incarnation,
                process_generation,
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
            if !common_control_matches(&control, &binding.spec) {
                return Err(ColumnarRegistrationError::synchronization());
            }
            let selected = control
                .servable_generation()
                .or_else(|| control.candidate())
                .ok_or_else(ColumnarRegistrationError::synchronization)?;
            if selected.layout() != ColumnarProjectionLayoutV1::V1 {
                return Err(ColumnarRegistrationError::synchronization());
            }
            engines.insert(
                name.clone(),
                Arc::new(ColumnarEngineSlot::cold_controlled(
                    binding.definition.clone(),
                    controlled_generation_directory(
                        projections_root,
                        binding.spec.hash(),
                        selected.generation(),
                    ),
                    selected.generation(),
                    selected.frontier(),
                    selected
                        .artifact()
                        .map(|artifact| (artifact.length(), artifact.checksum())),
                )),
            );
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
            apply_source: ServerColumnarApplySource::new(storage),
            activation_wake: Arc::new(ColumnarActivationWake::default()),
            admitted_cold_sources: AtomicU64::new(admitted_cold_sources),
            activations: AtomicU64::new(0),
            population_passes: AtomicU64::new(0),
            observed_head: AtomicU64::new(0),
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
        };
        if artifact.is_some() {
            spec = spec.with_expected_durable_frontier(generation.frontier());
        }
        spec.open(self.history_incarnation)
            .map(|(engine, _)| engine)
    }

    pub(crate) fn replace_vector_engine(
        &self,
        name: &str,
        engine: ColumnarEngine,
        generation: ProjectionGeneration,
    ) -> Result<(), ColumnarPortError> {
        let engines = self
            .engines
            .read()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        engines
            .get(name)
            .ok_or(ColumnarPortError::Integrity)?
            .replace_active(engine, generation)
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
        if !common_control_matches(&control, &binding.spec) {
            return Err(ColumnarPortError::Integrity);
        }
        let selected = control
            .servable_generation()
            .or_else(|| control.candidate())
            .ok_or(ColumnarPortError::Integrity)?;
        if selected.layout() != ColumnarProjectionLayoutV1::V1 {
            return Err(ColumnarPortError::Integrity);
        }
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
        if !common_control_matches(&control, &binding.spec) {
            return Err(ColumnarPortError::Integrity);
        }
        let frontier = control
            .servable_generation()
            .or_else(|| control.candidate())
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
        let observed = storage
            .recover_expected_control(binding.spec.source())
            .map_err(|error| ColumnarRegistrationError::control_storage(error, &binding.name))?
            .ok_or_else(ColumnarRegistrationError::synchronization)?;
        reconcile_common_control(storage, binding, observed)?;
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
        return Ok(());
    }
    let outcome = if observed.published().is_none() && observed.predecessor().is_none() {
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
    if outcome == ColumnarProjectionControlWriteResultV1::Applied {
        return Ok(());
    }
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

fn common_control_matches(
    control: &StoredColumnarProjectionControlV1,
    spec: &ColumnarProjectionSpecV1,
) -> bool {
    control.target_definition_fingerprint() == spec.definition_fingerprint()
        && control.target_spec_hash() == spec.hash()
        && control.replay_limits() == spec.replay_limits()
}

pub(crate) fn controlled_generation_directory(
    projections_root: &Path,
    spec_hash: riffdb_types::ColumnarProjectionSpecHashV1,
    generation: ProjectionGeneration,
) -> PathBuf {
    projections_root
        .join(format!("source-{}", lower_hex(spec_hash.as_bytes())))
        .join(format!("generation-{:020}", generation.get()))
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
        let observation = slot.with_engine(|engine| {
            let definition = engine.definition().clone();
            let snapshot = engine.published_snapshot();
            let published_frontier = engine.published_frontier();
            let outcome = engine.outcome(head_position);
            let (has_published, lifecycle) = map_outcome_lifecycle(&outcome);
            ColumnarObservation::new(
                definition,
                snapshot,
                published_frontier,
                head.clone(),
                has_published,
                lifecycle,
            )
        })?;
        // Engine lock dropped before return; callers query the Arc snapshot freely.
        match observation {
            Some(observation) => Ok(observation),
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
        let control = self
            .runtime
            .storage()
            .recover_expected_control(binding.spec.source())
            .map_err(|error| match error.kind() {
                StorageErrorKind::Unavailable => VectorProjectionPortError::Unavailable,
                _ => VectorProjectionPortError::Integrity,
            })?
            .ok_or(VectorProjectionPortError::Integrity)?;
        if !common_control_matches(&control, &binding.spec) {
            return Err(VectorProjectionPortError::Integrity);
        }
        let selected = control
            .servable_generation()
            .ok_or_else(|| match control.lifecycle() {
                riffdb_storage_api::ColumnarProjectionLifecycleV1::Building => {
                    VectorProjectionPortError::Building
                }
                riffdb_storage_api::ColumnarProjectionLifecycleV1::Rebuilding => {
                    VectorProjectionPortError::Rebuilding
                }
                riffdb_storage_api::ColumnarProjectionLifecycleV1::Degraded => {
                    VectorProjectionPortError::Degraded
                }
                riffdb_storage_api::ColumnarProjectionLifecycleV1::Invalid => {
                    VectorProjectionPortError::Integrity
                }
                riffdb_storage_api::ColumnarProjectionLifecycleV1::CatchingUp
                | riffdb_storage_api::ColumnarProjectionLifecycleV1::Ready => {
                    VectorProjectionPortError::Integrity
                }
            })?;
        if slot.generation() != Some(selected.generation()) {
            return Err(VectorProjectionPortError::Rebuilding);
        }
        let observation = self
            .observe(request.source_name())
            .map_err(map_vector_port_error)?;
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
    storage: &SharedRedbOperationalPorts,
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
    storage: &SharedRedbOperationalPorts,
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
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use riffdb_catalog::{
        ActiveCatalogSnapshot as CatalogSnapshotReadback, CatalogHistoryOutcome,
        ValidatedContractBundle, validate_catalog_history,
    };
    use riffdb_columnar::{ColumnarQueryRequest, QueryBudget, QueryResult, query_snapshot};
    use riffdb_storage_api::{
        AuditPrincipalV1, CatalogActivationIntentV1, CatalogActivationResult,
        CatalogAdministrationRepository, DatabaseInitializationPort, DatabaseInitializationResult,
        EvidencePageLimit, ReadableCapabilityDigestInventory, ReadableDigestKey,
        ReadableIdempotencyDigestInventory, StartupValidationInputs, StructuralEvidenceCursor,
        StructuralEvidenceOpen, StructuralEvidencePage, StructuralEvidenceSession,
        StructuralOpenOutcome,
    };
    use riffdb_storage_redb::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
    use riffdb_types::{
        ActorId, ActorKind, CanonicalValue, CapabilityId, DatabaseId, DigestKeyId, RequestId,
        Timestamp,
    };

    use super::*;
    use crate::config::ConfiguredProjection;

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
                    assert!(findings.is_empty(), "fresh adapter store has no findings");
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
        let store = RedbStore::open(scope.path().join("db.redb")).expect("reopen board database");
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
            &scope.path().join("projections"),
            1,
            [0x5a; 16],
        )
        .expect("reopen board columnar runtime")
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

    fn request_projection(runtime: &ColumnarRuntime, name: &str) {
        let slot = runtime
            .engine(name)
            .expect("engine registry")
            .expect("known projection");
        runtime
            .request_activation(&slot)
            .expect("request projection activation");
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

    // req: PRJ-004, PRJ-008, PRJ-009, OQ-019, OQ-020, PERF-007, PERF-008
    #[test]
    fn failed_columnar_activation_never_serves_rows_or_falls_back() {
        let (runtime, scope) = board_runtime("failed-activation");
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
            .expect("control binding");
        let ready_control = runtime
            .storage()
            .recover_expected_control(binding.spec().source())
            .expect("read ready control")
            .expect("ready control");
        let published = ready_control.published().expect("published generation");
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
        assert_eq!(reopened.lifecycle_observation().activations(), 1);
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
