//! Server-side columnar apply source and published projection port.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use riffdb_catalog::ActiveCatalogSnapshot;
use riffdb_columnar::{
    CheckpointError, ColumnarEngine, ColumnarError, ColumnarOutcome, ColumnarProjectionDefinition,
    NearestCandidate, NearestCandidateAdmission, NearestQueryAdmissionError, NearestQueryRequest,
    OpenOptions, QueryBudget, QueryError, RegisteredDefinition,
};
use riffdb_contract_ir::{ContractBundle, ExpressionKind, ValueType, ValueTypeTag};
use riffdb_service::{
    ColumnarLifecycle, ColumnarNotifier, ColumnarObservation, ColumnarPortError,
    ColumnarProjectionPort, VectorProjectionPort, VectorProjectionPortError,
    VectorProjectionRequest, VectorProjectionResult,
};
use riffdb_storage_api::{
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest, EntityTarget,
    IdempotencyIdentity, StorageError, StorageErrorKind, StorageScanLimit, StoredCommitRecordV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1,
    StoredVectorProjectionControlV1, VectorProjectionControlRepository,
    VectorProjectionControlWriteResultV1, VectorProjectionLifecycleV1,
    VectorProjectionRebuildReasonV1, VectorProjectionReplayLimitsV1, VectorProjectionSourceV1,
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

/// One named engine guarded so observe never holds the lock across query work.
pub(crate) struct ColumnarEngineSlot {
    engine: Mutex<ColumnarEngine>,
    definition: RegisteredDefinition,
    generation: Option<ProjectionGeneration>,
}

impl ColumnarEngineSlot {
    fn new(engine: ColumnarEngine) -> Self {
        Self::with_generation(engine, None)
    }

    fn new_vector(engine: ColumnarEngine, generation: ProjectionGeneration) -> Self {
        Self::with_generation(engine, Some(generation))
    }

    fn with_generation(engine: ColumnarEngine, generation: Option<ProjectionGeneration>) -> Self {
        let definition = engine.definition().clone();
        Self {
            engine: Mutex::new(engine),
            definition,
            generation,
        }
    }

    /// Registered definition for this projection.
    #[must_use]
    pub(crate) fn definition(&self) -> &RegisteredDefinition {
        &self.definition
    }

    pub(crate) const fn generation(&self) -> Option<ProjectionGeneration> {
        self.generation
    }

    /// Locks the engine briefly for apply or observe snapshot capture.
    pub(crate) fn lock_engine(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, ColumnarEngine>, ColumnarPortError> {
        self.engine
            .lock()
            .map_err(|_| ColumnarPortError::Unavailable)
    }
}

impl fmt::Debug for ColumnarEngineSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ColumnarEngineSlot")
            .field("name", &self.definition.name())
            .field("generation", &self.generation)
            .finish()
    }
}

/// Process-local columnar engines and notifier fixed at startup registration.
pub(crate) struct ColumnarRuntime {
    engines: RwLock<BTreeMap<String, Arc<ColumnarEngineSlot>>>,
    notifier: ColumnarNotifier,
    names: RwLock<Vec<String>>,
    configured_names: BTreeSet<String>,
    vector_registrations: RwLock<BTreeMap<String, VectorProjectionRegistration>>,
    projections_root: PathBuf,
    history_incarnation: u64,
    apply_source: ServerColumnarApplySource,
}

#[derive(Clone)]
pub(crate) struct VectorProjectionRegistration {
    source: VectorProjectionSourceV1,
    limits: VectorProjectionReplayLimitsV1,
    definition_fingerprint: [u8; 32],
    definition: RegisteredDefinition,
}

impl VectorProjectionRegistration {
    pub(crate) const fn source(&self) -> &VectorProjectionSourceV1 {
        &self.source
    }

    pub(crate) const fn limits(&self) -> VectorProjectionReplayLimitsV1 {
        self.limits
    }

    pub(crate) const fn definition_fingerprint(&self) -> &[u8; 32] {
        &self.definition_fingerprint
    }

    pub(crate) fn definition(&self) -> &RegisteredDefinition {
        &self.definition
    }
}

impl ColumnarRuntime {
    /// Empty runtime when no columnar projections are configured.
    #[must_use]
    pub(crate) fn empty(
        storage: SharedRedbOperationalPorts,
        projections_root: PathBuf,
        history_incarnation: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            engines: RwLock::new(BTreeMap::new()),
            notifier: ColumnarNotifier::from_names(Vec::new()),
            names: RwLock::new(Vec::new()),
            configured_names: BTreeSet::new(),
            vector_registrations: RwLock::new(BTreeMap::new()),
            projections_root,
            history_incarnation,
            apply_source: ServerColumnarApplySource::new(storage),
        })
    }

    /// Opens engines for each configured projection against the active catalog.
    pub(crate) fn open(
        storage: SharedRedbOperationalPorts,
        projections: &[ConfiguredProjection],
        projections_root: &Path,
        history_incarnation: u64,
    ) -> Result<Arc<Self>, ColumnarRegistrationError> {
        let active = ActiveCatalogSnapshot::read(&storage).map_err(|error| {
            ColumnarRegistrationError::storage(error, first_projection_name(projections))
        })?;
        let Some(active) = active else {
            if projections.is_empty() {
                return Ok(Self::empty(
                    storage,
                    projections_root.to_path_buf(),
                    history_incarnation,
                ));
            }
            return Err(ColumnarRegistrationError::no_active_catalog(
                first_projection_name(projections),
            ));
        };
        let bundle = active.bundle().bundle();
        let mut engines = BTreeMap::new();
        let mut names =
            Vec::with_capacity(projections.len() + bundle.schema().vector_production_specs().len());
        let mut configured_names = BTreeSet::new();
        let mut vector_registrations = BTreeMap::new();
        for configured in projections {
            let name = configured.name().to_owned();
            if engines.contains_key(&name) {
                return Err(ColumnarRegistrationError::duplicate_name(name));
            }
            let definition = resolve_configured_projection(configured, bundle)?;
            let directory = projection_directory(projections_root, &name);
            let engine = ColumnarEngine::open(
                definition,
                OpenOptions::new(directory).with_history_incarnation(history_incarnation),
            )
            .map_err(|error| ColumnarRegistrationError::open(name.clone(), error))?;
            engines.insert(name.clone(), Arc::new(ColumnarEngineSlot::new(engine)));
            configured_names.insert(name.clone());
            names.push(name);
        }
        for spec in bundle.schema().vector_production_specs() {
            let entity = bundle
                .schema()
                .entity(spec.entity())
                .ok_or_else(|| ColumnarRegistrationError::definition("production-vector"))?;
            let vector = entity
                .record()
                .field(spec.field())
                .ok_or_else(|| ColumnarRegistrationError::definition("production-vector"))?;
            let name = format!("{}.{}", entity.name(), vector.name());
            if engines.contains_key(&name) {
                return Err(ColumnarRegistrationError::duplicate_name(name));
            }
            let registration = resolve_vector_registration(bundle, entity, spec, &name)?;
            let control = reconcile_vector_control(&storage, &registration)?;
            let definition = registration.definition().clone();
            let directory =
                vector_generation_directory(projections_root, &name, control.generation());
            let engine = ColumnarEngine::open(
                definition,
                OpenOptions::new(directory).with_history_incarnation(history_incarnation),
            )
            .map_err(|error| ColumnarRegistrationError::open(name.clone(), error))?;
            engines.insert(
                name.clone(),
                Arc::new(ColumnarEngineSlot::new_vector(engine, control.generation())),
            );
            vector_registrations.insert(name.clone(), registration);
            names.push(name);
        }
        names.sort();
        let notifier = ColumnarNotifier::from_names(names.iter().cloned());
        Ok(Arc::new(Self {
            engines: RwLock::new(engines),
            notifier,
            names: RwLock::new(names),
            configured_names,
            vector_registrations: RwLock::new(vector_registrations),
            projections_root: projections_root.to_path_buf(),
            history_incarnation,
            apply_source: ServerColumnarApplySource::new(storage),
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

    pub(crate) fn vector_registrations(
        &self,
    ) -> Result<Vec<(String, VectorProjectionRegistration)>, ColumnarPortError> {
        self.vector_registrations
            .read()
            .map(|registrations| {
                registrations
                    .iter()
                    .map(|(name, registration)| (name.clone(), registration.clone()))
                    .collect()
            })
            .map_err(|_| ColumnarPortError::Unavailable)
    }

    fn vector_registration(
        &self,
        name: &str,
    ) -> Result<Option<VectorProjectionRegistration>, ColumnarPortError> {
        self.vector_registrations
            .read()
            .map(|registrations| registrations.get(name).cloned())
            .map_err(|_| ColumnarPortError::Unavailable)
    }

    pub(crate) fn replace_vector_engine(
        &self,
        name: &str,
        engine: ColumnarEngine,
        generation: ProjectionGeneration,
    ) -> Result<(), ColumnarPortError> {
        let replacement = Arc::new(ColumnarEngineSlot::new_vector(engine, generation));
        let mut engines = self
            .engines
            .write()
            .map_err(|_| ColumnarPortError::Unavailable)?;
        if !engines.contains_key(name) {
            return Err(ColumnarPortError::Integrity);
        }
        engines.insert(name.to_owned(), replacement);
        Ok(())
    }

    pub(crate) fn projections_root(&self) -> &Path {
        &self.projections_root
    }

    /// Reconciles compiler-declared vector projections after catalog activation.
    ///
    /// A definition change never reuses an existing engine generation. Until
    /// the durable rebuild lifecycle replaces it, the worker fails closed.
    pub(crate) fn synchronize_active_vector_projections(
        &self,
    ) -> Result<(), ColumnarRegistrationError> {
        let active = ActiveCatalogSnapshot::read(self.storage())
            .map_err(|error| ColumnarRegistrationError::storage(error, "production-vector"))?;
        let Some(active) = active else {
            return Ok(());
        };
        let bundle = active.bundle().bundle();
        let mut desired = BTreeMap::new();
        for spec in bundle.schema().vector_production_specs() {
            let entity = bundle
                .schema()
                .entity(spec.entity())
                .ok_or_else(|| ColumnarRegistrationError::definition("production-vector"))?;
            let vector = entity
                .record()
                .field(spec.field())
                .ok_or_else(|| ColumnarRegistrationError::definition("production-vector"))?;
            let name = format!("{}.{}", entity.name(), vector.name());
            if self.configured_names.contains(&name) || desired.contains_key(&name) {
                return Err(ColumnarRegistrationError::duplicate_name(name));
            }
            desired.insert(
                name.clone(),
                resolve_vector_registration(bundle, entity, spec, &name)?,
            );
        }

        let previous_registrations = self
            .vector_registrations
            .read()
            .map_err(|_| ColumnarRegistrationError::synchronization())?
            .clone();
        for (name, registration) in &previous_registrations {
            if desired.contains_key(name) {
                continue;
            }
            if let Some(control) = self
                .storage()
                .read_vector_projection_control(registration.source())
                .map_err(|error| ColumnarRegistrationError::storage(error.into(), name.clone()))?
            {
                let invalid = StoredVectorProjectionControlV1::new(
                    registration.source().clone(),
                    control.generation(),
                    *control.definition_fingerprint(),
                    VectorProjectionLifecycleV1::Invalid,
                    control.published_frontier(),
                    None,
                    None,
                    control.limits(),
                )
                .map_err(|_| ColumnarRegistrationError::definition(name.clone()))?;
                match self
                    .storage()
                    .compare_and_set_vector_projection_control(Some(&control), &invalid)
                    .map_err(|error| {
                        ColumnarRegistrationError::storage(error.into(), name.clone())
                    })? {
                    VectorProjectionControlWriteResultV1::Applied
                    | VectorProjectionControlWriteResultV1::Unchanged => {}
                    VectorProjectionControlWriteResultV1::CompareMismatch => {
                        return Err(ColumnarRegistrationError::synchronization());
                    }
                }
            }
        }

        let existing = self
            .engines
            .read()
            .map_err(|_| ColumnarRegistrationError::synchronization())?;
        let mut additions = Vec::new();
        for (name, registration) in &desired {
            let control = reconcile_vector_control(self.storage(), registration)?;
            if let Some(slot) = existing.get(name)
                && slot.definition().fingerprint() == registration.definition().fingerprint()
                && slot.generation() == Some(control.generation())
            {
                continue;
            }
            let directory =
                vector_generation_directory(&self.projections_root, name, control.generation());
            let engine = ColumnarEngine::open(
                registration.definition().clone(),
                OpenOptions::new(directory).with_history_incarnation(self.history_incarnation),
            )
            .map_err(|error| ColumnarRegistrationError::open(name.clone(), error))?;
            additions.push((
                name.clone(),
                Arc::new(ColumnarEngineSlot::new_vector(engine, control.generation())),
            ));
        }
        drop(existing);

        let mut engines = self
            .engines
            .write()
            .map_err(|_| ColumnarRegistrationError::synchronization())?;
        engines
            .retain(|name, _| self.configured_names.contains(name) || desired.contains_key(name));
        for (name, slot) in additions {
            engines.insert(name, slot);
        }
        let names = engines.keys().cloned().collect::<Vec<_>>();
        self.notifier
            .synchronize_names(names.iter().cloned())
            .map_err(|_| ColumnarRegistrationError::synchronization())?;
        *self
            .names
            .write()
            .map_err(|_| ColumnarRegistrationError::synchronization())? = names;
        *self
            .vector_registrations
            .write()
            .map_err(|_| ColumnarRegistrationError::synchronization())? = desired;
        Ok(())
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
    pub(crate) fn read_application_head(&self) -> Result<FrontierPosition, ColumnarPortError> {
        read_application_head(self.storage())
    }
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
    spec: &riffdb_contract_ir::VectorProductionSpecV1,
    name: &str,
) -> Result<VectorProjectionRegistration, ColumnarRegistrationError> {
    let definition = resolve_production_vector_projection(bundle, entity, name)?;
    let limits = VectorProjectionReplayLimitsV1::new(
        spec.replay_age_seconds(),
        spec.replay_bytes(),
        spec.replay_backlog(),
    )
    .ok_or_else(|| ColumnarRegistrationError::definition(name))?;
    Ok(VectorProjectionRegistration {
        source: VectorProjectionSourceV1::new(
            bundle.lineage().clone(),
            spec.entity(),
            spec.field(),
        ),
        limits,
        // The complete canonical bundle hash is deliberately conservative:
        // every compiler-visible successor gets a distinct derived generation
        // rather than risking reuse across model or replay-descriptor changes.
        definition_fingerprint: *bundle.bundle_hash().as_bytes(),
        definition,
    })
}

fn reconcile_vector_control(
    storage: &SharedRedbOperationalPorts,
    registration: &VectorProjectionRegistration,
) -> Result<StoredVectorProjectionControlV1, ColumnarRegistrationError> {
    let current = storage
        .read_vector_projection_control(registration.source())
        .map_err(|error| ColumnarRegistrationError::storage(error.into(), "production-vector"))?;
    let replacement = match current.as_ref() {
        None => StoredVectorProjectionControlV1::initial(
            registration.source().clone(),
            registration.definition_fingerprint,
            registration.limits,
        ),
        Some(control)
            if control.definition_fingerprint() == registration.definition_fingerprint()
                && control.limits() == registration.limits() =>
        {
            return Ok(control.clone());
        }
        Some(control) => StoredVectorProjectionControlV1::new(
            registration.source().clone(),
            control
                .generation()
                .checked_next()
                .ok_or_else(|| ColumnarRegistrationError::definition("production-vector"))?,
            registration.definition_fingerprint,
            VectorProjectionLifecycleV1::RebuildRequired,
            control.published_frontier(),
            None,
            Some(VectorProjectionRebuildReasonV1::DefinitionChanged),
            registration.limits,
        )
        .map_err(|_| ColumnarRegistrationError::definition("production-vector"))?,
    };
    match storage
        .compare_and_set_vector_projection_control(current.as_ref(), &replacement)
        .map_err(|error| ColumnarRegistrationError::storage(error.into(), "production-vector"))?
    {
        VectorProjectionControlWriteResultV1::Applied
        | VectorProjectionControlWriteResultV1::Unchanged => Ok(replacement),
        VectorProjectionControlWriteResultV1::CompareMismatch => {
            let observed = storage
                .read_vector_projection_control(registration.source())
                .map_err(|error| {
                    ColumnarRegistrationError::storage(error.into(), "production-vector")
                })?
                .ok_or_else(ColumnarRegistrationError::synchronization)?;
            if observed.definition_fingerprint() == registration.definition_fingerprint()
                && observed.limits() == registration.limits()
            {
                Ok(observed)
            } else {
                Err(ColumnarRegistrationError::synchronization())
            }
        }
    }
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
        // Head from storage without holding the engine lock.
        let head_position = self.runtime.read_application_head()?;
        let head = ProjectionFrontier::new(self.runtime.history_incarnation(), head_position);
        // Lock only long enough to clone Arc snapshot + frontiers + lifecycle.
        let observation = {
            let engine = slot.lock_engine()?;
            let definition = engine.definition().clone();
            let snapshot = engine.published_snapshot();
            let published_frontier = engine.published_frontier();
            let outcome = engine.outcome(head_position);
            let (has_published, lifecycle) = map_outcome_lifecycle(&outcome);
            ColumnarObservation::new(
                definition,
                snapshot,
                published_frontier,
                head,
                has_published,
                lifecycle,
            )
        };
        // Engine lock dropped before return; callers query the Arc snapshot freely.
        Ok(observation)
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
        if let Some(registration) = self
            .runtime
            .vector_registration(request.source_name())
            .map_err(map_vector_port_error)?
        {
            let control = self
                .runtime
                .storage()
                .read_vector_projection_control(registration.source())
                .map_err(|error| match error.kind() {
                    StorageErrorKind::Unavailable => VectorProjectionPortError::Unavailable,
                    _ => VectorProjectionPortError::Integrity,
                })?
                .ok_or(VectorProjectionPortError::Integrity)?;
            if control.definition_fingerprint() != registration.definition_fingerprint() {
                return Err(VectorProjectionPortError::Integrity);
            }
            let slot = self
                .runtime
                .engine(request.source_name())
                .map_err(map_vector_port_error)?
                .ok_or(VectorProjectionPortError::Integrity)?;
            if slot.generation() != Some(control.generation()) {
                return Err(VectorProjectionPortError::Rebuilding);
            }
            match control.lifecycle() {
                VectorProjectionLifecycleV1::Building => {
                    return Err(VectorProjectionPortError::Building);
                }
                VectorProjectionLifecycleV1::RebuildRequired
                | VectorProjectionLifecycleV1::Rebuilding => {
                    return Err(VectorProjectionPortError::Rebuilding);
                }
                VectorProjectionLifecycleV1::Invalid => {
                    return Err(VectorProjectionPortError::Integrity);
                }
                VectorProjectionLifecycleV1::Ready => {}
            }
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
            self.runtime.storage(),
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
    Open,
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

    fn open(projection_name: impl Into<String>, error: ColumnarError) -> Self {
        let _ = error;
        Self {
            projection_name: projection_name.into(),
            kind: ColumnarRegistrationErrorKind::Open,
        }
    }

    fn storage(error: riffdb_catalog::CatalogError, projection_name: impl Into<String>) -> Self {
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
            ColumnarRegistrationErrorKind::Open => write!(
                formatter,
                "columnar projection '{}' could not open durable engine state",
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

fn projection_directory(projections_root: &Path, name: &str) -> PathBuf {
    projections_root.join(name)
}

pub(crate) fn vector_generation_directory(
    projections_root: &Path,
    name: &str,
    generation: ProjectionGeneration,
) -> PathBuf {
    projections_root
        .join(name)
        .join(format!("generation-{:020}", generation.get()))
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
    let limit = StorageScanLimit::new(1).ok_or(ColumnarPortError::Integrity)?;
    let page = AuthoritativeScanReader::scan_commits(storage, CommitScanRequest::initial(limit))
        .map_err(map_port_storage)?;
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
pub(crate) fn is_holdback_active(error: &ColumnarError) -> bool {
    matches!(
        error,
        ColumnarError::Checkpoint(CheckpointError::HoldbackActive { .. })
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
        let runtime = ColumnarRuntime::open(storage, &[projection], &projections_root, 1)
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
        let runtime = ColumnarRuntime::open(storage, &[], &projections_root, 1)
            .expect("open production vector runtime");
        (runtime, scope)
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
        let registration = runtime
            .vector_registration("Document.embedding")
            .expect("registration lock")
            .expect("vector registration");
        let building = runtime
            .storage()
            .read_vector_projection_control(registration.source())
            .expect("read building control")
            .expect("building control");
        assert_eq!(building.lifecycle(), VectorProjectionLifecycleV1::Building);
        assert!(!building.retention_attached());

        assert!(crate::columnar_worker::run_one_test_pass(&runtime));

        let ready = runtime
            .storage()
            .read_vector_projection_control(registration.source())
            .expect("read ready control")
            .expect("ready control");
        assert_eq!(ready.lifecycle(), VectorProjectionLifecycleV1::Ready);
        assert!(ready.retention_attached());
        let engine = runtime
            .engine("Document.embedding")
            .expect("engine registry")
            .expect("ready vector engine");
        assert_eq!(
            engine
                .lock_engine()
                .expect("engine lock")
                .durable_frontier()
                .position(),
            ready.published_frontier()
        );
    }

    #[test]
    fn detached_and_rebuilding_vector_controls_resume_without_frontier_overclaim() {
        let (runtime, _scope) = production_vector_runtime("production-vector-resume");
        let registration = runtime
            .vector_registration("Document.embedding")
            .expect("registration lock")
            .expect("vector registration");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let first_ready = runtime
            .storage()
            .read_vector_projection_control(registration.source())
            .expect("read first generation")
            .expect("first generation");

        let detached = StoredVectorProjectionControlV1::new(
            registration.source().clone(),
            first_ready
                .generation()
                .checked_next()
                .expect("next generation"),
            *first_ready.definition_fingerprint(),
            VectorProjectionLifecycleV1::RebuildRequired,
            first_ready.published_frontier(),
            None,
            Some(VectorProjectionRebuildReasonV1::ReplayBacklog),
            first_ready.limits(),
        )
        .expect("detached control");
        assert_eq!(
            runtime
                .storage()
                .compare_and_set_vector_projection_control(Some(&first_ready), &detached)
                .expect("detach"),
            VectorProjectionControlWriteResultV1::Applied
        );
        assert!(
            runtime
                .storage()
                .attached_vector_projection_frontiers()
                .expect("retention frontiers")
                .is_empty()
        );
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let second_ready = runtime
            .storage()
            .read_vector_projection_control(registration.source())
            .expect("read second generation")
            .expect("second generation");
        assert_eq!(second_ready.lifecycle(), VectorProjectionLifecycleV1::Ready);
        assert_eq!(second_ready.generation(), detached.generation());

        let rebuilding = StoredVectorProjectionControlV1::new(
            registration.source().clone(),
            second_ready
                .generation()
                .checked_next()
                .expect("next generation"),
            *second_ready.definition_fingerprint(),
            VectorProjectionLifecycleV1::Rebuilding,
            second_ready.published_frontier(),
            Some(FrontierPosition::BeforeFirst),
            Some(VectorProjectionRebuildReasonV1::ReplayBytes),
            second_ready.limits(),
        )
        .expect("rebuilding control");
        assert_eq!(
            runtime
                .storage()
                .compare_and_set_vector_projection_control(Some(&second_ready), &rebuilding)
                .expect("persist rebuilding"),
            VectorProjectionControlWriteResultV1::Applied
        );
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let recovered = runtime
            .storage()
            .read_vector_projection_control(registration.source())
            .expect("read recovered generation")
            .expect("recovered generation");
        assert_eq!(recovered.lifecycle(), VectorProjectionLifecycleV1::Ready);
        assert_eq!(recovered.generation(), rebuilding.generation());
        assert_eq!(
            runtime
                .engine("Document.embedding")
                .expect("engine registry")
                .expect("recovered engine")
                .generation(),
            Some(recovered.generation())
        );
    }

    #[test]
    fn persisted_vector_rebuild_resumes_after_storage_reopen_without_overclaim() {
        let (runtime, scope) = production_vector_runtime("production-vector-reopen");
        let registration = runtime
            .vector_registration("Document.embedding")
            .expect("registration lock")
            .expect("vector registration");
        assert!(crate::columnar_worker::run_one_test_pass(&runtime));
        let ready = runtime
            .storage()
            .read_vector_projection_control(registration.source())
            .expect("read ready generation")
            .expect("ready generation");
        let rebuilding = StoredVectorProjectionControlV1::new(
            registration.source().clone(),
            ready.generation().checked_next().expect("next generation"),
            *ready.definition_fingerprint(),
            VectorProjectionLifecycleV1::Rebuilding,
            ready.published_frontier(),
            Some(ready.published_frontier()),
            Some(VectorProjectionRebuildReasonV1::ReplayAge),
            ready.limits(),
        )
        .expect("rebuilding control");
        assert_eq!(
            runtime
                .storage()
                .compare_and_set_vector_projection_control(Some(&ready), &rebuilding)
                .expect("persist rebuilding generation"),
            VectorProjectionControlWriteResultV1::Applied
        );
        drop(runtime);

        let store = RedbStore::open(scope.path().join("db.redb")).expect("reopen database");
        let ports = open_operational(store);
        let storage =
            SharedRedbOperationalPorts::new(ports, None).expect("share reopened operational ports");
        let reopened = ColumnarRuntime::open(storage, &[], &scope.path().join("projections"), 1)
            .expect("reopen vector runtime");
        let persisted = reopened
            .storage()
            .read_vector_projection_control(registration.source())
            .expect("read persisted rebuilding generation")
            .expect("persisted rebuilding generation");
        assert_eq!(
            persisted.lifecycle(),
            VectorProjectionLifecycleV1::Rebuilding
        );
        assert_eq!(persisted.generation(), rebuilding.generation());
        assert_eq!(persisted.published_frontier(), ready.published_frontier());
        assert!(
            reopened
                .storage()
                .attached_vector_projection_frontiers()
                .expect("detached retention frontiers")
                .is_empty()
        );

        assert!(crate::columnar_worker::run_one_test_pass(&reopened));
        let recovered = reopened
            .storage()
            .read_vector_projection_control(registration.source())
            .expect("read recovered generation")
            .expect("recovered generation");
        assert_eq!(recovered.lifecycle(), VectorProjectionLifecycleV1::Ready);
        assert_eq!(recovered.generation(), rebuilding.generation());
        let engine = reopened
            .engine("Document.embedding")
            .expect("engine registry")
            .expect("recovered engine");
        assert_eq!(engine.generation(), Some(recovered.generation()));
        assert_eq!(
            engine
                .lock_engine()
                .expect("engine lock")
                .durable_frontier()
                .position(),
            recovered.published_frontier()
        );
    }

    #[test]
    fn first_contract_activation_registers_vector_source_without_process_restart() {
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
        let runtime = ColumnarRuntime::open(storage.clone(), &[], &projections_root, 1)
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

        runtime
            .synchronize_active_vector_projections()
            .expect("synchronize vector registry");
        assert_eq!(
            runtime.names().expect("runtime names"),
            ["Document.embedding"]
        );
        let registration = runtime
            .notifier()
            .register("Document.embedding".to_owned())
            .expect("new name must be waitable after synchronization");
        drop(registration);
        assert!(
            runtime
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
        // First worker pass publishes the (empty) snapshot.
        {
            let slot = runtime
                .engine("ticket_board")
                .expect("engine registry")
                .expect("board slot");
            let mut engine = slot.lock_engine().expect("engine lock");
            engine
                .apply_available(runtime.apply_source())
                .expect("initial apply pass");
        }
        let port = ServerColumnarProjectionPort::new(Arc::clone(&runtime));
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
                let mut engine = slot.lock_engine().expect("worker engine lock");
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
                drop(engine);
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
        let error = ColumnarError::Checkpoint(CheckpointError::HoldbackActive {
            published: FrontierPosition::BeforeFirst,
            processed: FrontierPosition::BeforeFirst,
            deferred: 1,
        });
        assert!(is_holdback_active(&error));
    }
}
