//! Server-side columnar apply source and published projection port.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use riffdb_catalog::ActiveCatalogSnapshot;
use riffdb_columnar::{
    CheckpointError, ColumnarEngine, ColumnarError, ColumnarOutcome, ColumnarProjectionDefinition,
    OpenOptions, RegisteredDefinition,
};
use riffdb_contract_ir::ContractBundle;
use riffdb_service::{
    ColumnarLifecycle, ColumnarNotifier, ColumnarObservation, ColumnarPortError,
    ColumnarProjectionPort,
};
use riffdb_storage_api::{
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest, EntityTarget,
    IdempotencyIdentity, StorageError, StorageErrorKind, StorageScanLimit, StoredCommitRecordV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1,
};
use riffdb_types::{
    CommitSequence, EventId, FieldId, FrontierPosition, ProjectionFrontier, ProvenanceId,
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
}

impl ColumnarEngineSlot {
    fn new(engine: ColumnarEngine) -> Self {
        let definition = engine.definition().clone();
        Self {
            engine: Mutex::new(engine),
            definition,
        }
    }

    /// Registered definition for this projection.
    #[must_use]
    pub(crate) fn definition(&self) -> &RegisteredDefinition {
        &self.definition
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
            .finish()
    }
}

/// Process-local columnar engines and notifier fixed at startup registration.
pub(crate) struct ColumnarRuntime {
    engines: BTreeMap<String, Arc<ColumnarEngineSlot>>,
    notifier: ColumnarNotifier,
    names: Vec<String>,
    history_incarnation: u64,
    apply_source: ServerColumnarApplySource,
}

impl ColumnarRuntime {
    /// Empty runtime when no columnar projections are configured.
    #[must_use]
    pub(crate) fn empty(
        storage: SharedRedbOperationalPorts,
        history_incarnation: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            engines: BTreeMap::new(),
            notifier: ColumnarNotifier::from_names(Vec::new()),
            names: Vec::new(),
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
        if projections.is_empty() {
            return Ok(Self::empty(storage, history_incarnation));
        }
        let active = ActiveCatalogSnapshot::read(&storage).map_err(|error| {
            ColumnarRegistrationError::storage(error, first_projection_name(projections))
        })?;
        let Some(active) = active else {
            return Err(ColumnarRegistrationError::no_active_catalog(
                first_projection_name(projections),
            ));
        };
        let bundle = active.bundle().bundle();
        let mut engines = BTreeMap::new();
        let mut names = Vec::with_capacity(projections.len());
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
            names.push(name);
        }
        names.sort();
        let notifier = ColumnarNotifier::from_names(names.iter().cloned());
        Ok(Arc::new(Self {
            engines,
            notifier,
            names,
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
    #[must_use]
    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    /// History incarnation bound into published frontiers.
    #[must_use]
    pub(crate) const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Registered engine slots in name order.
    #[must_use]
    pub(crate) fn engines(&self) -> &BTreeMap<String, Arc<ColumnarEngineSlot>> {
        &self.engines
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

impl fmt::Debug for ColumnarRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ColumnarRuntime")
            .field("projection_count", &self.names.len())
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
            .engines
            .get(projection_name)
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
            .engines
            .get(projection_name)
            .map(|slot| slot.definition().clone())
    }

    fn notifier(&self) -> &ColumnarNotifier {
        self.runtime.notifier()
    }

    fn known_names(&self) -> &[String] {
        self.runtime.names()
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
    use super::*;

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
