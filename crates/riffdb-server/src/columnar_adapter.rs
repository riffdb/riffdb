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

    static NEXT_ADAPTER_PATH: AtomicU64 = AtomicU64::new(1);

    fn adapter_temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "riffdb-columnar-adapter-{label}-{}-{}",
            std::process::id(),
            NEXT_ADAPTER_PATH.fetch_add(1, Ordering::Relaxed)
        ))
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

    fn board_runtime(label: &str) -> (Arc<ColumnarRuntime>, PathBuf, PathBuf) {
        let database_path = adapter_temp_path(&format!("{label}-db"));
        let projections_root = adapter_temp_path(&format!("{label}-proj"));
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
        (runtime, database_path, projections_root)
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
        let (runtime, database_path, projections_root) = board_runtime("lockfree");
        // First worker pass publishes the (empty) snapshot.
        {
            let slot = runtime.engines().get("ticket_board").expect("board slot");
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
                let slot = runtime.engines().get("ticket_board").expect("board slot");
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
        let _ = std::fs::remove_file(&database_path);
        let _ = std::fs::remove_dir_all(&projections_root);
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
