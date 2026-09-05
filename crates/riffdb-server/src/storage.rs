#![expect(
    clippy::expect_used,
    reason = "the fixed inventory scan page is nonzero and within the storage scan bound"
)]

//! Private sharing bridge for the one activated production redb port bundle.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use riffdb_columnar::{PreparedColumnarGenerationRepository, PreparedColumnarGenerationV1};
use riffdb_query_executor::StorageQueryExecutor;
use riffdb_service::{AuthoritativeReadinessFailure, ServiceHealthHooks};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, ActiveQueryModulePointerV1, AdmissionLookupResultV1,
    AdmissionRepository, AdmissionRequestV1, AdmissionResultV1, ApplicationCommandTransactionPort,
    ApplicationExportOperationRepository, ApplicationExportOperationWriteResultV1,
    ApplicationExportSnapshotPort, ApplicationExportSnapshotReader,
    ApplicationInstallationCampaignRepository, ApplicationInstallationCampaignWriteResultV1,
    AuditedAdmissionRepository, AuditedAdmissionRequestV1, AuditedAdmissionResultV1,
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, CapabilityAdministrationTransactionPort,
    CapabilityBootstrapAdministrationRepository, CapabilityBootstrapIntentV1,
    CapabilityBootstrapResult, CapabilityCreateAwaitingDecision,
    CapabilityCreateCandidateTransaction, CapabilityCreateCandidateV1, CapabilityCreateIntentV1,
    CapabilityCreateResult, CapabilityInventoryReader, CapabilityLookupResult, CapabilityReader,
    CapabilityRevokeAwaitingDecision, CapabilityRevokeCandidateTransaction,
    CapabilityRevokeCandidateV1, CapabilityRevokeIntentV1, CapabilityRevokeResult,
    CatalogActivationIntentV1, CatalogActivationResult, CatalogAdministrationRepository,
    CatalogRepository, ColumnarProjectionControlRepository, ColumnarProjectionControlWriteResultV1,
    ColumnarProjectionFailureReasonV1, ColumnarProjectionLayoutV1,
    ColumnarProjectionReplayLimitsV1, ColumnarProjectionRetentionRepository, CommitScanPageV1,
    CommitScanRequest, DeferredCommandEpochPort, EventConsumerRepository, EventConsumerSnapshotV1,
    EventConsumerTransitionResultV1, EventConsumerTransitionV1, EventRouteScanRequestV1,
    EventRouteScanV1, ExecutionFailureAdmissionResult, ExecutionFailureTransitionPort,
    ExecutionFailureTransitionRequestV1, FilteredAuthoritativeIndexScanPage,
    FilteredAuthoritativeIndexScanRequest, FilteredAuthoritativeScanReader,
    FreshColumnarProjectionControlV1, IdempotencyIdentity, IdempotencyLookupCandidatesV1,
    OutboxClaimV1, OutboxDeadLetterV1, OutboxPageLimit, OutboxRenewV1, OutboxRepository,
    OutboxRetryV1, OutboxStatusReadResultV1, OutboxSucceedV1, OutboxTransitionResultV1,
    PartitionEventRouteReader, PendingOutboxScanV1, ProjectionApplyRequestV1,
    ProjectionApplyResult, ProjectionApplySnapshot, ProjectionApplySnapshotReader,
    ProjectionApplySnapshotRequest, ProjectionControlOperation, ProjectionControlResult,
    ProjectionControlScanV1, ProjectionMutationRepository, ProjectionQueryReader,
    ProjectionQueryRequest, ProjectionQueryResult, ProjectionRecoveryPageLimit,
    ProjectionRecoveryRepository, ProjectionRecoveryValidationRequestV1,
    ProjectionRecoveryValidationResultV1, ProjectionStatus, QueryModuleActivationIntentV1,
    QueryModuleActivationResult, QueryModuleAdministrationRepository, QueryModuleRepository,
    ReactiveModuleAdministrationRepository, ReactiveModulePublicationIntentV1,
    ReactiveModulePublicationResult, ReactiveModuleRepository, ReadSnapshot,
    ServiceAuditAppendIntentV1, ServiceAuditAppendRepository, ServiceAuditAppendResult,
    ServiceAuditGroupAppend, SnapshotReader, SnapshotRequest, StorageError, StorageErrorKind,
    StorageScanLimit, StoredApplicationExportOperationV1, StoredApplicationInstallationCampaignV1,
    StoredCapabilityRecordV1, StoredColumnarProjectionControlV1, StoredCommitRecordV1,
    StoredContractBundleV1, StoredContractMigrationEdgeV1, StoredDurableEventV1,
    StoredEntityRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1, StoredQueryModuleV1,
    StoredReactiveModuleV1, StoredVectorProjectionControlV1, UndeliveredOutboxStatusScanRequestV1,
    UndeliveredOutboxStatusScanV1, VectorEvidenceIndexPageV1, VectorEvidenceIndexRepository,
    VectorEvidenceIndexScanRequestV1, VectorObservationCountsV1, VectorObservationRepository,
    VectorObservationTargetV1, VectorProjectionControlRepository,
    VectorProjectionControlWriteResultV1, VectorProjectionSourceV1,
};
use riffdb_storage_redb::{RedbOperationalPorts, RedbSharedPorts};
use riffdb_types::{
    CapabilityId, CapabilityTokenDigest, ColumnarProjectionSourceV1, ColumnarProjectionSpecHashV1,
    CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, DefinitionFingerprint,
    EventConsumerIdentityHash, EventId, FrontierPosition, ProvenanceId, QueryModuleHash,
    ReactiveModuleHash,
};

/// A cloneable handle to the sole activated redb semantic-port bundle.
///
/// This type remains crate-private. Composition must pass clones only into the
/// coordinator or behind narrower consumer adapters and trait objects. The
/// outer lock protects the otherwise non-cloneable port value; redb's owned
/// transaction type states retain their separate internal mutation lease after
/// a `begin_*` call returns.
#[allow(
    dead_code,
    reason = "WP-130 process composition constructs this private bridge after staged startup"
)]
pub(crate) struct SharedRedbOperationalPorts {
    cell: SharedStorageCell<RedbOperationalPorts>,
    shared: RedbSharedPorts,
    catalog: Arc<CurrentCatalogView>,
    capabilities: Arc<CurrentCapabilityView>,
    query_modules: Arc<CurrentQueryModuleView>,
    bounded_clean_startup: bool,
    health: Option<Arc<dyn ServiceHealthHooks>>,
}

impl SharedRedbOperationalPorts {
    /// Wraps the exact activated port bundle released by staged startup.
    #[allow(
        dead_code,
        reason = "WP-130 process composition constructs this private bridge after staged startup"
    )]
    pub(crate) fn new(
        ports: RedbOperationalPorts,
        health: Option<Arc<dyn ServiceHealthHooks>>,
    ) -> Result<Self, StorageError> {
        let bounded_clean_startup = ports.clean_close_fast_startup();
        crate::startup_census::record_mode(bounded_clean_startup);
        let shared = ports.shared_ports();
        let catalog = if bounded_clean_startup {
            CurrentCatalogView::rebuild_from_clean_startup(&shared)?
        } else {
            CurrentCatalogView::rebuild(&shared)?
        };
        let capabilities = if bounded_clean_startup {
            CurrentCapabilityView::cold()
        } else {
            CurrentCapabilityView::rebuild(&shared)?
        };
        let query_modules = CurrentQueryModuleView::rebuild(&shared)?;
        Ok(Self {
            cell: SharedStorageCell::new(ports),
            shared,
            catalog: Arc::new(catalog),
            capabilities: Arc::new(capabilities),
            query_modules: Arc::new(query_modules),
            bounded_clean_startup,
            health,
        })
    }

    #[cfg(test)]
    pub(crate) fn append_columnar_worker_commit_fixture(
        &self,
        rows: &[riffdb_storage_api::StoredEntityRecordV1],
        commits: &[riffdb_storage_api::StoredCommitRecordV1],
    ) -> Result<(), StorageError> {
        self.cell.with_mut(|ports| {
            riffdb_storage_redb::append_columnar_worker_commit_fixture(ports, rows, commits)
        })
    }

    #[cfg(test)]
    pub(crate) fn append_columnar_worker_commit_with_crosslinks_fixture(
        &self,
        rows: &[riffdb_storage_api::StoredEntityRecordV1],
        commits: &[riffdb_storage_api::StoredCommitRecordV1],
        outcomes: &[riffdb_storage_api::StoredOutcomeV1],
        provenance: &[riffdb_storage_api::StoredProvenanceRecordV1],
    ) -> Result<(), StorageError> {
        self.cell.with_mut(|ports| {
            riffdb_storage_redb::append_columnar_worker_commit_with_crosslinks_fixture(
                ports, rows, commits, outcomes, provenance,
            )
        })
    }

    pub(crate) const fn bounded_clean_startup(&self) -> bool {
        self.bounded_clean_startup
    }

    /// Assembles the sole executor implementation over this cloneable lower reader.
    pub(crate) fn query_executor(&self) -> StorageQueryExecutor<RedbSharedPorts> {
        StorageQueryExecutor::new(self.shared.clone())
    }

    /// True only when bounded startup proved no in-flight `Delivering` outbox
    /// entry exists. False on the complete path, and false when the bounded
    /// probe could not decide, so ignorance never reads as proof.
    pub(crate) fn outbox_delivering_proven_absent(&self) -> bool {
        self.cell
            .with_mut(|ports| Ok(ports.outbox_delivering_proven_absent()))
            .unwrap_or(false)
    }

    /// Publishes the storage adapter's transient-index rebuild census into the
    /// startup census so a bounded start that warmed the caches anyway is
    /// visible on the readiness line.
    pub(crate) fn record_transient_index_census(&self) {
        let census = self.cell.with_mut(|ports| {
            Ok((
                ports.transient_index_rebuilds(),
                ports.transient_index_commit_rows(),
            ))
        });
        if let Ok((rebuilds, commit_rows)) = census {
            crate::startup_census::record_transient_index_rebuilds(rebuilds, commit_rows);
        }
    }

    /// Reads the active query-module pointer from process-local state only.
    ///
    /// Never touches redb, never takes a write lock, and never blocks on the
    /// view lock: the outer `Option` is `None` whenever the answer is not
    /// already resident (unpublished identity, or a contended/poisoned view),
    /// which callers must treat as "consult the blocking port". The inner
    /// `Option` distinguishes a cached active pointer from a cached
    /// known-absent identity.
    pub(crate) fn cached_active_query_module(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
    ) -> Option<Option<ActiveQueryModulePointerV1>> {
        self.query_modules.state.try_read().ok()?.active(
            lineage,
            contract_version,
            contract_bundle_hash,
        )
    }

    /// Creates exact read-only migration-preflight authority for the active predecessor.
    pub(crate) fn migration_preflight(
        &self,
        active_bundle: ContractBundleHash,
    ) -> Result<riffdb_storage_redb::RedbContractMigrationPreflight, StorageError> {
        riffdb_storage_redb::RedbContractMigrationPreflight::from_shared(
            self.shared.clone(),
            active_bundle,
        )
    }

    /// Reads permanent evidence for one predecessor migration edge.
    pub(crate) fn contract_migration_edge(
        &self,
        predecessor: ContractBundleHash,
    ) -> Result<Option<StoredContractMigrationEdgeV1>, StorageError> {
        CatalogRepository::read_contract_migration_edge(&self.shared, predecessor)
    }

    fn current_view_failure(&self, error: StorageError) -> StorageError {
        if let Some(health) = &self.health {
            health.fail_authoritative_readiness(AuthoritativeReadinessFailure::Integrity);
        }
        error
    }

    /// Runs the storage-owned barrier, checkpoint classification, and final CLEAN.
    pub(crate) fn complete_graceful_close(
        &self,
    ) -> riffdb_storage_redb::GracefulCheckpointCloseReceiptV1 {
        self.cell
            .with_mut(|ports| Ok(ports.complete_graceful_close()))
            .unwrap_or_else(|_| {
                riffdb_storage_redb::GracefulCheckpointCloseReceiptV1::barrier_failed([0; 3])
            })
    }

    /// Executes protected event selection inside the same redb mutation fence
    /// as current capability, row, relationship, checkpoint, and lease state.
    pub(crate) fn coordinate_protected_event_consumer_lease(
        &self,
        request: riffdb_storage_redb::ProtectedEventConsumerLeaseV1,
    ) -> Result<riffdb_storage_api::CoordinateConsumerLeaseResultV1, StorageError> {
        self.cell
            .with_mut(|ports| ports.coordinate_protected_event_consumer_lease(request))
    }

    /// Validates one reaction lease together with current trigger-event row
    /// authority in one redb mutation fence.
    pub(crate) fn validate_protected_event_consumer_lease(
        &self,
        request: riffdb_storage_redb::ProtectedEventConsumerLeaseValidationV1,
    ) -> Result<riffdb_storage_redb::ProtectedEventConsumerLeaseValidationResultV1, StorageError>
    {
        self.cell
            .with_mut(|ports| ports.validate_protected_event_consumer_lease(request))
    }

    /// Resolves one protected event lease only while its current authority and
    /// anchored row remain valid in the same redb mutation fence.
    pub(crate) fn coordinate_protected_event_consumer_resolution(
        &self,
        request: riffdb_storage_redb::ProtectedEventConsumerResolutionV1,
    ) -> Result<riffdb_storage_api::EventConsumerTransitionResultV1, StorageError> {
        self.cell
            .with_mut(|ports| ports.coordinate_protected_event_consumer_resolution(request))
    }

    /// Executes protected replay under one current capability/row snapshot.
    pub(crate) fn replay_protected_events(
        &self,
        request: riffdb_storage_redb::ProtectedEventReplayV1,
    ) -> Result<riffdb_storage_redb::ProtectedEventReplayResultV1, StorageError> {
        self.cell
            .with_mut(|ports| ports.replay_protected_events(request))
    }
}

impl Clone for SharedRedbOperationalPorts {
    fn clone(&self) -> Self {
        Self {
            cell: self.cell.clone(),
            shared: self.shared.clone(),
            catalog: Arc::clone(&self.catalog),
            capabilities: Arc::clone(&self.capabilities),
            query_modules: Arc::clone(&self.query_modules),
            bounded_clean_startup: self.bounded_clean_startup,
            health: self.health.clone(),
        }
    }
}

impl fmt::Debug for SharedRedbOperationalPorts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SharedRedbOperationalPorts([OPERATIONAL])")
    }
}

/// Centralizes the fail-closed poison policy and guarantees guard-local calls.
#[allow(
    dead_code,
    reason = "used through the WP-130 private bridge once the hosted process graph is constructed"
)]
struct SharedStorageCell<T> {
    inner: Arc<RwLock<T>>,
}

#[allow(
    dead_code,
    reason = "used through the WP-130 private bridge once the hosted process graph is constructed"
)]
impl<T> SharedStorageCell<T> {
    fn new(value: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(value)),
        }
    }

    fn with_mut<R>(
        &self,
        operation: impl FnOnce(&mut T) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        let mut guard = self.inner.write().map_err(|_| poisoned_storage_bridge())?;
        operation(&mut guard)
    }
}

impl<T> Clone for SharedStorageCell<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[allow(
    dead_code,
    reason = "used through the WP-130 private bridge once the hosted process graph is constructed"
)]
fn poisoned_storage_bridge() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}

#[derive(Default)]
struct CurrentCatalogViewState {
    active: Option<ActiveCatalogPointerV1>,
    active_bundle: Option<StoredContractBundleV1>,
}

impl CurrentCatalogViewState {
    fn install_rebuilt(
        &mut self,
        active: ActiveCatalogPointerV1,
        bundle: StoredContractBundleV1,
    ) -> Result<(), StorageError> {
        if !active.matches_bundle(&bundle) {
            return Err(poisoned_storage_bridge());
        }
        self.active = Some(active);
        self.active_bundle = Some(bundle);
        Ok(())
    }

    fn publish_activation(
        &mut self,
        expected_active_version: Option<ContractVersion>,
        active: ActiveCatalogPointerV1,
        bundle: StoredContractBundleV1,
        changed: bool,
    ) -> Result<(), StorageError> {
        if !active.matches_bundle(&bundle) {
            return Err(poisoned_storage_bridge());
        }
        if changed {
            let current_version = self
                .active
                .as_ref()
                .map(ActiveCatalogPointerV1::contract_version);
            if current_version != expected_active_version {
                return Err(poisoned_storage_bridge());
            }
        } else if self.active.as_ref() != Some(&active) {
            return Err(poisoned_storage_bridge());
        }
        self.active = Some(active);
        self.active_bundle = Some(bundle);
        Ok(())
    }

    fn active_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Option<StoredContractBundleV1> {
        self.active_bundle
            .as_ref()
            .filter(|bundle| {
                bundle.lineage() == lineage && bundle.contract_version() == contract_version
            })
            .cloned()
    }
}

struct CurrentCatalogView {
    state: RwLock<CurrentCatalogViewState>,
}

impl CurrentCatalogView {
    fn rebuild_from_clean_startup(ports: &RedbSharedPorts) -> Result<Self, StorageError> {
        let mut state = CurrentCatalogViewState::default();
        if let Some((active, bundle)) = ports.load_clean_startup_active_catalog()? {
            state.install_rebuilt(active, bundle)?;
        }
        Ok(Self {
            state: RwLock::new(state),
        })
    }

    fn rebuild(ports: &RedbSharedPorts) -> Result<Self, StorageError> {
        let active = CatalogRepository::read_active_catalog(ports)?;
        let mut state = CurrentCatalogViewState::default();
        if let Some(active) = active {
            let bundle = CatalogRepository::read_contract_bundle(
                ports,
                active.lineage(),
                active.contract_version(),
            )?
            .ok_or_else(poisoned_storage_bridge)?;
            state.install_rebuilt(active, bundle)?;
        }
        Ok(Self {
            state: RwLock::new(state),
        })
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, CurrentCatalogViewState>, StorageError> {
        self.state.read().map_err(|_| poisoned_storage_bridge())
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, CurrentCatalogViewState>, StorageError> {
        self.state.write().map_err(|_| poisoned_storage_bridge())
    }
}

/// Active-pointer map is catalog-cardinality-bounded (one entry per exact
/// contract identity). Bodies are LRU-cached by hash under this cap.
const MAX_CURRENT_QUERY_MODULE_BODY_CACHE: usize = 4_096;

#[derive(Default)]
struct CurrentQueryModuleViewState {
    /// Active pointer keyed by exact contract identity.
    active: BTreeMap<
        (ContractLineage, ContractVersion, ContractBundleHash),
        ActiveQueryModulePointerV1,
    >,
    /// Negative cache for contract identities with no active module.
    known_absent: BTreeSet<(ContractLineage, ContractVersion, ContractBundleHash)>,
    known_absent_order: VecDeque<(ContractLineage, ContractVersion, ContractBundleHash)>,
    /// Module body cache (hash → body); request path serves hashes when present.
    modules: BTreeMap<QueryModuleHash, StoredQueryModuleV1>,
    module_order: VecDeque<QueryModuleHash>,
}

impl CurrentQueryModuleViewState {
    fn install_rebuilt(
        &mut self,
        pointer: ActiveQueryModulePointerV1,
        module: StoredQueryModuleV1,
    ) -> Result<(), StorageError> {
        if !pointer.matches_module(&module) {
            return Err(poisoned_storage_bridge());
        }
        let key = (
            pointer.contract_lineage().clone(),
            pointer.contract_version(),
            pointer.contract_bundle_hash(),
        );
        self.known_absent.remove(&key);
        self.known_absent_order.retain(|existing| existing != &key);
        self.active.insert(key, pointer);
        self.insert_module(module)?;
        Ok(())
    }

    fn publish_activation(
        &mut self,
        pointer: ActiveQueryModulePointerV1,
        module: StoredQueryModuleV1,
        changed: bool,
    ) -> Result<(), StorageError> {
        if !pointer.matches_module(&module) {
            return Err(poisoned_storage_bridge());
        }
        let key = (
            pointer.contract_lineage().clone(),
            pointer.contract_version(),
            pointer.contract_bundle_hash(),
        );
        if !changed && self.active.get(&key) != Some(&pointer) {
            return Err(poisoned_storage_bridge());
        }
        self.known_absent.remove(&key);
        self.known_absent_order.retain(|existing| existing != &key);
        self.active.insert(key, pointer);
        self.insert_module(module)?;
        Ok(())
    }

    fn insert_module(&mut self, module: StoredQueryModuleV1) -> Result<(), StorageError> {
        let hash = module.module_hash();
        if let std::collections::btree_map::Entry::Occupied(mut entry) = self.modules.entry(hash) {
            entry.insert(module);
            // LRU touch.
            self.module_order.retain(|existing| *existing != hash);
            self.module_order.push_back(hash);
            return Ok(());
        }
        while self.modules.len() >= MAX_CURRENT_QUERY_MODULE_BODY_CACHE {
            let Some(evict) = self.module_order.pop_front() else {
                break;
            };
            self.modules.remove(&evict);
        }
        self.module_order.push_back(hash);
        self.modules.insert(hash, module);
        Ok(())
    }

    fn active(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
    ) -> Option<Option<ActiveQueryModulePointerV1>> {
        let key = (lineage.clone(), contract_version, contract_bundle_hash);
        if let Some(pointer) = self.active.get(&key) {
            return Some(Some(pointer.clone()));
        }
        if self.known_absent.contains(&key) {
            return Some(None);
        }
        None
    }

    fn note_absent(
        &mut self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
    ) {
        let key = (lineage.clone(), contract_version, contract_bundle_hash);
        if self.active.contains_key(&key) || self.known_absent.contains(&key) {
            return;
        }
        while self.known_absent.len() >= MAX_CURRENT_QUERY_MODULE_BODY_CACHE {
            if let Some(evict) = self.known_absent_order.pop_front() {
                self.known_absent.remove(&evict);
            } else {
                break;
            }
        }
        self.known_absent.insert(key.clone());
        self.known_absent_order.push_back(key);
    }

    fn module(&self, hash: QueryModuleHash) -> Option<StoredQueryModuleV1> {
        self.modules.get(&hash).cloned()
    }
}

struct CurrentQueryModuleView {
    state: RwLock<CurrentQueryModuleViewState>,
}

impl CurrentQueryModuleView {
    fn rebuild(ports: &RedbSharedPorts) -> Result<Self, StorageError> {
        let mut state = CurrentQueryModuleViewState::default();
        for (pointer, module) in ports.load_active_query_modules()? {
            state.install_rebuilt(pointer, module)?;
        }
        Ok(Self {
            state: RwLock::new(state),
        })
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, CurrentQueryModuleViewState>, StorageError> {
        self.state.read().map_err(|_| poisoned_storage_bridge())
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, CurrentQueryModuleViewState>, StorageError> {
        self.state.write().map_err(|_| poisoned_storage_bridge())
    }
}

const MAX_CURRENT_CAPABILITY_VIEW_RECORDS: usize = 4_096;
const MAX_KNOWN_ABSENT_CAPABILITY_DIGESTS: usize = 4_096;
type RedbCapabilityCreateCandidate =
    <RedbOperationalPorts as CapabilityAdministrationTransactionPort>::CreateCandidate;
type RedbCapabilityCreateAwaiting =
    <RedbCapabilityCreateCandidate as CapabilityCreateCandidateTransaction>::AwaitingDecision;
type RedbCapabilityRevokeCandidate =
    <RedbOperationalPorts as CapabilityAdministrationTransactionPort>::RevokeCandidate;
type RedbCapabilityRevokeAwaiting =
    <RedbCapabilityRevokeCandidate as CapabilityRevokeCandidateTransaction>::AwaitingDecision;

#[derive(Default)]
struct CurrentCapabilityViewState {
    records: BTreeMap<CapabilityId, StoredCapabilityRecordV1>,
    digests: BTreeMap<CapabilityTokenDigest, CapabilityId>,
    known_absent_digests: BTreeSet<CapabilityTokenDigest>,
    known_absent_order: VecDeque<CapabilityTokenDigest>,
    record_order: VecDeque<CapabilityId>,
    /// Monotonic view generation bumped on every real publish mutation.
    ///
    /// Race window (no wider than today): a recheck concurrent with revoke
    /// either observes the pre-publish generation (revoke not yet published —
    /// same as today's read-before-publish window) or the post-publish
    /// generation (full re-evaluation). Generation is bumped inside
    /// [`CurrentCapabilityViewState::publish`] under the view write lock that
    /// also serializes revoke publication, so the window cannot widen.
    generation: AtomicU64,
}

impl CurrentCapabilityViewState {
    fn insert_rebuilt(&mut self, record: StoredCapabilityRecordV1) -> Result<(), StorageError> {
        let capability_id = record.capability_id();
        let digest = record.token_digest();
        if self.records.contains_key(&capability_id)
            || self
                .digests
                .get(&digest)
                .is_some_and(|existing| *existing != capability_id)
        {
            return Err(poisoned_storage_bridge());
        }
        if self.records.len() >= MAX_CURRENT_CAPABILITY_VIEW_RECORDS {
            // Warm rebuild stops at the cap; remaining records are storage fallthrough.
            return Ok(());
        }
        self.remove_known_absent(digest);
        self.digests.insert(digest, capability_id);
        self.record_order.push_back(capability_id);
        self.records.insert(capability_id, record);
        Ok(())
    }

    fn publish(&mut self, record: StoredCapabilityRecordV1) -> Result<(), StorageError> {
        let capability_id = record.capability_id();
        let digest = record.token_digest();
        if let Some(existing) = self.records.get(&capability_id) {
            if existing == &record {
                return Ok(());
            }
            // Monotonicity enforced only for resident entries.
            let expected_revision = existing
                .revision()
                .get()
                .checked_add(1)
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(poisoned_storage_bridge)?;
            if record.revision() != expected_revision || existing.token_digest() != digest {
                return Err(poisoned_storage_bridge());
            }
        } else if self.records.len() >= MAX_CURRENT_CAPABILITY_VIEW_RECORDS {
            self.evict_one_record();
        } else if record.revision() != std::num::NonZeroU64::MIN {
            // Cache-fill accepts stored revision for non-resident inserts.
        }
        if self
            .digests
            .get(&digest)
            .is_some_and(|existing| *existing != capability_id)
        {
            return Err(poisoned_storage_bridge());
        }
        // Bump before installing the new record so concurrent rechecks either
        // see the old generation (revoke/create not yet published) or the new
        // generation (full re-eval). Same race window as today's view publish.
        self.generation.fetch_add(1, Ordering::Release);
        self.remove_known_absent(digest);
        if !self.records.contains_key(&capability_id) {
            self.record_order.push_back(capability_id);
        }
        self.digests.insert(digest, capability_id);
        self.records.insert(capability_id, record);
        Ok(())
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn evict_one_record(&mut self) {
        while let Some(id) = self.record_order.pop_front() {
            if let Some(record) = self.records.remove(&id) {
                self.digests.remove(&record.token_digest());
                return;
            }
        }
    }

    fn remove_known_absent(&mut self, digest: CapabilityTokenDigest) {
        if self.known_absent_digests.remove(&digest) {
            self.known_absent_order.retain(|value| *value != digest);
        }
    }

    fn note_absent(&mut self, digest: CapabilityTokenDigest) {
        if self.digests.contains_key(&digest) {
            return;
        }
        if self.known_absent_digests.contains(&digest) {
            // True LRU: refresh recency without growing the set.
            self.known_absent_order.retain(|value| *value != digest);
            self.known_absent_order.push_back(digest);
            return;
        }
        while self.known_absent_digests.len() >= MAX_KNOWN_ABSENT_CAPABILITY_DIGESTS {
            if let Some(evict) = self.known_absent_order.pop_front() {
                self.known_absent_digests.remove(&evict);
            } else {
                break;
            }
        }
        self.known_absent_digests.insert(digest);
        self.known_absent_order.push_back(digest);
    }

    fn resolve(&self, candidates: &[CapabilityTokenDigest]) -> Option<CapabilityLookupResult> {
        let mut matched = None;
        for candidate in candidates {
            let Some(capability_id) = self.digests.get(candidate) else {
                if self.known_absent_digests.contains(candidate) {
                    continue;
                }
                return None;
            };
            let Some(record) = self.records.get(capability_id) else {
                return Some(CapabilityLookupResult::MultipleMatches);
            };
            if matched.is_some() {
                return Some(CapabilityLookupResult::MultipleMatches);
            }
            matched = Some(record.clone());
        }
        Some(matched.map_or(CapabilityLookupResult::NotFound, |record| {
            CapabilityLookupResult::Found(Box::new(record))
        }))
    }

    fn note_lookup(
        &mut self,
        candidates: &[CapabilityTokenDigest],
        result: &CapabilityLookupResult,
    ) -> Result<(), StorageError> {
        if let CapabilityLookupResult::Found(record) = result {
            self.publish((**record).clone())?;
        }
        for candidate in candidates {
            if !self.digests.contains_key(candidate) {
                self.note_absent(*candidate);
            }
        }
        Ok(())
    }
}

struct CurrentCapabilityView {
    state: RwLock<CurrentCapabilityViewState>,
}

impl CurrentCapabilityView {
    fn cold() -> Self {
        Self {
            state: RwLock::new(CurrentCapabilityViewState::default()),
        }
    }

    fn rebuild(ports: &RedbSharedPorts) -> Result<Self, StorageError> {
        let mut state = CurrentCapabilityViewState::default();
        let limit = StorageScanLimit::new(500).expect("fixed inventory page limit is valid");
        let mut after = None;
        loop {
            let page = ports.scan_capabilities(after, limit)?;
            let has_more = page.has_more();
            for record in page.into_records() {
                after = Some(record.capability_id());
                state.insert_rebuilt(record)?;
            }
            if !has_more {
                break;
            }
        }
        Ok(Self {
            state: RwLock::new(state),
        })
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, CurrentCapabilityViewState>, StorageError> {
        self.state.read().map_err(|_| poisoned_storage_bridge())
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, CurrentCapabilityViewState>, StorageError> {
        self.state.write().map_err(|_| poisoned_storage_bridge())
    }
}

pub(crate) struct SharedCapabilityCreateCandidate {
    inner: RedbCapabilityCreateCandidate,
    storage: SharedRedbOperationalPorts,
}

pub(crate) struct SharedCapabilityCreateAwaiting {
    inner: RedbCapabilityCreateAwaiting,
    storage: SharedRedbOperationalPorts,
}

pub(crate) struct SharedCapabilityRevokeCandidate {
    inner: RedbCapabilityRevokeCandidate,
    storage: SharedRedbOperationalPorts,
}

pub(crate) struct SharedCapabilityRevokeAwaiting {
    inner: RedbCapabilityRevokeAwaiting,
    storage: SharedRedbOperationalPorts,
}

impl AdmissionRepository for SharedRedbOperationalPorts {
    fn admit_or_resolve(
        &self,
        request: AdmissionRequestV1,
    ) -> Result<AdmissionResultV1, StorageError> {
        AdmissionRepository::admit_or_resolve(&self.shared, request)
    }

    fn admit_or_resolve_group(
        &self,
        requests: Vec<AdmissionRequestV1>,
    ) -> Result<Vec<AdmissionResultV1>, StorageError> {
        AdmissionRepository::admit_or_resolve_group(&self.shared, requests)
    }

    fn lookup_admission(
        &self,
        candidates: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        AdmissionRepository::lookup_admission(&self.shared, candidates)
    }

    fn lookup_admission_group(
        &self,
        candidates: Vec<IdempotencyLookupCandidatesV1>,
    ) -> Result<Vec<AdmissionLookupResultV1>, StorageError> {
        AdmissionRepository::lookup_admission_group(&self.shared, candidates)
    }
}

impl AuditedAdmissionRepository for SharedRedbOperationalPorts {
    fn admit_or_resolve_audited_group(
        &self,
        requests: Vec<AuditedAdmissionRequestV1>,
    ) -> Result<Vec<AuditedAdmissionResultV1>, StorageError> {
        AuditedAdmissionRepository::admit_or_resolve_audited_group(&self.shared, requests)
    }
}

impl SnapshotReader for SharedRedbOperationalPorts {
    fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
        SnapshotReader::read_snapshot(&self.shared, request)
    }

    fn read_snapshot_group(
        &self,
        requests: Vec<SnapshotRequest>,
    ) -> Result<Vec<ReadSnapshot>, StorageError> {
        SnapshotReader::read_snapshot_group(&self.shared, requests)
    }
}

impl ApplicationCommandTransactionPort for SharedRedbOperationalPorts {
    type EmptyBatch = <RedbOperationalPorts as ApplicationCommandTransactionPort>::EmptyBatch;

    fn begin_empty_batch(&self) -> Result<Self::EmptyBatch, StorageError> {
        ApplicationCommandTransactionPort::begin_empty_batch(&self.shared)
    }
}

impl DeferredCommandEpochPort for SharedRedbOperationalPorts {
    type Epoch = <RedbOperationalPorts as DeferredCommandEpochPort>::Epoch;

    fn begin_deferred_command_epoch(&self) -> Result<Self::Epoch, StorageError> {
        DeferredCommandEpochPort::begin_deferred_command_epoch(&self.shared)
    }
}

impl ExecutionFailureTransitionPort for SharedRedbOperationalPorts {
    type Rechecked = <RedbOperationalPorts as ExecutionFailureTransitionPort>::Rechecked;

    fn begin_execution_failure(
        &self,
        request: ExecutionFailureTransitionRequestV1,
    ) -> Result<ExecutionFailureAdmissionResult<Self::Rechecked>, StorageError> {
        ExecutionFailureTransitionPort::begin_execution_failure(&self.shared, request)
    }
}

impl ServiceAuditAppendRepository for SharedRedbOperationalPorts {
    fn append_service_audit(
        &mut self,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError> {
        self.cell
            .with_mut(|ports| ServiceAuditAppendRepository::append_service_audit(ports, intent))
    }

    fn append_service_audit_group(
        &mut self,
        intents: &[ServiceAuditAppendIntentV1],
    ) -> Result<Vec<ServiceAuditAppendResult>, StorageError> {
        self.cell.with_mut(|ports| {
            ServiceAuditAppendRepository::append_service_audit_group(ports, intents)
        })
    }

    fn submit_service_audit_group(
        &mut self,
        intents: &[ServiceAuditAppendIntentV1],
    ) -> Result<ServiceAuditGroupAppend, StorageError> {
        self.cell.with_mut(|ports| {
            ServiceAuditAppendRepository::submit_service_audit_group(ports, intents)
        })
    }

    fn append_service_audit_fused_pair(
        &mut self,
        started: &ServiceAuditAppendIntentV1,
        terminal: &ServiceAuditAppendIntentV1,
    ) -> Result<(), StorageError> {
        self.cell.with_mut(|ports| {
            ServiceAuditAppendRepository::append_service_audit_fused_pair(ports, started, terminal)
        })
    }
}

impl CatalogAdministrationRepository for SharedRedbOperationalPorts {
    fn activate_catalog(
        &mut self,
        intent: &CatalogActivationIntentV1,
    ) -> Result<CatalogActivationResult, StorageError> {
        let mut view = self.catalog.write()?;
        let result = self
            .cell
            .with_mut(|ports| CatalogAdministrationRepository::activate_catalog(ports, intent))?;
        let publication = match &result {
            CatalogActivationResult::Activated { active, .. } => view.publish_activation(
                intent.expected_active_version(),
                active.clone(),
                intent.bundle().clone(),
                true,
            ),
            CatalogActivationResult::AlreadyActive { active, .. } => view.publish_activation(
                intent.expected_active_version(),
                active.clone(),
                intent.bundle().clone(),
                false,
            ),
            CatalogActivationResult::ExpectedActiveVersionMismatch { .. }
            | CatalogActivationResult::BundleConflict => Ok(()),
        };
        if let Err(error) = publication {
            return Err(self.current_view_failure(error));
        }
        Ok(result)
    }
}

impl QueryModuleAdministrationRepository for SharedRedbOperationalPorts {
    fn activate_query_module(
        &mut self,
        intent: &QueryModuleActivationIntentV1,
    ) -> Result<QueryModuleActivationResult, StorageError> {
        let mut view = self.query_modules.write()?;
        let result = self.cell.with_mut(|ports| {
            QueryModuleAdministrationRepository::activate_query_module(ports, intent)
        })?;
        let publication = match &result {
            QueryModuleActivationResult::Activated { active, .. } => {
                view.publish_activation(active.clone(), intent.module().clone(), true)
            }
            QueryModuleActivationResult::AlreadyActive { active, .. } => {
                view.publish_activation(active.clone(), intent.module().clone(), false)
            }
            QueryModuleActivationResult::ContractUnavailable
            | QueryModuleActivationResult::ModuleVersionConflict
            | QueryModuleActivationResult::ExpectedActiveMismatch { .. } => Ok(()),
        };
        if let Err(error) = publication {
            return Err(self.current_view_failure(error));
        }
        Ok(result)
    }
}

impl ReactiveModuleAdministrationRepository for SharedRedbOperationalPorts {
    fn publish_reactive_module(
        &mut self,
        intent: &ReactiveModulePublicationIntentV1,
    ) -> Result<ReactiveModulePublicationResult, StorageError> {
        self.cell.with_mut(|ports| {
            ReactiveModuleAdministrationRepository::publish_reactive_module(ports, intent)
        })
    }
}

impl ReactiveModuleRepository for SharedRedbOperationalPorts {
    fn read_reactive_module(
        &self,
        module_hash: ReactiveModuleHash,
    ) -> Result<Option<StoredReactiveModuleV1>, StorageError> {
        self.cell
            .with_mut(|ports| ReactiveModuleRepository::read_reactive_module(ports, module_hash))
    }
}

impl ApplicationInstallationCampaignRepository for SharedRedbOperationalPorts {
    fn read_application_installation_campaign(
        &self,
        campaign_id: riffdb_types::ApplicationInstallationCampaignId,
    ) -> Result<Option<StoredApplicationInstallationCampaignV1>, StorageError> {
        self.cell.with_mut(|ports| {
            ApplicationInstallationCampaignRepository::read_application_installation_campaign(
                ports,
                campaign_id,
            )
        })
    }

    fn compare_and_swap_application_installation_campaign(
        &mut self,
        expected: Option<&StoredApplicationInstallationCampaignV1>,
        replacement: &StoredApplicationInstallationCampaignV1,
    ) -> Result<ApplicationInstallationCampaignWriteResultV1, StorageError> {
        self.cell.with_mut(|ports| {
            ApplicationInstallationCampaignRepository::compare_and_swap_application_installation_campaign(
                ports,
                expected,
                replacement,
            )
        })
    }
}

impl ApplicationExportOperationRepository for SharedRedbOperationalPorts {
    fn read_application_export_operation(
        &self,
        operation_id: riffdb_types::ApplicationExportOperationId,
    ) -> Result<Option<StoredApplicationExportOperationV1>, StorageError> {
        self.cell.with_mut(|ports| {
            ApplicationExportOperationRepository::read_application_export_operation(
                ports,
                operation_id,
            )
        })
    }

    fn compare_and_swap_application_export_operation(
        &mut self,
        expected: Option<&StoredApplicationExportOperationV1>,
        replacement: &StoredApplicationExportOperationV1,
    ) -> Result<ApplicationExportOperationWriteResultV1, StorageError> {
        self.cell.with_mut(|ports| {
            ApplicationExportOperationRepository::compare_and_swap_application_export_operation(
                ports,
                expected,
                replacement,
            )
        })
    }

    fn list_application_export_operations(
        &self,
        maximum: usize,
    ) -> Result<Vec<StoredApplicationExportOperationV1>, StorageError> {
        self.cell.with_mut(|ports| {
            ApplicationExportOperationRepository::list_application_export_operations(ports, maximum)
        })
    }
}

impl ApplicationExportSnapshotPort for SharedRedbOperationalPorts {
    fn capture_application_export_snapshot(
        &self,
        lineage: &ContractLineage,
    ) -> Result<Arc<dyn ApplicationExportSnapshotReader>, StorageError> {
        self.cell.with_mut(|ports| {
            ApplicationExportSnapshotPort::capture_application_export_snapshot(ports, lineage)
        })
    }
}

impl VectorProjectionControlRepository for SharedRedbOperationalPorts {
    fn read_vector_projection_control(
        &self,
        source: &VectorProjectionSourceV1,
    ) -> Result<Option<StoredVectorProjectionControlV1>, StorageError> {
        VectorProjectionControlRepository::read_vector_projection_control(&self.shared, source)
    }

    fn compare_and_set_vector_projection_control(
        &self,
        expected: Option<&StoredVectorProjectionControlV1>,
        replacement: &StoredVectorProjectionControlV1,
    ) -> Result<VectorProjectionControlWriteResultV1, StorageError> {
        VectorProjectionControlRepository::compare_and_set_vector_projection_control(
            &self.shared,
            expected,
            replacement,
        )
    }

    fn attached_vector_projection_frontiers(
        &self,
    ) -> Result<Vec<riffdb_types::FrontierPosition>, StorageError> {
        VectorProjectionControlRepository::attached_vector_projection_frontiers(&self.shared)
    }
}

impl ColumnarProjectionControlRepository for SharedRedbOperationalPorts {
    fn initialize_fresh_v1(
        &self,
        controls: &[FreshColumnarProjectionControlV1],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        ColumnarProjectionControlRepository::initialize_fresh_v1(&self.shared, controls)
    }

    fn begin_v2_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        physical_generation_fingerprint: [u8; 32],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        ColumnarProjectionControlRepository::begin_v2_candidate(
            &self.shared,
            expected,
            physical_generation_fingerprint,
        )
    }

    fn allocate_same_spec_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        physical_generation_fingerprint: [u8; 32],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        ColumnarProjectionControlRepository::allocate_same_spec_candidate(
            &self.shared,
            expected,
            physical_generation_fingerprint,
        )
    }

    fn allocate_unservable_rebuild_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        replay_limits: ColumnarProjectionReplayLimitsV1,
        layout: ColumnarProjectionLayoutV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        ColumnarProjectionControlRepository::allocate_unservable_rebuild_candidate(
            &self.shared,
            expected,
            target_definition_fingerprint,
            target_spec_hash,
            replay_limits,
            layout,
            physical_generation_fingerprint,
        )
    }

    fn record_candidate_failure(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        reason: ColumnarProjectionFailureReasonV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        ColumnarProjectionControlRepository::record_candidate_failure(
            &self.shared,
            expected,
            reason,
        )
    }

    fn replace_failed_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        layout: ColumnarProjectionLayoutV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        ColumnarProjectionControlRepository::replace_failed_candidate(
            &self.shared,
            expected,
            layout,
            physical_generation_fingerprint,
        )
    }

    fn retarget_initial_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        replay_limits: ColumnarProjectionReplayLimitsV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        ColumnarProjectionControlRepository::retarget_initial_candidate(
            &self.shared,
            expected,
            target_definition_fingerprint,
            target_spec_hash,
            replay_limits,
        )
    }

    fn record_published_failure(
        &self,
        expected: &StoredColumnarProjectionControlV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        ColumnarProjectionControlRepository::record_published_failure(&self.shared, expected)
    }

    fn recover_expected_control(
        &self,
        source: &ColumnarProjectionSourceV1,
    ) -> Result<Option<StoredColumnarProjectionControlV1>, StorageError> {
        ColumnarProjectionControlRepository::recover_expected_control(&self.shared, source)
    }

    fn mark_invalid(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        reason: ColumnarProjectionFailureReasonV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        ColumnarProjectionControlRepository::mark_invalid(&self.shared, expected, reason)
    }
}

impl PreparedColumnarGenerationRepository for SharedRedbOperationalPorts {
    fn record_durable_snapshot(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        prepared: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        PreparedColumnarGenerationRepository::record_durable_snapshot(
            &self.shared,
            expected,
            prepared,
            process_generation,
        )
    }

    fn record_candidate_frontier(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        PreparedColumnarGenerationRepository::record_candidate_frontier(
            &self.shared,
            expected,
            replacement,
            process_generation,
        )
    }

    fn advance_published_v1(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        PreparedColumnarGenerationRepository::advance_published_v1(
            &self.shared,
            expected,
            replacement,
            process_generation,
        )
    }

    fn publish_prepared_generation(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        prepared: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        PreparedColumnarGenerationRepository::publish_prepared_generation(
            &self.shared,
            expected,
            prepared,
            process_generation,
        )
    }
}

impl ColumnarProjectionRetentionRepository for SharedRedbOperationalPorts {
    fn columnar_projection_retention_frontiers(
        &self,
    ) -> Result<Vec<(ColumnarProjectionSourceV1, Option<FrontierPosition>)>, StorageError> {
        ColumnarProjectionRetentionRepository::columnar_projection_retention_frontiers(&self.shared)
    }
}

impl EventConsumerRepository for SharedRedbOperationalPorts {
    fn inspect_event_consumer(
        &self,
        consumer_identity_hash: EventConsumerIdentityHash,
    ) -> Result<Option<EventConsumerSnapshotV1>, StorageError> {
        self.cell.with_mut(|ports| {
            EventConsumerRepository::inspect_event_consumer(ports, consumer_identity_hash)
        })
    }

    fn transition_event_consumer(
        &mut self,
        transition: EventConsumerTransitionV1,
    ) -> Result<EventConsumerTransitionResultV1, StorageError> {
        self.cell
            .with_mut(|ports| EventConsumerRepository::transition_event_consumer(ports, transition))
    }

    fn inspect_event_consumer_inventory(
        &self,
    ) -> Result<Vec<EventConsumerSnapshotV1>, StorageError> {
        self.cell
            .with_mut(|ports| EventConsumerRepository::inspect_event_consumer_inventory(ports))
    }

    fn event_consumer_retention_low_water(&self) -> Result<Option<u64>, StorageError> {
        self.cell
            .with_mut(|ports| EventConsumerRepository::event_consumer_retention_low_water(ports))
    }
}

impl QueryModuleRepository for SharedRedbOperationalPorts {
    fn read_query_module(
        &self,
        module_hash: QueryModuleHash,
    ) -> Result<Option<StoredQueryModuleV1>, StorageError> {
        if let Some(module) = self.query_modules.read()?.module(module_hash) {
            return Ok(Some(module));
        }
        let module = QueryModuleRepository::read_query_module(&self.shared, module_hash)?;
        if let Some(module) = module.as_ref() {
            let mut view = self.query_modules.write()?;
            view.insert_module(module.clone())?;
        }
        Ok(module)
    }

    fn read_active_query_module(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
    ) -> Result<Option<ActiveQueryModulePointerV1>, StorageError> {
        if let Some(cached) =
            self.query_modules
                .read()?
                .active(lineage, contract_version, contract_bundle_hash)
        {
            return Ok(cached);
        }
        // Cold path once: load from storage and publish into the process view.
        let active = QueryModuleRepository::read_active_query_module(
            &self.shared,
            lineage,
            contract_version,
            contract_bundle_hash,
        )?;
        let mut view = self.query_modules.write()?;
        match active.as_ref() {
            Some(pointer) => {
                let module =
                    QueryModuleRepository::read_query_module(&self.shared, pointer.module_hash())?
                        .ok_or_else(poisoned_storage_bridge)?;
                view.install_rebuilt(pointer.clone(), module)?;
            }
            None => view.note_absent(lineage, contract_version, contract_bundle_hash),
        }
        Ok(active)
    }
}

impl CapabilityAdministrationTransactionPort for SharedRedbOperationalPorts {
    type CreateCandidate = SharedCapabilityCreateCandidate;
    type RevokeCandidate = SharedCapabilityRevokeCandidate;

    fn begin_capability_create(
        &self,
        candidate: CapabilityCreateCandidateV1,
    ) -> Result<Self::CreateCandidate, StorageError> {
        let inner = CapabilityAdministrationTransactionPort::begin_capability_create(
            &self.shared,
            candidate,
        )?;
        Ok(SharedCapabilityCreateCandidate {
            inner,
            storage: self.clone(),
        })
    }

    fn begin_capability_revoke(
        &self,
        candidate: CapabilityRevokeCandidateV1,
    ) -> Result<Self::RevokeCandidate, StorageError> {
        let inner = CapabilityAdministrationTransactionPort::begin_capability_revoke(
            &self.shared,
            candidate,
        )?;
        Ok(SharedCapabilityRevokeCandidate {
            inner,
            storage: self.clone(),
        })
    }
}

impl CapabilityCreateCandidateTransaction for SharedCapabilityCreateCandidate {
    type AwaitingDecision = SharedCapabilityCreateAwaiting;

    fn read_transaction_current(
        self,
    ) -> Result<
        (
            Self::AwaitingDecision,
            riffdb_storage_api::CapabilityMutationCurrentStateV1,
        ),
        StorageError,
    > {
        let Self { inner, storage } = self;
        let (inner, current) = inner.read_transaction_current()?;
        Ok((SharedCapabilityCreateAwaiting { inner, storage }, current))
    }

    fn abandon(self) -> CapabilityCreateCandidateV1 {
        self.inner.abandon()
    }
}

impl CapabilityCreateAwaitingDecision for SharedCapabilityCreateAwaiting {
    fn commit_create(
        self,
        intent: CapabilityCreateIntentV1,
    ) -> Result<CapabilityCreateResult, StorageError> {
        let Self { inner, storage } = self;
        let mut view = storage.capabilities.write()?;
        let result = inner.commit_create(intent)?;
        let capability_id = match result {
            CapabilityCreateResult::Created { capability_id, .. }
            | CapabilityCreateResult::AlreadyCreated { capability_id, .. } => Some(capability_id),
            CapabilityCreateResult::CapabilityIdConflict
            | CapabilityCreateResult::TokenDigestCollision => None,
        };
        if let Some(capability_id) = capability_id {
            let record = CapabilityReader::read_capability(&storage.shared, capability_id)
                .map_err(|error| storage.current_view_failure(error))?
                .ok_or_else(poisoned_storage_bridge)
                .map_err(|error| storage.current_view_failure(error))?;
            if let Err(error) = view.publish(record) {
                return Err(storage.current_view_failure(error));
            }
        }
        Ok(result)
    }

    fn abandon(self) -> CapabilityCreateCandidateV1 {
        self.inner.abandon()
    }
}

impl CapabilityRevokeCandidateTransaction for SharedCapabilityRevokeCandidate {
    type AwaitingDecision = SharedCapabilityRevokeAwaiting;

    fn read_transaction_current(
        self,
    ) -> Result<
        (
            Self::AwaitingDecision,
            riffdb_storage_api::CapabilityMutationCurrentStateV1,
        ),
        StorageError,
    > {
        let Self { inner, storage } = self;
        let (inner, current) = inner.read_transaction_current()?;
        Ok((SharedCapabilityRevokeAwaiting { inner, storage }, current))
    }

    fn abandon(self) -> CapabilityRevokeCandidateV1 {
        self.inner.abandon()
    }
}

impl CapabilityRevokeAwaitingDecision for SharedCapabilityRevokeAwaiting {
    fn commit_revoke(
        self,
        intent: CapabilityRevokeIntentV1,
    ) -> Result<CapabilityRevokeResult, StorageError> {
        let Self { inner, storage } = self;
        let mut view = storage.capabilities.write()?;
        let result = inner.commit_revoke(intent)?;
        let capability_id = match result {
            CapabilityRevokeResult::Revoked { capability_id, .. }
            | CapabilityRevokeResult::AlreadyRevoked { capability_id, .. } => Some(capability_id),
            CapabilityRevokeResult::CapabilityNotFound => None,
        };
        if let Some(capability_id) = capability_id {
            let record = CapabilityReader::read_capability(&storage.shared, capability_id)
                .map_err(|error| storage.current_view_failure(error))?
                .ok_or_else(poisoned_storage_bridge)
                .map_err(|error| storage.current_view_failure(error))?;
            if let Err(error) = view.publish(record) {
                return Err(storage.current_view_failure(error));
            }
        }
        Ok(result)
    }

    fn abandon(self) -> CapabilityRevokeCandidateV1 {
        self.inner.abandon()
    }
}

impl CapabilityBootstrapAdministrationRepository for SharedRedbOperationalPorts {
    fn bootstrap_capability(
        &mut self,
        intent: &CapabilityBootstrapIntentV1,
    ) -> Result<CapabilityBootstrapResult, StorageError> {
        let mut view = self.capabilities.write()?;
        let result = self.cell.with_mut(|ports| {
            CapabilityBootstrapAdministrationRepository::bootstrap_capability(ports, intent)
        })?;
        let capability_id = match result {
            CapabilityBootstrapResult::BootstrapCreated { capability_id, .. }
            | CapabilityBootstrapResult::BootstrapReplayed { capability_id, .. } => {
                Some(capability_id)
            }
            CapabilityBootstrapResult::BootstrapConflict => None,
        };
        if let Some(capability_id) = capability_id {
            let record = CapabilityReader::read_capability(&self.shared, capability_id)
                .map_err(|error| self.current_view_failure(error))?
                .ok_or_else(poisoned_storage_bridge)
                .map_err(|error| self.current_view_failure(error))?;
            if let Err(error) = view.publish(record) {
                return Err(self.current_view_failure(error));
            }
        }
        Ok(result)
    }
}

impl CatalogRepository for SharedRedbOperationalPorts {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(self.catalog.read()?.active.clone())
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        if let Some(bundle) = self
            .catalog
            .read()?
            .active_bundle(lineage, contract_version)
        {
            return Ok(Some(bundle));
        }
        CatalogRepository::read_contract_bundle(&self.shared, lineage, contract_version)
    }

    fn read_contract_migration_edge(
        &self,
        predecessor: ContractBundleHash,
    ) -> Result<Option<StoredContractMigrationEdgeV1>, StorageError> {
        CatalogRepository::read_contract_migration_edge(&self.shared, predecessor)
    }
}

impl CapabilityReader for SharedRedbOperationalPorts {
    fn read_capability(
        &self,
        capability_id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
        if let Some(record) = self.capabilities.read()?.records.get(&capability_id) {
            return Ok(Some(record.clone()));
        }
        let record = CapabilityReader::read_capability(&self.shared, capability_id)?;
        if let Some(record) = record.as_ref() {
            let mut view = self.capabilities.write()?;
            if let Some(current) = view.records.get(&capability_id) {
                return Ok(Some(current.clone()));
            }
            if let Err(error) = view.publish(record.clone()) {
                return Err(self.current_view_failure(error));
            }
        }
        Ok(record)
    }

    fn capability_view_generation(&self) -> Option<u64> {
        // A poisoned view is not a generation. Returning any constant would let
        // two unreadable observations compare equal and admit a revision-checked
        // reissue over state nobody can read, so this fails closed with `None`
        // and the caller performs a full evaluation.
        self.capabilities.read().ok().map(|view| view.generation())
    }

    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        if let Some(cached) = self.capabilities.read()?.resolve(candidates) {
            return Ok(cached);
        }
        let result = CapabilityReader::resolve_capability_digests(&self.shared, candidates)?;
        let mut view = self.capabilities.write()?;
        if let Some(cached) = view.resolve(candidates) {
            return Ok(cached);
        }
        // Re-validate Found records under the write lock so a concurrent revoke
        // cannot install a stale Active view after an eviction gap.
        let result = match &result {
            CapabilityLookupResult::Found(record) => {
                let fresh =
                    CapabilityReader::read_capability(&self.shared, record.capability_id())?
                        .ok_or_else(poisoned_storage_bridge)
                        .map_err(|error| self.current_view_failure(error))?;
                if fresh.token_digest() != record.token_digest() {
                    return Err(self.current_view_failure(poisoned_storage_bridge()));
                }
                CapabilityLookupResult::Found(Box::new(fresh))
            }
            other => other.clone(),
        };
        if let Err(error) = view.note_lookup(candidates, &result) {
            return Err(self.current_view_failure(error));
        }
        Ok(result)
    }
}

impl CapabilityInventoryReader for SharedRedbOperationalPorts {
    fn scan_capabilities(
        &self,
        after: Option<CapabilityId>,
        limit: StorageScanLimit,
    ) -> Result<riffdb_storage_api::CapabilityInventoryPageV1, StorageError> {
        CapabilityInventoryReader::scan_capabilities(&self.shared, after, limit)
    }
}

impl VectorObservationRepository for SharedRedbOperationalPorts {
    fn read_vector_observation(
        &self,
        target: &VectorObservationTargetV1,
    ) -> Result<Option<VectorObservationCountsV1>, StorageError> {
        VectorObservationRepository::read_vector_observation(&self.shared, target)
    }

    fn read_vector_health_observation(
        &self,
        lineage: &riffdb_types::ContractLineage,
    ) -> Result<Option<riffdb_storage_api::VectorHealthObservationV1>, StorageError> {
        VectorObservationRepository::read_vector_health_observation(&self.shared, lineage)
    }
}

impl VectorEvidenceIndexRepository for SharedRedbOperationalPorts {
    fn scan_vector_evidence_index(
        &self,
        request: &VectorEvidenceIndexScanRequestV1,
    ) -> Result<VectorEvidenceIndexPageV1, StorageError> {
        VectorEvidenceIndexRepository::scan_vector_evidence_index(&self.shared, request)
    }
}

impl AuthoritativePointReader for SharedRedbOperationalPorts {
    fn read_entity(
        &self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        AuthoritativePointReader::read_entity(&self.shared, target)
    }

    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        AuthoritativePointReader::read_stored_outcome(&self.shared, identity)
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        AuthoritativePointReader::read_commit(&self.shared, sequence)
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        AuthoritativePointReader::read_provenance(&self.shared, provenance_id)
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        AuthoritativePointReader::read_durable_event(&self.shared, event_id)
    }
}

impl PartitionEventRouteReader for SharedRedbOperationalPorts {
    fn scan_partition_event_routes(
        &self,
        request: EventRouteScanRequestV1,
    ) -> Result<EventRouteScanV1, StorageError> {
        PartitionEventRouteReader::scan_partition_event_routes(&self.shared, request)
    }
}

impl AuthoritativeScanReader for SharedRedbOperationalPorts {
    fn scan_entity_partition(
        &self,
        request: riffdb_storage_api::AuthoritativeEntityPartitionScanRequest,
    ) -> Result<riffdb_storage_api::AuthoritativeEntityPartitionScanPage, StorageError> {
        AuthoritativeScanReader::scan_entity_partition(&self.shared, request)
    }

    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        AuthoritativeScanReader::scan_index(&self.shared, request)
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        AuthoritativeScanReader::scan_commits(&self.shared, request)
    }
}

impl FilteredAuthoritativeScanReader for SharedRedbOperationalPorts {
    fn scan_index_filtered(
        &self,
        request: FilteredAuthoritativeIndexScanRequest,
    ) -> Result<FilteredAuthoritativeIndexScanPage, StorageError> {
        FilteredAuthoritativeScanReader::scan_index_filtered(&self.shared, request)
    }
}

impl OutboxRepository for SharedRedbOperationalPorts {
    fn has_undelivered_outbox(&self) -> Result<bool, StorageError> {
        OutboxRepository::has_undelivered_outbox(&self.shared)
    }

    fn read_outbox_status(
        &self,
        event_id: EventId,
    ) -> Result<OutboxStatusReadResultV1, StorageError> {
        OutboxRepository::read_outbox_status(&self.shared, event_id)
    }

    fn scan_pending_outbox(
        &self,
        after: Option<EventId>,
        limit: OutboxPageLimit,
    ) -> Result<PendingOutboxScanV1, StorageError> {
        OutboxRepository::scan_pending_outbox(&self.shared, after, limit)
    }

    fn scan_undelivered_outbox_statuses(
        &self,
        request: UndeliveredOutboxStatusScanRequestV1,
    ) -> Result<UndeliveredOutboxStatusScanV1, StorageError> {
        OutboxRepository::scan_undelivered_outbox_statuses(&self.shared, request)
    }

    fn claim_outbox(
        &mut self,
        transition: &OutboxClaimV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.cell
            .with_mut(|ports| OutboxRepository::claim_outbox(ports, transition))
    }

    fn renew_outbox(
        &mut self,
        transition: &OutboxRenewV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.cell
            .with_mut(|ports| OutboxRepository::renew_outbox(ports, transition))
    }

    fn succeed_outbox(
        &mut self,
        transition: &OutboxSucceedV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.cell
            .with_mut(|ports| OutboxRepository::succeed_outbox(ports, transition))
    }

    fn retry_outbox(
        &mut self,
        transition: &OutboxRetryV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.cell
            .with_mut(|ports| OutboxRepository::retry_outbox(ports, transition))
    }

    fn dead_letter_outbox(
        &mut self,
        transition: &OutboxDeadLetterV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.cell
            .with_mut(|ports| OutboxRepository::dead_letter_outbox(ports, transition))
    }
}

impl ProjectionApplySnapshotReader for SharedRedbOperationalPorts {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        ProjectionApplySnapshotReader::read_apply_snapshot(&self.shared, request)
    }
}

impl ProjectionMutationRepository for SharedRedbOperationalPorts {
    fn apply_projection(
        &mut self,
        request: &ProjectionApplyRequestV1,
    ) -> Result<ProjectionApplyResult, StorageError> {
        self.cell
            .with_mut(|ports| ProjectionMutationRepository::apply_projection(ports, request))
    }

    fn transition_projection_control(
        &mut self,
        operation: ProjectionControlOperation,
    ) -> Result<ProjectionControlResult, StorageError> {
        self.cell.with_mut(|ports| {
            ProjectionMutationRepository::transition_projection_control(ports, operation)
        })
    }
}

impl ProjectionQueryReader for SharedRedbOperationalPorts {
    fn query_projection(
        &self,
        request: &ProjectionQueryRequest,
    ) -> Result<ProjectionQueryResult, StorageError> {
        ProjectionQueryReader::query_projection(&self.shared, request)
    }

    fn read_projection_status(
        &self,
        identity: &riffdb_types::ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError> {
        ProjectionQueryReader::read_projection_status(&self.shared, identity)
    }
}

impl ProjectionRecoveryRepository for SharedRedbOperationalPorts {
    fn scan_projection_controls(
        &self,
        after: Option<&riffdb_types::ProjectionIdentity>,
        limit: ProjectionRecoveryPageLimit,
    ) -> Result<ProjectionControlScanV1, StorageError> {
        ProjectionRecoveryRepository::scan_projection_controls(&self.shared, after, limit)
    }

    fn validate_projection_recovery_page(
        &self,
        request: &ProjectionRecoveryValidationRequestV1,
    ) -> Result<ProjectionRecoveryValidationResultV1, StorageError> {
        ProjectionRecoveryRepository::validate_projection_recovery_page(&self.shared, request)
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::thread;

    use riffdb_storage_api::{
        AuditPrincipalV1, CapabilityPermissionV1, CapabilityPermissionsV1,
        CapabilityRequestedRecordV1, PartitionScopeV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, Audience, CapabilityGrantV1,
        ContractBundleHash, DatabaseId, DigestKeyId, Environment, RequestId,
        RevocationReasonCodeV1, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1,
        ServiceIngressKindV1, ServiceOperationV1, TenantScope, Timestamp,
    };

    use crate::real_storage_support::RealStorage;

    use super::*;

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn capability_record() -> StoredCapabilityRecordV1 {
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(Vec::<CapabilityPermissionV1>::new())
                .expect("empty permission set"),
            Vec::new(),
            NonZeroU16::MIN,
            Vec::new(),
        )
        .expect("grant");
        let requested = CapabilityRequestedRecordV1::new(
            DatabaseId::from_bytes(uuid_bytes(0x22)).expect("database ID"),
            Environment::new("test").expect("environment"),
            ActorId::new("view-principal").expect("principal"),
            ActorKind::Service,
            NonZeroU32::new(100).expect("duration"),
            vec![Audience::new("riffdb-test").expect("audience")],
            grant,
        )
        .expect("requested capability");
        StoredCapabilityRecordV1::active(
            CapabilityId::from_bytes(uuid_bytes(0x11)).expect("capability ID"),
            CapabilityTokenDigest::from_hmac_bytes(
                DigestKeyId::new(7).expect("digest key"),
                [0x44; 32],
            ),
            requested,
            Timestamp::new(100, 0).expect("issued at"),
            Timestamp::new(200, 0).expect("expires at"),
            AdministrationSequence::first(),
            RequestId::from_bytes(uuid_bytes(0x33)).expect("request ID"),
        )
        .expect("active capability")
    }

    fn catalog_bundle(version: u64, fill: u8) -> StoredContractBundleV1 {
        StoredContractBundleV1::new(
            ContractLineage::new("view-test").expect("lineage"),
            ContractVersion::new(version).expect("version"),
            ContractBundleHash::from_bytes([fill; 32]),
            vec![fill],
        )
        .expect("bundle")
    }

    #[test]
    fn current_catalog_view_accepts_only_the_expected_activation_successor() {
        let first = catalog_bundle(1, 0x11);
        let second = catalog_bundle(2, 0x22);
        let mut view = CurrentCatalogViewState::default();
        view.install_rebuilt(ActiveCatalogPointerV1::from_bundle(&first), first.clone())
            .expect("install startup view");

        assert!(
            view.publish_activation(
                Some(ContractVersion::new(2).expect("wrong expected version")),
                ActiveCatalogPointerV1::from_bundle(&second),
                second.clone(),
                true,
            )
            .is_err()
        );
        view.publish_activation(
            Some(first.contract_version()),
            ActiveCatalogPointerV1::from_bundle(&second),
            second.clone(),
            true,
        )
        .expect("publish exact successor");
        assert_eq!(
            view.active_bundle(second.lineage(), second.contract_version()),
            Some(second)
        );
    }

    #[test]
    fn current_capability_view_publishes_revisions_and_complete_digest_lookup_facts() {
        let active = capability_record();
        let digest = active.token_digest();
        let absent =
            CapabilityTokenDigest::from_hmac_bytes(DigestKeyId::new(8).expect("key"), [0x55; 32]);
        let mut view = CurrentCapabilityViewState::default();
        let initial = CapabilityLookupResult::Found(Box::new(active.clone()));

        assert!(view.resolve(&[digest, absent]).is_none());
        view.note_lookup(&[digest, absent], &initial)
            .expect("publish lookup result");
        let CapabilityLookupResult::Found(found) =
            view.resolve(&[digest, absent]).expect("lookup is complete")
        else {
            panic!("expected one cached match");
        };
        assert_eq!(found.revision(), NonZeroU64::MIN);

        let revoked = active
            .revoked(
                NonZeroU64::MIN,
                Timestamp::new(150, 0).expect("revoked at"),
                AdministrationSequence::new(2).expect("sequence"),
                RevocationReasonCodeV1::Requested,
            )
            .expect("revoked record");
        view.publish(revoked).expect("publish revoke");
        let CapabilityLookupResult::Found(found) = view
            .resolve(&[digest, absent])
            .expect("lookup remains complete")
        else {
            panic!("expected one cached match");
        };
        assert_eq!(found.revision(), NonZeroU64::new(2).expect("revision"));
        assert!(matches!(
            found.lifecycle(),
            riffdb_storage_api::CapabilityLifecycleV1::Revoked { .. }
        ));
    }

    #[test]
    fn capability_view_generation_bumps_on_publish_create_update_and_identical_is_noop() {
        let active = capability_record();
        let mut view = CurrentCapabilityViewState::default();
        assert_eq!(view.generation(), 0, "fresh view starts at generation 0");

        // Create / first insert via publish (same path as create + bootstrap install).
        view.publish(active.clone())
            .expect("publish create into empty view");
        assert_eq!(view.generation(), 1, "create publish bumps generation");

        // Identical re-publish is a no-op (no bump).
        view.publish(active.clone())
            .expect("identical publish is idempotent");
        assert_eq!(
            view.generation(),
            1,
            "identical record must not bump generation"
        );

        // Update / revoke successor revises the resident record.
        let revoked = active
            .revoked(
                NonZeroU64::MIN,
                Timestamp::new(150, 0).expect("revoked at"),
                AdministrationSequence::new(2).expect("sequence"),
                RevocationReasonCodeV1::Requested,
            )
            .expect("revoked record");
        view.publish(revoked).expect("publish revoke update");
        assert_eq!(view.generation(), 2, "revoke publish bumps generation");
    }

    #[test]
    fn capability_view_generation_bumps_on_note_lookup_bootstrap_style_fill() {
        // note_lookup → publish is the cache-fill path used when storage falls
        // through into the warm view (same mutation surface as bootstrap install).
        let active = capability_record();
        let mut view = CurrentCapabilityViewState::default();
        let before = view.generation();
        view.note_lookup(
            &[active.token_digest()],
            &CapabilityLookupResult::Found(Box::new(active)),
        )
        .expect("note_lookup publishes Found");
        assert_eq!(
            view.generation(),
            before + 1,
            "bootstrap-style note_lookup publish bumps generation"
        );
    }

    #[test]
    fn cloned_cells_share_one_value() {
        let first = SharedStorageCell::new(0_u8);
        let second = first.clone();

        first
            .with_mut(|value| {
                *value = 7;
                Ok(())
            })
            .expect("mutate shared value");

        assert_eq!(
            second
                .with_mut(|value| Ok(*value))
                .expect("read clone through write path"),
            7
        );
    }

    #[test]
    fn returned_owned_state_outlives_the_bridge_guard() {
        struct OwnedTransactionState {
            bridge: SharedStorageCell<u8>,
        }

        impl OwnedTransactionState {
            fn advance(self) -> Result<u8, StorageError> {
                self.bridge.with_mut(|value| {
                    *value += 1;
                    Ok(*value)
                })
            }
        }

        let bridge = SharedStorageCell::new(0_u8);
        let state = bridge
            .with_mut(|_| {
                Ok(OwnedTransactionState {
                    bridge: bridge.clone(),
                })
            })
            .expect("begin owned state");

        assert!(
            bridge.inner.try_write().is_ok(),
            "the outer guard must be gone when an owned state is returned"
        );
        assert_eq!(state.advance().expect("advance owned state"), 1);
    }

    #[test]
    fn storage_bridge_contains_no_async_suspension_point() {
        let source = include_str!("storage.rs");

        assert!(!source.contains(&["async", " fn"].concat()));
        assert!(!source.contains(&[".", "await"].concat()));
    }

    fn query_module_record(seed: u8) -> StoredQueryModuleV1 {
        StoredQueryModuleV1::new(
            riffdb_types::QueryModuleName::new("ticketdesk").expect("name"),
            riffdb_types::QueryModuleVersion::new(u64::from(seed)).expect("version"),
            QueryModuleHash::from_bytes([seed; 32]),
            ContractLineage::new("view-qm").expect("lineage"),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([seed.wrapping_add(1); 32]),
            vec![seed, 1, 2, 3],
        )
        .expect("stored query module")
    }

    #[test]
    fn current_query_module_view_populates_on_fresh_construction() {
        let module = query_module_record(0x11);
        let pointer = ActiveQueryModulePointerV1::from_module(&module);
        let mut view = CurrentQueryModuleViewState::default();
        view.install_rebuilt(pointer.clone(), module.clone())
            .expect("fresh install");
        assert_eq!(
            view.active(
                module.contract_lineage(),
                module.contract_version(),
                module.contract_bundle_hash(),
            ),
            Some(Some(pointer))
        );
        assert_eq!(view.module(module.module_hash()), Some(module));
    }

    #[test]
    fn current_query_module_view_reactivate_already_active_is_idempotent() {
        let module = query_module_record(0x22);
        let pointer = ActiveQueryModulePointerV1::from_module(&module);
        // Restart-shaped reconstruction: empty view + install_rebuilt, then
        // AlreadyActive publish with changed=false must not integrity-fail.
        let mut view = CurrentQueryModuleViewState::default();
        view.install_rebuilt(pointer.clone(), module.clone())
            .expect("reconstruct from storage");
        view.publish_activation(pointer.clone(), module.clone(), false)
            .expect("idempotent re-activate of already-active module");
        assert_eq!(
            view.active(
                module.contract_lineage(),
                module.contract_version(),
                module.contract_bundle_hash(),
            ),
            Some(Some(pointer))
        );
    }

    #[test]
    fn current_query_module_view_first_read_after_construction_is_warm() {
        let module = query_module_record(0x33);
        let pointer = ActiveQueryModulePointerV1::from_module(&module);
        let mut view = CurrentQueryModuleViewState::default();
        view.install_rebuilt(pointer.clone(), module.clone())
            .expect("warm rebuild");
        // Warm path: active() returns Some(_) meaning the cache hit and no
        // storage fallthrough is required (Some(None) = known absent;
        // None = cold miss). After construction this must be a warm hit.
        let cached = view
            .active(
                module.contract_lineage(),
                module.contract_version(),
                module.contract_bundle_hash(),
            )
            .expect("warm cache hit, not cold miss");
        assert_eq!(cached, Some(pointer));
        assert_eq!(
            view.module(module.module_hash()).expect("body warm"),
            module
        );
        // Unknown contract remains cold (None) until storage is consulted.
        assert!(
            view.active(
                &ContractLineage::new("other").expect("other"),
                ContractVersion::new(9).expect("version"),
                ContractBundleHash::from_bytes([0x99; 32]),
            )
            .is_none()
        );
    }

    #[test]
    fn poison_fails_closed_as_an_invariant_violation() {
        let cell = SharedStorageCell::new(());
        let poisoner = cell.clone();

        let result = thread::spawn(move || {
            let _guard = poisoner.inner.write().expect("initial lock");
            panic!("poison test cell");
        })
        .join();
        assert!(result.is_err());

        let error = cell
            .with_mut(|()| Ok(()))
            .expect_err("poisoned cell must fail closed");
        assert_eq!(error.kind(), StorageErrorKind::InvariantViolation);
    }

    #[test]
    fn production_bridge_implements_required_storage_boundaries() {
        fn assert_boundaries<T>()
        where
            T: AdmissionRepository
                + SnapshotReader
                + ApplicationCommandTransactionPort
                + ExecutionFailureTransitionPort
                + ServiceAuditAppendRepository
                + CatalogAdministrationRepository
                + QueryModuleAdministrationRepository
                + ReactiveModuleAdministrationRepository
                + CapabilityAdministrationTransactionPort
                + CapabilityBootstrapAdministrationRepository
                + CatalogRepository
                + QueryModuleRepository
                + ReactiveModuleRepository
                + EventConsumerRepository
                + CapabilityReader
                + CapabilityInventoryReader
                + AuthoritativePointReader
                + AuthoritativeScanReader
                + OutboxRepository
                + ProjectionApplySnapshotReader
                + ProjectionMutationRepository
                + ProjectionQueryReader
                + ProjectionRecoveryRepository
                + Clone
                + Send
                + Sync,
        {
        }

        assert_boundaries::<SharedRedbOperationalPorts>();
    }

    #[test]
    fn production_query_bridge_assembles_executor_over_lower_storage() {
        let source = include_str!("storage.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production storage source");
        assert!(
            source.contains("fn query_executor(&self) -> StorageQueryExecutor<RedbSharedPorts>")
        );
        assert!(source.contains("StorageQueryExecutor::new(self.shared.clone())"));
        assert!(!source.contains("impl QueryExecutionPort for SharedRedbOperationalPorts"));
    }

    #[test]
    fn production_bridge_preserves_deferred_service_audit_submission() {
        let mut real = RealStorage::open("deferred-service-audit");
        let principal = AuditPrincipalV1::new(
            ActorId::new("audit-principal").expect("principal"),
            ActorKind::Service,
            CapabilityId::from_bytes(uuid_bytes(0x71)).expect("capability ID"),
            NonZeroU64::MIN,
        );
        let intent = ServiceAuditAppendIntentV1::new(
            RequestId::from_bytes(uuid_bytes(0x72)).expect("request ID"),
            Timestamp::new(1_700_000_000, 0).expect("timestamp"),
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Denied,
            principal,
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("audit intent");

        let ServiceAuditGroupAppend::Submitted(fence) = real
            .storage
            .submit_service_audit_group(&[intent])
            .expect("submit through production bridge")
        else {
            panic!("the production bridge must preserve deferred journal submission");
        };
        let results = fence.wait().expect("publish deferred audit group");
        assert!(
            matches!(results.as_slice(), [ServiceAuditAppendResult::Appended(_)]),
            "the deferred append must publish its authoritative result"
        );
    }
}
