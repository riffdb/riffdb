//! Private sharing bridge for the one activated production redb port bundle.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use riffdb_query_executor::{
    QueryContinuation, QueryExecutionError, QueryExecutionPort, QueryOwnedSnapshot, QueryParameters,
};
use riffdb_query_ir::QueryAccessProgramV1;
use riffdb_service::{AuthoritativeReadinessFailure, ServiceHealthHooks};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, ActiveQueryModulePointerV1, AdmissionLookupResultV1,
    AdmissionRepository, AdmissionRequestV1, AdmissionResultV1, ApplicationCommandTransactionPort,
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
    CatalogRepository, CommitScanPageV1, CommitScanRequest, ExecutionFailureAdmissionResult,
    ExecutionFailureTransitionPort, ExecutionFailureTransitionRequestV1,
    FilteredAuthoritativeIndexScanPage, FilteredAuthoritativeIndexScanRequest,
    FilteredAuthoritativeScanReader, IdempotencyIdentity, IdempotencyLookupCandidatesV1,
    OutboxClaimV1, OutboxDeadLetterV1, OutboxPageLimit, OutboxRenewV1, OutboxRepository,
    OutboxRetryV1, OutboxStatusReadResultV1, OutboxSucceedV1, OutboxTransitionResultV1,
    PendingOutboxScanV1, ProjectionApplyRequestV1, ProjectionApplyResult, ProjectionApplySnapshot,
    ProjectionApplySnapshotReader, ProjectionApplySnapshotRequest, ProjectionControlOperation,
    ProjectionControlResult, ProjectionControlScanV1, ProjectionMutationRepository,
    ProjectionQueryReader, ProjectionQueryRequest, ProjectionQueryResult,
    ProjectionRecoveryPageLimit, ProjectionRecoveryRepository,
    ProjectionRecoveryValidationRequestV1, ProjectionRecoveryValidationResultV1, ProjectionStatus,
    QueryModuleActivationIntentV1, QueryModuleActivationResult,
    QueryModuleAdministrationRepository, QueryModuleRepository, ReadSnapshot,
    ServiceAuditAppendIntentV1, ServiceAuditAppendRepository, ServiceAuditAppendResult,
    SnapshotReader, SnapshotRequest, StorageError, StorageErrorKind, StorageScanLimit,
    StoredCapabilityRecordV1, StoredCommitRecordV1, StoredContractBundleV1,
    StoredContractMigrationEdgeV1, StoredDurableEventV1, StoredEntityRecordV1, StoredOutcomeV1,
    StoredProvenanceRecordV1, StoredQueryModuleV1, UndeliveredOutboxStatusScanRequestV1,
    UndeliveredOutboxStatusScanV1,
};
use riffdb_storage_redb::{RedbOperationalPorts, RedbSharedPorts};
use riffdb_types::{
    CapabilityId, CapabilityTokenDigest, CommitSequence, ContractBundleHash, ContractLineage,
    ContractVersion, EventId, ProvenanceId, QueryModuleHash,
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
        let shared = ports.shared_ports();
        let catalog = CurrentCatalogView::rebuild(&shared)?;
        let capabilities = CurrentCapabilityView::rebuild(&shared)?;
        let query_modules = CurrentQueryModuleView::rebuild(&shared)?;
        Ok(Self {
            cell: SharedStorageCell::new(ports),
            shared,
            catalog: Arc::new(catalog),
            capabilities: Arc::new(capabilities),
            query_modules: Arc::new(query_modules),
            health,
        })
    }

    fn current_view_failure(&self, error: StorageError) -> StorageError {
        if let Some(health) = &self.health {
            health.fail_authoritative_readiness(AuthoritativeReadinessFailure::Integrity);
        }
        error
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
            health: self.health.clone(),
        }
    }
}

impl QueryExecutionPort for SharedRedbOperationalPorts {
    fn execute_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        QueryExecutionPort::execute_query_page(&self.shared, program, parameters, prior)
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
        self.remove_known_absent(digest);
        if !self.records.contains_key(&capability_id) {
            self.record_order.push_back(capability_id);
        }
        self.digests.insert(digest, capability_id);
        self.records.insert(capability_id, record);
        Ok(())
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
}

impl ApplicationCommandTransactionPort for SharedRedbOperationalPorts {
    type EmptyBatch = <RedbOperationalPorts as ApplicationCommandTransactionPort>::EmptyBatch;

    fn begin_empty_batch(&self) -> Result<Self::EmptyBatch, StorageError> {
        ApplicationCommandTransactionPort::begin_empty_batch(&self.shared)
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

impl AuthoritativeScanReader for SharedRedbOperationalPorts {
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
        CapabilityPermissionV1, CapabilityPermissionsV1, CapabilityRequestedRecordV1,
        PartitionScopeV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, Audience, CapabilityGrantV1,
        ContractBundleHash, DatabaseId, DigestKeyId, Environment, RequestId,
        RevocationReasonCodeV1, TenantScope, Timestamp,
    };

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
                + CapabilityAdministrationTransactionPort
                + CapabilityBootstrapAdministrationRepository
                + CatalogRepository
                + QueryModuleRepository
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
}
