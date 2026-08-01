//! Production catalog and authoritative-read adapters for the API-neutral service.

use std::collections::BTreeMap;
use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogError, CatalogErrorKind, CatalogPreparationResult,
    QueryModuleCatalogError, ResolvedExecutablePlan, ValidatedContractBundle, ValidatedQueryModule,
    prepare_catalog_activation, resolve_executable_plan,
};
use riffdb_contract_ir::ContractBundle;
use riffdb_idempotency::{
    CommandIdempotencyScopeV1, IdempotencyDigestProvider, IdempotencyPreparationError,
    prepare_idempotency_lookup,
};
use riffdb_policy::{CapabilityActivity, PartitionConstraint, ProvenanceSelector};
use riffdb_service::{
    AbsentCapabilityRevokeTargetSnapshot, AffectedEntityView, AuthoritativeCommitPage,
    AuthoritativeCommitScanRequest, AuthoritativeCommitSnapshot,
    AuthoritativeCommitSubscriptionRequest, AuthoritativeEntityRequest,
    AuthoritativeEntitySnapshot, AuthoritativeIndexPage, AuthoritativeIndexRequest,
    AuthoritativeIndexRow, AuthoritativeJournaledOutcome, AuthoritativeOutcomeFacts,
    AuthoritativeOutcomeRequest, AuthoritativeOutcomeSelectorRef, AuthoritativeOutcomeSnapshot,
    AuthoritativeProvenanceSnapshot, AuthoritativeReadError, AuthoritativeReadPort,
    AuthoritativeSchemaBinding, BoxPortCapacityPermit, CapabilityRevokeTargetSnapshot,
    CatalogExecutablePlanRequest, CatalogReadPort, CommandDurability, CommitNotificationSource,
    ContractVersionReadPermit, DeclaredOutcomeView, DurableEventView, OutcomeLocatorDigestEvidence,
    PortAdmissionError, PortDriverStopped, PortFuture, PresentCapabilityRevokeTargetSnapshot,
    ProvenanceClaimsView, QueryModuleReadError, QueryModuleReadPort, RequestControl,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AdmissionLookupResultV1, AdmissionRepository, AuthoritativePointReader,
    AuthoritativeScanReader, CapabilityLifecycleV1, CapabilityReader, CatalogRepository,
    CommitScanPageV1, CommitScanRequest, DurabilityMode, EntityTarget, ExecutablePlanRef,
    FilteredAuthoritativeIndexScanPage, FilteredAuthoritativeIndexScanRequest,
    FilteredAuthoritativeScanReader, IdempotencyIdentity, IdempotencyKeyDigest,
    IdempotencyLookupCandidatesV1, IndexPartitionFilter, IndexPartitionFilterScope,
    IndexRangePrefixBuilder, IndexRangeTarget, QueryModuleRepository, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, StorageError, StorageErrorKind, StorageScanLimit,
    StoredAdmissionStateV1, StoredCommitRecordV1, StoredPendingAdmissionV1,
    StoredProvenanceRecordV1,
};
use riffdb_types::{
    CanonicalValue, CapabilityId, CommitSequence, ContractLineage, ContractVersion, DatabaseId,
    Environment, QueryModuleHash,
};

use crate::notifications::FirstCommitNotificationHub;
use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};
use crate::storage::SharedRedbOperationalPorts;

type ContractVersionRequest = (ContractLineage, ContractVersion);
type DeploymentRequest = (ContractBundle, Option<ContractVersion>);
type QueryModuleReadRequest = (ValidatedContractBundle, Option<QueryModuleHash>);
const MAX_HOT_HISTORICAL_CONTRACTS: usize = 4_096;
const MAX_HOT_QUERY_MODULES: usize = 4_096;
const MAX_HOT_EXECUTABLE_PLANS: usize = 4_096;

/// Counts blocking-pool dispatches for query-module lookup.
///
/// Test-only observation of whether the inline path served a request. Compiled
/// out of normal builds so no shipped configuration carries the counter.
#[cfg(test)]
static QUERY_MODULE_POOL_DISPATCHES: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
fn note_query_module_pool_dispatch() {
    QUERY_MODULE_POOL_DISPATCHES.fetch_add(1, Ordering::Relaxed);
}

#[cfg(not(test))]
const fn note_query_module_pool_dispatch() {}

/// Returns the number of query-module blocking-pool dispatches observed.
#[cfg(test)]
fn query_module_pool_dispatch_count() -> u64 {
    QUERY_MODULE_POOL_DISPATCHES.load(Ordering::Relaxed)
}

#[derive(Default)]
struct ActiveCatalogView {
    /// Published snapshot is immutable; publication replaces the Arc.
    /// Cache hits hand out `Arc::clone` instead of cloning catalog contents.
    snapshot: Option<Arc<ActiveCatalogSnapshot>>,
}

impl ActiveCatalogView {
    fn get(
        &self,
        pointer: Option<&ActiveCatalogPointerV1>,
    ) -> Option<Option<Arc<ActiveCatalogSnapshot>>> {
        match (pointer, self.snapshot.as_ref()) {
            (None, None) => Some(None),
            (Some(pointer), Some(snapshot)) if snapshot.pointer() == pointer => {
                Some(Some(Arc::clone(snapshot)))
            }
            _ => None,
        }
    }

    fn replace(&mut self, snapshot: Option<Arc<ActiveCatalogSnapshot>>) {
        self.snapshot = snapshot;
    }
}

fn read_active_catalog_cached<R: CatalogRepository>(
    repository: &R,
    cache: &Mutex<ActiveCatalogView>,
) -> Result<Option<ActiveCatalogSnapshot>, CatalogError> {
    let durable_pointer = repository.read_active_catalog()?;
    if let Some(snapshot) = cache
        .lock()
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?
        .get(durable_pointer.as_ref())
    {
        // Arc clone only — consumers receive a shared published snapshot.
        return Ok(snapshot.map(|shared| ActiveCatalogSnapshot::clone(shared.as_ref())));
    }

    // A cache miss always takes the complete catalog validation path. The
    // snapshot read observes the active pointer again, so a concurrent
    // activation yields either the prior or successor complete snapshot,
    // never a pointer/bundle mixture.
    let snapshot = ActiveCatalogSnapshot::read(repository)?;
    let published = snapshot.as_ref().map(|value| Arc::new(value.clone()));
    cache
        .lock()
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?
        .replace(published);
    Ok(snapshot)
}

#[derive(Default)]
struct ExecutablePlanView {
    plans: BTreeMap<ExecutablePlanRef, (ActiveCatalogPointerV1, ResolvedExecutablePlan)>,
}

impl ExecutablePlanView {
    fn get(
        &self,
        reference: &ExecutablePlanRef,
        active_pointer: &ActiveCatalogPointerV1,
    ) -> Option<ResolvedExecutablePlan> {
        self.plans
            .get(reference)
            .and_then(|(validated_under, plan)| {
                (validated_under == active_pointer).then(|| plan.clone())
            })
    }

    fn insert(
        &mut self,
        reference: ExecutablePlanRef,
        active_pointer: ActiveCatalogPointerV1,
        plan: ResolvedExecutablePlan,
    ) {
        if self.plans.len() == MAX_HOT_EXECUTABLE_PLANS
            && !self.plans.contains_key(&reference)
            && let Some(oldest_reference) = self.plans.keys().next().cloned()
        {
            self.plans.remove(&oldest_reference);
        }
        self.plans.insert(reference, (active_pointer, plan));
    }
}

#[derive(Default)]
struct HistoricalContractView {
    bundles: BTreeMap<ContractVersionRequest, ValidatedContractBundle>,
}

impl HistoricalContractView {
    fn get(
        &self,
        lineage: &ContractLineage,
        version: ContractVersion,
    ) -> Option<ValidatedContractBundle> {
        self.bundles.get(&(lineage.clone(), version)).cloned()
    }

    fn insert(&mut self, bundle: ValidatedContractBundle) -> Result<(), CatalogError> {
        let key = (bundle.lineage().clone(), bundle.contract_version());
        if let Some(existing) = self.bundles.get(&key) {
            if existing.bundle_hash() != bundle.bundle_hash() {
                return Err(CatalogError::new(CatalogErrorKind::BundleIdentityConflict));
            }
            return Ok(());
        }
        if self.bundles.len() == MAX_HOT_HISTORICAL_CONTRACTS {
            return Err(CatalogError::new(CatalogErrorKind::LineageBundleCountLimit));
        }
        self.bundles.insert(key, bundle);
        Ok(())
    }
}

#[derive(Default)]
struct QueryModulePlanCache {
    modules: BTreeMap<QueryModuleHash, ValidatedQueryModule>,
}

impl QueryModulePlanCache {
    fn get(
        &self,
        module_hash: QueryModuleHash,
        contract: &ValidatedContractBundle,
    ) -> Option<ValidatedQueryModule> {
        self.modules.get(&module_hash).and_then(|module| {
            let compiled = module.module();
            (compiled.contract_lineage() == contract.lineage()
                && compiled.contract_version() == contract.contract_version()
                && compiled.contract_hash() == contract.bundle_hash())
            .then(|| module.clone())
        })
    }

    fn insert(&mut self, module: ValidatedQueryModule) {
        if self.modules.len() == MAX_HOT_QUERY_MODULES
            && !self.modules.contains_key(&module.identity())
            && let Some(oldest_identity) = self.modules.keys().next().copied()
        {
            self.modules.remove(&oldest_identity);
        }
        self.modules.insert(module.identity(), module);
    }
}

/// Catalog-owned semantic reads driven on the retained blocking worker set.
pub(crate) struct ServerCatalogReadPort {
    active_storage: SharedRedbOperationalPorts,
    active_cache: Arc<Mutex<ActiveCatalogView>>,
    active: BlockingPortExecutor<(), Option<ActiveCatalogSnapshot>, CatalogError>,
    contract_version:
        BlockingPortExecutor<ContractVersionRequest, Option<ValidatedContractBundle>, CatalogError>,
    executable_plan:
        BlockingPortExecutor<CatalogExecutablePlanRequest, ResolvedExecutablePlan, CatalogError>,
    deployment: BlockingPortExecutor<DeploymentRequest, CatalogPreparationResult, CatalogError>,
    query_module: BlockingPortExecutor<
        QueryModuleReadRequest,
        Option<ValidatedQueryModule>,
        QueryModuleReadError,
    >,
    /// Shared with the blocking executor for non-blocking cache-hit inline lookup.
    module_storage: SharedRedbOperationalPorts,
    module_cache: Arc<Mutex<QueryModulePlanCache>>,
}

impl ServerCatalogReadPort {
    /// Builds every catalog operation from the same activated repository bridge.
    #[allow(
        dead_code,
        reason = "WP-130 composition constructs this adapter after staged storage activation"
    )]
    pub(crate) fn new(storage: SharedRedbOperationalPorts, driver: &BlockingPortDriver) -> Self {
        let active_cache = Arc::new(Mutex::new(ActiveCatalogView::default()));
        let active_storage = storage.clone();
        let reserved_active_storage = storage.clone();
        let reserved_active_cache = Arc::clone(&active_cache);
        let active = driver.executor(move |()| {
            read_active_catalog_cached(&reserved_active_storage, &reserved_active_cache)
        });

        let historical = Arc::new(Mutex::new(HistoricalContractView::default()));
        let version_storage = storage.clone();
        let version_historical = Arc::clone(&historical);
        let contract_version = driver.executor(move |(lineage, version)| {
            if let Some(bundle) = version_historical
                .lock()
                .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?
                .get(&lineage, version)
            {
                return Ok(Some(bundle));
            }
            let bundle = read_contract_version(&version_storage, lineage, version)?;
            if let Some(bundle) = bundle.as_ref() {
                version_historical
                    .lock()
                    .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?
                    .insert(bundle.clone())?;
            }
            Ok(bundle)
        });

        let plan_storage = storage.clone();
        let plan_cache = Arc::new(Mutex::new(ExecutablePlanView::default()));
        let executable_plan = driver.executor(move |request: CatalogExecutablePlanRequest| {
            let reference = ExecutablePlanRef::new(
                request.lineage().clone(),
                request.version(),
                request.bundle_hash(),
                request.command_id(),
                request.plan_hash(),
            );
            let before = plan_storage
                .read_active_catalog()?
                .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
            if let Some(plan) = plan_cache
                .lock()
                .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?
                .get(&reference, &before)
            {
                return Ok(plan);
            }

            let plan = resolve_executable_plan(&plan_storage, &reference)?;
            // Cache only across an unchanged exact active pointer. If deployment
            // raced resolution, this invocation may use the coherent snapshot it
            // observed, while the next invocation must resolve against the new
            // active lineage proof.
            if plan_storage.read_active_catalog()?.as_ref() == Some(&before) {
                plan_cache
                    .lock()
                    .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?
                    .insert(reference, before, plan.clone());
            }
            Ok(plan)
        });

        let module_storage = storage.clone();
        let module_cache = Arc::new(Mutex::new(QueryModulePlanCache::default()));
        let pool_module_storage = module_storage.clone();
        let pool_module_cache = Arc::clone(&module_cache);
        let query_module = driver.executor(move |(contract, selected): QueryModuleReadRequest| {
            resolve_query_module_on_pool(
                &pool_module_storage,
                &pool_module_cache,
                contract,
                selected,
            )
        });

        let deployment = driver.executor(
            move |(candidate, expected_active_version): DeploymentRequest| {
                let active = ActiveCatalogSnapshot::read(&storage)?;
                prepare_catalog_activation(candidate, expected_active_version, active.as_ref())
            },
        );

        Self {
            active_storage,
            active_cache,
            active,
            contract_version,
            executable_plan,
            deployment,
            query_module,
            module_storage,
            module_cache,
        }
    }
}

impl CatalogReadPort for ServerCatalogReadPort {
    fn prepare_active_catalog(
        &self,
        _control: &RequestControl,
    ) -> PortFuture<'_, Option<ActiveCatalogSnapshot>, CatalogError> {
        let result = read_active_catalog_cached(&self.active_storage, &self.active_cache);
        Box::pin(async move { result })
    }

    fn prepare_contract_version<'a>(
        &'a self,
        control: &'a RequestControl,
        lineage: ContractLineage,
        version: ContractVersion,
    ) -> PortFuture<'a, Option<ValidatedContractBundle>, CatalogError> {
        submit_catalog(
            self.contract_version.reserve_async(control),
            (lineage, version),
        )
    }

    fn reserve_active_catalog<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<(), Option<ActiveCatalogSnapshot>, CatalogError>,
        PortAdmissionError,
    > {
        ready_port_reservation(self.active.reserve_async(control))
    }

    fn reserve_contract_version<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<'a, ContractVersionReadPermit, PortAdmissionError> {
        ready_port_reservation(self.contract_version.reserve_async(control))
    }

    fn executable_plan<'a>(
        &'a self,
        control: &'a RequestControl,
        request: CatalogExecutablePlanRequest,
    ) -> PortFuture<'a, ResolvedExecutablePlan, CatalogError> {
        submit_catalog(self.executable_plan.reserve_async(control), request)
    }

    fn prepare_deployment<'a>(
        &'a self,
        control: &'a RequestControl,
        candidate: ContractBundle,
        expected_active_version: Option<ContractVersion>,
    ) -> PortFuture<'a, CatalogPreparationResult, CatalogError> {
        submit_catalog(
            self.deployment.reserve_async(control),
            (candidate, expected_active_version),
        )
    }
}

impl QueryModuleReadPort for ServerCatalogReadPort {
    fn prepare_active_query_module<'a>(
        &'a self,
        control: &'a RequestControl,
        contract: ValidatedContractBundle,
    ) -> PortFuture<'a, Option<ValidatedQueryModule>, QueryModuleReadError> {
        // Inline fast path. Admission control runs first (a draining, stopping,
        // cancelled, or deadline-exceeded request must not be served from
        // cache), then process-local state only. Anything else — including
        // every error — goes to the blocking pool unchanged.
        if self.query_module.precheck(control).is_ok()
            && let Some(module) =
                try_cached_query_module(&self.module_storage, &self.module_cache, &contract, None)
        {
            return Box::pin(async move { Ok(module) });
        }
        note_query_module_pool_dispatch();
        submit_query_module(self.query_module.reserve_async(control), (contract, None))
    }

    fn prepare_query_module<'a>(
        &'a self,
        control: &'a RequestControl,
        contract: ValidatedContractBundle,
        module_hash: QueryModuleHash,
    ) -> PortFuture<'a, Option<ValidatedQueryModule>, QueryModuleReadError> {
        if self.query_module.precheck(control).is_ok()
            && let Some(module) = try_cached_query_module(
                &self.module_storage,
                &self.module_cache,
                &contract,
                Some(module_hash),
            )
        {
            return Box::pin(async move { Ok(module) });
        }
        note_query_module_pool_dispatch();
        submit_query_module(
            self.query_module.reserve_async(control),
            (contract, Some(module_hash)),
        )
    }
}

/// Answers a query-module lookup from process-local state, or declines.
///
/// This is the inline fast path and it is deliberately incapable of failing.
/// It performs no storage I/O, acquires no write lock, and never blocks: it
/// reads the already-resident active-module pointer with `try_read` and the
/// compiled-plan cache with `try_lock`.
///
/// `Some(answer)` is a complete successful result — a cached compiled module,
/// or `None` for an identity the view already knows has no active module.
/// `None` means "not answerable here": the identity is not resident, the plan
/// cache misses, a lock is contended or poisoned, or any other would-block
/// condition. The caller then dispatches to the blocking pool, which owns
/// every cold read, every compile, and therefore the entire error taxonomy.
fn try_cached_query_module(
    storage: &SharedRedbOperationalPorts,
    cache: &Mutex<QueryModulePlanCache>,
    contract: &ValidatedContractBundle,
    selected: Option<QueryModuleHash>,
) -> Option<Option<ValidatedQueryModule>> {
    let module_hash = match selected {
        Some(module_hash) => module_hash,
        None => match storage.cached_active_query_module(
            contract.lineage(),
            contract.contract_version(),
            contract.bundle_hash(),
        )? {
            Some(pointer) => pointer.module_hash(),
            None => return Some(None),
        },
    };
    let module = cache.try_lock().ok()?.get(module_hash, contract)?;
    Some(Some(module))
}

/// Full query-module resolution for the blocking pool (unchanged semantics).
fn resolve_query_module_on_pool(
    storage: &SharedRedbOperationalPorts,
    cache: &Mutex<QueryModulePlanCache>,
    contract: ValidatedContractBundle,
    selected: Option<QueryModuleHash>,
) -> Result<Option<ValidatedQueryModule>, QueryModuleReadError> {
    let selected = match selected {
        Some(module_hash) => Some(module_hash),
        None => QueryModuleRepository::read_active_query_module(
            storage,
            contract.lineage(),
            contract.contract_version(),
            contract.bundle_hash(),
        )
        .map_err(map_query_module_storage)?
        .map(|pointer| pointer.module_hash()),
    };
    let Some(module_hash) = selected else {
        return Ok(None);
    };
    if let Some(module) = cache
        .lock()
        .map_err(|_| QueryModuleReadError::Integrity)?
        .get(module_hash, &contract)
    {
        return Ok(Some(module));
    }
    let module = QueryModuleRepository::read_query_module(storage, module_hash)
        .map_err(map_query_module_storage)?
        .map(|stored| {
            ValidatedQueryModule::from_stored(&stored, &contract).map_err(map_query_module_catalog)
        })
        .transpose()?;
    if let Some(module) = module.as_ref() {
        cache
            .lock()
            .map_err(|_| QueryModuleReadError::Integrity)?
            .insert(module.clone());
    }
    Ok(module)
}

impl fmt::Debug for ServerCatalogReadPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerCatalogReadPort([REDACTED])")
    }
}

/// Authoritative service reads driven through narrow storage and catalog operations.
pub(crate) struct ServerAuthoritativeReadPort {
    entity: BlockingPortExecutor<
        AuthoritativeEntityRequest,
        Option<AuthoritativeEntitySnapshot>,
        AuthoritativeReadError,
    >,
    index: BlockingPortExecutor<
        AuthoritativeIndexRequest,
        AuthoritativeIndexPage,
        AuthoritativeReadError,
    >,
    outcome: BlockingPortExecutor<
        AuthoritativeOutcomeRequest,
        Option<AuthoritativeOutcomeSnapshot>,
        AuthoritativeReadError,
    >,
    commit: BlockingPortExecutor<
        CommitSequence,
        Option<AuthoritativeCommitSnapshot>,
        AuthoritativeReadError,
    >,
    commit_scan: BlockingPortExecutor<
        AuthoritativeCommitScanRequest,
        AuthoritativeCommitPage,
        AuthoritativeReadError,
    >,
    subscription: BlockingPortExecutor<
        AuthoritativeCommitSubscriptionRequest,
        Box<dyn CommitNotificationSource>,
        AuthoritativeReadError,
    >,
    provenance: BlockingPortExecutor<
        ProvenanceSelector,
        Option<AuthoritativeProvenanceSnapshot>,
        AuthoritativeReadError,
    >,
    revoke_target:
        BlockingPortExecutor<CapabilityId, CapabilityRevokeTargetSnapshot, AuthoritativeReadError>,
}

impl ServerAuthoritativeReadPort {
    /// Joins the activated storage bridge, digest custody, and notification source once.
    #[allow(
        dead_code,
        reason = "WP-130 composition constructs this adapter after staged storage activation"
    )]
    pub(crate) fn new(
        storage: SharedRedbOperationalPorts,
        digest_provider: Arc<dyn IdempotencyDigestProvider>,
        readable_digests: ReadableIdempotencyDigestInventory,
        database_id: DatabaseId,
        environment: Environment,
        notifications: FirstCommitNotificationHub,
        driver: &BlockingPortDriver,
    ) -> Self {
        let entity_storage = storage.clone();
        let entity = driver.executor(move |request| read_entity(&entity_storage, request));

        let index_storage = storage.clone();
        let index = driver.executor(move |request| scan_index(&index_storage, request));

        let outcome_storage = storage.clone();
        let outcome_database_id = database_id;
        let outcome_environment = environment.clone();
        let outcome = driver.executor(move |request| {
            read_outcome(
                &outcome_storage,
                digest_provider.as_ref(),
                &readable_digests,
                outcome_database_id,
                &outcome_environment,
                request,
            )
        });

        let commit_storage = storage.clone();
        let commit = driver.executor(move |sequence| read_commit(&commit_storage, sequence));

        let scan_storage = storage.clone();
        let commit_scan = driver.executor(move |request| scan_commits(&scan_storage, request));

        let subscription =
            driver.executor(move |request: AuthoritativeCommitSubscriptionRequest| {
                notifications
                    .subscribe(request.after())
                    .map_err(|_| AuthoritativeReadError::Unavailable)
            });

        let provenance_storage = storage.clone();
        let provenance =
            driver.executor(move |selector| read_provenance(&provenance_storage, selector));

        let revoke_target = driver.executor(move |capability_id| {
            read_revoke_target(&storage, database_id, &environment, capability_id)
        });

        Self {
            entity,
            index,
            outcome,
            commit,
            commit_scan,
            subscription,
            provenance,
            revoke_target,
        }
    }
}

impl AuthoritativeReadPort for ServerAuthoritativeReadPort {
    fn reserve_read_entity<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeEntityRequest,
            Option<AuthoritativeEntitySnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.entity.reserve_async(control))
    }

    fn reserve_scan_index<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeIndexRequest,
            AuthoritativeIndexPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.index.reserve_async(control))
    }

    fn reserve_read_outcome<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeOutcomeRequest,
            Option<AuthoritativeOutcomeSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.outcome.reserve_async(control))
    }

    fn reserve_read_commit<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            CommitSequence,
            Option<AuthoritativeCommitSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.commit.reserve_async(control))
    }

    fn reserve_scan_commits<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeCommitScanRequest,
            AuthoritativeCommitPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.commit_scan.reserve_async(control))
    }

    fn reserve_subscribe_to_commits<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            AuthoritativeCommitSubscriptionRequest,
            Box<dyn CommitNotificationSource>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.subscription.reserve_async(control))
    }

    fn reserve_trace_provenance<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            ProvenanceSelector,
            Option<AuthoritativeProvenanceSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.provenance.reserve_async(control))
    }

    fn read_capability_revoke_target<'a>(
        &'a self,
        control: &'a RequestControl,
        capability_id: CapabilityId,
    ) -> PortFuture<'a, CapabilityRevokeTargetSnapshot, AuthoritativeReadError> {
        submit_authoritative(self.revoke_target.reserve_async(control), capability_id)
    }
}

impl fmt::Debug for ServerAuthoritativeReadPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerAuthoritativeReadPort([REDACTED])")
    }
}

fn ready_port_reservation<'a, Permit, F>(
    reservation: F,
) -> PortFuture<'a, Permit, PortAdmissionError>
where
    Permit: Send + 'a,
    F: std::future::Future<Output = Result<Permit, PortAdmissionError>> + Send + 'a,
{
    Box::pin(reservation)
}

fn submit_catalog<'a, Request, Response, F>(
    reservation: F,
    request: Request,
) -> PortFuture<'a, Response, CatalogError>
where
    Request: Send + 'a,
    Response: Send + 'a,
    F: std::future::Future<
            Output = Result<
                BoxPortCapacityPermit<Request, Response, CatalogError>,
                PortAdmissionError,
            >,
        > + Send
        + 'a,
{
    Box::pin(async move {
        let permit = reservation
            .await
            .map_err(|_| catalog_driver_unavailable())?;
        let receipt = permit
            .submit(request)
            .map_err(|_| catalog_driver_unavailable())?;
        match receipt.completion().await {
            Ok(result) => result,
            Err(PortDriverStopped) => Err(catalog_driver_unavailable()),
        }
    })
}

fn submit_query_module<'a, Request, Response, F>(
    reservation: F,
    request: Request,
) -> PortFuture<'a, Response, QueryModuleReadError>
where
    Request: Send + 'a,
    Response: Send + 'a,
    F: std::future::Future<
            Output = Result<
                BoxPortCapacityPermit<Request, Response, QueryModuleReadError>,
                PortAdmissionError,
            >,
        > + Send
        + 'a,
{
    Box::pin(async move {
        let permit = reservation
            .await
            .map_err(|_| QueryModuleReadError::Unavailable)?;
        let receipt = permit
            .submit(request)
            .map_err(|_| QueryModuleReadError::Unavailable)?;
        match receipt.completion().await {
            Ok(result) => result,
            Err(PortDriverStopped) => Err(QueryModuleReadError::Unavailable),
        }
    })
}

fn map_query_module_storage(_: StorageError) -> QueryModuleReadError {
    QueryModuleReadError::Unavailable
}

fn map_query_module_catalog(_: QueryModuleCatalogError) -> QueryModuleReadError {
    QueryModuleReadError::Integrity
}

fn submit_authoritative<'a, Request, Response, F>(
    reservation: F,
    request: Request,
) -> PortFuture<'a, Response, AuthoritativeReadError>
where
    Request: Send + 'a,
    Response: Send + 'a,
    F: std::future::Future<
            Output = Result<
                BoxPortCapacityPermit<Request, Response, AuthoritativeReadError>,
                PortAdmissionError,
            >,
        > + Send
        + 'a,
{
    Box::pin(async move {
        let permit = reservation.await.map_err(map_authoritative_admission)?;
        let receipt = permit
            .submit(request)
            .map_err(map_authoritative_admission)?;
        match receipt.completion().await {
            Ok(result) => result,
            Err(PortDriverStopped) => Err(AuthoritativeReadError::Integrity),
        }
    })
}

fn catalog_driver_unavailable() -> CatalogError {
    CatalogError::new(CatalogErrorKind::Storage)
}

fn map_authoritative_admission(error: PortAdmissionError) -> AuthoritativeReadError {
    match error {
        PortAdmissionError::Cancelled => AuthoritativeReadError::Cancelled,
        PortAdmissionError::DeadlineExceeded => AuthoritativeReadError::DeadlineExceeded,
        PortAdmissionError::Unavailable => AuthoritativeReadError::Unavailable,
        PortAdmissionError::Stopped => AuthoritativeReadError::Integrity,
    }
}

fn map_storage_error(error: StorageError) -> AuthoritativeReadError {
    map_storage_kind(error.kind())
}

fn map_storage_kind(kind: StorageErrorKind) -> AuthoritativeReadError {
    match kind {
        StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown => {
            AuthoritativeReadError::Unavailable
        }
        StorageErrorKind::CorruptData
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::LimitExceeded
        | StorageErrorKind::InvariantViolation
        | StorageErrorKind::SequenceExhausted => AuthoritativeReadError::Integrity,
    }
}

fn map_catalog_error(error: CatalogError) -> AuthoritativeReadError {
    match (error.kind(), error.storage_kind()) {
        (CatalogErrorKind::Storage, Some(kind)) => map_storage_kind(kind),
        (CatalogErrorKind::Storage, None) => AuthoritativeReadError::Unavailable,
        _ => AuthoritativeReadError::Integrity,
    }
}

fn read_contract_version(
    storage: &impl CatalogRepository,
    lineage: ContractLineage,
    version: ContractVersion,
) -> Result<Option<ValidatedContractBundle>, CatalogError> {
    let Some(stored) = storage.read_contract_bundle(&lineage, version)? else {
        return Ok(None);
    };
    let bundle = ValidatedContractBundle::from_stored(&stored)?;
    if bundle.lineage() != &lineage || bundle.contract_version() != version {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }
    Ok(Some(bundle))
}

fn read_entity(
    storage: &impl AuthoritativePointReader,
    request: AuthoritativeEntityRequest,
) -> Result<Option<AuthoritativeEntitySnapshot>, AuthoritativeReadError> {
    let target = EntityTarget::new(request.key().entity_type_id(), request.key().clone())
        .map_err(|_| AuthoritativeReadError::Integrity)?;
    let Some(record) = storage.read_entity(&target).map_err(map_storage_error)? else {
        return Ok(None);
    };
    if record.target() != &target || record.schema_binding().lineage() != request.lineage() {
        return Err(AuthoritativeReadError::Integrity);
    }
    Ok(Some(AuthoritativeEntitySnapshot::new(
        record.target().key().clone(),
        record.entity_version(),
        record.written_by_contract(),
        record.fields().clone(),
    )))
}

fn scan_index(
    storage: &impl FilteredAuthoritativeScanReader,
    request: AuthoritativeIndexRequest,
) -> Result<AuthoritativeIndexPage, AuthoritativeReadError> {
    let partition_filter =
        lower_index_partition_filter(request.lineage(), request.partition_constraint())?;
    scan_index_filtered(storage, request, partition_filter)
}

fn lower_index_partition_filter(
    target_lineage: &ContractLineage,
    constraint: &PartitionConstraint,
) -> Result<IndexPartitionFilter, AuthoritativeReadError> {
    let scope = match constraint {
        PartitionConstraint::Filter(riffdb_types::PartitionScopeV1::All) => {
            IndexPartitionFilterScope::All
        }
        PartitionConstraint::Filter(riffdb_types::PartitionScopeV1::Explicit(entries)) => {
            if entries.is_empty() {
                IndexPartitionFilterScope::None
            } else {
                let mut keys = Vec::with_capacity(entries.len());
                for entry in entries {
                    if entry.lineage() != target_lineage {
                        return Err(AuthoritativeReadError::Integrity);
                    }
                    keys.push(entry.partition_key().clone());
                }
                IndexPartitionFilterScope::Explicit(keys)
            }
        }
        PartitionConstraint::Exact(_) => return Err(AuthoritativeReadError::Integrity),
    };
    IndexPartitionFilter::new(target_lineage.clone(), scope)
        .map_err(|_| AuthoritativeReadError::Integrity)
}

fn scan_index_filtered(
    storage: &impl FilteredAuthoritativeScanReader,
    request: AuthoritativeIndexRequest,
    partition_filter: IndexPartitionFilter,
) -> Result<AuthoritativeIndexPage, AuthoritativeReadError> {
    let mut prefix = IndexRangePrefixBuilder::new(request.index_id());
    for component in request.leading_components() {
        push_index_component(&mut prefix, component)?;
    }
    let prefix = prefix.finish();
    if prefix.index_id() != request.prefix().index_id()
        || prefix.as_bytes() != request.prefix().as_bytes()
    {
        return Err(AuthoritativeReadError::Integrity);
    }
    let partition = match partition_filter.scope() {
        IndexPartitionFilterScope::Explicit(keys) if keys.len() == 1 => keys[0].clone(),
        IndexPartitionFilterScope::All
        | IndexPartitionFilterScope::None
        | IndexPartitionFilterScope::Explicit(_) => {
            return Err(AuthoritativeReadError::Integrity);
        }
    };
    let target = IndexRangeTarget::new(partition, prefix);
    let limit = StorageScanLimit::new(request.limit().get().get())
        .ok_or(AuthoritativeReadError::Integrity)?;
    let lower_request = FilteredAuthoritativeIndexScanRequest::new(
        target,
        partition_filter,
        request.after().cloned(),
        limit,
    )
    .map_err(|_| {
        if request.after().is_some() {
            AuthoritativeReadError::InvalidContinuation
        } else {
            AuthoritativeReadError::Integrity
        }
    })?;
    let lower = storage
        .scan_index_filtered(lower_request)
        .map_err(map_storage_error)?;
    let (entries, scanned_through, epoch) = match lower {
        FilteredAuthoritativeIndexScanPage::Page {
            entries,
            scanned_through,
            epoch,
        } => (entries, Some(scanned_through), epoch),
        FilteredAuthoritativeIndexScanPage::ExactEnd { entries, epoch } => (entries, None, epoch),
    };
    let rows = entries
        .into_iter()
        .map(|entry| {
            let (entry, _) = entry.into_parts();
            let binding = entry.schema_binding();
            AuthoritativeIndexRow::new(
                entry.key().clone(),
                AuthoritativeSchemaBinding::new(
                    binding.lineage().clone(),
                    binding.contract_version(),
                    binding.bundle_hash(),
                ),
                entry.covered_values().clone(),
                entry.partition_key().clone(),
            )
        })
        .collect();
    AuthoritativeIndexPage::new(&request, rows, scanned_through, epoch)
        .map_err(|_| AuthoritativeReadError::Integrity)
}

fn push_index_component(
    builder: &mut IndexRangePrefixBuilder,
    value: &CanonicalValue,
) -> Result<(), AuthoritativeReadError> {
    let result = match value {
        CanonicalValue::Bool(value) => builder.push_bool(*value),
        CanonicalValue::I64(value) => builder.push_i64(*value),
        CanonicalValue::U64(value) => builder.push_u64(*value),
        CanonicalValue::String(value) => builder.push_str(value.as_str()),
        CanonicalValue::Bytes(value) => builder.push_bytes(value.as_bytes()),
        CanonicalValue::Timestamp(value) => builder.push_timestamp(*value),
        CanonicalValue::Date(value) => builder.push_date(*value),
        CanonicalValue::Uuid(value) => builder.push_uuid(value),
        CanonicalValue::Enum { variant_id, .. } => builder.push_enum_variant(*variant_id),
        CanonicalValue::Null
        | CanonicalValue::Decimal(_)
        | CanonicalValue::Money(_)
        | CanonicalValue::List(_)
        | CanonicalValue::Record(_) => return Err(AuthoritativeReadError::Integrity),
    };
    result
        .map(|_| ())
        .map_err(|_| AuthoritativeReadError::Integrity)
}

fn read_outcome(
    storage: &impl AdmissionRepository,
    digest_provider: &dyn IdempotencyDigestProvider,
    readable_digests: &ReadableIdempotencyDigestInventory,
    database_id: DatabaseId,
    environment: &Environment,
    request: AuthoritativeOutcomeRequest,
) -> Result<Option<AuthoritativeOutcomeSnapshot>, AuthoritativeReadError> {
    let candidates = match request.selector() {
        AuthoritativeOutcomeSelectorRef::RawKey(lookup) => {
            let scope = CommandIdempotencyScopeV1::new(
                database_id,
                environment.clone(),
                lookup.tenant_scope().clone(),
                lookup.principal_id().clone(),
                lookup.lineage().clone(),
                lookup.command_id(),
            );
            prepare_idempotency_lookup(&scope, lookup.idempotency_key(), digest_provider)
                .map_err(map_idempotency_preparation)?
                .lookup_candidates()
                .clone()
        }
        AuthoritativeOutcomeSelectorRef::Digested(lookup) => {
            let evidence = lookup.digest_evidence();
            let readable =
                ReadableDigestKey::new(evidence.digest_scheme(), evidence.digest_key_id())
                    .map_err(|_| AuthoritativeReadError::Integrity)?;
            if !readable_digests.as_slice().contains(&readable) {
                return Ok(None);
            }
            let digest =
                IdempotencyKeyDigest::from_hmac_bytes(evidence.digest_key_id(), *evidence.digest());
            let identity = IdempotencyIdentity::new(
                database_id,
                environment.clone(),
                lookup.tenant_scope().clone(),
                lookup.principal_id().clone(),
                lookup.lineage().clone(),
                lookup.command_id(),
                digest,
            );
            IdempotencyLookupCandidatesV1::new(vec![identity])
                .map_err(|_| AuthoritativeReadError::Integrity)?
        }
    };
    match storage
        .lookup_admission(candidates.clone())
        .map_err(map_storage_error)?
    {
        AdmissionLookupResultV1::NotFound => Ok(None),
        AdmissionLookupResultV1::MultipleMatches => Err(AuthoritativeReadError::Integrity),
        AdmissionLookupResultV1::Found(state) => {
            if !candidates.contains(state.identity()) {
                return Err(AuthoritativeReadError::Integrity);
            }
            map_admission_state(*state).map(Some)
        }
    }
}

fn map_idempotency_preparation(error: IdempotencyPreparationError) -> AuthoritativeReadError {
    match error {
        IdempotencyPreparationError::DigestProvider(
            riffdb_idempotency::IdempotencyDigestError::Unavailable,
        ) => AuthoritativeReadError::Unavailable,
        IdempotencyPreparationError::MissingIdempotencyField
        | IdempotencyPreparationError::IdempotencyFieldNotString
        | IdempotencyPreparationError::IdempotencyKeyMismatch
        | IdempotencyPreparationError::InvalidCanonicalInput
        | IdempotencyPreparationError::DigestProvider(_)
        | IdempotencyPreparationError::InvalidLookupCandidates => AuthoritativeReadError::Integrity,
    }
}

fn map_admission_state(
    state: StoredAdmissionStateV1,
) -> Result<AuthoritativeOutcomeSnapshot, AuthoritativeReadError> {
    match state {
        StoredAdmissionStateV1::Pending(pending) => Ok(AuthoritativeOutcomeSnapshot::Pending(
            outcome_facts(&pending)?,
        )),
        StoredAdmissionStateV1::StoredOutcome(outcome) => {
            let facts = AuthoritativeOutcomeFacts::new(
                outcome.plan().contract_lineage().clone(),
                outcome.plan().contract_version(),
                outcome.plan().contract_bundle_hash(),
                outcome.plan().command_id(),
                outcome.plan().command_plan_hash(),
                outcome.identity().principal_id().clone(),
                outcome.identity().tenant_scope().clone(),
                outcome.partition_key().clone(),
                outcome_locator_digest(outcome.identity())?,
            );
            let result = AuthoritativeJournaledOutcome::new(
                outcome.commit_sequence(),
                outcome.declared_outcome().outcome_id(),
                outcome.declared_outcome().value().clone(),
                outcome.provenance_id(),
                map_durability(outcome.durability_mode())?,
            );
            Ok(AuthoritativeOutcomeSnapshot::journaled(facts, result))
        }
        StoredAdmissionStateV1::ExecutionFailed(failure) => {
            Ok(AuthoritativeOutcomeSnapshot::ExecutionFailed {
                facts: outcome_facts(failure.pending())?,
                code: failure.code(),
            })
        }
    }
}

fn outcome_facts(
    pending: &StoredPendingAdmissionV1,
) -> Result<AuthoritativeOutcomeFacts, AuthoritativeReadError> {
    Ok(AuthoritativeOutcomeFacts::new(
        pending.plan().contract_lineage().clone(),
        pending.plan().contract_version(),
        pending.plan().contract_bundle_hash(),
        pending.plan().command_id(),
        pending.plan().command_plan_hash(),
        pending.identity().principal_id().clone(),
        pending.identity().tenant_scope().clone(),
        pending.partition_key().clone(),
        outcome_locator_digest(pending.identity())?,
    ))
}

fn outcome_locator_digest(
    identity: &IdempotencyIdentity,
) -> Result<OutcomeLocatorDigestEvidence, AuthoritativeReadError> {
    let digest = identity.caller_key_digest();
    OutcomeLocatorDigestEvidence::new(digest.scheme(), digest.key_id(), *digest.as_bytes())
        .map_err(|_| AuthoritativeReadError::Integrity)
}

fn read_commit(
    storage: &(impl AuthoritativePointReader + CatalogRepository),
    sequence: CommitSequence,
) -> Result<Option<AuthoritativeCommitSnapshot>, AuthoritativeReadError> {
    storage
        .read_commit(sequence)
        .map_err(map_storage_error)?
        .map(|record| map_commit_record(storage, record))
        .transpose()
}

fn map_commit_record(
    catalog: &impl CatalogRepository,
    record: StoredCommitRecordV1,
) -> Result<AuthoritativeCommitSnapshot, AuthoritativeReadError> {
    let plan = resolve_executable_plan(catalog, record.plan()).map_err(map_catalog_error)?;
    let outcome = DeclaredOutcomeView::from_bundle(
        plan.bundle(),
        record.plan().command_id(),
        record.declared_outcome().outcome_id(),
        record.declared_outcome().value().clone(),
    )
    .map_err(|_| AuthoritativeReadError::Integrity)?;
    let affected_entities = record
        .entity_references()
        .iter()
        .map(|reference| {
            AffectedEntityView::new(reference.target().key().clone(), reference.entity_version())
        })
        .collect();
    let events = record
        .events()
        .iter()
        .map(|event| {
            DurableEventView::new(
                event.event_id(),
                event.event_type_id(),
                event.payload().clone(),
            )
        })
        .collect();
    AuthoritativeCommitSnapshot::new(
        record.commit_sequence(),
        record.admission_request_id(),
        record.plan().contract_lineage().clone(),
        record.plan().contract_version(),
        record.plan().command_id(),
        record.plan().command_plan_hash(),
        record.canonical_input_hash(),
        record.actor().clone(),
        record.logical_time(),
        record.partition_hash(),
        record.conflict_hashes().to_vec(),
        affected_entities,
        events,
        outcome,
        record.provenance_id(),
        map_durability(record.durability_mode())?,
    )
    .map_err(|_| AuthoritativeReadError::Integrity)
}

fn map_durability(mode: DurabilityMode) -> Result<CommandDurability, AuthoritativeReadError> {
    match mode {
        DurabilityMode::Sync => Ok(CommandDurability::Synchronous),
        DurabilityMode::Group => Ok(CommandDurability::Group),
        DurabilityMode::Memory => Err(AuthoritativeReadError::Integrity),
    }
}

fn scan_commits(
    storage: &(impl AuthoritativeScanReader + CatalogRepository),
    request: AuthoritativeCommitScanRequest,
) -> Result<AuthoritativeCommitPage, AuthoritativeReadError> {
    let limit = StorageScanLimit::new(request.limit().get().get())
        .ok_or(AuthoritativeReadError::Integrity)?;
    let lower_request = match request {
        AuthoritativeCommitScanRequest::Initial { .. } => CommitScanRequest::initial(limit),
        AuthoritativeCommitScanRequest::Continue {
            after,
            inclusive_upper,
            ..
        } => CommitScanRequest::continuing(after, inclusive_upper, limit)
            .map_err(|_| AuthoritativeReadError::InvalidContinuation)?,
    };
    let lower = storage
        .scan_commits(lower_request)
        .map_err(map_storage_error)?;
    let (records, next_after, inclusive_upper) = match lower {
        CommitScanPageV1::Page {
            records,
            next_after,
            inclusive_upper,
        } => (records, Some(next_after), inclusive_upper),
        CommitScanPageV1::ExactEnd {
            records,
            inclusive_upper,
        } => (records, None, inclusive_upper),
    };
    let commits = records
        .into_iter()
        .map(|record| map_commit_record(storage, record.into_parts().0))
        .collect::<Result<Vec<_>, _>>()?;
    AuthoritativeCommitPage::new(request, inclusive_upper, commits, next_after)
        .map_err(|_| AuthoritativeReadError::Integrity)
}

fn read_provenance(
    storage: &impl AuthoritativePointReader,
    selector: ProvenanceSelector,
) -> Result<Option<AuthoritativeProvenanceSnapshot>, AuthoritativeReadError> {
    let record = match selector {
        ProvenanceSelector::Commit(sequence) => {
            let Some(commit) = storage.read_commit(sequence).map_err(map_storage_error)? else {
                return Ok(None);
            };
            let record = storage
                .read_provenance(commit.provenance_id())
                .map_err(map_storage_error)?
                .ok_or(AuthoritativeReadError::Integrity)?;
            if record.commit_sequence() != sequence
                || record.provenance_id() != commit.provenance_id()
            {
                return Err(AuthoritativeReadError::Integrity);
            }
            record
        }
        ProvenanceSelector::Provenance(provenance_id) => {
            let Some(record) = storage
                .read_provenance(provenance_id)
                .map_err(map_storage_error)?
            else {
                return Ok(None);
            };
            if record.provenance_id() != provenance_id {
                return Err(AuthoritativeReadError::Integrity);
            }
            record
        }
    };
    map_provenance_record(record).map(Some)
}

fn map_provenance_record(
    record: StoredProvenanceRecordV1,
) -> Result<AuthoritativeProvenanceSnapshot, AuthoritativeReadError> {
    let affected_entities = record
        .affected_entities()
        .iter()
        .map(|affected| {
            AffectedEntityView::new(affected.target().key().clone(), affected.entity_version())
        })
        .collect();
    let claims = ProvenanceClaimsView::new(
        record.admitted_claims().source_repository().cloned(),
        record.admitted_claims().source_commit().cloned(),
        record.admitted_claims().reason().cloned(),
        record.admitted_claims().approval_id().cloned(),
    );
    AuthoritativeProvenanceSnapshot::new(
        record.provenance_id(),
        record.commit_sequence(),
        record.admission_request_id(),
        record.plan().contract_lineage().clone(),
        record.plan().contract_version(),
        record.plan().command_id(),
        record.plan().command_plan_hash(),
        record.actor().clone(),
        record.logical_time(),
        record.outcome_id(),
        affected_entities,
        record.event_ids().to_vec(),
        claims,
    )
    .map_err(|_| AuthoritativeReadError::Integrity)
}

fn read_revoke_target(
    storage: &impl CapabilityReader,
    database_id: DatabaseId,
    environment: &Environment,
    capability_id: CapabilityId,
) -> Result<CapabilityRevokeTargetSnapshot, AuthoritativeReadError> {
    let Some(record) = storage
        .read_capability(capability_id)
        .map_err(map_storage_error)?
    else {
        return Ok(CapabilityRevokeTargetSnapshot::Absent(
            AbsentCapabilityRevokeTargetSnapshot::new(
                capability_id,
                database_id,
                environment.clone(),
            ),
        ));
    };
    if record.capability_id() != capability_id
        || record.database_id() != database_id
        || record.environment() != environment
    {
        return Err(AuthoritativeReadError::Integrity);
    }
    let activity = match record.lifecycle() {
        CapabilityLifecycleV1::Active => CapabilityActivity::Active,
        CapabilityLifecycleV1::Revoked { .. } => CapabilityActivity::Revoked,
    };
    let snapshot = PresentCapabilityRevokeTargetSnapshot::new(
        record.capability_id(),
        record.revision(),
        activity,
        record.database_id(),
        record.environment().clone(),
        record.principal_id().clone(),
        record.actor_kind(),
        record.audiences().to_vec(),
        record.issued_at(),
        record.expires_at(),
        record.grant().clone(),
    )
    .map_err(|_| AuthoritativeReadError::Integrity)?;
    Ok(CapabilityRevokeTargetSnapshot::Present(Box::new(snapshot)))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use riffdb_contract_ir::{KeyComponentSchema, KeyPurpose, KeySchema, ValueType};
    use riffdb_idempotency::{IdempotencyDigestCandidatesV1, IdempotencyDigestError};
    use riffdb_policy::PartitionConstraint;
    use riffdb_storage_api::{
        AdmissionRequestV1, AdmissionResultV1, AuthoritativeIndexScanPage,
        AuthoritativeIndexScanRequest, DurableKeySchemaBindingV1, EncodedContentCharge,
        EncodedPageItem, StoredIndexEntryV2,
    };
    use riffdb_types::{
        ActorId, AggregateTypeId, CanonicalRecord, CanonicalString, CommandId, ContractBundleHash,
        DigestKeyId, EntityKeyBuilder, EntityTypeId, EventId, IdempotencyKey, IndexEntryKey,
        IndexEntryKeyBuilder, IndexEpoch, IndexId, PartitionKey, PartitionKeyBuilder,
        PartitionScopeV1, ProvenanceId, ScopedPartitionV1, TenantScope,
    };

    use super::*;

    struct EmptyCatalog;

    impl CatalogRepository for EmptyCatalog {
        fn read_active_catalog(
            &self,
        ) -> Result<Option<riffdb_storage_api::ActiveCatalogPointerV1>, StorageError> {
            Ok(None)
        }

        fn read_contract_bundle(
            &self,
            _lineage: &ContractLineage,
            _contract_version: ContractVersion,
        ) -> Result<Option<riffdb_storage_api::StoredContractBundleV1>, StorageError> {
            Ok(None)
        }
    }

    impl AuthoritativeScanReader for EmptyCatalog {
        fn scan_index(
            &self,
            _request: AuthoritativeIndexScanRequest,
        ) -> Result<AuthoritativeIndexScanPage, StorageError> {
            panic!("unexpected index scan")
        }

        fn scan_commits(
            &self,
            request: CommitScanRequest,
        ) -> Result<CommitScanPageV1, StorageError> {
            // Empty catalog has no commits. Only an initial scan with
            // `after = None` can fence on BeforeFirst; any resume `after`
            // must fail closed rather than silently ignore the cursor.
            if request.after().is_some() {
                return Err(StorageError::new(
                    StorageErrorKind::InvariantViolation,
                    None,
                ));
            }
            CommitScanPageV1::exact_end(
                request,
                riffdb_types::FrontierPosition::BeforeFirst,
                Vec::new(),
            )
            .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))
        }
    }

    struct EmptyPointReader;

    impl AuthoritativePointReader for EmptyPointReader {
        fn read_entity(
            &self,
            _target: &EntityTarget,
        ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError> {
            Ok(None)
        }

        fn read_stored_outcome(
            &self,
            _identity: &riffdb_storage_api::IdempotencyIdentity,
        ) -> Result<Option<riffdb_storage_api::StoredOutcomeV1>, StorageError> {
            Ok(None)
        }

        fn read_commit(
            &self,
            _sequence: CommitSequence,
        ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
            Ok(None)
        }

        fn read_provenance(
            &self,
            _provenance_id: ProvenanceId,
        ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
            Ok(None)
        }

        fn read_durable_event(
            &self,
            _event_id: EventId,
        ) -> Result<Option<riffdb_storage_api::StoredDurableEventV1>, StorageError> {
            Ok(None)
        }
    }

    struct EmptyCapabilityReader;

    impl CapabilityReader for EmptyCapabilityReader {
        fn read_capability(
            &self,
            _capability_id: CapabilityId,
        ) -> Result<Option<riffdb_storage_api::StoredCapabilityRecordV1>, StorageError> {
            Ok(None)
        }

        fn resolve_capability_digests(
            &self,
            _candidates: &[riffdb_types::CapabilityTokenDigest],
        ) -> Result<riffdb_storage_api::CapabilityLookupResult, StorageError> {
            Ok(riffdb_storage_api::CapabilityLookupResult::NotFound)
        }
    }

    struct FixedIdempotencyDigests(IdempotencyKeyDigest);

    impl IdempotencyDigestProvider for FixedIdempotencyDigests {
        fn digest_candidates(
            &self,
            _caller_key: &IdempotencyKey,
        ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
            IdempotencyDigestCandidatesV1::new(vec![self.0])
        }
    }

    #[derive(Default)]
    struct RecordingAdmissionRepository {
        lookups: Mutex<Vec<IdempotencyLookupCandidatesV1>>,
    }

    impl RecordingAdmissionRepository {
        fn lookups(&self) -> Vec<IdempotencyLookupCandidatesV1> {
            self.lookups.lock().expect("admission lookups").clone()
        }
    }

    impl AdmissionRepository for RecordingAdmissionRepository {
        fn admit_or_resolve(
            &self,
            _request: AdmissionRequestV1,
        ) -> Result<AdmissionResultV1, StorageError> {
            panic!("read adapter must never create admission state")
        }

        fn lookup_admission(
            &self,
            candidates: IdempotencyLookupCandidatesV1,
        ) -> Result<AdmissionLookupResultV1, StorageError> {
            self.lookups
                .lock()
                .expect("admission lookups")
                .push(candidates);
            Ok(AdmissionLookupResultV1::NotFound)
        }
    }

    struct FixedFilteredReader {
        entries: Vec<EncodedPageItem<StoredIndexEntryV2>>,
        scanned_through: Option<IndexEntryKey>,
        requests: Mutex<Vec<FilteredAuthoritativeIndexScanRequest>>,
    }

    impl FixedFilteredReader {
        fn new(
            entries: Vec<EncodedPageItem<StoredIndexEntryV2>>,
            scanned_through: Option<IndexEntryKey>,
        ) -> Self {
            Self {
                entries,
                scanned_through,
                requests: Mutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<FilteredAuthoritativeIndexScanRequest> {
            self.requests
                .lock()
                .expect("filtered requests mutex")
                .clone()
        }
    }

    impl FilteredAuthoritativeScanReader for FixedFilteredReader {
        fn scan_index_filtered(
            &self,
            request: FilteredAuthoritativeIndexScanRequest,
        ) -> Result<FilteredAuthoritativeIndexScanPage, StorageError> {
            self.requests
                .lock()
                .expect("filtered requests mutex")
                .push(request.clone());
            let epoch = riffdb_types::IndexEpochPosition::Value(IndexEpoch::first());
            let page = match &self.scanned_through {
                Some(scanned_through) => FilteredAuthoritativeIndexScanPage::page(
                    &request,
                    epoch,
                    self.entries.clone(),
                    scanned_through.clone(),
                ),
                None => FilteredAuthoritativeIndexScanPage::exact_end(
                    &request,
                    epoch,
                    self.entries.clone(),
                ),
            }
            .expect("fixed filtered fixture must satisfy the lower contract");
            Ok(page)
        }
    }

    fn adapter_lineage() -> ContractLineage {
        ContractLineage::new("read-adapter-index").expect("lineage")
    }

    fn adapter_partition(value: u64) -> PartitionKey {
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(value).expect("partition component");
        partition.finish().expect("partition")
    }

    fn adapter_index_key(value: u64) -> IndexEntryKey {
        let mut entity = EntityKeyBuilder::new(EntityTypeId::first());
        entity.push_u64(value).expect("entity component");
        let mut index = IndexEntryKeyBuilder::new(IndexId::new(7).expect("index"));
        index.push_u64(value).expect("index component");
        index
            .finish(entity.finish().expect("entity key"))
            .expect("index key")
    }

    fn adapter_index_prefix() -> riffdb_contract_ir::IndexScanPrefix {
        let component =
            KeyComponentSchema::new(ValueType::u64(), Vec::new()).expect("key component");
        let entity = KeySchema::new(
            KeyPurpose::Entity(EntityTypeId::first()),
            vec![component.clone()],
        )
        .expect("entity key schema");
        KeySchema::index(
            IndexId::new(7).expect("index"),
            EntityTypeId::first(),
            vec![component],
            entity,
        )
        .expect("index key schema")
        .encode_index_prefix(&[])
        .expect("whole-index prefix")
    }

    fn adapter_index_request(
        scope: PartitionScopeV1,
        after: Option<IndexEntryKey>,
    ) -> AuthoritativeIndexRequest {
        AuthoritativeIndexRequest::new(
            adapter_lineage(),
            ContractVersion::new(1).expect("version"),
            IndexId::new(7).expect("index"),
            Vec::new(),
            adapter_index_prefix(),
            PartitionConstraint::Filter(scope),
            after,
            riffdb_service::PageLimit::new(10).expect("page limit"),
        )
        .expect("authoritative index request")
    }

    fn adapter_index_row(partition: PartitionKey) -> EncodedPageItem<StoredIndexEntryV2> {
        let row = StoredIndexEntryV2::new(
            adapter_index_key(7),
            DurableKeySchemaBindingV1::new(
                adapter_lineage(),
                ContractVersion::new(1).expect("version"),
                ContractBundleHash::from_bytes([0x44; 32]),
            ),
            CanonicalRecord::new(Vec::new()).expect("covered values"),
            partition,
        )
        .expect("stored V2 index row");
        EncodedPageItem::new(
            row,
            EncodedContentCharge::new(1).expect("test envelope charge"),
        )
    }

    #[test]
    fn storage_failures_keep_unavailable_and_integrity_distinct() {
        assert_eq!(
            map_storage_error(StorageError::new(StorageErrorKind::Unavailable, None)),
            AuthoritativeReadError::Unavailable
        );
        assert_eq!(
            map_storage_error(StorageError::new(StorageErrorKind::CorruptData, None)),
            AuthoritativeReadError::Integrity
        );
    }

    #[test]
    fn absent_catalog_version_remains_ordinary_absence() {
        let lineage = ContractLineage::new("read-adapter-catalog").expect("lineage");

        assert!(
            read_contract_version(
                &EmptyCatalog,
                lineage,
                ContractVersion::new(1).expect("version"),
            )
            .expect("empty catalog read")
            .is_none()
        );
    }

    #[test]
    fn absent_entity_remains_ordinary_absence() {
        let mut key = EntityKeyBuilder::new(EntityTypeId::first());
        key.push_u64(41).expect("entity key component");
        let request = AuthoritativeEntityRequest::new(
            ContractLineage::new("read-adapter-entity").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            key.finish().expect("entity key"),
        );

        assert!(
            read_entity(&EmptyPointReader, request)
                .expect("empty entity read")
                .is_none()
        );
    }

    #[test]
    fn raw_key_and_locator_digest_share_one_exact_point_lookup_identity() {
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("database UUIDv7");
        let environment = Environment::new("test").expect("environment");
        let principal_id = ActorId::new("outcome-owner").expect("principal");
        let lineage = ContractLineage::new("outcome-lookup").expect("lineage");
        let command_id = CommandId::new(7).expect("command ID");
        let key_id = DigestKeyId::new(9).expect("digest key ID");
        let digest = IdempotencyKeyDigest::from_hmac_bytes(key_id, [0x5a; 32]);
        let provider = FixedIdempotencyDigests(digest);
        let readable = ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(key_id)])
            .expect("readable digest inventory");
        let storage = RecordingAdmissionRepository::default();

        let raw = AuthoritativeOutcomeRequest::raw_key(
            lineage.clone(),
            command_id,
            principal_id.clone(),
            TenantScope::Global,
            IdempotencyKey::new("same-outcome-key").expect("idempotency key"),
        );
        assert!(
            read_outcome(
                &storage,
                &provider,
                &readable,
                database_id,
                &environment,
                raw,
            )
            .expect("raw-key lookup")
            .is_none()
        );

        let locator = AuthoritativeOutcomeRequest::digested(
            lineage,
            command_id,
            principal_id,
            TenantScope::Global,
            OutcomeLocatorDigestEvidence::new(digest.scheme(), key_id, *digest.as_bytes())
                .expect("locator digest"),
        );
        assert!(
            read_outcome(
                &storage,
                &provider,
                &readable,
                database_id,
                &environment,
                locator,
            )
            .expect("locator lookup")
            .is_none()
        );

        let lookups = storage.lookups();
        assert_eq!(lookups.len(), 2);
        assert_eq!(lookups[0], lookups[1]);

        let unreadable = ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(
            DigestKeyId::new(10).expect("different key ID"),
        )])
        .expect("different readable inventory");
        let unreadable_locator = AuthoritativeOutcomeRequest::digested(
            ContractLineage::new("outcome-lookup").expect("lineage"),
            command_id,
            ActorId::new("outcome-owner").expect("principal"),
            TenantScope::Global,
            OutcomeLocatorDigestEvidence::new(digest.scheme(), key_id, *digest.as_bytes())
                .expect("locator digest"),
        );
        assert!(
            read_outcome(
                &storage,
                &provider,
                &unreadable,
                database_id,
                &environment,
                unreadable_locator,
            )
            .expect("unreadable locator is nondisclosing absence")
            .is_none()
        );
        assert_eq!(storage.lookups().len(), 2);
    }

    #[test]
    fn memory_durability_is_never_released_from_the_production_adapter() {
        assert_eq!(
            map_durability(DurabilityMode::Memory),
            Err(AuthoritativeReadError::Integrity)
        );
        assert_eq!(
            map_durability(DurabilityMode::Sync),
            Ok(CommandDurability::Synchronous)
        );
    }

    #[test]
    fn empty_commit_scan_preserves_the_atomic_before_first_fence() {
        let request = AuthoritativeCommitScanRequest::Initial {
            limit: riffdb_service::PageLimit::new(5).expect("page limit"),
        };

        let page = scan_commits(&EmptyCatalog, request).expect("empty commit page");

        assert!(page.commits().is_empty());
        assert_eq!(page.next_after(), None);
        assert_eq!(
            page.inclusive_upper(),
            riffdb_types::FrontierPosition::BeforeFirst
        );
    }

    #[test]
    fn empty_catalog_commit_scan_rejects_resume_after_rather_than_ignoring_it() {
        use riffdb_storage_api::StorageScanLimit;
        let request = CommitScanRequest::initial_after(
            riffdb_types::CommitSequence::first(),
            StorageScanLimit::new(5).expect("limit"),
        );
        let error = EmptyCatalog
            .scan_commits(request)
            .expect_err("resume after on empty catalog must fail closed");
        assert_eq!(error.kind(), StorageErrorKind::InvariantViolation);
    }

    #[test]
    fn storage_prefix_reconstruction_preserves_canonical_components() {
        let index_id = IndexId::first();
        let mut builder = IndexRangePrefixBuilder::new(index_id);
        let components = [
            CanonicalValue::String(CanonicalString::new("north").expect("bounded string")),
            CanonicalValue::U64(2026),
        ];
        for component in &components {
            push_index_component(&mut builder, component).expect("supported key component");
        }
        let prefix = builder.finish();

        let mut expected = IndexEntryKeyBuilder::new(index_id);
        expected.push_str("north").expect("string component");
        expected.push_u64(2026).expect("integer component");

        assert_eq!(prefix.as_bytes(), expected.as_bytes());
    }

    #[test]
    fn unsupported_index_component_fails_closed() {
        let mut builder = IndexRangePrefixBuilder::new(IndexId::first());

        assert_eq!(
            push_index_component(&mut builder, &CanonicalValue::Null),
            Err(AuthoritativeReadError::Integrity)
        );
    }

    #[test]
    fn partition_scope_conversion_is_exact_and_mixed_lineage_fails_closed() {
        let lineage = ContractLineage::new("read-adapter-test").expect("lineage");
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(7).expect("partition component");
        let partition = partition.finish().expect("partition");
        let scope = PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(
            lineage.clone(),
            partition.clone(),
        )])
        .expect("one explicit partition");

        let explicit = lower_index_partition_filter(&lineage, &PartitionConstraint::Filter(scope))
            .expect("exact explicit filter");
        assert!(matches!(
            explicit.scope(),
            IndexPartitionFilterScope::Explicit(keys) if keys == &[partition]
        ));

        let all = lower_index_partition_filter(
            &lineage,
            &PartitionConstraint::Filter(PartitionScopeV1::All),
        )
        .expect("all filter");
        assert!(matches!(all.scope(), IndexPartitionFilterScope::All));

        let other = ContractLineage::new("other-lineage").expect("other lineage");
        let mut mixed_partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        mixed_partition.push_u64(8).expect("partition component");
        let mixed = PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(
            other,
            mixed_partition.finish().expect("partition"),
        )])
        .expect("mixed scope");
        assert!(matches!(
            lower_index_partition_filter(&lineage, &PartitionConstraint::Filter(mixed),),
            Err(AuthoritativeReadError::Integrity)
        ));
    }

    #[test]
    fn explicit_partition_scope_reaches_the_filtered_storage_request() {
        let partition = adapter_partition(7);
        let scope = PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(
            adapter_lineage(),
            partition.clone(),
        )])
        .expect("one explicit partition");
        let storage = FixedFilteredReader::new(vec![adapter_index_row(partition.clone())], None);

        let page = scan_index(&storage, adapter_index_request(scope, None))
            .expect("filtered adapter scan");

        assert_eq!(page.rows().len(), 1);
        assert_eq!(page.rows()[0].key(), &adapter_index_key(7));
        assert_eq!(page.rows()[0].stored_partition(), &partition);
        assert_eq!(
            page.rows()[0].schema_binding().lineage(),
            &adapter_lineage()
        );
        assert!(page.scanned_through().is_none());
        let requests = storage.requests();
        assert!(matches!(
            requests.as_slice(),
            [request]
                if request.partition_filter().target_lineage() == &adapter_lineage()
                    && matches!(
                        request.partition_filter().scope(),
                        IndexPartitionFilterScope::Explicit(keys) if keys == &[partition]
                    )
        ));
    }

    #[test]
    fn sparse_filtered_progress_maps_to_an_empty_service_page() {
        let scanned_through = adapter_index_key(41);
        let storage = FixedFilteredReader::new(Vec::new(), Some(scanned_through.clone()));
        let partition = adapter_partition(7);
        let scope = PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(
            adapter_lineage(),
            partition.clone(),
        )])
        .expect("one exact partition");

        let page =
            scan_index(&storage, adapter_index_request(scope, None)).expect("sparse adapter scan");

        assert!(page.rows().is_empty());
        assert_eq!(page.scanned_through(), Some(&scanned_through));
        assert_eq!(
            page.epoch(),
            riffdb_types::IndexEpochPosition::Value(IndexEpoch::first())
        );
        let requests = storage.requests();
        assert!(matches!(
            requests.as_slice(),
            [request]
                if request.after().is_none()
                    && request.limit().get() == 10
                    && matches!(
                        request.partition_filter().scope(),
                        IndexPartitionFilterScope::Explicit(keys) if keys == &[partition]
                    )
        ));
    }

    #[test]
    fn absent_revoke_target_contains_only_trusted_scope() {
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1, [7; 10]).expect("database UUIDv7");
        let environment = Environment::new("test").expect("environment");
        let capability_id =
            CapabilityId::from_unix_milliseconds_and_random(2, [8; 10]).expect("capability UUIDv7");

        let snapshot = read_revoke_target(
            &EmptyCapabilityReader,
            database_id,
            &environment,
            capability_id,
        )
        .expect("absent capability read");

        let CapabilityRevokeTargetSnapshot::Absent(snapshot) = snapshot else {
            panic!("expected absent revoke target");
        };
        assert_eq!(snapshot.capability_id(), capability_id);
        assert_eq!(snapshot.database_id(), database_id);
        assert_eq!(snapshot.environment(), &environment);
    }

    #[test]
    fn four_admission_outcomes_map_to_four_distinct_errors() {
        assert_eq!(
            map_authoritative_admission(PortAdmissionError::Cancelled),
            AuthoritativeReadError::Cancelled
        );
        assert_eq!(
            map_authoritative_admission(PortAdmissionError::DeadlineExceeded),
            AuthoritativeReadError::DeadlineExceeded
        );
        assert_eq!(
            map_authoritative_admission(PortAdmissionError::Unavailable),
            AuthoritativeReadError::Unavailable
        );
        assert_eq!(
            map_authoritative_admission(PortAdmissionError::Stopped),
            AuthoritativeReadError::Integrity
        );
    }

    #[test]
    fn stopped_preparatory_driver_is_an_integrity_failure() {
        assert_eq!(
            map_authoritative_admission(PortAdmissionError::Stopped),
            AuthoritativeReadError::Integrity
        );
    }

    #[test]
    fn production_adapters_implement_the_exact_service_ports() {
        fn assert_catalog<T: CatalogReadPort + Send + Sync>() {}
        fn assert_authoritative<T: AuthoritativeReadPort + Send + Sync>() {}

        assert_catalog::<ServerCatalogReadPort>();
        assert_authoritative::<ServerAuthoritativeReadPort>();
    }

    /// Cache hits share one published snapshot. Load-bearing assertion is
    /// [`ActiveCatalogSnapshot::same_publication_as`] (Arc publication identity).
    /// Outer `Arc::ptr_eq` on the view's hand-out is supporting evidence only.
    #[test]
    fn active_catalog_view_hands_out_the_same_arc_without_republish() {
        use riffdb_contract_compiler::compile_contract_source;
        use std::num::NonZeroU64;

        use riffdb_storage_api::{
            AuditPrincipalV1, CatalogActivationIntentV1, CatalogActivationResult,
            CatalogAdministrationRepository, CatalogRepository,
        };
        use riffdb_types::{ActorId, ActorKind, CapabilityId, RequestId, Timestamp};

        use crate::real_storage_support::RealStorage;

        const CONTRACT: &str =
            include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");

        let real = RealStorage::open("catalog-arc");
        let mut storage = real.storage.clone();
        let bundle = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(CONTRACT).expect("ticketdesk contract compiles"),
        )
        .expect("compiled bundle is catalog-valid");
        let activated = CatalogAdministrationRepository::activate_catalog(
            &mut storage,
            &CatalogActivationIntentV1::new(
                None,
                bundle.to_stored().expect("encode contract bundle"),
                RequestId::from_unix_milliseconds_and_random(1, [0x41; 10]).expect("request"),
                AuditPrincipalV1::new(
                    ActorId::new("catalog-arc").expect("actor"),
                    ActorKind::Human,
                    CapabilityId::from_unix_milliseconds_and_random(3, [0x3c; 10])
                        .expect("capability"),
                    NonZeroU64::MIN,
                ),
                Timestamp::new(1_000, 0).expect("timestamp"),
                None,
            ),
        )
        .expect("activate");
        assert!(matches!(
            activated,
            CatalogActivationResult::Activated { .. }
        ));

        let cache = Mutex::new(ActiveCatalogView::default());
        let first = read_active_catalog_cached(&storage, &cache)
            .expect("first read")
            .expect("active catalog present");
        let second = read_active_catalog_cached(&storage, &cache)
            .expect("warm hit")
            .expect("active catalog present");
        assert!(
            first.same_publication_as(&second),
            "two gets without republish must share one publication (same_publication_as)"
        );
        let pointer = storage
            .read_active_catalog()
            .expect("pointer")
            .expect("active pointer");
        let a = cache
            .lock()
            .expect("cache")
            .get(Some(&pointer))
            .expect("hit")
            .expect("present");
        let b = cache
            .lock()
            .expect("cache")
            .get(Some(&pointer))
            .expect("hit")
            .expect("present");
        assert!(
            Arc::ptr_eq(&a, &b),
            "view hand-out is Arc::clone (supporting identity evidence)"
        );
    }

    /// Publication replaces the Arc: activate v1 → warm → activate v2 → reader
    /// observes v2 under a different publication identity.
    #[test]
    fn active_catalog_cache_republish_makes_successor_visible() {
        use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
        use std::num::NonZeroU64;

        use riffdb_storage_api::{
            AuditPrincipalV1, CatalogActivationIntentV1, CatalogActivationResult,
            CatalogAdministrationRepository,
        };
        use riffdb_types::{ActorId, ActorKind, CapabilityId, RequestId, Timestamp};

        use crate::real_storage_support::RealStorage;

        const CONTRACT: &str =
            include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");

        fn principal() -> AuditPrincipalV1 {
            AuditPrincipalV1::new(
                ActorId::new("catalog-republish").expect("actor"),
                ActorKind::Human,
                CapabilityId::from_unix_milliseconds_and_random(3, [0x3d; 10]).expect("capability"),
                NonZeroU64::MIN,
            )
        }

        let real = RealStorage::open("catalog-republish");
        let mut storage = real.storage.clone();
        let v1 = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(CONTRACT).expect("v1 compiles"),
        )
        .expect("v1 catalog-valid");
        let activated = CatalogAdministrationRepository::activate_catalog(
            &mut storage,
            &CatalogActivationIntentV1::new(
                None,
                v1.to_stored().expect("encode v1"),
                RequestId::from_unix_milliseconds_and_random(1, [0x51; 10]).expect("request"),
                principal(),
                Timestamp::new(1_000, 0).expect("timestamp"),
                None,
            ),
        )
        .expect("activate v1");
        assert!(matches!(
            activated,
            CatalogActivationResult::Activated { .. }
        ));

        let cache = Mutex::new(ActiveCatalogView::default());
        let warm_v1 = read_active_catalog_cached(&storage, &cache)
            .expect("warm v1")
            .expect("present");
        assert_eq!(warm_v1.pointer().contract_version().get(), 1);

        let successor_source = CONTRACT.replacen("version 1", "version 2", 1);
        let v2 = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_successor(&successor_source, v1.bundle())
                .expect("compatible successor"),
        )
        .expect("v2 catalog-valid");
        let activated = CatalogAdministrationRepository::activate_catalog(
            &mut storage,
            &CatalogActivationIntentV1::new(
                Some(v1.contract_version()),
                v2.to_stored().expect("encode v2"),
                RequestId::from_unix_milliseconds_and_random(2, [0x52; 10]).expect("request"),
                principal(),
                Timestamp::new(1_001, 0).expect("timestamp"),
                None,
            ),
        )
        .expect("activate v2");
        assert!(matches!(
            activated,
            CatalogActivationResult::Activated { .. }
        ));

        let after = read_active_catalog_cached(&storage, &cache)
            .expect("post-republish read")
            .expect("present");
        assert_eq!(after.pointer().contract_version().get(), 2);
        assert!(
            !warm_v1.same_publication_as(&after),
            "successor publication must not share Arc identity with the prior active snapshot"
        );
    }

    /// Real `ServerCatalogReadPort` over a real on-disk redb database with a
    /// deployed query module.
    ///
    /// This is the only construction of the production catalog read port
    /// outside `process_graph`, so it is the seam that makes the inline
    /// plan-lookup path falsifiable: deleting the fast path makes the
    /// "cache hit does not enter the pool" assertions fail, and serving the
    /// fast path without admission control makes the drained-routing
    /// assertion fail.
    #[test]
    fn real_catalog_read_port_serves_warm_plan_lookups_inline_and_pools_everything_else() {
        use std::time::{Duration, Instant};

        use riffdb_contract_compiler::compile_contract_source;
        use riffdb_query_module::{
            NamedQuerySource, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
        };
        use std::num::NonZeroU64;

        use riffdb_service::{
            AuthoritativeReadinessFailure, QueryModuleReadPort, ServiceHealthHooks,
        };
        use riffdb_storage_api::{
            AuditPrincipalV1, CatalogActivationIntentV1, CatalogActivationResult,
            CatalogAdministrationRepository, QueryModuleActivationIntentV1,
            QueryModuleActivationResult, QueryModuleActiveExpectationV1,
            QueryModuleAdministrationRepository,
        };
        use riffdb_types::{ActorId, ActorKind, CapabilityId, RequestId, Timestamp};

        use crate::port_driver::BlockingPortDriver;
        use crate::real_storage_support::RealStorage;
        use crate::runtime_support::RuntimeRoutingState;

        const CONTRACT: &str =
            include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
        const LIST_TICKETS: &str = include_str!("../../../queries/ticketdesk/list_tickets.riffq");

        fn principal() -> AuditPrincipalV1 {
            AuditPrincipalV1::new(
                ActorId::new("plan-lookup-operator").expect("bounded principal"),
                ActorKind::Human,
                CapabilityId::from_unix_milliseconds_and_random(3, [0x3c; 10])
                    .expect("capability UUIDv7"),
                NonZeroU64::MIN,
            )
        }

        fn request_id(seed: u8) -> RequestId {
            RequestId::from_unix_milliseconds_and_random(u64::from(seed), [seed; 10])
                .expect("request UUIDv7")
        }

        let real = RealStorage::open("plan-lookup");
        let mut storage = real.storage.clone();

        let bundle = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(CONTRACT).expect("ticketdesk contract compiles"),
        )
        .expect("compiled bundle is catalog-valid");
        let activated = CatalogAdministrationRepository::activate_catalog(
            &mut storage,
            &CatalogActivationIntentV1::new(
                None,
                bundle.to_stored().expect("encode contract bundle"),
                request_id(0x41),
                principal(),
                Timestamp::new(1_000, 0).expect("activation timestamp"),
                None,
            ),
        )
        .expect("activate the contract in real storage");
        assert!(matches!(
            activated,
            CatalogActivationResult::Activated { .. }
        ));

        let module = ValidatedQueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("ticketdesk").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![NamedQuerySource::new("ListTickets", LIST_TICKETS).expect("query source")],
            )
            .expect("module candidate"),
            &bundle,
        )
        .expect("query module compiles against the activated contract");
        let module_hash = module.identity();
        let deployed = QueryModuleAdministrationRepository::activate_query_module(
            &mut storage,
            &QueryModuleActivationIntentV1::new(
                QueryModuleActiveExpectationV1::Absent,
                module.to_stored().expect("encode query module"),
                request_id(0x42),
                principal(),
                Timestamp::new(1_001, 0).expect("deployment timestamp"),
                None,
            ),
        )
        .expect("deploy the query module into real storage");
        assert!(matches!(
            deployed,
            QueryModuleActivationResult::Activated { .. }
        ));

        let routing = RuntimeRoutingState::new();
        let driver = BlockingPortDriver::new(routing.clone()).expect("blocking port driver starts");
        let port = ServerCatalogReadPort::new(real.storage.clone(), &driver);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test runtime");
        let (control, _cancellation) =
            RequestControl::new(Instant::now() + Duration::from_secs(30));

        let baseline = query_module_pool_dispatch_count();
        runtime.block_on(async {
            // Cold: the plan cache is empty, so compilation must happen on the
            // blocking pool.
            let cold = port
                .prepare_query_module(&control, bundle.clone(), module_hash)
                .await
                .expect("cold query-module lookup succeeds");
            assert_eq!(
                cold.expect("deployed module resolves").identity(),
                module_hash
            );
            assert_eq!(
                query_module_pool_dispatch_count(),
                baseline + 1,
                "a cold plan lookup must enter the blocking pool"
            );

            // Warm: identical result, served inline.
            let warm = port
                .prepare_query_module(&control, bundle.clone(), module_hash)
                .await
                .expect("warm query-module lookup succeeds");
            assert_eq!(
                warm.expect("deployed module resolves").identity(),
                module_hash
            );
            assert_eq!(
                query_module_pool_dispatch_count(),
                baseline + 1,
                "a plan-cache hit must not enter the blocking pool"
            );

            // The active-pointer entry point resolves the same module inline:
            // the pointer is resident from the startup view rebuild plus the
            // deployment publish, and the plan cache is now warm.
            let active = port
                .prepare_active_query_module(&control, bundle.clone())
                .await
                .expect("active query-module lookup succeeds");
            assert_eq!(
                active.expect("active module resolves").identity(),
                module_hash
            );
            assert_eq!(
                query_module_pool_dispatch_count(),
                baseline + 1,
                "an active-pointer cache hit must not enter the blocking pool"
            );
        });

        // Admission control: once routing stops, a warm cache must not be a
        // bypass. The request has to reach the pool and be refused there.
        routing.fail_authoritative_readiness(AuthoritativeReadinessFailure::Integrity);
        runtime.block_on(async {
            let refused = port
                .prepare_query_module(&control, bundle.clone(), module_hash)
                .await;
            assert!(
                matches!(refused, Err(QueryModuleReadError::Unavailable)),
                "a request refused by port admission must not be served from cache"
            );
            assert_eq!(
                query_module_pool_dispatch_count(),
                baseline + 2,
                "a refused request must be routed to the pool, not answered inline"
            );
        });

        driver.shutdown_and_drain().expect("driver drains cleanly");
    }
}
