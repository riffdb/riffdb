//! Production catalog and authoritative-read adapters for the API-neutral service.

use std::fmt;
use std::sync::Arc;

use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogError, CatalogErrorKind, CatalogPreparationResult,
    ResolvedExecutablePlan, ValidatedContractBundle, prepare_catalog_activation,
    resolve_executable_plan,
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
    ProvenanceClaimsView, RequestControl,
};
use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, AuthoritativePointReader,
    AuthoritativeScanReader, CapabilityLifecycleV1, CapabilityReader, CatalogRepository,
    CommitScanPageV1, CommitScanRequest, DurabilityMode, EntityTarget, ExecutablePlanRef,
    FilteredAuthoritativeIndexScanPage, FilteredAuthoritativeIndexScanRequest,
    FilteredAuthoritativeScanReader, IdempotencyIdentity, IdempotencyKeyDigest,
    IdempotencyLookupCandidatesV1, IndexPartitionFilter, IndexPartitionFilterScope,
    IndexRangePrefixBuilder, IndexRangeTarget, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, StorageError, StorageErrorKind, StorageScanLimit,
    StoredAdmissionStateV1, StoredCommitRecordV1, StoredPendingAdmissionV1,
    StoredProvenanceRecordV1,
};
use riffdb_types::{
    CanonicalValue, CapabilityId, CommitSequence, ContractLineage, ContractVersion, DatabaseId,
    Environment,
};

use crate::notifications::FirstCommitNotificationHub;
use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};
use crate::storage::SharedRedbOperationalPorts;

type ContractVersionRequest = (ContractLineage, ContractVersion);
type DeploymentRequest = (ContractBundle, Option<ContractVersion>);

/// Catalog-owned semantic reads driven on the retained blocking worker set.
pub(crate) struct ServerCatalogReadPort {
    active: BlockingPortExecutor<(), Option<ActiveCatalogSnapshot>, CatalogError>,
    contract_version:
        BlockingPortExecutor<ContractVersionRequest, Option<ValidatedContractBundle>, CatalogError>,
    executable_plan:
        BlockingPortExecutor<CatalogExecutablePlanRequest, ResolvedExecutablePlan, CatalogError>,
    deployment: BlockingPortExecutor<DeploymentRequest, CatalogPreparationResult, CatalogError>,
}

impl ServerCatalogReadPort {
    /// Builds every catalog operation from the same activated repository bridge.
    #[allow(
        dead_code,
        reason = "WP-130 composition constructs this adapter after staged storage activation"
    )]
    pub(crate) fn new(storage: SharedRedbOperationalPorts, driver: &BlockingPortDriver) -> Self {
        let active_storage = storage.clone();
        let active = driver.executor(move |()| ActiveCatalogSnapshot::read(&active_storage));

        let version_storage = storage.clone();
        let contract_version = driver.executor(move |(lineage, version)| {
            read_contract_version(&version_storage, lineage, version)
        });

        let plan_storage = storage.clone();
        let executable_plan = driver.executor(move |request: CatalogExecutablePlanRequest| {
            let reference = ExecutablePlanRef::new(
                request.lineage().clone(),
                request.version(),
                request.bundle_hash(),
                request.command_id(),
                request.plan_hash(),
            );
            resolve_executable_plan(&plan_storage, &reference)
        });

        let deployment = driver.executor(
            move |(candidate, expected_active_version): DeploymentRequest| {
                let active = ActiveCatalogSnapshot::read(&storage)?;
                prepare_catalog_activation(candidate, expected_active_version, active.as_ref())
            },
        );

        Self {
            active,
            contract_version,
            executable_plan,
            deployment,
        }
    }
}

impl CatalogReadPort for ServerCatalogReadPort {
    fn prepare_active_catalog(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, Option<ActiveCatalogSnapshot>, CatalogError> {
        submit_catalog(self.active.reserve(control), ())
    }

    fn prepare_contract_version(
        &self,
        control: &RequestControl,
        lineage: ContractLineage,
        version: ContractVersion,
    ) -> PortFuture<'_, Option<ValidatedContractBundle>, CatalogError> {
        submit_catalog(self.contract_version.reserve(control), (lineage, version))
    }

    fn reserve_active_catalog(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<(), Option<ActiveCatalogSnapshot>, CatalogError>,
        PortAdmissionError,
    > {
        ready_port_reservation(self.active.reserve(control))
    }

    fn reserve_contract_version(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ContractVersionReadPermit, PortAdmissionError> {
        ready_port_reservation(self.contract_version.reserve(control))
    }

    fn executable_plan(
        &self,
        control: &RequestControl,
        request: CatalogExecutablePlanRequest,
    ) -> PortFuture<'_, ResolvedExecutablePlan, CatalogError> {
        submit_catalog(self.executable_plan.reserve(control), request)
    }

    fn prepare_deployment(
        &self,
        control: &RequestControl,
        candidate: ContractBundle,
        expected_active_version: Option<ContractVersion>,
    ) -> PortFuture<'_, CatalogPreparationResult, CatalogError> {
        submit_catalog(
            self.deployment.reserve(control),
            (candidate, expected_active_version),
        )
    }
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
    fn reserve_read_entity(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeEntityRequest,
            Option<AuthoritativeEntitySnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.entity.reserve(control))
    }

    fn reserve_scan_index(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeIndexRequest,
            AuthoritativeIndexPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.index.reserve(control))
    }

    fn reserve_read_outcome(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeOutcomeRequest,
            Option<AuthoritativeOutcomeSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.outcome.reserve(control))
    }

    fn reserve_read_commit(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            CommitSequence,
            Option<AuthoritativeCommitSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.commit.reserve(control))
    }

    fn reserve_scan_commits(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeCommitScanRequest,
            AuthoritativeCommitPage,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.commit_scan.reserve(control))
    }

    fn reserve_subscribe_to_commits(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            AuthoritativeCommitSubscriptionRequest,
            Box<dyn CommitNotificationSource>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.subscription.reserve(control))
    }

    fn reserve_trace_provenance(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            ProvenanceSelector,
            Option<AuthoritativeProvenanceSnapshot>,
            AuthoritativeReadError,
        >,
        PortAdmissionError,
    > {
        ready_port_reservation(self.provenance.reserve(control))
    }

    fn read_capability_revoke_target(
        &self,
        control: &RequestControl,
        capability_id: CapabilityId,
    ) -> PortFuture<'_, CapabilityRevokeTargetSnapshot, AuthoritativeReadError> {
        submit_authoritative(self.revoke_target.reserve(control), capability_id)
    }
}

impl fmt::Debug for ServerAuthoritativeReadPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerAuthoritativeReadPort([REDACTED])")
    }
}

fn ready_port_reservation<'a, Permit>(
    reservation: Result<Permit, PortAdmissionError>,
) -> PortFuture<'a, Permit, PortAdmissionError>
where
    Permit: Send + 'a,
{
    Box::pin(async move { reservation })
}

fn submit_catalog<'a, Request, Response>(
    reservation: Result<BoxPortCapacityPermit<Request, Response, CatalogError>, PortAdmissionError>,
    request: Request,
) -> PortFuture<'a, Response, CatalogError>
where
    Request: Send + 'a,
    Response: Send + 'a,
{
    Box::pin(async move {
        let permit = reservation.map_err(|_| catalog_driver_unavailable())?;
        let receipt = permit
            .submit(request)
            .map_err(|_| catalog_driver_unavailable())?;
        match receipt.completion().await {
            Ok(result) => result,
            Err(PortDriverStopped) => Err(catalog_driver_unavailable()),
        }
    })
}

fn submit_authoritative<'a, Request, Response>(
    reservation: Result<
        BoxPortCapacityPermit<Request, Response, AuthoritativeReadError>,
        PortAdmissionError,
    >,
    request: Request,
) -> PortFuture<'a, Response, AuthoritativeReadError>
where
    Request: Send + 'a,
    Response: Send + 'a,
{
    Box::pin(async move {
        let permit = reservation.map_err(map_authoritative_admission)?;
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
        PortAdmissionError::Cancelled
        | PortAdmissionError::DeadlineExceeded
        | PortAdmissionError::Unavailable => AuthoritativeReadError::Unavailable,
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
    let target = IndexRangeTarget::new(prefix);
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
        .mutations()
        .iter()
        .map(|mutation| {
            AffectedEntityView::new(
                mutation.post_image().target().key().clone(),
                mutation.post_image().entity_version(),
            )
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

        let page = scan_index(&storage, adapter_index_request(PartitionScopeV1::All, None))
            .expect("sparse adapter scan");

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
                        IndexPartitionFilterScope::All
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
}
