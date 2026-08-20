//! Shared pure-read handles for the activated redb port bundle.
//!
//! Mutation exclusion is the exclusive mutation gate, not handle uniqueness.
//! Readers obtained through [`RedbOperationalPorts::shared_ports`] never take a
//! process-wide lock around storage access and never expose `&mut self` ports.

use std::sync::Arc;

use riffdb_policy::{AuthorizedProjectedRowAdmissionV1, AuthorizedQueryRowPolicyContextV1};
use riffdb_query_executor::{
    QueryContinuation, QueryExecutionError, QueryExecutionPort, QueryExecutionRequest,
    QueryOwnedSnapshot, QueryParameters,
};
use riffdb_query_ir::QueryAccessProgramV1;
use riffdb_storage_api::{
    ActiveCatalogPointerV1, ActiveQueryModulePointerV1, AdmissionLookupResultV1,
    AdmissionRepository, AdmissionRequestV1, AdmissionResultV1, ApplicationCommandTransactionPort,
    AuditedAdmissionRepository, AuditedAdmissionRequestV1, AuditedAdmissionResultV1,
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, CapabilityAdministrationTransactionPort, CapabilityCreateCandidateV1,
    CapabilityInventoryPageV1, CapabilityInventoryReader, CapabilityLookupResult, CapabilityReader,
    CapabilityRevokeCandidateV1, CatalogRepository, CommitScanPageV1, CommitScanRequest,
    DeferredCommandEpochPort, EntityTarget, ExecutionFailureAdmissionResult,
    ExecutionFailureTransitionPort, ExecutionFailureTransitionRequestV1,
    FilteredAuthoritativeIndexScanPage, FilteredAuthoritativeIndexScanRequest,
    FilteredAuthoritativeScanReader, IdempotencyIdentity, IdempotencyLookupCandidatesV1,
    OutboxClaimV1, OutboxDeadLetterV1, OutboxPageLimit, OutboxRenewV1, OutboxRepository,
    OutboxRetryV1, OutboxStatusReadResultV1, OutboxSucceedV1, OutboxTransitionResultV1,
    PendingOutboxScanV1, ProjectionApplySnapshot, ProjectionApplySnapshotReader,
    ProjectionApplySnapshotRequest, ProjectionControlScanV1, ProjectionQueryReader,
    ProjectionQueryRequest, ProjectionQueryResult, ProjectionRecoveryPageLimit,
    ProjectionRecoveryRepository, ProjectionRecoveryValidationRequestV1,
    ProjectionRecoveryValidationResultV1, ProjectionStatus, QueryModuleRepository, ReadSnapshot,
    SnapshotReader, SnapshotRequest, StorageError, StorageErrorKind, StorageScanLimit,
    StoredCapabilityRecordV1, StoredCommitRecordV1, StoredContractBundleV1,
    StoredContractMigrationEdgeV1, StoredDurableEventV1, StoredEntityRecordV1, StoredOutcomeV1,
    StoredProvenanceRecordV1, StoredQueryModuleV1, UndeliveredOutboxStatusScanRequestV1,
    UndeliveredOutboxStatusScanV1,
};
use riffdb_types::{
    CapabilityId, CapabilityTokenDigest, CommitSequence, ContractBundleHash, ContractLineage,
    ContractVersion, EventId, ProjectionIdentity, ProvenanceId, QueryModuleHash,
};

use crate::store::{RedbOperationalPorts, SharedRedb};

/// Cloneable pure-read handle over one activated redb database.
///
/// Obtained only via [`RedbOperationalPorts::shared_ports`]. Implements every
/// `&self` semantic storage port by constructing a temporary
/// [`RedbOperationalPorts`] that shares the same underlying database. Mutation
/// exclusion remains the exclusive mutation gate, not handle uniqueness.
#[derive(Clone)]
pub struct RedbSharedPorts(Arc<SharedRedb>);

impl RedbSharedPorts {
    pub(crate) fn new(shared: Arc<SharedRedb>) -> Self {
        Self(shared)
    }

    /// Temporary operational view for trait delegation inside this crate only.
    ///
    /// Not public: callers outside this crate must use the `&self` port traits
    /// implemented on [`RedbSharedPorts`]. Concurrent writers still serialize on
    /// the mutation gate, not handle uniqueness.
    #[must_use]
    pub(crate) fn operational(&self) -> RedbOperationalPorts {
        RedbOperationalPorts {
            shared: Arc::clone(&self.0),
        }
    }

    /// Loads every active query-module pointer and its matching module body.
    ///
    /// Used by the process view at construction so idempotent re-activation after
    /// restart does not integrity-fail on a cold empty view.
    pub fn load_active_query_modules(
        &self,
    ) -> Result<Vec<(ActiveQueryModulePointerV1, StoredQueryModuleV1)>, StorageError> {
        crate::administration::load_active_query_modules(&self.operational())
    }
}

impl std::fmt::Debug for RedbSharedPorts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RedbSharedPorts([SHARED_READ])")
    }
}

impl QueryExecutionPort for RedbSharedPorts {
    fn execute_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        QueryExecutionPort::execute_query_page(&self.operational(), program, parameters, prior)
    }

    fn execute_query_group(
        &self,
        requests: &[QueryExecutionRequest<'_>],
    ) -> Result<Vec<QueryOwnedSnapshot>, QueryExecutionError> {
        QueryExecutionPort::execute_query_group(&self.operational(), requests)
    }

    fn execute_operational_query_page(
        &self,
        program: &riffdb_query_ir::QueryAccessProgramV1,
        aggregates: &[riffdb_query_ir::OperationalAggregateV1],
        parameters: &riffdb_query_executor::QueryParameters,
        prior: Option<&riffdb_query_executor::QueryContinuation>,
    ) -> Result<riffdb_query_executor::QueryOwnedSnapshot, riffdb_query_executor::QueryExecutionError>
    {
        QueryExecutionPort::execute_operational_query_page(
            &self.operational(),
            program,
            aggregates,
            parameters,
            prior,
        )
    }

    fn execute_policy_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        QueryExecutionPort::execute_policy_query_page(
            &self.operational(),
            program,
            parameters,
            prior,
            policy,
        )
    }

    fn execute_policy_operational_query_page(
        &self,
        program: &riffdb_query_ir::QueryAccessProgramV1,
        aggregates: &[riffdb_query_ir::OperationalAggregateV1],
        parameters: &riffdb_query_executor::QueryParameters,
        prior: Option<&riffdb_query_executor::QueryContinuation>,
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<riffdb_query_executor::QueryOwnedSnapshot, riffdb_query_executor::QueryExecutionError>
    {
        QueryExecutionPort::execute_policy_operational_query_page(
            &self.operational(),
            program,
            aggregates,
            parameters,
            prior,
            policy,
        )
    }

    fn authorize_projected_candidates(
        &self,
        entity: riffdb_types::EntityTypeId,
        candidates: &[riffdb_types::EntityKey],
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<AuthorizedProjectedRowAdmissionV1, QueryExecutionError> {
        QueryExecutionPort::authorize_projected_candidates(
            &self.operational(),
            entity,
            candidates,
            policy,
        )
    }
}

impl AdmissionRepository for RedbSharedPorts {
    fn admit_or_resolve(
        &self,
        request: AdmissionRequestV1,
    ) -> Result<AdmissionResultV1, StorageError> {
        AdmissionRepository::admit_or_resolve(&self.operational(), request)
    }

    fn admit_or_resolve_group(
        &self,
        requests: Vec<AdmissionRequestV1>,
    ) -> Result<Vec<AdmissionResultV1>, StorageError> {
        AdmissionRepository::admit_or_resolve_group(&self.operational(), requests)
    }

    fn lookup_admission(
        &self,
        candidates: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        AdmissionRepository::lookup_admission(&self.operational(), candidates)
    }

    fn lookup_admission_group(
        &self,
        candidates: Vec<IdempotencyLookupCandidatesV1>,
    ) -> Result<Vec<AdmissionLookupResultV1>, StorageError> {
        AdmissionRepository::lookup_admission_group(&self.operational(), candidates)
    }
}

impl AuditedAdmissionRepository for RedbSharedPorts {
    fn admit_or_resolve_audited_group(
        &self,
        requests: Vec<AuditedAdmissionRequestV1>,
    ) -> Result<Vec<AuditedAdmissionResultV1>, StorageError> {
        AuditedAdmissionRepository::admit_or_resolve_audited_group(&self.operational(), requests)
    }
}

impl SnapshotReader for RedbSharedPorts {
    fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
        SnapshotReader::read_snapshot(&self.operational(), request)
    }

    fn read_snapshot_group(
        &self,
        requests: Vec<SnapshotRequest>,
    ) -> Result<Vec<ReadSnapshot>, StorageError> {
        SnapshotReader::read_snapshot_group(&self.operational(), requests)
    }
}

impl ApplicationCommandTransactionPort for RedbSharedPorts {
    type EmptyBatch = <RedbOperationalPorts as ApplicationCommandTransactionPort>::EmptyBatch;

    fn begin_empty_batch(&self) -> Result<Self::EmptyBatch, StorageError> {
        ApplicationCommandTransactionPort::begin_empty_batch(&self.operational())
    }
}

impl DeferredCommandEpochPort for RedbSharedPorts {
    type Epoch = <RedbOperationalPorts as DeferredCommandEpochPort>::Epoch;

    fn begin_deferred_command_epoch(&self) -> Result<Self::Epoch, StorageError> {
        DeferredCommandEpochPort::begin_deferred_command_epoch(&self.operational())
    }
}

impl ExecutionFailureTransitionPort for RedbSharedPorts {
    type Rechecked = <RedbOperationalPorts as ExecutionFailureTransitionPort>::Rechecked;

    fn begin_execution_failure(
        &self,
        request: ExecutionFailureTransitionRequestV1,
    ) -> Result<ExecutionFailureAdmissionResult<Self::Rechecked>, StorageError> {
        ExecutionFailureTransitionPort::begin_execution_failure(&self.operational(), request)
    }
}

impl CatalogRepository for RedbSharedPorts {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        CatalogRepository::read_active_catalog(&self.operational())
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        CatalogRepository::read_contract_bundle(&self.operational(), lineage, contract_version)
    }

    fn read_contract_migration_edge(
        &self,
        predecessor: ContractBundleHash,
    ) -> Result<Option<StoredContractMigrationEdgeV1>, StorageError> {
        CatalogRepository::read_contract_migration_edge(&self.operational(), predecessor)
    }
}

impl QueryModuleRepository for RedbSharedPorts {
    fn read_query_module(
        &self,
        module_hash: QueryModuleHash,
    ) -> Result<Option<StoredQueryModuleV1>, StorageError> {
        QueryModuleRepository::read_query_module(&self.operational(), module_hash)
    }

    fn read_active_query_module(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
    ) -> Result<Option<ActiveQueryModulePointerV1>, StorageError> {
        QueryModuleRepository::read_active_query_module(
            &self.operational(),
            lineage,
            contract_version,
            contract_bundle_hash,
        )
    }
}

impl CapabilityReader for RedbSharedPorts {
    fn read_capability(
        &self,
        capability_id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
        CapabilityReader::read_capability(&self.operational(), capability_id)
    }

    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        CapabilityReader::resolve_capability_digests(&self.operational(), candidates)
    }
}

impl CapabilityInventoryReader for RedbSharedPorts {
    fn scan_capabilities(
        &self,
        after: Option<CapabilityId>,
        limit: StorageScanLimit,
    ) -> Result<CapabilityInventoryPageV1, StorageError> {
        CapabilityInventoryReader::scan_capabilities(&self.operational(), after, limit)
    }
}

impl CapabilityAdministrationTransactionPort for RedbSharedPorts {
    type CreateCandidate =
        <RedbOperationalPorts as CapabilityAdministrationTransactionPort>::CreateCandidate;
    type RevokeCandidate =
        <RedbOperationalPorts as CapabilityAdministrationTransactionPort>::RevokeCandidate;

    fn begin_capability_create(
        &self,
        candidate: CapabilityCreateCandidateV1,
    ) -> Result<Self::CreateCandidate, StorageError> {
        CapabilityAdministrationTransactionPort::begin_capability_create(
            &self.operational(),
            candidate,
        )
    }

    fn begin_capability_revoke(
        &self,
        candidate: CapabilityRevokeCandidateV1,
    ) -> Result<Self::RevokeCandidate, StorageError> {
        CapabilityAdministrationTransactionPort::begin_capability_revoke(
            &self.operational(),
            candidate,
        )
    }
}

impl AuthoritativePointReader for RedbSharedPorts {
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        AuthoritativePointReader::read_entity(&self.operational(), target)
    }

    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        AuthoritativePointReader::read_stored_outcome(&self.operational(), identity)
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        AuthoritativePointReader::read_commit(&self.operational(), sequence)
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        AuthoritativePointReader::read_provenance(&self.operational(), provenance_id)
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        AuthoritativePointReader::read_durable_event(&self.operational(), event_id)
    }
}

impl AuthoritativeScanReader for RedbSharedPorts {
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        AuthoritativeScanReader::scan_index(&self.operational(), request)
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        AuthoritativeScanReader::scan_commits(&self.operational(), request)
    }
}

impl riffdb_storage_api::PartitionEventRouteReader for RedbSharedPorts {
    fn scan_partition_event_routes(
        &self,
        request: riffdb_storage_api::EventRouteScanRequestV1,
    ) -> Result<riffdb_storage_api::EventRouteScanV1, StorageError> {
        riffdb_storage_api::PartitionEventRouteReader::scan_partition_event_routes(
            &self.operational(),
            request,
        )
    }
}

impl FilteredAuthoritativeScanReader for RedbSharedPorts {
    fn scan_index_filtered(
        &self,
        request: FilteredAuthoritativeIndexScanRequest,
    ) -> Result<FilteredAuthoritativeIndexScanPage, StorageError> {
        FilteredAuthoritativeScanReader::scan_index_filtered(&self.operational(), request)
    }
}

impl ProjectionApplySnapshotReader for RedbSharedPorts {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        ProjectionApplySnapshotReader::read_apply_snapshot(&self.operational(), request)
    }
}

impl ProjectionQueryReader for RedbSharedPorts {
    fn query_projection(
        &self,
        request: &ProjectionQueryRequest,
    ) -> Result<ProjectionQueryResult, StorageError> {
        ProjectionQueryReader::query_projection(&self.operational(), request)
    }

    fn read_projection_status(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError> {
        ProjectionQueryReader::read_projection_status(&self.operational(), identity)
    }
}

impl ProjectionRecoveryRepository for RedbSharedPorts {
    fn scan_projection_controls(
        &self,
        after: Option<&ProjectionIdentity>,
        limit: ProjectionRecoveryPageLimit,
    ) -> Result<ProjectionControlScanV1, StorageError> {
        ProjectionRecoveryRepository::scan_projection_controls(&self.operational(), after, limit)
    }

    fn validate_projection_recovery_page(
        &self,
        request: &ProjectionRecoveryValidationRequestV1,
    ) -> Result<ProjectionRecoveryValidationResultV1, StorageError> {
        ProjectionRecoveryRepository::validate_projection_recovery_page(
            &self.operational(),
            request,
        )
    }
}

impl OutboxRepository for RedbSharedPorts {
    fn has_undelivered_outbox(&self) -> Result<bool, StorageError> {
        OutboxRepository::has_undelivered_outbox(&self.operational())
    }

    fn read_outbox_status(
        &self,
        event_id: EventId,
    ) -> Result<OutboxStatusReadResultV1, StorageError> {
        OutboxRepository::read_outbox_status(&self.operational(), event_id)
    }

    fn scan_pending_outbox(
        &self,
        after: Option<EventId>,
        limit: OutboxPageLimit,
    ) -> Result<PendingOutboxScanV1, StorageError> {
        OutboxRepository::scan_pending_outbox(&self.operational(), after, limit)
    }

    fn scan_undelivered_outbox_statuses(
        &self,
        request: UndeliveredOutboxStatusScanRequestV1,
    ) -> Result<UndeliveredOutboxStatusScanV1, StorageError> {
        OutboxRepository::scan_undelivered_outbox_statuses(&self.operational(), request)
    }

    fn claim_outbox(
        &mut self,
        _transition: &OutboxClaimV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        Err(shared_ports_mutation_denied())
    }

    fn renew_outbox(
        &mut self,
        _transition: &OutboxRenewV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        Err(shared_ports_mutation_denied())
    }

    fn succeed_outbox(
        &mut self,
        _transition: &OutboxSucceedV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        Err(shared_ports_mutation_denied())
    }

    fn retry_outbox(
        &mut self,
        _transition: &OutboxRetryV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        Err(shared_ports_mutation_denied())
    }

    fn dead_letter_outbox(
        &mut self,
        _transition: &OutboxDeadLetterV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        Err(shared_ports_mutation_denied())
    }
}

fn shared_ports_mutation_denied() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}
