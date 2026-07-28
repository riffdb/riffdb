//! Private sharing bridge for the one activated production redb port bundle.

use std::fmt;
use std::sync::{Arc, Mutex};

use riffdb_storage_api::{
    ActiveCatalogPointerV1, AdmissionLookupResultV1, AdmissionRepository, AdmissionRequestV1,
    AdmissionResultV1, ApplicationCommandTransactionPort, AuthoritativeIndexScanPage,
    AuthoritativeIndexScanRequest, AuthoritativePointReader, AuthoritativeScanReader,
    CapabilityAdministrationTransactionPort, CapabilityBootstrapAdministrationRepository,
    CapabilityBootstrapIntentV1, CapabilityBootstrapResult, CapabilityCreateCandidateV1,
    CapabilityLookupResult, CapabilityReader, CapabilityRevokeCandidateV1,
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
    ReadSnapshot, ServiceAuditAppendIntentV1, ServiceAuditAppendRepository,
    ServiceAuditAppendResult, SnapshotReader, SnapshotRequest, StorageError, StorageErrorKind,
    StoredCommitRecordV1, StoredContractBundleV1, StoredDurableEventV1, StoredEntityRecordV1,
    StoredOutcomeV1, StoredProvenanceRecordV1, UndeliveredOutboxStatusScanRequestV1,
    UndeliveredOutboxStatusScanV1,
};
use riffdb_storage_redb::RedbOperationalPorts;
use riffdb_types::{
    CapabilityId, CapabilityTokenDigest, CommitSequence, ContractLineage, ContractVersion, EventId,
    ProvenanceId,
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
}

impl SharedRedbOperationalPorts {
    /// Wraps the exact activated port bundle released by staged startup.
    #[allow(
        dead_code,
        reason = "WP-130 process composition constructs this private bridge after staged startup"
    )]
    pub(crate) fn new(ports: RedbOperationalPorts) -> Self {
        Self {
            cell: SharedStorageCell::new(ports),
        }
    }
}

impl Clone for SharedRedbOperationalPorts {
    fn clone(&self) -> Self {
        Self {
            cell: self.cell.clone(),
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
    inner: Arc<Mutex<T>>,
}

#[allow(
    dead_code,
    reason = "used through the WP-130 private bridge once the hosted process graph is constructed"
)]
impl<T> SharedStorageCell<T> {
    fn new(value: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(value)),
        }
    }

    fn with_ref<R>(
        &self,
        operation: impl FnOnce(&T) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        let guard = self.inner.lock().map_err(|_| poisoned_storage_bridge())?;
        operation(&guard)
    }

    fn with_mut<R>(
        &self,
        operation: impl FnOnce(&mut T) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        let mut guard = self.inner.lock().map_err(|_| poisoned_storage_bridge())?;
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

impl AdmissionRepository for SharedRedbOperationalPorts {
    fn admit_or_resolve(
        &self,
        request: AdmissionRequestV1,
    ) -> Result<AdmissionResultV1, StorageError> {
        self.cell
            .with_ref(|ports| AdmissionRepository::admit_or_resolve(ports, request))
    }

    fn lookup_admission(
        &self,
        candidates: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        self.cell
            .with_ref(|ports| AdmissionRepository::lookup_admission(ports, candidates))
    }
}

impl SnapshotReader for SharedRedbOperationalPorts {
    fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
        self.cell
            .with_ref(|ports| SnapshotReader::read_snapshot(ports, request))
    }
}

impl ApplicationCommandTransactionPort for SharedRedbOperationalPorts {
    type EmptyBatch = <RedbOperationalPorts as ApplicationCommandTransactionPort>::EmptyBatch;

    fn begin_empty_batch(&self) -> Result<Self::EmptyBatch, StorageError> {
        self.cell
            .with_ref(ApplicationCommandTransactionPort::begin_empty_batch)
    }
}

impl ExecutionFailureTransitionPort for SharedRedbOperationalPorts {
    type Rechecked = <RedbOperationalPorts as ExecutionFailureTransitionPort>::Rechecked;

    fn begin_execution_failure(
        &self,
        request: ExecutionFailureTransitionRequestV1,
    ) -> Result<ExecutionFailureAdmissionResult<Self::Rechecked>, StorageError> {
        self.cell.with_ref(|ports| {
            ExecutionFailureTransitionPort::begin_execution_failure(ports, request)
        })
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
}

impl CatalogAdministrationRepository for SharedRedbOperationalPorts {
    fn activate_catalog(
        &mut self,
        intent: &CatalogActivationIntentV1,
    ) -> Result<CatalogActivationResult, StorageError> {
        self.cell
            .with_mut(|ports| CatalogAdministrationRepository::activate_catalog(ports, intent))
    }
}

impl CapabilityAdministrationTransactionPort for SharedRedbOperationalPorts {
    type CreateCandidate =
        <RedbOperationalPorts as CapabilityAdministrationTransactionPort>::CreateCandidate;
    type RevokeCandidate =
        <RedbOperationalPorts as CapabilityAdministrationTransactionPort>::RevokeCandidate;

    fn begin_capability_create(
        &self,
        candidate: CapabilityCreateCandidateV1,
    ) -> Result<Self::CreateCandidate, StorageError> {
        self.cell.with_ref(|ports| {
            CapabilityAdministrationTransactionPort::begin_capability_create(ports, candidate)
        })
    }

    fn begin_capability_revoke(
        &self,
        candidate: CapabilityRevokeCandidateV1,
    ) -> Result<Self::RevokeCandidate, StorageError> {
        self.cell.with_ref(|ports| {
            CapabilityAdministrationTransactionPort::begin_capability_revoke(ports, candidate)
        })
    }
}

impl CapabilityBootstrapAdministrationRepository for SharedRedbOperationalPorts {
    fn bootstrap_capability(
        &mut self,
        intent: &CapabilityBootstrapIntentV1,
    ) -> Result<CapabilityBootstrapResult, StorageError> {
        self.cell.with_mut(|ports| {
            CapabilityBootstrapAdministrationRepository::bootstrap_capability(ports, intent)
        })
    }
}

impl CatalogRepository for SharedRedbOperationalPorts {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        self.cell.with_ref(CatalogRepository::read_active_catalog)
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        self.cell.with_ref(|ports| {
            CatalogRepository::read_contract_bundle(ports, lineage, contract_version)
        })
    }
}

impl CapabilityReader for SharedRedbOperationalPorts {
    fn read_capability(
        &self,
        capability_id: CapabilityId,
    ) -> Result<Option<riffdb_storage_api::StoredCapabilityRecordV1>, StorageError> {
        self.cell
            .with_ref(|ports| CapabilityReader::read_capability(ports, capability_id))
    }

    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        self.cell
            .with_ref(|ports| CapabilityReader::resolve_capability_digests(ports, candidates))
    }
}

impl AuthoritativePointReader for SharedRedbOperationalPorts {
    fn read_entity(
        &self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        self.cell
            .with_ref(|ports| AuthoritativePointReader::read_entity(ports, target))
    }

    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        self.cell
            .with_ref(|ports| AuthoritativePointReader::read_stored_outcome(ports, identity))
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        self.cell
            .with_ref(|ports| AuthoritativePointReader::read_commit(ports, sequence))
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        self.cell
            .with_ref(|ports| AuthoritativePointReader::read_provenance(ports, provenance_id))
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        self.cell
            .with_ref(|ports| AuthoritativePointReader::read_durable_event(ports, event_id))
    }
}

impl AuthoritativeScanReader for SharedRedbOperationalPorts {
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        self.cell
            .with_ref(|ports| AuthoritativeScanReader::scan_index(ports, request))
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        self.cell
            .with_ref(|ports| AuthoritativeScanReader::scan_commits(ports, request))
    }
}

impl FilteredAuthoritativeScanReader for SharedRedbOperationalPorts {
    fn scan_index_filtered(
        &self,
        request: FilteredAuthoritativeIndexScanRequest,
    ) -> Result<FilteredAuthoritativeIndexScanPage, StorageError> {
        self.cell
            .with_ref(|ports| FilteredAuthoritativeScanReader::scan_index_filtered(ports, request))
    }
}

impl OutboxRepository for SharedRedbOperationalPorts {
    fn read_outbox_status(
        &self,
        event_id: EventId,
    ) -> Result<OutboxStatusReadResultV1, StorageError> {
        self.cell
            .with_ref(|ports| OutboxRepository::read_outbox_status(ports, event_id))
    }

    fn scan_pending_outbox(
        &self,
        after: Option<EventId>,
        limit: OutboxPageLimit,
    ) -> Result<PendingOutboxScanV1, StorageError> {
        self.cell
            .with_ref(|ports| OutboxRepository::scan_pending_outbox(ports, after, limit))
    }

    fn scan_undelivered_outbox_statuses(
        &self,
        request: UndeliveredOutboxStatusScanRequestV1,
    ) -> Result<UndeliveredOutboxStatusScanV1, StorageError> {
        self.cell
            .with_ref(|ports| OutboxRepository::scan_undelivered_outbox_statuses(ports, request))
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
        self.cell
            .with_ref(|ports| ProjectionApplySnapshotReader::read_apply_snapshot(ports, request))
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
        self.cell
            .with_ref(|ports| ProjectionQueryReader::query_projection(ports, request))
    }

    fn read_projection_status(
        &self,
        identity: &riffdb_types::ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError> {
        self.cell
            .with_ref(|ports| ProjectionQueryReader::read_projection_status(ports, identity))
    }
}

impl ProjectionRecoveryRepository for SharedRedbOperationalPorts {
    fn scan_projection_controls(
        &self,
        after: Option<&riffdb_types::ProjectionIdentity>,
        limit: ProjectionRecoveryPageLimit,
    ) -> Result<ProjectionControlScanV1, StorageError> {
        self.cell.with_ref(|ports| {
            ProjectionRecoveryRepository::scan_projection_controls(ports, after, limit)
        })
    }

    fn validate_projection_recovery_page(
        &self,
        request: &ProjectionRecoveryValidationRequestV1,
    ) -> Result<ProjectionRecoveryValidationResultV1, StorageError> {
        self.cell.with_ref(|ports| {
            ProjectionRecoveryRepository::validate_projection_recovery_page(ports, request)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use super::*;

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

        assert_eq!(second.with_ref(|value| Ok(*value)).expect("read clone"), 7);
    }

    #[test]
    fn cloned_cells_serialize_calls_without_sleeps() {
        const WORKERS: usize = 8;

        let cell = SharedStorageCell::new(());
        let start = Arc::new(Barrier::new(WORKERS + 1));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::with_capacity(WORKERS);

        for _ in 0..WORKERS {
            let cell = cell.clone();
            let start = Arc::clone(&start);
            let active = Arc::clone(&active);
            let maximum = Arc::clone(&maximum);
            workers.push(thread::spawn(move || {
                start.wait();
                cell.with_ref(|()| {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(now, Ordering::SeqCst);
                    thread::yield_now();
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
                .expect("serialized operation");
            }));
        }

        start.wait();
        for worker in workers {
            worker.join().expect("worker completed");
        }

        assert_eq!(maximum.load(Ordering::SeqCst), 1);
        assert_eq!(active.load(Ordering::SeqCst), 0);
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
            .with_ref(|_| {
                Ok(OwnedTransactionState {
                    bridge: bridge.clone(),
                })
            })
            .expect("begin owned state");

        assert!(
            bridge.inner.try_lock().is_ok(),
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

    #[test]
    fn poison_fails_closed_as_an_invariant_violation() {
        let cell = SharedStorageCell::new(());
        let poisoner = cell.clone();

        let result = thread::spawn(move || {
            let _guard = poisoner.inner.lock().expect("initial lock");
            panic!("poison test cell");
        })
        .join();
        assert!(result.is_err());

        let error = cell
            .with_ref(|()| Ok(()))
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
                + CapabilityAdministrationTransactionPort
                + CapabilityBootstrapAdministrationRepository
                + CatalogRepository
                + CapabilityReader
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
