//! Private worker-only cancellation boundary over existing bounded ports.

use super::StopState;
use riffdb_storage_api as api;
use riffdb_types::{ContractBundleHash, ContractLineage, ContractVersion, ProjectionIdentity};
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[derive(Clone)]
pub(super) struct ProjectionWorkerRepository<R> {
    inner: R,
    stop: Arc<StopState>,
}

impl<R> ProjectionWorkerRepository<R> {
    pub(super) fn new(inner: R, stop: Arc<StopState>) -> Self {
        Self { inner, stop }
    }

    fn before_call(&self) -> Result<(), api::StorageError> {
        let requested =
            self.stop.requested.lock().map_err(|_| {
                api::StorageError::new(api::StorageErrorKind::InvariantViolation, None)
            })?;
        if *requested {
            self.stop.interrupted.store(true, Ordering::Release);
            return Err(api::StorageError::new(
                api::StorageErrorKind::Unavailable,
                None,
            ));
        }
        Ok(())
    }
}

impl<R: api::CatalogRepository> api::CatalogRepository for ProjectionWorkerRepository<R> {
    fn read_active_catalog(
        &self,
    ) -> Result<Option<api::ActiveCatalogPointerV1>, api::StorageError> {
        self.before_call()?;
        self.inner.read_active_catalog()
    }
    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        version: ContractVersion,
    ) -> Result<Option<api::StoredContractBundleV1>, api::StorageError> {
        self.before_call()?;
        self.inner.read_contract_bundle(lineage, version)
    }
    fn read_contract_migration_edge(
        &self,
        predecessor: ContractBundleHash,
    ) -> Result<Option<api::StoredContractMigrationEdgeV1>, api::StorageError> {
        self.before_call()?;
        self.inner.read_contract_migration_edge(predecessor)
    }
}

impl<R: api::AuthoritativeScanReader> api::AuthoritativeScanReader
    for ProjectionWorkerRepository<R>
{
    fn scan_entity_partition(
        &self,
        request: api::AuthoritativeEntityPartitionScanRequest,
    ) -> Result<api::AuthoritativeEntityPartitionScanPage, api::StorageError> {
        self.before_call()?;
        self.inner.scan_entity_partition(request)
    }
    fn scan_index(
        &self,
        request: api::AuthoritativeIndexScanRequest,
    ) -> Result<api::AuthoritativeIndexScanPage, api::StorageError> {
        self.before_call()?;
        self.inner.scan_index(request)
    }
    fn scan_commits(
        &self,
        request: api::CommitScanRequest,
    ) -> Result<api::CommitScanPageV1, api::StorageError> {
        self.before_call()?;
        self.inner.scan_commits(request)
    }
}

impl<R: api::ProjectionApplySnapshotReader> api::ProjectionApplySnapshotReader
    for ProjectionWorkerRepository<R>
{
    fn capture_apply_batch_snapshot(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<Box<dyn api::ProjectionBatchSnapshot>, api::StorageError> {
        self.before_call()?;
        self.inner.capture_apply_batch_snapshot(identity)
    }
    fn read_apply_snapshot(
        &self,
        request: &api::ProjectionApplySnapshotRequest,
    ) -> Result<api::ProjectionApplySnapshot, api::StorageError> {
        self.before_call()?;
        self.inner.read_apply_snapshot(request)
    }
}

impl<R: api::ProjectionMutationRepository> api::ProjectionMutationRepository
    for ProjectionWorkerRepository<R>
{
    fn resolve_projection_batch(
        &self,
        request: &api::ProjectionApplyBatchV1,
    ) -> Result<api::ProjectionApplyBatchResult, api::StorageError> {
        self.before_call()?;
        self.inner.resolve_projection_batch(request)
    }
    fn apply_projection_batch(
        &mut self,
        request: &api::ProjectionApplyBatchV1,
    ) -> Result<api::ProjectionApplyBatchResult, api::StorageError> {
        self.before_call()?;
        self.inner.apply_projection_batch(request)
    }
    fn apply_projection(
        &mut self,
        request: &api::ProjectionApplyRequestV1,
    ) -> Result<api::ProjectionApplyResult, api::StorageError> {
        self.before_call()?;
        self.inner.apply_projection(request)
    }
    fn transition_projection_control(
        &mut self,
        operation: api::ProjectionControlOperation,
    ) -> Result<api::ProjectionControlResult, api::StorageError> {
        self.before_call()?;
        self.inner.transition_projection_control(operation)
    }
}

impl<R: api::ProjectionQueryReader> api::ProjectionQueryReader for ProjectionWorkerRepository<R> {
    fn query_projection(
        &self,
        request: &api::ProjectionQueryRequest,
    ) -> Result<api::ProjectionQueryResult, api::StorageError> {
        self.before_call()?;
        self.inner.query_projection(request)
    }
    fn read_projection_status(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<api::ProjectionStatus, api::StorageError> {
        self.before_call()?;
        self.inner.read_projection_status(identity)
    }
}

impl<R: api::ProjectionRecoveryRepository> api::ProjectionRecoveryRepository
    for ProjectionWorkerRepository<R>
{
    fn scan_projection_controls(
        &self,
        after: Option<&ProjectionIdentity>,
        limit: api::ProjectionRecoveryPageLimit,
    ) -> Result<api::ProjectionControlScanV1, api::StorageError> {
        self.before_call()?;
        self.inner.scan_projection_controls(after, limit)
    }
    fn validate_projection_recovery_page(
        &self,
        request: &api::ProjectionRecoveryValidationRequestV1,
    ) -> Result<api::ProjectionRecoveryValidationResultV1, api::StorageError> {
        self.before_call()?;
        self.inner.validate_projection_recovery_page(request)
    }
}
