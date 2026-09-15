//! Restricted pre-startup phase of the sole follower writer. Historical proof
//! permits only derived replay; complete ordinary startup still owns activation.
use super::*;
use riffdb_storage_api::{
    AuthoritativeEntityPartitionScanPage, AuthoritativeEntityPartitionScanRequest,
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativeScanReader,
    CommitScanPageV1, CommitScanRequest, EntityTarget, EvidencePageLimit, HistoricalBundleEvidence,
    HistoricalEvidenceCursor, HistoricalEvidencePage, OpenSessionId, ProjectionApplyRequestV1,
    ProjectionApplySnapshot, ProjectionApplySnapshotReader, ProjectionApplySnapshotRequest,
    ProjectionControlScanV1, ProjectionRecoveryPageLimit, ProjectionRecoveryRepository,
    ProjectionRecoveryValidationRequestV1, ProjectionRecoveryValidationResultV1,
    StoredEntityRecordV1, StoredProjectionApplyV1, StoredProjectionControlV1,
    StructuralEvidenceCursor, StructuralEvidencePage, StructuralEvidenceSession,
    StructuralOpenOutcome, UniqueIndexTarget, UniqueOccupancyKind,
};
use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, ProjectionIdentity,
};

/// Historical-only evidence under retained engine and namespace exclusion.
/// No structural completion or application-facing writer can be obtained here.
pub struct RedbFollowerRecoveryCatalogSession {
    session: crate::RedbStructuralEvidenceSession,
    shared: Arc<SharedRedb>,
    cancellation: Arc<AtomicBool>,
}

/// Derived-only phase of the follower applier. It cannot apply frames, acknowledge
/// positions, mutate source controls, or release serving ports. The engine stays
/// locked until this owner is consumed by the ordinary startup evidence pass.
pub struct RedbFollowerProjectionRecovery {
    applier: RedbFollowerApplier,
    catalog_session: OpenSessionId,
    cancellation: Arc<AtomicBool>,
}

impl RedbFollowerStore {
    /// Starts historical validation before restoring lagging local projections.
    /// Neither this preflight nor its derived writer grants startup readiness.
    pub fn begin_projection_recovery(
        self,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<RedbFollowerRecoveryCatalogSession, StorageError> {
        if cancellation.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let shared = Arc::clone(&self.0.shared);
        let mut session = self.begin_offline_bootstrap_scrub(inputs)?;
        session.set_cancellation(Arc::clone(&cancellation));
        Ok(RedbFollowerRecoveryCatalogSession {
            session,
            shared,
            cancellation,
        })
    }
}
impl RedbFollowerRecoveryCatalogSession {
    fn check_cancel(&self) -> Result<(), StorageError> {
        if self.cancellation.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(())
    }

    /// Consumes exact historical EOF. Server composition must retain and match
    /// the catalog-owned proof from this same session before evaluating rows.
    pub fn finish_preflight(
        self,
        end: crate::RedbHistoricalEvidenceEnd,
    ) -> Result<RedbFollowerProjectionRecovery, StorageError> {
        self.check_cancel()?;
        let catalog_session = self.session.open_session_id();
        self.session.finish_bootstrap_catalog_preflight(end)?;
        // The Arc retained above keeps the original engine/namespace lock held.
        // This internal applier is never exposed; only its derived ports follow.
        Ok(RedbFollowerProjectionRecovery {
            applier: RedbFollowerApplier::from_validated_shared(self.shared)?,
            catalog_session,
            cancellation: self.cancellation,
        })
    }
}
impl RedbFollowerProjectionRecovery {
    fn check_live(&self) -> Result<(), StorageError> {
        self.applier.ensure_live()?;
        if self.cancellation.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(())
    }
    /// Process-local binding to the historical evidence, never a readiness proof.
    pub const fn catalog_validation_session(&self) -> OpenSessionId {
        self.catalog_session
    }

    /// Exact source history read under this exclusive owner; it is never advanced.
    pub fn durable_history(&self) -> Result<ChangelogHistoryStateV3, StorageError> {
        self.check_live()?;
        self.applier.durable_history()
    }

    /// Reuses the sole applier's bounded derived-only transaction boundary.
    pub fn rebuild_projection_commit(
        &mut self,
        expected: &StoredProjectionControlV1,
        request: &ProjectionApplyRequestV1,
    ) -> Result<StoredProjectionApplyV1, StorageError> {
        self.check_live()?;
        let marker = self.applier.rebuild_projection_commit(expected, request)?;
        self.check_live()?;
        Ok(marker)
    }

    /// Transfers the same locked engine into a dormant follower store. The caller
    /// must now run complete unchanged structural and catalog startup validation.
    pub fn finish_rebuild(self) -> Result<RedbFollowerStore, StorageError> {
        self.check_live()?;
        Ok(RedbFollowerStore(RedbStore {
            shared: self.applier.shared,
        }))
    }
}
impl StructuralEvidenceSession for RedbFollowerRecoveryCatalogSession {
    type DormantPorts = crate::RedbDormantPorts;
    type StructuralEnd = crate::RedbStructuralEvidenceEnd;
    type HistoricalEnd = crate::RedbHistoricalEvidenceEnd;
    type MigrationPort = crate::RedbStartupIndexMigrationPort;
    fn database_id(&self) -> DatabaseId {
        self.session.database_id()
    }
    fn open_session_id(&self) -> OpenSessionId {
        self.session.open_session_id()
    }
    fn read_structural_evidence(
        &mut self,
        _cursor: StructuralEvidenceCursor,
        _limit: EvidencePageLimit,
    ) -> Result<StructuralEvidencePage<Self::StructuralEnd>, StorageError> {
        Err(storage_error(StorageErrorKind::CorruptData))
    }
    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
        self.check_cancel()?;
        self.session.read_historical_evidence(cursor, limit)
    }
    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        version: ContractVersion,
        hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        self.check_cancel()?;
        self.session.read_historical_bundle(lineage, version, hash)
    }
    fn read_integrity_entity(
        &mut self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        self.check_cancel()?;
        self.session.read_integrity_entity(target)
    }
    fn read_integrity_unique_occupancy(
        &mut self,
        target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError> {
        self.check_cancel()?;
        self.session.read_integrity_unique_occupancy(target)
    }
    fn finish(
        self,
        _structural: Self::StructuralEnd,
        _historical: Self::HistoricalEnd,
    ) -> Result<StructuralOpenOutcome<Self::DormantPorts, Self::MigrationPort>, StorageError> {
        Err(storage_error(StorageErrorKind::CorruptData))
    }
}

impl ProjectionApplySnapshotReader for RedbFollowerProjectionRecovery {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        self.check_live()?;
        self.applier.read_apply_snapshot(request)
    }
}
impl AuthoritativeScanReader for RedbFollowerProjectionRecovery {
    fn scan_entity_partition(
        &self,
        request: AuthoritativeEntityPartitionScanRequest,
    ) -> Result<AuthoritativeEntityPartitionScanPage, StorageError> {
        self.check_live()?;
        self.applier.scan_entity_partition(request)
    }
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        self.check_live()?;
        self.applier.scan_index(request)
    }
    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        self.check_live()?;
        self.applier.scan_commits(request)
    }
}
impl ProjectionRecoveryRepository for RedbFollowerProjectionRecovery {
    fn scan_projection_controls(
        &self,
        after: Option<&ProjectionIdentity>,
        limit: ProjectionRecoveryPageLimit,
    ) -> Result<ProjectionControlScanV1, StorageError> {
        self.check_live()?;
        self.applier.scan_projection_controls(after, limit)
    }
    fn validate_projection_recovery_page(
        &self,
        request: &ProjectionRecoveryValidationRequestV1,
    ) -> Result<ProjectionRecoveryValidationResultV1, StorageError> {
        self.check_live()?;
        self.applier.validate_projection_recovery_page(request)
    }
}
