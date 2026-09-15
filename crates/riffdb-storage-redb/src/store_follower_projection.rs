//! Derived projection persistence under the same move-only follower writer.
//! No authoritative row, source control, receipt, or allocator is changed here.
use super::*;
use crate::projection_replay::{
    ProjectionReplayStage, read_projection_replay_snapshot, stage_projection_replay,
};
use riffdb_storage_api::{
    AuthoritativeEntityPartitionScanPage, AuthoritativeEntityPartitionScanRequest,
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativeScanReader,
    CommitScanPageV1, CommitScanRequest, ProjectionApplyRequestV1, ProjectionApplySnapshot,
    ProjectionApplySnapshotReader, ProjectionApplySnapshotRequest, ProjectionControlScanV1,
    ProjectionRecoveryPageLimit, ProjectionRecoveryRepository,
    ProjectionRecoveryValidationRequestV1, ProjectionRecoveryValidationResultV1,
    StoredProjectionApplyV1, StoredProjectionControlV1,
};
use riffdb_types::ProjectionIdentity;

impl RedbFollowerApplier {
    /// Opens the complete canonical authoritative inventory at one immutable
    /// follower pin. This is read evidence only; it grants no source publication,
    /// downstream replication, or serving-readiness authority.
    pub fn authoritative_state_v3(
        &self,
    ) -> Result<
        Box<dyn riffdb_storage_api::AuthoritativeStateCursorV3>,
        riffdb_storage_api::ChangelogCursorErrorV3,
    > {
        self.ensure_live()?;
        let root = self.shared.capture_checkpoint_root()?;
        let history = crate::changelog_v3_roots::read_checkpoint_roots(&root)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        crate::follower_lifecycle::preserve_attached_lifecycle(
            &root,
            history.lineage().database_id(),
            history.lineage().history_incarnation(),
        )?;
        crate::changelog_v3_cursor::state::open_attached(root, history)
    }

    /// Pins one immutable durable follower root and its exact history binding.
    /// This grants read capabilities only, not publication or serving readiness.
    pub fn capture_read_snapshot(
        &self,
    ) -> Result<(ChangelogHistoryStateV3, crate::RedbOwnedSnapshot), StorageError> {
        let (history, _, snapshot) = self.capture_read_progress_snapshot()?;
        Ok((history, snapshot))
    }

    /// Captures applied history, the separately retained local acknowledgement,
    /// and semantic reads in one immutable pin. Restart may expose an applied
    /// prefix whose acknowledgement is absent or older; observation never fills it.
    pub fn capture_read_progress_snapshot(
        &self,
    ) -> Result<
        (
            ChangelogHistoryStateV3,
            ReplicationFollowerStateV3,
            crate::RedbOwnedSnapshot,
        ),
        StorageError,
    > {
        let access = self.replay_read_access()?;
        let history = crate::changelog_v3_roots::read_checkpoint_roots(&access)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        crate::follower_lifecycle::preserve_attached_lifecycle(
            &access,
            history.lineage().database_id(),
            history.lineage().history_incarnation(),
        )?;
        let meta = access.open_table(META).map_err(table_error)?;
        let row = meta
            .get(key(N::ReplicationFollowerState)?)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let state = *decode_replication_follower_state_v3(row.value())
            .map_err(crate::error::codec_error)?
            .value();
        let (lineage, applied, _) = state
            .attached_state()
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if lineage != history.lineage() || applied != history.tail() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        drop(row);
        drop(meta);
        Ok((
            history,
            state,
            crate::owned_snapshot::RedbOwnedSnapshot::from_read_access(access),
        ))
    }

    fn replay_read_access(&self) -> Result<RedbReadAccess, StorageError> {
        self.ensure_live()?;
        Ok(RedbReadAccess::Durable(
            self.shared.capture_checkpoint_root()?,
        ))
    }

    fn replay_snapshot(&self) -> Result<crate::owned_snapshot::RedbOwnedSnapshot, StorageError> {
        Ok(crate::owned_snapshot::RedbOwnedSnapshot::from_read_access(
            self.replay_read_access()?,
        ))
    }

    /// Persists one bounded pure-engine replay result at its exact local frontier.
    /// Source controls and applied/acknowledged changelog positions stay unchanged.
    /// Failure fuses the sole writer; a retry requires validated follower reopen.
    pub fn rebuild_projection_commit(
        &mut self,
        expected: &StoredProjectionControlV1,
        request: &ProjectionApplyRequestV1,
    ) -> Result<StoredProjectionApplyV1, StorageError> {
        self.ensure_live()?;
        self.failed = true;
        let _lease = self.shared.mutation_gate.acquire()?;
        let mut write = self
            .shared
            .database
            .begin_write()
            .map_err(transaction_error)?;
        write.set_two_phase_commit(true);
        write
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let marker = match stage_projection_replay(&write, expected, request)? {
            ProjectionReplayStage::Existing(marker) => {
                write.abort().map_err(precommit_storage_error)?;
                marker
            }
            ProjectionReplayStage::Staged(marker) => {
                crash_edge("projection-staged");
                self.shared.commit_durable(write)?;
                crash_edge("projection-committed");
                marker
            }
        };
        self.failed = false;
        Ok(marker)
    }
}

impl ProjectionApplySnapshotReader for RedbFollowerApplier {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        read_projection_replay_snapshot(&self.replay_read_access()?, request)
    }
}
impl AuthoritativeScanReader for RedbFollowerApplier {
    fn scan_entity_partition(
        &self,
        request: AuthoritativeEntityPartitionScanRequest,
    ) -> Result<AuthoritativeEntityPartitionScanPage, StorageError> {
        self.replay_snapshot()?.scan_entity_partition(request)
    }
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        self.replay_snapshot()?.scan_index(request)
    }
    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        self.replay_snapshot()?.scan_commits(request)
    }
}
impl ProjectionRecoveryRepository for RedbFollowerApplier {
    fn scan_projection_controls(
        &self,
        after: Option<&ProjectionIdentity>,
        limit: ProjectionRecoveryPageLimit,
    ) -> Result<ProjectionControlScanV1, StorageError> {
        crate::derived::scan_projection_controls_at(&self.replay_read_access()?, after, limit)
    }
    fn validate_projection_recovery_page(
        &self,
        request: &ProjectionRecoveryValidationRequestV1,
    ) -> Result<ProjectionRecoveryValidationResultV1, StorageError> {
        crate::derived::validate_projection_recovery_page_at(&self.replay_read_access()?, request)
    }
}

// Same bounded catalog owner and codecs, at a fresh immutable follower pin.
impl riffdb_storage_api::CatalogRepository for RedbFollowerApplier {
    fn read_active_catalog(
        &self,
    ) -> Result<Option<riffdb_storage_api::ActiveCatalogPointerV1>, StorageError> {
        self.replay_snapshot()?.read_active_catalog()
    }
    fn read_contract_bundle(
        &self,
        lineage: &riffdb_types::ContractLineage,
        version: riffdb_types::ContractVersion,
    ) -> Result<Option<riffdb_storage_api::StoredContractBundleV1>, StorageError> {
        self.replay_snapshot()?
            .read_contract_bundle(lineage, version)
    }
    fn read_contract_migration_edge(
        &self,
        predecessor: riffdb_types::ContractBundleHash,
    ) -> Result<Option<riffdb_storage_api::StoredContractMigrationEdgeV1>, StorageError> {
        self.replay_snapshot()?
            .read_contract_migration_edge(predecessor)
    }
}
