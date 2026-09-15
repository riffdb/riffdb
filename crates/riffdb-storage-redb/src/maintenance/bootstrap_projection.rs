//! Derived-only replay into an excluded private candidate. Pure projection
//! evaluation stays in its existing engine; this port never advances controls.
use super::*;
use crate::{
    projection_replay::{
        ProjectionReplayStage, read_projection_replay_snapshot, stage_projection_replay,
    },
    store::RedbReadAccess,
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

impl RedbBootstrapCandidate {
    fn rebuild_read_access(&self) -> Result<RedbReadAccess, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        self.owner.verify()?;
        Ok(RedbReadAccess::Durable(Arc::new(
            crate::checkpoint_root::CheckpointRoot::new(
                self.database.begin_read().map_err(unavailable)?,
                0,
            ),
        )))
    }
    fn rebuild_snapshot(&self) -> Result<crate::owned_snapshot::RedbOwnedSnapshot, StorageError> {
        Ok(crate::owned_snapshot::RedbOwnedSnapshot::from_read_access(
            self.rebuild_read_access()?,
        ))
    }

    /// Persists one bounded pure-engine replay result in derived rows/markers.
    /// The exact source control is required and never changed. Equal retries
    /// are read-only; failure fuses this private candidate until reopen.
    pub fn rebuild_projection_commit(
        &mut self,
        expected: &StoredProjectionControlV1,
        request: &ProjectionApplyRequestV1,
    ) -> Result<StoredProjectionApplyV1, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        self.failed = true;
        self.owner.verify()?;
        let mut write = self.database.begin_write().map_err(unavailable)?;
        immediate(&mut write)?;
        let marker = match stage_projection_replay(&write, expected, request)? {
            ProjectionReplayStage::Existing(marker) => {
                write.abort().map_err(unavailable)?;
                marker
            }
            ProjectionReplayStage::Staged(marker) => {
                rebuild_edge("derived-staged");
                self.owner.verify()?;
                write.commit().map_err(unavailable)?;
                rebuild_edge("derived-committed");
                marker
            }
        };
        self.owner.verify()?;
        self.failed = false;
        Ok(marker)
    }
}
impl ProjectionApplySnapshotReader for RedbBootstrapCandidate {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        let result = read_projection_replay_snapshot(&self.rebuild_read_access()?, request)?;
        self.owner.verify()?;
        Ok(result)
    }
}
impl AuthoritativeScanReader for RedbBootstrapCandidate {
    fn scan_entity_partition(
        &self,
        request: AuthoritativeEntityPartitionScanRequest,
    ) -> Result<AuthoritativeEntityPartitionScanPage, StorageError> {
        self.rebuild_snapshot()?.scan_entity_partition(request)
    }
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        self.rebuild_snapshot()?.scan_index(request)
    }
    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        self.rebuild_snapshot()?.scan_commits(request)
    }
}
impl ProjectionRecoveryRepository for RedbBootstrapCandidate {
    fn scan_projection_controls(
        &self,
        after: Option<&ProjectionIdentity>,
        limit: ProjectionRecoveryPageLimit,
    ) -> Result<ProjectionControlScanV1, StorageError> {
        crate::derived::scan_projection_controls_at(&self.rebuild_read_access()?, after, limit)
    }
    fn validate_projection_recovery_page(
        &self,
        request: &ProjectionRecoveryValidationRequestV1,
    ) -> Result<ProjectionRecoveryValidationResultV1, StorageError> {
        crate::derived::validate_projection_recovery_page_at(&self.rebuild_read_access()?, request)
    }
}
fn rebuild_edge(_edge: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_BOOTSTRAP_PROJECTION_EDGE")
        .ok()
        .as_deref()
        == Some(_edge)
    {
        std::process::exit(93);
    }
}
