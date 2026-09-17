//! Complete source artifacts are held durably before any bytes are released.
use super::bootstrap_repository::RepositoryInner;
use super::{RedbVerifiedBootstrapTransfer, bootstrap_stage::SourceBuild};
use crate::RedbOperationalPorts;
use riffdb_storage_api::{
    ChangelogCursorErrorV3 as Refusal, ChangelogHistoryPointV3,
    ReplicationBootstrapFenceV3 as Fence, ReplicationBootstrapManifestV1 as Manifest,
    ReplicationBootstrapPageV3 as Page, ReplicationSourceHoldIdV1 as HoldId,
    ReplicationSourceHoldKindV1 as Kind, ReplicationSourceHoldV1 as Hold, StorageError,
};
use std::{path::Path, sync::Arc};

/// Bounded private source build or resume. Drop releases local handles and any
/// source pin. Scratch files remain private; no retention hold is released.
/// The caller schedules one advance at a time and owns cancellation/deadlines.
pub struct RedbBootstrapSourceBuild {
    build: SourceBuild,
    ports: RedbOperationalPorts,
    repository: Option<Arc<RepositoryInner>>,
}

impl RedbBootstrapSourceBuild {
    pub(super) fn with_repository(mut self, repository: Arc<RepositoryInner>) -> Self {
        self.repository = Some(repository);
        self
    }
    fn verify_repository(&self) -> Result<(), StorageError> {
        self.repository
            .as_ref()
            .map_or(Ok(()), |repository| repository.verify())
    }
    /// Copies or verifies at most one bounded page; true means ready to finish.
    pub fn advance(&mut self) -> Result<bool, StorageError> {
        self.verify_repository()?;
        self.build.advance()
    }

    /// Registers the exact durable hold before exposing any manifest or page.
    /// An uncertain registration retains the private artifact for safe resume.
    pub fn finish(self) -> Result<RedbHeldBootstrapSource, Refusal> {
        self.verify_repository()?;
        let mut held = self.ports.hold_bootstrap_transfer(self.build.finish()?)?;
        held.repository = self.repository;
        held.verify_private_identity()?;
        Ok(held)
    }
}

/// Exact source artifact whose bootstrap retention hold is already durable.
/// Drop closes local handles but never releases the hold. The authorized RPC
/// must still recheck current policy before each outbound manifest/page.
pub struct RedbHeldBootstrapSource {
    transfer: RedbVerifiedBootstrapTransfer,
    ports: RedbOperationalPorts,
    repository: Option<Arc<RepositoryInner>>,
}

impl RedbOperationalPorts {
    /// Advances only an existing follower acknowledgement under the exact
    /// current source lineage. Caller owns current administrative authorization
    /// and the receiver's already-durable local acknowledgement.
    pub fn acknowledge_replication_follower_v3(
        &self,
        id: HoldId,
        lineage: riffdb_storage_api::ChangelogLineageV3,
        acknowledged: ChangelogHistoryPointV3,
    ) -> Result<(), Refusal> {
        self.replication_source_control()
            .advance_acknowledgement(Hold::new(
                id,
                Kind::FollowerAcknowledgement,
                lineage,
                acknowledged,
            ))?;
        Ok(())
    }

    /// Records the authenticated receiver's exact bootstrap acknowledgement.
    /// Replication composition must establish current authorization and durable
    /// receiver publication before calling. The source replaces its bootstrap
    /// hold with an identical follower fence atomically; it never removes the
    /// retention fence. Exact retries need no artifact reopen or new hold.
    pub fn attach_replication_bootstrap_v3(
        &self,
        manifest: Manifest,
        acknowledged: ChangelogHistoryPointV3,
    ) -> Result<(), Refusal> {
        let fence = manifest.fence();
        if acknowledged != fence.history().tail() {
            return Err(Refusal::InvalidPosition);
        }
        self.replication_source_control()
            .attach_bootstrap(Hold::new(
                fence.hold_id(),
                Kind::Bootstrap,
                fence.history().lineage(),
                acknowledged,
            ))?;
        Ok(())
    }

    /// Pins one published source snapshot and creates a private build owner.
    /// This does not scan the source or acquire its exclusive writer gate.
    pub fn begin_replication_bootstrap_v3(
        &self,
        path: &Path,
        id: HoldId,
    ) -> Result<RedbBootstrapSourceBuild, Refusal> {
        let snapshot = self.published_changelog_snapshot_v3()?;
        let input = snapshot.authoritative_state_v3()?;
        let fence = Fence::new(id, input.history());
        Ok(RedbBootstrapSourceBuild {
            build: SourceBuild::begin(path, fence, input)?,
            ports: RedbOperationalPorts {
                shared: std::sync::Arc::clone(&self.shared),
            },
            repository: None,
        })
    }

    /// Opens only bounded durable progress. Every page must subsequently be
    /// verified by advance before exact hold registration can release bytes.
    pub fn begin_replication_bootstrap_resume_v3(
        &self,
        path: &Path,
        id: HoldId,
    ) -> Result<RedbBootstrapSourceBuild, Refusal> {
        Ok(RedbBootstrapSourceBuild {
            build: SourceBuild::resume(path, id)?,
            ports: RedbOperationalPorts {
                shared: std::sync::Arc::clone(&self.shared),
            },
            repository: None,
        })
    }

    /// Materializes one immutable published cursor without holding the writer
    /// gate, then registers its fence at the existing drained control barrier.
    /// If history was pruned during materialization, registration refuses before
    /// any manifest or page can leave this method.
    pub fn prepare_replication_bootstrap_v3(
        &self,
        path: &Path,
        id: HoldId,
    ) -> Result<RedbHeldBootstrapSource, Refusal> {
        let mut build = self.begin_replication_bootstrap_v3(path, id)?;
        while !build.advance()? {}
        build.finish()
    }

    /// Revalidates a complete durable artifact and idempotently registers its
    /// exact fence before releasing it. Incomplete source artifacts fail closed.
    pub fn resume_replication_bootstrap_v3(
        &self,
        path: &Path,
        id: HoldId,
    ) -> Result<RedbHeldBootstrapSource, Refusal> {
        let mut build = self.begin_replication_bootstrap_resume_v3(path, id)?;
        while !build.advance()? {}
        build.finish()
    }

    fn hold_bootstrap_transfer(
        &self,
        transfer: RedbVerifiedBootstrapTransfer,
    ) -> Result<RedbHeldBootstrapSource, Refusal> {
        let fence = transfer.manifest().fence();
        self.replication_source_control().register(Hold::new(
            fence.hold_id(),
            Kind::Bootstrap,
            fence.history().lineage(),
            fence.history().tail(),
        ))?;
        Ok(RedbHeldBootstrapSource {
            transfer,
            ports: RedbOperationalPorts {
                shared: Arc::clone(&self.shared),
            },
            repository: None,
        })
    }
}

impl RedbHeldBootstrapSource {
    fn verify_paths(&self) -> Result<(), StorageError> {
        self.repository
            .as_ref()
            .map_or(Ok(()), |repository| repository.verify())?;
        self.transfer.verify_private_identity()
    }
    fn verify_current_custody(&self) -> Result<(), StorageError> {
        let fence = self.transfer.manifest().fence();
        self.ports.validate_bootstrap_custody(Hold::new(
            fence.hold_id(),
            Kind::Bootstrap,
            fence.history().lineage(),
            fence.history().tail(),
        ))
    }
    /// Checks retained paths and current source custody before releasing a manifest.
    pub fn verify_private_identity(&self) -> Result<(), StorageError> {
        self.verify_paths()?;
        self.verify_current_custody()
    }
    /// Exact fixed manifest whose source hold is durable.
    #[must_use]
    pub const fn manifest(&self) -> Manifest {
        self.transfer.manifest()
    }
    /// One bounded immutable page. Current caller authorization remains required.
    pub fn read_page(&self, ordinal: u32) -> Result<Page, StorageError> {
        self.verify_paths()?;
        let page = self.transfer.read_page(ordinal)?;
        // Validate after bounded page I/O, so an old handle cannot release bytes
        // after observing retirement or loss of its exact source job.
        self.verify_current_custody()?;
        Ok(page)
    }
}
