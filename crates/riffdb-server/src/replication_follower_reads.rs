//! One completed-prefix publication. Readers never acquire a writer or latest root.
#[path = "replication_follower_read_catalog.rs"]
mod catalog;
use riffdb_catalog::ActiveCatalogSnapshot;
use riffdb_projection::{ProjectionNotifier, ProjectionSchemaRegistry};
use riffdb_storage_api::{
    CapabilityLookupResult, CapabilityReader, ChangelogHistoryStateV3, OwnedSnapshotReader,
    StorageError, StorageErrorKind, StoredCapabilityRecordV1,
};
use riffdb_storage_redb::RedbOwnedSnapshot;
use riffdb_types::{CapabilityId, CapabilityTokenDigest};
use std::sync::Arc;
use tokio::sync::watch;

/// Immutable semantic reads and checked catalog from one completed follower prefix.
/// Internal request drivers must bound pin lifetimes and drain them at shutdown.
pub struct FollowerReadView {
    pub(crate) history: ChangelogHistoryStateV3,
    pub(crate) snapshot: RedbOwnedSnapshot,
    pub(crate) catalog: Option<ActiveCatalogSnapshot>,
    pub(crate) source_head: Option<riffdb_service::ReplicationSourceHead>,
    pub(crate) acknowledged: Option<riffdb_storage_api::ChangelogHistoryPointV3>,
}
impl FollowerReadView {
    /// Local acknowledgement recorded at this exact read pin. It may lag the
    /// applied head after restart and is never inferred from source emission.
    pub fn acknowledged(&self) -> Option<riffdb_storage_api::ChangelogHistoryPointV3> {
        self.acknowledged
    }
    /// Last source-head report validated against this completed frame. Absence
    /// means unknown source progress, not zero lag. This never supplies freshness.
    pub fn source_head(&self) -> Option<riffdb_service::ReplicationSourceHead> {
        self.source_head
    }
    /// Exact durable history bound to every read in this view.
    pub fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }
    /// Immutable semantic storage reads from this completed prefix.
    pub fn snapshot(&self) -> &RedbOwnedSnapshot {
        &self.snapshot
    }
    /// Catalog checked by its semantic owner at this prefix.
    pub fn catalog(&self) -> Option<&ActiveCatalogSnapshot> {
        self.catalog.as_ref()
    }
}
impl std::fmt::Debug for FollowerReadView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FollowerReadView([redacted])")
    }
}
/// Cloneable observation only. Publication failure/withdrawal is fail-closed even
/// if an older immutable pin remains alive in an already admitted request.
#[derive(Clone)]
pub struct FollowerReadSnapshots {
    receiver: watch::Receiver<Option<Arc<FollowerReadView>>>,
}
impl FollowerReadSnapshots {
    /// Pins the latest completed prefix, or refuses after withdrawal.
    pub fn latest(&self) -> Result<Arc<FollowerReadView>, StorageError> {
        self.receiver.borrow().clone().ok_or_else(unavailable)
    }
    /// Register before checking a freshness condition; watch retains changes
    /// across cancellation and never requires a polling sleep to avoid lost wakeups.
    pub async fn changed(&mut self) -> Result<Arc<FollowerReadView>, StorageError> {
        self.receiver.changed().await.map_err(|_| unavailable())?;
        self.receiver
            .borrow_and_update()
            .clone()
            .ok_or_else(unavailable)
    }
}
impl std::fmt::Debug for FollowerReadSnapshots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FollowerReadSnapshots([redacted])")
    }
}
impl OwnedSnapshotReader for FollowerReadSnapshots {
    type Snapshot<'a> = RedbOwnedSnapshot;

    fn open_owned_snapshot(&self) -> Result<Self::Snapshot<'_>, StorageError> {
        Ok(self.latest()?.snapshot.clone())
    }
}
impl CapabilityReader for FollowerReadSnapshots {
    fn read_capability(
        &self,
        capability_id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
        self.latest()?.snapshot.read_capability(capability_id)
    }

    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        self.latest()?
            .snapshot
            .resolve_capability_digests(candidates)
    }
    // Every authorization samples the live publication. An immutable pin cannot
    // certify that no newer capability revision has become visible.
}
pub(super) struct Publication {
    projections: Option<ProjectionPublication>,
    sender: watch::Sender<Option<Arc<FollowerReadView>>>,
}
impl Publication {
    pub(super) fn new() -> Self {
        let (sender, _) = watch::channel(None);
        Self {
            sender,
            projections: None,
        }
    }
    pub(super) fn readers(&self) -> FollowerReadSnapshots {
        FollowerReadSnapshots {
            receiver: self.sender.subscribe(),
        }
    }
    pub(super) fn attempt(&self) -> Attempt {
        Attempt {
            sender: self.sender.clone(),
            projections: self
                .projections
                .as_ref()
                .map(|p| (p.notifier.clone(), p.empty.clone())),
            armed: true,
        }
    }
    pub(super) fn projection_notifier(&mut self) -> Result<ProjectionNotifier, StorageError> {
        if let Some(published) = &self.projections {
            return Ok(published.notifier.clone());
        }
        let view = self.sender.borrow().clone().ok_or_else(unavailable)?;
        let registry = crate::projection_adapter::active_projection_registry(view.snapshot())
            .map_err(|_| unavailable())?;
        let empty = ProjectionSchemaRegistry::new(Vec::new()).map_err(|_| unavailable())?;
        let notifier = ProjectionNotifier::from_registry(&registry);
        self.projections = Some(ProjectionPublication {
            notifier: notifier.clone(),
            registry,
            empty,
            pointer: view.catalog().map(|catalog| catalog.pointer().clone()),
        });
        Ok(notifier)
    }
    pub(super) fn publish(
        &mut self,
        view: FollowerReadView,
        changed: &[riffdb_types::ProjectionIdentity],
    ) -> Result<(), StorageError> {
        let previous = self.sender.borrow().clone();
        if let Some(previous) = previous {
            if previous.history.lineage() != view.history.lineage()
                || previous.history.tail().sequence() > view.history.tail().sequence()
                || (previous.history.tail().sequence() == view.history.tail().sequence()
                    && previous.history != view.history)
            {
                return Err(StorageError::new(StorageErrorKind::CorruptData, None));
            }
            if previous.history == view.history {
                return Ok(());
            }
        }
        if let Some(projections) = &mut self.projections {
            let pointer = view.catalog().map(|catalog| catalog.pointer().clone());
            if pointer != projections.pointer {
                let registry =
                    crate::projection_adapter::active_projection_registry(view.snapshot())
                        .map_err(|_| unavailable())?;
                projections
                    .notifier
                    .synchronize_registry(&registry)
                    .map_err(|_| unavailable())?;
                projections.registry = registry;
                projections.pointer = pointer;
            }
        }
        drop(self.sender.send_replace(Some(Arc::new(view))));
        if let Some(projections) = &self.projections {
            for identity in changed {
                if projections.registry.get(identity).is_some() {
                    projections
                        .notifier
                        .notify(identity)
                        .map_err(|_| unavailable())?;
                }
            }
        }
        Ok(())
    }
    pub(super) fn withdraw(&self) {
        drop(self.sender.send_replace(None));
        if let Some(p) = &self.projections {
            let _ = p.notifier.synchronize_registry(&p.empty);
        }
    }
}
impl Drop for Publication {
    fn drop(&mut self) {
        self.withdraw();
    }
}
/// Any failed or cancelled receiver operation withdraws the live read publication.
/// Ordinary transient retries disarm this guard after restoring the sole owner.
struct ProjectionPublication {
    notifier: ProjectionNotifier,
    registry: ProjectionSchemaRegistry,
    empty: ProjectionSchemaRegistry,
    pointer: Option<riffdb_storage_api::ActiveCatalogPointerV1>,
}
pub(super) struct Attempt {
    projections: Option<(ProjectionNotifier, ProjectionSchemaRegistry)>,
    sender: watch::Sender<Option<Arc<FollowerReadView>>>,
    armed: bool,
}
impl Attempt {
    pub(super) fn complete(&mut self) {
        self.armed = false;
    }
}
impl Drop for Attempt {
    fn drop(&mut self) {
        if self.armed {
            drop(self.sender.send_replace(None));
            if let Some((notifier, empty)) = &self.projections {
                let _ = notifier.synchronize_registry(empty);
            }
        }
    }
}
fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
