//! Incremental projection catch-up under continuous receiver custody.
use super::*;
use riffdb_catalog::ActiveCatalogSnapshot;
use riffdb_storage_api::{AuthoritativeNamespaceV1 as N, ChangelogFrameV3};
use riffdb_storage_redb::RedbFollowerApplier;
use riffdb_types::ProjectionFrontierKey;
use std::collections::BTreeMap;

/// A frame-bounded final control set. It retains only decoded controls, never
/// command payloads or an inventory of unrelated projection generations.
pub(super) struct ProjectionTailChanges {
    catalog_changed: bool,
    controls: BTreeMap<ProjectionIdentity, StoredProjectionControlV1>,
}
impl ProjectionTailChanges {
    pub(super) fn identities(&self) -> Vec<ProjectionIdentity> {
        self.controls.keys().cloned().collect()
    }
}
#[derive(Default)]
pub(super) struct FollowerProjectionTail {
    catalog: Option<ActiveCatalogSnapshot>,
    loaded: bool,
    failed: bool,
    #[cfg(test)]
    pub(super) catalog_loads: usize,
    #[cfg(test)]
    pub(super) replay_steps: usize,
}
impl FollowerProjectionTail {
    pub(super) fn capture_read_view(
        &mut self,
        owner: &RedbFollowerApplier,
    ) -> Result<super::FollowerReadView, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        let (history, state, snapshot) = owner.capture_read_progress_snapshot()?;
        if !self.loaded {
            self.catalog = ActiveCatalogSnapshot::read(&snapshot).map_err(|_| corrupt())?;
            self.loaded = true;
            #[cfg(test)]
            {
                self.catalog_loads += 1;
            }
        }
        Ok(super::FollowerReadView {
            history,
            snapshot,
            catalog: self.catalog.clone(),
            source_head: None,
            acknowledged: state.attached_state().ok_or_else(corrupt)?.2,
        })
    }

    pub(super) fn plan(
        &self,
        frame: &ChangelogFrameV3,
    ) -> Result<ProjectionTailChanges, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        let mut changes = ProjectionTailChanges {
            catalog_changed: false,
            controls: BTreeMap::new(),
        };
        for receipt in frame.receipts() {
            for mutation in receipt.mutations() {
                changes.catalog_changed |= matches!(
                    mutation.namespace(),
                    N::ContractBundles
                        | N::CatalogActive
                        | N::ContractMigrations
                        | N::ContractWriteRetirements
                );
                if mutation.namespace() != N::ProjectionFrontier {
                    continue;
                }
                let key = ProjectionFrontierKey::from_bytes(mutation.key().to_vec())
                    .map_err(|_| corrupt())?;
                // ADR-0017 retains generation allocation authority permanently;
                // retired rows stay inert until a separate GC decision. No
                // accepted control transition deletes or resets this record.
                let bytes = mutation.value().ok_or_else(corrupt)?;
                let value = riffdb_storage_api::proto_codec::decode_projection_control_v1(bytes)
                    .map_err(|_| corrupt())?
                    .into_parts()
                    .0;
                if value.identity() != key.identity() {
                    return Err(corrupt());
                }
                changes.controls.insert(key.identity().clone(), value);
            }
        }
        Ok(changes)
    }

    pub(super) fn replay(
        &mut self,
        owner: &mut RedbFollowerApplier,
        changes: ProjectionTailChanges,
        cancellation: &AtomicBool,
    ) -> Result<(), StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        self.failed = true;
        check_cancel(cancellation)?;
        if changes.catalog_changed {
            self.catalog = None;
            self.loaded = false;
        }
        if changes.catalog_changed || (!self.loaded && !changes.controls.is_empty()) {
            self.catalog = ActiveCatalogSnapshot::read(owner).map_err(|_| corrupt())?;
            self.loaded = true;
            #[cfg(test)]
            {
                self.catalog_loads += 1;
            }
        }
        let head = owner
            .durable_history()?
            .tail()
            .frontier()
            .application()
            .map_or(
                FrontierPosition::BeforeFirst,
                FrontierPosition::AppliedThrough,
            );
        for control in changes.controls.into_values() {
            check_cancel(cancellation)?;
            let resolved = self
                .catalog
                .as_ref()
                .ok_or_else(corrupt)?
                .resolve_projection(control.identity())
                .map_err(|_| corrupt())?;
            let schema = resolved.checked_group_schema().map_err(|_| corrupt())?;
            for position in [control.published(), control.candidate()]
                .into_iter()
                .flatten()
            {
                loop {
                    check_cancel(cancellation)?;
                    if replay_projection_commit(
                        owner, &resolved, &schema, &control, position, head,
                    )? {
                        break;
                    }
                    #[cfg(test)]
                    {
                        self.replay_steps += 1;
                    }
                }
            }
        }
        check_cancel(cancellation)?;
        self.failed = false;
        Ok(())
    }
}
impl ProjectionReplayStorage for RedbFollowerApplier {
    fn persist_replay(
        &mut self,
        control: &StoredProjectionControlV1,
        request: &riffdb_storage_api::ProjectionApplyRequestV1,
    ) -> Result<(), StorageError> {
        self.rebuild_projection_commit(control, request).map(|_| ())
    }
}
