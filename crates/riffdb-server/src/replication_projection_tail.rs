//! Incremental projection catch-up under continuous receiver custody.
use super::*;
use riffdb_catalog::ActiveCatalogSnapshot;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, ChangelogFrameV3, ProjectionGenerationPosition,
    ProjectionLifecycleV1, StoredProjectionControlV1,
};
use riffdb_storage_redb::RedbFollowerApplier;
use riffdb_types::{FrontierPosition, ProjectionGeneration, ProjectionIdentity};
use std::collections::BTreeMap;

/// Frame-bounded catalog change. Projection list and catch-up target come from
/// the active catalog and the follower's own durable frontier (ADR-0248), not
/// from replicated ProjectionFrontier mutations.
pub(super) struct ProjectionTailChanges {
    catalog_changed: bool,
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
        let mut catalog_changed = false;
        for receipt in frame.receipts() {
            for mutation in receipt.mutations() {
                catalog_changed |= matches!(
                    mutation.namespace(),
                    N::ContractBundles
                        | N::CatalogActive
                        | N::ContractMigrations
                        | N::ContractWriteRetirements
                );
            }
        }
        Ok(ProjectionTailChanges { catalog_changed })
    }

    pub(super) fn replay(
        &mut self,
        owner: &mut RedbFollowerApplier,
        changes: ProjectionTailChanges,
        cancellation: &AtomicBool,
    ) -> Result<Vec<ProjectionIdentity>, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        self.failed = true;
        check_cancel(cancellation)?;
        if changes.catalog_changed {
            self.catalog = None;
            self.loaded = false;
        }
        if !self.loaded {
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
        let mut advanced = Vec::new();
        let controls = catalog_follower_controls(self.catalog.as_ref(), head)?;
        for control in controls.into_values() {
            check_cancel(cancellation)?;
            let resolved = self
                .catalog
                .as_ref()
                .ok_or_else(corrupt)?
                .resolve_projection(control.identity())
                .map_err(|_| corrupt())?;
            let schema = resolved.checked_group_schema().map_err(|_| corrupt())?;
            let identity = control.identity().clone();
            let mut applied = false;
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
                    applied = true;
                    #[cfg(test)]
                    {
                        self.replay_steps += 1;
                    }
                }
            }
            if applied {
                advanced.push(identity);
            }
        }
        check_cancel(cancellation)?;
        self.failed = false;
        Ok(advanced)
    }
}

fn catalog_follower_controls(
    catalog: Option<&ActiveCatalogSnapshot>,
    head: FrontierPosition,
) -> Result<BTreeMap<ProjectionIdentity, StoredProjectionControlV1>, StorageError> {
    let mut controls = BTreeMap::new();
    let Some(catalog) = catalog else {
        return Ok(controls);
    };
    for plan in catalog.bundle().bundle().projections() {
        let identity = ProjectionIdentity::new(
            catalog.bundle().lineage().clone(),
            plan.projection_id(),
            plan.plan_hash(),
        );
        let control = StoredProjectionControlV1::new(
            identity,
            ProjectionGeneration::first(),
            None,
            Some(ProjectionGenerationPosition::new(
                ProjectionGeneration::first(),
                head,
            )),
            None,
            ProjectionLifecycleV1::CatchingUp,
            None,
        )
        .map_err(|_| corrupt())?;
        controls.insert(control.identity().clone(), control);
    }
    Ok(controls)
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
