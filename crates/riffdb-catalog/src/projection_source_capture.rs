//! Read-only checked plan custody for a native source worker. This owner supplies
//! neither an engine/prefix pin nor permission to publish an authoritative row.
use crate::projection_materialization::select_projection_from_lineage;
use crate::{ActiveCatalogSnapshot, ResolvedProjectionPlan};
use riffdb_storage_api::{ActiveCatalogPointerV1, CheckedProjectionSchema};
use riffdb_types::{
    InspectOwnedCapacityV1, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionV1,
    ProjectionIdentity, ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
};

/// Closed, redaction-safe source planning refusal. Ordinary catalog resolution
/// remains independent of the native source worker's capacity.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionSourcePlanCaptureErrorV1 {
    /// The complete retained graph or cloned checked plan could not be admitted.
    Unavailable,
    /// The identity was not an exact member of this immutable checked catalog.
    Integrity,
}
impl std::fmt::Display for ProjectionSourcePlanCaptureErrorV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "projection source plan capture unavailable",
            Self::Integrity => "projection source plan capture integrity failure",
        })
    }
}
impl std::error::Error for ProjectionSourcePlanCaptureErrorV1 {}

type Error = ProjectionSourcePlanCaptureErrorV1;

/// The exact catalog owner remains borrowed through every checked-plan use.
/// A provider-accounted input therefore retains its original physical owner;
/// this type never settles or reassigns the catalog's shared allocations. Its
/// separate conservative reservation covers the whole retained graph and the
/// incremental checked-plan/schema clones until they have physically dropped.
///
/// Native composition must bind the catalog pointer to the originating engine's
/// captured active pointer, then recheck authority at publication. This pure
/// value alone makes none of those claims. The owner has no consuming conversion or Clone implementation.
#[doc(hidden)]
pub struct ProjectionSourcePlanCaptureV1<'catalog> {
    plan: ResolvedProjectionPlan,
    schema: CheckedProjectionSchema,
    catalog: &'catalog ActiveCatalogSnapshot,
    budget: ProjectionSourceWorkingBudgetV1,
    // Last: the cloned graphs drop before their complete reserved capacity.
    _work: ProjectionSourceWorkReservationV1,
}
// Retain the borrowed physical input owner through destruction, including
// reservations stored outside its shared graph. NLL must not end this borrow
// merely because the final plan accessor has already returned.
impl Drop for ProjectionSourcePlanCaptureV1<'_> {
    fn drop(&mut self) {}
}

impl ProjectionSourcePlanCaptureV1<'_> {
    /// Exact checked plan; its lifetime cannot exceed this admitted owner.
    pub fn plan(&self) -> &ResolvedProjectionPlan {
        &self.plan
    }
    /// Exact compiler-bound group/measure schema; not a publication capability.
    pub fn schema(&self) -> &CheckedProjectionSchema {
        &self.schema
    }
    /// Exact active pointer of the borrowed immutable catalog publication.
    pub fn catalog_pointer(&self) -> &ActiveCatalogPointerV1 {
        self.catalog.pointer()
    }
    /// Same actual source ledger; durable database-ID equality is insufficient.
    pub fn belongs_to(&self, budget: &ProjectionSourceWorkingBudgetV1) -> bool {
        self.budget.shares_owner_with(budget)
    }
    /// The caller must compare this with its actual originating native ledger.
    pub fn originating_budget(&self) -> &ProjectionSourceWorkingBudgetV1 {
        &self.budget
    }
}

// Inspect only after census scratch admission. Existing owned-capacity traversal
// uses a finite node/visit bound and allocator allowance. Clone requests at most
// the existing capacities; include every shared allocation conservatively, even
// when a lifecycle already owns it in a different accounting graph. This is a
// reservation for retention/overlap, never a second physical allocation owner.
fn retained_capacity(
    value: &(impl InspectOwnedCapacityV1 + ?Sized),
    scope: riffdb_types::OwnedCapacityInspectionScopeV1<'_>,
) -> Result<u64, Error> {
    let report =
        OwnedCapacityInspectionV1::preview(value, scope).map_err(|_| Error::Unavailable)?;
    let mut total = report.unshared_bytes();
    for allocation in report.shared_allocations() {
        total = total
            .checked_add(allocation.bytes())
            .ok_or(Error::Unavailable)?;
    }
    Ok(total)
}

impl ActiveCatalogSnapshot {
    /// Reserves census first, then the exact selected immutable catalog/IR and
    /// group-schema capacities before ordinary checked resolution can clone.
    /// This does not activate a catalog, read storage, or claim engine affinity.
    #[doc(hidden)]
    pub fn capture_projection_source_admitted_v1<'catalog>(
        &'catalog self,
        identity: &ProjectionIdentity,
        budget: &ProjectionSourceWorkingBudgetV1,
    ) -> Result<ProjectionSourcePlanCaptureV1<'catalog>, Error> {
        let mut census = budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .map_err(|_| Error::Unavailable)?;
        let (_, _, selected) = select_projection_from_lineage(self.lineage_proof(), identity)
            .map_err(|_| Error::Integrity)?;
        // One retained catalog graph, one new ProjectionPlan clone and one new
        // bound group-schema clone. Identity/lineage Arc clones allocate no new
        // backing storage; their original graph is included in catalog capacity.
        let mut scope = census.inspection_scope().map_err(|_| Error::Unavailable)?;
        let bytes = retained_capacity(self, scope.reborrow())?
            .checked_add(retained_capacity(selected, scope.reborrow())?)
            .ok_or(Error::Unavailable)?
            .checked_add(retained_capacity(
                selected.group_schema(),
                scope.reborrow(),
            )?)
            .ok_or(Error::Unavailable)?;
        let work = budget.reserve(bytes).map_err(|_| Error::Unavailable)?;
        let plan = self
            .resolve_projection(identity)
            .map_err(|_| Error::Integrity)?;
        let schema = plan.checked_group_schema().map_err(|_| Error::Integrity)?;
        if schema.identity() != plan.identity() {
            return Err(Error::Integrity);
        }
        drop(census);
        Ok(ProjectionSourcePlanCaptureV1 {
            plan,
            schema,
            catalog: self,
            budget: budget.clone(),
            _work: work,
        })
    }
}

#[cfg(test)]
#[path = "projection_source_capture_tests.rs"]
mod tests;
