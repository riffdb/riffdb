//! A read-only opaque checked-plan view. It is not engine or input authority.
use riffdb_catalog::ProjectionSourcePlanCaptureV1;
use riffdb_storage_api::{ActiveCatalogPointerV1, CheckedProjectionSchema};
use riffdb_types::{ProjectionIdentity, ProjectionSourceWorkingBudgetV1};

/// Retains the genuine checked catalog/plan owner while exposing no IR to native.
/// Native must independently compare its captured pointer and actual ledger.
#[doc(hidden)]
pub struct SourceDriverPlan<'plan> {
    pub(super) captured: &'plan ProjectionSourcePlanCaptureV1<'plan>,
}
impl<'plan> SourceDriverPlan<'plan> {
    /// Wraps an existing admitted checked owner; this grants no source authority.
    #[must_use]
    pub fn new(captured: &'plan ProjectionSourcePlanCaptureV1<'plan>) -> Self {
        Self { captured }
    }
    /// Exact immutable active pointer to compare with the native capture.
    #[must_use]
    pub fn catalog_pointer(&self) -> &ActiveCatalogPointerV1 {
        self.captured.catalog_pointer()
    }
    /// IR-blind compiled group/measure schema, borrowed from the retained owner.
    #[must_use]
    pub fn schema(&self) -> &CheckedProjectionSchema {
        self.captured.schema()
    }
    /// Exact projection identity; no caller-selected source locator.
    #[must_use]
    pub fn identity(&self) -> &ProjectionIdentity {
        self.captured.schema().identity()
    }
    /// Accounting affinity, never independently an engine/source proof.
    #[must_use]
    pub fn belongs_to(&self, budget: &ProjectionSourceWorkingBudgetV1) -> bool {
        self.captured.belongs_to(budget)
    }
    /// Retained actual source ledger; the adapter never constructs another one.
    #[must_use]
    pub fn originating_budget(&self) -> &ProjectionSourceWorkingBudgetV1 {
        self.captured.originating_budget()
    }
}
impl Drop for SourceDriverPlan<'_> {
    fn drop(&mut self) {}
}
