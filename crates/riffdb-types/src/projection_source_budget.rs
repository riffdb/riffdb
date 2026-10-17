//! A distinct source-worker ledger using the existing ownership accountant.
//! It never attaches to an exact-provider registry or borrows its 1-GiB budget.

use super::{
    BudgetObservation, BudgetUnavailable, PreparedSharedPhysicalOwnership, ProviderAllocation,
    ProviderBudget, Reservation,
};
use std::{ops::Deref, sync::Arc};

/// Compiled aggregate incremental memory limit for one source worker/database.
pub const PROJECTION_SOURCE_WORKING_MEMORY_BYTES: u64 = 64 * 1024 * 1024;

const SOURCE_ACCOUNTANT_BYTES: u64 = 512;

/// Source-worker accounting owner. Native worker admission must first establish
/// the single worker/database; cloning shares this exact ledger. Construction
/// charges its bounded accountant allocation before admitting any data.
#[derive(Clone)]
pub struct ProjectionSourceWorkingBudgetV1(ProviderBudget);

impl Default for ProjectionSourceWorkingBudgetV1 {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionSourceWorkingBudgetV1 {
    /// Creates a separate fixed-limit ledger, never a provider budget handle.
    #[must_use]
    pub fn new() -> Self {
        Self(ProviderBudget::with_limit_and_owner(
            PROJECTION_SOURCE_WORKING_MEMORY_BYTES,
            SOURCE_ACCOUNTANT_BYTES,
        ))
    }

    /// Admits the full simultaneous capacity/overhead bound before allocation.
    pub fn reserve(
        &self,
        bytes: u64,
    ) -> Result<ProjectionSourceWorkReservationV1, BudgetUnavailable> {
        self.0.reserve(bytes).map(ProjectionSourceWorkReservationV1)
    }

    /// Conservative overlap for the fixed accountant physically retained by
    /// an exact-provider graph. This grants no extra source-work capacity.
    #[doc(hidden)]
    pub fn provider_control_ownership_needed(
        &self,
        budget: &ProviderBudget,
    ) -> Result<u64, BudgetUnavailable> {
        if Arc::ptr_eq(&self.0.0, &budget.0) {
            return Err(BudgetUnavailable);
        }
        {
            let accounting = budget.0.lock().map_err(|_| BudgetUnavailable)?;
            if accounting.refused || accounting.limit != super::EXACT_PROVIDER_BUDGET_BYTES {
                return Err(BudgetUnavailable);
            }
        }
        self.0
            .0
            .provider_control
            .provider_ownership_needed(SOURCE_ACCOUNTANT_BYTES, budget)
    }

    /// Whole-plan preparation only; cancellation attaches no foreign owner.
    /// The cell lives with the actual accountant Arc, not the engine handle.
    #[doc(hidden)]
    pub fn prepare_provider_control_ownership_from(
        &self,
        work: &mut Reservation,
        budget: &ProviderBudget,
    ) -> Result<PreparedSharedPhysicalOwnership<'_>, BudgetUnavailable> {
        if Arc::ptr_eq(&self.0.0, &budget.0) {
            return Err(BudgetUnavailable);
        }
        {
            let accounting = budget.0.lock().map_err(|_| BudgetUnavailable)?;
            if accounting.refused || accounting.limit != super::EXACT_PROVIDER_BUDGET_BYTES {
                return Err(BudgetUnavailable);
            }
        }
        self.0.0.provider_control.prepare_provider_ownership_from(
            SOURCE_ACCOUNTANT_BYTES,
            work,
            budget,
        )
    }

    /// Fixed numeric control observation; never data or publication authority.
    #[doc(hidden)]
    pub fn provider_control_owned_bytes(&self) -> Result<u64, BudgetUnavailable> {
        self.0.0.provider_control.owned_bytes()
    }

    /// One move-only maintenance-worker claim for this actual source ledger.
    /// Native role checks and its existing drained process lifecycle are still
    /// required; this claim alone grants no database mutation authority.
    #[doc(hidden)]
    pub fn try_acquire_worker(
        &self,
    ) -> Result<ProjectionSourceWorkerOwnershipV1, BudgetUnavailable> {
        let mut accounting = self.0.0.lock().map_err(|_| BudgetUnavailable)?;
        if accounting.refused || accounting.source_worker_claimed {
            return Err(BudgetUnavailable);
        }
        accounting.source_worker_claimed = true;
        Ok(ProjectionSourceWorkerOwnershipV1 {
            budget: self.clone(),
        })
    }

    /// Bounded counters only; no paths, source keys or payload values.
    pub fn observation(&self) -> Result<BudgetObservation, BudgetUnavailable> {
        self.0.observation()
    }
}

/// Move-only capacity admission for source work. Its private representation
/// prevents mixing it with provider reservations or native locator ownership.
pub struct ProjectionSourceWorkReservationV1(Reservation);

impl ProjectionSourceWorkReservationV1 {
    /// Already admitted allowance; never permission to exceed this capacity.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.0.bytes
    }

    /// Transfers a nonzero portion without acquiring new allowance.
    pub fn split(&mut self, bytes: u64) -> Result<Self, BudgetUnavailable> {
        self.0.split(bytes).map(Self)
    }

    /// Combines only unpublished allowances from this same worker ledger.
    pub fn absorb(&mut self, other: Self) -> Result<(), BudgetUnavailable> {
        self.0.absorb(other.0)
    }

    /// Settles data constructed under this prior admission. The bound must
    /// already include construction and the existing census scratch. Inspection
    /// confirms retained ownership; it never substitutes for prior admission.
    /// Shared catalog/native inputs require their own complete owner and are
    /// refused by this wholly-owned helper.
    pub fn settle_owned_value<T: crate::InspectOwnedCapacityV1>(
        self,
        value: T,
    ) -> Result<ProjectionSourceWorkAllocationV1<T>, BudgetUnavailable> {
        self.0
            .settle_owned_value(value)
            .map(ProjectionSourceWorkAllocationV1)
    }

    /// Settles freshly constructed semantic values containing shared typed
    /// identity data. Actual allocation lifecycles retain their source charge
    /// until final destruction. Existing ownership in a foreign ledger refuses;
    /// no identity allocation may be omitted, reassigned or borrowed silently.
    /// Construction and graph-census capacity must be admitted beforehand.
    pub fn settle_shared_value<T: crate::InspectOwnedCapacityV1>(
        self,
        value: T,
    ) -> Result<ProjectionSourceWorkAllocationV1<T>, BudgetUnavailable> {
        self.0
            .settle_shared_value(value)
            .map(ProjectionSourceWorkAllocationV1)
    }
}

/// Read-only shared data whose complete conservative charge lasts until the
/// final physical reference drops. The existing allocator drops data first.
pub struct ProjectionSourceWorkAllocationV1<T>(Arc<ProviderAllocation<T>>);

impl<T> Clone for ProjectionSourceWorkAllocationV1<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> ProjectionSourceWorkAllocationV1<T> {
    /// Accounting identity only; grants no source or publication authority.
    #[must_use]
    pub fn belongs_to(&self, budget: &ProjectionSourceWorkingBudgetV1) -> bool {
        self.0.belongs_to(&budget.0)
    }
}

impl<T> Deref for ProjectionSourceWorkAllocationV1<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A single worker's nondurable accounting ownership. It cannot be cloned or
/// converted into a command, writer lease, source proof or publication right.
#[doc(hidden)]
pub struct ProjectionSourceWorkerOwnershipV1 {
    budget: ProjectionSourceWorkingBudgetV1,
}

impl Drop for ProjectionSourceWorkerOwnershipV1 {
    fn drop(&mut self) {
        let mut accounting = match self.budget.0.0.lock() {
            Ok(accounting) => accounting,
            Err(error) => {
                let mut accounting = error.into_inner();
                accounting.refused = true;
                accounting
            }
        };
        if accounting.source_worker_claimed {
            accounting.source_worker_claimed = false;
        } else {
            accounting.refused = true;
        }
    }
}
#[cfg(test)]
#[path = "projection_source_budget_tests.rs"]
mod tests;
