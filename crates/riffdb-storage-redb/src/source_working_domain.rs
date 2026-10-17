//! One independent source-work ledger retained by each actual native engine.
//! Snapshot cursors borrow it; they never create their own 64-MiB allowance.

use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::exact_provider_budget_v1::{
    ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
};

pub(crate) struct SourceWorkingDomain {
    budget: ProjectionSourceWorkingBudgetV1,
    // Admitted before the enclosing EngineOwner allocation. This is bounded
    // inline control, not a second heap-allocated domain or an engine pin.
    _control: ProjectionSourceWorkReservationV1,
}

impl SourceWorkingDomain {
    pub(crate) fn try_new() -> Result<Self, StorageError> {
        // This constructor already charges the accountant's fixed 512 bytes.
        let budget = ProjectionSourceWorkingBudgetV1::new();
        let control_bytes = u64::try_from(std::mem::size_of::<Self>())
            .ok()
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or_else(unavailable)?;
        let control = budget.reserve(control_bytes).map_err(|_| unavailable())?;
        Ok(Self {
            budget,
            _control: control,
        })
    }

    pub(crate) fn budget(&self) -> &ProjectionSourceWorkingBudgetV1 {
        &self.budget
    }
}

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
