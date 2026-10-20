//! One Source ledger shared by the primary and its paired derived engines.
//! Snapshot cursors borrow it; they never create their own 64-MiB allowance.

use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::exact_provider_budget_v1::{
    ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
};

pub(crate) struct SourceWorkingDomain {
    budget: ProjectionSourceWorkingBudgetV1,
    usage: crate::source_usage_domain::SourceUsageDomain,
    prefix_pins: crate::source_prefix_pin::SourcePrefixPins,
    execution: Option<crate::source_execution_domain::ConfiguredSourceExecutionDomainV1>,
    // The configured domain alone does not retain its active operation/lock.
    // This SAME closed ClaimData alias follows every actual native read alias.
    operation: Option<crate::source_execution_domain::SourceOperationPhysicalCustodyV1>,
    paired_derived: bool,
    // Admitted before the enclosing EngineOwner allocation. This is bounded
    // inline control, not a second heap-allocated domain or an engine pin.
    _control: ProjectionSourceWorkReservationV1,
}

impl SourceWorkingDomain {
    fn control_bytes() -> Result<u64, StorageError> {
        u64::try_from(std::mem::size_of::<Self>())
            .ok()
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or_else(unavailable)
    }

    pub(crate) fn try_new() -> Result<Self, StorageError> {
        // This constructor already charges the accountant's fixed 512 bytes.
        let budget = ProjectionSourceWorkingBudgetV1::new();
        let control_bytes = Self::control_bytes()?;
        let control = budget.reserve(control_bytes).map_err(|_| unavailable())?;
        let prefix_pins = crate::source_prefix_pin::SourcePrefixPins::new(&budget)?;
        Ok(Self {
            budget,
            usage: crate::source_usage_domain::SourceUsageDomain::new_unavailable(),
            prefix_pins,
            execution: None,
            operation: None,
            paired_derived: false,
            _control: control,
        })
    }

    pub(crate) fn from_configured(
        domain: &crate::source_execution_domain::ConfiguredSourceExecutionDomainV1,
        operation: Option<&crate::source_execution_domain::SourceOperationClaimV1>,
    ) -> Result<Self, StorageError> {
        match operation {
            Some(claim) => {
                claim.require_current()?;
                if !claim.belongs_to(domain) {
                    return Err(unavailable());
                }
            }
            None => domain.require_idle()?,
        }
        let control_bytes = Self::control_bytes()?;
        let control = domain
            .budget()
            .reserve(control_bytes)
            .map_err(|_| unavailable())?;
        Ok(Self {
            budget: domain.budget().clone(),
            usage: crate::source_usage_domain::SourceUsageDomain::new_unavailable(),
            prefix_pins: domain.prefix_pins().clone(),
            execution: Some(domain.clone()),
            operation: operation
                .map(|claim| claim.retain_physical_custody())
                .transpose()?,
            paired_derived: false,
            _control: control,
        })
    }
    // Inherit an already-owned paired domain, never a new Source operation.
    // Only the closed ordinary construction guard calls this helper.
    pub(super) fn try_share(&self) -> Result<Self, StorageError> {
        let control_bytes = Self::control_bytes()?;
        let control = self
            .budget
            .reserve(control_bytes)
            .map_err(|_| unavailable())?;
        Ok(Self {
            budget: self.budget.clone(),
            usage: crate::source_usage_domain::SourceUsageDomain::new_unavailable(),
            prefix_pins: self.prefix_pins.clone(),
            execution: self.execution.clone(),
            operation: self
                .operation
                .as_ref()
                .map(|actual| actual.try_share())
                .transpose()?,
            paired_derived: true,
            _control: control,
        })
    }

    pub(crate) const fn is_paired_derived(&self) -> bool {
        self.paired_derived
    }

    pub(super) fn begin_ordinary_derived_construction(
        &self,
    ) -> Result<SourceOrdinaryDerivedEntryV1, StorageError> {
        let execution = self
            .execution
            .as_ref()
            .map(|domain| domain.begin_ordinary_derived_construction())
            .transpose()?;
        let worker = self
            .budget
            .try_acquire_worker()
            .map_err(|_| unavailable())?;
        Ok(SourceOrdinaryDerivedEntryV1 {
            _worker: worker,
            _execution: execution,
        })
    }

    pub(crate) fn provider_execution_ownership_needed(
        &self,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<u64, StorageError> {
        self.execution
            .as_ref()
            .map_or(Ok(0), |domain| domain.provider_ownership_needed(budget))
    }
    pub(crate) fn prepare_provider_execution_ownership_from<'a>(
        &'a self,
        work: &mut riffdb_types::exact_provider_budget_v1::Reservation,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<
        Option<riffdb_types::exact_provider_budget_v1::PreparedSharedPhysicalOwnership<'a>>,
        StorageError,
    > {
        self.execution
            .as_ref()
            .map(|domain| domain.prepare_provider_ownership_from(work, budget))
            .transpose()
    }
    #[cfg(test)]
    pub(crate) fn provider_execution_charge_bytes(&self) -> u64 {
        self.execution
            .as_ref()
            .map_or(0, |domain| domain.provider_charge_bytes())
    }

    pub(crate) fn prefix_pins(&self) -> &crate::source_prefix_pin::SourcePrefixPins {
        &self.prefix_pins
    }

    pub(crate) fn require_same_operation(
        &self,
        actual: &crate::source_execution_domain::SourceOperationPhysicalCustodyV1,
    ) -> Result<(), StorageError> {
        let operation = self.operation.as_ref().ok_or_else(unavailable)?;
        operation.require_current()?;
        actual.require_current()?;
        if !operation.same_operation_as(actual) {
            return Err(unavailable());
        }
        Ok(())
    }
    pub(crate) fn usage(&self) -> &crate::source_usage_domain::SourceUsageDomain {
        &self.usage
    }

    pub(crate) fn budget(&self) -> &ProjectionSourceWorkingBudgetV1 {
        &self.budget
    }
}

// No native state, engine back-reference, heap allocation or Source authority.
// The configured gate stays closed until the numeric worker claim retires.
pub(super) struct SourceOrdinaryDerivedEntryV1 {
    _worker: riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkerOwnershipV1,
    _execution: Option<crate::source_execution_domain::OrdinaryDerivedConstructionV1>,
}

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
