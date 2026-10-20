//! Startup numerical continuity only; no native origin or Source completion.
//! The configured maintenance constructor consumes the sole public bootstrap.
use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::OwnedCapacitySharedV1;
use riffdb_types::exact_provider_budget_v1::{
    PreparedSharedPhysicalOwnership, ProjectionSourceWorkReservationV1,
    ProjectionSourceWorkingBudgetV1, ProviderBudget, Reservation, SharedPhysicalOwnershipCell,
};
use std::sync::Mutex;

/// Fixed-policy numerical startup funding. This creates no engine, input,
/// backend brand, command, candidate, stage, write capability or completion.
/// The daemon creates one per configured database before its runtime callbacks.
#[doc(hidden)]
pub struct SourceExecutionBootstrapV1 {
    domain: ConfiguredSourceExecutionDomainV1,
}
impl SourceExecutionBootstrapV1 {
    /// Creates the existing fixed Source ledger, with no caller-selected limit.
    pub fn try_new() -> Result<Self, StorageError> {
        let budget = ProjectionSourceWorkingBudgetV1::new();
        let control = budget
            .reserve(allocation_bound::<DomainData>()?)
            .map_err(|_| unavailable())?;
        let prefix_pins = crate::source_prefix_pin::SourcePrefixPins::new(&budget)?;
        Ok(Self {
            domain: ConfiguredSourceExecutionDomainV1(OwnedCapacitySharedV1::new(DomainData {
                state: Mutex::new(DomainState {
                    next_serial: 1,
                    active: None,
                    ordinary_derived_constructors: 0,
                    refused: false,
                }),
                prefix_pins,
                provider: SharedPhysicalOwnershipCell::new(),
                budget,
                _control: control,
            })),
        })
    }
    /// Numerical admission only, for the daemon's fixed compiled callback quote.
    /// It cannot bind a caller-selected engine or grant Source authority.
    pub fn numerical_budget(&self) -> &ProjectionSourceWorkingBudgetV1 {
        self.domain.budget()
    }
    pub(crate) fn into_configured(self) -> ConfiguredSourceExecutionDomainV1 {
        self.domain
    }
}

#[derive(Clone)]
pub(crate) struct ConfiguredSourceExecutionDomainV1(OwnedCapacitySharedV1<DomainData>);
struct DomainData {
    state: Mutex<DomainState>,
    prefix_pins: crate::source_prefix_pin::SourcePrefixPins,
    provider: SharedPhysicalOwnershipCell,
    budget: ProjectionSourceWorkingBudgetV1,
    // Closed shared shell retires before this concrete payload/charge drops.
    _control: ProjectionSourceWorkReservationV1,
}
struct DomainState {
    next_serial: u64,
    active: Option<u64>,
    ordinary_derived_constructors: u64,
    refused: bool,
}
impl ConfiguredSourceExecutionDomainV1 {
    pub(crate) fn budget(&self) -> &ProjectionSourceWorkingBudgetV1 {
        &self.0.budget
    }
    pub(crate) fn prefix_pins(&self) -> &crate::source_prefix_pin::SourcePrefixPins {
        &self.0.prefix_pins
    }
    pub(crate) fn same_domain_as(&self, other: &Self) -> bool {
        self.0.same_allocation(&other.0)
    }
    pub(crate) fn require_idle(&self) -> Result<(), StorageError> {
        let state = self.0.state.lock().map_err(|_| unavailable())?;
        if state.refused || state.active.is_some() || state.ordinary_derived_constructors != 0 {
            return Err(unavailable());
        }
        Ok(())
    }
    pub(crate) fn begin_ordinary_derived_construction(
        &self,
    ) -> Result<OrdinaryDerivedConstructionV1, StorageError> {
        let mut state = self.0.state.lock().map_err(|_| unavailable())?;
        if state.refused || state.active.is_some() {
            return Err(unavailable());
        }
        let next = state
            .ordinary_derived_constructors
            .checked_add(1)
            .ok_or_else(unavailable)?;
        state.ordinary_derived_constructors = next;
        Ok(OrdinaryDerivedConstructionV1 {
            domain: self.clone(),
        })
    }

    pub(crate) fn begin_operation(
        &self,
        maintenance_lock: SourceMaintenanceLockV1,
    ) -> Result<SourceOperationClaimsV1, StorageError> {
        if !maintenance_lock.belongs_to(self) {
            return Err(unavailable());
        }
        // Before allocation or state installation: the same original pool pays
        // for the concrete closed claim shell and both inline alias facades.
        let control = self
            .budget()
            .reserve(allocation_bound::<ClaimData>()?)
            .map_err(|_| unavailable())?;
        let mut state = self.0.state.lock().map_err(|_| unavailable())?;
        if state.refused || state.active.is_some() || state.ordinary_derived_constructors != 0 {
            return Err(unavailable());
        }
        let serial = state.next_serial;
        let next = serial.checked_add(1).ok_or_else(unavailable)?;
        let claim = OwnedCapacitySharedV1::new(ClaimData {
            maintenance_lock: Some(maintenance_lock),
            domain: self.clone(),
            serial,
            _control: control,
        });
        state.next_serial = next;
        state.active = Some(serial);
        // The operation and physical task-retirement owner are distinct aliases
        // of this SAME claim. Final native/semantic disposal alone cannot clear
        // the configured slot while its task/header remains physically live.
        Ok(SourceOperationClaimsV1 {
            semantic: SourceOperationClaimV1(claim.clone()),
            task: SourceOperationTaskRetirementV1 { _claim: claim },
        })
    }
    pub(crate) fn provider_ownership_needed(
        &self,
        budget: &ProviderBudget,
    ) -> Result<u64, StorageError> {
        self.0
            .provider
            .provider_ownership_needed(self.0._control.bytes(), budget)
            .map_err(|_| unavailable())
    }
    pub(crate) fn prepare_provider_ownership_from<'a>(
        &'a self,
        work: &mut Reservation,
        budget: &ProviderBudget,
    ) -> Result<PreparedSharedPhysicalOwnership<'a>, StorageError> {
        self.0
            .provider
            .prepare_provider_ownership_from(self.0._control.bytes(), work, budget)
            .map_err(|_| unavailable())
    }
    #[cfg(test)]
    pub(crate) fn provider_charge_bytes(&self) -> u64 {
        self.0.provider.owned_bytes().unwrap()
    }
}

// Fields-private, non-Clone exclusion for a real ordinary derived constructor.
// Acquisition and Source entry use the same DomainState mutex; neither keeps
// that mutex locked across native I/O or waits for the other operation.
pub(crate) struct OrdinaryDerivedConstructionV1 {
    domain: ConfiguredSourceExecutionDomainV1,
}
impl Drop for OrdinaryDerivedConstructionV1 {
    fn drop(&mut self) {
        let mut state = match self.domain.0.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.refused = true;
                state
            }
        };
        if let Some(next) = state.ordinary_derived_constructors.checked_sub(1) {
            state.ordinary_derived_constructors = next;
        } else {
            state.refused = true;
        }
    }
}

pub(crate) struct SourceOperationClaimsV1 {
    pub(crate) semantic: SourceOperationClaimV1,
    pub(crate) task: SourceOperationTaskRetirementV1,
}
pub(crate) struct SourceOperationClaimV1(OwnedCapacitySharedV1<ClaimData>);
// Closed, allocation-free numerical custody of the SAME live ClaimData. It has
// no storage operation, raw owner, serial getter or semantic Source authority.
pub(crate) struct SourceOperationPhysicalCustodyV1(OwnedCapacitySharedV1<ClaimData>);
impl SourceOperationPhysicalCustodyV1 {
    pub(crate) fn try_share(&self) -> Result<Self, StorageError> {
        self.require_current()?;
        Ok(Self(self.0.clone()))
    }
    pub(crate) fn same_operation_as(&self, other: &Self) -> bool {
        self.0.same_allocation(&other.0)
    }
    pub(crate) fn require_current(&self) -> Result<(), StorageError> {
        let state = self.0.domain.0.state.lock().map_err(|_| unavailable())?;
        if state.refused || state.active != Some(self.0.serial) {
            return Err(unavailable());
        }
        Ok(())
    }
}
/// Opaque physical task tail. No Clone, raw shared owner, native authority or
/// explicit release API: the complete worker retains it until actual retirement.
#[doc(hidden)]
pub struct SourceOperationTaskRetirementV1 {
    _claim: OwnedCapacitySharedV1<ClaimData>,
}
struct ClaimData {
    maintenance_lock: Option<SourceMaintenanceLockV1>,
    domain: ConfiguredSourceExecutionDomainV1,
    serial: u64,
    _control: ProjectionSourceWorkReservationV1,
}
impl SourceOperationClaimV1 {
    pub(crate) fn retain_physical_custody(
        &self,
    ) -> Result<SourceOperationPhysicalCustodyV1, StorageError> {
        self.require_current()?;
        Ok(SourceOperationPhysicalCustodyV1(self.0.clone()))
    }
    pub(crate) fn domain(&self) -> &ConfiguredSourceExecutionDomainV1 {
        &self.0.domain
    }
    pub(crate) fn belongs_to(&self, domain: &ConfiguredSourceExecutionDomainV1) -> bool {
        self.0.domain.0.same_allocation(&domain.0)
    }
    pub(crate) fn require_current(&self) -> Result<(), StorageError> {
        let state = self.0.domain.0.state.lock().map_err(|_| unavailable())?;
        if state.refused || state.active != Some(self.0.serial) {
            return Err(unavailable());
        }
        Ok(())
    }
}
impl Drop for ClaimData {
    fn drop(&mut self) {
        // This value is obtained only after the closed strong shell retired.
        // No caller can copy a serial or manufacture another claim owner.
        drop(self.maintenance_lock.take());
        let mut state = self
            .domain
            .0
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if state.active == Some(self.serial) {
            state.active = None;
        } else {
            state.refused = true;
        }
    }
}

// The SAME inherited lock is promoted once. Never create an independently
// unlocking LegacyFileCustody from File::try_clone: either Drop would unlock it.
pub(crate) struct ConfiguredMaintenanceLockV1(MaintenanceLockKind);
enum MaintenanceLockKind {
    Original(Option<crate::native_file_custody::LegacyFileCustody>),
    Source(OwnedCapacitySharedV1<SourceMaintenanceLockData>),
}
pub(crate) struct SourceMaintenanceLockV1(OwnedCapacitySharedV1<SourceMaintenanceLockData>);
impl SourceMaintenanceLockV1 {
    fn belongs_to(&self, domain: &ConfiguredSourceExecutionDomainV1) -> bool {
        self.0.budget.shares_owner_with(domain.budget())
    }
}
struct SourceMaintenanceLockData {
    lock: crate::native_file_custody::LegacyFileCustody,
    budget: ProjectionSourceWorkingBudgetV1,
    _control: ProjectionSourceWorkReservationV1,
}
impl ConfiguredMaintenanceLockV1 {
    pub(crate) fn original(lock: crate::native_file_custody::LegacyFileCustody) -> Self {
        Self(MaintenanceLockKind::Original(Some(lock)))
    }
    pub(crate) fn file(&self) -> Result<&std::fs::File, StorageError> {
        match &self.0 {
            MaintenanceLockKind::Original(Some(lock)) => Ok(lock.file()),
            MaintenanceLockKind::Source(owner) => Ok(owner.lock.file()),
            MaintenanceLockKind::Original(None) => Err(unavailable()),
        }
    }
    pub(crate) fn retain_for_source(
        &mut self,
        domain: &ConfiguredSourceExecutionDomainV1,
    ) -> Result<SourceMaintenanceLockV1, StorageError> {
        if let MaintenanceLockKind::Source(owner) = &self.0 {
            if !owner.budget.shares_owner_with(domain.budget()) {
                return Err(unavailable());
            }
            return Ok(SourceMaintenanceLockV1(owner.clone()));
        }
        // Preadmit BEFORE taking the actual inherited lock or allocating its
        // closed shell. All failure paths before take preserve the original.
        let control = domain
            .budget()
            .reserve(allocation_bound::<SourceMaintenanceLockData>()?)
            .map_err(|_| unavailable())?;
        let MaintenanceLockKind::Original(slot) = &mut self.0 else {
            return Err(unavailable());
        };
        let original = slot.take().ok_or_else(unavailable)?;
        let owner = OwnedCapacitySharedV1::new(SourceMaintenanceLockData {
            lock: original,
            budget: domain.budget().clone(),
            _control: control,
        });
        let retained = SourceMaintenanceLockV1(owner.clone());
        self.0 = MaintenanceLockKind::Source(owner);
        Ok(retained)
    }
}
fn allocation_bound<T>() -> Result<u64, StorageError> {
    crate::source_prefix_pin::allocation_bound::<T>()
}
fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

// Diagnostic-only observation; retains domain, never operation/lock custody.
#[cfg(test)]
pub(crate) struct SourceOperationIdleProbeV1(ConfiguredSourceExecutionDomainV1);
#[cfg(test)]
impl SourceOperationIdleProbeV1 {
    pub(crate) fn require_idle(&self) -> Result<(), StorageError> {
        self.0.require_idle()
    }
}
#[cfg(test)]
impl SourceOperationClaimV1 {
    pub(crate) fn idle_probe_for_test(&self) -> SourceOperationIdleProbeV1 {
        SourceOperationIdleProbeV1(self.0.domain.clone())
    }
}
