//! Checked numeric ownership for one physically shared allocation family.
//! No data, graph traversal or application authority is held here.
// req: OQ-040

use super::{BudgetUnavailable, ProviderBudget, Reservation};
use std::sync::Arc;

/// One variable byte balance whose individual physical members release fragments.
/// Store after its represented family control; all mutations require the native
/// family's existing exclusive guard. No member list or predecessor is retained.
#[doc(hidden)]
pub struct PhysicalOwnershipBalance {
    budget: ProviderBudget,
    bytes: u64,
}

impl PhysicalOwnershipBalance {
    pub fn belongs_to(&self, budget: &ProviderBudget) -> bool {
        Arc::ptr_eq(&self.budget.0, &budget.0)
    }

    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Prepares privately owned bytes without changing this receiver. Refusal or
    /// cancellation leaves it unchanged; commit later performs only assignment.
    pub fn prepare_increment(
        &mut self,
        mut work: Reservation,
    ) -> Result<PreparedPhysicalOwnershipIncrement<'_>, BudgetUnavailable> {
        if !work.live || work.bytes == 0 || !self.belongs_to(&work.budget) {
            return Err(BudgetUnavailable);
        }
        let next = self
            .bytes
            .checked_add(work.bytes)
            .ok_or(BudgetUnavailable)?;
        let bytes = work.bytes;
        let mut accounting = self.budget.0.lock().map_err(|_| BudgetUnavailable)?;
        if accounting.refused {
            return Err(BudgetUnavailable);
        }
        let counters = accounting.counters;
        let reserved = counters
            .reserved_bytes
            .checked_sub(bytes)
            .ok_or(BudgetUnavailable)?;
        let reservations = counters
            .reservations
            .checked_sub(1)
            .ok_or(BudgetUnavailable)?;
        let owned = counters
            .owned_bytes
            .checked_add(bytes)
            .ok_or(BudgetUnavailable)?;
        accounting.counters.reserved_bytes = reserved;
        accounting.counters.reservations = reservations;
        accounting.counters.owned_bytes = owned;
        // This is an increment to the same accounting owner, not another live
        // allocation owner. Failed preparation releases only its own bytes.
        work.live = false;
        drop(accounting);
        Ok(PreparedPhysicalOwnershipIncrement {
            balance: self,
            bytes,
            next,
            live: true,
        })
    }

    /// Called only at actual physical member destruction, never logical removal.
    /// A failure preserves the charge and permanently refuses new accounting.
    pub fn release_fragment(&mut self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut accounting = self
            .budget
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match (
            self.bytes.checked_sub(bytes),
            accounting.counters.owned_bytes.checked_sub(bytes),
        ) {
            (Some(remaining), Some(owned)) => {
                self.bytes = remaining;
                accounting.counters.owned_bytes = owned;
            }
            _ => accounting.refused = true,
        }
    }
}

impl Reservation {
    /// Turns a whole admitted allowance into a PRIVATE physical-family balance.
    /// Attach only after every other whole-plan piece has been prepared. Drop
    /// before attachment rolls its complete numerical ownership back.
    pub fn commit_physical_balance(
        mut self,
    ) -> Result<PhysicalOwnershipBalance, BudgetUnavailable> {
        if !self.live || self.bytes == 0 {
            return Err(BudgetUnavailable);
        }
        let mut accounting = self.budget.0.lock().map_err(|_| BudgetUnavailable)?;
        if accounting.refused {
            return Err(BudgetUnavailable);
        }
        let counters = accounting.counters;
        let reserved = counters
            .reserved_bytes
            .checked_sub(self.bytes)
            .ok_or(BudgetUnavailable)?;
        let reservations = counters
            .reservations
            .checked_sub(1)
            .ok_or(BudgetUnavailable)?;
        let owned = counters
            .owned_bytes
            .checked_add(self.bytes)
            .ok_or(BudgetUnavailable)?;
        let allocations = counters
            .allocations
            .checked_add(1)
            .ok_or(BudgetUnavailable)?;
        accounting.counters.reserved_bytes = reserved;
        accounting.counters.reservations = reservations;
        accounting.counters.owned_bytes = owned;
        accounting.counters.allocations = allocations;
        self.live = false;
        drop(accounting);
        Ok(PhysicalOwnershipBalance {
            budget: self.budget.clone(),
            bytes: self.bytes,
        })
    }
}

/// Holds a private increment while its receiver remains exclusively borrowed.
/// Its final commit cannot allocate, lock or refuse after another plan piece
/// becomes visible. No caller can apply it to another balance or budget.
#[doc(hidden)]
pub struct PreparedPhysicalOwnershipIncrement<'a> {
    balance: &'a mut PhysicalOwnershipBalance,
    bytes: u64,
    next: u64,
    live: bool,
}

impl PreparedPhysicalOwnershipIncrement<'_> {
    pub fn commit(mut self) {
        self.balance.bytes = self.next;
        self.live = false;
    }
}

impl Drop for PreparedPhysicalOwnershipIncrement<'_> {
    fn drop(&mut self) {
        if self.live {
            let mut accounting = self
                .balance
                .budget
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match accounting.counters.owned_bytes.checked_sub(self.bytes) {
                Some(owned) => accounting.counters.owned_bytes = owned,
                None => accounting.refused = true,
            }
        }
    }
}

impl Drop for PhysicalOwnershipBalance {
    fn drop(&mut self) {
        let mut accounting = self
            .budget
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let counters = accounting.counters;
        match (
            counters.owned_bytes.checked_sub(self.bytes),
            counters.allocations.checked_sub(1),
            counters.released_allocations.checked_add(1),
        ) {
            (Some(owned), Some(allocations), Some(released)) => {
                accounting.counters.owned_bytes = owned;
                accounting.counters.allocations = allocations;
                accounting.counters.released_allocations = released;
            }
            _ => accounting.refused = true,
        }
    }
}

#[cfg(test)]
#[path = "exact_provider_physical_ownership_tests.rs"]
mod tests;

/// One bounded numerical ledger for allocations shared by immutable successors.
/// It owns no data, source or member list. Callers admit the actual control
/// allocation before constructing it; physical members alone retain its Arc.
#[doc(hidden)]
pub struct PhysicalOwnershipFamily {
    state: std::sync::Mutex<PhysicalOwnershipFamilyState>,
    control_bytes: u64,
}

struct PhysicalOwnershipFamilyState {
    enabled: bool,
    next: u64,
    cut: u64,
    uncovered: u64,
    covered: u64,
    marker: Option<Arc<super::OwnershipCharge>>,
    provider: Option<PhysicalOwnershipBalance>,
}

/// A bounded numeric observation, without identities or retained data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct PhysicalOwnershipFamilyObservation {
    pub enabled: bool,
    pub next: u64,
    pub cut: u64,
    pub uncovered: u64,
    pub covered: u64,
}

const PHYSICAL_FAMILY_MARKER_BYTES: u64 = 128;

impl PhysicalOwnershipFamily {
    pub fn new(control_bytes: u64) -> Self {
        Self {
            state: std::sync::Mutex::new(PhysicalOwnershipFamilyState {
                enabled: control_bytes != 0,
                next: 0,
                cut: 0,
                uncovered: 0,
                covered: 0,
                marker: None,
                provider: None,
            }),
            control_bytes,
        }
    }

    pub fn identity(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    pub fn same_family(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }

    pub fn lock(&self) -> Result<PhysicalOwnershipFamilyGuard<'_>, BudgetUnavailable> {
        Ok(PhysicalOwnershipFamilyGuard {
            family: self,
            state: self.state.lock().map_err(|_| BudgetUnavailable)?,
        })
    }

    pub fn observation(&self) -> Result<PhysicalOwnershipFamilyObservation, BudgetUnavailable> {
        let state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        Ok(PhysicalOwnershipFamilyObservation {
            enabled: state.enabled,
            next: state.next,
            cut: state.cut,
            uncovered: state.uncovered,
            covered: state.covered,
        })
    }

    /// Registers before allocating the member. Its caller must release on any
    /// subsequent failure and only after the represented allocation is gone.
    pub fn refuse(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .enabled = false;
    }

    pub fn register_member_bytes(&self, bytes: u64) -> Result<u64, BudgetUnavailable> {
        let mut state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if !state.enabled || bytes == 0 {
            return Err(BudgetUnavailable);
        }
        match (
            state.next.checked_add(1),
            state.uncovered.checked_add(bytes),
        ) {
            (Some(next), Some(uncovered)) => {
                state.next = next;
                state.uncovered = uncovered;
                Ok(next)
            }
            _ => {
                state.enabled = false;
                Err(BudgetUnavailable)
            }
        }
    }

    pub fn release_member_bytes(&self, ordinal: u64, bytes: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if ordinal == 0 || ordinal > state.next || bytes == 0 {
            state.enabled = false;
            return;
        }
        if ordinal <= state.cut {
            match state.covered.checked_sub(bytes) {
                Some(covered) => {
                    if let Some(balance) = &mut state.provider {
                        balance.release_fragment(bytes);
                        state.covered = covered;
                    } else {
                        state.enabled = false;
                    }
                }
                None => state.enabled = false,
            }
        } else {
            match state.uncovered.checked_sub(bytes) {
                Some(uncovered) => state.uncovered = uncovered,
                None => state.enabled = false,
            }
        }
    }

    pub fn register_member(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<PhysicalOwnershipMember, BudgetUnavailable> {
        let ordinal = self.register_member_bytes(bytes)?;
        Ok(PhysicalOwnershipMember {
            family: Arc::clone(self),
            ordinal,
            bytes,
        })
    }
}

/// Move-only numerical member. Keep after represented payload/control fields.
#[doc(hidden)]
pub struct PhysicalOwnershipMember {
    family: Arc<PhysicalOwnershipFamily>,
    ordinal: u64,
    bytes: u64,
}
impl PhysicalOwnershipMember {
    pub fn family(&self) -> &Arc<PhysicalOwnershipFamily> {
        &self.family
    }
}
impl Drop for PhysicalOwnershipMember {
    fn drop(&mut self) {
        self.family.release_member_bytes(self.ordinal, self.bytes);
    }
}

#[doc(hidden)]
pub struct PhysicalOwnershipFamilyGuard<'a> {
    family: &'a PhysicalOwnershipFamily,
    state: std::sync::MutexGuard<'a, PhysicalOwnershipFamilyState>,
}

#[doc(hidden)]
pub struct PreparedPhysicalFamilyAdmission<'a>(PreparedPhysicalFamilyAdmissionInner<'a>);

enum PreparedPhysicalFamilyAdmissionInner<'a> {
    First {
        state: &'a mut PhysicalOwnershipFamilyState,
        balance: PhysicalOwnershipBalance,
        marker: Arc<super::OwnershipCharge>,
        cut: u64,
        covered: u64,
    },
    Increment {
        increment: Option<PreparedPhysicalOwnershipIncrement<'a>>,
        cut: &'a mut u64,
        uncovered: &'a mut u64,
        covered: &'a mut u64,
        next_cut: u64,
        next_covered: u64,
        marker: Arc<super::OwnershipCharge>,
    },
}

impl PreparedPhysicalFamilyAdmission<'_> {
    pub fn marker(&self) -> Arc<super::OwnershipCharge> {
        match &self.0 {
            PreparedPhysicalFamilyAdmissionInner::First { marker, .. }
            | PreparedPhysicalFamilyAdmissionInner::Increment { marker, .. } => Arc::clone(marker),
        }
    }
    pub fn commit(self) -> Arc<super::OwnershipCharge> {
        match self.0 {
            PreparedPhysicalFamilyAdmissionInner::First {
                state,
                balance,
                marker,
                cut,
                covered,
            } => {
                state.provider = Some(balance);
                state.marker = Some(Arc::clone(&marker));
                state.cut = cut;
                state.covered = covered;
                state.uncovered = 0;
                marker
            }
            PreparedPhysicalFamilyAdmissionInner::Increment {
                increment,
                cut,
                uncovered,
                covered,
                next_cut,
                next_covered,
                marker,
            } => {
                if let Some(increment) = increment {
                    increment.commit();
                }
                *cut = next_cut;
                *uncovered = 0;
                *covered = next_covered;
                marker
            }
        }
    }
}

impl PhysicalOwnershipFamilyGuard<'_> {
    pub fn observation(&self) -> PhysicalOwnershipFamilyObservation {
        PhysicalOwnershipFamilyObservation {
            enabled: self.state.enabled,
            next: self.state.next,
            cut: self.state.cut,
            uncovered: self.state.uncovered,
            covered: self.state.covered,
        }
    }
    pub fn provider_owned_bytes(&self) -> u64 {
        self.state
            .provider
            .as_ref()
            .map_or(0, PhysicalOwnershipBalance::bytes)
            + self
                .state
                .marker
                .as_ref()
                .map_or(0, |marker| marker.bytes())
    }
    pub fn provider_ownership_needed(
        &self,
        budget: &ProviderBudget,
    ) -> Result<u64, BudgetUnavailable> {
        if !self.state.enabled {
            return Err(BudgetUnavailable);
        }
        if let Some(marker) = &self.state.marker
            && !marker.belongs_to(budget)
        {
            return Err(BudgetUnavailable);
        }
        if self.state.provider.is_none() {
            self.state
                .uncovered
                .checked_add(self.family.control_bytes)
                .and_then(|bytes| bytes.checked_add(PHYSICAL_FAMILY_MARKER_BYTES))
                .ok_or(BudgetUnavailable)
        } else {
            Ok(self.state.uncovered)
        }
    }
    pub fn prepare_provider_ownership<'a>(
        &'a mut self,
        work: &mut Reservation,
        budget: &ProviderBudget,
    ) -> Result<PreparedPhysicalFamilyAdmission<'a>, BudgetUnavailable> {
        self.provider_ownership_needed(budget)?;
        let next_cut = self.state.next;
        let uncovered = self.state.uncovered;
        let next_covered = self
            .state
            .covered
            .checked_add(uncovered)
            .ok_or(BudgetUnavailable)?;
        if self.state.provider.is_none() {
            let bytes = uncovered
                .checked_add(self.family.control_bytes)
                .ok_or(BudgetUnavailable)?;
            let balance = work.split(bytes)?.commit_physical_balance()?;
            let marker = work.split(PHYSICAL_FAMILY_MARKER_BYTES)?.commit()?;
            if !balance.belongs_to(budget) || !marker.belongs_to(budget) {
                return Err(BudgetUnavailable);
            }
            return Ok(PreparedPhysicalFamilyAdmission(
                PreparedPhysicalFamilyAdmissionInner::First {
                    state: &mut self.state,
                    balance,
                    marker,
                    cut: next_cut,
                    covered: next_covered,
                },
            ));
        }
        let PhysicalOwnershipFamilyState {
            cut,
            uncovered: live_uncovered,
            covered,
            provider,
            marker,
            ..
        } = &mut *self.state;
        let marker = marker.as_ref().cloned().ok_or(BudgetUnavailable)?;
        let balance = provider.as_mut().ok_or(BudgetUnavailable)?;
        let increment = if uncovered == 0 {
            None
        } else {
            Some(balance.prepare_increment(work.split(uncovered)?)?)
        };
        Ok(PreparedPhysicalFamilyAdmission(
            PreparedPhysicalFamilyAdmissionInner::Increment {
                increment,
                cut,
                uncovered: live_uncovered,
                covered,
                next_cut,
                next_covered,
                marker,
            },
        ))
    }
}

/// One immutable physical allocation's optional provider ownership. Store after
/// the represented data, and move it with data that enters deferred retirement.
/// The caller supplies its preadmitted bound; no data or application authority
/// is held here. Multi-allocation callers serialize their entire admission plan.
#[doc(hidden)]
#[derive(Default)]
pub struct SharedPhysicalOwnershipCell {
    provider: std::sync::Mutex<Option<Arc<super::OwnershipCharge>>>,
}

impl std::fmt::Debug for SharedPhysicalOwnershipCell {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedPhysicalOwnershipCell")
            .finish_non_exhaustive()
    }
}

impl SharedPhysicalOwnershipCell {
    pub const fn new() -> Self {
        Self {
            provider: std::sync::Mutex::new(None),
        }
    }

    pub fn provider_ownership_needed(
        &self,
        bound: u64,
        budget: &ProviderBudget,
    ) -> Result<u64, BudgetUnavailable> {
        if bound == 0 {
            return Err(BudgetUnavailable);
        }
        let provider = self.provider.lock().map_err(|_| BudgetUnavailable)?;
        match provider.as_ref() {
            Some(owner) if owner.belongs_to(budget) && owner.bytes() >= bound => Ok(0),
            Some(_) => Err(BudgetUnavailable),
            None => Ok(bound),
        }
    }

    /// All fallible work remains private while the actual allocation's slot is
    /// locked. Dropping a prepared candidate restores its numerical ownership.
    pub fn prepare_provider_ownership_from(
        &self,
        bound: u64,
        work: &mut Reservation,
        budget: &ProviderBudget,
    ) -> Result<PreparedSharedPhysicalOwnership<'_>, BudgetUnavailable> {
        if bound == 0 || !work.live || !Arc::ptr_eq(&work.budget.0, &budget.0) {
            return Err(BudgetUnavailable);
        }
        let provider = self.provider.lock().map_err(|_| BudgetUnavailable)?;
        let candidate = match provider.as_ref() {
            Some(owner) if owner.belongs_to(budget) && owner.bytes() >= bound => None,
            Some(_) => return Err(BudgetUnavailable),
            None => Some(work.split(bound)?.commit()?),
        };
        Ok(PreparedSharedPhysicalOwnership {
            provider,
            candidate,
        })
    }

    #[doc(hidden)]
    pub fn owned_bytes(&self) -> Result<u64, BudgetUnavailable> {
        Ok(self
            .provider
            .lock()
            .map_err(|_| BudgetUnavailable)?
            .as_ref()
            .map_or(0, |owner| owner.bytes()))
    }
}

/// A private candidate plus its held physical-owner slot. No operation after
/// whole-plan preparation needs allocation, a new lock, or capacity admission.
#[doc(hidden)]
pub struct PreparedSharedPhysicalOwnership<'a> {
    // Roll back a private candidate before releasing the slot to another
    // contender, so cancellation cannot transiently double-reserve its owner.
    candidate: Option<Arc<super::OwnershipCharge>>,
    provider: std::sync::MutexGuard<'a, Option<Arc<super::OwnershipCharge>>>,
}
impl PreparedSharedPhysicalOwnership<'_> {
    pub fn commit(mut self) {
        if let Some(candidate) = self.candidate.take() {
            *self.provider = Some(candidate);
        }
    }
}
