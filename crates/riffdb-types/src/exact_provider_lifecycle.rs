//! Closed provider lifecycle counters; no identities enter the accounting pool.
// req: OQ-040, PRJ-004
use super::{BudgetObservation, BudgetUnavailable, ProviderBudget};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Active,
    Retiring,
}

/// First-party accounting authority for one actual provider owner. This token
/// grants no storage authority, pin, epoch lease or publication permission.
#[doc(hidden)]
pub struct ProviderOwnerAccounting {
    phase: Phase,
    pins: u32,
    budget: ProviderBudget,
}
impl ProviderBudget {
    #[doc(hidden)]
    pub fn register_provider_owner(&self) -> Result<ProviderOwnerAccounting, BudgetUnavailable> {
        let mut owner = ProviderOwnerAccounting {
            phase: Phase::Active,
            pins: 0,
            budget: self.clone(),
        };
        owner.update(|counts| {
            counts.active_owners = counts
                .active_owners
                .checked_add(1)
                .ok_or(BudgetUnavailable)?;
            Ok(())
        })?;
        Ok(owner)
    }
}
impl ProviderOwnerAccounting {
    // Copy first: checked counter updates publish together, never partially.
    fn update(
        &mut self,
        change: impl FnOnce(&mut BudgetObservation) -> Result<(), BudgetUnavailable>,
    ) -> Result<(), BudgetUnavailable> {
        let mut accounting = self.budget.0.lock().map_err(|_| BudgetUnavailable)?;
        if accounting.refused {
            return Err(BudgetUnavailable);
        }
        let mut counts = accounting.counters;
        if change(&mut counts).is_err() {
            accounting.refused = true;
            return Err(BudgetUnavailable);
        }
        accounting.counters = counts;
        Ok(())
    }
    #[doc(hidden)]
    pub fn pin_admitted(&mut self) -> Result<(), BudgetUnavailable> {
        if self.phase != Phase::Active {
            return Err(BudgetUnavailable);
        }
        let pins = self.pins.checked_add(1).ok_or(BudgetUnavailable)?;
        let first = self.pins == 0;
        self.update(|counts| {
            counts.pins = counts.pins.checked_add(1).ok_or(BudgetUnavailable)?;
            if first {
                counts.pinned_owners = counts
                    .pinned_owners
                    .checked_add(1)
                    .ok_or(BudgetUnavailable)?;
            }
            Ok(())
        })?;
        self.pins = pins;
        Ok(())
    }
    #[doc(hidden)]
    pub fn release_pin(&mut self) -> Result<(), BudgetUnavailable> {
        let prior = self.pins;
        self.update(|counts| {
            if prior == 0 {
                return Err(BudgetUnavailable);
            }
            counts.pins = counts.pins.checked_sub(1).ok_or(BudgetUnavailable)?;
            if prior == 1 {
                counts.pinned_owners = counts
                    .pinned_owners
                    .checked_sub(1)
                    .ok_or(BudgetUnavailable)?;
            }
            Ok(())
        })?;
        self.pins = prior - 1;
        Ok(())
    }
    #[doc(hidden)]
    pub fn retire(&mut self) -> Result<(), BudgetUnavailable> {
        if self.phase == Phase::Retiring {
            return Ok(());
        }
        self.update(|counts| {
            counts.active_owners = counts
                .active_owners
                .checked_sub(1)
                .ok_or(BudgetUnavailable)?;
            counts.retiring_owners = counts
                .retiring_owners
                .checked_add(1)
                .ok_or(BudgetUnavailable)?;
            Ok(())
        })?;
        self.phase = Phase::Retiring;
        Ok(())
    }
}
impl Drop for ProviderOwnerAccounting {
    fn drop(&mut self) {
        let phase = self.phase;
        let pins = self.pins;
        let _ = self.update(|counts| {
            if pins != 0 {
                return Err(BudgetUnavailable);
            }
            let present = match phase {
                Phase::Active => &mut counts.active_owners,
                Phase::Retiring => &mut counts.retiring_owners,
            };
            *present = present.checked_sub(1).ok_or(BudgetUnavailable)?;
            counts.released_owners = counts
                .released_owners
                .checked_add(1)
                .ok_or(BudgetUnavailable)?;
            Ok(())
        });
    }
}
