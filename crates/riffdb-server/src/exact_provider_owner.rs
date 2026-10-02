//! Explicit capture and retirement authority, shared across provider families.
// req: OQ-040
#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex};

use crate::exact_provider_budget::{BudgetUnavailable, OwnershipCharge, ProviderBudget};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Active,
    Retiring,
    Refused,
}

struct State {
    phase: Phase,
    pins: u32,
}

pub(super) struct ProviderOwner {
    state: Mutex<State>,
    budget: ProviderBudget,
    // Last field: the registration charge survives all owner state.
    _charge: Arc<OwnershipCharge>,
}

impl ProviderOwner {
    pub(super) fn new(
        budget: ProviderBudget,
        charge: Arc<OwnershipCharge>,
    ) -> Result<Arc<Self>, BudgetUnavailable> {
        let minimum = std::mem::size_of::<Self>()
            .checked_add(2 * std::mem::size_of::<usize>())
            .and_then(|bytes| bytes.checked_add(std::mem::align_of::<Self>()))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(BudgetUnavailable)?;
        if !charge.belongs_to(&budget) || charge.bytes() < minimum {
            return Err(BudgetUnavailable);
        }
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                phase: Phase::Active,
                pins: 0,
            }),
            budget,
            _charge: charge,
        }))
    }

    /// Must precede query/worker capture. Retirement closes new admission.
    pub(super) fn pin(self: &Arc<Self>) -> Result<ProviderPin, BudgetUnavailable> {
        let mut state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase != Phase::Active {
            return Err(BudgetUnavailable);
        }
        let pins = state.pins.checked_add(1).ok_or(BudgetUnavailable)?;
        // Conservative allocation overhead for the Arc guard and charge owner.
        let charge = self.budget.reserve(256)?.commit()?;
        state.pins = pins;
        Ok(ProviderPin(Arc::new(PinGuard {
            owner: self.clone(),
            _charge: charge,
        })))
    }

    /// Pressure eviction additionally proves retention/cursor obligations at
    /// the registry boundary. This closes admission atomically with pin census.
    pub(super) fn retire_if_unpinned(&self) -> Result<bool, BudgetUnavailable> {
        let mut state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase != Phase::Active || state.pins != 0 {
            return Ok(false);
        }
        state.phase = Phase::Retiring;
        Ok(true)
    }

    /// Exact supersession removes discovery even while captured work lives.
    /// Existing pins retain ownership; they cannot publish into retired state.
    pub(super) fn retire_superseded(&self) -> Result<(), BudgetUnavailable> {
        let mut state = self.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase == Phase::Refused {
            return Err(BudgetUnavailable);
        }
        state.phase = Phase::Retiring;
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct ProviderPin(Arc<PinGuard>);

impl ProviderPin {
    /// Publication holds this guard through the selection update, so retirement
    /// cannot race the check. Callers must never acquire registry locks inside.
    pub(super) fn publish<T>(&self, select: impl FnOnce() -> T) -> Result<T, BudgetUnavailable> {
        let state = self.0.owner.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase != Phase::Active {
            return Err(BudgetUnavailable);
        }
        Ok(select())
    }
}

struct PinGuard {
    owner: Arc<ProviderOwner>,
    _charge: Arc<OwnershipCharge>,
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(pins) = state.pins.checked_sub(1) {
            state.pins = pins;
        } else {
            state.phase = Phase::Refused;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ownership_cannot_attach_a_charge_from_another_budget() {
        let budget = ProviderBudget::new();
        let foreign = ProviderBudget::new();
        let charge = foreign.reserve(1024).unwrap().commit().unwrap();
        assert!(ProviderOwner::new(budget.clone(), charge).is_err());
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(foreign.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn captured_owner_survives_discovery_removal_and_cannot_publish_after_retirement() {
        let budget = ProviderBudget::new();
        let charge = budget.reserve(1024).unwrap().commit().unwrap();
        let owner = ProviderOwner::new(budget.clone(), charge).unwrap();
        let captured = owner.pin().unwrap();
        let continuation = captured.clone();
        assert!(!owner.retire_if_unpinned().unwrap());
        owner.retire_superseded().unwrap();
        assert!(owner.pin().is_err());
        let mut published = false;
        assert!(captured.publish(|| published = true).is_err());
        assert!(!published);
        drop(owner);
        drop(captured);
        assert_eq!(budget.observation().unwrap().owned_bytes, 1280);
        drop(continuation);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn pressure_retirement_admits_only_after_last_pin_releases() {
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(1024).unwrap().commit().unwrap(),
        )
        .unwrap();
        let pin = owner.pin().unwrap();
        assert_eq!(pin.publish(|| 42), Ok(42));
        assert!(!owner.retire_if_unpinned().unwrap());
        drop(pin);
        assert!(owner.retire_if_unpinned().unwrap());
        assert!(owner.pin().is_err());
        drop(owner);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn retirement_serializes_with_an_already_admitted_publication() {
        use std::sync::mpsc;
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(1024).unwrap().commit().unwrap(),
        )
        .unwrap();
        let pin = owner.pin().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (finish_tx, finish_rx) = mpsc::sync_channel(0);
        let publisher = std::thread::spawn(move || {
            pin.publish(|| {
                entered_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
                42
            })
        });
        entered_rx.recv().unwrap();
        let retiring = owner.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel(0);
        let retire = std::thread::spawn(move || {
            ready_tx.send(()).unwrap();
            retiring.retire_superseded()
        });
        ready_rx.recv().unwrap();
        finish_tx.send(()).unwrap();
        assert_eq!(publisher.join().unwrap(), Ok(42));
        assert_eq!(retire.join().unwrap(), Ok(()));
        assert!(owner.pin().is_err());
        drop(owner);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}
