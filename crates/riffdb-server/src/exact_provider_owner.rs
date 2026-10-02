//! Explicit capture and retirement authority, shared across provider families.
// req: OQ-040
#![forbid(unsafe_code)]

use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};

use riffdb_types::exact_provider_budget_v1::{BudgetUnavailable, OwnershipCharge, ProviderBudget};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Active,
    #[cfg(test)]
    Retiring,
    Refused,
}

// Use the same conservative allocation allowance as the capacity inspector,
// including each Arc header. These two closed metadata types cannot overflow.
const fn arc_capacity_bound<T>() -> u64 {
    (2 * (std::mem::size_of::<T>() + 2 * std::mem::size_of::<usize>())
        + 2 * std::mem::align_of::<T>()
        + 256) as u64
}
const PIN_ALLOCATION_BYTES: u64 =
    arc_capacity_bound::<PinGuard>() + arc_capacity_bound::<OwnershipCharge>();
const _: () = assert!(PIN_ALLOCATION_BYTES <= 1024);

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
        let minimum = arc_capacity_bound::<Self>() + arc_capacity_bound::<OwnershipCharge>();
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
        // Admit both the Arc guard and its immutable accounting owner first.
        let charge = self.budget.reserve(PIN_ALLOCATION_BYTES)?.commit()?;
        state.pins = pins;
        Ok(ProviderPin(Arc::new(PinGuard {
            owner: self.clone(),
            _charge: charge,
        })))
    }

    /// Pressure eviction additionally proves retention/cursor obligations at
    /// the registry boundary. This closes admission atomically with pin census.
    #[cfg(test)]
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
    #[cfg(test)]
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
    /// Lock order is registry -> owner -> slot. Keep this guard alive through
    /// the selection change; no registry lock may be acquired while it lives.
    pub(super) fn publication(&self) -> Result<ProviderPublication<'_>, BudgetUnavailable> {
        let state = self.0.owner.state.lock().map_err(|_| BudgetUnavailable)?;
        if state.phase != Phase::Active {
            return Err(BudgetUnavailable);
        }
        Ok(ProviderPublication { _state: state })
    }

    #[cfg(test)]
    pub(super) fn publish<T>(&self, select: impl FnOnce() -> T) -> Result<T, BudgetUnavailable> {
        let _publication = self.publication()?;
        Ok(select())
    }
}

pub(super) struct ProviderPublication<'a> {
    _state: MutexGuard<'a, State>,
}

pub(super) trait OwnedProviderSlot {
    fn provider_owner(&self) -> &Arc<ProviderOwner>;
}

/// Capture retains both the actual slot and its admission pin. Data drops
/// before the pin, so pressure cannot observe a released capture too early.
pub(super) struct CapturedProviderSlot<T> {
    slot: Arc<T>,
    pub(super) pin: ProviderPin,
}

impl<T: OwnedProviderSlot> CapturedProviderSlot<T> {
    pub(super) fn capture(slot: &Arc<T>) -> Result<Self, BudgetUnavailable> {
        let pin = slot.provider_owner().pin()?;
        Ok(Self {
            slot: Arc::clone(slot),
            pin,
        })
    }
}

impl<T> Deref for CapturedProviderSlot<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.slot.as_ref()
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
    fn captured_slot_keeps_data_alive_and_prevents_pressure_retirement() {
        struct Slot {
            data: Vec<u8>,
            owner: Arc<ProviderOwner>,
        }
        impl OwnedProviderSlot for Slot {
            fn provider_owner(&self) -> &Arc<ProviderOwner> {
                &self.owner
            }
        }
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(8192).unwrap().commit().unwrap(),
        )
        .unwrap();
        let slot = Arc::new(Slot {
            data: vec![7; 1024],
            owner: owner.clone(),
        });
        let capture = CapturedProviderSlot::capture(&slot).unwrap();
        drop(slot);
        assert_eq!(capture.data.len(), 1024);
        assert!(!owner.retire_if_unpinned().unwrap());
        owner.retire_superseded().unwrap();
        assert!(CapturedProviderSlot::capture(&capture.slot).is_err());
        assert!(capture.pin.publication().is_err());
        drop(owner);
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            8192 + PIN_ALLOCATION_BYTES
        );
        drop(capture);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn pin_charge_covers_both_actual_ownership_allocations() {
        use riffdb_types::{OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionV1};
        let budget = ProviderBudget::new();
        let owner = ProviderOwner::new(
            budget.clone(),
            budget.reserve(1024).unwrap().commit().unwrap(),
        )
        .unwrap();
        let scratch = budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
            inspection.allocation(
                std::mem::size_of::<PinGuard>() + 2 * std::mem::size_of::<usize>(),
                std::mem::align_of::<PinGuard>(),
            )?;
            inspection.allocation(
                std::mem::size_of::<OwnershipCharge>() + 2 * std::mem::size_of::<usize>(),
                std::mem::align_of::<OwnershipCharge>(),
            )
        })
        .unwrap();
        let minimum = report.unshared_bytes();
        drop(report);
        drop(scratch);
        let before = budget.observation().unwrap().owned_bytes;
        let pin = owner.pin().unwrap();
        assert!(budget.observation().unwrap().owned_bytes - before >= minimum);
        drop(pin);
        drop(owner);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

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
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            1024 + PIN_ALLOCATION_BYTES
        );
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
