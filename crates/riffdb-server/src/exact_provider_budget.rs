//! One conservative ownership budget for all five exact-provider families.
// req: OQ-040
#![forbid(unsafe_code)]

use std::ops::Deref;
use std::sync::{Arc, Mutex};

pub(super) const EXACT_PROVIDER_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BudgetUnavailable;

/// Contains only bounded counters. Identities and paths never enter accounting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct BudgetObservation {
    pub(super) fixed_owner_bytes: u64,
    pub(super) reserved_bytes: u64,
    pub(super) owned_bytes: u64,
    pub(super) reservations: u64,
    pub(super) allocations: u64,
    pub(super) released_allocations: u64,
}

struct Accounting {
    limit: u64,
    counters: BudgetObservation,
    refused: bool,
}

#[derive(Clone)]
pub(super) struct ProviderBudget(Arc<Mutex<Accounting>>);

impl ProviderBudget {
    pub(super) fn new() -> Self {
        Self::with_limit_and_owner(EXACT_PROVIDER_BUDGET_BYTES, 128)
    }

    #[cfg(test)]
    fn with_limit(limit: u64) -> Self {
        Self::with_limit_and_owner(limit, 0)
    }

    fn with_limit_and_owner(limit: u64, fixed_owner_bytes: u64) -> Self {
        Self(Arc::new(Mutex::new(Accounting {
            limit,
            counters: BudgetObservation {
                fixed_owner_bytes,
                ..BudgetObservation::default()
            },
            refused: false,
        })))
    }

    /// Caller supplies a conservative maximum including allocation capacities
    /// and ownership overhead, before allocating provider/checkpoint/scratch data.
    pub(super) fn reserve(&self, bytes: u64) -> Result<Reservation, BudgetUnavailable> {
        let mut accounting = self.0.lock().map_err(|_| BudgetUnavailable)?;
        if accounting.refused || bytes == 0 {
            return Err(BudgetUnavailable);
        }
        let counters = accounting.counters;
        let reserved = counters
            .reserved_bytes
            .checked_add(bytes)
            .ok_or(BudgetUnavailable)?;
        let total = reserved
            .checked_add(counters.owned_bytes)
            .and_then(|bytes| bytes.checked_add(counters.fixed_owner_bytes))
            .ok_or(BudgetUnavailable)?;
        let reservations = counters
            .reservations
            .checked_add(1)
            .ok_or(BudgetUnavailable)?;
        if total > accounting.limit {
            return Err(BudgetUnavailable);
        }
        accounting.counters.reserved_bytes = reserved;
        accounting.counters.reservations = reservations;
        Ok(Reservation {
            budget: self.clone(),
            bytes,
            live: true,
        })
    }

    pub(super) fn observation(&self) -> Result<BudgetObservation, BudgetUnavailable> {
        let accounting = self.0.lock().map_err(|_| BudgetUnavailable)?;
        if accounting.refused {
            return Err(BudgetUnavailable);
        }
        Ok(accounting.counters)
    }
}

/// Cancellation releases this caller's unpublished work only.
pub(super) struct Reservation {
    budget: ProviderBudget,
    bytes: u64,
    live: bool,
}

impl Reservation {
    /// The shared allocation keeps its charge after every discovery reference
    /// has gone. Its data drops before the accounting owner.
    pub(super) fn commit_value<T>(
        self,
        value: T,
    ) -> Result<Arc<ProviderAllocation<T>>, BudgetUnavailable> {
        let minimum = std::mem::size_of::<ProviderAllocation<T>>()
            .checked_add(std::mem::size_of::<OwnershipCharge>())
            .and_then(|bytes| bytes.checked_add(4 * std::mem::size_of::<usize>()))
            .and_then(|bytes| bytes.checked_add(2 * std::mem::align_of::<T>()))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(BudgetUnavailable)?;
        if self.bytes < minimum {
            return Err(BudgetUnavailable);
        }
        let charge = self.commit()?;
        Ok(Arc::new(ProviderAllocation {
            value,
            _charge: charge,
        }))
    }
    /// After the work has completed, retain its conservative capacity charge.
    /// Growing here is refused: extra allocation requires prior admission.
    pub(super) fn retain(mut self, bytes: u64) -> Result<Self, BudgetUnavailable> {
        if bytes == 0 || bytes > self.bytes {
            return Err(BudgetUnavailable);
        }
        let mut accounting = self.budget.0.lock().map_err(|_| BudgetUnavailable)?;
        if accounting.refused {
            return Err(BudgetUnavailable);
        }
        let remaining = accounting
            .counters
            .reserved_bytes
            .checked_sub(self.bytes - bytes)
            .ok_or(BudgetUnavailable)?;
        accounting.counters.reserved_bytes = remaining;
        self.bytes = bytes;
        drop(accounting);
        Ok(self)
    }

    /// Transfer the charge to shared ownership. Attach it to the allocation's
    /// final owner, after the charged data fields in destruction order.
    pub(super) fn commit(mut self) -> Result<Arc<OwnershipCharge>, BudgetUnavailable> {
        let minimum = std::mem::size_of::<OwnershipCharge>()
            .checked_add(2 * std::mem::size_of::<usize>())
            .and_then(|bytes| bytes.checked_add(std::mem::align_of::<OwnershipCharge>()))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(BudgetUnavailable)?;
        if self.bytes < minimum {
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
        Ok(Arc::new(OwnershipCharge {
            budget: self.budget.clone(),
            bytes: self.bytes,
        }))
    }
}

pub(super) struct ProviderAllocation<T> {
    value: T,
    // Last: data destruction still owns its byte charge.
    _charge: Arc<OwnershipCharge>,
}

impl<T> Deref for ProviderAllocation<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.live {
            return;
        }
        let mut accounting = self
            .budget
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let counters = accounting.counters;
        match (
            counters.reserved_bytes.checked_sub(self.bytes),
            counters.reservations.checked_sub(1),
        ) {
            (Some(bytes), Some(count)) => {
                accounting.counters.reserved_bytes = bytes;
                accounting.counters.reservations = count;
            }
            _ => accounting.refused = true,
        }
    }
}

/// Discovery removal does not release this charge. Arc destruction does.
pub(super) struct OwnershipCharge {
    budget: ProviderBudget,
    bytes: u64,
}

impl OwnershipCharge {
    pub(super) fn belongs_to(&self, budget: &ProviderBudget) -> bool {
        Arc::ptr_eq(&self.budget.0, &budget.0)
    }

    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for OwnershipCharge {
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
            (Some(bytes), Some(count), Some(released)) => {
                accounting.counters.owned_bytes = bytes;
                accounting.counters.allocations = count;
                accounting.counters.released_allocations = released;
            }
            _ => accounting.refused = true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn charge_metadata_cannot_be_allocated_from_an_undersized_reservation() {
        let budget = ProviderBudget::with_limit(128);
        assert!(budget.reserve(1).unwrap().commit().is_err());
        let observation = budget.observation().unwrap();
        assert_eq!(
            (observation.reserved_bytes, observation.owned_bytes),
            (0, 0)
        );
        assert!(budget.reserve(128).unwrap().commit().is_ok());
    }

    #[test]
    fn production_budget_is_fixed_at_one_gibibyte() {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        assert!(
            fixed
                >= u64::try_from(
                    std::mem::size_of::<Mutex<Accounting>>() + 2 * std::mem::size_of::<usize>()
                )
                .unwrap()
        );
        let all = budget.reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed).unwrap();
        assert!(budget.reserve(1).is_err());
        drop(all);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    #[test]
    fn a_captured_allocation_keeps_its_charge_through_data_destruction() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Payload {
            budget: ProviderBudget,
            destroyed: Arc<AtomicBool>,
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                assert_eq!(self.budget.observation().unwrap().owned_bytes, 128);
                self.destroyed.store(true, Ordering::Release);
            }
        }
        let budget = ProviderBudget::with_limit(128);
        let destroyed = Arc::new(AtomicBool::new(false));
        let reservation = budget.reserve(128).unwrap();
        let published = reservation
            .commit_value(Payload {
                budget: budget.clone(),
                destroyed: Arc::clone(&destroyed),
            })
            .unwrap();
        let captured = Arc::clone(&published);
        drop(published);
        assert!(!destroyed.load(Ordering::Acquire));
        assert!(budget.reserve(1).is_err());
        drop(captured);
        assert!(destroyed.load(Ordering::Acquire));
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn capacity_settlement_cannot_allocate_unreserved_bytes() {
        let budget = ProviderBudget::with_limit(128);
        let work = budget.reserve(128).unwrap();
        let selected = work.retain(48).unwrap().commit().unwrap();
        assert_eq!(budget.observation().unwrap().owned_bytes, 48);
        let other = budget.reserve(80).unwrap();
        assert!(other.retain(81).is_err());
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 48);
        drop(selected);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn reservation_precedes_allocation_and_final_reference_releases_capacity() {
        let budget = ProviderBudget::with_limit(128);
        let reserved = budget.reserve(96).unwrap();
        assert_eq!(budget.observation().unwrap().reserved_bytes, 96);
        assert!(budget.reserve(33).is_err());
        let owned = reserved.commit().unwrap();
        let captured = owned.clone();
        drop(owned);
        assert_eq!(budget.observation().unwrap().owned_bytes, 96);
        assert!(budget.reserve(33).is_err());
        drop(captured);
        let now = budget.observation().unwrap();
        assert_eq!((now.reserved_bytes, now.owned_bytes), (0, 0));
        assert!(budget.reserve(128).is_ok());
    }

    #[test]
    fn failed_and_cancelled_builds_release_only_their_reservation() {
        let budget = ProviderBudget::with_limit(128);
        let selected = budget.reserve(64).unwrap().commit().unwrap();
        let scratch = budget.reserve(64).unwrap();
        assert!(budget.reserve(1).is_err());
        drop(scratch);
        assert_eq!(budget.observation().unwrap().owned_bytes, 64);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        drop(selected);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    #[test]
    fn overflow_refuses_without_changing_the_existing_charge() {
        let budget = ProviderBudget::with_limit(u64::MAX);
        let first = budget.reserve(u64::MAX - 1).unwrap();
        assert!(budget.reserve(2).is_err());
        assert_eq!(budget.observation().unwrap().reserved_bytes, u64::MAX - 1);
        drop(first);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    #[test]
    fn concurrent_admission_has_one_winner_and_cancel_restores_all_bytes() {
        use std::sync::{Arc, Barrier};
        let budget = ProviderBudget::with_limit(128);
        let start = Arc::new(Barrier::new(3));
        let captured = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let tasks = (0..2)
            .map(|_| {
                let budget = budget.clone();
                let start = start.clone();
                let captured = captured.clone();
                let release = release.clone();
                std::thread::spawn(move || {
                    start.wait();
                    let result = budget.reserve(96);
                    captured.wait();
                    release.wait();
                    result.is_ok()
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        captured.wait();
        assert_eq!(budget.observation().unwrap().reserved_bytes, 96);
        release.wait();
        assert_eq!(
            tasks
                .into_iter()
                .map(|t| t.join().unwrap())
                .filter(|ok| *ok)
                .count(),
            1
        );
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
