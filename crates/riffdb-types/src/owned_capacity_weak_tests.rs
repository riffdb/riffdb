// req: OQ-040, STO-020, PRJ-004
//! Numerical lifetime controls; these do not certify an engine or heap census.
use super::*;
use crate::exact_provider_budget_v1::{ProviderBudget, Reservation};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, Ordering},
};

struct Data {
    destroyed: Arc<AtomicBool>,
    _charge: Reservation,
}
impl Drop for Data {
    fn drop(&mut self) {
        self.destroyed.store(true, Ordering::Release);
    }
}
// Intentionally no Clone: only the closed shared handle may be cloned.
struct NumericControl {
    _charge: Reservation,
}
fn fixture(
    budget: &ProviderBudget,
) -> (
    OwnedCapacitySharedV1<Data>,
    OwnedCapacityWeakV1<Data, NumericControl>,
    Arc<AtomicBool>,
) {
    let destroyed = Arc::new(AtomicBool::new(false));
    let owner = OwnedCapacitySharedV1::new(Data {
        destroyed: destroyed.clone(),
        _charge: budget.reserve(2048).unwrap(),
    });
    let control = OwnedCapacitySharedV1::new(NumericControl {
        _charge: budget.reserve(4096).unwrap(),
    });
    let weak = owner.downgrade_with_control(control);
    (owner, weak, destroyed)
}
#[test]
fn closed_weak_retains_backing_after_final_strong_and_cannot_revive_dead_value() {
    let budget = ProviderBudget::new();
    let (owner, weak, destroyed) = fixture(&budget);
    let final_weak = weak.clone();
    assert!(weak.same_allocation(&owner));
    drop(owner);
    assert!(destroyed.load(Ordering::Acquire));
    assert!(weak.upgrade().is_none());
    assert_eq!(budget.observation().unwrap().reserved_bytes, 4096);
    drop(weak);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 4096);
    drop(final_weak);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
#[test]
fn upgraded_strong_retains_original_charge_after_final_weak_control_drops() {
    let budget = ProviderBudget::new();
    let (owner, weak, destroyed) = fixture(&budget);
    let upgraded = weak.upgrade().unwrap();
    assert!(upgraded.same_allocation(&owner));
    drop(owner);
    drop(weak);
    assert!(!destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 2048);
    drop(upgraded);
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
#[test]
fn closed_weak_identity_and_unique_extraction_preserve_actual_allocation_rules() {
    let budget = ProviderBudget::new();
    let (mut owner, weak, destroyed) = fixture(&budget);
    let (foreign, foreign_weak, _) = fixture(&budget);
    assert!(!weak.same_allocation(&foreign));
    assert!(!foreign_weak.same_allocation(&owner));
    // Arc::get_mut refuses a weak observer, while try_unwrap permits one actual
    // sole strong owner. Neither rule is widened by the closed weak protocol.
    assert!(owner.get_mut().is_none());
    let alias = owner.clone();
    let owner = match owner.try_into_value() {
        Err(owner) => owner,
        Ok(_) => panic!("a genuine strong alias must refuse extraction"),
    };
    assert!(weak.same_allocation(&owner));
    drop(alias);
    let value = match owner.try_into_value() {
        Ok(value) => value,
        Err(_) => panic!("the actual sole strong owner permits extraction"),
    };
    assert!(weak.upgrade().is_none());
    assert!(!destroyed.load(Ordering::Acquire));
    drop(value);
    assert!(destroyed.load(Ordering::Acquire));
    drop(weak);
    drop(foreign_weak);
    drop(foreign);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
#[test]
fn concurrent_final_strong_and_weak_drop_retire_each_charge_exactly_once() {
    let budget = ProviderBudget::new();
    let (owner, weak, destroyed) = fixture(&budget);
    let barrier = Arc::new(Barrier::new(3));
    let a = barrier.clone();
    let strong = std::thread::spawn(move || {
        a.wait();
        drop(owner);
    });
    let b = barrier.clone();
    let backing = std::thread::spawn(move || {
        b.wait();
        drop(weak);
    });
    assert_eq!(budget.observation().unwrap().reserved_bytes, 6144);
    barrier.wait();
    strong.join().unwrap();
    backing.join().unwrap();
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn overlapping_final_retirement_keeps_each_allowance_through_its_actual_destructor() {
    struct HookedData {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
        destroyed: Arc<AtomicBool>,
        _charge: Reservation,
    }
    impl Drop for HookedData {
        fn drop(&mut self) {
            self.entered.wait();
            self.release.wait();
            self.destroyed.store(true, Ordering::Release);
        }
    }
    struct HookedControl {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
        _charge: Reservation,
    }
    impl Drop for HookedControl {
        fn drop(&mut self) {
            // The actual Weak and this control's Arc storage have retired, but
            // the control payload's numerical reservation must still be live.
            self.entered.wait();
            self.release.wait();
        }
    }
    let budget = ProviderBudget::new();
    let data_entered = Arc::new(Barrier::new(2));
    let data_release = Arc::new(Barrier::new(2));
    let control_entered = Arc::new(Barrier::new(2));
    let control_release = Arc::new(Barrier::new(2));
    let destroyed = Arc::new(AtomicBool::new(false));
    let owner = OwnedCapacitySharedV1::new(HookedData {
        entered: data_entered.clone(),
        release: data_release.clone(),
        destroyed: destroyed.clone(),
        _charge: budget.reserve(2048).unwrap(),
    });
    let control = OwnedCapacitySharedV1::new(HookedControl {
        entered: control_entered.clone(),
        release: control_release.clone(),
        _charge: budget.reserve(4096).unwrap(),
    });
    let weak = owner.downgrade_with_control(control);
    let strong = std::thread::spawn(move || drop(owner));
    data_entered.wait();
    assert!(
        weak.upgrade().is_none(),
        "T is in its actual final destructor"
    );
    assert_eq!(budget.observation().unwrap().reserved_bytes, 6144);
    assert!(!destroyed.load(Ordering::Acquire));
    let backing = std::thread::spawn(move || drop(weak));
    control_entered.wait();
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        6144,
        "both reservations survive their paused actual destructors"
    );
    control_release.wait();
    backing.join().unwrap();
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        2048,
        "T's original allowance survives final weak/control destruction"
    );
    assert!(!destroyed.load(Ordering::Acquire));
    data_release.wait();
    strong.join().unwrap();
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
