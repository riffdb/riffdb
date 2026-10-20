// req: OQ-040, STO-020, PRJ-004
use super::*;
use crate::exact_provider_budget_v1::ProviderBudget;
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, Ordering},
};

#[test]
fn numeric_preview_keeps_full_source_census_and_clones_no_original_arc() {
    let source = crate::exact_provider_budget_v1::ProjectionSourceWorkingBudgetV1::new();
    let available = crate::exact_provider_budget_v1::PROJECTION_SOURCE_WORKING_MEMORY_BYTES - 512;
    let mut census = source
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let blocked = source
        .reserve(available - OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 - 1024 + 1)
        .unwrap();
    let value = Arc::new(String::from("preview"));
    let input = [value.clone(), value.clone()];
    let strong = Arc::strong_count(&value);
    let report =
        OwnedCapacityInspectionV1::preview(&input, census.inspection_scope().unwrap()).unwrap();
    assert_eq!(report.shared_allocations().len(), 1);
    assert!(report.shared_allocations()[0].lifecycle().is_none());
    assert_eq!(Arc::strong_count(&value), strong);
    assert!(source.reserve(1024).is_err());
    drop(report);
    assert!(
        source.reserve(1024).is_err(),
        "census scope still holds all4MiB"
    );
    drop(census);
    let main = source.reserve(1024).unwrap();
    drop(main);
    drop(blocked);
    assert_eq!(source.observation().unwrap().reserved_bytes, 0);
}

struct Payload {
    bytes: Vec<u8>,
    budget: ProviderBudget,
    destroyed: Arc<AtomicBool>,
    lifecycle: crate::OwnedCapacityLifecycleV1,
}
impl InspectOwnedCapacityV1 for Payload {
    fn allocation_lifecycle(&self) -> Option<&crate::OwnedCapacityLifecycleV1> {
        Some(&self.lifecycle)
    }
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        inspection.visit(&self.bytes)?;
        inspection.visit(&self.lifecycle)
    }
}
impl Drop for Payload {
    fn drop(&mut self) {
        let observation = self.budget.observation().unwrap();
        assert!(observation.owned_bytes >= self.bytes.capacity() as u64);
        self.destroyed.store(true, Ordering::Release);
    }
}

fn inspect_from_borrowed_input<'input>(
    original: &'input crate::OwnedCapacitySharedV1<Payload>,
    scratch: crate::exact_provider_budget_v1::Reservation,
) -> OwnedCapacityReportV1<'input> {
    OwnedCapacityInspectionV1::inspect_owned(original, scratch).unwrap()
}

#[test]
fn owned_report_escape_retains_vec_carrier_and_original_until_actual_drop() {
    let budget = ProviderBudget::new();
    let destroyed = Arc::new(AtomicBool::new(false));
    let construction = budget.reserve(1024 * 1024).unwrap();
    let scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let original = crate::OwnedCapacitySharedV1::new(Payload {
        bytes: Vec::with_capacity(4096),
        budget: budget.clone(),
        destroyed: destroyed.clone(),
        lifecycle: Default::default(),
    });
    let report = inspect_from_borrowed_input(&original, scratch);
    let allocation = &report.shared_allocations()[0];
    allocation
        .lifecycle()
        .unwrap()
        .attach(
            budget
                .reserve(allocation.bytes())
                .unwrap()
                .commit_for_allocation(allocation.identity())
                .unwrap(),
        )
        .unwrap();
    assert!(!destroyed.load(Ordering::Acquire));
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1024 * 1024
    );
    drop(report);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 1024 * 1024);
    assert!(!destroyed.load(Ordering::Acquire));
    drop(original);
    assert!(destroyed.load(Ordering::Acquire));
    drop(construction);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn owned_inspection_refuses_raw_arc_without_cloning_weak_backing() {
    let budget = ProviderBudget::new();
    let scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let value = Arc::new(String::from("raw"));
    let weak = Arc::downgrade(&value);
    assert!(OwnedCapacityInspectionV1::inspect_owned(&value, scratch).is_err());
    assert_eq!(Arc::strong_count(&value), 1);
    assert_eq!(Arc::weak_count(&value), 1);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    drop(weak);
}

#[test]
fn unattached_lifecycle_cannot_mint_persistent_observer_and_dead_cannot_revive() {
    let budget = ProviderBudget::new();
    let lifecycle = crate::OwnedCapacityLifecycleV1::default();
    assert!(lifecycle.observer().is_err());
    // Numerical fixture128 tests control retirement, not a full layout quote.
    lifecycle
        .reference()
        .attach(budget.reserve(128).unwrap().commit().unwrap())
        .unwrap();
    let observer = lifecycle.observer().unwrap();
    drop(lifecycle);
    assert!(observer.attached_owner().is_err());
    assert_eq!(budget.observation().unwrap().owned_bytes, 128);
    drop(observer);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn two_final_numerical_observers_release_one_charge_after_dead_control() {
    let budget = ProviderBudget::new();
    let lifecycle = crate::OwnedCapacityLifecycleV1::default();
    lifecycle
        .reference()
        .attach(budget.reserve(128).unwrap().commit().unwrap())
        .unwrap();
    let left = lifecycle.observer().unwrap();
    let right = left.clone();
    drop(lifecycle);
    let gate = Arc::new(Barrier::new(3));
    let handles = [left, right].map(|observer| {
        let gate = gate.clone();
        std::thread::spawn(move || {
            gate.wait();
            drop(observer);
        })
    });
    assert_eq!(budget.observation().unwrap().owned_bytes, 128);
    gate.wait();
    for handle in handles {
        handle.join().unwrap();
    }
    let observation = budget.observation().unwrap();
    assert_eq!(observation.owned_bytes, 0);
    assert_eq!(observation.allocations, 0);
    assert_eq!(observation.released_allocations, 1);
}

#[test]
fn inspection_scope_refuses_partial_allowance_without_admitting_more_work() {
    let budget = ProviderBudget::new();
    let mut partial = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 - 1)
        .unwrap();
    assert!(partial.inspection_scope().is_err());
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 - 1
    );
    drop(partial);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn closed_shared_unique_access_refuses_alias_without_changing_value_or_identity() {
    let mut owner = crate::OwnedCapacitySharedV1::new(String::from("retained"));
    let alias = owner.clone();
    assert!(owner.get_mut().is_none());
    let mut owner = owner.try_into_value().unwrap_err();
    assert!(owner.same_allocation(&alias));
    assert_eq!(&*owner, "retained");
    drop(alias);
    owner.get_mut().unwrap().push_str("-unique");
    assert_eq!(owner.try_into_value().unwrap(), "retained-unique");
}

#[test]
fn closed_shared_unique_extraction_retains_real_data_charge_until_value_destruction() {
    let budget = ProviderBudget::new();
    let destroyed = Arc::new(AtomicBool::new(false));
    let lifecycle = crate::OwnedCapacityLifecycleV1::default();
    // A numerical lifecycle fixture: this is not a complete heap quote.
    lifecycle
        .reference()
        .attach(budget.reserve(8192).unwrap().commit().unwrap())
        .unwrap();
    let owner = crate::OwnedCapacitySharedV1::new(Payload {
        bytes: Vec::with_capacity(4096),
        budget: budget.clone(),
        destroyed: destroyed.clone(),
        lifecycle,
    });
    let value = match owner.try_into_value() {
        Ok(value) => value,
        Err(_) => panic!("actual owner is unique"),
    };
    assert!(!destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().owned_bytes, 8192);
    drop(value);
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn closed_erased_provider_downcast_borrows_semantic_value_and_keeps_original_charge() {
    let budget = ProviderBudget::new();
    let destroyed = Arc::new(AtomicBool::new(false));
    let owner = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1024 * 1024)
        .unwrap()
        .settle_owned_value(Payload {
            bytes: Vec::with_capacity(4096),
            budget: budget.clone(),
            destroyed: destroyed.clone(),
            lifecycle: Default::default(),
        })
        .unwrap();
    let expected = owner.owned_bytes();
    let erased = owner.clone().into_erased();
    assert!(erased.downcast_ref::<Payload>().is_some());
    assert!(erased.downcast_ref::<String>().is_none());
    assert!(
        erased
            .downcast_ref::<crate::exact_provider_budget_v1::ProviderAllocation<Payload>>()
            .is_none()
    );
    drop(owner);
    assert_eq!(budget.observation().unwrap().owned_bytes, expected);
    assert!(!destroyed.load(Ordering::Acquire));
    drop(erased);
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn mixed_typed_erased_final_aliases_release_once_after_real_data_drop() {
    let budget = ProviderBudget::new();
    let destroyed = Arc::new(AtomicBool::new(false));
    let owner = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1024 * 1024)
        .unwrap()
        .settle_owned_value(Payload {
            bytes: Vec::with_capacity(4096),
            budget: budget.clone(),
            destroyed: destroyed.clone(),
            lifecycle: Default::default(),
        })
        .unwrap();
    let erased = owner.clone().into_erased();
    let gate = Arc::new(Barrier::new(3));
    let a = {
        let gate = gate.clone();
        std::thread::spawn(move || {
            gate.wait();
            drop(owner);
        })
    };
    let b = {
        let gate = gate.clone();
        std::thread::spawn(move || {
            gate.wait();
            drop(erased);
        })
    };
    assert!(!destroyed.load(Ordering::Acquire));
    gate.wait();
    a.join().unwrap();
    b.join().unwrap();
    assert!(destroyed.load(Ordering::Acquire));
    let observation = budget.observation().unwrap();
    assert_eq!(observation.owned_bytes, 0);
    assert_eq!(observation.allocations, 0);
    assert_eq!(observation.released_allocations, 1);
}

// req: OQ-040, STO-020, PRJ-004
struct NumericScratchControl {
    budget: ProviderBudget,
    destroyed: Arc<AtomicBool>,
    work: crate::exact_provider_budget_v1::Reservation,
}
impl Drop for NumericScratchControl {
    fn drop(&mut self) {
        assert!(self.work.validate_inspection_scope().is_ok());
        assert!(
            self.budget.observation().unwrap().reserved_bytes
                >= OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
        );
        self.destroyed.store(true, Ordering::Release);
    }
}
fn numeric_scratch_control(
    budget: &ProviderBudget,
    destroyed: &Arc<AtomicBool>,
) -> NumericScratchControl {
    NumericScratchControl {
        budget: budget.clone(),
        destroyed: destroyed.clone(),
        work: budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap(),
    }
}
#[test]
fn preallocated_numeric_preview_holds_actual_control_through_report_and_buffer_retirement() {
    let budget = ProviderBudget::new();
    let destroyed = Arc::new(AtomicBool::new(false));
    assert!(
        OwnedCapacityInspectionScratchV1::<NumericScratchControl>::construction_upper_bound()
            .unwrap()
            <= OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
    );
    let mut scratch =
        OwnedCapacityInspectionScratchV1::try_new(numeric_scratch_control(&budget, &destroyed))
            .unwrap();
    let raw = Arc::new(vec![9_u8; 1024]);
    let strong = Arc::strong_count(&raw);
    let report = scratch.preview(&raw).unwrap();
    assert_eq!(report.shared_allocations().len(), 1);
    assert!(report.shared_allocations()[0].bytes() >= 1024);
    assert_eq!(Arc::strong_count(&raw), strong);
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
    );
    assert!(!destroyed.load(Ordering::Acquire));
    drop(report);
    assert!(!destroyed.load(Ordering::Acquire));
    let control = scratch.into_control();
    assert!(!destroyed.load(Ordering::Acquire));
    drop(control);
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
#[test]
fn preallocated_numeric_preview_refuses_second_use_without_replenishing_any_buffer() {
    let budget = ProviderBudget::new();
    let destroyed = Arc::new(AtomicBool::new(false));
    let mut scratch =
        OwnedCapacityInspectionScratchV1::try_new(numeric_scratch_control(&budget, &destroyed))
            .unwrap();
    let value = vec![3_u8; 128];
    drop(scratch.preview(&value).unwrap());
    assert!(scratch.observations.is_none());
    let held = budget.observation().unwrap();
    assert!(scratch.preview(&value).is_err());
    assert!(scratch.observations.is_none());
    assert_eq!(budget.observation().unwrap(), held);
    drop(scratch);
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
struct RefusedNumericPreview;
impl InspectOwnedCapacityV1 for RefusedNumericPreview {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        inspection.allocation(4096, 8)?;
        Err(OwnedCapacityInspectionErrorV1)
    }
}
#[test]
fn preallocated_numeric_preview_failure_retires_its_buffer_before_final_control_refund() {
    let budget = ProviderBudget::new();
    let destroyed = Arc::new(AtomicBool::new(false));
    let mut scratch =
        OwnedCapacityInspectionScratchV1::try_new(numeric_scratch_control(&budget, &destroyed))
            .unwrap();
    assert!(scratch.preview(&RefusedNumericPreview).is_err());
    assert!(scratch.observations.is_none());
    assert!(!destroyed.load(Ordering::Acquire));
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
    );
    drop(scratch);
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
#[test]
fn preallocated_numeric_preview_never_clones_carriers_or_authorizes_owned_settlement() {
    let budget = ProviderBudget::new();
    let destroyed = Arc::new(AtomicBool::new(false));
    let mut scratch =
        OwnedCapacityInspectionScratchV1::try_new(numeric_scratch_control(&budget, &destroyed))
            .unwrap();
    let original = crate::OwnedCapacitySharedV1::new(vec![7_u8; 4096]);
    let report = scratch.preview(&original).unwrap();
    assert_eq!(report.shared_allocations().len(), 1);
    assert!(
        report
            .shared_allocations()
            .iter()
            .all(|allocation| allocation.lifecycle().is_none())
    );
    drop(report);
    // A successful numerical preview cannot attach this unowned semantic graph.
    let actual = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    assert!(OwnedCapacityInspectionV1::inspect_owned(&original, actual).is_err());
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
    );
    drop(scratch);
    assert!(destroyed.load(Ordering::Acquire));
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
