//! Numerical ownership protocol tests, not allocator/RSS or Source authority.
// req: OQ-040, PRJ-004
use super::*;
use std::sync::{
    Barrier,
    atomic::{AtomicUsize, Ordering},
};

struct Payload {
    budget: ProviderBudget,
    destroyed: Arc<AtomicUsize>,
}
impl Drop for Payload {
    fn drop(&mut self) {
        assert_eq!(
            self.budget.observation().unwrap().owned_bytes,
            128,
            "data destruction must retain the whole admitted charge"
        );
        self.destroyed.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn provider_closed_final_aliases_destroy_data_before_one_charge_refund() {
    // Numerical fixture allowance, not a complete heap construction quote.
    let budget = ProviderBudget::with_limit(128);
    let destroyed = Arc::new(AtomicUsize::new(0));
    let first = budget
        .reserve(128)
        .unwrap()
        .commit_value(Payload {
            budget: budget.clone(),
            destroyed: destroyed.clone(),
        })
        .unwrap();
    let second = first.clone();
    assert!(first.same_allocation(&second));
    let arrived = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(3));
    let spawn = |owner: ProviderAllocationOwner<Payload>| {
        let arrived = arrived.clone();
        let release = release.clone();
        std::thread::spawn(move || {
            arrived.wait();
            release.wait();
            drop(owner);
        })
    };
    let left = spawn(first);
    let right = spawn(second);
    arrived.wait();
    let retained =
        budget.observation().unwrap().owned_bytes == 128 && destroyed.load(Ordering::SeqCst) == 0;
    release.wait(); // ALWAYS release both final owners before any assertion.
    left.join().unwrap();
    right.join().unwrap();
    assert!(retained);
    assert_eq!(destroyed.load(Ordering::SeqCst), 1);
    let after = budget.observation().unwrap();
    assert_eq!(
        (
            after.owned_bytes,
            after.allocations,
            after.released_allocations
        ),
        (0, 0, 1)
    );
    assert!(budget.reserve(128).is_ok());
}

#[test]
fn provider_closed_charge_aliases_preserve_membership_and_refund_once() {
    let budget = ProviderBudget::with_limit(128);
    let foreign = ProviderBudget::with_limit(128);
    let first = budget.reserve(128).unwrap().commit().unwrap();
    let last = first.clone();
    assert!(first.same_allocation(&last));
    assert!(first.belongs_to(&budget));
    assert!(!first.belongs_to(&foreign));
    drop(first);
    assert!(budget.reserve(1).is_err());
    assert_eq!(last.bytes(), 128);
    drop(last);
    let after = budget.observation().unwrap();
    assert_eq!(
        (
            after.owned_bytes,
            after.allocations,
            after.released_allocations
        ),
        (0, 0, 1)
    );
}

#[test]
fn source_closed_settlement_keeps_exact_ledger_through_final_alias() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let foreign = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let owned = budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 16384)
        .unwrap()
        .settle_owned_value(7_u64)
        .unwrap();
    let last = owned.clone();
    assert!(owned.belongs_to(&budget));
    assert!(!owned.belongs_to(&foreign));
    let charged = budget.observation().unwrap();
    assert!(charged.owned_bytes > 0);
    drop(owned);
    assert_eq!(
        budget.observation().unwrap().owned_bytes,
        charged.owned_bytes
    );
    assert_eq!(*last, 7);
    drop(last);
    let after = budget.observation().unwrap();
    assert_eq!(after.fixed_owner_bytes, before.fixed_owner_bytes);
    assert_eq!(
        (
            after.reserved_bytes,
            after.owned_bytes,
            after.reservations,
            after.allocations
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(after.released_allocations, before.released_allocations + 1);
}

#[test]
fn closed_accountant_control_fits_512_and_retains_foreign_overlap_until_final_owner() {
    let bound = 2 * (std::mem::size_of::<BudgetOwner>() + 2 * std::mem::size_of::<usize>())
        + 2 * std::mem::align_of::<BudgetOwner>()
        + 128;
    assert!(bound <= 512);
    assert_eq!(
        std::mem::size_of::<SharedBudgetOwner>(),
        std::mem::size_of::<Arc<BudgetOwner>>()
    );
    assert_eq!(
        std::mem::size_of::<SharedOwnershipCharge>(),
        std::mem::size_of::<Arc<OwnershipCharge>>()
    );
    let source = ProjectionSourceWorkingBudgetV1::new();
    let provider = ProviderBudget::new();
    let mut work = provider.reserve(4096).unwrap();
    source
        .prepare_provider_control_ownership_from(&mut work, &provider)
        .unwrap()
        .commit();
    drop(work);
    assert_eq!(provider.observation().unwrap().owned_bytes, 512);
    let last = source.clone();
    drop(source);
    assert_eq!(provider.observation().unwrap().owned_bytes, 512);
    drop(last);
    assert_eq!(provider.observation().unwrap().owned_bytes, 0);
}
