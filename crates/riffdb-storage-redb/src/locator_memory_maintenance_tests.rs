// req: TXN-042, STO-032, REC-001
use super::*;
use std::sync::Barrier;

fn counters(pool: &NativeLocatorPool) -> (usize, usize, usize, usize) {
    let state = lock(&pool.budget.state);
    (
        state.total,
        state.ordinary,
        state.owners,
        state.ordinary_owners,
    )
}

#[test]
fn maintenance_charge_shares_pressure_and_preserves_progress_class_and_owner_quota() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let baseline = counters(&pool);
    let anchor = pool.exhaust_ordinary_for_tests();
    let before = counters(&pool);
    assert!(pool.admit(1, LocatorAllocationClass::Ordinary).is_err());
    let work = anchor.admit_maintenance(4096).unwrap();
    assert!(Arc::ptr_eq(&anchor.budget, &work.budget));
    assert!(work.family.is_none());
    assert_eq!(work.class, LocatorAllocationClass::Maintenance);
    assert_eq!(
        counters(&pool),
        (before.0 + 4096, before.1, before.2, before.3)
    );
    let remaining = LOCATOR_TOTAL_BYTES - pool.charged_bytes();
    let occupied = pool
        .admit(remaining, LocatorAllocationClass::Maintenance)
        .unwrap();
    assert_eq!(
        work.admit_maintenance(1).unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(
        pool.admit(1, LocatorAllocationClass::Maintenance)
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(pool.charged_bytes(), LOCATOR_TOTAL_BYTES);
    drop(occupied);
    drop(work);
    assert_eq!(counters(&pool), before);
    drop(anchor);
    assert_eq!(counters(&pool), baseline);
}

#[test]
fn maintenance_charge_final_alias_releases_once_and_disabled_admission_stays_closed() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let baseline = counters(&pool);
    let anchor = pool.admit(128, LocatorAllocationClass::Ordinary).unwrap();
    let work = Arc::new(anchor.admit_maintenance(8192).unwrap());
    let alias = Arc::clone(&work);
    drop(anchor);
    assert_eq!(
        counters(&pool),
        (baseline.0 + 8192, baseline.1, baseline.2, baseline.3)
    );
    let before_refusal = counters(&pool);
    assert_eq!(
        work.admit_maintenance(usize::MAX).unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(counters(&pool), before_refusal);
    // This is the actual existing failed-worker admission fence. Releasing
    // retained charges must work, but must not reopen future admission.
    drop(WorkerAdmissionGuard(Arc::clone(&pool)));
    assert_eq!(
        work.admit_maintenance(1).unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(counters(&pool), before_refusal);
    drop(work);
    assert_eq!(counters(&pool), before_refusal);
    drop(alias);
    assert_eq!(counters(&pool), baseline);
    assert!(pool.admit(0, LocatorAllocationClass::Maintenance).is_err());
}

#[test]
fn competing_maintenance_charges_share_one_atomic_remaining_capacity() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let first = pool.admit(128, LocatorAllocationClass::Ordinary).unwrap();
    let second = pool
        .admit(128, LocatorAllocationClass::Maintenance)
        .unwrap();
    let before = counters(&pool);
    let occupied = pool
        .admit(
            LOCATOR_TOTAL_BYTES - before.0 - 4096,
            LocatorAllocationClass::Maintenance,
        )
        .unwrap();
    let held = Barrier::new(3);
    let release = Barrier::new(3);
    let (results, peak) = std::thread::scope(|scope| {
        let one = scope.spawn(|| {
            let result = first.admit_maintenance(4096);
            held.wait();
            release.wait();
            result.is_ok()
        });
        let two = scope.spawn(|| {
            let result = second.admit_maintenance(4096);
            held.wait();
            release.wait();
            result.is_ok()
        });
        held.wait();
        let peak = counters(&pool);
        // Release both owners before assertions so failure cannot strand a
        // test worker behind a synchronization barrier.
        release.wait();
        ([one.join().unwrap(), two.join().unwrap()], peak)
    });
    assert_eq!(results.into_iter().filter(|admitted| *admitted).count(), 1);
    assert_eq!(peak, (LOCATOR_TOTAL_BYTES, before.1, before.2, before.3));
    drop(occupied);
    assert_eq!(counters(&pool), before);
}
