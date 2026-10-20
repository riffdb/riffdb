// req: STO-032, REC-001, TXN-042
use super::*;
fn domain() -> (NativeLocatorPool, LocatorBuildDomain) {
    let pool = NativeLocatorPool::isolated_for_tests();
    let domain = LocatorBuildDomain::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    (pool, domain)
}
#[test]
fn open_dirty_activation_preserves_already_admitted_construction() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let baseline = pool.charged_bytes();
    let owner = LocatorBuildDomain::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    let work = owner.register().unwrap();
    owner.rearm_precondition().unwrap();
    owner.rearm_after_verified_dirty().unwrap();
    assert!(!owner.is_cancelled());
    assert_eq!(owner.registrations_for_test(), 1);
    drop(work);
    drop(owner);
    assert_eq!(pool.charged_bytes(), baseline);
}
#[test]
fn expired_close_refuses_without_reopening_or_abandoning_registration() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let baseline = pool.charged_bytes();
    let owner = LocatorBuildDomain::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    let work = owner.register().unwrap();
    let attempt = owner.begin_close(Instant::now()).unwrap();
    assert!(attempt.await_quiescence().is_err());
    assert!(owner.is_cancelled());
    assert!(owner.register().is_err());
    assert!(owner.rearm_precondition().is_err());
    assert!(owner.rearm_after_verified_dirty().is_err());
    assert_eq!(owner.registrations_for_test(), 1);
    drop(owner);
    assert_eq!(work.domain().registrations_for_test(), 1);
    assert!(work.is_cancelled());
    assert!(pool.charged_bytes() > baseline);
    drop(work);
    assert_eq!(pool.charged_bytes(), baseline);
}
#[test]
fn notification_before_wait_and_closed_epoch_reactivation_preserve_exclusion() {
    let (_pool, owner) = domain();
    let work = owner.register().unwrap();
    let attempt = owner
        .begin_close(Instant::now() + LOCATOR_DRAIN_LIMIT)
        .unwrap();
    drop(work);
    let old = attempt.await_quiescence().unwrap();
    old.verify_under_mutation_lease().unwrap();
    assert!(owner.register().is_err());
    owner.rearm_precondition().unwrap();
    owner.rearm_after_verified_dirty().unwrap();
    assert!(old.verify_under_mutation_lease().is_err());
    let work = owner.register().unwrap();
    let next = owner
        .begin_close(Instant::now() + LOCATOR_DRAIN_LIMIT)
        .unwrap();
    drop(old);
    assert!(
        owner
            .begin_close(Instant::now() + LOCATOR_DRAIN_LIMIT)
            .is_err()
    );
    drop(work);
    next.await_quiescence()
        .unwrap()
        .verify_under_mutation_lease()
        .unwrap();
    assert!(owner.register().is_err());
}
#[test]
fn native_domain_admission_refuses_without_uncharged_control() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let occupied = pool.exhaust_ordinary_for_tests();
    let before = pool.charged_bytes();
    assert!(LocatorBuildDomain::try_new(&pool, LocatorAllocationClass::Ordinary).is_err());
    assert_eq!(pool.charged_bytes(), before);
    drop(occupied);
}
