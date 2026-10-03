// req: OQ-040, PRJ-004
use super::*;

#[test]
fn lifecycle_observation_keeps_retired_pinned_owners_until_final_release() {
    let budget = ProviderBudget::new();
    let mut first = budget.register_provider_owner().unwrap();
    let second = budget.register_provider_owner().unwrap();
    first.pin_admitted().unwrap();
    first.pin_admitted().unwrap();
    let before = budget.observation().unwrap();
    assert_eq!(
        (
            before.active_owners,
            before.retiring_owners,
            before.pinned_owners,
            before.pins,
            before.released_owners
        ),
        (2, 0, 1, 2, 0)
    );
    first.retire().unwrap();
    first.retire().unwrap();
    let retired = budget.observation().unwrap();
    assert_eq!(
        (
            retired.active_owners,
            retired.retiring_owners,
            retired.pinned_owners,
            retired.pins,
            retired.released_owners
        ),
        (1, 1, 1, 2, 0)
    );
    assert!(first.pin_admitted().is_err());
    assert_eq!(budget.observation().unwrap(), retired);
    first.release_pin().unwrap();
    assert_eq!(budget.observation().unwrap().pinned_owners, 1);
    first.release_pin().unwrap();
    assert_eq!(budget.observation().unwrap().pinned_owners, 0);
    drop(first);
    drop(second);
    let released = budget.observation().unwrap();
    assert_eq!(
        (
            released.active_owners,
            released.retiring_owners,
            released.pinned_owners,
            released.pins,
            released.released_owners
        ),
        (0, 0, 0, 0, 2)
    );
}

#[test]
fn lifecycle_invalid_pin_release_fails_closed_without_underflow() {
    let budget = ProviderBudget::new();
    let mut owner = budget.register_provider_owner().unwrap();
    assert!(owner.release_pin().is_err());
    assert!(budget.observation().is_err());
    assert!(budget.reserve(1).is_err());
}

#[test]
fn lifecycle_counter_overflow_refuses_without_publishing_partial_counts() {
    let budget = ProviderBudget::new();
    let mut owner = budget.register_provider_owner().unwrap();
    let before = {
        let mut accounting = budget.0.lock().unwrap();
        accounting.counters.retiring_owners = u64::MAX;
        accounting.counters
    };
    assert!(owner.retire().is_err());
    assert!(budget.observation().is_err());
    assert_eq!(budget.0.lock().unwrap().counters, before);
    drop(owner);
    assert_eq!(budget.0.lock().unwrap().counters, before);
}
