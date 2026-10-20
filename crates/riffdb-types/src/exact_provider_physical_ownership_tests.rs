// req: OQ-040
use super::*;

#[test]
fn physical_fragments_release_independently_of_shared_family_control() {
    let budget = ProviderBudget::new();
    let mut balance = budget
        .reserve(1024)
        .unwrap()
        .commit_physical_balance()
        .unwrap();
    balance.release_fragment(700);
    assert_eq!(balance.bytes(), 324);
    assert_eq!(budget.observation().unwrap().owned_bytes, 324);
    assert_eq!(budget.observation().unwrap().allocations, 1);
    balance.release_fragment(300);
    assert_eq!(budget.observation().unwrap().owned_bytes, 24);
    drop(balance);
    let final_state = budget.observation().unwrap();
    assert_eq!(final_state.owned_bytes, 0);
    assert_eq!(final_state.allocations, 0);
    assert_eq!(final_state.released_allocations, 1);
}

#[test]
fn private_increment_cancellation_and_commit_preserve_one_owner() {
    let budget = ProviderBudget::new();
    let mut balance = budget
        .reserve(512)
        .unwrap()
        .commit_physical_balance()
        .unwrap();
    let before = budget.observation().unwrap();
    {
        let _private = balance
            .prepare_increment(budget.reserve(256).unwrap())
            .unwrap();
        assert_eq!(budget.observation().unwrap().owned_bytes, 768);
    }
    assert_eq!(balance.bytes(), 512);
    assert_eq!(budget.observation().unwrap(), before);
    balance
        .prepare_increment(budget.reserve(256).unwrap())
        .unwrap()
        .commit();
    assert_eq!(balance.bytes(), 768);
    assert_eq!(budget.observation().unwrap().allocations, 1);
    balance.release_fragment(744);
    assert_eq!(budget.observation().unwrap().owned_bytes, 24);
    drop(balance);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn foreign_increment_refuses_without_changing_either_family() {
    let first = ProviderBudget::new();
    let foreign = ProviderBudget::new();
    let mut balance = first
        .reserve(512)
        .unwrap()
        .commit_physical_balance()
        .unwrap();
    let before = first.observation().unwrap();
    assert!(
        balance
            .prepare_increment(foreign.reserve(128).unwrap())
            .is_err()
    );
    assert_eq!(first.observation().unwrap(), before);
    assert_eq!(foreign.observation().unwrap().reserved_bytes, 0);
    assert_eq!(foreign.observation().unwrap().owned_bytes, 0);
    assert_eq!(balance.bytes(), 512);
}

#[test]
fn private_first_balance_rolls_back_before_any_family_publication() {
    let budget = ProviderBudget::new();
    let private = budget
        .reserve(512)
        .unwrap()
        .commit_physical_balance()
        .unwrap();
    assert_eq!(budget.observation().unwrap().owned_bytes, 512);
    drop(private);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().allocations, 0);
}

#[test]
fn physical_fragment_underflow_preserves_owned_bytes_and_refuses() {
    let budget = ProviderBudget::new();
    let mut balance = budget
        .reserve(64)
        .unwrap()
        .commit_physical_balance()
        .unwrap();
    balance.release_fragment(65);
    assert_eq!(balance.bytes(), 64);
    assert!(budget.observation().is_err());
    assert!(budget.reserve(1).is_err());
}

fn admit_family(
    family: &PhysicalOwnershipFamily,
    budget: &ProviderBudget,
) -> super::super::SharedOwnershipCharge {
    let mut guard = family.lock().unwrap();
    let needed = guard.provider_ownership_needed(budget).unwrap();
    // Reservation::split preserves a nonzero caller-owned root allowance.
    let mut work = budget.reserve(needed.checked_add(1).unwrap()).unwrap();
    guard
        .prepare_provider_ownership(&mut work, budget)
        .unwrap()
        .commit()
}

#[test]
fn shared_family_cuts_charge_live_members_once_until_physical_release() {
    let budget = ProviderBudget::new();
    let family = PhysicalOwnershipFamily::new(64);
    let discarded = family.register_member(512).unwrap();
    let first = family.register_member(128).unwrap();
    let second = family.register_member(256).unwrap();
    drop(discarded);
    assert_eq!(family.observation().unwrap().uncovered, 384);
    let marker = admit_family(&family, &budget);
    assert_eq!(budget.observation().unwrap().owned_bytes, 64 + 128 + 384);
    let repeated = admit_family(&family, &budget);
    assert!(marker.same_allocation(&repeated));
    assert_eq!(budget.observation().unwrap().owned_bytes, 64 + 128 + 384);
    drop(first);
    assert_eq!(family.observation().unwrap().covered, 256);
    assert_eq!(budget.observation().unwrap().owned_bytes, 64 + 128 + 256);
    drop(second);
    assert_eq!(family.observation().unwrap().covered, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 64 + 128);
    drop(repeated);
    drop(marker);
    drop(family);
    let final_state = budget.observation().unwrap();
    assert_eq!(final_state.owned_bytes, 0);
    assert_eq!(final_state.reserved_bytes, 0);
    assert_eq!(final_state.allocations, 0);
}

#[test]
fn cancelled_family_first_plan_keeps_cut_and_members_unpublished() {
    let budget = ProviderBudget::new();
    let family = PhysicalOwnershipFamily::new(64);
    let member = family.register_member(256).unwrap();
    let before = family.observation().unwrap();
    let mut guard = family.lock().unwrap();
    let mut work = budget
        .reserve(
            guard
                .provider_ownership_needed(&budget)
                .unwrap()
                .checked_add(1)
                .unwrap(),
        )
        .unwrap();
    let private = guard
        .prepare_provider_ownership(&mut work, &budget)
        .unwrap();
    assert_eq!(budget.observation().unwrap().owned_bytes, 256 + 64 + 128);
    drop(private);
    drop(work);
    assert_eq!(guard.observation(), before);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    drop(guard);
    let marker = admit_family(&family, &budget);
    drop(member);
    drop(marker);
    drop(family);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn cancelled_family_increment_preserves_original_cut_and_owner() {
    let budget = ProviderBudget::new();
    let family = PhysicalOwnershipFamily::new(64);
    let original = family.register_member(100).unwrap();
    let marker = admit_family(&family, &budget);
    let successor = family.register_member(200).unwrap();
    let before = family.observation().unwrap();
    let owned = budget.observation().unwrap().owned_bytes;
    let mut guard = family.lock().unwrap();
    let mut work = budget.reserve(201).unwrap();
    let private = guard
        .prepare_provider_ownership(&mut work, &budget)
        .unwrap();
    assert_eq!(budget.observation().unwrap().owned_bytes, owned + 200);
    drop(private);
    drop(work);
    assert_eq!(guard.observation(), before);
    assert_eq!(budget.observation().unwrap().owned_bytes, owned);
    drop(guard);
    let successor_marker = admit_family(&family, &budget);
    assert!(marker.same_allocation(&successor_marker));
    drop(original);
    assert_eq!(budget.observation().unwrap().owned_bytes, 64 + 128 + 200);
    drop(successor);
    drop(successor_marker);
    drop(marker);
    drop(family);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn shared_family_foreign_graph_refusal_preserves_both_budgets() {
    let budget = ProviderBudget::new();
    let foreign = ProviderBudget::new();
    let family = PhysicalOwnershipFamily::new(64);
    let member = family.register_member(128).unwrap();
    let marker = admit_family(&family, &budget);
    let before_family = family.observation().unwrap();
    let before_budget = budget.observation().unwrap();
    let mut guard = family.lock().unwrap();
    assert!(guard.provider_ownership_needed(&foreign).is_err());
    let mut work = foreign.reserve(256).unwrap();
    assert!(
        guard
            .prepare_provider_ownership(&mut work, &foreign)
            .is_err()
    );
    drop(work);
    assert_eq!(guard.observation(), before_family);
    assert_eq!(budget.observation().unwrap(), before_budget);
    assert_eq!(foreign.observation().unwrap().owned_bytes, 0);
    assert_eq!(foreign.observation().unwrap().reserved_bytes, 0);
    drop(guard);
    drop(member);
    drop(marker);
    drop(family);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn family_ordinal_and_capacity_overflow_permanently_refuse_without_wrapping() {
    for ordinal_overflow in [true, false] {
        let family = PhysicalOwnershipFamily::new(64);
        {
            let mut state = family.0.state.lock().unwrap();
            if ordinal_overflow {
                state.next = u64::MAX;
            } else {
                state.uncovered = u64::MAX;
            }
        }
        let before = family.observation().unwrap();
        assert!(family.register_member(1).is_err());
        let after = family.observation().unwrap();
        assert!(!after.enabled);
        assert_eq!(after.next, before.next);
        assert_eq!(after.uncovered, before.uncovered);
        assert!(family.register_member(1).is_err());
        assert!(
            family
                .lock()
                .unwrap()
                .provider_ownership_needed(&ProviderBudget::new())
                .is_err()
        );
    }
}

#[test]
fn family_invalid_release_preserves_live_charge_and_refuses_admission() {
    let budget = ProviderBudget::new();
    let family = PhysicalOwnershipFamily::new(64);
    let member = family.register_member(16).unwrap();
    let ordinal = member.ordinal;
    let marker = admit_family(&family, &budget);
    let owned = budget.observation().unwrap().owned_bytes;
    family.release_member_bytes(ordinal, 17);
    assert!(!family.observation().unwrap().enabled);
    assert_eq!(family.observation().unwrap().covered, 16);
    assert_eq!(budget.observation().unwrap().owned_bytes, owned);
    assert!(
        family
            .lock()
            .unwrap()
            .provider_ownership_needed(&budget)
            .is_err()
    );
    drop(member);
    drop(marker);
    drop(family);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn family_cut_and_final_release_follow_explicit_concurrent_schedule() {
    let budget = ProviderBudget::new();
    let family = PhysicalOwnershipFamily::new(64);
    let member = family.register_member(256).unwrap();
    let mut guard = family.lock().unwrap();
    let mut work = budget
        .reserve(
            guard
                .provider_ownership_needed(&budget)
                .unwrap()
                .checked_add(1)
                .unwrap(),
        )
        .unwrap();
    let private = guard
        .prepare_provider_ownership(&mut work, &budget)
        .unwrap();
    let (ready, started) = std::sync::mpsc::channel();
    let (done, finished) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        ready.send(()).unwrap();
        drop(member);
        done.send(()).unwrap();
    });
    started
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    // The held family guard prevents member destruction from observing a
    // partly published cut, regardless of the worker's next instruction.
    assert!(matches!(
        finished.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    let marker = private.commit();
    assert_eq!(guard.observation().covered, 256);
    drop(work);
    drop(guard);
    worker.join().unwrap();
    finished
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    assert_eq!(family.observation().unwrap().covered, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 64 + 128);
    drop(marker);
    drop(family);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn concurrent_successor_registration_stays_outside_the_prepared_cut() {
    let budget = ProviderBudget::new();
    let family = PhysicalOwnershipFamily::new(64);
    let original = family.register_member(100).unwrap();
    let mut guard = family.lock().unwrap();
    let mut work = budget
        .reserve(
            guard
                .provider_ownership_needed(&budget)
                .unwrap()
                .checked_add(1)
                .unwrap(),
        )
        .unwrap();
    let private = guard
        .prepare_provider_ownership(&mut work, &budget)
        .unwrap();
    let next_family = family.clone();
    let (ready, started) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        ready.send(()).unwrap();
        next_family.register_member(200).unwrap()
    });
    started
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    let marker = private.commit();
    assert_eq!(guard.observation().cut, 1);
    drop(work);
    drop(guard);
    let successor = worker.join().unwrap();
    let after = family.observation().unwrap();
    assert_eq!(
        (after.cut, after.next, after.covered, after.uncovered),
        (1, 2, 100, 200)
    );
    assert_eq!(budget.observation().unwrap().owned_bytes, 64 + 128 + 100);
    let next_marker = admit_family(&family, &budget);
    assert!(marker.same_allocation(&next_marker));
    drop(original);
    drop(successor);
    drop(next_marker);
    drop(marker);
    drop(family);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

fn admit_shared_cell(cell: &SharedPhysicalOwnershipCell, bound: u64, budget: &ProviderBudget) {
    let needed = cell.provider_ownership_needed(bound, budget).unwrap();
    let mut work = budget.reserve(needed + 1).unwrap();
    cell.prepare_provider_ownership_from(bound, &mut work, budget)
        .unwrap()
        .commit();
}

#[test]
fn shared_cell_aliases_have_zero_increment_until_final_physical_owner_release() {
    let budget = ProviderBudget::new();
    let cell = crate::OwnedCapacitySharedV1::new(SharedPhysicalOwnershipCell::new());
    let alias = cell.clone();
    assert_eq!(cell.provider_ownership_needed(512, &budget), Ok(512));
    admit_shared_cell(&cell, 512, &budget);
    let before = budget.observation().unwrap();
    assert_eq!(before.owned_bytes, 512);
    assert_eq!(before.allocations, 1);
    assert_eq!(alias.provider_ownership_needed(512, &budget), Ok(0));
    admit_shared_cell(&alias, 512, &budget);
    assert_eq!(budget.observation().unwrap(), before);
    assert_eq!(alias.owned_bytes(), Ok(512));
    drop(cell);
    assert_eq!(budget.observation().unwrap(), before);
    drop(alias);
    let final_state = budget.observation().unwrap();
    assert_eq!(final_state.owned_bytes, 0);
    assert_eq!(final_state.reserved_bytes, 0);
    assert_eq!(final_state.allocations, 0);
    assert_eq!(final_state.reservations, 0);
}

#[test]
fn shared_cell_foreign_insufficient_and_wrong_reservation_refuse_without_attachment() {
    let budget = ProviderBudget::new();
    let foreign = ProviderBudget::new();
    let cell = SharedPhysicalOwnershipCell::new();
    assert_eq!(
        cell.provider_ownership_needed(0, &budget),
        Err(BudgetUnavailable)
    );
    {
        let mut work = foreign.reserve(513).unwrap();
        let before = foreign.observation().unwrap();
        assert!(
            cell.prepare_provider_ownership_from(512, &mut work, &budget)
                .is_err()
        );
        assert_eq!(foreign.observation().unwrap(), before);
        assert_eq!(cell.owned_bytes(), Ok(0));
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    admit_shared_cell(&cell, 512, &budget);
    let before = budget.observation().unwrap();
    assert_eq!(
        cell.provider_ownership_needed(513, &budget),
        Err(BudgetUnavailable)
    );
    assert_eq!(
        cell.provider_ownership_needed(512, &foreign),
        Err(BudgetUnavailable)
    );
    {
        let mut work = budget.reserve(514).unwrap();
        let admitted = budget.observation().unwrap();
        assert!(
            cell.prepare_provider_ownership_from(513, &mut work, &budget)
                .is_err()
        );
        assert_eq!(budget.observation().unwrap(), admitted);
    }
    {
        let mut work = foreign.reserve(513).unwrap();
        let admitted = foreign.observation().unwrap();
        assert!(
            cell.prepare_provider_ownership_from(512, &mut work, &foreign)
                .is_err()
        );
        assert_eq!(foreign.observation().unwrap(), admitted);
    }
    assert_eq!(cell.owned_bytes(), Ok(512));
    assert_eq!(budget.observation().unwrap(), before);
    assert_eq!(foreign.observation().unwrap().owned_bytes, 0);
    assert_eq!(foreign.observation().unwrap().reserved_bytes, 0);
    drop(cell);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn shared_cell_cancelled_private_attachment_releases_and_allows_another_graph() {
    let budget = ProviderBudget::new();
    let later = ProviderBudget::new();
    let cell = SharedPhysicalOwnershipCell::new();
    {
        let mut work = budget.reserve(513).unwrap();
        let prepared = cell
            .prepare_provider_ownership_from(512, &mut work, &budget)
            .unwrap();
        let private = budget.observation().unwrap();
        assert_eq!(private.owned_bytes, 512);
        assert_eq!(private.reserved_bytes, 1);
        assert_eq!(private.allocations, 1);
        drop(prepared);
        assert_eq!(cell.owned_bytes(), Ok(0));
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().allocations, 0);
        assert_eq!(cell.provider_ownership_needed(512, &later), Ok(512));
    }
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().reservations, 0);
    admit_shared_cell(&cell, 512, &later);
    assert_eq!(later.observation().unwrap().owned_bytes, 512);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    drop(cell);
    assert_eq!(later.observation().unwrap().owned_bytes, 0);
    assert_eq!(later.observation().unwrap().allocations, 0);
}

#[test]
fn shared_cell_held_guard_serializes_commit_and_cancellation_on_explicit_schedule() {
    for commit_first in [false, true] {
        let budget = ProviderBudget::new();
        let cell = crate::OwnedCapacitySharedV1::new(SharedPhysicalOwnershipCell::new());
        let mut work = budget.reserve(513).unwrap();
        let private = cell
            .prepare_provider_ownership_from(512, &mut work, &budget)
            .unwrap();
        let worker_cell = cell.clone();
        let worker_budget = budget.clone();
        let (blocked, observed_block) = std::sync::mpsc::channel();
        let (done, completed) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            // Observe actual contention before allowing the first candidate to
            // finish. A ready signal before lock acquisition would not prove it.
            assert!(matches!(
                worker_cell.provider.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            blocked.send(()).unwrap();
            let mut next = worker_budget.reserve(513).unwrap();
            worker_cell
                .prepare_provider_ownership_from(512, &mut next, &worker_budget)
                .unwrap()
                .commit();
            drop(next);
            done.send(worker_cell.owned_bytes().unwrap()).unwrap();
        });
        observed_block
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();
        assert!(matches!(
            completed.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        if commit_first {
            private.commit();
        } else {
            drop(private);
        }
        drop(work);
        assert_eq!(
            completed
                .recv_timeout(std::time::Duration::from_secs(30))
                .unwrap(),
            512
        );
        worker.join().unwrap();
        let attached = budget.observation().unwrap();
        assert_eq!(attached.owned_bytes, 512);
        assert_eq!(attached.allocations, 1);
        assert_eq!(attached.reserved_bytes, 0);
        assert_eq!(attached.reservations, 0);
        assert_eq!(cell.provider_ownership_needed(512, &budget), Ok(0));
        drop(cell);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().allocations, 0);
    }
}
