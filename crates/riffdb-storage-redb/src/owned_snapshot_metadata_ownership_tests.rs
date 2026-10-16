// req: OQ-040, STO-032, TXN-042

// A subprocess isolates actual global native retirement from other unit tests.
// Explicit bounded queue drains synchronize destruction; no sleeps or retries.
fn metadata_isolated_child(test_name: &str) -> bool {
    const CHILD: &str = "RIFFDB_SHARED_METADATA_CHILD";
    if std::env::var(CHILD).as_deref() == Ok(test_name) {
        return true;
    }
    let module = module_path!().split_once("::").unwrap().1;
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("{module}::{test_name}"),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, test_name)
        .status()
        .unwrap();
    assert!(
        status.success(),
        "isolated metadata ownership test failed: {status}"
    );
    false
}

fn drain_metadata_retirement() {
    let pool = crate::locator_memory::NativeLocatorPool::global();
    for _ in 0..4096 {
        if !pool.drain_retired_step() {
            return;
        }
    }
    panic!("bounded metadata physical retirement did not finish");
}

#[test]
fn separate_sources_share_actual_resolver_and_view_metadata_owners() {
    if !metadata_isolated_child("separate_sources_share_actual_resolver_and_view_metadata_owners") {
        return;
    }
    let (_scope, store, snapshot, root) = pending_checkpoint_composite_fixture();
    let RedbReadAccess::Composite(view) = &snapshot.access else {
        panic!("genuine composite source required");
    };
    assert!(root.retained_history_custody_bound() > 0);
    assert!(
        view.retained_provider_nonlocator_bound().unwrap()
            > root.retained_history_custody_bound() as u64
    );
    let budget = ProviderBudget::new();
    let first = snapshot
        .with_provider_ownership_admitted_v1(&budget)
        .unwrap();
    let before = budget.observation().unwrap();
    let held = budget
        .reserve(
            EXACT_PROVIDER_BUDGET_BYTES
                - before.fixed_owner_bytes
                - before.owned_bytes
                - before.reserved_bytes
                - 32 * 1024,
        )
        .unwrap();
    let second = RedbOwnedSnapshot::from_read_access(first.access.clone())
        .with_provider_ownership_admitted_v1(&budget)
        .expect("the same physical resolver and view require only unique 32-KiB source controls");
    assert_eq!(
        second.provider_ownership.as_ref().unwrap()._source.bytes(),
        32 * 1024
    );
    assert_eq!(
        budget.observation().unwrap().owned_bytes - before.owned_bytes,
        32 * 1024
    );
    drop(held);
    drop(first);
    drop(second);
    drop(root);
    drop(store);
    drain_metadata_retirement();
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn resolver_only_alias_keeps_its_actual_metadata_charge_until_queued_retirement() {
    if !metadata_isolated_child(
        "resolver_only_alias_keeps_its_actual_metadata_charge_until_queued_retirement",
    ) {
        return;
    }
    let (_scope, store, snapshot, root) = pending_checkpoint_composite_fixture();
    let resolver = root.history_resolver_for_tests().unwrap().clone();
    let physical_bound = resolver.retained_charge_bound() as u64;
    assert!(physical_bound > 0);
    let budget = ProviderBudget::new();
    let source = snapshot
        .with_provider_ownership_admitted_v1(&budget)
        .unwrap();
    drop(source);
    drop(root);
    drop(store);
    drain_metadata_retirement();
    assert!(
        budget.observation().unwrap().owned_bytes >= physical_bound,
        "a resolver alias must retain its own metadata ownership after all source and engine owners disappear"
    );
    drop(resolver);
    drain_metadata_retirement();
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn root_only_alias_keeps_lazy_table_handle_ownership_after_source_capsules_drop() {
    if !metadata_isolated_child(
        "root_only_alias_keeps_lazy_table_handle_ownership_after_source_capsules_drop",
    ) {
        return;
    }
    let (_scope, store, snapshot, root) = pending_checkpoint_composite_fixture();
    // Open real redb handles, including their owned table-name Strings.
    root.meta_table().unwrap();
    root.journal_byte_table(crate::journal::JournalTable::Commits)
        .unwrap()
        .unwrap();
    let resolver_bound = root.retained_history_custody_bound() as u64;
    assert!(resolver_bound > 0);
    let budget = ProviderBudget::new();
    let source = snapshot
        .with_provider_ownership_admitted_v1(&budget)
        .unwrap();
    drop(source);
    drop(store);
    drain_metadata_retirement();
    assert!(
        root.meta_table()
            .unwrap()
            .get(crate::layout::META_APPLICATION_SEQUENCE)
            .unwrap()
            .is_some()
    );
    let family = root.locator_capability().provider_family_pool().unwrap();
    let guard = family.lock_provider_family().unwrap();
    let other_live_owners =
        guard.provider_owned_bytes() + root.engine_custody().unwrap().provider_charge_bytes();
    assert!(
        budget.observation().unwrap().owned_bytes >= other_live_owners + resolver_bound + 32 * 1024,
        "the actual root owns its bounded lazy table handles separately from source capsules, engine and locator owners"
    );
    drop(guard);
    drop(family);
    drop(root);
    drain_metadata_retirement();
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
