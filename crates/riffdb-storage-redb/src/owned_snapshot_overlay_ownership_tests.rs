// req: OQ-040, TXN-042, STO-032
#[test]
fn separate_provider_sources_share_the_actual_overlay_allocation_owners() {
    use riffdb_storage_api::AuthoritativePointReader;
    let (_scope, store, snapshot, root) = pending_checkpoint_composite_fixture();
    let RedbReadAccess::Composite(view) = &snapshot.access else {
        panic!("fixture must contain a genuine command overlay");
    };
    let overlay_bytes = u64::try_from(view.overlay().charged_bytes()).unwrap();
    assert!(overlay_bytes > 0);
    // Keep room for a complete new source and all currently billed non-overlay
    // state, but not a second charge for the already retained overlay payload.
    let descriptor = crate::composite_view::RedbCompositeReadView::admit_descriptor(
        LocatorAllocationClass::Ordinary,
    )
    .unwrap();
    let new_source_room = 32 * 1024
        + u64::try_from(root.retained_history_custody_bound()).unwrap()
        + u64::try_from(descriptor.bytes()).unwrap()
        + u64::try_from(view.changelog_suffix().retained_ownership_bound()).unwrap();
    drop(descriptor);
    let command = crate::command_authority::command_member_at_access(
        &snapshot.access,
        riffdb_types::CommitSequence::new(2).unwrap(),
    )
    .unwrap()
    .unwrap();
    let outcome = command.base().outcome().clone();
    let provenance = command.base().provenance().clone();
    drop(command);
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
                - new_source_room,
        )
        .unwrap();
    let second = RedbOwnedSnapshot::from_read_access(first.access.clone())
        .with_provider_ownership_admitted_v1(&budget)
        .expect("a separately captured source shares the same physical overlay");
    assert!(budget.observation().unwrap().owned_bytes - before.owned_bytes <= new_source_room);
    drop(held);
    drop(first);
    drop(root);
    drop(store);
    assert_eq!(
        second.read_stored_outcome(outcome.identity()).unwrap(),
        Some(outcome)
    );
    assert_eq!(
        second.read_provenance(provenance.provenance_id()).unwrap(),
        Some(provenance)
    );
    drop(second);
    let pool = crate::locator_memory::NativeLocatorPool::global();
    for _ in 0..4096 {
        if budget.observation().unwrap().owned_bytes == 0 {
            break;
        }
        pool.drain_retired_step();
    }
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
