// req: OQ-040, TXN-042, STO-032, END-008
#[test]
fn provider_root_metadata_charge_survives_the_actual_final_tracked_weak_reference() {
    let (_scope, store, snapshot) = fixture();
    let root_identity = match &snapshot.access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => root.identity(),
        RedbReadAccess::Composite(_) => panic!("fixture must expose its physical checkpoint root"),
    };
    // Retain the actual native registry, not a replacement accounting object.
    // Its fixed native reservation covers residual allocations throughout this
    // deliberately delayed weak cleanup, including after the store closes.
    let tracking = store.shared.root_tracking_for_tests();
    let weak = {
        let roots = tracking.lock().unwrap();
        roots
            .iter()
            .find(|weak| {
                weak.upgrade()
                    .is_some_and(|root| root.identity() == root_identity)
            })
            .expect("the captured source is registered")
            .clone()
    };
    let budget = ProviderBudget::new();
    let snapshot = snapshot
        .with_provider_ownership_admitted_v1(&budget)
        .unwrap();
    assert!(budget.observation().unwrap().owned_bytes > 0);
    drop(snapshot);
    drop(store);
    let pool = crate::locator_memory::NativeLocatorPool::global();
    for _ in 0..4096 {
        if !pool.drain_retired_step() {
            break;
        }
    }
    assert!(
        weak.upgrade().is_none(),
        "weak custody must not keep the root or its engine alive"
    );
    // Arc's entire inline allocation remains while its real Weak exists. This
    // independent storage floor detects a cell released at final strong Drop.
    let retained = budget.observation().unwrap().owned_bytes;
    assert!(
        retained >= std::mem::size_of::<crate::checkpoint_root::CheckpointRoot>() as u64,
        "the actual residual root allocation must remain provider-accounted"
    );
    tracking.lock().unwrap().clear_for_tests();
    assert!(tracking.lock().unwrap().is_empty());
    for _ in 0..4096 {
        if !pool.drain_retired_step() {
            break;
        }
    }
    assert_eq!(
        budget.observation().unwrap().owned_bytes,
        retained,
        "the separately retained native weak alias still owns the same physical allocation"
    );
    drop(weak);
    for _ in 0..4096 {
        if !pool.drain_retired_step() {
            break;
        }
    }
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    drop(tracking);
}
