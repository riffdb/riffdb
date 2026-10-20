// req: TXN-042, STO-032, REC-001
use super::*;
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

struct Entries {
    values: Vec<usize>,
    retired: Arc<AtomicUsize>,
}
impl LocatorRetireContents for Entries {
    fn retire_one(&mut self) -> bool {
        if self.values.pop().is_some() {
            self.retired.fetch_add(1, AtomicOrdering::SeqCst);
        }
        self.values.is_empty()
    }
}
struct DeferredChild {
    _child: Option<ChargedLocatorAllocation<Entries>>,
}
impl LocatorRetireContents for DeferredChild {
    fn retire_one(&mut self) -> bool {
        // Its bounded handle releases only during physical task Drop, after
        // retire_one returns and after the owner's barrier was enqueued.
        true
    }
}
fn deferred_child(
    pool: &NativeLocatorPool,
    child: ChargedLocatorAllocation<Entries>,
) -> ChargedLocatorAllocation<DeferredChild> {
    ChargedLocatorAllocation::try_new(pool, LocatorAllocationClass::Ordinary, 0, || {
        Ok(DeferredChild {
            _child: Some(child),
        })
    })
    .unwrap()
}
fn entries(
    pool: &NativeLocatorPool,
    count: usize,
    retired: &Arc<AtomicUsize>,
) -> ChargedLocatorAllocation<Entries> {
    ChargedLocatorAllocation::try_new(
        pool,
        LocatorAllocationClass::Ordinary,
        count * std::mem::size_of::<usize>(),
        || {
            Ok(Entries {
                values: (0..count).collect(),
                retired: Arc::clone(retired),
            })
        },
    )
    .expect("admitted entries")
}
fn drain(pool: &NativeLocatorPool) {
    while pool.drain_retired_step() {
        std::thread::yield_now();
    }
}

#[test]
fn locator_admission_precedes_constructor_and_reserves_maintenance_bytes() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let constructor_calls = AtomicUsize::new(0);
    let ordinary = pool
        .admit(LOCATOR_ORDINARY_BYTES, LocatorAllocationClass::Ordinary)
        .expect("ordinary capacity");
    assert!(
        ChargedLocatorAllocation::<Entries>::try_new(
            &pool,
            LocatorAllocationClass::Ordinary,
            0,
            || {
                constructor_calls.fetch_add(1, AtomicOrdering::SeqCst);
                unreachable!("constructor must not run after refusal")
            }
        )
        .is_err()
    );
    assert_eq!(constructor_calls.load(AtomicOrdering::SeqCst), 0);
    let maintenance = pool
        .admit(1024, LocatorAllocationClass::Maintenance)
        .expect("reserved progress capacity");
    assert!(
        pool.admit(usize::MAX, LocatorAllocationClass::Maintenance)
            .is_err()
    );
    drop(maintenance);
    drop(ordinary);
}

#[test]
fn locator_owner_pressure_counts_queued_owners_and_reserves_progress_owner() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let owners: Vec<_> = (0..LOCATOR_MAX_OWNERS - 1)
        .map(|_| {
            LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).expect("ordinary owner")
        })
        .collect();
    assert!(LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).is_err());
    let progress = LocatorOwner::try_new(&pool, LocatorAllocationClass::Maintenance)
        .expect("reserved progress owner");
    assert!(LocatorOwner::try_new(&pool, LocatorAllocationClass::Maintenance).is_err());
    drop(owners);
    assert_eq!(lock(&pool.budget.state).owners, LOCATOR_MAX_OWNERS);
    assert!(LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).is_err());
    pool.drain_retired_step();
    assert_eq!(
        lock(&pool.budget.state).owners,
        LOCATOR_MAX_OWNERS - LOCATOR_RETIRE_QUOTA
    );
    let resumed =
        LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).expect("reclaimed owner");
    drop(resumed);
    drop(progress);
    drain(&pool);
    assert_eq!(lock(&pool.budget.state).owners, 0);
}

#[test]
fn locator_generation_last_owner_releases_all_charges() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let retired = Arc::new(AtomicUsize::new(0));
    // Warm the reusable descriptor chunk; its admitted capacity is pool-owned.
    drop(entries(&pool, 1, &retired));
    drain(&pool);
    retired.store(0, AtomicOrdering::SeqCst);
    let baseline = pool.charged_bytes();
    let owner = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).expect("owner");
    let allocation = entries(&pool, 1000, &retired);
    let last = allocation.clone();
    drop(allocation);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(lock(&pool.retirement.state).queued_count, 0);
    drop(last);
    drop(owner);
    assert!(pool.charged_bytes() > baseline);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 0);
    let before = retired.load(AtomicOrdering::SeqCst);
    assert!(pool.drain_retired_step());
    assert!(retired.load(AtomicOrdering::SeqCst) - before <= LOCATOR_RETIRE_QUOTA);
    assert!(pool.charged_bytes() > baseline);
    drain(&pool);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 1000);
    assert_eq!(pool.charged_bytes(), baseline);
    assert_eq!(lock(&pool.budget.state).owners, 0);
}

#[test]
fn locator_cancelled_construction_releases_admission_and_reserved_descriptor() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let retired = Arc::new(AtomicUsize::new(0));
    drop(entries(&pool, 0, &retired));
    drain(&pool);
    let baseline = pool.charged_bytes();
    let free_before = lock(&pool.retirement.state).free;
    let result = ChargedLocatorAllocation::<Entries>::try_new(
        &pool,
        LocatorAllocationClass::Ordinary,
        4096,
        || Err(unavailable()),
    );
    assert!(result.is_err());
    assert_eq!(pool.charged_bytes(), baseline);
    assert_eq!(lock(&pool.retirement.state).free, free_before);
    assert_eq!(lock(&pool.retirement.state).queued_count, 0);
}

#[test]
fn locator_parallel_final_reference_and_reaper_schedule_is_bounded() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let retired = Arc::new(AtomicUsize::new(0));
    let allocation = entries(&pool, 1024, &retired);
    let worker_reference = allocation.clone();
    let release = Arc::new(Barrier::new(2));
    let queued = Arc::new(Barrier::new(2));
    std::thread::scope(|scope| {
        let release_worker = Arc::clone(&release);
        let queued_worker = Arc::clone(&queued);
        scope.spawn(move || {
            release_worker.wait();
            drop(worker_reference);
            queued_worker.wait();
        });
        drop(allocation);
        assert_eq!(lock(&pool.retirement.state).queued_count, 0);
        release.wait();
        queued.wait();
        assert_eq!(lock(&pool.retirement.state).queued_count, 1);
        assert_eq!(retired.load(AtomicOrdering::SeqCst), 0);
    });
    let simultaneous = Arc::new(Barrier::new(3));
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let pool = (pool).clone();
            let simultaneous = Arc::clone(&simultaneous);
            scope.spawn(move || {
                simultaneous.wait();
                pool.drain_retired_step();
            });
        }
        simultaneous.wait();
    });
    assert_eq!(
        retired.load(AtomicOrdering::SeqCst),
        2 * LOCATOR_RETIRE_QUOTA
    );
    drain(&pool);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 1024);
}

#[test]
fn locator_poison_refuses_new_admission_but_allows_bounded_reclamation() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let retired = Arc::new(AtomicUsize::new(0));
    let allocation = entries(&pool, 300, &retired);
    let poison_pool = (pool).clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison_pool.budget.state.lock().expect("unpoisoned budget");
            panic!("deterministic injected budget poison");
        })
        .join()
        .is_err()
    );
    assert!(pool.admit(1, LocatorAllocationClass::Maintenance).is_err());
    assert!(LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).is_err());
    drop(allocation);
    assert!(pool.drain_retired_step());
    assert_eq!(retired.load(AtomicOrdering::SeqCst), LOCATOR_RETIRE_QUOTA);
    drain(&pool);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 300);
}

#[test]
fn locator_descriptor_reservation_refuses_before_constructor_and_rolls_back_charge() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let allocation_bytes = std::mem::size_of::<AllocationOwner<Entries>>()
        + std::mem::size_of::<AllocationTask<Entries>>()
        + 3 * ALLOCATION_OVERHEAD;
    let held = pool
        .admit(
            LOCATOR_ORDINARY_BYTES - allocation_bytes,
            LocatorAllocationClass::Ordinary,
        )
        .expect("leave allocation but no descriptor chunk capacity");
    let baseline = pool.charged_bytes();
    let constructor_calls = AtomicUsize::new(0);
    assert!(
        ChargedLocatorAllocation::<Entries>::try_new(
            &pool,
            LocatorAllocationClass::Ordinary,
            0,
            || {
                constructor_calls.fetch_add(1, AtomicOrdering::SeqCst);
                unreachable!("descriptor must be admitted before constructor")
            }
        )
        .is_err()
    );
    assert_eq!(constructor_calls.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(pool.charged_bytes(), baseline);
    assert!(lock(&pool.retirement.state).chunks.is_empty());
    drop(held);
}

#[test]
fn locator_worker_exit_disables_admission_without_abandoning_existing_ownership() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let retired = Arc::new(AtomicUsize::new(0));
    let allocation = entries(&pool, 300, &retired);
    drop(WorkerAdmissionGuard((pool).clone()));
    assert!(pool.admit(1, LocatorAllocationClass::Ordinary).is_err());
    assert!(pool.admit(1, LocatorAllocationClass::Maintenance).is_err());
    assert!(LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).is_err());
    drop(allocation);
    assert!(pool.drain_retired_step());
    assert_eq!(retired.load(AtomicOrdering::SeqCst), LOCATOR_RETIRE_QUOTA);
    drain(&pool);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 300);
}

#[test]
fn locator_key_final_release_is_an_independently_queued_physical_item() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let key = ChargedLocatorKey::try_new(&pool, LocatorAllocationClass::Ordinary, b"warm")
        .expect("warm key");
    drop(key);
    drain(&pool);
    let baseline = pool.charged_bytes();
    let key = ChargedLocatorKey::try_new(&pool, LocatorAllocationClass::Ordinary, b"retained-key")
        .expect("key");
    let last = key.clone();
    drop(key);
    assert_eq!(lock(&pool.retirement.state).queued_count, 0);
    drop(last);
    assert_eq!(lock(&pool.retirement.state).queued_count, 1);
    assert!(pool.charged_bytes() > baseline);
    assert!(!pool.drain_retired_step());
    assert_eq!(pool.charged_bytes(), baseline);
}

#[test]
fn locator_provider_charge_survives_all_queued_child_destruction() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let budget = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
    let charge = budget.reserve(1024 * 1024).unwrap().commit().unwrap();
    let owner = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    owner.attach_provider_charge(charge.clone()).unwrap();
    owner.attach_provider_charge(charge.clone()).unwrap();
    let retired = Arc::new(AtomicUsize::new(0));
    let children = entries(&pool, 1000, &retired);
    drop(charge);
    // Match the production owner-last invariant. Descendants enqueued later
    // by these roots must continue to hold the same physical retirement fence.
    drop(children);
    drop(owner);
    assert_eq!(budget.observation().unwrap().owned_bytes, 1024 * 1024);
    assert!(pool.drain_retired_step());
    assert!(retired.load(AtomicOrdering::SeqCst) <= LOCATOR_RETIRE_QUOTA);
    assert!(retired.load(AtomicOrdering::SeqCst) < 1000);
    assert_eq!(budget.observation().unwrap().owned_bytes, 1024 * 1024);
    drain(&pool);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 1000);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(lock(&pool.budget.state).owners, 0);
}

#[test]
fn locator_multiple_provider_barriers_never_wait_on_each_other() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let budget = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
    let first = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    let second = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    first
        .attach_provider_charge(budget.reserve(1024).unwrap().commit().unwrap())
        .unwrap();
    second
        .attach_provider_charge(budget.reserve(2048).unwrap().commit().unwrap())
        .unwrap();
    let retired = Arc::new(AtomicUsize::new(0));
    let children = entries(&pool, 600, &retired);
    drop(children);
    drop(first);
    drop(second);
    assert_eq!(budget.observation().unwrap().owned_bytes, 3072);
    assert!(pool.drain_retired_step());
    assert_eq!(budget.observation().unwrap().owned_bytes, 3072);
    let mut steps = 1;
    while pool.drain_retired_step() {
        steps += 1;
        assert!(steps <= 10, "barriers must not starve the physical queue");
    }
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 600);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(lock(&pool.retirement.state).non_barrier_count, 0);
    assert_eq!(lock(&pool.budget.state).owners, 0);
}

#[test]
fn locator_provider_owner_retires_without_waiting_for_later_independent_work() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let budget = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
    let owner = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    owner
        .attach_provider_charge(budget.reserve(1024).unwrap().commit().unwrap())
        .unwrap();
    let old_retired = Arc::new(AtomicUsize::new(0));
    let old_children = deferred_child(&pool, entries(&pool, 1000, &old_retired));
    let later_retired = Arc::new(AtomicUsize::new(0));
    let later_independent = entries(&pool, 100_000, &later_retired);
    // Generation roots release before their final owner. Later independent
    // releases must not prolong this generation's already finite retirement.
    drop(old_children);
    drop(owner);
    drop(later_independent);
    for _ in 0..20 {
        let before =
            old_retired.load(AtomicOrdering::SeqCst) + later_retired.load(AtomicOrdering::SeqCst);
        assert!(pool.drain_retired_step());
        let after =
            old_retired.load(AtomicOrdering::SeqCst) + later_retired.load(AtomicOrdering::SeqCst);
        assert!(after - before <= LOCATOR_RETIRE_QUOTA);
    }
    assert_eq!(old_retired.load(AtomicOrdering::SeqCst), 1000);
    assert!(later_retired.load(AtomicOrdering::SeqCst) < 100_000);
    assert_eq!(lock(&pool.budget.state).owners, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    drain(&pool);
}

#[test]
fn locator_retirement_progress_survives_continuous_independent_enqueue() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let owner = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    let retired = Arc::new(AtomicUsize::new(0));
    drop(deferred_child(&pool, entries(&pool, 100, &retired)));
    drop(owner);
    let independent_retired = Arc::new(AtomicUsize::new(0));
    for _ in 0..16 {
        // New work arrives before every bounded reaper turn; the queue never
        // becomes globally empty during this schedule.
        drop(entries(&pool, 512, &independent_retired));
        assert!(pool.drain_retired_step());
    }
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 100);
    assert!(lock(&pool.retirement.state).non_barrier_count > 0);
    assert_eq!(lock(&pool.budget.state).owners, 0);
    drain(&pool);
}

#[test]
fn locator_shared_descendant_keeps_its_remaining_generation_charge() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let budget = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
    let old_owner = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    let current_owner = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    old_owner
        .attach_provider_charge(budget.reserve(1024).unwrap().commit().unwrap())
        .unwrap();
    current_owner
        .attach_provider_charge(budget.reserve(2048).unwrap().commit().unwrap())
        .unwrap();
    let retired = Arc::new(AtomicUsize::new(0));
    let shared = entries(&pool, 600, &retired);
    let old_root = deferred_child(&pool, shared.clone());
    let current_root = deferred_child(&pool, shared);
    drop(old_root);
    drop(old_owner);
    drain(&pool);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(lock(&pool.budget.state).owners, 1);
    assert_eq!(budget.observation().unwrap().owned_bytes, 2048);

    drop(current_root);
    drop(current_owner);
    assert!(pool.drain_retired_step());
    assert!(retired.load(AtomicOrdering::SeqCst) < 600);
    assert_eq!(lock(&pool.budget.state).owners, 1);
    assert_eq!(budget.observation().unwrap().owned_bytes, 2048);
    drain(&pool);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 600);
    assert_eq!(lock(&pool.budget.state).owners, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn locator_epoch_is_reusable_before_owner_quota_release_admits_a_replacement() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let ordinary: Vec<_> = (0..LOCATOR_MAX_OWNERS - 1)
        .map(|_| LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap())
        .collect();
    let maintenance = LocatorOwner::try_new(&pool, LocatorAllocationClass::Maintenance).unwrap();
    drop(ordinary);
    drop(maintenance);
    assert_eq!(lock(&pool.budget.state).owners, LOCATOR_MAX_OWNERS);
    assert_eq!(lock(&pool.retirement.state).open_epoch, LOCATOR_MAX_OWNERS);
    let callback_pool = (pool).clone();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let callback_count = Arc::clone(&callbacks);
    OWNER_QUOTA_RELEASE_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            // Exact interleaving: a new constructor observes the first quota
            // release before that old task's Box or descriptor is reused.
            assert_eq!(lock(&callback_pool.retirement.state).oldest_epoch, 1);
            let replacement =
                LocatorOwner::try_new(&callback_pool, LocatorAllocationClass::Ordinary).unwrap();
            drop(replacement);
            let state = lock(&callback_pool.retirement.state);
            assert_eq!(state.open_epoch, 0);
            assert_eq!(state.oldest_epoch, 1);
            callback_count.fetch_add(1, AtomicOrdering::SeqCst);
        }));
    });
    assert!(pool.drain_retired_step());
    assert_eq!(callbacks.load(AtomicOrdering::SeqCst), 1);
    drain(&pool);
    assert_eq!(lock(&pool.budget.state).owners, 0);
    let state = lock(&pool.retirement.state);
    assert_eq!(state.open_epoch, state.oldest_epoch);
    assert!(state.epoch_outstanding.iter().all(|count| *count == 0));
}

#[test]
fn locator_unwinding_task_keeps_descendants_reclaimable_and_restores_context() {
    struct UnwindingParent {
        _child: Option<ChargedLocatorAllocation<Entries>>,
    }
    impl LocatorRetireContents for UnwindingParent {
        fn retire_one(&mut self) -> bool {
            panic!("deterministic internal retirement failure");
        }
    }
    let pool = NativeLocatorPool::isolated_for_tests();
    let budget = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
    let owner = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    owner
        .attach_provider_charge(budget.reserve(1024).unwrap().commit().unwrap())
        .unwrap();
    let retired = Arc::new(AtomicUsize::new(0));
    let parent =
        ChargedLocatorAllocation::try_new(&pool, LocatorAllocationClass::Ordinary, 0, || {
            Ok(UnwindingParent {
                _child: Some(entries(&pool, 600, &retired)),
            })
        })
        .unwrap();
    drop(parent);
    drop(owner);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _admission_guard = WorkerAdmissionGuard((pool).clone());
        pool.drain_retired_step();
    }));
    assert!(outcome.is_err());
    RETIRING.with(|context| assert!(context.get().is_none()));
    assert!(LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).is_err());
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 1024);
    // A poisoned worker remains fail closed, but admitted physical ownership
    // can still be reclaimed by bounded cleanup instead of leaking its epoch.
    drain(&pool);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 600);
    assert_eq!(lock(&pool.budget.state).owners, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn locator_pending_provider_marker_and_generation_charge_share_final_retirement_barrier() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let budget = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
    let owner = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    owner
        .attach_provider_control_charge(budget.reserve(512).unwrap().commit().unwrap())
        .unwrap();
    owner
        .attach_provider_charge(budget.reserve(1024).unwrap().commit().unwrap())
        .unwrap();
    let retired = Arc::new(AtomicUsize::new(0));
    let children = entries(&pool, 600, &retired);
    drop(children);
    drop(owner);
    assert_eq!(budget.observation().unwrap().owned_bytes, 1536);
    assert!(pool.drain_retired_step());
    assert_eq!(budget.observation().unwrap().owned_bytes, 1536);
    assert!(retired.load(AtomicOrdering::SeqCst) <= LOCATOR_RETIRE_QUOTA);
    drain(&pool);
    assert_eq!(retired.load(AtomicOrdering::SeqCst), 600);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn locator_maximum_legal_identity_key_is_charged_once_across_pins_and_retirement() {
    use riffdb_storage_api::{
        IdempotencyIdentity, IdempotencyIdentityKey, IdempotencyKeyDigest,
        MAX_IDEMPOTENCY_IDENTITY_KEY_V1_BYTES,
    };
    use riffdb_types::{
        ActorId, CommandId, ContractLineage, DatabaseId, DigestKeyId, Environment, TenantId,
        TenantScope,
    };
    let identity = IdempotencyIdentity::new(
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x91; 10]).unwrap(),
        Environment::new("e".repeat(riffdb_types::MAX_ENVIRONMENT_BYTES)).unwrap(),
        TenantScope::Tenant(TenantId::new("t".repeat(riffdb_types::MAX_TENANT_ID_BYTES)).unwrap()),
        ActorId::new("a".repeat(riffdb_types::MAX_ACTOR_ID_BYTES)).unwrap(),
        ContractLineage::new("l".repeat(riffdb_types::MAX_CONTRACT_LINEAGE_BYTES)).unwrap(),
        CommandId::new(u32::MAX).unwrap(),
        IdempotencyKeyDigest::from_hmac_bytes(DigestKeyId::new(u32::MAX).unwrap(), [0xa1; 32]),
    );
    let encoded = identity.storage_key().unwrap();
    assert_eq!(
        encoded.as_bytes().len(),
        MAX_IDEMPOTENCY_IDENTITY_KEY_V1_BYTES
    );
    assert_eq!(
        IdempotencyIdentityKey::decode(encoded.as_bytes()).unwrap(),
        identity
    );
    let pool = NativeLocatorPool::isolated_for_tests();
    drop(ChargedLocatorKey::try_new(&pool, LocatorAllocationClass::Ordinary, b"warm").unwrap());
    drain(&pool);
    let baseline = pool.charged_bytes();
    let key =
        ChargedLocatorKey::try_new(&pool, LocatorAllocationClass::Ordinary, encoded.as_bytes())
            .unwrap();
    let admitted = pool.charged_bytes();
    assert!(admitted - baseline >= encoded.as_bytes().len());
    let pins: Vec<_> = (0..32).map(|_| key.clone()).collect();
    assert_eq!(
        pool.charged_bytes(),
        admitted,
        "pinning never copies key capacity"
    );
    let pressure = pool.exhaust_ordinary_for_tests();
    let pressured = pool.charged_bytes();
    assert!(
        ChargedLocatorKey::try_new(&pool, LocatorAllocationClass::Ordinary, encoded.as_bytes())
            .is_err()
    );
    assert_eq!(pool.charged_bytes(), pressured);
    let progress = ChargedLocatorKey::try_new(
        &pool,
        LocatorAllocationClass::Maintenance,
        encoded.as_bytes(),
    )
    .unwrap();
    assert_eq!(progress.as_ref(), encoded.as_bytes());
    drop(progress);
    drain(&pool);
    drop(pressure);
    drop(key);
    assert_eq!(
        pool.charged_bytes(),
        admitted,
        "protected pins retain the original physical key"
    );
    for pin in &pins {
        assert_eq!(pin.as_ref(), encoded.as_bytes());
    }
    drop(pins);
    assert_eq!(
        pool.charged_bytes(),
        admitted,
        "queued key storage remains admitted"
    );
    drain(&pool);
    assert_eq!(pool.charged_bytes(), baseline);
}

// req: TXN-042, STO-032, REC-001, OQ-040
#[test]
fn sharing_family_cut_covers_existing_members_and_later_members_remain_unprovided() {
    use riffdb_types::exact_provider_budget_v1::ProviderBudget;
    let infrastructure = NativeLocatorPool::isolated_for_tests();
    let pool = infrastructure
        .fresh_source_family(LocatorAllocationClass::Ordinary)
        .unwrap();
    let retired = Arc::new(AtomicUsize::new(0));
    let original = entries(&pool, 8, &retired);
    let budget = ProviderBudget::new();
    let mut family = pool.lock_provider_family().unwrap();
    let needed = family.provider_ownership_needed(&budget).unwrap();
    let started = Arc::new(Barrier::new(2));
    let later = {
        let pool = (pool).clone();
        let retired = Arc::clone(&retired);
        let started = Arc::clone(&started);
        std::thread::spawn(move || {
            started.wait();
            entries(&pool, 4, &retired)
        })
    };
    started.wait();
    // The family guard holds ordinal assignment out until this cut commits.
    let mut work = budget.reserve(needed + 128).unwrap();
    let marker = family
        .prepare_provider_ownership(&mut work, &budget)
        .unwrap()
        .commit();
    drop(work);
    let cut = family.observation().cut;
    let covered = budget.observation().unwrap().owned_bytes;
    drop(family);
    let later = later.join().unwrap();
    assert!(later.0.task.as_ref().unwrap().charge().unwrap().ordinal > cut);
    assert_eq!(budget.observation().unwrap().owned_bytes, covered);
    let later_bytes = later.0.task.as_ref().unwrap().charge().unwrap().bytes as u64;
    let mut family = pool.lock_provider_family().unwrap();
    assert_eq!(
        family.provider_ownership_needed(&budget).unwrap(),
        later_bytes
    );
    let mut work = budget.reserve(later_bytes + 128).unwrap();
    family
        .prepare_provider_ownership(&mut work, &budget)
        .unwrap()
        .commit();
    drop(work);
    drop(family);
    assert_eq!(
        budget.observation().unwrap().owned_bytes,
        covered + later_bytes
    );
    drop(original);
    drop(later);
    assert_eq!(
        budget.observation().unwrap().owned_bytes,
        covered + later_bytes
    );
    drain(&infrastructure);
    assert!(budget.observation().unwrap().owned_bytes < covered);
    drop(marker);
    drop(pool);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert!(
        infrastructure.charged_bytes() > 0,
        "global backing does not bind provider ownership"
    );
}

// req: TXN-042, STO-032, REC-001, OQ-040
#[test]
fn sharing_family_private_increment_cancellation_does_not_advance_cut() {
    use riffdb_types::exact_provider_budget_v1::ProviderBudget;
    let infrastructure = NativeLocatorPool::isolated_for_tests();
    let pool = infrastructure
        .fresh_source_family(LocatorAllocationClass::Ordinary)
        .unwrap();
    let retired = Arc::new(AtomicUsize::new(0));
    let first = entries(&pool, 2, &retired);
    let budget = ProviderBudget::new();
    let mut family = pool.lock_provider_family().unwrap();
    let mut work = budget
        .reserve(family.provider_ownership_needed(&budget).unwrap() + 128)
        .unwrap();
    let marker = family
        .prepare_provider_ownership(&mut work, &budget)
        .unwrap()
        .commit();
    drop(work);
    drop(family);
    let second = entries(&pool, 3, &retired);
    let before = budget.observation().unwrap();
    let mut family = pool.lock_provider_family().unwrap();
    let old_cut = family.observation().cut;
    let old_uncovered = family.observation().uncovered;
    let mut work = budget.reserve(old_uncovered + 128).unwrap();
    {
        let _private = family
            .prepare_provider_ownership(&mut work, &budget)
            .unwrap();
    }
    drop(work);
    assert_eq!(family.observation().cut, old_cut);
    assert_eq!(family.observation().uncovered, old_uncovered);
    assert_eq!(budget.observation().unwrap(), before);
    drop(family);
    drop(second);
    drain(&infrastructure);
    let family = pool.lock_provider_family().unwrap();
    assert_eq!(family.observation().uncovered, 0);
    assert_eq!(budget.observation().unwrap(), before);
    drop(family);
    drop(first);
    drain(&infrastructure);
    drop(marker);
    drop(pool);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

// req: TXN-042, STO-032, REC-001, OQ-040
#[test]
fn independent_rebuild_families_keep_foreign_graphs_and_global_scratch_separate() {
    use riffdb_types::exact_provider_budget_v1::ProviderBudget;
    let infrastructure = NativeLocatorPool::isolated_for_tests();
    let first = infrastructure
        .fresh_source_family(LocatorAllocationClass::Ordinary)
        .unwrap();
    let second = first
        .fresh_source_family(LocatorAllocationClass::Ordinary)
        .unwrap();
    assert!(!first.same_provider_family(&second));
    let retired = Arc::new(AtomicUsize::new(0));
    let first_data = entries(&first, 1, &retired);
    let second_data = entries(&second, 1, &retired);
    let first_budget = ProviderBudget::new();
    let second_budget = ProviderBudget::new();
    for (pool, budget) in [(&first, &first_budget), (&second, &second_budget)] {
        let mut family = pool.lock_provider_family().unwrap();
        let mut work = budget
            .reserve(family.provider_ownership_needed(budget).unwrap() + 128)
            .unwrap();
        family
            .prepare_provider_ownership(&mut work, budget)
            .unwrap()
            .commit();
    }
    assert!(
        first
            .lock_provider_family()
            .unwrap()
            .provider_ownership_needed(&second_budget)
            .is_err()
    );
    assert!(
        second
            .lock_provider_family()
            .unwrap()
            .provider_ownership_needed(&first_budget)
            .is_err()
    );
    let before = first
        .lock_provider_family()
        .unwrap()
        .observation()
        .uncovered;
    let scratch = first.admit(4096, LocatorAllocationClass::Ordinary).unwrap();
    assert!(scratch.family.is_none());
    assert_eq!(
        first
            .lock_provider_family()
            .unwrap()
            .observation()
            .uncovered,
        before
    );
    {
        let backing = lock(&infrastructure.retirement.state);
        assert!(backing._directory_charge.family.is_none());
        assert!(
            backing
                .chunks
                .iter()
                .all(|chunk| chunk._charge.family.is_none())
        );
    }
    drop(scratch);
    drop(first_data);
    drop(second_data);
    drain(&infrastructure);
    drop(first);
    drop(second);
    assert_eq!(first_budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(second_budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn locator_single_live_value_fits_admitted_inline_payload_and_retires_once() {
    const INLINE_BYTES: usize = 64 * 1024;
    const ORDINARY_LIMIT: usize = 1024 * 1024;
    const ONE_VALUE_HEADROOM: usize = 96 * 1024;
    struct InlinePayload {
        bytes: [u8; INLINE_BYTES],
        remaining: usize,
        retirement_calls: Arc<AtomicUsize>,
        physical_drops: Arc<AtomicUsize>,
    }
    impl LocatorRetireContents for InlinePayload {
        fn retire_one(&mut self) -> bool {
            self.retirement_calls.fetch_add(1, AtomicOrdering::SeqCst);
            self.remaining -= 1;
            self.remaining == 0
        }
    }
    impl Drop for InlinePayload {
        fn drop(&mut self) {
            self.physical_drops.fetch_add(1, AtomicOrdering::SeqCst);
        }
    }
    let pool = NativeLocatorPool::new(2 * ORDINARY_LIMIT, ORDINARY_LIMIT);
    let warm_retired = Arc::new(AtomicUsize::new(0));
    drop(entries(&pool, 1, &warm_retired));
    drain(&pool);
    let warmed_baseline = pool.charged_bytes();
    let owner = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
    let provider = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
    let provider_bytes = ONE_VALUE_HEADROOM as u64;
    owner
        .attach_provider_charge(provider.reserve(provider_bytes).unwrap().commit().unwrap())
        .unwrap();
    let ordinary_before = lock(&pool.budget.state).ordinary;
    let pressure = pool
        .admit(
            ORDINARY_LIMIT - ordinary_before - ONE_VALUE_HEADROOM,
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
    let pressure_bytes = pressure.bytes();
    let constructor_calls = AtomicUsize::new(0);
    let retirement_calls = Arc::new(AtomicUsize::new(0));
    let physical_drops = Arc::new(AtomicUsize::new(0));
    // A real non-Clone inline value fits the fixed allowance once. Reserving a
    // second inline T in its empty retirement task must not consume the budget.
    let allocation =
        ChargedLocatorAllocation::try_new(&pool, LocatorAllocationClass::Ordinary, 0, || {
            constructor_calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(InlinePayload {
                bytes: [0x5a; INLINE_BYTES],
                remaining: LOCATOR_RETIRE_QUOTA + 1,
                retirement_calls: Arc::clone(&retirement_calls),
                physical_drops: Arc::clone(&physical_drops),
            })
        })
        .expect("one physical inline value must fit without a second T allocation");
    assert_eq!(constructor_calls.load(AtomicOrdering::SeqCst), 1);
    assert!(std::mem::size_of::<AllocationOwner<InlinePayload>>() < INLINE_BYTES);
    assert!(allocation.retained_charge_bound() >= std::mem::size_of::<InlinePayload>());
    assert!(allocation.bytes.iter().all(|byte| *byte == 0x5a));
    let alias = allocation.clone();
    drop(allocation);
    assert!(alias.bytes.iter().all(|byte| *byte == 0x5a));
    assert_eq!(lock(&pool.retirement.state).queued_count, 0);
    assert_eq!(physical_drops.load(AtomicOrdering::SeqCst), 0);
    drop(alias);
    assert_eq!(lock(&pool.retirement.state).queued_count, 1);
    assert_eq!(retirement_calls.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(physical_drops.load(AtomicOrdering::SeqCst), 0);
    drop(owner);
    assert_eq!(provider.observation().unwrap().owned_bytes, provider_bytes);
    assert_eq!(lock(&pool.budget.state).owners, 1);
    assert!(pool.drain_retired_step());
    assert!(retirement_calls.load(AtomicOrdering::SeqCst) <= LOCATOR_RETIRE_QUOTA);
    assert_eq!(physical_drops.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(provider.observation().unwrap().owned_bytes, provider_bytes);
    let mut steps = 1;
    while pool.drain_retired_step() {
        steps += 1;
        assert!(
            steps <= 4,
            "one inline value and its barrier must finish bounded retirement"
        );
    }
    assert_eq!(
        retirement_calls.load(AtomicOrdering::SeqCst),
        LOCATOR_RETIRE_QUOTA + 1
    );
    assert_eq!(physical_drops.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(constructor_calls.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(provider.observation().unwrap().owned_bytes, 0);
    assert_eq!(lock(&pool.budget.state).owners, 0);
    assert_eq!(pool.charged_bytes(), warmed_baseline + pressure_bytes);
    let free_before = lock(&pool.retirement.state).free;
    let cancelled = ChargedLocatorAllocation::<InlinePayload>::try_new(
        &pool,
        LocatorAllocationClass::Ordinary,
        0,
        || {
            constructor_calls.fetch_add(1, AtomicOrdering::SeqCst);
            Err(unavailable())
        },
    );
    assert!(cancelled.is_err());
    assert_eq!(constructor_calls.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(physical_drops.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(lock(&pool.retirement.state).queued_count, 0);
    assert_eq!(lock(&pool.retirement.state).free, free_before);
    assert_eq!(pool.charged_bytes(), warmed_baseline + pressure_bytes);
    drop(pressure);
    assert_eq!(pool.charged_bytes(), warmed_baseline);
}
