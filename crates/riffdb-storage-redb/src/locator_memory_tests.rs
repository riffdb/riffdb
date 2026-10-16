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
    fn take_for_retirement(&mut self) -> Self {
        Self {
            values: std::mem::take(&mut self.values),
            retired: Arc::clone(&self.retired),
        }
    }
}
fn entries(
    pool: &Arc<NativeLocatorPool>,
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
            let pool = Arc::clone(&pool);
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
    let poison_pool = Arc::clone(&pool);
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
    drop(WorkerAdmissionGuard(Arc::clone(&pool)));
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
    owner.attach_provider_charge(Arc::clone(&charge)).unwrap();
    owner.attach_provider_charge(Arc::clone(&charge)).unwrap();
    let retired = Arc::new(AtomicUsize::new(0));
    let children = entries(&pool, 1000, &retired);
    drop(charge);
    // Deliberately put the barrier before its children. FIFO and nonbarrier
    // accounting must still make progress without premature numerical release.
    drop(owner);
    drop(children);
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
    drop(first);
    drop(children);
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
    drop(owner);
    drop(children);
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
