// req: STO-020, PRJ-004, OQ-040
use super::*;
use redb::ReadableDatabase;
use riffdb_storage_api::DatabaseInitializationPort;
use riffdb_types::exact_provider_budget_v1::{
    PROJECTION_SOURCE_WORKING_MEMORY_BYTES, ProjectionSourceWorkingBudgetV1,
};

#[test]
fn actual_engine_checkpoint_captures_share_one_source_work_ledger() {
    let directory = crate::test_path::ScopedDirectory::new("source-ledger-captures");
    let mut store = crate::RedbStore::open(directory.join("db.redb")).unwrap();
    store
        .initialize_database(
            riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [17; 10]).unwrap(),
        )
        .unwrap();
    let first =
        crate::checkpoint_root::CheckpointRoot::new(store.shared.database.begin_read().unwrap(), 1)
            .unwrap()
            .retain_engine_and_history(Arc::clone(&store.shared.database), None);
    let second =
        crate::checkpoint_root::CheckpointRoot::new(store.shared.database.begin_read().unwrap(), 2)
            .unwrap()
            .retain_engine_and_history(Arc::clone(&store.shared.database), None);
    let first_budget = capture_budget(first.engine_custody().unwrap());
    let second_budget = capture_budget(second.engine_custody().unwrap());
    assert!(std::ptr::eq(first_budget, second_budget));
    assert!(std::ptr::eq(
        first_budget,
        store.shared.database.source_working_budget()
    ));
    let before = first_budget.observation().unwrap();
    assert_eq!(before.fixed_owner_bytes, 512);
    assert!(
        before.reserved_bytes
            >= std::mem::size_of::<crate::source_working_domain::SourceWorkingDomain>() as u64
    );
    let work = first_budget
        .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1024 * 1024)
        .unwrap();
    let captured = work.settle_owned_value(vec![9_u8; 4096]).unwrap();
    assert!(captured.belongs_to(second_budget));
    assert!(second_budget.observation().unwrap().owned_bytes >= 4096);
    drop(captured);
    assert_eq!(second_budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(
        second_budget.observation().unwrap().reserved_bytes,
        before.reserved_bytes
    );
}

#[test]
fn source_work_engine_ledger_refuses_at_64mib_and_does_not_borrow_native_or_provider_capacity() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let first =
        EngineOwner::try_new_in_pool((), 0, LocatorAllocationClass::Ordinary, &pool).unwrap();
    let other =
        EngineOwner::try_new_in_pool((), 0, LocatorAllocationClass::Ordinary, &pool).unwrap();
    let source = first.source_working_budget().clone();
    let alias = first.source_working_budget().clone();
    let before = source.observation().unwrap();
    let available = PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.reserved_bytes
        - before.owned_bytes;
    let native_bytes = pool.charged_bytes();
    let provider = ProviderBudget::new();
    let provider_before = provider.observation().unwrap();
    let full = source.reserve(available).unwrap();
    assert!(alias.reserve(1).is_err());
    assert!(source.reserve(available + 1).is_err());
    assert!(other.source_working_budget().reserve(1).is_ok());
    assert_eq!(pool.charged_bytes(), native_bytes);
    assert_eq!(provider.observation().unwrap(), provider_before);
    drop(full);
    let native_occupied = pool.exhaust_ordinary_for_tests();
    assert!(
        source.reserve(available).is_ok(),
        "source allowance is not borrowed from the full native locator pool"
    );
    assert_eq!(provider.observation().unwrap(), provider_before);
    drop(native_occupied);
    assert_eq!(
        source.observation().unwrap().reserved_bytes,
        before.reserved_bytes
    );
}

#[test]
fn source_work_allocation_and_lineage_survive_engine_without_retaining_it() {
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let weak_engine = Arc::downgrade(&engine);
    let source: ProjectionSourceWorkingBudgetV1 = engine.source_working_budget().clone();
    let work = source
        .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1024 * 1024)
        .unwrap();
    let identity = riffdb_types::ProjectionIdentity::new(
        riffdb_types::ContractLineage::new("source-working-domain").unwrap(),
        riffdb_types::ProjectionId::first(),
        riffdb_types::ProjectionPlanHash::from_bytes([7; 32]),
    );
    let retained_lineage = identity.clone();
    let owned = work.settle_shared_value(identity).unwrap();
    let captured = owned.clone();
    assert!(source.observation().unwrap().owned_bytes > 0);
    drop(engine);
    assert_eq!(
        weak_engine.strong_count(),
        0,
        "source accounting must not own an engine backreference"
    );
    assert_eq!(
        source.observation().unwrap().reserved_bytes,
        0,
        "inline domain control ended with the engine"
    );
    assert!(captured.belongs_to(&source));
    drop(owned);
    assert!(source.observation().unwrap().owned_bytes > 0);
    drop(captured);
    assert!(
        source.observation().unwrap().owned_bytes > 0,
        "the actual shared lineage still owns its source charge"
    );
    drop(retained_lineage);
    assert_eq!(source.observation().unwrap().owned_bytes, 0);
}

#[test]
fn provider_overlap_follows_source_accountant_after_engine_and_work_wrapper_release() {
    let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
    let provider = ProviderBudget::new();
    {
        let _gate = crate::locator_capability::native_provider_admission_gate().unwrap();
        let needed = engine.provider_ownership_needed(&provider).unwrap();
        let mut plan = provider.reserve(needed + 1).unwrap();
        engine
            .prepare_provider_ownership_from(&mut plan, &provider)
            .unwrap()
            .commit();
    }
    let source = engine.source_working_budget().clone();
    let accountant_bytes = source.provider_control_owned_bytes().unwrap();
    assert_eq!(
        accountant_bytes,
        source.observation().unwrap().fixed_owner_bytes
    );
    let work = source
        .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1024 * 1024)
        .unwrap();
    let identity = riffdb_types::ProjectionIdentity::new(
        riffdb_types::ContractLineage::new("native-source-provider-overlap").unwrap(),
        riffdb_types::ProjectionId::first(),
        riffdb_types::ProjectionPlanHash::from_bytes([3; 32]),
    );
    let retained_lineage = identity.clone();
    let owned = work.settle_shared_value(identity).unwrap();
    drop(engine);
    assert_eq!(
        provider.observation().unwrap().owned_bytes,
        accountant_bytes
    );
    drop(source);
    drop(owned);
    assert_eq!(
        provider.observation().unwrap().owned_bytes,
        accountant_bytes
    );
    drop(retained_lineage);
    assert_eq!(provider.observation().unwrap().owned_bytes, 0);
}

fn capture_budget(
    custody: &crate::checkpoint_root::CheckpointEngineCustody,
) -> &ProjectionSourceWorkingBudgetV1 {
    match custody {
        crate::checkpoint_root::CheckpointEngineCustody::Writable { owner } => {
            owner.source_working_budget()
        }
        crate::checkpoint_root::CheckpointEngineCustody::ReadOnly { owner } => {
            owner.source_working_budget()
        }
    }
}

#[test]
fn competing_actual_engine_captures_share_one_worker_claim_until_guard_release() {
    use std::sync::Barrier;
    let directory = crate::test_path::ScopedDirectory::new("source-worker-capture-claim");
    let mut store = crate::RedbStore::open(directory.join("db.redb")).unwrap();
    store
        .initialize_database(
            riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [18; 10]).unwrap(),
        )
        .unwrap();
    let first = Arc::new(
        crate::checkpoint_root::CheckpointRoot::new(store.shared.database.begin_read().unwrap(), 1)
            .unwrap()
            .retain_engine_and_history(Arc::clone(&store.shared.database), None),
    );
    let second = Arc::new(
        crate::checkpoint_root::CheckpointRoot::new(store.shared.database.begin_read().unwrap(), 2)
            .unwrap()
            .retain_engine_and_history(Arc::clone(&store.shared.database), None),
    );
    let source = store.shared.database.source_working_budget().clone();
    let ready = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for captured in [first, second] {
        let ready = Arc::clone(&ready);
        let release = Arc::clone(&release);
        threads.push(std::thread::spawn(move || {
            let owner = capture_budget(captured.engine_custody().unwrap()).try_acquire_worker();
            let admitted = owner.is_ok();
            ready.wait();
            release.wait();
            drop(owner);
            admitted
        }));
    }
    ready.wait();
    let while_held = source.try_acquire_worker();
    release.wait();
    let admitted = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .filter(|admitted| *admitted)
        .count();
    assert!(while_held.is_err());
    assert_eq!(admitted, 1);
    assert!(source.try_acquire_worker().is_ok());
}
