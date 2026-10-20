// req: TXN-042, STO-032, REC-001, PRJ-004
use super::*;

#[test]
fn engine_maintenance_decode_reuses_actual_pool_with_source_and_ordinary_pressure() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let engine =
        EngineOwner::try_new_in_pool((), 0, LocatorAllocationClass::Maintenance, &pool).unwrap();
    let alias = engine.clone();
    let budget = engine.source_working_budget();
    let source_before = budget.observation().unwrap();
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - source_before.fixed_owner_bytes
        - source_before.owned_bytes
        - source_before.reserved_bytes;
    let source_pressure = budget.reserve(remaining).unwrap();
    let source_full = budget.observation().unwrap();
    let ordinary_pressure = pool.exhaust_ordinary_for_tests();
    let before = pool.charged_bytes();
    let decoded = engine.admit_maintenance_decode(4096).unwrap();
    let second = alias.admit_maintenance_decode(4096).unwrap();
    assert_eq!(pool.charged_bytes(), before + 8192);
    assert_eq!(budget.observation().unwrap(), source_full);
    assert!(pool.admit(1, LocatorAllocationClass::Ordinary).is_err());
    drop(decoded);
    drop(second);
    assert_eq!(pool.charged_bytes(), before);
    assert_eq!(budget.observation().unwrap(), source_full);
    drop(ordinary_pressure);
    drop(source_pressure);
}

#[test]
fn maintenance_decode_charge_outlives_final_engine_without_retaining_engine_or_source_budget() {
    let pool = NativeLocatorPool::isolated_for_tests();
    let baseline = pool.charged_bytes();
    let engine =
        EngineOwner::try_new_in_pool((), 0, LocatorAllocationClass::Maintenance, &pool).unwrap();
    let before_weak = pool.charged_bytes();
    let weak = engine.admit_maintenance_weak_backing(&engine).unwrap();
    let weak_backing = pool.charged_bytes().checked_sub(before_weak).unwrap();
    assert!(weak_backing > 0);
    let source_ledger = engine.source_working_budget().clone();
    let source_before = source_ledger.observation().unwrap();
    let decoded = engine.admit_maintenance_decode(8192).unwrap();
    drop(engine);
    assert!(weak.upgrade().is_none());
    assert_eq!(pool.charged_bytes(), baseline + 8192 + weak_backing);
    drop(weak);
    assert_eq!(pool.charged_bytes(), baseline + 8192);
    let source_after_engine = source_ledger.observation().unwrap();
    assert_eq!(
        source_after_engine.fixed_owner_bytes,
        source_before.fixed_owner_bytes
    );
    assert_eq!(source_after_engine.owned_bytes, source_before.owned_bytes);
    assert_eq!(source_after_engine.reserved_bytes, 0);
    drop(decoded);
    assert_eq!(pool.charged_bytes(), baseline);
    assert_eq!(source_ledger.observation().unwrap(), source_after_engine);
}
