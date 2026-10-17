use super::*;

// req: STO-020, PRJ-004
#[test]
fn source_work_budget_exact_limit_one_over_and_cancellation_release() {
    let source = ProjectionSourceWorkingBudgetV1::new();
    let initial = source.observation().unwrap();
    assert_eq!(initial.fixed_owner_bytes, 512);
    let available = PROJECTION_SOURCE_WORKING_MEMORY_BYTES - initial.fixed_owner_bytes;
    assert!(source.reserve(available + 1).is_err());
    let work = source.reserve(available).unwrap();
    assert_eq!(work.bytes(), available);
    assert!(source.reserve(1).is_err());
    assert_eq!(source.observation().unwrap().reserved_bytes, available);
    drop(work);
    assert_eq!(source.observation().unwrap().reserved_bytes, 0);
    assert!(source.reserve(available).is_ok());
}

// req: STO-020, PRJ-004
#[test]
fn source_work_budget_is_independent_of_provider_and_foreign_worker_ledgers() {
    let source = ProjectionSourceWorkingBudgetV1::new();
    let foreign = ProjectionSourceWorkingBudgetV1::new();
    let providers = ProviderBudget::new();
    let provider_before = providers.observation().unwrap();
    let mut parent = source.reserve(100).unwrap();
    let child = parent.split(40).unwrap();
    assert_eq!(parent.bytes(), 60);
    assert_eq!(child.bytes(), 40);
    parent.absorb(child).unwrap();
    assert_eq!(parent.bytes(), 100);
    assert!(parent.absorb(foreign.reserve(10).unwrap()).is_err());
    assert_eq!(source.observation().unwrap().reserved_bytes, 100);
    assert_eq!(foreign.observation().unwrap().reserved_bytes, 0);
    assert_eq!(providers.observation().unwrap(), provider_before);
}

// req: STO-020, PRJ-004
#[test]
fn source_work_settled_data_retains_charge_through_final_shared_reference() {
    let source = ProjectionSourceWorkingBudgetV1::new();
    let work = source
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1024 * 1024)
        .unwrap();
    // The full construction and census bound was reserved before this allocation.
    let value = vec![7_u8; 4096];
    let owned = work.settle_owned_value(value).unwrap();
    assert!(owned.belongs_to(&source));
    let captured = owned.clone();
    let bytes = source.observation().unwrap().owned_bytes;
    assert!(bytes >= 4096);
    assert_eq!(source.observation().unwrap().reserved_bytes, 0);
    drop(owned);
    assert_eq!(source.observation().unwrap().owned_bytes, bytes);
    assert_eq!(captured.len(), 4096);
    drop(captured);
    assert_eq!(source.observation().unwrap().owned_bytes, 0);
}

// req: STO-020, PRJ-004
#[test]
fn source_work_concurrent_reservations_share_one_compiled_aggregate_limit() {
    use std::sync::{Arc, Barrier};
    let source = ProjectionSourceWorkingBudgetV1::new();
    let ready = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let source = source.clone();
        let ready = Arc::clone(&ready);
        let release = Arc::clone(&release);
        threads.push(std::thread::spawn(move || {
            let reservation = source.reserve(PROJECTION_SOURCE_WORKING_MEMORY_BYTES - 512);
            let admitted = reservation.is_ok();
            ready.wait();
            release.wait();
            drop(reservation);
            admitted
        }));
    }
    ready.wait();
    // Release the scheduled threads before assertions so failure cannot strand them.
    let observed = source.observation();
    release.wait();
    let admitted = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .filter(|admitted| *admitted)
        .count();
    assert_eq!(admitted, 1);
    assert_eq!(
        observed.unwrap().reserved_bytes,
        PROJECTION_SOURCE_WORKING_MEMORY_BYTES - 512
    );
    assert_eq!(source.observation().unwrap().reserved_bytes, 0);
}

// req: STO-020, PRJ-004
#[test]
fn source_work_shared_identity_charge_follows_actual_lineage_and_refuses_foreign_owner() {
    let source = ProjectionSourceWorkingBudgetV1::new();
    let work = source
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1024 * 1024)
        .unwrap();
    let identity = crate::ProjectionIdentity::new(
        crate::ContractLineage::new("source-owned-lineage").unwrap(),
        crate::ProjectionId::first(),
        crate::ProjectionPlanHash::from_bytes([1; 32]),
    );
    let captured = identity.clone();
    let owned = work.settle_shared_value(identity).unwrap();
    assert!(owned.belongs_to(&source));
    let foreign = ProjectionSourceWorkingBudgetV1::new();
    let other_work = foreign
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1024 * 1024)
        .unwrap();
    assert!(other_work.settle_shared_value(captured.clone()).is_err());
    assert_eq!(foreign.observation().unwrap().owned_bytes, 0);
    drop(owned);
    assert!(
        source.observation().unwrap().owned_bytes > 0,
        "the actual shared lineage is still alive"
    );
    drop(captured);
    assert_eq!(source.observation().unwrap().owned_bytes, 0);
}

// req: OQ-040, STO-020, PRJ-004
#[test]
fn source_accountant_provider_control_is_once_only_rollback_safe_and_refuses_self_binding() {
    let source = ProjectionSourceWorkingBudgetV1::new();
    let provider = ProviderBudget::new();
    assert_eq!(
        source.provider_control_ownership_needed(&provider).unwrap(),
        SOURCE_ACCOUNTANT_BYTES
    );
    assert!(source.provider_control_ownership_needed(&source.0).is_err());
    let foreign_source = ProjectionSourceWorkingBudgetV1::new();
    assert!(
        source
            .provider_control_ownership_needed(&foreign_source.0)
            .is_err()
    );
    let mut foreign_work = foreign_source.0.reserve(2048).unwrap();
    assert!(
        source
            .prepare_provider_control_ownership_from(&mut foreign_work, &foreign_source.0)
            .is_err()
    );
    drop(foreign_work);
    let mut self_work = source.0.reserve(1024).unwrap();
    assert!(
        source
            .prepare_provider_control_ownership_from(&mut self_work, &source.0)
            .is_err()
    );
    drop(self_work);
    let mut work = provider.reserve(2048).unwrap();
    {
        let prepared = source
            .prepare_provider_control_ownership_from(&mut work, &provider)
            .unwrap();
        assert_eq!(
            provider.observation().unwrap().owned_bytes,
            SOURCE_ACCOUNTANT_BYTES
        );
        drop(prepared);
    }
    assert_eq!(provider.observation().unwrap().owned_bytes, 0);
    source
        .prepare_provider_control_ownership_from(&mut work, &provider)
        .unwrap()
        .commit();
    assert_eq!(
        source.provider_control_ownership_needed(&provider).unwrap(),
        0
    );
    source
        .prepare_provider_control_ownership_from(&mut work, &provider)
        .unwrap()
        .commit();
    assert_eq!(
        provider.observation().unwrap().owned_bytes,
        SOURCE_ACCOUNTANT_BYTES
    );
    assert!(
        source
            .provider_control_ownership_needed(&ProviderBudget::new())
            .is_err()
    );
    drop(work);
    drop(source);
    assert_eq!(provider.observation().unwrap().owned_bytes, 0);
}

// req: OQ-040, STO-020, PRJ-004
#[test]
fn source_accountant_provider_control_outlives_all_budget_handles_until_actual_lineage_release() {
    let source = ProjectionSourceWorkingBudgetV1::new();
    let provider = ProviderBudget::new();
    let mut provider_work = provider.reserve(2048).unwrap();
    source
        .prepare_provider_control_ownership_from(&mut provider_work, &provider)
        .unwrap()
        .commit();
    drop(provider_work);
    let work = source
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 1024 * 1024)
        .unwrap();
    let identity = crate::ProjectionIdentity::new(
        crate::ContractLineage::new("source-accountant-provider-lifetime").unwrap(),
        crate::ProjectionId::first(),
        crate::ProjectionPlanHash::from_bytes([2; 32]),
    );
    let retained = identity.clone();
    let owned = work.settle_shared_value(identity).unwrap();
    drop(source);
    assert_eq!(
        provider.observation().unwrap().owned_bytes,
        SOURCE_ACCOUNTANT_BYTES
    );
    drop(owned);
    assert_eq!(
        provider.observation().unwrap().owned_bytes,
        SOURCE_ACCOUNTANT_BYTES
    );
    drop(retained);
    assert_eq!(provider.observation().unwrap().owned_bytes, 0);
}

// req: STO-020, PRJ-004, END-008
#[test]
fn one_source_worker_claim_is_shared_and_released_without_borrowing_data_allowance() {
    let source = ProjectionSourceWorkingBudgetV1::new();
    let before = source.observation().unwrap();
    let all = source
        .reserve(PROJECTION_SOURCE_WORKING_MEMORY_BYTES - before.fixed_owner_bytes)
        .unwrap();
    let worker = source.try_acquire_worker().unwrap();
    assert!(source.clone().try_acquire_worker().is_err());
    assert!(
        ProjectionSourceWorkingBudgetV1::new()
            .try_acquire_worker()
            .is_ok()
    );
    drop(worker);
    assert!(source.try_acquire_worker().is_ok());
    drop(all);
    assert_eq!(source.observation().unwrap().reserved_bytes, 0);
}

// req: STO-020, PRJ-004, END-008
#[test]
fn competing_source_worker_claims_admit_exactly_one_until_physical_owner_releases() {
    use std::sync::{Arc, Barrier};
    let source = ProjectionSourceWorkingBudgetV1::new();
    let ready = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let source = source.clone();
        let ready = Arc::clone(&ready);
        let release = Arc::clone(&release);
        threads.push(std::thread::spawn(move || {
            let owner = source.try_acquire_worker();
            let admitted = owner.is_ok();
            ready.wait();
            release.wait();
            drop(owner);
            admitted
        }));
    }
    ready.wait();
    release.wait();
    let admitted = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .filter(|admitted| *admitted)
        .count();
    assert_eq!(admitted, 1);
    assert!(source.try_acquire_worker().is_ok());
}

// req: STO-020, PRJ-004, END-008
#[test]
fn source_worker_claim_releases_on_failed_construction_cancel_and_caught_panic() {
    fn fails_to_construct(
        source: &ProjectionSourceWorkingBudgetV1,
    ) -> Result<ProjectionSourceWorkerOwnershipV1, BudgetUnavailable> {
        let owner = source.try_acquire_worker()?;
        // Its fixed accountant makes this full-limit request fail before allocation.
        let _work = source.reserve(PROJECTION_SOURCE_WORKING_MEMORY_BYTES)?;
        Ok(owner)
    }
    let source = ProjectionSourceWorkingBudgetV1::new();
    assert!(fails_to_construct(&source).is_err());
    let cancelled = source.try_acquire_worker().unwrap();
    assert!(source.try_acquire_worker().is_err());
    drop(cancelled);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _owner = source.try_acquire_worker().unwrap();
        panic!("fixture worker construction panic");
    }));
    assert!(panic.is_err());
    assert!(source.try_acquire_worker().is_ok());
    assert_eq!(source.observation().unwrap().reserved_bytes, 0);
}

// req: STO-020, PRJ-004, END-008
#[test]
fn poisoned_source_worker_cleanup_releases_claim_and_permanently_refuses_new_admission() {
    let source = ProjectionSourceWorkingBudgetV1::new();
    let owner = source.try_acquire_worker().unwrap();
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _accounting = source.0.0.lock().unwrap();
        panic!("fixture poison while accounting is locked");
    }));
    assert!(poisoned.is_err());
    drop(owner);
    assert!(source.try_acquire_worker().is_err());
    assert!(source.reserve(1).is_err());
    assert!(source.observation().is_err());
    let accounting = source.0.0.lock().err().unwrap().into_inner();
    assert!(!accounting.source_worker_claimed);
    assert!(accounting.refused);
}
