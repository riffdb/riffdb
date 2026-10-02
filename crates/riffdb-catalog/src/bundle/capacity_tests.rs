//! Final references, rather than catalog discovery, own shared charges.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_storage_api::StorageError;
use riffdb_types::exact_provider_budget_v1::{BudgetUnavailable, ProviderBudget};
use riffdb_types::{OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionV1};

struct Repository(StoredContractBundleV1);
impl CatalogRepository for Repository {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(Some(ActiveCatalogPointerV1::from_bundle(&self.0)))
    }
    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        Ok(
            (lineage == self.0.lineage() && version == self.0.contract_version())
                .then(|| self.0.clone()),
        )
    }
}
fn snapshot() -> ActiveCatalogSnapshot {
    let bundle =
        ValidatedContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin"))
            .unwrap();
    ActiveCatalogSnapshot::read(&Repository(bundle.to_stored().unwrap()))
        .unwrap()
        .unwrap()
}
fn attach(
    budget: &ProviderBudget,
    snapshot: &ActiveCatalogSnapshot,
) -> Result<(), BudgetUnavailable> {
    attach_scheduled(budget, snapshot, None)
}
fn attach_scheduled(
    budget: &ProviderBudget,
    snapshot: &ActiveCatalogSnapshot,
    before_attach: Option<&std::sync::Barrier>,
) -> Result<(), BudgetUnavailable> {
    let _scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
    let report = OwnedCapacityInspectionV1::inspect(snapshot).expect("complete catalog inventory");
    assert_eq!(report.unshared_bytes(), 0);
    // Split keeps a nonzero parent allowance through all candidate attachments.
    let mut total = 1024_u64;
    for allocation in report.shared_allocations() {
        total = total
            .checked_add(allocation.bytes())
            .ok_or(BudgetUnavailable)?;
        assert!(
            allocation.lifecycle().is_some(),
            "unowned shared catalog allocation: {} bytes",
            allocation.bytes()
        );
    }
    let mut admitted = budget.reserve(total)?;
    for allocation in report.shared_allocations() {
        let candidate = admitted
            .split(allocation.bytes())?
            .commit_for_allocation(allocation.identity())?;
        if let Some(barrier) = before_attach {
            barrier.wait();
        }
        let owner = allocation
            .lifecycle()
            .ok_or(BudgetUnavailable)?
            .attach(candidate)
            .expect("live catalog destruction owner");
        assert!(owner.belongs_to(budget), "same catalog budget");
        assert_eq!(owner.allocation(), Some(allocation.identity()));
        assert!(
            owner.bytes() >= allocation.bytes(),
            "complete catalog allocation charge"
        );
    }
    drop(report);
    Ok(())
}
#[test]
fn catalog_shared_roots_deduplicate_and_charge_escaped_plan_and_names_until_final_drop() {
    let budget = ProviderBudget::new();
    let snapshot = snapshot();
    let bytes = snapshot.bundle().bundle().canonical_bytes().to_vec();
    let command = &snapshot.bundle().bundle().commands()[0];
    let resolved = snapshot
        .resolve_active_command(command.command_id(), command.plan_hash())
        .unwrap();
    let plan = Arc::clone(&resolved.plan);
    let names = Arc::clone(snapshot.bundle().enum_variant_names());
    attach(&budget, &snapshot).unwrap();
    let before = budget.observation().unwrap().owned_bytes;
    let capture = snapshot.clone();
    attach(&budget, &capture).unwrap();
    assert_eq!(budget.observation().unwrap().owned_bytes, before);
    assert_eq!(snapshot.bundle().bundle().canonical_bytes(), bytes);
    drop(snapshot);
    assert_eq!(budget.observation().unwrap().owned_bytes, before);
    drop(capture);
    assert!(budget.observation().unwrap().owned_bytes > 0);
    drop(resolved);
    let _scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let escaped = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        inspection.visit(&plan)?;
        inspection.visit(&names)
    })
    .unwrap();
    let expected = escaped
        .shared_allocations()
        .iter()
        .map(|allocation| allocation.bytes())
        .sum::<u64>();
    drop(escaped);
    assert_eq!(budget.observation().unwrap().owned_bytes, expected);
    drop(plan);
    let report = OwnedCapacityInspectionV1::inspect(&names).unwrap();
    let names_bytes = report
        .shared_allocations()
        .iter()
        .map(|allocation| allocation.bytes())
        .sum::<u64>();
    drop(report);
    assert_eq!(budget.observation().unwrap().owned_bytes, names_bytes);
    drop(names);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}
#[test]
fn concurrent_catalog_captures_attach_one_charge_per_actual_allocation() {
    let budget = ProviderBudget::new();
    let snapshot = snapshot();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let barrier = barrier.clone();
            let budget = budget.clone();
            let capture = snapshot.clone();
            scope.spawn(move || {
                attach_scheduled(&budget, &capture, Some(&barrier)).unwrap();
            });
        }
    });
    let _scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let report = OwnedCapacityInspectionV1::inspect(&snapshot).unwrap();
    let expected = report
        .shared_allocations()
        .iter()
        .map(|allocation| allocation.bytes())
        .sum::<u64>();
    drop(report);
    assert_eq!(budget.observation().unwrap().owned_bytes, expected);
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
    );
    drop(snapshot);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

#[test]
fn catalog_shared_attachment_pressure_refuses_before_attaching_any_shared_root() {
    use riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES;
    let budget = ProviderBudget::new();
    let snapshot = snapshot();
    let blocker = budget
        .reserve(EXACT_PROVIDER_BUDGET_BYTES - OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 - 128)
        .unwrap();
    assert!(attach(&budget, &snapshot).is_err());
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    let scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let report = OwnedCapacityInspectionV1::inspect(&snapshot).unwrap();
    for allocation in report.shared_allocations() {
        assert!(
            allocation
                .lifecycle()
                .unwrap()
                .attached_owner()
                .unwrap()
                .is_none()
        );
    }
    drop(report);
    drop(scratch);
    drop(blocker);
    attach(&budget, &snapshot).unwrap();
    drop(snapshot);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
