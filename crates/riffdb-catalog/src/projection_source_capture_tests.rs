// req: PRJ-004, STO-012, STO-020
use super::*;
use crate::ValidatedContractBundle;
use riffdb_storage_api::{CatalogRepository, StorageError, StoredContractBundleV1};
use riffdb_types::exact_provider_budget_v1::ProviderBudget;

struct Repository(StoredContractBundleV1);
impl CatalogRepository for Repository {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(Some(ActiveCatalogPointerV1::from_bundle(&self.0)))
    }
    fn read_contract_bundle(
        &self,
        lineage: &riffdb_types::ContractLineage,
        version: riffdb_types::ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        Ok(
            (lineage == self.0.lineage() && version == self.0.contract_version())
                .then(|| self.0.clone()),
        )
    }
}
fn snapshot() -> (ActiveCatalogSnapshot, ProjectionIdentity) {
    let compiled = riffdb_contract_compiler::compile_contract_source(
        "contract SourcePlan version 1 { event Added { group: string<128> } projection Totals { source event Added key (group) measure n = count() frontier transactionally_ordered } }",
    ).unwrap();
    let bundle = ValidatedContractBundle::from_compiler_bundle(compiled).unwrap();
    let plan = &bundle.bundle().projections()[0];
    let identity = ProjectionIdentity::new(
        bundle.lineage().clone(),
        plan.projection_id(),
        plan.plan_hash(),
    );
    let snapshot = ActiveCatalogSnapshot::read(&Repository(bundle.to_stored().unwrap()))
        .unwrap()
        .unwrap();
    (snapshot, identity)
}

#[test]
fn source_plan_capture_keeps_exact_checked_semantics_and_releases_complete_capacity() {
    let (catalog, identity) = snapshot();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let expected = catalog.resolve_projection(&identity).unwrap();
    let captured = catalog
        .capture_projection_source_admitted_v1(&identity, &budget)
        .unwrap();
    assert_eq!(
        captured.plan().projection_plan(),
        expected.projection_plan()
    );
    assert_eq!(captured.schema(), &expected.checked_group_schema().unwrap());
    assert_eq!(captured.catalog_pointer(), catalog.pointer());
    assert!(captured.belongs_to(&budget.clone()));
    assert!(!captured.belongs_to(&ProjectionSourceWorkingBudgetV1::new()));
    let charged = budget.observation().unwrap();
    assert!(charged.reserved_bytes > before.reserved_bytes);
    // Independent actual constructed-graph census, separately admitted scratch.
    let census = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    // Test-only numerical census: admit its complete scratch before traversal.
    let capacity_test_budget_1 = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
    let mut capacity_test_work_1 = capacity_test_budget_1
        .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let observed = OwnedCapacityInspectionV1::preview_with(
        capacity_test_work_1.inspection_scope().unwrap(),
        |inspection| {
            inspection.visit(captured.plan())?;
            inspection.visit(captured.schema())
        },
    )
    .unwrap();
    let actual = observed
        .shared_allocations()
        .iter()
        .fold(observed.unshared_bytes(), |total, allocation| {
            total + allocation.bytes()
        });
    assert!(actual <= charged.reserved_bytes - before.reserved_bytes);
    drop(observed);
    drop(census);
    drop(captured);
    assert_eq!(budget.observation().unwrap(), before);
}

#[test]
fn source_plan_capture_refuses_pressure_before_cloning_without_gating_ordinary_resolution() {
    let (catalog, identity) = snapshot();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let initial = budget.observation().unwrap();
    let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - initial.fixed_owner_bytes
        - initial.owned_bytes
        - initial.reserved_bytes;
    let occupied = budget.reserve(free).unwrap();
    let blocked = budget.observation().unwrap();
    assert!(matches!(
        catalog.capture_projection_source_admitted_v1(&identity, &budget),
        Err(Error::Unavailable)
    ));
    assert_eq!(
        catalog.resolve_projection(&identity).unwrap().identity(),
        &identity
    );
    assert_eq!(budget.observation().unwrap(), blocked);
    drop(occupied);
    let foreign = ProjectionIdentity::new(
        identity.contract_lineage().clone(),
        identity.projection_id(),
        riffdb_types::ProjectionPlanHash::from_bytes([0x55; 32]),
    );
    assert!(matches!(
        catalog.capture_projection_source_admitted_v1(&foreign, &budget),
        Err(Error::Integrity)
    ));
    assert_eq!(budget.observation().unwrap(), initial);
}

#[test]
fn source_plan_capture_retains_foreign_physical_catalog_owner_without_reassigning_it() {
    let (catalog, identity) = snapshot();
    let provider = ProviderBudget::new();
    let census = provider
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let report = OwnedCapacityInspectionV1::inspect_owned(&catalog, census).unwrap();
    assert_eq!(report.unshared_bytes(), 0);
    let total = report
        .shared_allocations()
        .iter()
        .fold(1024, |total, allocation| total + allocation.bytes());
    let mut work = provider.reserve(total).unwrap();
    for allocation in report.shared_allocations() {
        let candidate = work
            .split(allocation.bytes())
            .unwrap()
            .commit_for_allocation(allocation.identity())
            .unwrap();
        let attached = allocation.lifecycle().unwrap().attach(candidate).unwrap();
        assert!(attached.belongs_to(&provider));
    }
    drop(report);
    drop(work);
    let before = provider.observation().unwrap();
    assert!(before.owned_bytes > 0);
    let source = ProjectionSourceWorkingBudgetV1::new();
    let baseline = source.observation().unwrap();
    let captured = catalog
        .capture_projection_source_admitted_v1(&identity, &source)
        .unwrap();
    assert_eq!(provider.observation().unwrap(), before);
    assert!(source.observation().unwrap().reserved_bytes > baseline.reserved_bytes);
    drop(captured);
    assert_eq!(source.observation().unwrap(), baseline);
    assert_eq!(provider.observation().unwrap(), before);
    drop(catalog);
    assert!(
        provider.observation().unwrap().owned_bytes > 0,
        "the independently retained identity still owns its physical lineage"
    );
    drop(identity);
    assert_eq!(provider.observation().unwrap().owned_bytes, 0);
}
