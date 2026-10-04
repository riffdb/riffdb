//! Provider catalog capture has no cold loader and charges final shared roots.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_storage_api::StoredContractBundleV1;
use std::sync::atomic::{AtomicBool, AtomicUsize};

struct Repository {
    bundle: StoredContractBundleV1,
    present: AtomicBool,
    bundle_reads: AtomicUsize,
}
impl CatalogRepository for Repository {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(self
            .present
            .load(Ordering::SeqCst)
            .then(|| ActiveCatalogPointerV1::from_bundle(&self.bundle)))
    }
    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        self.bundle_reads.fetch_add(1, Ordering::SeqCst);
        Ok(
            (lineage == self.bundle.lineage() && version == self.bundle.contract_version())
                .then(|| self.bundle.clone()),
        )
    }
}
fn repository() -> Repository {
    let bundle =
        ValidatedContractBundle::decode(include_bytes!("../../../fixtures/compiler/bundle.bin"))
            .unwrap();
    Repository {
        bundle: bundle.to_stored().unwrap(),
        present: AtomicBool::new(true),
        bundle_reads: AtomicUsize::new(0),
    }
}
#[test]
fn provider_catalog_probe_refuses_cold_and_stale_publication_without_decoding() {
    let repository = repository();
    let cache = RwLock::new(ActiveCatalogView::default());
    let graph = crate::exact_provider_graph::ProviderOwnershipGraph::isolated_for_test();
    assert!(matches!(
        probe_provider_catalog(&repository, &cache, &graph),
        Err(crate::exact_text_adapter::RebuildFailure::Transient)
    ));
    assert_eq!(repository.bundle_reads.load(Ordering::SeqCst), 0);
    let ordinary = read_active_catalog_cached(&repository, &cache)
        .unwrap()
        .unwrap();
    let reads = repository.bundle_reads.load(Ordering::SeqCst);
    let captured = probe_provider_catalog(&repository, &cache, &graph).unwrap();
    assert!(ordinary.same_publication_as(&captured));
    assert_eq!(repository.bundle_reads.load(Ordering::SeqCst), reads);
    assert!(graph.budget.observation().unwrap().owned_bytes > 8 * 1024 * 1024);
    repository.present.store(false, Ordering::SeqCst);
    assert!(matches!(
        probe_provider_catalog(&repository, &cache, &graph),
        Err(crate::exact_text_adapter::RebuildFailure::Transient)
    ));
    assert_eq!(repository.bundle_reads.load(Ordering::SeqCst), reads);
    repository.present.store(true, Ordering::SeqCst);
    let held = cache.write().unwrap();
    assert!(matches!(
        probe_provider_catalog(&repository, &cache, &graph),
        Err(crate::exact_text_adapter::RebuildFailure::Transient)
    ));
    drop(held);
    drop(captured);
    drop(ordinary);
    cache.write().unwrap().replace(None);
    drop(repository);
    assert_eq!(
        graph.budget.observation().unwrap().owned_bytes,
        8 * 1024 * 1024
    );
    assert_eq!(graph.budget.observation().unwrap().reserved_bytes, 0);
}
#[test]
fn provider_catalog_probe_pressure_preserves_cold_publication_and_final_capture() {
    let repository = repository();
    let cache = RwLock::new(ActiveCatalogView::default());
    let graph = crate::exact_provider_graph::ProviderOwnershipGraph::isolated_for_test();
    let ordinary = read_active_catalog_cached(&repository, &cache)
        .unwrap()
        .unwrap();
    let before = graph.budget.observation().unwrap();
    let pressure = graph
        .budget
        .reserve(
            riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES
                - before.fixed_owner_bytes
                - before.owned_bytes
                - before.reserved_bytes,
        )
        .unwrap();
    let full = graph.budget.observation().unwrap();
    assert!(matches!(
        probe_provider_catalog(&repository, &cache, &graph),
        Err(crate::exact_text_adapter::RebuildFailure::Transient)
    ));
    assert_eq!(graph.budget.observation().unwrap(), full);
    drop(pressure);
    let captured = probe_provider_catalog(&repository, &cache, &graph).unwrap();
    let escaped = captured.bundle().clone();
    let charged = graph.budget.observation().unwrap().owned_bytes;
    let alias = probe_provider_catalog(&repository, &cache, &graph).unwrap();
    assert_eq!(graph.budget.observation().unwrap().owned_bytes, charged);
    drop(alias);
    drop(captured);
    drop(ordinary);
    cache.write().unwrap().replace(None);
    assert!(graph.budget.observation().unwrap().owned_bytes > 8 * 1024 * 1024);
    drop(escaped);
    drop(repository);
    assert_eq!(
        graph.budget.observation().unwrap().owned_bytes,
        8 * 1024 * 1024
    );
    assert_eq!(graph.budget.observation().unwrap().reserved_bytes, 0);
}
