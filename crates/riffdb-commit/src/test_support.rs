use riffdb_catalog::{
    CatalogError, ResolvedExecutablePlan, ValidatedContractBundle, resolve_executable_plan,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, CatalogRepository, ExecutablePlanRef, StorageError,
    StoredContractBundleV1,
};
use riffdb_types::{ContractLineage, ContractVersion};

struct SingleBundleCatalog {
    active: ActiveCatalogPointerV1,
    bundle: StoredContractBundleV1,
}

impl CatalogRepository for SingleBundleCatalog {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(Some(self.active.clone()))
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        Ok(
            (self.bundle.lineage() == lineage
                && self.bundle.contract_version() == contract_version)
                .then(|| self.bundle.clone()),
        )
    }
}

pub(crate) fn resolve_genesis_plan(
    bundle: &ValidatedContractBundle,
    reference: &ExecutablePlanRef,
) -> Result<ResolvedExecutablePlan, CatalogError> {
    let stored = bundle.to_stored()?;
    let repository = SingleBundleCatalog {
        active: ActiveCatalogPointerV1::from_bundle(&stored),
        bundle: stored,
    };
    resolve_executable_plan(&repository, reference)
}
