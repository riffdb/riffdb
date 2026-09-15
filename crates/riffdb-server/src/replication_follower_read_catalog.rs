//! Reader-only repository bridge to the shared service catalog adapter.
use super::*;
use riffdb_storage_api::{CatalogRepository, QueryModuleRepository, ReactiveModuleRepository};
use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, QueryModuleHash, ReactiveModuleHash,
};
impl CatalogRepository for FollowerReadSnapshots {
    fn read_active_catalog(
        &self,
    ) -> Result<Option<riffdb_storage_api::ActiveCatalogPointerV1>, StorageError> {
        Ok(self
            .latest()?
            .catalog()
            .map(|catalog| catalog.pointer().clone()))
    }
    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        version: ContractVersion,
    ) -> Result<Option<riffdb_storage_api::StoredContractBundleV1>, StorageError> {
        self.latest()?
            .snapshot()
            .read_contract_bundle(lineage, version)
    }
    fn read_contract_migration_edge(
        &self,
        predecessor: ContractBundleHash,
    ) -> Result<Option<riffdb_storage_api::StoredContractMigrationEdgeV1>, StorageError> {
        self.latest()?
            .snapshot()
            .read_contract_migration_edge(predecessor)
    }
}
impl QueryModuleRepository for FollowerReadSnapshots {
    fn read_query_module(
        &self,
        hash: QueryModuleHash,
    ) -> Result<Option<riffdb_storage_api::StoredQueryModuleV1>, StorageError> {
        self.latest()?.snapshot().read_query_module(hash)
    }
    fn read_active_query_module(
        &self,
        lineage: &ContractLineage,
        version: ContractVersion,
        hash: ContractBundleHash,
    ) -> Result<Option<riffdb_storage_api::ActiveQueryModulePointerV1>, StorageError> {
        self.latest()?
            .snapshot()
            .read_active_query_module(lineage, version, hash)
    }
}
impl ReactiveModuleRepository for FollowerReadSnapshots {
    fn read_reactive_module(
        &self,
        hash: ReactiveModuleHash,
    ) -> Result<Option<riffdb_storage_api::StoredReactiveModuleV1>, StorageError> {
        self.latest()?.snapshot().read_reactive_module(hash)
    }
}
impl crate::read_adapters::CatalogReadSource for FollowerReadSnapshots {
    fn cached_active_query_module(
        &self,
        _: &ContractLineage,
        _: ContractVersion,
        _: ContractBundleHash,
    ) -> Option<Option<riffdb_storage_api::ActiveQueryModulePointerV1>> {
        // The shared catalog adapter dispatches a bounded snapshot read on a miss.
        None
    }
}
impl riffdb_storage_api::ProjectionQueryReader for FollowerReadSnapshots {
    fn query_projection(
        &self,
        request: &riffdb_storage_api::ProjectionQueryRequest,
    ) -> Result<riffdb_storage_api::ProjectionQueryResult, StorageError> {
        self.latest()?.snapshot().query_projection(request)
    }
    fn read_projection_status(
        &self,
        identity: &riffdb_types::ProjectionIdentity,
    ) -> Result<riffdb_storage_api::ProjectionStatus, StorageError> {
        self.latest()?.snapshot().read_projection_status(identity)
    }
}
