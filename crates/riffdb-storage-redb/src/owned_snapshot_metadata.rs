//! Bounded semantic metadata reads from one immutable snapshot; no writer ports.
use super::*;
use crate::error::{precommit_storage_error, table_error};
use riffdb_storage_api::{
    CapabilityLookupResult, CapabilityReader, ProjectionApplySnapshot,
    ProjectionApplySnapshotReader, ProjectionApplySnapshotRequest, ProjectionQueryReader,
    ProjectionQueryRequest, ProjectionQueryResult, ProjectionStatus, StoredCapabilityRecordV1,
};
use riffdb_types::{CapabilityId, CapabilityTokenDigest, ProjectionIdentity};
impl riffdb_storage_api::CatalogRepository for RedbOwnedSnapshot {
    fn read_active_catalog(
        &self,
    ) -> Result<Option<riffdb_storage_api::ActiveCatalogPointerV1>, StorageError> {
        let access = &self.access;
        let table = access
            .open_table(crate::layout::CATALOG_ACTIVE)
            .map_err(table_error)?;
        let Some(value) = table
            .get(crate::layout::CATALOG_ACTIVE_KEY.as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let pointer = crate::codec::decode_active_catalog_pointer_v1(value.value())?
            .into_parts()
            .0;
        let bundle = crate::administration::read_contract_bundle_from_table(
            &access
                .open_table(crate::layout::CONTRACT_BUNDLES)
                .map_err(table_error)?,
            pointer.lineage(),
            pointer.contract_version(),
        )?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if !pointer.matches_bundle(&bundle) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(Some(pointer))
    }
    fn read_contract_bundle(
        &self,
        lineage: &riffdb_types::ContractLineage,
        version: riffdb_types::ContractVersion,
    ) -> Result<Option<riffdb_storage_api::StoredContractBundleV1>, StorageError> {
        crate::administration::read_contract_bundle_from_table(
            &self
                .access
                .open_table(crate::layout::CONTRACT_BUNDLES)
                .map_err(table_error)?,
            lineage,
            version,
        )
    }
    fn read_contract_migration_edge(
        &self,
        predecessor: riffdb_types::ContractBundleHash,
    ) -> Result<Option<riffdb_storage_api::StoredContractMigrationEdgeV1>, StorageError> {
        crate::administration::read_contract_migration_edge_at(&self.access, predecessor)
    }
}

impl CapabilityReader for RedbOwnedSnapshot {
    fn read_capability(
        &self,
        capability_id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
        let database = crate::administration::read_database_id_readonly(&self.access)?;
        crate::administration::capability_from_tables(
            &self
                .access
                .open_table(crate::layout::CAPABILITIES)
                .map_err(table_error)?,
            &self
                .access
                .open_table(crate::layout::CAPABILITY_TOKENS)
                .map_err(table_error)?,
            database,
            capability_id,
        )
    }
    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        let database = crate::administration::read_database_id_readonly(&self.access)?;
        crate::administration::resolve_capability_digests_from_tables(
            &self
                .access
                .open_table(crate::layout::CAPABILITIES)
                .map_err(table_error)?,
            &self
                .access
                .open_table(crate::layout::CAPABILITY_TOKENS)
                .map_err(table_error)?,
            database,
            candidates,
        )
    }
    // No process-global generation is published by an immutable historical pin.
    // The trait's None default forces full current-view reauthorization.
}
impl ProjectionApplySnapshotReader for RedbOwnedSnapshot {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        crate::projection_replay::read_projection_replay_snapshot(&self.access, request)
    }
}
impl ProjectionQueryReader for RedbOwnedSnapshot {
    fn query_projection(
        &self,
        request: &ProjectionQueryRequest,
    ) -> Result<ProjectionQueryResult, StorageError> {
        self.require_replayed_projection(request.selector().identity())?;
        crate::derived::query_projection_at(&self.access, request)
    }
    fn read_projection_status(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError> {
        self.require_replayed_projection(identity)?;
        crate::derived::read_projection_status_at(&self.access, identity)
    }
}
impl RedbOwnedSnapshot {
    fn require_replayed_projection(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<(), StorageError> {
        let control = crate::derived::read_projection_control(
            &self
                .access
                .open_table(crate::layout::PROJECTION_FRONTIER)
                .map_err(table_error)?,
            identity,
        )?;
        if let Some(control) = control {
            let markers = self
                .access
                .open_table(crate::layout::PROJECTION_APPLIED)
                .map_err(table_error)?;
            for position in [control.published(), control.candidate()]
                .into_iter()
                .flatten()
            {
                if crate::projection_replay::local_frontier(
                    &markers,
                    identity,
                    position.generation(),
                    position.frontier(),
                )? != position.frontier()
                {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
            }
        }
        Ok(())
    }
}

impl riffdb_storage_api::QueryModuleRepository for RedbOwnedSnapshot {
    fn read_query_module(
        &self,
        module_hash: riffdb_types::QueryModuleHash,
    ) -> Result<Option<riffdb_storage_api::StoredQueryModuleV1>, StorageError> {
        crate::administration::read_database_id_readonly(&self.access)?;
        crate::administration::query_module_from_table(
            &self
                .access
                .open_table(crate::layout::QUERY_MODULES)
                .map_err(table_error)?,
            module_hash,
        )
    }
    fn read_active_query_module(
        &self,
        lineage: &riffdb_types::ContractLineage,
        version: riffdb_types::ContractVersion,
        bundle_hash: riffdb_types::ContractBundleHash,
    ) -> Result<Option<riffdb_storage_api::ActiveQueryModulePointerV1>, StorageError> {
        // Complete stream integrity was proved at startup. The pinned read still
        // checks every exact pointer/audit/module cross-link without a history scan.
        crate::administration::read_active_query_module_at(
            &self.access,
            lineage,
            version,
            bundle_hash,
        )
    }
}
impl riffdb_storage_api::ReactiveModuleRepository for RedbOwnedSnapshot {
    fn read_reactive_module(
        &self,
        module_hash: riffdb_types::ReactiveModuleHash,
    ) -> Result<Option<riffdb_storage_api::StoredReactiveModuleV1>, StorageError> {
        crate::administration::reactive_module_from_table(
            &self
                .access
                .open_table(crate::layout::REACTIVE_MODULES)
                .map_err(table_error)?,
            module_hash,
        )
    }
}

impl riffdb_storage_api::AuthoritativeEntitySnapshotReader for RedbOwnedSnapshot {
    fn read_entity_type_page(
        &self,
        entity: riffdb_types::EntityTypeId,
        after: Option<&[u8]>,
        limit: riffdb_storage_api::StorageScanLimit,
    ) -> Result<riffdb_storage_api::ApplicationExportSourcePageV1, StorageError> {
        let catalog = riffdb_storage_api::CatalogRepository::read_active_catalog(self)?
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        crate::application_export::read_entity_type_page_at(
            &self.access,
            catalog.lineage(),
            entity,
            after,
            limit,
        )
    }
}
impl RedbOwnedSnapshot {
    /// Reads the complete at-most-256 columnar control set from this exact pin.
    /// Structural validity grants no permission to mutate or select an artifact.
    pub fn read_columnar_projection_controls(
        &self,
    ) -> Result<Vec<riffdb_storage_api::StoredColumnarProjectionControlV1>, StorageError> {
        crate::columnar_projection_control::read_controls_at(&self.access)
    }
}
