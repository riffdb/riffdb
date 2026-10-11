//! Admitted catalog records from the same immutable captured metadata root.
// req: OQ-040, PRJ-004
use super::{RedbOwnedSnapshot, corrupt, provider_read::provider_decode_error};
use crate::error::{precommit_storage_error, storage_error, table_error};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, StorageError, StorageErrorKind, StoredContractBundleV1,
};
use riffdb_types::exact_provider_budget_v1::{ProviderAllocation, ProviderBudget};
use riffdb_types::{ContractBundleHash, ContractLineage, ContractVersion};
use std::sync::Arc;

// Join cloning owns only these three private bounded buffers. The root allowance
// includes the allocation wrapper, ownership bookkeeping and alignment slack.
const JOIN_PRIVATE_BOUND: usize =
    2 * std::mem::size_of::<riffdb_storage_api::StoredContractMigrationEdgeV1>()
        + 2 * std::mem::align_of::<riffdb_storage_api::StoredContractMigrationEdgeV1>()
        + 2 * (riffdb_types::MAX_BACKUP_NAME_V1_BYTES
            + riffdb_types::MAX_APPROVAL_ID_BYTES
            + riffdb_storage_api::MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES)
        + 4096;
const _: () = assert!(JOIN_PRIVATE_BOUND <= 32 * 1024);

impl RedbOwnedSnapshot {
    /// Join exact permanent migration evidence on this same captured root.
    /// Admission never substitutes absent evidence or bypasses the ordinary join.
    #[doc(hidden)]
    pub fn read_provider_contract_migration_edge_v1(
        &self,
        predecessor: ContractBundleHash,
        budget: &ProviderBudget,
    ) -> Result<
        Option<Arc<ProviderAllocation<riffdb_storage_api::StoredContractMigrationEdgeV1>>>,
        StorageError,
    > {
        self.verify_snapshot_custody()?;
        // The record's copied private strings/checksum are each bounded by 256
        // bytes; the actor is shared. Admit join copies and settlement census
        // before any metadata key or record reconstruction.
        let work = budget
            .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 32 * 1024)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let retirements = self
            .access
            .open_table(crate::layout::CONTRACT_WRITE_RETIREMENTS)
            .map_err(table_error)?;
        let key = crate::keys::encode_contract_write_retirement_key(predecessor);
        let Some(value) = retirements
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let retirement = riffdb_storage_api::decode_provider_contract_write_retirement_admitted_v1(
            value.value(),
            budget,
        )
        .map_err(migration_decode_error)?;
        if retirement.artifacts().parent() != predecessor {
            return Err(corrupt());
        }
        let migrations = self
            .access
            .open_table(crate::layout::CONTRACT_MIGRATIONS)
            .map_err(table_error)?;
        let key = crate::keys::encode_contract_migration_operation_key(retirement.operation_id());
        let value = migrations
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let migration = riffdb_storage_api::decode_provider_contract_migration_record_admitted_v1(
            value.value(),
            budget,
        )
        .map_err(migration_decode_error)?;
        let edge = riffdb_storage_api::StoredContractMigrationEdgeV1::new(
            **retirement,
            (**migration).clone(),
        )
        .map_err(|_| corrupt())?;
        drop(migration);
        drop(retirement);
        work.settle_shared_value(edge)
            .map(Some)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))
    }

    /// Reconstruct the active identity and validate its bundle on this exact pin.
    /// Opaque bundle reconstruction does not replace later ordinary IR validation.
    #[doc(hidden)]
    pub fn read_provider_active_catalog_v1(
        &self,
        budget: &ProviderBudget,
    ) -> Result<Option<Arc<ProviderAllocation<ActiveCatalogPointerV1>>>, StorageError> {
        self.verify_snapshot_custody()?;
        // Closed metadata handles and bounded key scratch, before opening/copying.
        let _work = budget
            .reserve(32 * 1024)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let table = self
            .access
            .open_table(crate::layout::CATALOG_ACTIVE)
            .map_err(table_error)?;
        let Some(value) = table
            .get(crate::layout::CATALOG_ACTIVE_KEY.as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let pointer = riffdb_storage_api::decode_provider_active_catalog_pointer_admitted_v1(
            value.value(),
            budget,
        )
        .map_err(provider_decode_error)?;
        let bundle = self
            .read_provider_contract_bundle_v1(
                pointer.lineage(),
                pointer.contract_version(),
                budget,
            )?
            .ok_or_else(corrupt)?;
        if !pointer.matches_bundle(&bundle) {
            return Err(corrupt());
        }
        Ok(Some(pointer))
    }

    /// Reconstruct one existing immutable bundle under its exact physical identity.
    /// Borrowed storage bytes stay on this pin; generated copies are pre-admitted.
    #[doc(hidden)]
    pub fn read_provider_contract_bundle_v1(
        &self,
        lineage: &ContractLineage,
        version: ContractVersion,
        budget: &ProviderBudget,
    ) -> Result<Option<Arc<ProviderAllocation<StoredContractBundleV1>>>, StorageError> {
        self.verify_snapshot_custody()?;
        let _work = budget
            .reserve(32 * 1024)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let key = crate::keys::encode_contract_bundle_key(lineage, version)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let table = self
            .access
            .open_table(crate::layout::CONTRACT_BUNDLES)
            .map_err(table_error)?;
        let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
            return Ok(None);
        };
        let bundle =
            riffdb_storage_api::decode_provider_catalog_bundle_admitted_v1(value.value(), budget)
                .map_err(provider_decode_error)?;
        if bundle.lineage() != lineage || bundle.contract_version() != version {
            return Err(corrupt());
        }
        Ok(Some(bundle))
    }
}

fn migration_decode_error(error: riffdb_storage_api::ProviderRecordDecodeErrorV1) -> StorageError {
    match error {
        riffdb_storage_api::ProviderRecordDecodeErrorV1::AdmissionUnavailable => {
            storage_error(StorageErrorKind::Unavailable)
        }
        // The ordinary migration-edge reader collapses every codec failure to
        // corrupt evidence; retain that same safe storage-level distinction.
        riffdb_storage_api::ProviderRecordDecodeErrorV1::Codec(_) => corrupt(),
    }
}
