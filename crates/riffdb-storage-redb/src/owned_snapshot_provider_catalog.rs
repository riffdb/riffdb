//! Admitted catalog records from the same immutable captured metadata root.
// req: OQ-040, PRJ-004
use super::{RedbOwnedSnapshot, corrupt, provider_read::provider_decode_error};
use crate::error::{precommit_storage_error, storage_error, table_error};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, StorageError, StorageErrorKind, StoredContractBundleV1,
};
use riffdb_types::exact_provider_budget_v1::{ProviderAllocation, ProviderBudget};
use riffdb_types::{ContractLineage, ContractVersion};
use std::sync::Arc;

impl RedbOwnedSnapshot {
    /// Reconstruct the active identity and validate its bundle on this exact pin.
    /// Opaque bundle reconstruction does not replace later ordinary IR validation.
    #[doc(hidden)]
    pub fn read_provider_active_catalog_v1(
        &self,
        budget: &ProviderBudget,
    ) -> Result<Option<Arc<ProviderAllocation<ActiveCatalogPointerV1>>>, StorageError> {
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
