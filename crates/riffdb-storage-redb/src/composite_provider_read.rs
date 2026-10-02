//! Provider copies inspect the exact captured overlay before allocating.
// req: OQ-040
use super::{RedbCompositeReadView, journal_table};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::store::{ProviderSourceBytesV1, copy_provider_bytes_v1};
use riffdb_storage_api::{
    BorrowedOverlayPointV1, CompositeTableV1, StorageError, StorageErrorKind,
};
use riffdb_types::exact_provider_budget_v1::ProviderBudget;

impl RedbCompositeReadView {
    pub(crate) fn resolve_provider_value_v1(
        &self,
        table: CompositeTableV1,
        key: &[u8],
        budget: &ProviderBudget,
    ) -> Result<Option<ProviderSourceBytesV1>, StorageError> {
        match self.overlay.borrowed_provider_point_v1(table, key) {
            BorrowedOverlayPointV1::Tombstone => Ok(None),
            BorrowedOverlayPointV1::Value(value) => copy_provider_bytes_v1(value, budget).map(Some),
            BorrowedOverlayPointV1::Checkpoint => {
                let table = self
                    .root
                    .journal_byte_table(journal_table(table))
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                    .map_err(table_error)?;
                table
                    .get(key)
                    .map_err(precommit_storage_error)?
                    .map(|value| copy_provider_bytes_v1(value.value(), budget))
                    .transpose()
            }
        }
    }
}

impl RedbCompositeReadView {
    pub(crate) fn resolve_provider_range_v1(
        &self,
        table: CompositeTableV1,
        range: (&[u8], &[u8]),
        max_rows: usize,
        budget: &ProviderBudget,
    ) -> Result<crate::store::ProviderSourcePageBytesV1, StorageError> {
        let base = self
            .root
            .journal_byte_table(journal_table(table))
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .map_err(table_error)?;
        crate::store::copy_provider_range_v1(
            base,
            Some((&self.overlay, table)),
            range,
            max_rows,
            budget,
        )
    }
}
