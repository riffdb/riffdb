//! Budgeted provider input from the same immutable captured source root.
// req: OQ-040
use super::RedbOwnedSnapshot;
use crate::journal::JournalTable;
use crate::store::ProviderSourceBytesV1;
use riffdb_storage_api::{EntityTarget, StorageError};
use riffdb_types::exact_provider_budget_v1::ProviderBudget;

impl RedbOwnedSnapshot {
    /// Admit an immutable encoded provider input before copying storage bytes.
    /// The ordinary durable semantic decoder remains required; this does not
    /// grant writer authority or reopen a newer read view.
    #[doc(hidden)]
    pub fn read_provider_entity_bytes_v1(
        &self,
        target: &EntityTarget,
        budget: &ProviderBudget,
    ) -> Result<Option<ProviderSourceBytesV1>, StorageError> {
        self.access
            .read_provider_value_v1(JournalTable::Entities, target.key().as_bytes(), budget)
    }
}

impl RedbOwnedSnapshot {
    /// Admit encoded partition input at the original bounded scan ceiling.
    /// Decoding, identity checks and page semantics remain required separately.
    #[doc(hidden)]
    pub fn read_provider_entity_partition_bytes_v1(
        &self,
        request: &riffdb_storage_api::AuthoritativeEntityPartitionScanRequest,
        budget: &ProviderBudget,
    ) -> Result<crate::store::ProviderSourcePageBytesV1, StorageError> {
        // Prefix and range metadata are scratch, separate from retained rows.
        let scratch = budget
            .reserve(32 * 1024)
            .map_err(|_| super::storage_error(riffdb_storage_api::StorageErrorKind::Unavailable))?;
        let upper = super::exclusive_prefix_end(request.prefix()).ok_or_else(super::corrupt)?;
        let start = request
            .after()
            .map_or(request.prefix(), riffdb_types::EntityKey::as_bytes);
        let result = self.access.read_provider_range_v1(
            JournalTable::Entities,
            (start, &upper),
            usize::from(request.limit().get()) + 2,
            budget,
        );
        drop(upper);
        drop(scratch);
        result
    }

    /// Admit encoded index input at the existing inspected-entry ceiling.
    /// Normal partition, semantic and canonical-key verification remain required.
    #[doc(hidden)]
    pub fn read_provider_index_scan_bytes_v1(
        &self,
        request: &riffdb_storage_api::AuthoritativeIndexScanRequest,
        budget: &ProviderBudget,
    ) -> Result<crate::store::ProviderSourcePageBytesV1, StorageError> {
        let scratch = budget
            .reserve(32 * 1024)
            .map_err(|_| super::storage_error(riffdb_storage_api::StorageErrorKind::Unavailable))?;
        let prefix = request.target().prefix().as_bytes();
        let upper = super::exclusive_prefix_end(prefix).ok_or_else(super::corrupt)?;
        let start = request
            .after()
            .map_or(prefix, riffdb_types::IndexEntryKey::as_bytes);
        let result = self.access.read_provider_range_v1(
            JournalTable::SecondaryIndexes,
            (start, &upper),
            riffdb_storage_api::MAX_INDEX_SCAN_INSPECTED_ENTRIES + 1,
            budget,
        );
        drop(upper);
        drop(scratch);
        result
    }
}
