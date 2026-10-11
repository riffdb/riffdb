//! Admitted index pages retain the ordinary target, epoch and continuation rules.
// req: OQ-040
use super::RedbOwnedSnapshot;
use riffdb_storage_api::{
    AdmittedProviderIndexPageV1, AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest,
    EncodedContentCharge, EncodedPageItem, IndexEpochPosition, IndexRangeEntry,
    MAX_INDEX_SCAN_INSPECTED_ENTRIES, MAX_SCAN_PAGE_BYTES, PartitionIndexTarget, StorageError,
    StorageErrorKind, decode_provider_index_epoch_admitted_v1,
    decode_provider_index_record_admitted_v1,
};
use riffdb_types::exact_provider_budget_v1::ProviderBudget;
use riffdb_types::{MAX_KEY_BYTES, inspect_canonical_decode_capacity_v1};

impl RedbOwnedSnapshot {
    /// Admit encoded inputs, private decode scratch and deep covered-value page
    /// copies before allocation on this same immutable source root. The ordinary
    /// page validator and all existing inspection/row/byte ceilings remain.
    #[doc(hidden)]
    pub fn read_provider_index_page_v1(
        &self,
        request: &AuthoritativeIndexScanRequest,
        budget: &ProviderBudget,
    ) -> Result<AdmittedProviderIndexPageV1, StorageError> {
        self.verify_snapshot_custody()?;
        let wanted = usize::from(request.limit().get());
        let per_row =
            2 * MAX_KEY_BYTES + 4096 + 4 * std::mem::size_of::<EncodedPageItem<IndexRangeEntry>>();
        let metadata = wanted
            .checked_mul(per_row)
            .and_then(|bytes| bytes.checked_add(32 * 1024))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(super::corrupt)?;
        let mut work = budget.reserve(metadata).map_err(|_| unavailable())?;
        let epoch = self
            .read_provider_index_epoch_position_v1(request.target().generation_target(), budget)?;
        let bytes = self.read_provider_index_scan_bytes_v1(request, budget)?;
        // Data is declared after work, so a partially constructed batch is
        // destroyed before its aggregate reservation on every failure path.
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(wanted)
            .map_err(|_| unavailable())?;
        let mut encoded_bytes = 0_usize;
        let mut has_more = false;
        for (candidate_index, row) in bytes.iter().enumerate() {
            let (physical_key, encoded) = &***row;
            if request
                .after()
                .is_some_and(|after| physical_key.as_ref() == after.as_bytes())
            {
                continue;
            }
            if candidate_index == MAX_INDEX_SCAN_INSPECTED_ENTRIES {
                has_more = true;
                break;
            }
            // The fixed metadata allowance covers one temporary key, including
            // candidates rejected by partition filtering.
            let key = super::decode_index_entry_key(physical_key).map_err(|_| super::corrupt())?;
            let source = decode_provider_index_record_admitted_v1(encoded, budget)
                .map_err(super::provider_read::provider_decode_error)?;
            if source.key() != &key {
                return Err(super::corrupt());
            }
            if source.partition_key() != request.target().generation_target().partition_key() {
                continue;
            }
            if entries.len() == wanted {
                has_more = true;
                break;
            }
            let charge = EncodedContentCharge::new(encoded.len()).ok_or_else(super::corrupt)?;
            let next_bytes = encoded_bytes
                .checked_add(charge.get())
                .ok_or_else(super::corrupt)?;
            if next_bytes > MAX_SCAN_PAGE_BYTES {
                if entries.is_empty() {
                    return Err(super::storage_error(StorageErrorKind::LimitExceeded));
                }
                has_more = true;
                break;
            }
            let heap = inspect_canonical_decode_capacity_v1(source.covered_values_encoded())
                .map_err(|_| super::corrupt())?
                .owned_heap_bytes();
            let row_bytes = heap
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(2 * encoded.len() as u64))
                .and_then(|bytes| bytes.checked_add(16 * 1024))
                .ok_or_else(super::corrupt)?;
            let row_work = budget.reserve(row_bytes).map_err(|_| unavailable())?;
            // Transfer BEFORE constructing the deep page copy. Refused transfer
            // cannot leave copied data after its row reservation is released.
            work.absorb(row_work).map_err(|_| unavailable())?;
            let entry = IndexRangeEntry::new(key.index_id(), key, source.covered_values().clone())
                .map_err(super::value_error)?;
            entries.push(EncodedPageItem::new(entry, charge));
            encoded_bytes = next_bytes;
        }
        drop(bytes);
        let page = if has_more {
            let next_after = entries
                .last()
                .ok_or_else(super::corrupt)?
                .value()
                .key()
                .clone();
            AuthoritativeIndexScanPage::page(request, epoch, entries, next_after)
        } else {
            AuthoritativeIndexScanPage::exact_end(request, epoch, entries)
        }
        .map_err(super::value_error)?;
        work.commit_value(page).map_err(|_| unavailable())
    }

    fn read_provider_index_epoch_position_v1(
        &self,
        target: &PartitionIndexTarget,
        budget: &ProviderBudget,
    ) -> Result<IndexEpochPosition, StorageError> {
        self.verify_snapshot_custody()?;
        let _scratch = budget.reserve(32 * 1024).map_err(|_| unavailable())?;
        let key = super::encode_partition_index_key(target);
        let Some(bytes) = self.access.read_provider_value_v1(
            crate::journal::JournalTable::IndexEpochs,
            &key,
            budget,
        )?
        else {
            return Ok(IndexEpochPosition::BeforeFirst);
        };
        let epoch = decode_provider_index_epoch_admitted_v1(&bytes, budget)
            .map_err(super::provider_read::provider_decode_error)?;
        if epoch.target() != target {
            return Err(super::corrupt());
        }
        Ok(IndexEpochPosition::Value(epoch.epoch()))
    }
}

fn unavailable() -> StorageError {
    super::storage_error(StorageErrorKind::Unavailable)
}
