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

impl RedbOwnedSnapshot {
    /// Reconstruct one admitted provider entity from this same captured root.
    /// Normal target identity and durable semantic validation remain mandatory.
    #[doc(hidden)]
    pub fn read_provider_entity_record_v1(
        &self,
        target: &EntityTarget,
        budget: &ProviderBudget,
    ) -> Result<Option<riffdb_storage_api::AdmittedProviderEntityRecordV1>, StorageError> {
        let Some(bytes) = self.read_provider_entity_bytes_v1(target, budget)? else {
            return Ok(None);
        };
        let record = riffdb_storage_api::decode_provider_entity_record_admitted_v1(&bytes, budget)
            .map_err(provider_decode_error)?;
        if record.target() != target {
            return Err(super::corrupt());
        }
        Ok(Some(record))
    }
}

impl RedbOwnedSnapshot {
    /// Reconstruct and validate the existing bounded entity page on this root.
    /// Page metadata and semantic copies are admitted before construction. The
    /// caller must have validated this captured root's frontier through the
    /// ordinary source capture; an uninitialized frontier cache is refused.
    #[doc(hidden)]
    pub fn read_provider_entity_partition_records_v1(
        &self,
        request: &riffdb_storage_api::AuthoritativeEntityPartitionScanRequest,
        budget: &ProviderBudget,
    ) -> Result<riffdb_storage_api::AdmittedProviderEntityPartitionPageV1, StorageError> {
        use riffdb_storage_api::{
            AuthoritativeEntityPartitionScanPage, EncodedContentCharge, EncodedPageItem,
            StoredEntityRecordV1,
        };
        let wanted = usize::from(request.limit().get());
        // Each copied row has a bounded entity key and lineage plus inline
        // record/item storage. The extra per-row slack covers allocator headers
        // and temporary identity/page-validation metadata; canonical payloads
        // retain their separately admitted original data owners.
        let per_row = 2 * riffdb_types::MAX_KEY_BYTES
            + 4096
            + 4 * std::mem::size_of::<EncodedPageItem<StoredEntityRecordV1>>();
        let maximum = wanted
            .checked_mul(per_row)
            .and_then(|bytes| bytes.checked_add(32 * 1024))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(super::corrupt)?;
        let work = budget
            .reserve(maximum)
            .map_err(|_| super::storage_error(riffdb_storage_api::StorageErrorKind::Unavailable))?;
        let validated_head = match &self.access {
            crate::store::RedbReadAccess::Current(root)
            | crate::store::RedbReadAccess::Durable(root) => {
                root.cached_provider_snapshot_head_v1()
            }
            crate::store::RedbReadAccess::Composite(view) => {
                view.cached_provider_snapshot_head_v1()
            }
        }
        .ok_or_else(|| super::storage_error(riffdb_storage_api::StorageErrorKind::Unavailable))?;
        let application_head = validated_head.map_or(
            riffdb_types::FrontierPosition::BeforeFirst,
            riffdb_types::FrontierPosition::AppliedThrough,
        );
        let bytes = self.read_provider_entity_partition_bytes_v1(request, budget)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(wanted)
            .map_err(|_| super::storage_error(riffdb_storage_api::StorageErrorKind::Unavailable))?;
        let mut has_more = false;
        for row in bytes.iter() {
            let (physical_key, encoded) = &***row;
            if request
                .after()
                .is_some_and(|after| physical_key.as_ref() == after.as_bytes())
            {
                continue;
            }
            if records.len() == wanted {
                has_more = true;
                break;
            }
            let record =
                riffdb_storage_api::decode_provider_entity_record_admitted_v1(encoded, budget)
                    .map_err(provider_decode_error)?;
            if record.target().key().as_bytes() != physical_key.as_ref() {
                return Err(super::corrupt());
            }
            let charge = EncodedContentCharge::new(encoded.len()).ok_or_else(super::corrupt)?;
            records.push(EncodedPageItem::new((**record).clone(), charge));
        }
        drop(bytes);
        let page =
            AuthoritativeEntityPartitionScanPage::new(request, application_head, records, has_more)
                .map_err(super::value_error)?;
        work.commit_value(page)
            .map_err(|_| super::storage_error(riffdb_storage_api::StorageErrorKind::Unavailable))
    }
}

fn provider_decode_error(error: riffdb_storage_api::ProviderRecordDecodeErrorV1) -> StorageError {
    match error {
        riffdb_storage_api::ProviderRecordDecodeErrorV1::AdmissionUnavailable => {
            super::storage_error(riffdb_storage_api::StorageErrorKind::Unavailable)
        }
        riffdb_storage_api::ProviderRecordDecodeErrorV1::Codec(error) => {
            crate::error::codec_error(error)
        }
    }
}
