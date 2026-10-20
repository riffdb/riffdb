//! Actual-engine short Source-table reads. No raw root/table or callback escapes.
use super::*;
use crate::{
    error::{precommit_storage_error, table_error},
    store::{RedbReadAccess, SourceReadWork},
};
use redb::TableDefinition;
use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, BorrowedOverlayPointV1, CompositeTableV1,
    proto_codec::decode_projection_checkpoint_ordinal_allocator_inline_v1,
};
use std::ops::Bound::{Excluded, Included, Unbounded};

pub(super) struct CensusRead<'ports> {
    // Explicit Drop destroys the snapshot before its admitted scratch/ports.
    access: Option<RedbReadAccess>,
    ports: &'ports crate::RedbOperationalPorts,
    history: riffdb_storage_api::ChangelogHistoryStateV3,
    _scratch: SourceReadWork,
}
impl Drop for CensusRead<'_> {
    fn drop(&mut self) {
        drop(self.access.take());
    }
}

pub(super) enum ReadRow {
    Row(EncodedRow),
    End,
    PageFull,
}
pub(super) struct EncodedRow {
    key: Vec<u8>,
    value: Vec<u8>,
    // Last: every copied byte physically dies before its admission is released.
    _work: riffdb_types::ProjectionSourceWorkReservationV1,
}
impl EncodedRow {
    pub(super) fn key(&self) -> &[u8] {
        &self.key
    }
    pub(super) fn value(&self) -> &[u8] {
        &self.value
    }
    pub(super) fn bytes(&self) -> u64 {
        (self.key.len() + self.value.len()) as u64
    }
}
impl Drop for EncodedRow {
    fn drop(&mut self) {}
}

#[derive(Default)]
pub(super) struct PageBudget {
    pub(super) rows: u16,
    pub(super) bytes: u64,
}
impl PageBudget {
    fn take(&mut self, key: usize, value: usize) -> Result<bool, StorageError> {
        let bytes = key
            .checked_add(value)
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(limit)?;
        if bytes > riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_BYTES {
            return Err(limit());
        }
        if self.rows == riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_ROWS
            || self.bytes.checked_add(bytes).ok_or_else(limit)?
                > riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_BYTES
        {
            return Ok(false);
        }
        self.rows += 1;
        self.bytes += bytes;
        Ok(true)
    }
    fn can_read(&self, namespace: N) -> Result<bool, StorageError> {
        // No speculative extra row beyond the byte ceiling: require room for
        // the compiled maximum complete key/value before opening the iterator.
        // Smaller legitimate rows still consume only their actual byte charge.
        let maximum = riffdb_types::MAX_KEY_BYTES
            .checked_add(max_value_bytes(namespace))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(limit)?;
        Ok(
            self.rows < riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_ROWS
                && riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_BYTES - self.bytes >= maximum,
        )
    }
    pub(super) fn full(&self) -> bool {
        self.rows == riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_ROWS
            || self.bytes == riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_BYTES
    }
}

impl<'ports> CensusRead<'ports> {
    pub(super) fn capture(
        ports: &'ports crate::RedbOperationalPorts,
        epoch: &SourceUsageEpoch<'_>,
        expected: Option<riffdb_storage_api::ChangelogLineageV3>,
    ) -> Result<(Self, riffdb_storage_api::ChangelogLineageV3), StorageError> {
        let domain = ports.shared.database.source_usage_domain();
        if !std::ptr::eq(domain, epoch.domain) {
            return Err(integrity());
        }
        domain.require_epoch(epoch)?;
        if !ports.shared.is_source_mode()
            || ports
                .shared
                .write_fenced
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(unavailable());
        }
        // Reuse the qualified native scratch primitive. Fixed metadata is512KiB;
        // row copies and every decoded graph have separate admitted ownership.
        if !riffdb_proto::envelope::release_locator_decode_scratch_v1() {
            return Err(unavailable());
        }
        let scratch =
            SourceReadWork::admit(ports.shared.database.source_working_budget(), 512 * 1024)?;
        let access = ports.begin_composite_read()?;
        let history = crate::changelog_v3_cursor::capture_history_position_v1(&access)?;
        let lineage = history.lineage();
        if lineage.catalog_digest() != riffdb_storage_api::AuthoritativeStateCatalogV3.digest()
            || expected.is_some_and(|before| {
                before.database_id() != lineage.database_id()
                    || before.history_incarnation() != lineage.history_incarnation()
                    || before.catalog_digest() != lineage.catalog_digest()
            })
        {
            return Err(unavailable());
        }
        // Origin is eligibility only, never a zero/census/source-closure proof.
        crate::native_source_fresh::validate_same_owner_cold_origin(
            &ports.shared.database,
            &access,
        )?;
        let admission = crate::primary_admission_roots::read_source_admission(&access)?;
        if admission.fence().is_some() || admission.lineage() != lineage {
            return Err(unavailable());
        }
        domain.require_epoch(epoch)?;
        Ok((
            Self {
                access: Some(access),
                ports,
                history,
                _scratch: scratch,
            },
            lineage,
        ))
    }
    fn access(&self) -> Result<&RedbReadAccess, StorageError> {
        self.access.as_ref().ok_or_else(integrity)
    }
    pub(super) fn source_through(&self) -> Option<riffdb_types::CommitSequence> {
        self.history.tail().frontier().application()
    }
    pub(super) fn budget(&self) -> &riffdb_types::ProjectionSourceWorkingBudgetV1 {
        self.ports.shared.database.source_working_budget()
    }
    // Closed four-table inventory. These tables are not JournalTable22 members;
    // all production Source row/control writers are Immediate barrier owners.
    // A future overlay table requires a new explicit arm and qualification.
    fn table(
        &self,
        namespace: N,
    ) -> Result<redb::ReadOnlyTable<&'static [u8], &'static [u8]>, StorageError> {
        match namespace {
            N::ProjectionSourceRows
            | N::ProjectionSourceObjects
            | N::ProjectionSourceRoots
            | N::ProjectionSourceMaintenance => {}
            _ => return Err(integrity()),
        }
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(namespace.table());
        self.access()?.open_table(definition).map_err(table_error)
    }
    fn copied(
        &self,
        namespace: N,
        key: &[u8],
        value: &[u8],
        page: &mut PageBudget,
    ) -> Result<ReadRow, StorageError> {
        if key.is_empty() || key.len() > riffdb_types::MAX_KEY_BYTES {
            return Err(integrity());
        }
        let max = max_value_bytes(namespace);
        if value.is_empty() || value.len() > max {
            return Err(limit());
        }
        if !page.take(key.len(), value.len())? {
            return Ok(ReadRow::PageFull);
        }
        // Two independently requested Vec allocations, no growth or shrink copy.
        let work_bytes = riffdb_types::owned_allocation_capacity_bound_v1(key.len(), 1)
            .map_err(|_| limit())?
            .checked_add(
                riffdb_types::owned_allocation_capacity_bound_v1(value.len(), 1)
                    .map_err(|_| limit())?,
            )
            .and_then(|n| n.checked_add(std::mem::size_of::<EncodedRow>() as u64))
            .ok_or_else(limit)?;
        let work = self
            .budget()
            .reserve(work_bytes)
            .map_err(|_| unavailable())?;
        let mut copied_key = Vec::with_capacity(key.len());
        copied_key.extend_from_slice(key);
        let mut copied_value = Vec::with_capacity(value.len());
        copied_value.extend_from_slice(value);
        Ok(ReadRow::Row(EncodedRow {
            key: copied_key,
            value: copied_value,
            _work: work,
        }))
    }
    pub(super) fn next(
        &self,
        namespace: N,
        after: &[u8],
        prefix: Option<&[u8]>,
        page: &mut PageBudget,
    ) -> Result<ReadRow, StorageError> {
        if after.len() > riffdb_types::MAX_KEY_BYTES
            || prefix.is_some_and(|p| p.len() > riffdb_types::MAX_KEY_BYTES)
        {
            return Err(limit());
        }
        if !page.can_read(namespace)? {
            return Ok(ReadRow::PageFull);
        }
        let table = self.table(namespace)?;
        let start = if after.is_empty() {
            prefix.map_or(Unbounded, Included)
        } else {
            Excluded(after)
        };
        let mut range = table
            .range::<&[u8]>((start, Unbounded))
            .map_err(precommit_storage_error)?;
        let Some((key, value)) = range.next().transpose().map_err(precommit_storage_error)? else {
            return Ok(ReadRow::End);
        };
        if prefix.is_some_and(|p| !key.value().starts_with(p)) {
            return Ok(ReadRow::End);
        };
        self.copied(namespace, key.value(), value.value(), page)
    }
    pub(super) fn point(
        &self,
        namespace: N,
        key: &[u8],
        page: &mut PageBudget,
    ) -> Result<ReadRow, StorageError> {
        if key.is_empty() || key.len() > riffdb_types::MAX_KEY_BYTES {
            return Err(limit());
        }
        if !page.can_read(namespace)? {
            return Ok(ReadRow::PageFull);
        }
        if namespace == N::ProjectionSourceAllocator {
            let expected = N::ProjectionSourceAllocator
                .metadata_key()
                .ok_or_else(integrity)?;
            if key != expected.as_bytes() {
                return Err(integrity());
            }
            let access = self.access()?;
            if let RedbReadAccess::Composite(view) = access {
                match view
                    .overlay()
                    .borrowed_provider_point_v1(CompositeTableV1::Meta, key)
                {
                    BorrowedOverlayPointV1::Tombstone => return Ok(ReadRow::End),
                    BorrowedOverlayPointV1::Value(value) => {
                        return self.copied(namespace, key, value, page);
                    }
                    BorrowedOverlayPointV1::Checkpoint => {}
                }
            }
            let metadata = access
                .open_table(crate::layout::META)
                .map_err(table_error)?;
            return match metadata.get(expected).map_err(precommit_storage_error)? {
                Some(value) => self.copied(namespace, key, value.value(), page),
                None => Ok(ReadRow::End),
            };
        }
        let table = self.table(namespace)?;
        let Some(value) = table.get(key).map_err(precommit_storage_error)? else {
            return Ok(ReadRow::End);
        };
        self.copied(namespace, key, value.value(), page)
    }
    pub(super) fn allocator(
        &self,
        page: &mut PageBudget,
    ) -> Result<
        (
            riffdb_storage_api::ProjectionCheckpointOrdinalAllocatorV1,
            u64,
        ),
        StorageError,
    > {
        let key = N::ProjectionSourceAllocator
            .metadata_key()
            .ok_or_else(integrity)?;
        let access = self.access()?;
        if let RedbReadAccess::Composite(view) = access {
            match view
                .overlay()
                .borrowed_provider_point_v1(CompositeTableV1::Meta, key.as_bytes())
            {
                BorrowedOverlayPointV1::Tombstone => return Err(integrity()),
                BorrowedOverlayPointV1::Value(value) => {
                    return allocator_charge(
                        key,
                        value,
                        self.history.lineage().history_incarnation(),
                        page,
                    );
                }
                BorrowedOverlayPointV1::Checkpoint => {}
            }
        }
        let meta = access
            .open_table(crate::layout::META)
            .map_err(table_error)?;
        let value = meta
            .get(key)
            .map_err(precommit_storage_error)?
            .ok_or_else(integrity)?;
        allocator_charge(
            key,
            value.value(),
            self.history.lineage().history_incarnation(),
            page,
        )
    }
}
fn allocator_charge(
    key: &str,
    bytes: &[u8],
    incarnation: u64,
    page: &mut PageBudget,
) -> Result<
    (
        riffdb_storage_api::ProjectionCheckpointOrdinalAllocatorV1,
        u64,
    ),
    StorageError,
> {
    if bytes.len() > riffdb_storage_api::MAX_PROJECTION_SOURCE_CONTROL_BYTES {
        return Err(limit());
    }
    // Strict allocation-free scalar decoder; no Vec/TLS/per-cursor ledger.
    let allocator = decode_projection_checkpoint_ordinal_allocator_inline_v1(bytes)
        .map_err(crate::error::codec_error)?;
    if allocator.value().allocation_history_incarnation() != incarnation {
        return Err(integrity());
    }
    if !page.take(key.len(), bytes.len())? {
        return Err(integrity());
    }
    let charge = key
        .len()
        .checked_add(bytes.len())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(limit)?;
    Ok((*allocator.value(), charge))
}

fn max_value_bytes(namespace: N) -> usize {
    if namespace == N::ProjectionSourceRows {
        use riffdb_proto::durable::ReadableRecordMessage;
        riffdb_proto::storage::v1::StoredProjectionSourceRowV1::record_schema().max_envelope_bytes()
    } else {
        riffdb_storage_api::MAX_PROJECTION_SOURCE_CONTROL_BYTES
    }
}
