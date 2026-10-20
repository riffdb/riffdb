//! One owned native cursor, sealed behind a mutable frozen-map borrow.
use super::*;
use redb::{OwnedRange, ReadableDatabase};

pub(crate) struct SourceStageCursor<'cursor, 'stage, 'work, 'schema> {
    range: Option<OwnedRange<&'static [u8], &'static [u8]>>,
    map: &'cursor mut SourceStageMap<'stage, 'work, 'schema>,
}

impl<'cursor, 'stage, 'work, 'schema> SourceStageCursor<'cursor, 'stage, 'work, 'schema> {
    pub(super) fn new(
        map: &'cursor mut SourceStageMap<'stage, 'work, 'schema>,
    ) -> Result<Self, StorageError> {
        Self::new_after(map, None)
    }

    pub(super) fn new_after(
        map: &'cursor mut SourceStageMap<'stage, 'work, 'schema>,
        after: Option<&[u8]>,
    ) -> Result<Self, StorageError> {
        if after.is_some_and(|key| key.is_empty() || key.len() > riffdb_types::MAX_KEY_BYTES) {
            return Err(integrity());
        }
        let result = (|| {
            let read = map.active()?.begin_read().map_err(|_| unavailable())?;
            let table = read.open_table(ROWS).map_err(|_| unavailable())?;
            match after {
                None => table.range_owned::<&[u8]>(..),
                Some(key) => table.range_owned::<&[u8]>((
                    std::ops::Bound::Excluded(key),
                    std::ops::Bound::Unbounded,
                )),
            }
            .map_err(|_| unavailable())
        })();
        match result {
            Ok(range) => Ok(Self {
                range: Some(range),
                map,
            }),
            Err(error) => {
                map.retire();
                Err(error)
            }
        }
    }

    pub(crate) fn next(&mut self) -> Result<Option<SourceStagedRow>, StorageError> {
        let result = self.next_inner();
        if result.is_err() {
            drop(self.range.take());
            self.map.retire();
        }
        result
    }

    fn next_inner(&mut self) -> Result<Option<SourceStagedRow>, StorageError> {
        self.map.active()?;
        let next = self.range.as_mut().ok_or_else(unavailable)?.next();
        let Some(next) = next else {
            return Ok(None);
        };
        let (stored_key, stored_value) = next.map_err(|_| unavailable())?;
        decode_staged_row(self.map, stored_key.value(), stored_value.value())
    }
}

pub(crate) struct SourceStagedRow {
    row: EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>>,
    order_key: ProjectionGroupKey,
    _key_work: ProjectionSourceWorkReservationV1,
}

impl SourceStagedRow {
    pub(crate) fn row(&self) -> &ProjectionSourceRowV1 {
        self.row.value()
    }
    pub(crate) fn order_key(&self) -> &ProjectionGroupKey {
        &self.order_key
    }
    pub(crate) fn encoded_content_charge(&self) -> riffdb_storage_api::EncodedContentCharge {
        self.row.encoded_content_charge()
    }
}

impl Drop for SourceStageCursor<'_, '_, '_, '_> {
    fn drop(&mut self) {
        drop(self.range.take());
    }
}

/// An immutable frozen-map read role. Errors produce no publication proof;
/// unlike mutable updates, this role cannot change or retire the map.
pub(crate) struct SourceReadonlyStageCursor<'cursor, 'stage, 'work, 'schema> {
    range: Option<OwnedRange<&'static [u8], &'static [u8]>>,
    map: &'cursor SourceStageMap<'stage, 'work, 'schema>,
}
impl<'cursor, 'stage, 'work, 'schema> SourceReadonlyStageCursor<'cursor, 'stage, 'work, 'schema> {
    pub(super) fn new(
        map: &'cursor SourceStageMap<'stage, 'work, 'schema>,
    ) -> Result<Self, StorageError> {
        let read = map.active()?.begin_read().map_err(|_| unavailable())?;
        let table = read.open_table(ROWS).map_err(|_| unavailable())?;
        let range = table.range_owned::<&[u8]>(..).map_err(|_| unavailable())?;
        Ok(Self {
            range: Some(range),
            map,
        })
    }
    pub(crate) fn next(&mut self) -> Result<Option<SourceStagedRow>, StorageError> {
        self.map.active()?;
        let Some(next) = self.range.as_mut().ok_or_else(unavailable)?.next() else {
            return Ok(None);
        };
        let (key, value) = next.map_err(|_| unavailable())?;
        decode_staged_row(self.map, key.value(), value.value())
    }
}
impl Drop for SourceReadonlyStageCursor<'_, '_, '_, '_> {
    fn drop(&mut self) {
        drop(self.range.take());
    }
}
fn decode_staged_row(
    map: &SourceStageMap<'_, '_, '_>,
    stored_key: &[u8],
    stored_value: &[u8],
) -> Result<Option<SourceStagedRow>, StorageError> {
    // Canonical/context decode admits all temporary/retained allocation
    // before construction; raw guards remain scoped to this call.
    let row = riffdb_storage_api::proto_codec::decode_projection_source_row_admitted_v1(
        stored_value,
        map.schema,
        map.tentative_locator,
        map.prefix.captured_through(),
        &map.budget,
    )
    .map_err(decode_error)?;
    let bytes = ProjectionSourceRowV1::stored_parts_work_bound_v1(
        row.value().group_values(),
        row.value().measures(),
    )
    .map_err(|_| limit())?;
    let key_work = map.budget.reserve(bytes).map_err(|_| unavailable())?;
    let order_key = map
        .schema
        .group_key(ProjectionGeneration::first(), row.value().group_values())
        .map_err(|_| integrity())?;
    if stored_key != order_key.as_bytes() {
        return Err(integrity());
    }
    // No page/transaction/DB alias escapes. The original ledger owns both
    // the decoded row and the reconstructed canonical key until destruction.
    Ok(Some(SourceStagedRow {
        row,
        order_key,
        _key_work: key_work,
    }))
}
