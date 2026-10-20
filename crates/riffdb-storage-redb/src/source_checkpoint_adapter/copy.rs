//! Fixed bounded canonical copy preparation from the genuine second frozen map.
use super::stage::{NativeSourceFrozen, NativeSourceFrozenRow};
use super::*;
use riffdb_projection::source_driver::{
    SourceFrozenRow, SourcePrepareCopyInstruction, SourcePreparedCopyBatch,
    SourcePreparedCopyEvidence, SourcePreparedCopyRow,
};
use riffdb_storage_api::proto_codec::{
    CanonicalStoredEnvelopeV1, encode_projection_source_row_v1,
    projection_source_row_encode_work_bound_v1,
};
use riffdb_storage_api::{EncodedContentCharge, ProjectionSourceRowV1};
use riffdb_types::{
    ProjectionGroupKey, ProjectionSourceWorkReservationV1, owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

const MAX_ROWS: usize = 128;
const MAX_CANONICAL_BYTES: usize = 4 * 1024 * 1024;

#[doc(hidden)]
pub struct NativeSourceCopyRow {
    row: NativeSourceFrozenRow,
    native_key: Vec<u8>,
    canonical_value: CanonicalStoredEnvelopeV1,
    // All generated encode buffers/output and native-key capacity were admitted
    // before construction; retain the allowance through physical bytes' lifetime.
    _wire_work: ProjectionSourceWorkReservationV1,
}
impl SourceFrozenRow for NativeSourceCopyRow {
    fn row(&self) -> &ProjectionSourceRowV1 {
        self.row.row()
    }
    fn order_key(&self) -> &ProjectionGroupKey {
        self.row.order_key()
    }
    fn encoded_content_charge(&self) -> EncodedContentCharge {
        self.row.encoded_content_charge()
    }
    fn native_key_bytes(&self) -> usize {
        self.native_key.len()
    }
}
impl SourcePreparedCopyRow for NativeSourceCopyRow {
    fn native_key(&self) -> &[u8] {
        &self.native_key
    }
    fn canonical_value(&self) -> &[u8] {
        self.canonical_value.as_bytes()
    }
}
#[doc(hidden)]
pub struct NativeSourceCopyBatch {
    rows: Vec<NativeSourceCopyRow>,
    _row_controls: ProjectionSourceWorkReservationV1,
}
impl SourcePreparedCopyBatch for NativeSourceCopyBatch {
    type Row = NativeSourceCopyRow;
    fn rows(&self) -> &[Self::Row] {
        &self.rows
    }
}

pub(super) fn prepare_copy_batch<'prepare, 'session, 'stage, 'slot, 'work>(
    frozen: &mut NativeSourceFrozen<'stage, 'slot, 'work>,
    actual: &NativeSourceSessionBrand<'work>,
    request: SourcePrepareCopyInstruction<
        'prepare,
        'session,
        'stage,
        NativeSourceCheckpointBackend<'work>,
    >,
) -> Result<
    SourcePreparedCopyEvidence<'session, 'stage, NativeSourceCheckpointBackend<'work>>,
    SourceDriverError,
>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
{
    request.check_before_prepare(actual, frozen.brand)?;
    let descriptor = request.descriptor();
    if !actual.prefix.belongs_to_ports(actual.ports)
        || Some(descriptor.reference().locator()) != actual.prefix.tentative_source_locator()
        || descriptor.reference().identity() != actual.plan.identity()
        || descriptor.captured_source() != actual.prefix.captured_history()
        || descriptor.applied_through() != actual.prefix.captured_through()
        || request.before_progress().descriptor() != descriptor
    {
        return Err(SourceDriverError::ForeignCustody);
    }
    let budget = actual.prefix.originating_budget();
    let controls = MAX_ROWS
        .checked_mul(size_of::<NativeSourceCopyRow>())
        .ok_or(SourceDriverError::SourceUnavailable)?;
    let controls = owned_allocation_capacity_bound_v1(controls, align_of::<NativeSourceCopyRow>())
        .map_err(|_| SourceDriverError::SourceUnavailable)?;
    let controls = controls
        .checked_add(size_of::<NativeSourceCopyBatch>() as u64)
        .ok_or(SourceDriverError::SourceUnavailable)?;
    let row_controls = budget
        .reserve(controls)
        .map_err(|_| SourceDriverError::SourceUnavailable)?;
    let mut rows = Vec::with_capacity(MAX_ROWS);
    let mut copied_bytes = 0_usize;
    {
        let mut cursor = frozen.inner.cursor_after(request.last_order_key())?;
        while rows.len() < MAX_ROWS {
            let Some(inner) = cursor.next()? else {
                break;
            };
            let key_bytes = crate::source_keys::source_row_key_len(inner.order_key())
                .map_err(|_| SourceDriverError::InputChanged)?;
            let value_bytes = inner.encoded_content_charge().get();
            let row_bytes = key_bytes
                .checked_add(value_bytes)
                .ok_or(SourceDriverError::SourceUnavailable)?;
            let next_bytes = copied_bytes
                .checked_add(row_bytes)
                .ok_or(SourceDriverError::SourceUnavailable)?;
            if next_bytes > MAX_CANONICAL_BYTES {
                // The next preparation seeks after the last accepted key. This
                // dropped lookahead cannot advance durable progress or lose a row.
                if rows.is_empty() {
                    return Err(SourceDriverError::SourceUnavailable);
                }
                break;
            }
            let work = projection_source_row_encode_work_bound_v1(inner.row())
                .map_err(|_| SourceDriverError::InputChanged)?;
            let key_work = owned_allocation_capacity_bound_v1(key_bytes, align_of::<u8>())
                .map_err(|_| SourceDriverError::SourceUnavailable)?;
            let work = work
                .checked_add(key_work)
                .ok_or(SourceDriverError::SourceUnavailable)?;
            let wire_work = budget
                .reserve(work)
                .map_err(|_| SourceDriverError::SourceUnavailable)?;
            let mut native_key = Vec::with_capacity(key_bytes);
            crate::source_keys::encode_source_row_key_into(
                &mut native_key,
                descriptor.reference().locator(),
                inner.order_key(),
            )
            .map_err(|_| SourceDriverError::InputChanged)?;
            let canonical_value = encode_projection_source_row_v1(inner.row())
                .map_err(|_| SourceDriverError::InputChanged)?;
            if canonical_value.as_bytes().len() != value_bytes || native_key.len() != key_bytes {
                return Err(SourceDriverError::InputChanged);
            }
            rows.push(NativeSourceCopyRow {
                row: NativeSourceFrozenRow {
                    inner,
                    native_key_bytes: key_bytes,
                },
                native_key,
                canonical_value,
                _wire_work: wire_work,
            });
            copied_bytes = next_bytes;
        }
    }
    if rows.is_empty() {
        return Err(SourceDriverError::IncompleteInput);
    }
    request.attach(
        actual,
        frozen.brand,
        NativeSourceCopyBatch {
            rows,
            _row_controls: row_controls,
        },
    )
}
