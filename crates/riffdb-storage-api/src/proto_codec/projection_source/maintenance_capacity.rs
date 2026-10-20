//! Exact size-only eligibility for a frozen candidate's future controls.
//! No semantic progress, canonical envelope or native authority is returned.
use super::*;
use riffdb_types::{ProjectionSourceWorkingBudgetV1, owned_allocation_capacity_bound_v1};
use std::mem::{align_of, size_of};

/// First-party storage/Projection composition only. This checks the actual
/// initial descriptor/prior root and the authenticated frozen-stream maximum
/// COMPLETE native key length. It is not an application API or proof of copying.
///
/// Row count/native bytes only increase up to the descriptor's exact totals;
/// their Prost widths are monotone. Every digest is fixed width. Descriptor and
/// prior root stay byte-identical. Therefore maximal counters plus maximal key
/// length bound each possible intermediate complete Maintenance envelope.
#[doc(hidden)]
pub fn projection_source_maintenance_future_encoded_bound_admitted_v1(
    initial: &ProjectionSourceMaintenanceV1,
    maximum_native_key_bytes: usize,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<EncodedContentCharge, ProjectionSourceRecordDecodeErrorV1> {
    if initial.copied_rows() != 0
        || initial.copied_canonical_row_bytes() != 0
        || initial.last_copied_key().is_some()
    {
        return Err(DurableCodecError::corrupt().into());
    }
    let target = initial.descriptor();
    if (target.row_count() == 0) != (maximum_native_key_bytes == 0) {
        return Err(DurableCodecError::corrupt().into());
    }
    if maximum_native_key_bytes > riffdb_types::MAX_KEY_BYTES {
        return Err(DurableCodecError::new(DurableCodecErrorKind::LimitExceeded).into());
    }
    // Same sole generated-buffer census as the admitted control encoder. The
    // borrowed semantic input keeps its original owner. No encode/output/census
    // scratch graph is built here: only one generated M plus one bounded key.
    let key_bound = owned_allocation_capacity_bound_v1(maximum_native_key_bytes, align_of::<u8>())
        .map_err(|_| DurableCodecError::new(DurableCodecErrorKind::LimitExceeded))?;
    let generated = control_encode_buffers::maintenance_heap(initial)?
        .checked_add(key_bound)
        .and_then(|n| n.checked_add(size_of::<wire::StoredProjectionSourceMaintenanceV1>() as u64))
        .ok_or_else(|| DurableCodecError::new(DurableCodecErrorKind::LimitExceeded))?;
    let work = budget.reserve(generated)?;
    let mut message = metadata::maintenance_to_wire(initial);
    message.copied_rows = target.row_count();
    message.copied_canonical_row_bytes = target.canonical_row_bytes();
    if maximum_native_key_bytes != 0 {
        let mut size_only_key = Vec::new();
        size_only_key
            .try_reserve_exact(maximum_native_key_bytes)
            .map_err(|_| DurableCodecError::new(DurableCodecErrorKind::LimitExceeded))?;
        if owned_allocation_capacity_bound_v1(size_only_key.capacity(), align_of::<u8>())
            .map_err(|_| DurableCodecError::new(DurableCodecErrorKind::LimitExceeded))?
            > key_bound
        {
            return Err(DurableCodecError::new(DurableCodecErrorKind::LimitExceeded).into());
        }
        // These bytes are used ONLY for Prost encoded_len. They are never
        // encoded/decoded, exposed as a semantic progress value or persisted.
        size_only_key.resize(maximum_native_key_bytes, 0);
        message.last_copied_key = Some(size_only_key);
    }
    let complete = encoded_message_charge(MAINTENANCE, &message)?;
    let fits = complete.get() <= MAX_PROJECTION_SOURCE_CONTROL_BYTES;
    drop(message);
    drop(work);
    if !fits {
        return Err(DurableCodecError::new(DurableCodecErrorKind::LimitExceeded).into());
    }
    Ok(complete)
}
