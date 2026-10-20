//! Shared exact native Source charge arithmetic; no reservation or authority.
//! Extracted from the actual1808 numerical census for its public-scrub consumer.
use riffdb_storage_api::{
    DurableCodecError, MAX_PROJECTION_SOURCE_CONTROL_BYTES, MAX_PROJECTION_SOURCE_OBJECT_BYTES,
    ProjectionSourceReferenceV1, StorageValueError,
    proto_codec::projection_source_root_future_encoded_bound_v1,
};

pub(crate) enum SourceChargeErrorV1 {
    Capacity,
    Codec(DurableCodecError),
}
fn add(left: u64, right: u64) -> Result<u64, StorageValueError> {
    left.checked_add(right)
        .ok_or(StorageValueError::SizeOverflow)
}
/// Entire immutable object charge, including its actual native Object KV.
pub(crate) fn object_charge(rows: u64, object_kv: u64) -> Result<u64, StorageValueError> {
    let charge = add(rows, object_kv)?;
    if charge > MAX_PROJECTION_SOURCE_OBJECT_BYTES {
        return Err(StorageValueError::LimitExceeded);
    }
    Ok(charge)
}
/// Selected charge includes actual Root+Object KV and the full measured rows.
pub(crate) fn selected_charge(
    rows: u64,
    object_kv: u64,
    root_kv: u64,
) -> Result<u64, StorageValueError> {
    add(add(root_kv, object_kv)?, rows)
}
/// Exact1808 full reservation and its physically represented part. The allocator
/// occurs in both, cancelling from the remaining reservation so it counts once.
pub(crate) fn candidate_charge(
    reference: &ProjectionSourceReferenceV1,
    full_rows: u64,
    object_kv: u64,
    copied_rows: u64,
    maintenance_kv: u64,
    allocator_kv: u64,
) -> Result<(u64, u64), SourceChargeErrorV1> {
    let future_value = projection_source_root_future_encoded_bound_v1(reference)
        .map_err(SourceChargeErrorV1::Codec)?
        .get() as u64;
    let future_root = add(
        crate::source_keys::source_root_key_len(reference.identity()) as u64,
        future_value,
    )
    .map_err(|_| SourceChargeErrorV1::Capacity)?;
    let maximum_maintenance = add(
        crate::source_keys::SOURCE_LOCATOR_KEY_BYTES as u64,
        MAX_PROJECTION_SOURCE_CONTROL_BYTES as u64,
    )
    .map_err(|_| SourceChargeErrorV1::Capacity)?;
    let full = add(
        add(
            add(
                add(full_rows, object_kv).map_err(|_| SourceChargeErrorV1::Capacity)?,
                future_root,
            )
            .map_err(|_| SourceChargeErrorV1::Capacity)?,
            maximum_maintenance,
        )
        .map_err(|_| SourceChargeErrorV1::Capacity)?,
        allocator_kv,
    )
    .map_err(|_| SourceChargeErrorV1::Capacity)?;
    let actual = add(
        add(
            add(copied_rows, object_kv).map_err(|_| SourceChargeErrorV1::Capacity)?,
            maintenance_kv,
        )
        .map_err(|_| SourceChargeErrorV1::Capacity)?,
        allocator_kv,
    )
    .map_err(|_| SourceChargeErrorV1::Capacity)?;
    Ok((full, actual))
}
