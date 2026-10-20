//! Allocation-free source-control sizing. Length alone grants no authority.
use super::*;
use riffdb_proto::durable::ReadableRecordMessage;
fn limit() -> DurableCodecError {
    DurableCodecError::new(DurableCodecErrorKind::LimitExceeded)
}
fn add(total: &mut usize, bytes: usize) -> Result<(), DurableCodecError> {
    *total = total.checked_add(bytes).ok_or_else(limit)?;
    Ok(())
}
fn varint_len(mut value: u64) -> usize {
    let mut bytes = 1;
    while value >= 128 {
        bytes += 1;
        value >>= 7;
    }
    bytes
}
// Every field in these existing SourceRoot/reference/binding messages has a
// one-byte tag. Optional future fields are present in the maximum-width bound.
fn framed_len(bytes: usize) -> Result<usize, DurableCodecError> {
    let length = u64::try_from(bytes).map_err(|_| limit())?;
    bytes
        .checked_add(varint_len(length))
        .and_then(|n| n.checked_add(1))
        .ok_or_else(limit)
}
fn reference_len(value: &ProjectionSourceReferenceV1) -> Result<usize, DurableCodecError> {
    let identity = value.identity();
    let mut identity_bytes = framed_len(identity.contract_lineage().as_str().len())?;
    add(
        &mut identity_bytes,
        1 + varint_len(u64::from(identity.projection_id().get())),
    )?;
    add(
        &mut identity_bytes,
        framed_len(identity.plan_hash().as_bytes().len())?,
    )?;
    let locator = value.locator();
    let mut locator_bytes = framed_len(locator.database_id().as_bytes().len())?;
    add(
        &mut locator_bytes,
        1 + varint_len(locator.allocation_history_incarnation()),
    )?;
    add(&mut locator_bytes, 1 + varint_len(locator.ordinal().get()))?;
    let mut bytes = framed_len(identity_bytes)?;
    add(&mut bytes, framed_len(locator_bytes)?)?;
    for digest in [value.digest(), value.origin()] {
        let mut digest_bytes = 1 + varint_len(u64::from(digest.scheme()));
        add(&mut digest_bytes, framed_len(digest.as_bytes().len())?)?;
        add(&mut bytes, framed_len(digest_bytes)?)?;
    }
    Ok(bytes)
}
/// Allocation-free upper bound for a future canonical compact SourceRoot over
/// this exact immutable reference. Publication sequence/predecessor/frontier
/// and optional administration counters use their largest legal varint widths.
/// The publication incarnation is the reference's exact incarnation, as the
/// existing SourceRoot structural constructor requires. Native key bytes are
/// separate. This neither reserves memory nor grants publication/ancestry.
#[doc(hidden)]
pub fn projection_source_root_future_encoded_bound_v1(
    reference: &ProjectionSourceReferenceV1,
) -> Result<EncodedContentCharge, DurableCodecError> {
    let scalar = 1 + varint_len(u64::MAX);
    let frontier = scalar.checked_mul(2).ok_or_else(limit)?;
    let locator = reference.locator();
    let mut publication = framed_len(locator.database_id().as_bytes().len())?;
    add(
        &mut publication,
        1 + varint_len(locator.allocation_history_incarnation()),
    )?;
    add(&mut publication, scalar.checked_mul(2).ok_or_else(limit)?)?;
    add(
        &mut publication,
        framed_len(frontier)?.checked_mul(2).ok_or_else(limit)?,
    )?;
    add(&mut publication, framed_len(32)?)?;
    let mut payload = framed_len(reference_len(reference)?)?;
    add(&mut payload, framed_len(publication)?)?;
    add(&mut payload, scalar)?;
    let complete = riffdb_proto::envelope::maximum_encoded_compact_record_bytes(
        wire::StoredProjectionSourceRootV1::record_schema(),
        payload,
    )
    .map_err(DurableCodecError::from_encode_envelope)?;
    if complete > MAX_PROJECTION_SOURCE_CONTROL_BYTES {
        return Err(limit());
    }
    EncodedContentCharge::new(complete).ok_or_else(limit)
}
#[cfg(test)]
mod tests;
