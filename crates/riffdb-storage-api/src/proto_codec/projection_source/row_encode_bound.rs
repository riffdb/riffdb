//! Allocation-free preflight for the exact existing SourceRow encoder.
use super::*;
use riffdb_proto::durable::ReadableRecordMessage;
use riffdb_types::{
    canonical_record_encoded_len, canonical_value_encoded_len, owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

fn limit() -> DurableCodecError {
    DurableCodecError::new(DurableCodecErrorKind::LimitExceeded)
}
fn length_add(total: &mut usize, bytes: usize) -> Result<(), DurableCodecError> {
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
// Every field in the unchanged three messages has a one-byte field tag.
fn framed_len(bytes: usize) -> Result<usize, DurableCodecError> {
    let length = u64::try_from(bytes).map_err(|_| limit())?;
    bytes
        .checked_add(varint_len(length))
        .and_then(|n| n.checked_add(1))
        .ok_or_else(limit)
}
fn canonical_measure_len(value: &ProjectionSourceRowV1) -> Result<usize, DurableCodecError> {
    canonical_record_encoded_len(value.measures()).map_err(|_| DurableCodecError::invariant())
}
fn canonical_group_len(value: &riffdb_types::CanonicalValue) -> Result<usize, DurableCodecError> {
    canonical_value_encoded_len(value).map_err(|_| DurableCodecError::invariant())
}
fn payload_len(value: &ProjectionSourceRowV1) -> Result<usize, DurableCodecError> {
    let identity = value.identity();
    let mut identity_bytes = framed_len(identity.contract_lineage().as_str().len())?;
    length_add(
        &mut identity_bytes,
        1 + varint_len(u64::from(identity.projection_id().get())),
    )?;
    length_add(
        &mut identity_bytes,
        framed_len(identity.plan_hash().as_bytes().len())?,
    )?;
    let locator = value.locator();
    let mut locator_bytes = framed_len(locator.database_id().as_bytes().len())?;
    length_add(
        &mut locator_bytes,
        1 + varint_len(locator.allocation_history_incarnation()),
    )?;
    length_add(&mut locator_bytes, 1 + varint_len(locator.ordinal().get()))?;
    let mut payload = framed_len(identity_bytes)?;
    length_add(&mut payload, framed_len(locator_bytes)?)?;
    for group in value.group_values() {
        length_add(&mut payload, framed_len(canonical_group_len(group)?)?)?;
    }
    length_add(&mut payload, framed_len(canonical_measure_len(value)?)?)?;
    length_add(
        &mut payload,
        1 + varint_len(value.last_changed_sequence().get()),
    )?;
    Ok(payload)
}

/// Allocation-free exact compact-envelope charge of the unchanged row encoding.
/// Counts canonical documents and all actual scalar varints, including the
/// tentative allocator ordinal. Native key bytes are a separate exact charge.
pub(super) fn projection_source_row_encoded_charge_v1(
    value: &ProjectionSourceRowV1,
) -> Result<EncodedContentCharge, DurableCodecError> {
    let framed = riffdb_proto::envelope::maximum_encoded_compact_record_bytes(
        wire::StoredProjectionSourceRowV1::record_schema(),
        payload_len(value)?,
    )
    .map_err(DurableCodecError::from_encode_envelope)?;
    EncodedContentCharge::new(framed).ok_or_else(DurableCodecError::invariant)
}

/// Compiled current-format maximum, including the exact compact header.
/// This grants neither a staging allocation nor admission for a maximum row.
pub fn projection_source_row_max_encoded_bytes_v1() -> Result<usize, DurableCodecError> {
    let schema = wire::StoredProjectionSourceRowV1::record_schema();
    riffdb_proto::envelope::maximum_encoded_compact_record_bytes(schema, schema.max_payload_bytes())
        .map_err(DurableCodecError::from_encode_envelope)
}

fn add_allocation(
    total: &mut u64,
    bytes: usize,
    alignment: usize,
) -> Result<(), DurableCodecError> {
    let charge = owned_allocation_capacity_bound_v1(bytes, alignment).map_err(|_| limit())?;
    *total = total.checked_add(charge).ok_or_else(limit)?;
    Ok(())
}

/// Allocation-free additional work allowance before encoding this borrowed row.
/// Includes the generated message's exact-capacity group vector, every owned
/// canonical document/string/identity buffer, and the one final compact output.
/// Each allocation uses the same alignment/allocator bound as owned inspection.
/// The original row remains separately charged. Retain this reservation until
/// all message/output buffers have physically dropped or transferred to an
/// independently admitted owner; computing the bound itself grants no budget.
pub fn projection_source_row_encode_work_bound_v1(
    value: &ProjectionSourceRowV1,
) -> Result<u64, DurableCodecError> {
    let encoded = projection_source_row_encoded_charge_v1(value)?.get();
    let mut total = u64::try_from(
        size_of::<wire::StoredProjectionSourceRowV1>() + size_of::<CanonicalStoredEnvelopeV1>(),
    )
    .map_err(|_| limit())?;
    add_allocation(
        &mut total,
        value.identity().contract_lineage().as_str().len(),
        align_of::<u8>(),
    )?;
    add_allocation(
        &mut total,
        value.identity().plan_hash().as_bytes().len(),
        align_of::<u8>(),
    )?;
    add_allocation(
        &mut total,
        value.locator().database_id().as_bytes().len(),
        align_of::<u8>(),
    )?;
    let headers = value
        .group_values()
        .len()
        .checked_mul(size_of::<Vec<u8>>())
        .ok_or_else(limit)?;
    add_allocation(&mut total, headers, align_of::<Vec<u8>>())?;
    for group in value.group_values() {
        add_allocation(&mut total, canonical_group_len(group)?, align_of::<u8>())?;
    }
    add_allocation(&mut total, canonical_measure_len(value)?, align_of::<u8>())?;
    add_allocation(&mut total, encoded, align_of::<u8>())?;
    Ok(total)
}

#[cfg(test)]
mod tests;
