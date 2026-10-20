//! Single generated Source-control buffer census, shared by encode and sizing.
use super::*;
use riffdb_types::owned_allocation_capacity_bound_v1;
use std::mem::align_of;

fn limit() -> DurableCodecError {
    DurableCodecError::new(DurableCodecErrorKind::LimitExceeded)
}
fn add(total: u64, bytes: u64) -> Result<u64, DurableCodecError> {
    total.checked_add(bytes).ok_or_else(limit)
}
fn allocation(bytes: usize) -> Result<u64, DurableCodecError> {
    owned_allocation_capacity_bound_v1(bytes, align_of::<u8>()).map_err(|_| limit())
}
fn buffers(lengths: &[usize]) -> Result<u64, DurableCodecError> {
    lengths
        .iter()
        .try_fold(0, |total, length| add(total, allocation(*length)?))
}
fn reference_heap(value: &ProjectionSourceReferenceV1) -> Result<u64, DurableCodecError> {
    // identity String/plan hash; locator DB; descriptor and origin digests.
    buffers(&[
        value.identity().contract_lineage().as_str().len(),
        32,
        16,
        32,
        32,
    ])
}
pub(super) fn descriptor_heap(
    value: &ProjectionSourceDescriptorV1,
) -> Result<u64, DurableCodecError> {
    // identity String/plan hash; locator DB; captured history DB/catalog and
    // all three history hashes; origin, row-root and descriptor digests.
    let mut heap = buffers(&[
        value
            .reference()
            .identity()
            .contract_lineage()
            .as_str()
            .len(),
        32,
        16,
        16,
        32,
        32,
        32,
        32,
        32,
        32,
        32,
    ])?;
    if value.prior_source_digest().is_some() {
        heap = add(heap, allocation(32)?)?;
    }
    Ok(heap)
}
pub(super) fn root_heap(value: &ProjectionSourceRootV1) -> Result<u64, DurableCodecError> {
    // The entire assigned publication binding is inline except DB/prior hash.
    add(reference_heap(value.reference())?, buffers(&[16, 32])?)
}
pub(super) fn maintenance_heap(
    value: &ProjectionSourceMaintenanceV1,
) -> Result<u64, DurableCodecError> {
    let mut heap = descriptor_heap(value.descriptor())?;
    if let Some(root) = value.expected_prior_root() {
        heap = add(heap, root_heap(root)?)?;
    }
    if let Some(key) = value.last_copied_key() {
        heap = add(heap, allocation(key.len())?)?;
    }
    add(heap, allocation(32)?) // rolling copy digest
}
