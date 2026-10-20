//! Source-owned encoding of the four authoritative control roles.
//! The borrowed semantic graph keeps its existing owner. Only newly constructed
//! generated buffers and the final canonical envelope are admitted here.
use super::*;
use riffdb_types::{ProjectionSourceWorkReservationV1, owned_allocation_capacity_bound_v1};
use std::mem::{align_of, size_of};

pub(super) use super::control_encode_buffers::{descriptor_heap, maintenance_heap, root_heap};

fn limit() -> DurableCodecError {
    DurableCodecError::new(DurableCodecErrorKind::LimitExceeded)
}
fn add(total: u64, bytes: u64) -> Result<u64, DurableCodecError> {
    total.checked_add(bytes).ok_or_else(limit)
}
fn allocation(bytes: usize) -> Result<u64, DurableCodecError> {
    owned_allocation_capacity_bound_v1(bytes, align_of::<u8>()).map_err(|_| limit())
}
pub(super) fn buffers(lengths: &[usize]) -> Result<u64, DurableCodecError> {
    lengths
        .iter()
        .try_fold(0, |total, length| add(total, allocation(*length)?))
}
pub(super) fn work_bound<M>(heap: u64) -> Result<u64, DurableCodecError> {
    // All nested generated messages are inline; the closed constructors above
    // enumerate every heap buffer. The ordinary encoder writes its one final
    // compact Vec directly and checks the unchanged4KiB limit before output.
    let inline = u64::try_from(size_of::<M>()).map_err(|_| limit())?;
    // This is the actual settlement metadata bound used by settle_owned_value;
    // ProviderAllocation's public size follows any future field/layout change.
    type Owner =
        riffdb_types::exact_provider_budget_v1::ProviderAllocation<CanonicalStoredEnvelopeV1>;
    let owner = size_of::<Owner>()
        .checked_mul(2)
        .and_then(|n| n.checked_add(2 * align_of::<Owner>()))
        .and_then(|n| n.checked_add(1024))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(limit)?;
    add(
        add(
            add(add(heap, inline)?, owner)?,
            allocation(MAX_PROJECTION_SOURCE_CONTROL_BYTES)?,
        )?,
        OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    )
}
pub(super) fn object_work_bound(
    value: &ProjectionSourceObjectV1,
) -> Result<u64, DurableCodecError> {
    work_bound::<wire::StoredProjectionSourceObjectV1>(descriptor_heap(value.descriptor())?)
}
fn root_work_bound(value: &ProjectionSourceRootV1) -> Result<u64, DurableCodecError> {
    work_bound::<wire::StoredProjectionSourceRootV1>(root_heap(value)?)
}
fn maintenance_work_bound(value: &ProjectionSourceMaintenanceV1) -> Result<u64, DurableCodecError> {
    work_bound::<wire::StoredProjectionSourceMaintenanceV1>(maintenance_heap(value)?)
}
fn finish<M: prost::Message + riffdb_proto::durable::WritableRecordMessage>(
    name: &'static str,
    message: M,
    work: ProjectionSourceWorkReservationV1,
) -> Result<
    ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    let encoded = encode_control(name, &message)?;
    drop(message);
    // The new envelope contains only its own Vec, never semantic input Arcs.
    work.settle_owned_value(encoded).map_err(Into::into)
}

/// Preadmits every generated object buffer and its retained canonical output.
/// The borrowed descriptor retains its original semantic owner and authority.
pub fn encode_projection_source_object_admitted_v1(
    value: &ProjectionSourceObjectV1,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    let work = budget.reserve(object_work_bound(value)?)?;
    finish(OBJECT, metadata::object_to_wire(value), work)
}
/// Preadmits the assigned root's generated graph and canonical output.
/// Encoding a structural root grants no native publication authority.
pub fn encode_projection_source_root_admitted_v1(
    value: &ProjectionSourceRootV1,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    let work = budget.reserve(root_work_bound(value)?)?;
    finish(ROOT, metadata::root_to_wire(value), work)
}
/// Preadmits complete maintenance metadata, including its optional prior root
/// and last key. The unchanged complete4KiB envelope limit still refuses.
pub fn encode_projection_source_maintenance_admitted_v1(
    value: &ProjectionSourceMaintenanceV1,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    let work = budget.reserve(maintenance_work_bound(value)?)?;
    finish(MAINTENANCE, metadata::maintenance_to_wire(value), work)
}
/// Preadmits one next-or-exhausted scalar allocator envelope. This creates no
/// ordinal assignment or permission to advance durable allocator state.
pub fn encode_projection_checkpoint_ordinal_allocator_admitted_v1(
    value: &ProjectionCheckpointOrdinalAllocatorV1,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    let work = budget.reserve(work_bound::<
        wire::StoredProjectionCheckpointOrdinalAllocatorV1,
    >(0)?)?;
    finish(ALLOCATOR, metadata::allocator_to_wire(*value), work)
}
