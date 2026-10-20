//! Allocation-by-allocation admission for the two closed control encoders.
use super::*;
use crate::ChangelogHistoryStateV3;
use riffdb_types::owned_allocation_capacity_bound_v1;
use std::mem::{align_of, size_of};

fn add(total: &mut u64, bytes: u64) -> Result<(), DurableCodecError> {
    *total = total
        .checked_add(bytes)
        .ok_or_else(DurableCodecError::invariant)?;
    Ok(())
}
fn allocation(total: &mut u64, bytes: usize, alignment: usize) -> Result<(), DurableCodecError> {
    add(
        total,
        owned_allocation_capacity_bound_v1(bytes, alignment)
            .map_err(|_| DurableCodecError::invariant())?,
    )
}
fn buffer(total: &mut u64, bytes: usize) -> Result<(), DurableCodecError> {
    allocation(total, bytes, align_of::<u8>())
}
fn history(total: &mut u64) -> Result<(), DurableCodecError> {
    // history_to_wire: database, catalog, anchor/tail/minimum history hashes.
    for bytes in [16, 32, 32, 32, 32] {
        buffer(total, bytes)?;
    }
    Ok(())
}
fn publication(total: &mut u64) -> Result<(), DurableCodecError> {
    // publication_to_wire: database and prior history hash; other fields inline.
    buffer(total, 16)?;
    buffer(total, 32)
}
pub(super) fn settlement_inline_bound() -> Result<u64, DurableCodecError> {
    type Allocation =
        riffdb_types::exact_provider_budget_v1::ProviderAllocation<CanonicalStoredEnvelopeV1>;
    size_of::<Allocation>()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(2 * align_of::<Allocation>()))
        .and_then(|bytes| bytes.checked_add(1024))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(DurableCodecError::invariant)
}
fn base<M>(cap: usize) -> Result<u64, DurableCodecError> {
    let mut total = OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1;
    add(
        &mut total,
        u64::try_from(size_of::<M>() + size_of::<CanonicalStoredEnvelopeV1>())
            .map_err(|_| DurableCodecError::invariant())?,
    )?;
    add(&mut total, settlement_inline_bound()?)?;
    // encode_message_preflighted writes directly into one final compact Vec,
    // with_capacity(total). No intermediate payload Vec or reallocation exists.
    buffer(&mut total, cap)?;
    Ok(total)
}
fn originals(total: &mut u64, active: bool) -> Result<(), DurableCodecError> {
    // One caller-owned scratch Vec is reused and never grows. Each legacy
    // envelope is destroyed before its generated original DTO is decoded.
    buffer(total, INNER_ENVELOPE_CAP)?;
    let role_bytes = [
        "riffdb.storage.v1.StoredRecordRegistryV2",
        "riffdb.storage.v1.StoredRecordRegistryV3",
        "riffdb.storage.v1.ReplicationPrimaryAdmissionV1",
        "riffdb.storage.v1.StoredHistoryIncarnationV1",
        "riffdb.storage.v1.StoredRetentionWatermarkV1",
    ]
    .iter()
    .fold(0, |maximum, role| maximum.max(role.len()));
    // Exact closed legacy-envelope owners: record_type, schema_hash, payload.
    buffer(total, role_bytes)?;
    buffer(total, 32)?;
    buffer(total, INNER_ENVELOPE_CAP)?;
    add(
        total,
        u64::try_from(size_of::<wire::StoredEnvelope>())
            .map_err(|_| DurableCodecError::invariant())?,
    )?;
    // V3 registry: root digest, Completed database and repeated exact digest.
    // V2 registry uses only the first allocation; incarnation owns no heap.
    for bytes in [32, 16, 32] {
        buffer(total, bytes)?;
    }
    add(
        total,
        u64::try_from(size_of::<wire::StoredRecordRegistryV3>())
            .map_err(|_| DurableCodecError::invariant())?,
    )?;
    if active {
        // Active is scalar + database16. A canonically encoded but inadmissible
        // Fenced variant is decoded before explicit rejection: one Box<Fence>,
        // operation/request/capability/database/hold IDs, observed hash, two
        // strings bounded256 by the existing closed preflight. No semantic
        // Fenced/principal reconstruction is performed by this codec.
        buffer(total, 16)?;
        allocation(
            total,
            size_of::<wire::StoredPrimaryFenceAdministrationV1>(),
            align_of::<wire::StoredPrimaryFenceAdministrationV1>(),
        )?;
        for bytes in [16, 16, 16, 16, 16, 32, 256, 256] {
            buffer(total, bytes)?;
        }
        add(
            total,
            u64::try_from(size_of::<wire::ReplicationPrimaryAdmissionV1>())
                .map_err(|_| DurableCodecError::invariant())?,
        )?;
    } else {
        // Watermark owns a hash and optional root digest. Source allocator is
        // the existing stack-only decoder; semantic scalar owners are Copy.
        buffer(total, 32)?;
        buffer(total, 32)?;
        add(
            total,
            u64::try_from(
                size_of::<wire::StoredRetentionWatermarkV1>()
                    + size_of::<wire::StoredHistoryIncarnationV1>(),
            )
            .map_err(|_| DurableCodecError::invariant())?,
        )?;
    }
    Ok(())
}

// One closed semantic predecessor-history Box. Native catalog decode admits
// this before constructing either a wire or semantic owner. Fresh reserves the
// same finite allowance but does not allocate a Box.
pub(super) fn catalog_semantic_history_box_work_bound() -> Result<u64, DurableCodecError> {
    owned_allocation_capacity_bound_v1(
        size_of::<ChangelogHistoryStateV3>(),
        align_of::<ChangelogHistoryStateV3>(),
    )
    .map_err(|_| DurableCodecError::invariant())
}

pub(super) fn catalog_encode_work_bound(
    value: &NativeCatalogTransitionV1,
) -> Result<u64, DurableCodecError> {
    let mut total = base::<wire::StoredAuthoritativeCatalogTransitionV1>(CATALOG_CAP)?;
    // The separately settled input DTO remains charged by its actual ledger.
    // Wire database, catalog and registry digest, then publication's two Vecs.
    for bytes in [16, 32, 32] {
        buffer(&mut total, bytes)?;
    }
    publication(&mut total)?;
    if let NativeCatalogTransitionOriginV1::V2ToV3 {
        original_registry,
        original_active_admission,
        ..
    } = &value.parts().origin
    {
        // Prost boxes this closed oneof arm; admit that exact allocation before
        // catalog_to_wire constructs it. The exhaustive wire inspector visits
        // the Box and its independently owned original/history buffers.
        allocation(
            &mut total,
            size_of::<wire::stored_authoritative_catalog_transition_v1::V2ToV3>(),
            align_of::<wire::stored_authoritative_catalog_transition_v1::V2ToV3>(),
        )?;
        originals(&mut total, true)?;
        history(&mut total)?;
        buffer(&mut total, original_registry.as_bytes().len())?;
        buffer(&mut total, original_active_admission.as_bytes().len())?;
        buffer(&mut total, 32)?;
    }
    Ok(total)
}

pub(super) fn inheritance_encode_work_bound(
    value: &SourceInheritanceAdministrationV1,
) -> Result<u64, DurableCodecError> {
    let mut total = base::<wire::StoredSourceInheritanceAdministrationV1>(INHERITANCE_CAP)?;
    originals(&mut total, false)?;
    history(&mut total)?;
    publication(&mut total)?;
    // Successor database/catalog, original-origin digest, authorized selection.
    for bytes in [16, 32, 32, 32] {
        buffer(&mut total, bytes)?;
    }
    let count = value.parts().witnesses.len();
    let headers = count
        .checked_mul(size_of::<Vec<u8>>())
        .ok_or_else(DurableCodecError::invariant)?;
    allocation(&mut total, headers, align_of::<Vec<u8>>())?;
    for _ in 0..count {
        buffer(&mut total, 96)?;
    }
    let scalars = &value.parts().predecessor_scalars;
    for bytes in [
        scalars
            .source_allocator
            .as_ref()
            .map(NativeTransitionEnvelopeBytesV1::as_bytes),
        scalars
            .incarnation
            .as_ref()
            .map(NativeTransitionEnvelopeBytesV1::as_bytes),
        scalars
            .registry
            .as_ref()
            .map(NativeTransitionEnvelopeBytesV1::as_bytes),
        scalars
            .watermark
            .as_ref()
            .map(NativeTransitionEnvelopeBytesV1::as_bytes),
    ]
    .into_iter()
    .flatten()
    {
        buffer(&mut total, bytes.len())?;
    }
    Ok(total)
}
