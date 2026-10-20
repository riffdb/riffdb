//! Bounded canonical key and cross-record checks, not publication/GC authority.
use super::*;
use riffdb_storage_api::{ProjectionSourceLifecycleV1, ProjectionSourceRootV1};
use riffdb_types::{
    CanonicalValue, ProjectionGeneration, ProjectionGroupKeyBuilder, ProjectionIdentity,
    canonical_value_clone_work_bound_v1, owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

pub(super) fn decode_error(error: ProjectionSourceRecordDecodeErrorV1) -> StorageError {
    match error {
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => unavailable(),
        _ => integrity(),
    }
}

pub(super) fn root_for_identity(
    read: &CensusRead<'_>,
    identity: &ProjectionIdentity,
    page: &mut PageBudget,
) -> Result<Option<ProjectionSourceWorkAllocationV1<ProjectionSourceRootV1>>, StorageError> {
    // Called only at the start of a fixed-metadata step, with room for its at
    // most two root records and one object/control point read. No population map.
    let bytes = owned_allocation_capacity_bound_v1(MAX_KEY_BYTES, 1)
        .map_err(|_| limit())?
        .checked_add(size_of::<Vec<u8>>() as u64)
        .ok_or_else(limit)?;
    let _work = read.budget().reserve(bytes).map_err(|_| unavailable())?;
    let mut prefix = Vec::with_capacity(MAX_KEY_BYTES);
    encode_source_root_identity_prefix_into(&mut prefix, identity).map_err(|_| integrity())?;
    let first = match read.next(N::ProjectionSourceRoots, &[], Some(&prefix), page)? {
        ReadRow::End => return Ok(None),
        ReadRow::PageFull => return Err(integrity()),
        ReadRow::Row(row) => row,
    };
    let root = decode_projection_source_root_admitted_v1(first.value(), read.budget())
        .map_err(decode_error)?
        .into_parts()
        .0;
    check_source_root_key(first.key(), identity, root.reference().locator())
        .map_err(|_| integrity())?;
    match read.next(N::ProjectionSourceRoots, first.key(), Some(&prefix), page)? {
        ReadRow::End => {}
        ReadRow::Row(_) => return Err(integrity()),
        ReadRow::PageFull => return Err(integrity()),
    }
    Ok(Some(root))
}

pub(super) fn validate_row(
    read: &CensusRead<'_>,
    encoded: &EncodedRow,
    object: &ProjectionSourceObjectV1,
) -> Result<(), StorageError> {
    let row = decode_projection_source_row_structural_admitted_v1(encoded.value(), read.budget())
        .map_err(decode_error)?
        .into_parts()
        .0;
    let descriptor = object.descriptor();
    if row.locator() != descriptor.reference().locator()
        || row.identity() != descriptor.reference().identity()
        || row.last_changed_sequence() > descriptor.applied_through()
    {
        return Err(integrity());
    }
    // The structural decoder already constructs a valid bounded complete group
    // key. Reconstruct its ordering witness under separately admitted clone and
    // Vec-growth work, without reinterpreting schema or expression semantics.
    let count = row.group_values().len().max(4);
    let old_components = count
        .checked_mul(size_of::<CanonicalValue>())
        .ok_or_else(limit)?;
    let new_components = old_components.checked_mul(2).ok_or_else(limit)?;
    let mut bytes = size_of::<ProjectionGroupKeyBuilder>() as u64;
    // Existing builder bytes: old<=MAX_KEY, new<=2*MAX_KEY. Its one temporary
    // identity/scalar canonical encoding is separately<=MAX_KEY, pre-reserved by
    // the current typed canonical encoder. Component Vec has one old/new growth.
    for (capacity, alignment) in [
        (MAX_KEY_BYTES, 1),
        (2 * MAX_KEY_BYTES, 1),
        (MAX_KEY_BYTES, 1),
        (old_components, align_of::<CanonicalValue>()),
        (new_components, align_of::<CanonicalValue>()),
    ] {
        bytes = add(
            bytes,
            owned_allocation_capacity_bound_v1(capacity, alignment).map_err(|_| limit())?,
        )?;
    }
    for value in row.group_values() {
        bytes = add(
            bytes,
            canonical_value_clone_work_bound_v1(value).map_err(|_| limit())?,
        )?;
    }
    let _work = read.budget().reserve(bytes).map_err(|_| unavailable())?;
    let mut builder =
        ProjectionGroupKeyBuilder::new(row.identity().clone(), ProjectionGeneration::first());
    for value in row.group_values() {
        builder
            .push_component(value.clone())
            .map_err(|_| integrity())?;
    }
    let key = builder.finish().map_err(|_| integrity())?;
    check_source_row_key(encoded.key(), row.locator(), &key).map_err(|_| integrity())?;
    // key dies before clone work, and both die before the admitted row graph.
    drop(key);
    drop(_work);
    drop(row);
    Ok(())
}

pub(super) fn validate_object_location(
    object: &ProjectionSourceObjectV1,
    lineage: ChangelogLineageV3,
    allocator: ProjectionCheckpointOrdinalAllocatorV1,
    through: Option<riffdb_types::CommitSequence>,
) -> Result<(), StorageError> {
    let locator = object.descriptor().reference().locator();
    if through.is_none_or(|through| object.descriptor().applied_through() > through) {
        return Err(integrity());
    }
    if locator.database_id() != lineage.database_id()
        || locator.allocation_history_incarnation() > lineage.history_incarnation()
    {
        return Err(integrity());
    }
    if locator.allocation_history_incarnation() == lineage.history_incarnation()
        && let riffdb_storage_api::ProjectionCheckpointOrdinalPositionV1::Next(next) =
            allocator.position()
        && locator.ordinal().get() >= next.get()
    {
        return Err(integrity());
    }
    if object.descriptor().canonical_row_bytes()
        > riffdb_storage_api::MAX_PROJECTION_SOURCE_OBJECT_BYTES
    {
        return Err(limit());
    }
    Ok(())
}

pub(super) fn finish_object(current: &CurrentObject, last_key: &[u8]) -> Result<(), StorageError> {
    let descriptor = current.object.descriptor();
    if current.rows > descriptor.row_count() || current.bytes > descriptor.canonical_row_bytes() {
        return Err(integrity());
    }
    match current.object.lifecycle() {
        ProjectionSourceLifecycleV1::Published | ProjectionSourceLifecycleV1::Superseded => {
            if current.rows != descriptor.row_count()
                || current.bytes != descriptor.canonical_row_bytes()
            {
                return Err(integrity());
            }
        }
        ProjectionSourceLifecycleV1::Copying => {
            let maintenance = current.maintenance.as_ref().ok_or_else(integrity)?;
            if current.rows != maintenance.copied_rows()
                || current.bytes != maintenance.copied_canonical_row_bytes()
                || maintenance.last_copied_key() != (!last_key.is_empty()).then_some(last_key)
            {
                return Err(integrity());
            }
        }
        ProjectionSourceLifecycleV1::Aborting => {
            let maintenance = current.maintenance.as_ref().ok_or_else(integrity)?;
            if current.rows > maintenance.copied_rows()
                || current.bytes > maintenance.copied_canonical_row_bytes()
                || (!last_key.is_empty()
                    && maintenance
                        .last_copied_key()
                        .is_none_or(|last| last_key > last))
            {
                return Err(integrity());
            }
        }
        ProjectionSourceLifecycleV1::Collecting => {}
        ProjectionSourceLifecycleV1::Removed => {
            if current.rows != 0 || current.bytes != 0 {
                return Err(integrity());
            }
        }
    }
    Ok(())
}
