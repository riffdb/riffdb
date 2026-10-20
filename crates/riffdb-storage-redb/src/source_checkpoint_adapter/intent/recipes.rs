//! Closed Source-only recipes from the actual native request and short current
//! read. These produce proposals only; Commit assignment and physical head CAS
//! remain necessary before any durable mutation or quota installation.
use super::super::{
    maintenance::NativeSourceCheckpointRequest, preparation::CurrentSourceStepRead,
};
use super::{NativeSourceStepIntent, builder::NativeSourceIntentBuilder};
use crate::source_keys::{source_maintenance_key, source_object_key};
use riffdb_projection::source_driver::{
    SourceCheckpointOperationKind as K, SourceFrozenRow, SourceNativePreparedAllocationEvidence,
    SourcePreparedCopyBatch, SourcePreparedCopyRow,
};
use riffdb_storage_api::proto_codec::{
    ProjectionSourceRecordDecodeErrorV1,
    encode_projection_checkpoint_ordinal_allocator_admitted_v1,
    encode_projection_source_maintenance_admitted_v1, encode_projection_source_object_admitted_v1,
    projection_source_maintenance_future_encoded_bound_admitted_v1,
};
use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, ProjectionSourceLifecycleV1 as Life, ProjectionSourceObjectV1,
    StorageError, StorageErrorKind,
};

pub(crate) fn prepare_allocation_intent<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
    request: &NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
) -> Result<NativeSourceStepIntent, StorageError>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    if request.operation() != K::Allocate
        || request.before_progress().is_some()
        || request.copy_batch().is_some()
        || request.transaction_binding_assignment().is_some()
    {
        return Err(corrupt());
    }
    let descriptor = request.descriptor().ok_or_else(corrupt)?;
    let after = request.after_progress().ok_or_else(corrupt)?;
    let observation = request.native_preparation().ok_or_else(corrupt)?;
    let assigned = request.ordinal_assignment().ok_or_else(corrupt)?;
    let locator = request.captured_locator();
    let expected = observation
        .allocator()
        .allocate_one(
            locator.database_id(),
            locator.allocation_history_incarnation(),
        )
        .map_err(|_| corrupt())?;
    if assigned != expected
        || assigned.locator() != locator
        || descriptor.reference().locator() != locator
        || descriptor.reference().identity() != request.projection_identity()
        || descriptor.captured_source() != request.captured_history()
        || after.descriptor() != descriptor
        || after.copied_rows() != 0
        || after.copied_canonical_row_bytes() != 0
        || after.last_copied_key().is_some()
        || after.expected_prior_root() != observation.prior_root()
    {
        return Err(corrupt());
    }
    let budget = request
        .actual_ports()
        .shared
        .database
        .source_working_budget();
    // Closed candidate sizing before intent/quota/input-obligation installation.
    // No scan, synthetic semantic progress, schema change or durable mutation.
    projection_source_maintenance_future_encoded_bound_admitted_v1(
        after,
        request.expected_maximum_native_key_bytes(),
        budget,
    )
    .map_err(codec)?;
    let read = CurrentSourceStepRead::capture(request)?;
    // Check canonical stored allocator bytes afresh. A known-before retry keeps
    // the same assignment/F and legitimately has a newer monotonic Source epoch.
    read.validate_expected_selection(observation)?;
    let allocator_key = N::ProjectionSourceAllocator
        .metadata_key()
        .ok_or_else(corrupt)?;
    let before_allocator = read
        .point(N::ProjectionSourceAllocator, allocator_key.as_bytes())?
        .ok_or_else(corrupt)?;
    if before_allocator.as_bytes() != observation.allocator_bytes() {
        return Err(stale());
    }
    let object_key = source_object_key(locator);
    let maintenance_key = source_maintenance_key(locator);
    if read
        .point(N::ProjectionSourceObjects, &object_key)?
        .is_some()
        || read
            .point(N::ProjectionSourceMaintenance, &maintenance_key)?
            .is_some()
    {
        return Err(stale());
    }
    // Every temporary semantic clone is admitted before construction. Its
    // canonical encoded owner and the final net-intent clones are also charged.
    let clone_work = budget
        .reserve(descriptor.clone_work_bound_v1().map_err(|_| limit())?)
        .map_err(|_| stale())?;
    let object = ProjectionSourceObjectV1::new(descriptor.clone(), Life::Copying);
    let object_bytes =
        encode_projection_source_object_admitted_v1(&object, budget).map_err(codec)?;
    drop(object);
    drop(clone_work);
    let maintenance_bytes =
        encode_projection_source_maintenance_admitted_v1(after, budget).map_err(codec)?;
    let allocator_bytes =
        encode_projection_checkpoint_ordinal_allocator_admitted_v1(&assigned.next(), budget)
            .map_err(codec)?;
    let mut builder = NativeSourceIntentBuilder::new(request, read.history())?;
    builder.put(
        N::ProjectionSourceObjects,
        &object_key,
        None,
        object_bytes.as_bytes(),
    )?;
    builder.put(
        N::ProjectionSourceMaintenance,
        &maintenance_key,
        None,
        maintenance_bytes.as_bytes(),
    )?;
    builder.put(
        N::ProjectionSourceAllocator,
        allocator_key.as_bytes(),
        Some(before_allocator.as_bytes()),
        allocator_bytes.as_bytes(),
    )?;
    read.finish()?;
    builder.finish()
}

pub(crate) fn prepare_copy_intent<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
    request: &NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
) -> Result<NativeSourceStepIntent, StorageError>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    if request.operation() != K::Copy
        || request.ordinal_assignment().is_none()
        || request.transaction_binding_assignment().is_some()
    {
        return Err(corrupt());
    }
    let descriptor = request.descriptor().ok_or_else(corrupt)?;
    let before = request.before_progress().ok_or_else(corrupt)?;
    let after = request.after_progress().ok_or_else(corrupt)?;
    let batch = request.copy_batch().ok_or_else(corrupt)?;
    let rows = batch.rows();
    if before.descriptor() != descriptor
        || after.descriptor() != descriptor
        || before.copy_complete()
        || rows.is_empty()
        || rows.len() > usize::from(riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_ROWS)
    {
        return Err(corrupt());
    }
    let budget = request
        .actual_ports()
        .shared
        .database
        .source_working_budget();
    let read = CurrentSourceStepRead::capture(request)?;
    let object_key = source_object_key(request.captured_locator());
    let stored_object = read
        .point(N::ProjectionSourceObjects, &object_key)?
        .ok_or_else(corrupt)?;
    let clone_work = budget
        .reserve(descriptor.clone_work_bound_v1().map_err(|_| limit())?)
        .map_err(|_| stale())?;
    let copying = ProjectionSourceObjectV1::new(descriptor.clone(), Life::Copying);
    let expected_object =
        encode_projection_source_object_admitted_v1(&copying, budget).map_err(codec)?;
    drop(copying);
    drop(clone_work);
    if stored_object.as_bytes() != expected_object.as_bytes() {
        return Err(stale());
    }
    let maintenance_key = source_maintenance_key(request.captured_locator());
    let stored_before = read
        .point(N::ProjectionSourceMaintenance, &maintenance_key)?
        .ok_or_else(corrupt)?;
    let expected_before =
        encode_projection_source_maintenance_admitted_v1(before, budget).map_err(codec)?;
    if stored_before.as_bytes() != expected_before.as_bytes() {
        return Err(stale());
    }
    // Recompute progress from complete native bytes off-gate. No nominal batch
    // count or structural complete flag can stand in for the actual copy chain.
    let mut pairs =
        [(&[][..], &[][..]); riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_ROWS as usize];
    let mut copied_bytes = 0_u64;
    for (index, row) in rows.iter().enumerate() {
        let value = row.row();
        if value.identity() != descriptor.reference().identity()
            || value.locator() != descriptor.reference().locator()
            || value.last_changed_sequence() > descriptor.applied_through()
            || row.order_key().identity() != value.identity()
            || row.order_key().components() != value.group_values()
            || row.encoded_content_charge().get() != row.canonical_value().len()
            || row.native_key_bytes() != row.native_key().len()
        {
            return Err(corrupt());
        }
        crate::source_keys::check_source_row_key(
            row.native_key(),
            request.captured_locator(),
            row.order_key(),
        )
        .map_err(|_| corrupt())?;
        copied_bytes = copied_bytes
            .checked_add(row.native_key().len() as u64)
            .and_then(|bytes| bytes.checked_add(row.canonical_value().len() as u64))
            .ok_or_else(limit)?;
        if copied_bytes > riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_BYTES {
            return Err(limit());
        }
        pairs[index] = (row.native_key(), row.canonical_value());
    }
    let progress_work = budget
        .reserve(before.copy_step_work_bound_v1().map_err(|_| limit())?)
        .map_err(|_| stale())?;
    let independently_after = before
        .after_copy_step(before, &pairs[..rows.len()])
        .map_err(|_| corrupt())?;
    if &independently_after != after {
        return Err(corrupt());
    }
    drop(independently_after);
    drop(progress_work);
    let after_bytes =
        encode_projection_source_maintenance_admitted_v1(after, budget).map_err(codec)?;
    let mut builder = NativeSourceIntentBuilder::new(request, read.history())?;
    for row in rows {
        builder.put(
            N::ProjectionSourceRows,
            row.native_key(),
            None,
            row.canonical_value(),
        )?;
    }
    builder.put(
        N::ProjectionSourceMaintenance,
        &maintenance_key,
        Some(stored_before.as_bytes()),
        after_bytes.as_bytes(),
    )?;
    read.finish()?;
    builder.finish()
}
fn corrupt() -> StorageError {
    crate::error::storage_error(StorageErrorKind::CorruptData)
}
fn limit() -> StorageError {
    crate::error::storage_error(StorageErrorKind::LimitExceeded)
}
fn stale() -> StorageError {
    crate::error::storage_error(StorageErrorKind::Unavailable)
}
fn codec(error: ProjectionSourceRecordDecodeErrorV1) -> StorageError {
    // All callers in this owner are PRODUCING encoders over already checked
    // semantic inputs, never a durable decoder. Capacity refusal grants no
    // exact-before/final-abort evidence and cannot release input obligations.
    match error {
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => stale(),
        ProjectionSourceRecordDecodeErrorV1::Codec(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::LimitExceeded =>
        {
            stale()
        }
        ProjectionSourceRecordDecodeErrorV1::Codec(_) => corrupt(),
    }
}

#[cfg(test)]
#[path = "../../native_producing_capacity_tests.rs"]
mod tests;
