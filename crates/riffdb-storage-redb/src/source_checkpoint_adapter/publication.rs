//! Actual copied-row equality against the genuine immutable second stage.
//! No projection IR, evaluation, detached success DTO or long primary read.
use super::{
    NativeSourceStageBrand, maintenance::NativeSourceCheckpointRequest,
    preparation::CurrentSourceStepRead,
};
use crate::{RedbOperationalPorts, source_usage_domain::SourceUsageEpoch};
use riffdb_projection::source_driver::SourceCheckpointOperationKind;
use riffdb_storage_api::proto_codec::{
    ProjectionSourceRecordDecodeErrorV1, encode_projection_source_maintenance_admitted_v1,
    encode_projection_source_object_admitted_v1, encode_projection_source_row_v1,
    projection_source_row_encode_work_bound_v1,
};
use riffdb_storage_api::{
    ProjectionSourceLifecycleV1, ProjectionSourceMaintenanceV1, ProjectionSourceObjectV1,
    StorageError, StorageErrorKind,
};
use riffdb_types::{
    CommitSequence, ContentDigest, MAX_KEY_BYTES, ProjectionSourceLocatorV1,
    ProjectionSourceWorkReservationV1, owned_allocation_capacity_bound_v1,
};
use std::mem::size_of;

// Constructor-private completed comparison, retaining only actual-owner/epoch
// and fixed committed facts. The raw pointer is never dereferenced or used as
// standalone authority: the genuine instruction keeps that exact stage alive.
// Raw pointer also preserves this worker-local owner's !Send boundary.
pub(crate) struct NativeSourcePublicationObservation<'work> {
    actual: &'work RedbOperationalPorts,
    stage_identity: *const NativeSourceStageBrand,
    locator: ProjectionSourceLocatorV1,
    descriptor: ContentDigest,
    frontier: CommitSequence,
    rows: u32,
    canonical_bytes: u64,
    rolling: ContentDigest,
    last_key: Vec<u8>,
    epoch: SourceUsageEpoch<'work>,
    // Last: copied key/control storage must physically drop before its charge.
    _work: ProjectionSourceWorkReservationV1,
}
impl Drop for NativeSourcePublicationObservation<'_> {
    fn drop(&mut self) {}
}
impl NativeSourcePublicationObservation<'_> {
    pub(super) fn validate_for<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
        &self,
        request: &NativeSourceCheckpointRequest<
            'request,
            'candidate,
            'stage,
            'slot,
            'session,
            'work,
        >,
    ) -> Result<(), StorageError>
    where
        'work: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        let descriptor = request.descriptor().ok_or_else(corrupt)?;
        let progress = request.before_progress().ok_or_else(corrupt)?;
        if request.operation() != SourceCheckpointOperationKind::Publish
            || !std::ptr::eq(self.actual, request.actual_ports())
            || !std::ptr::eq(
                self.stage_identity,
                std::ptr::from_ref(request.frozen_stage().brand),
            )
            || self.locator != request.captured_locator()
            || self.descriptor != descriptor.reference().digest()
            || self.frontier != descriptor.applied_through()
            || self.rows != descriptor.row_count()
            || self.rows != request.expected_row_count()
            || self.canonical_bytes != descriptor.canonical_row_bytes()
            || self.canonical_bytes != request.expected_canonical_row_bytes()
            || progress.descriptor() != descriptor
            || !progress.copy_complete()
            || progress.copied_rows() != self.rows
            || progress.copied_canonical_row_bytes() != self.canonical_bytes
            || progress.rolling_hash() != self.rolling
            || progress.last_copied_key().unwrap_or(&[]) != self.last_key.as_slice()
        {
            return Err(corrupt());
        }
        // This is cheap owner/version validation; no row hashing/serialization
        // is deferred to the primary gate. Ordinary command head changes do not
        // invalidate a fixed unchanged Source image.
        self.epoch
            .verify_for_native_step_observation(request.actual_ports(), self.locator)
    }
}

pub(crate) fn verify_publication_copy<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
    request: &NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
) -> Result<NativeSourcePublicationObservation<'work>, StorageError>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    if request.operation() != SourceCheckpointOperationKind::Publish
        || request.after_progress().is_some()
        || request.copy_batch().is_some()
        || request.transaction_binding_assignment().is_some()
    {
        return Err(corrupt());
    }
    let descriptor = request.descriptor().ok_or_else(corrupt)?;
    let expected = request.before_progress().ok_or_else(corrupt)?;
    if expected.descriptor() != descriptor
        || !expected.copy_complete()
        || descriptor.reference().locator() != request.captured_locator()
        || descriptor.row_count() != request.expected_row_count()
        || descriptor.canonical_row_bytes() != request.expected_canonical_row_bytes()
    {
        return Err(corrupt());
    }
    let actual = request.actual_ports();
    let budget = actual.shared.database.source_working_budget();
    let epoch = actual
        .shared
        .database
        .source_usage_domain()
        .capture_epoch()?;
    epoch.verify_for_native_step_observation(actual, request.captured_locator())?;
    // Both key buffers, fixed pair-reference scratch, semantic progress clones
    // and returned inline proof are admitted before any construction.
    let key_work = owned_allocation_capacity_bound_v1(MAX_KEY_BYTES, std::mem::align_of::<u8>())
        .map_err(|_| unavailable())?;
    let work = ProjectionSourceMaintenanceV1::initial_work_bound_v1()
        .map_err(|_| limit())?
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(key_work.checked_mul(2)?))
        .and_then(|bytes| {
            bytes.checked_add(size_of::<NativeSourcePublicationObservation<'work>>() as u64)
        })
        .and_then(|bytes| bytes.checked_add(size_of::<[(&[u8], &[u8]); 128]>() as u64))
        .ok_or_else(unavailable)?;
    let work = budget.reserve(work).map_err(|_| unavailable())?;
    let mut last_key = Vec::with_capacity(MAX_KEY_BYTES);
    let mut native_key = Vec::with_capacity(MAX_KEY_BYTES);
    let mut progress = ProjectionSourceMaintenanceV1::initial(
        descriptor.clone(),
        expected.expected_prior_root().cloned(),
    )
    .map_err(|_| corrupt())?;
    {
        let read = CurrentSourceStepRead::capture(request)?;
        let object_key = crate::source_keys::source_object_key(request.captured_locator());
        let maintenance_key =
            crate::source_keys::source_maintenance_key(request.captured_locator());
        let stored_object = read
            .point(
                riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceObjects,
                &object_key,
            )?
            .ok_or_else(corrupt)?;
        let clone_work = budget
            .reserve(descriptor.clone_work_bound_v1().map_err(|_| limit())?)
            .map_err(|_| unavailable())?;
        let object =
            ProjectionSourceObjectV1::new(descriptor.clone(), ProjectionSourceLifecycleV1::Copying);
        let object_bytes =
            encode_projection_source_object_admitted_v1(&object, budget).map_err(codec)?;
        if stored_object.as_bytes() != object_bytes.as_bytes() {
            return Err(corrupt());
        }
        drop(object_bytes);
        drop(object);
        drop(clone_work);
        drop(stored_object);
        let stored_progress = read
            .point(
                riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceMaintenance,
                &maintenance_key,
            )?
            .ok_or_else(corrupt)?;
        let expected_bytes =
            encode_projection_source_maintenance_admitted_v1(expected, budget).map_err(codec)?;
        if stored_progress.as_bytes() != expected_bytes.as_bytes() {
            return Err(corrupt());
        }
        read.finish()?;
    }
    let frozen = request.frozen_stage();
    let mut cursor = frozen.inner.readonly_cursor()?;
    let mut rows = 0_u32;
    let mut bytes = 0_u64;
    loop {
        epoch.verify_for_native_step_observation(actual, request.captured_locator())?;
        let page = {
            let read = CurrentSourceStepRead::capture(request)?;
            let page =
                read.source_rows_page((!last_key.is_empty()).then_some(last_key.as_slice()))?;
            read.finish()?;
            page
        }; // No primary read/transaction/scratch survives the owned page.
        let mut pairs = [(&[][..], &[][..]); 128];
        for (index, stored) in page.rows().iter().enumerate() {
            let staged = cursor.next()?.ok_or_else(corrupt)?;
            let row = staged.row();
            let encoded_work =
                projection_source_row_encode_work_bound_v1(row).map_err(|_| corrupt())?;
            let encoded_work = budget.reserve(encoded_work).map_err(|_| unavailable())?;
            let encoded = encode_projection_source_row_v1(row).map_err(|_| corrupt())?;
            crate::source_keys::encode_source_row_key_into(
                &mut native_key,
                request.captured_locator(),
                staged.order_key(),
            )
            .map_err(|_| corrupt())?;
            if native_key.as_slice() != stored.key()
                || encoded.as_bytes() != stored.value()
                || staged.encoded_content_charge().get() != stored.value().len()
            {
                return Err(corrupt());
            }
            rows = rows.checked_add(1).ok_or_else(limit)?;
            bytes = bytes
                .checked_add(stored.key().len() as u64)
                .and_then(|bytes| bytes.checked_add(stored.value().len() as u64))
                .ok_or_else(limit)?;
            if rows > descriptor.row_count() || bytes > descriptor.canonical_row_bytes() {
                return Err(corrupt());
            }
            pairs[index] = (stored.key(), stored.value());
            drop(encoded);
            drop(encoded_work);
            drop(staged);
        }
        if !page.rows().is_empty() {
            progress = progress
                .after_copy_step(&progress, &pairs[..page.rows().len()])
                .map_err(|_| corrupt())?;
            let last = page.rows().last().ok_or_else(corrupt)?.key();
            if last.len() > MAX_KEY_BYTES {
                return Err(limit());
            }
            last_key.clear();
            last_key.extend_from_slice(last);
        }
        epoch.verify_for_native_step_observation(actual, request.captured_locator())?;
        if page.complete() {
            if cursor.next()?.is_some() {
                return Err(corrupt());
            }
            break;
        }
        if page.rows().is_empty() {
            return Err(corrupt());
        }
    }
    drop(cursor);
    drop(native_key);
    if rows != descriptor.row_count()
        || bytes != descriptor.canonical_row_bytes()
        || &progress != expected
    {
        return Err(corrupt());
    }
    let rolling = progress.rolling_hash();
    drop(progress);
    epoch.verify_for_native_step_observation(actual, request.captured_locator())?;
    let observation = NativeSourcePublicationObservation {
        actual,
        stage_identity: std::ptr::from_ref(frozen.brand),
        locator: request.captured_locator(),
        descriptor: descriptor.reference().digest(),
        frontier: descriptor.applied_through(),
        rows,
        canonical_bytes: bytes,
        rolling,
        last_key,
        epoch,
        _work: work,
    };
    observation.validate_for(request)?;
    Ok(observation)
}
fn corrupt() -> StorageError {
    crate::error::storage_error(StorageErrorKind::CorruptData)
}
fn unavailable() -> StorageError {
    crate::error::storage_error(StorageErrorKind::Unavailable)
}
fn limit() -> StorageError {
    crate::error::storage_error(StorageErrorKind::LimitExceeded)
}
fn codec(error: ProjectionSourceRecordDecodeErrorV1) -> StorageError {
    match error {
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => unavailable(),
        ProjectionSourceRecordDecodeErrorV1::Codec(_) => corrupt(),
    }
}
