//! Finite exact canonical KV deltas from one actual native instruction/epoch.
//! This is numeric admission only; candidate/publication proof remains separate.
use super::super::read::{CensusRead, EncodedRow, PageBudget, ReadRow};
use super::*;
use crate::source_checkpoint_adapter::NativeSourceCheckpointRequest;
use crate::source_keys::*;
use riffdb_projection::source_driver::SourceCheckpointOperationKind as Operation;
use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV3 as N, ChangelogLineageV3,
    ProjectionSourceLifecycleV1 as Life,
    proto_codec::{
        decode_projection_source_object_admitted_v1, decode_projection_source_root_admitted_v1,
        projection_source_root_future_encoded_bound_v1,
    },
};

// Source rows plus at most two roots, two object controls, one maintenance and
// one permanent allocator. Non-Source receipt metadata has its own native bound.
const MAX_CONTROL_MUTATIONS: usize = 6;

pub(crate) fn prepare_native_source_usage<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
    request: &NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
    intent: NativeSourceStepIntent,
) -> Result<NativePreparedSourceUsage<'work>, StorageError>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    let actual = request.actual_ports();
    let budget = actual.shared.database.source_working_budget();
    if !intent.belongs_to(budget)
        || intent.locator() != request.captured_locator()
        || intent.operation() != request.operation()
        || intent.predecessor_history().lineage() != request.captured_history().lineage()
    {
        return Err(integrity());
    }
    // Abort/recovery/GC require their distinct closed native requests. The new
    // checkpoint chain cannot synthesize those roles or guess their preimages.
    if !matches!(
        request.operation(),
        Operation::Allocate | Operation::Copy | Operation::Publish
    ) {
        return Err(unavailable());
    }
    let control = budget
        .reserve(
            (std::mem::size_of::<NativePreparedSourceUsage<'_>>()
                + std::mem::size_of::<SourceUsageMutation<'_>>()) as u64,
        )
        .map_err(|_| unavailable())?;
    let domain = actual.shared.database.source_usage_domain();
    let epoch = domain.capture_epoch()?;
    let (before, candidate) = {
        let state = domain.state.lock().map_err(|_| unavailable())?;
        if state.version != epoch.version || state.pending.is_some() || state.publication.is_some()
        {
            return Err(unavailable());
        }
        match (request.operation(), &state.input_obligation) {
            (Operation::Allocate, None) => {}
            (Operation::Copy | Operation::Publish, Some(obligation))
                if obligation.unresolved_intent.is_none()
                    && obligation.inputs.matches_owner(&actual.shared.database)
                    && obligation.inputs.captured_locator() == intent.locator() => {}
            _ => return Err(unavailable()),
        }
        (
            state.complete.ok_or_else(unavailable)?,
            state.complete_candidate,
        )
    };
    if before.candidate_reserved_bytes() != candidate.map(|candidate| candidate.full) {
        return Err(unavailable());
    }
    let lineage = request.captured_history().lineage();
    let mut delta = Delta::default();
    for mutation in intent.receipt().mutations() {
        if !namespace_affects_source_usage(mutation.namespace()) {
            continue;
        }
        validate_step_key(request.operation(), mutation, intent.locator())?;
        let old = point(
            actual,
            &epoch,
            lineage,
            mutation.namespace(),
            mutation.key(),
        )?;
        if !mutation.matches_prior(old.as_ref().map(|row| row.value())) {
            return Err(unavailable());
        }
        let removed = old.as_ref().map_or(0, EncodedRow::bytes);
        let added = mutation
            .value()
            .map_or(Ok(0), |value| kv(mutation.key(), value))?;
        delta.removed = add(delta.removed, removed)?;
        delta.added = add(delta.added, added)?;
        if mutation.namespace() == N::ProjectionSourceRows {
            delta.rows = delta.rows.checked_add(1).ok_or_else(limit)?;
            delta.row_before = add(delta.row_before, removed)?;
            delta.row_after = add(delta.row_after, added)?;
            if delta.rows > riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_ROWS
                || delta.row_before > riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_BYTES
                || delta.row_after > riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_BYTES
            {
                return Err(limit());
            }
        } else {
            delta.controls = delta.controls.checked_add(1).ok_or_else(limit)?;
            if delta.controls > MAX_CONTROL_MUTATIONS {
                return Err(limit());
            }
        }
        if let Some(candidate) = candidate
            && belongs_to_candidate(mutation, candidate.locator)?
        {
            delta.candidate_removed = add(delta.candidate_removed, removed)?;
            delta.candidate_added = add(delta.candidate_added, added)?;
        }
        if mutation.namespace() == N::ProjectionSourceRoots {
            if let Some(old) = old.as_ref() {
                let charge = selected_charge(
                    actual,
                    &epoch,
                    lineage,
                    &intent,
                    mutation.key(),
                    old.value(),
                    false,
                )?;
                delta.selected_removed = add(delta.selected_removed, charge)?;
                delta.roots_removed = delta.roots_removed.checked_add(1).ok_or_else(limit)?;
            }
            if let Some(value) = mutation.value() {
                let charge = selected_charge(
                    actual,
                    &epoch,
                    lineage,
                    &intent,
                    mutation.key(),
                    value,
                    true,
                )?;
                delta.selected_added = add(delta.selected_added, charge)?;
                delta.roots_added = delta.roots_added.checked_add(1).ok_or_else(limit)?;
            }
        }
        drop(old);
    }
    match request.operation() {
        Operation::Allocate if delta.controls != 3 || delta.rows != 0 => return Err(integrity()),
        Operation::Copy if delta.controls != 1 || delta.rows == 0 || delta.row_before != 0 => {
            return Err(integrity());
        }
        Operation::Publish
            if delta.rows != 0
                || delta.roots_added != 1
                || delta.roots_removed > 1
                || delta.controls != 3 + 2 * usize::from(delta.roots_removed) =>
        {
            return Err(integrity());
        }
        _ => {}
    }
    let physical_before = match candidate {
        Some(candidate) => subtract_add(before.total_bytes(), candidate.full, candidate.actual)?,
        None => before.total_bytes(),
    };
    let physical_after = subtract_add(physical_before, delta.removed, delta.added)?;
    let after_candidate = match request.operation() {
        Operation::Allocate => {
            if candidate.is_some() || delta.rows != 0 {
                return Err(unavailable());
            }
            Some(allocation_candidate(
                actual,
                &epoch,
                &intent,
                request.descriptor().ok_or_else(integrity)?,
            )?)
        }
        Operation::Copy => {
            let mut candidate = candidate.ok_or_else(unavailable)?;
            if candidate.locator != intent.locator() {
                return Err(integrity());
            }
            candidate.actual = subtract_add(
                candidate.actual,
                delta.candidate_removed,
                delta.candidate_added,
            )?;
            if candidate.actual > candidate.full {
                return Err(limit());
            }
            Some(candidate)
        }
        Operation::Publish => {
            if candidate.is_none_or(|candidate| candidate.locator != intent.locator()) {
                return Err(unavailable());
            }
            let object_key = source_object_key(intent.locator());
            let value = postimage(&intent, N::ProjectionSourceObjects, &object_key)
                .flatten()
                .ok_or_else(integrity)?;
            let object = decode_projection_source_object_admitted_v1(value, budget)
                .map_err(decode_error)?
                .into_parts()
                .0;
            if object.lifecycle() != Life::Published
                || Some(object.descriptor()) != request.descriptor()
                || postimage(
                    &intent,
                    N::ProjectionSourceMaintenance,
                    &source_maintenance_key(intent.locator()),
                ) != Some(None)
            {
                return Err(integrity());
            }
            None
        }
        _ => return Err(unavailable()),
    };
    let total_after = match after_candidate {
        Some(candidate) => subtract_add(physical_after, candidate.actual, candidate.full)?,
        None => physical_after,
    };
    let roots = before
        .selected_roots()
        .checked_sub(delta.roots_removed)
        .and_then(|roots| roots.checked_add(delta.roots_added))
        .ok_or_else(integrity)?;
    let selected = subtract_add(
        before.selected_bytes(),
        delta.selected_removed,
        delta.selected_added,
    )?;
    let after = Usage::new(
        roots,
        selected,
        total_after,
        after_candidate.map(|candidate| candidate.full),
    )
    .map_err(|_| limit())?;
    domain.require_epoch(&epoch)?;
    let new_inputs = if request.operation() == Operation::Allocate {
        Some(request.retain_original_inputs()?)
    } else {
        None
    };
    Ok(NativePreparedSourceUsage {
        actual,
        epoch: Some(epoch),
        before,
        after,
        before_candidate: candidate,
        after_candidate,
        intent: Some(intent),
        new_inputs,
        control: Some(control),
    })
}

#[derive(Default)]
struct Delta {
    removed: u64,
    added: u64,
    candidate_removed: u64,
    candidate_added: u64,
    selected_removed: u64,
    selected_added: u64,
    roots_removed: u16,
    roots_added: u16,
    rows: u16,
    controls: usize,
    row_before: u64,
    row_after: u64,
}
fn add(before: u64, amount: u64) -> Result<u64, StorageError> {
    before.checked_add(amount).ok_or_else(limit)
}
fn subtract_add(before: u64, removed: u64, added: u64) -> Result<u64, StorageError> {
    before
        .checked_sub(removed)
        .ok_or_else(integrity)?
        .checked_add(added)
        .ok_or_else(limit)
}
fn kv(key: &[u8], value: &[u8]) -> Result<u64, StorageError> {
    key.len()
        .checked_add(value.len())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(limit)
}
fn point(
    actual: &RedbOperationalPorts,
    epoch: &SourceUsageEpoch<'_>,
    lineage: ChangelogLineageV3,
    namespace: N,
    key: &[u8],
) -> Result<Option<EncodedRow>, StorageError> {
    // Each bounded point owns a fresh short actual read, then drops its snapshot.
    // Unrelated application writes can advance between points; Source version
    // equality is checked again after every read and at the final primary CAS.
    let (read, _) = CensusRead::capture(actual, epoch, Some(lineage))?;
    let value = read.point(namespace, key, &mut PageBudget::default())?;
    drop(read);
    epoch.domain.require_epoch(epoch)?;
    match value {
        ReadRow::Row(row) => Ok(Some(row)),
        ReadRow::End => Ok(None),
        ReadRow::PageFull => Err(limit()),
    }
}
fn validate_step_key(
    operation: Operation,
    mutation: &AuthoritativeMutationV3,
    locator: riffdb_types::ProjectionSourceLocatorV1,
) -> Result<(), StorageError> {
    match (operation, mutation.namespace()) {
        (Operation::Allocate, N::ProjectionSourceObjects)
            if decode_source_object_key(mutation.key()).map_err(|_| integrity())? == locator
                && mutation.value().is_some()
                && mutation.expected_hash().is_none() =>
        {
            Ok(())
        }
        (Operation::Allocate, N::ProjectionSourceMaintenance)
            if decode_source_maintenance_key(mutation.key()).map_err(|_| integrity())?
                == locator
                && mutation.value().is_some()
                && mutation.expected_hash().is_none() =>
        {
            Ok(())
        }
        (Operation::Allocate, N::ProjectionSourceAllocator) if mutation.value().is_some() => Ok(()),
        (Operation::Copy, N::ProjectionSourceRows)
            if source_row_locator_prefix(mutation.key()).map_err(|_| integrity())? == locator
                && mutation.value().is_some()
                && mutation.expected_hash().is_none() =>
        {
            Ok(())
        }
        (Operation::Copy, N::ProjectionSourceMaintenance)
            if decode_source_maintenance_key(mutation.key()).map_err(|_| integrity())?
                == locator
                && mutation.value().is_some()
                && mutation.expected_hash().is_some() =>
        {
            Ok(())
        }
        (Operation::Publish, N::ProjectionSourceObjects)
            if mutation.value().is_some() && mutation.expected_hash().is_some() =>
        {
            decode_source_object_key(mutation.key()).map_err(|_| integrity())?;
            Ok(())
        }
        (Operation::Publish, N::ProjectionSourceRoots) => Ok(()),
        (Operation::Publish, N::ProjectionSourceMaintenance)
            if decode_source_maintenance_key(mutation.key()).map_err(|_| integrity())?
                == locator
                && mutation.value().is_none()
                && mutation.expected_hash().is_some() =>
        {
            Ok(())
        }
        _ => Err(integrity()),
    }
}
fn belongs_to_candidate(
    mutation: &AuthoritativeMutationV3,
    locator: riffdb_types::ProjectionSourceLocatorV1,
) -> Result<bool, StorageError> {
    let selected = match mutation.namespace() {
        N::ProjectionSourceRows => {
            source_row_locator_prefix(mutation.key()).map_err(|_| integrity())?
        }
        N::ProjectionSourceObjects => {
            decode_source_object_key(mutation.key()).map_err(|_| integrity())?
        }
        N::ProjectionSourceMaintenance => {
            decode_source_maintenance_key(mutation.key()).map_err(|_| integrity())?
        }
        N::ProjectionSourceAllocator => return Ok(true),
        N::ProjectionSourceRoots | N::Existing(_) | N::GenerationSourceBases => return Ok(false),
    };
    Ok(selected == locator)
}
fn postimage<'intent>(
    intent: &'intent NativeSourceStepIntent,
    namespace: N,
    key: &[u8],
) -> Option<Option<&'intent [u8]>> {
    intent
        .receipt()
        .mutations()
        .iter()
        .find(|mutation| mutation.namespace() == namespace && mutation.key() == key)
        .map(|mutation| mutation.value())
}
fn selected_charge(
    actual: &RedbOperationalPorts,
    epoch: &SourceUsageEpoch<'_>,
    lineage: ChangelogLineageV3,
    intent: &NativeSourceStepIntent,
    key: &[u8],
    value: &[u8],
    after: bool,
) -> Result<u64, StorageError> {
    let budget = actual.shared.database.source_working_budget();
    let root = decode_projection_source_root_admitted_v1(value, budget)
        .map_err(decode_error)?
        .into_parts()
        .0;
    check_source_root_key(key, root.reference().identity(), root.reference().locator())
        .map_err(|_| integrity())?;
    if after && root.reference().locator() != intent.locator() {
        return Err(integrity());
    }
    let object_key = source_object_key(root.reference().locator());
    let stored;
    let object_value = if after {
        match postimage(intent, N::ProjectionSourceObjects, &object_key) {
            Some(Some(value)) => value,
            Some(None) => return Err(integrity()),
            None => {
                stored = point(
                    actual,
                    epoch,
                    lineage,
                    N::ProjectionSourceObjects,
                    &object_key,
                )?
                .ok_or_else(integrity)?;
                stored.value()
            }
        }
    } else {
        stored = point(
            actual,
            epoch,
            lineage,
            N::ProjectionSourceObjects,
            &object_key,
        )?
        .ok_or_else(integrity)?;
        stored.value()
    };
    let object = decode_projection_source_object_admitted_v1(object_value, budget)
        .map_err(decode_error)?
        .into_parts()
        .0;
    if !root.matches_published_object(&object) {
        return Err(integrity());
    }
    add(
        add(kv(key, value)?, kv(&object_key, object_value)?)?,
        object.descriptor().canonical_row_bytes(),
    )
}
fn allocation_candidate(
    actual: &RedbOperationalPorts,
    epoch: &SourceUsageEpoch<'_>,
    intent: &NativeSourceStepIntent,
    expected_descriptor: &riffdb_storage_api::ProjectionSourceDescriptorV1,
) -> Result<CandidateUsage, StorageError> {
    let locator = intent.locator();
    let object_key = source_object_key(locator);
    let object_value = postimage(intent, N::ProjectionSourceObjects, &object_key)
        .flatten()
        .ok_or_else(integrity)?;
    let budget = actual.shared.database.source_working_budget();
    let object = decode_projection_source_object_admitted_v1(object_value, budget)
        .map_err(decode_error)?
        .into_parts()
        .0;
    if object.lifecycle() != Life::Copying
        || object.descriptor() != expected_descriptor
        || object.descriptor().reference().locator() != locator
    {
        return Err(integrity());
    }
    let maintenance_key = source_maintenance_key(locator);
    let maintenance = postimage(intent, N::ProjectionSourceMaintenance, &maintenance_key)
        .flatten()
        .ok_or_else(integrity)?;
    let allocator_key = N::ProjectionSourceAllocator
        .metadata_key()
        .ok_or_else(integrity)?
        .as_bytes();
    let allocator_value = postimage(intent, N::ProjectionSourceAllocator, allocator_key)
        .flatten()
        .ok_or_else(integrity)?;
    // The actual allocator postimage is independently canonical/semantic checked
    // by the native intent producer; this owner accounts its exact complete KV.
    let allocator = kv(allocator_key, allocator_value)?;
    let object_bytes = kv(&object_key, object_value)?;
    let future_root = add(
        source_root_key_len(object.descriptor().reference().identity()) as u64,
        projection_source_root_future_encoded_bound_v1(object.descriptor().reference())
            .map_err(crate::error::codec_error)?
            .get() as u64,
    )?;
    let maximum_maintenance = add(
        SOURCE_LOCATOR_KEY_BYTES as u64,
        riffdb_storage_api::MAX_PROJECTION_SOURCE_CONTROL_BYTES as u64,
    )?;
    let full = add(
        add(
            add(
                add(object.descriptor().canonical_row_bytes(), object_bytes)?,
                future_root,
            )?,
            maximum_maintenance,
        )?,
        allocator,
    )?;
    let actual_owned = add(
        add(object_bytes, kv(&maintenance_key, maintenance)?)?,
        allocator,
    )?;
    if actual_owned > full {
        return Err(limit());
    }
    // Numeric full-object and metadata limits remain the existing public limits.
    if object.descriptor().row_count() > riffdb_storage_api::MAX_PROJECTION_SOURCE_ROWS
        || add(object.descriptor().canonical_row_bytes(), object_bytes)?
            > riffdb_storage_api::MAX_PROJECTION_SOURCE_OBJECT_BYTES
        || add(add(future_root, maximum_maintenance)?, allocator)?
            > 3 * riffdb_storage_api::MAX_PROJECTION_SOURCE_CONTROL_BYTES as u64
    {
        return Err(limit());
    }
    epoch.domain.require_epoch(epoch)?;
    Ok(CandidateUsage {
        locator,
        full,
        actual: actual_owned,
    })
}
fn decode_error(error: riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1) -> StorageError {
    match error {
        riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
            unavailable()
        }
        _ => StorageError::new(StorageErrorKind::CorruptData, None),
    }
}
