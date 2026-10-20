//! A prepared root is a proposal. Only actual physical/logical ExactAfter
//! reconciliation can turn this closed owner into native publication evidence.
use super::super::{
    NativeSourcePublicationEvidence, maintenance::NativeSourceCheckpointRequest,
    preparation::CurrentSourceStepRead, publication::NativeSourcePublicationObservation,
};
use super::{NativeSourceStepIntent, NativeVerifiedSourceStep, builder::NativeSourceIntentBuilder};
use crate::{
    RedbOperationalPorts,
    source_keys::{
        encode_source_root_key_into, source_maintenance_key, source_object_key, source_root_key_len,
    },
};
use riffdb_projection::source_driver::{
    SourceCheckpointOperationKind as K, SourceNativePreparedAllocationEvidence,
};
use riffdb_storage_api::proto_codec::{
    ProjectionSourceRecordDecodeErrorV1, encode_projection_source_maintenance_admitted_v1,
    encode_projection_source_object_admitted_v1, encode_projection_source_root_admitted_v1,
};
use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, ProjectionSourceDescriptorPartsV1, ProjectionSourceDescriptorV1,
    ProjectionSourceLifecycleV1 as Life, ProjectionSourceObjectV1, ProjectionSourceRootV1,
    StorageError, StorageErrorKind,
};
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, ProjectionSourceWorkAllocationV1,
    owned_allocation_capacity_bound_v1,
};
use std::mem::size_of;

pub(crate) struct NativeSourcePublicationValues {
    descriptor: ProjectionSourceDescriptorV1,
    root: ProjectionSourceRootV1,
}
riffdb_types::inspect_owned_capacity_struct_v1!(NativeSourcePublicationValues {
    owned: [descriptor, root],
    inline: []
});
impl NativeSourcePublicationValues {
    pub(crate) fn descriptor(&self) -> &ProjectionSourceDescriptorV1 {
        &self.descriptor
    }
    pub(crate) fn root(&self) -> &ProjectionSourceRootV1 {
        &self.root
    }
}

pub(crate) struct NativePreparedPublication<'work> {
    actual: &'work RedbOperationalPorts,
    intent: NativeSourceStepIntent,
    values: Option<ProjectionSourceWorkAllocationV1<NativeSourcePublicationValues>>,
    observation: NativeSourcePublicationObservation<'work>,
    step: Option<super::step_evidence::NativePreparedPublicationStep<'work>>,
}
impl Drop for NativePreparedPublication<'_> {
    fn drop(&mut self) {}
}
impl NativePreparedPublication<'_> {
    pub(crate) fn intent(&self) -> &NativeSourceStepIntent {
        &self.intent
    }
    /// Retains the private complete physical-copy witness through preinstallation.
    /// An ordinary head change may need a new receipt proposal; Source/stage
    /// changes invalidate this witness before quota/input installation.
    pub(crate) fn validate_for<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
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
        self.observation.validate_for(request)?;
        if !std::ptr::eq(self.actual, request.actual_ports())
            || self.intent.locator() != request.captured_locator()
            || self.intent.operation() != K::Publish
        {
            return Err(corrupt());
        }
        Ok(())
    }
    /// Takes only the existing private ExactAfter proof of BOTH physical and
    /// logical publication. This consumes the proposed values, not a caller Ok.
    pub(crate) fn confirm(
        mut self,
        verified: &NativeVerifiedSourceStep<'_>,
    ) -> Result<
        (
            super::step_evidence::NativeSourceAppliedStepEvidence,
            NativeSourcePublicationEvidence,
        ),
        StorageError,
    > {
        if !std::ptr::eq(self.actual, verified.actual_ports())
            || !verified.matches_intent(&self.intent)
            || verified.is_exact_before()
            || verified.intent().operation() != K::Publish
        {
            return Err(corrupt());
        }
        let values = self.values.take().ok_or_else(corrupt)?;
        let step = self.step.take().ok_or_else(corrupt)?.confirm(verified)?;
        Ok((step, NativeSourcePublicationEvidence { values }))
    }
}

pub(crate) fn prepare_publication_intent<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
    request: &NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
    observation: NativeSourcePublicationObservation<'work>,
) -> Result<NativePreparedPublication<'work>, StorageError>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    observation.validate_for(request)?;
    if request.operation() != K::Publish
        || request.after_progress().is_some()
        || request.copy_batch().is_some()
        || request.transaction_binding_assignment().is_some()
    {
        return Err(corrupt());
    }
    let descriptor = request.descriptor().ok_or_else(corrupt)?;
    let before = request.before_progress().ok_or_else(corrupt)?;
    let allocation = request.native_preparation().ok_or_else(corrupt)?;
    if before.descriptor() != descriptor
        || !before.copy_complete()
        || before.expected_prior_root() != allocation.prior_root()
    {
        return Err(corrupt());
    }
    let actual = request.actual_ports();
    let budget = actual.shared.database.source_working_budget();
    let read = CurrentSourceStepRead::capture(request)?;
    read.validate_expected_selection(allocation)?;
    let object_key = source_object_key(request.captured_locator());
    let maintenance_key = source_maintenance_key(request.captured_locator());
    let old_object = read
        .point(N::ProjectionSourceObjects, &object_key)?
        .ok_or_else(corrupt)?;
    let old_maintenance = read
        .point(N::ProjectionSourceMaintenance, &maintenance_key)?
        .ok_or_else(corrupt)?;
    let expected_maintenance =
        encode_projection_source_maintenance_admitted_v1(before, budget).map_err(codec)?;
    if old_maintenance.as_bytes() != expected_maintenance.as_bytes() {
        return Err(stale());
    }
    let key_bytes = source_root_key_len(request.projection_identity());
    if key_bytes > riffdb_types::MAX_KEY_BYTES {
        return Err(limit());
    }
    let key_work = owned_allocation_capacity_bound_v1(key_bytes, 1)
        .map_err(|_| limit())?
        .checked_mul(2)
        .ok_or_else(limit)?;
    let _keys = budget.reserve(key_work).map_err(|_| stale())?;
    let mut root_key = Vec::with_capacity(key_bytes);
    encode_source_root_key_into(
        &mut root_key,
        request.projection_identity(),
        request.captured_locator(),
    )
    .map_err(|_| corrupt())?;
    if read.point(N::ProjectionSourceRoots, &root_key)?.is_some() {
        return Err(stale());
    }
    let clone_bytes = descriptor
        .clone_work_bound_v1()
        .map_err(|_| limit())?
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(descriptor.reference().clone_work_bound_v1().ok()?))
        .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
        .and_then(|bytes| {
            bytes.checked_add(4096 + size_of::<NativeSourcePublicationValues>() as u64)
        })
        .ok_or_else(limit)?;
    let work = budget.reserve(clone_bytes).map_err(|_| stale())?;
    let copying = ProjectionSourceObjectV1::new(descriptor.clone(), Life::Copying);
    let expected_object =
        encode_projection_source_object_admitted_v1(&copying, budget).map_err(codec)?;
    if old_object.as_bytes() != expected_object.as_bytes() {
        return Err(stale());
    }
    let published = copying.transition(Life::Published).map_err(|_| corrupt())?;
    drop(copying);
    drop(expected_object);
    let published_bytes =
        encode_projection_source_object_admitted_v1(&published, budget).map_err(codec)?;
    drop(published);
    let mut builder = NativeSourceIntentBuilder::new(request, read.history())?;
    // This remains only the current proposal. Native preinstall must require
    // Commit's exact binding before these canonical root bytes can be applied.
    // The candidate/catalog identity stays borrowed. Reconstruct a fresh
    // semantic graph before settlement so no provider-owned identity allocation
    // is reassigned or left charged on the catalog after worker destruction.
    let owned_descriptor = fresh_descriptor(descriptor)?;
    let root = ProjectionSourceRootV1::new(
        owned_descriptor.reference().clone(),
        builder.expected_publication_binding(),
        None,
    )
    .map_err(|_| corrupt())?;
    let root_bytes = encode_projection_source_root_admitted_v1(&root, budget).map_err(codec)?;
    let values = work
        .settle_shared_value(NativeSourcePublicationValues {
            descriptor: owned_descriptor,
            root,
        })
        .map_err(|_| stale())?;
    builder.put(
        N::ProjectionSourceObjects,
        &object_key,
        Some(old_object.as_bytes()),
        published_bytes.as_bytes(),
    )?;
    builder.put(
        N::ProjectionSourceRoots,
        &root_key,
        None,
        root_bytes.as_bytes(),
    )?;
    builder.remove(
        N::ProjectionSourceMaintenance,
        &maintenance_key,
        old_maintenance.as_bytes(),
    )?;
    if let Some(prior_root) = allocation.prior_root() {
        let prior_object = allocation.prior_object().ok_or_else(corrupt)?;
        if !prior_root.matches_published_object(prior_object)
            || prior_root.reference().locator() == request.captured_locator()
        {
            return Err(corrupt());
        }
        let mut prior_key = Vec::with_capacity(key_bytes);
        encode_source_root_key_into(
            &mut prior_key,
            request.projection_identity(),
            prior_root.reference().locator(),
        )
        .map_err(|_| corrupt())?;
        let old_root_bytes = allocation.prior_root_bytes().ok_or_else(corrupt)?;
        let old_prior_object_bytes = allocation.prior_object_bytes().ok_or_else(corrupt)?;
        let prior_work = budget
            .reserve(
                prior_object
                    .descriptor()
                    .clone_work_bound_v1()
                    .map_err(|_| limit())?,
            )
            .map_err(|_| stale())?;
        let superseded = prior_object
            .transition(Life::Superseded)
            .map_err(|_| corrupt())?;
        let superseded_bytes =
            encode_projection_source_object_admitted_v1(&superseded, budget).map_err(codec)?;
        drop(superseded);
        drop(prior_work);
        builder.remove(N::ProjectionSourceRoots, &prior_key, old_root_bytes)?;
        builder.put(
            N::ProjectionSourceObjects,
            &source_object_key(prior_root.reference().locator()),
            Some(old_prior_object_bytes),
            superseded_bytes.as_bytes(),
        )?;
    }
    read.finish()?;
    observation.validate_for(request)?;
    let intent = builder.finish()?;
    let step = super::step_evidence::prepare_publication_step(request, &intent)?;
    Ok(NativePreparedPublication {
        actual,
        intent,
        values: Some(values),
        observation,
        step: Some(step),
    })
}
// Caller reserves descriptor.clone_work_bound_v1 (both identity representations,
// canonical constructor scratch and control owners) before this fresh copy.
fn fresh_descriptor(
    value: &ProjectionSourceDescriptorV1,
) -> Result<ProjectionSourceDescriptorV1, StorageError> {
    let identity = value.reference().identity();
    let lineage = riffdb_types::ContractLineage::new(identity.contract_lineage().as_str())
        .map_err(|_| corrupt())?;
    let identity = riffdb_types::ProjectionIdentity::new(
        lineage,
        identity.projection_id(),
        identity.plan_hash(),
    );
    let owned = ProjectionSourceDescriptorV1::new(ProjectionSourceDescriptorPartsV1 {
        identity,
        locator: value.reference().locator(),
        applied_through: value.applied_through(),
        captured_source: value.captured_source(),
        origin: value.reference().origin(),
        prior_source_digest: value.prior_source_digest(),
        row_count: value.row_count(),
        canonical_row_bytes: value.canonical_row_bytes(),
        row_root: value.row_root(),
    })
    .map_err(|_| corrupt())?;
    if &owned != value {
        return Err(corrupt());
    }
    Ok(owned)
}
fn corrupt() -> StorageError {
    crate::error::storage_error(StorageErrorKind::CorruptData)
}
fn stale() -> StorageError {
    crate::error::storage_error(StorageErrorKind::Unavailable)
}
fn limit() -> StorageError {
    crate::error::storage_error(StorageErrorKind::LimitExceeded)
}
fn codec(error: ProjectionSourceRecordDecodeErrorV1) -> StorageError {
    match error {
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => stale(),
        ProjectionSourceRecordDecodeErrorV1::Codec(_) => corrupt(),
    }
}
