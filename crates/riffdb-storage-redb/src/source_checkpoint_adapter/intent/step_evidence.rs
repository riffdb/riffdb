//! Fresh, admitted progress from this intent's canonical durable post-image.
//! Preparation grants no applied result; only the actual ExactAfter proof can
//! release this value to the sealed Projection driver.
use super::super::maintenance::NativeSourceCheckpointRequest;
use super::{NativeSourceStepIntent, NativeVerifiedSourceStep};
use crate::{RedbOperationalPorts, source_keys::source_maintenance_key};
use riffdb_projection::source_driver::{
    SourceCheckpointOperationKind as K, SourceNativeStepEvidence,
};
use riffdb_storage_api::proto_codec::{
    ProjectionSourceRecordDecodeErrorV1, decode_projection_source_maintenance_admitted_v1,
};
use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, MAX_PROJECTION_SOURCE_CONTROL_BYTES,
    ProjectionSourceMaintenanceV1, StorageError, StorageErrorKind,
};
use riffdb_types::{ProjectionSourceWorkAllocationV1, ProjectionSourceWorkReservationV1};
use std::mem::size_of;

#[doc(hidden)]
pub struct NativeSourceAppliedStepEvidence {
    operation: K,
    progress: Option<ProjectionSourceWorkAllocationV1<ProjectionSourceMaintenanceV1>>,
    // Last: decoded progress physically dies before its fixed owner allowance.
    _control: ProjectionSourceWorkReservationV1,
}
impl SourceNativeStepEvidence for NativeSourceAppliedStepEvidence {
    fn operation(&self) -> K {
        self.operation
    }
    fn progress(&self) -> Option<&ProjectionSourceMaintenanceV1> {
        self.progress.as_deref()
    }
}

pub(crate) struct NativePreparedStepProgress<'work> {
    actual: &'work RedbOperationalPorts,
    intent: NativeSourceStepIntent,
    progress: Option<ProjectionSourceWorkAllocationV1<ProjectionSourceMaintenanceV1>>,
    control: Option<ProjectionSourceWorkReservationV1>,
}
impl Drop for NativePreparedStepProgress<'_> {
    fn drop(&mut self) {}
}
impl NativePreparedStepProgress<'_> {
    #[cfg(test)]
    pub(crate) fn allocated_progress_for_test(
        &self,
    ) -> Option<&ProjectionSourceWorkAllocationV1<ProjectionSourceMaintenanceV1>> {
        self.progress.as_ref()
    }
    pub(crate) fn confirm(
        mut self,
        verified: &NativeVerifiedSourceStep<'_>,
    ) -> Result<NativeSourceAppliedStepEvidence, StorageError> {
        if !std::ptr::eq(self.actual, verified.actual_ports())
            || !verified.matches_intent(&self.intent)
            || verified.is_exact_before()
            || !matches!(self.intent.operation(), K::Allocate | K::Copy)
        {
            return Err(corrupt());
        }
        Ok(NativeSourceAppliedStepEvidence {
            operation: self.intent.operation(),
            progress: Some(self.progress.take().ok_or_else(corrupt)?),
            _control: self.control.take().ok_or_else(corrupt)?,
        })
    }
}

// A Publish step has no Maintenance post-image. Its fixed result owner is still
// admitted before native submission and only the real ExactAfter can create it.
pub(crate) struct NativePreparedPublicationStep<'work> {
    actual: &'work RedbOperationalPorts,
    intent: NativeSourceStepIntent,
    control: Option<ProjectionSourceWorkReservationV1>,
}
impl Drop for NativePreparedPublicationStep<'_> {
    fn drop(&mut self) {}
}
impl NativePreparedPublicationStep<'_> {
    pub(crate) fn confirm(
        mut self,
        verified: &NativeVerifiedSourceStep<'_>,
    ) -> Result<NativeSourceAppliedStepEvidence, StorageError> {
        if !std::ptr::eq(self.actual, verified.actual_ports())
            || !verified.matches_intent(&self.intent)
            || verified.is_exact_before()
            || self.intent.operation() != K::Publish
        {
            return Err(corrupt());
        }
        Ok(NativeSourceAppliedStepEvidence {
            operation: K::Publish,
            progress: None,
            _control: self.control.take().ok_or_else(corrupt)?,
        })
    }
}
pub(crate) fn prepare_publication_step<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
    request: &NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
    intent: &NativeSourceStepIntent,
) -> Result<NativePreparedPublicationStep<'work>, StorageError>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    let budget = request
        .actual_ports()
        .shared
        .database
        .source_working_budget();
    if !intent.belongs_to(budget)
        || request.operation() != K::Publish
        || intent.operation() != K::Publish
        || request.after_progress().is_some()
        || intent.locator() != request.captured_locator()
    {
        return Err(corrupt());
    }
    let bytes = size_of::<NativePreparedPublicationStep<'work>>()
        .checked_add(size_of::<NativeSourceAppliedStepEvidence>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(corrupt)?;
    let control = budget.reserve(bytes).map_err(|_| unavailable())?;
    Ok(NativePreparedPublicationStep {
        actual: request.actual_ports(),
        intent: intent.clone(),
        control: Some(control),
    })
}

pub(crate) fn prepare_step_progress<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
    request: &NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
    intent: &NativeSourceStepIntent,
) -> Result<NativePreparedStepProgress<'work>, StorageError>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    let budget = request
        .actual_ports()
        .shared
        .database
        .source_working_budget();
    if !intent.belongs_to(budget)
        || !matches!(request.operation(), K::Allocate | K::Copy)
        || intent.operation() != request.operation()
        || intent.locator() != request.captured_locator()
        || intent.predecessor_history().lineage() != request.captured_history().lineage()
    {
        return Err(corrupt());
    }
    let expected = request.after_progress().ok_or_else(corrupt)?;
    let key = source_maintenance_key(request.captured_locator());
    let mut changes = intent
        .receipt()
        .mutations()
        .iter()
        .filter(|change| change.namespace() == N::ProjectionSourceMaintenance);
    let change = changes.next().ok_or_else(corrupt)?;
    if changes.next().is_some() || change.key() != key.as_slice() {
        return Err(corrupt());
    }
    let encoded = change.value().ok_or_else(corrupt)?;
    if encoded.len() > MAX_PROJECTION_SOURCE_CONTROL_BYTES {
        return Err(corrupt());
    }
    let bytes = size_of::<NativePreparedStepProgress<'work>>()
        .checked_add(size_of::<NativeSourceAppliedStepEvidence>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(corrupt)?;
    let control = budget.reserve(bytes).map_err(|_| unavailable())?;
    // Decode the already-admitted canonical bytes into a fresh semantic graph.
    // Cloning the candidate's progress would retain catalog-owned identities and
    // would not transfer their physical ownership to the Source ledger.
    let progress = decode_projection_source_maintenance_admitted_v1(encoded, budget)
        .map_err(codec)?
        .into_parts()
        .0;
    if &*progress != expected || progress.descriptor().reference().locator() != intent.locator() {
        return Err(corrupt());
    }
    Ok(NativePreparedStepProgress {
        actual: request.actual_ports(),
        intent: intent.clone(),
        progress: Some(progress),
        control: Some(control),
    })
}
fn corrupt() -> StorageError {
    crate::error::storage_error(StorageErrorKind::CorruptData)
}
fn unavailable() -> StorageError {
    crate::error::storage_error(StorageErrorKind::Unavailable)
}
fn codec(error: ProjectionSourceRecordDecodeErrorV1) -> StorageError {
    match error {
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => unavailable(),
        ProjectionSourceRecordDecodeErrorV1::Codec(_) => corrupt(),
    }
}
