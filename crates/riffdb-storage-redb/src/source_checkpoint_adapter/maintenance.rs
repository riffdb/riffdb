//! Actual native instruction context for the closed quota/input producer.
use super::*;
use riffdb_projection::source_driver::{
    SourceCheckpointInstruction, SourceCheckpointOperationKind,
};
use riffdb_storage_api::ChangelogHistoryStateV3;
use riffdb_types::ProjectionSourceLocatorV1;

// The only constructor is checked_request in this actual native owner below.
// Raw structural values, equal durable IDs and caller DTOs cannot create this.
pub(crate) struct NativeSourceCheckpointRequest<
    'request,
    'candidate,
    'stage,
    'slot,
    'session,
    'work,
> where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    instruction: &'request SourceCheckpointInstruction<
        'candidate,
        'stage,
        'slot,
        'session,
        NativeSourceCheckpointBackend<'work>,
    >,
    brand: &'request NativeSourceSessionBrand<'work>,
}
impl<'request, 'candidate, 'stage, 'slot, 'session, 'work>
    NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    pub(crate) fn operation(&self) -> SourceCheckpointOperationKind {
        self.instruction.operation()
    }
    pub(crate) fn captured_locator(&self) -> ProjectionSourceLocatorV1 {
        self.instruction.captured_locator()
    }
    pub(crate) fn captured_history(&self) -> ChangelogHistoryStateV3 {
        self.instruction.captured_history()
    }
    pub(crate) fn captured_catalog_pointer(&self) -> &riffdb_storage_api::ActiveCatalogPointerV1 {
        self.instruction.catalog_pointer()
    }
    pub(crate) fn projection_identity(&self) -> &riffdb_types::ProjectionIdentity {
        self.brand.plan.identity()
    }
    pub(crate) fn captured_native_origin(
        &self,
    ) -> Option<riffdb_storage_api::NativeCatalogOriginReferenceV1> {
        self.brand.prefix.captured_native_origin()
    }
    pub(crate) fn descriptor(&self) -> Option<&riffdb_storage_api::ProjectionSourceDescriptorV1> {
        self.instruction.descriptor()
    }
    pub(crate) fn expected_row_count(&self) -> u32 {
        self.instruction.expected_row_count()
    }
    pub(crate) fn expected_maximum_native_key_bytes(&self) -> usize {
        self.instruction.expected_maximum_native_key_bytes()
    }
    pub(crate) fn expected_canonical_row_bytes(&self) -> u64 {
        self.instruction.expected_canonical_row_bytes()
    }
    pub(crate) fn before_progress(
        &self,
    ) -> Option<&riffdb_storage_api::ProjectionSourceMaintenanceV1> {
        self.instruction.before_progress()
    }
    pub(crate) fn after_progress(
        &self,
    ) -> Option<&riffdb_storage_api::ProjectionSourceMaintenanceV1> {
        self.instruction.after_progress()
    }
    pub(crate) fn ordinal_assignment(
        &self,
    ) -> Option<riffdb_storage_api::ProjectionCheckpointOrdinalAllocationV1> {
        self.instruction.ordinal_assignment()
    }
    pub(crate) fn transaction_binding_assignment(
        &self,
    ) -> Option<riffdb_storage_api::AuthoritativeTransactionBindingV3> {
        self.instruction.transaction_binding_assignment()
    }
    pub(crate) fn native_preparation(&self) -> Option<&<NativeSourceCheckpointBackend<'work> as SourceCheckpointDriverBackend>::PreparationEvidence>{
        self.instruction.native_preparation()
    }
    pub(crate) fn copy_batch(
        &self,
    ) -> Option<&<NativeSourceCheckpointBackend<'work> as SourceCheckpointDriverBackend>::CopyBatch>
    {
        self.instruction.copy_batch()
    }
    pub(crate) fn prepared_step(
        &self,
    ) -> Option<
        &<NativeSourceCheckpointBackend<'work> as SourceCheckpointDriverBackend>::PreparedStep,
    > {
        self.instruction.prepared_step()
    }
    pub(super) fn frozen_stage(&self) -> &super::stage::NativeSourceFrozen<'stage, 'slot, 'work> {
        self.instruction.native_stage()
    }
    // Compact captured input custody only. No plan/EngineOwner/raw pin escapes.
    // The usage-domain factory consumes this real token before physical apply.
    pub(crate) fn retain_original_inputs(
        &self,
    ) -> Result<crate::store::RetainedOriginalSourceInputs, riffdb_storage_api::StorageError> {
        self.brand.prefix.retain_original_source_inputs()
    }
    pub(crate) fn actual_ports(&self) -> &'work RedbOperationalPorts {
        self.brand.ports
    }
}
pub(super) fn checked_request<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
    actual: &'request NativeSourceSessionBrand<'work>,
    instruction: &'request SourceCheckpointInstruction<
        'candidate,
        'stage,
        'slot,
        'session,
        NativeSourceCheckpointBackend<'work>,
    >,
) -> Result<
    NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
    SourceDriverError,
>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    instruction.check_native_custody(actual, instruction.native_stage().brand)?;
    if !actual.prefix.belongs_to_ports(actual.ports)
        || instruction.captured_history() != actual.prefix.captured_history()
        || Some(instruction.captured_locator()) != actual.prefix.tentative_source_locator()
        || instruction.catalog_pointer() != actual.plan.catalog_pointer()
    {
        return Err(SourceDriverError::ForeignCustody);
    }
    Ok(NativeSourceCheckpointRequest {
        instruction,
        brand: actual,
    })
}
impl<'session, 'work> NativeSourceMaintenancePort<'session, 'work> {
    pub(super) fn checked_request<'request, 'candidate, 'stage, 'slot>(
        &self,
        instruction: &'request SourceCheckpointInstruction<
            'candidate,
            'stage,
            'slot,
            'session,
            NativeSourceCheckpointBackend<'work>,
        >,
    ) -> Result<
        NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
        SourceDriverError,
    >
    where
        'work: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        checked_request(self.brand, instruction)
    }
}
impl Drop for NativeSourceCheckpointRequest<'_, '_, '_, '_, '_, '_> {
    fn drop(&mut self) {}
}
