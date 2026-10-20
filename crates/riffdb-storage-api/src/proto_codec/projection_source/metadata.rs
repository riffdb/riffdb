//! Closed source metadata mappings; structural records never grant authority.
use super::*;

pub(super) fn object_to_wire(
    value: &ProjectionSourceObjectV1,
) -> wire::StoredProjectionSourceObjectV1 {
    wire::StoredProjectionSourceObjectV1 {
        descriptor: Some(descriptor_to_wire(value.descriptor())),
        lifecycle: u32::from(value.lifecycle().tag()),
    }
}
pub(super) fn object_from_wire(
    value: wire::StoredProjectionSourceObjectV1,
) -> Result<ProjectionSourceObjectV1, DurableCodecError> {
    Ok(ProjectionSourceObjectV1::new(
        descriptor_from_wire(require(value.descriptor)?)?,
        u8::try_from(value.lifecycle)
            .ok()
            .and_then(ProjectionSourceLifecycleV1::from_tag)
            .ok_or_else(DurableCodecError::corrupt)?,
    ))
}
/// Caller admits bounded metadata/wire/output capacities before construction.
pub fn encode_projection_source_object_v1(
    value: &ProjectionSourceObjectV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_control(OBJECT, &object_to_wire(value))
}
/// Decode lifecycle plus immutable descriptor, verifying its repeated digest.
pub fn decode_projection_source_object_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionSourceObjectV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    decode_admitted::<wire::StoredProjectionSourceObjectV1, _>(encoded, budget, object_from_wire)
}

pub(super) fn root_to_wire(value: &ProjectionSourceRootV1) -> wire::StoredProjectionSourceRootV1 {
    wire::StoredProjectionSourceRootV1 {
        source: Some(reference_to_wire(value.reference())),
        publication_binding: Some(publication_to_wire(value.publication_binding())),
        administration_sequence: value
            .administration_sequence()
            .map(AdministrationSequence::get),
    }
}
pub(super) fn root_from_wire(
    value: wire::StoredProjectionSourceRootV1,
) -> Result<ProjectionSourceRootV1, DurableCodecError> {
    storage_result(ProjectionSourceRootV1::new(
        reference_from_wire(require(value.source)?)?,
        publication_from_wire(require(value.publication_binding)?)?,
        value
            .administration_sequence
            .map(|value| AdministrationSequence::new(value).ok_or_else(DurableCodecError::corrupt))
            .transpose()?,
    ))
}
/// Encodes a nonrecursive assigned publication binding; not its own frame hash.
pub fn encode_projection_source_root_v1(
    value: &ProjectionSourceRootV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_control(ROOT, &root_to_wire(value))
}
/// Retains ordinary exact locator and publication-frontier structural checks.
pub fn decode_projection_source_root_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionSourceRootV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    decode_admitted::<wire::StoredProjectionSourceRootV1, _>(encoded, budget, root_from_wire)
}

pub(super) fn maintenance_to_wire(
    value: &ProjectionSourceMaintenanceV1,
) -> wire::StoredProjectionSourceMaintenanceV1 {
    wire::StoredProjectionSourceMaintenanceV1 {
        descriptor: Some(descriptor_to_wire(value.descriptor())),
        expected_prior_root: value.expected_prior_root().map(root_to_wire),
        copied_rows: value.copied_rows(),
        copied_canonical_row_bytes: value.copied_canonical_row_bytes(),
        last_copied_key: value.last_copied_key().map(<[u8]>::to_vec),
        rolling_hash: Some(digest_to_wire(value.rolling_hash())),
    }
}
pub(super) fn maintenance_from_wire(
    value: wire::StoredProjectionSourceMaintenanceV1,
) -> Result<ProjectionSourceMaintenanceV1, DurableCodecError> {
    storage_result(ProjectionSourceMaintenanceV1::from_stored_parts(
        descriptor_from_wire(require(value.descriptor)?)?,
        value.expected_prior_root.map(root_from_wire).transpose()?,
        value.copied_rows,
        value.copied_canonical_row_bytes,
        value.last_copied_key,
        digest_from_wire(
            require(value.rolling_hash)?,
            HashDomain::ProjectionSourceCopyProgress,
        )?,
    ))
}
/// Complete control envelope is bounded to4KiB, including last key and metadata.
pub fn encode_projection_source_maintenance_v1(
    value: &ProjectionSourceMaintenanceV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_control(MAINTENANCE, &maintenance_to_wire(value))
}
/// Copies remain hidden maintenance state. Hashes are not publication authority.
pub fn decode_projection_source_maintenance_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionSourceMaintenanceV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    decode_admitted::<wire::StoredProjectionSourceMaintenanceV1, _>(
        encoded,
        budget,
        maintenance_from_wire,
    )
}

fn base_to_wire(value: &GenerationSourceBaseV1) -> wire::StoredGenerationSourceBaseV1 {
    wire::StoredGenerationSourceBaseV1 {
        source: Some(reference_to_wire(value.reference())),
        generation: value.generation().get(),
        applied_through: value.applied_through().get(),
    }
}
fn base_from_wire(
    value: wire::StoredGenerationSourceBaseV1,
) -> Result<GenerationSourceBaseV1, DurableCodecError> {
    Ok(GenerationSourceBaseV1::new(
        ProjectionGeneration::new(value.generation).ok_or_else(DurableCodecError::corrupt)?,
        reference_from_wire(require(value.source)?)?,
        CommitSequence::new(value.applied_through).ok_or_else(DurableCodecError::corrupt)?,
    ))
}
/// Encodes local generation metadata; callers cannot classify it authoritative.
pub fn encode_generation_source_base_v1(
    value: &GenerationSourceBaseV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_control(BASE, &base_to_wire(value))
}
/// Native lifetime custody must still validate this exact source plus full tail.
pub fn decode_generation_source_base_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<GenerationSourceBaseV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    decode_admitted::<wire::StoredGenerationSourceBaseV1, _>(encoded, budget, base_from_wire)
}

pub(in crate::proto_codec) fn allocator_to_wire(
    value: ProjectionCheckpointOrdinalAllocatorV1,
) -> wire::StoredProjectionCheckpointOrdinalAllocatorV1 {
    use wire::stored_projection_checkpoint_ordinal_allocator_v1::Position;
    wire::StoredProjectionCheckpointOrdinalAllocatorV1 {
        allocation_history_incarnation: value.allocation_history_incarnation(),
        position: Some(match value.position() {
            ProjectionCheckpointOrdinalPositionV1::Next(ordinal) => {
                Position::NextOrdinal(ordinal.get())
            }
            ProjectionCheckpointOrdinalPositionV1::Exhausted => {
                Position::Exhausted(wire::UnitV1 {})
            }
        }),
    }
}
pub(in crate::proto_codec) fn allocator_from_wire(
    value: wire::StoredProjectionCheckpointOrdinalAllocatorV1,
) -> Result<ProjectionCheckpointOrdinalAllocatorV1, DurableCodecError> {
    use wire::stored_projection_checkpoint_ordinal_allocator_v1::Position;
    let position = match require(value.position)? {
        Position::NextOrdinal(ordinal) => ProjectionCheckpointOrdinalPositionV1::Next(
            ProjectionCheckpointOrdinal::new(ordinal).ok_or_else(DurableCodecError::corrupt)?,
        ),
        Position::Exhausted(_) => ProjectionCheckpointOrdinalPositionV1::Exhausted,
    };
    storage_result(ProjectionCheckpointOrdinalAllocatorV1::new(
        value.allocation_history_incarnation,
        position,
    ))
}
/// Encodes the permanent next-or-exhausted state, never allocation authority.
pub fn encode_projection_checkpoint_ordinal_allocator_v1(
    value: &ProjectionCheckpointOrdinalAllocatorV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_control(ALLOCATOR, &allocator_to_wire(*value))
}
/// Refuses zero incarnation/ordinal and missing or unknown position.
pub fn decode_projection_checkpoint_ordinal_allocator_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<ProjectionCheckpointOrdinalAllocatorV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    decode_admitted::<wire::StoredProjectionCheckpointOrdinalAllocatorV1, _>(
        encoded,
        budget,
        allocator_from_wire,
    )
}

/// Strict scalar metadata validation independent of the source-worker ledger.
/// The codec proves canonical structure only; allocation-incarnation custody
/// and the coordinator's exact next-ordinal comparison remain mandatory.
pub fn decode_projection_checkpoint_ordinal_allocator_inline_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<ProjectionCheckpointOrdinalAllocatorV1>, DurableCodecError> {
    let message =
        riffdb_proto::envelope::decode_projection_checkpoint_ordinal_allocator_inline_v1(encoded)
            .map_err(DurableCodecError::from_decode_envelope)?;
    let value = allocator_from_wire(message)?;
    let charge = EncodedContentCharge::new(encoded.len()).ok_or_else(DurableCodecError::corrupt)?;
    Ok(EncodedPageItem::new(value, charge))
}
