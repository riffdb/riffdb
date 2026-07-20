use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    CanonicalValue, CommitSequence, ContractLineage, FrontierPosition, ProjectionApplyHash,
    ProjectionApplyKey, ProjectionGeneration, ProjectionId, ProjectionIdentity, ProjectionPlanHash,
    decode_canonical_value, encode_canonical_record, encode_canonical_value,
};

use crate::{
    CheckedProjectionSchema, EncodedPageItem, ProjectionFailureCodeV1, ProjectionFailureV1,
    ProjectionGenerationPosition, ProjectionLifecycleV1, PublishedApplyModeV1,
    StoredProjectionApplyV1, StoredProjectionControlV1, StoredProjectionStateV1,
    StructurallyDecodedProjectionStateV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, canonical_record_from_bytes, decode_message,
    encode_message, fixed, require, storage_result,
};

const STATE: &str = "riffdb.storage.v1.StoredProjectionStateV1";
const APPLY: &str = "riffdb.storage.v1.StoredProjectionApplyV1";
const CONTROL: &str = "riffdb.storage.v1.StoredProjectionControlV1";

fn identity_to_proto(value: &ProjectionIdentity) -> wire::ProjectionIdentityV1 {
    wire::ProjectionIdentityV1 {
        contract_lineage: value.contract_lineage().as_str().to_owned(),
        projection_id: value.projection_id().get(),
        projection_plan_hash: value.plan_hash().as_bytes().to_vec(),
    }
}

fn identity_from_proto(
    value: wire::ProjectionIdentityV1,
) -> Result<ProjectionIdentity, DurableCodecError> {
    Ok(ProjectionIdentity::new(
        ContractLineage::new(value.contract_lineage).map_err(|_| DurableCodecError::corrupt())?,
        ProjectionId::new(value.projection_id).ok_or_else(DurableCodecError::corrupt)?,
        ProjectionPlanHash::from_bytes(fixed(value.projection_plan_hash)?),
    ))
}

fn frontier_to_proto(value: FrontierPosition) -> wire::FrontierPositionV1 {
    use wire::frontier_position_v1::Position;
    let position = match value {
        FrontierPosition::BeforeFirst => Position::BeforeFirst(wire::UnitV1 {}),
        FrontierPosition::AppliedThrough(sequence) => Position::AppliedThrough(sequence.get()),
    };
    wire::FrontierPositionV1 {
        position: Some(position),
    }
}

fn frontier_from_proto(
    value: wire::FrontierPositionV1,
) -> Result<FrontierPosition, DurableCodecError> {
    use wire::frontier_position_v1::Position;
    match require(value.position)? {
        Position::BeforeFirst(_) => Ok(FrontierPosition::BeforeFirst),
        Position::AppliedThrough(value) => CommitSequence::new(value)
            .map(FrontierPosition::AppliedThrough)
            .ok_or_else(DurableCodecError::corrupt),
    }
}

fn position_to_proto(value: ProjectionGenerationPosition) -> wire::ProjectionGenerationPositionV1 {
    wire::ProjectionGenerationPositionV1 {
        generation: value.generation().get(),
        frontier: Some(frontier_to_proto(value.frontier())),
    }
}

fn position_from_proto(
    value: wire::ProjectionGenerationPositionV1,
) -> Result<ProjectionGenerationPosition, DurableCodecError> {
    Ok(ProjectionGenerationPosition::new(
        ProjectionGeneration::new(value.generation).ok_or_else(DurableCodecError::corrupt)?,
        frontier_from_proto(require(value.frontier)?)?,
    ))
}

fn failure_to_proto(value: &ProjectionFailureV1) -> wire::ProjectionFailureV1 {
    wire::ProjectionFailureV1 {
        generation: value.generation().get(),
        code: i32::from(value.code().tag()),
        at_sequence: value.at_sequence().map(CommitSequence::get),
    }
}

fn failure_from_proto(
    value: wire::ProjectionFailureV1,
) -> Result<ProjectionFailureV1, DurableCodecError> {
    Ok(ProjectionFailureV1::new(
        ProjectionGeneration::new(value.generation).ok_or_else(DurableCodecError::corrupt)?,
        u8::try_from(value.code)
            .ok()
            .and_then(ProjectionFailureCodeV1::from_tag)
            .ok_or_else(DurableCodecError::corrupt)?,
        value
            .at_sequence
            .map(|value| CommitSequence::new(value).ok_or_else(DurableCodecError::corrupt))
            .transpose()?,
    ))
}

fn state_to_proto(
    value: &StoredProjectionStateV1,
) -> Result<wire::StoredProjectionStateV1, DurableCodecError> {
    Ok(wire::StoredProjectionStateV1 {
        identity: Some(identity_to_proto(value.identity())),
        generation: value.generation().get(),
        canonical_group_values: value
            .group_values()
            .iter()
            .map(|value| encode_canonical_value(value).map_err(|_| DurableCodecError::invariant()))
            .collect::<Result<Vec<_>, _>>()?,
        canonical_measures: encode_canonical_record(value.measures())
            .map_err(|_| DurableCodecError::invariant())?,
        last_changed_sequence: value.last_changed_sequence().get(),
    })
}

fn structural_state_from_proto(
    value: wire::StoredProjectionStateV1,
) -> Result<StructurallyDecodedProjectionStateV1, DurableCodecError> {
    let group_values = value
        .canonical_group_values
        .into_iter()
        .map(|bytes| {
            let decoded =
                decode_canonical_value(&bytes).map_err(|_| DurableCodecError::corrupt())?;
            if encode_canonical_value(&decoded).map_err(|_| DurableCodecError::corrupt())? != bytes
            {
                return Err(DurableCodecError::corrupt());
            }
            Ok(decoded)
        })
        .collect::<Result<Vec<CanonicalValue>, _>>()?;
    storage_result(StructurallyDecodedProjectionStateV1::from_stored_parts(
        identity_from_proto(require(value.identity)?)?,
        ProjectionGeneration::new(value.generation).ok_or_else(DurableCodecError::corrupt)?,
        group_values,
        canonical_record_from_bytes(&value.canonical_measures)?,
        CommitSequence::new(value.last_changed_sequence).ok_or_else(DurableCodecError::corrupt)?,
    ))
}

/// Encodes one schema-checked projection group-state row.
pub fn encode_projection_state_v1(
    value: &StoredProjectionStateV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(STATE, &state_to_proto(value)?)
}

/// Structurally decodes one projection row without claiming schema validity.
pub fn decode_projection_state_structural_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StructurallyDecodedProjectionStateV1>, DurableCodecError> {
    decode_message::<wire::StoredProjectionStateV1, _, _>(
        STATE,
        encoded,
        structural_state_from_proto,
    )
}

/// Decodes and validates one projection row against its exact compiled schema.
pub fn decode_projection_state_v1(
    encoded: &[u8],
    schema: &CheckedProjectionSchema,
) -> Result<EncodedPageItem<StoredProjectionStateV1>, DurableCodecError> {
    let (structural, charge) = decode_projection_state_structural_v1(encoded)?.into_parts();
    let checked = structural
        .into_checked(schema)
        .map_err(DurableCodecError::from_storage_value)?;
    Ok(EncodedPageItem::new(checked, charge))
}

/// Encodes one projection application marker.
pub fn encode_projection_apply_v1(
    value: &StoredProjectionApplyV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        APPLY,
        &wire::StoredProjectionApplyV1 {
            identity: Some(identity_to_proto(value.key().identity())),
            generation: value.key().generation().get(),
            commit_sequence: value.key().commit_sequence().get(),
            projection_apply_hash: value.canonical_hash().as_bytes().to_vec(),
        },
    )
}

/// Decodes one projection application marker.
pub fn decode_projection_apply_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredProjectionApplyV1>, DurableCodecError> {
    decode_message::<wire::StoredProjectionApplyV1, _, _>(APPLY, encoded, |value| {
        Ok(StoredProjectionApplyV1::new(
            ProjectionApplyKey::new(
                identity_from_proto(require(value.identity)?)?,
                ProjectionGeneration::new(value.generation)
                    .ok_or_else(DurableCodecError::corrupt)?,
                CommitSequence::new(value.commit_sequence)
                    .ok_or_else(DurableCodecError::corrupt)?,
            ),
            ProjectionApplyHash::from_bytes(fixed(value.projection_apply_hash)?),
        ))
    })
}

/// Encodes one projection lifecycle and frontier control record.
pub fn encode_projection_control_v1(
    value: &StoredProjectionControlV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        CONTROL,
        &wire::StoredProjectionControlV1 {
            identity: Some(identity_to_proto(value.identity())),
            highest_allocated_generation: value.highest_allocated_generation().get(),
            published: value.published().map(position_to_proto),
            candidate: value.candidate().map(position_to_proto),
            published_apply_mode: value
                .published_apply_mode()
                .map(|value| i32::from(value.tag())),
            lifecycle: i32::from(value.lifecycle().tag()),
            failure: value.failure().map(failure_to_proto),
        },
    )
}

/// Decodes one projection lifecycle and frontier control record.
pub fn decode_projection_control_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredProjectionControlV1>, DurableCodecError> {
    decode_message::<wire::StoredProjectionControlV1, _, _>(CONTROL, encoded, |value| {
        storage_result(StoredProjectionControlV1::new(
            identity_from_proto(require(value.identity)?)?,
            ProjectionGeneration::new(value.highest_allocated_generation)
                .ok_or_else(DurableCodecError::corrupt)?,
            value.published.map(position_from_proto).transpose()?,
            value.candidate.map(position_from_proto).transpose()?,
            value
                .published_apply_mode
                .map(|value| {
                    u8::try_from(value)
                        .ok()
                        .and_then(PublishedApplyModeV1::from_tag)
                        .ok_or_else(DurableCodecError::corrupt)
                })
                .transpose()?,
            u8::try_from(value.lifecycle)
                .ok()
                .and_then(ProjectionLifecycleV1::from_tag)
                .ok_or_else(DurableCodecError::corrupt)?,
            value.failure.map(failure_from_proto).transpose()?,
        ))
    })
}
