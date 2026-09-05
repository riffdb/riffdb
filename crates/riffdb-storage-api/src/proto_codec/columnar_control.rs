//! Durable codec for the schema-bound common columnar control.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    ColumnarProjectionSourceV1, ColumnarProjectionSpecHashV1, CommitSequence, ContractLineage,
    DefinitionFingerprint, EntityTypeId, FieldId, FrontierPosition, ProjectionGeneration,
    VectorProjectionSourceV1,
};

use crate::{
    ColumnarProjectionArtifactV1, ColumnarProjectionFailureReasonV1,
    ColumnarProjectionFailureTargetV1, ColumnarProjectionGenerationRoleV1,
    ColumnarProjectionLayoutV1, ColumnarProjectionLifecycleV1, ColumnarProjectionReplayLimitsV1,
    EncodedPageItem, StoredColumnarProjectionControlV1, StoredColumnarProjectionFailureV1,
    StoredColumnarProjectionGenerationV1,
};

use super::{CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message, fixed};

const COLUMNAR_PROJECTION_CONTROL: &str = "riffdb.storage.v1.StoredColumnarProjectionControlV1";

fn source_to_proto(value: &ColumnarProjectionSourceV1) -> wire::StoredColumnarProjectionSourceV1 {
    use wire::stored_columnar_projection_source_v1::Source;

    let source = match value {
        ColumnarProjectionSourceV1::Scalar {
            lineage,
            definition_fingerprint,
        } => Source::Scalar(wire::StoredColumnarScalarSourceV1 {
            contract_lineage: lineage.as_str().to_owned(),
            definition_fingerprint: definition_fingerprint.as_bytes().to_vec(),
        }),
        ColumnarProjectionSourceV1::Vector(source) => {
            Source::Vector(wire::StoredColumnarVectorSourceV1 {
                contract_lineage: source.lineage().as_str().to_owned(),
                entity_type_id: source.entity_type().get(),
                vector_field_id: source.vector_field().get(),
            })
        }
    };
    wire::StoredColumnarProjectionSourceV1 {
        source: Some(source),
    }
}

fn source_from_proto(
    value: wire::StoredColumnarProjectionSourceV1,
) -> Result<ColumnarProjectionSourceV1, DurableCodecError> {
    use wire::stored_columnar_projection_source_v1::Source;

    match value.source.ok_or_else(DurableCodecError::corrupt)? {
        Source::Scalar(source) => Ok(ColumnarProjectionSourceV1::scalar(
            ContractLineage::new(source.contract_lineage)
                .map_err(|_| DurableCodecError::corrupt())?,
            DefinitionFingerprint::from_bytes(fixed(source.definition_fingerprint)?),
        )),
        Source::Vector(source) => Ok(ColumnarProjectionSourceV1::vector(
            VectorProjectionSourceV1::new(
                ContractLineage::new(source.contract_lineage)
                    .map_err(|_| DurableCodecError::corrupt())?,
                EntityTypeId::new(source.entity_type_id).ok_or_else(DurableCodecError::corrupt)?,
                FieldId::new(source.vector_field_id).ok_or_else(DurableCodecError::corrupt)?,
            ),
        )),
    }
}

fn frontier_to_proto(value: FrontierPosition) -> wire::StoredColumnarProjectionFrontierV1 {
    wire::StoredColumnarProjectionFrontierV1 {
        applied_through: match value {
            FrontierPosition::BeforeFirst => None,
            FrontierPosition::AppliedThrough(sequence) => Some(sequence.get()),
        },
    }
}

fn frontier_from_proto(
    value: wire::StoredColumnarProjectionFrontierV1,
) -> Result<FrontierPosition, DurableCodecError> {
    value
        .applied_through
        .map_or(Ok(FrontierPosition::BeforeFirst), |sequence| {
            CommitSequence::new(sequence)
                .map(FrontierPosition::AppliedThrough)
                .ok_or_else(DurableCodecError::corrupt)
        })
}

fn generation_to_proto(
    value: &StoredColumnarProjectionGenerationV1,
) -> wire::StoredColumnarProjectionGenerationV1 {
    wire::StoredColumnarProjectionGenerationV1 {
        generation: value.generation().get(),
        layout: i32::from(value.layout().tag()),
        frontier: Some(frontier_to_proto(value.frontier())),
        history_incarnation: value.history_incarnation(),
        artifact_length: value.artifact().map(ColumnarProjectionArtifactV1::length),
        checksum_bytes: value
            .artifact()
            .map(|artifact| artifact.checksum().to_vec()),
        definition_fingerprint: value.definition_fingerprint().as_bytes().to_vec(),
        spec_hash: value.spec_hash().as_bytes().to_vec(),
        physical_generation_fingerprint: value
            .physical_generation_fingerprint()
            .map(|fingerprint| fingerprint.to_vec()),
        snapshot_frontier: value.snapshot_frontier().map(frontier_to_proto),
        role: i32::from(value.role().tag()),
    }
}

fn generation_from_proto(
    value: wire::StoredColumnarProjectionGenerationV1,
) -> Result<StoredColumnarProjectionGenerationV1, DurableCodecError> {
    let artifact = match (value.artifact_length, value.checksum_bytes) {
        (None, None) => None,
        (Some(length), Some(checksum)) => Some(
            ColumnarProjectionArtifactV1::new(length, fixed(checksum)?)
                .ok_or_else(DurableCodecError::corrupt)?,
        ),
        _ => return Err(DurableCodecError::corrupt()),
    };
    StoredColumnarProjectionGenerationV1::new(
        ProjectionGeneration::new(value.generation).ok_or_else(DurableCodecError::corrupt)?,
        u8::try_from(value.layout)
            .ok()
            .and_then(ColumnarProjectionLayoutV1::from_tag)
            .ok_or_else(DurableCodecError::corrupt)?,
        frontier_from_proto(value.frontier.ok_or_else(DurableCodecError::corrupt)?)?,
        value.history_incarnation,
        artifact,
        DefinitionFingerprint::from_bytes(fixed(value.definition_fingerprint)?),
        ColumnarProjectionSpecHashV1::from_bytes(fixed(value.spec_hash)?),
        value
            .physical_generation_fingerprint
            .map(fixed)
            .transpose()?,
        value
            .snapshot_frontier
            .map(frontier_from_proto)
            .transpose()?,
        u8::try_from(value.role)
            .ok()
            .and_then(ColumnarProjectionGenerationRoleV1::from_tag)
            .ok_or_else(DurableCodecError::corrupt)?,
    )
    .map_err(|_| DurableCodecError::corrupt())
}

fn failure_to_proto(
    value: StoredColumnarProjectionFailureV1,
) -> wire::StoredColumnarProjectionFailureV1 {
    wire::StoredColumnarProjectionFailureV1 {
        target: i32::from(value.target().tag()),
        reason: i32::from(value.reason().tag()),
        generation: value.generation().map(ProjectionGeneration::get),
    }
}

fn failure_from_proto(
    value: wire::StoredColumnarProjectionFailureV1,
) -> Result<StoredColumnarProjectionFailureV1, DurableCodecError> {
    StoredColumnarProjectionFailureV1::new(
        u8::try_from(value.target)
            .ok()
            .and_then(ColumnarProjectionFailureTargetV1::from_tag)
            .ok_or_else(DurableCodecError::corrupt)?,
        u8::try_from(value.reason)
            .ok()
            .and_then(ColumnarProjectionFailureReasonV1::from_tag)
            .ok_or_else(DurableCodecError::corrupt)?,
        value
            .generation
            .map(|generation| {
                ProjectionGeneration::new(generation).ok_or_else(DurableCodecError::corrupt)
            })
            .transpose()?,
    )
    .map_err(|_| DurableCodecError::corrupt())
}

fn control_to_proto(
    value: &StoredColumnarProjectionControlV1,
) -> wire::StoredColumnarProjectionControlV1 {
    wire::StoredColumnarProjectionControlV1 {
        source: Some(source_to_proto(value.source())),
        target_definition_fingerprint: value.target_definition_fingerprint().as_bytes().to_vec(),
        target_spec_hash: value.target_spec_hash().as_bytes().to_vec(),
        highest_generation: value.highest_generation().get(),
        published: value.published().map(generation_to_proto),
        candidate: value.candidate().map(generation_to_proto),
        predecessor: value.predecessor().map(generation_to_proto),
        lifecycle: i32::from(value.lifecycle().tag()),
        failure: value.failure().map(failure_to_proto),
        replay_age_seconds: value.replay_limits().age_seconds(),
        replay_bytes: value.replay_limits().bytes(),
        replay_backlog: value.replay_limits().backlog(),
    }
}

fn control_from_proto(
    value: wire::StoredColumnarProjectionControlV1,
) -> Result<StoredColumnarProjectionControlV1, DurableCodecError> {
    StoredColumnarProjectionControlV1::new(
        source_from_proto(value.source.ok_or_else(DurableCodecError::corrupt)?)?,
        DefinitionFingerprint::from_bytes(fixed(value.target_definition_fingerprint)?),
        ColumnarProjectionSpecHashV1::from_bytes(fixed(value.target_spec_hash)?),
        ProjectionGeneration::new(value.highest_generation)
            .ok_or_else(DurableCodecError::corrupt)?,
        value.published.map(generation_from_proto).transpose()?,
        value.candidate.map(generation_from_proto).transpose()?,
        value.predecessor.map(generation_from_proto).transpose()?,
        u8::try_from(value.lifecycle)
            .ok()
            .and_then(ColumnarProjectionLifecycleV1::from_tag)
            .ok_or_else(DurableCodecError::corrupt)?,
        value.failure.map(failure_from_proto).transpose()?,
        ColumnarProjectionReplayLimitsV1::new(
            value.replay_age_seconds,
            value.replay_bytes,
            value.replay_backlog,
        )
        .ok_or_else(DurableCodecError::corrupt)?,
    )
    .map_err(|_| DurableCodecError::corrupt())
}

/// Encodes one canonical schema-bound columnar control record.
pub fn encode_columnar_projection_control_v1(
    value: &StoredColumnarProjectionControlV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(COLUMNAR_PROJECTION_CONTROL, &control_to_proto(value))
}

/// Decodes and semantically validates one schema-bound columnar control record.
pub fn decode_columnar_projection_control_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredColumnarProjectionControlV1>, DurableCodecError> {
    decode_message::<wire::StoredColumnarProjectionControlV1, _, _>(
        COLUMNAR_PROJECTION_CONTROL,
        encoded,
        control_from_proto,
    )
}
