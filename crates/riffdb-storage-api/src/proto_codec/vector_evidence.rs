//! Durable codec for authoritative vector evidence.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    CommitSequence, ContractLineage, EmbeddingMetadata, EntityTypeId, EntityVersion, FieldId,
    PartitionKey, ProvenanceId,
};

use crate::{
    EncodedPageItem, StoredVectorEmbeddingWriteV1, StoredVectorEvidenceV1,
    VectorObservationCountsV1, VectorObservationTargetV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, binding_from_proto, binding_to_proto,
    decode_message, encode_message, entity_target_from_proto, entity_target_to_proto, fixed,
    plan_from_proto, plan_to_proto, require, storage_result,
};

pub(super) const VECTOR_EVIDENCE: &str = "riffdb.storage.v1.StoredVectorEvidenceV1";
pub(super) const VECTOR_OBSERVATION: &str = "riffdb.storage.v1.StoredVectorObservationV1";

fn embedding_to_proto(value: &StoredVectorEmbeddingWriteV1) -> wire::StoredVectorEmbeddingWriteV1 {
    wire::StoredVectorEmbeddingWriteV1 {
        commit_sequence: value.sequence().get(),
        model_identity: value.metadata().model_identity().to_owned(),
        model_version: value.metadata().model_version().to_owned(),
    }
}

fn embedding_from_proto(
    value: wire::StoredVectorEmbeddingWriteV1,
) -> Result<StoredVectorEmbeddingWriteV1, DurableCodecError> {
    Ok(StoredVectorEmbeddingWriteV1::new(
        CommitSequence::new(value.commit_sequence).ok_or_else(DurableCodecError::corrupt)?,
        EmbeddingMetadata::new(value.model_identity, value.model_version)
            .ok_or_else(DurableCodecError::corrupt)?,
    ))
}

fn evidence_to_proto(value: &StoredVectorEvidenceV1) -> wire::StoredVectorEvidenceV1 {
    wire::StoredVectorEvidenceV1 {
        target: Some(entity_target_to_proto(value.target())),
        partition_key: value.partition_key().as_bytes().to_vec(),
        vector_field_id: value.vector_field().get(),
        entity_version: value.entity_version().get(),
        evidence_sequence: value.evidence_sequence().get(),
        newest_source_write_sequence: value.newest_source_write().map(CommitSequence::get),
        embedding_write: value.embedding_write().map(embedding_to_proto),
        schema_binding: Some(binding_to_proto(value.schema_binding())),
        provenance_id: value.provenance_id().as_bytes().to_vec(),
        plan: Some(plan_to_proto(value.plan())),
    }
}

fn evidence_from_proto(
    value: wire::StoredVectorEvidenceV1,
) -> Result<StoredVectorEvidenceV1, DurableCodecError> {
    storage_result(StoredVectorEvidenceV1::new(
        entity_target_from_proto(require(value.target)?)?,
        PartitionKey::from_bytes(value.partition_key).map_err(|_| DurableCodecError::corrupt())?,
        FieldId::new(value.vector_field_id).ok_or_else(DurableCodecError::corrupt)?,
        EntityVersion::new(value.entity_version).ok_or_else(DurableCodecError::corrupt)?,
        CommitSequence::new(value.evidence_sequence).ok_or_else(DurableCodecError::corrupt)?,
        value
            .newest_source_write_sequence
            .map(|sequence| CommitSequence::new(sequence).ok_or_else(DurableCodecError::corrupt))
            .transpose()?,
        value
            .embedding_write
            .map(embedding_from_proto)
            .transpose()?,
        binding_from_proto(require(value.schema_binding)?)?,
        ProvenanceId::from_bytes(fixed(value.provenance_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        plan_from_proto(require(value.plan)?)?,
    ))
}

/// Encodes one authoritative vector-evidence side record.
pub fn encode_vector_evidence_v1(
    value: &StoredVectorEvidenceV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(VECTOR_EVIDENCE, &evidence_to_proto(value))
}

/// Decodes and semantically validates one authoritative vector-evidence record.
pub fn decode_vector_evidence_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredVectorEvidenceV1>, DurableCodecError> {
    decode_message::<wire::StoredVectorEvidenceV1, _, _>(
        VECTOR_EVIDENCE,
        encoded,
        evidence_from_proto,
    )
}

fn observation_to_proto(value: &VectorObservationCountsV1) -> wire::StoredVectorObservationV1 {
    wire::StoredVectorObservationV1 {
        contract_lineage: value.target().lineage().as_str().to_owned(),
        partition_key: value.target().partition_key().as_bytes().to_vec(),
        entity_type_id: value.target().entity_type().get(),
        vector_field_id: value.target().vector_field().get(),
        total_entities: value.total_entities(),
        source_stale_entities: value.source_stale_entities(),
        model_counts: value
            .model_counts()
            .map(|(metadata, entity_count)| wire::StoredVectorModelCountV1 {
                model_identity: metadata.model_identity().to_owned(),
                model_version: metadata.model_version().to_owned(),
                entity_count,
            })
            .collect(),
        revision_sequence: value.revision().get(),
    }
}

fn observation_from_proto(
    value: wire::StoredVectorObservationV1,
) -> Result<VectorObservationCountsV1, DurableCodecError> {
    let target = VectorObservationTargetV1::new(
        ContractLineage::new(value.contract_lineage).map_err(|_| DurableCodecError::corrupt())?,
        PartitionKey::from_bytes(value.partition_key).map_err(|_| DurableCodecError::corrupt())?,
        EntityTypeId::new(value.entity_type_id).ok_or_else(DurableCodecError::corrupt)?,
        FieldId::new(value.vector_field_id).ok_or_else(DurableCodecError::corrupt)?,
    );
    let model_counts = value
        .model_counts
        .into_iter()
        .map(|model| {
            Ok((
                EmbeddingMetadata::new(model.model_identity, model.model_version)
                    .ok_or_else(DurableCodecError::corrupt)?,
                model.entity_count,
            ))
        })
        .collect::<Result<Vec<_>, DurableCodecError>>()?;
    storage_result(VectorObservationCountsV1::from_parts(
        target,
        value.total_entities,
        value.source_stale_entities,
        model_counts,
        CommitSequence::new(value.revision_sequence).ok_or_else(DurableCodecError::corrupt)?,
    ))
}

/// Encodes one authoritative maintained vector-observation row.
pub fn encode_vector_observation_v1(
    value: &VectorObservationCountsV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(VECTOR_OBSERVATION, &observation_to_proto(value))
}

/// Decodes and validates one authoritative maintained vector-observation row.
pub fn decode_vector_observation_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<VectorObservationCountsV1>, DurableCodecError> {
    decode_message::<wire::StoredVectorObservationV1, _, _>(
        VECTOR_OBSERVATION,
        encoded,
        observation_from_proto,
    )
}
