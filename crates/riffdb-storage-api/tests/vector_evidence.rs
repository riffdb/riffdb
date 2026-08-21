//! Authoritative vector-evidence semantic and durable-codec tests.

use riffdb_storage_api::{
    DurableKeySchemaBindingV1, EntityTarget, ExecutablePlanRef, StorageValueError,
    StoredVectorEmbeddingWriteV1, StoredVectorEvidenceV1, decode_vector_evidence_v1,
    encode_vector_evidence_v1,
};
use riffdb_types::{
    AggregateTypeId, CommandId, CommitSequence, ContractBundleHash, ContractLineage,
    ContractVersion, EmbeddingMetadata, EntityKeyBuilder, EntityTypeId, EntityVersion, FieldId,
    PartitionKey, PartitionKeyBuilder, PlanHash, ProvenanceId,
};

fn plan() -> ExecutablePlanRef {
    ExecutablePlanRef::new(
        ContractLineage::new("vector-evidence").expect("lineage"),
        ContractVersion::new(3).expect("contract version"),
        ContractBundleHash::from_bytes([0x31; 32]),
        CommandId::first(),
        PlanHash::from_bytes([0x32; 32]),
    )
}

fn target() -> EntityTarget {
    let entity = EntityTypeId::first();
    let mut key = EntityKeyBuilder::new(entity);
    key.push_u64(7).expect("key component");
    EntityTarget::new(entity, key.finish().expect("entity key")).expect("target")
}

fn partition() -> PartitionKey {
    let mut key = PartitionKeyBuilder::new(AggregateTypeId::first());
    key.push_str("org-a").expect("partition component");
    key.finish().expect("partition key")
}

fn metadata() -> EmbeddingMetadata {
    EmbeddingMetadata::new("embedder-a", "2026-08-21").expect("metadata")
}

fn provenance() -> ProvenanceId {
    let mut bytes = [0x41; 16];
    bytes[6] = 0x71;
    bytes[8] = 0x81;
    ProvenanceId::from_bytes(bytes).expect("provenance")
}

fn evidence(
    sequence: CommitSequence,
    source: Option<CommitSequence>,
    embedding: Option<StoredVectorEmbeddingWriteV1>,
) -> Result<StoredVectorEvidenceV1, StorageValueError> {
    let plan = plan();
    StoredVectorEvidenceV1::new(
        target(),
        partition(),
        FieldId::new(4).expect("vector field"),
        EntityVersion::new(sequence.get()).expect("entity version"),
        sequence,
        source,
        embedding,
        DurableKeySchemaBindingV1::from_plan(&plan),
        provenance(),
        plan,
    )
}

#[test]
fn closed_transition_shapes_preserve_exact_evidence() {
    let first = CommitSequence::new(5).expect("sequence");
    let later = CommitSequence::new(8).expect("sequence");

    let source_only = evidence(first, Some(first), None).expect("source-only evidence");
    assert_eq!(source_only.newest_source_write(), Some(first));
    assert!(source_only.embedding_write().is_none());

    let embedding_only = evidence(
        later,
        Some(first),
        Some(StoredVectorEmbeddingWriteV1::new(later, metadata())),
    )
    .expect("embedding-only evidence");
    assert_eq!(embedding_only.newest_source_write(), Some(first));
    assert_eq!(
        embedding_only
            .embedding_write()
            .expect("embedding")
            .metadata(),
        &metadata()
    );

    let both = evidence(
        later,
        Some(later),
        Some(StoredVectorEmbeddingWriteV1::new(later, metadata())),
    )
    .expect("same-transition evidence");
    assert_eq!(both.newest_source_write(), Some(later));
    assert_eq!(both.embedding_write().expect("embedding").sequence(), later);
}

#[test]
fn detached_or_future_revisions_are_rejected() {
    let sequence = CommitSequence::new(5).expect("sequence");
    let older = CommitSequence::new(4).expect("sequence");
    let future = CommitSequence::new(6).expect("sequence");

    assert_eq!(
        evidence(sequence, Some(older), None),
        Err(StorageValueError::InvalidShape)
    );
    assert_eq!(
        evidence(sequence, Some(future), None),
        Err(StorageValueError::InvalidShape)
    );
    assert_eq!(
        evidence(
            sequence,
            Some(sequence),
            Some(StoredVectorEmbeddingWriteV1::new(future, metadata())),
        ),
        Err(StorageValueError::InvalidShape)
    );
}

#[test]
fn schema_binding_must_match_the_command_plan() {
    let sequence = CommitSequence::first();
    let plan = plan();
    let wrong_binding = DurableKeySchemaBindingV1::new(
        plan.contract_lineage().clone(),
        ContractVersion::new(4).expect("other version"),
        plan.contract_bundle_hash(),
    );
    let result = StoredVectorEvidenceV1::new(
        target(),
        partition(),
        FieldId::first(),
        EntityVersion::first(),
        sequence,
        Some(sequence),
        None,
        wrong_binding,
        provenance(),
        plan,
    );
    assert_eq!(result, Err(StorageValueError::IdentityMismatch));
}

#[test]
fn durable_codec_round_trips_without_vector_or_post_image_duplication() {
    let sequence = CommitSequence::new(9).expect("sequence");
    let original = evidence(
        sequence,
        Some(sequence),
        Some(StoredVectorEmbeddingWriteV1::new(sequence, metadata())),
    )
    .expect("evidence");
    let encoded = encode_vector_evidence_v1(&original).expect("encode");
    let decoded = decode_vector_evidence_v1(encoded.as_bytes()).expect("decode");
    assert_eq!(decoded.value(), &original);
    assert_eq!(
        decoded.encoded_content_charge(),
        encoded.encoded_content_charge()
    );
}
