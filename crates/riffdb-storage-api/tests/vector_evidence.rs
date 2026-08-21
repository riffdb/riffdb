//! Authoritative vector-evidence semantic and durable-codec tests.

use riffdb_storage_api::{
    DurableKeySchemaBindingV1, EntityTarget, ExecutablePlanRef, StorageValueError,
    StoredVectorEmbeddingWriteV1, StoredVectorEvidenceV1, VectorEvidenceIndexEntryV1,
    VectorEvidenceTransitionPlanV1, VectorObservationCountsV1, decode_vector_evidence_index_v1,
    decode_vector_evidence_v1, decode_vector_observation_v1, encode_vector_evidence_index_v1,
    encode_vector_evidence_v1, encode_vector_observation_v1,
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

#[test]
fn partition_index_round_trips_and_proves_primary_classification() {
    let sequence = CommitSequence::new(9).expect("sequence");
    let primary = evidence(sequence, Some(sequence), None).expect("evidence");
    let index = VectorEvidenceIndexEntryV1::from_evidence(&primary).expect("index entry");
    assert!(index.source_stale());
    assert!(index.matches_evidence(&primary));

    let encoded = encode_vector_evidence_index_v1(&index).expect("encode index");
    let decoded = decode_vector_evidence_index_v1(encoded.as_bytes()).expect("decode index");
    assert_eq!(decoded.value(), &index);

    let other = evidence(
        CommitSequence::new(10).expect("later sequence"),
        Some(CommitSequence::new(10).expect("later sequence")),
        Some(StoredVectorEmbeddingWriteV1::new(
            CommitSequence::new(10).expect("later sequence"),
            metadata(),
        )),
    )
    .expect("other evidence");
    assert!(!index.matches_evidence(&other));
}

#[test]
fn canonical_classification_covers_missing_stale_and_current_embeddings() {
    let first = CommitSequence::new(5).expect("sequence");
    let later = CommitSequence::new(8).expect("sequence");

    let missing = evidence(first, Some(first), None).expect("source-only evidence");
    assert!(missing.classification().source_stale());
    assert!(missing.classification().embedding().is_none());

    let stale = evidence(
        later,
        Some(later),
        Some(StoredVectorEmbeddingWriteV1::new(first, metadata())),
    )
    .expect("stale evidence");
    assert!(stale.classification().source_stale());
    assert_eq!(
        stale
            .classification()
            .embedding()
            .expect("embedding")
            .sequence(),
        first
    );

    let current = evidence(
        later,
        Some(later),
        Some(StoredVectorEmbeddingWriteV1::new(later, metadata())),
    )
    .expect("current evidence");
    assert!(!current.classification().source_stale());
    assert_eq!(
        current
            .classification()
            .embedding()
            .expect("embedding")
            .metadata(),
        &metadata()
    );
}

#[test]
fn transition_classification_is_exact_for_create_update_and_delete() {
    let first = CommitSequence::new(5).expect("sequence");
    let later = CommitSequence::new(8).expect("sequence");
    let plan = plan();
    let schema = DurableKeySchemaBindingV1::from_plan(&plan);

    let create = VectorEvidenceTransitionPlanV1::live(
        target(),
        partition(),
        FieldId::new(4).expect("vector field"),
        EntityVersion::first(),
        None,
        true,
        None,
        schema.clone(),
        provenance(),
        plan.clone(),
    )
    .expect("create transition");
    let create_classes = create
        .classification_transition(first)
        .expect("create classification");
    assert!(create_classes.prior().is_none());
    assert!(
        create_classes
            .successor()
            .expect("successor")
            .source_stale()
    );

    let prior = evidence(first, Some(first), None).expect("prior evidence");
    let update = VectorEvidenceTransitionPlanV1::live(
        target(),
        partition(),
        FieldId::new(4).expect("vector field"),
        EntityVersion::new(2).expect("entity version"),
        Some(&prior),
        false,
        Some(metadata()),
        schema,
        provenance(),
        plan.clone(),
    )
    .expect("update transition");
    let update_classes = update
        .classification_transition(later)
        .expect("update classification");
    assert!(update_classes.prior().expect("prior").source_stale());
    assert!(
        !update_classes
            .successor()
            .expect("successor")
            .source_stale()
    );

    let successor = update.materialize(later).expect("materialized update");
    let riffdb_storage_api::VectorEvidenceMutationV1::Put(successor) = successor else {
        panic!("update must materialize a put");
    };
    let delete = VectorEvidenceTransitionPlanV1::delete(&successor, provenance(), plan)
        .expect("delete transition");
    let delete_classes = delete
        .classification_transition(CommitSequence::new(9).expect("sequence"))
        .expect("delete classification");
    assert!(delete_classes.prior().is_some());
    assert!(delete_classes.successor().is_none());
}

#[test]
fn maintained_counts_apply_the_same_transition_classification() {
    let first = CommitSequence::new(5).expect("sequence");
    let later = CommitSequence::new(8).expect("sequence");
    let plan = plan();
    let schema = DurableKeySchemaBindingV1::from_plan(&plan);
    let create = VectorEvidenceTransitionPlanV1::live(
        target(),
        partition(),
        FieldId::new(4).expect("vector field"),
        EntityVersion::first(),
        None,
        true,
        None,
        schema.clone(),
        provenance(),
        plan.clone(),
    )
    .expect("create transition");
    let mut counts = VectorObservationCountsV1::empty(create.observation_target(), first);
    counts
        .apply(
            &create
                .classification_transition(first)
                .expect("create classification"),
            first,
        )
        .expect("create counts");
    assert_eq!(counts.total_entities(), 1);
    assert_eq!(counts.source_stale_entities(), 1);
    assert_eq!(counts.model_count(&metadata()), 0);

    let prior = create.materialize(first).expect("create evidence");
    let riffdb_storage_api::VectorEvidenceMutationV1::Put(prior) = prior else {
        panic!("create must materialize a put");
    };
    let embed = VectorEvidenceTransitionPlanV1::live(
        target(),
        partition(),
        FieldId::new(4).expect("vector field"),
        EntityVersion::new(2).expect("entity version"),
        Some(&prior),
        false,
        Some(metadata()),
        schema,
        provenance(),
        plan.clone(),
    )
    .expect("embedding transition");
    counts
        .apply(
            &embed
                .classification_transition(later)
                .expect("embedding classification"),
            later,
        )
        .expect("embedding counts");
    assert_eq!(counts.total_entities(), 1);
    assert_eq!(counts.source_stale_entities(), 0);
    assert_eq!(counts.model_count(&metadata()), 1);
    let encoded = encode_vector_observation_v1(&counts).expect("encode observation");
    let decoded = decode_vector_observation_v1(encoded.as_bytes()).expect("decode observation");
    assert_eq!(decoded.value(), &counts);

    let embedded = embed.materialize(later).expect("embedded evidence");
    let riffdb_storage_api::VectorEvidenceMutationV1::Put(embedded) = embedded else {
        panic!("embedding transition must materialize a put");
    };
    let delete = VectorEvidenceTransitionPlanV1::delete(&embedded, provenance(), plan)
        .expect("delete transition");
    let delete_sequence = CommitSequence::new(9).expect("sequence");
    counts
        .apply(
            &delete
                .classification_transition(delete_sequence)
                .expect("delete classification"),
            delete_sequence,
        )
        .expect("delete counts");
    assert_eq!(counts.total_entities(), 0);
    assert_eq!(counts.source_stale_entities(), 0);
    assert_eq!(counts.model_count(&metadata()), 0);
}
