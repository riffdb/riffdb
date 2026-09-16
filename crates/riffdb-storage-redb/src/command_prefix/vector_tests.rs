//! Intrinsic graph checks; catalog and predecessor arithmetic have separate owners.
// req: REP-007, REP-003, REC-001

use super::*;
use riffdb_storage_api::*;
use riffdb_types::{
    CanonicalRecord, CanonicalVector, CommitSequence, EmbeddingMetadata, EntityVersion, FieldId,
};

fn fixture(embedding: bool, delete: bool) -> StoredCommandCapsuleV2 {
    let wire =
        include_str!("../../../../fixtures/proto/durable-command-prefix-v7-wire-vectors.txt");
    let hex = wire
        .lines()
        .find_map(|line| line.strip_prefix("riffdb.storage.v1.StoredCommandSegmentV6\t"))
        .unwrap();
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect::<Vec<_>>();
    let decoded = decode_command_segment_v1(&bytes).unwrap();
    let command = &decoded.value().commands()[0];
    let commit = command.base().commit();
    let target = commit.entity_references()[0].target().clone();
    let field = FieldId::new(123).unwrap();
    let version = EntityVersion::new(1).unwrap();
    let binding = DurableKeySchemaBindingV1::from_plan(commit.plan());
    let metadata = EmbeddingMetadata::new("model", "version").unwrap();
    let evidence = StoredVectorEvidenceV1::new(
        target.clone(),
        command.base().outcome().partition_key().clone(),
        field,
        version,
        command.commit_sequence(),
        Some(command.commit_sequence()),
        embedding.then(|| {
            StoredVectorEmbeddingWriteV1::new(command.commit_sequence(), metadata.clone())
        }),
        binding.clone(),
        commit.provenance_id(),
        commit.plan().clone(),
    )
    .unwrap();
    let entity = StoredEntityRecordV1::new(
        target.clone(),
        version,
        binding.contract_version(),
        binding,
        CanonicalRecord::new(vec![(
            field,
            if embedding {
                CanonicalValue::Vector(CanonicalVector::new(vec![1.0, 2.0]).unwrap())
            } else {
                CanonicalValue::Null
            },
        )])
        .unwrap(),
    )
    .unwrap();
    let index = VectorEvidenceIndexEntryV1::from_evidence(&evidence).unwrap();
    let observation = VectorObservationCountsV1::from_parts(
        index.target().clone(),
        1,
        u64::from(!embedding),
        if embedding {
            vec![(metadata, 1)]
        } else {
            vec![]
        },
        command.commit_sequence(),
    )
    .unwrap();
    let health = VectorHealthObservationV1::from_parts(
        index.target().lineage().clone(),
        vec![
            VectorHealthFieldObservationV1::from_parts(
                target.entity_type_id(),
                field,
                10,
                u64::from(!delete),
                0,
            )
            .unwrap(),
        ],
        command.commit_sequence(),
    )
    .unwrap();
    let values = [
        (
            N::Entities,
            target.key().as_bytes().to_vec(),
            encode_entity_record_v1(&entity).unwrap(),
        ),
        (
            N::VectorEvidence,
            keys::encode_vector_evidence_key(target.key(), field).unwrap(),
            encode_vector_evidence_v1(&evidence).unwrap(),
        ),
        (
            N::VectorEvidenceIndex,
            keys::encode_vector_evidence_index_key(index.target(), target.key()).unwrap(),
            encode_vector_evidence_index_v1(&index).unwrap(),
        ),
        (
            N::VectorObservations,
            keys::encode_vector_observation_key(index.target()).unwrap(),
            encode_vector_observation_v1(&observation).unwrap(),
        ),
    ];
    let mut rows = values
        .into_iter()
        .map(|(namespace, key, value)| {
            if delete {
                AuthoritativeMutationV3::delete(namespace, &key, [1; 32]).unwrap()
            } else {
                AuthoritativeMutationV3::put(namespace, &key, None, value.as_bytes()).unwrap()
            }
        })
        .collect::<Vec<_>>();
    rows.push(
        AuthoritativeMutationV3::put(
            N::VectorObservations,
            &keys::encode_vector_health_observation_key(health.lineage()).unwrap(),
            delete.then_some([1; 32]),
            encode_vector_health_observation_v1(&health)
                .unwrap()
                .as_bytes(),
        )
        .unwrap(),
    );
    with_rows(command, rows)
}

fn with_rows(
    command: &StoredCommandCapsuleV2,
    mut rows: Vec<AuthoritativeMutationV3>,
) -> StoredCommandCapsuleV2 {
    rows.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
    let prefix = command.prefix_evidence().unwrap();
    StoredCommandCapsuleV2::from_base_with_entity_transitions(
        command.base().clone(),
        command.index_generation_transitions().to_vec(),
        command.entity_transitions().to_vec(),
    )
    .unwrap()
    .with_prefix_evidence(
        CommandPrefixEvidenceV1::new(prefix.predecessor(), prefix.covered(), rows).unwrap(),
    )
    .unwrap()
}

#[test]
fn vector_prefix_requires_all_reciprocal_rows_for_live_and_deleted_entities() {
    for (embedding, delete) in [(false, false), (true, false), (true, true)] {
        let command = fixture(embedding, delete);
        validate_post_images(&command).unwrap();
        let rows = command.prefix_evidence().unwrap().mutations();
        for offset in 0..rows.len() {
            let mut omitted = rows.to_vec();
            omitted.remove(offset);
            assert!(
                validate_post_images(&with_rows(&command, omitted)).is_err(),
                "omitted {offset}, delete {delete}"
            );
        }
        // No interpretation of a typed but unrelated envelope is allowed.
        for offset in 0..rows.len() {
            let mut wrong = rows.to_vec();
            let row = &wrong[offset];
            wrong[offset] = AuthoritativeMutationV3::put(
                row.namespace(),
                row.key(),
                row.expected_hash(),
                encode_vector_health_observation_v1(&VectorHealthObservationV1::empty(
                    command.base().commit().plan().contract_lineage().clone(),
                    command.commit_sequence(),
                ))
                .unwrap()
                .as_bytes(),
            )
            .unwrap();
            assert!(
                validate_post_images(&with_rows(&command, wrong)).is_err(),
                "wrong envelope/empty health {offset}"
            );
        }
    }
}

#[test]
fn vector_prefix_refuses_foreign_revisions_metadata_and_entity_images() {
    let command = fixture(true, false);
    for case in 0..8 {
        let mut rows = command.prefix_evidence().unwrap().mutations().to_vec();
        let namespace = match case {
            0 | 1 => N::VectorEvidence,
            2 => N::VectorEvidenceIndex,
            3 | 4 => N::VectorObservations,
            _ => N::Entities,
        };
        let row = rows
            .iter_mut()
            .find(|row| {
                row.namespace() == namespace
                    && (case != 3 || keys::decode_vector_observation_key(row.key()).is_ok())
                    && (case != 4 || keys::decode_vector_health_observation_key(row.key()).is_ok())
            })
            .unwrap();
        let bytes = row.value().unwrap();
        let future = CommitSequence::new(2).unwrap();
        let encoded = match case {
            0 | 1 => {
                let decoded = decode_vector_evidence_v1(bytes).unwrap();
                let old = decoded.value();
                encode_vector_evidence_v1(
                    &StoredVectorEvidenceV1::new(
                        old.target().clone(),
                        old.partition_key().clone(),
                        old.vector_field(),
                        if case == 0 {
                            EntityVersion::new(2).unwrap()
                        } else {
                            old.entity_version()
                        },
                        if case == 1 {
                            future
                        } else {
                            old.evidence_sequence()
                        },
                        Some(if case == 1 {
                            future
                        } else {
                            old.evidence_sequence()
                        }),
                        old.embedding_write().cloned(),
                        old.schema_binding().clone(),
                        old.provenance_id(),
                        old.plan().clone(),
                    )
                    .unwrap(),
                )
                .unwrap()
            }
            2 => {
                let decoded = decode_vector_evidence_index_v1(bytes).unwrap();
                let old = decoded.value();
                encode_vector_evidence_index_v1(
                    &VectorEvidenceIndexEntryV1::from_parts(
                        old.target().clone(),
                        old.entity_key().clone(),
                        old.evidence_sequence(),
                        old.newest_source_write(),
                        Some(StoredVectorEmbeddingWriteV1::new(
                            old.evidence_sequence(),
                            EmbeddingMetadata::new("different", "model").unwrap(),
                        )),
                    )
                    .unwrap(),
                )
                .unwrap()
            }
            3 => {
                let decoded = decode_vector_observation_v1(bytes).unwrap();
                let old = decoded.value();
                encode_vector_observation_v1(
                    &VectorObservationCountsV1::from_parts(
                        old.target().clone(),
                        old.total_entities(),
                        old.source_stale_entities(),
                        old.model_counts()
                            .map(|(model, count)| (model.clone(), count))
                            .collect(),
                        future,
                    )
                    .unwrap(),
                )
                .unwrap()
            }
            4 => {
                let decoded = decode_vector_health_observation_v1(bytes).unwrap();
                let old = decoded.value();
                encode_vector_health_observation_v1(
                    &VectorHealthObservationV1::from_parts(
                        old.lineage().clone(),
                        old.fields().cloned().collect(),
                        future,
                    )
                    .unwrap(),
                )
                .unwrap()
            }
            _ => {
                let decoded = decode_entity_record_v1(bytes).unwrap();
                let old = decoded.value();
                let fields = match case {
                    5 => vec![],
                    6 => vec![(FieldId::new(123).unwrap(), CanonicalValue::Null)],
                    _ => vec![(FieldId::new(123).unwrap(), CanonicalValue::U64(9))],
                };
                encode_entity_record_v1(
                    &StoredEntityRecordV1::new(
                        old.target().clone(),
                        old.entity_version(),
                        old.written_by_contract(),
                        old.schema_binding().clone(),
                        CanonicalRecord::new(fields).unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap()
            }
        };
        *row = AuthoritativeMutationV3::put(
            row.namespace(),
            row.key(),
            row.expected_hash(),
            encoded.as_bytes(),
        )
        .unwrap();
        assert!(
            validate_post_images(&with_rows(&command, rows)).is_err(),
            "case {case}"
        );
    }
}
