// req: REP-007, REP-003
use super::*;
use riffdb_storage_api::StoredVectorEmbeddingWriteV1;
use riffdb_types::{
    CanonicalRecord, CanonicalString, CanonicalVector, EmbeddingMetadata, EntityKeyBuilder,
    EntityVersion,
};

struct Fixture {
    bundle: ValidatedContractBundle,
    plan: ExecutablePlanRef,
    target: EntityTarget,
    partition: PartitionKey,
    provenance: ProvenanceId,
    field: FieldId,
}
impl Fixture {
    fn new() -> Self {
        let source = include_str!("../../../fixtures/vector-exit/documents.riff").replace(
            "field title:",
            "field tag: optional<string<16>>\n    field title:",
        );
        let bundle = ValidatedContractBundle::from_compiler_bundle(
            riffdb_contract_compiler::compile_contract_source(&source).unwrap(),
        )
        .unwrap();
        let entity = &bundle.bundle().schema().entities()[0];
        let mut key = EntityKeyBuilder::new(entity.id());
        key.push_uuid(&[1; 16]).unwrap();
        key.push_uuid(&[2; 16]).unwrap();
        let key = key.finish().unwrap();
        let partition =
            crate::history::derive_historical_partition(bundle.bundle().schema(), entity, &key)
                .unwrap();
        let target = EntityTarget::new(entity.id(), key).unwrap();
        let field = bundle.bundle().schema().vector_production_specs()[0].field();
        let command = &bundle.bundle().commands()[0];
        let plan = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            command.command_id(),
            command.plan_hash(),
        );
        let mut id = [1; 16];
        id[6] = 0x70;
        id[8] = 0x80;
        Self {
            bundle,
            plan,
            target,
            partition,
            provenance: ProvenanceId::from_bytes(id).unwrap(),
            field,
        }
    }
    fn row(
        &self,
        version: u64,
        title: &str,
        tag: &str,
        vector: Option<f32>,
    ) -> StoredEntityRecordV1 {
        let fields = self.bundle.bundle().schema().entities()[0]
            .record()
            .fields()
            .iter()
            .map(|f| {
                (
                    f.id(),
                    match f.name() {
                        "organization_id" => CanonicalValue::Uuid([1; 16]),
                        "document_id" => CanonicalValue::Uuid([2; 16]),
                        "title" => CanonicalValue::String(CanonicalString::new(title).unwrap()),
                        "body" => CanonicalValue::String(CanonicalString::new("body").unwrap()),
                        "tag" => CanonicalValue::String(CanonicalString::new(tag).unwrap()),
                        "embedding" => vector
                            .map(|v| {
                                CanonicalValue::Vector(
                                    CanonicalVector::new(vec![v, 0.0, 0.0, 0.0]).unwrap(),
                                )
                            })
                            .unwrap_or(CanonicalValue::Null),
                        _ => panic!("unknown field"),
                    },
                )
            })
            .collect();
        StoredEntityRecordV1::new(
            self.target.clone(),
            EntityVersion::new(version).unwrap(),
            self.bundle.contract_version(),
            DurableKeySchemaBindingV1::from_plan(&self.plan),
            CanonicalRecord::new(fields).unwrap(),
        )
        .unwrap()
    }
    fn evidence(
        &self,
        version: u64,
        sequence: u64,
        source: u64,
        embedding: Option<(u64, &str)>,
    ) -> StoredVectorEvidenceV1 {
        StoredVectorEvidenceV1::new(
            self.target.clone(),
            self.partition.clone(),
            self.field,
            EntityVersion::new(version).unwrap(),
            CommitSequence::new(sequence).unwrap(),
            Some(CommitSequence::new(source).unwrap()),
            embedding.map(|(sequence, model)| {
                StoredVectorEmbeddingWriteV1::new(
                    CommitSequence::new(sequence).unwrap(),
                    EmbeddingMetadata::new(model, "2026-08-21").unwrap(),
                )
            }),
            DurableKeySchemaBindingV1::from_plan(&self.plan),
            self.provenance,
            self.plan.clone(),
        )
        .unwrap()
    }
    fn context<'a>(
        &'a self,
        sequence: u64,
        prior: Option<&'a StoredEntityRecordV1>,
        next: Option<StoredEntityRecordV1>,
    ) -> CommandPrefixVectorChecksV1<'a> {
        CommandPrefixVectorChecksV1 {
            bundle: &self.bundle,
            plan: &self.plan,
            target: &self.target,
            sequence: CommitSequence::new(sequence).unwrap(),
            provenance: self.provenance,
            partition: &self.partition,
            prior: prior.map(Cow::Borrowed),
            next,
        }
    }
}
fn put(value: StoredVectorEvidenceV1) -> VectorEvidenceMutationV1 {
    VectorEvidenceMutationV1::Put(Box::new(value))
}

#[test]
fn catalog_vector_prefix_derives_create_source_embedding_and_delete_inventory() {
    let f = Fixture::new();
    for embedding in [false, true] {
        let first = f.row(1, "first", "tag", embedding.then_some(1.0));
        let evidence = f.evidence(1, 1, 1, embedding.then_some((1, "embed-v1")));
        let create = f.context(1, None, Some(first.clone()));
        if !embedding {
            assert!(
                create
                    .validate(f.field, None, Some(&put(evidence)))
                    .is_err(),
                "nonoptional vector declaration cannot contain null"
            );
            continue;
        }
        assert!(
            create
                .validate(f.field, None, Some(&put(evidence.clone())))
                .unwrap()
                .is_some()
        );
        assert!(
            create.validate_inventory(f.field, None).is_err(),
            "all evidence omitted"
        );
        let second = f.row(2, "second", "tag", embedding.then_some(1.0));
        let source = f.context(2, Some(&first), Some(second.clone()));
        let source_evidence = f.evidence(2, 2, 2, embedding.then_some((1, "embed-v1")));
        assert!(
            source
                .validate(
                    f.field,
                    Some(&evidence),
                    Some(&put(source_evidence.clone()))
                )
                .unwrap()
                .is_some()
        );
        assert!(source.validate(f.field, Some(&evidence), None).is_err());
        let deleted = f.context(3, Some(&second), None);
        let mutation = VectorEvidenceMutationV1::Delete {
            target: f.target.clone(),
            vector_field: f.field,
        };
        assert!(
            deleted
                .validate(f.field, Some(&source_evidence), Some(&mutation))
                .unwrap()
                .is_some()
        );
        assert!(deleted.validate_inventory(f.field, None).is_err());
    }
    let first = f.row(1, "same", "tag", Some(1.0));
    let prior = f.evidence(1, 1, 1, Some((1, "embed-v1")));
    for changed in [false, true] {
        let next = f.row(2, "same", "tag", Some(if changed { 2.0 } else { 1.0 }));
        let context = f.context(2, Some(&first), Some(next));
        let rewrite = put(f.evidence(2, 2, 1, Some((2, "embed-v1"))));
        assert!(
            context
                .validate(f.field, Some(&prior), Some(&rewrite))
                .unwrap()
                .is_some()
        );
        if changed {
            assert!(context.validate_inventory(f.field, None).is_err());
        }
        let wrong_model = put(f.evidence(2, 2, 1, Some((2, "foreign"))));
        assert!(
            context
                .validate(f.field, Some(&prior), Some(&wrong_model))
                .is_err()
        );
    }
}

#[test]
fn catalog_vector_prefix_preserves_old_stamps_and_unchanged_evidence() {
    let f = Fixture::new();
    // Evidence belongs to version1; unrelated updates already reached version3.
    let prior_entity = f.row(3, "same", "third", Some(1.0));
    let prior = f.evidence(1, 1, 1, Some((1, "embed-v1")));
    let unrelated = f.context(
        4,
        Some(&prior_entity),
        Some(f.row(4, "same", "fourth", Some(1.0))),
    );
    assert!(
        unrelated
            .validate(f.field, Some(&prior), None)
            .unwrap()
            .is_none()
    );
    assert!(unrelated.validate(f.field, None, None).is_err());
    let invented_source = put(f.evidence(4, 4, 4, Some((1, "embed-v1"))));
    assert!(
        unrelated
            .validate(f.field, Some(&prior), Some(&invented_source))
            .is_err()
    );
    let changed = f.context(
        4,
        Some(&prior_entity),
        Some(f.row(4, "changed", "third", Some(1.0))),
    );
    let legitimate = put(f.evidence(4, 4, 4, Some((1, "embed-v1"))));
    assert!(
        changed
            .validate(f.field, Some(&prior), Some(&legitimate))
            .is_ok()
    );
    let rewritten_old_stamp = put(f.evidence(4, 4, 4, Some((2, "embed-v1"))));
    assert!(
        changed
            .validate(f.field, Some(&prior), Some(&rewritten_old_stamp))
            .is_err()
    );
    let foreign_old_model = put(f.evidence(4, 4, 4, Some((1, "foreign"))));
    assert!(
        changed
            .validate(f.field, Some(&prior), Some(&foreign_old_model))
            .is_err()
    );
    let future = f.evidence(5, 5, 5, Some((5, "embed-v1")));
    assert!(
        changed
            .validate(f.field, Some(&future), Some(&legitimate))
            .is_err()
    );
    let empty = f.row(3, "same", "third", None);
    assert!(
        f.context(4, Some(&empty), None)
            .validate(f.field, Some(&prior), None)
            .is_err()
    );
}

#[test]
fn prefix_entity_images_require_exact_fields_types_bounds_and_primary_key_values() {
    let f = Fixture::new();
    let original = f.row(1, "title", "tag", Some(1.0));
    let validate =
        |row: &StoredEntityRecordV1| crate::command_prefix_entity::validate_record(&f.bundle, row);
    validate(&original).unwrap();
    let field = |name| {
        f.bundle.bundle().schema().entities()[0]
            .record()
            .fields()
            .iter()
            .find(|f| f.name() == name)
            .unwrap()
            .id()
    };
    for case in 0..8 {
        let mut fields = original.fields().fields().to_vec();
        match case {
            0 => fields.push((FieldId::new(9999).unwrap(), CanonicalValue::U64(1))),
            1 => fields.retain(|(id, _)| *id != field("tag")),
            _ => {
                let (name, value) = match case {
                    2 => ("tag", CanonicalValue::U64(1)),
                    3 => (
                        "tag",
                        CanonicalValue::String(CanonicalString::new("01234567890123456").unwrap()),
                    ),
                    4 => ("document_id", CanonicalValue::Uuid([9; 16])),
                    5 => ("title", CanonicalValue::Null),
                    6 => (
                        "embedding",
                        CanonicalValue::Vector(CanonicalVector::new(vec![1.0, 2.0, 3.0]).unwrap()),
                    ),
                    7 => ("body", CanonicalValue::Bool(true)),
                    _ => unreachable!(),
                };
                fields
                    .iter_mut()
                    .find(|(id, _)| *id == field(name))
                    .unwrap()
                    .1 = value;
            }
        }
        fields.sort_by_key(|(id, _)| *id);
        let row = StoredEntityRecordV1::new(
            original.target().clone(),
            original.entity_version(),
            f.bundle.contract_version(),
            original.schema_binding().clone(),
            CanonicalRecord::new(fields).unwrap(),
        )
        .unwrap();
        assert!(validate(&row).is_err(), "schema case {case}");
    }
    let mut fields = original.fields().fields().to_vec();
    fields
        .iter_mut()
        .find(|(id, _)| *id == field("tag"))
        .unwrap()
        .1 = CanonicalValue::Null;
    let optional_null = StoredEntityRecordV1::new(
        original.target().clone(),
        original.entity_version(),
        f.bundle.contract_version(),
        original.schema_binding().clone(),
        CanonicalRecord::new(fields).unwrap(),
    )
    .unwrap();
    validate(&optional_null).unwrap();
}
