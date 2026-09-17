//! WP-594 acceptance: declared threshold routing, recall, and tenant isolation.

use std::collections::BTreeSet;

use rand::{Rng, SeedableRng, rngs::StdRng};
use riffdb_columnar::{
    ColumnPredicate, ColumnarProjectionDefinition, ColumnarSnapshot, LiveRow, NearestCandidate,
    NearestCandidateAdmission, NearestQueryRequest, NearestSearchKind, OrgKey, PrimaryKeyBytes,
    QueryBudget, RegisteredDefinition, VectorProviderProfileV1, nearest_query_snapshot,
    nearest_query_snapshot_with_admission,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::ContractBundle;
use riffdb_types::{
    CanonicalValue, CanonicalVector, CommitSequence, DistanceMetric, EntityKeyBuilder,
    EntityTypeId, EntityVersion, FieldId, FrontierPosition, ProjectionProviderCapabilitiesV1,
    ProjectionProviderKindV1, ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1,
    ProjectionProviderStaticBoundsV1,
};

const ANN_CONTRACT: &str = r#"
contract AnnDocs version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<64>
    vector_field embedding(12, cosine, (title), staleness_slo 60, ann_threshold 32, recall_target_bps 9500)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id, doc_id)
  }
}
"#;

const EXACT_CONTRACT: &str = r#"
contract AnnDocs version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<64>
    vector_field embedding(12, cosine, (title), staleness_slo 60)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id, doc_id)
  }
}
"#;

struct Fixture {
    ann: RegisteredDefinition,
    exact: RegisteredDefinition,
    entity: EntityTypeId,
    vector_field: FieldId,
    title_field: FieldId,
}

impl Fixture {
    fn new() -> Self {
        let ann_bundle = compile_contract_source(ANN_CONTRACT).expect("ANN contract");
        let exact_bundle = compile_contract_source(EXACT_CONTRACT).expect("exact contract");
        let entity = ann_bundle
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Document")
            .expect("Document")
            .id();
        let exact_entity = exact_bundle
            .schema()
            .entities()
            .iter()
            .find(|candidate| candidate.name() == "Document")
            .expect("exact Document")
            .id();
        assert_eq!(
            entity, exact_entity,
            "ANN metadata must not alter stable IDs"
        );
        let vector_field = field_id(&ann_bundle, "embedding");
        let title_field = field_id(&ann_bundle, "title");
        assert_eq!(vector_field, field_id(&exact_bundle, "embedding"));
        assert_eq!(title_field, field_id(&exact_bundle, "title"));
        Self {
            ann: register(&ann_bundle),
            exact: register(&exact_bundle),
            entity,
            vector_field,
            title_field,
        }
    }
}

fn field_id(bundle: &ContractBundle, name: &str) -> FieldId {
    bundle.schema().entities()[0]
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .expect("field")
        .id()
}

fn register(bundle: &ContractBundle) -> RegisteredDefinition {
    RegisteredDefinition::register(
        ColumnarProjectionDefinition {
            name: "document_vectors".into(),
            entity_name: "Document".into(),
            projected_fields: vec![field_id(bundle, "title"), field_id(bundle, "embedding")],
            org_scope_field: field_id(bundle, "org_id"),
        },
        bundle,
    )
    .expect("register")
}

fn primary_key(entity: EntityTypeId, org: [u8; 16], ordinal: u64) -> PrimaryKeyBytes {
    let mut doc_id = [0_u8; 16];
    doc_id[8..].copy_from_slice(&ordinal.to_be_bytes());
    let mut builder = EntityKeyBuilder::new(entity);
    builder.push_uuid(&org).expect("org");
    builder.push_uuid(&doc_id).expect("document id");
    PrimaryKeyBytes::from_entity_key_bytes(builder.finish().expect("key").as_bytes().to_vec())
}

fn row(title: &str, vector: CanonicalVector, version: u64) -> LiveRow {
    LiveRow {
        entity_version: EntityVersion::new(version).expect("version"),
        cells: vec![
            CanonicalValue::string(title).expect("title"),
            CanonicalValue::Vector(vector),
        ],
    }
}

fn random_vector(rng: &mut StdRng) -> CanonicalVector {
    CanonicalVector::new((0..12).map(|_| rng.gen_range(-1.0_f32..1.0_f32)).collect())
        .expect("bounded finite vector")
}

fn query(
    fixture: &Fixture,
    org: [u8; 16],
    vector: CanonicalVector,
    k: u32,
    predicates: Vec<ColumnPredicate>,
) -> NearestQueryRequest {
    NearestQueryRequest {
        org_scope: CanonicalValue::Uuid(org),
        vector_field: fixture.vector_field,
        query_vector: vector,
        k,
        metric: DistanceMetric::Cosine,
        predicates,
        budget: QueryBudget {
            max_scanned_rows: 10_000,
            ..QueryBudget::default()
        },
    }
}

fn insert_random_rows(
    snapshot: &mut ColumnarSnapshot,
    fixture: &Fixture,
    org: [u8; 16],
    start: u64,
    count: u64,
    title: &str,
    rng: &mut StdRng,
) {
    let org_key = OrgKey::from_value(&CanonicalValue::Uuid(org)).expect("org key");
    let delta = snapshot.delta.entry(org_key).or_default();
    for ordinal in start..start + count {
        delta.insert(
            primary_key(fixture.entity, org, ordinal),
            row(title, random_vector(rng), ordinal + 1),
        );
    }
}

fn result_identity_set(result: &riffdb_columnar::NearestQueryResult) -> BTreeSet<Vec<u8>> {
    result
        .rows
        .iter()
        .map(|row| match row.primary_key.get(1) {
            Some(CanonicalValue::Uuid(value)) => value.to_vec(),
            other => panic!("unexpected document id {other:?}"),
        })
        .collect()
}

#[test]
fn uncached_declared_threshold_queries_remain_exact() {
    let fixture = Fixture::new();
    let org = [1_u8; 16];
    let mut rng = StdRng::seed_from_u64(0x5940_0001);
    let mut snapshot = ColumnarSnapshot::empty();
    insert_random_rows(&mut snapshot, &fixture, org, 0, 32, "public", &mut rng);
    let probe = random_vector(&mut rng);

    let at_threshold = nearest_query_snapshot(
        &fixture.ann,
        &snapshot,
        &query(&fixture, org, probe.clone(), 10, Vec::new()),
    )
    .expect("at threshold");
    assert_eq!(at_threshold.search_kind, NearestSearchKind::Exact);
    assert_eq!(at_threshold.ann_stats, None);

    insert_random_rows(&mut snapshot, &fixture, org, 32, 1, "public", &mut rng);
    let above_threshold = nearest_query_snapshot(
        &fixture.ann,
        &snapshot,
        &query(&fixture, org, probe.clone(), 10, Vec::new()),
    )
    .expect("above threshold");
    assert_eq!(above_threshold.search_kind, NearestSearchKind::Exact);
    assert_eq!(above_threshold.ann_stats, None);

    let public = ColumnPredicate::Eq {
        field: fixture.title_field,
        value: CanonicalValue::string("public").expect("title"),
    };
    let org_key = OrgKey::from_value(&CanonicalValue::Uuid(org)).expect("org key");
    snapshot
        .delta
        .get_mut(&org_key)
        .expect("partition")
        .get_mut(&primary_key(fixture.entity, org, 32))
        .expect("row")
        .cells[0] = CanonicalValue::string("secret").expect("title");
    let filtered = nearest_query_snapshot(
        &fixture.ann,
        &snapshot,
        &query(&fixture, org, probe, 10, vec![public]),
    )
    .expect("filtered to threshold");
    assert_eq!(filtered.search_kind, NearestSearchKind::Exact);
    assert_eq!(filtered.ann_stats, None);
}

#[test]
fn randomized_histories_meet_declared_recall_at_matched_frontiers() {
    let fixture = Fixture::new();
    let org = [2_u8; 16];
    let mut rng = StdRng::seed_from_u64(0x5940_9500);
    let mut snapshot = ColumnarSnapshot::empty();

    for frontier in 1..=8_u64 {
        insert_random_rows(
            &mut snapshot,
            &fixture,
            org,
            (frontier - 1) * 48,
            48,
            "public",
            &mut rng,
        );
        snapshot.visible_frontier =
            FrontierPosition::AppliedThrough(CommitSequence::new(frontier).expect("frontier"));
        for probe in 0..8 {
            let request = query(&fixture, org, random_vector(&mut rng), 20, Vec::new());
            let exact = nearest_query_snapshot(&fixture.exact, &snapshot, &request)
                .expect("exact at matched frontier");
            let approximate = nearest_query_snapshot(&fixture.ann, &snapshot, &request)
                .expect("ANN at matched frontier");
            assert_eq!(approximate.search_kind, NearestSearchKind::Exact);
            let exact_ids = result_identity_set(&exact);
            let approximate_ids = result_identity_set(&approximate);
            let overlap = exact_ids.intersection(&approximate_ids).count();
            let recall_bps = overlap * 10_000 / exact_ids.len();
            assert!(
                recall_bps >= 9_500,
                "frontier {frontier}, probe {probe}: recall {recall_bps}bps"
            );
        }
    }
}

#[test]
fn another_organization_cannot_change_results_or_graph_statistics() {
    let fixture = Fixture::new();
    let org_a = [3_u8; 16];
    let org_b = [4_u8; 16];
    let mut rng = StdRng::seed_from_u64(0x5940_0008);
    let mut snapshot = ColumnarSnapshot::empty();
    insert_random_rows(&mut snapshot, &fixture, org_a, 0, 96, "public", &mut rng);
    let request = query(&fixture, org_a, random_vector(&mut rng), 12, Vec::new());
    let before = nearest_query_snapshot(&fixture.ann, &snapshot, &request).expect("before");

    insert_random_rows(&mut snapshot, &fixture, org_b, 0, 512, "public", &mut rng);
    let after = nearest_query_snapshot(&fixture.ann, &snapshot, &request).expect("after");
    assert_eq!(before.ann_stats, after.ann_stats);
    assert_eq!(before.scanned_rows, after.scanned_rows);
    assert_eq!(result_identity_set(&before), result_identity_set(&after));
    for (left, right) in before.rows.iter().zip(&after.rows) {
        assert_eq!(left.distance.to_bits(), right.distance.to_bits());
    }
}

struct PublicOnly {
    title_field: FieldId,
}

impl NearestCandidateAdmission for PublicOnly {
    type Error = std::convert::Infallible;

    fn admit(&mut self, candidate: NearestCandidate<'_>) -> Result<bool, Self::Error> {
        Ok(candidate.field_value(self.title_field)
            == Some(&CanonicalValue::string("public").expect("title")))
    }
}

#[test]
fn denied_rows_cannot_shape_the_ann_graph_or_result() {
    let fixture = Fixture::new();
    let org = [5_u8; 16];
    let mut rng = StdRng::seed_from_u64(0x5940_0007);
    let mut control = ColumnarSnapshot::empty();
    insert_random_rows(&mut control, &fixture, org, 0, 96, "public", &mut rng);
    let mut with_denied = control.clone();
    insert_random_rows(&mut with_denied, &fixture, org, 96, 256, "secret", &mut rng);
    let request = query(&fixture, org, random_vector(&mut rng), 16, Vec::new());
    let mut control_admission = PublicOnly {
        title_field: fixture.title_field,
    };
    let expected = nearest_query_snapshot_with_admission(
        &fixture.ann,
        &control,
        &request,
        &mut control_admission,
    )
    .expect("control");
    let mut admission = PublicOnly {
        title_field: fixture.title_field,
    };
    let actual =
        nearest_query_snapshot_with_admission(&fixture.ann, &with_denied, &request, &mut admission)
            .expect("denied rows excluded before graph build");

    assert_eq!(expected.ann_stats, actual.ann_stats);
    assert_eq!(result_identity_set(&expected), result_identity_set(&actual));
    for (left, right) in expected.rows.iter().zip(&actual.rows) {
        assert_eq!(left.distance.to_bits(), right.distance.to_bits());
    }
    assert_eq!(actual.ann_stats, None);
    assert_eq!(
        actual.scanned_rows, 352,
        "denied rows still charge scan work"
    );
}

#[test]
fn vector_provider_descriptor_matches_real_exact_and_ann_engine_contracts() {
    let fixture = Fixture::new();
    let bounds = ProjectionProviderStaticBoundsV1 {
        max_candidates: 1_000,
        max_output_rows: 100,
        max_measures: 0,
        max_input_bytes: 16_384,
        max_work_units: 1_000_000,
        max_state_bytes_per_row: 16_384,
        max_diagnostic_bytes: 4_096,
        retained_epochs: 8_192,
        max_catchup_lag: 100,
        max_epoch_lease_steps: 1_000,
    };
    let exact = fixture
        .exact
        .vector_provider_descriptor_v1(
            fixture.vector_field,
            VectorProviderProfileV1::Exact,
            ProjectionProviderPolicyModeV1::BoundedRowAdmission,
            bounds,
        )
        .unwrap();
    assert_eq!(exact.kind(), ProjectionProviderKindV1::Vector);
    assert_eq!(exact.posture(), ProjectionProviderPostureV1::Exact);
    assert!(
        exact
            .capabilities()
            .contains(ProjectionProviderCapabilitiesV1::RANK)
    );
    assert!(
        !exact
            .capabilities()
            .contains(ProjectionProviderCapabilitiesV1::MEASURE)
    );

    let approximate = fixture
        .ann
        .vector_provider_descriptor_v1(
            fixture.vector_field,
            VectorProviderProfileV1::Approximate,
            ProjectionProviderPolicyModeV1::BoundedRowAdmission,
            bounds,
        )
        .unwrap();
    assert_eq!(
        approximate.posture(),
        ProjectionProviderPostureV1::approximate(9_500).unwrap()
    );
    assert!(
        fixture
            .exact
            .vector_provider_descriptor_v1(
                fixture.vector_field,
                VectorProviderProfileV1::Approximate,
                ProjectionProviderPolicyModeV1::BoundedRowAdmission,
                bounds,
            )
            .is_err()
    );
}

// req: OQ-019
#[test]
fn review_null_cells_do_not_match_upper_range_bounds() {
    let fixture = Fixture::new();
    let bundle = compile_contract_source(&EXACT_CONTRACT.replace(
        "field title: string<64>",
        "field title: string<64>\n    field label: optional<string<64>>",
    ))
    .unwrap();
    let label = field_id(&bundle, "label");
    let definition = RegisteredDefinition::register(
        ColumnarProjectionDefinition {
            name: "optional_label_vectors".into(),
            entity_name: "Document".into(),
            projected_fields: vec![label, field_id(&bundle, "embedding")],
            org_scope_field: field_id(&bundle, "org_id"),
        },
        &bundle,
    )
    .unwrap();
    let org = [1; 16];
    let vector = CanonicalVector::new(vec![1.0; 12]).unwrap();
    let mut snapshot = ColumnarSnapshot::empty();
    let mut null_row = row("title", vector.clone(), 1);
    null_row.cells[0] = CanonicalValue::Null;
    snapshot
        .delta
        .entry(OrgKey::from_value(&CanonicalValue::Uuid(org)).unwrap())
        .or_default()
        .insert(primary_key(fixture.entity, org, 1), null_row);
    let mut request = query(
        &fixture,
        org,
        vector,
        1,
        vec![ColumnPredicate::Range {
            field: label,
            low: None,
            high: Some(CanonicalValue::string("z").unwrap()),
        }],
    );
    request.vector_field = field_id(&bundle, "embedding");
    assert!(
        nearest_query_snapshot(&definition, &snapshot, &request)
            .unwrap()
            .rows
            .is_empty()
    );
    request.predicates = vec![ColumnPredicate::Eq {
        field: label,
        value: CanonicalValue::Null,
    }];
    assert_eq!(
        nearest_query_snapshot(&definition, &snapshot, &request)
            .unwrap()
            .rows
            .len(),
        1
    );
}

// req: OQ-019
#[test]
fn review_nearest_refuses_oversized_merge_before_candidate_admission() {
    struct CountAdmissions(usize);
    impl NearestCandidateAdmission for CountAdmissions {
        type Error = std::convert::Infallible;
        fn admit(&mut self, _: NearestCandidate<'_>) -> Result<bool, Self::Error> {
            self.0 += 1;
            Ok(true)
        }
    }
    let fixture = Fixture::new();
    let org = [1; 16];
    let mut rng = StdRng::seed_from_u64(11);
    let mut snapshot = ColumnarSnapshot::empty();
    insert_random_rows(&mut snapshot, &fixture, org, 0, 3, "public", &mut rng);
    let mut request = query(&fixture, org, random_vector(&mut rng), 1, vec![]);
    request.budget.max_scanned_rows = 2;
    let mut admission = CountAdmissions(0);
    assert!(matches!(
        nearest_query_snapshot_with_admission(&fixture.exact, &snapshot, &request, &mut admission),
        Err(riffdb_columnar::NearestQueryAdmissionError::Query(
            riffdb_columnar::QueryError::ScanBudgetExceeded { max: 2 }
        ))
    ));
    assert_eq!(admission.0, 0);
    request.budget.max_scanned_rows = 3;
    assert_eq!(
        nearest_query_snapshot_with_admission(&fixture.exact, &snapshot, &request, &mut admission)
            .unwrap()
            .scanned_rows,
        3
    );
    assert_eq!(admission.0, 3);
}
