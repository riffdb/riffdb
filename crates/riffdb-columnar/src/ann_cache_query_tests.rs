// req: VEC-007, VEC-008, VEC-009, VEC-010, VEC-011
use crate as riffdb_columnar;

use std::collections::BTreeSet;

use rand::{Rng, SeedableRng, rngs::StdRng};
use riffdb_columnar::{
    ColumnPredicate, ColumnarProjectionDefinition, ColumnarSnapshot, LiveRow, NearestCandidate,
    NearestCandidateAdmission, NearestQueryRequest, NearestSearchKind, OrgKey, PrimaryKeyBytes,
    QueryBudget, RegisteredDefinition, nearest_query_snapshot, nearest_query_snapshot_with_cache,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::ContractBundle;
use riffdb_types::{
    CanonicalValue, CanonicalVector, CommitSequence, DistanceMetric, EntityKeyBuilder,
    EntityTypeId, EntityVersion, FieldId, FrontierPosition,
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

struct Admission {
    field: FieldId,
    inspected: usize,
}
impl NearestCandidateAdmission for Admission {
    type Error = std::convert::Infallible;
    fn admit(&mut self, row: NearestCandidate<'_>) -> Result<bool, Self::Error> {
        self.inspected += 1;
        Ok(row.field_value(self.field) == Some(&CanonicalValue::string("public").unwrap()))
    }
}

#[test]
fn cold_exact_warm_reuse_and_recall_at_the_production_bound() {
    let fixture = Fixture::new();
    let org = [1; 16];
    let mut rng = StdRng::seed_from_u64(229);
    let mut snapshot = ColumnarSnapshot::empty();
    insert_random_rows(&mut snapshot, &fixture, org, 0, 500, "public", &mut rng);
    snapshot.visible_frontier = FrontierPosition::AppliedThrough(CommitSequence::new(1).unwrap());
    let cache = crate::NearestGraphCache::new();
    let mut admission = Admission {
        field: fixture.title_field,
        inspected: 0,
    };
    for metric in [
        DistanceMetric::Cosine,
        DistanceMetric::Euclidean,
        DistanceMetric::DotProduct,
    ] {
        let mut request = query(&fixture, org, random_vector(&mut rng), 10, vec![]);
        request.metric = metric;
        crate::nearest::DISTANCE_COUNT.set(0);
        let started = std::time::Instant::now();
        let cold = nearest_query_snapshot_with_cache(
            &fixture.ann,
            &snapshot,
            &request,
            &mut admission,
            Some((&cache, b"source/generation/frontier/model/policy/revision")),
        )
        .unwrap();
        let cold_time = started.elapsed();
        assert_eq!(cold.search_kind, NearestSearchKind::Exact);
        assert_eq!(crate::nearest::DISTANCE_COUNT.get(), 500);
        cache.wait_for_idle();
        for _ in 0..8 {
            request.query_vector = random_vector(&mut rng);
            let exact = nearest_query_snapshot(&fixture.exact, &snapshot, &request).unwrap();
            crate::nearest::DISTANCE_COUNT.set(0);
            let started = std::time::Instant::now();
            let warm = nearest_query_snapshot_with_cache(
                &fixture.ann,
                &snapshot,
                &request,
                &mut admission,
                Some((&cache, b"source/generation/frontier/model/policy/revision")),
            )
            .unwrap();
            let warm_time = started.elapsed();
            let distances = crate::nearest::DISTANCE_COUNT.get();
            assert_eq!(warm.search_kind, NearestSearchKind::Approximate);
            assert_eq!(warm.ann_stats.unwrap().node_count, 500);
            let exact_ids = result_identity_set(&exact);
            assert!(
                exact_ids.intersection(&result_identity_set(&warm)).count() * 10_000
                    / exact_ids.len()
                    >= 9500
            );
            let (builds, build_distances, bytes) = cache.test_stats();
            let retained_allocations = cache.retained_allocation_ledger();
            assert!(
                distances < build_distances,
                "warm queries must avoid construction work"
            );
            eprintln!(
                "ANN 500 rows {metric:?}: cold={cold_time:?}/500 distances warm={warm_time:?}/{distances} distances build={build_distances} distances builds={builds} reserved={bytes} bytes modeled_retained_cache_allocations={retained_allocations} warm_new_cache_allocations=0"
            );
        }
    }
    assert_eq!(
        cache.test_stats().0,
        3,
        "one build per exact metric population"
    );
    assert_eq!(
        admission.inspected,
        3 * 9 * 500,
        "cache hits still admit every candidate"
    );
    let request = query(&fixture, org, random_vector(&mut rng), 10, vec![]);
    let continuation =
        nearest_query_snapshot_with_cache(&fixture.ann, &snapshot, &request, &mut admission, None)
            .unwrap();
    assert_eq!(continuation.search_kind, NearestSearchKind::Exact);
}

#[test]
fn denied_malformed_rows_and_other_orgs_never_enter_reused_graphs() {
    let fixture = Fixture::new();
    let org = [2; 16];
    let mut rng = StdRng::seed_from_u64(229007);
    let mut snapshot = ColumnarSnapshot::empty();
    insert_random_rows(&mut snapshot, &fixture, org, 0, 96, "public", &mut rng);
    let request = query(&fixture, org, random_vector(&mut rng), 10, vec![]);
    let cache = crate::NearestGraphCache::new();
    let mut admission = Admission {
        field: fixture.title_field,
        inspected: 0,
    };
    nearest_query_snapshot_with_cache(
        &fixture.ann,
        &snapshot,
        &request,
        &mut admission,
        Some((&cache, b"scope")),
    )
    .unwrap();
    cache.wait_for_idle();
    let before = nearest_query_snapshot_with_cache(
        &fixture.ann,
        &snapshot,
        &request,
        &mut admission,
        Some((&cache, b"scope")),
    )
    .unwrap();
    insert_random_rows(&mut snapshot, &fixture, org, 96, 100, "secret", &mut rng);
    insert_random_rows(&mut snapshot, &fixture, [3; 16], 0, 500, "public", &mut rng);
    let key = OrgKey::from_value(&CanonicalValue::Uuid(org)).unwrap();
    snapshot
        .delta
        .get_mut(&key)
        .unwrap()
        .get_mut(&primary_key(fixture.entity, org, 96))
        .unwrap()
        .cells[1] = CanonicalValue::Vector(CanonicalVector::new(vec![1.0]).unwrap());
    let after = nearest_query_snapshot_with_cache(
        &fixture.ann,
        &snapshot,
        &request,
        &mut admission,
        Some((&cache, b"scope")),
    )
    .unwrap();
    assert_eq!(after.search_kind, NearestSearchKind::Approximate);
    assert_eq!(before.ann_stats, after.ann_stats);
    assert_eq!(result_identity_set(&before), result_identity_set(&after));
    assert_eq!(cache.test_stats().0, 1);
    assert_eq!(after.scanned_rows, 196);
    // Same context/digest cannot hide a changed admitted row version or vector.
    snapshot
        .delta
        .get_mut(&key)
        .unwrap()
        .get_mut(&primary_key(fixture.entity, org, 0))
        .unwrap()
        .entity_version = EntityVersion::new(999).unwrap();
    let changed = nearest_query_snapshot_with_cache(
        &fixture.ann,
        &snapshot,
        &request,
        &mut admission,
        Some((&cache, b"scope")),
    )
    .unwrap();
    assert_eq!(changed.search_kind, NearestSearchKind::Exact);
    cache.wait_for_idle();
    snapshot
        .delta
        .get_mut(&key)
        .unwrap()
        .get_mut(&primary_key(fixture.entity, org, 0))
        .unwrap()
        .cells[1] = CanonicalValue::Vector(random_vector(&mut rng));
    let changed = nearest_query_snapshot_with_cache(
        &fixture.ann,
        &snapshot,
        &request,
        &mut admission,
        Some((&cache, b"scope")),
    )
    .unwrap();
    assert_eq!(changed.search_kind, NearestSearchKind::Exact);
    cache.wait_for_idle();
    let changed = nearest_query_snapshot_with_cache(
        &fixture.ann,
        &snapshot,
        &request,
        &mut admission,
        Some((&cache, b"new-policy-or-model")),
    )
    .unwrap();
    assert_eq!(changed.search_kind, NearestSearchKind::Exact);
    cache.wait_for_idle();
}

#[test]
fn memory_refusal_keeps_the_exact_result() {
    let fixture = Fixture::new();
    let mut rng = StdRng::seed_from_u64(229064);
    let org = [4; 16];
    let mut snapshot = ColumnarSnapshot::empty();
    insert_random_rows(&mut snapshot, &fixture, org, 0, 500, "public", &mut rng);
    let request = query(&fixture, org, random_vector(&mut rng), 10, vec![]);
    let exact = nearest_query_snapshot(&fixture.exact, &snapshot, &request).unwrap();
    let cache = crate::NearestGraphCache::new();
    cache.refuse_memory();
    let mut admission = Admission {
        field: fixture.title_field,
        inspected: 0,
    };
    let actual = nearest_query_snapshot_with_cache(
        &fixture.ann,
        &snapshot,
        &request,
        &mut admission,
        Some((&cache, b"scope")),
    )
    .unwrap();
    assert_eq!(actual.search_kind, NearestSearchKind::Exact);
    assert_eq!(result_identity_set(&actual), result_identity_set(&exact));
    assert_eq!(cache.test_stats().0, 0);
}
