//! WP-593 projection semantics: nearest() query syntax, and columnar
//! exact-KNN execution under org isolation, predicate filtering BEFORE
//! ranking (VEC-006/VEC-007), and exact-KNN determinism (WP-594 ground
//! truth).
//!
//! Acceptance command: `cargo test --test projection_semantics`
//!
//! The projection under test is registered over a genuinely vector-typed
//! contract field: `vector_field embedding(3, ...)` compiles through the
//! public front door (the WP-592-era bundle gap is fixed), so no stand-in
//! string column carrying smuggled vector cells is needed — the engine's
//! declared-dimension conformance would now reject exactly that.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_riffql_syntax::parse_query;
use riffdb_types::{
    CanonicalValue, CanonicalVector, CommitSequence, DistanceMetric, EntityVersion, FieldId,
    FrontierPosition,
};

use riffdb_columnar::{
    ColumnPredicate, ColumnarProjectionDefinition, ColumnarSnapshot, LiveRow, NearestQueryRequest,
    OrgKey, PrimaryKeyBytes, QueryBudget, QueryError, RegisteredDefinition, nearest_query_snapshot,
};

#[test]
fn nearest_syntax_parses_and_formats_correctly() {
    let source = r#"query Similar($org: OrgId, $query_vec: Embedding) {
    many results from Document
        where org_id == $org
        nearest(embedding, $query_vec, 10)
    return Found { results: results { doc_id, title } }
    outcomes Found
}"#;
    let document = parse_query(source).expect("parse");
    assert_eq!(document.body.bindings.len(), 1);
    let binding = &document.body.bindings[0];
    let nearest = binding.nearest.as_ref().expect("nearest clause");
    assert_eq!(nearest.field.value.as_str(), "embedding");
    assert_eq!(nearest.vector.value.as_str(), "query_vec");
    assert!(binding.order.is_empty());
    assert!(binding.take.is_none());

    // Round-trip through formatter
    let formatted = riffdb_riffql_syntax::format_query(&document);
    let reparsed = parse_query(&formatted).expect("reparse");
    assert_eq!(
        riffdb_riffql_syntax::format_query(&reparsed),
        formatted,
        "formatter not idempotent"
    );
}

// ─── Columnar execution path ───

/// A real vector-bearing contract: the embedding column in the projection is
/// vector-typed with a declared dimension of 3.
const VECTOR_CONTRACT: &str = r#"
contract Docs version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field body: string<65536>
    vector_field embedding(3, cosine, (title, body), staleness_slo 60)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id, doc_id)
  }
}
"#;

fn vector_bundle() -> riffdb_contract_ir::ContractBundle {
    compile_contract_source(VECTOR_CONTRACT).expect("vector contract compiles")
}

fn field_id(bundle: &riffdb_contract_ir::ContractBundle, entity: &str, name: &str) -> FieldId {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|e| e.name() == entity)
        .expect("entity");
    entity
        .record()
        .fields()
        .iter()
        .find(|f| f.name() == name)
        .map(|f| f.id())
        .unwrap_or_else(|| panic!("field {name} on {}", entity.name()))
}

/// Registers a projection over (title, embedding) with org_id as the scope.
fn document_vector_projection(
    bundle: &riffdb_contract_ir::ContractBundle,
) -> (RegisteredDefinition, riffdb_types::EntityTypeId) {
    let org = field_id(bundle, "Document", "org_id");
    let title = field_id(bundle, "Document", "title");
    let embedding = field_id(bundle, "Document", "embedding");
    let entity_type = bundle
        .schema()
        .entities()
        .iter()
        .find(|e| e.name() == "Document")
        .expect("Document entity")
        .id();
    let def = RegisteredDefinition::register(
        ColumnarProjectionDefinition {
            name: "document_vectors".into(),
            entity_name: "Document".into(),
            projected_fields: vec![title, embedding],
            org_scope_field: org,
        },
        bundle,
    )
    .expect("register projection");
    (def, entity_type)
}

fn build_pk(
    entity_type: riffdb_types::EntityTypeId,
    org: [u8; 16],
    doc_id: u64,
) -> PrimaryKeyBytes {
    let mut key_builder = riffdb_types::EntityKeyBuilder::new(entity_type);
    key_builder.push_uuid(&org).expect("uuid");
    let mut id_bytes = [0u8; 16];
    id_bytes[0..8].copy_from_slice(&doc_id.to_be_bytes());
    key_builder.push_uuid(&id_bytes).expect("doc uuid");
    PrimaryKeyBytes::from_entity_key_bytes(key_builder.finish().expect("key").as_bytes().to_vec())
}

/// Row cells in projection order: (title, embedding).
fn document_row(title: &str, embedding: &[f32]) -> LiveRow {
    LiveRow {
        entity_version: EntityVersion::new(1).expect("v"),
        cells: vec![
            CanonicalValue::string(title).expect("title"),
            CanonicalValue::Vector(CanonicalVector::new(embedding.to_vec()).expect("vec")),
        ],
    }
}

fn request(
    org: [u8; 16],
    vector_field: FieldId,
    query: &[f32],
    k: u32,
    predicates: Vec<ColumnPredicate>,
) -> NearestQueryRequest {
    NearestQueryRequest {
        org_scope: CanonicalValue::Uuid(org),
        vector_field,
        query_vector: CanonicalVector::new(query.to_vec()).expect("query"),
        k,
        metric: DistanceMetric::Cosine,
        predicates,
        budget: QueryBudget::default(),
    }
}

#[test]
fn nearest_query_returns_k_closest_by_cosine() {
    let bundle = vector_bundle();
    let (definition, entity_type) = document_vector_projection(&bundle);
    let vector_field = field_id(&bundle, "Document", "embedding");

    let org = [1u8; 16];
    let org_key = OrgKey::from_value(&CanonicalValue::Uuid(org)).expect("org key");

    // 5 documents at varying angles from query [1, 0, 0]
    let mut delta: BTreeMap<PrimaryKeyBytes, LiveRow> = BTreeMap::new();
    delta.insert(
        build_pk(entity_type, org, 1),
        document_row("a", &[1.0, 0.0, 0.0]),
    ); // identical
    delta.insert(
        build_pk(entity_type, org, 2),
        document_row("b", &[0.7, 0.7, 0.0]),
    ); // ~45 deg
    delta.insert(
        build_pk(entity_type, org, 3),
        document_row("c", &[0.0, 1.0, 0.0]),
    ); // orthogonal
    delta.insert(
        build_pk(entity_type, org, 4),
        document_row("d", &[-1.0, 0.0, 0.0]),
    ); // opposite
    delta.insert(
        build_pk(entity_type, org, 5),
        document_row("e", &[0.9, 0.1, 0.0]),
    ); // very close

    let mut snapshot = ColumnarSnapshot::empty();
    snapshot.delta.insert(org_key, delta);
    snapshot.visible_frontier =
        FrontierPosition::AppliedThrough(CommitSequence::new(5).expect("seq"));

    let result = nearest_query_snapshot(
        &definition,
        &snapshot,
        &request(org, vector_field, &[1.0, 0.0, 0.0], 3, Vec::new()),
    )
    .expect("nearest query");

    assert_eq!(result.rows.len(), 3, "expected k=3 results");
    assert!(result.rows[0].distance <= result.rows[1].distance);
    assert!(result.rows[1].distance <= result.rows[2].distance);
    assert!(
        result.rows[0].distance < 1e-6,
        "closest should be identical"
    );
    assert!(
        result.rows[2].distance < 0.5,
        "third should be within 45 degrees"
    );
}

#[test]
fn nearest_query_respects_org_isolation() {
    let bundle = vector_bundle();
    let (definition, entity_type) = document_vector_projection(&bundle);
    let vector_field = field_id(&bundle, "Document", "embedding");

    let org_a = [1u8; 16];
    let org_b = [2u8; 16];
    let org_a_key = OrgKey::from_value(&CanonicalValue::Uuid(org_a)).expect("a");
    let org_b_key = OrgKey::from_value(&CanonicalValue::Uuid(org_b)).expect("b");

    let mut delta_a: BTreeMap<PrimaryKeyBytes, LiveRow> = BTreeMap::new();
    delta_a.insert(
        build_pk(entity_type, org_a, 1),
        document_row("a", &[1.0, 0.0, 0.0]),
    ); // close

    let mut delta_b: BTreeMap<PrimaryKeyBytes, LiveRow> = BTreeMap::new();
    delta_b.insert(
        build_pk(entity_type, org_b, 1),
        document_row("b", &[-1.0, 0.0, 0.0]),
    ); // far

    let mut snapshot = ColumnarSnapshot::empty();
    snapshot.delta.insert(org_a_key, delta_a);
    snapshot.delta.insert(org_b_key, delta_b);
    snapshot.visible_frontier =
        FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("seq"));

    // Org A: finds close doc
    let result = nearest_query_snapshot(
        &definition,
        &snapshot,
        &request(org_a, vector_field, &[1.0, 0.0, 0.0], 10, Vec::new()),
    )
    .expect("org A");
    assert_eq!(result.rows.len(), 1);
    assert!(result.rows[0].distance < 1e-6);

    // Org B: finds only far doc — org A's close doc is invisible
    let result = nearest_query_snapshot(
        &definition,
        &snapshot,
        &request(org_b, vector_field, &[1.0, 0.0, 0.0], 10, Vec::new()),
    )
    .expect("org B");
    assert_eq!(result.rows.len(), 1);
    assert!(result.rows[0].distance > 1.5, "org B doc should be far");
}

/// VEC-007 adversarial proof: rows denied by the filter influence nothing —
/// presence, distances, ranking, or count — and the filter demonstrably
/// applies BEFORE ranking.
///
/// Construction: the DENIED row is the nearest to the query. At `k = 2` over
/// {denied-nearest, auth-mid, auth-far}:
///   - filter-before-rank returns BOTH authorized rows;
///   - filter-after-rank (rank all three, truncate to k, then drop denied)
///     returns only ONE row — the count assertion below reds that order swap.
/// The distances must equal a control run over a snapshot that never
/// contained the denied row at all.
#[test]
fn nearest_query_filters_denied_rows_before_ranking() {
    let bundle = vector_bundle();
    let (definition, entity_type) = document_vector_projection(&bundle);
    let vector_field = field_id(&bundle, "Document", "embedding");
    let title_field = field_id(&bundle, "Document", "title");

    let org = [1u8; 16];
    let org_key = OrgKey::from_value(&CanonicalValue::Uuid(org)).expect("org");

    let denied_pk = build_pk(entity_type, org, 1);
    let auth_mid_pk = build_pk(entity_type, org, 2);
    let auth_far_pk = build_pk(entity_type, org, 3);

    // The denied row is IDENTICAL to the query — strictly nearest.
    let mut delta: BTreeMap<PrimaryKeyBytes, LiveRow> = BTreeMap::new();
    delta.insert(denied_pk.clone(), document_row("secret", &[1.0, 0.0, 0.0]));
    delta.insert(
        auth_mid_pk.clone(),
        document_row("public", &[0.7, 0.7, 0.0]),
    );
    delta.insert(
        auth_far_pk.clone(),
        document_row("public", &[0.0, 1.0, 0.0]),
    );

    let mut snapshot = ColumnarSnapshot::empty();
    snapshot.delta.insert(org_key.clone(), delta);
    snapshot.visible_frontier =
        FrontierPosition::AppliedThrough(CommitSequence::new(3).expect("seq"));

    // Control: a world in which the denied row never existed.
    let mut control_delta: BTreeMap<PrimaryKeyBytes, LiveRow> = BTreeMap::new();
    control_delta.insert(
        auth_mid_pk.clone(),
        document_row("public", &[0.7, 0.7, 0.0]),
    );
    control_delta.insert(
        auth_far_pk.clone(),
        document_row("public", &[0.0, 1.0, 0.0]),
    );
    let mut control_snapshot = ColumnarSnapshot::empty();
    control_snapshot.delta.insert(org_key, control_delta);
    control_snapshot.visible_frontier =
        FrontierPosition::AppliedThrough(CommitSequence::new(3).expect("seq"));

    let deny_predicate = vec![ColumnPredicate::Eq {
        field: title_field,
        value: CanonicalValue::string("public").expect("value"),
    }];

    // Sanity: unfiltered, the denied row IS the top result.
    let unfiltered = nearest_query_snapshot(
        &definition,
        &snapshot,
        &request(org, vector_field, &[1.0, 0.0, 0.0], 2, Vec::new()),
    )
    .expect("unfiltered");
    assert_eq!(unfiltered.rows[0].primary_key.len(), 2);
    assert!(unfiltered.rows[0].distance < 1e-6, "denied row is nearest");

    // Filtered: k=2 must be filled from the authorized rows.
    let filtered = nearest_query_snapshot(
        &definition,
        &snapshot,
        &request(
            org,
            vector_field,
            &[1.0, 0.0, 0.0],
            2,
            deny_predicate.clone(),
        ),
    )
    .expect("filtered");

    // COUNT: exactly k results — a filter-after-rank order swap returns 1.
    assert_eq!(
        filtered.rows.len(),
        2,
        "filter must run before ranking: k results from authorized rows"
    );
    // PRESENCE: the denied row's cells are absent from every returned row.
    for row in &filtered.rows {
        assert_ne!(
            row.cells[0],
            CanonicalValue::string("secret").expect("value"),
            "denied row must be absent"
        );
    }
    // DISTANCES + RANKING: byte-identical to the control world without the
    // denied row — its existence influenced nothing.
    let control = nearest_query_snapshot(
        &definition,
        &control_snapshot,
        &request(org, vector_field, &[1.0, 0.0, 0.0], 2, deny_predicate),
    )
    .expect("control");
    assert_eq!(filtered.rows.len(), control.rows.len());
    for (filtered_row, control_row) in filtered.rows.iter().zip(control.rows.iter()) {
        assert_eq!(filtered_row.primary_key, control_row.primary_key);
        assert_eq!(
            filtered_row.distance.to_bits(),
            control_row.distance.to_bits(),
            "distances must be exactly those of the denied-row-free world"
        );
    }
    // And the denied row's absence did not zero the top distance.
    assert!(filtered.rows[0].distance > 0.01);
}

/// The engine rejects a query vector that does not match the declared
/// dimension as a typed error (M5: previously unchecked, and a mismatched
/// stored cell panicked in exact_knn).
#[test]
fn nearest_query_rejects_query_dimension_mismatch() {
    let bundle = vector_bundle();
    let (definition, entity_type) = document_vector_projection(&bundle);
    let vector_field = field_id(&bundle, "Document", "embedding");

    let org = [1u8; 16];
    let org_key = OrgKey::from_value(&CanonicalValue::Uuid(org)).expect("org");
    let mut delta: BTreeMap<PrimaryKeyBytes, LiveRow> = BTreeMap::new();
    delta.insert(
        build_pk(entity_type, org, 1),
        document_row("a", &[1.0, 0.0, 0.0]),
    );
    let mut snapshot = ColumnarSnapshot::empty();
    snapshot.delta.insert(org_key, delta);
    snapshot.visible_frontier =
        FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("seq"));

    // Declared dimension is 3; the query vector has 2 components.
    let error = nearest_query_snapshot(
        &definition,
        &snapshot,
        &request(org, vector_field, &[1.0, 0.0], 5, Vec::new()),
    )
    .expect_err("dimension mismatch must be a typed error");
    assert!(matches!(
        error,
        QueryError::VectorDimensionMismatch {
            expected: 3,
            actual: 2,
        }
    ));
}

/// Exact KNN is the WP-594 recall harness ground truth — must be deterministic.
#[test]
fn nearest_query_exact_knn_is_deterministic() {
    let bundle = vector_bundle();
    let (definition, entity_type) = document_vector_projection(&bundle);
    let vector_field = field_id(&bundle, "Document", "embedding");

    let org = [1u8; 16];
    let org_key = OrgKey::from_value(&CanonicalValue::Uuid(org)).expect("org");

    let mut delta: BTreeMap<PrimaryKeyBytes, LiveRow> = BTreeMap::new();
    for i in 1..=20u64 {
        let angle = (i as f32) * 0.1;
        delta.insert(
            build_pk(entity_type, org, i),
            document_row("doc", &[angle.cos(), angle.sin(), 0.0]),
        );
    }

    let mut snapshot = ColumnarSnapshot::empty();
    snapshot.delta.insert(org_key, delta);
    snapshot.visible_frontier =
        FrontierPosition::AppliedThrough(CommitSequence::new(20).expect("seq"));

    let query = request(org, vector_field, &[1.0, 0.0, 0.0], 5, Vec::new());
    let result_a = nearest_query_snapshot(&definition, &snapshot, &query).expect("a");
    let result_b = nearest_query_snapshot(&definition, &snapshot, &query).expect("b");

    assert_eq!(result_a.rows.len(), result_b.rows.len());
    for (a, b) in result_a.rows.iter().zip(result_b.rows.iter()) {
        assert_eq!(a.distance, b.distance, "exact KNN must be deterministic");
        assert_eq!(a.primary_key, b.primary_key);
    }
}

/// Vectors are entity field values, never org-scope keys: registration
/// rejects a vector-typed org scope (S7 — the gate previously admitted it
/// against its own comment).
#[test]
fn vector_org_scope_is_rejected_at_registration() {
    let bundle = vector_bundle();
    let title = field_id(&bundle, "Document", "title");
    let embedding = field_id(&bundle, "Document", "embedding");
    let error = RegisteredDefinition::register(
        riffdb_columnar::ColumnarProjectionDefinition {
            name: "bad_scope".into(),
            entity_name: "Document".into(),
            projected_fields: vec![title],
            org_scope_field: embedding,
        },
        &bundle,
    )
    .expect_err("a vector org scope must be rejected");
    assert!(matches!(
        error,
        riffdb_columnar::DefinitionError::UnsupportedColumnType { .. }
    ));
}
