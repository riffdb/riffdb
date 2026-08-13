#![forbid(unsafe_code)]

//! End-to-end front door for vector-bearing contracts (VEC-001, ADR-0091).
//!
//! The unit test `vector_field_compiles_to_vector_typed_entity_field` stops at
//! `lower_schema`; these tests exercise the complete public pipeline —
//! `compile_contract_source` through bundle assembly, canonical encoding, and
//! decode — which is exactly where the hardcoded JSON-schema preflight size
//! and the dropped vector dimension previously broke every vector contract.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{ContractBundle, ValueTypeTag};

fn vector_contract(dimension: u32) -> String {
    format!(
        r#"
contract Docs version 1 {{
  entity Document {{
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field body: string<65536>
    vector_field embedding({dimension}, cosine, (title, body), staleness_slo 60)
  }}
}}
"#
    )
}

/// A contract declaring a `vector_field` MUST compile through the public
/// entry point. Reds if the JSON-schema size preflight and the rendered
/// vector node diverge again (M1).
#[test]
fn vector_field_contract_compiles_through_the_public_front_door() {
    let bundle = compile_contract_source(&vector_contract(128))
        .expect("a contract declaring a vector_field must compile");
    let entity = &bundle.schema().entities()[0];
    let embedding = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "embedding")
        .expect("embedding field present in the compiled bundle");
    assert_eq!(embedding.value_type().tag(), ValueTypeTag::Vector);
    assert_eq!(
        embedding
            .value_type()
            .vector_dimension()
            .map(|dimension| dimension.get()),
        Some(128)
    );
}

/// The encoded bundle MUST round-trip through decode with the vector
/// dimension intact. Reds if the encoder drops the dimension payload or the
/// decoder loses its VECTOR arm again (M2).
#[test]
fn vector_field_bundle_round_trips_with_dimension_intact() {
    let bundle = compile_contract_source(&vector_contract(1536)).expect("compiles");
    let decoded = ContractBundle::decode(bundle.canonical_bytes())
        .expect("a vector-bearing bundle must decode");
    assert_eq!(decoded.bundle_hash(), bundle.bundle_hash());
    let entity = &decoded.schema().entities()[0];
    let embedding = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "embedding")
        .expect("embedding survives decode");
    assert_eq!(
        embedding
            .value_type()
            .vector_dimension()
            .map(|dimension| dimension.get()),
        Some(1536)
    );
}

/// Contracts that differ only in vector dimension MUST have distinct
/// canonical bytes and distinct bundle hashes: the dimension is part of the
/// contract's durable identity (M2's collision consequence).
///
/// De-confounded (N1): the two sources necessarily differ as text, and
/// `source_hash` is written into the bundle, so a bare byte/hash inequality
/// proves NOTHING about the encoder — the previous form of this test stayed
/// green with the dimension entirely absent from the encoding. The
/// non-confounded instrument is decode: each bundle's own canonical bytes
/// must carry its dimension back out, and only then is the hash inequality
/// evidence about the encoding rather than about the source text.
#[test]
fn vector_dimension_is_part_of_the_bundle_identity() {
    let bundle_128 = compile_contract_source(&vector_contract(128)).expect("compiles at 128");
    let bundle_1536 = compile_contract_source(&vector_contract(1536)).expect("compiles at 1536");

    let decoded_dimension = |bundle: &ContractBundle| {
        ContractBundle::decode(bundle.canonical_bytes())
            .expect("bundle decodes")
            .schema()
            .entities()[0]
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "embedding")
            .expect("embedding present after decode")
            .value_type()
            .vector_dimension()
            .map(|dimension| dimension.get())
    };
    assert_eq!(
        decoded_dimension(&bundle_128),
        Some(128),
        "the 128 bundle's canonical bytes must carry dimension 128"
    );
    assert_eq!(
        decoded_dimension(&bundle_1536),
        Some(1536),
        "the 1536 bundle's canonical bytes must carry dimension 1536"
    );
    assert_ne!(
        bundle_128.bundle_hash(),
        bundle_1536.bundle_hash(),
        "vector<128> and vector<1536> must not share a content address"
    );
}

/// The declared metric, source fields, and staleness SLO reach the compiled
/// artifact and survive encode/decode (S8 — previously parsed, validated,
/// and discarded; only the dimension survived).
#[test]
fn vector_search_configuration_reaches_the_bundle_and_round_trips() {
    let bundle = compile_contract_source(&vector_contract(128)).expect("compiles");
    let specs = bundle.schema().vector_field_specs();
    assert_eq!(specs.len(), 1, "one vector field spec must be carried");
    let spec = &specs[0];
    assert_eq!(spec.metric(), riffdb_types::DistanceMetric::Cosine);
    assert_eq!(spec.stale_entity_count_threshold(), 60);
    let entity = &bundle.schema().entities()[0];
    let source_names: Vec<&str> = spec
        .source_fields()
        .iter()
        .map(|field_id| {
            entity
                .record()
                .field(*field_id)
                .expect("source field resolves")
                .name()
        })
        .collect();
    // Source fields ride in canonical field-ID order.
    let mut sorted = source_names.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, ["body", "title"]);

    let decoded = ContractBundle::decode(bundle.canonical_bytes()).expect("decodes");
    assert_eq!(
        decoded.schema().vector_field_specs(),
        bundle.schema().vector_field_specs(),
        "the search configuration must survive the durable round trip"
    );
}

/// Rebuilds `bundle` around `schema`, holding every other constructor input
/// — including the source hash — exactly fixed.
fn rebundle_with_schema(
    bundle: &ContractBundle,
    schema: riffdb_contract_ir::SchemaIr,
) -> ContractBundle {
    ContractBundle::new_with_workflows_and_row_policies(
        bundle.compiler_version(),
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.parent(),
        bundle.source_hash(),
        bundle.ledger().clone(),
        schema,
        bundle.workflows().workflows().to_vec(),
        bundle.row_policies().clone(),
        bundle.commands().to_vec(),
        bundle.projections().to_vec(),
        bundle.schema_artifacts().to_vec(),
        bundle.mcp_command_names().clone(),
        bundle.compatibility().clone(),
    )
    .expect("rebundled")
}

/// Contracts that differ only in their vector search configuration have
/// distinct bundle identities: the metric and the staleness SLO are part of
/// the durable contract.
///
/// De-confounded (N1): ONE source is compiled ONCE, and the comparison
/// bundle is rebuilt from the same constructor inputs with only the schema's
/// `VectorFieldSpecV1` changed — the source hash is asserted identical, so
/// nothing but the configuration reaching the canonical encoding can make
/// these bytes differ. (Compiling two different source texts here would let
/// `source_hash` alone satisfy every assertion, which is exactly how the
/// previous form of this test stayed green with the whole spec section
/// deleted from the encoder.)
#[test]
fn distance_metric_and_slo_are_part_of_the_bundle_identity() {
    let bundle = compile_contract_source(&vector_contract(128)).expect("compiles");
    let spec = bundle.schema().vector_field_specs()[0].clone();
    assert_eq!(spec.metric(), riffdb_types::DistanceMetric::Cosine);

    // Metric changed; dimension, source fields, SLO, and SOURCE identical.
    let euclidean_spec = riffdb_contract_ir::VectorFieldSpecV1::new(
        spec.entity(),
        spec.field(),
        riffdb_types::DistanceMetric::Euclidean,
        spec.source_fields().to_vec(),
        spec.stale_entity_count_threshold(),
    )
    .expect("metric-changed spec");
    let metric_changed = rebundle_with_schema(
        &bundle,
        bundle
            .schema()
            .clone()
            .with_vector_field_specs(vec![euclidean_spec])
            .expect("schema with changed metric"),
    );
    assert_eq!(
        bundle.source_hash(),
        metric_changed.source_hash(),
        "the source-hash confound is held fixed"
    );
    assert_ne!(
        bundle.canonical_bytes(),
        metric_changed.canonical_bytes(),
        "the metric must reach the canonical bundle bytes"
    );
    assert_ne!(
        bundle.bundle_hash(),
        metric_changed.bundle_hash(),
        "cosine and euclidean configurations must not share a content address"
    );

    // Staleness SLO changed; everything else (including source) identical.
    let slo_spec = riffdb_contract_ir::VectorFieldSpecV1::new(
        spec.entity(),
        spec.field(),
        spec.metric(),
        spec.source_fields().to_vec(),
        spec.stale_entity_count_threshold() + 60,
    )
    .expect("slo-changed spec");
    let slo_changed = rebundle_with_schema(
        &bundle,
        bundle
            .schema()
            .clone()
            .with_vector_field_specs(vec![slo_spec])
            .expect("schema with changed SLO"),
    );
    assert_eq!(bundle.source_hash(), slo_changed.source_hash());
    assert_ne!(
        bundle.canonical_bytes(),
        slo_changed.canonical_bytes(),
        "the staleness SLO must reach the canonical bundle bytes"
    );
    assert_ne!(bundle.bundle_hash(), slo_changed.bundle_hash());
}
