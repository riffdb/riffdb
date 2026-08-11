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
#[test]
fn vector_dimension_is_part_of_the_bundle_identity() {
    let bundle_128 = compile_contract_source(&vector_contract(128)).expect("compiles at 128");
    let bundle_1536 = compile_contract_source(&vector_contract(1536)).expect("compiles at 1536");
    assert_ne!(
        bundle_128.canonical_bytes(),
        bundle_1536.canonical_bytes(),
        "dimension must reach the canonical bundle bytes"
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
    assert_eq!(spec.staleness_slo_secs(), 60);
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

/// Contracts that differ only in distance metric have distinct bundle
/// identities: the metric is part of the durable contract.
#[test]
fn distance_metric_is_part_of_the_bundle_identity() {
    let cosine = compile_contract_source(&vector_contract(128)).expect("cosine compiles");
    let euclidean_source = vector_contract(128).replace("cosine", "euclidean");
    let euclidean = compile_contract_source(&euclidean_source).expect("euclidean compiles");
    assert_ne!(
        cosine.bundle_hash(),
        euclidean.bundle_hash(),
        "cosine and euclidean contracts must not share a content address"
    );
}
