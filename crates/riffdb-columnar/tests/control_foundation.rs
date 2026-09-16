//! Schema-bound columnar identity and hash-domain acceptance.

mod common;

use riffdb_columnar::{
    ColumnarDefinitionSemanticsV1, ColumnarEngine, ColumnarProjectionSourceV1,
    ColumnarProjectionSpecV1, ColumnarVectorSpecExtensionV1, OpenOptions,
};
use riffdb_storage_api::{
    ColumnarProjectionArtifactV1, ColumnarProjectionLayoutV1, FreshColumnarProjectionControlV1,
    StoredColumnarProjectionGenerationV1,
};
use riffdb_types::{FrontierPosition, ProjectionGeneration};

// req: PRJ-002, PRJ-004, PRJ-006, PRJ-007, PRJ-010, OQ-024, OQ-053
#[test]
fn controlled_v1_manifest_opens_only_the_exact_immutable_artifact() {
    let bundle = common::compile_bundle();
    let definition = common::register_ticket_board(&bundle);
    let directory = common::temp_dir("controlled-v1-manifest");
    let mut engine = ColumnarEngine::open(
        definition.clone(),
        OpenOptions::new(&directory).with_controlled_manifest(None),
    )
    .expect("fresh controlled engine");
    let manifest = engine.checkpoint().expect("durable manifest");
    let (length, checksum) = manifest.artifact_identity();
    let name = format!("MANIFEST-V1-{}", hex(&checksum));
    assert!(directory.join(&name).is_file());
    assert!(!directory.join("MANIFEST").exists());

    std::fs::write(directory.join("MANIFEST"), b"legacy must remain inert")
        .expect("write inert legacy manifest");
    let reopened = ColumnarEngine::open(
        definition.clone(),
        OpenOptions::new(&directory).with_controlled_manifest(Some((length, checksum))),
    )
    .expect("open exact selected artifact");
    assert_eq!(
        reopened.durable_frontier().position(),
        manifest.durable_frontier
    );
    let spec = ColumnarProjectionSpecV1::for_scalar(&definition, &bundle).expect("scalar spec");
    let pointer = StoredColumnarProjectionGenerationV1::prepared_candidate(
        ProjectionGeneration::first(),
        ColumnarProjectionLayoutV1::V1,
        FrontierPosition::BeforeFirst,
        manifest.durable_frontier,
        1,
        ColumnarProjectionArtifactV1::new(length, checksum).expect("artifact"),
        definition.fingerprint(),
        spec.hash(),
        None,
    )
    .expect("prepared pointer");
    let prepared = reopened
        .prepared_v1_generation(&spec, pointer, [0x41; 16])
        .expect("validated reopen mints the witness");
    let expected = FreshColumnarProjectionControlV1::new(
        spec.source().clone(),
        spec.definition_fingerprint(),
        spec.hash(),
        spec.replay_limits(),
        1,
    )
    .expect("fresh control");
    assert!(
        prepared
            .replacement_for(expected.control(), [0x41; 16])
            .is_ok()
    );
    assert!(
        prepared
            .replacement_for(expected.control(), [0x42; 16])
            .is_err(),
        "a witness is exact to the validating process generation"
    );

    let uncontrolled_directory = common::temp_dir("uncontrolled-v1-manifest");
    let uncontrolled =
        ColumnarEngine::open(definition.clone(), OpenOptions::new(uncontrolled_directory))
            .expect("uncontrolled engine");
    assert!(
        uncontrolled
            .prepared_v1_generation(&spec, prepared.generation().clone(), [0x41; 16])
            .is_err(),
        "an ordinary or fresh engine cannot forge validated reopen evidence"
    );
    assert!(
        ColumnarEngine::open(
            definition,
            OpenOptions::new(directory).with_controlled_manifest(Some((length, [0x55; 32]))),
        )
        .is_err(),
        "a missing selected artifact must not fall back to any manifest"
    );
}
use riffdb_contract_compiler::compile_contract_source;
use riffdb_types::{
    HashDomain, hash, hash_columnar_definition_semantics, hash_columnar_projection_spec,
};

// req: PRJ-005, PRJ-010, OQ-017, OQ-019, OQ-021, OQ-022, OQ-024, OQ-053
#[test]
fn columnar_hash_domain_and_definition_semantics_are_collision_closed() {
    let bundle = common::compile_bundle();
    let registered = common::register_ticket_board(&bundle);
    let renamed = riffdb_columnar::RegisteredDefinition::register(
        riffdb_columnar::ColumnarProjectionDefinition {
            name: "renamed_board".into(),
            entity_name: registered.entity_name().into(),
            projected_fields: registered.projected_fields().to_vec(),
            org_scope_field: registered.org_scope_field(),
        },
        &bundle,
    )
    .expect("renamed registration");

    let semantics = ColumnarDefinitionSemanticsV1::from_registered(&registered)
        .expect("bounded definition semantics");
    let renamed_semantics =
        ColumnarDefinitionSemanticsV1::from_registered(&renamed).expect("renamed semantics");
    assert_eq!(registered.fingerprint(), renamed.fingerprint());
    assert_eq!(
        semantics, renamed_semantics,
        "names are not semantic identity"
    );

    let source =
        ColumnarProjectionSourceV1::scalar(bundle.lineage().clone(), registered.fingerprint());
    assert_eq!(
        ColumnarProjectionSourceV1::from_canonical_bytes(&source.to_canonical_bytes())
            .expect("canonical source round trip"),
        source
    );
    assert_eq!(
        source.to_canonical_bytes(),
        decode_hex(
            "01000f436f6c756d6e61724861726e657373e2e9f298499719bc8ec87ad595e0f3a60473100750248adf5f2670ffbba078fa"
        )
    );
    assert_eq!(
        semantics.as_bytes(),
        decode_hex(
            "00010000000200000002000000010000001e0000000500010a01000000100000000000000004000103010000000800000000000000030000000200010300000001000506000000c8000000030001020000000500010a00000000"
        )
    );
    assert_eq!(semantics.hash_payload().len(), 95);
    assert_eq!(semantics.hash_payload()[..5], [0x01, 0, 0, 0, 90]);
    assert_eq!(
        semantics.hash().as_bytes(),
        &<[u8; 32]>::try_from(decode_hex(
            "2b89cdb631e35b7182c910e4b0880b1fceb5d6a7c66c2e2ab2ff408f3a46867a"
        ))
        .expect("32-byte golden")
    );

    let spec =
        ColumnarProjectionSpecV1::for_scalar(&registered, &bundle).expect("bounded scalar spec");
    assert_eq!(spec.source(), &source);
    assert_eq!(
        spec.hash_payload(),
        decode_hex(
            "020001003201000f436f6c756d6e61724861726e657373e2e9f298499719bc8ec87ad595e0f3a60473100750248adf5f2670ffbba078fae2e9f298499719bc8ec87ad595e0f3a60473100750248adf5f2670ffbba078fa2b89cdb631e35b7182c910e4b0880b1fceb5d6a7c66c2e2ab2ff408f3a46867a015250504400010101000000fb03010100000186a0000001f4001000000000000000002000000000000000006400000001e2e9f298499719bc8ec87ad595e0f3a60473100750248adf5f2670ffbba078fa0000400000000000000f4240000040000000100000000000000003e8000000000000000000015180000000004000000000000000000186a000000000"
        )
    );
    assert_ne!(
        spec.hash().as_bytes(),
        hash(HashDomain::CanonicalValue, spec.hash_payload()).as_bytes(),
        "an identical payload in another domain must not collide"
    );
    assert_ne!(
        semantics.hash().as_bytes(),
        spec.hash().as_bytes(),
        "purpose tags must separate the two typed hashes"
    );
    assert_eq!(
        spec.hash().as_bytes(),
        &<[u8; 32]>::try_from(decode_hex(
            "4b7419ee00a87fdc8e6bfb4d31559b87ce3730a2c02aa7bbd8cc7fc99a9b0016"
        ))
        .expect("32-byte golden")
    );
    assert_eq!(HashDomain::ALL.len(), 53);
    assert_eq!(
        HashDomain::ColumnarProjectionSpec.label(),
        "riffdb.columnar-projection-spec/v1"
    );
    assert_eq!(HashDomain::ColumnarProjectionSpec.label().len(), 34);
    let spec_frame = hash_frame(HashDomain::ColumnarProjectionSpec, spec.hash_payload());
    assert_eq!(spec.hash_payload().len(), 260);
    assert_eq!(spec_frame.len(), 317);
    assert_eq!(
        &spec_frame[..57],
        decode_hex(
            "5249464644422d48415348000100227269666664622e636f6c756d6e61722d70726f6a656374696f6e2d737065632f76310000000000000104"
        )
    );
    assert_eq!(
        hash_columnar_projection_spec(spec.hash_payload()).as_bytes(),
        spec.hash().as_bytes()
    );
    let mut alternate_purpose = semantics.hash_payload().to_vec();
    alternate_purpose[0] = 0x02;
    assert_ne!(
        hash_columnar_definition_semantics(semantics.hash_payload()).as_bytes(),
        hash_columnar_projection_spec(&alternate_purpose).as_bytes()
    );
    let mut trailing_source = source.to_canonical_bytes();
    trailing_source.push(0);
    assert!(ColumnarProjectionSourceV1::from_canonical_bytes(&trailing_source).is_err());
    let mut unknown_source = source.to_canonical_bytes();
    unknown_source[0] = 0xff;
    assert!(ColumnarProjectionSourceV1::from_canonical_bytes(&unknown_source).is_err());
}

// req: PRJ-005, PRJ-010, OQ-017, OQ-019, OQ-020, OQ-021, OQ-022, OQ-024, OQ-053
#[test]
fn columnar_vector_extension_binds_complete_checked_production_semantics() {
    const VECTOR_CONTRACT: &str = r#"
contract ColumnarVector version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    vector_field embedding(4, cosine, (title), staleness_slo 60, model "embed-v1", current_version "2026-08-21", replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000, ann_threshold 32, recall_target_bps 9500)
  }
}
"#;
    let bundle = compile_contract_source(VECTOR_CONTRACT).expect("production vector contract");
    let org = common::field_id(&bundle, "Document", "org_id");
    let embedding = common::field_id(&bundle, "Document", "embedding");
    let definition = riffdb_columnar::RegisteredDefinition::register(
        riffdb_columnar::ColumnarProjectionDefinition {
            name: "document_embeddings".into(),
            entity_name: "Document".into(),
            projected_fields: vec![embedding],
            org_scope_field: org,
        },
        &bundle,
    )
    .expect("vector registration");
    let extension = ColumnarVectorSpecExtensionV1::from_registered(&definition, embedding, &bundle)
        .expect("checked vector extension");
    assert_eq!(
        extension.as_bytes(),
        decode_hex(
            "0000000401000100000001000000000000003c0008656d6265642d7631000a323032362d30382d323101000000200000251c"
        )
    );
    let semantics =
        ColumnarDefinitionSemanticsV1::from_registered(&definition).expect("vector semantics");
    let spec = ColumnarProjectionSpecV1::for_vector(&definition, embedding, &bundle)
        .expect("compiler-bound vector spec");
    assert_eq!(spec.vector_extension(), extension.as_bytes());
    assert_eq!(spec.descriptors().len(), 2, "ANN adds only Approximate");
    assert_eq!(spec.replay_limits().age_seconds(), 86_400);
    assert_eq!(spec.replay_limits().bytes(), 1_073_741_824);
    assert_eq!(spec.replay_limits().backlog(), 100_000);
    assert_eq!(
        spec.hash_payload(),
        decode_hex(
            "020001001902000e436f6c756d6e6172566563746f720000000100000004b8fb46c6e88b943a36dcfe3396a7e028daa919d4fee34d50da216dc335fada35ccbb31bf4f3205730fada7c568658e10932806e91ca3046312de357eb9dbe30d025250504400010201000000c703010100000001f4000001f3000000000000000000002000000000000000006400000001b8fb46c6e88b943a36dcfe3396a7e028daa919d4fee34d50da216dc335fada350000400000000000001f4000000040000000100000000000000003e8000000005250504400010202251c00c703010100000001f4000001f3000000000000000000002000000000000000006400000001b8fb46c6e88b943a36dcfe3396a7e028daa919d4fee34d50da216dc335fada350000400000000000001f4000000040000000100000000000000003e8000000000000000000015180000000004000000000000000000186a0000000320000000401000100000001000000000000003c0008656d6265642d7631000a323032362d30382d323101000000200000251c"
        )
    );
    assert_eq!(
        spec.hash().as_bytes(),
        &<[u8; 32]>::try_from(decode_hex(
            "4d4dd5a49513c09a7c429d203d8b0de05f9406a23807327e6b5b0683b7bab3ee"
        ))
        .expect("32-byte vector spec golden")
    );
    let no_ann_source = VECTOR_CONTRACT.replace(", ann_threshold 32, recall_target_bps 9500", "");
    let no_ann_bundle =
        compile_contract_source(&no_ann_source).expect("exact-only vector contract");
    let no_ann_embedding = common::field_id(&no_ann_bundle, "Document", "embedding");
    let no_ann_definition = riffdb_columnar::RegisteredDefinition::register(
        riffdb_columnar::ColumnarProjectionDefinition {
            name: "document_embeddings".into(),
            entity_name: "Document".into(),
            projected_fields: vec![no_ann_embedding],
            org_scope_field: common::field_id(&no_ann_bundle, "Document", "org_id"),
        },
        &no_ann_bundle,
    )
    .expect("exact-only vector registration");
    let no_ann_spec =
        ColumnarProjectionSpecV1::for_vector(&no_ann_definition, no_ann_embedding, &no_ann_bundle)
            .expect("exact-only vector spec");
    assert_eq!(no_ann_spec.descriptors().len(), 1);
    assert_eq!(
        no_ann_spec.vector_extension(),
        decode_hex(
            "0000000401000100000001000000000000003c0008656d6265642d7631000a323032362d30382d323100"
        )
    );
    let dimension_five_bundle =
        compile_contract_source(&VECTOR_CONTRACT.replace("embedding(4,", "embedding(5,"))
            .expect("dimension-five vector contract");
    let dimension_five_embedding =
        common::field_id(&dimension_five_bundle, "Document", "embedding");
    let dimension_five = riffdb_columnar::RegisteredDefinition::register(
        riffdb_columnar::ColumnarProjectionDefinition {
            name: "document_embeddings".into(),
            entity_name: "Document".into(),
            projected_fields: vec![dimension_five_embedding],
            org_scope_field: common::field_id(&dimension_five_bundle, "Document", "org_id"),
        },
        &dimension_five_bundle,
    )
    .expect("dimension-five registration");
    let dimension_five_semantics = ColumnarDefinitionSemanticsV1::from_registered(&dimension_five)
        .expect("dimension-five semantics");
    assert_ne!(
        semantics.hash(),
        dimension_five_semantics.hash(),
        "vector dimension is part of definition semantics"
    );
    assert_ne!(
        spec.hash(),
        ColumnarProjectionSpecV1::for_vector(
            &dimension_five,
            dimension_five_embedding,
            &dimension_five_bundle,
        )
        .expect("dimension-five vector spec")
        .hash(),
        "vector dimension changes the complete spec"
    );
}

fn decode_hex(text: &str) -> Vec<u8> {
    assert_eq!(text.len() % 2, 0);
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII"), 16).expect("golden hex")
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[usize::from(byte >> 4)] as char);
        output.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    output
}

fn hash_frame(domain: HashDomain, payload: &[u8]) -> Vec<u8> {
    let mut frame = b"RIFFDB-HASH\0".to_vec();
    frame.push(0x01);
    frame.extend_from_slice(&(domain.label().len() as u16).to_be_bytes());
    frame.extend_from_slice(domain.label().as_bytes());
    frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}
