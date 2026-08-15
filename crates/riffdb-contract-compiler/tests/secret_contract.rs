#![forbid(unsafe_code)]

//! End-to-end front door for secret-classified contracts (ADR-0118, WP-597).
//!
//! These tests exercise the complete public pipeline — `compile_contract_source`
//! through bundle assembly, canonical encoding, and decode — proving the
//! classification is carried, versioned, and part of the bundle identity,
//! while contracts without the keyword keep their exact prior encoding.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{
    BUNDLE_FORMAT_VERSION_V8, BUNDLE_FORMAT_VERSION_V11, CommandExplain, ContractBundle, SchemaIr,
    SecretFieldSpecV1, SecretRevealDestinationV1,
};

const SECRET_CONTRACT: &str = r#"
contract AuthShape version 1 {
  entity Account {
    key (org_id: uuid)
  }
  entity Session {
    key (org_id: uuid, session_id: uuid)
    field secret token_hash: string<256>
    field secret refresh_secret: string<256>
    field expires_at: timestamp
    unique token (org_id, token_hash)
  }
  aggregate AccountRoot {
    root Account
    child Session
    partition_by org_id
    conflict_key (org_id)
  }
}
"#;

const UNCLASSIFIED_CONTRACT: &str = r#"
contract AuthShape version 1 {
  entity Account {
    key (org_id: uuid)
  }
  entity Session {
    key (org_id: uuid, session_id: uuid)
    field token_hash: string<256>
    field refresh_secret: string<256>
    field expires_at: timestamp
    unique token (org_id, token_hash)
  }
  aggregate AccountRoot {
    root Account
    child Session
    partition_by org_id
    conflict_key (org_id)
  }
}
"#;

fn session(bundle: &ContractBundle) -> &riffdb_contract_ir::EntitySchema {
    bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Session")
        .expect("Session entity present")
}

fn field_id(bundle: &ContractBundle, name: &str) -> riffdb_types::FieldId {
    session(bundle)
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .expect("field present")
        .id()
}

/// A contract declaring `field secret` MUST compile through the public entry
/// point, carry one spec per classified field, and classify nothing else.
#[test]
fn secret_classification_compiles_through_the_public_front_door() {
    let bundle =
        compile_contract_source(SECRET_CONTRACT).expect("a secret-classified contract compiles");
    let schema = bundle.schema();
    let entity = session(&bundle).id();
    assert_eq!(schema.secret_field_specs().len(), 2);
    assert!(schema.is_secret_field(entity, field_id(&bundle, "token_hash")));
    assert!(schema.is_secret_field(entity, field_id(&bundle, "refresh_secret")));
    assert!(!schema.is_secret_field(entity, field_id(&bundle, "expires_at")));
    assert!(!schema.is_secret_field(entity, field_id(&bundle, "org_id")));
    assert!(schema.requires_ir_v8());
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V8);
}

/// The classification MUST survive the durable round trip byte-exactly.
#[test]
fn secret_classification_round_trips_through_canonical_bytes() {
    let bundle = compile_contract_source(SECRET_CONTRACT).expect("compiles");
    let decoded = ContractBundle::decode(bundle.canonical_bytes())
        .expect("a secret-bearing bundle must decode");
    assert_eq!(decoded.bundle_hash(), bundle.bundle_hash());
    assert_eq!(
        decoded.schema().secret_field_specs(),
        bundle.schema().secret_field_specs(),
        "the classification must survive the durable round trip"
    );
    assert_eq!(decoded.format_version(), BUNDLE_FORMAT_VERSION_V8);
}

/// Rebuilds `bundle` around `schema`, holding every other constructor input —
/// including the source hash — exactly fixed.
fn rebundle_with_schema(bundle: &ContractBundle, schema: SchemaIr) -> ContractBundle {
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

/// The classification is part of the bundle's durable identity.
///
/// De-confounded (N1): ONE source is compiled ONCE, and the comparison bundle
/// is rebuilt from the same constructor inputs with only the schema's
/// `SecretFieldSpecV1` set changed — the source hash is asserted identical,
/// so nothing but the classification reaching the canonical encoding can make
/// these bytes differ.
#[test]
fn secret_classification_is_part_of_the_bundle_identity() {
    let bundle = compile_contract_source(UNCLASSIFIED_CONTRACT).expect("compiles");
    assert!(bundle.schema().secret_field_specs().is_empty());
    let entity = session(&bundle).id();
    let classified = rebundle_with_schema(
        &bundle,
        bundle
            .schema()
            .clone()
            .with_secret_field_specs(vec![SecretFieldSpecV1::new(
                entity,
                field_id(&bundle, "token_hash"),
            )])
            .expect("schema with classification"),
    );
    assert_eq!(
        bundle.source_hash(),
        classified.source_hash(),
        "the source-hash confound is held fixed"
    );
    assert_ne!(
        bundle.canonical_bytes(),
        classified.canonical_bytes(),
        "the classification must reach the canonical bundle bytes"
    );
    assert_ne!(
        bundle.bundle_hash(),
        classified.bundle_hash(),
        "classified and unclassified contracts must not share a content address"
    );
    let decoded = ContractBundle::decode(classified.canonical_bytes()).expect("decodes");
    assert_eq!(
        decoded.schema().secret_field_specs(),
        classified.schema().secret_field_specs(),
        "the rebuilt bundle's own bytes carry the classification back out"
    );
}

/// Contracts without the keyword MUST NOT change their encoding: the secret
/// extension is emitted only when at least one field is classified, so no
/// existing bundle hash rotates (ADR-0118 §Compatibility).
#[test]
fn unclassified_contracts_keep_their_prior_encoding_and_version() {
    let bundle = compile_contract_source(UNCLASSIFIED_CONTRACT).expect("compiles");
    assert!(!bundle.schema().requires_ir_v8());
    assert!(
        bundle.format_version() < BUNDLE_FORMAT_VERSION_V8,
        "a contract without secret fields must not require the v8 framing"
    );
}

/// The checked-in v7 compatibility fixture MUST keep decoding: a future
/// decoder that loses the secret extension arm reds here against pinned
/// bytes, not freshly generated ones.
#[test]
fn checked_in_secret_bundle_fixture_decodes_with_classification_intact() {
    let bytes: &[u8] = include_bytes!("../../../fixtures/compiler/secret/bundle.bin");
    let pinned_hash = include_str!("../../../fixtures/compiler/secret/bundle-hash.txt").trim_end();
    let decoded = ContractBundle::decode(bytes).expect("the pinned v7 fixture must decode");
    assert_eq!(decoded.format_version(), BUNDLE_FORMAT_VERSION_V8);
    assert_eq!(decoded.schema().secret_field_specs().len(), 2);
    let rendered: String = decoded
        .bundle_hash()
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(rendered, pinned_hash, "the pinned fixture hash must match");
}

/// A programmatically constructed schema MUST reject classifying a
/// primary-key field: key values are the identity that provenance, audit
/// targets, and diagnostics legitimately name.
#[test]
fn secret_classification_rejects_primary_key_fields() {
    let bundle = compile_contract_source(UNCLASSIFIED_CONTRACT).expect("compiles");
    let entity = session(&bundle).id();
    let error = bundle
        .schema()
        .clone()
        .with_secret_field_specs(vec![SecretFieldSpecV1::new(
            entity,
            field_id(&bundle, "org_id"),
        )])
        .expect_err("a primary-key field must not be classifiable");
    assert!(matches!(
        error,
        riffdb_contract_ir::IrValidationError::InvalidReference { .. }
    ));
}

fn reveal_contract(flow: &str) -> String {
    format!(
        r#"
contract SecretReveal version 1 {{
  entity SecretRow {{
    key (organization_id: uuid, row_id: uuid)
    field secret digest: string<256>
    field public_digest: string<256>
  }}
  aggregate SecretRows {{
    root SecretRow
    partition_by organization_id
    conflict_key (organization_id, row_id)
  }}
  command ReadDigest {{
    input organization_id: uuid
    input row_id: uuid
    read SecretRow(organization_id, row_id) as row else Missing {{}}
    return Found {{ digest: {flow} }}
  }}
}}
"#
    )
}

#[test]
fn secret_flow_without_reveal_fails_with_both_source_spans() {
    let source = reveal_contract("row.digest");
    let error = compile_contract_source(&source).expect_err("undeclared disclosure must fail");
    let diagnostic = error
        .semantic()
        .expect("semantic diagnostics")
        .as_slice()
        .iter()
        .find(|diagnostic| diagnostic.code().as_str() == "RDB-C046")
        .expect("secret reveal diagnostic");
    let destination = source.find("digest: row.digest").expect("destination");
    let secret_source = source.find("digest: string<256>").expect("source field");
    assert_eq!(diagnostic.primary_span().start() as usize, destination);
    assert_eq!(
        diagnostic.related_span().map(|span| span.start() as usize),
        Some(secret_source)
    );
}

#[test]
fn exact_reveal_is_versioned_hashed_and_round_trips() {
    let source = reveal_contract("row.digest reveals row.digest");
    let bundle = compile_contract_source(&source).expect("declared disclosure compiles");
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V11);
    let command = bundle.commands().first().expect("command");
    let reveal = command.secret_reveals().first().expect("reveal");
    assert_eq!(command.secret_reveals().len(), 1);
    assert!(matches!(
        reveal.destination(),
        SecretRevealDestinationV1::OutcomeField { .. }
    ));
    let decoded = ContractBundle::decode(bundle.canonical_bytes()).expect("v11 round trip");
    assert_eq!(decoded, bundle);
    assert_eq!(
        decoded.commands()[0].secret_reveals(),
        command.secret_reveals()
    );
}

#[test]
fn checked_in_secret_reveal_fixture_pins_v11_bytes_hash_and_explain() {
    let bytes = include_bytes!("../../../fixtures/compiler/secret-reveal/bundle.bin");
    let pinned_hash =
        include_str!("../../../fixtures/compiler/secret-reveal/bundle-hash.txt").trim_end();
    let pinned_explain =
        include_str!("../../../fixtures/compiler/secret-reveal/command-explain.txt");
    let decoded = ContractBundle::decode(bytes).expect("the pinned v11 fixture must decode");
    assert_eq!(decoded.format_version(), BUNDLE_FORMAT_VERSION_V11);
    let command = decoded.commands().first().expect("fixture command");
    assert_eq!(command.secret_reveals().len(), 1);
    let rendered_hash: String = decoded
        .bundle_hash()
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(rendered_hash, pinned_hash);
    assert_eq!(
        CommandExplain::from_plan(command).render_text(),
        pinned_explain
    );
}

#[test]
fn reveal_must_name_an_exact_secret_dependency() {
    let source = reveal_contract("row.public_digest reveals row.digest");
    let error = compile_contract_source(&source).expect_err("excess reveal must fail");
    let diagnostic = error
        .semantic()
        .expect("semantic diagnostics")
        .as_slice()
        .iter()
        .find(|diagnostic| diagnostic.code().as_str() == "RDB-C046")
        .expect("secret reveal diagnostic");
    let annotation = source.rfind("row.digest").expect("annotation");
    assert_eq!(diagnostic.primary_span().start() as usize, annotation);
    assert!(diagnostic.related_span().is_some());
}

#[test]
fn derived_reveal_requires_every_secret_source_and_no_other_source() {
    let base = r#"
contract DerivedSecretReveal version 1 {
  entity Row {
    key (organization_id: uuid, row_id: uuid)
    field secret secret_score: i64
    field secret secret_offset: i64
  }
  aggregate Rows {
    root Row
    partition_by organization_id
    conflict_key (organization_id, row_id)
  }
  command ReadScore {
    input organization_id: uuid
    input row_id: uuid
    read Row(organization_id, row_id) as row else Missing {}
    return Found { score: FLOW }
  }
}
"#;
    let missing = base.replace(
        "FLOW",
        "row.secret_score + row.secret_offset reveals row.secret_score",
    );
    let error = compile_contract_source(&missing).expect_err("one missing source must fail");
    assert!(
        error
            .semantic()
            .expect("semantic diagnostics")
            .as_slice()
            .iter()
            .any(|diagnostic| diagnostic.code().as_str() == "RDB-C046")
    );

    let exact = base.replace(
        "FLOW",
        concat!(
            "row.secret_score + row.secret_offset ",
            "reveals row.secret_score reveals row.secret_offset"
        ),
    );
    let bundle = compile_contract_source(&exact).expect("both exact sources compile");
    assert_eq!(bundle.commands()[0].secret_reveals().len(), 2);
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V11);
}

#[test]
fn a_field_named_reveals_remains_source_compatible() {
    let source = r#"
contract RevealsIdentifier version 1 {
  entity Row {
    key (organization_id: uuid, row_id: uuid)
    field reveals: string<32>
  }
  aggregate Rows {
    root Row
    partition_by organization_id
    conflict_key (organization_id, row_id)
  }
}
"#;
    compile_contract_source(source).expect("reveals remains a valid identifier");
}

#[test]
fn secret_classification_is_sticky_across_entity_assignments() {
    let base = r#"
contract StickySecret version 1 {
  entity Row {
    key (organization_id: uuid, row_id: uuid)
    field secret secret_value: string<128>
    field secret secret_copy: string<128>
    field public_copy: string<128>
  }
  aggregate Rows {
    root Row
    partition_by organization_id
    conflict_key (organization_id, row_id)
  }
  command CopySecret {
    input request_id: uuid
    input organization_id: uuid
    input row_id: uuid
    idempotency_key request_id
    mutate Row(organization_id, row_id) as row else Missing {}
    FLOW
    return Copied {}
  }
}
"#;
    let sticky = base.replace("FLOW", "set row.secret_copy = row.secret_value");
    let bundle = compile_contract_source(&sticky).expect("secret-to-secret flow stays classified");
    assert!(bundle.commands()[0].secret_reveals().is_empty());
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V8);

    let leaked = base.replace("FLOW", "set row.public_copy = row.secret_value");
    let error = compile_contract_source(&leaked).expect_err("public copy requires reveal");
    assert!(
        error
            .semantic()
            .expect("semantic")
            .as_slice()
            .iter()
            .any(|diagnostic| {
                diagnostic.code().as_str() == "RDB-C046" && diagnostic.related_span().is_some()
            })
    );

    let declared = base.replace(
        "FLOW",
        "set row.public_copy = row.secret_value reveals row.secret_value",
    );
    let bundle = compile_contract_source(&declared).expect("declared entity disclosure compiles");
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V11);
    assert!(matches!(
        bundle.commands()[0].secret_reveals()[0].destination(),
        SecretRevealDestinationV1::EntityField { .. }
    ));
}

#[test]
fn durable_event_disclosure_requires_the_same_exact_annotation() {
    let base = r#"
contract SecretEvent version 1 {
  entity Row {
    key (organization_id: uuid, row_id: uuid)
    field secret digest: string<128>
  }
  event DigestPublished { digest: string<128> }
  aggregate Rows {
    root Row
    partition_by organization_id
    conflict_key (organization_id, row_id)
  }
  command PublishDigest {
    input request_id: uuid
    input organization_id: uuid
    input row_id: uuid
    idempotency_key request_id
    mutate Row(organization_id, row_id) as row else Missing {}
    emit DigestPublished { digest: FLOW }
    return Published {}
  }
}
"#;
    let missing = base.replace("FLOW", "row.digest");
    let error = compile_contract_source(&missing).expect_err("event disclosure requires reveal");
    assert!(
        error
            .semantic()
            .expect("semantic")
            .as_slice()
            .iter()
            .any(|diagnostic| { diagnostic.code().as_str() == "RDB-C046" })
    );

    let declared = base.replace("FLOW", "row.digest reveals row.digest");
    let bundle = compile_contract_source(&declared).expect("declared event disclosure compiles");
    assert!(matches!(
        bundle.commands()[0].secret_reveals()[0].destination(),
        SecretRevealDestinationV1::EventField { .. }
    ));
}
