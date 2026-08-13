#![forbid(unsafe_code)]

//! End-to-end front door for secret-classified contracts (ADR-0118, WP-597).
//!
//! These tests exercise the complete public pipeline — `compile_contract_source`
//! through bundle assembly, canonical encoding, and decode — proving the
//! classification is carried, versioned, and part of the bundle identity,
//! while contracts without the keyword keep their exact prior encoding.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{BUNDLE_FORMAT_VERSION_V8, ContractBundle, SchemaIr, SecretFieldSpecV1};

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
