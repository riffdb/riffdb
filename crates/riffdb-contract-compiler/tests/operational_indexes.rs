//! Compiler-owned operational index metadata and type-safety acceptance.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{IndexFieldEncodingV1, TextKeyProfileV1, ValueTypeTag};

const SOURCE: &str = r#"
contract Search version 1 {
  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field deleted_at: optional<timestamp>
    field title: string<200>
    index by_deleted (organization_id, deleted_at, document_id) presence(deleted_at)
    index by_title (organization_id, title, document_id) text_key(title, binary_utf8_v1)
  }
  aggregate Documents {
    root Document
    partition_by organization_id
    conflict_key (organization_id, document_id)
  }
}
"#;

#[test]
fn lowers_logical_fields_to_closed_physical_index_encodings() {
    let bundle = compile_contract_source(SOURCE).expect("operational indexes compile");
    let entity = &bundle.schema().entities()[0];
    let deleted = entity
        .indexes()
        .iter()
        .find(|index| index.name() == "by_deleted")
        .expect("presence index");
    assert_eq!(
        deleted.encodings(),
        &[
            IndexFieldEncodingV1::Canonical,
            IndexFieldEncodingV1::Presence,
            IndexFieldEncodingV1::Canonical,
        ]
    );
    assert_eq!(
        deleted.key_schema().components()[1].value_type().tag(),
        ValueTypeTag::U64
    );
    assert_eq!(
        deleted.key_schema().components()[2].value_type().tag(),
        ValueTypeTag::Timestamp
    );
    let title = entity
        .indexes()
        .iter()
        .find(|index| index.name() == "by_title")
        .expect("text index");
    assert_eq!(
        title.encodings()[1],
        IndexFieldEncodingV1::TextKey(TextKeyProfileV1::BinaryUtf8)
    );
    assert_eq!(
        title.key_schema().components()[1].value_type().tag(),
        ValueTypeTag::Bytes
    );

    let decoded = riffdb_contract_ir::ContractBundle::decode(bundle.canonical_bytes())
        .expect("operational index metadata round-trips through canonical bundle bytes");
    assert_eq!(decoded.schema(), bundle.schema());
}

#[test]
fn rejects_wrong_option_types_unknown_fields_and_duplicate_encodings() {
    for source in [
        SOURCE.replace("presence(deleted_at)", "presence(title)"),
        SOURCE.replace(
            "text_key(title, binary_utf8_v1)",
            "text_key(deleted_at, binary_utf8_v1)",
        ),
        SOURCE.replace("presence(deleted_at)", "presence(absent)"),
        SOURCE.replace(
            "presence(deleted_at)",
            "presence(deleted_at) presence(deleted_at)",
        ),
    ] {
        compile_contract_source(&source).expect_err("invalid operational index must fail closed");
    }
}

/// A unique key not prefixed by its aggregate's partition route is refused, and
/// the refusal is legible enough to act on.
///
/// This is the shape a real adapter hit: an entity partitioned by its own
/// identifier, carrying a `unique` on a name that has to be unique across every
/// such entity. The rule is correct -- uniqueness is enforced inside one
/// partition -- but the message used to restate the rule twice and offer only
/// the repair that does not help, so the author concluded the constraint was
/// unexpressible and reached for a saga.
///
/// Two properties keep it legible: the diagnostic points at the aggregate whose
/// route the key is measured against, and the help names BOTH repairs.
#[test]
fn a_unique_key_outside_its_partition_route_is_refused_and_names_both_repairs() {
    const GLOBAL_NAME: &str = r#"
contract Registry version 1 {
  entity Item {
    key (item_id: u64)
    field name: string<100>
    unique by_name (name)
  }
  aggregate Items {
    root Item
    partition_by item_id
    conflict_key (item_id)
  }
}
"#;
    let error = compile_contract_source(GLOBAL_NAME)
        .expect_err("a unique key outside the partition route is refused");
    let diagnostic = error
        .semantic()
        .expect("semantic diagnostics")
        .as_slice()
        .iter()
        .find(|diagnostic| {
            diagnostic.code() == riffdb_contract_compiler::CompilerDiagnosticCode::InvalidUniqueKey
        })
        .expect("RDB-C025");

    // The aggregate is the other half of the rule; without it the author cannot
    // see which route the key failed against.
    let related = diagnostic
        .related_span()
        .expect("the refusal names the aggregate whose route the key must match");
    let aggregate_at = GLOBAL_NAME.find("aggregate Items").expect("aggregate");
    assert!(
        (related.start() as usize) >= aggregate_at,
        "the related span must point at the aggregate, not back at the key"
    );

    let help = diagnostic.code().help().expect("RDB-C025 help");
    // Repair one: prefix the route, accepting per-route uniqueness.
    assert!(
        help.contains("partition route"),
        "help must name the route requirement: {help}"
    );
    // Repair two: widen the partition. This is the one that unblocks a value
    // that must be unique across a wider scope, and the one that was missing.
    assert!(
        help.contains("partition the aggregate by that scope"),
        "help must offer widening the partition, not only fixing the key: {help}"
    );
    // And the reason widening is affordable, which is the belief that made the
    // correct repair look unacceptable.
    assert!(
        help.contains("conflict_key"),
        "help must say writers contend on conflict_key, not on the partition: {help}"
    );
}

/// The same rule still refuses a unique key whose fields are not key-compatible,
/// and that refusal does NOT point at the aggregate, because the route is not
/// what went wrong.
#[test]
fn a_unique_key_over_an_optional_field_is_refused_without_blaming_the_route() {
    const OPTIONAL_UNIQUE: &str = r#"
contract Registry version 1 {
  entity Item {
    key (tenant_id: uuid, item_id: u64)
    field name: optional<string<100>>
    unique by_name (tenant_id, name)
  }
  aggregate Items {
    root Item
    partition_by tenant_id
    conflict_key (tenant_id, item_id)
  }
}
"#;
    let error = compile_contract_source(OPTIONAL_UNIQUE)
        .expect_err("a unique key over an optional field is refused");
    let diagnostic = error
        .semantic()
        .expect("semantic diagnostics")
        .as_slice()
        .iter()
        .find(|diagnostic| {
            diagnostic.code() == riffdb_contract_compiler::CompilerDiagnosticCode::InvalidUniqueKey
        })
        .expect("RDB-C025");
    assert!(
        diagnostic.related_span().is_none(),
        "the route is correct here, so the aggregate must not be implicated"
    );
}

/// ADR-0172: the fold is linked, reachable, and produces a distinct identity.
///
/// It was declared, specified, bound-charged, and wire-encoded for a long time
/// while refusing to lower, so the value of this arm is that the profile now
/// reaches a compiled contract at all.
#[test]
fn the_unicode_fold_profile_compiles_and_changes_index_identity() {
    const FOLDED: &str = r#"
contract Folded version 1 {
  entity Doc {
    key (tenant_id: uuid, doc_id: uuid)
    field title: string<40>
    index by_title (tenant_id, title, doc_id)
      text_key(title, unicode_fold_v1)
  }
  aggregate Docs { root Doc partition_by tenant_id conflict_key (tenant_id, doc_id) }
}
"#;
    let folded = compile_contract_source(FOLDED).expect("the fold profile compiles");
    let binary = compile_contract_source(&FOLDED.replace("unicode_fold_v1", "binary_utf8_v1"))
        .expect("the binary profile compiles");

    // Two profiles over the same field are two different durable indexes.
    // Sharing an identity would let a rebuild silently reinterpret stored keys.
    assert_ne!(
        folded.bundle_hash(),
        binary.bundle_hash(),
        "a text profile is part of durable index identity"
    );
}

/// The fold charges its worst-case expansion, so a field that fits under the
/// binary profile can legitimately exceed the key bound under the fold. That is
/// honest arithmetic rather than a regression, and it must be a bound refusal
/// rather than a silent truncation.
#[test]
fn the_fold_charges_its_expansion_against_the_key_bound() {
    const WIDE: &str = r#"
contract Folded version 1 {
  entity Doc {
    key (tenant_id: uuid, doc_id: uuid)
    field title: string<200>
    index by_title (tenant_id, title, doc_id)
      text_key(title, unicode_fold_v1)
  }
  aggregate Docs { root Doc partition_by tenant_id conflict_key (tenant_id, doc_id) }
}
"#;
    // The same field is fine unfolded.
    compile_contract_source(&WIDE.replace("unicode_fold_v1", "binary_utf8_v1"))
        .expect("200 bytes fits the binary profile");

    let error = compile_contract_source(WIDE).expect_err("200 bytes folded exceeds the key bound");
    assert!(
        error
            .semantic()
            .expect("semantic diagnostics")
            .as_slice()
            .iter()
            .any(|diagnostic| {
                diagnostic.code() == riffdb_contract_compiler::CompilerDiagnosticCode::BoundExceeded
            }),
        "the expansion must be charged as a bound refusal"
    );
}
