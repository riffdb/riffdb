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
