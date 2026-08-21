//! Versioned operational-index grammar and source-span tests.

use riffdb_contract_syntax::ast::{Declaration, EntityItem, IndexOption, TextKeyProfile};
use riffdb_contract_syntax::parse_contract;

#[test]
fn parses_closed_presence_and_text_key_options() {
    let source = concat!(
        "contract Search version 1 { entity Document { ",
        "key (organization_id: uuid, document_id: uuid) ",
        "field deleted_at: optional<timestamp> field title: string<200> ",
        "index by_deleted (organization_id, deleted_at, document_id) presence(deleted_at) ",
        "index by_title (organization_id, title, document_id) ",
        "text_key(title, unicode_fold_v1) } }",
    );
    let document = parse_contract(source).expect("operational indexes parse");
    let Declaration::Entity(entity) = &document.contract.value.declarations[0].value else {
        panic!("entity declaration")
    };
    let indexes = entity
        .items
        .iter()
        .filter_map(|item| match &item.value {
            EntityItem::Index(index) => Some(index),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(indexes.len(), 2);
    assert!(matches!(
        indexes[0].options[0].value,
        IndexOption::Presence { ref field } if field.value == "deleted_at"
    ));
    assert!(matches!(
        indexes[1].options[0].value,
        IndexOption::TextKey {
            ref field,
            ref profile,
        } if field.value == "title" && profile.value == TextKeyProfile::UnicodeFoldV1
    ));
    for index in indexes {
        for option in &index.options {
            assert!(option.span.start() < option.span.end());
        }
    }
}

#[test]
fn parses_ordered_cover_fields_with_source_spans() {
    let source = concat!(
        "contract Board version 1 { entity Card { ",
        "key (organization_id: uuid, card_id: uuid) ",
        "field lane: string<32> field title: string<200> field owner_id: optional<uuid> ",
        "index by_lane (organization_id, lane, card_id) cover (title, owner_id) } }",
    );
    let document = parse_contract(source).expect("covering index parses");
    let Declaration::Entity(entity) = &document.contract.value.declarations[0].value else {
        panic!("entity declaration")
    };
    let index = entity
        .items
        .iter()
        .find_map(|item| match &item.value {
            EntityItem::Index(index) => Some(index),
            _ => None,
        })
        .expect("index declaration");
    let IndexOption::Cover { fields } = &index.options[0].value else {
        panic!("cover option")
    };
    assert_eq!(
        fields
            .iter()
            .map(|field| field.value.as_str())
            .collect::<Vec<_>>(),
        ["title", "owner_id"]
    );
    assert!(index.options[0].span.start() < fields[0].span.start());
    assert!(fields[0].span.end() < fields[1].span.start());
    assert!(fields[1].span.end() <= index.options[0].span.end());
}

#[test]
fn legacy_indexes_retain_an_empty_option_set() {
    let document =
        parse_contract("contract C version 1 { entity E { key (id: uuid) index by_id (id) } }")
            .expect("legacy index");
    let Declaration::Entity(entity) = &document.contract.value.declarations[0].value else {
        panic!("entity declaration")
    };
    let EntityItem::Index(index) = &entity.items[1].value else {
        panic!("index declaration")
    };
    assert!(index.options.is_empty());
}

#[test]
fn operational_index_words_remain_contextual_identifiers() {
    let source = r#"
contract Contextual version 1 {
  entity Document {
    key (id: uuid)
    field presence: string<16>
    field text_key: string<16>
    field binary_utf8_v1: string<16>
    field unicode_fold_v1: string<16>
  }
}
"#;
    let document = parse_contract(source).expect("new words remain contextual identifiers");
    let Declaration::Entity(entity) = &document.contract.value.declarations[0].value else {
        panic!("entity declaration")
    };
    let fields = entity
        .items
        .iter()
        .filter_map(|item| match &item.value {
            EntityItem::Field(field) => Some(field.name.value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        fields,
        ["presence", "text_key", "binary_utf8_v1", "unicode_fold_v1"]
    );
}
