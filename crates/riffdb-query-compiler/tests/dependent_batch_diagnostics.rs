//! `RDB-QP007` must name the condition that failed, not the shape as a whole.
//!
//! The dependent key batch has eleven requirements. Reporting them as one
//! sentence -- "not a complete ordered bounded primary-key traversal" -- tells
//! an author nothing about the edit to make. A real adapter read that message
//! as proof that composing two indexed predicates was unsupported and asked for
//! a new engine capability; its query was two edits from compiling.
//!
//! Each arm below breaks exactly one condition and asserts the message names
//! that condition, so the diagnostic cannot regress to a combined restatement.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_query;
use riffdb_query_ir::SymbolicCatalog;
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = r#"
contract Registry version 1 {
  entity Item {
    key (scope: string<32>, item_id: u64)
    field name: string<100>
  }
  entity ItemTag {
    key (scope: string<32>, item_id: u64, tag_key: string<64>, value_digest: bytes<32>)
    field tag_value: string<500>
    index by_key_value (scope, tag_key, value_digest, item_id)
      text_key(tag_key, binary_utf8_v1)
  }
  aggregate Items {
    root Item
    child ItemTag
    partition_by scope
    conflict_key (scope, item_id)
  }
}
"#;

/// The adapter's original shape: the tag value is a field, so the primary key
/// is complete without it and `key_only` is the single failing condition.
const CONTRACT_VALUE_AS_FIELD: &str = r#"
contract Registry version 1 {
  entity Item {
    key (scope: string<32>, item_id: u64)
    field name: string<100>
  }
  entity ItemTag {
    key (scope: string<32>, item_id: u64, tag_key: string<64>)
    field tag_value: string<500>
    index by_key_value (scope, tag_key, tag_value, item_id)
      text_key(tag_key, binary_utf8_v1)
      text_key(tag_value, binary_utf8_v1)
  }
  aggregate Items {
    root Item
    child ItemTag
    partition_by scope
    conflict_key (scope, item_id)
  }
}
"#;

/// The shape that must compile: two bounded index reads intersected by an
/// ordered dependent batch, then ordered hydration.
const VALID: &str = r#"
query TwoTags(
    $scope: Item.scope,
    $first_key: ItemTag.tag_key,
    $first_digest: ItemTag.value_digest,
    $second_key: ItemTag.tag_key,
    $second_digest: ItemTag.value_digest,
) {
    many first_tags from ItemTag
        where scope == $scope && tag_key == $first_key && value_digest == $first_digest
        order by item_id asc
        take 32

    many second_tags from ItemTag
        where scope == $scope
          && item_id in first_tags.item_id
          && tag_key == $second_key
          && value_digest == $second_digest
        order by item_id asc
        take 32
        else IntegrityFailure

    many items from Item
        where scope == $scope && item_id in second_tags.item_id
        order by item_id asc
        take 32
        else IntegrityFailure

    return Found { items: items { item_id name } }

    outcomes Found | IntegrityFailure
}
"#;

fn diagnose(query: &str) -> Result<(), String> {
    diagnose_with(CONTRACT, query)
}

fn diagnose_with(contract: &str, query: &str) -> Result<(), String> {
    let bundle = compile_contract_source(contract).expect("contract compiles");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
    let document = parse_query(query).expect("query parses");
    match compile_query(&document, &catalog) {
        Ok(_) => Ok(()),
        Err(diagnostics) => Err(diagnostics
            .as_slice()
            .iter()
            .map(|diagnostic| diagnostic.summary().to_owned())
            .collect::<Vec<_>>()
            .join(" | ")),
    }
}

/// `VALID` with every digest comparison rewritten to the non-key value field.
fn value_as_field_query() -> String {
    VALID
        .replace(
            "$first_digest: ItemTag.value_digest,",
            "$first_value: ItemTag.tag_value,",
        )
        .replace(
            "$second_digest: ItemTag.value_digest,",
            "$second_value: ItemTag.tag_value,",
        )
        .replace(
            "&& value_digest == $first_digest",
            "&& tag_value == $first_value",
        )
        .replace(
            "&& value_digest == $second_digest",
            "&& tag_value == $second_value",
        )
}

#[test]
fn the_intersection_shape_compiles() {
    diagnose(VALID).expect("two bounded index reads compose through a dependent batch");
}

#[test]
fn a_non_key_predicate_is_named_as_a_residual_filter() {
    // The exact shape the adapter wrote: match on a value that is a field.
    let message = diagnose_with(CONTRACT_VALUE_AS_FIELD, &value_as_field_query())
        .expect_err("a non-key comparison is refused");
    assert!(
        message.contains("residual filter") && message.contains("digest"),
        "must name the non-key predicate and the repair: {message}"
    );
}

#[test]
fn a_missing_absence_outcome_is_named() {
    let query = VALID.replacen(
        "        take 32\n        else IntegrityFailure\n\n    many items",
        "        take 32\n\n    many items",
        1,
    );
    let message = diagnose(&query).expect_err("a dependent batch without `else` is refused");
    assert!(
        message.contains("absence outcome") && message.contains("else"),
        "must name the missing absence outcome: {message}"
    );
}

#[test]
fn a_descending_dependent_order_is_named() {
    let query = VALID.replacen(
        "          && value_digest == $second_digest\n        order by item_id asc",
        "          && value_digest == $second_digest\n        order by item_id desc",
        1,
    );
    let message = diagnose(&query).expect_err("a descending dependent order is refused");
    assert!(
        message.contains("ascending"),
        "must name the order requirement: {message}"
    );
}

/// Every arm must produce a DIFFERENT message. A split that reported the same
/// sentence for each condition would pass the arms above individually while
/// leaving the original defect in place.
#[test]
fn each_broken_condition_reports_a_distinct_message() {
    let non_key_message =
        diagnose_with(CONTRACT_VALUE_AS_FIELD, &value_as_field_query()).expect_err("refused");
    let no_else = VALID.replacen(
        "        take 32\n        else IntegrityFailure\n\n    many items",
        "        take 32\n\n    many items",
        1,
    );
    let descending = VALID.replacen(
        "          && value_digest == $second_digest\n        order by item_id asc",
        "          && value_digest == $second_digest\n        order by item_id desc",
        1,
    );
    let mut messages = vec![non_key_message];
    messages.extend([&no_else, &descending].map(|query| diagnose(query).expect_err("refused")));
    for (left, right) in [(0, 1), (0, 2), (1, 2)] {
        assert_ne!(
            messages[left], messages[right],
            "conditions {left} and {right} must not share a message"
        );
    }
}
