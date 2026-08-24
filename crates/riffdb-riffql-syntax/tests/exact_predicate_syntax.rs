//! Rich exact-result syntax and compatibility identity tests.

use riffdb_riffql_syntax::{
    BinaryOperator, Expression, NullPlacement, RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1,
    RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1, RIFFQL_LANGUAGE_VERSION_NULLABLE_EXACT_ORDER_V1,
    format_query, parse_query,
};

const RICH: &str = r#"
query SearchUsers($organization_id: User.organization_id, $needle: User.email, $states: Set<User.state>, $limit: Limit, $offset: u64) {
  many users from User
    where organization_id == $organization_id
      && email contains $needle
      && state not_in $states
    order by created_at desc, user_id asc
    take $limit offset $offset
  aggregate totals from users { exact_count() as total }
  return Found { users: users { user_id email created_at } totals: totals { total } }
  outcomes Found
}
"#;

#[test]
fn rich_exact_predicate_and_independent_order_rotate_source_identity() {
    let document = parse_query(RICH).expect("rich exact source");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1
    );
    let formatted = format_query(&document);
    assert!(formatted.contains("state not_in $states"));
    let reparsed = parse_query(&formatted).expect("formatted source");
    assert_eq!(
        reparsed.language_version,
        RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1
    );
    assert_eq!(format_query(&reparsed), formatted);

    let predicate = &document.body.bindings[0].predicate.value;
    assert!(contains_not_in(predicate));
}

#[test]
fn narrow_exact_result_source_keeps_v4_identity() {
    let source = RICH
        .replace("      && state not_in $states\n", "")
        .replace("order by created_at desc", "order by email desc");
    let document = parse_query(&source).expect("narrow exact source");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1
    );
}

#[test]
fn explicit_null_placement_rotates_source_identity_and_round_trips() {
    let source = RICH.replace(
        "order by created_at desc, user_id asc",
        "order by created_at desc nulls last, user_id asc",
    );
    let document = parse_query(&source).expect("nullable exact order source");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_NULLABLE_EXACT_ORDER_V1
    );
    assert_eq!(
        document.body.bindings[0].order[0]
            .null_placement
            .as_ref()
            .map(|placement| placement.value),
        Some(NullPlacement::Last)
    );
    assert!(document.body.bindings[0].order[1].null_placement.is_none());

    let formatted = format_query(&document);
    assert!(formatted.contains("created_at desc nulls last, user_id asc"));
    let reparsed = parse_query(&formatted).expect("formatted nullable exact order source");
    assert_eq!(format_query(&reparsed), formatted);
}

#[test]
fn only_closed_null_placement_spellings_are_accepted() {
    let invalid = RICH.replace(
        "order by created_at desc, user_id asc",
        "order by created_at desc nulls middle, user_id asc",
    );
    let diagnostics = parse_query(&invalid).expect_err("unknown placement must fail closed");
    assert!(
        diagnostics
            .as_slice()
            .iter()
            .any(|diagnostic| diagnostic.summary().contains("first or last"))
    );
}

fn contains_not_in(expression: &Expression) -> bool {
    match expression {
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            operator.value == BinaryOperator::NotIn
                || contains_not_in(&left.value)
                || contains_not_in(&right.value)
        }
        Expression::PresenceGuard { predicate, .. } => contains_not_in(&predicate.value),
        Expression::Unary { operand, .. } => contains_not_in(&operand.value),
        Expression::Parameter(_) | Expression::Path(_) | Expression::Literal(_) => false,
    }
}
