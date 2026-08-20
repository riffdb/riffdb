//! Exact result-set source syntax and unsafe-combination rejection.

use riffdb_riffql_syntax::{
    AggregateFunction, BinaryOperator, RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1, TypeReference,
    format_query, parse_query,
};

#[test]
fn exact_text_count_and_ordinal_window_are_canonical_and_finite() {
    let source = r#"
query SearchUsers(
    $organization_id: User.organization_id,
    $needle: User.name,
    $limit: Limit = 50,
    $offset: u64 = 0
) {
    many users from User
        where organization_id == $organization_id && name contains $needle
        order by name asc, user_id asc
        take $limit offset $offset

    aggregate total from users {
        exact_count() as value
    }

    return Found {
        users: users { user_id name }
        total: total { value }
    }
    outcomes Found
}
"#;
    let document = parse_query(source).unwrap();
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1
    );
    let binding = &document.body.bindings[0];
    assert!(binding.take.as_ref().unwrap().offset.is_some());
    let riffdb_riffql_syntax::Expression::Binary { right, .. } = &binding.predicate.value else {
        panic!("conjunction");
    };
    let riffdb_riffql_syntax::Expression::Binary { operator, .. } = &right.value else {
        panic!("exact predicate");
    };
    assert_eq!(operator.value, BinaryOperator::Contains);
    assert_eq!(
        document.body.aggregates[0].measures[0].function.value,
        AggregateFunction::ExactCount
    );
    assert!(matches!(
        document.parameters[3].ty.value,
        TypeReference::Named(_)
    ));
    let canonical = format_query(&document);
    assert_eq!(format_query(&parse_query(&canonical).unwrap()), canonical);
}

#[test]
fn cursor_and_ordinal_windows_are_unrepresentable_together() {
    let source = r#"
query Invalid($org: User.organization_id, $cursor: Cursor?, $offset: u64) {
    many users from User where organization_id == $org order by user_id asc
        take 25 after $cursor offset $offset
    return Found { users: users { user_id } }
    outcomes Found
}
"#;
    let diagnostics = parse_query(source).unwrap_err();
    assert_eq!(
        diagnostics.as_slice()[0].summary(),
        "cursor and ordinal windows cannot be combined"
    );
}
