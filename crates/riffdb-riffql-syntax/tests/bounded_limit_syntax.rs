//! ADR-0158 bounded runtime page-limit syntax acceptance.

use riffdb_riffql_syntax::{
    DiagnosticCode, RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1, TypeReference, format_query,
    parse_query,
};

const QUERY: &str = r#"query BoundedPage($org: Item.org_id, $limit: Limit<100> = 50, $after: Cursor?) {
    many items from Item
        where org_id == $org
        order by item_id asc
        take $limit after $after
    return Found { items: items { item_id } }
    outcomes Found
}
"#;

#[test]
fn bounded_limit_round_trips_and_selects_language_v9() {
    let document = parse_query(QUERY).expect("bounded Limit source");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1
    );
    assert!(matches!(
        document.parameters[1].ty.value,
        TypeReference::BoundedLimit(100)
    ));
    let canonical = format_query(&document);
    assert!(canonical.contains("$limit: Limit<100> = 50"));
    assert_eq!(
        format_query(&parse_query(&canonical).expect("canonical source")),
        canonical
    );
}

#[test]
fn bounded_limit_maximum_is_canonical_and_inside_the_global_page_ceiling() {
    for source in [
        QUERY.replace("Limit<100>", "Limit<0>"),
        QUERY.replace("Limit<100>", "Limit<500>"),
        QUERY.replace("Limit<100>", "Limit<010>"),
    ] {
        let diagnostics = parse_query(&source).expect_err("invalid bounded maximum");
        assert_eq!(
            diagnostics.as_slice()[0].code(),
            DiagnosticCode::InvalidToken
        );
        assert_eq!(
            diagnostics.as_slice()[0].summary(),
            "bounded Limit maximum is outside the supported range"
        );
    }
}

#[test]
fn plain_limit_keeps_its_existing_language_identity_and_format() {
    let plain = QUERY.replace("Limit<100>", "Limit");
    let document = parse_query(&plain).expect("plain Limit source");
    assert_ne!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1
    );
    assert!(matches!(
        document.parameters[1].ty.value,
        TypeReference::Limit
    ));
    assert!(format_query(&document).contains("$limit: Limit = 50"));
}

#[test]
fn bounded_limit_rejects_optional_and_collection_wrappers_at_the_type_span() {
    for source in [
        QUERY.replace("Limit<100>", "Limit<100>?"),
        QUERY.replace("Limit<100>", "Set<Limit<100>>"),
    ] {
        let diagnostics = parse_query(&source).expect_err("nested bounded limit");
        assert_eq!(
            diagnostics.as_slice()[0].code(),
            DiagnosticCode::InvalidToken
        );
        assert!(diagnostics.as_slice()[0].span().end > diagnostics.as_slice()[0].span().start);
    }
}
