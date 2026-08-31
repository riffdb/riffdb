//! Closed tokenized text syntax and source-span coverage.

use riffdb_riffql_syntax::{
    RIFFQL_LANGUAGE_VERSION_TOKENIZED_TEXT_V1, TokenizedMatchKind, TokenizedRanking, format_query,
    parse_query,
};

fn source(kind: &str) -> String {
    format!(
        "query search_docs($org: Document.organization_id, $query: Document.title, $limit: Limit<20>, $cursor: Cursor) {{\n    many docs from Document where docs.organization_id == $org\n        matching(search, {kind}, $query)\n        order by docs.id asc\n        take $limit after $cursor\n    return {{ id: docs.id }}\n}}"
    )
}

#[test]
fn fixed_ranking_is_compile_time_and_canonical() {
    let ranked = source("conjunction").replace("$query)", "$query, riff_bm25_v1)");
    let document = parse_query(&ranked).unwrap();
    assert_eq!(
        document.body.bindings[0]
            .tokenized_match
            .as_ref()
            .unwrap()
            .ranking,
        TokenizedRanking::RiffBm25V1
    );
    let formatted = format_query(&document);
    assert_eq!(format_query(&parse_query(&formatted).unwrap()), formatted);
    assert!(parse_query(&ranked.replace("riff_bm25_v1", "caller_score")).is_err());
}

#[test]
fn all_four_compile_time_match_shapes_parse_and_format_idempotently() {
    for (source_kind, expected) in [
        ("conjunction", TokenizedMatchKind::Conjunction),
        ("disjunction", TokenizedMatchKind::Disjunction),
        ("phrase", TokenizedMatchKind::Phrase),
        ("proximity, 5", TokenizedMatchKind::Proximity(5)),
    ] {
        let document = parse_query(&source(source_kind)).unwrap();
        assert_eq!(
            document.language_version,
            RIFFQL_LANGUAGE_VERSION_TOKENIZED_TEXT_V1
        );
        assert_eq!(
            document.body.bindings[0]
                .tokenized_match
                .as_ref()
                .unwrap()
                .kind
                .value,
            expected
        );
        let formatted = format_query(&document);
        assert_eq!(format_query(&parse_query(&formatted).unwrap()), formatted);
    }
}

#[test]
fn runtime_structure_and_unbounded_proximity_are_not_expressible() {
    let runtime_kind = source("$operator");
    assert!(parse_query(&runtime_kind).is_err());
    let runtime_distance = source("proximity, $distance");
    assert!(parse_query(&runtime_distance).is_err());
    let zero = source("proximity, 0");
    assert!(parse_query(&zero).is_err());
    let excessive = source("proximity, 1025");
    assert!(parse_query(&excessive).is_err());
}
