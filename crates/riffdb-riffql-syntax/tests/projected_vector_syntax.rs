#![forbid(unsafe_code)]

//! Compiler-owned projected-vector source and freshness syntax.

use riffdb_riffql_syntax::{
    ProjectedFreshness, RIFFQL_LANGUAGE_VERSION_PROJECTED_VECTOR_V1, format_query, parse_query,
};

const AVAILABLE: &str = r#"
query Similar($org: Document.org_id, $vector: Document.embedding) {
    source projected Document.embedding
    freshness available

    many rows from Document
        where org_id == $org
        nearest(embedding, $vector, 10)
    return Found { rows: rows { title } }
    outcomes Found
}
"#;

#[test]
fn projected_source_and_freshness_round_trip_canonically() {
    let document = parse_query(AVAILABLE).expect("projected query parses");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_PROJECTED_VECTOR_V1
    );
    let source = document.projected_source.as_ref().expect("source");
    assert_eq!(source.path.value.0.len(), 2);
    assert_eq!(source.path.value.0[0].value.as_str(), "Document");
    assert_eq!(source.path.value.0[1].value.as_str(), "embedding");
    assert_eq!(source.freshness.value, ProjectedFreshness::Available);
    let canonical = format_query(&document);
    assert_eq!(format_query(&parse_query(&canonical).unwrap()), canonical);
}

#[test]
fn causal_and_bounded_freshness_are_literal_and_bounded() {
    let causal = AVAILABLE.replace(
        "freshness available",
        "freshness causal inherit_session_commit true max_wait_ms 500",
    );
    assert!(matches!(
        parse_query(&causal)
            .unwrap()
            .projected_source
            .unwrap()
            .freshness
            .value,
        ProjectedFreshness::Causal {
            inherit_session_commit: true,
            max_wait_ms: 500
        }
    ));
    let bounded = AVAILABLE.replace("freshness available", "freshness bounded max_lag_ms 1000");
    assert!(matches!(
        parse_query(&bounded)
            .unwrap()
            .projected_source
            .unwrap()
            .freshness
            .value,
        ProjectedFreshness::Bounded { max_lag_ms: 1000 }
    ));

    for invalid in [
        AVAILABLE.replace(
            "freshness available",
            "freshness causal inherit_session_commit true max_wait_ms 0",
        ),
        AVAILABLE.replace(
            "freshness available",
            "freshness causal inherit_session_commit true max_wait_ms 30001",
        ),
        AVAILABLE.replace("freshness available", "freshness bounded max_lag_ms 0"),
        AVAILABLE.replace(
            "freshness available",
            "freshness bounded max_lag_ms 86400001",
        ),
    ] {
        assert!(parse_query(&invalid).is_err());
    }
}
