//! ADR-0175 bounded-set syntax and version identity coverage.

use riffdb_riffql_syntax::{
    DiagnosticCode, RIFFQL_LANGUAGE_VERSION_PARTITION_SET_V1, TypeReference, format_query,
    parse_query,
};

const QUERY: &str = r#"query SearchRuns(
    $experiment_ids: Set<Run.experiment_id, 65535>,
    $limit: Limit<50> = 50,
    $after: Cursor?,
) {
    many runs from Run
        where experiment_id in $experiment_ids
        order by start_time desc, run_id asc
        take $limit after $after
        else IntegrityFailure

    return Found { runs: runs { experiment_id run_id start_time } }
    outcomes Found | IntegrityFailure
}
"#;

#[test]
fn explicitly_bounded_set_is_canonical_and_selects_v13() {
    let document = parse_query(QUERY).expect("bounded partition set parses");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_PARTITION_SET_V1
    );
    assert!(matches!(
        document.parameters[0].ty.value,
        TypeReference::BoundedSet {
            maximum: 65_535,
            ..
        }
    ));
    let canonical = format_query(&document);
    assert!(canonical.contains("Set<Run.experiment_id, 65535>"));
    assert_eq!(
        format_query(&parse_query(&canonical).expect("reparse")),
        canonical
    );
}

#[test]
fn bounded_set_maximum_is_positive_canonical_and_u16_bounded() {
    for (maximum, summary) in [
        ("0", "canonical integer from 1 through 65535"),
        ("01", "canonical integer from 1 through 65535"),
        ("65536", "canonical integer from 1 through 65535"),
    ] {
        let source = QUERY.replace("65535", maximum);
        let diagnostics = parse_query(&source).expect_err("invalid maximum must fail");
        assert_eq!(
            diagnostics.as_slice()[0].code(),
            DiagnosticCode::InvalidToken
        );
        assert!(diagnostics.as_slice()[0].summary().contains(summary));
        let span = diagnostics.as_slice()[0].span();
        assert!(span.start < span.end);
    }
}

#[test]
fn legacy_set_keeps_its_predecessor_language_identity() {
    let legacy = QUERY.replace("Set<Run.experiment_id, 65535>", "Set<Run.experiment_id>");
    let document = parse_query(&legacy).expect("legacy set parses");
    assert_ne!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_PARTITION_SET_V1
    );
    assert!(matches!(
        document.parameters[0].ty.value,
        TypeReference::Set(_)
    ));
}
