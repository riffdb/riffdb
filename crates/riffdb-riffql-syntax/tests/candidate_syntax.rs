//! Candidate-set syntax acceptance and refusal coverage.

use riffdb_riffql_syntax::{
    CandidateSetExpression, RIFFQL_LANGUAGE_VERSION_BOUNDED_RESULT_PIPELINE_V1, format_query,
    parse_query,
};

const QUERY: &str = include_str!(
    "../../../fixtures/riffql/bounded-filtered-result-v1/queries/two_tag_before_root_order.riffq"
);

#[test]
fn candidate_intersection_is_bounded_and_canonical() {
    let document = parse_query(QUERY).expect("candidate query parses");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_BOUNDED_RESULT_PIPELINE_V1
    );
    let candidate = &document.body.candidates[0];
    assert_eq!(candidate.within, 65_535);
    assert!(matches!(
        candidate.expression,
        CandidateSetExpression::Intersection(_)
    ));
    let canonical = format_query(&document);
    assert_eq!(
        format_query(&parse_query(&canonical).expect("canonical query reparses")),
        canonical
    );
}

#[test]
fn candidate_bounds_and_source_count_fail_closed() {
    let excessive_bound = QUERY.replace("within 65535", "within 65536");
    assert!(parse_query(&excessive_bound).is_err());

    let source = "ExperimentTag.experiment_id using by_tag_digest where scope == $scope";
    let nine = std::iter::repeat_n(source, 9).collect::<Vec<_>>().join(",");
    let excessive_sources = QUERY.replacen(
        "intersect {\n            ExperimentTag.experiment_id using by_tag_digest\n                where scope == $scope\n                    && tag_key == $first_key\n                    && value_digest == $first_digest,\n            ExperimentTag.experiment_id using by_tag_digest\n                where scope == $scope\n                    && tag_key == $second_key\n                    && value_digest == $second_digest,\n        }",
        &format!("intersect {{ {nine} }}"),
        1,
    );
    assert!(parse_query(&excessive_sources).is_err());
}
