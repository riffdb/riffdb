//! Canonical syntax coverage for compiler-lowered relationship existence.
// req: OQ-117

use riffdb_riffql_syntax::{
    RIFFQL_LANGUAGE_VERSION_RELATIONAL_OPERATORS_V1, format_query, parse_query,
};

const QUERY: &str = r#"query Tagged(
    $scope: Experiment.scope,
    $key: ExperimentTag.tag_key,
    $limit: Limit<100> = 50
) {
    many experiments from Experiment
        where scope == $scope
        not exists ExperimentTag using by_tag
            where scope == $scope && tag_key == $key
        order by updated_at desc, experiment_id asc
        take $limit
        else IntegrityFailure
    return Found { experiments: experiments { experiment_id } }
    outcomes Found | IntegrityFailure
}"#;

#[test]
fn existence_clause_is_canonical_relational_operator_syntax() {
    let document = parse_query(QUERY).expect("existence syntax");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_RELATIONAL_OPERATORS_V1
    );
    let existence = document.body.bindings[0]
        .existence
        .as_ref()
        .expect("existence clause");
    assert!(existence.negated);
    assert_eq!(existence.junction.value.as_str(), "ExperimentTag");
    assert_eq!(existence.access.value.as_str(), "by_tag");
    let formatted = format_query(&document);
    assert_eq!(
        format_query(&parse_query(&formatted).expect("formatted query")),
        formatted
    );
}
