//! Checked candidate binding identity and reference-algebra coverage.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_ir::{
    CandidateSetOperatorV1, CandidateSetRefusal, QUERY_IR_VERSION_BOUNDED_RESULT_PIPELINE_V1,
    QueryDiagnosticCode, SymbolicCatalog, evaluate_candidate_set_v1, resolve_query_surface,
};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = r#"
contract CandidateResolution version 1 {
  entity Experiment {
    key (scope: string<32>, experiment_id: u64)
    field last_update_time: i64
    index by_updated (scope, last_update_time, experiment_id)
  }
  entity ExperimentTag {
    key (scope: string<32>, experiment_id: u64, tag_key: string<250>)
    field value_digest: bytes<32>
    index by_tag_digest (scope, tag_key, value_digest, experiment_id)
    reference experiment (scope, experiment_id) -> Experiment(scope, experiment_id)
  }
  aggregate Experiments {
    root Experiment
    partition_by scope
    conflict_key (scope, experiment_id)
  }
  aggregate ExperimentTags {
    root ExperimentTag
    partition_by scope
    conflict_key (scope, experiment_id)
  }
}
"#;

const QUERY: &str = r#"query MatchTags(
    $scope: Experiment.scope,
    $key: ExperimentTag.tag_key,
    $digest: ExperimentTag.value_digest,
    $limit: Limit<50000> = 1000
) {
    candidates matching: Experiment.experiment_id
        from intersect {
            ExperimentTag.experiment_id using by_tag_digest
                where scope == $scope && tag_key == $key && value_digest == $digest,
            ExperimentTag.experiment_id using by_tag_digest
                where scope == $scope && tag_key == $key && value_digest == $digest,
        }
        within 65535
        else IntegrityFailure
    many experiments from Experiment
        where scope == $scope && experiment_id in matching
        order by last_update_time desc, experiment_id asc
        take $limit
        else IntegrityFailure
    return Found { experiments: experiments { experiment_id, last_update_time } }
    outcomes Found | IntegrityFailure
}"#;

#[test]
fn candidate_binding_resolves_to_v14_identity() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let document = parse_query(QUERY).expect("syntax");
    let resolved = resolve_query_surface(&document, &catalog).expect("checked candidate");
    assert_eq!(
        resolved.ir_version(),
        QUERY_IR_VERSION_BOUNDED_RESULT_PIPELINE_V1
    );
    let candidate = &resolved.candidates()[0];
    assert_eq!(candidate.operator(), CandidateSetOperatorV1::Intersection);
    assert_eq!(candidate.sources().len(), 2);
    assert_eq!(candidate.maximum_distinct_keys(), 65_535);
}

#[test]
fn reference_algebra_deduplicates_and_refuses_without_partial_output() {
    let sources = vec![
        vec![b"a".to_vec(), b"b".to_vec(), b"b".to_vec()],
        vec![b"b".to_vec(), b"c".to_vec()],
    ];
    assert_eq!(
        evaluate_candidate_set_v1(CandidateSetOperatorV1::Intersection, &sources, 3, 16),
        Ok(vec![b"b".to_vec()])
    );
    assert_eq!(
        evaluate_candidate_set_v1(CandidateSetOperatorV1::Union, &sources, 2, 16),
        Err(CandidateSetRefusal::DistinctKeyLimit)
    );
    assert_eq!(
        evaluate_candidate_set_v1(CandidateSetOperatorV1::Difference, &sources, 3, 16),
        Ok(vec![b"a".to_vec()])
    );
    assert_eq!(
        evaluate_candidate_set_v1(CandidateSetOperatorV1::Single, &[sources[0].clone()], 3, 1),
        Err(CandidateSetRefusal::KeyByteLimit)
    );
}

#[test]
fn candidate_must_have_exactly_one_root_consumer() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let unused = QUERY.replace("experiment_id in matching", "experiment_id == 1");
    let diagnostics = resolve_query_surface(&parse_query(&unused).expect("syntax"), &catalog)
        .expect_err("unused candidate must reject");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), QueryDiagnosticCode::InvalidPath);
    assert!(diagnostic.primary().end > diagnostic.primary().start);

    let duplicated = QUERY.replace(
        "scope == $scope && experiment_id in matching",
        "scope == $scope && experiment_id in matching && experiment_id in matching",
    );
    let diagnostics = resolve_query_surface(&parse_query(&duplicated).expect("syntax"), &catalog)
        .expect_err("duplicated candidate consumer must reject");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        QueryDiagnosticCode::InvalidPath
    );
}
