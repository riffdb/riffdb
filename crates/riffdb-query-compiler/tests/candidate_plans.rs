//! Candidate-source planning and root-order regression coverage.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{PlannerDiagnosticCode, compile_order_query_family, compile_query};
use riffdb_query_ir::{QueryPredicateValue, QueryRowLimit, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = r#"
contract CandidatePlans version 1 {
  enum ExperimentOrder { NameAsc, NameDesc, CreatedDesc, UpdatedDesc }
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
  aggregate Experiments { root Experiment partition_by scope conflict_key (scope, experiment_id) }
  aggregate ExperimentTags { root ExperimentTag partition_by scope conflict_key (scope, experiment_id) }
}
"#;

const ORDER_QUERY: &str = r#"query MatchTagsOrdered(
    $scope: Experiment.scope,
    $key: ExperimentTag.tag_key,
    $digest: ExperimentTag.value_digest,
    $order: ExperimentOrder,
    $limit: Limit<1000> = 1000
) {
    candidates matching: Experiment.experiment_id
        from intersect {
            ExperimentTag.experiment_id using by_tag_digest where scope == $scope && tag_key == $key && value_digest == $digest,
            ExperimentTag.experiment_id using by_tag_digest where scope == $scope && tag_key == $key && value_digest == $digest,
        }
        within 65535 else IntegrityFailure
    many experiments from Experiment
        where scope == $scope && experiment_id in matching
        order by $order {
            NameAsc: name asc, experiment_id asc;
            NameDesc: name desc, experiment_id asc;
            CreatedDesc: creation_time desc, experiment_id asc;
            UpdatedDesc: last_update_time desc, experiment_id asc;
        }
        take $limit else IntegrityFailure
    return Found { experiments: experiments { experiment_id, name, creation_time, last_update_time } }
    outcomes Found | IntegrityFailure
}"#;

const QUERY: &str = r#"query MatchTags(
    $scope: Experiment.scope,
    $key: ExperimentTag.tag_key,
    $digest: ExperimentTag.value_digest,
    $limit: Limit<5000> = 1000
) {
    candidates matching: Experiment.experiment_id
        from intersect {
            ExperimentTag.experiment_id using by_tag_digest where scope == $scope && tag_key == $key && value_digest == $digest,
            ExperimentTag.experiment_id using by_tag_digest where scope == $scope && tag_key == $key && value_digest == $digest,
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
fn candidate_sources_precede_the_root_consumer() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let document = parse_query(QUERY).expect("query");
    let plan = compile_query(&document, &catalog).expect("candidate plan");
    assert_eq!(plan.steps().len(), 3);
    assert!(matches!(
        plan.steps()[0].row_limit(),
        QueryRowLimit::CandidateComplete { maximum: 65_535 }
    ));
    assert!(
        plan.steps()[2]
            .predicates()
            .iter()
            .any(|predicate| matches!(
                predicate.value(),
                QueryPredicateValue::CandidateBinding { name } if name == "matching"
            ))
    );
}

#[test]
fn candidate_root_order_is_a_complete_contract_enum_family() {
    let contract = CONTRACT
        .replace("field last_update_time: i64", "field name: string<500>\n    field creation_time: i64\n    field last_update_time: i64")
        .replace("index by_updated (scope, last_update_time, experiment_id)", "index by_name (scope, name, experiment_id)\n    index by_created (scope, creation_time, experiment_id)\n    index by_updated (scope, last_update_time, experiment_id)");
    let bundle = compile_contract_source(&contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_order_query_family(&parse_query(ORDER_QUERY).expect("query"), &catalog)
        .expect("family");
    assert_eq!(family.members().len(), 4);
    assert_ne!(
        family.members()[0].program().identity(),
        family.members()[1].program().identity()
    );
    assert!(
        family
            .members()
            .iter()
            .all(|member| member.program().surface().has_bounded_result_pipeline())
    );
}

#[test]
fn difference_rejects_a_non_root_positive_universe_with_its_source_span() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query = QUERY.replace(
        r#"from intersect {
            ExperimentTag.experiment_id using by_tag_digest where scope == $scope && tag_key == $key && value_digest == $digest,
            ExperimentTag.experiment_id using by_tag_digest where scope == $scope && tag_key == $key && value_digest == $digest,
        }"#,
        r#"from difference {
            ExperimentTag.experiment_id using by_tag_digest where scope == $scope && tag_key == $key && value_digest == $digest;
            ExperimentTag.experiment_id using by_tag_digest where scope == $scope && tag_key == $key && value_digest == $digest,
        }"#,
    );
    let diagnostics = compile_query(&parse_query(&query).expect("query"), &catalog)
        .expect_err("non-root difference universe must reject");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::CandidateInvalid);
    assert_eq!(diagnostic.code().as_str(), "RDB-QP011");
    assert!(diagnostic.primary().end > diagnostic.primary().start);
    assert_eq!(
        diagnostic.summary(),
        "candidate difference requires one policy-filtered partition-complete positive root universe"
    );
}
