#![forbid(unsafe_code)]

//! Candidate-pipeline module identity and safe generated-surface coverage.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_BOUNDED_RESULT_PIPELINE_V1, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion, generate_go_application_client,
    generate_mcp_tools, generate_python_application_client, generate_rust_application_client,
    generate_typescript_application_client,
};

const CONTRACT: &str = r#"
contract CandidateModule version 1 {
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

fn module() -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("candidate_search").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("MatchTags", QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("module");
    (contract, module)
}

#[test]
fn candidate_module_uses_v14_and_round_trips_exactly() {
    let (contract, module) = module();
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_BOUNDED_RESULT_PIPELINE_V1
    );
    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("strict candidate module decode");
    assert_eq!(decoded.identity(), module.identity());
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
}

#[test]
fn generated_surfaces_expose_business_parameters_but_no_candidate_structure() {
    let (contract, module) = module();
    let generated = [
        generate_rust_application_client(&module, &contract, &[]),
        generate_go_application_client(&module, &contract, &[]),
        generate_typescript_application_client(&module, &contract, &[]),
        generate_python_application_client(&module, &contract, &[]).expect("Python client"),
    ];
    for source in generated {
        let source = source.to_ascii_lowercase();
        assert!(source.contains("matchtags"));
        assert!(source.contains("digest"));
        for forbidden in [
            "candidatebinding",
            "intersect",
            "by_tag_digest",
            "maximum_candidates",
        ] {
            assert!(!source.contains(forbidden), "exposed {forbidden}");
        }
    }
    let tools = generate_mcp_tools(&module).expect("MCP tools");
    assert_eq!(tools.len(), 1);
    for forbidden in ["candidatebinding", "intersect", "by_tag_digest"] {
        assert!(!tools[0].input_schema.contains(forbidden));
        assert!(!tools[0].result_schema.contains(forbidden));
    }
}
