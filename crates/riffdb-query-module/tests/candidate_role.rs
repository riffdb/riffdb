#![forbid(unsafe_code)]

//! Candidate-aware application-role authority remains finite per access.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    ApplicationSourceManifest, NamedQuerySource, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion, compile_application_role,
};

const CONTRACT: &str = r#"
contract CandidateRole version 1 {
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

const MANIFEST: &str = r#"{
  "application": "candidate-search",
  "contract": {"lineage": "CandidateRole", "source": "contract.riff", "version": 1},
  "generation": {"go": "generated/go/client.go", "mcp": "generated/mcp/tools.json", "python": "generated/python/client.py", "rust": "generated/rust/client.rs", "typescript": "generated/typescript/client.ts"},
  "migrations": [],
  "query_modules": [{"name": "candidate_search", "queries": [{"name": "MatchTags", "source": "queries/match_tags.riffq"}], "version": 1}],
  "reactive_modules": [],
  "roles": [{"agent_subscriptions": [], "commands": [], "environment": "development", "event_streams": [], "name": "Reader", "queries": ["MatchTags"], "row_policies": [], "tenant_scope": "global", "watch_queries": []}],
  "schema": "riffdb.application-source/v6",
  "seed_inputs": []
}"#;

fn compile(query: &str) -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("candidate_search").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![NamedQuerySource::new("MatchTags", query).expect("query")],
        )
        .expect("candidate"),
        &contract,
    )
    .expect("module");
    (contract, module)
}

#[test]
fn two_maximum_candidate_sources_receive_one_per_access_u16_authority() {
    let (contract, module) = compile(QUERY);
    let exact = ApplicationSourceManifest::parse(MANIFEST)
        .expect("source manifest")
        .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
        .expect("exact manifest");
    let role = compile_application_role(
        &exact,
        "Reader",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("candidate role");
    assert_eq!(role.internal_grant().max_scan_rows().get(), 65_535);

    let narrower_query = QUERY.replace("within 65535", "within 65534");
    let (contract, narrower_module) = compile(&narrower_query);
    let narrower_exact = ApplicationSourceManifest::parse(MANIFEST)
        .expect("source manifest")
        .exact_manifest_v2(&contract, std::slice::from_ref(&narrower_module), &[])
        .expect("exact manifest");
    let narrower_role = compile_application_role(
        &narrower_exact,
        "Reader",
        None,
        &contract,
        std::slice::from_ref(&narrower_module),
    )
    .expect("narrower candidate role");
    assert_eq!(narrower_role.internal_grant().max_scan_rows().get(), 65_534);
    assert_ne!(role.identity(), narrower_role.identity());
}
