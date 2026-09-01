//! Closed order-family module, codec, and generated-surface coverage.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_ORDER_FAMILY_V1, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion, generate_go_client,
    generate_mcp_tools, generate_python_client, generate_rust_client, generate_typescript_client,
};

const CONTRACT: &str = r#"
contract OrderFamily version 1 {
  enum ExperimentOrder { NameAsc, NameDesc, CreatedDesc, UpdatedDesc }
  entity Experiment {
    key (scope: string<32>, experiment_id: u64)
    field name: string<500>
    field creation_time: i64
    field last_update_time: i64
    index by_name (scope, name, experiment_id)
    index by_created (scope, creation_time, experiment_id)
    index by_updated (scope, last_update_time, experiment_id)
  }
  entity ExperimentTag {
    key (scope: string<32>, experiment_id: u64, tag_key: string<32>)
    field digest: bytes<32>
    index by_tag (scope, tag_key, digest, experiment_id)
    reference experiment (scope, experiment_id) -> Experiment(scope, experiment_id)
  }
  aggregate Experiments { root Experiment partition_by scope conflict_key (scope, experiment_id) }
  aggregate ExperimentTags { root ExperimentTag partition_by scope conflict_key (scope, experiment_id) }
}
"#;

const QUERY: &str = r#"query Experiments(
    $scope: Experiment.scope,
    $tag_key: ExperimentTag.tag_key,
    $digest: ExperimentTag.digest,
    $order: ExperimentOrder,
    $limit: Limit<1000> = 100
) {
    candidates matching: Experiment.experiment_id
        from intersect {
            ExperimentTag.experiment_id using by_tag where scope == $scope && tag_key == $tag_key && digest == $digest,
            ExperimentTag.experiment_id using by_tag where scope == $scope && tag_key == $tag_key && digest == $digest,
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
        take $limit
    return Found { experiments: experiments { experiment_id, name, creation_time, last_update_time } }
    outcomes Found | IntegrityFailure
}"#;

#[test]
fn order_selector_is_closed_on_every_generated_surface_and_round_trips() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("experiments").expect("name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("Experiments", QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &bundle).expect("module");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_ORDER_FAMILY_V1
    );
    let family = module
        .query("Experiments")
        .and_then(|query| query.order_family())
        .expect("family");
    assert_eq!(family.members().len(), 4);
    let decoded =
        QueryModule::decode_and_validate(module.canonical_bytes(), &bundle).expect("round trip");
    assert_eq!(decoded.identity(), module.identity());

    let rust = generate_rust_client(&module, &bundle);
    let go = generate_go_client(&module, &bundle);
    let typescript = generate_typescript_client(&module, &bundle);
    let python = generate_python_client(&module, &bundle).expect("python");
    for generated in [&rust, &go, &python] {
        assert!(generated.contains("ExperimentOrder"));
        assert!(!generated.contains("order_fields"));
        assert!(!generated.contains("index_hint"));
    }
    assert!(typescript.contains(
        "readonly order: \"NameAsc\" | \"NameDesc\" | \"CreatedDesc\" | \"UpdatedDesc\""
    ));
    assert!(!typescript.contains("order_fields"));
    assert!(!typescript.contains("index_hint"));
    let tools = generate_mcp_tools(&module).expect("mcp");
    let schema: serde_json::Value = serde_json::from_str(&tools[0].input_schema).expect("schema");
    assert_eq!(
        schema["properties"]["order"]["enum"]
            .as_array()
            .expect("closed enum")
            .len(),
        4
    );
    assert_eq!(schema["properties"]["order"]["type"], "string");
}
