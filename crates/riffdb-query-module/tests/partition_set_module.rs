//! ADR-0175 module identity, codec, and generated bounded-input coverage.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_ir::QUERY_IR_VERSION_PARTITION_SET_EXACT_PREFIX_V1;
use riffdb_query_module::{
    NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_PARTITION_SET_EXACT_PREFIX_V1,
    QUERY_MODULE_FORMAT_VERSION_PARTITION_SET_V1, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion, generate_go_client, generate_mcp_tools,
    generate_python_client, generate_rust_client, generate_typescript_client,
};

const CONTRACT: &str = r#"
contract PartitionSetModule version 1 {
  enum LifecycleStage { Active, Deleted }
  entity Run {
    key (experiment_id: u64, run_id: string<32>)
    field start_time: i64
    field lifecycle_stage: LifecycleStage
    index by_start (experiment_id, start_time, run_id)
    index by_lifecycle_start (experiment_id, lifecycle_stage, start_time, run_id)
  }
  aggregate Runs { root Run partition_by experiment_id conflict_key (experiment_id, run_id) }
}
"#;

const FILTERED_QUERY: &str = r#"
query SearchRunsByLifecycle(
  $experiment_ids: Set<Run.experiment_id, 1000>,
  $lifecycle_stage: Run.lifecycle_stage,
  $limit: Limit<50> = 50,
  $after: Cursor?,
) {
  many runs from Run
    where experiment_id in $experiment_ids && lifecycle_stage == $lifecycle_stage
    order by start_time desc, run_id asc
    take $limit after $after
  return Found { runs: runs { experiment_id run_id start_time lifecycle_stage } }
  outcomes Found
}
"#;

const QUERY: &str = r#"
query SearchRuns(
  $experiment_ids: Set<Run.experiment_id, 1000>,
  $limit: Limit<50> = 50,
  $after: Cursor?,
) {
  many runs from Run
    where experiment_id in $experiment_ids
    order by start_time desc, run_id asc
    take $limit after $after
  return Found { runs: runs { experiment_id run_id start_time } }
  outcomes Found
}
"#;

#[test]
fn exact_prefix_partition_set_selects_additive_plan_and_module_identities() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("partition_set_exact_prefix").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("SearchRunsByLifecycle", FILTERED_QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &bundle).expect("module");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_PARTITION_SET_EXACT_PREFIX_V1
    );
    assert_eq!(
        module
            .query("SearchRunsByLifecycle")
            .expect("query")
            .plan()
            .representative_program()
            .ir_version(),
        QUERY_IR_VERSION_PARTITION_SET_EXACT_PREFIX_V1
    );
    let decoded =
        QueryModule::decode_and_validate(module.canonical_bytes(), &bundle).expect("round trip");
    assert_eq!(decoded.identity(), module.identity());
}

#[test]
fn partition_set_module_round_trips_and_publishes_the_exact_input_bound() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("partition_set").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("SearchRuns", QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &bundle).expect("module");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_PARTITION_SET_V1
    );
    let decoded =
        QueryModule::decode_and_validate(module.canonical_bytes(), &bundle).expect("round trip");
    assert_eq!(decoded.identity(), module.identity());

    let tool = generate_mcp_tools(&module)
        .expect("MCP tools")
        .into_iter()
        .next()
        .expect("query tool");
    let schema: serde_json::Value = serde_json::from_str(&tool.input_schema).expect("input schema");
    assert_eq!(schema["properties"]["experiment_ids"]["type"], "array");
    assert_eq!(schema["properties"]["experiment_ids"]["maxItems"], 1_000);
    assert_eq!(
        schema["properties"]["experiment_ids"]["items"]["type"],
        "string"
    );

    for generated in [
        generate_rust_client(&module, &bundle),
        generate_go_client(&module, &bundle),
        generate_typescript_client(&module, &bundle),
        generate_python_client(&module, &bundle).expect("Python client"),
    ] {
        assert!(generated.contains("experiment_ids") || generated.contains("ExperimentIds"));
        assert!(!generated.contains("partition_cursor"));
        assert!(!generated.contains("index_hint"));
    }
}
