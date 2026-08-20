#![forbid(unsafe_code)]

//! Exact source-to-module activation and strict V5 codec coverage.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    CompiledNamedQueryPlan, NamedQuerySource,
    QUERY_MODULE_FORMAT_VERSION_EXACT_FILTERED_RESULT_SET_V1,
    QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion, generate_go_application_client, generate_mcp_tools,
    generate_python_application_client, generate_rust_application_client,
    generate_typescript_application_client,
};
use riffdb_types::{ExactTextOperatorV1, ExactTextOrderV1, ProjectionProviderPolicyModeV1};

const CONTRACT: &str = r#"
contract ExactUsers version 1 {
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field name: string<128>
    index by_name (organization_id, name, user_id) text_key(name, binary_utf8_v1)
  }
  aggregate Users {
    root User
    partition_by organization_id
    conflict_key (organization_id, user_id)
  }
}

"#;

const QUERY: &str = r#"
query SearchUsers(
  $organization_id: User.organization_id,
  $needle: User.name,
  $limit: Limit = 50,
  $offset: u64 = 0
) {
  many users from User
    where organization_id == $organization_id && name contains $needle
    order by name asc, user_id asc
    take $limit offset $offset
  aggregate total from users { exact_count() as value }
  return Found { users: users { user_id name } total: total { value } }
  outcomes Found
}
"#;

fn module() -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("exact_users").expect("name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchUsers", QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("exact module");
    (contract, module)
}

#[test]
fn optional_typed_filter_rotates_to_v6_without_changing_unfiltered_v5() {
    let contract_source = CONTRACT
        .replace(
            "    field name: string<128>",
            "    field name: string<128>\n    field active: bool",
        )
        .replace(
            "    index by_name (organization_id, name, user_id) text_key(name, binary_utf8_v1)",
            concat!(
                "    index by_name (organization_id, name, user_id) text_key(name, binary_utf8_v1)\n",
                "    index by_active_name (organization_id, active, name, user_id) text_key(name, binary_utf8_v1)"
            ),
        );
    let query = QUERY
        .replace(
            "$needle: User.name,",
            "$needle: User.name,\n  $active: User.active?,",
        )
        .replace(
            "where organization_id == $organization_id && name contains $needle",
            "where organization_id == $organization_id && when $active { active == $active } && name contains $needle",
        )
        .replace("{ user_id name }", "{ user_id name active }");
    let contract = compile_contract_source(&contract_source).expect("filtered contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("filtered_exact_users").expect("name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchUsers", query).expect("query")],
    )
    .expect("candidate");
    let filtered_module =
        QueryModule::compile(candidate, &contract).expect("filtered exact module");
    assert_eq!(
        filtered_module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_EXACT_FILTERED_RESULT_SET_V1
    );
    let exact = filtered_module
        .query("SearchUsers")
        .and_then(|query| query.exact_text_result())
        .expect("filtered exact plan");
    let filter = exact.filter().expect("sealed filter");
    assert_eq!(filter.parameter(), "active");
    assert_eq!(
        exact
            .binding()
            .family()
            .descriptor()
            .state_identity()
            .layout_version()
            .get(),
        3
    );
    let decoded = QueryModule::decode_and_validate(filtered_module.canonical_bytes(), &contract)
        .expect("strict filtered module decode");
    assert_eq!(decoded.canonical_bytes(), filtered_module.canonical_bytes());

    let (_, unfiltered) = module();
    assert_eq!(
        unfiltered.format_version(),
        QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1
    );
}

#[test]
fn exact_source_compiles_to_one_sealed_provider_plan_and_v5_round_trips() {
    let (contract, module) = module();
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1
    );
    let query = module.query("SearchUsers").expect("query");
    let CompiledNamedQueryPlan::ExactTextResultV1(exact) = query.plan() else {
        panic!("exact plan kind");
    };
    assert_eq!(exact.operator(), ExactTextOperatorV1::Contains);
    assert_eq!(exact.order(), ExactTextOrderV1::ValueAscEntityKey);
    assert_eq!(exact.needle_parameter(), "needle");
    assert_eq!(exact.limit_parameter(), "limit");
    assert_eq!(exact.offset_parameter(), "offset");
    assert_eq!(exact.authorization()[0].entity(), "User");
    assert!(exact.cost().scanned_index_rows() >= 4_096);
    assert_eq!(exact.authorization_cost().scanned_index_rows(), 0);
    assert_eq!(exact.authorization_cost().point_reads(), 0);

    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("strict exact module decode");
    assert_eq!(decoded.identity(), module.identity());
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
}

#[test]
fn exact_page_count_and_offset_are_generated_identically_for_every_public_sdk() {
    let (contract, module) = module();
    let generated = [
        generate_rust_application_client(&module, &contract, &[]),
        generate_go_application_client(&module, &contract, &[]),
        generate_typescript_application_client(&module, &contract, &[]),
        generate_python_application_client(&module, &contract, &[]).expect("Python client"),
    ];
    for client in generated {
        assert!(client.contains("SearchUsersParams"));
        assert!(client.contains("SearchUsersResult"));
        assert!(client.to_ascii_lowercase().contains("offset"));
        assert!(client.to_ascii_lowercase().contains("total"));
        assert!(client.contains("SearchUsers"));
        let lowered = client.to_ascii_lowercase();
        for forbidden in ["scan_index", "page_walk"] {
            assert!(
                !lowered.contains(forbidden),
                "generated exact query exposed {forbidden}"
            );
        }
    }

    let tool = generate_mcp_tools(&module)
        .expect("MCP tools")
        .into_iter()
        .find(|tool| tool.operation_name == "SearchUsers")
        .expect("SearchUsers tool");
    let input: serde_json::Value = serde_json::from_str(&tool.input_schema).expect("input schema");
    let output: serde_json::Value =
        serde_json::from_str(&tool.result_schema).expect("output schema");
    assert!(input["properties"]["offset"].is_object());
    assert!(input["properties"]["limit"].is_object());
    assert!(output.to_string().contains("users"));
    assert!(output.to_string().contains("total"));
}

#[test]
fn protected_exact_module_binds_bounded_admission_and_round_trips() {
    let protected_contract = CONTRACT.replace(
        "  aggregate Users {",
        concat!(
            "  row policy UserAccess on User {\n",
            "    allow read when user_id == principal.id\n",
            "  }\n",
            "  aggregate Users {"
        ),
    );
    let contract = compile_contract_source(&protected_contract).expect("protected contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("protected_exact_users").expect("name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchUsers", QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("protected exact module");
    let CompiledNamedQueryPlan::ExactTextResultV1(exact) =
        module.query("SearchUsers").expect("query").plan()
    else {
        panic!("exact plan kind");
    };
    assert_eq!(
        exact.binding().family().descriptor().policy_mode(),
        ProjectionProviderPolicyModeV1::BoundedRowAdmission
    );
    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("strict protected exact module decode");
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
}
