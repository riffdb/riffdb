#![forbid(unsafe_code)]

//! ADR-0163 generated-surface conformance: callers submit only the named
//! command input and cannot observe or select the compiler-sealed arm.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    generate_go_application_client, generate_mcp_commands, generate_python_application_client,
    generate_rust_application_client, generate_typescript_application_client,
};

fn application() -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(include_str!(
        "../../../fixtures/compiler/command-decision/contract.riff"
    ))
    .expect("command decision contract");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("command_decision").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![],
        )
        .expect("module candidate"),
        &contract,
    )
    .expect("empty query module");
    (contract, module)
}

#[test]
fn generated_clients_expose_no_arm_origin_or_transaction_selector() {
    let (contract, module) = application();
    let generated = [
        generate_rust_application_client(&module, &contract, &[]),
        generate_go_application_client(&module, &contract, &[]),
        generate_typescript_application_client(&module, &contract, &[]),
        generate_python_application_client(&module, &contract, &[]).expect("Python client"),
    ];
    for client in generated {
        let lowered = client.to_ascii_lowercase();
        assert!(lowered.contains("applystate") || lowered.contains("apply_state"));
        for forbidden in [
            "observe_or_initialize",
            "no_effect",
            "arm_selector",
            "state_origin",
            "transaction_option",
            "conflict_policy",
        ] {
            assert!(
                !lowered.contains(forbidden),
                "generated application client exposed {forbidden}"
            );
        }
    }
}

#[test]
fn generated_mcp_schema_contains_only_declared_business_inputs() {
    let (contract, module) = application();
    let command = generate_mcp_commands(&module, &contract)
        .expect("MCP commands")
        .into_iter()
        .find(|command| command.operation_name == "ApplyState")
        .expect("ApplyState command tool");
    let schema: serde_json::Value =
        serde_json::from_str(&command.input_schema).expect("input schema");
    let properties = schema["properties"].as_object().expect("input properties");
    assert_eq!(
        properties.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["change_id", "organization_id", "request_id", "state_id"]
    );
}
