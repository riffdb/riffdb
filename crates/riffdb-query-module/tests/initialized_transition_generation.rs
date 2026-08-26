#![forbid(unsafe_code)]

//! ADR-0153 generated-surface conformance: the selected create/replace path
//! remains entirely inside the compiled command.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    generate_go_application_client, generate_mcp_commands, generate_python_application_client,
    generate_rust_application_client, generate_typescript_application_client,
};

fn application() -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(include_str!(
        "../../../fixtures/compiler/initialized-state-transition/contract.riff"
    ))
    .expect("initialized transition contract");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("initialized_state").expect("module name"),
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
fn generated_clients_expose_only_the_named_command_input_and_outcomes() {
    let (contract, module) = application();
    let generated = [
        generate_rust_application_client(&module, &contract, &[]),
        generate_go_application_client(&module, &contract, &[]),
        generate_typescript_application_client(&module, &contract, &[]),
        generate_python_application_client(&module, &contract, &[]).expect("Python client"),
    ];
    for client in generated {
        let lowered = client.to_ascii_lowercase();
        assert!(lowered.contains("putstate") || lowered.contains("put_state"));
        for forbidden in [
            "init_or_mutate",
            "state_path",
            "conflict_target",
            "overwrite_policy",
            "upsert_option",
        ] {
            assert!(
                !lowered.contains(forbidden),
                "generated application client exposed {forbidden}"
            );
        }
    }
}

#[test]
fn generated_mcp_input_has_no_runtime_state_selector() {
    let (contract, module) = application();
    let command = generate_mcp_commands(&module, &contract)
        .expect("MCP commands")
        .into_iter()
        .find(|command| command.operation_name == "PutState")
        .expect("PutState command tool");
    let schema: serde_json::Value =
        serde_json::from_str(&command.input_schema).expect("input schema");
    let properties = schema["properties"].as_object().expect("input properties");
    assert_eq!(
        properties.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["organization_id", "payload", "request_id", "state_id"]
    );
}
