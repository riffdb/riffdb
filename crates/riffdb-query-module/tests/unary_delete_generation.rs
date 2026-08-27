#![forbid(unsafe_code)]

//! WP-692: every generated application surface preserves atomic unary
//! delete/preimage outcomes and their explicit secret disclosure metadata.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::SecretRevealDestinationV1;
use riffdb_query_module::{
    NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    generate_go_application_client, generate_mcp_commands, generate_python_application_client,
    generate_rust_application_client, generate_typescript_application_client,
};

const CONTRACT: &str = r#"
contract OneTimeTokens version 1 {
  entity OneTimeToken {
    key (organization_id: uuid, token_id: uuid)
    field identifier: string<256>
    field secret value: string<512>
    delete_policy no_inbound
  }
  aggregate Tokens {
    root OneTimeToken
    partition_by organization_id
    conflict_key (organization_id, token_id)
  }
  command ConsumeToken {
    input request_id: uuid
    input organization_id: uuid
    input token_id: uuid
    idempotency_key request_id
    delete OneTimeToken(organization_id, token_id) as token else TokenMissing {}
    return TokenConsumed {
      token_id: token.token_id,
      identifier: token.identifier,
      value: token.value reveals token.value
    }
  }
}
"#;

const QUERY: &str = r#"
query GetToken(
  $organization_id: OneTimeToken.organization_id,
  $token_id: OneTimeToken.token_id,
) {
  one token from OneTimeToken
    where organization_id == $organization_id && token_id == $token_id
    else TokenMissing
  return TokenFound { token_id: token.token_id }
  outcomes TokenFound | TokenMissing
}
"#;

fn application() -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(CONTRACT).expect("one-time-token contract");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("one_time_tokens").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![NamedQuerySource::new("GetToken", QUERY).expect("query source")],
        )
        .expect("module candidate"),
        &contract,
    )
    .expect("query module");
    (contract, module)
}

#[test]
fn generated_clients_publish_the_exact_command_secret_output() {
    let (contract, module) = application();
    let command = contract
        .commands()
        .iter()
        .find(|command| command.name() == "ConsumeToken")
        .expect("consume command");
    assert_eq!(command.secret_reveals().len(), 1);
    assert!(matches!(
        command.secret_reveals()[0].destination(),
        SecretRevealDestinationV1::OutcomeField { .. }
    ));

    let rust = generate_rust_application_client(&module, &contract, &[]);
    assert!(rust.contains("CONSUME_TOKEN_SECRET_OUTPUTS"));
    assert!(rust.contains("(\"TokenConsumed\", \"value\", \"OneTimeToken\", \"value\")"));
    assert!(rust.contains("ConsumeTokenOutcome([REDACTED])"));

    let go = generate_go_application_client(&module, &contract, &[]);
    assert!(go.contains("ConsumeTokenSecretOutputs"));
    assert!(go.contains("Outcome: \"TokenConsumed\", Field: \"value\", Entity: \"OneTimeToken\", SourceField: \"value\""));
    assert!(go.contains("ConsumeTokenTokenConsumed{<secret outputs redacted>}"));

    let typescript = generate_typescript_application_client(&module, &contract, &[]);
    assert!(typescript.contains("CONSUME_TOKEN_SECRET_OUTPUTS"));
    assert!(typescript.contains("outcome: \"TokenConsumed\", field: \"value\", entity: \"OneTimeToken\", sourceField: \"value\""));
    assert!(typescript.contains("redactConsumeTokenOutcome"));

    let python =
        generate_python_application_client(&module, &contract, &[]).expect("Python client");
    assert!(python.contains("CONSUME_TOKEN_SECRET_OUTPUTS"));
    assert!(python.contains("(\"TokenConsumed\", \"value\", \"OneTimeToken\", \"value\")"));
    assert!(python.contains("ConsumeTokenTokenConsumed(<secret outputs redacted>)"));
}

#[test]
fn generated_mcp_schema_marks_the_exact_secret_outcome_without_values() {
    let (contract, module) = application();
    let command = generate_mcp_commands(&module, &contract)
        .expect("MCP commands")
        .into_iter()
        .find(|command| command.operation_name == "ConsumeToken")
        .expect("consume MCP schema");
    let result: serde_json::Value =
        serde_json::from_str(&command.result_schema).expect("result schema");
    assert_eq!(
        result["x-riffdb-secretOutputs"],
        serde_json::json!([{
            "outcome": "TokenConsumed",
            "field": "value",
            "entity": "OneTimeToken",
            "sourceField": "value"
        }]),
    );
    assert!(!command.result_schema.contains("one-time-secret"));
}
