#![forbid(unsafe_code)]

//! WP-562: generated clients preserve compiler-owned collection bounds.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    generate_go_application_client, generate_mcp_commands, generate_python_application_client,
    generate_rust_application_client, generate_typescript_application_client,
};

const CONTRACT: &str = include_str!("../../../fixtures/contracts/bulk/openfga-tuples.riff");
const QUERY: &str = r#"
query GetTuple(
    $store_id: Tuple.store_id,
    $tuple_id: Tuple.tuple_id,
) {
    one tuple from Tuple
        where store_id == $store_id
            && tuple_id == $tuple_id
        else NotFound

    return Found {
        tuple: tuple { store_id tuple_id object relation subject }
    }

    outcomes Found | NotFound
}
"#;

fn application() -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(CONTRACT).expect("bulk contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("openfga_bulk").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("GetTuple", QUERY).expect("query source")],
    )
    .expect("module candidate");
    let module = QueryModule::compile(candidate, &contract).expect("query module");
    (contract, module)
}

fn aggregate_budget_application() -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(include_str!(
        "../../../fixtures/compiler/aggregate-collection-budget/contract.riff"
    ))
    .expect("aggregate-budget contract");
    let query = NamedQuerySource::new(
        "GetPolicyMutation",
        r#"
query GetPolicyMutation(
    $organization_id: PolicyMutation.organization_id,
    $mutation_id: PolicyMutation.mutation_id,
) {
    one mutation from PolicyMutation
        where organization_id == $organization_id
            && mutation_id == $mutation_id
        else NotFound
    return Found { mutation: mutation { organization_id mutation_id relation } }
    outcomes Found | NotFound
}
"#,
    )
    .expect("query source");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("policy_mutation_budget").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![query],
        )
        .expect("module candidate"),
        &contract,
    )
    .expect("query module");
    (contract, module)
}

#[test]
fn generated_collection_surfaces_preflight_the_exact_compiled_bounds() {
    let (contract, module) = application();
    let command = contract
        .commands()
        .iter()
        .find(|command| command.name() == "WriteTuples")
        .expect("command");
    let expansion = command.collection_expansion().expect("collection plan");
    assert_eq!(expansion.minimum_elements(), 1);
    assert_eq!(expansion.maximum_elements(), 128);

    let rust = generate_rust_application_client(&module, &contract, &[]);
    assert!(rust.contains("self.tuples.is_empty() || self.tuples.len() > 128"));
    assert!(rust.contains("GeneratedCommandError::InvalidInputShape"));

    let go = generate_go_application_client(&module, &contract, &[]);
    assert!(go.contains("len(input.Tuples) < 1 || len(input.Tuples) > 128"));
    assert!(
        go.contains("errors.New(\"invalid bounded collection length for WriteTuples.tuples\")")
    );

    let typescript = generate_typescript_application_client(&module, &contract, &[]);
    assert!(typescript.contains(r#""kind":"list""#));
    assert!(typescript.contains(r#""minimum":1"#));
    assert!(typescript.contains(r#""maximum":128"#));

    let python =
        generate_python_application_client(&module, &contract, &[]).expect("Python client");
    assert!(python.contains("if not 1 <= len(input.tuples) <= 128:"));
    assert!(
        python.contains("ValueError(\"invalid bounded collection length for WriteTuples.tuples\")")
    );

    let command_tool = generate_mcp_commands(&module, &contract)
        .expect("MCP commands")
        .into_iter()
        .find(|tool| tool.operation_name == "WriteTuples")
        .expect("WriteTuples tool");
    let schema: serde_json::Value =
        serde_json::from_str(&command_tool.input_schema).expect("input schema");
    assert_eq!(schema["properties"]["tuples"]["minItems"], 1);
    assert_eq!(schema["properties"]["tuples"]["maxItems"], 128);
}

#[test]
fn generated_collection_surfaces_expose_no_generic_write_escape_hatch() {
    let (contract, module) = application();
    let generated = [
        generate_rust_application_client(&module, &contract, &[]),
        generate_go_application_client(&module, &contract, &[]),
        generate_typescript_application_client(&module, &contract, &[]),
        generate_python_application_client(&module, &contract, &[]).expect("Python client"),
    ];
    for client in generated {
        let lowered = client.to_ascii_lowercase();
        for forbidden in [
            "begin_transaction",
            "raw_mutation",
            "delete_entity",
            "scan_index",
            "entity_type_id",
        ] {
            assert!(
                !lowered.contains(forbidden),
                "generated application client exposed {forbidden}"
            );
        }
    }
}

#[test]
// req: AAA-001, AAA-003, AAA-009, AAA-010, BLK-014, BLK-019
fn aggregate_collection_annotation_is_exact_bounded_and_array_only() {
    let (contract, module) = aggregate_budget_application();

    let rust = generate_rust_application_client(&module, &contract, &[]);
    assert!(rust.contains("wire_canonical_value_encoded_len(&encoded)?"));
    assert!(rust.contains("aggregate_element_bytes > 900000"));

    let go = generate_go_application_client(&module, &contract, &[]);
    assert!(go.contains("riffdb.CanonicalValueEncodedLength(encoded)"));
    assert!(go.contains("length > 900000-aggregateElementBytes"));

    let typescript = generate_typescript_application_client(&module, &contract, &[]);
    assert!(typescript.contains(r#""aggregateCanonicalElementBytes":900000"#));

    let python =
        generate_python_application_client(&module, &contract, &[]).expect("Python client");
    assert!(python.contains("canonical_value_encoded_length(encode_value(item"));
    assert!(python.contains("aggregate_element_bytes > 900000"));

    let command_tool = generate_mcp_commands(&module, &contract)
        .expect("MCP commands")
        .into_iter()
        .find(|tool| tool.operation_name == "WritePolicyMutations")
        .expect("WritePolicyMutations tool");
    let schema: serde_json::Value =
        serde_json::from_str(&command_tool.input_schema).expect("input schema");
    assert_eq!(
        schema["properties"]["mutations"]["x-riffdb-aggregateCanonicalElementBytes"],
        900_000
    );
    assert_eq!(schema["properties"]["mutations"]["type"], "array");
    assert!(
        schema["properties"]["request_id"]
            .get("x-riffdb-aggregateCanonicalElementBytes")
            .is_none()
    );
}

#[test]
// req: AAA-001, AAA-002, AAA-003, AAA-009, AAA-010, BLK-014, BLK-019
fn generated_aggregate_preflight_causes_are_closed_and_transport_free() {
    let (contract, module) = aggregate_budget_application();

    let rust = generate_rust_application_client(&module, &contract, &[]);
    let go = generate_go_application_client(&module, &contract, &[]);
    let typescript = generate_typescript_application_client(&module, &contract, &[]);
    let python =
        generate_python_application_client(&module, &contract, &[]).expect("Python client");

    let typescript_runtime = include_str!("../../../clients/typescript/runtime/src/index.ts");
    let rust_runtime = include_str!("../../riffdb-client-rust/src/generated/mod.rs");
    for generated in [&go, &python, typescript_runtime, rust_runtime] {
        assert!(generated.contains("collection_count"));
        assert!(generated.contains("individual_value_bytes"));
        assert!(generated.contains("aggregate_canonical_element_bytes"));
        assert!(!generated.contains("send_on_preflight_failure"));
    }
    assert!(rust.contains("GeneratedInputBudgetCause::CollectionCount"));
    assert!(rust.contains("GeneratedInputBudgetCause::IndividualValueBytes"));
    assert!(rust.contains("GeneratedInputBudgetCause::AggregateCanonicalElementBytes"));
    assert!(rust.contains("Some(index)"));
    assert!(go.contains("Index: &index"));
    assert!(typescript.contains(r#""maximumBytes":524288"#));
    assert!(
        !typescript.contains(r#""name":"object","schema":{"kind":"string","maximumBytes":256}"#)
    );
    assert!(typescript_runtime.contains(
        r#"fieldSchema.kind === "list" && fieldSchema.aggregateCanonicalElementBytes !== undefined"#
    ));
    assert!(typescript_runtime.contains("{ ...budgetPath, leaf: field.name }"));
    assert!(python.contains("index, \"context\""));
    assert!(python.contains("len(item.relation.encode(\"utf-8\")) > 64"));
    assert!(!python.contains("len(item.relation) > 64"));
}

#[test]
// req: ID-001, ID-004, BLK-019
fn aggregate_budget_public_and_durable_identities_are_unchanged() {
    let (contract, module) = aggregate_budget_application();
    let command = contract
        .commands()
        .iter()
        .find(|command| command.name() == "WritePolicyMutations")
        .expect("command");
    assert_eq!(
        command
            .plan_hash()
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        "f727de9c81e0c211aaaa2054b3c87dba7744c8b639756fb30bdaf673e9bda6ee"
    );
    let typescript = generate_typescript_application_client(&module, &contract, &[]);
    assert!(typescript.contains(
        r#"inputSchemaHash: "ba2997d742db78a7dabca330fce3c282480b6c02b32397b658868aba87fb1e53""#
    ));
}
