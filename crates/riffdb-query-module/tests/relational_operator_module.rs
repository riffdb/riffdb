//! ADR-0185 least-sufficient module identity and interface-safety proofs.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_ir::QUERY_IR_VERSION_RELATIONAL_OPERATORS_V1;
use riffdb_query_module::{
    NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_RELATIONAL_OPERATORS_V1, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion, generate_go_client,
    generate_mcp_tools, generate_python_client, generate_rust_client, generate_typescript_client,
};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const QUERY: &str = include_str!("../../../fixtures/riffql/ticket_comments_expansion.riffql");

// req: OQ-113, OQ-116
#[test]
fn relational_operator_module_uses_v18_without_exposing_plan_structure() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("ticket_comments").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("TicketComments", QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("module");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_RELATIONAL_OPERATORS_V1
    );
    assert_eq!(
        module
            .query("TicketComments")
            .expect("query")
            .plan()
            .representative_program()
            .ir_version(),
        QUERY_IR_VERSION_RELATIONAL_OPERATORS_V1
    );
    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("strict V18 round trip");
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());

    let rust = generate_rust_client(&module, &contract);
    let go = generate_go_client(&module, &contract);
    let typescript = generate_typescript_client(&module, &contract);
    let python = generate_python_client(&module, &contract).expect("Python client");
    for source in [&rust, &go, &typescript, &python] {
        for forbidden in ["by_ticket", "per_driver_maximum", "driver_binding"] {
            assert!(
                !source.contains(forbidden),
                "generated surface exposed {forbidden}"
            );
        }
    }
    assert!(rust.contains("pub comments: Vec<TicketCommentsFoundTicketsComments>"));
    assert!(go.contains("Comments []struct"));
    assert!(typescript.contains("readonly comments: ReadonlyArray<"));
    assert!(python.contains("comments: tuple[TicketCommentsFoundTicketsComments, ...]"));
    let tools = generate_mcp_tools(&module).expect("MCP tools");
    assert_eq!(tools.len(), 1);
    for forbidden in ["by_ticket", "per_driver_maximum", "driver_binding"] {
        assert!(!tools[0].input_schema.contains(forbidden));
        assert!(!tools[0].result_schema.contains(forbidden));
    }
    assert!(tools[0].result_schema.contains("comments"));
    assert!(tools[0].result_schema.contains("maxItems"));
}
