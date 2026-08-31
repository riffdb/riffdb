#![forbid(unsafe_code)]

//! Named tokenized-text module identity and generated-surface parity (ADR-0173).

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_ir::{TokenizedMatchKindV1, TokenizedRankingV1};
use riffdb_query_module::{
    CompiledNamedQueryPlan, NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_TOKENIZED_TEXT_V1,
    QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    generate_go_application_client, generate_mcp_tools, generate_python_application_client,
    generate_rust_application_client, generate_typescript_application_client,
};
use riffdb_types::ProjectionProviderKindV1;

const CONTRACT: &str = include_str!("../../../fixtures/compiler/text-index/contract.riff");

const QUERY: &str = r#"
query SearchDocuments(
  $org_id: Document.org_id,
  $query: Document.title,
  $limit: Limit<499> = 50,
  $offset: u64 = 0
) {
  many documents from Document
    where org_id == $org_id
    matching(search, conjunction, $query)
    order by doc_id asc
    take $limit offset $offset
  return Found { documents: documents { doc_id title } }
  outcomes Found
}
"#;

fn module() -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("tokenized_documents").expect("name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchDocuments", QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("tokenized module");
    (contract, module)
}

#[test]
fn tokenized_source_seals_v13_and_round_trips_by_exact_recompilation() {
    let (contract, module) = module();
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_TOKENIZED_TEXT_V1
    );
    let query = module.query("SearchDocuments").expect("query");
    let CompiledNamedQueryPlan::TokenizedTextV1(tokenized) = query.plan() else {
        panic!("tokenized plan kind");
    };
    assert_eq!(
        tokenized.tokenized_plan().kind(),
        TokenizedMatchKindV1::Conjunction
    );
    assert_eq!(
        tokenized.tokenized_plan().descriptor().kind(),
        ProjectionProviderKindV1::TokenizedText
    );
    assert_eq!(tokenized.query_parameter(), "query");
    assert_eq!(tokenized.authorization()[0].indexes(), &["search"]);
    assert_eq!(tokenized.authorization_cost().scanned_index_rows(), 0);
    assert_eq!(tokenized.cost().scanned_index_rows(), 10_000);

    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("strict tokenized module decode");
    assert_eq!(decoded.identity(), module.identity());
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
}

#[test]
fn generated_surfaces_expose_only_the_named_typed_operation() {
    let (contract, module) = module();
    let generated = [
        generate_rust_application_client(&module, &contract, &[]),
        generate_go_application_client(&module, &contract, &[]),
        generate_typescript_application_client(&module, &contract, &[]),
        generate_python_application_client(&module, &contract, &[]).expect("python"),
    ];
    for source in generated {
        assert!(source.to_ascii_lowercase().contains("searchdocuments"));
        for forbidden in ["analyzer", "boost", "score", "provider_choice", "wildcard"] {
            assert!(!source.to_ascii_lowercase().contains(forbidden));
        }
    }
    let tools = generate_mcp_tools(&module).expect("mcp");
    assert_eq!(tools.len(), 1);
    assert!(tools[0].input_schema.contains("query"));
}

#[test]
fn ranked_module_keeps_scores_and_tuning_out_of_every_generated_surface() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let ranked = QUERY.replace("$query)", "$query, riff_bm25_v1)");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("ranked_documents").expect("name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchDocuments", ranked).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("ranked module");
    assert_eq!(
        module
            .query("SearchDocuments")
            .and_then(|query| query.tokenized_text_result())
            .expect("tokenized")
            .tokenized_plan()
            .ranking(),
        TokenizedRankingV1::RiffBm25V1
    );
    for source in [
        generate_rust_application_client(&module, &contract, &[]),
        generate_go_application_client(&module, &contract, &[]),
        generate_typescript_application_client(&module, &contract, &[]),
        generate_python_application_client(&module, &contract, &[]).expect("python"),
    ] {
        for forbidden in ["score", "boost", "k1", "bm25"] {
            assert!(!source.to_ascii_lowercase().contains(forbidden));
        }
    }
}
