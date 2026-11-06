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

#[test]
fn fuzzy_snippets_hybrid_rrf_and_matches_regex_compile_roundtrip_and_generate_clients() {
    let hybrid_contract = r#"
contract HybridSearchApp version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field body: optional<string<4096>>
    field embedding: vector<4>
    index by_org(org_id, doc_id)
    pattern_index by_title_regex(title, profile unicode_fold_v1, operators (matches_regex), max_source_bytes 256, max_matched_bytes 2560, max_rows 10000, max_total_matched_bytes 25600000, max_grams_per_row 2560, max_distinct_grams 200000, max_postings 1000000, max_postings_bytes 40000000, max_pattern_bytes 256, max_pattern_atoms 2560, max_candidates 10000, max_verification_bytes 25600000, max_results 1000, staleness_slo 60, replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000, retained_generations 4)
    text_index search((title weight 4, body weight 1), analyzer english_stem_v1, staleness_slo 60, replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000, result boolean_v1, max_terms 16, max_candidates 10000, max_results 1000)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id, doc_id)
  }
}
"#;
    let hybrid_query = r#"
query SearchDocumentsHybrid(
    $org_id: Document.org_id,
    $query: Document.title,
    $query_vec: Document.embedding,
    $limit: Limit<499> = 20,
    $offset: u64 = 0
) {
    many documents from Document
        where org_id == $org_id
        matching(search, fuzzy, 1, $query, hybrid_rrf_snippets_v1(embedding, $query_vec))
        order by doc_id asc
        take $limit offset $offset

    return Found {
        documents: documents {
            doc_id
            title
            body
        }
    }

    outcomes Found
}
"#;
    let regex_query = r#"
query FilterDocumentsByRegex(
    $org_id: Document.org_id,
    $pattern: Document.title,
    $limit: Limit<499> = 20
) {
    candidates matching_docs: Document.doc_id
        from Document.doc_id using by_title_regex
            where org_id == $org_id
                && title matches_regex $pattern
        within 10000
        else IntegrityFailure

    many documents from Document
        where org_id == $org_id && doc_id in matching_docs
        order by doc_id asc
        take $limit
        else IntegrityFailure

    return Found {
        documents: documents {
            doc_id
            title
        }
    }

    outcomes Found | IntegrityFailure
}
"#;
    let contract = compile_contract_source(hybrid_contract).expect("hybrid contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("hybrid_search_queries").expect("name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![
            NamedQuerySource::new("SearchDocumentsHybrid", hybrid_query).expect("hybrid query"),
            NamedQuerySource::new("FilterDocumentsByRegex", regex_query).expect("regex query"),
        ],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("hybrid module");
    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("round-trip decode");
    assert_eq!(decoded.identity(), module.identity());
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());

    let tokenized = module
        .query("SearchDocumentsHybrid")
        .and_then(|q| q.tokenized_text_result())
        .expect("tokenized hybrid result");
    assert_eq!(
        tokenized.tokenized_plan().kind(),
        TokenizedMatchKindV1::Fuzzy(1)
    );
    assert!(tokenized.tokenized_plan().ranking().snippets_enabled());
    assert!(
        tokenized
            .tokenized_plan()
            .ranking()
            .vector_field()
            .is_some()
    );
    assert_eq!(tokenized.vector_parameter(), Some("query_vec"));

    for source in [
        generate_rust_application_client(&module, &contract, &[]),
        generate_go_application_client(&module, &contract, &[]),
        generate_typescript_application_client(&module, &contract, &[]),
        generate_python_application_client(&module, &contract, &[]).expect("python"),
    ] {
        let lower = source.to_ascii_lowercase();
        assert!(lower.contains("searchdocumentshybrid"));
        assert!(lower.contains("filterdocumentsbyregex"));
        assert!(lower.contains("query_vec") || lower.contains("queryvec"));
    }
    let tools = generate_mcp_tools(&module).expect("mcp");
    assert_eq!(tools.len(), 2);
}
