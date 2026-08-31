//! Compiler sealing for bounded tokenized named queries (ADR-0173).

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{PlannerDiagnosticCode, compile_tokenized_text_query_v1};
use riffdb_query_ir::{SymbolicCatalog, TokenizedMatchKindV1, TokenizedRankingV1};
use riffdb_riffql_syntax::parse_query;
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

fn catalog() -> SymbolicCatalog {
    let bundle = compile_contract_source(CONTRACT).expect("text-index contract");
    SymbolicCatalog::from_bundle(&bundle).expect("catalog")
}

#[test]
fn declared_index_seals_analyzer_fields_shape_and_budgets() {
    let document = parse_query(QUERY).expect("tokenized query");
    let compiled = compile_tokenized_text_query_v1(&document, &catalog()).expect("compiled plan");
    assert_eq!(compiled.plan.kind(), TokenizedMatchKindV1::Conjunction);
    assert_eq!(
        compiled.plan.descriptor().kind(),
        ProjectionProviderKindV1::TokenizedText
    );
    assert_eq!(compiled.plan.max_terms(), 16);
    assert_eq!(compiled.plan.max_candidates(), 10_000);
    assert_eq!(compiled.plan.max_results(), 1_000);
    assert_eq!(compiled.plan.fields().len(), 2);
    assert_eq!(compiled.query_parameter, "query");
}

#[test]
fn undeclared_index_fails_closed_at_its_source_span() {
    let source = QUERY.replace("matching(search,", "matching(missing,");
    let document = parse_query(&source).expect("syntactically valid query");
    let diagnostics = compile_tokenized_text_query_v1(&document, &catalog())
        .expect_err("unknown index must fail");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::ExactTextProvider);
    assert_eq!(diagnostic.summary(), "tokenized text index is unknown");
    assert!(diagnostic.primary().start < diagnostic.primary().end);
}

#[test]
fn ranked_source_seals_the_only_scoring_identity() {
    let source = QUERY.replace("$query)", "$query, riff_bm25_v1)");
    let document = parse_query(&source).expect("ranked tokenized query");
    let compiled = compile_tokenized_text_query_v1(&document, &catalog()).expect("compiled plan");
    assert_eq!(compiled.plan.ranking(), TokenizedRankingV1::RiffBm25V1);
}
