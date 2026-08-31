#![forbid(unsafe_code)]

//! ADR-0173 stage-one front-door, identity, and diagnostic coverage.

use riffdb_contract_compiler::{
    CompilerDiagnosticCause, CompilerDiagnosticCode, compile_contract_source,
};
use riffdb_contract_ir::{
    BUNDLE_FORMAT_VERSION_V20, ContractBundle, EXECUTABLE_IR_VERSION_V20, GRAMMAR_VERSION_V20,
};
use riffdb_types::{TextAnalyzerV1, TextSearchResultModelV1};

const DECLARATION: &str = "text_index search((title weight 4, body weight 1), analyzer standard_v1, staleness_slo 60, replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000, result boolean_v1, max_terms 16, max_candidates 10000, max_results 1000)";

fn contract(declaration: &str) -> String {
    format!(
        r#"
contract Docs version 1 {{
  entity Document {{
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field body: optional<string<65536>>
    field revision: u64
    {declaration}
  }}
}}
"#
    )
}

#[test]
fn text_index_allocates_v20_retains_every_field_and_round_trips() {
    let bundle = compile_contract_source(&contract(DECLARATION)).expect("text index compiles");
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V20);
    assert_eq!(bundle.grammar_version(), GRAMMAR_VERSION_V20);
    assert_eq!(bundle.ir_version(), EXECUTABLE_IR_VERSION_V20);
    let spec = &bundle.schema().text_index_specs()[0];
    assert_eq!(spec.analyzer(), TextAnalyzerV1::StandardV1);
    assert_eq!(spec.result_model(), TextSearchResultModelV1::BooleanV1);
    assert_eq!(spec.stale_entity_count_threshold(), 60);
    assert_eq!(spec.replay_age_seconds(), 86_400);
    assert_eq!(spec.replay_bytes(), 1_073_741_824);
    assert_eq!(spec.replay_backlog(), 100_000);
    assert_eq!(spec.max_terms(), 16);
    assert_eq!(spec.max_candidates(), 10_000);
    assert_eq!(spec.max_results(), 1_000);
    assert_eq!(
        spec.source_fields()
            .iter()
            .map(|source| (
                bundle.schema().entities()[0]
                    .record()
                    .field(source.field())
                    .expect("source field")
                    .name(),
                source.weight(),
            ))
            .collect::<Vec<_>>(),
        [("body", 1), ("title", 4)]
    );

    let decoded = ContractBundle::decode(bundle.canonical_bytes()).expect("V20 decodes");
    assert_eq!(decoded, bundle);
    assert_eq!(decoded.bundle_hash(), bundle.bundle_hash());
}

#[test]
fn analyzer_and_weight_are_identity_bearing_but_absence_keeps_v1() {
    let standard = compile_contract_source(&contract(DECLARATION)).expect("standard");
    let keyword =
        compile_contract_source(&contract(&DECLARATION.replace("standard_v1", "keyword_v1")))
            .expect("keyword");
    let reweighted = compile_contract_source(&contract(
        &DECLARATION.replace("title weight 4", "title weight 5"),
    ))
    .expect("reweighted");
    assert_ne!(standard.bundle_hash(), keyword.bundle_hash());
    assert_ne!(standard.bundle_hash(), reweighted.bundle_hash());

    let without = compile_contract_source(&contract("")).expect("declaration-free contract");
    assert_eq!(without.format_version(), 1);
    assert!(without.schema().text_index_specs().is_empty());
}

#[test]
fn checked_in_v20_fixture_is_the_canonical_compiler_output() {
    let source = include_str!("../../../fixtures/compiler/text-index/contract.riff");
    let expected = include_bytes!("../../../fixtures/compiler/text-index/bundle.bin");
    let bundle = compile_contract_source(source).expect("fixture contract compiles");
    assert_eq!(bundle.canonical_bytes(), expected);

    let decoded = ContractBundle::decode(expected).expect("checked-in V20 fixture decodes");
    assert_eq!(decoded, bundle);
    assert_eq!(decoded.format_version(), BUNDLE_FORMAT_VERSION_V20);
}

#[test]
fn every_text_index_failure_names_the_condition_at_the_narrow_span() {
    for (changed, code, cause, offending) in [
        (
            DECLARATION.replace("title weight 4, body weight 1", ""),
            CompilerDiagnosticCode::InvalidTextIndex,
            Some(CompilerDiagnosticCause::TextIndexEmptySources),
            "text_index search",
        ),
        (
            DECLARATION.replace("standard_v1", "custom_v1"),
            CompilerDiagnosticCode::InvalidTextIndex,
            Some(CompilerDiagnosticCause::TextIndexAnalyzerUnknown),
            "custom_v1",
        ),
        (
            DECLARATION.replace("title weight 4", "revision weight 4"),
            CompilerDiagnosticCode::InvalidTextIndex,
            Some(CompilerDiagnosticCause::TextIndexSourceNotString),
            "revision",
        ),
        (
            DECLARATION.replace("title weight 4", "title weight 0"),
            CompilerDiagnosticCode::InvalidTextIndex,
            Some(CompilerDiagnosticCause::TextIndexWeightOutOfRange),
            "0",
        ),
        (
            DECLARATION.replace("boolean_v1", "ranked_v1"),
            CompilerDiagnosticCode::InvalidTextIndex,
            Some(CompilerDiagnosticCause::TextIndexResultModelUnknown),
            "ranked_v1",
        ),
        (
            DECLARATION.replace("max_results 1000", "max_results 10001"),
            CompilerDiagnosticCode::InvalidTextIndex,
            Some(CompilerDiagnosticCause::TextIndexResultExceedsCandidate),
            "10001",
        ),
    ] {
        let source = contract(&changed);
        let diagnostics = compile_contract_source(&source).expect_err("invalid declaration");
        let diagnostic = diagnostics
            .semantic()
            .expect("semantic diagnostic")
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == code && diagnostic.cause() == cause)
            .unwrap_or_else(|| panic!("missing {cause:?}: {diagnostics}"));
        let span = diagnostic.primary_span();
        assert!(
            &source[span.start() as usize..span.end() as usize] == offending
                || source[span.start() as usize..span.end() as usize].starts_with(offending),
            "wrong span for {cause:?}: {:?}",
            &source[span.start() as usize..span.end() as usize]
        );
    }
}

#[test]
fn duplicate_and_unknown_sources_are_not_silently_rewritten() {
    for (changed, code, offending) in [
        (
            DECLARATION.replace(
                "title weight 4, body weight 1",
                "title weight 4, title weight 1",
            ),
            CompilerDiagnosticCode::DuplicateName,
            "title",
        ),
        (
            DECLARATION.replace("body weight 1", "missing weight 1"),
            CompilerDiagnosticCode::UnknownName,
            "missing",
        ),
    ] {
        let source = contract(&changed);
        let diagnostics = compile_contract_source(&source).expect_err("source must reject");
        let diagnostic = diagnostics
            .semantic()
            .expect("semantic diagnostic")
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == code)
            .unwrap_or_else(|| panic!("missing {code:?}: {diagnostics}"));
        let span = diagnostic.primary_span();
        assert_eq!(
            &source[span.start() as usize..span.end() as usize],
            offending
        );
    }
}
