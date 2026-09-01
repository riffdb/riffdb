//! ADR-0174 long-pattern contract compilation and compatibility tests.

#![forbid(unsafe_code)]

use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_contract_ir::{
    BUNDLE_FORMAT_VERSION_V21, ContractBundle, EXECUTABLE_IR_VERSION_V21, GRAMMAR_VERSION_V21,
};
use riffdb_types::{LongPatternOperatorV1, LongPatternProfileV1};

const DECLARATION: &str = "pattern_index by_name(name, profile unicode_fold_v1, operators (equals, starts_with, ends_with, contains, like, ilike, not_like, not_ilike), max_source_bytes 500, max_matched_bytes 9000, max_rows 50000, max_total_matched_bytes 450000000, max_grams_per_row 9000, max_distinct_grams 200000, max_postings 1000000, max_postings_bytes 40000000, max_pattern_bytes 5000, max_pattern_atoms 90000, max_candidates 50000, max_verification_bytes 450000000, max_results 50000, staleness_slo 60, replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000, retained_generations 4)";

fn source(declaration: &str) -> String {
    format!(
        r#"contract LongNames version 1 {{
  entity Experiment {{
    key (org_id: uuid, experiment_id: uuid)
    field name: string<500>
    {declaration}
  }}
}}"#
    )
}

#[test]
fn long_pattern_declaration_is_v21_identity_bearing_and_round_trips() {
    let bundle = compile_contract_source(&source(DECLARATION)).expect("long pattern compiles");
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V21);
    assert_eq!(bundle.grammar_version(), GRAMMAR_VERSION_V21);
    assert_eq!(bundle.ir_version(), EXECUTABLE_IR_VERSION_V21);
    let [spec] = bundle.schema().long_pattern_specs() else {
        panic!("one provider");
    };
    assert_eq!(spec.profile(), LongPatternProfileV1::UnicodeFoldV1);
    assert_eq!(spec.name(), "by_name");
    assert_eq!(spec.bounds().source_bytes(), 500);
    assert_eq!(spec.bounds().rows(), 50_000);
    assert_eq!(spec.bounds().results(), 50_000);
    assert_eq!(
        spec.operators(),
        [
            LongPatternOperatorV1::Equals,
            LongPatternOperatorV1::StartsWith,
            LongPatternOperatorV1::EndsWith,
            LongPatternOperatorV1::Contains,
            LongPatternOperatorV1::Like,
            LongPatternOperatorV1::ILike,
            LongPatternOperatorV1::NotLike,
            LongPatternOperatorV1::NotILike,
        ]
    );
    let decoded = ContractBundle::decode(bundle.canonical_bytes()).expect("V21 decodes");
    assert_eq!(decoded, bundle);

    let binary = compile_contract_source(&source(
        &DECLARATION.replace("unicode_fold_v1", "binary_utf8_v1"),
    ))
    .expect("binary");
    assert_ne!(binary.bundle_hash(), bundle.bundle_hash());
    let legacy = compile_contract_source(&source("")).expect("legacy");
    assert_eq!(legacy.format_version(), 1);
    assert!(legacy.schema().long_pattern_specs().is_empty());

    let successor_source = source(DECLARATION).replace("version 1", "version 2");
    let successor = compile_contract_successor(&successor_source, &bundle)
        .expect("an unchanged provider retains its stable identity");
    assert_eq!(
        successor.schema().long_pattern_specs()[0].index(),
        spec.index()
    );
}
