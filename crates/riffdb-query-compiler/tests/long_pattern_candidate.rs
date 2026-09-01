//! ADR-0174 long-pattern candidate planning against the frozen compatibility corpus.

#![forbid(unsafe_code)]

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{PlannerDiagnosticCode, compile_query};
use riffdb_query_ir::{QueryAccessKind, SymbolicCatalog, resolve_query_surface};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str =
    include_str!("../../../fixtures/riffql/bounded-filtered-result-v1/contract.riff");
const QUERY: &str = include_str!(
    "../../../fixtures/riffql/bounded-filtered-result-v1/queries/combined_tag_pattern.riffq"
);

#[test]
fn frozen_combined_source_compiles_to_an_explicit_provider_step() {
    let bundle = compile_contract_source(CONTRACT).expect("contract corpus");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    // The frozen surface intentionally demonstrates a 50,000-row structural
    // bound; this wide projection independently exceeds the unchanged 4 MiB
    // result budget. Narrow only that independent bound for this plan-shape
    // assertion.
    let compilable_query = QUERY.replace("Limit<50000>", "Limit<1000>");
    let document = parse_query(&compilable_query).expect("query corpus");
    resolve_query_surface(&document, &catalog).expect("resolved provider source");
    let program = compile_query(&document, &catalog).expect("provider candidate plan");
    assert!(matches!(
        program.steps()[1].access(),
        QueryAccessKind::LongPatternCandidate { provider, pattern }
            if provider == "by_name_pattern"
                && pattern.pattern_parameter() == "name_pattern"
                && pattern.field() == "name"
    ));
}

#[test]
fn wide_rows_remain_independent_from_the_four_mib_result_budget() {
    let bundle = compile_contract_source(CONTRACT).expect("contract corpus");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let document = parse_query(QUERY).expect("query corpus");
    resolve_query_surface(&document, &catalog).expect("50,000 rows are structurally valid");
    let diagnostics = compile_query(&document, &catalog).expect_err("wide result bytes refuse");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        PlannerDiagnosticCode::CostCeilingExceeded
    );
}

#[test]
fn negation_requires_an_explicit_authorized_difference() {
    let bundle = compile_contract_source(CONTRACT).expect("contract corpus");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let direct_negation = QUERY
        .replace("Limit<50000>", "Limit<1000>")
        .replace("name ilike $name_pattern", "name not_ilike $name_pattern");
    let document = parse_query(&direct_negation).expect("negative operator syntax");
    assert!(resolve_query_surface(&document, &catalog).is_err());

    let difference = include_str!(
        "../../../fixtures/riffql/bounded-filtered-result-v1/queries/authorized_pattern_difference.riffq"
    )
    .replace("Limit<50000>", "Limit<1000>");
    let document = parse_query(&difference).expect("authorized difference syntax");
    resolve_query_surface(&document, &catalog).expect("authorized difference resolves");
    compile_query(&document, &catalog).expect("authorized difference compiles");
}
