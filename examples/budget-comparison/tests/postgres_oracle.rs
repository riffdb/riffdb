//! Offline oracle and generated-fixture tests.

#![forbid(unsafe_code)]

use riffdb_budget_comparison_core::{
    canonical_workload, generated_fixtures, parse_workload_fixture, render_workload_fixture,
};

#[test]
fn canonical_workload_round_trips_without_normalization() {
    let rendered = render_workload_fixture(&canonical_workload());
    let parsed = parse_workload_fixture(&rendered).expect("generated workload is valid");
    assert_eq!(parsed, canonical_workload());
    assert_eq!(render_workload_fixture(&parsed), rendered);
}

#[test]
fn checked_in_fixtures_match_the_reference_model() {
    for fixture in generated_fixtures().expect("reference fixture generation succeeds") {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(&fixture.relative_path);
        let actual = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        assert_eq!(
            actual,
            fixture.contents,
            "fixture {} is stale",
            path.display()
        );
    }
}

#[test]
fn parser_rejects_noncanonical_and_unknown_workload_json() {
    let canonical = render_workload_fixture(&canonical_workload());
    let compact = canonical.replace("\n", "").replace("  ", "");
    assert!(parse_workload_fixture(&compact).is_err());

    let unknown = canonical.replacen("{\n", "{\n  \"unknown\": true,\n", 1);
    assert!(parse_workload_fixture(&unknown).is_err());

    let duplicate = canonical.replacen("{\n", "{\n  \"schema_version\": 1,\n", 1);
    assert!(parse_workload_fixture(&duplicate).is_err());

    let version_line = "  \"schema_version\": 1,\n";
    let missing = canonical.replacen(version_line, "", 1);
    assert!(parse_workload_fixture(&missing).is_err());

    let reordered = missing.replacen("{\n", &format!("{{\n{version_line}"), 1);
    assert!(parse_workload_fixture(&reordered).is_err());

    let alternate_number =
        canonical.replacen("\"fiscal_year\": 2026", "\"fiscal_year\": 2.026e3", 1);
    assert!(parse_workload_fixture(&alternate_number).is_err());

    let alternate_type =
        canonical.replacen("\"fiscal_year\": 2026", "\"fiscal_year\": \"2026\"", 1);
    assert!(parse_workload_fixture(&alternate_type).is_err());

    let alternate_amount = canonical.replacen("\"amount\": \"80.00\"", "\"amount\": \"080.00\"", 1);
    assert!(parse_workload_fixture(&alternate_amount).is_err());

    let missing_final_lf = canonical.strip_suffix('\n').expect("generated final LF");
    assert!(parse_workload_fixture(missing_final_lf).is_err());

    let oversized = " ".repeat(riffdb_budget_comparison_core::MAX_FIXTURE_BYTES + 1);
    assert!(parse_workload_fixture(&oversized).is_err());
}
