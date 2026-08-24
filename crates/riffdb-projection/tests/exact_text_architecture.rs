//! Architecture negatives for the compile-only ADR-0131 provider.

#[test]
fn exact_text_provider_has_no_scan_full_text_or_framework_escape() {
    let provider = include_str!("../src/exact_text.rs");
    let manifest = include_str!("../Cargo.toml");
    for forbidden in [
        ".scan(",
        "materialize_all",
        "page_fold",
        "cursor_walk",
        "cross_provider_bridge",
        "better_auth",
        "BetterAuth",
    ] {
        assert!(
            !provider.contains(forbidden),
            "forbidden provider shape: {forbidden}"
        );
    }
    for forbidden_dependency in ["regex =", "tantivy =", "fst ="] {
        assert!(
            !manifest.contains(forbidden_dependency),
            "exact-text provider must not acquire full-text dependency {forbidden_dependency}"
        );
    }
}

#[test]
fn nullable_exact_order_pays_proofs_once_and_has_no_request_time_escape() {
    let provider = include_str!("../src/exact_predicate.rs");
    let v5 = provider
        .split_once("impl ExactPredicatePartitionIndexV5")
        .expect("V5 provider")
        .1
        .split_once("struct BitSet")
        .expect("V5 boundary")
        .0;
    assert!(
        !v5.contains("provider_descriptor"),
        "descriptor/layout proof belongs to provider binding, not V5 rows or probes"
    );
    let page = v5
        .split_once("pub fn result_page")
        .expect("V5 page")
        .1
        .split_once("pub const fn binding")
        .expect("V5 page boundary")
        .0;
    for forbidden in [
        "sort_by",
        ".scan(",
        "materialize",
        "cursor_walk",
        "page_walk",
        "sentinel",
        "cross_provider",
        "provider_descriptor",
    ] {
        assert!(
            !page.contains(forbidden),
            "nullable request-time provider path contains {forbidden}"
        );
    }
}
