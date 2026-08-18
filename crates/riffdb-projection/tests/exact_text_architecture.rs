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
