//! Static boundary evidence for the compiler-only ADR-0134 package.

use std::fs;
use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn semantic_compiler_has_no_provider_runtime_or_framework_branch() {
    let root = workspace_root();
    let compiler =
        fs::read_to_string(root.join("crates/riffdb-query-compiler/src/exact_predicate.rs"))
            .expect("compiler source");
    for forbidden in [
        "riffdb_projection",
        "riffdb_query_executor",
        "riffdb_storage",
        "better_auth",
        "BetterAuth",
        "scan_entity",
        "fallback",
    ] {
        assert!(
            !compiler.contains(forbidden),
            "compiler-only exact predicate source contains forbidden token {forbidden}"
        );
    }
}

#[test]
fn generated_surfaces_cannot_accept_a_predicate_or_order_ast() {
    let root = workspace_root();
    for path in [
        "crates/riffdb-query-module/src/generation.rs",
        "crates/riffdb-query-module/src/go_generation.rs",
        "crates/riffdb-query-module/src/python_generation.rs",
    ] {
        let source = fs::read_to_string(root.join(path)).expect("generation source");
        for forbidden in [
            "ExactPredicateNodeV1",
            "ExactPredicateOperatorV1",
            "ExactOrderTermV1",
            "ExactOrderTermV2",
            "ExactStatePlacementV1",
            "provider_requirement",
        ] {
            assert!(
                !source.contains(forbidden),
                "generated surface {path} exposes compiler structure {forbidden}"
            );
        }
    }
}
