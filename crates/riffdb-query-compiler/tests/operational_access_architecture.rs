//! Static ownership guards for ADR-0150's ordinary access-path algebra.

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
fn one_compiler_registry_and_one_executor_lowering_own_component_semantics() {
    let root = workspace_root();
    let compiler = fs::read_to_string(root.join("crates/riffdb-query-compiler/src/lib.rs"))
        .expect("compiler source");
    assert_eq!(
        compiler
            .matches("const OPERATIONAL_COMPONENT_CAPABILITY_REGISTRY")
            .count(),
        1,
        "operational component eligibility has one compiler owner"
    );

    let executor = fs::read_to_string(root.join("crates/riffdb-query-executor/src/lib.rs"))
        .expect("executor source");
    assert_eq!(
        executor.matches("fn push_exact_index_prefix_value").count(),
        1,
        "logical exact values have one physical prefix transform"
    );
    assert_eq!(
        executor
            .matches("pub fn bound_index_range_schedule_v1")
            .count(),
        1,
        "all ordinary selections have one physical range-schedule owner"
    );
    assert_eq!(
        executor
            .matches("fn encode_canonical_interval_schedule")
            .count(),
        1,
        "typed interval lowering has one executor owner"
    );

    for path in [
        "crates/riffdb-storage-memory/src/query.rs",
        "crates/riffdb-storage-redb/src/query.rs",
    ] {
        let storage = fs::read_to_string(root.join(path)).expect("storage query source");
        assert!(
            storage.contains("bound_index_range_schedule_v1") && storage.contains("resume_window"),
            "storage adapter {path} must consume the shared range and continuation contract"
        );
        assert!(
            !storage.contains("bound_index_prefix_bytes_v1"),
            "storage adapter {path} must not retain the prefix-only planner"
        );
        for forbidden in ["TextKeyProfileV1", "BinaryUtf8", "UnicodeFold"] {
            assert!(
                !storage.contains(forbidden),
                "storage adapter {path} must not own text profile transform {forbidden}"
            );
        }
    }
}

#[test]
fn no_external_framework_branch_enters_the_generic_access_path() {
    let root = workspace_root();
    for path in [
        "crates/riffdb-query-compiler/src/lib.rs",
        "crates/riffdb-query-executor/src/lib.rs",
        "crates/riffdb-storage-memory/src/query.rs",
        "crates/riffdb-storage-redb/src/query.rs",
    ] {
        let source = fs::read_to_string(root.join(path)).expect("generic query source");
        for forbidden in ["openfga", "OpenFGA", "better_auth", "BetterAuth"] {
            assert!(
                !source.contains(forbidden),
                "generic access source {path} contains framework token {forbidden}"
            );
        }
    }
}
