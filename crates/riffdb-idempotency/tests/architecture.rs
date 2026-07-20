#![forbid(unsafe_code)]

//! Dependency and durable-ownership guards for command idempotency.

const MANIFEST: &str = include_str!("../Cargo.toml");
const PREPARATION_SOURCE: &str = include_str!("../src/prepare.rs");
const LIB_ROOT: &str = include_str!("../src/lib.rs");

#[test]
fn production_dependencies_are_exact_and_exclude_authentication() {
    let dependencies = MANIFEST
        .lines()
        .skip_while(|line| *line != "[dependencies]")
        .skip(1)
        .take_while(|line| !line.starts_with('['))
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    assert_eq!(
        dependencies,
        [
            "riffdb-storage-api = { version = \"0.1.0\", path = \"../riffdb-storage-api\", default-features = false }",
            "riffdb-types = { version = \"0.1.0\", path = \"../riffdb-types\", default-features = false }",
        ]
    );
    assert!(!MANIFEST.contains("riffdb-auth"));
}

#[test]
fn preparation_api_excludes_deployment_and_invocation_versions_by_shape() {
    assert!(!PREPARATION_SOURCE.contains("ContractVersion"));
    assert!(!PREPARATION_SOURCE.contains("RequestId"));
    assert!(!PREPARATION_SOURCE.contains("CommittedOutcome"));
}

#[test]
fn crate_root_keeps_safe_rust_mandatory() {
    assert!(LIB_ROOT.starts_with("#![forbid(unsafe_code)]"));
}
