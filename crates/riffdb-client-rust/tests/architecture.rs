#![forbid(unsafe_code)]

//! Architecture constraints for the public Rust transport client.

const LIB: &str = include_str!("../src/lib.rs");
const CLIENT: &str = include_str!("../src/client.rs");
const CAPABILITY: &str = include_str!("../src/capability.rs");
const COMMAND: &str = include_str!("../src/command.rs");
const CREDENTIAL_FILE: &str = include_str!("../src/credential_file.rs");
const IDS: &str = include_str!("../src/ids.rs");
const METADATA: &str = include_str!("../src/metadata.rs");
const STATUS: &str = include_str!("../src/status.rs");
const GENERATED: &str = include_str!("../src/generated/mod.rs");
const LEGAL_SPEND: &str = include_str!("../src/generated/legal_spend.rs");

#[test]
fn client_has_no_direct_lower_semantic_authority_dependency() {
    let manifest = include_str!("../Cargo.toml");
    for banned in [
        "riffdb-auth",
        "riffdb-catalog",
        "riffdb-commit",
        "riffdb-conflict",
        "riffdb-policy",
        "riffdb-runtime",
        "riffdb-service",
        "riffdb-storage-api",
        "riffdb-storage-memory",
        "riffdb-storage-redb",
    ] {
        assert!(
            !manifest
                .lines()
                .any(|line| line.trim_start().starts_with(banned)),
            "Rust client must not depend directly on {banned}"
        );
    }
}

#[test]
fn reviewed_transport_and_entropy_graph_remains_exact() {
    let manifest = include_str!("../Cargo.toml");
    assert!(manifest.contains("default = []"));
    assert!(manifest.contains("getrandom = { version = \"=0.3.4\", default-features = false }"));
    assert!(manifest.contains(
        "tonic = { version = \"=0.14.6\", default-features = false, features = [\"channel\", \"codegen\"] }"
    ));
    assert!(manifest.contains("tonic-prost = { version = \"=0.14.6\", default-features = false }"));
    assert!(manifest.contains(
        "zeroize = { version = \"=1.8.1\", default-features = false, features = [\"alloc\"] }"
    ));
    assert!(manifest.contains("default-features = false, features = [\"client\"]"));
    for forbidden in ["base64 =", "tls-", "gzip", "deflate", "zstd"] {
        assert!(!manifest.contains(forbidden));
    }
}

#[test]
fn operating_system_identity_sources_are_isolated_and_stateless() {
    for source in [
        CLIENT,
        CAPABILITY,
        COMMAND,
        CREDENTIAL_FILE,
        METADATA,
        STATUS,
        GENERATED,
        LEGAL_SPEND,
    ] {
        assert!(!source.contains("SystemTime"));
        assert!(!source.contains("getrandom::"));
    }
    assert!(IDS.contains("SystemTime::now"));
    assert!(IDS.contains("getrandom::fill"));
    assert!(!IDS.contains("static mut"));
    assert!(!IDS.contains("Mutex<"));
}

#[test]
fn first_party_client_forbids_unsafe_rust() {
    assert!(LIB.starts_with("#![forbid(unsafe_code)]"));
    for source in [
        CLIENT,
        CAPABILITY,
        COMMAND,
        CREDENTIAL_FILE,
        IDS,
        METADATA,
        STATUS,
        GENERATED,
        LEGAL_SPEND,
    ] {
        assert!(!source.contains("unsafe {"));
        assert!(!source.contains("unsafe fn"));
    }
}

#[test]
fn public_error_owner_is_reexported_without_an_sdk_copy() {
    for owner_type in [
        "ErrorClass",
        "PublicError",
        "PublicErrorDetails",
        "PublicErrorKind",
        "RecoveryAction",
        "ValidationCode",
        "ValidationIssue",
        "ValidationIssues",
        "ValidationPath",
        "ValidationPathSegment",
    ] {
        assert!(LIB.contains(owner_type));
    }
    assert!(LIB.contains("pub use riffdb_errors::"));
    assert!(!LIB.contains("pub struct PublicError"));
    assert!(!LIB.contains("pub enum PublicError"));
}

#[test]
fn retry_and_credential_boundaries_remain_operation_specific() {
    for helper in [
        "pub async fn execute_with_retry(",
        "pub async fn create_capability_with_retry(",
        "pub async fn create_bootstrap_capability_with_retry(",
    ] {
        assert!(CLIENT.contains(helper), "missing reviewed helper: {helper}");
    }
    for forbidden in ["sleep(", "FnMut", "FnOnce", "retry_rpc", "retry_status"] {
        assert!(
            !CLIENT.contains(forbidden),
            "generic retry authority: {forbidden}"
        );
    }
    assert!(
        CREDENTIAL_FILE
            .contains("Zeroizing::new(Vec::with_capacity(BEARER_PRESENTATION_BYTES + 1))")
    );
    assert!(CREDENTIAL_FILE.contains(".take((BEARER_PRESENTATION_BYTES + 1) as u64)"));
    assert_eq!(
        CREDENTIAL_FILE
            .matches("pub fn load_protected_bearer_credential(")
            .count(),
        1
    );
    assert!(CREDENTIAL_FILE.contains("O_NOFOLLOW"));
    assert!(CREDENTIAL_FILE.contains("O_NONBLOCK"));
    for forbidden in ["base64", "riffdb_auth", "decode", "digest", "authenticate"] {
        assert!(
            !CREDENTIAL_FILE.contains(forbidden),
            "credential loader crossed its presentation-only boundary: {forbidden}"
        );
    }
}
