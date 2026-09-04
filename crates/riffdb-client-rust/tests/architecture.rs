#![forbid(unsafe_code)]

//! Architecture constraints for the public Rust transport client.

use std::path::PathBuf;
use std::process::Command;

const LIB: &str = include_str!("../src/lib.rs");
const CLIENT: &str = include_str!("../src/generated/client.rs");
const CAPABILITY: &str = include_str!("../src/capability.rs");
const COMMAND: &str = include_str!("../src/command.rs");
const CREDENTIAL_FILE: &str = include_str!("../src/credential_file.rs");
const IDS: &str = include_str!("../src/ids.rs");
const METADATA: &str = include_str!("../src/metadata.rs");
const STATUS: &str = include_str!("../src/status.rs");
const TLS: &str = include_str!("../src/tls.rs");
const GENERATED: &str = include_str!("../src/generated/mod.rs");
const LEGAL_SPEND: &str = include_str!("../src/generated/legal_spend.rs");
const PYTHON_NATIVE: &str = include_str!("../../riffdb-client-python-native/src/lib.rs");

fn production_workspace_dependencies(package_name: &str) -> Vec<String> {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = crate_root
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--locked", "--no-deps"])
        .current_dir(workspace_root)
        .output()
        .expect("cargo metadata runs");
    assert!(
        output.status.success(),
        "cargo metadata failed closed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata emits JSON");
    let packages = metadata["packages"].as_array().expect("packages array");
    let workspace_names = packages
        .iter()
        .filter_map(|package| package["name"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let package = packages
        .iter()
        .find(|package| package["name"].as_str() == Some(package_name))
        .expect("workspace package");
    let mut dependencies = package["dependencies"]
        .as_array()
        .expect("dependencies array")
        .iter()
        .filter(|dependency| dependency["kind"].as_str() != Some("dev"))
        .filter(|dependency| {
            dependency["name"]
                .as_str()
                .is_some_and(|name| workspace_names.contains(name))
        })
        .map(|dependency| {
            dependency["name"]
                .as_str()
                .expect("dependency name")
                .to_owned()
        })
        .collect::<Vec<_>>();
    dependencies.sort();
    dependencies
}

// req: DEP-003
#[test]
fn client_depends_only_on_proto_tonic_types_errors_and_config() {
    assert_eq!(
        production_workspace_dependencies("riffdb-client-rust"),
        [
            "riffdb-config",
            "riffdb-errors",
            "riffdb-proto",
            "riffdb-types",
        ]
    );
}

#[test]
fn reviewed_transport_and_entropy_graph_remains_exact() {
    let manifest = include_str!("../Cargo.toml");
    assert!(manifest.contains("default = []"));
    assert!(manifest.contains("getrandom = { version = \"=0.3.4\", default-features = false }"));
    assert!(manifest.contains(
        "tonic = { version = \"=0.14.6\", default-features = false, features = [\"channel\", \"codegen\", \"tls-ring\"] }"
    ));
    assert!(manifest.contains("tonic-prost = { version = \"=0.14.6\", default-features = false }"));
    assert!(manifest.contains(
        "zeroize = { version = \"=1.8.1\", default-features = false, features = [\"alloc\"] }"
    ));
    assert!(manifest.contains("default-features = false, features = [\"client\"]"));
    assert!(manifest.contains(
        "riffdb-config = { version = \"0.1.0\", path = \"../riffdb-config\", default-features = false }"
    ));
    assert!(manifest.contains(
        "rustls-pki-types = { version = \"=1.15.1\", default-features = false, features = [\"std\"] }"
    ));
    assert!(manifest.contains(
        "rustls-webpki = { version = \"=0.103.13\", default-features = false, features = [\"std\"] }"
    ));
    for forbidden in [
        "base64 =",
        "tokio-rustls =",
        "ring =",
        "tls-aws-lc",
        "gzip",
        "deflate",
        "zstd",
    ] {
        assert!(!manifest.contains(forbidden));
    }
    assert!(
        !manifest
            .lines()
            .any(|line| line.trim_start().starts_with("rustls ="))
    );
}

#[test]
fn python_adapter_collapses_tls_failures_without_exposing_configuration() {
    assert!(PYTHON_NATIVE.contains("binding_error(classify_client_error(error))"));
    assert!(!PYTHON_NATIVE.contains("TlsClientFailure::"));
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
        TLS,
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
        TLS,
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
