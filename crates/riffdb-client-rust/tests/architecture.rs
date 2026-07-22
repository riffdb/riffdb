#![forbid(unsafe_code)]

//! Architecture constraints for the public Rust transport client.

const LIB: &str = include_str!("../src/lib.rs");
const CLIENT: &str = include_str!("../src/client.rs");
const COMMAND: &str = include_str!("../src/command.rs");
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
    assert!(manifest.contains("default-features = false, features = [\"client\"]"));
    for forbidden in ["base64 =", "tls-", "gzip", "deflate", "zstd"] {
        assert!(!manifest.contains(forbidden));
    }
}

#[test]
fn operating_system_identity_sources_are_isolated_and_stateless() {
    for source in [CLIENT, COMMAND, METADATA, STATUS, GENERATED, LEGAL_SPEND] {
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
        COMMAND,
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
