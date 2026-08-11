#![forbid(unsafe_code)]

//! Architecture pins for the least-authority scheduler crate.

use std::fs;
use std::path::PathBuf;

#[test]
fn scheduler_dependency_graph_has_no_storage_kernel_or_transport_authority() {
    let manifest = fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .expect("scheduler manifest");
    for forbidden in [
        "riffdb-storage",
        "riffdb-commit",
        "riffdb-runtime",
        "riffdb-proto",
        "riffdb-api-grpc",
        "riffdb-api-mcp",
        "riffdb-auth",
        "riffdb-policy",
        "redb",
        "tonic",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "scheduler must not depend on {forbidden}"
        );
    }
}

#[test]
fn scheduler_source_exposes_only_closed_application_operations() {
    let source = fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"))
        .expect("scheduler source");
    for forbidden in [
        "GetEntity",
        "ScanIndex",
        "execute_query(",
        "execute_command(",
        "field_id",
        "entity_type_id",
        "capability_mask",
        "std::thread::sleep",
        "tokio::time::sleep",
        "SystemTime::now",
    ] {
        assert!(
            !source.contains(forbidden),
            "scheduler source contains forbidden authority or timing surface: {forbidden}"
        );
    }
    for required in [
        "fn next(",
        "fn resolve_business(",
        "fn claim(",
        "fn execute(",
        "fn release(",
        "fn acknowledge(",
        "fn negative_acknowledge(",
    ] {
        assert!(
            source.contains(required),
            "missing closed operation: {required}"
        );
    }
}
