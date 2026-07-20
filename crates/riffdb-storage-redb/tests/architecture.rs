#![forbid(unsafe_code)]

//! Dependency and readiness-boundary guards for the concrete redb adapter.

use std::fs;
use std::path::{Path, PathBuf};

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).expect("architecture input is readable UTF-8")
}

fn rust_sources() -> String {
    let source = crate_root().join("src");
    let mut paths = fs::read_dir(source)
        .expect("read source directory")
        .map(|entry| entry.expect("source entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect::<Vec<_>>();
    paths.sort();
    paths.into_iter().map(read).collect::<Vec<_>>().join("\n")
}

#[test]
fn dependency_surface_keeps_redb_private_and_excludes_infrastructure_assemblies() {
    let manifest = read(crate_root().join("Cargo.toml"));
    assert!(manifest.contains("redb = { version = \"=4.1.0\", default-features = false }"));
    assert!(manifest.contains("sha2 = { version = \"=0.11.0\", default-features = false }"));
    for forbidden in [
        "criterion",
        "riffdb-catalog",
        "riffdb-contract-ir",
        "riffdb-storage-memory",
        "rmcp",
        "tokio",
        "tonic",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "redb adapter must not depend on {forbidden}"
        );
    }

    let public_root = read(crate_root().join("src/lib.rs"));
    assert!(!public_root.contains("pub use redb"));
    assert!(!public_root.contains("extern crate redb"));
}

#[test]
fn sha256_dependency_is_confined_to_offline_backup_manifests() {
    let source = crate_root().join("src");
    for entry in fs::read_dir(source).expect("read source directory") {
        let path = entry.expect("source entry").path();
        if path.file_name().is_some_and(|name| name == "backup.rs")
            || path.extension().is_none_or(|extension| extension != "rs")
        {
            continue;
        }
        assert!(
            !read(&path).contains("sha2"),
            "sha2 must remain confined to backup.rs: {}",
            path.display()
        );
    }
}

#[test]
fn storage_never_imports_contract_ir_or_catalog_readiness_proofs() {
    let sources = rust_sources();
    for forbidden in [
        "riffdb_catalog",
        "riffdb_contract_ir",
        "ValidatedCatalogHistory",
    ] {
        assert!(
            !sources.contains(forbidden),
            "storage source must not contain {forbidden}"
        );
    }
}

#[test]
fn only_operational_ports_implement_semantic_runtime_traits() {
    let sources = rust_sources();
    for forbidden in [
        "impl SnapshotReader for RedbStore",
        "impl AdmissionRepository for RedbStore",
        "impl ApplicationCommandTransactionPort for RedbStore",
        "impl SnapshotReader for RedbDormantPorts",
        "impl AdmissionRepository for RedbDormantPorts",
        "impl ApplicationCommandTransactionPort for RedbDormantPorts",
    ] {
        assert!(
            !sources.contains(forbidden),
            "readiness bypass: {forbidden}"
        );
    }
    assert!(sources.contains("impl SnapshotReader for RedbOperationalPorts"));
    assert!(sources.contains("impl ApplicationCommandTransactionPort for RedbOperationalPorts"));
}
