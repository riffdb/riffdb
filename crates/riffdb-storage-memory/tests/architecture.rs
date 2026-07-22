#![forbid(unsafe_code)]

//! Dependency and migration-authority guards for the reference storage adapter.

use std::fs;
use std::path::{Path, PathBuf};

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).expect("architecture input is readable UTF-8")
}

fn production_source(path: impl AsRef<Path>) -> String {
    read(path)
        .split("\n#[cfg(test)]")
        .next()
        .expect("production source prefix")
        .to_owned()
}

#[test]
fn catalog_edge_is_startup_only_and_compiler_is_test_only() {
    let manifest = read(crate_root().join("Cargo.toml"));
    let (dependencies, dev_dependencies) = manifest
        .split_once("[dev-dependencies]")
        .expect("memory manifest keeps normal and test-only dependencies separate");
    assert!(dependencies.contains(
        "riffdb-catalog = { version = \"0.1.0\", path = \"../riffdb-catalog\", default-features = false }"
    ));
    assert!(!dependencies.contains("riffdb-contract-compiler"));
    assert!(dev_dependencies.contains(
        "riffdb-contract-compiler = { version = \"0.1.0\", path = \"../riffdb-contract-compiler\", default-features = false }"
    ));

    let source = crate_root().join("src");
    for entry in fs::read_dir(source).expect("read source directory") {
        let path = entry.expect("source entry").path();
        if path.extension().is_none_or(|extension| extension != "rs") {
            continue;
        }
        let production = production_source(&path);
        for forbidden in [
            "riffdb_contract_compiler",
            "riffdb_contract_ir",
            "riffdb_invariant",
            "ValidatedCatalogHistory",
        ] {
            assert!(
                !production.contains(forbidden),
                "memory production source must not contain {forbidden}: {}",
                path.display()
            );
        }
        if path.file_name().is_none_or(|name| name != "startup.rs") {
            assert!(
                !production.contains("riffdb_catalog"),
                "only startup migration may import catalog: {}",
                path.display()
            );
        }
    }
}

#[test]
fn public_root_exports_no_migration_intermediate() {
    let public_root = read(crate_root().join("src/lib.rs"));
    for forbidden in [
        "MemoryIndexMigrationPage",
        "MemoryIndexMigrationRow",
        "MemoryIndexMigrationRowWithBundle",
        "MemoryIndexMigrationReadyBatch",
        "MemoryIndexMigrationExactEnd",
    ] {
        assert!(
            !public_root.contains(forbidden),
            "public backend root must not export {forbidden}"
        );
    }
}
