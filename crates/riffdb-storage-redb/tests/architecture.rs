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

fn production_source(path: impl AsRef<Path>) -> String {
    read(path)
        .split("\n#[cfg(test)]\nmod tests")
        .next()
        .expect("production source prefix")
        .to_owned()
}

fn without_whitespace(source: &str) -> String {
    source.split_whitespace().collect()
}

#[test]
fn dependency_surface_keeps_redb_private_and_excludes_infrastructure_assemblies() {
    let manifest = read(crate_root().join("Cargo.toml"));
    assert!(manifest.contains("cap-std = { version = \"=4.0.2\", default-features = false }"));
    assert!(manifest.contains("redb = { version = \"=4.1.0\", default-features = false }"));
    assert!(manifest.contains("sha2 = { version = \"=0.11.0\", default-features = false }"));
    assert!(manifest.contains(
        "riffdb-catalog = { version = \"0.1.0\", path = \"../riffdb-catalog\", default-features = false }"
    ));
    let (dependencies, dev_dependencies) = manifest
        .split_once("[dev-dependencies]")
        .expect("redb manifest keeps normal and test-only dependencies separate");
    assert!(!dependencies.contains("riffdb-contract-compiler"));
    assert!(dev_dependencies.contains(
        "riffdb-contract-compiler = { version = \"0.1.0\", path = \"../riffdb-contract-compiler\", default-features = false }"
    ));
    for forbidden in [
        "criterion",
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
    assert!(!public_root.contains("pub use cap_std"));
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
fn only_startup_imports_the_catalog_driver_and_storage_never_imports_ir() {
    let sources = rust_sources();
    for forbidden in [
        "riffdb_contract_ir",
        "riffdb_invariant",
        "ValidatedCatalogHistory",
    ] {
        assert!(
            !sources.contains(forbidden),
            "storage source must not contain {forbidden}"
        );
    }

    let source = crate_root().join("src");
    for entry in fs::read_dir(source).expect("read source directory") {
        let path = entry.expect("source entry").path();
        if path.file_name().is_some_and(|name| name == "startup.rs")
            || path.extension().is_none_or(|extension| extension != "rs")
        {
            continue;
        }
        assert!(
            !read(&path).contains("riffdb_catalog"),
            "only startup migration may import catalog: {}",
            path.display()
        );
    }

    let startup = read(crate_root().join("src/startup.rs"));
    assert!(startup.contains("CatalogIndexMigrationBackend"));
    assert!(!startup.contains("ValidatedCatalogHistory"));
}

#[test]
fn public_root_exports_no_migration_intermediate_or_apply_surface() {
    let public_root = read(crate_root().join("src/lib.rs"));
    for forbidden in [
        "RedbIndexMigrationPage",
        "RedbIndexMigrationRow",
        "RedbIndexMigrationBundleRead",
        "RedbIndexMigrationBatch",
        "RedbIndexMigrationExactEnd",
    ] {
        assert!(
            !public_root.contains(forbidden),
            "public backend root must not export {forbidden}"
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

#[test]
fn index_migration_rechecks_replacement_charge_before_staging_any_write() {
    let startup = read(crate_root().join("src/startup.rs"));
    let charge_check = startup
        .find("replacement.encoded_content_charge().get()")
        .expect("migration replacement charge check");
    let insert = startup[charge_check..]
        .find("table\n                    .insert(key, replacement.as_bytes())")
        .map(|offset| charge_check + offset)
        .expect("migration replacement insertion");
    assert!(charge_check < insert);
    assert!(
        startup[charge_check..insert].contains("expected.conservative_v2_envelope_charge().get()")
    );
    assert!(startup[charge_check..insert].contains("return Err(invariant())"));
}

#[test]
fn startup_session_never_retains_a_redb_read_transaction() {
    let startup = read(crate_root().join("src/startup.rs"));
    let session = startup
        .split_once("pub struct RedbStructuralEvidenceSession {")
        .expect("startup session declaration")
        .1
        .split_once("\n}")
        .expect("startup session body")
        .0;
    assert!(!session.contains("ReadTransaction"));
    assert!(!session.contains("transaction:"));
    assert!(session.contains("durable_commit_epoch: u64"));
    assert!(startup.contains("fn open_snapshot_read(&self) -> Result<ReadTransaction"));
}

#[test]
fn every_live_database_engine_commit_routes_through_the_epoch_boundary() {
    let source_dir = crate_root().join("src");
    for entry in fs::read_dir(&source_dir).expect("read source directory") {
        let path = entry.expect("source entry").path();
        if path.extension().is_none_or(|extension| extension != "rs")
            || path.file_name().is_some_and(|name| {
                name == "store.rs"
                    || name == "startup.rs"
                    || name == "fixtures.rs"
                    || name == "benchmark_support.rs"
            })
        {
            continue;
        }
        let compact = without_whitespace(&production_source(&path));
        assert!(
            !compact.contains(".database.begin_write("),
            "live database writes must enter through store/startup epoch boundaries: {}",
            path.display()
        );
    }

    let store = without_whitespace(&production_source(source_dir.join("store.rs")));
    assert_eq!(store.matches("transaction.commit()").count(), 1);
    assert!(store.contains("fncommit_durable("));
    assert!(store.contains("self.shared.commit_durable(transaction)?"));
    assert!(store.contains("self.shared.commit_durable(transaction)"));

    let startup = without_whitespace(&production_source(source_dir.join("startup.rs")));
    assert!(!startup.contains("transaction.commit()"));
    assert_eq!(startup.matches("commit_durable(transaction)?").count(), 1);
}

#[test]
fn administration_writes_preserve_a_startup_proof_without_history_rescans() {
    let administration = without_whitespace(&production_source(
        crate_root().join("src/administration.rs"),
    ));
    let tail = administration
        .split_once("fnvalidate_administration_tail(")
        .expect("tail validator")
        .1
        .split_once("fnvalidate_administration_stream_readonly(")
        .expect("tail validator end")
        .0;
    assert!(tail.contains(".len()"));
    assert!(tail.contains(".last()"));
    assert!(!tail.contains(".iter()"));

    let full_read = administration
        .split_once("fnvalidate_administration_stream_readonly(")
        .expect("read validator")
        .1
        .split_once("fnallocate_sequences(")
        .expect("read validator end")
        .0;
    assert!(full_read.contains("validate_administration_table(&table,allocator)"));

    let append = administration
        .split_once("implServiceAuditAppendRepositoryforRedbOperationalPorts")
        .expect("service audit repository")
        .1
        .split_once("fnprincipal_matches_observation(")
        .expect("service audit repository end")
        .0;
    assert!(append.contains("validate_administration_tail(transaction)?"));
    assert!(!append.contains("validate_administration_table"));
}
