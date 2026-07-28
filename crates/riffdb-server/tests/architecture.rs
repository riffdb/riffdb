#![forbid(unsafe_code)]

//! Production dependency-policy guards for the hosted P1 server.

const MANIFEST: &str = include_str!("../Cargo.toml");
const LOCKFILE: &str = include_str!("../../../Cargo.lock");

fn rust_sources(root: &std::path::Path) -> Vec<(std::path::PathBuf, String)> {
    fn visit(root: &std::path::Path, sources: &mut Vec<(std::path::PathBuf, String)>) {
        for entry in std::fs::read_dir(root).expect("read server source directory") {
            let path = entry.expect("read server source entry").path();
            if path.is_dir() {
                visit(&path, sources);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let source = std::fs::read_to_string(&path).expect("read server Rust source");
                sources.push((path, source));
            }
        }
    }

    let mut sources = Vec::new();
    visit(root, &mut sources);
    sources.sort_by(|left, right| left.0.cmp(&right.0));
    sources
}

fn production_dependencies() -> &'static str {
    MANIFEST
        .split_once("[dependencies]")
        .expect("server dependencies section")
        .1
        .split_once("[dev-dependencies]")
        .expect("server dev-dependencies boundary")
        .0
}

fn development_dependencies() -> &'static str {
    MANIFEST
        .split_once("[dev-dependencies]")
        .expect("server dev-dependencies section")
        .1
        .split_once("[[test]]")
        .expect("server external-test boundary")
        .0
}

#[test]
fn production_transport_features_are_exact_and_default_disabled() {
    let production = production_dependencies();
    assert!(MANIFEST.contains("[features]\ndefault = []"));
    assert!(production.contains(
        "riffdb-api-grpc = { version = \"0.1.0\", path = \"../riffdb-api-grpc\", default-features = false, features = [\"server\"] }"
    ));
    assert!(production.contains(
        "tokio = { version = \"=1.52.0\", default-features = false, features = [\"macros\", \"net\", \"rt-multi-thread\", \"signal\", \"sync\", \"time\"] }"
    ));
    assert!(production.contains(
        "tonic = { version = \"=0.14.6\", default-features = false, features = [\"router\", \"server\"] }"
    ));
    assert!(production.contains(
        "axum = { version = \"=0.8.9\", default-features = false, features = [\"http1\", \"tokio\"] }"
    ));
    assert!(production.contains(
        "riffdb-api-mcp = { version = \"0.1.0\", path = \"../riffdb-api-mcp\", default-features = false, features = [\"streamable-http\"] }"
    ));

    for forbidden in [
        "riffdb-client-rust",
        "riffdb-proto",
        "riffdb-storage-memory",
        "base64 =",
        "features = [\"transport\"]",
        "channel",
        "tls",
        "compression",
        "gzip",
        "zstd",
    ] {
        assert!(
            !production.contains(forbidden),
            "forbidden production dependency capability: {forbidden}"
        );
    }
}

#[test]
fn client_and_public_message_helpers_remain_test_only() {
    let development = development_dependencies();
    assert!(development.contains("riffdb-client-rust"));
    assert!(development.contains("riffdb-proto"));
}

#[test]
fn lockfile_has_one_base64_and_no_tls_or_compression_stack() {
    assert_eq!(LOCKFILE.matches("name = \"base64\"").count(), 1);
    let base64 = LOCKFILE
        .split_once("name = \"base64\"")
        .expect("locked base64 package")
        .1;
    assert!(base64.starts_with("\nversion = \"0.22.1\""));

    for forbidden_package in [
        "rustls",
        "tokio-rustls",
        "native-tls",
        "openssl",
        "ring",
        "aws-lc-rs",
        "aws-lc-sys",
        "flate2",
        "zstd",
        "brotli",
    ] {
        assert!(
            !LOCKFILE.contains(&format!("name = \"{forbidden_package}\"")),
            "forbidden locked transport package: {forbidden_package}"
        );
    }
}

#[test]
fn server_source_cannot_receive_catalog_branded_migration_states() {
    let source_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let sources = rust_sources(&source_root);
    for forbidden in [
        "CatalogIndexMigrationBackend",
        "CatalogIndexMigrationScanRequest",
        "CatalogIndexMigrationScan",
        "CatalogIndexMigrationPage",
        "CatalogIndexMigrationExactEnd",
        "CatalogIndexMigrationBundleRequest",
        "CatalogIndexMigrationBundleResponse",
        "CatalogIndexMigrationInstruction",
        "CatalogIndexMigrationV1Rewrite",
        "CatalogIndexMigrationV2Confirm",
        "CatalogIndexMigrationBatch",
        "CatalogIndexMigrationPendingBatch",
        "CatalogIndexMigrationApplied",
        "CatalogIndexMigrationCompletion",
        "RedbIndexMigrationPage",
        "RedbIndexMigrationBatch",
    ] {
        for (path, source) in &sources {
            assert!(
                !source.contains(forbidden),
                "server source {} acquired branded migration state through `{forbidden}`",
                path.display()
            );
        }
    }

    let startup = std::fs::read_to_string(source_root.join("startup.rs"))
        .expect("read server startup source");
    assert!(startup.contains("CatalogIndexMigrationDriver::new(context, port)"));
}

#[test]
fn recovery_abort_controller_is_closed_and_absent_from_riffdbd_entrypoint() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let main = std::fs::read_to_string(root.join("main.rs")).expect("read riffdbd main");
    let daemon = std::fs::read_to_string(root.join("daemon.rs")).expect("read daemon");
    let controller = std::fs::read_to_string(root.join("maintenance_recovery_controller.rs"))
        .expect("read recovery controller");

    assert!(main.contains("riffdb_server::riffdbd_main()"));
    for forbidden in [
        "test_fixtures",
        "MaintenanceRecoveryTestPoint",
        "RIFFDB_WP190",
        "std::env",
    ] {
        assert!(
            !main.contains(forbidden),
            "normal riffdbd entrypoint acquired recovery fixture trigger `{forbidden}`"
        );
    }
    assert!(daemon.contains("run_from_process(MaintenanceRecoveryController::disabled())"));
    assert!(daemon.contains("#[cfg(feature = \"test-fixtures\")]"));
    assert!(!controller.contains("std::env"));
    assert!(!controller.contains("args_os"));
    assert!(!controller.contains("RIFFDB_"));
}
