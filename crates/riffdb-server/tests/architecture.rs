#![forbid(unsafe_code)]

//! Production dependency-policy guards for the hosted P1 server.

const MANIFEST: &str = include_str!("../Cargo.toml");
const LOCKFILE: &str = include_str!("../../../Cargo.lock");
const LIFECYCLE_SERVICE: &str = include_str!("../src/lifecycle_service.rs");
const LIFECYCLE: &str = include_str!("../src/lifecycle.rs");
const COLUMNAR_ADAPTER: &str = include_str!("../src/columnar_adapter.rs");

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
fn lifecycle_wrappers_cover_the_symbolic_catalog_surface() {
    for source in [LIFECYCLE_SERVICE, LIFECYCLE] {
        assert!(
            source.contains("get_application_catalog"),
            "every lifecycle wrapper must fail closed or delegate the complete symbolic catalog surface"
        );
    }
    assert!(LIFECYCLE_SERVICE.contains(") -> ApplicationCatalogResult => DescribeContract;"));
}

#[test]
fn production_vector_adapter_admits_current_authoritative_rows_before_ranking() {
    let execution = COLUMNAR_ADAPTER
        .split_once("impl VectorProjectionPort for ServerColumnarProjectionPort")
        .expect("production vector projection port")
        .1
        .split_once("fn trusted_commit_lag_ms(")
        .expect("vector execution boundary")
        .0;
    let evidence = execution
        .find("inspect_vector_evidence(")
        .expect("authoritative candidate and policy evidence");
    let admission = execution
        .find("ProductionVectorAdmission")
        .expect("current-model candidate admission");
    let ranking = execution
        .find("nearest_query_snapshot_with_admission(")
        .expect("admission-aware ranking");
    assert!(evidence < admission && admission < ranking);
    assert!(execution.contains("NonZeroU16::new(500)"));
    assert!(!execution.contains("nearest_query_snapshot("));
}

fn locked_version(package: &str) -> &'static str {
    let marker = format!("name = \"{package}\"\nversion = \"");
    LOCKFILE
        .split_once(&marker)
        .unwrap_or_else(|| panic!("missing locked package {package}"))
        .1
        .split_once('"')
        .expect("locked version terminator")
        .0
}

#[test]
fn production_transport_features_are_exact_default_disabled_and_confined() {
    let production = production_dependencies();
    assert!(MANIFEST.contains("[features]\ndefault = []"));
    assert!(production.contains(
        "riffdb-api-grpc = { version = \"0.1.0\", path = \"../riffdb-api-grpc\", default-features = false, features = [\"server\"] }"
    ));
    assert!(production.contains(
        "tokio = { version = \"=1.52.0\", default-features = false, features = [\"macros\", \"net\", \"rt-multi-thread\", \"signal\", \"sync\", \"time\"] }"
    ));
    assert!(production.contains(
        "tonic = { version = \"=0.14.6\", default-features = false, features = [\"router\", \"server\", \"tls-ring\"] }"
    ));
    assert!(production.contains(
        "tokio-stream = { version = \"=0.1.18\", default-features = false, features = [\"net\"] }"
    ));
    assert!(production.contains(
        "futures-util = { version = \"=0.3.33\", default-features = false, features = [\"async-await\", \"std\"] }"
    ));
    assert!(production.contains(
        "rustls-pki-types = { version = \"=1.15.1\", default-features = false, features = [\"std\"] }"
    ));
    assert!(production.contains(
        "rustls-webpki = { version = \"=0.103.13\", default-features = false, features = [\"std\"] }"
    ));
    assert!(production.contains(
        "tokio-rustls = { version = \"=0.26.4\", default-features = false, features = [\"logging\", \"ring\", \"tls12\"] }"
    ));
    assert!(production.contains(
        "zeroize = { version = \"=1.8.1\", default-features = false, features = [\"alloc\"] }"
    ));
    assert!(production.contains(
        "axum = { version = \"=0.8.9\", default-features = false, features = [\"http1\", \"tokio\"] }"
    ));
    assert!(production.contains(
        "riffdb-api-mcp = { version = \"0.1.0\", path = \"../riffdb-api-mcp\", default-features = false, features = [\"streamable-http\"] }"
    ));
    assert!(MANIFEST.contains(
        "test-fixtures = [\"dep:prost\", \"dep:riffdb-api-exclusive\", \"dep:riffdb-proto\", \"tokio/io-util\"]"
    ));
    assert!(production.contains(
        "riffdb-proto = { version = \"0.1.0\", path = \"../riffdb-proto\", default-features = false, optional = true }"
    ));

    for forbidden in [
        "riffdb-client-rust",
        "riffdb-storage-memory",
        "base64 =",
        "features = [\"transport\"]",
        "channel",
        "compression",
        "gzip",
        "zstd",
        "tls-aws-lc",
        "ring =",
    ] {
        assert!(
            !production.contains(forbidden),
            "forbidden production dependency capability: {forbidden}"
        );
    }
    assert!(
        !production
            .lines()
            .any(|line| line.trim_start().starts_with("rustls ="))
    );
}

#[test]
fn client_and_public_message_helpers_remain_test_only() {
    let development = development_dependencies();
    assert!(development.contains("riffdb-client-rust"));
    assert!(development.contains("riffdb-proto"));
}

#[test]
fn lockfile_has_one_exact_ring_tls_stack_and_no_alternative_or_compression_stack() {
    assert_eq!(LOCKFILE.matches("name = \"base64\"").count(), 1);
    let base64 = LOCKFILE
        .split_once("name = \"base64\"")
        .expect("locked base64 package")
        .1;
    assert!(base64.starts_with("\nversion = \"0.22.1\""));

    for (package, version) in [
        ("ring", "0.17.14"),
        ("rustls", "0.23.43"),
        ("rustls-pki-types", "1.15.1"),
        ("rustls-webpki", "0.103.13"),
        ("tokio-rustls", "0.26.4"),
    ] {
        assert_eq!(
            LOCKFILE.matches(&format!("name = \"{package}\"")).count(),
            1
        );
        assert_eq!(locked_version(package), version);
    }

    for forbidden_package in [
        "native-tls",
        "openssl",
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
fn cryptography_is_nameable_only_by_reviewed_transport_manifests_and_server_source() {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    for crate_name in [
        "riffdb-service",
        "riffdb-commit",
        "riffdb-storage-api",
        "riffdb-storage-redb",
        "riffdb-runtime",
        "riffdb-config",
    ] {
        let crate_root = workspace.join("crates").join(crate_name);
        let manifest = std::fs::read_to_string(crate_root.join("Cargo.toml"))
            .expect("read confined crate manifest");
        for forbidden in ["rustls =", "tokio-rustls =", "ring =", "tls-ring"] {
            assert!(
                !manifest.contains(forbidden),
                "{crate_name} named confined TLS dependency `{forbidden}`"
            );
        }
        for (path, source) in rust_sources(&crate_root.join("src")) {
            for forbidden in [
                "rustls::",
                "tokio_rustls::",
                "use ring::",
                "ring::aead",
                "ring::digest",
                "ring::rand",
                "ring::signature",
            ] {
                assert!(
                    !source.contains(forbidden),
                    "confined source {} named `{forbidden}`",
                    path.display()
                );
            }
        }
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

#[test]
fn direct_stream_probe_is_confined_to_test_fixtures() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let library = std::fs::read_to_string(root.join("src/lib.rs")).expect("read server library");
    let daemon = std::fs::read_to_string(root.join("src/daemon.rs")).expect("read daemon");
    let diagnostic = std::fs::read_to_string(root.join("src/direct_stream_diagnostic.rs"))
        .expect("read direct-stream diagnostic");

    assert!(library.contains("#[cfg(feature = \"test-fixtures\")]\nmod direct_stream_diagnostic;"));
    assert!(daemon.contains(
        "#[cfg(feature = \"test-fixtures\")]\nuse crate::direct_stream_diagnostic::HostedDirectStreamDiagnostic;"
    ));
    assert!(diagnostic.contains("RIFFDB_DIRECT_STREAM_DIAGNOSTIC"));
    assert!(diagnostic.contains("config.max_early_data_size = 0;"));
    assert!(diagnostic.contains("config.send_half_rtt_data = false;"));
    assert!(!diagnostic.contains("send_half_rtt_data = true"));
    for (path, source) in rust_sources(&root.join("src")) {
        if path.ends_with("direct_stream_diagnostic.rs") {
            continue;
        }
        assert!(
            !source.contains("RIFFDB_DIRECT_STREAM_DIAGNOSTIC"),
            "production server source {} acquired the direct-stream diagnostic trigger",
            path.display()
        );
        assert!(
            !source.contains("riffdb_api_exclusive"),
            "production server source {} acquired the diagnostic protocol crate",
            path.display()
        );
    }
}
