#![forbid(unsafe_code)]

//! Production dependency-policy guards for the hosted P1 server.

const MANIFEST: &str = include_str!("../Cargo.toml");
const LOCKFILE: &str = include_str!("../../../Cargo.lock");
const LIFECYCLE_SERVICE: &str = include_str!("../src/lifecycle_service.rs");
const LIFECYCLE: &str = include_str!("../src/lifecycle.rs");
const COLUMNAR_ADAPTER: &str = include_str!("../src/columnar_adapter.rs");
const COLUMNAR_WORKER: &str = include_str!("../src/columnar_worker.rs");
const PROCESS_GRAPH: &str = include_str!("../src/process_graph.rs");

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
// req: STO-023, REC-004, PERF-019
fn graceful_checkpoint_receipt_has_only_one_closed_output_path() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let sources = rust_sources(&root);
    let type_holders = sources
        .iter()
        .filter(|(_, source)| source.contains("GracefulCheckpointCloseReceiptV1"))
        .map(|(path, _)| {
            path.file_name()
                .expect("server source file")
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(type_holders, ["process_graph.rs", "storage.rs"]);

    let daemon = std::fs::read_to_string(root.join("daemon.rs")).expect("read daemon");
    assert!(
        daemon.contains("eprintln!(\"{}\", shutdown_stages.format_checkpoint_close_v1_line())")
    );
    for public_boundary in [
        "hosted_mcp.rs",
        "main.rs",
        "lifecycle.rs",
        "lifecycle_service.rs",
        "operational_status.rs",
        "runtime_support.rs",
    ] {
        let source = std::fs::read_to_string(root.join(public_boundary))
            .expect("read public boundary source");
        for forbidden in [
            "GracefulCheckpointCloseReceiptV1",
            "riffdb-graceful-checkpoint-close-v1",
            "format_checkpoint_close_v1_line",
        ] {
            assert!(
                !source.contains(forbidden),
                "{public_boundary} could expose graceful checkpoint receipt through {forbidden}"
            );
        }
    }

    let process_graph =
        std::fs::read_to_string(root.join("process_graph.rs")).expect("read process graph");
    let formatter = process_graph
        .split_once("pub(crate) fn format_checkpoint_close_v1_line")
        .expect("closed receipt formatter")
        .1
        .split_once("}")
        .expect("formatter end")
        .0;
    assert!(formatter.contains("self.checkpoint_close.format_v1_line()"));
    for forbidden in ["metrics", "Mcp", "Status", "Error", "path", "identity"] {
        assert!(!formatter.contains(forbidden));
    }
}

// req: PERF-019
#[test]
fn dedicated_production_threads_apply_the_fixed_stack_budget() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let library = std::fs::read_to_string(root.join("lib.rs")).expect("read server library");
    assert!(library.contains("PRODUCTION_THREAD_STACK_BYTES: usize = 384 * 1024;"));
    for owner in [
        "daemon.rs",
        "port_driver.rs",
        "exact_text_adapter.rs",
        "projection_worker.rs",
        "columnar_worker.rs",
    ] {
        let source = std::fs::read_to_string(root.join(owner)).expect("read thread owner");
        assert!(
            source.contains(".stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)"),
            "{owner} must apply the fixed production stack budget"
        );
    }
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

#[test]
// req: PERF-019, PRJ-008, PRJ-009, OQ-019, OQ-020, OQ-022
fn columnar_activation_is_private_single_worker_and_absent_from_public_surfaces() {
    let cold_registration = COLUMNAR_ADAPTER
        .split_once("pub(crate) fn open(\n        storage: SharedRedbOperationalPorts,")
        .expect("columnar cold registration boundary")
        .1
        .split_once("/// Shared notifier for register-before-read waits.")
        .expect("columnar cold registration end")
        .0;
    assert!(!cold_registration.contains("ColumnarEngine::open("));
    assert_eq!(COLUMNAR_ADAPTER.matches("ColumnarEngine::open(").count(), 1);
    let production_adapter = COLUMNAR_ADAPTER
        .split_once("#[cfg(test)]")
        .expect("columnar adapter test boundary")
        .0;
    for request_owned_task in ["thread::spawn", "tokio::spawn", "JoinHandle"] {
        assert!(
            !production_adapter.contains(request_owned_task),
            "columnar request path acquired task ownership through `{request_owned_task}`"
        );
    }

    assert_eq!(
        COLUMNAR_WORKER
            .matches(".name(\"riffdb-columnar\".to_owned())")
            .count(),
        1
    );
    let production_graph = PROCESS_GRAPH
        .split_once("#[cfg(test)]")
        .expect("process graph test boundary")
        .0;
    assert_eq!(
        production_graph
            .matches("RunningColumnarWorker::start(")
            .count(),
        1
    );
    assert!(COLUMNAR_WORKER.contains("RunningColumnarWorker([DERIVED_AUTHORITY])"));

    let source_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for public_boundary in ["config.rs", "daemon.rs", "hosted_mcp.rs", "main.rs"] {
        let source = std::fs::read_to_string(source_root.join(public_boundary))
            .expect("read public server boundary");
        for forbidden in [
            "columnar_activation",
            "columnar_prewarm",
            "columnar_eager",
            "prewarm_columnar",
            "eager_columnar",
        ] {
            assert!(
                !source.contains(forbidden),
                "{public_boundary} exposed private activation control `{forbidden}`"
            );
        }
    }
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
// req: NET-005
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
        "rustls-webpki = { version = \"=0.103.14\", default-features = false, features = [\"std\"] }"
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

    for forbidden in [
        "riffdb-client-rust",
        "riffdb-proto",
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

// req: PERF-014
#[test]
fn external_kill_fixture_feature_propagates_to_the_storage_backend() {
    assert!(MANIFEST.contains("\"riffdb-storage-redb/test-fixtures\","));
}

#[test]
fn client_and_public_message_helpers_remain_test_only() {
    let development = development_dependencies();
    assert!(development.contains("riffdb-client-rust"));
    assert!(development.contains("riffdb-proto"));
}

#[test]
// req: NET-005
fn lockfile_has_one_exact_ring_tls_stack_and_no_alternative_or_compression_stack() {
    assert_eq!(LOCKFILE.matches("name = \"base64\"").count(), 1);
    let base64 = LOCKFILE
        .split_once("name = \"base64\"")
        .expect("locked base64 package")
        .1;
    assert!(base64.starts_with("\nversion = \"0.22.1\""));

    for (package, version) in [
        ("ring", "0.17.14"),
        ("rustls", "0.23.45"),
        ("rustls-pki-types", "1.15.1"),
        ("rustls-webpki", "0.103.14"),
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
