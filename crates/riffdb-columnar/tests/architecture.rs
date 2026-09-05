//! Interface-safety proof for the worker-only shutdown seam.

use std::path::{Path, PathBuf};

fn rust_sources(root: &Path) -> Vec<(PathBuf, String)> {
    let mut pending = vec![root.to_path_buf()];
    let mut sources = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path).expect("read source directory") {
            let path = entry.expect("source entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let source = std::fs::read_to_string(&path).expect("read Rust source");
                sources.push((path, source));
            }
        }
    }
    sources
}

// req: PERF-007, PERF-008
#[test]
fn worker_shutdown_seam_is_absent_from_application_transport_and_operator_surfaces() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    for crate_name in [
        "riffdb-api-grpc",
        "riffdb-api-mcp",
        "riffdb-cli",
        "riffdb-client-python-native",
        "riffdb-client-rust",
        "riffdb-driver-host",
        "riffdb-mcp-stdio",
        "riffdb-service",
    ] {
        let root = workspace.join("crates").join(crate_name).join("src");
        for (path, source) in rust_sources(&root) {
            for forbidden in ["WorkerApplyOutcome", "apply_available_for_worker"] {
                assert!(
                    !source.contains(forbidden),
                    "{} re-exports the worker-only seam through {forbidden}",
                    path.display()
                );
            }
        }
    }

    let engine = std::fs::read_to_string(workspace.join("crates/riffdb-columnar/src/engine.rs"))
        .expect("read columnar engine");
    assert!(engine.contains("#[doc(hidden)]\n    pub fn apply_available_for_worker("));

    let server = rust_sources(&workspace.join("crates/riffdb-server/src"));
    let callers = server
        .iter()
        .filter(|(_, source)| source.contains(".apply_available_for_worker("))
        .map(|(path, _)| {
            path.file_name()
                .expect("server source file")
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(callers, ["columnar_worker.rs"]);
}

// req: PRJ-008, PRJ-009, PRJ-010, OQ-022
#[test]
fn prepared_generation_mint_authority_is_columnar_owned_and_absent_from_public_surfaces() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let storage_control = std::fs::read_to_string(
        workspace.join("crates/riffdb-storage-api/src/columnar_control.rs"),
    )
    .expect("read storage control source");
    assert!(!storage_control.contains("PreparedColumnarGenerationV1"));

    let prepared = std::fs::read_to_string(
        workspace.join("crates/riffdb-columnar/src/prepared_generation.rs"),
    )
    .expect("read prepared generation owner");
    assert!(prepared.contains("pub struct PreparedColumnarGenerationV1"));
    assert!(prepared.contains("pub(crate) fn from_validated("));
    assert!(!prepared.contains("pub fn from_validated("));
    for forbidden_field in [
        "pub source:",
        "pub definition_semantics_hash:",
        "pub generation:",
        "pub process_generation:",
    ] {
        assert!(!prepared.contains(forbidden_field));
    }

    for crate_name in [
        "riffdb-api-grpc",
        "riffdb-api-mcp",
        "riffdb-cli",
        "riffdb-client-python-native",
        "riffdb-client-rust",
        "riffdb-driver-host",
        "riffdb-mcp-stdio",
        "riffdb-service",
    ] {
        let root = workspace.join("crates").join(crate_name).join("src");
        for (path, source) in rust_sources(&root) {
            assert!(
                !source.contains("PreparedColumnarGenerationV1"),
                "{} exposes prepared-generation authority",
                path.display()
            );
        }
    }
}
