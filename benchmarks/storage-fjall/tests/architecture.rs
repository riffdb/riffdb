#![forbid(unsafe_code)]

//! Architecture and dependency-isolation evidence for WP-075.

use std::fs;
use std::path::{Path, PathBuf};

fn manifest_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).expect("read WP-075 architecture input")
}

#[test]
fn workspace_is_isolated_and_fjall_is_exactly_pinned_without_defaults() {
    let root = manifest_root();
    let manifest = read(root.join("Cargo.toml"));
    assert!(manifest.contains("[workspace]"));
    assert!(manifest.contains("members = [\".\"]"));
    assert!(manifest.contains("fjall = { version = \"=3.1.8\", default-features = false }"));
    assert!(!manifest.contains("redb ="));

    let repository_manifest = read(root.join("../../Cargo.toml"));
    assert!(!repository_manifest.contains("benchmarks/storage-fjall"));
    assert!(!repository_manifest.contains("fjall ="));
}

#[test]
fn lockfile_excludes_the_disabled_lz4_feature_and_records_the_exact_engine() {
    let lock = read(manifest_root().join("Cargo.lock"));
    assert!(lock.contains("name = \"fjall\"\nversion = \"3.1.8\""));
    assert!(!lock.contains("name = \"lz4_flex\""));
}

#[test]
fn first_party_comparison_code_forbids_unsafe_and_does_not_claim_semantic_impls() {
    let root = manifest_root();
    assert!(read(root.join("src/lib.rs")).contains("#![forbid(unsafe_code)]"));
    let mut sources = Vec::new();
    for directory in ["src", "tests", "benches"] {
        collect_rs(&root.join(directory), &mut sources);
    }
    assert!(!sources.is_empty());
    let unsafe_block = ["unsafe", " {"].concat();
    for source_path in sources {
        let source = read(&source_path);
        assert!(!source.contains(&unsafe_block));
        if source_path
            .file_name()
            .is_some_and(|name| name != "architecture.rs")
        {
            assert!(!source.contains("impl ProjectionMutationRepository for"));
            assert!(!source.contains("impl ApplicationCommandTransactionPort for"));
            assert!(!source.contains("impl StructuralEvidenceOpen for"));
            assert!(!source.contains("impl CatalogIndexMigrationBackend for"));
        }
    }
}

fn collect_rs(directory: &Path, output: &mut Vec<PathBuf>) {
    if !directory.exists() {
        return;
    }
    for entry in fs::read_dir(directory).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            collect_rs(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}
