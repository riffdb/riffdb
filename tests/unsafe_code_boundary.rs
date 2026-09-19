#![forbid(unsafe_code)]

//! The workspace forbids unsafe code. Exactly one compilation unit is allowed
//! below that bar -- the `riffdbd` binary root, which must select a global
//! allocator -- and exactly one item inside it. This test pins that boundary so
//! a second relaxation cannot arrive unnoticed.

use std::fs;
use std::path::{Path, PathBuf};

/// The single package permitted to lower `unsafe_code` from forbid to deny.
const ALLOWED_PACKAGE: &str = "riffdb-server";
/// The single file permitted to contain `#[allow(unsafe_code)]`.
const ALLOWED_FILE: &str = "crates/riffdb-server/src/main.rs";

/// `CARGO_MANIFEST_DIR` is the owning package, which is not the workspace root,
/// so walk up to the manifest that declares the workspace.
fn workspace_root() -> PathBuf {
    let mut candidate = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        let manifest = candidate.join("Cargo.toml");
        if fs::read_to_string(&manifest).is_ok_and(|manifest| manifest.contains("[workspace]")) {
            return candidate;
        }
        assert!(
            candidate.pop(),
            "no workspace manifest above the test package"
        );
    }
}

fn crate_manifests() -> Vec<PathBuf> {
    let mut manifests = fs::read_dir(workspace_root().join("crates"))
        .expect("crates directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("Cargo.toml"))
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    manifests.sort();
    assert!(manifests.len() > 1, "expected many crates");
    manifests
}

fn rust_sources(root: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn the_workspace_still_forbids_unsafe_code() {
    let manifest = fs::read_to_string(workspace_root().join("Cargo.toml")).expect("workspace");
    assert!(
        manifest.contains("unsafe_code = \"forbid\""),
        "the workspace lint table must still forbid unsafe code"
    );
}

#[test]
fn only_the_server_package_lowers_the_unsafe_code_lint() {
    for manifest_path in crate_manifests() {
        let manifest = fs::read_to_string(&manifest_path).expect("crate manifest");
        let package = manifest_path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .expect("crate directory name")
            .to_owned();

        if manifest.contains("unsafe_code = ") {
            assert_eq!(
                package, ALLOWED_PACKAGE,
                "{package} declares its own unsafe_code lint; only {ALLOWED_PACKAGE} may"
            );
            assert!(
                manifest.contains("unsafe_code = \"deny\""),
                "{ALLOWED_PACKAGE} must lower the lint to deny, never below it"
            );
        } else {
            assert!(
                manifest.contains("workspace = true"),
                "{package} must inherit the workspace lints"
            );
        }
    }
}

#[test]
fn every_library_root_still_forbids_unsafe_code() {
    for manifest_path in crate_manifests() {
        let crate_root = manifest_path.parent().expect("crate directory");
        let library = crate_root.join("src/lib.rs");
        if !library.is_file() {
            continue;
        }
        let source = fs::read_to_string(&library).expect("library root");
        assert!(
            source.contains("#![forbid(unsafe_code)]"),
            "{} must forbid unsafe code at its root",
            library.display()
        );
    }
}

#[test]
fn exactly_one_file_allows_unsafe_code_and_only_for_the_global_allocator() {
    let mut sources = Vec::new();
    rust_sources(&workspace_root().join("crates"), &mut sources);
    sources.sort();

    let mut relaxed = Vec::new();
    for path in &sources {
        let source = fs::read_to_string(path).expect("rust source");
        if !source.contains("#[allow(unsafe_code)]") {
            continue;
        }
        let relative = path
            .strip_prefix(workspace_root())
            .expect("path under the workspace")
            .to_string_lossy()
            .replace('\\', "/");
        assert_eq!(
            source.matches("#[allow(unsafe_code)]").count(),
            1,
            "{relative} may relax the lint for at most one item"
        );
        assert!(
            source.contains("#[global_allocator]"),
            "{relative} relaxes the lint for something other than the global allocator"
        );
        relaxed.push(relative);
    }

    assert_eq!(
        relaxed,
        vec![ALLOWED_FILE.to_owned()],
        "exactly one file may relax the unsafe_code lint"
    );
}
