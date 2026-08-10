//! Dependency and ambient-authority checks for the simulator boundary.
//!
//! ADR-0113 composes the simulator as a dev-only crate and ADR-0012 forbids
//! any test clock, scheduler seed, or fuzz seed from compiling into the
//! production runtime path, so `riffdb-sim` must never enter a production
//! dependency graph (`SIM-007`) — and the simulator itself must be free of
//! ambient nondeterminism, or its determinism pin proves nothing.

use std::{fs, path::PathBuf};

const SIM_MANIFEST: &str = include_str!("../Cargo.toml");

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn workspace_root() -> PathBuf {
    crate_root()
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

/// The `[dependencies]` section names of one manifest; `[dev-dependencies]`
/// are deliberately excluded — development-only use is the sanctioned shape.
/// (Used only on riffdb-sim's own small manifest; the workspace-wide boundary
/// check below drives from `cargo metadata`, never from hand parsing.)
fn production_dependency_names(manifest: &str) -> Vec<String> {
    manifest
        .lines()
        .skip_while(|line| *line != "[dependencies]")
        .skip(1)
        .take_while(|line| !line.starts_with('['))
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim().to_owned()))
        .collect()
}

fn rust_sources(root: PathBuf) -> Vec<PathBuf> {
    let mut pending = vec![root];
    let mut sources = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).expect("read source directory") {
            let path = entry.expect("source entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }
    sources.sort();
    sources
}

/// SIM-007: the simulator is absent from every production dependency graph.
///
/// Drives from `cargo metadata` rather than hand-parsed manifests, so it sees
/// the complete cargo-resolved member set (including members cargo promotes
/// via path dependencies that the root `members` array never names, such as
/// `riffdb-query-syntax`) and every dependency declaration form — inline
/// tables, `[dependencies.name]` tables, `workspace = true` inheritance,
/// `[build-dependencies]`, and target-specific sections. Only `kind == "dev"`
/// edges are exempt: a build dependency genuinely compiles the simulator into
/// the production build graph, which ADR-0012 forbids.
#[test]
fn no_production_crate_depends_on_the_simulator() {
    let output = std::process::Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--locked", "--no-deps"])
        .current_dir(workspace_root())
        .output()
        .expect("cargo metadata runs");
    assert!(
        output.status.success(),
        "cargo metadata failed (a lockfile-drifting dependency edit also \
         fails closed here): {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata emits valid JSON");

    let member_ids: Vec<&str> = metadata["workspace_members"]
        .as_array()
        .expect("workspace_members array")
        .iter()
        .map(|id| id.as_str().expect("member id string"))
        .collect();
    assert!(!member_ids.is_empty(), "no workspace members resolved");

    let mut checked_names = Vec::new();
    for package in metadata["packages"].as_array().expect("packages array") {
        let id = package["id"].as_str().expect("package id");
        if !member_ids.contains(&id) {
            continue;
        }
        let name = package["name"].as_str().expect("package name");
        for dependency in package["dependencies"]
            .as_array()
            .expect("package dependencies array")
        {
            let dependency_name = dependency["name"].as_str().expect("dependency name");
            // `kind` is null for normal dependencies, "build" or "dev"
            // otherwise; the `target` field never exempts an edge.
            let kind = dependency["kind"].as_str().unwrap_or("normal");
            assert!(
                !(dependency_name == "riffdb-sim" && kind != "dev"),
                "{name} lists riffdb-sim as a {kind} dependency \
                 (target {}); the simulator must never enter a production \
                 dependency graph (ADR-0113, ADR-0012, SIM-007)",
                dependency["target"]
            );
        }
        checked_names.push(name.to_owned());
    }
    assert_eq!(
        checked_names.len(),
        member_ids.len(),
        "every cargo-resolved workspace member must be dependency-checked"
    );
    // Canary members: the simulator itself, and the member cargo promotes
    // into the workspace without a root members-array entry — the exact crate
    // a manifest-walking parser missed.
    for canary in ["riffdb-sim", "riffdb-query-syntax"] {
        assert!(
            checked_names.iter().any(|name| name == canary),
            "cargo-resolved member set lost {canary}; the boundary check no \
             longer covers the crates it was built to cover"
        );
    }
}

#[test]
fn simulator_manifest_carries_no_entropy_async_or_clock_dependencies() {
    for forbidden in ["rand", "getrandom", "tokio"] {
        assert!(
            !SIM_MANIFEST.contains(forbidden),
            "riffdb-sim manifest names forbidden dependency {forbidden}; \
             determinism is the product"
        );
    }
    let dependencies = production_dependency_names(SIM_MANIFEST);
    assert_eq!(
        dependencies,
        vec!["redb", "riffdb-storage-redb"],
        "the simulator depends on exactly the engine under simulation and \
         the production storage crate whose media port it implements \
         (SIM-B; the correct-direction dev-crate-on-production-crate edge \
         ADR-0113 composes — SIM-007 constrains the reverse direction only)"
    );
}

#[test]
fn simulator_sources_have_no_ambient_time_entropy_or_hash_randomness() {
    let sources = rust_sources(crate_root().join("src"));
    assert!(!sources.is_empty(), "simulator sources were not found");
    for path in sources {
        let source = fs::read_to_string(&path).expect("simulator source is UTF-8");
        for forbidden in [
            "SystemTime",
            "Instant::now",
            "std::time::",
            "thread_rng",
            "rand::",
            "getrandom",
            "tokio::",
            "async fn",
            ".await",
            // Hash-randomized iteration would leak into trace digests.
            "HashMap",
            "HashSet",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} contains forbidden nondeterminism source {forbidden}",
                path.display()
            );
        }
    }
    let lib_source = fs::read_to_string(crate_root().join("src/lib.rs")).expect("lib.rs");
    assert!(lib_source.contains("#![forbid(unsafe_code)]"));
}
