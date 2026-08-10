//! Dependency and ambient-authority checks for the simulator boundary.
//!
//! ADR-0113 composes the simulator as a dev-only crate and ADR-0012 forbids
//! any test clock, scheduler seed, or fuzz seed from compiling into the
//! production runtime path, so `riffdb-sim` must never enter a production
//! dependency graph — and the simulator itself must be free of ambient
//! nondeterminism, or its determinism pin proves nothing.

use std::{fs, path::PathBuf};

const SIM_MANIFEST: &str = include_str!("../Cargo.toml");
const ROOT_MANIFEST: &str = include_str!("../../../Cargo.toml");

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

/// The workspace member directories named by the root manifest.
fn workspace_members() -> Vec<PathBuf> {
    let members: Vec<PathBuf> = ROOT_MANIFEST
        .lines()
        .skip_while(|line| !line.starts_with("members = ["))
        .skip(1)
        .take_while(|line| !line.trim_start().starts_with(']'))
        .map(|line| workspace_root().join(line.trim().trim_matches(&['"', ','][..])))
        .collect();
    assert!(
        members.len() > 30,
        "workspace member parse collapsed: {members:?}"
    );
    members
}

/// The `[dependencies]` section names of one manifest; `[dev-dependencies]`
/// are deliberately excluded — development-only use is the sanctioned shape.
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

#[test]
fn no_production_crate_depends_on_the_simulator() {
    let mut checked = 0;
    for member in workspace_members() {
        let manifest_path = member.join("Cargo.toml");
        let manifest = fs::read_to_string(&manifest_path)
            .unwrap_or_else(|_| panic!("read {}", manifest_path.display()));
        let owners = production_dependency_names(&manifest);
        assert!(
            !owners.iter().any(|name| name == "riffdb-sim"),
            "{} lists riffdb-sim in [dependencies]; the simulator must never \
             enter a production dependency graph (ADR-0113, ADR-0012)",
            manifest_path.display()
        );
        checked += 1;
    }
    assert!(checked > 30, "member manifests were not actually checked");
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
        vec!["redb"],
        "the simulator depends on exactly the engine under simulation"
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
