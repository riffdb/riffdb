#![forbid(unsafe_code)]

//! Cargo-resolved dependency proof for the observability leaf.

use std::path::PathBuf;
use std::process::Command;

fn production_dependencies(package_name: &str) -> Vec<String> {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = crate_root
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--locked", "--no-deps"])
        .current_dir(workspace_root)
        .output()
        .expect("cargo metadata runs");
    assert!(
        output.status.success(),
        "cargo metadata failed closed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata emits JSON");
    let package = metadata["packages"]
        .as_array()
        .expect("packages array")
        .iter()
        .find(|package| package["name"].as_str() == Some(package_name))
        .expect("workspace package");
    let mut dependencies = package["dependencies"]
        .as_array()
        .expect("dependencies array")
        .iter()
        .filter(|dependency| dependency["kind"].as_str() != Some("dev"))
        .map(|dependency| {
            dependency["name"]
                .as_str()
                .expect("dependency name")
                .to_owned()
        })
        .collect::<Vec<_>>();
    dependencies.sort();
    dependencies
}

// req: DEP-004
#[test]
fn observability_is_a_leaf_crate() {
    assert_eq!(
        production_dependencies("riffdb-observability"),
        ["riffdb-errors", "riffdb-types", "tracing"]
    );
}
