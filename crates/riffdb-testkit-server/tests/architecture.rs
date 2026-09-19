#![forbid(unsafe_code)]

//! Cargo-metadata proof for the core/server test-kit boundary.

use std::path::PathBuf;
use std::process::Command;

// req: DEP-005
/// Root test targets this package owns that link no daemon: they inspect the
/// workspace itself rather than a running server, so they are not part of the
/// daemon-linked inventory the assertion below pins.
const NON_DAEMON_TEST_TARGETS: [&str; 2] = ["architecture", "unsafe_code_boundary"];

#[test]
fn testkit_core_excludes_daemon_edges_and_server_kit_owns_root_targets() {
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
    let packages = metadata["packages"].as_array().expect("packages array");
    let core = packages
        .iter()
        .find(|package| package["name"].as_str() == Some("riffdb-testkit"))
        .expect("core test kit");
    let dependencies = core["dependencies"]
        .as_array()
        .expect("dependency array")
        .iter()
        .filter_map(|dependency| dependency["name"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for forbidden in ["riffdb-service", "riffdb-server", "riffdb-cli"] {
        assert!(
            !dependencies.contains(forbidden),
            "core test kit retains forbidden edge {forbidden}"
        );
    }

    let server = packages
        .iter()
        .find(|package| package["name"].as_str() == Some("riffdb-testkit-server"))
        .expect("server test kit");
    let daemon_linked_targets = server["targets"]
        .as_array()
        .expect("target array")
        .iter()
        .filter(|target| {
            target["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind.as_str() == Some("test")))
                && !NON_DAEMON_TEST_TARGETS.contains(&target["name"].as_str().unwrap_or_default())
        })
        .count();
    assert_eq!(daemon_linked_targets, 30);
}
