#![forbid(unsafe_code)]

//! WP-753 three-hand-written-places architecture proof.

use std::path::PathBuf;
use std::process::Command;

// req: GEN-006
#[test]
fn check_three_places_rejects_hand_written_adapter_edits() {
    let manifest_directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repository = manifest_directory
        .parent()
        .and_then(|path| path.parent())
        .expect("query-module crate is under the repository crates directory");
    let output = Command::new(repository.join("scripts/check-three-places"))
        .arg("--self-test")
        .output()
        .expect("check-three-places must be executable");

    assert!(
        output.status.success(),
        "check-three-places self-test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
