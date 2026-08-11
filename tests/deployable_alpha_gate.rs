#![forbid(unsafe_code)]
//! Structure coverage for the final installed alpha gate.

use std::path::PathBuf;
use std::process::Command;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("testkit must remain under crates/")
        .to_path_buf()
}

#[test]
fn final_gate_has_one_closed_phase_inventory() {
    let root = repository_root();
    let output = Command::new(root.join("scripts/deployable-alpha-acceptance"))
        .arg("--self-test")
        .current_dir(&root)
        .output()
        .expect("deployable alpha gate self-test must launch");
    assert!(
        output.status.success(),
        "gate inventory failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("gate output must be UTF-8"),
        "deployable alpha phase inventory: passed\n"
    );
}
