#![forbid(unsafe_code)]
//! Process-boundary coverage for retained alpha performance evidence.

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
fn retained_performance_evidence_is_complete_and_fail_closed() {
    let root = repository_root();
    let output = Command::new(root.join("scripts/check-alpha-performance-evidence"))
        .arg("--self-test")
        .current_dir(&root)
        .output()
        .expect("alpha performance evidence verifier must launch");

    assert!(
        output.status.success(),
        "performance evidence verification failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8(output.stdout).expect("verifier output must be UTF-8");
    assert!(stdout.contains("ineligible_performance_evidence: rejected"));
    assert!(stdout.contains("WP-552 retained interactive and write-only alpha evidence: passed"));
}
