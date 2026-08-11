#![forbid(unsafe_code)]
//! Process-boundary acceptance for the closed endurance receipt validator.

use std::path::PathBuf;
use std::process::Command;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("testkit must remain under crates/")
        .to_path_buf()
}

fn task_temporary_root() -> PathBuf {
    PathBuf::from(std::env::var_os("HOME").expect("HOME must name the test user's home"))
        .join("tmp")
}

#[test]
fn self_test_rejects_incomplete_and_invalid_endurance_evidence() {
    let root = repository_root();
    let output = Command::new(root.join("scripts/alpha-endurance"))
        .arg("--self-test")
        .current_dir(&root)
        .env("RIFFDB_TMP_ROOT", task_temporary_root())
        .output()
        .expect("alpha-endurance self-test must launch");

    assert!(
        output.status.success(),
        "self-test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8(output.stdout).expect("self-test output must be UTF-8");
    assert!(stdout.contains("short_duration: rejected"));
    assert!(stdout.contains("invalid_structure: rejected"));
    assert!(stdout.contains("host_interference: rejected"));
    assert!(stdout.contains("missing_lifecycle: rejected"));
    assert!(stdout.contains("resource_leak: rejected"));
    assert!(stdout.contains("quadratic_lifecycle: rejected"));
    assert!(stdout.contains("queue_growth: rejected"));
    assert!(stdout.contains("hidden_retry: rejected"));
    assert!(stdout.contains("missing_generation: rejected"));
    assert!(stdout.contains("starvation: rejected"));
    assert!(stdout.contains("storage_unavailable: rejected"));
    assert!(stdout.contains("conformance_failure: rejected"));
    assert!(stdout.contains("valid_complete_run: passed"));
}
