#![forbid(unsafe_code)]
// req: DX-003, ID-005, END-007, END-008
//! Exercise the real process boundary, including failure and final sink drain.

use serde_json::Value;
use std::process::{Command, Stdio};

#[test]
fn json_startup_failure_and_final_accounting_do_not_corrupt_stdout() {
    let root = tempfile::TempDir::new().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_riffdbd"))
        .env_clear()
        .current_dir(root.path())
        .stdin(Stdio::null())
        .args([
            "--log-format",
            "json",
            "--log-components",
            "projection=debug",
            "--capability-keys",
            "missing-secret-canary.keys",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        output.stdout.is_empty(),
        "operator output must not enter the daemon evidence protocol"
    );
    assert!(output.stderr.len() < 64 * 1024);
    let text = std::str::from_utf8(&output.stderr).unwrap();
    assert!(!text.contains("missing-secret-canary"));
    assert!(!text.contains(root.path().to_str().unwrap()));
    let records: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(records.iter().all(|v| v["schema"] == "riffdb.log/v1"));
    assert!(records.iter().any(|v|v["event_code"]=="RDB-LOG-0004"&&v["attributes"]["cause"]=="digest_keys"));
    assert_eq!(
        records
            .iter()
            .filter(|v| v["event_code"] == "RDB-LOG-0200")
            .count(),
        2,
        "one process startup sample and one final sample must drain even on failure"
    );
}

#[test]
fn invalid_logging_configuration_is_bounded_redacted_and_precedes_storage() {
    let root = tempfile::TempDir::new().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_riffdbd"))
        .env_clear()
        .current_dir(root.path())
        .stdin(Stdio::null())
        .args(["--log-components", "secret-canary-dependency=trace"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let text = std::str::from_utf8(&output.stderr).unwrap();
    assert!(text.contains("RDB-CONFIG-0001"));
    assert!(text.contains("logging configuration is invalid"));
    assert!(!text.contains("secret-canary"));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[cfg(unix)]
#[test]
fn protected_file_only_logs_drain_and_leave_stderr_empty() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::TempDir::new().unwrap();
    let directory = root.path().join("logs");
    let output = Command::new(env!("CARGO_BIN_EXE_riffdbd"))
        .env_clear()
        .current_dir(root.path())
        .stdin(Stdio::null())
        .args([
            "--log-format",
            "json",
            "--log-stderr",
            "false",
            "--log-directory",
        ])
        .arg(&directory)
        .args(["--capability-keys", "secret-path-canary"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    let path = directory.join("riffdb.log");
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(!text.contains("secret-path-canary"));
    assert!(!text.contains(root.path().to_str().unwrap()));
    let records: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(records.iter().all(|v| v["schema"] == "riffdb.log/v1"));
    assert!(records.iter().any(|v| v["event_code"] == "RDB-LOG-0004"));
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o7777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(directory).unwrap().permissions().mode() & 0o7777,
        0o700
    );
}
