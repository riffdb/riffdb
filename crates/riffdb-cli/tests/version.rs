#![forbid(unsafe_code)]
//! Real-process offline build identity and argument-isolation regressions.
// req: DX-003, ID-005, VER-001
use std::process::{Command, Stdio};
#[test]
fn version_flags_are_offline_and_precede_configuration_or_storage() {
    let root = tempfile::TempDir::new().unwrap();
    for flag in ["--version", "-V"] {
        let output = Command::new(env!("CARGO_BIN_EXE_riffdb"))
            .env_clear()
            .current_dir(root.path())
            .stdin(Stdio::null())
            .env("RIFFDB_CONFIG", "missing-secret-config-canary")
            .env("RIFFDB_ENDPOINT", "invalid-secret-endpoint-canary")
            .env("RIFFDB_CREDENTIAL_FILE", "missing-secret-token-canary")
            .env("RIFFDB_OUTPUT", "invalid-secret-output-canary")
            .arg(flag)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(0));
        assert_eq!(
            output.stdout,
            concat!("riffdb ", env!("CARGO_PKG_VERSION"), "\n").as_bytes()
        );
        assert!(output.stderr.is_empty());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
#[test]
fn mixed_version_flags_fail_consistently_before_side_effects() {
    let root = tempfile::TempDir::new().unwrap();
    for arguments in [
        vec!["--version", "--config", "secret-canary"],
        vec!["--config", "secret-canary", "--version"],
        vec!["--version", "-V"],
        vec!["server", "health", "--version"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_riffdb"))
            .env_clear()
            .current_dir(root.path())
            .stdin(Stdio::null())
            .args(arguments)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_eq!(
            output.stderr,
            b"riffdb: invalid_version_arguments: --version/-V must be used alone\n"
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
