#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Architecture evidence for the WP-576 remote destructive-restore drill.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("testkit has a repository parent")
        .to_path_buf()
}

#[test]
fn operator_container_has_authority_but_no_database_or_backup_mount() {
    let root = repository_root();
    let compose = fs::read_to_string(root.join("release/container/compose.yaml"))
        .expect("Compose source is readable");
    let operator = compose
        .split("  operator-probe:")
        .nth(1)
        .expect("operator service exists");
    assert!(operator.contains("/run/secrets/operator.credential"));
    assert!(operator.contains("/run/secrets/riffdb-ca.pem"));
    assert!(!operator.contains("/var/lib/riffdb/data"));
    assert!(!operator.contains("/var/lib/riffdb/backups"));
    assert!(!operator.contains("application.credential"));
}

#[test]
fn destructive_drill_uses_only_public_maintenance_and_proves_a_real_rewind() {
    let root = repository_root();
    let script = fs::read_to_string(root.join("scripts/remote-compose-acceptance"))
        .expect("remote acceptance source is readable");
    for required in [
        "fixtures/adapters/operational-conformance",
        "adapter_operational_remote_client",
        "adapter-before.json",
        "adapter-after.json",
        "restored four-domain adapter observations diverged",
        "operator_cli backup create alpha-disaster",
        "operator_cli backup operation \"$operation_id\"",
        "operator_cli backup restore alpha-disaster",
        "--confirm-replace-current-database",
        "poll_maintenance_operation",
        "find \"$expected_data_root\" -mindepth 1 -delete",
        "post-backup authority survived destructive restore",
        "backups/alpha-disaster/manifest.riffdb",
        "riffdb.adapter-disaster-receipt/v1",
        "frontier_binding",
        "expected_destroyed_suffix",
        "secrets_included: false",
    ] {
        assert!(
            script.contains(required),
            "missing disaster invariant: {required}"
        );
    }
    assert!(script.contains("refusing to destroy an unexpected data root"));
    assert!(!script.contains("operator_cli storage"));
    assert!(!script.contains("operator_cli capability bootstrap"));
}

#[test]
fn remote_drill_script_is_valid_shell() {
    let root = repository_root();
    for script in [
        "scripts/remote-compose-acceptance",
        "scripts/adapter-disaster-recovery-acceptance",
    ] {
        let status = Command::new("bash")
            .arg("-n")
            .arg(root.join(script))
            .status()
            .expect("bash is available");
        assert!(status.success(), "invalid shell: {script}");
    }
}

#[test]
fn adapter_gate_binds_all_four_manifests_to_the_remote_drill() {
    let root = repository_root();
    let script = fs::read_to_string(root.join("scripts/adapter-disaster-recovery-acceptance"))
        .expect("adapter disaster gate is readable");
    assert!(script.contains("for domain in openfga mlflow better-auth woodpecker"));
    assert!(script.contains("application conformance"));
    assert!(script.contains("./scripts/remote-compose-acceptance --backup-restore"));
}

#[test]
fn remote_drill_exercises_better_auth_state_and_labels_payload_as_regression_only() {
    let root = repository_root();
    let contract = fs::read_to_string(
        root.join("fixtures/adapters/operational-conformance/riffdb/contract.riff"),
    )
    .expect("operational contract is readable");
    let client = fs::read_to_string(root.join("tests/adapter_operational_remote_client.rs"))
        .expect("remote adapter client is readable");
    let script = fs::read_to_string(root.join("scripts/remote-compose-acceptance"))
        .expect("remote acceptance source is readable");

    for required in [
        "entity AuthUser",
        "entity AuthSession",
        "bulk command CreateAuthSessions",
    ] {
        assert!(
            contract.contains(required),
            "remote disaster corpus lacks Better Auth behavior: {required}"
        );
    }
    for required in [".create_auth_sessions(", ".get_auth_session("] {
        assert!(
            client.contains(required),
            "remote disaster client does not exercise Better Auth behavior: {required}"
        );
    }
    for required in [
        r#""adapters": ["mlflow", "openfga", "better-auth", "woodpecker"]"#,
        r#""regression_adapters": ["payload"]"#,
    ] {
        assert!(
            client.contains(required),
            "remote observation does not classify adapter evidence exactly: {required}"
        );
    }
    for required in [
        r#"adapters: ["mlflow", "openfga", "better-auth", "woodpecker"]"#,
        r#"regression_adapters: ["payload"]"#,
    ] {
        assert!(
            script.contains(required),
            "disaster receipt does not classify adapter evidence exactly: {required}"
        );
    }
}
