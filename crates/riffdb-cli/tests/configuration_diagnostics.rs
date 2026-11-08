#![forbid(unsafe_code)]
// req: DX-003, DX-005, ID-005
//! Real CLI resolution must fail before dialing and preserve v1 bytes.
use serde_json::Value;
use std::process::{Command, Output};
fn invoke(arguments: &[&str], environment: &[(&str, &str)]) -> Output {
    let root = tempfile::TempDir::new().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_riffdb"));
    command
        .env_clear()
        .current_dir(root.path())
        .args(arguments)
        .envs(environment.iter().copied());
    command.output().unwrap()
}
#[test]
fn configuration_diagnostics_preserve_winning_source_and_safe_cause() {
    let output = invoke(
        &["--endpoint", "secret-endpoint-canary", "server", "health"],
        &[("RIFFDB_ENDPOINT", "http://127.0.0.1:7443")],
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let text = std::str::from_utf8(&output.stderr).unwrap();
    assert!(text.contains("setting=client.endpoint source=flag cause=endpoint_invalid"));
    assert!(text.contains("fix="));
    assert!(!text.contains("secret-endpoint-canary"));
    let output = invoke(
        &["--output", "json-v2", "server", "health"],
        &[("RIFFDB_MAX_ATTEMPTS", "secret-retry-canary")],
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["diagnostics"][0]["setting"], "client.max_attempts");
    assert_eq!(value["diagnostics"][0]["source"], "environment");
    assert_eq!(value["diagnostics"][0]["cause"], "retry_bounds");
    assert!(!String::from_utf8_lossy(&output.stdout).contains("secret-retry-canary"));
}
#[test]
fn json_v2_diagnostics_preserve_json_v1_compatibility() {
    let arguments = [
        "--output",
        "json",
        "--max-attempts",
        "11",
        "server",
        "health",
    ];
    let v1 = invoke(&arguments, &[]);
    assert_eq!(v1.status.code(), Some(2));
    assert!(v1.stderr.is_empty());
    assert_eq!(v1.stdout,b"{\"schema\":\"riffdb.cli.output/v1\",\"command\":\"server.health\",\"ok\":false,\"error\":{\"type\":\"local\",\"code\":\"configuration_invalid\",\"message\":\"client configuration is invalid\"}}\n");
    let v2 = invoke(
        &[
            "--output",
            "json-v2",
            "--max-attempts",
            "11",
            "server",
            "health",
        ],
        &[],
    );
    assert_eq!(v2.status.code(), Some(2));
    assert!(v2.stderr.is_empty());
    let old: Value = serde_json::from_slice(&v1.stdout).unwrap();
    let new: Value = serde_json::from_slice(&v2.stdout).unwrap();
    assert_eq!(
        v2.stdout,
        include_bytes!(
            "../../../fixtures/diagnostics/cli-output-v2/server.health.configuration_invalid.jsonl"
        )
    );
    assert_eq!(new["schema"], "riffdb.cli.output/v2");
    assert_eq!(new["error"], old["error"]);
    assert_eq!(new["diagnostics"][0]["cause"], "retry_bounds");
    assert!(v2.stdout.len() < 65536);
}

#[test]
fn file_shape_tls_identity_and_selected_output_errors_are_specific_and_value_free() {
    let root = tempfile::TempDir::new().unwrap();
    let path = root.path().join("config.toml");
    for (source, setting, cause) in [
        (
            "[client]\nsecret_canary_setting = 'secret-canary-value'\n",
            "config",
            "unknown_setting",
        ),
        ("client = [\n", "config", "toml_invalid"),
        (
            "[client]\nmax_attempts = 'secret-canary-value'\n",
            "client.max_attempts",
            "toml_shape",
        ),
        (
            "[client]\nmax_attempts = -1\n",
            "client.max_attempts",
            "retry_bounds",
        ),
        (
            "[client]\ndatabase = 'Secret-Canary-Alias'\n",
            "client.database",
            "database_invalid",
        ),
        (
            "[client]\nmax_attempts = 11\n",
            "client.max_attempts",
            "retry_bounds",
        ),
    ] {
        std::fs::write(&path, source).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_riffdb"))
            .env_clear()
            .current_dir(root.path())
            .args(["--output", "json-v2", "--config"])
            .arg(&path)
            .args(["server", "health"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stderr.is_empty());
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["diagnostics"][0]["setting"], setting, "{source}");
        assert_eq!(value["diagnostics"][0]["cause"], cause, "{source}");
        assert_eq!(
            value["diagnostics"][0]["source"],
            if setting == "config" { "flag" } else { "file" }
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(!text.contains("secret_canary_setting"));
        assert!(!text.contains("secret-canary-value"));
        assert!(!text.contains("Secret-Canary-Alias"));
        assert!(!text.contains(root.path().to_str().unwrap()));
    }
    for (arguments, setting, cause) in [
        (
            vec![
                "--endpoint",
                "https://example.com:7443",
                "--tls-trust-root",
                "relative-canary.pem",
            ],
            "client.tls_trust_root",
            "absolute_path_required",
        ),
        (
            vec![
                "--endpoint",
                "https://example.com:7443",
                "--tls-server-name",
                "secret-canary.example.com",
            ],
            "client.tls_server_name",
            "tls_identity_mismatch",
        ),
        (
            vec!["--tls-server-name", "secret-canary.example.com"],
            "client.tls_server_name",
            "tls_requires_https",
        ),
        (
            vec![
                "--endpoint",
                "https://example.com:7443",
                "--tls-client-cert",
                "/synthetic-client.pem",
            ],
            "client.tls_client_key",
            "tls_pair_required",
        ),
    ] {
        let mut args = vec!["--output", "json-v2"];
        args.extend(arguments);
        args.extend(["server", "health"]);
        let output = invoke(&args, &[]);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stderr.is_empty());
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["diagnostics"][0]["setting"], setting);
        assert_eq!(value["diagnostics"][0]["cause"], cause);
        assert!(!String::from_utf8_lossy(&output.stdout).contains("secret-canary"));
    }
    std::fs::write(&path, "[client]\noutput = 'json-v2'\nmax_attempts = 11\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_riffdb"))
        .env_clear()
        .args(["--config"])
        .arg(&path)
        .args(["server", "health"])
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schema"], "riffdb.cli.output/v2");
    let output = invoke(
        &["server", "health"],
        &[("RIFFDB_OUTPUT", "json-v2"), ("RIFFDB_MAX_ATTEMPTS", "11")],
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schema"], "riffdb.cli.output/v2");
}

#[cfg(unix)]
#[test]
fn protected_tls_failure_preserves_sdk_cause_and_effective_source() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::TempDir::new().unwrap();
    let path = root.path().join("canary-ca.pem");
    std::fs::write(&path, "synthetic-public-trust-root-canary").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
    for mode in ["human", "json-v2", "json"] {
        let output = Command::new(env!("CARGO_BIN_EXE_riffdb"))
            .env_clear()
            .current_dir(root.path())
            .env("RIFFDB_TLS_TRUST_ROOT", &path)
            .args([
                "--output",
                mode,
                "--endpoint",
                "https://127.0.0.1:7443",
                "server",
                "health",
            ])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        let bytes = if mode == "human" {
            assert!(output.stdout.is_empty());
            &output.stderr
        } else {
            assert!(output.stderr.is_empty());
            &output.stdout
        };
        let text = String::from_utf8_lossy(bytes);
        assert!(!text.contains("canary"));
        assert!(!text.contains(root.path().to_str().unwrap()));
        if mode == "human" {
            assert!(text.contains(
                "setting=client.tls_trust_root source=environment cause=trust_root_permissions"
            ));
        } else if mode == "json-v2" {
            let value: Value = serde_json::from_slice(bytes).unwrap();
            assert_eq!(value["diagnostics"][0]["cause"], "trust_root_permissions");
            assert_eq!(value["diagnostics"][0]["source"], "environment");
        } else {
            let value: Value = serde_json::from_slice(bytes).unwrap();
            assert_eq!(value["schema"], "riffdb.cli.output/v1");
            assert!(value.get("diagnostics").is_none());
        }
    }
}

#[cfg(unix)]
#[test]
fn credential_custody_diagnostics_preserve_the_original_safe_failure() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::TempDir::new().unwrap();
    let path = root.path().join("credential-canary");
    for (length, mode, cause) in [(43, 0o644, "permissions"), (44, 0o600, "invalid_length")] {
        std::fs::write(&path, "A".repeat(length)).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_riffdb"))
            .env_clear()
            .current_dir(root.path())
            .env("RIFFDB_CREDENTIAL_FILE", &path)
            .args(["--output", "json-v2", "server", "health"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stderr.is_empty());
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["error"]["code"], "credential_invalid");
        assert_eq!(value["diagnostics"][0]["cause"], cause);
        assert_eq!(value["diagnostics"][0]["source"], "environment");
        assert_eq!(value["diagnostics"][0]["setting"], "client.credential_file");
        assert!(!String::from_utf8_lossy(&output.stdout).contains("credential-canary"));
    }
}

#[test]
fn saga_opt_in_v2_uses_the_same_bounded_terminal_without_changing_legacy_output() {
    let tail = [
        "saga",
        "get",
        "--saga-id",
        "018f22a1-7b3c-7def-8123-456789abcdef",
    ];
    let mut args = vec!["--output", "json-v2"];
    args.extend(tail);
    let output = invoke(&args, &[]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schema"], "riffdb.cli.output/v2");
    assert_eq!(value["command"], "saga.get");
    assert_eq!(value["error"]["code"], "credential_required");
    assert_eq!(value["diagnostics"], serde_json::json!([]));
    let mut args = vec!["--output", "json"];
    args.extend(tail);
    let output = invoke(&args, &[]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(
        output.stderr,
        b"error: --credential-file is required for saga operations\n"
    );
}
