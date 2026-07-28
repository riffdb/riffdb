#![forbid(unsafe_code)]

//! Architecture boundaries for the public-only CLI package.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

const DIRECT_DEPENDENCIES: [&str; 9] = [
    "base64",
    "clap",
    "riffdb-auth",
    "riffdb-client-rust",
    "serde",
    "serde_json",
    "tokio",
    "toml",
    "zeroize",
];
const EXACT_DEPENDENCY_ROWS: &str = concat!(
    "base64 = { version = \"=0.22.1\", default-features = false, features = [\"alloc\"] }\n",
    "clap = { version = \"=4.6.3\", default-features = false, features = [\"derive\", \"std\", \"help\", \"usage\", \"error-context\"] }\n",
    "riffdb-auth = { version = \"0.1.0\", path = \"../riffdb-auth\", default-features = false }\n",
    "riffdb-client-rust = { version = \"0.1.0\", path = \"../riffdb-client-rust\", default-features = false }\n",
    "serde = { version = \"=1.0.229\", default-features = false, features = [\"derive\", \"std\"] }\n",
    "serde_json = { version = \"=1.0.150\", default-features = false, features = [\"std\"] }\n",
    "tokio = { version = \"=1.52.0\", default-features = false, features = [\"macros\", \"rt-multi-thread\"] }\n",
    "toml = { version = \"=1.1.3\", default-features = false, features = [\"parse\", \"serde\", \"std\"] }\n",
    "zeroize = { version = \"=1.8.1\", default-features = false, features = [\"alloc\"] }\n",
);

#[test]
fn production_manifest_matches_the_accepted_direct_allowlist() {
    let manifest = fs::read_to_string(root().join("Cargo.toml")).expect("manifest");
    let dependencies = manifest
        .split("[dependencies]\n")
        .nth(1)
        .expect("dependencies")
        .split("\n\n")
        .next()
        .expect("dependency section")
        .lines()
        .filter_map(|line| line.split_once(" = ").map(|(name, _)| name))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        dependencies,
        DIRECT_DEPENDENCIES.into_iter().collect::<BTreeSet<_>>()
    );
    assert_eq!(
        manifest
            .split("[dependencies]\n")
            .nth(1)
            .expect("dependencies")
            .split("\n\n")
            .next()
            .expect("dependency rows"),
        EXACT_DEPENDENCY_ROWS.trim_end()
    );
    assert!(manifest.contains("[features]\ndefault = []"));
    assert!(!manifest.contains("unsafe"));
}

#[test]
fn source_has_no_internal_database_or_unchecked_transport_path() {
    let source = production_source();
    for forbidden in [
        "riffdb_api_grpc",
        "riffdb_catalog",
        "riffdb_commit",
        "riffdb_conflict",
        "riffdb_contract",
        "riffdb_errors",
        "riffdb_idempotency",
        "riffdb_policy",
        "riffdb_proto",
        "riffdb_runtime",
        "riffdb_server",
        "riffdb_service",
        "riffdb_storage",
        "riffdb_types",
        "tonic::",
        "redb",
        "load_capability_token_file",
    ] {
        assert!(
            !source.contains(forbidden),
            "forbidden source edge: {forbidden}"
        );
    }
    assert!(source.contains("RiffDbClient"));
    assert!(source.contains("execute_with_retry"));
    assert!(source.contains("create_bootstrap_capability_with_retry"));
    assert!(source.contains("create_capability_with_retry"));
    assert!(source.contains("load_protected_bearer_credential"));
}

#[test]
fn auth_access_is_confined_to_the_isolated_bootstrap_module() {
    let source = production_source();
    let auth_lines = source
        .lines()
        .filter(|line| line.contains("riffdb_auth"))
        .collect::<Vec<_>>();
    assert!(!auth_lines.is_empty());
    assert!(
        auth_lines
            .iter()
            .all(|line| line.contains("riffdb_auth::bootstrap_secret"))
    );
    assert!(!source.contains("use riffdb_auth::*"));
    assert!(!source.contains("pub use riffdb_auth"));
}

#[test]
fn bootstrap_durability_and_demo_process_boundaries_are_structural() {
    let app = fs::read_to_string(root().join("src/app.rs")).expect("app");
    let bootstrap = app
        .split("CapabilityCommand::Bootstrap")
        .nth(1)
        .expect("bootstrap branch")
        .split("CapabilityCommand::Create")
        .next()
        .expect("bounded branch");
    let material = bootstrap
        .find("bootstrap_material(")
        .expect("durable material");
    let metadata = bootstrap.find("material.metadata()").expect("metadata");
    let connect = bootstrap
        .find("connect(config)")
        .expect("public connection");
    let rpc = bootstrap
        .find("submit_bootstrap_retry")
        .expect("injected retry boundary");
    assert!(material < metadata && metadata < connect && connect < rpc);
    assert!(
        app.contains("self.create_bootstrap_capability_with_retry(template, attempts, metadata)"),
        "concrete retry boundary must use the public SDK helper"
    );

    let runner = fs::read_to_string(root().join("src/runner.rs")).expect("runner");
    for required in [
        "Command::new(runner)",
        ".env_clear()",
        ".stdin(Stdio::null())",
        "RUNNER_DEADLINE: Duration = Duration::from_secs(180)",
        "RUNNER_REAP_DEADLINE: Duration = Duration::from_secs(5)",
        "MAX_RUNNER_STREAM_BYTES: usize = 4_096",
        "child.kill_and_reap()",
        "receiver.recv_timeout(RUNNER_REAP_DEADLINE)",
        "Err(()) => Err(RunnerError::ProtocolInvalid)",
    ] {
        assert!(
            runner.contains(required),
            "missing runner boundary: {required}"
        );
    }
    for forbidden in [
        "Command::new(\"sh\")",
        "Command::new(\"bash\")",
        "Command::new(\"cmd\")",
        "Command::new(\"powershell\")",
    ] {
        assert!(!runner.contains(forbidden), "shell bypass: {forbidden}");
    }
}

#[test]
fn retention_abort_controller_is_closed_and_absent_from_riffdb_entrypoint() {
    let main = fs::read_to_string(root().join("src/main.rs")).expect("main");
    let library = fs::read_to_string(root().join("src/lib.rs")).expect("library");
    let credential = fs::read_to_string(root().join("src/credential.rs")).expect("credential");

    assert!(main.contains("riffdb_cli::run().await"));
    for forbidden in [
        "test_fixtures",
        "BootstrapRetentionTestPoint",
        "RIFFDB_WP190",
        "std::env",
    ] {
        assert!(
            !main.contains(forbidden),
            "normal riffdb entrypoint acquired retention fixture trigger `{forbidden}`"
        );
    }
    assert!(library.contains("#[cfg(feature = \"test-fixtures\")]"));
    assert!(credential.contains("RetentionRecoveryController::disabled()"));
    assert!(!credential.contains("RIFFDB_WP190"));
}

#[test]
fn accepted_interface_checkpoint_still_verifies_byte_for_byte() {
    let status = Command::new(root().join("interface/verify-checkpoint"))
        .arg("--check")
        .status()
        .expect("checkpoint verifier starts");
    assert!(status.success());
}

#[test]
fn configured_json_mode_is_preserved_for_other_configuration_failures() {
    let output = Command::new(env!("CARGO_BIN_EXE_riffdb"))
        .args(["server", "health"])
        .env_clear()
        .env("RIFFDB_OUTPUT", "json")
        .env("RIFFDB_ENDPOINT", "bad")
        .output()
        .expect("CLI starts");
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        output.stdout,
        include_bytes!("../fixtures/output-v1/server.health.local_error.jsonl")
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn demo_dry_run_is_truthful_and_does_not_relay_credentials() {
    const CANARY: &str = "riffdb_demo_secret_canary";
    let output = Command::new(root().join("../../scripts/demo"))
        .arg("--dry-run")
        .env("RIFFDB_CAPABILITY_TOKEN", CANARY)
        .output()
        .expect("demo dry-run starts");
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        output.stdout,
        concat!(
            "riffdb-demo-v1: WP-150 dry-run checks the packaged public comparison handoff\n",
            "riffdb-demo-v1: live mode requires an already prepared loopback RiffDB server\n",
            "riffdb-demo-v1: live mode requires RIFFDB_DEMO_RUNNER and RIFFDB_CREDENTIAL_FILE\n",
            "riffdb-demo-v1: live mode runs the checked sequential public comparison\n",
            "riffdb-demo-v1: child output is validated and never relayed\n",
            "riffdb-demo-v1: full one-command POC orchestration remains owned by WP-200\n",
        )
        .as_bytes()
    );
    assert!(
        !output
            .stdout
            .windows(CANARY.len())
            .any(|bytes| bytes == CANARY.as_bytes())
    );
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn production_source() -> String {
    let mut paths = fs::read_dir(root().join("src"))
        .expect("src")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut source = String::new();
    for path in paths {
        source.push_str(&fs::read_to_string(&path).expect("source"));
    }
    source
}
