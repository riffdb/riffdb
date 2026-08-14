#![forbid(unsafe_code)]
//! Process-boundary acceptance for the closed endurance receipt validator.

use std::process::Command;
use std::{fs, path::PathBuf};

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
    assert!(stdout.contains("action_manifest_drift: rejected"));
    assert!(stdout.contains("missing_environment_lifecycle: rejected"));
    assert!(stdout.contains("short_duration: rejected"));
    assert!(stdout.contains("invalid_structure: rejected"));
    assert!(stdout.contains("host_interference: rejected"));
    assert!(stdout.contains("missing_lifecycle: rejected"));
    assert!(stdout.contains("missing_fault: rejected"));
    assert!(stdout.contains("missing_language_progress: rejected"));
    assert!(stdout.contains("missing_workload_progress: rejected"));
    assert!(stdout.contains("missing_tenant_progress: rejected"));
    assert!(stdout.contains("missing_restart_observation: rejected"));
    assert!(stdout.contains("unreceipted_action: rejected"));
    assert!(stdout.contains("forged_action_result: rejected"));
    assert!(stdout.contains("inaccurate_process_inventory: rejected"));
    assert!(stdout.contains("too_few_conformance_checks: rejected"));
    assert!(stdout.contains("missing_domain: rejected"));
    assert!(stdout.contains("policy_mismatch: rejected"));
    assert!(stdout.contains("data_loss: rejected"));
    assert!(stdout.contains("resource_leak: rejected"));
    assert!(stdout.contains("quadratic_lifecycle: rejected"));
    assert!(stdout.contains("queue_growth: rejected"));
    assert!(stdout.contains("hidden_retry: rejected"));
    assert!(stdout.contains("missing_generation: rejected"));
    assert!(stdout.contains("starvation: rejected"));
    assert!(stdout.contains("storage_unavailable: rejected"));
    assert!(stdout.contains("conformance_failure: rejected"));
    assert!(stdout.contains("release_evidence_omission: rejected"));
    assert!(stdout.contains("forged_release_binding: rejected"));
    assert!(stdout.contains("bound_release_receipt: passed"));
    assert!(stdout.contains("valid_complete_run: passed"));
}

#[test]
fn controller_rejects_unreceipted_lifecycle_success() {
    let root = repository_root();
    let output = Command::new(root.join("scripts/alpha-endurance-controller"))
        .arg("--self-test")
        .current_dir(&root)
        .env("RIFFDB_TMP_ROOT", task_temporary_root())
        .output()
        .expect("alpha-endurance controller self-test must launch");

    assert!(
        output.status.success(),
        "controller self-test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8(output.stdout).expect("self-test output must be UTF-8");
    assert!(stdout.contains("nonadvancing_lifecycle_result: rejected"));
    assert!(stdout.contains("bounded_action_result: passed"));
    assert!(stdout.contains("bounded_environment_result: passed"));
    assert!(stdout.contains("silent_loss_conformance: rejected"));
    assert!(stdout.contains("bounded_conformance_result: passed"));
}

#[test]
fn endurance_receipts_use_the_accepted_four_alpha_domains() {
    let root = repository_root();
    for script in [
        "scripts/alpha-endurance",
        "scripts/alpha-endurance-controller",
    ] {
        let source = fs::read_to_string(root.join(script)).expect("endurance source is readable");
        assert!(
            source.contains(
                "REQUIRED_DOMAINS = {\"openfga\", \"mlflow\", \"better-auth\", \"woodpecker\"}"
            ),
            "{script} does not bind the accepted alpha adapter inventory"
        );
        assert!(
            !source.contains("\"payload\""),
            "{script} incorrectly promotes the retained Payload regression into the alpha gate"
        );
    }
}

#[test]
fn endurance_environment_is_tls_exact_and_least_authority() {
    let root = repository_root();
    let script = root.join("scripts/endurance-environment");
    let syntax = Command::new("bash")
        .args(["-n", script.to_str().expect("script path is UTF-8")])
        .current_dir(&root)
        .output()
        .expect("bash syntax check must launch");
    assert!(
        syntax.status.success(),
        "environment syntax check failed:\n{}",
        String::from_utf8_lossy(&syntax.stderr)
    );

    let source = fs::read_to_string(script).expect("environment source is readable");
    for required in [
        "mode = \"direct_tls\"",
        "tls_trust_root",
        "TicketDeskApplication",
        "TicketDeskSeeder",
        "TicketDeskAgent",
        "application deploy",
        "role bind",
        "riffdb.alpha-endurance-environment-start/v1",
        "riffdb.alpha-endurance-environment-stop/v1",
    ] {
        assert!(source.contains(required), "environment omits {required}");
    }
    assert!(
        !source.contains("<\"/dev/null\"") && !source.contains("</dev/null"),
        "server stdin must remain open for the complete environment lifetime"
    );
    assert!(
        !source.contains("--no-tls") && !source.contains("mode = \"loopback\""),
        "the evidentiary environment must not fall back to loopback cleartext"
    );
}

#[test]
fn endurance_lifecycle_evidence_uses_durable_observations() {
    let root = repository_root();
    let output = Command::new(root.join("scripts/endurance-lifecycle"))
        .arg("--self-test")
        .current_dir(&root)
        .env("RIFFDB_TMP_ROOT", task_temporary_root())
        .output()
        .expect("endurance lifecycle self-test must launch");
    assert!(
        output.status.success(),
        "lifecycle self-test failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let lifecycle = fs::read_to_string(root.join("scripts/endurance-lifecycle"))
        .expect("lifecycle source is readable");
    for required in [
        "RDBJEX03",
        "checkpoint_commit_sequence",
        "dispatch_deferred_max",
        "consumer_acknowledgements",
        "endurance lifecycle action is not implemented",
    ] {
        assert!(lifecycle.contains(required), "lifecycle omits {required}");
    }
    assert!(
        !lifecycle.contains("generation_after\": before + 1")
            && !lifecycle.contains("checkpoint_count\"] += 1"),
        "lifecycle evidence must not manufacture durable frontiers"
    );

    let sampler = fs::read_to_string(root.join("scripts/endurance-sample"))
        .expect("sampler source is readable");
    assert!(sampler.contains("events_emitted * 2 - consumer_acknowledgements"));
    for worker in [
        "examples/ticketdesk/src/bin/endurance.rs",
        "examples/ticketdesk/endurance/go/main.go",
        "examples/ticketdesk/web/src/endurance.ts",
        "examples/ticketdesk/endurance/python/main.py",
    ] {
        let source = fs::read_to_string(root.join(worker)).expect("worker source is readable");
        assert!(
            source.contains("consumer_acknowledgements"),
            "{worker} omits exact consumer acknowledgements"
        );
        assert!(
            source.contains("events_emitted"),
            "{worker} omits exact emitted-event accounting"
        );
    }
}
