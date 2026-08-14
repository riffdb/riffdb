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
    assert!(stdout.contains("reviewed_action_manifest_default: passed"));
    assert!(stdout.contains("action_manifest_drift: rejected"));
    assert!(stdout.contains("checked_action_manifest: passed"));
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
        source.contains("sleep infinity >\"$environment_root/server.input\" 2>/dev/null &"),
        "the stdin keeper must close the controller capture pipe"
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
        "backup.operation",
        "capability create administration sequence",
        "retention fencing has not advanced",
        "exact_lock_sha256",
        "certificate_sha256_after",
        "termination_signal\": \"SIGKILL",
        "unclean recovery regressed a durable frontier",
        "if after_frontier >= minimum_frontier",
        "required {minimum_frontier}, observed {after_frontier}",
        "payload.get(\"status\") not in (\"ready\", \"degraded\")",
        "authoritative_storage\", \"catalog\", \"commit_coordinator",
        "endurance lifecycle action is not implemented",
    ] {
        assert!(lifecycle.contains(required), "lifecycle omits {required}");
    }
    assert!(
        !lifecycle.contains("generation_after\": before + 1")
            && !lifecycle.contains("checkpoint_count\"] += 1"),
        "lifecycle evidence must not manufacture durable frontiers"
    );
    let inspector = fs::read_to_string(root.join("tests/endurance_inspect.rs"))
        .expect("stopped-database inspector source is readable");
    assert!(inspector.contains("read_validated_prefix_checkpoint_commit_sequence_fixture"));
    for forbidden in [
        "begin_structural_evidence",
        "scan_administration_audit",
        "validate_catalog_history",
    ] {
        assert!(
            !inspector.contains(forbidden),
            "checkpoint inspection must not perform O(history) work: {forbidden}"
        );
    }

    let sampler = fs::read_to_string(root.join("scripts/endurance-sample"))
        .expect("sampler source is readable");
    assert!(sampler.contains("events_emitted * 2 - consumer_acknowledgements"));
    assert!(sampler.contains("payload.get(\"status\") not in (\"ready\", \"degraded\")"));
    let controller = fs::read_to_string(root.join("scripts/alpha-endurance-controller"))
        .expect("controller source is readable");
    assert!(controller.contains("report_subprocess_failure(\"sampler_failed\", result)"));
    assert!(controller.contains("report_subprocess_failure(\"conformance_failed\", result)"));
    assert!(controller.contains("worker_exited_early[{language}]"));
    assert!(controller.contains("if worker_exited:\n                break"));
    assert!(controller.contains(
        "next_conformance = started + action_manifest[\"conformance\"][\"interval_seconds\"]"
    ));
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
        assert!(
            !source.contains("organization_id = id(10")
                && !source.contains("organizationID := id(10")
                && !source.contains("organizationId = id(10n")
                && !source.contains("organization_id = riff_id(10"),
            "{worker} collapses language workers into the same reaction partition"
        );
    }
    for (worker, acknowledgement) in [
        (
            "examples/ticketdesk/src/bin/endurance.rs",
            "ack_triage_ticket",
        ),
        ("examples/ticketdesk/endurance/go/main.go", "triage.Ack"),
        (
            "examples/ticketdesk/web/src/endurance.ts",
            "ackTriageTicket",
        ),
        (
            "examples/ticketdesk/endurance/python/main.py",
            "ack_triage_ticket",
        ),
    ] {
        let source = fs::read_to_string(root.join(worker)).expect("worker source is readable");
        assert!(
            source.contains(acknowledgement),
            "{worker} counts contextual work without a durable acknowledgement"
        );
    }
    for (worker, stable_identity) in [
        (
            "examples/ticketdesk/src/bin/endurance.rs",
            "event_id.commit_sequence, event_id.event_ordinal",
        ),
        (
            "examples/ticketdesk/endurance/go/main.go",
            "item.Delivery.EventID",
        ),
        (
            "examples/ticketdesk/web/src/endurance.ts",
            "item.delivery.eventId",
        ),
        (
            "examples/ticketdesk/endurance/python/main.py",
            "item.event_id",
        ),
    ] {
        let source = fs::read_to_string(root.join(worker)).expect("worker source is readable");
        assert!(
            source.contains(stable_identity),
            "{worker} does not derive contextual-reaction identity from the durable event"
        );
        assert!(
            !source.contains("reaction-{client_index}-{counter}")
                && !source.contains("reaction-%d-%d")
                && !source.contains("reaction-${index}-${counter}"),
            "{worker} reuses contextual-reaction identities after a worker restart"
        );
    }
    for (worker, retry, classifier, limit) in [
        (
            "examples/ticketdesk/src/bin/endurance.rs",
            "transient_retry",
            "transient_error",
            "MAX_TRANSIENT_RETRIES",
        ),
        (
            "examples/ticketdesk/endurance/go/main.go",
            "transientRetry",
            "transientError",
            "maxTransientRetries",
        ),
        (
            "examples/ticketdesk/web/src/endurance.ts",
            "transientRetry",
            "transientError",
            "MAX_TRANSIENT_RETRIES",
        ),
        (
            "examples/ticketdesk/endurance/python/main.py",
            "transient_retry",
            "transient_error",
            "MAX_TRANSIENT_RETRIES",
        ),
    ] {
        let source = fs::read_to_string(root.join(worker)).expect("worker source is readable");
        for required in [
            retry,
            classifier,
            limit,
            "declared_retries",
            "transport_attempts",
        ] {
            assert!(
                source.contains(required),
                "{worker} omits bounded visible retry evidence: {required}"
            );
        }
    }
    for (worker, bounded_retry_window) in [
        (
            "examples/ticketdesk/src/bin/endurance.rs",
            "MAX_TRANSIENT_RETRIES: u64 = 90",
        ),
        (
            "examples/ticketdesk/endurance/go/main.go",
            "maxTransientRetries uint64 = 90",
        ),
        (
            "examples/ticketdesk/web/src/endurance.ts",
            "MAX_TRANSIENT_RETRIES = 90",
        ),
        (
            "examples/ticketdesk/endurance/python/main.py",
            "MAX_TRANSIENT_RETRIES: Final = 90",
        ),
    ] {
        let source = fs::read_to_string(root.join(worker)).expect("worker source is readable");
        assert!(
            source.contains(bounded_retry_window),
            "{worker} cannot survive one supported offline maintenance window"
        );
    }
    for worker in [
        "examples/ticketdesk/endurance/go/main.go",
        "examples/ticketdesk/web/src/endurance.ts",
    ] {
        let source = fs::read_to_string(root.join(worker)).expect("worker source is readable");
        assert!(
            source.contains("RDB-DRIVER-0101"),
            "{worker} does not retry the stable driver transport interruption code"
        );
    }
}

#[test]
fn endurance_faults_are_real_orchestrator_only_recovery_cells() {
    let root = repository_root();
    let output = Command::new(root.join("scripts/endurance-fault"))
        .arg("--self-test")
        .current_dir(&root)
        .env("RIFFDB_TMP_ROOT", task_temporary_root())
        .output()
        .expect("endurance fault self-test must launch");
    assert!(
        output.status.success(),
        "fault self-test failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let fault =
        fs::read_to_string(root.join("scripts/endurance-fault")).expect("fault source is readable");
    for required in [
        "signal.SIGKILL",
        "signal.SIGSTOP",
        "pending_signal\": \"SIGTERM",
        "generation_at_crash",
        "time.sleep(61)",
        "command_replayed",
        "acknowledged_after_redelivery",
        "lifecycle.lock",
        "riffdb.alpha-endurance-action-result/v1",
    ] {
        assert!(fault.contains(required), "fault controls omit {required}");
    }
    assert!(
        !fault.contains("frontier_after\": before + 1")
            && !fault.contains("generation_at_crash\": generation_before + 1"),
        "fault evidence must not manufacture durable frontiers"
    );

    let client = fs::read_to_string(root.join("scripts/endurance-fault-client"))
        .expect("fault client source is readable");
    for required in [
        "AsyncTicketDeskReactiveClient",
        "AsyncTicketDeskClient",
        "seed-event",
        "create_organization",
        "create_user",
        "create_project",
        "create_ticket",
        "next_triage_ticket",
        "react_comment",
        "ack_triage_ticket",
        "await asyncio.Event().wait()",
    ] {
        assert!(client.contains(required), "fault client omits {required}");
    }
    for required in [
        "endurance-consumer-fault-organization-{ordinal}",
        "endurance-consumer-fault-ticket-{ordinal}",
        "mode=\"seed-event\"",
        "CONSUMER_SEED_SCHEMA",
    ] {
        assert!(fault.contains(required), "fault controls omit {required}");
    }
    assert!(
        !fault.contains("0000000a-0000-0000-0000-000000000001"),
        "consumer fault must not depend on ambient worker seed identities"
    );
}

#[test]
fn endurance_conformance_reconciles_public_state_and_policy() {
    let root = repository_root();
    let source = fs::read_to_string(root.join("scripts/endurance-conformance"))
        .expect("conformance source is readable");
    for required in [
        "riffdb.alpha-endurance-conformance-result/v1",
        "riffdb.alpha-endurance-conformance-evidence/v1",
        "TicketPageFound",
        "ApplicationErrorCode.AUTHORIZATION_DENIED",
        "adapter_conformance",
        "row_policy_conformance",
        "attempts != logical + retries",
        "application_lock_sha256",
    ] {
        assert!(source.contains(required), "conformance omits {required}");
    }
    assert!(
        !source.contains("GetEntity") && !source.contains("ScanIndex"),
        "conformance must remain on generated application operations"
    );
}
