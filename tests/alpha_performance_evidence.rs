#![forbid(unsafe_code)]
//! Process-boundary coverage for retained alpha performance evidence.

use std::fs;
use std::io::Write;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::{Value, json};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("testkit must remain under crates/")
        .to_path_buf()
}

fn canonical_host_sha256(value: &Value) -> String {
    let mut child = Command::new("python3")
        .args([
            "-c",
            "import hashlib,json,sys; value=json.load(sys.stdin); print(hashlib.sha256((json.dumps(value,sort_keys=True)+'\\n').encode()).hexdigest())",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("canonical host digest fixture helper");
    child
        .stdin
        .take()
        .expect("sha256sum stdin")
        .write_all(
            serde_json::to_string(value)
                .expect("host observation fixture serialization")
                .as_bytes(),
        )
        .expect("hash fixture JSON");
    let output = child.wait_with_output().expect("host digest completion");
    assert!(output.status.success(), "host digest fixture helper failed");
    String::from_utf8(output.stdout)
        .expect("host digest UTF-8")
        .split_whitespace()
        .next()
        .expect("host digest")
        .to_owned()
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

fn write_executable(path: &Path, source: &str) {
    fs::write(path, source).expect("fixture executable must be written");
    let mut permissions = fs::metadata(path)
        .expect("fixture executable metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("fixture executable must be executable");
}

fn run_git(root: &Path, arguments: &[&str]) {
    let output = Command::new("git")
        .args(arguments)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "core.hooksPath")
        .env("GIT_CONFIG_VALUE_0", root.join("disabled-hooks"))
        .current_dir(root)
        .output()
        .expect("git fixture command must launch");
    assert!(
        output.status.success(),
        "git fixture command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

struct RemoveDirOnDrop(PathBuf);

impl Drop for RemoveDirOnDrop {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

const HOST_INVALID_RECEIPT: &str = concat!(
    r#"{"schema":"riffdb.app-baseline-non-evidentiary/v1","status":"non_evidentiary","reason":{"code":"host_interference","retry":"rerun_on_idle_host"},"requested":{"mode":"full","profile":"parity","duration_secs":90,"reps":5,"postgres_comparator":"safe-app","concurrency_sweep":false},"partial_phase_evidence":null,"host_validity":{"preflight":{"active_processes":[{"pid":7,"comm":"fixture-load","cpu_percent_of_one_logical_cpu":6.0,"read_bytes_per_sec":0,"write_bytes_per_sec":0,"rss_bytes":4096}],"baseline_report_sha256":null,"boot_identity_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","cpu_accounting":{"steal_ticks":0,"total_ticks":1000},"hardware_identity":{"cpu_model":"AMD Ryzen 9 7950X 16-Core Processor","operating_system":{"id":"fixture","version_id":"1"},"system_product":"fixture","system_vendor":"fixture"},"interfering_processes":[{"pid":7,"comm":"fixture-load","cpu_percent_of_one_logical_cpu":6.0,"read_bytes_per_sec":0,"write_bytes_per_sec":0,"rss_bytes":4096}],"inventory_bounds":{"maximum_reported_processes":32,"process_arguments_included":false},"load_average_after":[0.0,0.0,0.0],"load_average_before":[0.0,0.0,0.0],"logical_cpus":32,"memory_available_bytes":1073741824,"reason":"host_interference","sample_interval_ms":3000,"schema":"riffdb.app-baseline-host-validity/v2","thresholds":{"cpu_percent_of_one_logical_cpu":5.0,"io_bytes_per_sec":8388608,"maximum_whole_cell_steal_percent":1.0},"valid":false,"whole_cell":null},"postflight":null},"evidence_eligibility":{"eligible":false,"host_idle":false,"reason_codes":["host_interference"]}}"#,
    "\n"
);

const HOST_INVALID_PROFILE_DRIFT: &str = concat!(
    r#"{"schema":"riffdb.app-baseline-non-evidentiary/v1","status":"non_evidentiary","reason":{"code":"host_interference","retry":"rerun_on_idle_host"},"requested":{"mode":"full","profile":"parity","duration_secs":90,"reps":5,"postgres_comparator":"safe-app","concurrency_sweep":false},"partial_phase_evidence":null,"host_validity":{"preflight":{"active_processes":[{"pid":7,"comm":"fixture-load","cpu_percent_of_one_logical_cpu":6.0,"read_bytes_per_sec":0,"write_bytes_per_sec":0,"rss_bytes":4096}],"baseline_report_sha256":null,"boot_identity_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","cpu_accounting":{"steal_ticks":0,"total_ticks":1000},"hardware_identity":{"cpu_model":"AMD EPYC 7B12","operating_system":{"id":"fixture","version_id":"1"},"system_product":"fixture","system_vendor":"fixture"},"interfering_processes":[{"pid":7,"comm":"fixture-load","cpu_percent_of_one_logical_cpu":6.0,"read_bytes_per_sec":0,"write_bytes_per_sec":0,"rss_bytes":4096}],"inventory_bounds":{"maximum_reported_processes":32,"process_arguments_included":false},"load_average_after":[0.0,0.0,0.0],"load_average_before":[0.0,0.0,0.0],"logical_cpus":32,"memory_available_bytes":1073741824,"reason":"host_interference","sample_interval_ms":3000,"schema":"riffdb.app-baseline-host-validity/v2","thresholds":{"cpu_percent_of_one_logical_cpu":5.0,"io_bytes_per_sec":8388608,"maximum_whole_cell_steal_percent":1.0},"valid":false,"whole_cell":null},"postflight":null},"evidence_eligibility":{"eligible":false,"host_idle":false,"reason_codes":["host_interference"]}}"#,
    "\n"
);

const POSTFLIGHT_HOST_INVALID_RECEIPT: &str = concat!(
    r#"{"schema":"riffdb.app-baseline/v1","report_id":"ticketdesk-app-baseline","domain":"TicketDesk","tables":["organization","app_user","project","project_member","ticket","comment","label","ticket_label"],"scale":{},"configuration":{"warmup_iterations":20,"measured_samples":1000},"board_page_query":{},"backends":[{"backend_id":"postgres_safe_app"},{"backend_id":"riffdb_public_grpc"}],"comparisons":{"available":true,"scenarios":[{"scenario":"point_get_ticket","last_row_count_equal":true}]},"reps":5,"backend_execution_order":["postgres_safe_app","riffdb_public_grpc","riffdb_public_grpc","postgres_safe_app","postgres_safe_app","riffdb_public_grpc","riffdb_public_grpc","postgres_safe_app","postgres_safe_app","riffdb_public_grpc"],"scale_shape":{},"environment":{},"device_baseline":{},"riffdb_database_root":"x","riffdb_storage_medium":{},"postgres_data_host_path":"x","postgres_storage_medium":{},"postgres_durability":{},"same_device":true,"limitations":[],"riffdb_process_evidence":[{},{},{},{},{}],"unary_qualification":{"schema":"riffdb.app-baseline-unary-qualification/v1","class":"ordinary_named_read","scenario":"point_get_ticket","daemon_restart_after_common_setup":true,"same_scenario_warmups_after_restart":20,"measured_operations_per_generation":1000,"process_generations":5},"host_validity":{"preflight":{"active_processes":[],"baseline_report_sha256":null,"boot_identity_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","cpu_accounting":{"steal_ticks":0,"total_ticks":1000},"hardware_identity":{"cpu_model":"AMD Ryzen 9 7950X 16-Core Processor","operating_system":{"id":"fixture","version_id":"1"},"system_product":"fixture","system_vendor":"fixture"},"interfering_processes":[],"inventory_bounds":{"maximum_reported_processes":32,"process_arguments_included":false},"load_average_after":[0.0,0.0,0.0],"load_average_before":[0.0,0.0,0.0],"logical_cpus":32,"memory_available_bytes":1073741824,"reason":null,"sample_interval_ms":3000,"schema":"riffdb.app-baseline-host-validity/v2","thresholds":{"cpu_percent_of_one_logical_cpu":5.0,"io_bytes_per_sec":8388608,"maximum_whole_cell_steal_percent":1.0},"valid":true,"whole_cell":null},"postflight":{"active_processes":[],"baseline_report_sha256":"85905cf8a0ab4d7e2971c81427970b4e7f32eb0d263b03fb4cb816ca336044ff","boot_identity_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","cpu_accounting":{"steal_ticks":20,"total_ticks":2000},"hardware_identity":{"cpu_model":"AMD Ryzen 9 7950X 16-Core Processor","operating_system":{"id":"fixture","version_id":"1"},"system_product":"fixture","system_vendor":"fixture"},"interfering_processes":[],"inventory_bounds":{"maximum_reported_processes":32,"process_arguments_included":false},"load_average_after":[0.0,0.0,0.0],"load_average_before":[0.0,0.0,0.0],"logical_cpus":32,"memory_available_bytes":1073741824,"reason":"host_interference","sample_interval_ms":3000,"schema":"riffdb.app-baseline-host-validity/v2","thresholds":{"cpu_percent_of_one_logical_cpu":5.0,"io_bytes_per_sec":8388608,"maximum_whole_cell_steal_percent":1.0},"valid":false,"whole_cell":{"elapsed_steal_ticks":20,"elapsed_total_ticks":1000,"maximum_steal_percent":1.0,"reason":"cpu_steal_exceeded","steal_percent":2.0,"valid":false}}},"evidence_eligibility":{"host_idle":false,"reason_codes":["host_interference"],"postgres_central_three_spread_disclosed":[],"stable":false,"correctness_clean":true,"same_device_comparable":true,"comparison_complete":true,"missing_required_fields":["stability"],"non_evidentiary_window":false,"eligible":false}}"#,
    "\n"
);

fn host_receipt_with_scan_bound(source: &str) -> String {
    let mut receipt: Value = serde_json::from_str(source).expect("host receipt fixture JSON");
    for boundary in ["preflight", "postflight"] {
        if receipt["host_validity"][boundary].is_object() {
            receipt["host_validity"][boundary]["inventory_bounds"]["maximum_scanned_processes"] =
                json!(4096);
            receipt["host_validity"][boundary]["process_interval"] = json!({
                "stable_identity_required": true,
                "started_processes": 0,
                "exited_processes": 0,
                "pid_reuses": 0,
                "kernel_thread_events_excluded": 0
            });
        }
    }
    format!(
        "{}\n",
        serde_json::to_string(&receipt).expect("host receipt fixture serialization")
    )
}

fn host_invalid_receipt() -> String {
    let mut receipt: Value =
        serde_json::from_str(&host_receipt_with_scan_bound(HOST_INVALID_RECEIPT))
            .expect("host-invalid fixture JSON");
    receipt["requested"]["scenario"] = json!("point_get_ticket");
    format!(
        "{}\n",
        serde_json::to_string(&receipt).expect("host receipt")
    )
}

fn host_invalid_profile_drift() -> String {
    let mut receipt: Value =
        serde_json::from_str(&host_receipt_with_scan_bound(HOST_INVALID_PROFILE_DRIFT))
            .expect("host-invalid fixture JSON");
    receipt["requested"]["scenario"] = json!("point_get_ticket");
    format!(
        "{}\n",
        serde_json::to_string(&receipt).expect("host receipt")
    )
}

fn host_invalid_receipt_with_type_drift(pointer: &str, replacement: Value) -> String {
    let mut receipt: Value =
        serde_json::from_str(&host_invalid_receipt()).expect("host-invalid fixture JSON");
    *receipt
        .pointer_mut(pointer)
        .expect("type-drift fixture pointer must exist") = replacement;
    format!(
        "{}\n",
        serde_json::to_string(&receipt).expect("type-drift fixture serialization")
    )
}

fn minimal_preflight_host_invalid_receipt() -> String {
    let mut receipt: Value =
        serde_json::from_str(&host_invalid_receipt()).expect("host-invalid fixture JSON");
    receipt["requested"]["postgres_comparator"] = json!("minimal");
    receipt["requested"]
        .as_object_mut()
        .expect("requested object")
        .remove("scenario");
    format!(
        "{}\n",
        serde_json::to_string(&receipt).expect("minimal preflight fixture serialization")
    )
}

fn minimal_postflight_host_invalid_receipt() -> String {
    // The retained 2026-08-30 column is the historical, non-counterbalanced
    // minimal topology; the live column under wp-674/n1 is the banked one.
    production_shaped_postflight_host_invalid_receipt(
        "release/evidence/wp-674/superseded/n1-9bf995d9/minimal.json",
    )
}

fn production_shaped_postflight_host_invalid_receipt(relative: &str) -> String {
    let source = fs::read_to_string(repository_root().join(relative))
        .expect("retained production-shaped WP-674 report");
    let mut receipt: Value = serde_json::from_str(&source).expect("retained report JSON");
    receipt
        .as_object_mut()
        .expect("retained report object")
        .remove("qualification_candidate");
    let before_total = receipt["host_validity"]["preflight"]["cpu_accounting"]["total_ticks"]
        .as_u64()
        .expect("preflight total ticks");
    let before_steal = receipt["host_validity"]["preflight"]["cpu_accounting"]["steal_ticks"]
        .as_u64()
        .expect("preflight steal ticks");
    let after_total = receipt["host_validity"]["postflight"]["cpu_accounting"]["total_ticks"]
        .as_u64()
        .expect("postflight total ticks");
    let elapsed_total = after_total - before_total;
    let elapsed_steal = elapsed_total / 50;
    assert!(
        elapsed_steal > 0,
        "production fixture must have elapsed ticks"
    );
    receipt["host_validity"]["postflight"]["cpu_accounting"]["steal_ticks"] =
        json!(before_steal + elapsed_steal);
    receipt["host_validity"]["postflight"]["valid"] = json!(false);
    receipt["host_validity"]["postflight"]["reason"] = json!("host_interference");
    receipt["host_validity"]["postflight"]["whole_cell"] = json!({
        "valid": false,
        "reason": "cpu_steal_exceeded",
        "elapsed_total_ticks": elapsed_total,
        "elapsed_steal_ticks": elapsed_steal,
        "steal_percent": ((elapsed_steal as f64 / elapsed_total as f64) * 100.0 * 1_000_000.0).round() / 1_000_000.0,
        "maximum_steal_percent": 1.0
    });
    receipt["evidence_eligibility"]["host_idle"] = json!(false);
    receipt["evidence_eligibility"]["eligible"] = json!(false);
    receipt["evidence_eligibility"]["reason_codes"] = json!(["host_interference"]);
    for boundary in ["preflight", "postflight"] {
        receipt["host_validity"][boundary]["inventory_bounds"]["maximum_scanned_processes"] =
            json!(4096);
        receipt["host_validity"][boundary]["process_interval"] = json!({
            "stable_identity_required": true,
            "started_processes": 0,
            "exited_processes": 0,
            "pid_reuses": 0,
            "kernel_thread_events_excluded": 0
        });
    }
    receipt["host_validity"]["postflight"]["baseline_report_sha256"] = json!(
        canonical_host_sha256(&receipt["host_validity"]["preflight"])
    );
    format!(
        "{}\n",
        serde_json::to_string(&receipt).expect("production fixture serialization")
    )
}

fn production_shaped_postflight_with_drift(pointer: &str, replacement: Value) -> String {
    let source = production_shaped_postflight_host_invalid_receipt(
        "release/evidence/wp-674/n1/safe-app-point_get_ticket.json",
    );
    let mut receipt: Value = serde_json::from_str(&source).expect("production fixture JSON");
    *receipt
        .pointer_mut(pointer)
        .expect("production-drift fixture pointer must exist") = replacement;
    format!(
        "{}\n",
        serde_json::to_string(&receipt).expect("production-drift fixture serialization")
    )
}

fn exclusive_fixture_parent() -> PathBuf {
    for _ in 0..16 {
        let nonce = fs::read_to_string("/proc/sys/kernel/random/uuid")
            .expect("kernel-generated fixture nonce");
        let nonce = nonce.trim();
        assert!(
            !nonce.is_empty()
                && nonce.len() <= 64
                && nonce
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() || byte == b'-'),
            "kernel-generated fixture nonce must be path-safe"
        );
        let parent = std::env::temp_dir().join(format!("riffdb-wp674-retry-{nonce}"));
        match fs::create_dir(&parent) {
            Ok(()) => return parent,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("exclusive fixture directory failed: {error}"),
        }
    }
    panic!("could not create an exclusive WP-674 fixture directory")
}

struct ProfileRunnerFixture {
    parent: PathBuf,
    root: PathBuf,
    fake_bin: PathBuf,
    counter: PathBuf,
    runner_bin: PathBuf,
    riffdbd_bin: PathBuf,
    _cleanup: RemoveDirOnDrop,
}

impl ProfileRunnerFixture {
    fn new() -> Self {
        let parent = exclusive_fixture_parent();
        let root = parent.join("repository");
        let fake_bin = parent.join("bin");
        fs::create_dir_all(root.join("benchmarks")).expect("benchmark fixture directory");
        fs::create_dir_all(root.join("examples/app-baseline"))
            .expect("app-baseline fixture directory");
        fs::create_dir_all(root.join("scripts")).expect("script fixture directory");
        fs::create_dir_all(root.join("disabled-hooks")).expect("isolated empty Git hooks");
        fs::create_dir_all(&fake_bin).expect("fake executable directory");

        fs::copy(
            repository_root().join("benchmarks/run-wp674-unary-profile"),
            root.join("benchmarks/run-wp674-unary-profile"),
        )
        .expect("profile runner fixture");
        fs::copy(
            repository_root().join("scripts/check-wp674-unary-baseline"),
            root.join("scripts/check-wp674-unary-baseline"),
        )
        .expect("WP-674 checker fixture");
        fs::copy(
            repository_root().join("scripts/wp674_host_policy.py"),
            root.join("scripts/wp674_host_policy.py"),
        )
        .expect("WP-674 host policy fixture");
        fs::copy(
            repository_root().join("scripts/bounded_evidence_io.py"),
            root.join("scripts/bounded_evidence_io.py"),
        )
        .expect("bounded evidence I/O fixture");
        fs::write(root.join("Cargo.lock"), "# retry fixture\n").expect("lock fixture");
        fs::write(
            root.join("examples/app-baseline/Cargo.toml"),
            "[workspace]\n",
        )
        .expect("manifest fixture");
        write_executable(&fake_bin.join("cargo"), "#!/bin/sh\nexit 0\n");
        let runner_bin = parent.join("riffdb-app-baseline");
        let riffdbd_bin = parent.join("riffdbd");
        write_executable(&runner_bin, "#!/bin/sh\nexit 0\n");
        write_executable(&riffdbd_bin, "#!/bin/sh\nexit 0\n");

        let counter = parent.join("attempt-count");
        write_executable(
            &root.join("benchmarks/run-app-baseline"),
            r#"#!/bin/sh
set -eu
output=
while [ "$#" -gt 0 ]; do
    if [ "$1" = "--output" ]; then
        output="$2"
        shift 2
    else
        shift
    fi
done
count=0
if [ -f "$WP674_RETRY_COUNT" ]; then
    count=$(cat "$WP674_RETRY_COUNT")
fi
count=$((count + 1))
printf '%s\n' "$count" >"$WP674_RETRY_COUNT"
mkdir -p "$(dirname "$output")"
if [ "${WP674_OVERSIZED_REPORT:-0}" = 1 ]; then
    dd if=/dev/zero of="$output" bs=1048576 count=33 2>/dev/null
    exit 0
fi
if [ "$count" -eq 1 ]; then
    if [ "${WP674_PROFILE_DRIFT:-0}" = 1 ]; then
        printf '%s' "$WP674_HOST_INVALID_PROFILE_DRIFT" >"$output"
    elif [ "${WP674_POSTFLIGHT_HOST_INVALID:-0}" = 1 ]; then
        printf '%s' "$WP674_POSTFLIGHT_HOST_INVALID_RECEIPT" >"$output"
    elif [ "${WP674_TYPE_DRIFT:-0}" = 1 ]; then
        printf '%s' "$WP674_TYPE_DRIFT_RECEIPT" >"$output"
    else
        printf '%s' "$WP674_HOST_INVALID_RECEIPT" >"$output"
    fi
    if [ "${WP674_BLOCK_ATTEMPT_TWO:-0}" = 1 ]; then
        : >"$(dirname "$(dirname "$output")")/host-attempt-2"
    fi
    if [ "${WP674_MUTATE_RUNNER_IDENTITY:-0}" = 1 ]; then
        printf '# drift\n' >>"$RIFFDB_APP_BASELINE_RUNNER_BIN"
    fi
elif [ "${WP674_ALWAYS_HOST_INVALID:-0}" = 1 ]; then
    printf '%s' "$WP674_HOST_INVALID_RECEIPT" >"$output"
else
    printf '%s\n' '{}' >"$output"
fi
exit 1
"#,
        );

        run_git(&root, &["init", "-q"]);
        run_git(&root, &["add", "."]);
        run_git(
            &root,
            &[
                "-c",
                "user.email=test@riffdb.invalid",
                "-c",
                "user.name=RiffDB Test",
                "commit",
                "-qm",
                "fixture",
            ],
        );

        Self {
            parent: parent.clone(),
            root,
            fake_bin,
            counter,
            runner_bin,
            riffdbd_bin,
            _cleanup: RemoveDirOnDrop(parent),
        }
    }

    fn command_for_profile(&self, output_root: &Path, profile: &str) -> Command {
        let path = format!(
            "{}:{}",
            self.fake_bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut command = Command::new(self.root.join("benchmarks/run-wp674-unary-profile"));
        command
            .args([
                "--profile",
                profile,
                "--output-dir",
                output_root.to_str().expect("UTF-8 evidence path"),
            ])
            .env("PATH", path)
            .env("WP674_RETRY_COUNT", &self.counter)
            .env("RIFFDB_APP_BASELINE_RUNNER_BIN", &self.runner_bin)
            .env("RIFFDB_APP_BASELINE_RIFFDBD_BIN", &self.riffdbd_bin)
            .env("WP674_HOST_INVALID_RECEIPT", host_invalid_receipt())
            .env(
                "WP674_POSTFLIGHT_HOST_INVALID_RECEIPT",
                production_shaped_postflight_host_invalid_receipt(
                    "release/evidence/wp-674/n1/safe-app-point_get_ticket.json",
                ),
            )
            .env(
                "WP674_HOST_INVALID_PROFILE_DRIFT",
                host_invalid_profile_drift(),
            )
            .current_dir(&self.root);
        command
    }

    fn command(&self, output_root: &Path) -> Command {
        self.command_for_profile(output_root, "workstation")
    }

    fn run(&self, output_root: &Path) -> std::process::Output {
        self.command(output_root)
            .output()
            .expect("profile runner fixture must launch")
    }

    fn validate_host_invalid(
        &self,
        receipt: &Path,
        profile: &str,
        comparator: &str,
        scenario: Option<&str>,
    ) -> std::process::Output {
        let mut command = Command::new(self.root.join("scripts/check-wp674-unary-baseline"));
        command
            .args([
                "--validate-host-invalid-receipt",
                receipt.to_str().expect("UTF-8 receipt path"),
                "--profile",
                profile,
                "--comparator",
                comparator,
            ])
            .current_dir(&self.root);
        if let Some(scenario) = scenario {
            command.args(["--scenario", scenario]);
        }
        command.output().expect("WP-674 checker must launch")
    }
}

// req: PERF-018
#[test]
fn wp674_profile_runner_retains_host_invalid_attempt_before_one_replacement() {
    let fixture = ProfileRunnerFixture::new();
    let output_root = fixture.parent.join("evidence");
    let output = fixture.run(&output_root);

    assert!(
        !output.status.success(),
        "second non-host failure must stop the run"
    );
    assert_eq!(
        fs::read_to_string(&fixture.counter).expect("attempt counter"),
        "2\n",
        "one host-invalid attempt must advance to exactly one replacement"
    );
    assert_eq!(
        fs::read_to_string(output_root.join("attempts/status.tsv"))
            .expect("retained attempt status"),
        "1\thost_invalid\n"
    );
    assert_eq!(
        fs::read_to_string(
            output_root.join("attempts/host-attempt-1/safe-app-point_get_ticket.json")
        )
        .expect("first raw host-invalid receipt"),
        host_invalid_receipt()
    );
}

// req: PERF-018
#[test]
fn wp674_profile_runner_refuses_ambient_external_postgres() {
    let fixture = ProfileRunnerFixture::new();
    let output_root = fixture.parent.join("external-postgres-evidence");
    let output = fixture
        .command(&output_root)
        .env(
            "RIFFDB_APP_BASELINE_POSTGRES_URL",
            "postgres://external.invalid/ticketdesk",
        )
        .output()
        .expect("profile runner fixture must launch");

    assert!(
        !output.status.success(),
        "external PostgreSQL must be refused"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("ambient PostgreSQL URL is forbidden"),
        "refusal must identify the external-PostgreSQL attribution hazard"
    );
    assert!(
        !fixture.counter.exists(),
        "no measurement may start with an external PostgreSQL URL"
    );
    assert!(
        !output_root.exists(),
        "refusal must precede evidence custody"
    )
}

// req: PERF-018
#[test]
fn wp674_profile_runner_replaces_exact_postflight_host_invalid_attempt() {
    let fixture = ProfileRunnerFixture::new();
    let output_root = fixture.parent.join("postflight-host-invalid-evidence");
    let output = fixture
        .command_for_profile(&output_root, "n1")
        .env("WP674_POSTFLIGHT_HOST_INVALID", "1")
        .output()
        .expect("postflight-host-invalid runner fixture must launch");

    assert!(
        !output.status.success(),
        "second non-host failure must stop"
    );
    assert_eq!(
        fs::read_to_string(&fixture.counter).expect("attempt counter"),
        "2\n",
        "an independently invalid exact V2 postflight must receive one replacement"
    );
    assert_eq!(
        fs::read_to_string(output_root.join("attempts/status.tsv")).expect("attempt status"),
        "1\thost_invalid\n"
    );
    assert_eq!(
        fs::read_to_string(
            output_root.join("attempts/host-attempt-1/safe-app-point_get_ticket.json")
        )
        .expect("retained postflight-host-invalid report"),
        production_shaped_postflight_host_invalid_receipt(
            "release/evidence/wp-674/n1/safe-app-point_get_ticket.json"
        )
    );
}

// req: PERF-018
#[test]
fn wp674_host_invalid_validator_covers_exact_minimal_preflight_and_scenario_binding() {
    let fixture = ProfileRunnerFixture::new();
    let minimal_preflight = fixture.parent.join("minimal-preflight.json");
    fs::write(&minimal_preflight, minimal_preflight_host_invalid_receipt())
        .expect("minimal preflight receipt fixture");
    let safe_postflight = fixture.parent.join("safe-postflight.json");
    fs::write(
        &safe_postflight,
        production_shaped_postflight_host_invalid_receipt(
            "release/evidence/wp-674/n1/safe-app-point_get_ticket.json",
        ),
    )
    .expect("safe-app postflight receipt fixture");

    let output = fixture.validate_host_invalid(&minimal_preflight, "workstation", "minimal", None);
    assert!(
        output.status.success(),
        "minimal host-invalid preflight was refused:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !fixture
            .validate_host_invalid(
                &minimal_preflight,
                "workstation",
                "safe-app",
                Some("point_get_ticket"),
            )
            .status
            .success(),
        "minimal preflight must not classify as a safe-app failure"
    );
    assert!(
        !fixture
            .validate_host_invalid(&safe_postflight, "n1", "safe-app", Some("point_get_user"),)
            .status
            .success(),
        "a postflight report from another scenario must not consume the replacement"
    );
}

// req: PERF-018
#[test]
fn wp674_host_invalid_validator_accepts_production_shaped_postflight_report() {
    let fixture = ProfileRunnerFixture::new();
    let cases = [(
        "release/evidence/wp-674/n1/safe-app-point_get_ticket.json",
        "safe-app",
        Some("point_get_ticket"),
    )];
    for (index, (source, comparator, scenario)) in cases.into_iter().enumerate() {
        let receipt = fixture
            .parent
            .join(format!("production-shaped-postflight-{index}.json"));
        fs::write(
            &receipt,
            production_shaped_postflight_host_invalid_receipt(source),
        )
        .expect("production-shaped postflight fixture");

        let output = fixture.validate_host_invalid(&receipt, "n1", comparator, scenario);
        assert!(
            output.status.success(),
            "production-shaped {comparator} invalid postflight was refused:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let historical_minimal = fixture.parent.join("historical-minimal-postflight.json");
    fs::write(
        &historical_minimal,
        minimal_postflight_host_invalid_receipt(),
    )
    .expect("historical minimal postflight fixture");
    assert!(
        !fixture
            .validate_host_invalid(&historical_minimal, "n1", "minimal", None)
            .status
            .success(),
        "the historical non-counterbalanced minimal topology must not be reclassified"
    );
}

// req: PERF-018
#[test]
fn wp674_host_invalid_validator_refuses_non_host_method_and_topology_drift() {
    let incomplete_fixture = ProfileRunnerFixture::new();
    let incomplete = incomplete_fixture.parent.join("incomplete-postflight.json");
    fs::write(&incomplete, POSTFLIGHT_HOST_INVALID_RECEIPT).expect("incomplete postflight fixture");
    assert!(
        !incomplete_fixture
            .validate_host_invalid(
                &incomplete,
                "workstation",
                "safe-app",
                Some("point_get_ticket"),
            )
            .status
            .success(),
        "a skeletal host-invalid report must not consume the replacement"
    );
    let cases = [
        ("/scale/profile", json!("smoke")),
        ("/reps", json!(5.0)),
        ("/configuration/warmup_iterations", json!(20.0)),
        ("/configuration/distribution_method", json!("average")),
        ("/backends/0/scenarios/0/timing/sample_count", json!(1000.0)),
        ("/postgres_durability/server_version_num", json!("170006")),
        (
            "/postgres_durability/wal_sync_method",
            json!("open_datasync"),
        ),
        ("/backends/0/scenarios/0/last_row_count", json!(2)),
        ("/evidence_eligibility/correctness_clean", json!(false)),
        (
            "/riffdb_process_evidence/0/post_shutdown_structural_reopen",
            json!(false),
        ),
        (
            "/riffdb_process_evidence/0/read_pipeline_stages/0/count",
            json!(0),
        ),
        (
            "/postgres_storage_medium/device_model",
            json!("other-device"),
        ),
        (
            "/evidence_eligibility/missing_required_fields",
            json!(["environment"]),
        ),
        ("/backends/0/scenarios/0/scenario", json!("point_get_user")),
    ];
    for (index, (pointer, replacement)) in cases.into_iter().enumerate() {
        let fixture = ProfileRunnerFixture::new();
        let receipt = fixture
            .parent
            .join(format!("production-drift-{index}.json"));
        fs::write(
            &receipt,
            production_shaped_postflight_with_drift(pointer, replacement),
        )
        .expect("production-drift postflight fixture");

        let output =
            fixture.validate_host_invalid(&receipt, "n1", "safe-app", Some("point_get_ticket"));
        assert!(
            !output.status.success(),
            "{pointer} non-host drift must not consume the replacement"
        );
    }
}

// req: PERF-018
#[test]
fn wp674_profile_runner_refuses_json_type_drift_without_replacement() {
    let cases = [
        ("/requested/scenario", json!("point_get_user")),
        ("/requested/concurrency_sweep", json!(0)),
        ("/evidence_eligibility/eligible", json!(0)),
        ("/requested/reps", json!(5.0)),
        ("/host_validity/preflight/logical_cpus", json!(32.0)),
        (
            "/host_validity/preflight/thresholds/maximum_whole_cell_steal_percent",
            json!(true),
        ),
        (
            "/host_validity/preflight/inventory_bounds/process_arguments_included",
            json!(0),
        ),
    ];

    for (index, (pointer, replacement)) in cases.into_iter().enumerate() {
        let fixture = ProfileRunnerFixture::new();
        let output_root = fixture.parent.join(format!("type-drift-{index}"));
        let receipt = host_invalid_receipt_with_type_drift(pointer, replacement);
        let output = fixture
            .command(&output_root)
            .env("WP674_TYPE_DRIFT", "1")
            .env("WP674_TYPE_DRIFT_RECEIPT", &receipt)
            .output()
            .expect("type-drift runner fixture must launch");

        assert!(
            !output.status.success(),
            "{pointer} type drift must fail closed"
        );
        assert_eq!(
            fs::read_to_string(&fixture.counter).expect("attempt counter"),
            "1\n",
            "{pointer} type drift must not consume a replacement"
        );
        assert_eq!(
            fs::read_to_string(output_root.join("attempts/status.tsv"))
                .expect("attempt status ledger"),
            "",
            "{pointer} type drift must not be classified as host-invalid"
        );
    }
}

// req: PERF-018
#[test]
fn wp674_profile_runner_refuses_cross_revision_interrupted_receipt() {
    let fixture = ProfileRunnerFixture::new();
    let resume_root = fixture.parent.join("resume-evidence");
    let resume_attempt = resume_root.join("attempts/host-attempt-1");
    fs::create_dir_all(&resume_attempt).expect("resume attempt fixture directory");
    fs::write(
        resume_attempt.join("safe-app-point_get_ticket.json"),
        host_invalid_receipt(),
    )
    .expect("retained interrupted receipt fixture");
    fs::write(resume_root.join("attempts/status.tsv"), "").expect("empty interrupted status");

    let resumed = fixture.run(&resume_root);
    assert!(
        !resumed.status.success(),
        "an interrupted receipt without exact artifact identity must be refused"
    );
    assert_eq!(
        fs::read_to_string(
            resume_root.join("attempts/host-attempt-1/safe-app-point_get_ticket.json")
        )
        .expect("preserved interrupted receipt"),
        host_invalid_receipt()
    );
    assert_eq!(
        fs::read_to_string(resume_root.join("attempts/status.tsv"))
            .expect("preserved empty interrupted status"),
        ""
    );
    assert!(
        !resume_root
            .join("attempts/host-attempt-2/safe-app-point_get_ticket.json")
            .is_file(),
        "refusal must not begin a replacement against another revision"
    );
    assert!(
        !fixture.counter.exists(),
        "refusal must not invoke a backend"
    );
}

// req: PERF-018
#[test]
fn wp674_profile_runner_refuses_linked_attempt_custody_without_writing() {
    let fixture = ProfileRunnerFixture::new();
    let output_root = fixture.parent.join("linked-evidence");
    let linked_custody = fixture.parent.join("linked-custody");
    fs::create_dir(&output_root).expect("linked output fixture directory");
    fs::create_dir(&linked_custody).expect("linked custody fixture directory");
    symlink(&linked_custody, output_root.join("attempts")).expect("attempt custody symlink");

    let output = fixture.run(&output_root);
    assert!(
        !output.status.success(),
        "linked attempt custody must fail closed"
    );
    assert!(
        !fixture.counter.exists(),
        "refusal must not invoke a backend"
    );
    assert_eq!(
        fs::read_dir(&linked_custody)
            .expect("linked custody inventory")
            .count(),
        0,
        "the runner must not write through linked custody"
    );
}

// req: PERF-018
#[test]
fn wp674_profile_runner_refuses_linked_root_alias_without_writing() {
    let fixture = ProfileRunnerFixture::new();
    let linked_custody = fixture.parent.join("linked-root-custody");
    let linked_root = fixture.parent.join("linked-root");
    fs::create_dir(&linked_custody).expect("linked root custody fixture directory");
    symlink(&linked_custody, &linked_root).expect("evidence root symlink");

    let output = fixture.run(&linked_root.join("."));
    assert!(
        !output.status.success(),
        "linked evidence root must fail closed"
    );
    assert!(
        !fixture.counter.exists(),
        "refusal must not invoke a backend"
    );
    assert_eq!(
        fs::read_dir(&linked_custody)
            .expect("linked root custody inventory")
            .count(),
        0,
        "the runner must not write through a linked root alias"
    );
}

// req: PERF-018
#[test]
fn wp674_profile_runner_refuses_symlink_ancestors_for_outputs_and_artifacts() {
    let fixture = ProfileRunnerFixture::new();
    let custody = fixture.parent.join("ancestor-custody");
    let linked_parent = fixture.parent.join("linked-parent");
    fs::create_dir(&custody).expect("ancestor custody fixture");
    symlink(&custody, &linked_parent).expect("output ancestor symlink");
    let output = fixture.run(&linked_parent.join("evidence"));
    assert!(
        !output.status.success(),
        "linked output ancestor must fail closed"
    );
    assert!(
        !fixture.counter.exists(),
        "no backend may run through linked custody"
    );
    assert_eq!(
        fs::read_dir(&custody).expect("custody inventory").count(),
        0
    );

    let artifact_custody = fixture.parent.join("artifact-custody");
    let artifact_link = fixture.parent.join("artifact-link");
    fs::create_dir(&artifact_custody).expect("artifact custody fixture");
    let linked_runner = artifact_custody.join("riffdb-app-baseline");
    fs::copy(&fixture.runner_bin, &linked_runner).expect("linked runner fixture");
    symlink(&artifact_custody, &artifact_link).expect("artifact ancestor symlink");
    let output = fixture
        .command(&fixture.parent.join("artifact-link-evidence"))
        .env(
            "RIFFDB_APP_BASELINE_RUNNER_BIN",
            artifact_link.join("riffdb-app-baseline"),
        )
        .output()
        .expect("artifact ancestor runner fixture must launch");
    assert!(
        !output.status.success(),
        "linked artifact ancestor must fail closed"
    );
    assert!(!fixture.parent.join("artifact-link-evidence").exists());
}

// req: PERF-018
#[test]
fn wp674_profile_runner_refuses_failed_output_inventory_without_writing() {
    let fixture = ProfileRunnerFixture::new();
    write_executable(&fixture.fake_bin.join("find"), "#!/bin/sh\nexit 37\n");
    let output_root = fixture.parent.join("failed-inventory");

    let output = fixture.run(&output_root);
    assert!(
        !output.status.success(),
        "failed bounded inventory must fail closed"
    );
    assert!(
        !fixture.counter.exists(),
        "refusal must not invoke a backend"
    );
    assert!(
        !output_root.join("attempts").exists(),
        "failed inventory must stop before attempt custody is created"
    );
}

// req: PERF-018
#[test]
fn wp674_profile_runner_refuses_oversized_generated_report_before_jq() {
    let fixture = ProfileRunnerFixture::new();
    let output_root = fixture.parent.join("oversized-report-evidence");
    let output = fixture
        .command(&output_root)
        .env("WP674_OVERSIZED_REPORT", "1")
        .output()
        .expect("oversized-report runner fixture must launch");
    assert!(
        !output.status.success(),
        "oversized generated report must fail closed"
    );
    assert_eq!(
        fs::read_to_string(&fixture.counter).expect("attempt counter"),
        "1\n",
        "oversized evidence must not consume a replacement"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("exceeds its byte bound"),
        "the refusal must precede jq parsing"
    );
}

// req: PERF-018
#[test]
fn wp674_profile_runner_refuses_host_invalid_profile_drift_without_replacement() {
    let fixture = ProfileRunnerFixture::new();
    let output_root = fixture.parent.join("profile-drift-evidence");
    let output = fixture
        .command(&output_root)
        .env("WP674_PROFILE_DRIFT", "1")
        .output()
        .expect("profile-drift runner fixture must launch");

    assert!(!output.status.success(), "profile drift must fail closed");
    assert_eq!(
        fs::read_to_string(&fixture.counter).expect("attempt counter"),
        "1\n",
        "profile drift must not consume a replacement"
    );
    assert_eq!(
        fs::read_to_string(output_root.join("attempts/status.tsv")).expect("attempt status ledger"),
        "",
        "profile drift must not be classified as host-invalid"
    );
    assert!(
        !output_root.join("attempts/host-attempt-2").exists(),
        "profile drift must not begin attempt two"
    );
}

// req: PERF-018
#[test]
fn wp674_profile_runner_refuses_artifact_drift_before_replacement() {
    let fixture = ProfileRunnerFixture::new();
    let output_root = fixture.parent.join("artifact-drift-evidence");
    let output = fixture
        .command(&output_root)
        .env("WP674_MUTATE_RUNNER_IDENTITY", "1")
        .output()
        .expect("artifact-drift runner fixture must launch");

    assert!(!output.status.success(), "artifact drift must fail closed");
    assert_eq!(
        fs::read_to_string(&fixture.counter).expect("attempt counter"),
        "1\n",
        "artifact drift must stop before a replacement"
    );
    assert_eq!(
        fs::read_to_string(output_root.join("attempts/status.tsv")).expect("attempt status ledger"),
        "",
        "artifact drift must not classify the first attempt as host-invalid"
    );
    assert!(
        output_root
            .join("attempts/campaign-identity.json")
            .is_file(),
        "the immutable pre-attempt campaign identity must be retained"
    );
}

// req: PERF-018
#[test]
fn wp674_profile_runner_records_two_host_invalid_attempts_without_a_third() {
    let fixture = ProfileRunnerFixture::new();
    let output_root = fixture.parent.join("two-host-invalid-evidence");
    let output = fixture
        .command(&output_root)
        .env("WP674_ALWAYS_HOST_INVALID", "1")
        .output()
        .expect("two-host-invalid runner fixture must launch");

    assert!(
        !output.status.success(),
        "two invalid host attempts must fail"
    );
    assert_eq!(
        fs::read_to_string(&fixture.counter).expect("attempt counter"),
        "2\n"
    );
    assert_eq!(
        fs::read_to_string(output_root.join("attempts/status.tsv")).expect("attempt status ledger"),
        "1\thost_invalid\n2\thost_invalid\n"
    );
    for attempt in 1..=2 {
        assert_eq!(
            fs::read_to_string(output_root.join(format!(
                "attempts/host-attempt-{attempt}/safe-app-point_get_ticket.json"
            )))
            .expect("retained host-invalid receipt"),
            host_invalid_receipt()
        );
    }
    assert!(
        !output_root.join("attempts/host-attempt-3").exists(),
        "a third attempt must never be created"
    );
}

// req: PERF-018
#[test]
fn wp674_profile_runner_keeps_errexit_enabled_inside_an_attempt() {
    let fixture = ProfileRunnerFixture::new();
    let output_root = fixture.parent.join("errexit-evidence");
    let output = fixture
        .command(&output_root)
        .env("WP674_BLOCK_ATTEMPT_TWO", "1")
        .output()
        .expect("errexit profile runner fixture must launch");
    let attempts = output_root.join("attempts");

    assert!(
        !output.status.success(),
        "unexpected attempt custody failure must stop the runner"
    );
    assert_eq!(
        fs::read_to_string(&fixture.counter).expect("attempt counter"),
        "1\n",
        "the runner must stop before invoking a backend for attempt two"
    );
    assert_eq!(
        fs::read_to_string(attempts.join("status.tsv")).expect("attempt status"),
        "1\thost_invalid\n"
    );
    assert!(
        attempts.join("host-attempt-2").is_file(),
        "the privilege-independent custody failure fixture must remain a file"
    );
}
