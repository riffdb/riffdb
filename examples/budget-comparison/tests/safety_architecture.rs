//! Static ownership and contamination checks for WP-139 evidence.

#![forbid(unsafe_code)]

mod bench_root_support;

use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

const POSTGRES_IMAGE: &str = "postgres:18.4-bookworm@sha256:d9c83446333daec3f0588cc709adb80c26090b7f9f0f7ec8d43c243385d79818";
const PUBLIC_RUN_SUCCESS: &[u8] =
    b"{\"schema\":\"riffdb.budget.public-run/v1\",\"adapter\":\"riffdb-public-grpc-v1\",\"case\":\"sequential\",\"workload_version\":1,\"status\":\"passed\"}\n";

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
fn safety_package_is_nonproduction_and_cannot_enter_benchmarks() -> TestResult<()> {
    let comparison_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repository_root = comparison_root
        .parent()
        .and_then(Path::parent)
        .ok_or("comparison workspace was not below the repository root")?;
    let safety_manifest = fs::read_to_string(comparison_root.join("safety-evidence/Cargo.toml"))?;
    assert!(safety_manifest.contains("name = \"riffdb-budget-safety-evidence\""));
    assert!(!safety_manifest.contains("[[bench]]"));
    assert!(!safety_manifest.contains("bench = true"));

    let dependencies = dependency_names(&safety_manifest)?;
    let allowed = BTreeSet::from([
        "postgres",
        "riffdb-budget-comparison-core",
        "riffdb-budget-comparison-postgres",
        "riffdb-budget-comparison-riffdb-grpc",
        "riffdb-client-rust",
        "serde_json",
        "tokio",
        "tonic",
    ]);
    assert!(
        dependencies.is_subset(&allowed),
        "safety package has an unapproved direct dependency"
    );
    for forbidden in [
        "riffdb-api-mcp",
        "riffdb-auth",
        "riffdb-catalog",
        "riffdb-commit",
        "riffdb-conflict",
        "riffdb-contract-compiler",
        "riffdb-idempotency",
        "riffdb-policy",
        "riffdb-runtime",
        "riffdb-server",
        "riffdb-service",
        "riffdb-storage-api",
        "riffdb-storage-redb",
    ] {
        assert!(!dependencies.contains(forbidden), "{forbidden}");
    }

    let root_manifest = fs::read_to_string(repository_root.join("Cargo.toml"))?;
    let root_lock = fs::read_to_string(repository_root.join("Cargo.lock"))?;
    assert!(!root_manifest.contains("riffdb-budget-safety-evidence"));
    assert!(!root_lock.contains("name = \"riffdb-budget-safety-evidence\""));
    assert!(!root_manifest.contains("postgres"));
    assert!(!root_lock.contains("name = \"postgres\""));

    for manifest in files_named(&repository_root.join("benchmarks"), "Cargo.toml", 128)? {
        let source = fs::read_to_string(manifest)?;
        assert!(!source.contains("riffdb-budget-safety-evidence"));
        assert!(!source.contains("safety-evidence"));
    }

    let safety_source = rust_source_tree(&comparison_root.join("safety-evidence/src"), 128)?;
    assert!(!safety_source.contains("impl BudgetBackend for"));
    assert!(!safety_source.contains("impl riffdb_budget_comparison_core::BudgetBackend for"));
    for performance_measurement in [
        "Instant::now",
        ".elapsed()",
        "\"latency\"",
        "\"throughput\"",
        "transactions_per_second",
    ] {
        assert!(
            !safety_source.contains(performance_measurement),
            "safety source contains performance measurement {performance_measurement}"
        );
    }
    Ok(())
}

#[test]
fn benchmark_manifest_scan_ignores_generated_target_trees() -> TestResult<()> {
    let owned = bench_root_support::unique_bench_dir("wp139-target-scan");
    let root = owned.path().to_path_buf();
    // Keep owned alive for the test body (Drop cleans the tree).
    let _owned = owned;
    let cleanup = TestDirectory(root.clone());
    fs::create_dir(root.join("source"))?;
    fs::write(
        root.join("source/Cargo.toml"),
        "[package]\nname = \"source\"\n",
    )?;
    fs::create_dir(root.join("target"))?;
    for index in 0..256 {
        fs::write(root.join("target").join(format!("artifact-{index}")), [])?;
    }

    assert_eq!(
        files_named(&root, "Cargo.toml", 8)?,
        [root.join("source/Cargo.toml")]
    );
    drop(cleanup);
    Ok(())
}

#[test]
fn canonical_adapters_and_public_runner_remain_separate() {
    let postgres = include_str!("../postgres/src/lib.rs");
    let profile = include_str!("../core/src/profile.rs");
    let public_runner = include_str!("../riffdb-grpc/src/bin/riffdb-budget-public.rs");
    let public_success = include_bytes!("../riffdb-grpc/fixtures/public-run-v1-success.jsonl");

    assert!(postgres.contains("FOR UPDATE"));
    assert!(postgres.contains("impl BudgetBackend for PostgresBudgetAdapter"));
    assert!(!postgres.contains("safety-evidence"));
    assert!(!postgres.contains("NegativeControl"));
    assert!(profile.contains("adapter: \"postgresql-explicit-sql-v1\""));
    assert!(profile.contains("conflict_control: \"SELECT ... FOR UPDATE"));
    assert!(!profile.contains("safety-evidence"));

    assert!(public_runner.contains("const PROTOCOL: &str = \"riffdb.budget.public-run/v1\";"));
    assert!(public_runner.contains("\"same_key_replay\""));
    assert!(!public_runner.contains("riffdb.budget.safety-evidence/v1"));
    assert_eq!(public_success, PUBLIC_RUN_SUCCESS);
}

#[test]
fn canonical_comparison_artifact_bytes_are_frozen() {
    for (bytes, expected) in [
        (
            include_bytes!("../postgres/src/lib.rs").as_slice(),
            0xe4f1_1835_06a5_f4fb,
        ),
        (
            include_bytes!("../core/src/profile.rs").as_slice(),
            0xc798_b88b_02e3_24e5,
        ),
        (
            include_bytes!("../fixtures/postgres-guarantees-v1.json").as_slice(),
            0x7ffb_3f10_1959_574b,
        ),
        (
            include_bytes!("../riffdb-grpc/src/bin/riffdb-budget-public.rs").as_slice(),
            0x01c8_6804_c9d8_fb9e,
        ),
        (
            include_bytes!("../riffdb-grpc/fixtures/public-run-v1-success.jsonl").as_slice(),
            0x3eb3_2f53_9e80_10b4,
        ),
        (
            include_bytes!("../riffdb-grpc/fixtures/public-run-v1-checked-error.txt").as_slice(),
            0x2872_e5cf_f818_7b20,
        ),
        (
            include_bytes!("../riffdb-grpc/fixtures/public-run-v1-invalid-invocation.txt")
                .as_slice(),
            0x005a_3cb8_b275_a179,
        ),
    ] {
        assert_eq!(fnv1a64(bytes), expected);
    }
}

#[test]
fn public_inventory_has_no_generic_application_dml() {
    let services = include_str!("../../../proto/riffdb/v1/services.proto");
    let command_service = service_body(services, "CommandService").expect("CommandService");
    let rpcs = command_service
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("rpc "))
        .collect::<Vec<_>>();
    assert_eq!(
        rpcs,
        [
            "rpc Execute(ExecuteCommandRequest) returns (ExecuteCommandResponse);",
            "rpc ExecuteBatch(ExecuteCommandBatchRequest) returns (ExecuteCommandBatchResponse);",
            "rpc GetOutcome(GetOutcomeRequest) returns (GetOutcomeResponse);",
        ]
    );
    let lower = services.to_ascii_lowercase();
    for generic_dml in ["rpc insert", "rpc update", "rpc delete"] {
        assert!(!lower.contains(generic_dml), "{generic_dml}");
    }

    let client = include_str!("../../../crates/riffdb-client-rust/src/generated/client.rs");
    for generic_dml in [
        "pub async fn insert",
        "pub async fn update",
        "pub async fn delete",
    ] {
        assert!(!client.contains(generic_dml), "{generic_dml}");
    }

    let service_adr = include_str!("../../../adr/0007-shared-application-service-boundary.md");
    assert!(service_adr.contains("- **Status:** Accepted"));
    assert!(
        service_adr.contains("API adapters may call the credential-authentication entry point")
    );
    assert!(service_adr.contains("They do not call a policy evaluator"));
}

#[test]
fn safety_ci_and_process_contract_are_registered() {
    let workflow = include_str!("../../../.github/workflows/budget-comparison.yml");
    let expected_image_row = format!("image: {POSTGRES_IMAGE}");
    assert_eq!(
        workflow
            .lines()
            .filter(|line| line.trim() == expected_image_row)
            .count(),
        1
    );
    assert!(workflow.contains("RIFFDB_BUDGET_POSTGRES_REQUIRED: \"1\""));
    assert!(workflow.contains("riffdb-budget-safety"));
    assert!(workflow.contains("--test safety_evidence"));
    assert!(workflow.contains("budget-safety-fixtures"));

    let manifest = include_str!("../Cargo.toml");
    assert!(registered_test(
        manifest,
        "safety_evidence",
        "tests/safety_evidence.rs"
    ));
    assert!(registered_test(
        manifest,
        "safety_architecture",
        "tests/safety_architecture.rs"
    ));

    let script = include_str!("../../../scripts/budget-safety-demo");
    assert!(script.contains("RIFFDB_BUDGET_POSTGRES_REQUIRED=1"));
    assert!(script.contains("RIFFDB_BUDGET_RIFFDBD_BIN="));
    assert!(script.contains("RIFFDB_BUDGET_SAFETY_BIN="));
    assert!(script.contains("RIFFDB_BUDGET_SAFETY_REPORT_PATH="));
    assert!(script.contains("--test safety_evidence"));
    assert!(script.contains("cmp -s \"$report_path\" \"$expected_report\""));
    assert!(script.contains("cat \"$expected_report\""));
    assert!(!script.contains("--nocapture"));

    let process_test = include_str!("safety_evidence.rs");
    assert!(process_test.contains(".arg(\"--backup-root\")"));
}

fn dependency_names(manifest: &str) -> Result<BTreeSet<&str>, &'static str> {
    let dependencies = manifest
        .split_once("[dependencies]\n")
        .ok_or("safety manifest has no dependency section")?
        .1
        .split_once("\n[")
        .map_or_else(
            || {
                manifest
                    .split_once("[dependencies]\n")
                    .map(|(_, rest)| rest)
                    .unwrap_or_default()
            },
            |(section, _)| section,
        );
    dependencies
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            line.split_once('=')
                .map(|(name, _)| name.trim())
                .ok_or("invalid dependency row")
        })
        .collect()
}

fn files_named(root: &Path, name: &str, maximum: usize) -> TestResult<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut pending = vec![root.to_path_buf()];
    let mut matches = Vec::new();
    let mut visited = 0_usize;
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            visited = visited.saturating_add(1);
            if visited > maximum {
                return Err("architecture scan exceeded its file bound".into());
            }
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                if entry.file_name() != "target" {
                    pending.push(entry.path());
                }
            } else if file_type.is_file() && entry.file_name() == name {
                matches.push(entry.path());
            }
        }
    }
    matches.sort();
    Ok(matches)
}

struct TestDirectory(PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn rust_source_tree(root: &Path, maximum: usize) -> TestResult<String> {
    let files = files_with_extension(root, "rs", maximum)?;
    let mut combined = String::new();
    for path in files {
        let metadata = fs::metadata(&path)?;
        if metadata.len() > 1_048_576 {
            return Err("safety source file exceeded its architecture bound".into());
        }
        combined.push_str(&fs::read_to_string(path)?);
        combined.push('\n');
    }
    Ok(combined)
}

fn files_with_extension(root: &Path, extension: &str, maximum: usize) -> TestResult<Vec<PathBuf>> {
    if !root.exists() {
        return Err("safety source directory is missing".into());
    }
    let mut pending = vec![root.to_path_buf()];
    let mut matches = Vec::new();
    let mut visited = 0_usize;
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            visited = visited.saturating_add(1);
            if visited > maximum {
                return Err("safety source scan exceeded its file bound".into());
            }
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file()
                && entry.path().extension().and_then(|value| value.to_str()) == Some(extension)
            {
                matches.push(entry.path());
            }
        }
    }
    matches.sort();
    Ok(matches)
}

fn service_body<'a>(source: &'a str, name: &str) -> Option<&'a str> {
    let marker = format!("service {name} {{");
    let body = source.split_once(&marker)?.1;
    body.split_once('}').map(|(body, _)| body)
}

fn registered_test(manifest: &str, name: &str, path: &str) -> bool {
    manifest.split("[[test]]").skip(1).any(|section| {
        let section = section.split_once("\n[").map_or(section, |(head, _)| head);
        section.contains(&format!("name = \"{name}\""))
            && section.contains(&format!("path = \"{path}\""))
    })
}

const fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut index = 0_usize;
    while index < bytes.len() {
        hash ^= bytes[index] as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        index += 1;
    }
    hash
}
