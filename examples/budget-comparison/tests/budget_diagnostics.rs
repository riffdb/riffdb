//! Layered budget diagnostics for optimization triage.
//!
//! Measures Postgres SQL and in-process RiffDB service paths with:
//! - per-operation latencies from the canonical sequential workload
//! - contention suite wall-clock
//! - amortized conflict-free create throughput on a warm database
//! - amortized successful allocate throughput on pre-created budgets
//!
//! This is diagnostic evidence, not the frozen WP-200 publication suite.
//! Run through `benchmarks/run-budget-diagnostics`.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use riffdb_budget_comparison_core::{
    AllocateBudget, Amount, BudgetBackend, BudgetKey, BudgetOperation, BudgetOutcome, CreateBudget,
    MatterId, OperationId, OrganizationId, WorkloadIdempotencyKey, canonical_workload,
    evaluate_sequential, expected_contention_observation, verify_contention, verify_sequential,
};
use riffdb_budget_comparison_postgres::{PostgresBudgetAdapter, live_database_url};
use serde_json::{Value, json};

use super::{BudgetServiceHarness, uuid_bytes_from_ordinal};

const REPORT_SCHEMA: &str = "riffdb.budget.diagnostics/v1";
const REPORT_ID: &str = "budget-layered-diagnostics";
const OUTPUT_ENV: &str = "RIFFDB_BUDGET_DIAGNOSTICS_OUTPUT";
const MODE_ENV: &str = "RIFFDB_BUDGET_DIAGNOSTICS_MODE";

#[test]
#[ignore = "run through benchmarks/run-budget-diagnostics"]
fn budget_layered_diagnostics_report() {
    // Service harness needs a multi-thread Tokio runtime for the coordinator.
    // The sync PostgreSQL client creates its own runtime and cannot connect while
    // a Tokio runtime is entered, so layers are measured in sequence.
    let config = DiagnosticsConfig::from_environment();
    let workload = canonical_workload();
    let expected_sequential =
        evaluate_sequential(&workload.sequential).expect("reference sequential");
    let expected_contention =
        expected_contention_observation(&workload.contention).expect("reference contention");

    let service_layer = {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_time()
            .build()
            .expect("diagnostics service runtime");
        let _guard = runtime.enter();
        measure_service_layer(
            &config,
            &workload.sequential,
            &workload.contention,
            &expected_sequential,
            &expected_contention,
        )
        // Drop runtime (and exit guard) before PostgreSQL work.
    };

    let mut layers = vec![service_layer];
    if let Some(database_url) = live_database_url().expect("live PostgreSQL config is valid") {
        let postgres = PostgresBudgetAdapter::new(database_url).expect("bounded database URL");
        layers.push(measure_postgres_layer(
            &postgres,
            &config,
            &workload.sequential,
            &workload.contention,
            &expected_sequential,
            &expected_contention,
        ));
    } else if required_environment("RIFFDB_BUDGET_DIAGNOSTICS_POSTGRES_REQUIRED") == "true" {
        panic!(
            "PostgreSQL diagnostics required but RIFFDB_BUDGET_POSTGRES_URL is unset and Docker auto-provision is not available in this test process"
        );
    }

    let comparisons = build_comparisons(&layers);
    let bottleneck_hints = bottleneck_hints(&layers, &comparisons);
    let report = json!({
        "schema": REPORT_SCHEMA,
        "report_id": REPORT_ID,
        "mode": config.mode.as_str(),
        "configuration": {
            "warmup_iterations": config.warmup_iterations,
            "measured_iterations": config.measured_iterations,
            "amortized_commands": config.amortized_commands,
            "contract_version": "LegalSpend/1",
            "distribution_method": "nearest-rank p50/p95/p99 over raw Instant samples",
            "timing_source": "std::time::Instant",
        },
        "layers": layers,
        "comparisons": comparisons,
        "bottleneck_hints": bottleneck_hints,
        "limitations": [
            "Diagnostic only: not the frozen WP-200 budget-comparison publication report.",
            "In-process RiffDB service excludes gRPC, MCP, server process start, bootstrap, and credential issuance.",
            "PostgreSQL timings include per-command connect+transaction+commit for the canonical adapter.",
            "Canonical sequential operations share fixed keys; each measured sequential/contention sample resets schema or harness state.",
            "Amortized create/allocate use unique keys on one warm backend and are the best kernel-path signal.",
        ],
    });
    let output = output_path();
    let encoded = serde_json::to_string_pretty(&report).expect("serialize diagnostics report");
    fs::write(&output, format!("{encoded}\n")).expect("write diagnostics report");
    println!("RIFFDB_BUDGET_DIAGNOSTICS_REPORT={}", output.display());
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiagnosticsMode {
    Smoke,
    Full,
}

impl DiagnosticsMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Full => "full",
        }
    }
}

#[derive(Clone, Debug)]
struct DiagnosticsConfig {
    mode: DiagnosticsMode,
    warmup_iterations: usize,
    measured_iterations: usize,
    amortized_commands: usize,
}

impl DiagnosticsConfig {
    fn from_environment() -> Self {
        match required_environment(MODE_ENV).as_str() {
            "smoke" => Self {
                mode: DiagnosticsMode::Smoke,
                warmup_iterations: 0,
                measured_iterations: 1,
                amortized_commands: 8,
            },
            "full" => Self {
                mode: DiagnosticsMode::Full,
                warmup_iterations: bounded_usize_environment(
                    "RIFFDB_BUDGET_DIAGNOSTICS_WARMUPS",
                    2,
                    0,
                    20,
                ),
                measured_iterations: bounded_usize_environment(
                    "RIFFDB_BUDGET_DIAGNOSTICS_SAMPLES",
                    12,
                    3,
                    100,
                ),
                amortized_commands: bounded_usize_environment(
                    "RIFFDB_BUDGET_DIAGNOSTICS_COMMANDS",
                    64,
                    8,
                    512,
                ),
            },
            mode => panic!("unsupported {MODE_ENV} value {mode:?}"),
        }
    }
}

#[derive(Default)]
struct SampleSet {
    samples_ns: Vec<u64>,
}

impl SampleSet {
    fn record(&mut self, elapsed: Duration) {
        self.samples_ns.push(duration_ns(elapsed));
    }

    fn summary(&self) -> Value {
        assert!(!self.samples_ns.is_empty(), "sample set is nonempty");
        let mut sorted = self.samples_ns.clone();
        sorted.sort_unstable();
        let (p50_ns, p95_ns, p99_ns) = percentiles(&sorted);
        json!({
            "sample_count": sorted.len(),
            "samples_ns": sorted,
            "min_ns": sorted[0],
            "p50_ns": p50_ns,
            "p95_ns": p95_ns,
            "p99_ns": p99_ns,
            "max_ns": *sorted.last().expect("nonempty"),
            "mean_ns": mean_ns(&sorted),
        })
    }
}

fn measure_service_layer(
    config: &DiagnosticsConfig,
    sequential: &riffdb_budget_comparison_core::SequentialWorkload,
    contention: &riffdb_budget_comparison_core::ContentionWorkload,
    expected_sequential: &riffdb_budget_comparison_core::SequentialObservation,
    expected_contention: &riffdb_budget_comparison_core::ContentionObservation,
) -> Value {
    let mut op_samples: BTreeMap<String, SampleSet> = BTreeMap::new();
    let mut harness_create = SampleSet::default();
    let mut sequential_wall = SampleSet::default();
    let mut contention_wall = SampleSet::default();
    let mut amortized_create = SampleSet::default();
    let mut amortized_allocate = SampleSet::default();
    let mut amortized_create_batch = SampleSet::default();
    let mut amortized_allocate_batch = SampleSet::default();

    for _ in 0..config.warmup_iterations {
        let _ = sample_service_canonical(sequential, contention);
        let _ = sample_service_amortized(config.amortized_commands);
    }

    for _ in 0..config.measured_iterations {
        let sample = sample_service_canonical(sequential, contention);
        verify_sequential(expected_sequential, &sample.sequential)
            .expect("service sequential oracle");
        verify_contention(expected_contention, &sample.contention)
            .expect("service contention oracle");
        harness_create.record(sample.harness_create);
        sequential_wall.record(sample.sequential_wall);
        contention_wall.record(sample.contention_wall);
        for (op_id, elapsed) in sample.operation_latencies {
            op_samples.entry(op_id).or_default().record(elapsed);
        }

        let amortized = sample_service_amortized(config.amortized_commands);
        for latency in amortized.create_latencies {
            amortized_create.record(latency);
        }
        for latency in amortized.allocate_latencies {
            amortized_allocate.record(latency);
        }
        amortized_create_batch.record(amortized.create_batch_wall);
        amortized_allocate_batch.record(amortized.allocate_batch_wall);
    }

    let create_ops = config.amortized_commands * config.measured_iterations;
    let allocate_ops = create_ops;
    layer_json(
        "riffdb_service_inprocess",
        "API-neutral RiffDB service over redb (no gRPC transport)",
        harness_create,
        sequential_wall,
        contention_wall,
        op_samples,
        amortized_create,
        amortized_allocate,
        amortized_create_batch,
        amortized_allocate_batch,
        create_ops,
        allocate_ops,
        config.amortized_commands,
    )
}

fn measure_postgres_layer(
    adapter: &PostgresBudgetAdapter,
    config: &DiagnosticsConfig,
    sequential: &riffdb_budget_comparison_core::SequentialWorkload,
    contention: &riffdb_budget_comparison_core::ContentionWorkload,
    expected_sequential: &riffdb_budget_comparison_core::SequentialObservation,
    expected_contention: &riffdb_budget_comparison_core::ContentionObservation,
) -> Value {
    let mut op_samples: BTreeMap<String, SampleSet> = BTreeMap::new();
    let mut schema_reset = SampleSet::default();
    let mut sequential_wall = SampleSet::default();
    let mut contention_wall = SampleSet::default();
    let mut amortized_create = SampleSet::default();
    let mut amortized_allocate = SampleSet::default();
    let mut amortized_create_batch = SampleSet::default();
    let mut amortized_allocate_batch = SampleSet::default();

    for _ in 0..config.warmup_iterations {
        let _ = sample_postgres_canonical(adapter, sequential, contention);
        let _ = sample_postgres_amortized(adapter, config.amortized_commands, 9_000_000);
    }

    for iteration in 0..config.measured_iterations {
        let sample = sample_postgres_canonical(adapter, sequential, contention);
        verify_sequential(expected_sequential, &sample.sequential)
            .expect("postgres sequential oracle");
        verify_contention(expected_contention, &sample.contention)
            .expect("postgres contention oracle");
        schema_reset.record(sample.schema_reset);
        sequential_wall.record(sample.sequential_wall);
        contention_wall.record(sample.contention_wall);
        for (op_id, elapsed) in sample.operation_latencies {
            op_samples.entry(op_id).or_default().record(elapsed);
        }

        let base = 10_000_000 + (iteration as u64) * 10_000;
        let amortized = sample_postgres_amortized(adapter, config.amortized_commands, base);
        for latency in amortized.create_latencies {
            amortized_create.record(latency);
        }
        for latency in amortized.allocate_latencies {
            amortized_allocate.record(latency);
        }
        amortized_create_batch.record(amortized.create_batch_wall);
        amortized_allocate_batch.record(amortized.allocate_batch_wall);
    }

    let create_ops = config.amortized_commands * config.measured_iterations;
    let allocate_ops = create_ops;
    layer_json(
        "postgres_sql",
        "canonical PostgreSQL READ COMMITTED adapter (connect per command, synchronous_commit=on)",
        schema_reset,
        sequential_wall,
        contention_wall,
        op_samples,
        amortized_create,
        amortized_allocate,
        amortized_create_batch,
        amortized_allocate_batch,
        create_ops,
        allocate_ops,
        config.amortized_commands,
    )
}

struct CanonicalSample {
    harness_create: Duration,
    schema_reset: Duration,
    sequential_wall: Duration,
    contention_wall: Duration,
    operation_latencies: Vec<(String, Duration)>,
    sequential: riffdb_budget_comparison_core::SequentialObservation,
    contention: riffdb_budget_comparison_core::ContentionObservation,
}

struct AmortizedSample {
    create_latencies: Vec<Duration>,
    allocate_latencies: Vec<Duration>,
    create_batch_wall: Duration,
    allocate_batch_wall: Duration,
}

fn sample_service_canonical(
    sequential: &riffdb_budget_comparison_core::SequentialWorkload,
    contention: &riffdb_budget_comparison_core::ContentionWorkload,
) -> CanonicalSample {
    let started = Instant::now();
    let mut harness = BudgetServiceHarness::new();
    let harness_create = started.elapsed();

    let sequential_started = Instant::now();
    let mut operation_latencies = Vec::with_capacity(sequential.operations.len());
    let mut outcomes = Vec::with_capacity(sequential.operations.len());
    for operation in &sequential.operations {
        let op_started = Instant::now();
        let observation = harness
            .adapter
            .execute(operation)
            .expect("service sequential operation");
        operation_latencies.push((
            operation.operation_id().as_str().to_owned(),
            op_started.elapsed(),
        ));
        outcomes.push(observation);
    }
    let sequential_wall = sequential_started.elapsed();
    let keys = sequential
        .operations
        .iter()
        .map(BudgetOperation::key)
        .collect::<std::collections::BTreeSet<_>>();
    let mut final_budgets = Vec::new();
    for key in keys {
        if let Some(budget) = harness.adapter.read_budget(key).expect("read final budget") {
            final_budgets.push(budget);
        }
    }
    final_budgets.sort();
    let sequential = riffdb_budget_comparison_core::SequentialObservation {
        case_id: sequential.case_id.clone(),
        outcomes,
        final_budgets,
    };
    // Sequential and contention share canonical keys; use a fresh harness.
    harness.stop();

    let contention_started = Instant::now();
    let mut contention_harness = BudgetServiceHarness::new();
    let contention_observation = contention_harness
        .adapter
        .run_contention(contention)
        .expect("service contention");
    let contention_wall = contention_started.elapsed();
    contention_harness.stop();

    CanonicalSample {
        harness_create,
        schema_reset: Duration::ZERO,
        sequential_wall,
        contention_wall,
        operation_latencies,
        sequential,
        contention: contention_observation,
    }
}

fn sample_postgres_canonical(
    adapter: &PostgresBudgetAdapter,
    sequential: &riffdb_budget_comparison_core::SequentialWorkload,
    contention: &riffdb_budget_comparison_core::ContentionWorkload,
) -> CanonicalSample {
    let reset_started = Instant::now();
    adapter.reset_schema().expect("postgres schema reset");
    let schema_reset = reset_started.elapsed();

    let sequential_started = Instant::now();
    let mut operation_latencies = Vec::with_capacity(sequential.operations.len());
    let mut outcomes = Vec::with_capacity(sequential.operations.len());
    for operation in &sequential.operations {
        let op_started = Instant::now();
        let observation = adapter
            .execute(operation)
            .expect("postgres sequential operation");
        operation_latencies.push((
            operation.operation_id().as_str().to_owned(),
            op_started.elapsed(),
        ));
        outcomes.push(observation);
    }
    let sequential_wall = sequential_started.elapsed();
    let keys = sequential
        .operations
        .iter()
        .map(BudgetOperation::key)
        .collect::<std::collections::BTreeSet<_>>();
    let mut final_budgets = Vec::new();
    for key in keys {
        if let Some(budget) = adapter.read_budget(key).expect("read final budget") {
            final_budgets.push(budget);
        }
    }
    final_budgets.sort();
    let sequential = riffdb_budget_comparison_core::SequentialObservation {
        case_id: sequential.case_id.clone(),
        outcomes,
        final_budgets,
    };

    adapter
        .reset_schema()
        .expect("postgres contention schema reset");
    let contention_started = Instant::now();
    let contention_observation = adapter
        .run_contention(contention)
        .expect("postgres contention");
    let contention_wall = contention_started.elapsed();

    CanonicalSample {
        harness_create: Duration::ZERO,
        schema_reset,
        sequential_wall,
        contention_wall,
        operation_latencies,
        sequential,
        contention: contention_observation,
    }
}

fn sample_service_amortized(command_count: usize) -> AmortizedSample {
    let mut harness =
        BudgetServiceHarness::with_durability(riffdb_commit::CoordinatorDurability::Sync);
    // Discard cold first command after harness construction.
    let _ = harness
        .adapter
        .execute(&create_operation("diag-warmup", 1))
        .expect("service warm create");

    let mut create_latencies = Vec::with_capacity(command_count);
    let create_batch = Instant::now();
    for ordinal in 0..command_count {
        let started = Instant::now();
        let observation = harness
            .adapter
            .execute(&create_operation("diag-create", 100 + ordinal as u64))
            .expect("service amortized create");
        create_latencies.push(started.elapsed());
        assert!(matches!(
            observation.outcome,
            BudgetOutcome::BudgetCreated { .. }
        ));
    }
    let create_batch_wall = create_batch.elapsed();

    let mut allocate_latencies = Vec::with_capacity(command_count);
    let allocate_batch = Instant::now();
    for ordinal in 0..command_count {
        let started = Instant::now();
        let observation = harness
            .adapter
            .execute(&allocate_operation("diag-allocate", 100 + ordinal as u64))
            .expect("service amortized allocate");
        allocate_latencies.push(started.elapsed());
        assert!(matches!(
            observation.outcome,
            BudgetOutcome::Allocated { .. }
        ));
    }
    let allocate_batch_wall = allocate_batch.elapsed();
    harness.stop();

    AmortizedSample {
        create_latencies,
        allocate_latencies,
        create_batch_wall,
        allocate_batch_wall,
    }
}

fn sample_postgres_amortized(
    adapter: &PostgresBudgetAdapter,
    command_count: usize,
    base_ordinal: u64,
) -> AmortizedSample {
    // Unique keys avoid reset; leave prior amortized rows in place.
    let mut create_latencies = Vec::with_capacity(command_count);
    let create_batch = Instant::now();
    for ordinal in 0..command_count {
        let started = Instant::now();
        let observation = adapter
            .execute(&create_operation(
                "diag-create",
                base_ordinal + ordinal as u64,
            ))
            .expect("postgres amortized create");
        create_latencies.push(started.elapsed());
        assert!(matches!(
            observation.outcome,
            BudgetOutcome::BudgetCreated { .. }
        ));
    }
    let create_batch_wall = create_batch.elapsed();

    let mut allocate_latencies = Vec::with_capacity(command_count);
    let allocate_batch = Instant::now();
    for ordinal in 0..command_count {
        let started = Instant::now();
        let observation = adapter
            .execute(&allocate_operation(
                "diag-allocate",
                base_ordinal + ordinal as u64,
            ))
            .expect("postgres amortized allocate");
        allocate_latencies.push(started.elapsed());
        assert!(matches!(
            observation.outcome,
            BudgetOutcome::Allocated { .. }
        ));
    }
    let allocate_batch_wall = allocate_batch.elapsed();

    AmortizedSample {
        create_latencies,
        allocate_latencies,
        create_batch_wall,
        allocate_batch_wall,
    }
}

#[allow(clippy::too_many_arguments)]
fn layer_json(
    layer_id: &str,
    description: &str,
    setup: SampleSet,
    sequential_wall: SampleSet,
    contention_wall: SampleSet,
    op_samples: BTreeMap<String, SampleSet>,
    amortized_create: SampleSet,
    amortized_allocate: SampleSet,
    amortized_create_batch: SampleSet,
    amortized_allocate_batch: SampleSet,
    create_ops: usize,
    allocate_ops: usize,
    amortized_commands: usize,
) -> Value {
    let setup_summary = setup.summary();
    let sequential_summary = sequential_wall.summary();
    let contention_summary = contention_wall.summary();
    let create_summary = amortized_create.summary();
    let allocate_summary = amortized_allocate.summary();
    let create_batch_summary = amortized_create_batch.summary();
    let allocate_batch_summary = amortized_allocate_batch.summary();

    let create_ops_per_second = rate_from_batch(&create_batch_summary, create_ops);
    let allocate_ops_per_second = rate_from_batch(&allocate_batch_summary, allocate_ops);

    let sequential_ops: Vec<Value> = op_samples
        .into_iter()
        .map(|(operation_id, samples)| {
            let mut summary = samples.summary();
            summary["operation_id"] = json!(operation_id);
            summary
        })
        .collect();

    let sequential_share = share_table(&sequential_ops);

    json!({
        "layer_id": layer_id,
        "description": description,
        "phases": {
            "setup": setup_summary,
            "sequential_wall": sequential_summary,
            "contention_wall": contention_summary,
            "amortized_create_command": create_summary,
            "amortized_allocate_command": allocate_summary,
            "amortized_create_batch": create_batch_summary,
            "amortized_allocate_batch": allocate_batch_summary,
        },
        "sequential_operations": sequential_ops,
        "sequential_time_share": sequential_share,
        "rates": {
            "amortized_create_ops_per_second": create_ops_per_second,
            "amortized_allocate_ops_per_second": allocate_ops_per_second,
            "amortized_commands_per_batch": amortized_commands,
            "amortized_create_total_ops": create_ops,
            "amortized_allocate_total_ops": allocate_ops,
        },
    })
}

fn share_table(operations: &[Value]) -> Vec<Value> {
    let total: u128 = operations
        .iter()
        .map(|row| u128::from(row["mean_ns"].as_u64().expect("operation mean is present")))
        .sum();
    if total == 0 {
        return Vec::new();
    }
    let mut rows: Vec<Value> = operations
        .iter()
        .map(|row| {
            let mean = row["mean_ns"].as_u64().expect("operation mean");
            let share_bps = (u128::from(mean) * 10_000) / total;
            json!({
                "operation_id": row["operation_id"],
                "mean_ns": mean,
                "share_basis_points": share_bps,
            })
        })
        .collect();
    rows.sort_by(|left, right| right["mean_ns"].as_u64().cmp(&left["mean_ns"].as_u64()));
    rows
}

fn rate_from_batch(batch_summary: &Value, total_ops: usize) -> u64 {
    let mean_batch_ns = batch_summary["mean_ns"].as_u64().expect("batch mean");
    if mean_batch_ns == 0 || total_ops == 0 {
        return 0;
    }
    // mean_batch is for amortized_commands; total_ops / measured iterations ≈ commands per batch
    // Use total wall implied by mean across measured iterations:
    // rate ≈ (ops_per_batch * 1e9) / mean_batch_ns where ops_per_batch = total_ops / sample_count
    let sample_count = batch_summary["sample_count"]
        .as_u64()
        .expect("sample count");
    let ops_per_batch = (total_ops as u64) / sample_count.max(1);
    ops_per_batch
        .saturating_mul(1_000_000_000)
        .checked_div(mean_batch_ns)
        .unwrap_or(0)
        .max(1)
}

fn build_comparisons(layers: &[Value]) -> Value {
    let service = layers
        .iter()
        .find(|layer| layer["layer_id"] == "riffdb_service_inprocess");
    let postgres = layers
        .iter()
        .find(|layer| layer["layer_id"] == "postgres_sql");
    let (Some(service), Some(postgres)) = (service, postgres) else {
        return json!({
            "available": false,
            "reason": "both riffdb_service_inprocess and postgres_sql layers are required for ratios",
        });
    };

    let mut sequential_ops = Vec::new();
    let service_ops = service["sequential_operations"]
        .as_array()
        .expect("service sequential operations");
    let postgres_ops = postgres["sequential_operations"]
        .as_array()
        .expect("postgres sequential operations");
    for service_op in service_ops {
        let op_id = service_op["operation_id"].as_str().expect("operation_id");
        let Some(postgres_op) = postgres_ops.iter().find(|row| row["operation_id"] == op_id) else {
            continue;
        };
        let service_p50 = service_op["p50_ns"].as_u64().expect("service p50");
        let postgres_p50 = postgres_op["p50_ns"].as_u64().expect("postgres p50");
        sequential_ops.push(json!({
            "operation_id": op_id,
            "riffdb_service_p50_ns": service_p50,
            "postgres_p50_ns": postgres_p50,
            "ratio_service_over_postgres": ratio(service_p50, postgres_p50),
        }));
    }
    sequential_ops.sort_by(|left, right| {
        right["ratio_service_over_postgres"]
            .as_f64()
            .partial_cmp(&left["ratio_service_over_postgres"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let phase_names = [
        "sequential_wall",
        "contention_wall",
        "amortized_create_command",
        "amortized_allocate_command",
    ];
    let mut phases = Vec::new();
    for name in phase_names {
        let service_p50 = service["phases"][name]["p50_ns"]
            .as_u64()
            .expect("service phase");
        let postgres_p50 = postgres["phases"][name]["p50_ns"]
            .as_u64()
            .expect("postgres phase");
        phases.push(json!({
            "phase": name,
            "riffdb_service_p50_ns": service_p50,
            "postgres_p50_ns": postgres_p50,
            "ratio_service_over_postgres": ratio(service_p50, postgres_p50),
        }));
    }

    json!({
        "available": true,
        "phases": phases,
        "sequential_operations": sequential_ops,
        "rates": {
            "riffdb_service_create_ops_per_second": service["rates"]["amortized_create_ops_per_second"],
            "postgres_create_ops_per_second": postgres["rates"]["amortized_create_ops_per_second"],
            "riffdb_service_allocate_ops_per_second": service["rates"]["amortized_allocate_ops_per_second"],
            "postgres_allocate_ops_per_second": postgres["rates"]["amortized_allocate_ops_per_second"],
        },
    })
}

fn bottleneck_hints(layers: &[Value], comparisons: &Value) -> Vec<Value> {
    let mut hints = Vec::new();

    if let Some(service) = layers
        .iter()
        .find(|layer| layer["layer_id"] == "riffdb_service_inprocess")
    {
        if let Some(shares) = service["sequential_time_share"].as_array()
            && let Some(top) = shares.first()
        {
            let share_bps = top["share_basis_points"].as_u64().unwrap_or(0);
            if share_bps >= 2_000 {
                hints.push(json!({
                    "severity": "high",
                    "layer": "riffdb_service_inprocess",
                    "signal": "sequential_time_share",
                    "detail": format!(
                        "operation {} accounts for {:.1}% of sequential mean time; profile that command class first",
                        top["operation_id"].as_str().unwrap_or("?"),
                        share_bps as f64 / 100.0
                    ),
                }));
            }
        }

        let setup_p50 = service["phases"]["setup"]["p50_ns"].as_u64().unwrap_or(0);
        let sequential_p50 = service["phases"]["sequential_wall"]["p50_ns"]
            .as_u64()
            .unwrap_or(1);
        if setup_p50 > sequential_p50 {
            hints.push(json!({
                "severity": "medium",
                "layer": "riffdb_service_inprocess",
                "signal": "setup_dominates_suite",
                "detail": "harness/database create exceeds sequential suite wall time; whole-process suite timings are setup-dominated",
            }));
        }

        let create_p50 = service["phases"]["amortized_create_command"]["p50_ns"]
            .as_u64()
            .unwrap_or(0);
        let allocate_p50 = service["phases"]["amortized_allocate_command"]["p50_ns"]
            .as_u64()
            .unwrap_or(0);
        if create_p50 > 0 && allocate_p50 > create_p50.saturating_mul(2) {
            hints.push(json!({
                "severity": "medium",
                "layer": "riffdb_service_inprocess",
                "signal": "allocate_heavier_than_create",
                "detail": "amortized AllocateBudget p50 is more than 2x CreateBudget; focus conflict/precondition/event paths",
            }));
        } else if allocate_p50 > 0 && create_p50 > allocate_p50.saturating_mul(2) {
            hints.push(json!({
                "severity": "medium",
                "layer": "riffdb_service_inprocess",
                "signal": "create_heavier_than_allocate",
                "detail": "amortized CreateBudget p50 is more than 2x AllocateBudget; focus catalog/entity-insert paths",
            }));
        }
    }

    if comparisons["available"] == true {
        if let Some(phases) = comparisons["phases"].as_array() {
            for phase in phases {
                let ratio = phase["ratio_service_over_postgres"].as_f64().unwrap_or(1.0);
                let name = phase["phase"].as_str().unwrap_or("phase");
                if ratio >= 5.0 {
                    hints.push(json!({
                        "severity": "high",
                        "layer": "cross_layer",
                        "signal": "large_service_gap",
                        "detail": format!(
                            "{name} service/postgres p50 ratio is {ratio:.2}; kernel path is a primary optimization candidate"
                        ),
                    }));
                } else if ratio <= 0.5 {
                    hints.push(json!({
                        "severity": "info",
                        "layer": "cross_layer",
                        "signal": "service_ahead",
                        "detail": format!(
                            "{name} service/postgres p50 ratio is {ratio:.2}; RiffDB service is faster on this phase"
                        ),
                    }));
                }
            }
        }

        if layers
            .iter()
            .any(|layer| layer["layer_id"] == "postgres_sql")
            && layers
                .iter()
                .any(|layer| layer["layer_id"] == "riffdb_service_inprocess")
        {
            hints.push(json!({
                "severity": "info",
                "layer": "methodology",
                "signal": "public_path_separate",
                "detail": "Compare these kernel ratios to public gRPC phase diagnostics; if public/service >> service/postgres, transport and process lifecycle dominate",
            }));
        }
    }

    if hints.is_empty() {
        hints.push(json!({
            "severity": "info",
            "layer": "methodology",
            "signal": "no_dominant_signal",
            "detail": "No single phase crossed diagnostic thresholds; inspect raw p50 tables and public-path phase report",
        }));
    }
    hints
}

fn create_operation(prefix: &str, ordinal: u64) -> BudgetOperation {
    BudgetOperation::Create(CreateBudget {
        operation_id: OperationId::new(format!("{prefix}-{ordinal}"))
            .expect("bounded diagnostics operation ID"),
        idempotency_key: WorkloadIdempotencyKey::new(format!("diag-{prefix}-create-{ordinal}"))
            .expect("bounded diagnostics idempotency key"),
        key: BudgetKey {
            organization_id: OrganizationId::from_bytes(uuid_bytes_from_ordinal(0x41, ordinal)),
            fiscal_year: 2026,
        },
        approved_amount: Amount::from_minor_units(1_000_000_000)
            .expect("diagnostics approval fits decimal<28,2>"),
    })
}

fn allocate_operation(prefix: &str, ordinal: u64) -> BudgetOperation {
    BudgetOperation::Allocate(AllocateBudget {
        operation_id: OperationId::new(format!("{prefix}-{ordinal}"))
            .expect("bounded diagnostics operation ID"),
        idempotency_key: WorkloadIdempotencyKey::new(format!("diag-{prefix}-allocate-{ordinal}"))
            .expect("bounded diagnostics idempotency key"),
        key: BudgetKey {
            organization_id: OrganizationId::from_bytes(uuid_bytes_from_ordinal(0x41, ordinal)),
            fiscal_year: 2026,
        },
        matter_id: MatterId::from_bytes(uuid_bytes_from_ordinal(0x42, ordinal)),
        amount: Amount::MINIMUM_POSITIVE,
    })
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    numerator as f64 / denominator as f64
}

fn percentiles(sorted: &[u64]) -> (u64, u64, u64) {
    (
        nearest_rank(sorted, 50),
        nearest_rank(sorted, 95),
        nearest_rank(sorted, 99),
    )
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    assert!(!sorted.is_empty());
    assert!((1..=100).contains(&percentile));
    let numerator = sorted
        .len()
        .checked_mul(percentile)
        .expect("bounded percentile numerator");
    let rank = numerator.div_ceil(100).max(1);
    sorted[rank - 1]
}

fn mean_ns(sorted: &[u64]) -> u64 {
    let sum: u128 = sorted.iter().map(|sample| u128::from(*sample)).sum();
    u64::try_from(sum / sorted.len() as u128).expect("mean fits u64")
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("diagnostics sample fits u64 nanoseconds")
}

fn output_path() -> PathBuf {
    let raw = required_environment(OUTPUT_ENV);
    assert!(raw.len() <= 4_096, "{OUTPUT_ENV} exceeds path bound");
    let path = PathBuf::from(raw);
    assert!(path.is_absolute(), "{OUTPUT_ENV} must be absolute");
    path
}

fn required_environment(name: &str) -> String {
    let value = std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    assert!(!value.is_empty(), "{name} must not be empty");
    assert!(value.len() <= 16_384, "{name} exceeds metadata bound");
    value
}

fn bounded_usize_environment(name: &str, default: usize, minimum: usize, maximum: usize) -> usize {
    let value = std::env::var(name).map_or(default, |raw| {
        raw.parse()
            .unwrap_or_else(|_| panic!("{name} must be a usize"))
    });
    assert!(
        (minimum..=maximum).contains(&value),
        "{name} must be in {minimum}..={maximum}"
    );
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratio_and_percentiles_are_stable() {
        assert!((ratio(350, 100) - 3.5).abs() < f64::EPSILON);
        assert_eq!(percentiles(&[10, 20, 30, 40, 50]), (30, 50, 50));
    }

    #[test]
    fn share_table_orders_by_mean_descending() {
        let operations = vec![
            json!({"operation_id": "a", "mean_ns": 100}),
            json!({"operation_id": "b", "mean_ns": 300}),
            json!({"operation_id": "c", "mean_ns": 100}),
        ];
        let shares = share_table(&operations);
        assert_eq!(shares[0]["operation_id"], "b");
        assert_eq!(shares[0]["share_basis_points"], 6_000);
    }
}
