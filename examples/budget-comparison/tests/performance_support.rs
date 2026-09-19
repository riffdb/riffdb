//! WP-200-only semantic performance evidence over the real comparison service.

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use riffdb_budget_comparison_core::{
    AllocateBudget, Amount, BudgetBackend, BudgetKey, BudgetOperation, BudgetOutcome, CreateBudget,
    MatterId, OperationId, OrganizationId, WorkloadIdempotencyKey,
};
use riffdb_commit::CoordinatorDurability;
use riffdb_projection::{
    ProjectionController, ProjectionInitializationResult, ProjectionNotifier,
    ProjectionBatchBuilder, ProjectionSchemaRegistry, evaluate_projection_commit,
};
use riffdb_service::{CommandDurability, JournaledCompletion};
use riffdb_storage_api::{
    AuthoritativeScanReader, CheckedProjectionSchema, CommitScanPageV1, CommitScanRequest,
    DurabilityMode, ProjectionApplyBatchResult, ProjectionApplySnapshotReader,
    ProjectionControlResult, ProjectionGenerationPosition,
    ProjectionLifecycleV1, ProjectionQueryReader, StorageScanLimit,
};
use riffdb_types::{CommitSequence, FrontierPosition, ProjectionGeneration};
use serde_json::{Value, json};

use super::{BudgetServiceHarness, uuid_bytes_from_ordinal};

const REPORT_SCHEMA: &str = "riffdb.poc-semantic-benchmark-report/v1";
const REPORT_ID: &str = "poc-semantic-workloads";
const OUTPUT_ENV: &str = "RIFFDB_POC_BENCHMARK_OUTPUT";
const MODE_ENV: &str = "RIFFDB_POC_BENCHMARK_MODE";
const ANALYSIS_COMMAND_ENV: &str = "RIFFDB_POC_BENCHMARK_ANALYSIS_COMMAND";

#[test]
#[ignore = "run through benchmarks/poc-semantic-workloads/run"]
fn poc_semantic_performance_report() {
    super::run_async(async move {
        let config = BenchmarkConfig::from_environment();
        let report = run_report(&config);
        let output = output_path();
        let encoded = serde_json::to_string_pretty(&report).expect("serialize benchmark report");
        fs::write(&output, format!("{encoded}\n"))
            .expect("write benchmark report atomically enough for evidence runner");
        println!("RIFFDB_POC_BENCHMARK_REPORT={}", output.display());
    });
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BenchmarkMode {
    Smoke,
    Full,
}

impl BenchmarkMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Full => "full",
        }
    }
}

#[derive(Clone, Debug)]
struct BenchmarkConfig {
    mode: BenchmarkMode,
    warmup_iterations: usize,
    measured_iterations: usize,
    commands_per_iteration: usize,
    scan_records: usize,
    projection_records: usize,
    restart_record_counts: [usize; 3],
}

impl BenchmarkConfig {
    fn from_environment() -> Self {
        match required_environment(MODE_ENV).as_str() {
            "smoke" => Self {
                mode: BenchmarkMode::Smoke,
                warmup_iterations: 0,
                measured_iterations: 1,
                commands_per_iteration: 2,
                scan_records: 8,
                projection_records: 4,
                restart_record_counts: [2, 16, 128],
            },
            "full" => Self {
                mode: BenchmarkMode::Full,
                warmup_iterations: bounded_usize_environment(
                    "RIFFDB_POC_BENCHMARK_WARMUPS",
                    2,
                    0,
                    20,
                ),
                measured_iterations: bounded_usize_environment(
                    "RIFFDB_POC_BENCHMARK_SAMPLES",
                    9,
                    3,
                    100,
                ),
                commands_per_iteration: bounded_usize_environment(
                    "RIFFDB_POC_BENCHMARK_COMMANDS",
                    64,
                    1,
                    512,
                ),
                scan_records: bounded_usize_environment(
                    "RIFFDB_POC_BENCHMARK_SCAN_RECORDS",
                    256,
                    8,
                    2_048,
                ),
                projection_records: bounded_usize_environment(
                    "RIFFDB_POC_BENCHMARK_PROJECTION_RECORDS",
                    128,
                    4,
                    1_024,
                ),
                restart_record_counts: [16, 128, 1_024],
            },
            mode => panic!("unsupported {MODE_ENV} value {mode:?}"),
        }
    }
}

#[derive(Default)]
struct Measurement {
    samples_ns: Vec<u64>,
    total_work: u128,
    total_wall_ns: u128,
}

impl Measurement {
    fn record_latency(&mut self, elapsed: Duration) {
        self.samples_ns.push(duration_ns(elapsed));
    }

    fn record_batch(&mut self, elapsed: Duration, work: usize) {
        self.total_work = self
            .total_work
            .checked_add(u128::try_from(work).expect("bounded benchmark work"))
            .expect("bounded benchmark work sum");
        self.total_wall_ns = self
            .total_wall_ns
            .checked_add(elapsed.as_nanos())
            .expect("bounded benchmark duration sum");
    }

    fn row(self, family: &str, workload_id: &str) -> Value {
        assert!(!self.samples_ns.is_empty(), "measurement has samples");
        let mut samples_ns = self.samples_ns;
        samples_ns.sort_unstable();
        let (p50_ns, p95_ns, p99_ns) = percentiles(&samples_ns);
        json!({
            "family": family,
            "workload_id": workload_id,
            "sample_count": samples_ns.len(),
            "samples_ns": samples_ns,
            "p50_ns": p50_ns,
            "p95_ns": p95_ns,
            "p99_ns": p99_ns,
        })
    }

    fn rate_evidence(&self) -> (u64, u64, u64) {
        assert!(self.total_work > 0, "rate measurement has work");
        assert!(self.total_wall_ns > 0, "rate measurement has elapsed time");
        let rate = self
            .total_work
            .checked_mul(1_000_000_000)
            .expect("bounded benchmark rate numerator")
            / self.total_wall_ns;
        (
            u64::try_from(self.total_work).expect("benchmark work fits u64"),
            u64::try_from(self.total_wall_ns).expect("benchmark duration fits u64"),
            u64::try_from(rate.max(1)).expect("benchmark rate fits u64"),
        )
    }
}

fn run_report(config: &BenchmarkConfig) -> Value {
    correctness_preflight();

    let mut workloads = Vec::new();
    workloads.push(conflict_free_row(config));
    for concurrency in [1_usize, 8, 32, 128] {
        workloads.push(hot_key_row(config, concurrency));
    }
    workloads.push(replay_row(config));
    workloads.push(commit_scan_row(config));
    workloads.push(projection_catch_up_row(config));
    workloads.extend(restart_rows(config));
    workloads.push(durability_row(config, CoordinatorDurability::Sync));
    workloads.push(durability_row(config, CoordinatorDurability::Group));
    let sample_count = workloads
        .iter()
        .map(|row| {
            row["sample_count"]
                .as_u64()
                .expect("workload sample count is a u64")
        })
        .sum::<u64>();

    json!({
        "schema": REPORT_SCHEMA,
        "report_id": REPORT_ID,
        "publication_status": required_environment("RIFFDB_POC_BENCHMARK_PUBLICATION_STATUS"),
        "source": {
            "revision": required_environment("RIFFDB_POC_BENCHMARK_GIT_REVISION"),
            "dirty": parse_bool_environment("RIFFDB_POC_BENCHMARK_GIT_DIRTY"),
            "rust_version": required_environment("RIFFDB_POC_BENCHMARK_RUST_VERSION"),
            "target": required_environment("RIFFDB_POC_BENCHMARK_TARGET"),
            "raw_artifact": required_environment("RIFFDB_POC_BENCHMARK_RAW_ARTIFACT"),
            "raw_sha256": required_environment("RIFFDB_POC_BENCHMARK_RAW_SHA256"),
        },
        "environment": {
            "cpu": required_environment("RIFFDB_POC_BENCHMARK_CPU"),
            "memory": required_environment("RIFFDB_POC_BENCHMARK_MEMORY"),
            "storage_medium": required_environment("RIFFDB_POC_BENCHMARK_STORAGE_MEDIUM"),
            "operating_system": required_environment("RIFFDB_POC_BENCHMARK_OS"),
            "kernel": required_environment("RIFFDB_POC_BENCHMARK_KERNEL"),
            "database_root": required_environment("RIFFDB_POC_BENCHMARK_DATABASE_ROOT"),
            "filesystem": required_environment("RIFFDB_POC_BENCHMARK_FILESYSTEM"),
            "mount_options": required_environment("RIFFDB_POC_BENCHMARK_MOUNT_OPTIONS"),
        },
        "configuration": {
            "build_profile": "release",
            "features": [],
            "contract_version": "LegalSpend/1",
            "workload_distribution": "deterministic generated LegalSpend commands",
            "durability_modes": ["group_commit", "synchronous"],
            "warmup_runs": config.warmup_iterations,
            "sample_count": sample_count,
            "distribution_method": "raw samples with nearest-rank p50/p95/p99",
            "analysis_command": required_environment(ANALYSIS_COMMAND_ENV),
            "mode": config.mode.as_str(),
            "measured_iterations": config.measured_iterations,
            "commands_per_iteration": config.commands_per_iteration,
            "scan_records": config.scan_records,
            "projection_records": config.projection_records,
            "restart_commit_counts": config.restart_record_counts,
        },
        "workloads": workloads,
        "limitations": [
            "POC results are architectural feedback, not marketing claims or an absolute TPS gate.",
            "The API-neutral service harness uses production semantic components; transport framing is outside these component measurements.",
            "group_commit measures the current explicit mode with one command per transaction; it makes no batching, scheduling, fairness, or amortization claim.",
            "Projection catch-up is accepted only after the derived frontier equals the authoritative head.",
        ],
    })
}

fn correctness_preflight() {
    let workload = riffdb_budget_comparison_core::canonical_workload();
    let mut harness = BudgetServiceHarness::new();
    let expected = riffdb_budget_comparison_core::evaluate_sequential(&workload.sequential)
        .expect("reference preflight");
    let actual =
        riffdb_budget_comparison_core::observe_sequential(&harness.adapter, &workload.sequential)
            .expect("service preflight");
    riffdb_budget_comparison_core::verify_sequential(&expected, &actual)
        .expect("benchmark correctness preflight");
    harness.stop();
}

fn conflict_free_row(config: &BenchmarkConfig) -> Value {
    for _ in 0..config.warmup_iterations {
        let _ = measure_conflict_free(config.commands_per_iteration, CoordinatorDurability::Sync);
    }
    let mut measurement = Measurement::default();
    for iteration in 0..config.measured_iterations {
        let (latencies, elapsed) =
            measure_conflict_free(config.commands_per_iteration, CoordinatorDurability::Sync);
        for latency in latencies {
            measurement.record_latency(latency);
        }
        measurement.record_batch(elapsed, config.commands_per_iteration);
        assert!(iteration < 100, "bounded measured iterations");
    }
    let (rate_total_work, rate_total_wall_ns, rate) = measurement.rate_evidence();
    let mut row = measurement.row("conflict_free_command", "conflict-free-command-v1");
    row["rate_total_work"] = json!(rate_total_work);
    row["rate_total_wall_ns"] = json!(rate_total_wall_ns);
    row["ops_per_second"] = json!(rate);
    row["concurrency"] = json!(1);
    row["durability_mode"] = json!("synchronous");
    row["commands_per_iteration"] = json!(config.commands_per_iteration);
    row["correctness"] = json!("all commands committed on disjoint conflict keys");
    row
}

fn measure_conflict_free(
    command_count: usize,
    durability: CoordinatorDurability,
) -> (Vec<Duration>, Duration) {
    let mut harness = BudgetServiceHarness::with_durability(durability);
    let operations = (0..command_count)
        .map(|ordinal| create_operation("conflict-free", 1_000_000 + ordinal as u64))
        .collect::<Vec<_>>();
    let wall = Instant::now();
    let mut latencies = Vec::with_capacity(command_count);
    for operation in &operations {
        let started = Instant::now();
        let observation = harness
            .adapter
            .execute_with_metadata(operation)
            .expect("conflict-free command");
        latencies.push(started.elapsed());
        assert_eq!(observation.completion, JournaledCompletion::Committed);
        assert_eq!(
            observation.durability,
            expected_service_durability(durability)
        );
        assert!(matches!(
            observation.observation.outcome,
            BudgetOutcome::BudgetCreated { .. }
        ));
    }
    let elapsed = wall.elapsed();
    harness.stop();
    let ports = harness.database.open();
    scan_all_commits_with(&ports, command_count, |commit| {
        assert_eq!(
            commit.durability_mode(),
            expected_storage_durability(durability)
        );
    });
    (latencies, elapsed)
}

fn hot_key_row(config: &BenchmarkConfig, concurrency: usize) -> Value {
    for iteration in 0..config.warmup_iterations {
        let _ = measure_hot_key(concurrency, iteration as u64);
    }
    let mut measurement = Measurement::default();
    for iteration in 0..config.measured_iterations {
        let (latencies, elapsed) = measure_hot_key(concurrency, 10_000 + iteration as u64);
        for latency in latencies {
            measurement.record_latency(latency);
        }
        measurement.record_batch(elapsed, concurrency);
    }
    let (rate_total_work, rate_total_wall_ns, rate) = measurement.rate_evidence();
    let mut row = measurement.row(
        "hot_key_contention",
        &format!("hot-key-contention-{concurrency}-v1"),
    );
    row["rate_total_work"] = json!(rate_total_work);
    row["rate_total_wall_ns"] = json!(rate_total_wall_ns);
    row["ops_per_second"] = json!(rate);
    row["concurrency"] = json!(concurrency);
    row["durability_mode"] = json!("synchronous");
    row["correctness"] =
        json!("every caller committed exactly once and the final allocation equals caller count");
    row
}

fn measure_hot_key(concurrency: usize, iteration: u64) -> (Vec<Duration>, Duration) {
    let mut harness = BudgetServiceHarness::new();
    let key = budget_key(2_000_000 + iteration);
    let seed = create_for_key("hot-seed", 2_000_000 + iteration, key);
    let seeded = harness
        .adapter
        .execute_with_metadata(&BudgetOperation::Create(seed))
        .expect("hot-key seed");
    assert!(matches!(
        seeded.observation.outcome,
        BudgetOutcome::BudgetCreated { .. }
    ));

    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);
    for caller in 0..concurrency {
        let adapter = harness.adapter.clone();
        let barrier = Arc::clone(&barrier);
        let command = BudgetOperation::Allocate(allocate_for_key(
            "hot-allocate",
            iteration
                .checked_mul(256)
                .and_then(|base| base.checked_add(caller as u64))
                .expect("bounded hot-key ordinal"),
            key,
        ));
        workers.push(thread::spawn(move || {
            barrier.wait();
            let started = Instant::now();
            let result = adapter.execute_with_metadata(&command);
            (started.elapsed(), result)
        }));
    }
    let wall = Instant::now();
    barrier.wait();
    let mut latencies = Vec::with_capacity(concurrency);
    for worker in workers {
        let (latency, result) = worker.join().expect("hot-key worker did not panic");
        let observation = result.expect("hot-key command completed");
        assert_eq!(observation.completion, JournaledCompletion::Committed);
        assert!(matches!(
            observation.observation.outcome,
            BudgetOutcome::Allocated { .. }
        ));
        latencies.push(latency);
    }
    let elapsed = wall.elapsed();
    let final_budget = harness
        .adapter
        .read_budget(key)
        .expect("read final hot-key budget")
        .expect("hot-key budget exists");
    assert_eq!(
        final_budget.allocated_amount,
        Amount::from_minor_units(concurrency as i128).expect("bounded allocation total")
    );
    harness.stop();
    (latencies, elapsed)
}

fn replay_row(config: &BenchmarkConfig) -> Value {
    for _ in 0..config.warmup_iterations {
        let _ = measure_replays(config.commands_per_iteration);
    }
    let mut measurement = Measurement::default();
    for _ in 0..config.measured_iterations {
        let (latencies, elapsed) = measure_replays(config.commands_per_iteration);
        for latency in latencies {
            measurement.record_latency(latency);
        }
        measurement.record_batch(elapsed, config.commands_per_iteration);
    }
    let (rate_total_work, rate_total_wall_ns, rate) = measurement.rate_evidence();
    let mut row = measurement.row("idempotent_replay", "idempotent-replay-v1");
    row["rate_total_work"] = json!(rate_total_work);
    row["rate_total_wall_ns"] = json!(rate_total_wall_ns);
    row["ops_per_second"] = json!(rate);
    row["durability_mode"] = json!("synchronous");
    row["correctness"] =
        json!("every retry returned replayed=true with the original sequence and provenance");
    row
}

fn measure_replays(replay_count: usize) -> (Vec<Duration>, Duration) {
    let mut harness = BudgetServiceHarness::new();
    let operation = create_operation("replay", 3_000_000);
    let original = harness
        .adapter
        .execute_with_metadata(&operation)
        .expect("original replay command");
    assert_eq!(original.completion, JournaledCompletion::Committed);

    let wall = Instant::now();
    let mut latencies = Vec::with_capacity(replay_count);
    for _ in 0..replay_count {
        let started = Instant::now();
        let replayed = harness
            .adapter
            .execute_with_metadata(&operation)
            .expect("idempotent replay");
        latencies.push(started.elapsed());
        assert_eq!(replayed.completion, JournaledCompletion::Replayed);
        assert_eq!(replayed.commit_sequence, original.commit_sequence);
        assert_eq!(replayed.provenance_id, original.provenance_id);
        assert_eq!(replayed.observation, original.observation);
    }
    let elapsed = wall.elapsed();
    harness.stop();
    (latencies, elapsed)
}

fn commit_scan_row(config: &BenchmarkConfig) -> Value {
    let mut harness = populated_harness(config.scan_records, CoordinatorDurability::Sync);
    harness.stop();
    for _ in 0..config.warmup_iterations {
        let ports = harness.database.open();
        let _ = scan_all_commits(&ports, config.scan_records);
    }
    let mut measurement = Measurement::default();
    for _ in 0..config.measured_iterations {
        let ports = harness.database.open();
        let started = Instant::now();
        let count = scan_all_commits(&ports, config.scan_records);
        let elapsed = started.elapsed();
        measurement.record_latency(elapsed);
        measurement.record_batch(elapsed, count);
    }
    let (rate_total_work, rate_total_wall_ns, rate) = measurement.rate_evidence();
    let mut row = measurement.row("commit_log_scan", "commit-log-scan-v1");
    row["rate_total_work"] = json!(rate_total_work);
    row["rate_total_wall_ns"] = json!(rate_total_wall_ns);
    row["records_per_second"] = json!(rate);
    row["record_count"] = json!(config.scan_records);
    row["correctness"] =
        json!("the frozen scan returned every commit exactly once in contiguous sequence order");
    row
}

fn scan_all_commits(ports: &impl AuthoritativeScanReader, expected: usize) -> usize {
    scan_all_commits_with(ports, expected, |_| {})
}

fn scan_all_commits_with(
    ports: &impl AuthoritativeScanReader,
    expected: usize,
    mut inspect: impl FnMut(&riffdb_storage_api::StoredCommitRecordV1),
) -> usize {
    let limit = StorageScanLimit::new(64).expect("bounded scan limit");
    let mut request = CommitScanRequest::initial(limit);
    let mut count = 0_usize;
    let mut prior = None;
    loop {
        let page = ports.scan_commits(request).expect("scan commit log");
        for charged in page.records() {
            let commit = charged.value();
            inspect(commit);
            let sequence = commit.commit_sequence();
            let expected_sequence = prior.map_or(1, |prior: CommitSequence| prior.get() + 1);
            assert_eq!(sequence.get(), expected_sequence);
            prior = Some(sequence);
            count += 1;
        }
        match page {
            CommitScanPageV1::Page {
                next_after,
                inclusive_upper,
                ..
            } => {
                let FrontierPosition::AppliedThrough(upper) = inclusive_upper else {
                    panic!("nonempty scan has an authoritative head")
                };
                request = CommitScanRequest::continuing(next_after, upper, limit)
                    .expect("valid scan continuation");
            }
            CommitScanPageV1::ExactEnd {
                inclusive_upper, ..
            } => {
                assert_eq!(
                    inclusive_upper,
                    prior.map_or(
                        FrontierPosition::BeforeFirst,
                        FrontierPosition::AppliedThrough
                    )
                );
                break;
            }
        }
    }
    assert_eq!(count, expected);
    count
}

fn projection_catch_up_row(config: &BenchmarkConfig) -> Value {
    for _ in 0..config.warmup_iterations {
        let _ = measure_projection_catch_up(config.projection_records);
    }
    let mut measurement = Measurement::default();
    for _ in 0..config.measured_iterations {
        let (elapsed, commit_count) = measure_projection_catch_up(config.projection_records);
        measurement.record_latency(elapsed);
        measurement.record_batch(elapsed, commit_count);
    }
    let (rate_total_work, rate_total_wall_ns, rate) = measurement.rate_evidence();
    let mut row = measurement.row("projection_catch_up", "projection-catch-up-v1");
    row["rate_total_work"] = json!(rate_total_work);
    row["rate_total_wall_ns"] = json!(rate_total_wall_ns);
    row["records_per_second"] = json!(rate);
    row["source_event_count"] = json!(config.projection_records);
    row["commit_record_count"] = json!(config.projection_records * 2);
    row["correctness"] = json!(
        "the derived generation published only after its frontier reached the authoritative head"
    );
    row
}

fn measure_projection_catch_up(event_count: usize) -> (Duration, usize) {
    let mut harness = BudgetServiceHarness::new();
    for ordinal in 0..event_count {
        let key = budget_key(4_000_000 + ordinal as u64);
        let created = harness
            .adapter
            .execute_with_metadata(&BudgetOperation::Create(create_for_key(
                "projection-create",
                4_000_000 + ordinal as u64,
                key,
            )))
            .expect("projection seed command");
        assert!(matches!(
            created.observation.outcome,
            BudgetOutcome::BudgetCreated { .. }
        ));
        let allocated = harness
            .adapter
            .execute_with_metadata(&BudgetOperation::Allocate(allocate_for_key(
                "projection-allocate",
                4_000_000 + ordinal as u64,
                key,
            )))
            .expect("projection event command");
        assert!(matches!(
            allocated.observation.outcome,
            BudgetOutcome::Allocated { .. }
        ));
    }
    harness.stop();

    let ports = harness.database.open();
    let bundle = harness.database.active.bundle().bundle();
    let projection = bundle.projections().first().expect("LegalSpend projection");
    let schema = CheckedProjectionSchema::new(
        bundle
            .bound_projection_group_schema(projection.projection_id())
            .expect("bound LegalSpend projection schema"),
    );
    let resolved = harness
        .database
        .active
        .resolve_projection(schema.identity())
        .expect("resolve LegalSpend projection");
    let registry =
        ProjectionSchemaRegistry::new([schema.clone()]).expect("projection benchmark registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let mut controller = ProjectionController::new(ports, notifier);
    assert!(matches!(
        controller
            .initialize(schema.clone())
            .expect("initialize derived generation"),
        ProjectionInitializationResult::Initialized(_)
    ));
    assert!(matches!(
        controller
            .start_initial_catch_up(schema.identity())
            .expect("start projection catch-up"),
        ProjectionControlResult::Updated(_)
    ));

    let status = controller
        .repository()
        .read_projection_status(schema.identity())
        .expect("read catching-up projection");
    let generation = status
        .candidate()
        .map(ProjectionGenerationPosition::generation)
        .expect("catch-up candidate");
    assert_eq!(generation, ProjectionGeneration::first());

    let started = Instant::now();
    let (commit_count, changed_rows, upper) =
        apply_projection_log(&mut controller, &resolved, &schema, generation);
    assert_eq!(commit_count, event_count * 2);
    assert_eq!(changed_rows, event_count);
    assert!(matches!(
        controller
            .publish_candidate(schema.identity())
            .expect("publish caught-up generation"),
        ProjectionControlResult::Updated(_)
    ));
    let elapsed = started.elapsed();

    let final_status = controller
        .repository()
        .read_projection_status(schema.identity())
        .expect("read published projection");
    assert_eq!(final_status.lifecycle(), ProjectionLifecycleV1::Ready);
    assert_eq!(final_status.authoritative_head(), upper);
    assert_eq!(
        final_status
            .published()
            .map(ProjectionGenerationPosition::frontier),
        Some(upper)
    );
    (elapsed, commit_count)
}

fn apply_projection_log(
    controller: &mut ProjectionController<riffdb_storage_redb::RedbOperationalPorts>,
    resolved: &riffdb_catalog::ResolvedProjectionPlan,
    schema: &CheckedProjectionSchema,
    generation: ProjectionGeneration,
) -> (usize, usize, FrontierPosition) {
    let limit = StorageScanLimit::new(64).expect("bounded projection scan limit");
    let mut request = CommitScanRequest::initial(limit);
    let mut count = 0_usize;
    let mut changed_rows = 0_usize;
    loop {
        let page = controller
            .repository()
            .scan_commits(request)
            .expect("scan projection source commits");
        let upper = page.inclusive_upper();
        // Catch-up is measured through the batched path the projection worker
        // uses in production (`ProjectionBatchBuilder` + `apply_batch`). The
        // earlier per-record `evaluate_and_prepare_projection_commit` +
        // `apply` loop measured a path production does not take, so every
        // catch-up number it produced was scored against the wrong work.
        let mut position = 0_usize;
        while position < page.records().len() {
            let base = controller
                .repository()
                .capture_apply_batch_snapshot(schema.identity())
                .expect("capture projection apply snapshot");
            let mut batch = ProjectionBatchBuilder::new(base, schema.clone(), generation)
                .expect("bounded projection batch");
            while position < page.records().len() && !batch.is_full() {
                let evaluated = evaluate_projection_commit(
                    resolved,
                    schema.clone(),
                    generation,
                    page.records()[position].value(),
                )
                .expect("evaluate checked projection commit");
                if !batch.try_push(&evaluated).expect("push projection member") {
                    break;
                }
                changed_rows += evaluated.changed_group_count();
                position += 1;
                count += 1;
            }
            assert!(!batch.is_empty(), "every batch admits at least one member");
            let batch_request = batch.finish().expect("finish projection batch");
            assert!(matches!(
                controller
                    .apply_batch(&batch_request)
                    .expect("apply projection batch"),
                ProjectionApplyBatchResult::Applied(_)
                    | ProjectionApplyBatchResult::AlreadyApplied
            ));
        }
        match page {
            CommitScanPageV1::Page {
                next_after,
                inclusive_upper,
                ..
            } => {
                let FrontierPosition::AppliedThrough(upper) = inclusive_upper else {
                    panic!("projection source has a nonempty head")
                };
                request = CommitScanRequest::continuing(next_after, upper, limit)
                    .expect("valid projection scan continuation");
            }
            CommitScanPageV1::ExactEnd { .. } => return (count, changed_rows, upper),
        }
    }
}

fn restart_rows(config: &BenchmarkConfig) -> Vec<Value> {
    let mut rows = Vec::with_capacity(config.restart_record_counts.len());
    let mut prior_size = 0_u64;
    for (index, record_count) in config.restart_record_counts.iter().copied().enumerate() {
        let mut harness = populated_harness(record_count, CoordinatorDurability::Sync);
        harness.stop();
        let database_size = fs::metadata(&harness.database.path)
            .expect("stat benchmark database")
            .len();
        assert!(database_size > 0, "database size is positive");
        assert!(
            database_size >= prior_size,
            "restart benchmark database size regressed; size {database_size} is below {prior_size}"
        );
        prior_size = database_size;

        for _ in 0..config.warmup_iterations {
            drop(harness.database.open());
        }
        let mut measurement = Measurement::default();
        for _ in 0..config.measured_iterations {
            let started = Instant::now();
            let ports = harness.database.open();
            let elapsed = started.elapsed();
            drop(ports);
            measurement.record_latency(elapsed);
        }
        let mut row = measurement.row(
            "restart_recovery",
            &format!("restart-recovery-size-{}-v1", index + 1),
        );
        row["database_size_bytes"] = json!(database_size);
        row["authoritative_commit_count"] = json!(record_count);
        row["correctness"] =
            json!("complete structural and catalog validation reopened the exact durable history");
        rows.push(row);
    }
    rows
}

fn durability_row(config: &BenchmarkConfig, durability: CoordinatorDurability) -> Value {
    for _ in 0..config.warmup_iterations {
        let _ = measure_conflict_free(config.commands_per_iteration, durability);
    }
    let mut measurement = Measurement::default();
    for _ in 0..config.measured_iterations {
        let (latencies, elapsed) = measure_conflict_free(config.commands_per_iteration, durability);
        for latency in latencies {
            measurement.record_latency(latency);
        }
        measurement.record_batch(elapsed, config.commands_per_iteration);
    }
    let (rate_total_work, rate_total_wall_ns, rate) = measurement.rate_evidence();
    let mode = match durability {
        CoordinatorDurability::Sync => "synchronous",
        CoordinatorDurability::Group => "group_commit",
    };
    let mut row = measurement.row("durability_mode", &format!("durability-{mode}-v1"));
    row["rate_total_work"] = json!(rate_total_work);
    row["rate_total_wall_ns"] = json!(rate_total_wall_ns);
    row["ops_per_second"] = json!(rate);
    row["durability_mode"] = json!(mode);
    row["commands_per_iteration"] = json!(config.commands_per_iteration);
    row["correctness"] =
        json!("every outcome and durable commit record retained the selected closed durability");
    if durability == CoordinatorDurability::Group {
        row["scope"] = json!(
            "current single-command group durability semantics; no batching or scheduling claim"
        );
    }
    row
}

fn populated_harness(
    command_count: usize,
    durability: CoordinatorDurability,
) -> BudgetServiceHarness {
    let harness = BudgetServiceHarness::with_durability(durability);
    for ordinal in 0..command_count {
        let observation = harness
            .adapter
            .execute_with_metadata(&create_operation("populate", 5_000_000 + ordinal as u64))
            .expect("populate benchmark database");
        assert_eq!(observation.completion, JournaledCompletion::Committed);
        assert!(matches!(
            observation.observation.outcome,
            BudgetOutcome::BudgetCreated { .. }
        ));
    }
    harness
}

fn create_operation(prefix: &str, ordinal: u64) -> BudgetOperation {
    BudgetOperation::Create(create_for_key(prefix, ordinal, budget_key(ordinal)))
}

fn create_for_key(prefix: &str, ordinal: u64, key: BudgetKey) -> CreateBudget {
    CreateBudget {
        operation_id: operation_id(prefix, ordinal),
        idempotency_key: idempotency_key(prefix, "create", ordinal),
        key,
        approved_amount: Amount::from_minor_units(1_000_000_000)
            .expect("benchmark approval fits decimal<28,2>"),
    }
}

fn allocate_for_key(prefix: &str, ordinal: u64, key: BudgetKey) -> AllocateBudget {
    AllocateBudget {
        operation_id: operation_id(prefix, ordinal),
        idempotency_key: idempotency_key(prefix, "allocate", ordinal),
        key,
        matter_id: MatterId::from_bytes(uuid_bytes_from_ordinal(0x31, ordinal)),
        amount: Amount::MINIMUM_POSITIVE,
    }
}

fn budget_key(ordinal: u64) -> BudgetKey {
    BudgetKey {
        organization_id: OrganizationId::from_bytes(uuid_bytes_from_ordinal(0x21, ordinal)),
        fiscal_year: 2026,
    }
}

fn operation_id(prefix: &str, ordinal: u64) -> OperationId {
    OperationId::new(format!("{prefix}-{ordinal}")).expect("bounded benchmark operation ID")
}

fn idempotency_key(prefix: &str, operation: &str, ordinal: u64) -> WorkloadIdempotencyKey {
    WorkloadIdempotencyKey::new(format!("poc-{prefix}-{operation}-{ordinal}"))
        .expect("bounded benchmark idempotency key")
}

const fn expected_service_durability(durability: CoordinatorDurability) -> CommandDurability {
    match durability {
        CoordinatorDurability::Sync => CommandDurability::Synchronous,
        CoordinatorDurability::Group => CommandDurability::Group,
    }
}

const fn expected_storage_durability(durability: CoordinatorDurability) -> DurabilityMode {
    match durability {
        CoordinatorDurability::Sync => DurabilityMode::Sync,
        CoordinatorDurability::Group => DurabilityMode::Group,
    }
}

fn percentiles(samples: &[u64]) -> (u64, u64, u64) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    (
        nearest_rank(&sorted, 50),
        nearest_rank(&sorted, 95),
        nearest_rank(&sorted, 99),
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

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("one benchmark sample fits u64 nanoseconds")
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

fn parse_bool_environment(name: &str) -> bool {
    match required_environment(name).as_str() {
        "true" => true,
        "false" => false,
        value => panic!("{name} must be true or false, received {value:?}"),
    }
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
    fn percentile_contract_uses_nearest_rank_without_reordering_source() {
        let samples = vec![50, 10, 40, 20, 30];
        assert_eq!(percentiles(&samples), (30, 50, 50));
        assert_eq!(samples, [50, 10, 40, 20, 30]);
    }

    #[test]
    fn rate_evidence_freezes_work_elapsed_and_flooring() {
        let mut measurement = Measurement::default();
        measurement.record_batch(Duration::from_nanos(3_000_000_001), 9);
        assert_eq!(measurement.rate_evidence(), (9, 3_000_000_001, 2));

        let mut subunit = Measurement::default();
        subunit.record_batch(Duration::from_secs(2), 1);
        assert_eq!(subunit.rate_evidence(), (1, 2_000_000_000, 1));
    }

    #[test]
    fn stable_workload_inventory_is_complete() {
        let families = [
            "conflict_free_command",
            "hot_key_contention",
            "idempotent_replay",
            "commit_log_scan",
            "projection_catch_up",
            "restart_recovery",
            "durability_mode",
        ];
        assert_eq!(families.len(), 7);
        assert_eq!([1_usize, 8, 32, 128], [1, 8, 32, 128]);
        assert_eq!(["synchronous", "group_commit"].len(), 2);
    }
}
