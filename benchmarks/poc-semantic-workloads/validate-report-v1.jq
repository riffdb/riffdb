def positive_integer:
  type == "number" and . > 0 and . == floor;

def nonnegative_integer:
  type == "number" and . >= 0 and . == floor;

def exact_keys($expected):
  (keys | sort) == ($expected | sort);

def nearest_rank($samples; $percent):
  (((($samples | length) * $percent / 100) | ceil) - 1) as $index
  | $samples[$index];

def distribution:
  . as $row
  | ($row.sample_count | positive_integer)
  and ($row.samples_ns | type == "array" and length == $row.sample_count)
  and all($row.samples_ns[]; positive_integer)
  and ($row.samples_ns == ($row.samples_ns | sort))
  and ($row.p50_ns == nearest_rank($row.samples_ns; 50))
  and ($row.p95_ns == nearest_rank($row.samples_ns; 95))
  and ($row.p99_ns == nearest_rank($row.samples_ns; 99))
  and ($row.correctness | type == "string" and length > 0 and length <= 512);

def base_row:
  (.family | type == "string" and length > 0 and length <= 64)
  and (.workload_id | type == "string" and length > 0 and length <= 96)
  and distribution;

def command_row($id; $concurrency):
  exact_keys([
    "commands_per_iteration",
    "concurrency",
    "correctness",
    "durability_mode",
    "family",
    "ops_per_second",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "sample_count",
    "samples_ns",
    "workload_id"
  ])
  and base_row
  and .family == "conflict_free_command"
  and .workload_id == $id
  and .concurrency == $concurrency
  and .durability_mode == "synchronous"
  and (.commands_per_iteration | positive_integer)
  and (.ops_per_second | positive_integer);

def hot_key_row($id; $concurrency):
  exact_keys([
    "concurrency",
    "correctness",
    "durability_mode",
    "family",
    "ops_per_second",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "sample_count",
    "samples_ns",
    "workload_id"
  ])
  and base_row
  and .family == "hot_key_contention"
  and .workload_id == $id
  and .concurrency == $concurrency
  and .durability_mode == "synchronous"
  and (.ops_per_second | positive_integer);

def replay_row:
  exact_keys([
    "correctness",
    "durability_mode",
    "family",
    "ops_per_second",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "sample_count",
    "samples_ns",
    "workload_id"
  ])
  and base_row
  and .family == "idempotent_replay"
  and .workload_id == "idempotent-replay-v1"
  and .durability_mode == "synchronous"
  and (.ops_per_second | positive_integer);

def scan_row:
  exact_keys([
    "correctness",
    "family",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "record_count",
    "records_per_second",
    "sample_count",
    "samples_ns",
    "workload_id"
  ])
  and base_row
  and .family == "commit_log_scan"
  and .workload_id == "commit-log-scan-v1"
  and (.record_count | positive_integer)
  and (.records_per_second | positive_integer);

def projection_row:
  exact_keys([
    "commit_record_count",
    "correctness",
    "family",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "records_per_second",
    "sample_count",
    "samples_ns",
    "source_event_count",
    "workload_id"
  ])
  and base_row
  and .family == "projection_catch_up"
  and .workload_id == "projection-catch-up-v1"
  and (.source_event_count | positive_integer)
  and (.commit_record_count | positive_integer)
  and .commit_record_count == (.source_event_count * 2)
  and (.records_per_second | positive_integer);

def restart_row($id):
  exact_keys([
    "authoritative_commit_count",
    "correctness",
    "database_size_bytes",
    "family",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "sample_count",
    "samples_ns",
    "workload_id"
  ])
  and base_row
  and .family == "restart_recovery"
  and .workload_id == $id
  and (.authoritative_commit_count | positive_integer)
  and (.database_size_bytes | positive_integer);

def durability_row($id; $mode; $has_scope):
  exact_keys(
    [
      "commands_per_iteration",
      "correctness",
      "durability_mode",
      "family",
      "ops_per_second",
      "p50_ns",
      "p95_ns",
      "p99_ns",
      "sample_count",
      "samples_ns",
      "workload_id"
    ] + (if $has_scope then ["scope"] else [] end)
  )
  and base_row
  and .family == "durability_mode"
  and .workload_id == $id
  and .durability_mode == $mode
  and (.commands_per_iteration | positive_integer)
  and (.ops_per_second | positive_integer)
  and (
    if $has_scope
    then .scope
         == "current single-command group durability semantics; no batching or scheduling claim"
    else true
    end
  );

. as $report
| exact_keys([
  "configuration",
  "environment",
  "limitations",
  "publication_status",
  "report_id",
  "schema",
  "source",
  "workloads"
])
and .schema == "riffdb.poc-semantic-benchmark-report/v1"
and .report_id == "poc-semantic-workloads"
and (
  .publication_status == "smoke_after_correctness_preflight"
  or .publication_status == "measured_after_correctness_preflight"
)
and (
  .source | exact_keys([
    "dirty",
    "raw_artifact",
    "raw_sha256",
    "revision",
    "rust_version",
    "target"
  ])
)
and (.source.revision | test("^[0-9a-f]{40,64}$"))
and (.source.dirty | type == "boolean")
and (.source.rust_version | type == "string" and length > 0 and length <= 4096)
and (.source.target | type == "string" and length > 0 and length <= 512)
and (.source.raw_artifact | type == "string" and length > 0 and length <= 512)
and (.source.raw_sha256 | test("^[0-9a-f]{64}$"))
and (
  .environment | exact_keys([
    "cpu",
    "database_root",
    "filesystem",
    "kernel",
    "memory",
    "mount_options",
    "operating_system",
    "storage_medium"
  ])
)
and all(.environment[]; type == "string" and length > 0 and length <= 4096)
and (
  .configuration | exact_keys([
    "analysis_command",
    "build_profile",
    "commands_per_iteration",
    "contract_version",
    "distribution_method",
    "durability_modes",
    "features",
    "measured_iterations",
    "mode",
    "projection_records",
    "restart_commit_counts",
    "sample_count",
    "scan_records",
    "warmup_runs",
    "workload_distribution"
  ])
)
and .configuration.build_profile == "release"
and .configuration.features == []
and .configuration.contract_version == "LegalSpend/1"
and .configuration.workload_distribution
    == "deterministic generated LegalSpend commands"
and .configuration.durability_modes == ["group_commit", "synchronous"]
and (.configuration.warmup_runs | nonnegative_integer)
and (.configuration.sample_count | positive_integer)
and .configuration.distribution_method
    == "raw samples with nearest-rank p50/p95/p99"
and (
  (.configuration.analysis_command | try fromjson catch null)
  == [
    "benchmarks/poc-semantic-workloads/run",
    (if .configuration.mode == "full" then "--publish" else "--smoke" end),
    "--storage-medium",
    .environment.storage_medium
  ]
)
and (.configuration.measured_iterations | positive_integer)
and (.configuration.commands_per_iteration | positive_integer)
and (.configuration.scan_records | positive_integer)
and (.configuration.projection_records | positive_integer)
and (
  .configuration.restart_commit_counts
  | type == "array" and length == 3 and all(.[]; positive_integer)
)
and (.configuration.restart_commit_counts
     == (.configuration.restart_commit_counts | sort))
and (.configuration.restart_commit_counts | unique | length == 3)
and (.limitations | type == "array" and length == 4)
and all(.limitations[]; type == "string" and length > 0 and length <= 256)
and (.workloads | type == "array" and length == 13)
and (.workloads[0] | command_row("conflict-free-command-v1"; 1))
and (.workloads[1] | hot_key_row("hot-key-contention-1-v1"; 1))
and (.workloads[2] | hot_key_row("hot-key-contention-8-v1"; 8))
and (.workloads[3] | hot_key_row("hot-key-contention-32-v1"; 32))
and (.workloads[4] | hot_key_row("hot-key-contention-128-v1"; 128))
and (.workloads[5] | replay_row)
and (.workloads[6] | scan_row)
and (.workloads[7] | projection_row)
and (.workloads[8] | restart_row("restart-recovery-size-1-v1"))
and (.workloads[9] | restart_row("restart-recovery-size-2-v1"))
and (.workloads[10] | restart_row("restart-recovery-size-3-v1"))
and (.workloads[11]
     | durability_row("durability-synchronous-v1"; "synchronous"; false))
and (.workloads[12]
     | durability_row("durability-group_commit-v1"; "group_commit"; true))
and (.workloads[0].commands_per_iteration
     == .configuration.commands_per_iteration)
and (.workloads[11].commands_per_iteration
     == .configuration.commands_per_iteration)
and (.workloads[12].commands_per_iteration
     == .configuration.commands_per_iteration)
and (.workloads[0].sample_count
     == (.configuration.measured_iterations
         * .configuration.commands_per_iteration))
and (.workloads[1].sample_count == .configuration.measured_iterations)
and (.workloads[2].sample_count
     == (.configuration.measured_iterations * 8))
and (.workloads[3].sample_count
     == (.configuration.measured_iterations * 32))
and (.workloads[4].sample_count
     == (.configuration.measured_iterations * 128))
and (.workloads[5].sample_count
     == (.configuration.measured_iterations
         * .configuration.commands_per_iteration))
and all(.workloads[6:11][];
        .sample_count == $report.configuration.measured_iterations)
and (.workloads[11].sample_count
     == (.configuration.measured_iterations
         * .configuration.commands_per_iteration))
and (.workloads[12].sample_count
     == (.configuration.measured_iterations
         * .configuration.commands_per_iteration))
and .workloads[6].record_count == .configuration.scan_records
and .workloads[7].source_event_count == .configuration.projection_records
and ([.workloads[8:11][].authoritative_commit_count]
     == .configuration.restart_commit_counts)
and ([.workloads[8:11][].database_size_bytes]
     == ([.workloads[8:11][].database_size_bytes] | sort))
and ([.workloads[8:11][].database_size_bytes] | unique | length == 3)
and (.configuration.sample_count
     == ([.workloads[].sample_count] | add))
and (
  if .publication_status == "measured_after_correctness_preflight"
  then .source.dirty == false
    and .configuration.mode == "full"
    and .configuration.measured_iterations >= 3
  else .configuration.mode == "smoke"
    and .configuration.measured_iterations == 1
  end
)
