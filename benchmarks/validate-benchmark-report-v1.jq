def positive_integer:
  type == "number" and . > 0 and . == floor;

def nonnegative_integer:
  type == "number" and . >= 0 and . == floor;

def nonempty_bounded($maximum):
  type == "string" and length > 0 and length <= $maximum;

def exact_keys($expected):
  (keys | sort) == ($expected | sort);

def nearest_rank($samples; $percent):
  (((($samples | length) - 1) * $percent / 100) | ceil) as $index
  | $samples[$index];

def distribution:
  . as $row
  | ($row.samples_ns | type == "array" and length > 0)
  and all($row.samples_ns[]; positive_integer)
  and ($row.samples_ns == ($row.samples_ns | sort))
  and ($row.sample_count == ($row.samples_ns | length))
  and ($row.min_ns == $row.samples_ns[0])
  and ($row.p50_ns == nearest_rank($row.samples_ns; 50))
  and ($row.p95_ns == nearest_rank($row.samples_ns; 95))
  and ($row.p99_ns == nearest_rank($row.samples_ns; 99))
  and ($row.max_ns == $row.samples_ns[-1])
  and ($row.mean_ns == (($row.samples_ns | add) / $row.sample_count | floor));

def base_environment:
  exact_keys([
    "cpu",
    "database_root",
    "filesystem",
    "kernel",
    "memory",
    "mount_options",
    "operating_system",
    "storage_medium"
  ])
  and all(.[]; nonempty_bounded(4096));

def methodology:
  exact_keys([
    "analysis_command",
    "build_profile",
    "contract_version",
    "database_size",
    "distribution_method",
    "durability_modes",
    "features",
    "sample_count",
    "warmup_runs",
    "workload_distribution"
  ])
  and (.analysis_command | nonempty_bounded(4096))
  and (.build_profile | nonempty_bounded(512))
  and (.contract_version | nonempty_bounded(512))
  and (.database_size | nonempty_bounded(512))
  and (.distribution_method | nonempty_bounded(1024))
  and (.durability_modes
       | type == "array" and length > 0 and length <= 16
         and all(.[]; nonempty_bounded(512)))
  and (.features
       | type == "array" and length <= 64
         and all(.[]; nonempty_bounded(512)))
  and (.sample_count | nonnegative_integer)
  and (.warmup_runs | nonnegative_integer)
  and (.workload_distribution | nonempty_bounded(1024));

def common:
  .schema == "riffdb.benchmark-report/v1"
  and (.source_revision | test("^[0-9a-f]{40,64}$"))
  and .dirty == false
  and (.rust_version | nonempty_bounded(4096))
  and (.target | nonempty_bounded(512))
  and (.methodology | methodology)
  and .correctness_status == "passed"
  and .performance_status == "measured"
  and (.raw_artifact | nonempty_bounded(512))
  and (.raw_sha256 | test("^[0-9a-f]{64}$"))
  and .unsafe_postgresql_variants_included == false
  and (.results | type == "array")
  and (.methodology.sample_count
       == ([.results[].sample_count] | add));

def redb_result:
  exact_keys([
    "database_size_bytes",
    "durability_mode",
    "max_ns",
    "mean_ns",
    "min_ns",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "sample_count",
    "samples_ns",
    "unit",
    "workload",
    "workload_distribution",
    "workload_id"
  ])
  and (.workload_id | nonempty_bounded(96))
  and (.workload | nonempty_bounded(1024))
  and .workload_distribution
      == "deterministic single-operation component samples"
  and .durability_mode == "redb immediate, two-phase commit"
  and (.database_size_bytes | positive_integer)
  and .unit == "nanoseconds"
  and distribution;

def budget_result:
  . as $row
  | exact_keys([
    "database_size_bytes",
    "database_size_samples_bytes",
    "guarantee_profile",
    "max_ns",
    "mean_ns",
    "min_ns",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "sample_count",
    "samples_ns",
    "unit",
    "workload",
    "workload_id"
  ])
  and (.workload_id | nonempty_bounded(96))
  and (.workload | nonempty_bounded(1024))
  and (.guarantee_profile | nonempty_bounded(1024))
  and (.database_size_bytes | positive_integer)
  and (
    .database_size_samples_bytes
    | type == "array" and length == $row.sample_count
      and all(.[]; positive_integer)
  )
  and (.database_size_samples_bytes
       == (.database_size_samples_bytes | sort))
  and $row.database_size_bytes == $row.database_size_samples_bytes[-1]
  and .unit == "nanoseconds"
  and .sample_count >= 3
  and distribution;

if $report_id == "storage-redb" then
  exact_keys([
    "correctness_status",
    "dirty",
    "environment",
    "methodology",
    "performance_status",
    "raw_artifact",
    "raw_sha256",
    "report_id",
    "results",
    "rust_version",
    "schema",
    "source_revision",
    "target",
    "unsafe_postgresql_variants_included"
  ])
  and common
  and .report_id == $report_id
  and (.environment | base_environment)
  and .methodology.build_profile == "bench"
  and .methodology.features == ["default (empty)"]
  and .methodology.database_size == "reported per workload in bytes"
  and .methodology.contract_version
      == "none (storage-only WP-070 fixture)"
  and .methodology.workload_distribution
      == "deterministic single-operation component samples"
  and .methodology.durability_modes
      == ["redb immediate, two-phase commit"]
  and (.results | length == 4)
  and ([.results[].workload_id] == [
    "initialized_open_probe",
    "full_startup_evidence",
    "populated_history_full_startup",
    "durable_capability_bootstrap"
  ])
  and all(.results[]; redb_result)
elif $report_id == "budget-postgresql-riffdb" then
  exact_keys([
    "comparison_qualification",
    "correctness_status",
    "dirty",
    "environment",
    "methodology",
    "performance_status",
    "raw_artifact",
    "raw_sha256",
    "report_id",
    "results",
    "rust_version",
    "schema",
    "source_revision",
    "target",
    "unsafe_postgresql_variants_included"
  ])
  and common
  and .report_id == $report_id
  and (
    .environment | exact_keys([
      "cpu",
      "database_root",
      "filesystem",
      "kernel",
      "memory",
      "mount_options",
      "operating_system",
      "postgres_server",
      "storage_medium"
    ])
  )
  and all(
    .environment
    | to_entries[]
    | select(.key != "postgres_server")
    | .value;
    nonempty_bounded(4096)
  )
  and (
    .environment.postgres_server | exact_keys([
      "filesystem",
      "fsync",
      "full_page_writes",
      "image",
      "image_id",
      "mount_options",
      "network",
      "server_version_num",
      "synchronous_commit"
    ])
  )
  and all(.environment.postgres_server[]; nonempty_bounded(4096))
  and .environment.postgres_server.image
      == "postgres:18.4-bookworm@sha256:d9c83446333daec3f0588cc709adb80c26090b7f9f0f7ec8d43c243385d79818"
  and (.environment.postgres_server.image_id
       | test("^sha256:[0-9a-f]{64}$"))
  and .environment.postgres_server.server_version_num == "180004"
  and .environment.postgres_server.synchronous_commit == "on"
  and .environment.postgres_server.fsync == "on"
  and .environment.postgres_server.full_page_writes == "on"
  and .environment.postgres_server.network
      == "loopback-only ephemeral Docker publication"
  and .methodology.build_profile
      == "test executables prebuilt once; timed child invocations only"
  and .methodology.features == []
  and .methodology.database_size
      == "actual PostgreSQL logical database bytes and peak riffdb.redb file bytes for every timed sample"
  and .methodology.contract_version == "LegalSpend v1"
  and .methodology.workload_distribution
      == "complete canonical sequential and contention workloads; RiffDB also executes the required same-key replay case"
  and .methodology.durability_modes[0]
      == "PostgreSQL synchronous_commit=on, fsync=on, full_page_writes=on"
  and (.methodology.durability_modes[1] ==
         "RiffDB sync (exact response/commit identity checked)"
       or .methodology.durability_modes[1] ==
         "RiffDB group (exact response/commit identity checked)")
  and (.results | length == 2)
  and ([.results[].workload_id] == [
    "postgres_canonical_sequential_contention",
    "riffdb_public_sequential_contention_replay"
  ])
  and (.results[0].guarantee_profile
       == "examples/budget-comparison/fixtures/postgres-guarantees-v1.json")
  and (.results[1].guarantee_profile
       == "compiled LegalSpend v1 plus public RiffDB guarantees")
  and all(.results[]; budget_result)
  and (.results[0].sample_count == .results[1].sample_count)
  and (.comparison_qualification | exact_keys([
    "matched_semantics",
    "stronger_riffdb_guarantees",
    "timing_scope"
  ]))
  and .comparison_qualification.matched_semantics[0:3] == [
    "typed canonical LegalSpend workload outcomes",
    "atomic exact-decimal row/entity mutation",
    "same-budget conflict exclusion"
  ]
  and (.comparison_qualification.matched_semantics[3] ==
         "server-acknowledged durability (PostgreSQL synchronous; RiffDB sync)"
       or .comparison_qualification.matched_semantics[3] ==
         "server-acknowledged durability (PostgreSQL synchronous; RiffDB group)")
  and ((.methodology.durability_modes[1] | startswith("RiffDB sync "))
       == (.comparison_qualification.matched_semantics[3] | endswith("RiffDB sync)")))
  and .comparison_qualification.stronger_riffdb_guarantees == [
    "compiled command-only mutation",
    "idempotent uncertainty recovery",
    "durable events and provenance",
    "projection frontier",
    "shared authorization"
  ]
  and .comparison_qualification.timing_scope
      == "suite-level process distributions are not per-command TPS and the RiffDB suite intentionally includes the stronger replay proof"
else
  false
end
