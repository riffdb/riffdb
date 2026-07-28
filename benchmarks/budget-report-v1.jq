def pair:
  capture("^(?<key>[^=]+)=(?<value>.*)$");

def nearest_rank($samples; $percent):
  ($samples | length) as $length
  | (((($length - 1) * $percent) / 100) | ceil) as $index
  | $samples[$index];

def summarize($id; $samples; $database_sizes; $description; $guarantees):
  ($samples | sort) as $ordered
  | ($database_sizes | sort) as $ordered_database_sizes
  | {
      workload_id: $id,
      workload: $description,
      guarantee_profile: $guarantees,
      unit: "nanoseconds",
      sample_count: ($ordered | length),
      samples_ns: $ordered,
      database_size_bytes: $ordered_database_sizes[-1],
      database_size_samples_bytes: $ordered_database_sizes,
      min_ns: $ordered[0],
      p50_ns: nearest_rank($ordered; 50),
      p95_ns: nearest_rank($ordered; 95),
      p99_ns: nearest_rank($ordered; 99),
      max_ns: $ordered[-1],
      mean_ns: (($ordered | add) / ($ordered | length) | floor)
    };

(reduce inputs as $line
  ({metadata: {}, samples: {}, database_sizes: {}};
   if ($line | startswith("sample\t")) then
     ($line | split("\t")) as $columns
     | .samples[$columns[1]] =
         ((.samples[$columns[1]] // []) + [($columns[3] | tonumber)])
     | .database_sizes[$columns[1]] =
         ((.database_sizes[$columns[1]] // []) + [($columns[4] | tonumber)])
   else
     ($line | pair) as $pair
     | .metadata[$pair.key] = $pair.value
   end)) as $capture
| {
    schema: "riffdb.benchmark-report/v1",
    report_id: "budget-postgresql-riffdb",
    source_revision: $capture.metadata.git_revision,
    dirty: ($capture.metadata.git_dirty == "true"),
    rust_version: $capture.metadata.rust_version,
    target: $capture.metadata.rust_target,
    environment: {
      cpu: $capture.metadata.cpu,
      memory: $capture.metadata.memory,
      storage_medium: $capture.metadata.storage_medium,
      operating_system: $capture.metadata.operating_system,
      kernel: $capture.metadata.operating_system,
      filesystem: $capture.metadata.filesystem,
      mount_options: $capture.metadata.mount_options,
      database_root: $capture.metadata.database_root,
      postgres_server: {
        image: $capture.metadata.postgres_image,
        image_id: $capture.metadata.postgres_image_id,
        server_version_num: $capture.metadata.postgres_server_version_num,
        synchronous_commit: $capture.metadata.postgres_synchronous_commit,
        fsync: $capture.metadata.postgres_fsync,
        full_page_writes: $capture.metadata.postgres_full_page_writes,
        network: $capture.metadata.postgres_network,
        filesystem: $capture.metadata.postgres_filesystem,
        mount_options: $capture.metadata.postgres_mount_options
      }
    },
    methodology: {
      build_profile: "test executables prebuilt once; timed child invocations only",
      features: [],
      database_size: "actual PostgreSQL logical database bytes and peak riffdb.redb file bytes for every timed sample",
      contract_version: "LegalSpend v1",
      workload_distribution: "complete canonical sequential and contention workloads; RiffDB also executes the required same-key replay case",
      durability_modes: [
        "PostgreSQL synchronous_commit=on, fsync=on, full_page_writes=on",
        "RiffDB synchronous"
      ],
      warmup_runs: ($capture.metadata.warmup_runs | tonumber),
      sample_count: (
        $capture.samples
        | to_entries
        | map(.value | length)
        | add
      ),
      distribution_method: "complete repeated-run wall-clock nanosecond distributions with nearest-rank p50/p95/p99",
      analysis_command: $analysis_command
    },
    correctness_status: (
      if $capture.metadata.correctness_preflight == "passed"
      then "passed"
      else "failed"
      end
    ),
    performance_status: "measured",
    raw_artifact: $raw_path,
    raw_sha256: $raw_hash,
    unsafe_postgresql_variants_included: false,
    comparison_qualification: {
      matched_semantics: [
        "typed canonical LegalSpend workload outcomes",
        "atomic exact-decimal row/entity mutation",
        "same-budget conflict exclusion",
        "synchronous acknowledged durability"
      ],
      stronger_riffdb_guarantees: [
        "compiled command-only mutation",
        "idempotent uncertainty recovery",
        "durable events and provenance",
        "projection frontier",
        "shared authorization"
      ],
      timing_scope: "suite-level process distributions are not per-command TPS and the RiffDB suite intentionally includes the stronger replay proof"
    },
    results: [
      summarize(
        "postgres_canonical_sequential_contention";
        $capture.samples.postgres_canonical_sequential_contention;
        $capture.database_sizes.postgres_canonical_sequential_contention;
        "canonical PostgreSQL sequential plus two-caller contention correctness suite";
        "examples/budget-comparison/fixtures/postgres-guarantees-v1.json"
      ),
      summarize(
        "riffdb_public_sequential_contention_replay";
        $capture.samples.riffdb_public_sequential_contention_replay;
        $capture.database_sizes.riffdb_public_sequential_contention_replay;
        "canonical public Rust SDK/gRPC RiffDB sequential, contention, and same-key replay suite";
        "compiled LegalSpend v1 plus public RiffDB guarantees"
      )
    ]
  }
