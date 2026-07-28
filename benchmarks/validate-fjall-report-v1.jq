($metadata[0]) as $raw_metadata
|

def nonempty_bounded($maximum):
  type == "string" and length > 0 and length <= $maximum;

def exact_keys($expected):
  (keys | sort) == ($expected | sort);

exact_keys([
  "correctness_status",
  "dirty",
  "environment",
  "failure_classification",
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
and .schema == "riffdb.benchmark-report/v1"
and .report_id == "storage-fjall"
and .source_revision == $raw_metadata.git_revision
and .dirty == false
and $raw_metadata.git_dirty == "false"
and .rust_version == $raw_metadata.rust_version
and .target == $raw_metadata.rust_target
and (
  .environment | exact_keys([
    "cpu",
    "filesystem",
    "kernel",
    "memory",
    "mount_options",
    "operating_system",
    "storage_medium"
  ])
)
and .environment.cpu == $raw_metadata.cpu
and .environment.memory == $raw_metadata.memory
and .environment.storage_medium == $raw_metadata.storage_medium
and .environment.operating_system == $raw_metadata.operating_system
and .environment.kernel == $raw_metadata.operating_system
and .environment.filesystem == $raw_metadata.filesystem
and .environment.mount_options == $raw_metadata.mount_options
and all(.environment[]; nonempty_bounded(4096))
and (
  .methodology | exact_keys([
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
)
and .methodology.build_profile
    == "test and benchmark compile only; no timed workload"
and .methodology.features == ["fjall 3.1.8 default-features=false"]
and .methodology.database_size
    == "not measured because semantic conformance failed"
and .methodology.contract_version
    == "accepted V2 registry: 27 readable, 26 writable"
and .methodology.workload_distribution
    == "unchanged semantic, projection, migration, and crash suite"
and .methodology.durability_modes == ["not measured"]
and .methodology.warmup_runs == 0
and .methodology.sample_count == 0
and .methodology.distribution_method
    == "no performance distribution after failed correctness preflight"
and (
  (.methodology.analysis_command | try fromjson catch null)
  == [
    "benchmarks/run-storage-fjall",
    "--storage-medium",
    $raw_metadata.storage_medium
  ]
)
and .correctness_status == "conformance_failure"
and $raw_metadata.correctness_status == "conformance_failure"
and .performance_status == "not_run_due_to_conformance_failure"
and $raw_metadata.performance_status == "not_run_due_to_conformance_failure"
and .failure_classification
    == "Fjall adapter lacks the unchanged semantic, projection, migration, and crash-recovery ports; substrate timings are excluded."
and .raw_artifact == $raw_path
and .raw_sha256 == $raw_hash
and .unsafe_postgresql_variants_included == false
and $raw_metadata.unsafe_postgresql_variants_included == "false"
and .results == []
