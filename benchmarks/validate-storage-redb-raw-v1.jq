def positive_decimal:
  test("^[1-9][0-9]{0,19}$");

def exact_keys($pairs; $expected):
  [$pairs[].key] == $expected;

def valid_result($pairs; $expected_id):
  exact_keys($pairs; [
    "benchmark",
    "workload",
    "workload_distribution",
    "contract_version",
    "durability_mode",
    "database_size_bytes",
    "sample_count",
    "sample_ns",
    "min_ns",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "max_ns",
    "mean_ns"
  ])
  and $pairs[0].value == $expected_id
  and ($pairs[1].value | length > 0 and length <= 1024)
  and $pairs[2].value
      == "deterministic single-operation component samples"
  and $pairs[3].value == "none (storage-only WP-070 fixture)"
  and $pairs[4].value == "redb immediate, two-phase commit"
  and ($pairs[5].value | positive_decimal)
  and ($pairs[6].value | positive_decimal)
  and ($pairs[7].value | fromjson | type == "array")
  and ($pairs[8].value | positive_decimal)
  and ($pairs[9].value | positive_decimal)
  and ($pairs[10].value | positive_decimal)
  and ($pairs[11].value | positive_decimal)
  and ($pairs[12].value | positive_decimal)
  and ($pairs[13].value | positive_decimal);

(split("\n") | if .[-1] == "" then .[:-1] else . end) as $lines
| ($lines | map(capture("^(?<key>[^=]+)=(?<value>.*)$"))) as $pairs
| ($pairs[0:20]) as $metadata
| ($pairs[20:34]) as $initialized
| ($pairs[34:48]) as $startup
| ($pairs[48:62]) as $populated
| ($pairs[62:76]) as $bootstrap
| ($lines | length == 76)
and exact_keys($metadata; [
  "riffdb_benchmark_capture_format",
  "database_root",
  "mount_options",
  "riffdb_storage_benchmark_format",
  "git_revision",
  "git_dirty",
  "rust_version",
  "rust_target",
  "build_profile",
  "enabled_features",
  "operating_system",
  "cpu",
  "memory",
  "storage_medium",
  "filesystem",
  "correctness_preflight",
  "iterations",
  "warmup_iterations",
  "populated_history_records",
  "populated_history_total_audit_records"
])
and $metadata[0].value == "1"
and ($metadata[1].value | length > 0 and length <= 4096)
and ($metadata[2].value | length > 0 and length <= 4096)
and $metadata[3].value == "1"
and ($metadata[4].value | test("^[0-9a-f]{40,64}$"))
and $metadata[5].value == "false"
and all($metadata[6:15][]; .value | length > 0 and length <= 4096)
and $metadata[8].value == "bench"
and $metadata[9].value == "default (empty)"
and $metadata[15].value == "passed"
and ($metadata[16].value | positive_decimal)
and ($metadata[17].value | positive_decimal)
and ($metadata[18].value | positive_decimal)
and ($metadata[19].value | positive_decimal)
and (($metadata[19].value | tonumber)
     == (($metadata[18].value | tonumber) + 2))
and valid_result($initialized; "initialized_open_probe")
and valid_result($startup; "full_startup_evidence")
and valid_result($populated; "populated_history_full_startup")
and valid_result($bootstrap; "durable_capability_bootstrap")
and all([$initialized, $startup, $populated, $bootstrap][];
        .[6].value == $metadata[16].value)
