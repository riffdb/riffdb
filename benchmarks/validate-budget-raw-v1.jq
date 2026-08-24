def positive_decimal:
  test("^[1-9][0-9]{0,19}$");

def nonnegative_decimal:
  test("^(0|[1-9][0-9]{0,19})$");

(split("\n") | if .[-1] == "" then .[:-1] else . end) as $lines
| ($lines[0:25]
   | map(capture("^(?<key>[^=]+)=(?<value>.*)$"))) as $metadata
| ($lines[25:]
   | map(capture("^sample\\t(?<workload>[^\\t]+)\\t(?<ordinal>[^\\t]+)\\t(?<elapsed>[^\\t]+)\\t(?<database_size>[^\\t]+)$")))
    as $samples
| ($metadata | map(.key)) == [
  "riffdb_budget_benchmark_capture_format",
  "git_revision",
  "git_dirty",
  "rust_version",
  "rust_target",
  "cpu",
  "memory",
  "storage_medium",
  "operating_system",
  "database_root",
  "filesystem",
  "mount_options",
  "postgres_image",
  "postgres_image_id",
  "postgres_server_version_num",
  "postgres_synchronous_commit",
  "postgres_fsync",
  "postgres_full_page_writes",
  "riffdb_durability_mode",
  "postgres_filesystem",
  "postgres_mount_options",
  "postgres_network",
  "correctness_preflight",
  "warmup_runs",
  "sample_count_per_workload"
]
and $metadata[0].value == "1"
and ($metadata[1].value | test("^[0-9a-f]{40,64}$"))
and $metadata[2].value == "false"
and all($metadata[3:22][]; .value | length > 0 and length <= 4096)
and $metadata[12].value
    == "postgres:18.4-bookworm@sha256:d9c83446333daec3f0588cc709adb80c26090b7f9f0f7ec8d43c243385d79818"
and ($metadata[13].value | test("^sha256:[0-9a-f]{64}$"))
and $metadata[14].value == "180004"
and $metadata[15].value == "on"
and $metadata[16].value == "on"
and $metadata[17].value == "on"
and ($metadata[18].value == "sync" or $metadata[18].value == "group")
and $metadata[21].value
    == "loopback-only ephemeral Docker publication"
and $metadata[22].value == "passed"
and ($metadata[23].value | nonnegative_decimal)
and ($metadata[24].value | positive_decimal)
and (($metadata[24].value | tonumber) >= 3)
and (($samples | length) == (($metadata[24].value | tonumber) * 2))
and all($samples[];
        (.elapsed | positive_decimal)
        and (.database_size | positive_decimal))
and all(
  range(0; ($metadata[24].value | tonumber));
  . as $index
  | $samples[$index * 2] as $postgres
  | $samples[$index * 2 + 1] as $riffdb
  |
  $postgres.workload == "postgres_canonical_sequential_contention"
  and $riffdb.workload == "riffdb_public_sequential_contention_replay"
  and $postgres.ordinal == (($index + 1) | tostring)
  and $riffdb.ordinal == (($index + 1) | tostring)
)
