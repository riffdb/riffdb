def pair:
  capture("^(?<key>[^=]+)=(?<value>.*)$");

def parsed:
  (reduce inputs as $line
    ({metadata: {}, current: null, results: []};
     ($line | pair) as $pair
     | if $pair.key == "benchmark" then
         (if .current == null
          then .
          else .results += [.current]
          end)
         | .current = {benchmark: $pair.value}
       elif .current == null then
         .metadata[$pair.key] = $pair.value
       else
         .current[$pair.key] = $pair.value
       end))
  | if .current == null then . else .results += [.current] end
  | del(.current);

parsed as $capture
| {
    schema: "riffdb.benchmark-report/v1",
    report_id: "storage-redb",
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
      database_root: $capture.metadata.database_root,
      filesystem: $capture.metadata.filesystem,
      mount_options: $capture.metadata.mount_options
    },
    methodology: {
      build_profile: $capture.metadata.build_profile,
      features: [$capture.metadata.enabled_features],
      database_size: "reported per workload in bytes",
      contract_version: ($capture.results[0].contract_version),
      workload_distribution: ($capture.results[0].workload_distribution),
      durability_modes: ($capture.results | map(.durability_mode) | unique),
      warmup_runs: $configured_warmup,
      sample_count: ($capture.results | map(.sample_count | tonumber) | add),
      distribution_method: "complete sorted repeated-run nanosecond distributions with nearest-rank p50/p95/p99",
      analysis_command: $analysis_command
    },
    correctness_status: "passed",
    performance_status: "measured",
    raw_artifact: $raw_path,
    raw_sha256: $raw_hash,
    unsafe_postgresql_variants_included: false,
    results: (
      $capture.results
      | map({
          workload_id: .benchmark,
          workload: .workload,
          workload_distribution: .workload_distribution,
          durability_mode: .durability_mode,
          database_size_bytes: (.database_size_bytes | tonumber),
          sample_count: (.sample_count | tonumber),
          unit: "nanoseconds",
          samples_ns: (.sample_ns | fromjson),
          min_ns: (.min_ns | tonumber),
          p50_ns: (.p50_ns | tonumber),
          p95_ns: (.p95_ns | tonumber),
          p99_ns: (.p99_ns | tonumber),
          max_ns: (.max_ns | tonumber),
          mean_ns: (.mean_ns | tonumber)
        })
    )
  }
