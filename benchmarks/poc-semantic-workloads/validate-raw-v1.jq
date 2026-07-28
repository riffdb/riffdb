def positive_integer:
  type == "number" and . > 0 and . == floor;

def nonnegative_integer:
  type == "number" and . >= 0 and . == floor;

def exact_keys($expected):
  (keys | sort) == ($expected | sort);

def measured_rate:
  ((.work_units * 1000000000 / .elapsed_ns) | floor)
  | if . < 1 then 1 else . end;

($report[0]) as $summary
| [
    $summary.workloads[] as $workload
    | range(0; ($workload.samples_ns | length)) as $ordinal
    | {
        schema: "riffdb.poc-semantic-benchmark-raw/v1",
        record_type: "latency_sample",
        report_id: "poc-semantic-workloads",
        workload_id: $workload.workload_id,
        family: $workload.family,
        sample_ordinal: $ordinal,
        elapsed_ns: $workload.samples_ns[$ordinal]
      }
  ] as $expected_samples
| ($summary.configuration.measured_iterations) as $iterations
| ($summary.configuration.commands_per_iteration) as $commands
| [
    {
      workload_id: "conflict-free-command-v1",
      family: "conflict_free_command",
      work_units: ($iterations * $commands),
      rate_kind: "operations_per_second",
      reported_per_second: $summary.workloads[0].ops_per_second
    },
    (
      range(1; 5) as $index
      | {
          workload_id: $summary.workloads[$index].workload_id,
          family: "hot_key_contention",
          work_units:
            ($iterations * $summary.workloads[$index].concurrency),
          rate_kind: "operations_per_second",
          reported_per_second:
            $summary.workloads[$index].ops_per_second
        }
    ),
    {
      workload_id: "idempotent-replay-v1",
      family: "idempotent_replay",
      work_units: ($iterations * $commands),
      rate_kind: "operations_per_second",
      reported_per_second: $summary.workloads[5].ops_per_second
    },
    {
      workload_id: "commit-log-scan-v1",
      family: "commit_log_scan",
      work_units: ($iterations * $summary.configuration.scan_records),
      rate_kind: "records_per_second",
      reported_per_second: $summary.workloads[6].records_per_second
    },
    {
      workload_id: "projection-catch-up-v1",
      family: "projection_catch_up",
      work_units:
        ($iterations * $summary.configuration.projection_records * 2),
      rate_kind: "records_per_second",
      reported_per_second: $summary.workloads[7].records_per_second
    },
    {
      workload_id: "durability-synchronous-v1",
      family: "durability_mode",
      work_units: ($iterations * $commands),
      rate_kind: "operations_per_second",
      reported_per_second: $summary.workloads[11].ops_per_second
    },
    {
      workload_id: "durability-group_commit-v1",
      family: "durability_mode",
      work_units: ($iterations * $commands),
      rate_kind: "operations_per_second",
      reported_per_second: $summary.workloads[12].ops_per_second
    }
  ] as $expected_rates
| [.[] | select(.record_type == "latency_sample")] as $samples
| [.[] | select(.record_type == "rate_aggregate")] as $rates
| type == "array"
and length == (($expected_samples | length) + ($expected_rates | length))
and ($samples == $expected_samples)
and all($samples[];
        exact_keys([
          "elapsed_ns",
          "family",
          "record_type",
          "report_id",
          "sample_ordinal",
          "schema",
          "workload_id"
        ])
        and .schema == "riffdb.poc-semantic-benchmark-raw/v1"
        and .record_type == "latency_sample"
        and .report_id == "poc-semantic-workloads"
        and (.sample_ordinal | nonnegative_integer)
        and (.elapsed_ns | positive_integer))
and all($rates[];
        exact_keys([
          "elapsed_ns",
          "family",
          "rate_kind",
          "record_type",
          "report_id",
          "reported_per_second",
          "schema",
          "work_units",
          "workload_id"
        ])
        and .schema == "riffdb.poc-semantic-benchmark-raw/v1"
        and .record_type == "rate_aggregate"
        and .report_id == "poc-semantic-workloads"
        and (.elapsed_ns | positive_integer)
        and (.work_units | positive_integer)
        and (.reported_per_second | positive_integer)
        and .reported_per_second == measured_rate)
and ([
       $rates[]
       | {
           workload_id,
           family,
           work_units,
           rate_kind,
           reported_per_second
         }
     ] == $expected_rates)
