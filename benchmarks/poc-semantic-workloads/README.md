# RiffDB POC Semantic Workloads

This non-production harness measures the semantic paths required by `SPEC.md`
Section 2.3. It drives the real LegalSpend API-neutral application service,
authorization, conflict manager, command runtime, commit coordinator, redb
storage, idempotency ledger, authoritative commit scan, and derived projection
implementation assembled by the long-lived budget comparison.

The report is architectural evidence, not a marketing claim or an absolute TPS
gate. Correctness preflight and workload-specific assertions run before a report
is written.

## Workload Contract

The stable report ID is `poc-semantic-workloads`; the summary schema is
`riffdb.poc-semantic-benchmark-report/v1`. `workloads` contains:

| Family | Stable rows |
|---|---|
| `conflict_free_command` | one synchronous, disjoint-key command row |
| `hot_key_contention` | exact concurrency values 1, 8, 32, and 128 |
| `idempotent_replay` | equal-input retries of one committed command |
| `commit_log_scan` | one frozen-fence contiguous authoritative scan |
| `projection_catch_up` | one commit-log-to-published-frontier catch-up |
| `restart_recovery` | three increasing positive authoritative commit counts with positive, nondecreasing allocated database sizes |
| `durability_mode` | exact `synchronous` and `group_commit` rows |

Every row retains raw nanosecond samples and nearest-rank p50, p95, and p99.
Command and durability rows report `ops_per_second`; scan and projection rows
report `records_per_second`. Each rate has a raw aggregate record containing
the exact work units and monotonic elapsed nanoseconds used to recompute it.
The runner records the Git state, Rust compiler and target, build profile, CPU,
memory, storage medium, operating system, filesystem, database size where
relevant, contract version, workload distribution, and exact JSON-encoded
analysis argv.

The runner recreates an isolated database root beneath
`benchmarks/poc-semantic-workloads/target/`, sets `TMPDIR` to that root, and
records that root's exact filesystem and mount options. Semantic database files
therefore cannot silently fall back to a different `/tmp` mount.

The `group_commit` row is deliberately narrow: the POC coordinator selects and
durably records `CoordinatorDurability::Group`, but currently stages one command
per transaction. The row does not claim command batching, scheduling-window
behavior, fairness, or flush amortization.

## Smoke

Smoke mode exercises every family, including 128 callers, with one measured
iteration. It is suitable for contract and correctness verification but is not
publishable performance evidence.

```bash
./benchmarks/poc-semantic-workloads/run \
  --smoke \
  --storage-medium "local NVMe SSD"
```

The ignored smoke report is written under
`benchmarks/poc-semantic-workloads/target/`.

## Publication

Publish only from a committed clean checkout. Full mode defaults to two warmups,
nine measured iterations, 64 commands per command workload, a 256-record scan,
a 128-event projection catch-up, and restart databases containing 16, 128, and
1,024 authoritative commits.

```bash
./benchmarks/poc-semantic-workloads/run \
  --publish \
  --storage-medium "local NVMe SSD"
```

Publication writes the frozen integration paths
`benchmarks/reports/poc-semantic-workloads/report-v1.json` and
`benchmarks/reports/poc-semantic-workloads/raw-v1.jsonl`. The summary binds the
raw JSONL by path and SHA-256.

Optional full-run controls are bounded environment variables:

- `RIFFDB_POC_BENCHMARK_WARMUPS`: 0 through 20.
- `RIFFDB_POC_BENCHMARK_SAMPLES`: 3 through 100.
- `RIFFDB_POC_BENCHMARK_COMMANDS`: 1 through 512.
- `RIFFDB_POC_BENCHMARK_SCAN_RECORDS`: 8 through 2,048.
- `RIFFDB_POC_BENCHMARK_PROJECTION_RECORDS`: 4 through 1,024.

Changing these does not change the stable workload inventory. The report
captures the effective values.

The restart rows are ordered by authoritative commit count. Their physical file
sizes are positive and nondecreasing, but may be equal across adjacent rows:
redb and the durability journal preallocate bounded extents, so a larger logical
history need not cross a physical allocation boundary in every sample.

## Contract Checks

```bash
./benchmarks/poc-semantic-workloads/test-contract
cargo +1.97.0 test \
  --manifest-path examples/budget-comparison/Cargo.toml \
  -p riffdb-budget-comparison --test service_comparison \
  performance_support::tests
cargo +1.97.0 test \
  --manifest-path examples/budget-comparison/Cargo.toml \
  -p riffdb-budget-comparison --test service_comparison --no-run --release
```

`validate-report-v1.jq` is the executable semantic validator.
`report-schema-v1.json` documents the same public evidence shape for tools that
consume JSON Schema.
