# Benchmark Reports

POC performance results are architectural feedback, not marketing claims.
Every published report must include the exact revision, CPU, memory, kernel,
filesystem and mount options, storage medium, durability mode, database size,
workload, concurrency, warmup, sample count, raw output, and analysis command.

The release requires two independent comparisons:

- the same storage semantics and benchmark harness against redb and Fjall; and
- the canonical PostgreSQL adapter against the public Rust SDK/gRPC RiffDB
  adapter after both correctness preflights pass.

Required workload result families are:

- conflict-free throughput and p50/p95/p99 latency;
- hot-key contention at 1, 8, 32, and 128 callers;
- idempotent replay latency;
- commit-log scan and projection catch-up throughput;
- restart recovery at multiple database sizes; and
- synchronous versus group-commit durability.

The dependency-free redb component capture uses the stable workload IDs
`initialized_open_probe`, `full_startup_evidence`,
`populated_history_full_startup`, and `durable_capability_bootstrap`. Those
measure storage/open and durable-bootstrap costs only. They do not stand in for
the command, hot-key, replay, projection, restart-size, or durability-mode
families above, and they cannot satisfy the PostgreSQL-versus-public-RiffDB
comparison.

The canonical budget capture uses the stable suite IDs
`postgres_canonical_sequential_contention` and
`riffdb_public_sequential_contention_replay`. Correctness preflight gates both
suites and unsafe PostgreSQL controls are structurally absent. The RiffDB suite
also includes same-key replay, so the result is a qualified suite-level
comparison, not a per-command TPS equivalence. This capture does not replace
the six POC workload families above.

The report names the exact production durability selected by the captured
RiffDB server (`sync` or `group`). The public adapter accepts only that closed
production set and requires the response, replay, commit notification, and
exact-end commit scan to agree. Test-only `memory`, absent or unknown values,
and cross-surface mismatches invalidate the evidence; applications receive no
durability selector.

Every budget timing row records the complete database-size sample distribution.
PostgreSQL sizes are logical bytes returned by `pg_database_size` after the
sample; RiffDB sizes are peak bytes of the exact `riffdb.redb` file observed
under the isolated database root while the public suite runs.

The canonical PostgreSQL adapter may participate in comparative performance
work only after its live correctness preflight. The four deliberately unsafe
PostgreSQL variants under `safety-evidence` are never benchmark eligible.

The isolated WP-075 workspace lives at `benchmarks/storage-fjall`. No
engine-performance comparison can be published or inferred from it: the
unchanged semantic-conformance suite currently records a conformance failure.
`benchmarks/run-storage-fjall` publishes that failure as zero-sample evidence
and deliberately does not run the substrate timing harness.

`benchmarks/run-budget-comparison` is the dependency-free repeated-run wrapper
around the frozen canonical PostgreSQL and public RiffDB adapters. It launches
the digest-pinned PostgreSQL 18.4 image on loopback with its data bind-mounted
beneath the declared benchmark root. Both correctness preflights run before
timing, and the wrapper cannot import or execute any unsafe PostgreSQL control.
Its small `std`-only process helper uses Rust `Instant`; no OS wall-clock value
is a duration source. Docker is therefore a required host tool for this
publication runner; arbitrary remote PostgreSQL servers are not accepted.

`status-v1.json` points to each checked summary and raw artifact by path and
SHA-256. Each summary is bounded at 1 MiB and each raw artifact at 64 MiB. The
redb, Fjall, and budget summaries use `riffdb.benchmark-report/v1` and record:

- `report_id`, source revision, clean state, Rust version, and target;
- CPU, memory, storage medium, operating system, kernel, the database root,
  filesystem, and mount options;
- build profile, enabled features, database size, contract version, workload
  distribution, durability modes, warmup, sample count, repeated-run
  distribution method, and exact analysis command as a compact JSON argv
  array rather than a shell-escaped placeholder;
- correctness status, performance status, structured results, and the exact raw
  artifact path and hash; and
- `unsafe_postgresql_variants_included: false`.

The hashes are necessary but not sufficient evidence. `benchmarks/verify-evidence`
parses each raw artifact with its canonical v1 grammar, rebuilds the redb and
budget summaries, compares every semantic raw sample and ordinal with the
semantic summary, and checks the fixed Fjall failure capture against the
checked conformance and eligibility records. It also recalculates sample
counts, sorted distributions, means, the report-specific percentile contract,
and every semantic throughput rate from raw aggregate work and monotonic
elapsed-time evidence. Both `benchmarks/update-status` and the release evidence
gate invoke this validator.

Every source revision in a published report is a full Git commit object ID. It
must be available locally and must be an ancestor of the release revision being
verified. This permits the four clean reports to be captured and committed
sequentially: each later evidence commit still descends from every earlier
source revision. A report from an unrelated history, a missing object, an
abbreviated object ID, or evidence captured after the proposed release revision
fails closed. Release verification therefore needs enough Git history to
resolve every recorded source commit.

The status document has four independently verified entries:
`redb_report`, `fjall_report`, `budget_comparison_report`, and
`poc_semantic_report`. The last uses report ID `poc-semantic-workloads` and is
the only report allowed to satisfy the six required POC families. It uses
`riffdb.poc-semantic-benchmark-report/v1` with top-level `publication_status`,
`source`, `environment`, `configuration`, bounded nonempty `limitations`, and
`workloads`. Publication status is exactly
`measured_after_correctness_preflight`; source binds the clean revision,
toolchain, target, raw artifact, and raw SHA-256. Its workload rows use these
exact `family` values:

- one `conflict_free_command`;
- four `hot_key_contention` rows ordered at concurrency 1, 8, 32, and 128;
- one `idempotent_replay`;
- one `commit_log_scan`;
- one `projection_catch_up`;
- at least three `restart_recovery` rows with distinct positive database sizes;
  and
- two `durability_mode` rows, exactly `synchronous` and `group_commit`.

Every row has a unique bounded `workload_id`, a positive `sample_count`, the
complete sorted `samples_ns`, and ordered `p50_ns`, `p95_ns`, and `p99_ns`.
Conflict and hot-key rows carry positive `concurrency`; restart rows carry
positive `database_size_bytes`; durability rows identify `synchronous` or
`group_commit`. Command and durability rows record positive `ops_per_second`;
scan and projection rows record positive `records_per_second`. The report-level
sample count equals the sum of row counts. Storage probes and the suite-level
budget comparison cannot populate these families by alias.

A checked Fjall `conformance_failure` is a valid WP-075 result but must use
`performance_status: not_run_due_to_conformance_failure`, an empty result set,
and a bounded failure classification. It cannot support a cross-engine
performance claim.

Publish each report from a clean source revision, using an accurate description
of the storage medium:

```bash
benchmarks/run-storage-redb --storage-medium '<DESCRIPTION>'
benchmarks/run-storage-fjall --storage-medium '<DESCRIPTION>'
benchmarks/run-budget-comparison \
  --storage-medium '<DESCRIPTION>'
benchmarks/poc-semantic-workloads/run --publish \
  --storage-medium '<DESCRIPTION>'
```

After all four report/raw pairs are checked in, regenerate their exact hashes
and the qualified claim:

```bash
benchmarks/update-status
./scripts/release-evidence --verify
```

The validation contract itself is dependency-free and does not execute a
benchmark:

```bash
benchmarks/test-evidence-validation
```
