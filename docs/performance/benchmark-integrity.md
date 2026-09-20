# Benchmark integrity

This note freezes the placement, leak-hygiene, and measurement protocol that
every RiffDB performance harness must follow so published numbers are honest
about durable media and not RAM-flattered by tmpfs.

## Host selection and attribution

Follow the [measurement-host guidance](benchmark-host-selection.md) and
ADR-0245's accepted hardware baseline, including SHA-2 acceleration. N1 is
retired from testing. Its historical hashing cost was 7.8 times C3D's with identical
binaries; accelerator differences can reverse optimization priorities. Coordinate
exclusive host use, refresh controls after toolchain/build changes, and keep P99
as a selection gate. WP-797 owns profile validation and N1's replacement.

Before using writer stages, read the
[open census defects and reconciliation method](wp749-writer-census-defects.md).
`busy_us + idle_us` undercounted independent writer wall by 15.30% on N1 and
20.54% on C3D. These are observed errors, not correction factors. Report signed
stage reconciliation and the unattributed remainder against independent wall.

## Placement rules

1. **Never put multi-GB database files on `/tmp` or any other tmpfs/ramfs**
   unless the operator passes an explicit `--allow-tmpfs` (or equivalent) for a
   deliberate diagnostic run. On many developer machines `/tmp` is an 8 GiB
   tmpfs; filling it crashes unrelated processes mid-run.
2. **Default roots live under a `perf-db` path component**, typically
   `<crate-or-repo>/target/perf-db/<harness>/`. The `target/` tree is
   gitignored; the `perf-db` component is the hostile-env belt for stale-dir
   sweeps (sweeps refuse to run outside it).
3. **Resolution order** for every harness that uses `riffdb-bench-root`:
   - CLI `--database-root`
   - environment `RIFFDB_BENCH_DB_ROOT`
   - harness-specific default under `target/perf-db/<harness>/`
4. Before any daemon spawn, the harness **classifies the medium** (`statfs` +
   `/proc/self/mounts`) and **refuses** tmpfs/ramfs without allow. Free space
   below the configured floor is a hard error (ENOSPC honesty).

## Same-device requirement (app-baseline)

When comparing RiffDB to PostgreSQL, both engines must place durable state on
the **same physical device**:

- RiffDB session dirs under `$repo/target/perf-db/app-baseline/`
- PostgreSQL data dir bind-mounted to
  `$repo/target/perf-db/app-baseline/postgres` (see `benchmarks/run-app-baseline`)

Reports record:

| Field | Meaning |
|-------|---------|
| `riffdb_database_root` / `riffdb_storage_medium` | RiffDB session root + classified medium |
| `postgres_data_host_path` / `postgres_storage_medium` | Host path bind-mounted into the container + classified medium |
| `same_device` | `true` when mount/device model match; otherwise `false` plus a stderr warning and `integrity_notes` entry |

Parity gates **refuse** when the PostgreSQL host data medium is RamBacked (mirror
of the RiffDB tmpfs gate). Mixed-device runs (`same_device: false`) are recorded
explicitly so operators do not treat them as evidentiary by accident.

## Durability settings (PostgreSQL)

Parity and write-parity gates refuse (settings message, not ratio) when any of
`synchronous_commit`, `fsync`, or `full_page_writes` is not `on`. The harness
also records `server_version_num`, `wal_sync_method`, and `data_directory`.

## Leak hygiene

- Session directories are named `riffdb-bench-<harness>-<pid>-<n>` (plus legacy
  prefixes still recognized by the sweeper).
- `BenchDir` removes itself on drop (including panic unwind).
- At harness start, `sweep_stale` removes matching dirs whose pid is dead
  (`/proc/<pid>` absent) or whose mtime is older than 24 h.
- After a clean run the bench root should contain only the root itself (no
  leaked multi-GB children).

## Dead-peer abort

Closed-loop load coordinators sleep in ≤250 ms slices and poll an abort hook.
When `riffdbd` exits mid-load the harness fails within about two seconds with
exit status, stderr tail, resolved database root, and free-bytes-at-start.
Workers are joined; `BenchDir` Drop still runs. PostgreSQL load sites pass no
abort hook.

### Enforceable self-test entry point

The kill-9 e2e lives in `examples/app-baseline/tests/dead_peer_abort.rs`. Soft-skip
without a built daemon is intentional for plain `cargo test`; the **enforced**
path is:

```bash
benchmarks/run-app-baseline --self-test
```

That builds `riffdbd` (release), exports `RIFFDB_APP_BASELINE_RIFFDBD_BIN` and
`RUN_RIFFDB_DEAD_PEER=1`, then runs:

```bash
cargo +1.97.0 test --manifest-path examples/app-baseline/Cargo.toml --workspace
```

Use `--workspace` always: the manifest is both package and workspace root;
without it member crates (and most T2 unit tests) are skipped.

For writer-stage attribution only, `--seed-only` stops immediately after the
generated full seed and retains the bounded process-generation writer evidence
in the JSON report:

```bash
./benchmarks/run-app-baseline --full --skip-postgres --seed-only \
  --samples 1 --warmup 0 --reps 1 --output "$HOME/tmp/riffdb-seed-ledger.json"
```

This is a diagnostic boundary, not a `PERF-018` comparator or release-evidence
substitute. It exists so seed wall time and shutdown telemetry cover the same
commands without page-query or projection-catch-up work.

## Statistical protocol

| Mode | Measure | Warmup | Default reps |
|------|---------|--------|--------------|
| smoke | short (5 s load) | 1 s | 1 |
| full  | 90 s load        | 15 s | 3 |
| ADR-0146 unary | 1,000 operations | 20 same-operation calls after RiffDB restart | 5 process generations |

- Load duration **&lt; 60 s** is marked `non_evidentiary_window` in the report.
- Dual-backend load reps execute as isolated, counterbalanced phases
  (`PG1, R1, R2, PG2, …`). PostgreSQL and its proxy are stopped before a
  RiffDB phase; odd repetitions reverse the order to decorrelate device/cache
  drift from backend identity.
- Gated scalars report `{median, min, max, reps, spread_ratio}`;
  `spread_ratio > 2.0` ⇒ `stability: "unstable"`.
- `--require-stable` exits nonzero on unstable gated metrics, and **refuses**
  when `--reps < 2` (single-rep stability is trivial).
- Parity gates evaluate the **median** of reps and **refuse** (do not pass)
  when either backend’s gated metric is unstable.
- ADR-0146 unary evidence counterbalances backend order as
  `PG,RiffDB / RiffDB,PG / PG,RiffDB / RiffDB,PG / PG,RiffDB`. It retains all
  five per-generation p50/p95 values, qualifies their median, and refuses when
  the sorted central-three spread `x4 / x2` exceeds `1.20`.
  Scenario class is fixed before execution; every cell retains the unfavorable
  safe-application PostgreSQL ratio even though only the cloud absolute SLO and
  frozen RiffDB low-water regression are unary release gates. Preflight and
  postflight V2 host evidence additionally binds the complete-cell CPU-steal
  delta and refuses above the fixed 1.00-percent ceiling.

### Development performance sentinel

Performance-sensitive changes should use
`./scripts/app-baseline-performance-sentinel` before and after the change. This
RiffDB-only diagnostic uses 5-second windows for an interactive concurrency
sweep and one write-only saturation cell. The diagnostic sweep uses one daemon
and accumulated history to avoid repeated setup; release evidence retains fresh
per-level isolation. Each sentinel phase includes the full seed and checks
versioned floors/ceilings for seed time, throughput, p50/p99 latency,
unary-write p50, and correctness. It reruns only a failed cell once and normally
finishes in 60–90 seconds.

The sentinel is intentionally marked `evidentiary: false`: it detects a large
regression early and creates useful bisect points, but it cannot establish a
cross-engine ratio or close a release requirement. PostgreSQL is not a useful
per-commit regression oracle because its own run-to-run variation would hide
the identity of the changed RiffDB code. The retained 90-second,
three-repetition, same-device comparison remains the milestone/release path.

## Device baseline

Once per invocation the harness runs a cheap probe (no `fio` dependency):

- 10 s loop of 4 KiB write + `sync_data` → `fdatasync_p50_us`, `p99_us`,
  `fsyncs_per_s`
- one 64 MiB buffered write + final `fdatasync` → sequential MB/s

Results are embedded as `device_baseline` next to an `environment` block
(fstype, mount options, device model when available, kernel, CPU governor).

## Sweep isolation modes (app-baseline)

| Mode | Flag | Semantics |
|------|------|-----------|
| per-level daemon (default) | (none) / `--load-sweep-per-level-daemon` | Fresh daemon and identical seed per client point. RiffDB restarts after setup, so shutdown telemetry covers that point's warmup and measurement but excludes bootstrap/deploy/seed (`histogram_scope: per_level_process_after_setup_including_warmup`). |
| accumulated history | `--load-accumulate-history` | One daemon for the whole sweep; later points include earlier writes and the final histogram has `histogram_scope: cumulative_final`. |

Both modes remain available. Accumulation is a deliberate history-growth
experiment, never an implicit concurrency comparison. Reports record
`sweep_isolation`.

## Evidence eligibility and report families

An evidentiary load report must contain environment/device identity,
PostgreSQL durability (when present), same-device comparison, at least two
repetitions, stable gated metrics, clean semantic outcomes, and resource deltas
for every point. A duration under 60 seconds is always marked
`non_evidentiary_window`.

The alpha gate uses three report families rather than one score:

1. parity — warm operation and seed comparisons;
2. load — closed/open arrivals, contention, tenants, shapes, fairness;
3. resilience — process death, replay, durable consumers, authorization and
   retained-history recovery.

`benchmarks/run-app-baseline --alpha-matrix` hashes every required report into
an eligibility manifest. `--alpha-matrix-smoke` validates plumbing but is
explicitly never release evidence.

Resource deltas are attribution aids, not application metrics: process CPU
ticks/RSS/I/O and durable bytes for RiffDB; database blocks/temp/WAL/database
size for PostgreSQL. Byte growth and kernel/WAL writes are also normalized by
successful mutations; a read-only point reports `null`, never a fabricated
zero-per-mutation ratio. Identifiers and application values are never labels.

## Stderr capture

`riffdbd` stderr is retained in a ring buffer (200 lines / 64 KiB) and included
in ready-timeout, shutdown-failure, and dead-peer messages. Child processes set
`RUST_BACKTRACE=1`.

## Owned helper crate

`crates/riffdb-bench-root` is the shared implementation. Nested workspaces
(`examples/app-baseline`, `examples/budget-comparison`,
`benchmarks/command-growth`, `benchmarks/storage-fjall`) depend on it by path.
Do not reintroduce `std::env::temp_dir()` on performance-critical paths.
