# Budget Comparison Workspace

This isolated, non-production workspace owns the long-lived LegalSpend budget
workload used by WP-045, later RiffDB adapters, and eventual comparison reports.
It is not a benchmark and does not contribute PostgreSQL code or dependencies to
the RiffDB production workspace.

## Contents

- `core`: typed workload inputs, exact `decimal<28,2>` values, normalized
  outcomes, a deterministic reference model, guarantee profiles, and fixture
  generation.
- `postgres`: synchronous `postgres`/`NoTls` adapter using explicit SQL
  transactions.
- `safety-evidence`: correctness-only PostgreSQL negative controls and public
  RiffDB contrasts. It is never a benchmark adapter or shipped product.
- `riffdb-grpc`: the canonical public Rust SDK/gRPC adapter and the frozen
  `riffdb-budget-public` runner.
- `fixtures`: canonical JSON workload, reference observations, and the exact
  PostgreSQL guarantee profile, plus the separately versioned safety report.
- `tests`: offline oracle/isolation checks and the optional-local,
  mandatory-in-CI live correctness preflight.

The sequential case covers invalid creation, missing mutation, command-based
creation of a 100.00 budget, duplicate-binding priority, invalid allocation,
successful allocations, and insufficient funds. The contention case releases
two independently prepared 80.00 allocations against the same 100.00 row at an
explicit barrier. Its normalized oracle requires exactly one `Allocated`, one
`InsufficientBudget`, and final allocation 80.00 without depending on which
contender acquires the row first.

## PostgreSQL Semantics

Every command opens an explicit `READ COMMITTED` transaction, sets
`synchronous_commit=on`, and bounds lock waits to 5 seconds and statements/idle
transactions to 10 seconds. `AllocateBudget` locks the annual row with
`SELECT ... FOR UPDATE`, evaluates declared preconditions in contract order,
updates exact `NUMERIC(28,2)` text, and commits. No floating-point conversion is
used.

`CreateBudget` inserts a zero-valued transaction-local candidate with
`ON CONFLICT DO NOTHING` before checking positive approval. A duplicate therefore
wins over an invalid approval as required by binding-failure priority. Invalid
candidates roll back; successful candidates are fully initialized before commit.
The adapter assumes it is the only writer to its dedicated table.

The PostgreSQL profile matches this workload's command outcomes, atomic row
mutation, decimal invariants, and same-row conflict exclusion. It deliberately
does **not** claim RiffDB idempotent uncertainty recovery, durable events,
provenance, outbox intent, projections/frontiers, shared authorization, compiled
contract enforcement, or deterministic logical time. PostgreSQL
`transaction_timestamp()` is checked for presence and normalized out of cross-
backend observations. The exact machine-readable profile is
`fixtures/postgres-guarantees-v1.json`.

## Safety Counterexamples

PostgreSQL supports safe implementations, including the canonical comparison
adapter. The counterexamples show that hazardous patterns remain expressible
through general SQL and host transaction code, while the corresponding patterns
are absent or rejected through RiffDB's supported application mutation and
compiled-contract surfaces.

The shorter statement that a pattern is "impossible in RiffDB" means only that
it is not expressible through RiffDB's supported application mutation surface.
It does not cover a malicious contract or administrator, operating-system or
database-file compromise, implementation defects, or features outside the POC.

The checked `riffdb.budget.safety-evidence/v1` report contains exactly four
correctness scenarios:

1. `lost_update_without_lock` contrasts a deterministic PostgreSQL
   read/check/absolute-write lost update with the compiled RiffDB conflict
   domain.
2. `direct_dml_precondition_bypass` shows that direct PostgreSQL DML can omit
   the command precondition while RiffDB returns the declared `InvalidAmount`
   outcome without mutation.
3. `duplicate_retry_after_discarded_response` contrasts two PostgreSQL
   mutations with one RiffDB mutation and an identity-preserving replay.
4. `same_key_different_input` contrasts PostgreSQL ignoring the key with
   RiffDB's safe `idempotency_key_reuse` rejection.

The canonical PostgreSQL adapter remains unchanged beside these negative
controls and demonstrates the safe `FOR UPDATE` remedy. The safety package does
not implement the normal `BudgetBackend` trait, emit timing results, or enter a
benchmark target. It also does not claim real TCP response loss or restart
recovery; those remain WP-190/WP-200 evidence.

## Commands

Offline acceptance (the live test reports a skip unless configured):

```bash
cargo test --manifest-path examples/budget-comparison/Cargo.toml --workspace
cargo run --manifest-path examples/budget-comparison/Cargo.toml \
  -p riffdb-budget-comparison-core --bin budget-fixtures -- --check
cargo run --manifest-path examples/budget-comparison/Cargo.toml \
  -p riffdb-budget-safety-evidence --bin budget-safety-fixtures -- --check
cargo fmt --manifest-path examples/budget-comparison/Cargo.toml \
  --all -- --check
cargo clippy --manifest-path examples/budget-comparison/Cargo.toml \
  --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc \
  --manifest-path examples/budget-comparison/Cargo.toml --workspace --no-deps
```

To regenerate reviewed fixtures, replace `--check` with `--write` and review the
full diff.

For a live local run, start a dedicated database with the exact accepted image:

```bash
docker run --rm --name riffdb-wp045-postgres \
  -e POSTGRES_USER=riffdb -e POSTGRES_PASSWORD=riffdb \
  -e POSTGRES_DB=riffdb_budget -p 55432:5432 \
  postgres:18.4-bookworm@sha256:d9c83446333daec3f0588cc709adb80c26090b7f9f0f7ec8d43c243385d79818
```

Then run the exact acceptance command in fail-closed mode:

```bash
RIFFDB_BUDGET_POSTGRES_REQUIRED=1 \
RIFFDB_BUDGET_POSTGRES_URL=postgres://riffdb:riffdb@127.0.0.1:55432/riffdb_budget \
cargo test --manifest-path examples/budget-comparison/Cargo.toml \
  -p riffdb-budget-comparison --test postgres_live -- --test-threads=1
```

The live test drops and recreates `riffdb_wp045_budget_v1`; the configured
database must be dedicated to this preflight. Required-live evidence checks
`server_version_num=180004`, `synchronous_commit=on`, `fsync=on`, and
`full_page_writes=on` in addition to transaction isolation and wait bounds. CI
always sets required mode and uses the digest-pinned service. A skipped local
test is not WP-045 exit evidence.

To build a fresh `riffdbd`, provision only the accepted normal public
capability, run all four live contrasts, and print the exact checked JSONL
report:

```bash
RIFFDB_BUDGET_POSTGRES_URL=postgres://riffdb:riffdb@127.0.0.1:55432/riffdb_budget \
./scripts/budget-safety-demo --assert
```

This command is fail-closed. The PostgreSQL database must be dedicated to the
destructive comparison, and unavailable PostgreSQL, failed RiffDB readiness,
missing evidence binaries, a skipped test, or any report mismatch is an error.
The URL, bearer and bootstrap credentials, and temporary paths are never
printed. `riffdb-budget-safety` is a non-shipped evidence runner, not a fourth
RiffDB product binary.

Correctness preflight is mandatory before later benchmark work. WP-125 adds the
in-process RiffDB service adapter and WP-135 adds the canonical public gRPC/SDK
adapter; both must reuse this core workload and oracle instead of redefining it.

## Optimization diagnostics

The frozen publication suite (`benchmarks/run-budget-comparison`) reports only
suite-level process wall-clock. For triage of *where* time goes, use:

```bash
./benchmarks/run-budget-diagnostics --smoke
# or, for a longer sample set:
./benchmarks/run-budget-diagnostics --full --samples 12 --warmup 2 --commands 64
```

This writes layered JSON under `target/budget-diagnostics/`:

- `layered-report-v1.json` — in-process RiffDB service vs canonical PostgreSQL
  with per-operation sequential timings, contention wall, and amortized
  create/allocate throughput on unique keys.
- `public-path-report-v1.json` — public gRPC process phases
  (`server_start_ready`, `bootstrap_deploy_credentials`, `runner_sequential`,
  `runner_contention`, `runner_same_key_replay`, `server_shutdown`).
- `summary-v1.txt` — human-readable ratios and bottleneck hints.

Diagnostic reports are not WP-200 publication evidence and do not replace
`benchmarks/run-budget-comparison`. Use them to decide whether to profile the
semantic kernel, public transport, or process lifecycle first.
