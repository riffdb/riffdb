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
- `fixtures`: canonical JSON workload, reference observations, and the exact
  PostgreSQL guarantee profile.
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

## Commands

Offline acceptance (the live test reports a skip unless configured):

```bash
cargo test --manifest-path examples/budget-comparison/Cargo.toml --workspace
cargo run --manifest-path examples/budget-comparison/Cargo.toml \
  -p riffdb-budget-comparison-core --bin budget-fixtures -- --check
cargo fmt --manifest-path examples/budget-comparison/Cargo.toml \
  -p riffdb-budget-comparison-core -p riffdb-budget-comparison-postgres \
  -p riffdb-budget-comparison -- --check
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
cargo test --manifest-path examples/budget-comparison/Cargo.toml --workspace
```

The live test drops and recreates `riffdb_wp045_budget_v1`; the configured
database must be dedicated to this preflight. Required-live evidence checks
`server_version_num=180004`, `synchronous_commit=on`, `fsync=on`, and
`full_page_writes=on` in addition to transaction isolation and wait bounds. CI
always sets required mode and uses the digest-pinned service. A skipped local
test is not WP-045 exit evidence.

Correctness preflight is mandatory before later benchmark work. WP-125 adds the
in-process RiffDB service adapter and WP-135 adds the canonical public gRPC/SDK
adapter; both must reuse this core workload and oracle instead of redefining it.
