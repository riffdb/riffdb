# PostgreSQL Safety Comparison

RiffDB includes equivalent LegalSpend and TicketDesk workloads for PostgreSQL
and RiffDB. The comparison is a semantic demonstration first and a benchmark
second: it identifies mistakes an ordinary PostgreSQL application can make and
the corresponding write path RiffDB does not expose.

## Run the public comparison

The PostgreSQL side requires a disposable database URL. Never point the runner
at a database containing valuable data.

```bash
export RIFFDB_COMPARISON_POSTGRES_URL='postgres://localhost/riffdb_comparison'
cargo test --locked --manifest-path examples/budget-comparison/Cargo.toml \
  --test public_comparison -- --nocapture
```

The TicketDesk performance runner is under `examples/app-baseline`. Its README
records the exact setup, workload, and measurement rules. Benchmark numbers are
environment-specific; the checked semantic assertions are the durable result.
`benchmarks/run-app-baseline-python` and
`benchmarks/run-app-baseline-typescript` run that same mix from Python or
TypeScript so the language-runtime cost is visible: PostgreSQL still implements
the `postgres_safe_app` obligations in that language's SQL, while the RiffDB
path uses the generated TicketDesk client with no application-language safety
code (`riffdbd` enforces authorization, idempotency, audit, events, and
outbox). TypeScript reaches `riffdbd` through `riffdb-driverd`. These runners
are not release evidence.

## Patterns under test

| Application mistake | PostgreSQL responsibility | RiffDB boundary |
|---|---|---|
| Read a balance, check it, then update later | Correct isolation, locking, constraint, and retry loop | Compiled command records the influential read and revalidates it at commit |
| Retry after an uncertain response with a new operation identity | Application idempotency table and atomic outcome handling | Caller key plus canonical input hash resolves one durable typed outcome |
| Update state and enqueue an external effect separately | Transactional outbox discipline | Effect intent is part of the command's atomic commit graph |
| Grant an agent generic write access | SQL permission and query-generation controls | Policy exposes only compiled named commands and bounded named queries |

PostgreSQL can implement each safe pattern. The negative controls intentionally
show what happens when the application does not. RiffDB's value is that its
normal application protocol cannot express the unsafe alternative.

## What the comparison does not claim

- It is not a general database performance ranking.
- It does not compare analytical SQL or ad-hoc joins; RiffDB does not provide
  them in the POC.
- It does not claim agents can skip contract review. Unsafe semantics in an
  accepted contract remain unsafe semantics.
- It does not place PostgreSQL on RiffDB's critical path. RiffDB owns its
  standalone storage and commit semantics.

Review the source in `examples/budget-comparison` and `examples/app-baseline`
before interpreting measurements.
