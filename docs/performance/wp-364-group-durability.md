# WP-364 bounded group durability

RiffDB keeps redb as the authoritative storage engine and keeps redb
`Immediate` two-phase durability. The performance change amortizes physical
commits; it does not acknowledge volatile state.

Production application commands use `CoordinatorDurability::Group`. The one
commit-owned FIFO writer selects at most 16 consecutive compatible transitions,
subject to the existing 64-command and 16 MiB transaction limits. Commands may
share a completion transaction only when their compiler-declared conflict keys
are disjoint. Overlapping or abnormal attempts fall back to independent
execution.

A newly executed audited command now has two authoritative transitions:

1. the current-policy-authorized `Started` audit and exact `Pending` admission;
2. the complete command graph and its linked terminal `Succeeded` audit.

The second transition still atomically contains mutations, outcome, events,
outbox intent, provenance, commit record, pending removal, and terminal audit.
Each command in a physical group retains its own idempotency identity, command
and commit sequence, provenance, outcome, audit link, retry classification, and
acknowledgement. A public batch is therefore never an application transaction.

If a grouped commit returns unknown, the coordinator fences new writes and
looks up every selected command through its own original identity. It never
uses one command's presence as proof for another. A proven pre-commit failure
releases every retained attempt without publishing any member.

`CoordinatorDurability::Sync` remains the semantic and recovery oracle.
`DurabilityMode::Memory` remains test-only. There is no production
`Eventual`-durability path.

Run the checked comparison with:

```bash
./scripts/benchmark-command-growth --assert-perf-004
```

The PERF-004 evidence compares identical immediate two-phase mechanics at
group size 1 and group size 16. The checked gate requires the grouped window to
complete in at most 75% of the synchronous-oracle elapsed time while all
semantic, crash, recovery, coordinator, and server preflight suites pass.

The checked 2026-07-29 run is retained in
`fixtures/performance/wp-364-group-durability.jsonl`: 128 commands completed in
7.254 ms at group size 1 and 2.988 ms at group size 16 (41.18% of the Sync
oracle time), passing PERF-004.

Generated Rust and TypeScript batch helpers remain bounded schedulers over
ordinary symbolic unary commands. They preserve stable per-item idempotency and
typed results; they expose no bulk mutation or cross-item atomicity surface.
