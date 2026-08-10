# What RiffDB Is

RiffDB moves operational state rules out of scattered application code and into
a checked contract. The application asks for a named operation such as
`CreateTicket` or `AllocateBudget`; it does not assemble arbitrary mutations.

## Contract-first state

A contract declares:

- entities and exact value types;
- aggregates and the partition that owns a mutation;
- commands, inputs, preconditions, mutations, and closed outcomes;
- invariants and commit-time checks;
- durable events and outbox intents;
- indexes and projections; and
- symbolic roles that name allowed commands and queries.

The compiler resolves symbols, assigns internal identities, proves bounds, and
produces an executable plan. Application authors review a lock containing those
derived identities; they do not hand-author them.

## Why commands instead of SQL writes

An application-level SQL statement can be individually valid while a workflow
using it is unsafe: a read/check/write sequence can race, a retry can duplicate
an effect, or two services can implement an invariant differently. RiffDB
admits only compiled command plans and makes those properties part of database
execution.

This does not make PostgreSQL unsafe. PostgreSQL can implement the same rules
with careful transactions, constraints, locking, retry design, and application
discipline. RiffDB's claim is narrower: its public application surface removes
the unchecked write path that permits common mistakes. See [PostgreSQL Safety
Comparison](../tutorials/POSTGRESQL-COMPARISON.md).

## Reads and derived state

RiffQL provides bounded, symbolic operational queries. Named queries are
compiled into immutable modules and execute in one snapshot. General SQL,
unbounded scans, and analytical joins are outside the POC.

Entity state and the commit log are authoritative. Projections are derived and
rebuildable. A projection frontier identifies the greatest commit incorporated,
so a caller can wait for a known commit without treating projection state as
the source of truth.

## One semantic path

Generated Rust, Go, TypeScript, and Python clients, the CLI, public gRPC, and native
MCP all use the shared application service. Authentication and authorization
run before command execution. The deterministic runtime cannot use the network,
filesystem, operating-system clock, process-global mutation, or untracked
randomness.

## POC boundary

The current implementation is standalone and local-only. It deliberately does
not include Raft, replication, distributed transactions, SQL compatibility, or
production authentication. Review [Known Limitations](../known-limitations.md)
for the complete current boundary.
