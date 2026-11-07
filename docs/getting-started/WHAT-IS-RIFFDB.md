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

### Concrete example: competing budget reservations and retry replay

Consider two concurrent workers attempting to reserve `100` units from the same
budget envelope when only `100` units remain, followed by a network timeout and
retry:

```riff
command ReserveBudget {
  input request_id: string<128>
  input org_id: uuid
  input envelope_id: uuid
  input amount: u64
  idempotency_key request_id

  mutate BudgetEnvelope(org_id, envelope_id) as env else EnvelopeMissing {}
  require SufficientFunds: env.remaining >= amount else InsufficientFunds {
    remaining: env.remaining
  }

  set env.remaining = env.remaining - amount checked_by SufficientFunds
  return Reserved { remaining: env.remaining }
}
```

- **Conflict serialization**: Both calls derive conflict key `(org_id,
  envelope_id)`. One commits `Reserved { remaining: 0 }`; the second observes
  the post-image and receives the closed `InsufficientFunds { remaining: 0 }`
  outcome without negative balances or unhandled serialization exceptions.
- **Compiler-checked write evidence**: `checked_by SufficientFunds` proves at
  compile time that the subtraction cannot underflow `u64`.
- **Durable idempotent replay**: If the winning caller loses its connection and
  resubmits `request_id = "req-a-001"`, RiffDB returns the stored `Reserved {
  remaining: 0 }` outcome at the original `commit_sequence` with `replayed:
  true` rather than deducting `100` a second time.

## Reads and derived state

RiffQL provides bounded, symbolic operational queries. Named queries are
compiled into immutable modules and execute in one snapshot. General SQL,
unbounded scans, and arbitrary analytical joins are outside the `0.2.x` scope.

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

## Scope and boundaries

The current implementation supports local and verified direct-TLS deployments,
single-primary asynchronous and quorum-acknowledged follower replication, and
incarnation-fenced primary promotion. It deliberately excludes multi-primary
writes, distributed cross-partition transactions outside compiled sagas, SQL
compatibility, and built-in end-user OAuth or identity providers. Review
[Compatibility](../compatibility.md) and [Safety by Construction](../safety-by-construction.md)
for the complete current boundary.
