# ADR-0082: Single Total Order and Pre-Alpha Format Acceptances

- **Status:** Proposed
- **Date:** 2026-07-31
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `TXN-002`, `ID-004`, `STO-001`, `PERF-004`
- **Related work packages:** pre-alpha format acceptances
- **Amends:** ADR-0004 (records the retention conclusions it deferred), ADR-0005
  (records key-layout acceptance), ADR-0063 (names multi-database as the scale
  unit for capacity)

## Context

Concurrency-sweep evidence shows the production write path plateaus at a flat
per-database ceiling governed by the single commit coordinator, with bounded
tails, while alternatives peak higher and then regress with multi-second
worst cases. Several other performance-relevant format properties — the
idempotency key layout and the absence of retention affordances — were
audited before alpha. Each is either cheap to change now and breaking later,
or provably safe to leave alone. This record makes those dispositions
explicit so none is relitigated as an accidental fix.

## Decision

### One database is one total order — permanently

Every database has exactly one contiguous `CommitSequence` domain assigned by
exactly one commit coordinator. Event identities, commit subscriptions,
projection frontiers, read-your-writes heads, provenance links, and history
incarnation validation are all anchored to that order. This is permanent:

- No future work may shard, partition, or parallelize sequence assignment
  within one database. A proposal that requires it is a proposal for a new
  major storage and API version, not an optimization.
- Capacity beyond one database's ceiling is obtained by operating multiple
  databases (ADR-0063). The per-database throughput plateau is a documented
  product characteristic, not a defect.
- Throughput work inside one database is limited to implementation of the
  existing contract (evaluation parallelism, admission, storage costs); the
  sequencer remains the sole orderer.

### Idempotency identity key v1 is accepted as final

The v1 idempotency storage key (ADR-0005: bounded 908-byte layout with the
keyed caller digest in tail position) is deliberately accepted for the alpha
and beyond. The tail digest scatters inserts within one principal-and-command
cluster; the resulting B-tree churn is measured as tolerable, and the layout
keeps digest material inseparable from its scoping components. Future
retention or locality needs are met by additive secondary structures, never
by a key-layout revision.

### Retention needs no format reservations

A pre-alpha audit confirmed future retention can be built additively:

- Sequence-keyed tables (commits, events, outbox, audit) prune by sequence
  watermark using existing keys.
- Age-ordered pruning of digest-keyed idempotency outcomes requires only an
  additive time-ordered index table.
- A "pruned" versus "never existed" response distinction is an additive
  response arm plus a typed error, following the history-incarnation
  precedent.
- **Constraint recorded:** durable event payloads MUST NOT be pruned at or
  behind the slowest projection frontier. Commit materialization rehydrates
  events by reference and treats a missing event row as corruption; any
  future retention policy must fence on the minimum projection frontier.

## Consequences

- The plateau number is published as a per-database characteristic beside the
  typed saturation contract.
- Scale-out design work targets multi-database operation, routing, and
  aggregation — never intra-database order splitting.
- The idempotency table's insert pattern is a known, accepted cost.
- Retention remains deferred with a recorded, affordance-complete path.

## Rejected alternatives

- **Reserve lane/partition discriminants on sequence-bearing APIs.** Complicates
  every client for a capability the multi-database model makes unnecessary.
- **Idempotency key v2 with locality prefix.** An identity migration of the
  retry contract itself, for a moderate B-tree win obtainable additively.
- **Defer these decisions.** Each would otherwise resurface as an innocent
  optimization with breaking consequences.

## Acceptance

Pending maintainer acceptance. Doc-only; blocks no package.
