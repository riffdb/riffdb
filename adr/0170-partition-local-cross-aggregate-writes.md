# ADR-0170: Partition-Local Cross-Aggregate Writes

- **Status:** Accepted
- **Direction approved:** 2026-08-29
- **Exact text accepted:** Yes, 2026-08-29
- **Accepted:** 2026-08-29
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Claude Code session, on the measurement recorded under "Evidence"
- **Decision deadline:** Before an application ships whose write path was split
  into a saga solely to satisfy `RDB-C017`
- **Requires:** ADR-0094
- **Amends:** SPEC `PERF-005` and the `RDB-C017` diagnostic
- **Defines or blocks:** Nothing

## Context

`RDB-C017` refuses a command whose `create` and `mutate` bindings span two
aggregate roots. A checkout that decrements inventory and writes an order must
therefore be split into two commands and a compensating action, even when both
aggregates declare the same `partition_by` and the write is one local
transaction on one node.

The rule's stated justification, in the scaffold's `AUTHORING.md`, is that
"RiffDB does not turn cross-aggregate writes into an implicit distributed
transaction." That reasoning is sound for a write that crosses a partition. It
does not apply to two aggregates that derive the same partition route.

Three facts narrow what the restriction is actually protecting:

- Multi-key atomicity is already implemented. One `bulk command` mutates up to
  32 distinct `(tenant_id, product_id)` conflict keys in a single commit.
  Holding several leases and committing once is not a new capability.
- The restriction is on writes only. A command may already `read` another
  aggregate in the same partition.
- The same physical write is expressible today by placing both entities under
  one root. The compiler does not forbid the transaction; it forbids combining
  a fine-grained conflict key with cross-entity atomicity.

So the rule is a modelling constraint, not an engine limitation, and the
author is never told that a trade is being made.

## Evidence

The trade was measured on 2026-08-29: a release daemon on the workstation, one
tenant, three-line checkout, each concurrent client holding a **disjoint**
product triple so the fine-grained conflict key had every opportunity to pay
off. Median of three runs, throughput in operations per second. The harnesses
are preserved in `examples/checkout-comparison`.

| contract | c=1 | c=8 | c=32 | scaling |
|---|---:|---:|---:|---:|
| one aggregate, `conflict_key (tenant_id)`, one commit | 483 | 2377 | 5259 | 10.9× |
| two aggregates, fine conflict keys, two commits | 244 | 1380 | 2559 | 10.5× |
| Postgres, one transaction | 599 | 2983 | 5833 | 9.7× |

Two results matter, and both are RiffDB against RiffDB on identical footing.

**The fine-grained conflict key bought no measured scalability.** 10.9× against
10.5× is indistinguishable. Clients sharing one coarse conflict domain scaled
as well as clients sharing none, because group commit amortizes the flush
across concurrent writers whether or not they contend on a lease. This is the
second refutation of the same premise: the OpenFGA suite's `conflict_key
(store_id)` scaled 7.2× at c=32 with same-store and distinct-store writes
performing identically.

**The split cost roughly 2× throughput at every concurrency** and doubled p50
latency (5.6 ms against 9.9 ms at c=32). Most of that is the second durable
commit; the remainder is three additional reservation rows and a second round
trip.

The Postgres column is context rather than evidence for this record, and it is
stated here because an earlier draft of this measurement got it wrong in
Postgres's disfavour: the RiffDB runs each started from a fresh database while
the Postgres control accumulated rows across runs. Corrected, Postgres leads
the single-aggregate contract by 11–26%, consistent with the 1.18× and 1.26×
measured on a single-transaction order insert and on the OpenFGA suite. RiffDB
trails Postgres modestly and consistently on durable write throughput; the
aggregate split doubles that gap.

## Outcome

Implemented and measured. The shape this record unlocks — fine conflict keys
with one atomic commit — was benchmarked on the same harness that justified the
record, and is the best RiffDB configuration measured:

| contract | c=1 | c=8 | c=32 |
|---|---:|---:|---:|
| two aggregates, fine keys, **one** commit | **485** | **2629** | **5740** |
| one aggregate, coarse key, one commit | 483 | 2377 | 5259 |
| two aggregates, fine keys, two commits | 244 | 1380 | 2559 |

It recovers the whole ~2× the split was costing while keeping the per-product
conflict key, and matches or beats the coarse-key contract at every
concurrency. That is the expected result once the premise that a coarse key
serialises writers is refuted: the coarse key was never costing anything, and
the second commit was costing everything. Harness in
`examples/checkout-comparison/fine-atomic`.

## Proposed Decision

A command MAY write entities belonging to more than one aggregate when the
compiler proves that every binding derives the same partition route. The
command's conflict ownership becomes the union of the conflict keys its
bindings derive, which the writer already supports for multiple keys within one
aggregate.

`RDB-C017` continues to reject a command whose bindings derive different
partition routes. That is the case the distributed-transaction argument
actually covers, and it is unchanged.

Proposed SPEC text, replacing the cross-partition mutation rule's second
sentence:

> A command's `create` and `mutate` bindings MUST all derive the same partition
> route. They MAY belong to different aggregates in that partition, in which
> case the command's conflict ownership is the union of the conflict keys its
> bindings derive and the complete union MUST be statically enumerable. A
> command whose bindings derive different partition routes MUST be rejected.

WP-721 implements this at executable IR V19. `RDB-C017` now rejects only a
write whose bindings derive different partition routes, and a command writing
one aggregate is unchanged in every respect including its plan hash. Two
proofs are still outstanding, both tracked by WP-721: deadlock-freedom of the
lease order under an arbitrary acquisition sequence, and crash atomicity of a
command writing two aggregates.

## Options Considered

**Keep the restriction and document the trade.** Already done: the `RDB-C017`
help text now names both repairs and states that placing entities under one
root costs that root's coarser conflict key. This is strictly better than the
previous guidance and is independent of this record. It is insufficient on its
own, because the guidance now points authors at a choice whose concurrency
justification the measurement does not support.

**Keep the restriction to preserve aggregates as a future sharding unit.** This
is the strongest argument against the proposal and the one the maintainer
weighed in accepting it. If aggregates later become the unit of physical
placement, a cross-aggregate write becomes a distributed transaction and this
relaxation would have to be withdrawn — a breaking change to contracts that
adopted it. Whether to spend that option is a roadmap decision, not a
performance one, and it belongs to the maintainer. What this record asserts is
only that the decision should be made on those grounds and stated as such,
rather than defended on concurrency grounds that do not hold.

**Relax to any two aggregates regardless of partition.** Rejected. That is the
implicit distributed transaction the current rule exists to prevent, and
nothing measured here bears on it.

## Consequences

If accepted, an author may express a checkout, a transfer, or any other
write that spans two consistency domains in one partition as one atomic command
with the conflict granularity they actually want, instead of choosing between
atomicity and a fine conflict key.

The writer must acquire a lease set spanning aggregates. Within one aggregate
this is already done; across aggregates it needs a canonical ordering over
`(aggregate, key)` to keep lease acquisition deadlock-free. That ordering is
mechanical but it is real work and it is where the implementation risk sits.

Contracts written against the relaxed rule cannot be deployed to a build that
predates it, and would have to be rewritten if aggregates later become a
placement boundary.

## Compatibility

Additive for contracts. Every contract valid today remains valid; a rejection
becomes an acceptance. The reverse is not true, which is what makes the
sharding option above the substantive question.

Executable IR gains a command whose conflict ownership spans aggregates. That
does not fit the current encoding and requires V19; single-aggregate commands
keep their existing bytes and plan identity, so no deployed contract or lock is
disturbed.

## Security

None. Partition routing is unchanged, and the proposal does not widen what a
command may reach: it may already read any aggregate in its partition, and
authorization is unchanged.

## Standing Design Tests

- A command writing two aggregates that derive the same partition route
  compiles, and its conflict ownership is the union of the derived keys.
- A command writing two aggregates that derive different partition routes is
  still rejected with `RDB-C017`.
- Two concurrent commands whose union conflict sets are disjoint do not
  serialise against each other.
- A command whose union conflict set is not statically enumerable is rejected.

## Testing

The measurement above is reproducible from the two contracts and three
benchmark harnesses used to produce it. It is a latency and throughput
comparison, not a correctness test, and it does not exercise the lease
acquisition this proposal would add.

No test in this repository currently covers a cross-aggregate write, because
none can be expressed. Implementation would need deadlock coverage over the
proposed lease ordering, and a crash-consistency arm proving a command that
writes two aggregates is atomic across a restart.

## Interim

Independent of this record's status, and already implemented:

- The `RDB-C017` help now names both repairs and states the conflict-key cost
  of each, rather than addressing only the shared-route case.
- `AUTHORING.md` explains the trade, and no longer says RiffDB "never" turns
  cross-aggregate writes into a distributed transaction where "does not" is
  accurate.
- `riffdb contract explain` reports each aggregate's conflict-key granularity,
  so an author can see the structure the compiler derived without provoking a
  rejection.

## Requirements and Work Packages

Amends `PERF-005`. WP-721 must deliver the compiler proof, the union conflict
ownership, and a deadlock-free lease ordering across aggregates before any
writer behaviour changes.

The format question this record deferred is settled in
`docs/performance/wp-721-cross-aggregate-conflict-ownership.md`: the current
encoding cannot carry it, because `LocalityPlan` serializes one aggregate and
types every conflict key by it, and that encoding feeds the command plan hash.
It needs executable IR V19 under ADR-0126's least-sufficient writer rule, which
keeps every single-aggregate command's locality bytes and plan hash unchanged.
ADR-0126 requires a separately accepted record before the conflict-ownership
rule changes; this record is it.

## Decision Deadline

Before an application ships whose write path was split into a saga solely to
satisfy `RDB-C017`. The riffdb-openfga and riffdb-better-auth adapters are not
affected: each writes one aggregate per command for reasons independent of this
rule.
