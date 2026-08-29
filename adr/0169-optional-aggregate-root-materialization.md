# ADR-0169: Optional Aggregate Root Materialization

- **Status:** Accepted
- **Direction approved:** 2026-08-29
- **Exact text accepted:** Yes, 2026-08-29
- **Accepted:** 2026-08-29
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Claude Code session for drafted commit `a448af6d`
- **Decision deadline:** Before an application ships whose aggregate root no
  command can create
- **Requires:** ADR-0094 and ADR-0129
- **Amends if accepted:** Nothing; this states an invariant that was never
  written down
- **Defines or blocks:** Nothing

## Context

A contract declares an aggregate with a root and children:

```riffql
aggregate Tuples {
  root TuplePartition
  child TupleState
  child TupleChange
  partition_by store_id
  conflict_key (store_id)
}
```

The OpenFGA adapter reached a state where no command creates a
`TuplePartition`. Its previous `EnsureTuplePartition` command was a durable
no-op on every write after the first, and removing it collapsed a seven-round-
trip write to two. The complete upstream conformance suite — 149 tests
covering stores, tuples, changelog, models, and assertions — passes with zero
root rows in the database. `TupleState`'s key is self-contained and does not
reference the root.

Nothing in the language, the compiler, or SPEC says whether that is legal. The
declaration reads as though a root is structural, and an author removing the
only command that creates one gets no signal of any kind.

There is evidence the absence is intended rather than merely tolerated.
ADR-0094 permits the FIFO writer to share one aggregate conflict lease among
commands whose "writes are input-keyed creates of child entities in that
aggregate, and the plan has no root or existing-row mutation." Child appends
that never touch a root are a recognised and optimised case. Materializing a
root on every write would forfeit exactly that optimisation, because the root
mutation would make the lease unshareable.

So the question is not whether a root may be absent during normal operation —
ADR-0094 already assumes it can be untouched. The question is whether an
aggregate whose root *no command can ever create* is a well-formed contract.

## Proposed Decision

An aggregate root need not be materialized. A contract in which no command
creates the root entity is well-formed, and a partition whose root row does not
exist is a valid state for every path that reads, exports, backs up, or
retains that aggregate.

The root declaration names the aggregate's identity and partition derivation.
It does not assert that a row exists.

Proposed SPEC text, added to the aggregate section:

> An aggregate root names the aggregate's identity and partition derivation.
> It is not required to be materialized: a contract MAY omit any command that
> creates the root entity, and child entities MAY be created, read, exported,
> and retained in a partition whose root row does not exist. A command that
> mutates the root forfeits the ADR-0094 shared-lease optimisation for child
> appends in that aggregate, so materializing a root solely to make it present
> is a cost with no corresponding guarantee.

## Options Considered

**Require every root to have a creating command.** Diagnose an aggregate whose
root no command can create. This restores the invariant the declaration
implies. Rejected because it would reject a contract that demonstrably works,
force a durable command per store back into the OpenFGA adapter's write path,
and contradict ADR-0094's premise that child appends need not touch a root.

**Leave it unstated.** Rejected. The current position is that an author makes
this choice without knowing a choice exists, which is the failure mode this
review was commissioned to find.

## Consequences

One invariant that readers might reasonably assume is explicitly denied, which
is more useful than leaving it ambiguous. Contracts may declare a root purely
for identity and partition derivation, which is what several already do.

This record states the rule; it does not verify every consumer. The conformance
suite exercises reads, writes, changelog, and restart, but export, backup, and
retention over a rootless partition are asserted by this record rather than
demonstrated by it. If any of those paths turns out to assume a root row, that
is a defect in the path under this decision, not a reason to require the row.

## Compatibility

None. No existing contract becomes invalid; a rule that was never enforced
becomes stated.

## Security

None. Root presence does not participate in authorization, partition routing,
or conflict ownership.

## Standing Design Tests

- A contract whose aggregate root has no creating command compiles, locks, and
  deploys.
- Child entities created in a partition with no root row are readable and
  survive a close and reopen.

## Testing

The OpenFGA adapter's 149-test upstream conformance run against a live service
with no `TuplePartition` rows. A first-party arm asserting export and retention
over a rootless partition is not yet written and is the natural follow-on.

## Requirements and Work Packages

Adds to SPEC's aggregate section. No work package.

## Decision Deadline

Before an application ships whose aggregate root no command can create.
