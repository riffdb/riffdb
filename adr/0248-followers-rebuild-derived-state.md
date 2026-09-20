---
adr: "0248"
title: Follower Projection Control Without A Replicated Frontier
status: accepted
tier: guarantee
date: 2026-09-20
accepted: 2026-09-20
acceptance: 'maintainer, in session, 2026-09-20: "I approve", after the record was
  rewritten in response to "is there a better option? i assume this is only
  after an unsafe shutdown?", which corrected the diagnosis (ADR-0248 as
  written)'
requires: [ADR-0240, ADR-0246, ADR-0017]
amends:
  - ADR-0240 by naming what supplies a follower's projection control signal,
    which its decision 3 removed from the changelog without replacing.
supersedes: []
requirements: []
packages: [WP-791]
obligations:
  - id: OBL-0248-1
    package: WP-791
    proof: a_follower_reaches_source_derived_state_without_a_replicated_frontier
    says: A follower converges on the same derived rows as its source using the
      active catalog and its own durable frontier, with no projection control
      replicated to it.
review_triggers:
  - Derived rows or derived control would be shipped to a follower rather than
    computed there.
  - A follower would rebuild a projection from empty for any reason other than
    absent or torn local derived state.
---
# ADR-0248: Follower Projection Control Without A Replicated Frontier

## Context

ADR-0240 decision 3 moved derived state to a sidecar, and seven replication
tests began failing. The first reading of that failure was that followers had
been receiving derived state and no longer could. **That reading was wrong, and
it is recorded here because it nearly bought an expensive answer to a cheap
problem.**

A follower has always computed its own derived rows.
`replication_bootstrap.rs:420 replay_projection_commit` reads the follower's own
apply snapshot and folds forward from commits the follower already holds. No
projection row has ever crossed the wire.

What crossed the wire was the **control signal**, not the state.
`replication_projection_tail.rs:76` scanned replication frames for
`ProjectionFrontier` mutations to learn which projections exist, which
generation is current, and what position to catch up to. Those mutations existed
only because control was written through the primary write transaction, and
ADR-0240 decision 3 stopped producing them.

So the question is narrow: where does a follower get the projection list and the
target position, given that it computes the rows itself either way.

## Decision

1. A follower derives its projection control from **the active catalog** — which
   projections the bundle declares — and from **its own durable history
   frontier** — how far it may catch up. Neither is replicated projection state;
   both are already present on the follower.
2. **No derived rows and no derived control are shipped to a follower.** This is
   not a new restriction. It is what was already true of the rows, now also true
   of the control.
3. A follower's projection generation and lifecycle are **its own**, not its
   source's. Two followers of one source may sit at different positions, and
   that is correct: derived state is local, and the frontier is the record of
   how far each has come.
4. A follower rebuilds a projection from empty **only when its local derived
   state is absent or torn** — a fresh bootstrap, or an unsafe shutdown that
   lost the sidecar. Steady-state following remains incremental, unchanged from
   before this record.

## Options considered

**Dual-write the frontier through the primary so it reaches the changelog again**
was tried and refused itself: it re-acquired the primary mutation gate, which the
gate-ticket proof caught at 0 to 16, and produced `CorruptHistory`. ADR-0240
decision 2 forecloses it regardless of the error.

**A gate-free frontier notification in the primary changelog** was rejected as
unnecessary once the rows were understood to be local. It would need a primary
write path that takes no gate and orders against command commits, which does not
exist, to carry a signal the catalog already implies.

**Replicating the sidecar as its own stream** was rejected for the same reason,
at higher cost: a second ordering problem between two stores sharing no
transaction, to ship state the follower can compute.

**Having every follower rebuild from empty on promotion** was considered when the
failure was misread as lost rows, and is rejected. It would have been a real
regression — cold projections after promotion, and rebuild work multiplied across
a fleet — bought for a problem that did not exist. Decision 4 states the actual
condition instead.

## Consequences

Steady-state replication is unchanged: a follower keeps its projections current
incrementally, and a promoted follower serves the derived state it had already
built. There is no cold-promotion regression, and no additional rebuild work.

What is genuinely lost is the source's exact control values. A follower's
generation and lifecycle are now its own rather than a copy, so a test that pins
a follower's control row to its source's is pinning a mechanism this record
retires. The seven failing tests are rewritten to assert that a follower
**converges on** the same derived rows, which is the property that matters, not
that it observes the same control bytes.

Promotion RPO statements are unaffected for authoritative records. Derived state
carries no replication guarantee and never did; the frontier reports the lag.
