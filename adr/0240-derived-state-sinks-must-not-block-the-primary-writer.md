---
adr: "0240"
title: Derived State Sinks Must Not Block The Primary Writer
status: accepted
tier: guarantee
date: 2026-09-19
accepted: 2026-09-19
acceptance: 'maintainer, in session, 2026-09-19: "Approved" (ADR-0240 as written);
  amended 2026-09-20, maintainer, in session: "i approve" (Amendment 1)'
requires: [ADR-0010, ADR-0017, ADR-0093, ADR-0171, ADR-0239]
amends:
  - ADR-0010 by requiring that projection application hold no lock the primary
    writer needs, rather than leaving the coupling to the storage layer.
supersedes: []
requirements: []
packages: [WP-791]
obligations:
  - id: OBL-0240-1
    package: WP-791
    proof: no_derived_state_path_acquires_the_primary_mutation_gate
    says: No derived-state apply path reaches the primary store's exclusive
      mutation gate, checked against the source rather than by convention.
  - id: OBL-0240-2
    package: WP-791
    proof: derived_state_disagreeing_with_the_primary_is_rebuilt_not_trusted
    says: Derived state that claims commits the primary store does not have is
      discarded and rebuilt rather than served.
  - id: OBL-0240-3
    package: WP-791
    proof: declaring_a_projection_costs_no_write_throughput
    says: A contract declaring a projection sustains write throughput within the
      bench host's run-to-run spread of one that declares none, where that spread
      is the range of the baseline variant across repetitions of the same run as a
      percentage of its mean. A declaration-only control measured beside it is the
      sharper instrument and is reported alongside, but does not set the bar.
review_triggers:
  - A derived-state path would acquire the primary store's mutation gate, or any
    lock a command commit waits on.
  - Derived state would be treated as authoritative, or served when it disagrees
    with the primary store.
  - A sink's availability, latency or backpressure would become able to fail or
    delay a command commit.
  - The frontier would stop being the record of how far derived state has
    consumed, or ordering would be relaxed along with synchrony.
---
# ADR-0240: Derived State Sinks Must Not Block The Primary Writer

## Context

RiffDB's projections exist so one database can serve the transactional and the
analytical shape of an application: the role Postgres and ClickHouse play
together today, plus a search index whose staleness is part of its contract.
Three mechanisms carry that: event-derived projections, columnar projections,
and tokenized text indexes. All three are derived, all three are rebuildable
from commits and events, and all three are stale by contract, with the frontier
recording how far behind they are.

They have arrived at three different couplings to the primary store. Tokenized
text touches it not at all. Columnar keeps bulk data outside it but writes its
control records through the primary store's exclusive gate. Event-derived
projections take that gate for the whole apply, and measurably: on the C3D bench
host a contract declaring one projection sustains 34 percent of the write
throughput of an identical contract without one. Idling the apply restores it to
between 99.5 and 101.5 percent, so the entire cost is where the work happens
rather than what it is. Making the apply cheaper recovered 92 percent and left
the cost at 62; polling it less often, and relaxing its durability, recovered
nothing. `docs/performance/perf-surface-mechanism-costs-2026-09.md` carries the
measurements.

## Decision

1. Derived state is not authoritative. It is rebuildable from commits and
   events, and its safety guarantees are those of a cache, not of the primary
   store.
2. A derived-state apply must not acquire the primary store's exclusive
   mutation gate, or any other lock a command commit waits on. This is a
   structural requirement, not a performance target: it must hold when a sink
   is slow, backpressured, or unavailable.
3. Derived state is written outside the primary store's write transaction.
   Reads of commits and events that feed an apply use a read transaction, which
   the primary writer does not wait for.
4. Ordering is unchanged. `TransactionallyOrdered` constrains the order in
   which a projection consumes commits; it does not require the projection to
   be synchronous with them, and the frontier remains the record of lag.
5. On any disagreement between derived state and the primary store, the derived
   state is discarded and rebuilt. It is never served in preference to the
   primary, and it is never evidence that a commit occurred.
6. This applies to every derived-state mechanism, including ones whose target
   is not first-party storage. A sink's availability must not be able to fail a
   commit.

## Standing design tests

- **Interface safety (AGENTS.md boundary 11):** no. An application cannot
  express a projection that blocks its own writes, because the coupling is a
  property of the apply path rather than of anything a contract declares. The
  decision removes expressibility rather than adding it: no declaration, and no
  sink, can put itself on the command commit path.
- **Scale:** no, and it is the point. The current coupling means analytical
  derived state is bounded by the transactional writer's exclusive gate, which
  forecloses running both shapes in one system at scale. Decoupling removes
  that ceiling. It assumes no co-located storage; a sink may be a separate
  file, a separate store, or remote.

## Consequences

Derived state may be stale, torn, or absent after a crash, and is repaired by
rebuild rather than by a commit protocol spanning two stores. Backup and
restore treat it as rebuildable rather than as authoritative bytes, which is
what it already is.

A sink that falls far behind consumes space and serves increasingly stale
answers. That is visible through the frontier, and is the trade the contract
already makes; it is not a new failure mode, but decoupling makes it reachable
in cases where contention previously throttled the writer instead.

The rebuild path becomes load-bearing. It exists today as `AllocateRebuild` and
`StartInitialScan`, and moves from a recovery convenience to the mechanism that
makes the safety argument work.

## Options considered

Keeping projected state in the primary store and optimising the apply was
measured and rejected: the cheapest available decode saved 92 percent of the
apply's own cost and moved the throughput cost from 80 to 62 percent, because
the cost is the exclusive gate rather than the work.

Relaxing the apply's durability, on the argument that rebuildable state needs
none, was measured and rejected: it saves 0.26 ms of a 4 ms hold and no
throughput. Rebuildability cannot be cashed in while the writers share a
database.

Reducing apply frequency was measured and rejected: it defers work rather than
removing it, and once the measurement waits for the projection to catch up the
gain disappears.

Moving only the apply's reads out of the exclusive section was rejected without
measurement, because it leaves the sink's write on the shared gate and so fails
requirement 2 even where it would measure well.

## Checks

- `no_derived_state_path_acquires_the_primary_mutation_gate` reads the source
  and fails if an apply path reaches the gate, so the requirement cannot decay
  into convention.
- `derived_state_disagreeing_with_the_primary_is_rebuilt_not_trusted` plants a
  derived store claiming commits the primary does not have and requires a
  rebuild rather than a read.
- `declaring_a_projection_costs_no_write_throughput` holds the boundary where a
  unit test can hold it: an apply batch takes no primary mutation-gate ticket,
  and a sidecar write held open does not delay a command commit. It does not
  compare throughput, and no unit test can. The throughput claim is a bench-host
  measurement, carried by OBL-0240-3 and recorded in
  `docs/performance/wp-791-derived-sinks-c3d-2026-09.md`.

## Amendment 1 — proof description, named estimator, and OBL-0240-3 discharged (Accepted 2026-09-20)

The maintainer accepted this exact text on 2026-09-20. Two corrections and one
discharge.

The description of `declaring_a_projection_costs_no_write_throughput` said the
proof runs the perf-surface contract with and without a projection and requires
the two to agree. It never did: it asserts gate tickets and non-blocking, which
is the part a unit test can own. `check-adr-obligations` did not catch the
mismatch because it verifies that a test of that name exists, not that the test
does what the record says. The description is corrected above.

"The bench host's run-to-run spread" was not defined, and the measurement it
gates turns on which spread is meant. It is now named in the obligation: the
baseline variant's range across repetitions.

OBL-0240-3 is discharged on the C3D measurement of 2026-09-20, twelve
repetitions at 32 clients on revision `c29722f1e`. Declaring a projection cost
-80.4%, -79.9% and -81.0% of write throughput on this host when this record was
written; it costs 3.27% on average now, against a baseline run-to-run spread of
3.5%, and the -33% one-client cost is gone. Inside the bar this record sets.

A declaration-only control measured beside it disagrees, and the record says so
rather than leaving it to be rediscovered: `tokenized_text` does no work and
sits at -0.64%, so the projection cell is distinguishable from free by 2.62
points at t = -3.6 on about 22 degrees of freedom. About 2.6 points of real
residual cost remain, 96% of the original coupling having been removed. The
residual is named as outstanding in WP-791's closure and is not held against
this record.
