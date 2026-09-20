---
adr: "0250"
title: A Defect Breaker Trips On Rate, Not On Lifetime Count
status: accepted
tier: guarantee
date: 2026-09-20
accepted: 2026-09-20
acceptance: 'maintainer, in session, 2026-09-20: "i approve" (ADR-0250 as
  written, with the amends entry corrected before acceptance)'
requires: []
amends: []
supersedes: []
requirements: []
packages: [WP-801]
obligations:
  - id: OBL-0250-1
    package: WP-801
    proof: a_client_reachable_defect_path_cannot_stop_the_runtime
    says: A repeatable defect reachable from one client's requests does not stop
      the runtime, however many times that client repeats it.
  - id: OBL-0250-2
    package: WP-801
    proof: a_burst_of_process_scoped_defects_still_stops_the_runtime
    says: A burst of process-scoped defects trips the breaker, so narrowing what
      feeds it does not make the runtime fail open.
  - id: OBL-0250-3
    package: WP-801
    proof: the_defect_budget_refills_over_time
    says: The budget refills, so sporadic defects spread across a long uptime
      never accumulate into a shutdown.
review_triggers:
  - A defect class would be added without deciding whether it is request-scoped
    or process-scoped.
  - The breaker would be made to count anything a single caller can drive.
  - A fail-closed path would be added that a request can reach repeatedly.
---
# ADR-0250: A Defect Breaker Trips On Rate, Not On Lifetime Count

## Context

`ProductionServiceDiagnostics::record_internal` retains the last 256 internal
errors. On the 257th it calls `routing.stop(DiagnosticCapacityExceeded)`, which
ends the process. The retention itself is a ring and rotates correctly; the stop
is not about losing diagnostics, it is a circuit breaker on the count.

The count is cumulative over the process lifetime and has no decay. Two very
different situations therefore reach the same wall:

- A daemon accumulates 256 unrelated defects over weeks of uptime and stops,
  though nothing is wrong with it now.
- A client sends 257 requests that hit one defect path in half a minute and the
  process stops for every tenant on it.

The second was observed rather than reasoned about. Deploying a vector contract
into a running daemon registers no columnar source until the process restarts,
so a projected query reaches a port that has never heard of its source and
fails as an internal defect. Repeating that query killed a live daemon on the
bench host: demands 1 through 257 returned `InternalDefect`, demand 258 got
`TransportUnavailable`, and the process had begun its shutdown census. No
privilege was needed and nothing was malformed. It is an ordinary operator
sequence.

No accepted record governs this breaker. The bound, the stop, and the reason
code were introduced in the server's supervision layer and pinned by a test;
nothing decided them, which is why this record is a decision over previously
unrecorded behaviour rather than an amendment to anything.

**A breaker on a lifetime total cannot tell a broken process from a broken
request.** Rate is what separates them, and rate is what the current design
does not measure.

Narrowing the breaker to a rate is necessary but not sufficient, because a
sufficiently fast client still fills any burst allowance. The second half is
that a request-scoped defect should not feed a process-level breaker at all.
Today it cannot be told apart: `InternalError` carries an incident identifier
and a boxed source, and the defect class that produced it -- `Panic`,
`UnterminatedAudit`, `ProofMismatch`, `LowerIntegrity` -- is erased by the time
the diagnostics sink sees it. The sink counts every defect equally because the
type it receives gives it nothing else to count.

## Decision

1. **Internal defects carry their scope.** `InternalError` gains a defect scope,
   set where the defect is raised, with exactly two values. *Process-scoped*
   says the process's own state is in doubt: a contained panic, an omitted
   terminal audit, a lower-integrity failure. *Request-scoped* says this request
   could not be completed and says nothing about the next one.

2. **Only process-scoped defects feed the breaker.** A request-scoped defect
   remains an opaque incident to the caller, a diagnostic retained in the ring,
   and a metric. It never counts toward stopping the runtime. This is what
   closes the denial of service: the client-reachable paths are exactly the
   request-scoped ones.

3. **The breaker trips on rate.** Process-scoped defects draw on a budget with a
   burst capacity and a refill rate, both fixed constants in this record. The
   budget is exhausted by a burst and recovers with time, so sporadic defects
   across a long uptime never accumulate into a shutdown, and a storm still
   stops the process promptly.

4. **Burst capacity is 16 and the budget refills one defect per minute.** A
   process that is genuinely broken produces defects far faster than one a
   minute and trips in seconds; a healthy process that sees one every few hours
   never approaches the wall. These are chosen to be obviously safe rather than
   tuned, and changing them is a change to this record.

5. **Retention stays as it is.** The ring keeps the most recent 256 errors of
   either scope and counts what it drops. Retention and the breaker are separate
   concerns, and conflating them is what produced the defect this record
   repairs.

6. **A tripped breaker is unchanged in effect.** `RuntimeStopReason` keeps
   `DiagnosticCapacityExceeded` and its meaning: the process stops, fails
   closed, and does not attempt to continue.

## Consequences

The existing test `diagnostics_are_bounded_and_overflow_fails_readiness_closed`
pins the behaviour this record changes and is replaced by tests for the three
obligations. That test was not wrong; it pinned a deliberate design, and this
record is what supersedes it.

ADR-0026's fail-closed rule for a failing `IncidentIdSource` is untouched and
stays fail-closed. That rule is about being unable to mint a correlation
identifier at all, which is a different condition from having minted many; this
record deliberately leaves it alone.

`InternalError` is a public type in `riffdb-errors`, so adding scope is a
surface change that every construction site must make deliberately. That cost is
the point: the scope is a judgment about what a defect implies, and a default
would let new defect paths inherit an answer nobody made.

- `a_client_reachable_defect_path_cannot_stop_the_runtime` drives one
  request-scoped defect far past the old bound and past the new burst capacity,
  and requires the runtime to still be routing.
- `a_burst_of_process_scoped_defects_still_stops_the_runtime` drives the burst
  capacity in process-scoped defects and requires the stop, so the narrowing
  cannot be mistaken for failing open.
- `the_defect_budget_refills_over_time` drives the budget to empty, advances the
  clock, and requires the next defect not to trip, against an injected time
  source rather than by sleeping.

What this record does not do: it does not repair the live-deploy path that
exposed the defect. Deploying a vector contract into a running daemon still
registers no columnar source, and a projected query against it still fails as an
internal defect rather than saying so. That is a separate defect in a separate
crate, and after this record it is a request-scoped one.
