---
adr: "0236"
title: Remove Fresh Locator Coverage
status: proposed
tier: guarantee
date: 2026-09-16
accepted: null
acceptance: null
requires: [ADR-0102, ADR-0183, ADR-0197, ADR-0234, ADR-0235]
amends:
  - ADR-0183 only to admit the single enumerated repair below.
supersedes: [ADR-0197, ADR-0235]
requirements: []
packages: []
obligations:
  - id: OBL-0236-1
    package: WP-787
    proof: absence_answers_are_unchanged_without_fresh_locator_coverage
    says: For every identity, startup mode and index state, the admission and
      operational absence answers equal the answers the same inputs produce
      today. Removal changes which proof answers, never what is answered.
  - id: OBL-0236-2
    package: WP-787
    proof: queued_seal_structural_checks_survive_coverage_removal
    says: Each structural check the queued witness performed is enumerated and
      either still runs on every seal or is proven redundant with a check that
      does. Retained command count against sealed count, span endpoints against
      the frame, and segment adjacency are named individually.
  - id: OBL-0236-3
    package: WP-787
    proof: bounded_history_fallback_remains_the_only_absence_backstop
    says: A fresh process over retained history completes its publications with
      the bounded history fallback as the sole backstop, with the scan count and
      the rows each scan covers reported as evidence and bounded by the span
      between the checkpoint frontier and the captured frontier.
review_triggers:
  - An absence answer for any identity would change, or the bounded history
    fallback would be reached on a path that does not reach it today.
  - A structural check the queued witness performed would stop running without
    a named check that covers it.
  - The bounded history fallback would scan an unbounded span, or its evidence
    counter would stop being reported.
  - A durable byte, storage key, encoding, transaction ordering, acknowledgement
    or public surface would change.
---
# ADR-0236: Remove Fresh Locator Coverage

## Context

ADR-0197 gave a fresh process a way to answer idempotency absence without a
bounded history scan. It arms from exact empty authority, then maintains an
affine witness chain seal by seal, so a later miss can be answered from a
published proof instead of a scan. ADR-0235 then decided to retire that chain
once the command-derived index covers the captured frontier, because measurement
showed the witness costs 29 to 33 percent of every group commit.

Instrumentation since taken shows the mechanism never arms in the running
server, so neither the benefit ADR-0197 claims nor the retirement ADR-0235
designs applies to anything. A counting build of the write-only smoke arm,
eight clients over twenty seconds, reports the following over the load daemon.

| observation | count |
|---|---|
| arming decisions reached, any daemon | 0 |
| lookups finding coverage uninitialized | 1 |
| seals finding coverage disabled | 8788 of 8788 |
| absence resolutions answered by coverage | 0 |
| absence resolutions answered by the derived index | 80970 of 81003 |
| bounded history fallback scans | 0 |

Three facts explain it, and they compound. ADR-0197 Decision 1 names the grouped
admission entry as the sole initialization site, and no production path calls
that entry; every caller is a test. The live command pipeline writes through the
ADR-0104 composite stage, which carries no writer-private redb transaction, and
Decision 1 excludes that stage by name, so the proof cannot be evaluated where
the writes happen. The proof also requires every authority table empty and the
transient index dormant, and a first start over a fresh database rebuilds the
index to ready while a start that leaves it dormant follows a clean close and so
has commits. The production call additionally discards the boolean that reports
whether arming succeeded, which is why a mechanism that never armed raised no
signal.

## Decision

1. Remove fresh-locator coverage. The coverage state machine, its affine witness
   roles, the queued witness at seal, the arming entry and the two consumption
   predicates all leave the codebase, together with the architecture rules that
   exist only to guard them.
2. The absence proofs that remain are unchanged in content and order. Derived
   member lookup, then the exact durable locator point read, then the
   command-derived index coverage predicate, then the bounded history fallback
   scan. Only the coverage short-circuit between the locator read and the
   coverage predicate is removed.
3. The bounded history fallback becomes the sole backstop. It is unchanged. It
   scans from the successor of the checkpoint application frontier to the
   captured frontier, and the implementing package must evidence both how often
   it runs and how many rows it covers.
4. Every structural check the queued witness performed is enumerated and either
   preserved independently of coverage or proven redundant with a check that
   still runs. Retained command count against sealed count, span endpoints
   against the frame, and segment adjacency are decided one by one, not in bulk.
   No check may stop running merely because the witness that hosted it is gone.
5. Supersede ADR-0197 and ADR-0235. ADR-0235's measurements stand and are
   carried forward; its decision does not, because retirement presumes a
   mechanism that arms.
6. Admit this one repair as a narrow amendment to ADR-0183, on the ADR-0234
   pattern. No sealed package, banked baseline, threshold or permitted closure
   changes. Register the implementing non-PERF package after acceptance, with
   exact allowed paths and the proofs above, before implementation begins.
7. No durable byte, storage key, encoding, transaction ordering, conflict
   ownership, acknowledgement, outcome sequencing or public surface changes.

## Options considered

1. **Remove coverage.** Selected. A mechanism that cannot arm on any path the
   server takes is not answering anything, and the fallback it exists to avoid
   did not run once in the measured arm. Removal is the only option that stops
   paying without first building something whose benefit is unmeasured.
2. **Redesign arming so the live path can arm.** Rejected as unproven. It
   requires an exact empty-authority proof constructible from the composite
   stage, which ADR-0197 Decision 1 excluded deliberately, and it would restore
   a mechanism whose benefit has never been observed. If the fallback later
   proves costly, this is the option to reopen, on evidence.
3. **Retire once the derived index covers, per ADR-0235.** Superseded. It keeps
   the state machine, the affine roles and a new terminal state in order to stop
   maintaining a proof that never armed. The simpler removal obtains the same
   measured win with less surface.
4. **Make a refused arming loud and leave the mechanism.** Rejected. It pays the
   full per-seal cost to observe a gap this record already establishes, and the
   observation would only justify one of the options above.

## Consequences

- Steady-state group commit stops paying for the queued witness. The measured
  ceiling is the coverage-disabled arm, 25 to 31 percent faster group commit
  with 16 to 21 percent more throughput. No speedup is claimed until the
  implemented change is measured.
- Absence answers do not move. Coverage answered none of 81003 resolutions, so
  removing it removes a branch that never fired.
- The bounded history fallback carries the whole backstop. Measurement shows it
  scanning nothing under continuous checkpointing, but that is a property of the
  workload, not a guarantee, so obligation three evidences the span rather than
  asserting it is small.
- The affine witness discipline leaves the codebase. That discipline is a real
  asset, and losing it is the genuine cost of this record. It is accepted
  because the discipline currently guards a state machine that never arms.
- A defect is recorded rather than repaired. If the fresh-process case ADR-0197
  was built for matters later, it will have to be rebuilt on evidence that it
  matters, which this record does not have.

## Standing design tests

- **Interface safety:** no public, operator, agent, SDK, transport or
  configuration surface changes. Nothing becomes selectable, observable or
  bypassable that was not already.
- **Scale:** removal deletes per-command seal work and adds none. The remaining
  fallback stays bounded by the span between the checkpoint frontier and the
  captured frontier, which continuous checkpointing keeps short.

## Checks

- `absence_answers_are_unchanged_without_fresh_locator_coverage`
- `queued_seal_structural_checks_survive_coverage_removal`
- `bounded_history_fallback_remains_the_only_absence_backstop`
- `cold_fresh_database_publications_complete_without_history_scans`, retired
  with the mechanism it tests and replaced by obligation three
- Every existing `fresh_locator_*` unit test, retired with the mechanism
- `./scripts/check-adr-obligations` and `./scripts/check-performance-freeze`
