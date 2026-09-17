---
adr: "0235"
title: Retire Fresh Locator Coverage Once The Derived Index Covers
status: superseded
tier: guarantee
date: 2026-09-17
accepted: 2026-09-17
acceptance: 'maintainer, in session, 2026-09-17: "Okay let''s more forward with
  the proposal" (ADR-0235 as written)'
requires: [ADR-0102, ADR-0183, ADR-0197, ADR-0234]
amends:
  - ADR-0183 only to admit the single enumerated repair below.
  - ADR-0197 only to bound how long coverage is maintained, without changing
    what it proves while it is maintained.
superseded_by: [ADR-0236]
requirements: []
packages: [WP-786]
obligations:
  - id: OBL-0235-1
    package: WP-786
    proof: coverage_retires_once_the_derived_index_covers_the_captured_frontier
    says: On the first seal whose captured frontier the command-derived index
      already covers, coverage retires and no later seal in that process
      performs the queued witness. A process whose index has not covered
      arms and maintains coverage exactly as before.
  - id: OBL-0235-2
    package: WP-786
    proof: retired_coverage_is_distinguishable_from_failure_disabled_coverage
    says: Retirement and proof failure are separate terminal states. Every
      malformed span, count, ordinal, manifest entry, locator, capsule identity
      and audit pairing still disables by failure, still fences where it fences
      today, and is never reported as retirement.
  - id: OBL-0235-3
    package: WP-786
    proof: cold_fresh_database_publications_complete_without_history_scans
    says: A fresh process over a database with retained history still completes
      its publications with zero history fallback scans and zero transient
      index rebuilds, because coverage is maintained for exactly as long as the
      derived index cannot answer absence.
review_triggers:
  - Coverage would retire while the command-derived index does not cover the
    captured frontier, or while the transient index is dormant.
  - Retirement would occur on any path that today disables coverage because a
    proof failed, or would suppress a write fence.
  - The absence answer for a novel identity would change, or an operational
    miss would be answered from anything other than an exact covering proof.
  - A durable byte, storage key, encoding, transaction ordering, acknowledgement
    or public surface would change.
---
# ADR-0235: Retire Fresh Locator Coverage Once The Derived Index Covers

## Context

A bisect on the E2 bench host attributes a 2026-09-05 write-path regression to
`89f2e78bcf`, which moved ADR-0197's queued coverage witness inside the journal
runtime guard and ahead of `lane.submit`. That commit added no work; it moved
existing work onto the critical path. Measured on E2, write-only smoke, mean
group-commit time over two interleaved rounds at 1, 8 and 32 clients:

| arm | c=1 | c=8 | c=32 |
|---|---|---|---|
| main | 1739 us | 2971 us | 6572 us |
| witness skipped | 1234 us | 1999 us | 4747 us |
| coverage disabled every seal | 1308 us | 2054 us | 4590 us |

The witness costs 29 to 33 percent of every group commit. Disabling coverage
outright is 25 to 31 percent faster with 16 to 21 percent more throughput, and
costs nothing measurable: both arms report zero transient index rebuilds and a
clean-certificate startup.

The reason is in the admission path. The bounded history scan coverage exists
to avoid is the last resort, not the first. Ahead of it sits a second absence
proof against the command-derived index, and once that index covers the
captured frontier it answers every absence coverage would have answered. From
that point the witness maintains a proof nothing consults.

That claim is measured, not inferred. A counting build of the same write-only
smoke arm, eight clients over twenty seconds on a fresh database reaching
readiness by clean certificate, reports which branch answered every absence:

| branch | count |
|---|---|
| operational absence resolutions | 60901 |
| answered by coverage | 0 |
| answered by the command-derived index | 60868 |
| answered by checkpoint equality | 32 |
| bounded history fallback scans | 0 |
| admission misses reaching coverage | 29099 |
| admission misses coverage allowed | 0 |

The dormant transient index a bounded clean-close start leaves behind is a
startup state, not a steady state: the first operational use arms it, after
which the derived branch answers. Coverage answered nothing in either path.

The same build also counts the coverage state each seal observed: of 7503
seals, 7503 found coverage already disabled and none found it armed. Coverage
disables itself on the first admission miss whose private stamp does not match
and never re-arms within a process, so in steady state the seal is not merely
maintaining a proof nothing consults, it is computing an exactness proof to
feed a state machine that has already terminally disabled itself.

## Decision

1. Admit this one repair as a narrow amendment to ADR-0183, on the ADR-0234
   pattern. No sealed package, banked baseline, threshold or permitted closure
   changes. Register the implementing non-PERF package after acceptance, with
   exact allowed paths and the proofs below, before implementation begins.
2. Amend ADR-0197 so coverage is maintained only while it can answer something
   the command-derived index cannot. What coverage proves while maintained,
   and every fact Decision 6 enumerates, are unchanged.
3. At each queued seal, before any witness work, the seal asks whether the
   command-derived index already covers the captured frontier, by the same
   predicate the admission path uses. If it does, coverage retires: the seal
   performs no witness, advances no chain, and no later seal in that process
   performs one.
4. Retirement is a distinct terminal state from failure. Coverage today has one
   terminal state reached by proof failure, which fences on the paths that fence
   today. Retirement reaches a separate terminal state, reports separately, and
   fences nothing. No condition that disables by failure may report retirement.
5. A process whose derived index has not covered is unchanged. It arms from
   exact empty authority, maintains the chain seal by seal, and answers
   operational misses from its published proof exactly as today. That is the
   fresh-process case ADR-0197 exists for, and it keeps its full benefit.
6. Retirement is irreversible within a process, as disabling is today. It is
   safe because it is reachable only when the index covers, and because every
   site that invalidates the transient index also fences writes; one of those
   sites already disables coverage alongside. A store whose index has gone
   invalid has stopped accepting writes, so coverage answering absence there
   buys nothing.
7. No durable byte, storage key, encoding, transaction ordering, conflict
   ownership, acknowledgement, outcome sequencing or public surface changes.

## Options considered

1. **Make the witness cheaper by proving at staging.** Mint fixed-size evidence
   in the walk that writes the locator rows and reduce the seal to a chain
   check. Sound, and it keeps the pre-submit ordering, but it optimises work
   that in steady state should not happen at all, and it is a far larger change
   to a guarantee-tier proof system. Rejected in favour of not doing the work.
2. **Remove only the witness's redundant reads.** Resolving its locator reads
   from the staged composite mutations, an input Decision 6 already allows,
   measured 4 to 7 percent and needs no record. It remains worth taking for the
   window where coverage is still maintained, and is complementary to this
   decision rather than an alternative.
3. **Move the proof after the journal submit.** Recovers most of the cost but
   reverses a deliberate guarantee-tier safety decision, and retirement obtains
   more without touching the ordering.
4. **Retire fresh-locator coverage entirely.** Tempting on these numbers, but
   the measurement starts from an empty database, so the derived index covers
   everything the process wrote. The fresh-process-over-retained-history case
   is not exercised here and is exactly what ADR-0197 was built for. Rejected
   as unproven.

## Consequences

- Steady-state group commit stops paying for a proof nothing consults. The
  measured ceiling is the coverage-disabled arm; no speedup is claimed until
  the implemented change is measured.
- Coverage becomes a startup-window mechanism rather than a permanent one,
  which is what its name and its arming rule already describe.
- A process that retires coverage and later loses its derived index answers
  absence no worse than one that disabled coverage by failure, and only after
  writes are already fenced.
- Two terminal states must be told apart in evidence and tests, which is new
  surface in the coverage state machine.
- Skipping the witness also stops running the structural checks it performs on
  the way to its answer: retained command counts against the sealed count, span
  endpoints against the frame, and segment adjacency, seven of which fail the
  seal outright today. They are incidental to coverage but load-bearing while
  they run. Retirement must not be the only thing standing between a malformed
  staged segment and a journal submit, so the implementing package has to
  establish that each check is either redundant with a check that still runs or
  is preserved independently of coverage.
- Because coverage is already disabled at every steady-state seal, a predicate
  narrower than Decision 3 would capture the same measured win: skipping the
  witness when coverage is already disabled needs no new terminal state and no
  amendment to ADR-0197. It is not taken here because Decision 3 is accepted
  text and this record does not change accepted text on its own; it is recorded
  so the maintainer can choose it.

## Standing design tests

- **Interface safety:** no public, operator, agent, SDK, transport or
  configuration surface changes. Nothing can arm, retire, observe, extend or
  bypass coverage, and retirement is not selectable.
- **Scale:** retirement removes per-command seal work in steady state and adds
  one predicate evaluation per seal. No new retained state, and no key,
  locator, segment or history collection is introduced.

## Checks

- `coverage_retires_once_the_derived_index_covers_the_captured_frontier`
- `retired_coverage_is_distinguishable_from_failure_disabled_coverage`
- `cold_fresh_database_publications_complete_without_history_scans`, unchanged
- `grouped_fresh_locator_seal_resolves_the_durable_segment_once`, unchanged
- Every existing `fresh_locator_*` unit test, unchanged
- `./scripts/check-adr-obligations` and `./scripts/check-performance-freeze`
