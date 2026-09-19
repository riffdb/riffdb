---
adr: "0239"
title: Performance Programme Baseline And Measurement Discipline
status: accepted
tier: guarantee
date: 2026-09-19
accepted: 2026-09-19
acceptance: 'maintainer, in session, 2026-09-19: "Accept all three as written",
  then "Lift it now as a recorded override" for decision 5, which amended this
  record from banking-without-lifting to lifting ADR-0183 early'
requires: [ADR-0171, ADR-0183, ADR-0237, ADR-0238]
amends:
  - ADR-0183 by lifting its performance-package freeze before WP-750 and
    WP-760 close, which that record requires and which its checker rejects
    except for the single enumerated exception named below.
supersedes: []
requirements: []
packages: [WP-790]
obligations:
  - id: OBL-0239-1
    package: WP-790
    proof: scripts/check-performance-freeze
    says: The freeze lifts only under the one enumerated early-lift exception
      naming this record; any other lift while WP-750 or WP-760 is open is
      still rejected as premature.
review_triggers:
  - A performance claim would be made publicly, or in a release note or
    benchmark publication, before the coverage gap named below is closed.
  - The banked baseline would be replaced by numbers taken on a host other
    than C3D, or by a single run where the change is smaller than this host's
    run-to-run variance.
  - Public tokenized text queries, contract-declared projections carrying the
    named index types, or contract row policies would be activated without
    first revisiting docs/performance/deferred-dormant-path-optimizations.md.
  - A release would be cut without re-banking on the N1 and E2 profiles.
  - A second early lift would be enumerated, or the premature-lift guard in
    check-performance-freeze would be weakened rather than extended by one
    named exception.
  - A PERF-* work package would be registered relying on the lift, without the
    measurement discipline in decisions 2 and 3 applying to it.
---
# ADR-0239: Performance Programme Baseline And Measurement Discipline

## Context

The 2026-09 performance programme changed durable-record framing, the reuse of
input-derived command facts (ADR-0237), the release profile, and the server's
global allocator (ADR-0238). Measured on the C3D bench host, write throughput
rose 13 percent at one client and 22 to 31 percent at 8 to 128 concurrent
clients, with p95 at 128 clients falling from 88 ms to 55 ms. The receipts are
in `docs/performance/c3d-programme-bank-2026-09.md`.

Two things the programme learned matter more than the numbers. First, this
host's run-to-run variance is 2 to 4 percent, not the 1 percent assumed: two
banks of identical write-path code differed by 3.9 percent at one client count.
Second, the benchmark contract declares no projection, no text index and no row
policy, so whole subsystems have never been measured at all. A banked number
that does not say what it covers invites the reader to assume it covers
everything.

ADR-0183 froze performance-package registration until a banked baseline existed
and WP-750 and WP-760 closed. The baseline now exists; those two packages do
not. This record supplies the first and accepts the deviation on the second.

## Decision

1. `docs/performance/c3d-programme-bank-2026-09.md` is the banked baseline for
   development. It records the revision, the load, the client count, and the
   limits of each figure, including that read_only and interactive have no
   prior comparison because they had never been run on this host before.
2. C3D is the development standard. Changes are measured there, on the
   `write_only`, `read_only` and `interactive` loads, against the previous
   banked revision. The N1 and E2 profiles are release gates, re-banked before
   a release rather than before a merge, because clearing three hosts per
   change made the programme slower without changing a single decision.
3. A single-run delta smaller than this host's run-to-run variance is not a
   result. It is either reproduced across runs or it is noise, and it must not
   be banked, quoted, or used to justify relaxing a reviewed boundary.
4. This baseline covers the commit and write path and named-query point reads
   on the ticketdesk contract. It does not cover projections, tokenized text,
   or row-policy evidence, because no contract in the repository declares any
   of them. Coverage for those paths must exist before any public performance
   claim is made, and the baseline must say what it covers wherever it is
   quoted.
5. ADR-0183's performance-package freeze is lifted by this record, before
   WP-750 and WP-760 close. ADR-0183 requires both to close first and names an
   early lift as a review trigger, so this is a deliberate, accepted deviation
   rather than an oversight.
6. The lift is enumerated, not general. `check-performance-freeze` keeps its
   premature-lift guard and gains one named exception permitting exactly this
   record to lift with those two packages open. Any other early lift, by any
   other record, is still rejected, and the self-test proves it. Weakening the
   guard instead of extending it by one named exception is a review trigger.
7. Lifting the freeze does not lift the discipline. Decisions 2 and 3 apply to
   every performance package registered after the lift: measured on C3D
   against the previous banked revision, and a delta smaller than this host's
   run-to-run variance is not a result.

## Standing design tests

- **Interface safety (AGENTS.md boundary 11):** not applicable. This record
  adds no surface and changes no runtime behaviour; it governs how measurements
  are taken, banked, and quoted.
- **Scale:** no. The measurement discipline assumes nothing about storage
  topology. It does assume a single bench host as the development standard,
  which is a deliberate and reversible choice recorded in decision 2, made
  because per-change measurement on three hosts cost time without changing
  decisions.

## Consequences

Development measurement gets faster and, because the same host is used
throughout, more comparable. The cost is that a regression peculiar to the
older N1 and E2 profiles, including their lack of SHA-NI, will be found at
release rather than at merge. That is accepted deliberately: it is the same
trade the programme already made in practice, now written down.

Recording the variance figure has a sharper consequence than it looks. It
retires several small results this programme produced, including a cross-crate
change measured at roughly 1 percent that was reverted on those grounds rather
than kept.

## Options considered

Leaving the freeze in force was drafted first and is the conservative option:
it needs no deviation, and the freeze was demonstrably not blocking this
programme, which registers no PERF package and passes the checker with the
freeze in force. It was rejected because the thing the freeze protects
-- a banked baseline that nobody moves quietly -- is what this record supplies,
so the constraint now guards something that exists.

A second reason given when this record was accepted was that WP-750 and WP-760
depend on a workstream that had not advanced since 2026-09-17. That was wrong
and is withdrawn: WP-748 and WP-749 branches carry commits from 2026-09-19,
including N1 latency and archive-cost work. The lift stands on the first reason
alone. Anyone re-reading this decision should weigh it knowing those
prerequisites are being actively worked, not abandoned.

Deleting the premature-lift guard was rejected. The guard and its self-test
exist precisely to prevent this action, and removing them would convert one
accepted deviation into a permanent hole. Extending the guard by one named
exception keeps the mechanism, keeps every other early lift rejected, and
leaves the deviation enumerated where a reader will find it.

Waiting for WP-750 and WP-760 to close was rejected as open-ended. Both remain
open, with dependencies still in flight, and neither has a closure date.

Keeping all three hosts as the per-change standard was rejected under decision
2. Banking only the write path was rejected because it would have left the
first read and interactive measurements this project has ever taken unrecorded.

## Checks

- `scripts/check-performance-freeze` records the lift by this record and
  rejects any other early lift; `--self-test` covers both the permitted
  exception and a premature lift by a different record.
- The banked receipts and their stated limits are
  `docs/performance/c3d-programme-bank-2026-09.md`; the deferred findings on
  unreachable paths are `docs/performance/deferred-dormant-path-optimizations.md`.
- `~/tmp/c3d-probe2.sh` on the bench host produces the per-load, per-level
  lines the bank is built from.
