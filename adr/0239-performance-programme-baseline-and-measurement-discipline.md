---
adr: "0239"
title: Performance Programme Baseline And Measurement Discipline
status: proposed
tier: guarantee
date: 2026-09-19
accepted: null
acceptance: null
requires: [ADR-0171, ADR-0183, ADR-0237, ADR-0238]
amends: []
supersedes: []
requirements: []
packages: []
obligations: []
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
  - ADR-0183's freeze would lift, which this record deliberately does not do.
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
5. This record does **not** lift ADR-0183's freeze. That freeze lifts only
   after WP-750 and WP-760 close, through a separately accepted record, and
   both packages are open. The freeze was never what gated this programme: it
   governs PERF work-package registrations, this programme registers none, and
   `check-performance-freeze` passes with the freeze in force.

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

Lifting ADR-0183's freeze in this record was the original intent and was
rejected on the facts: WP-750 and WP-760 are open, and ADR-0183 names lifting
before they close as a review trigger. The freeze also turned out not to be
blocking anything, so lifting it would have bought nothing.

Keeping all three hosts as the per-change standard was rejected under decision
2. Banking only the write path was rejected because it would have left the
first read and interactive measurements this project has ever taken unrecorded.

## Checks

- `scripts/check-performance-freeze` passes with ADR-0183's freeze in force,
  which is the evidence for decision 5.
- The banked receipts and their stated limits are
  `docs/performance/c3d-programme-bank-2026-09.md`; the deferred findings on
  unreachable paths are `docs/performance/deferred-dormant-path-optimizations.md`.
- `~/tmp/c3d-probe2.sh` on the bench host produces the per-load, per-level
  lines the bank is built from.
