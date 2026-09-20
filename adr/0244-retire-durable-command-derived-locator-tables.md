---
adr: "0244"
title: Retire Durable Command Derived Locator Tables
status: proposed
tier: guarantee
date: 2026-09-19
accepted: null
acceptance: null
requires: [ADR-0102, ADR-0165, ADR-0234, ADR-0236, ADR-0239]
amends:
  - ADR-0165 by removing the three durable locator tables it added, while
    keeping its fail-closed rule that a missing or mismatched locator is typed
    corruption rather than absence.
supersedes: []
requirements: []
packages: [WP-796]
obligations:
  - id: OBL-0244-1
    package: WP-796
    proof: a_missing_or_mismatched_locator_is_corruption_not_absence
    says: A locator that cannot be resolved fails closed as typed corruption and
      is never reported as a record that does not exist.
review_triggers:
  - A locator would be served from derived state that has not been proven
    complete against the segment frontier.
  - The retirement would be taken without measuring the write path it is
    justified by, or on a delta inside the bench host's run-to-run spread.
  - A fourth durable locator table would be proposed for a new derived key.
---
# ADR-0244: Retire Durable Command Derived Locator Tables

## Context

ADR-0165 added three durable locator tables — `idempotency_locators`,
`provenance_locators` and `audit_by_request_locators` — and measured what they
cost. On an idle N1, interleaved arms, three complete pairs over 115,690
commands, seed throughput fell from ~2,495.7 to ~2,157.5 ops/s: **≈ −13.5%**,
with no overlap between the groups. Each table adds ~135.9 bytes of engine
storage and ~15.6 bytes of B-tree metadata per command (**+4.96%** database size
for one table), and the three together add **180.0 bytes of journal frame per
command (+5.78%)**. No additional fsync, because the rows join their segment's
frame.

That is the largest single measured write-path tax recorded in this repository,
and it is paid on every command.

ADR-0165 chose durable rows for a reason that no longer holds. At the time the
transient index was the only locator, and a dormant index meant absence could
not be distinguished from a record that had not been indexed yet — absence
reported for a record that was present. ADR-0102, accepted and titled
*Segmented Command Authority and Rebuildable Exact Locators*, supplies a
checksummed index snapshot bound to database identity, incarnation, registry
digest, segment frontier and root digest, with suffix-only startup scanning.
That removes the premise: the derived index can be proven complete, so it can be
the locator.

The direction is already established. ADR-0234's finding-8 repair and ADR-0236,
*Remove Fresh Locator Coverage*, both accepted, have been removing locator
machinery rather than adding it.

## Decision

1. Retire the three durable locator tables ADR-0165 added. The ADR-0102 derived
   index becomes the locator: snapshot-loaded when the snapshot proves complete
   against the segment frontier, and otherwise rebuilt within a bounded scan
   before readiness is published.
2. Keep ADR-0165 §3 unchanged. A locator that is missing or does not match is
   **typed corruption**, never absence. Retiring the durable rows removes a
   second encoding of proven bytes; it does not relax what a failed lookup
   means.
3. A locator is never served from derived state that has not been proven
   complete. If the index cannot be proven, reads fail closed rather than
   answering from a partial index.
4. The retirement is justified by a write-path measurement and is not taken
   without one. Under ADR-0239's discipline it is measured on the bench host
   against the previous banked revision, and a delta inside that host's
   run-to-run spread is not a result.

## Options considered

**Keep the tables and optimise their encoding** was rejected. The cost is three
durable rows per command, not the shape of each row; a cheaper encoding of a
redundant record is still redundant.

**Retire one table and keep two** was considered. ADR-0165 records that an
earlier single-table prototype cost −6.16% against the shipped three-table
−13.5%, so the cost is roughly linear in table count and a partial retirement
returns a proportional fraction. It was rejected because it keeps the second
encoding and its validation surface while recovering only part of the cost.

**Defer until after alpha** was rejected on timing. This is a durable-format
change. Before alpha it is a pre-alpha cut needing no compatibility ceremony
beyond the epoch gate; after alpha it needs a migration for every deployed
database. The window is open now and closes at alpha.

## Consequences

The write path stops paying for a second durable encoding of bytes ADR-0102's
segment manifest already proves. The expected recovery is the cost ADR-0165
measured; the actual recovery is what WP-796 measures, and this record does not
claim it in advance.

Startup takes on more work in the case where the snapshot cannot be proven
complete: a bounded rebuild replaces reading durable rows. That is the trade —
a bounded, infrequent startup cost in place of a per-command write cost — and it
is the same trade ADR-0102 already accepted for its index.

Recovery paths that read locator rows directly must instead consult the derived
index and honour decision 3. Any path that cannot prove completeness must fail
closed rather than degrade to a partial answer, which is the behaviour ADR-0165
§3 already requires and this record preserves.
