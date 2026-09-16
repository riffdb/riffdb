---
adr: "0234"
title: Bounded Repairs For Reviewed Runtime Resource Costs
status: accepted
tier: guarantee
date: 2026-09-16
accepted: 2026-09-16
acceptance: 'maintainer, in session, 2026-09-16: "yes, i approve" (ADR-0234 as written)'
requires: [ADR-0102, ADR-0173, ADR-0175, ADR-0183]
amends:
  - ADR-0183 only to admit the explicitly enumerated review repairs below.
supersedes: []
requirements: []
packages: []
obligations: []
review_triggers:
  - A cache miss would become authoritative absence or bypass reciprocal validation.
  - Read visibility, retention, publication, transaction ordering or durability would change.
  - A sealed performance package or inert columnar batch implementation would be activated.
  - A resource ceiling, public result, cursor, score or canonical encoding would change.
---
# ADR-0234: Bounded Repairs For Reviewed Runtime Resource Costs

## Context

The September 16 static review identifies real resource costs in live code:
resident decoded command history, per-row scratch-file opens, single-row
partition scans, repeated BM25 corpus statistics, group-key allocation, vector
re-encoding, repeated mixed-page sorting and pairwise partition merging. The
review's other allegations include already-fixed behavior, deliberate public
limits and inert implementation costs; their individual dispositions are in
`docs/reviews/2026-09-16-runtime-findings.md`.

ADR-0183 freezes new performance work and explicitly permits no fifth package
exception. Evicting decoded command segments also needs an exact design for
cache absence, immutable snapshot reads, manifest reciprocity and follower
replacement; deleting map entries alone would break those guarantees. This
proposal admits only the repairs enumerated below. It is not accepted, does
not lift the general freeze, and does not authorize implementation before
human acceptance of this text.

## Decision

1. Admit the eight repairs corresponding to findings 8, 10, 12, 13, 16, 17, 19
   and 20 of the September 16 review. This narrow amendment to ADR-0183 does not
   change any sealed package, banked baseline, threshold, or permitted closure.
   Existing functionality and bounds remain the acceptance oracle. New
   features, dependencies, public controls and inert batch activation are out
   of scope. Register the implementing non-PERF repair package after acceptance,
   with exact allowed paths and the proofs below before implementation begins.
2. Replace the published index's ownership of all decoded command payloads
   with complete exact locators and immutable segment metadata, plus a bounded
   payload cache. Metadata binds first/last sequence and segment digest. Keep
   the complete index required by ADR-0102; its size remains proportional to
   retained identities. Do not describe this as bounding all database memory.
   Bound cached payload ownership to 64 MiB and 64 entries, accounting for
   decoded owned storage before insertion. A larger individual segment is
   validated and used without cache retention. At most one bounded segment
   decode is owned by each active lookup outside that cache.
3. Cache eviction never means authoritative absence, index invalidity or a
   missing durable member. Resolve an evicted locator from the caller's frozen
   read or write transaction, validate the canonical segment, exact digest,
   sequence coverage and reciprocal manifest member, and propagate corruption
   or I/O failure. A cached segment must satisfy the same snapshot/frontier
   and member checks. Never open a newer transaction to satisfy an older pin.
   Keep unpublished writer overlays separate under their existing admission,
   durability and publication bounds; do not evict their required capabilities.
4. Rebuild exact indexes and route/outbox metadata by streaming one bounded
   segment at a time. Follower replacement and retention must remove the exact
   prior manifest and dependent membership even when its payload was evicted.
   Capture the necessary old evidence before its physical replacement; preserve
   the existing atomic transition, duplicate rejection and fail-closed behavior.
   No durable locator table, storage key, format or authority is introduced.
5. Buffer scratch-lane frames with at most 16 open lane writers, each with a
   64 KiB buffer. Explicitly flush on eviction and before any lane read or
   successful build completion. Propagate flush failure, retain checksum and
   length framing, and leave partial failed builds unselected. Keep the current
   row, file, disk-byte, cancellation and cleanup bounds.
6. Batch uniform partition scans in pages of at most 64 rows, further bounded
   by remaining scan/point/intermediate fuel. Retain at most one bounded page
   per stream. Charge all fetched work, including prefetched rows; never reset
   fuel or raise a ceiling. Preserve exact partition observations, same-epoch
   checks, row validation, policy admission, exhausted-stream detection, and
   continuations derived from the last emitted total-order marker. Boundary
   requests must not gain premature refusals merely from speculative prefetch.
7. Compute term document frequencies and field corpus lengths once per BM25
   execution snapshot. Reuse the exact statistics for identity and scores while
   retaining repeated query terms' existing score contribution. Preserve all
   checked arithmetic, truncation, policy-aligned corpus, tie breaks, term and
   candidate limits, and public score invisibility.
8. Encode grouped keys into reusable scratch storage using borrowed source
   cells. Allocate retained key values and bytes only for a new group. Preserve
   canonical length framing, group order, exact aggregate results and byte,
   work and cardinality fuel. Do not change the inert `batch/group.rs` owner,
   its conservative shift accounting or architecture seal.
9. Eliminate vector re-encoding only where the existing canonical decoder
   already proves exact version/tag/dimension, finite components, positive
   zero and exact end. Keep preallocation length checks and schema validation.
   Malformed bytes must remain rejected on every read, including hot reads;
   write-time validation alone is insufficient.
10. Sort each new mixed-order partition page once and merge it into the retained
    ordered run, preserving duplicate detection and the physical-prefix boundary
    needed for mixed directions. Merge partition runs with one bounded fallible
    k-way heap and one output allocation, preserving all comparison errors,
    total ordering, discarded-row detection, cursor boundaries and policy/fuel
    checks. Do not replace fallible comparison with an equality fallback.

## Options considered

1. Apply all report recommendations literally: rejected. Some findings are
   false, and raw eviction, unbounded channels, relaxed canonical checks and
   lowered work charges can weaken guarantees.
2. Leave every confirmed cost unchanged: preserves the freeze but retains the
   decoded-history memory problem and verified redundant work.
3. Admit only the enumerated repairs with unchanged semantics: proposed.

## Consequences

- Decoded history payload retention becomes bounded; exact locator metadata
  remains proportional to retained history as required by ADR-0102.
- Cold lookups may require additional I/O. Strict snapshot and integrity checks
  apply equally to cold and cached access.
- The remaining performance program stays frozen. Static cost analysis is not
  throughput evidence; no speedup is claimed until measured.

## Standing design tests

- **Interface safety:** applications cannot choose cache bypass, buffers, fuel,
  weaker validation, publication timing, unsafe concurrency or durability. All
  existing public refusal, policy, tenant and freshness guarantees remain.
- **Scale:** cache entries/owned bytes, scratch writers, prefetch, merge state
  and temporary key storage have server-owned ceilings. No history-sized
  payload cache, unbounded channel or per-row postings reconstruction is added.

## Checks

- Force cache eviction between every read. Compare cached/cold command outcome,
  provenance, audit and event reads, including old pinned snapshots, replay and
  idempotency. Reject missing, corrupt, wrong-digest and wrong-member segments.
- Exercise follower replacement, retention splits, restart and process crashes
  with cold payloads. Prove all indexes match an independent streamed rebuild.
- Count retained bytes, resident entries, lane opens and flushes, scan calls,
  posting enumerations and allocations under growing bounded workloads.
- Inject append/flush failures; no incomplete candidate becomes readable.
- Compare partition pages/cursors across uniform and mixed directions, ties,
  empty/denied streams and every fuel boundary against an independent oracle.
- Compare exact BM25 scores/statistics (including repeated terms) and grouped
  values/bytes with existing fixtures; test all canonical-vector corruptions.
- Run scoped and full acceptance, generated/handbook checks, crash/recovery and
  the existing performance sentinel. Review fixtures and measured cost evidence.
