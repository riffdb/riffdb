---
adr: "0280"
title: Paced Journal Extent Zero Fill And Writer Hash Deduplication
status: proposed
tier: guarantee
date: 2026-10-07
accepted: null
acceptance: null
requires: [ADR-0263, ADR-0239]
amends: []
supersedes: []
requirements: [REC-001, STO-032, PERF-008]
packages: [WP-828]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - Extent preparation leaves more than one bounded chunk of zero-fill unsynced, or the chunk bound moves without a measured interference comparison.
  - Any final extent sync, directory sync, or spare publication order changes.
  - A receipt byte, history checksum, journal frame byte, or expected-state hash changes.
  - Overlay staging can change state before a refused mutation returns, or its memory charge stops equalling the per-entry charge of the entries it holds.
---
# ADR-0280: Paced Journal Extent Zero Fill And Writer Hash Deduplication

## Context

Every journal checkpoint rotates to a fresh 320 MiB extent (ADR-0263 Decision
7), and retained extents are not yet recycled, so each checkpoint zero-fills a
new spare beside the live journal. The fill buffered the whole extent and synced
it once, queueing the entire extent on the device ahead of the next journal
`fdatasync`. WP-814 attributes the remaining ~0.7 s write maximum to this; on the
GCP development host a concurrent 136 KiB journal sync stalled up to 177 ms in
isolation, and the ordered writer's idle time and journal queue wait were both
about doubled under c32 write-only load. Separately, the serial writer and the
checkpoint worker hash identical bytes more than once: the receipt accumulator
digests every put value although an insert can never be a restoration, history
advancement encodes and hashes the complete receipt and then encodes it again,
and overlay staging searches the persistent map twice per mutation.

These paths sit under guarantee-tier storage owners (extent durability, receipt
history, overlay bounds), so the decision to change them, and the invariants
that must not move, are recorded here rather than left to a package.

## Decision

1. Extent preparation, both fresh creation and recycled-extent regrowth, syncs
   file data after at most 2 MiB of zero-fill, and keeps the existing final data
   sync, header write, directory sync and spare publication order. Chunk syncs
   only precede the existing durability points; no extent becomes eligible or
   visible earlier, and preparation stays off the ordered writer's path.
2. The receipt accumulator computes a key's post-image digest only when the key
   has an existing predecessor (the only case that can be an exact
   restoration) or a later observation of the same key requires it. Receipt
   bytes, net transitions, predecessor checks, and the provider work bound are
   unchanged; the deferred state has the same size as the digest it replaces.
3. A caller that persists receipt bytes derives the successor history point from
   the trailing checksum of the single canonical encoding it persists
   (`advance_and_encode`), after the same predecessor and catalog checks as
   `advance`, which remains for callers that persist no bytes.
4. Overlay staging inserts in one descent when the conservative charge (current
   charge plus the replacement, releasing nothing) fits the overlay bound, and
   accounts exactly from the replaced entry. Otherwise it uses the existing
   exact pre-check. A refused or failed mutation leaves the stage's entries,
   charge and staged mutations unchanged in both paths.
5. No public interface, configuration, durable byte, hash value, extent format,
   bound, ordering, publication, acknowledgement, or recovery rule changes.
   Measured effects remain host-specific evidence until confirmed on the
   ADR-0239 C3D host with a same-run PostgreSQL comparison.

## Options considered

1. **Single sync per extent (current):** rejected; one 320 MiB writeback burst
   per checkpoint stalls the live journal on a shared device.
2. **`fallocate`/`FALLOC_FL_ZERO_RANGE`:** deferred; needs a native filesystem
   dependency and its own review, and unwritten extents can reintroduce journal
   metadata commits on first write.
3. **Recycling sealed extents:** owned by WP-815 and gated on durable pruning;
   it removes most fills but not the first fill per extent.
4. **Chunked syncs (selected):** standard library only, no format change; the
   development host bounds a concurrent journal sync at 7.8 ms for about 10%
   longer background preparation.
5. **Persistent B+tree overlay:** measured and rejected; about 2x faster lookups
   at 300K keys in isolation but no end-to-end gain, because per-table overlays
   stay small between checkpoints on the measured workload.
6. **Principal cache in authentication:** measured and rejected; even an unsafe
   skip-everything ceiling saved ~4 us per request and no throughput.
7. **Bounded writeback for the redb checkpoint commit:** measured and rejected;
   syncing the primary file every 2 MiB turned one checkpoint burst into
   hundreds of slow random-write syncs and cut write-only throughput by 17%.

## Consequences

- On the GCP development host (8 vCPU, AMD EPYC 7B12, persistent disk), the four
  changes together raised c32 write-only throughput by 11.5% (every candidate
  run above every base run), halved p99 (48 ms to 23 ms median), and cut maximum
  latency and writer idle time; interactive rose 6.7% at the median with
  overlapping runs. Zero-fill alone gave +2.3% at c32, within noise, and +10% at
  c64 with p99 168-193 ms falling to 40-42 ms.
- Extent preparation issues about 160 additional data syncs per checkpoint.
- Unchanged: WP-815 extent recycling, checkpoint thresholds, redb checkpoint
  commit bursts, and every accepted bound.

## Standing design tests

- **Interface safety:** nothing is exposed to applications or operators; no
  durability, idempotency, scoping, freshness, or bound becomes optional.
- **Scale:** unsynced zero-fill is bounded by one 2 MiB chunk regardless of
  extent size; hashing and staging do strictly less work per mutation, and the
  overlay's charged memory and refusal behavior are unchanged at its bound.

## Checks

- `fresh_spare_zero_fill_syncs_each_bounded_chunk_and_stays_an_empty_extent` and
  `recycled_spare_zero_fill_syncs_each_bounded_chunk_and_keeps_allocated_frames`
  fail on the single-sync implementation (40 MiB unsynced in test builds).
- `advance_and_encode_is_exactly_advance_plus_one_canonical_encoding`.
- `all_five_step_put_delete_histories_match_an_independent_net_state_oracle`
  and the existing coalescing tests cover the lazy digest.
- `single_descent_staging_charges_exactly_the_overlay_it_leaves` and
  `staging_near_the_overlay_bound_is_exact_and_a_refusal_changes_nothing`.
- Storage-api and storage-redb all-feature suites, including the process crash
  and recovery matrices; scoped acceptance and full CI before merge.
