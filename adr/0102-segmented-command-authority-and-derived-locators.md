# ADR-0102: Segmented Command Authority and Rebuildable Exact Locators

- **Status:** Accepted
- **Date:** 2026-08-07
- **Accepted:** 2026-08-07
- **Exact text accepted:** 2026-08-07
- **Amendment acceptance:** maintainer, in session, 2026-09-15: "Approve exact amendment";
  `docs/architecture/WP-749-EXACT-STOP-REVIEW.md`
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `STO-002`, `STO-020`, `STO-021`, `STO-022`,
  `REC-001`, `REC-002`, `TXN-041`, `TXN-042`, `TXN-043`, `TXN-044`,
  `PERF-008`, `PERF-009`, `PERF-010`, `PERF-016`, `PERF-017`
- **Related work package:** `WP-479`
- **Amends:** ADR-0005, ADR-0006, ADR-0021, ADR-0061, ADR-0080,
  ADR-0082, ADR-0083, ADR-0085, ADR-0098, ADR-0099, ADR-0101

## Context

ADR-0099 made one command capsule authoritative but retained six durable
per-command lookup rows: idempotency, provenance, two command-audit members,
and two request/audit locators. A simple command that creates one entity and
one secondary-index entry still causes roughly thirteen redb row mutations
after events, routes, outbox intent, index generation, capsule, and locators are
included.

After grouping and journal flush amortization, write performance is CPU-bound
on those B-tree mutations. A production append-only run reaches approximately
6,900 commands per second. A benchmark-only redb projection that retains one
group capsule authority and only independently mutable rows reduces terminal
table/page work from 196 microseconds to 69 microseconds for 32 commands, about
2.8 times less. Transaction-local observation caching reduces the measured
staging stage but changes total throughput by less than one percent, confirming
that physical write amplification is the structural ceiling.

The product guarantee is durable, typed, atomic, idempotent, auditable command
authority. It is not one durable B-tree row for every lookup path. Rebuildable
exact indexes may accelerate an authoritative record provided readiness and
every read fail closed when the index cannot be proven complete.

## Decision

### Accepted amendment: exact command-prefix restoration evidence

The exact "Exact command-prefix restoration evidence" amendment in SPEC §4.10
is incorporated here in full. The maintainer accepted it in session on
2026-09-15: "Approve exact amendment", referring to
`docs/architecture/WP-749-EXACT-STOP-REVIEW.md`. WP-749 owns successor capsule V7
(tag 54, revision 6), segment V6 (tag 55, revision 6), complete bounded prefix
evidence and validated private reconstruction before the unchanged receipt,
authorization and incarnation publication ceremony. Original V3 receipt semantics
and bytes remain unchanged; missing legacy evidence never permits a rounded stop.


### Bounded contiguous command segments

Successful commands committed by one compatible physical group are encoded in
one `StoredCommandSegmentV1`. A segment:

- is keyed by its first commit sequence in the `commits` table;
- contains a nonempty contiguous sequence interval in FIFO assignment order;
- contains at most 256 commands and 16 MiB of complete canonical bytes;
- contains one complete `StoredCommandCapsuleV2` per command, including the
  exact logical index-generation advances currently represented by independent
  per-command generation rows; and
- carries a canonical header with database identity, history incarnation,
  predecessor segment hash, first/last commit and administration sequences,
  command count, and a digest over the complete segment.

The segment is the sole authority for successful outcome, provenance, commit,
command audit, immutable event, event-route, and outbox-intent facts. Entity
post-images, secondary-index entries, mutable outbox delivery status, consumer
checkpoints, and projection state remain independently stored because they are
read or changed without reconstructing a command segment.

Point lookup by commit or event sequence performs a predecessor range lookup on
the segment table, then proves that the requested sequence and ordinal are
inside the canonical segment. It does not require a per-command directory row.

### Exact rebuildable indexes

Idempotency identity, provenance identity, audit sequence, audit request, event
route, and pending-outbox membership become rebuildable exact indexes over
command-segment headers and capsules. They are not independent authority.

Each committed segment includes a bounded canonical index manifest containing
only the keys, member roles, and command ordinals needed to rebuild those
indexes. After the segment and unchanged authoritative state are durable, the
runtime applies the manifest to generation-scoped in-memory indexes before
publishing any result. A duplicate key, missing reciprocal member, wrong
ordinal, noncanonical order, or manifest/capsule disagreement fences readiness
or writes and is authoritative corruption.

Startup scans every retained segment manifest in sequence order, validates its
hash chain and capsule reciprocity, and rebuilds the exact indexes before
application readiness. A validated checkpoint may contain a checksummed index
snapshot bound to database identity, history incarnation, registry digest,
segment frontier, and segment-root digest. Startup may load that snapshot and
scan only the suffix. The snapshot is derived: absence or invalidity causes a
bounded rebuild, never acceptance of an incomplete index.

Pending admissions and deterministic execution failures remain complete
durable idempotency rows because no successful command segment owns them.
Non-command service-audit records remain complete rows in the standalone audit
stream. Audit scans merge those rows with the exact command-audit index under
one frozen frontier and reject duplicate or missing administration sequences.

### Logical generation overlay and one physical post-image

Within one segment, every command retains its exact prior and successor
partition/index generation. The sole writer validates commands in order against
a bounded transaction-local overlay. Each overlay transition must equal the
command capsule's logical advance. Redb receives one final generation post-image
per distinct partition/index target in the physical group, with an assertion
that the initial physical value equals the first logical prior.

The journal frame retains the complete segment and its ordered logical
transitions. Recovery verifies the full logical chain and applies the same final
physical generation post-images. No query, conflict check, snapshot, or later
command observes a generation other than the exact value it observes today.

### Atomicity, acknowledgement, and recovery

The entity/index post-images, final physical generations, command segment,
allocator post-images, and independently mutable state are staged in one redb
transaction or one ADR-0101 journal frame. Acknowledgement still means the
complete command survives a crash. Commands in a segment retain independent
identities, outcomes, retry resolution, and public responses.

No transient index, result, event notification, projection work, subscription,
or outbox attempt is visible before the segment's durability fence publishes.
On an unknown fence result, writes remain fenced until recovery validates or
replays the exact segment. Equal-input retry resolves through the rebuilt
idempotency index and returns the original persisted outcome.

### Retention and migration

A retention watermark may remove a whole segment only when every member is
eligible. If a watermark falls inside a segment, retention atomically rewrites
the retained suffix as a new canonical segment with the same command sequences
and semantic bytes before advancing the watermark.

Before pruning authority needed beyond the history watermark, retention
materializes the same complete outcome, provenance, audit, event, or outbox
views required by current policy. This cold-path materialization may use the
predecessor complete row formats; it does not restore hot-path locator writes.

The offline migration from ADR-0099 groups contiguous validated capsules into
bounded segments. It validates every existing capsule and locator before
replacement, is restartable at a segment boundary, and publishes the successor
registry only after a complete segment/index rebuild proves semantic
equivalence. Rollback after registry publication is unsupported.

## Safety invariants

- The command segment is sufficient to reconstruct every semantic view
  byte-for-byte; a derived index never supplies business payload.
- Every lookup joins its key to a canonical segment member under one snapshot
  frontier before release.
- Index completeness is positively proven at readiness and after every durable
  publication; an empty or stale index is never treated as absence.
- Segment, event, audit, and generation order remain bounded and deterministic.
- No application surface can bypass compiled commands or access raw segments.
- Cancellation, authorization, redaction, idempotency, provenance, causation,
  and declared outcomes do not change.

## Consequences

- The common one-entity/one-index/event command falls from roughly thirteen
  redb row mutations to two per-command state mutations plus amortized segment,
  generation, allocator, and mutable-status writes.
- Hot-path Protobuf framing, checksums, allocation, B-tree traversal, and page
  mutation fall with the physical row count.
- Startup and recovery own an exact index-rebuild phase, mitigated by a bound
  checkpoint plus suffix proof.
- Retention and corruption tests become segment-aware and must cover partial
  segments, manifest substitution, duplicate derived keys, and checkpoint
  mismatch.
- This is an incompatible pre-alpha format cut. It must not be inferred from
  benchmark evidence; implementation requires explicit acceptance of this ADR.

## Rejected alternatives

- **Continue scheduler and cache tuning.** Measurements show only single-digit
  gains after durability is amortized.
- **Trust an in-memory map without rebuilding proof.** A stale negative result
  would break idempotency and audit safety.
- **Keep durable locator rows as a second authority.** Preserves the B-tree
  amplification and creates reciprocal state that can drift.
- **Remove idempotency, provenance, audit, or outbox semantics.** Improves speed
  by weakening the product and is not acceptable.
- **Replace redb.** The measured cost follows the requested row graph; reducing
  that graph should be tested before replacing the engine.

## Acceptance

The maintainer explicitly accepted the segmented authority, rebuildable-index,
logical-generation-overlay, migration, retention, and crash semantics above on
2026-08-07 and authorized `WP-479` implementation.
