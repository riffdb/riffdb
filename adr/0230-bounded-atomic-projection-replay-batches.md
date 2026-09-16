---
adr: "0230"
title: Bounded Atomic Projection Replay Batches
status: accepted
tier: guarantee
date: 2026-09-16
accepted: 2026-09-16
acceptance: 'maintainer, in session, 2026-09-16: "Accept all four as written" (ADR-0227, ADR-0230, ADR-0231, ADR-0232)'
requires: [ADR-0010, ADR-0017]
amends: [ADR-0017 single-sequence atomic application operation]
supersedes: []
requirements: [PRJ-001, PRJ-002, PRJ-003, PRJ-004]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers: []
---
# ADR-0230: Bounded Atomic Projection Replay Batches

## Context

Aggregate projection catch-up reads bounded commit pages but applies one commit
per durable redb transaction, including irrelevant commits. Long backlogs incur
one immediate commit per sequence and can delay freshness recovery substantially.
This is a repair of an existing high-severity catch-up bottleneck under D-001.
The report's specific wall-clock estimates have not been benchmarked.

A watermark-only jump would violate the exact apply-marker prefix and can skip
relevant effects. Independently preparing several deltas against the same old
projection state also produces incorrect running aggregates. ADR-0017 specifies
one exact next-sequence operation; expanding the transaction boundary needs
human review under AGENTS.md and D-003. This record is proposed, not accepted.

## Decision

1. Add one closed projection batch operation for a single exact bound schema,
   identity, generation and complete expected control. It contains at most 64
   contiguous commit requests, including requests with no relevant row changes.
   Each member retains its existing canonical apply hash, marker identity and
   expected predecessor frontier. No member may skip an authoritative sequence.
2. Prepare members sequentially over a private bounded projection-state overlay
   on one captured base. Each member observes earlier members' post-images. The
   overlay records the exact base observations used on first access; accepting
   a batch requires those base observations and expected control still to match.
   No callback, projection evaluation or arbitrary application code runs inside
   the storage transaction. Projection arithmetic remains first-party and checked.
3. Before retaining a member, charge its canonical request bytes, row updates,
   apply marker, control change, observation set and overlay state. The aggregate
   batch must fit the existing MAX_PROJECTION_WRITE_SET_BYTES and corresponding
   apply/snapshot state bounds. The 64-member ceiling does not multiply any byte
   or row-state ceiling. Flush a nonempty batch before exceeding a bound; a single
   oversized member retains the existing typed failure.
4. In one write transaction, storage revalidates every member's canonical request,
   full identity and consecutive frontier chain. It validates the exact base
   observations, checks lifecycle/control, then applies member transitions in
   order against transaction-current state. Every sequence still gets its exact
   canonical marker. Persist final row state, all member markers and final
   frontier/control atomically, with unchanged immediate durability.
5. A wholly already-applied batch is a no-op only after every retained member
   marker and canonical hash matches; it does not require its obsolete base
   observations to match current rows. A mixed already-applied/new batch returns
   StateChanged after checking the retained markers, without applying its suffix;
   the worker prepares that suffix afresh at the current base. A missing or
   mismatched marker fails closed. Changed control, missing source commits or
   integrity failures cannot partially advance a batch.
6. Notify waiters only after durable success at the actual final frontier. Unknown
   commit must reconcile exact batch markers and control before reporting success
   or retrying. Its new suffix commits wholly or not at all. An observed partial
   prefix cannot be guessed to prove this batch succeeded; independently valid
   competing progress instead requires StateChanged and fresh preparation.
7. This changes no persisted record/key/hash, projection expression, arithmetic,
   row ordering, retention frontier meaning or application acknowledgement.
   Source commits and outbox effects remain authoritative and unchanged. Batch
   size is internal and bounded; callers cannot bypass validation or durability.
8. Scope is aggregate event-derived replay/catch-up only. Columnar V2 root
   publication, text provider generations, authoritative group commit and outbox
   claim/delivery transitions are not authorized by this decision.

## Options and consequences

Per-commit transactions preserve current behavior but amplify durable commits.
Skipping irrelevant markers or jumping the watermark is rejected. Parallel
preparation against a stale base is rejected because aggregates depend on earlier
post-images. Sequential bounded preparation with complete base validation allows
up to 64 sequences per durable transaction while retaining the same logical
prefix. Intermediate frontiers inside a batch become visible together; a
snapshot never sees markers or rows beyond its reported frontier.

## Standing design tests

- **Interface safety:** applications cannot choose a frontier, create apply
  evidence, inject storage callbacks or relax any acknowledgement guarantee.
- **Scale:** fixed count and existing aggregate byte/state ceilings bound both
  preparation and transaction work. One worker owns one prepared batch at a time.
- **Recovery:** existing exact member markers identify durable completion; no new
  opaque batch-success flag can replace reciprocal state/control validation.

## Checks

- Compare every completed batch with the existing per-commit reference over
  overlapping group keys, irrelevant commits, sums, minima/maxima, nulls and
  arithmetic failures. Verify exact marker hashes and final canonical rows.
- Reject gaps, duplicates with changed hashes, substituted source/schema/control,
  stale base observations, lifecycle changes and oversized aggregate state before
  committing any new member. Verify bounded flushes at count and byte ceilings.
- Process crashes before engine commit, after commit and before notification;
  reopening observes precisely the old or complete new prefix. Unknown outcomes
  and retries cannot double-apply any aggregate or discard an irrelevant marker.
- Deterministically race rebuild/retirement with batch preparation. The complete
  expected-control comparison must prevent application to another generation.
- Count storage transactions for 1,024 irrelevant commits and for repeated updates
  to one group, then benchmark catch-up alongside live writers. At the count
  ceiling, a fitting workload uses 16 transactions rather than 1,024.
- Retain projection prefix, policy, idempotency, retention, crash and freshness
  suites. Update projection operations documentation after accepted implementation.
