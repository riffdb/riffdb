---
adr: "0281"
title: Changelog Root Validation Once Per Replay Transaction
status: accepted
tier: guarantee
date: 2026-10-08
accepted: '2026-10-08'
acceptance: 'maintainer, in session, 2026-10-08: "i approve", in response to the exact-text acceptance request for ADR-0281 as committed at ef92b5615'
requires: [ADR-0104, ADR-0186, ADR-0263]
amends: []
supersedes: []
requirements: [REC-001, REP-003]
packages: [WP-829]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - A journal frame is applied without being decoded and validated from its own bytes, or without its receipt being derived and its recorded binding checked.
  - Any ordered mutation's transaction-current before-image stops being checked.
  - A replay or drain transaction commits without a full changelog root validation after its final receipt-bearing frame.
  - A root check moves from per frame to per transaction for anything other than a single redb write transaction.
---
# ADR-0281: Changelog Root Validation Once Per Replay Transaction

## Context

Recovery replay (`replay_frames`) and the published-suffix drain that every direct primary write performs under the writer gate
(`apply_published_journal_suffix`) materialize journal frames through `materialize_native_frame`. It validates the complete changelog roots three times per
frame: before the frame, inside history-advance preparation, and after staging. Each full validation resolves the terminal receipt through its history stub,
which re-reads the previous frame from the journal extent, re-verifies its physical and logical frame hashes, re-derives its receipt and hashes it again. The
live checkpoint, which has its own retained-proof path, measured this cost at 64% of its CPU before `fc357d14e` validated its roots once per batch; on C3D that
change raised c32 write-only throughput 5.6% and interactive 5.3%. Replay and the drain still pay it per frame, the drain while holding the writer gate.

## Decision

1. When one redb write transaction materializes several journal frames, the complete changelog roots, including the terminal receipt resolved through the
   history index, are validated in full before its first receipt-bearing frame and again after its last, before commit. A failure at either point aborts the
   transaction.
2. Between them each frame starts from the history state the previous frame staged in the same transaction. Per frame, the stored history-state and allocator
   rows must equal that state, the physical application and administration frontier must equal its tail, the admission roots must accept the receipt, the
   allocation must match, and the history slot must be empty.
3. Unchanged per frame: every frame is decoded and validated from its own bytes; its receipt is derived from those bytes and its recorded binding checked against
   the chained state (ADR-0263 Decision 2 and Amendment 2); every ordered mutation, including cancelled ones, has its transaction-current before-image checked;
   the drain's native-preimage source-continuation check runs before any mutation.
4. A frame without a receipt source ends the chain. The deferred full validation runs before that frame is applied, exactly where the per-frame path ran it, and
   the next receipt-bearing frame validates the roots in full first.
5. A single standalone frame keeps the existing full validation before and after it.

## Options considered

1. **Keep three validations per frame:** rejected. No state between two frames of one transaction is durable or visible, so intermediate validations bind
   nothing the final one does not, while each re-reads and re-hashes the preceding frame from the extent.
2. **Validate once per transaction and also resolve every intermediate history stub against the extent:** rejected. Stub resolution is most of the cost, and
   each stub is resolved by every reader and by the background scrub (ADR-0263 Decision 8), which refuse a mismatch as typed corruption.
3. **Skip receipt re-derivation as well:** not decided here. It is about 6% of checkpoint CPU, and recovery deliberately derives receipts from durable bytes.

## Consequences

- Recovery replay and the direct-write drain stop re-reading and re-hashing the predecessor frame per frame. The drain's time under the writer gate falls in
  proportion to the published suffix it drains.
- The history stubs of frames before the last in one transaction are not resolved against the extent at materialization time. A damaged stub target is found
  by its first reader or by the scrub rather than by the transaction that wrote the stub; both refuse it.
- No durable byte, encoding, ordering, atomicity, acknowledgement, publication or public interface changes. No recovery outcome changes for a valid journal; a
  journal the per-frame path refused at a frame is refused by this path no later than its commit.

## Standing design tests

- **Interface safety:** nothing is added to or removed from any public surface; durability, idempotency, scoping, freshness and bounds are unchanged, and no
  caller can opt out of the final validation.
- **Scale:** full root validations per transaction fall from three per frame to two plus one per receipt-source break, independent of suffix length; per-frame
  work is bounded by the frame.

## Checks

- `recovery_replay_validates_roots_once_per_transaction_and_matches_sequential_receipts` and
  `published_suffix_drain_validates_roots_once_per_transaction_and_matches_sequential_receipts` replay three chained frames, require two full root validations
  (nine before this decision), and require each receipt and the final history state to equal standalone sequential materialization.
- The existing recovery, crash-matrix, drain, and storage recovery suites remain unchanged and passing.
