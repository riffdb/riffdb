---
adr: "0287"
title: Validation Walks Read Command History In One Pass
status: proposed
tier: guarantee
date: 2026-10-12
accepted: null
acceptance: null
requires: [ADR-0286, ADR-0283, ADR-0182, ADR-0203]
amends: []
supersedes: []
requirements: [REC-007]
packages: [WP-834]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - A consumer of the single pass sees a row, an order, or a decoder other than its own pass used.
  - A refusal of the entity chains or the commit plans surfaces at a different point, or with a different error, than their own passes produced.
  - Commit plans read from one snapshot are used for a historical plan built over another.
---
# ADR-0287: Validation Walks Read Command History In One Pass

## Context

The complete structural walk (ADR-0182 Decision 3, run by the background scrub and by complete startup validation) reads the `COMMITS`
table four times. At the walk's start it builds the command history's audit index (one pass over the rows above the validated-prefix
checkpoint). At the ENTITIES phase it builds the entity chains (a second pass over the same rows). The commit phase inspects each row (a
third). The historical evidence plan then collects every row's plan reference (a fourth pass, over all rows). Each pass decodes every
command segment, resolving its history stub from the journal and verifying it, so about four fifths of the scrub thread's time is segment
decoding, and three of the four decodes produce nothing a single decode could not.

On the second development machine (8 vCPU, AMD EPYC 7B12), frame-pointer samples of the post-start scrub, taken from its first instruction,
put 18% of the scrub thread in the history pass, 21% in the entity-chain pass, 30% in the commit phase and 24% in the plan pass. With the
pass below, the scrub takes 3.9 s instead of 6.6 s (median of six alternating write-only runs, the harness waiting for the scrub).

## Decision

1. The command history, the entity chains and the historical plan's `COMMITS` plan references may be produced by one forward pass over
   `COMMITS` at the walk's start, on the walk's pinned snapshot. Each row is decoded once for all three through one byte-keyed segment memo
   (ADR-0283 Decisions 1 and 2, ADR-0286 Decision 2).
2. Each consumer sees exactly the rows, in the order, and through the decoder its own pass used: the history and the chains the rows above
   the validated-prefix checkpoint, the plans every row. Rows at or below the checkpoint serve the plans alone.
3. The history's refusals return from the pass as they did from its own. The entity chains' build result, success or refusal, is held and
   installed when the ENTITIES phase begins, where its own pass ran; the checkpoint's entity-head seed is consumed once, as before. The
   commit plans are held with the first refusal their own loop would have met, and the historical plan inserts them, then returns that
   refusal, where the loop ran, after the outcome, pending and before the provenance plans.
4. Held commit plans are used only by a historical plan built over the same snapshot; reopening the walk's snapshot discards them. The
   held plans are bounded by the evidence-index ceiling; beyond it they are discarded and the historical plan reads `COMMITS` itself.
5. The commit phase keeps its own pass. Bounded startup paths, which build no historical commit plans, and a walk whose chains are already
   built keep their current passes.

Every row is still read and verified on each of the two remaining passes. The results are equal because each consumer's per-row work is
unchanged and the shared decode is reused only for byte-equal rows.

## Options considered

1. **Keep four passes:** rejected; three of them repeat a decode and verification that changes nothing.
2. **Also fold the commit phase into the pass:** rejected; it reports findings page by page in table order and would have to retain decoded
   history or findings to keep that order.
3. **Lighter per-consumer decoders:** rejected for now; a decoder that accepts what the full decoder refuses would let a pass validate rows
   on a different basis than the commit phase.

## Consequences

- Two `COMMITS` passes per complete walk instead of four; startup validation and the scrub both gain.
- The entity chains are built at the walk's start instead of the ENTITIES phase and held until then; a walk retains at most one segment
  memo entry and the distinct commit plans, which are a few per contract command, beyond what it retained before.
- Deferred: the commit phase's own decode, and the work ADR-0283 deferred.

## Standing design tests

- **Interface safety:** nothing on a public surface changes; no caller can select the pass, and the scrub's health states, rate bound and
  cancellation are untouched.
- **Scale:** memory stays bounded by the existing evidence-index ceiling and the entity-chain capacity; passes fall from four to two.

## Checks

- `single_commit_pass_matches_the_separate_passes_and_decodes_each_row_once`: over three segments, with and without a checkpoint floor and
  with a malformed row, the single pass's history, entity chains and historical plan equal those of the separate passes, and an intact
  segment row is decoded once instead of up to three times.
- The storage architecture scans pin the history to the single pass and the chains to the rows after the checkpoint.
- The unchanged ADR-0182, ADR-0203, ADR-0283 and ADR-0286 walk and scrub proofs.
