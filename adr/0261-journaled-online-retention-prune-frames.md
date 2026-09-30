---
adr: "0261"
title: Journaled Online Retention Prune Frames
status: superseded
tier: guarantee
date: 2026-09-27
accepted: "2026-09-28"
acceptance: "maintainer, in session, 2026-09-28: \"Accept as written; close WP-811\""
requires: [ADR-0182, ADR-0085, ADR-0101, ADR-0103, ADR-0104, ADR-0165]
amends:
  - ADR-0182 section 6's execution of a prune step as a direct engine write that drains
    the journal suffix, and its permanent deletion of the validated-prefix checkpoint
  - ADR-0101's closed set of journaled transition attributions
  - ADR-0103's closed set of journal table tags (additive)
supersedes: []
superseded_by: [ADR-0263]
requirements: [STO-030, STO-031, STO-032]
packages: [WP-811, WP-812]
obligations:
  - id: OBL-0261-3
    package: WP-812
    proof: check-online-retention-interference
    says: Release evidence shows the journaled lane's keep-up fraction under 32-client
      interactive load, with p95 inside ADR-0182's five-percent bound.
review_triggers:
  - A prune frame would become durable other than through the ordered writer's group
    sync, or would be applied without revalidating its preconditions at sequencing.
  - A journal carrying the new table tags or attribution would be readable by a binary
    that does not understand them, instead of being refused.
  - The validated-prefix checkpoint would be installed for a prefix no complete
    validation covered.
---
# ADR-0261: Journaled Online Retention Prune Frames

## Context

ADR-0182 section 6 runs online retention as bounded administration-class
transitions "journaled like every other transition". WP-761 implemented each step
as a direct engine write instead. A direct write must first materialize the whole
published journal suffix into the engine under the writer gate, then pay its own
durable engine commit. WP-811 measured it on the ADR-0239 dev-standard host at 32
interactive clients. A step held the gate about 300 ms: 74% draining the suffix, 22%
the step's commit, and 4% the prune body itself. Under ADR-0182 section 7's
five-percent bound, the lane pruned 0.24% as many sequences as the load committed
(0.29% in WP-761's banked evidence). Stepping right after each journal checkpoint
installs raised that to 1.31% (release/evidence/wp-811). But a checkpoint installs
with the commands that arrived during its own work still in the suffix, so each
step still held the gate about 360 ms: 63% drain, 24% commit, and 12% prune body. The first step also deletes the
validated-prefix checkpoint, so every later restart takes complete startup
validation. That defeats WP-760's bounded restart for any database that retains
online.

Neither cost is intrinsic to pruning. The journal already records puts and deletes
with expected-hash preconditions (ADR-0103), becomes durable by the writer's shared
group sync (ADR-0101), and is materialized into the engine by the background
checkpoint off the gate (ADR-0104). A prune step expressed as a journal frame pays
none of the drain, commit, or checkpoint costs under the gate.

## Decision

1. An online prune step is one journaled transition with its own closed attribution,
   `JournaledRetentionPrune`, sequenced by the sole ordered writer. It becomes
   durable in the same group sync as the commands around it, and never through a
   separate engine commit or a journal-suffix drain. The offline operation keeps
   ADR-0085's direct exclusive path unchanged.
2. The lane prepares each frame outside the writer gate from one composite read
   view (engine plus published journal suffix). The frame carries every put and
   delete of the step: retained outcome, provenance, audit, and route views;
   locator removals; command, event, outbox, and outbox-status deletions; the
   tombstone; and the watermark. Every delete and overwrite carries the expected
   hash of the row it replaces. The tombstone digest is computed in one pass during
   preparation.
3. At sequencing, the writer applies the frame only if every expected hash still
   matches, the watermark equals the frame's predecessor watermark, and a fresh
   read of the complete ADR-0182 fencing set through the same composite view still
   permits the frame's last sequence. Otherwise it refuses the frame, which changes
   nothing, and the lane re-plans. The fence read under the gate is bounded: point
   and bounded-table reads only, with the retained-history walk performed only when
   a follower acknowledgement exists to validate.
4. At most 256 sequences per frame and the five-percent p95 bound of ADR-0182
   sections 6 and 7 are unchanged. The lane's writer-time accounting counts only
   sequencing and apply time.
5. The live retention watermark is raised when the frame is published, before its
   deletions become visible in any composite view, and restored if the frame is
   refused or its sync fails. A below-watermark read therefore returns
   `HistoryPruned`, never absence.
6. Journal recovery materializes a prune frame exactly as any other frame. An
   incomplete trailing frame is dropped, as today. Replay re-verifies the tombstone
   chain link and the watermark predecessor, and a mismatch is a typed recovery
   refusal, never a partial prune.
7. The journal gains additive table tags for `HistoryTombstones` and
   `OutboxStatus`, and the attribution above. A binary that does not know them
   refuses the journal, fail-closed, as with ADR-0165's locator tags. Compatibility
   fixtures pin the new frame encoding.
8. The first frame still deletes the validated-prefix checkpoint. When the
   background scrub (ADR-0182 section 3) completes a full exact-end validation, it
   submits one administration transition that installs a fresh validated-prefix
   checkpoint for the prefix it validated. The writer installs it only if the
   watermark and head it validated against are still current, and discards it
   otherwise. No checkpoint is ever installed for a prefix no complete validation
   covered.

## Options considered

1. **Keep direct transitions and step right after each journal checkpoint
   (WP-811, commit 2440c3e76):** within ADR-0182 as written it measured a keep-up
   of 1.31% at p95 ratio 1.03. It still pays a residual drain and a separate
   durable engine commit per step, and it still loses the bounded restart. Kept as
   the interim implementation, not the target.
2. **Journaled frames (chosen):** the step rides the writer's group sync and the
   background checkpoint, the prune body moves off the gate, and correctness rests
   on the journal's existing expected-hash preconditions.
3. **Compact retained "husk" segments instead of relocating views per command:**
   fewer rows written per pruned command, but it changes the segment and tombstone
   formats and ADR-0165's locator model. Deferred until measurements under option 2
   show the relocation itself is the limit.
4. **Steps larger than 256 sequences:** violates STO-031. Rejected.

## Consequences

- Online retention stops holding the writer for journal drains and extra commits;
  its gate cost becomes sequencing and apply of a prepared frame.
- A retention-active database returns to the bounded restart path once the
  background scrub completes after pruning.
- Cost: a new journaled frame kind, two additive journal tags, and fixtures, plus
  fence revalidation at sequencing and preconditions that make a racing frame retry
  rather than apply.
- Deferred: husk segments (option 3), and replicating prune frames to followers,
  whose retention remains their own.

## Standing design tests

- **Interface safety:** nothing public changes. Retention stays configuration-driven
  and fenced; no application principal can start, target, or observe a prune frame.
  A frame whose preconditions moved cannot apply.
- **Scale:** population-proportional prune work leaves the single ordered writer's
  critical section. The writer's per-frame cost stays bounded by 256 sequences, and
  the scrub, not the writer, re-earns the checkpoint.

## Checks

- `journaled_prune_frame_crash_windows_recover_to_a_valid_watermark`: process kills
  and seeded simulation across preparation, sequencing, group sync, and checkpoint
  materialization.
- `a_prune_frame_whose_preconditions_moved_is_refused_at_sequencing`: a row, the
  watermark, and each fence moved between preparation and sequencing.
- Journal compatibility fixtures for the new tags and attribution, with the
  refusal of an older binary.
- `check-online-retention-interference` on the WP-811 evidence: p95 inside the
  five-percent bound and the keep-up fraction stated.
- All WP-761 crash, fence, and offline tests unchanged.
