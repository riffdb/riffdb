---
adr: "0279"
title: Finite Checkpoint Licensing For Sustainable Source Reclamation
status: proposed
tier: guarantee
date: 2026-10-07
accepted: null
acceptance: null
requires: [ADR-0265, ADR-0268, ADR-0271, ADR-0278]
amends: [ADR-0265, ADR-0268, ADR-0271, ADR-0278]
supersedes: []
requirements: [STO-030, STO-031, STO-032, REC-001, END-007, END-008]
packages: [WP-825]
obligations:
  - id: OBL-0279-1
    proof: checkpoint_license_reclaims_its_complete_finite_prefix_without_new_progress
    says: One independently durable checkpoint licenses multiple bounded cuts through its already covered prefix, then refuses without new independent progress.
  - id: OBL-0279-2
    proof: checkpoint_license_refuses_changed_lineage_holds_and_unclassified_publication
    says: Current validation, holds and exact checkpoint coverage are rechecked at every admission; changed or missing evidence never authorizes deletion.
  - id: OBL-0279-3
    proof: source_byte_continuation_matches_exact_pinned_full_walk
    says: Exact read-only byte accounting continues only through checked appends and removed preimages, matches an independent pinned oracle and refuses incomplete evidence.
review_triggers:
  - A license grows through reclamation, replay or a checkpoint containing no new independent progress.
  - Source accounting supplies deletion authority or omits a current hold or canonical validation.
  - Durable formats, public interfaces, ordering, maintenance pacing or workload limits change.
---
# ADR-0279: Finite Checkpoint Licensing For Sustainable Source Reclamation

## Context

The unchanged 1,500-second diagnostic on an 8-vCPU, 32-GiB Linux SSD host,
using native revision `f153ae48`, completes 150,000 commands and 600,000 reads
without retries but fails `source_bytes_sample_coverage`. Native output contains
one complete source-byte observation during load, while logical and physical
observations continue. At shutdown, 35,249 source receipts have been removed;
the source plan has resume 34,994 and eligible ceiling 336,768. The last physical
observation reports 1,918,873,600 reference-protected allocated bytes. These are
failed diagnostic observations, not qualification. Full CI remains incomplete.

A separate deterministic native regression independently checkpoints 1,030
previously observed receipts, then supplies a later independent checkpoint.
After 128 bounded maintenance attempts only 256 receipts are removed; the
assertion requiring the covered finite prefix to finish fails. The current
owner consumes independent checkpoint progress after each quantum. Increasing
checkpoint frequency or the quantum would retain that coupling or weaken a
bound. ADR-0278 preserves the previous per-cut licensing rule; changing it
requires exact human acceptance. Source-byte reconstruction starts a new exact
prefix walk per capture with a 30-second expiry and no checked-prefix reuse.
The failed observation alone does not identify every omitted capture or expiry;
approximate or missing evidence cannot qualify the workload.

## Decision

1. Amend ADR-0265 decision 8, ADR-0268 decisions 1 and 7, ADR-0271 decision 7
   and ADR-0278 decision 5 only for the online source owner. Replace spending
   independent checkpoint progress once per quantum with a finite license over
   the actual earlier observed prefix subsumed by a later independently durable
   checkpoint. Keep the existing first-observation requirement. No checkpoint,
   acknowledgement, elapsed time or caller claim substitutes for that evidence.
2. Bind the license to the validated source lineage, native continuation
   qualification, actual installed checkpoint evidence, independently committed
   progress and exact upper history point. Its ceiling cannot exceed the earlier
   observed checkpoint tail covered by the later checkpoint. Current source,
   checkpoint coverage, write fencing and every derived and durable hold must
   pass the existing validators at every admission. A license alone grants no
   mutation authority.
3. Permit multiple separately paced cuts below that fixed ceiling. Each removes
   at most 256 receipts and at most 32 MiB of resolved canonical preimages using
   the existing V5 proof, sole ordered writer and atomic completion protocol.
   Recheck the current floor and exact preimages for every cut. Shrink a quantum
   to its byte bound; never skip an oversized, corrupt or protected receipt.
4. Consume the license only through its own fully checked, acknowledged prefix
   deletions. Its remaining interval decreases monotonically and is exhausted
   when the actual resume reaches its ceiling. A blocking current hold preserves
   the license but grants no deletion. Reclamation,
   its checkpoint materialization, replay and repeated observations cannot
   extend or renew the ceiling or manufacture independent progress. Further
   licensing requires new independently committed progress and real later
   checkpoint coverage under the same first-observation rule.
5. Changed lineage, unclassified publication, lost qualification, incompatible
   restore, missing checkpoint coverage or failed validation revokes the license
   before admission. Hold changes invalidate the reusable evidence as required
   by ADR-0268; reconstruct it completely before reuse. Restart has no inherited
   process-local license: rebuild the native proof and checkpoint observations.
   Cancellation, refusal and uncertainty preserve existing fencing and recovery.
6. Continue exact read-only source-byte accounting through the same closed
   native publication classification. Establish the initial exact interval with
   canonical validation; validate each added receipt and subtract only checked
   removed canonical preimages. Bind each completed observation to one actual
   validated immutable pin, exact resume and eligibility ceiling. A changed
   ceiling or hold for which exact bytes are unavailable requires bounded
   validation before publishing; incomplete work stays unknown. Accounting
   cannot authorize deletion, alter the floor or substitute for source proof.
   Introduce a bounded read-only V2 numeric observation that distinguishes rows
   reused through qualification from newly checked rows and actual new scan
   pages. Do not report reused rows as scans performed during the current
   capture. Preserve the V1 reader and its exact meaning; final-source evidence
   freezes the V2 basis. Keep the existing 30-second capture freshness and
   95-second observation-gap limits, with no default or approximate counts.
7. Retain one license and one accounting certificate/candidate in the existing
   owner. Keep their combined new bookkeeping within the existing 64-KiB
   capacity and at most two validation snapshot pins in total. Share immutable
   pins where needed; do not add per-receipt or per-hold histories, mutation
   queues, durable byte indexes or a chain of historical snapshots. Checked
   counters revoke reusable evidence before overflow. Drop retired pins promptly.
8. Preserve all existing canonical, checksum, source/audit, hold, lineage and
   exact-end checks; V5, writer 4 and earlier compatibility fixtures remain
   unchanged. Keep the 100-ms FIFO admission deadline, bounded off-gate pages,
   per-turn writer limits, full residence accounting, maintenance rest and idle
   backoff. Source progress may schedule the existing paced next turn. No
   extra writer priority, forced checkpoint or drain, foreground pause, relaxed
   sampler freshness or workload reduction is introduced.
9. Preserve startup and its 64-MiB recovery-owned state ceiling, atomic replay,
   application/admin frontiers, acknowledgement durability and physical
   reference/reader protection. A virtual cut never permits deleting a referenced
   extent. Logical, source and physical observations remain separate and exact;
   emit only bounded numeric diagnostics. Add no public license, accounting,
   retry, deletion, validation or guarantee-bypass selector.
10. Qualify only after final-source CI and unchanged required-host preparation
    pass, then the complete short rehearsal covers all seven faults, independent
    reconciliation, three physically retired generations and six final quiet
    hours. Preserve failed attempts. Acceptance and synthetic tests alone do
    not establish sustainable throughput, accounting completeness or readiness
    for the 24-hour run.

## Options considered

1. **More frequent checkpoints:** keeps deletion throughput coupled to costly
   checkpoint cadence and does not repair repeated accounting work. Rejected.
2. **Larger quanta, relaxed freshness or paused foreground work:** weakens the
   accepted bounds or hides the measured limitation. Rejected.
3. **Unbounded reuse of a checkpoint:** would let maintenance perpetually
   stimulate itself and advance beyond independently covered data. Rejected.
4. **Finite covered-prefix license with checked accounting continuation:**
   selected; decouples bounded consumption from new checkpoint creation while
   preserving the finite independently evidenced deletion ceiling.

## Consequences

- A checkpoint can cover more than one reclamation quantum without requiring
  an extra independent transaction and checkpoint for each already covered cut.
- The owner must reconcile license exhaustion, current holds and exact byte
  continuation independently. A retained certificate is never proof of current
  admission, and missing accounting remains unknown.
- No storage migration, format fixture change, public surface or new dependency
  is required. Sustained host performance remains a measured gate.

## Standing design tests

- **Interface safety:** applications retain compiled writes, scoped authority,
  exact idempotency and durable acknowledgements. They cannot supply licensing
  evidence, change accounting, bypass a hold or opt out of a guarantee.
- **Scale:** every cut, scan page, proof, pin, queue, wait and diagnostic retains
  its existing bound. Reuse consumes a finite prefix; checked continuation
  avoids rebuilding already proven bytes for each observation or quantum.

## Checks

- Freeze the native 1,030-receipt regression; cover full finite consumption,
  exhaustion, new independent licensing and inability to stimulate itself.
- Deterministically change holds, checkpoint coverage, lineage and publication
  between preparation and admission; each invalidates or refuses safely.
- Compare byte continuation with an independent exact pinned full walk through
  append, cut, hold change, invalidation, cancellation, expiry and overflow.
- Process kills before acknowledgement, after append and during materialization
  preserve exact source state and acknowledged application outcomes. Restart
  rebuilds its license and byte evidence without trusting process-local state.
- Run final-source scoped acceptance and full CI; repeat the unchanged measured
  preparation and all complete short-rehearsal gates before any 24-hour run.
