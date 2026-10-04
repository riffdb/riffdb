---
adr: "0268"
title: Bounded Native Continuation Of Source Reclamation Proofs
status: proposed
tier: guarantee
date: 2026-10-04
accepted: null
acceptance: null
requires: [ADR-0265, ADR-0186, ADR-0263]
amends: [ADR-0265, ADR-0186]
supersedes: []
requirements: [STO-030, STO-031, STO-032, REC-001, END-008]
packages: [WP-825]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - A source proof survives an unclassified write, incompatible lineage, or changed validated before-image.
  - A hold is omitted, acknowledgement is manufactured, or a surviving authoritative row is rewritten.
  - Qualification uses a maintenance pause, raised workload limit, or repeated full-history validation per deletion batch.
  - Public controls, durable encodings, startup validation, or validated-prefix refresh guarantees change.
---
# ADR-0268: Bounded Native Continuation Of Source Reclamation Proofs

## Context

WP-825 reproduced zero source-receipt reclamation across 24 independently
durable writer turns after 600 commands. The same owner reclaimed 256 receipts
after an idle validation pass and a later checkpoint. The real application
preparation pruned 115,460 application sequences but reclaimed no receipts or
extents. Whole-root validation followed by exact-root admission starves when
writes change the root between pages. Repeating the entire retained receipt and
audit walk for each 256-row deletion also conflicts with END-008's scale gate.

The maintainer approved the necessary native repair scope on 2026-10-04. This
record proposes a different source-proof lifecycle, rather than relaxing client
checks or pausing the workload. It is proposed; exact human acceptance is
required before implementing this continuation protocol.

## Decision

1. Keep ADR-0265's single graph-owned source-reclamation owner, independently
   durable checkpoint requirement, existing drained barrier, every hold,
   at-most-256 receipt deletion, off-gate extent retirement and maintenance
   pacing. Preserve ordering, acknowledgements, authorization, durable formats,
   startup validation and validated-prefix checkpoint refresh.
2. Replace repeated whole-root source-proof reconstruction with one complete
   initial receipt-chain and source-audit validation followed by closed native
   continuation. Both initial and continuation work use existing canonical
   decoders, source-evidence validators, bounded pages and exact-end checks.
   Incomplete or failed work never grants reclamation authority.
3. A process-local storage owner tracks the complete source-validation footprint
   of every durable publication, including direct transactions, journal commits,
   checkpoints and source-control changes. Qualification is based on actual
   mutations and checked preimages, never caller declarations, elapsed time or
   a monotonically increasing root identity alone. Uncovered transitions
   invalidate the candidate and certificate before successor publication.
4. Ordinary append continuation is eligible only when the previously validated
   source interval and audit entries remain unchanged, lineage and source
   authority are identical, and the source hold set and its evidence are
   unchanged except for the closed acknowledgement advancement in decision 8.
   Validate every added receipt and audit entry to exact end against
   the newly captured pin, including predecessor hash/frontier continuity and
   existing registration, fence and promotion semantics. Missing rows, changed
   before-images, gaps, corruption or unclassified updates refuse continuation.
5. Continue validation across qualified append publications rather than discard
   a valid ancestor whenever unrelated application work publishes. Bind the
   completed result to the exact successor state only after every intervening
   source change has been checked. Retire replaced pins promptly; never retain
   a chain of historical snapshots or an unbounded queue of mutation evidence.
6. Admission still acquires the existing nonwaiting drained writer barrier.
   Recheck the complete current fencing set and exact lineage. A remaining
   suffix may be closed under that barrier only within the existing per-page
   receipt/audit row and byte limits; otherwise release it and continue outside
   the gate. Charge all gate work to existing maintenance pacing. Never perform
   an unbounded catch-up or a full-history walk while holding the writer.
7. One successful reclamation may continue its certificate only through its
   own closed, fully checked prefix deletion. Validate exact removed preimages,
   the new retained resume anchor, unchanged surviving source evidence and its
   canonical reclamation receipt. Rebase the certified retained interval after
   durable commit; do not reuse deleted evidence. The reclamation receipt cannot
   manufacture the next independently durable checkpoint.
8. Acknowledgement advancement may continue only for the same retained source
   identity and generation, with exact prior evidence, monotonic checked frontier
   and the existing native hold/audit validators. Recheck all current holds at
   admission; never invent acknowledgement or advance a bootstrap hold.
   A hold registration, retirement, source fence, promotion, restore, incompatible
   catalog/lineage transition, unclassified mutation or lost qualification
   invalidates continuation. Reestablish the affected evidence with the existing
   complete validators before reclamation resumes. No fallback drops holds or
   treats missing evidence as an empty set. Unavailability preserves data.
9. Keep at most one certificate and one candidate per owner, with at most two
   validation snapshot pins. New bookkeeping has a compiled 64-KiB capacity
   bound; existing native validator state, decoded work and hold collections
   retain their existing row/frame, collection and allocation bounds. Do not
   introduce an additional per-hold history or mutation queue.
   Checked counters invalidate qualification before overflow rather than wrap.
   Failure, cancellation and graph retirement release all nondurable pins.
10. Diagnostics distinguish initial validation, continuation, invalidation,
    admission refusal, real holds, actual receipts removed and actual physical
    retirement. Emit bounded counts, states and durations only. No payload,
    tenant, capability, path or machine identity. No public cache, certificate,
    maintenance-pause, deletion or validation-bypass control is introduced.

## Options considered

1. **Pause writes to finish validation:** hides the application liveness failure
   and cannot qualify the continuing-load rehearsal. Rejected.
2. **Trust an old root after arbitrary writes:** loses current fencing and
   corruption checks. Rejected.
3. **Hold the writer for the full walk:** makes foreground delay grow with
   retained history and violates bounded maintenance. Rejected.
4. **Closed native continuation:** chosen; complete initial validation plus
   proven unchanged preimages and checked source deltas establish each current
   proof without re-reading all previously validated history per deletion.

## Consequences

- Sequential append workloads can reclaim while writes continue; this must be
  demonstrated, not inferred from the continuation design.
- Every durable publication path needs explicit footprint coverage. An omitted
  path must invalidate qualification safely, even if that delays reclamation.
- Source-authority changes can require a new complete validation. This record
  does not promise progress under perpetual incompatible control-plane churn.
- No new engine, dependency, persistent identity, format or application surface.
- The seven real faults, physical retirement, normal-load latency, recovery and
  final six fault-free hours still gate the milestone. No 24-hour run starts
  until the complete short rehearsal and required implementation checks pass.

## Standing design tests

- **Interface safety:** compiled writes and existing authority remain mandatory;
  applications cannot issue proof assertions, choose holds or request deletion.
- **Scale:** bounded bookkeeping, pins, pages and barrier work; ordinary append
  progress must not cause one full-history scan for each deletion batch.

## Checks

- The existing failing
  `production_receipt_reclamation_progresses_with_interleaved_durable_writes`
  must pass without changing its writer schedule or idle positive control.
- Deterministic capture/append/catch-up/admission, hold changes, unknown mutation,
  preimage substitution, gap, cancellation, overflow and final-pin release tests.
- Increasing retained populations measure actual source rows read: one initial
  validation plus new source work, not repeated retained-population scans.
- Full-validator equivalence against independent pinned-state oracles, including
  source registrations, primary fences, promotion and older-incarnation audit.
- Process kills at qualification, receipt deletion and certificate publication;
  recovery trusts no process-local proof and preserves exact outcomes and holds.
- Real continuing-load preparation must show receipt and extent progress with
  existing latency, concurrency and maintenance-interference gates intact.
- Scoped acceptance, handbook updates, compatibility checks and full CI.
