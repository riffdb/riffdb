---
adr: "0278"
title: Journaled Bounded Source Reclamation
status: accepted
tier: guarantee
date: 2026-10-07
accepted: '2026-10-07'
acceptance: 'maintainer, in session, 2026-10-07: I accept, in response to the exact-text acceptance request for ADR-0278'
requires: [ADR-0263, ADR-0265, ADR-0268, ADR-0271, ADR-0272, ADR-0276, ADR-0277]
amends: [ADR-0268, ADR-0271]
supersedes: []
requirements: [AFC-001, AFC-002, STO-030, STO-031, STO-032, REC-001, END-008]
packages: [WP-825]
obligations:
  - id: OBL-0278-1
    package: WP-825
    proof: source_reclamation_progresses_without_draining_oversized_published_tail
    says: A fully validated published composite source permits a licensed bounded journaled cut despite an oversized active suffix, without materializing that suffix or changing current holds.
  - id: OBL-0278-2
    package: WP-825
    proof: journaled_source_reclamation_crashes_preserve_atomic_floor_and_receipt
    says: Process kills before acknowledgement, after durable append and during checkpoint materialization preserve acknowledged work, the exact source floor and receipt, and recover without process-local certificates.
  - id: OBL-0278-3
    package: WP-825
    proof: source_reclamation_writer_upgrade_refuses_predecessor_before_mutation
    says: Exact backup-bound writer-3 to writer-4 upgrades retain old fixtures and acknowledged data, and old binaries refuse the successor before engine open or replay.
review_triggers:
  - A cut omits current fencing, complete source validation, exact preimages or independent checkpoint licensing.
  - Journaled reclamation exceeds row, byte, proof, pin, allocation or writer-rest bounds, or adds writer priority.
  - A new frame kind is written under an old format identity or an old marker opens without its declared upgrade.
  - Application ordering, acknowledgements, public controls, unsafe Rust or dependencies change.
---
# ADR-0278: Journaled Bounded Source Reclamation

## Context

The unchanged 1,500-second preparation on `e6a4005b` fails
`source_bytes_sample_gap` after its first five-minute fault window. Logical
observations remain fresh, but there is no source-byte observation under load.
Native admission repeatedly finds oversized published suffixes; no unknown
accounting is reported. A deterministic native schedule establishes the same
barrier limitation after an actual independent checkpoint and 257 acknowledged
subsequent audit transitions. Off-gate checkpoint continuation alone does not
permit a source cut. These observations do not establish that every possible
checkpoint schedule fails, but they prevent qualifying the selected workload.

ADR-0271 requires a drained root and permits only a complete 256-transition,
1-MiB drain. It explicitly defers journaled source reclamation. Finding a small
post-checkpoint suffix remains a timing prerequisite under continuing writes.
Repeated waiting or larger drain limits do not remove that dependency. This
record selects a separate native publication design; exact acceptance must
precede implementation. Failed preparations and their artifacts remain intact.

## Decision

1. Amend ADR-0268 decision 6 and ADR-0271 decisions 1, 2, 4–7 only for the
   online source-reclamation owner. It may validate the current acknowledged
   composite source and publish its bounded cut through the existing ordered
   journal, without materializing the active suffix first. Keep offline source
   control and its existing drained barrier. Preserve ordinary ordering,
   acknowledgement, durability profiles, complete validation and every hold.
2. Generalize the existing native source validator and closed continuation to
   one immutable published composite pin. Validate the complete initial chain,
   source audits, registrations, fences and promotions before authority exists.
   Continue only through actual classified publications and checked preimages;
   never trust a root number, timestamp, caller assertion or incomplete walk.
   Retire replaced pins; keep one certificate, one candidate, at most two
   validation pins and the existing 64-KiB bookkeeping bound.
3. Prepare one prefix outside the writer: at most 256 receipts and at most the
   existing 32-MiB changelog-frame ceiling of resolved canonical receipt bytes.
   Record exact sequence, canonical digest and encoded length for each removed
   preimage, plus predecessor history and the retained resume point. Stream
   canonical validation using existing decode/allocation limits. Keep the proof
   inside 64 KiB; shrink a quantum to its byte bound, never skip an oversized
   or invalid row. A legal maximum-sized single receipt must remain reclaimable.
4. Admission keeps the single fixed FIFO source slot, 100-ms deadline, graph
   cancellation and no priority over earlier writers. Once granted, recheck
   write fencing, exact lineage, current published source/frontiers, certificate
   qualification and every current derived/durable hold. Close only an existing
   bounded validation page; otherwise yield and continue outside the gate.
   Unpublished work grants no authority. Do not wait for a checkpoint or perform
   a whole-suffix or whole-history scan under the writer.
5. Preserve the existing prior-observation and independently durable checkpoint
   rules for the deletion floor. Bind checkpoint licensing to actual installed
   checkpoint evidence, separately from the composite journal tail. A source
   reclamation frame, its acknowledgement, replay or materialization cannot
   manufacture the independent progress licensing its next cut. Missing or
   changed evidence refuses without advancing the floor.
6. Add journal frame version 5 with closed kind `SourceReclamation = 4`.
   Only V5 admits kind 4; retained old decoders still reject it. Existing
   command, audit and logical-prune kinds retain their exact semantics.
   The new kind describes only the checked source-prefix removal, source-history
   metadata/allocator successor and one existing V3 `HistoryReclamation` receipt.
   Preserve application and administration frontiers. No arbitrary table writes,
   command, event, outcome, provenance, projection or outbox mutation is allowed.
   Bound the entire encoded frame by the existing 16-MiB journal-frame ceiling.
7. Use the existing sole writer and journal completion protocol. The cut,
   retained resume anchor, allocator advance and canonical control receipt are
   one acknowledged durable transition. Publish its composite successor only
   through the existing durable boundary; acknowledge nothing beforehand.
   Rebase the certificate only through its own fully checked deletion. Abort,
   cancellation, error and uncertainty preserve existing fencing and recovery.
8. Checkpoint materialization and crash replay validate the closed frame and
   every bounded removed preimage against the actual prior source, applying the
   complete transition atomically. Recovery trusts no process-local certificate.
   Incomplete tails retain existing uncertainty rules; conflicting metadata,
   missing preimages, gaps and substitutions refuse rather than repair or skip.
   Keep bounded-root startup and its 64-MiB recovery-owned state ceiling.
9. Frame descriptors, overlay roots, checkpoint roots and readers retain every
   surviving physical receipt locator. Retire extents only through the existing
   off-gate reference/reader protocol after actual root retirement. A virtual
   source deletion does not authorize deleting a still-referenced extent.
   Logical, source and physical byte observers remain separate, bounded and
   read-only; capture source eligibility from a fully validated current pin.
10. Charge all actual writer residence, including validation, publication and
    failure cleanup, through the unchanged maintenance rest factor and cap.
    Preserve source-progress rechecks, idle backoff, cancellation, the 32-request
    workload bound and four auxiliary lanes. Add no force, pause, priority,
    durability, validation, retry or deletion selector to public interfaces.
11. Keep alpha epoch 1 and advance the format writer from 3 to 4. Advertise
    readable journal frames 2–5 and writable frame 5; preserve extent, storage,
    registry, backup and canonical V3 receipt encodings. Freeze writer 3's registry
    digest `2bca7f169fdce97af9f32f62b321259edf40743636e3d1bfbd60d88499e6fa6c`
    and compatibility digest
    `70a01a3ed4f619d759a027189a65ecba3cdd5804022c035f2df946edec6bf3dd`.
    Only that exact predecessor pair permits the existing backup-bound, one-way
    OfflineInPlace upgrade, with downtime and two source-size multiples of free
    space enforced natively. Preserve earlier reviewed predecessor edges.
12. Ordinary startup compares the immutable marker before engine open or replay;
    it cannot silently upgrade. Old binaries refuse writer 4. The upgrade retains
    immutable predecessor backups and acknowledged data; Pending completion
    earns eligibility only through the existing complete native proof. Current
    backups/restores use the successor identity. Retain old fixtures byte for
    byte; regenerate current manifests/inventory and review their actual diffs.
    Keep database, principal and command identities unchanged. Add no new
    dependency, unsafe Rust or public application surface.
13. WP-825 remains open until final-source acceptance, full CI, unchanged required
    host preparation, populated public restore and the complete short campaign
    pass. Preserve freshness, latency, resource, physical-retirement and final
    six-hour quiet-period gates. No 24-hour run is authorized by this record.

## Options considered

1. **Keep the drained-root timing dependency:** remains unqualified on the host.
2. **Raise drain limits or pause load:** increases interference or avoids proving
   continuing-load reclamation; does not provide the selected bounded design.
3. **Journal a closed source cut:** selected; source maintenance uses the existing
   durable writer with its own bounded checked transition and compatibility edge.

## Consequences

- Source progress no longer requires materializing unrelated active suffix work.
- A new frame, composite validator support and reviewed offline upgrade are costs.
- This design does not promise sustained throughput or a filesystem deadline;
  actual host latency, reclamation and all campaign gates still decide readiness.

## Standing design tests

- **Interface safety:** applications still use compiled commands and the shared
  authorization/runtime/coordinator. No caller can express reclamation, release
  a hold, choose weaker durability or bypass validation. Export remains supported.
- **Scale:** fixed FIFO capacity, bounded proofs/pins/rows/bytes and full writer
  rest; no population-sized mutation queue or whole-history admission scan.

## Checks

- The failing 257-published-transition regression must pass without draining the
  tail, changing its writer schedule or removing the independent checkpoint.
- Full-validator equivalence, exact-limit/overflow/preimage/gap refusals, holds,
  unknown mutations, cancellation, pin release and FIFO schedule coverage.
- Process kills at append, acknowledgement, publication, checkpoint, source
  deletion and recovery boundaries; repeated recovery preserves exact outcomes.
- V2–V4 compatibility vectors, new V5 closed-shape/canonicality fixtures, exact
  predecessor backup/upgrade tests and old-binary refusal before mutation.
- Handbook, scoped acceptance, full CI and unchanged real-host preparation;
  all faults, full resource reconciliation and populated restore remain required.
