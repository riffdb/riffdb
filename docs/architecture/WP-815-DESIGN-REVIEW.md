# WP-815 design review: bounded pruning and whole-file reclamation

Package: WP-815. Tier: guarantee. Status: correction accepted by the maintainer in session on
2026-09-30 ("approved"); implementation and qualification are in progress. The dependency WP-814 is complete on the worktree base.

## Conflict found before implementation

ADR-0263 Decision 6 and WP-815 require one administration transition to
materialize an entire sealed extent's retained views and advance the watermark,
while retaining the hard 256-sequence limit of STO-031. WP-814 permits an extent
to contain up to 65,536 transitions. These requirements cannot both hold for
an extent containing more than 256 application sequences.

The test `a_sealed_extent_can_exceed_the_online_retention_step_bound` builds
257 consecutive one-command frames, seals them using the real journal media,
and validates every frame and the sealed footer. No corruption, oversized
frame, or direct-command gap is needed to reproduce the conflict.

There is also an independent reference lifetime: application retention deletes
COMMITS rows, but leaves V3 HISTORY receipt stubs pointing into the same
extents. The existing source checkpoint/reclamation protocol owns deletion of
those receipts under its own holds. An application watermark alone cannot
authorize deleting or recycling an extent. Copying those receipt bytes into
redb would violate ADR-0263 Amendment 1.

## Accepted exact amendment to ADR-0263

Decision 6 and A6 distinguish logical pruning from physical reclamation:

1. Online logical pruning uses administration transitions covering at most
   256 application sequences each. Each transition atomically materializes
   retained outcome, provenance, audit, and route views, deletes whole command
   rows and the covered event/outbox rows, appends its tombstone, and advances
   the application retention watermark under every existing fence. The first
   transition invalidates the validated-prefix checkpoint as before. A logical
   pruning boundary may lie inside a sealed extent; extent bytes remain
   immutable. Commands in an unsealed extent wait for sealing.
2. Physical reclamation deletes or recycles only an entire sealed extent,
   after durable logical pruning has covered every application command in it
   and the existing source checkpoint/reclamation protocol has durably removed
   every V3 receipt reference into it. The retained resume-anchor receipt and
   every source/follower/projection hold retain their existing meaning. No
   acknowledgement, checkpoint, detach, or receipt deletion is manufactured to
   make a file eligible. Unreadable or inconsistent evidence refuses reclamation.
3. File reclamation runs off the ordered writer gate with a bounded pass and
   at most one extent operation per pass. It cannot advance an allocator,
   watermark, or consumer frontier. A crash before or after the file operation
   must leave durable authoritative state sufficient to retry safely. Reuse
   must install a fresh extent header and generation without exposing old bytes
   as new frames. No command segment or journaled receipt is copied elsewhere.
4. A target inside an extent may make bounded logical progress while physical
   space remains allocated. Diagnostics and evidence report logical pruning,
   physically reclaimed/recycled extents, and receipts or other fences still
   retaining files separately. The five-percent p95 interference gate and
   keep-up fraction greater than 0.0131 remain mandatory; logical progress alone
   does not establish sustainable physical storage.
5. Offline retention remains available. It preserves references and immutable
   extent bytes; its existing journal rebase remains required. This amendment
   adds no public operation, durable identity, or relaxed application guarantee.

WP-815's objective and deliverables must reflect this distinction in the same
governance commit before implementation starts.

## Alternatives

Sealing an extent after at most 256 application sequences would also avoid the
sequence-limit conflict, but it would discard WP-814's measured batching choice
and add many seals, files, and extent allocations. It still needs the independent
receipt-reference eligibility check. Raising the prune-step limit would weaken
STO-031 and is not proposed.

## Required proof and evidence after acceptance

- Deterministic pruning across a sealed extent larger than 256 sequences, with
  every step bounded and every retained view and outbox/follower fence intact.
- Receipt references and the retained resume anchor keep the extent alive after
  application pruning; only legitimate source reclamation makes it reusable.
- Process kills at retained-view materialization, tombstone/watermark commit,
  reference removal, file retirement, and spare installation.
- Restart, backup/restore, and surviving readers resolve the same history;
  missing retained extents continue to fail closed.
- Counterbalanced 32-client interference evidence passes the unchanged checker
  and discloses physical reclamation and any remaining receipt retention.

## Implementation review (SPEC 19.7)

- Package: WP-815. Tier: guarantee.
- Behavior added or changed: sealed command rows prune in bounded logical steps;
  a separate pass validates at most one sealed extent and retires it only after
  application and source-history references have been durably removed. Older
  captured roots and background scrubs retain files. At most one retired file
  supplies the next spare; a fresh generation fences every preserved old frame.
- Checks run: the initial storage suite passed (783 unit tests, the integration
  targets, and 212 recovery-matrix tests). The harness's 28 unit tests pass.
  Scoped acceptance without tests passes all 14 checks. Full CI and fresh
  counterbalanced performance qualification are still in progress; this package
  is not closed by those preliminary results.
- Compatibility: no storage key, journal encoding, application operation, or
  configuration key changes. Offline retention and journal rebasing remain.
  The existing shutdown-release V1 registry remains unchanged; a separate
  `riffdb-retention-extents-v1` record exposes five bounded numeric counters.
- Handbook pages updated: [Configuration](../configuration.md#online-retention),
  [Known limitations](../known-limitations.md#retention), and this review page,
  reachable from the handbook summary.
- Hazards and follow-ups: source-history reclamation has no composed production
  scheduler in this POC. Logical progress cannot establish physical steady state;
  live receipts may keep every file allocated. Long-lived snapshots or scrubs
  conservatively hold all extents. Pool contents are disposable after a crash;
  startup discards them only after authoritative journal recovery. Composed
  outbox, archive/export, and follower lifecycle qualification remains separate.

Implementation decisions within the accepted amendment:

- Keep the existing retained-view, tombstone, outbox, consumer, and follower
  fences; resolve canonical row bytes before charging the prune byte budget.
- Probe at most 64 generations per pass with a process-local fair cursor. Check
  the bounded checksummed footer before walking frames, then recheck current
  authoritative references under the reader-capture reservation. An inconsistent
  incarnation, footer, or lingering reference refuses reclamation.
- Track at most 4,096 captured checkpoint roots using weak references. Release
  stale internally cached roots, preserve externally held roots, and fail closed
  at the tracking bound. Reserve capture only through file namespace removal;
  directory sync and full extent validation run outside that reservation.
- Reuse existing allocated frame blocks and allocate only the trimmed tail off
  the writer. Preserve the existing alternate-header, generation, and rotation
  protocol; never interpret retained old frames as new-generation data.
- Report physical deletion/recycling, reference holds, reader holds, and off-gate
  reclamation time separately from logical sequences. The benchmark parser rejects
  changed registries, duplicate fields, negative/overflowing counters, and counts
  that change after the worker has shut down.

No performance improvement or sustainable physical-storage claim is made before
qualification.
