---
adr: "0300"
title: Bounded Owned Backup Validation Read Intervals
status: accepted
tier: guarantee
date: 2026-10-13
accepted: '2026-10-13'
acceptance: 'maintainer, in session, 2026-10-13: approved the exact ADR-0300 and WP-825 registration review; proposed text SHA-256 7f6e3003f76a5fe998f556ceb5ac317bdabb196738537ddf3c6f39c380a3d561'
requires: [ADR-0293, ADR-0299]
amends: [ADR-0299]
supersedes: []
requirements: [REC-001, REC-004, REC-005, REC-006, REC-008, END-001, END-008]
packages: [WP-825]
obligations: []
review_triggers:
  - A parsed value escapes its unfinished source-read interval.
  - Unrelated readers acquire interval authority or ambient resolver fallback.
  - Original mutation or cancellation can be hidden by refreshing a seal.
  - Any existing resource, validation, compatibility or timing gate changes.
---
# ADR-0300: Bounded Owned Backup Validation Read Intervals

## Context

The saved whole 60-second primary-loss gate still fails at `ca7ec010`.
Same-source release diagnostics using the same original backup take 71.77
seconds with borrowed history and 48.70 seconds with complete copying.
Complete retained-history capture takes 30.86 versus 11.13 seconds; surviving
command hydration takes 8.05 versus 1.55 seconds. These longer-timeout probes
qualify neither recovery gate. Their independently verified receipts are in
WP-825-OWNED-BACKUP-HISTORY-REVIEW.md. Borrowing currently adds more work than
its temporary-copy removal saves.

ADR-0299 checks original custody around each frame before returning its decoded
value. Native restore needs those values only inside private hydration and
complete history validation. A bounded private interval can retain the actual
original owner across several reads, then verify its custody before any result
can authorize a committed preparation batch or completed validation proof.
This changes the custody-check boundary and requires explicit acceptance.

## Decision

1. Apply only inside ADR-0299's eligible ordinary native private restore, after
   complete original-artifact verification. Preserve the complete-copy path for
   unsupported states. Add no application selector, serialized authority or
   change to archive, follower, promotion or ordinary operational readers.
2. The actual continuously retained original-backup owner may create one private
   validation-read interval for the exact operation, database, incarnation,
   original inventory and private engine generation. Bind it to the existing
   native preparation or validation owner and its original descriptors. A
   decoded manifest, cached checksum, database identifier or unrelated resolver
   cannot create, reconstruct or substitute for that authority.
3. Bound each interval by both 256 frames and 64 MiB of physical frame reads,
   without increasing any existing frame, scan, memory, descriptor, queue or
   worker limit. Stream with existing admitted single-frame scratch; retain no
   population cache or new read-ahead buffer. Finish the current interval before
   opening the next. All original read positions remain independently owned.
4. Before first read and after the last read in each interval, verify the entire
   original closed inventory and manifest, pinned directories, original
   descriptor/name identities, regular single-link status, lengths,
   modification/change times, cancellation and private shadow absence. Use
   the prior recorded seals, never new observations that reseal changed files.
   Preserve all other existing custody handoff checks.
5. Inside a live interval, individual frame reads may reuse that retained
   custody instead of repeating filesystem identity checks. Check cancellation
   and sticky owner invalidation throughout reads and decoding. Preserve every
   frame-size, checksum, footer, generation, database, canonicality, admission
   and semantic check. Resolve only through that interval's exact original
   artifacts; never try another resolver after a custody failure.
6. Parsed values inside the interval are unfinished private input. They may
   advance only interval-local validation accumulators or the existing private
   preparation write batch. No parsed value, trusted accumulator, authorization
   reader or completed history proof may escape before successful interval
   completion. Verify interval completion before committing its hydration batch.
   Do not retain an interval across an unrelated transaction or mutation.
7. Keep the complete retained-chain, source-hold, registration-link,
   primary-admission and cutover checks on the critical path. Mint ADR-0293's
   same-state history proof only after every required interval and the complete
   validation finish successfully, under the original transaction-current
   roots, native mutation generation and cancellation state. Neither a finished
   batch nor an artifact checksum establishes that complete semantic proof.
8. A changed identity, unexpected write, missing/extra/shadow file, link,
   cancellation, uncertain I/O, unfinished drop or failed interval permanently
   invalidates its actual owner. Abort the private preparation batch or discard
   all unfinished validation state. Do not retry with fresh seals, fall back to
   copied content or accept another resolver after that failure.
9. Preserve complete original-backup content revalidation at publication, full
   final source seals and independent complete transferred-target readback.
   Preserve source-reference retirement, fresh authorization, receipt-authorized
   stamp, no-overwrite transfer, marker-last ordering, synchronization,
   StartupConsumed, fresh startup, durable Succeeded and admission fencing.
   A restart reconstructs no interval or semantic proof from durable metadata.
10. This amends only ADR-0299 Decision 5's per-read custody boundary for the
    explicitly bounded private intervals above. Outside them, retain the
    existing checked individual/frame reads. Preserve public interfaces,
    authorization, ordering, atomicity, idempotency, acknowledged durability,
    all durable encodings and compatibility fixtures. Keep the one 60-second
    primary-loss clock, fixed 45-second campaign window, scheduled population
    and every resource gate. No timing or campaign success is promised.

## Options considered

1. **Keep per-frame filesystem checks:** simple custody, but measured borrowing
   regresses restore and complete history validation substantially.
2. **Return permanently to copied history:** retains existing guarantees and is
   the diagnostic control, but retains temporary copy and verification work.
3. **Bound private reads under one retained owner:** selected; amortizes custody
   checks while preventing unfinished values from granting authority.
4. **Cache successful checks globally or defer history validation:** rejected;
   neither binds actual source custody to the consuming validation operation.

## Consequences

- Requires an explicit unfinished-input lifetime and failure-safe consumption.
- Introduces no durable certificate, public bypass, dependency or unsafe Rust.
- Performance improvement remains an experiment; complete-copy evidence and
  all previous failed gates remain preserved and cannot be reclassified.

## Standing design tests

- **Interface safety:** public callers cannot start intervals, inject a resolver,
  construct proof, skip original checks or write before ordinary admission.
- **Scale:** at most one admitted frame is decoded at a time; intervals do not
  raise existing limits or retain population-sized state or extra workers.

## Checks

- First preserve the failed shipping gate and same-source copied/borrowed
  comparison. Add a failing work-bound regression before implementation.
- Assert full custody checks scale with completed bounded intervals, while every
  frame is still fully decoded and checked. Exercise both interval limits.
- Inject restored-mtime writes, replacement, links, missing/extra/shadow files,
  cancellation and I/O failure between reads and at completion. Refuse before
  any result escapes or hydration batch commits; retry and unfinished drop
  cannot restore authority. Include a competing same-database resolver.
- Assert different operations, engines, transactions and concurrent readers
  cannot inherit interval authority. Explore the interval/cancellation schedule.
- Compare copied and interval results with exact command outcomes, provenance,
  events, history roots, incarnation and independent full-state assertions.
- Exercise process-crash cuts during hydration and validation; original backup
  hashes remain unchanged and partial stages grant no publication authority.
- Retest the closest failed shipping restore, then required-host acceptance and
  full CI, scheduled population, resources and every unchanged gate before the
  eight-hour rehearsal or 24-hour campaign. Diagnostics qualify neither.
