---
# Originally ADR-0280, then ADR-0283. Local main now owns ADR-0283.
# This identifier-only reconciliation preserves the exact accepted decision
# and original acceptance references; identifier reconciliation approved
# by the maintainer in session, 2026-10-10.
# Local main now owns ADR-0286. This identifier-only move to ADR-0296
# preserves the original acceptance and exact Decision; identifier reconciliation
# approved by the maintainer in session, 2026-10-13: "Approve the identifier-only reconciliation".
adr: "0296"
title: Bounded Native Follower Acknowledgement Validation
status: accepted
tier: guarantee
date: 2026-10-07
accepted: '2026-10-07'
acceptance: 'maintainer, in session, 2026-10-07: i accept, in response to the exact-text acceptance request for ADR-0280; renumbering to ADR-0283 and reference updates approved by maintainer, in session, 2026-10-09: "i approve"; identifier-only ADR-0283 to ADR-0286 reconciliation approved by maintainer, in session, 2026-10-10: "Approve the identifier-only reconciliation"'
requires: [ADR-0240, ADR-0248, ADR-0268, ADR-0271, ADR-0279]
amends: [ADR-0240, ADR-0268, ADR-0271]
supersedes: []
requirements: [REP-003, REP-006, REC-001, END-008, LOG-001]
packages: [WP-825]
obligations: []
review_triggers:
  - A proof omits canonical source, audit, registration, fence, promotion or hold validation.
  - Acknowledgement is invented, hold identity or generation changes, or an unclassified publication preserves qualification.
  - Registration, retirement, bootstrap attachment, promotion, startup, ordering, durability, public protocols or existing workload bounds change.
  - Validation runs as a full-history walk while holding the writer, or proof storage exceeds the existing bounded owner capacity.
---
# ADR-0296: Bounded Native Follower Acknowledgement Validation

## Context

The frozen `c59c42a0` full validation on an 8-vCPU, 32-GiB Linux SSD host
fails two replication process tests. The unchanged frontier test also fails
alone. A separate timing-only checkout preserves its 60-second wait and
assertions: 24 completed source acknowledgements consume 60.356 seconds,
increasing from 224 ms to as much as 3,483 ms. The test stops at application
frontier 380 of 408 after 102.38 seconds. These failed debug diagnostics do not
establish release throughput or campaign qualification. Missing Graphviz and
Python YAML prerequisites are separate host setup failures and are corrected.

The source acknowledgement path calls `Barrier::acquire`, which fully walks
retained history before and after journal drain. `Barrier::begin` and the
post-staging `ControlWrite::validate` each walk it again. This work holds the
writer barrier and repeats for each report. ADR-0268 permits closed acknowledgement
continuation in a reclamation proof; ADR-0271 bounds reclamation admission.
Neither explicitly replaces the live acknowledgement operation's full barrier
protocol. Extending that authority requires exact human acceptance of this record.

## Decision

1. Extend bounded source-proof preparation and admission only to advancing an
   existing follower acknowledgement hold. Preserve the source's actual
   registered identity, generation, lineage and lifecycle, the receiver's
   independently durable acknowledgement, every other current hold and the
   existing canonical acknowledgement validators. This grants no registration,
   retirement, bootstrap attachment, archive acknowledgement or promotion path.
2. Establish one complete initial receipt-chain, source-audit, registration,
   fence, promotion and hold proof with the existing validators and exact-end
   checks. Perform this initial walk outside the writer barrier in bounded
   native pages. Incomplete, cancelled or failed work authorizes no mutation.
   Startup, offline validation and recovery retain their complete validators.
3. Continue a completed proof only through actual closed, predecessor-checked
   native publications under ADR-0268. Validate every new receipt and audit
   entry and all affected source evidence against the actual successor pin.
   Root counters, elapsed time, a capability assertion or the receiver's
   claimed position alone cannot establish source ancestry or qualification.
4. The existing graph-owned source-control owner retains at most one certificate
   and one candidate, sharing its source validation with reclamation when safe.
   Keep at most two validation snapshot pins and the existing combined 64-KiB
   new-bookkeeping limit. Existing decoded-work, page, frame, hold and allocation
   ceilings still apply. Add no per-follower proof history, mutation queue,
   retained snapshot chain or population-sized cache. Unknown bounds refuse.
5. Prepare before submitting the mutating control operation to the existing
   commit coordinator. Preparation owns no writer lease and cannot occupy a
   coordinator execution turn for its full walk. A request keeps its original
   deadline, cancellation and bounded capacity. Submit only the closed typed
   acknowledgement operation; no arbitrary transaction callback or second writer.
   Existing queued command and administration order remains FIFO. Yielded work
   cannot reserve priority or pass an earlier queued writer.
6. At the existing ordered writer turn, recheck write fencing, exact current
   lineage, the actual prior hold bytes, registered generation and phase, the
   acknowledged receipt/hash/frontier, all current holds and source qualification.
   Rebind only through the checked source footprint. Close remaining receipt
   and audit work only within the existing native per-page row and byte limits;
   otherwise release the turn and continue outside the gate. Never walk retained
   population or wait for an asynchronous checkpoint while holding the writer.
7. Prepare a drained root only through the existing bounded source protocol:
   at most 256 transitions and 1 MiB of encoded suffix, with all decode and
   allocation limits established beforehand. An oversized, unknown, unpublished
   or asynchronously checkpointing suffix yields. No forced unbounded drain,
   checkpoint invention, foreground pause or changed fsync boundary is permitted.
8. Stage the exact monotonic hold advancement and its canonical source receipt
   in the existing single atomic Immediate control transaction. Its sealed
   write owner checks the complete actual mutation inventory and exact prior
   bytes, successor roots, receipt continuity, source policy and all current
   fencing. Only that closed operation may establish the successor proof
   inductively; unrelated raw writes invalidate it. A complete predecessor proof
   plus these checked deltas replaces repeated full-history scans in that turn.
9. After acknowledged durable publication, continuation across that owner's
   hold advancement requires the same identity and generation, exact old/new
   policy bytes and the existing native hold/audit validators. Lost qualification,
   changed before-images, missing evidence, overflow, incompatible lineage,
   registration, retirement, attachment, promotion, restore or unclassified
   publication revokes reuse. Reestablish complete affected evidence before
   another operation. Never drop a hold, manufacture an acknowledgement or treat
   missing source evidence as an empty set. Reclamation still checks its current
   floor and independently durable checkpoint license; this proof cannot renew it.
10. Cancellation and refusal release nondurable capacity, candidates and pins.
    Once mutation is submitted, preserve existing uncertain-completion custody,
    fencing and restart recovery. No process-local proof survives restart.
    Charge any shared background preparation and writer work to existing source
    maintenance pacing; report actual validation, waiting and gate residence
    separately. Keep existing request deadlines and the 32-frame report cadence.
11. Preserve public application, CLI, MCP, SDK and replication protocols, durable
    envelopes and compatibility fixtures. Add no public proof, cache, timing,
    acknowledgement, drain, validation-bypass or durability selector. Diagnostics
    contain bounded numeric counts, durations and closed failure classes only.
    This record changes no ordering boundary, atomic outcome semantics or
    acknowledged-write durability and makes no new throughput or real-time promise.

## Options considered

1. **Raise deadlines, reduce the fixture, pace reports less often or serialize
   until a repeat passes:** does not repair population-dependent native writer
   work. Rejected as the repair or qualification evidence.
2. **Memoize by root counter or trust receiver progress:** fails to prove current
   source ancestry, hold custody and complete source validation. Rejected.
3. **Remove duplicate scans but retain a population walk in each writer turn:**
   lowers the constant while retaining the scaling limitation. Rejected as the
   architecture for live acknowledgement.
4. **Complete initial validation and bounded checked continuation:** selected;
   reuse native proof machinery while preserving every semantic validator and
   measuring real writer interference and progress.

## Consequences

- Ordinary acknowledgement progress can avoid repeated retained-population walks
  and an increasing writer stall. Actual required-host measurements must prove it.
- Unknown and incompatible source transitions may still require a fresh full
  off-gate validation. This does not promise progress under perpetual authority
  churn or ignore native refusal to meet a deadline.
- The private control preparation/result protocol and its cancellation ownership
  need coordinated storage, server and commit changes; no application facade changes.
- Follower command-image catalog decoding is separately measured repeated work;
  this record grants no catalog-validation bypass or unrelated caching change.
- Failed CI and diagnostics remain failed. Final-source full CI, unchanged
  preparation and the complete short campaign still gate 24-hour readiness.

## Standing design tests

- **Interface safety:** applications cannot assert proof, set an unsafe limit,
  weaken durability, release custody or bypass authorization. Existing shared
  service authority and compiled command writes remain mandatory.
- **Scale:** one bounded owner, certificate/candidate, two pins and bounded
  preparation/admission work; no full retained-history walk per report or under
  the writer. Preserve all workload and resource gates.

## Checks

- The unchanged frontier and bootstrap/crash process tests pass on the required
  host with their existing deadlines, workloads and exact byte/frontier assertions.
- An independent complete pinned-state oracle agrees with initial and continued
  acknowledgement proofs and all refusals, including registered and legacy holds.
- Increasing retained populations count actual old and new rows visited; after
  initial validation, advancing reports check new work and current bounded evidence.
- Deterministic schedules cover live appends, queued writers, suffix limits,
  changed generations, old/equal/stale acknowledgements, unknown mutations,
  corrupt receipts/audit links, cancellation, overflow and final pin release.
- Process kills at preparation, staged hold, source receipt and durable
  publication preserve exact commands, outcomes and recovery without proof reuse.
- Scoped acceptance, handbook maintenance and frozen final-source full CI pass
  on the required host. All testing stays on that host. No 24-hour run starts.
