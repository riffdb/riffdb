---
adr: "0271"
title: Fair Bounded Native Source-Reclamation Admission
status: accepted
tier: guarantee
date: 2026-10-05
accepted: '2026-10-05'
acceptance: 'maintainer, in session, 2026-10-05: Approved, in response to the revised exact-text ADR-0271 review request (version 9d70c483)'
requires: [ADR-0265, ADR-0268, ADR-0263, ADR-0269]
amends: [ADR-0268]
supersedes: []
requirements: [STO-030, STO-031, STO-032, REC-001, END-008, LOG-001]
packages: [WP-825]
obligations: []
review_triggers:
  - Source admission barges existing writers, adds an unbounded waiter list, or exceeds its fixed pending-request capacity.
  - A forced source drain exceeds the compiled transition, byte, decode or allocation limits.
  - Admission omits a current hold, source validator, independent checkpoint or exact lineage check.
  - Public controls, durable formats, command ordering, acknowledgement or maintenance pacing change.
---
# ADR-0271: Fair Bounded Native Source-Reclamation Admission

## Context

The unchanged 900-second required-host preparation on `645cda9f` completes
90,000 commands and 360,000 reads, but fails normal-window latency. Native
source planning observes no derived or durable source holds. Its last measured
pre-deletion plan has resume 8,707, prior checkpoint ceiling 219,782 and tail
366,549, while total receipt removal is only 8,962. Planning is not deletion or
physical eligibility evidence, but these current checks permit a large range
beyond the resume point. Persistent eligible-history growth cannot be counted
as protected history or repaired by pausing offered load.

ADR-0268 decision 6 requires nonwaiting drained admission. The current gate
refuses maintenance whenever any writer is held or queued; ordinary writers
have FIFO tickets. Source attempts therefore have no fair turn under sustained
queued work, and published journal data independently refuses the barrier.
The preparation records 391 busy-writer, 316 pending-journal and 127 page-bound
refusals. General native-repair approval does not accept a change to that
explicit decision. This proposed record requires exact human acceptance before
its admission implementation or package-scope changes.

## Decision

1. Amend only ADR-0268 decisions 1 and 6's source-admission scheduling and
   preparation of a drained root. Preserve its complete initial/continuation
   validation, closed mutation classification, one certificate/candidate,
   two validation pins, independently durable checkpoint evidence, all current
   holds, 256-receipt deletion bound and 64-KiB bookkeeping capacity. Preserve
   off-gate extent retirement, startup checks and validated-prefix refresh.
2. Give the graph's sole online source-reclamation owner at most one pending
   FIFO maintenance request in the existing sole-writer gate. A turn may
   capture the first drained validation pin or admit a complete candidate.
   Initial and incomplete validation authorize no reclamation; their full walk
   stays outside the gate. The request joins behind every existing ticket and
   gains no priority over earlier writers. Ordinary
   command and administration ordering, conflict ownership and acknowledgement
   remain unchanged; no second writer or lease is opened.
3. Pending admission has a compiled 100-ms deadline and graph cancellation.
   Waiting owns no writer lease. Expiration, cancellation or retirement removes
   eligibility for a future grant. Resolve grant-versus-cancel under the gate
   state lock; never execute a cancelled request or strand an ordinary ticket.
   Keep at most one fixed pending/cancelled maintenance slot; until its ticket
   is consumed, another maintenance attempt cannot allocate a second slot.
   No general cancellation set, callback queue or population-sized wait list.
4. Once granted, recheck write fencing and exact lineage. Before reclamation,
   recheck complete current source evidence and every current hold. Never wait for an asynchronous checkpoint while
   holding the writer; an incomplete checkpoint yields and releases the lease.
   Preserve fail-closed behavior on unavailable or changed qualification.
5. A granted source turn may materialize its already acknowledged published
   journal suffix through the existing native checkpoint/drain protocol only
   when exact native metadata proves the entire suffix contains at most 256
   transitions and 1 MiB of encoded bytes, and all existing frame, decoded-work
   and allocation ceilings also fit. Check those bounds before materializing;
   unknown accounting or an oversized suffix yields without draining. No
   partial drain, invented checkpoint, changed fsync boundary, commit-profile
   change or new journal encoding is authorized.
6. Validate every materialized transition and checked preimage with the existing
   canonical native machinery. Capture an actually drained successor root only
   after durable publication. Rebind the prior source proof through its actual
   closed materialization footprint when a prior proof exists. A first capture
   only starts bounded validation outside the gate. Unclassified or mismatched
   changes revoke qualification; no root counter or caller declaration
   substitutes for proof.
7. Close any remaining receipt/audit suffix only within ADR-0268's existing
   page row and byte limits, otherwise release and continue validation outside
   the gate. Use the existing independent-checkpoint and previous-observation
   rules to compute the deletion floor. Neither the reclamation receipt nor
   replay of already accounted evidence manufactures independent progress or
   a checkpoint that licenses its own deletion. Preserve every hold, resume
   anchor, retained-reader check and atomic source-reclamation receipt.
8. Charge the full actual gate residence, including bounded drain, validation,
   deletion, durable publication and failure cleanup, to existing maintenance
   pacing. Apply the same rest discipline before another source turn. Waiting
   time is separately observed and does not become extra maintenance budget.
   The admission deadline is not a filesystem completion deadline; no new
   hard real-time or sustained-throughput guarantee is claimed.
9. Timeout, cancellation, error and owner retirement release nondurable leases,
   requests and replaced pins. Commit uncertainty retains the existing native
   write fence and recovery protocol; it cannot produce a false removal receipt
   or discard acknowledged history. Reclamation still requires the current
   complete authority after any wait; a previously valid candidate alone cannot
   authorize mutation.
10. Keep the new machinery private to native graph-owned maintenance. Add only
    bounded numeric diagnostics for queued/granted/expired/cancelled turns,
    drain yields, actual gate/wait costs and actual removal. Application, CLI,
    SDK and MCP surfaces gain no timing, priority, drain, deletion, cache or
    validation-bypass controls. No public protocol or durable identity changes.

## Options considered

1. **Pause load or classify the backlog as protected:** conceals the measured
   native admission limitation and cannot qualify END-008. Rejected.
2. **Retry the same nonwaiting barrier more often:** changes sampling frequency
   but gives no FIFO turn and still refuses a published journal. Rejected as
   the progress design; bounded notifications remain an implementation detail.
3. **Unbounded forced drain or catch-up under the writer:** risks application
   starvation and breaks existing limits. Rejected.
4. **Journal the source-reclamation transaction:** requires a separate format
   and source-control publication design. Deferred.
5. **One bounded FIFO turn with a small closed native drain:** selected. Reuse
   current durable protocols and authority; bound admission and work explicitly.

## Consequences

- Background source reclamation can obtain a fair ordered turn instead of
  depending exclusively on gaps between queued foreground writers.
- A source turn can add bounded interference and a native checkpoint commit.
  The original workload, latency and resource gates must measure that cost.
- Oversized suffixes and incomplete checkpoints still yield. Acceptance does
  not qualify progress on the required host or complete the milestone.
- No sharding, replication acknowledgement, analytical features or broader
  transaction callbacks are introduced.

## Standing design tests

- **Interface safety:** applications and agents cannot express unsafe admission,
  pick a weaker durability mode, release holds or bypass source validation.
  Existing compiled writes, exact idempotency, authorization, bounded queries,
  typed freshness and acknowledged durability remain intact.
- **Scale:** keep one fixed request slot and existing bounded proof state,
  suffix/page/allocation ceilings and maintenance pacing. No full-history walk
  per cut or population-sized queue. The single-node, single-writer POC remains
  explicit; this record makes no distributed placement guarantee.

## Checks

- Deterministic queued-writer schedules prove FIFO order, an actual source turn,
  no barging, bounded capacity and grant/cancellation/retirement races.
- Native interleaved-write tests preserve exact outcomes, provenance, complete
  source validation, real holds and independent checkpoint licensing.
- Exact-limit and oversized suffix tests prove bounds precede materialization;
  corruption, stale evidence and incomplete checkpoint cases refuse progress.
- Process crashes cover bounded drain and source deletion/publication, preserving
  acknowledged commands and recoverable uncertainty with no false success.
- Scoped acceptance and full CI pass on the frozen final source. Required-host
  preparation retains 100 commands/s, 400 reads/s, 28 load and four auxiliary
  slots, unchanged latency checks and independent reconciliation.
- The complete short rehearsal still requires all seven faults, three actual
  retired generations and six final fault-free hours. The 24-hour qualification
  is a separate run; this decision alone starts neither run.
