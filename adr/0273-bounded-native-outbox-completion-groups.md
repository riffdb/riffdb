---
adr: "0273"
title: Bounded Native Outbox Completion Groups
status: accepted
tier: guarantee
date: 2026-10-06
accepted: 2026-10-06
acceptance: 'maintainer, in session, 2026-10-06: "Approved" in response to the exact ADR-0273 proposal at 4ed57e5a'
requires: [ADR-0265, ADR-0267, ADR-0269]
amends: [ADR-0269]
supersedes: []
requirements: [EFF-002, EFF-003, REC-001, END-008]
packages: [WP-825]
obligations:
  - id: outbox-completion-group-atomicity
    package: WP-825
    proof: outbox_completion_group_preserves_exact_status_cas_and_atomic_receipt
    description: Checked success groups preserve reciprocal validation, per-event CAS and one atomic captured receipt.
  - id: outbox-completion-group-crash
    package: WP-825
    proof: outbox_completion_group_crash_redelivers_without_losing_archive_records
    description: Native process crashes before and after grouped completion preserve archive deduplication and pending retention fences.
  - id: outbox-completion-group-bound
    package: WP-825
    proof: outbox_completion_group_flushes_at_bounds_and_before_retry
    description: Successful completions flush at sixteen entries, page end and before ordinary failure handling without claiming ahead.
review_triggers:
  - Claims, archive commits, retries, dead letters or renewals are merged or acknowledgements precede durable destination acceptance.
  - Group size, receipt limits, FIFO writer policy, journal format or selected commit profiles change.
  - External I/O occurs under the writer lease, or uncertain completion loses its recovery and retention fence.
---
# ADR-0273: Bounded Native Outbox Completion Groups

## Context

Final-source CI and scoped acceptance pass on `81644271`, but its required-host
900-second sink diagnostic ends with `sink_outage_incomplete`. All offered
commands and reads complete without retries; recovery windows overflow the
latency histogram. Native timing observations after resumption show about
1.27–1.47 ms per archive record-commit attempt and 1.58–1.75 ms per primary
commit-finish attempt, with two primary transitions per archive acceptance.
Counts are attempts and timings overlap; these observations do not isolate
fsync or prove a causal throughput ceiling. A separate complete native scan of
an earlier failed run's private copy confirms 13,004 pending source events.

The dispatcher durably claims, archives and completes each event separately.
Successful completion granularity is a concrete native review target, rather
than a reason to reduce the workload or weaken recovery. ADR-0269 decision 9
explicitly leaves transaction merging to a separate review; this record supplies
that review for bounded successful completions only. Broad repair authorization
does not accept this exact text. Runtime implementation waits for acceptance.

## Decision

1. Amend ADR-0269 decision 9 only to permit one primary Immediate transaction
   to complete a checked group of at most sixteen successfully archived events.
   Keep the selected primary commit profile, archive two-phase Immediate
   per-record commits, directory syncs and existing journal protocol unchanged.
   Claims, renewals, retries and dead letters retain individual transactions.
2. Keep delivery sequential in the existing recovered dispatcher. For each
   event retain eligibility, attempt-limit and destination-availability/quota
   preflight, the exact durable claim and existing lease/timeout policy before
   connector I/O. Do not claim ahead, reserve a group of archive records or
   perform any connector I/O while holding the primary writer lease.
3. After destination acceptance, construct the existing checked success
   transition using that event's exact Delivering observation and its actual
   completion timestamp. Retain only those transitions and bounded report
   metadata, without payload copies, in a group of at most sixteen entries.
   Flush at the bound, at scan-page end, and before deferral, state-change or
   ordinary retry/dead-letter handling. Add no batching timer, queue or worker.
4. Validate the group before mutation: nonempty, at most sixteen entries,
   strictly increasing distinct event IDs, existing transition encoding and
   existing receipt byte/entry ceilings. Reject invalid input before acquiring
   a writer turn. Singleton completion uses the same semantics. Repositories
   without atomic group support retain the existing sequential dispatcher;
   a sequence of separate commits must never be represented as one atomic group.
5. Admit each group through the existing sole FIFO writer. Revalidate every
   reciprocal event/intent/commit and exact status using the existing canonical,
   checksum, key/identity and semantic checks. An ordinary CAS mismatch leaves
   that row unchanged and returns its observed status; matching rows complete
   together. Missing authoritative intent, corruption or a typed storage failure
   aborts the complete transaction. If no rows match, abort without a receipt.
6. Commit matching status rows in one existing OutboxTransition captured V3
   receipt, canonically ordered, with exact preimages and unchanged transaction
   lineage/frontier validation. Apply all corresponding transient membership
   changes under the same mutation lease only after durable commit. Commit or
   publication uncertainty fences writes and invalidates derived indexes as
   today; return no successful group result or Delivered telemetry beforehand.
7. Grouping changes internal interleaving: a later claim may become durable
   before an earlier accepted event's source completion. Database-wide command
   ordering, authoritative commit-sequence assignment, command atomicity,
   outcomes, events and provenance remain unchanged. A group advances the
   transaction/changelog frontier once for its actual transaction, not once per
   event. Use existing durable encodings; if any existing validator or supported
   reader cannot represent this legal receipt, stop for renewed exact review.
8. Until completion commits, every accepted event remains Delivering and fences
   retention. Crash, cancellation or a fatal/fenced error may leave that bounded
   group uncertain; existing startup recovery normalizes those attempts and
   immutable archive deduplication permits redelivery. Ordinary exits flush the
   group before returning; never replay after a storage fence, fabricate a
   success or normalize a live claim using an unreviewed recovery shortcut.
   Existing interruption failpoints continue to model abrupt process loss.
9. Keep public command, SDK, MCP, authorization, operator configuration and
   archive inspection behavior unchanged. Applications gain no batch selector,
   unsafe profile, durability opt-out or transaction callback. Document the
   bounded internal completion delay and unchanged at-least-once guarantee.
10. Prove semantic failure on the previous source, deterministic CAS/ordering,
    native process crashes, compatibility, corruption and membership accounting.
    Run scoped acceptance and full CI on frozen final source, then repeat the
    unchanged required-host preparation. A lower transaction count is not a
    passing rehearsal: recovery, normal-window latency, actual reclamation,
    byte/resource bounds and every lifecycle gate remain required. Start no
    24-hour run as part of this repair.

## Options considered

1. **Keep every completion separate:** simplest state machine, but retains a
   primary transaction per archived success during backlog recovery.
2. **Group claims and completions:** removes more transactions but introduces
   claims ahead of I/O, aggregate quota admission and additional lease exposure;
   deferred until evidence requires that larger change.
3. **Group successful completions only:** chosen as the smaller native change;
   preserves existing claim and archive durability boundaries.
4. **Change archive durability or reduce offered load:** outside this proposal;
   neither substitutes for proving the selected application milestone.

## Consequences

- A full sixteen-success group uses sixteen primary claim commits and one
  completion commit instead of thirty-two primary commits. Actual gains depend
  on page occupancy and contention; no throughput claim follows from this count.
- Up to sixteen accepted events can remain uncertain together after a crash.
  Their archive records are durable and retention stays fenced until completion.
- Earlier completion can be delayed by later deliveries in the same bounded
  page. There is no added wait to fill a group, and no additional payload queue.
- Eligible-history byte accounting and complete fault coverage remain required
  milestone work; this repair does not establish readiness or close WP-825.

## Standing design tests

- **Interface safety:** all application writes remain compiled commands;
  scoping, authorization, exact idempotency, outcome recovery and acknowledged
  durability remain enforced. The new port accepts only checked success
  transitions below the application boundary and exposes no arbitrary mutation.
- **Scale:** one dispatcher and writer, existing scan byte ceilings, at most
  sixteen success transitions, bounded receipts/results and no population-sized
  retained state. Existing retention quanta, holds and pacing remain unchanged.

## Checks

- Real sixteen-event dispatch on prior source proves sixteen separate primary
  completion transactions; repaired source proves one with exact same outcomes.
- Named obligations cover per-event CAS mismatch, missing intent, duplicate/order
  rejection, exact receipt preimages, all-or-none committed matches, cancellation,
  membership indexes and no premature Delivered telemetry or retention advance.
- Process kills after archive acceptance, before group commit and immediately
  after commit; reopen/recover/redeliver with exact immutable archive bytes.
- Existing lost-unsynced-write schedules, corruption checks, source continuation,
  replication/backup/export compatibility and recovery conformance remain gates.
- Update the affected handbook page; scoped acceptance and full CI pass before
  repeating the unchanged 100-command/s, 400-read/s, 32-slot host preparation.
