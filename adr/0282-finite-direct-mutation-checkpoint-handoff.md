---
adr: "0282"
title: Finite Direct Mutation Checkpoint Handoff
status: proposed
tier: guarantee
date: 2026-10-09
accepted: null
acceptance: null
requires: [ADR-0263, ADR-0267, ADR-0271, ADR-0279]
amends: [ADR-0263, ADR-0279]
supersedes: []
requirements: [REC-001, END-007, END-008, STO-031]
packages: [WP-825]
obligations:
  - id: OBL-0282-1
    package: WP-825
    proof: direct_mutation_handoff_cannot_chase_successive_checkpoint_tails
    says: Later acknowledged journal growth cannot repeatedly overtake a direct mutation after its finite handoff reservation.
  - id: OBL-0282-2
    package: WP-825
    proof: direct_mutation_handoff_preserves_reads_and_initial_checkpoint_writer_progress
    says: Reads continue during the handoff and ordinary audited writes still progress while the earlier checkpoint is paused before reservation.
  - id: OBL-0282-3
    package: WP-825
    proof: direct_mutation_handoff_cancellation_failure_and_crash_preserve_authority
    says: Cancellation and failure release nondurable admission, while process crashes retain every acknowledged command and exact consumer and outbox state.
review_triggers:
  - A reservation changes authoritative sequence assignment, command atomicity, acknowledgement durability or current authorization validation.
  - Source reclamation receives writer priority, initiates this handoff or weakens its pacing and admission bounds.
  - A durable format, public interface, dependency, workload, deadline or qualification threshold changes.
---
# ADR-0282: Finite Direct Mutation Checkpoint Handoff

## Context

An unchanged preparation of revision `788fa224` on an 8-vCPU, 32-GiB Linux SSD
host acknowledges 65,536 seeded tickets in 30.2 seconds, but its native consumer
fails before measured load. A separate diagnostic reproduces the original
transport failure before a debugger attaches: the consumer is waiting in
checkpoint completion during `transition_event_consumer`, while checkpoint
materialization resolves retained history. The TicketDesk consumer is an
unprotected event stream; the protected-replay prefix repair is outside this
failure path. Neither diagnostic nor component acceptance qualifies preparation.

A further diagnostic enables existing bounded checkpoint counters. Fourteen
checkpoints start and finish while the consumer publishes no progress; selected
batches grow from 1,159,982 to 65,858,775 encoded bytes. Ordinary capacity trigger
counters remain zero. That repetition ends with a distinct protocol refusal,
which remains evidence rather than being relabeled as the transport failure.
The native admission loop releases its FIFO ticket while waiting, then checks
the newly grown suffix again. Repeated successful checkpointing does not bound
an older direct operation's progress. Keeping writers moving during the earlier
checkpoint is useful, but continuous replenishment cannot be its completion rule.

## Decision

1. Amend ADR-0263's native checkpoint/direct-write coordination to provide a
   finite handoff for an already waiting consumer or outbox transition. Preserve
   the journal-first application path, Immediate direct transition, complete
   ordered receipt history and database-wide ordering boundary of ADR-0267.
   A handoff is process-local admission, never durable mutation authority.
2. Preserve ordinary journal acknowledgement progress while the earlier
   checkpoint is running. After that completion, the waiting direct owner joins
   one FIFO mutation-admission queue. Writers admitted before its reservation
   finish under their existing bounds. Once granted, later authoritative writes
   cannot append another journal tail ahead of that direct owner.
3. Under the reservation, capture and publish the existing finite admitted
   prefix using the current validators and sole writer. Materialize any remaining
   tail through the existing bounded asynchronous checkpoint path. No later write
   can refill that tail while this reservation is held. Do not drain an oversized
   suffix under the short mutation-gate lease or join checkpoint I/O there.
   The existing compiled frame, suffix, memory and engine-transaction bounds hold.
4. Keep read-only snapshot capture and payload resolution outside this writer
   reservation. Release the short mutation gate while materialization runs so
   readers and checkpoint publication can proceed. After materialization, the
   reserved owner reacquires the gate, validates the actual current predecessor,
   capability and operation preconditions, and performs its one existing atomic
   direct transition. Release the reservation on completion or refusal.
5. Every authoritative mutation entry joins or observes the same reservation
   discipline; a second native owner cannot bypass it or publish through a raw
   engine writer. Keep application commit-sequence assignment in the commit
   coordinator. Preserve admission-request identity, outcome sequencing,
   transaction attribution, reciprocal evidence and exact expected-state checks.
   An earlier snapshot or reservation never supplies an authorization decision.
6. Bound waiting owners by the existing native port and coordinator admission
   capacities. Retain one active reservation and constant-sized FIFO counters
   and synchronization state, with checked overflow and no unbounded waiter
   collection. Preserve existing operation deadlines. Cancellation or expiry
   before mutation releases all nondurable admission and returns the existing
   typed refusal. Durable uncertainty retains existing fencing and exact recovery;
   never declare a committed transition absent or silently retry an operation.
7. Amend ADR-0279 decision 8 only to permit the ordinary-writer backpressure
   required by this consumer/outbox handoff. The source-reclamation owner cannot
   initiate or acquire this reservation, force a checkpoint, gain priority or
   pause foreground work. Its existing 100-ms FIFO admission deadline, bounded
   per-turn residence, pacing, license, holds and refusal behavior remain intact.
   It must refuse or yield safely when a reserved writer prevents its admission.
8. Preserve every canonical, checksum, journal, lineage, hold, current-row,
   checkpoint and exact-end validation. Preserve all durable formats and reviewed
   fixtures, writer 4, engine commit profile, application/admin frontiers,
   acknowledgement fences and physical reference protection. Introduce no new
   transaction callback, application selector, retry control or public surface.
9. Add bounded numeric observations that distinguish initial checkpoint wait,
   reservation wait, final captured-tail materialization and direct completion.
   Record actual selected sizes, elapsed residence and refusals. Missing or
   incomplete evidence stays unknown; counters cannot authorize a mutation or
   replace independent consumer, outbox and acknowledged-state reconciliation.
10. Require deterministic continuous-arrival tests, current-authority races,
    cancellation and process-crash coverage before repeating unchanged required-
    host preparation. Keep existing audited-writer progress tests and all latency,
    workload, queue, resource, observation and fault gates. A finite handoff alone
    proves neither acceptable latency nor sustainable throughput. If the same
    gates still fail, surface that native limitation rather than enlarging waits,
    pausing offered load or treating backpressure as a fault exclusion.

## Options considered

1. **Longer consumer deadline or slower seeding:** hides a native scheduling
   limitation, and cannot bound progress with continuous arrivals. Rejected.
2. **Faster decoding or smaller checkpoint thresholds alone:** may reduce a
   particular wait but does not prevent successive tail replenishment. Rejected
   as the admission solution; duplicate work still warrants separate evidence.
3. **Hold the existing mutation gate across checkpoint waiting:** obstructs
   snapshot reads and violates the established audited-writer progress test.
   Rejected.
4. **Journal every native control transition:** could remove engine barriers,
   but changes durable frame coverage, recovery and compatibility. Defer unless
   the finite handoff cannot satisfy the unchanged measured gates; that option
   requires a separately accepted format and migration decision.
5. **Finite FIFO reservation after the earlier checkpoint:** selected. It bounds
   overtaking without changing authoritative formats or skipping validation.

## Consequences

- Later ordinary writers can experience explicit native backpressure while one
  final captured tail is materialized. This is the exact change requiring human
  acceptance; it must pass the existing service-level gates under real load.
- The implementation must coordinate all mutation owners without placing reads,
  checkpoint completion or source-maintenance waits behind a held gate.
- Failed attempts remain evidence. WP-825 stays open until full final-source CI
  and a complete short rehearsal cover all seven faults, independent restore,
  three physical retirements and six final quiet hours. No 24-hour run starts
  through this decision's acceptance alone.

## Standing design tests

- **Interface safety:** applications retain compiled writes, exact idempotency,
  scoped authority and durable acknowledgements. No public input can acquire,
  bypass or configure the reservation, skip validation or relax a guarantee.
- **Scale:** new bookkeeping and waiting owners remain bounded. Later arrivals
  cannot enlarge the reserved prefix. Reads continue, source turns retain their
  bounds, and actual checkpoint/backpressure latency remains a measured gate.

## Checks

- Explicitly pause the earlier checkpoint, acknowledge later writes, then
  schedule continuous arrivals through the handoff. Prove a fixed captured tail,
  FIFO ownership, no successive catch-up starvation and exact durable outcomes.
- Read old and current snapshots during final materialization. Preserve the
  existing writer-progress test during the earlier checkpoint. Exercise competing
  consumers, outbox transitions, ordinary commands and source turns.
- Change capability, consumer lease, row, hold and checkpoint authority before
  final admission; existing validators must refuse or re-evaluate safely.
- Cancel before reservation, during materialization and before mutation; inject
  completion failure and process kills around existing durable boundaries.
- Run scoped acceptance, full final-source CI, unchanged preparation and the
  complete short rehearsal. Preserve every failure and every original threshold.
