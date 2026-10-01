---
adr: "0265"
title: Sustainable TicketDesk Storage Lifecycle
status: accepted
tier: guarantee
date: 2026-10-01
accepted: '2026-10-01'
acceptance: 'maintainer, in session, 2026-10-01: Accept all three as written (ADR-0265, ADR-0266, ADR-0267)'
requires: [ADR-0004, ADR-0006, ADR-0012, ADR-0182, ADR-0186, ADR-0240, ADR-0263]
amends: [ADR-0182, ADR-0186]
supersedes: []
requirements: [OUT-001, OUT-002, REC-001, REC-002, REC-005, STO-020, STO-030, STO-031, STO-032, END-001, END-002, END-003, END-004, END-007, END-008, END-009, END-010]
packages: [WP-821, WP-822, WP-823, WP-825, WP-826]
obligations: []
review_triggers:
  - Delivery acknowledges before destination durability or bypasses the coordinator.
  - Reclamation manufactures acknowledgements, drops holds, or trusts invalidated evidence.
  - Synthetic observations or stopped-database edits qualify production behavior.
  - Rehearsal changes alpha release requirements or existing performance gates.
  - Dependencies, durable identities, or public interfaces exceed this decision.
---
# ADR-0265: Sustainable TicketDesk Storage Lifecycle

## Context

The selected milestone proves one sustainable TicketDesk application. Production
has no outbox destination; WP-815 qualified logical pruning but reclaimed no
extents because source receipts held them. Its campaign pre-marked events
delivered. Delivery, reclamation, and recovery remain unqualified together.

The maintainer authorized this milestone and a local destination in session on
2026-10-01. This authorizes drafting despite D-001's functionality restriction,
not acceptance of this exact text. The record remains proposed; dependent
runtime and public-interface implementation waits for human acceptance.

## Decision

1. Register WP-821 through WP-826 as `ticketdesk-lifecycle`, a focused 24-hour
   rehearsal. Preserve the writer, command authority, durability profiles,
   idempotency, provenance, export, alpha's 72-hour END-009 gate, and existing
   comparative/interference gates. Incomplete performance work stays incomplete.
2. Compose one optional `durable_local` outbox destination per primary database.
   Operator configuration supplies a protected directory and positive encoded-
   record byte quota. Configuration is restart-applied; followers refuse it.
   Applications cannot select a destination. Absence retains current behavior.
3. Reuse redb as a separate archive owned by riffdb-outbox behind its connector
   interface. Add direct dependencies using the repository's existing exact
   redb and cap-std versions/features, not a new engine or custom append log.
   The archive is external destination state, never primary transaction state.
4. Version LocalOutboxArchiveRecordV1 using existing Protobuf/envelope conventions.
   Key by DatabaseId, history incarnation, and EventId; retain the exact event
   identity, hash, and canonical payload. Equal retries succeed after validation;
   unequal bytes under one identity are corruption, never replacement.
5. Insert record and byte accounting in one two-phase redb Immediate transaction.
   Sync first-creation directory publication before acceptance. Reject unsafe
   path/ownership, symlinks, incompatible formats, and conflicting database
   bindings. Recover the archive before dispatch. Quota accounts encoded rows;
   redb allocation overhead is separately disclosed. No automatic deletion.
6. Use the existing recovered dispatcher outside command evaluation. Durable
   claims and completion retain coordinator-owned transitions. Crash after sink
   commit but before source completion may redeliver: the guarantee is at least
   once. Observed unavailability/quota exhaustion pauses before new claims;
   failures after a claim retain the existing bounded retry/dead-letter rules.
   Dead letters never become delivered and continue to fence retention.
7. Add bounded operator-only local CLI archive inspection: format, database
   binding, counts, encoded bytes, and integrity. Default output contains no
   event payload. The campaign reconciles through the same validated reader.
   No application RPC/MCP method, arbitrary destination, or payload logging.
8. Retain one source-reclamation owner per primary graph across observations.
   Its closed step requires a later independently durable checkpoint subsuming
   the earlier observed prefix and rechecks every hold. Remove at most 256
   receipts; reclamation's own receipt cannot perpetually stimulate itself.
9. Schedule only at existing drained checkpoint/barrier opportunities, with
   bounded admission and existing retention pacing. Do not add command-path
   flushes or force a drain on each poll. Logical pruning and receipt release
   separately precede off-gate extent deletion/reuse. Preserve resume anchors,
   consumer, follower, archive, bootstrap, projection, reader, and scrub holds.
10. Invalidate old validated-prefix evidence exactly as today. Reestablish it
    from complete validated pinned state in bounded background steps, publishing
    only against the exact current binding; changed evidence discards the
    candidate. This is the narrow online-refresh amendment to ADR-0182. Never
    reuse deleted evidence or skip corruption checks. Measure first successful
    write as well as readiness; a blocked first write is recovery time.
11. Bounded redacted diagnostics separate delivery backlog, logical pruning,
    receipt release, deleted/recycled extents, reference holds, and recovery.
    Inventory actual artifacts and production counters, not scheduled intent.
12. Seed 65,536 tickets across 64 organizations. Use an inventoried 8-vCPU,
    32-GiB Linux host with durable SSD storage; offer 100 commands/s and 400
    named reads/s with at most 32 in flight. Rust drives sustained load;
    retained TypeScript bindings exercise smoke tests; retain four-language
    conformance. Keep an event consumer, live query, and queried projection active.
13. Normal five-minute windows sustain both offered rates, with p95 below 100 ms,
    p99 below 250 ms, and no unexplained failures. Predetermined faults stay
    visible; interrupted operations resolve original identities. Recovery to
    a successful write is at most 60 seconds, including after pruning.
14. A five-minute sink outage and a separate five-minute consumer outage each
    drain their backlog within ten minutes of recovery at the same offered
    load. Exercise capability rotation, additive deployment, graceful restart,
    process kill, and backup/restore into an empty replacement.
15. Require 24 continuous hours of controller observation, at least three
    distinct physically reclaimed extent generations, and no increasing
    eligible-history backlog during the final six fault-free hours. Separately
    account live data, retained outcomes/provenance, protected history, archive,
    backups, and derived state. A per-command allowance cannot excuse growth of
    reclaimable history or process memory.
16. Freeze workload, fault schedule, resource model, hashes, hardware inventory,
    and thresholds before qualification. Missing, synthetic, changed, interfered,
    or partial observations are ineligible. Preserve failed runs. The receipt
    says rehearsal, never alpha qualification. Close WP-826 only after a real
    uninterrupted campaign and all implementation package checks pass.

## Options considered

- FileTestConnector flush: rejected; it has no durable destination contract.
- New append-file format: rejected; it adds a recovery engine to implement.
- HTTPS webhooks: deferred in favor of the selected local destination.
- Logical-only qualification: rejected; it cannot establish physical reclamation.

## Consequences

- History becomes eligible through real delivery and consumer progress.
- Archive custody and storage cost remain explicit; it is not free space.
- Unmet sustainability or recovery targets block closure instead of lowering gates.
- Epoch reset, driver redesign, distribution, and full release remain separate.

## Standing design tests

- **Interface safety:** only operator configuration controls the archive;
  applications cannot force delivery, release holds, prune, or bypass validation.
- **Scale:** records, queues, inspections, reclamation passes, and recovery
  candidates are bounded. Slow destination I/O never runs on the command path.

## Checks

- Process kills at archive commit/source completion, receipt pruning, extent
  deletion/reuse, and prefix refresh preserve atomic outcomes and exact replay.
- Quota, corruption, unsafe paths, duplicate equality, and unavailable destination.
- Every hold independently blocks deletion; legitimate release permits progress.
- Evidence tests reject logical-only reclamation, gaps, drift, hidden faults,
  incomplete duration, and alpha/rehearsal confusion.
- Scoped acceptance, generated-client conformance, and full CI precede rehearsal.
