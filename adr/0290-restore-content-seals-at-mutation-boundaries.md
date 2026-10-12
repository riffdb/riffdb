---
adr: "0290"
title: Restore Content Seals At Mutation Boundaries
status: accepted
tier: guarantee
date: 2026-10-12
accepted: '2026-10-12'
acceptance: 'maintainer, in session, 2026-10-12: i approve, in response to the joint exact-text review of ADR-0290, ADR-0291 and ADR-0292 in WP-825-RESTORE-RECOVERY-REVIEW.md; proposed text SHA-256 73bc2a07dac8821dcc9c0f1a644069f84cdfdb2aa707d0e35c241a1fa48a7c2d'
requires: [ADR-0289]
amends: [ADR-0289]
supersedes: []
requirements: [REC-001, REC-006, END-001]
packages: [WP-825]
obligations: []
review_triggers:
  - A content check crosses a mutation, uncertain custody or publication boundary.
  - Descriptor metadata is treated as a content checksum or corruption is resealed.
  - Native exclusion, repair refusal, checksum algorithms or durable formats change.
---
# ADR-0290: Restore Content Seals At Mutation Boundaries

## Context

The ordinary populated restore takes 162.7 seconds on the required host.
An independent profile of the same revision attributes about 40 seconds to
repeated complete hashing of the same private primary and journal files.
OwnedMaterialization::verify checks content at immutable authorization handoffs
as well as at actual mutation and publication boundaries. These measurements
identify a cost; they do not qualify the recovery gate.

ADR-0289 requires continuous native custody and unchanged validated bytes.
Replacing intermediate hashes with descriptor continuity checks changes that
proof protocol. It must preserve full content checks around known mutations
so an unexpected change cannot become valid merely by capturing a new hash.

## Decision

1. Preserve native exclusive custody, retained file descriptors, pinned parent
   directories, exact artifact inventory, cancellation and repair refusal.
   This record applies only to the operation-local private restore lifecycle;
   it grants no ordinary source, follower, scrub or application bypass.
2. At immutable handoffs, check original descriptor/path identity, regular-file
   status, length, modification and change times, native mutation generation,
   and existing custody/repair latches. Include the complete bounded artifact
   inventory. Unsupported metadata retains full content verification; missing
   identity or uncertain custody fails closed.
   Check metadata before and after content hashing and before each handoff.
3. Metadata checks are continuity evidence, not proof of byte equality. Preserve
   complete cryptographic checks of the immutable backup and copied artifacts.
   Establish a full seal after the required private validation, verify that
   seal before a controlled mutation, and establish the final full seal after
   the controlled mutation and engine close. Publication must verify its input
   against that final seal and verify any copied destination against it.
   Retain the existing algorithm and canonical artifact/manifest checks.
4. Read-only authorization and ownership transfers within one unchanged custody
   generation may reuse the established seal after Decision 2 passes. They do
   not each re-hash the complete files. Repeated access to an already verified
   seal within that immutable generation has the same rule.
5. Only existing native compatibility preparation, known engine-close effects,
   and receipt-authorized incarnation/RestoreAnchor transitions may advance the
   mutation generation. Before any such transition, verify the previous full
   seal and release all reads and engine leases required by the existing
   lifecycle. An unclassified write, replacement, repair or I/O uncertainty
   invalidates the owner; a subsequent checksum cannot restore its authority.
6. Preserve the existing Immediate two-phase stamp, transaction-current history
   checks, retention-watermark rebind, replication reset, publication ordering,
   directory sync and receipt uncertainty. An expected primary change does not
   authorize changes to the journal, marker or other immutable artifacts.
   Verify those against their previous seals before accepting the successor.
7. Do not persist or reconstruct the continuity proof. Restart or uncertain
   ownership discards it and rebuilds from the immutable backup. A failed
   checksum preserves the backup and publishes neither success nor readiness.
8. Change no public selector, authorization rule, command guarantee, durable
   encoding, dependency, worker/cache/resource ceiling or workload threshold.
   This record alone does not defer any required validator or change the clock.

## Options considered

1. **Hash at every immutable handoff:** safe but repeatedly linear; rejected.
2. **Trust metadata throughout publication:** cannot prove content; rejected.
3. **Continuity checks between full mutation-boundary seals:** selected.

## Consequences

- Immutable handoffs become independent of artifact size; full content checks
  remain proportional to bytes at the specified boundaries.
- The design does not promise that timestamps detect every possible byte change.
  Mandatory content checks remain the acceptance boundary for such changes.
- The exact number of passes follows actual mutations and copies, not a target
  of two hashes that would omit a required pre-mutation or publication check.

## Standing design tests

- **Interface safety:** applications cannot construct custody, supply a seal,
  request cheap verification, authorize a mutation or skip a checksum.
- **Scale:** retain the existing artifact bound, streaming buffers and one
  private verification artifact; add no decoded population cache or worker pool.

## Checks

- Native pass-count tests distinguish immutable handoffs from mutation boundaries.
- Changed bytes of equal length, restored modification time, descriptor/path
  replacement, changed journal/marker and I/O uncertainty
  refuse success; an unexpected change is never laundered by resealing.
- Unsupported metadata retains the full-content-check path and target support.
- Deterministic read-release, engine-close, cancellation and publication schedules,
  plus existing repair, compatibility and process-crash recovery oracles.
- Test the focused saved-fixture restore after each fix, then scoped acceptance,
  final full CI and the accepted public rehearsal before qualification.
