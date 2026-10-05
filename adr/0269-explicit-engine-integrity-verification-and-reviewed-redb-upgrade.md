---
adr: "0269"
title: Explicit Engine Integrity Verification And Reviewed Redb Upgrade
status: accepted
tier: guarantee
date: 2026-10-04
accepted: 2026-10-05
acceptance: 'maintainer, in session, 2026-10-05: "Approved" in response to the exact ADR-0269 proposal'
requires: [ADR-0004, ADR-0156, ADR-0182, ADR-0203]
amends: [ADR-0004, ADR-0156, ADR-0182, ADR-0203]
supersedes: []
requirements: [REC-001, REC-002, REC-005, REC-006, REC-007, REC-008, END-008]
packages: [WP-825]
obligations: []
review_triggers:
  - The exact redb version, enabled features, dependency graph, unsafe surface or file-lock ownership changes.
  - Scrub gains an automatic repair, scope selector, validation bypass or successful receipt after detected corruption.
  - Population-wide engine verification enters startup, background maintenance or foreground requests.
---
# ADR-0269: Explicit Engine Integrity Verification And Reviewed Redb Upgrade

## Context

An external read-only review at `08aa6e8a`, checked against the pinned source,
found that redb 4.2.0 verifies page-tree checksums during repair and explicit
`Database::check_integrity`, not ordinary reads. RiffDB's complete offline scrub
runs structural and catalog-semantic validation but never calls that engine
checker. ADR-0156 and ADR-0182's context overstates verification on access.
Canonical envelopes and key/record comparisons remain necessary; they cannot
stand in for verification of the physical engine tree.

The engine checker may repair the database and return `Ok(false)`. Adding it
blindly could mutate authoritative state while reporting an integrity operation.
The redb 4.3.0 release also fixes recovery after such repair and changes custom
backend locking. A critical-dependency upgrade and the precise offline proof
boundary need exact human acceptance; general repair-scope approval is not it.

## Decision

1. Review and replace both exact redb 4.2.0 pins with `=4.3.0`, preserving
   disabled default features and enabling no experimental, multiprocess,
   logging or optional application-type feature. Freeze the resolved graph;
   review new Linux/Apple libc locking calls, licenses, unsafe code and custom
   backend contracts before the production dependency changes. An unexpected
   graph or target-support change stops for renewed review.
2. Every first-party wrapper over a file-backed engine backend must delegate
   the new lock, shared-lock and unlock methods to that same underlying file.
   Preserve exclusive database ownership across source, follower, archive,
   checkpoint, backup and restore paths. Never silently inherit a no-lock
   default for a production file. Retain the single authoritative writer;
   multiprocess writers are not introduced.
3. Correct the engine reliance statement: ordinary reads retain the existing
   canonical envelope, CRC, schema, semantic and key/identity validation, but
   do not claim redb verifies every traversed page checksum. Keep bounded-root
   readiness and the existing paged background semantic scrub. Neither a clean
   close nor a completed logical-only walk certifies the entire physical tree.
   This correction adds no validation bypass and weakens no REC-006 row check.
4. Extend the existing authorized, selector-free offline scrub with an explicit
   complete engine-integrity pass over the authoritative primary redb file,
   followed by the existing complete structural and catalog-semantic validator.
   Publish its existing success receipt only when both pass for the same
   exclusively held source artifact and exact authoritative frontier. Preserve
   native journal and extent validation; engine checks do not replace it.
5. The engine-integrity pass must not repair the authoritative artifact. Capture
   one private verification copy under native exclusive file ownership before
   any potentially repairing engine open. Run the engine checker on that owned
   copy. A repair callback, `Ok(false)`, corruption, unreadable input, mismatched
   source binding or uncertain custody refuses success and preserves the
   original. A repaired private copy is never published or substituted into
   the graph. No public repair or force control is added.
6. Use existing bounded native copying, file custody, allocation and diagnostic
   policies. Retain at most one private verification artifact; reject insufficient
   disk space or unavailable accounting rather than spill or skip verification.
   Cancellation and failure publish no success receipt and release nondurable
   capabilities. Describe the extra offline disk requirement in the handbook.
7. Freeze compatibility with genuine 4.2.0 fixtures and backups. Demonstrate
   current open, journal recovery, archive deduplication, backup/restore and
   exact outcomes after the upgrade. Do not claim downgrade compatibility unless
   separately proven; refuse incompatible input before authoritative replacement.
   This record changes no RiffDB envelope, key, wire field or command ordering.
8. Add deterministic media-corruption faults and lost-unsynced-write schedules
   to native/simulator evidence. Process abort and SIGKILL remain process-crash
   tests, not evidence of power-loss persistence. Include physical-tree damage
   with still-valid record envelopes, and canonical records under the wrong key.
   The existing checks must refuse corruption rather than return absence,
   partial data, a business refusal, or a successful scrub receipt.
9. Keep current commit profiles, journal protocol and external-file durability
   unchanged during this repair. Recurring two-phase commits and the checkpoint
   rewrite backend are separate implementation reviews with their own evidence;
   this record authorizes no transaction merge or removal of sync boundaries.
10. The sustainable milestone remains unqualified until dependency review,
    compatibility, file-lock exclusion, physical corruption, cancellation,
    private-copy preservation and native crash/recovery checks pass, together
    with scoped acceptance and full CI. Re-run required-host preparation on the
    final dependency/source revision before qualifying the short rehearsal.

## Options considered

1. **Trust envelopes as physical integrity:** misses engine-tree corruption;
   rejected.
2. **Call the repairing checker on the sole authoritative file:** can modify
   the artifact being diagnosed; rejected.
3. **Verify the entire engine before every startup or query:** contradicts
   bounded readiness and foreground work; rejected.
4. **Private offline engine verification plus native logical validation:**
   chosen; preserves the original and makes the proof boundary explicit.

## Consequences

- Offline success gains actual physical-tree verification, while recovery,
  first-access validation and background logical inspection remain distinct.
- The POC deliberately pays one additional database-sized temporary disk
  artifact and a complete offline walk. It adds no retained in-memory population.
- File-lock wrapper adaptation is mandatory for this upgrade, not optional
  optimization. No engine substitution or distributed storage model is chosen.
- Direct read-time engine checksum verification, online physical scrub,
  checkpoint rewrite removal, cache sizing and transaction merging are deferred.

## Standing design tests

- **Interface safety:** existing authority and selector-free scrub remain;
  applications cannot select verification scope, repair an artifact, request
  an unsafe profile or accept a partial integrity result.
- **Scale:** offline verification is linear and needs additional disk capacity,
  an explicit reversible POC constraint. Foreground and readiness paths gain
  no full-history work; temporary memory retains existing native page/frame bounds.

## Checks

- Exact dependency graph, license/unsafe review, `cargo deny check`, `cargo audit`.
- Two-process file-lock exclusion through every production wrapper; acquisition
  and release failure retain existing safe refusal and custody semantics.
- 4.2.0 open/recovery/backup fixtures, 4.3.0 round trips, archive replay/deduplication.
- Physical tree bit-flip with valid record CRC; record/key substitution;
  repaired-copy refusal with original bytes unchanged; typed I/O and quota refusal.
- Healthy scrub and exact source-frontier binding; cancellation, cleanup,
  process kills and restart after the first subsequent durable commit.
- Native loss-of-unsynced-write schedules, semantic conformance, scoped acceptance,
  handbook checks, full CI and required-host continuing-load preparation.
