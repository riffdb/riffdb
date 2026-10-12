---
adr: "0292"
title: Recovery Clock Begins At Primary Loss
status: accepted
tier: guarantee
date: 2026-10-12
accepted: '2026-10-12'
acceptance: 'maintainer, in session, 2026-10-12: i approve, in response to the joint exact-text review of ADR-0290, ADR-0291 and ADR-0292 in WP-825-RESTORE-RECOVERY-REVIEW.md; proposed text SHA-256 3591b1673756d165e09acde9164592815d112b5b516b31928c792ac5a4d7c8fd'
requires: [ADR-0265]
amends: [ADR-0265]
supersedes: []
requirements: [END-001, END-008]
packages: [WP-825, WP-826]
obligations: []
review_triggers:
  - Recovery work is excluded, the clock resets or backup costs disappear from evidence.
  - Backup selection changes after failure or acknowledged data loss is hidden.
  - Workload, fault placement, recovery threshold or reconciliation requirements change.
---
# ADR-0292: Recovery Clock Begins At Primary Loss

## Context

ADR-0265 requires recovery to a successful write within 60 seconds and includes
backup/restore into an empty replacement. The implementation starts that clock
before creating the backup, so it measures planned backup preparation as well
as recovery from loss of the primary. The latest measured preparation alone
took 17.5 seconds. Previous failed receipts remain valid evidence of that
stricter implemented method, not successes under a newly chosen clock.

Recovery from a lost primary assumes a backup already durably exists. That is
the selected recovery-time objective. Making its clock explicit changes the
frozen harness/evidence protocol and requires human acceptance; no existing
receipt can be reclassified to claim qualification.

## Decision

1. For the backup/empty-replacement fault, select and durably complete the named
   backup through the existing public operation before deliberately losing the
   primary. Bind its manifest, artifact hashes, included frontier and original
   acknowledgement witnesses before fault injection. Keep the predetermined
   fault placement and report backup preparation as a separate visible phase.
2. Start the one monotonic 60-second recovery clock immediately before the
   primary-loss/empty-replacement transition. The selected durable backup exists
   at this instant. Do not start the clock at restore submission, readiness or
   the beginning of validation, and never reset it after a retry or unknown poll.
3. Count every subsequent recovery action: target setup and access, transfer or
   copying from the selected backup location, format/hash/journal verification,
   authorization, required preparation and validation, stamping, publication,
   reopen, readiness, and the first acknowledged successful command. Preserve
   the existing stronger focused check that also completes exact replay and
   independent full state reconciliation inside the same 60 seconds.
4. Report backup creation, its source-service interruption, artifact size and
   durability completion separately. Preserve complete controller observation,
   offered-work accounting, bounded in-flight work, visible interrupted windows
   and all other latency/rate gates. No preparation outage becomes an ordinary
   successful window or disappears because it precedes the recovery clock.
5. Do not silently discard acknowledgements between the backup frontier and the
   injected loss. The fault protocol must bind all affected acknowledgement
   witnesses and preserve existing reconciliation: either no later acknowledged
   writes exist, or supported recovery reproduces them exactly. This record
   introduces no data-loss allowance, new replay mechanism or archive assumption.
6. Keep the named backup immutable, the target genuinely empty, and all existing
   public authorization, operation identities, uncertainty and crash checks.
   A missing/incomplete backup before injection invalidates the attempt; it is
   never created after loss and excluded from timing. Change no other fault's
   clock or the 60-second threshold.
7. Freeze and hash the revised protocol before new qualification. Evidence must
   distinguish the previous combined clock from this primary-loss clock, retain
   all failures, and reject receipts with mixed or unspecified methods. Re-run
   the focused gate and actual rehearsal/campaign; do not subtract backup time
   from old failed attempts and call them passing results.

## Options considered

1. **Include backup creation:** measures planned backup plus restore interruption,
   but does not model recovery after losing a primary with an existing backup.
2. **Start when the restore RPC is submitted:** hides target/transfer work;
   rejected.
3. **Start at primary loss with a selected durable backup:** selected.

## Consequences

- The 60-second target measures recovery availability while backup costs remain
  independently visible. This is a method amendment, not an elapsed-time fix.
- Backup size can grow with retained outcomes and provenance despite fixed live
  ticket count; qualification must use the actual scheduled-fault population.
- Backup-frequency objectives and a nonzero recovery-point allowance are not
  introduced. Existing acknowledgement reconciliation remains mandatory.

## Standing design tests

- **Interface safety:** no application interface or database guarantee changes;
  operators and harnesses cannot turn a partial backup into a recovery proof.
- **Scale:** preserve all existing workload and request/resource bounds. Report
  actual bytes and include their recovery transfer cost rather than hiding it.

## Checks

- Deterministic clock tests include delayed target setup, copy, authorization,
  unknown polling, readiness and first-write failure under the original deadline.
- Evidence rejects missing pre-fault durability, changed backup selection,
  unaccounted acknowledgements, reset clocks and mixed-method receipts.
- Report backup preparation and recovery separately; preserve original public
  command replay, provenance, archive and independent state reconciliation.
- Focused saved-fixture gate first, then frozen-protocol acceptance, full CI,
  actual short rehearsal and uninterrupted 24-hour campaign on the required host.
