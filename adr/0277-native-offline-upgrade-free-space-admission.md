---
adr: "0277"
title: Native Offline Upgrade Free Space Admission
status: proposed
tier: guarantee
date: 2026-10-06
accepted: null
acceptance: null
requires: [ADR-0112, ADR-0276]
amends: []
supersedes: []
requirements: [AFC-001, AFC-002, REC-001, STO-032, END-008]
packages: [WP-825]
obligations:
  - id: OBL-0277-1
    package: WP-825
    proof: offline_format_upgrade_refuses_insufficient_space_before_any_mutation
    says: Initial and resumed upgrades enforce the declared free-space floor with checked bounded source accounting and typed refusal before engine, receipt, journal or marker mutation.
review_triggers:
  - The exact rustix pin, default-feature posture, fs/std feature set or dependency graph changes.
  - Space refusal becomes advisory, gains an override, follows ambiguous source inventory, or stops covering retry.
  - An availability observation is represented as a reservation or a guarantee against concurrent external consumption.
---
# ADR-0277: Native Offline Upgrade Free Space Admission

## Context

WP-825's registry-completion compatibility repair exposed a native upgrade
limitation: the release manifest declares two source-size multiples of free
space, but `RedbDurableFormatUpgrade::run` never checks available space. It can
publish its Accepted receipt and begin storage mutation without satisfying
its advertised admission condition. A harness-side check would leave the
same public offline upgrade defect for operators.

Checking filesystem availability requires a safe operating-system interface.
The workspace already pins rustix 1.1.4 for bounded filesystem operations,
but its accepted native purposes do not authorize this storage upgrade use.
This record admits that exact dependency and a closed admission check. No
implementation or dependency change precedes exact human acceptance.

## Decision

1. Enforce the manifest-declared free-space requirement inside the existing
   native offline upgrade owner, on initial admission and every resumed attempt
   before any engine open, journal recovery, migration, receipt publication or
   marker mutation. A true AlreadyCurrent result that changes nothing needs no
   admission. Keep the backup, source identity, receipt phases and offline
   compatibility graph unchanged.
2. Account the complete closed source lifecycle unit using checked integer
   arithmetic: primary database, present active journal, retained sealed extents,
   format marker and retained upgrade receipt. Count logical file lengths,
   conservatively including preallocated or sparse space; do not substitute
   allocated blocks, a population count or the backup's size. Exclude unrelated
   sibling files and the immutable backup. Missing mandatory files, unreadable
   metadata, ambiguous names, nonregular source artifacts, duplicate generations
   or arithmetic overflow refuse before mutation.
3. Bound discovery by at most 16,384 directory entries and by the existing
   backup sealed-artifact ceiling for matching extents. Stream accounting,
   retaining no contents or population-sized list. A ceiling overflow refuses;
   it cannot produce a partial size or admission. An absent active journal or
   upgrade receipt is legitimate only under the existing lifecycle rules.
4. Query the target filesystem's bytes available to the invoking principal,
   using checked `f_bavail * f_frsize` from safe rustix `fstatvfs` on the opened
   source-directory descriptor. Required bytes equal the checked source total
   times the exact manifest factor (currently two). Admit equality and refuse
   one byte below. An unsupported platform, unreadable filesystem statistic,
   zero fragment size or overflow cannot grant admission.
5. Add only the exact direct dependency
   `rustix = { version = "=1.1.4", default-features = false, features =
   ["fs", "std"] }` to riffdb-storage-redb. Use only its safe filesystem
   availability call for this purpose. Retain first-party `forbid(unsafe_code)`;
   add no unsafe Rust, native shim, process invocation or broader feature set.
   Preserve the locked package version, checksum and transitive graph. Run
   dependency policy and advisory checks before integration.
6. Return a typed, redacted insufficient-space refusal through the existing
   offline upgrade result. Unknown/unavailable accounting or filesystem evidence
   uses the existing typed unavailable/refusal path. Keep CLI error envelopes
   closed and document the recovery: provision space and retry the same checked
   upgrade with its exact immutable backup. Add no force, skip, quota override,
   caller-supplied size, free-space value or admission selector.
7. This is a conservative admission observation, not a filesystem reservation.
   Concurrent external consumption, quotas or subsequent I/O failure can still
   exhaust space after admission. Preserve existing durability errors, uncertain
   outcome fencing and backup-bound recovery; never acknowledge success or
   weaken fail-closed behavior because the observation passed.
8. Test admission through a private deterministic filesystem seam, unavailable
   to production callers. Prove the exact floor, one byte below, missing or
   ambiguous artifacts, overflow, entry/extent limits and every resumed phase.
   A refused attempt leaves database, journal, extents, marker and receipt bytes
   unchanged. Real-filesystem coverage proves native measurement and successful
   existing upgrades without reserving or filling a shared host's disk.
9. Keep application commands, ordering, transaction encodings, public backup
   formats and Completed eligibility unchanged. Update the handbook and scoped
   acceptance, full CI and required-host preparation. This repair authorizes no
   24-hour qualification run.

## Options considered

1. **Check only in the harness:** rejected; operators retain the same defect.
2. **Shell out or add first-party unsafe syscalls:** rejected; unnecessary new
   machinery when an exact pinned safe filesystem dependency already exists.
3. **Native checked admission with pinned rustix:** selected; the database
   enforces its declared precondition without an application bypass.

## Consequences

- Initial and interrupted upgrades can refuse until the operator supplies
  sufficient space; no declared floor is silently treated as advice.
- The conservative file-length bound may refuse a sparse source despite lower
  actual allocation. Disk quotas and post-admission consumption remain I/O
  failure risks; the check does not promise reserved capacity.
- No archive, history, command or steady-state pacing behavior changes.

## Standing design tests

- **Interface safety:** applications cannot supply accounting, weaken the floor,
  assert admission or choose an unsafe upgrade. The supported operation retains
  exact backup binding, offline ownership, typed refusal and safe retry.
- **Scale:** one bounded metadata-only directory walk, constant retained
  accounting state and one filesystem statistic; no engine or population walk
  can run before admission.

## Checks

- Deterministic no-mutation admission and retry tests at every floor/overflow/
  inventory boundary; real target-filesystem measurement and existing upgrade
  process crashes.
- Frozen registry, journal, marker, receipt and backup fixtures unchanged;
  cargo deny, cargo audit, scoped acceptance and full CI on final source.
