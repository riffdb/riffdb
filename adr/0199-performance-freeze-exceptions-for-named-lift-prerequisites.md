---
adr: 0199
title: Performance Freeze Exceptions for Named Lift Prerequisites
status: proposed
tier: guarantee
date: 2026-09-05
accepted: null
requires: [ADR-0178, ADR-0182, ADR-0183]
amends: [ADR-0183, PERF-020]
supersedes: []
requirements: [PERF-020]
packages: [WP-780, WP-762]
obligations:
  - id: OBL-0199-1
    package: WP-762
    proof: check-performance-freeze
    says: The freeze checker seals every pre-freeze PERF package, permits progress
      only for WP-750, WP-760, WP-762, and WP-763 under their exact roles, and
      rejects every new package, widened exception, or unauthorized state change.
review_triggers:
  - A fifth package would be permitted to advance while the freeze is in force,
    or an inert pre-freeze PERF package would change or close.
  - WP-750 or WP-760 would add a PERF requirement, activate a performance
    candidate, alter a performance threshold, or lift the freeze.
  - The freeze would start without the WP-762 bank, or lift before WP-750 and
    WP-760 close or without a separately accepted ADR.
---
# ADR-0199: Performance Freeze Exceptions for Named Lift Prerequisites

## Context

ADR-0183 intends to freeze new performance work while replication acceptance
and bounded dirty recovery close. Its exact checker obligation instead rejects
every open package that lists a `PERF-*` requirement except WP-762 and WP-763.
That rejects WP-750 and WP-760 themselves even though ADR-0183 names their
closure as the condition for a later lifting record. Enforcing the text creates
a cycle; exempting packages ad hoc creates an unbounded escape.

The manifest also retains open performance-bearing packages registered before
the freeze starts. Their unchanged presence must not make every repository
check fail, but presence cannot authorize work. The check therefore needs a
sealed pre-freeze inventory and an exact, purpose-limited progress set.

## Decision

1. **The start and lift rules do not change.** The freeze starts only in the
   same change that banks WP-762's qualified baseline. It remains in force
   until both WP-750 and WP-760 close and a later, separately accepted ADR
   carries that baseline as its starting receipt and names admitted packages.
   A closure, manifest edit, or this record never lifts it.
2. **Pre-freeze declarations are sealed, not grandfathered for work.** The
   WP-762 freeze record inventories every package that lists a `PERF-*`
   requirement at freeze start. For each then-open package it stores an exact
   digest of the normalized package definition excluding `closure`. The digest
   input is the entire parsed package mapping after removing only its top-level
   `closure` member. The checker rejects a non-JSON scalar or non-finite number,
   applies RFC 8785 JSON Canonicalization Scheme to the remaining mapping, and
   stores SHA-256 over the canonical UTF-8 bytes as 64 lowercase hexadecimal
   characters. An unchanged entry
   may remain open without failing checks. Unless named in Decision 3, it may
   not change that definition or gain a closure while the freeze is in force.
   A package absent from the inventory may not add or activate a `PERF-*`
   requirement.
3. **Exactly four packages may advance.** WP-762 may bank the baseline and
   start the freeze, and WP-763 may build and either activate or remove the one
   ADR-0183 candidate. WP-750 may complete only its already-registered
   replication acceptance campaign, including its existing no-hot-path-
   regression evidence. WP-760 may complete only its already-registered
   bounded dirty-recovery work, including its existing recovery-performance
   evidence. WP-750 and WP-760 may retain their exact pre-freeze definitions
   and add a closure; they may not add a `PERF-*` requirement, authorize or
   activate a performance candidate, or admit another package.
4. **WP-750 supplies a lift prerequisite, not a lift.** Its existing phrase
   "performance freeze lifting" means that its accepted campaign supplies one
   prerequisite and evidence for the later lifting ADR. WP-780 replaces that
   phrase in the package objective with this exact meaning after this record is
   accepted. WP-750 closure alone changes no freeze state.
5. **The check has a closed decision table.** `check-performance-freeze`
   accepts packages closed before the recorded start, byte-exact inert open
   entries from the sealed inventory, the exact WP-750/WP-760 closure-only
   transition, and the existing WP-762/WP-763 transitions. It rejects a new
   performance-bearing package, an altered or closed inert entry, a changed
   exception definition or `PERF-*` set, a fifth exception, and any lift that
   does not satisfy Decision 1. The exception identifiers and their allowed
   transitions are first-party constants, not fields a package may append to.
6. **No performance or runtime decision changes.** ADR-0183's c32, write-only,
   unary, host-validity, stability, durability, group-formation, and activation
   arithmetic remains exact. This record changes no code path, evidence value,
   retry rule, acknowledgement, fence, durable byte, public surface, or
   package other than the governance reconciliation above.

On acceptance, ADR-0183 OBL-0183-1 is read as:

> While the freeze is in force, `check-performance-freeze` seals all
> pre-freeze PERF-package definitions and rejects every new package or
> unauthorized state change. Only WP-750 and WP-760 may retain their exact
> definitions and add a closure for their named non-candidate prerequisites;
> WP-762 and WP-763 retain their existing authority. No other open package may
> change or close, and no fifth exception exists.

`PERF-020` receives the same closed decision table. Its existing baseline,
measurement-continuity, human-review, and accepted-lifting-ADR clauses remain
unchanged.

## Options considered

1. **Require WP-750 and WP-760 to close before the freeze starts:** rejected.
   It reverses ADR-0183's prioritization and makes the freeze ineffective while
   the named prerequisites are being built.
2. **Allow every pre-freeze open package to close:** rejected. Registration
   history would become a broad escape for performance candidates the freeze
   explicitly deferred.
3. **Ignore all open entries except by package number or declaration order:**
   rejected. Reordering or allocating an older-looking identifier would bypass
   the rule, and unchanged stale declarations would remain indistinguishable
   from activated work.
4. **Seal existing definitions and grant four exact transitions:** chosen. It
   lets the named prerequisites close, keeps old declarations inert, and makes
   every later addition or widening mechanically visible.

## Consequences

- WP-750 and WP-760 can satisfy the condition ADR-0183 already assigns them.
- Existing open PERF declarations do not break the repository merely by
  remaining present, but they cannot advance during the freeze.
- The freeze record gains a bounded inventory of package-definition digests;
  it contains governance metadata only and no benchmark or application data.
- A change to either prerequisite's package definition requires human review
  rather than silently expanding its exception.

## Standing design tests

- **Interface safety:** this changes repository governance only. Applications
  and operators gain no performance selector, retry, durability option, or
  weaker guarantee.
- **Scale:** the checker hashes the finite work-package manifest once. It reads
  no database state and introduces no runtime or co-location assumption.

## Checks

- `check-performance-freeze --self-test` covers an inert pre-freeze package, a
  new PERF package, an inert-package edit and closure, exact WP-750 and WP-760
  closure, a widened prerequisite, a fifth exception, and premature lift.
- `check-adr-obligations`, `check-requirement-coverage`, and `adr-index --check`
  bind the amendment, package, requirement, and proof identities.
