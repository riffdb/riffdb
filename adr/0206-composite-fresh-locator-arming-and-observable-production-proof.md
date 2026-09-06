---
adr: "0206"
title: Composite Fresh-Locator Arming and Observable Production Proof
status: proposed
tier: guarantee
date: 2026-09-06
accepted: null
requires: [ADR-0070, ADR-0100, ADR-0104, ADR-0165, ADR-0197, ADR-0205]
amends: [ADR-0205]
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-019]
packages: [WP-705]
obligations:
  - id: OBL-0206-1
    package: WP-705
    proof: production_grouped_command_arms_fresh_locator_coverage_without_manual_arm
    says: A real production Group-durability command observes the exact fresh-locator proof as armed after its first deferred command completes, before any later lookup, and later new-key inspections and transaction rechecks retain zero fallback history scans without manual arming.
  - id: OBL-0206-2
    package: WP-705
    proof: production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths
    says: Architecture checks freeze the sole shared arming site and the private one-row exact logical-table emptiness implementation for both direct redb and deferred composite-stage access.
review_triggers:
  - Logical emptiness would use a different view than the owning mutation-gated command access, inspect or return more than its fixed bound, infer emptiness from allocators or frontiers alone, or omit any of COMMITS, IDEMPOTENCY, IDEMPOTENCY_PENDING, and IDEMPOTENCY_LOCATORS.
  - The helper would mutate, persist, publish, checkpoint, drain, await, acquire scheduling authority, alter ordering, or turn a read, bound, borrow, poison, or contradiction failure into absence or an armed proof.
  - The observation would compile outside test-fixtures, expose more than the exact armed boolean, arm/reset/disable coverage, expose a stamp, witness, role, epoch, frontier, key, table, or population, or add a hook, barrier, failpoint, callback, configuration, or production/public application surface.
  - The grouped proof would rely only on the fallback counter or derived-index coverage, manually arm, bypass Group durability or the production coordinator, observe after a later lookup, or omit the explicit unarmed-to-armed transition.
  - WP-705 would touch another path, or ADR-0205 audit-cause, threshold, transaction, acknowledgement, publication, failure, restart, bound, or evidence rules would otherwise change.
---
# ADR-0206: Composite Fresh-Locator Arming and Observable Production Proof

## Context

ADR-0205 moves the sole exact-empty arming attempt to
`BatchCore::open_with_access`, shared by direct and deferred command batches.
The existing arming primitive opens redb tables through
`RedbWriteAccess::transaction()`. A direct access owns that transaction, but a
Group-durability access owns a `RedbDurabilityEpoch` and composite mutation
stage with `transaction: None`; the accepted call therefore returns an
invariant failure before the first deferred command can commit. Fixing that
mechanism requires `crates/riffdb-storage-redb/src/store.rs`, outside
ADR-0205's exact implementation paths.

The accepted grouped semantic check is also masked on an exact fresh sequential
handle: after the first command, the transient derived-index frontier can
independently prove a later miss, leaving the fallback counter at zero even
while fresh-locator coverage remains `Uninitialized`. The proof must observe
arming itself without granting production code or callers a coverage control.

## Decision

1. ADR-0205 Decision 9 is amended only to add
   `crates/riffdb-storage-redb/src/store.rs` to WP-705's implementation
   authority. Its existing six paths, purposes, and restrictions remain exact.
   The added path is solely for Decisions 2 through 5 below; another path or
   purpose requires a separately accepted amendment.

2. `RedbWriteAccess::arm_fresh_locator_coverage` may use one private read-only
   exact logical-table emptiness helper. For direct access, the helper retains
   redb `Table::is_empty` on the access's writer transaction. For deferred
   access, it borrows that access's existing writer-private composite mutation
   stage and performs an ascending logical merge from the empty start key to an
   unbounded end, returning at most one row and inspecting at most the existing
   fixed composite-overlay bound plus that row. An empty page proves emptiness;
   one row proves non-emptiness. It does this separately for `COMMITS`,
   `IDEMPOTENCY`, `IDEMPOTENCY_PENDING`, and `IDEMPOTENCY_LOCATORS`.

3. The helper reads the same mutation-gated logical view that the new command
   batch will stage against, before allocator read, staging, or mutation. It
   does not create or apply a journal mutation, change an overlay, checkpoint,
   publish, persist, drain, wait, or alter any root, frontier, allocator,
   transient index, retention state, or fence. An access with neither a direct
   transaction nor a composite stage, a borrow conflict, poison, I/O failure,
   exceeded bound, or any uncertainty returns the existing typed failure; the
   existing arming attempt disables coverage and fails closed.

4. ADR-0197's exact-empty conjunction remains unchanged. The helper supplies
   only its four table facts; application frontier `None`, allocator
   `Next(first)`, dormant transient indexes, zero retention watermark, matching
   stamp, and unfenced state remain independently required. `Uninitialized`
   remains the sole state that can attempt arming, and every private/public
   witness, publication, rebase, loss, mismatch, restart, and disablement rule
   remains exact.

5. `store.rs` may add one `#[cfg(feature = "test-fixtures")]`, `#[doc(hidden)]`
   fixed-size, non-cloneable read-only observer captured from the exact
   `RedbOperationalPorts` before those ports move into the coordinator. It may
   report only whether that handle's fresh-locator coverage is currently
   `Armed`, by reading the existing coverage state under its existing lock. It
   cannot arm, reset, disable, clone a proof role, expose a stamp, witness,
   epoch, frontier, key, table, or population, affect scheduling, or survive in
   a default production build. No hook, barrier, failpoint, callback, public
   application API, protocol, configuration, or operator control is added.

6. The production semantic proof captures that observer, proves it reports
   false before submission, then sends the first distinct command through the
   real production coordinator with `CoordinatorDurability::Group`. After that
   command completes and before any later lookup, it must report true. A later
   novel-key operational preinspection and a second command's transaction-local
   recheck must retain their existing absence results with the fallback-history
   counter exactly zero. The test does not call a storage batch, repository
   admission, arming method, or any state-changing test control.

7. The source architecture proof freezes one production arming call in
   `BatchCore::open_with_access`, before allocator read and staging; direct and
   deferred constructors both reach it; dormant admission no longer arms. It
   also freezes the two closed helper branches, the four exact tables, one-row
   and inspected-work bounds, fail-closed propagation, and the observer's
   test-fixture-only read-only shape.

8. ADR-0205 Decisions 4 and 7 through 10 otherwise remain exact. In particular,
   fused terminal admission, transaction and audit ordering, acknowledgement,
   outcome sequencing, publication visibility, durable bytes, failure algebra,
   RequestScoped cause preservation, immediate subsystem stop, success reset,
   consecutive threshold eight, deadlines, cancellation, capacity, retry, and
   evidence eligibility do not change.

## Options considered

1. **Force Group durability onto direct redb writes:** rejected; it changes the
   accepted durability and publication path instead of making the exact proof
   read its owning view.
2. **Trust zero fallback scans alone:** rejected; derived-index coverage can
   produce the same observation while fresh-locator coverage is uninitialized.
3. **Add a controller hook or publication barrier:** rejected; arming is
   synchronous before staging and needs observation, not scheduling control.
4. **Use the bounded logical view plus a hidden test-fixture observer:** chosen;
   it repairs deferred read access and distinguishes the proof without adding
   production authority.

## Consequences

- The first Group-durability command can perform the same exact-empty arming
  attempt as a direct command without acquiring another transaction or changing
  its commit path.
- Four first-attempt logical emptiness checks inspect bounded work once per
  process; all later commands retain the existing constant state check.
- General storage introspection, coverage controls, new hooks, and any change to
  audit-cause handling or production evidence remain unauthorized.

## Standing design tests

- **Interface safety:** applications, agents, operators, transports, and default
  production builds cannot observe, request, reset, preserve, or bypass
  coverage. The hidden fixture observer reports one boolean and has no mutation
  or scheduling authority.
- **Scale:** each of four logical table reads returns at most one row and has a
  fixed inspected-work ceiling; the observer and proof retain fixed-size state,
  and no key or population collection is retained.

## Checks

- `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`
  proves the explicit false-to-true arming transition on the real deferred
  coordinator before later zero-scan misses.
- `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`
  freezes the sole arming site, both access shapes, exact tables and bounds,
  fail-closed behavior, and test-only observation surface.
- Existing ADR-0197 state-machine, exact-empty, direct/deferred publication,
  rebase, mismatch, corruption, restart, and fallback tests plus ADR-0205's
  terminal-audit threshold proof remain unchanged and pass together before any
  production evidence is eligible.
