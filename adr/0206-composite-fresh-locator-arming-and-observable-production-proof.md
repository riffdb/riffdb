---
adr: "0206"
title: Composite Fresh-Locator Arming and Observable Production Proof
status: proposed
tier: guarantee
date: 2026-09-06
accepted: null
requires: [ADR-0070, ADR-0100, ADR-0104, ADR-0165, ADR-0197, ADR-0205]
amends: [ADR-0197, ADR-0205]
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-019]
packages: [WP-705]
obligations:
  - id: OBL-0206-1
    package: WP-705
    proof: production_grouped_command_arms_fresh_locator_coverage_without_manual_arm
    says: A verified bounded fast start keeps transient indexes Dormant while two real production Group-durability commands and their post-publication novel-key inspections complete with zero rebuilds and zero fallback history scans without manual arming.
  - id: OBL-0206-2
    package: WP-705
    proof: production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths
    says: Architecture checks freeze the sole shared arming site and the private one-row exact logical-table emptiness implementation for both direct redb and deferred composite-stage access.
review_triggers:
  - Logical emptiness would use a different view than the owning mutation-gated command access, inspect or return more than its fixed bound, infer emptiness from allocators or frontiers alone, or omit any of COMMITS, IDEMPOTENCY, IDEMPOTENCY_PENDING, and IDEMPOTENCY_LOCATORS.
  - Composite evidence would come from a non-live, later, startup, checkpoint, reconstructed, caller-supplied, or externally supplied view, or direct access would stop using its original writer transaction.
  - The helper would mutate, persist, publish, checkpoint, drain, await, acquire scheduling authority, alter ordering, or turn a read, bound, borrow, poison, or contradiction failure into absence or an armed proof.
  - The grouped proof would start with active, rebuilt, invalid, or unknown transient indexes; manually arm; bypass the verified clean-close fast start, Group durability, or production coordinator; allow a rebuild; omit either post-publication novel miss; or change Dormant apply-delta behavior.
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

The accepted grouped semantic check is masked when transient indexes are
`Ready`: their derived frontier can independently prove a later miss, leaving
the fallback counter at zero while fresh-locator coverage is uninitialized. A
verified bounded clean-close fast start instead leaves the transient state
`Dormant`; its current `apply_delta` is a no-op in that state. A later novel
miss therefore scans unless the fresh-locator proof was armed and published.

## Decision

1. ADR-0205 Decision 9 is amended only to add
   `crates/riffdb-storage-redb/src/store.rs` to WP-705's implementation
   authority. Its existing six paths, purposes, and restrictions remain exact.
   The added path is solely for Decisions 2 through 4 below; another path or
   purpose requires a separately accepted amendment.

2. `RedbWriteAccess::arm_fresh_locator_coverage` may use one private read-only
   exact logical-table emptiness helper. This narrows ADR-0197 Decision 1's
   direct-redb-transaction-only clause solely for the live mutation-gated
   deferred Group access's same writer-private composite view used by
   `BatchCore::open_with_access`. Non-live, later, startup, checkpoint,
   reconstructed, caller-supplied, or externally supplied composite evidence
   remains refused. Direct access retains redb `Table::is_empty` on its original
   writer transaction. Deferred access borrows its existing composite mutation
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

5. The production semantic proof creates an exact command-empty database,
   completes a clean close, and reopens it through the verified bounded fast
   start. It must assert `clean_close_fast_startup()` is true and
   `transient_index_rebuilds()` is zero, then prepare commands only against
   those live reopened ports. The first distinct command runs through the real
   production coordinator with `CoordinatorDurability::Group`; rebuilds remain
   zero. A post-publication novel-key operational preinspection returns the
   existing absence result with fallback-history scans exactly zero. A second
   distinct command then commits, rebuilds remain zero, and a second
   post-publication novel-key miss leaves fallback scans exactly zero.

6. The proof does not call a storage batch, repository admission, arming
   method, test-only arming hook, transient-index builder, or state-changing
   test control. `TransientIndexState::apply_delta` remains a no-op while
   `Dormant`; no command publication may activate it. Any rebuild, loss of the
   fast-start classification, or unknown transient state invalidates the proof.
   Without fresh-locator arming, the first post-command novel miss has neither
   transient nor fresh coverage and must take the counted history fallback.

7. The source architecture proof freezes one production arming call in
   `BatchCore::open_with_access`, before allocator read and staging; direct and
   deferred constructors both reach it; dormant admission no longer arms. It
   also freezes the two closed helper branches, the four exact tables, one-row
   and inspected-work bounds, fail-closed propagation, and Dormant `apply_delta`
   as a no-op without authorizing an edit to transient-index code.

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
   produce the same zero count while fresh-locator coverage is uninitialized.
3. **Force a rebuild or add test authority:** rejected; either masks the proof
   under another absence authority or adds unnecessary control surface.
4. **Use the bounded logical view plus cold-Dormant behavior:** chosen; it
   repairs deferred read access and makes fallback behavior discriminate the
   proof without adding a surface or hook.

## Consequences

- The first Group-durability command can perform the same exact-empty arming
  attempt as a direct command without acquiring another transaction or changing
  its commit path.
- Four first-attempt logical emptiness checks inspect bounded work once per
  process; all later commands retain the existing constant state check.
- The cold-Dormant proof reuses existing startup classifications and counters;
  general storage introspection, coverage controls, new hooks, and any change to
  audit-cause handling or production evidence remain unauthorized.

## Standing design tests

- **Interface safety:** applications, agents, operators, transports, and default
  production builds cannot observe, request, reset, preserve, or bypass
  coverage. No test hook, public method, protocol, configuration, or scheduling
  authority is added.
- **Scale:** each of four logical table reads returns at most one row and has a
  fixed inspected-work ceiling; the proof retains fixed-size state, and no key
  or population collection is retained.

## Checks

- `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`
  proves verified cold-Dormant fast start, zero rebuilds across two real
  deferred commands, and two post-publication novel misses with zero fallback
  scans.
- `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`
  freezes the sole arming site, both access shapes, exact tables and bounds,
  fail-closed behavior, and Dormant apply-delta behavior.
- Existing ADR-0197 state-machine, exact-empty, direct/deferred publication,
  rebase, mismatch, corruption, restart, and fallback tests plus ADR-0205's
  terminal-audit threshold proof remain unchanged and pass together before any
  production evidence is eligible.
