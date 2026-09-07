---
adr: "0210"
title: Passive Transient-Index Rebuild Observation for Production Proof
status: accepted
tier: guarantee
date: 2026-09-06
accepted: "2026-09-07"
requires: [ADR-0197, ADR-0205, ADR-0206, ADR-0208]
amends: [ADR-0205, ADR-0206, ADR-0208]
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-019]
packages: [WP-705]
obligations:
  - id: OBL-0210-1
    package: WP-705
    proof: production_grouped_command_arms_fresh_locator_coverage_without_manual_arm
    says: A cloned passive controller proves zero transient-index rebuilds while two real production Group commands, their distinct post-publication novel-key inspections, and successful coordinator shutdown complete with zero fallback scans.
  - id: OBL-0210-2
    package: WP-705
    proof: passive_rebuild_observation_matches_one_real_rebuild
    says: A calibration test causes exactly one real transient-index rebuild and proves the cloned controller reports one in parity with the operational-port counter.
  - id: OBL-0210-3
    package: WP-705
    proof: production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths
    says: Architecture checks freeze the passive controller mirror, its sole increment site, both shared-state rebuild sites, the bounded-start exclusion on the raw ephemeral rebuild, saturation, and the fresh-locator arming location and bounds.
review_triggers:
  - The controller observation would callback, block, allocate per rebuild, fail, reset, decrement, wrap, affect a branch or result, or expose state-changing authority.
  - Installation or observation would occur without explicitly selecting the pre-existing doc-hidden Rust test API, or ordinary `RedbStore::open`, production composition, a service, protocol, or configuration would select it.
  - A shared-state rebuild or any rebuild reachable from verified bounded startup would bypass the sole note method, the mirror would increment other than exactly once per note, or calibration would not prove exact parity after a real rebuild.
  - The semantic proof would retain or fabricate another operational repository, widen shared-port traits, bypass the existing concrete production Group helper or coordinator inspector, manually arm, or omit any zero assertion.
  - WP-705 would touch another path, or fresh-locator, transient-index, audit-cause, threshold, transaction, acknowledgement, publication, restart, bound, or evidence rules would otherwise change.
---
# ADR-0210: Passive Transient-Index Rebuild Observation for Production Proof

## Context

ADR-0208 requires the WP-705 semantic proof to retain
`RedbOperationalPorts` while a real production Group coordinator runs on
`ports.shared_ports()`. The public coordinator's repository bound includes
five administration mutation traits intentionally absent from
`RedbSharedPorts`; only the move-only `RedbOperationalPorts` implements the
complete bound. Widening the pure-read shared handle or fabricating a test
repository would change or bypass the production path instead of proving it.

The storage handle already counts every transient-index rebuild, but that
counter becomes inaccessible when the complete operational ports move into the
coordinator. `RedbTestController` is already clone-shared, installed only by an
explicit doc-hidden recovery-test open, and passively mirrors bounded migration
and fresh-locator observations. Mirroring the existing rebuild notification
there lets the test retain observation, not repository authority.

## Decision

1. ADR-0205 Decision 9, ADR-0206 Decision 1, and ADR-0208 Decision 1 are
   amended only to add `crates/riffdb-storage-redb/src/hooks.rs` to WP-705's
   implementation authority. `store.rs`, `architecture.rs`, and
   `command_concurrency.rs` retain their existing authority. The other eight
   exact paths and purposes remain unchanged. The new path is solely for the
   passive observation in Decisions 2 through 4.

2. `TestControllerInner` gains one clone-shared `AtomicU64` transient-rebuild
   observation initialized to zero by all six controller constructors.
   `RedbTestController` exposes a doc-hidden read-only getter and a crate-private
   increment method. The increment uses relaxed atomic ordering and saturating
   update, so it is monotone and can never wrap from a positive value to zero.
   There is no setter, reset, decrement, callback, closure, wait, barrier,
   failure result, state reference, or controller attachment after open.

3. The sole existing `SharedRedb::note_transient_index_rebuild` forwards
   exactly once to the installed controller, when present, and retains its
   existing internal rebuild and walked-row counter updates. The forwarding is
   infallible and does not select, skip, retry, delay, or alter a rebuild. A
   normal production open has `test_controller: None` and performs no mirrored
   atomic update.

4. Both real `TransientIndexes::rebuild_counted` completion sites continue to
   call the sole note method: initial non-bounded operational activation and
   lazy activation from `Dormant`. No other site increments the mirrored
   counter. Architecture checks freeze both call sites, the single forwarding
   call, all constructor initializers, saturation, read-only access, and absence
   of reset or control authority. The existing raw `TransientIndexes::rebuild`
   exact-view branch remains ephemeral and uncounted, and is reachable only
   while `bounded_clean_startup` is false; architecture checks freeze that guard
   so raw rebuild cannot bypass observation on a verified bounded start.

5. ADR-0206 Decisions 5 and 6 and ADR-0208 Decisions 5 and 6 are narrowed only
   in how the proof retains rebuild observation. The test clones an observing
   controller, opens the exact command-empty database with it through verified
   bounded fast start, and checks both the live ports and controller report zero
   rebuilds. It prepares both audited commands against those live ports, with
   zero asserted after each preparation, then moves the concrete
   `RedbOperationalPorts` into the existing production Group-durability helper.

6. After each command completion and after each distinct exact-scope novel-key
   `command_idempotency_inspector` result, the retained controller must still
   report zero rebuilds. Each inspection returns
   `CommandIdempotencyPlanSelection::Absent`, and the existing cloned controller
   fallback counter remains zero. The proof does not call repository admission,
   a storage batch, arming, a transient builder, the private observation
   increment, or any state-changing test control. After successful coordinator
   shutdown, the controller must still report zero rebuilds.

7. A separate calibration opens storage with the controller and causes exactly
   one real transient-index rebuild through an existing production activation
   path. While the operational ports are still retained, it proves their
   existing counter and the cloned controller counter both equal exactly one.
   The test never invokes the observation increment directly. This prevents a
   disconnected, duplicated, or permanently-zero mirror from validating the
   semantic proof.

8. The required red sequence first lands ADR-0208's exact-empty administration
   branch and removes dormant admission arming while retaining the passive
   controller. The first post-command novel inspection must increment the
   fallback counter while rebuilds remain zero. Moving the sole arming attempt
   to `BatchCore::open_with_access` then makes both novel inspections complete
   with zero fallback scans and zero rebuilds.

9. ADR-0206's rejection of new test authority and statement that no hook or
   public method is added are narrowed only for this passive extension of the
   pre-existing doc-hidden recovery-test controller. ADR-0208's retained-ports
   and shared-coordinator wording is replaced by Decisions 5 and 6. No
   ADR-0197 proof state, coverage role, storage or journal byte, transaction,
   ordering, acknowledgement, publication, failure, restart, audit-cause,
   threshold, or evidence rule changes.

## Options considered

1. **Widen `RedbSharedPorts`:** rejected; administration mutation traits would
   violate its least-authority pure-read purpose to make a test compile.
2. **Build a local repository facade:** rejected; duplicated delegation would
   prove a test repository rather than the concrete production composition.
3. **Infer rebuilds from events or time:** rejected; events are bounded and may
   truncate, while timing cannot prove absence of a rebuild.
4. **Add callbacks, reset, or state control:** rejected; the proof needs one
   monotone observation, not scheduling or mutation authority.
5. **Mirror the existing notification:** chosen; the production rebuild and
   coordinator paths remain exact while the test retains only a counter clone.

## Consequences

- The semantic proof uses the same move-only repository and Group helper as
  production while retaining exact rebuild observation.
- One fixed-size atomic is added to an explicitly installed test controller;
  production storage state and default-open work are unchanged.
- General storage introspection, shared mutation authority, attachable
  controllers, callbacks, resets, and application-visible diagnostics remain
  unsupported.

## Standing design tests

- **Interface safety:** the passive read-only getter is a Rust-visible extension
  of the pre-existing controller test API. Installation and observation require
  explicitly selecting that pre-existing doc-hidden Rust test API; ordinary
  `RedbStore::open`, production composition, services, protocols, and
  configuration never select it. The getter is passive and provides no control.
- **Scale:** observation is one saturating fixed-size atomic update per actual
  rebuild and one atomic load per assertion. It retains no rows, keys, events,
  histories, callbacks, or population state.

## Checks

- `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`
  proves the exact cold start, preparations, real Group commands, operational
  inspections, zero rebuilds, and zero fallback scans.
- `passive_rebuild_observation_matches_one_real_rebuild` proves a real rebuild
  yields exact controller/port parity of one.
- `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`
  freezes passive observation, both counted shared-state rebuild sites, and the
  raw ephemeral branch's non-bounded-start guard alongside the sole arming site,
  direct/composite helper, tables, bounds, and fail-closed behavior.
- Existing controller, transient-index, clean-start, administration, ADR-0197,
  ADR-0205 threshold, and ADR-0206 arming tests remain green before evidence is
  eligible.
