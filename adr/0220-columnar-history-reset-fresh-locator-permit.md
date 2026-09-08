---
adr: "0220"
title: Columnar History Reset Fresh-Locator Permit
status: proposed
tier: guarantee
date: 2026-09-08
accepted: null
requires: [ADR-0213, ADR-0214]
amends: [ADR-0213, ADR-0214]
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-019, END-008]
packages: [WP-705]
obligations: []
review_triggers:
  - Any production path other than reset_for_current_history_incarnation, any table or key other than its already-derived COLUMNAR_PROJECTION_CONTROLS key, or any mutation other than its existing insert would change.
  - Permit registration would move before the existing current-value guard or after the physical insert, or the close, actual-record, insert, drop, or commit ordering would change.
  - Durable bytes, reset semantics, history-incarnation selection, transaction ownership, acknowledgement, publication, fencing, restart, or public behavior would change.
  - The amendment would authorize a general columnar-control repair, a new class, action, helper, hook, interface, dependency, or evidence claim.
---
# ADR-0220: Columnar History Reset Fresh-Locator Permit

## Context

ADR-0213 requires every recognized preserving Immediate class to be fully
assessed while fresh-locator coverage is `Uninitialized`. The first repair's
scoped suite found one existing production omission:
`reset_for_current_history_incarnation` commits a recognized
`ColumnarProjectionControl` replacement without registering the exact
mutation that adjacent columnar-control writes already register. It therefore
fails closed before its existing insert during valid stale-incarnation startup.

ADR-0213 and ADR-0214 deliberately authorize exact repair paths. The required
source is outside them, so implementation must stop until a human accepts one
narrow path-and-purpose amendment. No reset behavior or fresh-locator permit
inventory needs to expand; the method already derives the sole exact key and
performs the already-permitted insert.

## Decision

1. ADR-0213 Decision 1 and ADR-0214 Decision 1 are amended only to add
   `crates/riffdb-storage-redb/src/columnar_projection_control.rs` to WP-705
   authority, solely for the exact
   `reset_for_current_history_incarnation` permit registration in Decisions
   2 through 5 below. No other item, method, path, or purpose is authorized.

2. In `reset_for_current_history_incarnation`, after the existing current-row
   equality guard, replacement construction, and canonical replacement
   encoding, and immediately before the existing physical insert, the same
   `RedbWriteAccess` registers exactly one expected byte insert for
   `COLUMNAR_PROJECTION_CONTROLS` at the already-derived `key`, closes the
   expected inventory, and records exactly one actual byte insert for that
   same table and key. The three calls are exactly:
   `expect_fresh_locator_byte_insert(COLUMNAR_PROJECTION_CONTROLS, &key)`,
   `close_fresh_locator_mutation_expectations()`, and
   `record_actual_fresh_locator_byte_insert(COLUMNAR_PROJECTION_CONTROLS,
   &key)`, each propagating its existing typed error with `?`.

3. The current-row mismatch branch still drops the table, aborts the
   transaction, and returns `StateChanged` before any expectation is
   registered. The successful path retains this exact order: validate current
   history incarnation; read and equality-check the current row; construct and
   encode the same replacement; register expected insert; close inventory;
   record actual insert; perform the existing insert; drop the table; call the
   existing `commit_for(RedbTestOperation::ColumnarProjectionControl)`; return
   `Applied`. No read, mutation, transaction, commit, or result moves.

4. The calls describe the method's existing physical action; they add no
   durable mutation, locator, witness, class, table, key, action, retry, or
   success path. Permit refusal retains ADR-0213's typed fail-closed
   disablement and fencing before physical mutation. Successful pre-arm reset
   retains `Uninitialized`, mints no witness, and leaves the sole later
   command arming opportunity unchanged.

5. This edit belongs to ADR-0213's first pre-arm repair commit and must pass
   with that repair before its separate queued repair begins. It does not
   absorb, reorder, or amend the queued repair. WP-705 runtime evidence remains
   ineligible until both repairs pass together exactly as ADR-0213 Decision 12
   requires.

6. OBL-0213-1 remains the semantic owner; no duplicate obligation is added.
   Its existing proof
   `fresh_locator_recognized_pre_arm_control_writes_preserve_uninitialized`
   retains the complete pre-arm matrix. OBL-0213-4's existing proof
   `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`
   is strengthened to freeze the named method's sole table/key, one
   expect-close-actual sequence, and its ordering before insert and commit.
   The existing server tests named under Checks prove the affected valid
   startup paths.

7. Only after exact-text human acceptance, one separate acceptance commit may
   add the exact source path to the existing guarantee rule in
   `governance/tiers.yaml`, add ADR-0220 to `WP-705.required_adrs`, and
   regenerate required ADR status/index metadata. It must add no glob,
   directory rule, package deliverable, acceptance command, dependency, or
   closure change. This proposal commit changes only this record and its
   generated ADR index.

8. ADR-0213 and ADR-0214 otherwise remain exact. In particular, their class and
   permit inventories, direct and queued helpers, durable bytes, interfaces,
   transaction ordering, acknowledgement, publication, fencing, restart,
   bounds, and evidence eligibility do not change.

## Options considered

1. **Leave the reset unregistered:** rejected; the accepted pre-arm validator
   correctly refuses an unproven recognized mutation.
2. **Exclude reset from pre-arm assessment:** rejected; that would weaken
   ADR-0213 and make one recognized lane a bypass.
3. **Broaden columnar-control authority:** rejected; the suite identified one
   exact omission and no general change is required.
4. **Register the existing exact insert:** chosen; it makes the current
   operation honest without changing its storage effect or result.

## Consequences

- Valid stale-incarnation startup can perform its existing exact reset while
  preserving `Uninitialized` under the accepted permit rules.
- One production file receives three bookkeeping calls and one architecture
  proof gains exact source assertions.
- Any other missing columnar-control permit, behavior change, or repair remains
  unapproved and requires its own evidence and accepted amendment.

## Standing design tests

- **Interface safety:** no application, agent, operator, transport,
  configuration, or test controller can request, omit, forge, observe, or reuse
  the private permit; the public reset contract and result remain unchanged.
- **Scale:** the method records one already-bounded key in the existing
  fixed-capacity expected and actual inventories and adds no read, traversal,
  retained state, or population-dependent work.

## Checks

- `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`
  freezes the exact method, table/key, call counts, and ordering.
- `stale_history_incarnation_resets_columnar_control_before_serving`,
  `columnar_history_reset_retires_exact_colliding_candidate_paths`, and
  `columnar_history_reset_precedes_spec_retarget` prove the three valid
  startup reset paths that exposed the omission.
- The repair-1 named tests, `./scripts/check-file-size-guard`, and
  `./scripts/acceptance --wp WP-705` must pass before its commit; the combined
  suite must pass again after the queued repair and before evidence.
