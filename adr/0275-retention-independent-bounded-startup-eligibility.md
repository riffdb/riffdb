---
adr: "0275"
title: Retention Independent Bounded Startup Eligibility
status: proposed
tier: guarantee
date: 2026-10-06
accepted: null
acceptance: null
requires: [ADR-0182, ADR-0260, ADR-0265, ADR-0186]
amends: [ADR-0260 Decision 1, ADR-0182 bounded dirty eligibility]
supersedes: []
requirements: [REC-005, REC-006, REC-007, STO-030, STO-032, END-008]
packages: [WP-825]
obligations:
  - id: OBL-0275-1
    package: WP-825
    proof: pruned_v2_startup_uses_registry_completion_without_population_walk
    says: Actual online pruning removes the validated-prefix checkpoint; a completed registry binding still permits bounded V2 startup without population enumeration or a checkpoint refresh.
  - id: OBL-0275-2
    package: WP-825
    proof: registry_completion_requires_exact_end_and_survives_only_compatible_writes
    says: Incomplete validation, partial migration, changed registry or incarnation and uncertain completion never grant completed eligibility; checked completion and later ordinary writes and prune preserve it atomically.
review_triggers:
  - Completion could be asserted by an application, operator argument, incomplete walk or unclassified migration.
  - A source admission or referenced fence check, first-access validation or scrub authority withdrawal changes.
  - A legacy record is reinterpreted as completed, a frozen encoding is rewritten, or old incompatible binaries mutate the new registry.
  - Eligibility requires a population walk, history-prefix refresh, extra writer, or process-local proof after restart.
---
# ADR-0275: Retention Independent Bounded Startup Eligibility

## Context

The 1,500-second WP-825 diagnostic prunes history while maintaining the full
65,536-ticket population. A stopped-source private copy reconciles all 215,536
archive records but does not reach native readiness within 60 seconds. Startup
declines the V2 CLEAN shortcut and selects complete validation. Online pruning
must delete the validated-prefix checkpoint; ADR-0260 Decision 1 requires a
loadable checkpoint for bounded dirty startup. That checkpoint also serves as
the durable evidence that legacy catalog/index migration finished. Losing a
history optimization therefore loses unrelated migration eligibility.

The maintainer authorized necessary native repairs, including architecture
investigation. Broad authorization does not accept this exact record. It remains
proposed; no new encoding or startup eligibility is implemented before acceptance.

## Decision

1. Separate completed current-registry validation from validated history-prefix
   evidence. Add `StoredRecordRegistryV3` at the existing `record_registry/v2`
   metadata key and existing RecordRegistry authoritative namespace. Preserve
   the frozen V2 record and all old receipts. The new canonical envelope uses
   the existing versioned Protobuf registry/checksum conventions and a compiled
   256-byte encoded bound; no new dependency, table, catalog namespace or key.
2. Freeze V3 fields: `registry_digest` (bytes, field 1, exactly 32 bytes), and a
   required oneof with `pending` (empty message, field 2) or `completed` (message,
   field 3). Completed has `database_id` (bytes, field 1, exactly 16 bytes),
   `history_incarnation` (uint64, field 2, existing incarnation bounds), and
   `registry_digest` (bytes, field 3, exactly 32 bytes equal to the outer digest).
   Missing/unknown states, noncanonical encoding, invalid sizes, checksum or
   duplicate-field failures retain existing typed corruption/refusal behavior.
3. Only the closed native complete-validation owner may publish Completed.
   Require successful exact-end structural and catalog-semantic proof under
   the exclusive startup/maintenance owner, all required catalog/index migration
   completed, the exact database/incarnation and current registry unchanged,
   and zero findings. Bounded startup, a background scrub, a stale prefix
   candidate, row counts or a caller boolean cannot create this proof.
4. Publish completion through the sole ordered writer as the existing
   coordinator-owned validated-prefix completion transition, with the exact
   registry before-image and its canonical V3 after-image in the same atomic
   receipt as the normal checkpoint completion. Success is visible only after
   existing durability and publication boundaries. Inactive legacy activation
   uses its existing complete-proof cutover. Unknown commit outcomes retain
   fencing and recovery; no separate unreceipted eligibility write is allowed.
5. Read V2 as having no independent completion witness. Existing supported
   registries and receipts remain readable byte for byte. A compatible legacy
   V2 checkpoint may still admit the existing bounded path, but cannot be
   reinterpreted as a V3 completion witness. An old database without either
   proof takes the complete path once. Registry migration installs Pending
   before serving under the successor registry and earns Completed only through
   decision 3. A crash between migration batches must never yield completion.
6. Amend ADR-0260 Decision 1: a V2 source with V3 active, current registry, not
   a follower and no valid clean certificate admits bounded dirty startup when
   either the existing checkpoint loads or a Completed V3 registry binding
   matches its exact database, incarnation and current registry. A pending,
   absent or stale completion grants no shortcut. Preserve automatic selection,
   dirty-generation consumption and the existing closed startup mode.
7. Keep every pre-readiness bounded-root, allocator, source-admission and
   referenced-fence/receipt check. Keep fail-closed first-access row validation,
   the rate-bounded complete background scrub and V2 authority withdrawal on
   retained-history/fence failure. V2 CLEAN remains declined under ADR-0260
   Decision 5. Followers retain complete validation at the applied head.
8. Ordinary current-format writes and retention preserve the completed registry
   binding. Online prune still deletes every validated-prefix checkpoint in
   its first step, with unchanged tombstones, watermarks, holds and crash rules.
   A registry or incarnation change invalidates completion before the successor
   serves. Restore, import and activation may establish a successor completion
   only from their existing complete proof for that exact successor; a copied
   marker of another binding is insufficient. Preserve public export and exact
   changelog/backup reconstruction of both V2 and V3 registry bytes.
9. Include V3 in the readable/current durable registry and schema fixtures;
   keep old fixture bytes intact and route every registry read through one
   semantic type. Older binaries must refuse the successor registry before
   writing. Add no application/agent/CLI option to request completion, skip
   validation, force readiness, suppress the scrub or pause the workload.
10. Preserve ADR-0265's exact-binding background prefix-refresh rule. The new
    marker proves completed format/catalog-index validation, never current
    entity/history correctness, source-reclamation authority or checkpoint
    eligibility. No old prefix certificate survives pruning, and no full
    population walk moves into shutdown or the ordinary write path.

## Options considered

1. **Refresh a whole prefix after every prune:** depends on exact-root stability
   under continuing writes and repeats population work; rejected as readiness
   eligibility. Optional history-prefix refresh remains unchanged.
2. **Treat the current digest as proof:** cannot distinguish interrupted legacy
   catalog-index migration from completed validation; rejected.
3. **Independent completion inside registry metadata:** selected. One bounded
   durable witness survives ordinary writes and pruning, while a real registry
   migration must earn a new witness through complete validation.

## Consequences

- Populated restart after pruning can retain bounded eligibility without waiting
  for workload silence or a fresh full-history checkpoint.
- One reviewed durable record version and its compatibility/migration proofs
  are required. Old databases lacking a usable proof still pay one complete pass.
- This does not solve normal latency, source-reclamation capacity, tombstone
  traversal cost, archive capacity or public restore duration. Those gates remain
  measured independently. No 24-hour run starts as part of this repair.

## Standing design tests

- **Interface safety:** applications cannot assert a proof or opt out of
  transactions, idempotency, scoping, authorization, bounds or durability.
  No new public request exists; native completion requires the existing sealed
  full-proof owner and coordinator-owned durable transition.
- **Scale:** one 256-byte bounded record, fixed lookups and the existing 64-MiB
  dirty-recovery ceiling. No per-entity ledger, mutation queue or extra snapshot
  chain. Exhaustive assurance remains first access plus explicit/background scrub.

## Checks

- A failing native regression first performs actual online prune, observes the
  deleted checkpoint, and requires bounded V2 reopen without population rows.
- Complete-proof, interrupted migration, incompatible registry/incarnation,
  invalid envelope, planted legacy index and exact namespace/key tests.
- Process kills before/after completion and first prune: recover either the
  original Pending/legacy state or fully committed Completed with exact receipts;
  repeat recovery and compare outcomes, events, provenance and source authority.
- First-access corruption and scrub withdrawal regressions with absent prefix
  checkpoints; independent offline full-validator equivalence and no repair.
- Byte-exact V2 fixtures, canonical V3 vectors, public changelog export/replay,
  checkpoint-plus-journal backup/restore and follower conformance.
- Frozen-source acceptance and full CI, then the unchanged required-host
  continuing-load and real populated restart faults with first write within
  60 seconds. The milestone's remaining short-campaign gates stay unchanged.
