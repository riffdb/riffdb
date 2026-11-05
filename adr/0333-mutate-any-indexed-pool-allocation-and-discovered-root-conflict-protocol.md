---
adr: "0333"
title: Mutate Any Indexed Pool Allocation and Discovered Root Conflict Protocol
status: accepted
tier: guarantee
date: 2026-11-05
accepted: 2026-11-05
acceptance: 'maintainer, in session, 2026-11-05: "approved, lets build" (ADR-0333 as written)'
requires: [ADR-0061, ADR-0170, ADR-0252, ADR-0254, ADR-0262, ADR-0264]
amends: [ADR-0170, ADR-0264]
supersedes: []
requirements: [TXN-001, TXN-002, CMP-013, CMP-014, CMP-015, PERF-012]
packages: [WP-893, WP-894]
obligations: []
review_triggers:
  - "Existing `mutate first` commands change selection semantics, conflict ownership, or plan identity."
  - "A conflict-key component names a child entity field, stored non-key field, or another aggregate's field, violating ADR-0254."
  - "Candidate contention, exhausted candidate window, or row-policy denial is reported as the selector's empty business outcome."
  - "Conflict acquisition waits for a lease while holding a writer slot, staged transaction handle, or partial conflict set."
  - "Non-empty selection omits selected-entity version, index-prefix membership, root invariant, companion, relationship, unique-index, or row-policy validation."
---
# ADR-0333: Mutate Any Indexed Pool Allocation and Discovered Root Conflict Protocol

## Context

`ADR-0264` introduced `mutate first Entity.index(prefix_arguments) as binding else Outcome {}`, which guarantees selection of the globally first entry in canonical ascending complete-index-key order and requires every conflict-key component to be input-computable before reading state. Consequently, allocating an unowned slot from a shared pool requires a pool-wide conflict key (`conflict_key (pool_id)`) and validates the coarse `(partition, index)` epoch at commit time, serializing concurrent allocations against the same pool.

Furthermore, skip-locked selection cannot be retrofitted onto `mutate first`: if caller `A` locks `slot_1`, caller `B` skips `slot_1` and commits `slot_2`, and caller `A` aborts without mutating `slot_1`, then `slot_1` remained available throughout the committed history and `B`'s allocation of `slot_2` is not a legal serial execution of `mutate first`. Supporting concurrent single-round-trip pool allocation without pool-wide conflict serialization therefore requires a distinct contract construct (`mutate any`), a deadlock-free discovered root conflict protocol, and strict separation of empty pool, candidate contention, business rejection, and policy denial.

## Decision

1. **Distinct `mutate any` Construct Preserving `mutate first`:**
   - Add `mutate any Entity.index(prefix_arguments) as binding else EmptyOutcome {}` as a distinct compiled selector mode alongside `mutate first`.
   - `mutate first` (`ADR-0264`) retains its exact canonical-first semantics, input-computable conflict-key requirement, `(partition, index)` epoch validation on all outcomes, and historical IR/bundle/plan identities.
   - `mutate any` promises that a committed selection binds **some** eligible row matching the equality `prefix_arguments` in the target partition at the commit point, without promising that no earlier index key was concurrently locked in flight. Candidate discovery traverses canonical ascending complete-index-key order deterministically.

2. **Preservation of `ADR-0254` Root-Key Conflict Invariant Protection:**
   - Conflict-key components remain restricted to fields of the **aggregate root's primary key** (`CMP-014` / `ADR-0254`). Child primary-key fields, stored non-key fields, and foreign aggregate fields remain forbidden in `conflict_key`.
   - For `mutate any`, the selected entity MUST be the aggregate root of its owning aggregate (e.g., `root Slot` with `key (pool_id, slot_id)`, `partition_by pool_id`, `conflict_key (pool_id, slot_id)`).
   - `validate_locality`, `validate_indexed_selection`, and `validate_binding_plans` accept a `mutate any` root binding whose aggregate `conflict_key` components are each either `is_input_computable()` or `is_selected_key()` of that selected root, provided:
     - All partition-key components remain strictly `is_input_computable()`,
     - Any companion mutable bindings have strictly `is_input_computable()` conflict keys and targets,
     - The selected aggregate declares no secondary `unique` index whose uniqueness key depends on post-mutation values outside the root conflict key.

3. **Deadlock-Free Discovered Ownership Protocol:**
   - Candidate discovery and conflict acquisition occur **outside** deterministic command evaluation and **before** acquiring any storage writer resource or commit-batch slot:
     1. Read a bounded candidate window of at most `K = 16` index entries under `(partition, index, prefix)` from a consistent storage read view.
     2. For candidate `c_i` (`0 <= i < K`), derive the complete conflict key set `C(c_i)` comprising the candidate root's instantiated `conflict_key` plus all input-derived companion and unique-ownership conflict keys.
     3. Attempt an atomic, all-or-nothing, non-blocking acquisition (`try_acquire_mut`) of `C(c_i)` in canonical `ConflictKey` / shard order. If any key in `C(c_i)` is held or has queued waiters, acquire nothing for `c_i` and advance to `c_{i+1}`.
     4. Once `C(c_k)` is granted as a `MutationLease`, materialize the authoritative `ReadSnapshot` for `c_k` and its companion/root targets under that lease and verify via `IndexedSelectionRequest::validate_record` that `c_k` is present and still satisfies the equality prefix. If `c_k` was concurrently consumed or modified out of the prefix between discovery and lease grant, release `C(c_k)` completely and continue scanning remaining candidates in the window.
     5. Never wait on a conflict lease while holding any partial conflict capability or storage writer resource.

4. **Four-Way Separation of Absence, Contention, Business Rejection, and Policy Denial:**
   - **Proven Empty Range (`ProvenEmpty`):** The index range `(partition, index, prefix)` contains zero entries in the authoritative snapshot (or all entries in a stale window were absent on re-read and a fresh range observation has zero entries), and commit-time validation verifies the `(partition, index)` `IndexRangeTarget` epoch is unchanged. **Only `ProvenEmpty` may execute the command's `else EmptyOutcome {}` branch.**
   - **Candidate Contention (`CandidateContended`):** One or more index entries exist under `prefix`, but every candidate in the bounded window `K = 16` across the 3-attempt evaluation budget was locked by concurrent attempts or invalidated before commit. Returns the typed retryable contention error (`CommandAttemptError::RetryBudgetExhausted` / `PoolCandidateContended`), **never** `EmptyOutcome`.
   - **Selected-Row Business Rejection (`BusinessRejected`):** Candidate `c_k` was locked and verified in the snapshot, and deterministic command evaluation selected a declared business rejection outcome (`require ... else Rejected {}`). Validates `c_k` and all command dependencies at commit time and persists `Rejected` without advancing to `c_{k+1}`.
   - **Authorization Denial (`PolicyDenied`):** Command, scope, or row-policy authorization fails on `c_k`. Fails closed immediately with redacted denial, **never** returning `EmptyOutcome` and **never** skipping `c_k` to probe subsequent rows.

5. **Commit-Time Dependency Validation for `mutate any`:**
   - When `mutate any` selects candidate `c_k` (whether mutating `c_k` or returning a `BusinessRejected` outcome), transaction-current validation checks:
     - `c_k`'s exact `EntityObservation` (`EntityVersion` and pre-mutation prefix membership),
     - Every companion binding `EntityObservation`,
     - Every aggregate-root validation observation (`ADR-0252`),
     - Every relationship/delete check, unique-index epoch, and row-policy recheck (`ADR-0264` §10).
   - Commit validation for a selected candidate `c_k` under `mutate any` omits the coarse `(partition, selector_index)` range epoch so concurrent allocations of disjoint slots `slot_1` and `slot_2` in the same pool do not mutually invalidate each other at commit time.
   - Commit validation for `ProvenEmpty` (`else EmptyOutcome {}`) retains the `(partition, selector_index)` range epoch check so an empty outcome cannot commit if an eligible slot existed or was inserted concurrently.

6. **IR Versioning and Transaction-Local Serial Batching:**
   - Allocate the next IR/bundle format version so `IndexedSelectionPlan` encodes `selection_mode: First | Any` and `candidate_window_limit: u8` (`1` for `First`, `16` for `Any`). Plans using `First` or no selector preserve their exact semantics; old readers refuse the new version.
   - Transaction-local serial micro-batching (`serial_micro_batch_eligible`) is evaluated independently for `mutate first` (`WP-892`) and preserved for `mutate any` when staged writes target the same partition.

Example `mutate any` pool allocation with per-slot root conflict key:

```riff
entity Slot {
  key (pool_id: uuid, slot_id: uuid)
  field available: bool
  field holder: uuid
  index by_available (pool_id, available, slot_id)
}

aggregate SlotPool {
  root Slot
  partition_by pool_id
  conflict_key (pool_id, slot_id)
}

command AllocateSlot {
  input (request: idempotency_key, pool_id: uuid, holder: uuid)
  mutate any Slot.by_available(pool_id, true) as slot else NoCapacity {}
  require available: slot.available else NoCapacity {}
  overwrite slot.available = false checked_by available
  overwrite slot.holder = holder checked_by available
  return Allocated { slot = slot }
}
```

## Options considered

1. **Change `mutate first` to skip locked rows and drop index-epoch validation:** rejected because skipping a locked earlier row that later aborts produces a committed history impossible under strict `first` semantics (`ADR-0264`).
2. **Allow child primary-key fields in `conflict_key`:** rejected because widening conflict keys beyond the aggregate root primary key breaks `ADR-0254` (`CMP-014`) invariant protection by construction.
3. **Treat `K` locked candidates as `EmptyOutcome`:** rejected because transient contention on 16 in-flight slots does not prove the pool is empty, and persisting a terminal `NoCapacity` outcome under contention violates idempotent correctness.
4. **Introduce `mutate any` on root-keyed pool entities with non-blocking candidate-window acquisition and four-way outcome separation:** chosen because it preserves `mutate first` and `ADR-0254` while eliminating pool-wide conflict serialization.

## Consequences

- Concurrent callers allocating from an unowned `Slot` pool with `conflict_key (pool_id, slot_id)` acquire disjoint per-slot conflict keys and commit without invalidating each other on the coarse `(partition, index)` epoch.
- `mutate any` explicitly relaxes canonical-first ordering to any eligible row in the prefix while preserving deterministic candidate traversal, point + companion validation, and strict empty-range proof.
- Under extreme contention exceeding `K = 16` concurrent holders per attempt across 3 attempts, callers receive a typed retryable contention error rather than a false empty-pool outcome.

## Standing design tests

- **Interface safety:** Applications cannot opt out of root-key invariant protection (`ADR-0254`), mistake contention or policy denial for an empty pool, or skip unauthorized rows. `mutate first` and `mutate any` have explicit, compiler-checked contracts.
- **Scale:** Candidate inspection is bounded to `K <= 16` index entries per attempt and at most 3 attempts; `try_acquire_mut` locks shards in canonical order without queueing waiters; no work scales with total pool size.

## Checks

- Deterministic counterexample and schedule tests (`skipped_earlier_candidate_that_later_aborts_cannot_satisfy_strict_first_order`, `all_candidates_locked_in_flight_remains_distinct_from_proven_empty_range`, `companion_and_candidate_conflict_keys_acquire_in_canonical_order_without_deadlock`).
- Compiler positive/negative corpus for `mutate any`, `ADR-0254` root-key enforcement, companion input-computability, and IR format round trips.
- Memory and redb parity, concurrent 1/8/32-caller pool allocation benchmarks, row-policy denial non-skipping tests, and process-crash recovery tests.
