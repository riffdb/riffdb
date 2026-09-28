# WP-817: Atomic indexed selection preparation

This is a preparation checkpoint, not a completed feature. ADR-0264 is proposed;
its exact-text acceptance is pending before semantic implementation. The work
uses a dedicated worktree on `feat/atomic-indexed-selection`, based on the
committed main revision, without touching concurrent main-checkout edits.

## Frozen product choices

The maintainer chose atomic selection first, an indexed pool with conflict keys
known before selection, and bounded server retries. One ordinary command selects
the first eligible indexed row, revalidates at commit, and persists its exact
outcome. No cross-partition transaction, cross-aggregate invariant, arbitrary
query, background wait, or durable multi-command orchestration is added.

The proposed record defines source syntax, one-candidate access, scope and
ownership proofs, authorization, current validation for empty outcomes, three
attempts, idempotent empty results, compatibility, and the acceptance matrix.

## Baseline evidence

`explicit_slot_selection_is_the_existing_checked_control` passes under the
current compiler with four input fields and IR V25. The new regression
`an_indexed_pool_allocation_compiles_without_a_caller_selected_slot` fails at
`mutate first Slot.by_available(...)`: the current parser expects `(` after
`first`. The fixture has only request, pool, and holder inputs and retains its
explicit availability requirement and per-write business associations.

This is the required failing test before implementation. Package closure and
full acceptance are intentionally not claimed while this regression is red.

## Integration constraints from inspection

- The existing input-fact proof derives every ordinary binding/root target before
  evaluation. A selected target requires a distinct snapshot-derived proof;
  do not disguise it as caller input or overwrite pre-admission facts.
- The current redb range reader can read beyond its retained entry limit and
  skip entries from other partitions. The new selection path must instead prove
  a partition-constrained equality prefix and stop after one matching entry.
- Ordinary cascade discovery can perform a second snapshot read. Selection must
  capture its entry, entity, and required roots in one consistent owned view;
  simply reusing separate discovery/read calls is insufficient.
- Catalog normalization, runtime target validation, commit dependency matching,
  index mutation derivation, and invariant coverage all consume binding-position
  maps. Extend their shared proof pipeline rather than reconstructing unchecked
  target lists independently in each layer.
- The coordinator already has a three-evaluation ceiling, typed retry exhaustion,
  current index-generation validation, ordered evaluation, and atomic outcome
  recovery. Retain those owners and exclude selectors from optimizations whose
  input-only target assumptions do not hold.
- Selection must carry row-policy evidence even when a requirement rejects with
  no mutations. A denied candidate cannot be skipped or presented as no capacity.

## PR note

- Package: WP-817; Tier: guarantee.
- Behavior: preparation only; no public semantic implementation yet.
- Checks: baseline compiler control passes; new selection regression fails as
  expected. ADR form, scope, and governance checks accompany the preparation.
- Compatibility: no grammar, IR, persisted state, or existing artifact changes.
- Handbook: no available feature is advertised; this internal note is excluded
  from the public handbook until implementation supplies the reachable guide.
- Hazards and follow-ups: exact ADR acceptance, implementation, deterministic
  races, crash coverage, generated surfaces, benchmarks, and full acceptance
  remain open. Human review precedes merge; remove the worktree after that merge.
