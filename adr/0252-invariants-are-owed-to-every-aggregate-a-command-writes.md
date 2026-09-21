---
adr: "0252"
title: Invariants Are Owed To Every Aggregate A Command Writes
status: accepted
tier: guarantee
date: 2026-09-21
accepted: 2026-09-21
acceptance: 'maintainer, in session, 2026-09-21: "you have permission to write and
  accept your own ADRs for this session without a human in interest of advancing
  these findings" (ADR-0252 as written), after the defect was reproduced against
  the compiler'
requires: [ADR-0170]
amends: []
supersedes: []
requirements: [POC-003]
packages: []
obligations: []
review_triggers:
  - A command shape that writes entities of more than one aggregate.
  - Any change to how commit checks or root-validation reads are derived.
---
# ADR-0252: Invariants Are Owed To Every Aggregate A Command Writes

## Context

ADR-0170 admitted partition-local cross-aggregate writes: one command may write
entities of more than one aggregate when every binding derives the same
partition route. Invariant lowering predates that record and was never extended
to it. `lower_locality` picks an *anchor* -- the first mutating binding's
aggregate -- and commit-check lowering derived aggregate invariants from that
anchor alone. The IR coverage proof (`validate_commit_checks`,
`validate_root_validation_reads`) took its expected set from the same anchor, so
it could not observe the omission it was there to prevent.

Two failures follow, and both are reachable from source that compiles. When the
invariant's aggregate is not the anchor, its commit checks are not lowered at
all; the runtime is not a backstop, because an empty check list returns
`CheckedCommandDecision::NonZero` without evaluating anything. When it *is* the
anchor, the root-validation target loop accepted key arguments from bindings of
other aggregates, deriving a root key from a foreign aggregate's key prefix and
validating the invariant against a record the command never writes. Enforcement
therefore depended on binding declaration order, which is not a semantic input.
This is POC-003 ("every committed state satisfies all supported invariants")
failing inside the compiler's own proof, and it is risk R-03 realized.

## Decision

1. Commit checks cover every aggregate that any mutating binding writes. The
   anchor determines partition route and conflict ownership only; it does not
   determine which invariants are enforced.
2. Aggregates are lowered in ascending aggregate-identifier order, and the
   lowered checks are emitted in the IR coverage proof's canonical order
   (invariant, then source bindings, then root-validation reads).
3. A root-validation target for an aggregate is derived only from a binding that
   aggregate owns. A binding of another aggregate never contributes a root key.
4. The IR coverage proof derives its expected set from the written aggregates
   rather than from the locality anchor, and matches a root-validation read to an
   aggregate only when the read reads that aggregate's own root entity.
5. Permuting a command's mutating bindings does not change which invariants it
   enforces or which aggregate roots it validates.

## Options considered

1. **Reject a cross-aggregate command whose non-anchor aggregate declares
   invariants.** Fail-closed and a smaller change, but it makes a contract that
   ADR-0170 explicitly admits uncompilable, and the only way to express the
   application is to move the invariant into application code -- the pathology
   the safety claim exists to remove. Rejected.
2. **Evaluate the missing invariants at commit time instead.** Rejected. The
   commit coordinator evaluates exactly what the plan carries, by design; adding
   a second, plan-independent source of invariant enforcement would make the
   compiler's proof no longer the authority on what a command enforces.
3. **Lower per written aggregate and make the coverage proof total.** Chosen.
   It keeps a single authority for enforcement, and the proof now fails closed
   on any future shape that lowers fewer checks than the schema owes.

## Consequences

- POC-003 holds for V19 cross-aggregate commands. The compiler proof is total
  over the aggregates a command writes, so a future lowering that drops an
  invariant fails IR validation rather than compiling.
- The fabricated-root-key read is gone: an aggregate invariant is now evaluated
  against the root of the aggregate whose invariant it is.
- A V19+ cross-aggregate contract whose non-anchor aggregate declares invariants
  now compiles to a different plan -- more commit checks -- so its plan hash
  moves and its application lock is refreshed. That plan was unsound, so no
  correct deployment depended on it. A command writing one aggregate is
  unchanged: the canonical emission order is the order any plan that passes IR
  validation already had, which the unchanged compatibility fixtures confirm.
- `ADB-C042`, the diagnostic relating invariant dependencies to declared
  conflict-key components, remains unimplemented. This record makes the checks
  exist; it does not prove the declared conflict key serializes them. That gap
  is recorded separately.

## Standing design tests

- **Interface safety:** no contract source can express a command that writes an
  aggregate without also carrying that aggregate's invariants. There is no
  ordering, binding mode, or aggregate arrangement that opts a write out, and
  nothing on the public surface names the anchor.
- **Scale:** work is bounded by the aggregates one command writes, which the
  partition-local rule and `MAX_COMMAND_ITEMS` already bound. Root-validation
  reads remain deduplicated by derived root key within each aggregate.

## Checks

- `declaration_order_of_mutating_bindings_does_not_change_what_is_enforced` --
  the permutation property, over the enforced invariants and validated roots.
- `an_invariant_is_enforced_for_every_aggregate_the_command_writes` -- the
  soundness claim, including that a root-validation read reads its own
  aggregate's root.
- `validate_commit_checks` and `validate_root_validation_reads` -- the IR
  coverage proof, now derived from the written aggregates.
- The unchanged contract bundle and plan fixtures, which prove single-aggregate
  identities did not move.
