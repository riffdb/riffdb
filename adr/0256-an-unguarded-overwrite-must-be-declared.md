---
adr: "0256"
title: An Unguarded Overwrite Must Be Declared
status: accepted
tier: guarantee
date: 2026-09-21
accepted: 2026-09-21
acceptance: 'maintainer, in session, 2026-09-21, choosing the distinct keyword over an
  additive declaration or a deploy-time report, and choosing that the classification
  participates in the plan hash (ADR-0256 as written, after both questions were put
  with their migration costs stated)'
requires: [ADR-0252]
amends: []
supersedes: []
requirements: []
packages: [WP-807]
obligations: []
review_triggers:
  - Any change to how a mutation's precondition is analysed or declared.
  - A grammar change to the write surface.
---
# ADR-0256: An Unguarded Overwrite Must Be Declared

## Context

A design review held that the safety claim stops where ordinary multi-tenant
patterns begin: slot allocation, a limit summing rows the command does not
name, and rollups were called inexpressible, forcing an application to
implement them unsafely outside a command. Checked against the compiler, two of
the three are expressible today, and the residue is narrower and sharper than
the review described.

A limit over rows a command does not name is expressible by maintaining the
rollup on the aggregate root in the same command and putting the invariant on
the root; the conflict key covers the root, so concurrent writers serialize and
the invariant is checked at commit against the value the command wrote. Choosing
a target in the application is safe when the command re-checks the condition the
choice depended on: `require is_free: slot.free else SlotTaken {}` makes a
losing choice a declared outcome rather than a lost update. Neither is a hole.

The hole is that nothing requires the re-check. The same command with the
`require` deleted compiles, and the two forms are indistinguishable in commit
checks, conflict ownership, and the plan. Two concurrent claims then both
succeed and the second silently replaces the first holder. Last-writer-wins is
the default, it is invisible in source and in review, and an author who believes
the database proves their writes is not warned. That is a public surface on
which an application can silently opt out of a guarantee, which architecture
boundary 11 forbids.

Not every unconditional overwrite is wrong. Setting a display name is
last-writer-wins by intent, and forcing a guard onto it would be noise that
teaches authors to write a vacuous `require`. The defect is not that blind
writes exist; it is that a blind write and a proven one look the same.

## Decision

1. The compiler classifies every field write on a `mutate`, `init_or_mutate`,
   or `observe_or_initialize` binding as **guarded** -- the command declares a
   `require`, or the aggregate an invariant, that reads that binding's
   pre-image -- or **unguarded**.
2. An unguarded write is spelled `overwrite`, distinct from `set`. `set` on
   such a binding requires a precondition on the field's prior value; a write
   with none must say `overwrite` at the point of use. This is a breaking
   grammar change and selects a new grammar, executable IR, and bundle version.
3. The compiler proves the spelling against the command. A `set` whose field
   has no precondition is a diagnostic naming the field and what would satisfy
   it; an `overwrite` that is in fact guarded is a diagnostic telling the
   author to use `set`. Deleting a `require` therefore fails to compile rather
   than silently changing concurrency semantics, which is the property this
   record exists for.
4. The classification participates in the plan hash. Moving a write between
   guarded and unguarded is a change in concurrency semantics, so it moves the
   contract's identity and trips the application lock like any other semantic
   change, and a deployed plan can be audited for the property rather than only
   a compiling source.
5. `riffdb contract explain` reports each write's classification, so the
   property is legible without reading the lowering.

## Options considered

1. **A distinct spelling for the unguarded write**: `overwrite slot.held_by =
   holder` beside the guarded `set`. Chosen. Clearest at the point of use and
   impossible to miss in review. It is a breaking grammar change: every
   existing contract's unguarded writes stop compiling until respelled, and
   every one must be classified by a human as part of the migration. That
   migration is the point, and it is also the cost. It is cheapest now:
   ADR-0072 made the same argument for the incarnation fence, that before alpha
   an additive change becomes, after alpha, a breaking migration against live
   client assumptions.
2. **A binding- or command-level declaration** naming the fields written
   without a precondition, leaving `set` unchanged. Additive, so existing
   contracts keep their identity until they opt in, and it puts the whole list
   in one place a reviewer reads. Weaker at the point of use: the declaration
   can drift from the writes it describes, which decision 3 must then catch.
3. **A deploy-time report with no rejection.** Cheapest and breaks nothing, but
   it makes the guarantee advisory. A surface an author can ignore is the thing
   this record exists to remove.
4. **Require a guard on every overwrite.** Rejected. It would make a
   display-name write ceremonial and train authors to write vacuous
   preconditions, which is worse than the current silence.

## Consequences

- Last-writer-wins becomes a choice an author makes and a reviewer sees, never
  a default, and deleting a `require` becomes a compile error rather than a
  silent change in concurrency semantics.
- Every existing contract must be classified once. Under option 1 that is a
  breaking change gated on a grammar version; under option 2 it is additive.
- This does not make application-side selection unnecessary, and does not add
  a range read. It makes the command that follows a selection prove the
  condition the selection assumed, which is the part that was optional.
- Every affected contract's identity moves, by decision 4 and again by the
  grammar version decision 2 selects. Both were weighed: the classification is
  a semantic property, and a plan whose write semantics changed should not keep
  its old hash. Contracts using only `set` on guarded writes are unaffected in
  meaning but still recompile under the new grammar version.

## Standing design tests

- **Interface safety:** after this record, an application cannot express a
  silent lost update. It can still express a deliberate overwrite, but only by
  saying so where a reviewer reads it.
- **Scale:** classification is a per-write local analysis over a command's
  already-bounded bindings and requires. It adds no runtime work and no
  unbounded compile-time work.

## Checks

- `an_unguarded_overwrite_is_accepted_and_unmarked` characterises today's
  behavior and must change when this record is implemented.
- `a_claim_that_rechecks_its_precondition_compiles` and
  `a_limit_over_unnamed_rows_is_expressible_as_a_root_rollup` pin what is
  already expressible, so the fix is not mistaken for new expressiveness.
