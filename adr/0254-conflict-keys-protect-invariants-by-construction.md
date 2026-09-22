---
adr: "0254"
title: Conflict Keys Protect Invariants By Construction
status: accepted
tier: surface
date: 2026-09-21
accepted: 2026-09-21
acceptance: 'maintainer, in session, 2026-09-21: "you have permission to write and
  accept your own ADRs for this session without a human in interest of advancing
  these findings" (ADR-0254 as written)'
requires: [ADR-0252]
amends: []
supersedes: []
requirements: [CMP-014]
packages: [WP-805]
obligations: []
review_triggers:
  - Any grammar change widening what a conflict-key component may name.
---
# ADR-0254: Conflict Keys Protect Invariants By Construction

## Context

`SPEC.md` §8.3 illustrated its diagnostic requirements with `ADB-C042`,
"annual budget invariant is not protected by the declared conflict key". No
compiler pass emits it, and no pass relates an invariant's dependencies to the
declared conflict components. R-03 is the risk that a declared conflict key is
too narrow to serialize the commands that could jointly violate an invariant,
and a printed diagnostic that cannot fire is worse than silence: it reads as a
mitigation that exists.

The condition the diagnostic describes is not reachable under the current
grammar. An aggregate invariant is evaluated against the aggregate root, so two
commands can jointly violate it only by writing one root while deriving
different conflict key values. A conflict-key component may name only a field
of that root's primary key: a child key field or another aggregate's field is
not in scope, and a stored root field is refused by `RDB-C016` because it is
not computable from validated inputs. Every legal conflict key is therefore a
function of the root key.

## Decision

1. `CMP-014`: a declared conflict key protects the invariants its aggregate
   declares, by construction rather than by a dedicated pass. Two commands
   writing one aggregate root always derive one conflict key value.
2. The `ADB-C042` example is removed from `SPEC.md` and replaced by a real
   `RDB-C016` diagnostic and a statement of the construction.
3. The property is proven by enumerating the categories of name a conflict-key
   component could hold, not asserted in prose. Admitting a component outside
   the root primary key breaks the property, and the tests fail if a grammar
   change does so.

## Consequences

- R-03's compiler-side mitigation is now a named requirement with a
  code-derived proof, instead of an unimplemented diagnostic code.
- A future grammar that admits a wider conflict-key component -- for instance
  to reduce contention on a coarse key -- must first implement the dependency
  pass `ADB-C042` described. The failing test names that obligation at the
  moment the grammar changes, rather than after a history is found that
  violates an invariant.
- This record makes no claim about conflict keys that are *coarser* than they
  need to be. Over-serialization is a performance property, and the hot-key
  contention recorded against the museum example is unaffected.

## Standing design tests

- **Interface safety:** an application author cannot declare an aggregate whose
  invariants its conflict key fails to serialize. The unsafe shape is not
  expressible, so there is nothing to opt out of.
- **Scale:** the property is structural and costs no compile-time work. It does
  not grow with the number of invariants, aggregates, or commands.

## Checks

- `a_conflict_key_may_name_the_aggregate_root_key_fields`
- `a_conflict_key_may_not_name_a_child_key_field`
- `a_conflict_key_may_not_name_a_stored_root_field`
- `a_conflict_key_may_not_name_another_aggregates_field`
