# ADR-0167: Required Bounded Limit Maximum

- **Status:** Accepted
- **Direction approved:** 2026-08-29
- **Exact text accepted:** Yes, 2026-08-29
- **Accepted:** 2026-08-29
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Claude Code session for drafted commit `a448af6d`
- **Decision deadline:** Before the next application lock freezes a query
  module compiled from `Limit` source
- **Requires:** ADR-0055, ADR-0108, ADR-0124, ADR-0158, and ADR-0159
- **Amends if accepted:** ADR-0158's retention of the unbounded `Limit` form,
  and SPEC 1.15's statement that old `Limit` source remains byte-exact
- **Defines or blocks:** Nothing further; the implementation is complete and
  awaits this record

> **Amendment:** ADR-0174 raises the required bounded maximum's structural
> range to 65,534. Bounds through 499 retain V9/V12/V12 identity; larger bounds
> use the additive V11/V14/V14 identity.

## Context

ADR-0158 added `Limit<MAX>` and deliberately kept the unbounded `Limit` form,
on the reasoning that old source should keep compiling. Building a production
OpenFGA datastore adapter against the language exposed what keeping both costs.

An unbounded `Limit` is charged at its type maximum of 499 whatever default it
declares. `OQ-063` is explicit that this is not only a planner rule: "planner,
role, scan, hydration, policy, intermediate, output-row, and result-byte
budgets use the declared maximum cumulatively for every binding." Role
authority is sized the same way.

So this declaration:

```riffql
$limit: Limit = 50,
```

reads as a fifty-row bound, is charged as 499 rows, and grants the role
authority for 499 rows. It is the only construct in RiffQL whose apparent
meaning differs from its enforced meaning, and it differs in the direction of
more authority. An author who writes it has made a security-relevant choice
without any surface indicating a choice was made.

The friction is measurable. Bringing that adapter's paged queries under the
whole-request cost ceiling took six trial compilations and, in the end, reading
`TypeReference::Limit => max_query_page_take()` in the compiler, because
nothing in the language, the diagnostic, or the error said that the declared
default was not the charged bound. RDB-QP010 now reports the charge and the
ceiling, which removes the guessing, but it does not remove the trap: a query
that compiles with `Limit = 50` still authorizes 499 rows silently.

Every bare use is either exactly equivalent to `Limit<499>` or a mistake.

## Proposed Decision

RiffQL source MUST declare a maximum on every `Limit` parameter. The unbounded
form is removed from the grammar with a diagnostic that names the bounded form
and states why the maximum matters:

```
RDB-QS003 Limit must declare its maximum
  help: use Limit<MAX> with MAX from 1 through 499; the declared maximum is
  what cost and role authority are charged at, not the default
```

The removal is scoped to source. `QueryRowLimit::Parameter` and every module
codec that can encode it remain readable, so an already-compiled module still
decodes, validates, and replays byte-exactly under ADR-0124 topology
governance. What is no longer possible is reproducing such a module from
source at its former codec version.

SPEC 1.15's revision entry states that "old `Limit` source and every
V1-V8/V1-V11/V1-V11 artifact remain byte-exact and readable." Accepting this
record amends that sentence to cover artifacts only. The exact proposed
replacement text is:

> Every V1-V8/V1-V11/V1-V11 artifact remains byte-exact and readable. Source
> declaring an unbounded `Limit` is rejected from RiffQL V9 onward and must
> declare a maximum; already-compiled modules that encode an unbounded runtime
> limit remain readable and replayable and are not rewritten.

`OQ-062` gains one sentence:

> A `Limit` parameter MUST declare its maximum. The unbounded spelling is not
> accepted in source.

## Options Considered

**Keep both forms and diagnose only on ceiling rejection.** Non-breaking, and
it addresses discoverability. Rejected because it leaves the authority trap
intact: a query that compiles is never diagnosed, and that is exactly the query
that silently grants 499 rows.

**Treat a declared default as the maximum.** Rejected for the reason ADR-0158
already gives: an existing caller may submit 499, and defaults may change
independently of type domains.

**Keep both forms and document the rule.** Rejected. The rule is already
documented in `OQ-063` and was still missed by an agent reading the language
surface, which is the population this language is designed for.

## Consequences

The cost is a version floor, and it is larger than it first appears.

Any query with a parameterised page is now RiffQL V9 and query-module format
V12. An exact-predicate family *requires* a typed limit parameter, so every
such family is V12 and can no longer be expressed at V9 or V10. A parameterised
nearest-K is a typed limit, so vector queries with a runtime K are V12 as well.
Tests that pinned those versions move accordingly; the properties they assert
about codec stability are preserved by asserting the module is not the
operational codec rather than by pinning a specific older number.

53 declarations across 42 files migrate to `Limit<499>`, which preserves the
charge, the authority, and every cost vector exactly, so no plan hash changes
for a reason other than the source text itself.

Two latent defects surfaced and are fixed with this change. `Limit<50>=25`
did not parse, because `>=` lexes as one relational token and closing a
bounded maximum has to split it; with a maximum now mandatory every author
would have met it. The TypeScript generator emitted
`{"kind":"limit","maximum":N}` into a union that declared `"limit"` with no
`maximum`, so any corpus using the bounded form failed to typecheck — latent
since ADR-0158 because nothing checked in used it.

## Compatibility

Durable artifacts are unaffected. Source is not. This is a breaking language
change taken while the project is pre-alpha and no external corpus exists; it
would not be available later without a deprecation period.

## Security

Narrowing. Role authority for a paged query was sized at 499 rows regardless
of the author's declared default; it is now sized at a maximum the author
stated deliberately. No path widens.

## Standing Design Tests

- A source `Limit` without a maximum is rejected, and the diagnostic names
  `Limit<MAX>` and states that the maximum governs cost and role authority.
- `Limit<MAX>` closes correctly whether or not a space precedes a default.
- A module compiled before this change still decodes, validates, and replays
  byte-exactly.

## Testing

`plain_limit_is_rejected_and_the_diagnostic_names_the_bounded_form` and
`a_bounded_maximum_closes_even_when_the_default_follows_without_a_space` in
`crates/riffdb-riffql-syntax/tests/bounded_limit_syntax.rs`.

## Requirements and Work Packages

Amends `OQ-062`. No new work package: the implementation landed with this
record and is reversible by restoring the parser branch.

## Decision Deadline

Before the next application lock freezes a query module compiled from `Limit`
source.
