---
adr: "0255"
title: A Declared Approximate Vector Tier Must Be Reachable
status: accepted
tier: surface
date: 2026-09-21
accepted: 2026-09-21
acceptance: 'maintainer, in session, 2026-09-21, choosing "Honor the declaration"
  over rejecting unservable declarations or leaving the limit documented, under
  the grant to write and accept records this session (ADR-0255 as written)'
requires: [ADR-0229]
amends: []
supersedes: []
requirements: [VEC-010]
packages: []
obligations: []
review_triggers:
  - Any change to the production vector scan budget.
  - A declared ANN bound that the serving path cannot reach.
---
# ADR-0255: A Declared Approximate Vector Tier Must Be Reachable

## Context

A contract declares an approximate vector tier with `ann_threshold` and
`recall_target_bps`. The threshold is the admitted row count above which the
first-party HNSW tier engages; below it, search is exact. The declarable range
is 1 to `MAX_VECTOR_ANN_THRESHOLD_ROWS_PER_ORG`, 65,536, and the format
registry describes that bound as rows per organization.

The production vector path passed a scan budget of 500 rows for every field.
The merge that feeds candidate admission happens before the threshold is
tested, and `merged_org_bounded` returns `None` once an organization's merged
rows exceed the budget, so any organization above 500 rows received
`ScanBudgetExceeded`. A declared threshold of 500 or more could therefore never
engage: the query was refused before the tier it declares could run, and
`recall_target_bps` described a tier that never executed in production. The
declaration was accepted by the compiler, encoded into the bundle, folded into
the columnar identity, and inert.

That is a public surface an application author can express which the engine
silently does not honor, which architecture boundary 11 forbids.

## Decision

1. A vector field that declares an approximate tier is served up to
   `MAX_VECTOR_ANN_THRESHOLD_ROWS_PER_ORG` rows per organization. The
   declaration's own bound is expressed in that unit, so an author who declares
   a threshold has accepted being measured against it.
2. A vector field that declares no approximate tier keeps the server's own
   smaller ceiling of 500 rows. It asked for exact search and nothing else, and
   nothing in the contract asks the server to carry more.
3. The scan budget is derived from the definition, never a literal at the call
   site. A declared bound the serving path cannot reach is a defect in this
   decision, not a documented limitation.

## Consequences

- A declared ANN tier can engage. `recall_target_bps` describes a tier that
  runs, so `VEC-010`'s recall contract becomes measurable in production rather
  than in tests only.
- Worst-case per-query merge work for a declaring field rises from 500 rows to
  65,536. That is the bound the contract author chose; it is bounded, declared,
  and per organization. A field that declares nothing is unaffected.
- Above 65,536 rows per organization the query still refuses rather than
  degrading. The ceiling remains a bound on what is answerable, and
  `docs/known-limitations.md` says so.
- This record does not change when ANN engages, the graph's construction,
  freshness selection, or the exact tier. Only the budget that gates the merge.

## Standing design tests

- **Interface safety:** a contract cannot declare an approximate bound the
  serving path refuses before reaching. The declaration and the budget are now
  one value with one source.
- **Scale:** work stays bounded per organization by a compiler-validated
  declaration, not by a server literal, and the undeclared path keeps the
  smaller bound.

## Checks

- `a_declared_approximate_tier_is_served_up_to_its_declarable_bound`
- `an_undeclared_field_keeps_the_servers_own_ceiling`
