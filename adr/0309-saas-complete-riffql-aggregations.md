---
adr: "0309"
title: SaaS-Complete RiffQL Aggregations Filtered Measures Group Ordering Temporal Buckets Quantiles And Nested Rollups
status: accepted
tier: surface
date: 2026-10-15
accepted: '2026-10-15'
acceptance: 'maintainer, in session, 2026-10-15: "(Recommended) All 6 SaaS aggregation capabilities below"'
requires: [ADR-0051, ADR-0108, ADR-0152, ADR-0174, ADR-0307]
amends: [ADR-0108, ADR-0152]
supersedes: []
requirements: [RQL-002, RQL-003, RQL-004, RQL-008, QRY-001, QRY-002, QRY-007]
packages: [WP-855, WP-856, WP-857, WP-858]
obligations:
  - id: OBL-0309-1
    owner: WP-855
    summary: Optional numeric, money, and boolean fields and per-measure where filters compile and execute in operational aggregates
    proof: optional_money_and_filtered_aggregate_measures_execute
  - id: OBL-0309-2
    owner: WP-856
    summary: Grouped aggregates support UTC temporal bucketing, having predicates, order by over measures and keys, and top-N take truncation
    proof: grouped_aggregate_temporal_buckets_having_and_order_by_execute
  - id: OBL-0309-3
    owner: WP-857
    summary: Quantile, positional, stddev, and bounded collection measures and per-parent nested rollups over expanded bindings compile and execute
    proof: saas_quantiles_collections_and_per_parent_rollups_execute
  - id: OBL-0309-4
    owner: WP-858
    summary: End-to-end SaaS dashboard queries combining all six aggregation capabilities execute across compiler, module, executor, and service boundaries
    proof: saas_complete_aggregations_compile_and_execute_end_to_end
review_triggers:
  - Any aggregate measure or grouping step introduces floating-point arithmetic, unbounded group state, or wall-clock time dependency.
  - Existing V1/V11 aggregate query plan hashes or module bytes change when none of the ADR-0309 extensions are used.
  - ADR-0152's 11-entry V1 semantic registry slice or provider capability sets are altered.
---
# ADR-0309: SaaS-Complete RiffQL Aggregations Filtered Measures Group Ordering Temporal Buckets Quantiles And Nested Rollups

## Context

`ADR-0108` and `ADR-0152` established bounded operational aggregates (`count`, `sum`, `min`, `max`, `count_present`, `count_distinct`, `count_distinct_present`, `mean`, `any`, `all`) over bounded collection bindings. While sound as an initial foundation, six limitations block real-world SaaS dashboards, billing summaries, cohort tables, SLA monitors, and relational list views:

1. **Optional numeric/boolean and `money<C>` inputs are rejected:** `sum` and `mean` reject optional numeric fields (`i64?`, `u64?`, `decimal<P,S>?`) and fixed-currency `money<C>` / `money<C>?` fields, and `any` / `all` reject `bool?` fields.
2. **No conditional per-measure filters (`where`):** Computing multiple conditional metrics (e.g. `open_count`, `closed_count`, `paid_revenue`, `past_due_revenue`) in one pass over a binding or group requires `count() where <predicate> as <alias>` (`FILTER (WHERE ...)` semantics).
3. **No group `having`, `order by`, or top-N `take` truncation:** Grouped aggregates emit rows only in canonical group-key order, cannot filter groups by measure thresholds (`having`), and fail the whole query when distinct groups exceed `take N` instead of sorting by a measure (`order by total_spend desc`) and truncating to the top `N` groups.
4. **No deterministic UTC time-bucket grouping or group-key/field aliases:** `group by` accepts only bare entity fields, preventing time-series rollups (`hour(ts)`, `day(ts)`, `week(ts)`, `month(ts)`, `year(ts)`) without redundant stored columns, and aggregate result selections reject field aliases.
5. **Missing quantile, positional, dispersion, and bounded collection measures:** SaaS SLA and analytics views require exact nearest-rank quantiles (`median` / `p50`, `p90`, `p95`, `p99`), ordered positional selectors (`first`, `last`), deterministic integer-sqrt fixed-scale `stddev`, and bounded value collections (`collect`, `collect_distinct`).
6. **No per-parent nested rollup aggregates over `for each ... in ...` expanded bindings:** A query that expands child rows per parent (`many tasks for each project in projects ...`) cannot fold child rows per parent and nest the rollup record directly inside each returned parent item (`projects { project_id task_stats { ... } }`).

## Decision

1. **Optional & `money<C>` measure inputs and per-measure `where` filters (`WP-855`):**
   - `sum` and `mean` accept `i64?`, `u64?`, `decimal<P,S>?`, `money<C>`, and `money<C>?` (at the contract currency's fixed scale `2`, widened to `decimal<39,2>`). `NoValue` (`Null`) rows are skipped in `sum` and `mean` (contributing neither to `total` nor `count`). `any` and `all` accept `bool?`, skipping `NoValue`.
   - Any measure may declare `where <predicate>` before `as <alias>` (`sum(amount) where status == InvoiceStatus.Paid as paid_amount`). Conjunctive predicates are resolved and normalized into `Vec<QueryPredicate>` on `OperationalAggregateMeasureV1`, included in source step field projection/covering/authorization, and evaluated per row before folding.
2. **UTC temporal group buckets, group-key/field aliases, `having`, `order by`, and top-N `take` (`WP-856`):**
   - `group by` keys accept deterministic UTC bucket functions `hour(field)`, `day(field)`, `week(field)` (ISO Monday 00:00 UTC), `month(field)`, and `year(field)` on `timestamp` / ` timestamp?` and `date` / `date?` (`day`/`week`/`month`/`year`) with optional `as <alias>`, plus `as <alias>` on plain group keys and aggregate result selection fields.
   - Grouped `aggregate` blocks accept optional `having <predicate>` (over group keys and measure aliases), `order by <key_or_measure> asc|desc [nulls first|last], ...`, and trailing `take <N> | $limit`. When `order by` or `having` is declared, the executor folds up to the bounded group state ceiling (`256`), filters by `having`, sorts by `order by` with canonical group-key tie-breaking, and truncates to `maximum_groups`.
3. **Exact quantiles, positional selectors, fixed-scale `stddev`, bounded collections, and per-parent nested rollups (`WP-857`):**
   - Add operational measures `median(field)` (and alias `p50`), `p90(field)`, `p95(field)`, `p99(field)` (nearest-rank exact order statistics over non-null ordered scalars returning `Optional<T>`), `first(field)` and `last(field)` (first/last non-null value in source order returning `Optional<T>`), `stddev(field)` (and alias `variance`, exact fixed-scale population standard deviation via checked `i128` / `u128::isqrt` returning `decimal<39,S>`), and `collect(field)` / `collect_distinct(field)` (bounded non-null scalar list `List<T>` capped by `MAX_AGGREGATE_DISTINCT_VALUES_V1`).
   - An `aggregate` over an expanded child binding (`for each <item> in <parent>`) may declare `per <parent_or_item>` (or be selected nested inside `<parent> { ... }`), folding child rows per parent key in one snapshot and emitting zero-initialized/empty-set defaults for parents with zero matching children.
4. **Canonical encoding preservation (`WP-855`..`WP-858`):**
   - When an aggregate uses only the pre-ADR-0309 features, `canonical_surface` emits the exact legacy byte layout. ADR-0309 extensions append a domain-separated `SAAS-AGGREGATE-EXTENSIONS-V1\0` trailer only when active.

## Consequences

- Complex SaaS applications can express multi-metric conditional scorecards, time-series charts, top-N leaderboards with `having` thresholds, SLA percentiles (`p50`/`p90`/`p95`/`p99`), tag/member array rollups, and per-parent child summaries in a single bounded, one-snapshot RiffQL query.

## Standing design tests

- **Interface safety:** Every measure, bucket, filter, `having` clause, `order by`, and per-parent rollup is statically typed, deterministic, free of floating-point arithmetic, and fail-closed on overflow or budget exhaustion.
- **Scale:** Group state, distinct sets, and `collect` lists remain bounded by `MAX_AGGREGATE_DISTINCT_VALUES_V1` (`256`) and `MAX_AGGREGATE_STATE_BYTES_V1` (`1 MiB`).

## Checks

- End-to-end compiler, module, executor, and service tests (`OBL-0309-1`..`OBL-0309-4`) and handbook updates in `docs/riffql/LANGUAGE.md` and `docs/riffql/PLANNING.md`.
