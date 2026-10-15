---
adr: "0307"
title: Real-World RiffQL Planner Composition K-Way Merge And Covering Generalization
status: accepted
tier: surface
date: 2026-10-15
accepted: '2026-10-15'
acceptance: 'maintainer, in session, 2026-10-15: "yes, all", in response to the 5 real-world RiffQL planner and executor improvements proposal'
requires: [ADR-0051, ADR-0108, ADR-0133, ADR-0134, ADR-0174, ADR-0175]
amends: [ADR-0108, ADR-0133, ADR-0134, ADR-0174]
supersedes: []
requirements: [RQL-002, RQL-003, RQL-004, QRY-001, QRY-002, QRY-007]
packages: [WP-848, WP-849, WP-850, WP-851]
obligations: []
review_triggers:
  - Combined optional-presence and order-family compilation admits more than 256 total plan members or skips per-member index validation.
  - Non-partition bounded-set k-way merge or single-partition mixed ordering executes an unbounded index scan without a static scan ceiling.
  - Covered-index step execution alters negotiated compact wire carriage for multi-step, singular, or aggregate queries.
---
# ADR-0307: Real-World RiffQL Planner Composition K-Way Merge And Covering Generalization

## Context

RiffQL's operational query compiler (`riffdb-query-compiler`) and executor (`riffdb-query-executor`) already enforce strict compile-time index proof, bounded scan ceilings, and one-snapshot pagination. However, five V1 planner restrictions force application authors to split or duplicate common real-world SaaS queries:

1. **Disjoint `when $param` and `order by $order { ... }` families:** `compile_order_query_family_inner` rejects any query combining optional-presence guards (`when $param`) with a caller-selected sort family (`order by $order { ... }`), forcing $2^k \times m$ separate named queries for filterable, sortable tables.
2. **Non-partition `field in $set` with trailing `order by`:** `operational_access_shape` only permits `in $set` as a non-terminal index prefix on the partition key; `where org_id == $org && status in $statuses order by updated_at desc, id desc` is rejected even when `index by_status_updated (org_id, status, updated_at, id)` is declared.
3. **Equality-only ordinary `candidates` sources:** `type_check_candidate` restricts ordinary `candidates` sources to `==` predicates, rejecting range (`>=`, `<`, `!=`), `in`, `prefix`, and `is null` / `is not null` predicates inside `candidates` set-algebra (`intersect`, `union`, `difference`) pipelines.
4. **Single-binding `many`-only covering index plans:** `Planner::compile` only attaches `CoveredResultLayoutV1` to single-binding, single-result, non-aggregate `many` queries without dependencies, and `QueryCostAccumulator` still charges `point_reads` for covered steps, even though `execute_operational_page_in_snapshot_with_policy` already supports per-step `scan_covered` with zero point reads.
5. **Single-partition mixed `asc`/`desc` ordering:** `choose_access` rejects mixed `asc`/`desc` order terms on single-partition index scans, even though bounded group-order scanning and continuation markers are already implemented for `PartitionSetIndex`.

## Decision

1. **Cartesian `(presence_mask, order_variant)` plan family (`WP-848`):**
   - `OrderQueryFamilyV1` carries optional `presence_parameters: Vec<String>` and `aggregates: Vec<OperationalAggregateV1>`, and `OrderFamilyMemberV1` carries `presence_mask: u16` alongside `variant_id`.
   - `compile_order_query_family` expands all $2^k \times m$ `(presence_mask, order_variant)` combinations whenever $2^k \times m \le 256$ (`MAX_OPERATIONAL_PLAN_MEMBERS`), proving an index-backed access plan for every member and sealing the union of member authorizations and component-wise maximum cost.
2. **Operational predicates in ordinary `candidates` sources (`WP-848`):**
   - Ordinary `candidates` sources admit any single-index operational access shape (`==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `prefix`, `is null`, `is not null`) that constrains the partition route and projects the candidate root key within the declared `within <MAX>` ceiling.
3. **Bounded k-way merge for non-partition `field in $set` + `order by` and single-partition mixed ordering (`WP-849`):**
   - When a declared index has exact equality prefix components, at most one bounded `field in $set` (`set<T, MAX>`) component, optional trailing equality components, and an `order by` suffix matching the remaining index key columns, the compiler plans a bounded multi-prefix index access charging `MAX` index range scans and `scan_ceiling` rows.
   - The executor merges the distinct `field in $set` prefix streams in one snapshot using the existing bounded k-way merge and canonical continuation marker machinery, and admits single-partition mixed `asc`/`desc` index ordering under `MAX_OPERATIONAL_SCAN_ROWS`.
4. **Per-step covering index execution and zero-point-read cost accounting (`WP-850`, `WP-851`):**
   - `Planner::compile` includes operational `aggregate` group-key and measure input fields in their source binding's `predicate_fields` and `cover_required_fields`, attaches `CoveredResultLayoutV1` to any `QueryAccessKind::Index` step without a read row policy whose index (`key + primary_key + cover`) covers all required step fields, and charges `0` `point_reads` in `QueryCostAccumulator`.
   - Compact positional wire carriage (`common_covered_result_layout` / `execute_covered_page_in_snapshot`) remains restricted to single-step, single-result, non-aggregate `many` queries without dependencies.

## Consequences

- Real-world SaaS dashboards and inbox queries can combine optional filters with caller-chosen sort orders, merge bounded status/tag sets by timestamp, compose range/prefix predicates inside `candidates` set algebra, use mixed `asc`/`desc` sort keys, and execute covered multi-binding, `one`/`maybe`, and `aggregate` steps with zero entity point reads.

## Standing design tests

- **Interface safety:** Every plan member, k-way merge stream, candidate source, and covered step remains statically index-proven, partition-scoped, and bounded before execution; no public surface admits an unbounded scan or runtime fallback.
- **Scale:** Combined `(presence_mask, order_variant)` families are bounded by `MAX_OPERATIONAL_PLAN_MEMBERS` (`256`), non-partition `in $set` merges are bounded by `MAX_BOUNDED_SET_ITEMS_V1` (`64`), and mixed-order scans are bounded by `MAX_OPERATIONAL_SCAN_ROWS` (`10,000`).

## Checks

- Compiler, module, executor, and service tests for combined `when` + `order by $order`, non-partition `in $set` k-way merge with cursors, operational `candidates` predicates, per-step `CoveredResultLayoutV1` zero-point-read execution, and single-partition mixed `asc`/`desc` ordering.
