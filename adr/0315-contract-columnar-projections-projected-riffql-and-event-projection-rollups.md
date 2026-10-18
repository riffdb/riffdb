---
adr: "0315"
title: Contract Columnar Projections, Projected RiffQL, and Event Projection Rollups
status: accepted
tier: guarantee
date: 2026-10-17
accepted: 2026-10-17
acceptance: 'user, in session, 2026-10-17: "i accept. lets go with expanding mlflow to cover traces/spans/assessment, generally anything else they currently support."'
requires: [ADR-0017, ADR-0086, ADR-0087, ADR-0152, ADR-0162, ADR-0251, ADR-0309, ADR-0312, ADR-0313, ADR-0314]
amends: [ADR-0017, ADR-0087]
supersedes: []
requirements: [PRJ-001, PRJ-002, PRJ-003, PRJ-004, PRJ-005, PRJ-008, OQ-019, OQ-020, OQ-022, OQ-045, OQ-046, OQ-047, OQ-048]
packages: [WP-873, WP-874, WP-875]
obligations:
  - id: OBL-0315-1
    package: WP-873
    summary: Event projections support min, max, and mean measures, UTC temporal bucket key expressions, and trailing key range bounds
    proof: event_projection_supports_min_max_mean_temporal_keys_and_trailing_range_scans
  - id: OBL-0315-2
    package: WP-873
    summary: Columnar queries support UTC temporal bucket group keys, quantiles, stddev, per-measure filters, measure ordering, and 4,096 distinct values
    proof: columnar_query_supports_temporal_buckets_quantiles_measure_order_and_4096_distinct
review_triggers:
  - Existing contracts or query modules that do not declare new projection features change their canonical bundle or plan hashes.
  - Projected RiffQL queries bypass org-scope partition routing, row-level policy admission, or declared freshness fences.
  - Event or columnar projection rollups accept unbounded distinct/quantile state or floating-point business arithmetic.
---
# ADR-0315: Contract Columnar Projections, Projected RiffQL, and Event Projection Rollups

## Context

Workloads that traditionally split across a transactional store and an analytical engine (such as LLM observability, trace/span/score ingestion, and token/cost/latency rollups) expose five gaps between RiffDB's derived storage engines and its contract/RiffQL surface:

1. **Event-driven `.riff` projections lack `min`/`max`/`mean`, event-timestamp bucketing, and trailing key range scans**:
   `.riff` `projection <Name>` declarations currently accept only `count()` and `sum(expr)` measures, expose only `tx.date` (not `hour`/`day`/`week`/`month`/`year` over an event `timestamp` or `date` field), and `QueryProjection` supports only exact prefix `leading_components` rather than a trailing range bound (`lower`/`upper`) on the next group key component (such as a date or hour bucket).
2. **Columnar engine (`crates/riffdb-columnar`) lacks `ADR-0309` analytical parity**:
   While operational RiffQL supports UTC temporal bucket group keys (`hour`, `day`, `week`, `month`, `year`), exact nearest-rank quantiles (`p50`/`median`, `p90`, `p95`, `p99`), `stddev`, per-measure `where` filters, `having`, and ordering grouped results by aggregate measures (`order by total_cost desc`), `crates/riffdb-columnar` supports only the 10 `ADR-0152` base ops over bare `FieldId` group keys, orders groups only by group keys, and caps `count_distinct` at `256` instead of `4,096` (`MAX_INCREMENTAL_DISTINCT_VALUES_V1`).
3. **Scalar columnar projections are configured in daemon TOML (`[[projections]]`) instead of `.riff` contracts**:
   Unlike `projection`, `vector_field`, `text_index`, and `pattern_index`, scalar columnar projections cannot be declared in `contract.riff` or auto-admitted on contract deploy under `ADR-0251`.
4. **Named RiffQL queries (`source projected`) cannot target event projections or scalar columnar projections**:
   In `crates/riffdb-query-compiler`, `source projected <Path>` is restricted to `Entity.vector_field` with a `nearest(...)` binding. Neither event projections nor scalar columnar projections can be queried from compiled `.riffq` modules or generated Rust, Go, TypeScript, and Python application SDK facades.

## Decision

1. **Event Projection Rollups, Temporal Keys, Range Scans & Columnar `ADR-0309` Parity (`WP-873`)**:
   - Extend `.riff` `projection` measures (`crates/riffdb-contract-syntax`, `crates/riffdb-contract-compiler`, `crates/riffdb-contract-ir`, `crates/riffdb-projection`) with monotonic `min(expr)`, `max(expr)`, and exact `mean(expr)` over `i64`, `u64`, `decimal<P,S>`, and `money<C>`, and allow deterministic UTC temporal bucketing (`hour(ts)`, `day(ts)`, `week(ts)`, `month(ts)`, `year(ts)`) over event `timestamp` and `date` fields in projection `key (...)` expressions. Contracts using only V1 projection features retain byte-exact bundle and plan hashes.
   - Extend `ProjectionQuerySelector` and `QueryProjection` (`proto/riffdb/v1/projection.proto`, `crates/riffdb-storage-api`, `crates/riffdb-storage-memory`, `crates/riffdb-storage-redb`, `crates/riffdb-projection`, `crates/riffdb-service`, `crates/riffdb-api-grpc`, `crates/riffdb-client-rust`, `crates/riffdb-cli`) with an optional trailing single-component range bound (`lower_bound` inclusive/exclusive, `upper_bound` inclusive/exclusive) immediately following `leading_components`.
   - Extend `crates/riffdb-columnar` and `ExecuteProjectedQuery` (`crates/riffdb-service`, `crates/riffdb-api-grpc`, `crates/riffdb-client-rust`, `crates/riffdb-cli`) with UTC temporal bucket group keys, exact quantiles (`p50`, `p90`, `p95`, `p99`) and population `stddev`, per-measure equality filters, ordering grouped results by aggregate measure outputs, and a `4,096` distinct-value ceiling (`MAX_COLUMNAR_DISTINCT_VALUES_V1 = 4,096`) bounded by `MAX_AGGREGATE_STATE_BYTES_V1` (`1 MiB`).

2. **Contract-Declared Scalar Columnar Projections & Live Admission (`WP-874`)**:
   - Add `columnar_projection <Name> { source entity <Entity> fields (<field>, ...) }` to `.riff` contract syntax (`crates/riffdb-contract-syntax`, `crates/riffdb-contract-compiler`, `crates/riffdb-contract-ir`), deriving `org_scope_field` from the enclosing aggregate's leading `partition_by` field and validating that all projected fields are non-vector scalar columns on `<Entity>`.
   - Preserve byte-exact V1 `ContractBundle` and `SchemaIr` encodings when a contract declares zero `columnar_projection` blocks; serialize declared columnar projection specs additively when present.
   - Update `resolve_columnar_bindings` (`crates/riffdb-server/src/columnar_admission.rs`) to admit contract-declared scalar columnar projections automatically upon contract activation alongside `vector_field` specs and any daemon TOML `[[projections]]` entries, keeping cold demand-driven activation (`ADR-0195`).

3. **Named RiffQL Queries Over Event & Scalar Columnar Projections (`WP-875`)**:
   - Extend `source projected <Target>` in `.riffq` (`crates/riffdb-query-compiler`, `crates/riffdb-query-module`, `crates/riffdb-service`) to resolve three checked targets against the contract catalog:
     1. `Entity.vector_field` (existing `nearest` path, unchanged).
     2. `ProjectionName` (an event-driven `projection`), with `freshness available` or `freshness causal`, compiling leading key equalities and optional trailing key range bounds (`>=`, `>`, `<=`, `<`) into a typed projection scan that projects key components and measures as ordinary named RiffQL result fields.
     3. `ColumnarProjectionName` (a contract-declared `columnar_projection`), with `freshness available`, `freshness bounded`, or `freshness causal`, compiling org-scoped equality/range predicates, `order by`, `take`, and `aggregate` / `group by` blocks into a checked columnar projected query plan executed through `RiffDbService`.
   - Expose compiled event-projection and columnar-projection RiffQL queries through the standard named-query execution path so `riffdb generate` emits typed methods on Rust, Go, TypeScript, and Python application SDK clients without transport-specific projection glue.

## Options considered

1. **Require applications to call `QueryProjection` and `ExecuteProjectedQuery` directly outside `.riffq`:** rejected; bypasses compiled query modules, application role bindings, and generated multi-language SDK facades.
2. **Keep scalar columnar projections in daemon TOML only:** rejected; prevents the RiffQL compiler from proving that a `source projected` query's selected, filtered, and grouped columns are present in the deployed contract bundle.
3. **Unify contract-declared columnar projections, enriched event projections, and `source projected` RiffQL queries:** selected.

## Consequences

- Applications can express both transactional writes and derived analytical rollups (event-driven pre-aggregations and V2 columnar scans/aggregations) inside one `.riff` contract and `.riffq` query module, consumed through the same generated Rust, Go, TypeScript, and Python SDKs.
- Existing contracts, query modules, and durable artifacts without the new declarations retain their exact hashes and wire bytes.

## Standing design tests

- **Interface safety:** Every projected RiffQL query requires an explicit `freshness` declaration (`available`, `bounded`, or `causal`), proves single-partition org-scope routing at compile time, enforces row-level policy admission before columnar aggregation, and exposes only named operations on generated application SDKs.
- **Scale:** Event-projection range scans remain bounded by page limit (`<= 1,000`) and key width (`<= 4,096` bytes); columnar queries remain bounded by `max_scanned_rows` (`<= 100,000`), `max_group_cardinality` (`<= 10,000`), `MAX_COLUMNAR_DISTINCT_VALUES_V1` (`4,096`), and `MAX_AGGREGATE_STATE_BYTES_V1` (`1 MiB`).

## Checks

- `event_projection_supports_min_max_mean_temporal_keys_and_trailing_range_scans` in `crates/riffdb-contract-compiler` and `crates/riffdb-projection`.
- `columnar_query_supports_temporal_buckets_quantiles_measure_order_and_4096_distinct` in `crates/riffdb-columnar` and `crates/riffdb-service`.
- `contract_declared_columnar_projections_compile_and_admit_live_without_restart` in `crates/riffdb-contract-compiler` and `crates/riffdb-server`.
- `riffql_source_projected_executes_event_and_columnar_projections_across_sdks` in `crates/riffdb-query-compiler` and `crates/riffdb-service`.
