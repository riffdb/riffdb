---
adr: 0196
title: Cold Columnar Query Outcome Alignment
status: accepted
tier: guarantee
date: 2026-09-04
accepted: 2026-09-04
requires: [ADR-0086, ADR-0195]
amends:
  - ADR-0195 Decision 3 by naming the existing columnar Building result instead of the distinct event-derived projection Degraded shape
supersedes: []
requirements: [OQ-019, OQ-020]
packages: [WP-777]
obligations: []
review_triggers:
  - Cold or activating columnar demand could return success, rows, stale data, a generic empty result, or any outcome other than the existing typed Building/unavailable forms.
  - ExecuteProjectedQueryResult, ProjectionQueryResult, ColumnarLifecycle, VectorProjectionPortError, Protobuf, SDK, CLI, or MCP shapes would change.
  - Event-derived and columnar projection lifecycle vocabularies could be conflated or converted without an explicit checked mapping.
---
# ADR-0196: Cold Columnar Query Outcome Alignment

## Context

ADR-0195 Decision 3 correctly requires cold and activating columnar demand to
return immediately with a typed rowless outcome, but it names
`Degraded { reason: Building }`. That shape belongs to the distinct
event-derived `ProjectionQueryResult<T>` contract in SPEC section 15.3. The
existing columnar application surface is
`ExecuteProjectedQueryResult::Building { applied_through, head }`, backed by
`ColumnarLifecycle::Building`; its `Degraded` arm carries
`riffdb_columnar::DegradedReason`, whose closed values do not include Building.

Adding Building to that unrelated degradation registry or changing public
wire/SDK shapes would violate ADR-0195's own no-new-public-shape decision. The
accepted record must therefore be corrected explicitly rather than silently
implemented with a different outcome.

## Decision

1. Replace only ADR-0195 Decision 3's sentence “For the ADR-0086 projected-query
   result this is `Degraded { reason: Building }`” with: “For the current
   columnar projected-query surface this is
   `ExecuteProjectedQueryResult::Building { applied_through, head }`, backed by
   `ColumnarLifecycle::Building`; vector execution uses its existing `Building`
   error.” All surrounding requirements remain byte-for-byte authoritative.

2. A cold or activating columnar observation has `has_published = false`, an
   empty process-local snapshot that is never queried, the exact checked
   frontier available without opening an artifact (or `BeforeFirst`), and
   `ColumnarLifecycle::Building`. The service's existing lifecycle gate returns
   `ExecuteProjectedQueryResult::Building` before query execution, so it emits
   no rows and cannot become success-with-empty-data.

3. The event-derived `ProjectionQueryResult<T>::Degraded { reason: Building }`
   contract is unchanged and is not reused by the columnar adapter. The
   columnar `ExecuteProjectedQueryResult::Degraded` arm and
   `riffdb_columnar::DegradedReason` registry are also unchanged.

4. No public enum, field, Protobuf message, SDK type, MCP result, CLI rendering,
   retry hint, durable state, freshness rule, or authorization safe point
   changes. ADR-0195's coalescing, worker ownership, validation, failure,
   shutdown, health, and evidence decisions remain unchanged.

## Options considered

1. **Add Building to `DegradedReason`:** rejected because it changes a closed
   public semantic registry and conflates initialization with unhealthy serving.
2. **Add a new public result variant:** rejected because the exact existing
   `Building` variant already expresses the required rowless state.
3. **Align ADR-0195 to the existing variant:** selected because it preserves
   byte and semantic compatibility while retaining the fail-closed behavior.

## Consequences

- WP-777 can implement the accepted first-demand behavior without a public or
  wire-format change.
- Callers continue handling the same existing `Building` result they already
  understand; the only changed behavior is when that result occurs.
- A future unification of event-derived and columnar lifecycle vocabularies is
  deferred and requires its own accepted interface decision.

## Standing design tests

- **Interface safety (AGENTS.md boundary 11):** Cold demand remains rowless and
  typed. No caller gains a fallback, activation switch, validation override, or
  way to reinterpret Building as successful empty data.
- **Scale:** The correction adds no state, allocation, scan, task, wait, or
  artifact work; it only aligns the decision text with an existing bounded
  result variant.

## Checks

- ADR-0195 proof
  `first_columnar_demand_coalesces_one_activation_and_returns_building_without_rows`
  asserts the exact `ColumnarLifecycle::Building`, empty snapshot, single
  activation, and absence of artifact I/O across concurrent first demand.
- Existing service response and transport conformance fixtures remain
  byte-identical; no generated source changes.
