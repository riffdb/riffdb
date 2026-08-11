# Vector Projection — ADR-0086 Obligation Map

**Work package:** WP-593 (revised by the vectors fix round)
**Status:** honest proven-vs-pending ledger. The previous revision of this
document claimed every unamended ADR-0086 obligation was discharged; an
independent audit classified 0 of its 13 rows as proven. This revision states
exactly what an automated test proves today and what remains pending. A row is
**PROVEN** only when the named test fails if the property is broken.

## Proven today

| Obligation | Status | Evidence (test that reds if broken) |
|---|---|---|
| Org-partitioned execution: one org scope per nearest query (VEC-005/VEC-009) | PROVEN | `tests/projection_semantics.rs::nearest_query_respects_org_isolation` — cross-org rows are invisible to the scan |
| Org scope unrepresentable to omit at the plan layer (VEC-006) | PROVEN | `crates/riffdb-query-executor/tests/nearest_execution.rs::scopeless_nearest_query_is_uncompilable` and `nearest_program_carries_k_and_the_mandatory_partition_parameter`; the engine's `NearestQueryRequest.org_scope` is a mandatory field |
| Mandatory positive K, K is the checked page bound (VEC-006/VEC-010) | PROVEN | `nearest_execution.rs::nearest_program_carries_k_and_the_mandatory_partition_parameter`; K inherits the 499 page-take ceiling in the resolver |
| Row filters apply BEFORE distance ranking at the engine (VEC-007) | PROVEN | `tests/projection_semantics.rs::nearest_query_filters_denied_rows_before_ranking` — the denied-nearest row influences neither presence, distances, ranking, nor count, and a filter-after-rank order swap reds the test |
| Exact KNN determinism (WP-594 ground truth) | PROVEN | `tests/projection_semantics.rs::nearest_query_exact_knn_is_deterministic` |
| Bounded scan cost, honestly charged (ADR-0087) | PROVEN | `nearest_execution.rs` — static cost charges the partition-scan ceiling, the executor charges the adapter's examined-row count against fuel, and over-ceiling scans are typed refusals |
| Declared dimension enforced at query time | PROVEN | `tests/projection_semantics.rs::nearest_query_rejects_query_dimension_mismatch`; stored-cell skew is a typed error, not a panic |

## Pending (no automated proof exists — do not cite as discharged)

| Obligation | Status | What is missing |
|---|---|---|
| Row-level POLICY (principal-based) before ranking (VEC-007) | PENDING | Principal-aware row-policy runtime evaluation does not exist yet for ANY query access path — the compiled row-policy IR (WP-59x concurrent work) has no execution-side consumer. The engine enforces filter-before-rank for whatever predicate set it is handed; binding compiled row policies into that predicate set is open for every access kind, nearest included. |
| Derived / non-authoritative lifecycle, frontier reporting, freshness, snapshot rebuild, replay-budget detach (VEC-005) | PENDING | `crates/riffdb-projection/` contains no vector-aware consumer; `nearest_query_snapshot` is not wired into any `ProjectionController` lifecycle, and no adapter implements `QueryReadView::nearest` against the columnar engine (row stores refuse, fail closed). Inheritance-by-construction is a design intention, not a proof. |
| Per-org index statistics (VEC-008) | PENDING (vacuously satisfied) | The exact tier has no statistics at all; the obligation binds when the approximate tier (WP-594) introduces them. |
| Staleness tracking and SLO health (VEC-003/VEC-004/VEC-012) | PENDING | The declared metric, source fields, and staleness SLO now reach the compiled bundle (`VectorFieldSpecV1`), but nothing computes staleness, counts stale entities, or emits `HealthComponentKind::VectorStaleness`. VEC-003's sequence-versus-duration basis needs a SPEC clarification (flagged for human review). |
| Frontier contract for vector projections (VEC-011) | PENDING | No vector projection participates in frontier/crash-recovery machinery yet; nothing to weaken and nothing proven. |
| Client ingress for embeddings (VEC-002) | PENDING | No wire variant carries a typed vector; the proto surface refuses vector values (typed error) instead of punning them into bytes, and generated clients exclude vector fields until a distinct wire variant is accepted. |

## What this means

The nearest execution path is real at the columnar engine layer and at the
query plan layer, with org scoping, K bounds, predicate-before-ranking, and
honest scan accounting proven by adversarial tests. The projection-plane
integration, principal row-policy binding, staleness surface, and client
ingress are pending and tracked on the WP-593/594/595 ladder entries — they
must not be cited as discharged until a redding test exists for each.
