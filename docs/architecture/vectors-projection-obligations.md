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
| Org scope unrepresentable to omit at the plan layer (VEC-006) | PROVEN | `crates/riffdb-query-executor/tests/nearest_execution.rs::scopeless_nearest_query_is_uncompilable` — a single-difference query (predicate field only) asserting the `NonLocal` code and predicate span, so no other rejection cause can satisfy it — plus `nearest_program_carries_k_and_the_mandatory_partition_parameter`; the engine's `NearestQueryRequest.org_scope` is a mandatory field |
| Mandatory positive K, K is the checked page bound (VEC-006/VEC-010) | PROVEN | `nearest_execution.rs::nearest_program_carries_k_and_the_mandatory_partition_parameter`; the 499 ceiling itself is pinned at the boundary by `nearest_k_at_the_499_ceiling_compiles` (k=499 accepted) and `nearest_k_over_the_499_ceiling_is_a_typed_refusal` (k=500 refused with `ArtifactLimit` at the K token; k=0 refused at parse) |
| Row filters apply BEFORE distance ranking at the engine (VEC-007) | PROVEN | `tests/projection_semantics.rs::nearest_query_filters_denied_rows_before_ranking` — the denied-nearest row influences neither presence, distances, ranking, nor count, and a filter-after-rank order swap reds the test |
| Exact KNN determinism (WP-594 ground truth) | PROVEN | `tests/projection_semantics.rs::nearest_query_exact_knn_is_deterministic` |
| Bounded scan cost, honestly charged (ADR-0087) | PROVEN | `nearest_execution.rs::nearest_static_cost_charges_the_partition_scan_ceiling_not_k` (static side); `nearest_scan_work_is_charged_against_shared_fuel` (runtime side — two guard-passing full-ceiling reports exhaust the shared scan fuel as `FuelExhausted`, so deleting the nearest arm's `fuel.scans` charge reds the test); over-ceiling and over-K reports are typed refusals (`nearest_execution_refuses_*`). The columnar engine reports the honest examined-row count as `NearestQueryResult::scanned_rows`, pinned by `tests/projection_semantics.rs::nearest_query_reports_examined_rows_not_returned_rows` |
| Declared dimension enforced at query time | PROVEN | `tests/projection_semantics.rs::nearest_query_rejects_query_dimension_mismatch`; stored-cell skew is a typed error, not a panic |

## Proven prerequisites that do not discharge end-to-end obligations

| Prerequisite | Status | Evidence and boundary |
|---|---|---|
| Typed vector compatibility branch | PROVEN PREREQUISITE | `Value.vector_value = 15` exists, and `public_client_vectors.rs::canonical_client_vector_pins_branch_order_and_positive_zero_bits` plus `checked_in_vector_wire_boundary_recipes_hit_every_strict_trigger` pin canonical bits, packed and unpacked 4,096 acceptance, 4,097 refusal, malformed packed input, duplicate oneof refusal, and additive field-16 compatibility. `grpc_end_to_end.rs::execute_command_raw_vector_preflight_is_exact_before_prost_allocation` proves the generated gRPC server boundary performs that preflight before application dispatch. This is low-level wire reachability, not complete embedding persistence. |
| Hosted MCP and CLI vector conversion | PROVEN PREREQUISITE | `service_backend.rs::dynamic_mcp_vector_reaches_the_shared_service_as_a_typed_vector` proves schema-approved MCP input reaches `SubmittedValue::Vector`; `value.rs::vector_json_round_trips_binary32_bits_and_enforces_closed_boundaries` proves the accepted CLI shape and exact binary32 boundaries. Stable generated application facades still do not expose vector fields. |
| Dedicated health identity and provisional publication | PROVEN PREREQUISITE | `service_backend.rs::authenticated_health_publishes_provisional_vector_staleness_exactly` pins the distinct `vector_staleness` component as `unavailable` and aggregate `degraded`. This proves fail-closed publication of a placeholder, not an observer-backed staleness measurement. |

## Pending (do not cite as discharged)

| Obligation | Status | What is missing |
|---|---|---|
| Row-level POLICY (principal-based) before ranking (VEC-007) | PENDING | Principal-aware row-policy runtime evaluation does not exist yet for ANY query access path — the compiled row-policy IR (WP-59x concurrent work) has no execution-side consumer. The engine enforces filter-before-rank for whatever predicate set it is handed; binding compiled row policies into that predicate set is open for every access kind, nearest included. |
| Derived / non-authoritative lifecycle, frontier reporting, freshness, snapshot rebuild, replay-budget detach (VEC-005) | PENDING | `crates/riffdb-projection/` contains no vector-aware consumer; `nearest_query_snapshot` is not wired into any `ProjectionController` lifecycle, and no adapter implements `QueryReadView::nearest` against the columnar engine (row stores refuse, fail closed). Inheritance-by-construction is a design intention, not a proof. |
| Per-org index statistics (VEC-008) | PENDING (vacuously satisfied) | The exact tier has no statistics at all; the obligation binds when the approximate tier (WP-594) introduces them. |
| Authoritative embedding and model-version production (VEC-002/VEC-003) | PENDING | Low-level typed values and paginated DTOs exist, but authoritative storage does not consume model identity/version, stamp the embedding provenance and source-write-sequence evidence, or produce the staleness rows. No public operation currently enumerates those DTOs. |
| Staleness tracking and observer-backed SLO health (VEC-003/VEC-004/VEC-012) | PENDING | The v1 semantic question is settled: the declared positive count threshold breaches exactly when `stale_count > threshold`; equality does not breach, and duration/clock semantics require a future amendment. Nothing authoritative computes the stale count or drives `HealthComponentKind::VectorStaleness`; the current `Unavailable` component is deliberately provisional and must not be cited as observer proof. |
| Frontier contract for vector projections (VEC-011) | PENDING | No vector projection participates in frontier/crash-recovery machinery yet; nothing to weaken and nothing proven. |
| Complete application ingress for embeddings (VEC-002) | PENDING | The native value, Protobuf field 15, generated gRPC boundary, hosted MCP conversion, and CLI conversion are reachable and bounded. Stable generated Rust, Go, and Python application facades still exclude vector fields, and no end-to-end path persists the required authoritative embedding/model-version evidence. Low-level transport staging is not complete ingress. |
| Production nearest-query reachability | PENDING | The columnar exact engine exists, but no production vector projection/storage adapter connects it to the shared query path. The production nearest path and its projection lifecycle therefore remain fail-closed. |

## What this means

The nearest execution path is real at the columnar engine and query-plan layers,
with org scoping, K bounds, predicate-before-ranking, and honest scan accounting
proven by adversarial tests. Vector values also have real low-level native,
Protobuf/gRPC, hosted MCP, and CLI compatibility paths, and health has a distinct
fail-closed `vector_staleness` identity.

Those prerequisites do not complete WP-596's production obligations. The
authoritative embedding/model-version producer, observer-backed staleness
measurement, stable generated application facades, vector projection lifecycle,
production nearest adapter, and principal row-policy binding remain pending.
The current `vector_staleness: unavailable` report is evidence that the missing
observer is disclosed, not evidence that it exists.
