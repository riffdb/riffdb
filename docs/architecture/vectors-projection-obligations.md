# Vector Projection — ADR-0086 Obligation Map

**Work package:** WP-593 baseline, updated through WP-594 and WP-596
**Status:** honest proven-vs-pending ledger. The original WP-593 revision
claimed every unamended ADR-0086 obligation was discharged; an independent
audit classified 0 of its 13 rows as proven. This ledger states exactly what
automated tests prove now and what remains pending. A row is **PROVEN** only
when the named test fails if the property is broken. A proven prerequisite is
not production reachability and must not be credited as completing WP-596.

## Proven today

| Obligation | Status | Evidence (test that reds if broken) |
|---|---|---|
| Org-partitioned execution: one org scope per nearest query (VEC-005/VEC-009) | PROVEN | `tests/projection_semantics.rs::nearest_query_respects_org_isolation` — cross-org rows are invisible to the scan |
| Org scope unrepresentable to omit at the plan layer (VEC-006) | PROVEN | `crates/riffdb-query-executor/tests/nearest_execution.rs::scopeless_nearest_query_is_uncompilable` — a single-difference query (predicate field only) asserting the `NonLocal` code and predicate span, so no other rejection cause can satisfy it — plus `nearest_program_carries_k_and_the_mandatory_partition_parameter`; the engine's `NearestQueryRequest.org_scope` is a mandatory field |
| Mandatory positive K, with literal and parameter bounds sharing the checked page maximum (VEC-006/VEC-010) | PROVEN | `nearest_execution.rs::parameterized_nearest_k_499_compiles_binds_and_executes` proves `$k: Limit` retains the runtime parameter while the access plan carries the compiler-proven maximum, and that 499 reaches the backend. `parameterized_nearest_k_zero_and_500_are_refused_before_backend_work` proves 0/500 are typed bind refusals before backend work. Literal boundaries remain pinned by `nearest_k_at_the_499_ceiling_compiles`, `nearest_k_over_the_499_ceiling_is_a_typed_refusal`, and `nearest_k_zero_is_rejected_at_parse`. |
| Scalar row filters apply before distance ranking at the exact engine (VEC-007 prerequisite) | PROVEN | `tests/projection_semantics.rs::nearest_query_filters_denied_rows_before_ranking` — the denied-nearest row influences neither presence, distances, ranking, nor count, and a filter-after-rank order swap reds the test |
| Exact KNN determinism (WP-594 ground truth) | PROVEN | `tests/projection_semantics.rs::nearest_query_exact_knn_is_deterministic` |
| Declared exact/ANN threshold routing (VEC-009) | PROVEN | `crates/riffdb-columnar/tests/ann.rs::declared_threshold_routes_exact_at_or_below_and_ann_only_above` pins exact search at and below the compiler-owned per-org threshold, ANN strictly above it, and predicate reduction back to exact |
| Declared recall at matched frontiers (VEC-010) | PROVEN | `crates/riffdb-columnar/tests/ann.rs::randomized_histories_meet_declared_recall_at_matched_frontiers` and `hnsw::tests::randomized_histories_meet_declared_recall_against_exact_at_matched_frontiers` compare ANN recall@K with the exact reference over bounded seeded histories at identical visible frontiers |
| Per-org ANN statistics and policy isolation (VEC-007/VEC-008) | PROVEN | `another_organization_cannot_change_results_or_graph_statistics` proves 512 rows in another org change neither graph stats, work, distances, nor results; `denied_rows_cannot_shape_the_ann_graph_or_result` proves 256 denied rows are excluded before graph construction while still consuming scan work |
| Frozen-frontier ANN maintenance (VEC-010) | PROVEN | `hnsw::tests::maintenance_rebuild_at_a_frozen_frontier_preserves_recall_and_statistics` rebuilds the ephemeral graph from one unchanged logical snapshot and checks deterministic statistics/results plus the declared recall floor |
| Bounded scan cost, honestly charged (ADR-0087) | PROVEN | `nearest_execution.rs::nearest_static_cost_charges_the_partition_scan_ceiling_not_k` (static side); `nearest_scan_work_is_charged_against_shared_fuel` (runtime side — two guard-passing full-ceiling reports exhaust the shared scan fuel as `FuelExhausted`, so deleting the nearest arm's `fuel.scans` charge reds the test); over-ceiling and over-K reports are typed refusals (`nearest_execution_refuses_*`). The columnar engine reports the honest examined-row count as `NearestQueryResult::scanned_rows`, pinned by `tests/projection_semantics.rs::nearest_query_reports_examined_rows_not_returned_rows`. |
| Declared dimension enforced at query time | PROVEN | `tests/projection_semantics.rs::nearest_query_rejects_query_dimension_mismatch`; stored-cell skew is a typed error, not a panic |

## Proven prerequisites that do not discharge end-to-end obligations

| Prerequisite | Status | Evidence and boundary |
|---|---|---|
| Typed vector compatibility branch | PROVEN PREREQUISITE | `Value.vector_value = 15` exists, and `public_client_vectors.rs::canonical_client_vector_pins_branch_order_and_positive_zero_bits` plus `checked_in_vector_wire_boundary_recipes_hit_every_strict_trigger` pin canonical bits, packed and unpacked 4,096 acceptance, 4,097 refusal, malformed packed input, duplicate oneof refusal, and additive field-16 compatibility. `grpc_end_to_end.rs::execute_command_raw_vector_preflight_is_exact_before_prost_allocation` proves the generated gRPC server boundary performs that preflight before application dispatch. This is low-level wire reachability, not complete embedding persistence. |
| Hosted MCP and CLI vector conversion | PROVEN PREREQUISITE | `service_backend.rs::dynamic_mcp_vector_reaches_the_shared_service_as_a_typed_vector` proves schema-approved MCP input reaches `SubmittedValue::Vector`; `value.rs::vector_json_round_trips_binary32_bits_and_enforces_closed_boundaries` proves the accepted CLI shape and exact binary32 boundaries. Stable generated application facades still do not expose vector fields. |
| Dedicated health identity and provisional publication | PROVEN PREREQUISITE | `service_backend.rs::authenticated_health_publishes_provisional_vector_staleness_exactly` pins the distinct `vector_staleness` component as `unavailable` and aggregate `degraded`. This proves fail-closed publication of a placeholder, not an observer-backed staleness measurement. |
| Compiler-owned principal row-policy runtime | PROVEN PREREQUISITE | ADR-0111/ADR-0114 are implemented by the closed `AuthorizedQueryRowPolicyContextV1` path, including compiler-selected policy authority, principal facts, and index-derived relationship evidence. `QueryReadView` receives that context for point, batch, scan, and nearest access. This proves a shared typed authority handoff; it does not prove a production columnar-nearest adapter or settle policy evidence at a projection frontier. |
| Principal admission before vector validation and ranking in the exact engine | PROVEN PREREQUISITE | `tests/projection_semantics.rs::principal_admission_runs_before_candidate_validation_and_ranking` proves denied malformed/nearest rows cannot influence vector validation, score, rank, count, or K fill; denied rows still consume the bounded scan; an admission failure aborts without partial output and exposes a redacted public message. `NearestCandidateAdmission` is a narrow infrastructure port for the compiler-owned evaluator, not application-selectable authority. No production path composes the two yet. |
| Generic columnar lifecycle, frontier, checkpoint, and replay mechanics | PROVEN PREREQUISITE | `ColumnarRuntime` and the columnar worker consume authoritative commits, publish snapshots/frontiers, checkpoint, reopen, and replay under the generic ADR-0086 machinery. The server exposes this separately through `ColumnarProjectionPort`. This proves the reusable projection substrate, not symbolic nearest routing, freshness selection, or replay-budget detachment. |

## Pending (do not cite as discharged)

| Obligation | Status | What is missing |
|---|---|---|
| Production row-level policy before nearest ranking (VEC-007) | PENDING | The compiled evaluator and exact-engine admission port now exist, replacing the earlier claim that no runtime evaluator existed. Production composition is still absent: storage-backed `MemoryQueryView` and `RedbQueryView` refuse nearest fail-closed, while the columnar runtime is not a `QueryReadView` nearest source. A lawful adapter also requires accepted temporal semantics for relationship evidence and capability narrowing at the projection frontier; post-rank reauthorization is forbidden. |
| Derived/non-authoritative lifecycle, source selection, freshness, and replay-budget detach for nearest (VEC-005) | PENDING | The generic columnar projection lifecycle exists, but symbolic nearest IR carries no projected source identity, projection name, freshness policy, typed projection frontier, or mixed-source rule. The process graph therefore cannot select a columnar snapshot lawfully and continues routing the shared query executor only to fail-closed row stores. No accepted configuration or implementation defines replay age/bytes/backlog limits, detach persistence, restart behavior, or retention interaction. |
| Authoritative embedding and model-version production (VEC-002/VEC-003) | PENDING | Low-level typed values and paginated DTOs exist, but the frozen authoritative entity record has no accepted model identity/version, embedding-write sequence, or source-field write evidence. No command atomically persists that evidence with provenance/idempotency, and no reachable service operation produces the staleness/model rows. A side table or durable-record successor requires explicit acceptance; neither may be invented here. |
| Staleness tracking and observer-backed SLO health (VEC-003/VEC-004/VEC-012) | PENDING | The declared positive count threshold breaches exactly when `stale_count > threshold`; equality does not breach, and duration/clock semantics require a future amendment. Nothing authoritative computes the stale count or drives `HealthComponentKind::VectorStaleness`. The contract also has no accepted current-model identity/version authority, so old-model classification cannot be implemented safely. The current `Unavailable` component is deliberately provisional and must not be cited as observer proof. |
| Frontier contract for production vector nearest (VEC-011) | PENDING | Generic columnar snapshots distinguish visible/durable frontiers and have crash/replay coverage, but symbolic nearest cannot name that projection or request `Causal`/`Bounded`/`Available` freshness. No production result returns the selected projection frontier or lifecycle outcome, so the end-to-end vector frontier contract is not proven. |
| Complete application ingress for embeddings (VEC-002) | PENDING | The native value, Protobuf field 15, generated gRPC boundary, hosted MCP conversion, and CLI conversion are reachable and bounded. Stable generated Rust, Go, and Python application facades still exclude vector fields, and no end-to-end path persists the required authoritative embedding/model-version evidence. Low-level transport staging is not complete ingress. |
| Production nearest-query reachability | PENDING | The exact columnar engine and compiler-owned policy context exist, but no production adapter joins them. Adding an implicit projection lookup would be unsafe when zero, one, or multiple projections contain the field and would silently mix authoritative and projected snapshots in a multi-step program. Redb/memory refusal remains the correct behavior until a versioned projected-source/freshness contract is accepted. |

## Decisions required before further WP-596 implementation

1. **Projected nearest query contract:** versioned projected-source identity,
   freshness (`Causal`/`Bounded`/`Available`), frontier/lifecycle result shape,
   metric ownership, and mixed authoritative/projected program rules, including
   query-IR compatibility and gRPC/MCP/SDK parity.
2. **Authoritative embedding durable model:** model identity/version,
   embedding-write commit sequence, source-field last-write evidence, atomic
   command/provenance/idempotency behavior, backup, migration, startup
   validation, and compatibility without mutating frozen V1 records.
3. **Current-model authority:** where the contract-current model identity and
   version live, how they evolve across contract versions, and how existing
   rows are classified.
4. **Projected policy temporal semantics:** whether relationship evidence is
   projection-frontier-matched or transaction-current, plus pre-release
   revocation/narrowing behavior that never becomes post-rank filtering.
5. **Replay-budget contract:** exact age/byte/backlog ownership and bounds,
   detach trigger, durable/restart lifecycle, retention-watermark interaction,
   rebuild source/tail fence, and typed outcome mapping.
6. **Staleness/model public operation:** after the durable and current-model
   decisions, the API-neutral paginated operations and parity-preserving
   gRPC/MCP schemas.

## What this means

The nearest execution path is real at the columnar engine and query-plan layers,
with org scoping, literal and parameter K bounds, scalar predicates and a
principal-admission seam before ranking, deterministic exact KNN, and honest
scan accounting proven adversarially. Compiler-owned row-policy evaluation and
generic columnar frontier/checkpoint/replay machinery also exist as reusable
prerequisites. Vector values have bounded low-level native, Protobuf/gRPC,
hosted MCP, and CLI compatibility paths, and health has a distinct fail-closed
`vector_staleness` identity.

Those prerequisites do not complete WP-596's production obligations. The
projected-source/freshness contract, authoritative embedding/model evidence,
current-model declaration, observer-backed staleness, replay-budget detach,
stable generated application facades, production nearest adapter, and accepted
projection-frontier policy semantics remain pending. The current
`vector_staleness: unavailable` report and row-store nearest refusals are
correct disclosures of missing production composition, not evidence that the
obligations are discharged.
