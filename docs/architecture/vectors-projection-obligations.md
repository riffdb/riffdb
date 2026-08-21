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
| Bounded scan cost, honestly charged (ADR-0087) | PROVEN | `nearest_execution.rs::nearest_static_cost_charges_the_partition_scan_ceiling_not_k` pins the static 500-row charge; `nearest_execution_charges_reported_scan_work_and_succeeds_within_budget` proves runtime work consumes that fuel; over-ceiling and over-K reports are typed refusals (`nearest_execution_refuses_*`). `architecture::production_vector_adapter_admits_current_authoritative_rows_before_ranking` pins the same 500-row evidence/admission ceiling on the production adapter. The columnar engine reports examined rows rather than returned rows in `projection_semantics.rs::nearest_query_reports_examined_rows_not_returned_rows`. |
| Declared dimension enforced at query time | PROVEN | `tests/projection_semantics.rs::nearest_query_rejects_query_dimension_mismatch`; stored-cell skew is a typed error, not a panic |

## Proven prerequisites that do not discharge end-to-end obligations

| Prerequisite | Status | Evidence and boundary |
|---|---|---|
| Typed vector compatibility branch | PROVEN PREREQUISITE | `Value.vector_value = 15` exists, and `public_client_vectors.rs::canonical_client_vector_pins_branch_order_and_positive_zero_bits` plus `checked_in_vector_wire_boundary_recipes_hit_every_strict_trigger` pin canonical bits, packed and unpacked 4,096 acceptance, 4,097 refusal, malformed packed input, duplicate oneof refusal, and additive field-16 compatibility. `grpc_end_to_end.rs::execute_command_raw_vector_preflight_is_exact_before_prost_allocation` proves the generated gRPC server boundary performs that preflight before application dispatch. This is low-level wire reachability, not complete embedding persistence. |
| Hosted MCP, CLI, and generated-facade vector conversion | PROVEN PREREQUISITE | `service_backend.rs::dynamic_mcp_vector_reaches_the_shared_service_as_a_typed_vector` proves schema-approved MCP input reaches `SubmittedValue::Vector`; `value.rs::vector_json_round_trips_binary32_bits_and_enforces_closed_boundaries` proves the accepted CLI shape and exact binary32 boundaries; and `vector_generation.rs::generators_emit_typed_vector_entity_and_command_models` proves Rust, Go, TypeScript, and Python emit their canonical vector type plus contract-sealed model constructors. Runtime tests pin exact driver bits, dimensions, and non-finite refusal. This is generated assembly proof, not the remote WP-596 exit demonstration. |
| Dedicated health identity and provisional publication | PROVEN PREREQUISITE | `service_backend.rs::authenticated_health_publishes_provisional_vector_staleness_exactly` pins the distinct `vector_staleness` component as `unavailable` and aggregate `degraded`. This proves fail-closed publication of a placeholder, not an observer-backed staleness measurement. |
| Compiler-owned principal row-policy runtime | PROVEN PREREQUISITE | ADR-0111/ADR-0114 are implemented by the closed `AuthorizedQueryRowPolicyContextV1` path, including compiler-selected policy authority, principal facts, and index-derived relationship evidence. `QueryReadView` receives that context for point, batch, scan, and nearest access. This proves a shared typed authority handoff; it does not prove a production columnar-nearest adapter or settle policy evidence at a projection frontier. |
| Principal admission before vector validation and ranking in the exact engine | PROVEN PREREQUISITE | `tests/projection_semantics.rs::principal_admission_runs_before_candidate_validation_and_ranking` proves denied malformed/nearest rows cannot influence vector validation, score, rank, count, or K fill; denied rows still consume the bounded scan; an admission failure aborts without partial output and exposes a redacted public message. `NearestCandidateAdmission` is a narrow infrastructure port for the compiler-owned evaluator, not application-selectable authority. No production path composes the two yet. |
| Generic columnar lifecycle, frontier, checkpoint, and replay mechanics | PROVEN PREREQUISITE | `ColumnarRuntime` and the columnar worker consume authoritative commits, publish snapshots/frontiers, checkpoint, reopen, and replay under the generic ADR-0086 machinery. The server exposes this separately through `ColumnarProjectionPort`. This proves the reusable projection substrate, not symbolic nearest routing, freshness selection, or replay-budget detachment. |

## Pending (do not cite as discharged)

| Obligation | Status | What is missing |
|---|---|---|
| Production row-level policy before nearest ranking (VEC-007) | PROVEN | The production `VectorProjectionPort` reads one bounded authoritative evidence/policy snapshot, constructs current-model admission, and calls only `nearest_query_snapshot_with_admission`. `riffdb-server/tests/architecture.rs::production_vector_adapter_admits_current_authoritative_rows_before_ranking` reds if evidence/policy moves after ranking or an admission-free evaluator appears. `riffdb-service/tests/architecture.rs::projected_vector_release_revalidates_authority_after_ranked_provider_work` pins the second authorization safe point before release. Engine-level adversarial effects remain pinned by `projection_semantics.rs::principal_admission_runs_before_candidate_validation_and_ranking`. |
| Derived/non-authoritative lifecycle, source selection, freshness, and replay-budget detach for nearest (VEC-005) | IN PROGRESS | RiffQL V5/query IR V8/module V8 now bind one exact `Entity.field` source and `Available`, causal, or duration-bounded freshness. The server automatically registers that source (`production_vector_contract_registers_exact_symbolic_projection_automatically`), causal reads use register-before-observe waits, and bounded freshness uses trusted commit timestamps (`bounded_vector_freshness_uses_elapsed_time_across_second_boundaries`). Declared replay age/bytes/backlog detachment and crash/restart evidence are still pending, so this combined obligation is not discharged. |
| Authoritative embedding and model-version production (VEC-002/VEC-003) | PARTIAL — ingress/evidence/facades landed | Contract IR V15 carries a compiler-sealed `SetEmbedding`; service validation rejects wrong model identity/version at exact symbolic input paths; runtime emits one bounded evidence intent; commit re-derives transaction-current evidence transitions; memory/redb persist entity state plus evidence atomically; and all four generated languages expose canonical vectors with declared-model constructors. The public staleness/model enumeration operation remains missing, so the obligation is not discharged end to end. |
| Staleness tracking and observer-backed SLO health (VEC-003/VEC-004/VEC-012) | PROVEN IN REPOSITORY; REMOTE EXIT EVIDENCE PENDING | Redb maintains the authoritative evidence row, ordered inspection index, per-partition observation, and global threshold observation atomically. `vector_inspection_reads_counts_evidence_and_frontier_from_one_snapshot` proves one exact read snapshot; startup fixtures prove registry installation/backfill before publication; `vector_health_is_present_and_fails_closed_by_observer_state` proves healthy/degraded/unavailable behavior without a probe scan. gRPC conversion and generated-client tests pin the symbolic public shape. The WP-596 remote generated-client demonstration remains outstanding. |
| Frontier contract for production vector nearest (VEC-011) | IN PROGRESS | Production results now bind the selected published frontier, and RiffQL declares `Causal`, duration-`Bounded`, or `Available`. Generic lifecycle tests prove register-before-observe causal waiting, timeout, revocation, and bounded behavior; vector-specific source registration and elapsed-time arithmetic are pinned. A live generated vector query still needs to exercise causal wait, lifecycle refusal, and restart/replay before this row is discharged. |
| Complete application ingress for embeddings (VEC-002) | PARTIAL — generated path assembled | The native value, Protobuf field 15, generated gRPC boundary, hosted MCP conversion, CLI conversion, compiler-sealed `embed` effect, atomic authoritative evidence persistence, shared driver value registry, and generated Rust/Go/TypeScript/Python vector models are reachable and bounded. A remote generated-client write demonstration under the WP-596 exit gate is still required before this row becomes proven. |
| Production nearest-query reachability | IN PROGRESS | The API-neutral service and server now join one compiled V8 source to the exact columnar engine, authoritative evidence/current-model admission, scalar filters, K, freshness, and pre-release policy revalidation. Source-less retained plans fail with `RDB-PROJECTION-0104`; mixed bindings and mismatched sources fail at compile time. The remaining proof is a live generated-client query over committed embeddings plus crash/replay/lifecycle acceptance; until that lands this row is not credited as the WP exit gate. |

## Accepted resolution implementation status

[ADR-0136](../../adr/0136-authoritative-vector-evidence-and-projected-nearest.md)
now defines one coherent resolution for all six decisions below. Its exact text
was accepted on 2026-08-21. This ledger continues to classify every affected
row as pending or in progress until WP-596 supplies the named production
evidence. Implemented pieces remain fail closed when their exact source,
freshness, evidence, policy, model, or lifecycle proof is unavailable.

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

The accepted resolution uses an authoritative per-vector-field evidence record
rather than modifying frozen entity V1 bytes; derives one explicit
`Entity.field` projection identity; requires contract-current model and replay
budgets; applies transaction-current ADR-0111 policy admission to the complete
projection candidate set before all vector work; and exposes symbolic bounded
inspection rather than numeric field IDs. These statements describe the
accepted design, not yet available behavior.

## What this means

The nearest execution path is real at the columnar engine and query-plan layers,
with org scoping, literal and parameter K bounds, scalar predicates and a
principal-admission seam before ranking, deterministic exact KNN, and honest
scan accounting proven adversarially. Compiler-owned row-policy evaluation and
generic columnar frontier/checkpoint/replay machinery also exist as reusable
prerequisites. Vector values have bounded low-level native, Protobuf/gRPC,
hosted MCP, and CLI compatibility paths, and health has a distinct fail-closed
`vector_staleness` identity.

WP-596 now has the projected-source/freshness contract, authoritative
embedding/model evidence, observer-backed staleness, symbolic inspection, and
the exact production nearest adapter. Missing or invalid observer state still
reports `vector_staleness: unavailable`, and source-less retained plans fail
with `RDB-PROJECTION-0104`. Replay-budget detach/rebuild crash evidence and the
remote generated-client write/inspect/nearest demonstration remain the two
honest completion gaps; neither is inferred from lower-layer unit coverage.
