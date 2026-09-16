# WP-595 Vector Acceptance Closure

WP-595 closes the native vector pillar by collecting the independently landed
WP-594 approximate-search evidence and WP-596 production-lifecycle evidence
under one adversarial acceptance ledger. It adds no weaker alternate path.

## Requirement evidence

| Requirement | Semantic evidence |
|---|---|
| VEC-005 derived lifecycle and replay budget | `production_vector_snapshot_generation_becomes_ready_only_after_checkpoint`, `detached_and_rebuilding_vector_controls_resume_without_frontier_overclaim`, and `persisted_vector_rebuild_resumes_after_storage_reopen_without_overclaim` prove private build, detach, restart, and no frontier overclaim. |
| VEC-007 policy before ranking | `denied_rows_cannot_shape_the_ann_graph_or_result`, `principal_admission_runs_before_candidate_validation_and_ranking`, and `projected_vector_release_revalidates_authority_after_ranked_provider_work` prove denial before graph work and a second authority safe point before release. |
| VEC-008 statistics isolation | `another_organization_cannot_change_results_or_graph_statistics` and `denied_rows_cannot_shape_the_ann_graph_or_result` prove neither another organization nor hidden rows alter entry/topology statistics, scores, or identities. |
| VEC-010 recall contract | `randomized_histories_meet_declared_recall_at_matched_frontiers`, `randomized_histories_meet_declared_recall_against_exact_at_matched_frontiers`, and `maintenance_rebuild_at_a_frozen_frontier_preserves_recall_and_statistics` compare the declared approximate tier with exact search at identical frontiers. |
| VEC-011 frontier and freshness | `bounded_vector_freshness_uses_elapsed_time_across_second_boundaries`, the rebuild/reopen tests above, and `remote_generated_vector_client_writes_inspects_and_queries_exact_projection` prove typed freshness and a generated causal read fenced to its embedding commit. |

The production embedding tests additionally prove exact dimension validation,
contract-current model enforcement, atomic evidence persistence, stale/outdated
inspection from one snapshot, and observer-backed health. The real-process test
uses the public generated Rust client against `riffdbd`; low-level storage IDs,
raw keys, and caller-selected ANN settings are absent.

## Adversarial matrix

- Randomized histories are deterministic and bounded, and compare recall at
  each matched frontier rather than comparing results from different states.
- Interleaved source and embedding writes are classified from authoritative
  write-sequence evidence; stale rows and outdated model versions remain
  separately inspectable.
- Policy-denied candidates are excluded before vector validation, graph build,
  distance, rank, result count, and K fill, while still consuming honest scan
  budget.
- Replay-budget breach detaches retention before rebuilding; only a complete
  checkpoint can publish a successor generation as `Ready`.
- Restart and corruption paths fail closed and never claim a frontier beyond a
  durable checkpoint.

## Public boundary

The application declares vector properties in `.riff`, reads through named
`.riffql`, and uses generated commands and inspection methods. It cannot submit
an index choice, threshold, recall target, raw query AST, storage identifier, or
freshness bypass. Public failures name the corrective contract/query action;
internal graph and storage details remain redacted.

ADR-0229 replaces synchronous per-query construction with exact cold execution
and bounded background reuse over an exactly matched admitted population.
Current admission and frontier checks still run on every query. Graphs remain
memory-only; durable incremental graph storage remains deferred and requires
a separate accepted design with the same recall/isolation gates.
