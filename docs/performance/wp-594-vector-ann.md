# WP-594 first-party vector ANN evidence

Status: implementation and bounded acceptance evidence complete.

WP-594 adds an optional atomic vector-field clause:

```riff
vector_field embedding(1536, cosine, (title, body), staleness_slo 60,
    ann_threshold 256, recall_target_bps 9500)
```

Both values are compiler-owned projection properties. The row threshold is
bounded to 1–65,536 per organization and the recall target to 1–10,000 basis
points. Exact search remains active at and below the threshold. Under accepted
ADR-0229, an uncached population also executes exactly above the threshold; a
matching warm graph may use ANN. The result reports the path actually used.
A query cannot lower the declared recall target.

## Isolation and boundedness

Every query repeats current organization, model/evidence, scalar-predicate and
principal-policy admission before cache lookup or distance computation. Reuse
requires the exact provider generation, history/frontier, model, policy bundle,
capability revision, vector field and metric, plus byte-equivalent ordered
admitted keys, entity versions and canonical vectors. A population hash never
substitutes for these comparisons.

The primary provider may offer a cold admitted population to a background
builder. Each provider has at most one active and one queued population, each
bounded to 500 rows. Cache and builder state share a process-wide 64 MiB budget,
reserved before copying retained data. Memory refusal or busy cache state keeps
the exact result. Supersession cancels outdated builds; generation invalidation
drops its cache. Outstanding searches and builders retain their reservations
until they release their data.

Graphs remain memory-only and reopen cold. Follower providers currently use
exact execution. Continuation callers use exact execution on every page; the
current production projected-vector endpoint rejects continuation cursors.
Freshness, current model evidence and policy release checks remain unchanged.

## Reproducible evidence

- `uncached_declared_threshold_queries_remain_exact` proves cold routing.
- `cold_exact_warm_reuse_and_recall_at_the_production_bound` exercises 500
  candidates and all three metrics, proving one build per population, repeated
  admission, exact cold answers and the declared warm recall.
- `denied_malformed_rows_and_other_orgs_never_enter_reused_graphs` checks warm
  policy isolation and refusal to reuse changed versions or vector bytes.
- `paused_build_supersession_drop_and_search_keep_bounded_custody` and
  `invalidated_provider_cancels_paused_builder_without_installation` use explicit
  channel schedules to prove cancellation and memory custody.
- `randomized_histories_meet_declared_recall_at_matched_frontiers` grows eight
  seeded histories and compares recall@20 with exact search at each identical
  published frontier.
- `randomized_histories_meet_declared_recall_against_exact_at_matched_frontiers`
  independently exercises the graph over eight seeded candidate-set growth
  frontiers and twelve probes per frontier.
- `maintenance_rebuild_at_a_frozen_frontier_preserves_recall_and_statistics`
  rebuilds the graph twice from one frozen candidate set and checks identical
  statistics/results plus the declared 9,500-basis-point recall floor.
- `another_organization_cannot_change_results_or_graph_statistics` adds 512
  vectors to another organization and checks that the selected organization's
  statistics, scan work, distances, and identities remain unchanged.
- `denied_rows_cannot_shape_the_ann_graph_or_result` adds 256 policy-denied
  vectors and proves they affect only honest scan charging, not graph node
  count, distances, or result identity.

All randomized cases use checked-in deterministic seeds and fixed loop bounds.

## Compatibility

The optional clause allocates bundle/grammar/executable-IR V12 and a distinct
`VectorAnnSpecV1`. Bundles without ANN declarations retain their prior version,
canonical bytes, and hash. Catalog startup explicitly accepts V12. Commit-side
tests prove the new projection-only metadata does not alter authoritative
index derivation.

## Bounded workload measurement and limitations

The deterministic 500-row, 12-dimensional fixture records distance counts and
latency in `cold_exact_warm_reuse_and_recall_at_the_production_bound` (`--nocapture`).
A development-profile run used 500 distance evaluations for cold exact queries,
roughly 530–590 for warm traversal, and about 212,000–216,000 for graph construction.
Cold query latency was roughly 0.9–1.1 ms and warm latency 1.0–1.2 ms on that run;
these are local observations, not production latency guarantees. The cache
reserved approximately 4.47 MB per retained fixture population, including a
conservative graph/build allowance. The retained cache allocation ledger counted
2,178 owned buffers/Arc allocations; warm hits allocated no new retained cache
state. This ledger excludes transient query/build allocations and allocator
metadata; allocator call profiling was unavailable. It avoids rebuilding on repeated queries;
it does not establish a speed advantage over exact scanning at this small bound.

No persisted graph format or incremental graph maintenance is introduced.
Historical standalone graph recall and compaction tests remain applicable.
