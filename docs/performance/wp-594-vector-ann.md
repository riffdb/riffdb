# WP-594 first-party vector ANN evidence

Status: implementation and bounded acceptance evidence complete.

WP-594 adds an optional atomic vector-field clause:

```riff
vector_field embedding(1536, cosine, (title, body), staleness_slo 60,
    ann_threshold 256, recall_target_bps 9500)
```

Both values are compiler-owned projection properties. The row threshold is
bounded to 1–65,536 per organization and the recall target to 1–10,000 basis
points. Exact search remains active at and below the threshold; HNSW engages
strictly above it. A query cannot select the tier or lower the declared target.

## Isolation and boundedness

The first-party Rust graph uses no external ANN dependency or unsafe code. It
is built ephemerally from the query's admitted vectors after organization
scope, scalar predicates, and principal-policy admission. Consequently its
entry point, levels, neighbor links, traversal, and safe execution statistics
are functions only of rows eligible for that query. Graph construction is
bounded by the existing per-query scan budget; levels, neighbor count,
construction breadth, and search breadth have fixed implementation ceilings.

The ephemeral design deliberately stores no durable graph. A query rebuilds
from the visible merged snapshot in canonical primary-key order. Segment
compaction therefore cannot preserve or corrupt graph bytes; maintenance is
accepted by recall at an unchanged frontier, as VEC-010 requires.

## Reproducible evidence

- `declared_threshold_routes_exact_at_or_below_and_ann_only_above` checks both
  sides of the threshold and proves pre-ranking filters can route a reduced
  candidate set back to exact search.
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

## POC limitation

The graph is rebuilt per query rather than retained as a durable derived
structure. This gives the POC a small, auditable tenant-isolation boundary and
strong compaction invariance, but it does not claim production-scale latency
for very large partitions. Persisted incremental graph maintenance would be a
separate design requiring the same per-org isolation and frozen-frontier recall
gate; it must not silently replace this behavior.
