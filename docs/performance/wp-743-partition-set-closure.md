# WP-743 bounded partition-set closure

WP-743 was exercised at RiffDB revision
`39e208a44e63a860ec9d4d7d512da72cb2ab821b` with a disposable copy of the
current out-of-tree MLflow application. The original adapter worktree was not
modified. Its existing ten experiment queries were retained and one generated
`SearchRuns` query was added with this exact route and page domain:

```riffql
$experiment_ids: Set<Run.experiment_id, 1000>
$limit: Limit<5000>

many runs from Run
    where experiment_id in $experiment_ids
    order by start_time desc, run_id asc
    take $limit after $after
```

The source-only compiler and exact application generation accepted all eleven
queries. `riffdb dev --run` then completed server startup, capability bootstrap,
contract deployment, query-module deployment, `MLflowTrackingServer` role
binding, current Python wheel generation, and authenticated generated-client
startup.

The runner created two experiments, created three Runs across both experiment
partitions through compiled commands, and requested a globally ordered first
page of one row. It resumed with a page size of 100 after reversing the route
order and adding a duplicate route. The continuation returned the other two
rows in the independent `(start_time DESC, run_id ASC)` oracle order. The same
run also completed folded-name search, two-tag intersection, exact tag search,
revision-guarded tag update, and revision-guarded tag deletion.

This exercise found and closed two generic gaps before producing the passing
receipt:

- mixed-direction plans now retain a separately bounded 67,107,840-row
  whole-operation scan domain, preserving the full local scan allowance for up
  to 1,024 declared partitions; and
- a bounded partition-set `Limit` is now recognized as page cardinality rather
  than invariant cursor identity, while the normalized route set remains
  identity-bearing.

No application code performed partition fan-out, filtering, sorting, merge,
counting, cursor walking, hidden-state retention, or a partition-model rewrite.
The checked value-free receipt is
[`release/evidence/partition-set-mlflow-loopback-v1.json`](../../release/evidence/partition-set-mlflow-loopback-v1.json).

Follow-up lifecycle search found that V16 admitted only a route immediately
followed by its order suffix. The correction uses additive query IR/module V17
for a compiler-proved invariant exact prefix, keeps V16 bytes unchanged, and
binds the exact filter outside the compact global marker. Parameter and enum
constant forms compile, the cursor resumes from page size 1 to 3 across two
partitions with deleted rows excluded before merge, and the original
application completes `application lock --write`. The value-free follow-up
receipt is
[`release/evidence/partition-set-exact-prefix-repro-v1.json`](../../release/evidence/partition-set-exact-prefix-repro-v1.json).
