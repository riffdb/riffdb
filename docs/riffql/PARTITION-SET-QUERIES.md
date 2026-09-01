# Bounded partition-set queries

RiffQL can execute one compiler-sealed operational query across an explicit,
finite set of aggregate partitions while preserving database-owned ordering,
authorization, snapshots, and cursor semantics. Use the additive bounded set
form on the entity's declared partition field:

```riffql
query SearchRuns(
    $experiment_ids: Set<Run.experiment_id, 1000>,
    $limit: Limit<5000> = 1000,
    $after: Cursor?,
) {
    many runs from Run
        where experiment_id in $experiment_ids
        order by start_time desc, run_id desc
        take $limit after $after
    return Found { runs: runs { experiment_id run_id start_time } }
    outcomes Found
}
```

`MAX` is a canonical integer from 1 through 65,535. Generated Rust, Go,
TypeScript, Python, gRPC, CLI, and MCP inputs expose one bounded list. Submitted
values are type-checked, sorted by canonical encoding, and deduplicated before
authorization or execution. An empty list is valid and returns the declared
empty result without storage or provider work.

The compiler accepts only `partition_field in $bounded_set` as the route for
every access in the query. It selects one partition-prefixed declared index,
replicates the same immutable local plan, proves one global total order, and
multiplies authority and work from the declared maximum—not the submitted
count. Values discovered by another binding, an index, a provider, or a
relationship cannot become routes. Cross-partition joins, writes, aggregates,
caller-selected plans, and partial per-partition results remain unavailable.

Invariant exact filters may occupy index components between the partition and
order suffix. For example, MLflow lifecycle filtering uses
`(experiment_id, lifecycle_stage, start_time, run_id)` with
`experiment_id in $experiment_ids`, `lifecycle_stage == $lifecycle_stage`, and
`order by start_time desc, run_id asc`. RiffDB applies lifecycle membership
inside every partition scan before the global merge. Enum constants such as
`LifecycleStage.Active` have the same semantics. The filter value is bound into
the plan or cursor parameter identity; the adapter never fetches and discards
the opposite lifecycle.

Execution opens one storage read view for the complete operation. Uniform
index orders use a bounded k-way heap: one initial row is observed from each
selected partition, then only the winning stream is advanced. Mixed-direction
orders use the compiler-bounded complete-group path. Candidate indexes and
long-pattern providers complete independently in every selected partition;
candidate identity is the pair `(partition, key)` before root hydration and
global ordering. Row policy is applied below each stream and no denial releases
partition counts or partial output.

One opaque continuation represents the global page. It binds the normalized
route set, every invariant filter, the selected plan and role, the last global
order/key/partition marker, and a domain-separated digest of all observed
partition or provider epochs. Page size may change on resume within its
declared `Limit<MAX>` domain. Route changes, epoch drift, policy drift,
tampering, expiry, or observation loss fail closed as a typed cursor error;
the cursor never exposes one token or epoch per partition.

The structural page ceiling remains 65,534 rows, not the predecessor 499-row
limit. Query-specific result-byte, route-byte, partition, scan, policy,
candidate, provider, merge, and response ceilings remain independent. Uniform
heap execution therefore supports large pages without work proportional to
`partitions × page size`. Mixed orders use a separate 67,107,840-physical-row
whole-operation ceiling, enough to retain the full 65,535-row local scan domain
through 1,024 declared partitions; larger declared route sets receive a
proportionally smaller per-partition allowance.

This surface uses RiffQL V13. A route followed directly by its order suffix uses
query IR/module V16; a route followed by invariant exact index-prefix predicates
uses additive query IR/module V17. Existing V16 and scalar-route queries and
their modules, locks, roles, generated bindings, and cursors retain their prior
exact bytes. Adopting either bounded partition-set shape requires normal query
recompilation, module deployment, role binding, and binding regeneration;
authoritative stored data does not migrate.
