# ADR-0086: Columnar Projection, Read Sources, and Freshness Policies

- **Status:** Accepted
- **Date:** 2026-08-01
- **Decision owners:** RiffDB maintainers
- **Related:** ADR-0010 (frontiers), ADR-0070 (read stability), ADR-0080 (partitioned events, durable consumers), ADR-0082 (single total order), ADR-0085 (retention), ADR-0087 (projected ad-hoc query surface)

## Context

Measured on real disk (board-scale harness, 2026-08-01): RiffDB's per-row read
cost is 8,741 ns/row vs PostgreSQL's 837 ns (10.4×); a 450-row board page costs
4.06 ms with ~96% of latency in the per-row pipeline. Arbitrary filter
combinations do not map to compiled named queries with declared access paths,
and analytical aggregation has no story. A columnar projection — derived,
fed from the single total order — addresses all three. At moderate
per-organization scale, a column scan provides a universal *fallback* access
path and avoids requiring a composite index for every filter combination; it
is not promised to solve every scale tier, and the architecture leaves room
for dictionary encoding, zone maps, bitmap and sorted-segment indexes, top-K
lane structures, text indexes, and vectorized execution without selecting any
of them here.

## Decision

### 1. Projected state is derived and non-authoritative — with a qualified rebuildability claim

Only projections whose **complete source closure is recoverable** from
backup-authoritative state plus retained authoritative history qualify as
rebuildable accelerators. Projections are classified at deploy time:

- **Snapshot-rebuildable** — reconstructible from authoritative current state
  (e.g. a current ticket board). Segments are excluded from backup identity.
- **History-rebuildable** — additionally require retained authoritative
  events/records (e.g. daily status-transition rollups). Segments may be
  excluded from backup only while the required source history is itself
  backup-authoritative or retained under an explicit source-retention policy
  (ADR-0085 interaction below).
- **Non-rebuildable derived state** — disallowed, unless explicitly promoted
  into backup identity or bound to a declared external authoritative source.

Projection definitions and catalog metadata are always part of backed-up
contract state; only accelerator segments are exempt, and only under the
closure rule above.

### 2. Queries declare a read source

- **Authoritative** (existing path; semantics unchanged), or
- **Projected(projection_name)**.

### 3. Projected queries declare a freshness policy

- **Causal(commit_token, max_wait)** — serve only once the projection
  contains at least the supplied commit token; typed lagging outcome on
  `max_wait` expiry. Example: `freshness causal { inherit_session_commit true; max_wait 500ms }`.
- **Bounded(max_lag)** — serve only if head-to-frontier lag is within the
  declared duration.
- **Available** — serve current projection state, reporting its frontier,
  with no staleness guarantee.

Every projected response reports the projection frontier. **Commit tokens and
frontiers are opaque, scoped types** (`CommitToken`, `ProjectionFrontier`) —
today internally one sequence, later possibly database/partition identity,
contract lineage, placement epoch, or vector positions — never a public bare
`u64`. SDK propagation of the causal token is automatic, inspectable, and
overrideable (`read_context.last_commit`, `with_read_context(..)`,
`without_causal_fence()`); "session" is defined as the read context object,
not a connection, to stay meaningful across pools, stateless servers, tabs,
and agent invocations.

### 4. A frontier is an atomic visibility guarantee

If a projection reports frontier F, every projection effect of every relevant
commit at or before F is completely visible, and no reader can observe a
partially applied commit; the frontier advances only after all changes for a
commit are published atomically to a stable, queryable projection snapshot.
Compaction preserves logical query results and never moves the frontier
backward. The **visible frontier** (may lead, e.g. in-memory delta) and the
**durable/recoverable frontier** (checkpointed) are distinct: retention
decisions use the durable frontier; after restart the projection may regress
to it and replay. Crash invariants: the durable frontier never overclaims
recoverable state; applying a commit twice is idempotent; a crash before
checkpoint advancement causes replay; a crash after it cannot lose the
associated projection changes.

### 5. Authorization is enforced twice

The projection definition determines which data classifications and columns
are **eligible to enter the derived plane** — the maximum exposure envelope.
Every query is then authorized at planning and execution time for the
requesting principal: authorization applies to every field referenced in
selection, predicates, ordering, grouping, and aggregation (and joins, when
supported), and row-level policy is applied **before** aggregation, so a
principal cannot infer a protected field through counts or groupings.
Capability revocation takes effect for subsequent projected queries
immediately; it is never frozen into a compiled physical plan. Build-time
pruning is defense in depth, not a substitute for principal-specific
authorization.

### 6. Lag is an explicit per-projection SLO

Each projection declares a profile and target, e.g. `profile serving,
target_lag p99 <= 100ms` for the board projection vs `profile analytical,
target_lag p99 <= 5s` for rollups. The metric: `projection_lag =` wall-clock
time of the application head minus wall-clock time represented by the durable
projection frontier, reported alongside a sequence-distance backlog metric
(time lag alone hides backlog shape). Targets are stated with their hardware
profile, sustained write rate, burst shape, recovery time, and
compaction-concurrency conditions. Violation changes projection health and
produces typed freshness outcomes — never silent stale read-after-write.

### 7. Projection lifecycle outcomes are typed

Projected queries return typed outcomes across the lifecycle: `Ready {result,
frontier, head}`, `ProjectionLagging {required, current, head, lag,
retry_after}`, `ProjectionBuilding {progress, current_frontier}`,
`ProjectionRebuilding {reason, progress}`, `ProjectionDegraded {reason,
current_frontier}`, `ProjectionInvalid {projection_version, contract_version,
reason}`. Fallback to the authoritative path occurs only when the query
explicitly declares a compiled equivalent and a fallback policy — never
silently.

### 8. Retention interaction: bounded replay budget (amends ADR-0085's consequence)

A healthy projection's durable frontier fences the retention watermark. An
unhealthy one must not become a disk-exhaustion incident: each
snapshot-rebuildable projection carries a replay budget (max retention age,
max retained bytes, max sequence backlog). On breach, the projection is
marked `RebuildRequired`, **detached from the retention watermark**, and
rebuilt from an authoritative snapshot plus the remaining tail, returning
typed rebuilding outcomes meanwhile. History-rebuildable projections require
an explicit source-retention policy and never inherit unlimited retention
accidentally.

### 9. Ad-hoc projected queries require separate bounded-query governance

This ADR establishes only that the projection plane is an allowed source for
bounded symbolic ad-hoc queries — tenant-scoped (exactly one organization
scope per query; segments logically partitioned by organization;
cross-organization queries require a separate explicit analytical
capability), resource-budgeted (required limits, scan/grouping budgets),
cancellable, subject to the authorization model above, and never permitted to
starve the apply consumer. Grammar, budgets, admission control, explain
output, and deferred features are specified by **ADR-0087**.

## Consequences

- Board/filter/aggregation workloads move off the per-row authoritative
  pipeline; the authoritative row pipeline remains worth one optimization
  pass (single-copy + batch encode, est. 8.7 → 2–3 µs/row) independently.
- Read sources and freshness policies become permanent public API surface —
  decided now, while no external clients exist.
- New durable consumer + segment files: operational surface (disk, rebuild
  tooling, lag/backlog monitoring), all derived-state under the closure rule.

## Rejected alternatives

- **Serving interactive reads from the projection without a causal fence.**
  Silent staleness after own writes — the failure users notice most.
- **Tail-merge as v1.** Better latency under lag, substantially more
  machinery; deferred as an optimization of Causal, not a different contract.
- **General ad-hoc queries on the authoritative path.** Would bypass
  RiffDB's deployed access-plan, locality, authorization, and resource-bound
  guarantees. Projected ad-hoc queries preserve authoritative write safety
  and are governed by explicit scan, memory, time, and tenant bounds.

## Acceptance criteria (feasibility prototype + evidence)

**Correctness:** projected results match a reference evaluator at the same
frontier across randomized command histories; readers see all-or-none of a
commit's effects; `Causal(token)` never returns pre-token state; irrelevant
commits still advance the processed frontier; duplicate application is
idempotent; deletes/retractions/updates correct; compaction result-invariant;
crash injection never yields an overclaiming durable frontier.
**Authorization:** selected/predicate/order/group/aggregate fields authorized
per principal; row policy before aggregation; revocation effective for
subsequent reads; no inference of hidden values through counts/groupings
beyond stated policy.
**Operability:** rebuild from authoritative backup succeeds; all lifecycle
outcomes typed; a stuck projection cannot exhaust retention storage; apply
continues under concurrent query load; cancellation releases resources;
compaction/rebuild admission-limited.
**Performance:** 450 / 10k / 100k / 1M rows × 1%/10%/100% selectivity ×
{filter, filter+sort+limit, group+aggregate} × 1/16/64 readers × {sustained
writes, bursts, compaction running, cold/warm}; recording query p50/p95/p99,
apply-lag p50/p95/p99, burst catch-up, write/disk amplification, memory per
projected row, CPU split, rebuild throughput.

## Acceptance

Accepted by the maintainer on 2026-08-01.
