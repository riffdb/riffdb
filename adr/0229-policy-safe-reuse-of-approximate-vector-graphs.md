---
adr: "0229"
title: Policy Safe Reuse Of Approximate Vector Graphs
status: accepted
tier: guarantee
date: 2026-09-16
accepted: 2026-09-16
acceptance: 'maintainer, in session, 2026-09-16: "Accept both ADRs as written" (ADR-0228 and ADR-0229)'
requires: [ADR-0091, ADR-0136]
amends: [ADR-0091 approximate routing on an uncached admitted population]
supersedes: []
requirements: [VEC-007, VEC-008, VEC-009, VEC-010, VEC-011]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers: []
---
# ADR-0229: Policy Safe Reuse Of Approximate Vector Graphs

## Context

The vector executor constructs an HNSW graph synchronously for every ANN query,
even when many queries use the same admitted population. Graph construction
costs substantially more distance work than one exact scan. Heap improvements
reduce construction cost but do not remove that repeated work. Production
candidate admission is currently bounded to 500 rows; unrestricted large-data
examples in the report do not describe that production path. This proposal
repairs the reported high-severity query-cost issue without adding a new feature.

A generation-wide ingestion graph is not an admissible shortcut: ADR-0136 and
VEC-007/008 require policy, model and organization exclusion before topology or
statistics can be influenced. Exact execution on a cold population also changes
current compiler-selected ANN routing. Both require explicit review; this record
is proposed, not accepted, and authorizes no current runtime change.

## Decision

1. All existing current organization, model/evidence, scalar predicate and
   principal-policy checks run on every query before graph lookup or distance
   computation. A graph cache is never an admission or authorization cache.
2. A graph is reusable only for an exact immutable population binding: source
   and generation, history incarnation, definition and model, selected frontier,
   organization, vector field, distance metric, policy identity and capability
   revision, plus the complete ordered sequence of admitted entity keys,
   versions and canonical vectors. A digest may accelerate lookup but cannot
   replace exact population verification. Query vector and K are not graph inputs.
3. Below the declared threshold execution remains exact. Above it, an exact
   matching graph may serve ANN under the unchanged recall target. When none is
   available, execute the bounded exact reference scan immediately and report
   the actual exact search kind. Lack of a graph is not an error and never permits
   reduced admission checks or results from a different population.
4. Cold execution may offer its fully admitted immutable population to a bounded
   background builder. Admission is nonblocking: at most one build runs and one
   population waits per provider, each at most the existing 500-row production
   bound. A process-wide cache/build budget of 64 MiB charges retained canonical
   vectors, identity data and worst-case graph allocation before accepting work.
   Budget refusal drops the optimization and leaves the exact result unchanged.
5. Builder topology and tie-breaking remain deterministic. A completed graph is
   installed only under its original exact binding; cancellation, supersession,
   failed construction or source invalidation releases all retained state.
   No graph is installed as the current generation merely because it completed
   later. Cache replacement cannot change authoritative or derived frontiers.
6. Graphs are disposable memory-only derived state in this revision. Reopening
   starts cold. No graph format, root manifest, authoritative record or durable
   hash changes. Eviction can affect cost, never policy admission or correctness.
7. Continuation-based vector query execution uses the exact path for every page
   in this revision. Warming or evicting an ANN cache must not change a paged
   candidate set. Reusable ANN is restricted to non-continuation execution until
   a separately reviewed cursor binding can retain its exact graph selection.
   Existing cursor frontier, policy and module validation remains mandatory.
8. Existing freshness and degradation behavior is unchanged. Exact fallback does
   not bypass an unavailable generation, stale model, excessive embedding
   staleness or a failed authoritative evidence check. SPEC VEC-009 and associated
   routing tests will explicitly permit cold exact execution above the threshold;
   the selected path is never misreported as successful ANN execution.

## Options and consequences

Keeping per-query builds preserves current routing but pays construction on
every request. Persisting an unfiltered generation graph violates the admission
boundary. Exact-only execution is safe but gives up reuse entirely. Bounded
post-admission graph reuse retains the approximate quality contract for warm
non-continuation queries while making cold work one exact scan plus optional
background construction. Workload evidence must establish the benefit within
the actual production candidate and memory bounds before implementation closes.

## Standing design tests

- **Interface safety:** no caller can provide a graph, relax admission, increase
  budgets, bypass typed freshness or select another model's population.
- **Scale:** existing row/dimension limits plus one active and one queued build
  per provider and a shared 64 MiB allocation budget. A queue never grows per
  request; exact fallback remains independently query-budgeted.
- **Recovery:** graphs are never authoritative; cache loss requires no recovery
  decision and cannot advertise an unapplied frontier.

## Checks

- Repeated queries over one population build once and reuse identical topology;
  cold execution matches exact reference answers and warm execution meets recall.
- Change policy/capability revision, model, org, field, metric, frontier, key,
  version or vector bytes: an old graph must not match. Deliberately collide the
  lookup digest and prove full binding comparison still refuses reuse.
- Denied rows, including malformed vectors, affect neither graph contents nor
  reported statistics. Every cache hit still performs current admission.
- Deterministically pause a build, invalidate its source, complete it and prove it
  cannot replace the selected generation or satisfy a different binding.
- Explore concurrent cache lookup/eviction/cancellation and prove allocation
  budget custody through outstanding builders; memory refusal still yields exact.
- Page before and after background warming and eviction; exact candidate order,
  cursor semantics and authorization remain unchanged.
- Record cold/warm distance counts, allocations and latency at the real 500-row
  bound; run existing model, policy, recall and freshness suites. Update vector
  operations documentation when the accepted implementation lands.
