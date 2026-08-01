# ADR-0086: Columnar Projection and Read Freshness Classes

- **Status:** Proposed
- **Date:** 2026-08-01
- **Decision owners:** RiffDB maintainers
- **Related:** ADR-0010 (frontiers), ADR-0070 (read stability), ADR-0080 (partitioned events, durable consumers), ADR-0082 (single total order)

## Context

Measured on real disk (board-scale harness, 2026-08-01): RiffDB's per-row read
cost is 8,741 ns/row vs PostgreSQL's 837 ns (10.4×); a 450-row board page costs
4.06 ms (UX-acceptable) with ~96% of that latency in the per-row pipeline.
Point reads are 1.19× at 50 rows. Separately, arbitrary filter combinations
(issue-tracker-style ad-hoc predicates) do not map to compiled named queries
with declared access paths, and analytical aggregation has no story. A
columnar projection — derived, rebuildable, fed from the single total order —
addresses all three: at per-organization scale a column scan is a universal
index, and aggregation is native.

## Decision

### 1. The projection is a derived accelerator, never authoritative

A per-database columnar store (delta buffer + compacted segments) built by a
durable consumer (ADR-0080) from the commit stream. It is rebuildable from
authoritative records, excluded from backup identity (SPEC's accelerator
clause), and its loss is a rebuild, never data loss. No contract amendment to
STO-011/STO-012/backup format is required while this holds.

### 2. Two public read-freshness classes, declared per query

- **Authoritative** (existing): serves from the authoritative snapshot;
  read-your-writes via `read_after_commit` against the application head.
  Unchanged semantics, unchanged path.
- **Projection-fenced** (new): serves from the columnar projection;
  `read_after_commit(seq)` fences against the PROJECTION frontier — the query
  waits (bounded, typed timeout) until the frontier reaches `seq`, then reads.
  A client that just committed sees its own write or a typed
  `projection-lagging` error, never silent staleness. Queries declare their
  class at deploy time; the class is part of the compiled contract surface.

Interactive board/filter queries use projection-fenced reads with the fence
carried automatically by the SDK from the session's last commit. Analytics
run projection-fenced without a fence (bounded-staleness, frontier reported
in the response).

### 3. Bounded lag is a stated, measured property

The consumer's apply lag under the write-parity workloads is a published
gate (target: frontier within 100 ms of head at sustained parity-load write
rates; burst recovery bound stated). A projection that violates its lag gate
is unhealthy: it alerts, and — per ADR-0085 — blocks the retention watermark.
Projection health is therefore operationally first-class, not best-effort.

### 4. Ad-hoc query surface (scoped)

The projection MAY expose a bounded ad-hoc predicate/aggregation surface
(filter combinations over projected columns, order, limit, aggregate) — the
JQL-shaped workload — because projected reads carry no authoritative-path
risk: worst case is a slow scan of derived data, bounded by per-organization
scale. The authoritative path's compiled-only discipline is unchanged.
Capability enforcement applies identically to projected reads (field
visibility filters projected columns at build time, not query time).

## Consequences

- Board/filter/aggregation workloads move off the per-row authoritative
  pipeline; the 10.4× marginal-cost gap becomes a columnar scan.
- The authoritative row pipeline remains worth one optimization pass
  (single-copy + batch encode, est. 8.7 → 2–3 µs/row) for authoritative-class
  list reads — independent of this decision.
- Two freshness classes become permanent public API surface — decided now,
  while no external clients exist, per the format-acceptance discipline.
- New durable consumer + segment files: operational surface (disk for
  segments, rebuild tooling, lag monitoring) — all derived-state, all
  rebuildable.

## Rejected alternatives

- **Serve interactive reads from the projection without a fence.** Silent
  staleness after own-writes; the exact failure users notice most.
- **Tail-merge (columnar + authoritative delta overlay) as v1.** Strictly
  better latency under lag, substantially more machinery; deferred as an
  optimization of the projection-fenced class, not a different contract.
- **SQL on the authoritative path.** Reopens every injection/planning/
  un-indexed-query failure mode the compiled contract exists to exclude.

## Acceptance

Pending maintainer acceptance. On acceptance: consumer + delta-store
feasibility prototype measured against the board-scale harness (scan latency
and apply-lag under burst) before the implementation package is briefed.
