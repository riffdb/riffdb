# ADR-0087: Projected Ad-Hoc Query Surface and Resource Governance

- **Status:** Accepted
- **Date:** 2026-08-01
- **Decision owners:** RiffDB maintainers
- **Related:** ADR-0086 (establishes the projection plane as an allowed source for bounded symbolic ad-hoc queries; this record governs the surface itself)

## Context

ADR-0086 admits ad-hoc queries against projected state because they carry no
authoritative-path risk to writes — but "worst case is a slow scan" is not
the worst case: unbounded ad-hoc access risks CPU starvation, memory
pressure, disk churn, apply-lag amplification, and information leakage.
Those are manageable only with explicit budgets and a deliberately scoped
grammar. Agent-generated queries make governance a launch requirement, not
a hardening item.

## Decision (initial scope)

**Allowed in v1:**
- selected projected columns (typed, authorization-checked per ADR-0086 §5)
- typed parameters
- equality and range predicates
- AND, and bounded OR
- ordering
- **required** result limits
- count, sum, min, max
- bounded group-by

**Deferred (each a future amendment, not an implicit extension):**
- joins
- subqueries
- user-defined functions
- recursive queries
- unbounded global aggregation
- cross-organization fan-out

**Governance (normative):**
- Exactly one organization scope per query; cross-organization access
  requires a separate explicit analytical capability.
- Per-query budgets: scan rows, grouping cardinality, memory, wall time —
  exceeded budgets return typed rejections, never partial results.
- Queries are cancellable; cancellation releases resources.
- Admission control: projected reads never starve the apply consumer;
  concurrent ad-hoc admission is bounded per database.
- Explain output: every query can report its access shape and budget
  consumption without executing.
- Authorization identical to ADR-0086 §5, including inference protection
  (row policy before aggregation; every referenced field checked).

## Consequences

- The grammar is small enough to compile, budget, and reason about, and
  large enough for issue-tracker-shaped filtering and dashboard aggregation.
- Every deferral is explicit, so surface growth is a decision, not drift.

## Acceptance

Accepted by the maintainer on 2026-08-01.
Implementation follows ADR-0086's prototype evidence.
