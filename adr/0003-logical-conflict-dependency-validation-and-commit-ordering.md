# ADR-0003: Logical Conflict Ownership, Dependency Validation, and Commit Ordering

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-13
- **Decision deadline:** Before WP-060 or WP-090 public interfaces merge

The human maintainer accepted this exact text on 2026-07-13.

## Context

Commands execute optimistically over bounded snapshots but may commit only if
their declared conflict and dependency assumptions still hold. Correctness
depends on canonical keys, all-or-nothing acquisition, complete evidence, exact
predicate re-evaluation, and a single owner for commit ordering.

## Decision

The compiler produces the complete, bounded set or derivation plan of logical
`ConflictKey` values before mutable capability acquisition. Keys have one
canonical byte encoding and total order. The conflict manager sorts, deduplicates,
and acquires the entire set exclusively; it never exposes partial acquisition.
The acquired capability is non-cloneable, non-serializable, non-transferable, and
released on cancellation or terminal completion.

Runtime evaluation records every influential entity read, observed absence,
ordered range/epoch read, and predicate dependency in canonical evidence. At the
durable boundary, the commit coordinator validates versions/epochs and re-runs
the exact compiler-produced validation plan identified by contract lineage,
version, and plan hash against current values. Missing evidence, missing
historical plans, or hash mismatch fails closed.

Only the commit coordinator admits terminal execution, assigns a contiguous
`CommitSequence`, and orders authoritative mutations, outcome, events,
provenance, and commit record. Storage provides atomic mechanics but does not
choose semantic ordering.

## Options Considered

1. **Known-upfront exclusive logical keys plus validation:** Approved POC model.
2. **Storage-engine row locks:** Leaks engine concepts and cannot express logical
   domains portably.
3. **Partial/incremental key acquisition:** Risks deadlock and violates declared
   dependency visibility.
4. **Trust captured predicate booleans:** Misses changes between evaluation and
   commit.

## Consequences

- Compiler and runtime must reject undeclared or unbounded dependencies.
- The catalog must retain the exact immutable validation plan for admitted work,
  or the intent must carry an independently validated bounded representation.
- Cancellation and fairness are explicit conflict-manager semantics.
- Commit validation may reject and require retry even after successful evaluation.

## Compatibility

`ConflictKey` bytes, evidence variants, plan identity, and commit ordering become
semantic compatibility boundaries. Engine lock identities are never durable or
public.

## Security

Conflict keys and evidence must not expose secrets in errors, tracing, or metrics.
Fail-closed plan lookup prevents substituting weaker invariants after deployment.

## Testing

Golden key ordering, compiler dependency-negative tests, mutate-before-commit
tests for every evidence variant, reference-model histories, Loom reduced lock
models, Shuttle multi-key schedules, explicit cancellation barriers, and durable
record-set assertions freeze the decision.

## Requirements and Work Packages

- **Requirements:** `TXN-002`, `TXN-020` through `TXN-023`,
  `TXN-030`, `TXN-031`, `TXN-040` through `TXN-044`, `STO-001`, `STO-010`
- **Defines or blocks:** `WP-060`, `WP-080`, `WP-090`, `WP-100`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Accept key/evidence ownership before WP-060 and acquisition semantics before
WP-090. Accept exact predicate-plan and ordering text before WP-100 begins.
