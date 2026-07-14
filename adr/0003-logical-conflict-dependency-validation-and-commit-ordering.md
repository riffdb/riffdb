# ADR-0003: Logical Conflict Ownership, Dependency Validation, and Commit Ordering

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-13, clarified 2026-07-13
- **Clarified by:** ADR-0016 for canonical structural root-key derivation equality
- **Decision deadline:** Before WP-060 or WP-090 public interfaces merge

The human maintainer accepted this exact text and the ADR-0016 root-key
derivation equality clarification on 2026-07-13.

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

### 2026-07-13 aggregate-root validation amendment

The human maintainer accepted this companion amendment on 2026-07-13 before the
WP-040 executable IR was frozen.

When a grammar-v1 command mutates a child entity and the owning aggregate has an
aggregate invariant, the compiler makes the required aggregate-root observation
explicit. It emits one dense, plan-local `RootValidationReadPlan` unless an exact
source-declared binding of that root supplies the record. This plan is not a
source binding, does not acquire an additional conflict domain, and has no
declared business outcome. Multiple child mutations share one root-validation
read exactly when their checked root-key derivations are canonically
structurally equal under ADR-0016. That comparison ignores plan-local expression
IDs, arena insertion order, and shared-versus-duplicated DAG representation; it
does not use runtime value equality or algebraic rewriting.

The snapshot and commit-validation request carry root-validation observations
separately from source binding observations. Each produces the existing
`EntityObservation` dependency, so no new dependency tag or durable record is
introduced. Storage may coalesce physical reads of an identical entity target,
but it must return both semantic positions and the coordinator must validate the
complete plan-declared source/root/range target set. Source binding failures are
resolved first in ascending `BindingId`. A required internal root that is then
absent is an `ExecutionFault::Integrity`: it produces no application write or
sequence and leaves any mutating admission pending for operator intervention.

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
