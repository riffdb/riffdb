---
adr: "0264"
title: Atomic Indexed Selection Within One Partition
status: accepted
tier: guarantee
date: 2026-09-28
accepted: '2026-09-28'
acceptance: 'maintainer, in session, 2026-09-28: i accept, looks good (ADR-0264 as written)'
requires: [ADR-0061, ADR-0170, ADR-0252, ADR-0262]
amends: [ADR-0002, ADR-0061]
supersedes: []
requirements: [TXN-001, TXN-002, CMP-013, CMP-015, PERF-012]
packages: [WP-817]
obligations: []
review_triggers:
  - Selection discovers conflict ownership or reaches another partition.
  - Selection skips candidates, filters residual predicates, or expands its one-row bound.
  - An empty or rejected outcome omits selection or policy validation.
  - A selected value determines another explicit binding target.
  - Existing artifacts acquire selector semantics or changed identity.
---
# ADR-0264: Atomic Indexed Selection Within One Partition

## Context

Commands name exact targets before evaluation. Allocating an unspecified free
slot therefore requires an application query, choice, and checked command.
That protects a chosen slot when the command rechecks availability, but leaves
selection and contention retries outside the command boundary. Existing
workflow transitions and fenced leases do not supply durable orchestration.

The maintainer selected atomic indexed allocation first, with input-computable
pool conflict keys and bounded server-side retries. This record admits one
closed selection construct, not general transactional queries, cross-partition
transactions, cross-aggregate invariants, or durable multi-command workflows.

## Decision

1. Add `mutate first Entity.index(prefix_arguments) as binding else Outcome {}`.
   `first` is contextual; existing identifiers keep their meaning. The named
   authoritative secondary index supplies the equality prefix. Selection returns
   its first entry in canonical ascending complete-index-key order, including
   the existing entity-key tie-breaker. The empty outcome is mandatory.
2. A command may have one selector in its ordinary binding phase, plus existing
   input-addressed companion bindings and effects. Reject collection commands,
   decision-arm selectors, cascade/restrict deletion, other explicit bindings
   to the selected entity type, and another binding target depending on a selected
   value. Compiler-owned invariant root reads are not explicit binding targets.
3. The compiler proves the prefix confines every candidate to one partition and
   proves the complete conflict-key union from validated inputs and constants,
   using the prefix's equality-bound root-key components. No selected value
   determines a route or conflict key, including unique-conflict ownership.
   Acquire the complete canonical conflict set before selection; no late lock,
   lock upgrade, or optimistic ownership substitution is admitted.
4. Prefix expressions contain only validated inputs and constants. Eligibility
   is index membership, with no residual filter, cursor, offset, reverse order,
   random choice, skip-locked behavior, or caller-selected scan bound. Each
   evaluation retains and materializes at most one candidate. The physical
   access path must seek the proven partition-constrained prefix and stop at its
   first entry; a generic reader that scans unrelated partitions is insufficient.
5. The storage snapshot owns the index observation, selected entity, and required
   aggregate-root observations from one consistent authoritative view. Typed,
   bounded evidence binds the exact plan, prefix, epoch, selected key, and record.
   The selected key and record must reconstruct the index entry and lie within
   the proved partition and conflict domain. Inconsistency fails closed.
   Deterministic runtime receives owned observations and performs no I/O.
6. Preserve the conservative `(partition, index)` generation of ADR-0061 and
   PERF-012, plus exact selected-entity and root dependencies. Validate all of
   them at the authoritative boundary for writes, empty outcomes, and every
   other zero-mutation outcome. A changed generation or entity observation
   requires complete reevaluation, never mutation of the previous candidate.
7. Reuse the existing three-evaluation budget and deadline/cancellation rules.
   Internal retries may select a different row while preserving the original
   normalized input, logical context, and idempotency identity. Exhaustion uses
   the existing typed retryable failure. Unknown commit status must resolve by
   identity before further allocation. No server-side waiting or background
   workflow is introduced. No fairness or starvation bound is promised.
8. An empty prefix returns the declared business outcome only after current
   validation. Persist and replay it normally: retrying that identity after new
   capacity appears still returns its original empty result. A new business
   attempt uses a new identity. A requirement failing on the first candidate
   returns its declared outcome; it does not search for another candidate.
9. Selection does not supply assignment evidence. ADR-0262's arithmetic,
   explicit business associations, caller-version fences, initialization, and
   overwrite rules still apply. Every written aggregate retains ADR-0252's
   invariant coverage, with selected/root targets resolved from snapshot proof.
10. Authorize the command and selection scope before disclosure. Preserve all
    current capability, row-policy, relationship, secret-flow, and outcome
    controls. Selected-row policies apply to mutations and no-effect outcomes.
    An unauthorized candidate yields the existing redacted denial, never an
    empty result or a search past that row. Empty results retain command/scope
    authorization and generation evidence. No policy bypass or public raw scan
    capability is added.
11. Extend the executable plan with a bounded selector descriptor and structural
    proof. Construction and decoding rederive its locality, index, scope, target,
    resource, and invariant coverage. Canonical selector semantics participate
    in plan identity. Allocate the next unused grammar/IR/bundle version together
    under the least-sufficient writer policy. Old readers refuse it; plans not
    using selection retain their exact historical bytes and identities.
12. Use the existing ordered evaluation path. Exclude selectors from detached
    or parallel optimizations that assume every target is input-derived. Grouped
    and deferred-durability execution must preserve current selection evidence,
    predecessor visibility, atomic effects, and acknowledged durability.
13. SDK, CLI, gRPC, and MCP expose only the ordinary generated named command,
    declared inputs, and outcomes. Explain shows selector order, prefix,
    conflict ownership, generation dependency, bound, and retries without data.
    Diagnostics identify unsupported shapes with safe source-span help.

Example body, with an index beginning `(pool_id, available, ...)` and a conflict
key derived from `pool_id`:

```riff
mutate first Slot.by_available(pool_id, true) as slot else NoCapacity {}
require available: slot.available else NoCapacity {}
set slot.available = false checked_by available
set slot.holder = holder checked_by available
return Allocated { slot: slot }
```

## Options considered

- **Keep selection in applications:** retains the loop that motivated this work.
- **Discover per-row conflicts after selection:** rejected for this release;
  it needs a different acquisition and retry proof.
- **Arbitrary bounded predicates or candidate pages:** deferred; they introduce
  exhaustion, filtering, authorization, and partial-search semantics.
- **One indexed candidate under proved ownership:** chosen; it removes the
  select-and-claim round trip while retaining bounded dependency visibility.

## Consequences

- An eligible allocation needs one command submission and one atomic commit,
  with ordinary uncertainty resolution when the response is lost.
- Aggregate/pool design still affects contention. An index mutation in another
  pool of the same partition can invalidate selection conservatively. Finer
  epochs, repartitioning, general joins, and durable orchestration are deferred.
- New selector deployments require ordinary exact bundle, lock, and generated
  binding rotation. Existing deployments do not acquire selector behavior.

## Standing design tests

- **Interface safety:** applications cannot choose a weaker validation mode,
  arbitrary query, dynamic lock acquisition, partial commit, policy bypass,
  retry subset, or replacement idempotency identity. Business relevance remains
  the explicit author claim already accepted in ADR-0262.
- **Scale:** one candidate per evaluation, at most three evaluations, existing
  byte/dependency/deadline limits, and a compiler-proven index seek. No work
  scales with pool population. Report contention rather than claim fairness or
  disjoint-pool isolation from conservative index generations.

## Checks

- Source-span compiler and malformed-IR tests for scope, index, locality,
  conflict/unique ownership, resource bounds, write evidence, and invariant
  coverage; byte-exact old-artifact and new encoding round trips.
- Deterministic allocation races, first-row movement, empty-prefix insertion,
  release/reallocation, same-partition cross-pool invalidation, retry exhaustion,
  cancellation, policy revocation/denial, and no-effect dependency checks.
- Memory/redb parity and real process crashes prove atomic state, outcome,
  idempotency, events, provenance, and replay after uncertain acknowledgement.
- Generated clients, CLI, gRPC, MCP schemas/authorization, and shared-service tests.
- Fresh-population comparisons at 1/8/32 clients for shared and independent pools:
  latency, throughput, retries, lock wait, and inspected entries. Require bounded
  selection work; make no unmeasured performance or fairness guarantee.
- Package acceptance, full CI, reachable handbook updates, and human review
  before merge. Use a dedicated worktree and remove it after the reviewed merge.
