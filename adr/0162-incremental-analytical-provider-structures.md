---
adr: "0162"
title: Compiler-Declared Incremental Analytical Provider Structures
status: accepted
tier: guarantee
date: 2026-09-05
accepted: "2026-10-14"
acceptance: 'maintainer, in session, 2026-10-14: "Option 2: Accept ADR-0162 (upgrade to ADR v2 guarantee) and implement the full WP-714 incremental bitmap, rollup, and top-K provider subsystem across the compiler, catalog, columnar, and projection crates before closing WP-715"'
requires: [ADR-0010, ADR-0017, ADR-0051, ADR-0053, ADR-0085, ADR-0086,
  ADR-0111, ADR-0124, ADR-0130, ADR-0152, ADR-0160, ADR-0161]
amends: []
supersedes: []
requirements: [PRJ-001, PRJ-002, PRJ-003, PRJ-004, OQ-017, OQ-018, OQ-019,
  OQ-020, OQ-021, OQ-022, OQ-023, OQ-024, OQ-044, OQ-045, OQ-046, OQ-047,
  OQ-048, OQ-049, OQ-050, OQ-051, OQ-052, OQ-053, OQ-054, OQ-055, PERF-001,
  PERF-007, PERF-008, PERF-018]
packages: [WP-714, WP-715]
obligations:
  - id: OBL-0162-1
    package: WP-714
    proof: incremental_bitmap_rollup_and_top_k_match_independent_reference_oracle
    says: Every admitted segment-local equality/presence bitmap, retraction-aware exact rollup, and maintained bounded top-K lane with repair reserve equals an independent reference evaluator across inserts, replacements, optional transitions, retractions, and merges.
  - id: OBL-0162-2
    package: WP-714
    proof: incremental_provider_structures_apply_atomically_and_enforce_policy_alignment
    says: Source transitions apply idempotently and atomically to a private candidate snapshot before visible frontier publication, overflow or exhausted reserve degrades to typed rebuilding/unavailable without approximation or authoritative row reads, and policy-invariant or policy-aligned structure identities prevent denied rows from influencing counts, membership, rank, top-K, timing class, or availability.
  - id: OBL-0162-3
    package: WP-714
    proof: incremental_provider_descriptor_and_bounds_refuse_unsupported_or_excessive_plans
    says: The closed compiler/provider descriptor registry validates structure kind, exact source/predicate/policy/order/aggregate semantics, state/write-amplification/rebuild bounds, and cross-provider bridge prohibition, refusing unsupported or excessive definitions before activation.
  - id: OBL-0162-4
    package: WP-714
    proof: scripts/columnar-incremental-qualification
    says: Registered ticket-queue top-K and run-metric rollup qualification verifies the predeclared query-CPU reduction, maintenance amplification, sustained-write catch-up lag, and no-structure non-regression gates before incremental capability selection.
review_triggers:
  - A caller or configuration value could name or select a structure, field, aggregate, predicate, policy partition, precision, refresh mode, state size, fallback, or provider.
  - An incremental rollup, bitmap, or top-K lane would return an approximate answer, guess a replacement extreme after deletion, or repair by reading authoritative rows inside apply.
  - Denied rows could influence a count, Boolean state, rank, top-K member, exact-total metadata, cursor, lifecycle choice, public diagnostic, notification timing class, or shared availability.
  - Superficially compatible bitmaps or row IDs would create a cross-provider bridge without a later accepted real-consumer plan and bounded bridge representation.
  - A query IR, module, descriptor, cursor, protocol, durable control, generation root, manifest, segment V1/V2, or application-lock byte format would rotate incompatibly.
---
# ADR-0162: Compiler-Declared Incremental Analytical Provider Structures

## Context

Typed V2 segments plus vectorized and parallel scans (ADR-0160, ADR-0161) make
bounded analytical queries cheaper, but repeated queues, dashboards, and rollups
still redo work whose exact state can be maintained from the projection's
ordered change stream. RiffDB already has the right ownership model: derived
state is event-fed, rebuildable, generation- and frontier-labelled, and never
authoritative.

A generic materialized-view language would be unsafe and premature: arbitrary
expressions, joins, cubes, user-defined aggregate state, or caller-selected
indexes would weaken compiler-visible cost and policy reasoning. Conversely,
adding one bespoke structure per application would fragment provider identity,
recovery, policy, and compatibility. The first incremental structures therefore
form one small compiler-owned capability family, reuse the exact aggregate and
total-order registries, and activate only when a named query proves bounded
write amplification, state, rebuild, and inference behavior.

## Proposed Decision

1. **Closed incremental-provider descriptor.** A projection definition may
   declare a finite set of maintained analytical structures. Each descriptor
   binds: projection, entity, organization-partition, history-incarnation,
   generation, and provider-state identities; compiler-fixed predicate/policy
   shape and source fields; one closed structure kind and semantic-registry
   version; exact key, comparison, missing/null, retraction, and merge
   semantics; maximum structures, groups, distinct keys, entries, bytes, write
   amplification, replay work, rebuild work, and diagnostic size; freshness and
   provider-epoch participation; and the finite named-query plan family
   permitted to consume it. The descriptor is compiler/catalog state, never
   request input. Applications choose only typed parameters of a generated named
   operation.

2. **Three initial exact structure families.** The first registry contains only:
   - *Segment-local equality/presence bitmaps:* For a compiler-declared
     low-cardinality field or V2 dictionary lane, immutable bitmaps identify
     exact canonical values and missing/null/present states within one segment,
     intersecting only the finite predicate family named by the plan.
     Cardinality and bitmap bytes are bounded; high-cardinality overflow omits
     the capability or degrades the candidate generation rather than building
     unbounded state.
   - *Exact mergeable rollups:* A fixed partition/group key maintains only
     ADR-0152 functions whose descriptor supplies exact bounded update,
     retraction, and merge state: `count`, `count_present`, checked exact `sum`,
     exact `mean` (`ExactMeanV1 { total, count }`), Boolean `any`/`all` with
     counted truth states, `min`/`max` over a bounded counted ordered multiset
     (deletion of an extreme never guesses a replacement), and `count_distinct`
     / `count_distinct_present` over bounded exact counted membership.
   - *Maintained bounded top-K lanes:* One compiler-fixed predicate family and
     complete total order maintains at most declared K plus a bounded repair
     reserve and exact entity-key tie-breaker. Updates and retractions preserve
     exact membership; if the reserve is exhausted by retractions and cannot
     prove the next result, the structure becomes rebuilding/unavailable rather
     than returning an approximate queue.
   These families are independent provider capabilities; superficially
   compatible bitmaps or row IDs never authorize an implicit cross-provider
   bridge (`OQ-023`).

3. **Atomic frontier apply and rebuild.** For one authoritative commit,
   projection row changes and every affected incremental structure are applied
   to a private candidate snapshot before the visible frontier advances.
   Publication exposes all or none of those effects. Apply is idempotent by
   `(ProjectionIdentity, ProjectionGeneration, CommitSequence)` and exact
   `ProjectionApplyHash` (`PRJ-003`). Insert, replacement, optional-state
   transition, and deletion/retraction update both additions and removals.
   Checked arithmetic or structural overflow records one typed generation
   failure without advancing. No structure repairs by reading authoritative rows
   inside apply; repair is a separate bounded rebuild from an exact snapshot
   plus retained tail. Compaction preserves exact logical results, frontier,
   generation, and independent-evaluator digest.

4. **Policy alignment in structure identity.** Each structure is either valid
   for a compiler-proved role-invariant public row universe or built for one
   exact policy shape and capability revision family whose partitioning prevents
   denied rows from entering shared counts, membership, rank, top-K repair,
   statistics, or timing class (`OQ-021`). Current request authorization, field
   visibility, row-policy facts, and pre-release safe points remain mandatory.
   Capability or policy changes retire the affected provider epoch and rebuild
   under the new identity rather than filtering a wider aggregate after the
   fact.

5. **Bounded state and operational honesty.** Each definition declares maximum
   state and write amplification. Admission sums all maintained structures
   affected by one source change and rejects excessive definitions before
   activation. Runtime accounts actual entries and bytes; exceeding a bound
   degrades the candidate generation and returns a typed rebuilding/unavailable
   outcome rather than dropping updates, evicting exact state, or serving stale
   success. Metrics remain fixed-cardinality and redaction-safe.

6. **Exact provider selection and explicit fallback.** The compiler selects an
   incremental capability only when its semantics, policy shape, provider epoch,
   bounds, and result algebra exactly satisfy the named query. Runtime never
   substitutes a nearby rollup, widens a bitmap, truncates K, changes grouping,
   uses stale state, or converts unavailability into empty data. An
   independently compiled V2 vectorized-scan fallback may exist only if sealed
   in the plan identity; callers cannot request or suppress fallback.

7. **Defer approximation and general materialized views.** Approximate distinct
   sketches, quantiles, histograms, cubes, windows, arbitrary expression
   indexes, user-defined aggregates, general joins, recursive views, and
   cross-partition rollups remain absent (`OQ-054`). WP-714 proves two neutral
   workload shapes—a ticket/work queue using exact top-K and a run-metric
   dashboard using exact rollups.

8. **Activation gates against scans and maintenance cost.** Incremental state
   activates only when the registered workload corpus demonstrates: at least 3x
   lower p50 and p95 query CPU for repeated rollup and top-K reads than V2
   vectorized scan; at most 1.20x projection-apply CPU and at most 1.50x derived
   bytes; bounded catch-up lag p95 within the declared SLO under sustained
   writes; and at most 5% regression for projections declaring no incremental
   structures. Any miss leaves the failed capability disabled with value-free
   evidence.

## Options Considered

1. **General materialized views:** rejected because arbitrary expressions and
   joins escape finite compiler-owned semantics and cost.
2. **Application-specific cached dashboards:** rejected because policy,
   frontier, recovery, and compatibility would fragment outside RiffDB.
3. **Approximate sketches first:** rejected because exact result and inference
   semantics are required and approximation needs a separate contract.
4. **A small exact compiler-owned structure registry:** selected because it
   serves measured queue and rollup workloads while remaining rebuildable and
   statically bounded.

## Consequences

- Repeated queues and dashboards avoid rescanning unchanged projection rows.
- Derived write and storage amplification is statically bounded and accounted.
- Exact deletes and replacements use counted retraction-aware state rather than
  append-only shortcuts; exhausted top-K reserve degrades to typed rebuilding.
- Approximate and general analytical view constructs remain absent.

## Compatibility

Existing authoritative, projection V1, segment V1/V2, query IR/module, wire,
generated, cursor, and application-lock bytes remain unchanged. Old binaries
refuse unknown provider state before publication while prior scan plans remain
intact.

## Security

Provider state is scoped to one organization and either a role-invariant row
universe or an exact policy-aligned identity. Denied rows never enter shared
result-shaping state. Fresh authority remains per request; persisted allow
decisions and redact-after-aggregation are forbidden.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** Applications invoke only finite
  generated operations and typed values. They cannot request a structure,
  arbitrary aggregate, approximation, materialized view, refresh, fallback,
  stale success, policy bypass, or resource increase. Unsupported or unavailable
  state fails typed.
- **Scale:** Structure count, groups, keys, entries, dictionaries, bitmaps,
  top-K reserve, bytes, write amplification, apply/rebuild work, lag, and
  diagnostics are statically bounded per partition and provider generation,
  assuming neither database-wide memory nor co-located authoritative storage.

## Testing

- The four front-matter obligations (`OBL-0162-1..4`) verify the independent
  bitmap/rollup/top-K reference oracle, atomic frontier apply and policy
  alignment, descriptor and bound refusal, and registered mechanics
  qualification (`scripts/columnar-incremental-qualification`).
- WP-715's cross-plane referee (`scripts/columnar-program-b-referee`) verifies
  scalar, vectorized, parallel, and incremental equivalence and honest
  per-capability mechanics gating.

## Requirements and Work Packages

- **Requirements:** `PRJ-001` through `PRJ-004`, `OQ-017` through `OQ-024`,
  `OQ-044` through `OQ-055`, `PERF-001`, `PERF-007`, `PERF-008`, `PERF-018`
- **Incremental structures:** `WP-714`
- **Cross-plane acceptance:** `WP-715`

## Decision Deadline

Accepted by the maintainer in session on 2026-10-14 before WP-714 and WP-715
completion. Any approximate function, general view language, cross-provider
bridge, or policy relaxation requires a separate exact decision.
