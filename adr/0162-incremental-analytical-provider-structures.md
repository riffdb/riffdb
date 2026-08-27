# ADR-0162: Compiler-Declared Incremental Analytical Provider Structures

- **Status:** Proposed
- **Direction approved:** No
- **Exact text accepted:** No
- **Decision deadline:** Before WP-714 persists a bitmap, rollup, or maintained
  top-K structure or advertises its capability to a compiled query
- **Requires:** ADR-0010, ADR-0017, ADR-0051, ADR-0053, ADR-0085, ADR-0086,
  ADR-0111, ADR-0124, ADR-0130, ADR-0152, ADR-0160, ADR-0161
- **Defines or blocks:** WP-714 and WP-715

This record is planning input only until its exact text is accepted.

## Context

Typed V2 segments plus vectorized scans make bounded analytical queries cheaper,
but repeated queues, dashboards, and rollups still redo work whose exact state
can be maintained from the projection's ordered change stream. RiffDB already
has the correct ownership model: derived state is event-fed, rebuildable,
generation- and frontier-labelled, and never authoritative.

A generic materialized-view language would be unsafe and premature. Arbitrary
expressions, joins, cubes, user-defined aggregate state, or caller-selected
indexes would weaken compiler-visible cost and policy reasoning. Conversely,
adding one bespoke structure per application would fragment provider identity,
recovery, policy, and compatibility.

The first incremental structures should therefore form one small compiler-owned
capability family, reuse the exact aggregate and total-order registries, and be
admitted only when a real named query can prove bounded write amplification,
state, rebuild, and inference behavior.

## Proposed Decision

### 1. Add a closed incremental-provider descriptor

A projection definition may declare a finite set of maintained analytical
structures. Each descriptor binds:

- projection, entity, organization-partition, history-incarnation, generation,
  and provider-state identities;
- compiler-fixed predicate/policy shape and source fields;
- one closed structure kind and semantic-registry version;
- exact key, comparison, missing/null, retraction, and merge semantics;
- maximum structures, groups, distinct keys, entries, bytes, write
  amplification, replay work, rebuild work, and diagnostic size;
- freshness and provider-epoch participation; and
- a named-query plan family permitted to consume it.

The descriptor is compiler/catalog state, never request input. Applications may
choose only typed parameters of a generated named operation. They cannot name a
structure, field, aggregate, predicate, policy partition, precision, refresh
mode, state size, fallback, or provider.

### 2. Admit three initial exact structure families

The first registry contains only:

1. **Segment-local equality/presence bitmaps.** For a compiler-declared
   low-cardinality field or V2 dictionary lane, immutable bitmaps identify exact
   canonical values and missing/null/present states within one segment. They
   may intersect only the finite predicate family named by the plan. Cardinality
   and bitmap bytes are bounded; high-cardinality overflow omits the capability
   or degrades the candidate generation rather than building unbounded state.
2. **Exact mergeable rollups.** A fixed partition/group key maintains only
   ADR-0152 functions whose descriptor supplies exact bounded update,
   retraction, and merge state. Initial eligibility is `count`,
   `count_present`, checked exact `sum`, exact `mean` state, and Boolean
   `any`/`all` with counted truth states. `min`/`max` require a bounded counted
   ordered multiset or remain scan-executed; deletion of the current extreme
   cannot guess a replacement. Distinct counts require bounded exact counted
   membership and may not silently become approximate.
3. **Maintained bounded top-K lanes.** One compiler-fixed predicate family and
   complete total order maintains at most the declared K plus bounded repair
   reserve and exact entity-key tie-breaker. Updates and retractions preserve
   exact membership. If the reserve cannot prove the next result, the structure
   becomes rebuilding/unavailable; it cannot return an approximate queue.

These families are independent provider capabilities. A query may combine them
only when ADR-0130 supplies one exact common epoch and the compiler defines a
bounded bridge representation. Superficially compatible bitmaps or row IDs do
not authorize a new cross-provider bridge.

### 3. Apply every source transition atomically at the projection frontier

For one authoritative commit, projection row changes and every affected
incremental structure are applied to a private candidate snapshot before the
visible frontier advances. Publication exposes all or none of those effects.
Apply is idempotent by source commit/event identity and validates an existing
apply marker before accepting replay.

Insert, replacement, optional-state transition, and deletion/retraction update
both additions and removals. Checked arithmetic or structural overflow records
one typed generation failure without advancing. No structure may be repaired by
reading current authoritative rows inside the apply transaction. Repair is a
separate bounded rebuild from an exact authoritative snapshot plus retained
tail.

Compaction may rewrite physical state but must preserve exact logical results,
frontier, generation, and independent-evaluator digest. Published structures
are immutable snapshot members; a query never observes partial mutation.

### 4. Make policy alignment part of structure identity

A structure is either:

- valid for a role-invariant public row universe proven by the compiler; or
- built for one exact policy shape and capability revision family whose
  partitioning prevents denied rows from entering shared counts, membership,
  rank, top-K repair, statistics, or timing class.

Current request authorization, field visibility, row-policy facts, and
pre-release reauthorization remain mandatory. A cached historical allow
decision is never persisted. Capability or policy changes retire the affected
provider epoch and rebuild/narrow under the new identity; they do not filter a
wider aggregate after the fact.

Hidden rows may not affect a released count, Boolean state, rank, top-K member,
exact-total metadata, cursor, lifecycle choice, public diagnostic, notification
timing class, or provider availability visible to an unauthorized principal.
When that isolation cannot be proved within bounds, the compiler refuses the
structure and retains a separately safe scan plan only if one was explicitly
compiled.

### 5. Keep state bounded and operationally honest

Each definition declares maximum state and write amplification. Admission sums
all maintained structures affected by one source change and rejects an
excessive projection definition before activation. Runtime accounts actual
entries and bytes; exceeding a bound degrades the candidate generation and
returns a typed rebuild/unavailable result rather than dropping updates,
evicting exact state, or serving stale success.

Lag, rebuild, compaction, retained-tail pressure, and state bytes use fixed
redaction-safe metrics. No metric labels a tenant, field, group, value,
predicate, structure key, or top-K member. A stuck rebuild obeys ADR-0086's
replay budget and cannot pin history without bound.

### 6. Require exact provider selection and explicit fallback

The compiler selects an incremental capability only when its semantics, policy
shape, provider epoch, bounds, and result algebra exactly satisfy the named
query. Runtime cannot substitute a nearby rollup, widen a bitmap, truncate K,
change grouping, use stale state, or convert an unavailable structure into
empty data.

A plan may contain an independently compiled V2 vectorized-scan fallback. Its
freshness, work bound, policy, and typed lifecycle behavior are part of the plan
identity. Callers cannot request or suppress fallback. If no equivalent bounded
fallback exists, unavailability remains explicit.

### 7. Defer approximation and general materialized views

This decision does not add approximate distinct sketches, quantiles,
histograms, cubes, windows, arbitrary expression indexes, user-defined
aggregates, general joins, recursive views, BM25, cross-partition rollups, or a
materialized-view query language. Each needs real consumers, exact error and
inference semantics, and a separate accepted capability/format decision.

WP-714 must first prove two neutral real-workload shapes: a ticket/work queue
using exact top-K and a run-metric dashboard using exact rollups. Framework code
stays in its owning repository; RiffDB retains only generic fixtures and a
value-free external receipt.

### 8. Set activation gates against scans and maintenance cost

Incremental state activates only if the exact workload corpus demonstrates:

- at least 3 times lower p50 and p95 query CPU for repeated rollup and top-K
  reads than the accepted V2 vectorized scan;
- no more than 1.20 times projection-apply CPU and no more than 1.50 times
  derived bytes for the registered maintained-state workload;
- bounded catch-up with projection lag p95 inside the existing declared SLO
  under the registered sustained-write profile; and
- no regression greater than five percent for projections that declare no
  incremental structures.

If query gain or maintenance bounds fail, the failed structure remains absent
from production provider capabilities. Thresholds are mechanics gates, not a
public performance promise.

## Options Considered

1. **General materialized views:** rejected because arbitrary expressions and
   joins would escape finite compiler-owned semantics and cost.
2. **Application-specific cached dashboards:** rejected because policy,
   frontier, recovery, and compatibility would fragment outside RiffDB.
3. **Approximate sketches first:** rejected because exact result and inference
   semantics are already available and approximation needs a separate product
   contract.
4. **A small exact compiler-owned structure registry:** proposed because it
   serves measured workloads while remaining rebuildable and bounded.

## Consequences

- Repeated queues and dashboards avoid rescanning unchanged projection rows.
- Derived write and storage amplification becomes an explicit deployment cost.
- Policy changes may require rebuilding policy-aligned provider generations.
- Exact deletes and replacements require counted/retraction-aware state rather
  than append-only shortcuts.
- Approximate and general analytical constructs remain deliberately absent.

## Compatibility

Incremental structures use new rebuildable provider-state identities registered
under ADR-0124. Existing authoritative, projection V1, segment V1/V2, query,
wire, generated, cursor, and application-lock bytes remain unchanged unless an
implementation package proves a least-sufficient compiler/provider descriptor
successor. Old binaries refuse unknown provider state before publication; the
prior supported scan plan and generation remain intact when declared.

## Security

Provider state is scoped to one organization and either a role-invariant row
universe or an exact policy-aligned identity. Denied rows never enter shared
result-shaping state. Structure contents, distribution, cardinality, and repair
behavior are not public. Fresh authority remains per request; persisted allow
decisions and redact-after-aggregation are forbidden.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications invoke only finite
  generated operations. They cannot request a structure, arbitrary aggregate,
  approximation, materialized view, refresh, fallback, stale success, policy
  bypass, or resource increase. Unsupported or unavailable state fails typed.
- **Scale:** structure count, groups, keys, entries, dictionaries, bitmaps,
  top-K reserve, bytes, write amplification, apply/rebuild work, lag, and
  diagnostics are compiler bounded. State is partition-scoped, incrementally
  maintained, stream-rebuildable, and never requires database-wide memory or
  co-located authoritative storage.

## Testing

- Independent bitmap, rollup, and top-K reference evaluators over randomized
  inserts, replacements, optional transitions, deletes, replay, and compaction.
- Deterministic apply/publication schedules proving all-or-none visibility,
  idempotency, exact retractions, and frontier equality.
- Crash matrices for state file, apply marker, checkpoint, manifest,
  compaction, generation publication, and retirement boundaries.
- Policy adversaries for cross-tenant, role revision, revocation, hidden group,
  hidden extreme, hidden top-K, timing class, and public diagnostics.
- State/write-amplification boundary tests, overflow degradation, bounded
  rebuild/cancellation, retention detachment, and no-structure non-regression.
- Compiler and architecture negatives for arbitrary views, unregistered
  functions, approximate substitution, scan fallback drift, framework branches,
  and caller/provider selection.
- Ticket-queue and run-metric differential/performance receipts at exact matched
  frontiers, plus value-free external-consumer confirmation.

## Requirements and Work Packages

- **Requirements:** `PRJ-001` through `PRJ-004`, `OQ-017` through `OQ-024`,
  `OQ-044` through `OQ-055`, `PERF-001`, `PERF-007`, `PERF-008`, `PERF-018`
- **Incremental structures:** `WP-714`
- **Cross-plane acceptance:** `WP-715`

## Decision Deadline

Exact human acceptance is required before WP-714 adds a provider-state tag,
bitmap, rollup state, top-K lane, compiler capability, rebuild artifact, or
query activation. Any approximate function, general view language,
cross-provider bridge, or policy relaxation requires a separate exact decision.
