# ADR-0150: Compiler-Sealed Operational Access-Path Algebra and Bounded Relationship Composition

- **Status:** Accepted
- **Direction approved:** Yes
- **Exact text accepted:** Yes, 2026-08-25
- **Accepted:** 2026-08-25
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for commit `a9093c56`
- **Decision deadline:** Before WP-687 broadens ordinary index-component
  eligibility or physical predicate lowering
- **Requires:** ADR-0038, ADR-0051, ADR-0053, ADR-0054, ADR-0055, ADR-0108,
  ADR-0111, ADR-0124, ADR-0129, ADR-0130, ADR-0133, ADR-0134, and ADR-0145
- **Defines or blocks:** WP-687 through WP-690; companion aggregate work is
  governed by ADR-0152 and WP-693 through WP-696

This record is authoritative for WP-687 through WP-690.

## Context

Ordinary bounded RiffQL already has the language vocabulary needed by several
real applications: typed equality, bounded membership, ranges, null/existence,
binary prefix matching, total index order, opaque cursors, and bounded
dependent complete-key batches. RiffDB also has three physical operational
index-component encodings: canonical values, presence-aware values, and
versioned text keys.

The implementation still decides compatibility through several local rules.
The compiler separately asks whether a predicate appears on an index, whether
an equality or membership advances the leading prefix, and whether the
remaining fields match the declared order. The executor independently infers
how logical predicate values become physical index-prefix bytes. Memory and
redb then independently sort, deduplicate, resume, and traverse the resulting
prefix ranges.

The same audit exposes a more serious latent class of mismatch. The ordinary
planner can recognize canonical range or inequality predicates as index-
associated while the current prefix builder stops before those predicates and
the executor retains nonmatching rows only after the backend has selected a
page. Applying a logical predicate after page limit or continuation selection
can under-fill a page, omit later matches, and mint a cursor from the wrong
candidate population. Accepted language vocabulary is not proof that every
operator/encoding cell already has a correct physical implementation.

That structure has produced a sequence of individually small but related gaps:

- a binary text-key component could serve a prefix predicate but not ordinary
  bytewise order;
- after ordering was enabled, it could not serve exact equality in a narrower
  query; and
- after equality was enabled, it still could not serve bounded membership
  while providing the query order.

The last shape is a finite union of exact keys:

```riffql
where organization_id == $organization_id
  && object_id in $object_ids
order by object_id asc
take 25 after $after
```

Duplicating a canonical index for each newly discovered combination is not a
general answer. It increases write, validation, rebuild, and retained-state
amplification and can make an otherwise bounded atomic command exceed its
compiler-owned index-work budget. Removing an order, filtering after `take`,
or walking cursor pages would instead make results incorrect.

The repeated examples now satisfy the rule of three: existing operational
applications exercise canonical membership and dependent batches, an exact-
result consumer exercises independent filters and orders, and a tuple-oriented
consumer exercises binary text order, equality, and membership. The shared
lesson is not that RiffDB needs SQL or a runtime optimizer. It needs one
compiler-owned algebra describing how a declared physical component may be
used by a closed named query.

ADR-0130 and ADR-0134 already govern projection-backed whole-result set
algebra, exact counts, facets, ordinal windows, and future search providers.
This ADR governs ordinary authoritative row-store index traversal. The two
planes share typed logical comparison profiles and safety invariants, but they
do not acquire a shared runtime, cross-provider bridge, or interchangeable
cost model.

RiffQL also already exposes bounded `count`, `sum`, `min`, `max`, and group-by,
while the columnar plane implements the same small fold set. Access-path
consolidation must not hard-code that list into a second planner vocabulary or
pretend that a fold over one bounded page is a whole-population measure.
ADR-0152 is the companion aggregate-function decision for this broader query
capability program. It expands the exact fold vocabulary and freezes how
ordinary bounded folds, projection-backed measures, grouping, null semantics,
precision, and policy compose without delaying WP-687's real membership
blocker.

## Proposed Decision

### 1. Define one closed operational component-capability registry

The first-party compiler owns one bounded registry mapping each operational
index-component encoding and profile to the exact roles it can prove. A role is
compiler vocabulary, never a caller option:

- partition or leading exact equality;
- finite bounded membership;
- an exact lower and/or upper range bound;
- finite ordered complement ranges;
- null, non-null, or existence state selection;
- leading-byte prefix selection;
- ordered value production; and
- deterministic unique tie-breaking.

Roles compose only where the registry explicitly permits the combination. A
component may provide membership selection and the first remaining order term
in one plan; it is not forced into one mutually exclusive category. The
compiler still selects one declared index and one complete shape. Applications
cannot select a role, encoding, profile, index, range strategy, or fallback.

The initial executable matrix is:

| Component encoding | Exact | Bounded `in` | Range/complement | Prefix | Order |
|---|---:|---:|---:|---:|---:|
| Canonical scalar | yes | yes, when it supplies the first remaining order term | only after WP-688 installs typed physical interval execution | no | canonical |
| Presence-aware | state-specific only | no | no | no | only with explicit accepted state placement |
| `binary_utf8_v1` text key | yes | yes, when it supplies the first remaining order term | no | exact leading bytes | exact UTF-8 bytes |
| `unicode_fold_v1` text key | unavailable | unavailable | unavailable | unavailable | unavailable |

`binary_utf8_v1` membership is a finite union of that profile's exact equality,
not a canonical string range and not full-text search. Logical `<`, `<=`, `>`,
and `>=` retain RiffDB's frozen canonical typed comparison and therefore cannot
silently use bytewise text-key order. Suffix, substring, token, relevance,
facet, count, and ordinal semantics remain in their accepted exact-result or
projection-provider planes.

Until a matrix cell has compiler, lowering, storage, cursor, and conformance
evidence, it is unavailable in the ordinary row-store plane even if its syntax
is accepted for another provider. WP-687 must enumerate every currently
planner-admitted ordinary cell and make any unproved cell fail compilation
before it can reach page selection. WP-688 may reactivate accepted canonical
range, inequality, and complement cells only with physical interval execution
that applies the complete predicate before page and cursor formation.

Adding an encoding, profile, role, role combination, or comparison meaning
requires an accepted amendment or independently accepted ADR. A backend may
not advertise a local superset that the compiler registry does not name.

### 2. Compile a complete access shape, not independent eligibility facts

For every ordinary index access, the compiler proves one complete access shape
containing:

- the exact partition-bound leading component;
- every additional exact leading component;
- at most one finite branching or interval component for bounded membership,
  presence state, an accepted leading-byte prefix, a typed scalar range, or a
  finite ordered complement;
- the complete remaining total-order suffix and wholly forward or wholly
  reverse direction;
- the deterministic unique tie-breaker;
- the logical-to-physical lowering profile for each consumed component;
- the physical range-count and encoded-input maxima;
- the page, continuation-probe, scan, point-read, intermediate, output, and
  diagnostic ceilings; and
- the complete entity, field, index, output, and row-policy authority.

The ordinary row-store plane initially permits at most one branching or
interval component. It does not form Cartesian products of several `in` sets,
disjunction branches, or state ranges. A later real query requiring a bounded
product or overlapping range union must return for review with an explicit
product bound, merge algorithm, cost proof, cursor identity, and storage
conformance.

Every logical query predicate must be consumed by the selected access shape or
by an already accepted bounded policy mechanism. An application predicate
cannot become a residual filter after `take`, continuation, ordering, or scan
admission. The compiler rejects incomplete orders, mixed directions,
unconsumed predicates, missing partition proofs, and unsupported physical
profiles with source-spanned safe remediation.

WP-687 and WP-688 materialize this shape from the existing exact query program,
selected key schema, and component encodings. They must not add a new grammar,
query-IR, module, cursor, storage-key, durable-state, wire, or generated-surface
identity merely to restate facts already present. If implementation proves
that the existing program cannot carry or reconstruct one required proof
without runtime access-path discovery, work stops for an ADR-0124-classified
successor rather than adding an implicit side channel.

### 3. Lower one bounded physical range set once per step and request

After typed parameter validation and canonical set normalization, the executor
materializes one transient compiler-sealed ordered physical range set for each
index step. A range has checked inclusive/exclusive lower and upper endpoints
or one exact component prefix; unbounded sides are confined to the already
partition-bound selected index. The lowering step:

1. validates the access shape once;
2. lowers each exact scalar or set member through the selected component
   profile exactly once;
3. enforces the compiler-fixed member-count and aggregate encoded-byte bounds;
4. forms exact prefixes or typed interval endpoints before page selection;
5. sorts physical ranges in the selected byte order;
6. rejects overlaps, or safely coalesces/deduplicates them only where the
   frozen operator/profile truth table proves equivalence;
7. rejects any query predicate that remains for post-page filtering; and
8. produces one bounded immutable range schedule for the storage read.

No compatibility proof, profile lookup, normalization, range construction, or
set sorting occurs per row, page item, point read, or storage operation. This is
ADR-0129's pay-once rule applied to reads. Row predicate evaluation and public
results retain logical typed values; physical bytes do not replace strings in
returned rows or application-visible parameters.

The executable `binary_utf8_v1` transform is injective over valid canonical
UTF-8, so distinct logical set members produce distinct physical exact
prefixes. A future non-injective profile must define its equality truth table,
collision handling, original-value return strategy, and compatibility fixtures
before it becomes executable. A covering index may return a transformed key as
the original logical field only when the profile has an exact checked inverse;
otherwise it must carry the authorized original value separately or be
ineligible for that covered layout.

### 4. Make multi-range order, continuation, and work one shared contract

For membership, the finite exact prefixes are disjoint and the membership
component is the first remaining order term. For canonical inequality and
complement, WP-688 may form a finite set of nonoverlapping intervals only when
that same component supplies the first remaining order term. Sorting the
ranges physically and traversing each in the same direction therefore produces
the complete global order without a heap merge or materializing the result
set. Reverse traversal reverses both range order and each range.

Memory and redb consume the same bounded range schedule and shared continuation
rules. They may retain storage-specific range primitives, but may not maintain
different semantic algorithms for prefix order, range skipping, exact-bound
exclusion, plus-one probing, or continuation across range boundaries.

One page limit, one continuation probe, and one scan/fuel budget apply across
the complete range union, not separately to every member. The opaque cursor
contains or registry-binds the last returned physical key and exact index epoch;
the existing plan and canonical parameter hashes bind the selected operation,
direction, profile, and complete normalized set. Changing set membership,
order, capability revision, module/plan, database history, or index epoch fails
closed rather than resuming in a nearby range.

An empty set retains the accepted typed `in` truth semantics and returns an
exact empty page without a storage scan. Duplicate submitted members are
removed by canonical parameter binding before range work. Maximum-size sets,
maximum encoded members, and the page that crosses a prefix boundary remain
bounded and deterministic.

This ADR does not build a generic k-way merge, overlapping range union, index
intersection engine, adaptive optimizer, skip scan, or bitmap bridge. Those
are added only when a real accepted query cannot use disjoint monotone ranges.

### 5. Charge index flexibility where work actually grows

Static query cost and runtime fuel separately account for:

- logical set elements and aggregate encoded input bytes;
- physical range count and range-schedule bytes;
- storage range probes;
- inspected index rows across all ranges;
- row-policy evaluation and entity hydration;
- continuation probing and cursor bytes; and
- returned values and encoded output bytes.

The compiler uses the declared maximum, not a typical submitted set. Runtime
uses the actual canonical set only to remain within that maximum; a smaller set
cannot widen any other budget. Prefix validation and descriptor checks are
per-plan or per-step/request costs, never per-row costs. No new query feature
may hide write amplification by requiring duplicate indexes, or hide read
amplification by multiplying the scan ceiling per range.

Partition routing remains exact before range expansion. Row policy is applied
inside the one authoritative snapshot before a row, aggregate, or continuation
is released. Authorization covers the complete logical predicate, order,
selected index, selected and predicate fields, and maximum work. Diagnostics
may name safe source symbols, the unsupported role, and bounded actual/maximum
integers; they reveal no parameter value, member text, row, match count, hidden
index, or policy fact.

### 6. Share semantic vocabulary with providers without sharing runtimes

The ordinary component registry and ADR-0130 provider descriptors use the same
frozen logical meanings for equality, membership, range, state, text profile,
order, and tie-breaking. Conformance must detect semantic drift between them.
They remain different execution contracts:

- ordinary access traverses one authoritative partition-local row-store index
  with a bounded page and opaque cursor; and
- projection providers own candidate sets, whole-result measures, facets,
  ranking, ordinal windows, provider epochs, and rebuildable state.

A columnar, vector, lexical, or future BM25 provider does not consume the
ordinary physical range schedule. An ordinary index does not acquire exact
whole-population count, facets, relevance, or offset merely because both planes
spell `in` or `order`. No bitmap, row-ID, candidate, score, or materialization
bridge is added by WP-687 through WP-690.

### 7. Keep relationship composition bounded and separately visible

The access-path algebra describes one physical access step; it is not itself a
join language. ADR-0054's bounded dependent complete-key batch remains the
first relationship composition and must be represented in the conformance
inventory as a bounded driver plus ordered point targets, not as an index-range
exception.

Future relationship work may compose access shapes only when all of these are
compiler-proved:

- the relationship or key mapping is declared and type exact;
- every access remains in the same complete partition and one read snapshot;
- the driving collection has a fixed positive bound;
- each target is a complete primary-key point or a separately bounded declared
  index access;
- fan-out per edge, total intermediate rows, access steps, point/range probes,
  bytes, and output are independently capped;
- missing, duplicate, null, and policy-denied targets have explicit semantics;
  and
- all dependencies and authority remain visible in source, explain output,
  plan identity, and cost.

WP-689 audits existing point dependencies and dependent batches against this
model and adds generic conformance where current accepted behavior lacks it. It
adds no new relationship syntax or runtime operator. A real consumer requiring
an indexed semijoin, correlated existence test, bounded one-to-many expansion,
or another new dependency shape must supply that exact shape and receive an
accepted amendment before implementation.

Arbitrary joins, caller-provided join conditions, cross-partition joins,
Cartesian products, unbounded nested collections, recursive traversal,
materialize-all intermediates, and a cost-based runtime join optimizer remain
forbidden.

### 8. Sequence the foundation around the real alpha blocker

WP-687 is the first implementation slice. It introduces the single component-
capability registry and pay-once physical lowering helper, audits every
currently admitted ordinary predicate against pre-page physical execution,
makes unproved cells fail closed, then proves bounded `binary_utf8_v1`
membership plus order as its first new accepted matrix cell. It must be small
enough to unblock the external tuple adapter and cannot wait for speculative
relationship or provider work.

WP-688 consolidates ordinary compiler and memory/redb range scheduling around
the registry, installs physical canonical range/inequality/complement intervals
where the complete ordered shape is provable, deletes duplicated semantic tests
or branches, and completes the canonical, presence-aware, and binary-text
conformance matrix. It does not broaden the matrix beyond behavior accepted by
ADR-0108 and this ADR.

WP-689 performs the bounded relationship inventory and conformance audit
described above. WP-690 runs generic cross-surface and external-consumer
acceptance and documents access-path extension rules. ADR-0152/WP-693 through
WP-696 provide the aggregate-function track of the same query-capability
program without making either track a hard dependency of the other's first
real slice. External adapter source, schemas, routes, and generated profiles
remain in their owning repositories; RiffDB retains only generic fixtures and
capability evidence.

Paper acceptance and WP-687 may proceed without displacing standing reliability
or release gates already in flight. Later consolidation packages may not become
the reason an otherwise-ready alpha blocker waits. This sequencing permits the
real membership case to validate the abstraction before broader migration.

### 9. Repair unproved existing plans fail closed

WP-687 must inventory every repository query, exact lock, active module
fixture, and generic adapter fixture whose ordinary plan contains a non-equality
predicate. For each predicate it must prove that storage applies the physical
constraint before page and continuation selection. A plan without that proof
is unsafe even if an older compiler emitted it.

The new runtime must reject an unproved existing plan before storage access as
`RDB-QUERY-0102` (`QueryUnavailable`) with the existing bounded
`refresh_contract`/active-module recovery guidance. Installation and
reconciliation must refuse to newly activate the same unproved plan. The
runtime may decode and identify the old artifact for compatibility diagnosis;
decodability does not authorize execution. Recompilation either produces the
same proved plan, produces a new ordinary plan after WP-688 installs interval
execution, or returns a source-spanned unsupported-capability diagnostic.

This is an explicit pre-alpha correctness repair. It is not permissible to
keep an unsafe plan executable merely to preserve historical acceptance, and
it is not permissible to relabel a post-page residual filter as an index
access. WP-687 must report the complete affected repository inventory and any
required external application rebuild before merge. If an affected artifact
cannot use an existing typed refusal and ordinary redeployment path, work stops
for a separately classified compatibility decision.

## Options Considered

1. **Continue adding planner conditionals for each rejected query:** rejected
   because capability, lowering, cursor, and storage semantics remain
   distributed and the next operator/encoding combination will repeat the
   exercise.
2. **Require one canonical index per filter/order combination:** rejected
   because write, validation, rebuild, and durable-state amplification can
   exceed bounded atomic-command budgets even when one physical encoding has
   the required semantics.
3. **Expose a SQL-like optimizer or caller-selected access path:** rejected
   because plans, cost, policy, and behavior would become request-time choices
   outside the safe application boundary.
4. **Unify row-store and projection-provider execution:** rejected because
   authoritative page traversal and projection result-set algebra have
   different state, freshness, count, ranking, and availability contracts.
5. **Freeze the capability algebra now and implement it through real slices:**
   proposed because semantics become composable and reviewable without a
   speculative universal runtime or durable format.

## Consequences

- A declared index can serve every compiler-proved compatible query shape
  instead of requiring semantically redundant indexes.
- New physical encodings and operators have one explicit integration and test
  matrix rather than several scattered planner/executor/storage branches.
- Membership over an ordered binary text key becomes the first real proof of
  composed selection and ordering roles.
- Range scheduling, continuation, and work accounting become one cross-storage
  contract.
- Some valid logical queries remain rejected when they require multiple
  branching components, overlapping unions, residual filtering, a missing
  provider capability, or unbounded relationship fan-out.
- Currently planner-admitted ordinary shapes without pre-page physical
  predicate execution become source-spanned compilation failures until WP-688
  supplies the required interval proof; this is a correctness repair, not a
  silent semantic narrowing.
- General joins and speculative provider bridges remain deferred.

## Compatibility

This Proposed ADR itself changes no current bytes or public behavior. WP-687
and WP-688 use existing grammar, predicate tags, query-program fields,
text-key profiles, key codecs, cursor binding, and storage index bytes. Existing
proved sources, plans, modules, locks, generated clients, cursors, and persisted
index state retain their exact identities, bytes, and behavior. Newly admitted
sources receive their ordinary distinct plan/module identities without
reinterpreting an old source.

An older plan whose predicate was never physically applied before page
selection remains byte-decodable but becomes non-executable as described in
Decision 9. A previously accepted source relying on that defect must be
recompiled after WP-688 or will receive a source-spanned refusal. This is a
public semantic compatibility change explicitly accepted by this ADR, with an
inventory, release note, typed refusal, and ordinary application redeployment
path; it does not change or silently reinterpret the artifact's bytes.

The component registry, compiler access shape, and bound physical range set are
internal, bounded, nonserializable witnesses unless implementation proves that
an existing executable artifact cannot carry the necessary proof. Any new or
changed grammar, canonical IR, module format, cursor token, key codec, durable
state, provider descriptor, wire field, or generated public surface requires a
classified ADR-0124 successor and exact compatibility fixtures before merge.

No existing index is rebuilt or duplicated merely to activate the new planner
cell. A deployment adding a new declared index still follows ordinary contract
migration and index-generation rules.

## Security

Applications continue to invoke finite generated named queries with typed
bounded parameters. They cannot supply fields, operators, orders, indexes,
encodings, profiles, access roles, ranges, merge strategies, costs, policies,
or fallback behavior. Every selected access shape is authorized and bounded
before storage access; runtime mismatch fails closed as an internal integrity
error and releases no partial page or cursor.

Physical parameter bytes and set members remain private. They are never placed
in source diagnostics, metrics, tracing, MCP text, or public errors. Fresh
authentication, capability revision, row/field policy, secret-output authority,
and release-time authorization checks remain unchanged. A broader index role
does not broaden field visibility or kernel access.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** the public surface remains a
  finite named operation with typed bounded scalar/set values and an opaque
  cursor. The compiler, not the caller, selects the complete access shape.
  Unsupported roles, combinations, bounds, relationship fan-out, and runtime
  proof mismatches fail closed without scan, client shaping, or fallback.
- **Scale:** registry and access-shape work are bounded by declared index fields
  and plan steps. Per-request lowering is bounded by the compiler-fixed set
  count and bytes; traversal is bounded by one global scan/page/fuel budget.
  The design requires no database-wide memory, full-state rewrite,
  materialized matching population, co-located projection provider, or
  cross-partition work.

## Testing

- A table-driven compiler matrix for every canonical, presence-aware, and
  binary-text encoding/operator/order role, plus source-span snapshots for
  every unsupported cell and incomplete shape.
- A negative audit proving no ordinary range, inequality, membership, state,
  or text predicate can be applied only after backend page or cursor selection.
- Property tests proving logical values lower to the same physical bytes as
  index maintenance, with empty, duplicate, maximum-count, maximum-byte,
  Unicode, invalid-type, and physical-collision cases.
- Forward and reverse one-row cursor pages across several membership prefixes,
  including values such as `doc6` and `doc-3` whose canonical input order and
  bytewise index order differ; no duplicate, omission, restart, or per-prefix
  page budget is permitted.
- Memory/redb parity for uncovered and eligible covered reads, row-policy
  admission, exact-end versus plus-one continuation, range-boundary resume,
  stale epoch, changed parameter set, cancellation, and scan-fuel exhaustion.
- Architecture checks proving one capability registry, one logical/physical
  lowering owner, no storage-local profile transform, no per-row access-shape
  validation, and no external-framework branch.
- Existing dependent-point-batch conformance plus negative same-partition,
  cardinality, missing-target, duplicate, null, policy, and whole-query-cost
  cases.
- A generic application fixture and external repository rerun proving the new
  capability removes redundant-index pressure without adding adapter artifacts
  to RiffDB.

## Requirements and Work Packages

- **Future requirements after exact acceptance:** `OQ-032` through `OQ-043`
- **Capability registry and first binary-membership proof:** WP-687
- **Physical interval execution and compiler/executor/storage consolidation:**
  WP-688
- **Bounded relationship inventory and conformance:** WP-689
- **Generic and external acceptance, documentation, final evidence:** WP-690
- **Companion aggregate-function registry and expansion:** ADR-0152, WP-693
  through WP-696

## Decision Deadline

Exact human acceptance is required before WP-687 changes the ordinary
component-capability matrix or admits `binary_utf8_v1` membership. A package
requiring a new public, executable, durable, storage, cursor, wire, provider,
or relationship identity must stop for classified human review rather than
broaden this internal foundation silently.
