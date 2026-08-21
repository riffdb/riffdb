# ADR-0134: Compiler-Declared Exact Predicate and Independent Order Families

- **Status:** Accepted
- **Direction approved:** 20 August 2026 (maintainer, in session)
- **Exact text accepted:** Yes — 2026-08-21 (maintainer, as written)
- **Decision deadline:** Before WP-656 changes RiffQL predicate syntax, exact-
  result IR, provider-state formats, or generated query surfaces
- **Requires:** ADR-0002, ADR-0051, ADR-0052, ADR-0053, ADR-0055, ADR-0108,
  ADR-0111, ADR-0117, ADR-0124, ADR-0128, ADR-0130, ADR-0131, and ADR-0133
- **Defines or blocks:** WP-656 through WP-658 and completion of external
  framework exact-result acceptance

The maintainer accepted this exact text on 2026-08-21. This ADR is now
authoritative.

## Context

ADR-0131 requires each exact-result named query to compile a finite family of
typed search, filter, total-order, exact-count, and ordinal-window choices. Its
first provider implementation deliberately activated a smaller family: one
partition equality, one exact-text predicate, at most one optional equality
filter, and an order beginning with the searched text field. That slice proves
exact substring matching, whole-population count, policy isolation, and direct
numeric offset, but it is not a complete implementation of the accepted
predicate/order model.

A real external adapter exposed the missing shape:

```riffql
where organization_id == $organization_id
  && email contains $needle
  && when $state { state != $state }
order by created_at desc, user_id asc
take $limit offset $offset
```

The searched field and ordered field are independent. Other ordinary
application lists require equality and inequality, scalar ranges, bounded
membership and non-membership, null/existence tests, exact text operators,
multiple conjunctive filters, and occasionally a small declared disjunction.
Client sorting, client filtering, counting a returned page, walking cursor
pages, or changing the order after ordinal selection produces incorrect
results. Merely removing `RDB-QP009` would therefore weaken the exact-count and
offset guarantees rather than add the missing capability.

This is not specific to an authentication framework. Administrative lists,
catalogs, queues, audit views, and projected application indexes need the same
closed result-set algebra. The design must remain usable by later columnar or
search providers without turning RiffQL into SQL, exposing a caller-supplied
query AST, or building a speculative cross-provider bitmap bridge.

## Proposed Decision

### 1. Compile one closed exact-result predicate and order program

An exact-result named query compiles these independent dimensions:

1. complete partition and policy scope;
2. zero or more compiler-declared candidate/search predicates;
3. a bounded compiler-normalized filter expression;
4. one compiler-declared total-order tuple;
5. whole-result measures, including `exact_count` when requested;
6. one bounded ordinal window; and
7. one typed output layout.

Search/filter fields and order fields need not be the same. Every field,
operator, Boolean edge, comparison profile, order direction, state-order rule,
measure, window bound, and output field is resolved to a stable compiler
identity and participates in plan and module identity. The provider descriptor
must advertise the complete resolved shape; partial support is a source-
spanned compilation error.

The normalized program is semantic compiler IR, not a public expression tree.
One provider may execute all stages internally. No stage boundary creates a
public RPC, application callback, runtime provider choice, or permission to
materialize an intermediate population.

### 2. Admit a bounded typed predicate algebra, not arbitrary predicates

The exact-result predicate vocabulary is the applicable subset of ordinary
operational RiffQL:

- equality and inequality: `==`, `!=`;
- scalar range: `<`, `<=`, `>`, `>=`;
- bounded typed set membership: `in`, `not_in`;
- state tests: `is null`, `is not null`, `exists`;
- declared text profiles: `prefix`, `starts_with`, `ends_with`, `contains`;
- conjunction; and
- compiler-capped disjunction when every branch has one exact provider plan.

There is no general unary `not`, user function, regex, wildcard, script,
subquery, caller field reference, caller order expression, or request-supplied
predicate node. `not_in` is a first-class closed operator rather than a general
negation escape. Its right-hand input is a typed set with a compiler-fixed
element and encoded-byte maximum. Binding canonicalizes set order and duplicate
values before hashing and execution.

Value predicates use one two-valued alpha truth table. A missing or explicit
null field satisfies none of `==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `not_in`,
`prefix`, `starts_with`, `ends_with`, or `contains`; state is tested only with
`is null`, `is not null`, and `exists`. A binary predicate with a null
right-hand value is a compile or bind error rather than an alias for a state
test. For a present non-null field, `in` over an empty canonical set is false
and `not_in` is true. These rules prevent complement operators from turning
missing or null rows into implicit matches and are identical in the reference
evaluator and every provider.

Each operator is type-checked. Range operators require a scalar type with one
frozen canonical comparison profile. Text operators require an explicit
versioned text profile and never inherit locale or platform collation.
Membership requires exact element-type identity. Complemented operators such
as `!=` and `not_in` are evaluated only relative to the already-authorized
partition or policy subpartition; an unauthorized row cannot enter the
complement universe.

The compiler fixes ceilings for predicate leaves, Boolean depth, disjunction
branches, optional-presence parameters, set elements and bytes, order terms,
resolved family members, provider work, and provider-state amplification.
Every optional presence combination and declared branch is compiled and
validated before deployment. One unsupported or excessive member rejects the
whole named family; there is no request-time fallback.

### 3. Make total order independent, typed, and complete

An exact-result order is a nonempty ordered tuple of compiler-resolved entity
fields and fixed directions. The tuple may use fields unrelated to candidate
generation or filtering. The compiler appends or verifies the complete
canonical entity key as an ascending unique tie-breaker, excluding an already
fixed partition component. A caller cannot omit, replace, or reverse the
tie-breaker.

Every order term names a frozen scalar comparison and missing/null placement
profile. The first activation accepts only fields the compiler proves present
and non-null across the complete installed lineage. Ordering an optional or
historically missing field requires a later explicit versioned state-placement
profile already representable by this IR; the compiler cannot invent a default.
Unsupported types, missing state profiles, and implicit locale-sensitive
ordering fail compilation. Ascending and descending reverse only the declared
value order. The canonical entity-key tie-breaker remains ascending so equal
values have one stable order.

Several allowed sort fields or directions are represented as several finite
compiled members. Generated bindings may expose separate named methods or a
closed generated choice whose values map one-to-one to those members. They do
not accept a string field name, raw direction, arbitrary order tuple, or a
generic sort object.

### 4. Require indexed set algebra and order-statistic execution

An exact provider must evaluate the complete normalized predicate before
`exact_count` and ordinal selection. It must have declared indexed capability
for candidate sets, equality/range/membership sets, authorized complement,
Boolean combination, each total order, exact cardinality, and ordinal seek.
Execution may use provider-internal postings, bitsets, trees, rank/select
summaries, or another bounded physical algorithm, but the representation is not
public or frozen by this ADR.

The physical implementation must satisfy all of these conditions:

- predicate combination operates over provider-owned indexed identities, not
  authoritative entity scans or returned-row filtering;
- exact cardinality is derived without materializing the matching population;
- ordinal selection seeks through maintained order/cardinality information and
  does not walk skipped rows or pages;
- ordering happens before ordinal selection and returns only the requested
  bounded page;
- fetching or decoding output values is proportional only to the requested
  page, except for separately declared bounded policy evidence; and
- static charge and runtime fuel cover worst-case index probes, set operations,
  rank/select blocks, output, and diagnostics independently.

A provider-internal indexed-set representation is not ADR-0130 cross-provider
composition. This package adds no bitmap, row-ID, or materialization transfer
between engines. A future columnar, lexical, or other provider may advertise
the same semantic program only after implementing its own exact capability and
reference conformance; compatible descriptor shapes alone do not connect the
providers.

### 5. Keep policy, count, order, and offset on one provider epoch

Partition-scoped provider state remains the primary policy mode. A compiler-
proved policy-aligned subpartition is second. Bounded row admission is allowed
only when its complete admitted universe fits the provider's declared ceiling
and admission finishes before candidate sets, complements, counts, order
statistics, ordinals, cursors, work-class decisions, or diagnostics are
formed.

The selected predicate member, total order, exact total, and ordinal page share
one ADR-0130 provider-plan proof and one exact servable epoch. A concurrent
write cannot make the total describe a different population from the returned
page. Fresh authentication, authorization, capability revision, and row/field
policy checks retain their existing request and release safe points.

Descriptor validation remains once per deployment/catalog generation and
provider-plan identity. Epoch negotiation remains once per opened result set.
Predicate layout, order layout, complement-universe proof, and rank/select
verification are constructed once per plan and epoch, never once per row,
predicate result, skipped ordinal, page item, or provider operation.

### 6. Preserve a finite generated application surface

Application authors gain flexibility by writing named RiffQL documents and
declaring indexes/projection profiles. Application callers receive generated
typed operations for only the finite compiled family. Requests carry typed
scalar or bounded-set values, optional-presence choices, bounded limit and
offset, and at most a closed generated member choice. They never carry a field,
index, provider, policy mode, comparison profile, raw operator, predicate AST,
order AST, cost hint, scan flag, fallback, or freshness downgrade.

An external adapter may dispatch its own finite operator or sort enum to the
corresponding generated method/member. That dispatch chooses among already
compiled safe operations; it does not authorize adapter-side filtering,
sorting, counting, page walking, raw RiffQL, or storage access.

Rust, Go, TypeScript, Python, CLI, and MCP use the same application service,
module/plan identity, parameter bounds, exact total, page, epoch proof, and
typed errors. ADR-0133 compact carriage may encode eligible result rows, but it
does not alter predicate, order, count, policy, or epoch semantics and cannot
be selected by the application.

### 7. Add successor identities and retain the narrow family exactly

WP-656 must introduce least-sufficient successor identities for every changed
source-language, query IR, exact-result plan, query module, generated surface,
and provider-state boundary. Exact provider state carrying independent order
and predicate algebra is a new rebuildable-state identity; it does not
reinterpret V1 through V3 checkpoints.

Existing exact-result sources, modules, hashes, locks, checkpoints, generated
methods, and canonical bytes remain readable and byte-exact. The compiler
continues to emit the oldest identity that represents a query without loss.
Moving an installed query to the richer family requires ordinary compilation,
deployment, provider rebuild/catch-up, readiness proof, and typed refusal until
the new state is servable. Decoder retirement follows ADR-0124 and is not part
of WP-656 through WP-658.

### 8. Keep framework evidence external and correct the acceptance ledger

RiffDB owns only generic language, compiler, provider, policy, service,
transport, generated-surface, and framework-neutral conformance artifacts.
WP-658 must contain no Better Auth schema, role, route, generated profile,
adapter runtime, or framework-specific branch.

The owning external adapter repository must pin an immutable RiffDB development
publication and prove its real route/operator/order matrix. Until that evidence
is complete, the external portion of `OQ-030` remains outstanding regardless of
historical WP-648 wording. RiffDB may publish a generic capability receipt, but
must not claim the external route passed based on a copied fixture or partial
adapter profile.

## Options Considered

1. **Remove the current searched-field-first compiler check:** rejected because
   current provider postings cannot produce a different total order before
   ordinal selection.
2. **Generate every possible application query dynamically at runtime:**
   rejected because caller structure defeats static index, policy, cost, and
   compatibility proofs.
3. **Add framework-specific list methods:** rejected because route vocabulary
   would leak into durable provider state and first-party runtime code.
4. **Permit bounded authoritative scans and in-memory sort:** rejected because
   cost grows with the population, count/offset become a materialize-all path,
   and large partitions cannot preserve the public safety boundary.
5. **Compile a bounded generic predicate/order program into provider-owned
   indexed set algebra:** proposed because it supports real query flexibility
   while keeping structure, bounds, policy, and execution compiler-owned.

## Consequences

- Named queries can search one field, filter other fields, and sort by an
  independent deterministic tuple while preserving exact count and offset.
- Equality, inequality, ranges, bounded membership/non-membership, state tests,
  exact text, conjunction, and capped disjunction share one typed semantic
  model rather than accumulating use-case branches.
- Index state and write/rebuild amplification can grow with declared predicate
  and order families. The compiler must reject excessive families rather than
  hiding that cost at query time.
- Some semantically valid RiffQL remains unavailable when no provider can prove
  its exact indexed execution. Flexible authoring does not imply universal
  execution.
- BM25, relevance scoring, facets, ad-hoc columnar querying, cross-provider
  composition, SQL, joins, regex, and caller-supplied query structure remain
  outside this campaign.

## Compatibility

This Proposed ADR changes no bytes or public behavior. If accepted, WP-656
through WP-658 add source-language and executable-IR successors, exact-result
plan/module identities, generated-surface fixtures, and an additive exact
provider-state format registered under ADR-0124. Existing decoders remain
active, existing narrow plans keep their identities, and no persisted V1--V3
checkpoint is rewritten in place.

No public Protobuf query AST or generic filter/sort message is added. Any
generated closed member choice must use the existing typed named-query
parameter/result envelope or an independently reviewed additive wire identity;
it cannot repurpose a field or silently change old-client behavior.

## Security

All candidate, filter, complement, count, order, ordinal, and work statistics
are scoped to the complete authorized partition or policy subpartition.
Unauthorized rows cannot affect presence, totals, order, offsets, timing/work
class, lifecycle choice, or diagnostics. Complement operators never complement
against global or pre-policy state. Secret outputs still require ADR-0128
authority and are not made visible by being searchable, filterable, or ordered.

Public errors identify a closed unsupported capability, type, bound, lifecycle,
or freshness reason and a safe remedy. They do not reveal hidden values,
matching counts, partition sizes, policy facts, index layout, or which denied
row changed a complement or order.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications invoke only finite
  generated named operations with typed bounded values. They cannot submit
  fields, operators, Boolean trees, order expressions, provider choices,
  scans, client fallback, or weakened freshness. Every complete family member
  is proven before deployment, and unsupported members fail closed.
- **Scale:** predicate leaves, branches, family members, input sets, provider
  probes, indexed-set work, rank/select work, state amplification, page rows,
  output bytes, rebuild work, and diagnostics are statically bounded.
  Execution does not require database-wide memory, a full matching result,
  co-located authoritative storage, or a full-state rewrite.

## Testing

- Source-span snapshots and semantic assertions for every operator, type,
  optional combination, Boolean bound, order tuple, missing index, unsupported
  provider capability, and excessive state/work family.
- Canonical predicate/order IR, plan/module/hash/lock, generated schema, and
  V1--V3 compatibility fixtures with topology checks for every successor.
- Independent reference-evaluator properties over randomized create, update,
  delete, null/missing, Unicode, set canonicalization, range boundary,
  duplicate-value, disjunction, and mixed-direction order cases.
- Provider equivalence for search-field/order-field independence, exact
  cardinality, direct ordinal zero/end/beyond-end, concurrent writes, rebuild,
  compaction, restart, corruption, cancellation, and maximum bounds.
- Policy adversaries for partition isolation, policy subpartitions, bounded row
  admission, complemented predicates, revocation, and inference-safe errors.
- Architecture negatives forbidding entity scans, page walks, result filtering,
  request-time sorting, materialize-all, per-row proof construction, caller
  structure, framework branches, and cross-provider bitmap/ID transfer.
- Framework-neutral Rust, Go, TypeScript, Python, CLI, and MCP fixtures covering
  every activated operator class and independent order shape from one corpus.

## Requirements and Work Packages

- **Requirements:** `OQ-001` through `OQ-006`, `OQ-009`, `OQ-010`, `OQ-015`
  through `OQ-017`, `OQ-019` through `OQ-024`, and `OQ-026` through `OQ-030`
- **Compiler and semantic model:** WP-656
- **Provider and execution:** WP-657
- **Generated surfaces and generic evidence:** WP-658
- **External framework evidence:** owned by the external adapter repository

## Decision Deadline

Exact human acceptance is required before WP-656 edits SPEC, RiffQL syntax,
query IR, module/plan identities, provider-state formats, generated surfaces,
or compatibility fixtures. A physical design unable to provide exact count and
direct ordinal selection for the complete declared predicate/order family must
return for review rather than narrow the accepted source silently.
