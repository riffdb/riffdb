# ADR-0145: Compiler-Declared Nullable Total-Order Placement

- **Status:** Accepted
- **Direction approved:** 2026-08-24 (maintainer, in session)
- **Exact text accepted:** Yes, 2026-08-24 (maintainer, in session)
- **Decision deadline:** Before WP-675 changes RiffQL order syntax, exact-order
  IR, provider-state formats, or generated query surfaces
- **Requires:** ADR-0002, ADR-0051, ADR-0052, ADR-0053, ADR-0055, ADR-0108,
  ADR-0111, ADR-0117, ADR-0124, ADR-0128, ADR-0130, ADR-0131, and ADR-0134
- **Amends:** ADR-0134 Section 3's deliberate deferral of ordering optional or
  historically missing fields
- **Defines or blocks:** WP-675 through WP-677 and resumption of the external
  adapter's nullable-sort acceptance

Direction, package boundaries, and this exact text were accepted on
2026-08-24.

## Context

ADR-0134 established compiler-declared exact predicate and independent order
families. It deliberately limited its first activation to order fields proven
present and non-null across the installed lineage, while reserving a versioned
state-placement profile in semantic IR. WP-656 through WP-658 implemented and
published that generic capability as RiffQL V6, query IR V9, query-module V9,
and exact-provider state V4.

A real external adapter then exercised an ordinary list shape that the first
activation cannot express. Its public API permits sorting by optional scalar
fields, including optional text and timestamp fields. RiffDB correctly refuses
such a named query with `RDB-QP009` because neither source nor V4 provider state
declares where missing and null values belong. Restricting the adapter, sorting
after retrieval, walking pages, or converting the refusal to an empty page
would produce incorrect exact totals and numeric-offset semantics.

The missing capability is generic. Catalogs sort by an optional subtitle,
queues by an optional completion time, inventories by an optional external
code, and administrative lists by optional metadata. The database must provide
one deterministic total order across present values, explicit nulls, and fields
absent from historical records without inheriting a backend, locale, language,
or adapter default. It must do so inside the existing compiler-owned finite
family, exact-count, direct-ordinal, policy, and single-provider-epoch
boundaries.

## Decision

### 1. Add explicit per-term `nulls first` and `nulls last` source semantics

RiffQL V7 adds an optional state-placement clause to an ordinary exact-result
order term:

```riffql
order by subtitle asc nulls last, created_at desc, record_id asc
```

The only admitted spellings are `nulls first` and `nulls last`. Placement is
part of the compiler-owned named query, not a request parameter. It is resolved
with the field, comparison profile, direction, and complete ascending entity-
key tie-breaker and participates in source, module, plan, lock, provider, and
generated-surface identity.

Omitting the clause is valid only when the compiler proves that every admitted
member's normalized predicate and complete installed schema lineage restrict
that order field to a present, non-null value. An optional or historically
missing field without such a proof is a source-spanned `RDB-QP009` error that
offers the two explicit clauses. An explicit placement clause is permitted on
a currently required field and remains semantically significant across later
compatible lineage; the compiler does not erase it as redundant. The canonical
entity-key tie-breaker is intrinsically present and non-null and cannot carry a
placement clause.

Several nullable sort fields, directions, or placements remain several finite
compiled query members. A request cannot choose a field name, raw direction,
placement string, order tuple, comparison profile, or expression.

### 2. Freeze one `NoValue` order class

For ordering only, a historically missing field and a present field containing
explicit null are members of one `NoValue` class. They compare equal for that
order term. Subsequent declared order terms are then compared, followed by the
complete ascending entity key. State predicates retain their ADR-0134 truth
model and may distinguish field states; this ADR does not change predicate
semantics or stored values.

For one term, the total-order rules are:

| Placement | Left state | Right state | Result before direction |
|---|---|---|---|
| either | `NoValue` | `NoValue` | equal; continue to the next term |
| `nulls first` | `NoValue` | value | left first |
| `nulls first` | value | `NoValue` | right first |
| `nulls last` | `NoValue` | value | right first |
| `nulls last` | value | `NoValue` | left first |
| either | value | value | use the frozen scalar comparison profile |

Ascending or descending direction changes only the comparison of two present
non-null values. It never reverses the declared state-class placement. Thus
`desc nulls last` keeps `NoValue` after all values while reversing only value
order. This rule is independent of SQL dialect, host language, storage engine,
locale, and index encoding.

Folding missing and explicit null into one order class avoids exposing schema-
history state through rank and keeps this first real-consumer profile narrow.
A future requirement to order those states separately needs a new explicit
profile and ADR; an implementation package cannot assign an undocumented
suborder.

### 3. Add semantic IR successors without reinterpreting existing artifacts

WP-675 introduces RiffQL V7, query IR V10, and query-module V10 as the least-
sufficient successors at the topology current when this ADR was drafted. The
semantic order-term successor contains a closed state-placement identity with
`PresentOnlyV1`, `NullsFirstV1`, and `NullsLastV1`; it does not encode backend
key bytes. `PresentOnlyV1` is valid only with the complete compiler proof from
Section 1.

Existing RiffQL V6, query IR V9, query-module V9, plan/module hashes, locks,
fixtures, and generated surfaces remain readable and byte-exact. The compiler
continues to emit those identities when the source omits placement and the
present/non-null proof succeeds. A source term with an explicit placement uses
the successor identities even when the current schema makes the placement
vacuous. No old decoder or semantic enum is extended in place.

Topology allocation is re-audited immediately before implementation. If an
accepted intervening change has consumed one of these numeric successors,
WP-675 must stop for an administrative renumbering; it may not reuse or
reinterpret the occupied identity.

### 4. Build additive provider-state V5 order statistics

Exact-provider state V5 represents each declared order term as a closed state-
class rank followed, only for a value, by that field's existing canonical
scalar order key. Later order terms and the complete ascending entity key
follow normally. Missing and explicit null use the same state-class rank and
carry no invented scalar value. The physical encoding remains provider-owned,
but its comparison must equal the independent semantic evaluator byte for byte
at page boundaries.

The provider maintains exact cardinality and direct ordinal selection over the
complete authorized predicate and V5 order at one provider epoch. State
placement happens before ordinal selection. It cannot be implemented by an
authoritative entity scan, request-time sort, matching-population
materialization, skipped-row or skipped-page walk, result filtering, sentinel
scalar substitution, or client shaping. Output hydration remains proportional
to the bounded returned page.

Provider descriptors declare each state-aware order layout, scalar comparison,
partition/policy scope, state and write amplification, rank/select work, and
retention/readiness behavior. Descriptor and layout validation are paid once
per provider-plan generation; epoch proof is paid once per opened result set.
Neither may be reconstructed per row, order term, index probe, skipped ordinal,
or page item.

V5 is rebuildable derived state. Before epoch two, V4 checkpoints remained
readable and byte-exact, and plans representable without state placement used
the least-sufficient V4 writer. In epoch two, ADR-0219 makes V5 the only
readable, writable, current exact-provider state for every exact family; V1
through V4 are absent and refused before interpretation, and no plan may
downgrade to them. Deployment rebuilds and catches up genuine V5 state from
authoritative data, then publishes typed readiness at a servable epoch; requests
fail with the existing safe typed lifecycle outcome until that proof exists.

### 5. Preserve exact count, direct offset, policy, and freshness

The selected predicate member, nullable total order, exact total, and ordinal
page share one exact provider and one servable epoch under ADR-0130 and
ADR-0134. A concurrent create, update, delete, null transition, or migration-
era missing-to-value transition cannot make the count describe a different
population or order from the returned page.

Partition-scoped state remains the primary policy-enforcement mode, followed
by compiler-proved policy-aligned subpartitions. Any bounded row admission must
complete before state classes, counts, ranks, offsets, cursors, work classes,
or diagnostics are formed. Unauthorized rows and fields cannot affect or be
inferred from a result's state distribution, total, position, timing class,
lifecycle response, or error detail. Permission to order by a field does not
grant permission to project it; ADR-0128 secret-output authority remains
independent.

### 6. Keep every application surface finite and framework-neutral

Generated Rust, Go, TypeScript, and Python clients, CLI, and MCP expose only
the finite named operations or closed generated member choices already sealed
by compilation. Null placement never appears as a caller-provided option. All
surfaces traverse the same application service, authorization, exact provider,
epoch proof, output policy, response budget, and typed failure path.

WP-677 uses a framework-neutral fixture entity with optional text and timestamp
fields. It covers both placements and directions, later-term comparison inside
the `NoValue` class, exact totals, direct offsets, and concurrent state
transitions. RiffDB contains no external framework schema, route, adapter,
profile, error translation, or runtime branch.

The external adapter repository remains responsible for selecting a fixed
generated member for each of its finite public sort choices, pinning the
immutable development publication, and proving the real route matrix. RiffDB's
generic capability receipt is necessary handoff evidence, not a claim that the
external migration has passed.

## Options Considered

1. **Continue rejecting optional order fields:** rejected because ordinary
   bounded application lists require them and the existing public integration
   cannot safely emulate them.
2. **Use the provider or host language's default null order:** rejected because
   direction, backend, and locale defaults differ and would make plan identity,
   offsets, and compatibility dishonest.
3. **Replace null or missing with a sentinel scalar:** rejected because every
   scalar domain can contain the sentinel and schema history would leak into
   value semantics.
4. **Expose placement as a request parameter:** rejected because callers could
   select an uncompiled order and provider-state cost at runtime.
5. **Give missing and explicit null separate implicit ranks:** rejected for the
   first activation because no real consumer requires the distinction and it
   would expose schema-history state unnecessarily.
6. **Compile explicit `nulls first` and `nulls last` into one `NoValue` class:**
   proposed because it provides deterministic total ordering for current real
   consumers while preserving finite compilation, exactness, and a versioned
   path for later evidence.

## Consequences

- Named exact queries can sort optional text, timestamp, and other already-
  ordered scalar fields while retaining exact count and direct numeric offset.
- Authors must state placement when the compiler cannot prove all matching
  rows have values; there is no convenient but unstable default.
- Each declared state-aware order may add rebuildable index and write
  amplification. The compiler rejects families exceeding static state or work
  ceilings.
- Missing and explicit null are deliberately indistinguishable for this order
  term. Separate placement remains deferred until a real consumer and privacy
  review justify another durable profile.
- Relevance ranking, facets, ad-hoc ordering, locale collation, caller-provided
  sort structure, and cross-provider composition remain outside this change.

## Compatibility

This ADR changed no bytes or public behavior by itself. Its epoch-one
implementation added RiffQL V7, query IR V10, query-module V10, generated-
surface successors, and rebuildable exact-provider state V5 under ADR-0124.
V6/V9/V9/V4 sources, modules, plans, hashes, locks, generated methods,
checkpoints, and canonical fixtures remained readable and byte-exact, and
least-sufficient writers remained active during that epoch.

In epoch two, ADR-0216 makes RiffQL V14, query IR V18, and query module V18 sole
current, while ADR-0219 makes exact-provider state V5 sole current. Provider-
state V1 through V4 are neither readable nor writable and are refused before
interpretation; V5 checkpoint bytes and nullable total-order semantics remain
exact.

A nullable-order deployment fails closed until V5 is rebuilt, caught up, and
servable. It neither rewrites or adopts V1 through V4 in place nor changes
authoritative entity, commit-log, journal, export, changelog, backup, command,
Protobuf, or transport formats. Epoch-two predecessor decoder retirement is the
closed ADR-0219 breaking-epoch transition.

No public generic sort or null-placement message is added. Generated closed
member choices continue to use the existing named-query envelopes unless a
separate accepted ADR authorizes a new wire identity.

## Security

State-class statistics are computed only over the complete authorized
partition or compiler-proved policy subpartition. Denied rows cannot affect
counts, ranks, offsets, order, work class, readiness, timing categories, or
diagnostics. Missing and explicit null share one order class, reducing schema-
history disclosure through ordering. Field output and secret projection remain
separately authorized.

Compiler and runtime errors identify only the source term, closed unsupported
capability, bound, lifecycle state, or safe remedy. They do not disclose which
rows lack values, how many values are null, partition size, hidden order keys,
index layout, or the position at which a denied row would sort.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** application callers receive
  only finite generated operations with compiler-fixed field, direction,
  placement, comparison, predicate, policy, count, offset, and freshness. They
  cannot submit a sort field, placement, expression, provider, scan, fallback,
  or client-shaping instruction, and unsupported state-aware orders fail
  closed before deployment or with typed readiness before execution.
- **Scale:** order terms, family members, state layouts, indexes, write
  amplification, rank/select work, offsets, pages, outputs, rebuild work,
  waits, and diagnostics are statically bounded. Exact count and ordinal seek
  do not allocate the matching population, walk skipped rows/pages, assume
  co-located authoritative storage, or require a request-time full-state
  rewrite.

## Testing

- Source-span snapshots and semantic assertions for omitted, first, last,
  repeated, misplaced, key-tie-breaker, unsupported-type, and excessive-family
  placement clauses.
- Canonical truth-table and randomized reference-evaluator properties covering
  missing, explicit null, values, later order terms, duplicate values, complete
  keys, every direction/placement pair, and optional text/timestamp fields.
- V6/V9/V9/V4 byte-exact fixtures, V7/V10/V10/V5 canonical fixtures,
  least-sufficient writer checks, topology validation, mixed-version refusal,
  rebuild/catch-up/restart/corruption coverage, and decoder-retirement guards.
- Provider equivalence for zero, boundary, end, beyond-end, and maximum offsets;
  exact totals; concurrent create/update/delete and state transitions; policy
  isolation; cancellation; compaction; retained epochs; and maximum bounds.
- Architecture negatives forbidding scans, request-time sorting, sentinel
  substitution, page walking, matching-population materialization, per-row
  proof construction, caller placement, framework branches, and cross-provider
  transfer.
- Framework-neutral shared-service, Rust, Go, TypeScript, Python, CLI, and MCP
  conformance from one optional-field corpus and one immutable development
  publication.

## Requirements and Work Packages

- **Requirements:** `OQ-001` through `OQ-006`, `OQ-015` through `OQ-017`,
  `OQ-019` through `OQ-024`, and `OQ-026` through `OQ-030`
- **Semantic model and compiler:** WP-675
- **Provider state and execution:** WP-676
- **Generated surfaces and external handoff:** WP-677
- **External route evidence:** owned by the external adapter repository

## Decision Deadline

Exact human acceptance is required before WP-675 edits SPEC, RiffQL syntax,
query IR, module/plan identities, provider-state formats, generated surfaces,
or compatibility fixtures. If implementation cannot preserve one authorized
epoch, exact cardinality, direct ordinal seek, or the frozen `NoValue` truth
table, it must return for human review rather than narrow or reinterpret this
record.
