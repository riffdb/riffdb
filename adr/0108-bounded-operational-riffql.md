# ADR-0108: Bounded Operational RiffQL and Safe Catalog Introspection

- **Status:** Proposed
- **Direction approved:** 2026-08-09
- **Exact text accepted:** No
- **Decision deadline:** Before WP-563 changes RiffQL grammar or query IR
- **Requires:** ADR-0035, ADR-0038, ADR-0051, ADR-0053, ADR-0054,
  ADR-0055, ADR-0070, ADR-0086, ADR-0087, and ADR-0092
- **Defines or blocks:** WP-563 through WP-565 and WP-570

## Context

Named RiffQL proves locality and bounded page shapes, while the projected
ad-hoc plane has a separate initial predicate and aggregate vocabulary. Real
operational adapters still need bounded optional/dynamic filters, stable cursor
pages, top-N order, prefix and case-insensitive lookup, null/existence tests,
multiple aggregates, exact decimal aggregates, and safe symbolic catalog
inspection. Without these, applications either proliferate named queries or
attempt client filtering and raw catalog access.

The answer cannot be a SQL escape or an unindexed runtime scan. Each construct
must preserve the safe-application rule: a finite compiler-owned access family,
one partition, explicit cost, one snapshot, current authorization, and bounded
output.

## Proposed Decision

RiffQL adds a versioned operational subset shared by named queries and, where a
projection can prove the same semantics, projected ad-hoc queries.

### Dynamic predicates are finite plan choices

A query may declare optional predicate parameters and bounded predicate groups.
The compiler enumerates a finite closed plan family at deployment. At runtime,
parameter presence selects one exact member; callers never submit a predicate
AST, field name, operator, order expression, or index hint.

Supported predicates are typed equality, inequality/range, bounded `in`,
`is null`, `is not null`, `exists`, prefix, and conjunction plus compiler-capped
OR. Every selected family member must prove the same partition route, bounded
cost vector, authorization result shape, and an index or projection access path.
A family with excessive combinations or an unindexed member fails compilation
with a source-spanned remediation.

### Cursor pages and top-N

Every operational collection remains explicitly bounded and uses the existing
snapshot-per-request cursor identity. Top-N is accepted only when ordering is a
total order produced by a declared index/projection or when the compiler proves
an in-memory candidate bound small enough to charge completely. A deterministic
unique tie-breaker is mandatory. Offset pagination remains unavailable.

Changing predicate presence, ordering, module/plan identity, authorization,
text-normalization version, projection generation, or database history makes a
cursor fail closed or return the already typed reset where that surface permits
one.

### Text prefix and case-insensitive semantics

Case-insensitive or prefix lookup requires a declared versioned text-key index.
The contract names the normalization profile; compiler and storage use one
first-party canonical byte transform for write and query keys. Runtime locale,
platform collation, database-engine collation, and caller-supplied normalization
are forbidden.

The initial profiles are exact binary UTF-8 and one explicitly versioned
Unicode normalization-plus-case-fold profile frozen with checked data tables.
Changing the profile or Unicode table version is an index migration, not a
silent software upgrade. Prefix bounds operate on canonical text-key bytes and
return original authorized field values, never normalized substitutes.

This text-key surface is not a second full-text-search engine. It performs
exact equality or leading-byte-range lookup over a declared canonical key. It
has no tokenization, stemming, relevance score, corpus statistic, phrase
matching, snippet, or ranking vocabulary. Those remain exclusively governed by
ADR-0092's projection-backed FTS, including fixed-point scoring, per-tenant
corpus statistics, policy-before-scoring, and inference protections. A query
requiring token or relevance semantics must use that projection rather than
silently degrading to an operational text-key scan.

### Null and existence

Optional-field predicates have explicit index semantics distinguishing missing,
null, and non-null values according to the field model. An index either records
the required discriminator or is ineligible. The planner never answers
existence by fetching an unbounded candidate set and post-filtering it.

### Aggregates

Operational RiffQL supports multiple `count`, `sum`, `min`, and `max`
expressions and bounded group-by under the existing projection/query budgets.
Integer and fixed-scale decimal sums accumulate in a checked wider exact type;
overflow is a typed whole-query failure, never saturation or floating point.
Money sums require identical currency or an explicit grouping by currency.

Every predicate field, group key, aggregate input, and returned value is
authorized before execution. Row policy applies before aggregation, and
inference-sensitive aggregates may require a policy-owned minimum group size.
No partial aggregate is released on budget exhaustion.

This is one aggregate semantics surface, not a competing implementation beside
the projected aggregate path shipped by WP-492. Named operational queries
lower to the same canonical aggregate descriptors, exact evaluator, and result
carriage wherever their access source is a projection. The following WP-492
decisions are normative here as well: integer sums accumulate as checked i128
and cross the public boundary as `Decimal`; empty-set identity and absent
`min`/`max` meanings remain exact; group order is canonical encoded-key order;
and the requested row limit clamps maximum group cardinality before execution.
Decimal and money extensions must preserve that shared result algebra rather
than add a second response arm. Policy-owned minimum group size supplements,
and never replaces, the output-volume clamp and authorization union.

### Receipted module-identity rotation

The grammar, query IR, canonical source, plan hash, and module hash are durable
application identities. A new operational construct may not silently
reinterpret an already deployed module or let local locks and generated
bindings disagree with server state. Before activation, WP-563 performs one
receipted identity-rotation campaign:

1. freeze an additive grammar/IR/module format version and keep old decoders;
2. regenerate compiler and protocol fixtures plus every frozen literal;
3. rewrite exact application locks and role bindings through the canonical
   compiler/deploy path, never by hand;
4. regenerate Rust, Go, TypeScript, Python, and MCP bindings for every checked
   example, explicitly including TicketDesk and `examples/agent-alpha`;
5. record old/new source, plan, module, lock, binding, and generated-artifact
   identities in a redacted receipt; and
6. prove `application check`, deploy, role reconciliation, and
   `check-application-bindings` agree before any new module becomes active.

A partial rotation is a typed incomplete campaign with an exact next action,
not a successful deploy. The c063c95 identity-rotation failure is the standing
regression: no grammar/IR change can merge on a false-green lock or stale
generated binding.

### Safe catalog introspection

The application service adds bounded symbolic catalog operations for authorized
contracts, entities, fields, relationships, indexes, commands, query modules,
roles, operation schemas, and supported feature flags. Results contain names,
public types, source locations, bounds, and stable symbolic identities only
when visible to the principal. They exclude numeric IDs, hidden declarations,
capability internals, raw IR, storage keys, plans, credentials, and existence
signals for unauthorized symbols.

Catalog pages are snapshot-bound, budgeted, database-selected, and
authorization-filtered. Generated adapters use the same catalog schema for
feature preflight; there is no direct catalog/storage handle.

## Options Considered

1. **Expose read-only SQL:** rejected for the application path because locality,
   authorization, cost, result identity, and generated schema become runtime
   concerns.
2. **Allow arbitrary runtime predicate objects:** rejected because callers can
   select unreviewed fields and access paths.
3. **Add every combination as a named query:** safe but operationally
   unmanageable and hostile to adapters.
4. **Finite compiler-owned plan families plus declared text indexes:** proposed
   because dynamic use remains closed and explainable.

## Consequences

- Operational adapters can express common indexed pages and dashboards without
  raw reads or client filtering.
- Text normalization becomes a versioned compatibility boundary.
- Plan-family explosion is rejected rather than hidden behind runtime planning.
- General joins, subqueries, recursive queries, regex, fuzzy search, and
  unbounded analytics remain unavailable; full-text/vector projections retain
  their separate accepted designs.

## Compatibility

Grammar, query IR, module hash, cursor, text-index, projected request/response,
and catalog schemas require additive versioned successors. Existing named
queries and row plan hashes remain pinned. A text-profile change requires
migration and rebuild of the affected index/projection. Activation additionally
requires the complete receipted rotation above; repository fixtures, examples,
locks, roles, and generated clients are part of the compatibility closure.

## Security

All plan members are authorized, not merely the selected happy path. Catalog
introspection is policy-filtered before serialization and uses indistinguishable
absence for unauthorized symbols. Aggregate inference protections and response
budgets remain mandatory.

## Standing Design Tests

- **Interface safety:** callers choose values and presence flags only; they
  cannot name a field/operator/order/index or request an uncompiled plan. No
  construct has an unbounded fallback.
- **Scale:** every operational query is single-partition and index/projection-
  planned with explicit row, group, memory, and output bounds. No full-state
  rewrite or co-located global scan is required.

## Testing

- Grammar/formatter/IR/cursor/text-profile compatibility goldens.
- Plan-family combinatorial bounds and diagnostics.
- Unicode normalization/case-fold conformance and migration fixtures.
- Differential query tests against a pure evaluator for predicate, null,
  prefix, order, pagination, and exact aggregates.
- Authorization/inference/redaction matrices for every referenced symbol.
- Catalog hostile-page, revocation, hidden-symbol, and multi-database tests.

## Requirements and Work Packages

- **Provisional requirements:** `OQ-001` through `OQ-016`, to be added to
  `SPEC.md` only after exact acceptance.
- **Defines or blocks:** WP-563, WP-564, WP-565, and WP-570.
- **Final evidence:** WP-565 and WP-570.

## Decision Deadline

Exact acceptance is required before operational grammar, text normalization,
aggregate, cursor, projected, or catalog public interfaces change.
