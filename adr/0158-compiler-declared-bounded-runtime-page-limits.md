# ADR-0158: Compiler-Declared Bounded Runtime Page Limits

- **Status:** Accepted
- **Direction approved:** 2026-08-26
- **Exact text accepted:** Yes, 2026-08-26
- **Accepted:** 2026-08-26
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for upstream commit `fc76a107`
- **Decision deadline:** Before WP-706 changes RiffQL parameter syntax, query
  IR, module identity, cost derivation, or generated input schemas
- **Requires:** ADR-0002, ADR-0013, ADR-0027, ADR-0051, ADR-0052, ADR-0055,
  ADR-0108, ADR-0112, ADR-0124, ADR-0129, ADR-0130, ADR-0134, ADR-0150, and
  ADR-0155
- **Amends if accepted:** ADR-0055's rule that every runtime `Limit` is charged
  at the complete 499-row type range
- **Defines or blocks:** WP-706 and WP-707

This record is authoritative for WP-706 and WP-707.

## Context

RiffQL deliberately separates a fixed source bound from a caller-selected page
size. A literal `take 25` has an exact static cost, while a `$limit: Limit`
parameter may carry any positive value through 499 and is therefore charged at
499 regardless of its default. ADR-0055 freezes that conservative rule and
correctly rejects plans whose complete parameter domain exceeds the
whole-request cost ceiling.

Two real generated-driver applications now expose the missing middle. They
need one finite named query, one nullable opaque cursor, and a caller-selected
page size whose reviewed maximum is substantially below 499. Rows can also
contain legitimate bounded values large enough that charging 499 rows exceeds
the 4 MiB result or whole-request ceilings even though a 10-, 50-, or 100-row
page is safe. A one-row literal compiles but turns a public page into repeated
driver crossings. A larger literal cannot accept the requested page size.

The reproduced generic shape is:

```riffql
query ReadItems(
    $organization_id: Item.organization_id,
    $limit: Limit = 100,
    $after: Cursor?,
) {
    many items from Item
        where organization_id == $organization_id
        order by item_id asc
        take $limit after $after
    return Found { items: items { item_id, bounded_payload } }
    outcomes Found
}
```

The query is rejected because `Limit` honestly means 1 through 499. Treating
the default as a maximum would be unsafe: an existing caller may submit 499,
and defaults may change independently of type domains. Fetching a larger fixed
page and slicing, walking one-row pages, splitting one public page across
calls, lowering field bounds, or generating many literal-size query variants
would respectively distort cursor semantics, multiply crossings and snapshots,
weaken the contract, or create an artificial operation family.

The missing capability is a source-declared refinement of `Limit` whose upper
bound is part of the immutable query type, plan cost, input schema, plan and
module hashes, and runtime validation. This is a public language and immutable
IR change. It cannot be implemented by changing the meaning or bytes of an
existing `Limit`, query plan, or module version.

## Proposed Decision

### 1. Add the closed `Limit<MAX>` query-only type

RiffQL adds one query-only parameter type:

```riffql
$limit: Limit<100> = 50
```

`MAX` is a canonical unsigned decimal literal in the inclusive range 1 through
499. The lower bound is always 1; zero is never a page size. `Limit<MAX>` is
valid wherever the existing `Limit` parameter is valid as a `take` or
`nearest` bound. It is not a contract scalar, collection cardinality, offset,
byte budget, arbitrary integer refinement, or caller-defined type.

Plain `Limit` remains source-compatible and byte-compatible and retains its
exact current meaning. It is semantically the unrestricted page type with
maximum 499, but it retains its existing syntax and legacy encoding rather
than being rewritten to `Limit<499>`.

The declaration is closed:

- `MAX` must be present exactly once inside angle brackets and must be in
  1..=499;
- a default, when present, must be a positive unsigned literal no greater than
  `MAX`;
- a default never narrows or widens the declared domain;
- optional, nested, set, field-derived, negative, zero, nonliteral, and
  multi-bound forms are rejected with source-spanned diagnostics; and
- one parameter has one maximum everywhere it is referenced in the query.

`Limit<MAX>` is intentionally specific. A future need for bounded offsets,
arbitrary numeric refinements, or a non-one minimum requires its own real
consumer and accepted decision rather than generalizing this page-safety type.

### 2. Carry the maximum through every compiler-owned representation

The resolver retains the declared maximum in the name-addressed parameter
schema and every result-list page-bound reference. The compiler retains it in
the row-limit source and uses it as the binding's maximum rows. Exact-result,
projection-backed, vector, aggregate, covered, and ordinary operational query
families may use the type only when their existing semantics already admit a
runtime `Limit`; this ADR does not make a previously unsupported query shape
executable.

Every derived quantity uses `MAX`, never the submitted value or default:

- index rows and the one continuation probe;
- provider page and ordinal-window work where already supported;
- point reads, hydration, intermediate rows, and row-policy work;
- projected values, aggregate input/group work, and exact-result output rows;
- maximum encoded result bytes and driver frame admission; and
- role requirements and the complete whole-request cost vector.

Costs remain cumulative across bindings. Reusing one `Limit<100>` parameter in
two independent collections charges both 100-row maxima. A smaller submitted
value cannot lend unused authority to another step or widen another budget.
Projection providers retain their separately declared full-population,
candidate, exact-count, facet, ranking, or nearest-search costs; a bounded
returned page does not pretend that those costs shrink.

Maximum validation, schema compatibility, and cost derivation are paid once
per compilation or admitted request as appropriate. They are never per row,
page item, provider operation, or cursor fetch.

### 3. Reject out-of-domain values before provider or storage work

All public paths validate an effective runtime page value in 1..=`MAX`. The
effective value is the submitted value or the compiled default. Missing input
without a default, zero, a value above `MAX`, a wrong canonical type, or a value
outside the unchanged global page ceiling returns the existing typed invalid-
input class before authorization-derived execution fuel is consumed and before
any provider, index, row-policy, hydration, or storage operation begins.

Generated Rust, Go, TypeScript, and Python facades validate the same declared
maximum for early developer feedback. CLI, MCP when declared, local driver,
and remote gRPC use the same compiler-generated input schema. Target-language
validation is additive only; the first-party Rust service remains
authoritative and revalidates every request.

The actual value controls only how many rows the already admitted plan may
return in that invocation. Cursor construction and continuation probing use
that effective value without changing the immutable plan. A cursor remains
bound to the exact module, plan, normalized parameters other than the protected
continuation channel, principal, policy/capability revision, database history,
snapshot, order, and provider epoch already required by accepted ADRs.

### 4. Introduce additive language, IR, and module identities

The first accepted implementation assigns the next available identities:

- RiffQL language version 9;
- query IR version 12; and
- query-module format version 12.

Any source using `Limit<MAX>` selects those successor identities. Version 12
encodes the positive maximum explicitly and canonically before the optional
default wherever the parameter or row-limit source is encoded. The maximum is
included in canonical source, input-schema, plan, module, role, application
lock, and generated-artifact identities. Changing 100 to 101 is an ordinary
immutable query change and cannot reuse an old cursor or deployed operation
identity.

Existing RiffQL versions 1 through 8, query IR versions 1 through 11, module
formats 1 through 11, plain `Limit` plans, canonical bytes, hashes, fixtures,
generated artifacts, and deployed locks remain byte-exact and readable. Old
sources are not reformatted to the new spelling. The normal query-module and
application-lock deployment rotation activates a successor; no entity data,
index key, cursor wire format, public Protobuf, storage format, or durable
database migration changes.

The version-topology registry records the new nodes and their readable and
writable edges. Retirement later follows ADR-0124; an implementation package
may not overwrite an old decoder, silently reinterpret an old tag, or shorten
the compatibility window to avoid adding the successor.

### 5. Keep performance evidence semantic and framework-neutral

The RiffDB acceptance corpus uses generic bounded-payload entities and named
queries. It proves that N rows requested through `Limit<MAX>` require the same
single generated operation invocation and one bounded backend page, rather
than asserting a machine-dependent wall-clock threshold. It includes distinct
queries whose safe maxima differ because their declared row byte bounds differ.

External application suites remain downstream evidence. No external framework
name, route, tuple model, authorization model, adapter branch, or package is
added to RiffDB source, fixtures, generated code, diagnostics, or public
documentation.

## Options Considered

1. **Treat the default as the maximum.** Rejected because defaults are values,
   not type domains. It would silently widen existing plans at runtime or make
   their static cost dishonest.
2. **Use a fixed literal and a separate requested value.** Rejected because
   fetching and discarding rows changes cursor/page behavior and charges work
   that the caller did not request.
3. **Generate one named query for each page size.** Rejected because it creates
   a combinatorial public operation family and still cannot express every
   finite requested size cleanly.
4. **Add a general numeric refinement system.** Rejected until a second real
   non-page consumer establishes common semantics. `Limit<MAX>` solves the
   demonstrated safety boundary without inventing unrelated type machinery.
5. **Raise whole-request or result-byte ceilings.** Rejected because the defect
   is false charging against an unnecessarily broad parameter domain, not an
   insufficient global safety ceiling.

## Consequences

- Applications can choose one reviewed maximum per named page while retaining
  a caller-selected smaller page size and direct cursor continuation.
- Cost, authorization, generated validation, and runtime execution share one
  immutable upper bound.
- Plain `Limit` remains conservative and compatible; adopting the refinement
  requires an explicit source and deployment identity change.
- The compiler, codecs, module loader, generators, editor grammar, LSP,
  handbook, fixtures, and conformance corpus must understand the new type.
- This does not add joins, unbounded scans, adaptive limits, caller-selected
  fields or predicates, page walking, framework behavior, or larger global
  byte/row ceilings.

## Compatibility

This is an additive source-language and immutable query-artifact change. It
adds language/IR/module successor versions and generated input-schema metadata.
It does not change existing artifact bytes, public Protobuf, driver framing,
storage keys, entity state, indexes, durable command outcomes, or cursor wire
bytes. A newly bounded query requires ordinary module/application rotation and
invalidates cursors from its predecessor because its plan identity changes.

## Security

The refinement narrows caller authority. The compiler charges and policy
authorizes the declared maximum, generated clients reject obvious violations,
and the Rust service rejects invalid values before data work. Error diagnostics
may name the parameter and safe declared/allowed integers but reveal no
submitted payload, row, match count, policy fact, index contents, or hidden
field. Row and field policy still execute before any result, count, rank,
cursor, or aggregate is released.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** The caller may select only a
  positive page size within a compiler-declared maximum on an immutable named
  query. It cannot change the maximum, cost, index, provider, predicate, order,
  field set, policy, snapshot, or cursor identity. All surfaces fail closed and
  the Rust service remains authoritative.
- **Scale:** Work is bounded by the declared maximum and existing global row,
  byte, step, provider, policy, and response ceilings. The decision adds no
  population scan, full-state materialization, co-location assumption, or
  per-row compatibility work. It reduces avoidable crossings without changing
  storage complexity.

## Testing

- Parser and formatter round trips for `Limit<1>`, representative maxima, and
  `Limit<499>`, with source-span snapshots for zero, 500, malformed, nested,
  optional, and default-above-maximum declarations.
- Resolver, canonical source, schema-hash, plan-cost, role-requirement, and
  plan/module-hash goldens proving the declared maximum is used and the default
  is not.
- Byte fixtures proving every existing language, IR, and module version is
  unchanged and the version-9/12/12 successor rejects substitution or maximum
  drift.
- Executor and backend-spy tests proving 1 and `MAX` execute, while 0 and
  `MAX + 1` fail before provider/storage work; forward and reverse cursor pages
  have no duplicate or omission.
- Generic large-field tests proving separately declared page maxima compile
  beneath exact result-byte ceilings and maximum-plus-one fails statically.
- Rust, Go, TypeScript, Python, CLI, MCP, local-driver, and remote-gRPC
  conformance for input schemas, defaults, rejection classes, and cursor
  routing.
- A fresh sparse Go-only installed-package test proving a multi-row bounded
  page uses one generated operation invocation without an MCP artifact or
  unrelated toolchain.

## Requirements and Work Packages

- **Requirements:** `OQ-062` through `OQ-067`, `DX-050`, and `DRV-016`
- **Defines or blocks:**
  - `WP-706`: language, resolver, IR/module versioning, planner cost, runtime
    validation, storage/provider semantics, compatibility fixtures, and core
    handbook coverage;
  - `WP-707`: generated surfaces, editor/LSP support, driver parity, sparse
    Go-only installed-package and invocation-count conformance, and final
    documentation/evidence.
- **Final evidence:** `WP-707`

## Decision Deadline

Exact-text human acceptance is required before any grammar, query schema,
canonical encoding, version registry, generated surface, or runtime behavior
changes. Discovery and this paper draft may proceed beforehand; implementation
may not.
