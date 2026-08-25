# Generated operational queries

Operational RiffQL gives an application a finite set of compiler-owned read
plans without accepting a predicate tree, field name, operator, order clause,
or index from the caller. A deployed named query may expose typed optional
values, null/existence tests, binary prefix lookup, stable cursors, and bounded
exact aggregates. Every possible optional-value presence combination is
compiled, authorized, costed, and indexed before deployment.

Generate bindings from the reviewed exact lock:

```bash
riffdb application lock --write riffdb.application.json
```

The generated Rust, Go, TypeScript, and Python methods accept only the values
declared by the query. For example, a relation filter is an optional string,
not a generic condition:

```rust,ignore
let page = db.list_fga_tuples(ListFgaTuplesParams {
    store_id,
    relation: Some("viewer".to_owned()),
    after: None,
}).await?;
```

```typescript,ignore
const page = await db.listFgaTuples({
  store_id: storeId,
  relation: "viewer",
});
```

Go and TypeScript use the long-lived Rust driver host; Python and Rust may use
the verified-TLS application transport directly. In every case the result is a
closed generated outcome with bounded arrays, optional values, opaque cursors,
and exact integer/decimal carriage. Aggregate sums that do not assert a wire
precision are decoded under the generated result schema; a conflicting wire
precision or scale is rejected.

## Cursor rule

Persist the opaque `next_cursor` returned with a page and submit it only to the
same exact generated operation, contract, module, parameters, principal, and
page shape. Do not decode it. An invalid, expired, cross-principal, or stale
cursor is a typed failure; clients must not silently restart from the first
page.

When a named query declares `Cursor` or `Cursor?`, generated Rust, Go,
TypeScript, and Python methods route a present value to the protected query
options cursor. They omit it from the symbolic parameter record. Absence means
the first page; supplying both the generated cursor parameter and the existing
low-level cursor option fails locally instead of choosing one. Cursor bytes and
server validation are unchanged.

## Binary text-key ordering

Use a declared `text_key(field, binary_utf8_v1)` index component when a named
query must traverse text in exact UTF-8 byte order. It can supply the bounded
`order by` suffix without requiring a prefix predicate on that field. For
example:

```riff
index by_title (organization_id, title, document_id) text_key(title, binary_utf8_v1)
```

proves `order by title asc, document_id asc` after the required organization
equality, or the wholly reversed descending order. In that order `doc-3`
precedes `doc6`. A plain string index has canonical length-first ordering, so
declare the text-key profile when bytewise interoperability matters. RiffDB
still requires the complete deterministic index order, bounded `take`, and an
opaque snapshot-bound cursor; callers cannot select the collation or index at
runtime.

## Feature preflight

The authorized application catalog returns the closed
`riffdb.application-catalog/v1` feature registry for one exact contract. Rust
applications can call
`StableApplicationClient::preflight_application_features`. The checked result
contains only the exact contract identity, active module hashes, and the six
closed feature states; it contains no raw catalog symbols, numeric compiler
identities, capability record, IR, or storage key.

`unavailable` is a prohibition, not a request for client emulation. In the
current alpha, binary UTF-8 prefix lookup is available and
`unicode_fold_v1` remains explicitly unavailable. Application code must not
replace an unavailable feature with client filtering, raw reads, N+1 requests,
or handwritten query text.

## Exact count and numeric offset

An exact-result named query may combine a bounded indexed text predicate,
`take $limit offset $offset`, and `exact_count()`. Generated Rust, Go,
TypeScript, and Python methods carry `limit` and `offset` as ordinary typed
parameters and decode both the bounded row collection and `{ value: u64 }`
total. The CLI and generated MCP tool use the same operation and schema.

The server chooses one authorized provider epoch and returns the page and total
from that epoch. For a protected query, bounded background activation applies
the current row policy before it constructs count or ordinal state; the
capability identity and revision select a separate derived slot. Denied rows
therefore cannot influence the released total or order. The request path seeks
the indexed ordinal directly; it never scans, walks cursor pages, filters in
the client, or counts only the page. A fresh invocation is a new current-
snapshot observation, so numeric offset is intended for admin-style windows
rather than stable traversal across concurrent writes. Use ordinary cursor
queries when snapshot-bound continuation is required.

The initial request waits for a fixed, bounded readiness window while the
derived provider builds in the background. It may still return
`RDB-QUERY-0102` when that window expires. `RDB-PROJECTION-0101` means the
required providers cannot prove one common epoch, `RDB-PROJECTION-0102` means
the requested snapshot retired, and `RDB-PROJECTION-0103` means no provider
snapshot yet satisfies the request's current or read-after-commit freshness.
Generated clients expose these as the same typed application error codes on
every transport. A host may retry these lifecycle outcomes with a bounded
attempt budget; it must not emulate a scan, retain a stale page, or weaken
freshness.

The narrow exact-result form accepts one partition equality, one exact text
predicate, and at most one compiler-declared optional typed equality filter.
The richer V6 form accepts the closed exact-predicate vocabulary documented in
[Exact Predicate and Order Families](../riffql/EXACT-PREDICATES.md): comparisons,
bounded typed sets, state tests, binary text operations, capped Boolean shape,
and compiler-enumerated optional guards. It also permits a total order over
fields independent from the search fields. Any unsupported or unindexed family
member rejects the complete source with a span; it is never a request-time
filter or ignored condition.

Set parameters accept at most 64 submitted values. RiffDB sorts and
deduplicates them canonically before indexed execution; empty `in` and
`not_in` sets retain their documented exact semantics. `Limit` is positive and
at most 499; offset is bounded by the compiled family. Every declared
predicate and order field contributes provider storage and rebuild work, and
every optional guard multiplies the finite member count. Keep named operations
small and task-shaped rather than building one combinatorial search endpoint.

## Framework-neutral exact-result proof

The retained operational fixture exposes a generic `Document` query family for
binary UTF-8 contains, starts-with, and ends-with matching. The family combines
one typed optional document filter, deterministic ascending and descending
orders, bounded limit and numeric offset, and a complete exact total. Its
generated Rust, Go, TypeScript, Python, CLI, and MCP surfaces prove the RiffDB
semantics without embedding an external framework's schema, route, role, or
application policy in this repository. Its generic `DirectoryUser` family
additionally proves bounded sets, comparison ranges, null/existence state,
optional-presence members, Unicode values, and search-by-one-field/order-by-
another through the same six public surfaces.

Framework integrations own their generated profile and route-level acceptance
in their own repositories. They pin a RiffDB build and prove that their public
API delegates to generated named methods without client-side filtering,
sorting, counting, page walking, raw query construction, or storage access.
RiffDB keeps the generic compiler, provider, policy, transport, and conformance
surface; it does not ship a Better Auth admin route or generated Better Auth
admin SDK.

## CLI and MCP

The same named query can be invoked by CLI or its generated MCP tool. CLI
parameters use the public tagged JSON value format; MCP input and output JSON
Schemas contain only symbolic parameters and bounded result fields. Neither
surface accepts an index, plan, field ID, or arbitrary predicate.

The retained acceptance corpus is:

```bash
./scripts/adapter-operational-query-acceptance --all-languages
```

It deploys one exact application over verified TLS, seeds through compiled
commands, and checks OpenFGA tuple filtering, an MLflow exact metric dashboard,
Payload binary-prefix and null pages, a Woodpecker state queue, a typed session
lookup, and the framework-neutral exact-result family through Rust, Go,
TypeScript, Python, CLI, and generated MCP schemas. The exact-result phase
proves all three binary-text operators, both declared order directions, direct
offset boundaries, complete totals, optional-filter null/present forms, and
the V6 set/range/state/independent-order family.
The corpus also proves a malformed cursor fails closed and scans the
application runners for kernel, storage, numeric-ID, raw-transaction, and
client-filter escape hatches. External adapter repositories own their separate
route-level compatibility matrices.
