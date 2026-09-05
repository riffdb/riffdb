# Generated operational queries

Generated MCP tools for a paginated named query return the business outcome
under `result` and the sole nullable opaque continuation under
`page.next_cursor`. Reuse a non-null cursor only through the compiler-declared
input cursor property; the service retains snapshot, order, expiry, and
authorization authority. Hosted and stdio MCP use this same envelope. Unpaged
tools retain their flat generated result, and generated SDK query options keep
their existing typed cursor behavior.

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
same exact generated operation, contract, module, invariant parameters,
principal, and page semantics. Do not decode it. For an ordinary ordered
`take $limit after $cursor` binding, a resumed invocation may choose a different
valid runtime `Limit` or `Limit<MAX>` value; page cardinality does not identify
the keyset position. The plan and declared maximum remain immutable, and
nearest/vector K, filters, partition, ordering, authority, snapshot, and
provider epoch remain bound. An invalid, expired, cross-principal, or stale
cursor is a typed failure; clients must not silently restart from the first
page.

When a named query declares `Cursor` or `Cursor?`, generated Rust, Go,
TypeScript, and Python methods route a present value to the protected query
options cursor. They omit it from the symbolic parameter record. Absence means
the first page for `Cursor?`; an exact `Cursor` remains a required method
parameter. Supplying both the generated cursor parameter and the existing
low-level cursor option fails locally instead of choosing one. Cursor bytes and
server validation are unchanged. Generated methods issue exactly one bounded
request. They do not expose an iterator that walks pages to implement count,
offset, filtering, sorting, or any other query semantic.

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

The same component also supports exact equality as a complete index-prefix
term. For example, an index ordered by `(organization_id, relation, user,
document_id)` with `binary_utf8_v1` text keys on `relation` and `user` can back
both `order by relation, user, document_id` and `relation == $relation` followed
by `order by user, document_id`. This is one maintained index: the compiler
must prove each named shape independently, and the runtime does not walk pages
or filter after `take`.

It also supports one bounded membership branch when the text component is the
first remaining order term:

```riffql
where organization_id == $organization_id
  && object_id in $object_ids
order by object_id asc, relation asc, user asc
take 25 after $after
```

Declare `object_id` as `text_key(object_id, binary_utf8_v1)` in that shared
index. RiffDB canonicalizes the submitted set once, lowers each distinct member
to an exact byte prefix once, orders the prefixes physically, and applies one
global page, plus-one probe, scan budget, and cursor across the union. Empty
sets return an exact empty page. Forward and reverse traversal both retain
bytewise order, so length-divergent values such as `doc-3` and `doc6` do not
fall back to canonical string ordering.

## Closed ordinary access matrix

| Index component | Exact | Bounded `in` | Interval / complement | Prefix | Order |
|---|---:|---:|---:|---:|---:|
| Canonical scalar | yes | first remaining order term | ordered numeric/time/UUID/enum scalars | no | canonical |
| Presence-aware | state only | no | no | no | explicit accepted state placement |
| `binary_utf8_v1` | yes | first remaining order term | UTF-8 byte interval / complement | exact leading bytes | UTF-8 bytes |
| `unicode_fold_v1` | yes | first remaining order term | folded-byte interval / complement | folded leading bytes | folded bytes |

For an order-preserving canonical `i64`, `u64`, `timestamp`, `date`, `uuid`, or
enum component, or a bounded string declared with either text-key profile, `<`,
`<=`, `>`, and `>=` compile to inclusive/exclusive physical endpoints. Binary
text uses exact valid UTF-8 byte order without normalization, collation, case
folding, or token parsing. `unicode_fold_v1` uses the frozen Unicode 17.0.0 NFKC
plus full non-Turkic case fold and compares the resulting bytes. Index
maintenance and submitted predicates share that exact transform, while results
retain the original authorized string. One lower and one upper bound may form
an interval; `!=` forms two disjoint complement intervals. The selected
component must supply the first remaining order term. Memory and redb traverse
the same normalized half-open schedule, including reverse continuation across
an interval boundary, under one page, plus-one probe, scan, hydration, cursor,
and output budget. Contradictory runtime bounds produce an exact empty page
without a scan.

Canonical length-prefixed strings are not logical text-order ranges; use an
explicit `binary_utf8_v1` component for bytewise string intervals or
`unicode_fold_v1` for folded-byte intervals. Multiple
branching dimensions, overlapping unions, joins, provider bridges,
caller-selected indexes, and general client page walking remain unsupported. An older
module whose range was never physically proved is refused as query unavailable
with `refresh_contract` guidance; recompile and redeploy rather than filtering
a returned page.

The repository audit found no active unsafe ordinary fixture: Ticketdesk's
canonical membership, the presence and binary-prefix conformance queries, and
dependent complete-key batches all constrain storage before page selection.
The richer directory filtering fixture uses the separately governed exact
result-set provider. Historical external ordinary range queries require
recompilation so the compiler can prove their physical interval shape;
unsupported scalar or multi-branch forms remain refused.

## Extending ordinary access paths

Ordinary access support is a closed compiler matrix, not a collection of
backend conveniences. Adding an encoding, comparison profile, role, or role
combination requires all of the following in one reviewed change:

1. Freeze the logical equality/order meaning and the exact compatible
   component roles.
2. Prove one complete partition prefix, branching or interval component,
   total-order suffix, unique tie-breaker, authority set, and static bounds.
3. Lower typed values once per step and request into the shared immutable range
   schedule; never validate profiles or construct ranges per row.
4. Pass the same forward/reverse, exact-end/plus-one, cursor, policy, fuel, and
   cancellation matrix in memory and redb.
5. Add a real-consumer amendment for any new relationship shape and separately
   bound driver rows, fan-out, intermediates, probes, bytes, output, missing
   behavior, policy, and identity.

The neutral application at
`fixtures/riffql/operational-access-corpus-v1` freezes equality, canonical and
binary-text membership, prefix, typed canonical and binary-text interval and
complement, nullable state and order, forward/reverse cursor, row-policy
authority, and dependent complete-key shapes. Its exact lock and Rust, Go,
TypeScript, Python, and MCP artifacts are checked by
`scripts/generate-operational-access-corpus --check`.

`fixtures/riffql/operational-query-capability-v5.json` binds that corpus, the
unchanged V4 capability receipt, and a value-free external tuple-changelog
source-compilation and Go-only package receipt. The external receipt proves
only the named access shapes it lists. It deliberately does not copy an
external contract, schema, route, adapter, generated profile, or stored value
into RiffDB, and it does not claim external runtime or full framework
conformance.

The separate terminal receipt at
`fixtures/riffql/wp775-external-binary-text-interval-execution-v1.json`
records one fresh owning-repository execution. A consumer pinned to the exact
recorded RiffDB lock and generated Go artifact invoked one named strict-lower/
strict-upper query twice over one partition and reused an original logical
string returned by the first invocation as the second invocation's
consumer-owned lower token. The receipt retains only exact identity hashes,
bounded counts and duration, and closed verification facts. It proves this
narrow delegation and bytewise-order case only—not a framework route, full
adapter, or full-application conformance—and it neither imports consumer
vocabulary or values nor turns that logical token into a RiffDB cursor.
The receipt's `riffdb_revision` is the exact runtime revision that performed
the run; the later repository revision that carries the immutable receipt is
not reinterpreted as an execution revision.

## Feature preflight

The authorized application catalog returns the closed
`riffdb.application-catalog/v1` feature registry for one exact contract. Rust
applications can call
`StableApplicationClient::preflight_application_features`. The checked result
contains only the exact contract identity, active module hashes, and the six
closed feature states; it contains no raw catalog symbols, numeric compiler
identities, capability record, IR, or storage key.

`unavailable` is a prohibition, not a request for client emulation. In the
current alpha, binary UTF-8 and `unicode_fold_v1` prefix lookup are available.
Applications submit only logical strings; the compiler-selected profile owns
the exact transform and physical range. Application code must not replace an
unavailable feature with client filtering, raw reads, N+1 requests, or
handwritten query text.

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
`not_in` sets retain their documented exact semantics. Plain `Limit` is
positive and at most 499; `Limit<MAX>` enforces its smaller compiler-declared
maximum and is charged at that maximum. Offset is bounded by the compiled family. Every declared
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
sorting, counting, raw query construction, or storage access. An owning
external adapter may perform ADR-0159's narrow exact-page translation when its
authoritative interface requires more rows than one compiled RiffDB page: it
requests `min(remaining, MAX)`, follows one unchanged cursor chain, appends
every row exactly once in order, and returns the final cursor unchanged or none
on true exhaustion. It may not discard, reshape, restart, or release partial
success after any cancellation or error.
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
The corpus also proves optional first-page and generated continuation calls in
all four languages, that a malformed generated cursor fails closed, and that
each response stays within its declared page bound. It scans the application
runners for kernel, storage, numeric-ID, raw-transaction, and client-filter
escape hatches. External adapter repositories own their separate route-level
compatibility matrices.
