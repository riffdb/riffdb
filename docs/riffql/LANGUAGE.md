# RiffQL language versions

RiffQL is RiffDB's formal, symbolic, read-only application language. It is an
inspectable compiler input, not natural-language execution and not SQL.

A named query declares typed `$parameters`, then ordered `one`, `maybe`, or
`many` bindings. Every binding has a `where` predicate. `one` declares an
absence outcome; every `many` has an explicit positive `take` bound. Ordering
is explicit and cursors are optional typed parameters.

For v1 application pages, start with a fixed bound such as `take 25` or
`take 50`. Use a `Limit` parameter only when the role can afford its full
499-row page maximum; its default does not reduce that proof obligation.

`one` and `maybe` are primary-key point reads: their predicates must constrain
the target entity's complete primary key. A secondary index does not satisfy a
singular binding. For a singular lookup by an external identifier such as a
slug, declare a small route entity keyed by that identifier, point-read the
route, and then point-read the target entity through its stored key. This
makes the cardinality proof explicit and prevents data-dependent scans.

One deliberately narrow collection dependency is available for operational
junction reads. An earlier bounded `many` field may be consumed by `in` only to
supply one component of a later `many` binding's complete primary key:

```riffql
many ticket_labels from TicketLabel
    where organization_id == $organization_id
      && ticket_id == ticket.ticket_id
    order by label_id asc
    take 50

many labels from Label
    where organization_id == $organization_id
      && label_id in ticket_labels.label_id
    order by label_id asc
    take 50
    else IntegrityFailure
```

The target bound cannot exceed the source bound. Both accesses must remain in
one partition, source and target keys must be in canonical ascending order, and
every target must exist. An empty source produces an empty list; a missing
target selects the declared `else` outcome. Collection-as-scalar use, non-key
fan-out, nested per-row collections, and Cartesian products are rejected.

The source collection must be a complete ordered set of target keys: its
`order by` is exactly the consumed field, ascending. If the source model allows
the same target key more than once and therefore needs a tie-breaker such as
`line_id`, it is not a dependent-key set and is rejected with `RDB-QP007`.
Model one line per product when that is the domain invariant, denormalize the
small immutable display field onto the line, or use a separately justified
projection. RiffDB does not silently deduplicate, reorder, or issue per-row
reads because each would change declared cardinality or hide work.

```riffql
query OpenTickets($tenant: TenantId, $limit: Limit = 25) {
    many tickets from Ticket
        where tenant_id == $tenant && status == TicketStatus.Open
        order by updated_at desc, ticket_id desc
        take $limit

    return Found {
        tickets: tickets {
            ticket_id
            title
            priority
        }
    }

    outcomes Found
}
```

Multiple declared outcomes use `|`, never commas or whitespace alone:

```riffql
outcomes Found | NotFound | IntegrityFailure
```

Every `many` binding must have a declared contract index whose equality prefix
and ordering match the query. When one is absent, add the compiler-suggested
bounded index to the contract rather than filtering or sorting in application
code.

The ordinary operational expression set is `==`, `!=`, `<`, `<=`, `>`, `>=`,
bounded `in`, `&&`, and `||`. Exact-result language V6 additionally recognizes
bounded `not_in`, `starts_with`, `ends_with`, and `contains` while compiling a
provider-independent semantic family. See
[Exact Predicate and Order Families](EXACT-PREDICATES.md) for its stricter
shape and current activation status. RiffQL has no mutation, SQL escape,
function call, recursion,
loop, callback, clock, randomness, network, filesystem, group-by, unrestricted
scan, or arbitrary join.

The parser accepts at most 1 MiB of UTF-8 source, 131,072 tokens/AST nodes,
32 nesting levels, 4,096 bindings, and 1,024 items in any local collection.
Diagnostics are value-free and use stable `RDB-QS001` through `RDB-QS009`
codes.

## Exact secret outputs (language V3)

A newly compiled named query may return a stored `secret` field only when the
result leaf declares the exact source once:

```riffql
return Found {
    session: session {
        token_hash reveals session.token_hash
    }
}
```

`reveals` is contextual; contracts, bindings, aliases, and fields may still be
named `reveals`. The declaration is legal only on a direct stored scalar leaf,
must match the projected binding and field exactly, and must name a
secret-classified field. Missing, duplicate, ordinary-field, mismatched,
record, aggregate, and excess declarations fail compilation with `RDB-QR007`.
There is no wildcard, whole-record, expression, cursor, or parameter form, and
one query is limited to 1,024 declared secret leaves.

When a result field itself is the bare contextual identifier `reveals`, keep
its comma delimiter before a following field. The canonical formatter emits
that delimiter automatically so parse-format-parse cannot reinterpret the
field as another secret-output clause.

The declaration records review intent in RiffQL language V3 and query IR V4;
it is not caller-supplied authority. Selecting the immutable query in an
application role derives only its exact dedicated secret-field visibility.
Without that current role authority, execution returns `RDB-AUTH-0214` before
releasing any result. Secret-bearing named queries remain available through
typed SDK and gRPC execution, but are omitted from MCP tool generation and
cannot be referenced by reactive watches, live queries, hydrations, or
contextual subscriptions.

A `Limit` parameter is statically charged at its full 499-row page range,
not at its default. The index-scan maxima of all bindings in one query are
cumulative. Pages with multiple collections should use fixed `take` values
whose complete scan total fits the authorized whole-query budget. This is
checked again while deriving every symbolic application role; `RDB-AR007`
names the role and query before a lock is written.

## Nearest-neighbor bindings (alpha)

A `many` binding over an entity that declares a contract `vector_field` may
replace its ordering and bound with a `nearest` clause:

```riffql
query SimilarDocuments(
    $org_id: Document.org_id,
    $query_vec: Document.embedding,
    $k: Limit,
) {
    source projected Document.embedding
    freshness causal inherit_session_commit true max_wait_ms 500

    many results from Document
        where org_id == $org_id
        nearest(embedding, $query_vec, $k)
    return Found { results: results { title } }
    outcomes Found
}
```

- `nearest` is valid only on `many` bindings. It replaces `order by` (rows
  come back in ascending distance order) and `take` (K is the binding's
  checked page bound).
- K is either a positive integer literal or a typed `Limit` parameter. Both
  share the 499-row page-take ceiling. A submitted parameter value of 0 or
  500 is rejected as invalid input before nearest storage work begins.
- The binding still requires the exact partition (organization) equality
  predicate that routes all query access; a nearest query without it does
  not compile (`RDB-QP002`).
- The static plan cost charges the full 500-row partition-scan ceiling, not
  K: exact nearest search examines the whole partition.
- Production execution requires the exact compiler-owned
  `source projected Entity.vector_field` declaration. The source must match
  the nearest binding and a production-capable contract vector field; it is
  never selected from a request or inferred from whichever projection happens
  to exist.
- `freshness available` serves the currently published generation and reports
  its frontier. `freshness causal inherit_session_commit true max_wait_ms N`
  waits for the generated client's scoped commit fence for at most the
  compiler-bounded duration. `freshness bounded max_lag_ms N` compares trusted
  logical timestamps at the authoritative head and projection frontier; it
  never estimates elapsed time from sequence distance or process wall time.
- A vector source is queryable only while its durable generation is `Ready`
  and exactly matches the compiler-owned definition. Initial build, successor
  rebuild, replay-budget detachment, and restart recovery return typed
  building/rebuilding failures; they never silently serve the previous or a
  partially rebuilt generation. A successful result's frontier is the exact
  durable checkpoint frontier of that generation.
- The first production form is intentionally one nearest `many` binding with
  equality-parameter scalar predicates and fields from that entity. Mixed
  authoritative/projected bindings, dependent relations, aggregates, and
  multiple vector sources fail at compile time rather than mixing snapshots.
- Current row policy filters the complete bounded candidate set before scalar
  predicates, distance calculation, ranking, K, counts, or output. The same
  capability and policy identity is revalidated before release.
- `nearest` is a contextual word, not reserved — a contract field named
  `nearest` remains fully queryable.

The production path uses the automatically registered `Entity.field` columnar
projection and the shared 500-row admission ceiling. If the contract declares
`ann_threshold` and `recall_target_bps`, the server uses exact search at or
below that per-organization threshold and first-party HNSW strictly above it.
The choice and recall floor are compiler-owned; neither is a request option.
See [Vector Search](../getting-started/VECTOR-SEARCH.md) and
[Known Limitations](../known-limitations.md).

## Accepted application-profile target

The following boundary is accepted by ADR-0055 and assigned to WP-280 through
WP-300; it is not a claim about the pre-WP-300 surface.

- Stable applications execute exact named deployed queries with typed
  parameters. They do not submit source text.
- Scoped agents may check, explain, or execute ad-hoc RiffQL only through
  separately granted permissions.
- Raw `GetEntity` and `ScanIndex` remain kernel/administrative operations. Their
  permissions do not authorize RiffQL, and RiffQL permissions do not authorize
  them.
- Generated stable-application clients expose neither raw field/index IDs nor
  constructors for source text, field masks, encoded keys, or kernel requests.

Required same-partition relationships and same-partition unique keys are
contract declarations rather than RiffQL predicates. RiffQL may navigate a
declared relationship only when it still lowers to the bounded complete-key
accesses described above. A query never creates a missing integrity guarantee
by convention or inference.

The symbolic catalog exposes each relationship by source name, ordered source
fields, target entity, and complete ordered target key. That metadata grants no
new join operator. Current RiffQL must still spell an accepted point read or
bounded dependent batch; future navigation syntax must compile to those same
plans.

Declared uniqueness is not implemented as a RiffQL `exists` or `count`
preflight. Commands that establish or change a unique tuple carry
compiler-derived conflict and transaction-current occupancy plans; a read
query cannot acquire write authority or make a later write safe.

## Application-safety product rule

An application operation is accepted only when RiffDB can prove its complete
typed access program before any data access. The proof must identify the exact
contract and operation, authorize every returned field and access path, route
every access to one partition, select declared indexes or complete keys, bound
all work and output, establish cardinality, and execute the read against one
engine-owned snapshot. If any proof is missing, the operation is rejected with
a source-spanned symbolic diagnostic.

There is no application fallback to an unrestricted scan, client-side join or
sort, partial field redaction, N+1 public requests, cross-partition access,
string-built predicate, or optimizer-dependent plan. This is the key
difference from treating RiffQL as a smaller SQL dialect: the rejected forms
are product safety properties, not optimizer preferences.

WP-335 exercised this rule against independent Blog/CMS and Orders/Inventory
corpora. Both completed without a new grammar, IR node, hash input, access
operator, cursor, or result semantic. The positive corpus proves:

- primary-key page and slug-route reads;
- declared-index feeds, moderation queues, histories, and dashboards;
- singular foreign-key reads from prior singular bindings;
- bounded junction-to-entity and line-to-product batches;
- numeric inventory invariants and command-only reservation transitions; and
- identical generated Rust, TypeScript, and MCP operation schemas.

The retained negative corpus rejects an unindexed title ordering
(`RDB-QP003`), a cross-partition author scan (`RDB-QP002`), collection-as-scalar
fan-out (`RDB-QP007`), and a collection without `take` (`RDB-QS009`). Exact
spans, classifications, and smallest remedies are generated in
`fixtures/agent-alpha/gap-report-v1.json`.

The original application evidence did not justify computed-field, fragment,
projection-source, or general-join syntax. Those constructs remain outside the
language. Operational evidence has since justified the closed version-2
predicate and aggregate declarations below; this does not introduce a general
query-expression or optimizer surface.

## Operational optional predicates (language version 2)

Named deployed queries may declare up to eight optional predicate parameters
and guard a top-level conjunct with the matching parameter:

```riffql
query SearchTickets(
    $organization_id: Ticket.organization_id,
    $status: Ticket.status?
) {
    many tickets from Ticket
        where organization_id == $organization_id
          && when $status { status == $status }
        order by updated_at asc, ticket_id asc
        take 25

    return Found { tickets: tickets { ticket_id status updated_at } }
    outcomes Found
}
```

The compiler enumerates the complete presence family at deployment: at most
eight parameters and 256 ordinary bounded plans. Every member must independently
prove the same partition route, result schema, locality, declared access path,
and ordinary RiffQL safety properties. One missing index or unsafe member
rejects the whole query with a source-spanned diagnostic; there is no runtime
planner, scan fallback, predicate object, or caller-selected field/operator.

At invocation, a value selects the present member; omission or explicit null
selects the absent member. Generated bindings expose the parameter as an
optional language value. Authorization covers the union of every family member
and policy charges the component-wise maximum cost before execution. The query
service then selects the exact compiler-owned member by presence and executes
it against one snapshot. Values never influence plan selection.

An operational source has a version-2 language and query-module identity.
Ordinary version-1 sources retain their original canonical bytes and identities.
The ad-hoc source path and version-1 reactive/live-query modules reject
operational families until those surfaces receive their own versioned
presence-selection contracts.

Version 2 executes `is null`, `is not null`, `exists`, and binary UTF-8
`prefix` predicates only through explicitly declared operational indexes. For
example:

```riffql
query SearchDocuments(
    $organization_id: Document.organization_id,
    $title_prefix: Document.title,
) {
    many documents from Document
        where organization_id == $organization_id
          && title prefix $title_prefix
        order by title asc, document_id asc
        take 25
    return Found { documents: documents { document_id title } }
    outcomes Found
}
```

`is null` matches only an explicitly stored null. `is not null` matches only a
present non-null value. `exists` matches both explicit null and present
non-null values; it does not match a field missing from an older compatible
record. The compiler requires `presence(field)` on the selected index so these
three states cannot collapse during planning or execution.

Binary prefix lookup compares the exact UTF-8 bytes of the original string and
requires `text_key(field, binary_utf8_v1)`. It performs no case folding,
normalization, locale collation, tokenization, or relevance scoring. The
`unicode_fold_v1` spelling is reserved but remains unavailable until its frozen
Unicode tables and migration fixtures ship. A parsed spelling is not an
executable feature unless every required storage and planning proof exists.

## Exact indexed result sets (language version 4)

An application that needs an exact whole-population total and numeric offset
may declare the closed exact-result shape. It is available only for a bounded
`binary_utf8_v1` text index, partition-aligned or bounded-row-policy admission,
and one compiler-proved total order with the entity key as the unique tie-breaker:

```riffql
query SearchUsers(
    $organization_id: User.organization_id,
    $needle: User.name,
    $active: User.active?,
    $limit: Limit = 50,
    $offset: u64 = 0
) {
    many users from User
        where organization_id == $organization_id
          && when $active { active == $active }
          && name contains $needle
        order by name asc, user_id asc
        take $limit offset $offset

    aggregate total from users { exact_count() as value }

    return Found {
        users: users { user_id name }
        total: total { value }
    }
    outcomes Found
}
```

The same provider epoch supplies `users` and `total`. `exact_count()` counts
the complete authorized matching population, not the returned page or a
candidate cap. `offset` is a direct indexed ordinal: zero selects the first
row, exact-end and beyond-end return an empty page with the unchanged exact
total, and no earlier row or cursor page is read or discarded. Offset stability
is scoped to the selected snapshot; a later current-snapshot request may see
intervening writes.

When the selected role carries a row predicate, the background provider first
evaluates one bounded authoritative candidate set under the current capability
revision. Only admitted rows enter its count, order, and ordinal structures.
This is not request-time filtering: each capability revision selects separate
derived state, and a revision change requires fresh activation before release.

The closed predicates are `==`, `starts_with`, `ends_with`, and `contains`.
They compare exact UTF-8 bytes; missing and null values do not match, and an
empty needle is rejected. The compiler refuses absent indexes, unsupported
policy shapes, non-total orders, excessive bounds, and any combination the
provider cannot prove. There is no scan, page-walk, client-filter, approximate
count, or fallback execution path.

The current public source shape admits the complete partition equality, one
exact-text predicate, and at most one top-level optional typed equality guard.
The guard must use the same optional parameter on both sides, target a distinct
entity field, and compile in every presence member against declared indexes.
When absent, the V3 provider uses its complete admitted posting family; when
present, it selects the disjoint canonical-value posting family before count
and ordinal selection. Additional, required, disjunctive, or otherwise
unimplemented predicates are rejected at their source span and are never
retained only as metadata or silently omitted by execution.

Ascending and descending text orders are separate compiler-fixed named plan
members; callers pass no field name or sort expression. Both append the
complete ascending entity-key tie breaker. Applications that need several
operator/order choices declare several named queries, preserving a finite
generated surface rather than accepting an arbitrary query structure.

Exact-result requests currently start a fresh one-page result and therefore do
not accept an ordinary query cursor. A causal minimum may be supplied through
the normal generated read-after-commit option. Rebuild and capacity return
temporary query unavailability; divergence, retired snapshots, and an
unsatisfied freshness floor return the typed `RDB-PROJECTION-0101` through
`RDB-PROJECTION-0103` application errors.

## Bounded exact aggregates (language version 2)

Version 2 also reserves a closed aggregate declaration over one earlier,
bounded collection binding:

```riffql
query TicketSummary($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by ticket_id asc
        take 50

    aggregate summary from tickets {
        group by status, priority
        count() as ticket_count
        sum(story_points) as total_points
        min(created_at) as earliest
        max(updated_at) as latest
    }

    return Found {
        summary: summary {
            status
            priority
            ticket_count
            total_points
            earliest
            latest
        }
    }
    outcomes Found
}
```

The grammar is deliberately closed: a query may declare at most 16 aggregate
results, each with at most eight grouping keys and 16 measures. The only
functions are `count()`, `sum(field)`, `min(field)`, and `max(field)`. Function
names, input fields, aliases, grouping keys, and the source binding are source
declarations; callers cannot submit any of them at runtime.

This syntax and its version-3 query-module representation are executable for
named queries. Finite-family compilation resolves every grouping and
measure field, derives result types, includes all source fields in the
authorization union, clamps grouped output to the source row limit, and seals
the descriptors and cost ceiling into module identity. The ordinary version-1
compiler still rejects aggregate declarations with `RDB-QP008`.

The selected source access and every aggregate fold run inside the same
authoritative read snapshot. `count` and `sum` return zero for an empty whole
set; `min` and `max` return absence. Integer sums and fixed-decimal sums use a
checked signed 128-bit accumulator and cross the public boundary as a decimal
with the source scale and no false precision assertion. Overflow, group-bound
exhaustion, fuel exhaustion, or a malformed backend row withholds the complete
query result—there is no partial aggregate response.

Grouped rows are ordered by the canonical encoded group key and their count is
clamped by the source `take` value, including a submitted `Limit`. Version 1
requires aggregate result names and selected aggregate field names to remain
the declaration names; renaming either is rejected at compile time rather than
accepted without a sealed projection mapping. Aggregate live watches remain a
separate future surface: current reactive compilation accepts only an ordinary
single access program and cannot substitute one operational-family member.

## Symbolic catalog schema

The versioned application-catalog response model is frozen as
`riffdb.application-catalog/v1` with at most 100 authorized symbols per page.
It can represent contracts, enums, entities, fields, relationships, indexes,
commands and outcomes, events, query modules and queries, roles, generated
operations, and a closed feature-preflight registry. Paths contain names only;
types and source-relative spans are bounded.

The type deliberately has no hidden or total count, per-symbol visibility bit,
numeric compiler ID, capability record, raw IR, storage key, source text, or
filesystem path. Unauthorized and nonexistent symbols therefore have the same
representation: absence from the authorized page. The exact registry and its
forbidden-field ledger are frozen in
`fixtures/riffql/application-catalog-schema-v1.json`.

The catalog is available through the public
`ApplicationQueryService.GetApplicationCatalog` RPC, the Rust client's
`get_application_catalog` method, and the `riffdb_application_catalog` MCP
tool. Each request selects an active or exact contract, requests between 1 and
100 visible symbols, and may carry the preceding opaque cursor. MCP uses the
same canonical 32-character lowercase cursor spelling as its other bounded
discovery tools.

Catalog access requires contract-read authority. Command and named-query
symbols are then filtered by the caller's exact operation grants before the
page is formed. A continuation is bound to the principal, exact contract,
active query-module identity, requested limit, and prior visibility. A later
authorization change may only narrow subsequent pages. Invalid, expired,
cross-principal, or identity-stale cursors fail as invalid input; RiffDB never
falls back to a first page or discloses which hidden symbol changed.

The feature list is a closed preflight view of the selected application
surface. `available` means the exact compiler/runtime path implements the
feature; `unavailable` is an explicit result and must not be interpreted as
permission to emulate the feature with kernel reads or client-side scans.
