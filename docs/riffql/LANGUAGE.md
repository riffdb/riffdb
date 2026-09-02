# RiffQL language versions

RiffQL is RiffDB's formal, symbolic, read-only application language. It is an
inspectable compiler input, not natural-language execution and not SQL.

A named query declares typed `$parameters`, then ordered `one`, `maybe`, or
`many` bindings. Every binding has a `where` predicate. `one` declares an
absence outcome; every `many` has an explicit positive `take` bound. Ordering
is explicit and cursors are optional typed parameters.

For application pages, start with a fixed bound such as `take 25` or `take 50`.
Use `Limit<MAX>` when callers need to choose a smaller page at runtime while
the compiler must prove a reviewed maximum. `MAX` may be as large as 65,534;
the compiler still refuses a query whose independently bounded result bytes,
cost, authority, provider work, or transport envelope cannot support it.

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
query OpenTickets($tenant: TenantId, $limit: Limit<499> = 25) {
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
shape and current activation status. Tokenized-text language V10 adds four
compiler-sealed `matching` shapes and the optional fixed `riff_bm25_v1` order;
see [Tokenized Text Search](TOKENIZED-TEXT.md). Candidate language V11 adds
complete-before-order set sources, including declared long-value exact pattern
providers; see [Long-Value Pattern Search](LONG-PATTERN.md). RiffQL has no
mutation, SQL escape, function call, recursion,
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

## Compiler-declared bounded runtime limits (language V9 and V11)

`Limit<MAX>` is a query-only page-size refinement with an implicit minimum of
one and an inclusive maximum from 1 through 65,534:

```riffql
query OpenTickets(
    $tenant: TenantId,
    $limit: Limit<100> = 25,
    $after: Cursor?,
) {
    many tickets from Ticket
        where tenant_id == $tenant && status == TicketStatus.Open
        order by updated_at desc, ticket_id desc
        take $limit after $after
    return Found { tickets: tickets { ticket_id title priority } }
    outcomes Found
}
```

`MAX` is a canonical unsigned decimal literal. Zero, leading zeros, a maximum
above 65,534, optional or set wrappers, and defaults outside 1..=`MAX` are
compile-time errors. At runtime, an omitted parameter uses its compiled default;
otherwise zero, a wrong type, or a value above `MAX` is rejected before any
provider or storage work.

The compiler charges every binding at its declared `MAX`, never its default or
submitted value. Charges remain cumulative when a query has several bindings.
Bounds through 499 retain language V9, query IR V12, and module V12. Bounds
from 500 through 65,534 use the additive language V11, query IR V14, and module
V14 identities. The maximum is also part of generated input schemas, roles,
plans, locks, and cursor-bound operation identity, so changing it requires the
normal immutable query deployment rotation.

Fixed `take` literals retain their exact bound. `Limit<MAX>` is also accepted
as an existing `nearest` K parameter, but it narrows only returned page work;
a projection provider's separately declared candidate or full-population work
remains unchanged. This is not a general integer refinement and cannot be used
for offsets, byte budgets, or contract fields.

For an ordinary ordered `take $limit after $cursor` page, including the final
root page after a complete candidate pipeline, the submitted limit controls
only that invocation's cardinality. A continuation may be resumed with another
valid value in the same `Limit` or `Limit<MAX>` domain. The cursor still binds
the immutable plan—including the declared maximum—and every invariant
parameter, predicate, order, authority, snapshot, physical profile, and
provider epoch. A limit used by `nearest`, by an unpaged binding, by a candidate
source, or by any mixed semantic shape remains cursor-identity-bearing. There
is no source annotation for this distinction; the compiler derives it from the
closed plan.

For an explicit finite set of aggregate partitions, use
`Set<Entity.partition_field, MAX>` and route the binding with
`partition_field in $parameter`. `MAX` is 1 through 65,535; the empty submitted
set is valid. The database executes one sealed local plan at one snapshot and
applies global order, limit, and cursor selection after merging the selected
partitions. See [Bounded partition-set queries](PARTITION-SET-QUERIES.md) for
the complete locality, scale, authorization, candidate, and cursor rules.

## Complete candidate sets before root ordering (language V11)

A query may declare one compiler-owned, non-output candidate binding before its
ordinary root binding. The binding projects one type-exact root-key component
from up to eight declared, same-partition indexes and applies exactly one of
single-source deduplication, intersection, union, or authorized difference:

```riffql
candidates matching: Experiment.experiment_id
    from intersect {
        ExperimentTag.experiment_id using by_tag_digest
            where scope == $scope && tag_key == $first_key && value_digest == $first_digest,
        ExperimentTag.experiment_id using by_tag_digest
            where scope == $scope && tag_key == $second_key && value_digest == $second_digest,
    }
    within 65535
    else IntegrityFailure

many experiments from Experiment
    where scope == $scope && experiment_id in matching
    order by last_update_time desc, experiment_id asc
    take $limit after $after
    else IntegrityFailure
```

Every source completes and deduplicates independently, then the set operation
completes, before any root entity is hydrated, policy-checked, ordered, paged,
or assigned a cursor. Exceeding a source, distinct-key, key-byte, hydration,
sort, work, or output bound returns only the declared refusal; no partial set,
row, hidden count, or cursor is observable. A difference expression must name
the complete policy-filtered root-key universe first, followed by its negative
sources separated by `;`, so denied rows cannot be inferred through complement.

Authorization retains each compiler-declared candidate source as an
independent scan ceiling. A role must cover every source individually, and the
authorizer checks their bounded sum against the sealed plan cost; one source
cannot borrow unused authority from another. Ordinary non-candidate queries
continue to use one whole-request scan ceiling.

The compiler proves the relationship mapping, common partition parameter,
declared source index, exact key type, finite source count, and exactly one root
`in` consumer. Candidate names, sources, operators, indexes, and budgets do not
appear in SDK or MCP schemas. They are immutable module/plan/role identity,
not request data. This is bounded same-partition existence filtering, not a
general join, recursive query, caller-selected plan, or runtime optimizer.

## Bounded one-to-many expansion (language V14)

A later `many` binding may declare one earlier bounded `many` binding as its
driver and one per-driver target bound:

```riffql
many comments from Comment
    for each ticket in tickets
    where organization_id == $organization_id
        && ticket_id == ticket.ticket_id
    order by created_at asc, comment_id asc
    take 8 per ticket
```

The target must use one declared same-partition index whose prefix is bound by
the common partition route and driver key. The compiler rejects an absent or
later driver, a driver that is itself expanded, a mismatched `per` name, an
independent target cursor or offset, a non-prefix index shape, a cross-partition
shape, or a driver-maximum times per-driver-maximum product above 65,535. The
return block must nest the target binding directly below its driver binding.

Expansion structure is immutable compiler identity. Generated clients, MCP
tools, and application callers still submit only the declared business
parameters. Memory and redb execute every target access inside the driver's
one authorized snapshot and release a result only after every driver's bounded
target set is complete. Row and field policy run before a target can affect
membership, order, count, or nesting. Generated Rust, Go, TypeScript, and
Python clients expose the nested list as a typed field; gRPC, MCP, and CLI use
the same name-addressed list-of-record carriage. A target has no independent
cursor: paging remains on the driver binding.

A candidate source may instead name a compatible contract `pattern_index` and
one compiled exact pattern predicate. That source is supplied by one ready,
policy-aligned provider epoch and is completed under the same participant proof
as ordinary sources before root work. Pattern postings only choose candidates;
the provider verifies complete retained matched values. See
[Long-Value Pattern Search](LONG-PATTERN.md) for declaration, matching, bounds,
freshness, and negation rules.

### Finite root-order families (language V12)

When one public operation needs several caller-selected orders, the query may
bind a contract enum to a finite family of complete immutable orders:

```riffql
$order: ExperimentOrder,

many experiments from Experiment
    where scope == $scope && experiment_id in matching
    order by $order {
        NameAsc: name asc, experiment_id asc;
        NameDesc: name desc, experiment_id asc;
        CreatedDesc: creation_time desc, experiment_id asc;
        UpdatedDesc: last_update_time desc, experiment_id asc;
    }
    take $limit after $after
    else IntegrityFailure
```

The family must declare every variant of its contract enum exactly once and is
limited to 32 members. The compiler expands each member before deployment and
checks its fields, directions, null placement, unique tie-breaker, authority,
cost, and candidate/root semantics independently. The service resolves the
enum to a compiled member before storage work. Generated Rust, Go, Python,
TypeScript, MCP, CLI, gRPC, local, and remote paths carry only that closed enum;
there is no public field list, expression, index hint, collation, direction, or
tie-breaker input.

Order families use additive RiffQL V12, query-family IR V15, and module V15.
A successful result reports the complete finite family's compiler identity,
which is the identity pinned by generated clients and application catalogs.
The selected member plan and the enum parameter are additionally cursor-bound,
so a cursor cannot be resumed under another order. Candidate families always
capture one admission head; all sources complete at the resulting snapshot
before root policy, total sorting, paging, and cursor formation.

## Nearest-neighbor bindings (alpha)

A `many` binding over an entity that declares a contract `vector_field` may
replace its ordering and bound with a `nearest` clause:

```riffql
query SimilarDocuments(
    $org_id: Document.org_id,
    $query_vec: Document.embedding,
    $k: Limit<499>,
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
- K is either a positive integer literal, `Limit`, or `Limit<MAX>` parameter.
  All share the 65,534-row structural page-take ceiling; `Limit<MAX>` also
  enforces its declared maximum. A provider's smaller compiled candidate or
  partition ceiling remains authoritative. Invalid values are rejected before
  nearest provider or storage work begins.
- The binding still requires the exact partition (organization) equality
  predicate that routes all query access; a nearest query without it does
  not compile (`RDB-QP002`).
- The static plan cost charges the provider's complete compiled partition-scan
  ceiling, not K: exact nearest search examines the whole admitted partition.
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

That declared text-key component may also provide ordinary bounded ordering
without a prefix predicate. For example, an index
`(organization_id, title, document_id) text_key(title, binary_utf8_v1)` proves
`order by title asc, document_id asc` after the partition equality, and proves
the wholly reversed descending order as well. Ordering is by the original
canonical UTF-8 bytes: `doc-3` sorts before `doc6`. A plain string index retains
RiffDB's canonical length-first value order and is not reinterpreted as a text
key. The compiler still requires the complete index suffix and deterministic
key tie-breaker, and an opaque continuation remains bound to the exact plan,
direction, text profile, index epoch, authorization, and snapshot.

Exact equality consumes a complete `binary_utf8_v1` component as an index
prefix. This permits progressively narrower compiled queries to reuse one
declared index: `(organization_id, relation, user, document_id)` with text keys
on `relation` and `user` proves both `order by relation, user, document_id` and,
after `relation == $relation`, `order by user, document_id`. Equality retains
the ordinary typed string truth semantics; the profile transform is used only
to form the matching physical index prefix.

Bounded `in` may consume one `binary_utf8_v1` component when that component is
also the first remaining order term. Each canonical set member becomes one
disjoint exact byte prefix; the complete set shares one page limit, scan
budget, plus-one continuation probe, and opaque cursor. Submitted sets are
sorted and deduplicated once, and an empty set returns an exact empty page
without storage work. Multiple `in` dimensions and an `in` predicate followed
by another filtered index component are rejected rather than forming a
Cartesian prefix product or a residual filter.

The ordinary executable component matrix is closed: canonical components
support exact equality, bounded membership at the first remaining order term,
canonical order, and typed intervals for order-preserving `i64`, `u64`,
`timestamp`, `date`, `uuid`, and enum components; presence components support
state selection and explicit state placement; `binary_utf8_v1` supports
equality, bounded membership, leading-byte prefix, bytewise interval and
complement predicates, and bytewise order;
`unicode_fold_v1` remains unavailable. A lower and upper comparison form one
half-open physical interval. `!=` forms at most two disjoint complement
intervals. Inclusive source bounds are normalized around the complete encoded
component before storage traversal, and the interval component must be the
first remaining total-order term.

All intervals in one query share a single page limit, continuation probe, scan
budget, cursor, and output budget. Reverse traversal reverses both interval
order and the rows inside each interval. Contradictory submitted bounds return
an exact empty page without scanning. Canonical length-prefixed strings do not
preserve logical text comparison and therefore cannot prove a range; declare
`text_key(field, binary_utf8_v1)` for exact valid UTF-8 bytewise interval
semantics. Physical ordered bytes never replace the returned logical string,
and the database performs no token parsing, normalization, or collation.
Multiple interval dimensions, overlapping unions, intersections, skip scans,
and post-page residual predicates remain unavailable.

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
    $limit: Limit<499> = 50,
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

## Bounded exact aggregates (language versions 2 and 8)

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
results, each with at most eight grouping keys and 16 measures. Version 2
provides `count()`, `sum(field)`, `min(field)`, and `max(field)`. Additive
language version 8 is selected when a query uses any of:

- `count_present(field)`, which excludes canonical `NoValue`;
- `count_distinct(field)`, which includes `NoValue` as one exact typed value;
- `count_distinct_present(field)`, which excludes `NoValue`;
- `mean(field)`, which returns `ExactMeanV1 { total, count }` without division,
  rounding, or floating point; or
- `any(field)` and `all(field)` over required Boolean fields.

Function names, input fields, aliases, grouping keys, and the source binding
are source declarations; callers cannot submit any of them at runtime.

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

The exact aggregate core independently bounds admitted rows, groups, distinct
values per measure/group, partial-state bytes, checked arithmetic operations,
scans, result cells, and encoded output. Empty distinct and present counts are
zero; empty `mean` is `{ total: 0, count: 0 }`; empty `any` is `false`; and
empty `all` is `true`. A bound or arithmetic failure refuses the whole query.

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
