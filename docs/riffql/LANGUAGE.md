# RiffQL language version 1

RiffQL is RiffDB's formal, symbolic, read-only application language. It is an
inspectable compiler input, not natural-language execution and not SQL.

A named query declares typed `$parameters`, then ordered `one`, `maybe`, or
`many` bindings. Every binding has a `where` predicate. `one` declares an
absence outcome; every `many` has an explicit positive `take` bound. Ordering
is explicit and cursors are optional typed parameters.

For v1 application pages, start with a fixed bound such as `take 25` or
`take 50`. Use a `Limit` parameter only when the role can afford its full
500-row static charge; its default does not reduce that proof obligation.

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

The v1 expression set is `==`, `!=`, `<`, `<=`, `>`, `>=`, bounded `in`,
`&&`, and `||`. It has no mutation, SQL escape, function call, recursion,
loop, callback, clock, randomness, network, filesystem, group-by, unrestricted
scan, or arbitrary join.

The parser accepts at most 1 MiB of UTF-8 source, 131,072 tokens/AST nodes,
32 nesting levels, 4,096 bindings, and 1,024 items in any local collection.
Diagnostics are value-free and use stable `RDB-QS001` through `RDB-QS009`
codes.

A `Limit` parameter is statically charged at its full 500-row range, not at its
default. The index-scan maxima of all bindings in one query are cumulative.
Pages with multiple collections should use fixed `take` values whose complete
scan total is at most 500. This is checked again while deriving every symbolic
application role; `RDB-AR007` names the role and query before a lock is written.

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

Application evidence did not justify `exists`, aggregate, computed-field,
fragment, search, projection-source, or general-join syntax. Such a construct
still requires repeated domain evidence and a separate accepted ADR before any
grammar or IR change.
