# RiffQL language version 1

RiffQL is RiffDB's formal, symbolic, read-only application language. It is an
inspectable compiler input, not natural-language execution and not SQL.

A named query declares typed `$parameters`, then ordered `one`, `maybe`, or
`many` bindings. Every binding has a `where` predicate. `one` declares an
absence outcome; every `many` has an explicit positive `take` bound. Ordering
is explicit and cursors are optional typed parameters.

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
