# RiffQL language version 1

RiffQL is RiffDB's formal, symbolic, read-only application language. It is an
inspectable compiler input, not natural-language execution and not SQL.

A named query declares typed `$parameters`, then ordered `one`, `maybe`, or
`many` bindings. Every binding has a `where` predicate. `one` declares an
absence outcome; every `many` has an explicit positive `take` bound. Ordering
is explicit and cursors are optional typed parameters.

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
