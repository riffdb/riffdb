# Exact predicate and order families

RiffQL language V6 freezes a provider-independent semantic model for bounded,
exact operational result sets. It is a compiler surface, not a public query
object. Applications still invoke named generated operations; callers never
submit fields, operators, Boolean trees, order expressions, provider choices,
cost hints, or fallback flags.

The compiler accepts the following closed predicate vocabulary inside one
partition-routed `many` binding:

- `==`, `!=`, `<`, `<=`, `>`, and `>=` over one exact scalar type;
- bounded typed `in` and `not_in` sets;
- `is null`, `is not null`, and `exists` state tests;
- binary UTF-8 `prefix`, `starts_with`, `ends_with`, and `contains`;
- conjunction and compiler-capped disjunction; and
- `when $optional { ... }` guards whose complete presence family is compiled
  before deployment.

Value predicates are two-valued. Missing and explicit-null fields satisfy none
of the value or complement operators. Use a state test when state is relevant.
An empty `in` set matches nothing; an empty `not_in` set matches every present,
non-null value. Submitted sets are sorted and deduplicated canonically before
they enter execution or identity. Text uses exact binary UTF-8 semantics; no
locale, regex, wildcard, or implicit normalization exists.

An exact family also declares a nonempty total order independent from its
search fields. The order ends with the complete canonical entity key in
ascending direction. Order fields must be present and non-null for the whole
installed lineage. The query combines that order with `exact_count()` and a
bounded `take $limit offset $offset` window:

```riffql
query SearchUsers(
  $organization_id: User.organization_id,
  $needle: User.email,
  $states: Set<User.state>,
  $after: User.created_at?,
  $limit: Limit,
  $offset: u64
) {
  many users from User
    where organization_id == $organization_id
      && email contains $needle
      && state not_in $states
      && when $after { created_at >= $after }
    order by created_at desc, user_id asc
    take $limit offset $offset

  aggregate totals from users { exact_count() as total }
  return Found {
    users: users { user_id email state created_at }
    totals: totals { total }
  }
  outcomes Found
}
```

Every predicate and order field requires a declared partition-routed index.
One missing index, invalid type, incomplete key tie-breaker, excessive Boolean
shape, or unsupported family member rejects the complete named query with a
source-spanned diagnostic. There is no partial family and no runtime scan.

## Current activation status

WP-656 makes V6 source, semantic IR V9, and query-module V9 compilable and
reproducible. WP-657 adds rebuildable provider-state format V4. The compiler
seals optional-family members, comparison profiles, policy mode, exact-count
and ordinal requirements, independent order layouts, and static work/state
bounds into its provider identity. Existing V1–V8 source, plan, module, and V1
through V3 provider identities remain byte-exact.

V4 builds one policy-aligned partition in the background. Equality, range,
set, state, and exact-text postings combine inside that provider. Each declared
order has its own bounded ordinal index; exact count and offset selection use
bitset rank/select rather than walking skipped rows or pages. Bounded row-level
policy admission happens before a row can enter the provider universe, so a
denied row cannot affect complements, counts, ordering, ordinals, diagnostics,
or lifecycle selection.

The shared public service, transports, and generated clients remain owned by
WP-658. Until that package activates, application traffic must use the existing
narrow exact-text result form. A V6 query never degrades to an authoritative
request-time scan, client-side filter or sort, page walk, materialized full
population, cross-provider identifier transfer, or a nearby older provider.

Provider rebuilds are currently bounded full-partition rebuilds after an
authoritative frontier change. This is derived work, not request work, and is
limited by the compiler-declared candidate and amplification ceilings. A later
implementation may incrementally maintain the same V4 identity without
changing query semantics.
