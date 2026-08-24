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

## Nullable order placement

RiffQL V7 adds explicit placement for an order field that may be missing in an
installed schema version or may contain null:

```riffql
order by subtitle asc nulls last, created_at desc nulls first, record_id asc
```

Only `nulls first` and `nulls last` are accepted. They are part of the named
query and its plan/module identity; an application request cannot choose or
override them. Omission is valid only when the compiler proves every admitted
row and installed schema version has a present, non-null value. The complete
ascending entity-key suffix is always present-only and cannot carry a null
placement.

For ordering, a missing field and an explicit null are one `NoValue` class.
They compare equal for that term, so later order terms and finally the complete
entity key resolve the tie. Direction changes only value-versus-value order:
`desc nulls last` still places every `NoValue` after all present values. RiffDB
does not inherit SQL, storage-engine, locale, or host-language null defaults and
does not substitute a sentinel scalar.

WP-675 makes the V7 source and its provider-independent V10 semantic/module
artifacts compilable. WP-676 activates rebuildable provider-state V5 and its
internal exact-count/direct-ordinal execution lane. WP-677 connects that lane
through the shared service, gRPC, CLI, MCP, and generated Rust, Go, TypeScript,
and Python methods. There is no scan, request-time sort, page walk, or older-
provider fallback.

Declare every nullable placement in the checked-in named `.riffq` document,
then regenerate the application lock and typed clients. Generated callers pass
only the operation's domain parameters, `limit`, and `offset`; there is no sort
descriptor to construct. The framework-neutral `InventoryRecord` corpus in
`fixtures/adapters/operational-conformance` demonstrates optional text and
timestamp orders in both directions and placements, later-term `NoValue` ties,
exact totals, direct ordinal boundaries, and missing/null/value transitions.

Every predicate and order field requires a declared partition-routed index.
One missing index, invalid type, incomplete key tie-breaker, excessive Boolean
shape, or unsupported family member rejects the complete named query with a
source-spanned diagnostic. There is no partial family and no runtime scan.

## Current activation status

WP-656 makes V6 source, semantic IR V9, and query-module V9 compilable and
reproducible. WP-657 adds rebuildable provider-state format V4, and WP-658
activates that provider through the shared application service, gRPC, CLI,
MCP, and generated Rust, Go, TypeScript, and Python methods. The compiler
seals optional-family members, comparison profiles, policy mode, exact-count
and ordinal requirements, independent order layouts, and static work/state
bounds into its provider identity. Existing V1–V8 source, plan, module, and V1
through V3 provider identities remain byte-exact.

Explicit nullable placement selects the additive V7/V10/V10 identities even
when the current field is required. Sources without placement continue to emit
V6/V9/V9 when the complete present/non-null proof succeeds; earlier bytes and
hashes are not reinterpreted.

For a successor contract, compilation that omits placement must receive the
complete contiguous lineage beginning at version 1. A current bundle by
itself, an out-of-order lineage, or a lineage with a version gap is not proof
and fails closed with a diagnostic suggesting `nulls first` or `nulls last`.

V4 builds one policy-aligned partition in the background. Equality, range,
set, state, and exact-text postings combine inside that provider. Each declared
order has its own bounded ordinal index; exact count and offset selection use
bitset rank/select rather than walking skipped rows or pages. Bounded row-level
policy admission happens before a row can enter the provider universe, so a
denied row cannot affect complements, counts, ordering, ordinals, diagnostics,
or lifecycle selection.

V5 uses the same partition-scoped lifecycle and admission boundary for nullable
orders. It stores missing and explicit null in one `NoValue` state class, with
the compiler-selected first/last rank, and stores present values under the
existing canonical scalar comparison. Optional text and timestamp orders are
covered by the same state-aware index. Every V5 checkpoint binds the compiled
program, descriptor, policy shape, partition, history incarnation, generation,
frontier, and checksum; V4 refuses V5 bytes rather than reinterpreting them.
Descriptor/layout validation is paid when a provider generation is bound, and
the epoch proof is paid once for the opened result set—not once per row, term,
probe, or returned item.

Callers submit only the generated typed values for one named operation. They do
not submit a predicate, field, operator, order, provider, plan member, policy,
or cost. The service materializes and type-checks values before authorization,
selects the compiler-enumerated optional-presence member, and reauthorizes both
before provider execution and before releasing values. A V6 query never
degrades to an authoritative request-time scan, client-side filter or sort,
page walk, materialized full population, cross-provider identifier transfer,
or a nearby older provider.

The initial request for a new plan/policy/partition slot can return
`RDB-QUERY-0102` while bounded background construction is in progress. The
same typed lifecycle, freshness (`RDB-PROJECTION-0103`), retired-snapshot, divergence, and revocation
failures cross every public transport. The application may use a bounded retry
budget; it may not emulate the result while the provider is unavailable.

Numeric offsets are bounded direct ordinal selections inside one provider
epoch. Use generated cursor pagination instead when a user journey spans
multiple requests and must retain snapshot identity across concurrent writes;
do not turn successive numeric offsets into a client-side page walk.

Provider rebuilds are currently bounded full-partition rebuilds after an
authoritative frontier change. This is derived work, not request work, and is
limited by the compiler-declared candidate and amplification ceilings. A later
implementation may incrementally maintain the same V4 identity without
changing query semantics.

The immutable generic capability receipt is
`release/evidence/operational-query-capability-v3.json`. It preserves the V1
and V2 receipts and binds the neutral contract, nullable operation plans,
generated-language artifacts, and strict TypeScript helper-reachability gate.
It does not claim any external framework route has passed: that evidence
remains the responsibility of the repository owning the adapter.
