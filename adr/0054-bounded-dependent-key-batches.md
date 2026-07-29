# ADR-0054: Bounded Dependent Key Batches

- **Status:** Accepted
- **Direction approved:** 2026-07-29
- **Exact text accepted:** 2026-07-29
- **Acceptance reference:** Maintainer request in the current Codex session to
  implement WP-275 and close the TicketDesk POC gaps
- **Requires:** ADR-0013, ADR-0016, ADR-0027, ADR-0035, ADR-0038, ADR-0051,
  ADR-0052, and ADR-0053
- **Amends:** SPEC Sections 20.7 and 24.3
- **Implementation boundary:** Before WP-275 extends canonical query IR or
  claims complete TicketDesk detail-page parity

## Context

TicketDesk's detail page has a bounded junction traversal:

```text
Ticket -> many TicketLabel -> many Label
```

The accepted RiffQL model permits bounded same-partition equijoins through a
complete key, and the accepted composite executor permits dependent point
targets derived from earlier observations. The first implementation nevertheless
lowered every binding-field predicate as one scalar from the first prior row.
It can express singular dependencies and one indexed collection, but cannot
express the bounded set of complete `Label` keys produced by `TicketLabel`.

Adding arbitrary joins, Cartesian products, unbounded nested collections, or a
cost-based SQL planner would exceed the accepted operational-query boundary.
Returning labels through a second public query would also preserve the
multi-request page composition that RiffQL is intended to remove.

## Decision

### Language semantics

The existing `in` operator may consume `many_binding.field` only when all of the
following are statically proven:

1. the source binding is an earlier `many` binding with an explicit positive
   bound;
2. the consuming binding is `many`, has an explicit positive bound no greater
   than the source bound, and has no cursor;
3. the collection-valued term supplies exactly one component of the target's
   complete primary key, while equality predicates supply every remaining key
   component;
4. the target partition component is the query's exact partition parameter;
5. source and target field types are equal after the existing optional-field
   rule, and the source dependency field is compiler-required;
6. the target order is the complete primary-key order after equality-bound
   components and matches the source's canonical order; and
7. the whole query remains within the existing row, point-read, intermediate,
   byte, and deadline ceilings.

This construct is a bounded dependent key batch, not a general join. A
collection binding cannot be consumed by `==`, comparison, range, or any other
scalar operator. A singular binding cannot be consumed by collection `in`.
There is no collection projection expression, correlated subquery, nested
per-row `many`, Cartesian product, non-key lookup, aggregation, or implicit
index creation.

The existing optional `else Outcome` clause on the consuming `many` binding is
required. An empty source collection yields an empty target collection. If a
non-null source key has no target row, execution returns the declared outcome
with no partial result. Null, duplicate, noncanonical, or over-bound source keys
fail closed as an execution integrity error. This makes broken junction
references visible and preserves the all-or-nothing result shape.

### Canonical IR and execution

Query IR v1 gains additive closed tags for:

- a collection binding-field predicate value; and
- a dependent primary-key batch access.

Those tags enter the existing canonical program bytes and plan hash. No
previously valid source changes meaning or bytes: scalar binding fields retain
their tag and point/index access retains its tags. Query modules containing the
new access require a runtime that supports the additive tags; exact module and
plan identities prevent substitution.

The query execution port gains one closed batch-point operation. Its input is
the compiler-produced target step plus an ordered bounded vector of complete
bound key predicates. Its output preserves input position with one optional
owned row per key. Memory and redb execute the complete batch while their one
existing authoritative read view remains open. No transaction, iterator,
callback, policy decision, or storage key escapes.

The executor validates source cardinality, canonical uniqueness, target
identity, returned order, predicates, target completeness, per-step and
whole-query bounds, and the declared missing-target outcome before releasing a
result. Authorization is still derived from every source dependency field and
every target field before execution.

## Options Considered

1. **A second named query for labels:** rejected because page composition would
   again require multiple public requests and snapshots.
2. **Denormalize `label_name` onto `TicketLabel`:** rejected as the general
   platform answer because it duplicates mutable domain state solely around a
   missing read operator.
3. **General SQL or arbitrary joins:** rejected by the POC architecture and
   unnecessary for the bounded operational relationship.
4. **Silently omit missing labels:** rejected because it hides referential
   corruption and changes a requested typed result without a declared branch.

## Consequences

- TicketDesk can load labels in its existing one named detail query and one
  snapshot.
- The compiler has an explicit cardinality distinction between scalar and
  collection dependencies.
- The executor and both storage adapters gain a bounded batch-point method.
- More general many-to-many relationships remain rejected unless they reduce
  to this complete-key, one-collection-component shape.

## Compatibility

There is no public Protobuf, durable storage, entity/index key, contract IR,
command IR, cursor, or kernel gRPC change. New canonical query-program tags
change plan and module identities only for queries that use the new construct.
Existing query-program bytes and identities remain unchanged.

## Security

The source and target access sets are compiler-derived and authorized before
execution. Expansion is bounded by both source and target `take` clauses.
Diagnostics name only safe contract/query symbols and source spans. Missing or
malformed dependent targets produce no partial data.

## Testing

- Parser/formatter stability for `many ... in binding.field ... else`.
- Source-spanned compiler diagnostics for scalar/collection misuse, non-key
  fan-out, nonlocal access, incompatible order, missing outcome, and bounds.
- Canonical-program compatibility tests proving old bytes are unchanged and
  new tags affect plan identity.
- Executor tests for empty, ordered, missing, duplicate, null, over-bound, and
  backend-error batches.
- Memory/redb one-snapshot parity and authorization coverage.
- Full TicketDesk detail result with project, organization, assignee, comments,
  and labels through one named query request.

## Requirements and Work Packages

- **Requirements:** `RQL-006`, `QRY-005`, and `DX-006`
- **Implements:** WP-275
- **Final evidence:** WP-275 TicketDesk acceptance and WP-280 independent-agent
  rerun

