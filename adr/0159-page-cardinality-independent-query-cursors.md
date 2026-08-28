# ADR-0159: Page-Cardinality-Independent Query Cursors and External Page Coalescing

- **Status:** Accepted
- **Direction approved:** 2026-08-27
- **Exact text accepted:** Yes, 2026-08-27
- **Accepted:** 2026-08-27
- **Acceptance reference:** Maintainer approval in the current Codex session of
  the complete two-part decision immediately preceding implementation
- **Decision deadline:** Before WP-708 changes named-query cursor identity or
  WP-709 permits an external adapter to assemble one upstream page from more
  than one bounded RiffDB page
- **Requires:** ADR-0027, ADR-0051, ADR-0052, ADR-0055, ADR-0108, ADR-0112,
  ADR-0124, ADR-0130, ADR-0134, ADR-0150, and ADR-0158
- **Amends if accepted:** ADR-0158's normalized-parameter cursor binding and
  no-page-walking rule, only as stated below
- **Defines or blocks:** WP-708 and WP-709

This record is authoritative for WP-708 and WP-709.

## Context

ADR-0158 correctly added a compiler-declared maximum for one RiffDB query
invocation. It reduced unnecessary driver crossings while retaining the global
499-row ceiling and honest row/byte/cost accounting. Real external storage
conformance then proved two distinct interoperability facts.

First, page cardinality is not part of a keyset continuation position. One
authoritative storage interface reads a one-row page, then resumes the returned
continuation while requesting 100 rows. RiffDB's query cursor currently binds
the complete normalized parameter record, including the effective `Limit` or
`Limit<MAX>` value, so the valid continuation is rejected even though the
operation, predicates, order, plan, authority, snapshot, provider epoch, and
last physical key are unchanged.

Second, that storage interface may request pages larger than one safe RiffDB
response. Its conformance suite uses requests in the millions to mean "return
all remaining rows." When more results exist, a conforming implementation must
return the exact requested page size with a continuation, rather than silently
cap the page at its internal database limit. Raising RiffDB's public row or byte
ceiling would weaken its safety boundary and still would not honestly admit the
largest external request.

The external adapter can preserve both contracts by following one unchanged
RiffDB cursor chain and appending every returned row until it fills the finite
upstream request or reaches true exhaustion. Earlier requirements forbade that
because cursor walking must never implement filtering, counting, sorting,
offset, overfetch-and-discard, or an unbounded public query. The demonstrated
case is narrower: it changes only transport/page granularity while preserving
the exact ordered result stream.

## Decision

### 1. Separate continuation identity from invocation cardinality

For an ordinary ordered RiffQL binding using `take $limit after $cursor`, the
effective runtime value of `$limit` controls only the maximum rows returned by
that invocation. Its value is excluded from the cursor's invariant-parameter
hash. The immutable plan continues to bind the parameter name, type, default,
declared `MAX`, static cost, result-byte ceiling, order, predicates, provider,
and every other query property.

The exclusion is compiler-derived and closed. A parameter is eligible only
when every use of that parameter is as the `take` cardinality of a cursor-paged
ordered binding. Literal limits remain in the plan. A parameter used by a
nearest/vector K, an unpaged binding, or any other semantic bound remains in
the cursor identity. Callers cannot mark a parameter invariant or select hash
fields.

All other normalized parameter values and presence choices remain bound.
Changing a filter, partition, set member, range endpoint, offset, vector,
predicate-family member, or other input still rejects the cursor.

### 2. Use a cursor-specific successor hash domain

The general `QueryParameterHash` used by live queries, protected consumers,
causation, and other identities is unchanged. Named query cursors derive one
cursor-specific invariant-parameter hash under a new canonical domain tag and
the filtered canonical parameter count. Parameter names and values retain the
existing ordered length-delimited encoding.

The query cursor registry is bounded and process-local. Public cursor tokens
contain no parameter bytes or page size and do not survive registry restart.
Therefore this successor changes no durable format, public token bytes,
Protobuf field, query IR, query module, plan hash, application lock, driver
frame, or storage key. Cursors minted by a predecessor process remain invalid
after restart as they already do; no old durable decoder is reinterpreted.

### 3. Apply the submitted size to the resumed invocation

After the invariant cursor resolves, the service validates the new effective
runtime page value through the unchanged `Limit` or `Limit<MAX>` domain before
provider/storage work. The resumed invocation uses that value directly from
the cursor's exact continuation position in the same snapshot and provider
epoch.

Changing only page cardinality must preserve exact forward and reverse order,
with no duplicate, omission, restart, skipped row, stale read, epoch change, or
additional authorization authority. A continuation remains invalid when its
operation, module, plan, declared maximum, non-cardinality parameters,
principal, capability/policy revision, database history, snapshot, order,
physical profile, or provider epoch differs.

### 4. Permit one narrow external page-coalescing adapter

An owning external adapter MAY assemble one authoritative upstream page from
multiple bounded RiffDB named-query invocations only when all of the following
hold:

- the upstream interface requires a finite requested page larger than the
  compiled RiffDB query maximum or otherwise requires exact full pages;
- every invocation uses the same generated named operation, non-cardinality
  parameters, principal, authority, order, and returned cursor chain;
- each invocation requests `min(remaining, compiled MAX)` rows;
- the adapter appends every returned row exactly once and in returned order;
- it performs no filtering, sorting, counting, deduplication, discarding,
  offset simulation, page restart, query construction, or storage access;
- it stops only after filling the finite upstream request or receiving true
  RiffDB exhaustion;
- cancellation, invalid/stale cursor, authorization change, provider failure,
  and every other RiffDB error terminate the operation without releasing a
  partial success; and
- when the upstream page fills, its continuation is the final RiffDB cursor
  unchanged; exhaustion returns no continuation.

This is an external compatibility translation, not a RiffDB query capability.
The adapter owns the upstream request/response allocation and checked integer
arithmetic. RiffDB retains its per-invocation row, byte, fuel, policy, scan,
provider, frame, and response ceilings. No adapter, framework schema, route,
or special branch enters the RiffDB repository.

### 5. Keep larger RiffDB pages and streaming deferred

RiffDB does not raise the 499-row ceiling, accept a caller-selected unbounded
page, materialize millions of rows in one response, or hide a server-side page
walk behind an ordinary generated query. A future chunked/streaming operation
may reduce external crossings only after a separate real-consumer ADR defines
flow control, cancellation, snapshot retention, authorization rechecks, byte
budgets, partial-failure semantics, and driver/protocol compatibility.

## Options Considered

1. **Bind the original submitted limit forever.** Rejected because page size
   does not identify a keyset position and the real interface requires it to
   change.
2. **Raise RiffDB's page ceiling to the external maximum.** Rejected because it
   weakens bounded public responses, violates existing byte budgets, and still
   cannot honestly cover every upstream integer.
3. **Return the internal cap with a continuation.** Rejected because the
   authoritative upstream contract requires a full requested page whenever a
   continuation is returned.
4. **Generate operations for every external page size.** Rejected because the
   external domain is enormous and operation families remain compiler-finite.
5. **Permit general client page walking.** Rejected. Only exact ordered append
   under the closed external translation above is admitted.
6. **Build a speculative streaming protocol now.** Rejected until a real
   production workload, rather than a conformance boundary alone, establishes
   its complete semantics.

## Consequences

- A cursor-paged named query may resume with a different valid runtime page
  size while retaining every semantic, authority, snapshot, and epoch binding.
- Shared query parameter identities outside cursor lookup remain byte-exact.
- External adapters can meet exact larger-page contracts without weakening a
  RiffDB invocation or embedding framework behavior in RiffDB.
- One very large external request may require many bounded invocations. That
  cost is explicit and remains a candidate for a later streaming decision, not
  grounds for an unsafe hidden fast path.
- Existing prohibitions on client filtering, sorting, counting, offset walks,
  discard, restart, and arbitrary query construction remain normative.

## Compatibility

This decision changes only process-local named-query cursor lookup semantics
and external acceptance policy. It adds a cursor-specific hash domain but no
durable or wire-format version. Existing query sources, IR, modules, plans,
locks, generated clients, public cursor tokens, and storage formats remain
byte-exact. Restart already invalidates the bounded cursor registry, so no
predecessor token is silently reinterpreted.

## Security

Removing the runtime page value from cursor identity grants no additional row,
field, provider, predicate, order, snapshot, or cost authority. The plan and
declared maximum remain bound, the new value is revalidated before data work,
and fresh authentication/authorization plus current capability and row-policy
checks still run on every invocation. Cursor errors and coalescing failures
must reveal no partial protected result or hidden population fact.

The external adapter may retain more rows because its authoritative API asks
for them; that allocation and its finite integer bound belong to the adapter.
RiffDB never admits more than one compiled page per invocation.

## Standing Design Tests

- **Interface safety:** Callers can vary only a value already admitted by the
  immutable `Limit` domain. They cannot select excluded hash fields, alter the
  declared maximum, or weaken plan, parameter, authority, snapshot, order, or
  epoch binding. External coalescing is exact append-only translation in the
  owning repository, never a RiffDB escape hatch.
- **Scale:** Cursor hashing is paid once per request over the already bounded
  parameter record. Each RiffDB invocation remains bounded by the compiled
  maximum and existing ceilings. External invocation count is finite and
  explicit; no per-row proof, hidden server materialization, or population scan
  is added.

## Testing

- Hash-unit tests prove only eligible cursor-page cardinality values are
  excluded; other values, presence, unpaged limits, and nearest K remain bound.
- Service tests page `1 -> 100`, `100 -> 1`, and varying final remainders in
  forward and reverse order without duplicate, omission, or restart.
- Negative tests vary predicates, partition, order/plan, declared maximum,
  capability revision, snapshot/epoch, and over-maximum values.
- Memory and redb use the same continuation contract; generated Rust, Go,
  TypeScript, Python, CLI, MCP, local-driver, and remote-gRPC paths retain one
  opaque cursor and the newly submitted page size.
- A framework-neutral external receipt proves exact append-only coalescing,
  cancellation/error atomicity, exhaustion, final-token identity, and absence
  of RiffDB framework branches. The real adapter repository owns its upstream
  conformance evidence.
