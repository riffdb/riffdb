# ADR-0051: RiffQL Bounded Symbolic Query Language

- **Status:** Accepted
- **Direction approved:** 2026-07-28
- **Exact text accepted:** 2026-07-28; plan-identity amendment 2026-07-28
- **Acceptance reference:** Maintainer authorization in the current Codex
  session to make the implementation decisions required through WP-270
- **Requires:** ADR-0002, ADR-0007, ADR-0011, ADR-0013, ADR-0016,
  ADR-0027, ADR-0035, ADR-0036, and ADR-0038
- **Amends:** SPEC Sections 1.1, 2.4, 4.1, 7.3, 7.4, 7.7,
  9, 12, 13, 19, 20.7, and 21
- **Implementation boundary:** Before WP-210 freezes RiffQL grammar or WP-230
  publishes query-plan IR

The maintainer accepted this exact RiffQL-first application boundary on
2026-07-28 and authorized the implementation decisions required through
WP-270.

## Context

The POC intentionally exposed primary-key entity lookup and exact-prefix index
scan as public kernel operations. The TicketDesk application baseline proved
that those operations are too physical for normal application and agent work:
callers resolve compiler IDs, construct keys and masks, decode index-entry
bytes, perform one scan plus N point reads, and assemble page-shaped results.

ADR-0002 explicitly deferred named contract queries and a `QueryPlan` decision.
The next phase needs a symbolic language without adding SQL, natural-language
execution, arbitrary joins, storage callbacks, or a second authorization path.

## Decision

### Language boundary

RiffQL is a versioned, formal, read-only language. It has no mutation,
host-language callback, network or filesystem access, clock, randomness,
unbounded loop, recursion, user-defined function, SQL escape, or natural-
language interpretation. Agents may translate natural language into RiffQL, but
RiffDB parses and executes only the formal text.

Compiled contract commands remain the only application mutation surface. A CLI
or test-harness spelling such as `run CreateTicket { ... }` is presentation
syntax that lowers to the existing API-neutral command operation. It is not a
RiffQL AST node and cannot enter a read-only query RPC.

RiffQL source has two forms:

1. an ad-hoc query compiled against one selected active or exact contract; and
2. a named query declaration compiled into ADR-0052's immutable query module.

Both forms use the same parser, resolver, type checker, planner, authorization
analysis, canonical IR, executor, value algebra, and diagnostics.

### Canonical query model

One query declares typed parameters, ordered `one`, `maybe`, or `many`
bindings, and one returned record or declared result union.

- `one` requires exactly one row and names an explicit declared absence result.
  A second row is an unexpected-cardinality failure.
- `maybe` permits zero or one row. A second row is an
  unexpected-cardinality failure.
- `many` requires an explicit positive per-parent `take` bound and participates
  in a separately checked whole-query row and fan-out bound.

Parameters may use a scalar contract type, an enum, a field-referenced type, an
optional type, or a bounded query-only set of one scalar/enum type. Query
parameters and results reuse the canonical RiffDB value algebra. Set submission
is canonicalized by typed value order and rejects duplicates before plan- or
cursor-hash construction.

The first executable grammar supports:

- equality and inequality predicates over like typed values;
- one index-compatible range predicate;
- bounded `in` over a parameter set, lowered to a bounded ordered union;
- Boolean conjunction and disjunction only when every branch has a bounded
  access proof;
- primary-key lookup;
- declared-index prefix/range scan;
- same-partition equijoin through a complete primary key or declared index;
- nested result records and bounded lists; and
- ordering that matches the unused suffix of one selected index either wholly
  forward or wholly reverse, with a stable key tiebreaker.

It does not support a cost-based optimizer, unrestricted full scan, Cartesian
product, arbitrary hash/merge join, group-by, distinct, window, approximation,
general aggregation, mixed authoritative/projection snapshot, or implicit
client-side follow-up read.

### Locality and bounded planning

Every authoritative query must prove one partition route from parameters or an
earlier binding. Every joined binding must prove equality with that same
partition identity under the selected contract. Single-node deployment does not
relax this rule.

Planning is deterministic and rule based. For identical canonical source,
parameters' types, contract bundle, compiler version, and language version, the
compiler produces byte-identical canonical query IR, result schema, required-
access set, explain plan, and plan hash.

The compiler calculates and enforces bounds for source bytes, AST/IR nodes,
parameters, set expansion, bindings, recursion, point reads, index ranges,
physically scanned rows, returned rows, fan-out, intermediate bytes, result
bytes, diagnostic count, and execution deadline. A residual predicate is legal
only after a statically bounded indexed access. No runtime budget turns an
otherwise unbounded plan into an accepted query.

When no legal access path exists, compilation fails with a source-spanned
diagnostic that names the relevant contract symbols and renders the minimum
index shape that would make the query legal. It never creates or changes an
index automatically.

### Authorization analysis

The compiler derives a canonical sorted conjunction of every entity, index,
partition, field, and row-limit requirement in the complete query. The shared
policy layer authorizes that conjunction as one application operation before
execution and checks current policy again before release.

Selection is all or nothing. A query that requests an invisible field is
rejected; a successful record never silently omits it. Authorization diagnostics
may name safe contract/query symbols, the selected principal identity, and the
denied relationship path, but never capability tokens, submitted business
values, hidden field values, storage keys, or internal sources.

### Diagnostics

Every syntax, resolution, type, locality, boundedness, planning, authorization,
and execution failure has a stable code, stage, primary source span, optional
related span, bounded symbol path, safe summary, and bounded suggested fixes.
Ad-hoc query text and parameter values are redacted before logs, metrics,
service audit, MCP text, or generic public errors.

## Options Considered

1. **Generated SDK first:** rejected as the primary interface because it does
   not solve agent discovery or iterative symbolic querying.
2. **Natural-language database execution:** rejected because it is not
   deterministic, reviewable, or replayable.
3. **General SQL/relational optimizer:** rejected because it violates the
   bounded operational-query scope and creates an unowned semantic subsystem.
4. **Point and batch lookup only:** rejected because it cannot express the
   accepted TicketDesk list and detail acceptance corpus.

## Consequences

- Applications and agents use contract names and never stable numeric IDs,
  encoded keys, field masks, or scan-plus-N composition.
- The compiler and policy layer gain a conjunction-shaped read requirement.
- Queries that a general relational database could run may be deliberately
  rejected when locality, access path, or work cannot be proven.
- Covering index payloads and richer analytical operators remain deferred.

## Compatibility

RiffQL introduces a new language and IR version. It does not change contract
grammar v1, command IR, command plan hashes, durable command records, entity
keys, index keys, existing cursors, or existing `riffdb.v1` request bytes.
Query-language or query-IR compatibility is governed separately from contract
compatibility.

## Security

Parsing, compilation, diagnostics, explain, and execution are bounded.
Authorization is derived by the compiler but decided only by the shared policy
layer. MCP, CLI, gRPC, SDK, and in-process tests consume the same application
operation and cannot submit a caller-authored access mask or executable plan.

## Testing

- Parser corpus, formatter idempotence, fuzzing, and source-span snapshots.
- Type/locality/boundedness semantic assertions for every diagnostic.
- Golden canonical IR, result schema, access set, explain, and plan hashes.
- Property tests over planner determinism and bounded set/range expansion.
- Authorization tests for every binding and selected field.
- TicketDesk queries that compile without numeric IDs or encoded keys.

## Requirements and Work Packages

- **Requirements:** New post-POC RiffQL requirements assigned in WP-205
- **Defines or blocks:** WP-210, WP-220, WP-230, WP-240, WP-250, WP-260,
  and WP-270
- **Final evidence:** WP-280

## Decision Deadline

Exact acceptance is required before RiffQL parser, public query IR, planner,
authorization conjunction, or executable query behavior is implemented.

## 2026-07-28 query-plan identity amendment

The accepted plan hash is the typed `QueryPlanHash` computed with ADR-0011's
unchanged SHA-256 frame under the immutable unkeyed domain
`riffdb.query-plan/v1`. Its payload is the complete canonical
`QueryAccessProgramV1` bytes, including query IR version, exact contract
lineage/version/bundle hash, declared query name when present, partition
parameter, ordered access steps and bounds, and the canonical authorization
conjunction. Source spans, diagnostics, explain text, numeric display
formatting, and submitted parameter values are absent.

Adding this previously unassigned domain is additive. It does not reuse or
change command `PlanHash`, projection `ProjectionPlanHash`, schema hashes, or
any durable/public v1 field. The central domain uniqueness and golden-vector
tests cover the new label before WP-230 publishes query-plan IR.
