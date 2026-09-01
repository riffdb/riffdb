# ADR-0175: Bounded Partition-Set Operational Queries

- **Status:** Accepted
- **Direction approved:** 2026-09-01
- **Exact text accepted:** 2026-09-01
- **Acceptance reference:** Maintainer approval in the current Codex session
- **Requires:** ADR-0035, ADR-0038, ADR-0051, ADR-0053, ADR-0055,
  ADR-0108, ADR-0111, ADR-0124, ADR-0130, ADR-0134, ADR-0150,
  ADR-0159, ADR-0164, and ADR-0174
- **Amends:** ADR-0051, ADR-0108, ADR-0150, and ADR-0174 only where
  they require every operational query to select exactly one partition
- **Defines or blocks:** WP-739 through WP-743
- **Decision deadline:** Before WP-740 freezes the new language, query IR,
  module, role, generated-schema, plan, or cursor identity

The maintainer accepted the exact boundary summarized in this record on
2026-09-01 and authorized its complete end-to-end implementation. This record
does not permit cross-partition mutation or a general cross-partition join.

## Context

MLflow's `search_runs(experiment_ids=[...])` requires one globally ordered,
cursor-paginated result over runs whose aggregate partition is the experiment
identifier. A client fan-out would create multiple cursor chains, duplicate
sorting outside first-party Rust, and either cross snapshots or retain hidden
adapter state. Moving every run to one global partition would discard intended
write and storage locality.

RiffDB already has the essential lower-level guarantees: memory and redb execute
a complete query inside one engine-owned read snapshot; operational indexes are
partition-prefixed; query plans own total ordering, authorization, cost, result
bounds, and opaque cursors; and ADR-0174 can complete a partition-local
candidate pipeline before ordering. The missing capability is a compiler-sealed
finite set of partition routes over which the same local plan is evaluated and
whose results are merged by the server.

The accepted architecture previously rejected every operational query without
one exact partition equality. That rejection is intentionally amended for this
one shape. Arbitrary cross-partition joins, runtime-selected plans, distributed
transactions, scans that discover partitions, and client-side merge semantics
remain outside the POC.

## Decision

### 1. Add an explicitly bounded set type

RiffQL adds the additive query-only type `Set<T, MAX>`, where `T` is one
existing scalar, enum, or field-referenced scalar type and `MAX` is a canonical
positive integer no greater than 65,535. Existing `Set<T>` source and compiled
artifacts retain their exact meaning and legacy ceiling.

The submitted set is type-checked, encoded, sorted, and deduplicated once before
authorization, plan/cursor parameter hashing, or storage work. Its distinct
cardinality must not exceed `MAX`; request bytes and the existing whole-request
input envelope remain independent limits. The maximum is source-visible,
compiler-owned identity and is emitted into generated schemas and handbook
documentation. A caller cannot increase it.

### 2. Admit one finite partition-set route

A root predicate of the exact form `partition_field in $routes` may establish
the query route only when `$routes` has type
`Set<Entity.partition_field, MAX>`. The compiler proves exact field identity,
type, aggregate partition ownership, non-optional presence, and a positive
bound. No expression, binding result, provider result, discovered value, or
unbounded collection may supply the route.

Every query plan has exactly one route shape: the existing exact scalar route,
or this finite set route. Every binding, ordinary access, relationship mapping,
candidate source, provider participant, hydration, and row-policy dependency
must use the same route shape. Within each selected partition, all existing
same-partition proofs remain unchanged. The feature does not permit an edge
whose source and target belong to different partitions.

An empty submitted partition set is valid and returns the declared empty
result without storage access beyond the common authorization/snapshot
boundary. Duplicate submitted routes are unobservable after canonicalization.

### 3. Execute one replicated local plan at one snapshot

The engine opens one authoritative read snapshot and captures one application
frontier for the complete request. It evaluates the immutable compiler-sealed
partition-local plan for every canonical selected partition inside that same
snapshot. It must not open one public request, authorization decision, or
snapshot per partition.

Ordinary index execution uses one declared index whose leading component is the
partition field and whose remaining physical order produces the complete
declared result order and unique tie-breaker. Each partition contributes one
ordered stream. The executor performs a bounded deterministic k-way merge over
those streams and applies limit plus the one continuation probe only after the
global order is established. Equivalent bounded materialization and sort is
permitted for an ADR-0174 candidate-root plan that must complete before order.

The executor may seek each partition from the prior global order key. It may
not expose per-partition pages, stop a candidate source early, change plan by
partition, or let canonical partition order affect result order. Equal result
keys are impossible because the compiled total order includes the complete
root key, including its partition component.

### 4. Preserve one opaque continuation

One cursor represents the global result. It binds:

- contract, module, plan, role, and selected finite order-family identities;
- the canonical normalized partition-set parameter and every filter parameter;
- the query-specific partition maximum and all work/result ceilings;
- the captured database history and admission-head semantics;
- the last complete global physical order key; and
- a collision-resistant digest of the ordered selected partition/index/provider
  epoch observations required to resume safely.

Continuation revalidates the complete digest in one new snapshot. Any selected
partition change, parameter drift, policy/role drift, retired provider epoch,
history change, or incompatible plan fails closed through existing typed cursor
or freshness behavior. The cursor does not carry one independently mutable
client cursor per partition and does not depend on process-global state.

ADR-0159 continues to exclude only a submitted page cardinality from invariant
parameter identity. Changing the partition set is never page-cardinality drift.

### 5. Bound every multiplied dimension independently

The compiler and runtime independently cap and charge:

- declared and submitted distinct partition count and encoded route bytes;
- per-partition and whole-request index/provider candidates and inspected rows;
- partition probes, seeks, continuation checks, and epoch observations;
- candidate keys, hydration, relationship, row-policy, and provider work;
- merge heap entries, materialized rows, sort keys, retained bytes, and total
  comparisons;
- result rows, encoded response bytes, cursor bytes, and total request work.

The structural partition-set maximum is 65,535, but no query receives that
budget implicitly. Its declared `MAX`, selected access/provider bounds, role
authority, request bytes, global scan/work ceiling, and 4 MiB result ceiling
must all pass. Multiplication uses checked arithmetic. Crossing any bound
refuses the whole operation without rows, cursor, per-partition counts, or
partial success.

### 6. Replicate only partition-local candidate and provider work

ADR-0174 candidate algebra and provider participation may execute once per
selected partition under the common admission head and snapshot when every
source and relationship remains local to that partition. The compiler seals
one identical source/set/hydration/order program and global multiplied bounds;
the caller cannot choose different predicates, providers, or source structure
per partition.

Each provider participant supplies an observation compatible with the common
admission head for its partition. The ordered complete participant-set digest
is bound into plan/cursor execution. Missing, stale, incompatible, or excessive
state fails the whole request. This extension is intended to support later
MLflow run attribute, tag, metric, and parameter filters without moving those
semantics into the adapter.

### 7. Authorize the complete operation before observation

The shared policy layer authorizes the compiler-derived union of entities,
fields, indexes, providers, maximum partitions, maximum rows, and total work as
one operation. A role grant must cover the complete declared maximum and every
possible compiled member. Current capability and row/field policy are checked
before release.

Unauthorized partitions or rows cannot influence returned membership, order,
cursor, overflow class, provider statistic, timing label, or public diagnostic.
Where a role's partition scope cannot cover the complete submitted route set,
the request is denied as a whole; it is not silently narrowed. Ordinary row
policy still applies independently to candidate rows before observable order.

### 8. Use additive identities and retain old bytes

`Set<T, MAX>` and a finite partition-set route select least-sufficient additive
RiffQL, checked query IR, executable plan, query-module, role-authority,
application-lock, generated-schema, explain, and cursor identities. Existing
single-partition and legacy `Set<T>` sources keep their previous writers and
byte-exact semantics. Old readers reject the successors rather than
reinterpret them.

No entity key, index key, command IR, mutation, commit record, event,
provenance, storage format, or public transport envelope changes. Provider
checkpoint identities change only if a provider explicitly adopts the new
participant-set plan identity; authoritative state remains unchanged.

## Options Considered

1. **Adapter fan-out and merge:** Rejected because it moves total ordering and
   cursor semantics outside the database and cannot preserve one snapshot.
2. **One global Run partition:** Rejected because it discards aggregate
   locality and turns a query limitation into a write-scaling workaround.
3. **General cross-partition joins or SQL:** Rejected. The accepted shape is
   replication of one sealed local plan over explicit routes, not a join
   optimizer or partition discovery language.
4. **One cursor per partition exposed to the adapter:** Rejected because it
   exposes physical execution state, multiplies public state, and permits
   inconsistent or incomplete merge behavior.
5. **A fixed small partition cap:** Rejected. Query-specific explicit maxima,
   bytes, work, and role authority are the safe controls; the structural
   ceiling must not recreate the earlier 499-row product limitation.
6. **Reuse unbounded `Set<T>` invisibly:** Rejected because the application
   source and generated schema must expose the exact maximum being authorized.

## Consequences

- MLflow Run search can retain experiment-local aggregate partitions while
  returning one exact globally ordered page.
- The compiler, query IR/module, executor, storage adapters, policy, service,
  cursor, generators, topology, and handbook gain an additive route shape.
- A request may open many partition-local index ranges inside one storage read
  transaction. The work remains finite and query-declared, but applications
  must choose honest maxima and suitable covering indexes.
- Candidate/provider searches can later reuse the same route replication;
  general cross-partition aggregation and relationships remain unavailable.

## Compatibility

The change is source-additive and uses least-sufficient successor identities.
Existing queries, modules, locks, role hashes, generated inputs, plan hashes,
cursors, and storage remain exact. No migration is required for data. A query
adopting `Set<T, MAX>` or partition-set routing must be recompiled, redeployed,
rebound, and regenerated as one exact application identity.

## Security

Partition values are ordinary submitted business values and remain redacted
from public errors, logs, metrics, service audit targets, and MCP diagnostics.
The compiler owns route shape, fields, indexes, plan, order, maxima, providers,
and fallback. Authorization is whole-request and fail-closed. Bounds are checked
before allocation and again at execution; no overflow returns a partial page.
Canonical set and epoch digests are domain-separated first-party hashes and are
integrity bindings, not authentication or encryption.

## Standing Design Tests

- **Interface safety:** Applications may submit only typed route values, a
  bounded limit, ordinary typed filters, a closed order selector when declared,
  and an opaque cursor. They cannot discover partitions, submit a plan or join,
  select per-partition behavior, weaken snapshot/policy checks, or request
  partial results. Every maximum is compiler-owned and generated.
- **Scale:** V1 may inspect up to 65,535 explicit partitions in one single-node
  snapshot. That is a deliberate reversible POC implementation ceiling, not an
  assumption that all authoritative rows are co-located or fit in memory. The
  semantic interface is compatible with future partition-local parallel scans
  or spillable merge, but this ADR adds no distributed snapshot protocol.

## Testing

- Parser/formatter and source-spanned diagnostics for zero, excessive,
  optional, type-mismatched, non-partition, discovered, and multiple routes.
- Golden old/new RiffQL, IR, module, plan, role, generated-schema, topology, and
  cursor identities proving least-sufficient writers.
- Independent global-order oracle over empty, one, duplicate, maximum, and
  over-maximum partition sets in both directions with page-size changes.
- Memory/redb parity for common-snapshot execution, epoch-digest drift,
  cancellation, bounds, policy denial, candidate/provider completion, and no
  partial result.
- Concurrent-write tests proving continuation either resumes the exact bound
  observations or fails typed; it never crosses silently to a mixed snapshot.
- Rust, Go, TypeScript, Python, gRPC, MCP, CLI, local/remote, generated binding,
  handbook, downstream-adapter, and real MLflow multi-experiment loopback.

## Requirements and Work Packages

- **Requirements:** `RQL-007`, `QRY-010`, and `OQ-101` through `OQ-112`
- **Defines or blocks:** WP-739 through WP-743
- **Final evidence:** WP-743

## Decision Deadline

This exact text was accepted before implementation. Any expansion to discovered
partitions, cross-partition relationships, mutation, caller-authored plans,
partial results, a larger structural ceiling, or a distributed snapshot
protocol requires renewed human review.
