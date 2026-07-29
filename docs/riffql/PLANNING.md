# RiffQL v1 planning

`riffdb-query-compiler` compiles one parsed query against one exact immutable
contract catalog. Successful compilation produces a `QueryAccessProgramV1`;
execution does not receive source text or perform access-path discovery.

The v1 planner accepts only:

- one exact partition parameter shared by every binding;
- complete primary-key point reads for `one` and `maybe`;
- bounded `many` reads whose equality/membership prefix and complete ordering
  match one declared index;
- a bounded dependent primary-key batch where one earlier `many` field supplies
  exactly one complete-key component through `in`, the target bound does not
  exceed the source bound, and a missing-target outcome is declared;
- one wholly forward or wholly reverse traversal direction;
- explicit row bounds no greater than the 500-row service ceiling.

The program contains ordered accesses, dependency edges, cardinality and row
bounds, and the complete entity/field/index authorization requirement. The
application supplies names; stable numeric IDs remain compiler-internal.
Authorization is fail-closed before execution and never silently removes a
selected field.

Canonical program bytes are hashed in the accepted
`riffdb.query-plan/v1` domain. Explain output is a bounded, deterministic,
name-only view of the same program. An absent compatible index produces
`RDB-QP003` with a source span and a suggested symbolic index declaration.

RiffQL v1 does not perform an unbounded fallback scan, client-side sort,
cross-partition join, or optimizer-dependent plan choice.

Dependent batches execute as ordered point reads inside the same engine-owned
snapshot as the source scan. They do not become public N+1 requests. Null,
duplicate, noncanonical, over-bound, or missing dependent keys fail closed
before a partial result can be released.

## Accepted authorization and fuel target

ADR-0055 and WP-280 establish the exact application-query authorization
boundary below. Whole-query cost and execution fuel remain the WP-285 portion
of the accepted target until that work package is complete.

The service resolves the entire query before data access and presents policy
with one application-query request. For a named query this includes the exact
contract lineage, immutable module hash, query name, plan hash, partition route,
complete entity/field/index/output requirements, principal and capability
revision, and one whole-request cost vector. Ad-hoc execution uses a distinct
permission and classification.

An allow decision produces a process-local, non-cloneable and nonserializable
proof bound to that exact request and current capability revision. The service
consumes the proof at the executor boundary with the matching plan and
parameters, then reauthorizes before releasing output. It cannot be converted
into, or reused to authorize, public `GetEntity`/`ScanIndex` requests.

The plan-hashed cost vector includes at least step count, scanned rows, point
reads, dependent keys, intermediate rows, projected values, and encoded result
bytes. Policy compares the complete vector once. Execution consumes matching
fuel and checks backend-reported work; exhaustion returns no partial result or
cursor. Per-step bounds remain defense in depth, not the aggregate authority
model.
