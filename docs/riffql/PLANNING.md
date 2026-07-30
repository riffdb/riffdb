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

Declared relationship metadata may justify symbolic navigation only when it
lowers to the target's complete primary-key point read or an already bounded
dependent-key batch. It cannot infer colocation, omit a partition predicate,
introduce a hidden scan, or accept an otherwise unsupported join.

Dependent batches execute as ordered point reads inside the same engine-owned
snapshot as the source scan. They do not become public N+1 requests. Null,
duplicate, noncanonical, over-bound, or missing dependent keys fail closed
before a partial result can be released.

## Whole-query authorization and execution fuel

ADR-0055, WP-280, and WP-285 establish the exact application-query
authorization and work boundary below.

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

The compiler derives each component conservatively:

- index scans charge their complete declared `take` maximum;
- point reads include direct reads, every dependent key, and index-result
  hydration;
- dependent batches charge their complete source-key maximum;
- intermediate rows are summed across all bindings rather than reset per
  entity or step;
- projected values include every declared result copy; and
- result bytes use contract field byte bounds plus deterministic envelope
  reserves.

The complete vector is appended to the canonical access-program bytes before
the `riffdb.query-plan/v1` hash is computed. Changing any component therefore
changes plan identity and invalidates substitution.

The current compatible capability record has one `max_scan_rows` field. It is
the whole-query aggregate allowance for index-scan rows; every scan step is
summed and no step may independently reuse it. Point reads, dependent keys, and
retained intermediate rows have separate conservative ceilings derived as that
scan bound times the fixed maximum query-step count. Projected values add the
existing maximum visible-field multiplier, and encoded bytes retain the fixed
4 MiB service ceiling. The exact plan cost is still authorized once and
consumed as execution fuel, so these derived ceilings do not create ambient or
unmetered read authority.

A `Limit` parameter is charged at its complete 500-row type range. Therefore,
two index scans each controlled by an independent `Limit` parameter require
1,000 aggregate scan rows and fail role derivation. Fixed `take` bounds let a
multi-collection page divide the 500-row aggregate deliberately.

At runtime the executor creates a move-only fuel value from the exact program
cost. It decrements fuel for every access step, backend-reported scanned row,
point read, dependent key, retained intermediate row, projected value, and
encoded result byte. Backend scan reports are reconciled with returned rows.
An impossible, under-reported, over-reported, or exhausted execution returns a
closed failure before a result or cursor is constructed.
