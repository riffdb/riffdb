# RiffQL v1 planning

`riffdb-query-compiler` compiles one parsed query against one exact immutable
contract catalog. Successful compilation produces a `QueryAccessProgramV1`;
execution does not receive source text or perform access-path discovery.

The v1 planner accepts only:

- one exact partition parameter shared by every binding;
- complete primary-key point reads for `one` and `maybe`;
- bounded `many` reads whose equality/membership prefix and complete ordering
  match one declared index;
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
