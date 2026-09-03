---
adr: 0194
title: Generated MCP Named-Query Pagination Envelope
status: accepted
tier: surface
date: 2026-09-03
accepted: "2026-09-03"
requires: [ADR-0008, ADR-0040, ADR-0052, ADR-0055, ADR-0108, ADR-0124,
  ADR-0128, ADR-0154, ADR-0155, ADR-0179, ADR-0193]
amends:
  - ADR-0008 cursor presentation only by distinguishing generated named-query application continuations from MCP discovery and resource cursors
  - ADR-0052 generated named-query MCP presentation only by adding a compiler-owned envelope for paginated successful results
supersedes: []
requirements: [OQ-004, OQ-006, OQ-016, OQ-058, OQ-061]
packages: [WP-701]
obligations:
  - id: OBL-0194-1
    package: WP-701
    proof: generated_paginated_mcp_queries_expose_collision_safe_cursor_envelope
    says: Every generated paginated named-query MCP tool routes its compiler-declared cursor parameter through the protected cursor slot and returns the exact required result/page envelope with a bounded nullable next_cursor, while unpaged tools remain byte-exact.
  - id: OBL-0194-2
    package: WP-701
    proof: generated_mcp_query_cursor_round_trip_preserves_authority_and_redaction
    says: Hosted and stdio MCP resume the same authorized snapshot-bound query from the returned opaque cursor, reject malformed or stale cursors without structured output, and reveal no cursor or physical bytes in diagnostics or telemetry.
  - id: OBL-0194-3
    package: WP-701
    proof: generated_application_operations_v5_topology_and_selection_are_exact
    says: V5 is a strict V4 structural successor, composes pagination with exact SDK-only and vector registries, selects the least-sufficient V2 through V5 identity, freezes every reader/writer window and reviewed rotation, and preserves every unpaged entry byte-exact.
review_triggers:
  - The envelope, field names, cursor text, nullability, schema profile marker, or paginated-tool classification would change.
  - A generated MCP catalog, application lock, or checked fixture would rotate without exact human diff and hash review.
  - A query IR, module, plan, cursor byte, Protobuf, durable, authorization, redaction, tool-name, or unpaged-result identity would change.
  - A second cursor authority, flat business-field injection, client-side page walk, or fallback result shape would be introduced.
---
# ADR-0194: Generated MCP Named-Query Pagination Envelope

## Context

Generated named-query MCP input schemas expose a compiler-declared `Cursor` or
`Cursor?`, commonly `after`, but generated success schemas and both backends
expose only the business result, so MCP cannot resume a page. Cursor properties
also look like ordinary strings rather than identifying the protected slot.

Adding `next_cursor` to the flat business result is unsafe: result-field names
belong to the query, and a legal field may collide. The repair must preserve one
service-owned cursor, exact unpaged output, and every query and wire identity.

## Decision

1. A generated MCP named-query tool is **paginated** exactly when its checked
   query has one compiler-declared `Cursor` or `Cursor?` parameter bound to the
   plan's sole continuation slot. Its generated input schema retains the exact
   source property and adds the root annotation
   `x-riffdb-continuationParameter: "<exact-source-name>"`. The annotation is
   absent from unpaged tools. Generation rejects zero/multiple/mismatched slots;
   adapters validate the annotation and property rather than guessing a name.

   MCP omits either cursor property from root `required` because a first page has
   no token. For `Cursor`, absence means first page, string means continuation,
   and null rejects. For `Cursor?`, absence/null means first page and string means
   continuation. SDK and query input presence rules stay exact.

2. A paginated tool uses the closed successful structured-content shape:

   ```json
   {"result":<business-result>,"page":{"next_cursor":<string-or-null>}}
   ```

   Root properties are exactly `result` then `page`, both required, with
   `additionalProperties: false`. `result` is the unchanged generated business
   result union. `page` is closed, contains only required `next_cursor`, and
   `next_cursor` is either canonical cursor text or JSON null. It is null exactly
   when the service returns no continuation and is never omitted. The result
   schema carries `x-riffdb-resultProfile: "paginated-envelope-v1"`. Text content
   is the bounded compact JSON rendering of that same validated object.
   For business-result compact JSON of `B` bytes, the envelope is exactly `B+59`
   bytes with a 22-character cursor and `B+39` with null; generation checked-adds
   and reserves 59, never the final-page discount. Each complete input/result
   schema is at most `MAX_OPERATION_SCHEMA_SOURCE_BYTES=65,536`. This sum does not
   prove transport fit: MCP emits structured content plus JSON-string-escaped text.
   Before emission, the existing exact ledger checks complete `CallToolResult`
   plus worst-case framing against 4,194,304 bytes; excess refuses without
   truncating, dropping, or walking pages.
3. The cursor string is the existing public application-query presentation:
   exactly 22 ASCII characters matching `^[A-Za-z0-9_-]{22}$`, whose canonical
   unpadded RFC 4648 base64url decoding is exactly the existing 16 opaque cursor
   bytes and whose re-encoding is byte-equal. This narrowly amends ADR-0008;
   MCP discovery/resource cursors remain exactly 32 lowercase hexadecimal
   characters. No cursor payload, binding, lifetime, or byte format changes.
4. After input-schema validation, each backend removes the annotated cursor
   property from ordinary parameters. It applies section 1's exact required or
   optional presence/null rule; a string is strictly decoded and supplied only
   through the API-neutral or public-client cursor slot. The service alone
   issues, binds, expires, supersedes, and authorizes it. A returned cursor is
   copied only into `page.next_cursor` after response identity, authorization,
   and schema checks; no adapter inspects it.
5. Malformed input is the existing bounded invalid-arguments failure. Stale,
   mismatched, expired, denied, or service-invalid continuation uses the existing
   typed public/application error mapping. Errors have no structured content or
   cursor; cursor text, values, physical keys, policy facts, and internal causes
   remain absent from logs, metrics, diagnostics, and evidence.
6. Paginated input/result schema document IDs advance from
   `riffdb.named-query/<module-hash>/<query>/input/v1` and `/result/v1` to the
   corresponding `/input/v2` and `/result/v2`. V5 is a strict structural
   successor of V4: it retains exact V2 root/query/command/reactive fields, V3
   `sdk_tools` when SDK-only secret outputs exist, and V4 `vector_tools` when
   vector inspection exists. Only paginated entries change; unpaged entries are
   byte-exact.
   Selection is exact and least-sufficient: V2 when none of SDK-only, vector, or
   pagination features is present; V3 for SDK-only without vector or pagination;
   V4 whenever vector exists without pagination, retaining any `sdk_tools`; V5
   whenever an MCP-visible query paginates, retaining either/both predecessor
   registries. V2-V5 are readable/writable/current under that policy; no reader
   reinterprets old flat results, and nonpagination catalogs retain V2-V4 bytes.
7. Regenerating a paginated catalog rotates its schema hashes, catalog artifact
   hash, and the enclosing application lock's MCP-artifact hash and lock identity.
   It does not advance the application-lock schema. Old clients may continue
   with an exact old catalog/lock; a client using V5 must rediscover and consume
   the envelope or fail locally. There is no flat compatibility alias, dual
   emission, content negotiation, or cursor-less fallback under V5.
8. Query grammar, IR, module and plan hashes, business result schemas, cursor
   bytes and authority, Protobuf, gRPC semantics, durable formats, tool names,
   permissions, authorization safe points, row policy, order, page/scan/output
   bounds, and unpaged per-tool schemas and success objects are unchanged.
   Secret-output queries remain absent from MCP under ADR-0128.

## Consequences

Paginated MCP callers can resume the same bounded named query without exposing a
physical key. The reserved envelope prevents every business-name collision at
the cost of an explicit generated MCP V5 compatibility boundary.

## Standing design tests

- **Interface safety:** callers submit only the compiler-named cursor property
  and receive one opaque token; they cannot select a plan, snapshot, cursor
  binding, field injection, authorization behavior, or fallback.
- **Scale:** one 16-byte cursor and one fixed two-object envelope are added per
  bounded page. Existing page, schema, result, transport, and diagnostic byte
  ceilings still apply, with no page walk or retained population state.

## Checks

- `generated_paginated_mcp_queries_expose_collision_safe_cursor_envelope` freezes
  V5/V2 schemas, collisions, null/final-page behavior, and unpaged exact bytes.
- `generated_mcp_query_cursor_round_trip_preserves_authority_and_redaction`
  executes two pages through hosted and stdio MCP and covers malformed, stale,
  cross-query, cross-database, authorization, and redaction failures.
- `generated_application_operations_v5_topology_and_selection_are_exact` freezes
  V2-V5 windows, least-sufficient combinations, reviewed rotations, and exact
  preservation of every unpaged entry.
- `scripts/adapter-operational-query-acceptance --all-languages`, MCP conformance,
  `scripts/check-generated`, application-binding, topology, obligation,
  handbook, and allowed-path checks cover the complete generated/public flow.
