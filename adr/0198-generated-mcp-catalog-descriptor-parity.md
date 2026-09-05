---
adr: 0198
title: Generated MCP Catalog Descriptor Parity
status: proposed
tier: surface
date: 2026-09-05
accepted: null
requires: [ADR-0008, ADR-0020, ADR-0040, ADR-0047, ADR-0064, ADR-0124,
  ADR-0155, ADR-0194]
amends:
  - ADR-0155 generated MCP package artifact only by adding an exact projection of the accepted hosted MCP descriptors
  - ADR-0194 generated application-operation topology only by adding V6 for nonempty command registries while retaining its exact V2 through V5 profile selection
supersedes: []
requirements: [MCP-001, MCP-020, MCP-021, MCP-026, MCP-040, MCP-043,
  MCP-045, DX-044, DX-047, DX-049, VER-001, VER-002, VER-003, VER-004,
  VER-008]
packages: [WP-779]
obligations:
  - id: OBL-0198-1
    package: WP-779
    proof: generated_mcp_command_catalog_v6_composes_authoritative_schemas
    says: A strict V6 successor binds each command ID to its exact V2 name registry entry, compiler input and outcome keys, IDs, bodies and hashes, accepted service envelope, composed output identity, and closed serialized descriptor.
  - id: OBL-0198-2
    package: WP-779
    proof: generated_mcp_catalog_matches_complete_hosted_descriptor_pages
    says: An independently derived fully authorized expected set streams against every bounded full-schema hosted page through the terminal cursor and rejects every descriptor, authority, collision, ordering, cursor, or bound mismatch.
  - id: OBL-0198-3
    package: WP-779
    proof: generated_mcp_catalog_v6_topology_and_lock_migration_are_exact
    says: The exact nonempty-command predicate, predecessor-profile preservation, dependency edge, topology, fixture rotation, V8 lock-identity migration, and old-artifact behavior are frozen and reviewed.
review_triggers:
  - A schema owner, schema key, schema ID/hash/body, command ID/name, descriptor key/optional, annotation, visibility, collision, ordering, page, cursor, or bound would change.
  - A generated catalog, composed schema, artifact hash, application-lock identity, fixture, dependency feature, or lock graph would rotate without exact human review.
  - A duplicate composer, copied envelope source, compiler-to-MCP/service edge, runtime fallback, compatibility alias, or aggregate full-schema inventory would be introduced.
---
# ADR-0198: Generated MCP Catalog Descriptor Parity

## Context

ADR-0008 fixes hosted command descriptors: the compiler owns the command name,
input schema, and outcome union; the service owns the operation envelope; MCP
mechanically composes `outputSchema`. The generated artifact instead presents a
different name and the bare outcome union as `result_schema`. It cannot prove
hosted parity. This corrects only that generated evidence boundary; ADR-0040's
DTO, ADR-0047's root, ADR-0194's query envelope, and runtime behavior stay exact.

## Decision

1. Add `riffdb-generated-application-operations/v6`. First compute the exact
   ADR-0194 V2/V3/V4/V5 profile from pagination, SDK-only, and vector predicates.
   If the post-reimport-exclusion generated command registry is empty, emit that
   predecessor byte-exact; otherwise emit V6 with its identity recorded as
   `predecessor_schema`. V6 is its strict structural successor: it retains
   `application_manifest_hash`, `tools`, `commands`, `reactive_tools`, and every
   applicable `sdk_tools`/`vector_tools` member and order, adding only V6 fields.
2. Each retained command entry adds one closed identity object binding its
   nonzero `CommandId`; exact `McpCommandNameRegistryV2` entry and underscore
   `McpCommandToolNameV2` from ADR-0064; `CommandInput(id)` and
   `CommandOutcomeUnion(id)` keys; exact `/command-input/<id>/v1` and
   `/command-outcome-union/<id>/v1` document IDs, canonical bodies, and hashes;
   service `riffdb.command-operation-envelope/v1` ID/hash; and composed ID/body/
   hash. ADR-0020's lowercase mapping, source restrictions, stable-ID registry
   order, collision rejection, and 128-byte limit remain; dotted V1 is forbidden.
3. Each V6 MCP-visible command/query entry adds a closed `mcp_descriptor` whose
   root keys are exactly `name`, `inputSchema`, `outputSchema`, `annotations`.
   `title`, `description`, `execution`, `icons`, `_meta`, and unknown optionals
   are absent. `annotations` has exactly `readOnlyHint`, `destructiveHint`,
   `idempotentHint`, `openWorldHint`; commands use false/false/true/false and
   queries use true/false/true/false. Legacy title/description/result fields
   remain artifact metadata and never project into a V6 MCP descriptor.
4. Command `inputSchema` is the bound compiler input body. `outputSchema` is the
   one existing MCP algorithm: verify the exact envelope ID/hash/body and its
   sole false `$defs.outcome`, remove only the outcome union's Draft 2020-12
   `$schema`, replace that placeholder, validate bounds/canonical form, and hash.
   Any other edit or identity mismatch rejects; raw outcome is never output.
5. `riffdb-cli` may add one direct default-feature-disabled
   `riffdb-api-mcp` dependency solely for its safe common schema composer and
   descriptor projection. It enables no stdio/HTTP, service, or auth feature.
   `Cargo.toml`/`Cargo.lock` receive exact graph review. Hosted and artifact
   assembly call that one owner; no copied source/composer or compiler/query-
   module-to-MCP/service dependency is permitted.
6. Expected visibility is derived before discovery from the exact deployed
   bundle/query/role artifacts and the accepted fixed operation-permission
   registry—not from returned tools or name patterns. The test role explicitly
   grants every fixed operation and every exact command ID and module-hash/query
   pair, with no wildcard/admin inference. Expected order is visible fixed tools
   in registry order, then the lexicographically named merge of visible commands
   and queries; any fixed/dynamic or dynamic/dynamic name collision rejects.
7. Parity streams each full-schema hosted `tools/list` page against that expected
   iterator, comparing array order and every closed descriptor member without
   retaining an aggregate schema inventory. Each request uses the accepted limit
   500; each response independently satisfies the 4 MiB ledger and may contain
   fewer byte-fitted items. A continued page is nonempty, cursor text is
   canonical/new/bound, and absent `nextCursor` is required exactly after the
   final expected item. Extra/missing/duplicate/reordered/malformed items, cursor
   loops, excess pages/items, schema bounds, and one-byte overflows reject.
8. ADR-0008's compact watcher is separate evidence: it alone retains at most
   1,024 fingerprints and proves limit-500 pages 500/500/24 in at most three
   calls. No compact hash comparison, three-call claim, or 1,024 retention limit
   substitutes for the streaming full-schema parity proof.
9. V6 rotates affected fixtures and MCP artifact hashes, hence enclosing V8
   application-lock identities; lock schema V8 does not advance. Old exact
   locks/artifacts remain readable and make no parity claim. Explicit migration
   previews catalog/artifact/lock hashes and uses existing exact acceptance.
10. Hosted construction, filtering, dispatch, invocation, validation, results,
    notifications, protocol, Protobuf, IR, bundles, plans, durable data, and
    storage are unchanged. Generated descriptors are evidence, never authority.

## Compatibility

V6 is an additive generated-surface identity, with no dual descriptor, raw-union
fallback, negotiation, or V2-V5 reinterpretation. Human review covers every V6
catalog, composed schema, fixture/hash, dependency/lock diff, and migration guide.

## Standing design tests

- **Interface safety:** applications cannot supply schemas, names, metadata,
  envelope bytes, visibility, dispatch, or a fallback descriptor.
- **Scale:** composition is once per bounded command; parity retains one bounded
  full page/current descriptor and bounded cursor identities, never all schemas.

## Checks

- The three obligation proofs plus generated, topology, MCP/driver, handbook,
  dependency, requirement, allowed-path, workspace, clippy, and `ci-all` checks
  freeze V6 structure, exact descriptors, streaming pagination, and migration.
