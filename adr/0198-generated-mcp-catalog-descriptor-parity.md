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
    says: A strict V6 successor emits the exact closed mcp_identity layout and binds its nonzero command ID, V2 name, compiler keys and exact generated-schema IDs/bodies/lowercase hashes, accepted envelope, exact composed ID/body/hash, and closed descriptor; outcome $defs rejects.
  - id: OBL-0198-2
    package: WP-779
    proof: generated_mcp_catalog_matches_complete_hosted_descriptor_pages
    says: A role artifact selects candidates but grants nothing; a separate closed Global/All/no-approval test capability with exact dynamic permissions and every fixed-kind witness drives policy visibility, and registry-sourced expected descriptors stream equal through the terminal cursor with collision/order/bound rejection.
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
2. Each retained command entry adds only `mcp_identity`, a closed object whose
   six required members in canonical order are `command_id`, `composed_output`, `input_schema`,
   `operation_envelope`, `outcome_schema`, `tool_name`. `command_id` is a JSON
   integer in 1..=4294967295; `tool_name` is the exact underscore
   `McpCommandToolNameV2`. Each schema child is closed: compiler children order
   `command_id`,`key`,`schema_hash`,`schema_id`, while the other children order
   `schema_hash`,`schema_id`; all listed child members are required and no other
   member is allowed. Child IDs are respectively
   `riffdb.generated-schema/command-input/{id}/v1`,
   `riffdb.generated-schema/command-outcome-union/{id}/v1`,
   `riffdb.command-operation-envelope/v1`, and
   `riffdb.command-operation-envelope/v1+compiler-outcome`; keys are exactly
   `command_input`/`command_outcome_union`. Names, keys, IDs, and hashes are UTF-8
   JSON strings; hashes contain exactly 64 lowercase hex characters.
   `{id}` is unsigned canonical decimal without sign/zero/leading zero. All IDs,
   keys, hashes, bodies, the identity's `command_id`, and the exact V2 name entry
   cross-bind one command or reject. ADR-0020 normalization/order/collisions and
   ADR-0064's 128-byte limit remain; dotted V1 is forbidden.
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
   The outcome union containing any `$defs` rejects. Any other edit or identity
   mismatch rejects; raw outcome is never output.
5. `riffdb-cli` may add one direct default-feature-disabled
   `riffdb-api-mcp` dependency solely for its safe common schema composer and
   descriptor projection. It enables no stdio/HTTP, service, or auth feature.
   `Cargo.toml`/`Cargo.lock` receive exact graph review. Hosted and artifact
   assembly call that one owner; no copied source/composer or compiler/query-
   module-to-MCP/service dependency is permitted.
6. The generated application-role artifact selects its declared commands and
   queries from exact deployed bundle/module identities but is not authority.
   Separately construct one closed catalog-test `CapabilityGrantV1`: tenant
   `TenantScope::Global`, partition `PartitionScopeV1::All`, empty field
   visibility, max scan
   rows 65535, no approval-required kinds, and no wildcard/admin inference. Its
   row-policy/export/reimport/vector-inspection extensions are `None`. Its
   canonical permission-set union is exactly every selected `InvokeCommand(lineage,command_id)`
   and `ExecuteNamedQuery(lineage,module_hash,query_name)`, plus one real atom
   witnessing every distinct fixed kind: `Unparameterized(kind)` only where
   permitted, otherwise the appropriate parameterized variant and valid fixture
   lineage/ID/module/name. Expected fixed descriptors come from the
   `riffdb-api-mcp` fixed registry; fixed visibility comes only from policy's
   `FixedToolCandidate::ALL` candidate-to-permission mapping under that grant.
   Dynamic visibility likewise comes only from exact policy candidates and the
   grant. Reject fixed/dynamic and dynamic/dynamic collisions before comparison;
   order visible fixed registry entries first, then visible dynamic names in
   exact lexicographic order. Returned tools and name patterns supply nothing.
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
