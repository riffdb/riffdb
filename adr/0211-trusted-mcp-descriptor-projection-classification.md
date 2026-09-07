---
adr: "0211"
title: Trusted MCP Descriptor Projection Classification
status: proposed
tier: guarantee
date: 2026-09-07
accepted: null
requires: [ADR-0008, ADR-0155, ADR-0198]
amends:
  - ADR-0198 Decision 5 only by classifying its exact shared descriptor-projection access in guarantee-tier handler.rs; ADR-0198 Decisions 1 through 10 otherwise remain exact
supersedes: []
requirements: [MCP-001, MCP-020, MCP-021, MCP-026, MCP-040, MCP-043, MCP-045, DX-044, DX-047, DX-049]
packages: [WP-779]
obligations:
  - id: OBL-0211-1
    package: WP-779
    proof: check-generated-mcp-projection-owner
    says: The exact descriptor_json signature delegates only to the private hosted to_mcp_tool projection, V6 production assembly calls it only on checked discovered command/query definitions, and no copied projection, inverse adoption, registration, dispatch, authority, or second production handler change is admitted.
  - id: OBL-0211-2
    package: WP-779
    proof: generated_mcp_catalog_matches_complete_hosted_descriptor_pages
    says: Generated V6 descriptors assembled through the trusted projection stream equal to the independently authorized hosted catalog through its exact terminal cursor.
review_triggers:
  - descriptor_json, to_mcp_tool, their visibility, return/error types, or exact delegation would change, or another descriptor projection owner would appear.
  - V6 assembly would use the generic constructor or accept caller-supplied schemas, names, metadata, annotations, envelope bytes, visibility, authority, or descriptor JSON.
  - Serialized descriptor JSON could be parsed, adopted, installed, registered, authorized, dispatched, or used as a fallback by RiffDB.
  - Hosted construction, descriptor bytes, filtering, authorization, ordering, pagination, dispatch, protocol, features, or dependency direction would change.
  - A classifier exception would weaken handler.rs guarantee review or this record would authorize another production handler change.
---
# ADR-0211: Trusted MCP Descriptor Projection Classification

## Context

ADR-0198 requires hosted discovery and generated MCP V6 artifacts to reuse one
descriptor projection owner. WP-779 therefore exposes the existing private
hosted projection through a narrow first-party Rust helper. The helper belongs
in `crates/riffdb-api-mcp/src/handler.rs`, which governance deliberately
classifies as guarantee tier before the crate's general surface rule. ADR-0198
and WP-779 are surface tier, so their accepted behavior is insufficient process
authority for that exact path even though the reuse itself is required.

`McpDynamicToolDefinition::new` already permits a Rust caller to construct a
local definition with title, description, annotations, and schemas. Rendering
such a local value neither makes it an ADR-0198 V6 descriptor nor supplies it to
RiffDB. This decision must preserve that distinction: authoritative V6 assembly
uses only checked discovered constructors, while arbitrary local serialization
remains inert and gains no registration, visibility, or dispatch authority.

## Decision

1. ADR-0198 remains the sole semantic authority for V6 identity, schema
   composition, descriptor shape, visibility, ordering, pagination, migration,
   and compatibility. This record changes only the governance classification
   and trust boundary of its shared descriptor-projection access.
2. The only newly authorized guarantee-path production change is
   `McpDynamicToolDefinition::descriptor_json(&self) ->
   Result<serde_json::Value, McpHandlerContractError>` in
   `crates/riffdb-api-mcp/src/handler.rs`. It serializes the result of the
   existing private `to_mcp_tool()` and maps serialization failure to the
   existing `McpHandlerContractError`.
3. `descriptor_json` may add no branch, default, field rewrite, schema
   composition, policy, filtering, dispatch, registration, fallback, or
   caller-selected projection behavior. No other production `handler.rs`
   change is authorized by this record.
4. `to_mcp_tool()` remains private and is the single descriptor projection used
   by hosted and stdio discovery. CLI artifact assembly calls that owner through
   `descriptor_json`; it may not copy or independently reconstruct the
   descriptor.
5. V6 command assembly may call `descriptor_json` only on a definition produced
   by `from_discovered_command` after ADR-0198's exact command, compiler-schema,
   name, envelope, body, and hash checks. V6 query assembly may call it only on
   a definition produced by `from_discovered_query` after exact module, name,
   schema-body, and hash checks. Neither path may use the generic `new`
   constructor to inject artifact or caller metadata.
6. `descriptor_json` does not claim that an arbitrary value built through the
   pre-existing generic constructor has ADR-0198's closed V6 shape. In
   particular, such a local definition may carry title or description. Only the
   checked discovered constructors fix the absent optionals and exact
   annotations required for authoritative V6 evidence.
7. Returned JSON is an owned, inert presentation value. RiffDB accepts it as no
   registration, adoption, installation, capability, visibility, authorization,
   dispatch, invocation, or fallback input. This decision adds no inverse
   parser or descriptor-adoption path.
8. The helper exposes one existing first-party projection to another first-party
   crate. It adds no stdio/HTTP, service, auth, compiler-to-MCP, or
   query-module-to-MCP feature or dependency. Hosted construction, filtering,
   authorization, dispatch, invocation, validation, results, notifications,
   protocol, Protobuf, IR, bundles, plans, durable data, and storage remain
   unchanged.
9. Governance receives no path exception. `handler.rs` remains guarantee tier.
   After exact acceptance, a separate governance-only change retiers WP-779 to
   guarantee, adds this ADR to `required_adrs` and `allowed_paths`, and
   strengthens its shared-owner deliverable with Decisions 2 through 7. No
   implementation or generated artifact belongs in that governance change.
10. Existing unmerged WP-779 commits need not be rewritten: a surface trailer
    cannot lower the handler path's computed guarantee tier. Exact acceptance,
    the post-acceptance package edit, and guarantee review must precede merge.

## Options considered

1. **Copy the descriptor fields in CLI:** rejected because optional MCP fields,
   annotation defaults, and future hosted changes could drift between owners.
2. **Move or except `handler.rs` from guarantee classification:** rejected
   because the classifier correctly protects authorization-adjacent hosted MCP
   behavior and a local exception would conceal future changes.
3. **Make arbitrary serialized definitions authoritative:** rejected because it
   would let caller-chosen schemas or metadata cross the safe interface.
4. **Authorize the exact inert projection and retier WP-779:** selected because
   it satisfies ADR-0198's one-owner rule without widening authority or runtime
   behavior.

## Consequences

- Hosted and generated evidence use the same projection, so optional fields and
  annotations cannot drift through duplicated assembly.
- The additive public Rust helper is guarantee-reviewed even though it performs
  no state transition and changes no hosted or database semantic result.
- V6 assembly is constrained to checked discovered constructors; generic local
  construction remains possible but non-authoritative.
- WP-779 gains one architecture checker and guarantee ceremony. Existing
  ADR-0198 semantic, fixture, migration, and compatibility obligations remain.
- General descriptor import, external tool registration, caller metadata,
  fallback descriptors, and runtime configuration remain outside this decision.

## Standing design tests

- **Interface safety:** Applications, agents, SDKs, roles, transports, and
  configuration cannot install descriptor JSON, supply authoritative schemas or
  metadata, change visibility, gain authority, or dispatch a tool. A local DTO
  and its serialized value carry no RiffDB capability.
- **Scale:** Projection remains one bounded serialization per already bounded
  generated descriptor. It adds no catalog retention, aggregate inventory,
  pagination state, I/O, task, queue, or population-sized work.

## Checks

- `check-generated-mcp-projection-owner --self-test` rejects changed delegation,
  public `to_mcp_tool`, a copied owner, generic-constructor V6 use, inverse
  adoption/registration, another production handler change, and each allowed
  exact shape's near miss.
- `generated_mcp_command_catalog_v6_composes_authoritative_schemas` proves the
  checked command/query constructors and exact closed V6 descriptor shapes.
- `generated_mcp_catalog_matches_complete_hosted_descriptor_pages` proves exact
  generated-to-hosted parity under independent policy and terminal pagination.
- `generated_mcp_catalog_v6_topology_and_lock_migration_are_exact`, allowed-path,
  dependency, requirement, obligation, generated, MCP, handbook, workspace,
  clippy, and `ci-all` checks retain ADR-0198's compatibility evidence.
