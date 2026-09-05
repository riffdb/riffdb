---
adr: 0198
title: Generated MCP Catalog Descriptor Parity
status: proposed
tier: surface
date: 2026-09-05
accepted: null
requires: [ADR-0008, ADR-0020, ADR-0040, ADR-0047, ADR-0124, ADR-0155, ADR-0194]
amends:
  - ADR-0155 generated MCP package artifact only by making its command descriptors exact projections of the accepted hosted MCP descriptors
  - ADR-0194 generated application-operation topology only by adding V6 for command-bearing catalogs while retaining its V2 through V5 query selection
supersedes: []
requirements: [MCP-001, MCP-020, MCP-021, MCP-026, MCP-040, MCP-043,
  MCP-045, DX-044, DX-047, DX-049, VER-001, VER-002, VER-003, VER-004,
  VER-008]
packages: [WP-779]
obligations:
  - id: OBL-0198-1
    package: WP-779
    proof: generated_mcp_command_catalog_v6_composes_authoritative_schemas
    says: V6 command entries use the exact compiler name, input, and outcome union and the exact MCP-owned composition with the accepted service envelope, without a copied semantic source or alternate descriptor metadata.
  - id: OBL-0198-2
    package: WP-779
    proof: generated_mcp_catalog_matches_complete_hosted_descriptor_pages
    says: A bounded all-page hosted discovery proves exact ordered descriptor equality and rejects missing, extra, duplicate, malformed, over-bound, or cursor-looping tools.
  - id: OBL-0198-3
    package: WP-779
    proof: generated_mcp_catalog_v6_topology_and_lock_migration_are_exact
    says: V6 selection, predecessor preservation, reviewed fixture rotation, application-lock identity migration, and refusal of a mismatched envelope or generated artifact are exact.
review_triggers:
  - A compiler input or outcome schema, service envelope, composed output schema, tool name, descriptor field, annotation, ordering, or visibility rule would change.
  - A generated MCP catalog, fixture, artifact hash, or application-lock identity would rotate without an exact human diff and hash review.
  - A compiler-to-service or compiler-to-MCP dependency, copied envelope source, runtime fallback, compatibility alias, or unbounded discovery walk would be introduced.
---
# ADR-0198: Generated MCP Catalog Descriptor Parity

## Context

ADR-0008 already fixes the hosted command descriptor: the compiler owns its
tool name, input schema, and declared-outcome union; the service owns the generic
operation envelope; and MCP mechanically composes the union into the advertised
`outputSchema`. The generated application-operation artifact instead labels the
bare outcome union `result_schema`, derives another command name and descriptive
metadata, and therefore cannot predict the hosted descriptor. Treating that
artifact as conformance evidence would bless two public contracts.

This is an artifact defect, not permission to change the accepted runtime.
ADR-0040's discovery DTO, ADR-0047's object-root correction, ADR-0194's query
envelope, authorization, dispatch, execution, and result rendering stay exact.

## Decision

1. Add `riffdb-generated-application-operations/v6`. Every newly compiled
   command-bearing MCP artifact uses V6; a command-free artifact retains
   ADR-0194's least-sufficient V2 through V5 selection. V6 retains applicable
   query, SDK-only, vector, and pagination registries without reinterpreting
   their accepted schemas. V2 through V5 bytes and readers remain exact.
2. Each V6 command entry contains the exact ADR-0020 bundle registry tool name,
   source command and immutable bundle/plan identities, canonical
   `input_schema`, canonical `outcome_schema`, and canonical `output_schema`.
   Input and outcome are the unchanged compiler artifacts. Output is solely the
   existing MCP composition that replaces the accepted service envelope's one
   false `outcome` definition with that exact union after removing only its
   Draft 2020-12 `$schema` member. Any other edit or composition rejects.
3. The V6 catalog records the accepted command-operation-envelope ID and hash,
   not a second authoritative envelope source. Artifact assembly calls one
   bounded pure composer owned with the MCP presentation boundary; the compiler
   and query-module compiler logic gain no service or MCP dependency. The
   composer verifies the exact service-owned source identity before producing
   output bytes and repeats the composed hash.
4. The public descriptor projection of a V6 command is exact: `name` is the
   compiler tool name; `title` and `description` are absent; annotations are
   `readOnlyHint=false`, `destructiveHint=false`, `idempotentHint=true`, and
   `openWorldHint=false`; `inputSchema` is `input_schema`; and `outputSchema` is
   `output_schema`. Documentation metadata is not a tool descriptor and cannot
   override that projection. Query descriptors retain their accepted direct
   input/result schemas, pagination envelope, absent title/description, and
   conservative annotations.
5. Conformance deploys the exact lock and role, enumerates hosted `tools/list`
   to terminal `nextCursor`, and compares the complete ordered role-visible
   inventory with the union of the accepted fixed registry and the generated
   descriptor projection. It compares every name, title, description,
   annotation, `inputSchema`, and `outputSchema`; it does not filter away an
   unknown dynamic-looking tool. Missing, extra, duplicate, reordered, malformed,
   or schema-different descriptors fail.
6. Enumeration preserves ADR-0008's service page limit 500, three-call and
   1,024-item ceilings, exact cursor binding, and 4 MiB output ledger. It rejects
   an item or schema over its accepted bound, a continuation after item 1,024,
   repeated/noncanonical cursor text, a cursor loop, a nonterminal empty page,
   and any aggregate overflow before retaining or serializing excess data. No
   page is skipped, synthesized, filtered, or fetched concurrently.
7. V6 rotates every affected generated MCP fixture and artifact hash and thus
   the enclosing application-lock identity. It does not advance application
   lock V8's schema: V8 already binds the exact kind, path, and content hash.
   Old locks continue to validate their exact V2-V5 artifacts; they do not claim
   V6 descriptor parity. Migration is explicit, previews catalog/artifact/lock
   hashes, requires the existing exact lock acceptance, and never rewrites a
   deployed catalog or application source implicitly.
8. Hosted catalog construction, list-change notification, policy filtering,
   invocation, input validation, operation-envelope bytes, completion objects,
   query cursor authority, protocol, Protobuf, IR, bundle, plan, durable data,
   and storage are unchanged. A generated artifact is expectation evidence, not
   authority and never enables, hides, or dispatches a tool.

## Compatibility

V6 is a new generated-surface identity and its lock-hash rotation is intentional.
There is no dual descriptor, raw-union output fallback, runtime content
negotiation, or in-place reinterpretation of V2-V5. Consumers pinned to an old
lock may keep its exact artifact; consumers claiming hosted parity migrate to an
accepted V6 lock. Handbook compatibility guidance names that distinction.

## Standing design tests

- **Interface safety:** an application can select or inspect only exact generated
  descriptors; it cannot supply schemas, envelope bytes, annotations, authority,
  dispatch, validation, or a fallback result shape.
- **Scale:** each schema is composed once under existing checked schema/artifact
  limits, and conformance retains at most 1,024 bounded descriptors across at
  most three sequential pages with the existing 4 MiB response ledger.

## Checks

- The three obligation proofs freeze authoritative schema composition, exact
  all-page descriptor equality, fail-closed pagination, V2-V6 topology, fixtures,
  migration, and lock identities.
- `scripts/check-generated`, version-topology, driver/MCP conformance, handbook,
  requirement, obligation, allowed-path, workspace test, clippy, and `ci-all`
  checks cover the complete generated and hosted path.
