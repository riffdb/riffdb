# ADR-0047: MCP Object-Root Schema Conformance

- **Status:** Accepted
- **Direction approved:** 2026-07-23
- **Exact text accepted:** 2026-07-23
- **Accepted:** 2026-07-23
- **Acceptance reference:** Human maintainer confirmation in the current Codex
  session on 2026-07-23
- **Requires:** ADR-0007, ADR-0008, ADR-0011, ADR-0027, ADR-0040, and
  ADR-0046
- **Amends:** ADR-0008's exact MCP schema compatibility boundary and the
  WP-137/WP-140 fixed-tool and operation-schema checkpoints
- **Decision deadline:** Before WP-140 MCP Inspector and conformance evidence
  can pass

The human maintainer accepted this one-time pre-release correction after the
official MCP Inspector rejected otherwise valid closed-object schemas whose
root omitted the literal JSON Schema assertion `"type":"object"`.

## Context

RiffDB's fixed-tool result schemas and service-owned command-operation schemas
already accept only object instances: their top level is a nonempty `oneOf` of
closed object branches. One fixed input schema likewise has two closed object
branches. The common validator therefore correctly enforces the intended
instance set.

The MCP SDK and Inspector boundary has a narrower structural requirement for
advertised `inputSchema` and `outputSchema`: the schema root itself must carry
the literal object type. Eleven accepted fixed-tool sources and two
service-owned operation sources omit that redundant assertion. Presentation
cannot add it after loading because the advertised bytes would then disagree
with the checked schema identity and hash.

The POC has not shipped, and the affected `/v1` fixtures are still awaiting
their final combined WP-137/WP-140 human acceptance. A correction must still be
explicit because ADR-0008 otherwise treats their exact IDs, bytes, and hashes as
public compatibility boundaries.

## Decision

As a one-time pre-release exception, retain the existing schema IDs and add the
single top-level member `"type":"object"` to exactly these 13 canonical
sources:

1. Fixed input:
   `riffdb.fixed-tool/riffdb.command.get_outcome/input/v1`.
2. Fixed results for `riffdb.contract.validate`,
   `riffdb.contract.get_active`, `riffdb.contract.explain_command`,
   `riffdb.contract.deploy`, `riffdb.entity.get`, `riffdb.commit.get`,
   `riffdb.provenance.trace`, `riffdb.projection.query`,
   `riffdb.projection.status`, and `riffdb.server.health`.
3. Service-owned `riffdb.command-get-outcome-result/v1`.
4. Service-owned `riffdb.command-operation-envelope/v1`.

No other fixed, generated, compiler-owned, service-owned, public, or durable
schema changes under this exception. In particular, compiler-owned declared-
outcome unions remain unchanged because they are nested inside the typed
command-operation envelope rather than advertised as the MCP result root.

Canonical key ordering is preserved. The fixed-source aggregate changes from
65,363 to 65,539 bytes. The command-operation envelope changes from 2,545 to
2,561 bytes with schema hash
`781ff93c2dbfd2ee2bec286f7810300a0fec0a170548b1405cb8ba2ac8d90398`.
The GetOutcome result changes from 4,729 to 4,745 bytes with schema hash
`4056f01c297120b06ac905f33482132a9085865975ada36e2396a61ebf19fc0d`.
Every affected fixed-schema length and hash is regenerated from its exact
canonical source.

The root assertion is redundant with the existing closed object branches.
Consequently, the set of accepted JSON instances, request and result
converters, compiler artifacts, bundle and plan hashes, contract IR, canonical
values, public Protobuf, service DTOs, resource URIs, durable encodings, and
storage keys do not change.

Future changes to any accepted schema ID, exact bytes, hash, or accepted
instance set require normal compatibility review and a new version where
applicable. This exception is not a general in-place evolution policy.

WP-137 owns the two existing service schema sources and their exact service
hash constants. WP-140 owns the eleven existing fixed schema sources, the
fixed-tool registry, interface checkpoint, tool-descriptor conformance, and
Inspector evidence. The already accepted paths for those packages cover this
correction. There is no new dependency, Cargo feature, RPC, service operation,
transport route, resource, work-package dependency, or gate change.

## Options Considered

1. **Add a presentation-only root overlay:** rejected because the MCP
   descriptor bytes would no longer match the immutable source identity and
   hash released by discovery.
2. **Publish corrected `/v2` schema IDs:** rejected for this unshipped,
   instance-equivalent checkpoint correction because it would introduce
   version negotiation and duplicate operation identities without preserving a
   useful deployed compatibility boundary.
3. **Leave the roots implicit:** rejected because the official SDK/Inspector
   boundary requires the explicit object root and WP-140 conformance cannot
   pass.
4. **Change every `oneOf` schema, including compiler artifacts:** rejected
   because only advertised MCP tool roots need the literal assertion and
   compiler artifact bytes are independent semantic interfaces.

## Consequences

- The official MCP Inspector can consume every advertised fixed and dynamic
  tool input/output descriptor without adapter-side schema rewriting.
- Exact bytes, lengths, hashes, the fixed registry, and checkpoint inventory
  change for the affected sources.
- Existing JSON instances and all semantic conversions remain byte-for-byte
  unchanged.
- Final combined WP-137/WP-140 fixture acceptance remains a separate human
  review; this ADR authorizes the correction but does not pre-accept generated
  artifacts.

## Compatibility

This is an explicit one-time pre-release in-place `/v1` correction. It changes
public schema bytes and hashes but is instance-set equivalent. There is no wire,
SDK, command-language, IR, bundle, plan-hash, durable-data, storage, URI, or
runtime migration.

Any consumer that pinned the provisional hashes must update to the corrected
checkpoint. No released consumer or persisted database format is affected.

## Security

The change adds no data, authority, or execution path. Requiring the advertised
root type makes schema interpretation more explicit and prevents transport
tooling from applying a looser or divergent default. MCP remains a
policy-filtered transport over the shared application service.

## Testing

- Canonical-source checks prove all 27 fixed schemas remain compact,
  key-sorted Draft 2020-12 JSON and reproduce their exact lengths and
  domain-separated hashes.
- Registry tests prove every advertised fixed input and output schema has the
  literal object root and that the two operation sources reproduce the service
  identities.
- Dynamic descriptor tests prove the service command-operation envelope is the
  advertised output root while compiler outcome schemas remain nested and
  unchanged.
- The common MCP conformance suite checks every listed tool over stdio and
  Streamable HTTP.
- `scripts/mcp-inspector-smoke` supplies official Inspector evidence for both
  transports.
- The generated interface-checkpoint script proves exact artifact
  reproducibility.

## Requirements and Work Packages

- **Requirements:** `API-001`, `MCP-001`, `MCP-010`, `MCP-020`, `MCP-021`,
  `MCP-030`, `MCP-040`, `MCP-043`, `MCP-045`, and `POC-007`
- **Defines or blocks:** Corrective `WP-137` schema identity evidence and
  `WP-140`
- **Final evidence:** `WP-140` and `WP-200`

## Decision Deadline

Accepted on 2026-07-23 before the corrected WP-140 compatibility fixtures and
official Inspector evidence merge. Any broader or incompatible alternative
requires a new ADR and explicit human review.
