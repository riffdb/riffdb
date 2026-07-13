# ADR-0008: Native MCP Tool and Resource Model

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Partially resolved by:** ADR-0020 for command tool-name grammar,
  normalization, collision handling, and compiler/catalog ownership only
- **Decision deadline:** Before WP-180 or WP-140, whichever starts first, merges
  any remaining MCP resource, presentation, audience, or transport fixture

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

MCP resource identity, schema presentation, transport composition, and
authorization are public and security boundaries. Current examples use conflicting
tool names. Stdio and HTTP must not become separate semantic implementations.

## Proposed Decision

Accepted ADR-0020 owns command tool-name grammar, normalization, collision
handling, and compiler/catalog ownership. This Proposed record neither changes
nor reopens those decisions. Resource URI construction still needs stable typed
identifiers and one specified percent-encoding/canonicalization rule.

Input and output schemas are the compiler's transport-neutral JSON Schema
artifacts. MCP wraps but does not reinterpret them. Discovery is policy-filtered,
but every tool invocation and resource read reauthorizes and applies obligations.
Stale names fail safely after catalog change.

Production `riffdb-mcp` stdio is a gRPC client and uses a gRPC-scoped capability.
It does not link storage or an in-process privileged service. HTTP MCP is composed
into `riffdbd` by WP-185 and calls the same API-neutral service. HTTP uses the
configured canonical `/mcp` resource URI/audience, never a caller-controlled
`Host` header.

## Options Considered

1. **Qualified contract and command names:** Approved, stable collision domain.
2. **Unqualified command names:** Collide across contracts.
3. **Collision hashes/suffixes:** Avoid deployment failure but make compatibility
   dependent on catalog population and algorithm choices.
4. **In-process stdio:** Simpler but risks a privileged path distinct from gRPC.

## Consequences

- Contract deployment performs ADR-0020's accepted name validation.
- Renaming contracts or commands has ADR-0020's public compatibility effect.
- Schema bugs are fixed in compiler artifacts, not transport-specific copies.
- Final HTTP/server composition waits for WP-185.

## Compatibility

ADR-0020 freezes command tool names. Resource URI grammar, schema
bytes/semantics, pagination cursor text, and MCP protocol behavior remain public
fixtures to freeze here.

## Security

Discovery never grants access. Invocation-time policy checks, canonical audience
binding, safe text rendering, bounds, cancellation, and redaction are mandatory
for both transports.

## Testing

ADR-0020 owns name and normalization fixtures. This record retains golden URI
and presentation-schema fixtures, JSON/value conversion fuzzing,
init/list/pagination/progress/cancel protocol tests, stale-name denial,
authorization matrices, secret canaries, and MCP Inspector tests over stdio and
HTTP.

## Requirements and Work Packages

- **Requirements:** `MCP-001`, `MCP-010`, `MCP-011`, `MCP-020` through `MCP-024`,
  `MCP-030` through `MCP-033`, `MCP-040` through `MCP-049`
- **Defines or blocks:** the remaining MCP work in `WP-140`, MCP-aware
  presentation/observability in `WP-180`, and final composition in `WP-185`;
  ADR-0020 separately defines the early `WP-040`/`WP-050` name boundary
- **Final evidence:** `WP-200`

## Decision Deadline

Accept URI, schema-presentation, cursor-presentation, audience, and transport
text before WP-180 or WP-140, whichever starts first, creates any corresponding
public compatibility fixture or exposes a production tool.
