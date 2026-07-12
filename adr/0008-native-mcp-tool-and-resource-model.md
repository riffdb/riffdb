# ADR-0008: Native MCP Tool and Resource Model

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Decision deadline:** Before WP-140 public names, schemas, or fixtures merge

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

MCP naming, URI identity, schema generation, transport composition, and
authorization are public and security boundaries. Current examples use conflicting
tool names. Stdio and HTTP must not become separate semantic implementations.

## Proposed Decision

Command tools use:

```text
riffdb.cmd.<normalized_contract>.<normalized_command>
```

For example, `riffdb.cmd.legal_spend.allocate_budget`. Segments are lowercase
ASCII snake case under explicit length/character bounds. Catalog deployment
rejects normalization collisions; it never repairs names with unstable suffixes
or hashes. Resource URI construction uses stable typed identifiers and one
specified percent-encoding/canonicalization rule.

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

- Contract deployment performs name normalization and collision validation.
- Renaming contracts or commands is a public MCP compatibility change.
- Schema bugs are fixed in compiler artifacts, not transport-specific copies.
- Final HTTP/server composition waits for WP-185.

## Compatibility

Tool names, resource URI grammar, schema bytes/semantics, pagination cursors, and
MCP protocol behavior are public fixtures. Normalization rules are versioned.

## Security

Discovery never grants access. Invocation-time policy checks, canonical audience
binding, safe text rendering, bounds, cancellation, and redaction are mandatory
for both transports.

## Testing

Golden name/URI/schema fixtures, normalization collision properties, JSON/value
conversion fuzzing, init/list/pagination/progress/cancel protocol tests, stale-name
denial, authorization matrices, secret canaries, and MCP Inspector tests over
stdio and HTTP.

## Requirements and Work Packages

- **Requirements:** `MCP-001`, `MCP-010`, `MCP-011`, `MCP-020` through `MCP-024`,
  `MCP-030` through `MCP-033`, `MCP-040` through `MCP-049`
- **Defines or blocks:** `WP-040`, `WP-050`, `WP-110`, `WP-130`, `WP-140`, `WP-185`
- **Final evidence:** `WP-200`

## Decision Deadline

Accept normalization, URI, schema, and transport text before WP-140 creates any
public compatibility fixture or exposes a production tool.
