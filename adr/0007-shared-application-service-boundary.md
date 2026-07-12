# ADR-0007: Shared Application-Service Boundary

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Decision deadline:** Before WP-120 public service traits merge

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

gRPC, MCP, CLI, and SDK must expose identical command, authorization, outcome,
and consistency semantics. If an adapter reaches storage or orchestrates only a
subset of policy and commit behavior, it becomes a privileged bypass.

## Proposed Decision

Transport adapters authenticate credentials and convert them into a bounded
internal `Principal` plus transport context. The API-neutral application service
authorizes every operation, applies policy obligations and redaction, resolves
catalog and query semantics, and invokes the commit executor. The commit layer
alone owns command admission, idempotency, conflict acquisition, runtime
evaluation, revalidation, and terminal commit.

Service traits and DTOs contain domain types only. They expose no Tonic, rmcp,
redb, raw storage, or transaction types. MCP HTTP, MCP stdio through gRPC, the CLI,
generated Rust SDK, and direct gRPC all use the same service semantics. Every
resource read, query, mutation, wait, and administrative operation reauthorizes;
discovery filtering is not an authorization decision.

Authoritative catalog and capability changes use typed coordinator operations.
They may have a separate ordered administrative audit record rather than an
application `CommitSequence`, but never mutate storage directly. Initial local
operator bootstrap is a one-time coordinator operation valid only for an empty
database.

## Options Considered

1. **API-neutral service plus commit executor:** Approved layering.
2. **Transport-specific orchestration:** Duplicates semantics and creates drift.
3. **Service exposes storage transactions:** Violates the coordinator boundary.
4. **MCP reads storage directly:** Creates a policy and redaction bypass.

## Consequences

- Transport crates remain thin conversion, protocol, deadline, and streaming
  adapters.
- Authorization is testable once and again at every transport boundary.
- Service DTO changes must be coordinated before adapter work proceeds in parallel.
- Administrative audit ordering is separate from application command sequencing
  unless a later accepted ADR deliberately unifies them.

## Compatibility

Service traits are internal shared interfaces, while gRPC/MCP/SDK mappings are
public compatibility surfaces. Storage types and policy-engine internals never
leak into them.

## Security

Authentication does not imply authorization. Policy is deny-by-default, caller
provenance is untrusted, obligations apply before serialization, and redaction
precedes logs, metrics, public errors, and MCP text.

## Testing

Use dependency architecture checks, fake-port service tests, deny/default and
obligation matrices, stale-discovery invocation tests, secret canaries across all
adapters, and transport parity tests over one shared scenario corpus.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `API-001`, `SEC-001` through `SEC-004`, `MCP-001`
- **Defines or blocks:** `WP-110`, `WP-120`, `WP-130`, `WP-140`, `WP-150`, `WP-185`
- **Final evidence:** `WP-200`

## Decision Deadline

Accept exact service, policy, and commit ownership before WP-120 traits merge.
WP-100 may develop its result ports alongside this Proposed record.
