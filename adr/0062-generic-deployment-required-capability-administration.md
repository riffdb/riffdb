# ADR-0062: Generic Deployment-Required Capability Administration

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `MCP-010`, `MCP-011`, `MCP-030`, `MCP-041`,
  `MCP-046`, `SEC-001`, `SEC-002`, `SEC-003`
- **Related work packages:** `WP-130`, `WP-140`, `WP-150`, `WP-185`
- **Amends:** ADR-0007, ADR-0009, ADR-0032, ADR-0034, ADR-0043, ADR-0044

## Context

A newly initialized generic RiffDB database has an authenticated bootstrap owner
but no active application contract. ADR-0007 therefore places it in
`DeploymentRequired` and admits authenticated health and contract-management
operations while withholding application commands, entity access, and other
ready-only operations.

The source installation helper must not select or deploy an example application
on the operator's behalf. It must be possible to leave the database in this
empty-catalog state and create a distinct, attributable agent identity that can
validate, inspect, and deploy the agent's first application contract. Reusing
the bootstrap owner's credential would collapse operator and agent provenance.
Deploying LegalSpend, TicketDesk, or any other bundled example merely to unlock
normal capability administration would make generic installation depend on an
unrelated application.

The existing capability-administration path already authenticates through the
normal credential entry point, authorizes against current capability facts,
records durable service audit, and applies transitions only through the commit
coordinator. The missing behavior is the server lifecycle admission of that
path before the first active contract.

The human maintainer explicitly accepted this decision in the current Codex
session on 2026-07-30.

## Decision

`DeploymentRequired` admits authenticated normal `CreateCapability`,
`RevokeCapability`, `DiscoverCommandTools`, and `DiscoverResources` operations in
addition to the authenticated operations already accepted by ADR-0007:

- `Health`;
- contract validation;
- command explanation;
- contract deployment; and
- active-contract discovery.

This is an additive lifecycle admission decision only. Capability creation and
revocation continue to use the same API-neutral application service, current
deny-by-default policy evaluation, transaction-current capability verification,
ordered durable administration audit, idempotency and uncertainty behavior, and
commit-coordinator transition used in `Ready`.

Command and resource discovery continue to use the shared, authenticated,
policy-filtered application-service operations. With no active contract, their
catalog fence is `NoActiveContract`: fixed control-plane tools and resources may
be returned according to current capability policy, while the dynamic
application-command collection is empty. MCP must use these operations for
`tools/list`, `resources/list`, and `resources/templates/list`; it does not
synthesize an unfiltered pre-deployment registry.

The exception does not admit command execution, query execution, entity reads,
commit or provenance reads, projections, outbox operations, statistics, offline
maintenance, or any storage access. MCP, gRPC, CLI, and SDK callers receive no
alternate path and no transport gains direct policy, coordinator, or storage
authority.

Bootstrap remains the sole principal-less mutation and the sole operation that
may create durable service-audit records without a principal. Normal capability
administration in `DeploymentRequired` requires an authenticated current
capability carrying the exact authority required by existing policy:

- ordinary `CreateCapability` and `RevokeCapability` remain constrained by the
  complete subset rules;
- `AdministerCapabilities` retains its existing same-database and
  same-environment administrative semantics; and
- revocation, expiry, audience, database, environment, tenant, partition,
  approval, and output obligations continue to fail closed.

Before an active catalog exists, a requested explicit partition scope cannot be
proved against contract metadata and therefore continues to fail closed. A
generic pre-deployment agent capability uses global tenant scope, all
partitions, and only the contract and health permissions needed to author and
deploy the first application. Application-specific authority is created or
bound only after that application contract is active.

## Consequences

- Source installation can leave the database generic and empty while producing
  separate operator and application-author identities.
- The database correctly reports `NotReady` until an application contract is
  active even though its authenticated control plane is usable.
- Agent provenance no longer needs to reuse the bootstrap owner identity.
- The exact `DeploymentRequired` allowlist grows by four operations and must
  remain exhaustively tested.
- Installation does not grant wildcard command, entity, or query access to an
  undeclared future application.

## Rejected Alternatives

- **Auto-deploy a bundled example.** This makes generic installation
  application-specific and misrepresents the empty-database lifecycle.
- **Reuse the bootstrap owner token for an agent.** This loses distinct actor
  attribution and gives the agent broad administrative authority.
- **Permit all authenticated administration or discovery-adjacent operations.** Statistics,
  maintenance, application reads, and other ready-only surfaces are unnecessary
  and would weaken the closed lifecycle.
- **Add a second bootstrap mutation.** This would violate the singleton
  principal-less bootstrap and audit exception.
- **Give MCP direct capability or storage access.** This violates the shared
  application-service and policy boundary.

## Compatibility

This decision adds no public RPC, MCP method, protocol field, source-language
construct, IR encoding, durable record, storage key, or migration. Existing
credentials and databases are compatible. It changes only authenticated server
lifecycle routing for four existing operations.

## Security

Every newly admitted request is authenticated and reauthorized against current
policy. Existing subset or administrator checks, transaction-current
verification, durable auditing, redaction, bounds, and coordinator ownership
remain mandatory. The server keeps an exhaustive closed allowlist so unknown or
new operations remain denied in `DeploymentRequired`.

## Testing

- Exhaustively compare every `ServiceOperationV1` against the exact
  `DeploymentRequired` allowlist.
- Prove the allowlist remains closed both after startup into
  `DeploymentRequired` and immediately after successful bootstrap.
- Run the source-bootstrap smoke test against a database with no active
  contract; create a distinct generic MCP developer capability through gRPC and
  confirm the server remains `NotReady`.
- Confirm generated MCP visibility for that capability contains only health and
  contract authoring/deployment tools, with no dynamic application command.
- Preserve capability create/revoke authorization, idempotency, uncertainty,
  audit, and coordinator tests at the shared service boundary.

## Requirements and Work Packages

- **Requirements:** `MCP-010`, `MCP-011`, `MCP-030`, `MCP-041`, `MCP-046`,
  `SEC-001`, `SEC-002`, `SEC-003`
- **Defines or amends:** `WP-130`, `WP-140`, `WP-150`, `WP-185`
- **Final evidence:** generic source-bootstrap smoke and MCP authorization
  conformance
