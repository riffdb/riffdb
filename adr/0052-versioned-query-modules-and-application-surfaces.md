# ADR-0052: Versioned Query Modules and Application Surfaces

- **Status:** Accepted
- **Direction approved:** 2026-07-28
- **Exact text accepted:** 2026-07-28
- **Acceptance reference:** Maintainer authorization in the current Codex
  session to make the implementation decisions required through WP-270
- **Requires:** ADR-0006, ADR-0007, ADR-0008, ADR-0013, ADR-0020,
  ADR-0021, ADR-0027, ADR-0037, ADR-0040, ADR-0041, ADR-0046, ADR-0047,
  and ADR-0051
- **Amends:** SPEC Sections 1.1, 4.1, 7.7, 9, 12, 13, 17,
  19, and 20.7
- **Implementation boundary:** Before WP-250 changes a public transport or WP-260
  persists a prepared-query artifact

The maintainer accepted this exact versioned-query and application-surface
boundary on 2026-07-28 and authorized the implementation decisions required
through WP-270.

## Context

Stable application queries need reviewable source, compatible result types,
cached validated plans, generated SDKs, and generated MCP tools. Ad-hoc RiffQL
must remain available for agents without making generated clients a prerequisite.
The existing public Protobuf is a frozen low-level surface and MCP currently
advertises storage-shaped fixed tools alongside dynamic command tools.

## Decision

### Immutable query modules

A query module is a canonical immutable artifact containing:

- module name and positive module version;
- exact contract lineage, contract version, and bundle hash;
- RiffQL language and query-IR versions;
- bounded source and source hash;
- canonical named-query IR and plan hashes;
- parameter, result, and declared-result-union schemas;
- canonical required-access sets and explain plans; and
- source maps and compiler compatibility metadata.

Module identity is the canonical hash of all semantic fields above. It contains
no deployment time, compiler clock, machine path, runtime parameter, credential,
or environment value.

One exact contract version may retain multiple immutable module versions. It has
at most one active query-module pointer. Deployment validates and stores the
candidate plus an optional expected active module identity atomically as an
audited control-plane operation. Same-identity deployment is idempotent;
same-version/different-content deployment conflicts. A module cannot be
activated unless its exact contract bundle remains available and hash-equal.

Named execution selects either the active module for an exact contract version
or an exact module identity. Generated clients always pin the exact contract and
module identities. Responses repeat contract, module, query, and plan identity.
Query-module deployment never activates or changes a contract.

### Public application API

Add an additive `riffdb.app.v1` unary gRPC API over the shared API-neutral
application service:

- check an ad-hoc query or module;
- explain an ad-hoc or named query;
- execute an ad-hoc query;
- execute a named query;
- deploy a query module; and
- inspect active or exact module metadata and generated schemas.

Requests submit RiffQL text or a named selector plus name-addressed parameters.
They never submit stable IDs, field masks, encoded keys, an access plan, a
permission set, or storage cursor bytes.

Results use the existing exact value algebra but every application record field
is name-addressed and schema-bound. Result envelopes contain an exact result
branch, schema identity, snapshot position, and optional opaque cursor. Public
messages and API-neutral DTOs receive explicit independent size bounds and
versioned compatibility fixtures.

The existing `riffdb.v1` services remain byte-compatible and supported as the
kernel protocol. Documentation de-emphasizes their storage-shaped query
operations for application work, but neither renames nor calls them unstable.
The application service does not call its own gRPC adapter; both protocol
families call API-neutral services.

The existing `CommandService.Execute` remains the command wire operation because
it already accepts a command name and name-addressed submitted fields. WP-250
adds ergonomic client/CLI builders rather than a second command executor or a
mixed read/write text RPC.

### MCP and CLI

The primary MCP builder interface adds fixed symbolic operations for contract
description, query check/explain/execute, symbolic command run, and diagnostic
explanation. Query execution and command execution remain distinct MCP risk
classes and methods. Existing MCP tools and resources remain compatible.

Active contract/module resources expose bounded policy-filtered schema, grammar,
examples, query metadata, explain plans, and diagnostics. Runtime query text,
parameters, hidden schema, capability tokens, and internal incidents are never
published as resources.

Each visible named query may additionally generate one MCP tool. The compiler
owns deterministic tool naming, schema generation, collision rejection, and
catalog identity; adapters consume those artifacts verbatim.

The CLI and REPL use the same public application gRPC operations. `.riffq`
check/deploy/run, ad-hoc query, explain, and client generation do not access
storage directly. `run Command { ... }` is CLI parsing that builds the existing
symbolic command request.

### Generated clients

Rust and TypeScript generators consume exact query-module artifacts. They
generate parameter, result, declared-result-union, cursor, and operation types
for named queries plus name-addressed command input/outcome conveniences.
Generated code embeds exact contract/module identities and rejects an
identity-mismatched response. Generation is optional and reproducible.

## Options Considered

1. **Put queries inside each contract bundle:** rejected because query-only
   iteration would force contract activation and couple compatibility domains.
2. **Store only query text in a mutable registry:** rejected because it lacks
   immutable plan/schema identity and reproducible generation.
3. **Replace the existing gRPC protocol:** rejected before evidence shows unary
   transport framing is material.
4. **One mixed text execution operation:** rejected because read/write risk
   classification would occur too late and weaken MCP review boundaries.

## Consequences

- Stable applications can pin generated types while agents retain ad-hoc text.
- Query deployment adds an authoritative catalog artifact and active pointer,
  but no new application write path.
- Public protocol, MCP tool visibility, and compatibility fixtures expand
  additively.
- TypeScript output is generated data; every first-party generator and runtime
  implementation remains Rust.

## Compatibility

This is an additive public protocol and catalog-artifact family. It changes no
existing Protobuf field number, RPC byte shape, durable entity/index/command
encoding, command plan hash, or MCP tool name. Query-module durable encoding,
hash preimage, strict decoder, and migration policy must be accepted before
WP-260 implementation.

## Security

Module deployment is an audited control-plane action. Query execution uses the
same authentication, authorization, admission, cancellation, response budgets,
redaction, and telemetry boundaries across gRPC and MCP. Generated clients and
MCP schemas convey no authority.

## Testing

- Durable/query-IR codecs, strict decoders, hash goldens, and restart recovery.
- Module idempotency, conflict, expected-active, missing-contract, and
  incompatible-contract tests.
- Protobuf descriptor, generated-source, MCP schema, and CLI output fixtures.
- Cross-transport semantic, authorization, diagnostic, and cursor parity.
- Rust/TypeScript generation determinism and identity-mismatch tests.

## Requirements and Work Packages

- **Requirements:** New post-POC application/query-module requirements assigned
  in WP-205
- **Defines or blocks:** WP-220, WP-250, WP-260, and WP-270
- **Final evidence:** WP-280

## Decision Deadline

Exact acceptance is required before public Protobuf, MCP registry, durable query
module, active pointer, generated compatibility fixture, or SDK signature is
changed.
