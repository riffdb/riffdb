# ADR-0037: Rust SDK Foundational Type Dependencies

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Accepted:** 2026-07-21
- **Requires:** ADR-0006, ADR-0007, ADR-0018, and ADR-0028
- **Amends:** The `riffdb-client-rust` dependency row and WP-130 dependency evidence
- **Decision deadline:** Before WP-130 completion

The human maintainer accepted this exact decision and its authoritative-file
amendments on 2026-07-21.

## Context

The Rust SDK must return the one checked `PublicError` type and generate the
same UUIDv7 `RequestId`, `CapabilityId`, and `AgentSessionId` types used at the
public boundary. Those types have single semantic owners in `riffdb-errors` and
`riffdb-types`. The current specification describes the client dependency
boundary as Protobuf/Tonic plus the exact entropy dependency only, which makes
the direct foundational type edges nonconforming even though neither crate
grants database authority or implements database semantics.

Removing the direct edges would require either duplicating semantic types or
re-exporting unrelated Rust domain APIs through `riffdb-proto`. Duplication is
forbidden, and making the protocol crate a general facade obscures actual
ownership and produces an equivalent transitive dependency.

## Decision

`riffdb-client-rust` may depend directly on `riffdb-errors` and `riffdb-types`,
with default features disabled, solely to expose checked public errors and
foundational public identifier/value newtypes. It may also retain its already
reviewed `riffdb-proto`, client-only `riffdb-api-grpc`, exact Tonic/Tonic-Prost,
and exact ADR-0018 `getrandom` dependencies.

This exception grants no dependency on `riffdb-auth`, `riffdb-catalog`,
`riffdb-commit`, `riffdb-conflict`, `riffdb-policy`, `riffdb-runtime`,
`riffdb-service`, any storage crate, or server-only `riffdb-api-grpc` features.
The SDK owns transport ergonomics only. It does not validate contracts, select
schemas, authorize operations, derive idempotency identity, execute commands,
or resolve durable state except by calling the public protocol.

The generic retry wrapper retains identical submitted input bytes and a fresh
outer `RequestId`; it does not claim that a caller-supplied separate key is
schema-bound. Contract-generated modules own typed idempotency-key placement and
construct matching outcome-recovery requests.

## Options Considered

1. **Direct type-only foundational edges:** Accepted. Ownership stays explicit
   and the architecture tests can forbid every authority-bearing crate.
2. **Re-export types through `riffdb-proto`:** Rejected. This hides the same
   dependency and turns a protocol owner into a general Rust API facade.
3. **Duplicate SDK-local errors and identifiers:** Rejected. It creates two
   semantic owners and risks divergent validation.
4. **Return only generated Protobuf messages and raw bytes:** Rejected. It
   weakens the required checked, ergonomic Rust SDK boundary.

## Consequences

- The specification crate row and WP-130 dependency evidence name the two
  direct, type-only edges explicitly.
- Architecture tests freeze the allowlist and continue to reject every lower
  semantic or authoritative component.
- No public Protobuf, durable record, hash, key, service operation, or server
  composition changes.

## Testing

WP-130 tests assert the exact direct dependency allowlist, fail-closed public
error decoding, UUIDv7 construction vectors, fresh request IDs across retries,
byte-identical retry input, and generated typed idempotency recovery. `cargo
deny` and feature-tree evidence verify that the exception adds no optional
features or authority-bearing edge.

## Requirements and Work Packages

- **Requirements:** `API-001`, `ID-001`, `ID-005`, `POC-008`
- **Blocks:** `WP-130`
- **Final evidence:** `WP-130` and `WP-200`
