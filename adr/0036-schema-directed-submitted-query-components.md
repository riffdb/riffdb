# ADR-0036: Schema-Directed Submitted Query Components

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Accepted:** 2026-07-21
- **Requires:** ADR-0007, ADR-0011, ADR-0017, ADR-0028, ADR-0029, ADR-0031
- **Amends:** ADR-0028 index/projection request conversion and ADR-0031 scope
- **Decision deadline:** Before WP-130 implements ScanIndex and QueryProjection

The human maintainer accepted this exact decision and its authoritative-file
amendments on 2026-07-21.

## Context

Public `ScanIndexRequest.leading_components` and
`QueryProjectionRequest.leading_components` contain `riffdb.v1.Value`. A public
decimal carries coefficient and scale but intentionally omits precision. Enum
display names and referenced record values also require compiled schema
context. The current API-neutral request DTOs nevertheless require
`Vec<CanonicalValue>` before the service selects the requested contract.

The gRPC adapter cannot construct every valid canonical value without guessing
type information or loading the catalog. Catalog access in gRPC is forbidden by
ADR-0007 and ADR-0029. ADR-0028's wording that WP-130 resolves these values
before service invocation therefore conflicts with the accepted layering and
the implemented DTO boundary.

## Decision

`ScanIndexRequest` and `QueryProjectionRequest` own bounded
`Vec<SubmittedValue>` leading components. `SubmittedValue` remains the exact
service-owned, pre-schema, nonserializable family accepted by ADR-0031.
Constructors enforce the complete request, document, recursion, component-count,
and scalar bounds but claim no compiled-schema validity.

After selecting the exact active or historical contract, `riffdb-service`:

1. finds the requested index or projection schema;
2. requires no more submitted values than its ordered component count;
3. materializes each component against that exact declared `ValueType` and
   enum registry using the same schema-directed scalar materializer as command
   input;
4. validates and encodes the resulting canonical prefix; and
5. uses only that canonical vector for authorization facts, cursor binding,
   lower requests, projection waits, and response validation.

Caller-correctable materialization failures return bounded public Validation
before policy or lower-port access. An impossible mismatch in a checked schema
is an internal integrity failure. Submitted values never enter policy,
canonical hashing, cursor state, storage, or projection state.

WP-130 performs only structural Protobuf-to-`SubmittedValue` conversion. It has
no catalog or contract-IR dependency. Trusted API-neutral callers may use the
existing checked `CanonicalValue -> SubmittedValue` conversion; this does not
bypass service materialization.

## Options Considered

1. **Submitted components with service-owned materialization:** Proposed. It
   extends the already accepted command-input boundary consistently.
2. **Give gRPC catalog access:** Rejected. It creates a transport-specific
   semantic path and would not protect MCP, CLI, or in-process consumers.
3. **Guess decimal precision or discard names:** Rejected. It changes type
   validity and can change canonical bytes.
4. **Add redundant type parameters to public Value:** Rejected. It changes the
   public value algebra and lets callers select schema identity.
5. **Restrict query prefixes to schema-neutral scalar variants:** Rejected. It
   silently narrows the accepted public Value contract and index/projection
   expressiveness.

## Consequences

- The change is an internal service Rust API correction. Public Protobuf,
  durable records, canonical encoding, hashes, keys, cursor tokens, and
  projection formats do not change.
- WP-120 owns one reusable materializer and does not duplicate type rules in
  query orchestration.
- WP-130 conversion becomes total while retaining its no-catalog architecture
  test.

## Testing

WP-120 tests cover every submitted scalar, decimal precision/scale, money,
enum membership/display name, too many components, nested-value rejection for
scalar keys, canonical prefix parity, validation before policy/lower access,
and cursor binding to canonical rather than submitted representation. WP-130
tests cover total conversion and prove the gRPC crate has no catalog or IR edge.
WP-170 and WP-200 provide projection and public end-to-end evidence.

## Requirements and Work Packages

- **Requirements:** `API-001`, `VAL-003`, `POC-008`
- **Corrects:** `WP-120`
- **Blocks:** `WP-130`
- **Consumed by:** `WP-140`, `WP-170`, and `WP-200`
- **Final evidence:** `WP-200`
