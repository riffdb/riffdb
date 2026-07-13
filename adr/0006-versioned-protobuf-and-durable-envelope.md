# ADR-0006: Versioned Protobuf and Durable Envelope

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Decision deadline:** Before WP-020 schemas or fixtures merge

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

Before the 2026-07-12 governance reconciliation, public gRPC sketches disagreed,
and `google.protobuf.Struct` cannot preserve all RiffDB integer and decimal values
exactly. Protobuf definitions are also needed before later semantic owners know
their complete records, so a one-time freeze in WP-020 would force guesses or
out-of-scope changes.

## Proposed Decision

SPEC Section 11 is the canonical v0.1 gRPC service inventory and naming surface:
its five services and descriptive RPC names are preserved, and Appendix B must be
reconciled to it. Public dynamic data uses a tagged `riffdb.v1.Value`, not
`google.protobuf.Struct`. The value supports exact signed and unsigned integers,
canonical fixed-scale decimal and money, UUID, date, timestamp, bounded list, and
record fields in canonical order. Floating-point values are not a business-value
fallback.

One proto owner maintains public and durable schemas. WP-020 freezes generation,
package/version rules, common exact values, error/envelope conventions, public
service names, bounds, descriptors, and compatibility fixtures. Semantic records
for commits, capabilities, outbox, and projections are added through small
proto-owner interface PRs only after their Rust semantic owner stabilizes. No
package duplicates a wire type to avoid coordination.

Durable payloads use explicit record/envelope versions and integrity metadata.
Field numbers are never reused, removed fields are reserved, decoders reject
unsupported versions safely, and the unknown-field retention policy is explicit
per persisted boundary. Pure-Rust deterministic generation and checked-in golden
descriptors/records are required.

The approved API-neutral Rust error boundary has exactly these transport-error
kinds: validation, idempotency-key reuse, authorization denial, concurrency
deadline, contract mismatch, storage unavailable, outcome unknown, and internal
defect. Declared business outcomes are never encoded as transport errors.
Projection `WaitTimedOut` and `Degraded` states remain typed projection query
results. WP-020 must map this boundary without adding transport-specific core
semantics.

Validation errors contain one through 16 issues. Each issue has a closed stable
code and a path of at most 16 structured segments; a segment is a stable
`FieldId` or a zero-based list index, never caller-controlled diagnostic text.
Contract mismatch always carries the active `ContractVersion`. Public messages
are static and safe, internal sources are absent, and an optional UUIDv7 incident
ID is the only diagnostic correlation value. The exact Protobuf field and enum
numbers remain Proposed until this ADR is accepted for WP-020.

## Options Considered

1. **Section 11 plus custom exact `Value` and phased ownership:** Approved model.
2. **Appendix B services:** Omits or renames parts of the canonical surface.
3. **`google.protobuf.Struct`:** Loses exact large-integer/decimal semantics.
4. **Freeze all future records in WP-020:** Forces premature durable design.
5. **Package-local proto ownership:** Risks duplicate and incompatible wire types.

## Consequences

- WP-020 needs a reviewed initial inventory but does not invent later record fields.
- Public wire errors must preserve the approved kind/detail invariants and bounds.
- Later schema changes require coordinated interface PRs and compatibility review.
- Adapters convert between proto values and `riffdb-types`; proto types do not
  become API-neutral service DTOs.
- Decode/re-encode behavior must be tested anywhere unknown-field preservation is
  promised.

## Compatibility

Package names, service/RPC names, field numbers, enum numbers, descriptor sets,
exact value semantics, envelope versions, and durable golden bytes are explicit
compatibility boundaries.

## Security

All decoders enforce size, nesting, collection, and diagnostic bounds. Public
errors contain safe fields only. Malformed, unknown, or oversized values fail
closed without panics or resource amplification.

## Testing

Golden current/previous descriptors and records, reserved-field checks,
cross-language exact-value vectors, deterministic regeneration, decoder fuzzing,
malformed/nesting/size limits, unknown-version tests, and public error redaction.

## Requirements and Work Packages

- **Requirements:** `API-001`, `VAL-001` through `VAL-003`, `STO-002`, `ID-004`,
  `ENT-002`, `ENT-003`
- **Defines or blocks:** `WP-020`; later interface PRs for `WP-050`, `WP-070`,
  `WP-100`, `WP-110`, `WP-130`, `WP-160`, `WP-170`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Exact acceptance is required before the first `.proto` compatibility fixture
merges. Later records require review under this policy, not a new ownership model.
