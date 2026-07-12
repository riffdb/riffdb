# ADR-0011: Canonical Values, Fixed-Scale Decimals, Keys, and Hashing

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Decision deadline:** Before WP-010 semantic types or fixtures merge

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

Values cross source, IR, runtime, storage, hashing, Protobuf, JSON Schema, gRPC,
MCP, and SDK boundaries. Rust layout, map iteration, floating point, locale, or
implicit Unicode normalization would make hashes and durable keys platform- or
process-dependent.

## Proposed Decision

`riffdb-types` owns one closed canonical value algebra for the POC. Business
decimals use a checked signed `i128` coefficient with explicitly declared
precision and scale; money includes an explicit currency identifier and uses no
floating point. Integer signedness and width, UUID, date, timestamp, boolean,
bounded text/bytes, bounded list, and record types remain distinct.

Canonical binary encoding uses explicit versioned type tags, fixed endianness,
length prefixes, and limits. Record fields are ordered by stable field ID; maps
and sets are sorted by canonical key bytes before encoding. Rust memory layout,
serde defaults, locale, insertion order, and randomized hashing never determine
bytes. Text policy is explicit UTF-8 with grammar/field-specific validation; no
implicit Unicode normalization changes identity.

Durable keys are purpose-specific encodings built from domain tags and canonical
components. Hashes use a specified, versioned, domain-separated cryptographic
hash over canonical bytes. Plan, source, input, event, and identity domains cannot
collide by construction. Secret/idempotency/capability lookup digests use the
separately specified keyed construction rather than an unkeyed content hash.
The custom Protobuf `riffdb.v1.Value` is an exact mapping of this algebra.

## Options Considered

1. **Checked i128 fixed-scale plus custom canonical bytes:** Approved POC choice.
2. **Arbitrary precision decimal:** More range but greater dependency and encoding
   surface than the POC requires.
3. **IEEE floating point:** Cannot provide exact decimal business semantics.
4. **Deterministic Protobuf as the only canonical encoding:** Couples domain
   identity to wire-generation behavior and does not alone define key ordering.
5. **Implicit NFC normalization:** Changes caller bytes and requires a broader
   Unicode/version policy.

## Consequences

- Arithmetic overflow, scale mismatch, and precision loss are typed failures.
- Conversion adapters must prove exact round trips or reject values.
- Changing any tag, ordering, bound, hash algorithm, or text rule is a versioned
  compatibility change.
- The exact hash algorithm and domain registry must be enumerated in the accepted
  revision before fixtures are generated.

## Compatibility

Value variants, numeric bounds, decimal/money encoding, text policy, field order,
key domains, canonical bytes, hash algorithm/version, and Protobuf mapping are
public and durable compatibility boundaries.

## Security

All lengths/nesting are bounded before allocation. Secret values are redacted
before diagnostics. Untrusted canonical bytes are decoded without panics.
Cryptographic hashes are content identifiers, not authorization or signatures.

## Testing

Cross-platform golden byte/hash/key vectors; decimal boundary, arithmetic, and
ordering properties; canonical collection permutation tests; exact Proto/JSON
round trips; malformed decoder fuzzing; maximum depth/length tests; and a central
domain-tag collision registry check.

## Requirements and Work Packages

- **Requirements:** `ID-001` through `ID-005`, `VAL-001` through `VAL-003`,
  `ENT-002`, `STO-002`, `TXN-042`
- **Defines or blocks:** `WP-010`, `WP-020`, `WP-040`, `WP-060`, `WP-090`, `WP-100`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Exact acceptance, including the algorithm/domain registry and decimal bounds, is
required before WP-010 publishes types or golden fixtures.
